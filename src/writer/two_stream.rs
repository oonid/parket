use super::DeltaWriter;
use super::hwm::build_two_stream_commit_properties;
use anyhow::{Context, Result};
use deltalake::arrow::array::{Array, Int32Array, Int64Array, UInt32Array, UInt64Array};
use deltalake::arrow::datatypes::DataType;
use deltalake::arrow::record_batch::RecordBatch;
use deltalake::datafusion::datasource::MemTable;
use deltalake::datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
use deltalake::datafusion::execution::memory_pool::FairSpillPool;
use deltalake::datafusion::execution::runtime_env::RuntimeEnvBuilder;
use deltalake::datafusion::prelude::{SessionConfig, SessionContext};
use deltalake::protocol::SaveMode;
use tracing::info;

impl DeltaWriter {
    /// Upsert `batches` into the table, matching on `key_col`: existing keys updated
    /// (non-key columns only), new keys inserted. Both stream watermarks ride the commit.
    /// The table must already exist (caller runs ensure_table first).
    ///
    /// Deduplicates the source by `key_col` before merging to prevent MERGE cardinality
    /// violations when the source contains duplicate keys.
    pub async fn merge_batch(
        &self,
        table_name: &str,
        batches: Vec<RecordBatch>,
        key_col: &str,
        insert_id: Option<i64>,
        update_hwm: Option<&super::Hwm>,
    ) -> Result<()> {
        if batches.is_empty() || batches.iter().all(|b| b.num_rows() == 0) {
            return Ok(());
        }
        let url = self.table_url(table_name)?;
        let mut table = deltalake::DeltaTableBuilder::from_url(url)?
            .with_storage_options(self.storage_options.clone())
            .build()?;
        table.load().await?;

        let schema = batches[0].schema();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

        let pool_bytes = (self.merge_memory_mb as usize) * 1024 * 1024;
        // Route the external sort's spill to the configured dir (MERGE_SPILL_DIR); else system temp.
        let disk_builder = match &self.merge_spill_dir {
            Some(dir) => DiskManagerBuilder::default()
                .with_mode(DiskManagerMode::Directories(vec![dir.clone()])),
            None => DiskManagerBuilder::default(),
        };
        let runtime = std::sync::Arc::new(
            RuntimeEnvBuilder::new()
                .with_memory_pool(std::sync::Arc::new(FairSpillPool::new(pool_bytes)))
                .with_disk_manager_builder(disk_builder)
                .build()?,
        );
        let mut session_config = SessionConfig::new();
        // Force a spillable SortMergeJoin — datafusion 53's HashJoin does NOT spill, so under a
        // bounded pool it would error instead of spilling.
        session_config.options_mut().optimizer.prefer_hash_join = false;
        // Optional tuning: override datafusion's `sort_spill_reservation_bytes` (default 10 MB),
        // the memory reserved for the external sort's merge phase. On "Not enough memory to
        // continue external sort", a SMALLER value shrinks the merge's reservation request and
        // can let a bounded pool finish (datafusion's own hint). Read from env (advanced knob).
        if let Ok(raw) = std::env::var("MERGE_SORT_RESERVATION_MB")
            && let Ok(mb) = raw.trim().parse::<usize>()
        {
            session_config.options_mut().execution.sort_spill_reservation_bytes = mb * 1024 * 1024;
            info!(
                table = table_name,
                merge_sort_reservation_mb = mb,
                "merge: sort_spill_reservation_bytes overridden"
            );
        }
        // Pin the external sort to a single partition by default. datafusion runs one external
        // sorter per partition (default = CPU count), and they ALL share this one FairSpillPool,
        // so fan-out fragments the pool and the merge phase starves even with a large pool
        // ("Failed to allocate … for ExternalSorterMerge[N]"). One partition = one sorter owns the
        // whole pool → fewer runs, the merge fits. Override via MERGE_TARGET_PARTITIONS (>0).
        let merge_partitions = std::env::var("MERGE_TARGET_PARTITIONS")
            .ok()
            .and_then(|v| v.trim().parse::<usize>().ok())
            .filter(|&n| n > 0)
            .unwrap_or(1);
        session_config.options_mut().execution.target_partitions = merge_partitions;
        let ctx = SessionContext::new_with_config_rt(session_config, runtime);
        info!(
            table = table_name,
            merge_memory_mb = self.merge_memory_mb,
            merge_target_partitions = merge_partitions,
            "merge: bounded datafusion session (spills to disk)"
        );

        // Zero-copy registration: MemTable takes ownership of `batches` directly (a Vec of
        // partitions, one Vec<RecordBatch> per partition) instead of concat_batches'ing them
        // into a single contiguous copy first (M3 — halves peak memory for the update window).
        let provider = MemTable::try_new(schema.clone(), vec![batches])?;
        ctx.register_table("merge_source_raw", std::sync::Arc::new(provider))?;

        let col_names: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
        let col_list = col_names.join(", ");
        let dedup_sql = format!(
            "SELECT {col_list} FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY {key_col} ORDER BY {key_col}) AS __rn FROM merge_source_raw) WHERE __rn = 1"
        );
        let source = ctx.sql(&dedup_sql).await?;

        use deltalake::datafusion::prelude::col;
        let predicate = col(format!("target.{key_col}")).eq(col(format!("source.{key_col}")));
        let commit_properties = build_two_stream_commit_properties(insert_id, update_hwm);

        table
            .merge(source, predicate)
            .with_source_alias("source")
            .with_target_alias("target")
            .with_commit_properties(commit_properties)
            .with_session_state(std::sync::Arc::new(ctx.state()))
            .when_matched_update(|mut update| {
                for name in &col_names {
                    if name == key_col { continue; }
                    update = update.update(name.clone(), col(format!("source.{name}")));
                }
                update
            })?
            .when_not_matched_insert(|mut insert| {
                for name in &col_names {
                    insert = insert.set(name.clone(), col(format!("source.{name}")));
                }
                insert
            })?
            .await?;

        info!(table = table_name, rows = total_rows, "merge committed");
        Ok(())
    }

    /// Append new rows (insert stream of two-stream mode) carrying BOTH watermarks on
    /// the commit. Insert-stream rows are strictly new ids, so append (not merge) is
    /// correct and cheap.
    pub async fn append_two_stream(
        &self,
        table_name: &str,
        batches: Vec<RecordBatch>,
        insert_id: Option<i64>,
        update_hwm: Option<&super::Hwm>,
    ) -> Result<()> {
        if batches.is_empty() || batches.iter().all(|b| b.num_rows() == 0) {
            return Ok(());
        }
        let url = self.table_url(table_name)?;
        let mut table = deltalake::DeltaTableBuilder::from_url(url)?
            .with_storage_options(self.storage_options.clone())
            .build()?;
        table.load().await?;

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        let commit_properties = build_two_stream_commit_properties(insert_id, update_hwm);

        table
            .write(batches)
            .with_save_mode(SaveMode::Append)
            .with_commit_properties(commit_properties)
            .await?;

        info!(table = table_name, rows = total_rows, "two-stream insert appended");
        Ok(())
    }

    pub async fn read_insert_hwm(&self, table_name: &str) -> Result<Option<i64>> {
        let table = match self.open_table(table_name).await {
            Ok(t) => t,
            Err(e) => {
                if super::is_missing_table_error(&e) {
                    return Ok(None);
                }
                return Err(e).context(format!(
                    "read insert HWM: could not open Delta table `{table_name}`"
                ));
            }
        };
        let mut history = table.history(Some(1)).await?.collect::<Vec<_>>();
        let commit_info = match history.pop() {
            Some(ci) => ci,
            None => return Ok(None),
        };
        match commit_info.info.get("hwm_insert_id") {
            Some(serde_json::Value::String(s)) =>
                Ok(Some(s.parse().context("invalid hwm_insert_id in commitInfo")?)),
            _ => Ok(None),
        }
    }

    /// Upsert via DELETE-by-key + APPEND (bounded-memory alternative to `merge_batch`).
    /// Deletes every target row whose `key_col` is in the incoming batch, then appends the
    /// batch (the new versions). The DELETE is a streaming scan+filter+rewrite (no join/sort),
    /// so memory stays bounded regardless of target size. The APPEND carries the commit
    /// properties (watermarks), so the HWM advances only after the append commits.
    ///
    /// Deduplicates the source by `key_col` before deleting/appending to prevent issues
    /// when the source contains duplicate keys (keeps one row per key).
    pub async fn delete_then_append(
        &self,
        table_name: &str,
        batches: Vec<RecordBatch>,
        key_col: &str,
        insert_id: Option<i64>,
        update_hwm: Option<&super::Hwm>,
    ) -> Result<()> {
        use deltalake::datafusion::prelude::{cast, col, lit};

        if batches.is_empty() || batches.iter().all(|b| b.num_rows() == 0) {
            return Ok(());
        }
        let url = self.table_url(table_name)?;
        let mut table = deltalake::DeltaTableBuilder::from_url(url)?
            .with_storage_options(self.storage_options.clone())
            .build()?;
        table.load().await?;

        // Dedup the incoming batches by key_col (same pattern as merge_batch).
        let schema = batches[0].schema();
        let ctx = SessionContext::new();
        // Zero-copy registration: MemTable moves `batches` in directly instead of
        // concat_batches'ing them into a separate contiguous copy first (M3).
        let provider = MemTable::try_new(schema.clone(), vec![batches])?;
        ctx.register_table("delete_source_raw", std::sync::Arc::new(provider))?;

        let col_names: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
        let col_list = col_names.join(", ");
        let dedup_sql = format!(
            "SELECT {col_list} FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY {key_col} ORDER BY {key_col}) AS __rn FROM delete_source_raw) WHERE __rn = 1"
        );
        let deduped_source = ctx.sql(&dedup_sql).await?;
        let deduped_batches = deduped_source.collect().await?;

        // Collect the distinct i64 keys present in the deduplicated batches.
        // Handle Int64Array, Int32Array, UInt64Array, UInt32Array.
        // Keys are already distinct (the dedup SQL keeps one row per key_col), so a Vec
        // preserves distinctness without the HashSet's hash overhead.
        let mut ids: Vec<i64> = Vec::new();
        for b in &deduped_batches {
            let idx = b.schema().index_of(key_col)?;
            let c = b.column(idx);
            if let Some(a) = c.as_any().downcast_ref::<Int64Array>() {
                for i in 0..a.len() {
                    if !a.is_null(i) {
                        ids.push(a.value(i));
                    }
                }
            } else if let Some(a) = c.as_any().downcast_ref::<Int32Array>() {
                for i in 0..a.len() {
                    if !a.is_null(i) {
                        ids.push(a.value(i) as i64);
                    }
                }
            } else if let Some(a) = c.as_any().downcast_ref::<UInt64Array>() {
                for i in 0..a.len() {
                    if !a.is_null(i) {
                        ids.push(a.value(i) as i64);
                    }
                }
            } else if let Some(a) = c.as_any().downcast_ref::<UInt32Array>() {
                for i in 0..a.len() {
                    if !a.is_null(i) {
                        ids.push(a.value(i) as i64);
                    }
                }
            } else {
                return Err(anyhow::anyhow!(
                    "delete_then_append: key column `{key_col}` has unsupported type {:?} (expected an integer)",
                    c.data_type()
                ));
            }
        }

        // 1) DELETE the existing rows for those keys (streaming scan+filter+rewrite), in
        //    bounded-size chunks. A single IN-list over every key gets OR-normalized into a
        //    deep predicate tree whose plan traversal overflows the stack on large batches
        //    (the reason main.rs runs on a 512 MB stack); chunking caps the depth.
        const DELETE_KEYS_PER_CHUNK: usize = 1024;
        if !ids.is_empty() {
            for chunk in ids.chunks(DELETE_KEYS_PER_CHUNK) {
                let list: Vec<_> = chunk.iter().map(|id| lit(*id)).collect();
                let predicate = cast(col(key_col), DataType::Int64).in_list(list, false);
                let (t, _metrics) = table.delete().with_predicate(predicate).await?;
                table = t;
            }
            info!(table = table_name, keys = ids.len(), "delete_then_append: deleted existing rows for incoming keys");
        }

        // 2) APPEND the deduplicated versions; the watermarks ride on this commit.
        let total_rows: usize = deduped_batches.iter().map(|b| b.num_rows()).sum();
        let commit_properties = build_two_stream_commit_properties(insert_id, update_hwm);
        table
            .write(deduped_batches)
            .with_save_mode(SaveMode::Append)
            .with_commit_properties(commit_properties)
            .await?;
        info!(table = table_name, rows = total_rows, "delete_then_append: appended new versions");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use deltalake::arrow::array::{Int32Array, Int64Array, StringArray, UInt32Array, UInt64Array};
    use deltalake::arrow::datatypes::{DataType, Schema as ArrowSchema, Field};
    use deltalake::arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    #[tokio::test]
    async fn read_insert_hwm_nonexistent_table() {
        let temp = tempfile::tempdir().unwrap();
        let writer = DeltaWriter::new_local(temp.path().to_str().unwrap());
        let insert_hwm = writer.read_insert_hwm("nonexistent").await.unwrap();
        assert!(insert_hwm.is_none());
    }

    #[tokio::test]
    async fn read_insert_hwm_s3_error_propagates() {
        let writer = DeltaWriter::new(
            "nonexistent-bucket",
            "prefix",
            Some("http://localhost:1"),
            "us-east-1",
            "fake",
            "fake",
        );

        let result = writer.read_insert_hwm("nonexistent").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn read_insert_hwm_round_trip() {
        let temp = tempfile::tempdir().unwrap();
        let writer = DeltaWriter::new_local(temp.path().to_str().unwrap());
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
        ]));

        writer
            .ensure_table("test_table", schema.clone())
            .await
            .unwrap();

        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![1i64, 2i64]))],
        )
        .unwrap();

        let update_hwm = super::super::Hwm {
            updated_at: "2026-03-28 10:00:00".to_string(),
            last_id: 5,
        };

        let table = writer.open_table("test_table").await.unwrap();
        table.write(vec![batch])
            .with_save_mode(SaveMode::Append)
            .with_commit_properties(build_two_stream_commit_properties(Some(42), Some(&update_hwm)))
            .await
            .unwrap();

        let insert_hwm = writer.read_insert_hwm("test_table").await.unwrap();
        assert_eq!(insert_hwm, Some(42));
    }

    #[tokio::test]
    async fn merge_batch_upsert_workflow() {
        use deltalake::arrow::array::StringViewArray;
        let temp = tempfile::tempdir().unwrap();
        let writer = DeltaWriter::new_local(temp.path().to_str().unwrap());
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Utf8, false),
        ]));

        writer
            .ensure_table("t", schema.clone())
            .await
            .unwrap();

        // First merge: insert (1,"a") and (2,"b")
        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1i64, 2i64])),
                Arc::new(StringArray::from(vec!["a", "b"])),
            ],
        )
        .unwrap();

        writer
            .merge_batch("t", vec![batch1], "id", Some(2), None)
            .await
            .unwrap();

        // Second merge: update (1,"A") and insert (3,"c")
        let batch2 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1i64, 3i64])),
                Arc::new(StringArray::from(vec!["A", "c"])),
            ],
        )
        .unwrap();

        writer
            .merge_batch("t", vec![batch2], "id", Some(3), None)
            .await
            .unwrap();

        // Read back and verify final state
        let t = writer.open_table("t").await.unwrap();
        let ctx = deltalake::datafusion::prelude::SessionContext::new();
        let provider = t.table_provider().await.unwrap();
        ctx.register_table("t", provider).unwrap();
        let batches = ctx
            .sql("SELECT id, value FROM t ORDER BY id")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 3);

        let id_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let value_col = batch.column(1);

        // Check id values
        assert_eq!(id_col.value(0), 1i64);
        assert_eq!(id_col.value(1), 2i64);
        assert_eq!(id_col.value(2), 3i64);

        // Check string values (may be StringArray or StringViewArray)
        let value_0 = if let Some(str_arr) = value_col.as_any().downcast_ref::<StringArray>() {
            str_arr.value(0).to_string()
        } else if let Some(str_view_arr) = value_col.as_any().downcast_ref::<StringViewArray>() {
            str_view_arr.value(0).to_string()
        } else {
            panic!("Unexpected value column type");
        };

        let value_1 = if let Some(str_arr) = value_col.as_any().downcast_ref::<StringArray>() {
            str_arr.value(1).to_string()
        } else if let Some(str_view_arr) = value_col.as_any().downcast_ref::<StringViewArray>() {
            str_view_arr.value(1).to_string()
        } else {
            panic!("Unexpected value column type");
        };

        let value_2 = if let Some(str_arr) = value_col.as_any().downcast_ref::<StringArray>() {
            str_arr.value(2).to_string()
        } else if let Some(str_view_arr) = value_col.as_any().downcast_ref::<StringViewArray>() {
            str_view_arr.value(2).to_string()
        } else {
            panic!("Unexpected value column type");
        };

        assert_eq!(value_0, "A");
        assert_eq!(value_1, "b");
        assert_eq!(value_2, "c");

        // Check insert hwm
        let insert_hwm = writer.read_insert_hwm("t").await.unwrap();
        assert_eq!(insert_hwm, Some(3));
    }

    #[tokio::test]
    async fn merge_batch_bounded_pool_preserves_correctness() {
        use deltalake::arrow::array::StringViewArray;
        let temp = tempfile::tempdir().unwrap();
        let spill = tempfile::tempdir().unwrap();
        let writer = DeltaWriter::new_local(temp.path().to_str().unwrap())
            .with_merge_limits(32, Some(spill.path().to_path_buf()));
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Utf8, false),
        ]));

        writer
            .ensure_table("t", schema.clone())
            .await
            .unwrap();

        // First merge: insert (1,"a") and (2,"b")
        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1i64, 2i64])),
                Arc::new(StringArray::from(vec!["a", "b"])),
            ],
        )
        .unwrap();

        writer
            .merge_batch("t", vec![batch1], "id", Some(2), None)
            .await
            .unwrap();

        // Second merge: update (1,"A") and insert (3,"c")
        let batch2 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1i64, 3i64])),
                Arc::new(StringArray::from(vec!["A", "c"])),
            ],
        )
        .unwrap();

        writer
            .merge_batch("t", vec![batch2], "id", Some(3), None)
            .await
            .unwrap();

        // Read back and verify final state
        let t = writer.open_table("t").await.unwrap();
        let ctx = deltalake::datafusion::prelude::SessionContext::new();
        let provider = t.table_provider().await.unwrap();
        ctx.register_table("t", provider).unwrap();
        let batches = ctx
            .sql("SELECT id, value FROM t ORDER BY id")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 3);

        let id_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let value_col = batch.column(1);

        // Check id values
        assert_eq!(id_col.value(0), 1i64);
        assert_eq!(id_col.value(1), 2i64);
        assert_eq!(id_col.value(2), 3i64);

        // Check string values (may be StringArray or StringViewArray)
        let value_0 = if let Some(str_arr) = value_col.as_any().downcast_ref::<StringArray>() {
            str_arr.value(0).to_string()
        } else if let Some(str_view_arr) = value_col.as_any().downcast_ref::<StringViewArray>() {
            str_view_arr.value(0).to_string()
        } else {
            panic!("Unexpected value column type");
        };

        let value_1 = if let Some(str_arr) = value_col.as_any().downcast_ref::<StringArray>() {
            str_arr.value(1).to_string()
        } else if let Some(str_view_arr) = value_col.as_any().downcast_ref::<StringViewArray>() {
            str_view_arr.value(1).to_string()
        } else {
            panic!("Unexpected value column type");
        };

        let value_2 = if let Some(str_arr) = value_col.as_any().downcast_ref::<StringArray>() {
            str_arr.value(2).to_string()
        } else if let Some(str_view_arr) = value_col.as_any().downcast_ref::<StringViewArray>() {
            str_view_arr.value(2).to_string()
        } else {
            panic!("Unexpected value column type");
        };

        assert_eq!(value_0, "A");
        assert_eq!(value_1, "b");
        assert_eq!(value_2, "c");

        // Check insert hwm
        let insert_hwm = writer.read_insert_hwm("t").await.unwrap();
        assert_eq!(insert_hwm, Some(3));
    }

    #[tokio::test]
    async fn merge_batch_empty_batches_noop() {
        let temp = tempfile::tempdir().unwrap();
        let writer = DeltaWriter::new_local(temp.path().to_str().unwrap());
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Utf8, false),
        ]));

        writer
            .ensure_table("t", schema.clone())
            .await
            .unwrap();

        // Merge with empty batch vector — should be no-op
        writer
            .merge_batch("t", vec![], "id", Some(1), None)
            .await
            .unwrap();

        let t = writer.open_table("t").await.unwrap();
        let ctx = deltalake::datafusion::prelude::SessionContext::new();
        let provider = t.table_provider().await.unwrap();
        ctx.register_table("t", provider).unwrap();
        let batches = ctx
            .sql("SELECT COUNT(*) as cnt FROM t")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        assert_eq!(batches.len(), 1);
        let cnt_col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<deltalake::arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(cnt_col.value(0), 0i64);
    }

    #[tokio::test]
    async fn merge_batch_dedup_duplicate_keys() {
        // Test that merge_batch deduplicates source keys, fixing cardinality violations.
        // Step F10.3a: Ensure MERGE never hits "matched a target row with multiple source rows".
        use deltalake::arrow::array::StringViewArray;
        let temp = tempfile::tempdir().unwrap();
        let writer = DeltaWriter::new_local(temp.path().to_str().unwrap());
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Utf8, false),
        ]));

        writer
            .ensure_table("t", schema.clone())
            .await
            .unwrap();

        // First merge: insert id=1 with value="a"
        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1i64])),
                Arc::new(StringArray::from(vec!["a"])),
            ],
        )
        .unwrap();

        writer
            .merge_batch("t", vec![batch1], "id", Some(1), None)
            .await
            .unwrap();

        // Second merge: source contains DUPLICATE keys: id=1 twice (values "X" and "Y") and id=2 once (value "b")
        // Before fix: MERGE would fail with "matched a target row with multiple source rows"
        // After fix (dedup): MERGE succeeds, keeping one row per key
        let batch2 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1i64, 1i64, 2i64])),
                Arc::new(StringArray::from(vec!["X", "Y", "b"])),
            ],
        )
        .unwrap();

        writer
            .merge_batch("t", vec![batch2], "id", Some(2), None)
            .await
            .unwrap();

        // Read back and verify
        let t = writer.open_table("t").await.unwrap();
        let ctx = deltalake::datafusion::prelude::SessionContext::new();
        let provider = t.table_provider().await.unwrap();
        ctx.register_table("t", provider).unwrap();
        let batches = ctx
            .sql("SELECT id, value FROM t ORDER BY id")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 2, "Should have 2 rows: id=1 and id=2");

        let id_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let value_col = batch.column(1);

        // Verify ids
        assert_eq!(id_col.value(0), 1i64);
        assert_eq!(id_col.value(1), 2i64);

        // Verify id=1 has been updated (dedup keeps one row deterministically)
        // and id=2 has been inserted
        let value_0 = if let Some(str_arr) = value_col.as_any().downcast_ref::<StringArray>() {
            str_arr.value(0).to_string()
        } else if let Some(str_view_arr) = value_col.as_any().downcast_ref::<StringViewArray>() {
            str_view_arr.value(0).to_string()
        } else {
            panic!("Unexpected value column type");
        };

        let value_1 = if let Some(str_arr) = value_col.as_any().downcast_ref::<StringArray>() {
            str_arr.value(1).to_string()
        } else if let Some(str_view_arr) = value_col.as_any().downcast_ref::<StringViewArray>() {
            str_view_arr.value(1).to_string()
        } else {
            panic!("Unexpected value column type");
        };

        // id=1 should have one of the deduped values (dedup keeps first by row order)
        assert!(value_0 == "X" || value_0 == "Y", "id=1 value should be X or Y, got {}", value_0);
        assert_eq!(value_1, "b", "id=2 should have value 'b'");
    }

    #[tokio::test]
    async fn append_two_stream_inserts_rows_with_both_watermarks() {
        let temp = tempfile::tempdir().unwrap();
        let writer = DeltaWriter::new_local(temp.path().to_str().unwrap());
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Utf8, false),
        ]));

        writer
            .ensure_table("t", schema.clone())
            .await
            .unwrap();

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1i64, 2i64])),
                Arc::new(StringArray::from(vec!["a", "b"])),
            ],
        )
        .unwrap();

        writer
            .append_two_stream("t", vec![batch], Some(2), None)
            .await
            .unwrap();

        // Verify insert_id is recorded
        let insert_hwm = writer.read_insert_hwm("t").await.unwrap();
        assert_eq!(insert_hwm, Some(2));

        // Verify rows are in the table
        let t = writer.open_table("t").await.unwrap();
        let ctx = deltalake::datafusion::prelude::SessionContext::new();
        let provider = t.table_provider().await.unwrap();
        ctx.register_table("t", provider).unwrap();
        let batches = ctx
            .sql("SELECT id FROM t ORDER BY id")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 2);

        let id_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(id_col.value(0), 1i64);
        assert_eq!(id_col.value(1), 2i64);
    }

    #[tokio::test]
    async fn append_two_stream_empty_batches_noop() {
        let temp = tempfile::tempdir().unwrap();
        let writer = DeltaWriter::new_local(temp.path().to_str().unwrap());
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Utf8, false),
        ]));

        writer
            .ensure_table("t", schema.clone())
            .await
            .unwrap();

        // Append with empty batch vector — should be no-op
        writer
            .append_two_stream("t", vec![], Some(1), None)
            .await
            .unwrap();

        let t = writer.open_table("t").await.unwrap();
        let ctx = deltalake::datafusion::prelude::SessionContext::new();
        let provider = t.table_provider().await.unwrap();
        ctx.register_table("t", provider).unwrap();
        let batches = ctx
            .sql("SELECT COUNT(*) as cnt FROM t")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        assert_eq!(batches.len(), 1);
        let cnt_col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<deltalake::arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(cnt_col.value(0), 0i64);
    }

    #[tokio::test]
    async fn delete_then_append_upsert_workflow() {
        use deltalake::arrow::array::StringViewArray;
        let temp = tempfile::tempdir().unwrap();
        let writer = DeltaWriter::new_local(temp.path().to_str().unwrap());
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Utf8, false),
        ]));

        writer
            .ensure_table("t", schema.clone())
            .await
            .unwrap();

        // First append: insert (1,"a") and (2,"b")
        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1i64, 2i64])),
                Arc::new(StringArray::from(vec!["a", "b"])),
            ],
        )
        .unwrap();

        writer
            .append_two_stream("t", vec![batch1], Some(2), None)
            .await
            .unwrap();

        // Second operation: delete-then-append with (1,"A") and (3,"c")
        // This deletes id=1 and appends id=1 with new value and id=3 as new row
        let batch2 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1i64, 3i64])),
                Arc::new(StringArray::from(vec!["A", "c"])),
            ],
        )
        .unwrap();

        writer
            .delete_then_append("t", vec![batch2], "id", Some(3), None)
            .await
            .unwrap();

        // Read back and verify final state
        let t = writer.open_table("t").await.unwrap();
        let ctx = deltalake::datafusion::prelude::SessionContext::new();
        let provider = t.table_provider().await.unwrap();
        ctx.register_table("t", provider).unwrap();
        let batches = ctx
            .sql("SELECT id, value FROM t ORDER BY id")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 3);

        let id_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let value_col = batch.column(1);

        // Check id values
        assert_eq!(id_col.value(0), 1i64);
        assert_eq!(id_col.value(1), 2i64);
        assert_eq!(id_col.value(2), 3i64);

        // Check string values (may be StringArray or StringViewArray)
        let value_0 = if let Some(str_arr) = value_col.as_any().downcast_ref::<StringArray>() {
            str_arr.value(0).to_string()
        } else if let Some(str_view_arr) = value_col.as_any().downcast_ref::<StringViewArray>() {
            str_view_arr.value(0).to_string()
        } else {
            panic!("Unexpected value column type");
        };

        let value_1 = if let Some(str_arr) = value_col.as_any().downcast_ref::<StringArray>() {
            str_arr.value(1).to_string()
        } else if let Some(str_view_arr) = value_col.as_any().downcast_ref::<StringViewArray>() {
            str_view_arr.value(1).to_string()
        } else {
            panic!("Unexpected value column type");
        };

        let value_2 = if let Some(str_arr) = value_col.as_any().downcast_ref::<StringArray>() {
            str_arr.value(2).to_string()
        } else if let Some(str_view_arr) = value_col.as_any().downcast_ref::<StringViewArray>() {
            str_view_arr.value(2).to_string()
        } else {
            panic!("Unexpected value column type");
        };

        assert_eq!(value_0, "A");
        assert_eq!(value_1, "b");
        assert_eq!(value_2, "c");

        // Check insert hwm
        let insert_hwm = writer.read_insert_hwm("t").await.unwrap();
        assert_eq!(insert_hwm, Some(3));
    }

    #[tokio::test]
    async fn delete_then_append_upsert_uint64_key() {
        use deltalake::arrow::array::StringViewArray;
        let temp = tempfile::tempdir().unwrap();
        let writer = DeltaWriter::new_local(temp.path().to_str().unwrap());
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::UInt64, false),
            Field::new("value", DataType::Utf8, false),
        ]));

        writer
            .ensure_table("t", schema.clone())
            .await
            .unwrap();

        // First append: insert (1,"a") and (2,"b")
        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from(vec![1u64, 2u64])),
                Arc::new(StringArray::from(vec!["a", "b"])),
            ],
        )
        .unwrap();

        writer
            .append_two_stream("t", vec![batch1], Some(2), None)
            .await
            .unwrap();

        // Second operation: delete-then-append with (1,"A") and (3,"c")
        // This deletes id=1 and appends id=1 with new value and id=3 as new row
        let batch2 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from(vec![1u64, 3u64])),
                Arc::new(StringArray::from(vec!["A", "c"])),
            ],
        )
        .unwrap();

        writer
            .delete_then_append("t", vec![batch2], "id", Some(3), None)
            .await
            .unwrap();

        // Read back and verify final state
        let t = writer.open_table("t").await.unwrap();
        let ctx = deltalake::datafusion::prelude::SessionContext::new();
        let provider = t.table_provider().await.unwrap();
        ctx.register_table("t", provider).unwrap();
        let batches = ctx
            .sql("SELECT id, value FROM t ORDER BY id")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 3);

        let id_col_raw = batch.column(0);
        let value_col = batch.column(1);

        // Check id values - handle both UInt64Array and Int64Array conversions
        let (id_0, id_1, id_2) = if let Some(arr) = id_col_raw.as_any().downcast_ref::<UInt64Array>() {
            (arr.value(0), arr.value(1), arr.value(2))
        } else if let Some(arr) = id_col_raw.as_any().downcast_ref::<Int64Array>() {
            (arr.value(0) as u64, arr.value(1) as u64, arr.value(2) as u64)
        } else {
            panic!("Unexpected id column type");
        };
        assert_eq!(id_0, 1u64);
        assert_eq!(id_1, 2u64);
        assert_eq!(id_2, 3u64);

        // Check string values (may be StringArray or StringViewArray)
        let value_0 = if let Some(str_arr) = value_col.as_any().downcast_ref::<StringArray>() {
            str_arr.value(0).to_string()
        } else if let Some(str_view_arr) = value_col.as_any().downcast_ref::<StringViewArray>() {
            str_view_arr.value(0).to_string()
        } else {
            panic!("Unexpected value column type");
        };

        let value_1 = if let Some(str_arr) = value_col.as_any().downcast_ref::<StringArray>() {
            str_arr.value(1).to_string()
        } else if let Some(str_view_arr) = value_col.as_any().downcast_ref::<StringViewArray>() {
            str_view_arr.value(1).to_string()
        } else {
            panic!("Unexpected value column type");
        };

        let value_2 = if let Some(str_arr) = value_col.as_any().downcast_ref::<StringArray>() {
            str_arr.value(2).to_string()
        } else if let Some(str_view_arr) = value_col.as_any().downcast_ref::<StringViewArray>() {
            str_view_arr.value(2).to_string()
        } else {
            panic!("Unexpected value column type");
        };

        assert_eq!(value_0, "A");
        assert_eq!(value_1, "b");
        assert_eq!(value_2, "c");

        // Check insert hwm
        let insert_hwm = writer.read_insert_hwm("t").await.unwrap();
        assert_eq!(insert_hwm, Some(3));
    }

    #[tokio::test]
    async fn delete_then_append_dedups_duplicate_keys() {
        use deltalake::arrow::array::StringViewArray;
        let temp = tempfile::tempdir().unwrap();
        let writer = DeltaWriter::new_local(temp.path().to_str().unwrap());
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Utf8, false),
        ]));

        writer
            .ensure_table("t", schema.clone())
            .await
            .unwrap();

        // Batch with DUPLICATE keys: id=1 appears twice (values "x" and "y"), id=2 once (value "b")
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1i64, 1i64, 2i64])),
                Arc::new(StringArray::from(vec!["x", "y", "b"])),
            ],
        )
        .unwrap();

        // delete_then_append should dedup and keep only one row per key
        writer
            .delete_then_append("t", vec![batch], "id", Some(2), None)
            .await
            .unwrap();

        // Read back and verify
        let t = writer.open_table("t").await.unwrap();
        let ctx = deltalake::datafusion::prelude::SessionContext::new();
        let provider = t.table_provider().await.unwrap();
        ctx.register_table("t", provider).unwrap();
        let batches = ctx
            .sql("SELECT id, value FROM t ORDER BY id")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        // After dedup: exactly 2 rows (id=1 once, id=2 once)
        assert_eq!(batch.num_rows(), 2, "Should have exactly 2 rows after dedup");

        let id_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let value_col = batch.column(1);

        // Check ids
        assert_eq!(id_col.value(0), 1i64);
        assert_eq!(id_col.value(1), 2i64);

        // Check that id=1 has one of the deduped values (either "x" or "y")
        let value_0 = if let Some(str_arr) = value_col.as_any().downcast_ref::<StringArray>() {
            str_arr.value(0).to_string()
        } else if let Some(str_view_arr) = value_col.as_any().downcast_ref::<StringViewArray>() {
            str_view_arr.value(0).to_string()
        } else {
            panic!("Unexpected value column type");
        };

        let value_1 = if let Some(str_arr) = value_col.as_any().downcast_ref::<StringArray>() {
            str_arr.value(1).to_string()
        } else if let Some(str_view_arr) = value_col.as_any().downcast_ref::<StringViewArray>() {
            str_view_arr.value(1).to_string()
        } else {
            panic!("Unexpected value column type");
        };

        // id=1 should have one of the deduplicated values
        assert!(
            value_0 == "x" || value_0 == "y",
            "id=1 value should be 'x' or 'y', got '{}'",
            value_0
        );
        // id=2 should have value "b"
        assert_eq!(value_1, "b", "id=2 should have value 'b'");
    }

    #[tokio::test]
    async fn delete_then_append_upsert_int32_key() {
        use deltalake::arrow::array::StringViewArray;
        let temp = tempfile::tempdir().unwrap();
        let writer = DeltaWriter::new_local(temp.path().to_str().unwrap());
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Utf8, false),
        ]));

        writer
            .ensure_table("t", schema.clone())
            .await
            .unwrap();

        // First append: insert (1,"a") and (2,"b")
        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1i32, 2i32])),
                Arc::new(StringArray::from(vec!["a", "b"])),
            ],
        )
        .unwrap();

        writer
            .append_two_stream("t", vec![batch1], Some(2), None)
            .await
            .unwrap();

        // Second operation: delete-then-append with (1,"A") and (3,"c")
        let batch2 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1i32, 3i32])),
                Arc::new(StringArray::from(vec!["A", "c"])),
            ],
        )
        .unwrap();

        writer
            .delete_then_append("t", vec![batch2], "id", Some(3), None)
            .await
            .unwrap();

        // Read back and verify final state
        let t = writer.open_table("t").await.unwrap();
        let ctx = deltalake::datafusion::prelude::SessionContext::new();
        let provider = t.table_provider().await.unwrap();
        ctx.register_table("t", provider).unwrap();
        let batches = ctx
            .sql("SELECT id, value FROM t ORDER BY id")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 3);

        let id_col_raw = batch.column(0);
        let value_col = batch.column(1);

        // Check id values - handle both Int32Array and Int64Array conversions
        let (id_0, id_1, id_2) = if let Some(arr) = id_col_raw.as_any().downcast_ref::<Int32Array>() {
            (arr.value(0) as i64, arr.value(1) as i64, arr.value(2) as i64)
        } else if let Some(arr) = id_col_raw.as_any().downcast_ref::<Int64Array>() {
            (arr.value(0), arr.value(1), arr.value(2))
        } else {
            panic!("Unexpected id column type");
        };
        assert_eq!(id_0, 1i64);
        assert_eq!(id_1, 2i64);
        assert_eq!(id_2, 3i64);

        // Check string values (may be StringArray or StringViewArray)
        let value_0 = if let Some(str_arr) = value_col.as_any().downcast_ref::<StringArray>() {
            str_arr.value(0).to_string()
        } else if let Some(str_view_arr) = value_col.as_any().downcast_ref::<StringViewArray>() {
            str_view_arr.value(0).to_string()
        } else {
            panic!("Unexpected value column type");
        };

        let value_1 = if let Some(str_arr) = value_col.as_any().downcast_ref::<StringArray>() {
            str_arr.value(1).to_string()
        } else if let Some(str_view_arr) = value_col.as_any().downcast_ref::<StringViewArray>() {
            str_view_arr.value(1).to_string()
        } else {
            panic!("Unexpected value column type");
        };

        let value_2 = if let Some(str_arr) = value_col.as_any().downcast_ref::<StringArray>() {
            str_arr.value(2).to_string()
        } else if let Some(str_view_arr) = value_col.as_any().downcast_ref::<StringViewArray>() {
            str_view_arr.value(2).to_string()
        } else {
            panic!("Unexpected value column type");
        };

        assert_eq!(value_0, "A");
        assert_eq!(value_1, "b");
        assert_eq!(value_2, "c");

        // Check insert hwm
        let insert_hwm = writer.read_insert_hwm("t").await.unwrap();
        assert_eq!(insert_hwm, Some(3));
    }

    #[tokio::test]
    async fn delete_then_append_upsert_uint32_key() {
        use deltalake::arrow::array::StringViewArray;
        let temp = tempfile::tempdir().unwrap();
        let writer = DeltaWriter::new_local(temp.path().to_str().unwrap());
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::UInt32, false),
            Field::new("value", DataType::Utf8, false),
        ]));

        writer
            .ensure_table("t", schema.clone())
            .await
            .unwrap();

        // First append: insert (1,"a") and (2,"b")
        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt32Array::from(vec![1u32, 2u32])),
                Arc::new(StringArray::from(vec!["a", "b"])),
            ],
        )
        .unwrap();

        writer
            .append_two_stream("t", vec![batch1], Some(2), None)
            .await
            .unwrap();

        // Second operation: delete-then-append with (1,"A") and (3,"c")
        let batch2 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt32Array::from(vec![1u32, 3u32])),
                Arc::new(StringArray::from(vec!["A", "c"])),
            ],
        )
        .unwrap();

        writer
            .delete_then_append("t", vec![batch2], "id", Some(3), None)
            .await
            .unwrap();

        // Read back and verify final state
        let t = writer.open_table("t").await.unwrap();
        let ctx = deltalake::datafusion::prelude::SessionContext::new();
        let provider = t.table_provider().await.unwrap();
        ctx.register_table("t", provider).unwrap();
        let batches = ctx
            .sql("SELECT id, value FROM t ORDER BY id")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 3);

        let id_col_raw = batch.column(0);
        let value_col = batch.column(1);

        // Check id values - handle UInt32/UInt64/Int32/Int64 array conversions
        let (id_0, id_1, id_2) = if let Some(arr) = id_col_raw.as_any().downcast_ref::<UInt32Array>() {
            (arr.value(0) as u64, arr.value(1) as u64, arr.value(2) as u64)
        } else if let Some(arr) = id_col_raw.as_any().downcast_ref::<UInt64Array>() {
            (arr.value(0), arr.value(1), arr.value(2))
        } else if let Some(arr) = id_col_raw.as_any().downcast_ref::<Int32Array>() {
            (arr.value(0) as u64, arr.value(1) as u64, arr.value(2) as u64)
        } else if let Some(arr) = id_col_raw.as_any().downcast_ref::<Int64Array>() {
            (arr.value(0) as u64, arr.value(1) as u64, arr.value(2) as u64)
        } else {
            panic!("Unexpected id column type");
        };
        assert_eq!(id_0, 1u64);
        assert_eq!(id_1, 2u64);
        assert_eq!(id_2, 3u64);

        // Check string values (may be StringArray or StringViewArray)
        let value_0 = if let Some(str_arr) = value_col.as_any().downcast_ref::<StringArray>() {
            str_arr.value(0).to_string()
        } else if let Some(str_view_arr) = value_col.as_any().downcast_ref::<StringViewArray>() {
            str_view_arr.value(0).to_string()
        } else {
            panic!("Unexpected value column type");
        };

        let value_1 = if let Some(str_arr) = value_col.as_any().downcast_ref::<StringArray>() {
            str_arr.value(1).to_string()
        } else if let Some(str_view_arr) = value_col.as_any().downcast_ref::<StringViewArray>() {
            str_view_arr.value(1).to_string()
        } else {
            panic!("Unexpected value column type");
        };

        let value_2 = if let Some(str_arr) = value_col.as_any().downcast_ref::<StringArray>() {
            str_arr.value(2).to_string()
        } else if let Some(str_view_arr) = value_col.as_any().downcast_ref::<StringViewArray>() {
            str_view_arr.value(2).to_string()
        } else {
            panic!("Unexpected value column type");
        };

        assert_eq!(value_0, "A");
        assert_eq!(value_1, "b");
        assert_eq!(value_2, "c");

        // Check insert hwm
        let insert_hwm = writer.read_insert_hwm("t").await.unwrap();
        assert_eq!(insert_hwm, Some(3));
    }

    #[tokio::test]
    async fn delete_then_append_empty_batch_noop() {
        let temp = tempfile::tempdir().unwrap();
        let writer = DeltaWriter::new_local(temp.path().to_str().unwrap());
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Utf8, false),
        ]));

        writer
            .ensure_table("t", schema.clone())
            .await
            .unwrap();

        // delete_then_append with an empty batch vector — should be a no-op
        let result = writer
            .delete_then_append("t", vec![], "id", None, None)
            .await;
        assert!(result.is_ok());

        let t = writer.open_table("t").await.unwrap();
        let ctx = deltalake::datafusion::prelude::SessionContext::new();
        let provider = t.table_provider().await.unwrap();
        ctx.register_table("t", provider).unwrap();
        let batches = ctx
            .sql("SELECT COUNT(*) as cnt FROM t")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        assert_eq!(batches.len(), 1);
        let cnt_col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<deltalake::arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(cnt_col.value(0), 0i64);
    }

    #[tokio::test]
    async fn delete_then_append_spans_multiple_delete_chunks() {
        let temp = tempfile::tempdir().unwrap();
        let writer = DeltaWriter::new_local(temp.path().to_str().unwrap());
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Utf8, false),
        ]));
        writer.ensure_table("t", schema.clone()).await.unwrap();

        // Seed 2500 rows (value="old") — spans ~3 delete chunks at 1024 keys/chunk.
        let n: i64 = 2500;
        let seed_ids: Vec<i64> = (1..=n).collect();
        let seed_vals: Vec<&str> = vec!["old"; n as usize];
        let seed = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(seed_ids.clone())),
                Arc::new(StringArray::from(seed_vals)),
            ],
        )
        .unwrap();
        let t = writer.open_table("t").await.unwrap();
        t.write(vec![seed])
            .with_save_mode(SaveMode::Append)
            .await
            .unwrap();

        // Upsert every row to value="new" via delete_then_append (crosses chunk boundaries).
        let new_vals: Vec<&str> = vec!["new"; n as usize];
        let update = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(seed_ids.clone())),
                Arc::new(StringArray::from(new_vals)),
            ],
        )
        .unwrap();
        writer
            .delete_then_append("t", vec![update], "id", Some(n), None)
            .await
            .unwrap();

        // Read back: exactly n rows, all ids distinct, and none left as "old".
        let t = writer.open_table("t").await.unwrap();
        let ctx = deltalake::datafusion::prelude::SessionContext::new();
        let provider = t.table_provider().await.unwrap();
        ctx.register_table("t", provider).unwrap();
        let batches = ctx
            .sql("SELECT COUNT(*) AS c, COUNT(DISTINCT id) AS d, SUM(CASE WHEN value = 'old' THEN 1 ELSE 0 END) AS old_cnt FROM t")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let b = &batches[0];
        let c = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap().value(0);
        let d = b.column(1).as_any().downcast_ref::<Int64Array>().unwrap().value(0);
        let old_cnt = b.column(2).as_any().downcast_ref::<Int64Array>().unwrap().value(0);
        assert_eq!(c, n, "row count after upsert should equal seed count (no dupes)");
        assert_eq!(d, n, "all ids distinct");
        assert_eq!(old_cnt, 0, "every row must be updated to the new version");
    }
}
