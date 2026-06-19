use std::time::Instant;

use anyhow::Result;
use tracing::info;

use crate::query::QueryBuilder;
use crate::writer::{extract_hwm_from_batch, extract_max_id, Hwm};

use super::Orchestrator;
use super::{DeltaWrite, Extract, SchemaInspect, StateManage};

impl<S, E, W, M> Orchestrator<S, E, W, M>
where
    S: SchemaInspect + Send + Sync,
    E: Extract + Send,
    W: DeltaWrite + Send + Sync,
    M: StateManage + Send,
{
    pub(super) async fn process_two_stream(
        &mut self,
        table_name: &str,
        columns: &[String],
        insert_col: &str,
        update_col: &str,
    ) -> Result<u64> {
        let mut hwm_id = self.writer.read_insert_hwm(table_name).await?;
        let mut update_hwm = self.writer.read_hwm(table_name).await?;
        let mut total_rows = 0u64;

        // Bootstrap seeding: on first run (no stored update HWM), seed from the current
        // MAX(update_col) so the update stream only catches completions after the bootstrap
        // (the insert stream already loaded every row's current state). Avoids the redundant
        // — and previously crashing — full re-merge of existing completions.
        let seed = if update_hwm.is_none() {
            self.schema_inspect.max_timestamp(table_name, update_col).await?
        } else {
            None
        };
        if let Some(seed) = seed {
            if self.progress {
                info!(table = table_name, seed = %seed, "two-stream: seeding update watermark (skip already-loaded completions)");
            }
            update_hwm = Some(Hwm { updated_at: seed, last_id: i64::MAX });
        }

        // ---- Stream A: new rows by insert cursor (append) ----
        loop {
            if self.check_shutdown() { break; }
            let batch_size = self.extractor.batch_size();
            if self.progress { info!(table = table_name, after_id = ?hwm_id, "two-stream insert: fetching chunk"); }
            let t_extract = Instant::now();
            let sql = QueryBuilder::build_insert_stream_query(table_name, columns, insert_col, hwm_id, batch_size);
            let batches = self.extractor.extract(&sql)?;
            let extract_ms = t_extract.elapsed().as_millis();
            if batches.is_empty() || batches.iter().all(|b| b.num_rows() == 0) { break; }
            let chunk_rows: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
            let arrow_bytes: usize = batches.iter().map(|b| b.get_array_memory_size()).sum();
            let new_max = batches.iter().filter_map(|b| extract_max_id(b, insert_col)).max();
            if self.progress { info!(table = table_name, rows = chunk_rows, extract_ms, "two-stream insert: extracted, appending"); }
            let t_write = Instant::now();
            self.writer.append_two_stream(table_name, batches, new_max.or(hwm_id), update_hwm.clone()).await?;
            let write_ms = t_write.elapsed().as_millis();
            if let Some(m) = new_max { hwm_id = Some(hwm_id.map_or(m, |c| c.max(m))); }
            total_rows += chunk_rows;
            if self.progress {
                let cumulative_rows = total_rows;
                info!(
                    table = table_name,
                    rows = chunk_rows,
                    cumulative_rows,
                    extract_ms,
                    write_ms,
                    "two-stream insert: appended"
                );
            } else {
                info!(
                    table = table_name,
                    rows = chunk_rows,
                    arrow_bytes,
                    "batch extracted"
                );
            }
            if chunk_rows < batch_size { break; }
        }

        // ---- Stream B: completions by update cursor (merge key = insert_col) ----
        loop {
            if self.check_shutdown() { break; }
            let batch_size = self.extractor.batch_size();
            if self.progress { info!(table = table_name, after_hwm = ?update_hwm, "two-stream update: fetching chunk"); }
            let t_extract = Instant::now();
            let sql = QueryBuilder::build_incremental_query(
                table_name, columns, update_col,
                update_hwm.as_ref().map(|h| h.updated_at.as_str()),
                update_hwm.as_ref().map(|h| h.last_id),
                batch_size,
            );
            let batches = self.extractor.extract(&sql)?;
            let extract_ms = t_extract.elapsed().as_millis();
            if batches.is_empty() || batches.iter().all(|b| b.num_rows() == 0) { break; }
            let chunk_rows: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
            let arrow_bytes: usize = batches.iter().map(|b| b.get_array_memory_size()).sum();
            let new_hwm = batches.last().and_then(|b| extract_hwm_from_batch(b, update_col));
            if self.progress { info!(table = table_name, rows = chunk_rows, extract_ms, "two-stream update: extracted, merging"); }
            let t_write = Instant::now();
            if std::env::var("UPDATE_STRATEGY").as_deref() == Ok("merge") {
                if self.progress {
                    info!(table = table_name, "two-stream update: strategy=merge (MERGE upsert, opt-out)");
                }
                self.writer
                    .merge_batch(table_name, batches, insert_col.to_string(), hwm_id, new_hwm.clone())
                    .await?;
            } else {
                if self.progress {
                    info!(table = table_name, "two-stream update: strategy=delete_append (default, DELETE+APPEND)");
                }
                self.writer
                    .delete_then_append(table_name, batches, insert_col.to_string(), hwm_id, new_hwm.clone())
                    .await?;
            }
            let write_ms = t_write.elapsed().as_millis();
            if let Some(h) = new_hwm { update_hwm = Some(h); }
            total_rows += chunk_rows;
            if self.progress {
                let cumulative_rows = total_rows;
                info!(
                    table = table_name,
                    rows = chunk_rows,
                    cumulative_rows,
                    extract_ms,
                    write_ms,
                    "two-stream update: merged"
                );
            } else {
                info!(
                    table = table_name,
                    rows = chunk_rows,
                    arrow_bytes,
                    "batch extracted"
                );
            }
            if chunk_rows < batch_size { break; }
        }

        Ok(total_rows)
    }
}

#[cfg(test)]
mod tests {
    use crate::orchestrator::*;
    use crate::orchestrator::test_support::*;
    
    use deltalake::arrow::record_batch::RecordBatch;
    use serial_test::serial;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tempfile::TempDir;

    #[tokio::test]
    #[serial]
    async fn two_stream_insert_stream_merges_new_rows_then_stops() {
        let dir = TempDir::new().unwrap();
        let mut config = make_config(vec!["orders".to_string()]);
        config.table_insert_cursor.insert("orders".to_string(), "id".to_string());
        config.table_update_cursor.insert("orders".to_string(), "updated_at".to_string());

        let mut schema_mock = MockSchemaInspect::new();
        let mut extract_mock = MockExtract::new();
        let mut writer_mock = MockDeltaWrite::new();
        let mut state_mock = MockStateManage::new();

        state_mock
            .expect_load_or_default()
            .returning(|_| crate::state::AppState::default());
        schema_mock
            .expect_discover_columns()
            .returning(move |_| Ok(make_columns()));
        schema_mock
            .expect_get_avg_row_length()
            .returning(|_| Ok(Some(100)));
        schema_mock
            .expect_max_timestamp()
            .returning(|_, _| Ok(None));
        extract_mock
            .expect_calculate_batch_size()
            .returning(|_| 10000);
        extract_mock
            .expect_batch_size()
            .returning(|| 10000);
        writer_mock
            .expect_ensure_table()
            .returning(|_, _| Ok(()));
        writer_mock
            .expect_get_schema()
            .returning(|_| Ok(None));
        writer_mock
            .expect_read_insert_hwm()
            .returning(|_| Ok(None));
        writer_mock
            .expect_read_hwm()
            .returning(|_| Ok(None));

        let call_count = Arc::new(AtomicUsize::new(0));
        let call_count_clone = call_count.clone();
        extract_mock
            .expect_extract()
            .returning(move |_| {
                let count = call_count_clone.fetch_add(1, Ordering::SeqCst);
                if count == 0 {
                    let schema = Arc::new(deltalake::arrow::datatypes::Schema::new(vec![
                        deltalake::arrow::datatypes::Field::new("id", deltalake::arrow::datatypes::DataType::Int64, false),
                        deltalake::arrow::datatypes::Field::new("name", deltalake::arrow::datatypes::DataType::Utf8, false),
                        deltalake::arrow::datatypes::Field::new(
                            "updated_at",
                            deltalake::arrow::datatypes::DataType::Timestamp(deltalake::arrow::datatypes::TimeUnit::Microsecond, None),
                            false,
                        ),
                    ]));
                    let batch = RecordBatch::try_new(
                        schema,
                        vec![
                            Arc::new(deltalake::arrow::array::Int64Array::from(vec![1i64, 2i64])),
                            Arc::new(deltalake::arrow::array::StringArray::from(vec!["a", "b"])),
                            Arc::new(deltalake::arrow::array::TimestampMicrosecondArray::from(vec![1743158400000000i64, 1743158400000000i64])),
                        ],
                    )
                    .unwrap();
                    Ok(vec![batch])
                } else {
                    Ok(vec![])
                }
            });

        writer_mock
            .expect_append_two_stream()
            .returning(|_, _, _, _| Ok(()));
        state_mock
            .expect_update_table()
            .returning(|_, _, _| Ok(()));

        let mut orch = make_orchestrator(config, schema_mock, extract_mock, writer_mock, state_mock, dir.path().to_path_buf());
        let result = orch.run().await;
        assert!(matches!(result, ExitCode::Success));
    }

    #[tokio::test]
    async fn two_stream_only_insert_cursor_fails() {
        let dir = TempDir::new().unwrap();
        let mut config = make_config(vec!["orders".to_string()]);
        config.table_insert_cursor.insert("orders".to_string(), "id".to_string());

        let mut schema_mock = MockSchemaInspect::new();
        let extract_mock = MockExtract::new();
        let writer_mock = MockDeltaWrite::new();
        let mut state_mock = MockStateManage::new();

        state_mock
            .expect_load_or_default()
            .returning(|_| crate::state::AppState::default());
        schema_mock
            .expect_discover_columns()
            .returning(move |_| Ok(make_columns()));
        state_mock
            .expect_update_table()
            .returning(|_, _, _| Ok(()));

        let mut orch = make_orchestrator(config, schema_mock, extract_mock, writer_mock, state_mock, dir.path().to_path_buf());
        let result = orch.run().await;
        assert!(matches!(result, ExitCode::Fatal));
    }

    #[tokio::test]
    async fn two_stream_only_update_cursor_fails() {
        let dir = TempDir::new().unwrap();
        let mut config = make_config(vec!["orders".to_string()]);
        config.table_update_cursor.insert("orders".to_string(), "updated_at".to_string());

        let mut schema_mock = MockSchemaInspect::new();
        let extract_mock = MockExtract::new();
        let writer_mock = MockDeltaWrite::new();
        let mut state_mock = MockStateManage::new();

        state_mock
            .expect_load_or_default()
            .returning(|_| crate::state::AppState::default());
        schema_mock
            .expect_discover_columns()
            .returning(move |_| Ok(make_columns()));
        state_mock
            .expect_update_table()
            .returning(|_, _, _| Ok(()));

        let mut orch = make_orchestrator(config, schema_mock, extract_mock, writer_mock, state_mock, dir.path().to_path_buf());
        let result = orch.run().await;
        assert!(matches!(result, ExitCode::Fatal));
    }

    #[tokio::test]
    #[serial]
    async fn two_stream_update_default_delete_append() {
        let dir = TempDir::new().unwrap();
        let mut config = make_config(vec!["orders".to_string()]);
        config.table_insert_cursor.insert("orders".to_string(), "id".to_string());
        config.table_update_cursor.insert("orders".to_string(), "updated_at".to_string());

        let mut schema_mock = MockSchemaInspect::new();
        let mut extract_mock = MockExtract::new();
        let mut writer_mock = MockDeltaWrite::new();
        let mut state_mock = MockStateManage::new();

        state_mock
            .expect_load_or_default()
            .returning(|_| crate::state::AppState::default());
        schema_mock
            .expect_discover_columns()
            .returning(move |_| Ok(make_columns()));
        schema_mock
            .expect_get_avg_row_length()
            .returning(|_| Ok(Some(100)));
        schema_mock
            .expect_max_timestamp()
            .returning(|_, _| Ok(None));
        extract_mock
            .expect_calculate_batch_size()
            .returning(|_| 10000);
        extract_mock
            .expect_batch_size()
            .returning(|| 10000);
        writer_mock
            .expect_ensure_table()
            .returning(|_, _| Ok(()));
        writer_mock
            .expect_get_schema()
            .returning(|_| Ok(None));
        writer_mock
            .expect_read_insert_hwm()
            .returning(|_| Ok(Some(100)));
        writer_mock
            .expect_read_hwm()
            .returning(|_| Ok(Some(crate::writer::Hwm {
                updated_at: "2026-06-01T00:00:00.000000".to_string(),
                last_id: 50,
            })));

        let call_count = Arc::new(AtomicUsize::new(0));
        let call_count_clone = call_count.clone();
        extract_mock
            .expect_extract()
            .returning(move |_| {
                let count = call_count_clone.fetch_add(1, Ordering::SeqCst);
                if count == 0 {
                    Ok(vec![])
                } else if count == 1 {
                    let schema = Arc::new(deltalake::arrow::datatypes::Schema::new(vec![
                        deltalake::arrow::datatypes::Field::new("id", deltalake::arrow::datatypes::DataType::Int64, false),
                        deltalake::arrow::datatypes::Field::new("name", deltalake::arrow::datatypes::DataType::Utf8, false),
                        deltalake::arrow::datatypes::Field::new(
                            "updated_at",
                            deltalake::arrow::datatypes::DataType::Timestamp(deltalake::arrow::datatypes::TimeUnit::Microsecond, None),
                            false,
                        ),
                    ]));
                    let batch = RecordBatch::try_new(
                        schema,
                        vec![
                            Arc::new(deltalake::arrow::array::Int64Array::from(vec![50i64, 51i64])),
                            Arc::new(deltalake::arrow::array::StringArray::from(vec!["x", "y"])),
                            Arc::new(deltalake::arrow::array::TimestampMicrosecondArray::from(vec![1743158400000000i64, 1743158401000000i64])),
                        ],
                    )
                    .unwrap();
                    Ok(vec![batch])
                } else {
                    Ok(vec![])
                }
            });

        writer_mock
            .expect_append_two_stream()
            .times(0)
            .returning(|_, _, _, _| Ok(()));
        writer_mock
            .expect_delete_then_append()
            .returning(|_, _, _, _, _| Ok(()));
        state_mock
            .expect_update_table()
            .returning(|_, _, _| Ok(()));

        let mut orch = make_orchestrator(config, schema_mock, extract_mock, writer_mock, state_mock, dir.path().to_path_buf());
        let result = orch.run().await;
        assert!(matches!(result, ExitCode::Success));
    }

    #[tokio::test]
    #[serial]
    async fn two_stream_seeds_update_hwm_when_none_stored() {
        let dir = TempDir::new().unwrap();
        let mut config = make_config(vec!["orders".to_string()]);
        config.table_insert_cursor.insert("orders".to_string(), "id".to_string());
        config.table_update_cursor.insert("orders".to_string(), "updated_at".to_string());

        let mut schema_mock = MockSchemaInspect::new();
        let mut extract_mock = MockExtract::new();
        let mut writer_mock = MockDeltaWrite::new();
        let mut state_mock = MockStateManage::new();

        state_mock
            .expect_load_or_default()
            .returning(|_| crate::state::AppState::default());
        schema_mock
            .expect_discover_columns()
            .returning(move |_| Ok(make_columns()));
        schema_mock
            .expect_get_avg_row_length()
            .returning(|_| Ok(Some(100)));
        schema_mock
            .expect_max_timestamp()
            .returning(|_, _| Ok(Some("2026-06-01T00:00:00.000000".to_string())));
        extract_mock
            .expect_calculate_batch_size()
            .returning(|_| 10000);
        extract_mock
            .expect_batch_size()
            .returning(|| 10000);
        writer_mock
            .expect_ensure_table()
            .returning(|_, _| Ok(()));
        writer_mock
            .expect_get_schema()
            .returning(|_| Ok(None));
        writer_mock
            .expect_read_insert_hwm()
            .returning(|_| Ok(None));
        writer_mock
            .expect_read_hwm()
            .returning(|_| Ok(None));

        let call_count = Arc::new(AtomicUsize::new(0));
        let call_count_clone = call_count.clone();
        extract_mock
            .expect_extract()
            .returning(move |sql| {
                let count = call_count_clone.fetch_add(1, Ordering::SeqCst);
                if count == 0 {
                    Ok(vec![])
                } else if count == 1 {
                    assert!(sql.contains("2026-06-01T00:00:00.000000"), "seed should be in update stream SQL");
                    Ok(vec![])
                } else {
                    Ok(vec![])
                }
            });

        writer_mock
            .expect_append_two_stream()
            .times(0)
            .returning(|_, _, _, _| Ok(()));
        writer_mock
            .expect_merge_batch()
            .times(0)
            .returning(|_, _, _, _, _| Ok(()));
        state_mock
            .expect_update_table()
            .returning(|_, _, _| Ok(()));

        let mut orch = make_orchestrator(config, schema_mock, extract_mock, writer_mock, state_mock, dir.path().to_path_buf());
        let result = orch.run().await;
        assert!(matches!(result, ExitCode::Success));
    }

    #[tokio::test]
    #[serial]
    async fn two_stream_update_merge_optout() {
        unsafe { std::env::set_var("UPDATE_STRATEGY", "merge"); }

        let dir = TempDir::new().unwrap();
        let mut config = make_config(vec!["orders".to_string()]);
        config.table_insert_cursor.insert("orders".to_string(), "id".to_string());
        config.table_update_cursor.insert("orders".to_string(), "updated_at".to_string());

        let mut schema_mock = MockSchemaInspect::new();
        let mut extract_mock = MockExtract::new();
        let mut writer_mock = MockDeltaWrite::new();
        let mut state_mock = MockStateManage::new();

        state_mock
            .expect_load_or_default()
            .returning(|_| crate::state::AppState::default());
        schema_mock
            .expect_discover_columns()
            .returning(move |_| Ok(make_columns()));
        schema_mock
            .expect_get_avg_row_length()
            .returning(|_| Ok(Some(100)));
        schema_mock
            .expect_max_timestamp()
            .returning(|_, _| Ok(None));
        extract_mock
            .expect_calculate_batch_size()
            .returning(|_| 10000);
        extract_mock
            .expect_batch_size()
            .returning(|| 10000);
        writer_mock
            .expect_ensure_table()
            .returning(|_, _| Ok(()));
        writer_mock
            .expect_get_schema()
            .returning(|_| Ok(None));
        writer_mock
            .expect_read_insert_hwm()
            .returning(|_| Ok(Some(100)));
        writer_mock
            .expect_read_hwm()
            .returning(|_| Ok(Some(crate::writer::Hwm {
                updated_at: "2026-06-01T00:00:00.000000".to_string(),
                last_id: 50,
            })));

        let call_count = Arc::new(AtomicUsize::new(0));
        let call_count_clone = call_count.clone();
        extract_mock
            .expect_extract()
            .returning(move |_| {
                let count = call_count_clone.fetch_add(1, Ordering::SeqCst);
                if count == 0 {
                    Ok(vec![])
                } else if count == 1 {
                    let schema = Arc::new(deltalake::arrow::datatypes::Schema::new(vec![
                        deltalake::arrow::datatypes::Field::new("id", deltalake::arrow::datatypes::DataType::Int64, false),
                        deltalake::arrow::datatypes::Field::new("name", deltalake::arrow::datatypes::DataType::Utf8, false),
                        deltalake::arrow::datatypes::Field::new(
                            "updated_at",
                            deltalake::arrow::datatypes::DataType::Timestamp(deltalake::arrow::datatypes::TimeUnit::Microsecond, None),
                            false,
                        ),
                    ]));
                    let batch = RecordBatch::try_new(
                        schema,
                        vec![
                            Arc::new(deltalake::arrow::array::Int64Array::from(vec![50i64, 51i64])),
                            Arc::new(deltalake::arrow::array::StringArray::from(vec!["x", "y"])),
                            Arc::new(deltalake::arrow::array::TimestampMicrosecondArray::from(vec![1743158400000000i64, 1743158401000000i64])),
                        ],
                    )
                    .unwrap();
                    Ok(vec![batch])
                } else {
                    Ok(vec![])
                }
            });

        writer_mock
            .expect_append_two_stream()
            .times(0)
            .returning(|_, _, _, _| Ok(()));
        writer_mock
            .expect_merge_batch()
            .returning(|_, _, _, _, _| Ok(()));
        state_mock
            .expect_update_table()
            .returning(|_, _, _| Ok(()));

        let mut orch = make_orchestrator(config, schema_mock, extract_mock, writer_mock, state_mock, dir.path().to_path_buf());
        let result = orch.run().await;

        unsafe { std::env::remove_var("UPDATE_STRATEGY"); }

        assert!(matches!(result, ExitCode::Success));
    }
}
