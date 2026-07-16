use std::time::Instant;

use anyhow::{Result, anyhow, bail};
use deltalake::arrow::array::{Array, Int16Array, Int32Array, Int64Array, Int8Array, UInt16Array, UInt32Array, UInt64Array, UInt8Array};
use deltalake::arrow::datatypes::SchemaRef;
use deltalake::arrow::record_batch::RecordBatch;
use tracing::info;

use crate::discovery::{ColumnInfo, IndexInfo, select_integer_pk};
use crate::query::QueryBuilder;

use super::Orchestrator;
use super::schema::align_batches_to_schema;
use super::{DeltaWrite, Extract, SchemaInspect, StateManage};

/// N8: string/text types whose default (often case-insensitive) collation can tie two distinct
/// values, making an all-columns ORDER BY non-total across OFFSET pages. BINARY forces byte order.
fn is_collated_string_type(data_type: &str) -> bool {
    matches!(
        data_type,
        "varchar" | "char" | "text" | "tinytext" | "mediumtext" | "longtext" | "enum" | "set"
    )
}

/// N8: a UNIQUE index over NOT-NULL columns gives a TOTAL order (no skip/dup) and lets the DB
/// order by the index (no per-page filesort). Prefer PRIMARY, then any other unique index;
/// every indexed column must be present and NOT NULL among the discovered columns (a UNIQUE
/// index permits multiple NULLs, which would tie).
fn select_unique_ordering_index(columns: &[ColumnInfo], indexes: &[IndexInfo]) -> Option<Vec<String>> {
    let all_present_not_null = |cols: &[String]| {
        !cols.is_empty()
            && cols.iter().all(|name| columns.iter().any(|c| &c.name == name && !c.nullable))
    };
    indexes
        .iter()
        .find(|i| i.name == "PRIMARY" && all_present_not_null(&i.columns))
        .or_else(|| indexes.iter().find(|i| i.unique && all_present_not_null(&i.columns)))
        .map(|i| i.columns.clone())
}

/// N8: choose the OFFSET-path ORDER BY. Unique NOT-NULL index → plain terms (total + index-usable);
/// else all columns with BINARY on string columns for a deterministic total order.
fn build_offset_order_terms(
    columns: &[String],
    source_columns: &[ColumnInfo],
    indexes: &[IndexInfo],
) -> Vec<crate::query::OrderTerm> {
    if let Some(unique_cols) = select_unique_ordering_index(source_columns, indexes) {
        unique_cols
            .into_iter()
            .map(|column| crate::query::OrderTerm { column, binary: false })
            .collect()
    } else {
        columns
            .iter()
            .map(|name| {
                let binary = source_columns
                    .iter()
                    .any(|c| &c.name == name && is_collated_string_type(&c.data_type));
                crate::query::OrderTerm { column: name.clone(), binary }
            })
            .collect()
    }
}

fn extract_batch_max_key(batch: &RecordBatch, key_col: &str) -> Result<Option<i64>> {
    let column = batch
        .column_by_name(key_col)
        .ok_or_else(|| anyhow!("full refresh keyset paging expected key column `{key_col}` in batch"))?;

    if let Some(array) = column.as_any().downcast_ref::<Int64Array>() {
        return Ok((0..array.len()).filter(|&i| !array.is_null(i)).map(|i| array.value(i)).max());
    }
    if let Some(array) = column.as_any().downcast_ref::<Int32Array>() {
        return Ok((0..array.len()).filter(|&i| !array.is_null(i)).map(|i| i64::from(array.value(i))).max());
    }
    if let Some(array) = column.as_any().downcast_ref::<Int16Array>() {
        return Ok((0..array.len()).filter(|&i| !array.is_null(i)).map(|i| i64::from(array.value(i))).max());
    }
    if let Some(array) = column.as_any().downcast_ref::<Int8Array>() {
        return Ok((0..array.len()).filter(|&i| !array.is_null(i)).map(|i| i64::from(array.value(i))).max());
    }
    if let Some(array) = column.as_any().downcast_ref::<UInt32Array>() {
        return Ok((0..array.len()).filter(|&i| !array.is_null(i)).map(|i| i64::from(array.value(i))).max());
    }
    if let Some(array) = column.as_any().downcast_ref::<UInt16Array>() {
        return Ok((0..array.len()).filter(|&i| !array.is_null(i)).map(|i| i64::from(array.value(i))).max());
    }
    if let Some(array) = column.as_any().downcast_ref::<UInt8Array>() {
        return Ok((0..array.len()).filter(|&i| !array.is_null(i)).map(|i| i64::from(array.value(i))).max());
    }
    if let Some(array) = column.as_any().downcast_ref::<UInt64Array>() {
        let mut max_value: Option<i64> = None;
        for i in 0..array.len() {
            if array.is_null(i) {
                continue;
            }
            let value = array.value(i);
            let value = i64::try_from(value).map_err(|_| {
                anyhow!(
                    "full refresh keyset paging cannot represent key `{key_col}` larger than i64"
                )
            })?;
            max_value = Some(match max_value {
                Some(current) => current.max(value),
                None => value,
            });
        }
        return Ok(max_value);
    }

    bail!("full refresh keyset paging requires integer Arrow data for key column `{key_col}`")
}

fn extract_max_key(batches: &[RecordBatch], key_col: &str) -> Result<Option<i64>> {
    let mut max_key: Option<i64> = None;
    for batch in batches {
        if let Some(batch_max) = extract_batch_max_key(batch, key_col)? {
            max_key = Some(match max_key {
                Some(current) => current.max(batch_max),
                None => batch_max,
            });
        }
    }
    Ok(max_key)
}

impl<S, E, W, M> Orchestrator<S, E, W, M>
where
    S: SchemaInspect + Send + Sync,
    E: Extract + Send,
    W: DeltaWrite + Send + Sync,
    M: StateManage + Send,
{
    pub(super) async fn process_full_refresh(
        &mut self,
        table_name: &str,
        columns: &[String],
        source_columns: &[ColumnInfo],
        indexes: &[IndexInfo],
        schema: &SchemaRef,
    ) -> Result<u64> {
        let batch_size = self.extractor.batch_size();
        let mut total_rows = 0u64;
        let mut chunk_index: u64 = 0;
        let key_col = select_integer_pk(source_columns, indexes);
        let mut last_key = None;
        let offset_order_terms = if key_col.is_none() {
            build_offset_order_terms(columns, source_columns, indexes)
        } else {
            Vec::new()
        };

        // O2-r CP2: stage every chunk's parquet without committing, then commit ONCE at the
        // end. Nothing is visible to readers (the prior snapshot stays live) until the final
        // `commit_overwrite`, so an interruption anywhere in the loop below is a clean,
        // non-destructive abandonment of the staged files rather than a partial rewrite.
        self.writer.begin_overwrite(table_name, schema.clone()).await?;
        let mut staged_chunks: u64 = 0;

        if let Some(key_col) = key_col.as_deref() {
            info!(table = table_name, key_col, "full refresh using keyset pagination");
        } else if let Some(unique_cols) = select_unique_ordering_index(source_columns, indexes) {
            info!(
                table = table_name,
                order_columns = ?unique_cols,
                "full refresh using deterministic offset pagination ordered by a unique not-null index"
            );
        } else {
            info!(
                table = table_name,
                "full refresh using deterministic offset pagination ordered by all columns (BINARY-strengthened on string columns)"
            );
        }

        loop {
            if self.check_shutdown() {
                // O2-r CP2: nothing is committed until the final `commit_overwrite` after
                // this loop, so a shutdown at ANY point here — before or after chunks were
                // staged — is safe to just break on: the staged parquet (if any) is
                // abandoned uncommitted and the previous snapshot is left fully intact. The
                // caller (process_table) will mark this table "interrupted" rather than
                // "success" once it observes the shutdown signal.
                info!(
                    table = table_name,
                    "shutdown during full refresh; not committing — the staged overwrite is \
                     abandoned and the previous snapshot is left intact"
                );
                break;
            }

            let chunk_start = Instant::now();
            // M2: `batch_size` above is captured ONCE before this loop (not re-read per
            // iteration, unlike incremental.rs), so this table's offset arithmetic is a
            // simple, fixed-stride sequence for its whole run — a mid-table circuit-breaker
            // halving of `self.extractor`'s internal batch_size (see extract() below) does
            // NOT retroactively change `batch_size` here, so `chunk_index * batch_size` stays
            // correct for this table regardless of any truncation. And `calculate_batch_size`
            // unconditionally recomputes `self.extractor`'s batch_size from scratch (avg row
            // length or the configured default) at the start of the NEXT table's
            // process_table call, so a halving from this table's breaker never leaks into
            // another table's offset math either.
            let offset = chunk_index * batch_size;
            let sql = if let Some(key_col) = key_col.as_deref() {
                QueryBuilder::build_full_refresh_query_keyset(
                    table_name,
                    columns,
                    key_col,
                    last_key,
                    batch_size,
                )
            } else {
                QueryBuilder::build_full_refresh_query_paged(
                    table_name,
                    columns,
                    &offset_order_terms,
                    batch_size,
                    offset,
                )
            };

            let extraction = self.extractor.extract(&sql)?;
            let truncated = extraction.truncated;
            let batches = extraction.batches;

            if batches.is_empty() || batches.iter().all(|b| b.num_rows() == 0) {
                break;
            }

            // N5: connector_arrow emits UInt8/16/32/64 for unsigned MariaDB columns; widen
            // those columns to match the (possibly-widened) signed Delta schema before any
            // further processing touches them (keyset max-key extraction, then the write
            // itself). All-signed batches pass through unchanged.
            let batches = align_batches_to_schema(batches, schema, table_name)?;

            // M2: the OFFSET-fallback path (no integer PK for keyset pagination) assumes
            // each chunk consumes exactly `batch_size` rows to compute the NEXT chunk's
            // offset (`chunk_index * batch_size` above). A breaker-truncated window returns
            // fewer rows than requested, which would silently skip or duplicate rows once
            // pagination continued on that assumption — so bail loudly instead. This runs
            // before writing the chunk, so an OFFSET-paged table's previous snapshot (or, for
            // chunk_index > 0, the rows already appended) is left untouched. The keyset path
            // has no such assumption (`last_key` only ever advances over rows actually
            // received), so it continues safely below instead of bailing.
            if truncated && key_col.is_none() {
                bail!(
                    "full refresh for table `{table_name}`: window exceeded the memory ceiling \
                     mid-extraction on an OFFSET-paged table; lower TARGET_MEMORY_MB or add an \
                     integer PRIMARY key so keyset pagination can resume safely"
                );
            }

            let chunk_rows: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
            let arrow_bytes: usize = batches.iter().map(|b| b.get_array_memory_size()).sum();
            let next_key = if let Some(key_col) = key_col.as_deref() {
                extract_max_key(&batches, key_col)?
            } else {
                None
            };

            self.writer
                .stage_overwrite_chunk(table_name, batches)
                .await?;
            staged_chunks += 1;

            total_rows += chunk_rows;
            chunk_index += 1;
            let chunk_elapsed = chunk_start.elapsed();

            if self.progress {
                info!(
                    table = table_name,
                    chunk_index,
                    rows = chunk_rows,
                    cumulative_rows = total_rows,
                    arrow_bytes,
                    chunk_duration_ms = chunk_elapsed.as_millis(),
                    "full refresh chunk"
                );
            } else {
                info!(
                    table = table_name,
                    rows = chunk_rows,
                    arrow_bytes,
                    "batch extracted"
                );
            }

            // M2: a truncated window means more rows remain at this `last_key` position even
            // though fewer than `batch_size` rows came back this round; only the keyset path
            // reaches here truncated (the offset path already bailed above), and keyset
            // pagination resumes safely from a partial window (see the key-advance update
            // just below, which runs for this iteration too).
            if chunk_rows < batch_size && !truncated {
                break;
            }

            if let Some(key_col) = key_col.as_deref() {
                let next_key = next_key.ok_or_else(|| {
                    anyhow!(
                        "full refresh keyset paging could not extract key `{key_col}` from a full batch for table `{table_name}`"
                    )
                })?;
                if Some(next_key) == last_key {
                    bail!(
                        "full refresh keyset paging did not advance key `{key_col}` for table `{table_name}`"
                    );
                }
                last_key = Some(next_key);
            }
        }

        // O2-r CP2: commit only on a normal finish AND only if something was actually staged.
        // An empty source must leave the table unchanged (matching the pre-CP2 behavior, where
        // chunk 0 being empty meant `overwrite_table` was never called); on shutdown, skipping
        // the commit is exactly what leaves the prior snapshot intact.
        if !self.check_shutdown() && staged_chunks > 0 {
            self.writer.commit_overwrite(table_name, None).await?;
        }

        Ok(total_rows)
    }
}

#[cfg(test)]
mod tests {
    use crate::orchestrator::*;
    use crate::orchestrator::test_support::*;
    use crate::discovery::ColumnInfo;
    use deltalake::arrow::record_batch::RecordBatch;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tempfile::TempDir;
    use tokio::sync::watch;

    #[tokio::test]
    async fn full_refresh_table_succeeds() {
        let dir = TempDir::new().unwrap();
        let config = make_config_with_full_refresh(vec!["products".to_string()]);
        let mut schema_mock = MockSchemaInspect::new();
        let mut extract_mock = MockExtract::new();
        let mut writer_mock = MockDeltaWrite::new();
        writer_mock.expect_has_data().returning(|_| Ok(false));
        let mut state_mock = MockStateManage::new();

        state_mock
            .expect_load_or_default()
            .returning(|_| crate::state::AppState::default());
        schema_mock
            .expect_discover_columns()
            .returning(move |_| Ok(make_full_refresh_columns()));
        schema_mock
            .expect_discover_indexes()
            .returning(|_| Ok(make_full_refresh_indexes()));
        schema_mock
            .expect_get_avg_row_length()
            .returning(|_| Ok(Some(100)));
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
        extract_mock
            .expect_extract()
            .returning(|_| {
                let schema = Arc::new(deltalake::arrow::datatypes::Schema::new(vec![
                    deltalake::arrow::datatypes::Field::new("id", deltalake::arrow::datatypes::DataType::Int64, false),
                    deltalake::arrow::datatypes::Field::new("name", deltalake::arrow::datatypes::DataType::Utf8, false),
                ]));
                let batch = deltalake::arrow::record_batch::RecordBatch::try_new(
                    schema,
                    vec![
                        Arc::new(deltalake::arrow::array::Int64Array::from(vec![1i64, 2i64])),
                        Arc::new(deltalake::arrow::array::StringArray::from(vec!["a", "b"])),
                    ],
                )
                .unwrap();
                ok_batches(vec![batch])
            });
        writer_mock
            .expect_begin_overwrite()
            .returning(|_, _| Ok(()));
        writer_mock
            .expect_stage_overwrite_chunk()
            .returning(|_, _| Ok(()));
        writer_mock
            .expect_commit_overwrite()
            .returning(|_, _| Ok(()));
        state_mock
            .expect_update_table()
            .withf(|_, state, _| {
                state.last_run_status.as_deref() == Some("success")
                    && state.extraction_mode.as_deref() == Some("full_refresh")
            })
            .returning(|_, _, _| Ok(()));

        let mut orch = make_orchestrator(config, schema_mock, extract_mock, writer_mock, state_mock, dir.path().to_path_buf());
        let result = orch.run().await;
        assert!(matches!(result, ExitCode::Success));
    }

    #[tokio::test]
    async fn full_refresh_multi_chunk_overwrite_then_append() {
        let dir = TempDir::new().unwrap();
        let config = make_config_with_full_refresh(vec!["products".to_string()]);
        let mut schema_mock = MockSchemaInspect::new();
        let mut extract_mock = MockExtract::new();
        let mut writer_mock = MockDeltaWrite::new();
        writer_mock.expect_has_data().returning(|_| Ok(false));
        let mut state_mock = MockStateManage::new();

        state_mock.expect_load_or_default().returning(|_| crate::state::AppState::default());
        schema_mock.expect_discover_columns().returning(move |_| Ok(make_full_refresh_columns()));
        schema_mock.expect_discover_indexes().returning(|_| Ok(make_full_refresh_indexes()));
        schema_mock.expect_get_avg_row_length().returning(|_| Ok(Some(100)));
        extract_mock.expect_calculate_batch_size().returning(|_| 2);
        extract_mock.expect_batch_size().returning(|| 2);
        writer_mock.expect_ensure_table().returning(|_, _| Ok(()));
        writer_mock.expect_get_schema().returning(|_| Ok(None));

        let call_count = Arc::new(AtomicUsize::new(0));
        let call_count_clone = call_count.clone();
        extract_mock.expect_extract().returning(move |_| {
            let count = call_count_clone.fetch_add(1, Ordering::SeqCst);
            let schema = Arc::new(deltalake::arrow::datatypes::Schema::new(vec![
                deltalake::arrow::datatypes::Field::new("id", deltalake::arrow::datatypes::DataType::Int64, false),
            ]));
            let rows: Vec<i64> = match count {
                0 => vec![1, 2],
                1 => vec![3, 4],
                2 => vec![5],
                _ => vec![],
            };
            if rows.is_empty() {
                return ok_batches(vec![]);
            }
            let batch = RecordBatch::try_new(
                schema,
                vec![Arc::new(deltalake::arrow::array::Int64Array::from(rows))],
            ).unwrap();
            ok_batches(vec![batch])
        });

        // O2-r CP2: three chunks (2, 2, 1 rows) are all staged via stage_overwrite_chunk
        // (parquet written, not committed) and the whole rewrite becomes visible with a
        // single commit_overwrite at the end.
        writer_mock.expect_begin_overwrite().times(1).returning(|_, _| Ok(()));
        writer_mock.expect_stage_overwrite_chunk().times(3).returning(|_, _| Ok(()));
        writer_mock.expect_commit_overwrite().times(1).returning(|_, _| Ok(()));
        state_mock.expect_update_table().returning(|_, _, _| Ok(()));

        let mut orch = make_orchestrator(config, schema_mock, extract_mock, writer_mock, state_mock, dir.path().to_path_buf());
        let result = orch.run().await;
        assert!(matches!(result, ExitCode::Success));
        assert_eq!(call_count.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn full_refresh_keyset_pagination_uses_last_key() {
        let dir = TempDir::new().unwrap();
        let config = make_config_with_full_refresh(vec!["products".to_string()]);
        let mut schema_mock = MockSchemaInspect::new();
        let mut extract_mock = MockExtract::new();
        let mut writer_mock = MockDeltaWrite::new();
        writer_mock.expect_has_data().returning(|_| Ok(false));
        let mut state_mock = MockStateManage::new();

        state_mock.expect_load_or_default().returning(|_| crate::state::AppState::default());
        schema_mock.expect_discover_columns().returning(move |_| Ok(make_full_refresh_columns()));
        schema_mock.expect_discover_indexes().returning(|_| Ok(make_full_refresh_primary_key("id")));
        schema_mock.expect_get_avg_row_length().returning(|_| Ok(Some(100)));
        extract_mock.expect_calculate_batch_size().returning(|_| 2);
        extract_mock.expect_batch_size().returning(|| 2);
        writer_mock.expect_ensure_table().returning(|_, _| Ok(()));
        writer_mock.expect_get_schema().returning(|_| Ok(None));

        let call_count = Arc::new(AtomicUsize::new(0));
        let call_count_clone = call_count.clone();
        extract_mock.expect_extract().returning(move |sql| {
            let count = call_count_clone.fetch_add(1, Ordering::SeqCst);
            let schema = Arc::new(deltalake::arrow::datatypes::Schema::new(vec![
                deltalake::arrow::datatypes::Field::new("id", deltalake::arrow::datatypes::DataType::Int64, false),
            ]));
            match count {
                0 => {
                    assert!(sql.contains("ORDER BY `id` ASC LIMIT 2"));
                    assert!(!sql.contains("OFFSET"));
                    let batch = RecordBatch::try_new(
                        schema,
                        vec![Arc::new(deltalake::arrow::array::Int64Array::from(vec![1, 2]))],
                    ).unwrap();
                    ok_batches(vec![batch])
                }
                1 => {
                    assert!(sql.contains("WHERE `id` > 2 ORDER BY `id` ASC LIMIT 2"));
                    let batch = RecordBatch::try_new(
                        schema,
                        vec![Arc::new(deltalake::arrow::array::Int64Array::from(vec![3]))],
                    ).unwrap();
                    ok_batches(vec![batch])
                }
                _ => ok_batches(vec![]),
            }
        });

        writer_mock.expect_begin_overwrite().times(1).returning(|_, _| Ok(()));
        writer_mock.expect_stage_overwrite_chunk().times(2).returning(|_, _| Ok(()));
        writer_mock.expect_commit_overwrite().times(1).returning(|_, _| Ok(()));
        state_mock.expect_update_table().returning(|_, _, _| Ok(()));

        let mut orch = make_orchestrator(config, schema_mock, extract_mock, writer_mock, state_mock, dir.path().to_path_buf());
        let result = orch.run().await;
        assert!(matches!(result, ExitCode::Success));
        assert_eq!(call_count.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn full_refresh_unique_index_falls_back_to_offset_pagination() {
        let dir = TempDir::new().unwrap();
        let config = make_config_with_full_refresh(vec!["products".to_string()]);
        let mut schema_mock = MockSchemaInspect::new();
        let mut extract_mock = MockExtract::new();
        let mut writer_mock = MockDeltaWrite::new();
        writer_mock.expect_has_data().returning(|_| Ok(false));
        let mut state_mock = MockStateManage::new();

        state_mock.expect_load_or_default().returning(|_| crate::state::AppState::default());
        schema_mock.expect_discover_columns().returning(move |_| Ok(make_full_refresh_columns()));
        schema_mock.expect_discover_indexes().returning(|_| Ok(make_full_refresh_unique_key("id")));
        schema_mock.expect_get_avg_row_length().returning(|_| Ok(Some(100)));
        extract_mock.expect_calculate_batch_size().returning(|_| 2);
        extract_mock.expect_batch_size().returning(|| 2);
        writer_mock.expect_ensure_table().returning(|_, _| Ok(()));
        writer_mock.expect_get_schema().returning(|_| Ok(None));

        let call_count = Arc::new(AtomicUsize::new(0));
        let call_count_clone = call_count.clone();
        extract_mock.expect_extract().returning(move |sql| {
            let count = call_count_clone.fetch_add(1, Ordering::SeqCst);
            let schema = Arc::new(deltalake::arrow::datatypes::Schema::new(vec![
                deltalake::arrow::datatypes::Field::new("id", deltalake::arrow::datatypes::DataType::Int64, false),
            ]));
            match count {
                0 => {
                    // N8: a UNIQUE index over a NOT-NULL column ("id") gives a total order on
                    // its own, so the OFFSET path orders by just that index (not all columns) —
                    // total order without a filesort over every selected column.
                    assert!(sql.contains("ORDER BY `id` LIMIT 2 OFFSET 0"));
                    assert!(!sql.contains("WHERE `id` >"));
                    let batch = RecordBatch::try_new(
                        schema,
                        vec![Arc::new(deltalake::arrow::array::Int64Array::from(vec![1, 2]))],
                    ).unwrap();
                    ok_batches(vec![batch])
                }
                1 => {
                    assert!(sql.contains("ORDER BY `id` LIMIT 2 OFFSET 2"));
                    let batch = RecordBatch::try_new(
                        schema,
                        vec![Arc::new(deltalake::arrow::array::Int64Array::from(vec![3]))],
                    ).unwrap();
                    ok_batches(vec![batch])
                }
                _ => ok_batches(vec![]),
            }
        });

        writer_mock.expect_begin_overwrite().times(1).returning(|_, _| Ok(()));
        writer_mock.expect_stage_overwrite_chunk().times(2).returning(|_, _| Ok(()));
        writer_mock.expect_commit_overwrite().times(1).returning(|_, _| Ok(()));
        state_mock.expect_update_table().returning(|_, _, _| Ok(()));

        let mut orch = make_orchestrator(config, schema_mock, extract_mock, writer_mock, state_mock, dir.path().to_path_buf());
        let result = orch.run().await;
        assert!(matches!(result, ExitCode::Success));
        assert_eq!(call_count.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn full_refresh_keyless_pagination_uses_stable_offset_order() {
        let dir = TempDir::new().unwrap();
        let config = make_config_with_full_refresh(vec!["products".to_string()]);
        let mut schema_mock = MockSchemaInspect::new();
        let mut extract_mock = MockExtract::new();
        let mut writer_mock = MockDeltaWrite::new();
        writer_mock.expect_has_data().returning(|_| Ok(false));
        let mut state_mock = MockStateManage::new();

        state_mock.expect_load_or_default().returning(|_| crate::state::AppState::default());
        schema_mock.expect_discover_columns().returning(move |_| Ok(make_full_refresh_columns()));
        schema_mock.expect_discover_indexes().returning(|_| Ok(make_full_refresh_indexes()));
        schema_mock.expect_get_avg_row_length().returning(|_| Ok(Some(100)));
        extract_mock.expect_calculate_batch_size().returning(|_| 2);
        extract_mock.expect_batch_size().returning(|| 2);
        writer_mock.expect_ensure_table().returning(|_, _| Ok(()));
        writer_mock.expect_get_schema().returning(|_| Ok(None));

        let call_count = Arc::new(AtomicUsize::new(0));
        let call_count_clone = call_count.clone();
        extract_mock.expect_extract().returning(move |sql| {
            let count = call_count_clone.fetch_add(1, Ordering::SeqCst);
            let schema = Arc::new(deltalake::arrow::datatypes::Schema::new(vec![
                deltalake::arrow::datatypes::Field::new("id", deltalake::arrow::datatypes::DataType::Int64, false),
            ]));
            match count {
                0 => {
                    // N8: no unique/PRIMARY index at all, so this falls to the all-columns
                    // fallback — `id` (bigint) orders plainly, `name` (varchar) is
                    // BINARY-strengthened so a ci-collation tie can't reorder rows across pages.
                    assert!(sql.contains("ORDER BY `id`, BINARY `name` LIMIT 2 OFFSET 0"));
                    let batch = RecordBatch::try_new(
                        schema,
                        vec![Arc::new(deltalake::arrow::array::Int64Array::from(vec![1, 2]))],
                    ).unwrap();
                    ok_batches(vec![batch])
                }
                1 => {
                    assert!(sql.contains("ORDER BY `id`, BINARY `name` LIMIT 2 OFFSET 2"));
                    let batch = RecordBatch::try_new(
                        schema,
                        vec![Arc::new(deltalake::arrow::array::Int64Array::from(vec![3]))],
                    ).unwrap();
                    ok_batches(vec![batch])
                }
                _ => ok_batches(vec![]),
            }
        });

        writer_mock.expect_begin_overwrite().times(1).returning(|_, _| Ok(()));
        writer_mock.expect_stage_overwrite_chunk().times(2).returning(|_, _| Ok(()));
        writer_mock.expect_commit_overwrite().times(1).returning(|_, _| Ok(()));
        state_mock.expect_update_table().returning(|_, _, _| Ok(()));

        let mut orch = make_orchestrator(config, schema_mock, extract_mock, writer_mock, state_mock, dir.path().to_path_buf());
        let result = orch.run().await;
        assert!(matches!(result, ExitCode::Success));
        assert_eq!(call_count.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn full_refresh_empty_table_writes_nothing() {
        let dir = TempDir::new().unwrap();
        let config = make_config_with_full_refresh(vec!["products".to_string()]);
        let mut schema_mock = MockSchemaInspect::new();
        let mut extract_mock = MockExtract::new();
        let mut writer_mock = MockDeltaWrite::new();
        writer_mock.expect_has_data().returning(|_| Ok(false));
        let mut state_mock = MockStateManage::new();

        state_mock.expect_load_or_default().returning(|_| crate::state::AppState::default());
        schema_mock.expect_discover_columns().returning(move |_| Ok(make_full_refresh_columns()));
        schema_mock.expect_discover_indexes().returning(|_| Ok(make_full_refresh_indexes()));
        schema_mock.expect_get_avg_row_length().returning(|_| Ok(Some(100)));
        extract_mock.expect_calculate_batch_size().returning(|_| 10000);
        extract_mock.expect_batch_size().returning(|| 10000);
        writer_mock.expect_ensure_table().returning(|_, _| Ok(()));
        writer_mock.expect_get_schema().returning(|_| Ok(None));
        // O2-r CP2: begin_overwrite always starts the session, but an empty source stages
        // zero chunks, so stage_overwrite_chunk/commit_overwrite must never be called —
        // no expectations registered for either means any call into them would panic,
        // proving the table is left unchanged (matching the pre-CP2 behavior where an
        // empty chunk 0 never called overwrite_table).
        writer_mock.expect_begin_overwrite().returning(|_, _| Ok(()));
        extract_mock.expect_extract().returning(|_| ok_batches(vec![]));
        state_mock.expect_update_table()
            .withf(|_, state, _| {
                state.last_run_status.as_deref() == Some("success")
                    && state.last_run_rows == Some(0)
            })
            .returning(|_, _, _| Ok(()));

        let mut orch = make_orchestrator(config, schema_mock, extract_mock, writer_mock, state_mock, dir.path().to_path_buf());
        let result = orch.run().await;
        assert!(matches!(result, ExitCode::Success));
    }

    #[tokio::test]
    async fn full_refresh_second_chunk_append_failure_propagates() {
        let dir = TempDir::new().unwrap();
        let config = make_config_with_full_refresh(vec!["products".to_string()]);
        let mut schema_mock = MockSchemaInspect::new();
        let mut extract_mock = MockExtract::new();
        let mut writer_mock = MockDeltaWrite::new();
        writer_mock.expect_has_data().returning(|_| Ok(false));
        let mut state_mock = MockStateManage::new();

        state_mock.expect_load_or_default().returning(|_| crate::state::AppState::default());
        schema_mock.expect_discover_columns().returning(move |_| Ok(make_full_refresh_columns()));
        schema_mock.expect_discover_indexes().returning(|_| Ok(make_full_refresh_indexes()));
        schema_mock.expect_get_avg_row_length().returning(|_| Ok(Some(100)));
        extract_mock.expect_calculate_batch_size().returning(|_| 1);
        extract_mock.expect_batch_size().returning(|| 1);
        writer_mock.expect_ensure_table().returning(|_, _| Ok(()));
        writer_mock.expect_get_schema().returning(|_| Ok(None));

        let call_count = Arc::new(AtomicUsize::new(0));
        let call_count_clone = call_count.clone();
        extract_mock.expect_extract().returning(move |_| {
            let count = call_count_clone.fetch_add(1, Ordering::SeqCst);
            let schema = Arc::new(deltalake::arrow::datatypes::Schema::new(vec![
                deltalake::arrow::datatypes::Field::new("id", deltalake::arrow::datatypes::DataType::Int64, false),
            ]));
            if count < 2 {
                let batch = RecordBatch::try_new(
                    schema,
                    vec![Arc::new(deltalake::arrow::array::Int64Array::from(vec![count as i64 + 1]))],
                ).unwrap();
                ok_batches(vec![batch])
            } else {
                ok_batches(vec![])
            }
        });

        // O2-r CP2: the first chunk stages fine; the second chunk's stage_overwrite_chunk
        // call fails — must propagate just like the old second-chunk append_batch failure
        // did, and commit_overwrite must never be reached.
        writer_mock.expect_begin_overwrite().times(1).returning(|_, _| Ok(()));
        let stage_count = Arc::new(AtomicUsize::new(0));
        let stage_count_clone = stage_count.clone();
        writer_mock.expect_stage_overwrite_chunk().times(2).returning(move |_, _| {
            let n = stage_count_clone.fetch_add(1, Ordering::SeqCst);
            if n == 0 {
                Ok(())
            } else {
                Err(anyhow::anyhow!("stage failed"))
            }
        });
        writer_mock.expect_commit_overwrite().times(0);
        state_mock.expect_update_table()
            .withf(|_, state, _| state.last_run_status.as_deref() == Some("failed"))
            .returning(|_, _, _| Ok(()));

        let mut orch = make_orchestrator(config, schema_mock, extract_mock, writer_mock, state_mock, dir.path().to_path_buf());
        let result = orch.run().await;
        assert!(matches!(result, ExitCode::Fatal));
    }

    #[tokio::test]
    async fn rejects_hwm_config_on_full_refresh_table() {
        let dir = TempDir::new().unwrap();
        let mut config = make_config_with_full_refresh(vec!["products".to_string()]);
        config.table_initial_hwm.insert(
            "products".to_string(),
            ("2026-05-01T00:00:00.000000".to_string(), 999),
        );
        let mut schema_mock = MockSchemaInspect::new();
        let extract_mock = MockExtract::new();
        let mut writer_mock = MockDeltaWrite::new();
        writer_mock.expect_has_data().returning(|_| Ok(false));
        let mut state_mock = MockStateManage::new();

        state_mock
            .expect_load_or_default()
            .returning(|_| crate::state::AppState::default());
        // N3-r: process_table now discovers indexes up front (before mode is resolved), so
        // even this early-bailing full_refresh test reaches the discover_indexes call. Inert
        // here — the table is forced full_refresh and bails on the TABLE_HWM check regardless.
        schema_mock
            .expect_discover_indexes()
            .returning(|_| Ok(vec![]));
        schema_mock
            .expect_discover_columns()
            .returning(move |_| {
                Ok(vec![
                    ColumnInfo {
                        name: "id".to_string(),
                        data_type: "bigint".to_string(),
                        column_type: "bigint(20)".to_string(),
                        nullable: false,
                    },
                    ColumnInfo {
                        name: "name".to_string(),
                        data_type: "varchar".to_string(),
                        column_type: "varchar(255)".to_string(),
                        nullable: false,
                    },
                ])
            });
        schema_mock
            .expect_get_avg_row_length()
            .returning(|_| Ok(Some(100)));
        writer_mock
            .expect_ensure_table()
            .returning(|_, _| Ok(()));
        state_mock
            .expect_update_table()
            .withf(|_, state, _| state.last_run_status.as_deref() == Some("failed"))
            .returning(|_, _, _| Ok(()));

        let mut orch = make_orchestrator(config, schema_mock, extract_mock, writer_mock, state_mock, dir.path().to_path_buf());
        let result = orch.run().await;
        assert!(matches!(result, ExitCode::Fatal));
    }

    #[tokio::test]
    async fn full_refresh_shutdown_after_first_chunk_is_non_destructive() {
        // O2-r CP2: this used to bail with a "partially rewritten" error, because chunk 0
        // committed an OVERWRITE (destroying the previous snapshot) before the shutdown was
        // observed on the next loop iteration. That's gone now — chunk 0 only STAGES parquet
        // (stage_overwrite_chunk), nothing is committed until the final commit_overwrite, so
        // a shutdown after the first staged chunk is a clean, non-destructive abandonment:
        // the staged parquet is orphaned and the previous snapshot stays fully intact.
        // Drive this through `process_table` (not `process_full_refresh` directly) so the
        // real interrupted-status handling is exercised — mirrors
        // `full_refresh_process_table_marks_interrupted_when_shutdown_before_any_chunk` in
        // orchestrator.rs, just with a chunk actually staged before the signal fires.
        let dir = TempDir::new().unwrap();
        let config = make_config_with_full_refresh(vec!["products".to_string()]);
        let mut schema_mock = MockSchemaInspect::new();
        let mut extract_mock = MockExtract::new();
        let mut writer_mock = MockDeltaWrite::new();
        writer_mock.expect_has_data().returning(|_| Ok(false));
        let mut state_mock = MockStateManage::new();
        let (tx, rx) = watch::channel(false);

        schema_mock.expect_discover_columns().returning(move |_| Ok(make_full_refresh_columns()));
        schema_mock.expect_discover_indexes().returning(|_| Ok(make_full_refresh_indexes()));
        schema_mock.expect_get_avg_row_length().returning(|_| Ok(Some(100)));
        extract_mock.expect_calculate_batch_size().returning(|_| 2);
        extract_mock.expect_batch_size().returning(|| 2);
        writer_mock.expect_ensure_table().returning(|_, _| Ok(()));
        writer_mock.expect_get_schema().returning(|_| Ok(None));

        let call_count = Arc::new(AtomicUsize::new(0));
        let call_count_clone = call_count.clone();
        let tx_clone = tx.clone();
        extract_mock.expect_extract().returning(move |_| {
            let count = call_count_clone.fetch_add(1, Ordering::SeqCst);
            let schema = Arc::new(deltalake::arrow::datatypes::Schema::new(vec![
                deltalake::arrow::datatypes::Field::new("id", deltalake::arrow::datatypes::DataType::Int64, false),
            ]));
            if count == 0 {
                // Full chunk (2 rows == batch_size 2): staged (parquet written, NOT
                // committed), then the signal fires before the next iteration.
                let _ = tx_clone.send(true);
                let batch = RecordBatch::try_new(
                    schema,
                    vec![Arc::new(deltalake::arrow::array::Int64Array::from(vec![1i64, 2i64]))],
                ).unwrap();
                ok_batches(vec![batch])
            } else {
                ok_batches(vec![])
            }
        });

        writer_mock.expect_begin_overwrite().times(1).returning(|_, _| Ok(()));
        writer_mock.expect_stage_overwrite_chunk().times(1).returning(|_, _| Ok(()));
        // The whole point of O2-r: the previous snapshot must stay intact, so the final
        // commit must NEVER be reached when the run is interrupted mid-refresh.
        writer_mock.expect_commit_overwrite().times(0);

        state_mock
            .expect_update_table()
            .withf(|name, state, _| {
                name == "products"
                    && state.last_run_status.as_deref() == Some("interrupted")
                    && state.last_run_rows == Some(2)
            })
            .times(1)
            .returning(|_, _, _| Ok(()));

        let mut orch = Orchestrator::new(
            config,
            schema_mock,
            extract_mock,
            writer_mock,
            state_mock,
            rx,
            dir.path().to_path_buf(),
            false,
        );

        orch.process_table("products")
            .await
            .expect("process_table should return Ok even though the table was interrupted — the staged overwrite is simply abandoned, not a failure");
        assert_eq!(call_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn full_refresh_shutdown_before_any_chunk_breaks_cleanly() {
        // O2/R4 nuance: if the shutdown arrives before any chunk was extracted or
        // written, nothing was destroyed yet, so this must NOT bail — it should break
        // cleanly and let the caller (process_table) mark the table "interrupted".
        // extract()/stage_overwrite_chunk()/commit_overwrite() have no expectations
        // registered, so any call into them would panic — proving nothing was extracted
        // or staged. begin_overwrite IS called unconditionally right before the loop
        // (O2-r CP2), so it alone is mocked here.
        let dir = TempDir::new().unwrap();
        let config = make_config_with_full_refresh(vec!["products".to_string()]);
        let schema_mock = MockSchemaInspect::new();
        let mut extract_mock = MockExtract::new();
        let mut writer_mock = MockDeltaWrite::new();
        let state_mock = MockStateManage::new();
        let (tx, rx) = watch::channel(false);
        tx.send(true).unwrap();

        extract_mock.expect_batch_size().returning(|| 2);
        writer_mock.expect_begin_overwrite().returning(|_, _| Ok(()));

        let mut orch = Orchestrator::new(
            config,
            schema_mock,
            extract_mock,
            writer_mock,
            state_mock,
            rx,
            dir.path().to_path_buf(),
            false,
        );

        let columns = make_full_refresh_columns();
        let select_columns: Vec<String> = columns.iter().map(|c| c.name.clone()).collect();
        let indexes = make_full_refresh_indexes();
        let result = orch
            .process_full_refresh("products", &select_columns, &columns, &indexes, &schema_from_columns(&columns))
            .await;

        let rows = result.expect("shutdown before any chunk must not bail — nothing was destroyed yet");
        assert_eq!(rows, 0);
    }

    #[tokio::test]
    async fn full_refresh_offset_path_truncated_window_bails_with_actionable_message() {
        // M2: the OFFSET-fallback path (no integer PK) assumes each chunk consumes exactly
        // `batch_size` rows to compute the next chunk's offset; a breaker-truncated window
        // breaks that assumption, so it must bail with an actionable message instead of
        // silently corrupting pagination — and must bail BEFORE writing the truncated chunk
        // (stage_overwrite_chunk/commit_overwrite have no expectations registered, so either
        // being called would panic, proving nothing was written; begin_overwrite alone runs
        // unconditionally before the loop, so it alone is mocked).
        let dir = TempDir::new().unwrap();
        let config = make_config_with_full_refresh(vec!["products".to_string()]);
        let schema_mock = MockSchemaInspect::new();
        let mut extract_mock = MockExtract::new();
        let mut writer_mock = MockDeltaWrite::new();
        let state_mock = MockStateManage::new();
        let (_tx, rx) = watch::channel(false);

        extract_mock.expect_batch_size().returning(|| 5);
        writer_mock.expect_begin_overwrite().returning(|_, _| Ok(()));
        extract_mock.expect_extract().returning(|_| {
            let schema = Arc::new(deltalake::arrow::datatypes::Schema::new(vec![
                deltalake::arrow::datatypes::Field::new("id", deltalake::arrow::datatypes::DataType::Int64, false),
            ]));
            let batch = RecordBatch::try_new(
                schema,
                vec![Arc::new(deltalake::arrow::array::Int64Array::from(vec![1i64, 2i64]))],
            )
            .unwrap();
            Ok(crate::extractor::Extraction { batches: vec![batch], truncated: true })
        });

        let mut orch = Orchestrator::new(
            config,
            schema_mock,
            extract_mock,
            writer_mock,
            state_mock,
            rx,
            dir.path().to_path_buf(),
            false,
        );

        let columns = make_full_refresh_columns();
        let select_columns: Vec<String> = columns.iter().map(|c| c.name.clone()).collect();
        let indexes = make_full_refresh_indexes(); // no PRIMARY key => OFFSET-fallback path
        let result = orch
            .process_full_refresh("products", &select_columns, &columns, &indexes, &schema_from_columns(&columns))
            .await;

        let err = result.expect_err("a truncated window on the OFFSET-fallback path must bail");
        let msg = err.to_string();
        assert!(
            msg.contains("OFFSET-paged table"),
            "expected the actionable offset-path message, got: {msg}"
        );
        assert!(
            msg.contains("PRIMARY key"),
            "expected the actionable offset-path message, got: {msg}"
        );
    }

    #[tokio::test]
    async fn full_refresh_keyset_path_truncated_window_continues_pagination() {
        // M2: keyset pagination is cursor-based (`last_key` only ever advances over rows
        // actually received), so a truncated window is safe to resume from — the loop must
        // NOT treat a truncated partial chunk as end-of-data, and must still advance
        // `last_key` so the following request resumes from the right place.
        let dir = TempDir::new().unwrap();
        let config = make_config_with_full_refresh(vec!["products".to_string()]);
        let schema_mock = MockSchemaInspect::new();
        let mut extract_mock = MockExtract::new();
        let mut writer_mock = MockDeltaWrite::new();
        writer_mock.expect_has_data().returning(|_| Ok(false));
        let state_mock = MockStateManage::new();
        let (_tx, rx) = watch::channel(false);

        extract_mock.expect_batch_size().returning(|| 5);

        let call_count = Arc::new(AtomicUsize::new(0));
        let call_count_clone = call_count.clone();
        extract_mock.expect_extract().returning(move |sql| {
            let count = call_count_clone.fetch_add(1, Ordering::SeqCst);
            let schema = Arc::new(deltalake::arrow::datatypes::Schema::new(vec![
                deltalake::arrow::datatypes::Field::new("id", deltalake::arrow::datatypes::DataType::Int64, false),
            ]));
            match count {
                0 => {
                    assert!(!sql.contains("WHERE"), "first keyset chunk should not filter on id yet, got: {sql}");
                    // Truncated: only 2 of the requested 5 rows came back.
                    let batch = RecordBatch::try_new(
                        schema,
                        vec![Arc::new(deltalake::arrow::array::Int64Array::from(vec![1, 2]))],
                    )
                    .unwrap();
                    Ok(crate::extractor::Extraction { batches: vec![batch], truncated: true })
                }
                1 => {
                    assert!(
                        sql.contains("WHERE `id` > 2"),
                        "second chunk must resume from the truncated window's last key, got: {sql}"
                    );
                    let batch = RecordBatch::try_new(
                        schema,
                        vec![Arc::new(deltalake::arrow::array::Int64Array::from(vec![3]))],
                    )
                    .unwrap();
                    Ok(crate::extractor::Extraction { batches: vec![batch], truncated: false })
                }
                _ => panic!("should not be called a third time: the second window was final"),
            }
        });

        writer_mock.expect_begin_overwrite().times(1).returning(|_, _| Ok(()));
        writer_mock.expect_stage_overwrite_chunk().times(2).returning(|_, _| Ok(()));
        writer_mock.expect_commit_overwrite().times(1).returning(|_, _| Ok(()));

        let mut orch = Orchestrator::new(
            config,
            schema_mock,
            extract_mock,
            writer_mock,
            state_mock,
            rx,
            dir.path().to_path_buf(),
            false,
        );

        let columns = make_full_refresh_columns();
        let select_columns: Vec<String> = columns.iter().map(|c| c.name.clone()).collect();
        let indexes = make_full_refresh_primary_key("id");
        let result = orch
            .process_full_refresh("products", &select_columns, &columns, &indexes, &schema_from_columns(&columns))
            .await;

        let rows = result.expect("a truncated keyset window must not fail the table");
        assert_eq!(rows, 3);
        assert_eq!(call_count.load(Ordering::SeqCst), 2);
    }

    // N8: OFFSET-path ordering helpers.
    use super::{build_offset_order_terms, select_unique_ordering_index};
    use crate::discovery::IndexInfo;

    #[test]
    fn select_unique_ordering_index_prefers_primary_not_null() {
        let columns = vec![
            ColumnInfo {
                name: "id".to_string(),
                data_type: "bigint".to_string(),
                column_type: "bigint(20)".to_string(),
                nullable: false,
            },
            ColumnInfo {
                name: "name".to_string(),
                data_type: "varchar".to_string(),
                column_type: "varchar(255)".to_string(),
                nullable: true,
            },
        ];
        let indexes = vec![
            IndexInfo { name: "PRIMARY".to_string(), unique: true, columns: vec!["id".to_string()] },
            IndexInfo { name: "name_uniq".to_string(), unique: true, columns: vec!["name".to_string()] },
        ];

        let result = select_unique_ordering_index(&columns, &indexes);
        assert_eq!(result, Some(vec!["id".to_string()]));
    }

    #[test]
    fn select_unique_ordering_index_skips_nullable_unique() {
        let columns = vec![
            ColumnInfo {
                name: "id".to_string(),
                data_type: "bigint".to_string(),
                column_type: "bigint(20)".to_string(),
                nullable: false,
            },
            ColumnInfo {
                name: "email".to_string(),
                data_type: "varchar".to_string(),
                column_type: "varchar(255)".to_string(),
                nullable: true,
            },
        ];
        // Only candidate is a UNIQUE index on the nullable `email` column — a UNIQUE index
        // permits multiple NULL rows, which would tie, so it must be rejected.
        let indexes = vec![IndexInfo {
            name: "email_uniq".to_string(),
            unique: true,
            columns: vec!["email".to_string()],
        }];

        let result = select_unique_ordering_index(&columns, &indexes);
        assert_eq!(result, None);
    }

    #[test]
    fn select_unique_ordering_index_uses_non_primary_unique() {
        let columns = vec![
            ColumnInfo {
                name: "id".to_string(),
                data_type: "bigint".to_string(),
                column_type: "bigint(20)".to_string(),
                nullable: false,
            },
            ColumnInfo {
                name: "sku".to_string(),
                data_type: "varchar".to_string(),
                column_type: "varchar(64)".to_string(),
                nullable: false,
            },
        ];
        // No PRIMARY key at all, but a not-null UNIQUE index on `sku`.
        let indexes = vec![IndexInfo {
            name: "sku_uniq".to_string(),
            unique: true,
            columns: vec!["sku".to_string()],
        }];

        let result = select_unique_ordering_index(&columns, &indexes);
        assert_eq!(result, Some(vec!["sku".to_string()]));
    }

    #[test]
    fn build_offset_order_terms_binary_on_strings() {
        let source_columns = vec![
            ColumnInfo {
                name: "qty".to_string(),
                data_type: "int".to_string(),
                column_type: "int(11)".to_string(),
                nullable: false,
            },
            ColumnInfo {
                name: "label".to_string(),
                data_type: "varchar".to_string(),
                column_type: "varchar(255)".to_string(),
                nullable: false,
            },
        ];
        // No unique/PRIMARY index — falls to the all-columns fallback.
        let indexes: Vec<IndexInfo> = vec![];
        let columns = vec!["qty".to_string(), "label".to_string()];

        let terms = build_offset_order_terms(&columns, &source_columns, &indexes);

        assert_eq!(terms.len(), 2);
        assert_eq!(terms[0].column, "qty");
        assert!(!terms[0].binary, "int column must order plainly");
        assert_eq!(terms[1].column, "label");
        assert!(terms[1].binary, "varchar column must be BINARY-strengthened");
    }
}
