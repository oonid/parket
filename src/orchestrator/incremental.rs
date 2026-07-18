use std::time::Instant;

use anyhow::{Result, bail};
use deltalake::arrow::datatypes::SchemaRef;
use tracing::{info, warn};

use crate::query::QueryBuilder;
use crate::writer::{extract_hwm_from_batch, hwm_has_advanced};

use super::Orchestrator;
use super::schema::align_batches_to_schema;
use super::{DeltaWrite, Extract, SchemaInspect, StateManage};

impl<S, E, W, M> Orchestrator<S, E, W, M>
where
    S: SchemaInspect + Send + Sync,
    E: Extract + Send,
    W: DeltaWrite + Send + Sync,
    M: StateManage + Send,
{
    pub(super) async fn process_incremental(
        &mut self,
        table_name: &str,
        columns: &[String],
        ts_col: &str,
        key_col: &str,
        schema: &SchemaRef,
        cursor_nullable: bool,
    ) -> Result<u64> {
        // D2: an explicitly-configured nullable incremental cursor still drops NULL-cursor rows
        // (the incremental query filters `WHERE <ts> IS NOT NULL`; O3 only stopped auto-selecting
        // nullable cursors). Make that loss OBSERVABLE — once per run, count the excluded rows and
        // warn loudly. A NOT NULL cursor can't have any, so the COUNT(*) is skipped in that case.
        if cursor_nullable {
            let null_rows = self.schema_inspect.count_null(table_name, ts_col).await?;
            if null_rows > 0 {
                warn!(
                    table = table_name,
                    cursor = ts_col,
                    null_rows,
                    "incremental cursor `{ts_col}` on table `{table_name}` is nullable and {null_rows} \
                     row(s) have a NULL `{ts_col}` — those rows are EXCLUDED from incremental \
                     extraction (the cursor query filters `{ts_col} IS NOT NULL`) and will never \
                     sync; run this table as full_refresh if they must be captured (see audit D2/O3)"
                );
            }
        }

        let mut current_hwm = match self.writer.read_hwm(table_name).await? {
            Some(h) => Some(h),
            None => self.config.table_initial_hwm.get(table_name).map(|(ua, id)| {
                info!(
                    table = table_name,
                    hwm_updated_at = %ua,
                    hwm_last_id = id,
                    "seeding HWM from config (no stored HWM)"
                );
                crate::writer::Hwm { updated_at: ua.clone(), last_id: *id }
            }),
        };
        // H-2026-07-11-1: no watermark resolved (nothing stored, no config seed) but the
        // Delta table already holds data — re-extracting from scratch with APPEND would
        // duplicate every row (e.g. after a full-refresh rebuild wiped HWM visibility, or
        // a no-HWM commit shadowed it). Refuse loudly instead.
        if current_hwm.is_none() && self.writer.has_data(table_name).await? {
            anyhow::bail!(
                "table `{table_name}` already has data in Delta but no stored HWM — refusing to \
                 re-extract from scratch with append (every row would be duplicated). Set \
                 TABLE_HWM_{table_name} to the snapshot's max cursor, or full-refresh the table"
            );
        }

        let mut total_rows = 0u64;
        let mut batch_index: u64 = 0;

        loop {
            if self.check_shutdown() {
                info!(
                    table = table_name,
                    "shutdown signal received during batch loop, finishing table"
                );
                break;
            }

            let batch_size = self.extractor.batch_size();
            let sql = QueryBuilder::build_incremental_query(
                table_name,
                columns,
                ts_col,
                key_col,
                current_hwm.as_ref().map(|h| h.updated_at.as_str()),
                current_hwm.as_ref().map(|h| h.last_id),
                batch_size,
                None,
            );

            let extraction = self.extractor.extract(&sql)?;
            let truncated = extraction.truncated;
            let batches = extraction.batches;
            if batches.is_empty()
                || batches.iter().all(|b| b.num_rows() == 0)
            {
                break;
            }

            // N5: widen any UInt8/16/32/64 columns (unsigned MariaDB types) to match the
            // signed Delta schema before HWM extraction and the write below.
            let batches = align_batches_to_schema(batches, schema, table_name)?;

            let batch_rows: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
            let arrow_bytes: usize = batches.iter().map(|b| b.get_array_memory_size()).sum();
            let batch_start = Instant::now();

            let batch_hwm = batches
                .last()
                .and_then(|b| extract_hwm_from_batch(b, ts_col, key_col));

            // N2-r: a non-empty batch we are about to APPEND but from which no HWM can be
            // extracted — cursor `{ts_col}`/`{key_col}` present by name but of an
            // unextractable Arrow type (or a BIGINT UNSIGNED key past i64::MAX) — would
            // leave the stored watermark unadvanced, re-extracting and duplicating these
            // rows on every subsequent run. The incremental query filters `{ts_col} IS NOT
            // NULL`, so a non-empty batch here always has a non-NULL cursor; None means an
            // unextractable type. Unsafe on ANY chunk (full or terminal-partial) → bail.
            if batch_hwm.is_none() {
                bail!(
                    "incremental batch for table `{table_name}` could not extract a HWM from a \
                     non-empty batch — cursor `{ts_col}`/`{key_col}` is present but not an \
                     extractable type (or the key overflowed i64); appending would duplicate \
                     rows on the next run. Fix the cursor column type or run this table as \
                     full_refresh"
                );
            }
            // N2/R2: a FULL batch whose HWM did not advance means keyset pagination is stuck —
            // bail before appending. A terminal partial chunk may legitimately sit at the
            // boundary, so this stays gated to full batches.
            if batch_rows == batch_size
                && batch_hwm
                    .as_ref()
                    .is_some_and(|next_hwm| !hwm_has_advanced(current_hwm.as_ref(), next_hwm))
            {
                bail!(
                    "incremental batch for table `{table_name}` did not advance HWM on a full batch"
                );
            }
            // M2-r2: a breaker-TRUNCATED window (fewer rows than `batch_size`, so the guard
            // above never fires) whose HWM ALSO failed to advance is not a legitimate final
            // partial chunk — it's pagination stuck at the same cursor position (needs an
            // unextractable-but-non-NULL cursor value, e.g. an unsupported type or a BIGINT
            // UNSIGNED id past i64::MAX, since the query already filters `{ts_col} IS NOT
            // NULL`). Left unguarded, `batch_rows < batch_size && !truncated` below never
            // breaks the loop (truncated is true), so it would re-extract and re-append this
            // exact non-advancing window forever. Mirrors the full-refresh keyset paging
            // "did not advance" bail.
            if truncated
                && batch_hwm
                    .as_ref()
                    .is_some_and(|next_hwm| !hwm_has_advanced(current_hwm.as_ref(), next_hwm))
            {
                bail!(
                    "incremental batch for table `{table_name}` was truncated by the memory \
                     breaker but did not advance HWM — pagination is stuck at the same cursor \
                     position and would loop forever; fix the cursor column type or lower \
                     TARGET_MEMORY_MB"
                );
            }

            self.writer
                .append_batch(table_name, batches, batch_hwm.clone())
                .await?;

            if let Some(h) = batch_hwm {
                current_hwm = Some(h);
            }
            total_rows += batch_rows;
            batch_index += 1;

            let batch_elapsed = batch_start.elapsed();

            if self.progress {
                let cumulative_rows = total_rows;
                info!(
                    table = table_name,
                    batch_index,
                    rows = batch_rows,
                    cumulative_rows,
                    arrow_bytes,
                    batch_duration_ms = batch_elapsed.as_millis(),
                    "batch progress"
                );
            } else {
                info!(
                    table = table_name,
                    rows = batch_rows,
                    arrow_bytes,
                    "batch extracted"
                );
            }

            // M2: a truncated window (the mid-stream memory circuit breaker cut it short)
            // means more rows remain in MariaDB for this same cursor position even though
            // fewer than `batch_size` rows came back — cursor-based pagination handles this
            // safely (the cursor only advanced over rows actually received), so do NOT treat
            // a truncated partial window as "end of data".
            if batch_rows < batch_size && !truncated {
                break;
            }
        }

        Ok(total_rows)
    }
}

#[cfg(test)]
mod tests {
    use crate::orchestrator::*;
    use crate::orchestrator::test_support::*;
    use deltalake::arrow::record_batch::RecordBatch;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tempfile::TempDir;

    #[tokio::test]
    async fn incremental_hwm_updates_between_batches() {
        let dir = TempDir::new().unwrap();
        let config = make_config(vec!["orders".to_string()]);
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
            .returning(move |_| Ok(make_columns()));
        schema_mock
            .expect_discover_indexes()
            .returning(|_| Ok(make_full_refresh_primary_key("id")));
        schema_mock
            .expect_get_avg_row_length()
            .returning(|_| Ok(Some(100)));
        extract_mock
            .expect_calculate_batch_size()
            .returning(|_| 10000);
        writer_mock
            .expect_ensure_table()
            .returning(|_, _| Ok(()));
        writer_mock
            .expect_get_schema()
            .returning(|_| Ok(None));
        writer_mock
            .expect_read_hwm()
            .returning(|_| Ok(None));
        extract_mock
            .expect_batch_size()
            .returning(|| 1);

        let call_count = Arc::new(AtomicUsize::new(0));
        let call_count_clone = call_count.clone();
        extract_mock
            .expect_extract()
            .returning(move |_| {
                let count = call_count_clone.fetch_add(1, Ordering::SeqCst);
                if count == 0 {
                    let schema = Arc::new(deltalake::arrow::datatypes::Schema::new(vec![
                        deltalake::arrow::datatypes::Field::new("id", deltalake::arrow::datatypes::DataType::Int64, false),
                        deltalake::arrow::datatypes::Field::new(
                            "updated_at",
                            deltalake::arrow::datatypes::DataType::Timestamp(deltalake::arrow::datatypes::TimeUnit::Microsecond, None),
                            false,
                        ),
                    ]));
                    let batch = RecordBatch::try_new(
                        schema,
                        vec![
                            Arc::new(deltalake::arrow::array::Int64Array::from(vec![1i64])),
                            Arc::new(deltalake::arrow::array::TimestampMicrosecondArray::from(vec![1743158400000000i64])),
                        ],
                    )
                    .unwrap();
                    ok_batches(vec![batch])
                } else if count == 1 {
                    let schema = Arc::new(deltalake::arrow::datatypes::Schema::new(vec![
                        deltalake::arrow::datatypes::Field::new("id", deltalake::arrow::datatypes::DataType::Int64, false),
                        deltalake::arrow::datatypes::Field::new(
                            "updated_at",
                            deltalake::arrow::datatypes::DataType::Timestamp(deltalake::arrow::datatypes::TimeUnit::Microsecond, None),
                            false,
                        ),
                    ]));
                    let batch = RecordBatch::try_new(
                        schema,
                        vec![
                            Arc::new(deltalake::arrow::array::Int64Array::from(vec![2i64])),
                            Arc::new(deltalake::arrow::array::TimestampMicrosecondArray::from(vec![1743158401000000i64])),
                        ],
                    )
                    .unwrap();
                    ok_batches(vec![batch])
                } else {
                    ok_batches(vec![])
                }
            });

        writer_mock
            .expect_append_batch()
            .returning(|_, _, _| Ok(()));
        state_mock
            .expect_update_table()
            .returning(|_, _, _| Ok(()));

        let mut orch = make_orchestrator(config, schema_mock, extract_mock, writer_mock, state_mock, dir.path().to_path_buf());
        let result = orch.run().await;
        assert!(matches!(result, ExitCode::Success));
        assert_eq!(call_count.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn incremental_truncated_window_continues_loop_and_advances_hwm() {
        // M2: a breaker-truncated window (rows < batch_size but truncated=true) is a
        // cursor-based table's safe-to-resume case, not end-of-data — the loop must keep
        // going, appending the truncated window's rows and advancing the HWM over them,
        // then pick up the remainder in the next (smaller) request.
        let dir = TempDir::new().unwrap();
        let config = make_config(vec!["orders".to_string()]);
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
            .returning(move |_| Ok(make_columns()));
        schema_mock
            .expect_discover_indexes()
            .returning(|_| Ok(make_full_refresh_primary_key("id")));
        schema_mock
            .expect_get_avg_row_length()
            .returning(|_| Ok(Some(100)));
        extract_mock
            .expect_calculate_batch_size()
            .returning(|_| 10000);
        writer_mock
            .expect_ensure_table()
            .returning(|_, _| Ok(()));
        writer_mock
            .expect_get_schema()
            .returning(|_| Ok(None));
        writer_mock
            .expect_read_hwm()
            .returning(|_| Ok(None));
        extract_mock
            .expect_batch_size()
            .returning(|| 5);

        let call_count = Arc::new(AtomicUsize::new(0));
        let call_count_clone = call_count.clone();
        extract_mock
            .expect_extract()
            .returning(move |_| {
                let count = call_count_clone.fetch_add(1, Ordering::SeqCst);
                let schema = Arc::new(deltalake::arrow::datatypes::Schema::new(vec![
                    deltalake::arrow::datatypes::Field::new("id", deltalake::arrow::datatypes::DataType::Int64, false),
                    deltalake::arrow::datatypes::Field::new(
                        "updated_at",
                        deltalake::arrow::datatypes::DataType::Timestamp(deltalake::arrow::datatypes::TimeUnit::Microsecond, None),
                        false,
                    ),
                ]));
                if count == 0 {
                    // Truncated window: the circuit breaker cut it short at 2 of the
                    // requested 5 rows. Must NOT be treated as end-of-data.
                    let batch = RecordBatch::try_new(
                        schema,
                        vec![
                            Arc::new(deltalake::arrow::array::Int64Array::from(vec![1i64, 2i64])),
                            Arc::new(deltalake::arrow::array::TimestampMicrosecondArray::from(vec![
                                1743158400000000i64,
                                1743158401000000i64,
                            ])),
                        ],
                    )
                    .unwrap();
                    Ok(crate::extractor::Extraction { batches: vec![batch], truncated: true })
                } else if count == 1 {
                    // A genuinely final, non-truncated partial window: this really is the
                    // end of data, and the HWM must reflect both windows by now.
                    let batch = RecordBatch::try_new(
                        schema,
                        vec![
                            Arc::new(deltalake::arrow::array::Int64Array::from(vec![3i64])),
                            Arc::new(deltalake::arrow::array::TimestampMicrosecondArray::from(vec![1743158402000000i64])),
                        ],
                    )
                    .unwrap();
                    Ok(crate::extractor::Extraction { batches: vec![batch], truncated: false })
                } else {
                    panic!(
                        "extract should not be called a third time: the second window was \
                         not truncated and returned fewer rows than batch_size"
                    );
                }
            });

        writer_mock
            .expect_append_batch()
            .withf(|_, batches, _| !batches.is_empty() && !batches.iter().all(|b| b.num_rows() == 0))
            .times(2)
            .returning(|_, _, _| Ok(()));
        state_mock
            .expect_update_table()
            .withf(|_, state, _| {
                state.last_run_status.as_deref() == Some("success") && state.last_run_rows == Some(3)
            })
            .returning(|_, _, _| Ok(()));

        let mut orch = make_orchestrator(config, schema_mock, extract_mock, writer_mock, state_mock, dir.path().to_path_buf());
        let result = orch.run().await;
        assert!(matches!(result, ExitCode::Success));
        assert_eq!(
            call_count.load(Ordering::SeqCst),
            2,
            "loop must continue past the truncated window and stop after the genuinely-final one (both windows written, HWM advanced across both)"
        );
    }

    #[tokio::test]
    async fn incremental_truncated_window_not_advancing_hwm_bails() {
        // M2-r2: a breaker-truncated window (fewer rows than batch_size, so the
        // `batch_rows == batch_size` full-batch guard never fires) whose extracted HWM does
        // NOT advance past the current watermark must bail actionably — left unguarded, the
        // loop's `batch_rows < batch_size && !truncated` break never fires either (truncated is
        // true), so it would re-extract and re-append this exact non-advancing window forever.
        let dir = TempDir::new().unwrap();
        let mut config = make_config(vec!["orders".to_string()]);
        // Seed an initial HWM so `current_hwm` is `Some` from the very first iteration (a `None`
        // baseline always counts as "advanced", so the non-advance case needs a starting point).
        config.table_initial_hwm.insert(
            "orders".to_string(),
            ("2026-03-28 10:00:00".to_string(), 5),
        );
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
            .returning(move |_| Ok(make_columns()));
        schema_mock
            .expect_discover_indexes()
            .returning(|_| Ok(make_full_refresh_primary_key("id")));
        schema_mock
            .expect_get_avg_row_length()
            .returning(|_| Ok(Some(100)));
        extract_mock
            .expect_calculate_batch_size()
            .returning(|_| 10000);
        writer_mock
            .expect_ensure_table()
            .returning(|_, _| Ok(()));
        writer_mock
            .expect_get_schema()
            .returning(|_| Ok(None));
        writer_mock
            .expect_read_hwm()
            .returning(|_| Ok(None));
        extract_mock
            .expect_batch_size()
            .returning(|| 5);

        // A single truncated call: 2 of the requested 5 rows came back, both sitting AT the
        // seeded watermark (ts exactly equal, id <= the seeded last_id) — zero forward progress.
        // 2026-03-28 10:00:00 UTC == 1774692000000000 micros.
        extract_mock.expect_extract().times(1).returning(|_| {
            let schema = Arc::new(deltalake::arrow::datatypes::Schema::new(vec![
                deltalake::arrow::datatypes::Field::new("id", deltalake::arrow::datatypes::DataType::Int64, false),
                deltalake::arrow::datatypes::Field::new(
                    "updated_at",
                    deltalake::arrow::datatypes::DataType::Timestamp(deltalake::arrow::datatypes::TimeUnit::Microsecond, None),
                    false,
                ),
            ]));
            let batch = RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(deltalake::arrow::array::Int64Array::from(vec![4i64, 5i64])),
                    Arc::new(deltalake::arrow::array::TimestampMicrosecondArray::from(vec![
                        1774692000000000i64,
                        1774692000000000i64,
                    ])),
                ],
            )
            .unwrap();
            Ok(crate::extractor::Extraction { batches: vec![batch], truncated: true })
        });

        // No expectation for append_batch: if the new guard didn't fire, this would be called
        // and panic — proving the bail happens BEFORE any append.
        state_mock
            .expect_update_table()
            .withf(|_, state, _| state.last_run_status.as_deref() == Some("failed"))
            .returning(|_, _, _| Ok(()));

        let mut orch = make_orchestrator(config, schema_mock, extract_mock, writer_mock, state_mock, dir.path().to_path_buf());
        let result = orch.run().await;
        assert!(matches!(result, ExitCode::Fatal));
    }

    #[tokio::test]
    async fn incremental_full_batch_without_hwm_fails_before_append() {
        let dir = TempDir::new().unwrap();
        let config = make_config(vec!["orders".to_string()]);
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
            .returning(move |_| Ok(make_columns()));
        schema_mock
            .expect_discover_indexes()
            .returning(|_| Ok(make_full_refresh_primary_key("id")));
        schema_mock
            .expect_get_avg_row_length()
            .returning(|_| Ok(Some(100)));
        extract_mock
            .expect_calculate_batch_size()
            .returning(|_| 10000);
        writer_mock
            .expect_ensure_table()
            .returning(|_, _| Ok(()));
        writer_mock
            .expect_get_schema()
            .returning(|_| Ok(None));
        writer_mock
            .expect_read_hwm()
            .returning(|_| Ok(None));
        extract_mock
            .expect_batch_size()
            .returning(|| 1);
        extract_mock
            .expect_extract()
            .returning(|_| {
                let schema = Arc::new(deltalake::arrow::datatypes::Schema::new(vec![
                    deltalake::arrow::datatypes::Field::new(
                        "updated_at",
                        deltalake::arrow::datatypes::DataType::Timestamp(
                            deltalake::arrow::datatypes::TimeUnit::Microsecond,
                            None,
                        ),
                        false,
                    ),
                ]));
                let batch = RecordBatch::try_new(
                    schema,
                    vec![Arc::new(
                        deltalake::arrow::array::TimestampMicrosecondArray::from(vec![1743158400000000i64]),
                    )],
                )
                .unwrap();
                ok_batches(vec![batch])
            });
        writer_mock
            .expect_append_batch()
            .times(0)
            .returning(|_, _, _| Ok(()));
        state_mock
            .expect_update_table()
            .withf(|_, state, _| state.last_run_status.as_deref() == Some("failed"))
            .returning(|_, _, _| Ok(()));

        let mut orch = make_orchestrator(config, schema_mock, extract_mock, writer_mock, state_mock, dir.path().to_path_buf());
        let result = orch.run().await;
        assert!(matches!(result, ExitCode::Fatal));
    }

    #[tokio::test]
    async fn incremental_partial_batch_without_hwm_bails_before_append() {
        // N2-r: a terminal PARTIAL batch (rows < batch_size) whose cursor column is present
        // by name but of an unextractable Arrow type (here: Boolean — not one of the types
        // `extract_timestamp_as_strings`/`extract_hwm_from_batch` understand) must NOT be
        // silently appended. Previously the HWM-extraction guard only fired when
        // `batch_rows == batch_size`, so this terminal-partial case slipped through and
        // would append the batch without advancing the stored watermark, re-extracting and
        // duplicating these rows on every subsequent run. Drives `process_incremental`
        // directly (bypassing `run()`/`process_table` dispatch) so the returned `Err` message
        // can be inspected.
        let dir = TempDir::new().unwrap();
        let config = make_config(vec!["orders".to_string()]);
        let schema_mock = MockSchemaInspect::new();
        let mut extract_mock = MockExtract::new();
        let mut writer_mock = MockDeltaWrite::new();
        writer_mock.expect_has_data().returning(|_| Ok(false));
        writer_mock.expect_read_hwm().returning(|_| Ok(None));
        writer_mock
            .expect_append_batch()
            .times(0)
            .returning(|_, _, _| Ok(()));

        // batch_size = 10 but the extractor returns a single 1-row batch: rows < batch_size,
        // i.e. a terminal partial chunk — the case the old `batch_rows == batch_size` guard
        // missed.
        extract_mock.expect_batch_size().returning(|| 10);
        extract_mock.expect_extract().returning(|_| {
            let schema = Arc::new(deltalake::arrow::datatypes::Schema::new(vec![
                deltalake::arrow::datatypes::Field::new("id", deltalake::arrow::datatypes::DataType::Int64, false),
                // Boolean: present by name, but not an extractable timestamp/cursor type.
                deltalake::arrow::datatypes::Field::new(
                    "updated_at",
                    deltalake::arrow::datatypes::DataType::Boolean,
                    false,
                ),
            ]));
            let batch = RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(deltalake::arrow::array::Int64Array::from(vec![1i64])),
                    Arc::new(deltalake::arrow::array::BooleanArray::from(vec![true])),
                ],
            )
            .unwrap();
            ok_batches(vec![batch])
        });

        let columns = vec!["id".to_string(), "name".to_string(), "updated_at".to_string()];
        let schema = schema_from_columns(&make_columns());

        let mut orch = make_orchestrator(
            config,
            schema_mock,
            extract_mock,
            writer_mock,
            MockStateManage::new(),
            dir.path().to_path_buf(),
        );
        let result = orch
            .process_incremental("orders", &columns, "updated_at", "id", &schema, false)
            .await;
        let err = result.expect_err("must bail on a non-empty partial batch with an unextractable HWM");
        assert!(
            err.to_string().contains("could not extract a HWM from a non-empty batch"),
            "unexpected error message: {err}"
        );
    }

    #[tokio::test]
    async fn incremental_seeds_hwm_from_config_when_none_stored() {
        let dir = TempDir::new().unwrap();
        let mut config = make_config(vec!["orders".to_string()]);
        config.table_initial_hwm.insert(
            "orders".to_string(),
            ("2026-05-01T00:00:00.000000".to_string(), 999),
        );
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
            .returning(move |_| Ok(make_columns()));
        schema_mock
            .expect_discover_indexes()
            .returning(|_| Ok(make_full_refresh_primary_key("id")));
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
        writer_mock
            .expect_read_hwm()
            .returning(|_| Ok(None));
        extract_mock
            .expect_extract()
            .withf(|sql| sql.contains("2026-05-01T00:00:00.000000") && sql.contains("999"))
            .returning(|_| ok_batches(vec![]));
        writer_mock
            .expect_append_batch()
            .returning(|_, _, _| Ok(()));
        state_mock
            .expect_update_table()
            .returning(|_, _, _| Ok(()));

        let mut orch = make_orchestrator(config, schema_mock, extract_mock, writer_mock, state_mock, dir.path().to_path_buf());
        let result = orch.run().await;
        assert!(matches!(result, ExitCode::Success));
    }

    #[tokio::test]
    async fn incremental_ignores_config_hwm_when_stored_present() {
        let dir = TempDir::new().unwrap();
        let mut config = make_config(vec!["orders".to_string()]);
        config.table_initial_hwm.insert(
            "orders".to_string(),
            ("2026-05-01T00:00:00.000000".to_string(), 999),
        );
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
            .returning(move |_| Ok(make_columns()));
        schema_mock
            .expect_discover_indexes()
            .returning(|_| Ok(make_full_refresh_primary_key("id")));
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
        writer_mock
            .expect_read_hwm()
            .returning(|_| {
                Ok(Some(crate::writer::Hwm {
                    updated_at: "2026-09-09T00:00:00.000000".to_string(),
                    last_id: 5000,
                }))
            });
        extract_mock
            .expect_extract()
            .withf(|sql| sql.contains("2026-09-09T00:00:00.000000") && !sql.contains("2026-05-01"))
            .returning(|_| ok_batches(vec![]));
        writer_mock
            .expect_append_batch()
            .returning(|_, _, _| Ok(()));
        state_mock
            .expect_update_table()
            .returning(|_, _, _| Ok(()));

        let mut orch = make_orchestrator(config, schema_mock, extract_mock, writer_mock, state_mock, dir.path().to_path_buf());
        let result = orch.run().await;
        assert!(matches!(result, ExitCode::Success));
    }

    #[tokio::test]
    async fn custom_timestamp_column_incremental() {
        let dir = TempDir::new().unwrap();
        let mut config = make_config(vec!["orders".to_string()]);
        config.table_timestamp_col.insert("orders".to_string(), "completed_at".to_string());
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
            .returning(move |_| {
                Ok(vec![
                    crate::discovery::ColumnInfo {
                        name: "id".to_string(),
                        data_type: "bigint".to_string(),
                        column_type: "bigint(20)".to_string(),
                        nullable: false,
                    },
                    crate::discovery::ColumnInfo {
                        name: "name".to_string(),
                        data_type: "varchar".to_string(),
                        column_type: "varchar(255)".to_string(),
                        nullable: false,
                    },
                    crate::discovery::ColumnInfo {
                        name: "completed_at".to_string(),
                        data_type: "timestamp".to_string(),
                        column_type: "timestamp".to_string(),
                        nullable: false,
                    },
                ])
            });
        schema_mock
            .expect_discover_indexes()
            .returning(|_| Ok(make_full_refresh_primary_key("id")));
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
        writer_mock
            .expect_read_hwm()
            .returning(|_| Ok(None));
        extract_mock
            .expect_extract()
            .withf(|sql| sql.contains("completed_at"))
            .returning(|_| ok_batches(vec![]));
        state_mock
            .expect_update_table()
            .returning(|_, _, _| Ok(()));

        let mut orch = make_orchestrator(config, schema_mock, extract_mock, writer_mock, state_mock, dir.path().to_path_buf());
        let result = orch.run().await;
        assert!(matches!(result, ExitCode::Success));
    }

    #[tokio::test]
    async fn incremental_uses_discovered_primary_key_not_hardcoded_id() {
        // N3: the tiebreak/ORDER BY key column must come from the discovered PRIMARY
        // key (`order_id`), not a hardcoded "id".
        let dir = TempDir::new().unwrap();
        let mut config = make_config(vec!["orders".to_string()]);
        // detect_mode's auto-detection still keys off a literal "id" column (unrelated
        // to N3 — out of scope for this fix); force Incremental via override so this
        // test can isolate the key-column threading through the query builder / HWM
        // extraction instead.
        config.table_modes.insert("orders".to_string(), crate::config::ExtractionMode::Incremental);
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
            .returning(move |_| {
                Ok(vec![
                    crate::discovery::ColumnInfo {
                        name: "order_id".to_string(),
                        data_type: "bigint".to_string(),
                        column_type: "bigint(20)".to_string(),
                        nullable: false,
                    },
                    crate::discovery::ColumnInfo {
                        name: "name".to_string(),
                        data_type: "varchar".to_string(),
                        column_type: "varchar(255)".to_string(),
                        nullable: false,
                    },
                    crate::discovery::ColumnInfo {
                        name: "updated_at".to_string(),
                        data_type: "timestamp".to_string(),
                        column_type: "timestamp".to_string(),
                        nullable: false,
                    },
                ])
            });
        schema_mock
            .expect_discover_indexes()
            .returning(|_| Ok(make_full_refresh_primary_key("order_id")));
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
        writer_mock
            .expect_read_hwm()
            .returning(|_| Ok(None));
        extract_mock
            .expect_extract()
            .withf(|sql| {
                sql.contains("`order_id`")
                    && sql.contains("ORDER BY `updated_at` ASC, `order_id` ASC")
                    && !sql.contains("`id`")
            })
            .returning(|_| ok_batches(vec![]));
        state_mock
            .expect_update_table()
            .returning(|_, _, _| Ok(()));

        let mut orch = make_orchestrator(config, schema_mock, extract_mock, writer_mock, state_mock, dir.path().to_path_buf());
        let result = orch.run().await;
        assert!(matches!(result, ExitCode::Success));
    }

    #[tokio::test]
    async fn incremental_additively_includes_cursor_column_missing_from_delta() {
        // D1 (reverses the old N3 fail-fast): when the Delta schema predates an EXTRACTABLE
        // cursor column, additive schema evolution now INCLUDES it in the SELECT (rather than
        // dropping it and failing). The run proceeds — the extracted SQL carries `updated_at`,
        // and the append writes it with SchemaMode::Merge so Delta gains the column. (The N3
        // fail-fast guard remains for a cursor that is genuinely un-selectable, e.g. a default
        // cursor name absent from the source columns entirely.)
        let dir = TempDir::new().unwrap();
        let config = make_config(vec!["orders".to_string()]);
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
            .returning(move |_| Ok(make_columns()));
        schema_mock
            .expect_discover_indexes()
            .returning(|_| Ok(make_full_refresh_primary_key("id")));
        schema_mock
            .expect_get_avg_row_length()
            .returning(|_| Ok(Some(100)));
        extract_mock
            .expect_calculate_batch_size()
            .returning(|_| 10000);
        extract_mock.expect_batch_size().returning(|| 10000);
        // The cursor column absent from Delta is now selected (D1): the query must carry it.
        extract_mock
            .expect_extract()
            .times(1)
            .withf(|sql: &str| sql.contains("updated_at") && sql.contains("id") && sql.contains("name"))
            .returning(|_| ok_batches(vec![]));
        writer_mock
            .expect_ensure_table()
            .returning(|_, _| Ok(()));
        writer_mock
            .expect_get_schema()
            .returning(|_| {
                Ok(Some(Arc::new(deltalake::arrow::datatypes::Schema::new(vec![
                    deltalake::arrow::datatypes::Field::new("id", deltalake::arrow::datatypes::DataType::Int64, false),
                    deltalake::arrow::datatypes::Field::new("name", deltalake::arrow::datatypes::DataType::Utf8, false),
                ]))))
            });
        writer_mock.expect_read_hwm().returning(|_| Ok(None));
        state_mock
            .expect_update_table()
            .withf(|_, state, _| state.last_run_status.as_deref() == Some("success"))
            .returning(|_, _, _| Ok(()));

        let mut orch = make_orchestrator(config, schema_mock, extract_mock, writer_mock, state_mock, dir.path().to_path_buf());
        let result = orch.run().await;
        assert!(matches!(result, ExitCode::Success), "expected additive-evolution Success, got {result:?}");
    }

    #[tokio::test]
    async fn incremental_bails_when_delta_has_data_but_no_hwm() {
        // H-2026-07-11-1: nothing stored, no config seed, but the Delta table already
        // holds data (e.g. a full-refresh rebuild wiped HWM visibility). Re-extracting
        // from scratch with APPEND would duplicate every row — must fail fast, before
        // any extract call.
        let dir = TempDir::new().unwrap();
        let config = make_config(vec!["orders".to_string()]);
        let mut schema_mock = MockSchemaInspect::new();
        let mut extract_mock = MockExtract::new();
        let mut writer_mock = MockDeltaWrite::new();
        // NOTE: deliberately overriding the blanket Ok(false): this table HAS data.
        writer_mock.expect_has_data().returning(|_| Ok(true));
        let mut state_mock = MockStateManage::new();

        state_mock
            .expect_load_or_default()
            .returning(|_| crate::state::AppState::default());
        schema_mock
            .expect_discover_columns()
            .returning(move |_| Ok(make_columns()));
        schema_mock
            .expect_discover_indexes()
            .returning(|_| Ok(make_full_refresh_primary_key("id")));
        schema_mock
            .expect_get_avg_row_length()
            .returning(|_| Ok(Some(100)));
        extract_mock
            .expect_calculate_batch_size()
            .returning(|_| 10000);
        writer_mock.expect_ensure_table().returning(|_, _| Ok(()));
        writer_mock.expect_get_schema().returning(|_| Ok(None));
        writer_mock.expect_read_hwm().returning(|_| Ok(None));
        // The guard must fire BEFORE any extraction or write:
        extract_mock.expect_extract().times(0);
        writer_mock.expect_append_batch().times(0);
        state_mock.expect_update_table().returning(|_, _, _| Ok(()));

        let mut orch = make_orchestrator(config, schema_mock, extract_mock, writer_mock, state_mock, dir.path().to_path_buf());
        let result = orch.run().await;
        assert!(matches!(result, ExitCode::Fatal), "sole table must fail: {result:?}");
    }

    #[tokio::test]
    async fn incremental_nullable_cursor_counts_and_warns_but_still_succeeds() {
        // D2: an explicit TABLE_MODE=incremental on a NULLABLE cursor is honored (O3 decision b),
        // but rows whose cursor is NULL are silently excluded by `WHERE <ts> IS NOT NULL`. The run
        // must probe count_null ONCE, warn (side effect), and STILL succeed — the loss is made
        // observable, not fatal. Assert Success + that count_null was called exactly once.
        let dir = TempDir::new().unwrap();
        let mut config = make_config(vec!["orders".to_string()]);
        config
            .table_modes
            .insert("orders".to_string(), crate::config::ExtractionMode::Incremental);
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
            .returning(move |_| {
                Ok(vec![
                    crate::discovery::ColumnInfo {
                        name: "id".to_string(),
                        data_type: "bigint".to_string(),
                        column_type: "bigint(20)".to_string(),
                        nullable: false,
                    },
                    crate::discovery::ColumnInfo {
                        name: "updated_at".to_string(),
                        data_type: "timestamp".to_string(),
                        column_type: "timestamp".to_string(),
                        nullable: true, // nullable cursor — the D2 trap
                    },
                ])
            });
        schema_mock
            .expect_discover_indexes()
            .returning(|_| Ok(make_full_refresh_indexes()));
        schema_mock
            .expect_get_avg_row_length()
            .returning(|_| Ok(Some(100)));
        // The D2 probe: called exactly once per run for the (nullable) cursor column, reporting
        // 5 excluded NULL-cursor rows. Its result is a warn side effect, not a failure.
        schema_mock
            .expect_count_null()
            .withf(|table, col| table == "orders" && col == "updated_at")
            .times(1)
            .returning(|_, _| Ok(5));
        extract_mock
            .expect_calculate_batch_size()
            .returning(|_| 10000);
        extract_mock.expect_batch_size().returning(|| 10000);
        writer_mock.expect_ensure_table().returning(|_, _| Ok(()));
        writer_mock.expect_get_schema().returning(|_| Ok(None));
        writer_mock.expect_read_hwm().returning(|_| Ok(None));
        extract_mock
            .expect_extract()
            .returning(|_| ok_batches(vec![]));
        state_mock
            .expect_update_table()
            .withf(|_, state, _| state.last_run_status.as_deref() == Some("success"))
            .returning(|_, _, _| Ok(()));

        let mut orch = make_orchestrator(config, schema_mock, extract_mock, writer_mock, state_mock, dir.path().to_path_buf());
        let result = orch.run().await;
        assert!(matches!(result, ExitCode::Success), "nullable-cursor run must succeed (warn only): {result:?}");
    }

    #[tokio::test]
    async fn incremental_not_null_cursor_skips_count_null_probe() {
        // D2: a NOT NULL cursor provably has zero NULL-cursor rows, so the count_null probe (a
        // COUNT(*) scan) is skipped entirely — count_null must NEVER be called. No expectation is
        // registered for it, so any call would panic the mock.
        let dir = TempDir::new().unwrap();
        let config = make_config(vec!["orders".to_string()]); // make_columns() → NOT NULL updated_at
        let mut schema_mock = MockSchemaInspect::new();
        let mut extract_mock = MockExtract::new();
        let mut writer_mock = MockDeltaWrite::new();
        writer_mock.expect_has_data().returning(|_| Ok(false));
        let mut state_mock = MockStateManage::new();

        setup_incremental_mocks(&mut schema_mock, &mut extract_mock, &mut writer_mock, &mut state_mock);
        // Deliberately NO expect_count_null: the probe must not run for a NOT NULL cursor.

        let mut orch = make_orchestrator(config, schema_mock, extract_mock, writer_mock, state_mock, dir.path().to_path_buf());
        let result = orch.run().await;
        assert!(matches!(result, ExitCode::Success));
    }

    #[tokio::test]
    async fn incremental_proceeds_when_no_hwm_and_delta_empty() {
        // First run: table freshly created by ensure_table, zero data files — the
        // H-2026-07-11-1 guard must NOT fire; extraction proceeds from scratch.
        let dir = TempDir::new().unwrap();
        let config = make_config(vec!["orders".to_string()]);
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
            .returning(move |_| Ok(make_columns()));
        schema_mock
            .expect_discover_indexes()
            .returning(|_| Ok(make_full_refresh_primary_key("id")));
        schema_mock
            .expect_get_avg_row_length()
            .returning(|_| Ok(Some(100)));
        extract_mock
            .expect_calculate_batch_size()
            .returning(|_| 10000);
        extract_mock.expect_batch_size().returning(|| 10000);
        writer_mock.expect_ensure_table().returning(|_, _| Ok(()));
        writer_mock.expect_get_schema().returning(|_| Ok(None));
        writer_mock.expect_read_hwm().returning(|_| Ok(None));
        extract_mock
            .expect_extract()
            .returning(|_| Ok(crate::extractor::Extraction { batches: vec![], truncated: false }));
        state_mock.expect_update_table().returning(|_, _, _| Ok(()));

        let mut orch = make_orchestrator(config, schema_mock, extract_mock, writer_mock, state_mock, dir.path().to_path_buf());
        let result = orch.run().await;
        assert!(matches!(result, ExitCode::Success), "fresh empty table must proceed: {result:?}");
    }
}
