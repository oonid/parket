use std::time::Instant;

use anyhow::{Result, bail};
use tracing::info;

use crate::query::QueryBuilder;
use crate::writer::{extract_hwm_from_batch, hwm_has_advanced};

use super::Orchestrator;
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
    ) -> Result<u64> {
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
            );

            let batches = self.extractor.extract(&sql)?;
            if batches.is_empty()
                || batches.iter().all(|b| b.num_rows() == 0)
            {
                break;
            }

            let batch_rows: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
            let arrow_bytes: usize = batches.iter().map(|b| b.get_array_memory_size()).sum();
            let batch_start = Instant::now();

            let batch_hwm = batches
                .last()
                .and_then(|b| extract_hwm_from_batch(b, ts_col, key_col));

            if batch_rows == batch_size {
                match batch_hwm.as_ref() {
                    None => bail!(
                        "incremental batch for table `{table_name}` could not extract HWM from a full batch"
                    ),
                    Some(next_hwm) if !hwm_has_advanced(current_hwm.as_ref(), next_hwm) => bail!(
                        "incremental batch for table `{table_name}` did not advance HWM on a full batch"
                    ),
                    Some(_) => {}
                }
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

            if batch_rows < batch_size {
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
        let mut state_mock = MockStateManage::new();

        state_mock
            .expect_load_or_default()
            .returning(|_| crate::state::AppState::default());
        schema_mock
            .expect_discover_columns()
            .returning(move |_| Ok(make_columns()));
        schema_mock
            .expect_discover_indexes()
            .returning(|_| Ok(make_full_refresh_indexes()));
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
                    Ok(vec![batch])
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
                    Ok(vec![batch])
                } else {
                    Ok(vec![])
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
    async fn incremental_full_batch_without_hwm_fails_before_append() {
        let dir = TempDir::new().unwrap();
        let config = make_config(vec!["orders".to_string()]);
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
            .expect_discover_indexes()
            .returning(|_| Ok(make_full_refresh_indexes()));
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
                Ok(vec![batch])
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
        let mut state_mock = MockStateManage::new();

        state_mock
            .expect_load_or_default()
            .returning(|_| crate::state::AppState::default());
        schema_mock
            .expect_discover_columns()
            .returning(move |_| Ok(make_columns()));
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
        writer_mock
            .expect_read_hwm()
            .returning(|_| Ok(None));
        extract_mock
            .expect_extract()
            .withf(|sql| sql.contains("2026-05-01T00:00:00.000000") && sql.contains("999"))
            .returning(|_| Ok(vec![]));
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
        let mut state_mock = MockStateManage::new();

        state_mock
            .expect_load_or_default()
            .returning(|_| crate::state::AppState::default());
        schema_mock
            .expect_discover_columns()
            .returning(move |_| Ok(make_columns()));
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
            .returning(|_| Ok(vec![]));
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
                    },
                    crate::discovery::ColumnInfo {
                        name: "name".to_string(),
                        data_type: "varchar".to_string(),
                        column_type: "varchar(255)".to_string(),
                    },
                    crate::discovery::ColumnInfo {
                        name: "completed_at".to_string(),
                        data_type: "timestamp".to_string(),
                        column_type: "timestamp".to_string(),
                    },
                ])
            });
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
        writer_mock
            .expect_read_hwm()
            .returning(|_| Ok(None));
        extract_mock
            .expect_extract()
            .withf(|sql| sql.contains("completed_at"))
            .returning(|_| Ok(vec![]));
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
                    },
                    crate::discovery::ColumnInfo {
                        name: "name".to_string(),
                        data_type: "varchar".to_string(),
                        column_type: "varchar(255)".to_string(),
                    },
                    crate::discovery::ColumnInfo {
                        name: "updated_at".to_string(),
                        data_type: "timestamp".to_string(),
                        column_type: "timestamp".to_string(),
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
            .returning(|_| Ok(vec![]));
        state_mock
            .expect_update_table()
            .returning(|_, _, _| Ok(()));

        let mut orch = make_orchestrator(config, schema_mock, extract_mock, writer_mock, state_mock, dir.path().to_path_buf());
        let result = orch.run().await;
        assert!(matches!(result, ExitCode::Success));
    }

    #[tokio::test]
    async fn incremental_fails_fast_when_delta_schema_missing_cursor_column() {
        // N3: when the schema-evolution filter drops the cursor column (Delta schema
        // predates it), fail immediately with an actionable message instead of
        // extracting a full batch and only then discovering the HWM can't be read.
        let dir = TempDir::new().unwrap();
        let config = make_config(vec!["orders".to_string()]);
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
            .expect_discover_indexes()
            .returning(|_| Ok(make_full_refresh_indexes()));
        schema_mock
            .expect_get_avg_row_length()
            .returning(|_| Ok(Some(100)));
        extract_mock
            .expect_calculate_batch_size()
            .returning(|_| 10000);
        extract_mock
            .expect_extract()
            .times(0)
            .returning(|_| Ok(vec![]));
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
        state_mock
            .expect_update_table()
            .withf(|_, state, _| state.last_run_status.as_deref() == Some("failed"))
            .returning(|_, _, _| Ok(()));

        let mut orch = make_orchestrator(config, schema_mock, extract_mock, writer_mock, state_mock, dir.path().to_path_buf());
        let result = orch.run().await;
        assert!(matches!(result, ExitCode::Fatal));
    }
}
