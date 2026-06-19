use std::time::Instant;

use anyhow::Result;
use tracing::info;

use crate::query::QueryBuilder;

use super::Orchestrator;
use super::{DeltaWrite, Extract, SchemaInspect, StateManage};

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
    ) -> Result<u64> {
        let batch_size = self.extractor.batch_size();
        let mut total_rows = 0u64;
        let mut chunk_index: u64 = 0;

        loop {
            if self.check_shutdown() {
                info!(table = table_name, "shutdown signal received during full refresh");
                break;
            }

            let chunk_start = Instant::now();
            let offset = chunk_index * batch_size;
            let sql = QueryBuilder::build_full_refresh_query_paged(
                table_name, columns, batch_size, offset,
            );

            let batches = self.extractor.extract(&sql)?;

            if batches.is_empty() || batches.iter().all(|b| b.num_rows() == 0) {
                break;
            }

            let chunk_rows: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
            let arrow_bytes: usize = batches.iter().map(|b| b.get_array_memory_size()).sum();

            if chunk_index == 0 {
                self.writer
                    .overwrite_table(table_name, batches, None)
                    .await?;
            } else {
                self.writer
                    .append_batch(table_name, batches, None)
                    .await?;
            }

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

            if chunk_rows < batch_size {
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
    use crate::discovery::ColumnInfo;
    use deltalake::arrow::record_batch::RecordBatch;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tempfile::TempDir;

    #[tokio::test]
    async fn full_refresh_table_succeeds() {
        let dir = TempDir::new().unwrap();
        let config = make_config_with_full_refresh(vec!["products".to_string()]);
        let mut schema_mock = MockSchemaInspect::new();
        let mut extract_mock = MockExtract::new();
        let mut writer_mock = MockDeltaWrite::new();
        let mut state_mock = MockStateManage::new();

        state_mock
            .expect_load_or_default()
            .returning(|_| crate::state::AppState::default());
        schema_mock
            .expect_discover_columns()
            .returning(move |_| Ok(make_full_refresh_columns()));
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
                Ok(vec![batch])
            });
        writer_mock
            .expect_overwrite_table()
            .returning(|_, _, _| Ok(()));
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
        let mut state_mock = MockStateManage::new();

        state_mock.expect_load_or_default().returning(|_| crate::state::AppState::default());
        schema_mock.expect_discover_columns().returning(move |_| Ok(make_full_refresh_columns()));
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
                return Ok(vec![]);
            }
            let batch = RecordBatch::try_new(
                schema,
                vec![Arc::new(deltalake::arrow::array::Int64Array::from(rows))],
            ).unwrap();
            Ok(vec![batch])
        });

        writer_mock.expect_overwrite_table().times(1).returning(|_, _, _| Ok(()));
        writer_mock.expect_append_batch().times(2).returning(|_, _, _| Ok(()));
        state_mock.expect_update_table().returning(|_, _, _| Ok(()));

        let mut orch = make_orchestrator(config, schema_mock, extract_mock, writer_mock, state_mock, dir.path().to_path_buf());
        let result = orch.run().await;
        assert!(matches!(result, ExitCode::Success));
        assert_eq!(call_count.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn full_refresh_empty_table_writes_nothing() {
        let dir = TempDir::new().unwrap();
        let config = make_config_with_full_refresh(vec!["products".to_string()]);
        let mut schema_mock = MockSchemaInspect::new();
        let mut extract_mock = MockExtract::new();
        let mut writer_mock = MockDeltaWrite::new();
        let mut state_mock = MockStateManage::new();

        state_mock.expect_load_or_default().returning(|_| crate::state::AppState::default());
        schema_mock.expect_discover_columns().returning(move |_| Ok(make_full_refresh_columns()));
        schema_mock.expect_get_avg_row_length().returning(|_| Ok(Some(100)));
        extract_mock.expect_calculate_batch_size().returning(|_| 10000);
        extract_mock.expect_batch_size().returning(|| 10000);
        writer_mock.expect_ensure_table().returning(|_, _| Ok(()));
        writer_mock.expect_get_schema().returning(|_| Ok(None));
        extract_mock.expect_extract().returning(|_| Ok(vec![]));
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
        let mut state_mock = MockStateManage::new();

        state_mock.expect_load_or_default().returning(|_| crate::state::AppState::default());
        schema_mock.expect_discover_columns().returning(move |_| Ok(make_full_refresh_columns()));
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
                Ok(vec![batch])
            } else {
                Ok(vec![])
            }
        });

        writer_mock.expect_overwrite_table().times(1).returning(|_, _, _| Ok(()));
        writer_mock.expect_append_batch().times(1)
            .returning(|_, _, _| Err(anyhow::anyhow!("append failed")));
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
        let mut state_mock = MockStateManage::new();

        state_mock
            .expect_load_or_default()
            .returning(|_| crate::state::AppState::default());
        schema_mock
            .expect_discover_columns()
            .returning(move |_| {
                Ok(vec![
                    ColumnInfo {
                        name: "id".to_string(),
                        data_type: "bigint".to_string(),
                        column_type: "bigint(20)".to_string(),
                    },
                    ColumnInfo {
                        name: "name".to_string(),
                        data_type: "varchar".to_string(),
                        column_type: "varchar(255)".to_string(),
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
}
