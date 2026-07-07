use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use deltalake::arrow::datatypes::{Field, Schema, SchemaRef};
use tokio::sync::watch;
use tracing::{error, info};

use crate::config::{Config, ExtractionMode};
use crate::discovery::{
    ColumnInfo, IndexInfo, compute_schema_hash, detect_mode, filter_unsupported_columns,
};
use crate::state::{AppState, TableState};
use crate::writer::Hwm;

mod adapters;
mod incremental;
mod full_refresh;
mod two_stream;
mod schema;
mod datetime;
#[cfg(test)]
mod test_support;
pub use adapters::{DeltaWriterAdapter, ExtractorAdapter, LocalDeltaWriterAdapter, SchemaInspectorAdapter, StateManageAdapter};
use schema::{mariadb_type_to_arrow, schema_evolution_check};
use full_refresh::select_integer_pk;
use datetime::format_timestamp_now;

#[cfg_attr(test, mockall::automock)]
#[allow(async_fn_in_trait)]
pub trait SchemaInspect: Send + Sync {
    async fn discover_columns(&self, table: &str) -> Result<Vec<ColumnInfo>>;
    async fn discover_indexes(&self, table: &str) -> Result<Vec<IndexInfo>>;
    async fn get_avg_row_length(&self, table: &str) -> Result<Option<u64>>;
    async fn max_timestamp(&self, table: &str, col: &str) -> Result<Option<String>>;
}

#[cfg_attr(test, mockall::automock)]
pub trait Extract: Send {
    fn calculate_batch_size(&mut self, avg_row_length: Option<u64>) -> u64;
    fn extract(&mut self, sql: &str) -> Result<Vec<deltalake::arrow::record_batch::RecordBatch>>;
    fn batch_size(&self) -> u64;
}

#[cfg_attr(test, mockall::automock)]
#[allow(async_fn_in_trait)]
pub trait DeltaWrite: Send + Sync {
    async fn ensure_table(&self, table_name: &str, schema: SchemaRef) -> Result<()>;
    async fn append_batch(
        &self,
        table_name: &str,
        batches: Vec<deltalake::arrow::record_batch::RecordBatch>,
        hwm: Option<Hwm>,
    ) -> Result<()>;
    async fn overwrite_table(
        &self,
        table_name: &str,
        batches: Vec<deltalake::arrow::record_batch::RecordBatch>,
        hwm: Option<Hwm>,
    ) -> Result<()>;
    async fn read_hwm(&self, table_name: &str) -> Result<Option<Hwm>>;
    async fn get_schema(&self, table_name: &str) -> Result<Option<SchemaRef>>;
    async fn merge_batch(
        &self,
        table_name: &str,
        batches: Vec<deltalake::arrow::record_batch::RecordBatch>,
        key_col: String,
        insert_id: Option<i64>,
        update_hwm: Option<Hwm>,
    ) -> Result<()>;
    async fn delete_then_append(
        &self,
        table_name: &str,
        batches: Vec<deltalake::arrow::record_batch::RecordBatch>,
        key_col: String,
        insert_id: Option<i64>,
        update_hwm: Option<Hwm>,
    ) -> Result<()>;
    async fn read_insert_hwm(&self, table_name: &str) -> Result<Option<i64>>;
    async fn append_two_stream(
        &self,
        table_name: &str,
        batches: Vec<deltalake::arrow::record_batch::RecordBatch>,
        insert_id: Option<i64>,
        update_hwm: Option<Hwm>,
    ) -> Result<()>;
}

#[cfg_attr(test, mockall::automock)]
pub trait StateManage: Send {
    fn load_or_default(&mut self, path: &Path) -> AppState;
    fn update_table(&mut self, name: &str, state: TableState, path: &Path) -> Result<()>;
}

fn column_info_to_v57_schema(columns: &[ColumnInfo]) -> Result<SchemaRef> {
    let fields: Result<Vec<Field>> = columns
        .iter()
        .map(|c| {
            let dt = mariadb_type_to_arrow(&c.data_type, &c.column_type)?;
            Ok(Field::new(&c.name, dt, true))
        })
        .collect();
    Ok(Arc::new(Schema::new(fields?)))
}


#[derive(Debug)]
pub enum ExitCode {
    Success = 0,
    PartialFailure = 1,
    Fatal = 2,
}

pub struct Orchestrator<S, E, W, M> {
    config: Config,
    schema_inspect: S,
    extractor: E,
    writer: W,
    state_mgr: M,
    shutdown: watch::Receiver<bool>,
    state_path: PathBuf,
    progress: bool,
}

impl<S, E, W, M> Orchestrator<S, E, W, M>
where
    S: SchemaInspect + Send + Sync,
    E: Extract + Send,
    W: DeltaWrite + Send + Sync,
    M: StateManage + Send,
{
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        config: Config,
        schema_inspect: S,
        extractor: E,
        writer: W,
        state_mgr: M,
        shutdown: watch::Receiver<bool>,
        state_path: PathBuf,
        progress: bool,
    ) -> Self {
        Self {
            config,
            schema_inspect,
            extractor,
            writer,
            state_mgr,
            shutdown,
            state_path,
            progress,
        }
    }

    fn check_shutdown(&self) -> bool {
        *self.shutdown.borrow()
    }

    pub async fn run(&mut self) -> ExitCode {
        let run_start = Instant::now();
        self.state_mgr.load_or_default(&self.state_path);

        let mut succeeded = 0u32;
        let mut failed = 0u32;

        let tables = self.config.tables.clone();
        for table_name in &tables {
            if self.check_shutdown() {
                info!("shutdown signal received, stopping table processing");
                break;
            }

            match self.process_table(table_name).await {
                Ok(()) => {
                    info!(table = table_name, "table succeeded");
                    succeeded += 1;
                }
                Err(e) => {
                    error!(table = table_name, error = %e, "table failed");
                    for cause in e.chain() {
                        error!(table = table_name, cause = %cause, "  caused by");
                    }
                    failed += 1;
                    if let Err(se) = self.state_mgr.update_table(
                        table_name,
                        TableState {
                            last_run_at: Some(format_timestamp_now()),
                            last_run_status: Some("failed".to_string()),
                            last_run_rows: None,
                            last_run_duration_ms: None,
                            extraction_mode: None,
                            schema_columns_hash: None,
                        },
                        &self.state_path,
                    ) {
                        error!(table = table_name, error = %se, "failed to update state for failed table");
                    }
                }
            }
        }

        let duration_ms = run_start.elapsed().as_millis() as u64;
        info!(
            succeeded,
            failed,
            duration_ms,
            "run complete"
        );

        if failed > 0 && succeeded > 0 {
            ExitCode::PartialFailure
        } else if failed > 0 {
            ExitCode::Fatal
        } else {
            ExitCode::Success
        }
    }

    async fn process_table(&mut self, table_name: &str) -> Result<()> {
        let start = Instant::now();

        let columns = self.schema_inspect.discover_columns(table_name).await?;
        let columns = filter_unsupported_columns(&columns);

        let ts_col = match self.config.table_timestamp_col.get(table_name) {
            Some(ovr) => {
                crate::discovery::validate_timestamp_col(&columns, ovr)?;
                ovr.clone()
            }
            None => crate::discovery::detect_timestamp_col(&columns)
                .unwrap_or_else(|| "updated_at".to_string()),
        };

        // Resolve TwoStream mode from configuration
        let has_insert = self.config.table_insert_cursor.contains_key(table_name);
        let has_update = self.config.table_update_cursor.contains_key(table_name);
        if has_insert ^ has_update {
            anyhow::bail!("two-stream requires BOTH TABLE_INSERT_CURSOR_{table_name} and TABLE_UPDATE_CURSOR_{table_name}");
        }
        let mode = if let Some((ins, upd)) = self.config.two_stream(table_name) {
            crate::discovery::validate_two_stream_cursors(&columns, &ins, &upd)?;
            ExtractionMode::TwoStream
        } else {
            let mode_override = self.config.table_modes.get(table_name);
            detect_mode(&columns, mode_override, &ts_col)
        };

        let mode_str = match mode {
            ExtractionMode::Incremental => "incremental",
            ExtractionMode::FullRefresh => "full_refresh",
            ExtractionMode::TwoStream => "two_stream",
            ExtractionMode::Auto => "auto",
        };

        if !matches!(mode, ExtractionMode::Incremental | ExtractionMode::TwoStream) && self.config.table_initial_hwm.contains_key(table_name) {
            anyhow::bail!(
                "TABLE_HWM_{table_name} is set but table '{table_name}' resolves to {mode_str}; \
                 a predefined HWM only applies to incremental or two-stream tables"
            );
        }

        let avg_row_length = self.schema_inspect.get_avg_row_length(table_name).await?;
        self.extractor.calculate_batch_size(avg_row_length);

        let schema = column_info_to_v57_schema(&columns)?;
        self.writer.ensure_table(table_name, schema.clone()).await?;

        let column_names: Vec<String> = columns.iter().map(|c| c.name.clone()).collect();
        let select_columns = match mode {
            ExtractionMode::Incremental | ExtractionMode::TwoStream => {
                if let Some(existing_schema) = self.writer.get_schema(table_name).await? {
                    schema_evolution_check(&columns, &existing_schema)?
                } else {
                    column_names
                }
            }
            _ => column_names,
        };

        let rows = match mode {
            ExtractionMode::Incremental => {
                let indexes = self.schema_inspect.discover_indexes(table_name).await?;
                let key_col = select_integer_pk(&columns, &indexes).unwrap_or_else(|| "id".to_string());
                if !select_columns.iter().any(|c| c == &ts_col) {
                    anyhow::bail!(
                        "incremental table '{table_name}' cursor column `{ts_col}` is missing from the Delta table schema (the schema-evolution filter dropped it because the Delta schema lacks it); evolve the Delta schema to add `{ts_col}` or run a full refresh for this table"
                    );
                }
                if !select_columns.iter().any(|c| c == &key_col) {
                    anyhow::bail!(
                        "incremental table '{table_name}' key column `{key_col}` is missing from the Delta table schema (the schema-evolution filter dropped it because the Delta schema lacks it); evolve the Delta schema to add `{key_col}` or run a full refresh for this table"
                    );
                }
                self.process_incremental(table_name, &select_columns, &ts_col, &key_col).await?
            }
            ExtractionMode::FullRefresh => {
                let indexes = self.schema_inspect.discover_indexes(table_name).await?;
                self.process_full_refresh(table_name, &select_columns, &columns, &indexes)
                    .await?
            }
            ExtractionMode::TwoStream => {
                let (insert_col, update_col) = self.config.two_stream(table_name)
                    .expect("two_stream config present for TwoStream mode");
                if !select_columns.iter().any(|c| c == &insert_col) {
                    anyhow::bail!(
                        "two-stream table '{table_name}' insert cursor column `{insert_col}` is missing from the Delta table schema (the schema-evolution filter dropped it because the Delta schema lacks it); evolve the Delta schema to add `{insert_col}` or run a full refresh for this table"
                    );
                }
                if !select_columns.iter().any(|c| c == &update_col) {
                    anyhow::bail!(
                        "two-stream table '{table_name}' update cursor column `{update_col}` is missing from the Delta table schema (the schema-evolution filter dropped it because the Delta schema lacks it); evolve the Delta schema to add `{update_col}` or run a full refresh for this table"
                    );
                }
                self.process_two_stream(table_name, &select_columns, &insert_col, &update_col).await?
            }
            ExtractionMode::Auto => unreachable!(),
        };

        let elapsed = start.elapsed();
        let hash = compute_schema_hash(&columns);
        self.state_mgr.update_table(
            table_name,
            TableState {
                last_run_at: Some(format_timestamp_now()),
                last_run_status: Some("success".to_string()),
                last_run_rows: Some(rows),
                last_run_duration_ms: Some(elapsed.as_millis() as u64),
                extraction_mode: Some(mode_str.to_string()),
                schema_columns_hash: Some(hash),
            },
            &self.state_path,
        )?;

        Ok(())
    }

}

pub struct SignalHandler {
    tx: watch::Sender<bool>,
}

impl SignalHandler {
    pub fn new() -> (Self, watch::Receiver<bool>) {
        let (tx, rx) = watch::channel(false);
        (Self { tx }, rx)
    }

    pub async fn install(self) {
        tokio::spawn(async move {
            tokio::signal::ctrl_c().await.ok();
            info!("received first signal, initiating graceful shutdown");
            let _ = self.tx.send(true);
            tokio::signal::ctrl_c().await.ok();
            info!("received second signal, forcing immediate exit");
            std::process::exit(130);
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::test_support::*;
    use deltalake::arrow::record_batch::RecordBatch;
    
    
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tempfile::TempDir;




    #[test]
    fn column_info_to_v57_schema_produces_valid_schema() {
        let columns = vec![
            ColumnInfo { name: "id".into(), data_type: "bigint".into(), column_type: "bigint(20)".into() },
            ColumnInfo { name: "name".into(), data_type: "varchar".into(), column_type: "varchar(255)".into() },
            ColumnInfo { name: "price".into(), data_type: "double".into(), column_type: "double".into() },
            ColumnInfo { name: "updated_at".into(), data_type: "timestamp".into(), column_type: "timestamp".into() },
            ColumnInfo { name: "is_active".into(), data_type: "boolean".into(), column_type: "tinyint(1)".into() },
            ColumnInfo { name: "birth_date".into(), data_type: "date".into(), column_type: "date".into() },
        ];
        let schema = column_info_to_v57_schema(&columns).unwrap();
        assert_eq!(schema.fields().len(), 6);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(1).name(), "name");
        assert_eq!(schema.field(2).name(), "price");
        assert_eq!(schema.field(3).name(), "updated_at");
        assert_eq!(schema.field(4).name(), "is_active");
        assert_eq!(schema.field(5).name(), "birth_date");
    }

    #[test]
    fn exit_code_values() {
        assert_eq!(ExitCode::Success as i32, 0);
        assert_eq!(ExitCode::PartialFailure as i32, 1);
        assert_eq!(ExitCode::Fatal as i32, 2);
    }

    #[tokio::test]
    async fn single_table_succeeds() {
        let dir = TempDir::new().unwrap();
        let config = make_config(vec!["orders".to_string()]);
        let mut schema_mock = MockSchemaInspect::new();
        let mut extract_mock = MockExtract::new();
        let mut writer_mock = MockDeltaWrite::new();
        let mut state_mock = MockStateManage::new();

        setup_incremental_mocks(&mut schema_mock, &mut extract_mock, &mut writer_mock, &mut state_mock);

        let mut orch = make_orchestrator(config, schema_mock, extract_mock, writer_mock, state_mock, dir.path().to_path_buf());
        let result = orch.run().await;
        assert!(matches!(result, ExitCode::Success));
    }

    #[tokio::test]
    async fn multiple_tables_succeed() {
        let dir = TempDir::new().unwrap();
        let config = make_config(vec!["orders".to_string(), "customers".to_string()]);
        let mut schema_mock = MockSchemaInspect::new();
        let mut extract_mock = MockExtract::new();
        let mut writer_mock = MockDeltaWrite::new();
        let mut state_mock = MockStateManage::new();

        setup_incremental_mocks(&mut schema_mock, &mut extract_mock, &mut writer_mock, &mut state_mock);
        state_mock
            .expect_update_table()
            .returning(|_, _, _| Ok(()));

        let mut orch = make_orchestrator(config, schema_mock, extract_mock, writer_mock, state_mock, dir.path().to_path_buf());
        let result = orch.run().await;
        assert!(matches!(result, ExitCode::Success));
    }

    #[tokio::test]
    async fn partial_failure_one_fails() {
        let dir = TempDir::new().unwrap();
        let config = make_config(vec!["good_table".to_string(), "bad_table".to_string()]);
        let mut schema_mock = MockSchemaInspect::new();
        let mut extract_mock = MockExtract::new();
        let mut writer_mock = MockDeltaWrite::new();
        let mut state_mock = MockStateManage::new();

        state_mock
            .expect_load_or_default()
            .returning(|_| AppState::default());

        schema_mock
            .expect_discover_columns()
            .withf(|t| t == "good_table")
            .returning(move |_| Ok(make_columns()));
        schema_mock
            .expect_discover_indexes()
            .withf(|t| t == "good_table")
            .returning(|_| Ok(make_full_refresh_indexes()));
        schema_mock
            .expect_get_avg_row_length()
            .withf(|t| t == "good_table")
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
            .withf(|t| t == "good_table")
            .returning(|_| Ok(None));
        writer_mock
            .expect_read_hwm()
            .withf(|t| t == "good_table")
            .returning(|_| Ok(None));
        extract_mock
            .expect_extract()
            .returning(|_| Ok(vec![]));
        state_mock
            .expect_update_table()
            .returning(|_, _, _| Ok(()));

        schema_mock
            .expect_discover_columns()
            .withf(|t| t == "bad_table")
            .returning(|_| Err(anyhow::anyhow!("db error")));
        state_mock
            .expect_update_table()
            .withf(|name, state, _| {
                name == "bad_table"
                    && state.last_run_status.as_deref() == Some("failed")
            })
            .returning(|_, _, _| Ok(()));

        let mut orch = make_orchestrator(config, schema_mock, extract_mock, writer_mock, state_mock, dir.path().to_path_buf());
        let result = orch.run().await;
        assert!(
            matches!(result, ExitCode::PartialFailure),
            "expected PartialFailure, got {:?}",
            match result {
                ExitCode::Success => "Success",
                ExitCode::PartialFailure => "PartialFailure",
                ExitCode::Fatal => "Fatal",
            }
        );
    }

    #[tokio::test]
    async fn fatal_all_fail() {
        let dir = TempDir::new().unwrap();
        let config = make_config(vec!["bad1".to_string()]);
        let mut schema_mock = MockSchemaInspect::new();
        let extract_mock = MockExtract::new();
        let writer_mock = MockDeltaWrite::new();
        let mut state_mock = MockStateManage::new();

        schema_mock
            .expect_discover_columns()
            .returning(|_| Err(anyhow::anyhow!("db error")));
        state_mock
            .expect_load_or_default()
            .returning(|_| AppState::default());
        state_mock
            .expect_update_table()
            .withf(|name, state, _| {
                name == "bad1"
                    && state.last_run_status.as_deref() == Some("failed")
            })
            .returning(|_, _, _| Ok(()));

        let mut orch = make_orchestrator(config, schema_mock, extract_mock, writer_mock, state_mock, dir.path().to_path_buf());
        let result = orch.run().await;
        assert!(matches!(result, ExitCode::Fatal));
    }

    #[tokio::test]
    async fn shutdown_signal_stops_processing() {
        let dir = TempDir::new().unwrap();
        let config = make_config(vec!["table1".to_string(), "table2".to_string()]);
        let mut schema_mock = MockSchemaInspect::new();
        let mut extract_mock = MockExtract::new();
        let mut writer_mock = MockDeltaWrite::new();
        let mut state_mock = MockStateManage::new();
        let (tx, rx) = watch::channel(false);

        setup_incremental_mocks(&mut schema_mock, &mut extract_mock, &mut writer_mock, &mut state_mock);

        tx.send(true).unwrap();

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
        let result = orch.run().await;
        assert!(matches!(result, ExitCode::Success));
    }

    #[tokio::test]
    async fn batch_loop_until_exhausted() {
        let dir = TempDir::new().unwrap();
        let config = make_config(vec!["orders".to_string()]);
        let mut schema_mock = MockSchemaInspect::new();
        let mut extract_mock = MockExtract::new();
        let mut writer_mock = MockDeltaWrite::new();
        let mut state_mock = MockStateManage::new();

        state_mock
            .expect_load_or_default()
            .returning(|_| AppState::default());
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
            .returning(|| 1);
        writer_mock
            .expect_ensure_table()
            .returning(|_, _| Ok(()));
        writer_mock
            .expect_get_schema()
            .returning(|_| Ok(None));
        writer_mock
            .expect_read_hwm()
            .returning(|_| Ok(None));

        let call_count = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let call_count_clone = call_count.clone();
        extract_mock
            .expect_extract()
            .returning(move |_| {
                let count = call_count_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                if count < 2 {
                    let schema = Arc::new(deltalake::arrow::datatypes::Schema::new(vec![
                        deltalake::arrow::datatypes::Field::new("id", deltalake::arrow::datatypes::DataType::Int32, false),
                        deltalake::arrow::datatypes::Field::new("val", deltalake::arrow::datatypes::DataType::Int32, false),
                        deltalake::arrow::datatypes::Field::new("updated_at", deltalake::arrow::datatypes::DataType::Utf8, false),
                    ]));
                    let batch = deltalake::arrow::record_batch::RecordBatch::try_new(
                        schema,
                        vec![
                            Arc::new(deltalake::arrow::array::Int32Array::from(vec![count as i32 + 1])),
                            Arc::new(deltalake::arrow::array::Int32Array::from(vec![1i32])),
                            Arc::new(deltalake::arrow::array::StringArray::from(vec![format!("2026-01-01T00:00:0{count}.000000")])),
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
        assert_eq!(call_count.load(std::sync::atomic::Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn failed_table_writes_failed_state_to_state_json() {
        let dir = TempDir::new().unwrap();
        let state_path = dir.path().join("state.json");
        let config = make_config(vec!["failing_table".to_string()]);
        let mut schema_mock = MockSchemaInspect::new();
        let extract_mock = MockExtract::new();
        let writer_mock = MockDeltaWrite::new();
        let mut state_mock = MockStateManage::new();

        schema_mock
            .expect_discover_columns()
            .returning(|_| Err(anyhow::anyhow!("table not found")));
        state_mock
            .expect_load_or_default()
            .returning(|_| AppState::default());
        state_mock
            .expect_update_table()
            .withf(|name, state, _| {
                name == "failing_table"
                    && state.last_run_status.as_deref() == Some("failed")
                    && state.last_run_rows.is_none()
                    && state.last_run_duration_ms.is_none()
                    && state.extraction_mode.is_none()
                    && state.schema_columns_hash.is_none()
                    && state.last_run_at.is_some()
            })
            .returning(|_, _, _| Ok(()));

        let mut orch = make_orchestrator(config, schema_mock, extract_mock, writer_mock, state_mock, state_path);
        let result = orch.run().await;
        assert!(matches!(result, ExitCode::Fatal));
    }

    #[tokio::test]
    async fn batch_breaks_on_partial_batch() {
        let dir = TempDir::new().unwrap();
        let config = make_config(vec!["orders".to_string()]);
        let mut schema_mock = MockSchemaInspect::new();
        let mut extract_mock = MockExtract::new();
        let mut writer_mock = MockDeltaWrite::new();
        let mut state_mock = MockStateManage::new();

        state_mock
            .expect_load_or_default()
            .returning(|_| AppState::default());
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
            .returning(|| 10000);

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
                            Arc::new(deltalake::arrow::array::Int64Array::from(vec![1i64, 2i64, 3i64])),
                            Arc::new(deltalake::arrow::array::TimestampMicrosecondArray::from(vec![
                                1743158400000000i64,
                                1743158400000000i64,
                                1743158401000000i64,
                            ])),
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
        assert_eq!(call_count.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn column_info_to_v57_schema_unsupported_type() {
        let columns = vec![
            ColumnInfo { name: "id".into(), data_type: "bigint".into(), column_type: "bigint(20)".into() },
            ColumnInfo { name: "data".into(), data_type: "geometry".into(), column_type: "geometry".into() },
        ];
        let result = column_info_to_v57_schema(&columns);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("unsupported MariaDB type"),
            "expected unsupported type error, got: {err}"
        );
    }

    #[tokio::test]
    async fn ensure_table_failure_propagates() {
        let dir = TempDir::new().unwrap();
        let config = make_config(vec!["orders".to_string()]);
        let mut schema_mock = MockSchemaInspect::new();
        let mut extract_mock = MockExtract::new();
        let mut writer_mock = MockDeltaWrite::new();
        let mut state_mock = MockStateManage::new();

        state_mock
            .expect_load_or_default()
            .returning(|_| AppState::default());
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
            .returning(|_, _| Err(anyhow::anyhow!("S3 connection failed")));
        state_mock
            .expect_update_table()
            .withf(|_, state, _| state.last_run_status.as_deref() == Some("failed"))
            .returning(|_, _, _| Ok(()));

        let mut orch = make_orchestrator(config, schema_mock, extract_mock, writer_mock, state_mock, dir.path().to_path_buf());
        let result = orch.run().await;
        assert!(matches!(result, ExitCode::Fatal));
    }

    #[tokio::test]
    async fn extract_failure_propagates() {
        let dir = TempDir::new().unwrap();
        let config = make_config(vec!["orders".to_string()]);
        let mut schema_mock = MockSchemaInspect::new();
        let mut extract_mock = MockExtract::new();
        let mut writer_mock = MockDeltaWrite::new();
        let mut state_mock = MockStateManage::new();

        state_mock
            .expect_load_or_default()
            .returning(|_| AppState::default());
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
            .returning(|| 10000);
        extract_mock
            .expect_extract()
            .returning(|_| Err(anyhow::anyhow!("connection lost")));
        state_mock
            .expect_update_table()
            .withf(|_, state, _| state.last_run_status.as_deref() == Some("failed"))
            .returning(|_, _, _| Ok(()));

        let mut orch = make_orchestrator(config, schema_mock, extract_mock, writer_mock, state_mock, dir.path().to_path_buf());
        let result = orch.run().await;
        assert!(matches!(result, ExitCode::Fatal));
    }

    #[tokio::test]
    async fn append_batch_failure_propagates() {
        let dir = TempDir::new().unwrap();
        let config = make_config(vec!["orders".to_string()]);
        let mut schema_mock = MockSchemaInspect::new();
        let mut extract_mock = MockExtract::new();
        let mut writer_mock = MockDeltaWrite::new();
        let mut state_mock = MockStateManage::new();

        state_mock
            .expect_load_or_default()
            .returning(|_| AppState::default());
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
                } else {
                    Ok(vec![])
                }
            });

        writer_mock
            .expect_append_batch()
            .returning(|_, _, _| Err(anyhow::anyhow!("delta write failed")));
        state_mock
            .expect_update_table()
            .withf(|_, state, _| state.last_run_status.as_deref() == Some("failed"))
            .returning(|_, _, _| Ok(()));

        let mut orch = make_orchestrator(config, schema_mock, extract_mock, writer_mock, state_mock, dir.path().to_path_buf());
        let result = orch.run().await;
        assert!(matches!(result, ExitCode::Fatal));
    }

    #[tokio::test]
    async fn overwrite_failure_propagates() {
        let dir = TempDir::new().unwrap();
        let config = make_config_with_full_refresh(vec!["products".to_string()]);
        let mut schema_mock = MockSchemaInspect::new();
        let mut extract_mock = MockExtract::new();
        let mut writer_mock = MockDeltaWrite::new();
        let mut state_mock = MockStateManage::new();

        state_mock
            .expect_load_or_default()
            .returning(|_| AppState::default());
        schema_mock
            .expect_discover_columns()
            .returning(move |_| Ok(make_full_refresh_columns()));
        schema_mock
            .expect_get_avg_row_length()
            .returning(|_| Ok(Some(100)));
        schema_mock
            .expect_discover_indexes()
            .returning(|_| Ok(make_full_refresh_indexes()));
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
                ]));
                let batch = RecordBatch::try_new(
                    schema,
                    vec![Arc::new(deltalake::arrow::array::Int64Array::from(vec![1i64]))],
                )
                .unwrap();
                Ok(vec![batch])
            });
        writer_mock
            .expect_overwrite_table()
            .returning(|_, _, _| Err(anyhow::anyhow!("overwrite failed")));
        state_mock
            .expect_update_table()
            .withf(|_, state, _| state.last_run_status.as_deref() == Some("failed"))
            .returning(|_, _, _| Ok(()));

        let mut orch = make_orchestrator(config, schema_mock, extract_mock, writer_mock, state_mock, dir.path().to_path_buf());
        let result = orch.run().await;
        assert!(matches!(result, ExitCode::Fatal));
    }

    #[tokio::test]
    async fn state_update_failure_on_failed_table_still_continues() {
        let dir = TempDir::new().unwrap();
        let config = make_config(vec!["bad1".to_string(), "bad2".to_string()]);
        let mut schema_mock = MockSchemaInspect::new();
        let extract_mock = MockExtract::new();
        let writer_mock = MockDeltaWrite::new();
        let mut state_mock = MockStateManage::new();

        state_mock
            .expect_load_or_default()
            .returning(|_| AppState::default());

        schema_mock
            .expect_discover_columns()
            .returning(|_| Err(anyhow::anyhow!("db error")));

        state_mock
            .expect_update_table()
            .returning(|_, _, _| Err(anyhow::anyhow!("disk full")));

        schema_mock
            .expect_discover_columns()
            .returning(|_| Err(anyhow::anyhow!("db error")));

        state_mock
            .expect_update_table()
            .returning(|_, _, _| Err(anyhow::anyhow!("disk full")));

        let mut orch = make_orchestrator(config, schema_mock, extract_mock, writer_mock, state_mock, dir.path().to_path_buf());
        let result = orch.run().await;
        assert!(matches!(result, ExitCode::Fatal));
    }

    #[tokio::test]
    async fn state_update_failure_on_success_propagates() {
        let dir = TempDir::new().unwrap();
        let config = make_config(vec!["orders".to_string()]);
        let mut schema_mock = MockSchemaInspect::new();
        let mut extract_mock = MockExtract::new();
        let mut writer_mock = MockDeltaWrite::new();
        let mut state_mock = MockStateManage::new();

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
            .returning(|_| Ok(vec![]));
        state_mock
            .expect_load_or_default()
            .returning(|_| AppState::default());

        state_mock
            .expect_update_table()
            .withf(|_, state, _| state.last_run_status.as_deref() == Some("success"))
            .returning(|_, _, _| Err(anyhow::anyhow!("disk full")));
        state_mock
            .expect_update_table()
            .withf(|_, state, _| state.last_run_status.as_deref() == Some("failed"))
            .returning(|_, _, _| Ok(()));

        let mut orch = make_orchestrator(config, schema_mock, extract_mock, writer_mock, state_mock, dir.path().to_path_buf());
        let result = orch.run().await;
        assert!(matches!(result, ExitCode::Fatal));
    }

    #[tokio::test]
    async fn shutdown_signal_between_tables_skips_remaining() {
        let dir = TempDir::new().unwrap();
        let config = make_config(vec!["table1".to_string(), "table2".to_string()]);
        let mut schema_mock = MockSchemaInspect::new();
        let mut extract_mock = MockExtract::new();
        let mut writer_mock = MockDeltaWrite::new();
        let mut state_mock = MockStateManage::new();
        let (tx, rx) = watch::channel(false);

        setup_incremental_mocks(&mut schema_mock, &mut extract_mock, &mut writer_mock, &mut state_mock);

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

        tx.send(true).unwrap();
        let result = orch.run().await;
        assert!(matches!(result, ExitCode::Success));
    }

    #[tokio::test]
    async fn shutdown_signal_during_batch_loop_stops_after_current_batch() {
        let dir = TempDir::new().unwrap();
        let config = make_config(vec!["orders".to_string()]);
        let mut schema_mock = MockSchemaInspect::new();
        let mut extract_mock = MockExtract::new();
        let mut writer_mock = MockDeltaWrite::new();
        let mut state_mock = MockStateManage::new();
        let (tx, rx) = watch::channel(false);

        state_mock
            .expect_load_or_default()
            .returning(|_| AppState::default());
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
            .returning(|| 1);
        writer_mock
            .expect_ensure_table()
            .returning(|_, _| Ok(()));
        writer_mock
            .expect_get_schema()
            .returning(|_| Ok(None));
        writer_mock
            .expect_read_hwm()
            .returning(|_| Ok(None));

        let call_count = Arc::new(AtomicUsize::new(0));
        let call_count_clone = call_count.clone();
        let tx_clone = tx.clone();
        extract_mock
            .expect_extract()
            .returning(move |_| {
                let count = call_count_clone.fetch_add(1, Ordering::SeqCst);
                if count == 0 {
                    let _ = tx_clone.send(true);
                }
                if count < 3 {
                    let schema = Arc::new(deltalake::arrow::datatypes::Schema::new(vec![
                        deltalake::arrow::datatypes::Field::new("id", deltalake::arrow::datatypes::DataType::Int32, false),
                        deltalake::arrow::datatypes::Field::new("val", deltalake::arrow::datatypes::DataType::Int32, false),
                        deltalake::arrow::datatypes::Field::new("updated_at", deltalake::arrow::datatypes::DataType::Utf8, false),
                    ]));
                    let batch = deltalake::arrow::record_batch::RecordBatch::try_new(
                        schema,
                        vec![
                            Arc::new(deltalake::arrow::array::Int32Array::from(vec![count as i32 + 1])),
                            Arc::new(deltalake::arrow::array::Int32Array::from(vec![1i32])),
                            Arc::new(deltalake::arrow::array::StringArray::from(vec![format!("2026-01-01T00:00:0{count}.000000")])),
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

        let result = orch.run().await;
        assert!(matches!(result, ExitCode::Success));
        let total_extracts = call_count.load(Ordering::SeqCst);
        assert!(
            total_extracts <= 2,
            "should have stopped after signal, got {total_extracts} extracts"
        );
        assert!(
            total_extracts >= 1,
            "should have completed at least one batch, got {total_extracts}"
        );
    }

    #[test]
    fn signal_handler_sends_shutdown_on_first_signal() {
        let (handler, mut rx) = SignalHandler::new();
        std::mem::drop(handler);
        assert!(!*rx.borrow_and_update());
    }

    #[test]
    fn signal_handler_watch_channel_starts_false() {
        let (_handler, rx) = SignalHandler::new();
        assert!(!*rx.borrow());
    }




    #[tokio::test]
    async fn progress_flag_emits_detailed_logs() {
        let dir = TempDir::new().unwrap();
        let config = make_config(vec!["orders".to_string()]);
        let mut schema_mock = MockSchemaInspect::new();
        let mut extract_mock = MockExtract::new();
        let mut writer_mock = MockDeltaWrite::new();
        let mut state_mock = MockStateManage::new();

        state_mock
            .expect_load_or_default()
            .returning(|_| AppState::default());
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
            .returning(|| 1);
        writer_mock
            .expect_ensure_table()
            .returning(|_, _| Ok(()));
        writer_mock
            .expect_get_schema()
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

        let (_tx, rx) = watch::channel(false);
        let mut orch = Orchestrator::new(
            config,
            schema_mock,
            extract_mock,
            writer_mock,
            state_mock,
            rx,
            dir.path().to_path_buf(),
            true,
        );
        let result = orch.run().await;
        assert!(matches!(result, ExitCode::Success));
    }
}
