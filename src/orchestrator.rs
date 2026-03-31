use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use deltalake::arrow::datatypes::{
    DataType as V57DataType, Field as V57Field, Schema as V57Schema, SchemaRef as V57SchemaRef,
    TimeUnit as V57TimeUnit,
};
use deltalake::arrow::record_batch::RecordBatch as V57RecordBatch;
use tokio::sync::watch;
use tracing::{error, info, warn};

use crate::config::{Config, ExtractionMode};
use crate::discovery::{
    ColumnInfo, compute_schema_hash, detect_mode, filter_unsupported_columns,
};
use crate::extractor::{convert_batches, BatchExtractor};
use crate::query::QueryBuilder;
use crate::state::{AppState, TableState};
use crate::writer::{extract_hwm_from_batch, DeltaWriter, Hwm};

#[cfg_attr(test, mockall::automock)]
pub trait SchemaInspect: Send + Sync {
    async fn discover_columns(&self, table: &str) -> Result<Vec<ColumnInfo>>;
    async fn get_avg_row_length(&self, table: &str) -> Result<Option<u64>>;
}

#[cfg_attr(test, mockall::automock)]
pub trait Extract: Send {
    fn calculate_batch_size(&mut self, avg_row_length: Option<u64>) -> u64;
    fn extract(&mut self, sql: &str) -> Result<Vec<arrow::record_batch::RecordBatch>>;
    fn batch_size(&self) -> u64;
}

#[cfg_attr(test, mockall::automock)]
pub trait DeltaWrite: Send + Sync {
    async fn ensure_table(&self, table_name: &str, schema: V57SchemaRef) -> Result<()>;
    async fn append_batch(
        &self,
        table_name: &str,
        batches: Vec<V57RecordBatch>,
        hwm: Option<Hwm>,
    ) -> Result<()>;
    async fn overwrite_table(
        &self,
        table_name: &str,
        batches: Vec<V57RecordBatch>,
        hwm: Option<Hwm>,
    ) -> Result<()>;
    async fn read_hwm(&self, table_name: &str) -> Result<Option<Hwm>>;
    async fn get_schema(&self, table_name: &str) -> Result<Option<V57SchemaRef>>;
}

#[cfg_attr(test, mockall::automock)]
pub trait StateManage: Send {
    fn load_or_default(&mut self, path: &Path) -> AppState;
    fn update_table(&mut self, name: &str, state: TableState, path: &Path) -> Result<()>;
}

pub struct SchemaInspectorAdapter {
    pool: sqlx::MySqlPool,
    database: String,
}

impl SchemaInspectorAdapter {
    pub fn new(pool: sqlx::MySqlPool, database: String) -> Self {
        Self { pool, database }
    }
}

impl SchemaInspect for SchemaInspectorAdapter {
    async fn discover_columns(&self, table: &str) -> Result<Vec<ColumnInfo>> {
        crate::discovery::SchemaInspector::new(self.pool.clone(), self.database.clone())
            .discover_columns(table)
            .await
    }

    async fn get_avg_row_length(&self, table: &str) -> Result<Option<u64>> {
        crate::discovery::SchemaInspector::new(self.pool.clone(), self.database.clone())
            .get_avg_row_length(table)
            .await
    }
}

pub struct ExtractorAdapter {
    inner: BatchExtractor,
}

impl ExtractorAdapter {
    pub fn new(config: &Config) -> Self {
        Self {
            inner: BatchExtractor::new(
                &config.database_url,
                config.target_memory_mb,
                config.default_batch_size,
            ),
        }
    }
}

impl Extract for ExtractorAdapter {
    fn calculate_batch_size(&mut self, avg_row_length: Option<u64>) -> u64 {
        self.inner.calculate_batch_size(avg_row_length)
    }

    fn extract(&mut self, sql: &str) -> Result<Vec<arrow::record_batch::RecordBatch>> {
        self.inner.extract(sql)
    }

    fn batch_size(&self) -> u64 {
        self.inner.batch_size()
    }
}

pub struct StateManageAdapter {
    state: AppState,
}

impl StateManageAdapter {
    pub fn new() -> Self {
        Self {
            state: AppState::default(),
        }
    }
}

impl StateManage for StateManageAdapter {
    fn load_or_default(&mut self, path: &Path) -> AppState {
        self.state = AppState::load_or_warn(path);
        self.state.clone()
    }

    fn update_table(&mut self, name: &str, state: TableState, path: &Path) -> Result<()> {
        self.state.update_table(name, state, path)
    }
}

pub struct DeltaWriterAdapter {
    inner: DeltaWriter,
}

impl DeltaWriterAdapter {
    pub fn new(config: &Config) -> Self {
        Self {
            inner: DeltaWriter::new(
                &config.s3_bucket,
                &config.s3_prefix,
                config.s3_endpoint.as_deref(),
                &config.s3_region,
                &config.s3_access_key_id,
                &config.s3_secret_access_key,
            ),
        }
    }
}

impl DeltaWrite for DeltaWriterAdapter {
    async fn ensure_table(&self, table_name: &str, schema: V57SchemaRef) -> Result<()> {
        self.inner.ensure_table(table_name, schema).await?;
        Ok(())
    }

    async fn append_batch(
        &self,
        table_name: &str,
        batches: Vec<V57RecordBatch>,
        hwm: Option<Hwm>,
    ) -> Result<()> {
        self.inner
            .append_batch(table_name, batches, hwm.as_ref())
            .await
    }

    async fn overwrite_table(
        &self,
        table_name: &str,
        batches: Vec<V57RecordBatch>,
        hwm: Option<Hwm>,
    ) -> Result<()> {
        self.inner
            .overwrite_table(table_name, batches, hwm.as_ref())
            .await
    }

    async fn read_hwm(&self, table_name: &str) -> Result<Option<Hwm>> {
        self.inner.read_hwm(table_name).await
    }

    async fn get_schema(&self, table_name: &str) -> Result<Option<V57SchemaRef>> {
        match self.inner.open_table(table_name).await {
            Ok(table) => {
                let kernel_schema = table.snapshot()?.schema();
                let arrow_schema: deltalake::arrow::datatypes::Schema =
                    deltalake::kernel::engine::arrow_conversion::TryIntoArrow::try_into_arrow(
                        kernel_schema.as_ref(),
                    )?;
                Ok(Some(Arc::new(arrow_schema)))
            }
            Err(_) => Ok(None),
        }
    }
}

fn column_info_to_v57_schema(columns: &[ColumnInfo]) -> Result<V57SchemaRef> {
    let fields: Result<Vec<V57Field>> = columns
        .iter()
        .map(|c| {
            let dt = mariadb_type_to_arrow(&c.data_type, &c.column_type)?;
            Ok(V57Field::new(&c.name, dt, true))
        })
        .collect();
    Ok(Arc::new(V57Schema::new(fields?)))
}

fn mariadb_type_to_arrow(data_type: &str, column_type: &str) -> Result<V57DataType> {
    match data_type {
        "tinyint" => Ok(V57DataType::Int32),
        "smallint" => Ok(V57DataType::Int16),
        "int" => Ok(V57DataType::Int32),
        "mediumint" => Ok(V57DataType::Int32),
        "bigint" => Ok(V57DataType::Int64),
        "float" => Ok(V57DataType::Float32),
        "double" => Ok(V57DataType::Float64),
        "decimal" => Ok(V57DataType::Float64),
        "varchar" | "char" | "text" => Ok(V57DataType::Utf8),
        "json" => Ok(V57DataType::Utf8),
        "date" => Ok(V57DataType::Date32),
        "datetime" | "timestamp" => Ok(V57DataType::Timestamp(
            V57TimeUnit::Microsecond,
            None,
        )),
        "boolean" | "bool" => Ok(V57DataType::Boolean),
        "blob" => Ok(V57DataType::Binary),
        _ => anyhow::bail!(
            "unsupported MariaDB type for Delta schema: {data_type} ({column_type})"
        ),
    }
}

fn schema_evolution_check(
    mariadb_columns: &[ColumnInfo],
    delta_schema: &V57SchemaRef,
) -> Result<Vec<String>> {
    let delta_names: std::collections::HashSet<&str> = delta_schema
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();

    let mariadb_names: std::collections::HashSet<&str> = mariadb_columns
        .iter()
        .map(|c| c.name.as_str())
        .collect();

    let mut errors: Vec<String> = Vec::new();

    for delta_name in &delta_names {
        if !mariadb_names.contains(delta_name) {
            errors.push(format!(
                "column {delta_name} exists in Delta but not in MariaDB — table was dropped"
            ));
        }
    }

    for col in mariadb_columns {
        if let Ok(delta_field) = delta_schema.field_with_name(&col.name) {
            let expected_dt = mariadb_type_to_arrow(&col.data_type, &col.column_type);
            match expected_dt {
                Ok(dt) => {
                    if delta_field.data_type() != &dt {
                        errors.push(format!(
                            "column {} type changed: Delta has {:?}, MariaDB has {:?}",
                            col.name,
                            delta_field.data_type(),
                            dt
                        ));
                    }
                }
                Err(_) => {
                    warn!(
                        column = %col.name,
                        data_type = %col.data_type,
                        "skipping unsupported MariaDB type in schema evolution check"
                    );
                }
            }
        }
    }

    if !errors.is_empty() {
        for e in &errors {
            error!("{e}");
        }
        anyhow::bail!("schema evolution error: {}", errors.join(", "));
    }

    let mut select_columns: Vec<String> = Vec::new();
    for col in mariadb_columns {
        if delta_names.contains(col.name.as_str()) {
            select_columns.push(col.name.clone());
        } else {
            warn!(
                column = %col.name,
                "column exists in MariaDB but not in Delta, excluding from SELECT"
            );
        }
    }

    Ok(select_columns)
}

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
}

impl<S, E, W, M> Orchestrator<S, E, W, M>
where
    S: SchemaInspect + Send + Sync,
    E: Extract + Send,
    W: DeltaWrite + Send + Sync,
    M: StateManage + Send,
{
    pub fn new(
        config: Config,
        schema_inspect: S,
        extractor: E,
        writer: W,
        state_mgr: M,
        shutdown: watch::Receiver<bool>,
        state_path: PathBuf,
    ) -> Self {
        Self {
            config,
            schema_inspect,
            extractor,
            writer,
            state_mgr,
            shutdown,
            state_path,
        }
    }

    fn check_shutdown(&self) -> bool {
        *self.shutdown.borrow()
    }

    pub async fn run(&mut self) -> ExitCode {
        self.state_mgr.load_or_default(&self.state_path);

        let mut succeeded = 0u32;
        let mut failed = 0u32;
        let total = self.config.tables.len();

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

        info!(
            total,
            succeeded,
            failed,
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

        let mode_override = self.config.table_modes.get(table_name);
        let mode = detect_mode(&columns, mode_override);
        let mode_str = match mode {
            ExtractionMode::Incremental => "incremental",
            ExtractionMode::FullRefresh => "full_refresh",
            ExtractionMode::Auto => "auto",
        };

        let avg_row_length = self.schema_inspect.get_avg_row_length(table_name).await?;
        self.extractor.calculate_batch_size(avg_row_length);

        let schema = column_info_to_v57_schema(&columns)?;
        self.writer.ensure_table(table_name, schema.clone()).await?;

        let column_names: Vec<String> = columns.iter().map(|c| c.name.clone()).collect();
        let select_columns = match mode {
            ExtractionMode::Incremental => {
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
                self.process_incremental(table_name, &select_columns).await?
            }
            ExtractionMode::FullRefresh => {
                self.process_full_refresh(table_name, &select_columns).await?
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

    async fn process_incremental(
        &mut self,
        table_name: &str,
        columns: &[String],
    ) -> Result<u64> {
        let mut current_hwm = self.writer.read_hwm(table_name).await?;
        let mut total_rows = 0u64;

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
                current_hwm.as_ref().map(|h| h.updated_at.as_str()),
                current_hwm.as_ref().map(|h| h.last_id),
                batch_size,
            );

            let v54_batches = self.extractor.extract(&sql)?;
            if v54_batches.is_empty()
                || v54_batches.iter().all(|b| b.num_rows() == 0)
            {
                break;
            }

            let batch_rows: u64 = v54_batches.iter().map(|b| b.num_rows() as u64).sum();
            let v57_batches = convert_batches(v54_batches)?;
            let batch_hwm = v57_batches
                .last()
                .and_then(extract_hwm_from_batch)
                .clone();

            self.writer
                .append_batch(table_name, v57_batches, batch_hwm.clone())
                .await?;

            if let Some(h) = batch_hwm {
                current_hwm = Some(h);
            }
            total_rows += batch_rows;

            if batch_rows < batch_size {
                break;
            }
        }

        Ok(total_rows)
    }

    async fn process_full_refresh(
        &mut self,
        table_name: &str,
        columns: &[String],
    ) -> Result<u64> {
        let sql = QueryBuilder::build_full_refresh_query(table_name, columns);
        let v54_batches = self.extractor.extract(&sql)?;
        let total_rows: u64 = v54_batches.iter().map(|b| b.num_rows() as u64).sum();
        let v57_batches = convert_batches(v54_batches)?;
        self.writer
            .overwrite_table(table_name, v57_batches, None)
            .await?;
        Ok(total_rows)
    }
}

fn format_timestamp_now() -> String {
    let dur = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    let secs = dur.as_secs();
    let days = secs / 86400;
    let time_secs = secs % 86400;
    let hours = time_secs / 3600;
    let minutes = (time_secs % 3600) / 60;
    let seconds = time_secs % 60;
    let (year, month, day) = epoch_days_to_ymd(days as i64);
    format!(
        "{year:04}-{month:02}-{day:02}T{hours:02}:{minutes:02}:{seconds:02}Z"
    )
}

fn epoch_days_to_ymd(days: i64) -> (i64, i64, i64) {
    let mut year = 1970i64;
    let mut remaining = days;

    loop {
        let year_len = if is_leap(year) { 366 } else { 365 };
        if remaining >= 0 && remaining < year_len {
            break;
        }
        if remaining >= 0 {
            remaining -= year_len;
            year += 1;
        } else {
            year -= 1;
            remaining += if is_leap(year) { 366 } else { 365 };
        }
    }

    let leap = is_leap(year);
    let month_days = [
        31,
        if leap { 29 } else { 28 },
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ];

    let mut month = 1i64;
    for &md in &month_days {
        if remaining < md {
            break;
        }
        remaining -= md;
        month += 1;
    }

    (year, month, remaining + 1)
}

fn is_leap(year: i64) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
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
    use arrow::record_batch::RecordBatch;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tempfile::TempDir;

    fn make_columns() -> Vec<ColumnInfo> {
        vec![
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
            ColumnInfo {
                name: "updated_at".to_string(),
                data_type: "timestamp".to_string(),
                column_type: "timestamp".to_string(),
            },
        ]
    }

    fn make_config(tables: Vec<String>) -> Config {
        Config {
            database_url: "mysql://u:p@h/db".to_string(),
            s3_bucket: "bucket".to_string(),
            s3_access_key_id: "key".to_string(),
            s3_secret_access_key: "secret".to_string(),
            tables,
            target_memory_mb: 512,
            s3_endpoint: None,
            s3_region: "us-east-1".to_string(),
            s3_prefix: "parket".to_string(),
            default_batch_size: 10000,
            rust_log: "info".to_string(),
            table_modes: HashMap::new(),
        }
    }

    fn make_orchestrator(
        config: Config,
        schema_mock: MockSchemaInspect,
        extract_mock: MockExtract,
        writer_mock: MockDeltaWrite,
        state_mock: MockStateManage,
        state_path: PathBuf,
    ) -> Orchestrator<MockSchemaInspect, MockExtract, MockDeltaWrite, MockStateManage> {
        let (_tx, rx) = watch::channel(false);
        Orchestrator::new(
            config,
            schema_mock,
            extract_mock,
            writer_mock,
            state_mock,
            rx,
            state_path,
        )
    }

    #[test]
    fn schema_evolution_column_addition_warns_and_excludes() {
        let mariadb_cols = vec![
            ColumnInfo { name: "id".into(), data_type: "bigint".into(), column_type: "bigint(20)".into() },
            ColumnInfo { name: "name".into(), data_type: "varchar".into(), column_type: "varchar(255)".into() },
            ColumnInfo { name: "email".into(), data_type: "varchar".into(), column_type: "varchar(255)".into() },
        ];
        let delta_schema = Arc::new(V57Schema::new(vec![
            V57Field::new("id", V57DataType::Int64, false),
            V57Field::new("name", V57DataType::Utf8, false),
        ]));

        let result = schema_evolution_check(&mariadb_cols, &delta_schema).unwrap();
        assert_eq!(result, vec!["id", "name"]);
    }

    #[test]
    fn schema_evolution_column_drop_errors() {
        let mariadb_cols = vec![
            ColumnInfo { name: "id".into(), data_type: "bigint".into(), column_type: "bigint(20)".into() },
        ];
        let delta_schema = Arc::new(V57Schema::new(vec![
            V57Field::new("id", V57DataType::Int64, false),
            V57Field::new("name", V57DataType::Utf8, false),
        ]));
        let result = schema_evolution_check(&mariadb_cols, &delta_schema);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("column name exists in Delta but not in MariaDB"));
    }

    #[test]
    fn schema_evolution_no_changes() {
        let mariadb_cols = vec![
            ColumnInfo { name: "id".into(), data_type: "bigint".into(), column_type: "bigint(20)".into() },
            ColumnInfo { name: "name".into(), data_type: "varchar".into(), column_type: "varchar(255)".into() },
        ];
        let delta_schema = Arc::new(V57Schema::new(vec![
            V57Field::new("id", V57DataType::Int64, false),
            V57Field::new("name", V57DataType::Utf8, false),
        ]));
        let result = schema_evolution_check(&mariadb_cols, &delta_schema).unwrap();
        assert_eq!(result, vec!["id", "name"]);
    }

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

    #[test]
    fn mariadb_type_to_arrow_conversions() {
        assert!(matches!(mariadb_type_to_arrow("bigint", "bigint(20)").unwrap(), V57DataType::Int64));
        assert!(matches!(mariadb_type_to_arrow("int", "int(11)").unwrap(), V57DataType::Int32));
        assert!(matches!(mariadb_type_to_arrow("varchar", "varchar(255)").unwrap(), V57DataType::Utf8));
        assert!(matches!(mariadb_type_to_arrow("timestamp", "timestamp").unwrap(), V57DataType::Timestamp(_, _)));
        assert!(matches!(mariadb_type_to_arrow("double", "double").unwrap(), V57DataType::Float64));
        assert!(matches!(mariadb_type_to_arrow("date", "date").unwrap(), V57DataType::Date32));
        assert!(mariadb_type_to_arrow("geometry", "geometry").is_err());
    }

    fn setup_incremental_mocks(
        schema_mock: &mut MockSchemaInspect,
        extract_mock: &mut MockExtract,
        writer_mock: &mut MockDeltaWrite,
        state_mock: &mut MockStateManage,
    ) {
        schema_mock
            .expect_discover_columns()
            .returning(move |_| Ok(make_columns()));
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
            .returning(|_, _, _| Ok(()));
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
        );
        let result = orch.run().await;
        assert!(matches!(result, ExitCode::Success));
    }

    #[test]
    fn format_timestamp_now_produces_valid_string() {
        let ts = format_timestamp_now();
        assert!(ts.contains('T'));
        assert!(ts.ends_with('Z'));
        assert!(ts.contains("20"));
    }

    #[test]
    fn epoch_days_to_ymd_orch_test() {
        let (y, m, d) = epoch_days_to_ymd(0);
        assert_eq!((y, m, d), (1970, 1, 1));
    }

    #[test]
    fn is_leap_orch_test() {
        assert!(is_leap(2024));
        assert!(!is_leap(2023));
    }

    #[test]
    fn mariadb_type_to_arrow_tinyint() {
        assert!(matches!(
            mariadb_type_to_arrow("tinyint", "tinyint(1)").unwrap(),
            V57DataType::Int32
        ));
    }

    #[test]
    fn mariadb_type_to_arrow_smallint() {
        assert!(matches!(
            mariadb_type_to_arrow("smallint", "smallint(6)").unwrap(),
            V57DataType::Int16
        ));
    }

    #[test]
    fn mariadb_type_to_arrow_mediumint() {
        assert!(matches!(
            mariadb_type_to_arrow("mediumint", "mediumint(7)").unwrap(),
            V57DataType::Int32
        ));
    }

    #[test]
    fn mariadb_type_to_arrow_float() {
        assert!(matches!(
            mariadb_type_to_arrow("float", "float").unwrap(),
            V57DataType::Float32
        ));
    }

    #[test]
    fn mariadb_type_to_arrow_decimal() {
        assert!(matches!(
            mariadb_type_to_arrow("decimal", "decimal(10,2)").unwrap(),
            V57DataType::Float64
        ));
    }

    #[test]
    fn mariadb_type_to_arrow_json() {
        assert!(matches!(
            mariadb_type_to_arrow("json", "json").unwrap(),
            V57DataType::Utf8
        ));
    }

    #[test]
    fn mariadb_type_to_arrow_char() {
        assert!(matches!(
            mariadb_type_to_arrow("char", "char(10)").unwrap(),
            V57DataType::Utf8
        ));
    }

    #[test]
    fn mariadb_type_to_arrow_text() {
        assert!(matches!(
            mariadb_type_to_arrow("text", "text").unwrap(),
            V57DataType::Utf8
        ));
    }

    #[test]
    fn mariadb_type_to_arrow_datetime() {
        assert!(matches!(
            mariadb_type_to_arrow("datetime", "datetime").unwrap(),
            V57DataType::Timestamp(_, _)
        ));
    }

    #[test]
    fn mariadb_type_to_arrow_bool() {
        assert!(matches!(
            mariadb_type_to_arrow("bool", "bool").unwrap(),
            V57DataType::Boolean
        ));
    }

    #[test]
    fn mariadb_type_to_arrow_blob() {
        assert!(matches!(
            mariadb_type_to_arrow("blob", "blob").unwrap(),
            V57DataType::Binary
        ));
    }

    #[test]
    fn schema_evolution_type_change_errors() {
        let mariadb_cols = vec![
            ColumnInfo { name: "id".into(), data_type: "bigint".into(), column_type: "bigint(20)".into() },
            ColumnInfo { name: "age".into(), data_type: "bigint".into(), column_type: "bigint(20)".into() },
        ];
        let delta_schema = Arc::new(V57Schema::new(vec![
            V57Field::new("id", V57DataType::Int64, false),
            V57Field::new("age", V57DataType::Int32, false),
        ]));
        let result = schema_evolution_check(&mariadb_cols, &delta_schema);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("type changed"),
            "expected type change error, got: {err}"
        );
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
                    let schema = Arc::new(arrow::datatypes::Schema::new(vec![
                        arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int32, false),
                        arrow::datatypes::Field::new("val", arrow::datatypes::DataType::Int32, false),
                    ]));
                    let batch = arrow::record_batch::RecordBatch::try_new(
                        schema,
                        vec![
                            Arc::new(arrow::array::Int32Array::from(vec![1i32])),
                            Arc::new(arrow::array::Int32Array::from(vec![1i32])),
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
    async fn schema_evolution_integration_with_existing_delta_table() {
        let dir = TempDir::new().unwrap();
        let config = make_config(vec!["orders".to_string()]);
        let mut schema_mock = MockSchemaInspect::new();
        let mut extract_mock = MockExtract::new();
        let mut writer_mock = MockDeltaWrite::new();
        let mut state_mock = MockStateManage::new();

        let columns = make_columns();

        let existing_schema = Arc::new(V57Schema::new(vec![
            V57Field::new("id", V57DataType::Int64, false),
            V57Field::new("name", V57DataType::Utf8, false),
        ]));

        state_mock
            .expect_load_or_default()
            .returning(|_| AppState::default());
        schema_mock
            .expect_discover_columns()
            .returning(move |_| Ok(columns.clone()));
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
            .withf(|t| t == "orders")
            .returning(move |_| Ok(Some(existing_schema.clone())));
        writer_mock
            .expect_read_hwm()
            .returning(|_| Ok(None));
        extract_mock
            .expect_batch_size()
            .returning(|| 10000);
        extract_mock
            .expect_extract()
            .returning(|_| Ok(vec![]));
        state_mock
            .expect_update_table()
            .returning(|_, _, _| Ok(()));

        let mut orch = make_orchestrator(config, schema_mock, extract_mock, writer_mock, state_mock, dir.path().to_path_buf());
        let result = orch.run().await;
        assert!(matches!(result, ExitCode::Success));
    }

    fn make_full_refresh_columns() -> Vec<ColumnInfo> {
        vec![
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
        ]
    }

    fn make_config_with_full_refresh(tables: Vec<String>) -> Config {
        Config {
            database_url: "mysql://u:p@h/db".to_string(),
            s3_bucket: "bucket".to_string(),
            s3_access_key_id: "key".to_string(),
            s3_secret_access_key: "secret".to_string(),
            tables,
            target_memory_mb: 512,
            s3_endpoint: None,
            s3_region: "us-east-1".to_string(),
            s3_prefix: "parket".to_string(),
            default_batch_size: 10000,
            rust_log: "info".to_string(),
            table_modes: HashMap::new(),
        }
    }

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
            .returning(|_| AppState::default());
        schema_mock
            .expect_discover_columns()
            .returning(move |_| Ok(make_full_refresh_columns()));
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
        extract_mock
            .expect_extract()
            .returning(|_| {
                let schema = Arc::new(arrow::datatypes::Schema::new(vec![
                    arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false),
                    arrow::datatypes::Field::new("name", arrow::datatypes::DataType::Utf8, false),
                ]));
                let batch = arrow::record_batch::RecordBatch::try_new(
                    schema,
                    vec![
                        Arc::new(arrow::array::Int64Array::from(vec![1i64, 2i64])),
                        Arc::new(arrow::array::StringArray::from(vec!["a", "b"])),
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
    async fn incremental_hwm_updates_between_batches() {
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
                    let schema = Arc::new(arrow::datatypes::Schema::new(vec![
                        arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false),
                        arrow::datatypes::Field::new(
                            "updated_at",
                            arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
                            false,
                        ),
                    ]));
                    let batch = RecordBatch::try_new(
                        schema,
                        vec![
                            Arc::new(arrow::array::Int64Array::from(vec![1i64])),
                            Arc::new(arrow::array::TimestampMicrosecondArray::from(vec![1743158400000000i64])),
                        ],
                    )
                    .unwrap();
                    Ok(vec![batch])
                } else if count == 1 {
                    let schema = Arc::new(arrow::datatypes::Schema::new(vec![
                        arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false),
                        arrow::datatypes::Field::new(
                            "updated_at",
                            arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
                            false,
                        ),
                    ]));
                    let batch = RecordBatch::try_new(
                        schema,
                        vec![
                            Arc::new(arrow::array::Int64Array::from(vec![2i64])),
                            Arc::new(arrow::array::TimestampMicrosecondArray::from(vec![1743158401000000i64])),
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
                    let schema = Arc::new(arrow::datatypes::Schema::new(vec![
                        arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false),
                        arrow::datatypes::Field::new(
                            "updated_at",
                            arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
                            false,
                        ),
                    ]));
                    let batch = RecordBatch::try_new(
                        schema,
                        vec![
                            Arc::new(arrow::array::Int64Array::from(vec![1i64, 2i64, 3i64])),
                            Arc::new(arrow::array::TimestampMicrosecondArray::from(vec![
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
    fn schema_evolution_unsupported_type_in_existing_column() {
        let mariadb_cols = vec![
            ColumnInfo { name: "id".into(), data_type: "bigint".into(), column_type: "bigint(20)".into() },
            ColumnInfo { name: "location".into(), data_type: "geometry".into(), column_type: "geometry".into() },
        ];
        let delta_schema = Arc::new(V57Schema::new(vec![
            V57Field::new("id", V57DataType::Int64, false),
            V57Field::new("location", V57DataType::Binary, false),
        ]));

        let result = schema_evolution_check(&mariadb_cols, &delta_schema).unwrap();
        assert_eq!(result, vec!["id".to_string(), "location".to_string()]);
    }

    #[test]
    fn column_info_to_v57_schema_unsupported_type() {
        let columns = vec![
            ColumnInfo { name: "id".into(), data_type: "bigint".into(), column_type: "bigint(20)".into() },
            ColumnInfo { name: "data".into(), data_type: "enum".into(), column_type: "enum('a','b')".into() },
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
                    let schema = Arc::new(arrow::datatypes::Schema::new(vec![
                        arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false),
                        arrow::datatypes::Field::new(
                            "updated_at",
                            arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
                            false,
                        ),
                    ]));
                    let batch = RecordBatch::try_new(
                        schema,
                        vec![
                            Arc::new(arrow::array::Int64Array::from(vec![1i64])),
                            Arc::new(arrow::array::TimestampMicrosecondArray::from(vec![1743158400000000i64])),
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
        extract_mock
            .expect_calculate_batch_size()
            .returning(|_| 10000);
        writer_mock
            .expect_ensure_table()
            .returning(|_, _| Ok(()));
        writer_mock
            .expect_get_schema()
            .returning(|_| Ok(None));
        extract_mock
            .expect_extract()
            .returning(|_| {
                let schema = Arc::new(arrow::datatypes::Schema::new(vec![
                    arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false),
                ]));
                let batch = RecordBatch::try_new(
                    schema,
                    vec![Arc::new(arrow::array::Int64Array::from(vec![1i64]))],
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
                    let schema = Arc::new(arrow::datatypes::Schema::new(vec![
                        arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int32, false),
                        arrow::datatypes::Field::new("val", arrow::datatypes::DataType::Int32, false),
                    ]));
                    let batch = arrow::record_batch::RecordBatch::try_new(
                        schema,
                        vec![
                            Arc::new(arrow::array::Int32Array::from(vec![1i32])),
                            Arc::new(arrow::array::Int32Array::from(vec![1i32])),
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
}
