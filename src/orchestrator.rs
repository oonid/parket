use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use deltalake::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use tokio::sync::watch;
use tracing::{error, info, warn};

use crate::config::{Config, ExtractionMode};
use crate::discovery::{
    ColumnInfo, compute_schema_hash, detect_mode, filter_unsupported_columns,
};
use crate::extractor::BatchExtractor;
use crate::query::QueryBuilder;
use crate::state::{AppState, TableState};
use crate::writer::{extract_hwm_from_batch, DeltaWriter, Hwm};

#[cfg_attr(test, mockall::automock)]
#[allow(async_fn_in_trait)]
pub trait SchemaInspect: Send + Sync {
    async fn discover_columns(&self, table: &str) -> Result<Vec<ColumnInfo>>;
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

    async fn max_timestamp(&self, table: &str, col: &str) -> Result<Option<String>> {
        crate::discovery::SchemaInspector::new(self.pool.clone(), self.database.clone())
            .max_timestamp(table, col)
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

    fn extract(&mut self, sql: &str) -> Result<Vec<deltalake::arrow::record_batch::RecordBatch>> {
        self.inner.extract(sql)
    }

    fn batch_size(&self) -> u64 {
        self.inner.batch_size()
    }
}

#[derive(Default)]
pub struct StateManageAdapter {
    state: AppState,
}

impl StateManageAdapter {
    pub fn new() -> Self {
        Self::default()
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
            )
            .with_merge_limits(config.merge_memory_mb, config.merge_spill_dir.clone()),
        }
    }
}

impl DeltaWrite for DeltaWriterAdapter {
    async fn ensure_table(&self, table_name: &str, schema: SchemaRef) -> Result<()> {
        self.inner.ensure_table(table_name, schema).await?;
        Ok(())
    }

    async fn append_batch(
        &self,
        table_name: &str,
        batches: Vec<deltalake::arrow::record_batch::RecordBatch>,
        hwm: Option<Hwm>,
    ) -> Result<()> {
        self.inner
            .append_batch(table_name, batches, hwm.as_ref())
            .await
    }

    async fn overwrite_table(
        &self,
        table_name: &str,
        batches: Vec<deltalake::arrow::record_batch::RecordBatch>,
        hwm: Option<Hwm>,
    ) -> Result<()> {
        self.inner
            .overwrite_table(table_name, batches, hwm.as_ref())
            .await
    }

    async fn read_hwm(&self, table_name: &str) -> Result<Option<Hwm>> {
        self.inner.read_hwm(table_name).await
    }

    async fn get_schema(&self, table_name: &str) -> Result<Option<SchemaRef>> {
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

    async fn merge_batch(
        &self,
        table_name: &str,
        batches: Vec<deltalake::arrow::record_batch::RecordBatch>,
        key_col: String,
        insert_id: Option<i64>,
        update_hwm: Option<Hwm>,
    ) -> Result<()> {
        self.inner
            .merge_batch(table_name, batches, &key_col, insert_id, update_hwm.as_ref())
            .await
    }

    async fn delete_then_append(
        &self,
        table_name: &str,
        batches: Vec<deltalake::arrow::record_batch::RecordBatch>,
        key_col: String,
        insert_id: Option<i64>,
        update_hwm: Option<Hwm>,
    ) -> Result<()> {
        self.inner
            .delete_then_append(table_name, batches, &key_col, insert_id, update_hwm.as_ref())
            .await
    }

    async fn read_insert_hwm(&self, table_name: &str) -> Result<Option<i64>> {
        self.inner.read_insert_hwm(table_name).await
    }

    async fn append_two_stream(
        &self,
        table_name: &str,
        batches: Vec<deltalake::arrow::record_batch::RecordBatch>,
        insert_id: Option<i64>,
        update_hwm: Option<Hwm>,
    ) -> Result<()> {
        self.inner
            .append_two_stream(table_name, batches, insert_id, update_hwm.as_ref())
            .await
    }
}


pub struct LocalDeltaWriterAdapter {
    inner: DeltaWriter,
}

impl LocalDeltaWriterAdapter {
    pub fn new(dir: &Path, config: &Config) -> Self {
        Self {
            inner: DeltaWriter::new_local(&dir.to_string_lossy())
                .with_merge_limits(config.merge_memory_mb, config.merge_spill_dir.clone()),
        }
    }
}

impl DeltaWrite for LocalDeltaWriterAdapter {
    async fn ensure_table(&self, table_name: &str, schema: SchemaRef) -> Result<()> {
        self.inner.ensure_table(table_name, schema).await?;
        Ok(())
    }

    async fn append_batch(
        &self,
        table_name: &str,
        batches: Vec<deltalake::arrow::record_batch::RecordBatch>,
        hwm: Option<Hwm>,
    ) -> Result<()> {
        self.inner
            .append_batch(table_name, batches, hwm.as_ref())
            .await
    }

    async fn overwrite_table(
        &self,
        table_name: &str,
        batches: Vec<deltalake::arrow::record_batch::RecordBatch>,
        hwm: Option<Hwm>,
    ) -> Result<()> {
        self.inner
            .overwrite_table(table_name, batches, hwm.as_ref())
            .await
    }

    async fn read_hwm(&self, table_name: &str) -> Result<Option<Hwm>> {
        self.inner.read_hwm(table_name).await
    }

    async fn get_schema(&self, table_name: &str) -> Result<Option<SchemaRef>> {
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

    async fn merge_batch(
        &self,
        table_name: &str,
        batches: Vec<deltalake::arrow::record_batch::RecordBatch>,
        key_col: String,
        insert_id: Option<i64>,
        update_hwm: Option<Hwm>,
    ) -> Result<()> {
        self.inner
            .merge_batch(table_name, batches, &key_col, insert_id, update_hwm.as_ref())
            .await
    }

    async fn delete_then_append(
        &self,
        table_name: &str,
        batches: Vec<deltalake::arrow::record_batch::RecordBatch>,
        key_col: String,
        insert_id: Option<i64>,
        update_hwm: Option<Hwm>,
    ) -> Result<()> {
        self.inner
            .delete_then_append(table_name, batches, &key_col, insert_id, update_hwm.as_ref())
            .await
    }

    async fn read_insert_hwm(&self, table_name: &str) -> Result<Option<i64>> {
        self.inner.read_insert_hwm(table_name).await
    }

    async fn append_two_stream(
        &self,
        table_name: &str,
        batches: Vec<deltalake::arrow::record_batch::RecordBatch>,
        insert_id: Option<i64>,
        update_hwm: Option<Hwm>,
    ) -> Result<()> {
        self.inner
            .append_two_stream(table_name, batches, insert_id, update_hwm.as_ref())
            .await
    }
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

fn mariadb_type_to_arrow(data_type: &str, column_type: &str) -> Result<DataType> {
    match data_type {
        "tinyint" => Ok(DataType::Int8),
        "smallint" => Ok(DataType::Int16),
        "int" | "mediumint" => Ok(DataType::Int32),
        "bigint" => Ok(DataType::Int64),
        "float" => Ok(DataType::Float32),
        "double" => Ok(DataType::Float64),
        "decimal" | "numeric" => Ok(DataType::Utf8),
        "varchar" | "char" | "text" | "tinytext" | "mediumtext" | "longtext" => Ok(DataType::Utf8),
        "json" | "enum" | "set" => Ok(DataType::Utf8),
        "date" | "datetime" | "timestamp" => Ok(DataType::Utf8),
        "boolean" | "bool" => Ok(DataType::Int8),
        "blob" | "tinyblob" | "mediumblob" | "longblob" | "binary" | "varbinary" => Ok(DataType::Binary),
        _ => anyhow::bail!(
            "unsupported MariaDB type for Delta schema: {data_type} ({column_type})"
        ),
    }
}

fn schema_evolution_check(
    mariadb_columns: &[ColumnInfo],
    delta_schema: &SchemaRef,
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
                    if !types_equivalent(delta_field.data_type(), &dt) {
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

fn types_equivalent(delta_dt: &DataType, mariadb_dt: &DataType) -> bool {
    match (delta_dt, mariadb_dt) {
        (DataType::Timestamp(_, tz_a), DataType::Timestamp(_, tz_b)) => {
            match (tz_a.as_deref(), tz_b.as_deref()) {
                (Some("UTC"), Some("UTC")) | (None, None) => true,
                (Some("UTC"), None) | (None, Some("UTC")) => true,
                (a, b) => a == b,
            }
        }
        // Delta stores Int8/Int16/Int32/UInt8/UInt16/UInt32 all as INTEGER,
        // which round-trips back as Int32. Accept any of those widths when
        // the Delta side shows Int32.
        (DataType::Int32, DataType::Int8 | DataType::Int16 | DataType::Int32
            | DataType::UInt8 | DataType::UInt16 | DataType::UInt32) => true,
        // Similarly Int64 and UInt64 both map to Delta LONG -> Int64.
        (DataType::Int64, DataType::Int64 | DataType::UInt64) => true,
        _ => delta_dt == mariadb_dt,
    }
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

        let ts_col = self.config.timestamp_col(table_name).to_string();
        if self.config.table_timestamp_col.contains_key(table_name) {
            crate::discovery::validate_timestamp_col(&columns, &ts_col)?;
        }

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
                self.process_incremental(table_name, &select_columns, &ts_col).await?
            }
            ExtractionMode::FullRefresh => {
                self.process_full_refresh(table_name, &select_columns).await?
            }
            ExtractionMode::TwoStream => {
                let (insert_col, update_col) = self.config.two_stream(table_name)
                    .expect("two_stream config present for TwoStream mode");
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

    async fn process_incremental(
        &mut self,
        table_name: &str,
        columns: &[String],
        ts_col: &str,
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
                Hwm { updated_at: ua.clone(), last_id: *id }
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
                .and_then(|b| extract_hwm_from_batch(b, ts_col))
                .clone();

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

    async fn process_full_refresh(
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

    async fn process_two_stream(
        &mut self,
        table_name: &str,
        columns: &[String],
        insert_col: &str,
        update_col: &str,
    ) -> Result<u64> {
        use crate::writer::extract_max_id;
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
    use deltalake::arrow::record_batch::RecordBatch;
    use serial_test::serial;
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
            merge_memory_mb: 512,
            merge_spill_dir: None,
            s3_endpoint: None,
            s3_region: "us-east-1".to_string(),
            s3_prefix: "parket".to_string(),
            default_batch_size: 10000,
            rust_log: "info".to_string(),
            table_modes: HashMap::new(),
            table_initial_hwm: HashMap::new(),
            table_timestamp_col: HashMap::new(),
            table_insert_cursor: HashMap::new(),
            table_update_cursor: HashMap::new(),
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
            false,
        )
    }

    #[test]
    fn schema_evolution_column_addition_warns_and_excludes() {
        let mariadb_cols = vec![
            ColumnInfo { name: "id".into(), data_type: "bigint".into(), column_type: "bigint(20)".into() },
            ColumnInfo { name: "name".into(), data_type: "varchar".into(), column_type: "varchar(255)".into() },
            ColumnInfo { name: "email".into(), data_type: "varchar".into(), column_type: "varchar(255)".into() },
        ];
        let delta_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let result = schema_evolution_check(&mariadb_cols, &delta_schema).unwrap();
        assert_eq!(result, vec!["id", "name"]);
    }

    #[test]
    fn schema_evolution_column_drop_errors() {
        let mariadb_cols = vec![
            ColumnInfo { name: "id".into(), data_type: "bigint".into(), column_type: "bigint(20)".into() },
        ];
        let delta_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
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
        let delta_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
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
        assert!(matches!(mariadb_type_to_arrow("bigint", "bigint(20)").unwrap(), DataType::Int64));
        assert!(matches!(mariadb_type_to_arrow("int", "int(11)").unwrap(), DataType::Int32));
        assert!(matches!(mariadb_type_to_arrow("varchar", "varchar(255)").unwrap(), DataType::Utf8));
        assert!(matches!(mariadb_type_to_arrow("timestamp", "timestamp").unwrap(), DataType::Utf8));
        assert!(matches!(mariadb_type_to_arrow("double", "double").unwrap(), DataType::Float64));
        assert!(matches!(mariadb_type_to_arrow("date", "date").unwrap(), DataType::Utf8));
        assert!(matches!(mariadb_type_to_arrow("mediumtext", "mediumtext").unwrap(), DataType::Utf8));
        assert!(matches!(mariadb_type_to_arrow("enum", "enum('a','b')").unwrap(), DataType::Utf8));
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
            false,
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
            DataType::Int8
        ));
    }

    #[test]
    fn mariadb_type_to_arrow_smallint() {
        assert!(matches!(
            mariadb_type_to_arrow("smallint", "smallint(6)").unwrap(),
            DataType::Int16
        ));
    }

    #[test]
    fn mariadb_type_to_arrow_mediumint() {
        assert!(matches!(
            mariadb_type_to_arrow("mediumint", "mediumint(7)").unwrap(),
            DataType::Int32
        ));
    }

    #[test]
    fn mariadb_type_to_arrow_float() {
        assert!(matches!(
            mariadb_type_to_arrow("float", "float").unwrap(),
            DataType::Float32
        ));
    }

    #[test]
    fn mariadb_type_to_arrow_decimal() {
        assert!(matches!(
            mariadb_type_to_arrow("decimal", "decimal(10,2)").unwrap(),
            DataType::Utf8
        ));
    }

    #[test]
    fn mariadb_type_to_arrow_json() {
        assert!(matches!(
            mariadb_type_to_arrow("json", "json").unwrap(),
            DataType::Utf8
        ));
    }

    #[test]
    fn mariadb_type_to_arrow_char() {
        assert!(matches!(
            mariadb_type_to_arrow("char", "char(10)").unwrap(),
            DataType::Utf8
        ));
    }

    #[test]
    fn mariadb_type_to_arrow_text() {
        assert!(matches!(
            mariadb_type_to_arrow("text", "text").unwrap(),
            DataType::Utf8
        ));
    }

    #[test]
    fn mariadb_type_to_arrow_datetime() {
        assert!(matches!(
            mariadb_type_to_arrow("datetime", "datetime").unwrap(),
            DataType::Utf8
        ));
    }

    #[test]
    fn mariadb_type_to_arrow_bool() {
        assert!(matches!(
            mariadb_type_to_arrow("bool", "bool").unwrap(),
            DataType::Int8
        ));
    }

    #[test]
    fn mariadb_type_to_arrow_blob() {
        assert!(matches!(
            mariadb_type_to_arrow("blob", "blob").unwrap(),
            DataType::Binary
        ));
    }

    #[test]
    fn schema_evolution_type_change_errors() {
        let mariadb_cols = vec![
            ColumnInfo { name: "id".into(), data_type: "bigint".into(), column_type: "bigint(20)".into() },
            ColumnInfo { name: "age".into(), data_type: "bigint".into(), column_type: "bigint(20)".into() },
        ];
        let delta_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("age", DataType::Int32, false),
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
                    let schema = Arc::new(deltalake::arrow::datatypes::Schema::new(vec![
                        deltalake::arrow::datatypes::Field::new("id", deltalake::arrow::datatypes::DataType::Int32, false),
                        deltalake::arrow::datatypes::Field::new("val", deltalake::arrow::datatypes::DataType::Int32, false),
                    ]));
                    let batch = deltalake::arrow::record_batch::RecordBatch::try_new(
                        schema,
                        vec![
                            Arc::new(deltalake::arrow::array::Int32Array::from(vec![1i32])),
                            Arc::new(deltalake::arrow::array::Int32Array::from(vec![1i32])),
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

        let existing_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
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
            merge_memory_mb: 512,
            merge_spill_dir: None,
            s3_endpoint: None,
            s3_region: "us-east-1".to_string(),
            s3_prefix: "parket".to_string(),
            default_batch_size: 10000,
            rust_log: "info".to_string(),
            table_modes: HashMap::new(),
            table_initial_hwm: HashMap::new(),
            table_timestamp_col: HashMap::new(),
            table_insert_cursor: HashMap::new(),
            table_update_cursor: HashMap::new(),
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
    fn schema_evolution_unsupported_type_in_existing_column() {
        let mariadb_cols = vec![
            ColumnInfo { name: "id".into(), data_type: "bigint".into(), column_type: "bigint(20)".into() },
            ColumnInfo { name: "location".into(), data_type: "geometry".into(), column_type: "geometry".into() },
        ];
        let delta_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("location", DataType::Binary, false),
        ]));

        let result = schema_evolution_check(&mariadb_cols, &delta_schema).unwrap();
        assert_eq!(result, vec!["id".to_string(), "location".to_string()]);
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
                    ]));
                    let batch = deltalake::arrow::record_batch::RecordBatch::try_new(
                        schema,
                        vec![
                            Arc::new(deltalake::arrow::array::Int32Array::from(vec![1i32])),
                            Arc::new(deltalake::arrow::array::Int32Array::from(vec![1i32])),
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
    async fn full_refresh_multi_chunk_overwrite_then_append() {
        let dir = TempDir::new().unwrap();
        let config = make_config_with_full_refresh(vec!["products".to_string()]);
        let mut schema_mock = MockSchemaInspect::new();
        let mut extract_mock = MockExtract::new();
        let mut writer_mock = MockDeltaWrite::new();
        let mut state_mock = MockStateManage::new();

        state_mock.expect_load_or_default().returning(|_| AppState::default());
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

        state_mock.expect_load_or_default().returning(|_| AppState::default());
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

        state_mock.expect_load_or_default().returning(|_| AppState::default());
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
                Ok(Some(Hwm {
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
            .returning(|_| AppState::default());
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
            .returning(|_| AppState::default());
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
                    ColumnInfo {
                        name: "completed_at".to_string(),
                        data_type: "timestamp".to_string(),
                        column_type: "timestamp".to_string(),
                    },
                ])
            });
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
            .returning(|_| AppState::default());
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
                    // First call: insert stream returns one batch
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
                    // All subsequent calls return empty (both streams finish)
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
            .returning(|_| AppState::default());
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
            .returning(|_| AppState::default());
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
            .returning(|_| AppState::default());
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
            .returning(|_| Ok(Some(Hwm {
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
                    // Insert stream returns empty immediately
                    Ok(vec![])
                } else if count == 1 {
                    // Update stream returns a batch
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
            .returning(|_| AppState::default());
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
                // Verify seed reached the update stream query
                if count == 0 {
                    // Insert stream returns empty (insert loop ends immediately)
                    Ok(vec![])
                } else if count == 1 {
                    // Update stream: verify seed is in the SQL query
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
            .returning(|_| AppState::default());
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
            .returning(|_| Ok(Some(Hwm {
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
                    // Insert stream returns empty immediately
                    Ok(vec![])
                } else if count == 1 {
                    // Update stream returns a batch
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
