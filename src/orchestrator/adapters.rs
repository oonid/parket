use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use deltalake::arrow::datatypes::SchemaRef;

use crate::config::Config;
use crate::discovery::ColumnInfo;
use crate::extractor::BatchExtractor;
use crate::state::{AppState, TableState};
use crate::writer::{DeltaWriter, Hwm};

use super::{DeltaWrite, Extract, SchemaInspect, StateManage};

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
