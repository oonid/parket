use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use deltalake::arrow::datatypes::SchemaRef;

use crate::config::Config;
use crate::discovery::{ColumnInfo, IndexInfo};
use crate::extractor::BatchExtractor;
use crate::state::{AppState, TableState};
use crate::writer::{is_missing_table_error, DeltaWriter, Hwm};

use super::{DeltaWrite, Extract, SchemaInspect, StateManage};

/// O6: shared `get_schema` body for both the S3 and local Delta writer adapters (both wrap
/// the same `DeltaWriter`). `open_table` failing must NOT be blanket-collapsed to "no schema
/// yet" — a transient error (S3 hiccup, auth blip) silently disables the schema-evolution
/// guard (an R1-class recurrence). Only a genuinely missing table is `Ok(None)`; anything
/// else propagates with context.
async fn get_schema_impl(inner: &DeltaWriter, table_name: &str) -> Result<Option<SchemaRef>> {
    match inner.open_table(table_name).await {
        Ok(table) => {
            let kernel_schema = table.snapshot()?.schema();
            let arrow_schema: deltalake::arrow::datatypes::Schema =
                deltalake::kernel::engine::arrow_conversion::TryIntoArrow::try_into_arrow(
                    kernel_schema.as_ref(),
                )?;
            Ok(Some(Arc::new(arrow_schema)))
        }
        Err(e) if is_missing_table_error(&e) => Ok(None),
        Err(e) => Err(e).context(format!(
            "get_schema: could not open Delta table `{table_name}` (not treating a transient error as missing)"
        )),
    }
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

    async fn discover_indexes(&self, table: &str) -> Result<Vec<IndexInfo>> {
        crate::discovery::SchemaInspector::new(self.pool.clone(), self.database.clone())
            .discover_indexes(table)
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

    fn extract(&mut self, sql: &str) -> Result<crate::extractor::Extraction> {
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

    async fn has_data(&self, table_name: &str) -> Result<bool> {
        self.inner.has_data(table_name).await
    }

    async fn get_schema(&self, table_name: &str) -> Result<Option<SchemaRef>> {
        get_schema_impl(&self.inner, table_name).await
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

    async fn has_data(&self, table_name: &str) -> Result<bool> {
        self.inner.has_data(table_name).await
    }

    async fn get_schema(&self, table_name: &str) -> Result<Option<SchemaRef>> {
        get_schema_impl(&self.inner, table_name).await
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::orchestrator::test_support::make_config;
    use deltalake::arrow::array::{Int64Array, StringArray};
    use deltalake::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use deltalake::arrow::record_batch::RecordBatch;

    fn make_schema() -> SchemaRef {
        Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Utf8, false),
        ]))
    }

    fn make_batch(schema: SchemaRef, ids: Vec<i64>, values: Vec<&str>) -> RecordBatch {
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(values)),
            ],
        )
        .unwrap()
    }

    /// O6: exercises the full `LocalDeltaWriterAdapter` surface end-to-end against a real
    /// local Delta table (no network), including both `get_schema_impl` branches — an
    /// existing table (`Ok(Some(..))`) and a genuinely missing one (`Ok(None)`).
    #[tokio::test]
    async fn local_delta_writer_adapter_full_lifecycle() {
        let temp = tempfile::tempdir().unwrap();
        let config = make_config(vec![]);
        let adapter = LocalDeltaWriterAdapter::new(temp.path(), &config);
        let schema = make_schema();

        adapter.ensure_table("t", schema.clone()).await.unwrap();

        let existing_schema = adapter.get_schema("t").await.unwrap();
        assert!(existing_schema.is_some());

        let missing_schema = adapter.get_schema("does_not_exist").await.unwrap();
        assert!(missing_schema.is_none());

        let hwm = Hwm {
            updated_at: "2024-01-01 00:00:00".to_string(),
            last_id: 1,
        };
        adapter
            .append_batch(
                "t",
                vec![make_batch(schema.clone(), vec![1, 2], vec!["a", "b"])],
                Some(hwm),
            )
            .await
            .unwrap();

        let read_back = adapter.read_hwm("t").await.unwrap();
        assert_eq!(read_back.map(|h| h.last_id), Some(1));

        adapter
            .merge_batch(
                "t",
                vec![make_batch(schema.clone(), vec![1, 3], vec!["A", "c"])],
                "id".to_string(),
                Some(3),
                None,
            )
            .await
            .unwrap();

        adapter
            .delete_then_append(
                "t",
                vec![make_batch(schema.clone(), vec![2], vec!["B"])],
                "id".to_string(),
                Some(4),
                None,
            )
            .await
            .unwrap();

        let hwm2 = Hwm {
            updated_at: "2024-01-02 00:00:00".to_string(),
            last_id: 4,
        };
        adapter
            .append_two_stream(
                "t",
                vec![make_batch(schema.clone(), vec![5], vec!["e"])],
                Some(5),
                Some(hwm2),
            )
            .await
            .unwrap();

        let insert_hwm = adapter.read_insert_hwm("t").await.unwrap();
        assert_eq!(insert_hwm, Some(5));

        adapter
            .overwrite_table(
                "t",
                vec![make_batch(schema.clone(), vec![9], vec!["z"])],
                None,
            )
            .await
            .unwrap();
    }

    /// O6: `DeltaWriterAdapter` (S3-backed) delegates to the same `DeltaWriter` used by
    /// `LocalDeltaWriterAdapter`. Pointing at an unroutable endpoint (mirrors the
    /// `ensure_table_s3_connection_error` / `read_hwm_s3_error_propagates` pattern in
    /// `writer.rs`) exercises every delegation without needing real S3 infrastructure: the
    /// empty-batch calls short-circuit to `Ok(())` before touching the network, and the
    /// others fail fast with a connection error (including the `get_schema` "not a missing
    /// table" context-wrapping branch).
    #[tokio::test]
    async fn delta_writer_adapter_propagates_s3_connection_errors() {
        let mut config = make_config(vec![]);
        config.s3_endpoint = Some("http://localhost:1".to_string());
        config.s3_bucket = "nonexistent-bucket".to_string();
        let adapter = DeltaWriterAdapter::new(&config);
        let schema = make_schema();

        assert!(adapter.ensure_table("t", schema.clone()).await.is_err());
        assert!(adapter.append_batch("t", vec![], None).await.is_ok());
        assert!(adapter.overwrite_table("t", vec![], None).await.is_ok());
        assert!(adapter.read_hwm("t").await.is_err());
        assert!(adapter.get_schema("t").await.is_err());
        assert!(
            adapter
                .merge_batch("t", vec![], "id".to_string(), None, None)
                .await
                .is_ok()
        );
        assert!(
            adapter
                .delete_then_append("t", vec![], "id".to_string(), None, None)
                .await
                .is_ok()
        );
        assert!(adapter.read_insert_hwm("t").await.is_err());
        assert!(
            adapter
                .append_two_stream("t", vec![], None, None)
                .await
                .is_ok()
        );
    }

    /// O6: `SchemaInspectorAdapter` is a thin delegation to `discovery::SchemaInspector`.
    /// A lazy pool pointed at an unroutable address, with a short `acquire_timeout`, fails
    /// fast on first use (sqlx's default 30s acquire timeout would otherwise make each of
    /// the 4 calls below take up to 30s), exercising every delegation without a real MySQL
    /// server.
    #[tokio::test]
    async fn schema_inspector_adapter_propagates_connection_errors() {
        let pool = sqlx::mysql::MySqlPoolOptions::new()
            .acquire_timeout(std::time::Duration::from_millis(200))
            .connect_lazy("mysql://user:pass@127.0.0.1:1/testdb")
            .unwrap();
        let adapter = SchemaInspectorAdapter::new(pool, "testdb".to_string());

        assert!(adapter.discover_columns("orders").await.is_err());
        assert!(adapter.discover_indexes("orders").await.is_err());
        assert!(adapter.get_avg_row_length("orders").await.is_err());
        assert!(adapter.max_timestamp("orders", "updated_at").await.is_err());
    }

    /// O6: `calculate_batch_size`/`batch_size` are pure delegations; `extract` fails before
    /// any network I/O when the configured URL doesn't even parse.
    #[test]
    fn extractor_adapter_delegates_to_batch_extractor() {
        let mut config = make_config(vec![]);
        config.database_url = "not-a-valid-database-url".to_string();
        let mut adapter = ExtractorAdapter::new(&config);

        let size = adapter.calculate_batch_size(Some(1024));
        assert!(size > 0);
        assert_eq!(adapter.batch_size(), size);
        assert!(adapter.extract("SELECT 1").is_err());
    }

    /// O6: `StateManageAdapter` delegates to `AppState`, backed by a real state file on
    /// disk (no network involved at all).
    #[test]
    fn state_manage_adapter_load_and_update_round_trip() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("state.json");
        let mut adapter = StateManageAdapter::new();

        let initial = adapter.load_or_default(&path);
        assert!(initial.tables.is_empty());

        let table_state = TableState {
            last_run_at: Some("2024-01-01T00:00:00Z".to_string()),
            last_run_status: Some("success".to_string()),
            last_run_rows: Some(10),
            last_run_duration_ms: Some(5),
            extraction_mode: Some("full".to_string()),
            schema_columns_hash: Some("abc".to_string()),
        };
        adapter
            .update_table("orders", table_state, &path)
            .unwrap();

        let reloaded = adapter.load_or_default(&path);
        assert!(reloaded.tables.contains_key("orders"));
    }
}
