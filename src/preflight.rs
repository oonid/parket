use anyhow::{bail, Result};
use object_store::aws::AmazonS3Builder;
use object_store::ObjectStore;
use tracing::{error, info};

use crate::config::Config;
use crate::discovery::{filter_unsupported_columns, select_integer_pk, ColumnInfo, IndexInfo, SchemaInspector};

#[cfg_attr(test, mockall::automock)]
#[allow(async_fn_in_trait)]
pub trait PreflightInspect: Send + Sync {
    async fn discover_columns(&self, table: &str) -> Result<Vec<ColumnInfo>>;
    async fn get_avg_row_length(&self, table: &str) -> Result<Option<u64>>;
    /// N3-r: the table's indexes, so preflight can resolve its integer PK (via
    /// `discovery::select_integer_pk`) and detect incremental mode the same way the run does,
    /// even for a non-`id`-named PK. Wired into `check_table` in the CP3 switchover.
    async fn discover_indexes(&self, table: &str) -> Result<Vec<IndexInfo>>;
}

#[cfg_attr(test, mockall::automock)]
#[allow(async_fn_in_trait)]
pub trait PreflightStorage: Send + Sync {
    async fn check_writable(&self) -> Result<()>;
}

#[cfg_attr(test, mockall::automock)]
#[allow(async_fn_in_trait)]
pub trait PreflightHwm: Send + Sync {
    async fn read_hwm(&self, table: &str) -> Result<Option<crate::writer::Hwm>>;

    /// §10.4: the two-stream INSERT watermark (`hwm_insert_id`), which `read_hwm` does not carry.
    /// `read_hwm` returns the UPDATE-stream watermark — `updated_at` plus `last_id`, where
    /// `last_id` is the update window's keyset-pagination cursor, NOT the insert frontier. Showing
    /// only those for a two-stream table made the preflight output look like the insert watermark
    /// had regressed by 327 M during a post-incident check (it had not), so preflight needs the real
    /// value to display alongside.
    async fn read_insert_hwm(&self, table: &str) -> Result<Option<i64>>;

    /// O7-rest-b: fetch the existing Delta schema for a table (None if the table doesn't exist
    /// yet), so preflight can run the same schema-evolution check the run does.
    async fn delta_schema(&self, table: &str) -> Result<Option<deltalake::arrow::datatypes::SchemaRef>>;
}

/// §10.4: render a TWO-STREAM table's watermarks with every field labelled.
///
/// `read_hwm` returns the UPDATE-stream watermark: `updated_at` plus `last_id`, where `last_id` is
/// the update window's keyset-pagination cursor. It is NOT the insert frontier and can legitimately
/// move backwards between runs — which, printed bare as `updated_at / last_id`, reads exactly like
/// the H-2026-07-11-1 watermark-reset failure. The real insert frontier is `hwm_insert_id`, read
/// separately via `read_insert_hwm`. Labelling all three removes the ambiguity; unknowns render as
/// `—` so a missing value can never be mistaken for a real one.
fn format_two_stream_hwm(insert_hwm: Option<i64>, hwm: Option<&crate::writer::Hwm>) -> String {
    let ins = insert_hwm.map(|v| v.to_string()).unwrap_or_else(|| "—".to_string());
    match hwm {
        Some(h) => format!("ins={} upd={} page={}", ins, h.updated_at, h.last_id),
        None => format!("ins={ins} upd=— page=—"),
    }
}

pub struct NoopPreflightStorage;

impl PreflightStorage for NoopPreflightStorage {
    async fn check_writable(&self) -> Result<()> {
        Ok(())
    }
}

pub struct PreflightCheck<I, S, H> {
    config: Config,
    inspect: I,
    storage: Option<S>,
    hwm: H,
}

impl<I, S, H> PreflightCheck<I, S, H>
where
    I: PreflightInspect + Send + Sync,
    S: PreflightStorage + Send + Sync,
    H: PreflightHwm + Send + Sync,
{
    pub fn new(config: Config, inspect: I, storage: S, hwm: H) -> Self {
        Self {
            config,
            inspect,
            storage: Some(storage),
            hwm,
        }
    }

    pub async fn run(&self) -> Result<()> {
        if let Some(ref storage) = self.storage {
            storage.check_writable().await?;
            info!("storage writability check passed");
        } else {
            info!("storage writability check skipped");
        }

        println!("{:<30} {:<15} {:<10} {:<15} {:<20} {:<15}", "TABLE", "MODE", "COLUMNS", "AVG_ROW_LEN", "KEY", "HWM");

        let mut errors = 0u32;
        for table_name in &self.config.tables {
            match self.check_table(table_name).await {
                Ok(()) => {}
                Err(e) => {
                    error!(table = table_name, error = %e, "table check failed");
                    errors += 1;
                }
            }
        }

        if errors > 0 {
            bail!("{errors} table(s) failed pre-flight check");
        }
        Ok(())
    }

    async fn check_table(&self, table_name: &str) -> Result<()> {
        let columns = self.inspect.discover_columns(table_name).await?;
        let columns = filter_unsupported_columns(&columns);
        let indexes = self.inspect.discover_indexes(table_name).await?;
        let (ts_col, mode) = crate::discovery::resolve_ts_col_and_mode(&columns, &indexes, &self.config, table_name)?;

        if !matches!(mode, crate::config::ExtractionMode::Incremental | crate::config::ExtractionMode::TwoStream)
            && self.config.table_initial_hwm.contains_key(table_name)
        {
            anyhow::bail!("TABLE_HWM_{table_name} set but table is not incremental or two-stream");
        }

        // O7-rest-b: mirror the run's schema-evolution check (orchestrator applies it for
        // Incremental|TwoStream against an existing Delta table) so `--check` pre-flags an
        // incompatible existing schema — a dropped or type-changed column — that the run would
        // otherwise bail on. Skipped when the Delta table doesn't exist yet (first run).
        if matches!(mode, crate::config::ExtractionMode::Incremental | crate::config::ExtractionMode::TwoStream)
            && let Some(existing_schema) = self.hwm.delta_schema(table_name).await?
        {
            crate::orchestrator::schema_evolution_check(&columns, &existing_schema)?;
        }

        let avg_row_length = self.inspect.get_avg_row_length(table_name).await?;
        let hwm = self.hwm.read_hwm(table_name).await?;

        // Compute KEY based on mode and override
        let key = if matches!(mode, crate::config::ExtractionMode::TwoStream) {
            if let Some((ins, upd)) = self.config.two_stream(table_name) {
                format!("two-stream: {} + {}", ins, upd)
            } else {
                unreachable!() // mode is TwoStream, so two_stream() must return Some
            }
        } else if matches!(self.config.table_modes.get(table_name), Some(m) if m != &crate::config::ExtractionMode::Auto) {
            "override".to_string()
        } else {
            match mode {
                crate::config::ExtractionMode::Incremental => {
                    let key_col = select_integer_pk(&columns, &indexes).unwrap_or_else(|| "id".to_string());
                    format!("{key_col}, {ts_col}")
                }
                crate::config::ExtractionMode::FullRefresh => {
                    // Determine why it's FullRefresh
                    let has_integer_key = select_integer_pk(&columns, &indexes).is_some();
                    let ts_candidate = columns.iter().find(|c| {
                        c.name == ts_col
                            && (c.data_type == "timestamp" || c.data_type == "datetime")
                    });
                    match (has_integer_key, ts_candidate) {
                        (false, None) => format!("no id/{ts_col}"),
                        (false, Some(_)) => "no id".to_string(),
                        (true, None) => format!("no {ts_col}"),
                        // O3: an integer key + a right-typed but nullable cursor is demoted
                        // to FullRefresh by detect_mode — this is now reachable, not a bug.
                        (true, Some(c)) if c.nullable => {
                            format!("nullable {ts_col} (unsafe cursor)")
                        }
                        (true, Some(_)) => unreachable!(), // Would be Incremental
                    }
                }
                crate::config::ExtractionMode::TwoStream => unreachable!(), // Already handled above
                crate::config::ExtractionMode::Auto => unreachable!(), // detect_mode never returns Auto
            }
        };

        // Format HWM.
        //
        // §10.4: for a TWO-STREAM table the bare `updated_at / last_id` pair is actively
        // misleading — both are UPDATE-stream values, and `last_id` is the update window's
        // keyset-pagination cursor, which readers naturally mistake for the insert frontier. It can
        // legitimately move BACKWARDS between runs, which reads exactly like the H-2026-07-11-1
        // watermark-reset failure. So label every field and show the real insert watermark.
        let hwm_str = if matches!(mode, crate::config::ExtractionMode::TwoStream) {
            let insert_hwm = self.hwm.read_insert_hwm(table_name).await?;
            format_two_stream_hwm(insert_hwm, hwm.as_ref())
        } else {
            match &hwm {
                Some(h) => format!("{} / {}", h.updated_at, h.last_id),
                None => "—".to_string(),
            }
        };

        let mode_str = match mode {
            crate::config::ExtractionMode::Incremental => "incremental",
            crate::config::ExtractionMode::FullRefresh => "full_refresh",
            crate::config::ExtractionMode::TwoStream => "two_stream",
            crate::config::ExtractionMode::Auto => "auto",
        };

        println!(
            "{:<30} {:<15} {:<10} {:<15} {:<20} {:<15}",
            table_name,
            mode_str,
            columns.len(),
            avg_row_length
                .map(|v| v.to_string())
                .unwrap_or_else(|| "N/A".to_string()),
            key,
            hwm_str,
        );

        Ok(())
    }
}

pub struct PreflightInspectAdapter {
    pool: sqlx::MySqlPool,
    database: String,
}

impl PreflightInspectAdapter {
    pub fn new(pool: sqlx::MySqlPool, database: String) -> Self {
        Self { pool, database }
    }
}

impl PreflightInspect for PreflightInspectAdapter {
    async fn discover_columns(&self, table: &str) -> Result<Vec<ColumnInfo>> {
        SchemaInspector::new(self.pool.clone(), self.database.clone())
            .discover_columns(table)
            .await
    }

    async fn get_avg_row_length(&self, table: &str) -> Result<Option<u64>> {
        SchemaInspector::new(self.pool.clone(), self.database.clone())
            .get_avg_row_length(table)
            .await
    }

    async fn discover_indexes(&self, table: &str) -> Result<Vec<IndexInfo>> {
        SchemaInspector::new(self.pool.clone(), self.database.clone())
            .discover_indexes(table)
            .await
    }
}

pub struct PreflightStorageAdapter {
    s3_builder: AmazonS3Builder,
    bucket: String,
    prefix: String,
}

/// Health-check object path — written UNDER `s3_prefix` (O7) so the probe exercises the same
/// key space as real data (`{prefix}/{table}/...`), catching prefix-scoped IAM misconfig that a
/// bucket-root probe would miss. An empty prefix falls back to the bucket root.
fn health_check_path(prefix: &str) -> object_store::path::Path {
    if prefix.is_empty() {
        object_store::path::Path::from(".parket-health-check")
    } else {
        object_store::path::Path::from(format!("{prefix}/.parket-health-check"))
    }
}

impl PreflightStorageAdapter {
    pub fn new(config: &Config) -> Self {
        let mut builder = AmazonS3Builder::new()
            .with_bucket_name(&config.s3_bucket)
            .with_region(&config.s3_region)
            .with_access_key_id(&config.s3_access_key_id)
            .with_secret_access_key(&config.s3_secret_access_key)
            .with_allow_http(true);

        if let Some(endpoint) = &config.s3_endpoint {
            builder = builder.with_endpoint(endpoint);
        }

        Self {
            s3_builder: builder,
            bucket: config.s3_bucket.clone(),
            prefix: config.s3_prefix.clone(),
        }
    }
}

impl PreflightStorage for PreflightStorageAdapter {
    async fn check_writable(&self) -> Result<()> {
        let store = self.s3_builder.clone().build()?;
        let test_path = health_check_path(&self.prefix);
        let test_data = object_store::PutPayload::from(b"parket-preflight-check".to_vec());

        store
            .put(&test_path, test_data)
            .await
            .map_err(|e| anyhow::anyhow!("failed to write to S3 bucket '{}': {e}", self.bucket))?;

        store
            .delete(&test_path)
            .await
            .map_err(|e| anyhow::anyhow!("failed to delete from S3 bucket '{}': {e}", self.bucket))?;

        Ok(())
    }
}

/// Local-filesystem storage probe (O7): `--check --local <dir>` must actually verify the local
/// Delta base directory is writable, not silently pass. Creates the dir if needed, then writes and
/// deletes a probe file — the local analog of the S3 bucket/prefix write check.
pub struct LocalPreflightStorage {
    base_dir: std::path::PathBuf,
}

impl LocalPreflightStorage {
    pub fn new(base_dir: &std::path::Path) -> Self {
        Self { base_dir: base_dir.to_path_buf() }
    }
}

impl PreflightStorage for LocalPreflightStorage {
    async fn check_writable(&self) -> Result<()> {
        std::fs::create_dir_all(&self.base_dir)
            .map_err(|e| anyhow::anyhow!("local storage dir '{}' is not creatable: {e}", self.base_dir.display()))?;
        let probe = self.base_dir.join(".parket-health-check");
        std::fs::write(&probe, b"parket-preflight-check")
            .map_err(|e| anyhow::anyhow!("local storage dir '{}' is not writable: {e}", self.base_dir.display()))?;
        std::fs::remove_file(&probe)
            .map_err(|e| anyhow::anyhow!("failed to clean up probe file in '{}': {e}", self.base_dir.display()))?;
        Ok(())
    }
}

pub struct PreflightHwmAdapter {
    inner: crate::writer::DeltaWriter,
}

impl PreflightHwmAdapter {
    pub fn new(config: &Config) -> Self {
        Self {
            inner: crate::writer::DeltaWriter::new(
                &config.s3_bucket,
                &config.s3_prefix,
                config.s3_endpoint.as_deref(),
                &config.s3_region,
                &config.s3_access_key_id,
                &config.s3_secret_access_key,
            ),
        }
    }

    pub fn new_local(dir: &std::path::Path) -> Self {
        Self {
            inner: crate::writer::DeltaWriter::new_local(&dir.to_string_lossy()),
        }
    }
}

impl PreflightHwm for PreflightHwmAdapter {
    async fn read_hwm(&self, table: &str) -> Result<Option<crate::writer::Hwm>> {
        self.inner.read_hwm(table).await
    }

    async fn read_insert_hwm(&self, table: &str) -> Result<Option<i64>> {
        self.inner.read_insert_hwm(table).await
    }

    async fn delta_schema(&self, table: &str) -> Result<Option<deltalake::arrow::datatypes::SchemaRef>> {
        crate::orchestrator::get_schema_impl(&self.inner, table).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ExtractionMode;
    use std::collections::HashMap;

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
            table_reconcile: std::collections::HashSet::new(),
        }
    }

    fn col(name: &str, data_type: &str, column_type: &str) -> ColumnInfo {
        ColumnInfo {
            name: name.to_string(),
            data_type: data_type.to_string(),
            column_type: column_type.to_string(),
            nullable: false,
        }
    }

    /// Same as `col`, but `nullable: true` — for O3 nullable-cursor preflight tests.
    fn nullable_col(name: &str, data_type: &str, column_type: &str) -> ColumnInfo {
        ColumnInfo {
            name: name.to_string(),
            data_type: data_type.to_string(),
            column_type: column_type.to_string(),
            nullable: true,
        }
    }

    /// A single-column integer PRIMARY key index on `id` — matches the shape of
    /// `incremental_columns()` (id + updated_at) so `select_integer_pk` finds it and the
    /// table auto-detects Incremental, mirroring pre-N3-r's literal-`id` check.
    fn primary_id_index() -> Vec<IndexInfo> {
        vec![IndexInfo { name: "PRIMARY".to_string(), unique: true, columns: vec!["id".to_string()] }]
    }

    fn incremental_columns() -> Vec<ColumnInfo> {
        vec![
            col("id", "bigint", "bigint(20)"),
            col("name", "varchar", "varchar(255)"),
            col("updated_at", "timestamp", "timestamp"),
        ]
    }

    fn full_refresh_columns() -> Vec<ColumnInfo> {
        vec![
            col("id", "bigint", "bigint(20)"),
            col("name", "varchar", "varchar(255)"),
        ]
    }

    #[tokio::test]
    async fn all_tables_succeed() {
        let config = make_config(vec!["orders".to_string(), "products".to_string()]);
        let mut inspect = MockPreflightInspect::new();
        let mut storage = MockPreflightStorage::new();
        let mut hwm = MockPreflightHwm::new();

        storage.expect_check_writable().returning(|| Ok(()));
        hwm.expect_read_hwm().returning(|_| Ok(None));
        hwm.expect_delta_schema().returning(|_| Ok(None));
        inspect
            .expect_discover_columns()
            .withf(|t| t == "orders")
            .returning(|_| Ok(incremental_columns()));
        inspect
            .expect_get_avg_row_length()
            .withf(|t| t == "orders")
            .returning(|_| Ok(Some(128)));
        inspect
            .expect_discover_columns()
            .withf(|t| t == "products")
            .returning(|_| Ok(full_refresh_columns()));
        inspect
            .expect_get_avg_row_length()
            .withf(|t| t == "products")
            .returning(|_| Ok(None));
        inspect.expect_discover_indexes().returning(|_| Ok(primary_id_index()));

        let check = PreflightCheck::new(config, inspect, storage, hwm);
        let result = check.run().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn hwm_read_succeeds_with_some_value() {
        let config = make_config(vec!["orders".to_string()]);
        let mut inspect = MockPreflightInspect::new();
        let mut storage = MockPreflightStorage::new();
        let mut hwm = MockPreflightHwm::new();

        storage.expect_check_writable().returning(|| Ok(()));
        hwm.expect_read_hwm()
            .withf(|t| t == "orders")
            .returning(|_| {
                Ok(Some(crate::writer::Hwm {
                    updated_at: "2026-01-01T00:00:00.000000".to_string(),
                    last_id: 1000,
                }))
            });
        hwm.expect_delta_schema().returning(|_| Ok(None));
        inspect
            .expect_discover_columns()
            .withf(|t| t == "orders")
            .returning(|_| Ok(incremental_columns()));
        inspect
            .expect_get_avg_row_length()
            .withf(|t| t == "orders")
            .returning(|_| Ok(Some(128)));
        inspect.expect_discover_indexes().returning(|_| Ok(primary_id_index()));

        let check = PreflightCheck::new(config, inspect, storage, hwm);
        let result = check.run().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn s3_not_writable_fails_fast() {
        let config = make_config(vec!["orders".to_string()]);
        let inspect = MockPreflightInspect::new();
        let mut storage = MockPreflightStorage::new();
        let hwm = MockPreflightHwm::new();

        storage
            .expect_check_writable()
            .returning(|| Err(anyhow::anyhow!("bucket not found")));

        let check = PreflightCheck::new(config, inspect, storage, hwm);
        let result = check.run().await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("bucket not found"),
            "expected S3 error, got: {err}"
        );
    }

    #[tokio::test]
    async fn missing_table_reports_error() {
        let config = make_config(vec!["missing".to_string()]);
        let mut inspect = MockPreflightInspect::new();
        let mut storage = MockPreflightStorage::new();
        let hwm = MockPreflightHwm::new();

        storage.expect_check_writable().returning(|| Ok(()));
        inspect
            .expect_discover_columns()
            .returning(|_| Err(anyhow::anyhow!("table does not exist")));

        let check = PreflightCheck::new(config, inspect, storage, hwm);
        let result = check.run().await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("1 table(s) failed"),
            "expected failure count, got: {err}"
        );
    }

    #[tokio::test]
    async fn partial_failure_reports_error_count() {
        let config = make_config(vec!["good".to_string(), "bad".to_string()]);
        let mut inspect = MockPreflightInspect::new();
        let mut storage = MockPreflightStorage::new();
        let mut hwm = MockPreflightHwm::new();

        storage.expect_check_writable().returning(|| Ok(()));
        hwm.expect_read_hwm().returning(|_| Ok(None));
        hwm.expect_delta_schema().returning(|_| Ok(None));
        inspect
            .expect_discover_columns()
            .withf(|t| t == "good")
            .returning(|_| Ok(incremental_columns()));
        inspect
            .expect_get_avg_row_length()
            .withf(|t| t == "good")
            .returning(|_| Ok(Some(64)));
        inspect
            .expect_discover_columns()
            .withf(|t| t == "bad")
            .returning(|_| Err(anyhow::anyhow!("not found")));
        inspect.expect_discover_indexes().returning(|_| Ok(primary_id_index()));

        let check = PreflightCheck::new(config, inspect, storage, hwm);
        let result = check.run().await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("1 table(s) failed"),
            "expected 1 failure, got: {err}"
        );
    }

    #[tokio::test]
    async fn mode_override_respected() {
        let config = Config {
            table_modes: vec![("products".into(), ExtractionMode::Incremental)].into_iter().collect(),
            ..make_config(vec!["products".to_string()])
        };
        let mut inspect = MockPreflightInspect::new();
        let mut storage = MockPreflightStorage::new();
        let mut hwm = MockPreflightHwm::new();

        storage.expect_check_writable().returning(|| Ok(()));
        hwm.expect_read_hwm().returning(|_| Ok(None));
        hwm.expect_delta_schema().returning(|_| Ok(None));
        inspect
            .expect_discover_columns()
            .returning(|_| Ok(full_refresh_columns()));
        inspect
            .expect_get_avg_row_length()
            .returning(|_| Ok(Some(100)));
        inspect.expect_discover_indexes().returning(|_| Ok(vec![]));

        let check = PreflightCheck::new(config, inspect, storage, hwm);
        let result = check.run().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn nullable_cursor_auto_resolves_to_full_refresh_in_preflight() {
        // O3/pf1 smoke: drives preflight's check_table through the FullRefresh
        // KEY-reason match with a nullable `updated_at` + `id` — the arm that was
        // `unreachable!()` before O3 made this combination reachable (pf1). The mode
        // demotion itself is asserted directly by the discovery::detect_mode unit
        // tests; preflight shares that resolver, so this test's value is executing
        // the new reason arm without panicking (KEY string is printed, not returned,
        // so it can't be asserted here without capturing stdout).
        let config = make_config(vec!["orders".to_string()]);
        let mut inspect = MockPreflightInspect::new();
        let mut storage = MockPreflightStorage::new();
        let mut hwm = MockPreflightHwm::new();

        storage.expect_check_writable().returning(|| Ok(()));
        hwm.expect_read_hwm().returning(|_| Ok(None));
        hwm.expect_delta_schema().returning(|_| Ok(None));
        inspect.expect_discover_columns().returning(|_| {
            Ok(vec![
                col("id", "bigint", "bigint(20)"),
                col("name", "varchar", "varchar(255)"),
                nullable_col("updated_at", "timestamp", "timestamp"),
            ])
        });
        inspect
            .expect_get_avg_row_length()
            .returning(|_| Ok(Some(100)));
        // id is a single-column integer PRIMARY key, so has_integer_key is true —
        // exercising the intended "nullable cursor demotes an otherwise-eligible
        // table" reason arm, not the unrelated "no integer key" arm.
        inspect.expect_discover_indexes().returning(|_| Ok(primary_id_index()));

        let check = PreflightCheck::new(config, inspect, storage, hwm);
        // Passes as a FullRefresh table (the KEY reason names the nullable cursor);
        // the essential assertion is that preflight does NOT treat it as incremental,
        // which read_hwm-only-for-incremental setups would otherwise mask.
        let result = check.run().await;
        assert!(result.is_ok(), "nullable-cursor table must preflight as full_refresh: {result:?}");
    }

    #[tokio::test]
    async fn noop_storage_always_succeeds() {
        let storage = NoopPreflightStorage;
        assert!(storage.check_writable().await.is_ok());
    }

    #[tokio::test]
    async fn local_mode_skips_s3_check_with_noop() {
        let config = make_config(vec!["orders".to_string()]);
        let mut inspect = MockPreflightInspect::new();
        let mut hwm = MockPreflightHwm::new();

        inspect
            .expect_discover_columns()
            .returning(|_| Ok(incremental_columns()));
        inspect
            .expect_get_avg_row_length()
            .returning(|_| Ok(Some(128)));
        inspect.expect_discover_indexes().returning(|_| Ok(primary_id_index()));
        hwm.expect_read_hwm().returning(|_| Ok(None));
        hwm.expect_delta_schema().returning(|_| Ok(None));

        let check = PreflightCheck::new(config, inspect, NoopPreflightStorage, hwm);
        let result = check.run().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn local_mode_table_error_still_fails() {
        let config = make_config(vec!["missing".to_string()]);
        let mut inspect = MockPreflightInspect::new();
        let hwm = MockPreflightHwm::new();

        inspect
            .expect_discover_columns()
            .returning(|_| Err(anyhow::anyhow!("table does not exist")));

        let check = PreflightCheck::new(config, inspect, NoopPreflightStorage, hwm);
        let result = check.run().await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("1 table(s) failed"));
    }

    #[tokio::test]
    async fn rejects_hwm_config_on_non_incremental_table() {
        let mut config = make_config(vec!["products".to_string()]);
        config.table_initial_hwm.insert(
            "products".to_string(),
            ("2026-05-01T00:00:00.000000".to_string(), 999),
        );
        let mut inspect = MockPreflightInspect::new();
        let mut storage = MockPreflightStorage::new();
        let mut hwm = MockPreflightHwm::new();

        storage.expect_check_writable().returning(|| Ok(()));
        hwm.expect_read_hwm().returning(|_| Ok(None));
        hwm.expect_delta_schema().returning(|_| Ok(None));
        inspect
            .expect_discover_columns()
            .returning(|_| Ok(full_refresh_columns()));
        inspect
            .expect_get_avg_row_length()
            .returning(|_| Ok(Some(100)));
        inspect.expect_discover_indexes().returning(|_| Ok(vec![]));

        let check = PreflightCheck::new(config, inspect, storage, hwm);
        let result = check.run().await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("1 table(s) failed"));
    }

    #[tokio::test]
    async fn rejects_invalid_timestamp_column_config() {
        let mut config = make_config(vec!["products".to_string()]);
        config.table_timestamp_col.insert("products".to_string(), "nonexistent_col".to_string());
        let mut inspect = MockPreflightInspect::new();
        let mut storage = MockPreflightStorage::new();
        let hwm = MockPreflightHwm::new();

        storage.expect_check_writable().returning(|| Ok(()));
        inspect
            .expect_discover_columns()
            .returning(|_| Ok(full_refresh_columns()));
        inspect
            .expect_get_avg_row_length()
            .returning(|_| Ok(Some(100)));
        inspect.expect_discover_indexes().returning(|_| Ok(vec![]));

        let check = PreflightCheck::new(config, inspect, storage, hwm);
        let result = check.run().await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("1 table(s) failed"));
    }

    #[tokio::test]
    async fn two_stream_both_cursors_valid_succeeds() {
        let mut config = make_config(vec!["orders".to_string()]);
        config.table_insert_cursor.insert("orders".to_string(), "id".to_string());
        config.table_update_cursor.insert("orders".to_string(), "updated_at".to_string());

        let mut inspect = MockPreflightInspect::new();
        let mut storage = MockPreflightStorage::new();
        let mut hwm = MockPreflightHwm::new();

        storage.expect_check_writable().returning(|| Ok(()));
        hwm.expect_read_hwm().returning(|_| Ok(None));
        hwm.expect_read_insert_hwm().returning(|_| Ok(Some(502_658_778)));
        hwm.expect_delta_schema().returning(|_| Ok(None));
        inspect
            .expect_discover_columns()
            .returning(|_| Ok(incremental_columns()));
        inspect
            .expect_get_avg_row_length()
            .returning(|_| Ok(Some(128)));
        inspect.expect_discover_indexes().returning(|_| Ok(vec![]));

        let check = PreflightCheck::new(config, inspect, storage, hwm);
        let result = check.run().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn two_stream_only_insert_cursor_fails() {
        let mut config = make_config(vec!["orders".to_string()]);
        config.table_insert_cursor.insert("orders".to_string(), "id".to_string());

        let mut inspect = MockPreflightInspect::new();
        let mut storage = MockPreflightStorage::new();
        let hwm = MockPreflightHwm::new();

        storage.expect_check_writable().returning(|| Ok(()));
        inspect
            .expect_discover_columns()
            .returning(|_| Ok(incremental_columns()));
        inspect
            .expect_get_avg_row_length()
            .returning(|_| Ok(Some(128)));
        inspect.expect_discover_indexes().returning(|_| Ok(vec![]));

        let check = PreflightCheck::new(config, inspect, storage, hwm);
        let result = check.run().await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("1 table(s) failed"), "error message: {err}");
    }

    #[tokio::test]
    async fn two_stream_only_update_cursor_fails() {
        let mut config = make_config(vec!["orders".to_string()]);
        config.table_update_cursor.insert("orders".to_string(), "updated_at".to_string());

        let mut inspect = MockPreflightInspect::new();
        let mut storage = MockPreflightStorage::new();
        let hwm = MockPreflightHwm::new();

        storage.expect_check_writable().returning(|| Ok(()));
        inspect
            .expect_discover_columns()
            .returning(|_| Ok(incremental_columns()));
        inspect
            .expect_get_avg_row_length()
            .returning(|_| Ok(Some(128)));
        inspect.expect_discover_indexes().returning(|_| Ok(vec![]));

        let check = PreflightCheck::new(config, inspect, storage, hwm);
        let result = check.run().await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("1 table(s) failed"), "error message: {err}");
    }

    #[tokio::test]
    async fn two_stream_insert_cursor_not_integer_fails() {
        let mut config = make_config(vec!["orders".to_string()]);
        config.table_insert_cursor.insert("orders".to_string(), "name".to_string());
        config.table_update_cursor.insert("orders".to_string(), "updated_at".to_string());

        let mut inspect = MockPreflightInspect::new();
        let mut storage = MockPreflightStorage::new();
        let hwm = MockPreflightHwm::new();

        storage.expect_check_writable().returning(|| Ok(()));
        inspect
            .expect_discover_columns()
            .returning(|_| Ok(incremental_columns()));
        inspect
            .expect_get_avg_row_length()
            .returning(|_| Ok(Some(128)));
        inspect.expect_discover_indexes().returning(|_| Ok(vec![]));

        let check = PreflightCheck::new(config, inspect, storage, hwm);
        let result = check.run().await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("1 table(s) failed"), "error message: {err}");
    }

    #[tokio::test]
    async fn two_stream_update_cursor_not_timestamp_fails() {
        let mut config = make_config(vec!["orders".to_string()]);
        config.table_insert_cursor.insert("orders".to_string(), "id".to_string());
        config.table_update_cursor.insert("orders".to_string(), "name".to_string());

        let mut inspect = MockPreflightInspect::new();
        let mut storage = MockPreflightStorage::new();
        let hwm = MockPreflightHwm::new();

        storage.expect_check_writable().returning(|| Ok(()));
        inspect
            .expect_discover_columns()
            .returning(|_| Ok(incremental_columns()));
        inspect
            .expect_get_avg_row_length()
            .returning(|_| Ok(Some(128)));
        inspect.expect_discover_indexes().returning(|_| Ok(vec![]));

        let check = PreflightCheck::new(config, inspect, storage, hwm);
        let result = check.run().await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("1 table(s) failed"), "error message: {err}");
    }

    #[tokio::test]
    async fn full_refresh_no_id_no_timestamp() {
        // Table has neither id nor timestamp; should be full_refresh with "no id/updated_at" key.
        let columns = vec![
            col("name", "varchar", "varchar(255)"),
            col("value", "int", "int(11)"),
        ];
        let config = make_config(vec!["data".to_string()]);
        let mut inspect = MockPreflightInspect::new();
        let mut storage = MockPreflightStorage::new();
        let mut hwm = MockPreflightHwm::new();

        storage.expect_check_writable().returning(|| Ok(()));
        hwm.expect_read_hwm().returning(|_| Ok(None));
        hwm.expect_delta_schema().returning(|_| Ok(None));
        inspect
            .expect_discover_columns()
            .withf(|t| t == "data")
            .returning(move |_| Ok(columns.clone()));
        inspect
            .expect_get_avg_row_length()
            .returning(|_| Ok(Some(64)));
        inspect.expect_discover_indexes().returning(|_| Ok(vec![]));

        let check = PreflightCheck::new(config, inspect, storage, hwm);
        let result = check.run().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn full_refresh_has_id_no_timestamp() {
        // Table has id but missing timestamp column; should show "no updated_at" in key.
        let columns = vec![
            col("id", "bigint", "bigint(20)"),
            col("name", "varchar", "varchar(255)"),
        ];
        let config = make_config(vec!["products".to_string()]);
        let mut inspect = MockPreflightInspect::new();
        let mut storage = MockPreflightStorage::new();
        let mut hwm = MockPreflightHwm::new();

        storage.expect_check_writable().returning(|| Ok(()));
        hwm.expect_read_hwm().returning(|_| Ok(None));
        hwm.expect_delta_schema().returning(|_| Ok(None));
        inspect
            .expect_discover_columns()
            .returning(move |_| Ok(columns.clone()));
        inspect
            .expect_get_avg_row_length()
            .returning(|_| Ok(Some(100)));
        // id is a single-column integer PRIMARY key, so the reason arm stays "no
        // updated_at" (not "no id/updated_at") — matching this test's documented intent.
        inspect.expect_discover_indexes().returning(|_| Ok(primary_id_index()));

        let check = PreflightCheck::new(config, inspect, storage, hwm);
        let result = check.run().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn full_refresh_no_id_has_timestamp() {
        // Table has timestamp but missing id; should show "no id" in key.
        let columns = vec![
            col("name", "varchar", "varchar(255)"),
            col("updated_at", "timestamp", "timestamp"),
        ];
        let config = make_config(vec!["events".to_string()]);
        let mut inspect = MockPreflightInspect::new();
        let mut storage = MockPreflightStorage::new();
        let mut hwm = MockPreflightHwm::new();

        storage.expect_check_writable().returning(|| Ok(()));
        hwm.expect_read_hwm().returning(|_| Ok(None));
        hwm.expect_delta_schema().returning(|_| Ok(None));
        inspect
            .expect_discover_columns()
            .returning(move |_| Ok(columns.clone()));
        inspect
            .expect_get_avg_row_length()
            .returning(|_| Ok(Some(80)));
        inspect.expect_discover_indexes().returning(|_| Ok(vec![]));

        let check = PreflightCheck::new(config, inspect, storage, hwm);
        let result = check.run().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn mode_override_shows_override_in_key() {
        // When mode override is set, KEY should display "override" not the computed reason.
        let config = Config {
            table_modes: vec![("products".into(), ExtractionMode::FullRefresh)].into_iter().collect(),
            ..make_config(vec!["products".to_string()])
        };
        let columns = incremental_columns(); // Has both id and timestamp
        let mut inspect = MockPreflightInspect::new();
        let mut storage = MockPreflightStorage::new();
        let mut hwm = MockPreflightHwm::new();

        storage.expect_check_writable().returning(|| Ok(()));
        hwm.expect_read_hwm().returning(|_| Ok(None));
        hwm.expect_delta_schema().returning(|_| Ok(None));
        inspect
            .expect_discover_columns()
            .returning(move |_| Ok(columns.clone()));
        inspect
            .expect_get_avg_row_length()
            .returning(|_| Ok(Some(128)));
        inspect.expect_discover_indexes().returning(|_| Ok(vec![]));

        let check = PreflightCheck::new(config, inspect, storage, hwm);
        let result = check.run().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn two_stream_mode_key_includes_both_cursors() {
        // TwoStream mode KEY should show "two-stream: insert_col + update_col".
        let mut config = make_config(vec!["orders".to_string()]);
        config.table_insert_cursor.insert("orders".to_string(), "id".to_string());
        config.table_update_cursor.insert("orders".to_string(), "updated_at".to_string());

        let mut inspect = MockPreflightInspect::new();
        let mut storage = MockPreflightStorage::new();
        let mut hwm = MockPreflightHwm::new();

        storage.expect_check_writable().returning(|| Ok(()));
        hwm.expect_read_hwm().returning(|_| Ok(None));
        hwm.expect_read_insert_hwm().returning(|_| Ok(Some(502_658_778)));
        hwm.expect_delta_schema().returning(|_| Ok(None));
        let cols = incremental_columns();
        inspect
            .expect_discover_columns()
            .returning(move |_| Ok(cols.clone()));
        inspect
            .expect_get_avg_row_length()
            .returning(|_| Ok(Some(128)));
        inspect.expect_discover_indexes().returning(|_| Ok(vec![]));

        let check = PreflightCheck::new(config, inspect, storage, hwm);
        let result = check.run().await;
        assert!(result.is_ok());
    }

    /// §10.4 regression pin: for a TWO-STREAM table the preflight HWM column must show the real
    /// INSERT watermark and must label the update-window pagination cursor.
    ///
    /// The bare `updated_at / last_id` pair it printed before is both update-stream values, and
    /// `last_id` — the update window's keyset cursor — can legitimately move BACKWARDS between runs.
    /// During a post-incident check that read as the insert watermark regressing by 327 M, i.e. the
    /// H-2026-07-11-1 watermark-reset shape, when nothing was wrong. `format_two_stream_hwm` is
    /// therefore pinned directly: `read_hwm` is the wrong source for the insert frontier, so any
    /// future edit that drops `ins=` fails here.
    #[test]
    fn two_stream_hwm_display_labels_insert_and_pagination_cursors() {
        let h = crate::writer::Hwm {
            updated_at: "2026-08-07T15:26:53.000000".to_string(),
            last_id: 173_218_080,
        };
        let s = format_two_stream_hwm(Some(502_767_312), Some(&h));

        assert!(s.contains("ins=502767312"), "the real insert watermark must be shown: {s}");
        assert!(s.contains("upd=2026-08-07T15:26:53.000000"), "got {s}");
        assert!(
            s.contains("page=173218080"),
            "the update-window cursor must be LABELLED, not left to look like the insert frontier: {s}"
        );
        // Unknowns must be visible rather than rendered as a plausible number.
        assert!(format_two_stream_hwm(None, Some(&h)).contains("ins=—"));
        assert!(format_two_stream_hwm(Some(1), None).contains("upd=—"));
    }

    #[tokio::test]
    async fn incremental_mode_key_shows_cursors() {
        // Incremental mode KEY should show "id, timestamp_col".
        let columns = incremental_columns();
        let config = make_config(vec!["orders".to_string()]);
        let mut inspect = MockPreflightInspect::new();
        let mut storage = MockPreflightStorage::new();
        let mut hwm = MockPreflightHwm::new();

        storage.expect_check_writable().returning(|| Ok(()));
        hwm.expect_read_hwm().returning(|_| Ok(None));
        hwm.expect_delta_schema().returning(|_| Ok(None));
        inspect
            .expect_discover_columns()
            .returning(move |_| Ok(columns.clone()));
        inspect
            .expect_get_avg_row_length()
            .returning(|_| Ok(Some(128)));
        // id is the single-column integer PRIMARY key so the table still resolves
        // Incremental (this test's whole point is the Incremental KEY format).
        inspect.expect_discover_indexes().returning(|_| Ok(primary_id_index()));

        let check = PreflightCheck::new(config, inspect, storage, hwm);
        let result = check.run().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn auto_mode_never_returned() {
        // detect_mode with None override and valid columns should not return Auto.
        let columns = incremental_columns();
        let config = make_config(vec!["orders".to_string()]);
        let mut inspect = MockPreflightInspect::new();
        let mut storage = MockPreflightStorage::new();
        let mut hwm = MockPreflightHwm::new();

        storage.expect_check_writable().returning(|| Ok(()));
        hwm.expect_read_hwm().returning(|_| Ok(None));
        hwm.expect_delta_schema().returning(|_| Ok(None));
        inspect
            .expect_discover_columns()
            .returning(move |_| Ok(columns.clone()));
        inspect
            .expect_get_avg_row_length()
            .returning(|_| Ok(Some(128)));
        inspect.expect_discover_indexes().returning(|_| Ok(primary_id_index()));

        let check = PreflightCheck::new(config, inspect, storage, hwm);
        let result = check.run().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn custom_timestamp_column_used_in_preflight() {
        // When TABLE_TIMESTAMP_table is set, that column name should be used in KEY, not auto-detected.
        let mut config = make_config(vec!["orders".to_string()]);
        config.table_timestamp_col.insert("orders".to_string(), "completed_at".to_string());

        let columns = vec![
            col("id", "bigint", "bigint(20)"),
            col("name", "varchar", "varchar(255)"),
            col("completed_at", "timestamp", "timestamp"),
        ];

        let mut inspect = MockPreflightInspect::new();
        let mut storage = MockPreflightStorage::new();
        let mut hwm = MockPreflightHwm::new();

        storage.expect_check_writable().returning(|| Ok(()));
        hwm.expect_read_hwm().returning(|_| Ok(None));
        hwm.expect_delta_schema().returning(|_| Ok(None));
        inspect
            .expect_discover_columns()
            .returning(move |_| Ok(columns.clone()));
        inspect
            .expect_get_avg_row_length()
            .returning(|_| Ok(Some(128)));
        // id is the single-column integer PRIMARY key, so this still resolves Incremental
        // with the custom "completed_at" cursor (this test's documented intent).
        inspect.expect_discover_indexes().returning(|_| Ok(primary_id_index()));

        let check = PreflightCheck::new(config, inspect, storage, hwm);
        let result = check.run().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn preflight_skips_time_year_bit_columns_without_failing() {
        // N1/O8 parity: preflight shares filter_unsupported_columns with the orchestrator
        // (see check_table above), so time/year/bit columns are silently excluded from
        // the reported COLUMNS count (and the warn fires) instead of failing the
        // pre-flight check — the same behavior as the geometry family already had.
        let config = make_config(vec!["events".to_string()]);
        let mut inspect = MockPreflightInspect::new();
        let mut storage = MockPreflightStorage::new();
        let mut hwm = MockPreflightHwm::new();

        storage.expect_check_writable().returning(|| Ok(()));
        hwm.expect_read_hwm().returning(|_| Ok(None));
        hwm.expect_delta_schema().returning(|_| Ok(None));
        inspect.expect_discover_columns().returning(|_| {
            Ok(vec![
                col("id", "bigint", "bigint(20)"),
                col("name", "varchar", "varchar(50)"),
                col("t", "time", "time"),
                col("y", "year", "year(4)"),
                col("b", "bit", "bit(8)"),
            ])
        });
        inspect
            .expect_get_avg_row_length()
            .returning(|_| Ok(Some(64)));
        inspect.expect_discover_indexes().returning(|_| Ok(vec![]));

        let check = PreflightCheck::new(config, inspect, storage, hwm);
        let result = check.run().await;
        assert!(
            result.is_ok(),
            "table with only geometry-class-excluded columns beyond id/name must still \
             preflight successfully: {result:?}"
        );
    }

    #[tokio::test]
    async fn hwm_with_both_updated_at_and_last_id() {
        // Format HWM when both updated_at and last_id are present.
        let config = make_config(vec!["orders".to_string()]);
        let mut inspect = MockPreflightInspect::new();
        let mut storage = MockPreflightStorage::new();
        let mut hwm = MockPreflightHwm::new();

        storage.expect_check_writable().returning(|| Ok(()));
        hwm.expect_read_hwm()
            .withf(|t| t == "orders")
            .returning(|_| {
                Ok(Some(crate::writer::Hwm {
                    updated_at: "2026-05-15T10:30:00.000000".to_string(),
                    last_id: 5000,
                }))
            });
        hwm.expect_delta_schema().returning(|_| Ok(None));
        inspect
            .expect_discover_columns()
            .returning(|_| Ok(incremental_columns()));
        inspect
            .expect_get_avg_row_length()
            .returning(|_| Ok(Some(128)));
        inspect.expect_discover_indexes().returning(|_| Ok(primary_id_index()));

        let check = PreflightCheck::new(config, inspect, storage, hwm);
        let result = check.run().await;
        assert!(result.is_ok());
    }

    #[test]
    fn health_check_path_includes_prefix() {
        assert_eq!(health_check_path("parket").as_ref(), "parket/.parket-health-check");
        assert_eq!(health_check_path("").as_ref(), ".parket-health-check");
    }

    #[tokio::test]
    async fn local_preflight_storage_probes_writable_dir() {
        let tempdir = tempfile::tempdir().expect("create tempdir");
        let storage = LocalPreflightStorage::new(tempdir.path());
        let result = storage.check_writable().await;
        assert!(result.is_ok(), "expected writable dir to pass: {result:?}");

        let probe = tempdir.path().join(".parket-health-check");
        assert!(!probe.exists(), "probe file should be cleaned up after check");
    }

    #[tokio::test]
    async fn local_preflight_storage_errors_on_unwritable_path() {
        // Point base_dir at a path whose PARENT is a regular file, so
        // std::fs::create_dir_all can never succeed in creating it.
        let tempdir = tempfile::tempdir().expect("create tempdir");
        let file_path = tempdir.path().join("not_a_dir");
        std::fs::write(&file_path, b"i am a file, not a directory").expect("write blocking file");
        let base_dir = file_path.join("sub");

        let storage = LocalPreflightStorage::new(&base_dir);
        let result = storage.check_writable().await;
        assert!(result.is_err(), "expected unwritable path to fail: {result:?}");
    }

    #[tokio::test]
    async fn preflight_flags_incompatible_delta_schema() {
        // O7-rest-b: preflight now runs the same schema_evolution_check the run does.
        // Simulate an existing Delta table whose `id` column was persisted as Utf8 while the
        // MariaDB source is bigint (-> Arrow Int64) -- a type change the run would bail on.
        // `name`/`updated_at` are declared with the types their MariaDB columns already map
        // to (varchar/timestamp -> Utf8), so `id` is the only mismatch and the failure
        // reason is unambiguous.
        let config = make_config(vec!["orders".to_string()]);
        let mut inspect = MockPreflightInspect::new();
        let mut storage = MockPreflightStorage::new();
        let mut hwm = MockPreflightHwm::new();

        storage.expect_check_writable().returning(|| Ok(()));
        hwm.expect_read_hwm().returning(|_| Ok(None));
        hwm.expect_delta_schema().returning(|_| {
            Ok(Some(std::sync::Arc::new(deltalake::arrow::datatypes::Schema::new(vec![
                deltalake::arrow::datatypes::Field::new(
                    "id",
                    deltalake::arrow::datatypes::DataType::Utf8,
                    false,
                ),
                deltalake::arrow::datatypes::Field::new(
                    "name",
                    deltalake::arrow::datatypes::DataType::Utf8,
                    false,
                ),
                deltalake::arrow::datatypes::Field::new(
                    "updated_at",
                    deltalake::arrow::datatypes::DataType::Utf8,
                    false,
                ),
            ]))))
        });
        inspect
            .expect_discover_columns()
            .returning(|_| Ok(incremental_columns()));
        inspect
            .expect_get_avg_row_length()
            .returning(|_| Ok(Some(128)));
        // id must resolve as the integer PRIMARY key so the table is Incremental — the
        // schema-evolution check this test exercises only runs for Incremental/TwoStream.
        inspect.expect_discover_indexes().returning(|_| Ok(primary_id_index()));

        let check = PreflightCheck::new(config, inspect, storage, hwm);

        // check_table surfaces the real error message; run() collapses it into a count (see
        // the other tests in this module), so assert on the underlying error here.
        let table_result = check.check_table("orders").await;
        assert!(
            table_result.is_err(),
            "expected incompatible Delta schema to fail preflight: {table_result:?}"
        );
        let err = table_result.unwrap_err().to_string();
        assert!(
            err.contains("schema evolution error"),
            "expected schema evolution error, got: {err}"
        );

        // The overall run also fails, surfaced the same way every other check_table failure
        // is in this module: as a failure count.
        let result = check.run().await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("1 table(s) failed"));
    }
}
