use anyhow::{bail, Result};
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;
use tracing::{error, info};

use crate::config::Config;
use crate::discovery::{detect_mode, filter_unsupported_columns, ColumnInfo, SchemaInspector};

#[cfg_attr(test, mockall::automock)]
#[allow(async_fn_in_trait)]
pub trait PreflightInspect: Send + Sync {
    async fn discover_columns(&self, table: &str) -> Result<Vec<ColumnInfo>>;
    async fn get_avg_row_length(&self, table: &str) -> Result<Option<u64>>;
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
            info!("S3 bucket connectivity check passed");
        } else {
            info!("S3 connectivity check skipped (local mode)");
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
            crate::config::ExtractionMode::TwoStream
        } else {
            let mode_override = self.config.table_modes.get(table_name);
            detect_mode(&columns, mode_override, &ts_col)
        };

        if !matches!(mode, crate::config::ExtractionMode::Incremental | crate::config::ExtractionMode::TwoStream)
            && self.config.table_initial_hwm.contains_key(table_name)
        {
            anyhow::bail!("TABLE_HWM_{table_name} set but table is not incremental or two-stream");
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
                crate::config::ExtractionMode::Incremental => format!("id, {ts_col}"),
                crate::config::ExtractionMode::FullRefresh => {
                    // Determine why it's FullRefresh
                    let has_id = columns.iter().any(|c| c.name == "id");
                    let has_ts = columns.iter().any(|c| {
                        c.name == ts_col
                            && (c.data_type == "timestamp" || c.data_type == "datetime")
                    });
                    match (has_id, has_ts) {
                        (false, false) => format!("no id/{ts_col}"),
                        (false, true) => "no id".to_string(),
                        (true, false) => format!("no {ts_col}"),
                        (true, true) => unreachable!(), // Would be Incremental
                    }
                }
                crate::config::ExtractionMode::TwoStream => unreachable!(), // Already handled above
                crate::config::ExtractionMode::Auto => unreachable!(), // detect_mode never returns Auto
            }
        };

        // Format HWM
        let hwm_str = match hwm {
            Some(h) => format!("{} / {}", h.updated_at, h.last_id),
            None => "—".to_string(),
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
}

pub struct PreflightStorageAdapter {
    s3_builder: AmazonS3Builder,
    bucket: String,
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
        }
    }
}

impl PreflightStorage for PreflightStorageAdapter {
    async fn check_writable(&self) -> Result<()> {
        let store = self.s3_builder.clone().build()?;
        let test_path = ObjectPath::from(".parket-health-check");
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
        }
    }

    fn col(name: &str, data_type: &str, column_type: &str) -> ColumnInfo {
        ColumnInfo {
            name: name.to_string(),
            data_type: data_type.to_string(),
            column_type: column_type.to_string(),
        }
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
        inspect
            .expect_discover_columns()
            .withf(|t| t == "orders")
            .returning(|_| Ok(incremental_columns()));
        inspect
            .expect_get_avg_row_length()
            .withf(|t| t == "orders")
            .returning(|_| Ok(Some(128)));

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
        inspect
            .expect_discover_columns()
            .returning(|_| Ok(full_refresh_columns()));
        inspect
            .expect_get_avg_row_length()
            .returning(|_| Ok(Some(100)));

        let check = PreflightCheck::new(config, inspect, storage, hwm);
        let result = check.run().await;
        assert!(result.is_ok());
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
        hwm.expect_read_hwm().returning(|_| Ok(None));

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
        inspect
            .expect_discover_columns()
            .returning(|_| Ok(full_refresh_columns()));
        inspect
            .expect_get_avg_row_length()
            .returning(|_| Ok(Some(100)));

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
        inspect
            .expect_discover_columns()
            .returning(|_| Ok(incremental_columns()));
        inspect
            .expect_get_avg_row_length()
            .returning(|_| Ok(Some(128)));

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

        let check = PreflightCheck::new(config, inspect, storage, hwm);
        let result = check.run().await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("1 table(s) failed"), "error message: {err}");
    }
}
