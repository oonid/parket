use std::collections::HashMap;
use std::path::PathBuf;

use anyhow::{bail, Context, Result};

mod parse;
mod mask;

use parse::*;
pub use mask::{mask_database_url, mask_secret};

#[derive(Debug, Clone)]
pub struct Config {
    pub database_url: String,
    pub s3_bucket: String,
    pub s3_access_key_id: String,
    pub s3_secret_access_key: String,
    pub tables: Vec<String>,
    pub target_memory_mb: u64,
    /// Memory budget (MB) for the two-stream MERGE's datafusion session (bounded
    /// FairSpillPool). Independent of `target_memory_mb`; defaults to it when unset.
    pub merge_memory_mb: u64,
    /// Optional spill directory for the MERGE's external sort; None = system temp.
    pub merge_spill_dir: Option<PathBuf>,
    pub s3_endpoint: Option<String>,
    pub s3_region: String,
    pub s3_prefix: String,
    pub default_batch_size: u64,
    pub rust_log: String,
    pub table_modes: HashMap<String, ExtractionMode>,
    pub table_initial_hwm: HashMap<String, (String, i64)>,
    pub table_timestamp_col: HashMap<String, String>,
    pub table_insert_cursor: HashMap<String, String>,
    pub table_update_cursor: HashMap<String, String>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ExtractionMode {
    Auto,
    Incremental,
    FullRefresh,
    TwoStream,
}

impl Config {
    pub fn timestamp_col(&self, table: &str) -> &str {
        self.table_timestamp_col.get(table).map(|s| s.as_str()).unwrap_or("updated_at")
    }

    /// Returns (insert_cursor, update_cursor) when both are configured for the table.
    pub fn two_stream(&self, table: &str) -> Option<(String, String)> {
        match (self.table_insert_cursor.get(table), self.table_update_cursor.get(table)) {
            (Some(i), Some(u)) => Some((i.clone(), u.clone())),
            _ => None,
        }
    }

    /// Minimal load for `--inspect`: only DATABASE_URL is required.
    pub fn load_inspect() -> Result<String> {
        let _ = dotenvy::dotenv();
        let database_url = env("DATABASE_URL")?;
        validate_database_url(&database_url)?;
        Ok(database_url)
    }

    pub fn load() -> Result<Self> {
        let _ = dotenvy::dotenv();

        let database_url = env("DATABASE_URL")?;
        let s3_bucket = env("S3_BUCKET")?;
        let s3_access_key_id = env("S3_ACCESS_KEY_ID")?;
        let s3_secret_access_key = env("S3_SECRET_ACCESS_KEY")?;
        let tables_raw = env("TABLES")?;
        let target_memory_mb_raw = env("TARGET_MEMORY_MB")?;

        validate_database_url(&database_url)?;

        let tables = parse_tables(&tables_raw)?;
        if tables.is_empty() {
            bail!("TABLES must not be empty");
        }

        let target_memory_mb: u64 = target_memory_mb_raw
            .parse()
            .context("TARGET_MEMORY_MB must be a positive integer")?;
        if target_memory_mb == 0 {
            bail!("TARGET_MEMORY_MB must be greater than 0");
        }

        // MERGE memory budget: optional, defaults to TARGET_MEMORY_MB. Bounds the
        // two-stream MERGE's datafusion session so it spills to disk instead of OOM.
        let merge_memory_mb: u64 = std::env::var("MERGE_MEMORY_MB")
            .ok()
            .filter(|s| !s.is_empty())
            .map(|s| s.parse())
            .transpose()
            .context("MERGE_MEMORY_MB must be a positive integer")?
            .unwrap_or(target_memory_mb);
        if merge_memory_mb == 0 {
            bail!("MERGE_MEMORY_MB must be greater than 0");
        }
        let merge_spill_dir = std::env::var("MERGE_SPILL_DIR")
            .ok()
            .filter(|s| !s.is_empty())
            .map(PathBuf::from);

        let s3_endpoint = std::env::var("S3_ENDPOINT").ok().filter(|s| !s.is_empty());
        let s3_region = std::env::var("S3_REGION")
            .ok()
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "us-east-1".to_string());
        let s3_prefix = std::env::var("S3_PREFIX")
            .ok()
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "parket".to_string());
        let default_batch_size: u64 = std::env::var("DEFAULT_BATCH_SIZE")
            .ok()
            .filter(|s| !s.is_empty())
            .map(|s| s.parse())
            .transpose()
            .context("DEFAULT_BATCH_SIZE must be a positive integer")?
            .unwrap_or(10000);
        let rust_log = std::env::var("RUST_LOG")
            .ok()
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "info".to_string());

        let table_modes = parse_table_modes(&tables);
        let table_initial_hwm = parse_table_initial_hwm(&tables)?;
        let table_timestamp_col = parse_table_timestamp_col(&tables);
        let table_insert_cursor = parse_table_insert_cursor(&tables);
        let table_update_cursor = parse_table_update_cursor(&tables);

        Ok(Self {
            database_url,
            s3_bucket,
            s3_access_key_id,
            s3_secret_access_key,
            tables,
            target_memory_mb,
            merge_memory_mb,
            merge_spill_dir,
            s3_endpoint,
            s3_region,
            s3_prefix,
            default_batch_size,
            rust_log,
            table_modes,
            table_initial_hwm,
            table_timestamp_col,
            table_insert_cursor,
            table_update_cursor,
        })
    }

    pub fn load_local() -> Result<Self> {
        let _ = dotenvy::dotenv();

        let database_url = env("DATABASE_URL")?;
        let tables_raw = env("TABLES")?;
        let target_memory_mb_raw = env("TARGET_MEMORY_MB")?;

        validate_database_url(&database_url)?;

        let tables = parse_tables(&tables_raw)?;
        if tables.is_empty() {
            bail!("TABLES must not be empty");
        }

        let target_memory_mb: u64 = target_memory_mb_raw
            .parse()
            .context("TARGET_MEMORY_MB must be a positive integer")?;
        if target_memory_mb == 0 {
            bail!("TARGET_MEMORY_MB must be greater than 0");
        }

        // MERGE memory budget: optional, defaults to TARGET_MEMORY_MB. Bounds the
        // two-stream MERGE's datafusion session so it spills to disk instead of OOM.
        let merge_memory_mb: u64 = std::env::var("MERGE_MEMORY_MB")
            .ok()
            .filter(|s| !s.is_empty())
            .map(|s| s.parse())
            .transpose()
            .context("MERGE_MEMORY_MB must be a positive integer")?
            .unwrap_or(target_memory_mb);
        if merge_memory_mb == 0 {
            bail!("MERGE_MEMORY_MB must be greater than 0");
        }
        let merge_spill_dir = std::env::var("MERGE_SPILL_DIR")
            .ok()
            .filter(|s| !s.is_empty())
            .map(PathBuf::from);

        let s3_endpoint = std::env::var("S3_ENDPOINT").ok().filter(|s| !s.is_empty());
        let s3_region = std::env::var("S3_REGION")
            .ok()
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "us-east-1".to_string());
        let s3_prefix = std::env::var("S3_PREFIX")
            .ok()
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "parket".to_string());
        let default_batch_size: u64 = std::env::var("DEFAULT_BATCH_SIZE")
            .ok()
            .filter(|s| !s.is_empty())
            .map(|s| s.parse())
            .transpose()
            .context("DEFAULT_BATCH_SIZE must be a positive integer")?
            .unwrap_or(10000);
        let rust_log = std::env::var("RUST_LOG")
            .ok()
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "info".to_string());

        let table_modes = parse_table_modes(&tables);
        let table_initial_hwm = parse_table_initial_hwm(&tables)?;
        let table_timestamp_col = parse_table_timestamp_col(&tables);
        let table_insert_cursor = parse_table_insert_cursor(&tables);
        let table_update_cursor = parse_table_update_cursor(&tables);

        Ok(Self {
            database_url,
            s3_bucket: String::new(),
            s3_access_key_id: String::new(),
            s3_secret_access_key: String::new(),
            tables,
            target_memory_mb,
            merge_memory_mb,
            merge_spill_dir,
            s3_endpoint,
            s3_region,
            s3_prefix,
            default_batch_size,
            rust_log,
            table_modes,
            table_initial_hwm,
            table_timestamp_col,
            table_insert_cursor,
            table_update_cursor,
        })
    }

    pub fn display_safe(&self) -> String {
        let masked_url = mask_database_url(&self.database_url);
        let masked_secret = mask_secret(&self.s3_secret_access_key);
        let tables_joined = self.tables.join(", ");
        format!(
            "database_url={masked_url} s3_bucket={} s3_access_key_id={} s3_secret_access_key={masked_secret} tables=[{tables_joined}] target_memory_mb={} s3_region={} s3_prefix={} default_batch_size={}",
            self.s3_bucket,
            self.s3_access_key_id,
            self.target_memory_mb,
            self.s3_region,
            self.s3_prefix,
            self.default_batch_size,
        )
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;
    use std::env;

    const ALL_CONFIG_VARS: &[&str] = &[
        "DATABASE_URL",
        "S3_BUCKET",
        "S3_ACCESS_KEY_ID",
        "S3_SECRET_ACCESS_KEY",
        "TABLES",
        "TARGET_MEMORY_MB",
        "MERGE_MEMORY_MB",
        "MERGE_SPILL_DIR",
        "S3_ENDPOINT",
        "S3_REGION",
        "S3_PREFIX",
        "DEFAULT_BATCH_SIZE",
        "RUST_LOG",
    ];

    fn clear_config_env() {
        unsafe {
            for var in ALL_CONFIG_VARS {
                env::remove_var(var);
            }
            for (key, _) in env::vars().filter(|(k, _)| k.starts_with("TABLE_MODE_")) {
                env::remove_var(&key);
            }
            for (key, _) in env::vars().filter(|(k, _)| k.starts_with("TABLE_HWM_")) {
                env::remove_var(&key);
            }
            for (key, _) in env::vars().filter(|(k, _)| k.starts_with("TABLE_TIMESTAMP_")) {
                env::remove_var(&key);
            }
            for (key, _) in env::vars().filter(|(k, _)| k.starts_with("TABLE_INSERT_CURSOR_")) {
                env::remove_var(&key);
            }
            for (key, _) in env::vars().filter(|(k, _)| k.starts_with("TABLE_UPDATE_CURSOR_")) {
                env::remove_var(&key);
            }
        }
    }

    fn set_required_vars() {
        unsafe {
            env::set_var("DATABASE_URL", "mysql://user:pass@host:3306/dbname");
            env::set_var("S3_BUCKET", "data-lake");
            env::set_var("S3_ACCESS_KEY_ID", "minioadmin");
            env::set_var("S3_SECRET_ACCESS_KEY", "minioadmin");
            env::set_var("TABLES", "orders,customers,products");
            env::set_var("TARGET_MEMORY_MB", "512");
        }
    }

    struct CwdGuard(std::path::PathBuf);
    impl Drop for CwdGuard {
        fn drop(&mut self) {
            let _ = std::env::set_current_dir(&self.0);
        }
    }

    fn no_dotenv() -> CwdGuard {
        let original = std::env::current_dir()
            .unwrap_or_else(|_| std::path::PathBuf::from("/"));
        let _ = std::env::set_current_dir("/tmp");
        CwdGuard(original)
    }

    #[test]
    #[serial]
    fn load_valid_config_with_all_required_vars() {
        clear_config_env();
        set_required_vars();

        let config = Config::load().expect("load should succeed");

        assert_eq!(config.database_url, "mysql://user:pass@host:3306/dbname");
        assert_eq!(config.s3_bucket, "data-lake");
        assert_eq!(config.s3_access_key_id, "minioadmin");
        assert_eq!(config.s3_secret_access_key, "minioadmin");
        assert_eq!(config.tables, vec!["orders", "customers", "products"]);
        assert_eq!(config.target_memory_mb, 512);
    }

    #[test]
    #[serial]
    fn load_fails_when_database_url_missing() {
        let _guard = no_dotenv();
        clear_config_env();
        set_required_vars();
        unsafe {
            env::remove_var("DATABASE_URL");
        }

        let result = Config::load();
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.to_lowercase().contains("database_url"),
            "error should mention DATABASE_URL, got: {err_msg}"
        );
    }

    #[test]
    #[serial]
    fn load_fails_when_s3_bucket_missing() {
        clear_config_env();
        set_required_vars();
        unsafe {
            env::remove_var("S3_BUCKET");
        }

        let result = Config::load();
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.to_lowercase().contains("s3_bucket"),
            "error should mention S3_BUCKET, got: {err_msg}"
        );
    }

    #[test]
    #[serial]
    fn load_fails_when_s3_access_key_id_missing() {
        clear_config_env();
        set_required_vars();
        unsafe {
            env::remove_var("S3_ACCESS_KEY_ID");
        }

        let result = Config::load();
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.to_lowercase().contains("s3_access_key_id"),
            "error should mention S3_ACCESS_KEY_ID, got: {err_msg}"
        );
    }

    #[test]
    #[serial]
    fn load_fails_when_s3_secret_access_key_missing() {
        clear_config_env();
        set_required_vars();
        unsafe {
            env::remove_var("S3_SECRET_ACCESS_KEY");
        }

        let result = Config::load();
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.to_lowercase().contains("s3_secret_access_key"),
            "error should mention S3_SECRET_ACCESS_KEY, got: {err_msg}"
        );
    }

    #[test]
    #[serial]
    fn load_fails_when_tables_missing() {
        let _guard = no_dotenv();
        clear_config_env();
        set_required_vars();
        unsafe {
            env::remove_var("TABLES");
        }

        let result = Config::load();
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.to_lowercase().contains("tables"),
            "error should mention TABLES, got: {err_msg}"
        );
    }

    #[test]
    #[serial]
    fn load_fails_when_target_memory_mb_missing() {
        let _guard = no_dotenv();
        clear_config_env();
        set_required_vars();
        unsafe {
            env::remove_var("TARGET_MEMORY_MB");
        }

        let result = Config::load();
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.to_lowercase().contains("target_memory_mb"),
            "error should mention TARGET_MEMORY_MB, got: {err_msg}"
        );
    }

    #[test]
    #[serial]
    fn load_fails_when_tables_empty() {
        clear_config_env();
        set_required_vars();
        unsafe {
            env::set_var("TABLES", "");
        }

        let result = Config::load();
        assert!(result.is_err());
    }

    #[test]
    #[serial]
    fn load_fails_when_tables_whitespace_only() {
        clear_config_env();
        set_required_vars();
        unsafe {
            env::set_var("TABLES", "   ");
        }

        let result = Config::load();
        assert!(result.is_err());
    }

    #[test]
    #[serial]
    fn load_fails_when_database_url_wrong_scheme() {
        clear_config_env();
        set_required_vars();
        unsafe {
            env::set_var("DATABASE_URL", "postgres://user:pass@host:5432/db");
        }

        let result = Config::load();
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.to_lowercase().contains("mysql"),
            "error should mention mysql scheme, got: {err_msg}"
        );
    }

    #[test]
    #[serial]
    fn load_fails_when_target_memory_mb_zero() {
        clear_config_env();
        set_required_vars();
        unsafe {
            env::set_var("TARGET_MEMORY_MB", "0");
        }

        let result = Config::load();
        assert!(result.is_err());
    }

    #[test]
    #[serial]
    fn load_fails_when_target_memory_mb_negative() {
        clear_config_env();
        set_required_vars();
        unsafe {
            env::set_var("TARGET_MEMORY_MB", "-1");
        }

        let result = Config::load();
        assert!(result.is_err());
    }

    #[test]
    #[serial]
    fn merge_memory_mb_defaults_to_target_when_unset() {
        let _cwd = no_dotenv(); // isolate from a real .env that may set MERGE_MEMORY_MB
        clear_config_env();
        set_required_vars(); // TARGET_MEMORY_MB=512, MERGE_MEMORY_MB unset

        let config = Config::load().expect("load should succeed");

        assert_eq!(config.merge_memory_mb, 512, "should default to target_memory_mb");
        assert_eq!(config.merge_spill_dir, None, "spill dir defaults to None (system temp)");
    }

    #[test]
    #[serial]
    fn merge_memory_mb_override_parsed() {
        let _cwd = no_dotenv();
        clear_config_env();
        set_required_vars();
        unsafe {
            env::set_var("MERGE_MEMORY_MB", "2048");
        }

        let config = Config::load().expect("load should succeed");

        assert_eq!(config.merge_memory_mb, 2048);
        assert_eq!(config.target_memory_mb, 512, "extract budget stays independent");
    }

    #[test]
    #[serial]
    fn load_fails_when_merge_memory_mb_zero() {
        let _cwd = no_dotenv();
        clear_config_env();
        set_required_vars();
        unsafe {
            env::set_var("MERGE_MEMORY_MB", "0");
        }

        assert!(Config::load().is_err());
    }

    #[test]
    #[serial]
    fn load_fails_when_merge_memory_mb_non_numeric() {
        let _cwd = no_dotenv();
        clear_config_env();
        set_required_vars();
        unsafe {
            env::set_var("MERGE_MEMORY_MB", "lots");
        }

        assert!(Config::load().is_err());
    }

    #[test]
    #[serial]
    fn merge_spill_dir_parsed_when_set() {
        let _cwd = no_dotenv();
        clear_config_env();
        set_required_vars();
        unsafe {
            env::set_var("MERGE_SPILL_DIR", "/tmp/parket-spill");
        }

        let config = Config::load().expect("load should succeed");

        assert_eq!(config.merge_spill_dir, Some(PathBuf::from("/tmp/parket-spill")));
    }

    #[test]
    #[serial]
    fn load_fails_when_target_memory_mb_non_numeric() {
        clear_config_env();
        set_required_vars();
        unsafe {
            env::set_var("TARGET_MEMORY_MB", "abc");
        }

        let result = Config::load();
        assert!(result.is_err());
    }

    #[test]
    #[serial]
    fn load_fails_when_database_url_empty() {
        clear_config_env();
        set_required_vars();
        unsafe {
            env::set_var("DATABASE_URL", "");
        }

        let result = Config::load();
        assert!(result.is_err());
    }

    #[test]
    #[serial]
    fn load_fails_when_s3_bucket_empty() {
        clear_config_env();
        set_required_vars();
        unsafe {
            env::set_var("S3_BUCKET", "");
        }

        let result = Config::load();
        assert!(result.is_err());
    }

    #[test]
    #[serial]
    fn load_fails_when_default_batch_size_non_numeric() {
        clear_config_env();
        set_required_vars();
        unsafe {
            env::set_var("DEFAULT_BATCH_SIZE", "not_a_number");
        }

        let result = Config::load();
        assert!(result.is_err());
    }

    #[test]
    #[serial]
    fn load_uses_defaults_for_optional_vars() {
        clear_config_env();
        set_required_vars();

        let config = Config::load().expect("load should succeed");

        assert_eq!(config.s3_endpoint, None);
        assert_eq!(config.s3_region, "us-east-1");
        assert_eq!(config.s3_prefix, "parket");
        assert_eq!(config.default_batch_size, 10000);
        assert_eq!(config.rust_log, "info");
    }

    #[test]
    #[serial]
    fn load_uses_provided_optional_vars() {
        clear_config_env();
        set_required_vars();
        unsafe {
            env::set_var("S3_ENDPOINT", "http://localhost:9000");
            env::set_var("S3_REGION", "eu-west-1");
            env::set_var("S3_PREFIX", "custom-prefix");
            env::set_var("DEFAULT_BATCH_SIZE", "5000");
            env::set_var("RUST_LOG", "parket=debug");
        }

        let config = Config::load().expect("load should succeed");

        assert_eq!(
            config.s3_endpoint,
            Some("http://localhost:9000".to_string())
        );
        assert_eq!(config.s3_region, "eu-west-1");
        assert_eq!(config.s3_prefix, "custom-prefix");
        assert_eq!(config.default_batch_size, 5000);
        assert_eq!(config.rust_log, "parket=debug");
    }

    #[test]
    #[serial]
    fn parse_single_table() {
        clear_config_env();
        set_required_vars();
        unsafe {
            env::set_var("TABLES", "orders");
        }

        let config = Config::load().expect("load should succeed");
        assert_eq!(config.tables, vec!["orders"]);
    }

    #[test]
    #[serial]
    fn parse_tables_trims_whitespace() {
        clear_config_env();
        set_required_vars();
        unsafe {
            env::set_var("TABLES", "orders, customers, products");
        }

        let config = Config::load().expect("load should succeed");
        assert_eq!(config.tables, vec!["orders", "customers", "products"]);
    }

    #[test]
    #[serial]
    fn per_table_mode_override_incremental() {
        clear_config_env();
        set_required_vars();
        unsafe {
            env::set_var("TABLE_MODE_orders", "incremental");
        }

        let config = Config::load().expect("load should succeed");

        assert_eq!(
            config.table_modes.get("orders"),
            Some(&ExtractionMode::Incremental)
        );
    }

    #[test]
    #[serial]
    fn per_table_mode_override_full_refresh() {
        clear_config_env();
        set_required_vars();
        unsafe {
            env::set_var("TABLE_MODE_customers", "full_refresh");
        }

        let config = Config::load().expect("load should succeed");

        assert_eq!(
            config.table_modes.get("customers"),
            Some(&ExtractionMode::FullRefresh)
        );
    }

    #[test]
    #[serial]
    fn per_table_mode_defaults_to_auto() {
        clear_config_env();
        set_required_vars();

        let config = Config::load().expect("load should succeed");

        assert_eq!(config.table_modes.get("orders"), None);
    }

    #[test]
    #[serial]
    fn per_table_mode_override_auto_explicit() {
        clear_config_env();
        set_required_vars();
        unsafe {
            env::set_var("TABLE_MODE_orders", "auto");
        }

        let config = Config::load().expect("load should succeed");

        assert_eq!(
            config.table_modes.get("orders"),
            Some(&ExtractionMode::Auto)
        );
    }

    #[test]
    #[serial]
    fn multiple_per_table_overrides() {
        clear_config_env();
        set_required_vars();
        unsafe {
            env::set_var("TABLE_MODE_orders", "incremental");
            env::set_var("TABLE_MODE_customers", "full_refresh");
        }

        let config = Config::load().expect("load should succeed");

        assert_eq!(
            config.table_modes.get("orders"),
            Some(&ExtractionMode::Incremental)
        );
        assert_eq!(
            config.table_modes.get("customers"),
            Some(&ExtractionMode::FullRefresh)
        );
        assert_eq!(config.table_modes.get("products"), None);
    }

    #[test]
    #[serial]
    fn parse_table_initial_hwm_valid_single_entry() {
        clear_config_env();
        unsafe {
            env::set_var("TABLE_HWM_orders", "2026-01-01T00:00:00.000000,1000");
        }

        let result = parse_table_initial_hwm(&["orders".to_string()]);
        assert!(result.is_ok());
        let map = result.unwrap();
        assert_eq!(
            map.get("orders"),
            Some(&("2026-01-01T00:00:00.000000".to_string(), 1000))
        );
    }

    #[test]
    #[serial]
    fn parse_table_initial_hwm_missing_comma() {
        clear_config_env();
        unsafe {
            env::set_var("TABLE_HWM_orders", "2026-01-01T00:00:00.000000");
        }

        let result = parse_table_initial_hwm(&["orders".to_string()]);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("must be '<updated_at>,<last_id>'"));
    }

    #[test]
    #[serial]
    fn parse_table_initial_hwm_non_numeric_last_id() {
        clear_config_env();
        unsafe {
            env::set_var("TABLE_HWM_orders", "2026-01-01T00:00:00.000000,not_a_number");
        }

        let result = parse_table_initial_hwm(&["orders".to_string()]);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("is not a valid i64"));
    }

    #[test]
    #[serial]
    fn parse_table_initial_hwm_empty_updated_at() {
        clear_config_env();
        unsafe {
            env::set_var("TABLE_HWM_orders", ",5");
        }

        let result = parse_table_initial_hwm(&["orders".to_string()]);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("updated_at must not be empty"));
    }

    #[test]
    #[serial]
    fn parse_table_initial_hwm_absent_var() {
        clear_config_env();

        let result = parse_table_initial_hwm(&["orders".to_string()]);
        assert!(result.is_ok());
        let map = result.unwrap();
        assert!(map.is_empty());
    }

    #[test]
    #[serial]
    fn parse_table_initial_hwm_underscore_table_name() {
        clear_config_env();
        unsafe {
            env::set_var("TABLE_HWM_my_orders", "2026-05-01T00:00:00.000000,999");
        }

        let result = parse_table_initial_hwm(&["my_orders".to_string()]);
        assert!(result.is_ok());
        let map = result.unwrap();
        assert_eq!(
            map.get("my_orders"),
            Some(&("2026-05-01T00:00:00.000000".to_string(), 999))
        );
    }

    #[test]
    #[serial]
    fn display_safe_masks_password_in_database_url() {
        clear_config_env();
        set_required_vars();
        let config = Config::load().expect("load should succeed");
        let display = config.display_safe();
        assert!(
            !display.contains("pass"),
            "display_safe should mask password, got: {display}"
        );
        assert!(
            display.contains("****:****"),
            "display_safe should show masked credentials, got: {display}"
        );
    }

    #[test]
    #[serial]
    fn display_safe_masks_s3_secret_key() {
        clear_config_env();
        set_required_vars();
        let config = Config::load().expect("load should succeed");
        let display = config.display_safe();
        assert!(
            !display.contains("minioadmin") || display.contains("****"),
            "display_safe should mask S3 secret, got: {display}"
        );
    }

    #[test]
    #[serial]
    fn load_local_succeeds_without_s3_vars() {
        clear_config_env();
        unsafe {
            env::set_var("DATABASE_URL", "mysql://user:pass@host:3306/dbname");
            env::set_var("TABLES", "orders,customers");
            env::set_var("TARGET_MEMORY_MB", "256");
        }

        let config = Config::load_local().expect("load_local should succeed without S3 vars");
        assert_eq!(config.database_url, "mysql://user:pass@host:3306/dbname");
        assert_eq!(config.tables, vec!["orders", "customers"]);
        assert_eq!(config.target_memory_mb, 256);
        assert!(config.s3_bucket.is_empty());
        assert!(config.s3_access_key_id.is_empty());
        assert!(config.s3_secret_access_key.is_empty());
    }

    #[test]
    #[serial]
    fn load_local_fails_without_database_url() {
        let _guard = no_dotenv();
        clear_config_env();
        unsafe {
            env::set_var("TABLES", "orders");
            env::set_var("TARGET_MEMORY_MB", "512");
        }

        let result = Config::load_local();
        assert!(result.is_err());
    }

    #[test]
    #[serial]
    fn load_local_fails_without_tables() {
        let _guard = no_dotenv();
        clear_config_env();
        unsafe {
            env::set_var("DATABASE_URL", "mysql://user:pass@host:3306/dbname");
            env::set_var("TARGET_MEMORY_MB", "512");
        }

        let result = Config::load_local();
        assert!(result.is_err());
    }

    #[test]
    #[serial]
    fn load_local_fails_without_target_memory() {
        let _guard = no_dotenv();
        clear_config_env();
        unsafe {
            env::set_var("DATABASE_URL", "mysql://user:pass@host:3306/dbname");
            env::set_var("TABLES", "orders");
        }

        let result = Config::load_local();
        assert!(result.is_err());
    }

    #[test]
    #[serial]
    fn load_local_fails_with_wrong_scheme() {
        clear_config_env();
        unsafe {
            env::set_var("DATABASE_URL", "postgres://user:pass@host:5432/db");
            env::set_var("TABLES", "orders");
            env::set_var("TARGET_MEMORY_MB", "512");
        }

        let result = Config::load_local();
        assert!(result.is_err());
    }

    #[test]
    #[serial]
    fn load_local_fails_with_zero_memory() {
        clear_config_env();
        unsafe {
            env::set_var("DATABASE_URL", "mysql://user:pass@host:3306/dbname");
            env::set_var("TABLES", "orders");
            env::set_var("TARGET_MEMORY_MB", "0");
        }

        let result = Config::load_local();
        assert!(result.is_err());
    }

    #[test]
    #[serial]
    fn load_local_ignores_s3_vars_if_set() {
        clear_config_env();
        unsafe {
            env::set_var("DATABASE_URL", "mysql://user:pass@host:3306/dbname");
            env::set_var("TABLES", "orders");
            env::set_var("TARGET_MEMORY_MB", "128");
            env::set_var("S3_BUCKET", "should-be-ignored");
        }

        let config = Config::load_local().expect("load_local should succeed");
        assert!(config.s3_bucket.is_empty());
    }

    #[test]
    #[serial]
    fn timestamp_col_default_returns_updated_at() {
        clear_config_env();
        set_required_vars();

        let config = Config::load().expect("load should succeed");
        assert_eq!(config.timestamp_col("orders"), "updated_at");
    }

    #[test]
    #[serial]
    fn timestamp_col_override_present() {
        clear_config_env();
        set_required_vars();
        unsafe {
            env::set_var("TABLE_TIMESTAMP_orders", "completed_at");
        }

        let config = Config::load().expect("load should succeed");
        assert_eq!(config.timestamp_col("orders"), "completed_at");
    }

    #[test]
    #[serial]
    fn timestamp_col_override_absent_returns_default() {
        clear_config_env();
        set_required_vars();
        unsafe {
            env::set_var("TABLE_TIMESTAMP_orders", "completed_at");
        }

        let config = Config::load().expect("load should succeed");
        assert_eq!(config.timestamp_col("customers"), "updated_at");
    }

    #[test]
    #[serial]
    fn parse_table_timestamp_col_underscore_table_name() {
        clear_config_env();
        unsafe {
            env::set_var("TABLE_TIMESTAMP_my_table", "finished_at");
        }

        let result = parse_table_timestamp_col(&["my_table".to_string()]);
        assert_eq!(result.get("my_table").map(|s| s.as_str()), Some("finished_at"));
    }

    #[test]
    #[serial]
    fn parse_table_timestamp_col_empty_value_not_inserted() {
        clear_config_env();
        unsafe {
            env::set_var("TABLE_TIMESTAMP_orders", "");
        }

        let result = parse_table_timestamp_col(&["orders".to_string()]);
        assert!(result.is_empty());
    }

    #[test]
    #[serial]
    fn load_local_uses_optional_defaults() {
        clear_config_env();
        unsafe {
            env::set_var("DATABASE_URL", "mysql://user:pass@host:3306/dbname");
            env::set_var("TABLES", "orders");
            env::set_var("TARGET_MEMORY_MB", "512");
        }

        let config = Config::load_local().expect("load_local should succeed");
        assert_eq!(config.s3_region, "us-east-1");
        assert_eq!(config.s3_prefix, "parket");
        assert_eq!(config.default_batch_size, 10000);
        assert_eq!(config.rust_log, "info");
        assert!(config.s3_endpoint.is_none());
    }

    #[test]
    #[serial]
    fn load_inspect_valid_database_url() {
        clear_config_env();
        unsafe {
            env::set_var("DATABASE_URL", "mysql://user:pass@host:3306/dbname");
        }

        let result = Config::load_inspect();
        assert!(result.is_ok());
        let url = result.unwrap();
        assert_eq!(url, "mysql://user:pass@host:3306/dbname");
    }

    #[test]
    #[serial]
    fn load_inspect_missing_database_url() {
        let _guard = no_dotenv();
        clear_config_env();

        let result = Config::load_inspect();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("DATABASE_URL"));
    }

    #[test]
    #[serial]
    fn load_inspect_wrong_scheme() {
        clear_config_env();
        unsafe {
            env::set_var("DATABASE_URL", "postgres://user:pass@host:5432/db");
        }

        let result = Config::load_inspect();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .to_lowercase()
            .contains("mysql"));
    }

    #[test]
    #[serial]
    fn load_inspect_does_not_require_other_vars() {
        clear_config_env();
        unsafe {
            env::set_var("DATABASE_URL", "mysql://user:pass@host:3306/dbname");
        }

        let result = Config::load_inspect();
        assert!(result.is_ok(), "load_inspect should not require S3 or TABLES vars");
    }

    #[test]
    #[serial]
    fn parse_table_insert_cursor_both_set_returns_some() {
        clear_config_env();
        set_required_vars();
        unsafe {
            env::set_var("TABLE_INSERT_CURSOR_orders", "insert_id");
            env::set_var("TABLE_UPDATE_CURSOR_orders", "update_id");
        }

        let config = Config::load().expect("load should succeed");
        let result = config.two_stream("orders");
        assert_eq!(
            result,
            Some(("insert_id".to_string(), "update_id".to_string()))
        );
    }

    #[test]
    #[serial]
    fn parse_table_insert_cursor_only_insert_set_returns_none() {
        clear_config_env();
        set_required_vars();
        unsafe {
            env::set_var("TABLE_INSERT_CURSOR_orders", "insert_id");
        }

        let config = Config::load().expect("load should succeed");
        assert_eq!(config.two_stream("orders"), None);
        assert_eq!(
            config.table_insert_cursor.get("orders").map(|s| s.as_str()),
            Some("insert_id")
        );
    }

    #[test]
    #[serial]
    fn parse_table_insert_cursor_only_update_set_returns_none() {
        clear_config_env();
        set_required_vars();
        unsafe {
            env::set_var("TABLE_UPDATE_CURSOR_orders", "update_id");
        }

        let config = Config::load().expect("load should succeed");
        assert_eq!(config.two_stream("orders"), None);
        assert_eq!(
            config.table_update_cursor.get("orders").map(|s| s.as_str()),
            Some("update_id")
        );
    }

    #[test]
    #[serial]
    fn parse_table_insert_cursor_neither_set_returns_none() {
        clear_config_env();
        set_required_vars();

        let config = Config::load().expect("load should succeed");
        assert_eq!(config.two_stream("orders"), None);
        assert!(config.table_insert_cursor.is_empty());
        assert!(config.table_update_cursor.is_empty());
    }

    #[test]
    #[serial]
    fn parse_table_insert_cursor_underscore_table_name() {
        clear_config_env();
        unsafe {
            env::set_var("TABLE_INSERT_CURSOR_my_table", "cursor_a");
            env::set_var("TABLE_UPDATE_CURSOR_my_table", "cursor_b");
        }

        let result_insert = parse_table_insert_cursor(&["my_table".to_string()]);
        let result_update = parse_table_update_cursor(&["my_table".to_string()]);
        assert_eq!(result_insert.get("my_table").map(|s| s.as_str()), Some("cursor_a"));
        assert_eq!(result_update.get("my_table").map(|s| s.as_str()), Some("cursor_b"));
    }

    #[test]
    #[serial]
    fn parse_table_insert_cursor_empty_value_not_inserted() {
        clear_config_env();
        unsafe {
            env::set_var("TABLE_INSERT_CURSOR_orders", "");
            env::set_var("TABLE_UPDATE_CURSOR_orders", "");
        }

        let result_insert = parse_table_insert_cursor(&["orders".to_string()]);
        let result_update = parse_table_update_cursor(&["orders".to_string()]);
        assert!(result_insert.is_empty());
        assert!(result_update.is_empty());
    }

    #[test]
    #[serial]
    fn parse_table_insert_cursor_multiple_tables() {
        clear_config_env();
        unsafe {
            env::set_var("TABLE_INSERT_CURSOR_orders", "orders_insert");
            env::set_var("TABLE_UPDATE_CURSOR_orders", "orders_update");
            env::set_var("TABLE_INSERT_CURSOR_customers", "customers_insert");
            env::set_var("TABLE_UPDATE_CURSOR_customers", "customers_update");
        }

        let config = Config {
            database_url: "mysql://u:p@h/db".to_string(),
            s3_bucket: "bucket".to_string(),
            s3_access_key_id: "key".to_string(),
            s3_secret_access_key: "secret".to_string(),
            tables: vec!["orders".to_string(), "customers".to_string()],
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
            table_insert_cursor: parse_table_insert_cursor(&["orders".to_string(), "customers".to_string()]),
            table_update_cursor: parse_table_update_cursor(&["orders".to_string(), "customers".to_string()]),
        };

        assert_eq!(
            config.two_stream("orders"),
            Some(("orders_insert".to_string(), "orders_update".to_string()))
        );
        assert_eq!(
            config.two_stream("customers"),
            Some(("customers_insert".to_string(), "customers_update".to_string()))
        );
    }
}
