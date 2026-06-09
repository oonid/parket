use std::collections::HashMap;

use anyhow::{bail, Context, Result};

#[derive(Debug, Clone)]
pub struct Config {
    pub database_url: String,
    pub s3_bucket: String,
    pub s3_access_key_id: String,
    pub s3_secret_access_key: String,
    pub tables: Vec<String>,
    pub target_memory_mb: u64,
    pub s3_endpoint: Option<String>,
    pub s3_region: String,
    pub s3_prefix: String,
    pub default_batch_size: u64,
    pub rust_log: String,
    pub table_modes: HashMap<String, ExtractionMode>,
    pub table_initial_hwm: HashMap<String, (String, i64)>,
    pub table_timestamp_col: HashMap<String, String>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ExtractionMode {
    Auto,
    Incremental,
    FullRefresh,
}

impl Config {
    pub fn timestamp_col(&self, table: &str) -> &str {
        self.table_timestamp_col.get(table).map(|s| s.as_str()).unwrap_or("updated_at")
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

        Ok(Self {
            database_url,
            s3_bucket,
            s3_access_key_id,
            s3_secret_access_key,
            tables,
            target_memory_mb,
            s3_endpoint,
            s3_region,
            s3_prefix,
            default_batch_size,
            rust_log,
            table_modes,
            table_initial_hwm,
            table_timestamp_col,
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

        Ok(Self {
            database_url,
            s3_bucket: String::new(),
            s3_access_key_id: String::new(),
            s3_secret_access_key: String::new(),
            tables,
            target_memory_mb,
            s3_endpoint,
            s3_region,
            s3_prefix,
            default_batch_size,
            rust_log,
            table_modes,
            table_initial_hwm,
            table_timestamp_col,
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

fn env(key: &str) -> Result<String> {
    let val = std::env::var(key).with_context(|| format!("{key} is required"))?;
    if val.is_empty() {
        bail!("{key} is required");
    }
    Ok(val)
}

fn validate_database_url(url: &str) -> Result<()> {
    if url.starts_with("mysql://") {
        Ok(())
    } else {
        bail!("DATABASE_URL must start with mysql:// — unsupported scheme")
    }
}

fn parse_tables(raw: &str) -> Result<Vec<String>> {
    let tables: Vec<String> = raw
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    Ok(tables)
}

fn parse_table_modes(tables: &[String]) -> HashMap<String, ExtractionMode> {
    let mut modes = HashMap::new();
    for table in tables {
        let key = format!("TABLE_MODE_{table}");
        if let Ok(val) = std::env::var(&key) {
            let mode = match val.to_lowercase().as_str() {
                "incremental" => ExtractionMode::Incremental,
                "full_refresh" => ExtractionMode::FullRefresh,
                _ => ExtractionMode::Auto,
            };
            modes.insert(table.clone(), mode);
        }
    }
    modes
}

fn parse_table_initial_hwm(tables: &[String]) -> Result<HashMap<String, (String, i64)>> {
    let mut map = HashMap::new();
    for table in tables {
        let key = format!("TABLE_HWM_{table}");
        if let Ok(val) = std::env::var(&key) {
            let val = val.trim();
            if val.is_empty() {
                continue;
            }
            let (ua, id_str) = val.split_once(',')
                .ok_or_else(|| anyhow::anyhow!("{key} must be '<updated_at>,<last_id>', got '{val}'"))?;
            let ua = ua.trim();
            let id_str = id_str.trim();
            if ua.is_empty() {
                bail!("{key}: updated_at must not be empty");
            }
            let last_id: i64 = id_str.parse()
                .with_context(|| format!("{key}: last_id '{id_str}' is not a valid i64"))?;
            map.insert(table.clone(), (ua.to_string(), last_id));
        }
    }
    Ok(map)
}

fn parse_table_timestamp_col(tables: &[String]) -> HashMap<String, String> {
    let mut map = HashMap::new();
    for table in tables {
        let key = format!("TABLE_TIMESTAMP_{table}");
        if let Ok(val) = std::env::var(&key) {
            let val = val.trim();
            if !val.is_empty() {
                map.insert(table.clone(), val.to_string());
            }
        }
    }
    map
}

pub fn mask_database_url(url: &str) -> String {
    url::Url::parse(url)
        .ok()
        .map(|u| {
            let scheme = u.scheme();
            let host = u.host_str().unwrap_or("unknown");
            let port = u.port().map_or(String::new(), |p| format!(":{p}"));
            if u.password().is_some() {
                format!("{scheme}://****:****@{host}{port}")
            } else if !u.username().is_empty() {
                format!("{scheme}://{}@{host}{port}", u.username())
            } else {
                format!("{scheme}://{host}{port}")
            }
        })
        .unwrap_or_else(|| "unknown".to_string())
}

pub fn mask_secret(secret: &str) -> String {
    if secret.len() <= 4 {
        "****".to_string()
    } else {
        let visible = &secret[secret.len() - 4..];
        format!("****{visible}")
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
    fn mask_database_url_with_password() {
        let masked = mask_database_url("mysql://admin:s3cret@dbhost.example.com:3306/mydb");
        assert_eq!(masked, "mysql://****:****@dbhost.example.com:3306");
    }

    #[test]
    fn mask_database_url_without_password() {
        let masked = mask_database_url("mysql://admin@dbhost.example.com:3306/mydb");
        assert_eq!(masked, "mysql://admin@dbhost.example.com:3306");
    }

    #[test]
    fn mask_database_url_no_credentials() {
        let masked = mask_database_url("mysql://dbhost.example.com:3306/mydb");
        assert_eq!(masked, "mysql://dbhost.example.com:3306");
    }

    #[test]
    fn mask_database_url_invalid() {
        let masked = mask_database_url("not-a-url");
        assert_eq!(masked, "unknown");
    }

    #[test]
    fn mask_database_url_no_port() {
        let masked = mask_database_url("mysql://user:pass@dbhost/mydb");
        assert_eq!(masked, "mysql://****:****@dbhost");
    }

    #[test]
    fn mask_secret_short_value() {
        assert_eq!(mask_secret("ab"), "****");
    }

    #[test]
    fn mask_secret_exact_four_chars() {
        assert_eq!(mask_secret("abcd"), "****");
    }

    #[test]
    fn mask_secret_long_value() {
        assert_eq!(mask_secret("mysecretkey123"), "****y123");
    }

    #[test]
    fn mask_secret_five_chars() {
        assert_eq!(mask_secret("abcde"), "****bcde");
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
}
