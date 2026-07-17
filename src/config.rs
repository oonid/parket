use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

use anyhow::{bail, Context, Result};

mod parse;
mod mask;

use parse::*;
pub use mask::{mask_database_url, mask_secret};

#[derive(Clone)]
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
    /// PS-H-B: tables with `TABLE_RECONCILE_<table>=true` set for this run — a one-shot
    /// full snapshot (reusing the full-refresh atomic-overwrite path) whose final commit is
    /// ALSO stamped with the two-stream HWM keys, so the next (flag-removed) run resumes as
    /// normal two-stream incremental with no manual HWM seed.
    pub table_reconcile: HashSet<String>,
}

// S1: a derived Debug would print `database_url` (with password) and
// `s3_secret_access_key` verbatim through any `{config:?}`/`dbg!`/anyhow context.
// Hand-write Debug so those two fields are masked while everything else is shown
// normally.
impl std::fmt::Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Config")
            .field("database_url", &mask_database_url(&self.database_url))
            .field("s3_bucket", &self.s3_bucket)
            .field("s3_access_key_id", &self.s3_access_key_id)
            .field(
                "s3_secret_access_key",
                &mask_secret(&self.s3_secret_access_key),
            )
            .field("tables", &self.tables)
            .field("target_memory_mb", &self.target_memory_mb)
            .field("merge_memory_mb", &self.merge_memory_mb)
            .field("merge_spill_dir", &self.merge_spill_dir)
            .field("s3_endpoint", &self.s3_endpoint)
            .field("s3_region", &self.s3_region)
            .field("s3_prefix", &self.s3_prefix)
            .field("default_batch_size", &self.default_batch_size)
            .field("rust_log", &self.rust_log)
            .field("table_modes", &self.table_modes)
            .field("table_initial_hwm", &self.table_initial_hwm)
            .field("table_timestamp_col", &self.table_timestamp_col)
            .field("table_insert_cursor", &self.table_insert_cursor)
            .field("table_update_cursor", &self.table_update_cursor)
            .field("table_reconcile", &self.table_reconcile)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ExtractionMode {
    Auto,
    Incremental,
    FullRefresh,
    TwoStream,
}

/// FA9: `UPDATE_STRATEGY`, `MERGE_SORT_RESERVATION_MB`, and `MERGE_TARGET_PARTITIONS` are
/// read deep in the write path (orchestrator/two_stream.rs, writer/two_stream.rs) with no
/// validation — a typo'd or malformed value there silently falls back to a default instead
/// of being reported, so the operator's intent is lost without any indication. Validated here
/// at config load (both `Config::load` and `Config::load_local`) so a bad value bails loudly at
/// startup; the write path's direct env reads then only ever observe a validated value or unset.
fn validate_advanced_env_knobs() -> Result<()> {
    if let Ok(v) = std::env::var("UPDATE_STRATEGY")
        && !v.is_empty()
        && v != "merge"
    {
        bail!(
            "UPDATE_STRATEGY='{v}' is not recognized (only 'merge' selects the MERGE update \
             strategy; unset = default delete_then_append)"
        );
    }
    if let Ok(v) = std::env::var("MERGE_SORT_RESERVATION_MB")
        && !v.is_empty()
        && v.trim().parse::<usize>().is_err()
    {
        bail!("MERGE_SORT_RESERVATION_MB='{v}' must be a positive integer (MB)");
    }
    if let Ok(v) = std::env::var("MERGE_TARGET_PARTITIONS")
        && !v.is_empty()
    {
        let valid = v.trim().parse::<usize>().is_ok_and(|n| n > 0);
        if !valid {
            bail!("MERGE_TARGET_PARTITIONS='{v}' must be a positive integer greater than 0");
        }
    }
    Ok(())
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

    /// PS-H-B: true when `TABLE_RECONCILE_<table>=true` was set for this run — the table
    /// should run its one-shot full-snapshot-plus-HWM-reseed path instead of its normal mode.
    pub fn is_reconcile(&self, table: &str) -> bool {
        self.table_reconcile.contains(table)
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
        validate_memory_budget(target_memory_mb, merge_memory_mb, detect_total_ram_mb())?;
        validate_advanced_env_knobs()?;
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
        if default_batch_size == 0 {
            bail!("DEFAULT_BATCH_SIZE must be greater than 0");
        }
        let rust_log = std::env::var("RUST_LOG")
            .ok()
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "info".to_string());

        let table_modes = parse_table_modes(&tables)?;
        let table_initial_hwm = parse_table_initial_hwm(&tables)?;
        let table_timestamp_col = parse_table_timestamp_col(&tables);
        let table_insert_cursor = parse_table_insert_cursor(&tables);
        let table_update_cursor = parse_table_update_cursor(&tables);
        validate_mode_conflicts(&tables, &table_modes, &table_insert_cursor, &table_update_cursor)?;
        let table_reconcile = parse_table_reconcile(&tables)?;
        validate_reconcile_requirements(&table_reconcile, &table_modes, &table_insert_cursor, &table_update_cursor)?;

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
            table_reconcile,
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
        validate_memory_budget(target_memory_mb, merge_memory_mb, detect_total_ram_mb())?;
        validate_advanced_env_knobs()?;
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
        if default_batch_size == 0 {
            bail!("DEFAULT_BATCH_SIZE must be greater than 0");
        }
        let rust_log = std::env::var("RUST_LOG")
            .ok()
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "info".to_string());

        let table_modes = parse_table_modes(&tables)?;
        let table_initial_hwm = parse_table_initial_hwm(&tables)?;
        let table_timestamp_col = parse_table_timestamp_col(&tables);
        let table_insert_cursor = parse_table_insert_cursor(&tables);
        let table_update_cursor = parse_table_update_cursor(&tables);
        validate_mode_conflicts(&tables, &table_modes, &table_insert_cursor, &table_update_cursor)?;
        let table_reconcile = parse_table_reconcile(&tables)?;
        validate_reconcile_requirements(&table_reconcile, &table_modes, &table_insert_cursor, &table_update_cursor)?;

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
            table_reconcile,
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
        "UPDATE_STRATEGY",
        "MERGE_SORT_RESERVATION_MB",
        "MERGE_TARGET_PARTITIONS",
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
            for (key, _) in env::vars().filter(|(k, _)| k.starts_with("TABLE_RECONCILE_")) {
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
        // A real `.env` in an ancestor directory (this crate lives inside a larger workspace
        // checkout) would otherwise re-populate S3_BUCKET via `dotenvy::dotenv()` right after
        // this test removes it — `no_dotenv()` (cwd -> /tmp) defeats that upward search so the
        // test observes a genuinely-missing var, matching the other `no_dotenv()` uses above.
        let _guard = no_dotenv();
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
        // See `load_fails_when_s3_bucket_missing` above: guard against an ancestor `.env`
        // re-populating the var this test just removed.
        let _guard = no_dotenv();
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
        // See `load_fails_when_s3_bucket_missing` above: guard against an ancestor `.env`
        // re-populating the var this test just removed.
        let _guard = no_dotenv();
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
    fn load_fails_when_default_batch_size_zero() {
        clear_config_env();
        set_required_vars();
        unsafe {
            env::set_var("DEFAULT_BATCH_SIZE", "0");
        }

        let result = Config::load();
        assert!(result.is_err());
    }

    #[test]
    #[serial]
    fn load_uses_defaults_for_optional_vars() {
        // See `load_fails_when_s3_bucket_missing` above: guard against an ancestor `.env`
        // setting S3_REGION (among others) and masking the actual defaulting behavior.
        let _guard = no_dotenv();
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

    // O4: unknown TABLE_MODE_<t> values (typos, hyphenated variants, `two_stream`) used to
    // silently degrade to Auto, discarding operator intent. They must bail with an
    // actionable error instead.

    #[test]
    #[serial]
    fn table_mode_invalid_value_bails() {
        clear_config_env();
        set_required_vars();
        unsafe {
            env::set_var("TABLE_MODE_orders", "full-refresh");
        }

        let result = Config::load();
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("TABLE_MODE_orders"),
            "error should name the offending var, got: {err}"
        );
        assert!(
            err.contains("full-refresh"),
            "error should echo the invalid value, got: {err}"
        );
        assert!(
            err.contains("auto") && err.contains("incremental") && err.contains("full_refresh"),
            "error should list accepted values, got: {err}"
        );
    }

    #[test]
    #[serial]
    fn table_mode_fullrefresh_typo_bails() {
        clear_config_env();
        set_required_vars();
        unsafe {
            env::set_var("TABLE_MODE_orders", "fullrefresh");
        }

        let result = Config::load();
        assert!(result.is_err());
    }

    #[test]
    #[serial]
    fn table_mode_two_stream_value_bails_with_cursor_hint() {
        clear_config_env();
        set_required_vars();
        unsafe {
            env::set_var("TABLE_MODE_orders", "two_stream");
        }

        let result = Config::load();
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("TABLE_INSERT_CURSOR_orders"),
            "error should point at the insert cursor var, got: {err}"
        );
        assert!(
            err.contains("TABLE_UPDATE_CURSOR_orders"),
            "error should point at the update cursor var, got: {err}"
        );
    }

    // O5: TABLE_INSERT_CURSOR_<t> + TABLE_UPDATE_CURSOR_<t> silently override an explicit,
    // non-Auto TABLE_MODE_<t> at resolution time. This must instead be rejected at config
    // load, so both a real run and `--check` see the same actionable conflict error.

    #[test]
    #[serial]
    fn cursor_and_explicit_mode_conflict_bails() {
        clear_config_env();
        set_required_vars();
        unsafe {
            env::set_var("TABLE_MODE_orders", "full_refresh");
            env::set_var("TABLE_INSERT_CURSOR_orders", "id");
            env::set_var("TABLE_UPDATE_CURSOR_orders", "updated_at");
        }

        let result = Config::load();
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("orders"), "error should name the table, got: {err}");
    }

    #[test]
    #[serial]
    fn cursors_without_table_mode_ok() {
        clear_config_env();
        set_required_vars();
        unsafe {
            env::set_var("TABLE_INSERT_CURSOR_orders", "id");
            env::set_var("TABLE_UPDATE_CURSOR_orders", "updated_at");
        }

        let result = Config::load();
        assert!(result.is_ok(), "cursors alone (no TABLE_MODE) should not conflict");
    }

    #[test]
    #[serial]
    fn cursors_with_auto_mode_ok() {
        clear_config_env();
        set_required_vars();
        unsafe {
            env::set_var("TABLE_MODE_orders", "auto");
            env::set_var("TABLE_INSERT_CURSOR_orders", "id");
            env::set_var("TABLE_UPDATE_CURSOR_orders", "updated_at");
        }

        let result = Config::load();
        assert!(result.is_ok(), "cursors + explicit TABLE_MODE=auto should not conflict");
    }

    // PS-H-B: `TABLE_RECONCILE_<table>` one-shot reconcile flag.

    #[test]
    #[serial]
    fn reconcile_true_with_two_stream_cursors_is_reconcile() {
        clear_config_env();
        set_required_vars();
        unsafe {
            env::set_var("TABLE_INSERT_CURSOR_orders", "id");
            env::set_var("TABLE_UPDATE_CURSOR_orders", "updated_at");
            env::set_var("TABLE_RECONCILE_orders", "true");
        }

        let config = Config::load().expect("reconcile with two-stream cursors should load");
        assert!(config.is_reconcile("orders"));
        assert!(!config.is_reconcile("customers"));
    }

    #[test]
    #[serial]
    fn reconcile_false_is_not_reconcile() {
        clear_config_env();
        set_required_vars();
        unsafe {
            env::set_var("TABLE_INSERT_CURSOR_orders", "id");
            env::set_var("TABLE_UPDATE_CURSOR_orders", "updated_at");
            env::set_var("TABLE_RECONCILE_orders", "false");
        }

        let config = Config::load().expect("reconcile=false should load");
        assert!(!config.is_reconcile("orders"));
    }

    #[test]
    #[serial]
    fn reconcile_unset_is_not_reconcile() {
        clear_config_env();
        set_required_vars();

        let config = Config::load().expect("load should succeed");
        assert!(!config.is_reconcile("orders"));
    }

    #[test]
    #[serial]
    fn reconcile_invalid_value_bails() {
        clear_config_env();
        set_required_vars();
        unsafe {
            env::set_var("TABLE_INSERT_CURSOR_orders", "id");
            env::set_var("TABLE_UPDATE_CURSOR_orders", "updated_at");
            env::set_var("TABLE_RECONCILE_orders", "yes");
        }

        let result = Config::load();
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("TABLE_RECONCILE_orders"), "got: {err}");
        assert!(err.contains("yes"), "got: {err}");
    }

    #[test]
    #[serial]
    fn reconcile_without_two_stream_cursors_bails() {
        clear_config_env();
        set_required_vars();
        unsafe {
            env::set_var("TABLE_RECONCILE_orders", "true");
        }

        let result = Config::load();
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("TABLE_INSERT_CURSOR_orders"), "got: {err}");
        assert!(err.contains("TABLE_UPDATE_CURSOR_orders"), "got: {err}");
    }

    #[test]
    #[serial]
    fn reconcile_conflicts_with_table_mode_bails() {
        clear_config_env();
        set_required_vars();
        unsafe {
            env::set_var("TABLE_MODE_orders", "auto");
            env::set_var("TABLE_INSERT_CURSOR_orders", "id");
            env::set_var("TABLE_UPDATE_CURSOR_orders", "updated_at");
            env::set_var("TABLE_RECONCILE_orders", "true");
        }

        let result = Config::load();
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("TABLE_RECONCILE_orders"), "got: {err}");
        assert!(err.contains("TABLE_MODE_orders"), "got: {err}");
    }

    #[test]
    #[serial]
    fn load_local_reconcile_without_cursors_bails() {
        clear_config_env();
        unsafe {
            env::set_var("DATABASE_URL", "mysql://user:pass@host:3306/dbname");
            env::set_var("TABLES", "orders");
            env::set_var("TARGET_MEMORY_MB", "512");
            env::set_var("TABLE_RECONCILE_orders", "true");
        }

        let result = Config::load_local();
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("TABLE_RECONCILE_orders"), "got: {err}");
    }

    #[test]
    #[serial]
    fn load_local_cursor_and_mode_conflict_bails() {
        clear_config_env();
        unsafe {
            env::set_var("DATABASE_URL", "mysql://user:pass@host:3306/dbname");
            env::set_var("TABLES", "orders");
            env::set_var("TARGET_MEMORY_MB", "512");
            env::set_var("TABLE_MODE_orders", "incremental");
            env::set_var("TABLE_INSERT_CURSOR_orders", "id");
            env::set_var("TABLE_UPDATE_CURSOR_orders", "updated_at");
        }

        let result = Config::load_local();
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("orders"), "error should name the table, got: {err}");
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

    // S1: `{config:?}` must not leak the DB password or the raw S3 secret. The
    // hand-written Debug masks database_url + s3_secret_access_key while still
    // printing the non-secret fields.
    #[test]
    #[serial]
    fn debug_masks_database_password_and_s3_secret() {
        clear_config_env();
        set_required_vars();
        unsafe {
            // A recognizable password and secret we can assert are absent.
            env::set_var(
                "DATABASE_URL",
                "mysql://admin:sup3rs3cr3tpw@dbhost:3306/mydb",
            );
            env::set_var("S3_SECRET_ACCESS_KEY", "RAWSECRETVALUE12345");
            env::set_var("S3_REGION", "eu-west-1");
        }
        let config = Config::load().expect("load should succeed");
        let debug = format!("{config:?}");

        assert!(
            !debug.contains("sup3rs3cr3tpw"),
            "debug must not contain DB password, got: {debug}"
        );
        assert!(
            !debug.contains("RAWSECRETVALUE12345"),
            "debug must not contain raw S3 secret, got: {debug}"
        );
        // Masked forms present.
        assert!(
            debug.contains("****:****@dbhost:3306"),
            "debug should contain masked database_url, got: {debug}"
        );
        assert!(
            debug.contains("****2345"),
            "debug should contain masked S3 secret tail, got: {debug}"
        );
        // Non-secret fields still shown.
        assert!(debug.contains("data-lake"), "bucket should be shown: {debug}");
        assert!(debug.contains("eu-west-1"), "region should be shown: {debug}");
        assert!(debug.contains("minioadmin"), "access_key_id should be shown: {debug}");
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
    fn load_local_fails_when_default_batch_size_zero() {
        clear_config_env();
        unsafe {
            env::set_var("DATABASE_URL", "mysql://user:pass@host:3306/dbname");
            env::set_var("TABLES", "orders");
            env::set_var("TARGET_MEMORY_MB", "128");
            env::set_var("DEFAULT_BATCH_SIZE", "0");
        }

        let result = Config::load_local();
        assert!(result.is_err());
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
        // See `load_fails_when_s3_bucket_missing` above: guard against an ancestor `.env`
        // setting S3_REGION (among others) and masking the actual defaulting behavior.
        let _guard = no_dotenv();
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
            table_reconcile: HashSet::new(),
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

    // FA9: UPDATE_STRATEGY / MERGE_SORT_RESERVATION_MB / MERGE_TARGET_PARTITIONS are read
    // deep in the write path with no validation there — a typo'd value silently falls back
    // to the default instead of being reported. These are now validated at config load so a
    // bad value bails loudly at startup.

    #[test]
    #[serial]
    fn update_strategy_unset_is_ok() {
        let _cwd = no_dotenv();
        clear_config_env();
        set_required_vars();

        assert!(Config::load().is_ok());
    }

    #[test]
    #[serial]
    fn update_strategy_merge_is_ok() {
        let _cwd = no_dotenv();
        clear_config_env();
        set_required_vars();
        unsafe {
            env::set_var("UPDATE_STRATEGY", "merge");
        }

        assert!(Config::load().is_ok());
    }

    #[test]
    #[serial]
    fn update_strategy_invalid_value_bails() {
        let _cwd = no_dotenv();
        clear_config_env();
        set_required_vars();
        unsafe {
            env::set_var("UPDATE_STRATEGY", "Merge");
        }

        let result = Config::load();
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("UPDATE_STRATEGY"), "got: {err}");
        assert!(err.contains("Merge"), "should echo the invalid value, got: {err}");
        assert!(err.contains("merge"), "should mention the only accepted value, got: {err}");
    }

    #[test]
    #[serial]
    fn update_strategy_delete_append_typo_bails() {
        let _cwd = no_dotenv();
        clear_config_env();
        set_required_vars();
        unsafe {
            env::set_var("UPDATE_STRATEGY", "delete_append");
        }

        assert!(Config::load().is_err());
    }

    #[test]
    #[serial]
    fn merge_sort_reservation_mb_unset_is_ok() {
        let _cwd = no_dotenv();
        clear_config_env();
        set_required_vars();

        assert!(Config::load().is_ok());
    }

    #[test]
    #[serial]
    fn merge_sort_reservation_mb_valid_is_ok() {
        let _cwd = no_dotenv();
        clear_config_env();
        set_required_vars();
        unsafe {
            env::set_var("MERGE_SORT_RESERVATION_MB", "64");
        }

        assert!(Config::load().is_ok());
    }

    #[test]
    #[serial]
    fn merge_sort_reservation_mb_non_numeric_bails() {
        let _cwd = no_dotenv();
        clear_config_env();
        set_required_vars();
        unsafe {
            env::set_var("MERGE_SORT_RESERVATION_MB", "lots");
        }

        let result = Config::load();
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("MERGE_SORT_RESERVATION_MB"), "got: {err}");
    }

    #[test]
    #[serial]
    fn merge_target_partitions_unset_is_ok() {
        let _cwd = no_dotenv();
        clear_config_env();
        set_required_vars();

        assert!(Config::load().is_ok());
    }

    #[test]
    #[serial]
    fn merge_target_partitions_valid_is_ok() {
        let _cwd = no_dotenv();
        clear_config_env();
        set_required_vars();
        unsafe {
            env::set_var("MERGE_TARGET_PARTITIONS", "4");
        }

        assert!(Config::load().is_ok());
    }

    #[test]
    #[serial]
    fn merge_target_partitions_non_numeric_bails() {
        let _cwd = no_dotenv();
        clear_config_env();
        set_required_vars();
        unsafe {
            env::set_var("MERGE_TARGET_PARTITIONS", "abc");
        }

        let result = Config::load();
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("MERGE_TARGET_PARTITIONS"), "got: {err}");
    }

    #[test]
    #[serial]
    fn merge_target_partitions_zero_bails() {
        let _cwd = no_dotenv();
        clear_config_env();
        set_required_vars();
        unsafe {
            env::set_var("MERGE_TARGET_PARTITIONS", "0");
        }

        let result = Config::load();
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("MERGE_TARGET_PARTITIONS"), "got: {err}");
    }

    #[test]
    #[serial]
    fn merge_target_partitions_negative_bails() {
        let _cwd = no_dotenv();
        clear_config_env();
        set_required_vars();
        unsafe {
            env::set_var("MERGE_TARGET_PARTITIONS", "-1");
        }

        assert!(Config::load().is_err());
    }

    #[test]
    #[serial]
    fn load_local_update_strategy_invalid_value_bails() {
        let _cwd = no_dotenv();
        clear_config_env();
        unsafe {
            env::set_var("DATABASE_URL", "mysql://user:pass@host:3306/dbname");
            env::set_var("TABLES", "orders");
            env::set_var("TARGET_MEMORY_MB", "512");
            env::set_var("UPDATE_STRATEGY", "bogus");
        }

        let result = Config::load_local();
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("UPDATE_STRATEGY"), "got: {err}");
    }

    #[test]
    #[serial]
    fn load_local_merge_sort_reservation_mb_non_numeric_bails() {
        let _cwd = no_dotenv();
        clear_config_env();
        unsafe {
            env::set_var("DATABASE_URL", "mysql://user:pass@host:3306/dbname");
            env::set_var("TABLES", "orders");
            env::set_var("TARGET_MEMORY_MB", "512");
            env::set_var("MERGE_SORT_RESERVATION_MB", "nope");
        }

        assert!(Config::load_local().is_err());
    }

    #[test]
    #[serial]
    fn load_local_merge_target_partitions_zero_bails() {
        let _cwd = no_dotenv();
        clear_config_env();
        unsafe {
            env::set_var("DATABASE_URL", "mysql://user:pass@host:3306/dbname");
            env::set_var("TABLES", "orders");
            env::set_var("TARGET_MEMORY_MB", "512");
            env::set_var("MERGE_TARGET_PARTITIONS", "0");
        }

        assert!(Config::load_local().is_err());
    }
}
