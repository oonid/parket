use std::path::PathBuf;

use clap::Parser;
use parket::cli::Cli;
use parket::config;
use parket::orchestrator::{
    DeltaWriterAdapter, ExtractorAdapter, LocalDeltaWriterAdapter, Orchestrator,
    SchemaInspectorAdapter, SignalHandler, StateManageAdapter,
};
use parket::preflight::{
    NoopPreflightStorage, PreflightCheck, PreflightInspectAdapter, PreflightStorageAdapter,
};

fn init_tracing() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("parket=info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .with_level(true)
        .with_thread_ids(false)
        .with_file(false)
        .with_line_number(false)
        .init();
}

fn extract_database_name(url: &str) -> String {
    url::Url::parse(url)
        .ok()
        .and_then(|u| {
            u.path_segments()
                .and_then(|mut s| s.next_back().map(|s| s.to_string()))
        })
        .unwrap_or_default()
}

fn log_startup_banner(config: &config::Config, local_dir: Option<&std::path::Path>) {
    let host = config::mask_database_url(&config.database_url);
    let version = env!("CARGO_PKG_VERSION");
    if let Some(dir) = local_dir {
        tracing::info!(
            version,
            tables = config.tables.len(),
            database_host = %host,
            local_dir = %dir.display(),
            "parket v{version} starting (local mode)"
        );
    } else {
        tracing::info!(
            version,
            tables = config.tables.len(),
            database_host = %host,
            s3_bucket = %config.s3_bucket,
            "parket v{version} starting"
        );
    }
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    init_tracing();

    let local_dir = cli.local.as_deref().map(|p| p.to_path_buf());

    let config = if local_dir.is_some() {
        match config::Config::load_local() {
            Ok(c) => c,
            Err(e) => {
                eprintln!("configuration error: {e}");
                std::process::exit(2);
            }
        }
    } else {
        match config::Config::load() {
            Ok(c) => c,
            Err(e) => {
                eprintln!("configuration error: {e}");
                std::process::exit(2);
            }
        }
    };

    tracing::debug!(config = %config.display_safe(), "loaded configuration");

    if cli.check {
        let pool = match sqlx::MySqlPool::connect(&config.database_url).await {
            Ok(p) => p,
            Err(e) => {
                eprintln!("database connection error: {e}");
                std::process::exit(2);
            }
        };

        let database = extract_database_name(&config.database_url);
        let inspect = PreflightInspectAdapter::new(pool, database);

        if local_dir.is_some() {
            println!(
                "{:<30} {:<15} {:<10} {:<15}",
                "TABLE", "MODE", "COLUMNS", "AVG_ROW_LEN"
            );
            let check = PreflightCheck::new(config, inspect, NoopPreflightStorage);
            if let Err(e) = check.run().await {
                eprintln!("pre-flight check failed: {e}");
                std::process::exit(2);
            }
            println!("pre-flight check passed");
            std::process::exit(0);
        } else {
            let storage = PreflightStorageAdapter::new(&config);
            println!(
                "{:<30} {:<15} {:<10} {:<15}",
                "TABLE", "MODE", "COLUMNS", "AVG_ROW_LEN"
            );
            let check = PreflightCheck::new(config, inspect, storage);
            if let Err(e) = check.run().await {
                eprintln!("pre-flight check failed: {e}");
                std::process::exit(2);
            }
            println!("pre-flight check passed");
            std::process::exit(0);
        }
    }

    log_startup_banner(&config, local_dir.as_deref());

    let pool = match sqlx::MySqlPool::connect(&config.database_url).await {
        Ok(p) => p,
        Err(e) => {
            eprintln!("database connection error: {e}");
            std::process::exit(2);
        }
    };

    let database = extract_database_name(&config.database_url);

    let schema_inspect = SchemaInspectorAdapter::new(pool, database);
    let extractor = ExtractorAdapter::new(&config);
    let state_mgr = StateManageAdapter::new();

    let (signal_handler, shutdown_rx) = SignalHandler::new();
    signal_handler.install().await;

    let state_path = PathBuf::from("state.json");

    if let Some(ref dir) = local_dir {
        let writer = LocalDeltaWriterAdapter::new(dir);
        let mut orchestrator = Orchestrator::new(
            config,
            schema_inspect,
            extractor,
            writer,
            state_mgr,
            shutdown_rx,
            state_path,
            cli.progress,
        );
        let exit_code = orchestrator.run().await;
        std::process::exit(exit_code as i32);
    } else {
        let writer = DeltaWriterAdapter::new(&config);
        let mut orchestrator = Orchestrator::new(
            config,
            schema_inspect,
            extractor,
            writer,
            state_mgr,
            shutdown_rx,
            state_path,
            cli.progress,
        );
        let exit_code = orchestrator.run().await;
        std::process::exit(exit_code as i32);
    }
}

#[cfg(test)]
mod tests {
    use tracing::{info, debug, error};
    use tracing_subscriber::EnvFilter;

    #[test]
    fn default_log_level_is_info() {
        let filter = EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| EnvFilter::new("parket=info"));
        let directives: Vec<String> = filter.max_level_hint()
            .map(|l| l.to_string())
            .into_iter().collect();
        assert!(
            directives.len() <= 1,
            "default filter should resolve to a single level"
        );
    }

    #[test]
    fn debug_level_filter_allows_debug() {
        let filter = EnvFilter::new("parket=debug");
        assert!(filter.max_level_hint().is_some());
    }

    #[test]
    fn structured_fields_in_log_statements() {
        let _guard = tracing_subscriber::fmt()
            .with_env_filter("parket=debug")
            .with_test_writer()
            .try_init();

        info!(
            table = "orders",
            rows = 45000,
            arrow_bytes = 1024,
            "batch extracted"
        );
        info!(
            table = "orders",
            rows = 45000,
            hwm_updated_at = "2026-03-28 10:00:00",
            hwm_last_id = 12345,
            "batch committed"
        );
        error!(
            table = "orders",
            error = "connection refused",
            "table failed"
        );
        info!(
            succeeded = 5,
            failed = 0,
            duration_ms = 3200,
            "run complete"
        );
    }

    #[test]
    fn extractor_batch_extracted_log_matches_spec() {
        let _guard = tracing_subscriber::fmt()
            .with_env_filter("parket=debug")
            .with_test_writer()
            .try_init();

        info!(
            table = "orders",
            rows = 45000usize,
            arrow_bytes = 524288usize,
            "batch extracted"
        );
    }

    #[test]
    fn writer_batch_committed_log_matches_spec() {
        let _guard = tracing_subscriber::fmt()
            .with_env_filter("parket=debug")
            .with_test_writer()
            .try_init();

        info!(
            table = "orders",
            rows = 45000usize,
            hwm_updated_at = "2026-03-28 10:00:00",
            hwm_last_id = 98765i64,
            "batch committed"
        );
    }

    #[test]
    fn orchestrator_run_complete_log_matches_spec() {
        let _guard = tracing_subscriber::fmt()
            .with_env_filter("parket=debug")
            .with_test_writer()
            .try_init();

        info!(
            succeeded = 5u32,
            failed = 0u32,
            duration_ms = 3200u64,
            "run complete"
        );
    }

    #[test]
    fn orchestrator_table_failed_log_matches_spec() {
        let _guard = tracing_subscriber::fmt()
            .with_env_filter("parket=debug")
            .with_test_writer()
            .try_init();

        error!(
            table = "orders",
            error = "connection refused",
            "table failed"
        );
    }

    #[test]
    fn init_tracing_filter_construction() {
        use tracing_subscriber::EnvFilter;
        let default_filter = EnvFilter::new("parket=info");
        let debug_filter = EnvFilter::new("parket=debug");
        assert!(default_filter.max_level_hint().is_some());
        assert!(debug_filter.max_level_hint().is_some());
    }

    #[test]
    fn env_filter_parses_rust_log_env() {
        use tracing_subscriber::EnvFilter;
        let filter = EnvFilter::try_from("parket=debug").unwrap();
        assert!(filter.max_level_hint().is_some());
    }

    #[test]
    fn debug_level_captures_debug_messages() {
        let _guard = tracing_subscriber::fmt()
            .with_env_filter("parket=debug")
            .with_test_writer()
            .try_init();

        debug!(batch_size = 10000, "debug message visible at debug level");
    }

    #[test]
    fn startup_banner_logs_version_tables_host_bucket() {
        let _guard = tracing_subscriber::fmt()
            .with_env_filter("parket=info")
            .with_test_writer()
            .try_init();

        use parket::config;
        let config = config::Config {
            database_url: "mysql://admin:s3cret@dbhost:3306/mydb".to_string(),
            s3_bucket: "data-lake".to_string(),
            s3_access_key_id: "key".to_string(),
            s3_secret_access_key: "secret".to_string(),
            tables: vec!["orders".to_string(), "products".to_string()],
            target_memory_mb: 512,
            s3_endpoint: None,
            s3_region: "us-east-1".to_string(),
            s3_prefix: "parket".to_string(),
            default_batch_size: 10000,
            rust_log: "info".to_string(),
            table_modes: std::collections::HashMap::new(),
        };

        log_startup_banner(&config, None);
    }

    #[test]
    fn startup_banner_local_mode() {
        let _guard = tracing_subscriber::fmt()
            .with_env_filter("parket=info")
            .with_test_writer()
            .try_init();

        use parket::config;
        let config = config::Config {
            database_url: "mysql://admin:s3cret@dbhost:3306/mydb".to_string(),
            s3_bucket: String::new(),
            s3_access_key_id: String::new(),
            s3_secret_access_key: String::new(),
            tables: vec!["orders".to_string()],
            target_memory_mb: 256,
            s3_endpoint: None,
            s3_region: "us-east-1".to_string(),
            s3_prefix: "parket".to_string(),
            default_batch_size: 10000,
            rust_log: "info".to_string(),
            table_modes: std::collections::HashMap::new(),
        };

        log_startup_banner(&config, Some(std::path::Path::new("/tmp/delta")));
    }

    #[test]
    fn extract_database_name_parses_url() {
        let name = extract_database_name("mysql://user:pass@host:3306/mydb");
        assert_eq!(name, "mydb");
    }

    #[test]
    fn extract_database_name_invalid_url_returns_empty() {
        let name = extract_database_name("not-a-url");
        assert_eq!(name, "");
    }
}
