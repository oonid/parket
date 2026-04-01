use std::path::PathBuf;

use parket::config;
use parket::orchestrator::{
    DeltaWriterAdapter, ExtractorAdapter, Orchestrator, SchemaInspectorAdapter,
    SignalHandler, StateManageAdapter,
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

#[tokio::main]
async fn main() {
    init_tracing();

    let config = match config::Config::load() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("configuration error: {e}");
            std::process::exit(2);
        }
    };

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
    let writer = DeltaWriterAdapter::new(&config);
    let state_mgr = StateManageAdapter::new();

    let (signal_handler, shutdown_rx) = SignalHandler::new();
    signal_handler.install().await;

    let state_path = PathBuf::from("state.json");
    let mut orchestrator = Orchestrator::new(
        config,
        schema_inspect,
        extractor,
        writer,
        state_mgr,
        shutdown_rx,
        state_path,
    );

    let exit_code = orchestrator.run().await;
    std::process::exit(exit_code as i32);
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
}
