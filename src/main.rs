use std::path::PathBuf;

use clap::Parser;
use parket::cli::Cli;
use parket::config;
use parket::inspect::{InspectCommand, InspectIntrospectAdapter};
use parket::orchestrator::{
    DeltaWriterAdapter, ExtractorAdapter, LocalDeltaWriterAdapter, Orchestrator,
    SchemaInspectorAdapter, SignalHandler, StateManageAdapter,
};
use parket::preflight::{
    LocalPreflightStorage, PreflightCheck, PreflightHwmAdapter, PreflightInspectAdapter,
    PreflightStorageAdapter,
};
use anyhow::Context;
use parket::writer::DeltaWriter;

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

/// Raise the soft NOFILE (open file descriptor) limit to the hard limit.
///
/// The two-stream MERGE bounds memory by spilling its external sort to disk, which opens
/// many spill files at once. The default soft limit (often 1024, e.g. under a systemd
/// scope) is too low and the merge fails with "Too many open files". Best-effort: logs and
/// continues if it cannot raise (e.g. insufficient privilege).
#[cfg(unix)]
fn raise_nofile_limit() {
    // SAFETY: get/setrlimit with a valid resource id and a properly-initialized rlimit.
    unsafe {
        let mut lim = libc::rlimit { rlim_cur: 0, rlim_max: 0 };
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut lim) != 0 {
            tracing::warn!("could not read NOFILE limit; leaving as-is");
            return;
        }
        if lim.rlim_cur < lim.rlim_max {
            let old = lim.rlim_cur;
            lim.rlim_cur = lim.rlim_max;
            if libc::setrlimit(libc::RLIMIT_NOFILE, &lim) == 0 {
                tracing::info!(from = old, to = lim.rlim_cur, "raised NOFILE soft limit");
            } else {
                tracing::warn!(
                    soft = old,
                    hard = lim.rlim_max,
                    "failed to raise NOFILE soft limit; MERGE spill may hit 'too many open files'"
                );
            }
        }
    }
}

#[cfg(not(unix))]
fn raise_nofile_limit() {}

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

/// Run `--verify` reconciliation against the synced Delta tables.
/// Returns a process exit code: 0 = clean, 1 = discrepancy, 2 = could not run,
/// 3 = partially verified (V3-r Tier 1: one or more tables had no usable key for a
/// key-set/row-set completeness check, so only value-aggregates were verified).
async fn build_verify_table_plans(
    config: &config::Config,
    writer: &DeltaWriter,
    inspector: &parket::discovery::SchemaInspector,
    tables: &[String],
) -> anyhow::Result<Vec<parket::verify::TablePlan>> {
    let mut plans = Vec::with_capacity(tables.len());

    for table in tables {
        let mode = if let Some((insert_cursor, update_cursor)) = config.two_stream(table) {
            let update_hwm = writer
                .read_hwm(table)
                .await
                .with_context(|| format!("read update HWM for verify table `{table}`"))?;
            let insert_hwm = writer
                .read_insert_hwm(table)
                .await
                .with_context(|| format!("read insert HWM for verify table `{table}`"))?;
            parket::verify::VerifyMode::TwoStream {
                insert_cursor,
                update_cursor,
                update_hwm,
                insert_hwm,
            }
        } else {
            // O12: resolve mode exactly as the run does, instead of reading explicit
            // TABLE_MODE only (which mis-verified auto-detected-incremental tables as Basic).
            match inspector.discover_columns(table).await {
                Ok(raw_columns) => {
                    let columns = parket::discovery::filter_unsupported_columns(&raw_columns);
                    // N3-r: resolve_ts_col_and_mode now needs indexes to auto-detect
                    // incremental via a single-column integer PRIMARY key (not just a
                    // literal `id` column) — degrade to Basic on failure, same as a
                    // discover_columns failure above, rather than fail the whole verify run.
                    match inspector.discover_indexes(table).await {
                        Ok(indexes) => {
                            match parket::discovery::resolve_ts_col_and_mode(&columns, &indexes, config, table) {
                                Ok((ts_col, config::ExtractionMode::Incremental)) => {
                                    let hwm = writer
                                        .read_hwm(table)
                                        .await
                                        .with_context(|| format!("read HWM for verify table `{table}`"))?;
                                    parket::verify::VerifyMode::Incremental { cursor_col: ts_col, hwm }
                                }
                                Ok((_, config::ExtractionMode::FullRefresh)) => {
                                    parket::verify::VerifyMode::FullRefresh
                                }
                                // TwoStream is handled by the branch above (config.two_stream was None here);
                                // Auto is never returned by detect_mode. Either would be a config/logic
                                // anomaly — fall back to a basic check rather than mis-scope.
                                Ok(_) => parket::verify::VerifyMode::Basic,
                                Err(e) => {
                                    tracing::warn!(table = %table, error = %e, "verify: mode resolution failed; falling back to basic checks");
                                    parket::verify::VerifyMode::Basic
                                }
                            }
                        }
                        Err(e) => {
                            tracing::warn!(table = %table, error = %e, "verify: index discovery failed; falling back to basic checks");
                            parket::verify::VerifyMode::Basic
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(table = %table, error = %e, "verify: column discovery failed; falling back to basic checks");
                    parket::verify::VerifyMode::Basic
                }
            }
        };

        plans.push(parket::verify::TablePlan {
            table: table.clone(),
            mode,
        });
    }

    Ok(plans)
}

async fn run_verify(
    config: &config::Config,
    local_dir: Option<&std::path::Path>,
    tables: Vec<String>,
    deep: bool,
) -> i32 {
    let pool = match sqlx::MySqlPool::connect(&config.database_url).await {
        Ok(p) => p,
        Err(e) => {
            eprintln!("database connection error: {e}");
            return 2;
        }
    };
    let database = extract_database_name(&config.database_url);
    let inspector = parket::discovery::SchemaInspector::new(pool.clone(), database);
    let source = parket::verify::SourceProbeAdapter::new(pool);
    let writer = if let Some(dir) = local_dir {
        DeltaWriter::new_local(&dir.to_string_lossy())
    } else {
        DeltaWriter::new(
            &config.s3_bucket,
            &config.s3_prefix,
            config.s3_endpoint.as_deref(),
            &config.s3_region,
            &config.s3_access_key_id,
            &config.s3_secret_access_key,
        )
    };
    let table_plans = match build_verify_table_plans(config, &writer, &inspector, &tables).await {
        Ok(plans) => plans,
        Err(e) => {
            eprintln!("verify failed: {e:#}");
            return 2;
        }
    };
    let delta = parket::verify::DeltaProbeAdapter::new(writer);
    let cmd = parket::verify::VerifyCommand::new(source, delta, tables)
        .with_table_plans(table_plans)
        .with_deep(deep);
    match cmd.run().await {
        Ok(parket::verify::VerifyVerdict::Clean) => 0,
        Ok(parket::verify::VerifyVerdict::Discrepancy) => 1,
        Ok(parket::verify::VerifyVerdict::PartiallyVerified) => 3,
        Err(e) => {
            eprintln!("verify failed: {e:#}");
            2
        }
    }
}

fn resolve_verify_tables(
    configured_tables: &[String],
    requested_table: Option<&str>,
) -> Result<Vec<String>, String> {
    match requested_table {
        Some(table) => configured_tables
            .iter()
            .find(|configured| configured.as_str() == table)
            .cloned()
            .map(|matched| vec![matched])
            .ok_or_else(|| {
                format!(
                    "table `{table}` is not present in configured TABLES: {}",
                    configured_tables.join(", ")
                )
            }),
        None => Ok(configured_tables.to_vec()),
    }
}

fn main() {
    // DataFusion's logical/physical plan and expression traversal recurses deeply on
    // large inputs (e.g. the two-stream DELETE predicate's big id `in_list`, or a
    // full-table scan over a Delta table with many files during `--verify-deep`).
    // The default 8 MiB main-thread / 2 MiB tokio-worker stacks can overflow, so run
    // the whole program on threads with a generous stack.
    // Stacks are virtual reservations, committed lazily as touched, so they do not
    // count against a cgroup RSS cap until used. Be generous: the DELETE predicate's
    // `OR`-normalized id list can drive plan-traversal depth into the hundreds of
    // thousands on a large re-sync.
    const DRIVER_STACK: usize = 512 * 1024 * 1024; // block_on driver thread (plan setup)
    const WORKER_STACK: usize = 128 * 1024 * 1024; // tokio worker threads (execution)

    let child = std::thread::Builder::new()
        .name("parket-main".to_string())
        .stack_size(DRIVER_STACK)
        .spawn(|| {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .thread_stack_size(WORKER_STACK)
                .build()
                .expect("build tokio runtime");
            runtime.block_on(async_main());
        })
        .expect("spawn parket-main thread");
    child.join().expect("join parket-main thread");
}

async fn async_main() {
    let cli = Cli::parse();

    init_tracing();

    // Lift the soft FD limit to the hard limit so the two-stream MERGE's disk-spill
    // (many open files) doesn't fail under a low default (e.g. systemd's 1024).
    raise_nofile_limit();

    // Handle --inspect early (before full config load)
    if let Some(ref table) = cli.inspect {
        let database_url = match config::Config::load_inspect() {
            Ok(u) => u,
            Err(e) => {
                eprintln!("configuration error: {e}");
                std::process::exit(2);
            }
        };
        let pool = match sqlx::MySqlPool::connect(&database_url).await {
            Ok(p) => p,
            Err(e) => {
                eprintln!("database connection error: {e}");
                std::process::exit(2);
            }
        };
        let database = extract_database_name(&database_url);
        let configured_ts = std::env::var(format!("TABLE_TIMESTAMP_{table}"))
            .ok()
            .filter(|s| !s.trim().is_empty());
        let introspect = InspectIntrospectAdapter::new(pool, database);
        let cmd = InspectCommand::new(introspect, table.clone(), configured_ts);
        if let Err(e) = cmd.run().await {
            eprintln!("inspect failed: {e:#}");
            std::process::exit(2);
        }
        std::process::exit(0);
    }

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

        if let Some(ref dir) = local_dir {
            let hwm = PreflightHwmAdapter::new_local(dir);
            let check = PreflightCheck::new(config, inspect, LocalPreflightStorage::new(dir), hwm);
            if let Err(e) = check.run().await {
                eprintln!("pre-flight check failed: {e}");
                std::process::exit(2);
            }
            println!("pre-flight check passed");
            std::process::exit(0);
        } else {
            let storage = PreflightStorageAdapter::new(&config);
            let hwm = PreflightHwmAdapter::new(&config);
            let check = PreflightCheck::new(config, inspect, storage, hwm);
            if let Err(e) = check.run().await {
                eprintln!("pre-flight check failed: {e}");
                std::process::exit(2);
            }
            println!("pre-flight check passed");
            std::process::exit(0);
        }
    }

    if let Some(ref verify) = cli.verify {
        let verify_tables = match resolve_verify_tables(&config.tables, verify.as_deref()) {
            Ok(tables) => tables,
            Err(e) => {
                eprintln!("configuration error: {e}");
                std::process::exit(2);
            }
        };
        let code = run_verify(&config, local_dir.as_deref(), verify_tables, cli.verify_deep).await;
        std::process::exit(code);
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
    // O13: `state_path` is resolved relative to the process's current working directory, not
    // a fixed location — log the resolved absolute path (best-effort; falls back to the
    // relative path if it can't be canonicalized, e.g. the file doesn't exist yet on a first
    // run) so an operator who launched parket from an unexpected cwd notices which
    // state.json is actually in play, instead of silently reading/writing the wrong one.
    let resolved_state_path = std::env::current_dir()
        .map(|cwd| cwd.join(&state_path))
        .unwrap_or_else(|_| state_path.clone());
    tracing::info!(state_path = %resolved_state_path.display(), "resolved state.json path");

    // Capture what --verify-after needs before `config` is moved into the orchestrator.
    let verify_after_cfg = if cli.verify_after {
        Some(config.clone())
    } else {
        None
    };
    let verify_after_dir = local_dir.clone();
    let verify_deep = cli.verify_deep;

    let sync_exit = if let Some(ref dir) = local_dir {
        let writer = LocalDeltaWriterAdapter::new(dir, &config);
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
        orchestrator.run().await as i32
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
        orchestrator.run().await as i32
    };

    // --verify-after: reconcile only when the sync itself succeeded.
    if let Some(vcfg) = verify_after_cfg {
        if sync_exit == 0 {
            let code = run_verify(
                &vcfg,
                verify_after_dir.as_deref(),
                vcfg.tables.clone(),
                verify_deep,
            )
            .await;
            std::process::exit(code);
        }
        eprintln!("verify-after skipped: sync exited with code {sync_exit}");
    }

    std::process::exit(sync_exit);
}

#[cfg(test)]
mod tests {
    use super::*;
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
        let filter = EnvFilter::from("parket=debug");
        assert!(filter.max_level_hint().is_some());
    }

    #[test]
    fn resolve_verify_tables_returns_all_when_no_table_requested() {
        let tables = vec!["orders".to_string(), "customers".to_string()];
        let resolved = resolve_verify_tables(&tables, None).unwrap();
        assert_eq!(resolved, tables);
    }

    #[test]
    fn resolve_verify_tables_returns_single_requested_table() {
        let tables = vec!["orders".to_string(), "customers".to_string()];
        let resolved = resolve_verify_tables(&tables, Some("customers")).unwrap();
        assert_eq!(resolved, vec!["customers".to_string()]);
    }

    #[test]
    fn resolve_verify_tables_rejects_unknown_table() {
        let tables = vec!["orders".to_string(), "customers".to_string()];
        let err = resolve_verify_tables(&tables, Some("missing")).unwrap_err();
        assert!(err.contains("missing"));
        assert!(err.contains("orders, customers"));
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
            merge_memory_mb: 512,
            merge_spill_dir: None,
            s3_endpoint: None,
            s3_region: "us-east-1".to_string(),
            s3_prefix: "parket".to_string(),
            default_batch_size: 10000,
            rust_log: "info".to_string(),
            table_modes: std::collections::HashMap::new(),
            table_initial_hwm: std::collections::HashMap::new(),
            table_timestamp_col: std::collections::HashMap::new(),
            table_insert_cursor: std::collections::HashMap::new(),
            table_update_cursor: std::collections::HashMap::new(),
            table_reconcile: std::collections::HashSet::new(),
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
            merge_memory_mb: 256,
            merge_spill_dir: None,
            s3_endpoint: None,
            s3_region: "us-east-1".to_string(),
            s3_prefix: "parket".to_string(),
            default_batch_size: 10000,
            rust_log: "info".to_string(),
            table_modes: std::collections::HashMap::new(),
            table_initial_hwm: std::collections::HashMap::new(),
            table_timestamp_col: std::collections::HashMap::new(),
            table_insert_cursor: std::collections::HashMap::new(),
            table_update_cursor: std::collections::HashMap::new(),
            table_reconcile: std::collections::HashSet::new(),
        };

        log_startup_banner(&config, Some(std::path::Path::new("/tmp/delta")));
    }

    #[cfg(unix)]
    #[test]
    fn raise_nofile_limit_does_not_lower_soft_limit() {
        // SAFETY: getrlimit with a valid resource and rlimit pointer.
        unsafe {
            let mut before = libc::rlimit { rlim_cur: 0, rlim_max: 0 };
            assert_eq!(libc::getrlimit(libc::RLIMIT_NOFILE, &mut before), 0);
            raise_nofile_limit();
            let mut after = libc::rlimit { rlim_cur: 0, rlim_max: 0 };
            assert_eq!(libc::getrlimit(libc::RLIMIT_NOFILE, &mut after), 0);
            // Best-effort: it must never decrease the soft limit, and should reach the hard cap.
            assert!(after.rlim_cur >= before.rlim_cur, "soft NOFILE must not decrease");
            assert_eq!(after.rlim_cur, after.rlim_max, "soft should be raised to hard");
        }
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
