## 1. Project Scaffolding

- [ ] 1.1 Initialize Rust project with `cargo init` and create `Cargo.toml` with dependencies: connectorx (features: src_mysql), deltalake (features: s3, datafusion), sqlx (features: mysql, runtime-tokio, macros), tokio (features: full), serde, serde_json, dotenvy, tracing, tracing-subscriber, anyhow
- [ ] 1.2 Create module files: `src/main.rs`, `src/config.rs`, `src/state.rs`, `src/discovery.rs`, `src/query.rs`, `src/extractor.rs`, `src/writer.rs`, `src/orchestrator.rs`
- [ ] 1.3 Create `.env.example` with all configuration variables and comments
- [ ] 1.4 Create `.gitignore` (target/, .env, state.json)

## 2. Configuration Module (config.rs)

- [ ] 2.1 Define `Config` struct with all required and optional fields (DATABASE_URL, S3 credentials, TABLES, TARGET_MEMORY_MB, per-table mode overrides, defaults)
- [ ] 2.2 Implement `Config::load()` that reads env vars via `std::env::var`, uses dotenvy as fallback for `.env` file
- [ ] 2.3 Implement `Config::validate()` that checks required fields are present, DATABASE_URL starts with `mysql://`, TABLES is non-empty, TARGET_MEMORY_MB > 0
- [ ] 2.4 Implement table list parsing (comma-separated, whitespace-trimmed) and per-table mode override lookup (`TABLE_MODE_<NAME>`)

## 3. State Manager Module (state.rs)

- [ ] 3.1 Define `TableState` struct: `last_run_at`, `last_run_status`, `last_run_rows`, `last_run_duration_ms`, `extraction_mode`, `schema_columns_hash`
- [ ] 3.2 Define `AppState` struct wrapping a HashMap of table names to TableState
- [ ] 3.3 Implement `AppState::load()` — read and parse state.json, handle missing file and corrupted JSON gracefully
- [ ] 3.4 Implement `AppState::update_table()` — update state for a single table and write to state.json atomically (write to temp file, rename)

## 4. Schema Discovery Module (discovery.rs)

- [ ] 4.1 Implement `ColumnInfo` struct and `SchemaInspector` with sqlx MySqlPool
- [ ] 4.2 Implement `SchemaInspector::discover_columns()` — query `information_schema.columns` for column names, data_types, column_types; filter unsupported types (GEOMETRY, POINT, etc.) with warning logs
- [ ] 4.3 Implement `SchemaInspector::detect_mode()` — check for `updated_at` (TIMESTAMP/DATETIME) + `id` columns; respect per-table mode override from Config
- [ ] 4.4 Implement `SchemaInspector::get_avg_row_length()` — query `information_schema.tables` for AVG_ROW_LENGTH
- [ ] 4.5 Implement `SchemaInspector::check_updated_at_index()` — query `information_schema.statistics` to check if `updated_at` is indexed; log warning if not
- [ ] 4.6 Implement `compute_schema_hash()` — hash column names and types for change detection

## 5. Query Builder Module (query.rs)

- [ ] 5.1 Implement `QueryBuilder::build_incremental_query()` — cursor-based windowing: `WHERE (updated_at = ? AND id > ?) OR (updated_at > ?) ORDER BY updated_at ASC, id ASC LIMIT {batch_size}`; handle no-HWM case (first run)
- [ ] 5.2 Implement `QueryBuilder::build_full_refresh_query()` — plain `SELECT {columns} FROM {table}`
- [ ] 5.3 Implement column list formatting with backtick-quoting for all table and column names

## 6. Batch Extraction Module (extractor.rs)

- [ ] 6.1 Implement `BatchExtractor` struct holding connector-x connection config and current batch_size
- [ ] 6.2 Implement initial batch size calculation: `batch_size = (TARGET_MEMORY_MB * 1024 * 1024) / AVG_ROW_LENGTH` with DEFAULT_BATCH_SIZE fallback
- [ ] 6.3 Implement `BatchExtractor::extract()` — execute SQL via connector-x streaming API (`new_record_batch_iter`), collect RecordBatches up to batch_size rows
- [ ] 6.4 Implement adaptive batch sizing: after first batch, measure `RecordBatch::get_array_memory_size()`, recalculate bytes_per_row, adjust batch_size if off by >2x
- [ ] 6.5 Implement hard memory ceiling check: if batch exceeds `2 * TARGET_MEMORY_MB`, log warning and halve batch_size

## 7. Delta Writer Module (writer.rs)

- [ ] 7.1 Implement `DeltaWriter` struct with S3 storage configuration built from Config (endpoint, region, credentials)
- [ ] 7.2 Implement `DeltaWriter::ensure_table()` — check if Delta table exists at S3 path; create with Arrow schema if not
- [ ] 7.3 Implement `DeltaWriter::append_batch()` — write RecordBatch with SaveMode::Append, include HWM in commitInfo metadata
- [ ] 7.4 Implement `DeltaWriter::overwrite_table()` — write RecordBatches with SaveMode::Overwrite for FullRefresh mode
- [ ] 7.5 Implement `DeltaWriter::read_hwm()` — read latest commitInfo from Delta log, extract `hwm_updated_at` and `hwm_last_id`
- [ ] 7.6 Implement `extract_hwm_from_batch()` — scan RecordBatch to find max `updated_at` and corresponding `id`

## 8. Orchestrator Module (orchestrator.rs)

- [ ] 8.1 Implement `Orchestrator` struct holding Config, StateManager, SchemaInspector, BatchExtractor, DeltaWriter
- [ ] 8.2 Implement main table loop: iterate tables sequentially, for each table run discover → read HWM → build query → extract batch → write batch → update HWM → repeat until 0 rows
- [ ] 8.3 Implement schema evolution check: compare current MariaDB schema with Delta table Arrow schema; handle additions (warn + skip), drops/type-changes (fail table)
- [ ] 8.4 Implement per-table error handling: catch errors, log, mark table failed in state.json, continue
- [ ] 8.5 Implement exit code logic: 0 (all success), 1 (partial failure), 2 (fatal misconfiguration)

## 9. Signal Handler (main.rs)

- [ ] 9.1 Install `tokio::signal` handler for SIGTERM and SIGINT using `tokio::signal::ctrl_c()` and Unix signal handling
- [ ] 9.2 Use `tokio::sync::watch` channel to communicate shutdown signal to orchestrator
- [ ] 9.3 Implement graceful shutdown: orchestrator checks shutdown signal before starting each new table/batch, finishes in-flight batch before exiting
- [ ] 9.4 Implement double-signal handling: exit immediately with code 130 on second signal

## 10. Logging Setup (main.rs)

- [ ] 10.1 Initialize `tracing_subscriber::fmt` with structured output to stdout, reading log level from `RUST_LOG` env var (default: `parket=info`)
- [ ] 10.2 Add structured span/field instrumentation to key events: batch extracted, batch committed, table failed, run complete

## 11. Integration & End-to-End

- [ ] 11.1 Wire all modules together in `main.rs`: load config → init tracing → init sqlx pool → create orchestrator → run
- [ ] 11.2 Test against local MariaDB + MinIO with sample tables (one incremental, one full_refresh)
- [ ] 11.3 Test graceful shutdown by sending SIGTERM during batch processing
- [ ] 11.4 Test crash recovery: kill process mid-batch, re-run, verify HWM is correct
- [ ] 11.5 Test schema evolution: add column to MariaDB table, re-run, verify warning and exclusion
