## 1. Project Scaffolding & CI Setup

- [x] 1.1 Initialize Rust project with `Cargo.toml` with dependencies: connectorx (features: src_mysql), deltalake (features: s3, datafusion), sqlx (features: mysql, runtime-tokio, macros), tokio (features: full), serde, serde_json, dotenvy, tracing, tracing-subscriber, anyhow; dev-dependencies: mockall, testcontainers, testcontainers-modules
- [x] 1.2 Create module files: `src/main.rs`, `src/config.rs`, `src/state.rs`, `src/discovery.rs`, `src/query.rs`, `src/extractor.rs`, `src/writer.rs`, `src/orchestrator.rs`
- [x] 1.3 Create `.env.example` with all configuration variables and comments
- [x] 1.4 Update `.gitignore` (target/, .env, state.json, .idea/, .opencode/, coverage/)
- [x] 1.5 Create `rust-toolchain.toml` with pinned Rust version
- [x] 1.6 Verify: `cargo build && cargo clippy -- -D warnings`

## 2. Configuration Module — TDD (config.rs)

- [ ] 2.1 Write unit tests for `Config::load()`: valid env vars, missing required vars, empty TABLES, DATABASE_URL scheme validation, optional defaults, per-table mode overrides
- [ ] 2.2 Implement `Config` struct with all required and optional fields
- [ ] 2.3 Implement `Config::load()` — read env vars via `std::env::var`, use dotenvy as `.env` fallback
- [ ] 2.4 Implement `Config::validate()` — check required fields, DATABASE_URL starts with `mysql://`, TABLES non-empty, TARGET_MEMORY_MB > 0
- [ ] 2.5 Implement table list parsing (comma-separated, trimmed) and `TABLE_MODE_<NAME>` override lookup
- [ ] 2.6 Document: create `docs/config.md` with env var reference, validation rules, defaults table, and per-table override examples
- [ ] 2.7 Verify: `cargo build && cargo clippy -- -D warnings && cargo test`

## 3. State Manager Module — TDD (state.rs)

- [ ] 3.1 Write unit tests for state load/save: missing file, valid JSON, corrupted JSON, update table success, update table failure, atomic write
- [ ] 3.2 Define `TableState` struct: `last_run_at`, `last_run_status`, `last_run_rows`, `last_run_duration_ms`, `extraction_mode`, `schema_columns_hash`
- [ ] 3.3 Define `AppState` struct wrapping HashMap of table names to TableState
- [ ] 3.4 Implement `AppState::load()` — parse state.json, handle missing file and corrupted JSON gracefully
- [ ] 3.5 Implement `AppState::update_table()` — update state for a table and write to state.json atomically (temp file + rename)
- [ ] 3.6 Document: create `docs/state-management.md` with state.json schema, atomic write strategy, HWM separation rationale
- [ ] 3.7 Verify: `cargo build && cargo clippy -- -D warnings && cargo test`

## 4. Query Builder Module — TDD (query.rs)

- [ ] 4.1 Write unit tests for `QueryBuilder`: incremental with HWM, incremental without HWM (first run), full_refresh query, backtick-quoting, reserved word table names, column list formatting
- [ ] 4.2 Implement `QueryBuilder::build_incremental_query()` — cursor-based windowing with `(updated_at = ? AND id > ?) OR (updated_at > ?)` ORDER BY + LIMIT; handle no-HWM case
- [ ] 4.3 Implement `QueryBuilder::build_full_refresh_query()` — plain `SELECT {columns} FROM {table}`
- [ ] 4.4 Implement backtick-quoting for all table and column names
- [ ] 4.5 Document: create `docs/query-patterns.md` with SQL templates for each mode, cursor pagination logic, and generated SQL examples
- [ ] 4.6 Verify: `cargo build && cargo clippy -- -D warnings && cargo test`

## 5. Schema Discovery Module — TDD (discovery.rs)

- [ ] 5.1 Write unit tests with mocked sqlx pool: column discovery, unsupported type filtering (GEOMETRY etc.), mode detection (both columns present, missing updated_at, missing id), mode override precedence, AVG_ROW_LENGTH retrieval, AVG_ROW_LENGTH zero/null fallback, index check, schema hash computation
- [ ] 5.2 Implement `ColumnInfo` struct and `SchemaInspector` with sqlx MySqlPool
- [ ] 5.3 Implement `SchemaInspector::discover_columns()` — query `information_schema.columns`, filter unsupported types with warning logs
- [ ] 5.4 Implement `SchemaInspector::detect_mode()` — check for `updated_at` (TIMESTAMP/DATETIME) + `id` columns, respect per-table mode override
- [ ] 5.5 Implement `SchemaInspector::get_avg_row_length()` — query `information_schema.tables`
- [ ] 5.6 Implement `SchemaInspector::check_updated_at_index()` — query `information_schema.statistics`, warn if no index
- [ ] 5.7 Implement `compute_schema_hash()` — hash column names and types for change detection
- [ ] 5.8 Document: create `docs/schema-discovery.md` with mode detection heuristics, unsupported types list, schema hash algorithm, and information_schema queries used
- [ ] 5.9 Verify: `cargo build && cargo clippy -- -D warnings && cargo test`

## 6. Batch Extraction Module — TDD (extractor.rs)

- [ ] 6.1 Write unit tests with mocked connector-x: batch size calculation (AVG_ROW_LENGTH > 0, = 0, = NULL), adaptive sizing after first batch (>2x diff, <2x diff), hard ceiling enforcement, zero-row result, multi-row result
- [ ] 6.2 Implement `BatchExtractor` struct holding connector-x connection config and current batch_size
- [ ] 6.3 Implement initial batch size calculation: `batch_size = (TARGET_MEMORY_MB * 1024 * 1024) / AVG_ROW_LENGTH` with DEFAULT_BATCH_SIZE fallback
- [ ] 6.4 Implement `BatchExtractor::extract()` — execute SQL via connector-x streaming API, collect RecordBatches up to batch_size rows
- [ ] 6.5 Implement adaptive batch sizing: measure `RecordBatch::get_array_memory_size()`, recalculate bytes_per_row, adjust if >2x off
- [ ] 6.6 Implement hard memory ceiling check: if batch exceeds `2 * TARGET_MEMORY_MB`, log warning and halve batch_size
- [ ] 6.7 Document: create `docs/batch-extraction.md` with memory model, adaptive sizing algorithm, connector-x streaming API usage, and batch size calculation examples
- [ ] 6.8 Verify: `cargo build && cargo clippy -- -D warnings && cargo test`

## 7. Delta Writer Module — TDD (writer.rs)

- [ ] 7.1 Write unit tests with mocked delta-rs: ensure_table (exists/not exists), append_batch with HWM metadata, overwrite_table, read_hwm (with/without metadata), extract_hwm_from_batch, S3 connection error handling
- [ ] 7.2 Implement `DeltaWriter` struct with S3 storage configuration from Config
- [ ] 7.3 Implement `DeltaWriter::ensure_table()` — check Delta table at S3 path, create with Arrow schema if missing
- [ ] 7.4 Implement `DeltaWriter::append_batch()` — write RecordBatch with SaveMode::Append, HWM in commitInfo metadata
- [ ] 7.5 Implement `DeltaWriter::overwrite_table()` — write with SaveMode::Overwrite for FullRefresh
- [ ] 7.6 Implement `DeltaWriter::read_hwm()` — read latest commitInfo from Delta log, extract `hwm_updated_at` and `hwm_last_id`
- [ ] 7.7 Implement `extract_hwm_from_batch()` — scan RecordBatch for max `updated_at` and corresponding `id`
- [ ] 7.8 Document: create `docs/delta-writer.md` with S3 path layout, HWM in commitInfo, Append vs Overwrite semantics, and delta-rs API surface used
- [ ] 7.9 Verify: `cargo build && cargo clippy -- -D warnings && cargo test`

## 8. Orchestrator Module — TDD (orchestrator.rs)

- [ ] 8.1 Write unit tests with mocked dependencies: all tables succeed (exit 0), partial failure (exit 1), fatal misconfig (exit 2), schema evolution (addition: warn+skip, drop: fail, type change: fail), batch loop until exhausted, graceful shutdown signal
- [ ] 8.2 Implement `Orchestrator` struct holding Config, StateManager, SchemaInspector, BatchExtractor, DeltaWriter, shutdown receiver
- [ ] 8.3 Implement main table loop: discover → read HWM → build query → extract batch → write batch → update HWM → repeat until 0 rows
- [ ] 8.4 Implement schema evolution check: compare MariaDB schema with Delta Arrow schema; warn+skip additions, fail drops/type-changes
- [ ] 8.5 Implement per-table error handling: catch errors, log, mark table failed in state.json, continue
- [ ] 8.6 Implement exit code logic: 0 (all success), 1 (partial failure), 2 (fatal misconfiguration)
- [ ] 8.7 Document: create `docs/orchestrator.md` with execution flow, error handling strategy, exit codes, and schema evolution rules
- [ ] 8.8 Verify: `cargo build && cargo clippy -- -D warnings && cargo test`

## 9. Signal Handler (main.rs)

- [ ] 9.1 Write unit tests for shutdown signal propagation: signal between tables, signal during batch, second signal (exit 130)
- [ ] 9.2 Install `tokio::signal` handler for SIGTERM and SIGINT
- [ ] 9.3 Use `tokio::sync::watch` channel to communicate shutdown signal to orchestrator
- [ ] 9.4 Implement graceful shutdown in orchestrator: check signal before each new table/batch, finish in-flight batch
- [ ] 9.5 Implement double-signal handling: exit immediately with code 130 on second signal
- [ ] 9.6 Document: create `docs/signal-handling.md` with graceful shutdown sequence, double-signal behavior, and tokio::signal usage
- [ ] 9.7 Verify: `cargo build && cargo clippy -- -D warnings && cargo test`

## 10. Logging Setup (main.rs)

- [ ] 10.1 Write unit tests verifying log output: default level, debug level, structured fields present
- [ ] 10.2 Initialize `tracing_subscriber::fmt` with structured output to stdout, RUST_LOG env var (default: `parket=info`)
- [ ] 10.3 Add structured span/field instrumentation: batch extracted, batch committed, table failed, run complete
- [ ] 10.4 Document: create `docs/logging.md` with structured log format, field reference, log level guide, and example log output
- [ ] 10.5 Verify: `cargo build && cargo clippy -- -D warnings && cargo test`

## 11. Integration & End-to-End

- [ ] 11.1 Wire all modules together in `main.rs`: load config → init tracing → init sqlx pool → create orchestrator → run
- [ ] 11.2 Write integration tests using testcontainers (MariaDB + MinIO): incremental table extraction, full_refresh table extraction, graceful shutdown via SIGTERM, crash recovery (HWM correct after restart), schema evolution (add column → warn+skip)
- [ ] 11.3 Verify: `cargo build && cargo clippy -- -D warnings && cargo test`

## 12. Coverage Gate

- [ ] 12.1 Install `cargo-llvm-cov` and verify setup: `cargo llvm-cov --summary-only`
- [ ] 12.2 Run `cargo llvm-cov --fail-under-lines 90` — if below 90%, add missing tests for uncovered paths
- [ ] 12.3 Verify: `cargo llvm-cov --fail-under-lines 90` passes

## 13. CI/CD & Release Pipeline

- [ ] 13.1 Create `.github/workflows/ci.yml`: checkout → install toolchain (from rust-toolchain.toml) + llvm-tools-preview → cargo test → cargo clippy -- -D warnings → cargo llvm-cov --fail-under-lines 90 → upload coverage report artifact
- [ ] 13.2 Create `.github/workflows/release.yml` (trigger on `v*` tag): build 4 cross-compiled targets (x86_64-musl, aarch64-musl, x86_64-gnu, aarch64-gnu) using `cross` → generate SHA256 checksums → create GitHub Release with binaries + checksums
- [ ] 13.3 Add GHCR publish job to `release.yml`: multi-arch Docker build (linux/amd64, linux/arm64) → push to `ghcr.io/<OWNER>/parket:latest` and `ghcr.io/<OWNER>/parket:<version>`
- [ ] 13.4 Create `Dockerfile`: multi-stage build — `rust:alpine` builder stage (musl target) → `alpine:latest` runtime stage with binary at `/usr/local/bin/parket`
- [ ] 13.5 Document: create `docs/ci-cd.md` with workflow descriptions, release process, Docker usage, and multi-arch build details
- [ ] 13.6 Verify: `docker build .` succeeds locally
- [ ] 13.7 Verify: `cargo build && cargo clippy -- -D warnings && cargo test && cargo llvm-cov --fail-under-lines 90`

## 14. Project Documentation Consolidation

- [ ] 14.1 Create `README.md` at project root: project overview, quickstart guide, usage examples (one-shot binary, Docker), build instructions, configuration summary, and links to all `docs/` files
- [ ] 14.2 Create `docs/architecture.md`: high-level architecture, data flow diagram (text-based), module responsibilities, key design decisions summary (cross-referencing design.md), and dependency graph
- [ ] 14.3 Create `docs/configuration.md`: complete env var reference (all required + optional), defaults table, per-table override examples, S3/MinIO setup guide, and troubleshooting common config errors
- [ ] 14.4 Create `docs/contributing.md`: development setup (rustup, llvm-cov, Docker), TDD workflow (red-green-refactor cycle), PR checklist (tests pass, clippy clean, coverage >90%, docs updated), CI expectations, and commit message conventions
- [ ] 14.5 Review all `docs/*.md` files for consistency, cross-link correctness, and completeness against specs
- [ ] 14.6 Verify: all documentation files exist, README.md links are valid, no placeholder content remains
