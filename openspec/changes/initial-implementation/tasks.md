## 1. Project Scaffolding & CI Setup

- [x] 1.1 Initialize Rust project with `Cargo.toml` with dependencies: connectorx (features: src_mysql), deltalake (features: s3, datafusion), sqlx (features: mysql, runtime-tokio, macros), tokio (features: full), serde, serde_json, dotenvy, tracing, tracing-subscriber, anyhow; dev-dependencies: mockall, testcontainers, testcontainers-modules
- [x] 1.2 Create module files: `src/main.rs`, `src/config.rs`, `src/state.rs`, `src/discovery.rs`, `src/query.rs`, `src/extractor.rs`, `src/writer.rs`, `src/orchestrator.rs`
- [x] 1.3 Create `.env.example` with all configuration variables and comments
- [x] 1.4 Update `.gitignore` (target/, .env, state.json, .idea/, .opencode/, coverage/)
- [x] 1.5 Create `rust-toolchain.toml` with pinned Rust version
- [x] 1.6 Verify: `cargo build && cargo clippy -- -D warnings`

## 2. Configuration Module — TDD (config.rs)

- [x] 2.1 Write unit tests for `Config::load()`: valid env vars, missing required vars, empty TABLES, DATABASE_URL scheme validation, optional defaults, per-table mode overrides
- [x] 2.2 Implement `Config` struct with all required and optional fields
- [x] 2.3 Implement `Config::load()` — read env vars via `std::env::var`, use dotenvy as `.env` fallback
- [x] 2.4 Implement `Config::validate()` — check required fields, DATABASE_URL starts with `mysql://`, TABLES non-empty, TARGET_MEMORY_MB > 0
- [x] 2.5 Implement table list parsing (comma-separated, trimmed) and `TABLE_MODE_<NAME>` override lookup
- [x] 2.6 Document: create `docs/config.md` with env var reference, validation rules, defaults table, and per-table override examples
- [x] 2.7 Verify: `cargo build && cargo clippy -- -D warnings && cargo test`

## 3. State Manager Module — TDD (state.rs)

- [x] 3.1 Write unit tests for state load/save: missing file, valid JSON, corrupted JSON, update table success, update table failure, atomic write
- [x] 3.2 Define `TableState` struct: `last_run_at`, `last_run_status`, `last_run_rows`, `last_run_duration_ms`, `extraction_mode`, `schema_columns_hash`
- [x] 3.3 Define `AppState` struct wrapping HashMap of table names to TableState
- [x] 3.4 Implement `AppState::load()` — parse state.json, handle missing file and corrupted JSON gracefully
- [x] 3.5 Implement `AppState::update_table()` — update state for a table and write to state.json atomically (temp file + rename)
- [x] 3.6 Document: create `docs/state-management.md` with state.json schema, atomic write strategy, HWM separation rationale
- [x] 3.7 Verify: `cargo build && cargo clippy -- -D warnings && cargo test`

## 4. Query Builder Module — TDD (query.rs)

- [x] 4.1 Write unit tests for `QueryBuilder`: incremental with HWM, incremental without HWM (first run), full_refresh query, backtick-quoting, reserved word table names, column list formatting
- [x] 4.2 Implement `QueryBuilder::build_incremental_query()` — cursor-based windowing with `(updated_at = ? AND id > ?) OR (updated_at > ?)` ORDER BY + LIMIT; handle no-HWM case
- [x] 4.3 Implement `QueryBuilder::build_full_refresh_query()` — plain `SELECT {columns} FROM {table}`
- [x] 4.4 Implement backtick-quoting for all table and column names
- [x] 4.5 Document: create `docs/query-patterns.md` with SQL templates for each mode, cursor pagination logic, and generated SQL examples
- [x] 4.6 Verify: `cargo build && cargo clippy -- -D warnings && cargo test`

## 5. Schema Discovery Module — TDD (discovery.rs)

- [x] 5.1 Write unit tests with mocked sqlx pool: column discovery, unsupported type filtering (GEOMETRY etc.), mode detection (both columns present, missing updated_at, missing id), mode override precedence, AVG_ROW_LENGTH retrieval, AVG_ROW_LENGTH zero/null fallback, index check, schema hash computation
- [x] 5.2 Implement `ColumnInfo` struct and `SchemaInspector` with sqlx MySqlPool
- [x] 5.3 Implement `SchemaInspector::discover_columns()` — query `information_schema.columns`, filter unsupported types with warning logs
- [x] 5.4 Implement `SchemaInspector::detect_mode()` — check for `updated_at` (TIMESTAMP/DATETIME) + `id` columns, respect per-table mode override
- [x] 5.5 Implement `SchemaInspector::get_avg_row_length()` — query `information_schema.tables`
- [x] 5.6 Implement `SchemaInspector::check_updated_at_index()` — query `information_schema.statistics`, warn if no index
- [x] 5.7 Implement `compute_schema_hash()` — hash column names and types for change detection
- [x] 5.8 Document: create `docs/schema-discovery.md` with mode detection heuristics, unsupported types list, schema hash algorithm, and information_schema queries used
- [x] 5.9 Verify: `cargo build && cargo clippy -- -D warnings && cargo test`

## 6. Batch Extraction Module — TDD (extractor.rs)

- [x] 6.1 Write unit tests with mocked connector-x: batch size calculation (AVG_ROW_LENGTH > 0, = 0, = NULL), adaptive sizing after first batch (>2x diff, <2x diff), hard ceiling enforcement, zero-row result, multi-row result
- [x] 6.2 Implement `BatchExtractor` struct holding connector-x connection config and current batch_size
- [x] 6.3 Implement initial batch size calculation: `batch_size = (TARGET_MEMORY_MB * 1024 * 1024) / AVG_ROW_LENGTH` with DEFAULT_BATCH_SIZE fallback
- [x] 6.4 Implement `BatchExtractor::extract()` — execute SQL via connector-x streaming API, collect RecordBatches up to batch_size rows
- [x] 6.5 Implement adaptive batch sizing: measure `RecordBatch::get_array_memory_size()`, recalculate bytes_per_row, adjust if >2x off
- [x] 6.6 Implement hard memory ceiling check: if batch exceeds `2 * TARGET_MEMORY_MB`, log warning and halve batch_size
- [x] 6.7 Document: create `docs/batch-extraction.md` with memory model, adaptive sizing algorithm, connector-x streaming API usage, and batch size calculation examples
- [x] 6.8 Verify: `cargo build && cargo clippy -- -D warnings && cargo test`

## 7. Delta Writer Module — TDD (writer.rs)

- [x] 7.1 Write unit tests with mocked delta-rs: ensure_table (exists/not exists), append_batch with HWM metadata, overwrite_table, read_hwm (with/without metadata), extract_hwm_from_batch, S3 connection error handling
- [x] 7.2 Implement `DeltaWriter` struct with S3 storage configuration from Config
- [x] 7.3 Implement `DeltaWriter::ensure_table()` — check Delta table at S3 path, create with Arrow schema if missing
- [x] 7.4 Implement `DeltaWriter::append_batch()` — write RecordBatch with SaveMode::Append, HWM in commitInfo metadata
- [x] 7.5 Implement `DeltaWriter::overwrite_table()` — write with SaveMode::Overwrite for FullRefresh
- [x] 7.6 Implement `DeltaWriter::read_hwm()` — read latest commitInfo from Delta log, extract `hwm_updated_at` and `hwm_last_id`
- [x] 7.7 Implement `extract_hwm_from_batch()` — scan RecordBatch for max `updated_at` and corresponding `id`
- [x] 7.8 Document: create `docs/delta-writer.md` with S3 path layout, HWM in commitInfo, Append vs Overwrite semantics, and delta-rs API surface used
- [x] 7.9 Verify: `cargo build && cargo clippy -- -D warnings && cargo test`

## 8. Arrow v54→v57 FFI Conversion — TDD (extractor.rs)

connector-x is pinned to arrow v54 with no upgrade path. Add **additional standalone functions** — do NOT change any existing `BatchExtractor` methods or signatures. When connector-x eventually supports v57 (or is replaced), these conversion functions are simply deleted. See `docs/arrow_v54_to_v57.md` section B for design rationale and FFI reference.

- [x] 8.1 TDD: tests for `convert_datatype()` — all supported types (Null, Boolean, Int8–UInt64, Float16–Float64, Utf8, LargeUtf8, Binary, LargeBinary, Date32, Date64, Timestamp variants); unsupported type returns error
- [x] 8.2 TDD: tests for `convert_schema_v54_to_v57()` — field names, types, nullability preserved; unsupported field type returns error
- [x] 8.3 TDD: tests for `convert_v54_to_v57()` — single-column batches (Int64, Utf8, Float64 with nulls, Timestamp, Boolean, Date32), multi-column batch, empty batch (0 rows)
- [x] 8.4 TDD: tests for `convert_batches()` — empty vec, single batch, multiple batches
- [x] 8.5 Implement `convert_datatype()` — map v54 `DataType` → v57 `DataType` by value (ref: `examples/ffi_vs_ipc.rs`)
- [x] 8.6 Implement `convert_schema_v54_to_v57()` — convert full Arrow schema (v54→v57)
- [x] 8.7 Implement `convert_v54_to_v57()` — FFI zero-copy single RecordBatch conversion (export via `arrow::ffi::to_ffi`, copy C ABI structs, `mem::forget` v54 side, import via `deltalake::arrow::ffi::from_ffi`)
- [x] 8.8 Implement `convert_batches()` — apply `convert_v54_to_v57()` over `Vec` of v54 batches
- [x] 8.9 Update `docs/arrow_v54_to_v57.md` — add section documenting the production conversion API surface in `extractor.rs`, removal instructions when connector-x upgrades
- [x] 8.10 Verify: `cargo build && cargo clippy -- -D warnings && cargo test`

## 9. Orchestrator Module — TDD (orchestrator.rs)

- [x] 9.1 Write unit tests with mocked dependencies: all tables succeed (exit 0), partial failure (exit 1), fatal misconfig (exit 2), schema evolution (addition: warn+skip, drop: fail, type change: fail), batch loop until exhausted, graceful shutdown signal
- [x] 9.2 Implement `Orchestrator` struct holding Config, StateManager, SchemaInspector, BatchExtractor, DeltaWriter, shutdown receiver
- [x] 9.3 Implement main table loop: discover → read HWM → build query → extract batch → write batch → update HWM → repeat until 0 rows
- [x] 9.4 Implement schema evolution check: compare MariaDB schema with Delta Arrow schema; warn+skip additions, fail drops/type-changes
- [x] 9.5 Implement per-table error handling: catch errors, log, mark table failed in state.json, continue
- [x] 9.6 Implement exit code logic: 0 (all success), 1 (partial failure), 2 (fatal misconfiguration)
- [x] 9.7 Document: create `docs/orchestrator.md` with execution flow, error handling strategy, exit codes, and schema evolution rules
- [x] 9.8 Verify: `cargo build && cargo clippy -- -D warnings && cargo test`

## 10. Signal Handler (main.rs)

- [x] 10.1 Write unit tests for shutdown signal propagation: signal between tables, signal during batch, second signal (exit 130)
- [x] 10.2 Install `tokio::signal` handler for SIGTERM and SIGINT
- [x] 10.3 Use `tokio::sync::watch` channel to communicate shutdown signal to orchestrator
- [x] 10.4 Implement graceful shutdown in orchestrator: check signal before each new table/batch, finish in-flight batch
- [x] 10.5 Implement double-signal handling: exit immediately with code 130 on second signal
- [x] 10.6 Document: create `docs/signal-handling.md` with graceful shutdown sequence, double-signal behavior, and tokio::signal usage
- [x] 10.7 Verify: `cargo build && cargo clippy -- -D warnings && cargo test`

## 11. Logging Setup (main.rs)

- [ ] 11.1 Write unit tests verifying log output: default level, debug level, structured fields present
- [ ] 11.2 Initialize `tracing_subscriber::fmt` with structured output to stdout, RUST_LOG env var (default: `parket=info`)
- [ ] 11.3 Add structured span/field instrumentation: batch extracted, batch committed, table failed, run complete
- [ ] 11.4 Document: create `docs/logging.md` with structured log format, field reference, log level guide, and example log output
- [ ] 11.5 Verify: `cargo build && cargo clippy -- -D warnings && cargo test`

## 12. Integration & End-to-End

- [ ] 12.1 Wire all modules together in `main.rs`: load config → init tracing → init sqlx pool → create orchestrator → run
- [ ] 12.2 Write integration tests using testcontainers (MariaDB + MinIO): incremental table extraction, full_refresh table extraction, graceful shutdown via SIGTERM, crash recovery (HWM correct after restart), schema evolution (add column → warn+skip)
- [ ] 12.3 Verify: `cargo build && cargo clippy -- -D warnings && cargo test`

## 13. Coverage Gate

- [ ] 13.1 Install `cargo-llvm-cov` and verify setup: `cargo llvm-cov --summary-only`
- [ ] 13.2 Run `cargo llvm-cov --fail-under-lines 90` — if below 90%, add missing tests for uncovered paths
- [ ] 13.3 Verify: `cargo llvm-cov --fail-under-lines 90` passes

## 14. CI/CD & Release Pipeline

- [ ] 14.1 Create `.github/workflows/ci.yml`: checkout → install toolchain (from rust-toolchain.toml) + llvm-tools-preview → cargo test → cargo clippy -- -D warnings → cargo llvm-cov --fail-under-lines 90 → upload coverage report artifact
- [ ] 14.2 Create `.github/workflows/release.yml` (trigger on `v*` tag): build 4 cross-compiled targets (x86_64-musl, aarch64-musl, x86_64-gnu, aarch64-gnu) using `cross` → generate SHA256 checksums → create GitHub Release with binaries + checksums
- [ ] 14.3 Add GHCR publish job to `release.yml`: multi-arch Docker build (linux/amd64, linux/arm64) → push to `ghcr.io/<OWNER>/parket:latest` and `ghcr.io/<OWNER>/parket:<version>`
- [ ] 14.4 Create `Dockerfile`: multi-stage build — `rust:alpine` builder stage (musl target) → `alpine:latest` runtime stage with binary at `/usr/local/bin/parket`
- [ ] 14.5 Document: create `docs/ci-cd.md` with workflow descriptions, release process, Docker usage, and multi-arch build details
- [ ] 14.6 Verify: `docker build .` succeeds locally
- [ ] 14.7 Verify: `cargo build && cargo clippy -- -D warnings && cargo test && cargo llvm-cov --fail-under-lines 90`

## 15. Project Documentation Consolidation

- [ ] 15.1 Create `README.md` at project root: project overview, quickstart guide, usage examples (one-shot binary, Docker), build instructions, configuration summary, and links to all `docs/` files
- [ ] 15.2 Create `docs/architecture.md`: high-level architecture, data flow diagram (text-based), module responsibilities, key design decisions summary (cross-referencing design.md), and dependency graph
- [ ] 15.3 Create `docs/configuration.md`: complete env var reference (all required + optional), defaults table, per-table override examples, S3/MinIO setup guide, and troubleshooting common config errors
- [ ] 15.4 Create `docs/contributing.md`: development setup (rustup, llvm-cov, Docker), TDD workflow (red-green-refactor cycle), PR checklist (tests pass, clippy clean, coverage >90%, docs updated), CI expectations, and commit message conventions
- [ ] 15.5 Review all `docs/*.md` files for consistency, cross-link correctness, and completeness against specs
- [ ] 15.6 Verify: all documentation files exist, README.md links are valid, no placeholder content remains
