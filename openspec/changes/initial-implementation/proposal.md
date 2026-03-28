## Why

There is no automated pipeline to extract data from MariaDB and land it into a Delta Lake (S3/MinIO) in Apache Arrow format. Manual schema mapping is error-prone and brittle. Parket fills this gap as a zero-configuration, one-shot Rust binary that auto-discovers table schemas, manages memory dynamically, and writes to Delta Lake with ACID guarantees — eliminating manual EL work entirely.

## What Changes

- **New Rust binary (`parket`)** that runs as a one-shot EL tool: extract from MariaDB, load into Delta Lake on S3/MinIO.
- **Configuration via environment variables** loaded from `.env` (DATABASE_URL, S3 credentials, table list, memory budget, per-table mode overrides).
- **Auto-discovery** of table schemas and extraction modes (Incremental vs FullRefresh) by inspecting `information_schema`.
- **Dynamic memory-bounded batch extraction** using connector-x streaming API, with sample-based and adaptive batch sizing.
- **Delta Lake writes** via delta-rs: Append for Incremental mode, Overwrite for FullRefresh mode. High Watermark (HWM) stored in Delta `commitInfo` metadata as the single source of truth.
- **Operational state** (`state.json`) for run-status bookkeeping only (not HWM).
- **Explicit column selection** from `information_schema.columns`, skipping unsupported types (GEOMETRY, etc.).
- **Schema evolution**: fail-fast on column drops/type-changes, warn-and-skip on additions.
- **Structured logging** to stdout via `tracing`.
- **Graceful signal handling** via `tokio::signal` (finish current batch, then exit).
- **Test-Driven Development (TDD)** across all modules — tests written before implementation, using `cargo test` with `mockall` for unit tests (mocked MariaDB/S3) and Docker-based integration tests (real MariaDB + MinIO).
- **Test coverage via `cargo-llvm-cov`** targeting >90% line coverage, enforced as a hard gate in CI.
- **CI/CD via GitHub Actions**: automated CI pipeline (test, clippy, coverage gate on every push/PR) and release pipeline (cross-compiled binary releases to GitHub Releases + multi-arch Alpine container images to GHCR on version tags).
- **Inline documentation** — every task group produces a `docs/<topic>.md` documenting what was built, design rationale, and usage notes. A final task consolidates all per-topic docs into a complete `README.md` and project documentation set.

## Capabilities

### New Capabilities

- `config`: Load and validate all configuration from environment variables (.env), including DATABASE_URL, S3 credentials, table list, TARGET_MEMORY_MB, per-table mode overrides, and optional defaults.
- `state-manager`: Manage operational state in state.json (per-table run status, timing, schema hash). HWM is NOT stored here — it lives in Delta commit metadata.
- `schema-discovery`: Query MariaDB information_schema to discover table columns, detect extraction mode (Incremental if `updated_at` + `id` exist, else FullRefresh), retrieve AVG_ROW_LENGTH, and build explicit column lists skipping unsupported types.
- `query-builder`: Construct safe, paginated SQL queries based on extraction mode and HWM. Incremental uses cursor-based windowing on `(updated_at, id)`. FullRefresh uses plain SELECT with column list.
- `batch-extraction`: Execute SQL queries via connector-x streaming API (`new_record_batch_iter`), producing Arrow RecordBatch within dynamic memory bounds. Sample-based batch sizing with adaptive adjustment.
- `delta-writer`: Write Arrow RecordBatch to Delta Lake on S3/MinIO via delta-rs. Ensure Delta table exists (create if missing). Append for Incremental, Overwrite for FullRefresh. Record HWM in commitInfo metadata atomically.
- `orchestrator`: Main application loop — iterate configured tables sequentially, run the extract-and-load cycle in batches until 0 rows, handle errors per-table (skip and continue), update state.json, and exit with appropriate code (0/1/2).
- `logging`: Structured logging to stdout via tracing with tracing-subscriber. Log level controlled by RUST_LOG.
- `signal-handler`: Catch SIGTERM/SIGINT via tokio::signal, stop starting new batches, let in-flight batch complete, exit gracefully.
- `ci-cd`: GitHub Actions CI workflow (test, clippy, llvm-cov >90% gate) on push/PR. Release workflow (4 cross-compiled binary targets to GitHub Releases + Alpine multi-arch container image to GHCR) on version tags (`v*`).
- `documentation`: Every task group produces a `docs/<topic>.md` capturing design decisions, API surface, and examples. Final task consolidates into README.md, architecture overview, configuration reference, and contributing guide.

### Modified Capabilities

(None — this is the initial implementation with no existing specs.)

## Impact

- **New Rust project**: Cargo.toml with dependencies (connectorx, deltalake, sqlx, tokio, serde_json, dotenvy, tracing, tracing-subscriber).
- **Module structure**: `src/main.rs`, `src/config.rs`, `src/state.rs`, `src/discovery.rs`, `src/query.rs`, `src/extractor.rs`, `src/writer.rs`, `src/orchestrator.rs`.
- **External dependencies**: MariaDB instance (source), S3/MinIO bucket (target).
- **Dev dependencies**: `mockall` (unit test mocks), `cargo-llvm-cov` (coverage), `llvm-tools-preview` rustup component.
- **CI/CD**: `.github/workflows/ci.yml`, `.github/workflows/release.yml`, `Dockerfile` (Alpine-based multi-arch).
- **Documentation**: `docs/` folder with per-module docs, `README.md`, `docs/architecture.md`, `docs/configuration.md`, `docs/contributing.md`.
- **No existing code affected** — greenfield implementation.
