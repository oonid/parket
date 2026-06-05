# AGENTS.md — Guide for AI Coding Agents

## Build, Lint, and Test Commands

```bash
cargo build                              # Compile
cargo clippy -- -D warnings              # Lint (zero-tolerance)
cargo test --lib                         # Unit + lib tests only (fast, no Docker)
cargo test                               # All tests including integration (requires Docker)
cargo llvm-cov --lib                     # Coverage report (lib only)
cargo llvm-cov --lib --fail-under-lines 90  # Coverage gate (CI hard gate: >90%)
```

**All changes must pass:** `cargo build && cargo clippy -- -D warnings && cargo test --lib && cargo llvm-cov --lib --fail-under-lines 90`

## Project Overview

Parket is a one-shot Rust binary that extracts data from MariaDB and loads it into Delta Lake on S3/MinIO in Apache Arrow format. It auto-discovers table schemas, manages memory dynamically, and writes to Delta Lake with ACID guarantees.

**Rust edition:** 2024, **MSRV:** 1.92

## Module Structure

```
src/
  main.rs          — Entry point: CLI parsing, tracing init, signal handler, orchestrator invocation
  cli.rs           — Clap CLI struct with --check, --progress, --local, --version flags
  config.rs        — Config struct, env var loading via dotenvy, display_safe masking
  state.rs         — AppState/TableState: read/write state.json for run-status bookkeeping
  discovery.rs     — SchemaInspector: column discovery, mode detection, AVG_ROW_LENGTH from information_schema
  query.rs         — QueryBuilder: SQL generation for Incremental (cursor-based) and FullRefresh modes
  extractor.rs     — BatchExtractor: connector_arrow MySQL extraction, adaptive batch sizing (controls SQL LIMIT)
  writer.rs        — DeltaWriter: delta-rs write (Append/Overwrite), HWM extract/store in commitInfo metadata
  orchestrator.rs  — Orchestrator: main loop over tables, error handling, exit codes, schema evolution, signal handling
  preflight.rs     — PreflightCheck: --check mode with DB + S3 connectivity validation
  lib.rs           — Public module tree for integration test access
```

**Dependency flow (unidirectional):** `main → orchestrator → {discovery, extractor, writer, state, query}`

## Key Design Decisions

- **HWM storage:** High Watermark stored in Delta commitInfo metadata (not state.json) for crash safety
- **Two DB connections:** sqlx for metadata queries, connector_arrow (mysql crate) for data extraction
- **Single Arrow version:** both extractor (connector_arrow 0.11.0) and writer (deltalake 0.32.3) use arrow 58 — no conversion layer
- **Type mapping:** `mariadb_type_to_arrow()` in `orchestrator.rs` must match connector_arrow's output: `datetime/timestamp → Utf8`, `decimal → Utf8`, `tinyint/bool → Int8`
- **Async runtime:** tokio. connector_arrow (sync) called directly in async context (acceptable for this workload)
- **Error handling:** `anyhow` everywhere. Exit codes: 0 (success), 1 (partial failure), 2 (fatal)
- **Extraction modes:** Incremental (cursor on `updated_at, id`) and FullRefresh (overwrite)

## Testing Patterns

### Unit Tests
- Live in `#[cfg(test)] mod tests` within each source file
- Use `mockall` for mocking external dependencies (sqlx, connector-x, delta-rs)
- Config tests use `serial_test` to prevent env var race conditions
- All tests must pass without Docker

### Integration Tests
- Live in `tests/` directory
- Use `testcontainers` with real MariaDB + MinIO containers
- Require Docker daemon running
- Tagged with `#[serial_test::serial]`

### Coverage
- Measured with `cargo-llvm-cov`
- Hard gate: >90% line coverage
- Exclusions: `main.rs` entry point wiring (covered by integration tests)

## Testing Conventions

- Construct `Config` directly in orchestrator/preflight tests (don't call `Config::load()`)
- Use `MockSchemaInspect`, `MockExtract`, `MockDeltaWrite`, `MockStateManage` from orchestrator module
- For batches in tests, use `deltalake::arrow` types (arrow 58) — no separate `arrow` v54 crate
- For HWM tests, mocks may use `TimestampMicrosecondArray` + `Int64Array`; production batches use `Utf8` for datetime columns (connector_arrow behaviour)

## CLI Flags

| Flag | Behavior |
|------|----------|
| (default) | Run extraction pipeline |
| `--check` | Pre-flight: validate DB tables + S3 writability, print mode summary, exit |
| `--progress` | Emit per-batch progress logs (batch index, rows, cumulative rows, arrow bytes, duration) |
| `--local <dir>` | Write Delta Lake files to local filesystem instead of S3 (skips S3 config & connectivity) |
| `--version` | Print version from Cargo.toml |
| `--help` | Print help |

## CI Expectations

- **CI workflow** (`.github/workflows/ci.yml`): test → clippy → llvm-cov >90% on every push/PR
- **Release workflow** (`.github/workflows/release.yml`): 4 cross-compiled binaries + multi-arch Docker image on `v*` tags
- **Clippy:** zero warnings (`-D warnings`)
- **Coverage:** `cargo llvm-cov --fail-under-lines 90` must pass

## Environment Variables

See `docs/config.md` for the complete reference. Key required vars:

- `DATABASE_URL` (mysql:// scheme)
- `S3_BUCKET`, `S3_ACCESS_KEY_ID`, `S3_SECRET_ACCESS_KEY`
- `TABLES` (comma-separated)
- `TARGET_MEMORY_MB` (positive integer)
