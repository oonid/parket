# Task 12 — Integration & End-to-End: Reflection

This document captures what happened during Task 12, what was planned versus what actually occurred, bugs discovered, design decisions made during implementation, and lessons learned.

## Planned vs Actual

### Plan

Task 12 had three checkpoints:

| Checkpoint | Description |
|------------|-------------|
| 12.1 | Wire all modules together in `main.rs` |
| 12.2 | Write integration tests using testcontainers (MariaDB + MinIO) |
| 12.3 | Verify: build + clippy + tests pass |

The exploration document (`docs/integration-exploration.md`) laid out 9 implementation checkpoints (A–I) for 12.2, progressing from infrastructure smoke tests through to full verification.

### Actual

All checkpoints completed, but two critical bugs were discovered during integration testing that blocked checkpoints E, F, and G. The bugs were in production code (`writer.rs` and `orchestrator.rs`), not test code — the integration tests correctly exposed real defects that unit tests missed.

---

## Bugs Discovered

### Bug 1: Missing Nanosecond Timestamp Handling in HWM Extraction

**Location**: `src/writer.rs:419` — `extract_timestamp_as_strings()`

**Symptom**: Integration tests for incremental extraction, crash recovery, and schema evolution all failed. HWM was stored as `None` in Delta commitInfo, causing every orchestrator run to re-extract all rows from the beginning instead of only new rows.

**Root cause**: `extract_timestamp_as_strings()` only handled `TimestampMicrosecondArray`, `TimestampMillisecondArray`, `TimestampSecondArray`, and `StringArray`. connector-x produces `Timestamp(Nanosecond, None)` from MariaDB's `TIMESTAMP(6)` columns. When the function encountered a `TimestampNanosecondArray`, it fell through all match arms and returned an empty vector — yielding `None` HWM.

**Fix**: Added `TimestampNanosecondArray` import and a new match arm in `extract_timestamp_as_strings()`. Created `nanos_to_string()` helper (analogous to `micros_to_string()` and `millis_to_string()`) that converts nanoseconds-since-epoch to a formatted datetime string. Added unit tests `extract_hwm_timestamp_nanos` and `nanos_to_string_conversion`.

**Why unit tests missed it**: The existing unit tests for `extract_hwm_from_batch` only tested with `TimestampMicrosecondArray` and `TimestampSecondArray`, which matched the assumptions about connector-x output at the time. The actual connector-x behavior (nanosecond precision) was only discovered when running against a real MariaDB container.

### Bug 2: `types_equivalent()` Rejected Timestamps with Different Time Units

**Location**: `src/orchestrator.rs:242` — `types_equivalent()`

**Symptom**: Even after fixing Bug 1 (HWM extraction), the schema evolution check in the orchestrator rejected the MariaDB schema as incompatible with the Delta schema, causing tables to fail during the second orchestrator run.

**Root cause**: delta-rs stores timestamps as `Timestamp(Microsecond, Some("UTC"))` when writing to Delta tables, while connector-x produces `Timestamp(Nanosecond, None)` from MariaDB. The `types_equivalent()` function was comparing `DataType` directly (`delta_dt == mariadb_dt`), which compared time unit (Microsecond vs Nanosecond) and timezone (`Some("UTC")` vs `None`) — both mismatched.

**Fix**: Added a specific match arm for `(Timestamp, Timestamp)` that ignores time unit differences (binding both as `_du` / `_mu`) and only compares timezone equivalence. Both `None` and `Some("UTC")` are treated as equivalent (since MariaDB doesn't have timezone-aware timestamps and delta-rs always writes UTC).

**Why unit tests missed it**: The mocked unit tests in `orchestrator.rs` used consistent timestamp types across both sides of the comparison. The type mismatch only manifests when real connector-x output (Nanosecond/None) meets real delta-rs storage (Microsecond/Some("UTC")).

### Both Bugs Were Required Together

Neither fix was sufficient alone:
- Without Bug 1 fix: HWM was `None` → all rows re-extracted every run → crash recovery test failed
- Without Bug 2 fix: Schema evolution check rejected the table → second run errored instead of appending → crash recovery and schema evolution tests failed

---

## Design Decisions During Implementation

### TestEnv Uses `SignalHandler::new()` for Shutdown Channel

The `SignalHandler` wraps a `watch::Sender<bool>` privately, exposing it via `SignalHandler::new()` which returns `(SignalHandler, watch::Receiver<bool>)`. Integration tests use this directly in `make_orchestrator()` for normal tests, and `tokio::sync::watch::channel(false)` for the shutdown test where the test needs to control the sender independently.

### Delta Table Existence Check via `writer.open_table()`

Tests use `DeltaWriter::new()` + `writer.open_table()` (returns `Result<DeltaTable>`) to check if a Delta table exists, not a dedicated `exists()` method. This avoids adding API surface to `DeltaWriter` solely for testing. The `is_ok()` / `is_err()` pattern is clear enough in test assertions.

### Serial Test Execution via `serial_test`

All integration tests use `#[serial_test::serial]` to prevent parallel Docker container conflicts. Each test creates its own containers (~8-10s startup each) for hermetic isolation. Full integration suite runs in ~38s (6 tests), unit tests in ~3s (270 tests).

### Pre-Signal Strategy for Shutdown Test

The graceful shutdown test sends the signal (`tx.send(true)`) before calling `orchestrator.run()`. This guarantees deterministic behavior: the orchestrator sees the shutdown flag immediately on its first check and exits before processing any tables. Attempting mid-batch shutdown with small datasets proved non-deterministic.

---

## Test Architecture

### Six Integration Tests

| Test | What It Validates |
|------|-------------------|
| `smoke_mariadb_and_minio_containers_start` | Container infrastructure works (no parket code) |
| `testenv_fixture_creates_containers_and_bucket` | TestEnv fixture lifecycle |
| `graceful_shutdown_signal_skips_all_tables` | Signal propagation via watch channel |
| `full_refresh_extraction_creates_delta_table_with_all_rows` | FullRefresh mode end-to-end |
| `crash_recovery_hwm_advances_and_only_new_rows_appended` | HWM persistence + incremental append |
| `schema_evolution_add_column_warns_and_skips` | Column addition handled gracefully |

### TestEnv Fixture Design

```
TestEnv::new(tables)
  ├── Start MariaDB container (mariadb:11.3)
  ├── Start MinIO container (minio/minio:RELEASE.2025-02-28T09-55-16Z)
  ├── Create S3 bucket via aws-sdk-s3
  ├── Connect MySqlPool
  ├── Construct Config with dynamic ports
  ├── Create TempDir for state.json isolation
  └── Return TestEnv with all handles

TestEnv::make_orchestrator()
  ├── Create real SchemaInspectorAdapter (MySqlPool)
  ├── Create real ExtractorAdapter (connector-x config)
  ├── Create real DeltaWriterAdapter (S3 config)
  ├── Create real StateManageAdapter
  ├── Create SignalHandler → shutdown_rx
  └── Return Orchestrator with real adapters

TestEnv::make_orchestrator_with_shutdown(rx)
  └── Same as above, but accepts custom shutdown receiver
```

Key property: `make_orchestrator()` creates a fresh `Orchestrator` each call. The crash recovery test calls it twice to simulate a process restart — the second orchestrator reads HWM from Delta commitInfo on S3, not from in-memory state.

### Helper Functions

- `create_minio_bucket(endpoint, bucket)` — Creates S3 bucket via aws-sdk-s3 (delta-rs doesn't auto-create buckets)
- `count_delta_rows(env, table_name)` — Scans Delta table and counts total rows across all parquet files
- `make_config(db_url, s3_endpoint, tables)` — Constructs `Config` with pub fields (bypasses `Config::load()` env var reading)

---

## Lessons Learned

### 1. Integration Tests Expose Real Dependency Mismatches

The two bugs were caused by assumptions about connector-x and delta-rs internal type representations that unit tests with mocked data didn't catch. Integration tests with real containers are essential for validating FFI bridges and type conversion pipelines.

### 2. Test the Full Type Pipeline, Not Just Individual Conversions

The FFI conversion (`convert_v54_to_v57`) was tested in isolation with unit tests, and the HWM extraction was tested with unit tests, but the end-to-end path — connector-x produces nanosecond timestamps → FFI conversion preserves the type → delta-rs writes microsecond timestamps → orchestrator compares both schemas — was only exercised by integration tests.

### 3. Watch Channel Is a Clean Shutdown Abstraction

Using `tokio::sync::watch::channel<bool>` for shutdown propagation worked well. The orchestrator checks `shutdown_rx.has_changed()` at natural boundaries (between tables, between batches). This is simple, testable, and doesn't require signal-specific test infrastructure.

### 4. Container Startup Time Is Acceptable

At ~8-10s per test for container startup, the full integration suite runs in ~38s. This is a reasonable trade-off for hermetic, deterministic tests that validate against real infrastructure.

### 5. Separate Exploration from Reflection

The `docs/integration-exploration.md` document was invaluable during implementation — it captured API investigations, container details, and checkpoint-by-checkpoint progress. The reflection document (`docs/integration.md`) serves a different purpose: recording what went wrong, why, and what was learned. Keeping them separate avoids cluttering the exploration document with retrospective analysis.
