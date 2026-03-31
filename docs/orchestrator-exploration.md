# Orchestrator Exploration & Design Plan (Task 9)

> **Note:** This was originally prepared for Task 8. Task numbers shifted: former Task 8 (Orchestrator) is now Task 9. The new Task 8 covers FFI-based Arrow v54→v57 conversion in extractor.rs.

## 1. Module Interfaces Summary

### config.rs
 - `Config::load() -> Result<Config>` — loads from env vars
- Fields: `database_url`, `s3_bucket`, `s3_prefix`, `s3_endpoint`, `s3_region`, `s3_access_key_id`, `s3_secret_access_key`, `tables`, `target_memory_mb`, `default_batch_size`, `table_modes`
- `ExtractionMode` enum: `Auto`, `Incremental`, `FullRefresh`

### state.rs
 - `AppState::load(path) -> Result<Self>` / `AppState::load_or_warn(path) -> Self` / `AppState::update_table(name, TableState, path) -> Result<()>`
- `TableState` struct: `last_run_at`, `last_run_status`, `last_run_rows`, `last_run_duration_ms`, `extraction_mode`, `schema_columns_hash`

### discovery.rs
 - `SchemaInspector::new(pool, database)` — needs MySqlPool, cannot be constructed without adapter
- `ColumnInfo` struct: `name`, `data_type`, `column_type`
- Free functions: `filter_unsupported_columns`, `detect_mode`, `compute_schema_hash`

### query.rs
 - `QueryBuilder::build_incremental_query(table, columns, hwm_updated_at, hwm_last_id, batch_size) -> String`
- `QueryBuilder::build_full_refresh_query(table, columns) -> String`

### extractor.rs (produces arrow v54 types)
 - `BatchExtractor::new(database_url, target_memory_mb, default_batch_size)`
- `BatchExtractor::calculate_batch_size(&mut self, avg_row_length) -> u64`
- `BatchExtractor::extract(&mut self, sql) -> Result<Vec<arrow::record_batch::RecordBatch>>` — synchronous
- `convert_batches(v54_batches) -> Result<Vec<V57RecordBatch>>`
- `convert_schema_v54_to_v57(schema) -> Result<Arc<V57Schema>>`

### writer.rs (consumes arrow v57 types via deltalake::arrow)
 - `DeltaWriter::new(bucket, prefix, endpoint, region, access_key, secret_key)`
- `DeltaWriter::new_local(base_dir)`
- `DeltaWriter::ensure_table(&self, table_name, schema) -> Result<DeltaTable>` — async
- `DeltaWriter::append_batch(table_name, batches, hwm)` — async
- `DeltaWriter::overwrite_table(table_name, batches, hwm)` - async
- `DeltaWriter::read_hwm(table_name) -> Result<Option<Hwm>>`
- `extract_hwm_from_batch(batch) -> Option<Hwm>` — free function (v57)
- `Hwm` struct: `updated_at: String`, `last_id: i64`

## 2. Arrow v54 -> v57 Conversion

 Per Task 8 and `docs/arrow_v54_to_v57.md`: FFI-based zero-copy conversion in `extractor.rs`. The orchestrator calls:
- `extractor::convert_batches(v54_batches)` — convert all extracted batches
- `extractor::convert_schema_v54_to_v57(schema)` — convert schema for ensure_table()
The orchestrator does NOT define any conversion logic itself.

## 3. Trait Design for Testability

```rust
#[cfg_attr(test, mockall::automock)]
pub trait SchemaInspect: Send + Sync {
    async fn discover_columns(&self, table: &str) -> Result<Vec<ColumnInfo>>;
    async fn get_avg_row_length(&self, table: &str) -> Result<Option<u64>>;
}

#[cfg_attr(test, mockall::automock)]
pub trait Extract: Send {
    fn calculate_batch_size(&mut self, avg_row_length: Option<u64>) -> u64;
    fn extract(&mut self, sql: &str) -> Result<Vec<arrow::record_batch::RecordBatch>>;
}
#[cfg_attr(test, mockall::automock)]
pub trait DeltaWrite: Send + Sync {
    async fn ensure_table(&self, table_name: &str, schema: V57SchemaRef) -> Result<()>;
    async fn append_batch(
        &self,
        table_name: &str,
        batches: Vec<V57RecordBatch>,
        hwm: Option<&Hwm>,
    ) -> Result<()>;
    async fn overwrite_table(
        &self,
        table_name: &str,
        batches: Vec<V57RecordBatch>,
        hwm: Option<&Hwm>,
    ) -> Result<()>;
    async fn read_hwm(&self, table_name: &str) -> Result<Option<Hwm>>;
    async fn get_schema(&self, table_name: &str) -> Result<Option<V57SchemaRef>>;
}
#[cfg_attr(test, mockall::automock)]
pub trait StateManage {
    fn load_or_default(&mut self, path: &std::path::Path) -> AppState;
    fn update_table(
        &mut self,
        name: &str,
        state: TableState,
        path: &std::path::Path,
    ) -> Result<()>;
}
```

## 4. Thin Wrappers (Adapters)

### SchemaInspectorAdapter
Wraps `SchemaInspector` — constructed from `MySqlPool` (cannot be mocked directly).
```rust
struct SchemaInspectorAdapter {
    pool: sqlx::MySqlPool,
    database: String,
}
// impl SchemaInspect for SchemaInspectorAdapter: delegates to real SchemaInspector
```

### ExtractorAdapter
Wraps `BatchExtractor` — synchronous, &mut self methods.
```rust
struct ExtractorAdapter {
    inner: BatchExtractor,
}
// impl Extract for ExtractorAdapter: delegates to real BatchExtractor
```

### StateManageAdapter
Wraps `AppState` — owns inner state, &mut self for load/update.
```rust
struct StateManageAdapter {
    state: AppState,
}
// impl StateManage for StateManageAdapter: delegates to AppState
```

### DeltaWriterAdapter
Wraps `DeltaWriter` — needs `open_table()` for `get_schema()`.
```rust
struct DeltaWriterAdapter {
    inner: DeltaWriter,
}
// impl DeltaWrite for DeltaWriterAdapter: delegates to real DeltaWriter
// get_schema() uses self.inner.open_table() -> read schema from DeltaTable metadata
```

## 5. Orchestrator Struct
```rust
pub struct Orchestrator<S, E, W, M> {
    config: Config,
    schema_inspect: S,
    extractor: E,
    writer: W,
    state_mgr: M,
    shutdown: watch::Receiver<bool>,
    state_path: PathBuf,
}
```
- Generic parameters enable full monomorphization and test type safety.
- Production uses concrete adapter types; tests use mocks.

 Shutdown from separate task (signal handler). Orchestrator checks before each table/batch.

## 6. Execution Flow

```
run():
  1. Load AppState
  2. For each table in config.tables:
     a. Check shutdown signal → if set, break
     b. Discover columns (SchemaInspect)
     c. Filter unsupported columns
     d. Detect mode (Incremental / FullRefresh)
     e. Get AVG_ROW_LENGTH
     f. Calculate batch size
     g. Build v54 schema from ColumnInfo for ensure_table()
     h. Schema evolution check (Incremental only)
        - Column addition: warn + exclude from SELECT
        - Column drop: error + skip table
        - Type change: error + skip table
     i. Build query (QueryBuilder)
     j. If Incremental: loop extracting batches until 0 rows
        - Extract via BatchExtractor (v54)
        - Convert v54 → v57
        - Extract HWM from batch
        - Append to Delta with HWM
     k. If FullRefresh:
        - Extract all data
        - Convert v54 → v57
        - Overwrite Delta table
     l. Update state.json (success/failure)
  3. Log summary
  4. Return exit code (0/1/2)
```

## 7. Schema Evolution Logic

For Incremental tables only:
1. Discover MariaDB columns → `Vec<ColumnInfo>`
2. Get existing Delta schema → `Option<SchemaRef>` (v57)
3. If Delta table exists:
   - Column in MariaDB but not in Delta → warn, exclude from SELECT
   - Column in Delta but not in MariaDB → error, skip table
   - Column in both but type changed → error, skip table
4. If Delta table doesn't exist → first run, no evolution check

## 8. Exit Codes

| Code | Meaning | When |
|------|---------|------|
| 0 | All tables succeeded | Every table completed without error |
| 1 | Partial failure | At least one table failed, at least one succeeded |
| 2 | Fatal misconfiguration | Config load failed, DB connection failed at startup |

### Checkpoint 9.1 — VERIFIED COMPLETE

All 9 required test scenarios from task 9.1 are implemented and passing (27 orchestrator tests total, 263 project-wide).

| Scenario | Test Name | Result |
|---|---|---|
| All tables succeed (exit 0) | `single_table_succeeds`, `multiple_tables_succeed` | Pass |
| Partial failure (exit 1) | `partial_failure_one_fails` (uses `withf` predicates) | Pass |
| Fatal misconfig (exit 2) | `fatal_all_fail` | Pass |
| Schema evolution: addition (warn+skip) | `schema_evolution_column_addition_warns_and_excludes` | Pass |
| Schema evolution: drop (fail) | `schema_evolution_column_drop_errors` | Pass |
| Schema evolution: type change (fail) | `schema_evolution_type_change_errors` | Pass |
| Batch loop until exhausted | `batch_loop_until_exhausted` (3 extract calls, batch_size=1) | Pass |
| Graceful shutdown signal | `shutdown_signal_stops_processing` (watch channel sends true) | Pass |

**Implementation details:**
- `schema_evolution_check()` at line 241: checks column drops (error), type changes (error), and additions (warn + exclude from SELECT)
- `partial_failure_one_fails` uses `withf` predicates on mock expectations to route `good_table` → success, `bad_table` → error at `discover_columns`
- `batch_loop_until_exhausted` sets `batch_size()` returning 1, `extract()` returns data for calls 0,1 then empty on call 2 → verifies loop calls extract 3 times

**Bug fix (post-checkpoint):** `state_update_failure_on_success_propagates` was failing because `setup_incremental_mocks()` added a generic `expect_update_table()` that consumed the success-state call in FIFO order, returning `Ok(())` instead of the intended `Err("disk full")`. Fixed by setting up state mock expectations explicitly without `setup_incremental_mocks`.

**tasks.md:** 9.1 marked `[x]`

### Checkpoint 9.2 — VERIFIED COMPLETE

`Orchestrator<S,E,W,M>` struct at line 319 with all required fields:
- `config: Config`, `schema_inspect: S`, `extractor: E`, `writer: W`, `state_mgr: M`, `shutdown: watch::Receiver<bool>`, `state_path: PathBuf`
- Constructor `new()` at line 336 with trait bounds: `S: SchemaInspect + Send + Sync`, `E: Extract + Send`, `W: DeltaWrite + Send + Sync`, `M: StateManage + Send`
- 4 adapter structs: `SchemaInspectorAdapter`, `ExtractorAdapter`, `StateManageAdapter`, `DeltaWriterAdapter` (lines 61-203)
- All adapters delegate to real implementations: `SchemaInspector`, `BatchExtractor`, `AppState`, `DeltaWriter`

**tasks.md:** 9.2 marked `[x]`

### Checkpoint 9.3 — VERIFIED COMPLETE

Main table loop implemented across 3 methods:

**`run()`** (lines 360-400): Loads state, iterates `config.tables` sequentially, checks shutdown signal before each table, catches per-table errors, counts succeeded/failed.

**`process_table()`** (lines 402-460): Per-table flow: discover_columns → filter_unsupported → detect_mode → get_avg_row_length → calculate_batch_size → ensure_table → schema_evolution_check (Incremental only) → process_incremental or process_full_refresh → update_table state.

**`process_incremental()`** (lines 462-508): Loop: build_incremental_query with current HWM → extract v54 → convert v57 → extract_hwm_from_batch → append_batch → update HWM → break if 0 rows or batch_rows < batch_size.

**`process_full_refresh()`** (lines 511-524): build_full_refresh_query → extract v54 → convert v57 → overwrite_table.

Verified against spec: sequential processing, batch loop until exhausted, FullRefresh in one pass.

**tasks.md:** 9.3 marked `[x]`

### Checkpoint 9.4 — VERIFIED COMPLETE

`schema_evolution_check()` at lines 241-311 implements all 3 spec scenarios:

| Spec Scenario | Code Path | Behavior | Test |
|---|---|---|---|
| Column added (MariaDB, not Delta) | Line 298-307 | Warn + exclude from SELECT | `schema_evolution_column_addition_warns_and_excludes` Pass |
| Column dropped (Delta, not MariaDB) | Line 258-264 | Error + bail | `schema_evolution_column_drop_errors` Pass |
| Column type changed (both present, different type) | Line 266-288 | Error + bail | `schema_evolution_type_change_errors` Pass |
| First run (no Delta table) | `process_table()` line 428 | Returns `column_names` unchanged | Pass |

**Integration in `process_table()`** (lines 423-432): Only called for `Incremental` mode, Calls `self.writer.get_schema()` → if `Some`, runs `schema_evolution_check()` and returns `select_columns` to subsequent extract.

**tasks.md:** 9.4 marked `[x]`

### Checkpoint 9.5 — VERIFIED COMPLETE

**Task:** Implement per-table error handling: catch errors, log, mark table failed in state.json, continue

**Implementation in `run()` (lines 379-396):**

When `process_table()` returns `Err`, the orchestrator now:
1. Logs the error via `error!()` macro
2. Increments `failed` counter
3. Calls `state_mgr.update_table()` with `TableState { last_run_status: Some("failed"), ... }` to persist the failure to `state.json`
4. If the state update itself fails, logs an additional error but does NOT abort the run
5. Continues to the next table

```rust
Err(e) => {
    error!(table = table_name, error = %e, "table failed");
    failed += 1;
    if let Err(se) = self.state_mgr.update_table(
        table_name,
        TableState {
            last_run_at: Some(format_timestamp_now()),
            last_run_status: Some("failed".to_string()),
            last_run_rows: None,
            last_run_duration_ms: None,
            extraction_mode: None,
            schema_columns_hash: None,
        },
        &self.state_path,
    ) {
        error!(table = table_name, error = %se, "failed to update state for failed table");
    }
}
```

**Test verification:**
- `partial_failure_one_fails`: Updated to expect `update_table` with `last_run_status == "failed"` for `bad_table` — PASS
- `fatal_all_fail`: Updated to expect `update_table` with `last_run_status == "failed"` for `bad1` — PASS
- `failed_table_writes_failed_state_to_state_json`: New test verifying failed state is written with correct fields — PASS
- `failed_state_update_error_does_not_abort_run`: New test verifying that a failed state write doesn't abort processing of subsequent tables — PASS

**tasks.md:** 9.5 marked `[x]`

### Checkpoint 9.6 — VERIFIED COMPLETE

**Task:** Implement exit code logic: 0 (all success), 1 (partial failure), 2 (fatal)

**Already implemented in `run()` (lines 407-413):**

```rust
if failed > 0 && succeeded > 0 {
    ExitCode::PartialFailure   // code 1
} else if failed > 0 {
    ExitCode::Fatal              // code 2
} else {
    ExitCode::Success            // code 0
}
```

**Test verification:**
- `single_table_succeeds` / `multiple_tables_succeed`: ExitCode::Success — PASS
- `partial_failure_one_fails`: ExitCode::PartialFailure — PASS
- `fatal_all_fail`: ExitCode::Fatal — PASS
- `exit_code_values`: Verifies enum values 0, 1, 2 — PASS

**tasks.md:** 9.6 marked `[x]`

### Checkpoint 9.7 — VERIFIED COMPLETE

**Task:** Document: create `docs/orchestrator.md` with execution flow, error handling strategy, exit codes, and schema evolution rules

**Created:** `docs/orchestrator.md` covering:
- Execution flow (ASCII diagram of `run()` → `process_table()` → `process_incremental()` / `process_full_refresh()`)
- Error handling strategy table (per-table, state-update-on-success, state-update-on-failure, fatal)
- Exit code table (0/1/2 with conditions)
- Schema evolution rules (addition/drop/type-change/unsupported/first-run)
- Module structure: `Orchestrator<S,E,W,M>` generics, 4 trait definitions, 4 adapter structs
- Shutdown signal behavior
- MariaDB to Arrow type mapping table
- State update payloads (success vs failure)

**tasks.md:** 9.7 marked `[x]`

### Checkpoint 9.8 — VERIFIED COMPLETE

**Task:** Verify: `cargo build && cargo clippy -- -D warnings && cargo test`

```
cargo build          → ok
cargo clippy -D warn → ok (0 warnings)
cargo test           → 263 passed, 0 failed
cargo llvm-cov       → 94.74% line coverage (orchestrator.rs: 91.86%)
```

**Bug fix in this verification pass:** `state_update_failure_on_success_propagates` test was failing. Root cause: `setup_incremental_mocks()` registered a generic `expect_update_table()` returning `Ok(())` before the specific expectation returning `Err("disk full")`. mockall processes expectations in FIFO order, so the generic one consumed the call. Fixed by setting up state mock expectations manually without `setup_incremental_mocks`.

**Supporting changes in other modules:**
- `src/state.rs`: `AppState` now derives `Clone` — needed for `StateManageAdapter` to return a clone from `load_or_default()`
- `src/writer.rs`: `open_table()` changed from private to `pub` — needed for `DeltaWriterAdapter::get_schema()` to read existing Delta table schema
- `src/extractor.rs`: `make_v54_single_col_int64()` changed from private to `pub` — shared test helper for orchestrator tests

**tasks.md:** 9.8 marked `[x]`

---

## Task 9 — COMPLETE

All subtasks 9.1–9.8 verified and passing. Task 9 is fully complete.

