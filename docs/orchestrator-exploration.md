# Orchestrator Exploration & Design Plan (Task 9)

> **Note:** This was originally prepared for Task 8. Task numbers have shifted: former Task 8 (Orchestrator) is now Task 9. The new Task 8 covers FFI-based Arrow v54→v57 conversion in extractor.rs.

## 1. Module Interfaces Summary

### config.rs
- `Config::load() -> Result<Config>` — loads from env vars
- Fields used by orchestrator: `database_url`, `s3_bucket`, `s3_prefix`, `s3_endpoint`, `s3_region`, `s3_access_key_id`, `s3_secret_access_key`, `tables`, `target_memory_mb`, `default_batch_size`, `table_modes`
- `ExtractionMode` enum: `Auto`, `Incremental`, `FullRefresh`

### state.rs
- `AppState::load(path) -> Result<Self>` — load from state.json
- `AppState::load_or_warn(path) -> Self` — load or return default (corrupted = first run)
- `AppState::update_table(name, TableState, path) -> Result<()>` — atomic write
- `TableState` struct: `last_run_at`, `last_run_status`, `last_run_rows`, `last_run_duration_ms`, `extraction_mode`, `schema_columns_hash`

### discovery.rs
- `SchemaInspector::new(pool: MySqlPool, database: String) -> Self`
- `SchemaInspector::discover_columns(&self, table: &str) -> Result<Vec<ColumnInfo>>` — async
- `SchemaInspector::get_avg_row_length(&self, table: &str) -> Result<Option<u64>>` — async
- `SchemaInspector::check_updated_at_index(&self, table: &str) -> Result<bool>` — async
- `filter_unsupported_columns(columns: &[ColumnInfo]) -> Vec<ColumnInfo>` — free function
- `detect_mode(columns: &[ColumnInfo], override_mode: Option<&ExtractionMode>) -> ExtractionMode` — free function
- `compute_schema_hash(columns: &[ColumnInfo]) -> String` — free function
- `ColumnInfo` struct: `name`, `data_type`, `column_type`

### query.rs
- `QueryBuilder::build_incremental_query(table, columns, hwm_updated_at, hwm_last_id, batch_size) -> String`
- `QueryBuilder::build_full_refresh_query(table, columns) -> String`

### extractor.rs (produces arrow v54 types)
- `BatchExtractor::new(database_url, target_memory_mb, default_batch_size) -> Self`
- `BatchExtractor::calculate_batch_size(&mut self, avg_row_length: Option<u64>) -> u64`
- `BatchExtractor::extract(&mut self, sql: &str) -> Result<Vec<arrow::record_batch::RecordBatch>>` — synchronous
- `CxStreamer` trait for testability — `prepare()`, `next_batch()`
- Output: `Vec<arrow::record_batch::RecordBatch>` (v54)

### writer.rs (consumes arrow v57 types via deltalake::arrow)
- `DeltaWriter::new(bucket, prefix, endpoint, region, access_key, secret_key) -> Self`
- `DeltaWriter::new_local(base_dir) -> Self`
- `DeltaWriter::ensure_table(&self, table_name, schema: SchemaRef) -> Result<DeltaTable>` — async
- `DeltaWriter::append_batch(&self, table_name, batches: Vec<RecordBatch>, hwm: Option<&Hwm>) -> Result<()>` — async
- `DeltaWriter::overwrite_table(&self, table_name, batches: Vec<RecordBatch>, hwm: Option<&Hwm>) -> Result<()>` — async
- `DeltaWriter::read_hwm(&self, table_name) -> Result<Option<Hwm>>` — async
- `extract_hwm_from_batch(batch: &RecordBatch) -> Option<Hwm>` — free function (v57 RecordBatch)
- `Hwm` struct: `updated_at: String`, `last_id: i64`

## 2. Arrow v54 → v57 Conversion

Critical design constraint: connector-x produces `arrow::record_batch::RecordBatch` (v54), but `DeltaWriter` expects `deltalake::arrow::record_batch::RecordBatch` (v57). These are completely different types.

**Solution** (Task 8, per `docs/arrow_v54_to_v57.md`): FFI-based zero-copy conversion in `extractor.rs`.

The orchestrator calls the public conversion functions from `extractor.rs`:

- `extractor::convert_batches(v54_batches) -> Result<Vec<V57RecordBatch>>` — convert all extracted batches
- `extractor::convert_schema_v54_to_v57(&v54_schema) -> Result<Arc<V57Schema>>` — convert schema for `ensure_table()`

The orchestrator does NOT define any conversion logic itself — it imports from `extractor.rs`. When connector-x eventually supports arrow v57, the conversion functions in `extractor.rs` are simply deleted.

## 3. Trait Design for Testability

The orchestrator needs trait abstractions over external services (DB, S3) to enable unit testing with `mockall`. Without traits, we cannot test orchestrator logic without real MariaDB/MinIO.

### Proposed Traits

```rust
// In orchestrator.rs — defines the contract for each dependency

#[cfg_attr(test, mockall::automock)]
pub trait SchemaInspect: Send + Sync {
    async fn discover_columns(&self, table: &str) -> Result<Vec<ColumnInfo>>;
    async fn get_avg_row_length(&self, table: &str) -> Result<Option<u64>>;
}

#[cfg_attr(test, mockall::automock)]
pub trait StateManage: Send + Sync {
    fn load_or_default(&self, path: &Path) -> AppState;
    fn update_table(&mut self, name: &str, state: TableState, path: &Path) -> Result<()>;
}

#[cfg_attr(test, mockall::automock)]
pub trait DeltaWrite: Send + Sync {
    async fn ensure_table(&self, table: &str, schema: SchemaRef) -> Result<()>;
    async fn append_batch(&self, table: &str, batches: Vec<RecordBatch>, hwm: Option<&Hwm>) -> Result<()>;
    async fn overwrite_table(&self, table: &str, batches: Vec<RecordBatch>, hwm: Option<&Hwm>) -> Result<()>;
    async fn read_hwm(&self, table: &str) -> Result<Option<Hwm>>;
    async fn get_schema(&self, table: &str) -> Result<Option<SchemaRef>>;
}

#[cfg_attr(test, mockall::automock)]
pub trait Extract: Send {
    fn calculate_batch_size(&mut self, avg_row_length: Option<u64>) -> u64;
    fn extract(&mut self, sql: &str) -> Result<Vec<arrow::record_batch::RecordBatch>>;
}
```

### Thin Wrappers Around Existing Types

```rust
struct SchemaInspectorAdapter {
    inner: SchemaInspector,
}
impl SchemaInspect for SchemaInspectorAdapter { /* delegates */ }

struct StateManagerAdapter {
    state: AppState,
}
impl StateManage for StateManagerAdapter { /* delegates */ }

struct DeltaWriterAdapter {
    inner: DeltaWriter,
}
impl DeltaWrite for DeltaWriterAdapter { /* delegates */ }

struct ExtractorAdapter {
    inner: BatchExtractor,
}
impl Extract for ExtractorAdapter { /* delegates */ }
```

### Orchestrator Struct

```rust
pub struct Orchestrator<S, E, W, M>
where
    S: SchemaInspect,
    E: Extract,
    W: DeltaWrite,
    M: StateManage,
{
    config: Config,
    schema: S,
    extractor: E,
    writer: W,
    state_mgr: M,
    shutdown: tokio::sync::watch::Receiver<bool>,
}
```

**Note:** Generic parameters make the struct testable. Production code uses concrete types; tests use mocks.

## 4. Execution Flow

```
main() → Orchestrator::run() → exit_code

run():
  1. Load AppState
  2. For each table in config.tables:
     a. Check shutdown signal → if set, break
     b. Discover columns (SchemaInspect)
     c. Filter unsupported columns
     d. Detect mode (Incremental / FullRefresh)
     e. Get AVG_ROW_LENGTH
     f. Calculate batch size
     g. Read HWM from Delta (Incremental only)
     h. Schema evolution check (compare MariaDB schema vs Delta schema)
        - Column addition: warn + exclude from SELECT
        - Column drop: error + skip table
        - Type change: error + skip table
     i. Build query (QueryBuilder)
     j. If Incremental: loop extracting batches until 0 rows
        - Extract via BatchExtractor (v54)
        - Convert v54 → v57
        - Extract HWM from batch
        - Append to Delta with HWM metadata
     k. If FullRefresh:
        - Extract all data
        - Convert v54 → v57
        - Overwrite Delta table
     l. Update state.json (success/failure)
  3. Log summary
  4. Return exit code (0/1/2)
```

## 5. Schema Evolution Logic

For each Incremental table, before extraction:
1. Discover MariaDB columns → `Vec<ColumnInfo>`
2. Read existing Delta table schema → `Option<SchemaRef>` (v57)
3. If Delta table exists:
   - Compare column names: MariaDB columns vs Delta schema field names
   - **Column in MariaDB but not in Delta** → warn, exclude from SELECT
   - **Column in Delta but not in MariaDB** → error, skip table (column was dropped)
   - **Column in both but type changed** → error, skip table
4. If Delta table doesn't exist → first run, no evolution check needed

The SELECT column list = intersection of MariaDB columns and Delta columns (only matching columns).

## 6. Exit Codes

| Code | Meaning | When |
|------|---------|------|
| 0 | All tables succeeded | Every table completed without error |
| 1 | Partial failure | At least one table failed, at least one succeeded |
| 2 | Fatal misconfiguration | Config load failed, DB connection failed at startup |

## 7. Key Design Decisions

### Why traits instead of concrete types?
The task requires unit tests with mocked dependencies. Without trait abstractions, there's no way to inject mock behavior for SchemaInspector (needs real MySqlPool), BatchExtractor (needs real MariaDB), or DeltaWriter (needs real S3/MinIO). The traits are thin wrappers — zero runtime overhead with monomorphization.

### Why generic parameters instead of Box<dyn Trait>?
Generic parameters enable full monomorphization — the compiler generates specialized code for each concrete type. For a one-shot binary, this is preferred over dynamic dispatch. It also makes the mock types compile-time verified.

### Where does the Arrow conversion live?
Per Task 8 and `docs/arrow_v54_to_v57.md`, the conversion functions live in `extractor.rs` — the module that owns the connector-x (v54) boundary. The orchestrator imports and calls them:

- `extractor::convert_batches(batches)` — `Vec<v54 RecordBatch>` → `Vec<v57 RecordBatch>`
- `extractor::convert_schema_v54_to_v57(schema)` — v54 `Schema` → v57 `SchemaRef`

### Schema conversion for ensure_table()
`ensure_table()` needs a v57 `SchemaRef`. After calling `extractor::convert_schema_v54_to_v57()` on the discovered column schema (built from `ColumnInfo` into a v54 `Schema`), the orchestrator passes the resulting v57 `SchemaRef` to `ensure_table()`.

### FullRefresh: single-pass Overwrite
For FullRefresh mode, all data is extracted and then written with `SaveMode::Overwrite`. This means we collect all batches first, then call `overwrite_table()` once. There's no HWM for FullRefresh.

### Incremental: batch loop
For Incremental mode:
1. Read HWM from Delta
2. Build query with HWM
3. Extract batch
4. If 0 rows → done
5. Convert v54 → v57
6. Compute HWM from batch
7. Append to Delta with HWM
8. Go to step 2 with updated HWM

### Signal handling (task 9, but orchestrator needs to check)
The orchestrator receives a `watch::Receiver<bool>` for shutdown signals. Before starting each new table and before each new batch, it checks if shutdown has been signaled. If so, it finishes the current operation and returns.

## 8. Risks & Open Items

1. **mockall + async**: `mockall::automock` supports async methods. Methods return `Pin<Box<dyn Future>>`. Need to verify the mock expectations work with `await`.

2. **StateManage trait with &mut self**: `update_table()` on AppState takes `&mut self`. The trait needs to reflect this. In the orchestrator, we'll own the StateManage implementation.

3. **DeltaWrite::get_schema()**: Need a way to read the existing Delta table's Arrow schema for schema evolution. This may require adding a method to DeltaWriter or reading the schema from the DeltaTable returned by `ensure_table()`. Alternative: read schema from DeltaTable after `ensure_table()` — the table metadata contains the schema.

4. **Extract trait with &mut self**: `calculate_batch_size()` and `extract()` both take `&mut self`. The trait needs `&mut self`.

5. **FullRefresh batch strategy**: Per the design open questions, for FullRefresh, all batches must be collected before writing (Overwrite replaces the entire table). The extractor returns all data in one call (no LIMIT in FullRefresh queries), so this is natural.

6. **Connector-x batch_size in FullRefresh**: The `build_full_refresh_query()` doesn't include LIMIT. The extractor's `batch_size` parameter controls the internal streaming buffer, not the SQL LIMIT. For FullRefresh, the extractor returns all rows in multiple RecordBatch chunks, but they should all be collected and written as a single Overwrite.
