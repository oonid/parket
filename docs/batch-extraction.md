# Batch Extraction

The batch extraction module (`src/extractor.rs`) reads data from MariaDB via connector-x's streaming API and produces Arrow `RecordBatch`es within configurable memory bounds.

## Memory Model

Parket uses a single memory budget (`TARGET_MEMORY_MB`) to control how much Arrow data is buffered at any time. The budget applies per-batch, not across all tables.

The flow:

1. **Initial estimate** — `AVG_ROW_LENGTH` from MariaDB `information_schema.tables` is used to calculate a row count per batch: `batch_size = (TARGET_MEMORY_MB * 1024 * 1024) / AVG_ROW_LENGTH`
2. **Streaming extraction** — connector-x produces `RecordBatch`es with at most `batch_size` rows each
3. **Adaptive adjustment** — after the first batch, actual Arrow memory is measured and `batch_size` is recalibrated if the estimate was off by more than 2x
4. **Hard ceiling** — if any single batch exceeds `2 * TARGET_MEMORY_MB` bytes, the batch size is halved

## Batch Size Calculation

### From AVG_ROW_LENGTH

```
batch_size = floor(TARGET_MEMORY_MB * 1024 * 1024 / AVG_ROW_LENGTH)
```

| TARGET_MEMORY_MB | AVG_ROW_LENGTH | batch_size |
|------------------|----------------|------------|
| 512 | 100 | 5,368,709 |
| 512 | 1000 | 536,870 |
| 1 | 100 | 10,485 |
| 1 | 8 | 131,072 |

### Fallback (AVG_ROW_LENGTH unavailable)

When `AVG_ROW_LENGTH` is 0 or NULL (e.g. a new or empty table), the `DEFAULT_BATCH_SIZE` env var is used (default: 10,000 rows).

## Adaptive Sizing Algorithm

After the first non-empty `RecordBatch` is received:

1. Measure actual Arrow bytes: `actual_bytes = RecordBatch::get_array_memory_size()`
2. Calculate actual bytes per row: `actual_bytes_per_row = actual_bytes / row_count`
3. Calculate estimated bytes per row from the initial config: `estimated_bytes_per_row = TARGET_MEMORY_BYTES / batch_size`
4. Compute ratio: `ratio = actual_bytes_per_row / estimated_bytes_per_row`
5. If ratio is outside `[0.5, 2.0]` (i.e. actual differs from estimate by more than 2x in either direction):
   - Recalculate: `new_batch_size = TARGET_MEMORY_BYTES / actual_bytes_per_row`
   - Update batch_size for subsequent extractions

Adaptation happens **once** per `BatchExtractor` instance (tracked by the `adapted` flag). Subsequent tables get fresh extractors.

## Hard Memory Ceiling

The hard ceiling is `2 * TARGET_MEMORY_MB * 1024 * 1024` bytes. If any `RecordBatch` exceeds this:

- A warning is logged with actual bytes, ceiling bytes, old and new batch sizes
- `batch_size` is halved (minimum 1)
- This check runs on every batch (not just the first)

## connector-x Streaming API

Parket uses `connectorx::get_arrow::new_record_batch_iter` — the streaming variant of the connector-x API. This avoids loading the entire result set into memory.

### API Usage

```rust
let mut iter = new_record_batch_iter(&source_conn, None, queries, batch_size, None);
let (_schema_batch, _col_names) = iter.get_schema();
iter.prepare();
while let Some(batch) = iter.next_batch() {
    // process RecordBatch
}
```

- `batch_size` controls the maximum rows per `RecordBatch` produced by connector-x
- `prepare()` spawns a background thread for data fetching
- `next_batch()` returns `Option<RecordBatch>` — `None` signals end-of-result

### Abstraction for Testability

The `CxStreamer` trait wraps connector-x's `RecordBatchIterator`:

```rust
pub trait CxStreamer {
    fn prepare(&mut self);
    fn next_batch(&mut self) -> Option<RecordBatch>;
}
```

`BatchExtractor::extract_from_stream<S: CxStreamer>()` uses this trait, allowing unit tests to inject a `MockStreamer` without needing a real MariaDB connection.

## Interaction with Query Builder

The SQL passed to `BatchExtractor::extract(sql)` comes from `QueryBuilder` (see [Query Patterns](query-patterns.md)). For Incremental mode, each call receives a query windowed by the current HWM. The extractor returns all batches for that query; the orchestrator loops until 0 rows are returned.

## Key Struct: BatchExtractor

| Field | Purpose |
|-------|---------|
| `database_url` | MariaDB connection string |
| `target_memory_mb` | Memory budget from config |
| `default_batch_size` | Fallback when AVG_ROW_LENGTH unavailable |
| `batch_size` | Current batch size (may be adapted) |
| `adapted` | Whether adaptive sizing has run (once-only) |
