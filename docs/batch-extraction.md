# Batch Extraction

The batch extraction module (`src/extractor.rs`) reads data from MariaDB via `connector_arrow`'s MySQL API and produces Arrow `RecordBatch`es (arrow 58) within configurable memory bounds.

## Memory Model

Parket uses a single memory budget (`TARGET_MEMORY_MB`) to control how much Arrow data is buffered at any time. The budget applies per-batch, not across all tables.

The flow:

1. **Initial estimate** — `AVG_ROW_LENGTH` from MariaDB `information_schema.tables` is used to calculate a row count per batch: `batch_size = (TARGET_MEMORY_MB * 1024 * 1024) / AVG_ROW_LENGTH`
2. **SQL-limited extraction** — the `LIMIT {batch_size}` clause in the query controls how many rows are fetched per call; connector_arrow internally produces `RecordBatch`es of ~1024 rows each from the result set
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

## connector_arrow API

Parket uses `connector_arrow` 0.11.0 with the `src_mysql` feature. The API is synchronous and uses a prepared-statement model.

### API Usage

```rust
use connector_arrow::api::{Connector, Statement};
use connector_arrow::mysql::MySQLConnection;

let opts = mysql::Opts::from_url(&database_url)?;
let conn = mysql::Conn::new(opts)?;
let mut ca_conn = MySQLConnection::new(conn);

let mut stmt = ca_conn.query(sql)?;
let reader = stmt.start([])?;  // [] = no bound parameters
// reader implements Iterator<Item = Result<RecordBatch, ConnectorError>>
let batches: Vec<RecordBatch> = reader.collect::<Result<_, _>>()?;
```

- A new connection is opened per `extract()` call (connector_arrow does not pool connections)
- connector_arrow internally produces `RecordBatch`es of ~1024 rows; the SQL `LIMIT` clause controls total row count
- `stmt.start([])` accepts bind parameters (an improvement over connectorx, which had no parameterized query support)

### MariaDB → Arrow Type Mapping

connector_arrow determines the Arrow type from the MySQL wire protocol column type:

| MariaDB Type | Arrow Type | Notes |
|---|---|---|
| `TINYINT` (signed) | `Int8` | |
| `TINYINT UNSIGNED` | `UInt8` | |
| `SMALLINT` | `Int16` | |
| `INT`, `MEDIUMINT` | `Int32` | |
| `BIGINT` | `Int64` | |
| `FLOAT` | `Float32` | |
| `DOUBLE` | `Float64` | |
| `DECIMAL`, `NUMERIC` | `Utf8` | Exact decimal as string — avoids Float64 precision loss |
| `VARCHAR`, `TEXT`, `CHAR` | `Utf8` | |
| `BLOB`, `BINARY` | `Binary` | |
| `BOOL`, `BOOLEAN` | `Int8` | MySQL TINYINT(1); value is 0 or 1 |
| `DATETIME`, `TIMESTAMP` | `Utf8` | Format: `"YYYY-MM-DDTHH:MM:SS.ffffff"` — timezone is unknown at extraction time |
| `DATE` | `Utf8` | Format: `"YYYY-MM-DDTHH:MM:SS.ffffff"` with zeros |
| `JSON` | `Utf8` | Stringified JSON |

These types directly determine the Delta table schema via `mariadb_type_to_arrow()` in `orchestrator.rs`. `arrow_schema_to_delta()` in `writer.rs` then maps `Utf8` → `STRING`, `Int8`/`Int32` → `INTEGER`, `Int64` → `LONG`.

### Testability

Unit tests for `BatchExtractor` construct `deltalake::arrow::record_batch::RecordBatch` objects directly and pass them to `extract_from_stream_ca()`. No real MariaDB connection is needed for unit tests. Integration tests use `testcontainers-modules` with a real MariaDB container.

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
