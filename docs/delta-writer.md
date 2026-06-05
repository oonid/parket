# Delta Writer

The Delta Writer module (`src/writer.rs`) handles all interactions with Delta Lake on S3/MinIO. It provides table creation, batch writes (Append and Overwrite), and High Watermark (HWM) tracking via Delta `commitInfo` metadata.

## Status Update (2026-06-06) — UPGRADE COMPLETED

**deltalake upgraded 0.31.1 → 0.32.3** and **connectorx replaced with connector_arrow 0.11.0**, both on 2026-06-06. The writer and extractor now use the same arrow version (58). The FFI/IPC conversion layer has been deleted entirely. All `#[allow(deprecated)]` `DeltaOps` workarounds have been removed — the writer now uses `DeltaTable::create()` and `DeltaTable::write()` directly. See `docs/arrow_v54_to_v57.md` for the full migration record.

## S3 Path Layout

Each MariaDB table maps to one independent Delta table:

```
s3://{S3_BUCKET}/{S3_PREFIX}/{table_name}/
```

| Component | Source | Example |
|-----------|--------|---------|
| `S3_BUCKET` | env var | `data-lake` |
| `S3_PREFIX` | env var, default `parket` | `parket` |
| `table_name` | from `TABLES` list | `orders` |

Full example: `s3://data-lake/parket/orders/`

Independent tables allow independent schema evolution, independent HWM, and independent querying by downstream consumers.

## S3/MinIO Connection

`DeltaWriter` configures delta-rs storage options from environment variables:

```rust
DeltaWriter::new(
    &config.s3_bucket,
    &config.s3_prefix,
    config.s3_endpoint.as_deref(),
    &config.s3_region,
    &config.s3_access_key_id,
    &config.s3_secret_access_key,
)
```

The following storage options are set:

| Option | Value |
|--------|-------|
| `AWS_REGION` | from `S3_REGION` |
| `AWS_ACCESS_KEY_ID` | from `S3_ACCESS_KEY_ID` |
| `AWS_SECRET_ACCESS_KEY` | from `S3_SECRET_ACCESS_KEY` |
| `AWS_ENDPOINT_URL` | from `S3_ENDPOINT` (only if set) |
| `AWS_ALLOW_HTTP` | `"true"` (required for MinIO) |
| `AWS_S3_ALLOW_UNSAFE_RENAME` | `"true"` (single-writer mode) |

## Table Creation: `ensure_table()`

Before writing, the writer checks whether a Delta table exists at the target path:

- **Table exists**: returns the existing `DeltaTable` handle, no-op.
- **Table does not exist** (`NotATable` error): creates a new Delta table using the Arrow schema from the first `RecordBatch`.
- **Other error** (e.g., S3 unreachable): propagates the error with context.

The Arrow schema is converted to a Delta schema via `arrow_schema_to_delta()`, which maps Arrow types to Delta types:

| Arrow Type | Delta Type |
|------------|------------|
| `Boolean` | `BOOLEAN` |
| `Int8`, `Int16`, `Int32` | `INTEGER` |
| `Int64` | `LONG` |
| `UInt8`, `UInt16`, `UInt32` | `INTEGER` |
| `UInt64` | `LONG` |
| `Float16`, `Float32` | `FLOAT` |
| `Float64` | `DOUBLE` |
| `Utf8`, `LargeUtf8` | `STRING` |
| `Binary`, `LargeBinary` | `BINARY` |
| `Date32`, `Date64` | `DATE` |
| `Timestamp(_, _)` | `TIMESTAMP` |
| `Decimal128(p, s)`, `Decimal256(p, s)` | `DECIMAL(p, s)` |
| Any other type | **Error** (unsupported) |

> **Note (2026-06-06):** All 5 `#[allow(deprecated)]` `DeltaOps` sites have been removed. The writer now uses `DeltaTable::create()` (not feature-gated) and `DeltaTable::write()` (requires the `datafusion` feature — same as the deprecated path). The `datafusion` feature remains enabled. Dropping it requires migrating writes to `deltalake::writer::RecordBatchWriter` (not feature-gated). `ensure_table()` handles both `DeltaTableError::NotATable` (path exists, no `_delta_log/`) and `DeltaTableError::KernelError` containing "does not exist" (path itself absent) — deltalake 0.32 changed this error variant for non-existent paths.

## Write Operations

### Append (Incremental mode)

`append_batch()` writes `RecordBatch`es to an existing Delta table using `SaveMode::Append`. Each batch is committed independently with its HWM metadata. Multiple append commits accumulate — data from previous runs persists.

### Overwrite (FullRefresh mode)

`overwrite_table()` writes data using `SaveMode::Overwrite`, atomically replacing the entire Delta table contents. If extraction fails mid-run before the overwrite commit, the existing data remains intact (Delta Lake ACID guarantee).

### Empty batch handling

Both `append_batch()` and `overwrite_table()` return `Ok(())` immediately when called with an empty `Vec<RecordBatch>`, avoiding unnecessary Delta log commits.

## High Watermark (HWM) Tracking

The HWM is the single source of truth for incremental extraction progress. It is stored **only** in Delta `commitInfo` metadata — not in `state.json`.

### Writing HWM

When a batch is committed, `extract_hwm_from_batch()` scans the `RecordBatch` to find:
- The maximum `updated_at` timestamp value
- The corresponding maximum `id` (for tiebreaking when timestamps are equal)

The HWM is written as custom metadata in the Delta `commitInfo`:

```json
{
  "hwm_updated_at": "2026-03-28 10:00:00",
  "hwm_last_id": "98765"
}
```

### Reading HWM

`read_hwm()` reads the latest commit from the Delta log:

| Scenario | Behavior |
|----------|----------|
| Delta table does not exist | Returns `None` (first run) |
| Delta table exists but no commits | Returns `None` (warns) |
| Delta table has commits but no HWM fields | Returns `None` (warns — written by another tool) |
| Delta table has HWM in latest commit | Returns `Some(Hwm)` |

### Timestamp type support

`extract_hwm_from_batch()` handles multiple Arrow timestamp representations:

| Arrow Type | Source |
|------------|--------|
| `Timestamp(Microsecond, _)` | connector-x default for MariaDB `DATETIME`/`TIMESTAMP` |
| `Timestamp(Millisecond, _)` | alternative precision |
| `Timestamp(Second, _)` | alternative precision |
| `Utf8` | string timestamps |
| Any other type | returns `None` |

Null values within timestamp columns are treated as empty strings (sorted below any real timestamp).

### Why commitInfo, not state.json?

The HWM is stored in Delta `commitInfo` because:

1. **Atomicity**: The HWM is written atomically with the data commit. A crash between committing data and updating a separate state file cannot lose HWM progress.
2. **Single source of truth**: The HWM always reflects the actual data committed to Delta Lake.
3. **Trade-off**: Reading HWM requires an S3 round-trip at startup (once per table per run). This is acceptable for a one-shot binary.

## Arrow Version Compatibility

**Current state (as of 2026-06-06, post-migration):**

| Crate | Version | Arrow Dependency |
|-------|---------|-----------------|
| `connector_arrow` 0.11.0 | arrow 58 | extraction produces arrow 58 `RecordBatch` |
| `deltalake` 0.32.3 | arrow 58 | writer expects arrow 58 `RecordBatch` |

There is now a single arrow version (58) in the dependency tree. No conversion layer exists.

```
BatchExtractor (connector_arrow 0.11.0, arrow 58)
    produces Vec<RecordBatch> (arrow 58)
         │
         ▼  no conversion needed
         ▼
DeltaWriter (deltalake 0.32.3, arrow 58)
    writes Vec<RecordBatch> (arrow 58)
```

### Current extraction: connector_arrow (no conversion)

Extraction is via `connector_arrow` 0.11.0 (arrow 58). The extractor returns `Vec<deltalake::arrow::record_batch::RecordBatch>` directly — the same type the writer expects. No conversion step exists.

```rust
// BatchExtractor::extract() in extractor.rs
let opts = mysql::Opts::from_url(&self.database_url)?;
let conn = mysql::Conn::new(opts)?;
let mut ca_conn = connector_arrow::mysql::MySQLConnection::new(conn);
let mut stmt = ca_conn.query(sql)?;
let reader = stmt.start([])?;
let batches: Vec<deltalake::arrow::record_batch::RecordBatch> =
    reader.collect::<Result<_, _>>()?;
```

### Type coverage

MariaDB types as extracted by connector_arrow 0.11.0:

| MariaDB Type | Arrow Type | Delta Type | Notes |
|--------------|------------|------------|-------|
| `INT`, `MEDIUMINT` | `Int32` | `INTEGER` | |
| `BIGINT` | `Int64` | `LONG` | |
| `TINYINT` (signed) | `Int8` | `INTEGER` | |
| `TINYINT UNSIGNED` | `UInt8` | `INTEGER` | |
| `SMALLINT` | `Int16` | `INTEGER` | |
| `FLOAT` | `Float32` | `FLOAT` | |
| `DOUBLE` | `Float64` | `DOUBLE` | |
| `DECIMAL` | `Utf8` | `STRING` | Exact decimal string; changed from `Float64` |
| `VARCHAR`, `TEXT`, `CHAR` | `Utf8` | `STRING` | |
| `BLOB`, `BINARY` | `Binary` | `BINARY` | |
| `BOOLEAN`, `BOOL`, `TINYINT(1)` | `Int8` | `INTEGER` | 0 or 1; changed from `Boolean` |
| `DATE` | `Utf8` | `STRING` | |
| `DATETIME`, `TIMESTAMP` | `Utf8` | `STRING` | Format: `"YYYY-MM-DDTHH:MM:SS.ffffff"`; changed from `Timestamp(Microsecond, None)` |
| `JSON` | `Utf8` | `STRING` | Stringified JSON |

### Future improvements

| Approach | Status |
|----------|--------|
| **Upgrade deltalake to 0.32.3** | **Done** — arrow 58, `DeltaOps` removed |
| **Replace connectorx with `connector_arrow` 0.11.0** | **Done** — single arrow version (58), no conversion layer |
| **Remove `datafusion` feature** | Blocked — `DeltaTable::write()` is datafusion-gated; requires migrating to `RecordBatchWriter` first |
| **Arrow C Data Interface (FFI)** optimization | Not needed — no cross-version boundary exists anymore |

**Note on `DATETIME`/`TIMESTAMP` columns:** connector_arrow returns these as `Utf8` strings (format `"YYYY-MM-DDTHH:MM:SS.ffffff"`), not `Timestamp(Microsecond, None)`. `extract_hwm_from_batch()` handles both `Timestamp` and `Utf8` HWM columns — no change needed there. Delta tables created by connectorx (with `Timestamp` schema) will hit schema evolution errors on first incremental run; those tables need a one-time recreate.

## Table Introspection: `open_table()`

`open_table()` is a public method that opens a Delta table handle for a given table name. It is used by the orchestrator's `DeltaWriterAdapter` to implement `get_schema()`, which reads the existing Delta table's Arrow schema for schema evolution checks.

```rust
let table = writer.open_table("orders").await?;
let schema = table.snapshot()?.schema();
```

This method returns `Err` if the table does not exist or is unreachable.

## Testing

Writer tests use a dual strategy:

1. **Pure function tests**: `extract_hwm_from_batch()`, `build_commit_properties()`, `arrow_schema_to_delta()`, timestamp formatting — tested with constructed `RecordBatch`es in memory.
2. **Local filesystem integration tests**: `ensure_table()`, `append_batch()`, `overwrite_table()`, `read_hwm()` — tested against real Delta tables on local filesystem via `DeltaWriter::new_local()`, which uses `file://` URLs instead of `s3://`.

S3 connection error handling is verified by pointing the writer at an unreachable endpoint.

Coverage: **99.35% line coverage** on `writer.rs` (84 tests).
