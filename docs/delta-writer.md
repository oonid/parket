# Delta Writer

The Delta Writer module (`src/writer.rs`) handles all interactions with Delta Lake on S3/MinIO. It provides table creation, batch writes (Append and Overwrite), and High Watermark (HWM) tracking via Delta `commitInfo` metadata.

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

> **Note:** `DeltaOps` is deprecated in deltalake 0.31 in favor of `DeltaTable::create()`, but that API does not exist yet in 0.31.1. All `#[allow(deprecated)]` sites will be replaced when upgrading to deltalake 0.32+.

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

## Arrow Version Compatibility (v54 vs v57)

Parket depends on two crates that pull incompatible Arrow versions:

| Crate | Version | Arrow Dependency |
|-------|---------|-----------------|
| `connectorx` 0.4.5 | arrow 54 | extraction produces v54 `RecordBatch` |
| `deltalake` 0.31.1 | arrow 57 | writer expects v57 `RecordBatch` |

### Where the mismatch manifests

```
BatchExtractor (connectorx, arrow v54)
    produces Vec<RecordBatch> (v54)
         │
         ▼
   Orchestrator ← MUST convert here
         │
         ▼
DeltaWriter (deltalake, arrow v57)
    expects Vec<RecordBatch> (v57)
```

The `RecordBatch` types are completely different at the Rust type-system level. Attempting to pass a v54 batch to a v57 function produces `error[E0308]: mismatched types`.

### Current solution: Arrow IPC Stream Roundtrip

Conversion happens in `orchestrator.rs`:

1. Serialize v54 `RecordBatch` to Arrow IPC streaming format (a stable wire format defined by the Apache Arrow specification)
2. Deserialize the bytes as v57 `RecordBatch`

```rust
fn convert_v54_to_v57(batch: &arrow::record_batch::RecordBatch)
    -> Result<deltalake::arrow::record_batch::RecordBatch>
{
    let mut buf = Vec::new();
    {
        let mut writer = arrow::ipc::writer::StreamWriter::try_new(&mut buf, &*batch.schema())?;
        writer.write(batch)?;
        writer.finish()?;
    }
    let mut reader = deltalake::arrow::ipc::reader::StreamReader::try_new(Cursor::new(buf), None)?;
    reader.next().ok_or_else(|| anyhow::anyhow!("empty IPC stream"))?
}
```

**Performance**: ~5.8ms per 50,000-row batch, representing ~1-3% of total pipeline time (dominated by MariaDB read + S3 write). No data loss — all types, nullability, and metadata are preserved.

### Verified type coverage

All MariaDB types that connectorx extracts pass through the IPC roundtrip without data loss:

| MariaDB Type | Arrow Type | IPC Verified |
|--------------|------------|-------------|
| `INT` | `Int32` | Yes |
| `BIGINT` | `Int64` | Yes |
| `TINYINT`, `SMALLINT` | `Int8`, `Int16` | Yes |
| `FLOAT` | `Float32` | Yes |
| `DOUBLE` | `Float64` | Yes |
| `VARCHAR`, `TEXT` | `Utf8` | Yes |
| `BLOB` | `Binary` | Yes |
| `BOOLEAN` / `TINYINT(1)` | `Boolean` | Yes |
| `DATE` | `Date32` | Yes |
| `DATETIME`, `TIMESTAMP` | `Timestamp(Microsecond, None)` | Yes |
| `DECIMAL(p,s)` | `Decimal128(p,s)` | Yes |
| `JSON` | `Utf8` (stringified) | Yes |

### Future improvements (v2 roadmap)

| Approach | Safe? | Zero-copy? | Speedup vs IPC | Status |
|----------|-------|------------|----------------|--------|
| **Arrow C Data Interface (FFI)** | `unsafe` | Yes (O(1)) | 23-590x | Recommended for v2 |
| `connector_arrow` (arrow 57) | Safe | N/A | Eliminates dual-arrow | Needs spike |
| sqlx + manual Arrow construction | Safe | Yes | Eliminates dual-arrow | v2 candidate |

The FFI approach uses `unsafe` for `from_ffi()` and raw byte copying but benchmarks show 23-590x speedup at zero memory overhead. Correctness verified via `examples/ffi_vs_ipc.rs`.

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
