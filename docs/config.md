# Configuration Reference

All configuration is provided through environment variables. Parket uses `dotenvy` to load a `.env` file as a fallback, so you can either export variables in your shell or define them in `.env`.

## Required Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `DATABASE_URL` | MariaDB connection string. Must start with `mysql://`. | `mysql://user:pass@db:3306/mydb` |
| `S3_BUCKET` | Target S3/MinIO bucket name. | `data-lake` |
| `S3_ACCESS_KEY_ID` | S3 access key. | `minioadmin` |
| `S3_SECRET_ACCESS_KEY` | S3 secret key. | `minioadmin` |
| `TABLES` | Comma-separated list of MariaDB tables to extract. Whitespace is trimmed. | `orders,customers,products` |
| `TARGET_MEMORY_MB` | Memory budget per batch in megabytes. Controls the SQL `LIMIT` for both Incremental and FullRefresh modes. Must be a positive integer. | `512` |

## Optional Variables with Defaults

| Variable | Default | Description |
|----------|---------|-------------|
| `S3_ENDPOINT` | _(none)_ | Custom S3 endpoint URL (required for MinIO). |
| `S3_REGION` | `us-east-1` | S3 region. |
| `S3_PREFIX` | `parket` | Path prefix within the bucket. Each table lands at `s3://{S3_BUCKET}/{S3_PREFIX}/{table}/`. |
| `DEFAULT_BATCH_SIZE` | `10000` | Fallback batch row count when `AVG_ROW_LENGTH` is unavailable. |
| `RUST_LOG` | `info` | Log level filter (see [Logging](logging.md)). |

## Per-Table Extraction Mode Overrides

By default, Parket auto-detects the extraction mode for each table based on schema (see [Schema Discovery](schema-discovery.md)). You can override this per table:

```
TABLE_MODE_<TABLENAME>=<mode>
```

Valid modes:

| Mode | Behavior |
|------|----------|
| `auto` _(default)_ | Auto-detect from schema: Incremental if `updated_at` + `id` exist, otherwise FullRefresh. |
| `incremental` | Force incremental extraction with cursor-based pagination on `(updated_at, id)`. |
| `full_refresh` | Force full table overwrite every run. |

**Example:**

```env
TABLES=orders,customers,products
TABLE_MODE_orders=incremental
TABLE_MODE_products=full_refresh
# customers uses auto-detection
```

## VM Sizing for `TARGET_MEMORY_MB`

`TARGET_MEMORY_MB` is divided by `AVG_ROW_LENGTH` (MariaDB's on-disk byte estimate, not the Arrow in-memory size). Arrow's columnar format typically expands 2–4× over the disk representation. Set `TARGET_MEMORY_MB` conservatively enough that the first batch does not exhaust available RAM before the adaptive sizing corrects the estimate.

| Available RAM | Recommended `TARGET_MEMORY_MB` | Notes |
|---|---|---|
| 4 GB | 128 | Leaves ~3.6 GB for OS, delta-rs Parquet encoding overhead, and Arrow buffers |
| 8 GB | 256–512 | Comfortable headroom for large Parquet column encoding |
| 16 GB+ | 512–1024 | Use 1024 only if `AVG_ROW_LENGTH` is reliably close to actual Arrow row size |

**Why `AVG_ROW_LENGTH` underestimates Arrow memory:** MariaDB stores compressed, variable-length row data on disk. Arrow stores each column in fixed-width, uncompressed buffers with validity bitmaps. A row that is 50 bytes on disk may expand to 150–200 bytes in Arrow for wide or text-heavy tables.

**FullRefresh on large tables:** prior to chunked pagination, FullRefresh loaded the entire table into RAM in one call. With the LIMIT+OFFSET loop now in place, `TARGET_MEMORY_MB` bounds each FullRefresh chunk the same way it bounds each Incremental batch. A `TARGET_MEMORY_MB=128` setting on a 4 GB VM keeps peak Arrow memory around 400 MB per chunk, leaving room for the OS and delta-rs.

## Validation Rules

Parket validates configuration at startup and exits with code 2 on any failure:

- `DATABASE_URL` must start with `mysql://` — other schemes (e.g. `postgres://`) are rejected.
- `TABLES` must not be empty or whitespace-only.
- `TARGET_MEMORY_MB` must parse as a positive integer (> 0).
- All required variables must be present and non-empty.

Error messages identify the specific missing or invalid variable.

## CLI Usage

```
parket                    Run the extractor (default mode)
parket --check            Validate config and connectivity without extracting
parket --progress         Emit per-batch progress logs with timing and row counts
parket --local <dir>      Write Delta Lake files to local directory instead of S3
parket --version          Print version
parket --help             Show help
```

### `--check` (Pre-flight Mode)

Connects to the database and S3 bucket, verifies all configured tables exist in `information_schema`, checks S3 writability (write + delete a tiny test object), and prints a per-table mode detection summary:

```
TABLE                           MODE            COLUMNS    AVG_ROW_LEN
orders                          incremental     5          128
products                        full_refresh    3          N/A
pre-flight check passed
```

Exits with code 0 if all OK, code 2 on any failure. No data is extracted.

When combined with `--local`, the S3 connectivity check is skipped (only database connectivity and table discovery are validated).

### `--local <dir>` (Local Mode)

Writes Delta Lake files to a local directory instead of S3. When set:

- S3 credentials (`S3_BUCKET`, `S3_ACCESS_KEY_ID`, `S3_SECRET_ACCESS_KEY`) are **not required** and ignored if present.
- `Config::load_local()` is used instead of `Config::load()`, so validation only requires `DATABASE_URL`, `TABLES`, and `TARGET_MEMORY_MB`.
- Pre-flight (`--check`) skips the S3 writability check.
- The orchestrator uses `DeltaWriter::new_local(dir)` to write Delta tables on the local filesystem.

```
parket --local /data/delta
parket --local ./output --check
```

### `--progress` (Progress Logging)

When set, emits structured INFO logs for each batch with detailed timing and cumulative statistics. Without this flag, only a concise per-batch summary is logged.

Progress log fields for Incremental mode (`"batch progress"` log):

| Field | Description |
|-------|-------------|
| `table` | Table name |
| `batch_index` | 1-based batch counter |
| `rows` | Rows in this batch |
| `cumulative_rows` | Total rows extracted so far for this table |
| `arrow_bytes` | Arrow batch size in bytes |
| `batch_duration_ms` | Wall-clock time for this batch in milliseconds |

Progress log fields for FullRefresh mode (`"full refresh chunk"` log):

| Field | Description |
|-------|-------------|
| `table` | Table name |
| `chunk_index` | 1-based chunk counter |
| `rows` | Rows in this chunk |
| `cumulative_rows` | Total rows written so far for this table |
| `arrow_bytes` | Arrow chunk size in bytes |
| `chunk_duration_ms` | Wall-clock time for this chunk (extract + write) in milliseconds |

Without `--progress`, both modes emit a concise `"batch extracted"` log per batch/chunk.

### `--version` and `--help`

`--version` prints the version from `Cargo.toml`. `--help` prints self-documenting usage with all flags and their descriptions.

### Startup Banner

On every normal invocation (not `--check`), parket logs an INFO-level startup banner:

```
parket v0.1.0 starting  version=0.1.0 tables=3 database_host="mysql://****:****@dbhost:3306" s3_bucket=data-lake
```

In local mode:

```
parket v0.1.0 starting (local mode)  version=0.1.0 tables=3 database_host="mysql://****:****@dbhost:3306" local_dir=/data/delta
```

Sensitive values (database password, S3 secret key) are masked in all log output via `Config::display_safe()`.

### Exit Codes

| Code | Condition |
|------|-----------|
| 0 | All tables extracted successfully (or `--check` passed, or signal received after graceful shutdown) |
| 1 | Partial failure — some tables failed, others succeeded |
| 2 | Fatal error — config invalid, database unreachable, `--check` failed |

## Example `.env`

```env
DATABASE_URL=mysql://readonly:secret@mariadb.internal:3306/production
S3_BUCKET=data-lake
S3_ACCESS_KEY_ID=minioadmin
S3_SECRET_ACCESS_KEY=minioadmin
S3_ENDPOINT=http://minio.internal:9000
S3_REGION=us-east-1
S3_PREFIX=parket
TABLES=orders,customers,products
TARGET_MEMORY_MB=512
DEFAULT_BATCH_SIZE=10000
RUST_LOG=parket=info
TABLE_MODE_orders=incremental
```
