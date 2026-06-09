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

## Per-Table Initial High Watermark (HWM)

For incremental tables, you can predefine a starting High Watermark (HWM) to seed the first incremental run:

```
TABLE_HWM_<TABLENAME>=<updated_at>,<last_id>
```

**Semantics:** The predefined HWM is used **only** if no stored HWM exists (first run or after deletion). Once a stored HWM is recorded, it always takes precedence over the config value.

**Format:**
- `<updated_at>`: ISO 8601 timestamp (e.g., `2026-01-01T00:00:00.000000`)
- `<last_id>`: Integer (valid i64)
- **Separator:** First comma only; both parts are trimmed of whitespace

**Validation (fail at startup):**
- Missing comma → error
- Empty `updated_at` → error
- Non-numeric `last_id` → error
- Set on a non-incremental table → error (caught at runtime, surfaces in `--check`)

**Example:**

```env
TABLES=orders,customers,products
TABLE_HWM_orders=2026-05-01T00:00:00.000000,999
# First run resumes as if the last extracted row were
# (updated_at=2026-05-01T00:00:00.000000, id=999): only rows with a greater
# (updated_at, id) are extracted. Ignored once a real HWM is committed.
```

## VM Sizing for `TARGET_MEMORY_MB`

`TARGET_MEMORY_MB` sets the per-batch memory budget. The initial batch row count is `TARGET_MEMORY_MB × 1024 × 1024 / AVG_ROW_LENGTH`; after the first batch, adaptive sizing measures the actual Arrow footprint and corrects the estimate (see [Batch Extraction](batch-extraction.md)). It bounds memory the same way for both Incremental batches and FullRefresh chunks.

| Available RAM | Recommended `TARGET_MEMORY_MB` | Status |
|---|---|---|
| 16 GB | 1024 | Proven: 8-table run incl. a 112M-row FullRefresh table, ~28 min, 3.7 GB Parquet |
| 8 GB | 512 | Recommended starting point (proportional); re-test on your data |
| 4 GB | 256 | Recommended starting point (proportional); re-test on your data |

**Measured reality:** in the proven 16 GB run, observed per-batch `arrow_bytes` was ~260–320 MB even with `TARGET_MEMORY_MB=1024` — the target is a ceiling, not a tight fit. delta-rs needs additional working memory to encode each batch into Parquet, so keep the target well below total RAM and watch the `arrow_bytes` field in `--progress` logs to tune.

**`TARGET_MEMORY_MB` does not change total output size or correctness.** A 112M-row table produces the same ~GB of Parquet whether written in 12 large chunks or 44 small ones — only the file count and per-chunk memory differ. If you see output disk usage explode (e.g. one table dwarfing all others), that is a bug, not a tuning problem — see below.

**FullRefresh trade-off at small values:** FullRefresh paginates with `LIMIT … OFFSET …`. Without `ORDER BY`, `OFFSET M` makes MariaDB scan and discard M rows per chunk — O(N²) total read work across chunks. Smaller `TARGET_MEMORY_MB` means more chunks and more redundant scanning. This costs DB read time, not disk space. (Keyset pagination would remove this; see [Query Patterns](query-patterns.md).)

**Runaway disk usage is a symptom of a stuck Incremental loop, not memory tuning.** If an Incremental table re-reads its first page forever (HWM never advances), it appends the same rows on every iteration and never terminates — disk grows without bound and one table dwarfs the rest. The HWM must advance each batch; `extract_hwm_from_batch` supports `id` columns of type Int32/Int64/UInt32/UInt64 and `updated_at` as either a timestamp or a connector_arrow Utf8 string. If you hit this, the output is corrupt — delete the table directory and re-run after fixing the cause.

**Cleaning up between runs:** Delta Lake does not vacuum superseded Parquet files automatically. After an aborted or buggy run, remove the affected table directory and start fresh rather than letting tombstoned files accumulate.

## Verifying Memory Fit Before Deploying to a Constrained VM

Before provisioning a small VM (e.g. 4 GB), measure parket's peak memory on your current machine against your real data. Always build `--release` first — debug builds use more memory and run far slower, and the VM will run release:

```bash
cargo build --release
```

**Definitive test — enforce the cap with no swap (cgroup v2, simulates the VM exactly):**

```bash
systemd-run --user --scope -p MemoryMax=3500M -p MemorySwapMax=0 \
  ./target/release/parket --local /path/to/out --progress
```

- `MemoryMax=3500M` leaves ~500 MB for the OS inside a 4 GB VM — adjust to your target.
- `MemorySwapMax=0` forbids swap, so it tests "fits in RAM" honestly rather than silently thrashing.
- Completes → it fits. Kernel OOM-kills it → it does not; confirm with `journalctl --user -e | grep -i oom`.

**Quick peak-RSS number (no enforcement):**

```bash
/usr/bin/time -v ./target/release/parket --local /path/to/out --progress 2>&1 \
  | grep "Maximum resident"
```

Divide the reported kbytes by 1024 for MB. Run the binary directly — `/usr/bin/time` measures only the process it launches, so `cargo run` would report cargo's RSS, not parket's.

**Live high-water mark while running (second terminal):**

```bash
watch -n2 'grep VmHWM /proc/$(pgrep -f target/release/parket)/status'
```

`VmHWM` is the peak resident set size reached so far. Peak RSS ≈ one Arrow batch (bounded by `TARGET_MEMORY_MB`, up to the 2× hard ceiling) + delta-rs Parquet encode buffers + MySQL result buffering + allocator slack.

## Validation Rules

Parket validates configuration at startup and exits with code 2 on any failure:

- `DATABASE_URL` must start with `mysql://` — other schemes (e.g. `postgres://`) are rejected.
- `TABLES` must not be empty or whitespace-only.
- `TARGET_MEMORY_MB` must parse as a positive integer (> 0).
- All required variables must be present and non-empty.

Error messages identify the specific missing or invalid variable.

## CLI Usage

Command-line flags (`--check`, `--progress`, `--local`, `--version`, `--help`),
the startup banner, and exit codes are documented in the dedicated
[CLI Reference](cli.md).

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
TABLE_HWM_orders=2026-05-01T00:00:00.000000,999
```
