# Architecture Decision Record: Alternatives & Trade-offs

## 1. Extraction Engine: connector-x vs Alternatives

### Decision in Brief
Use `connector-x` for SQL → Arrow extraction with zero-config type mapping.

### connector-x Internal Behavior (Investigated)

connector-x has **two Rust APIs** for producing Arrow data:

| API | Function | Memory Model | Returns |
|---|---|---|---|
| Bulk | `get_arrow()` | Pre-allocates entire result set via `COUNT(*)` then allocates all Arrow arrays upfront | Single `ArrowDestination` with all rows |
| Streaming | `new_record_batch_iter(conn, origin_query, queries, batch_size, pre_exec)` | Produces `RecordBatch`es of configurable `batch_size` incrementally in a background thread | `impl Iterator<Item = RecordBatch>` |

**Key finding:** The streaming API (`ArrowBatchIter`) does **not** require holding the entire result in memory. It calls `dst.aquire_row(n)` per chunk fetched from the source, meaning memory is allocated incrementally. This is the API Parket should use.

#### How connector-x processes a query (internally)

1. Issues `LIMIT 1` subquery to discover result schema
2. If `partition_on` is set: issues `MIN/MAX` then `COUNT(*)` per partition
3. If no partition: issues `SELECT COUNT(*) FROM (subquery)` 
4. Pre-allocates destination memory (bulk API) or sets up batch-size buffers (streaming API)
5. Spawns one thread per partition, streams rows from DB wire → destination buffers

#### connector-x + MySQL specifics
- Supports both `binary` (native protocol) and `text` protocols
- MariaDB is supported through the MySQL protocol (`src_mysql` feature)
- No parameterized query support — queries must be raw SQL strings

### Alternative A: sqlx + Manual Arrow Construction

Use `sqlx::query()` with `.fetch()` streaming cursor, manually build Arrow arrays column-by-column.

**Pros:**
- True streaming: rows are fetched one at a time via async cursor
- Single connection library (sqlx already needed for metadata queries)
- Full control over type mapping and memory allocation
- Supports parameterized queries (no SQL injection risk from interpolated values)
- No `COUNT(*)` overhead query before each extraction

**Cons:**
- Must implement and maintain MariaDB → Arrow type mapping manually (~15-20 types)
- More code to write and test
- Must handle NULL bitmap construction, string deduplication, dictionary encoding manually
- Risk of subtle type-mapping bugs

### Alternative B: mysql crate + Arrow

Use the lower-level `mysql` crate directly (which connector-x itself uses internally).

**Pros:**
- Finer control over connection, streaming, and row buffering
- Avoids connector-x's `COUNT(*)` and `LIMIT 1` overhead queries
- Can use prepared statements with parameter binding

**Cons:**
- All the same manual Arrow construction work as Alternative A
- Lower-level API means more boilerplate for connection management
- connector-x essentially wraps this — why reimplement it?

### Alternative C: ODBC + Arrow

Use `odbc-api` crate for database-agnostic extraction.

**Pros:**
- Database-agnostic (could support PostgreSQL, SQL Server, etc.)
- Supports true cursor-based streaming

**Cons:**
- Requires ODBC driver manager + MariaDB ODBC driver installed on the host
- Complex deployment (system-level dependency)
- Type mapping is more opaque (ODBC C types → Rust → Arrow)
- Not suitable for a "standalone binary" goal

### Recommendation

**Use connector-x with the streaming API (`new_record_batch_iter`).**

The streaming API directly addresses the memory concern — it does not pre-allocate the full result. Combined with `LIMIT` in the SQL query, memory usage is bounded by `batch_size × max_row_size`.

However, be aware of these caveats:
- connector-x still issues auxiliary queries (`LIMIT 1` for schema, `COUNT(*)` for row count) before extraction begins. This adds latency per batch cycle.
- The `COUNT(*)` query on a large table with `WHERE` clause can be expensive if the column isn't indexed.
- No parameterized queries — HWM values must be interpolated into SQL strings (safe for internal use but worth noting).

---

## 2. Memory Management: Dynamic Batch Sizing

### Decision in Brief
Calculate `dynamic_batch_size = (TARGET_MEMORY_MB * 1024 * 1024) / AVG_ROW_LENGTH` using MariaDB's `information_schema.tables.AVG_ROW_LENGTH`.

### Problem: The Formula Has Critical Edge Cases

#### connector-x Memory Reality

Regardless of the batch formula, connector-x's actual memory usage is determined by:
1. **Row count** (from `COUNT(*)` or `LIMIT` clause)
2. **Arrow representation size** — which can differ significantly from MariaDB's stored row size

Arrow uses columnar storage with:
- Validity bitmaps (1 bit per value, per column)
- Fixed-width type padding (e.g., `Int32` is always 4 bytes regardless of MariaDB's storage)
- String columns as `Utf8Array` with offset buffer + data buffer + validity bitmap
- Potential dictionary encoding (not used by connector-x by default)

So the actual Arrow memory per row may be **larger or smaller** than `AVG_ROW_LENGTH`.

#### Edge Cases

| Case | AVG_ROW_LENGTH | Actual Arrow Memory | Risk |
|---|---|---|---|
| Table with large TEXT/BLOB | Low (compressed on disk) | High (uncompressed in Arrow) | **OOM** — batch uses far more than TARGET_MEMORY_MB |
| New/empty table | 0 or NULL | N/A | **Division by zero** or meaningless batch size |
| Wide table (100+ columns) | Accurate on average | Per-row variance is high | Some batches may overshoot |
| Table with many NULLs | Includes NULL overhead | Arrow validity bitmap is compact | Slight undershoot (acceptable) |

#### connector-x Streaming API: The Batch Size Mismatch

The `new_record_batch_iter()` API accepts a `batch_size` parameter, but this controls the number of **rows per RecordBatch**, not a memory limit. So:
- `batch_size = dynamic_batch_size` (rows) from our formula
- Actual memory per RecordBatch = `batch_size × actual_arrow_bytes_per_row`
- If `actual_arrow_bytes_per_row > AVG_ROW_LENGTH`, we overshoot `TARGET_MEMORY_MB`

### Alternative A: Sample-Based Memory Estimation (Recommended Enhancement)

Instead of relying solely on `AVG_ROW_LENGTH`, run a small sample query first:

```sql
SELECT * FROM table_name WHERE ... LIMIT 100
```

Then measure the actual Arrow `RecordBatch` memory footprint and compute:

```
estimated_bytes_per_row = actual_arrow_bytes / sample_row_count
dynamic_batch_size = (TARGET_MEMORY_MB * 1024 * 1024) / estimated_bytes_per_row
```

**Pros:**
- Measures actual Arrow memory, not MariaDB disk estimates
- Naturally accounts for TEXT/BLOB inflation, type padding, NULL bitmaps
- Simple to implement — just measure `RecordBatch::get_array_memory_size()`

**Cons:**
- Adds one extra query per table per run
- Sample may not be representative if data is skewed
- For incremental mode, the WHERE clause may filter to a very different row distribution

### Alternative B: Adaptive Batch Sizing

Start with a conservative batch size, measure actual memory after each batch, and adjust:

```
if last_batch_memory > TARGET_MEMORY_MB * 1.5:
    batch_size = batch_size * 0.5   # halve
elif last_batch_memory < TARGET_MEMORY_MB * 0.5:
    batch_size = batch_size * 1.5   # grow
```

**Pros:**
- Self-correcting — no reliance on estimates
- Adapts to data skew within a table

**Cons:**
- First batch may overshoot significantly
- Requires tracking memory per batch
- More complex state management

### Alternative C: Fixed Row Limit + Memory Guard

Use a conservative fixed row limit (e.g., 50,000 rows) and abort/warn if a batch exceeds a hard memory ceiling:

```rust
if batch.memory_size() > HARD_MEMORY_LIMIT_MB * 1024 * 1024 {
    log::warn!("Batch exceeded memory limit, consider reducing batch size");
}
```

**Pros:**
- Simplest implementation
- Predictable behavior

**Cons:**
- May under-utilize memory on tables with small rows
- May fail on tables with very large rows
- Requires manual tuning per deployment

### Recommendation

**Use Alternative A (Sample-Based) as primary, with Alternative B (Adaptive) as a safety net.**

1. Use `AVG_ROW_LENGTH` as the initial estimate (fast, no extra query)
2. On first batch, measure actual Arrow memory and recalculate
3. Apply adaptive adjustment on subsequent batches
4. Set a hard ceiling as a guardrail

For the `AVG_ROW_LENGTH = 0 or NULL` case: fall back to a safe default batch size (e.g., 10,000 rows).

---

## 3. Dual Connection Libraries: sqlx + connector-x

### Decision in Brief
Use `sqlx` for metadata queries, `connector-x` for data extraction.

### Analysis

Two separate MySQL connections to the same MariaDB instance.

| Concern | Impact |
|---|---|
| Connection overhead | Minimal — both use connection pooling |
| Dependency bloat | connector-x internally uses `r2d2_mysql` (different from `sqlx`'s driver) |
| Version conflicts | Both depend on `mysql_common` — potential version skew |
| Connection count | Doubles the number of connections to MariaDB per run |

### Alternative: sqlx Only

Replace connector-x entirely with sqlx streaming + manual Arrow construction (as discussed in Section 1, Alternative A).

**Verdict:** Not worth it for the metadata savings alone. The dual-library approach is fine — metadata queries are infrequent and lightweight.

### Recommendation

**Keep dual libraries.** The overhead is negligible and each library serves its strength.

---

## 4. State Management: Dual-Layer Design

### Decision in Brief

Two distinct responsibilities, each with its own storage:

| Responsibility | Storage | Role |
|---|---|---|
| Committed data HWM (what's been written to Delta Lake) | **Delta table metadata** (`commitInfo`) | Single source of truth for data committed to the lake |
| Operational state (run metadata, table config, last run status, etc.) | **`state.json`** | Local operational bookkeeping |

### Why This Separation

`state.json` is **not** limited to HWM tracking. It serves broader operational purposes:

- **Last run status:** per-table success/failure/timestamp for operational monitoring
- **Run history:** audit trail of extraction runs (start time, duration, rows extracted)
- **Table metadata cache:** extraction mode (incremental/full), schema version, batch size decisions
- **Configuration snapshot:** which tables were configured at last run, for change detection

HWM for committed data, however, must be **atomic with the Delta commit**. Storing it in a separate file creates an unrecoverable consistency gap:

```
[Delta Lake commit success] → [CRASH] → [state.json NOT updated]
                                                      ↑ stale HWM → duplicate extraction
```

By reading HWM from Delta table metadata, this gap cannot occur — the HWM is only recorded when the commit succeeds.

### HWM Recovery on Startup

```
On startup, for each configured table:
  1. Check if Delta table exists at S3 path
     - If NO → first run, start from beginning (no HWM)
     - If YES → read latest commitInfo from Delta log → extract HWM
  2. Use HWM to build the next incremental WHERE clause
  3. Proceed with extraction loop
```

**Bootstrapping:** First run has no Delta table. The extractor runs from the beginning (no HWM offset). After the first successful batch commits to Delta Lake, the HWM is established in the Delta log.

### Implementation: Writing HWM to Delta commitInfo

When appending a `RecordBatch` via `delta-rs`, include the HWM as custom metadata:

```rust
let batch_hwm = extract_hwm_from_batch(&record_batch);

delta_table.write(
    record_batch,
    SaveMode::Append,
    Some(HashMap::from([
        ("hwm_updated_at", batch_hwm.updated_at.to_string()),
        ("hwm_last_id", batch_hwm.last_id.to_string()),
    ])),
)?;
```

### What Goes in state.json (Not HWM)

```json
{
  "tables": {
    "orders": {
      "last_run_at": "2026-03-28T10:30:00Z",
      "last_run_status": "success",
      "last_run_rows": 45000,
      "last_run_duration_ms": 12340,
      "extraction_mode": "incremental",
      "schema_columns_hash": "abc123"
    }
  }
}
```

This is purely operational/observability data. If `state.json` is lost or corrupted, the extractor recovers fully by reading the Delta table metadata.

### Alternative: state.json as HWM Source (Rejected)

Using `state.json` as the HWM source (as originally described in the project brief) was rejected because:

- **Atomicity gap:** crash between Delta commit and state write causes duplicates
- **Not idempotent:** re-running after a crash produces different results than a clean run
- **Single point of failure:** local file corruption loses the HWM entirely
- **Multi-instance unsafe:** two extractor instances would corrupt each other's state.json

### Recommendation

**Delta table metadata is the single source of truth for committed data HWM. state.json is for operational bookkeeping only.** This eliminates the atomicity gap entirely and makes the system self-healing — any crash or file corruption is recoverable from the Delta log.

---

## 5. Extraction Mode Detection: updated_at + id Heuristic

### Decision in Brief
Auto-detect `Incremental` mode if table has both `updated_at` and `id` columns.

### Edge Cases

| Scenario | Current Behavior | Problem |
|---|---|---|
| Table has `updated_at` but no index on it | Incremental mode | Full table scan on each run — worse than FullRefresh |
| Table has `modified_at` instead of `updated_at` | FullRefresh (missed) | User expected incremental |
| Soft-deleted rows (`deleted_at IS NOT NULL`) | Not captured | Data loss |
| Composite primary key (`id` + `tenant_id`) | Incremental on `id` only | Incorrect pagination if `id` is not globally unique |
| `updated_at` is never actually updated by the application | Incremental mode | Stale data after first run |

### Recommendation

For v1: keep the simple heuristic but add:
- A **config override** in `.env`: allow specifying `TABLE_MODE=incremental` or `TABLE_MODE=full_refresh` per table
- A **warning log** if an incremental table has no index on `updated_at`

For future: consider a `TABLE_CONFIG` section that allows specifying the timestamp column, primary key column, and soft-delete column per table.

---

## 6. Concurrency Model

### Decision in Brief
Tables are processed strictly one at a time (by specification). Within a single table, batches are extracted and loaded sequentially in v1.

### Why One Table at a Time

This is a hard constraint, not a simplification:
- Memory is predictable: only one table's batch is in memory at any point
- State management is simple: HWM update for one table at a time
- Failure isolation: a crash during table N doesn't affect tables 1..N-1 (already committed)
- MariaDB connection pressure is minimal (one active query at a time)

### Intra-Table Batch Concurrency

Within a single table, the extraction loop is:

```
for each batch:
    1. Execute SELECT ... LIMIT {batch_size}   (DB I/O)
    2. Receive RecordBatch via connector-x      (CPU + memory)
    3. Append RecordBatch to Delta Lake on S3   (network I/O)
    4. Update state.json with new HWM           (local I/O)
```

In v1, these steps run **strictly sequentially** per batch, and batches run **strictly sequentially** within a table.

#### Future: Pipelined Batches

Steps 1-2 (extract) and step 3 (write to S3) are I/O-bound on different resources (DB vs S3). They can overlap:

```
Batch N:  [extract] ──────► [write to S3] ──► [update HWM]
Batch N+1:          [extract] ──────► [write to S3] ──► [update HWM]
                    ▲ overlap: extracting next batch while writing current to S3
```

This requires:
- A bounded async channel between the extractor and writer
- Careful memory accounting: two batches in memory simultaneously (`2 × TARGET_MEMORY_MB`)
- HWM must only be updated after the corresponding S3 write succeeds
- Backpressure: stop fetching if the writer falls behind

**Configuration placeholder for future implementation:**
```env
# Pipeline depth (1 = sequential, 2 = overlap extract+write)
# BATCH_PIPELINE_DEPTH=1
```

### Future: Table-Level Parallelism (Not Recommended, Documented for Completeness)

Process multiple tables concurrently using `tokio::spawn` or a thread pool.

**Pros:**
- Faster total extraction time when many tables are configured
- Naturally utilizes I/O wait time (network ↔ DB, network ↔ S3)

**Cons:**
- Memory multiplied by number of concurrent tables (`N × TARGET_MEMORY_MB`)
- Harder to reason about failure and state updates
- MariaDB connection limits may be hit
- Complicates state.json concurrent writes

**Configuration placeholder (do not implement):**
```env
# Maximum number of tables processed concurrently
# MAX_CONCURRENT_TABLES=1
```

### Recommendation

**v1: Strictly sequential — one table, one batch at a time.** No additional configuration needed.

**Near-term enhancement:** Add `BATCH_PIPELINE_DEPTH=1` config (default 1) with pipeline depth 2 as a future option. This doubles memory usage but can significantly improve throughput for tables with many batches.

**Long-term:** Table-level parallelism is out of scope unless a clear need arises (e.g., many small tables where sequential processing is dominated by per-table overhead).

---

## 7. Table List Configuration Format

### Decision in Brief
How to specify which tables to extract in `.env`.

### Alternatives

| Format | Example | Pros | Cons |
|---|---|---|---|
| Comma-separated | `TABLES=orders,customers,products` | Simple, one-liner | Hard to read with many tables |
| JSON array in env | `TABLES=["orders","customers"]` | Machine-friendly, extensible | Awkward in shell, quoting hell |
| External file | `TABLES_FILE=tables.json` | Clean separation, supports complex config | Extra file to manage |
| TOML config file | `parket.toml` with `[tables]` section | Rich per-table config | Adds `toml` dependency, overkill for v1 |

### Recommendation

**Comma-separated for v1**, with per-table overrides using `TABLE_MODE_<name>` convention:

```env
TABLES=orders,customers,products
TABLE_MODE_orders=incremental
TABLE_MODE_customers=full_refresh
```

Table names are unqualified (no schema prefix). The database is determined by `DATABASE_URL`. If cross-schema support is needed later, use `schema.table` format and migrate to a config file.

---

## 8. S3 Path Structure

### Decision in Brief
How Delta tables are organized in S3/MinIO.

### Alternatives

| Structure | Path Pattern | Pros | Cons |
|---|---|---|---|
| One Delta table per MariaDB table | `s3://bucket/parket/{table_name}/` | Clean isolation, independent schema evolution, can query individually | Many Delta tables to manage |
| Single Delta table with partition | `s3://bucket/parket/all_tables/` partitioned by `_source_table` | One table to manage | Schema must be union of all tables (incompatible), bloated |
| Nested by database | `s3://bucket/parket/{db_name}/{table_name}/` | Supports multiple source databases | Overkill for single-db v1 |

### Recommendation

**One Delta table per MariaDB table, flat layout:**

```
s3://{S3_BUCKET}/{S3_PREFIX}/{table_name}/
```

Example:
```
s3://data-lake/parket/orders/_delta_log/
s3://data-lake/parket/customers/_delta_log/
```

`S3_PREFIX` defaults to `parket` and is configurable. This gives each table independent schema, independent HWM in commit metadata, and can be queried independently by downstream consumers (Spark, Trino, etc.).

---

## 9. FullRefresh Mode Semantics

### Decision in Brief
What happens when a table runs in FullRefresh mode.

### Alternatives

| Strategy | Behavior | Pros | Cons |
|---|---|---|---|
| **Overwrite** | `SaveMode::Overwrite` — replace entire Delta table contents each run | Clean, no duplicates | Expensive for large tables, loses history |
| **Truncate + Append** | Delete Delta table, recreate, then append | Simple implementation | Not atomic — partial data visible during run |
| **Append** (naive) | `SaveMode::Append` all rows every run | Simplest code | Duplicates on every run, unbounded growth |
| **Overwrite + snapshot isolation** | Write to temp path, swap atomically | Clean, atomic | Complex, requires rename support on S3 |

### Recommendation

**`SaveMode::Overwrite` — replace entire contents each run.**

FullRefresh implies the table has no reliable change tracking. Overwrite is the only strategy that guarantees correctness without duplicates. The trade-off (re-reading the entire table) is acceptable because:

- FullRefresh tables are expected to be small (otherwise the user should add `updated_at` and switch to Incremental)
- Overwrite is atomic in Delta Lake — readers see either the old or new data, never a partial state
- No HWM tracking needed for FullRefresh tables

Edge case: if the extraction fails mid-run, the old data remains intact (Overwrite only commits on success).

---

## 10. Execution Model

### Decision in Brief
Is Parket a one-shot binary or a long-running daemon?

### Alternatives

| Model | Lifecycle | Scheduling | Complexity |
|---|---|---|---|
| **One-shot binary** | Run once, exit 0 on success | External scheduler (cron, systemd timer, k8s CronJob) | Minimal |
| Long-running daemon with sleep loop | Run forever, poll on interval | Self-scheduled | Must handle signal graceful shutdown, memory leaks over time |
| One-shot with auto-retry | Run once, retry on failure up to N times, then exit | External scheduler | Slightly more complex, but resilient |

### Recommendation

**One-shot binary (run to completion, then exit).**

```bash
# Typical usage via cron:
*/5 * * * * /usr/local/bin/parket
```

Reasons:
- Simpler code — no signal handling loop, no sleep/interval management
- Memory is reclaimed on exit (no long-running leak risk)
- Scheduling flexibility is external — different environments have different schedulers
- Crash recovery is natural — scheduler just re-runs the binary
- Aligns with the "standalone binary" project description

Exit codes:
- `0` — all tables processed successfully
- `1` — partial failure (some tables succeeded, some failed)
- `2` — fatal misconfiguration (bad .env, unreachable DB, etc.)

A table that fails is skipped (logged as error), and the binary continues with the next table. Exit code 1 signals that a retry may be needed.

---

## 11. Column Selection: SELECT * vs Explicit Columns

### Decision in Brief
Whether to use `SELECT *` or an explicit column list in extraction queries.

### Alternatives

| Approach | Behavior | Pros | Cons |
|---|---|---|---|
| `SELECT *` | All columns from table | Simple, no metadata query needed for columns | Breaks if column order changes, may include unwanted columns |
| Explicit columns from `information_schema` | Only known-supported types | Safe, handles schema evolution, can skip unsupported types | Extra metadata query per table per run |

### Recommendation

**Explicit column list from `information_schema.columns`, filtering out unsupported types.**

```sql
SELECT column_name, data_type, column_type
FROM information_schema.columns
WHERE table_schema = ? AND table_name = ?
ORDER BY ordinal_position
```

Then build: `SELECT col_a, col_b, col_c FROM table_name WHERE ...`

Unsupported column types (to be skipped with a warning log):
- `GEOMETRY`, `POINT`, `LINESTRING`, `POLYGON` — spatial types, no Arrow equivalent
- `BLOB` (if large) — may be supported but excluded by default for memory safety

Benefits:
- Detects schema changes between runs (new/dropped columns → log warning, proceed with known columns)
- Avoids selecting columns that connector-x cannot handle (fails gracefully)
- Explicit column list makes the generated SQL self-documenting

---

## 12. Schema Evolution Handling

### Decision in Brief
What happens when MariaDB table schema changes between runs.

### Scenarios

| Change | Risk | v1 Behavior |
|---|---|---|
| Column added | New column not in Delta table schema | **Warn + skip column** (use existing column list from last known schema) |
| Column dropped | Delta table expects the column | **Fail this table** (Delta write will reject — schema mismatch) |
| Column type changed | Arrow type mismatch | **Fail this table** (Delta write will reject) |
| Column renamed | Appears as drop + add | **Fail this table** |
| Table dropped | DB error on query | **Fail this table**, log clearly |

### Recommendation

**v1: Fail-fast on schema mismatches, warn and adapt on additions.**

On startup, for each table:
1. Query `information_schema.columns` for current schema
2. Compare with the Delta table's Arrow schema (read from Delta log)
3. If columns were **added**: log warning, exclude new columns from SELECT, proceed
4. If columns were **dropped, renamed, or type-changed**: log error, skip this table, continue with next table

This is conservative but safe. Full schema evolution support (column addition propagation, type widening) is a future milestone.

---

## 13. Logging & Observability

### Decision in Brief
What and how to log.

### Recommendation

**Structured logging to stdout via `tracing` crate.**

```
2026-03-28T10:30:00Z INFO parket::orchestrator starting run tables=3 target_memory_mb=512
2026-03-28T10:30:01Z INFO parket::discovery table detected mode=incremental table=orders batch_size=45000
2026-03-28T10:30:02Z INFO parket::extractor batch extracted table=orders rows=45000 arrow_bytes=41200000
2026-03-28T10:30:03Z INFO parket::writer batch committed table=orders rows=45000 hwm_updated_at="2026-03-28T09:00:00Z" hwm_last_id=98765
2026-03-28T10:30:04Z WARN parket::discovery table has no index on updated_at table=orders
2026-03-28T10:30:05Z ERROR parket::extractor table failed table=products error="connector-x: type MEDIUMBLOB not supported"
2026-03-28T10:30:06Z INFO parket::orchestrator run complete succeeded=2 failed=1 duration_ms=6200
```

Use `tracing` with `tracing-subscriber` (already common in the Rust/tokio ecosystem). No file logging — stdout is sufficient for container/cron environments. Log level controlled by `RUST_LOG=info` (standard `tracing` convention).

No metrics endpoint in v1. Structured logs are sufficient and can be ingested by any log aggregator.

---

## 14. Signal Handling

### Decision in Brief
How to handle SIGTERM/SIGINT for graceful shutdown.

### Recommendation

**Catch SIGTERM/SIGINT via `tokio::signal`. On signal:**
1. Stop starting new batches
2. Let the current in-flight batch complete (write to Delta + HWM commit)
3. Exit with code `0` if current batch finishes, or `130` (128 + SIGINT) if forced

Rationale: since v1 is one-shot, the run is expected to be finite. Graceful shutdown prevents a half-written Delta commit. SIGKILL is unrecoverable but acceptable — the HWM in Delta metadata ensures the next run resumes correctly.

---

## 15. Complete Configuration Reference

### Recommendation

All configuration via environment variables (loaded from `.env` by `dotenvy`):

```env
# === Required ===

# MariaDB connection string
DATABASE_URL=mysql://user:pass@host:3306/dbname

# S3 / MinIO target
S3_BUCKET=data-lake
S3_ACCESS_KEY_ID=minioadmin
S3_SECRET_ACCESS_KEY=minioadmin

# Tables to extract (comma-separated)
TABLES=orders,customers,products

# Memory budget per batch (in MB)
TARGET_MEMORY_MB=512

# === Optional ===

# S3 endpoint (required for MinIO, omit for AWS)
# S3_ENDPOINT=http://localhost:9000

# S3 region (default: us-east-1)
# S3_REGION=us-east-1

# S3 path prefix (default: parket)
# S3_PREFIX=parket

# Per-table mode override (auto, incremental, full_refresh)
# TABLE_MODE_<name>=incremental
# TABLE_MODE_customers=full_refresh

# Default batch size fallback when AVG_ROW_LENGTH is 0/NULL (default: 10000)
# DEFAULT_BATCH_SIZE=10000

# Log level (default: info, uses RUST_LOG convention)
# RUST_LOG=parket=info
```

No TLS config for MariaDB in v1 (assumes internal network). Add `DATABASE_TLS=true` and `DATABASE_TLS_CA_PATH` later if needed.

No retry/backoff config in v1 — the one-shot binary is simply re-run by the external scheduler on failure.

---

## 16. Idempotency Clarification

The project brief says "Idempotent & Resilient Writes" with `SaveMode::Append`. This needs clarification:

- **Incremental mode:** Is idempotent by design. HWM in Delta metadata ensures each batch is extracted exactly once and appended exactly once. If a run crashes mid-batch, the next run re-extracts from the last committed HWM. The re-extracted rows may overlap slightly, but Delta Lake supports `MERGE INTO` / deduplication queries for downstream cleanup.
- **FullRefresh mode:** Uses `SaveMode::Overwrite`, which is inherently idempotent — each run replaces the entire table.

The brief's use of "Append" for Incremental is correct but the term "idempotent" should be understood as **at-least-once with exactly-once for committed batches** (not exactly-once in the strict sense). True exactly-once would require deduplication on write, which is a future enhancement.

---

## Summary of Recommendations

| # | Decision | Brief Prescribes | Recommended Alternative |
|---|---|---|---|
| 1 | Extraction engine | connector-x | **connector-x streaming API** (`new_record_batch_iter`) |
| 2 | Memory estimation | `AVG_ROW_LENGTH` formula | **Sample-based + adaptive** (measure Arrow bytes from first batch) |
| 3 | Connection libraries | sqlx + connector-x | **Keep as-is** (dual libraries) |
| 4 | State management | `state.json` for HWM | **Delta commit metadata for HWM (source of truth), `state.json` for operational bookkeeping** |
| 5 | Mode detection | `updated_at` + `id` | **Keep + add config override** |
| 6 | Concurrency | Sequential (1 table, 1 batch) | **Sequential for v1, `BATCH_PIPELINE_DEPTH` for future pipelining** |
| 7 | Table list config | "a list of target tables" | **Comma-separated `TABLES=` env var** |
| 8 | S3 path structure | Not specified | **One Delta table per MariaDB table: `s3://bucket/parket/{table}/`** |
| 9 | FullRefresh semantics | Not specified | **`SaveMode::Overwrite` — replace entire contents** |
| 10 | Execution model | Not specified | **One-shot binary, external scheduling** |
| 11 | Column selection | Implied `SELECT *` | **Explicit column list from `information_schema`** |
| 12 | Schema evolution | Not specified | **Fail-fast on drop/type-change, warn + skip on additions** |
| 13 | Logging | Not specified | **Structured logging to stdout via `tracing`** |
| 14 | Signal handling | Not specified | **Graceful SIGTERM: finish current batch, then exit** |
| 15 | Configuration | Partial list in brief | **Complete env var reference (see section 15)** |
| 16 | Idempotency | "Idempotent & Resilient" | **At-least-once (Append + HWM), not strict exactly-once** |
