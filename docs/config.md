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
| `UPDATE_STRATEGY` | `delete_append` | Two-stream **continue-update** write op. Default = bounded **DELETE+APPEND** (fits a 4 GB VM); `=merge` selects the legacy **MERGE** (opt-out). ⚠ **Cost:** the default rewrites the **whole table once per 1024 update keys** — an 858 k-key window on a 115 M-row table = 839 rewrites (≈ 56 h / 2 TB egress on remote S3; ≈ 74 min on local disk). `=merge` is one pass (17.8 min measured **on a 46.5 GB host, peak RSS not sampled**) but its memory scales with the **whole table** (~6.9 GB floor at 112 M rows, and it grows) and **cannot** be bounded — so it is **NOT recommended** and is unvalidated at 8–16 GB. For a large update window use a one-shot `TABLE_RECONCILE_<table>=true` (single pass, bounded memory) regardless of host. See [two-stream-continue-update.md](two-stream-continue-update.md) §2–§3 and audit-findings §10.1. |
| `MERGE_MEMORY_MB` | _(= `TARGET_MEMORY_MB`)_ | **MERGE opt-out only** (`UPDATE_STRATEGY=merge`). Memory budget (MB) for the MERGE's datafusion session. See [Bounding the two-stream MERGE memory](#bounding-the-two-stream-merge-memory). |
| `MERGE_SPILL_DIR` | _(system temp)_ | **MERGE opt-out only.** External-sort disk spill dir. Must be real disk, not tmpfs. |
| `MERGE_SORT_RESERVATION_MB` | _(datafusion default: 10)_ | **MERGE opt-out only.** Advanced; overrides the external sort's merge-phase reservation. **Lower** it (e.g. `1`–`2`) on *"Not enough memory to continue external sort"*. |
| `MERGE_TARGET_PARTITIONS` | `1` | **MERGE opt-out only.** Parallelism of the MERGE's external sort. Keep at **1** — parallel sorters share the one pool and starve the merge. |

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

## Per-Table Two-Stream Mode (mutable tables)

For tables where you want to capture **both** new rows (inserts) **and** later mutations (updates), you can enable two-stream mode by setting both an insert cursor and an update cursor:

```
TABLE_INSERT_CURSOR_<TABLENAME>=<pk_column>
TABLE_UPDATE_CURSOR_<TABLENAME>=<timestamp_column>
```

**Both variables are required.** Setting only one is a startup validation error.

### What it does

Two-stream mode runs two independent extraction streams for the same table:
- **Insert stream:** captures new rows by advancing the insert cursor (integer primary key column), e.g., `id > last_seen_id`
- **Update stream:** captures mutations (rows that changed) by advancing the update cursor (timestamp/datetime column), e.g., `completed_at > last_seen_completed_at`

Both streams are merged into the Delta table using a MERGE (upsert) keyed on the insert cursor. This approach keeps every row's current state in the lake **without** a full refresh and **without** requiring a source `updated_at` column on every table.

### Requirements

- **Insert cursor column** must be an integer PRIMARY KEY (`INT`, `BIGINT`, `UNSIGNED INT`, etc.). No composite keys.
- **Update cursor column** must be a `TIMESTAMP` or `DATETIME` column. Can be nullable — the update stream filters out NULL values and does not reprocess them.
- (Recommended) Create an index on the update cursor or a composite index like `(completed_at, id)` for performance.

### Limitations

- **Hard deletes are not captured** — only inserts and updates. Rows deleted from the source remain in the lake.
- **"Un-completions" are not captured** — if a `completed_at` value is cleared (set back to NULL), that change is not extracted.
- If the update cursor is on a nullable column, NULL rows are initially ignored. Once they transition to a non-NULL value, they are captured by the update stream.

### Example

```env
TABLES=orders,events
TABLE_INSERT_CURSOR_events=id
TABLE_UPDATE_CURSOR_events=completed_at
```

In this config, the `events` table runs two-stream mode: the insert stream fetches new `events` by `id > last_id`, and the update stream fetches rows where `completed_at` has advanced. Both are merged into the Delta table keyed on `id`, ensuring the lake reflects the latest state of every row without a full refresh.

## Per-Table One-Shot Reconcile (`TABLE_RECONCILE`)

Over time a two-stream table accumulates drift the update stream cannot heal — mutations that never advance the update cursor (e.g. a `completed_at` cursor misses edits to already-completed rows, and rows whose `completed_at` is still NULL). To re-align the Delta copy with the source **without** permanently switching the table to full refresh, run a **one-shot reconcile**:

```
TABLE_RECONCILE_<TABLENAME>=true
```

### What it does

For a single run, the flagged table is extracted as a **full snapshot** and atomically overwritten (the same commit protocol as `full_refresh`) — and the overwrite commit is stamped with the two-stream high watermarks so the **next** run resumes as normal two-stream incremental:

- `hwm_insert_id` = the snapshot's max primary-key id (insert stream resumes at `id >` this).
- `hwm_updated_at` = the source's current `MAX(<update_cursor>)`, or `1970-01-01 00:00:00` if the cursor is currently all-NULL. The update stream is an idempotent upsert, so resuming here only re-writes identical rows in a small overlap window.
- `hwm_last_id` = `i64::MAX` (tie-break sentinel).

So a reconcile is a **repair, not a mode**: it heals accumulated drift in one full pass and hands the table straight back to cheap two-stream incremental — **no manual `TABLE_HWM` re-seed required** (this is what distinguishes it from a manual `full_refresh` round-trip).

### One-shot usage

Set the flag for a single run, then remove it:

```env
# normal days: two-stream incremental (cheap)
TABLE_INSERT_CURSOR_events=id
TABLE_UPDATE_CURSOR_events=completed_at

# repair day: add the flag for ONE run, then delete it
TABLE_RECONCILE_events=true
```

The next run (flag removed) is normal two-stream incremental again, resuming from the stamped watermarks.

### Requirements & validation (fail at startup)

- Value must be exactly `true` or `false` (case/whitespace-insensitive); anything else errors.
- The table **must** have two-stream cursor config (`TABLE_INSERT_CURSOR_<t>` **and** `TABLE_UPDATE_CURSOR_<t>`) — reconcile currently supports only two-stream tables.
- `TABLE_MODE_<t>` must **not** be set alongside `TABLE_RECONCILE_<t>` — they are conflicting ways of steering the same table.
- The insert cursor must be an integer primary key (keyset pagination is required to seed `hwm_insert_id`; a table with no integer PK bails at commit time).

### Cost

A reconcile reads the entire table (one full snapshot) — expensive relative to a normal incremental run. Run it **occasionally** (to clear accumulated drift), not every run; between reconciles, two-stream incremental carries the load.

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

## Two-Stream Recovery & Caveats

### Recovery after a crash mid-upsert (PS-L1)

Each two-stream run writes both data and watermark into Delta commits. The update stream's
upsert is a **DELETE commit** (removes the incoming keys) followed by an **APPEND commit** (writes
the new versions, carrying the watermarks). The DELETE commit deliberately carries **no** HWM
metadata, so a crash *between* the DELETE and the APPEND leaves the newest commit watermark-less.

**This now self-heals** and needs no manual action in the normal case: `read_hwm`/`read_insert_hwm`
scan back a bounded window (64 commits) past any watermark-less commit — the aborted DELETE, or an
`OPTIMIZE`/`VACUUM`/checkpoint — to the last commit that actually carries the HWM. The next run
resumes from that watermark, and its update stream re-selects the same window and re-appends the
rows the aborted APPEND missed (the upsert is idempotent).

**Manual recovery is only needed** in the rare case where **no commit within the last 64** carries
an HWM (e.g. many interleaved maintenance/DELETE commits since the last real sync). Symptom: the run
bails with *"has data but no stored watermark"*. To recover, read the last good watermark from the
Delta log and seed it:

```bash
# find the newest commit JSON that still contains the hwm_* keys
s3cmd ls s3://<bucket>/<prefix>/<table>/_delta_log/ | tail -20
s3cmd get s3://<bucket>/<prefix>/<table>/_delta_log/<NNNN>.json - | grep -o '"hwm_[^,]*'
# then seed it (see "Per-Table Initial High Watermark" above) and re-run:
#   TABLE_HWM_<table>=<hwm_updated_at>,<hwm_insert_id>
```

### Inherent timestamp-cursor caveats (PS-L2)

Two limitations are inherent to any timestamp-cursor incremental/two-stream extraction and are
**bounded by a periodic reconcile** (`TABLE_RECONCILE` / full refresh), not fixable by the cursor:

- **Sub-second boundary race** — a row whose update cursor lands in the *same wall-clock second* as
  the stored HWM, committed after that run's window query executed, with a key at/below the tie-break
  id, can be skipped (DATETIME has one-second granularity). Extremely narrow.
- **Backdated cursor values** — an update that sets the cursor to a value **≤ the current HWM** (a
  data fix writing a past `completed_at`) is never re-selected, because the stream only advances
  *forward* past the HWM.

Run a periodic reconcile (see `TABLE_RECONCILE` above) if either matters for a given table.

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

## Bounding the two-stream MERGE memory

> **Applies only to the MERGE opt-out** (`UPDATE_STRATEGY=merge`). The **default** continue-update
> write is **DELETE+APPEND**, bounded by construction (~2 GB, fits a 4 GB VM) and needing none of
> the tuning below. Full rationale: [two-stream-continue-update.md](two-stream-continue-update.md).

The two-stream **update** stream upserts via a Delta `MERGE`, which joins the (small) batch of
changed rows against the **entire target table**. On a large target this dominates parket's
memory — far more than the extract batches. Critically:

> **`TARGET_MEMORY_MB` does NOT bound the MERGE.** It bounds the extract/append/full-refresh
> chunk only — and the two phases don't overlap, so they peak independently. The merge is
> governed by the variables below.

`MERGE_MEMORY_MB` sets datafusion's memory pool for the merge session. **Measured on a 112M-row
target with `MERGE_TARGET_PARTITIONS=1`:** the merge **streams** the join (it does *not* do a
full external sort of the target, and barely spills). So `MERGE_MEMORY_MB` behaves as an
**in-memory buffering cap, not a disk-spill trigger**: a *smaller* pool keeps less in flight
(lower RSS); a *larger* pool over-buffers and the excess goes to **OS swap**. There is a
**working-set floor** (~6–7 GB for 112M rows) that the pool cannot shrink below.

> ⚠️ **Size the VM to peak RSS, not to the pool.** Earlier guidance to "give the merge most of
> RAM / 50–60 % of spare RAM" was **wrong** — a bigger pool means *more* RAM+swap, not less. Use
> a *moderate* pool that fits RAM without over-buffering.

**Proven config (8 GB VM, 112M-row table):**
```env
TARGET_MEMORY_MB=512
MERGE_MEMORY_MB=2048
MERGE_TARGET_PARTITIONS=1
MERGE_SPILL_DIR=/var/tmp/parket-spill   # optional; defaults to system temp; must be real disk
```
→ **peak RSS ~6.9 GB, no swap, no spill, completes.** (`MERGE_MEMORY_MB=4096` on the same table
over-buffered to ~7.4 GB RSS **+ 4 GB swap**; `=1536`/multi-partition starved and failed.)

- **`MERGE_MEMORY_MB`** — defaults to `TARGET_MEMORY_MB`. Tune it **down** to lower RSS (until the
  working-set floor); raise it only if the merge errors with *"not enough memory"*. It is **not**
  a "use most of RAM" budget.
- **Pool ≠ RSS, and the gap is large.** Peak RSS is the streaming working set (~6–7 GB for 112M
  rows), largely *independent* of the pool — always confirm with the acceptance test below.
- **`MERGE_SPILL_DIR`** — used only if/when the sort actually spills. **Must be real disk, not
  `tmpfs`** (RAM-backed spill defeats the point). Defaults to system temp.
- **`MERGE_TARGET_PARTITIONS`** — sort parallelism; defaults to **1**. See below.

> **Sub-floor sizing (e.g. a 4 GB VM) is not reachable for a table this large** with full-data
> semantics — the ~7 GB working set is intrinsic to joining the whole target and is not
> pool/spill-shrinkable. The only way below it is to avoid materializing the whole target
> (id-windowed merge with column stats — a future enhancement).

### Why `MERGE_TARGET_PARTITIONS` defaults to 1, not the CPU count

datafusion normally sets `target_partitions` to the number of CPUs and runs **one external
sorter per partition**. But its memory pool is **global — shared across all partitions, not
split per-partition**. So with N sorters, all N draw from the same bounded `MERGE_MEMORY_MB`
pool simultaneously: their sort buffers *and* each one's merge-phase reservation compete for
the same bytes. The pool fragments, and eventually one sorter's merge phase can't reserve its
slice and aborts with *"Failed to allocate … for ExternalSorterMerge[N]"*. We hit exactly this
— **a 4 GB pool with ~14 partitions starved**, while a single partition over a smaller pool
completes.

So the CPU count is **not** optimal here. It optimizes sort *throughput* (parallelism) at the
expense of *memory* — the opposite of what a bounded merge needs. `MERGE_TARGET_PARTITIONS` is a
**memory ↔ speed trade**, not a "match your cores" setting:

- **Memory-constrained VM → keep it at `1`.** One sorter owns the whole pool: fewer, larger
  spill runs, and the merge reliably fits. This is why it's the default (F10.4's goal is
  *fitting* a small VM, not maximizing merge speed).
- **Ample-RAM host → raise it** for a faster sort, but budget RAM proportionally: each added
  partition adds roughly another sorter's worth of pool pressure, so scale `MERGE_MEMORY_MB`
  (and total RAM) with the partition count, not just the CPU count.

> **File descriptors:** the disk-spill opens many files at once, so a low soft `NOFILE`
> limit (e.g. systemd's default 1024) makes the merge fail with *"Too many open files"*.
> parket **raises its own soft `RLIMIT_NOFILE` to the hard limit at startup**, so no
> `ulimit`/`LimitNOFILE` tuning is normally needed. If you still hit it, raise the *hard*
> limit (`LimitNOFILE=` in the systemd unit, or `ulimit -Hn`).

> **"Not enough memory to continue external sort":** the merge's external sort needs a
> reserved slice of the pool for its merge phase (`sort_spill_reservation_bytes`, default
> 10 MB). If the pool is nearly full when the merge starts, that reservation can't be
> satisfied. The biggest cause is **fan-out**: by default datafusion runs one sorter per CPU,
> all sharing the single pool. parket pins the merge to **`MERGE_TARGET_PARTITIONS=1`** so one
> sorter owns the whole pool — keep it at 1 unless you have RAM to spare. If you still hit the
> error, **raise `MERGE_MEMORY_MB`** or **lower `MERGE_SORT_RESERVATION_MB`** (e.g. `1`–`2`).
> (This error path is mostly seen with multi-partition fan-out; at `MERGE_TARGET_PARTITIONS=1`
> the merge streams instead — see the working-set note above for RSS sizing.)

**Acceptance test — does it fit without swap?** (cgroup v2 enforces the cap honestly):

```bash
cargo build --release
systemd-run --user --scope -p MemoryMax=3500M -p MemorySwapMax=0 \
  ./target/release/parket --local /path/to/out --progress
```

Completes → it fits at that `MERGE_MEMORY_MB`. Kernel OOM-kills it → lower `MERGE_MEMORY_MB`
(more spill, slower) and retry; confirm kills with `journalctl --user -e | grep -i oom`.

## Validation Rules

Parket validates configuration at startup and exits with code 2 on any failure:

- `DATABASE_URL` must start with `mysql://` — other schemes (e.g. `postgres://`) are rejected.
- `TABLES` must not be empty or whitespace-only.
- `TARGET_MEMORY_MB` must parse as a positive integer (> 0).
- `MERGE_MEMORY_MB`, if set, must parse as a positive integer (> 0); defaults to `TARGET_MEMORY_MB`.
- All required variables must be present and non-empty.

Error messages identify the specific missing or invalid variable.

## CLI Usage

Command-line flags (`--check`, `--inspect`, `--verify`, `--progress`, `--local`,
`--version`, `--help`), the startup banner, and exit codes are documented in the
dedicated [CLI Reference](cli.md).

Reconciliation (`--verify` / `--verify-after`) adds no environment variables: it
reuses `DATABASE_URL`, `TABLES`, and the same S3 (or `--local`) target as a normal
run. The 1,000,000-row strict-check cap is a built-in constant, overridden per-run
with `--verify-deep` rather than via config.

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
