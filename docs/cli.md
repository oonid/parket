# CLI Reference

Parket is a one-shot binary: it runs to completion and exits. Behaviour is
controlled by a small set of flags plus environment variables (see
[config.md](config.md) for the env reference).

```
parket [--check] [--inspect <TABLE>] [--progress] [--local <dir>] [--version] [--help]
```

All flags are defined in `src/cli.rs`.

## Flags

| Flag | Argument | Default | Effect |
|------|----------|---------|--------|
| _(none)_ | — | — | Run the extract-and-load pipeline |
| `--check` | — | off | Validate config + connectivity and print a per-table summary; extract nothing |
| `--inspect <TABLE>` | table name (required) | unset | Evaluate a single table's columns, indexes, and cursor suitability, then exit |
| `--progress` | — | off | Emit detailed per-batch / per-chunk progress logs |
| `--local <dir>` | path (required) | unset | Write Delta tables to a local directory instead of S3 |
| `--version` | — | — | Print the version from `Cargo.toml` and exit |
| `--help` | — | — | Print usage and exit |

Flags compose freely — e.g. `--local ./out --check --progress` is valid.

## Default mode (no flags)

```bash
parket
```

Runs the full pipeline against S3/MinIO: for each table in `TABLES`, discover
schema → detect mode → ensure Delta table → extract in batches → write. Emits a
[startup banner](#startup-banner), then one concise log line per batch/chunk,
then a `run complete` summary. Exits with a [status code](#exit-codes)
reflecting success / partial / fatal.

## `--check` (pre-flight)

```bash
parket --check
```

Validates without extracting any data:

- connects to the database and verifies every configured table exists,
- checks S3 writability (writes then deletes a tiny test object) — skipped under `--local`,
- prints a per-table mode-detection summary with current High Watermark:

```
TABLE                          MODE            COLUMNS    AVG_ROW_LEN      KEY                        HWM
orders                         incremental     5          128              id, updated_at             2026-01-01T00:00:00.000000 / 1000
events                         two_stream      6          256              two-stream: id + completed_at
customers                      incremental     4          256              override                   2026-01-15T12:30:45.500000 / 5000
attempts                       full_refresh    3          N/A              no updated_at              —
pre-flight check passed
```

The `KEY` column shows which columns drive the extraction mode:
- `override` if a mode override is set in config,
- `id, updated_at` if incremental (both required),
- `two-stream: <insert> + <update>` if two-stream mode is enabled (e.g., `two-stream: id + completed_at`),
- reason if full-refresh: `no id`, `no updated_at`, or `no id/updated_at`.

The `HWM` column shows the current stored High Watermark as `updated_at / last_id`,
or `—` if no HWM exists (new table or FullRefresh mode).

Exits `0` if all tables pass, `2` on any failure. Use it after editing `.env`
or before a first run against a new database.

## `--local <dir>` (local filesystem mode)

```bash
parket --local /data/delta
parket --local ./output --check
```

Writes Delta tables to `<dir>/<table>/` on the local filesystem instead of S3.
When set:

- S3 credentials (`S3_BUCKET`, `S3_ACCESS_KEY_ID`, `S3_SECRET_ACCESS_KEY`) are
  **not required** and ignored if present — `Config::load_local()` runs instead
  of `Config::load()`, so only `DATABASE_URL`, `TABLES`, and `TARGET_MEMORY_MB`
  are validated.
- Pre-flight (`--check`) skips the S3 writability check; DB + table discovery
  still run.
- The orchestrator uses `DeltaWriter::new_local(dir)` (writes via `file://` URLs).

The argument is mandatory: `parket --local` with no path is a parse error.

## `--inspect <TABLE>` (table cursor evaluator)

```bash
parket --inspect orders
parket --inspect events
```

Performs a focused, read-only, database-only evaluation of a single table's
suitability for incremental extraction **before configuring it**. Unlike `--check`
which summarises every table in `TABLES`, `--inspect` dives deep into one table's
structure, even if it is not in `TABLES` yet.

Requirements:
- **Only `DATABASE_URL` is required** — no S3 config, no `TABLES` list, no `TARGET_MEMORY_MB`.
  This makes it easy to evaluate new candidates before adding them to `.env`.
- The table need not exist in `TABLES` (it will be queried even if absent from config).

Output is a human-readable report:

```
Table: orders   (avg_row_length: 107 bytes)

Columns (9):
  NAME              TYPE       NULL   KEY
  id                bigint     NO     PRI
  completed_at      datetime   YES
  created_at        datetime   NO     MUL
  ...

Indexes:
  PRIMARY           unique      (id)
  idx_created_at    non-unique  (created_at)

Cursor evaluation:
  id column:        present  (bigint, PRIMARY)            ✓
  Timestamp candidates (datetime/timestamp columns):
    created_at      NOT NULL, indexed (leading)           → IDEAL
    completed_at    NULLABLE                               → UNSAFE (NULL rows skipped + filesort)

  Recommendation: incremental with TABLE_TIMESTAMP_<table>=created_at
                  (NOT NULL + indexed). Avoid completed_at.
```

Each timestamp/datetime column is scored:

| Nullable | Indexed | Score | Verdict | Notes |
|----------|---------|-------|---------|-------|
| no | leading | 0 | **IDEAL** | Best choice; add as `TABLE_TIMESTAMP_<table>` |
| no | non-leading | 1 | **OK** | Acceptable but index not optimal |
| no | none | 2 | **USABLE BUT SLOW** | Works but triggers ORDER BY filesort; consider indexing |
| yes | any | 3 | **UNSAFE** | NULL rows skipped + filesort; never use as cursor |

The recommendation suggests the best candidate and warns if a configured cursor
(via `TABLE_TIMESTAMP_<table>`) is unsafe while a better option exists.

Exits `0` on success (even if no safe cursor is found — that is information, not
an error). Exits `2` on database connection failure or table-not-found.

## `--progress` (detailed progress logging)

```bash
parket --progress
```

Without it, each batch/chunk emits a single concise `"batch extracted"` line.
With it, the orchestrator emits structured progress logs with timing and
cumulative counts.

Incremental mode — `"batch progress"`:

| Field | Description |
|-------|-------------|
| `table` | Table name |
| `batch_index` | 1-based batch counter |
| `rows` | Rows in this batch |
| `cumulative_rows` | Total rows extracted so far for this table |
| `arrow_bytes` | Arrow batch size in bytes |
| `batch_duration_ms` | Wall-clock time for this batch (ms) |

FullRefresh mode — `"full refresh chunk"`:

| Field | Description |
|-------|-------------|
| `table` | Table name |
| `chunk_index` | 1-based chunk counter |
| `rows` | Rows in this chunk |
| `cumulative_rows` | Total rows written so far for this table |
| `arrow_bytes` | Arrow chunk size in bytes |
| `chunk_duration_ms` | Wall-clock time for this chunk, extract + write (ms) |

`arrow_bytes` is the most useful field for memory tuning — watch it to pick
`TARGET_MEMORY_MB` (see [config.md](config.md) → VM Sizing).

## `--version` and `--help`

`--version` prints the version from `Cargo.toml`. `--help` prints
self-documenting usage with every flag. Both are handled by clap and exit
immediately without touching config or the database.

## Startup banner

On every normal invocation (not `--check`), parket logs an INFO startup banner.
S3 mode:

```
parket v0.1.0 starting  version=0.1.0 tables=3 database_host="mysql://****:****@dbhost:3306" s3_bucket=data-lake
```

Local mode:

```
parket v0.1.0 starting (local mode)  version=0.1.0 tables=3 database_host="mysql://****:****@dbhost:3306" local_dir=/data/delta
```

Sensitive values (database password, S3 secret key) are masked via
`Config::display_safe()`.

## Exit codes

| Code | Condition |
|------|-----------|
| 0 | All tables succeeded (or `--check` passed, or graceful shutdown after a signal) |
| 1 | Partial failure — some tables failed, others succeeded |
| 2 | Fatal — config invalid, database unreachable, or `--check` failed |

The code is decided after all tables are attempted: `failed == 0` → 0;
`succeeded > 0 && failed > 0` → 1; `succeeded == 0 && failed > 0` → 2. A single
failing table is logged and skipped; the run continues to the next table.

## Signals

`SIGINT`/`SIGTERM` triggers graceful shutdown: no new tables start, the
in-flight batch/chunk finishes and commits, then parket exits `0`. A second
signal forces immediate exit (`130`). See [signal-handling.md](signal-handling.md).

## Related

- [config.md](config.md) — environment variables, validation, VM sizing, memory-fit testing
- [logging.md](logging.md) — log format and `RUST_LOG` levels
- [orchestrator.md](orchestrator.md) — what happens per table during a run
- [incremental-extraction-design.md](incremental-extraction-design.md) — HWM, cursor config, two-stream + MERGE-memory design
