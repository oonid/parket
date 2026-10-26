# Archive mode — design

Status: **implemented**.

## Context

Parket was built to keep Delta Lake copies of live MariaDB tables in sync. Archive mode serves a
different use: rows that have stopped changing are copied to Delta, the copy is verified, and the
rows are then **deleted from the source** to shrink it. After that purge, Delta is the only copy.

That inverts parket's safety model. A sync can always be repaired by re-reading the source — a
`full_refresh`, a `TABLE_RECONCILE`, a re-seeded watermark. An archive cannot: each of those repairs
replaces the Delta table with the source's *current* rows and drops everything already purged.
Archive mode is therefore a separate mode with its own guarantees, not a variant of sync.

## Goals / Non-goals

**Goals**

- Archive in **units** — for example one campaign, one month, one tenant. A unit is copied and
  verified as a whole, across every table that holds its rows.
- **Never lose an archived row.** No code path may overwrite an archive table or drop a unit that
  has been verified.
- **Prove** each unit: an exact comparison of the source and Delta for that unit, suitable as the
  go-ahead for deleting it from the source.
- **Idempotent and crash-safe.** Re-running after any failure finishes the work without duplicating
  rows; a crash never leaves a unit half-visible.
- **Bounded cost.** One unit is extracted at a time, reading only that unit's rows.

**Non-goals**

- **Deleting from the source.** Parket stays read-only against the source. The application that
  drives the archive hands verified units to whoever holds write access.
- **Choosing units.** Which units are ready (for example "no activity for 90 days") is the calling
  application's rule; parket archives and verifies the units it is given.
- Continuous sync of archive tables. A table is either synced or archived, never both.

## Terms

- **Unit** — the archiving granule, identified by an integer id.
- **Scope** — per archive table, the SQL predicate selecting one unit's rows, with exactly one `?`
  for the unit id: `campaign_id = ?`, or a subquery for a table that only references the unit
  indirectly.
- **Archived** — a table's rows for the unit are committed to Delta.
- **Verified** — the archived copy matched the source exactly, in every archive table.

## Configuration

```env
# A unit is a campaign. `messages` carries the campaign id; `message_events` only references messages.
TABLE_ARCHIVE_messages=campaign_id = ?
TABLE_ARCHIVE_message_events=message_id IN (SELECT id FROM messages WHERE campaign_id = ?)

# Recorded with archived timestamps (see Timestamps); an IANA zone name.
ARCHIVE_SOURCE_TIME_ZONE=Asia/Jakarta
```

## Guard: archive tables cannot be overwritten

An archive table has exactly one mode. At startup parket refuses, for any `TABLE_ARCHIVE_<t>`
table, `TABLE_MODE_<t>` (any value, `auto` included), `TABLE_RECONCILE_<t>`, the two-stream cursors,
`TABLE_HWM_<t>` and `TABLE_TIMESTAMP_<t>` — naming every conflict at once. A sync run, and sync
`--verify`, **skip** an archive table before any discovery or write (logged, not a failure), so it
never falls through to an auto-detected `full_refresh`; `--check` lists it as `archive` with its
scope. The archive commands read their tables from the same `TABLES`.

The archive writer has no overwrite and no whole-table rewrite. Its only removal is the
*unit-scoped replace* below, for a unit not yet verified.

## Archiving a unit

For each archive table:

1. **Skip if already archived** — Delta already holds rows with `_archive_unit = unit`.
2. **Extract** the unit's rows with the scope predicate, in key-ordered pages
   (`WHERE <scope> AND key > ? ORDER BY key LIMIT n`), batch-sized by `TARGET_MEMORY_MB`. The
   paging key is the table's primary key minus the scope column; a table without a usable key is
   refused. On a source partitioned by the unit key, the scope prunes the read to one partition.
3. **Stage** the pages as parquet files and **commit them once**.

Archiving **refuses rather than drops**: a source column of a type parket cannot extract (the sync
path skips such a column with a warning; an archive cannot, since the source copy will be deleted),
a source schema that no longer matches the existing archive table (a column added, dropped or
retyped), and a value that does not fit its archive type (an error, never a silent NULL).

**The unit lives in the data.** Every archived row carries an extra `_archive_unit BIGINT` column,
written by the archive writer itself so rows cannot be filed under the wrong unit. This column, not
the Delta log, is the record of what is archived: delta-rs writes a checkpoint every 100 commits and
by default deletes commit files older than 30 days after it (`delta.enableExpiredLogCleanup`), and
checkpoints keep the table's files but not their `commitInfo`. A unit recorded only in `commitInfo`
would be forgotten and archived twice. The column costs almost nothing in Parquet (one constant per
file) and gives every reader one way to select a unit — `_archive_unit = X` — including tables that
have no unit column of their own.

**One append-only commit per (table, unit).** A unit's files for a table are staged and committed
together: `Add` actions only, never a `Remove`. A crash before the commit leaves only unreferenced
parquet (reclaimable with `--vacuum`) and the unit is retried; a crash after it is recognised from
the data. `commitInfo` also records `archive_unit` and `archive_rows` as an audit trail. A table with
no rows for a unit gets no commit.

**Exactly once.** Each commit carries a Delta transaction marker (`txn`) with application id
`parket-archive:<unit>`. Delta's conflict check rejects a concurrent commit carrying the same id, so
two overlapping runs cannot store a unit twice: the second fails with a conflict instead. Unlike
`commitInfo`, transaction markers are kept in checkpoints.

**Unit-scoped replace** — the only rewrite in archive mode — removes exactly the files whose
`_archive_unit` is the unit and adds re-extracted ones, in one commit. It is allowed only for a unit
that has not been verified. Every archive file holds a single unit (its statistics have
min = max = the unit), so no other unit is touched. It refuses a file that also holds another
unit, an empty new copy, and — unless explicitly allowed — a new copy with **fewer rows** than the
unit's archive: fewer rows is exactly what a partly purged unit looks like, and the replace would
drop the rows whose source copies are already gone. It bumps the unit's transaction marker
version, so it also conflicts with a concurrent archive or replace of the unit.

**Layout and compression.** No Delta partitioning: per-file `_archive_unit` statistics already let a
read skip other units' files. delta-rs keeps statistics for the first 32 columns by default, so
archive tables are created with `delta.dataSkippingNumIndexedCols = -1` (every column), keeping
`_archive_unit` — the last column — covered however wide the table is. Archive files are written with **ZSTD** — an archive is written once
and read rarely, and on campaign email data ZSTD measured about 40 % smaller than the Snappy
default. Synced tables keep the default.

## Timestamps

Archived `TIMESTAMP` columns become Delta **`timestamp`** values — true instants, stored as UTC —
with the source zone kept as metadata, so readers can show them in the source's local time.

- **Extraction reads UTC.** The archive connection sets `time_zone = '+00:00'`, so MariaDB returns
  every `TIMESTAMP` in UTC; no archived value depends on the server's `SYSTEM` zone.
- **The zone is metadata.** Each archived timestamp column carries
  `{"source_time_zone": "<ARCHIVE_SOURCE_TIME_ZONE>"}` as Delta field metadata, and the table the
  same as a property. A Delta `timestamp` cannot hold a zone name itself: delta-rs reads any zone
  annotation as "the value is UTC" and drops the name.
- **Readers convert explicitly** — in DataFusion, `ts AT TIME ZONE '<zone>'` (or an `arrow_cast`
  to a zoned timestamp) renders the instant in that zone with its offset; the
  `datafusion.execution.time_zone` session setting does not change how a stored timestamp
  displays. Spark and DuckDB have their own conversions.
- **`DATETIME` has no zone**, so converting it to an instant would assert something unknown. It is
  archived as `timestamp_ntz`, with the zone the application is assumed to use in its metadata.
- Verification compares in UTC on both sides.

## Verifying a unit

Every row of the unit is compared, source against Delta, by its primary key and by a digest of the
whole row. Both engines render each row canonically and hash it with MD5:

| Column type | Canonical text |
|---|---|
| integers | decimal text |
| text, `json`, `enum`, `set`, `date`, `decimal` | the stored text (Delta keeps these as text) |
| `TIMESTAMP`, `DATETIME` | microseconds since the epoch (the source session is UTC) |
| binary | lowercase hex |
| `float`, `double` | shortest round-trip text, a trailing `.0` dropped |

Every value is length-prefixed and NULL has its own marker, so neither a separator nor a NULL can
be confused with data. The source side is the unit's scope predicate; the Delta side is
`_archive_unit = X`. The result is `Verified`, or a mismatch naming the rows **missing from Delta**,
**in Delta but not in the source**, **changed**, or **duplicated**, by key.

Column aggregates (sums, extremes, lengths) are not enough for this check: an equal-length text edit
or a timestamp moved between the extremes leaves them unchanged, and this check is the go-ahead for
deleting the source rows. Any rendering difference between the engines that remains (for example
exponent notation for very large or very small floats) fails safe — it shows as a mismatch, never
as a false match. The Delta side of a unit is held as a key → digest map while the source streams
against it; a very large unit can be compared in key ranges if that memory becomes a concern.

After the source rows are deleted, the same check shows them as "in Delta but not in the source":
that is the expected state of a purged unit.

## Interface

- **Library** — functions to archive a set of units, verify a set of units, and replace a unit,
  returning per-unit, per-table results. Applications that drive archiving (choosing units,
  scheduling runs, handing verified units over for deletion) build on these.
- **CLI** — `--archive-units`, `--verify-archive-units` and `--replace-archive-units`, each taking
  unit ids, print one line per unit and table; the verify exits 1 when any unit differs. See
  [cli.md](../cli.md#archive-commands) and [config.md](../config.md#per-table-archive-mode-table_archive).

## Implementation steps

Each step ships with tests and passes the gate before the next starts.

1. **Guard** — `TABLE_ARCHIVE_<t>` parsing and the startup refusals. ✅
2. **Per-unit append** — (a) the writer: one append-only commit per `(table, unit)`, `_archive_unit`,
   ZSTD, "already archived" from the data ✅; (b) scoped, key-paged extraction in UTC with typed
   timestamps and zone metadata ✅; (c) the library API and CLI ✅.
3. **Unit verification** — row-by-row digest comparison on both engines, Docker-tested ✅.
4. **Unit-scoped replace** — refuses a file holding another unit, an empty new copy, and a smaller
   one unless allowed ✅.
5. **Operator docs** — configuration and the archive commands ✅.
