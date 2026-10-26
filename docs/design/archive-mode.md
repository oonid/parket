# Archive mode — design

Status: **proposed**; implementation in progress.

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
`TABLE_HWM_<t>` and `TABLE_TIMESTAMP_<t>` — naming every conflict at once. A sync run refuses an
archive table before any discovery or write, so it never falls through to an auto-detected
`full_refresh`; `--check` lists it as `archive` with its scope.

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

**Unit-scoped replace** — the only rewrite in archive mode — removes exactly the files whose
`_archive_unit` is the unit and adds re-extracted ones, in one commit. It is allowed only for a unit
that has not been verified. Every archive file holds a single unit (its statistics have
min = max = the unit), so no other unit is touched.

**Layout and compression.** No Delta partitioning: per-file `_archive_unit` statistics already let a
read skip other units' files. Archive files are written with **ZSTD** — an archive is written once
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

For each archive table, the source scoped to the unit (its scope predicate) is compared with Delta
scoped to the unit (`_archive_unit = X`), leaving `_archive_unit` out of the column comparison:

- **key set** — count, distinct, min, max, xor and sum of the key (verify's existing fingerprint);
  count, distinct and byte-ordered min/max for a string key;
- **values** — verify's parity-hardened per-column aggregates.

Equality must be exact in every table. Unlike sync verification there is no `Drift`: a unit chosen
for archiving is not expected to change, so any difference fails it. Large units reuse verify's
per-key bucketing, so a mismatch names the id ranges at fault. The result — per table row counts,
key fingerprints, and the Delta version holding the unit — is what the caller records as its
go-ahead for deleting the unit from the source; after that deletion, the same check shows zero
source rows while Delta keeps its counts.

## Interface

- **Library** — functions to archive a set of units, verify a set of units, and replace a unit,
  returning per-unit, per-table results. Applications that drive archiving (choosing units,
  scheduling runs, handing verified units over for deletion) build on these.
- **CLI** — `--archive-units <id,…>` and a matching verify, with the same results printed, for
  manual use.

## Implementation steps

Each step ships with tests and passes the gate before the next starts.

1. **Guard** — `TABLE_ARCHIVE_<t>` parsing and the startup refusals. ✅
2. **Per-unit append** — (a) the writer: one append-only commit per `(table, unit)`, `_archive_unit`,
   ZSTD, "already archived" from the data ✅; (b) scoped, key-paged extraction in UTC with typed
   timestamps and zone metadata; (c) the library API and CLI.
3. **Unit verification** — scoped key-set and value comparison on both engines, Docker-parity tested.
4. **Unit-scoped replace.**
5. **Operator docs** — configuration and the archive workflow.
