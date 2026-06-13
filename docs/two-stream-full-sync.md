# Two-stream full sync (insert-append) + incremental extraction design

> parket two-stream runs **both** streams on every execution, but each phase is dominated by one:
> - **Full sync from zero** (first run / re-bootstrap): the **insert stream appends** every row —
>   this replaced the old *full_refresh-only* path for mutable tables with no clean `updated_at`,
>   and we deliberately do **not** prune/window; the update stream is bootstrap-seeded to near-empty.
>   **→ this document.**
> - **Continue / ongoing updates** (every later run): the insert stream appends only new rows, and
>   the **update stream delete-appends** the mutations (bounded memory; MERGE is the opt-out).
>   **→ [two-stream-continue-update.md](two-stream-continue-update.md).**
>
> This doc also covers the shared extraction machinery the full sync builds on — the HWM model,
> `--check`, the configurable cursor, `--inspect`, and the nullable-cursor guard. (Historical note:
> sections below were written incrementally as a planning log; items marked done are implemented.)

## 1. Background — what the HWM is and where it lives

The High Watermark is the cursor that drives Incremental extraction. It is the
`(updated_at, id)` pair of the last row written to Delta Lake, used to build the
next query's `WHERE` clause so each run resumes exactly where the previous one
stopped.

| Property | Value |
|---|---|
| Type | `Hwm { updated_at: String, last_id: i64 }` (`src/writer.rs`) |
| Storage | Delta Lake `commitInfo` metadata — **not** `state.json` |
| Why there | Atomic with the data commit; a crash cannot desync HWM from data (see [project-brief-alternatives.md](project-brief-alternatives.md) §4) |
| First run | No Delta table → `read_hwm` returns `None` → query has no `WHERE` → full scan from the beginning |

On disk, the latest `_delta_log/*.json` commit carries:

```json
"hwm_updated_at":"2026-06-09T02:55:59.000000"
"hwm_last_id":"4352846"
```

Note: `hwm_last_id` is stored as a **JSON string**, and `hwm_updated_at` uses
connector_arrow's ISO format `YYYY-MM-DDTHH:MM:SS.ffffff` (microseconds, `T`
separator). This is the canonical format for any value compared against it.

### Read path (today)

```
process_incremental (orchestrator.rs)
  └─ current_hwm = writer.read_hwm(table)          // latest commitInfo, or None
       └─ build_incremental_query(..., hwm.updated_at, hwm.last_id, batch_size)
            WHERE (`updated_at` = '<ua>' AND `id` > <id>) OR (`updated_at` > '<ua>')
            ORDER BY `updated_at` ASC, `id` ASC LIMIT <batch_size>
       └─ each batch: extract_hwm_from_batch → advances current_hwm
```

## 2. Detecting table structure (today + gaps)

Structure detection lives in `src/discovery.rs`:

| Function | Source | Purpose |
|---|---|---|
| `discover_columns` | `information_schema.columns` | name, data_type, column_type per column |
| `detect_mode` | the column list | Incremental if a `timestamp`/`datetime` column named `updated_at` **and** a column named `id` both exist; otherwise FullRefresh |
| `get_avg_row_length` | `information_schema.tables` | drives initial batch sizing |

### CLI today: `--check`

`parket --check` (preflight) connects to the DB, verifies each table exists,
checks S3 writability (skipped with `--local`), and prints:

```
TABLE                          MODE            COLUMNS    AVG_ROW_LEN
orders                         incremental     5          128
events                         full_refresh    3          N/A
pre-flight check passed
```

**Gaps:**
- It does not show **which** columns make a table incremental (the `id` /
  `updated_at` pair), so you cannot tell *why* a table fell back to
  `full_refresh` (missing `id`? missing `updated_at`? wrong type?).
- It does not show the current **HWM**, so you cannot see the cursor position.

## 3. Detecting the HWM (today + gaps)

**There is no CLI parameter that shows the HWM today.** The only ways to read
it are:

### 3a. Manual Delta-log inspection (works today, no code)

```bash
TBL=/path/to/out/<table>
latest=$(ls "$TBL"/_delta_log/*.json | sort | tail -1)
grep -o '"hwm_updated_at":"[^"]*"\|"hwm_last_id":"[^"]*"' "$latest"
```

Output:
```
"hwm_updated_at":"2026-06-09T02:55:59.000000"
"hwm_last_id":"4352846"
```

If the grep returns nothing, the latest commit has no HWM (table written by
another tool, or first commit predates HWM support) → next run starts from the
beginning.

### 3b. In code

`DeltaWriter::read_hwm(table)` — used internally by the orchestrator; not
exposed on the CLI.

**Gap:** operators must dig into Delta log JSON to answer "where is the cursor?"

## 4. Proposed: surface structure + HWM on the CLI

### 4a. Extend `--check` output (preferred, low effort)

Add two columns — the incremental key and the current HWM:

```
TABLE                MODE          COLUMNS  AVG_ROW_LEN  KEY               HWM
orders               incremental   5        128          id, updated_at    2026-06-09T02:55:59.000000 / 4352846
events               full_refresh  3        N/A          —                 —
customers            full_refresh  4        96           id (no updated_at) —
```

- `KEY` makes the mode decision legible: shows the detected `id` / `updated_at`,
  or *why* a table is full_refresh (e.g. "id (no updated_at)").
- `HWM` shows `<updated_at> / <last_id>`, or `—` for first-run / full_refresh.

**Implementation note:** preflight (`src/preflight.rs`) currently has a DB
inspector (`PreflightInspect`) and an S3-writability checker
(`PreflightStorage`) but **no Delta reader**. Showing the HWM requires giving
preflight a read capability backed by `DeltaWriter::read_hwm` (a new
`PreflightHwm` trait, or reuse the `DeltaWrite::read_hwm` path). With `--local`
this reads the local Delta log; with S3 it reads the remote log.

### 4b. Optional focused flag (future)

`parket --inspect <table>` for a verbose single-table dump: full column list
with types, detected mode + reason, avg_row_length, computed batch_size, and
current HWM. Useful for debugging one table without scanning all of `TABLES`.

## 5. Proposed: predefine the HWM via config

### 5a. Env var

Follow the existing `TABLE_MODE_<name>` convention (`src/config.rs`
`parse_table_modes`, which builds the exact key per known table — so table
names containing underscores are unambiguous):

```env
TABLE_HWM_<table>=<updated_at>,<last_id>
```

Example:
```env
TABLES=orders,events
TABLE_HWM_orders=2026-01-01T00:00:00.000000,1000000
```

Single value, comma-separated. The timestamp never contains a comma, so split
on the **first** `,`: left = `updated_at`, right = `last_id`.

(Alternative two-var form — `TABLE_HWM_UPDATED_AT_<table>` +
`TABLE_HWM_LAST_ID_<table>` — is more explicit but verbose; the single-var form
is recommended.)

### 5b. Semantics — **seed**, not override

| Option | Behaviour | Verdict |
|---|---|---|
| **Seed** | Apply predefined HWM **only when `read_hwm` returns `None`** | ✅ Recommended — idempotent: bootstraps the first run, then the real stored HWM takes over |
| Override | Force predefined HWM every run | ❌ Re-scans from that point every run; can skip newer data |
| Floor | `max(stored, predefined)` | Niche; defer |

Plug-in site — one line in `process_incremental`:

```rust
let mut current_hwm = self.writer.read_hwm(table_name).await?
    .or_else(|| self.config.table_initial_hwm.get(table_name).cloned());
```

### 5c. Config wiring

- Add `table_initial_hwm: HashMap<String, Hwm>` (or `HashMap<String, (String, i64)>`
  to avoid a `config → writer::Hwm` dependency; convert in the orchestrator) to
  `Config`.
- Parse `TABLE_HWM_<table>` for each known table, mirroring `parse_table_modes`.
- Populate in both `Config::load` and `Config::load_local`.

### 5d. Validation

- `last_id` must parse as `i64`; otherwise fail at startup (exit 2), consistent
  with other config validation.
- A `TABLE_HWM_<table>` on a table that resolves to **FullRefresh** is
  meaningless — reject (or warn and ignore). This needs the mode, which is known
  only after `discover_columns`, so the check belongs in `process_table` /
  preflight, not in `Config::load`.
- Recommend (don't enforce) the ISO `T` microsecond format. A plain
  `2026-01-01 00:00:00` still works in the SQL `WHERE` (MySQL coerces it) and is
  replaced by the canonical form after the first batch.

### 5e. Use cases

- **Skip history on initial load** — start a fresh table from a recent point
  instead of extracting all historical rows.
- **Bootstrap a table backfilled by another tool** — the Delta table exists but
  has no parket HWM; seed it so incremental resumes correctly instead of
  re-scanning.
- **Re-anchor after a manual fix** — combined with deleting/rewriting the table.

## 6. Interaction with known behaviour

- A wrong/low predefined HWM that still resolves to incremental will just
  re-extract from that point — bounded, not a runaway loop. The runaway-disk bug
  (fixed in `extract_hwm_from_batch`: Int32/UInt32/UInt64 ids) was a *non-advancing*
  HWM, a different failure mode (see [config.md](config.md) and the writer fix).
- Predefined HWM only affects Incremental tables. FullRefresh ignores it.

## 7. Implementation checklist

- [ ] `config.rs`: `table_initial_hwm` field + `parse_table_initial_hwm` + tests
      (valid, missing-comma, non-numeric id, absent).
- [ ] `config.rs`: populate in `load` and `load_local`; add to `clear_config_env`
      test helper (the `TABLE_HWM_` prefix, like `TABLE_MODE_`).
- [ ] `orchestrator.rs`: seed `current_hwm` from config when `read_hwm` is `None`;
      tests for seed-applied-when-none and seed-ignored-when-stored-present.
- [ ] `preflight.rs`: add Delta-read capability; extend `--check` table with
      `KEY` and `HWM` columns; tests.
- [ ] Validation: reject `TABLE_HWM_<t>` on a non-incremental table.
- [ ] Docs: update [config.md](config.md) (env reference + `--check` output) and
      [query-patterns.md](query-patterns.md) (seed → first WHERE).

## 8. Decisions (proposed defaults — confirm before implementing)

1. **Env format:** single-var `TABLE_HWM_<table>=<updated_at>,<last_id>`
   (split on first comma). Recommended over the two-var form.
2. **Seed vs override:** **seed** — predefined HWM applies only when `read_hwm`
   returns `None`. Idempotent; stored HWM always wins once it exists.
3. **`TABLE_HWM_<t>` on a non-incremental table:** **reject** — treat it as a
   misconfiguration. Surfaced as an error in `--check`, and fails that table at
   runtime (the mode is only known after column discovery, so this cannot be
   caught at `Config::load` time).
4. **Malformed `TABLE_HWM_<t>` value** (no comma / non-`i64` id): **fail at
   startup** (exit 2), consistent with other config validation.
5. **`--check` HWM display:** read the HWM for every table during `--check`
   (one Delta read per table — acceptable for a one-shot check). No separate flag.

## 9. Implementation steps (each step is a confirmation gate)

Two independent features. **Feature B** (inspect tables via `--check`, §4) is
implemented **first** so the current key columns and HWM can be observed before
deciding what to predefine. **Feature A** (config predefine, §5) follows. Steps
are ordered; stop and verify (`cargo test` + `cargo clippy -- -D warnings`) after
each.

### Feature B — surface KEY + HWM on `--check` (FIRST)

- ✅ **Step 1 · `preflight.rs` Delta-read capability.** Add a `PreflightHwm` trait
  with `read_hwm(table) -> Result<Option<Hwm>>`, plus an adapter backed by
  `DeltaWriter::read_hwm` (S3 via `DeltaWriter::new`, local via
  `DeltaWriter::new_local`). Wire the mode-appropriate adapter in `main.rs`.
- ✅ **Step 2 · `preflight.rs` output.** Extend the `--check` table with two columns:
  `KEY` (the detected `id` / `updated_at`, or the reason a table is full_refresh —
  e.g. "no updated_at", "override") and `HWM` (`<updated_at> / <last_id>`, or `—`
  for first-run / full_refresh). KEY is computed from the already-fetched columns;
  HWM via the Step 1 reader. Update preflight tests (add a mock HWM reader).
- ✅ **Step 3 · docs.** Update `cli.md` `--check` output example; mark Feature B done
  in this doc.

### Feature A — predefine HWM via config (SECOND)

- ✅ **Step 4 · `config.rs` parsing.** Add `table_initial_hwm: HashMap<String, (String, i64)>`
  to `Config` (tuple, not `writer::Hwm`, to keep config independent of writer).
  Add fallible `parse_table_initial_hwm(tables) -> Result<HashMap<…>>` mirroring
  `parse_table_modes`: build `TABLE_HWM_<table>` per known table, split on first
  `,`, parse id as `i64` (error → `bail!`). Call in both `load()` and
  `load_local()`. Add `TABLE_HWM_` to the `clear_config_env` test helper. Unit
  tests: valid, missing-comma, non-numeric id, absent, underscore table name.
- ✅ **Step 5 · `orchestrator.rs` seed.** In `process_incremental`, when `read_hwm`
  returns `None`, seed `current_hwm` from `self.config.table_initial_hwm` (convert
  tuple → `Hwm`); emit an info log noting the seed. Tests: seed-applied-when-none,
  seed-ignored-when-stored-present.
- ✅ **Step 6 · validation (reject).** In `process_table`, after `detect_mode`, if a
  `TABLE_HWM_<t>` entry exists but the resolved mode is not Incremental, return an
  error (fail that table). Also surface it in `--check`/preflight as a table error
  so it is caught before a real run. Tests: orchestrator failure + preflight error.
- ✅ **Step 7 · docs.** `config.md`: add `TABLE_HWM_<table>` to the env reference +
  example. this doc: mark Feature A done.

## 10. Feature C — configurable incremental cursor column

### Problem

The incremental cursor timestamp column is **hardcoded to `updated_at`** in three
places, so a table whose change-tracking column has any other name (e.g.
`completed_at`) cannot run incrementally — `--check` reports `no updated_at →
full_refresh`:

| Site | Hardcoded reference |
|------|---------------------|
| `discovery.rs` `detect_mode` | requires a column named `updated_at` (timestamp/datetime) |
| `query.rs` `build_incremental_query` | `WHERE (\`updated_at\` = … AND \`id\` > …) OR (\`updated_at\` > …) ORDER BY \`updated_at\`, \`id\`` |
| `writer.rs` `extract_hwm_from_batch` | `column_by_name("updated_at")` |

The `id` column is also hardcoded but stays as-is for this feature (making the
id column configurable is a possible future extension; out of scope here).

### Design

New env var — the timestamp/cursor column name per table, defaulting to
`updated_at`:

```env
TABLE_TIMESTAMP_<table>=<column>
```

Example (achieves the requested `orders → incremental,
KEY: id, completed_at`):

```env
TABLE_TIMESTAMP_orders=completed_at
```

Behaviour: when set, `detect_mode` looks for the configured column (which must be
`timestamp`/`datetime`) plus `id`. If both exist, the table becomes Incremental
**automatically** — no `TABLE_MODE_…=incremental` needed. The cursor then orders
and filters on the configured column.

### Decisions (proposed defaults — confirm before implementing)

1. **Default column:** `updated_at` (fully backward compatible — unset behaves
   exactly as today).
2. **Scope:** only the timestamp column is configurable; the id column stays
   `id`. (Configurable id = future extension.)
3. **Auto-enable:** setting `TABLE_TIMESTAMP_<t>` to a valid timestamp/datetime
   column that exists, alongside an `id` column, makes the table Incremental
   without also needing `TABLE_MODE_`. (An explicit `TABLE_MODE_` override still
   wins, as today.)
4. **Invalid configured column** (named column missing, or not
   `timestamp`/`datetime`): **reject** — fail the table at runtime and surface it
   as an error in `--check`. (Consistent with the `TABLE_HWM_` reject decision;
   silent fallback to full_refresh would mask an operator mistake.)
5. **HWM commitInfo keys** stay `hwm_updated_at` / `hwm_last_id` regardless of the
   cursor column name — they are internal metadata keys, not the SQL column;
   keeping them avoids a Delta-log migration. (The stored value is just the
   cursor column's max value.)
6. **`--check` KEY column** shows the resolved cursor column, e.g. `id, completed_at`.

### Interactions

- **`TABLE_MODE_<t>`** — an explicit `incremental` override combined with a
  `TABLE_TIMESTAMP_<t>` that names a valid column works. An `incremental`
  override with NO valid timestamp column hits decision 4 (reject) instead of the
  current silent SQL failure.
- **`TABLE_HWM_<t>`** — the seed's `updated_at` value is interpreted against the
  configured cursor column; no format change.
- **connector_arrow** returns `datetime`/`timestamp` columns as `Utf8`, which
  `extract_hwm_from_batch` already handles for any column name.

### Implementation steps (each step is a confirmation gate; haiku per step, then review)

- **Step C1 · `config.rs`.** Add `table_timestamp_col: HashMap<String, String>`
  (overrides only) + `parse_table_timestamp_col(tables)` mirroring
  `parse_table_modes` (build `TABLE_TIMESTAMP_<table>`, store trimmed non-empty
  value). Add a resolver method `fn timestamp_col(&self, table: &str) -> &str`
  returning the override or `"updated_at"`. Populate in `load()` + `load_local()`.
  Add `TABLE_TIMESTAMP_` to `clear_config_env`. Tests: override present, absent →
  default, underscore table name. Add the field to every `Config { … }` literal
  (orchestrator, preflight, main, tests/integration).
- **Step C2 · `discovery.rs` + callers.** Change `detect_mode(columns,
  override_mode, timestamp_col: &str)` to test `c.name == timestamp_col` for the
  timestamp key. Update both callers (`orchestrator::process_table`,
  `preflight::check_table`) to pass `config.timestamp_col(table)`. Add a shared
  validation helper used by both: if a `TABLE_TIMESTAMP_<t>` override is set but
  the column is missing or not `timestamp`/`datetime`, produce an error (fail the
  table in the orchestrator; report it in `--check`). Update the `--check` KEY
  computation to show the resolved column. Tests: detect_mode with custom column,
  validation error, KEY display.
- **Step C3 · `query.rs` + caller.** Add `timestamp_col: &str` to
  `build_incremental_query`; replace the hardcoded `updated_at` in `WHERE` and
  `ORDER BY` with the backticked column (keep `id`). Update the
  `process_incremental` caller to pass `config.timestamp_col(table)`. Tests:
  query uses the custom column in WHERE + ORDER BY.
- **Step C4 · `writer.rs` + callers.** Add `timestamp_col: &str` to
  `extract_hwm_from_batch`; read `column_by_name(timestamp_col)`. Update the
  `process_incremental` caller (`.and_then(|b| extract_hwm_from_batch(b, ts_col))`).
  Tests: HWM extracted from a custom-named column.
- **Step C5 · docs.** `config.md`: document `TABLE_TIMESTAMP_<table>` (default,
  auto-enable, reject-on-invalid) + example. `cli.md`: note KEY shows the resolved
  cursor column. this doc: mark Feature C done.

### After implementation — the operator's `.env` for the requested table

```env
TABLE_TIMESTAMP_orders=completed_at
```

`parket --check` should then show:
```
orders                         incremental     9          107             id, completed_at   —
```
(`—` HWM until the first incremental run commits one.)

> **Post-implementation caveat (learned 2026-06-09):** `--check`/`validate_timestamp_col`
> only checks the cursor column's *type*, not its *nullability*. A nullable cursor
> (e.g. `completed_at`, NULL until completed) passes `--check` but breaks at run
> time: NULL → empty-string HWM → degenerate `WHERE col > ''` that skips NULL rows
> and triggers an unindexed filesort. This motivated **Feature D** (table evaluator)
> below, which surfaces nullability + index info *before* you commit to a cursor.

## 11. Feature D — `--inspect <table>` table evaluator

### Problem

There is no way to evaluate a single table's suitability for incremental
extraction before configuring it. `--check` summarises every table in `TABLES`
(mode / KEY / HWM) but shows neither **nullability** nor **indexes** nor a
per-candidate **cursor verdict** — so an unsafe cursor like a nullable
`completed_at` is only discovered by running it and watching it misbehave.

`--inspect <table>` is a focused, read-only, DB-only deep-dive on one table:
columns (with type/null/key), indexes, and a cursor-compatibility verdict.

### CLI

```
parket --inspect <TABLE>
```

- Standalone mode (like `--check`): prints the report and exits.
- **Requires only `DATABASE_URL`** — no S3, no `TARGET_MEMORY_MB`, and the table
  need **not** be in `TABLES` (the whole point is to evaluate candidates before
  adding them).
- Queries `information_schema` only; does not touch Delta/S3. (Current stored HWM
  is already visible via `--check`; `--inspect` focuses on the schema/cursor gap.)

### What it queries (`information_schema`)

| Data | Query |
|---|---|
| Columns | `SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, IS_NULLABLE, COLUMN_KEY FROM information_schema.columns WHERE TABLE_SCHEMA=? AND TABLE_NAME=? ORDER BY ORDINAL_POSITION` |
| Indexes | `SELECT INDEX_NAME, NON_UNIQUE, SEQ_IN_INDEX, COLUMN_NAME FROM information_schema.statistics WHERE TABLE_SCHEMA=? AND TABLE_NAME=? ORDER BY INDEX_NAME, SEQ_IN_INDEX` |
| Avg row length | existing `get_avg_row_length` |

`discover_columns` (extraction path) stays unchanged; these are **new, richer**
introspection methods used only by inspect.

### Output (mockup)

```
Table: <table>   (avg_row_length: 107 bytes)

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
  Configured cursor (TABLE_TIMESTAMP_<table>): completed_at → ✗ UNSAFE (nullable)

  Recommendation: incremental with TABLE_TIMESTAMP_<table>=created_at
                  (NOT NULL + indexed). Avoid completed_at.
```

### Cursor-compatibility rules (pure, unit-testable)

A **timestamp candidate** = a column whose `data_type` is `timestamp` or
`datetime`. For each candidate compute `nullable` (`IS_NULLABLE = 'YES'`) and
`indexed` (appears in any index; `leading` if `SEQ_IN_INDEX = 1`). Verdict:

| Nullable | Indexed | Verdict |
|---|---|---|
| no | leading | **IDEAL** |
| no | non-leading | **OK** (range scan may be suboptimal) |
| no | none | **USABLE BUT SLOW** (ORDER BY → filesort) |
| yes | any | **UNSAFE** (NULL → empty HWM, skipped rows, filesort) |

`id`: present? integer type? key? Overall recommendation:
- **id present + ≥1 non-nullable candidate** → recommend incremental with the
  best candidate (prefer IDEAL > OK > SLOW); note if best is only SLOW (no index).
- **id present + only nullable candidates** → recommend `full_refresh` now; note
  that an id-only cursor (append-only tables) is a possible future feature.
- **no id** → `full_refresh`.
- If a `TABLE_TIMESTAMP_<t>` is configured, evaluate it explicitly and flag a
  mismatch (e.g. configured = UNSAFE while a safe alternative exists).

### Decisions (proposed defaults — confirm before implementing)

1. **Config scope:** `--inspect` requires only `DATABASE_URL` (add a minimal
   `Config::load_inspect()` or read the URL directly). Lets you evaluate tables
   not yet in `TABLES`. *(Alternative: reuse full `load()`/`load_local()` and
   require a complete config — simpler code, worse UX.)*
2. **Scope of data:** DB-only (no Delta/HWM read). HWM stays in `--check`.
3. **Output:** human-readable report to stdout (not JSON) for v1.
4. **`indexed`** distinguishes leading (`SEQ_IN_INDEX=1`) from non-leading for the
   IDEAL vs OK verdict.
5. **Exit code:** `0` on a successful report (even if the verdict is "no safe
   cursor" — that is information, not an error); `2` if the table doesn't exist or
   the DB is unreachable.

### Implementation steps (gated; haiku per step or one feature dispatch, then review)

- ✅ **Step D1 · `discovery.rs` introspection.** Add `describe_columns(table) ->
  Vec<ColumnDescribe { name, data_type, column_type, nullable: bool, key: String }>`
  and `discover_indexes(table) -> Vec<IndexInfo { name, unique: bool, columns:
  Vec<String> /* in seq order */ }>` with the queries above. New `sqlx::FromRow`
  row structs. Leave `discover_columns` untouched.
- ✅ **Step D2 · `inspect.rs` (new module) — pure analysis.** `evaluate_cursor(cols:
  &[ColumnDescribe], indexes: &[IndexInfo], configured_ts: Option<&str>) ->
  CursorReport` implementing the rules table. Unit tests: ideal, nullable-unsafe,
  unindexed-slow, no-id, configured-mismatch, no-candidates.
- ✅ **Step D3 · `inspect.rs` — runner + rendering.** A struct that takes a
  `describe`/`indexes` provider (trait for testability, mirroring
  `PreflightInspect`) + table name, runs the queries, and prints the report
  (columns, indexes, evaluation). Mockable trait → unit-test the rendering path.
- ✅ **Step D4 · `cli.rs`.** Add `--inspect <TABLE>` as `Option<String>`. Tests:
  parsed value, mutual behaviour with other flags (define precedence: `--inspect`
  wins and runs its mode).
- ✅ **Step D5 · `config.rs` + `main.rs`.** Add `Config::load_inspect()` (validate
  only `DATABASE_URL`); in `main`, when `--inspect` is set, load_inspect → build
  pool → run the inspector for the one table → exit. Reuse `extract_database_name`.
- ✅ **Step D6 · docs.** `cli.md`: document `--inspect <table>` with the output
  mockup. this doc: mark Feature D done. Optionally cross-link from `config.md`
  (TABLE_TIMESTAMP section → "use `--inspect` to find a safe cursor").

### Related follow-up (not part of D, noted for later)

**Runtime NULL-cursor guard:** independent of inspect, parket should not store an
empty-string HWM for a NULL/empty max timestamp. This is planned as its own small,
standalone **Feature E (§13)** — it hardens any nullable cursor, enables the
optional "completion stream" (§12.2), and is the NULL-handling prerequisite of
Feature F (§14).

## 12. Syncing a mutable table that has no single cursor column

> Case study: `orders`. This chapter explains, in plain
> terms first, how to keep such a table in sync **incrementally** (without a full
> reload and without altering the source schema), then details the two designs.

### 12.1 The problem, in plain language

parket's incremental mode needs **one column that ticks up every time a row
changes** (it uses that column as a bookmark — "give me everything newer than
where I stopped"). Most tables have `updated_at` for this.

This table doesn't. A row has **two moments in its life**, and **no single column
moves on both**:

1. **Born** — a customer places an order. A new row is inserted: `status=0`,
   `completed_at = NULL`. The only thing that reliably identifies it is the
   auto-increment **`id`**.
2. **Completed** — the order is fulfilled. The *same* row is updated: `status=1`,
   `completed_at = <time>`. Now there's a real timestamp, but only on **`id`** did
   not change, and `completed_at` was NULL before this moment.

So:
- **`id`** moves only at birth (good for catching *new* rows, blind to completion).
- **`completed_at`** moves only at completion (good for catching *completions*,
  NULL for everything in progress).

No single bookmark covers both events — which is exactly why `--inspect` says "no
safe cursor; use full_refresh."

There are three honest ways forward. Pick by **what the lake must contain**.

| You need in the lake… | Strategy | Schema change | Complexity |
|---|---|---|---|
| Only **completed** records | **Completion stream** (§12.2) — cursor on `completed_at`, skip NULLs | none | small |
| **Every** row in its current state (in-progress *and* completed) | **Two-stream + upsert** (§12.3) | none (an index helps) | larger |
| Same, but you want zero new logic / accept the cost | **full_refresh** (chunked, ideally on a read replica) | none | none |

Because this dataset is meant for **daily, multi-purpose use including inspecting
in-progress status**, the completion-stream alone is **not** sufficient — it never
emits a row until it's completed. The realistic choices are **two-stream + upsert**
(efficient, more moving parts) or **full_refresh on a replica** (simple, heavier).

### 12.2 Completion stream (only completed rows) — for reference

A single incremental cursor on `completed_at`, made safe for the nullable column
by always filtering NULLs:

```sql
-- first run (no HWM)
SELECT <cols> FROM t WHERE completed_at IS NOT NULL ORDER BY completed_at, id LIMIT n
-- later runs
SELECT <cols> FROM t
WHERE completed_at IS NOT NULL
  AND ((completed_at = '<hwm>' AND id > <last_id>) OR completed_at > '<hwm>')
ORDER BY completed_at, id LIMIT n
```

This is **Feature E** (the NULL-cursor guard): add `AND <ts> IS NOT NULL` to the
incremental query and skip NULLs in `extract_hwm_from_batch`. Combined with the
existing `TABLE_TIMESTAMP_<table>=completed_at`, it yields a correct incremental
stream of completions.

**What it captures:** completed rows, when they complete. **What it misses:**
in-progress rows never appear until completed. → Insufficient for "check
in-progress status," so it is not the chosen design here. It remains the right
tool for any table where only the completed/terminal records matter.

### 12.3 Two-stream + upsert (full current state) — detailed design

**Goal:** the Delta table mirrors the **current state of every row** — in-progress
and completed — kept current incrementally, with no source schema change.

**Idea:** run **two independent incremental cursors** over the same table and
**merge their output by `id`** so the lake holds exactly one, current row per `id`.

```
Stream A  (catches BIRTHS)         Stream B  (catches COMPLETIONS)
cursor = id                        cursor = completed_at (NOT NULL)
WHERE id > hwm_id                  WHERE completed_at IS NOT NULL
ORDER BY id                          AND completed_at > hwm_completed_at
                                   ORDER BY completed_at, id
        \                                   /
         \                                 /
          ▼                               ▼
        MERGE into Delta  ON  delta.id = batch.id
        (insert new ids; update existing ids)
```

- **Stream A — births, cursor `id`.** New inserts arrive with `status=0`. `id` is
  the PK (indexed, non-null, monotonic) → cheap range scan. Each row is seen once,
  at birth, in its in-progress state.
- **Stream B — completions, cursor `completed_at`.** When a row completes,
  `completed_at` is set; Stream B re-emits that row in its completed state. Needs
  the NULL filter from §12.2. (`completed_at` is unindexed today → filesort; an
  index makes it a range scan — see "Performance".)
- **Merge by `id`.** Both streams key on `id`. delta-rs supports `MERGE`
  (datafusion, already enabled): insert when the `id` is new, overwrite when it
  exists. A row therefore moves from `status=0` (via A) to `status=1` (via B) in
  the lake over time, always converging to its current state.

**Two watermarks per table.** Today parket stores one HWM `(updated_at, last_id)`.
This design needs **two**: `hwm_id` (Stream A) and `hwm_completed_at` (Stream B),
stored independently in Delta `commitInfo` (e.g. `hwm_id`, `hwm_completed_at`).

**Worked example** (one row, across daily runs):

| Day | Source row state | Caught by | Lake row after merge |
|---|---|---|---|
| 1 | inserted: id=500, status=0, completed_at=NULL | Stream A (id 500 > hwm_id) | id=500, status=0, completed_at=NULL |
| 2 | (no change) | neither | unchanged |
| 5 | completed: id=500, status=1, completed_at=T5 | Stream B (T5 > hwm_completed_at) | id=500, status=1, completed_at=T5 ✅ |

If birth and completion happen **between the same two runs**, the row shows up in
**both** A (status 0) and B (status 1) that run; the merge must apply **Stream B
last** (or "completed wins") so the lake ends at `status=1`.

**Bootstrap (first run).** Stream A with `id > 0` pulls **all** rows in their
*current* state (already-completed rows come back as `status=1`), so the initial
load is complete and correct. Initialise `hwm_completed_at` to the current
`MAX(completed_at)` so Stream B then only tracks **future** completions.

**Correctness summary:**

| Event | Captured? |
|---|---|
| New in-progress row | ✅ Stream A |
| Row completed (`status 0→1`, `completed_at` set) | ✅ Stream B |
| Both in one window | ✅ both; merge → completed |
| Un-completion (`status 1→0`, `completed_at→NULL`) | ❌ missed (assumed terminal) |
| Hard `DELETE` | ❌ missed (true of all parket modes) |
| Edits to unrelated columns with no `id`/`completed_at` change (e.g. `last_viewed`) | ❌ missed |

These misses are acceptable **iff** completion is terminal and the only states you
care about are in-progress vs completed — which matches this table.

**What parket must gain (new capabilities):**
1. **Multi-cursor config** — declare an insert cursor and an update cursor per
   table, e.g. `TABLE_INSERT_CURSOR_<t>=id` and
   `TABLE_UPDATE_CURSOR_<t>=completed_at` (exact env shape TBD).
2. **Two independent HWMs** per table in `commitInfo`.
3. **Merge/upsert writes** by a key column (`id`) via delta-rs `MERGE`, replacing
   the append-only path for this mode. (parket currently appends/overwrites.)
4. **The §12.2 NULL filter** (Feature E), reused by Stream B.

**Performance.** Stream A is a PK range scan (cheap). Stream B scans/sorts by
`completed_at`; correctness holds unindexed, but a light **`ADD INDEX (completed_at,
id)`** (an index only — *not* a new column, far lighter than adding `updated_at`)
turns it into an efficient range scan. Both streams transfer only new rows since
the last run, so daily cost is proportional to **churn**, not table size — the core
win over full_refresh.

**Trade-off vs full_refresh.** Two-stream is efficient and keeps the lake current
between runs, but it is materially more code (multi-cursor, two HWMs, merge writes)
and silently misses deletes / un-completions. `full_refresh` on a **read replica**
is zero new logic and always fully correct (every row, every column, deletes
included), at the cost of re-reading the whole table each run (~30–60 min for 112M
rows, chunked). For a daily, multi-purpose dataset where simplicity and
completeness matter more than minimal runtime, full_refresh-on-replica is a
legitimate — often preferable — choice; two-stream is the optimization when that
cost becomes the bottleneck.

### 12.4 Recommendation

- **If daily full_refresh on a replica is acceptable** (~30–60 min, all rows, all
  columns, deletes handled) → do that. Zero new features, zero missed-change risk.
  Best first move while the downstream cases are still unknown.
- **If/when that runtime becomes the bottleneck** → build **two-stream + upsert**
  (§12.3) to sync only the churn while still holding every row's current state.
- **Completion stream (§12.2)** only if a future case needs *just* completed
  records — not as the primary, since you expect to inspect in-progress too.

Implementation of two-stream is a larger effort than Features A–D (it changes the
write path to MERGE and the HWM model to multi-cursor); the complete stepwise
breakdown is **Feature F** in §14. Its NULL-handling prerequisite is **Feature E**
(§13).

### 12.5 Why the update cursor is `completed_at`, not `order_status_hash`

The table has an indexed `order_status_hash` column
(`UNHEX(MD5(CONCAT_WS('!', customer_id, product_id, if(status='1',1,0))))`), and
it is tempting to use it to detect completions. But the two-stream **update
cursor** is used in a resumable, ordered query:

```sql
SELECT … WHERE completed_at IS NOT NULL AND completed_at > '<watermark>'
ORDER BY completed_at, id LIMIT n
```

A cursor column must therefore be **monotonic**, **`>`-orderable as "newer than"**,
and **resumable** (a stored watermark you advance). Compare:

| Cursor requirement | `completed_at` | `order_status_hash` |
|---|---|---|
| Monotonic (only increases over time) | ✅ completion time moves forward | ❌ a 16-byte MD5 digest with no time order |
| `>`-comparable as "newer" | ✅ `> X` means "completed after X" | ❌ a larger hash is not a later change |
| Resumable watermark | ✅ store max `completed_at` | ❌ no "max hash" means "where I stopped" |

The hash fails all three: it is a **change-detector**, not a cursor. `WHERE hash >
'<last>' ORDER BY hash` returns arbitrary rows in arbitrary order.

**The hash belongs to a different mechanism** — the hash-diff strategy: pull the
full `(id, hash)` set every run, set-diff it against the previous run, and
re-extract the ids whose hash changed. That is a full key-scan + diff each run
(heavier), not the cheap resumable cursor the two-stream design uses.

**They detect the same event anyway.** For this table the mutation that matters is
completion (`status 0→1`), and that single event both sets `completed_at` *and*
flips the hash. So either could detect it — but `completed_at` does it as a cheap,
ordered, resumable cursor, while the hash would force the expensive set-diff.
`completed_at` is strictly the better choice for the cursor mechanism.

Caveats (true for either): neither captures a mutation that touches neither value
(e.g. a `notes`-only update — not part of the completion lifecycle and not in
the hash). `completed_at`-as-cursor assumes completion timestamps only move forward
(true: completion is terminal). The insert stream (cursor `id`) catches the row's
birth; the update stream (cursor `completed_at`) catches its completion.

## 13. Feature E — nullable-cursor guard (standalone; not the chosen mutable-table path)

We did **not** adopt this as the primary strategy for the mutable-table case —
that is two-stream + MERGE (§14), chosen because we need in-progress rows in the
lake, not only completed ones. Feature E is documented on its own because it is a
small, independent fix that (a) hardens *any* nullable cursor column for plain
Incremental tables, (b) enables the optional "completion stream" (§12.2), and (c)
is the NULL-handling prerequisite of Feature F's update stream.

### 13.1 Problem

When an Incremental cursor column is **nullable**, parket misbehaves. On the first
run (no stored HWM) the query has no `WHERE`, so NULL-cursor rows are included and
sort first; `extract_hwm_from_batch` turns a NULL/empty max timestamp into an empty
string and stores `Some("")`. The next run then builds the degenerate `WHERE <col>
> ''`, which MySQL coerces oddly: it skips all NULL-cursor rows (silent data loss)
and triggers an unindexed filesort. (Once a *real* HWM exists, the existing `> '<value>'`
clause already excludes NULLs — the only hole is the first, unwatermarked query.)

### 13.2 Design

Two small changes:
1. **Query:** always add `AND <ts> IS NOT NULL` to the incremental query (both the
   first-run and with-HWM branches). A no-op for `NOT NULL` columns; correct for
   nullable ones.
2. **HWM extraction:** in `extract_hwm_from_batch`, skip NULL/empty timestamp
   values when computing the max; return `None` ("no usable HWM this batch")
   instead of `Some("")` when none remain.

### 13.3 Effect

- Plain Incremental on a nullable timestamp column no longer corrupts the HWM or
  silently skips rows.
- A cursor on a nullable *completion* timestamp becomes a valid **completion
  stream** (§12.2): only rows whose cursor is set are emitted, in cursor order —
  rows enter the lake when their cursor value appears.

### 13.4 Decisions

1. **Always-on filter:** apply `AND <ts> IS NOT NULL` unconditionally rather than
   gating on detected nullability — simpler and harmless.
2. **No-usable-HWM batch:** with the filter in place, NULL-cursor rows are not
   fetched, so the HWM always advances on a real value; the skip-NULL in extraction
   is belt-and-suspenders.

### 13.5 Implementation steps (gated)

- **Step E1 · `query.rs`.** Add `AND <ts> IS NOT NULL` to `build_incremental_query`
  (first-run and with-HWM branches). Update the exact-SQL tests; add a nullable-cursor
  case.
- **Step E2 · `writer.rs`.** In `extract_hwm_from_batch`, ignore NULL/empty timestamp
  entries when finding the max; return `None` if none remain. Tests: mixed NULL batch
  picks the max non-NULL; all-NULL batch → `None`.
- **Step E3 · docs.** Note in `config.md` that a nullable timestamp cursor is
  supported (rows appear when the cursor is set); mark Feature E done here.

### 13.6 Relationship to Feature F

Feature F's update stream (§14, Step F3) is exactly this NULL filter applied to the
update cursor. Feature E can ship first as a standalone, low-risk improvement;
Feature F then reuses it.

## 14. Feature F — two-stream incremental + MERGE upsert (full current state)

Complete implementation plan for the §12.3 design: keep every row (in-progress +
completed) current in Delta, incrementally, with no source schema change. This is
larger than Features A–D — it adds a new extraction **mode**, a **multi-watermark**
HWM model, and a **MERGE/upsert** write path. Each step below is a confirmation
gate (haiku per step, review after), and each must compile + pass `cargo test` +
`cargo clippy --all-targets -- -D warnings` before the next.

### 14.1 New concepts introduced

- **TwoStream mode** — a third `ExtractionMode` alongside Incremental / FullRefresh.
- **Insert cursor** (`id`) — monotonic PK, catches new rows. **Update cursor**
  (`completed_at`) — timestamp, catches mutations. **Merge key** — the column rows
  are upserted on (= the insert cursor / PK).
- **Two independent watermarks** per table: `hwm_insert` (max id, i64) and
  `hwm_update` (max update-cursor value + id tiebreak), both in Delta `commitInfo`.
- **MERGE upsert** — `when_matched_update_all` + `when_not_matched_insert_all`
  keyed on the merge key (delta-rs `MergeBuilder`, datafusion — already enabled;
  see `reference/delta-rs/crates/core/src/operations/merge/`).

### 14.2 Decisions (confirm before implementing)

1. **Opt-in / config shape:** a table runs in TwoStream mode when **both**
   `TABLE_INSERT_CURSOR_<t>` and `TABLE_UPDATE_CURSOR_<t>` are set. Example:
   ```env
   TABLE_INSERT_CURSOR_orders=id
   TABLE_UPDATE_CURSOR_orders=completed_at
   ```
   Setting only one → config/validation error (ambiguous). Merge key = the insert
   cursor column (must be PK/unique).
2. **Insert cursor type:** must be an integer column (used as a `>`-comparable
   monotonic key, stored as i64) — typically `id`.
3. **Update cursor type:** must be `timestamp`/`datetime`; may be nullable (handled
   by the §12.2 NULL filter).
4. **Merge semantics:** `when_matched_update_all` (existing id → overwrite with the
   newer row) + `when_not_matched_insert_all` (new id → insert). No
   `when_matched_delete` (deletes are out of scope).
5. **Within-run ordering:** apply **Stream A (inserts) first, then Stream B
   (completions)** so a row born-and-completed in the same window ends at its
   completed state. (Each stream is itself MERGEd; B after A.)
6. **Bootstrap:** first run, `hwm_insert = None` → Stream A pulls all rows by `id`
   in current state (full, correct load). To avoid Stream B re-pulling every
   completed row on first run, **seed `hwm_update`** to the max update-cursor value
   observed during the Stream A bootstrap. (If seeding is deferred, Stream B's
   first pass re-MERGEs already-correct rows — wasteful but idempotent; acceptable
   for a v1, note it.)
7. **Deletes / un-completion:** out of scope (documented limitation, per §12.3).

### 14.3 Implementation steps (gated)

> **Status: ✅ all steps F1–F9 implemented (2026-06-10).** Two-stream mode is wired
> end-to-end (config → mode/validation → insert + update stream queries → MERGE
> upsert with dual watermarks → orchestrator `process_two_stream`). Lives in the
> working tree; to be committed once validated on real data. Not yet run against a
> live table.

- **Step F1 · `config.rs` — multi-cursor config.** Add
  `table_insert_cursor: HashMap<String,String>` and
  `table_update_cursor: HashMap<String,String>`; parsers mirroring
  `parse_table_modes`. Add a resolver, e.g. `fn two_stream(&self, table) ->
  Option<(/*insert*/ String, /*update*/ String)>` returning `Some` only when both
  are set. Populate in `load()`/`load_local()`; add both prefixes to
  `clear_config_env`; add the fields to every `Config { … }` literal. Tests:
  both-set → Some, one-set → None (validation of the "only one" error happens at
  mode resolution, F2), absent → None, underscore table names.
- **Step F2 · mode + validation + display.** Add `ExtractionMode::TwoStream` (or
  resolve it from config in `detect_mode`'s callers). A table with both cursors →
  TwoStream; with exactly one → **error** (in `process_table` and `--check`).
  Validate: insert cursor exists and is integer; update cursor exists and is
  timestamp/datetime; both via a discovery helper. Surface in `--check` KEY (e.g.
  `two-stream: id + completed_at`) and in `--inspect`. Tests: mode resolution,
  one-cursor error, type validation, KEY/inspect rendering.
- **Step F3 · apply Feature E (§13) — the NULL-cursor guard — to the update
  stream.** Add `AND <ts> IS NOT NULL` to the incremental/update-stream query; skip
  NULL values in the timestamp HWM extractor (no more `Some("")`). Tests: query
  contains the NULL filter; HWM extraction ignores NULL rows. *(If Feature E ships
  first as a standalone, this step is already done.)*
- **Step F4 · `query.rs` — stream queries.** `build_insert_stream_query(table,
  cols, key_col, hwm_id: Option<i64>, batch_size)` → `WHERE <key> > <hwm> ORDER BY
  <key> ASC LIMIT n` (no WHERE on first run). `build_update_stream_query(table,
  cols, ts_col, hwm_ts, hwm_id, batch_size)` = the incremental query + the F3 NULL
  filter. Tests: exact SQL for both, first-run vs with-HWM, custom column names.
- **Step F5 · `writer.rs` — multi-watermark HWM.** Generalize HWM read/write in
  `commitInfo` to carry both `hwm_insert` (i64) and `hwm_update`
  (timestamp+id). Either extend `Hwm`/the commit metadata to a small named-map, or
  add explicit `hwm_insert_id` / `hwm_update_at` / `hwm_update_id` keys. Provide
  read/write for both, backward-compatible with existing single-HWM tables. Tests:
  round-trip write→read of both watermarks; missing → None.
- **Step F6 · HWM extraction per stream.** Stream A: `extract_max_id(batch,
  key_col) -> Option<i64>` (max of the integer key). Stream B: reuse the
  timestamp+id extractor (`extract_hwm_from_batch` with the update cursor + F3 NULL
  skip). Tests.
- **Step F7 · `writer.rs` — MERGE upsert.** `merge_batch(table, batches,
  key_col)` using delta-rs `DeltaTable`/`DeltaOps` merge:
  `.merge(source, predicate "target.<key> = source.<key>")
  .when_matched_update_all().when_not_matched_insert_all().await`. Verify the exact
  builder against `reference/delta-rs/crates/core/src/operations/merge/`
  (`when_matched_update`, `when_not_matched_insert`; the `_all` convenience may
  need to be expressed column-by-column). Requires the `datafusion` feature
  (already on). Local-filesystem integration tests: insert-then-update converges to
  one row in the updated state; new ids inserted; idempotent re-merge.
- **Step F8 · `orchestrator.rs` — `process_two_stream`.** Dispatch TwoStream mode
  here. Loop Stream A (insert cursor) to exhaustion, MERGE each batch, advance
  `hwm_insert`; then loop Stream B (update cursor) with the NULL filter, MERGE each
  batch, advance `hwm_update`. Implement the F6.6 bootstrap seeding. Honor shutdown
  between batches; per-table error handling as today. Tests with mocks:
  insert-only run, completion run, born-and-completed-same-run → completed wins,
  bootstrap seeds `hwm_update`, shutdown mid-stream.
- **Step F9 · docs.** `config.md`: document `TABLE_INSERT_CURSOR_<t>` /
  `TABLE_UPDATE_CURSOR_<t>`, the TwoStream mode, the merge-key requirement, and the
  optional `ADD INDEX (completed_at, id)` for Stream B speed. `cli.md` /
  `--inspect`: show the two-stream KEY. this doc: mark Feature F done.

### 14.4 Risks / notes

- **F7 (MERGE) is the highest-risk step** — it is parket's first non-append write
  and uses the datafusion merge builder; the `_all` helpers and source-frame
  construction must be verified against the vendored delta-rs. Prototype F7 in
  isolation (local fs) before wiring F8.
- **Merge cost:** MERGE rewrites affected Parquet files; on large churn this is
  heavier than append. Fine for incremental churn; would be poor for a bootstrap
  that MERGEs millions — hence bootstrap uses Stream A's plain write/append for the
  initial full load, switching to MERGE only for ongoing batches. (Decide in F8
  whether bootstrap uses overwrite then incremental-merge, or merge throughout.)
- **Update-cursor index:** Stream B does a filesort without `(completed_at, id)`;
  recommend the light index. Correctness holds without it.
- **Scope:** deletes and un-completion are not captured (documented). If those ever
  matter → binlog CDC (§ alternatives) is the escalation path.

### 14.5 Sequencing

F3 is **Feature E (§13)** — independently useful and low-risk; it can ship first
even if the rest of F is deferred. F1→F2 establish config
+ mode. F5/F6 establish state. **F7 is the gating unknown** — validate it early
(right after F1) as a spike if desired, since the whole design depends on MERGE
working. F8 ties it together; F9 documents.

## 14.6 Feature F refinement — F10: append the insert stream + finer progress

Discovered while first running two-stream on a 112M-row table: the run appeared to
"freeze" even with `--progress`, because (1) `process_two_stream` logs only once per
chunk, *after* both the extract and the write complete — so the slow operations are
silent; and (2) the **insert stream was using MERGE**, whose cost grows with target
size, so bootstrapping a large table is roughly quadratic and effectively never
finishes.

### 14.6.1 Root cause

- **Insert stream should not MERGE.** Insert-stream rows are `id > hwm_id` — strictly
  new ids that cannot already exist in the target. A plain **append** is correct and
  cheap. MERGE is only needed by the **update stream** (which re-touches existing
  rows). Using MERGE for the insert stream (especially the bootstrap, where every row
  is "new") pays the full join-against-target cost per batch for no benefit — the
  quadratic "freeze."
- **No phase-level progress.** The single per-chunk log hides the two long phases
  (DB fetch, write/MERGE), so the operator can't tell what is running.

### 14.6.2 Decisions

1. **Insert stream → `append`**, update stream → `merge` (unchanged). New ids never
   collide, so append cannot create duplicates.
2. The insert-stream append must still carry **both watermarks** on its commit
   (`hwm_insert_id` + the current update HWM), so reading the latest commit always
   recovers both stream positions — same invariant as the merge path.
3. **Progress is logged per phase** (fetch → write), gated on `--progress`, with
   elapsed timings, for both streams.

### 14.6.3 Implementation steps (gated; snapshot before each haiku dispatch)

> **Status: ✅ F10.1 + F10.2 implemented (2026-06-10).** Insert stream now uses
> `append_two_stream` (cheap, linear) instead of MERGE; update stream still MERGEs;
> `process_two_stream` emits per-phase `--progress` logs (fetching → extracted →
> appended/merged, with extract_ms/write_ms). Working tree, uncommitted.

- **Step F10.1 · `src/writer.rs` — `append_two_stream`.** Add a method like
  `append_batch` but using `build_two_stream_commit_properties(insert_id, update_hwm)`
  for the commit metadata:
  ```rust
  pub async fn append_two_stream(&self, table_name: &str, batches: Vec<RecordBatch>,
      insert_id: Option<i64>, update_hwm: Option<&Hwm>) -> Result<()>
  ```
  (Append via `table.write(batches).with_save_mode(SaveMode::Append).with_commit_properties(…)`, mirroring `append_batch`.) Empty-batch no-op. Local-fs test: append rows, assert `read_insert_hwm` round-trips the insert id and rows land.
- **Step F10.2 · `src/orchestrator.rs` — use append for the insert stream + finer
  progress.** Add `append_two_stream` to the `DeltaWrite` trait + both adapters
  (delegating to the writer) + regenerated mock. In `process_two_stream`:
  - Stream A: replace `merge_batch(...)` with `append_two_stream(...)` (same
    watermark args). Stream B keeps `merge_batch`.
  - Wrap each stream's extract and write with `Instant` timing and `--progress`-gated
    phase logs: "fetching chunk" → "extracted N rows in Xms, appending/merging" →
    "appended/merged in Yms (cumulative …)". Keep the concise non-progress log.
  - Tests: insert stream now calls `append_two_stream` (not `merge_batch`); update
    stream still calls `merge_batch`.

### 14.6.4 Operational notes

- Bootstrap is now: insert stream **appends** the full current table once (cheap),
  then the update stream MERGEs only the completed rows (idempotent). Far faster than
  merge-throughout.
- Still recommend `ADD INDEX (completed_at, id)` for the update stream's `ORDER BY`
  filesort.
- Delete any partial output dir from an interrupted merge-bootstrap before re-running.

## 14.7 F10.3 — bootstrap seeding + MERGE hardening (fixes the bootstrap merge crash)

First live run of two-stream on the 112M-row table **failed** during the bootstrap
update-merge with:
```
MERGE matched a target row with multiple source rows that satisfy duplicate
relevant WHEN MATCHED clauses
```
Root analysis: the dir was clean (all commits from that run, 0 removes) and `id` is
a genuine unique PK — so neither stale data nor a non-unique key. The failure was in
the **bootstrap update-merge**, which is **redundant in the first place**: the insert
stream already appended every row in its *current* state (completed rows arrive with
`completed_at` set), so the lake is complete after the insert bootstrap. The update
stream then tried to re-MERGE ~5M already-correct completed rows and hit a MERGE
cardinality violation (a target row matched by multiple source rows). Fix has two
independent parts:

### 14.7.1 Decisions

1. **Seed the update watermark after the insert bootstrap** so the first-run update
   stream merges (near-)nothing — removing both the wasteful re-merge and the crash
   site. Seed = `MAX(<update_col>)` captured **before** the insert stream (a scalar
   query), so any completion that happens during/after the bootstrap (`completed_at >
   seed`) is still caught — correct, not just fast. (Capturing *before* the scan,
   not the max-seen-during, avoids missing a row completed late but read early.)
2. **Harden `merge_batch`** so a MERGE can never hit a cardinality violation:
   (a) **dedup the source by the merge key** (keep one row per key) before merging;
   (b) **do not update the merge key** in `when_matched_update` (update only
   non-key columns — updating the join key is pointless and risky).

### 14.7.2 Implementation steps (gated; snapshot before each haiku dispatch)

> **Status: ✅ F10.3a + F10.3b implemented (2026-06-10).** `merge_batch` now dedups
> the source by key (datafusion `ROW_NUMBER` window) and excludes the key from
> `when_matched_update`; `process_two_stream` seeds the update watermark from
> `SchemaInspector::max_timestamp` (sqlx scalar, avoids extractor adaptive-sizing)
> on first run. CI clippy clean, 378 tests. Working tree, uncommitted.

- **Step F10.3a · `src/writer.rs` — harden `merge_batch`.**
  - Exclude `key_col` from the `when_matched_update` column loop (still insert all
    columns in `when_not_matched_insert`).
  - Dedup the source by `key_col` before the MERGE: register the source batch in the
    datafusion `SessionContext` and select one row per key (e.g.
    `ROW_NUMBER() OVER (PARTITION BY <key> ORDER BY <key>) = 1`), passing the deduped
    `DataFrame` to `.merge(...)`. (For a true PK source this is a no-op; it's
    insurance so a cardinality violation cannot occur.)
  - Test: a source batch containing a duplicate key merges successfully (no
    cardinality error) and the row lands once.
- **Step F10.3b · `src/orchestrator.rs` — bootstrap-seed the update watermark.**
  In `process_two_stream`, before the insert loop: if `self.writer.read_hwm(table)`
  is `None`, run `SELECT MAX(\`<update_col>\`) AS \`<update_col>\` FROM \`<table>\``
  via the extractor, read the single Utf8 value; if present, set
  `update_hwm = Some(Hwm { updated_at: max, last_id: i64::MAX })` so the first-run
  update query (`(ts = seed AND id > MAX) OR ts > seed`) returns only completions
  strictly after the seed. `--progress` log the seed. Test: with `read_hwm` → None
  and a seed value, the update stream's first query carries the seeded watermark
  (and on bootstrap merges nothing).

### 14.7.3 Notes

- After this: re-bootstrap is insert-append (~linear) + an effectively empty update
  pass; subsequent runs merge only genuinely-new completions (small, PK-distinct).
- The `MAX(completed_at)` seed query is itself a full scan without an index on
  `completed_at` — another reason to `ADD INDEX (completed_at, id)`.
- Corrupt output from the crashed run must be deleted before re-running.


## 14.8 — moved to its own document

The two-stream **MERGE memory** analysis (why the MERGE can't be memory-bounded) and the
**DELETE+APPEND** default — i.e. how the *continue-update write* is performed and kept within a
small RAM budget — now live in **[two-stream-continue-update.md](two-stream-continue-update.md)**.
