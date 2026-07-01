# `--verify` — source ↔ Delta reconciliation (design / proposal)

> **Status: PROPOSAL (not implemented).** Read-only mode that checks each synced
> Delta table against its source DB and reports per-table pass / discrepancy.
> Companion to [two-stream-full-sync.md](two-stream-full-sync.md)
> (which defines the extraction modes and the HWM model this doc relies on).

All examples use a generic `orders` table: integer PK `id`, a nullable completion
timestamp `completed_at`, and ordinary columns (`customer_id`, `amount`, `status`, …).

---

## 1. Goal & where it fits

After a sync, we want confidence that the Delta output faithfully reflects the source —
not just "the run exited 0". `--verify` is a **separate, read-only mode** (like `--inspect`/
`--check`) that, for each configured table with a Delta output, compares it against the
source DB across layered checks and exits `0` (clean) / `1` (discrepancies) / `2` (couldn't
run), with a per-table report.

It reuses existing machinery:
- **source side** — `sqlx` (as in `SchemaInspector`) for schema + aggregate + sample queries;
- **Delta side** — `DeltaWriter::open_table` + datafusion `table_provider` (as in `merge_batch`
  and the integration tests) to run the *same* queries over the Delta table;
- **HWM** — the stored high-water-mark read from Delta `commitInfo` (as in `writer::read_hwm`/
  `read_insert_hwm`).

**It is memory-light** — every check is an aggregate or a small sample computed engine-side,
so no full dataset is shipped. Unlike the MERGE, `--verify` runs comfortably on a small VM.

---

## 2. Verification layers (depth ↔ cost ↔ precision)

| # | Layer | Cost | Catches what cheaper layers miss |
|---|---|---|---|
| 0 | **Schema** — Delta columns + types vs source (via the parket type mapping) | instant | drift, dropped/extra columns, type-mapping errors |
| 1 | **Row count** (mode-scoped — §4) | 1 scan/side | gross row loss / duplication |
| 2 | **Key-set** — PK `count`, `count(distinct)`, `min`, `max`, order-independent id fingerprint | 1 scan/side | missing **and** duplicate keys that cancel out in a count |
| 3 | **Per-column aggregates** — `SUM`/`MIN`/`MAX`/null-count/distinct per column | 1 scan/side | value drift in numeric / date columns (pinpoints the column) |
| 4 | **Row-hash reconciliation** — order-independent aggregate of a per-row canonical hash | 1 scan/side | **any** value difference, anywhere — no data shipped |
| 5 | **Sampling** — N selected rows, full field-by-field compare | N rows | human-readable diffs / spot confidence |

**Default depth = 0–3** (cheap, full-table but memory-light). **Opt-in:** `--verify-deep` (4),
`--verify-sample N` (5). Layers 2 and 4 are the precise additions beyond a naive
columns/rows/sampling check — see §5.

---

## 2.5 Difficulty & recommended scope

Most of `--verify` reuses what already exists (sqlx on the source, datafusion-over-Delta via
`table_provider`, the `inspect.rs` shape), so it's **Low–Medium** to build. Exactly one
sub-problem is **High** — and it turns out to be avoidable.

| Piece | Difficulty | Note |
|---|---|---|
| Skeleton (`verify.rs`, `SourceProbe`/`DeltaProbe`, CLI, exit codes) | **Low** | mirrors `inspect.rs`; both engines already proven |
| L0 Schema | **Low–Med** | compare via parket's type mapping (datetime↔Utf8, int variants), not raw equality |
| L1 Row count | **Low** raw / **Med** scoped | the work is the per-mode contract (§4) + HWM scope, not the `COUNT` |
| L2 Key-set fingerprint | **Med** | `BIT_XOR(id)` exists in both engines; the id-type casting must match (Int32/UInt32/UInt64 — we hit this) |
| L3 Per-column aggregates | **Med** | numeric easy; datetime compared **semantically** (parse the Delta `Utf8` back to a timestamp), floats with tolerance |
| L5 Sampling | **Med → Low-Med** | with the single-engine trick below |
| Per-mode contracts + HWM scope + lag | **Med** | settled design; moderate code |
| **L4 Row-hash reconciliation** | **High** | the only hard piece — see below |

**The one hard piece (L4) is the cross-engine canonical render.** A per-row hash must be
byte-identical when computed in MariaDB SQL *and* in datafusion-over-the-Delta-`Utf8`. Real
evidence it's nasty: parket's own datetime rendering isn't even stable (the HWM logs showed both
`2026-… 20:18:35` and `2026-…T16:50:48.000000`); floats can't compare exactly; NULL needs a
shared sentinel; and the hash fn must exist+match on both (CRC32 isn't in datafusion — `md5()`
is, but the hex→number reduction must also match). So L4 is **spike-gated and may only work for
ints/strings**.

**Two things make that High piece avoidable:**
1. **Semantic compare for L3/L5** — don't string-match across engines; **parse** the Delta `Utf8`
   value back to a typed value (timestamp, number) and compare *values*. Sidesteps the
   format-string-match entirely for aggregates and samples.
2. **Single-engine sampling for L5** — fetch the sampled source rows via sqlx, build an Arrow
   batch, register it in datafusion *next to* the Delta table, and diff there (join on PK). Both
   sides are rendered by the **same** engine → no cross-engine render at all, exact field diffs.

The cross-engine *string* render is therefore needed **only** for L4 (where you can't parse a
hash back). So:

**Recommended scope:** **L0 → L1 → L2 → L3 → single-engine L5** — all **Low–Med**, high value,
~5–6 gated steps, **no canonical-render spike required**. **Defer/drop L4** (High, spike-gated) —
it adds only "provably byte-identical whole-table" certification, which counts + key-set +
aggregates + sampling already approximate well.

**PII note (matters for this data):** sampling reads real values, so diffs can surface PII (e.g.
phone/email columns). Keep `--verify` **aggregate-by-default**, make sampling opt-in, and
consider redaction — consistent with the project's PII rules.

---

## 3. Layer detail (with query sketches)

### 3.0 Schema
- Source: `SchemaInspector::discover_columns(table)` → `(name, type, nullable, key)`.
- Delta: `open_table(table)` → `table.get_schema()` (Arrow/Delta fields).
- Compare the **configured** column set (parket may project a subset) — every configured
  column present in Delta, with a **compatible** type per the parket write mapping (e.g. a
  source `datetime` is expected as Delta `Utf8`, *not* a timestamp — see §6). Report missing /
  extra / type-map mismatches.

### 3.1 Row count (mode-scoped — see §4 for why)
- Delta: `SELECT COUNT(*) FROM t` (and `COUNT(DISTINCT id)` for the append-log case).
- Source: count scoped to the HWM (or whole table for current-state modes).

### 3.2 Key-set fingerprint
One pass each side:
```sql
SELECT COUNT(*), COUNT(DISTINCT id), MIN(id), MAX(id), BIT_XOR(id)
FROM t   -- + WHERE <hwm scope>
```
- `COUNT` vs `COUNT(DISTINCT)` mismatch ⇒ duplicate keys.
- `BIT_XOR(id)` (+ `SUM(id)`, `MIN`, `MAX`, `COUNT`) is an order-independent fingerprint of the
  id-set: if all match across sides, the key sets are identical with very high probability
  (a single missing/extra/changed id breaks the XOR). Combine XOR **and** SUM **and** count to
  shrink collision odds.

### 3.3 Per-column aggregates
Per column, type-appropriate aggregates both sides → compare → the diverging column is named:
- numeric: `SUM`, `MIN`, `MAX`, `COUNT(col)` (non-null).
- datetime: `MIN`, `MAX`, `COUNT(col)` — compare **semantically** (parse the Delta `Utf8` back to a timestamp), not by string-matching the render (§2.5).
- string: `COUNT(col)`, `BIT_XOR(CRC32(col))`, optionally `SUM(CHAR_LENGTH(col))`.
- low-cardinality (status/bool/enum): per-value `COUNT` (a cardinality profile).

### 3.4 Row-hash reconciliation (DEFERRED — the only High-difficulty layer; see §2.5)
> **Deferred / optional.** Cross-engine canonical-render + hash parity (§6) is the hard part.
> Single-engine sampling (§3.5) plus L0–L3 give most of the value without it. Build only if you
> need provably byte-identical whole-table certification.

Per-row canonical hash, aggregated order-independently:
```sql
-- both engines, same expression shape:
SELECT BIT_XOR(CRC32(CONCAT_WS(0x1f,
         render(id), render(customer_id), render(amount),
         render(status), render(completed_at))))         AS xor_hash,
       SUM(CRC32(CONCAT_WS(0x1f, ...)))                   AS sum_hash,
       COUNT(*)                                           AS n
FROM t   -- + WHERE <hwm/mode scope>
```
Equal `(xor_hash, sum_hash, n)` ⇒ the row sets are identical **on the canonical form** with
high probability. Strengthen against CRC32 collisions by using a wider digest (e.g. two 64-bit
halves of `MD5` XORed and summed separately). The whole thing hinges on `render()` matching on
both engines — §6.

### 3.5 Sampling
- Pick sample ids cheaply (avoid `ORDER BY RAND()` on a huge table): `min`/`max` id, a handful
  of id-range midpoints / percentiles, and the most-recent rows by cursor.
- **Recommended technique — single-engine compare (§2.5):** fetch the sampled source rows via
  sqlx, build an Arrow `RecordBatch`, register it in datafusion *alongside* the Delta table, and
  diff there (join on PK, compare columns). Both sides are rendered by the **same** engine, so the
  cross-engine canonical-render problem (§6) disappears and you get exact field-level diffs —
  easier and more reliable than rendering+comparing across MariaDB and datafusion separately.
- Report readable per-field diffs. (Reads real values → see the PII note in §2.5.)

---

## 4. Verification contract per extraction mode (critical)

Strict `source == Delta` is **wrong** for most modes — each has a different contract:

| Mode | Delta shape | What "correct" means | Comparison |
|---|---|---|---|
| **full_refresh** | snapshot at sync time T1 | matches source *as of T1* | no HWM to scope by; source moved on → counts/aggregates **within tolerance**, schema exact, PK-keyed sampling; report drift as *lag*, not failure |
| **incremental** | **append-log** (versions accumulate; no dedupe) | every change `≤ HWM` was appended | **dedupe Delta by id (latest `updated_at`)**, compare that to source current-state for ids `≤ HWM`; key-set exact, raw row count is **not** (source no longer has old versions) |
| **two-stream** | current-state mirror via MERGE (no dupes) | every source row's current state is reflected | **`source ⊆ Delta`** by PK+value (Delta may legitimately hold extra rows: hard-deletes / un-completions aren't captured); value checks scoped to `completed_at ≤` update HWM |

Cross-cutting rules:
- **Scope every check to the stored HWM** where one exists, so you compare only what the sync
  claims to cover. The incremental scope predicate:
  `WHERE updated_at < :hwm_ts OR (updated_at = :hwm_ts AND id <= :hwm_id)`.
- **Report freshness lag** separately: `max(cursor)` source vs Delta. Lag is information, not a
  failure (it just means the source advanced after the sync).
- **two-stream asymmetry:** a PK in Delta but not in source = suspected source-side delete →
  report as informational, not error. A PK in source but not in Delta = real miss → error.

---

## 5. Why layers 2 & 4 matter (precision)
- A **row count** can match while rows are silently wrong (one row dropped, one duplicated).
- A **key-set fingerprint** (2) catches missing/duplicate *keys* a count can't.
- A **row-hash** (4) catches any *value* corruption across the whole covered slice — which
  sampling (5) only finds probabilistically. Together they give "the row sets are byte-identical"
  confidence without shipping the data.

---

## 6. The linchpin: canonical render (type normalization)

parket writes via connector_arrow's type mapping, so Delta values are **renderings** of source
values, not the raw source types (notably **`datetime`/`timestamp` → Utf8 string**; also
unsigned ints, decimals, floats). A naive value/hash compare therefore **falsely mismatches**.

Layers 3–5 must compute on a **canonical form identical on both engines**. Define one
`render(col)` spec, derived from parket's actual write path, and use it to build *both* the
MariaDB SQL and the datafusion SQL so they cannot drift:

| Source type | Delta representation | Canonical render (must match on both sides) |
|---|---|---|
| `datetime`/`timestamp` | Utf8 string | one exact format string (match connector_arrow's output, e.g. `%Y-%m-%dT%H:%i:%s.%f`); confirm by inspection |
| `int`/`bigint` | int | decimal string, no padding |
| `unsigned` | (un)signed int | value as decimal string |
| `decimal(p,s)` | decimal | fixed-scale string at the column's scale |
| `float`/`double` | float | **rounded** to a fixed precision both sides (exact float equality is unsafe) — or exclude from the hash and check via tolerance aggregate |
| `varchar`/`text` | Utf8 | as-is (watch trailing spaces, collation, charset) |
| `tinyint(1)`/bool | int/bool | `0`/`1` |
| `NULL` (any) | null | a fixed sentinel (e.g. `0x00`), never the empty string |

**Spike this first (§8 V0):** confirm `(xor_hash, sum_hash, n)` matches on a *known-good* table
before building layers 3–4 — datetime format and float rounding are the usual culprits. Source
of truth for the mapping is the parket write path (connector_arrow ↔ Arrow ↔ Delta) and
`writer.rs`.

---

## 7. Architecture & CLI

**Module:** new `src/verify.rs`, mirroring `inspect.rs`: a `VerifyCommand` plus narrow traits
(`SourceProbe` over sqlx, `DeltaProbe` over datafusion) so it's unit-testable with mocks; a
local-Delta + testcontainers integration test like `tests/integration.rs`.

**Inputs:** `TABLES`, per-table mode + cursors (from `Config`), stored HWM (from Delta
`commitInfo`). For `--verify` only `DATABASE_URL` + the Delta location are strictly needed (S3
or `--local`), like `--check`.

**CLI flags (proposed):**
- `parket --verify` — all configured tables, depth 0–3.
- `parket --verify <table>` — single table (mirrors `--inspect`).
- `parket --verify --verify-sample N` — add N-row spot checks (layer 5).
- `parket --verify --verify-deep` — add row-hash reconciliation (layer 4; full scan both sides).
- `parket --verify-after` — chain verification onto a normal sync, verifying each table that
  just succeeded.

**Output:** one block per table — schema OK/diff, counts (source/Delta, scoped), key-set match,
per-column aggregate diffs, optional hash match, optional sample diffs, freshness lag — then a
summary. **Exit:** `0` all clean / `1` any discrepancy / `2` couldn't run (DB/Delta unreachable).

**Composes with `--local`** (verify a local Delta dir) and S3.

---

## 8. Implementation steps (gated; each is a confirmation point)

Recommended path (no canonical-render spike needed — see §2.5):
- **V1 · skeleton + schema + count + key-set (layers 0–2)** for current-state modes
  (full_refresh / two-stream); `--verify [<table>]`, report, exit codes.
- **V2 · mode contracts:** incremental dedupe-by-id, HWM scoping predicate, two-stream
  `source ⊆ Delta` asymmetry, freshness-lag reporting.
- **V3 · per-column aggregates (layer 3)** — datetime compared semantically (parse Delta `Utf8`).
- **V5 · sampling (layer 5)** — `--verify-sample N`; use the **single-engine compare** (fetch
  source sample → Arrow → datafusion → diff), which needs no cross-engine render.

Deferred (only if byte-identical whole-table certification is required):
- **V0 · spike: canonical render** — confirm `(xor_hash, sum_hash, n)` matches between MariaDB
  and datafusion on a known-good table (datetime + float + null are the traps). Gates V4.
- **V4 · row-hash reconciliation (layer 4)** — uses V0's `render()`; `--verify-deep`.

Either way:
- **V6 · `--verify-after`** sync chaining + docs (cli.md / config.md) + this doc marked done.

Each step: unit tests (mock `SourceProbe`/`DeltaProbe`) + an integration test (seed source,
sync, mutate, verify) against testcontainers, mirroring the two-stream integration test.

---

## 9. Risks & open decisions

1. **Live-source race** — exact equality only holds for static tables; everything else is
   "covered slice (≤ HWM) + lag". Accept that framing.
2. **Canonical render correctness** — the make-or-break detail, but it **only affects the
   deferred L4** (§2.5); the recommended scope (L0–L3 + single-engine L5) avoids it.
3. **Cost on huge tables** — layers 1–4 are full scans on *both* sides (minutes on ~112M rows,
   but bounded memory). Sampling is cheap. Keep depth configurable; default 0–3.
4. **CRC32 collisions** — use a wider digest (MD5 halves) for layer 4 on large tables.
5. **Scope/ergonomics** — all tables vs `--verify <table>`; standalone vs `--verify-after`;
   default depth.
6. **PII in sampling output** — sample diffs surface real values (e.g. a phone/email column).
   Keep verify aggregate-by-default; make sampling opt-in + consider redaction.

---

## 10. Open question — better/more precise ideas to consider later
- **Block/range checksums** (hash per id-range bucket) → pinpoints *where* a mismatch is and
  enables incremental re-verify, at the cost of more aggregates.
- **Stats-based fast path** — if append/merge writes Delta column stats (min/max/null counts),
  some aggregates can be read from Delta metadata without a scan.
- **Sampling strategy** — boundary + percentile + most-recent rows tend to surface real bugs
  better than uniform random.
