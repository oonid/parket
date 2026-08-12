# `parket verify` — what fails the run vs. what's only reported

`parket --verify` reconciles each synced Delta table against the live source database and
returns a **verdict** (an exit code). Not every check it performs affects that verdict — some
are **verdict-gating** (a mismatch fails the run) and some are **diagnostic** (a mismatch is
printed for the operator but does *not* change the exit code). This split is deliberate; this
document explains it so the output isn't misread.

## Verdict / exit codes

| Verdict | Exit | Meaning |
|---|---|---|
| `Clean` | 0 | Every table passed the verdict-gating checks. |
| `Discrepancy` | 1 | At least one table failed a verdict-gating check. |
| `PartiallyVerified` | 3 | No discrepancy, but ≥1 table could not be fully key-verified (see V3-r). |
| (could-not-run) | 2 | A fatal error prevented verification (connection, etc.). |

A table's own outcome is one of `Pass`, `Drift` (source legitimately grew past the sync —
not a failure), `Discrepancy`, `Skipped` (e.g. `--verify` size guard), or `PartiallyVerified`.
`Drift` and size-guard `Skipped` roll up to `Clean`.

## Verdict-gating checks (a mismatch → `Discrepancy`, exit 1)

These are built for exact cross-engine parity (MySQL vs. DataFusion), so they can safely fail
the run without false alarms:

1. **Schema — column presence.** A column in the source but missing from Delta (or vice-versa)
   → `Discrepancy`. (Column *names* only; see the type diagnostic below for why *types* are
   not gated here.)
2. **Key-set fingerprint.** For a single-column integer PK: `count`, `distinct`, range
   (`min`/`max`, range-safe for `BIGINT UNSIGNED`), `bit_xor`, and a distinct-sum. For a
   single-column string PK (V3-r Tier 2): `count`, `distinct`, and `BINARY`-normalized
   `min`/`max` (byte-parity with DataFusion's Utf8 ordering). Detects missing/extra/duplicate
   rows.
3. **Per-column value aggregates.** `sum`/`min`/`max`/`count` per comparable column, summed
   into `DECIMAL(65)` at the column's native scale (parity-hardened — see VA1-r/VA2/V8).
   Detects value corruption and row-count drift. Runs for keyed *and* key-less tables (a
   key-less table with a clean value-aggregate pass ends `PartiallyVerified`, not `Clean`).

## Diagnostic checks (a mismatch is PRINTED, but does NOT change the verdict)

These compare things whose two-engine *representations* differ even for identical data, so
gating them on the verdict would raise false alarms. They are surfaced as clearly-marked
diagnostic lines for a human to review; they never change the exit code.

- **Column type family (V5).** For each column present on both sides, verify prints a
  `schema TYPE DIFF (diagnostic — does not affect verdict)` line when the coarse *type family*
  differs (e.g. a column that was `int` now reads back as a string). It is intentionally
  coarse and print-only for two reasons:
  - *Representation gap.* parket stores several MySQL types as Arrow `Utf8` in Delta —
    `decimal`, `date`, `datetime`, `timestamp`, `json`, `enum`, `set` and the text types all
    become `Utf8` (`orchestrator::schema::mariadb_type_to_arrow`). So on the Delta side those
    families are indistinguishable; the diagnostic groups them as `string` to avoid crying
    wolf. The families it *can* still distinguish and flag: `int`, `float`, `string`,
    `binary`.
  - *Redundancy.* A genuine type change cannot reach a synced table through parket — both the
    extraction run and `--check` (preflight) run the schema-evolution check and **bail** on a
    dropped or type-changed column before writing. So a `TYPE DIFF` here indicates the Delta
    table was altered *outside* parket (external tampering) — worth surfacing, not worth
    failing the run over.
- **Row sample (V6/V6-r).** In deep mode verify spot-checks a sample of rows — now spanning
  the **lowest and highest halves** of the id range (V6), so recently-synced rows are covered,
  not just the oldest. A per-row difference prints
  `sample: ... differ=N missing=M (diagnostic — does not affect verdict)` plus the differing
  column names per id. Print-only because the sample compares values stringified across two
  engines (decimal/float/datetime formatting differs); the parity-hardened value-aggregate
  check above is what actually gates value correctness for the verdict.
- **Non-null census (V6-r).** Per-column non-null counts, source vs. Delta, printed as
  `non-null census: DIFFERS ... (diagnostic — does not affect verdict)`. Same rationale.

## Why not just fail on everything?

The value-aggregate and key-set checks took deliberate parity work (DECIMAL(65), native
scale, `BINARY` normalization, range-safe casts) so they produce byte-identical results for
identical data on both engines. The diagnostic checks have not been given that treatment —
turning them into verdict-gating checks as-is would false-alarm on healthy data (representation
differences), which is worse than an honest, reviewable diagnostic line. Hardening any of them
into a verdict-gating check is a deliberate future effort (tracked as V5 / V6-r in
`docs/audit-findings.md`), not a bolt-on.

## `Drift` on a `full_refresh` table is COUNT-based, not key-based

Worth stating plainly, because the verdict text reads more confidently than the check behind it.

For an **incremental** table the key-set fingerprint gates the verdict: the HWM gives a cheap scope,
so `--verify` compares count, distinct, min, max and the XOR/sum fingerprint of the key column
(`verify/delta.rs`, `key_stats`). A `Drift` there is backed by keys.

A **`full_refresh`** table has no cursor to scope by, so the drift check compares **row counts only**.
The verdict is honest about it — it says "*likely* new/changed rows since sync" — but the consequence
is easy to miss:

```
verify user_profiles: source=1311542 delta=1309813  [differ — see verdict]
VERDICT: DRIFT: source advanced past sync — likely new/changed rows since sync, not a sync error
```

`+1729` is **arithmetically identical** to `+1829 inserted and 100 deleted`. The second case leaves
rows in Delta that no longer exist upstream, and any export reading that table would still emit them.
Counts cannot separate the two.

### Closing that gap by hand

Compare fingerprints, scoped to the sync frontier. Measured 2026-08-11 on `user_profiles`:

```bash
# Delta side — reads the key column only, does not touch the source DB.
cargo run --release -p parket --example delta_key_census -- user_profiles id
#   count 1309813  distinct 1309813  min/max 1/5608128
#   sum (distinct) 3836935567298     xor 3699244
```

```sql
-- Source side, scoped to Delta's max key so post-sync inserts are excluded.
SELECT COUNT(*), COUNT(DISTINCT id), MIN(id), MAX(id), BIT_XOR(id), SUM(DISTINCT id)
FROM user_profiles WHERE id <= 5608128;
--   all six identical to the Delta figures above
```

All six matching proves the key sets are **identical within the frontier**, so the drift is purely
additive and nothing deleted is lingering. XOR alone would catch a substitution; sum catches a
transposition that XOR cannot; count and distinct together rule out duplicates and gaps.

### Scope only on the table's own auto-increment key

**A referenced or unique key is not a sync frontier.** `user_profiles` has PK `id` (bigint
auto-increment) and `user_id` (UNIQUE, FK to `users.id`). Scoping on `user_id` is wrong: an account
created years ago can fill in its profile today, producing a **low `user_id` with a high `id`** — so
`WHERE user_id <= max` pulls post-sync rows into the comparison and the fingerprints mismatch for no
real reason.

The same mistake in a different guise: when attributing missing rows between a source and a joined
export, scope on the table the sync advances through (`scholarship_participants.created_at`), never on
a joined table's key (`users.id`). Keying on `users.id` made 408 purely-post-frontier rows look like
186 scattered drops.

## Operator guidance

- **Exit 0 (`Clean`)** means the parity-hardened checks passed. Still scan the output for
  `TYPE DIFF`, `sample ... differ`, or `non-null census: DIFFERS` diagnostic lines — they flag
  things worth a look (notably external Delta tampering or a representation surprise) that
  intentionally don't fail the run.
- **Also scan for `SKIPPED`.** The size guard rolls up to `Clean` (see the verdict table above), so a
  whole-set run can exit 0 while the *largest* tables — usually the ones that matter most — were never
  checked at all. Observed: a four-table run reported no failures with `user_profiles` skipped at
  1.3 M rows, above the 1 M cap. `--verify-deep` forces those, and is the only way to get a verdict on
  them.
- **Exit 3 (`PartiallyVerified`)** means a table's values were checked but its row-set
  completeness could not be key-verified (no usable PK) — see V3-r.
- For automated gating, treat exit `1` as a hard failure and exit `3` as "investigate"; parse
  the diagnostic lines if you want to alert on type/sample/census drift too.
