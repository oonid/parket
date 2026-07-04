# Verify: Per-Column Value Fingerprints (V1b-1 & V1b-2)

## Overview

The `--verify` command performs a multi-layer check of a synced table's integrity. **V1b-1** and **V1b-2** add per-column **value fingerprint** layers that detect data corruption beyond key-set identity checks.

When a table reaches the **Pass** verdict after key-set checks, the value-fingerprint layer computes engine-side aggregates for each column and compares them between source and Delta. A mismatch **downgrades the verdict to Discrepancy**, causing `verify` to exit with code 1.

- **V1b-1** (initial): Checks non-append-log, non-scoped tables (full-refresh, basic, two-stream)
- **V1b-2** (incremental): Checks incremental tables that have a HWM by comparing source scoped to the HWM window against Delta latest-per-id scoped to the same HWM

## How It Works

### Fingerprint Computation

For each column, a fingerprint is computed based on its data type. The fingerprint is designed to:
- Be **collation-safe** for text (no character-by-character comparison)
- Be **identical across both engines** (MariaDB and DataFusion)
- Fit in memory (one aggregate row per column, not per-row)

### Fingerprint Types

| Column Type Family | Fingerprint | Method |
|--------------------|------------|--------|
| Integer (`smallint`, `mediumint`, `int`, `integer`, `bigint`) | `sum=S\|min=M\|max=X` | Exact sum, min, max |
| Decimal (`decimal`, `numeric`) | `sum=S\|min=M\|max=X` | Sum/min/max at fixed scale 10 (see caveats) |
| DateTime (`datetime`, `timestamp`) | `min=M\|max=X` | Min/max truncated to whole seconds |
| Date (`date`) | `min=M\|max=X` | Min/max date only |
| Text (`varchar`, `char`, `text`, `*text`) | `len=L\|n=N` | Sum of character lengths + non-null count |

### Execution Path

The value-fingerprint check runs **only** when:
1. The table reaches **Pass** verdict after key-set checks
2. The table has at least one comparable column (id is excluded)

The check then takes one of three paths:
- **Incremental with HWM (V1b-2)**: Compare source scoped to the HWM vs Delta latest-per-id scoped to the same HWM
- **Non-scoped, non-append-log (V1b-1)**: Full comparison (full-refresh, basic, two-stream modes)
- **No-HWM append-log incremental**: Skip with a note (no fair comparison window possible)

### Mismatch Handling

If any column's fingerprint differs between source and Delta:
- The verdict is **downgraded from Pass → Discrepancy**
- The mismatch is logged with column name, kind, and both fingerprints
- `verify` exits with code 1

## Known Limitations & Blind Spots

### Type Coverage

- **Float, Double**: Not value-checked (precision loss during cast/store)
- **Time** (time-of-day): Not value-checked
- **JSON, Blob, Binary**: Not value-checked (type incompatibility)
- **Enum, Set**: Not value-checked

### Precision Limits

- **Decimal columns with >10 fractional digits**: Compared at scale 10; values with more fractional precision may differ and not be caught
- **DateTime sub-second differences**: Truncated to whole seconds; microsecond/nanosecond differences are not caught

### Text Collation Sensitivity

- **Text column fingerprints** (length + count) are **collation-safe** but **value-unsafe**
  - Example: changing "cat" → "dog" (same length, same row count) will NOT be detected
  - This is a deliberate tradeoff to avoid cross-engine collation issues

### Incremental Tables

- **Incremental tables WITH a HWM**: Value aggregates are checked (V1b-2), source scoped to the HWM vs Delta latest-per-id scoped to the same HWM
- **Incremental tables WITHOUT a HWM**: Value checks are skipped — no fair comparison window (the source may have advanced beyond the last sync point)
- **Append-log tables**: Row-level census/sample checks are deferred (they require deduplication); value aggregates are checked separately if a HWM is present

Note: The key-set (ID-presence) check's HWM scoping is a separate audit follow-up (V7); only the value-aggregate path is HWM-scoped on both sides here.

### Integration Testing

- Adapter SQL (MariaDB and DataFusion) is only exercised by **Docker integration tests** (`cargo test --test verify` or the integration suite)
- `cargo test --lib verify` runs unit tests with mocked probes; it does not execute actual SQL

## Example

```
$ porter verify --table orders --verify-deep

verify orders plan: mode=basic
verify orders schema: source_cols=5 delta_cols=5 missing_in_delta=[] extra_in_delta=[] [schema ok]
verify orders: source=1000 delta=1000 [match]
verify orders VERDICT: PASS
verify orders non-null census: 5 columns match
verify orders sample: checked=100 match=100 differ=0 missing=0
verify orders value-aggregates: 3 column(s) match
verify summary: pass=1 drift=0 discrepancy=0 skipped=0
```

vs. with a mismatch:

```
$ porter verify --table orders --verify-deep

verify orders VERDICT: PASS (before value check)
verify orders value-aggregates: column value mismatch: amount (Integer: source=sum=50000|min=10|max=500 delta=sum=49950|min=10|max=500)
verify orders VERDICT: DISCREPANCY: column value mismatch: ...
verify summary: pass=0 drift=0 discrepancy=1 skipped=0
```

Exit code: 1
