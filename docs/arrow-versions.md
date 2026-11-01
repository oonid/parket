# Arrow versions: where parket stands, and what blocks the next upgrade

**Status date:** 2026-11-01.

parket uses **one Arrow version, 58, throughout**: the extractor (connector_arrow) and the writer
(deltalake) exchange `RecordBatch`es directly, which needs both to use the *same* Arrow major
version. Two majors are two unrelated Rust types, which is why parket once needed a conversion
layer ([arrow_v54_to_v57.md](arrow_v54_to_v57.md), historical).

## Current

| Crate | Version | Arrow |
|---|---|---|
| deltalake / deltalake-core | 0.32.4 | `^58` |
| datafusion (through deltalake) | 53.1.0 | `^58` |
| connector_arrow | 0.12.1 | `58` |
| arrow-\*, parquet | 58.4.1 | — |

Arrow 58 still gets backports. **parquet 58.4.1** (2026-10-26) moved from thrift 0.17 to thrift
0.23, which clears the thrift 0.17 advisory that had no fix on 58.3. Within 58, take updates with
`cargo update`; keep every `arrow-*` crate on the same version.

## The ecosystem has split

| Crate | Newest release | Arrow |
|---|---|---|
| arrow, parquet | 60.1.0 (2026-10-28) | 60 |
| deltalake | 1.1.x (1.0.0 on 2026-09-21); delta-rs `main` too | **59** (with datafusion 55) |
| datafusion | 55.2 | **59** (54 and earlier: 58) |
| duckdb | 1.20000.x (2026-10-22) | **60**, having skipped 59 (58 → 60) |
| connector_arrow | 0.12.2 | **58**; upstream has open dependency PRs for arrow 60 and duckdb 1.20000 |

## What blocks moving to deltalake 1.x

deltalake 1.x needs Arrow 59, and connector_arrow has to produce batches of the same version.
connector_arrow keeps one Arrow version for all its sources, DuckDB included, and DuckDB has no
release on 59: connector_arrow can move to 60 with DuckDB, not to 59. Until the two sides meet,
the options are:

1. **Wait** for a deltalake release on Arrow 60 and a connector_arrow release on 60.
2. **Get connector_arrow onto 59** upstream, which means decoupling or dropping its DuckDB source
   at 58 or 60 (parket uses only its MySQL source).
3. **Convert between versions** with Arrow's C data interface, as parket did for 54 → 57: zero-copy,
   but a layer to maintain and test.

Staying on deltalake 0.32 is sound meanwhile. The cost grows with time: 1.x is where fixes land.
Moving to 1.x is also an API migration in its own right (writer, transaction and kernel APIs), so
it deserves its own plan once the Arrow versions line up.
