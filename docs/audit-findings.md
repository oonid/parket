# Audit findings register (living document)

**Status date:** 2026-07-06. This is the single source of truth for audit findings and remediation
process — it consolidates `docs/audit-2026-07-04.md` and `docs/handoff-2026-07-06.md` (both retired;
content carried forward here) and the second-pass results in `docs/audit-2026-07-06.md` (kept for its
detailed analysis; finding IDs below reference it). Target runtime: an **8 GB RAM** VM.

## 0. Process, gate, and state

**Gate (run after every change; never trust a sub-agent's self-reported exit codes — re-run yourself
and read the real output; stale IDE diagnostics have also misled here, `cargo` is the arbiter):**
```bash
cargo build
cargo clippy --all-targets -- -D warnings   # zero-tolerance; --all-targets, not just --lib (see N6)
cargo test --lib                             # unit tests, no Docker
cargo llvm-cov --lib --fail-under-lines 90   # CI hard gate
# cargo test  → integration tests, needs Docker (MariaDB + MinIO via testcontainers)
```

**Working conventions (from the maintainer):** no `Co-Authored-By`/AI-attribution trailers in
commits; conventional-commit subjects (`fix(...)`, `feat(...)`, `docs(...)`); one finding at a time in
a plan → confirm → implement+test → review → commit loop, stopping for confirmation between steps;
snapshot branch (`snapshot/pre-<step>-<date>`) before starting each fix; small reviewable diffs; never
weaken an existing test assertion to make it pass.

**Branch state (2026-07-06):** active work on `test/verify-docker-integration` (C1 `8065c38`,
R2 `4baa0f0`, docs, vendored pin bump `2cfcf59`, Docker verify tests `342e6f5`).
`vendor/connector_arrow` is pinned to `e84c87f` on the oonid fork (`fix/mysql-type-coverage`,
upstream PR #79) — repoint `.gitmodules` to aljazerzen once merged. `audit/critical-fixes` is
parked at R1 (`a600e77`); fast-forward it and prune the redundant `snapshot/*` /
`fix/r2-hwm-progress` branches once the Docker tests land. parket itself is not pushed to a
remote; base `b59fd47` (= origin/master).

**Cross-engine status (updated after `342e6f5`):** the verify value-aggregate SQL is now
**execution-proven** against real MariaDB + MinIO for: full-refresh/basic deep verify across
INT/DECIMAL/DATETIME(6)/DATE/VARCHAR incl. NULL + multibyte (Clean on match, Discrepancy on a pure
value drift), and incremental HWM-scoped verify (Clean in-window, post-HWM rows genuinely excluded,
in-window drift detected). Still unproven under Docker: the two-stream verify verdict path
(`two_stream_key_stats_outcome`), the Drift and size-guard Skipped tiers, VARCHAR-only drift in
isolation, and exact set-equality beyond the aggregate fingerprints.

---

## 1. Resolved

| ID | Fix commit | Summary |
|----|-----------|---------|
| M1 | `a972250` | `delete_then_append` IN-list chunked (1024/chunk) — bounded memory/recursion |
| V2 | `3905cc8` | distinct-sum added to key fingerprint (XOR-collision closed); clippy green |
| V1 | `11fcfee`, `2d8da09` | per-column value verification, mismatch → Discrepancy; HWM-scoped for incremental |
| R1 | `a600e77` | `read_hwm`/`read_insert_hwm` propagate transient errors (no more silent full re-extract) |
| C1 | `8065c38` | full-refresh keyset pagination (integer single-col PK) + deterministic OFFSET fallback — residuals: N8, T-gap |
| R2 | `4baa0f0` | HWM no-progress guard (incremental + two-stream update) — residuals: N2, N3, N6, N7 |
| V7 (value path) | `2d8da09` | Delta value aggregates HWM-scoped symmetric with source — key path still open (VA6) |
| T1–T5 | `342e6f5` | Docker verify tests committed + strengthened: corruption→Discrepancy (both paths), post-HWM scope exclusion, NULL + multibyte rows; ran green under real MariaDB+MinIO (Opus-reviewed) |
| N4 (+N1 mappings) | `2cfcf59` | vendored connector_arrow pinned to `e84c87f` (fork): DATE/MEDIUMINT/VARBINARY/JSON mapped instead of `todo!()` panic; upstreamed as [connector_arrow#79](https://github.com/aljazerzen/connector_arrow/pull/79) — switch `.gitmodules` back to upstream once merged |

## 2. Open — Critical
- **N1 — extraction panics the process on unmapped column types.** *Largely mitigated by `2cfcf59`* (DATE/MEDIUMINT/VARBINARY/JSON now mapped; upstream PR #79). **Still open:** (a) the `todo!()` in `create_field` remains for any *other* unmapped type (`time`, `year`, `bit`-variants, geometry passed through, future types) → still a process abort; add a parket **preflight allowlist** so unmapped types are per-table errors, and/or upstream a follow-up turning the `todo!()` into a `ConnectorError`. (b) O8 remains: `time`/`year` fail the whole table instead of being skipped like geometry. *(Detail: audit-2026-07-06 §1.)*

## 3. Open — High
- **N2** — two-stream **insert** loop has no progress guard; `SMALLINT`/`TINYINT` cursor → `extract_max_id` None on a full chunk → infinite duplicate appends. Fix: mirror R2 bail; widen `extract_id_as_i64` to Int8/16/UInt8/16 (`writer/hwm.rs`).
- **N3** — `incremental.rs:54,73` still hardcodes `"id"`; with the schema-evolution column filter, R2's guard becomes a permanent hard failure. Fix: thread the discovered key; force key+cursor into `select_columns`.
- **N4/T1** — ~~vendored fix separable from the tests~~ **done** (`2cfcf59`, PR #79): fresh clones now fetch the fixed commit from the fork. T1 completed in full by `342e6f5` (tests committed with T2–T5 strengthening).
- **N5** — unsigned ints: Delta schema created signed (`mariadb_type_to_arrow` ignores ` unsigned`), batches arrive `UInt*`; evolution check structurally blind (O10). Verify live in Docker, then map/cast. Related: `extract_id_as_i64` `as i64` wraps past 2⁶³; full-refresh keyset bails only after chunk-0 overwrite.
- **VA1** — DataFusion `sum(decimal(38,10))` silently wrong on precision overflow (measured) vs MySQL saturation → false Discrepancy on huge decimal sums. Guard by magnitude or skip SUM for at-risk columns.
- **VA3** — verify memory on 8 GB: `latest_key_stats` full-log window sort runs **before** the row-cap guard; cap measures source not Delta log; per-column CTE re-scans; unbounded SessionContext. Fix: cap first, one multi-column aggregate query, bounded RuntimeEnv.
- **O1** — `TABLE_HWM_<t>` accepted-but-ignored for two-stream (`orchestrator.rs:255` vs sole consumer `incremental.rs:27`). Honor it or bail.
- **O2 / R4 (escalated)** — shutdown mid-full-refresh: chunk-0 `overwrite` already destroyed the prior snapshot; break records `success` on a truncated table; interrupted runs generally exit 0 and skip remaining tables silently (`orchestrator.rs:167-215,294-307`). Fix: explicit "interrupted" outcome → `PartialFailure`; stage-and-swap for full refresh.
- **O3** — auto-detection selects **nullable** cursors (`discovery.rs:243-253` name+type only) that `--inspect` itself brands UNSAFE; preflight equally blind. Thread nullability into `detect_mode`.
- **M2** — `extractor.rs:56` `reader.collect()` materializes the whole result (plus mysql client buffer ≈ 2× budget) before writing. Stream to the writer.
- **M3** — `concat_batches` doubles the source payload outside the spill pool in `merge_batch` (`writer/two_stream.rs:40`; `delete_then_append` already drops early). Register without copy or drop early.
- **V3** — verify key-set verdict only runs for a column literally named `id` (`verify.rs:448` area); other PKs → Skipped→Clean. Derive key from `TablePlan`.
- **V4 / VA3(a)** — pre-guard full-log scan (same fix batch as VA3).

## 4. Open — Medium
- **N6** — R2 broke `cargo build --examples` (`examples/standalone_pipeline.rs:203,240`, E0061); widen the gate to `--all-targets`.
- **N7** — `hwm_has_advanced` string compare is format-coincident: safe on the production T-format path; hazard for space-format config seeds and any true-Timestamp batch path. Normalize at entry points or compare parsed values.
- **N8** — `ORDER BY <all columns>` OFFSET fallback: ci-collation and TEXT-prefix ties break total order (skip/dup on PK-less tables) + per-page full filesort. Prefer any UNIQUE index; BINARY-strengthen ordering.
- **VA2** — decimal scale>10: round-then-sum vs sum-then-round → deterministic false Discrepancy (measured). Skip or mirror per-row rounding on source.
- **VA4** — one unparseable string in a Utf8 numeric column aborts the whole verify run (`try_cast` / per-table error capture).
- **VA5** — `fp_num` has no non-null count → value↔NULL swap invisible in incremental scope. Append `|n=COUNT(col)`.
- **O4** — unknown `TABLE_MODE` values (typos, `two_stream`) silently → Auto (`config/parse.rs:37-41`). Bail.
- **O5** — two-stream cursor config silently overrides explicit `TABLE_MODE`. Bail/warn on conflict.
- **O6** — `adapters.rs:157-167` `get_schema` `Err(_)=>Ok(None)` (R1-class recurrence, ×2 duplicated impls) silently disables schema-evolution check. Classify missing-vs-transient like R1.
- **O7** — `--check`/run parity: mode-override skips column validation; no evolution check in preflight; S3 health-check written at bucket root ignoring `s3_prefix`; local mode probes nothing.
- **O8** — unsupported-type inconsistency: geometry skipped gracefully; `time`/`year`/`bit` fail the whole table (and would panic in the connector — N1 class). Extend skip-list or map them.
- **R3** — SIGTERM not handled (only SIGINT, `orchestrator.rs:324-333`); containers/systemd hard-kill mid-batch. Add `SignalKind::terminate()`.
- **R5** — `state.json` atomic-swap but no `sync_all`/dir fsync (`state.rs:52-64`) → empty file on power loss → silent full re-extract.
- **D1** — new source columns silently dropped forever by evolution filter (`orchestrator/schema.rs:84-96`); also feeds N3. Additive evolution or fail loudly.
- **D2** — `WHERE ts IS NOT NULL` permanently drops NULL-cursor rows (`query.rs:31,36`) with no backfill (pairs with O3).
- **D3** — two-stream first-run seed not persisted; completions between two seeds can be skipped forever (`orchestrator/two_stream.rs:34-44`). Persist via HWM-only commit.
- **M4** — `TARGET_MEMORY_MB` has no RAM-relative ceiling (`config.rs:83-88`); 64 GB on an 8 GB box OOMs at runtime, not config load.
- **CF1** — `DEFAULT_BATCH_SIZE=0` accepted (`config.rs:116-122`) → div-by-zero/no-progress.
- **CF2** — `AVG_ROW_LENGTH` NULL → sqlx decode hard-error instead of `None` fallback (`discovery.rs:304-306`).
- **S1** — `#[derive(Debug)]` on `Config` prints `database_url` + S3 secret verbatim (`config.rs:12`); hand-write Debug/Secret newtype.
- **S2** — identifiers not backtick-escaped (embedded `` ` `` breaks out) and HWM value string-interpolated (`query.rs:1-3,31`).
- **S3** — `mask_secret` byte-slices the last 4 bytes → panic on multibyte secrets (`config/mask.rs:23`).
- **P1** — per-batch `mysql::Conn::new` + per-write full Delta `load()` (`extractor.rs:47-51`; `writer.rs:166-169` etc.) — pool the connection; keep a table handle.
- **pf1** — `preflight.rs:144` `unreachable!()` reachable for nullable-timestamp FullRefresh tables → `--check` aborts.
- **V5** — verify schema check compares column *names* only; types read but unused.
- **V6** — verify sample = lowest 100 ids only; recent rows never spot-checked.
- **V8** — unsigned-key CAST asymmetry in verify (saturate vs overflow) → false verdicts on `BIGINT UNSIGNED > 2⁶³`.
- **O9/O11/O12** — failed-run state wipes last-success metadata + `schema_columns_hash` write-only; `TABLE_TIMESTAMP` validated before mode resolution (fails tables that never use it); `--verify` mode resolution is a third divergent copy — auto-detected incremental verifies as `Basic`. Shared mode-resolver fixes O5/O7/O12 together.
- **T2–T5** — ~~corruption/scope/NULL/multibyte test coverage~~ **done** (`342e6f5`). Still open (T6): two-stream verify verdicts, Drift + Skipped tiers, VARCHAR-only drift isolation under Docker; suite also lacks keyset page-boundary (C1) and R2-bail coverage.

## 5. Open — Low
- **L1** calendar: O(|years|) loop + `i64` overflow near extremes (`calendar.rs:7-19`).
- **L2** negative pre-1970 timestamps: truncating vs euclid division mismatch in rendering (`writer/datetime.rs:62-82`).
- **L4** dedup `ROW_NUMBER … ORDER BY key` keeps an arbitrary duplicate (order by version DESC).
- **L5** index-hint check hardcodes `updated_at` (`discovery.rs:121-136`).
- **L6** `extract_hwm_from_batch` builds three O(n) transient Vecs for a single max.
- **L7** `read_hwm` reads only the latest commit — a later non-HWM commit (OPTIMIZE etc.) shadows a real HWM.
- **O13** cosmetics: `inspect` prints PRIMARY without checking `key == "PRI"`; `state.json` path cwd-relative; second-signal exit(130) flushes nothing; VA6 `latest_key_stats` unscoped (V7 key path).

## 6. Suggested order
1. **N1 + N4/T1** — vendored fork fix + pin bump + tests, atomically; preflight allowlist.
2. **N2 + N3 + N6** — remaining R2-class guards, key threading, examples fix, `--all-targets` gate.
3. **Docker verify run** — settles N5/V8, validates value-aggregate SQL; add T2/T3/T4/T5 while there.
4. **VA1/VA2/VA3 (+V4)** — verify correctness/memory batch.
5. **O2/R4 + R3 + R5** — shutdown/durability batch.
6. **O1/O4/O5/O6 + shared mode-resolver (O7/O12)** — config-intent batch.
7. **M2/M3/M4, V3/V5/V6, D1–D3, CF/S/P/pf** — then Lows.
