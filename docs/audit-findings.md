# Audit findings register (living document)

**Release v0.2.6 (2026-08-07):** §10.1 **FIXED and validated at production scale** by v0.2.5
(`0c0c270`) — a 12,715-key window applied as ONE atomic overwrite in 14.4 min at **0.210 GB peak**
(vs ~52.6 min for the chunked path, and vs attempt 1 dying at 2,044.6 MB). v0.2.6 fixes the one
caveat that validation exposed: `STAGE_ROWS` 200k→5M, because flushing per staged chunk produced
**564 output files** where MERGE produces ~36. v0.2.4 is a **pre-release** — its overwrite path
failed in production; do not deploy it.

**Status date:** 2026-08-07. This is the single source of truth for audit findings and remediation
process — it consolidates `docs/audit-2026-07-04.md` and `docs/handoff-2026-07-06.md` (both retired;
content carried forward here) and the second-pass results in `docs/audit-2026-07-06.md` (kept for its
detailed analysis; finding IDs below reference it). Target runtime: an **8 GB RAM** VM.

**Completion status (2026-07-16, tagged `v0.2.1`):** BOTH audit passes are resolved. The original audit
(§1–§6): every **Critical, High, and Medium** finding is resolved or closed — the full O-series
(O12/O11/O9/O7 + O7-rest-b), N-series (N2-r/N7/N8/N3-r), P1 (both halves), M4, D1-r, the atomic
full-refresh (O2-r), and the entire verify V-series (V8, VA1-r, V3-r Tier 1+2, V5, V6, V6-r). The
**fresh `fable` audit (§7)**: every **Critical, High, and Medium** resolved — FA1/FA3 (full-refresh
safe-cast + source-schema adoption via a Metadata action), FA2 (two-stream update-window cap), FA4
(bounded default `delete_then_append` session), FA5 (quoted two-stream identifiers) — plus the
worthwhile Lows FA6/FA7/FA8/FA9/FA10/FA12 and §5's L5/L6 (L2 found already-fixed).

> **NO LONGER TRUE as of §10 (2026-08-06):** the claim below that only Low-tier residuals remain was
> accurate through `v0.2.3`, but a production two-stream sync has since surfaced a **High** —
> **§10.1, `delete_then_append` write amplification**: one full-table rewrite per 1024 update keys,
> measured at ~56 h and ~2 TB of egress to apply an 840 k-row update window to a 115 M-row table.
> It is a cost defect, not a correctness one, and is **FIXED** — see the §10.1 header. For the
> record: `UPDATE_STRATEGY=merge` was measured on 2026-08-07 at **1.757 GB peak under an 8 GiB cap**
> for a routine 26 k-row window, so the earlier "not recommended on memory grounds" gate is lifted
> for routine syncs (large windows still unmeasured).

Apart from §10, what remains open is
exclusively **Low-tier / deferred residuals**, each documented below with rationale: FA11, FA4-r, V8-r,
T6-r, M2-r2, D1-r2/-r3, P1-r-a2/-b, S2-r, N1-u, N1-r2, L1, L4, L7, O13, V6-r. Verify's verdict-gating vs
diagnostic layers are documented in `docs/verify-checks.md`; `crates/mysql-metadata-probe` carries a
PK-classification survey (V3-r input; local diagnostic tool, not part of the parket gate).

**Post-sync operational evaluation (2026-07-16, `fable` model, live DB+S3 verified) — see §8.** A full
production sync of all 8 configured tables to `s3://dcdanalytics/parket/` (v0.2.1 release binary, run
under an enforced 8 GB cgroup cap) was evaluated end-to-end against the live source. Result: **provably
consistent at the sync frontier** (exact row parity incl. the 114.3M-row two-stream table; sane HWMs;
atomic commit trail; FA2/D3 fixes confirmed working in production; 2.58 GB peak vs 8 GB). The residual
risks are **semantic, not mechanical** — chiefly the `completed_at` update-cursor blind spot on
`developer_journey_trackings` (§8). New/operator findings recorded in §8: **PS-M1** (a genuine new
verify false-DISCREPANCY bug), **PS-H-A** (the trackings freshness-contract decision), **PS-H-B**
(= the un-implemented fix (b) of H-2026-07-11-1), and PS-M2/M3/L1–L3. **PS-H-B and PS-M1 are now
resolved** (`bd13be5` — the `TABLE_RECONCILE` one-shot reconcile flag, §8.2; `00afcd5` — the verify
false-DISCREPANCY fix, §8.3). A **pre-v0.2.2 Low-batch cleanup (§9, `391210d`/`758a2b0`)** then resolved
L7, FA11, M2-r2, S2-r, N1-r2, D1-r2, L1, O13, and PS-L1/PS-L2; V8-r and O13/VA6 stay deferred with
rationale, and PS-M1-r/T6-r remain Docker-pass follow-ups. A `fable` follow-up investigation established that the
trackings engagement drift is a **frozen historical backlog** (`last_viewed` stopped advancing
~2025-02-03), so a *dynamic/multi-field* update cursor would catch ≈0 rows/day and can't heal the
backlog — the PS-H-A remedy is a one-time reconcile (now clean via PS-H-B), not a cursor change; a new
`crates/trackings-drift-census` diagnostic tracks the backlog day-by-day.

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

**Branch state (2026-08-05):** `master` is the ONLY local branch — the topic branches and the ~34
`snapshot/*` safety branches were pruned once their work had landed (each verified merged into
`origin/master` first) — and its head is now tagged **`v0.2.3`** (bumped from `0.2.2`). Since v0.2.2:
**FA2-r2 changes `--verify` exit-code semantics** — duplicate keys on a two-stream table now yield
`Discrepancy` instead of a printed diagnostic, enabled by measuring production FIRST (the only live
two-stream table reports `count == distinct == 114,412,210` via the new census, so no existing
deployment starts failing); **PS-M1-r + T6-r** Docker coverage (the true PS-M1 Drift shape, and keyset
full-refresh page boundaries — while the R2 no-progress bail is recorded as unreachable-by-construction
rather than untested, and kept as defence-in-depth); the new **`examples/delta_key_census`**, a
Delta-side key census that costs the source database nothing; **PS-H-A decided** (the `TABLE_RECONCILE`
one-shot adopted with a verify-driven cadence, source-side `updated_at` escalated, and the `last_viewed`
cursor swap investigated and REJECTED on live-DB evidence — 99.2% of completed rows have
`last_viewed < completed_at`, so it would stop completions re-syncing); plus two documentation
corrections that would each have misled an operator — the post-L7 runbook HWM check (it would
false-alarm on today's production log, whose two newest commits are `VACUUM START`/`END` and carry no
watermark) and PS-H-A's option-2 mechanics (still describing the pre-`bd13be5` manual `TABLE_HWM`
dance). **Released:** `master` is pushed and tagged `v0.2.3` — both the Release and CI workflows green
(CI proves it on a clean checkout with submodules initialised, independently of any local run), all four
target artifacts published, and the `x86_64-unknown-linux-gnu` binary deployed in porter (`--version`
and `--check` verified against the live config). porter pins the submodule at the released commit
`3ce0ea5`; post-release work may sit ahead of that tag, which is expected — do not chase the pin
forward without a new release. Remote gitops are run manually by the operator with an SSH key.
`vendor/connector_arrow`: upstream PR #79 **merged** and released as v0.12.1 — submodule on aljazerzen
upstream at `3e98df6`; the temporary fork pin is retired.
Coverage gate (re-measured 2026-08-05 at v0.2.3 release time): `cargo llvm-cov --lib -p parket
--fail-under-lines 90` reports **92.60% lines** (regions 91.79%, functions 90.21%), exit 0 — passes
(`verify/source.rs` reads ~0% under `--lib`: it is Docker-integration-only, covered by the 40-test
Docker suite instead). Local gate at release: `cargo build` clean, `cargo clippy --all-targets -D
warnings` clean, `cargo test --lib` **664 passed**, full Docker integration suite **40 passed**.

**Cross-engine status (updated after `108d9e3`):** the verify value-aggregate SQL is now
**execution-proven** against real MariaDB + MinIO for: full-refresh/basic deep verify across
INT/DECIMAL/DATETIME(6)/DATE/VARCHAR incl. NULL + multibyte (Clean on match, Discrepancy on a pure
value drift), and incremental HWM-scoped verify (Clean in-window, post-HWM rows genuinely excluded,
in-window drift detected), now including native-scale DECIMAL(20,12) (healthy Clean, digit-12
drift detected — `a9bf774`). **T6 (`87db089`) closed the remaining Docker gaps**: the two-stream
verify verdict path (Clean + Discrepancy), the Drift tier, the size-guard Skipped tier, and
VARCHAR-only drift are all now execution-proven under real MariaDB+MinIO. Further Docker-proven this
sweep: wide-decimal SUM at DECIMAL(65) (VA1-r), range-safe `BIGINT UNSIGNED > 2⁶³` key/value min/max
(V8), key-less value-aggregates + `PartiallyVerified` (V3-r Tier 1), single-column string-PK key-set
fingerprint (V3-r Tier 2), recent-row sampling (V6), and the verdict-neutral type-family diagnostic
with zero false positives across the suite (V5/V6-r). Residual (documented, low): exact set-equality
beyond the aggregate/`bit_xor` fingerprints; the sample/census layers remain diagnostic-only by
design (see `docs/verify-checks.md`).

---

## 1. Resolved

| ID | Fix commit | Summary |
|----|-----------|---------|
| M1 | `a50ff1e` | `delete_then_append` IN-list chunked (1024/chunk) — bounded memory/recursion |
| V2 | `a29f161` | distinct-sum added to key fingerprint (XOR-collision closed); clippy green |
| V1 | `d21eb52`, `9c0f384` | per-column value verification, mismatch → Discrepancy; HWM-scoped for incremental |
| R1 | `1ff706f` | `read_hwm`/`read_insert_hwm` propagate transient errors (no more silent full re-extract) |
| C1 | `2b8c3f9` | full-refresh keyset pagination (integer single-col PK) + deterministic OFFSET fallback — residuals: N8, T-gap |
| R2 | `313ba2b` | HWM no-progress guard (incremental + two-stream update) — residual: N7 |
| V7 (value path) | `9c0f384` | Delta value aggregates HWM-scoped symmetric with source — key path still open (VA6) |
| N2+N3 | `2399b50` | insert-stream progress guard (bail before append); extract_id_as_i64 widened to Int8/16 + UInt8/16, UInt64 overflow → None; real integer-PK threaded through incremental (fallback `id`); early actionable bail when cursor/key dropped from select_columns (both modes) |
| N6 | `54bbf45` (+ ci.yml) | examples call sites fixed; local gate widened to `cargo clippy --all-targets -- -D warnings`; **CI workflow's Clippy step updated to `--all-targets`** so the widening is enforced in CI, not just locally |
| T1–T5 | `108d9e3` | Docker verify tests committed + strengthened: corruption→Discrepancy (both paths), post-HWM scope exclusion, NULL + multibyte rows; ran green under real MariaDB+MinIO (Opus-reviewed) |
| N4 (+N1 mappings) | `36c485c`, `3a3059d` | DATE/MEDIUMINT/VARBINARY/JSON mapped instead of `todo!()` panic; [connector_arrow#79](https://github.com/aljazerzen/connector_arrow/pull/79) **merged upstream** (v0.12.1) — submodule back on aljazerzen |
| O2/R4, R3, R5 | `6978d6c`, `52aa55a` | interrupted runs exit PartialFailure + table state "interrupted" (never "success"); shutdown mid-full-refresh after chunk 0 bails as a failure naming the partial rewrite; SIGTERM joins SIGINT (second signal → exit 130); state.json fsync (file before rename, dir after) — residual: O2-r stage-and-swap |
| H-2026-07-11-1 | see log | has_data guard: incremental / two-stream-insert with no watermark refuse from-scratch APPEND onto a non-empty Delta table (converts the silent-duplication class incl. the L7-shadow path into an actionable error); Docker 15/15 confirms no false fire |
| H-2026-07-11-2 | see log | breaker counts UInt8/16/32 buffers twice (post-align widening) so the 2x ceiling holds for the written window |
| N1-residual + O8 | `ee09a7f` | explicit EXTRACTABLE_DATA_TYPES allowlist as the pipeline gatekeeper (single-sourced across orchestrator + preflight, bidirectional sync test vs mariadb_type_to_arrow); time/year/bit/uuid/inet/geometry/future types uniformly column-skipped with a warn instead of table-fail/process-abort; Docker-proven (TIME/YEAR/BIT table syncs, Delta schema exactly [id,name]) (Opus-reviewed) |
| V3 | `6c1fe9c` | verify resolves the real key: two-stream insert cursor > discovered single-column integer PK (new SourceProbe::integer_pk) > `id` fallback; threaded through key-stats/scoping/sampling/value filters incl. SourceScope.key_col; honest Skipped reason; Docker-proven on a `code_id`-keyed table (Clean → corruption → Discrepancy) (Opus-reviewed) — residual: V3-r |
| O3 (+pf1) | `e3395da`, `04e2678` | ColumnInfo carries nullability; auto-detection never selects a nullable cursor (demotes to full_refresh + warn when nullability is the deciding factor); explicit incremental/two-stream cursors honored + loud warn (row loss itself = D2); preflight inherits via shared detect_mode and its KEY reason names the nullable cursor — eliminating pf1's reachable unreachable!() (Opus-reviewed) |
| N5 | `2f0c4f8` | unsigned int columns widened to signed Arrow/Delta types (tinyint/smallint/mediumint→Int32-range, int/bigint→Int64) + batches cast before write (safe:false errors on >i64::MAX BIGINT UNSIGNED by name); Docker-proven round-trip incl. above-signed-max values across 2 runs + actionable overflow failure (Opus-reviewed). Migration: pre-fix unsigned Delta tables have narrower types → evolution check flags them → full-refresh to rebuild |
| M2, M3 | `0945149`, `04a90a7` | zero-copy MemTable registration for merge/delete sources (update-window peak ~halved); mid-stream memory circuit breaker at 2× budget with safe cursor truncation (OFFSET path bails before any write). NOTE: the audit's "mysql client buffers the whole result (2×)" claim was REFUTED — vendored connector_arrow streams via exec_iter, yielding 1024-row batches lazily; the window Vec is the by-design budget (one window = one HWM-carrying commit). Residual: M2-r2 |
| O1, O4, O5, O6 | `743fc95` | two-stream honors TABLE_HWM seeds (writer HWMs win; live bootstrap skipped); invalid TABLE_MODE bails actionably (two_stream → cursor-vars hint); mode/cursor conflicts bail at config load (both load paths); get_schema classifies missing-vs-transient like R1 (Opus-reviewed) |
| CF1, CF2 | `4c89bd7` | DEFAULT_BATCH_SIZE=0 rejected; AVG_ROW_LENGTH NULL → graceful fallback (Codex) |
| VA1/VA2/VA3/V4/VA4/VA5 | `a9bf774` | component fingerprints + central assembly; native-scale decimals (Docker-proven); sum-overflow guard; n= counts; cap-before-scan incl. Delta rows; one-query aggregates; bounded probe sessions; try_cast + per-table Skipped-on-error (Opus-reviewed) — residual: VA1-r |

## 2. Open — Critical
*(empty — all Critical findings resolved as of `ee09a7f`. N1's parket-side gatekeeper is in place; the remaining upstream nicety — turning connector_arrow's `todo!()` into a `ConnectorError` — is tracked as N1-u below, Low, since it is unreachable from parket's callers.)*

## 3. Open — High
- **H-2026-07-11-1** — **done** (`has_data` guard; see resolved table). Original finding: mode-round-trip wipes the HWM → silent full-table duplication. Full-refresh commits carry NO hwm keys (`full_refresh.rs:218,222` pass `None`; `build_commit_properties(None)` writes empty metadata) and `read_hwm` reads only the LATEST commit (`writer.rs:263`). Sequence: long-running incremental table → operator full-refreshes it once (exactly the rebuild this register's N5 migration note recommends) → switches back to incremental → `read_hwm`=None → re-extracts the whole table and APPENDS onto the complete snapshot → every row duplicated, exit 0. Same shadowing applies to any later no-HWM commit (composition of L7 + N2-r, both previously graded lower — this sequence is the realistic High). FIX: (a) guard — incremental with no stored HWM but a NON-EMPTY Delta table must bail actionably ("N rows but no HWM: full-refresh or set TABLE_HWM"), which also converts the L7-shadow path into a loud error; (b) optionally stamp the snapshot max-cursor HWM on full-refresh's final commit so the round-trip resumes seamlessly.
- **H-2026-07-11-2** — **done** (widening-weighted breaker; see resolved table). Original finding: M2 ceiling measured on PRE-alignment bytes. The circuit breaker accumulates `get_array_memory_size` of the raw extracted batches (`extractor.rs`), but N5's `align_batches_to_schema` (`incremental.rs:75` etc.) then widens unsigned columns (UInt8→Int16, UInt16→Int32, UInt32→Int64 = 2× buffer each) BEFORE the write. An unsigned-heavy window admitted at just under 2×TARGET can become ~4×TARGET resident post-align — with an aggressive TARGET_MEMORY_MB (M4: still uncapped) that reaches OOM territory on the 8 GB VM, the exact class M2 exists to prevent. Conditional-High (unsigned-heavy tables + large TARGET). FIX: count UInt-bearing batches at a widening-adjusted weight in the breaker (schema check once per window), or halve the effective ceiling when the batch schema contains UInt columns.
- **N2, N3** — **done** (`2399b50`, Opus-reviewed). Residuals registered below: N2-r (partial-chunk cross-run duplicates), N3-r (detect_mode literal-`id`).
- **N4/T1** — ~~vendored fix separable from the tests~~ **done** (`36c485c`, PR #79): fresh clones now fetch the fixed commit from the fork. T1 completed in full by `108d9e3` (tests committed with T2–T5 strengthening).
- **N5** — **done** (`2f0c4f8`, Opus-reviewed). Probe confirmed pre-fix delta-rs errored at write time (unsigned tables never synced). `extract_id_as_i64` u64-wrap was already fixed in N2's `try_from`; N5 additionally aligns batches so keys are signed before extraction.
- **VA1, VA3** — **done** (`a9bf774`, Opus-reviewed). Residual VA1-r below.
- **O1** — **done** (`743fc95`). Doc notes: one `TABLE_HWM_<t>` seeds BOTH two-stream streams (insert←id, update←ts) and the update boundary is strictly `ts > seed` (completions at exactly the seed instant are skipped — same as the live bootstrap).
- **O2/R4** — **done** (`6978d6c`, Opus-reviewed). **O2-r** — **done** (CP1 `e62df95` + CP2 `b3c00dc`): full refresh is now **atomic**. `process_full_refresh` begins a staged overwrite, stages each chunk via `RecordBatchWriter` (parquet written, NOT committed — bounded memory: only `Add` metadata accumulates), and commits ONCE at the end (all current files → `Remove` + staged `Add`, a single `CommitBuilder` `Write(Overwrite)` transaction — the Delta log IS the atomic swap). A mid-rewrite interruption now just skips the commit → staged parquet is orphaned (vacuum-able) and the **prior snapshot is left fully intact** (was: chunk-0 overwrite destroyed it → data-destructive failure). Empty source still leaves the table unchanged. `stage_overwrite_chunk` coerces each batch to the writer's exact target schema by name (casting types + aligning nullability), since `RecordBatchWriter` is stricter than the old tolerant `table.write()` path (Docker caught a NOT NULL-column + an Int16→Int32 gap). DeltaWrite trait + both adapters gain begin/stage/commit_overwrite; the P1 handle cache is evicted on commit. 3 local-FS proof/regression tests (single-commit swap, abort-safety, non-nullable-source coercion) + the interruption test flipped to assert non-destructive behavior + full 28-test Docker suite green (Opus-reviewed, Docker-proven).
- **O3** — **done** (`e3395da` + `04e2678`). Note: a `TABLE_HWM_<t>` on a table now demoted to full_refresh bails "not incremental" — fail-loud replacing a silently-lossy config; operator-facing docs should mention it.
- **M2, M3** — **done** (`0945149`, `04a90a7`, Opus-reviewed; see resolved table for the corrected M2 claim). True extract→write streaming deliberately NOT pursued: the window Vec is the crash-safety unit (one window = one atomic HWM commit).
- **M2-r2 (Low)** — the N2/R2 no-progress guards are gated on `rows == batch_size`; a TRUNCATED window with an unextractable cursor (narrow: needs an unsupported cursor type or u64 > i64::MAX, since `ts IS NOT NULL` filters the common case) could loop re-appending. Hardening: bail on a truncated window that fails to advance the cursor (keyset full-refresh already does). Also: keyset full-refresh ignores the breaker's batch_size halving mid-table (correct but wasteful — re-trips per chunk).
- **V3** — **done** (`6c1fe9c`, Opus-reviewed).
- **V3-r (Med)** — **done** (Tier 1 `e78be86` + Tier 2 `5cca3bd`). Live-DB survey (`crates/mysql-metadata-probe`, PK-classification over the `dicoding` schema) settled the shape: of 430 base tables, **425 have a single-column integer PK** (verify-keyed today) and only **5 are key-less** — `oauth_access_tokens` (varchar PK, 2.66M rows), `oauth_refresh_tokens` (varchar, 1.65M), `oauth_auth_codes` (varchar, 1.15M), `migrations` (no PK, 663), `password_reminders` (no PK, 0). **0 of the 5 are in the configured TABLES**, so V3-r is not reachable by today's config — but the `oauth_*` varchar-PK tables are large if ever synced. **Crucially every key-less case is single-column-string or no-PK — ZERO composite** — so no composite/hash parity machinery is ever needed here. Original bug: a key-less table short-circuits to `Skipped` at verify.rs:711 BEFORE both key-stats and value-aggregates, yet `Skipped` rolls up to exit-0 `Clean` (false confidence). **Closure spec:**
  - **Tier 1 — DONE** (`e78be86`): key-less tables now run the value-aggregate checks and report a new `PartiallyVerified` verdict (exit code 3) instead of silently rolling up to Clean; a value-agg mismatch still yields Discrepancy; keyed tables unchanged. Full 31-test Docker suite green.
  - **Tier 1 (honest closure) spec:** (a) run the per-column value-aggregate checks for key-less tables too — reorder `run_one_table` so the value-agg path (sum/min/max/count per column, already DECIMAL(65)-parity-proven) runs regardless of key; value-aggregates need no key and catch column-value corruption + row-count drift on any table. (b) add a distinct non-Clean verdict tier (e.g. `PartiallyVerified`) + exit code (e.g. 3) so a key-less table that passed value-aggs but couldn't key-verify is NOT reported as exit-0 Clean. Removes both "verified nothing" and "silent Clean".
  - **Tier 2 — DONE** (`5cca3bd`): single-column string PKs now get a real key-set fingerprint (`count`/`distinct`/BINARY-normalized `min`/`max` on both probes; MySQL `MIN/MAX/COUNT(DISTINCT BINARY key)` byte-matches DataFusion Utf8 ordering, Docker-parity-proven on a `token VARCHAR(64)` table: match→Pass/Clean, source row deleted→Discrepancy) — upgrading string-keyed tables from Tier-1 value-aggregates-only PartiallyVerified to full key-verified Pass/Discrepancy. String key kept separate from the integer `key_col` so the integer census/sample path is untouched (guarded `Pass && key_col.is_some()`). Full 32-test Docker suite green. Original Tier-2 spec: resolve a single-column STRING PK (extend `integer_pk`→`resolve_single_column_pk` accepting non-integer), and compute a string key-set fingerprint on both probes: count + distinct + **BINARY-normalized min/max** (wrap MariaDB min/max in BINARY so its collation matches DataFusion byte order — the N8 lesson). Deliberately SKIP the xor/distinct-sum set-equality component (no matching cross-engine string hash out of the box; low marginal value over count+distinct+min/max). Gives genuine missing/extra-row detection on the unique varchar key.
  - **NOT needed** (survey-proven): composite-key handling (zero composite), cross-engine string hash for exact set-equality (over-investment for auth-token tables).
  - Tier 1 and Tier 2 compose; both far smaller than the originally-sized "option 3" because the real data has no composite keys. (Reachable-in-practice residual after Tier 2: a same-count/distinct/min/max set SWAP on a string key — narrow; the value-aggregates catch the accompanying column-value changes.)
- **V4 / VA3(a)** — **done** (`a9bf774`).

## 4. Open — Medium
- **N2-r** — **done** (`1116029`): the `None`-HWM bail now fires on ANY non-empty batch about to be appended/merged (incremental + both two-stream streams), not just full chunks — a terminal partial chunk whose cursor is present-by-name but of an unextractable Arrow type (or a BIGINT UNSIGNED key past i64::MAX) previously appended without advancing the watermark → cross-run duplication. The `IS NOT NULL` query filter (unconditional, `query.rs:51,56`) means a non-empty batch here always has a non-NULL cursor, so None ⟺ unextractable type; the all-NULL-values domain (D2) is untouched. Unit test drives `process_incremental` with a Boolean-typed cursor on a partial batch, asserting it bails before `append_batch` (`.times(0)`). Full-chunk non-advancement check stays gated to full batches.
- **N3-r** — **done** (3 checkpoints: `061b576` relocate `select_integer_pk`→discovery; `8257ccc` add `discover_indexes` to `PreflightInspect`; `031d954` the switchover). `detect_mode` now keys off `select_integer_pk` (single-col integer PRIMARY) via a `has_integer_key` signal instead of a column literally named `id`; `resolve_ts_col_and_mode` threads `indexes` so the run, `--verify`, and `--check` all auto-detect incremental for a **non-`id` integer PRIMARY** (e.g. `code_id`) consistently — no longer silently full-refreshing such tables every run. **Behavior change:** incremental auto-detection now requires a real single-col integer PRIMARY, so an `id` column that is NOT the PK (and no other single-col integer PRIMARY) resolves to full_refresh instead of incremental — safer, since incremental's keyset needs a genuinely unique key (explicit `TABLE_MODE=incremental` still overrides). Orchestrator discovers indexes once up front; preflight/verify fetch+pass them; `--check` KEY display + full-refresh reason use the resolved key, not literal `id`. New resolver + verify unit tests + a Docker test (`code_id`-PK table auto-detects incremental; re-run appends only the post-HWM row). Full 24-test Docker suite green (Opus-reviewed, Docker-proven). Note: the CP switchover's up-front `discover_indexes` cascaded inert test-mock additions into the shared orchestrator mocks (`incremental`/`two_stream`/`test_support`/`full_refresh` tests) — no production or assertion change.
- **N7** — **done** (`1116029`): `hwm_has_advanced` now orders by PARSED timestamp components `(y,mo,d,h,mi,s,nanos)` via `ts_components` (fraction right-padded to 9 digits), so a space-vs-`T` separator or differing fractional-second width between the batch formatter (`format_naive_datetime` emits a space) and a config-seeded HWM can no longer distort the decision; raw-string fallback when either side is unparseable (no behavior change on well-formed same-format inputs). 6 unit tests incl. the `.9`-vs-`.10` case raw lexicographic got wrong.
- **N8** — **done** (`eadb045`): PK-less full-refresh OFFSET pagination now builds a TOTAL row order — `select_unique_ordering_index` prefers a UNIQUE all-NOT-NULL index (PRIMARY first, then any unique; NOT-NULL required since UNIQUE permits multiple NULLs), ordering by it plainly (total order + DB can use the index → no per-page filesort); when none exists it falls back to all columns with `BINARY` on the 8 collated string/text types so a case-insensitive-collation tie can't reorder rows across separate LIMIT/OFFSET pages and skip/duplicate them. New `OrderTerm` in query.rs drives `format_order_by`. Keyset (integer-PK) path untouched. 6 unit tests (index selection + BINARY term rendering) + a discriminating Docker test (180 ci-colliding rows across 3 forced OFFSET pages → exactly 180 rows / 180 distinct). Full 23-test Docker suite green (Opus-reviewed, Docker-proven). Residual: fully-identical duplicate rows in a keyless table still can't be totally ordered (inherent OFFSET limitation) and a shared >max_sort_length TEXT prefix isn't fully broken by BINARY — both narrow, documented.
- **VA2, VA4, VA5** — **done** (`a9bf774`): native-scale decimals; try_cast + per-table Skipped-on-error; n= in every fingerprint.
- **VA1-r** — **done** (`12f5c65`): value-aggregate SUM/MIN/MAX now sum into **DECIMAL(65)** (MariaDB's DECIMAL max = DataFusion 53's Decimal256 range) instead of DECIMAL(38) on BOTH probes, and the overflow guard threshold moves to 65 — so the SUM is only skipped if it would exceed 65 digits, astronomically unreachable for real data (was: skipped past 38 digits, reachable for a wide-decimal column on a very large table → a sum-only corruption invisible there). Key-set sums (bigint) stay at DECIMAL(38) — never overflow. Docker-verified parity (DataFusion Decimal256 sum byte-matches MariaDB DECIMAL(65)) + a discriminating test: a 39-digit-sum `DECIMAL(50,0)` column verifies Clean (sum computed, not skipped) and a +1 corruption → Discrepancy. Full 29-test Docker suite green (Opus-reviewed).
- **O4, O5, O6** — **done** (`743fc95`, config-intent batch; see §1): invalid `TABLE_MODE` bails actionably; two-stream/`TABLE_MODE` conflict bails at config load; `get_schema` classifies missing-vs-transient like R1.
- **O7** — **done** (fully closed across three parity dimensions): **mode-parity** (`a09642e`, O11 — run/verify/preflight share `discovery::resolve_ts_col_and_mode`); **storage-probe parity** (`2adc536` — S3 health-check writes under `s3_prefix` via `health_check_path` catching prefix-scoped IAM misconfig; local `--check` gets a real writable-dir probe `LocalPreflightStorage` replacing the silent no-op; storage-neutral log); **schema-evolution parity** (`d3f623f`, O7-rest-b — `--check` runs the run's `schema_evolution_check`). Unit-proven (preflight-only, no data path).
- **O7-rest-b** — **done** (`d3f623f`): preflight now runs the same `schema_evolution_check` the run does — for `Incremental|TwoStream` tables it fetches the existing Delta schema (via the shared `get_schema_impl`, made `pub(crate)`; `None`/first-run → skip) and bails on a dropped/type-changed column, so `--check` pre-flags a schema incompatibility instead of letting the next run discover it. Reuses the run's functions (no duplication); the visibility changes are logic-neutral (100/100 orchestrator tests unchanged). New test proves a type-changed column bails; 18 mock tests got a behavior-neutral `delta_schema→Ok(None)` default. **O7 is now fully closed** (mode-parity + storage-probe parity + evolution-check parity).
- **O8** — **done** (`ee09a7f`).
- **R3, R5** — **done** (`6978d6c`). Minor tidiness follow-up: a failed write still leaves a stale `.tmp` (pre-existing; overwritten on next success).
- **D1** — **done**: additive evolution. `schema_evolution_check` now INCLUDES a new extractable source column in the SELECT (info-logged) instead of silently excluding+warning; the three append paths (`append_batch`, `append_two_stream`, `delete_then_append`) write with delta-rs `SchemaMode::Merge`, so the new column is added to Delta and pre-existing rows read it back NULL (drops and type changes still bail; a new NON-extractable column stays excluded per the allowlist). Validated under Docker before wiring (`append_batch_schema_merge_adds_new_column_to_delta`: Append+Merge adds the column cleanly on MinIO, old row NULL, new row populated, id/name undisturbed) + e2e (`incremental_picks_up_new_column_via_schema_merge`). Scope: `merge_batch` (UPDATE_STRATEGY=merge opt-out) is NOT evolved — a schema change there needs a full refresh (documented on the fn). This also resolves the N3 fail-fast for an extractable cursor absent from Delta (now additively picked up; guard remains for a genuinely un-selectable cursor). (Opus-reviewed `838197f`.)
- **D1-r (Low-Med)** — **done** (`2ceed1c`): `merge_batch` now detects a batch column absent from the Delta table (additive evolution) and **falls back to `delete_then_append`** — the same key-based upsert, but its append carries `SchemaMode::Merge`, so the new column is captured instead of silently dropped by the MERGE op's fixed clauses. Local-FS discriminating test proves the new column persists (with the pre-evolution row correctly NULL, distinguishing backfilled-vs-dropped); normal (no-new-column) merge path untouched. Full 28-test Docker suite green (Opus-reviewed). Also **D1-r2 (Low)**: a column RENAME (drop+add) fails with the misleading "table was dropped" wording (safe — needs full refresh — but confusing). **D1-r3 (Info)**: always-Merge runs a proven-no-op schema reconciliation on every append; unbenchmarked on large multi-batch incremental loads.
- **D2, D3** — **done** (`9bc09a6`, `3e39029`, + discriminating test; Opus-reviewed). D3: freshly-derived two-stream seed persisted immediately via a validated zero-action HWM-only commit (delta-rs 0.32.4 supports it). Reachability note: the genuine data-loss window is a config-TABLE_HWM first run where both streams write nothing, then TABLE_HWM removed before the next run — the fix is sound defense-in-depth for it.
- **M4** — **done** (`b9de90c`): config load detects total RAM via `/proc/meminfo` (`detect_total_ram_mb`, std-only; `None`/skips on non-Linux) and `validate_memory_budget` **bails** when `TARGET_MEMORY_MB` or `MERGE_MEMORY_MB` strictly exceeds physical RAM, plus a `warn!` when `TARGET_MEMORY_MB > RAM/2` (breaker's 2× resident ceiling, ~4× unsigned-heavy → likely OOM). Called from both `load()`/`load_local()`; 5 unit tests on the pure validator (boundary/undetectable/both bails). Container caveat (reports HOST total, not cgroup limit) documented — acceptable for the VM target.
- **CF1, CF2** — **done** (`4c89bd7`, Codex).
- **S1** — **done** (`7240d15`): hand-written `Config` Debug masks `database_url` + `s3_secret_access_key`; all non-secret fields shown; regression test asserts the raw password/secret never appear.
- **S2** — **done for the extraction path** (`5f752bc`): `backtick` doubles embedded backticks; HWM `updated_at` doubles single quotes; tested. **S2-r (Low)**: verify's SQL builders (`verify/source.rs`, `verify/delta.rs`) already `.bind()`/escape all VALUES but still inline ~58 backtick-identifier sites — deferred (a large mechanical sweep, no unit coverage, and identifiers there are the same config/schema-derived names).
- **S3** — **done** (`cc20fd6`): `mask_secret` is char-based (no panic on multibyte secrets); multibyte + short-value regression tests added.
- **P1 (writer half)** — **done** (`68fd322`): `DeltaWriter` now holds a per-table `DeltaTable` handle cache (`Arc<tokio::sync::Mutex<HashMap<..>>>`); the four per-batch write paths (`append_batch`, `append_two_stream`, `merge_batch`, `delete_then_append`) TAKE the cached handle and STORE the post-commit handle back instead of rebuilding + full `_delta_log` replay every call. Single-writer-per-table + store-back ⇒ the cache is always current (no `update_incremental` needed); on a write error the entry is absent → next call fresh-loads. Once-per-table paths (`open_table`/`ensure_table`/`overwrite_table`/`commit_hwm_only`/`read_hwm`/`read_insert_hwm`/`has_data`/`get_schema`) left on fresh load. No public/trait/mock change. New local-FS coherence test (3 cached appends → 6 rows, 6 distinct) + the pre-existing multi-append/`delete_then_append` local tests + the full 20-test Docker suite (real MariaDB+MinIO, two-stream/incremental/schema-merge/full-refresh/crash) all green (Opus-reviewed, Docker-proven).
- **P1-r-a (connection pooling)** — **done** (`8fd0000`): `extract()` reuses one pooled `mysql::Conn` across a table's windows via `extract_once` + `open_connection`. **Discard-on-truncation/error**: a breaker-truncated or read-erroring window drops the connection instead of pooling it (server aborts on socket close) — behaviorally identical to the old fresh-conn-per-call, so M2 semantics are preserved by construction; reuse only changes the clean, fully-drained common path. **Retry-once-on-stale-reuse**: a pooled connection that fails is dropped and the extract retried once fresh (robustness parity with the old always-fresh path). Investigation measured the payoff at ~1 ms/window incl. prep+fetch (handshake is a fraction) locally — done at the maintainer's request for remote-DB latency + source connection-churn reasons. New Docker test `extractor_reuses_pooled_connection_across_windows` (10-window keyset loop over 5000 rows, asserts exact row count) + full 21-test Docker suite green (Opus-reviewed, Docker-proven). Residual **P1-r-a2 (Info)**: discard-on-truncation path not directly Docker-forced (correct by construction; the harness hardcodes `target_memory_mb`, so triggering the breaker mid-extraction needs a bespoke large-window fixture).
- **P1-r-b (Low)** — once-per-table read/create paths (`open_table`/`ensure_table`/`overwrite_table`/`read_hwm`/`read_insert_hwm`/`has_data`/`get_schema`/`commit_hwm_only`) still fresh-load; negligible (called once per table).
- **pf1** — **done** (folded into O3, `e3395da`): the arm is now a real "nullable <ts> (unsafe cursor)" reason; remaining unreachable!()s verified genuinely unreachable.
- **V5** — verify schema check compares column *names* only; types read but unused. **Reassessment (2026-07-15, on investigation — cost/risk higher than the Medium label):** a proper type check needs a cross-vocabulary compare (source MariaDB type-string vs Delta Arrow `DataType`) reusing the run's `mariadb_type_to_arrow`+`types_equivalent`, which requires threading the Delta `DataType` + source `COLUMN_TYPE` through `ColumnMeta` (**~63 construction sites** — heavy churn) AND it **duplicates the run+preflight evolution guards** (O7-rest-b: `--check` and the run already BAIL on a type change, so parket never writes a type-mismatched Delta table — V5 only backstops external Delta tampering) AND carries **false-positive-Discrepancy risk** on unsigned/edge types if `types_equivalent` doesn't perfectly round-trip. Shares the V6-r shape (verdict-gating an un-parity-hardened comparison). **Recommendation: defer** unless the independent-backstop value is specifically wanted; if pursued, do it conservatively (confirmed-incompatible KNOWN types → Discrepancy; unmappable → warn) with the full Docker suite as the false-positive gate. **RESOLVED as diagnostic** (`7d90cd2`, maintainer decision): verify now PRINTS a coarse column-type-**family** drift line (`schema TYPE DIFF (diagnostic — does not affect verdict)`) for columns whose family differs (int/float/string/binary — decimal/date/datetime/json/enum/set all map to Arrow `Utf8`→`string` in this pipeline, so they're grouped as string to avoid false positives), WITHOUT flipping the verdict. Zero code churn (uses the existing `type_str` on both sides; no `ColumnMeta`/probe changes), zero false positives (full 33-test Docker suite: 0 spurious TYPE DIFF, verify.rs-only). The verdict stays gated by the parity-hardened schema-name/key-set/value-aggregate checks; a genuine type change still can't reach a synced table (run + `--check` evolution guards bail), so the diagnostic surfaces external Delta tampering. Documented in `docs/verify-checks.md`. **Closed.**
- **V6** — **done** (`a52236a`): verify's deep-mode row sample now spans the LOWEST half + HIGHEST half of the id range (`sample_ids` UNION-deduped, 50 low + 50 high of SAMPLE_SIZE=100) instead of only the lowest 100 — recently-synced rows (highest ids), where the value-aggregate check can't help for non-numeric columns, are now spot-checked. Docker-proven: a high-id VARCHAR corruption the old lowest-100 sample would have missed is now surfaced by `sample_rows`. Full 33-test Docker suite green. (Small-table behavior unchanged — UNION dedups when rows < SAMPLE_SIZE.)
- **V6-r (Low-Med)** — **finding (registered, not fixed):** the row-**sample** AND **non-null-census** layers in `run_one_table` are DIAGNOSTIC-ONLY — a sample/census mismatch is `println!`-ed (`sample: ... differ=N`, `non-null census: DIFFERS`) but NEVER flips `outcome` to Discrepancy (only schema, key-set, and value-aggregates gate the verdict). So V6's wider sample improves the logged OUTPUT but a corrupted recent row still doesn't fail the exit code. Making these gate the verdict is a `verify.rs` change that needs the SAME value-parity hardening the value-aggregates got (DECIMAL(65)/native-scale/BINARY) — the sample compares stringified values across MySQL+DataFusion, so naive verdict-gating would false-alarm on representation diffs (decimal/datetime/float formatting). Deliberate design (diagnostic layer); a real fix is a scoped parity effort, not a bolt-on. **RESOLVED as diagnostic** (`7d90cd2`, maintainer decision): the sample + non-null-census diff lines now carry an explicit `(diagnostic — does not affect verdict)` marker so operators aren't misled; combined with V6's wider sample (recent rows now compared), differences are RAISED in output with the per-column/per-id detail, but by design do not gate the verdict (they compare stringified cross-engine values; the parity-hardened value-aggregates gate value correctness). Documented in `docs/verify-checks.md`. **Closed** (a future verdict-gating version would need the value-parity hardening — left as a deliberate follow-up, not required).
- **V8** — **done** (`1fa08c6`): verify's key-set MIN/MAX now cast to `DECIMAL(20,0)` and are held as **i128** (was `CAST AS SIGNED`/`bigint` into `i64`, which wrapped a `BIGINT UNSIGNED` value above 2⁶³ to a negative → corrupted fingerprint → false/misleading verdicts). The incremental-scope predicate (`CAST(key AS DECIMAL(20,0)) <= bound`) and the Delta **integer value-column** MIN/MAX (`decimal(65,0)`, symmetric with the source side + VA1-r) get the same range-safe treatment; `xor` stays `i64` (bit-preserving, injective even for wrapped keys); `latest_key_stats` computes MIN/MAX from a `key_dec` decimal column while keeping `key_value` bigint for xor/row_number. Reachability: N5 blocks writing a > i64::MAX key to Delta, but the live SOURCE can hold one (rows added since sync), and verify's `source.key_stats()` aggregates over it. New source-probe Docker test (u64::MAX key reads back exactly, not wrapped) + a u64-range `key_stats_outcome` unit test; parity for keys ≤ i64::MAX confirmed across the full 30-test Docker suite (Opus-reviewed, Docker-proven).
- **V8-r (Low)** — the deep-sample spot-check key cast (`source.rs` sample query `CAST(id AS SIGNED)`, `delta.rs` `cast(id as bigint)`) still narrows a u64 > i64::MAX key; deferred (the sample path is a lowest-N-ids spot check — V6-adjacent — and a key that large is above the sampled window anyway).
- **O12** — **done** (`1b89161`): the `--verify` mode-resolution was a third divergent copy that read explicit `TABLE_MODE` only, so an auto-detected-incremental table (id + non-null timestamp, no `TABLE_MODE`) verified as `Basic` (weaker, unscoped → false confidence). Extracted `discovery::resolve_ts_col_and_mode` as the SINGLE source of truth (behavior-preserving lift of the orchestrator's inline recipe); the run and `--verify` now both call it, so they can't disagree. Verify discovers columns + resolves identically (degrades to Basic + warn only on discovery/resolve failure). 5 unit tests on the resolver + a discriminating Docker test (resolver returns Incremental for the no-`TABLE_MODE` table; incremental-scoped verify excludes post-HWM source rows → Clean). Full 22-test Docker suite green (Opus-reviewed, Docker-proven).
- **O9** — **done** (`eb48a5f`): (1) sticky **`last_success_at`/`last_success_rows`** added to `TableState` (`#[serde(default)]` for old-state.json compat) — set only on a genuine `success` run and carried forward by `AppState::update_table` on failed/interrupted runs, so a later failure never erases the record of when the table last synced cleanly (previously `update_table`'s full-replace wiped it). (2) removed the never-read **`schema_columns_hash`** field + the now-dead `compute_schema_hash` helper (+ its 6 unit tests) — chose DROP over consume: the schema-evolution check already handles schema changes functionally, and the orchestrator holds no prior state to compare against without new trait plumbing. New `state.rs` carry-forward unit tests; a compiler-forced one-literal update in `adapters.rs` test. Not Docker-run — pure post-extraction state.json metadata (no data/extraction/run-behavior change), fully unit-covered incl. the real `write_atomic` path.
- **O11** — **done** (`a09642e`): `resolve_ts_col_and_mode` now validates an explicit `TABLE_TIMESTAMP` only when it's actually used as the incremental cursor — validation is skipped when the table is EXPLICITLY configured for a mode that never reads it (`TABLE_MODE=full_refresh` or two-stream). An invalid cursor that would otherwise leave the table incremental-eligible STILL fails fast (the `explicit_timestamp_cursor_on_filtered_time_column_bails_actionably` orchestrator test is preserved — that fail-fast is desirable, not the bug). 3 resolver unit tests. **Preflight adopted the shared resolver** (removed the last inline mode-resolution copy), so `--check`/run/verify are now unified — this also closes O7's mode-parity sub-item. Full 22-test Docker suite green (Opus-reviewed).
- **T2–T5** — ~~corruption/scope/NULL/multibyte test coverage~~ **done** (`108d9e3`).
- **T6** — **done** (`87db089`): Docker coverage added for the four previously-unproven verify outcome paths — the **two-stream verdict** (Clean, then Discrepancy via source-grew hitting the asymmetric else-branch), the **Drift tier** (`source advanced past sync`, asserted through the now-`pub run_one_table` since `run()` rolls Drift up to Clean), the **size-guard Skipped tier** (`with_row_cap(5)` + !deep on a 10-row table → Skipped; deep bypasses it → Pass), and a **VARCHAR-only value drift** (per-column value-aggregate catches a text-column change with unchanged ids). Two additive test-enablers in verify.rs (`pub run_one_table`, `with_row_cap`) — `run()`/outcome logic untouched. Full 28-test Docker suite green (Opus-reviewed, Docker-proven).
- **T6-r** — **done / resolved-as-unreachable** (`6c915d2`). (a) **keyset full-refresh page boundary (C1): now Docker-covered** — `full_refresh_keyset_pagination_crosses_page_boundary_exactly_once` forces 5 keyset pages over a 300-row integer-PK table (N8's page-forcing trick: an 8000-char filler column + `ANALYZE TABLE` + `target_memory_mb=1` shrinks `calculate_batch_size` to 60/page) and asserts exactly 300 rows / 300 distinct ids — no skip or duplicate across the 4 boundaries crossed. (b) **R2 HWM-no-progress bail: UNREACHABLE BY CONSTRUCTION, not missing coverage** — verified in source, three independent invariants each block it: the incremental predicate (`query.rs:72`) is `(ts = hwm AND key > last_id) OR (ts > hwm)`, so every row MariaDB returns strictly exceeds the current HWM in `(ts, key)` order and therefore so does their max; `validate_timestamp_col` (`discovery.rs`) admits only `timestamp`/`datetime` cursors, whose Arrow round-trip via `extract_timestamp_as_strings` covers every unit connector_arrow emits without precision loss; and N7's `ts_components` comparison preserves chronological ordering, so no formatting tie/reversal can occur. The guard is additionally gated on `batch_hwm.is_some()`, so the unextractable-cursor route produces the *None* bail instead (reachable, and covered by N2-r's Boolean-cursor unit test). **Keep the guard as defense-in-depth** — it is unreachable *given those invariants*, so it becomes live again if any of them changes (a future non-timestamp cursor type, or a comparison regression); it is not dead code to delete.

## 5. Open — Low
- **N1-u** — upstream follow-up: connector_arrow `create_field` `todo!()` → proper `ConnectorError` (offered in PR #79's description; unreachable from parket since `ee09a7f`).
- **N1-r2** — the allowlist↔mapping sync test proves allowlist ⊆ mapping + spot-checks known-excluded types; a NEW mapping arm added without allowlisting would be silently skipped (soft failure), not caught. Also: a source column ALTERed from an extractable to a non-extractable type yields a misleading "exists in Delta but not in MariaDB" evolution message (safe bail, wrong wording).
- **L1** calendar: O(|years|) loop + `i64` overflow near extremes (`calendar.rs:7-19`).
- **L2** — **already resolved** (verified 2026-07-16): `format_naive_datetime` (`writer/datetime.rs:85-86`) uses `div_euclid`/`rem_euclid`, the correct negative-timestamp handling — the truncating-vs-euclid mismatch was fixed (stale register entry).
- **L4** — DEFERRED (Low, marginal): dedup `ROW_NUMBER … ORDER BY key` keeps an arbitrary row among same-key dupes; a within-window PK appears once so this is near-unreachable, and 'order by version DESC' needs a version column the batch may not have.
- **L5** — **done** (`86a16f3`): removed the dead `check_updated_at_index` method (no callers — the index-hint feature was never wired up), eliminating the hardcoded `updated_at`.
- **L6** — **done** (`7d186cc`): `extract_hwm_from_batch` folds to the max in one pass over the zipped vectors (removed the transient `candidates` Vec); behavior-preserving.
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

---

## 7. New audit — 2026-07-15 (fresh pass over v0.2.0; `fable` model, Opus-spot-verified)

A second independent audit pass over the **post-v0.2.0** code (the entire §1–§6 Critical/High/Medium
set is already resolved). Fresh-eyes review; excludes everything already resolved/registered above.
IDs prefixed **`FA`** (fresh audit). Confidence is the reviewer's; **[Opus-verified]** marks findings
the orchestrator independently confirmed against the code. These are CANDIDATES pending the
plan→implement→review remediation loop; none fixed yet.

### 7.1 Open — Critical
*(none — the prior audit's Critical class re-traced and confirmed still closed.)*

### 7.2 Open — High
- **FA1** — **done** (`db45d91`). [Opus-verified] silent NULL-corruption on full-refresh type drift. `coerce_batch_to_schema`
  (`writer.rs:73-93`, used by `stage_overwrite_chunk`) casts each batch column to the *existing* Delta
  schema with arrow's plain `cast()` = `safe: true` → an out-of-range/unparseable value becomes **NULL**
  instead of erroring. Full refresh never runs `schema_evolution_check` (`orchestrator.rs:310-319` gates it
  to `Incremental|TwoStream`; `_ => column_names`). So a full-refresh table whose source type drifted vs its
  Delta column (e.g. `int→bigint` after ids pass 2³¹; or the register's own N5 "full-refresh to rebuild"
  migration for a pre-N5 `int unsigned` table where Delta is Int32 but batches arrive Int64) silently writes
  NULLs for every non-fitting value, exit 0. Directly undoes N5's discipline (`align_batches_to_schema`
  uses `CastOptions{safe:false}` and errors by table+column one step earlier). **Fix:** use
  `cast_with_options(.., {safe:false, ..})` in `coerce_batch_to_schema` (mirror `align_batches_to_schema`). **Resolved:** `coerce_batch_to_schema` now casts with `CastOptions{safe:false}` — a data-losing narrowing errors loud+actionable instead of NULLing; healthy full-refresh (identity/widening) unaffected (full 33-test Docker suite green); unit test covers widen-ok/narrow-errors.
- **FA2** — **done** (`c6ea932`). [Opus-verified] two-stream cross-window row duplication. The update stream (Stream B,
  `orchestrator/two_stream.rs:199-267`) is bounded only by the UPDATE cursor — no upper bound on the insert
  key. A row `id=Y` inserted after Stream A's window (insert watermark `X`, `Y>X`) whose `update_col` lands
  in Stream B's window is appended by `delete_then_append` (delete finds nothing → append Y); the commit
  keeps `insert_id=X`. Next run, Stream A extracts `id > X`, re-appends Y → **duplicate that persists** until
  Y is updated again. Verify's `two_stream_key_stats_outcome` grades delta-count>source-count as **Drift**
  (→ Clean), so it's invisible to the exit code and `--verify`. **Fix:** cap Stream B at the insert
  watermark (`AND {insert_col} <= {hwm_id}`) — rows beyond it belong to the next run's insert stream. (Do
  NOT advance the insert watermark past Stream B's max — that would lose not-yet-completed rows in `(X, maxB]`.)
  Optionally make `two_stream_key_stats_outcome` treat `delta count > delta distinct` as a loud diagnostic. **Resolved:** `build_incremental_query` gained an optional `key_upper_bound`; Stream B now passes the insert watermark `hwm_id`, so a row inserted past it (belonging to the next run's Stream A) isn't updated+appended now and re-appended next run. Plain incremental passes `None` (byte-identical, verified). In a quiescent DB the cap excludes nothing (Stream A ends at source max) so no behavior change — the race isn't deterministically Docker-reproducible; proof is the query-cap unit tests + a mock test asserting Stream A is uncapped while Stream B carries `AND \`id\` <= <hwm>`. Full 36-test Docker suite green, no regression. **FA2 diagnostic follow-up — DONE** (`37e7dd0`): the optional tweak is implemented. `duplicate_key_surplus`
  (a pure, single-engine invariant check) flags `count > distinct` on the **two-stream path only** — two-stream
  keeps exactly one row per key, so a surplus IS duplicated rows, whereas an incremental append-log legitimately
  holds many versions per id. This closes the blind spot that hid FA2: the verdict grades a Delta surplus as
  Drift, which `run()` folds into Clean, so duplication could not reach the exit code. **Diagnostic-only by
  deliberate choice** — not parity caution (there is none here; both numbers come from the same Delta-side
  aggregate) but *legacy-data* caution: a table synced before `c6ea932` may still carry residual duplicates and
  hard-failing would regress existing deployments instead of reporting. The line names the surplus and points at
  the `TABLE_RECONCILE` one-shot as remediation. **FA2-r2 (Low, follow-up) — EVIDENCE GATHERED 2026-08-05, promotion now justified:** frontier parity on
  `developer_journey_trackings` (the ONLY two-stream table in the live config, and the one that ran pre-fix) is
  **EXACT** — Delta Σ live-file `numRecords` = **114,412,210** (25 live parquet, log v36) vs source
  `COUNT(*) WHERE id <= hwm_insert_id(500147847)` = **114,412,210**. Zero net surplus ⇒ no duplicate rows and no
  in-scope deletes, so promoting the diagnostic to `Discrepancy` would not fail any existing table. Caveat: this
  is a NET row count, not `count` vs `count(distinct id)`, so it rules out duplication only up to the improbable
  case of duplicates exactly offset by deletes; a true distinct-count needs a full Delta parquet scan
  (`--verify --verify-deep`, runbook-reserved for off-peak post-reconcile). **DEFINITIVE 2026-08-05** — the net-count caveat above is now
  CLOSED by direct measurement: the new `examples/delta_key_census` (Delta-side only, no source-DB load)
  reports **count = 114,412,210 == distinct = 114,412,210**, with `xor == distinct_xor` and
  `max(id) = 500147847` exactly equal to the log's `hwm_insert_id`. `count == distinct` rules out
  duplication outright, so the "duplicates offset by an equal number of deletes" loophole is eliminated
  rather than merely improbable. Four independent measurements agree (parquet scan, `_delta_log`
  `numRecords` sum, source frontier count, and the HWM). **FA2-r2 — DONE** (`53cd1bc`, operator-approved
  2026-08-05): the check is promoted from diagnostic to **verdict** — `count > distinct` on a two-stream
  table now yields `Discrepancy`, so duplication reaches the exit code. It moved INSIDE
  `two_stream_key_stats_outcome` (pure + directly unit-testable) and runs FIRST so it dominates the Drift
  arm — the arm that, combined with `run()` folding Drift into Clean, is exactly how the original FA2
  duplication stayed invisible. A regression-guard test asserts the promotion did NOT break the legitimate
  two-stream Drift path (extra *distinct* ids enclosing source's range: `count == distinct`, no surplus →
  still Drift). 664 lib tests, clippy `--all-targets` clean, all 7 two-stream Docker tests green.
  **The FA2 blind spot is now closed at its root.**

### 7.3 Open — Medium
- **FA3** — **done** (`878d051`). Full refresh never evolved the Delta schema (confirmed). `coerce_batch_to_schema` iterates only
  *target* fields (extra batch columns silently dropped, no log); `commit_overwrite` emits only Remove+Add,
  no `Metadata` action, so schema can't change. Consequences: (a) a NEW source column is extracted then
  silently discarded every run (Incremental/TwoStream capture it via D1's `SchemaMode::Merge`; full refresh
  doesn't); (b) a DROPPED source column makes `coerce` error every run with the misleading "column expected
  by the Delta schema is missing from the extracted batch" — permanent, no remediation short of deleting the
  Delta dir; (c) the register's N5 "full-refresh to rebuild" advice doesn't actually rebuild the schema (and
  per FA1 silently NULLs the very values it was meant to fix). **Fix:** emit a `Metadata` action on
  overwrite when the schema differs (delta-rs `Overwrite + SchemaMode::Overwrite`), or at least warn (new
  col) / bail accurately (dropped col) in `process_full_refresh`; correct the N5 migration note. (Pairs with FA1.) **Resolved:** the staged overwrite now targets the current SOURCE schema — `begin_overwrite` compares it to the stored schema in Delta StructType space (N5-widening-safe), and when it differs builds the writer with the new schema (`RecordBatchWriter::try_new`) and `commit_overwrite` folds an `Action::Metadata(meta.with_schema(new))` into the same atomic commit → added columns appear, dropped disappear, type widenings adopted; unchanged-schema path is byte-identical (`for_table`, no Metadata). Docker-proven (add/drop/widen), full 36-test suite green. **N5 migration note now VALID** — a full refresh genuinely rebuilds the schema (and with FA1's safe cast, adopts the wider type rather than NULLing).
- **FA4** — **done** (`ba36a35`). `delete_then_append` (the DEFAULT two-stream update strategy) previously
  deduped in an UNBOUNDED DataFusion session (`SessionContext::new()`, no `FairSpillPool`/spill dir) and
  `.collect()`s the deduped output while the input `MemTable` is still resident → transient peak ~2–3× the
  window (so ~4–6× `TARGET_MEMORY_MB` with the breaker's 2× admission), uncovered by M4's RAM validation.
  `merge_batch` (the opt-out path) was hardened with a bounded pool; the default path was not — OOM-risk on
  the 8 GB target for large update windows. **Resolved:** extracted `merge_batch`'s bounded-session
  construction into a shared `build_bounded_session` helper (`FairSpillPool(merge_memory_mb)` + MERGE_SPILL_DIR
  + spillable SortMergeJoin + single partition) and used it in `delete_then_append` too, so the default path's
  dedup ROW_NUMBER sort spills to disk instead of OOMing; `merge_batch` behavior-preserving (same session).
  Full 36-test Docker suite green. **FA4-r (Low):** the `.collect()` still materializes the deduped window
  alongside the input MemTable (~2× window, bounded by the breaker) — dedup-without-materialization
  (distinct-keys-first, re-materialize only on actual dupes) is a deferred perf optimization, not required to
  close the OOM-risk (the bounded pool + spill does).
- **FA5** — **done** (`7ce3132`). Unquoted, case-normalized identifiers in `merge_batch`/`delete_then_append`
  DataFusion SQL + `col()` exprs. DataFusion normalizes unquoted identifiers to lowercase → a mixed-case
  column (`userId`), a reserved-word column (`order`), or a source column named like the dedup alias failed
  the table on every update window (hard error, not corruption). **Resolved:** the dedup SQL now
  backtick-quotes column identifiers (reusing `query::backtick`, made `pub(crate)`); the merge
  predicate/update-insert value exprs + delete predicate use non-normalizing `Expr::Column(Column::new(...))`
  instead of `col(format!(...))`; the rownum alias is collision-proof (`dedup_rownum_alias` picks a name not
  among the columns). Verified the merge `.update/.set` TARGET-column arg already preserves case
  (`DeltaColumn::from` → `Column::from_qualified_name_ignore_case`), so only the source value-expr needed the
  fix. Lowercase behavior unchanged. Docker-proven under BOTH strategies (`delete_then_append` +
  `UPDATE_STRATEGY=merge`) with a mixed-case `userId` + reserved-word column; full 38-test suite green.

### 7.4 Open — Low
- **FA6** — **done** (`7d186cc`): `calculate_batch_size` resets the `adapted` latch per table so tables 2..N re-adapt. Was: `BatchExtractor.adapted`
  (`extractor.rs:20,149-151`) latches on the first non-empty batch ever and never resets, so tables 2..N
  keep a mis-sized LIMIT (repeated breaker truncations). **Fix:** reset `adapted=false` in
  `calculate_batch_size` (called once per table).
- **FA7** — **done** (`7d186cc`): `extract_id_as_i64` returns `None` on any NULL key slot (fast `null_count()` check) instead of reading 0. Was: ignored NULL validity → a NULL key slot reads as
  0. Reachable only for a nullable fallback `id` key (explicit `TABLE_MODE=incremental`, no integer PK):
  yields `last_id: 0`, re-extracting rows at the max timestamp → duplicates. **Fix:** skip null slots / return None on null.
- **FA8** — **done** (`7d186cc`): `delete_then_append`'s UInt64 key collection uses checked `i64::try_from` (bails on >i64::MAX) instead of wrapping `as i64`. (`writer/two_stream.rs:328-333`).
  Unreachable from the orchestrator (`align_batches_to_schema` errors on >i64::MAX first) but a public-API
  hazard and inconsistent with `extract_id_as_i64`/`extract_batch_max_key`. **Fix:** `i64::try_from` with an error.
- **FA9** — **done** (`86a16f3`): `Config::load`/`load_local` now bail on an unrecognized `UPDATE_STRATEGY` / non-numeric `MERGE_SORT_RESERVATION_MB` / non-positive `MERGE_TARGET_PARTITIONS` (validate_advanced_env_knobs) — a typo is loud at startup instead of silently ignored. Was: `UPDATE_STRATEGY`
  (`orchestrator/two_stream.rs:251`, exact-match `"merge"` — a typo silently selects the default);
  `MERGE_SORT_RESERVATION_MB`/`MERGE_TARGET_PARTITIONS` (`writer/two_stream.rs:97-116`, unparseable silently
  ignored). Bypass `Config` entirely (invisible to `--check`). **Fix:** parse/validate in `Config::load`, thread through.
- **FA10** — **done** (`86a16f3`): `max_timestamp`/`count_null` route identifiers through `query::backtick` (S2 doubling on the extraction path). Was: interpolated WITHOUT the S2
  backtick-doubling — on the EXTRACTION path S2 claimed done (vs the verify path S2-r defers). Robustness
  (schema/config-derived names), not injection. **Fix:** route through a shared `backtick()` helper.
- **FA11** — DEFERRED (documented): a failed full refresh leaves its `OverwriteSession` (RecordBatchWriter buffers + accumulated Add
  metadata + table handle) resident until process exit (`writer.rs:112,356-371`; failure paths in
  `orchestrator/full_refresh.rs` never drain it). Bounded by table count; modest unaccounted retention.
  **Fix:** an `abort_overwrite(table)` on the table-failure path (also the natural spot for the vacuum-hint log).
- **FA12** — **done** (`86a16f3`): (a) `UInt32→LONG` not INTEGER (defense-in-depth; was unreachable but a real type hole); (b) `--inspect` now checks for any single-column integer PK (`select_integer_pk`) so a non-`id`-PK table gets an incremental recommendation, not a misleading full_refresh; (c) an interrupted full refresh reports 0 committed rows (nothing was committed). Original: `writer/schema.rs:28` mapped `UInt32→INTEGER` (range-lossy; currently unreachable
  — unsigned is widened first — but a defense-in-depth hole, map `→LONG`); `inspect.rs:78-79` still
  recommends full_refresh on "No `id` column" pre-dating N3-r's integer-PK generalization (O13-adjacent); an
  interrupted full refresh records the *staged* (uncommitted) row count in `state.json last_run_rows` (mildly misleading).

### 7.5 Coverage note (from the fresh pass)
Re-traced and found solid: the incremental loop (HWM ordering, N2-r/R2 guards, has-data guard, D2),
extractor/breaker (cumulative accounting, widening weights, discard-on-truncation), the atomic-overwrite
COMMIT protocol (single-commit, abort-safe, cache eviction — FA1/FA3 are about the *coercion/schema* inside
it, not the commit), P1 caches (coherent under single-writer; no lock held across await except the harmless
sessions lock), state fsync/rename + sticky last-success, signal/exit-code mapping, secret masking, verify
SQL parity (DECIMAL(65)/i128/BINARY consistent on both probes), discovery/mode-resolution. Not fully
explored: the ~2,700 lines of test bodies (function lists only), vendored connector_arrow internals, and
whether delta-rs's Overwrite `CommitBuilder` does any protocol-level schema validation that would incidentally
catch FA3(a) (staged parquet is written against the old schema regardless).

## 8. Post-sync operational evaluation — 2026-07-16 (`fable` model, live DB + S3 verified)

Not a code audit: an evaluation of the RESULTS of the first full production sync (all 8 configured
tables, v0.2.1 release binary, 8 GB cgroup cap). DB tunnel was UP; evidence = `--check`/`--inspect`/
`--verify` (7 light tables + 4 forced-deep + trackings non-deep), all 8 `_delta_log`s parsed for exact
live-row counts + HWM metadata, read-only source `COUNT`/single-pass aggregates, `state.json`, `s3cmd
du`. IDs prefixed **`PS`** (post-sync). All findings are CANDIDATES / operator decisions — **none
actioned**.

### 8.0 Consistency verdict (evidence)
All 8 tables **consistent at the sync frontier**; Delta rows = Σ live-file `numRecords` from `_delta_log`:

| Table | Mode | Delta rows | Frontier/verify check |
|---|---|---|---|
| developer_journeys | full_refresh | 229 | `--verify` PASS (counts+aggregates+census+sample) |
| partner_programs | incremental | 577 | PASS; a later re-run appended 0 rows (idempotent resume) |
| developer_journey_completions | full_refresh | 1,018,523 | deep verify → DRIFT = expected snapshot lag (+114 post-sync inserts) |
| users | full_refresh | 1,438,175 | deep verify → DRIFT (+195 post-sync) — expected lag |
| multi_course_tokens | incremental | 1,623,997 | frontier `COUNT(id≤6765879)` **exact**; deep verify DISCREPANCY = **PS-M1 false alarm** |
| multi_course_token_courses | incremental | 4,576,482 | frontier `COUNT(id≤19504722)` **exact**; deep verify PASS |
| developer_journey_tutorials | full_refresh | 11,475 | `--verify` PASS |
| **developer_journey_trackings** | **two_stream** | **114,314,708** | frontier `COUNT(id≤499855374)` = **114,314,708 exact**; verdict SKIPPED (>1M cap, by design) |

HWMs read back exactly what each run wrote; every trackings commit re-stamps all watermark keys (so
latest-commit-only `read_hwm` is coherent). `state.json`: all 8 `success`; trackings `last_run_rows =
114,315,491` = 114,314,708 inserts + 783 updates. The four full_refresh fallbacks (nullable
`updated_at`) are the correct self-healing choice (O2-r single-commit overwrite = exact snapshot each
run); their per-run cost is trivial vs trackings (users 225 s/1.44M, completions 44 s, tutorials 51 s,
journeys 3 s). Memory: 2.58 GB peak vs 8 GB (peak at Stream B's FA4 bounded merge, not the bulk insert).

### 8.1 The `developer_journey_trackings` update-cursor blind spot (quantified)
Schema ground truth (`--inspect` + full-table aggregate): columns are `id, journey_id, tutorial_id,
developer_id, status(char), last_viewed, first_opened_at, completed_at, developer_journey_status_hash`
— **no `created_at`/`updated_at` at all**; `completed_at` is nullable **and unindexed**. So
`completed_at` was the only possible update cursor without a source schema change. It tracks
**completions only**. The blind spot is wider than the NULL warning implies:
- **NULL `completed_at` now: 22,920,651** (~22.92M; 22,920,128 in-frontier). **1,539,399 of them (~6.7%)
  demonstrably mutate** (`last_viewed > first_opened_at`) — invisible until/unless the row completes.
- **11,267,003 completed rows (12.3% of the 91.4M completed) have post-completion edits**
  (`last_viewed > completed_at`) that **never re-sync** — `completed_at` doesn't advance, so Stream B
  never re-selects them.
- `status` is not derivable from `completed_at` (20,136,001 rows `status='1'` with NULL `completed_at`;
  83,924 `status='0'` with a completion ts). Backdated `completed_at` (≤ HWM) and source-side DELETEs
  would also never propagate (none observed in the 2.5 h window; frontier parity stayed exact).
- **Self-healing works** (PS case 0): when a NULL row finally gets a `completed_at`, it jumps past the
  HWM and Stream B re-captures the *entire current row* — erasing accumulated drift on it (~374 rows
  healed in the hours after the sync). D3 first-run seed left no gap (persisted at v1 before either
  stream wrote; every later commit carried watermarks forward). `delete_then_append` on 783 keys is
  correct + idempotent (`writer/two_stream.rs:321-440`); FA2 cap held in production (v26 max id
  499855371 ≤ 499855374). This is exactly the documented **D2/O3** trade-off, re-warned every run.
- **Bottom line:** **exact if downstream consumes *completion facts*** (id/developer/tutorial/journey/
  completed_at) — inserts complete + every completion re-syncs; **silently decays if it needs *current
  engagement state*** (`last_viewed`/`status`/`status_hash`) — ~6.7% of NULL rows + every post-completion
  edit go stale between full reconciles.

### 8.2 Open — High
- **PS-H-A** (High, **operator decision**) — close the trackings update blind spot per §8.1. Options,
  best first: **(1)** source team adds `updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE
  CURRENT_TIMESTAMP` (+index), then a **config-only** cursor swap
  `TABLE_UPDATE_CURSOR_developer_journey_trackings=updated_at` (captures every mutation; needs a one-time
  reconcile baseline) — the permanent fix, but gated on a source schema change; **(2)** available today as
  a **one-shot config flag** (mechanics corrected 2026-07-25: PS-H-B `bd13be5` superseded the original
  manual `TABLE_MODE=full_refresh` + hand-seeded `TABLE_HWM` dance this entry used to describe) — set
  `TABLE_RECONCILE_developer_journey_trackings=true` for a single run **keeping the two-stream cursor vars
  in place** (no `TABLE_MODE` juggling, so no O5 conflict), then remove the flag: the table is extracted as
  a full snapshot and atomically overwritten (O2-r protocol — an interrupted run leaves the prior snapshot
  intact), and that commit is stamped with all three two-stream watermarks, so the NEXT run resumes cheap
  incremental **with no manual `TABLE_HWM` re-seed** (see `docs/config.md` "Per-Table One-Shot Reconcile").
  ~20–25 min, ~3.7 GB S3 rewrite, proven under 8 GB; also flushes never-propagated DELETEs and backdated
  timestamps. Residual staleness is then bounded by the chosen cadence; **(3)** compound/COALESCE cursor
  (e.g. `GREATEST(completed_at, last_viewed)`) — **rejected**, three independent reasons: `query.rs`
  cursors are backticked column NAMES not expressions (so it is a code change, not config), an expression
  cursor cannot use an index — and both candidate columns are unindexed — so every extraction window
  degrades to a full 114M-row scan, and it still cannot see DELETEs; strictly worse than (1) for more work;
  **(4)** accept-and-document — valid ONLY if downstream consumes completion facts (§8.1). The *loss* is
  tracked+accepted (D2/O3).

  **DECIDED (2026-07-25, maintainer):** downstream **does** consume engagement state (at least one of
  `last_viewed` / `status` / `developer_journey_status_hash` is read by a consuming app). Therefore:
  - **(4) ruled out** — accept-and-document is not available; the decay is not benign here.
  - **(2) ADOPTED** as the standing remediation: run the `TABLE_RECONCILE_developer_journey_trackings=true`
    one-shot on a fixed cadence, **default monthly** (add to the §8.5 runbook). The cadence IS the
    staleness bound: between reconciles, engagement state is stale for ~6.7% of the 22.9M NULL-`completed_at`
    rows plus the 12.3% of completed rows carrying post-completion edits (§8.1). Tighten the interval if the
    consumer's tolerance is shorter than that window — each reconcile is only ~20–25 min / ~3.7 GB.
    **Cadence refinement (2026-08-05):** the `last_viewed` drift is a FROZEN historical backlog (stopped
    advancing ~2025-02-03; re-measured above — 0 post-completion views among recent rows), so it does **not
    accumulate** and the one-time reconcile already executed 2026-07-18 healed it. A fixed *monthly* calendar
    reconcile is therefore likely over-provisioned; prefer **verify-driven**: use §8.5's cheap monthly
    frontier-parity check as the TRIGGER and reconcile only when it shows drift. Residual open question
    (unmeasured — `status` changes carry no timestamp, so source-only cannot give a rate): do `status` /
    `developer_journey_status_hash` still mutate post-completion? If they do, drift still accumulates through
    that channel and a periodic reconcile stays justified; a post-reconcile `--verify --verify-deep` on
    trackings is the instrument that would show it.
  - **(1) ESCALATED** — now the priority ask of the source team, not a nice-to-have: an `updated_at`
    (+index) is the only option that keeps engagement state *continuously* correct rather than
    correct-at-each-reconcile. Once it exists, the cursor swap is config-only and (2) can drop to a rare
    safety net.
  - **(3) rejected** — settled, reasons above.
  - **Cursor swap to `last_viewed`: INVESTIGATED AND REJECTED (2026-08-05, live-DB measured).** Two
    independent disqualifiers, either alone fatal: **(i) completion does not touch it** — over the newest
    ~2M id range (667,026 rows, 642,231 completed): `last_viewed < completed_at` for **636,857 (99.2%)`,
    `= ` for 5,374 (0.8%), `> ` for **0**, and `last_viewed IS NULL` for **0**. Since Stream B selects
    `WHERE cursor > hwm`, a completion would not move `last_viewed` at all, so completions would **stop
    re-syncing** — sacrificing the one property that is currently exact. **(ii) it is a dead cursor** —
    independently corroborates the earlier `fable` finding that `last_viewed` **stopped advancing
    ~2025-02-03**: it is still WRITTEN at row creation (0 NULLs) but never UPDATED afterwards, so as an
    update cursor it would catch ≈0 rows/day AND cannot heal the historical backlog. Nullability is NOT
    the differentiator here (`--inspect` confirms `last_viewed`, `first_opened_at` and `completed_at` are
    all nullable+unindexed → all three branded UNSAFE), which is why (i)/(ii) are the decisive reasons.
    **No cursor change can fix PS-H-A; the remedy is the reconcile.** Closed — do not revisit without new
    source-side behavior.
- **PS-H-B** — **done** (`bd13be5`). Was: stamp the snapshot max-cursor HWM on full-refresh's final commit
  (`orchestrator/full_refresh.rs` called `commit_overwrite(table_name, None)`) — fix (b) of H-2026-07-11-1,
  left optional and unimplemented; without it every full_refresh→two-stream round-trip (incl. the PS-H-A
  option-2 reconcile) needed a manual `TABLE_HWM` re-seed. **Resolved** — but the clean, correct fix turned
  out **medium, not "small":** a *forced* full_refresh strips the cursor vars, so it can't know the future
  two-stream cursors, and the two resume paths read different keys (`read_insert_hwm`→`hwm_insert_id`;
  `read_hwm`→`hwm_updated_at`+`hwm_last_id`). Implemented as a dedicated one-shot flag **`TABLE_RECONCILE_<t>=true`**
  (used alongside the retained two-stream cursor config, so no O5 conflict; see `docs/config.md`): it routes
  the table through the O2-r atomic-overwrite path AND stamps all three two-stream HWM keys on that commit —
  `hwm_insert_id` from a new `max_key_seen` tracker (NOT `last_key`, which is deliberately un-advanced on the
  terminal chunk → `None` for single-chunk tables; a real bug caught in review), `hwm_updated_at` from source
  `MAX(update_col)` (or a `1970-01-01 00:00:00` epoch sentinel when the cursor is currently all-NULL — edge
  fixed so reconcile *always* stamps, never silently leaving an unstamped commit that would bail next run),
  `hwm_last_id`=`i64::MAX`. Non-reconcile paths byte-identical (`commit_overwrite` gained an optional
  `insert_id`; `None,None` = prior behavior). Strictly validated (requires two-stream cursors; rejects a
  `TABLE_MODE` conflict). Gate: 641 lib tests, clippy `--all-targets` clean, coverage 92.54% lines. No Docker
  test added — reuses the already-Docker-proven O2-r overwrite + two-stream HWM stamping. This makes the
  PS-H-A option-2 reconcile a clean, mistake-proof one-shot (no manual seed).

### 8.3 Open — Medium
- **PS-M1** — **done** (`00afcd5`). Was: `--verify` false-alarms **DISCREPANCY** on update-active
  incremental tables. Observed live: `multi_course_tokens` deep verify gave `delta_latest = 1,623,997 >
  source_scoped = 1,623,980` with identical min/max — the 17 "extra" rows were updated *after* the sync
  (source `updated_at` moved past the HWM, out of `source_scoped`, while the synced version remains in
  `delta_latest`), a benign steady state that exited non-zero and cried wolf on alerts. **Resolved** — a
  dedicated `incremental_scoped_key_stats_outcome` (used ONLY at the incremental-scoped call site) grades
  a `delta_latest` superset whose key range is contained **within** `source_scoped`'s `[min,max]` as
  **Drift** (source rows advanced past the HWM after sync), reserving Discrepancy for a surplus **outside**
  the source range (`delta.min < source.min` or `delta.max > source.max`). The containment direction is
  the OPPOSITE of `two_stream_key_stats_outcome`'s (caught mid-implementation via TDD): an already-synced
  row that leaves the scope keeps a key inside source's envelope, whereas two-stream legitimately retains
  extra ids that *enclose* source's range. `key_stats_outcome` (full_refresh/basic, `verify.rs:915`) and
  `two_stream_key_stats_outcome` are **byte-unchanged**; a regression-guard test asserts the full_refresh
  path still flags a superset as Discrepancy. 5 new unit tests; gate 646 lib tests, coverage 92.58% lines.
  Accepted residual: a genuine phantom id *within* source's range is masked as Drift — narrow, since
  `delta_latest` is deduped and value-aggregates run separately. Follow-up **PS-M1-r** (§8.4).
- **PS-M2** (Medium, **ops/DBA**, no code) — **DEFERRED** (2026-07-18, maintainer decision): a source index
  is a **multi-team decision** (DDL on a shared, hot production table) and is out of parket's hands, so it's
  documented and parked pending that cross-team sign-off. Finding stands: add a source index on
  `developer_journey_trackings.completed_at` (ideally composite `(completed_at, id)` to also serve the
  tie-break `ORDER BY`). Stream B's window query AND the per-run D2 NULL census each full-scan ~114M rows
  every sync because `completed_at` is unindexed — **confirmed live at v0.2.2**: the PS-H-A reconcile and the
  post-reconcile incremental run (#4) both spent minutes on that unindexed scan (compounded by the tunnel's
  idle-connection churn). Trade-off: one-time online DDL (`ALGORITHM=INPLACE, LOCK=NONE` or
  pt-online-schema-change) + marginal write overhead on a hot table. Suggested DDL when approved:
  `ALTER TABLE developer_journey_trackings ADD INDEX idx_djt_completed_at_id (completed_at, id);`
- **PS-M3** — **done** (2026-07-18, one-time reclaim executed + cadence set). Was: establish a
  vacuum/housekeeping cadence; tombstoned parquet accumulates (reconcile/full_refresh leave old snapshots).
  **Executed:** after the PS-H-A reconcile, a force VACUUM of `developer_journey_trackings`
  (`retention_hours=0, enforce_retention_duration=false` via `deltalake` Python — parket has no vacuum
  command) reclaimed **71 dead files** (~1.7 GB; footprint 3.7 GB→2.06 GB, 25 live parquet). Retention=0 was
  a deliberate one-time choice: the reclaimed files were the pre-reconcile *drifted* snapshot (verified
  superseded), so dropping its time-travel was intentional; a standard 168h VACUUM reclaimed 0 that day
  (tombstones <7d). **L7 HWM-safety PROVEN in production:** the VACUUM added two no-HWM commits (v35/v36)
  that shadow the watermark, and the next `--check` logged `read HWM recovered from an older commit — 2
  newer commit(s) … skipped, skipped_commits=2` → recovered the correct latest HWM (`500147844 /
  2026-07-18T17:17:23`), resolved `two_stream`, no bail, exit 0. **Ongoing cadence:** monthly `VACUUM …
  RETAIN 168 HOURS` (default retention; L7 makes it safe) once tombstones age past 7 days.

### 8.4 Open — Low
- **PS-M1-r** — **done** (`6c915d2`): `verify_incremental_scoped_drift_on_already_synced_row_advancing_past_hwm`
  reproduces the true PS-M1 shape end-to-end under Docker — seed 5 rows, sync, then (no re-run) advance the
  **non-extremal** id=3 row's `updated_at` past the HWM, so it leaves `source_scoped` while remaining in
  `delta_latest` with its key still inside `source_scoped`'s own `[1,5]` envelope. Asserts
  `TableOutcome::Drift` (reason: "advanced past the HWM scope after sync") via the public `run_one_table`,
  since `run()` folds Drift up into `Clean` and a run-level assertion cannot distinguish Drift from Pass.
  **Same test also proves the fix is containment-based, not a blanket downgrade:** advancing the *extremal*
  id=5 row shrinks `source_scoped`'s max to 4 while `delta_latest` keeps 5 — now OUTSIDE the envelope — and
  the outcome is correctly `Discrepancy`.
- **PS-L1** — **done** (docs, `docs/config.md` "Two-Stream Recovery & Caveats"). The crash-between-DELETE-
  and-APPEND case (DELETE commit carries no HWM) **now largely self-heals** thanks to **L7** (`391210d`):
  `read_insert_hwm`/`read_hwm` scan back past the watermark-less DELETE commit to the last HWM-carrying
  commit, so the run resumes (no bail) and the next update window re-appends the rows the aborted APPEND
  missed (idempotent). Documented, with the manual `TABLE_HWM` re-seed retained only as the fallback for the
  rare ">64 watermark-less commits since the last real sync" edge.
- **PS-L2** — **done** (docs, `docs/config.md` "Two-Stream Recovery & Caveats"). Documented the two inherent
  timestamp-cursor caveats — the sub-second HWM boundary race and the backdated-cursor (≤ HWM) exclusion —
  as bounded by a periodic `TABLE_RECONCILE`/full refresh.
- **PS-L3** (Low, no action) — keep the four full_refresh tables as-is: correct self-healing mode for
  nullable-`updated_at` tables at this size. If the app ever makes `updated_at` NOT NULL, auto-detection
  flips them to incremental by itself — re-baseline HWMs when that happens.

### 8.5 Recurring verification plan (cheap-first; operator runbook)
- **After every sync (~10 min):** (1) `--verify` on the 7 light tables (expect PASS/SKIPPED; a
  DISCREPANCY with `delta_latest > source_scoped` + equal min/max = the benign PS-M1 signature); (2)
  `TABLES=developer_journey_trackings --verify` (schema + counts; verdict SKIPPED expected; healthy =
  source−delta gap ≈ new inserts, schema 9=9); (3) the most recent HWM-carrying commit **within the last 64**
  must have all three hwm keys (`hwm_insert_id`/`hwm_updated_at`/`hwm_last_id`). **CORRECTED 2026-08-05
  (post-L7 `391210d`):** `read_hwm`/`read_insert_hwm` now scan back up to 64 commits, so watermark-less
  commits AT THE HEAD of the log (VACUUM START/END, OPTIMIZE, checkpoints) are NORMAL and do **not** cause a
  bail — the old wording ("latest commit must carry the keys ⇒ else next run bails") would raise a FALSE ALARM
  today: production is currently v36=`VACUUM END`, v35=`VACUUM START`, newest HWM at v34, and is healthy. Only
  >64 consecutive watermark-less commits, or no HWM anywhere in the lookback, needs the PS-L1 re-seed; (4) `state.json` all `last_run_status == "success"`.
- **Weekly:** `--verify --verify-deep` on everything except trackings (≤4.6M each, ~3 min total).
- **Monthly — trackings reconcile (PS-H-A option 2, ADOPTED 2026-07-25):** engagement state
  (`last_viewed`/`status`/`status_hash`) is consumed downstream and the `completed_at` cursor cannot heal it,
  so run the one-shot: set `TABLE_RECONCILE_developer_journey_trackings=true`, run once (~20–25 min, ~3.7 GB
  S3 rewrite; the two-stream cursor vars STAY in place), then remove the flag — the next run resumes
  incremental automatically (watermarks are stamped on the reconcile commit; no manual `TABLE_HWM`). Follow
  it with `--verify --verify-deep` on trackings, off-peak. This cadence IS the staleness bound — tighten it
  if the consumer needs fresher engagement state than one month.
- **Monthly / pre-critical-use:** trackings frontier parity (far cheaper than deep verify on 114M):
  read `hwm_insert_id` from the latest commit → source `COUNT(*) WHERE id <= <hwm_insert_id>` (~2 min, PK
  range) vs Σ live-file `numRecords` from `_delta_log` (seconds). Any inequality = real drift (in-scope
  deletes or sync loss) → schedule the PS-H-A reconcile. Blind-spot estimator: track growth of
  `COUNT(completed_at IS NULL AND last_viewed > first_opened_at)` (now 1,539,399) and, if engagement
  state matters downstream, `last_viewed > completed_at` (now 11,267,003). Reserve full `--verify-deep`
  on trackings for right after each PS-H-A full reconcile, off-peak.

## 9. Low-priority batch cleanup — 2026-07-18 (pre-v0.2.2)

A sweep of the remaining actionable Low residuals across §4/§5/§7.4/§8.4, done in two reviewed
sub-agent loops (each gated: `cargo build` / `clippy --all-targets -D warnings` / `test --lib` /
`llvm-cov --lib --fail-under-lines 90`, Opus-verified independently). Full lib suite **662 passed**,
line coverage **92.59%**.

**Batch A — substantive (`391210d`):**
- **L7** — `read_hwm`/`read_insert_hwm` now scan a bounded 64-commit lookback (`find_commit_with_keys`)
  to recover the watermark past shadowing non-HWM commits (OPTIMIZE/VACUUM/checkpoint, or an aborted
  two-stream DELETE commit); falls back to `None` unchanged when none carries the keys. Unblocks the
  PS-M3 vacuum work and largely self-heals PS-L1.
- **FA11** — `abort_overwrite` drains a failed/aborted full-refresh `OverwriteSession` (idempotent
  remove) on every failure and shutdown path (`abort_full_refresh`), with a VACUUM hint when parquet
  was already staged. Successful commit path (incl. PS-H-B reconcile stamping) unchanged.
- **M2-r2** — incremental + both two-stream loops bail on a truncated window that fails to advance the
  cursor/HWM (mirroring the keyset full-refresh guard) instead of looping.

**Batch B — cosmetics/wording (`758a2b0`):**
- **S2-r** — verify SOURCE-side SQL identifiers routed through `query::backtick`.
- **N1-r2** — allowlist↔mapping sync test hardened (iterates the full known MariaDB type universe,
  exact `mariadb_type_to_arrow` ⟺ `EXTRACTABLE_DATA_TYPES` agreement); the missing-column evolution
  message no longer claims "table was dropped".
- **D1-r2** — same reworded message (a column rename/drop no longer mislabeled as a dropped table).
- **L1** — calendar date math is overflow-safe at `i64::MIN/MAX` (i128 400-year-cycle fast path).
- **O13** — `inspect` labels PRIMARY only on a real `key=="PRI"`; stdout/stderr flushed before the
  second-signal `exit(130)`; resolved absolute `state.json` path logged at startup.

**Docs:** PS-L1/PS-L2 written into `docs/config.md` ("Two-Stream Recovery & Caveats"); this §9 + the
resolved-markers (this commit).

**Still deferred (assessed, deliberately not done):**
- **V8-r** — the deep-sample key path is `i64`-typed end-to-end (`sample_ids -> Vec<i64>`,
  `sample_rows(&[i64])`, mirrored on both probe traits + mocks + the `verify.rs` comparison). A
  `u64 > i64::MAX` key can't be represented without re-typing that whole path to `i128`; a naive SQL
  `DECIMAL` cast breaks the `try_get::<i64>` decode for normal keys. Multi-file public-API change —
  out of scope for a cosmetics sweep; kept deferred (as the original §5 note already had it).
- **O13 / VA6** (`latest_key_stats` HWM-scoping) — scoping it would disturb the deliberate
  `source_scoped` vs `delta_latest` asymmetry `incremental_scoped_key_stats_outcome` (PS-M1) relies on;
  left to avoid false-positive verdict risk.
- **PS-M1-r, T6-r** — Docker integration-test follow-ups (out of the `--lib` loop; do in a Docker pass).
- Info/perf/upstream residuals unchanged: FA4-r, D1-r3, P1-r-a2/-b, N1-u, L4.

---

## 10. Production incident — 2026-08-06: `delete_then_append` write amplification

Found by running a real two-stream sync of `developer_journey_trackings` (115 M rows, ~2.4 GB on
S3 `ap-southeast-1`) from a workstation. Three attempts were made; each was initially
misdiagnosed as a network failure (one host suspend, one transient connect stall). They were not:
at ~40 minutes each run had applied roughly **3%** of its update window. The network errors
merely interrupted a run that could not have finished.

### 10.1 **FIXED and VALIDATED AT SCALE (`0c0c270`, v0.2.5)** — High: one full-table rewrite per 1024 update keys

> **Validated in production 2026-08-07** against the real 115 M-row table, after attempt 1
> (`fb5c4eb`, v0.2.4) failed and was rolled back. v0.2.4 remains a **pre-release**.
>
> ```
> commit_overwrite: atomic overwrite committed (single commit swaps entire snapshot)
>   version=90 files_removed=38 files_added=564
>   hwm_insert_id=502767312 hwm_updated_at=2026-08-07T15:26:53 hwm_last_id=502767294
> delete_then_append: applied via ONE atomic overwrite (anti-join), not per-chunk DELETEs
>   keys=12715 survivors=115273044 new_rows=12715
> run complete succeeded=1 failed=0
> ```
>
> | claim | result |
> |---|---|
> | ONE commit, not `ceil(keys/1024)` = 13 | **1** (version 90) |
> | memory bounded | **0.210 GB of an 8 GB cap (2.6 %)**, zero swap |
> | faster than the chunked path | **14.4 min** vs 13 × 4.05 = **~52.6 min** ⇒ **3.7×** |
> | correctness | census `count == distinct == 115,285,759`, matching `survivors + new_rows` exactly |
> | survivors preserved | 115,273,044 carried across untouched |
> | HWMs | both stamped in the single commit (insert + update) |
>
> Memory is the notable figure: **0.210 GB where attempt 1 died at 2,044.6 MB**, and ~8× lighter than
> the MERGE path's 1.757 GB. Removing the join did not merely fix the failure — it made this the
> cheapest of the three strategies.
>
> **Caveat carried forward → §10.1-r2/-r3 (staged-chunk size).** `files_added=564` where MERGE
> produces ~36, because `stage_overwrite_chunk` flushes per call and `STAGE_ROWS` was 200 000. First
> "fixed" by raising it to 5 M rows — **which was itself unsound**: a row count cannot bound memory
> (5 M rows is a different size for a 9-column table than a 40-column one), the 25× increase rested
> on an estimate, and it had never been run at any window size, so the validated 0.210 GB figure did
> NOT apply to it. Superseded by **-r3**: the budget is now `with_stage_bytes`, measured with
> `get_array_memory_size()`, defaulting to 256 MB — which bounds memory for any schema AND lands the
> file count at ~40 for a 115 M-row table. Mirrors the M2 circuit breaker, which already accumulates
> real buffer bytes for the same reason.
>
> **§10.1-r3 also closes the large-window gap without waiting for an outage.** Memory here has two
> independent axes: TARGET SIZE (the survivor scan streams — validated in production at 115 M rows)
> and KEY COUNT (`ids`, the `HashSet`, the fully-materialised `deduped_batches`, the staging buffer).
> The key-count axis needs no 115 M-row target, so
> `delete_then_append_overwrite_handles_production_scale_key_count` drives it at **858 000 keys** —
> the largest window observed in production — with a 2 MB staging budget to force many flushes, and
> asserts one commit, no duplicates, and the untouched remainder surviving. The 858 k post-outage case
> is therefore no longer unmeasured. The read penalty is selective: a bounded single-partition key census
> went from ~5 min to >10, while the `target_partitions=14` rollup was unaffected (93 s vs 104 s). So
> it degrades exactly the single-partition scans every memory-bounded path here relies on. The 564
> files become reclaimable in the §10.5 VACUUM.
>
> **Attempt 1's post-mortem retained**, because the reasoning error matters more than the fix:
> `fb5c4eb` claimed "the BUILD side is the small key set while the huge target STREAMS". False.
> `NOT IN (SELECT …)` carries three-valued-logic semantics ⇒ datafusion plans a **null-aware**
> LeftAnti join; `null_aware` is **HashJoin-only** (`…/hash_join/exec.rs:407`) and HashJoin does not
> spill (§2.2 of `two-stream-continue-update.md` had already tabulated that), so it hash-built the
> 115 M-row TARGET. `prefer_hash_join=false` — already set by `build_bounded_session` — cannot help,
> the null-aware variant having no SortMergeJoin implementation. It was **POOL-bound, not host-bound**
> (died on `MERGE_MEMORY_MB=2048` with cgroup RSS at 1.02 GB of 8 GB), so it would fail identically on
> a 46 GB host and the "does it fit 8 GB" framing measured the wrong variable. Blast radius was nil:
> `abort_overwrite` committed nothing, the 87→88 commit was the independent insert stream, the census
> was clean, and the update HWM never advanced so the window stayed retryable.
>
> **Why the unit suite could not catch either defect.** A 12 000-row fixture fits any hash build, and
> file-count effects are invisible at that scale — the same blindness this finding documents about the
> chunking, reproduced twice in its own fixes. `survivor_scan_sql_is_join_free` therefore pins the
> STRUCTURE (no join) rather than pretending a fixture-scale behavioural test would help; a
> pool-independence test was attempted and discarded (a 4 MB pool dies on the dedup sort's fixed 10 MB
> reservation before reaching survivor selection, and making it decisive needs ~1.8 M rows).

The original finding follows.

`DeltaWriter::delete_then_append` (`src/writer/two_stream.rs:429-446`) deletes the incoming keys
in chunks and issues **a separate `table.delete()` per chunk**:

```rust
const DELETE_KEYS_PER_CHUNK: usize = 1024;
for chunk in ids.chunks(DELETE_KEYS_PER_CHUNK) {
    let predicate = cast(col, Int64).in_list(chunk_literals, false);
    let (t, _metrics) = table.delete().with_predicate(predicate).await?;   // <- one COMMIT each
    table = t;
}
```

A Delta `DELETE` rewrites every file containing a matching row. The keys in an update window are
arbitrary ids scattered across the whole table, so **essentially every file matches every chunk**
— each chunk rewrites the entire table. Cost is therefore
`ceil(distinct_update_keys / 1024) × full_table_size`, i.e. quadratic in table size × window size
rather than linear in the window.

**Measured** (commit `00000000000000000080.json`, one chunk):

```
operation         : DELETE
num_deleted_rows  : 1024
num_copied_rows   : 114,383,538      <- the whole table, per 1024 keys
num_added_files   : 23
num_removed_files : 23
execution_time_ms : 248,144          <- 4.1 min
```

Three chunks sampled independently (commits 70/75/80) cost **247.2 s / 241.8 s / 248.1 s** — i.e.
**4.05 min ± 3 s**, each copying ~114.39 M rows to delete 1024. Cross-checked against the run
itself: 110 min of wall clock produced ~29 commits ≈ 3.8 min each.

### Separate the amplification from the link speed

The headline number decomposes into two independent factors, and only the first is a parket defect:

| factor | value | environment-dependent? |
|---|---|---|
| **Amplification** | `ceil(keys/1024)` full-table rewrites = **839 chunks** for this window | **no** — intrinsic |
| Cost per rewrite | ~2.4 GB moved; 4.05 min at the measured 12.5 MB/s WAN link | **yes** |

So for the 858,473-key window on this setup: **839 × 4.05 min ≈ 56.6 h and ≈ 2 TB of S3 egress**
(~USD 170 at $0.09/GB).

**But do not read "56 hours" as parket's intrinsic cost.** §3's own validation did 13,660 rows in
~74 s on the same 112 M-row table — that is 14 chunks at **5.3 s each**, implying ~453 MB/s, i.e.
local disk rather than remote object storage (§3 does not state its storage backend — see the
correction filed against that section). Identical amplification, 47× cheaper per rewrite. **The
same 858 k window in-region or on local disk would be ≈ 74 minutes** — bad, but survivable, and it
would likely never have been noticed.

Honest framing for comparison, same table and same window:

| | rewrites | measured / projected |
|---|---|---|
| `delete_then_append`, remote S3 over 12.5 MB/s | 839 | ≈ 56.6 h *(projected from 3 samples)* |
| `delete_then_append`, local/in-region (extrapolating §3's 5.3 s/chunk) | 839 | ≈ 74 min |
| `UPDATE_STRATEGY=merge` | **1** | **17.8 min measured**, one MERGE commit |

**Why the existing tests do not catch it.** The chunking itself is correct and deliberate — the
comment states its purpose (a single IN-list over every key OR-normalizes into a predicate tree
deep enough to overflow the stack, hence the 512 MB stack in `main.rs`), and
`delete_then_append_spans_multiple_delete_chunks` verifies *correctness* across chunk boundaries.
Neither the chunking nor its test is wrong. What is missing is any notion of **cost**: at test
scale one full-table rewrite is microseconds, so the amplification is invisible until the table
is large and remote.

**The operator gets no signal.** `UPDATE_STRATEGY` unset selects `delete_then_append`, and nothing
logs the projected rewrite count. The run simply appears slow and then dies on whatever
network hiccup arrives first — which is exactly how the three attempts above were misdiagnosed as
network failures rather than as a cost problem.

**FIX — corrected 2026-08-06.** An earlier revision of this finding proposed *"make `merge` the
default"* as fix #1. **That was wrong**, and is retracted: §2 of
`docs/two-stream-continue-update.md` establishes with empirical sizing runs that the delta-rs MERGE
is `source FULL OUTER JOIN target`, **cannot** be memory-bounded (the dominant memory — Parquet
scan buffering plus delta-rs's own merge output buffering — is untracked by the `FairSpillPool`, so
it never spills), and has a **~6.9 GB working-set floor for 112 M rows that grows with the table**.
A 4 GB VM is infeasible for it. `delete_then_append` is the default *precisely because* it is
bounded and table-size-independent (2060 MB peak under a 4 GB cap). That trade was made
deliberately and on better evidence than the original filing had. The 17.8 min MERGE measured above
ran on a **46.5 GB workstation** with a 23.8 GB pool and its peak RSS was never sampled, so it says
nothing about the 8 GB target.

The real constraint is therefore **both** bounded memory **and** one pass. Only one option
satisfies both:

1. ~~**Single-DELETE via set/anti-join predicate**~~ — **ATTEMPTED 2026-08-06 and BLOCKED by
   delta-rs. Do not retry as specified.** The idea was sound and the plumbing exists:
   `DeleteBuilder::with_session_state` accepts our bounded session, and delta-rs applies the delete
   predicate at the LOGICAL level (`operations/delete.rs`: `.filter(predicate)` and
   `.filter(predicate.is_not_true())`), so datafusion's `decorrelate_predicate_subquery` would turn
   an `Expr::InSubquery` into LeftSemi/LeftAnti joins with the **small key set on the build side**
   and the huge target streaming — bounded, and one commit instead of 839.

   It compiles and clippy passes. It fails at runtime, in 7 existing `delete_then_append` tests:

   ```
   Generic DeltaTable error: Unable to convert expression to string
   ```

   `operations/delete.rs:331` serialises the predicate into the commit's `operationParameters`
   via `fmt_expr_to_sql`, and that writer (`delta_datafusion/expr.rs:613`) has no `InSubquery`
   arm — reasonably so, since a subquery is not meaningfully representable as a Delta commit
   predicate string. **Any** predicate that is not SQL-stringifiable is therefore rejected by
   `DeleteBuilder`, which rules out this whole class of fix at the library boundary. Closing it
   would need an upstream delta-rs change (or a fork), and the upstream design question — what
   string to record for a subquery predicate — has no obvious answer.

2. **Anti-join + atomic overwrite, implemented in parket** *(now the real fix)*. Bypass
   `DeleteBuilder` entirely: scan the target through the bounded session, LEFT ANTI JOIN it against
   the deduped key set (small side = build side ⇒ streaming and bounded), union the new versions,
   and commit the result as a single atomic overwrite. This needs **no new primitives** — parket
   already has `begin_overwrite` / `commit_overwrite` / `abort_overwrite` (`writer.rs:414/502/595`),
   built for full_refresh's atomic overwrite (O2-r) and already exercised by the reconcile path
   (PS-H-B). Properties: **one** table rewrite instead of 839, bounded memory, one commit (which
   also removes the two-commit window §3 lists as a trade-off), and no dependence on delta-rs
   predicate serialisation. Cost: rewrites the whole table even for a small window, so it should be
   chosen by size — small windows keep the existing chunked DELETE, large ones take this path.

3. **Raise `DELETE_KEYS_PER_CHUNK`** as a cheap interim mitigation. The constant exists only to cap
   predicate-tree depth, and at 1024 it produces the amplification measured above; e.g. 50,000
   would cut 839 rewrites to 18. **Do not pick a value without evidence** — the stack-overflow the
   comment describes is real, and it is what the current 1024 is defending against. Note the limit
   cannot be established from the unit tests: `main.rs` deliberately runs on a **512 MB stack**
   while test threads get far less, so a test-derived ceiling would be misleadingly low. Determining
   it needs a run on the real stack size.

4. **Warn loudly** with the projected rewrite count and estimated bytes before starting the loop
   (`ids.len()`, `ceil(ids.len()/1024)`, × current table size). Cheap, non-breaking, and would have
   saved all three runs above. Worth doing regardless of #1.
5. **Auto-escalate on a threshold** — above N chunks, route the window to the full-refresh
   atomic-overwrite path (`TABLE_RECONCILE` machinery), which is single-pass *and* bounded. Not to
   `merge_batch`, for the memory reason above.
6. **Revive §4's deferred APPEND + read-time-dedup design.** Its stated trigger is "only if the
   per-run target scan becomes the bottleneck" — that trigger has now fired, harder than the
   wording anticipated (see the correction filed against §3's cost claim).

**Operator workaround today.** The supported answer is a one-shot
**`TABLE_RECONCILE_<table>=true`**: single pass, bounded memory, correct on an 8 GB host. It costs a
full re-extract of the source table, which is the price of staying inside the memory budget.

`UPDATE_STRATEGY=merge` — **gap closed for routine windows, 2026-08-07:**

> **MEASURED and it passes comfortably.** The experiment prescribed here was run: MERGE path under a
> hard cgroup cap (`sudo systemd-run --scope -p MemoryMax=8G -p MemorySwapMax=0`) against the real
> **115.2 M-row** table with a **26,171-row** window, at §2.3's best config
> (`MERGE_MEMORY_MB=2048`, `MERGE_TARGET_PARTITIONS=1`):
>
> ```
> cgroup peak : 1.757 GB of 8.000 GB   (22 % of cap)
> peak VmHWM  : 1.824 GB
> peak VmSwap : 0.000 GB               <- swap.max=0, so not a swap-assisted "pass"
> merge write : 8.8 min                completed, succeeded=1 failed=0
> ```
>
> That is **4× below** §2.3's 6,908 MB on a **larger** table, and it overturned the model rather than
> confirming it: §2.3's rows are a **fresh bootstrap** while `FULL OUTER JOIN` materialises the
> *smaller* side, so continue-update memory tracks the **window**, not the target. Corroborated by
> timing — this 26 k merge took 8.8 min against 8.7 min for an 858 k merge, i.e. **time is
> target-bound, memory is source-bound**. The two documents have been corrected (§2.1, §2.3).
>
> **Still NOT established: large windows.** 26 k rows is a routine daily window. An 858 k window
> (the post-outage case) is ~33× more source rows and, under the source-bound model, could cost
> several GB. Measuring it needs a deliberately widened window. Until then `merge` is supportable
> for routine syncs and unproven for recovery-after-outage — which is the case
> `TABLE_RECONCILE_<table>=true` and the §10.1 overwrite path already cover.

`delete_then_append` remains correct and bounded — just do not point it at remote object storage
with a large update window.

### 10.2 **FIXED** — Low: `SIGINT` could not land inside the writer's long loops

> **Fixed 2026-08-07.** And note the prediction recorded here earlier was WRONG: §10.1's overwrite fix
> was expected to *dissolve* this finding by removing the long writer-side loop. It did not — it
> **moved** it. The writer had zero shutdown checks anywhere, and after §10.1 it had two long loops
> rather than one:
>
> * `stage_surviving_rows_and_new_versions`' per-batch survivor stream — **14.4 min** at production
>   scale, and the new default for any window above the routing threshold;
> * `delete_then_append`'s per-chunk DELETE loop — still used for windows below it.
>
> `DeltaWriter` now takes the orchestrator's `watch::Receiver<bool>` via `with_shutdown` (wired in
> `main.rs` through both writer adapters; `None` by default, so every test and any other caller is
> unaffected) and checks it once per batch and once per chunk.
>
> **The two paths stop with different guarantees, and the difference is the interesting part:**
>
> | path | on shutdown | table state |
> |---|---|---|
> | overwrite | bail before `commit_overwrite` ⇒ caller's error path runs `abort_overwrite` | **untouched** — staged parquet discarded (orphans only, VACUUM-able), nothing committed |
> | chunked | bail before the append commit | **temporarily short** — keys deleted so far are not yet re-appended |
>
> The chunked case is deliberately allowed to leave the table short, because the alternative is worse.
> The update HWM only advances on the append, so the next run re-extracts exactly this window and
> restores those rows — the identical state a crash mid-loop produces, which the design already
> tolerates ("a failed append self-heals next run via the unchanged watermark",
> `docs/two-stream-continue-update.md` §3). Continuing to the append after a partial delete would be
> the unsafe option: the un-deleted keys would then hold two versions. Both error messages say which
> case occurred and how it recovers, since an operator seeing a failed table needs to know whether to
> act or simply re-run.
>
> Tests: `delete_then_append_chunked_stops_on_shutdown_without_appending` and
> `delete_then_append_overwrite_stops_on_shutdown_and_commits_nothing` both pre-signal a
> `watch::channel`, then assert the call errors, the message explains recovery, **the table version is
> unchanged**, and every row retains its pre-update value.

### 10.3 Note — killing mid-write is safe, and confirmed so

Recorded because it was verified three times, twice by accident: aborting a two-stream update
mid-write (host suspend, connect timeout, and `SIGTERM`) left the table **consistent** each time.
The last successful commit remained the table version, the interrupted write's parquet files were
never referenced by the log (orphans, not corruption), and a Delta-side key census returned
`count == distinct` after each abort (115,226,755 / 115,213,106 respectively). Re-running is safe:
the update stream's HWM only advances on a successful commit, and `delete_then_append` is
idempotent. Orphaned parquet accumulates and is not reclaimed by anything short of VACUUM.

### 10.4 Open — Low: `--check` preflight hides the two-stream INSERT watermark

The preflight table prints `hwm_updated_at / hwm_last_id`. For a **two-stream** table those are both
*update-stream* values, and `hwm_last_id` is the update window's keyset-pagination cursor — **not**
the insert frontier. `hwm_insert_id`, the value an operator actually reasons about for a two-stream
table, is not shown at all.

Observed consequence (2026-08-06, during a post-incident recheck): preflight read
`… 2026-07-18T17:17:23 / 500147844` before a sync and `… 2026-08-06T14:31:31 / 173218080` after it,
which looks exactly like an insert watermark **regressing by 327 M** — the H-2026-07-11-1 failure
shape. It had not. The commit carried all three keys correctly:

```
hwm_insert_id  = 502658778     <- advanced from 500147844, correct
hwm_updated_at = 2026-08-06T14:31:31.000000
hwm_last_id    = 173218080     <- update-window pagination cursor
```

`read_insert_hwm` reads `hwm_insert_id` and `read_hwm` reads `hwm_updated_at`/`hwm_last_id`, so the
two streams resume from the right places and there is **no correctness bug**. The defect is purely
that the diagnostic most likely to be run *during an incident* displays a number that resembles the
insert watermark and can appear to move backwards. FIX: for two-stream tables print `hwm_insert_id`
alongside the update cursor, or label the column so `hwm_last_id` cannot be mistaken for it.

### 10.5 Open — Medium: superseded parquet is never reclaimed (PS-M3, now quantified)

`developer_journey_trackings` on S3, measured 2026-08-06 immediately after a successful sync:

| | |
|---|---|
| parquet files in the table prefix | **793** |
| prefix size | **73.1 GB** |
| files referenced by the current snapshot | **36** (`num_target_files_added` of the final MERGE) |
| live data size | **~2.4 GB** |

≈ **30× storage amplification**. Two sources, both expected-but-unreclaimed:

1. **Superseded files** — every one of §10.1's 839 DELETE chunks removed and rewrote ~23–27 files.
   The `remove` actions are logged, but Delta never deletes the bytes; only VACUUM does.
2. **True orphans** — parquet written by the three aborted runs that never reached a commit, so no
   `remove` action references them either.

**Correction (2026-08-07):** an earlier revision of this item called PS-M3 "already open". It is
**done** — §8.3 records the one-time reclaim executed 2026-07-18 *and* the ongoing cadence (monthly
`VACUUM … RETAIN 168 HOURS`, safe because L7's 64-commit lookback survives the no-HWM commits VACUUM
adds, proven in production). No new procedure is needed here; what was missing is **when** the
current bloat becomes eligible, and **which vacuum mode** reclaims it.

### Age breakdown and the eligibility date

`developer_journey_trackings` parquet, measured 2026-08-07 (831 files, ~76 GB; live snapshot = 36
files ≈ 2.4 GB):

| date | files | size | eligible under `RETAIN 168 HOURS` |
|---|---|---|---|
| 2026-07-18 | 25 | 2.1 GB | **yes** |
| 2026-08-05 | 81 | 7.4 GB | 2026-08-12 |
| **2026-08-06** | **687** | **63.6 GB** | **2026-08-13** |
| 2026-08-07 | 38 | 3.4 GB | ~36 are the LIVE snapshot |

So a standard VACUUM **today reclaims ~2.1 GB of ~76 GB (3 %)** — precisely what PS-M3 hit on
2026-07-18 ("a standard 168h VACUUM reclaimed 0 that day, tombstones <7d"). **Run the cadence on or
after 2026-08-13**, when the 63.6 GB crosses the boundary; running it sooner wastes the trip.

**Do NOT force `retention_hours=0` to reclaim it early.** PS-M3's retention=0 was justified by a
specific verified-superseded snapshot. The opposite applies now: v0.2.4 ships a NEW write path
(§10.1's overwrite), and older table versions are exactly the recovery mechanism if it misbehaves.
Destroying time travel the day before validating new write code is backwards.

### `VacuumMode::Lite` will NOT reclaim all of it

`deltalake-core`'s default is `VacuumMode::Lite`, which removes only files carrying a `remove`
action. The bloat here has two sources and only one qualifies:

* **superseded** files from §10.1's 839 DELETE chunks — have `remove` actions ⇒ Lite reclaims them;
* **true orphans** from the three aborted runs plus aborted staged-overwrite parquet (FA11) — were
  never committed, so nothing references OR removes them ⇒ **Lite skips them; `VacuumMode::Full`
  is required**, since it scans storage for files no longer named by any `add` action.

Any tooling built for this must therefore choose Full deliberately (see §10.7).

### The root cause is fixed; this is cleanup of historical damage

687 files in a single day is §10.1's amplification measured in bytes rather than hours — the same
defect. With `fb5c4eb` a two-stream update run adds ~2.4 GB (one rewrite) instead of ~64 GB (839).
So: ship the fix, then reclaim once. Note the validation run itself adds ~2.4 GB.

### 10.6 Post-incident verification — NO DATA LOSS (evidence, 2026-08-06)

The incident above involved three aborted writes to a 115 M-row two-stream table (host suspend,
S3 connect timeout, deliberate `SIGTERM`) followed by a successful `UPDATE_STRATEGY=merge` run.
This subsection records the evidence that none of it lost or duplicated data, because §10.3's
"aborting is safe" claim otherwise rests on assertion.

**1. `parket --verify` — clean, but weaker than it looks.**

```
verify summary: pass=3  drift=0  discrepancy=0  skipped=5
```

Schema matched on all 8 tables (`missing_in_delta=[] extra_in_delta=[]`). The three tables under the
row cap passed strictly (`partner_programs` 579=579 + 5 value-aggregates; `developer_journey_tutorials`
11,548=11,548 + 12 aggregates + 100/100 sample; `developer_journeys`). **But 5 of 8 tables SKIPPED
strict checks** — every large one — on `> cap 1000000`. `drift=0` here means *no drift was detected*,
not *none exists*: the checks that would detect it did not run. Worth internalising for this dataset,
where most tables exceed the cap by design.

**2. §8.5 frontier parity on `developer_journey_trackings` — EXACT.** The cheap check that answers
what the skipped verdict could not:

| | |
|---|---|
| insert HWM (`hwm_insert_id`) | 502,658,778 |
| source `COUNT(*) WHERE id <= hwm` | **115,249,572** |
| Delta total rows | **115,249,572** ← exact match |
| source `COUNT(*)` total | 115,252,316 |
| source rows above the frontier | 2,744 |

Delta holds precisely the number of rows the source holds at or below the insert watermark ⇒ **zero
rows lost** across all three aborted writes plus the MERGE. Paired with the Delta-side census
(`count == distinct == 115,249,572`) the table is both **complete up to its watermark** and **free of
duplicate keys**.

The shortfall `--verify` reported (`source=115,252,019 delta=115,249,572`, −2,447) is therefore
entirely rows that arrived after the sync cursor — `completed_at` source max was `15:29:59` against a
Delta cursor of `14:31:31`, i.e. ~58 min of new completions. The "rows above the frontier" figure grew
from 2,447 to 2,744 across the ~30 min between the two measurements, which is itself corroboration
that the gap is freshness and not loss.

**Reproduce** (read-only, one indexed range count; minutes, not hours — vastly cheaper than
`--verify-deep`'s 4–5 full source scans):

```sql
SELECT COUNT(*) FROM developer_journey_trackings WHERE id <= <hwm_insert_id>;
-- compare against the Delta count from:
--   cargo run -q -p parket --example delta_key_census -- developer_journey_trackings
```

**3. `--verify-deep` on the two `full_refresh` tables — DRIFT, and it is lag, not loss.** Scoped
runs on `users` and `developer_journey_completions` (both ~1–1.5 M rows, so minutes — the
`--verify-deep` cost warning is about the 115 M-row table, not these):

```
users:       DRIFT: source advanced past sync: source distinct=1454724 delta distinct=1454672
completions: DRIFT: source advanced past sync: source distinct=1029194 delta distinct=1029173
             — likely new/changed rows since sync, not a sync error
discrepancy=0 on both, schema ok on both
```

Comparing the two verify runs ~1 h apart makes it quantitative — **the gap grows by exactly what the
source grows, while Delta stays fixed**:

| table | source run 1 | source run 2 | source grew | Delta (both runs) | gap run 1 → run 2 |
|---|---|---|---|---|---|
| `users` | 1,454,706 | 1,454,724 | **+18** | 1,454,672 | 34 → 52 (**+18**) |
| `developer_journey_completions` | 1,029,191 | 1,029,194 | **+3** | 1,029,173 | 18 → 21 (**+3**) |

That is the signature of a consistent point-in-time snapshot falling behind a live table. Rows lost
by the aborted writes would leave a constant unexplained component in the gap; there is none.

**A `full_refresh` table on a live source can essentially never reach PASS — do not chase it.**
`--verify-deep` never reached its strict content checks on either table: the count mismatch produced
DRIFT first, and DRIFT short-circuits before `value-aggregates` / `non-null census` (neither line is
emitted). Since any source write between sync and verify guarantees a count mismatch, the DRIFT
verdict is the **expected steady state** for these tables, not an actionable signal. Consequence:
for `users` and `completions` there is strong *count* evidence and **no** *content* evidence, and
re-running cannot change that. A true PASS would require a quiescent source, or verifying against a
snapshot captured at the same instant as the sync. Structural limitation of verifying full_refresh
against a live DB, not a defect in either.

### 10.7 Proposed — parket has no `--vacuum`, and this is now a recurring chore

PS-M3's reclaim was done with **`deltalake` Python** because parket has no vacuum command
(`grep -ri vacuum src/` finds only log strings and comments). That is now the awkward part of a
*monthly* cadence: it needs a second toolchain, credentials duplicated outside parket's config, and
a hand-written script whose retention/mode flags are exactly the dangerous ones. `deltalake` Python
is not even installed on the current operator host.

**Proposal: `parket --vacuum [<TABLE>]`**, mirroring `--verify [<TABLE>]`'s shape (all configured
tables, or one named table).

| flag | default | rationale |
|---|---|---|
| `--vacuum [<TABLE>]` | — | one table or all; reuses the existing config + S3 storage options |
| *(dry run)* | **ON** | deletion is irreversible. Report files/bytes and exit 0 without deleting unless `--vacuum-apply` is passed. An ETL binary must not delete data as the default reading of a new flag. |
| `--vacuum-apply` | off | actually delete |
| `--vacuum-retention-hours <N>` | `168` | matches the PS-M3 cadence |
| `--vacuum-full` | off | select `VacuumMode::Full`; without it, Lite leaves true orphans behind (above) |
| `--vacuum-force` | off | REQUIRED to accept `retention-hours < 168`, i.e. to set
  `with_enforce_retention_duration(false)`. Keeps "retention=0" from being a typo away. |

Report: files considered, files deleted, bytes reclaimed, footprint before/after — PS-M3's write-up
had to state these by hand ("71 dead files … 3.7 GB→2.06 GB, 25 live parquet"), which is exactly the
output a command should produce itself.

Notes for the implementer:
* delta-rs API is `table.vacuum()` → `VacuumBuilder` with `with_retention_period(Duration)`,
  `with_mode(VacuumMode::{Lite,Full})`, `with_dry_run(bool)`,
  `with_enforce_retention_duration(bool)`, `with_keep_versions(&[Version])`. `VacuumMetrics` returns
  `dry_run` and `files_deleted: Vec<String>` — byte totals must be summed separately from the file
  listing.
* VACUUM appends two no-HWM commits (START/END). That is **safe** — L7's 64-commit lookback recovers
  the watermark past them and PS-M3 proved it in production (`skipped_commits=2`, exit 0) — but a
  regression test should pin it, because the failure mode is a silent HWM reset (H-2026-07-11-1).
* It must refuse to run while a sync/overwrite session is in progress for the same table
  (single-writer assumption).
* `with_keep_versions` is worth exposing later if time-travel windows are ever needed; not needed
  for the monthly cadence.
