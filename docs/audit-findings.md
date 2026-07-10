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

**Branch state (2026-07-08; history REWRITTEN — all pre-rewrite hashes remapped in this doc):** active work on `test/verify-docker-integration` (C1 `2b8c3f9`,
R2 `313ba2b`, docs, Docker verify tests `108d9e3`, N2/N3/N6 `2399b50`+`54bbf45`, VA batch
`a9bf774`). `vendor/connector_arrow`: upstream PR #79 **merged** and released as v0.12.1 —
submodule back on aljazerzen upstream at `3e98df6` (`3a3059d`); the temporary fork pin is retired. `audit/critical-fixes` is
parked at R1 (`1ff706f`); fast-forward it and prune the redundant `snapshot/*` /
`fix/r2-hwm-progress` branches once the Docker tests land. parket itself is not pushed to a
remote; base `b59fd47` (= origin/master).

**Cross-engine status (updated after `108d9e3`):** the verify value-aggregate SQL is now
**execution-proven** against real MariaDB + MinIO for: full-refresh/basic deep verify across
INT/DECIMAL/DATETIME(6)/DATE/VARCHAR incl. NULL + multibyte (Clean on match, Discrepancy on a pure
value drift), and incremental HWM-scoped verify (Clean in-window, post-HWM rows genuinely excluded,
in-window drift detected), now including native-scale DECIMAL(20,12) (healthy Clean, digit-12
drift detected — `a9bf774`). Still unproven under Docker: the two-stream verify verdict path
(`two_stream_key_stats_outcome`), the Drift and size-guard Skipped tiers, VARCHAR-only drift in
isolation, and exact set-equality beyond the aggregate fingerprints.

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
| N6 | `54bbf45` | examples call sites fixed; `cargo clippy --all-targets -- -D warnings` green — gate widened |
| T1–T5 | `108d9e3` | Docker verify tests committed + strengthened: corruption→Discrepancy (both paths), post-HWM scope exclusion, NULL + multibyte rows; ran green under real MariaDB+MinIO (Opus-reviewed) |
| N4 (+N1 mappings) | `36c485c`, `3a3059d` | DATE/MEDIUMINT/VARBINARY/JSON mapped instead of `todo!()` panic; [connector_arrow#79](https://github.com/aljazerzen/connector_arrow/pull/79) **merged upstream** (v0.12.1) — submodule back on aljazerzen |
| O2/R4, R3, R5 | `6978d6c`, `52aa55a` | interrupted runs exit PartialFailure + table state "interrupted" (never "success"); shutdown mid-full-refresh after chunk 0 bails as a failure naming the partial rewrite; SIGTERM joins SIGINT (second signal → exit 130); state.json fsync (file before rename, dir after) — residual: O2-r stage-and-swap |
| V3 | `6c1fe9c` | verify resolves the real key: two-stream insert cursor > discovered single-column integer PK (new SourceProbe::integer_pk) > `id` fallback; threaded through key-stats/scoping/sampling/value filters incl. SourceScope.key_col; honest Skipped reason; Docker-proven on a `code_id`-keyed table (Clean → corruption → Discrepancy) (Opus-reviewed) — residual: V3-r |
| O3 (+pf1) | `e3395da`, `04e2678` | ColumnInfo carries nullability; auto-detection never selects a nullable cursor (demotes to full_refresh + warn when nullability is the deciding factor); explicit incremental/two-stream cursors honored + loud warn (row loss itself = D2); preflight inherits via shared detect_mode and its KEY reason names the nullable cursor — eliminating pf1's reachable unreachable!() (Opus-reviewed) |
| N5 | `2f0c4f8` | unsigned int columns widened to signed Arrow/Delta types (tinyint/smallint/mediumint→Int32-range, int/bigint→Int64) + batches cast before write (safe:false errors on >i64::MAX BIGINT UNSIGNED by name); Docker-proven round-trip incl. above-signed-max values across 2 runs + actionable overflow failure (Opus-reviewed). Migration: pre-fix unsigned Delta tables have narrower types → evolution check flags them → full-refresh to rebuild |
| M2, M3 | `0945149`, `04a90a7` | zero-copy MemTable registration for merge/delete sources (update-window peak ~halved); mid-stream memory circuit breaker at 2× budget with safe cursor truncation (OFFSET path bails before any write). NOTE: the audit's "mysql client buffers the whole result (2×)" claim was REFUTED — vendored connector_arrow streams via exec_iter, yielding 1024-row batches lazily; the window Vec is the by-design budget (one window = one HWM-carrying commit). Residual: M2-r2 |
| O1, O4, O5, O6 | `743fc95` | two-stream honors TABLE_HWM seeds (writer HWMs win; live bootstrap skipped); invalid TABLE_MODE bails actionably (two_stream → cursor-vars hint); mode/cursor conflicts bail at config load (both load paths); get_schema classifies missing-vs-transient like R1 (Opus-reviewed) |
| CF1, CF2 | `4c89bd7` | DEFAULT_BATCH_SIZE=0 rejected; AVG_ROW_LENGTH NULL → graceful fallback (Codex) |
| VA1/VA2/VA3/V4/VA4/VA5 | `a9bf774` | component fingerprints + central assembly; native-scale decimals (Docker-proven); sum-overflow guard; n= counts; cap-before-scan incl. Delta rows; one-query aggregates; bounded probe sessions; try_cast + per-table Skipped-on-error (Opus-reviewed) — residual: VA1-r |

## 2. Open — Critical
- **N1 — extraction panics the process on unmapped column types.** *Largely mitigated by `36c485c`* (DATE/MEDIUMINT/VARBINARY/JSON now mapped; upstream PR #79). **Still open:** (a) the `todo!()` in `create_field` remains for any *other* unmapped type (`time`, `year`, `bit`-variants, geometry passed through, future types) → still a process abort; add a parket **preflight allowlist** so unmapped types are per-table errors, and/or upstream a follow-up turning the `todo!()` into a `ConnectorError`. (b) O8 remains: `time`/`year` fail the whole table instead of being skipped like geometry. *(Detail: audit-2026-07-06 §1.)*

## 3. Open — High
- **N2, N3** — **done** (`2399b50`, Opus-reviewed). Residuals registered below: N2-r (partial-chunk cross-run duplicates), N3-r (detect_mode literal-`id`).
- **N4/T1** — ~~vendored fix separable from the tests~~ **done** (`36c485c`, PR #79): fresh clones now fetch the fixed commit from the fork. T1 completed in full by `108d9e3` (tests committed with T2–T5 strengthening).
- **N5** — **done** (`2f0c4f8`, Opus-reviewed). Probe confirmed pre-fix delta-rs errored at write time (unsigned tables never synced). `extract_id_as_i64` u64-wrap was already fixed in N2's `try_from`; N5 additionally aligns batches so keys are signed before extraction.
- **VA1, VA3** — **done** (`a9bf774`, Opus-reviewed). Residual VA1-r below.
- **O1** — **done** (`743fc95`). Doc notes: one `TABLE_HWM_<t>` seeds BOTH two-stream streams (insert←id, update←ts) and the update boundary is strictly `ts > seed` (completions at exactly the seed instant are skipped — same as the live bootstrap).
- **O2/R4** — **done** (`6978d6c`, Opus-reviewed). Residual **O2-r**: stage-and-swap for full refresh — a mid-rewrite interruption is still data-destructive (chunk-0 overwrite already destroyed the prior snapshot); it is now *honest* (per-table failure, exit ≠ 0) but not *safe*. Note: a sole full-refresh table interrupted mid-rewrite yields Fatal (all-failed), subsuming the interrupted flag — defensible, documented here.
- **O3** — **done** (`e3395da` + `04e2678`). Note: a `TABLE_HWM_<t>` on a table now demoted to full_refresh bails "not incremental" — fail-loud replacing a silently-lossy config; operator-facing docs should mention it.
- **M2, M3** — **done** (`0945149`, `04a90a7`, Opus-reviewed; see resolved table for the corrected M2 claim). True extract→write streaming deliberately NOT pursued: the window Vec is the crash-safety unit (one window = one atomic HWM commit).
- **M2-r2 (Low)** — the N2/R2 no-progress guards are gated on `rows == batch_size`; a TRUNCATED window with an unextractable cursor (narrow: needs an unsupported cursor type or u64 > i64::MAX, since `ts IS NOT NULL` filters the common case) could loop re-appending. Hardening: bail on a truncated window that fails to advance the cursor (keyset full-refresh already does). Also: keyset full-refresh ignores the breaker's batch_size halving mid-table (correct but wasteful — re-trips per chunk).
- **V3** — **done** (`6c1fe9c`, Opus-reviewed).
- **V3-r (Med)** — non-integer / composite / UUID PKs still resolve no key → Skipped, and Skipped aggregates to Clean — the false-confidence class is narrowed, not eliminated. Options: string-key fingerprints, or make key-less tables surface as a distinct non-Clean summary state. (Delta-lacking-the-key edge is handled: missing-column Discrepancy or per-table probe-error Skipped.)
- **V4 / VA3(a)** — **done** (`a9bf774`).

## 4. Open — Medium
- **N2-r** — progress guards fire only on FULL chunks (by design, matching R2): a stream that fits one *partial* chunk whose cursor is present-by-name but unextractable (non-integer Arrow type) still appends without advancing the HWM → duplicates accumulate **across runs**. Consider a type-check in the early guard or a warn+skip.
- **N3-r** — `detect_mode` (discovery.rs:247-250) still requires a literal `id` column, so auto-detection never reaches Incremental for non-`id`-PK tables; N3's key threading only benefits explicit `TABLE_MODE=incremental`. Generalize detect_mode to the discovered integer PK (currently blocked on the CF WIP in discovery.rs; pairs with O3/O12 shared-resolver work).
- **N7** — `hwm_has_advanced` string compare is format-coincident: safe on the production T-format path; hazard for space-format config seeds and any true-Timestamp batch path. Normalize at entry points or compare parsed values.
- **N8** — `ORDER BY <all columns>` OFFSET fallback: ci-collation and TEXT-prefix ties break total order (skip/dup on PK-less tables) + per-page full filesort. Prefer any UNIQUE index; BINARY-strengthen ordering.
- **VA2, VA4, VA5** — **done** (`a9bf774`): native-scale decimals; try_cast + per-table Skipped-on-error; n= in every fingerprint.
- **VA1-r** — the overflow guard is conservative: near the DECIMAL(38,scale) capacity it drops SUM on both sides, so a corruption altering *only* the sum (min/max/n unchanged) is invisible in that narrow window. Acceptable trade-off vs DataFusion's silent corruption; note for completeness.
- **O4** — unknown `TABLE_MODE` values (typos, `two_stream`) silently → Auto (`config/parse.rs:37-41`). Bail.
- **O5** — two-stream cursor config silently overrides explicit `TABLE_MODE`. Bail/warn on conflict.
- **O6** — `adapters.rs:157-167` `get_schema` `Err(_)=>Ok(None)` (R1-class recurrence, ×2 duplicated impls) silently disables schema-evolution check. Classify missing-vs-transient like R1.
- **O7** — `--check`/run parity: mode-override skips column validation; no evolution check in preflight; S3 health-check written at bucket root ignoring `s3_prefix`; local mode probes nothing.
- **O8** — unsupported-type inconsistency: geometry skipped gracefully; `time`/`year`/`bit` fail the whole table (and would panic in the connector — N1 class). Extend skip-list or map them.
- **R3, R5** — **done** (`6978d6c`). Minor tidiness follow-up: a failed write still leaves a stale `.tmp` (pre-existing; overwritten on next success).
- **D1** — new source columns silently dropped forever by evolution filter (`orchestrator/schema.rs:84-96`); also feeds N3. Additive evolution or fail loudly.
- **D2** — `WHERE ts IS NOT NULL` permanently drops NULL-cursor rows (`query.rs:31,36`) with no backfill (pairs with O3).
- **D3** — two-stream first-run seed not persisted; completions between two seeds can be skipped forever (`orchestrator/two_stream.rs:34-44`). Persist via HWM-only commit.
- **M4** — `TARGET_MEMORY_MB` has no RAM-relative ceiling (`config.rs:83-88`); 64 GB on an 8 GB box OOMs at runtime, not config load.
- **CF1, CF2** — **done** (`4c89bd7`, Codex).
- **S1** — `#[derive(Debug)]` on `Config` prints `database_url` + S3 secret verbatim (`config.rs:12`); hand-write Debug/Secret newtype.
- **S2** — identifiers not backtick-escaped (embedded `` ` `` breaks out) and HWM value string-interpolated (`query.rs:1-3,31`).
- **S3** — `mask_secret` byte-slices the last 4 bytes → panic on multibyte secrets (`config/mask.rs:23`).
- **P1** — per-batch `mysql::Conn::new` + per-write full Delta `load()` (`extractor.rs:47-51`; `writer.rs:166-169` etc.) — pool the connection; keep a table handle.
- **pf1** — **done** (folded into O3, `e3395da`): the arm is now a real "nullable <ts> (unsafe cursor)" reason; remaining unreachable!()s verified genuinely unreachable.
- **V5** — verify schema check compares column *names* only; types read but unused.
- **V6** — verify sample = lowest 100 ids only; recent rows never spot-checked.
- **V8** — unsigned-key CAST asymmetry in verify (saturate vs overflow) → false verdicts on `BIGINT UNSIGNED > 2⁶³`.
- **O9/O11/O12** — failed-run state wipes last-success metadata + `schema_columns_hash` write-only; `TABLE_TIMESTAMP` validated before mode resolution (fails tables that never use it); `--verify` mode resolution is a third divergent copy — auto-detected incremental verifies as `Basic`. Shared mode-resolver fixes O5/O7/O12 together.
- **T2–T5** — ~~corruption/scope/NULL/multibyte test coverage~~ **done** (`108d9e3`). Still open (T6): two-stream verify verdicts, Drift + Skipped tiers, VARCHAR-only drift isolation under Docker; suite also lacks keyset page-boundary (C1) and R2-bail coverage.

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
