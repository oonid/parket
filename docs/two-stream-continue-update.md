# Two-stream continue-update write strategy: DELETE+APPEND (default) vs MERGE (opt-out)

> parket two-stream runs **both** streams on every execution, but each phase is dominated by one:
> - **Full sync from zero** (first run / re-bootstrap): the **insert stream appends** every row.
>   **→ [two-stream-full-sync.md](two-stream-full-sync.md).**
> - **Continue / ongoing updates** (every later run): the insert stream appends only new rows, and
>   the **update stream upserts the mutations** — this document.
>
> **Status: RESOLVED (2026-06-12).** The continue-update upsert defaults to **DELETE+APPEND**
> (bounded memory, fits a 4 GB VM). The delta-rs **MERGE** is retained only as an opt-out
> (`UPDATE_STRATEGY=merge`) and **cannot** be memory-bounded — see §2.
>
> **AMENDED 2026-08-06 — the memory conclusion stands; the COST conclusion does not.** DELETE+APPEND
> performs **one full-table rewrite per 1024 update keys**, not one target scan per run as §3
> originally claimed. On a 115 M-row table over remote object storage an 858 k-key window projects to
> **≈ 56 h / ≈ 2 TB egress** (839 chunks); on local disk the same window is ≈ 74 min. See the
> correction in §3 and audit-findings §10.1.
>
> **AMENDED AGAIN 2026-08-07 — the memory conclusion is now scoped.** "Cannot be memory-bounded" and
> the ~6–7 GB floor hold for a **fresh bootstrap**. A measured **continue-update** (115.2 M-row
> table, 26 k-row window, `MERGE_MEMORY_MB=2048`, `PARTITIONS=1`) peaked at **1.757 GB** under a hard
> 8 GiB cap with `swap.max=0` — memory tracked the WINDOW, not the target. DELETE+APPEND remains the
> default (it needs no tuning and is bounded by construction), but `UPDATE_STRATEGY=merge` is no
> longer disqualified on memory for routine windows. Large windows remain unmeasured. See the §2.1
> correction.

Generic `orders` table throughout: integer PK `id`, nullable `completed_at` (update cursor),
ordinary columns. Numbers are from the real 112M-row table the design was validated on.

---

## 1. The problem
On a continue run, the update stream produces the rows that changed since the last run (e.g.
orders that just got a `completed_at`). These must be **upserted** into the current-state Delta
table — keyed on `id`, one row per id. The question this doc answers: **which write operation
performs that upsert**, and why the obvious choice (delta-rs MERGE) doesn't scale.

---

## 2. Why the delta-rs MERGE cannot be memory-bounded

### 2.1 What it does
`merge_batch` runs a delta-rs `MERGE` = **`source FULL OUTER JOIN target`** (verified at
`deltalake-core-0.32.4/src/operations/merge/mod.rs:1020`). A full outer join forces datafusion to
**materialize a side** and flow **every** target row through (matched→updated, unmatched→kept),
then rewrite touched files. So memory scales with the **whole target**, not the change set.

> ### ⚠ CORRECTION (2026-08-07): measured, and it scales with the SOURCE
>
> A continue-update MERGE was run under a hard 8 GiB cgroup cap (`MemoryMax=8G`, `MemorySwapMax=0`)
> against the real **115.2 M-row** table with a **26,171-row** window, using this document's own
> best config (`MERGE_MEMORY_MB=2048`, `MERGE_TARGET_PARTITIONS=1`):
>
> ```
> cgroup peak : 1.757 GB of 8.000 GB   (22 % of cap)
> peak VmHWM  : 1.824 GB
> peak VmSwap : 0.000 GB               <- swap.max=0, so this is a genuine pass
> ```
>
> 1.76 GB on a table **larger** than the 112 M used below, where §2.3 records 6,908 MB. The claim
> above is therefore wrong for the continue-update case: `FULL OUTER JOIN` materialises the
> **smaller** side — the source/change set — while the target *streams*. Memory tracks the WINDOW.
>
> Two independent observations support that model. §2.3's rows are a **fresh bootstrap** (its own
> heading says so), where the insert stream appends all 112 M rows — a different workload from the
> continue-update this document is about. And *time* behaves oppositely to memory: this 26 k-row
> merge took **8.8 min** while an 858 k-row merge on the same table took **8.7 min** — near
> identical, because both rewrite the whole target. So **time is target-bound and
> window-independent; memory is source-bound and window-dependent.**
>
> **Still unmeasured:** a large window. If memory tracks the source, an 858 k-row window is ~33×
> more source rows and could be several GB. Merging after an extended outage is therefore the
> remaining risk case — see audit-findings §10.1.

- Unbounded (default `SessionContext::new()`): **21–25 GB** on the 112M-row table → OOM on any
  small VM. Lowering `TARGET_MEMORY_MB` does nothing (it bounds the extract chunk, not the join).
- It **grows with the table**: a config that completed at 6.9 GB was later OOM-killed on a resume
  after the table grew — the merge re-processes the entire (growing) target every run.

### 2.2 The bounded-pool attempt (F10.4) and the verified ceiling
We tried to bound it: a `FairSpillPool(MERGE_MEMORY_MB)` + `DiskManagerBuilder(MERGE_SPILL_DIR)`,
`prefer_hash_join=false` (force a spillable SortMergeJoin), `MERGE_TARGET_PARTITIONS=1`,
`MERGE_SORT_RESERVATION_MB`, passed via the **mandatory** `MergeBuilder::with_session_state`
(delta-rs falls back to a default *unbounded* pool if you don't pass it), plus a startup
**`RLIMIT_NOFILE` self-raise** (the spill opens many files; systemd's default soft limit ~1024
otherwise fails with *"Too many open files"*).

It still can't be bounded, and the datafusion 53 source says why:

| Operator | Spills? | Source |
|---|---|---|
| `SortExec` | yes | `sorts/sort.rs` (the "Not enough memory to continue external sort" error) |
| `SortMergeJoin` | **buffered side only** | `joins/sort_merge_join/exec.rs:78`: *"no spilling support for streamed input"* |
| `HashJoin` | no | join sources |

For `source FULL OUTER JOIN target` the **huge target is the streamed side (does not spill)**, and
the dominant memory is **untracked by the pool** — the Delta/Parquet scan buffering the target +
**delta-rs's own merge output buffering** (it assembles the rewritten rows outside the pool). The
pool never sees pressure → never spills → RSS grows → the **OS swaps**, then OOMs.

**The spill machinery only bounds the `SortExec`; a MERGE is join-stream + output-rewrite bound,
not sort-bound. Memory tuning is exhausted.**

### 2.3 Empirical sizing runs — FRESH BOOTSTRAP only (112M-row `orders`, `TARGET_MEMORY_MB=512`)

> **Scope warning.** Every row below is a **fresh bootstrap**, not the continue-update this document
> is about. They are the right numbers for sizing a bootstrap and the wrong ones for sizing a
> steady-state run — a measured continue-update came in at **1.757 GB** (§2.1 correction, and the
> continue-update row appended to the table below).

Top block = default multi-partition (~14 sorters); bottom = `MERGE_TARGET_PARTITIONS=1`.

| VM cap | `MERGE_MEMORY_MB` | Enforced? | Peak RSS | Peak swap | Spill | Result |
|---|---|---|---|---|---|---|
| (host, unbounded) | — (default pool) | n/a | 21–25 GB | — | — | ✅ completed (materialized the 112M target) |
| `--user` 3500M | 2048 | **no** (user scope, no mem delegation) | 4.2 GB | ~0 | 14 GB | ❌ `Too many open files` → fixed by NOFILE self-raise |
| `--user` 3500M | 2048 | no | 4.2 GB | ~0 | 14 GB | ❌ `Not enough memory to continue external sort` |
| `sudo` 3500M | 1536 + `RESERVATION_MB=2` | **yes** | 3282 MB | 1883 MB | **57 GB** | ❌ pool exhausted at `ExternalSorterMerge[5]` (multi-level re-spill) |
| `sudo` 7500M | 4096 | **yes** | 5410 MB | 0 | 38 GB | ❌ pool exhausted at `ExternalSorterMerge[1]` |
| `sudo` 7500M | 4096 + `PARTITIONS=1` | **yes** | 7446 MB | **4088 MB** | ~0 | ✅ completed — streamed in-RAM, over-buffered → 4 GB swap |
| `sudo` 7500M | 2048 + `PARTITIONS=1` | **yes** | **6908 MB** | **0** | ~0 | ✅ completed clean — merge 47 s (best MERGE config) |
| `sudo` 7500M | 2048 + `PARTITIONS=2` + `RES=2` | **yes** | 7551 MB | 3660 MB | (spilled) | ❌ OOM in the merge-of-runs |
| **CONTINUE-UPDATE, 115.2M rows, 26k-row window (2026-08-07)** | | | | | | |
| `sudo` **8G**, `swap.max=0` | 2048 + `PARTITIONS=1` | **yes** | **1757 MB** | **0** | ~0 | ✅ completed clean — merge **8.8 min**, 22 % of cap |

Key lessons, in order of discovery:
1. `MERGE_MEMORY_MB` (the `FairSpillPool`) is a **hard internal cap** independent of VM RAM — too
   small and the merge can't *complete* (datafusion errors with RAM still free).
2. **Parallelism, not pool size, was the first wall:** `target_partitions` defaulted to CPU count
   (~14), and all sorters share the one pool → each merge phase starves (`ExternalSorterMerge[N]`,
   N = partition index). Fixed by pinning `MERGE_TARGET_PARTITIONS=1`.
3. At one partition the merge **streams** (≈ no spill); `MERGE_MEMORY_MB` is then an **in-memory
   buffering cap, not a spill trigger** — `4096` over-buffers into swap, `2048` fits at ~6.9 GB.
4. **~6–7 GB working-set floor** for 112M rows, ~independent of the pool → a real 4 GB VM is
   infeasible for the MERGE. Best MERGE config: **8 GB VM, `MERGE_MEMORY_MB=2048`,
   `MERGE_TARGET_PARTITIONS=1`** (size the VM to peak RSS, not the pool).
   **Amended 2026-08-07:** this floor is a **BOOTSTRAP** figure. A continue-update on a larger
   (115.2 M-row) table peaked at **1.757 GB** with the same config, so there is no 6–7 GB floor for
   steady-state runs. The "floor **grows with the table**" note is likewise unsupported for
   continue-update — memory tracked the 26 k-row window, not the 115 M-row target (§2.1 correction).
   What does grow with the table is the merge's *runtime*, since it rewrites the whole target.
5. **Runtime is DB-bound** (the `completed_at` filesort in MariaDB), not parket — `user+sys ≈ 0`.

### 2.4 Rejected: forcing a streaming/hash join
delta-rs hard-codes `JoinType::Full` and exposes no build-side control; HashJoin doesn't spill and
the FULL-join streamed side doesn't spill. Any join variant still drives the whole target through a
non-spillable operator + a full output rewrite. Spill can't save it.

---

## 3. The default — DELETE + APPEND

For each update batch: dedup the source by key, then **`DELETE FROM target WHERE id IN (<changed
ids>)`** (delta-rs `DeleteBuilder`) followed by an **append** of the new versions
(`WriteBuilder(Append)`). The delete is a streaming **scan → filter → rewrite matched files**
(`delete.rs`: no `JoinType`, no `SortExec`), so **memory is bounded and ~constant regardless of
target size**. The result is the same current-state mirror (one row per id) the MERGE produced.

**Validation, same 112M-row table, `sudo systemd-run -p MemoryMax=3500M` (4 GB cap).**
**Storage backend not recorded** — the 74 s figure implies ~453 MB/s of parquet rewrite, so almost
certainly local disk, NOT remote object storage. This matters: see the correction below the table.

| | MERGE (best config) | **DELETE+APPEND** |
|---|---|---|
| Peak RSS on 4 GB cap | 7–11 GB → **OOM-killed** | **2060 MB — completed** |
| Swap / spill | up to 4 GB swap | 0 swap / 16 K spill |
| Update write | OOM | 13,660 rows delete+append in ~74 s |

Bounded, table-size-independent, fits a 4 GB VM, full current-state semantics, no
windowing/pruning. **This is the default.**

> ### ⚠ CORRECTION (2026-08-06): the cost below is understated by ~3 orders of magnitude
>
> The validation above was run at **13,660 rows = 14 delete chunks**, and its ~74 s implies
> ~453 MB/s of parquet rewrite — i.e. **local disk**. The backend was never recorded here, so the
> figure reads as general when it is not.
>
> The trade-off list originally said "**per-run** target scan", singular. That is wrong: the delete
> runs **once per `DELETE_KEYS_PER_CHUNK` (1024) keys**, and each one rewrites *every file holding a
> matching row* — which, for the scattered ids of a real update window, is the whole table. The true
> cost is **`ceil(distinct_update_keys / 1024)` full-table rewrites**, not one.
>
> Measured on a 115 M-row table (audit register §10.1): an 858,473-key window = **839 chunks**, each
> copying ~114.39 M rows at 4.05 min ± 3 s against remote S3 → **≈ 56 h and ≈ 2 TB of egress**.
> The same window on local disk would be ≈ 74 min (839 × 5.3 s) — the amplification is identical;
> only the per-rewrite cost differs. §4's deferred design names its own trigger as "only if the
> per-run target scan becomes the bottleneck": **that trigger has fired.**

Trade-offs (accepted):
- **2 commits** (DELETE then APPEND) → a brief window where the changed ids are absent; fine for a
  batch ETL with no concurrent mid-run readers.
- ~~**Per-run target scan**~~ → **one target rewrite per 1024 update keys** (time, not memory). See
  the correction above: this is `ceil(keys/1024)` full-table rewrites per run, and it is the
  dominant cost on remote storage.
- HWM advances **only after the APPEND commits** (failure-safe: a failed append self-heals next run
  via the unchanged watermark).
- Hard-deletes / un-completions still not captured (a general two-stream limitation).

Implementation: `DeltaWriter::delete_then_append` (source dedup parity with `merge_batch`; handles
Int32/Int64/UInt32/UInt64 keys; casts the predicate column to Int64). Routed in `process_two_stream`.

---

## 4. Deferred — APPEND + read-time dedup
The update stream could instead just **append** new versions (never read the target → cheapest,
constant writes), with "current state" as a **read-time dedup view** + a periodic **compaction**.
Deferred, not dropped: it's **additive** (DELETE+APPEND becomes the compaction step; no data
migration). **Trigger to add it:** only if the per-run target *scan* becomes the bottleneck at high
update frequency. If updates stay periodic, DELETE+APPEND suffices and this may never be needed.

---

## 5. Configuration

| Variable | Default | Effect |
|---|---|---|
| `UPDATE_STRATEGY` | `delete_append` | Continue-update write op. Default = bounded DELETE+APPEND. `=merge` selects the legacy MERGE (opt-out). |

**The `MERGE_*` knobs below apply ONLY when `UPDATE_STRATEGY=merge`** — the default
`delete_append` path needs none of them (bounded by construction):

| Variable | Default | Effect (merge opt-out only) |
|---|---|---|
| `MERGE_MEMORY_MB` | = `TARGET_MEMORY_MB` | FairSpillPool size for the MERGE session (in-memory buffering cap). |
| `MERGE_TARGET_PARTITIONS` | `1` | Sort parallelism; keep at 1 (parallel sorters share the pool and starve it). |
| `MERGE_SORT_RESERVATION_MB` | datafusion default (10) | External-sort merge-phase reservation; lower to `1`–`2` on "not enough memory". |
| `MERGE_SPILL_DIR` | system temp | External-sort spill dir; **must be real disk, not tmpfs**. |

**If you must run the MERGE opt-out** on the 112M-row table: **8 GB VM**, `MERGE_MEMORY_MB=2048`,
`MERGE_TARGET_PARTITIONS=1` → ~6.9 GB RSS (size the VM to peak RSS; the floor grows with the table).
**The default (`delete_append`) fits 4 GB** and is recommended.

---

## 6. Tests
- Unit (`writer.rs`): `delete_then_append` upsert (Int64 + UInt64 keys), duplicate-key dedup.
- Routing (`orchestrator.rs`): default → `delete_then_append`; `UPDATE_STRATEGY=merge` → `merge_batch`.
- Integration (`tests/integration.rs`): the two-stream end-to-end scenario runs under **both**
  strategies (`two_stream_inserts_and_delete_append_updates_across_runs` and
  `…_merges_mutations_across_runs`) and asserts the same current-state result.
