## Context

Parket is a greenfield Rust project — a standalone one-shot binary that extracts data from MariaDB and loads it into Delta Lake on S3/MinIO. There is no existing codebase. The project brief (in `docs/project-brief.md`) defines 6 milestones, and the alternatives document (`docs/project-brief-alternatives.md`) provides detailed architecture decisions across 16 areas.

The target environment is a cron-scheduled or container-orchestrated execution model where the binary runs to completion and exits. There is a single MariaDB source and a single S3/MinIO target.

## Goals / Non-Goals

**Goals:**
- Implement all 6 milestones from the project brief as a working Rust binary
- Zero-configuration type mapping: MariaDB → Arrow via connector-x, no intermediate Rust structs
- Memory-bounded extraction with dynamic batch sizing
- Crash-safe HWM tracking via Delta commitInfo metadata (not state.json)
- Support both Incremental (cursor-based on `updated_at, id`) and FullRefresh (Overwrite) modes
- Graceful signal handling (SIGTERM/SIGINT)

**Non-Goals:**
- Table-level parallelism (v1 is strictly sequential)
- Intra-table batch pipelining (v1 is sequential per batch)
- Deduplication on write (at-least-once semantics only)
- TLS for MariaDB connections
- Retry/backoff logic (external scheduler handles retries)
- Schema evolution beyond fail-fast (no automatic column addition propagation)
- Multi-database support (single DATABASE_URL)

## Decisions

### D1: Module structure

```
src/
  main.rs          — entry point, signal handler setup, orchestrator invocation
  config.rs        — Config struct, env var loading via dotenvy
  state.rs         — StateManager: read/write state.json
  discovery.rs     — SchemaInspector: column discovery, mode detection, AVG_ROW_LENGTH
  query.rs         — QueryBuilder: SQL generation for both modes
  extractor.rs     — BatchExtractor: connector-x streaming API, adaptive batch sizing
  writer.rs        — DeltaWriter: delta-rs write, HWM extract/store, table creation
  orchestrator.rs  — Orchestrator: main loop over tables, error handling, exit codes
```

**Rationale:** Each module maps to a single responsibility from the spec. The dependency flow is unidirectional: `main → orchestrator → {discovery, extractor, writer, state, query}`. No circular dependencies.

### D2: connector-x streaming API over bulk API

Use `connectorx::source_mysql::new_record_batch_iter()` with explicit `batch_size`. This avoids pre-allocating the entire result set in memory. The batch_size parameter controls rows per RecordBatch, not memory — so adaptive sizing is layered on top.

**Alternatives considered:**
- Bulk `get_arrow()`: pre-allocates full result via COUNT(*) — rejected for memory reasons
- sqlx + manual Arrow construction: full control but 15-20 type mappings to implement — rejected for v1 complexity
- mysql crate + Arrow: same manual construction problem as sqlx

### D3: HWM stored in Delta commitInfo, not state.json

The HWM (`hwm_updated_at`, `hwm_last_id`) is written as custom metadata in the Delta `commitInfo` when a batch is committed. On startup, the latest HWM is read from the Delta log. This makes HWM atomic with the data commit — a crash between commit and state update cannot lose HWM progress.

**Trade-off:** Reading HWM from Delta log requires an S3 round-trip at startup. This is acceptable because it happens once per table per run.

### D4: Memory management — sample-based + adaptive with hard ceiling

1. Initial estimate: `batch_size = TARGET_MEMORY_MB / AVG_ROW_LENGTH` (from information_schema)
2. After first batch: measure actual Arrow bytes, recalculate `estimated_bytes_per_row`
3. If actual differs from estimate by >2x: adjust batch_size for subsequent batches
4. Hard ceiling: if any batch exceeds `2 * TARGET_MEMORY_MB`, halve batch_size and log warning
5. Fallback: if `AVG_ROW_LENGTH = 0 or NULL`, use `DEFAULT_BATCH_SIZE` (default 10000)

### D5: sqlx for metadata, connector-x for data extraction

sqlx is used only for `information_schema` queries (lightweight, infrequent). connector-x handles all data extraction. Two separate MySQL connections to the same MariaDB instance — acceptable overhead for the separation of concerns.

### D6: S3 path layout

```
s3://{S3_BUCKET}/{S3_PREFIX}/{table_name}/
```

Each MariaDB table maps to one independent Delta table. `S3_PREFIX` defaults to `parket`. This allows independent schema evolution, independent HWM, and independent querying by downstream consumers.

### D7: Error handling strategy

- **Per-table errors:** Log error, mark table as failed in state.json, continue with next table, exit code 1
- **Fatal errors (config, DB connection at startup):** Exit code 2 immediately
- **Signal:** Finish current batch, skip remaining tables, exit code 0

Use `anyhow` for error propagation internally. `thiserror` is not needed — there are no custom error types that need pattern matching in the caller; all errors are either logged or propagated to the orchestrator.

### D8: Async runtime

Use `tokio` as the async runtime. The binary uses `#[tokio::main]` in main.rs. sqlx requires tokio. connector-x is synchronous (uses its own threads internally), so it is called via `tokio::task::spawn_blocking` to avoid blocking the async runtime.

## Risks / Trade-offs

| Risk | Mitigation |
|---|---|
| connector-x `COUNT(*)` overhead on large tables before each batch | Accept latency — connector-x internals issue this regardless of API. Log a debug message. |
| connector-x has no parameterized queries — HWM values interpolated into SQL | Safe for internal use (HWM values come from Delta metadata, not user input). Use string formatting, not user-supplied values. |
| Arrow memory per row differs from AVG_ROW_LENGTH — OOM risk | Adaptive batch sizing after first batch + hard ceiling at 2x TARGET_MEMORY_MB |
| connector-x version compatibility with MariaDB | Test against target MariaDB version early. connector-x uses MySQL protocol which MariaDB supports. |
| delta-rs S3 configuration complexity (credentials, endpoint) | Use delta-rs's built-in S3 storage options, map from env vars. Test with MinIO early. |
| Schema evolution false positives (hash changes due to column order) | Hash includes ordinal_position to detect reordering |
| Two DB connections (sqlx + connector-x) doubles connection count | Acceptable — metadata queries are short-lived, connector-x connection per batch cycle |

## Open Questions

- **connector-x streaming API batch size mapping:** Need to verify that `new_record_batch_iter` batch_size parameter maps to row count (not byte count) and that each yielded RecordBatch has at most `batch_size` rows. This affects whether LIMIT in SQL is redundant or complementary.
- **delta-rs commitInfo custom metadata API:** Need to verify the exact delta-rs API for writing custom metadata to commitInfo and reading it back from the Delta log. The API may have changed across versions.
- **FullRefresh batch strategy:** For FullRefresh mode, connector-x streaming with Overwrite means all batches must be collected before writing (Overwrite replaces the entire table). Need to confirm whether delta-rs supports streaming writes with Overwrite, or if data must be collected into a single write call.
