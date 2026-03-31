# Orchestrator

The orchestrator is the main coordination layer in Parket. It drives the sequential extract-and-load cycle for each configured table, delegating to specialized modules for schema discovery, batch extraction, Delta Lake writes, and state management.

## Execution Flow

```
run()
 │
 ├─ load_or_default(state.json)
 │
 └─ for each table in config.tables:
     │
     ├─ check_shutdown() ─── if true, break
     │
     ├─ process_table(table_name)
     │   │
     │   ├─ discover_columns(table)          → SchemaInspect
     │   ├─ filter_unsupported_columns()
     │   ├─ detect_mode(columns, override)    → Incremental | FullRefresh
     │   ├─ get_avg_row_length(table)         → SchemaInspect
     │   ├─ calculate_batch_size(avg_row_len) → Extract
     │   ├─ column_info_to_v57_schema(columns)
     │   ├─ ensure_table(table, schema)       → DeltaWrite
     │   │
     │   ├─ if Incremental:
     │   │   ├─ get_schema(table)             → DeltaWrite
     │   │   ├─ schema_evolution_check()      → compare MariaDB vs Delta
     │   │   └─ process_incremental()
     │   │       └─ loop:
     │   │           ├─ read_hwm(table)
     │   │           ├─ build_incremental_query(table, columns, hwm, batch_size)
     │   │           ├─ extract(sql)          → v54 batches
     │   │           ├─ convert_batches()     → v57 batches
     │   │           ├─ extract_hwm_from_batch()
     │   │           ├─ append_batch(table, batches, hwm)
     │   │           ├─ break if 0 rows or partial batch
     │   │           └─ repeat
     │   │
     │   └─ if FullRefresh:
     │       └─ process_full_refresh()
     │           ├─ build_full_refresh_query(table, columns)
     │           ├─ extract(sql)
     │           ├─ convert_batches()
     │           └─ overwrite_table(table, batches)
     │
     └─ update_table(name, state)             → StateManage
```

## Error Handling Strategy

| Error Type | Behavior |
|---|---|
| **Per-table error** (discovery failure, extraction error, write error) | Log error, mark table as `failed` in `state.json`, continue to next table |
| **State update failure on success** | Propagates as table failure — `process_table` returns Err, `run()` marks table failed |
| **State update failure on failed table** | Logged as warning, does not prevent further processing |
| **Fatal error** (config/DB connection) | Exit code 2 — though in the orchestrator these manifest as all-tables-failed |

## Exit Codes

| Code | Constant | Condition |
|---|---|---|
| 0 | `ExitCode::Success` | All tables processed successfully |
| 1 | `ExitCode::PartialFailure` | Some tables succeeded, some failed |
| 2 | `ExitCode::Fatal` | All tables failed (no successes) |

The exit code is determined after all tables have been attempted:
- `succeeded > 0 && failed == 0` → `Success`
- `succeeded > 0 && failed > 0` → `PartialFailure`
- `succeeded == 0 && failed > 0` → `Fatal`

## Schema Evolution Rules

Schema evolution is checked **only for Incremental tables** with an existing Delta table. It compares the current MariaDB column set against the Delta table's Arrow schema.

| Change | Behavior |
|---|---|
| **Column added** to MariaDB (not in Delta) | Log warning, exclude from SELECT column list |
| **Column dropped** from MariaDB (exists in Delta) | Log error, fail table |
| **Column type changed** in MariaDB | Log error, fail table |
| **Unsupported type** in MariaDB column | Log warning, skip comparison for that column |
| **No existing Delta table** | No schema evolution check; all columns selected |

The schema evolution check returns the list of columns to include in the SELECT query. For FullRefresh mode, all discovered columns are used directly.

## Module Structure

### Orchestrator Struct

```rust
pub struct Orchestrator<S, E, W, M> {
    config: Config,
    schema_inspect: S,     // trait SchemaInspect
    extractor: E,           // trait Extract
    writer: W,              // trait DeltaWrite
    state_mgr: M,           // trait StateManage
    shutdown: watch::Receiver<bool>,
    state_path: PathBuf,
}
```

The orchestrator is generic over four traits, enabling full unit test isolation via `mockall` mocks.

### Trait Definitions

- **`SchemaInspect`** — `discover_columns()`, `get_avg_row_length()`
- **`Extract`** — `calculate_batch_size()`, `extract()`, `batch_size()`
- **`DeltaWrite`** — `ensure_table()`, `append_batch()`, `overwrite_table()`, `read_hwm()`, `get_schema()`
- **`StateManage`** — `load_or_default()`, `update_table()`

### Adapter Structs

Production implementations wrap the real module logic:

- `SchemaInspectorAdapter` → delegates to `discovery::SchemaInspector`
- `ExtractorAdapter` → delegates to `extractor::BatchExtractor`
- `DeltaWriterAdapter` → delegates to `writer::DeltaWriter`
- `StateManageAdapter` → delegates to `state::AppState`

## Shutdown Signal

The orchestrator checks the shutdown signal (via `watch::Receiver<bool>`) before processing each table. If the signal is set:

1. No new tables are started
2. The in-flight batch completes normally
3. The orchestrator returns `ExitCode::Success` (all tables that were started completed)

The signal is set from `main.rs` via a `tokio::signal` handler that sends `true` on SIGTERM/SIGINT.

## MariaDB to Arrow Type Mapping

Used by `column_info_to_v57_schema()` to build the Delta table schema from MariaDB column metadata:

| MariaDB Type | Arrow Type |
|---|---|
| tinyint | Int32 |
| smallint | Int16 |
| int, mediumint | Int32 |
| bigint | Int64 |
| float | Float32 |
| double, decimal | Float64 |
| varchar, char, text, json | Utf8 |
| date | Date32 |
| datetime, timestamp | Timestamp(Microsecond, None) |
| boolean, bool | Boolean |
| blob | Binary |

Unsupported types (e.g., geometry, enum) cause `column_info_to_v57_schema()` to return an error, preventing table creation.

## State Updates

On successful table processing, the orchestrator writes:

```rust
TableState {
    last_run_at: Some(<ISO 8601 timestamp>),
    last_run_status: Some("success"),
    last_run_rows: Some(<total rows extracted>),
    last_run_duration_ms: Some(<elapsed ms>),
    extraction_mode: Some("incremental" | "full_refresh"),
    schema_columns_hash: Some(<sha256 hash of column names+types>),
}
```

On failure, the orchestrator writes:

```rust
TableState {
    last_run_at: Some(<ISO 8601 timestamp>),
    last_run_status: Some("failed"),
    last_run_rows: None,
    last_run_duration_ms: None,
    extraction_mode: None,
    schema_columns_hash: None,
}
```
