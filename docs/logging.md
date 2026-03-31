# Logging

Parket uses the `tracing` crate with `tracing-subscriber` for structured logging to stdout.

## Initialization

`init_tracing()` in `main.rs` sets up the subscriber:

```rust
fn init_tracing() {
    use tracing_subscriber::EnvFilter;
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("parket=info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .with_level(true)
        .with_thread_ids(false)
        .with_file(false)
        .with_line_number(false)
        .init();
}
```

## Log Levels

Controlled by the `RUST_LOG` environment variable:

| `RUST_LOG` | Effect |
|---|---|
| *(not set)* | `parket=info` — INFO and above from parket only |
| `parket=debug` | DEBUG and above from parket |
| `parket=trace` | TRACE and above from parket |
| `debug` | DEBUG and above from all crates |
| `parket=debug,sqlx=warn` | DEBUG from parket, WARN from sqlx |

## Structured Fields

All log statements use structured fields for machine-parseable output.

### Batch Extracted

Logged by the orchestrator after each extract call:

```
INFO parket::orchestrator batch extracted table=orders rows=45000 arrow_bytes=524288
```

| Field | Type | Source |
|---|---|---|
| `table` | &str | table name from orchestrator context |
| `rows` | u64 | total rows across all v54 batches |
| `arrow_bytes` | usize | total Arrow memory across all v54 batches |

The extractor also logs per-batch metrics at `INFO` level:

```
INFO parket::extractor batch extracted rows=45000 arrow_bytes=524288
```

### Batch Committed

Logged by the writer after each Delta commit:

```
INFO parket::writer batch committed table=orders rows=45000 hwm_updated_at=Some("2026-03-28 10:00:00") hwm_last_id=Some(98765)
```

| Field | Type | Source |
|---|---|---|
| `table` | &str | table name |
| `rows` | usize | total rows across all batches |
| `hwm_updated_at` | Option<&str> | HWM timestamp from the commit |
| `hwm_last_id` | Option<i64> | HWM last ID from the commit |

### Table Failed

Logged by the orchestrator when a table extraction fails:

```
ERROR parket::orchestrator table failed table=orders error="connection refused"
```

| Field | Type | Source |
|---|---|---|
| `table` | &str | table name |
| `error` | Display | error message from the failed operation |

### Run Complete

Logged by the orchestrator at the end of every run:

```
INFO parket::orchestrator run complete succeeded=5 failed=0 duration_ms=3200
```

| Field | Type | Source |
|---|---|---|
| `succeeded` | u32 | count of tables that succeeded |
| `failed` | u32 | count of tables that failed |
| `duration_ms` | u64 | total wall-clock duration of the run |

### Other Events

| Event | Level | Module | Fields |
|---|---|---|---|
| Batch size calculated | DEBUG | extractor | `batch_size`, `avg_row_length` |
| Adaptive sizing applied | INFO | extractor | `old_batch_size`, `new_batch_size`, `ratio` |
| Hard ceiling enforced | WARN | extractor | `actual_bytes`, `ceiling_bytes`, `old_batch_size`, `new_batch_size` |
| Delta table created | INFO | writer | `table` |
| Delta table exists | INFO | writer | `table` |
| Column discovered | INFO | discovery | count, table |
| Unsupported column skipped | WARN | discovery | `name`, `type` |
| Schema evolution warning | WARN | orchestrator | `column` |
| Shutdown signal received | INFO | orchestrator | — |
| State file corrupted | WARN | state | — |

## Example Output

```
2026-03-31T12:00:00.123456Z  INFO parket::discovery: discovered 8 columns for table orders
2026-03-31T12:00:00.456789Z DEBUG parket::extractor: batch extracted rows=45000 arrow_bytes=524288
2026-03-31T12:00:01.234567Z  INFO parket::orchestrator: batch extracted table=orders rows=45000 arrow_bytes=524288
2026-03-31T12:00:02.345678Z  INFO parket::writer: batch committed table=orders rows=45000 hwm_updated_at=Some("2026-03-28 10:00:00") hwm_last_id=Some(98765)
2026-03-31T12:00:02.350000Z  INFO parket::orchestrator: table succeeded table=orders
2026-03-31T12:00:02.400000Z  INFO parket::orchestrator: run complete succeeded=1 failed=0 duration_ms=2345
```

## Testing

Logging tests use `tracing_subscriber::fmt().with_test_writer()` to capture output without polluting test output:

| Test | Verifies |
|---|---|
| `default_log_level_is_info` | EnvFilter defaults to `parket=info` when `RUST_LOG` unset |
| `debug_level_filter_allows_debug` | `parket=debug` filter resolves correctly |
| `structured_fields_in_log_statements` | All spec-required structured fields compile and emit |
| `debug_level_captures_debug_messages` | DEBUG-level messages are captured at debug filter level |
| `extractor_batch_extracted_log_matches_spec` | batch extracted fields: table, rows, arrow_bytes |
| `writer_batch_committed_log_matches_spec` | batch committed fields: table, rows, hwm_updated_at, hwm_last_id |
| `orchestrator_table_failed_log_matches_spec` | table failed fields: table, error |
| `orchestrator_run_complete_log_matches_spec` | run complete fields: succeeded, failed, duration_ms |
| `init_tracing_filter_construction` | EnvFilter construction with default and explicit levels |
| `env_filter_parses_rust_log_env` | EnvFilter::try_from parses RUST_LOG-style strings |
