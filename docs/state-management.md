# State Management

Parket maintains a `state.json` file in the working directory for operational bookkeeping -- tracking per-table run status, timing, and schema hashes.

## state.json Schema

```json
{
  "tables": {
    "orders": {
      "last_run_at": "2026-03-28T10:00:00Z",
      "last_run_status": "success",
      "last_run_rows": 50000,
      "last_run_duration_ms": 3200,
      "extraction_mode": "incremental",
      "schema_columns_hash": "abc123def456"
    }
  }
}
```

### Fields per Table

| Field | Type | Description |
|-------|------|-------------|
| `last_run_at` | `string?` | ISO 8601 timestamp of last run |
| `last_run_status` | `string?` | `"success"` or `"failed"` |
| `last_run_rows` | `u64?` | Total rows extracted in last run |
| `last_run_duration_ms` | `u64?` | Duration of last run in milliseconds |
| `extraction_mode` | `string?` | `"incremental"` or `"full_refresh"` |
| `schema_columns_hash` | `string?` | Hash of column names and types for change detection |

## HWM is NOT stored here

The High Watermark (`hwm_updated_at`, `hwm_last_id`) is stored exclusively in Delta Lake `commitInfo` metadata -- not in `state.json`. This design ensures atomicity: HWM is committed alongside the data in a single Delta transaction. If the process crashes between writing data and updating state, the HWM in Delta is still correct.

`state.json` is purely operational bookkeeping. Deleting it between runs does not affect data correctness -- Parket reads HWM from the Delta log.

## Atomic Write Strategy

When `update_table()` writes `state.json`, it uses an atomic write pattern:

1. Serialize `AppState` to JSON
2. Write to a temporary file (`state.tmp`)
3. Flush and close the temporary file
4. Rename `state.tmp` to `state.json` (atomic on POSIX filesystems)

This ensures `state.json` is never in a partially-written state.

## Error Handling

| Condition | Behavior |
|-----------|----------|
| `state.json` does not exist | Return empty `AppState` -- treat as first run |
| `state.json` contains invalid JSON | `load()` returns error; `load_or_warn()` logs warning and returns empty state |
| `state.json` is empty file | `load()` returns error (invalid JSON) |

## API

```rust
// Load state (returns Err on corruption)
let state = AppState::load(Path::new("state.json"))?;

// Load state (logs warning on corruption, returns default)
let state = AppState::load_or_warn(Path::new("state.json"));

// Update a table's state (writes atomically)
state.update_table("orders", table_state, Path::new("state.json"))?;
```
