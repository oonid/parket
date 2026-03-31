# Signal Handling

Parket supports graceful shutdown via SIGTERM and SIGINT (Ctrl+C). The signal handler is implemented in `src/orchestrator.rs` as `SignalHandler`, using `tokio::signal` and a `watch` channel to communicate with the orchestrator.

## Architecture

```
SignalHandler::install()
 │
 └─ tokio::spawn
     ├─ ctrl_c() #1  ──→  tx.send(true)  ──→  orchestrator sees shutdown
     │                                       orchestrator finishes in-flight batch
     │                                       orchestrator skips remaining tables
     │                                       orchestrator returns ExitCode
     └─ ctrl_c() #2  ──→  process::exit(130)
```

## Graceful Shutdown Sequence

1. First signal (SIGTERM or SIGINT) received
2. `SignalHandler` sends `true` on the `watch::Sender<bool>` channel
3. Orchestrator's `check_shutdown()` reads `true` from the `watch::Receiver`
4. **Between tables**: the orchestrator skips all remaining tables immediately
5. **During incremental batch loop**: the orchestrator finishes writing the current in-flight batch (including Delta commit and HWM update), then breaks out of the batch loop
6. The orchestrator returns `ExitCode::Success` (all tables that were started completed)
7. `main.rs` exits with code 0

## Double-Signal Handling

If a second SIGINT is received while the first graceful shutdown is still in progress:

1. The spawned signal handler task calls `std::process::exit(130)` immediately
2. No cleanup is performed — the in-flight batch may be partially written
3. Exit code 130 = 128 + SIGINT(2)

**Note:** Since the orchestrator writes HWM atomically with each batch commit via Delta `commitInfo`, a forced exit mid-batch is safe — the HWM reflects the last *completed* batch, not the in-flight one.

## API

```rust
use crate::orchestrator::SignalHandler;

// Create handler and get the shutdown receiver
let (handler, shutdown_rx) = SignalHandler::new();

// Install the signal handler (spawns background task)
handler.install().await;

// Pass shutdown_rx to Orchestrator
let orchestrator = Orchestrator::new(config, ..., shutdown_rx, state_path);
```

### SignalHandler

| Method | Description |
|---|---|
| `new()` | Creates a handler and returns `(handler, watch::Receiver<bool>)` |
| `install(self)` | Spawns a background task that listens for SIGINT |

### Orchestrator shutdown integration

The orchestrator checks the shutdown signal at two points:

| Check Point | Method | Behavior |
|---|---|---|
| Before each table | `run()` | `if check_shutdown() { break; }` |
| Before each batch in incremental loop | `process_incremental()` | `if check_shutdown() { break; }` |

The `check_shutdown()` method reads the current value from the `watch::Receiver<bool>` without blocking.

## tokio::signal Usage

Parket uses `tokio::signal::ctrl_c()` which handles both SIGINT and SIGTERM on Unix systems. On Windows, it handles Ctrl+C and Ctrl+Break.

No platform-specific signal handling code is needed — `tokio::signal` abstracts this. The handler is installed after the tokio runtime is created, which is required by `ctrl_c()`.

## Testing

Signal handler tests use the `watch` channel directly without actual OS signals:

| Test | What it verifies |
|---|---|
| `shutdown_signal_between_tables_skips_remaining` | Signal set before run → skips all tables |
| `shutdown_signal_during_batch_loop_stops_after_current_batch` | Signal sent mid-batch → finishes current, stops loop |
| `shutdown_signal_stops_processing` | Pre-existing test from task 9 — signal set before orchestrator starts |
| `signal_handler_watch_channel_starts_false` | Channel initialized to `false` |

The double-signal exit-130 behavior is by design tested indirectly — `SignalHandler::install()` spawns a real signal listener, so unit tests verify the channel communication without triggering `process::exit`.
