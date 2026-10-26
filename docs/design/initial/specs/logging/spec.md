## ADDED Requirements

### Requirement: Structured logging to stdout
The system SHALL emit structured log output to stdout using the `tracing` crate with `tracing-subscriber`. Log level SHALL be controlled by the `RUST_LOG` environment variable (default: `parket=info`).

#### Scenario: Default log level
- **WHEN** `RUST_LOG` is not set
- **THEN** the system SHALL emit INFO and above level logs from the `parket` crate

#### Scenario: Debug log level configured
- **WHEN** `RUST_LOG=parket=debug`
- **THEN** the system SHALL emit DEBUG and above level logs from the `parket` crate

### Requirement: Log extraction progress
The system SHALL log key events with structured fields: table name, batch rows, arrow bytes, HWM values, extraction mode, and timing.

#### Scenario: Batch extracted
- **WHEN** a batch of 45000 rows is extracted from the `orders` table
- **THEN** the system SHALL log: `INFO parket::extractor batch extracted table=orders rows=45000 arrow_bytes={size}`

#### Scenario: Batch committed
- **WHEN** a batch is committed to Delta Lake
- **THEN** the system SHALL log: `INFO parket::writer batch committed table={name} rows={n} hwm_updated_at={ts} hwm_last_id={id}`

#### Scenario: Table failed
- **WHEN** a table extraction fails
- **THEN** the system SHALL log: `ERROR parket::orchestrator table failed table={name} error="{message}"`

### Requirement: Log run summary
The system SHALL log a summary line at the end of the run with succeeded count, failed count, and total duration.

#### Scenario: Run completes
- **WHEN** the run finishes processing all tables
- **THEN** the system SHALL log: `INFO parket::orchestrator run complete succeeded={n} failed={m} duration_ms={ms}`
