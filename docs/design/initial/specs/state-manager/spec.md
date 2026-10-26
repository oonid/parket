## ADDED Requirements

### Requirement: Read and write operational state to state.json
The system SHALL maintain a `state.json` file in the working directory containing per-table operational metadata: `last_run_at`, `last_run_status` (success/failed), `last_run_rows`, `last_run_duration_ms`, `extraction_mode`, `schema_columns_hash`.

#### Scenario: First run with no state.json
- **WHEN** `state.json` does not exist
- **THEN** the system SHALL treat all tables as never having been run and proceed without error

#### Scenario: State file exists with valid JSON
- **WHEN** `state.json` exists and contains valid JSON
- **THEN** the system SHALL parse and use the operational state for logging and status tracking

#### Scenario: State file is corrupted
- **WHEN** `state.json` exists but contains invalid JSON
- **THEN** the system SHALL log a warning and proceed as if no state exists (treat as first run)

### Requirement: Update state after each table run
The system SHALL update `state.json` with the table's run metadata after each table completes (success or failure).

#### Scenario: Table completes successfully
- **WHEN** a table extraction completes without error
- **THEN** the system SHALL write `last_run_status: "success"`, `last_run_rows` (total rows extracted), `last_run_duration_ms`, `extraction_mode`, and `schema_columns_hash` to state.json for that table

#### Scenario: Table fails
- **WHEN** a table extraction fails with an error
- **THEN** the system SHALL write `last_run_status: "failed"` and the current timestamp to state.json for that table

### Requirement: State is not required for HWM
The system SHALL NOT use `state.json` for High Watermark storage. HWM SHALL be read from Delta table commitInfo metadata only.

#### Scenario: state.json is deleted between runs
- **WHEN** `state.json` is deleted between runs
- **THEN** the system SHALL still extract correctly using HWM from Delta table metadata, logging a warning about missing operational state
