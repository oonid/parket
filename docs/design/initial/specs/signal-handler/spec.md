## ADDED Requirements

### Requirement: Catch SIGTERM and SIGINT for graceful shutdown
The system SHALL install a signal handler using `tokio::signal` to catch SIGTERM and SIGINT. On signal reception, the system SHALL stop initiating new batches and allow the current in-flight batch to complete.

#### Scenario: SIGTERM received during batch processing
- **WHEN** SIGTERM is received while a batch is being extracted or written
- **THEN** the system SHALL let the current batch finish (including Delta commit and HWM update), skip remaining tables, and exit with code 0

#### Scenario: SIGTERM received between tables
- **WHEN** SIGTERM is received between table extractions (no in-flight batch)
- **THEN** the system SHALL skip remaining tables and exit with code 0

#### Scenario: SIGINT received
- **WHEN** SIGINT (Ctrl+C) is received
- **THEN** the system SHALL behave identically to SIGTERM (graceful shutdown)

### Requirement: Exit with code 130 on forced termination
The system SHALL exit with code 130 (128 + SIGINT) if a second signal is received while a batch is still in-flight.

#### Scenario: Second signal during batch
- **WHEN** a second SIGINT is received while the first graceful shutdown is still in progress
- **THEN** the system SHALL exit immediately with code 130
