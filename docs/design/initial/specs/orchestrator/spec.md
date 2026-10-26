## ADDED Requirements

### Requirement: Process tables sequentially
The system SHALL process configured tables one at a time in the order specified. Only one table SHALL be actively extracted at any point in time.

#### Scenario: Three tables configured
- **WHEN** `TABLES=orders,customers,products` and orders completes successfully
- **THEN** the system SHALL begin processing customers next, then products

### Requirement: Extract and load in batches until exhausted
For each table, the system SHALL run the extract-and-load cycle repeatedly until connector-x returns 0 rows (Incremental) or the full dataset is written (FullRefresh).

#### Scenario: Incremental table with 3 batches
- **WHEN** an Incremental table has 130000 rows matching the query and batch_size is 50000
- **THEN** the system SHALL execute 3 extraction cycles (50000, 50000, 30000 rows), committing each batch to Delta Lake and updating HWM

#### Scenario: FullRefresh table
- **WHEN** a FullRefresh table is extracted
- **THEN** the system SHALL extract all data and write with SaveMode::Overwrite in one pass

### Requirement: Skip failed tables and continue
The system SHALL handle per-table errors by logging the error, marking the table as failed in state.json, and continuing with the next table. The system SHALL NOT abort the entire run due to a single table failure.

#### Scenario: Second table fails
- **WHEN** the `customers` table extraction fails with a connector-x error
- **THEN** the system SHALL log the error, mark `customers` as failed in state.json, and proceed to extract `products`

### Requirement: Exit with appropriate code
The system SHALL exit with code 0 if all tables succeed, code 1 if any table fails, and code 2 on fatal misconfiguration.

#### Scenario: All tables succeed
- **WHEN** all configured tables are processed without error
- **THEN** the system SHALL exit with code 0

#### Scenario: Partial failure
- **WHEN** some tables succeed and some fail
- **THEN** the system SHALL exit with code 1

#### Scenario: Fatal misconfiguration
- **WHEN** configuration is invalid (missing required env vars, bad DATABASE_URL, unreachable database)
- **THEN** the system SHALL exit with code 2 before processing any tables

### Requirement: Handle schema evolution on startup
For each Incremental table, the system SHALL compare the current MariaDB schema with the existing Delta table's Arrow schema before extraction. Column additions SHALL be logged as warnings and excluded from SELECT. Column drops, renames, or type changes SHALL cause the table to be skipped (marked as failed).

#### Scenario: Column added to MariaDB table
- **WHEN** a new column `phone` exists in MariaDB but not in the Delta table schema
- **THEN** the system SHALL log a warning and exclude `phone` from the SELECT column list

#### Scenario: Column dropped from MariaDB table
- **WHEN** a column `email` exists in the Delta table schema but not in MariaDB
- **THEN** the system SHALL log an error, skip the table, and mark it as failed

#### Scenario: Column type changed
- **WHEN** column `age` changed from INT to BIGINT in MariaDB
- **THEN** the system SHALL log an error, skip the table, and mark it as failed
