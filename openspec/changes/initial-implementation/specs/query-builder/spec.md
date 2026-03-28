## ADDED Requirements

### Requirement: Build Incremental mode SQL query with cursor-based pagination
The system SHALL construct SQL queries for Incremental mode using a cursor-based windowing pattern on `(updated_at, id)`. The query SHALL use the format:
`SELECT {columns} FROM {table} WHERE (updated_at = ? AND id > ?) OR (updated_at > ?) ORDER BY updated_at ASC, id ASC LIMIT {batch_size}`
with HWM values interpolated as parameters.

#### Scenario: First incremental extraction (no HWM)
- **WHEN** no Delta table exists for the table (first run)
- **THEN** the system SHALL generate: `SELECT {columns} FROM {table} ORDER BY updated_at ASC, id ASC LIMIT {batch_size}`

#### Scenario: Subsequent incremental extraction with HWM
- **WHEN** HWM is `updated_at=2026-03-28T09:00:00Z, last_id=500`
- **THEN** the system SHALL generate: `SELECT {columns} FROM {table} WHERE (updated_at = '2026-03-28 09:00:00' AND id > 500) OR (updated_at > '2026-03-28 09:00:00') ORDER BY updated_at ASC, id ASC LIMIT {batch_size}`

### Requirement: Build FullRefresh mode SQL query
The system SHALL construct SQL queries for FullRefresh mode using: `SELECT {columns} FROM {table}`.

#### Scenario: FullRefresh table extraction
- **WHEN** a table is in FullRefresh mode
- **THEN** the system SHALL generate: `SELECT {columns} FROM {table}` with no WHERE clause and no LIMIT (connector-x streaming handles batching)

### Requirement: Use explicit column list in queries
The system SHALL use the explicit column list from schema discovery in all generated SQL queries, never using `SELECT *`.

#### Scenario: Columns discovered successfully
- **WHEN** schema discovery returns columns `[id, name, updated_at]`
- **THEN** the generated query SHALL use `SELECT id, name, updated_at FROM ...`

### Requirement: Sanitize table and column names
The system SHALL backtick-quote all table and column names in generated SQL to prevent issues with reserved words.

#### Scenario: Table name is a reserved word
- **WHEN** a table is named `order` (a SQL reserved word)
- **THEN** the system SHALL generate: `` SELECT `col` FROM `order` ``

#### Scenario: Column name contains special characters
- **WHEN** a column name contains spaces or special characters
- **THEN** the system SHALL backtick-quote the column name in the SQL
