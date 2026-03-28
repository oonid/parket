## ADDED Requirements

### Requirement: Discover table columns from information_schema
The system SHALL query `information_schema.columns` to retrieve column names, data types, and column types for each configured table, ordered by ordinal position.

#### Scenario: Table exists with columns
- **WHEN** a configured table exists in MariaDB
- **THEN** the system SHALL return a list of columns with name, data_type, and column_type for that table

#### Scenario: Table does not exist
- **WHEN** a configured table does not exist in the database specified by DATABASE_URL
- **THEN** the system SHALL log an error, mark the table as failed, and continue with the next table

### Requirement: Filter unsupported column types
The system SHALL exclude columns with unsupported types from the column list and log a warning for each excluded column. Unsupported types: GEOMETRY, POINT, LINESTRING, POLYGON, GEOMETRYCOLLECTION, MULTIPOLYGON.

#### Scenario: Table contains GEOMETRY column
- **WHEN** a table has a column of type GEOMETRY
- **THEN** the system SHALL exclude it from the SELECT column list and log: "skipping unsupported column type: {column_name} ({column_type})"

#### Scenario: All columns are supported
- **WHEN** all columns have supported types
- **THEN** the system SHALL include all columns in the SELECT column list

### Requirement: Detect extraction mode
The system SHALL detect the extraction mode as `Incremental` if the table has both an `updated_at` column (type TIMESTAMP or DATETIME) and an `id` column. Otherwise, the mode SHALL be `FullRefresh`. If a per-table mode override is set in configuration, the override SHALL take precedence.

#### Scenario: Table has both updated_at and id
- **WHEN** a table has columns named `updated_at` (TIMESTAMP or DATETIME) and `id`
- **THEN** the system SHALL detect mode as `Incremental`

#### Scenario: Table missing updated_at
- **WHEN** a table has `id` but no `updated_at` column
- **THEN** the system SHALL detect mode as `FullRefresh`

#### Scenario: Table missing id
- **WHEN** a table has `updated_at` but no `id` column
- **THEN** the system SHALL detect mode as `FullRefresh`

#### Scenario: Mode override is set
- **WHEN** `TABLE_MODE_<name>=full_refresh` is set and auto-detection would return Incremental
- **THEN** the system SHALL use `full_refresh` mode

### Requirement: Retrieve AVG_ROW_LENGTH for batch sizing
The system SHALL query `information_schema.tables` to retrieve `AVG_ROW_LENGTH` for each configured table.

#### Scenario: AVG_ROW_LENGTH is available
- **WHEN** `AVG_ROW_LENGTH` is a positive integer
- **THEN** the system SHALL use it as the initial estimate for batch size calculation

#### Scenario: AVG_ROW_LENGTH is 0 or NULL
- **WHEN** `AVG_ROW_LENGTH` is 0 or NULL (empty/new table)
- **THEN** the system SHALL fall back to `DEFAULT_BATCH_SIZE` from configuration

### Requirement: Check index on updated_at for incremental tables
The system SHALL check whether the `updated_at` column has an index when a table is detected as Incremental. If no index exists, the system SHALL log a warning.

#### Scenario: updated_at has no index
- **WHEN** an Incremental table's `updated_at` column has no index
- **THEN** the system SHALL log: "table {name} has no index on updated_at — incremental queries may be slow"

#### Scenario: updated_at has an index
- **WHEN** the `updated_at` column is indexed
- **THEN** the system SHALL proceed without warning

### Requirement: Compute schema columns hash
The system SHALL compute a hash of the column list (names and types) for schema change detection between runs.

#### Scenario: Schema unchanged between runs
- **WHEN** the computed hash matches the hash stored in state.json
- **THEN** the system SHALL proceed with extraction

#### Scenario: Schema changed between runs
- **WHEN** the computed hash differs from the stored hash
- **THEN** the system SHALL log a warning about schema change and proceed with schema evolution checks
