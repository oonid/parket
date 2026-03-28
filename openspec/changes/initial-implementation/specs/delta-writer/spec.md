## ADDED Requirements

### Requirement: Ensure Delta table exists before writing
The system SHALL check if a Delta table exists at the target S3 path (`s3://{S3_BUCKET}/{S3_PREFIX}/{table_name}/`). If it does not exist, the system SHALL create it using the Arrow schema extracted from the first RecordBatch.

#### Scenario: First run — Delta table does not exist
- **WHEN** no Delta table exists at the S3 path for a given table
- **THEN** the system SHALL create a new Delta table using the Arrow schema from the extracted RecordBatch

#### Scenario: Subsequent run — Delta table exists
- **WHEN** a Delta table already exists at the S3 path
- **THEN** the system SHALL proceed to write without creating a new table

### Requirement: Append RecordBatch for Incremental mode
The system SHALL write RecordBatches to Delta Lake using `SaveMode::Append` for tables in Incremental mode.

#### Scenario: Incremental batch commit
- **WHEN** a RecordBatch is written for an Incremental table
- **THEN** the system SHALL append the batch to the existing Delta table using SaveMode::Append

### Requirement: Overwrite for FullRefresh mode
The system SHALL write the full dataset to Delta Lake using `SaveMode::Overwrite` for tables in FullRefresh mode. Overwrite SHALL only commit on success — if extraction fails mid-run, the old data remains intact.

#### Scenario: FullRefresh commit
- **WHEN** all data is extracted for a FullRefresh table
- **THEN** the system SHALL write using SaveMode::Overwrite, atomically replacing the entire Delta table contents

#### Scenario: FullRefresh extraction fails mid-run
- **WHEN** extraction fails after reading some batches but before committing
- **THEN** the existing Delta table data SHALL remain unchanged

### Requirement: Record HWM in Delta commitInfo metadata
The system SHALL extract the High Watermark (max `updated_at` and corresponding `id`) from each committed RecordBatch and write it as custom metadata in the Delta `commitInfo`. HWM SHALL be recorded as `hwm_updated_at` and `hwm_last_id` string values.

#### Scenario: HWM extracted from batch
- **WHEN** a RecordBatch is committed with max `updated_at=2026-03-28T10:00:00Z` and corresponding `id=98765`
- **THEN** the commitInfo SHALL contain `hwm_updated_at: "2026-03-28T10:00:00Z"` and `hwm_last_id: "98765"`

### Requirement: Read HWM from Delta table on startup
The system SHALL read the latest HWM from the Delta table's commitInfo metadata on startup for each Incremental table. If the Delta table does not exist, HWM SHALL be None (start from beginning).

#### Scenario: Delta table exists with prior HWM
- **WHEN** the Delta table exists and the latest commitInfo contains `hwm_updated_at` and `hwm_last_id`
- **THEN** the system SHALL use these values as the starting cursor for incremental extraction

#### Scenario: Delta table does not exist
- **WHEN** no Delta table exists for the table
- **THEN** the system SHALL set HWM to None and extract from the beginning

#### Scenario: Delta table exists but no HWM in metadata
- **WHEN** the Delta table exists but commitInfo has no HWM fields (e.g., written by another tool)
- **THEN** the system SHALL log a warning and extract from the beginning (FullRefresh fallback)

### Requirement: Handle S3 connection errors
The system SHALL handle S3/MinIO connection errors gracefully — log the error, mark the table as failed, and continue with the next table. The system SHALL NOT update any state for the failed table.

#### Scenario: S3 endpoint unreachable
- **WHEN** the S3 endpoint is unreachable
- **THEN** the system SHALL log an error with the connection details and skip the table
