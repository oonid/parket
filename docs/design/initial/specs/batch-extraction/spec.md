## ADDED Requirements

### Requirement: Execute SQL via connector-x streaming API
The system SHALL execute SQL queries using `connector-x` streaming API (`new_record_batch_iter`) to produce Arrow `RecordBatch` iterators. The system SHALL NOT use the bulk `get_arrow()` API.

#### Scenario: Query returns rows
- **WHEN** a SQL query is executed and the result set is non-empty
- **THEN** the system SHALL return RecordBatches via the iterator, each bounded by the configured batch_size

#### Scenario: Query returns zero rows
- **WHEN** a SQL query is executed and the result set is empty
- **THEN** the system SHALL return an empty iterator (no RecordBatches), signaling end-of-table

### Requirement: Calculate initial batch size from AVG_ROW_LENGTH
The system SHALL calculate `dynamic_batch_size = (TARGET_MEMORY_MB * 1024 * 1024) / AVG_ROW_LENGTH`. If AVG_ROW_LENGTH is 0 or NULL, the system SHALL use `DEFAULT_BATCH_SIZE`.

#### Scenario: Typical batch size calculation
- **WHEN** `TARGET_MEMORY_MB=512` and `AVG_ROW_LENGTH=100`
- **THEN** `dynamic_batch_size` SHALL be `5368709` (floor of 512*1024*1024/100)

#### Scenario: AVG_ROW_LENGTH is 0
- **WHEN** `AVG_ROW_LENGTH=0` and `DEFAULT_BATCH_SIZE=10000`
- **THEN** `dynamic_batch_size` SHALL be `10000`

### Requirement: Adapt batch size after first batch
The system SHALL measure actual Arrow memory of the first RecordBatch using `RecordBatch::get_array_memory_size()` and recalculate: `estimated_bytes_per_row = actual_arrow_bytes / row_count`. If the estimate differs from the initial by more than 2x, the system SHALL update `dynamic_batch_size` for subsequent batches.

#### Scenario: Arrow memory larger than estimated
- **WHEN** first batch has 10000 rows occupying 50MB (5000 bytes/row) but AVG_ROW_LENGTH suggested 100 bytes/row
- **THEN** the system SHALL recalculate batch_size to stay within TARGET_MEMORY_MB

#### Scenario: Arrow memory close to estimate
- **WHEN** first batch memory is within 2x of the initial estimate
- **THEN** the system SHALL keep the current batch_size

### Requirement: Enforce hard memory ceiling
The system SHALL set a hard memory ceiling of `2 * TARGET_MEMORY_MB`. If any single RecordBatch exceeds this ceiling, the system SHALL log a warning and halve the batch size for subsequent extractions.

#### Scenario: Batch exceeds hard ceiling
- **WHEN** a RecordBatch memory size exceeds `2 * TARGET_MEMORY_MB * 1024 * 1024`
- **THEN** the system SHALL log a warning and reduce batch_size by 50%
