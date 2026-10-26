## ADDED Requirements

### Requirement: Load configuration from environment variables
The system SHALL load all configuration from environment variables, using `dotenvy` to read `.env` file as fallback. Required variables: `DATABASE_URL`, `S3_BUCKET`, `S3_ACCESS_KEY_ID`, `S3_SECRET_ACCESS_KEY`, `TABLES`, `TARGET_MEMORY_MB`.

#### Scenario: All required variables present
- **WHEN** all required environment variables are set with valid values
- **THEN** the system SHALL parse and return a validated `Config` struct

#### Scenario: Missing required variable
- **WHEN** any required environment variable is missing or empty
- **THEN** the system SHALL exit with code 2 and log a clear error message identifying the missing variable

### Requirement: Load optional configuration with defaults
The system SHALL support optional environment variables with defaults: `S3_ENDPOINT` (none), `S3_REGION` ("us-east-1"), `S3_PREFIX` ("parket"), `DEFAULT_BATCH_SIZE` (10000), `RUST_LOG` ("info").

#### Scenario: Optional variables omitted
- **WHEN** optional environment variables are not set
- **THEN** the system SHALL use default values

#### Scenario: Optional variables provided
- **WHEN** optional environment variables are set
- **THEN** the system SHALL use the provided values

### Requirement: Parse table list from comma-separated string
The system SHALL parse the `TABLES` environment variable as a comma-separated list of table names, trimming whitespace from each name.

#### Scenario: Multiple tables configured
- **WHEN** `TABLES=orders, customers, products`
- **THEN** the system SHALL produce a vector of `["orders", "customers", "products"]`

#### Scenario: Single table configured
- **WHEN** `TABLES=orders`
- **THEN** the system SHALL produce a vector of `["orders"]`

#### Scenario: Empty table list
- **WHEN** `TABLES` is empty or whitespace-only
- **THEN** the system SHALL exit with code 2 and log an error

### Requirement: Support per-table mode overrides
The system SHALL read `TABLE_MODE_<NAME>` environment variables to override extraction mode per table. Valid values: `auto`, `incremental`, `full_refresh`. Default is `auto`.

#### Scenario: Override specified for a table
- **WHEN** `TABLE_MODE_orders=incremental` is set and `orders` is in the table list
- **THEN** the system SHALL use `incremental` mode for `orders` regardless of auto-detection

#### Scenario: No override for a table
- **WHEN** no `TABLE_MODE_<NAME>` is set for a given table
- **THEN** the system SHALL use `auto` mode (determined by schema discovery)

### Requirement: Validate DATABASE_URL format
The system SHALL validate that `DATABASE_URL` starts with `mysql://`.

#### Scenario: Invalid DATABASE_URL scheme
- **WHEN** `DATABASE_URL` starts with `postgres://` or any non-mysql scheme
- **THEN** the system SHALL exit with code 2 and log an error about unsupported scheme
