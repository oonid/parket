# Configuration Reference

All configuration is provided through environment variables. Parket uses `dotenvy` to load a `.env` file as a fallback, so you can either export variables in your shell or define them in `.env`.

## Required Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `DATABASE_URL` | MariaDB connection string. Must start with `mysql://`. | `mysql://user:pass@db:3306/mydb` |
| `S3_BUCKET` | Target S3/MinIO bucket name. | `data-lake` |
| `S3_ACCESS_KEY_ID` | S3 access key. | `minioadmin` |
| `S3_SECRET_ACCESS_KEY` | S3 secret key. | `minioadmin` |
| `TABLES` | Comma-separated list of MariaDB tables to extract. Whitespace is trimmed. | `orders,customers,products` |
| `TARGET_MEMORY_MB` | Memory budget per batch in megabytes. Must be a positive integer. | `512` |

## Optional Variables with Defaults

| Variable | Default | Description |
|----------|---------|-------------|
| `S3_ENDPOINT` | _(none)_ | Custom S3 endpoint URL (required for MinIO). |
| `S3_REGION` | `us-east-1` | S3 region. |
| `S3_PREFIX` | `parket` | Path prefix within the bucket. Each table lands at `s3://{S3_BUCKET}/{S3_PREFIX}/{table}/`. |
| `DEFAULT_BATCH_SIZE` | `10000` | Fallback batch row count when `AVG_ROW_LENGTH` is unavailable. |
| `RUST_LOG` | `info` | Log level filter (see [Logging](logging.md)). |

## Per-Table Extraction Mode Overrides

By default, Parket auto-detects the extraction mode for each table based on schema (see [Schema Discovery](schema-discovery.md)). You can override this per table:

```
TABLE_MODE_<TABLENAME>=<mode>
```

Valid modes:

| Mode | Behavior |
|------|----------|
| `auto` _(default)_ | Auto-detect from schema: Incremental if `updated_at` + `id` exist, otherwise FullRefresh. |
| `incremental` | Force incremental extraction with cursor-based pagination on `(updated_at, id)`. |
| `full_refresh` | Force full table overwrite every run. |

**Example:**

```env
TABLES=orders,customers,products
TABLE_MODE_orders=incremental
TABLE_MODE_products=full_refresh
# customers uses auto-detection
```

## Validation Rules

Parket validates configuration at startup and exits with code 2 on any failure:

- `DATABASE_URL` must start with `mysql://` — other schemes (e.g. `postgres://`) are rejected.
- `TABLES` must not be empty or whitespace-only.
- `TARGET_MEMORY_MB` must parse as a positive integer (> 0).
- All required variables must be present and non-empty.

Error messages identify the specific missing or invalid variable.

## Example `.env`

```env
DATABASE_URL=mysql://readonly:secret@mariadb.internal:3306/production
S3_BUCKET=data-lake
S3_ACCESS_KEY_ID=minioadmin
S3_SECRET_ACCESS_KEY=minioadmin
S3_ENDPOINT=http://minio.internal:9000
S3_REGION=us-east-1
S3_PREFIX=parket
TABLES=orders,customers,products
TARGET_MEMORY_MB=512
DEFAULT_BATCH_SIZE=10000
RUST_LOG=parket=info
TABLE_MODE_orders=incremental
```
