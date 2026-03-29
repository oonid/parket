# Schema Discovery

Parket queries MariaDB `information_schema` to discover table structure, detect extraction mode, estimate row sizes, and compute schema hashes.

## Column Discovery

Queries `information_schema.columns` for each configured table:

```sql
SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE
FROM information_schema.columns
WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
ORDER BY ORDINAL_POSITION
```

Returns a `ColumnInfo` with `name`, `data_type`, and `column_type` for each column.

### Unsupported Column Types

The following spatial types are excluded from the column list and logged with a warning:

| Data Type | Description |
|-----------|-------------|
| `geometry` | Generic spatial |
| `point` | Single point |
| `linestring` | Line |
| `polygon` | Polygon |
| `geometrycollection` | Collection of geometries |
| `multipolygon` | Multiple polygons |
| `multilinestring` | Multiple lines |
| `multipoint` | Multiple points |

Filtered columns are excluded from the SELECT column list in generated queries.

## Extraction Mode Detection

Auto-detection logic:

1. If `TABLE_MODE_<name>` override is set and not `auto` -> use the override
2. If table has both `updated_at` (type `timestamp` or `datetime`) AND `id` -> **Incremental**
3. Otherwise -> **FullRefresh**

Override precedence: `TABLE_MODE_<name>` env var always wins over auto-detection. Setting `auto` is equivalent to no override.

## AVG_ROW_LENGTH

Queries `information_schema.tables`:

```sql
SELECT AVG_ROW_LENGTH
FROM information_schema.tables
WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
```

- Returns `Some(u64)` if AVG_ROW_LENGTH is a positive integer
- Returns `None` if AVG_ROW_LENGTH is 0 or NULL (new/empty tables)
- Falls back to `DEFAULT_BATCH_SIZE` config value when None

## Updated_at Index Check

For Incremental tables, checks `information_schema.statistics`:

```sql
SELECT COUNT(*)
FROM information_schema.statistics
WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = 'updated_at'
```

- If count is 0, logs a warning: "table {name} has no index on updated_at -- incremental queries may be slow"
- Non-blocking: extraction proceeds regardless

## Schema Hash

Computes a SHA-256 hash of all column names, data types, and column types:

```
SHA256(name1 + data_type1 + column_type1 + name2 + data_type2 + column_type2 + ...)
```

Used for schema change detection between runs. If the hash differs from the one stored in `state.json`, Parket logs a warning about the schema change.

Properties:
- Deterministic: same columns always produce the same hash
- Order-sensitive: column reordering changes the hash
- Type-sensitive: data type changes are detected
