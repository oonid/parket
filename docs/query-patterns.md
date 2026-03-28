# Query Patterns

Parket generates SQL queries based on the extraction mode (Incremental or FullRefresh). All queries use explicit column lists and backtick-quoted identifiers.

## Incremental Mode

Incremental extraction uses cursor-based pagination on `(updated_at, id)` to fetch data in batches.

### SQL Template (with HWM)

```sql
SELECT `col1`, `col2`, ... FROM `table`
WHERE (`updated_at` = '<HWM_UPDATED_AT>' AND `id` > <HWM_LAST_ID>)
   OR (`updated_at` > '<HWM_UPDATED_AT>')
ORDER BY `updated_at` ASC, `id` ASC
LIMIT <batch_size>
```

The WHERE clause catches two cases:
- Rows with the same `updated_at` as the HWM but a higher `id` (tie-breaking)
- Rows with a `updated_at` strictly greater than the HWM (new data)

### SQL Template (first run, no HWM)

```sql
SELECT `col1`, `col2`, ... FROM `table`
ORDER BY `updated_at` ASC, `id` ASC
LIMIT <batch_size>
```

On the first run, no Delta table exists so there is no HWM. All rows are extracted starting from the beginning.

### Cursor Pagination

After each batch is written to Delta Lake, the HWM is updated to the maximum `(updated_at, id)` in the batch. The next query uses this new HWM as the cursor position. This continues until a batch returns fewer rows than `batch_size` (indicating the table is fully extracted up to the current moment).

HWM values are interpolated inline (not parameterized) because connector-x does not support parameterized queries.

## FullRefresh Mode

FullRefresh extraction reads the entire table in one query.

### SQL Template

```sql
SELECT `col1`, `col2`, ... FROM `table`
```

No WHERE clause, no LIMIT, no ORDER BY. connector-x streaming handles memory-bounded reading of the full result set.

## Backtick Quoting

All table and column names are wrapped in backticks to prevent conflicts with SQL reserved words or special characters:

| Input | Quoted |
|-------|--------|
| `order` | `` `order` `` |
| `create date` | `` `create date` `` |
| `select` | `` `select` `` |

## No SELECT *

All queries use an explicit column list from schema discovery. `SELECT *` is never used. This ensures deterministic column ordering and prevents issues when source tables are altered.

## Examples

### Incremental with HWM

Table: `orders`, columns: `id, name, updated_at`, HWM: `updated_at=2026-03-28 09:00:00, id=500`, batch_size: `10000`

```sql
SELECT `id`, `name`, `updated_at` FROM `orders` WHERE (`updated_at` = '2026-03-28 09:00:00' AND `id` > 500) OR (`updated_at` > '2026-03-28 09:00:00') ORDER BY `updated_at` ASC, `id` ASC LIMIT 10000
```

### Incremental first run

Table: `orders`, columns: `id`, batch_size: `5000`

```sql
SELECT `id` FROM `orders` ORDER BY `updated_at` ASC, `id` ASC LIMIT 5000
```

### FullRefresh

Table: `customers`, columns: `id, email`

```sql
SELECT `id`, `email` FROM `customers`
```
