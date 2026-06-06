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

FullRefresh extraction reads the entire table in paginated chunks using `LIMIT … OFFSET …`.

### SQL Template (per chunk)

```sql
SELECT `col1`, `col2`, ... FROM `table` LIMIT <batch_size> OFFSET <chunk_index * batch_size>
```

`batch_size` is the same value computed from `TARGET_MEMORY_MB` and `AVG_ROW_LENGTH` that Incremental uses. The orchestrator loops: chunk 0 calls `overwrite_table` (atomic replacement of existing data), chunks 1+ call `append_batch`. The loop exits when a chunk returns fewer rows than `batch_size`.

### Why LIMIT+OFFSET, not a single unbounded query

Without pagination, the entire table is loaded into RAM as `Vec<RecordBatch>` before any data reaches Delta Lake. For tables with tens or hundreds of millions of rows, this causes swap I/O and multi-hour wall-clock times dominated by Parquet serialisation under memory pressure. With chunked pagination, each chunk is extracted, written, and freed before the next is fetched.

### LIMIT+OFFSET performance note

`LIMIT N OFFSET M` on InnoDB without `ORDER BY` performs a sequential scan that reads and discards M rows before returning N — O(N²) total DB read work across all chunks. In practice this is acceptable because the bottleneck is writing to Delta Lake (Parquet encoding), not reading from MariaDB.

If DB read time becomes the bottleneck (very large tables, slow disk), the upgrade path is keyset pagination:

```sql
SELECT ... FROM `table` WHERE `pk` > <last_pk> ORDER BY `pk` LIMIT <batch_size>
```

This requires auto-detecting the primary key column from `information_schema` — not yet implemented.

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

### FullRefresh (first chunk)

Table: `customers`, columns: `id, email`, batch_size: `10000`

```sql
SELECT `id`, `email` FROM `customers` LIMIT 10000 OFFSET 0
```

### FullRefresh (second chunk)

```sql
SELECT `id`, `email` FROM `customers` LIMIT 10000 OFFSET 10000
```
