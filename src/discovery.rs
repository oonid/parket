use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tracing::{warn, info};
use sqlx::MySqlPool;

use crate::config::Config;
use crate::config::ExtractionMode;

/// Common timestamp cursor column names, in priority order. Used to auto-detect an
/// incremental HWM cursor when no `TABLE_TIMESTAMP_<table>` override is configured.
const TIMESTAMP_CANDIDATES: &[&str] = &[
    "updated_at",
    "modified_at",
    "changed_at",
    "created_at",
    "created_date",
    "modified_date",
];

/// N1/O8 gatekeeper — the single source of truth for "can parket safely extract this
/// column". The vendored connector_arrow (`vendor/connector_arrow`, `create_field`) still
/// has a `todo!()` for any wire type it doesn't recognize; hitting it **aborts the whole
/// process** (exit code 101, not parket's 0/1/2 contract), not just the one table. Its own
/// mapping is bigger than parket's — `mariadb_type_to_arrow`
/// (`src/orchestrator/schema.rs`) only maps a subset of what MariaDB can produce, and
/// `time`/`year`/`bit`/`uuid`/`inet4`/`inet6`/geometry/future types are outside it.
///
/// `mariadb_type_to_arrow` itself returns a graceful `Result::Err` (bail!, not a panic) for
/// anything outside this set — so a column reaching it unfiltered would fail *that one
/// table*, not abort the process. But nothing guarantees every parket-accepted type is
/// also connector-mappable, and "fail the whole table" is still worse than the
/// geometry-family precedent of skipping the one unsupported column with a warn. So
/// `filter_unsupported_columns` below is the actual enforcement point: a column survives
/// pipeline-wide ONLY if its DATA_TYPE is in this allowlist — everything else (including
/// any future/unknown type) is dropped with a warn before it can reach schema building,
/// `mariadb_type_to_arrow`, or the connector at all. See audit-findings.md N1 (§2) / O8.
///
/// This list mirrors `mariadb_type_to_arrow`'s match arms EXACTLY — the two are kept in
/// sync by `orchestrator::schema::mariadb_type_to_arrow_covers_exactly_the_extractable_allowlist`,
/// which asserts every entry here is accepted there and spot-checks known-excluded types
/// are rejected there. If you add a mapping to `mariadb_type_to_arrow`, add the matching
/// DATA_TYPE string(s) here too (or that type stays permanently skipped despite being
/// mappable).
pub(crate) const EXTRACTABLE_DATA_TYPES: &[&str] = &[
    "tinyint",
    "smallint",
    "mediumint",
    "int",
    "bigint",
    "float",
    "double",
    "decimal",
    "numeric",
    "varchar",
    "char",
    "text",
    "tinytext",
    "mediumtext",
    "longtext",
    "json",
    "enum",
    "set",
    "date",
    "datetime",
    "timestamp",
    "boolean",
    "bool",
    "blob",
    "tinyblob",
    "mediumblob",
    "longblob",
    "binary",
    "varbinary",
];

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: String,
    pub column_type: String,
    /// Whether the source column is `NULL`-able. Not fed into `compute_schema_hash`
    /// (structural mapping only). Used by `detect_mode`/`detect_timestamp_col` to
    /// refuse auto-selecting a nullable cursor (O3): the incremental query filters
    /// `WHERE <cursor> IS NOT NULL`, so a nullable cursor silently skips NULL rows.
    pub nullable: bool,
}

#[derive(Debug, Clone)]
pub struct ColumnDescribe {
    pub name: String,
    pub data_type: String,
    pub column_type: String,
    pub nullable: bool,
    pub key: String,
}

#[derive(Debug, Clone)]
pub struct IndexInfo {
    pub name: String,
    pub unique: bool,
    pub columns: Vec<String>,
}

pub struct SchemaInspector {
    pool: MySqlPool,
    database: String,
}

impl SchemaInspector {
    pub fn new(pool: MySqlPool, database: String) -> Self {
        Self { pool, database }
    }

    pub async fn discover_columns(&self, table: &str) -> Result<Vec<ColumnInfo>> {
        let rows: Vec<MySqlColumnRow> = sqlx::query_as(
            "SELECT COLUMN_NAME AS column_name, DATA_TYPE AS data_type, COLUMN_TYPE AS column_type, IS_NULLABLE AS is_nullable FROM information_schema.columns WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION"
        )
        .bind(&self.database)
        .bind(table)
        .fetch_all(&self.pool)
        .await
        .with_context(|| format!("failed to query columns for table {table}"))?;

        if rows.is_empty() {
            bail!("table {table} does not exist in database {}", self.database);
        }

        let columns: Vec<ColumnInfo> = rows
            .into_iter()
            .map(|r| ColumnInfo {
                name: r.column_name,
                data_type: r.data_type,
                column_type: r.column_type,
                nullable: r.is_nullable == "YES",
            })
            .collect();

        info!("discovered {} columns for table {table}", columns.len());
        Ok(columns)
    }

    pub async fn get_avg_row_length(&self, table: &str) -> Result<Option<u64>> {
        let row: Option<MySqlAvgRowRow> = sqlx::query_as(
            "SELECT AVG_ROW_LENGTH AS avg_row_length FROM information_schema.tables WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?"
        )
        .bind(&self.database)
        .bind(table)
        .fetch_optional(&self.pool)
        .await
        .with_context(|| format!("failed to query AVG_ROW_LENGTH for table {table}"))?;

        match row {
            Some(r) => Ok(normalize_avg_row_length(r.avg_row_length)),
            None => Ok(None),
        }
    }

    /// MAX of a timestamp/datetime column as a string (CAST to CHAR so it comes back
    /// as text regardless of sqlx type mapping). None if the table is empty / all NULL.
    pub async fn max_timestamp(&self, table: &str, col: &str) -> Result<Option<String>> {
        let sql = format!("SELECT CAST(MAX(`{col}`) AS CHAR) AS m FROM `{table}`");
        let row: Option<(Option<String>,)> = sqlx::query_as(&sql)
            .fetch_optional(&self.pool)
            .await
            .with_context(|| format!("failed to query MAX({col}) for table {table}"))?;
        Ok(row.and_then(|(m,)| m))
    }

    /// COUNT of rows whose `col` IS NULL. D2 observability probe: an explicitly-configured
    /// nullable incremental / two-stream *update* cursor silently excludes NULL-cursor rows
    /// (both incremental queries filter `WHERE <col> IS NOT NULL`), so the orchestrator counts
    /// and warns about them once per run instead of dropping them invisibly. `col` is
    /// backtick-quoted; this is only ever called with a discovered/validated cursor column.
    pub async fn count_null(&self, table: &str, col: &str) -> Result<i64> {
        let sql = format!("SELECT COUNT(*) FROM `{table}` WHERE `{col}` IS NULL");
        let row: (i64,) = sqlx::query_as(&sql)
            .fetch_one(&self.pool)
            .await
            .with_context(|| format!("failed to count NULL `{col}` rows for table {table}"))?;
        Ok(row.0)
    }

    pub async fn check_updated_at_index(&self, table: &str) -> Result<bool> {
        let row: Option<(i64,)> = sqlx::query_as(
            "SELECT COUNT(*) FROM information_schema.statistics WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = 'updated_at'"
        )
        .bind(&self.database)
        .bind(table)
        .fetch_optional(&self.pool)
        .await
        .with_context(|| format!("failed to query index info for table {table}"))?;

        let count = row.map(|(c,)| c).unwrap_or(0);
        if count == 0 {
            warn!("table {table} has no index on updated_at — incremental queries may be slow");
        }
        Ok(count > 0)
    }

    pub async fn describe_columns(&self, table: &str) -> Result<Vec<ColumnDescribe>> {
        let rows: Vec<MySqlColumnDescribeRow> = sqlx::query_as(
            "SELECT COLUMN_NAME AS name, DATA_TYPE AS data_type, COLUMN_TYPE AS column_type, IS_NULLABLE AS is_nullable, COLUMN_KEY AS column_key FROM information_schema.columns WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION"
        )
        .bind(&self.database)
        .bind(table)
        .fetch_all(&self.pool)
        .await
        .with_context(|| format!("failed to query columns for table {table}"))?;

        if rows.is_empty() {
            bail!("table {table} does not exist in database {}", self.database);
        }

        let columns: Vec<ColumnDescribe> = rows
            .into_iter()
            .map(|r| ColumnDescribe {
                name: r.name,
                data_type: r.data_type,
                column_type: r.column_type,
                nullable: r.is_nullable == "YES",
                key: r.column_key,
            })
            .collect();

        Ok(columns)
    }

    pub async fn discover_indexes(&self, table: &str) -> Result<Vec<IndexInfo>> {
        // CAST the unsigned information_schema integers to SIGNED so sqlx decodes
        // them into i64 (MySQL/MariaDB return NON_UNIQUE as an unsigned int, which
        // does not decode into i64 directly). COLUMN_NAME can be NULL (functional
        // key parts), so it is Option<String>. SEQ_IN_INDEX is only needed for the
        // ORDER BY, not selected.
        let rows: Vec<MySqlIndexRow> = sqlx::query_as(
            "SELECT INDEX_NAME AS index_name, CAST(NON_UNIQUE AS SIGNED) AS non_unique, COLUMN_NAME AS column_name FROM information_schema.statistics WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY INDEX_NAME, SEQ_IN_INDEX"
        )
        .bind(&self.database)
        .bind(table)
        .fetch_all(&self.pool)
        .await
        .with_context(|| format!("failed to query indexes for table {table}"))?;

        // Group rows by index name (rows arrive ordered by SEQ_IN_INDEX, so each
        // index's columns accumulate in key order — columns[0] is the leading column).
        let mut index_map: std::collections::HashMap<String, (i64, Vec<String>)> = std::collections::HashMap::new();
        for row in rows {
            let entry = index_map.entry(row.index_name.clone()).or_insert((row.non_unique, Vec::new()));
            if let Some(col) = row.column_name {
                entry.1.push(col);
            }
        }

        let indexes: Vec<IndexInfo> = index_map
            .into_iter()
            .map(|(name, (non_unique, columns))| IndexInfo {
                name,
                unique: non_unique == 0,
                columns,
            })
            .collect();

        Ok(indexes)
    }
}

/// N1/O8: allowlist-driven column filter — a column is kept ONLY if its DATA_TYPE is in
/// `EXTRACTABLE_DATA_TYPES` (mirrors `mariadb_type_to_arrow` exactly). Everything else —
/// `time`, `year`, `bit`-variants, `uuid`, `inet4`/`inet6`, the geometry family, and any
/// future/unknown type — is uniformly skipped with a warn naming the column, its declared
/// type, and that it is excluded from extraction. This replaces the old blocklist (which
/// only named the geometry family) with an explicit allowlist so a *new* MariaDB type
/// nobody has taught parket about is safe by default (skipped, not a process abort or a
/// whole-table failure). Used by both the orchestrator (`process_table`) and preflight
/// (`--check`) so the two paths never diverge on what gets extracted.
pub fn filter_unsupported_columns(columns: &[ColumnInfo]) -> Vec<ColumnInfo> {
    columns
        .iter()
        .filter(|c| {
            let dt = c.data_type.to_lowercase();
            if EXTRACTABLE_DATA_TYPES.contains(&dt.as_str()) {
                true
            } else {
                warn!(
                    "excluding column '{}' (type '{}', declared '{}') from extraction: not in \
                     the extractable-type allowlist — see audit finding N1/O8",
                    c.name, c.data_type, c.column_type
                );
                false
            }
        })
        .cloned()
        .collect()
}

/// Auto-detect a timestamp cursor column: returns the first `TIMESTAMP_CANDIDATES`
/// entry present as a `timestamp`/`datetime` column, or `None` if none match.
/// (Used only when there is no explicit `TABLE_TIMESTAMP_<table>` override.)
/// A candidate that is `NULL`-able is skipped (O3): auto-detection must never pick
/// a cursor that would silently drop NULL-cursor rows under incremental extraction.
pub fn detect_timestamp_col(columns: &[ColumnInfo]) -> Option<String> {
    for candidate in TIMESTAMP_CANDIDATES {
        if columns.iter().any(|c| {
            c.name == *candidate
                && (c.data_type == "timestamp" || c.data_type == "datetime")
                && !c.nullable
        }) {
            return Some((*candidate).to_string());
        }
    }
    None
}

/// Resolve the extraction mode for a table.
///
/// O3: a nullable cursor column is unsafe for incremental extraction — the
/// incremental query filters `WHERE <cursor> IS NOT NULL`, so rows with a NULL
/// cursor value would be silently skipped forever. Auto-detection therefore never
/// selects a nullable cursor (falls back to FullRefresh, `warn!`ing why). An
/// *explicit* `TABLE_MODE=incremental` override on a nullable cursor is still
/// honored (operator intent), but loudly `warn!`s about the NULL-row exclusion.
pub fn detect_mode(
    columns: &[ColumnInfo],
    override_mode: Option<&ExtractionMode>,
    timestamp_col: &str,
) -> ExtractionMode {
    if let Some(mode) = override_mode
        && *mode != ExtractionMode::Auto
    {
        info!("using mode override: {:?}", mode);
        if *mode == ExtractionMode::Incremental
            && let Some(c) = columns.iter().find(|c| c.name == timestamp_col)
            && c.nullable
        {
            warn!(
                "TABLE_MODE=incremental explicitly configured with nullable cursor column \
                 '{timestamp_col}' — rows where '{timestamp_col}' IS NULL will be silently \
                 excluded from incremental extraction (honoring explicit override; see audit \
                 finding O3/D2)"
            );
        }
        return mode.clone();
    }

    let ts_col = columns.iter().find(|c| {
        c.name == timestamp_col
            && (c.data_type == "timestamp" || c.data_type == "datetime")
    });
    let has_id = columns.iter().any(|c| c.name == "id");

    match ts_col {
        // Warn only when nullability is the DECIDING factor (id present, so the table
        // would otherwise have qualified for incremental). A table without `id` is
        // full_refresh regardless — attributing that to the cursor would mislead.
        Some(c) if has_id && c.nullable => {
            warn!(
                "auto-detection found timestamp cursor candidate '{timestamp_col}' but it is \
                 nullable — a nullable cursor is unsafe for incremental extraction (NULL-cursor \
                 rows would be silently skipped); falling back to full_refresh (see audit \
                 finding O3; run --inspect for details)"
            );
            ExtractionMode::FullRefresh
        }
        Some(_) if has_id => ExtractionMode::Incremental,
        _ => ExtractionMode::FullRefresh,
    }
}

/// Shared mode + timestamp-cursor resolver (O7/O12): the SINGLE source of truth for turning a
/// table's discovered columns + config into its `(ts_col, ExtractionMode)`. Both the extraction
/// run (orchestrator) and `--verify` call this so they can never disagree about a table's mode
/// (O12: a third divergent copy in the verify path previously verified auto-detected-incremental
/// tables as Basic). Behavior-preserving extraction of the orchestrator's prior inline logic.
pub fn resolve_ts_col_and_mode(
    columns: &[ColumnInfo],
    config: &Config,
    table: &str,
) -> Result<(String, ExtractionMode)> {
    let ts_col = match config.table_timestamp_col.get(table) {
        Some(ovr) => {
            validate_timestamp_col(columns, ovr)?;
            ovr.clone()
        }
        None => detect_timestamp_col(columns).unwrap_or_else(|| "updated_at".to_string()),
    };

    let has_insert = config.table_insert_cursor.contains_key(table);
    let has_update = config.table_update_cursor.contains_key(table);
    if has_insert ^ has_update {
        bail!("two-stream requires BOTH TABLE_INSERT_CURSOR_{table} and TABLE_UPDATE_CURSOR_{table}");
    }
    let mode = if let Some((ins, upd)) = config.two_stream(table) {
        validate_two_stream_cursors(columns, &ins, &upd)?;
        ExtractionMode::TwoStream
    } else {
        detect_mode(columns, config.table_modes.get(table), &ts_col)
    };
    Ok((ts_col, mode))
}

pub fn validate_timestamp_col(columns: &[ColumnInfo], timestamp_col: &str) -> anyhow::Result<()> {
    let ok = columns.iter().any(|c| c.name == timestamp_col
        && (c.data_type == "timestamp" || c.data_type == "datetime"));
    if !ok {
        anyhow::bail!("configured timestamp column '{timestamp_col}' is missing or not a timestamp/datetime column");
    }
    Ok(())
}

pub fn validate_two_stream_cursors(
    columns: &[ColumnInfo],
    insert_col: &str,
    update_col: &str,
) -> anyhow::Result<()> {
    let is_int = |c: &ColumnInfo| matches!(c.data_type.as_str(),
        "tinyint" | "smallint" | "mediumint" | "int" | "bigint");
    let insert_ok = columns.iter().any(|c| c.name == insert_col && is_int(c));
    if !insert_ok {
        anyhow::bail!("two-stream insert cursor '{insert_col}' is missing or not an integer column");
    }
    // reuse the timestamp/datetime check
    validate_timestamp_col(columns, update_col)
        .map_err(|_| anyhow::anyhow!("two-stream update cursor '{update_col}' is missing or not a timestamp/datetime column"))?;

    // O3: an explicitly configured two-stream update cursor is honored even if
    // nullable (operator intent), but rows with a NULL cursor are silently excluded
    // from the update stream — warn loudly (see D2).
    if let Some(c) = columns.iter().find(|c| c.name == update_col)
        && c.nullable
    {
        warn!(
            "two-stream update cursor '{update_col}' is nullable — rows where '{update_col}' IS \
             NULL will be silently excluded from the update stream (honoring explicit two-stream \
             cursor configuration; see audit finding O3/D2)"
        );
    }
    Ok(())
}

fn normalize_avg_row_length(avg_row_length: Option<u64>) -> Option<u64> {
    avg_row_length.filter(|v| *v > 0)
}

pub fn compute_schema_hash(columns: &[ColumnInfo]) -> String {
    let mut hasher = Sha256::new();
    for col in columns {
        hasher.update(col.name.as_bytes());
        hasher.update(col.data_type.as_bytes());
        hasher.update(col.column_type.as_bytes());
    }
    let result = hasher.finalize();
    format!("{result:x}")
}

#[derive(Debug, sqlx::FromRow)]
struct MySqlColumnRow {
    column_name: String,
    data_type: String,
    column_type: String,
    is_nullable: String,
}

#[derive(Debug, sqlx::FromRow)]
struct MySqlAvgRowRow {
    avg_row_length: Option<u64>,
}

#[derive(Debug, sqlx::FromRow)]
struct MySqlColumnDescribeRow {
    name: String,
    data_type: String,
    column_type: String,
    is_nullable: String,
    column_key: String,
}

#[derive(Debug, sqlx::FromRow)]
struct MySqlIndexRow {
    index_name: String,
    non_unique: i64,
    column_name: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    /// Minimal `Config` builder for `resolve_ts_col_and_mode` unit tests. `Config` has no
    /// test-only constructor and none is added here (config.rs is not touched) — all fields
    /// are `pub`, so this is just a struct literal with the fields the resolver reads left
    /// as their empty/default values, mirroring `tests/integration.rs`'s `make_config`.
    fn test_config() -> Config {
        Config {
            database_url: String::new(),
            s3_bucket: String::new(),
            s3_access_key_id: String::new(),
            s3_secret_access_key: String::new(),
            tables: Vec::new(),
            target_memory_mb: 64,
            merge_memory_mb: 64,
            merge_spill_dir: None,
            s3_endpoint: None,
            s3_region: String::new(),
            s3_prefix: String::new(),
            default_batch_size: 10_000,
            rust_log: String::new(),
            table_modes: HashMap::new(),
            table_initial_hwm: HashMap::new(),
            table_timestamp_col: HashMap::new(),
            table_insert_cursor: HashMap::new(),
            table_update_cursor: HashMap::new(),
        }
    }

    fn col(name: &str, data_type: &str, column_type: &str) -> ColumnInfo {
        ColumnInfo {
            name: name.to_string(),
            data_type: data_type.to_string(),
            column_type: column_type.to_string(),
            nullable: false,
        }
    }

    /// Same as `col`, but `nullable: true` — for O3 nullable-cursor tests.
    fn nullable_col(name: &str, data_type: &str, column_type: &str) -> ColumnInfo {
        ColumnInfo {
            name: name.to_string(),
            data_type: data_type.to_string(),
            column_type: column_type.to_string(),
            nullable: true,
        }
    }

    #[test]
    fn filter_removes_geometry() {
        let columns = vec![
            col("id", "int", "int(11)"),
            col("name", "varchar", "varchar(255)"),
            col("location", "geometry", "geometry"),
        ];
        let filtered = filter_unsupported_columns(&columns);
        assert_eq!(filtered.len(), 2);
        assert!(filtered.iter().all(|c| c.name != "location"));
    }

    #[test]
    fn filter_removes_point() {
        let columns = vec![
            col("id", "int", "int(11)"),
            col("coords", "point", "point"),
        ];
        let filtered = filter_unsupported_columns(&columns);
        assert_eq!(filtered.len(), 1);
    }

    #[test]
    fn filter_removes_linestring() {
        let columns = vec![col("path", "linestring", "linestring")];
        let filtered = filter_unsupported_columns(&columns);
        assert!(filtered.is_empty());
    }

    #[test]
    fn filter_removes_polygon() {
        let columns = vec![col("area", "polygon", "polygon")];
        let filtered = filter_unsupported_columns(&columns);
        assert!(filtered.is_empty());
    }

    #[test]
    fn filter_removes_geometrycollection() {
        let columns = vec![col("shapes", "geometrycollection", "geometrycollection")];
        let filtered = filter_unsupported_columns(&columns);
        assert!(filtered.is_empty());
    }

    #[test]
    fn filter_removes_multipolygon() {
        let columns = vec![col("regions", "multipolygon", "multipolygon")];
        let filtered = filter_unsupported_columns(&columns);
        assert!(filtered.is_empty());
    }

    #[test]
    fn filter_removes_multilinestring() {
        let columns = vec![col("paths", "multilinestring", "multilinestring")];
        let filtered = filter_unsupported_columns(&columns);
        assert!(filtered.is_empty());
    }

    #[test]
    fn filter_removes_multipoint() {
        let columns = vec![col("dots", "multipoint", "multipoint")];
        let filtered = filter_unsupported_columns(&columns);
        assert!(filtered.is_empty());
    }

    #[test]
    fn filter_keeps_all_supported_types() {
        let columns = vec![
            col("id", "int", "int(11)"),
            col("name", "varchar", "varchar(255)"),
            col("price", "decimal", "decimal(10,2)"),
            col("created_at", "timestamp", "timestamp"),
            col("data", "json", "json"),
            col("content", "text", "text"),
            col("is_active", "tinyint", "tinyint(1)"),
            col("weight", "float", "float"),
            col("bio", "blob", "blob"),
            col("birth_date", "date", "date"),
            col("modified", "datetime", "datetime"),
        ];
        let filtered = filter_unsupported_columns(&columns);
        assert_eq!(filtered.len(), columns.len());
    }

    #[test]
    fn filter_case_insensitive() {
        let columns = vec![
            col("id", "int", "int(11)"),
            col("loc", "GEOMETRY", "GEOMETRY"),
            col("pt", "Point", "point"),
        ];
        let filtered = filter_unsupported_columns(&columns);
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].name, "id");
    }

    #[test]
    fn normalize_avg_row_length_handles_null() {
        assert_eq!(normalize_avg_row_length(None), None);
    }

    #[test]
    fn normalize_avg_row_length_handles_zero() {
        assert_eq!(normalize_avg_row_length(Some(0)), None);
    }

    #[test]
    fn normalize_avg_row_length_keeps_positive_values() {
        assert_eq!(normalize_avg_row_length(Some(512)), Some(512));
    }

    #[test]
    fn filter_empty_columns() {
        let columns: Vec<ColumnInfo> = vec![];
        let filtered = filter_unsupported_columns(&columns);
        assert!(filtered.is_empty());
    }

    #[test]
    fn detect_mode_incremental_with_timestamp() {
        let columns = vec![
            col("id", "int", "int(11)"),
            col("name", "varchar", "varchar(255)"),
            col("updated_at", "timestamp", "timestamp"),
        ];
        let mode = detect_mode(&columns, None, "updated_at");
        assert_eq!(mode, ExtractionMode::Incremental);
    }

    #[test]
    fn detect_mode_incremental_with_datetime() {
        let columns = vec![
            col("id", "int", "int(11)"),
            col("updated_at", "datetime", "datetime"),
        ];
        let mode = detect_mode(&columns, None, "updated_at");
        assert_eq!(mode, ExtractionMode::Incremental);
    }

    #[test]
    fn detect_mode_full_refresh_missing_updated_at() {
        let columns = vec![
            col("id", "int", "int(11)"),
            col("name", "varchar", "varchar(255)"),
        ];
        let mode = detect_mode(&columns, None, "updated_at");
        assert_eq!(mode, ExtractionMode::FullRefresh);
    }

    #[test]
    fn detect_mode_full_refresh_missing_id() {
        let columns = vec![
            col("name", "varchar", "varchar(255)"),
            col("updated_at", "timestamp", "timestamp"),
        ];
        let mode = detect_mode(&columns, None, "updated_at");
        assert_eq!(mode, ExtractionMode::FullRefresh);
    }

    #[test]
    fn detect_mode_full_refresh_no_relevant_columns() {
        let columns = vec![col("data", "json", "json")];
        let mode = detect_mode(&columns, None, "updated_at");
        assert_eq!(mode, ExtractionMode::FullRefresh);
    }

    #[test]
    fn detect_mode_updated_at_wrong_type() {
        let columns = vec![
            col("id", "int", "int(11)"),
            col("updated_at", "varchar", "varchar(255)"),
        ];
        let mode = detect_mode(&columns, None, "updated_at");
        assert_eq!(mode, ExtractionMode::FullRefresh);
    }

    #[test]
    fn detect_mode_override_takes_precedence() {
        let columns = vec![
            col("id", "int", "int(11)"),
            col("updated_at", "timestamp", "timestamp"),
        ];
        let mode = detect_mode(&columns, Some(&ExtractionMode::FullRefresh), "updated_at");
        assert_eq!(mode, ExtractionMode::FullRefresh);
    }

    #[test]
    fn detect_mode_override_incremental_forces_incremental() {
        let columns = vec![col("name", "varchar", "varchar(255)")];
        let mode = detect_mode(&columns, Some(&ExtractionMode::Incremental), "updated_at");
        assert_eq!(mode, ExtractionMode::Incremental);
    }

    #[test]
    fn detect_mode_override_auto_same_as_none() {
        let columns = vec![col("data", "json", "json")];
        let mode = detect_mode(&columns, Some(&ExtractionMode::Auto), "updated_at");
        assert_eq!(mode, ExtractionMode::FullRefresh);
    }

    #[test]
    fn detect_mode_nullable_cursor_auto_falls_back_to_full_refresh() {
        // O3: id + a nullable updated_at, no override — auto-detection must NOT
        // select the nullable cursor for incremental (silent NULL-row loss trap).
        let columns = vec![
            col("id", "int", "int(11)"),
            nullable_col("updated_at", "timestamp", "timestamp"),
        ];
        let mode = detect_mode(&columns, None, "updated_at");
        assert_eq!(mode, ExtractionMode::FullRefresh);
    }

    #[test]
    fn detect_mode_override_incremental_with_nullable_cursor_still_incremental() {
        // O3 decision (b): an explicit TABLE_MODE=incremental override on a nullable
        // cursor is honored (operator intent), just loudly warned about.
        let columns = vec![
            col("id", "int", "int(11)"),
            nullable_col("updated_at", "timestamp", "timestamp"),
        ];
        let mode = detect_mode(&columns, Some(&ExtractionMode::Incremental), "updated_at");
        assert_eq!(mode, ExtractionMode::Incremental);
    }

    #[test]
    fn compute_schema_hash_deterministic() {
        let columns = vec![
            col("id", "int", "int(11)"),
            col("name", "varchar", "varchar(255)"),
        ];
        let hash1 = compute_schema_hash(&columns);
        let hash2 = compute_schema_hash(&columns);
        assert_eq!(hash1, hash2);
    }

    #[test]
    fn compute_schema_hash_changes_with_columns() {
        let cols_a = vec![
            col("id", "int", "int(11)"),
            col("name", "varchar", "varchar(255)"),
        ];
        let cols_b = vec![
            col("id", "int", "int(11)"),
            col("email", "varchar", "varchar(255)"),
        ];
        assert_ne!(compute_schema_hash(&cols_a), compute_schema_hash(&cols_b));
    }

    #[test]
    fn compute_schema_hash_changes_with_types() {
        let cols_a = vec![col("id", "int", "int(11)")];
        let cols_b = vec![col("id", "bigint", "bigint(20)")];
        assert_ne!(compute_schema_hash(&cols_a), compute_schema_hash(&cols_b));
    }

    #[test]
    fn compute_schema_hash_empty_columns() {
        let columns: Vec<ColumnInfo> = vec![];
        let hash = compute_schema_hash(&columns);
        assert!(!hash.is_empty());
    }

    #[test]
    fn compute_schema_hash_order_matters() {
        let cols_a = vec![
            col("id", "int", "int(11)"),
            col("name", "varchar", "varchar(255)"),
        ];
        let cols_b = vec![
            col("name", "varchar", "varchar(255)"),
            col("id", "int", "int(11)"),
        ];
        assert_ne!(compute_schema_hash(&cols_a), compute_schema_hash(&cols_b));
    }

    // N1/O8: `UNSUPPORTED_DATA_TYPES` (a geometry-only blocklist) was replaced by the
    // `EXTRACTABLE_DATA_TYPES` allowlist above — a column is now excluded whenever its
    // type is absent from the allowlist, not just when it matches a hardcoded blocklist
    // entry. `unsupported_types_list_complete` asserted the old blocklist's exact
    // contents; there is no equivalent blocklist left to assert against, so it is
    // replaced by the allowlist-coverage tests below (`filter_keeps_every_allowlisted_type`,
    // `filter_removes_time_year_bit_uuid_keeps_rest`, `filter_removes_inet_types`,
    // `filter_removes_unknown_future_type`).

    #[test]
    fn filter_keeps_every_allowlisted_type() {
        // Every DATA_TYPE mariadb_type_to_arrow maps must survive the filter.
        let columns: Vec<ColumnInfo> = EXTRACTABLE_DATA_TYPES
            .iter()
            .enumerate()
            .map(|(i, dt)| col(&format!("c{i}"), dt, dt))
            .collect();
        let filtered = filter_unsupported_columns(&columns);
        assert_eq!(
            filtered.len(),
            columns.len(),
            "every allowlisted type must survive filter_unsupported_columns"
        );
    }

    #[test]
    fn filter_removes_time_year_bit_uuid_keeps_rest() {
        // O8: time/year/bit used to fail the whole table (via mariadb_type_to_arrow's
        // bail); uuid was never mapped at all. All four are now skipped uniformly,
        // like geometry, instead of failing or reaching the connector.
        let columns = vec![
            col("id", "bigint", "bigint(20)"),
            col("name", "varchar", "varchar(50)"),
            col("t", "time", "time"),
            col("y", "year", "year(4)"),
            col("b", "bit", "bit(8)"),
            col("u", "uuid", "uuid"),
        ];
        let filtered = filter_unsupported_columns(&columns);
        let names: Vec<&str> = filtered.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, vec!["id", "name"]);
    }

    #[test]
    fn filter_removes_inet_types() {
        let columns = vec![
            col("id", "int", "int(11)"),
            col("ip4", "inet4", "inet4"),
            col("ip6", "inet6", "inet6"),
        ];
        let filtered = filter_unsupported_columns(&columns);
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].name, "id");
    }

    #[test]
    fn filter_removes_unknown_future_type() {
        // A type nobody has taught parket about yet must be safe-by-default (skipped),
        // not reach the connector or panic.
        let columns = vec![
            col("id", "int", "int(11)"),
            col("embedding", "vector", "vector(768)"),
        ];
        let filtered = filter_unsupported_columns(&columns);
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].name, "id");
    }

    #[test]
    fn detect_mode_custom_timestamp_col() {
        let columns = vec![
            col("id", "int", "int(11)"),
            col("completed_at", "timestamp", "timestamp"),
        ];
        let mode = detect_mode(&columns, None, "completed_at");
        assert_eq!(mode, ExtractionMode::Incremental);
    }

    #[test]
    fn validate_two_stream_cursors_both_valid() {
        let columns = vec![
            col("id", "int", "int(11)"),
            col("user_id", "bigint", "bigint(20)"),
            col("updated_at", "timestamp", "timestamp"),
        ];
        let result = validate_two_stream_cursors(&columns, "user_id", "updated_at");
        assert!(result.is_ok());
    }

    #[test]
    fn validate_two_stream_cursors_insert_missing() {
        let columns = vec![
            col("id", "int", "int(11)"),
            col("updated_at", "timestamp", "timestamp"),
        ];
        let result = validate_two_stream_cursors(&columns, "missing_col", "updated_at");
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("two-stream insert cursor"));
        assert!(err.contains("missing_col"));
    }

    #[test]
    fn validate_two_stream_cursors_insert_not_integer() {
        let columns = vec![
            col("id", "int", "int(11)"),
            col("key", "varchar", "varchar(255)"),
            col("updated_at", "timestamp", "timestamp"),
        ];
        let result = validate_two_stream_cursors(&columns, "key", "updated_at");
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("two-stream insert cursor"));
        assert!(err.contains("not an integer column"));
    }

    #[test]
    fn validate_two_stream_cursors_nullable_update_col_still_ok() {
        // O3 decision (b): a nullable update cursor is honored (Ok), just warned about.
        let columns = vec![
            col("id", "int", "int(11)"),
            col("user_id", "bigint", "bigint(20)"),
            nullable_col("updated_at", "timestamp", "timestamp"),
        ];
        let result = validate_two_stream_cursors(&columns, "user_id", "updated_at");
        assert!(result.is_ok());
    }

    #[test]
    fn validate_two_stream_cursors_update_missing() {
        let columns = vec![
            col("id", "int", "int(11)"),
            col("user_id", "bigint", "bigint(20)"),
        ];
        let result = validate_two_stream_cursors(&columns, "user_id", "completed_at");
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("two-stream update cursor"));
        assert!(err.contains("completed_at"));
    }

    #[test]
    fn validate_two_stream_cursors_update_not_timestamp() {
        let columns = vec![
            col("id", "int", "int(11)"),
            col("user_id", "bigint", "bigint(20)"),
            col("completed_at", "varchar", "varchar(255)"),
        ];
        let result = validate_two_stream_cursors(&columns, "user_id", "completed_at");
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("two-stream update cursor"));
    }

    #[test]
    fn validate_timestamp_col_present_and_timestamp() {
        let columns = vec![
            col("id", "int", "int(11)"),
            col("completed_at", "timestamp", "timestamp"),
        ];
        let result = validate_timestamp_col(&columns, "completed_at");
        assert!(result.is_ok());
    }

    #[test]
    fn validate_timestamp_col_present_and_datetime() {
        let columns = vec![
            col("id", "int", "int(11)"),
            col("finished_at", "datetime", "datetime"),
        ];
        let result = validate_timestamp_col(&columns, "finished_at");
        assert!(result.is_ok());
    }

    #[test]
    fn validate_timestamp_col_missing() {
        let columns = vec![
            col("id", "int", "int(11)"),
            col("updated_at", "timestamp", "timestamp"),
        ];
        let result = validate_timestamp_col(&columns, "completed_at");
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("completed_at"));
        assert!(err.contains("missing or not a timestamp/datetime column"));
    }

    #[test]
    fn validate_timestamp_col_wrong_type() {
        let columns = vec![
            col("id", "int", "int(11)"),
            col("completed_at", "varchar", "varchar(255)"),
        ];
        let result = validate_timestamp_col(&columns, "completed_at");
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("completed_at"));
    }

    #[test]
    fn detect_timestamp_col_finds_updated_at() {
        let columns = vec![
            col("updated_at", "timestamp", "timestamp"),
            col("id", "int", "int(11)"),
        ];
        assert_eq!(detect_timestamp_col(&columns), Some("updated_at".to_string()));
    }

    #[test]
    fn detect_timestamp_col_finds_modified_at() {
        let columns = vec![
            col("id", "int", "int(11)"),
            col("modified_at", "datetime", "datetime"),
        ];
        assert_eq!(detect_timestamp_col(&columns), Some("modified_at".to_string()));
    }

    #[test]
    fn detect_timestamp_col_finds_changed_at() {
        let columns = vec![col("changed_at", "timestamp", "timestamp")];
        assert_eq!(detect_timestamp_col(&columns), Some("changed_at".to_string()));
    }

    #[test]
    fn detect_timestamp_col_finds_created_at() {
        let columns = vec![col("created_at", "datetime", "datetime")];
        assert_eq!(detect_timestamp_col(&columns), Some("created_at".to_string()));
    }

    #[test]
    fn detect_timestamp_col_finds_created_date() {
        let columns = vec![col("created_date", "timestamp", "timestamp")];
        assert_eq!(detect_timestamp_col(&columns), Some("created_date".to_string()));
    }

    #[test]
    fn detect_timestamp_col_finds_modified_date() {
        let columns = vec![col("modified_date", "datetime", "datetime")];
        assert_eq!(detect_timestamp_col(&columns), Some("modified_date".to_string()));
    }

    #[test]
    fn detect_timestamp_col_priority_updated_over_modified() {
        let columns = vec![
            col("updated_at", "timestamp", "timestamp"),
            col("modified_at", "timestamp", "timestamp"),
        ];
        assert_eq!(detect_timestamp_col(&columns), Some("updated_at".to_string()));
    }

    #[test]
    fn detect_timestamp_col_skips_nullable_candidate() {
        // O3: a nullable updated_at must not be auto-selected as the cursor.
        let columns = vec![
            col("id", "int", "int(11)"),
            nullable_col("updated_at", "timestamp", "timestamp"),
        ];
        assert_eq!(detect_timestamp_col(&columns), None);
    }

    #[test]
    fn detect_timestamp_col_falls_through_when_top_candidate_nullable() {
        // A nullable updated_at is skipped; the next-priority NOT NULL candidate wins.
        let columns = vec![
            nullable_col("updated_at", "timestamp", "timestamp"),
            col("modified_at", "datetime", "datetime"),
        ];
        assert_eq!(detect_timestamp_col(&columns), Some("modified_at".to_string()));
    }

    #[test]
    fn detect_timestamp_col_not_null_candidate_returned() {
        let columns = vec![col("updated_at", "timestamp", "timestamp")];
        assert_eq!(detect_timestamp_col(&columns), Some("updated_at".to_string()));
    }

    #[test]
    fn detect_timestamp_col_ignores_wrong_type() {
        let columns = vec![
            col("updated_at", "varchar", "varchar(20)"),
        ];
        assert_eq!(detect_timestamp_col(&columns), None);
    }

    #[test]
    fn detect_timestamp_col_none_when_no_candidate() {
        let columns = vec![
            col("id", "int", "int(11)"),
            col("name", "varchar", "varchar(255)"),
        ];
        assert_eq!(detect_timestamp_col(&columns), None);
    }

    #[test]
    fn detect_timestamp_col_priority_updated_over_all() {
        // updated_at should win even when all candidates are present.
        let columns = vec![
            col("modified_at", "timestamp", "timestamp"),
            col("changed_at", "timestamp", "timestamp"),
            col("updated_at", "timestamp", "timestamp"),
            col("created_at", "datetime", "datetime"),
            col("created_date", "timestamp", "timestamp"),
            col("modified_date", "datetime", "datetime"),
        ];
        assert_eq!(detect_timestamp_col(&columns), Some("updated_at".to_string()));
    }

    #[test]
    fn detect_timestamp_col_modified_when_no_updated() {
        // modified_at is second priority.
        let columns = vec![
            col("changed_at", "timestamp", "timestamp"),
            col("modified_at", "datetime", "datetime"),
            col("created_at", "timestamp", "timestamp"),
        ];
        assert_eq!(detect_timestamp_col(&columns), Some("modified_at".to_string()));
    }

    #[test]
    fn detect_timestamp_col_changed_when_no_updated_or_modified() {
        let columns = vec![
            col("changed_at", "timestamp", "timestamp"),
            col("created_at", "datetime", "datetime"),
            col("id", "int", "int(11)"),
        ];
        assert_eq!(detect_timestamp_col(&columns), Some("changed_at".to_string()));
    }

    #[test]
    fn detect_timestamp_col_created_at_when_no_higher_priority() {
        let columns = vec![
            col("created_at", "timestamp", "timestamp"),
            col("name", "varchar", "varchar(255)"),
        ];
        assert_eq!(detect_timestamp_col(&columns), Some("created_at".to_string()));
    }

    #[test]
    fn detect_timestamp_col_created_date_when_no_created_at() {
        let columns = vec![
            col("created_date", "datetime", "datetime"),
            col("id", "int", "int(11)"),
        ];
        assert_eq!(detect_timestamp_col(&columns), Some("created_date".to_string()));
    }

    #[test]
    fn detect_timestamp_col_modified_date_fallback() {
        let columns = vec![
            col("modified_date", "timestamp", "timestamp"),
            col("id", "int", "int(11)"),
        ];
        assert_eq!(detect_timestamp_col(&columns), Some("modified_date".to_string()));
    }

    #[test]
    fn validate_two_stream_cursors_with_tinyint() {
        // tinyint should be accepted as integer.
        let columns = vec![
            col("id", "int", "int(11)"),
            col("version", "tinyint", "tinyint(4)"),
            col("updated_at", "timestamp", "timestamp"),
        ];
        let result = validate_two_stream_cursors(&columns, "version", "updated_at");
        assert!(result.is_ok());
    }

    #[test]
    fn validate_two_stream_cursors_with_smallint() {
        let columns = vec![
            col("id", "int", "int(11)"),
            col("counter", "smallint", "smallint(6)"),
            col("updated_at", "timestamp", "timestamp"),
        ];
        let result = validate_two_stream_cursors(&columns, "counter", "updated_at");
        assert!(result.is_ok());
    }

    #[test]
    fn validate_two_stream_cursors_with_mediumint() {
        let columns = vec![
            col("id", "int", "int(11)"),
            col("seq", "mediumint", "mediumint(9)"),
            col("updated_at", "timestamp", "timestamp"),
        ];
        let result = validate_two_stream_cursors(&columns, "seq", "updated_at");
        assert!(result.is_ok());
    }

    #[test]
    fn validate_two_stream_cursors_with_datetime_update() {
        // datetime should be accepted for update cursor (not just timestamp).
        let columns = vec![
            col("id", "int", "int(11)"),
            col("user_id", "bigint", "bigint(20)"),
            col("modified_at", "datetime", "datetime"),
        ];
        let result = validate_two_stream_cursors(&columns, "user_id", "modified_at");
        assert!(result.is_ok());
    }

    #[test]
    fn validate_timestamp_col_datetime_variant() {
        let columns = vec![
            col("id", "int", "int(11)"),
            col("event_time", "datetime", "datetime"),
        ];
        let result = validate_timestamp_col(&columns, "event_time");
        assert!(result.is_ok());
    }

    #[test]
    fn validate_timestamp_col_timestamp_variant() {
        let columns = vec![
            col("id", "int", "int(11)"),
            col("event_time", "timestamp", "timestamp"),
        ];
        let result = validate_timestamp_col(&columns, "event_time");
        assert!(result.is_ok());
    }

    #[test]
    fn filter_mixed_supported_and_unsupported() {
        // Mix of supported and unsupported types.
        let columns = vec![
            col("id", "int", "int(11)"),
            col("data", "json", "json"),
            col("bounds", "geometry", "geometry"),
            col("updated", "timestamp", "timestamp"),
            col("path", "linestring", "linestring"),
            col("text", "text", "text"),
        ];
        let filtered = filter_unsupported_columns(&columns);
        assert_eq!(filtered.len(), 4);
        assert!(filtered.iter().any(|c| c.name == "id"));
        assert!(filtered.iter().any(|c| c.name == "data"));
        assert!(filtered.iter().any(|c| c.name == "updated"));
        assert!(filtered.iter().any(|c| c.name == "text"));
        assert!(!filtered.iter().any(|c| c.name == "bounds"));
        assert!(!filtered.iter().any(|c| c.name == "path"));
    }

    #[test]
    fn compute_schema_hash_column_type_matters() {
        // Different column_type (same name/data_type) should produce different hash.
        let cols_a = vec![col("id", "int", "int(11)")];
        let cols_b = vec![col("id", "int", "int(20)")];
        assert_ne!(compute_schema_hash(&cols_a), compute_schema_hash(&cols_b));
    }

    // O12: `resolve_ts_col_and_mode` is the shared resolver both the orchestrator run and
    // `--verify` now call, so they can never disagree about a table's mode. These tests cover
    // the resolver's own logic (auto-detected incremental, no-id full_refresh, explicit
    // override honored, two-stream, and the has-one-of-two-cursors error) directly, in
    // addition to the pre-existing `detect_mode`/`detect_timestamp_col` unit tests it composes.

    #[test]
    fn resolve_ts_col_and_mode_auto_detects_incremental() {
        // id + a non-null updated_at, no explicit TABLE_MODE — this is exactly the O12 case:
        // the run auto-detects Incremental, and verify must resolve identically instead of
        // reading explicit table_modes only (which would see None and fall back to Basic).
        let columns = vec![
            col("id", "int", "int(11)"),
            col("updated_at", "timestamp", "timestamp"),
        ];
        let config = test_config();
        let (ts_col, mode) = resolve_ts_col_and_mode(&columns, &config, "orders").unwrap();
        assert_eq!(ts_col, "updated_at");
        assert_eq!(mode, ExtractionMode::Incremental);
    }

    #[test]
    fn resolve_ts_col_and_mode_no_id_is_full_refresh() {
        let columns = vec![
            col("name", "varchar", "varchar(255)"),
            col("updated_at", "timestamp", "timestamp"),
        ];
        let config = test_config();
        let (_, mode) = resolve_ts_col_and_mode(&columns, &config, "orders").unwrap();
        assert_eq!(mode, ExtractionMode::FullRefresh);
    }

    #[test]
    fn resolve_ts_col_and_mode_explicit_full_refresh_override_honored() {
        // id + updated_at would auto-detect Incremental, but an explicit TABLE_MODE override
        // must win.
        let columns = vec![
            col("id", "int", "int(11)"),
            col("updated_at", "timestamp", "timestamp"),
        ];
        let mut config = test_config();
        config
            .table_modes
            .insert("orders".to_string(), ExtractionMode::FullRefresh);
        let (_, mode) = resolve_ts_col_and_mode(&columns, &config, "orders").unwrap();
        assert_eq!(mode, ExtractionMode::FullRefresh);
    }

    #[test]
    fn resolve_ts_col_and_mode_two_stream_when_both_cursors_configured() {
        let columns = vec![
            col("id", "int", "int(11)"),
            col("user_id", "bigint", "bigint(20)"),
            col("updated_at", "timestamp", "timestamp"),
        ];
        let mut config = test_config();
        config
            .table_insert_cursor
            .insert("orders".to_string(), "user_id".to_string());
        config
            .table_update_cursor
            .insert("orders".to_string(), "updated_at".to_string());
        let (_, mode) = resolve_ts_col_and_mode(&columns, &config, "orders").unwrap();
        assert_eq!(mode, ExtractionMode::TwoStream);
    }

    #[test]
    fn resolve_ts_col_and_mode_one_sided_two_stream_cursor_errors() {
        // Only TABLE_INSERT_CURSOR set, no TABLE_UPDATE_CURSOR — must error, not silently
        // treat it as non-two-stream.
        let columns = vec![
            col("id", "int", "int(11)"),
            col("user_id", "bigint", "bigint(20)"),
            col("updated_at", "timestamp", "timestamp"),
        ];
        let mut config = test_config();
        config
            .table_insert_cursor
            .insert("orders".to_string(), "user_id".to_string());
        let result = resolve_ts_col_and_mode(&columns, &config, "orders");
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("two-stream requires BOTH"));
    }
}
