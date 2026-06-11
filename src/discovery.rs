use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tracing::{warn, info};
use sqlx::MySqlPool;

use crate::config::ExtractionMode;

const UNSUPPORTED_DATA_TYPES: &[&str] = &[
    "geometry",
    "point",
    "linestring",
    "polygon",
    "geometrycollection",
    "multipolygon",
    "multilinestring",
    "multipoint",
];

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: String,
    pub column_type: String,
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
            "SELECT COLUMN_NAME AS column_name, DATA_TYPE AS data_type, COLUMN_TYPE AS column_type FROM information_schema.columns WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION"
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
            Some(r) => match r.avg_row_length {
                0 => Ok(None),
                v => Ok(Some(v)),
            },
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

pub fn filter_unsupported_columns(columns: &[ColumnInfo]) -> Vec<ColumnInfo> {
    columns
        .iter()
        .filter(|c| {
            let dt = c.data_type.to_lowercase();
            if UNSUPPORTED_DATA_TYPES.contains(&dt.as_str()) {
                warn!("skipping unsupported column type: {} ({})", c.name, c.column_type);
                false
            } else {
                true
            }
        })
        .cloned()
        .collect()
}

pub fn detect_mode(
    columns: &[ColumnInfo],
    override_mode: Option<&ExtractionMode>,
    timestamp_col: &str,
) -> ExtractionMode {
    if let Some(mode) = override_mode
        && *mode != ExtractionMode::Auto
    {
        info!("using mode override: {:?}", mode);
        return mode.clone();
    }

    let has_timestamp = columns.iter().any(|c| {
        c.name == timestamp_col
            && (c.data_type == "timestamp" || c.data_type == "datetime")
    });
    let has_id = columns.iter().any(|c| c.name == "id");

    if has_timestamp && has_id {
        ExtractionMode::Incremental
    } else {
        ExtractionMode::FullRefresh
    }
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
    Ok(())
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
}

#[derive(Debug, sqlx::FromRow)]
struct MySqlAvgRowRow {
    avg_row_length: u64,
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

    fn col(name: &str, data_type: &str, column_type: &str) -> ColumnInfo {
        ColumnInfo {
            name: name.to_string(),
            data_type: data_type.to_string(),
            column_type: column_type.to_string(),
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

    #[test]
    fn unsupported_types_list_complete() {
        let expected = [
            "geometry",
            "point",
            "linestring",
            "polygon",
            "geometrycollection",
            "multipolygon",
            "multilinestring",
            "multipoint",
        ];
        for t in &expected {
            assert!(
                UNSUPPORTED_DATA_TYPES.contains(t),
                "missing unsupported type: {t}"
            );
        }
        assert_eq!(UNSUPPORTED_DATA_TYPES.len(), expected.len());
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
}
