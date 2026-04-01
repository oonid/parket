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
) -> ExtractionMode {
    if let Some(mode) = override_mode
        && *mode != ExtractionMode::Auto
    {
        info!("using mode override: {:?}", mode);
        return mode.clone();
    }

    let has_updated_at = columns.iter().any(|c| {
        c.name == "updated_at"
            && (c.data_type == "timestamp" || c.data_type == "datetime")
    });
    let has_id = columns.iter().any(|c| c.name == "id");

    if has_updated_at && has_id {
        ExtractionMode::Incremental
    } else {
        ExtractionMode::FullRefresh
    }
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
        let mode = detect_mode(&columns, None);
        assert_eq!(mode, ExtractionMode::Incremental);
    }

    #[test]
    fn detect_mode_incremental_with_datetime() {
        let columns = vec![
            col("id", "int", "int(11)"),
            col("updated_at", "datetime", "datetime"),
        ];
        let mode = detect_mode(&columns, None);
        assert_eq!(mode, ExtractionMode::Incremental);
    }

    #[test]
    fn detect_mode_full_refresh_missing_updated_at() {
        let columns = vec![
            col("id", "int", "int(11)"),
            col("name", "varchar", "varchar(255)"),
        ];
        let mode = detect_mode(&columns, None);
        assert_eq!(mode, ExtractionMode::FullRefresh);
    }

    #[test]
    fn detect_mode_full_refresh_missing_id() {
        let columns = vec![
            col("name", "varchar", "varchar(255)"),
            col("updated_at", "timestamp", "timestamp"),
        ];
        let mode = detect_mode(&columns, None);
        assert_eq!(mode, ExtractionMode::FullRefresh);
    }

    #[test]
    fn detect_mode_full_refresh_no_relevant_columns() {
        let columns = vec![col("data", "json", "json")];
        let mode = detect_mode(&columns, None);
        assert_eq!(mode, ExtractionMode::FullRefresh);
    }

    #[test]
    fn detect_mode_updated_at_wrong_type() {
        let columns = vec![
            col("id", "int", "int(11)"),
            col("updated_at", "varchar", "varchar(255)"),
        ];
        let mode = detect_mode(&columns, None);
        assert_eq!(mode, ExtractionMode::FullRefresh);
    }

    #[test]
    fn detect_mode_override_takes_precedence() {
        let columns = vec![
            col("id", "int", "int(11)"),
            col("updated_at", "timestamp", "timestamp"),
        ];
        let mode = detect_mode(&columns, Some(&ExtractionMode::FullRefresh));
        assert_eq!(mode, ExtractionMode::FullRefresh);
    }

    #[test]
    fn detect_mode_override_incremental_forces_incremental() {
        let columns = vec![col("name", "varchar", "varchar(255)")];
        let mode = detect_mode(&columns, Some(&ExtractionMode::Incremental));
        assert_eq!(mode, ExtractionMode::Incremental);
    }

    #[test]
    fn detect_mode_override_auto_same_as_none() {
        let columns = vec![col("data", "json", "json")];
        let mode = detect_mode(&columns, Some(&ExtractionMode::Auto));
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
}
