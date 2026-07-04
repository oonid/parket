use anyhow::{Context, Result};
use sqlx::Row;
use std::collections::HashMap;

use super::{AggKind, ColumnAgg, ColumnMeta, KeyStats, SourceProbe, SourceScope};

fn scope_predicate_sql(scope: &SourceScope) -> String {
    format!(
        "(`{cursor}` < ?) OR (`{cursor}` = ? AND CAST(`id` AS SIGNED) <= ?)",
        cursor = scope.cursor_col,
    )
}

pub struct SourceProbeAdapter {
    pool: sqlx::MySqlPool,
}
impl SourceProbeAdapter {
    pub fn new(pool: sqlx::MySqlPool) -> Self {
        Self { pool }
    }
}
impl SourceProbe for SourceProbeAdapter {
    async fn row_count(&self, table: &str) -> Result<i64> {
        let row: (i64,) = sqlx::query_as(&format!("SELECT COUNT(*) FROM `{table}`"))
            .fetch_one(&self.pool)
            .await
            .with_context(|| format!("source COUNT(*) for `{table}`"))?;
        Ok(row.0)
    }

    async fn row_count_scoped(&self, table: &str, scope: &SourceScope) -> Result<i64> {
        let predicate = scope_predicate_sql(scope);
        let sql = format!("SELECT COUNT(*) FROM `{table}` WHERE {predicate}");
        let row: (i64,) = sqlx::query_as(&sql)
            .bind(&scope.updated_at)
            .bind(&scope.updated_at)
            .bind(scope.last_id)
            .fetch_one(&self.pool)
            .await
            .with_context(|| {
                format!(
                    "source scoped COUNT(*) for `{table}` on `{}`",
                    scope.cursor_col
                )
            })?;
        Ok(row.0)
    }

    async fn max_cursor(&self, table: &str, cursor_col: &str) -> Result<Option<String>> {
        let row: (Option<String>,) = sqlx::query_as(&format!(
            "SELECT CAST(MAX(`{cursor_col}`) AS CHAR) FROM `{table}`"
        ))
        .fetch_one(&self.pool)
        .await
        .with_context(|| format!("source MAX(`{cursor_col}`) for `{table}`"))?;
        Ok(row.0)
    }

    async fn columns(&self, table: &str) -> Result<Vec<ColumnMeta>> {
        let rows: Vec<(String, String, String)> = sqlx::query_as(
            "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE FROM information_schema.columns \
             WHERE table_schema = DATABASE() AND table_name = ? ORDER BY ORDINAL_POSITION",
        )
        .bind(table)
        .fetch_all(&self.pool)
        .await
        .with_context(|| format!("source schema for `{table}`"))?;
        Ok(rows
            .into_iter()
            .map(|(name, type_str, nullable)| ColumnMeta {
                name,
                type_str,
                nullable: nullable == "YES",
            })
            .collect())
    }

    async fn key_stats(&self, table: &str, key_col: &str) -> Result<KeyStats> {
        // MariaDB's BIT_XOR does not accept DISTINCT (syntax error). The source key
        // column is the primary key (unique), so BIT_XOR over all rows already equals
        // the fingerprint over distinct values — we reuse it for `distinct_xor`. The
        // Delta side, which may carry append-log duplicates, computes a true
        // bit_xor(distinct ...).
        let row: (i64, i64, Option<i64>, Option<i64>, Option<i64>, Option<String>) = sqlx::query_as(&format!(
            "SELECT COUNT(*), COUNT(DISTINCT `{key_col}`), \
             CAST(MIN(`{key_col}`) AS SIGNED), CAST(MAX(`{key_col}`) AS SIGNED), \
             CAST(BIT_XOR(`{key_col}`) AS SIGNED), CAST(SUM(`{key_col}`) AS CHAR) FROM `{table}`"
        ))
        .fetch_one(&self.pool)
        .await
        .with_context(|| format!("source key_stats for `{table}`.`{key_col}`"))?;
        let xor = row.4.unwrap_or(0);
        let sum = row.5.as_deref().unwrap_or("0").parse::<i128>().unwrap_or(0);
        Ok(KeyStats {
            count: row.0,
            distinct: row.1,
            min: row.2,
            max: row.3,
            xor,
            distinct_xor: xor,
            sum,
        })
    }

    async fn key_stats_scoped(
        &self,
        table: &str,
        key_col: &str,
        scope: &SourceScope,
    ) -> Result<KeyStats> {
        let predicate = scope_predicate_sql(scope);
        let sql = format!(
            "SELECT COUNT(*), COUNT(DISTINCT `{key_col}`),              CAST(MIN(`{key_col}`) AS SIGNED), CAST(MAX(`{key_col}`) AS SIGNED),              CAST(BIT_XOR(`{key_col}`) AS SIGNED), CAST(SUM(`{key_col}`) AS CHAR) FROM `{table}`              WHERE {predicate}"
        );
        let row: (i64, i64, Option<i64>, Option<i64>, Option<i64>, Option<String>) = sqlx::query_as(&sql)
            .bind(&scope.updated_at)
            .bind(&scope.updated_at)
            .bind(scope.last_id)
            .fetch_one(&self.pool)
            .await
            .with_context(|| {
                format!(
                    "source scoped key_stats for `{table}`.`{key_col}` on `{}`",
                    scope.cursor_col
                )
            })?;
        let xor = row.4.unwrap_or(0);
        let sum = row.5.as_deref().unwrap_or("0").parse::<i128>().unwrap_or(0);
        Ok(KeyStats {
            count: row.0,
            distinct: row.1,
            min: row.2,
            max: row.3,
            xor,
            distinct_xor: xor,
            sum,
        })
    }

    async fn non_null_counts(&self, table: &str, columns: &[String]) -> Result<Vec<i64>> {
        if columns.is_empty() {
            return Ok(vec![]);
        }
        let select_list = columns
            .iter()
            .map(|c| format!("COUNT(`{c}`)"))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!("SELECT {select_list} FROM `{table}`");
        let row = sqlx::query(&sql)
            .fetch_one(&self.pool)
            .await
            .with_context(|| format!("source non_null_counts for `{table}`"))?;
        let mut out = Vec::with_capacity(columns.len());
        for i in 0..columns.len() {
            out.push(row.try_get::<i64, _>(i)?);
        }
        Ok(out)
    }

    async fn sample_ids(&self, table: &str, id_col: &str, limit: i64) -> Result<Vec<i64>> {
        if limit <= 0 {
            return Ok(vec![]);
        }
        let sql = format!(
            "SELECT CAST(`{id_col}` AS SIGNED) FROM `{table}` ORDER BY `{id_col}` LIMIT {limit}"
        );
        let rows = sqlx::query(&sql)
            .fetch_all(&self.pool)
            .await
            .with_context(|| format!("source sample_ids for `{table}`.`{id_col}`"))?;
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            out.push(row.try_get::<i64, _>(0)?);
        }
        Ok(out)
    }

    async fn sample_rows(
        &self,
        table: &str,
        id_col: &str,
        columns: &[String],
        ids: &[i64],
    ) -> Result<HashMap<i64, Vec<Option<String>>>> {
        if columns.is_empty() || ids.is_empty() {
            return Ok(HashMap::new());
        }
        let mut select_parts = vec![format!("CAST(`{id_col}` AS SIGNED)")];
        for c in columns {
            select_parts.push(format!("CAST(`{c}` AS CHAR)"));
        }
        let select_list = select_parts.join(", ");
        let ids_list = ids
            .iter()
            .map(|id| id.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!("SELECT {select_list} FROM `{table}` WHERE `{id_col}` IN ({ids_list})");
        let rows = sqlx::query(&sql)
            .fetch_all(&self.pool)
            .await
            .with_context(|| format!("source sample_rows for `{table}`.`{id_col}`"))?;
        let mut map = HashMap::new();
        for row in rows {
            let id = row.try_get::<i64, _>(0)?;
            let mut vals = Vec::with_capacity(columns.len());
            for i in 0..columns.len() {
                vals.push(row.try_get::<Option<String>, _>(i + 1)?);
            }
            map.insert(id, vals);
        }
        Ok(map)
    }

    async fn value_aggregates(&self, table: &str, columns: &[ColumnAgg]) -> Result<Vec<String>> {
        let mut fingerprints = Vec::with_capacity(columns.len());
        for col in columns {
            let fp = match col.kind {
                AggKind::Integer => {
                    let row: (Option<String>, Option<String>, Option<String>) = sqlx::query_as(&format!(
                        "SELECT CAST(SUM(`{col}`) AS CHAR), CAST(MIN(`{col}`) AS CHAR), CAST(MAX(`{col}`) AS CHAR) FROM `{table}`",
                        col = col.name
                    ))
                    .fetch_one(&self.pool)
                    .await
                    .with_context(|| format!("source value_aggregates {table}.{}", col.name))?;
                    super::fp_num(row.0.as_deref(), row.1.as_deref(), row.2.as_deref())
                }
                AggKind::Decimal => {
                    let row: (Option<String>, Option<String>, Option<String>) = sqlx::query_as(&format!(
                        "SELECT CAST(CAST(SUM(`{col}`) AS DECIMAL(38,10)) AS CHAR), CAST(CAST(MIN(`{col}`) AS DECIMAL(38,10)) AS CHAR), CAST(CAST(MAX(`{col}`) AS DECIMAL(38,10)) AS CHAR) FROM `{table}`",
                        col = col.name
                    ))
                    .fetch_one(&self.pool)
                    .await
                    .with_context(|| format!("source value_aggregates {table}.{}", col.name))?;
                    super::fp_num(row.0.as_deref(), row.1.as_deref(), row.2.as_deref())
                }
                AggKind::DatetimeSec => {
                    let row: (Option<String>, Option<String>) = sqlx::query_as(&format!(
                        "SELECT DATE_FORMAT(MIN(`{col}`), '%Y-%m-%d %H:%i:%s'), DATE_FORMAT(MAX(`{col}`), '%Y-%m-%d %H:%i:%s') FROM `{table}`",
                        col = col.name
                    ))
                    .fetch_one(&self.pool)
                    .await
                    .with_context(|| format!("source value_aggregates {table}.{}", col.name))?;
                    super::fp_minmax(row.0.as_deref(), row.1.as_deref())
                }
                AggKind::DateOnly => {
                    let row: (Option<String>, Option<String>) = sqlx::query_as(&format!(
                        "SELECT DATE_FORMAT(MIN(`{col}`), '%Y-%m-%d'), DATE_FORMAT(MAX(`{col}`), '%Y-%m-%d') FROM `{table}`",
                        col = col.name
                    ))
                    .fetch_one(&self.pool)
                    .await
                    .with_context(|| format!("source value_aggregates {table}.{}", col.name))?;
                    super::fp_minmax(row.0.as_deref(), row.1.as_deref())
                }
                AggKind::TextMass => {
                    let row: (Option<String>, i64) = sqlx::query_as(&format!(
                        "SELECT CAST(SUM(CHAR_LENGTH(`{col}`)) AS CHAR), COUNT(`{col}`) FROM `{table}`",
                        col = col.name
                    ))
                    .fetch_one(&self.pool)
                    .await
                    .with_context(|| format!("source value_aggregates {table}.{}", col.name))?;
                    super::fp_textmass(row.0.as_deref(), row.1)
                }
            };
            fingerprints.push(fp);
        }
        Ok(fingerprints)
    }
}
