use anyhow::{Context, Result};
use sqlx::Row;
use std::collections::HashMap;

use super::{
    AggKind, ColumnAgg, ColumnAggValues, ColumnMeta, KeyStats, SourceProbe, SourceScope,
    StringKeyStats,
};

fn scope_predicate_sql(scope: &SourceScope) -> String {
    // V8: DECIMAL(20,0) (not SIGNED) so a BIGINT UNSIGNED key above i64::MAX compares
    // correctly against the i64 `last_id` bound instead of wrapping negative and being
    // wrongly included.
    format!(
        "(`{cursor}` < ?) OR (`{cursor}` = ? AND CAST(`{key}` AS DECIMAL(20,0)) <= ?)",
        cursor = scope.cursor_col,
        key = scope.key_col,
    )
}

/// Parse a MariaDB `CAST(... AS DECIMAL(20,0)) AS CHAR` key-stats MIN/MAX reading into an
/// `i128` (V8: range-safe over the full u64 span, unlike the old SIGNED/i64 cast which
/// wrapped a BIGINT UNSIGNED value above i64::MAX to negative).
fn parse_key_bound(s: &Option<String>) -> Option<i128> {
    s.as_deref().and_then(|s| s.parse::<i128>().ok())
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
        // NUMERIC_SCALE is BIGINT UNSIGNED in MariaDB's information_schema; CAST to SIGNED
        // so sqlx decodes it into Option<i64> (same gotcha as NON_UNIQUE in discover_indexes).
        let rows: Vec<(String, String, String, Option<i64>)> = sqlx::query_as(
            "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, CAST(NUMERIC_SCALE AS SIGNED) FROM information_schema.columns \
             WHERE table_schema = DATABASE() AND table_name = ? ORDER BY ORDINAL_POSITION",
        )
        .bind(table)
        .fetch_all(&self.pool)
        .await
        .with_context(|| format!("source schema for `{table}`"))?;
        Ok(rows
            .into_iter()
            .map(|(name, type_str, nullable, numeric_scale)| ColumnMeta {
                name,
                type_str,
                nullable: nullable == "YES",
                // VA2: native scale drives the DECIMAL(65,scale) used in value aggregates,
                // instead of a historical fixed scale of 10. 65 is MariaDB's DECIMAL max
                // (and DataFusion Decimal256's max precision) — see VA1-r.
                numeric_scale: numeric_scale.and_then(|s| u32::try_from(s).ok()),
            })
            .collect())
    }

    /// V3: mirrors `select_integer_pk` in `discovery.rs` — exactly one
    /// column in the PRIMARY key, and that column is an integer type. Deliberately avoids
    /// selecting any numeric information_schema column (SEQ_IN_INDEX etc. are BIGINT
    /// UNSIGNED and need a CAST to decode via sqlx — simplest to just not select them).
    async fn integer_pk(&self, table: &str) -> Result<Option<String>> {
        let rows: Vec<(String, String)> = sqlx::query_as(
            "SELECT s.COLUMN_NAME, c.DATA_TYPE FROM information_schema.statistics s \
             JOIN information_schema.columns c \
               ON c.TABLE_SCHEMA = s.TABLE_SCHEMA AND c.TABLE_NAME = s.TABLE_NAME AND c.COLUMN_NAME = s.COLUMN_NAME \
             WHERE s.TABLE_SCHEMA = DATABASE() AND s.TABLE_NAME = ? AND s.INDEX_NAME = 'PRIMARY'",
        )
        .bind(table)
        .fetch_all(&self.pool)
        .await
        .with_context(|| format!("source integer_pk lookup for `{table}`"))?;
        if rows.len() != 1 {
            // No PRIMARY key, or a composite one — neither is a usable single-column key.
            return Ok(None);
        }
        let (key_col, data_type) = &rows[0];
        let is_integer = matches!(
            data_type.to_ascii_lowercase().as_str(),
            "tinyint" | "smallint" | "mediumint" | "int" | "integer" | "bigint"
        );
        Ok(is_integer.then(|| key_col.clone()))
    }

    /// V3-r Tier 2: mirrors `integer_pk`'s single-column PRIMARY key lookup, but returns the
    /// column only when it is NOT an integer type (composite keys and integer keys are both
    /// `None` here — the latter is `integer_pk`'s job).
    async fn string_pk(&self, table: &str) -> Result<Option<String>> {
        let rows: Vec<(String, String)> = sqlx::query_as(
            "SELECT s.COLUMN_NAME, c.DATA_TYPE FROM information_schema.statistics s \
             JOIN information_schema.columns c \
               ON c.TABLE_SCHEMA = s.TABLE_SCHEMA AND c.TABLE_NAME = s.TABLE_NAME AND c.COLUMN_NAME = s.COLUMN_NAME \
             WHERE s.TABLE_SCHEMA = DATABASE() AND s.TABLE_NAME = ? AND s.INDEX_NAME = 'PRIMARY'",
        )
        .bind(table)
        .fetch_all(&self.pool)
        .await
        .with_context(|| format!("source string_pk lookup for `{table}`"))?;
        if rows.len() != 1 {
            // No PRIMARY key, or a composite one — neither is a usable single-column key.
            return Ok(None);
        }
        let (key_col, data_type) = &rows[0];
        let is_integer = matches!(
            data_type.to_ascii_lowercase().as_str(),
            "tinyint" | "smallint" | "mediumint" | "int" | "integer" | "bigint"
        );
        Ok((!is_integer).then(|| key_col.clone()))
    }

    async fn key_stats(&self, table: &str, key_col: &str) -> Result<KeyStats> {
        // MariaDB's BIT_XOR does not accept DISTINCT (syntax error). The source key
        // column is the primary key (unique), so BIT_XOR over all rows already equals
        // the fingerprint over distinct values — we reuse it for `distinct_xor`. The
        // Delta side, which may carry append-log duplicates, computes a true
        // bit_xor(distinct ...).
        // V8: MIN/MAX cast to DECIMAL(20,0)-as-CHAR (not SIGNED) so a BIGINT UNSIGNED key
        // above i64::MAX is captured exactly instead of wrapping negative; parsed to i128
        // below. BIT_XOR stays SIGNED/i64 (bit-preserving, not a magnitude).
        let row: (i64, i64, Option<String>, Option<String>, Option<i64>, Option<String>) = sqlx::query_as(&format!(
            "SELECT COUNT(*), COUNT(DISTINCT `{key_col}`), \
             CAST(CAST(MIN(`{key_col}`) AS DECIMAL(20,0)) AS CHAR), CAST(CAST(MAX(`{key_col}`) AS DECIMAL(20,0)) AS CHAR), \
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
            min: parse_key_bound(&row.2),
            max: parse_key_bound(&row.3),
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
            "SELECT COUNT(*), COUNT(DISTINCT `{key_col}`),              CAST(CAST(MIN(`{key_col}`) AS DECIMAL(20,0)) AS CHAR), CAST(CAST(MAX(`{key_col}`) AS DECIMAL(20,0)) AS CHAR),              CAST(BIT_XOR(`{key_col}`) AS SIGNED), CAST(SUM(`{key_col}`) AS CHAR) FROM `{table}`              WHERE {predicate}"
        );
        let row: (i64, i64, Option<String>, Option<String>, Option<i64>, Option<String>) = sqlx::query_as(&sql)
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
            min: parse_key_bound(&row.2),
            max: parse_key_bound(&row.3),
            xor,
            distinct_xor: xor,
            sum,
        })
    }

    /// V3-r Tier 2: BINARY-normalized so MIN/MAX/COUNT(DISTINCT) use byte ordering instead of
    /// the column's (often case-insensitive) collation — matches DataFusion's byte-ordered
    /// Utf8 comparison on the Delta side (the N8 collation lesson applied to the key).
    async fn string_key_stats(&self, table: &str, key_col: &str) -> Result<StringKeyStats> {
        let row: (i64, i64, Option<String>, Option<String>) = sqlx::query_as(&format!(
            "SELECT COUNT(*), COUNT(DISTINCT BINARY `{key_col}`), \
             CAST(MIN(BINARY `{key_col}`) AS CHAR), CAST(MAX(BINARY `{key_col}`) AS CHAR) FROM `{table}`"
        ))
        .fetch_one(&self.pool)
        .await
        .with_context(|| format!("source string_key_stats for `{table}`.`{key_col}`"))?;
        Ok(StringKeyStats {
            count: row.0,
            distinct: row.1,
            min: row.2,
            max: row.3,
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
        // V6: sample the LOWEST half and the HIGHEST half of the id range (not just the
        // lowest `limit`), so the most recently synced rows (highest ids) — where fresh-sync
        // corruption is most likely — are spot-checked too. UNION dedups any overlap when the
        // table has fewer than `limit` rows. Both probes then compare rows for these same ids.
        let high = limit / 2;
        let low = limit - high;
        let sql = format!(
            "SELECT k FROM ( \
               (SELECT CAST(`{id_col}` AS SIGNED) AS k FROM `{table}` ORDER BY `{id_col}` ASC LIMIT {low}) \
               UNION \
               (SELECT CAST(`{id_col}` AS SIGNED) AS k FROM `{table}` ORDER BY `{id_col}` DESC LIMIT {high}) \
             ) AS spread ORDER BY k"
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

    async fn value_aggregates(&self, table: &str, columns: &[ColumnAgg]) -> Result<Vec<ColumnAggValues>> {
        self.value_aggregates_impl(table, columns, None).await
    }

    async fn value_aggregates_scoped(
        &self,
        table: &str,
        columns: &[ColumnAgg],
        scope: &SourceScope,
    ) -> Result<Vec<ColumnAggValues>> {
        self.value_aggregates_impl(table, columns, Some(scope)).await
    }
}

impl SourceProbeAdapter {
    /// One SELECT per table, not one per column: every column's aggregate expressions are
    /// concatenated into a single select list (VA3/V4 — avoids N re-scans of the table).
    /// Column ordering matches `columns` order exactly; `column_exprs` documents how many
    /// select-list slots each `AggKind` consumes so the result row can be read back
    /// positionally.
    fn column_exprs(col: &ColumnAgg) -> Vec<String> {
        let c = &col.name;
        match col.kind {
            AggKind::Integer => vec![
                format!("CAST(SUM(`{c}`) AS CHAR)"),
                format!("CAST(MIN(`{c}`) AS CHAR)"),
                format!("CAST(MAX(`{c}`) AS CHAR)"),
                format!("COUNT(`{c}`)"),
            ],
            AggKind::Decimal { scale } => vec![
                format!("CAST(CAST(SUM(`{c}`) AS DECIMAL(65,{scale})) AS CHAR)"),
                format!("CAST(CAST(MIN(`{c}`) AS DECIMAL(65,{scale})) AS CHAR)"),
                format!("CAST(CAST(MAX(`{c}`) AS DECIMAL(65,{scale})) AS CHAR)"),
                format!("COUNT(`{c}`)"),
            ],
            AggKind::DatetimeSec => vec![
                format!("DATE_FORMAT(MIN(`{c}`), '%Y-%m-%d %H:%i:%s')"),
                format!("DATE_FORMAT(MAX(`{c}`), '%Y-%m-%d %H:%i:%s')"),
                format!("COUNT(`{c}`)"),
            ],
            AggKind::DateOnly => vec![
                format!("DATE_FORMAT(MIN(`{c}`), '%Y-%m-%d')"),
                format!("DATE_FORMAT(MAX(`{c}`), '%Y-%m-%d')"),
                format!("COUNT(`{c}`)"),
            ],
            AggKind::TextMass => vec![
                format!("CAST(SUM(CHAR_LENGTH(`{c}`)) AS CHAR)"),
                format!("COUNT(`{c}`)"),
            ],
        }
    }

    /// Read one column's slice of the aggregate row, advancing `offset` past however many
    /// slots that column's `AggKind` consumed.
    fn read_column_values(row: &sqlx::mysql::MySqlRow, offset: &mut usize, kind: &AggKind) -> Result<ColumnAggValues> {
        let values = match kind {
            AggKind::Integer | AggKind::Decimal { .. } => {
                let sum = row.try_get::<Option<String>, _>(*offset)?;
                let min = row.try_get::<Option<String>, _>(*offset + 1)?;
                let max = row.try_get::<Option<String>, _>(*offset + 2)?;
                let non_null_count = row.try_get::<i64, _>(*offset + 3)?;
                *offset += 4;
                ColumnAggValues { sum, min, max, non_null_count }
            }
            AggKind::DatetimeSec | AggKind::DateOnly => {
                let min = row.try_get::<Option<String>, _>(*offset)?;
                let max = row.try_get::<Option<String>, _>(*offset + 1)?;
                let non_null_count = row.try_get::<i64, _>(*offset + 2)?;
                *offset += 3;
                ColumnAggValues { sum: None, min, max, non_null_count }
            }
            AggKind::TextMass => {
                let sum = row.try_get::<Option<String>, _>(*offset)?;
                let non_null_count = row.try_get::<i64, _>(*offset + 1)?;
                *offset += 2;
                ColumnAggValues { sum, min: None, max: None, non_null_count }
            }
        };
        Ok(values)
    }

    async fn value_aggregates_impl(
        &self,
        table: &str,
        columns: &[ColumnAgg],
        scope: Option<&SourceScope>,
    ) -> Result<Vec<ColumnAggValues>> {
        if columns.is_empty() {
            return Ok(vec![]);
        }
        let select_list = columns
            .iter()
            .flat_map(Self::column_exprs)
            .collect::<Vec<_>>()
            .join(", ");
        let sql = match scope {
            Some(scope) => format!(
                "SELECT {select_list} FROM `{table}` WHERE {}",
                scope_predicate_sql(scope)
            ),
            None => format!("SELECT {select_list} FROM `{table}`"),
        };
        let mut query = sqlx::query(&sql);
        if let Some(scope) = scope {
            query = query
                .bind(&scope.updated_at)
                .bind(&scope.updated_at)
                .bind(scope.last_id);
        }
        let row = query
            .fetch_one(&self.pool)
            .await
            .with_context(|| format!("source value_aggregates for `{table}`"))?;
        let mut offset = 0;
        let mut out = Vec::with_capacity(columns.len());
        for col in columns {
            out.push(Self::read_column_values(&row, &mut offset, &col.kind)?);
        }
        Ok(out)
    }
}
