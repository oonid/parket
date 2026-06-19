use anyhow::{Context, Result};
use deltalake::arrow::array::{Array, Int64Array, StringArray, StringViewArray};
use deltalake::datafusion::prelude::SessionContext;
use sqlx::Row;
use std::collections::HashMap;

use crate::writer::DeltaWriter;

/// Table verdict outcome from verification.
#[derive(Debug, Clone, PartialEq)]
pub enum TableOutcome {
    Pass,
    Drift { reason: String },
    Discrepancy { reason: String },
    Skipped { reason: String },
}

/// Overall verification verdict.
#[derive(Debug, Clone, PartialEq)]
pub enum VerifyVerdict {
    Clean,
    Discrepancy,
}

const DEFAULT_ROW_CAP: i64 = 1_000_000;
const SAMPLE_SIZE: i64 = 100;

/// Column types whose CAST-to-string is identical across MySQL and Delta, so they can be
/// value-compared safely. Excludes tinyint (bool ambiguity), decimal/float (precision),
/// date/datetime/timestamp/time (format/tz), json/blob/binary/enum/set.
fn is_value_comparable(type_str: &str) -> bool {
    matches!(
        type_str.to_ascii_lowercase().as_str(),
        "smallint" | "mediumint" | "int" | "integer" | "bigint"
            | "varchar" | "char" | "text" | "tinytext" | "mediumtext" | "longtext"
    )
}

/// Column metadata for a single table column (used by L0 schema reconciliation).
#[derive(Debug, Clone)]
pub struct ColumnMeta {
    pub name: String,
    pub type_str: String,
    pub nullable: bool,
}

/// L2 key-set fingerprint over a table's PK column.
#[derive(Debug, Clone, PartialEq)]
pub struct KeyStats {
    pub count: i64,
    pub distinct: i64,
    pub min: Option<i64>,
    pub max: Option<i64>,
    pub xor: i64,
    pub distinct_xor: i64,
}

/// Source-side probe (the live DB).
#[cfg_attr(test, mockall::automock)]
#[allow(async_fn_in_trait)]
pub trait SourceProbe: Send + Sync {
    async fn row_count(&self, table: &str) -> Result<i64>;
    async fn columns(&self, table: &str) -> Result<Vec<ColumnMeta>>;
    async fn key_stats(&self, table: &str, key_col: &str) -> Result<KeyStats>;
    async fn non_null_counts(&self, table: &str, columns: &[String]) -> Result<Vec<i64>>;
    async fn sample_ids(&self, table: &str, id_col: &str, limit: i64) -> Result<Vec<i64>>;
    async fn sample_rows(&self, table: &str, id_col: &str, columns: &[String], ids: &[i64]) -> Result<HashMap<i64, Vec<Option<String>>>>;
}

/// Delta-side probe (the synced output).
#[cfg_attr(test, mockall::automock)]
#[allow(async_fn_in_trait)]
pub trait DeltaProbe: Send + Sync {
    async fn row_count(&self, table: &str) -> Result<i64>;
    async fn columns(&self, table: &str) -> Result<Vec<ColumnMeta>>;
    async fn key_stats(&self, table: &str, key_col: &str) -> Result<KeyStats>;
    async fn non_null_counts(&self, table: &str, columns: &[String]) -> Result<Vec<i64>>;
    async fn sample_rows(&self, table: &str, id_col: &str, columns: &[String], ids: &[i64]) -> Result<HashMap<i64, Vec<Option<String>>>>;
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
        let row: (i64, i64, Option<i64>, Option<i64>, Option<i64>) = sqlx::query_as(&format!(
            "SELECT COUNT(*), COUNT(DISTINCT `{key_col}`), \
             CAST(MIN(`{key_col}`) AS SIGNED), CAST(MAX(`{key_col}`) AS SIGNED), \
             CAST(BIT_XOR(`{key_col}`) AS SIGNED) FROM `{table}`"
        ))
        .fetch_one(&self.pool)
        .await
        .with_context(|| format!("source key_stats for `{table}`.`{key_col}`"))?;
        let xor = row.4.unwrap_or(0);
        Ok(KeyStats {
            count: row.0,
            distinct: row.1,
            min: row.2,
            max: row.3,
            xor,
            distinct_xor: xor,
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

    async fn sample_rows(&self, table: &str, id_col: &str, columns: &[String], ids: &[i64]) -> Result<HashMap<i64, Vec<Option<String>>>> {
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
}

pub struct DeltaProbeAdapter {
    writer: DeltaWriter,
}
impl DeltaProbeAdapter {
    pub fn new(writer: DeltaWriter) -> Self {
        Self { writer }
    }
}
impl DeltaProbe for DeltaProbeAdapter {
    async fn row_count(&self, table: &str) -> Result<i64> {
        let t = self.writer.open_table(table).await?;
        let ctx = SessionContext::new();
        ctx.register_table("t", t.table_provider().await?)?;
        let batches = ctx.sql("SELECT count(*) AS n FROM t").await?.collect().await?;
        let n = batches
            .first()
            .and_then(|b| b.column(0).as_any().downcast_ref::<Int64Array>())
            .map(|a| a.value(0))
            .unwrap_or(0);
        Ok(n)
    }

    async fn columns(&self, table: &str) -> Result<Vec<ColumnMeta>> {
        let t = self.writer.open_table(table).await?;
        let schema = t.table_provider().await?.schema();
        Ok(schema
            .fields()
            .iter()
            .map(|f| ColumnMeta {
                name: f.name().clone(),
                type_str: format!("{:?}", f.data_type()),
                nullable: f.is_nullable(),
            })
            .collect())
    }

    async fn key_stats(&self, table: &str, key_col: &str) -> Result<KeyStats> {
        let t = self.writer.open_table(table).await?;
        let ctx = SessionContext::new();
        ctx.register_table("t", t.table_provider().await?)?;
        let sql = format!(
            "SELECT count(*) AS c, count(distinct `{key_col}`) AS d, \
             min(cast(`{key_col}` as bigint)) AS mn, max(cast(`{key_col}` as bigint)) AS mx, \
             bit_xor(cast(`{key_col}` as bigint)) AS x, bit_xor(distinct cast(`{key_col}` as bigint)) AS dx FROM t"
        );
        let batches = ctx.sql(&sql).await?.collect().await?;
        let b = batches.first().context("delta key_stats: empty result")?;
        // helper: read column `i` as Int64, returning Option (None if null)
        let col_opt = |i: usize| -> Option<i64> {
            b.column(i)
                .as_any()
                .downcast_ref::<Int64Array>()
                .and_then(|a| {
                    if a.is_empty() || a.is_null(0) {
                        None
                    } else {
                        Some(a.value(0))
                    }
                })
        };
        Ok(KeyStats {
            count: col_opt(0).unwrap_or(0),
            distinct: col_opt(1).unwrap_or(0),
            min: col_opt(2),
            max: col_opt(3),
            xor: col_opt(4).unwrap_or(0),
            distinct_xor: col_opt(5).unwrap_or(0),
        })
    }

    async fn non_null_counts(&self, table: &str, columns: &[String]) -> Result<Vec<i64>> {
        if columns.is_empty() {
            return Ok(vec![]);
        }
        let t = self.writer.open_table(table).await?;
        let ctx = SessionContext::new();
        ctx.register_table("t", t.table_provider().await?)?;
        let select_list = columns
            .iter()
            .enumerate()
            .map(|(i, c)| format!("count(`{c}`) AS c{i}"))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!("SELECT {select_list} FROM t");
        let batches = ctx.sql(&sql).await?.collect().await?;
        let b = batches.first().context("delta non_null_counts: empty result")?;
        let mut out = Vec::with_capacity(columns.len());
        for i in 0..columns.len() {
            let val = b.column(i)
                .as_any()
                .downcast_ref::<Int64Array>()
                .and_then(|a| {
                    if a.is_empty() || a.is_null(0) {
                        None
                    } else {
                        Some(a.value(0))
                    }
                })
                .unwrap_or(0);
            out.push(val);
        }
        Ok(out)
    }

    async fn sample_rows(&self, table: &str, id_col: &str, columns: &[String], ids: &[i64]) -> Result<HashMap<i64, Vec<Option<String>>>> {
        if columns.is_empty() || ids.is_empty() {
            return Ok(HashMap::new());
        }
        let t = self.writer.open_table(table).await?;
        let ctx = SessionContext::new();
        ctx.register_table("t", t.table_provider().await?)?;
        let mut select_parts = vec![format!("cast(`{id_col}` as bigint) AS k")];
        for (i, c) in columns.iter().enumerate() {
            select_parts.push(format!("cast(`{c}` as varchar) AS c{i}"));
        }
        let select_list = select_parts.join(", ");
        let ids_list = ids
            .iter()
            .map(|id| id.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!("SELECT {select_list} FROM t WHERE cast(`{id_col}` as bigint) IN ({ids_list})");
        let batches = ctx.sql(&sql).await?.collect().await?;
        let mut map = HashMap::new();
        for b in batches {
            let id_col_arr = b.column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .context("delta sample_rows: id column is not Int64Array")?;
            for r in 0..b.num_rows() {
                let id = id_col_arr.value(r);
                let mut vals = Vec::with_capacity(columns.len());
                for i in 0..columns.len() {
                    let col = b.column(i + 1);
                    let val = if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
                        if arr.is_null(r) {
                            None
                        } else {
                            Some(arr.value(r).to_string())
                        }
                    } else if let Some(arr) = col.as_any().downcast_ref::<StringViewArray>() {
                        if arr.is_null(r) {
                            None
                        } else {
                            Some(arr.value(r).to_string())
                        }
                    } else {
                        return Err(anyhow::anyhow!(
                            "delta sample_rows: value column {} has unsupported type {:?} (expected StringArray or StringViewArray)",
                            i,
                            col.data_type()
                        ));
                    };
                    vals.push(val);
                }
                map.insert(id, vals);
            }
        }
        Ok(map)
    }
}

pub struct VerifyCommand<S, D> {
    source: S,
    delta: D,
    tables: Vec<String>,
    deep: bool,
    row_cap: i64,
}
impl<S: SourceProbe, D: DeltaProbe> VerifyCommand<S, D> {
    pub fn new(source: S, delta: D, tables: Vec<String>) -> Self {
        Self {
            source,
            delta,
            tables,
            deep: false,
            row_cap: DEFAULT_ROW_CAP,
        }
    }

    pub fn with_deep(mut self, deep: bool) -> Self {
        self.deep = deep;
        self
    }

    /// VS2b: drift-gated tiered verdict + exit codes.
    /// Per-table verdict logic:
    /// 1. Schema: if missing_in_delta -> Discrepancy
    /// 2. Size guard: if !deep && source_row_count > row_cap -> Skipped
    /// 3. Count/key-set (if id column exists):
    ///    - Full match (all fields equal) -> Pass
    ///    - Distinct fallback (distinct+min+max+distinct_xor match) -> Pass
    ///    - Drift (delta range inside source range, delta smaller) -> Drift (not a failure)
    ///    - Else -> Discrepancy
    pub async fn run(&self) -> Result<VerifyVerdict> {
        let mut outcomes = Vec::new();

        for table in &self.tables {
            let scols = self.source.columns(table).await?;
            let dcols = self.delta.columns(table).await?;
            let dnames: std::collections::HashSet<&str> =
                dcols.iter().map(|c| c.name.as_str()).collect();
            let snames: std::collections::HashSet<&str> =
                scols.iter().map(|c| c.name.as_str()).collect();
            let missing_in_delta: Vec<&str> = scols
                .iter()
                .map(|c| c.name.as_str())
                .filter(|n| !dnames.contains(n))
                .collect();
            let extra_in_delta: Vec<&str> = dcols
                .iter()
                .map(|c| c.name.as_str())
                .filter(|n| !snames.contains(n))
                .collect();
            let schema_flag = if missing_in_delta.is_empty() && extra_in_delta.is_empty() {
                "schema ok"
            } else {
                "SCHEMA DIFF"
            };
            println!(
                "verify {table} schema: source_cols={} delta_cols={} missing_in_delta={:?} extra_in_delta={:?}  [{schema_flag}]",
                scols.len(), dcols.len(), missing_in_delta, extra_in_delta
            );

            let src_row_count = self.source.row_count(table).await?;
            let dlt_row_count = self.delta.row_count(table).await?;
            let flag = if src_row_count == dlt_row_count {
                "match"
            } else {
                "differ — see verdict"
            };
            println!("verify {table}: source={src_row_count} delta={dlt_row_count}  [{flag}]");

            // Capture delta key_stats for append-log detection
            let mut delta_keystats: Option<KeyStats> = None;

            // Compute verdict
            let outcome = if !missing_in_delta.is_empty() {
                // SCHEMA: missing columns
                TableOutcome::Discrepancy {
                    reason: format!("missing columns in Delta: {:?}", missing_in_delta),
                }
            } else if !self.deep && src_row_count > self.row_cap {
                // SIZE GUARD: skip large tables unless deep is enabled
                TableOutcome::Skipped {
                    reason: format!(
                        "table has {src_row_count} rows (> cap {cap}); pass --verify-deep to force strict checks",
                        cap = self.row_cap
                    ),
                }
            } else if !scols.iter().any(|c| c.name == "id") {
                // No id column for key-set verdict
                TableOutcome::Skipped {
                    reason: "no `id` column for key-set verdict".to_string(),
                }
            } else {
                // Compute key-set verdict
                let s = self.source.key_stats(table, "id").await?;
                let d = self.delta.key_stats(table, "id").await?;
                delta_keystats = Some(d.clone());

                if s == d {
                    // Full match
                    TableOutcome::Pass
                } else if s.distinct == d.distinct
                    && s.min == d.min
                    && s.max == d.max
                    && s.distinct_xor == d.distinct_xor
                {
                    // Distinct fallback: same unique ids and their fingerprint
                    TableOutcome::Pass
                } else if d.distinct <= s.distinct
                    && (d.max.is_none() || (s.max.is_some() && d.max <= s.max))
                    && (d.min.is_none() || (s.min.is_some() && d.min >= s.min))
                {
                    // Drift: Delta's range is inside source's range
                    TableOutcome::Drift {
                        reason: format!(
                            "source advanced past sync: source distinct={} delta distinct={} — \
                             likely new/changed rows since sync, not a sync error",
                            s.distinct, d.distinct
                        ),
                    }
                } else {
                    // Discrepancy: Delta has ids not in source or range mismatch
                    TableOutcome::Discrepancy {
                        reason: format!(
                            "Delta has ids/rows not in source: source(distinct={} min={:?} max={:?}) \
                             delta(distinct={} min={:?} max={:?})",
                            s.distinct, s.min, s.max, d.distinct, d.min, d.max
                        ),
                    }
                }
            };

            // Print per-table verdict
            match &outcome {
                TableOutcome::Pass => {
                    println!("verify {table} VERDICT: PASS");
                }
                TableOutcome::Drift { reason } => {
                    println!("verify {table} VERDICT: DRIFT: {reason}");
                }
                TableOutcome::Discrepancy { reason } => {
                    println!("verify {table} VERDICT: DISCREPANCY: {reason}");
                }
                TableOutcome::Skipped { reason } => {
                    println!("verify {table} VERDICT: SKIPPED: {reason}");
                }
            }

            // Run non-null census on Pass tables
            if matches!(&outcome, TableOutcome::Pass) {
                let delta_appendlog = matches!(&delta_keystats, Some(d) if d.count != d.distinct);
                if delta_appendlog {
                    let d = delta_keystats.as_ref().unwrap();
                    println!(
                        "verify {table} non-null census: skipped (Delta is append-log: {} rows / {} distinct ids — needs latest-per-id dedup)",
                        d.count, d.distinct
                    );
                    println!(
                        "verify {table} sample: skipped (Delta is append-log: {} rows / {} distinct ids)",
                        d.count, d.distinct
                    );
                } else {
                    let common: Vec<String> = scols
                        .iter()
                        .filter(|c| dnames.contains(c.name.as_str()))
                        .map(|c| c.name.clone())
                        .collect();
                    if !common.is_empty() {
                        let scounts = self.source.non_null_counts(table, &common).await?;
                        let dcounts = self.delta.non_null_counts(table, &common).await?;
                        let diffs: Vec<(String, i64, i64)> = common
                            .iter()
                            .zip(scounts.iter())
                            .zip(dcounts.iter())
                            .filter_map(|((col, s), d)| {
                                if s != d {
                                    Some((col.clone(), *s, *d))
                                } else {
                                    None
                                }
                            })
                            .collect();
                        if diffs.is_empty() {
                            println!(
                                "verify {table} non-null census: {} columns match",
                                common.len()
                            );
                        } else {
                            let diff_str = diffs
                                .iter()
                                .map(|(col, s, d)| format!("{col}={s}/{d}"))
                                .collect::<Vec<_>>()
                                .join(", ");
                            println!(
                                "verify {table} non-null census: DIFFERS in {} column(s): {diff_str}",
                                diffs.len()
                            );
                        }
                    }

                    // Run value spot-check on comparable columns
                    let comparable: Vec<String> = scols
                        .iter()
                        .filter(|c| {
                            dnames.contains(c.name.as_str())
                                && is_value_comparable(&c.type_str)
                                && c.name != "id"
                        })
                        .map(|c| c.name.clone())
                        .collect();
                    if !comparable.is_empty() {
                        let ids = self.source.sample_ids(table, "id", SAMPLE_SIZE).await?;
                        if !ids.is_empty() {
                            let srows = self
                                .source
                                .sample_rows(table, "id", &comparable, &ids)
                                .await?;
                            let drows = self
                                .delta
                                .sample_rows(table, "id", &comparable, &ids)
                                .await?;
                            let mut matched = 0;
                            let mut missing = 0;
                            let mut differing: Vec<(i64, Vec<String>)> = vec![];
                            for id in &ids {
                                match (srows.get(id), drows.get(id)) {
                                    (Some(sv), Some(dv)) => {
                                        let diff_cols: Vec<String> = comparable
                                            .iter()
                                            .enumerate()
                                            .filter_map(|(i, c)| {
                                                if sv.get(i) != dv.get(i) {
                                                    Some(c.clone())
                                                } else {
                                                    None
                                                }
                                            })
                                            .collect();
                                        if diff_cols.is_empty() {
                                            matched += 1;
                                        } else {
                                            differing.push((*id, diff_cols));
                                        }
                                    }
                                    _ => {
                                        missing += 1;
                                    }
                                }
                            }
                            println!(
                                "verify {table} sample: checked={} match={} differ={} missing={}",
                                ids.len(),
                                matched,
                                differing.len(),
                                missing
                            );
                            for (id, cols) in differing {
                                println!("verify {table} sample row {id}: differing columns {cols:?}");
                            }
                        }
                    }
                }
            }

            outcomes.push(outcome);
        }

        // Count outcomes
        let pass_count = outcomes.iter().filter(|o| **o == TableOutcome::Pass).count();
        let drift_count = outcomes
            .iter()
            .filter(|o| matches!(o, TableOutcome::Drift { .. }))
            .count();
        let discrepancy_count = outcomes
            .iter()
            .filter(|o| matches!(o, TableOutcome::Discrepancy { .. }))
            .count();
        let skipped_count = outcomes
            .iter()
            .filter(|o| matches!(o, TableOutcome::Skipped { .. }))
            .count();

        println!(
            "verify summary: pass={} drift={} discrepancy={} skipped={}",
            pass_count, drift_count, discrepancy_count, skipped_count
        );

        // Determine overall verdict
        if outcomes.iter().any(|o| matches!(o, TableOutcome::Discrepancy { .. })) {
            Ok(VerifyVerdict::Discrepancy)
        } else {
            Ok(VerifyVerdict::Clean)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn verify_reports_counts_for_each_table() {
        let mut source = MockSourceProbe::new();
        let mut delta = MockDeltaProbe::new();
        source.expect_row_count().returning(|_| Ok(100));
        delta.expect_row_count().returning(|_| Ok(100));
        let cols = || {
            Ok(vec![
                ColumnMeta {
                    name: "id".to_string(),
                    type_str: "bigint".to_string(),
                    nullable: false,
                },
                ColumnMeta {
                    name: "name".to_string(),
                    type_str: "varchar".to_string(),
                    nullable: true,
                },
            ])
        };
        source.expect_columns().returning(move |_| cols());
        delta.expect_columns().returning(move |_| cols());
        source.expect_key_stats().returning(|_, _| {
            Ok(KeyStats {
                count: 0,
                distinct: 0,
                min: None,
                max: None,
                xor: 0,
                distinct_xor: 0,
            })
        });
        delta.expect_key_stats().returning(|_, _| {
            Ok(KeyStats {
                count: 0,
                distinct: 0,
                min: None,
                max: None,
                xor: 0,
                distinct_xor: 0,
            })
        });
        source.expect_non_null_counts().returning(|_, cols: &[String]| Ok(vec![0i64; cols.len()]));
        delta.expect_non_null_counts().returning(|_, cols: &[String]| Ok(vec![0i64; cols.len()]));
        source.expect_sample_ids().returning(|_, _, _| Ok(vec![]));
        let cmd = VerifyCommand::new(
            source,
            delta,
            vec!["orders".to_string(), "users".to_string()],
        );
        let result = cmd.run().await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), VerifyVerdict::Clean);
    }

    #[tokio::test]
    async fn verify_reports_schema_diff() {
        let mut source = MockSourceProbe::new();
        let mut delta = MockDeltaProbe::new();
        source.expect_row_count().returning(|_| Ok(0));
        delta.expect_row_count().returning(|_| Ok(0));
        source.expect_columns().returning(|_| {
            Ok(vec![
                ColumnMeta {
                    name: "id".to_string(),
                    type_str: "bigint".to_string(),
                    nullable: false,
                },
                ColumnMeta {
                    name: "name".to_string(),
                    type_str: "varchar".to_string(),
                    nullable: true,
                },
                ColumnMeta {
                    name: "phone".to_string(),
                    type_str: "varchar".to_string(),
                    nullable: true,
                },
            ])
        });
        delta.expect_columns().returning(|_| {
            Ok(vec![
                ColumnMeta {
                    name: "id".to_string(),
                    type_str: "Int64".to_string(),
                    nullable: false,
                },
                ColumnMeta {
                    name: "name".to_string(),
                    type_str: "Utf8".to_string(),
                    nullable: true,
                },
            ])
        });
        source.expect_key_stats().returning(|_, _| {
            Ok(KeyStats {
                count: 0,
                distinct: 0,
                min: None,
                max: None,
                xor: 0,
                distinct_xor: 0,
            })
        });
        delta.expect_key_stats().returning(|_, _| {
            Ok(KeyStats {
                count: 0,
                distinct: 0,
                min: None,
                max: None,
                xor: 0,
                distinct_xor: 0,
            })
        });
        let cmd = VerifyCommand::new(source, delta, vec!["users".to_string()]);
        let result = cmd.run().await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), VerifyVerdict::Discrepancy);
    }

    #[tokio::test]
    async fn verify_reports_key_set() {
        let mut source = MockSourceProbe::new();
        let mut delta = MockDeltaProbe::new();
        source.expect_row_count().returning(|_| Ok(3));
        delta.expect_row_count().returning(|_| Ok(3));
        let cols = || {
            Ok(vec![ColumnMeta {
                name: "id".to_string(),
                type_str: "bigint".to_string(),
                nullable: false,
            }])
        };
        source.expect_columns().returning(move |_| cols());
        delta.expect_columns().returning(move |_| cols());
        let stats = || {
            Ok(KeyStats {
                count: 3,
                distinct: 3,
                min: Some(1),
                max: Some(3),
                xor: 1,
                distinct_xor: 1,
            })
        };
        source.expect_key_stats().returning(move |_, _| stats());
        delta.expect_key_stats().returning(move |_, _| stats());
        source.expect_non_null_counts().returning(|_, cols: &[String]| Ok(vec![0i64; cols.len()]));
        delta.expect_non_null_counts().returning(|_, cols: &[String]| Ok(vec![0i64; cols.len()]));
        source.expect_sample_ids().returning(|_, _, _| Ok(vec![]));
        let cmd = VerifyCommand::new(source, delta, vec!["users".to_string()]);
        let result = cmd.run().await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), VerifyVerdict::Clean);
    }

    #[tokio::test]
    async fn verify_verdict_pass() {
        let mut source = MockSourceProbe::new();
        let mut delta = MockDeltaProbe::new();
        source.expect_row_count().returning(|_| Ok(5));
        delta.expect_row_count().returning(|_| Ok(5));
        let cols = || {
            Ok(vec![ColumnMeta {
                name: "id".to_string(),
                type_str: "bigint".to_string(),
                nullable: false,
            }])
        };
        source.expect_columns().returning(move |_| cols());
        delta.expect_columns().returning(move |_| cols());
        let stats = || {
            Ok(KeyStats {
                count: 5,
                distinct: 5,
                min: Some(1),
                max: Some(5),
                xor: 7,
                distinct_xor: 7,
            })
        };
        source.expect_key_stats().returning(move |_, _| stats());
        delta.expect_key_stats().returning(move |_, _| stats());
        source.expect_non_null_counts().returning(|_, cols: &[String]| Ok(vec![0i64; cols.len()]));
        delta.expect_non_null_counts().returning(|_, cols: &[String]| Ok(vec![0i64; cols.len()]));
        source.expect_sample_ids().returning(|_, _, _| Ok(vec![]));
        let cmd = VerifyCommand::new(source, delta, vec!["items".to_string()]);
        let result = cmd.run().await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), VerifyVerdict::Clean);
    }

    #[tokio::test]
    async fn verify_verdict_drift_on_new_ids() {
        let mut source = MockSourceProbe::new();
        let mut delta = MockDeltaProbe::new();
        source.expect_row_count().returning(|_| Ok(10));
        delta.expect_row_count().returning(|_| Ok(5));
        let cols = || {
            Ok(vec![ColumnMeta {
                name: "id".to_string(),
                type_str: "bigint".to_string(),
                nullable: false,
            }])
        };
        source.expect_columns().returning(move |_| cols());
        delta.expect_columns().returning(move |_| cols());
        source.expect_key_stats().returning(|_, _| {
            Ok(KeyStats {
                count: 10,
                distinct: 10,
                min: Some(1),
                max: Some(10),
                xor: 11,
                distinct_xor: 11,
            })
        });
        delta.expect_key_stats().returning(|_, _| {
            Ok(KeyStats {
                count: 5,
                distinct: 5,
                min: Some(1),
                max: Some(5),
                xor: 15,
                distinct_xor: 15,
            })
        });
        let cmd = VerifyCommand::new(source, delta, vec!["events".to_string()]);
        let result = cmd.run().await;
        assert!(result.is_ok());
        // Drift is Clean (not a failure)
        assert_eq!(result.unwrap(), VerifyVerdict::Clean);
    }

    #[tokio::test]
    async fn verify_verdict_discrepancy_extra_delta_ids() {
        let mut source = MockSourceProbe::new();
        let mut delta = MockDeltaProbe::new();
        source.expect_row_count().returning(|_| Ok(5));
        delta.expect_row_count().returning(|_| Ok(5));
        let cols = || {
            Ok(vec![ColumnMeta {
                name: "id".to_string(),
                type_str: "bigint".to_string(),
                nullable: false,
            }])
        };
        source.expect_columns().returning(move |_| cols());
        delta.expect_columns().returning(move |_| cols());
        source.expect_key_stats().returning(|_, _| {
            Ok(KeyStats {
                count: 5,
                distinct: 5,
                min: Some(1),
                max: Some(5),
                xor: 7,
                distinct_xor: 7,
            })
        });
        delta.expect_key_stats().returning(|_, _| {
            Ok(KeyStats {
                count: 5,
                distinct: 5,
                min: Some(1),
                max: Some(10),
                xor: 15,
                distinct_xor: 15,
            })
        });
        let cmd = VerifyCommand::new(source, delta, vec!["orders".to_string()]);
        let result = cmd.run().await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), VerifyVerdict::Discrepancy);
    }

    #[tokio::test]
    async fn verify_verdict_distinct_fallback() {
        let mut source = MockSourceProbe::new();
        let mut delta = MockDeltaProbe::new();
        source.expect_row_count().returning(|_| Ok(3));
        delta.expect_row_count().returning(|_| Ok(5));
        let cols = || {
            Ok(vec![ColumnMeta {
                name: "id".to_string(),
                type_str: "bigint".to_string(),
                nullable: false,
            }])
        };
        source.expect_columns().returning(move |_| cols());
        delta.expect_columns().returning(move |_| cols());
        source.expect_key_stats().returning(|_, _| {
            Ok(KeyStats {
                count: 3,
                distinct: 3,
                min: Some(1),
                max: Some(3),
                xor: 1,
                distinct_xor: 1,
            })
        });
        delta.expect_key_stats().returning(|_, _| {
            Ok(KeyStats {
                count: 5,
                distinct: 3,
                min: Some(1),
                max: Some(3),
                xor: 5,
                distinct_xor: 1,
            })
        });
        let cmd = VerifyCommand::new(source, delta, vec!["logs".to_string()]);
        let result = cmd.run().await;
        assert!(result.is_ok());
        // Distinct fallback matches -> Clean
        assert_eq!(result.unwrap(), VerifyVerdict::Clean);
    }

    #[tokio::test]
    async fn verify_census_sample_skipped_on_appendlog() {
        let mut source = MockSourceProbe::new();
        let mut delta = MockDeltaProbe::new();
        source.expect_row_count().returning(|_| Ok(3));
        delta.expect_row_count().returning(|_| Ok(5));
        let cols = || {
            Ok(vec![
                ColumnMeta {
                    name: "id".to_string(),
                    type_str: "bigint".to_string(),
                    nullable: false,
                },
                ColumnMeta {
                    name: "data".to_string(),
                    type_str: "varchar".to_string(),
                    nullable: true,
                },
            ])
        };
        source.expect_columns().returning(move |_| cols());
        delta.expect_columns().returning(move |_| cols());
        source.expect_key_stats().returning(|_, _| {
            Ok(KeyStats {
                count: 3,
                distinct: 3,
                min: Some(1),
                max: Some(3),
                xor: 1,
                distinct_xor: 1,
            })
        });
        delta.expect_key_stats().returning(|_, _| {
            Ok(KeyStats {
                count: 5,
                distinct: 3,
                min: Some(1),
                max: Some(3),
                xor: 5,
                distinct_xor: 1,
            })
        });
        // Do NOT set expect_non_null_counts or expect_sample_ids — they must not be called
        // when Delta is append-log. If they are called, mockall will panic.
        let cmd = VerifyCommand::new(source, delta, vec!["events".to_string()]);
        let result = cmd.run().await;
        assert!(result.is_ok());
        // Pass due to distinct-fallback, but census/sample are skipped (no panic on unexpected calls)
        assert_eq!(result.unwrap(), VerifyVerdict::Clean);
    }

    #[tokio::test]
    async fn verify_verdict_size_skip() {
        let mut source = MockSourceProbe::new();
        let mut delta = MockDeltaProbe::new();
        source.expect_row_count().returning(|_| Ok(5_000_000));
        delta.expect_row_count().returning(|_| Ok(4_000_000));
        let cols = || {
            Ok(vec![ColumnMeta {
                name: "id".to_string(),
                type_str: "bigint".to_string(),
                nullable: false,
            }])
        };
        source.expect_columns().returning(move |_| cols());
        delta.expect_columns().returning(move |_| cols());
        let cmd = VerifyCommand::new(source, delta, vec!["big_table".to_string()]).with_deep(false);
        let result = cmd.run().await;
        assert!(result.is_ok());
        // Skipped is Clean (not a failure)
        assert_eq!(result.unwrap(), VerifyVerdict::Clean);
    }

    #[tokio::test]
    async fn verify_verdict_missing_column_fails() {
        let mut source = MockSourceProbe::new();
        let mut delta = MockDeltaProbe::new();
        source.expect_row_count().returning(|_| Ok(100));
        delta.expect_row_count().returning(|_| Ok(100));
        source.expect_columns().returning(|_| {
            Ok(vec![
                ColumnMeta {
                    name: "id".to_string(),
                    type_str: "bigint".to_string(),
                    nullable: false,
                },
                ColumnMeta {
                    name: "created_at".to_string(),
                    type_str: "timestamp".to_string(),
                    nullable: false,
                },
            ])
        });
        delta.expect_columns().returning(|_| {
            Ok(vec![ColumnMeta {
                name: "id".to_string(),
                type_str: "Int64".to_string(),
                nullable: false,
            }])
        });
        let cmd = VerifyCommand::new(source, delta, vec!["events".to_string()]);
        let result = cmd.run().await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), VerifyVerdict::Discrepancy);
    }

    #[tokio::test]
    async fn verify_census_match() {
        let mut source = MockSourceProbe::new();
        let mut delta = MockDeltaProbe::new();
        source.expect_row_count().returning(|_| Ok(10));
        delta.expect_row_count().returning(|_| Ok(10));
        let cols = || {
            Ok(vec![
                ColumnMeta {
                    name: "id".to_string(),
                    type_str: "bigint".to_string(),
                    nullable: false,
                },
                ColumnMeta {
                    name: "name".to_string(),
                    type_str: "varchar".to_string(),
                    nullable: true,
                },
            ])
        };
        source.expect_columns().returning(move |_| cols());
        delta.expect_columns().returning(move |_| cols());
        let stats = || {
            Ok(KeyStats {
                count: 10,
                distinct: 10,
                min: Some(1),
                max: Some(10),
                xor: 15,
                distinct_xor: 15,
            })
        };
        source.expect_key_stats().returning(move |_, _| stats());
        delta.expect_key_stats().returning(move |_, _| stats());
        source.expect_non_null_counts().returning(|_, _| Ok(vec![10, 8]));
        delta.expect_non_null_counts().returning(|_, _| Ok(vec![10, 8]));
        source.expect_sample_ids().returning(|_, _, _| Ok(vec![]));
        let cmd = VerifyCommand::new(source, delta, vec!["users".to_string()]);
        let result = cmd.run().await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), VerifyVerdict::Clean);
    }

    #[tokio::test]
    async fn verify_census_differs_is_non_failing() {
        let mut source = MockSourceProbe::new();
        let mut delta = MockDeltaProbe::new();
        source.expect_row_count().returning(|_| Ok(10));
        delta.expect_row_count().returning(|_| Ok(10));
        let cols = || {
            Ok(vec![
                ColumnMeta {
                    name: "id".to_string(),
                    type_str: "bigint".to_string(),
                    nullable: false,
                },
                ColumnMeta {
                    name: "name".to_string(),
                    type_str: "varchar".to_string(),
                    nullable: true,
                },
            ])
        };
        source.expect_columns().returning(move |_| cols());
        delta.expect_columns().returning(move |_| cols());
        let stats = || {
            Ok(KeyStats {
                count: 10,
                distinct: 10,
                min: Some(1),
                max: Some(10),
                xor: 15,
                distinct_xor: 15,
            })
        };
        source.expect_key_stats().returning(move |_, _| stats());
        delta.expect_key_stats().returning(move |_, _| stats());
        source.expect_non_null_counts().returning(|_, _| Ok(vec![10, 8]));
        delta.expect_non_null_counts().returning(|_, _| Ok(vec![10, 5]));
        source.expect_sample_ids().returning(|_, _, _| Ok(vec![]));
        let cmd = VerifyCommand::new(source, delta, vec!["users".to_string()]);
        let result = cmd.run().await;
        assert!(result.is_ok());
        // Census diff must NOT cause a failure, still Clean
        assert_eq!(result.unwrap(), VerifyVerdict::Clean);
    }

    #[tokio::test]
    async fn verify_sample_match() {
        let mut source = MockSourceProbe::new();
        let mut delta = MockDeltaProbe::new();
        source.expect_row_count().returning(|_| Ok(10));
        delta.expect_row_count().returning(|_| Ok(10));
        let cols = || {
            Ok(vec![
                ColumnMeta {
                    name: "id".to_string(),
                    type_str: "bigint".to_string(),
                    nullable: false,
                },
                ColumnMeta {
                    name: "name".to_string(),
                    type_str: "varchar".to_string(),
                    nullable: true,
                },
            ])
        };
        source.expect_columns().returning(move |_| cols());
        delta.expect_columns().returning(move |_| cols());
        let stats = || {
            Ok(KeyStats {
                count: 10,
                distinct: 10,
                min: Some(1),
                max: Some(10),
                xor: 15,
                distinct_xor: 15,
            })
        };
        source.expect_key_stats().returning(move |_, _| stats());
        delta.expect_key_stats().returning(move |_, _| stats());
        source.expect_non_null_counts().returning(|_, _| Ok(vec![10, 8]));
        delta.expect_non_null_counts().returning(|_, _| Ok(vec![10, 8]));
        source.expect_sample_ids().returning(|_, _, _| Ok(vec![1, 2]));
        let mut sample_rows_map = std::collections::HashMap::new();
        sample_rows_map.insert(1i64, vec![Some("a".to_string())]);
        sample_rows_map.insert(2i64, vec![Some("b".to_string())]);
        let sample_rows_map_src = sample_rows_map.clone();
        let sample_rows_map_delta = sample_rows_map.clone();
        source
            .expect_sample_rows()
            .returning(move |_, _, _, _| Ok(sample_rows_map_src.clone()));
        delta
            .expect_sample_rows()
            .returning(move |_, _, _, _| Ok(sample_rows_map_delta.clone()));
        let cmd = VerifyCommand::new(source, delta, vec!["users".to_string()]);
        let result = cmd.run().await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), VerifyVerdict::Clean);
    }

    #[tokio::test]
    async fn verify_sample_differs_non_failing() {
        let mut source = MockSourceProbe::new();
        let mut delta = MockDeltaProbe::new();
        source.expect_row_count().returning(|_| Ok(10));
        delta.expect_row_count().returning(|_| Ok(10));
        let cols = || {
            Ok(vec![
                ColumnMeta {
                    name: "id".to_string(),
                    type_str: "bigint".to_string(),
                    nullable: false,
                },
                ColumnMeta {
                    name: "name".to_string(),
                    type_str: "varchar".to_string(),
                    nullable: true,
                },
            ])
        };
        source.expect_columns().returning(move |_| cols());
        delta.expect_columns().returning(move |_| cols());
        let stats = || {
            Ok(KeyStats {
                count: 10,
                distinct: 10,
                min: Some(1),
                max: Some(10),
                xor: 15,
                distinct_xor: 15,
            })
        };
        source.expect_key_stats().returning(move |_, _| stats());
        delta.expect_key_stats().returning(move |_, _| stats());
        source.expect_non_null_counts().returning(|_, _| Ok(vec![10, 8]));
        delta.expect_non_null_counts().returning(|_, _| Ok(vec![10, 8]));
        source.expect_sample_ids().returning(|_, _, _| Ok(vec![1, 2]));
        let mut source_rows_map = std::collections::HashMap::new();
        source_rows_map.insert(1i64, vec![Some("a".to_string())]);
        source_rows_map.insert(2i64, vec![Some("b".to_string())]);
        let mut delta_rows_map = std::collections::HashMap::new();
        delta_rows_map.insert(1i64, vec![Some("a".to_string())]);
        delta_rows_map.insert(2i64, vec![Some("X".to_string())]);
        let source_rows_for_closure = source_rows_map.clone();
        let delta_rows_for_closure = delta_rows_map.clone();
        source
            .expect_sample_rows()
            .returning(move |_, _, _, _| Ok(source_rows_for_closure.clone()));
        delta
            .expect_sample_rows()
            .returning(move |_, _, _, _| Ok(delta_rows_for_closure.clone()));
        let cmd = VerifyCommand::new(source, delta, vec!["users".to_string()]);
        let result = cmd.run().await;
        assert!(result.is_ok());
        // Sample diff must NOT cause a failure, still Clean
        assert_eq!(result.unwrap(), VerifyVerdict::Clean);
    }

    // ===== INTEGRATION TESTS (real local Delta table) =====
    use deltalake::arrow::datatypes::Field;
    use deltalake::arrow::record_batch::RecordBatch;

    #[tokio::test]
    async fn delta_probe_row_count_real() {
        let temp = tempfile::tempdir().unwrap();
        let writer = DeltaWriter::new_local(temp.path().to_str().unwrap());
        let schema = std::sync::Arc::new(deltalake::arrow::datatypes::Schema::new(vec![
            Field::new("id", deltalake::arrow::datatypes::DataType::Int64, false),
            Field::new("name", deltalake::arrow::datatypes::DataType::Utf8, true),
            Field::new("qty", deltalake::arrow::datatypes::DataType::Int64, false),
        ]));
        writer.ensure_table("orders", schema.clone()).await.unwrap();
        let batch = RecordBatch::try_new(
            schema,
            vec![
                std::sync::Arc::new(Int64Array::from(vec![1i64, 2i64, 3i64])),
                std::sync::Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])),
                std::sync::Arc::new(Int64Array::from(vec![10i64, 20i64, 30i64])),
            ],
        )
        .unwrap();
        writer
            .append_batch("orders", vec![batch], None)
            .await
            .unwrap();

        let probe = DeltaProbeAdapter::new(writer);
        let count = probe.row_count("orders").await.unwrap();
        assert_eq!(count, 3);
    }

    #[tokio::test]
    async fn delta_probe_columns_real() {
        let temp = tempfile::tempdir().unwrap();
        let writer = DeltaWriter::new_local(temp.path().to_str().unwrap());
        let schema = std::sync::Arc::new(deltalake::arrow::datatypes::Schema::new(vec![
            Field::new("id", deltalake::arrow::datatypes::DataType::Int64, false),
            Field::new("name", deltalake::arrow::datatypes::DataType::Utf8, true),
            Field::new("qty", deltalake::arrow::datatypes::DataType::Int64, false),
        ]));
        writer.ensure_table("orders", schema.clone()).await.unwrap();
        let batch = RecordBatch::try_new(
            schema,
            vec![
                std::sync::Arc::new(Int64Array::from(vec![1i64, 2i64, 3i64])),
                std::sync::Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])),
                std::sync::Arc::new(Int64Array::from(vec![10i64, 20i64, 30i64])),
            ],
        )
        .unwrap();
        writer
            .append_batch("orders", vec![batch], None)
            .await
            .unwrap();

        let probe = DeltaProbeAdapter::new(writer);
        let cols = probe.columns("orders").await.unwrap();
        assert!(cols.iter().any(|c| c.name == "id"));
        assert!(cols.iter().any(|c| c.name == "name"));
        assert!(cols.iter().any(|c| c.name == "qty"));
    }

    #[tokio::test]
    async fn delta_probe_key_stats_real() {
        let temp = tempfile::tempdir().unwrap();
        let writer = DeltaWriter::new_local(temp.path().to_str().unwrap());
        let schema = std::sync::Arc::new(deltalake::arrow::datatypes::Schema::new(vec![
            Field::new("id", deltalake::arrow::datatypes::DataType::Int64, false),
            Field::new("name", deltalake::arrow::datatypes::DataType::Utf8, true),
            Field::new("qty", deltalake::arrow::datatypes::DataType::Int64, false),
        ]));
        writer.ensure_table("orders", schema.clone()).await.unwrap();
        let batch = RecordBatch::try_new(
            schema,
            vec![
                std::sync::Arc::new(Int64Array::from(vec![1i64, 2i64, 3i64])),
                std::sync::Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])),
                std::sync::Arc::new(Int64Array::from(vec![10i64, 20i64, 30i64])),
            ],
        )
        .unwrap();
        writer
            .append_batch("orders", vec![batch], None)
            .await
            .unwrap();

        let probe = DeltaProbeAdapter::new(writer);
        let ks = probe.key_stats("orders", "id").await.unwrap();
        assert_eq!(ks.count, 3);
        assert_eq!(ks.distinct, 3);
        assert_eq!(ks.min, Some(1));
        assert_eq!(ks.max, Some(3));
        assert_eq!(ks.xor, 0); // 1 ^ 2 ^ 3 = 0
        assert_eq!(ks.distinct_xor, 0); // 1 ^ 2 ^ 3 = 0
    }

    #[tokio::test]
    async fn delta_probe_non_null_counts_real() {
        let temp = tempfile::tempdir().unwrap();
        let writer = DeltaWriter::new_local(temp.path().to_str().unwrap());
        let schema = std::sync::Arc::new(deltalake::arrow::datatypes::Schema::new(vec![
            Field::new("id", deltalake::arrow::datatypes::DataType::Int64, false),
            Field::new("name", deltalake::arrow::datatypes::DataType::Utf8, true),
            Field::new("qty", deltalake::arrow::datatypes::DataType::Int64, false),
        ]));
        writer.ensure_table("orders", schema.clone()).await.unwrap();
        let batch = RecordBatch::try_new(
            schema,
            vec![
                std::sync::Arc::new(Int64Array::from(vec![1i64, 2i64, 3i64])),
                std::sync::Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])),
                std::sync::Arc::new(Int64Array::from(vec![10i64, 20i64, 30i64])),
            ],
        )
        .unwrap();
        writer
            .append_batch("orders", vec![batch], None)
            .await
            .unwrap();

        let probe = DeltaProbeAdapter::new(writer);
        let counts = probe
            .non_null_counts(
                "orders",
                &[
                    "id".to_string(),
                    "name".to_string(),
                    "qty".to_string(),
                ],
            )
            .await
            .unwrap();
        assert_eq!(counts, vec![3, 2, 3]); // name has 1 NULL
    }

    #[tokio::test]
    async fn delta_probe_sample_rows_real() {
        let temp = tempfile::tempdir().unwrap();
        let writer = DeltaWriter::new_local(temp.path().to_str().unwrap());
        let schema = std::sync::Arc::new(deltalake::arrow::datatypes::Schema::new(vec![
            Field::new("id", deltalake::arrow::datatypes::DataType::Int64, false),
            Field::new("name", deltalake::arrow::datatypes::DataType::Utf8, true),
            Field::new("qty", deltalake::arrow::datatypes::DataType::Int64, false),
        ]));
        writer.ensure_table("orders", schema.clone()).await.unwrap();
        let batch = RecordBatch::try_new(
            schema,
            vec![
                std::sync::Arc::new(Int64Array::from(vec![1i64, 2i64, 3i64])),
                std::sync::Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])),
                std::sync::Arc::new(Int64Array::from(vec![10i64, 20i64, 30i64])),
            ],
        )
        .unwrap();
        writer
            .append_batch("orders", vec![batch], None)
            .await
            .unwrap();

        let probe = DeltaProbeAdapter::new(writer);
        let rows = probe
            .sample_rows(
                "orders",
                "id",
                &["name".to_string(), "qty".to_string()],
                &[1, 2, 3],
            )
            .await
            .unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(
            rows.get(&1),
            Some(&vec![Some("a".to_string()), Some("10".to_string())])
        );
        assert_eq!(
            rows.get(&2),
            Some(&vec![None, Some("20".to_string())])
        );
        assert_eq!(
            rows.get(&3),
            Some(&vec![Some("c".to_string()), Some("30".to_string())])
        );
    }
}
