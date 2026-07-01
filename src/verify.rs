use anyhow::Result;
use std::collections::HashMap;

mod delta;
mod source;

use crate::writer::Hwm;
pub use delta::DeltaProbeAdapter;
pub use source::SourceProbeAdapter;

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

#[derive(Debug, Clone)]
pub enum VerifyMode {
    Basic,
    FullRefresh,
    Incremental {
        cursor_col: String,
        hwm: Option<Hwm>,
    },
    TwoStream {
        insert_cursor: String,
        update_cursor: String,
        update_hwm: Option<Hwm>,
        insert_hwm: Option<i64>,
    },
}

#[derive(Debug, Clone)]
pub struct TablePlan {
    pub table: String,
    pub mode: VerifyMode,
}

#[derive(Debug, Clone)]
pub struct SourceScope {
    pub cursor_col: String,
    pub updated_at: String,
    pub last_id: i64,
}

impl TablePlan {
    pub fn basic(table: impl Into<String>) -> Self {
        Self {
            table: table.into(),
            mode: VerifyMode::Basic,
        }
    }

    fn describe(&self) -> String {
        format!("mode={}", self.mode.describe())
    }
}

impl VerifyMode {
    fn describe(&self) -> String {
        match self {
            VerifyMode::Basic => "basic".to_string(),
            VerifyMode::FullRefresh => "full_refresh".to_string(),
            VerifyMode::Incremental { cursor_col, hwm } => {
                format!(
                    "incremental cursor={} hwm={}",
                    cursor_col,
                    format_hwm(hwm.as_ref())
                )
            }
            VerifyMode::TwoStream {
                insert_cursor,
                update_cursor,
                update_hwm,
                insert_hwm,
            } => format!(
                "two_stream insert_cursor={} update_cursor={} update_hwm={} insert_hwm={}",
                insert_cursor,
                update_cursor,
                format_hwm(update_hwm.as_ref()),
                format_i64_hwm(*insert_hwm)
            ),
        }
    }
}

fn format_hwm(hwm: Option<&Hwm>) -> String {
    match hwm {
        Some(hwm) => format!("updated_at={} last_id={}", hwm.updated_at, hwm.last_id),
        None => "none".to_string(),
    }
}

fn format_i64_hwm(hwm: Option<i64>) -> String {
    hwm.map(|value| value.to_string())
        .unwrap_or_else(|| "none".to_string())
}

const DEFAULT_ROW_CAP: i64 = 1_000_000;
const SAMPLE_SIZE: i64 = 100;

/// Column types whose CAST-to-string is identical across MySQL and Delta, so they can be
/// value-compared safely. Excludes tinyint (bool ambiguity), decimal/float (precision),
/// date/datetime/timestamp/time (format/tz), json/blob/binary/enum/set.
fn is_value_comparable(type_str: &str) -> bool {
    matches!(
        type_str.to_ascii_lowercase().as_str(),
        "smallint"
            | "mediumint"
            | "int"
            | "integer"
            | "bigint"
            | "varchar"
            | "char"
            | "text"
            | "tinytext"
            | "mediumtext"
            | "longtext"
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
    async fn row_count_scoped(&self, table: &str, scope: &SourceScope) -> Result<i64>;
    async fn columns(&self, table: &str) -> Result<Vec<ColumnMeta>>;
    async fn key_stats(&self, table: &str, key_col: &str) -> Result<KeyStats>;
    async fn key_stats_scoped(
        &self,
        table: &str,
        key_col: &str,
        scope: &SourceScope,
    ) -> Result<KeyStats>;
    async fn non_null_counts(&self, table: &str, columns: &[String]) -> Result<Vec<i64>>;
    async fn sample_ids(&self, table: &str, id_col: &str, limit: i64) -> Result<Vec<i64>>;
    async fn sample_rows(
        &self,
        table: &str,
        id_col: &str,
        columns: &[String],
        ids: &[i64],
    ) -> Result<HashMap<i64, Vec<Option<String>>>>;
}

/// Delta-side probe (the synced output).
#[cfg_attr(test, mockall::automock)]
#[allow(async_fn_in_trait)]
pub trait DeltaProbe: Send + Sync {
    async fn row_count(&self, table: &str) -> Result<i64>;
    async fn columns(&self, table: &str) -> Result<Vec<ColumnMeta>>;
    async fn key_stats(&self, table: &str, key_col: &str) -> Result<KeyStats>;
    async fn latest_key_stats(
        &self,
        table: &str,
        key_col: &str,
        cursor_col: &str,
    ) -> Result<KeyStats>;
    async fn non_null_counts(&self, table: &str, columns: &[String]) -> Result<Vec<i64>>;
    async fn sample_rows(
        &self,
        table: &str,
        id_col: &str,
        columns: &[String],
        ids: &[i64],
    ) -> Result<HashMap<i64, Vec<Option<String>>>>;
}

pub struct VerifyCommand<S, D> {
    source: S,
    delta: D,
    tables: Vec<String>,
    table_plans: std::collections::HashMap<String, TablePlan>,
    deep: bool,
    row_cap: i64,
}
impl<S: SourceProbe, D: DeltaProbe> VerifyCommand<S, D> {
    pub fn new(source: S, delta: D, tables: Vec<String>) -> Self {
        let table_plans = tables
            .iter()
            .cloned()
            .map(|table| {
                let plan = TablePlan::basic(table.clone());
                (table, plan)
            })
            .collect();
        Self {
            source,
            delta,
            tables,
            table_plans,
            deep: false,
            row_cap: DEFAULT_ROW_CAP,
        }
    }

    pub fn with_table_plans(mut self, table_plans: Vec<TablePlan>) -> Self {
        self.table_plans = table_plans
            .into_iter()
            .map(|plan| (plan.table.clone(), plan))
            .collect();
        self
    }

    pub fn with_deep(mut self, deep: bool) -> Self {
        self.deep = deep;
        self
    }

    fn key_stats_outcome(
        source_label: &str,
        delta_label: &str,
        source_stats: &KeyStats,
        delta_stats: &KeyStats,
    ) -> TableOutcome {
        if source_stats == delta_stats {
            TableOutcome::Pass
        } else if source_stats.distinct == delta_stats.distinct
            && source_stats.min == delta_stats.min
            && source_stats.max == delta_stats.max
            && source_stats.distinct_xor == delta_stats.distinct_xor
        {
            TableOutcome::Pass
        } else if delta_stats.distinct < source_stats.distinct
            && (delta_stats.max.is_none()
                || (source_stats.max.is_some() && delta_stats.max <= source_stats.max))
            && (delta_stats.min.is_none()
                || (source_stats.min.is_some() && delta_stats.min >= source_stats.min))
        {
            TableOutcome::Drift {
                reason: format!(
                    "{source_label} advanced past sync: {source_label} distinct={} {delta_label} distinct={} — likely new/changed rows since sync, not a sync error",
                    source_stats.distinct, delta_stats.distinct
                ),
            }
        } else {
            TableOutcome::Discrepancy {
                reason: format!(
                    "{delta_label} has ids/rows not in {source_label}: {source_label}(distinct={} min={:?} max={:?}) {delta_label}(distinct={} min={:?} max={:?})",
                    source_stats.distinct,
                    source_stats.min,
                    source_stats.max,
                    delta_stats.distinct,
                    delta_stats.min,
                    delta_stats.max
                ),
            }
        }
    }

    fn two_stream_key_stats_outcome(
        source_label: &str,
        delta_label: &str,
        source_stats: &KeyStats,
        delta_stats: &KeyStats,
    ) -> TableOutcome {
        if source_stats == delta_stats {
            return TableOutcome::Pass;
        }

        let source_range_contained_by_delta = match (source_stats.min, source_stats.max) {
            (None, None) => true,
            (Some(source_min), Some(source_max)) => match (delta_stats.min, delta_stats.max) {
                (Some(delta_min), Some(delta_max)) => {
                    delta_min <= source_min && source_max <= delta_max
                }
                _ => false,
            },
            _ => false,
        };

        let delta_has_extra_evidence = source_stats.count < delta_stats.count
            || source_stats.distinct < delta_stats.distinct;

        if delta_has_extra_evidence
            && source_stats.count <= delta_stats.count
            && source_stats.distinct <= delta_stats.distinct
            && source_range_contained_by_delta
        {
            TableOutcome::Drift {
                reason: format!(
                    "{delta_label} may legitimately retain extra ids/rows in two_stream mode: aggregate stats show {source_label}(count={} distinct={} min={:?} max={:?}) within {delta_label}(count={} distinct={} min={:?} max={:?}), but this does not prove exact set equality",
                    source_stats.count,
                    source_stats.distinct,
                    source_stats.min,
                    source_stats.max,
                    delta_stats.count,
                    delta_stats.distinct,
                    delta_stats.min,
                    delta_stats.max
                ),
            }
        } else {
            TableOutcome::Discrepancy {
                reason: format!(
                    "{source_label} appears to have ids/rows missing from {delta_label} in two_stream mode: {source_label}(count={} distinct={} min={:?} max={:?}) {delta_label}(count={} distinct={} min={:?} max={:?})",
                    source_stats.count,
                    source_stats.distinct,
                    source_stats.min,
                    source_stats.max,
                    delta_stats.count,
                    delta_stats.distinct,
                    delta_stats.min,
                    delta_stats.max
                ),
            }
        }
    }

    /// VS2b: drift-gated tiered verdict + exit codes.
    /// Per-table verdict logic:
    /// 1. Schema: if missing_in_delta -> Discrepancy
    /// 2. Size guard: if !deep && source_row_count > row_cap -> Skipped
    /// 3. Count/key-set (if id column exists):
    ///    - Full match (all fields equal) -> Pass
    ///    - Distinct fallback (distinct+min+max+distinct_xor match) -> Pass
    ///    - Drift (delta range inside source range, delta smaller) -> Drift (not a failure)
    ///    - Two-stream uses a separate conservative asymmetry path
    ///    - Else -> Discrepancy
    pub async fn run(&self) -> Result<VerifyVerdict> {
        let mut outcomes = Vec::new();

        for table in &self.tables {
            let default_plan = TablePlan::basic(table.clone());
            let plan = self.table_plans.get(table).unwrap_or(&default_plan);
            println!("verify {table} plan: {}", plan.describe());

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
                scols.len(),
                dcols.len(),
                missing_in_delta,
                extra_in_delta
            );
            let has_id = scols.iter().any(|c| c.name == "id");

            let incremental_scope = match &plan.mode {
                VerifyMode::Incremental {
                    cursor_col,
                    hwm: Some(hwm),
                } => Some(SourceScope {
                    cursor_col: cursor_col.clone(),
                    updated_at: hwm.updated_at.clone(),
                    last_id: hwm.last_id,
                }),
                _ => None,
            };

            let mut delta_keystats: Option<KeyStats> = None;
            let skip_pass_layers_reason = if incremental_scope.is_some() {
                Some("incremental scoped value reconciliation is deferred")
            } else {
                None
            };

            let src_row_count;
            let delta_label;
            if let Some(scope) = incremental_scope.as_ref().filter(|_| has_id) {
                src_row_count = self.source.row_count_scoped(table, scope).await?;
                delta_label = "delta_latest";
                let delta_stats = self
                    .delta
                    .latest_key_stats(table, "id", &scope.cursor_col)
                    .await?;
                let dlt_row_count = delta_stats.count;
                let flag = if src_row_count == dlt_row_count {
                    "match"
                } else {
                    "differ — see verdict"
                };
                println!(
                    "verify {table} incremental scope: source_scoped={src_row_count} delta_latest={dlt_row_count} cursor={} hwm={}  [{flag}]",
                    scope.cursor_col,
                    format!("updated_at={} last_id={}", scope.updated_at, scope.last_id)
                );
                delta_keystats = Some(delta_stats);
            } else {
                src_row_count = self.source.row_count(table).await?;
                let dlt_row_count = self.delta.row_count(table).await?;
                delta_label = "delta";
                let flag = if src_row_count == dlt_row_count {
                    "match"
                } else {
                    "differ — see verdict"
                };
                println!("verify {table}: source={src_row_count} delta={dlt_row_count}  [{flag}]");
            }

            let outcome = if !missing_in_delta.is_empty() {
                TableOutcome::Discrepancy {
                    reason: format!("missing columns in Delta: {:?}", missing_in_delta),
                }
            } else if !self.deep && src_row_count > self.row_cap {
                TableOutcome::Skipped {
                    reason: format!(
                        "table has {src_row_count} rows (> cap {cap}); pass --verify-deep to force strict checks",
                        cap = self.row_cap
                    ),
                }
            } else if !has_id {
                TableOutcome::Skipped {
                    reason: "no `id` column for key-set verdict".to_string(),
                }
            } else if let Some(scope) = incremental_scope.as_ref() {
                let s = self.source.key_stats_scoped(table, "id", scope).await?;
                let d = delta_keystats.clone().unwrap_or(
                    self.delta
                        .latest_key_stats(table, "id", &scope.cursor_col)
                        .await?,
                );
                delta_keystats = Some(d.clone());
                Self::key_stats_outcome("source_scoped", "delta_latest", &s, &d)
            } else {
                let s = self.source.key_stats(table, "id").await?;
                let d = self.delta.key_stats(table, "id").await?;
                delta_keystats = Some(d.clone());
                if matches!(&plan.mode, VerifyMode::TwoStream { .. }) {
                    Self::two_stream_key_stats_outcome("source", delta_label, &s, &d)
                } else {
                    Self::key_stats_outcome("source", delta_label, &s, &d)
                }
            };

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

            if matches!(&outcome, TableOutcome::Pass) {
                if let Some(reason) = skip_pass_layers_reason {
                    println!("verify {table} non-null census: skipped ({reason})");
                    println!("verify {table} sample: skipped ({reason})");
                } else {
                    let delta_appendlog =
                        matches!(&delta_keystats, Some(d) if d.count != d.distinct);
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
                                    println!(
                                        "verify {table} sample row {id}: differing columns {cols:?}"
                                    );
                                }
                            }
                        }
                    }
                }
            }

            outcomes.push(outcome);
        }

        let pass_count = outcomes
            .iter()
            .filter(|o| **o == TableOutcome::Pass)
            .count();
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

        if outcomes
            .iter()
            .any(|o| matches!(o, TableOutcome::Discrepancy { .. }))
        {
            Ok(VerifyVerdict::Discrepancy)
        } else {
            Ok(VerifyVerdict::Clean)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_plan_describe_includes_mode_and_hwms() {
        let incremental = TablePlan {
            table: "orders".to_string(),
            mode: VerifyMode::Incremental {
                cursor_col: "updated_at".to_string(),
                hwm: Some(Hwm {
                    updated_at: "2026-06-30 12:00:00".to_string(),
                    last_id: 42,
                }),
            },
        };
        assert_eq!(
            incremental.describe(),
            "mode=incremental cursor=updated_at hwm=updated_at=2026-06-30 12:00:00 last_id=42"
        );

        let two_stream = TablePlan {
            table: "users".to_string(),
            mode: VerifyMode::TwoStream {
                insert_cursor: "id".to_string(),
                update_cursor: "updated_at".to_string(),
                update_hwm: None,
                insert_hwm: Some(99),
            },
        };
        assert_eq!(
            two_stream.describe(),
            "mode=two_stream insert_cursor=id update_cursor=updated_at update_hwm=none insert_hwm=99"
        );
    }

    #[test]
    fn verify_command_new_defaults_table_plans_to_basic() {
        let source = MockSourceProbe::new();
        let delta = MockDeltaProbe::new();
        let cmd = VerifyCommand::new(source, delta, vec!["orders".to_string()]);
        assert_eq!(cmd.table_plans.len(), 1);
        let plan = cmd.table_plans.get("orders").unwrap();
        assert_eq!(plan.table, "orders");
        assert!(matches!(plan.mode, VerifyMode::Basic));
    }

    #[test]
    fn verify_command_matches_table_plans_by_name() {
        let source = MockSourceProbe::new();
        let delta = MockDeltaProbe::new();
        let cmd = VerifyCommand::new(
            source,
            delta,
            vec!["orders".to_string(), "users".to_string()],
        )
        .with_table_plans(vec![
            TablePlan {
                table: "users".to_string(),
                mode: VerifyMode::FullRefresh,
            },
            TablePlan::basic("orders"),
        ]);

        assert!(matches!(
            cmd.table_plans.get("users").unwrap().mode,
            VerifyMode::FullRefresh
        ));
        assert!(matches!(
            cmd.table_plans.get("orders").unwrap().mode,
            VerifyMode::Basic
        ));
    }

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
        source
            .expect_non_null_counts()
            .returning(|_, cols: &[String]| Ok(vec![0i64; cols.len()]));
        delta
            .expect_non_null_counts()
            .returning(|_, cols: &[String]| Ok(vec![0i64; cols.len()]));
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
        source
            .expect_non_null_counts()
            .returning(|_, cols: &[String]| Ok(vec![0i64; cols.len()]));
        delta
            .expect_non_null_counts()
            .returning(|_, cols: &[String]| Ok(vec![0i64; cols.len()]));
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
        source
            .expect_non_null_counts()
            .returning(|_, cols: &[String]| Ok(vec![0i64; cols.len()]));
        delta
            .expect_non_null_counts()
            .returning(|_, cols: &[String]| Ok(vec![0i64; cols.len()]));
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
    async fn verify_two_stream_exact_key_stats_pass() {
        let mut source = MockSourceProbe::new();
        let mut delta = MockDeltaProbe::new();
        source.expect_row_count().returning(|_| Ok(4));
        delta.expect_row_count().returning(|_| Ok(4));
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
                count: 4,
                distinct: 4,
                min: Some(10),
                max: Some(13),
                xor: 3,
                distinct_xor: 3,
            })
        };
        source.expect_key_stats().returning(move |_, _| stats());
        delta.expect_key_stats().returning(move |_, _| stats());
        source
            .expect_non_null_counts()
            .returning(|_, cols: &[String]| Ok(vec![0i64; cols.len()]));
        delta
            .expect_non_null_counts()
            .returning(|_, cols: &[String]| Ok(vec![0i64; cols.len()]));
        source.expect_sample_ids().returning(|_, _, _| Ok(vec![]));
        let cmd =
            VerifyCommand::new(source, delta, vec!["users".to_string()]).with_table_plans(vec![
                TablePlan {
                    table: "users".to_string(),
                    mode: VerifyMode::TwoStream {
                        insert_cursor: "id".to_string(),
                        update_cursor: "updated_at".to_string(),
                        update_hwm: None,
                        insert_hwm: Some(13),
                    },
                },
            ]);
        let result = cmd.run().await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), VerifyVerdict::Clean);
    }

    #[tokio::test]
    async fn verify_two_stream_delta_superset_is_non_failing_drift() {
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
                min: Some(2),
                max: Some(4),
                xor: 4,
                distinct_xor: 4,
            })
        });
        delta.expect_key_stats().returning(|_, _| {
            Ok(KeyStats {
                count: 5,
                distinct: 5,
                min: Some(1),
                max: Some(6),
                xor: 1,
                distinct_xor: 1,
            })
        });
        let cmd =
            VerifyCommand::new(source, delta, vec!["users".to_string()]).with_table_plans(vec![
                TablePlan {
                    table: "users".to_string(),
                    mode: VerifyMode::TwoStream {
                        insert_cursor: "id".to_string(),
                        update_cursor: "updated_at".to_string(),
                        update_hwm: None,
                        insert_hwm: Some(6),
                    },
                },
            ]);
        let result = cmd.run().await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), VerifyVerdict::Clean);
    }

    #[tokio::test]
    async fn verify_two_stream_equal_size_key_mismatch_is_discrepancy() {
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
        source.expect_key_stats().returning(|_, _| {
            Ok(KeyStats {
                count: 3,
                distinct: 3,
                min: Some(2),
                max: Some(4),
                xor: 5,
                distinct_xor: 5,
            })
        });
        delta.expect_key_stats().returning(|_, _| {
            Ok(KeyStats {
                count: 3,
                distinct: 3,
                min: Some(1),
                max: Some(5),
                xor: 7,
                distinct_xor: 7,
            })
        });
        let cmd =
            VerifyCommand::new(source, delta, vec!["users".to_string()]).with_table_plans(vec![
                TablePlan {
                    table: "users".to_string(),
                    mode: VerifyMode::TwoStream {
                        insert_cursor: "id".to_string(),
                        update_cursor: "updated_at".to_string(),
                        update_hwm: None,
                        insert_hwm: Some(5),
                    },
                },
            ]);
        let result = cmd.run().await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), VerifyVerdict::Discrepancy);
    }

    #[tokio::test]
    async fn verify_two_stream_source_missing_from_delta_is_discrepancy() {
        let mut source = MockSourceProbe::new();
        let mut delta = MockDeltaProbe::new();
        source.expect_row_count().returning(|_| Ok(5));
        delta.expect_row_count().returning(|_| Ok(4));
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
                max: Some(7),
                xor: 2,
                distinct_xor: 2,
            })
        });
        delta.expect_key_stats().returning(|_, _| {
            Ok(KeyStats {
                count: 4,
                distinct: 4,
                min: Some(1),
                max: Some(6),
                xor: 1,
                distinct_xor: 1,
            })
        });
        let cmd =
            VerifyCommand::new(source, delta, vec!["users".to_string()]).with_table_plans(vec![
                TablePlan {
                    table: "users".to_string(),
                    mode: VerifyMode::TwoStream {
                        insert_cursor: "id".to_string(),
                        update_cursor: "updated_at".to_string(),
                        update_hwm: None,
                        insert_hwm: Some(6),
                    },
                },
            ]);
        let result = cmd.run().await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), VerifyVerdict::Discrepancy);
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
        source
            .expect_non_null_counts()
            .returning(|_, _| Ok(vec![10, 8]));
        delta
            .expect_non_null_counts()
            .returning(|_, _| Ok(vec![10, 8]));
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
        source
            .expect_non_null_counts()
            .returning(|_, _| Ok(vec![10, 8]));
        delta
            .expect_non_null_counts()
            .returning(|_, _| Ok(vec![10, 5]));
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
        source
            .expect_non_null_counts()
            .returning(|_, _| Ok(vec![10, 8]));
        delta
            .expect_non_null_counts()
            .returning(|_, _| Ok(vec![10, 8]));
        source
            .expect_sample_ids()
            .returning(|_, _, _| Ok(vec![1, 2]));
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
        source
            .expect_non_null_counts()
            .returning(|_, _| Ok(vec![10, 8]));
        delta
            .expect_non_null_counts()
            .returning(|_, _| Ok(vec![10, 8]));
        source
            .expect_sample_ids()
            .returning(|_, _, _| Ok(vec![1, 2]));
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

    #[tokio::test]
    async fn verify_incremental_hwm_scoped_latest_pass() {
        let mut source = MockSourceProbe::new();
        let mut delta = MockDeltaProbe::new();
        let cols = || {
            Ok(vec![
                ColumnMeta {
                    name: "id".to_string(),
                    type_str: "bigint".to_string(),
                    nullable: false,
                },
                ColumnMeta {
                    name: "updated_at".to_string(),
                    type_str: "timestamp".to_string(),
                    nullable: false,
                },
            ])
        };
        source.expect_columns().returning(move |_| cols());
        delta.expect_columns().returning(move |_| cols());
        source.expect_row_count_scoped().returning(|_, scope| {
            assert_eq!(scope.cursor_col, "updated_at");
            assert_eq!(scope.updated_at, "2026-06-30 12:00:00");
            assert_eq!(scope.last_id, 42);
            Ok(2)
        });
        source
            .expect_key_stats_scoped()
            .returning(|_, key_col, scope| {
                assert_eq!(key_col, "id");
                assert_eq!(scope.cursor_col, "updated_at");
                Ok(KeyStats {
                    count: 2,
                    distinct: 2,
                    min: Some(43),
                    max: Some(44),
                    xor: 7,
                    distinct_xor: 7,
                })
            });
        delta
            .expect_latest_key_stats()
            .returning(|_, key_col, cursor_col| {
                assert_eq!(key_col, "id");
                assert_eq!(cursor_col, "updated_at");
                Ok(KeyStats {
                    count: 2,
                    distinct: 2,
                    min: Some(43),
                    max: Some(44),
                    xor: 7,
                    distinct_xor: 7,
                })
            });
        let cmd =
            VerifyCommand::new(source, delta, vec!["orders".to_string()]).with_table_plans(vec![
                TablePlan {
                    table: "orders".to_string(),
                    mode: VerifyMode::Incremental {
                        cursor_col: "updated_at".to_string(),
                        hwm: Some(Hwm {
                            updated_at: "2026-06-30 12:00:00".to_string(),
                            last_id: 42,
                        }),
                    },
                },
            ]);
        let result = cmd.run().await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), VerifyVerdict::Clean);
    }

    #[tokio::test]
    async fn verify_incremental_hwm_scoped_latest_discrepancy() {
        let mut source = MockSourceProbe::new();
        let mut delta = MockDeltaProbe::new();
        let cols = || {
            Ok(vec![
                ColumnMeta {
                    name: "id".to_string(),
                    type_str: "bigint".to_string(),
                    nullable: false,
                },
                ColumnMeta {
                    name: "updated_at".to_string(),
                    type_str: "timestamp".to_string(),
                    nullable: false,
                },
            ])
        };
        source.expect_columns().returning(move |_| cols());
        delta.expect_columns().returning(move |_| cols());
        source.expect_row_count_scoped().returning(|_, _| Ok(2));
        source.expect_key_stats_scoped().returning(|_, _, _| {
            Ok(KeyStats {
                count: 2,
                distinct: 2,
                min: Some(43),
                max: Some(44),
                xor: 7,
                distinct_xor: 7,
            })
        });
        delta.expect_latest_key_stats().returning(|_, _, _| {
            Ok(KeyStats {
                count: 2,
                distinct: 2,
                min: Some(43),
                max: Some(99),
                xor: 12,
                distinct_xor: 12,
            })
        });
        let cmd =
            VerifyCommand::new(source, delta, vec!["orders".to_string()]).with_table_plans(vec![
                TablePlan {
                    table: "orders".to_string(),
                    mode: VerifyMode::Incremental {
                        cursor_col: "updated_at".to_string(),
                        hwm: Some(Hwm {
                            updated_at: "2026-06-30 12:00:00".to_string(),
                            last_id: 42,
                        }),
                    },
                },
            ]);
        let result = cmd.run().await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), VerifyVerdict::Discrepancy);
    }

    #[tokio::test]
    async fn verify_incremental_hwm_without_id_skips_key_set() {
        let mut source = MockSourceProbe::new();
        let mut delta = MockDeltaProbe::new();
        let cols = || {
            Ok(vec![ColumnMeta {
                name: "updated_at".to_string(),
                type_str: "timestamp".to_string(),
                nullable: false,
            }])
        };
        source.expect_columns().returning(move |_| cols());
        delta.expect_columns().returning(move |_| cols());
        source.expect_row_count().returning(|_| Ok(2));
        delta.expect_row_count().returning(|_| Ok(2));
        let cmd =
            VerifyCommand::new(source, delta, vec!["orders".to_string()]).with_table_plans(vec![
                TablePlan {
                    table: "orders".to_string(),
                    mode: VerifyMode::Incremental {
                        cursor_col: "updated_at".to_string(),
                        hwm: Some(Hwm {
                            updated_at: "2026-06-30 12:00:00".to_string(),
                            last_id: 42,
                        }),
                    },
                },
            ]);
        let result = cmd.run().await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), VerifyVerdict::Clean);
    }

    #[tokio::test]
    async fn verify_incremental_without_hwm_uses_raw_path() {
        let mut source = MockSourceProbe::new();
        let mut delta = MockDeltaProbe::new();
        source.expect_row_count().returning(|_| Ok(5));
        delta.expect_row_count().returning(|_| Ok(5));
        let cols = || {
            Ok(vec![
                ColumnMeta {
                    name: "id".to_string(),
                    type_str: "bigint".to_string(),
                    nullable: false,
                },
                ColumnMeta {
                    name: "updated_at".to_string(),
                    type_str: "timestamp".to_string(),
                    nullable: false,
                },
            ])
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
                max: Some(5),
                xor: 7,
                distinct_xor: 7,
            })
        });
        source
            .expect_non_null_counts()
            .returning(|_, cols: &[String]| Ok(vec![0i64; cols.len()]));
        delta
            .expect_non_null_counts()
            .returning(|_, cols: &[String]| Ok(vec![0i64; cols.len()]));
        source.expect_sample_ids().returning(|_, _, _| Ok(vec![]));
        let cmd =
            VerifyCommand::new(source, delta, vec!["orders".to_string()]).with_table_plans(vec![
                TablePlan {
                    table: "orders".to_string(),
                    mode: VerifyMode::Incremental {
                        cursor_col: "updated_at".to_string(),
                        hwm: None,
                    },
                },
            ]);
        let result = cmd.run().await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), VerifyVerdict::Clean);
    }
}
