use anyhow::Result;
use crate::discovery::{ColumnDescribe, IndexInfo};

#[derive(Debug, Clone, PartialEq)]
pub enum Verdict {
    Ideal,
    Ok,
    UsableButSlow,
    Unsafe,
}

#[derive(Debug, Clone)]
pub struct CandidateVerdict {
    pub column: String,
    pub nullable: bool,
    pub indexed: bool,
    pub leading: bool,
    pub verdict: Verdict,
}

#[derive(Debug, Clone)]
pub struct CursorReport {
    pub has_id: bool,
    pub id_type: Option<String>,
    pub candidates: Vec<CandidateVerdict>,
    pub configured: Option<String>,
    pub recommendation: String,
}

/// Pure function: evaluate cursor suitability for a table.
pub fn evaluate_cursor(
    table: &str,
    columns: &[ColumnDescribe],
    indexes: &[IndexInfo],
    configured_ts: Option<&str>,
) -> CursorReport {
    // Check for id column
    let id_col = columns.iter().find(|c| c.name == "id");
    let has_id = id_col.is_some();
    let id_type = id_col.map(|c| c.data_type.clone());

    // Find timestamp candidates (datetime/timestamp columns)
    let candidates_raw: Vec<&ColumnDescribe> = columns
        .iter()
        .filter(|c| {
            c.data_type == "timestamp" || c.data_type == "datetime"
        })
        .collect();

    // For each candidate, compute indexed and leading status
    let mut candidates = Vec::new();
    for col in candidates_raw {
        let indexed = indexes.iter().any(|idx| idx.columns.contains(&col.name));
        let leading = indexes
            .iter()
            .any(|idx| idx.columns.first().map(|c| c == &col.name).unwrap_or(false));

        let verdict = if col.nullable {
            Verdict::Unsafe
        } else if leading {
            Verdict::Ideal
        } else if indexed {
            Verdict::Ok
        } else {
            Verdict::UsableButSlow
        };

        candidates.push(CandidateVerdict {
            column: col.name.clone(),
            nullable: col.nullable,
            indexed,
            leading,
            verdict,
        });
    }

    // Build recommendation
    let recommendation = if !has_id {
        "No `id` column — use full_refresh.".to_string()
    } else {
        // Find best non-Unsafe candidate by rank Ideal > Ok > UsableButSlow
        let best_safe = candidates
            .iter()
            .filter(|c| c.verdict != Verdict::Unsafe)
            .min_by_key(|c| match c.verdict {
                Verdict::Ideal => 0,
                Verdict::Ok => 1,
                Verdict::UsableButSlow => 2,
                Verdict::Unsafe => 3,
            });

        let recommendation_base = if let Some(best) = best_safe {
            let reason = match best.verdict {
                Verdict::Ideal => "ideal candidate: NOT NULL + leading index".to_string(),
                Verdict::Ok => {
                    "acceptable candidate: NOT NULL but not leading index".to_string()
                }
                Verdict::UsableButSlow => {
                    "usable but slow: NOT NULL but no index (filesort on ORDER BY)".to_string()
                }
                Verdict::Unsafe => unreachable!(),
            };
            format!(
                "Incremental viable: set TABLE_TIMESTAMP_{table}={col} ({reason}).",
                col = best.column
            )
        } else {
            "No safe cursor column — use full_refresh. (An id-only cursor for append-only tables is not yet supported.)".to_string()
        };

        // Check if configured_ts exists and is problematic
        let configured_note = if let Some(configured) = configured_ts {
            if let Some(configured_candidate) = candidates.iter().find(|c| c.column == configured) {
                if configured_candidate.verdict == Verdict::Unsafe {
                    if let Some(better) = best_safe {
                        if better.verdict != Verdict::Unsafe {
                            format!(
                                " Warning: TABLE_TIMESTAMP_{table}={configured} is unsafe (nullable); consider {better} instead.",
                                better = better.column
                            )
                        } else {
                            String::new()
                        }
                    } else {
                        String::new()
                    }
                } else {
                    String::new()
                }
            } else {
                String::new()
            }
        } else {
            String::new()
        };

        if recommendation_base.contains("use full_refresh") {
            recommendation_base
        } else {
            format!("{}{}", recommendation_base, configured_note)
        }
    };

    CursorReport {
        has_id,
        id_type,
        candidates,
        configured: configured_ts.map(|s| s.to_string()),
        recommendation,
    }
}

#[cfg_attr(test, mockall::automock)]
#[allow(async_fn_in_trait)]
pub trait InspectIntrospect: Send + Sync {
    async fn describe_columns(&self, table: &str) -> Result<Vec<ColumnDescribe>>;
    async fn discover_indexes(&self, table: &str) -> Result<Vec<IndexInfo>>;
    async fn get_avg_row_length(&self, table: &str) -> Result<Option<u64>>;
}

pub struct InspectIntrospectAdapter {
    pool: sqlx::MySqlPool,
    database: String,
}

impl InspectIntrospectAdapter {
    pub fn new(pool: sqlx::MySqlPool, database: String) -> Self {
        Self { pool, database }
    }
}

impl InspectIntrospect for InspectIntrospectAdapter {
    async fn describe_columns(&self, table: &str) -> Result<Vec<ColumnDescribe>> {
        crate::discovery::SchemaInspector::new(self.pool.clone(), self.database.clone())
            .describe_columns(table)
            .await
    }

    async fn discover_indexes(&self, table: &str) -> Result<Vec<IndexInfo>> {
        crate::discovery::SchemaInspector::new(self.pool.clone(), self.database.clone())
            .discover_indexes(table)
            .await
    }

    async fn get_avg_row_length(&self, table: &str) -> Result<Option<u64>> {
        crate::discovery::SchemaInspector::new(self.pool.clone(), self.database.clone())
            .get_avg_row_length(table)
            .await
    }
}

pub struct InspectCommand<I> {
    introspect: I,
    table: String,
    configured_ts: Option<String>,
}

impl<I: InspectIntrospect> InspectCommand<I> {
    pub fn new(introspect: I, table: String, configured_ts: Option<String>) -> Self {
        Self {
            introspect,
            table,
            configured_ts,
        }
    }

    pub async fn run(&self) -> Result<()> {
        let columns = self.introspect.describe_columns(&self.table).await?;
        let indexes = self.introspect.discover_indexes(&self.table).await?;
        let avg_row_length = self.introspect.get_avg_row_length(&self.table).await?;

        // Print header
        println!("Table: {}   (avg_row_length: {} bytes)\n",
            self.table,
            avg_row_length.map(|v| v.to_string()).unwrap_or_else(|| "unknown".to_string())
        );

        // Print columns
        println!("Columns ({}):", columns.len());
        println!("  {:<20} {:<15} {:<8} {:<10}", "NAME", "TYPE", "NULL", "KEY");
        for col in &columns {
            let null_str = if col.nullable { "YES" } else { "NO" };
            println!(
                "  {:<20} {:<15} {:<8} {:<10}",
                col.name, col.data_type, null_str, col.key
            );
        }

        // Print indexes
        println!("\nIndexes:");
        if indexes.is_empty() {
            println!("  (none)");
        } else {
            for idx in &indexes {
                let unique_str = if idx.unique { "unique" } else { "non-unique" };
                let cols = idx.columns.join(", ");
                println!("  {:<20} {:<12} ({})", idx.name, unique_str, cols);
            }
        }

        // Print cursor evaluation
        println!("\nCursor evaluation:");
        let report = evaluate_cursor(&self.table, &columns, &indexes, self.configured_ts.as_deref());

        if report.has_id {
            let id_type_str = report.id_type.as_deref().unwrap_or("unknown");
            println!("  id column:        present  ({}, PRIMARY)            ✓", id_type_str);
        } else {
            println!("  id column:        NOT PRESENT                      ✗");
        }

        println!("  Timestamp candidates (datetime/timestamp columns):");
        if report.candidates.is_empty() {
            println!("    (none)");
        } else {
            for candidate in &report.candidates {
                let null_label = if candidate.nullable { "NULLABLE" } else { "NOT NULL" };
                let index_label = if candidate.leading {
                    "indexed (leading)"
                } else if candidate.indexed {
                    "indexed (non-leading)"
                } else {
                    "not indexed"
                };
                let verdict_str = match candidate.verdict {
                    Verdict::Ideal => "→ IDEAL",
                    Verdict::Ok => "→ OK",
                    Verdict::UsableButSlow => "→ USABLE BUT SLOW",
                    Verdict::Unsafe => "→ UNSAFE (nullable → NULL rows skipped + filesort)",
                };
                let status = format!("{null_label}, {index_label}");
                println!("    {:<20} {:<26} {}", candidate.column, status, verdict_str);
            }
        }

        if let Some(configured) = &report.configured {
            let configured_candidate = report
                .candidates
                .iter()
                .find(|c| &c.column == configured);
            if let Some(cc) = configured_candidate {
                let verdict_display = match cc.verdict {
                    Verdict::Ideal => "✓ IDEAL",
                    Verdict::Ok => "✓ OK",
                    Verdict::UsableButSlow => "⚠ USABLE BUT SLOW",
                    Verdict::Unsafe => "✗ UNSAFE (nullable)",
                };
                println!("  Configured cursor (TABLE_TIMESTAMP_{}): {} {}", self.table, configured, verdict_display);
            }
        }

        println!("\n  Recommendation: {}\n", report.recommendation);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn col(
        name: &str,
        data_type: &str,
        column_type: &str,
        nullable: bool,
        key: &str,
    ) -> ColumnDescribe {
        ColumnDescribe {
            name: name.to_string(),
            data_type: data_type.to_string(),
            column_type: column_type.to_string(),
            nullable,
            key: key.to_string(),
        }
    }

    fn idx(name: &str, unique: bool, columns: &[&str]) -> IndexInfo {
        IndexInfo {
            name: name.to_string(),
            unique,
            columns: columns.iter().map(|s| s.to_string()).collect(),
        }
    }

    #[test]
    fn ideal_candidate() {
        let columns = &[
            col("id", "bigint", "bigint(20)", false, "PRI"),
            col("updated_at", "timestamp", "timestamp", false, "MUL"),
            col("name", "varchar", "varchar(255)", false, ""),
        ];
        let indexes = &[
            idx("PRIMARY", true, &["id"]),
            idx("idx_updated_at", false, &["updated_at"]),
        ];
        let report = evaluate_cursor("orders", columns, indexes, None);
        assert!(report.has_id);
        assert_eq!(report.candidates.len(), 1);
        assert_eq!(report.candidates[0].column, "updated_at");
        assert_eq!(report.candidates[0].verdict, Verdict::Ideal);
        assert!(report.recommendation.contains("TABLE_TIMESTAMP"));
        assert!(report.recommendation.contains("updated_at"));
    }

    #[test]
    fn nullable_unsafe() {
        let columns = &[
            col("id", "bigint", "bigint(20)", false, "PRI"),
            col("completed_at", "datetime", "datetime", true, ""),
        ];
        let indexes = &[idx("PRIMARY", true, &["id"])];
        let report = evaluate_cursor("orders", columns, indexes, None);
        assert!(report.has_id);
        assert_eq!(report.candidates.len(), 1);
        assert_eq!(report.candidates[0].verdict, Verdict::Unsafe);
        assert!(report.recommendation.contains("full_refresh"));
    }

    #[test]
    fn unindexed_slow() {
        let columns = &[
            col("id", "bigint", "bigint(20)", false, "PRI"),
            col("created_at", "timestamp", "timestamp", false, ""),
        ];
        let indexes = &[idx("PRIMARY", true, &["id"])];
        let report = evaluate_cursor("orders", columns, indexes, None);
        assert!(report.has_id);
        assert_eq!(report.candidates.len(), 1);
        assert_eq!(report.candidates[0].verdict, Verdict::UsableButSlow);
        assert!(report.recommendation.contains("created_at"));
        assert!(report.recommendation.contains("filesort"));
    }

    #[test]
    fn no_id() {
        let columns = &[
            col("name", "varchar", "varchar(255)", false, ""),
            col("created_at", "timestamp", "timestamp", false, "MUL"),
        ];
        let indexes = &[idx("idx_created_at", false, &["created_at"])];
        let report = evaluate_cursor("orders", columns, indexes, None);
        assert!(!report.has_id);
        assert!(report.recommendation.contains("full_refresh"));
        assert!(report.recommendation.contains("id"));
    }

    #[test]
    fn configured_mismatch() {
        let columns = &[
            col("id", "bigint", "bigint(20)", false, "PRI"),
            col("completed_at", "datetime", "datetime", true, ""),
            col("created_at", "timestamp", "timestamp", false, "MUL"),
        ];
        let indexes = &[
            idx("PRIMARY", true, &["id"]),
            idx("idx_created_at", false, &["created_at"]),
        ];
        let report = evaluate_cursor("orders", columns, indexes, Some("completed_at"));
        assert!(report.has_id);
        assert_eq!(report.candidates.len(), 2);
        let unsafe_cand = report.candidates.iter().find(|c| c.column == "completed_at").unwrap();
        assert_eq!(unsafe_cand.verdict, Verdict::Unsafe);
        let safe_cand = report.candidates.iter().find(|c| c.column == "created_at").unwrap();
        assert_eq!(safe_cand.verdict, Verdict::Ideal); // Leading index = Ideal
        assert!(report.recommendation.contains("unsafe"));
        assert!(report.recommendation.contains("created_at"));
    }

    #[test]
    fn no_candidates() {
        let columns = &[
            col("id", "bigint", "bigint(20)", false, "PRI"),
            col("name", "varchar", "varchar(255)", false, ""),
        ];
        let indexes = &[idx("PRIMARY", true, &["id"])];
        let report = evaluate_cursor("orders", columns, indexes, None);
        assert!(report.has_id);
        assert!(report.candidates.is_empty());
        assert!(report.recommendation.contains("full_refresh"));
    }

    #[test]
    fn non_leading_index_ok() {
        let columns = &[
            col("id", "bigint", "bigint(20)", false, "PRI"),
            col("created_at", "timestamp", "timestamp", false, "MUL"),
            col("status", "varchar", "varchar(50)", false, ""),
        ];
        let indexes = &[
            idx("PRIMARY", true, &["id"]),
            idx("idx_status_created", false, &["status", "created_at"]),
        ];
        let report = evaluate_cursor("orders", columns, indexes, None);
        let created_cand = report.candidates.iter().find(|c| c.column == "created_at").unwrap();
        assert!(created_cand.indexed);
        assert!(!created_cand.leading);
        assert_eq!(created_cand.verdict, Verdict::Ok);
    }

    #[test]
    fn id_type_populated() {
        let columns = &[
            col("id", "bigint", "bigint(20)", false, "PRI"),
            col("updated_at", "timestamp", "timestamp", false, ""),
        ];
        let indexes = &[idx("PRIMARY", true, &["id"])];
        let report = evaluate_cursor("orders", columns, indexes, None);
        assert_eq!(report.id_type.as_deref(), Some("bigint"));
    }
}
