//! Strict per-unit verification (docs/design/archive-mode.md): every row of one unit of one archive
//! table is compared, source against Delta, by key and by a digest of the whole row.
//!
//! Both engines render each row canonically and hash it with MD5: integers as decimal text, text
//! types as stored, `TIMESTAMP`/`DATETIME` as microseconds since the epoch (the source session is
//! UTC), binary as lowercase hex, and floats as text. Every value is length-prefixed and NULL has
//! its own marker, so neither a separator nor a NULL can be confused with data. Column aggregates
//! are not enough here: an equal-length text edit, or a timestamp moved between the minimum and the
//! maximum, leaves sums and extremes unchanged — and this check is the go-ahead for deleting the
//! source rows.

use std::collections::HashMap;

use anyhow::{Context, Result, bail};
use deltalake::datafusion::prelude::SessionContext;
use futures::TryStreamExt;
use sqlx::{Executor, MySqlPool, Row};

use crate::discovery::{ColumnInfo, filter_unsupported_columns};
use crate::orchestrator::SchemaInspect;
use crate::query::backtick;
use crate::verify::DeltaProbeAdapter;
use crate::writer::{ARCHIVE_UNIT_COLUMN, DeltaWriter};

use super::{ARCHIVE_SESSION_INIT, ArchiveTable, bind_unit};

/// How many keys of each kind of difference a report names.
const KEYS_LISTED: usize = 5;

/// How a column is rendered for the row digest — the same text on both engines.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Render {
    /// Decimal text of an integer.
    Int,
    /// The stored text (also `decimal`, `date`, `json`, `enum`, `set`, which Delta keeps as text).
    Text,
    /// Microseconds since the epoch (`TIMESTAMP` read in UTC; `DATETIME` as its wall clock).
    Micros,
    /// Lowercase hex.
    Binary,
    /// Text of a floating-point value: both engines print the shortest round-trip form, but
    /// DataFusion writes a whole number as `1.0` where MariaDB writes `1`, so the trailing `.0` is
    /// dropped. Any other formatting difference (exponent notation for very large or very small
    /// values) fails safe: it shows as a difference, never as a false match.
    Float,
}

fn render_of(data_type: &str) -> Result<Render> {
    Ok(match data_type {
        "tinyint" | "smallint" | "mediumint" | "int" | "integer" | "bigint" | "boolean" | "bool" => Render::Int,
        "varchar" | "char" | "text" | "tinytext" | "mediumtext" | "longtext" | "json" | "enum" | "set" | "date"
        | "decimal" | "numeric" => Render::Text,
        "timestamp" | "datetime" => Render::Micros,
        "blob" | "tinyblob" | "mediumblob" | "longblob" | "binary" | "varbinary" => Render::Binary,
        "float" | "double" => Render::Float,
        other => bail!("no canonical rendering for column type `{other}`"),
    })
}

/// One column as it enters the digest.
#[derive(Debug, Clone, PartialEq)]
pub struct DigestColumn {
    pub name: String,
    pub render: Render,
}

/// A field's canonical text, length-prefixed, or `N` for NULL — MariaDB SQL.
fn source_field(c: &DigestColumn) -> String {
    let col = backtick(&c.name);
    let text = match c.render {
        Render::Int | Render::Float => format!("CAST({col} AS CHAR)"),
        Render::Text => col.clone(),
        Render::Micros => format!("CAST(TIMESTAMPDIFF(MICROSECOND, '1970-01-01 00:00:00', {col}) AS CHAR)"),
        Render::Binary => format!("LOWER(HEX({col}))"),
    };
    format!("CASE WHEN {col} IS NULL THEN 'N' ELSE CONCAT(CHAR_LENGTH({text}), ':', {text}) END")
}

/// The same field — DataFusion SQL over the archive table.
fn delta_field(c: &DigestColumn) -> String {
    let col = format!("`{}`", c.name);
    let text = match c.render {
        Render::Int => format!("cast({col} as varchar)"),
        Render::Float => format!("regexp_replace(cast({col} as varchar), '\\.0$', '')"),
        Render::Text => col.clone(),
        Render::Micros => format!("cast(cast({col} as bigint) as varchar)"),
        Render::Binary => format!("lower(encode({col}, 'hex'))"),
    };
    format!("CASE WHEN {col} IS NULL THEN 'N' ELSE concat(cast(char_length({text}) as varchar), ':', {text}) END")
}

/// Join fields with `|`; the length prefixes make the result unambiguous.
fn source_row(cols: &[DigestColumn]) -> String {
    let parts: Vec<String> = cols.iter().map(source_field).collect();
    format!("CONCAT({})", parts.join(", '|', "))
}

fn delta_row(cols: &[DigestColumn]) -> String {
    let parts: Vec<String> = cols.iter().map(delta_field).collect();
    format!("concat({})", parts.join(", '|', "))
}

/// Turn an encoded key (`2:31|1:7`) back into a readable one (`31, 7`) for reports.
fn display_key(encoded: &str) -> String {
    let mut out = Vec::new();
    let mut rest = encoded;
    while !rest.is_empty() {
        if let Some(tail) = rest.strip_prefix('N') {
            out.push("NULL".to_string());
            rest = tail.strip_prefix('|').unwrap_or(tail);
            continue;
        }
        let Some((len, tail)) = rest.split_once(':') else { return encoded.to_string() };
        let Ok(len) = len.parse::<usize>() else { return encoded.to_string() };
        let value: String = tail.chars().take(len).collect();
        rest = &tail[value.len()..];
        rest = rest.strip_prefix('|').unwrap_or(rest);
        out.push(value);
    }
    out.join(", ")
}

#[derive(Debug, Clone, PartialEq)]
pub enum UnitVerdict {
    /// Every source row of the unit is in Delta, byte for byte, and Delta holds nothing else.
    Verified { rows: u64 },
    /// They differ; each entry names a kind of difference and its first keys.
    Mismatch(Vec<String>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct UnitVerification {
    pub unit: i64,
    pub table: String,
    pub verdict: UnitVerdict,
}

/// Rows of one side that differ from the other, by kind.
#[derive(Debug, Default, PartialEq)]
struct Differences {
    missing: Vec<String>,
    extra: Vec<String>,
    changed: Vec<String>,
    duplicated: Vec<String>,
}

impl Differences {
    fn report(self) -> Vec<String> {
        let line = |what: &str, mut keys: Vec<String>| {
            keys.sort();
            let n = keys.len();
            let listed: Vec<String> = keys.iter().take(KEYS_LISTED).map(|k| format!("({})", display_key(k))).collect();
            let more = if n > KEYS_LISTED { format!(", … {} more", n - KEYS_LISTED) } else { String::new() };
            format!("{n} row(s) {what}: {}{more}", listed.join(", "))
        };
        let mut out = Vec::new();
        if !self.missing.is_empty() {
            out.push(line("missing from Delta", self.missing));
        }
        if !self.extra.is_empty() {
            out.push(line("in Delta but not in the source", self.extra));
        }
        if !self.changed.is_empty() {
            out.push(line("differ", self.changed));
        }
        if !self.duplicated.is_empty() {
            out.push(line("duplicated in Delta", self.duplicated));
        }
        out
    }
}

/// Compare source rows (key, digest) against Delta's, consuming `delta`.
fn compare<I: IntoIterator<Item = (String, String)>>(source: I, mut delta: HashMap<String, String>, duplicated: Vec<String>) -> (u64, Differences) {
    let mut diff = Differences { duplicated, ..Default::default() };
    let mut rows = 0;
    for (key, digest) in source {
        rows += 1;
        match delta.remove(&key) {
            None => diff.missing.push(key),
            Some(d) if d != digest => diff.changed.push(key),
            Some(_) => {}
        }
    }
    diff.extra.extend(delta.into_keys());
    (rows, diff)
}

/// The source side: a pool whose connections run [`ARCHIVE_SESSION_INIT`], so `TIMESTAMP`
/// values render as the same UTC instants the archive stores.
pub struct SourceUnitProbe {
    pool: MySqlPool,
}

impl SourceUnitProbe {
    pub async fn connect(database_url: &str) -> Result<Self> {
        let pool = sqlx::mysql::MySqlPoolOptions::new()
            .after_connect(|conn, _| {
                Box::pin(async move {
                    for stmt in ARCHIVE_SESSION_INIT {
                        conn.execute(stmt).await?;
                    }
                    Ok(())
                })
            })
            .connect(database_url)
            .await
            .context("connecting the archive verification source")?;
        Ok(Self { pool })
    }

    fn sql(table: &str, predicate: &str, key: &[DigestColumn], cols: &[DigestColumn]) -> String {
        format!(
            "SELECT CONVERT({} USING utf8mb4), MD5(CONVERT({} USING utf8mb4)) FROM {} WHERE ({predicate})",
            source_row(key),
            source_row(cols),
            backtick(table)
        )
    }
}

/// The Delta side: the archive table filtered to `_archive_unit = unit`.
pub struct DeltaUnitProbe {
    writer: DeltaWriter,
}

impl DeltaUnitProbe {
    pub fn new(writer: DeltaWriter) -> Self {
        Self { writer }
    }

    fn sql(unit: i64, key: &[DigestColumn], cols: &[DigestColumn]) -> String {
        format!(
            "SELECT {} AS k, md5({}) AS d FROM t WHERE `{ARCHIVE_UNIT_COLUMN}` = {unit}",
            delta_row(key),
            delta_row(cols)
        )
    }

    /// The unit's rows as key → digest, plus any key seen more than once.
    async fn digests(&self, table: &str, unit: i64, key: &[DigestColumn], cols: &[DigestColumn]) -> Result<(HashMap<String, String>, Vec<String>)> {
        let t = self.writer.open_table(table).await?;
        let ctx = SessionContext::new();
        ctx.register_table("t", t.table_provider().await?)?;
        let batches = ctx.sql(&Self::sql(unit, key, cols)).await?.collect().await?;
        let mut map = HashMap::new();
        let mut duplicated = Vec::new();
        for b in &batches {
            for row in 0..b.num_rows() {
                let k = DeltaProbeAdapter::str_at(b, row, 0).unwrap_or_default();
                let d = DeltaProbeAdapter::str_at(b, row, 1).unwrap_or_default();
                if map.insert(k.clone(), d).is_some() {
                    duplicated.push(k);
                }
            }
        }
        Ok((map, duplicated))
    }
}

/// The table's digest layout: its primary key and every extractable column.
fn layout(table: &str, columns: &[ColumnInfo], primary: Vec<String>) -> Result<(Vec<DigestColumn>, Vec<DigestColumn>)> {
    if primary.is_empty() {
        bail!("archive table `{table}` has no PRIMARY key to identify rows by");
    }
    let digest = |c: &ColumnInfo| -> Result<DigestColumn> {
        Ok(DigestColumn {
            name: c.name.clone(),
            render: render_of(&c.data_type).with_context(|| format!("archive table `{table}`, column `{}`", c.name))?,
        })
    };
    let key = primary
        .iter()
        .map(|k| {
            columns
                .iter()
                .find(|c| &c.name == k)
                .with_context(|| format!("archive table `{table}`: key column `{k}` is not extractable"))
                .and_then(digest)
        })
        .collect::<Result<Vec<_>>>()?;
    let cols = columns.iter().map(digest).collect::<Result<Vec<_>>>()?;
    Ok((key, cols))
}

/// Verify every unit in `units` in every table of `tables`, row by row.
pub async fn verify_units<S: SchemaInspect>(
    inspect: &S,
    source: &SourceUnitProbe,
    delta: &DeltaUnitProbe,
    tables: &[ArchiveTable],
    units: &[i64],
) -> Result<Vec<UnitVerification>> {
    let mut prepared = Vec::with_capacity(tables.len());
    for table in tables {
        let columns = filter_unsupported_columns(&inspect.discover_columns(&table.name).await?);
        let primary = inspect
            .discover_indexes(&table.name)
            .await?
            .into_iter()
            .find(|i| i.name == "PRIMARY")
            .map(|i| i.columns)
            .unwrap_or_default();
        let (key, cols) = layout(&table.name, &columns, primary)?;
        prepared.push((table, key, cols));
    }
    let mut results = Vec::with_capacity(units.len() * prepared.len());
    for &unit in units {
        for (table, key, cols) in &prepared {
            let context = || format!("verifying unit {unit} of `{}`", table.name);
            let (delta_rows, duplicated) = delta.digests(&table.name, unit, key, cols).await.with_context(context)?;
            let predicate = bind_unit(&table.scope, unit)?;
            let sql = SourceUnitProbe::sql(&table.name, &predicate, key, cols);
            let source_rows: Vec<(String, String)> = sqlx::query(&sql)
                .fetch(&source.pool)
                .map_ok(|row| (row.get::<String, _>(0), row.get::<String, _>(1)))
                .try_collect()
                .await
                .with_context(context)?;
            let (rows, diff) = compare(source_rows, delta_rows, duplicated);
            let report = diff.report();
            let verdict = if report.is_empty() { UnitVerdict::Verified { rows } } else { UnitVerdict::Mismatch(report) };
            results.push(UnitVerification { unit, table: table.name.clone(), verdict });
        }
    }
    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn col(name: &str, data_type: &str) -> ColumnInfo {
        ColumnInfo { name: name.into(), data_type: data_type.into(), column_type: data_type.into(), nullable: true }
    }

    fn digests(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs.iter().map(|(k, d)| (k.to_string(), d.to_string())).collect()
    }

    fn rows(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs.iter().map(|(k, d)| (k.to_string(), d.to_string())).collect()
    }

    #[test]
    fn identical_rows_verify() {
        let (n, diff) = compare(rows(&[("1:1", "a"), ("1:2", "b")]), digests(&[("1:1", "a"), ("1:2", "b")]), vec![]);
        assert_eq!(n, 2);
        assert_eq!(diff, Differences::default());
        assert!(diff.report().is_empty());
    }

    #[test]
    fn every_kind_of_difference_is_named_by_key() {
        let (_, diff) = compare(
            rows(&[("1:1", "a"), ("1:2", "b"), ("1:3", "c")]),
            digests(&[("1:1", "a"), ("1:2", "B"), ("1:9", "z")]),
            vec!["1:1".into()],
        );
        let report = diff.report().join("; ");
        assert!(report.contains("1 row(s) missing from Delta: (3)"), "got: {report}");
        assert!(report.contains("1 row(s) in Delta but not in the source: (9)"), "got: {report}");
        assert!(report.contains("1 row(s) differ: (2)"), "got: {report}");
        assert!(report.contains("1 row(s) duplicated in Delta: (1)"), "got: {report}");
    }

    #[test]
    fn long_reports_list_the_first_keys_only() {
        let source: Vec<(String, String)> = (0..8).map(|i| (format!("1:{i}"), "x".to_string())).collect();
        let report = Differences { missing: source.into_iter().map(|(k, _)| k).collect(), ..Default::default() }.report();
        assert!(report[0].starts_with("8 row(s) missing from Delta: (0), (1), (2), (3), (4), … 3 more"), "got: {report:?}");
    }

    #[test]
    fn keys_display_without_their_encoding() {
        assert_eq!(display_key("2:31|1:7"), "31, 7");
        assert_eq!(display_key("3:a|b|N"), "a|b, NULL", "a value holding the separator stays whole");
        assert_eq!(display_key("5:héllo"), "héllo", "lengths count characters, not bytes");
    }

    #[test]
    fn both_engines_render_the_same_fields() {
        let ts = DigestColumn { name: "sent_at".into(), render: Render::Micros };
        assert_eq!(
            source_field(&ts),
            "CASE WHEN `sent_at` IS NULL THEN 'N' ELSE CONCAT(CHAR_LENGTH(CAST(TIMESTAMPDIFF(MICROSECOND, \
             '1970-01-01 00:00:00', `sent_at`) AS CHAR)), ':', CAST(TIMESTAMPDIFF(MICROSECOND, \
             '1970-01-01 00:00:00', `sent_at`) AS CHAR)) END"
        );
        assert!(delta_field(&ts).contains("cast(cast(`sent_at` as bigint) as varchar)"));
        let float = DigestColumn { name: "score".into(), render: Render::Float };
        assert!(delta_field(&float).contains(r"regexp_replace(cast(`score` as varchar), '\.0$', '')"));
        let bin = DigestColumn { name: "blob".into(), render: Render::Binary };
        assert!(source_field(&bin).contains("LOWER(HEX(`blob`))"));
        assert!(delta_field(&bin).contains("lower(encode(`blob`, 'hex'))"));
    }

    #[test]
    fn layout_keys_rows_by_the_primary_key_and_digests_every_column() {
        let columns = vec![col("id", "int"), col("subject", "varchar"), col("sent_at", "timestamp")];
        let (key, cols) = layout("t", &columns, vec!["id".into()]).unwrap();
        assert_eq!(key, vec![DigestColumn { name: "id".into(), render: Render::Int }]);
        assert_eq!(cols.len(), 3);
        assert!(layout("t", &columns, vec![]).is_err(), "rows need a key to be matched");
        assert!(layout("t", &[col("geom", "geometry")], vec!["geom".into()]).is_err());
    }
}
