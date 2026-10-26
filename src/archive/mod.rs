//! Archive mode building blocks (docs/design/archive-mode.md): the archive table's schema, the
//! scoped key-paged extraction query, and the conversion of extracted rows to the archive schema.
//!
//! Archive extraction reads with the session pinned to UTC (`ARCHIVE_SESSION_INIT`), so MariaDB
//! returns every `TIMESTAMP` as UTC text; `to_archive_batch` turns that text into typed instants.
//! The source time zone is kept as metadata on each timestamp column (and as a table property),
//! because a Delta `timestamp` cannot carry a zone name itself.

mod verify;

pub use verify::{DeltaUnitProbe, SourceUnitProbe, UnitVerdict, UnitVerification, verify_units};

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use chrono::NaiveDateTime;
use deltalake::arrow::array::{
    Array, ArrayRef, Int32Array, Int64Array, StringArray, StringViewArray, TimestampMicrosecondArray,
    UInt32Array, UInt64Array,
};
use deltalake::arrow::compute::cast;
use deltalake::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use deltalake::arrow::record_batch::RecordBatch;

use crate::discovery::{ColumnInfo, filter_unsupported_columns};
use crate::orchestrator::{Extract, SchemaInspect};
use crate::query::backtick;
use crate::writer::{ARCHIVE_UNIT_COLUMN, DeltaWriter, with_archive_unit_column};

/// Field-metadata key (and, prefixed, table property) recording the source time zone.
pub const SOURCE_TIME_ZONE_KEY: &str = "source_time_zone";
/// Table property recording the source time zone on an archive table.
pub const SOURCE_TIME_ZONE_PROPERTY: &str = "parket.source_time_zone";

/// Statements every archive connection runs on connect (and reconnect):
/// - `time_zone = '+00:00'`, so MariaDB renders `TIMESTAMP` values in UTC and no archived value
///   depends on the server's `SYSTEM` zone;
/// - `NO_BACKSLASH_ESCAPES` removed from `sql_mode`, so the backslash escaping `text_literal` uses
///   for paging keys means the same thing on every server.
pub const ARCHIVE_SESSION_INIT: [&str; 2] = [
    "SET time_zone = '+00:00'",
    "SET SESSION sql_mode = REPLACE(@@SESSION.sql_mode, 'NO_BACKSLASH_ESCAPES', '')",
];

/// The archive table's Arrow schema for the extracted source columns, plus `_archive_unit`.
/// `TIMESTAMP` columns become UTC instants and `DATETIME` columns zone-less `timestamp_ntz`
/// values; both carry `source_time_zone` metadata. Every other type maps as in a sync.
pub fn archive_schema(columns: &[ColumnInfo], source_time_zone: &str) -> Result<SchemaRef> {
    let zone = HashMap::from([(SOURCE_TIME_ZONE_KEY.to_string(), source_time_zone.to_string())]);
    let fields = columns
        .iter()
        .map(|c| {
            Ok(match c.data_type.as_str() {
                "timestamp" => Field::new(&c.name, DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), true)
                    .with_metadata(zone.clone()),
                "datetime" => Field::new(&c.name, DataType::Timestamp(TimeUnit::Microsecond, None), true)
                    .with_metadata(zone.clone()),
                _ => Field::new(
                    &c.name,
                    crate::orchestrator::mariadb_type_to_arrow(&c.data_type, &c.column_type)?,
                    true,
                ),
            })
        })
        .collect::<Result<Vec<Field>>>()?;
    with_archive_unit_column(&Schema::new(fields))
}

/// One value of a paging key, as read from the last row of a page.
#[derive(Debug, Clone, PartialEq)]
pub enum KeyValue {
    Int(i64),
    Text(String),
}

impl KeyValue {
    fn literal(&self) -> String {
        match self {
            KeyValue::Int(v) => v.to_string(),
            KeyValue::Text(v) => text_literal(v),
        }
    }
}

/// A quoted MariaDB string literal. Escapes the backslash as well as the quote: with
/// `NO_BACKSLASH_ESCAPES` off (which `ARCHIVE_SESSION_INIT` guarantees), a trailing `\` would
/// otherwise escape the closing quote.
fn text_literal(v: &str) -> String {
    format!("'{}'", v.replace('\\', "\\\\").replace('\'', "''"))
}

/// Replace the scope predicate's single `?` with the unit id.
fn bind_unit(scope: &str, unit: i64) -> Result<String> {
    let placeholders = scope.matches('?').count();
    if placeholders != 1 {
        bail!("archive scope `{scope}` must contain exactly one `?` for the unit id, found {placeholders}");
    }
    Ok(scope.replacen('?', &unit.to_string(), 1))
}

/// One page of a unit's rows: `WHERE (<scope>) [AND (<key>) > (<after>)] ORDER BY <key> LIMIT n`.
/// The key is the table's primary key (all of its columns); `after` is the previous page's last key.
pub fn page_query(
    table: &str,
    columns: &[String],
    scope: &str,
    unit: i64,
    key: &[String],
    after: Option<&[KeyValue]>,
    limit: u64,
) -> Result<String> {
    if key.is_empty() {
        bail!("archive table `{table}` has no primary key to page by");
    }
    let scope = bind_unit(scope, unit)?;
    let cols = columns.iter().map(|c| backtick(c)).collect::<Vec<_>>().join(", ");
    let key_cols = key.iter().map(|c| backtick(c)).collect::<Vec<_>>().join(", ");
    let page = match after {
        None => String::new(),
        Some(values) if values.len() == key.len() => {
            let values = values.iter().map(KeyValue::literal).collect::<Vec<_>>().join(", ");
            format!(" AND ({key_cols}) > ({values})")
        }
        Some(values) => bail!("paging key has {} columns but {} values were given", key.len(), values.len()),
    };
    Ok(format!(
        "SELECT {cols} FROM {} WHERE ({scope}){page} ORDER BY {key_cols} LIMIT {limit}",
        backtick(table)
    ))
}

/// The key of the last row in `batch`, for the next page's `after`.
pub fn last_key(batch: &RecordBatch, key: &[String]) -> Result<Vec<KeyValue>> {
    let row = batch.num_rows().checked_sub(1).context("last_key of an empty page")?;
    key.iter()
        .map(|name| {
            let col = batch
                .column_by_name(name)
                .with_context(|| format!("paging key `{name}` is missing from the page"))?;
            if col.is_null(row) {
                bail!("paging key `{name}` is NULL in the last row of the page");
            }
            let any = col.as_any();
            Ok(if let Some(a) = any.downcast_ref::<Int64Array>() {
                KeyValue::Int(a.value(row))
            } else if let Some(a) = any.downcast_ref::<Int32Array>() {
                KeyValue::Int(a.value(row).into())
            } else if let Some(a) = any.downcast_ref::<UInt32Array>() {
                KeyValue::Int(a.value(row).into())
            } else if let Some(a) = any.downcast_ref::<UInt64Array>() {
                KeyValue::Int(i64::try_from(a.value(row)).with_context(|| format!("paging key `{name}` exceeds i64"))?)
            } else if let Some(a) = any.downcast_ref::<StringArray>() {
                KeyValue::Text(a.value(row).to_string())
            } else if let Some(a) = any.downcast_ref::<StringViewArray>() {
                KeyValue::Text(a.value(row).to_string())
            } else {
                bail!("paging key `{name}` has unsupported type {:?}", col.data_type())
            })
        })
        .collect()
}

/// Parse timestamp text into microseconds since the epoch, reading it as UTC (the archive
/// session's zone). The vendored connector renders `TIMESTAMP`/`DATETIME` as ISO text
/// (`2026-10-26T04:30:44.000000`); MariaDB's own form (`2026-10-26 04:30:44`) is accepted too. A
/// zero date or any other unparseable value is an error: archiving must not guess an instant.
fn parse_micros(column: &str, text: &str) -> Result<i64> {
    let parsed = NaiveDateTime::parse_from_str(text, "%Y-%m-%dT%H:%M:%S%.f")
        .or_else(|_| NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%.f"))
        .with_context(|| format!("column `{column}`: `{text}` is not a valid timestamp; archive refuses to guess"))?;
    Ok(parsed.and_utc().timestamp_micros())
}

fn text_values(col: &ArrayRef) -> Option<Vec<Option<String>>> {
    let any = col.as_any();
    if let Some(a) = any.downcast_ref::<StringArray>() {
        Some((0..a.len()).map(|i| (!a.is_null(i)).then(|| a.value(i).to_string())).collect())
    } else {
        any.downcast_ref::<StringViewArray>()
            .map(|a| (0..a.len()).map(|i| (!a.is_null(i)).then(|| a.value(i).to_string())).collect())
    }
}

/// Convert an extracted batch to the archive schema `target` (without `_archive_unit`, which the
/// writer stamps): timestamp text becomes typed microseconds, every other column is cast by name.
pub fn to_archive_batch(batch: &RecordBatch, target: &Schema) -> Result<RecordBatch> {
    let columns = target
        .fields()
        .iter()
        .map(|field| {
            let col = batch
                .column_by_name(field.name())
                .with_context(|| format!("column `{}` is missing from the extracted page", field.name()))?;
            Ok(match field.data_type() {
                DataType::Timestamp(TimeUnit::Microsecond, tz) => match text_values(col) {
                    Some(values) => {
                        let micros = values
                            .iter()
                            .map(|v| v.as_deref().map(|t| parse_micros(field.name(), t)).transpose())
                            .collect::<Result<Vec<Option<i64>>>>()?;
                        Arc::new(TimestampMicrosecondArray::from(micros).with_timezone_opt(tz.clone())) as ArrayRef
                    }
                    None => cast(col, field.data_type())?,
                },
                other => cast(col, other).with_context(|| format!("casting column `{}`", field.name()))?,
            })
        })
        .collect::<Result<Vec<ArrayRef>>>()?;
    RecordBatch::try_new(Arc::new(target.clone()), columns).context("building the archive batch")
}

/// The archive writer operations the [`Archiver`] needs; implemented by [`DeltaWriter`].
#[cfg_attr(test, mockall::automock)]
#[allow(async_fn_in_trait)]
pub trait ArchiveSink: Send + Sync {
    async fn ensure_archive_table(&self, table: &str, schema: SchemaRef, source_time_zone: &str) -> Result<()>;
    async fn archive_unit_present(&self, table: &str, unit: i64) -> Result<bool>;
    async fn begin_archive_unit(&self, table: &str, unit: i64) -> Result<()>;
    async fn stage_archive_chunk(&self, table: &str, batches: Vec<RecordBatch>) -> Result<()>;
    async fn commit_archive_unit(&self, table: &str) -> Result<u64>;
    async fn abort_archive_unit(&self, table: &str);
}

impl ArchiveSink for DeltaWriter {
    async fn ensure_archive_table(&self, table: &str, schema: SchemaRef, source_time_zone: &str) -> Result<()> {
        DeltaWriter::ensure_archive_table(self, table, schema, source_time_zone).await.map(|_| ())
    }
    async fn archive_unit_present(&self, table: &str, unit: i64) -> Result<bool> {
        DeltaWriter::archive_unit_present(self, table, unit).await
    }
    async fn begin_archive_unit(&self, table: &str, unit: i64) -> Result<()> {
        DeltaWriter::begin_archive_unit(self, table, unit).await
    }
    async fn stage_archive_chunk(&self, table: &str, batches: Vec<RecordBatch>) -> Result<()> {
        DeltaWriter::stage_archive_chunk(self, table, batches).await
    }
    async fn commit_archive_unit(&self, table: &str) -> Result<u64> {
        DeltaWriter::commit_archive_unit(self, table).await
    }
    async fn abort_archive_unit(&self, table: &str) {
        DeltaWriter::abort_archive_unit(self, table).await
    }
}

/// A table in archive mode and the scope predicate that selects one unit's rows in it.
#[derive(Debug, Clone, PartialEq)]
pub struct ArchiveTable {
    pub name: String,
    pub scope: String,
}

/// What archiving one unit did to one table.
#[derive(Debug, Clone, PartialEq)]
pub enum UnitOutcome {
    /// The unit's rows were committed in one append-only commit.
    Archived { rows: u64 },
    /// Delta already holds the unit (`_archive_unit`); nothing was read or written.
    AlreadyArchived,
    /// The source holds no rows of the unit in this table; nothing was committed.
    NoRows,
}

#[derive(Debug, Clone, PartialEq)]
pub struct UnitResult {
    pub unit: i64,
    pub table: String,
    pub outcome: UnitOutcome,
}

/// A table ready to archive into: what to select, how to page, and the archive schema.
struct PreparedTable {
    name: String,
    scope: String,
    columns: Vec<String>,
    key: Vec<String>,
    /// The archive schema without `_archive_unit`, which the writer stamps.
    target: Schema,
    page_rows: u64,
}

/// Archives given units across archive tables: for each unit, each table's rows are extracted
/// page by page and appended in one commit, or skipped when Delta already holds them. Which units
/// to archive is the caller's decision (docs/design/archive-mode.md).
pub struct Archiver<S, E, W> {
    inspect: S,
    extract: E,
    sink: W,
    source_time_zone: String,
    max_page_rows: Option<u64>,
}

impl<S: SchemaInspect, E: Extract, W: ArchiveSink> Archiver<S, E, W> {
    /// `extract` should be an archive extractor (`BatchExtractor::with_session_init` with
    /// [`ARCHIVE_SESSION_INIT`]) so timestamps are read in UTC.
    pub fn new(inspect: S, extract: E, sink: W, source_time_zone: impl Into<String>) -> Self {
        Self { inspect, extract, sink, source_time_zone: source_time_zone.into(), max_page_rows: None }
    }

    /// Cap the rows per page below the memory-derived batch size.
    pub fn with_max_page_rows(mut self, rows: u64) -> Self {
        self.max_page_rows = Some(rows.max(1));
        self
    }

    /// Archive every unit in `units` in every table of `tables`, unit by unit. Stops at the first
    /// error, after aborting that table's in-flight unit; units committed before it stay committed
    /// and are skipped by a re-run.
    pub async fn archive_units(&mut self, tables: &[ArchiveTable], units: &[i64]) -> Result<Vec<UnitResult>> {
        let mut prepared = Vec::with_capacity(tables.len());
        for table in tables {
            prepared.push(self.prepare(table).await?);
        }
        let mut results = Vec::with_capacity(units.len() * prepared.len());
        for &unit in units {
            for table in &prepared {
                let outcome = self
                    .archive_one(table, unit)
                    .await
                    .with_context(|| format!("archiving unit {unit} of `{}`", table.name))?;
                results.push(UnitResult { unit, table: table.name.clone(), outcome });
            }
        }
        Ok(results)
    }

    async fn prepare(&mut self, table: &ArchiveTable) -> Result<PreparedTable> {
        let columns = filter_unsupported_columns(&self.inspect.discover_columns(&table.name).await?);
        if columns.is_empty() {
            bail!("archive table `{}` has no extractable columns (does it exist?)", table.name);
        }
        let key = self
            .inspect
            .discover_indexes(&table.name)
            .await?
            .into_iter()
            .find(|i| i.name == "PRIMARY")
            .map(|i| i.columns)
            .filter(|cols| !cols.is_empty())
            .with_context(|| format!("archive table `{}` has no PRIMARY key to page by", table.name))?;
        if let Some(missing) = key.iter().find(|k| !columns.iter().any(|c| &c.name == *k)) {
            bail!("archive table `{}`: primary key column `{missing}` is not extractable", table.name);
        }
        let schema = archive_schema(&columns, &self.source_time_zone)?;
        self.sink.ensure_archive_table(&table.name, schema.clone(), &self.source_time_zone).await?;
        let target = Schema::new(
            schema.fields().iter().filter(|f| f.name() != ARCHIVE_UNIT_COLUMN).cloned().collect::<Vec<_>>(),
        );
        let avg_row_length = self.inspect.get_avg_row_length(&table.name).await?;
        let batch = self.extract.calculate_batch_size(avg_row_length).max(1);
        let page_rows = self.max_page_rows.map_or(batch, |cap| batch.min(cap));
        Ok(PreparedTable {
            name: table.name.clone(),
            scope: table.scope.clone(),
            columns: columns.into_iter().map(|c| c.name).collect(),
            key,
            target,
            page_rows,
        })
    }

    async fn archive_one(&mut self, table: &PreparedTable, unit: i64) -> Result<UnitOutcome> {
        if self.sink.archive_unit_present(&table.name, unit).await? {
            return Ok(UnitOutcome::AlreadyArchived);
        }
        self.sink.begin_archive_unit(&table.name, unit).await?;
        if let Err(e) = self.stage_pages(table, unit).await {
            self.sink.abort_archive_unit(&table.name).await;
            return Err(e);
        }
        Ok(match self.sink.commit_archive_unit(&table.name).await? {
            0 => UnitOutcome::NoRows,
            rows => UnitOutcome::Archived { rows },
        })
    }

    async fn stage_pages(&mut self, table: &PreparedTable, unit: i64) -> Result<()> {
        let mut after: Option<Vec<KeyValue>> = None;
        loop {
            let sql = page_query(
                &table.name, &table.columns, &table.scope, unit, &table.key, after.as_deref(), table.page_rows,
            )?;
            let extraction = self.extract.extract(&sql)?;
            let pages: Vec<RecordBatch> = extraction.batches.into_iter().filter(|b| b.num_rows() > 0).collect();
            let rows: u64 = pages.iter().map(|b| b.num_rows() as u64).sum();
            let Some(last) = pages.last() else { return Ok(()) };
            after = Some(last_key(last, &table.key)?);
            let typed = pages.iter().map(|b| to_archive_batch(b, &table.target)).collect::<Result<Vec<_>>>()?;
            self.sink.stage_archive_chunk(&table.name, typed).await?;
            // A short page means the unit is exhausted — unless the memory breaker cut it short,
            // in which case the next page continues after its last key.
            if rows < table.page_rows && !extraction.truncated {
                return Ok(());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn col(name: &str, data_type: &str, column_type: &str) -> ColumnInfo {
        ColumnInfo { name: name.into(), data_type: data_type.into(), column_type: column_type.into(), nullable: true }
    }

    #[test]
    fn archive_schema_types_timestamps_and_records_the_zone() {
        let schema = archive_schema(
            &[
                col("id", "int", "int(10) unsigned"),
                col("sent_at", "timestamp", "timestamp"),
                col("scheduled_for", "datetime", "datetime"),
                col("subject", "varchar", "varchar(255)"),
            ],
            "Asia/Jakarta",
        )
        .unwrap();
        let sent = schema.field_with_name("sent_at").unwrap();
        assert_eq!(sent.data_type(), &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())));
        assert_eq!(sent.metadata().get(SOURCE_TIME_ZONE_KEY).map(String::as_str), Some("Asia/Jakarta"));
        let scheduled = schema.field_with_name("scheduled_for").unwrap();
        assert_eq!(scheduled.data_type(), &DataType::Timestamp(TimeUnit::Microsecond, None), "DATETIME has no zone");
        assert!(scheduled.metadata().contains_key(SOURCE_TIME_ZONE_KEY));
        assert!(schema.field_with_name("subject").unwrap().metadata().is_empty());
        assert_eq!(schema.fields().last().unwrap().name(), crate::writer::ARCHIVE_UNIT_COLUMN);
    }

    #[test]
    fn page_query_binds_the_unit_and_pages_by_the_whole_key() {
        let cols = vec!["campaign_id".to_string(), "subscriber_id".to_string(), "variables".to_string()];
        let key = vec!["campaign_id".to_string(), "subscriber_id".to_string()];
        let first = page_query("vars", &cols, "campaign_id = ?", 909, &key, None, 500).unwrap();
        assert_eq!(
            first,
            "SELECT `campaign_id`, `subscriber_id`, `variables` FROM `vars` WHERE (campaign_id = 909) \
             ORDER BY `campaign_id`, `subscriber_id` LIMIT 500"
        );
        let next = page_query(
            "vars", &cols, "campaign_id = ?", 909, &key,
            Some(&[KeyValue::Int(909), KeyValue::Int(41)]), 500,
        )
        .unwrap();
        assert!(next.contains(" AND (`campaign_id`, `subscriber_id`) > (909, 41) ORDER BY"), "got: {next}");
    }

    #[test]
    fn page_query_escapes_text_keys_and_keeps_a_subquery_scope() {
        let cols = vec!["message_id".to_string()];
        let key = vec!["message_id".to_string()];
        let scope = "message_id IN (SELECT id FROM messages WHERE campaign_id = ?)";
        let q = page_query("events", &cols, scope, 7, &key, Some(&[KeyValue::Text(r"a'b\".to_string())]), 10).unwrap();
        assert!(q.contains("WHERE (message_id IN (SELECT id FROM messages WHERE campaign_id = 7))"), "got: {q}");
        assert!(q.contains(r"> ('a''b\\')"), "quote doubled, backslash escaped: {q}");
    }

    #[test]
    fn page_query_refuses_a_bad_scope_or_a_missing_key() {
        assert!(page_query("t", &["id".into()], "a = ? OR b = ?", 1, &["id".into()], None, 10).is_err());
        assert!(page_query("t", &["id".into()], "a = ?", 1, &[], None, 10).is_err());
    }

    #[test]
    fn last_key_reads_the_final_row() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("campaign_id", DataType::UInt32, false),
            Field::new("message_id", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(UInt32Array::from(vec![909, 909])), Arc::new(StringArray::from(vec!["a", "z"]))],
        )
        .unwrap();
        let key = last_key(&batch, &["campaign_id".into(), "message_id".into()]).unwrap();
        assert_eq!(key, vec![KeyValue::Int(909), KeyValue::Text("z".into())]);
    }

    #[test]
    fn to_archive_batch_turns_utc_text_into_instants() {
        let source = Arc::new(Schema::new(vec![
            Field::new("id", DataType::UInt32, false),
            Field::new("sent_at", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            source,
            vec![
                Arc::new(UInt32Array::from(vec![1, 2, 3])),
                // The connector's ISO form, a NULL, and MariaDB's own spaced form with a fraction.
                Arc::new(StringArray::from(vec![
                    Some("2026-10-26T04:30:44.000000"),
                    None,
                    Some("2026-10-26 04:30:44.5"),
                ])),
            ],
        )
        .unwrap();
        let target = Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("sent_at", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), true),
        ]);
        let out = to_archive_batch(&batch, &target).unwrap();
        let ts = out.column(1).as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
        // 2026-10-26T04:30:44Z — the instant WIB shows as 11:30:44.
        assert_eq!(ts.value(0), 1_792_989_044_000_000);
        assert!(ts.is_null(1));
        assert_eq!(ts.value(2), 1_792_989_044_500_000);
        assert_eq!(out.column(0).data_type(), &DataType::Int64, "other columns are cast by name");
    }

    mod archiver {
        use super::super::*;
        use crate::discovery::IndexInfo;
        use crate::extractor::Extraction;
        use crate::orchestrator::{MockExtract, MockSchemaInspect};
        use std::sync::Mutex;

        fn columns() -> Vec<ColumnInfo> {
            vec![
                ColumnInfo { name: "id".into(), data_type: "bigint".into(), column_type: "bigint(20)".into(), nullable: false },
                ColumnInfo { name: "sent_at".into(), data_type: "timestamp".into(), column_type: "timestamp".into(), nullable: true },
            ]
        }

        fn page(ids: std::ops::RangeInclusive<i64>) -> RecordBatch {
            let ids: Vec<i64> = ids.collect();
            let sent: Vec<&str> = ids.iter().map(|_| "2026-10-26 04:30:44").collect();
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("id", DataType::Int64, false),
                    Field::new("sent_at", DataType::Utf8, true),
                ])),
                vec![Arc::new(Int64Array::from(ids)), Arc::new(StringArray::from(sent))],
            )
            .unwrap()
        }

        fn inspect() -> MockSchemaInspect {
            let mut m = MockSchemaInspect::new();
            m.expect_discover_columns().returning(|_| Ok(columns()));
            m.expect_discover_indexes().returning(|_| {
                Ok(vec![IndexInfo { name: "PRIMARY".into(), unique: true, columns: vec!["id".into()] }])
            });
            m.expect_get_avg_row_length().returning(|_| Ok(Some(100)));
            m
        }

        fn table() -> Vec<ArchiveTable> {
            vec![ArchiveTable { name: "messages".into(), scope: "campaign_id = ?".into() }]
        }

        /// Pages served in order; every SQL statement is recorded.
        fn extract(pages: Vec<(Vec<RecordBatch>, bool)>, seen: Arc<Mutex<Vec<String>>>) -> MockExtract {
            let mut m = MockExtract::new();
            m.expect_calculate_batch_size().returning(|_| 1_000_000);
            let pages = Mutex::new(pages.into_iter());
            m.expect_extract().returning(move |sql| {
                seen.lock().unwrap().push(sql.to_string());
                let (batches, truncated) = pages.lock().unwrap().next().unwrap_or_default();
                Ok(Extraction { batches, truncated })
            });
            m
        }

        fn sink(present: bool, staged: Arc<Mutex<Vec<usize>>>) -> MockArchiveSink {
            let mut m = MockArchiveSink::new();
            m.expect_ensure_archive_table().returning(|_, _, _| Ok(()));
            m.expect_archive_unit_present().returning(move |_, _| Ok(present));
            m.expect_begin_archive_unit().returning(|_, _| Ok(()));
            let counted = staged.clone();
            m.expect_stage_archive_chunk().returning(move |_, b| {
                counted.lock().unwrap().push(b.iter().map(|x| x.num_rows()).sum());
                Ok(())
            });
            m.expect_commit_archive_unit()
                .returning(move |_| Ok(staged.lock().unwrap().iter().sum::<usize>() as u64));
            m
        }

        #[tokio::test]
        async fn pages_until_a_short_page_then_commits_once() {
            let seen = Arc::new(Mutex::new(Vec::new()));
            let staged = Arc::new(Mutex::new(Vec::new()));
            let pages = vec![(vec![page(1..=3)], false), (vec![page(4..=6)], false), (vec![page(7..=7)], false)];
            let mut archiver = Archiver::new(inspect(), extract(pages, seen.clone()), sink(false, staged.clone()), "Asia/Jakarta")
                .with_max_page_rows(3);
            let results = archiver.archive_units(&table(), &[909]).await.unwrap();
            assert_eq!(results, vec![UnitResult { unit: 909, table: "messages".into(), outcome: UnitOutcome::Archived { rows: 7 } }]);
            assert_eq!(*staged.lock().unwrap(), vec![3, 3, 1]);
            let sql = seen.lock().unwrap().clone();
            assert_eq!(sql.len(), 3, "a short third page ends the unit");
            assert!(sql[0].contains("WHERE (campaign_id = 909) ORDER BY `id` LIMIT 3"), "got: {}", sql[0]);
            assert!(sql[1].contains("AND (`id`) > (3)"), "page 2 continues after id 3: {}", sql[1]);
            assert!(sql[2].contains("AND (`id`) > (6)"), "page 3 continues after id 6: {}", sql[2]);
        }

        #[tokio::test]
        async fn a_truncated_short_page_does_not_end_the_unit() {
            let seen = Arc::new(Mutex::new(Vec::new()));
            let staged = Arc::new(Mutex::new(Vec::new()));
            let pages = vec![(vec![page(1..=2)], true), (vec![page(3..=3)], false)];
            let mut archiver = Archiver::new(inspect(), extract(pages, seen.clone()), sink(false, staged.clone()), "UTC")
                .with_max_page_rows(3);
            archiver.archive_units(&table(), &[909]).await.unwrap();
            assert_eq!(seen.lock().unwrap().len(), 2, "the breaker cut page 1 short; page 2 follows");
        }

        #[tokio::test]
        async fn an_already_archived_unit_is_not_read_again() {
            let seen = Arc::new(Mutex::new(Vec::new()));
            let mut s = sink(true, Arc::new(Mutex::new(Vec::new())));
            s.expect_begin_archive_unit().times(0);
            let mut archiver = Archiver::new(inspect(), extract(vec![], seen.clone()), s, "UTC");
            let results = archiver.archive_units(&table(), &[909]).await.unwrap();
            assert_eq!(results[0].outcome, UnitOutcome::AlreadyArchived);
            assert!(seen.lock().unwrap().is_empty(), "no source read for an archived unit");
        }

        #[tokio::test]
        async fn a_unit_with_no_source_rows_reports_no_rows() {
            let staged = Arc::new(Mutex::new(Vec::new()));
            let mut archiver = Archiver::new(inspect(), extract(vec![], Arc::new(Mutex::new(Vec::new()))), sink(false, staged), "UTC");
            let results = archiver.archive_units(&table(), &[909]).await.unwrap();
            assert_eq!(results[0].outcome, UnitOutcome::NoRows);
        }

        #[tokio::test]
        async fn a_failing_page_aborts_the_unit_without_committing() {
            let mut ex = MockExtract::new();
            ex.expect_calculate_batch_size().returning(|_| 1000);
            ex.expect_extract().returning(|_| Err(anyhow::anyhow!("connection reset")));
            let mut s = MockArchiveSink::new();
            s.expect_ensure_archive_table().returning(|_, _, _| Ok(()));
            s.expect_archive_unit_present().returning(|_, _| Ok(false));
            s.expect_begin_archive_unit().returning(|_, _| Ok(()));
            s.expect_abort_archive_unit().times(1).returning(|_| ());
            s.expect_commit_archive_unit().times(0);
            let mut archiver = Archiver::new(inspect(), ex, s, "UTC");
            let err = format!("{:#}", archiver.archive_units(&table(), &[909]).await.unwrap_err());
            assert!(err.contains("archiving unit 909 of `messages`") && err.contains("connection reset"), "got: {err}");
        }

        #[tokio::test]
        async fn a_table_without_a_primary_key_is_refused() {
            let mut i = MockSchemaInspect::new();
            i.expect_discover_columns().returning(|_| Ok(columns()));
            i.expect_discover_indexes().returning(|_| Ok(vec![]));
            let mut archiver = Archiver::new(i, MockExtract::new(), MockArchiveSink::new(), "UTC");
            let err = archiver.archive_units(&table(), &[909]).await.unwrap_err().to_string();
            assert!(err.contains("no PRIMARY key"), "got: {err}");
        }
    }

    #[test]
    fn to_archive_batch_refuses_a_zero_date() {
        let source = Arc::new(Schema::new(vec![Field::new("sent_at", DataType::Utf8, true)]));
        let batch = RecordBatch::try_new(source, vec![Arc::new(StringArray::from(vec!["0000-00-00 00:00:00"]))]).unwrap();
        let target = Schema::new(vec![Field::new("sent_at", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), true)]);
        let err = format!("{:#}", to_archive_batch(&batch, &target).unwrap_err());
        assert!(err.contains("sent_at") && err.contains("0000-00-00"), "got: {err}");
    }
}
