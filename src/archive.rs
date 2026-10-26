//! Archive mode building blocks (docs/design/archive-mode.md): the archive table's schema, the
//! scoped key-paged extraction query, and the conversion of extracted rows to the archive schema.
//!
//! Archive extraction reads with the session pinned to UTC (`ARCHIVE_SESSION_INIT`), so MariaDB
//! returns every `TIMESTAMP` as UTC text; `to_archive_batch` turns that text into typed instants.
//! The source time zone is kept as metadata on each timestamp column (and as a table property),
//! because a Delta `timestamp` cannot carry a zone name itself.

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

use crate::discovery::ColumnInfo;
use crate::query::backtick;
use crate::writer::with_archive_unit_column;

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

/// Parse MariaDB's `YYYY-MM-DD HH:MM:SS[.ffffff]` text into microseconds since the epoch, reading
/// it as UTC (the archive session's zone). A zero date or any other unparseable value is an error:
/// archiving must not guess an instant.
fn parse_micros(column: &str, text: &str) -> Result<i64> {
    let parsed = NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%.f")
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
                Arc::new(StringArray::from(vec![Some("2026-10-26 04:30:44"), None, Some("2026-10-26 04:30:44.5")])),
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

    #[test]
    fn to_archive_batch_refuses_a_zero_date() {
        let source = Arc::new(Schema::new(vec![Field::new("sent_at", DataType::Utf8, true)]));
        let batch = RecordBatch::try_new(source, vec![Arc::new(StringArray::from(vec!["0000-00-00 00:00:00"]))]).unwrap();
        let target = Schema::new(vec![Field::new("sent_at", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), true)]);
        let err = format!("{:#}", to_archive_batch(&batch, &target).unwrap_err());
        assert!(err.contains("sent_at") && err.contains("0000-00-00"), "got: {err}");
    }
}
