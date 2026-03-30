use std::collections::HashMap;

use anyhow::{Context, Result};
use deltalake::arrow::array::{Array, Int64Array, StringArray, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampSecondArray};
use deltalake::arrow::datatypes::{DataType, Schema as ArrowSchema, SchemaRef, TimeUnit};
use deltalake::arrow::record_batch::RecordBatch;
use deltalake::kernel::StructType;
use deltalake::protocol::SaveMode;
use deltalake::DeltaTable;
use tracing::{info, warn};
use url::Url;

#[derive(Debug, Clone)]
pub struct Hwm {
    pub updated_at: String,
    pub last_id: i64,
}

pub struct DeltaWriter {
    bucket: String,
    prefix: String,
    storage_options: HashMap<String, String>,
    use_local_fs: bool,
}

impl DeltaWriter {
    pub fn new(
        bucket: &str,
        prefix: &str,
        endpoint: Option<&str>,
        region: &str,
        access_key: &str,
        secret_key: &str,
    ) -> Self {
        let mut storage_options = HashMap::new();
        storage_options.insert("AWS_REGION".to_string(), region.to_string());
        storage_options.insert("AWS_ACCESS_KEY_ID".to_string(), access_key.to_string());
        storage_options.insert("AWS_SECRET_ACCESS_KEY".to_string(), secret_key.to_string());
        if let Some(ep) = endpoint {
            storage_options.insert("AWS_ENDPOINT_URL".to_string(), ep.to_string());
        }
        storage_options.insert("AWS_ALLOW_HTTP".to_string(), "true".to_string());
        storage_options.insert("AWS_S3_ALLOW_UNSAFE_RENAME".to_string(), "true".to_string());

        Self {
            bucket: bucket.to_string(),
            prefix: prefix.to_string(),
            storage_options,
            use_local_fs: false,
        }
    }

    pub fn new_local(base_dir: &str) -> Self {
        Self {
            bucket: String::new(),
            prefix: base_dir.to_string(),
            storage_options: HashMap::new(),
            use_local_fs: true,
        }
    }

    fn table_url(&self, table_name: &str) -> Result<Url> {
        if self.use_local_fs {
            let path = std::path::Path::new(&self.prefix).join(table_name);
            Url::from_directory_path(&path)
                .map_err(|_| anyhow::anyhow!("invalid local path: {:?}", path))
        } else {
            let url_str = format!("s3://{}/{}/{}/", self.bucket, self.prefix, table_name);
            Url::parse(&url_str).context("invalid S3 URL")
        }
    }

    async fn open_table(&self, table_name: &str) -> Result<DeltaTable> {
        let url = self.table_url(table_name)?;
        let mut table = deltalake::DeltaTableBuilder::from_url(url)?
            .with_storage_options(self.storage_options.clone())
            .build()?;
        table.load().await?;
        Ok(table)
    }

    pub async fn ensure_table(
        &self,
        table_name: &str,
        schema: SchemaRef,
    ) -> Result<DeltaTable> {
        let url = self.table_url(table_name)?;

        let mut table = deltalake::DeltaTableBuilder::from_url(url.clone())?
            .with_storage_options(self.storage_options.clone())
            .build()?;

        match table.load().await {
            Ok(()) => {
                info!(table = table_name, "Delta table already exists");
                Ok(table)
            }
            Err(deltalake::DeltaTableError::NotATable(_)) => {
                info!(table = table_name, "Creating new Delta table");
                let delta_schema = arrow_schema_to_delta(&schema)?;

                // NOTE: DeltaOps is deprecated in deltalake 0.31 in favor of
                // DeltaTable::create(), but that API does not exist yet in 0.31.1.
                // Replace with DeltaTable::create() when upgrading to deltalake 0.32+.
                #[allow(deprecated)]
                let table = deltalake::DeltaOps::try_from_url_with_storage_options(
                    url,
                    self.storage_options.clone(),
                )
                .await?
                .create()
                .with_columns(delta_schema.fields().cloned())
                .with_table_name(table_name)
                .await?;

                info!(table = table_name, "Delta table created");
                Ok(table)
            }
            Err(e) => Err(e).context(format!("S3 connection error for table {table_name}")),
        }
    }

    pub async fn append_batch(
        &self,
        table_name: &str,
        batches: Vec<RecordBatch>,
        hwm: Option<&Hwm>,
    ) -> Result<()> {
        if batches.is_empty() {
            return Ok(());
        }

        let commit_properties = build_commit_properties(hwm);

        let url = self.table_url(table_name)?;
        // NOTE: DeltaOps deprecated — see ensure_table() comment. Replace with
        // DeltaTable::write() when upgrading to deltalake 0.32+.
        #[allow(deprecated)]
        let ops = deltalake::DeltaOps::try_from_url_with_storage_options(
            url,
            self.storage_options.clone(),
        )
        .await?;

        #[allow(deprecated)]
        {
            ops.write(batches)
                .with_save_mode(SaveMode::Append)
                .with_commit_properties(commit_properties)
                .await?;
        }

        info!(
            table = table_name,
            hwm = ?hwm,
            "batch appended to Delta table"
        );
        Ok(())
    }

    pub async fn overwrite_table(
        &self,
        table_name: &str,
        batches: Vec<RecordBatch>,
        hwm: Option<&Hwm>,
    ) -> Result<()> {
        if batches.is_empty() {
            return Ok(());
        }

        let commit_properties = build_commit_properties(hwm);

        let url = self.table_url(table_name)?;
        // NOTE: DeltaOps deprecated — see ensure_table() comment. Replace with
        // DeltaTable::write() when upgrading to deltalake 0.32+.
        #[allow(deprecated)]
        let ops = deltalake::DeltaOps::try_from_url_with_storage_options(
            url,
            self.storage_options.clone(),
        )
        .await?;

        #[allow(deprecated)]
        ops.write(batches)
            .with_save_mode(SaveMode::Overwrite)
            .with_commit_properties(commit_properties)
            .await?;

        info!(
            table = table_name,
            "table overwritten in Delta Lake"
        );
        Ok(())
    }

    pub async fn read_hwm(&self, table_name: &str) -> Result<Option<Hwm>> {
        let table = match self.open_table(table_name).await {
            Ok(t) => t,
            Err(_) => {
                info!(table = table_name, "Delta table does not exist, no HWM");
                return Ok(None);
            }
        };

        let mut history = table.history(Some(1)).await?.collect::<Vec<_>>();
        let commit_info = match history.pop() {
            Some(ci) => ci,
            None => {
                warn!(table = table_name, "Delta table has no commits, no HWM");
                return Ok(None);
            }
        };

        let updated_at = commit_info.info.get("hwm_updated_at");
        let last_id = commit_info.info.get("hwm_last_id");

        match (updated_at, last_id) {
            (Some(serde_json::Value::String(ua)), Some(serde_json::Value::String(id))) => {
                let id: i64 = id.parse().context("invalid hwm_last_id in commitInfo")?;
                info!(
                    table = table_name,
                    hwm_updated_at = %ua,
                    hwm_last_id = id,
                    "read HWM from Delta log"
                );
                Ok(Some(Hwm {
                    updated_at: ua.clone(),
                    last_id: id,
                }))
            }
            _ => {
                warn!(
                    table = table_name,
                    "Delta table exists but no HWM in commitInfo, starting from beginning"
                );
                Ok(None)
            }
        }
    }
}

fn build_commit_properties(hwm: Option<&Hwm>) -> deltalake::kernel::transaction::CommitProperties {
    let mut metadata = HashMap::new();
    if let Some(h) = hwm {
        metadata.insert(
            "hwm_updated_at".to_string(),
            serde_json::Value::String(h.updated_at.clone()),
        );
        metadata.insert(
            "hwm_last_id".to_string(),
            serde_json::Value::String(h.last_id.to_string()),
        );
    }
    deltalake::kernel::transaction::CommitProperties::default().with_metadata(metadata)
}

pub fn extract_hwm_from_batch(batch: &RecordBatch) -> Option<Hwm> {
    let updated_at_col = batch.column_by_name("updated_at")?;
    let id_col = batch.column_by_name("id")?;

    let n = batch.num_rows();
    if n == 0 {
        return None;
    }

    let timestamp_strings = extract_timestamp_as_strings(updated_at_col)?;
    let ids = id_col.as_any().downcast_ref::<Int64Array>()?;

    let mut max_ts: &str = &timestamp_strings[0];
    let mut max_id = ids.value(0);

    for (i, ts) in timestamp_strings.iter().enumerate().skip(1) {
        if ts.as_str() > max_ts
            || (ts.as_str() == max_ts && ids.value(i) > max_id)
        {
            max_ts = ts.as_str();
            max_id = ids.value(i);
        }
    }

    Some(Hwm {
        updated_at: max_ts.to_string(),
        last_id: max_id,
    })
}

fn extract_timestamp_as_strings(col: &std::sync::Arc<dyn Array>) -> Option<Vec<String>> {
    if let Some(ts) = col.as_any().downcast_ref::<TimestampMicrosecondArray>() {
        Some(
            (0..ts.len())
                .map(|i| {
                    if ts.is_null(i) {
                        String::new()
                    } else {
                        micros_to_string(ts.value(i))
                    }
                })
                .collect(),
        )
    } else if let Some(ts) = col.as_any().downcast_ref::<TimestampMillisecondArray>() {
        Some(
            (0..ts.len())
                .map(|i| {
                    if ts.is_null(i) {
                        String::new()
                    } else {
                        millis_to_string(ts.value(i))
                    }
                })
                .collect(),
        )
    } else if let Some(ts) = col.as_any().downcast_ref::<TimestampSecondArray>() {
        Some(
            (0..ts.len())
                .map(|i| {
                    if ts.is_null(i) {
                        String::new()
                    } else {
                        secs_to_string(ts.value(i))
                    }
                })
                .collect(),
        )
    } else {
        col.as_any()
            .downcast_ref::<StringArray>()
            .map(|s| (0..s.len()).map(|i| s.value(i).to_string()).collect())
    }
}

fn micros_to_string(micros: i64) -> String {
    let secs = micros / 1_000_000;
    let subsec_nanos = (micros % 1_000_000).unsigned_abs() as u32 * 1000;
    format_naive_datetime(secs, subsec_nanos)
}

fn millis_to_string(millis: i64) -> String {
    let secs = millis / 1000;
    let subsec_nanos = ((millis % 1000).unsigned_abs() as u32) * 1_000_000;
    format_naive_datetime(secs, subsec_nanos)
}

fn secs_to_string(secs: i64) -> String {
    format_naive_datetime(secs, 0)
}

fn format_naive_datetime(secs: i64, subsec_nanos: u32) -> String {
    let time_secs = secs.rem_euclid(86400);
    let days = secs.div_euclid(86400);
    let hours = time_secs / 3600;
    let minutes = (time_secs % 3600) / 60;
    let seconds = time_secs % 60;

    let (year, month, day) = epoch_days_to_ymd(days);

    if subsec_nanos > 0 {
        let frac = format!("{subsec_nanos:09}").trim_end_matches('0').to_string();
        format!(
            "{year:04}-{month:02}-{day:02} {:02}:{:02}:{:02}.{frac}",
            hours, minutes, seconds
        )
    } else {
        format!(
            "{year:04}-{month:02}-{day:02} {:02}:{:02}:{:02}",
            hours, minutes, seconds
        )
    }
}

fn epoch_days_to_ymd(days: i64) -> (i64, i64, i64) {
    let mut year = 1970i64;
    let mut remaining = days;

    loop {
        let year_len = if is_leap(year) { 366 } else { 365 };
        if remaining >= 0 && remaining < year_len {
            break;
        }
        if remaining >= 0 {
            remaining -= year_len;
            year += 1;
        } else {
            year -= 1;
            remaining += if is_leap(year) { 366 } else { 365 };
        }
    }

    let leap = is_leap(year);
    let month_days = [
        31,
        if leap { 29 } else { 28 },
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ];

    let mut month = 1i64;
    for &md in &month_days {
        if remaining < md {
            break;
        }
        remaining -= md;
        month += 1;
    }

    (year, month, remaining + 1)
}

fn is_leap(year: i64) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

fn arrow_schema_to_delta(schema: &ArrowSchema) -> Result<StructType> {
    let fields: Vec<deltalake::kernel::StructField> = schema
        .fields()
        .iter()
        .map(|f| {
            let dt = arrow_type_to_delta(f.data_type())?;
            Ok(deltalake::kernel::StructField::new(
                f.name(),
                dt,
                f.is_nullable(),
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    StructType::try_new(fields).context("failed to create Delta schema")
}

fn arrow_type_to_delta(dt: &DataType) -> Result<deltalake::kernel::DataType> {
    use deltalake::kernel::DataType as D;

    match dt {
        DataType::Boolean => Ok(D::BOOLEAN),
        DataType::Int8 | DataType::Int16 | DataType::Int32 => Ok(D::INTEGER),
        DataType::Int64 => Ok(D::LONG),
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 => Ok(D::INTEGER),
        DataType::UInt64 => Ok(D::LONG),
        DataType::Float16 | DataType::Float32 => Ok(D::FLOAT),
        DataType::Float64 => Ok(D::DOUBLE),
        DataType::Utf8 | DataType::LargeUtf8 => Ok(D::STRING),
        DataType::Binary | DataType::LargeBinary => Ok(D::BINARY),
        DataType::Date32 | DataType::Date64 => Ok(D::DATE),
        DataType::Timestamp(TimeUnit::Microsecond, _) => Ok(D::TIMESTAMP),
        DataType::Timestamp(TimeUnit::Millisecond, _) => Ok(D::TIMESTAMP),
        DataType::Timestamp(TimeUnit::Second, _) => Ok(D::TIMESTAMP),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => Ok(D::TIMESTAMP),
        DataType::Decimal128(p, s) | DataType::Decimal256(p, s) => {
            let scale_u8: u8 = (*s).try_into().context("invalid decimal scale")?;
            Ok(D::decimal(*p, scale_u8)?)
        }
        _ => anyhow::bail!("unsupported Arrow type for Delta: {dt:?}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use deltalake::arrow::datatypes::Field;
    use std::sync::Arc;

    fn make_batch_with_timestamps(
        ids: Vec<i64>,
        names: Vec<&str>,
        timestamps_micros: Vec<i64>,
    ) -> RecordBatch {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new(
                "updated_at",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
        ]));
        let id_arr = Int64Array::from(ids);
        let name_arr = StringArray::from(names);
        let ts_arr = TimestampMicrosecondArray::from(timestamps_micros);
        RecordBatch::try_new(schema, vec![Arc::new(id_arr), Arc::new(name_arr), Arc::new(ts_arr)])
            .unwrap()
    }

    fn make_batch_no_updated_at() -> RecordBatch {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let id_arr = Int64Array::from(vec![1i64]);
        let name_arr = StringArray::from(vec!["test"]);
        RecordBatch::try_new(schema, vec![Arc::new(id_arr), Arc::new(name_arr)]).unwrap()
    }

    fn make_batch_no_id() -> RecordBatch {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new(
                "updated_at",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
        ]));
        let name_arr = StringArray::from(vec!["test"]);
        let ts_arr = TimestampMicrosecondArray::from(vec![1743158400000000i64]);
        RecordBatch::try_new(schema, vec![Arc::new(name_arr), Arc::new(ts_arr)]).unwrap()
    }

    #[test]
    fn extract_hwm_single_row() {
        let batch = make_batch_with_timestamps(
            vec![42],
            vec!["a"],
            vec![1743158400000000i64],
        );
        let hwm = extract_hwm_from_batch(&batch).unwrap();
        assert_eq!(hwm.last_id, 42);
        assert!(hwm.updated_at.contains("2025"));
    }

    #[test]
    fn extract_hwm_multiple_rows_max_timestamp() {
        let batch = make_batch_with_timestamps(
            vec![1, 2, 3],
            vec!["a", "b", "c"],
            vec![1000000i64, 3000000i64, 2000000i64],
        );
        let hwm = extract_hwm_from_batch(&batch).unwrap();
        assert_eq!(hwm.last_id, 2);
    }

    #[test]
    fn extract_hwm_same_timestamp_picks_max_id() {
        let batch = make_batch_with_timestamps(
            vec![10, 50, 30],
            vec!["a", "b", "c"],
            vec![5000000i64, 5000000i64, 5000000i64],
        );
        let hwm = extract_hwm_from_batch(&batch).unwrap();
        assert_eq!(hwm.last_id, 50);
    }

    #[test]
    fn extract_hwm_empty_batch() {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "updated_at",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
        ]));
        let id_arr = Int64Array::from(Vec::<i64>::new());
        let ts_arr = TimestampMicrosecondArray::from(Vec::<i64>::new());
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(id_arr), Arc::new(ts_arr)]).unwrap();

        assert!(extract_hwm_from_batch(&batch).is_none());
    }

    #[test]
    fn extract_hwm_missing_updated_at_returns_none() {
        let batch = make_batch_no_updated_at();
        assert!(extract_hwm_from_batch(&batch).is_none());
    }

    #[test]
    fn extract_hwm_missing_id_returns_none() {
        let batch = make_batch_no_id();
        assert!(extract_hwm_from_batch(&batch).is_none());
    }

    #[test]
    fn extract_hwm_string_timestamp() {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("updated_at", DataType::Utf8, false),
        ]));
        let id_arr = Int64Array::from(vec![1i64, 2i64]);
        let ts_arr = StringArray::from(vec!["2026-03-28 09:00:00", "2026-03-28 10:00:00"]);
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(id_arr), Arc::new(ts_arr)]).unwrap();

        let hwm = extract_hwm_from_batch(&batch).unwrap();
        assert_eq!(hwm.last_id, 2);
        assert_eq!(hwm.updated_at, "2026-03-28 10:00:00");
    }

    #[test]
    fn extract_hwm_timestamp_millis() {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "updated_at",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
        ]));
        let id_arr = Int64Array::from(vec![1i64, 2i64]);
        let ts_arr = TimestampMillisecondArray::from(vec![1000i64, 2000i64]);
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(id_arr), Arc::new(ts_arr)]).unwrap();

        let hwm = extract_hwm_from_batch(&batch).unwrap();
        assert_eq!(hwm.last_id, 2);
    }

    #[test]
    fn extract_hwm_timestamp_seconds() {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "updated_at",
                DataType::Timestamp(TimeUnit::Second, None),
                false,
            ),
        ]));
        let id_arr = Int64Array::from(vec![1i64]);
        let ts_arr = TimestampSecondArray::from(vec![1743158400i64]);
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(id_arr), Arc::new(ts_arr)]).unwrap();

        let hwm = extract_hwm_from_batch(&batch).unwrap();
        assert_eq!(hwm.last_id, 1);
    }

    #[test]
    fn extract_hwm_descending_order() {
        let batch = make_batch_with_timestamps(
            vec![3, 2, 1],
            vec!["c", "b", "a"],
            vec![3000000i64, 2000000i64, 1000000i64],
        );
        let hwm = extract_hwm_from_batch(&batch).unwrap();
        assert_eq!(hwm.last_id, 3);
    }

    #[test]
    fn extract_hwm_same_ts_descending_id() {
        let batch = make_batch_with_timestamps(
            vec![30, 20, 10],
            vec!["c", "b", "a"],
            vec![5000000i64, 5000000i64, 5000000i64],
        );
        let hwm = extract_hwm_from_batch(&batch).unwrap();
        assert_eq!(hwm.last_id, 30);
    }

    #[test]
    fn delta_writer_new_builds_storage_options() {
        let writer = DeltaWriter::new(
            "my-bucket",
            "parket",
            Some("http://localhost:9000"),
            "us-east-1",
            "minioadmin",
            "minioadmin",
        );

        assert_eq!(writer.bucket, "my-bucket");
        assert_eq!(writer.prefix, "parket");
        assert_eq!(
            writer.storage_options.get("AWS_REGION"),
            Some(&"us-east-1".to_string())
        );
        assert_eq!(
            writer.storage_options.get("AWS_ACCESS_KEY_ID"),
            Some(&"minioadmin".to_string())
        );
        assert_eq!(
            writer.storage_options.get("AWS_SECRET_ACCESS_KEY"),
            Some(&"minioadmin".to_string())
        );
        assert_eq!(
            writer.storage_options.get("AWS_ENDPOINT_URL"),
            Some(&"http://localhost:9000".to_string())
        );
        assert_eq!(
            writer.storage_options.get("AWS_ALLOW_HTTP"),
            Some(&"true".to_string())
        );
    }

    #[test]
    fn delta_writer_new_no_endpoint() {
        let writer = DeltaWriter::new(
            "bucket",
            "prefix",
            None,
            "eu-west-1",
            "key",
            "secret",
        );

        assert!(writer.storage_options.get("AWS_ENDPOINT_URL").is_none());
    }

    #[test]
    fn table_url_format() {
        let writer = DeltaWriter::new(
            "data-lake",
            "parket",
            None,
            "us-east-1",
            "key",
            "secret",
        );

        let url = writer.table_url("orders").unwrap();
        assert_eq!(url.as_str(), "s3://data-lake/parket/orders/");
    }

    #[test]
    fn table_url_custom_prefix() {
        let writer = DeltaWriter::new(
            "bucket",
            "custom-prefix",
            None,
            "us-east-1",
            "key",
            "secret",
        );

        let url = writer.table_url("customers").unwrap();
        assert_eq!(url.as_str(), "s3://bucket/custom-prefix/customers/");
    }

    #[test]
    fn arrow_schema_to_delta_basic() {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("price", DataType::Float64, true),
        ]));

        let delta_schema = arrow_schema_to_delta(&schema).unwrap();

        let field_names: Vec<String> = delta_schema.fields().map(|f| f.name().clone()).collect();
        assert_eq!(field_names, vec!["id", "name", "price"]);
    }

    #[test]
    fn arrow_type_to_delta_conversions() {
        assert!(matches!(
            arrow_type_to_delta(&DataType::Boolean),
            Ok(deltalake::kernel::DataType::BOOLEAN)
        ));
        assert!(matches!(
            arrow_type_to_delta(&DataType::Int32),
            Ok(deltalake::kernel::DataType::INTEGER)
        ));
        assert!(matches!(
            arrow_type_to_delta(&DataType::Int64),
            Ok(deltalake::kernel::DataType::LONG)
        ));
        assert!(matches!(
            arrow_type_to_delta(&DataType::Float32),
            Ok(deltalake::kernel::DataType::FLOAT)
        ));
        assert!(matches!(
            arrow_type_to_delta(&DataType::Float64),
            Ok(deltalake::kernel::DataType::DOUBLE)
        ));
        assert!(matches!(
            arrow_type_to_delta(&DataType::Utf8),
            Ok(deltalake::kernel::DataType::STRING)
        ));
        assert!(matches!(
            arrow_type_to_delta(&DataType::Date32),
            Ok(deltalake::kernel::DataType::DATE)
        ));
        assert!(matches!(
            arrow_type_to_delta(&DataType::Timestamp(TimeUnit::Microsecond, None)),
            Ok(deltalake::kernel::DataType::TIMESTAMP)
        ));
    }

    #[test]
    fn arrow_type_to_delta_unsupported() {
        let result = arrow_type_to_delta(&DataType::List(Arc::new(Field::new(
            "item",
            DataType::Int32,
            true,
        ))));
        assert!(result.is_err());
    }

    #[test]
    fn format_naive_datetime_basic() {
        let result = format_naive_datetime(0, 0);
        assert_eq!(result, "1970-01-01 00:00:00");
    }

    #[test]
    fn format_naive_datetime_with_subsec() {
        let result = format_naive_datetime(0, 500_000_000);
        assert_eq!(result, "1970-01-01 00:00:00.5");
    }

    #[test]
    fn format_naive_datetime_known_date() {
        let result = format_naive_datetime(1743158400, 0);
        assert!(result.starts_with("2025-"));
    }

    #[test]
    fn micros_to_string_conversion() {
        let result = micros_to_string(1743158400000000i64);
        assert!(result.contains("2025"));
    }

    #[test]
    fn millis_to_string_conversion() {
        let result = millis_to_string(1743158400000i64);
        assert!(result.contains("2025"));
    }

    #[test]
    fn secs_to_string_conversion() {
        let result = secs_to_string(1743158400i64);
        assert!(result.contains("2025"));
    }

    #[test]
    fn epoch_days_to_ymd_epoch() {
        let (y, m, d) = epoch_days_to_ymd(0);
        assert_eq!((y, m, d), (1970, 1, 1));
    }

    #[test]
    fn epoch_days_to_ymd_known_date() {
        let (y, m, d) = epoch_days_to_ymd(365);
        assert_eq!((y, m, d), (1971, 1, 1));
    }

    #[test]
    fn epoch_days_to_ymd_negative_day() {
        let (y, m, d) = epoch_days_to_ymd(-1);
        assert_eq!((y, m, d), (1969, 12, 31));
    }

    #[test]
    fn epoch_days_to_ymd_negative_large() {
        let (y, m, d) = epoch_days_to_ymd(-365);
        assert_eq!((y, m, d), (1969, 1, 1));
    }

    #[test]
    fn epoch_days_to_ymd_month_boundary() {
        let (y, m, d) = epoch_days_to_ymd(31);
        assert_eq!((y, m, d), (1970, 2, 1));
    }

    #[test]
    fn epoch_days_to_ymd_leap_year_1972() {
        let days_to_1972_0203 = 365 + 365 + 33;
        let (y, m, d) = epoch_days_to_ymd(days_to_1972_0203);
        assert_eq!((y, m, d), (1972, 2, 3));
    }

    #[test]
    fn is_leap_true_div4() {
        assert!(is_leap(2024));
    }

    #[test]
    fn is_leap_true_div400() {
        assert!(is_leap(2000));
    }

    #[test]
    fn is_leap_false_div100() {
        assert!(!is_leap(1900));
    }

    #[test]
    fn is_leap_false_normal() {
        assert!(!is_leap(2023));
    }

    #[test]
    fn format_naive_datetime_negative_secs() {
        let result = format_naive_datetime(-86400, 0);
        assert_eq!(result, "1969-12-31 00:00:00");
    }

    #[test]
    fn format_naive_datetime_negative_secs_with_subsec() {
        let result = format_naive_datetime(-1, 500_000_000);
        assert_eq!(result, "1969-12-31 23:59:59.5");
    }

    #[test]
    fn micros_to_string_negative() {
        let result = micros_to_string(-1_000_000);
        assert!(result.contains("1969"));
    }

    #[test]
    fn millis_to_string_negative() {
        let result = millis_to_string(-1000);
        assert!(result.contains("1969"));
    }

    #[test]
    fn secs_to_string_negative() {
        let result = secs_to_string(-1);
        assert!(result.contains("1969"));
    }

    #[test]
    fn build_commit_properties_with_hwm() {
        let hwm = Hwm {
            updated_at: "2026-03-28 10:00:00".to_string(),
            last_id: 42,
        };
        let _props = build_commit_properties(Some(&hwm));
    }

    #[test]
    fn build_commit_properties_without_hwm() {
        let props = build_commit_properties(None);
        let _ = props;
    }

    #[test]
    fn extract_hwm_timestamp_micros_with_null() {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "updated_at",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
        ]));
        let id_arr = Int64Array::from(vec![1i64, 2i64, 3i64]);
        let ts_arr = TimestampMicrosecondArray::from(vec![
            Some(1000000i64),
            None,
            Some(3000000i64),
        ]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(id_arr), Arc::new(ts_arr)]).unwrap();

        let hwm = extract_hwm_from_batch(&batch).unwrap();
        assert_eq!(hwm.last_id, 3);
    }

    #[test]
    fn extract_hwm_timestamp_millis_with_null() {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "updated_at",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                true,
            ),
        ]));
        let id_arr = Int64Array::from(vec![1i64, 2i64]);
        let ts_arr = TimestampMillisecondArray::from(vec![Some(1000i64), None]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(id_arr), Arc::new(ts_arr)]).unwrap();

        let hwm = extract_hwm_from_batch(&batch).unwrap();
        assert_eq!(hwm.last_id, 1);
    }

    #[test]
    fn extract_hwm_timestamp_seconds_with_null() {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "updated_at",
                DataType::Timestamp(TimeUnit::Second, None),
                true,
            ),
        ]));
        let id_arr = Int64Array::from(vec![1i64, 2i64]);
        let ts_arr = TimestampSecondArray::from(vec![None, Some(2000i64)]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(id_arr), Arc::new(ts_arr)]).unwrap();

        let hwm = extract_hwm_from_batch(&batch).unwrap();
        assert_eq!(hwm.last_id, 2);
    }

    #[test]
    fn extract_hwm_unsupported_timestamp_type_returns_none() {
        use deltalake::arrow::array::Float64Array;
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("updated_at", DataType::Float64, false),
        ]));
        let id_arr = Int64Array::from(vec![1i64]);
        let ts_arr = Float64Array::from(vec![1.0f64]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(id_arr), Arc::new(ts_arr)]).unwrap();

        assert!(extract_hwm_from_batch(&batch).is_none());
    }

    #[test]
    fn extract_hwm_non_int64_id_returns_none() {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "updated_at",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
        ]));
        let id_arr = deltalake::arrow::array::Int32Array::from(vec![1i32]);
        let ts_arr = TimestampMicrosecondArray::from(vec![1000000i64]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(id_arr), Arc::new(ts_arr)]).unwrap();

        assert!(extract_hwm_from_batch(&batch).is_none());
    }

    #[test]
    fn arrow_type_to_delta_int8() {
        assert!(matches!(
            arrow_type_to_delta(&DataType::Int8),
            Ok(deltalake::kernel::DataType::INTEGER)
        ));
    }

    #[test]
    fn arrow_type_to_delta_int16() {
        assert!(matches!(
            arrow_type_to_delta(&DataType::Int16),
            Ok(deltalake::kernel::DataType::INTEGER)
        ));
    }

    #[test]
    fn arrow_type_to_delta_uint8() {
        assert!(matches!(
            arrow_type_to_delta(&DataType::UInt8),
            Ok(deltalake::kernel::DataType::INTEGER)
        ));
    }

    #[test]
    fn arrow_type_to_delta_uint16() {
        assert!(matches!(
            arrow_type_to_delta(&DataType::UInt16),
            Ok(deltalake::kernel::DataType::INTEGER)
        ));
    }

    #[test]
    fn arrow_type_to_delta_uint32() {
        assert!(matches!(
            arrow_type_to_delta(&DataType::UInt32),
            Ok(deltalake::kernel::DataType::INTEGER)
        ));
    }

    #[test]
    fn arrow_type_to_delta_uint64() {
        assert!(matches!(
            arrow_type_to_delta(&DataType::UInt64),
            Ok(deltalake::kernel::DataType::LONG)
        ));
    }

    #[test]
    fn arrow_type_to_delta_float16() {
        assert!(matches!(
            arrow_type_to_delta(&DataType::Float16),
            Ok(deltalake::kernel::DataType::FLOAT)
        ));
    }

    #[test]
    fn arrow_type_to_delta_large_utf8() {
        assert!(matches!(
            arrow_type_to_delta(&DataType::LargeUtf8),
            Ok(deltalake::kernel::DataType::STRING)
        ));
    }

    #[test]
    fn arrow_type_to_delta_binary() {
        assert!(matches!(
            arrow_type_to_delta(&DataType::Binary),
            Ok(deltalake::kernel::DataType::BINARY)
        ));
    }

    #[test]
    fn arrow_type_to_delta_large_binary() {
        assert!(matches!(
            arrow_type_to_delta(&DataType::LargeBinary),
            Ok(deltalake::kernel::DataType::BINARY)
        ));
    }

    #[test]
    fn arrow_type_to_delta_date64() {
        assert!(matches!(
            arrow_type_to_delta(&DataType::Date64),
            Ok(deltalake::kernel::DataType::DATE)
        ));
    }

    #[test]
    fn arrow_type_to_delta_timestamp_millis() {
        assert!(matches!(
            arrow_type_to_delta(&DataType::Timestamp(TimeUnit::Millisecond, None)),
            Ok(deltalake::kernel::DataType::TIMESTAMP)
        ));
    }

    #[test]
    fn arrow_type_to_delta_timestamp_second() {
        assert!(matches!(
            arrow_type_to_delta(&DataType::Timestamp(TimeUnit::Second, None)),
            Ok(deltalake::kernel::DataType::TIMESTAMP)
        ));
    }

    #[test]
    fn arrow_type_to_delta_timestamp_nanos() {
        assert!(matches!(
            arrow_type_to_delta(&DataType::Timestamp(TimeUnit::Nanosecond, None)),
            Ok(deltalake::kernel::DataType::TIMESTAMP)
        ));
    }

    #[test]
    fn arrow_type_to_delta_decimal128() {
        let result = arrow_type_to_delta(&DataType::Decimal128(10, 2));
        assert!(result.is_ok());
    }

    #[test]
    fn arrow_type_to_delta_decimal256() {
        let result = arrow_type_to_delta(&DataType::Decimal256(10, 2));
        assert!(result.is_ok());
    }

    #[test]
    fn arrow_type_to_delta_decimal_invalid_precision() {
        let result = arrow_type_to_delta(&DataType::Decimal128(0, 0));
        assert!(result.is_err());
    }

    #[test]
    fn arrow_schema_to_delta_unsupported_type() {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("data", DataType::List(Arc::new(Field::new("item", DataType::Int32, true))), false),
        ]));
        let result = arrow_schema_to_delta(&schema);
        assert!(result.is_err());
    }

    #[test]
    fn extract_hwm_string_timestamp_with_null() {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("updated_at", DataType::Utf8, true),
        ]));
        let id_arr = Int64Array::from(vec![1i64, 2i64, 3i64]);
        let ts_arr = StringArray::from(vec![
            Some("2026-03-28 09:00:00"),
            None,
            Some("2026-03-28 11:00:00"),
        ]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(id_arr), Arc::new(ts_arr)]).unwrap();

        let hwm = extract_hwm_from_batch(&batch).unwrap();
        assert_eq!(hwm.last_id, 3);
        assert_eq!(hwm.updated_at, "2026-03-28 11:00:00");
    }

    #[test]
    fn format_naive_datetime_trailing_zeros() {
        let result = format_naive_datetime(0, 123_456_000);
        assert_eq!(result, "1970-01-01 00:00:00.123456");
    }

    #[test]
    fn format_naive_datetime_zero_subsec_nanos() {
        let result = format_naive_datetime(0, 0);
        assert_eq!(result, "1970-01-01 00:00:00");
    }

    #[test]
    fn epoch_days_to_ymd_year_boundary() {
        let (y, m, d) = epoch_days_to_ymd(730);
        assert_eq!((y, m, d), (1972, 1, 1));
    }

    #[test]
    fn delta_writer_table_url_with_special_chars() {
        let writer = DeltaWriter::new(
            "my-bucket",
            "parket",
            None,
            "us-east-1",
            "key",
            "secret",
        );
        let url = writer.table_url("my_table").unwrap();
        assert!(url.as_str().contains("my_table"));
    }

    #[test]
    fn new_local_creates_writer() {
        let writer = DeltaWriter::new_local("/tmp/test");
        assert!(writer.use_local_fs);
        assert_eq!(writer.prefix, "/tmp/test");
        assert!(writer.storage_options.is_empty());
    }

    #[test]
    fn new_local_table_url_format() {
        let writer = DeltaWriter::new_local("/tmp/delta");
        let url = writer.table_url("orders").unwrap();
        assert!(url.as_str().starts_with("file:///"));
        assert!(url.as_str().contains("orders"));
    }

    #[tokio::test]
    async fn ensure_table_creates_new_table() {
        let temp = tempfile::tempdir().unwrap();
        let writer = DeltaWriter::new_local(temp.path().to_str().unwrap());
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        let table = writer.ensure_table("test_table", schema).await.unwrap();
        let files: Vec<_> = table.get_file_uris().unwrap().collect();
        assert!(files.is_empty());
    }

    #[tokio::test]
    async fn ensure_table_existing_table_returns_same() {
        let temp = tempfile::tempdir().unwrap();
        let writer = DeltaWriter::new_local(temp.path().to_str().unwrap());
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
        ]));

        writer
            .ensure_table("test_table", schema.clone())
            .await
            .unwrap();
        let table = writer.ensure_table("test_table", schema).await.unwrap();
        let files: Vec<_> = table.get_file_uris().unwrap().collect();
        assert!(files.is_empty());
    }

    #[tokio::test]
    async fn append_batch_writes_data_with_hwm() {
        let temp = tempfile::tempdir().unwrap();
        let writer = DeltaWriter::new_local(temp.path().to_str().unwrap());
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Utf8, false),
        ]));

        writer
            .ensure_table("test_table", schema.clone())
            .await
            .unwrap();

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1i64, 2i64])),
                Arc::new(StringArray::from(vec!["a", "b"])),
            ],
        )
        .unwrap();

        let hwm = Hwm {
            updated_at: "2026-03-28 10:00:00".to_string(),
            last_id: 2,
        };
        writer
            .append_batch("test_table", vec![batch], Some(&hwm))
            .await
            .unwrap();

        let table = writer.open_table("test_table").await.unwrap();
        let files: Vec<_> = table.get_file_uris().unwrap().collect();
        assert_eq!(files.len(), 1);
    }

    #[tokio::test]
    async fn append_batch_empty_vec_is_noop() {
        let temp = tempfile::tempdir().unwrap();
        let writer = DeltaWriter::new_local(temp.path().to_str().unwrap());
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
        ]));

        writer
            .ensure_table("test_table", schema)
            .await
            .unwrap();
        writer
            .append_batch("test_table", vec![], None)
            .await
            .unwrap();

        let table = writer.open_table("test_table").await.unwrap();
        let files: Vec<_> = table.get_file_uris().unwrap().collect();
        assert!(files.is_empty());
    }

    #[tokio::test]
    async fn overwrite_table_replaces_all_data() {
        let temp = tempfile::tempdir().unwrap();
        let writer = DeltaWriter::new_local(temp.path().to_str().unwrap());
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
        ]));

        writer
            .ensure_table("test_table", schema.clone())
            .await
            .unwrap();

        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1i64]))],
        )
        .unwrap();
        writer
            .append_batch("test_table", vec![batch1], None)
            .await
            .unwrap();

        let batch2 = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![2i64, 3i64]))],
        )
        .unwrap();
        writer
            .overwrite_table("test_table", vec![batch2], None)
            .await
            .unwrap();

        let table = writer.open_table("test_table").await.unwrap();
        let files: Vec<_> = table.get_file_uris().unwrap().collect();
        assert_eq!(files.len(), 1);
    }

    #[tokio::test]
    async fn overwrite_table_empty_vec_is_noop() {
        let temp = tempfile::tempdir().unwrap();
        let writer = DeltaWriter::new_local(temp.path().to_str().unwrap());
        writer
            .overwrite_table("test_table", vec![], None)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn read_hwm_none_for_nonexistent_table() {
        let temp = tempfile::tempdir().unwrap();
        let writer = DeltaWriter::new_local(temp.path().to_str().unwrap());

        let hwm = writer.read_hwm("nonexistent").await.unwrap();
        assert!(hwm.is_none());
    }

    #[tokio::test]
    async fn read_hwm_none_for_table_without_hwm_metadata() {
        let temp = tempfile::tempdir().unwrap();
        let writer = DeltaWriter::new_local(temp.path().to_str().unwrap());
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
        ]));

        writer
            .ensure_table("test_table", schema.clone())
            .await
            .unwrap();

        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![1i64]))],
        )
        .unwrap();
        writer
            .append_batch("test_table", vec![batch], None)
            .await
            .unwrap();

        let hwm = writer.read_hwm("test_table").await.unwrap();
        assert!(hwm.is_none());
    }

    #[tokio::test]
    async fn read_hwm_returns_hwm_after_append() {
        let temp = tempfile::tempdir().unwrap();
        let writer = DeltaWriter::new_local(temp.path().to_str().unwrap());
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
        ]));

        writer
            .ensure_table("test_table", schema.clone())
            .await
            .unwrap();

        let hwm = Hwm {
            updated_at: "2026-03-28 10:00:00".to_string(),
            last_id: 42,
        };
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![1i64]))],
        )
        .unwrap();
        writer
            .append_batch("test_table", vec![batch], Some(&hwm))
            .await
            .unwrap();

        let read_back = writer.read_hwm("test_table").await.unwrap().unwrap();
        assert_eq!(read_back.updated_at, "2026-03-28 10:00:00");
        assert_eq!(read_back.last_id, 42);
    }

    #[tokio::test]
    async fn read_hwm_returns_latest_after_multiple_appends() {
        let temp = tempfile::tempdir().unwrap();
        let writer = DeltaWriter::new_local(temp.path().to_str().unwrap());
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
        ]));

        writer
            .ensure_table("test_table", schema.clone())
            .await
            .unwrap();

        let hwm1 = Hwm {
            updated_at: "2026-03-28 09:00:00".to_string(),
            last_id: 10,
        };
        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1i64]))],
        )
        .unwrap();
        writer
            .append_batch("test_table", vec![batch1], Some(&hwm1))
            .await
            .unwrap();

        let hwm2 = Hwm {
            updated_at: "2026-03-28 10:00:00".to_string(),
            last_id: 20,
        };
        let batch2 = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![2i64]))],
        )
        .unwrap();
        writer
            .append_batch("test_table", vec![batch2], Some(&hwm2))
            .await
            .unwrap();

        let read_back = writer.read_hwm("test_table").await.unwrap().unwrap();
        assert_eq!(read_back.updated_at, "2026-03-28 10:00:00");
        assert_eq!(read_back.last_id, 20);
    }

    #[tokio::test]
    async fn overwrite_with_hwm_stores_hwm() {
        let temp = tempfile::tempdir().unwrap();
        let writer = DeltaWriter::new_local(temp.path().to_str().unwrap());
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
        ]));

        writer
            .ensure_table("test_table", schema.clone())
            .await
            .unwrap();

        let hwm = Hwm {
            updated_at: "2026-03-28 12:00:00".to_string(),
            last_id: 99,
        };
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![1i64, 2i64]))],
        )
        .unwrap();
        writer
            .overwrite_table("test_table", vec![batch], Some(&hwm))
            .await
            .unwrap();

        let read_back = writer.read_hwm("test_table").await.unwrap().unwrap();
        assert_eq!(read_back.updated_at, "2026-03-28 12:00:00");
        assert_eq!(read_back.last_id, 99);
    }

    #[tokio::test]
    async fn ensure_table_s3_connection_error() {
        let writer = DeltaWriter::new(
            "nonexistent-bucket",
            "prefix",
            Some("http://localhost:1"),
            "us-east-1",
            "fake",
            "fake",
        );
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
        ]));

        let result = writer.ensure_table("test_table", schema).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn read_hwm_s3_error_returns_none() {
        let writer = DeltaWriter::new(
            "nonexistent-bucket",
            "prefix",
            Some("http://localhost:1"),
            "us-east-1",
            "fake",
            "fake",
        );

        let hwm = writer.read_hwm("nonexistent").await.unwrap();
        assert!(hwm.is_none());
    }
}
