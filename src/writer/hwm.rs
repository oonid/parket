use std::collections::HashMap;

use deltalake::arrow::array::{
    Array, Int8Array, Int16Array, Int32Array, Int64Array, UInt8Array, UInt16Array, UInt32Array,
    UInt64Array,
};
use deltalake::arrow::record_batch::RecordBatch;

use super::datetime::extract_timestamp_as_strings;
use super::Hwm;

pub fn extract_hwm_from_batch(batch: &RecordBatch, timestamp_col: &str, key_col: &str) -> Option<Hwm> {
    let timestamp_col_data = batch.column_by_name(timestamp_col)?;
    let key_col_data = batch.column_by_name(key_col)?;

    let n = batch.num_rows();
    if n == 0 {
        return None;
    }

    let timestamp_strings = extract_timestamp_as_strings(timestamp_col_data)?;
    let ids = extract_id_as_i64(key_col_data)?;

    // Build candidate list filtering out empty (NULL) timestamps
    let candidates: Vec<(usize, &str, i64)> = timestamp_strings
        .iter()
        .enumerate()
        .filter(|(_, ts)| !ts.is_empty())
        .map(|(i, ts)| (i, ts.as_str(), ids[i]))
        .collect();

    if candidates.is_empty() {
        return None;
    }

    // Find max by (ts, id)
    let (_, max_ts, max_id) = candidates.iter().max_by(|a, b| {
        match a.1.cmp(b.1) {
            std::cmp::Ordering::Equal => a.2.cmp(&b.2),
            other => other,
        }
    })?;

    Some(Hwm {
        updated_at: max_ts.to_string(),
        last_id: *max_id,
    })
}

pub fn hwm_has_advanced(current: Option<&Hwm>, next: &Hwm) -> bool {
    match current {
        None => true,
        Some(current) => {
            next.updated_at > current.updated_at
                || (next.updated_at == current.updated_at && next.last_id > current.last_id)
        }
    }
}

/// Max integer key in a batch — the insert-stream watermark. `key_col` is the
/// monotonic PK (e.g. `id`). None for an empty batch or unreadable column.
pub fn extract_max_id(batch: &RecordBatch, key_col: &str) -> Option<i64> {
    let col = batch.column_by_name(key_col)?;
    let ids = extract_id_as_i64(col)?;
    ids.into_iter().max()
}

// connector_arrow maps TINYINT → Int8, SMALLINT → Int16, INT/MEDIUMINT → Int32,
// BIGINT → Int64, and the UNSIGNED variants to the matching UInt* type. All but
// BIGINT UNSIGNED fit safely in i64; BIGINT UNSIGNED is checked and returns None
// on overflow past i64::MAX rather than silently wrapping negative.
pub(crate) fn extract_id_as_i64(col: &std::sync::Arc<dyn Array>) -> Option<Vec<i64>> {
    if let Some(a) = col.as_any().downcast_ref::<Int64Array>() {
        return Some((0..a.len()).map(|i| a.value(i)).collect());
    }
    if let Some(a) = col.as_any().downcast_ref::<Int32Array>() {
        return Some((0..a.len()).map(|i| a.value(i) as i64).collect());
    }
    if let Some(a) = col.as_any().downcast_ref::<Int16Array>() {
        return Some((0..a.len()).map(|i| i64::from(a.value(i))).collect());
    }
    if let Some(a) = col.as_any().downcast_ref::<Int8Array>() {
        return Some((0..a.len()).map(|i| i64::from(a.value(i))).collect());
    }
    if let Some(a) = col.as_any().downcast_ref::<UInt64Array>() {
        let mut out = Vec::with_capacity(a.len());
        for i in 0..a.len() {
            out.push(i64::try_from(a.value(i)).ok()?);
        }
        return Some(out);
    }
    if let Some(a) = col.as_any().downcast_ref::<UInt32Array>() {
        return Some((0..a.len()).map(|i| i64::from(a.value(i))).collect());
    }
    if let Some(a) = col.as_any().downcast_ref::<UInt16Array>() {
        return Some((0..a.len()).map(|i| i64::from(a.value(i))).collect());
    }
    if let Some(a) = col.as_any().downcast_ref::<UInt8Array>() {
        return Some((0..a.len()).map(|i| i64::from(a.value(i))).collect());
    }
    None
}

pub(crate) fn build_commit_properties(hwm: Option<&Hwm>) -> deltalake::kernel::transaction::CommitProperties {
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

pub(crate) fn build_two_stream_commit_properties(
    insert_id: Option<i64>,
    update: Option<&Hwm>,
) -> deltalake::kernel::transaction::CommitProperties {
    let mut metadata = HashMap::new();
    if let Some(id) = insert_id {
        metadata.insert("hwm_insert_id".to_string(), serde_json::Value::String(id.to_string()));
    }
    if let Some(h) = update {
        metadata.insert("hwm_updated_at".to_string(), serde_json::Value::String(h.updated_at.clone()));
        metadata.insert("hwm_last_id".to_string(), serde_json::Value::String(h.last_id.to_string()));
    }
    deltalake::kernel::transaction::CommitProperties::default().with_metadata(metadata)
}

#[cfg(test)]
mod tests {
    use super::*;
    use deltalake::arrow::array::{Int32Array, Int64Array, StringArray, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampSecondArray};
    use deltalake::arrow::datatypes::{DataType, Schema as ArrowSchema, TimeUnit, Field};
    use deltalake::arrow::record_batch::RecordBatch;
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
        let hwm = extract_hwm_from_batch(&batch, "updated_at", "id").unwrap();
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
        let hwm = extract_hwm_from_batch(&batch, "updated_at", "id").unwrap();
        assert_eq!(hwm.last_id, 2);
    }

    #[test]
    fn extract_hwm_same_timestamp_picks_max_id() {
        let batch = make_batch_with_timestamps(
            vec![10, 50, 30],
            vec!["a", "b", "c"],
            vec![5000000i64, 5000000i64, 5000000i64],
        );
        let hwm = extract_hwm_from_batch(&batch, "updated_at", "id").unwrap();
        assert_eq!(hwm.last_id, 50);
    }

    #[test]
    fn extract_hwm_int32_id_column() {
        // connector_arrow maps INT (not BIGINT) to Int32Array — must not return None
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("updated_at", DataType::Utf8, false),
        ]));
        let id_arr = Int32Array::from(vec![10i32, 20i32, 5i32]);
        let ts_arr = StringArray::from(vec!["2026-01-01T00:00:01.000000", "2026-01-01T00:00:03.000000", "2026-01-01T00:00:02.000000"]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(id_arr), Arc::new(ts_arr)]).unwrap();
        let hwm = extract_hwm_from_batch(&batch, "updated_at", "id").expect("Int32 id must produce a HWM");
        assert_eq!(hwm.last_id, 20);
        assert!(hwm.updated_at.contains("00:03"));
    }

    #[test]
    fn extract_hwm_utf8_timestamp_connector_arrow_format() {
        // connector_arrow returns datetime as Utf8 "YYYY-MM-DDTHH:MM:SS.ffffff"
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("updated_at", DataType::Utf8, false),
        ]));
        let id_arr = Int64Array::from(vec![1i64, 2i64, 3i64]);
        let ts_arr = StringArray::from(vec![
            "2026-06-07T12:00:00.000000",
            "2026-06-07T13:00:00.000000",
            "2026-06-07T12:30:00.000000",
        ]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(id_arr), Arc::new(ts_arr)]).unwrap();
        let hwm = extract_hwm_from_batch(&batch, "updated_at", "id").expect("Utf8 timestamp must produce a HWM");
        assert_eq!(hwm.last_id, 2);
        assert_eq!(hwm.updated_at, "2026-06-07T13:00:00.000000");
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

        assert!(extract_hwm_from_batch(&batch, "updated_at", "id").is_none());
    }

    #[test]
    fn extract_hwm_missing_updated_at_returns_none() {
        let batch = make_batch_no_updated_at();
        assert!(extract_hwm_from_batch(&batch, "updated_at", "id").is_none());
    }

    #[test]
    fn extract_hwm_missing_id_returns_none() {
        let batch = make_batch_no_id();
        assert!(extract_hwm_from_batch(&batch, "updated_at", "id").is_none());
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

        let hwm = extract_hwm_from_batch(&batch, "updated_at", "id").unwrap();
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

        let hwm = extract_hwm_from_batch(&batch, "updated_at", "id").unwrap();
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

        let hwm = extract_hwm_from_batch(&batch, "updated_at", "id").unwrap();
        assert_eq!(hwm.last_id, 1);
    }

    #[test]
    fn extract_hwm_descending_order() {
        let batch = make_batch_with_timestamps(
            vec![3, 2, 1],
            vec!["c", "b", "a"],
            vec![3000000i64, 2000000i64, 1000000i64],
        );
        let hwm = extract_hwm_from_batch(&batch, "updated_at", "id").unwrap();
        assert_eq!(hwm.last_id, 3);
    }

    #[test]
    fn extract_hwm_same_ts_descending_id() {
        let batch = make_batch_with_timestamps(
            vec![30, 20, 10],
            vec!["c", "b", "a"],
            vec![5000000i64, 5000000i64, 5000000i64],
        );
        let hwm = extract_hwm_from_batch(&batch, "updated_at", "id").unwrap();
        assert_eq!(hwm.last_id, 30);
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

        let hwm = extract_hwm_from_batch(&batch, "updated_at", "id").unwrap();
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

        let hwm = extract_hwm_from_batch(&batch, "updated_at", "id").unwrap();
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

        let hwm = extract_hwm_from_batch(&batch, "updated_at", "id").unwrap();
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

        assert!(extract_hwm_from_batch(&batch, "updated_at", "id").is_none());
    }

    #[test]
    fn extract_hwm_int32_id_returns_hwm() {
        // INT (not BIGINT) maps to Int32 in connector_arrow — must succeed
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "updated_at",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
        ]));
        let id_arr = Int32Array::from(vec![1i32]);
        let ts_arr = TimestampMicrosecondArray::from(vec![1000000i64]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(id_arr), Arc::new(ts_arr)]).unwrap();

        let hwm = extract_hwm_from_batch(&batch, "updated_at", "id").expect("Int32 id must produce a HWM");
        assert_eq!(hwm.last_id, 1);
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

        let hwm = extract_hwm_from_batch(&batch, "updated_at", "id").unwrap();
        assert_eq!(hwm.last_id, 3);
        assert_eq!(hwm.updated_at, "2026-03-28 11:00:00");
    }

    #[test]
    fn extract_hwm_custom_timestamp_col() {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("completed_at", DataType::Utf8, false),
        ]));
        let id_arr = Int64Array::from(vec![1i64, 2i64, 3i64]);
        let ts_arr = StringArray::from(vec![
            "2026-01-01 10:00:00",
            "2026-01-01 11:00:00",
            "2026-01-01 12:00:00",
        ]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(id_arr), Arc::new(ts_arr)]).unwrap();

        let hwm = extract_hwm_from_batch(&batch, "completed_at", "id").unwrap();
        assert_eq!(hwm.last_id, 3);
        assert_eq!(hwm.updated_at, "2026-01-01 12:00:00");
    }

    #[test]
    fn extract_hwm_custom_key_column_name() {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("order_id", DataType::Int64, false),
            Field::new("updated_at", DataType::Utf8, false),
        ]));
        let key_arr = Int64Array::from(vec![10i64, 20i64, 15i64]);
        let ts_arr = StringArray::from(vec![
            "2026-01-01 10:00:00",
            "2026-01-01 12:00:00",
            "2026-01-01 11:00:00",
        ]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(key_arr), Arc::new(ts_arr)]).unwrap();

        let hwm = extract_hwm_from_batch(&batch, "updated_at", "order_id").unwrap();
        assert_eq!(hwm.last_id, 20);
        assert_eq!(hwm.updated_at, "2026-01-01 12:00:00");
    }

    #[test]
    fn extract_hwm_mixed_null_and_real_timestamps() {
        // Mixed NULL and real timestamps — should skip NULLs and find max non-NULL
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("updated_at", DataType::Utf8, true),
        ]));
        let id_arr = Int64Array::from(vec![1i64, 2i64, 3i64, 4i64]);
        let ts_arr = StringArray::from(vec![
            None,
            Some("2026-03-28 09:00:00"),
            Some("2026-03-28 11:00:00"),
            None,
        ]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(id_arr), Arc::new(ts_arr)]).unwrap();

        let hwm = extract_hwm_from_batch(&batch, "updated_at", "id").unwrap();
        assert_eq!(hwm.last_id, 3);
        assert_eq!(hwm.updated_at, "2026-03-28 11:00:00");
    }

    #[test]
    fn extract_hwm_all_null_timestamps_returns_none() {
        // All timestamps are NULL — should return None
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("updated_at", DataType::Utf8, true),
        ]));
        let id_arr = Int64Array::from(vec![1i64, 2i64, 3i64]);
        let ts_arr = StringArray::from(vec![None as Option<&str>, None, None]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(id_arr), Arc::new(ts_arr)]).unwrap();

        assert!(extract_hwm_from_batch(&batch, "updated_at", "id").is_none());
    }

    #[test]
    fn extract_max_id_int64_basic() {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
        ]));
        let id_arr = Int64Array::from(vec![3i64, 1i64, 2i64]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(id_arr)]).unwrap();
        let max_id = extract_max_id(&batch, "id");
        assert_eq!(max_id, Some(3));
    }

    #[test]
    fn extract_max_id_int32_column() {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
        ]));
        let id_arr = Int32Array::from(vec![3i32, 1i32, 2i32]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(id_arr)]).unwrap();
        let max_id = extract_max_id(&batch, "id");
        assert_eq!(max_id, Some(3));
    }

    #[test]
    fn extract_max_id_empty_batch() {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
        ]));
        let id_arr = Int64Array::from(Vec::<i64>::new());
        let batch = RecordBatch::try_new(schema, vec![Arc::new(id_arr)]).unwrap();
        let max_id = extract_max_id(&batch, "id");
        assert!(max_id.is_none());
    }

    #[test]
    fn extract_max_id_custom_key_column_name() {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("pk", DataType::Int64, false),
        ]));
        let id_arr = Int64Array::from(vec![10i64, 5i64, 15i64]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(id_arr)]).unwrap();
        let max_id = extract_max_id(&batch, "pk");
        assert_eq!(max_id, Some(15));
    }

    #[test]
    fn extract_max_id_missing_column() {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
        ]));
        let id_arr = Int64Array::from(vec![3i64, 1i64, 2i64]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(id_arr)]).unwrap();
        let max_id = extract_max_id(&batch, "nonexistent");
        assert!(max_id.is_none());
    }

    #[test]
    fn extract_max_id_int16_column() {
        // SMALLINT cursor — connector_arrow maps it to Int16Array (N2)
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int16, false),
        ]));
        let id_arr = deltalake::arrow::array::Int16Array::from(vec![3i16, 1i16, 2i16]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(id_arr)]).unwrap();
        let max_id = extract_max_id(&batch, "id");
        assert_eq!(max_id, Some(3));
    }

    #[test]
    fn extract_max_id_int8_column() {
        // TINYINT cursor — connector_arrow maps it to Int8Array (N2)
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int8, false),
        ]));
        let id_arr = deltalake::arrow::array::Int8Array::from(vec![3i8, 1i8, 2i8]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(id_arr)]).unwrap();
        let max_id = extract_max_id(&batch, "id");
        assert_eq!(max_id, Some(3));
    }

    #[test]
    fn extract_max_id_uint16_column() {
        // SMALLINT UNSIGNED cursor — UInt16Array (N2)
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::UInt16, false),
        ]));
        let id_arr = deltalake::arrow::array::UInt16Array::from(vec![3u16, 1u16, 2u16]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(id_arr)]).unwrap();
        let max_id = extract_max_id(&batch, "id");
        assert_eq!(max_id, Some(3));
    }

    #[test]
    fn extract_max_id_uint8_column() {
        // TINYINT UNSIGNED cursor — UInt8Array (N2)
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::UInt8, false),
        ]));
        let id_arr = deltalake::arrow::array::UInt8Array::from(vec![3u8, 1u8, 2u8]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(id_arr)]).unwrap();
        let max_id = extract_max_id(&batch, "id");
        assert_eq!(max_id, Some(3));
    }

    #[test]
    fn extract_id_as_i64_uint64_over_i64_max_returns_none() {
        // BIGINT UNSIGNED value past i64::MAX must not silently wrap negative (N5/N2);
        // extract_id_as_i64 must return None so callers hit the progress-guard bail
        // instead of corrupting the HWM.
        let huge = (i64::MAX as u64) + 1;
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::UInt64, false),
        ]));
        let id_arr = UInt64Array::from(vec![huge]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(id_arr)]).unwrap();
        assert!(extract_max_id(&batch, "id").is_none());
    }

    #[test]
    fn extract_id_as_i64_uint64_within_range_ok() {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::UInt64, false),
        ]));
        let id_arr = UInt64Array::from(vec![10u64, 5u64, 15u64]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(id_arr)]).unwrap();
        assert_eq!(extract_max_id(&batch, "id"), Some(15));
    }

    #[test]
    fn build_commit_properties_with_hwm() {
        let hwm = super::super::Hwm {
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
}
