use std::convert::TryFrom;
use std::sync::Arc;

use anyhow::Result;
use arrow::record_batch::RecordBatch;
use tracing::{debug, info, warn};

use connectorx::prelude::{new_record_batch_iter, CXQuery, RecordBatchIterator, SourceConn};

use deltalake::arrow::array::Array as V57ArrayTrait;
use deltalake::arrow::datatypes::{
    DataType as V57DataType, Field as V57Field, Schema as V57Schema, TimeUnit as V57TimeUnit,
};
use deltalake::arrow::record_batch::RecordBatch as V57RecordBatch;

const DEFAULT_BATCH_SIZE: u64 = 10000;

pub trait CxStreamer {
    fn prepare(&mut self);
    fn next_batch(&mut self) -> Option<RecordBatch>;
}

// TODO(task-11.2): integration tests with real MariaDB covering DefaultCxStreamer + extract()
struct DefaultCxStreamer {
    inner: Box<dyn RecordBatchIterator>,
}

impl DefaultCxStreamer {
    fn new(source_conn: &SourceConn, queries: &[CXQuery<String>], batch_size: usize) -> Self {
        Self {
            inner: new_record_batch_iter(source_conn, None, queries, batch_size, None),
        }
    }
}

impl CxStreamer for DefaultCxStreamer {
    fn prepare(&mut self) {
        self.inner.prepare();
    }

    fn next_batch(&mut self) -> Option<RecordBatch> {
        self.inner.next_batch()
    }
}

pub struct BatchExtractor {
    database_url: String,
    target_memory_mb: u64,
    default_batch_size: u64,
    batch_size: u64,
    adapted: bool,
}

impl BatchExtractor {
    pub fn new(database_url: &str, target_memory_mb: u64, default_batch_size: u64) -> Self {
        Self {
            database_url: database_url.to_string(),
            target_memory_mb,
            default_batch_size,
            batch_size: default_batch_size,
            adapted: false,
        }
    }

    pub fn calculate_batch_size(&mut self, avg_row_length: Option<u64>) -> u64 {
        match avg_row_length {
            Some(row_len) if row_len > 0 => {
                self.batch_size = (self.target_memory_mb * 1024 * 1024) / row_len;
                self.batch_size = self.batch_size.max(1);
            }
            _ => {
                self.batch_size = self.default_batch_size;
            }
        }
        debug!(
            batch_size = self.batch_size,
            avg_row_length = ?avg_row_length,
            "calculated batch size"
        );
        self.batch_size
    }

    pub fn extract(&mut self, sql: &str) -> Result<Vec<RecordBatch>> {
        let source_conn = SourceConn::try_from(self.database_url.as_str())
            .map_err(|e| anyhow::anyhow!("invalid database url: {e}"))?;
        let queries = &[CXQuery::from(sql)];
        let mut streamer = DefaultCxStreamer::new(&source_conn, queries, self.batch_size as usize);
        self.extract_from_stream(&mut streamer)
    }

    fn extract_from_stream<S: CxStreamer>(&mut self, streamer: &mut S) -> Result<Vec<RecordBatch>> {
        streamer.prepare();
        let mut batches = Vec::new();
        while let Some(batch) = streamer.next_batch() {
            let rows = batch.num_rows();
            info!(
                rows,
                arrow_bytes = batch.get_array_memory_size(),
                "batch extracted"
            );

            if !self.adapted && rows > 0 {
                self.adapt_after_first_batch(&batch);
                self.adapted = true;
            }

            self.enforce_hard_ceiling(&batch);

            if rows == 0 {
                break;
            }
            batches.push(batch);
        }

        Ok(batches)
    }

    fn adapt_after_first_batch(&mut self, batch: &RecordBatch) {
        let row_count = batch.num_rows();
        if row_count == 0 {
            return;
        }

        let actual_bytes = batch.get_array_memory_size() as u64;
        let actual_bytes_per_row = actual_bytes / row_count as u64;
        let target_bytes = self.target_memory_mb * 1024 * 1024;

        let estimated_bytes_per_row = if self.batch_size > 0 {
            target_bytes / self.batch_size
        } else {
            return;
        };

        let ratio = if estimated_bytes_per_row > 0 {
            actual_bytes_per_row as f64 / estimated_bytes_per_row as f64
        } else {
            return;
        };

        if !(0.5..=2.0).contains(&ratio) {
            let new_batch_size = target_bytes / actual_bytes_per_row.max(1);
            info!(
                old_batch_size = self.batch_size,
                new_batch_size,
                ratio = format!("{ratio:.2}"),
                "adaptive sizing: batch_size adjusted"
            );
            self.batch_size = new_batch_size.max(1);
        }
    }

    fn enforce_hard_ceiling(&mut self, batch: &RecordBatch) {
        let ceiling = self.target_memory_mb * 2 * 1024 * 1024;
        let mem = batch.get_array_memory_size() as u64;
        if mem > ceiling {
            let halved = self.batch_size / 2;
            warn!(
                batch_bytes = mem,
                ceiling_bytes = ceiling,
                old_batch_size = self.batch_size,
                new_batch_size = halved,
                "batch memory exceeds hard ceiling, halving batch_size"
            );
            self.batch_size = halved.max(1);
        }
    }

    pub fn batch_size(&self) -> u64 {
        self.batch_size
    }
}

// ---------------------------------------------------------------------------
// Arrow v54 → v57 FFI Conversion Functions
// ---------------------------------------------------------------------------
// connector-x is pinned to arrow v54. These standalone functions convert
// RecordBatches from v54 to v57 via the Arrow C Data Interface (zero-copy).
// When connector-x upgrades to v57, delete this entire section.
// See docs/arrow_v54_to_v57.md section B for design rationale.
// ---------------------------------------------------------------------------

pub fn convert_datatype(dt: &arrow::datatypes::DataType) -> Result<V57DataType> {
    use arrow::datatypes::{DataType, TimeUnit};
    match dt {
        DataType::Null => Ok(V57DataType::Null),
        DataType::Boolean => Ok(V57DataType::Boolean),
        DataType::Int8 => Ok(V57DataType::Int8),
        DataType::Int16 => Ok(V57DataType::Int16),
        DataType::Int32 => Ok(V57DataType::Int32),
        DataType::Int64 => Ok(V57DataType::Int64),
        DataType::UInt8 => Ok(V57DataType::UInt8),
        DataType::UInt16 => Ok(V57DataType::UInt16),
        DataType::UInt32 => Ok(V57DataType::UInt32),
        DataType::UInt64 => Ok(V57DataType::UInt64),
        DataType::Float16 => Ok(V57DataType::Float16),
        DataType::Float32 => Ok(V57DataType::Float32),
        DataType::Float64 => Ok(V57DataType::Float64),
        DataType::Utf8 => Ok(V57DataType::Utf8),
        DataType::LargeUtf8 => Ok(V57DataType::LargeUtf8),
        DataType::Binary => Ok(V57DataType::Binary),
        DataType::LargeBinary => Ok(V57DataType::LargeBinary),
        DataType::Date32 => Ok(V57DataType::Date32),
        DataType::Date64 => Ok(V57DataType::Date64),
        DataType::Timestamp(unit, tz) => {
            let v57_unit = match unit {
                TimeUnit::Second => V57TimeUnit::Second,
                TimeUnit::Millisecond => V57TimeUnit::Millisecond,
                TimeUnit::Microsecond => V57TimeUnit::Microsecond,
                TimeUnit::Nanosecond => V57TimeUnit::Nanosecond,
            };
            let v57_tz = tz.as_deref().map(|s| Arc::<str>::from(s.to_string()));
            Ok(V57DataType::Timestamp(v57_unit, v57_tz))
        }
        other => Err(anyhow::anyhow!(
            "unsupported Arrow type for FFI conversion: {other:?}"
        )),
    }
}

pub fn convert_schema_v54_to_v57(schema: &arrow::datatypes::Schema) -> Result<Arc<V57Schema>> {
    let v57_fields: Result<Vec<V57Field>> = schema
        .fields()
        .iter()
        .map(|f| {
            let dt = convert_datatype(f.data_type())?;
            Ok(V57Field::new(f.name(), dt, f.is_nullable()))
        })
        .collect();
    Ok(Arc::new(V57Schema::new(v57_fields?)))
}

pub fn convert_v54_to_v57(batch: &RecordBatch) -> Result<V57RecordBatch> {
    let v57_schema = convert_schema_v54_to_v57(&batch.schema())?;

    let mut v57_columns: Vec<Arc<dyn V57ArrayTrait>> = Vec::new();
    for col_idx in 0..batch.num_columns() {
        let v54_data = batch.column(col_idx).to_data();

        let (ffi_array, ffi_schema) = arrow::ffi::to_ffi(&v54_data)?;

        let v57_ffi_schema = unsafe {
            let src = &ffi_schema as *const _ as *const u8;
            let mut dst = std::mem::zeroed::<deltalake::arrow::ffi::FFI_ArrowSchema>();
            std::ptr::copy_nonoverlapping(
                src,
                &mut dst as *mut _ as *mut u8,
                std::mem::size_of::<deltalake::arrow::ffi::FFI_ArrowSchema>(),
            );
            dst
        };

        let v57_ffi_array = unsafe {
            let src = &ffi_array as *const _ as *const u8;
            let mut dst = std::mem::zeroed::<deltalake::arrow::ffi::FFI_ArrowArray>();
            std::ptr::copy_nonoverlapping(
                src,
                &mut dst as *mut _ as *mut u8,
                std::mem::size_of::<deltalake::arrow::ffi::FFI_ArrowArray>(),
            );
            dst
        };

        std::mem::forget(ffi_array);
        std::mem::forget(ffi_schema);

        let v57_data = unsafe { deltalake::arrow::ffi::from_ffi(v57_ffi_array, &v57_ffi_schema)? };
        v57_columns.push(deltalake::arrow::array::make_array(v57_data));
    }

    Ok(V57RecordBatch::try_new(v57_schema, v57_columns)?)
}

pub fn convert_batches(v54_batches: Vec<RecordBatch>) -> Result<Vec<V57RecordBatch>> {
    v54_batches
        .into_iter()
        .map(|b| convert_v54_to_v57(&b))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn make_batch(rows: usize, val: i32) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("val", DataType::Int32, false),
        ]));
        let id = Int32Array::from(vec![1i32; rows]);
        let v = Int32Array::from(vec![val; rows]);
        RecordBatch::try_new(schema, vec![Arc::new(id), Arc::new(v)]).unwrap()
    }

    fn make_large_batch(rows: usize) -> RecordBatch {
        let fields: Vec<Field> = (0..50)
            .map(|i| Field::new(format!("col_{i}"), DataType::Int64, false))
            .collect();
        let schema = Arc::new(Schema::new(fields));
        let arrays: Vec<Arc<dyn arrow::array::Array>> = (0..50)
            .map(|_| {
                let arr = arrow::array::Int64Array::from(vec![1i64; rows]);
                Arc::new(arr) as Arc<dyn arrow::array::Array>
            })
            .collect();
        RecordBatch::try_new(schema, arrays).unwrap()
    }

    struct MockStreamer {
        batches: Vec<Option<RecordBatch>>,
        index: usize,
    }

    impl MockStreamer {
        fn new(batches: Vec<Option<RecordBatch>>) -> Self {
            Self { batches, index: 0 }
        }
    }

    impl CxStreamer for MockStreamer {
        fn prepare(&mut self) {}

        fn next_batch(&mut self) -> Option<RecordBatch> {
            if self.index >= self.batches.len() {
                return None;
            }
            let result = self.batches[self.index].clone();
            self.index += 1;
            result
        }
    }

    #[test]
    fn calculate_batch_size_with_avg_row_length() {
        let mut ext = BatchExtractor::new("mysql://u:p@h/db", 512, 10000);
        assert_eq!(ext.calculate_batch_size(Some(100)), 5368709);
        assert_eq!(ext.batch_size(), 5368709);
    }

    #[test]
    fn calculate_batch_size_zero_avg_row_length() {
        let mut ext = BatchExtractor::new("mysql://u:p@h/db", 512, 10000);
        assert_eq!(ext.calculate_batch_size(Some(0)), 10000);
    }

    #[test]
    fn calculate_batch_size_none_avg_row_length() {
        let mut ext = BatchExtractor::new("mysql://u:p@h/db", 512, 10000);
        assert_eq!(ext.calculate_batch_size(None), 10000);
    }

    #[test]
    fn calculate_batch_size_large_avg_row_floors_to_one() {
        let mut ext = BatchExtractor::new("mysql://u:p@h/db", 1, 10000);
        assert_eq!(ext.calculate_batch_size(Some(u64::MAX)), 1);
    }

    #[test]
    fn extract_returns_batches_from_stream() {
        let mut ext = BatchExtractor::new("mysql://u:p@h/db", 512, 10000);
        ext.calculate_batch_size(Some(100));

        let batch1 = make_batch(3, 42);
        let batch2 = make_batch(2, 99);
        let mut streamer =
            MockStreamer::new(vec![Some(batch1.clone()), Some(batch2.clone()), None]);

        let result = ext.extract_from_stream(&mut streamer).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].num_rows(), 3);
        assert_eq!(result[1].num_rows(), 2);
    }

    #[test]
    fn extract_empty_stream_returns_empty() {
        let mut ext = BatchExtractor::new("mysql://u:p@h/db", 512, 10000);
        let mut streamer = MockStreamer::new(vec![]);
        let result = ext.extract_from_stream(&mut streamer).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn extract_zero_row_batch_stops_iteration() {
        let mut ext = BatchExtractor::new("mysql://u:p@h/db", 512, 10000);
        let empty = make_batch(0, 0);
        let should_not_appear = make_batch(5, 1);
        let mut streamer = MockStreamer::new(vec![Some(empty), Some(should_not_appear)]);
        let result = ext.extract_from_stream(&mut streamer).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn adapt_after_first_batch_reduces_size_when_actual_much_larger() {
        let mut ext = BatchExtractor::new("mysql://u:p@h/db", 1, 10000);
        ext.calculate_batch_size(Some(8));
        let original = ext.batch_size();

        let batch_with_many_cols = make_large_batch(1000);
        ext.adapt_after_first_batch(&batch_with_many_cols);
        assert!(ext.batch_size() < original);
    }

    #[test]
    fn adapt_after_first_batch_increases_size_when_actual_much_smaller() {
        let mut ext = BatchExtractor::new("mysql://u:p@h/db", 1, 10000);
        ext.calculate_batch_size(Some(10000));
        let original = ext.batch_size();

        let tiny_batch = make_batch(100, 1);
        ext.adapt_after_first_batch(&tiny_batch);
        assert!(ext.batch_size() > original);
    }

    #[test]
    fn adapt_after_first_batch_no_change_within_2x() {
        let mut ext = BatchExtractor::new("mysql://u:p@h/db", 1, 10000);
        ext.calculate_batch_size(Some(8));
        let original = ext.batch_size();

        let batch = make_batch(100, 1);
        ext.adapt_after_first_batch(&batch);
        assert_eq!(ext.batch_size(), original);
    }

    #[test]
    fn adapt_after_first_batch_zero_rows_no_change() {
        let mut ext = BatchExtractor::new("mysql://u:p@h/db", 512, 10000);
        ext.calculate_batch_size(Some(100));
        let original = ext.batch_size();

        let empty = make_batch(0, 0);
        ext.adapt_after_first_batch(&empty);
        assert_eq!(ext.batch_size(), original);
    }

    #[test]
    fn hard_ceiling_halves_batch_size() {
        let mut ext = BatchExtractor::new("mysql://u:p@h/db", 1, 10000);
        ext.calculate_batch_size(Some(8));
        let original = ext.batch_size();

        let big = make_large_batch(100000);
        ext.enforce_hard_ceiling(&big);
        assert_eq!(ext.batch_size(), original / 2);
    }

    #[test]
    fn hard_ceiling_no_change_within_limit() {
        let mut ext = BatchExtractor::new("mysql://u:p@h/db", 1, 10000);
        ext.calculate_batch_size(Some(8));
        let original = ext.batch_size();

        let small = make_batch(10, 1);
        ext.enforce_hard_ceiling(&small);
        assert_eq!(ext.batch_size(), original);
    }

    #[test]
    fn extract_invalid_url_returns_error() {
        let mut ext = BatchExtractor::new("not-a-url", 512, 10000);
        let result = ext.extract("SELECT 1");
        assert!(result.is_err());
    }

    #[test]
    fn adapt_zero_batch_size_returns_early() {
        let mut ext = BatchExtractor::new("mysql://u:p@h/db", 1, 0);
        let batch = make_batch(100, 1);
        ext.adapt_after_first_batch(&batch);
        assert_eq!(ext.batch_size(), 0);
    }

    #[test]
    fn adapt_zero_target_memory_returns_early() {
        let mut ext = BatchExtractor::new("mysql://u:p@h/db", 0, 10000);
        ext.calculate_batch_size(Some(100));
        let before = ext.batch_size();
        let batch = make_batch(100, 1);
        ext.adapt_after_first_batch(&batch);
        assert_eq!(ext.batch_size(), before);
    }

    #[test]
    fn hard_ceiling_floor_to_one() {
        let mut ext = BatchExtractor::new("mysql://u:p@h/db", 0, 1);
        let big = make_large_batch(100000);
        ext.enforce_hard_ceiling(&big);
        assert_eq!(ext.batch_size(), 1);
    }

    #[test]
    fn adapted_flag_prevents_repeated_adaptation() {
        let mut ext = BatchExtractor::new("mysql://u:p@h/db", 1, 10000);
        ext.calculate_batch_size(Some(8));

        let batch1 = make_batch(100, 42);
        let batch2 = make_batch(100, 42);
        let mut streamer = MockStreamer::new(vec![Some(batch1), Some(batch2)]);
        let result = ext.extract_from_stream(&mut streamer).unwrap();
        assert_eq!(result.len(), 2);

        let size_after_first_adapt = ext.batch_size();
        assert!(ext.adapted);

        let mut streamer2 = MockStreamer::new(vec![Some(make_large_batch(100))]);
        let _ = ext.extract_from_stream(&mut streamer2).unwrap();
        assert_eq!(ext.batch_size(), size_after_first_adapt);
    }

    // -----------------------------------------------------------------------
    // FFI conversion tests (Task 8)
    // -----------------------------------------------------------------------
    use arrow::array::{
        BooleanArray, Date32Array, Float64Array, Int64Array, StringArray, TimestampMicrosecondArray,
    };
    use deltalake::arrow::array as v57_arr;
    use deltalake::arrow::datatypes::TimeUnit as V57TU;

    pub fn make_v54_single_col_int64(rows: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let arr = Int64Array::from((0..rows as i64).collect::<Vec<_>>());
        RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap()
    }

    fn make_v54_single_col_utf8(values: Vec<Option<&str>>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, true)]));
        let arr = StringArray::from(values);
        RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap()
    }

    fn make_v54_single_col_float64_nulls(n: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "price",
            DataType::Float64,
            true,
        )]));
        let arr = Float64Array::from(
            (0..n)
                .map(|i| {
                    if i % 3 == 0 {
                        None
                    } else {
                        Some(i as f64 * 1.1)
                    }
                })
                .collect::<Vec<_>>(),
        );
        RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap()
    }

    fn make_v54_single_col_timestamp(rows: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "updated_at",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
            false,
        )]));
        let arr = TimestampMicrosecondArray::from(
            (0..rows)
                .map(|i| 1743158400000000i64 + i as i64 * 1000000)
                .collect::<Vec<_>>(),
        );
        RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap()
    }

    fn make_v54_single_col_boolean(rows: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "is_active",
            DataType::Boolean,
            false,
        )]));
        let arr = BooleanArray::from((0..rows).map(|i| i % 2 == 0).collect::<Vec<_>>());
        RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap()
    }

    fn make_v54_single_col_date32(rows: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "birth_date",
            DataType::Date32,
            true,
        )]));
        let arr = Date32Array::from((0..rows).map(|_| Some(20000i32)).collect::<Vec<_>>());
        RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap()
    }

    fn make_v54_multi_col_batch(n: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("price", DataType::Float64, true),
            Field::new(
                "updated_at",
                DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
                false,
            ),
            Field::new("is_active", DataType::Boolean, false),
            Field::new("birth_date", DataType::Date32, true),
        ]));
        let id = Int64Array::from((0..n as i64).collect::<Vec<_>>());
        let name = StringArray::from((0..n).map(|i| format!("user_{i}")).collect::<Vec<_>>());
        let price = Float64Array::from(
            (0..n)
                .map(|i| {
                    if i % 5 == 0 {
                        None
                    } else {
                        Some(i as f64 * 1.1)
                    }
                })
                .collect::<Vec<_>>(),
        );
        let ts = TimestampMicrosecondArray::from(
            (0..n)
                .map(|i| 1743158400000000i64 + i as i64 * 1000000)
                .collect::<Vec<_>>(),
        );
        let active = BooleanArray::from((0..n).map(|i| i % 2 == 0).collect::<Vec<_>>());
        let bd = Date32Array::from((0..n).map(|_| Some(20000i32)).collect::<Vec<_>>());
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(id),
                Arc::new(name),
                Arc::new(price),
                Arc::new(ts),
                Arc::new(active),
                Arc::new(bd),
            ],
        )
        .unwrap()
    }

    // --- 8.1 convert_datatype tests ---

    #[test]
    fn convert_datatype_null() {
        assert!(matches!(
            convert_datatype(&DataType::Null),
            Ok(V57DataType::Null)
        ));
    }

    #[test]
    fn convert_datatype_boolean() {
        assert!(matches!(
            convert_datatype(&DataType::Boolean),
            Ok(V57DataType::Boolean)
        ));
    }

    #[test]
    fn convert_datatype_int8() {
        assert!(matches!(
            convert_datatype(&DataType::Int8),
            Ok(V57DataType::Int8)
        ));
    }

    #[test]
    fn convert_datatype_int16() {
        assert!(matches!(
            convert_datatype(&DataType::Int16),
            Ok(V57DataType::Int16)
        ));
    }

    #[test]
    fn convert_datatype_int32() {
        assert!(matches!(
            convert_datatype(&DataType::Int32),
            Ok(V57DataType::Int32)
        ));
    }

    #[test]
    fn convert_datatype_int64() {
        assert!(matches!(
            convert_datatype(&DataType::Int64),
            Ok(V57DataType::Int64)
        ));
    }

    #[test]
    fn convert_datatype_uint8() {
        assert!(matches!(
            convert_datatype(&DataType::UInt8),
            Ok(V57DataType::UInt8)
        ));
    }

    #[test]
    fn convert_datatype_uint16() {
        assert!(matches!(
            convert_datatype(&DataType::UInt16),
            Ok(V57DataType::UInt16)
        ));
    }

    #[test]
    fn convert_datatype_uint32() {
        assert!(matches!(
            convert_datatype(&DataType::UInt32),
            Ok(V57DataType::UInt32)
        ));
    }

    #[test]
    fn convert_datatype_uint64() {
        assert!(matches!(
            convert_datatype(&DataType::UInt64),
            Ok(V57DataType::UInt64)
        ));
    }

    #[test]
    fn convert_datatype_float16() {
        assert!(matches!(
            convert_datatype(&DataType::Float16),
            Ok(V57DataType::Float16)
        ));
    }

    #[test]
    fn convert_datatype_float32() {
        assert!(matches!(
            convert_datatype(&DataType::Float32),
            Ok(V57DataType::Float32)
        ));
    }

    #[test]
    fn convert_datatype_float64() {
        assert!(matches!(
            convert_datatype(&DataType::Float64),
            Ok(V57DataType::Float64)
        ));
    }

    #[test]
    fn convert_datatype_utf8() {
        assert!(matches!(
            convert_datatype(&DataType::Utf8),
            Ok(V57DataType::Utf8)
        ));
    }

    #[test]
    fn convert_datatype_large_utf8() {
        assert!(matches!(
            convert_datatype(&DataType::LargeUtf8),
            Ok(V57DataType::LargeUtf8)
        ));
    }

    #[test]
    fn convert_datatype_binary() {
        assert!(matches!(
            convert_datatype(&DataType::Binary),
            Ok(V57DataType::Binary)
        ));
    }

    #[test]
    fn convert_datatype_large_binary() {
        assert!(matches!(
            convert_datatype(&DataType::LargeBinary),
            Ok(V57DataType::LargeBinary)
        ));
    }

    #[test]
    fn convert_datatype_date32() {
        assert!(matches!(
            convert_datatype(&DataType::Date32),
            Ok(V57DataType::Date32)
        ));
    }

    #[test]
    fn convert_datatype_date64() {
        assert!(matches!(
            convert_datatype(&DataType::Date64),
            Ok(V57DataType::Date64)
        ));
    }

    #[test]
    fn convert_datatype_timestamp_microsecond() {
        let result = convert_datatype(&DataType::Timestamp(
            arrow::datatypes::TimeUnit::Microsecond,
            None,
        ));
        assert!(matches!(
            result,
            Ok(V57DataType::Timestamp(V57TU::Microsecond, None))
        ));
    }

    #[test]
    fn convert_datatype_timestamp_second() {
        let result = convert_datatype(&DataType::Timestamp(
            arrow::datatypes::TimeUnit::Second,
            None,
        ));
        assert!(matches!(
            result,
            Ok(V57DataType::Timestamp(V57TU::Second, None))
        ));
    }

    #[test]
    fn convert_datatype_timestamp_millisecond() {
        let result = convert_datatype(&DataType::Timestamp(
            arrow::datatypes::TimeUnit::Millisecond,
            None,
        ));
        assert!(matches!(
            result,
            Ok(V57DataType::Timestamp(V57TU::Millisecond, None))
        ));
    }

    #[test]
    fn convert_datatype_timestamp_nanosecond() {
        let result = convert_datatype(&DataType::Timestamp(
            arrow::datatypes::TimeUnit::Nanosecond,
            None,
        ));
        assert!(matches!(
            result,
            Ok(V57DataType::Timestamp(V57TU::Nanosecond, None))
        ));
    }

    #[test]
    fn convert_datatype_timestamp_with_timezone() {
        let result = convert_datatype(&DataType::Timestamp(
            arrow::datatypes::TimeUnit::Microsecond,
            Some("UTC".into()),
        ));
        match result {
            Ok(V57DataType::Timestamp(V57TU::Microsecond, Some(tz))) => {
                assert_eq!(tz.as_ref(), "UTC");
            }
            _ => panic!("expected Timestamp(Microsecond, Some(\"UTC\"))"),
        }
    }

    #[test]
    fn convert_datatype_unsupported_list() {
        let dt = DataType::List(Arc::new(Field::new("item", DataType::Int32, true)));
        assert!(convert_datatype(&dt).is_err());
    }

    #[test]
    fn convert_datatype_unsupported_struct() {
        let dt = DataType::Struct(arrow::datatypes::Fields::from(vec![Field::new(
            "a",
            DataType::Int32,
            false,
        )]));
        assert!(convert_datatype(&dt).is_err());
    }

    // --- 8.2 convert_schema_v54_to_v57 tests ---

    #[test]
    fn convert_schema_preserves_field_names() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let v57 = convert_schema_v54_to_v57(&schema).unwrap();
        let names: Vec<&str> = v57.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, vec!["id", "name"]);
    }

    #[test]
    fn convert_schema_preserves_nullability() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let v57 = convert_schema_v54_to_v57(&schema).unwrap();
        assert!(!v57.field(0).is_nullable());
        assert!(v57.field(1).is_nullable());
    }

    #[test]
    fn convert_schema_preserves_types() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("price", DataType::Float64, true),
        ]));
        let v57 = convert_schema_v54_to_v57(&schema).unwrap();
        assert!(matches!(v57.field(0).data_type(), V57DataType::Int64));
        assert!(matches!(v57.field(1).data_type(), V57DataType::Float64));
    }

    #[test]
    fn convert_schema_unsupported_type_returns_error() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "data",
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            false,
        )]));
        assert!(convert_schema_v54_to_v57(&schema).is_err());
    }

    // --- 8.3 convert_v54_to_v57 tests ---

    #[test]
    fn ffi_convert_int64_column() {
        let v54 = make_v54_single_col_int64(10);
        let v57 = convert_v54_to_v57(&v54).unwrap();
        assert_eq!(v57.num_rows(), 10);
        let col = v57
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<v57_arr::Int64Array>()
            .unwrap();
        assert_eq!(col.value(0), 0);
        assert_eq!(col.value(9), 9);
    }

    #[test]
    fn ffi_convert_utf8_column() {
        let v54 = make_v54_single_col_utf8(vec![Some("hello"), None, Some("world")]);
        let v57 = convert_v54_to_v57(&v54).unwrap();
        assert_eq!(v57.num_rows(), 3);
        let col = v57
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<v57_arr::StringArray>()
            .unwrap();
        assert_eq!(col.value(0), "hello");
        assert!(col.is_null(1));
        assert_eq!(col.value(2), "world");
    }

    #[test]
    fn ffi_convert_float64_with_nulls() {
        let v54 = make_v54_single_col_float64_nulls(10);
        let v57 = convert_v54_to_v57(&v54).unwrap();
        assert_eq!(v57.num_rows(), 10);
        let col = v57
            .column_by_name("price")
            .unwrap()
            .as_any()
            .downcast_ref::<v57_arr::Float64Array>()
            .unwrap();
        assert!(col.is_null(0));
        assert!(!col.is_null(1));
        assert!(col.is_null(3));
    }

    #[test]
    fn ffi_convert_timestamp_column() {
        let v54 = make_v54_single_col_timestamp(5);
        let v57 = convert_v54_to_v57(&v54).unwrap();
        assert_eq!(v57.num_rows(), 5);
        let col = v57.column_by_name("updated_at").unwrap();
        let ids = col
            .as_any()
            .downcast_ref::<v57_arr::TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(ids.value(0), 1743158400000000i64);
        assert_eq!(ids.value(4), 1743158404000000i64);
    }

    #[test]
    fn ffi_convert_boolean_column() {
        let v54 = make_v54_single_col_boolean(4);
        let v57 = convert_v54_to_v57(&v54).unwrap();
        assert_eq!(v57.num_rows(), 4);
        let col = v57
            .column_by_name("is_active")
            .unwrap()
            .as_any()
            .downcast_ref::<v57_arr::BooleanArray>()
            .unwrap();
        assert!(col.value(0));
        assert!(!col.value(1));
    }

    #[test]
    fn ffi_convert_date32_column() {
        let v54 = make_v54_single_col_date32(3);
        let v57 = convert_v54_to_v57(&v54).unwrap();
        assert_eq!(v57.num_rows(), 3);
        let col = v57
            .column_by_name("birth_date")
            .unwrap()
            .as_any()
            .downcast_ref::<v57_arr::Date32Array>()
            .unwrap();
        assert_eq!(col.value(0), 20000);
    }

    #[test]
    fn ffi_convert_multi_column_batch() {
        let v54 = make_v54_multi_col_batch(100);
        let v57 = convert_v54_to_v57(&v54).unwrap();
        assert_eq!(v57.num_columns(), 6);
        assert_eq!(v57.num_rows(), 100);

        let id_col = v57
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<v57_arr::Int64Array>()
            .unwrap();
        assert_eq!(id_col.value(0), 0);
        assert_eq!(id_col.value(99), 99);

        let name_col = v57
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<v57_arr::StringArray>()
            .unwrap();
        assert_eq!(name_col.value(0), "user_0");

        let price_col = v57
            .column_by_name("price")
            .unwrap()
            .as_any()
            .downcast_ref::<v57_arr::Float64Array>()
            .unwrap();
        assert!(price_col.is_null(0));
        assert!(!price_col.is_null(1));
    }

    #[test]
    fn ffi_convert_empty_batch() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let arr = Int64Array::from(Vec::<i64>::new());
        let v54 = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();

        let v57 = convert_v54_to_v57(&v54).unwrap();
        assert_eq!(v57.num_rows(), 0);
        assert_eq!(v57.num_columns(), 1);
    }

    #[test]
    fn ffi_convert_schema_field_names_preserved() {
        let v54 = make_v54_multi_col_batch(10);
        let v57 = convert_v54_to_v57(&v54).unwrap();
        let schema = v57.schema();
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(
            names,
            vec![
                "id",
                "name",
                "price",
                "updated_at",
                "is_active",
                "birth_date"
            ]
        );
    }

    #[test]
    fn ffi_convert_nullability_preserved() {
        let v54 = make_v54_multi_col_batch(10);
        let v57 = convert_v54_to_v57(&v54).unwrap();
        assert!(!v57.schema().field(0).is_nullable()); // id
        assert!(!v57.schema().field(1).is_nullable()); // name
        assert!(v57.schema().field(2).is_nullable()); // price
        assert!(v57.schema().field(5).is_nullable()); // birth_date
    }

    // --- 8.4 convert_batches tests ---

    #[test]
    fn convert_batches_empty_vec() {
        let result: Result<Vec<V57RecordBatch>> = convert_batches(vec![]);
        assert!(result.unwrap().is_empty());
    }

    #[test]
    fn convert_batches_single() {
        let v54 = make_v54_single_col_int64(5);
        let result = convert_batches(vec![v54]).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 5);
    }

    #[test]
    fn convert_batches_multiple() {
        let b1 = make_v54_single_col_int64(3);
        let b2 = make_v54_single_col_int64(7);
        let b3 = make_v54_single_col_int64(1);
        let result = convert_batches(vec![b1, b2, b3]).unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].num_rows(), 3);
        assert_eq!(result[1].num_rows(), 7);
        assert_eq!(result[2].num_rows(), 1);
    }

    #[test]
    fn convert_batches_mixed_types() {
        let b1 = make_v54_single_col_int64(5);
        let b2 = make_v54_single_col_utf8(vec![Some("a"), Some("b")]);
        let result = convert_batches(vec![b1, b2]).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].num_rows(), 5);
        assert_eq!(result[1].num_rows(), 2);
    }
}
