use anyhow::Result;
use deltalake::arrow::record_batch::RecordBatch;
use tracing::{debug, info, warn};


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
        use connector_arrow::api::{Connector, Statement};
        use connector_arrow::mysql::MySQLConnection;

        let opts = mysql::Opts::from_url(&self.database_url)
            .map_err(|e| anyhow::anyhow!("invalid database url: {e}"))?;
        let conn = mysql::Conn::new(opts)
            .map_err(|e| anyhow::anyhow!("MySQL connection failed: {e}"))?;
        let mut ca_conn = MySQLConnection::new(conn);
        let mut stmt = ca_conn.query(sql)
            .map_err(|e| anyhow::anyhow!("query prepare failed: {e}"))?;
        let reader = stmt.start([])
            .map_err(|e| anyhow::anyhow!("query start failed: {e}"))?;
        let raw_batches: Vec<RecordBatch> = reader
            .collect::<Result<_, _>>()
            .map_err(|e| anyhow::anyhow!("batch read failed: {e}"))?;
        self.extract_from_stream_ca(raw_batches)
    }

    fn extract_from_stream_ca(&mut self, raw_batches: Vec<RecordBatch>) -> Result<Vec<RecordBatch>> {
        let mut batches = Vec::new();
        for batch in raw_batches {
            let rows = batch.num_rows();
            info!(
                rows,
                arrow_bytes = batch.get_array_memory_size(),
                "batch extracted"
            );

            if !self.adapted && rows > 0 {
                self.adapt_after_first_batch_ca(&batch);
                self.adapted = true;
            }

            self.enforce_hard_ceiling_ca(&batch);

            if rows == 0 {
                break;
            }
            batches.push(batch);
        }

        Ok(batches)
    }

    fn adapt_after_first_batch_ca(&mut self, batch: &RecordBatch) {
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

    fn enforce_hard_ceiling_ca(&mut self, batch: &RecordBatch) {
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


#[cfg(test)]
mod tests {
    use super::*;
    use deltalake::arrow::array::Int32Array;
    use deltalake::arrow::datatypes::{DataType, Field, Schema};
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
        let arrays: Vec<Arc<dyn deltalake::arrow::array::Array>> = (0..50)
            .map(|_| {
                let arr = deltalake::arrow::array::Int64Array::from(vec![1i64; rows]);
                Arc::new(arr) as Arc<dyn deltalake::arrow::array::Array>
            })
            .collect();
        RecordBatch::try_new(schema, arrays).unwrap()
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
        let batches = vec![batch1.clone(), batch2.clone()];

        let result = ext.extract_from_stream_ca(batches).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].num_rows(), 3);
        assert_eq!(result[1].num_rows(), 2);
    }

    #[test]
    fn extract_empty_stream_returns_empty() {
        let mut ext = BatchExtractor::new("mysql://u:p@h/db", 512, 10000);
        let batches = vec![];
        let result = ext.extract_from_stream_ca(batches).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn extract_zero_row_batch_stops_iteration() {
        let mut ext = BatchExtractor::new("mysql://u:p@h/db", 512, 10000);
        let empty = make_batch(0, 0);
        let batches = vec![empty];
        let result = ext.extract_from_stream_ca(batches).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn adapt_after_first_batch_reduces_size_when_actual_much_larger() {
        let mut ext = BatchExtractor::new("mysql://u:p@h/db", 1, 10000);
        ext.calculate_batch_size(Some(8));
        let original = ext.batch_size();

        let batch_with_many_cols = make_large_batch(1000);
        ext.adapt_after_first_batch_ca(&batch_with_many_cols);
        assert!(ext.batch_size() < original);
    }

    #[test]
    fn adapt_after_first_batch_increases_size_when_actual_much_smaller() {
        let mut ext = BatchExtractor::new("mysql://u:p@h/db", 1, 10000);
        ext.calculate_batch_size(Some(10000));
        let original = ext.batch_size();

        let tiny_batch = make_batch(100, 1);
        ext.adapt_after_first_batch_ca(&tiny_batch);
        assert!(ext.batch_size() > original);
    }

    #[test]
    fn adapt_after_first_batch_no_change_within_2x() {
        let mut ext = BatchExtractor::new("mysql://u:p@h/db", 1, 10000);
        ext.calculate_batch_size(Some(8));
        let original = ext.batch_size();

        let batch = make_batch(100, 1);
        ext.adapt_after_first_batch_ca(&batch);
        assert_eq!(ext.batch_size(), original);
    }

    #[test]
    fn adapt_after_first_batch_zero_rows_no_change() {
        let mut ext = BatchExtractor::new("mysql://u:p@h/db", 512, 10000);
        ext.calculate_batch_size(Some(100));
        let original = ext.batch_size();

        let empty = make_batch(0, 0);
        ext.adapt_after_first_batch_ca(&empty);
        assert_eq!(ext.batch_size(), original);
    }

    #[test]
    fn hard_ceiling_halves_batch_size() {
        let mut ext = BatchExtractor::new("mysql://u:p@h/db", 1, 10000);
        ext.calculate_batch_size(Some(8));
        let original = ext.batch_size();

        let big = make_large_batch(100000);
        ext.enforce_hard_ceiling_ca(&big);
        assert_eq!(ext.batch_size(), original / 2);
    }

    #[test]
    fn hard_ceiling_no_change_within_limit() {
        let mut ext = BatchExtractor::new("mysql://u:p@h/db", 1, 10000);
        ext.calculate_batch_size(Some(8));
        let original = ext.batch_size();

        let small = make_batch(10, 1);
        ext.enforce_hard_ceiling_ca(&small);
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
        ext.adapt_after_first_batch_ca(&batch);
        assert_eq!(ext.batch_size(), 0);
    }

    #[test]
    fn adapt_zero_target_memory_returns_early() {
        let mut ext = BatchExtractor::new("mysql://u:p@h/db", 0, 10000);
        ext.calculate_batch_size(Some(100));
        let before = ext.batch_size();
        let batch = make_batch(100, 1);
        ext.adapt_after_first_batch_ca(&batch);
        assert_eq!(ext.batch_size(), before);
    }

    #[test]
    fn hard_ceiling_floor_to_one() {
        let mut ext = BatchExtractor::new("mysql://u:p@h/db", 0, 1);
        let big = make_large_batch(100000);
        ext.enforce_hard_ceiling_ca(&big);
        assert_eq!(ext.batch_size(), 1);
    }

    #[test]
    fn adapted_flag_prevents_repeated_adaptation() {
        let mut ext = BatchExtractor::new("mysql://u:p@h/db", 1, 10000);
        ext.calculate_batch_size(Some(8));

        let batch1 = make_batch(100, 42);
        let batch2 = make_batch(100, 42);
        let batches = vec![batch1, batch2];
        let result = ext.extract_from_stream_ca(batches).unwrap();
        assert_eq!(result.len(), 2);

        let size_after_first_adapt = ext.batch_size();
        assert!(ext.adapted);

        let batches2 = vec![make_large_batch(100)];
        let _ = ext.extract_from_stream_ca(batches2).unwrap();
        assert_eq!(ext.batch_size(), size_after_first_adapt);
    }
}
