use anyhow::Result;
use deltalake::arrow::record_batch::RecordBatch;
use tracing::{debug, info, warn};

/// The result of one `extract()` call: the batches pulled off the wire for this window, plus
/// whether the mid-stream memory circuit breaker (M2) cut the window short. `truncated` tells
/// the caller "there is more data in this window than what's in `batches`" — safe to act on for
/// cursor-based pagination (the tail simply arrives in the next, smaller window), but a signal
/// the OFFSET-fallback path (which assumes exactly `batch_size` rows per chunk) must not ignore.
pub struct Extraction {
    pub batches: Vec<RecordBatch>,
    pub truncated: bool,
}

pub struct BatchExtractor {
    database_url: String,
    target_memory_mb: u64,
    default_batch_size: u64,
    batch_size: u64,
    adapted: bool,
    /// P1: a pooled MySQL connection reused across batch windows. Taken out on each extract,
    /// returned to the pool ONLY after a clean (non-truncated, error-free) window; dropped on
    /// truncation or error so the next call opens fresh (identical to the old fresh-conn-per-call,
    /// preserving M2 breaker semantics).
    conn: Option<connector_arrow::mysql::MySQLConnection<mysql::Conn>>,
}

impl BatchExtractor {
    pub fn new(database_url: &str, target_memory_mb: u64, default_batch_size: u64) -> Self {
        Self {
            database_url: database_url.to_string(),
            target_memory_mb,
            default_batch_size,
            batch_size: default_batch_size,
            adapted: false,
            conn: None,
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

    fn open_connection(&self) -> Result<connector_arrow::mysql::MySQLConnection<mysql::Conn>> {
        let opts = mysql::Opts::from_url(&self.database_url)
            .map_err(|e| anyhow::anyhow!("invalid database url: {e}"))?;
        let conn = mysql::Conn::new(opts)
            .map_err(|e| anyhow::anyhow!("MySQL connection failed: {e}"))?;
        Ok(connector_arrow::mysql::MySQLConnection::new(conn))
    }

    pub fn extract(&mut self, sql: &str) -> Result<Extraction> {
        // Try the pooled connection; if it was a REUSED one and failed, the server may have
        // closed it — drop it and retry ONCE with a fresh connection (robustness parity with the
        // old always-fresh behavior). A genuine query error simply fails again on the retry.
        let had_pooled = self.conn.is_some();
        match self.extract_once(sql) {
            Ok(x) => Ok(x),
            Err(e) if had_pooled => {
                self.conn = None;
                tracing::warn!(error = %e, "pooled MySQL connection failed; retrying once with a fresh connection");
                self.extract_once(sql)
            }
            Err(e) => Err(e),
        }
    }

    fn extract_once(&mut self, sql: &str) -> Result<Extraction> {
        use connector_arrow::api::{Connector, Statement};

        // Take the pooled connection out (keeping it a LOCAL so the streaming borrow doesn't
        // collide with the `&mut self` call into extract_from_stream_ca), or open fresh.
        let mut ca_conn = match self.conn.take() {
            Some(c) => c,
            None => self.open_connection()?,
        };

        let extraction;
        let read_err;
        {
            let mut stmt = ca_conn.query(sql)
                .map_err(|e| anyhow::anyhow!("query prepare failed: {e}"))?;
            let reader = stmt.start([])
                .map_err(|e| anyhow::anyhow!("query start failed: {e}"))?;

            // M2: iterate the connector_arrow reader INCREMENTALLY (no intermediate collect) so
            // the mid-stream circuit breaker in extract_from_stream_ca can observe cumulative
            // window bytes as batches arrive and stop consuming before an unexpectedly fat window
            // (stale AVG_ROW_LENGTH, wide rows) is fully resident. `TrackErrors` adapts the
            // reader's `Iterator<Item = Result<RecordBatch, ConnectorError>>` into a plain
            // `Iterator<Item = RecordBatch>` that stops at the first read error and stashes it,
            // so extract_from_stream_ca can stay a plain-RecordBatch consumer shared with tests.
            let mut tracked = TrackErrors { inner: reader, err: None };
            extraction = self.extract_from_stream_ca(&mut tracked)?;
            read_err = tracked.err;
        }

        if let Some(e) = read_err {
            // ca_conn drops here (NOT returned to the pool) — a mid-stream failure leaves it dirty.
            return Err(anyhow::anyhow!("batch read failed: {e}"));
        }

        // Discard-on-truncation: a truncated window left the server mid-result, so dropping the
        // connection (server aborts on socket close) mirrors the old fresh-conn-per-call and keeps
        // M2 safe. A cleanly-drained full window leaves the connection reusable → pool it.
        if !extraction.truncated {
            self.conn = Some(ca_conn);
        }
        Ok(extraction)
    }

    /// Shared incremental consumer: accumulates batches from `raw_batches` and applies the
    /// mid-stream memory circuit breaker (M2). Generic over `IntoIterator<Item = RecordBatch>`
    /// so both `extract()` (a live, possibly-erroring connector_arrow reader wrapped in
    /// `TrackErrors`) and unit tests (a plain `Vec<RecordBatch>`) can drive it.
    ///
    /// Breaker: `window_bytes` accumulates `get_array_memory_size()` across pushed batches; once
    /// it exceeds `target_memory_mb * 2 MiB` (the same ceiling the old per-batch
    /// `enforce_hard_ceiling_ca` used, now applied cumulatively), `batch_size` is halved (floor
    /// 1), `truncated` is set, and consumption of `raw_batches` stops immediately — the batches
    /// accumulated so far (including the one that crossed the ceiling) are returned. This is
    /// safe for cursor-based callers: the cursor only advances over rows actually returned, so
    /// the remainder of the window arrives in the next (smaller) request.
    fn extract_from_stream_ca(&mut self, raw_batches: impl IntoIterator<Item = RecordBatch>) -> Result<Extraction> {
        let ceiling = self.target_memory_mb * 2 * 1024 * 1024;
        let mut batches = Vec::new();
        let mut window_bytes: u64 = 0;
        let mut truncated = false;

        for batch in raw_batches {
            let rows = batch.num_rows();
            let batch_bytes = batch.get_array_memory_size() as u64;
            info!(rows, arrow_bytes = batch_bytes, "batch extracted");

            if !self.adapted && rows > 0 {
                self.adapt_after_first_batch_ca(&batch);
                self.adapted = true;
            }

            if rows == 0 {
                break;
            }

            // H-2026-07-11-2: unsigned columns are widened AFTER extraction by
            // align_batch_to_schema (UInt8→Int16, UInt16→Int32, UInt32→Int64 — each
            // doubles that column's buffer; UInt64→Int64 is width-neutral). The breaker
            // must budget for the POST-alignment window, so UInt8/16/32 column buffers
            // are counted twice here — otherwise an unsigned-heavy window admitted at
            // just under the ceiling could double past it before the write.
            let widen_extra: u64 = batch
                .schema()
                .fields()
                .iter()
                .enumerate()
                .filter(|(_, f)| {
                    matches!(
                        f.data_type(),
                        deltalake::arrow::datatypes::DataType::UInt8
                            | deltalake::arrow::datatypes::DataType::UInt16
                            | deltalake::arrow::datatypes::DataType::UInt32
                    )
                })
                .map(|(i, _)| batch.column(i).get_array_memory_size() as u64)
                .sum();

            window_bytes += batch_bytes + widen_extra;
            batches.push(batch);

            if window_bytes > ceiling {
                let old_batch_size = self.batch_size;
                let new_batch_size = (self.batch_size / 2).max(1);
                warn!(
                    window_bytes,
                    ceiling_bytes = ceiling,
                    old_batch_size,
                    new_batch_size,
                    "mid-stream memory circuit breaker tripped, truncating window and halving batch_size"
                );
                self.batch_size = new_batch_size;
                truncated = true;
                break;
            }
        }

        Ok(Extraction { batches, truncated })
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

    pub fn batch_size(&self) -> u64 {
        self.batch_size
    }
}

/// Adapts a `connector_arrow` reader (`Iterator<Item = Result<RecordBatch, ConnectorError>>`)
/// into a plain `Iterator<Item = RecordBatch>`, stopping at the first read error and stashing it
/// in `err` for the caller to surface after iteration ends. This lets `extract_from_stream_ca`
/// stay a single plain-RecordBatch consumer shared between the live extraction path and unit
/// tests feeding synthetic batch vectors, while `extract()` still gets to `?`-propagate a
/// genuine read failure instead of silently truncating the window on error.
struct TrackErrors<I> {
    inner: I,
    err: Option<connector_arrow::ConnectorError>,
}

impl<I> Iterator for TrackErrors<I>
where
    I: Iterator<Item = Result<RecordBatch, connector_arrow::ConnectorError>>,
{
    type Item = RecordBatch;

    fn next(&mut self) -> Option<RecordBatch> {
        if self.err.is_some() {
            return None;
        }
        match self.inner.next() {
            Some(Ok(batch)) => Some(batch),
            Some(Err(e)) => {
                self.err = Some(e);
                None
            }
            None => None,
        }
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
        assert_eq!(result.batches.len(), 2);
        assert_eq!(result.batches[0].num_rows(), 3);
        assert_eq!(result.batches[1].num_rows(), 2);
        assert!(!result.truncated);
    }

    #[test]
    fn extract_empty_stream_returns_empty() {
        let mut ext = BatchExtractor::new("mysql://u:p@h/db", 512, 10000);
        let batches = vec![];
        let result = ext.extract_from_stream_ca(batches).unwrap();
        assert!(result.batches.is_empty());
        assert!(!result.truncated);
    }

    #[test]
    fn extract_zero_row_batch_stops_iteration() {
        let mut ext = BatchExtractor::new("mysql://u:p@h/db", 512, 10000);
        let empty = make_batch(0, 0);
        let batches = vec![empty];
        let result = ext.extract_from_stream_ca(batches).unwrap();
        assert!(result.batches.is_empty());
        assert!(!result.truncated);
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

    // M2: `enforce_hard_ceiling_ca` (a per-batch check that a single 1024-row batch almost
    // never tripped) is gone — the breaker now lives in `extract_from_stream_ca` and compares
    // *cumulative* window bytes across the whole stream against the same ceiling. These two
    // tests replace `hard_ceiling_halves_batch_size` / `hard_ceiling_no_change_within_limit`:
    // same ceiling-crossing semantics, but driven by a multi-batch window via the public
    // `Extraction` return type instead of a single direct batch check.
    #[test]
    fn breaker_truncates_and_halves_batch_size_when_window_crosses_ceiling() {
        let mut ext = BatchExtractor::new("mysql://u:p@h/db", 1, 10000);
        ext.calculate_batch_size(Some(8));
        // Skip adaptive sizing so batch_size stays fixed going into the breaker check —
        // this test is only about the cumulative-window ceiling, not adaptive sizing.
        ext.adapted = true;
        let original = ext.batch_size();

        // ceiling = target_memory_mb(1) * 2 * 1024 * 1024 = 2,097,152 bytes.
        // Each batch (50 int64 cols) is 4000 * 50 * 8 = 1,600,000 bytes: under the ceiling
        // alone, but the second batch pushes the cumulative window (3,200,000 bytes) over it.
        let batch1 = make_large_batch(4000);
        let batch2 = make_large_batch(4000);
        let batch3 = make_large_batch(4000);

        let result = ext.extract_from_stream_ca(vec![batch1, batch2, batch3]).unwrap();

        assert!(result.truncated, "cumulative window crossing the ceiling must set truncated");
        assert_eq!(
            result.batches.len(),
            2,
            "only the batches consumed up to and including the one that crossed the ceiling are returned"
        );
        assert_eq!(ext.batch_size(), original / 2, "batch_size must halve when the breaker trips");
    }

    #[test]
    fn breaker_does_not_trip_when_window_stays_under_ceiling() {
        let mut ext = BatchExtractor::new("mysql://u:p@h/db", 1, 10000);
        ext.calculate_batch_size(Some(8));
        ext.adapted = true;
        let original = ext.batch_size();

        let small = make_batch(10, 1);
        let result = ext.extract_from_stream_ca(vec![small]).unwrap();

        assert!(!result.truncated, "a window under the ceiling must not truncate");
        assert_eq!(result.batches.len(), 1, "the normal stream's batches are all returned");
        assert_eq!(ext.batch_size(), original, "batch_size is unchanged when the breaker does not trip");
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
    fn breaker_floors_batch_size_at_one() {
        // target_memory_mb=0 => ceiling=0, so any non-empty batch trips the breaker; halving
        // must floor at 1, never reach (or go below) 0.
        let mut ext = BatchExtractor::new("mysql://u:p@h/db", 0, 1);
        let big = make_large_batch(100000);
        let result = ext.extract_from_stream_ca(vec![big]).unwrap();
        assert!(result.truncated);
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
        assert_eq!(result.batches.len(), 2);

        let size_after_first_adapt = ext.batch_size();
        assert!(ext.adapted);

        let batches2 = vec![make_large_batch(100)];
        let _ = ext.extract_from_stream_ca(batches2).unwrap();
        assert_eq!(ext.batch_size(), size_after_first_adapt);
    }

    #[test]
    fn breaker_weights_unsigned_columns_for_post_align_widening() {
        // H-2026-07-11-2: UInt8/16/32 buffers double after align_batch_to_schema, so the
        // breaker counts them twice. An unsigned batch must therefore trip the ceiling at
        // roughly HALF the raw bytes a signed batch of identical layout would need.
        use deltalake::arrow::array::{Int16Array, UInt16Array};
        use deltalake::arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc;

        let rows = 300_000usize; // ~600 KB raw per column

        let unsigned_batch = {
            let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::UInt16, false)]));
            RecordBatch::try_new(schema, vec![Arc::new(UInt16Array::from(vec![1u16; rows]))]).unwrap()
        };
        let signed_batch = {
            let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int16, false)]));
            RecordBatch::try_new(schema, vec![Arc::new(Int16Array::from(vec![1i16; rows]))]).unwrap()
        };
        let raw = signed_batch.get_array_memory_size() as u64;
        assert_eq!(raw, unsigned_batch.get_array_memory_size() as u64, "same raw layout");

        // Ceiling: 1 MiB * 2 = 2 MiB. One ~600KB batch alone stays under; craft counts so
        // that WEIGHTED unsigned (2x) crosses after 2 batches while raw signed does not.
        let mut ext_unsigned = BatchExtractor::new("mysql://u:p@h/db", 1, 10000);
        let out = ext_unsigned
            .extract_from_stream_ca(vec![unsigned_batch.clone(), unsigned_batch.clone()])
            .unwrap();
        assert!(
            out.truncated,
            "2 unsigned batches (raw {}B x2, weighted x2) must cross the 2 MiB ceiling",
            raw
        );

        let mut ext_signed = BatchExtractor::new("mysql://u:p@h/db", 1, 10000);
        let out = ext_signed
            .extract_from_stream_ca(vec![signed_batch.clone(), signed_batch.clone()])
            .unwrap();
        assert!(
            !out.truncated,
            "2 signed batches of identical raw size must stay under the ceiling"
        );
    }
}
