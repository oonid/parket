use std::collections::HashMap;

use anyhow::{Context, Result};
use deltalake::arrow::array::{Array, Int32Array, Int64Array, StringArray, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt32Array, UInt64Array};
use deltalake::arrow::datatypes::{DataType, Schema as ArrowSchema, SchemaRef, TimeUnit};
use deltalake::arrow::record_batch::RecordBatch;
use deltalake::datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
use deltalake::datafusion::execution::memory_pool::FairSpillPool;
use deltalake::datafusion::execution::runtime_env::RuntimeEnvBuilder;
use deltalake::datafusion::prelude::{SessionConfig, SessionContext};
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
    /// Memory budget (MB) for the MERGE datafusion session's bounded FairSpillPool.
    merge_memory_mb: u64,
    /// Optional spill dir for the MERGE external sort; None = system temp.
    merge_spill_dir: Option<std::path::PathBuf>,
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
            merge_memory_mb: 512,
            merge_spill_dir: None,
        }
    }

    pub fn new_local(base_dir: &str) -> Self {
        Self {
            bucket: String::new(),
            prefix: base_dir.to_string(),
            storage_options: HashMap::new(),
            use_local_fs: true,
            merge_memory_mb: 512,
            merge_spill_dir: None,
        }
    }

    /// Override the MERGE memory budget + spill dir (called by the writer adapters from config).
    pub fn with_merge_limits(mut self, merge_memory_mb: u64, merge_spill_dir: Option<std::path::PathBuf>) -> Self {
        self.merge_memory_mb = merge_memory_mb;
        self.merge_spill_dir = merge_spill_dir;
        self
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

    pub async fn open_table(&self, table_name: &str) -> Result<DeltaTable> {
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
            Err(e) => {
                let is_new_table = matches!(
                    &e,
                    deltalake::DeltaTableError::NotATable(_)
                        | deltalake::DeltaTableError::InvalidTableLocation(_)
                ) || e.to_string().contains("does not exist");

                if is_new_table {
                    info!(table = table_name, "Creating new Delta table");
                    let delta_schema = arrow_schema_to_delta(&schema)?;

                    let table = deltalake::DeltaTableBuilder::from_url(url.clone())?
                        .with_storage_options(self.storage_options.clone())
                        .build()?;

                    let created = table.create()
                        .with_columns(delta_schema.fields().cloned())
                        .with_table_name(table_name)
                        .await?;

                    info!(table = table_name, "Delta table created");
                    Ok(created)
                } else {
                    Err(e).context(format!("S3 connection error for table {table_name}"))
                }
            }
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
        let mut table = deltalake::DeltaTableBuilder::from_url(url)?
            .with_storage_options(self.storage_options.clone())
            .build()?;
        table.load().await?;

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

        table.write(batches)
            .with_save_mode(SaveMode::Append)
            .with_commit_properties(commit_properties)
            .await?;

        info!(
            table = table_name,
            rows = total_rows,
            hwm_updated_at = ?hwm.as_ref().map(|h| h.updated_at.as_str()),
            hwm_last_id = ?hwm.as_ref().map(|h| h.last_id),
            "batch committed"
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
        let mut table = deltalake::DeltaTableBuilder::from_url(url)?
            .with_storage_options(self.storage_options.clone())
            .build()?;
        table.load().await?;

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

        table.write(batches)
            .with_save_mode(SaveMode::Overwrite)
            .with_commit_properties(commit_properties)
            .await?;

        info!(
            table = table_name,
            rows = total_rows,
            "table overwritten in Delta Lake"
        );
        Ok(())
    }

    /// Upsert `batches` into the table, matching on `key_col`: existing keys updated
    /// (non-key columns only), new keys inserted. Both stream watermarks ride the commit.
    /// The table must already exist (caller runs ensure_table first).
    ///
    /// Deduplicates the source by `key_col` before merging to prevent MERGE cardinality
    /// violations when the source contains duplicate keys.
    pub async fn merge_batch(
        &self,
        table_name: &str,
        batches: Vec<RecordBatch>,
        key_col: &str,
        insert_id: Option<i64>,
        update_hwm: Option<&Hwm>,
    ) -> Result<()> {
        if batches.is_empty() || batches.iter().all(|b| b.num_rows() == 0) {
            return Ok(());
        }
        let url = self.table_url(table_name)?;
        let mut table = deltalake::DeltaTableBuilder::from_url(url)?
            .with_storage_options(self.storage_options.clone())
            .build()?;
        table.load().await?;

        let schema = batches[0].schema();
        let merged = deltalake::arrow::compute::concat_batches(&schema, &batches)?;
        let total_rows = merged.num_rows();

        let pool_bytes = (self.merge_memory_mb as usize) * 1024 * 1024;
        // Route the external sort's spill to the configured dir (MERGE_SPILL_DIR); else system temp.
        let disk_builder = match &self.merge_spill_dir {
            Some(dir) => DiskManagerBuilder::default()
                .with_mode(DiskManagerMode::Directories(vec![dir.clone()])),
            None => DiskManagerBuilder::default(),
        };
        let runtime = std::sync::Arc::new(
            RuntimeEnvBuilder::new()
                .with_memory_pool(std::sync::Arc::new(FairSpillPool::new(pool_bytes)))
                .with_disk_manager_builder(disk_builder)
                .build()?,
        );
        let mut session_config = SessionConfig::new();
        // Force a spillable SortMergeJoin — datafusion 53's HashJoin does NOT spill, so under a
        // bounded pool it would error instead of spilling.
        session_config.options_mut().optimizer.prefer_hash_join = false;
        // Optional tuning: override datafusion's `sort_spill_reservation_bytes` (default 10 MB),
        // the memory reserved for the external sort's merge phase. On "Not enough memory to
        // continue external sort", a SMALLER value shrinks the merge's reservation request and
        // can let a bounded pool finish (datafusion's own hint). Read from env (advanced knob).
        if let Ok(raw) = std::env::var("MERGE_SORT_RESERVATION_MB")
            && let Ok(mb) = raw.trim().parse::<usize>()
        {
            session_config.options_mut().execution.sort_spill_reservation_bytes = mb * 1024 * 1024;
            info!(
                table = table_name,
                merge_sort_reservation_mb = mb,
                "merge: sort_spill_reservation_bytes overridden"
            );
        }
        // Pin the external sort to a single partition by default. datafusion runs one external
        // sorter per partition (default = CPU count), and they ALL share this one FairSpillPool,
        // so fan-out fragments the pool and the merge phase starves even with a large pool
        // ("Failed to allocate … for ExternalSorterMerge[N]"). One partition = one sorter owns the
        // whole pool → fewer runs, the merge fits. Override via MERGE_TARGET_PARTITIONS (>0).
        let merge_partitions = std::env::var("MERGE_TARGET_PARTITIONS")
            .ok()
            .and_then(|v| v.trim().parse::<usize>().ok())
            .filter(|&n| n > 0)
            .unwrap_or(1);
        session_config.options_mut().execution.target_partitions = merge_partitions;
        let ctx = SessionContext::new_with_config_rt(session_config, runtime);
        info!(
            table = table_name,
            merge_memory_mb = self.merge_memory_mb,
            merge_target_partitions = merge_partitions,
            "merge: bounded datafusion session (spills to disk)"
        );

        ctx.register_batch("merge_source_raw", merged)?;

        let col_names: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
        let col_list = col_names.join(", ");
        let dedup_sql = format!(
            "SELECT {col_list} FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY {key_col} ORDER BY {key_col}) AS __rn FROM merge_source_raw) WHERE __rn = 1"
        );
        let source = ctx.sql(&dedup_sql).await?;

        use deltalake::datafusion::prelude::col;
        let predicate = col(format!("target.{key_col}")).eq(col(format!("source.{key_col}")));
        let commit_properties = build_two_stream_commit_properties(insert_id, update_hwm);

        table
            .merge(source, predicate)
            .with_source_alias("source")
            .with_target_alias("target")
            .with_commit_properties(commit_properties)
            .with_session_state(std::sync::Arc::new(ctx.state()))
            .when_matched_update(|mut update| {
                for name in &col_names {
                    if name == key_col { continue; }
                    update = update.update(name.clone(), col(format!("source.{name}")));
                }
                update
            })?
            .when_not_matched_insert(|mut insert| {
                for name in &col_names {
                    insert = insert.set(name.clone(), col(format!("source.{name}")));
                }
                insert
            })?
            .await?;

        info!(table = table_name, rows = total_rows, "merge committed");
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

    /// Append new rows (insert stream of two-stream mode) carrying BOTH watermarks on
    /// the commit. Insert-stream rows are strictly new ids, so append (not merge) is
    /// correct and cheap.
    pub async fn append_two_stream(
        &self,
        table_name: &str,
        batches: Vec<RecordBatch>,
        insert_id: Option<i64>,
        update_hwm: Option<&Hwm>,
    ) -> Result<()> {
        if batches.is_empty() || batches.iter().all(|b| b.num_rows() == 0) {
            return Ok(());
        }
        let url = self.table_url(table_name)?;
        let mut table = deltalake::DeltaTableBuilder::from_url(url)?
            .with_storage_options(self.storage_options.clone())
            .build()?;
        table.load().await?;

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        let commit_properties = build_two_stream_commit_properties(insert_id, update_hwm);

        table
            .write(batches)
            .with_save_mode(SaveMode::Append)
            .with_commit_properties(commit_properties)
            .await?;

        info!(table = table_name, rows = total_rows, "two-stream insert appended");
        Ok(())
    }

    pub async fn read_insert_hwm(&self, table_name: &str) -> Result<Option<i64>> {
        let table = match self.open_table(table_name).await {
            Ok(t) => t,
            Err(_) => return Ok(None),
        };
        let mut history = table.history(Some(1)).await?.collect::<Vec<_>>();
        let commit_info = match history.pop() {
            Some(ci) => ci,
            None => return Ok(None),
        };
        match commit_info.info.get("hwm_insert_id") {
            Some(serde_json::Value::String(s)) =>
                Ok(Some(s.parse().context("invalid hwm_insert_id in commitInfo")?)),
            _ => Ok(None),
        }
    }

    /// Upsert via DELETE-by-key + APPEND (bounded-memory alternative to `merge_batch`).
    /// Deletes every target row whose `key_col` is in the incoming batch, then appends the
    /// batch (the new versions). The DELETE is a streaming scan+filter+rewrite (no join/sort),
    /// so memory stays bounded regardless of target size. The APPEND carries the commit
    /// properties (watermarks), so the HWM advances only after the append commits.
    ///
    /// Deduplicates the source by `key_col` before deleting/appending to prevent issues
    /// when the source contains duplicate keys (keeps one row per key).
    pub async fn delete_then_append(
        &self,
        table_name: &str,
        batches: Vec<RecordBatch>,
        key_col: &str,
        insert_id: Option<i64>,
        update_hwm: Option<&Hwm>,
    ) -> Result<()> {
        use std::collections::HashSet;
        use deltalake::datafusion::prelude::{cast, col, lit};

        if batches.is_empty() || batches.iter().all(|b| b.num_rows() == 0) {
            return Ok(());
        }
        let url = self.table_url(table_name)?;
        let mut table = deltalake::DeltaTableBuilder::from_url(url)?
            .with_storage_options(self.storage_options.clone())
            .build()?;
        table.load().await?;

        // Dedup the incoming batches by key_col (same pattern as merge_batch).
        let schema = batches[0].schema();
        let merged = deltalake::arrow::compute::concat_batches(&schema, &batches)?;
        let ctx = SessionContext::new();
        ctx.register_batch("delete_source_raw", merged)?;

        let col_names: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
        let col_list = col_names.join(", ");
        let dedup_sql = format!(
            "SELECT {col_list} FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY {key_col} ORDER BY {key_col}) AS __rn FROM delete_source_raw) WHERE __rn = 1"
        );
        let deduped_source = ctx.sql(&dedup_sql).await?;
        let deduped_batches = deduped_source.collect().await?;

        // Collect the distinct i64 keys present in the deduplicated batches.
        // Handle Int64Array, Int32Array, UInt64Array, UInt32Array.
        let mut ids: HashSet<i64> = HashSet::new();
        for b in &deduped_batches {
            let idx = b.schema().index_of(key_col)?;
            let c = b.column(idx);
            if let Some(a) = c.as_any().downcast_ref::<Int64Array>() {
                for i in 0..a.len() {
                    if !a.is_null(i) {
                        ids.insert(a.value(i));
                    }
                }
            } else if let Some(a) = c.as_any().downcast_ref::<Int32Array>() {
                for i in 0..a.len() {
                    if !a.is_null(i) {
                        ids.insert(a.value(i) as i64);
                    }
                }
            } else if let Some(a) = c.as_any().downcast_ref::<UInt64Array>() {
                for i in 0..a.len() {
                    if !a.is_null(i) {
                        ids.insert(a.value(i) as i64);
                    }
                }
            } else if let Some(a) = c.as_any().downcast_ref::<UInt32Array>() {
                for i in 0..a.len() {
                    if !a.is_null(i) {
                        ids.insert(a.value(i) as i64);
                    }
                }
            } else {
                return Err(anyhow::anyhow!(
                    "delete_then_append: key column `{key_col}` has unsupported type {:?} (expected an integer)",
                    c.data_type()
                ));
            }
        }

        // 1) DELETE the existing rows for those keys (streaming scan+filter+rewrite).
        let table = if ids.is_empty() {
            table
        } else {
            let list: Vec<_> = ids.iter().map(|id| lit(*id)).collect();
            let predicate = cast(col(key_col), DataType::Int64).in_list(list, false);
            let (t, _metrics) = table.delete().with_predicate(predicate).await?;
            info!(table = table_name, keys = ids.len(), "delete_then_append: deleted existing rows for incoming keys");
            t
        };

        // 2) APPEND the deduplicated versions; the watermarks ride on this commit.
        let total_rows: usize = deduped_batches.iter().map(|b| b.num_rows()).sum();
        let commit_properties = build_two_stream_commit_properties(insert_id, update_hwm);
        table
            .write(deduped_batches)
            .with_save_mode(SaveMode::Append)
            .with_commit_properties(commit_properties)
            .await?;
        info!(table = table_name, rows = total_rows, "delete_then_append: appended new versions");
        Ok(())
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

fn build_two_stream_commit_properties(
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

pub fn extract_hwm_from_batch(batch: &RecordBatch, timestamp_col: &str) -> Option<Hwm> {
    let timestamp_col_data = batch.column_by_name(timestamp_col)?;
    let id_col = batch.column_by_name("id")?;

    let n = batch.num_rows();
    if n == 0 {
        return None;
    }

    let timestamp_strings = extract_timestamp_as_strings(timestamp_col_data)?;
    let ids = extract_id_as_i64(id_col)?;

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

/// Max integer key in a batch — the insert-stream watermark. `key_col` is the
/// monotonic PK (e.g. `id`). None for an empty batch or unreadable column.
pub fn extract_max_id(batch: &RecordBatch, key_col: &str) -> Option<i64> {
    let col = batch.column_by_name(key_col)?;
    let ids = extract_id_as_i64(col)?;
    ids.into_iter().max()
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
    } else if let Some(ts) = col.as_any().downcast_ref::<TimestampNanosecondArray>() {
        Some(
            (0..ts.len())
                .map(|i| {
                    if ts.is_null(i) {
                        String::new()
                    } else {
                        nanos_to_string(ts.value(i))
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

// connector_arrow maps INT → Int32, BIGINT → Int64, INT UNSIGNED → UInt32,
// BIGINT UNSIGNED → UInt64. All fit safely in i64 for typical auto-increment ids.
fn extract_id_as_i64(col: &std::sync::Arc<dyn Array>) -> Option<Vec<i64>> {
    if let Some(a) = col.as_any().downcast_ref::<Int64Array>() {
        return Some((0..a.len()).map(|i| a.value(i)).collect());
    }
    if let Some(a) = col.as_any().downcast_ref::<Int32Array>() {
        return Some((0..a.len()).map(|i| a.value(i) as i64).collect());
    }
    if let Some(a) = col.as_any().downcast_ref::<UInt64Array>() {
        return Some((0..a.len()).map(|i| a.value(i) as i64).collect());
    }
    if let Some(a) = col.as_any().downcast_ref::<UInt32Array>() {
        return Some((0..a.len()).map(|i| a.value(i) as i64).collect());
    }
    None
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

fn nanos_to_string(nanos: i64) -> String {
    let secs = nanos / 1_000_000_000;
    let subsec_nanos = (nanos % 1_000_000_000).unsigned_abs() as u32;
    format_naive_datetime(secs, subsec_nanos)
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
        DataType::Timestamp(TimeUnit::Microsecond, None) => Ok(D::TIMESTAMP_NTZ),
        DataType::Timestamp(TimeUnit::Microsecond, Some(_)) => Ok(D::TIMESTAMP),
        DataType::Timestamp(TimeUnit::Millisecond, None) => Ok(D::TIMESTAMP_NTZ),
        DataType::Timestamp(TimeUnit::Millisecond, Some(_)) => Ok(D::TIMESTAMP),
        DataType::Timestamp(TimeUnit::Second, None) => Ok(D::TIMESTAMP_NTZ),
        DataType::Timestamp(TimeUnit::Second, Some(_)) => Ok(D::TIMESTAMP),
        DataType::Timestamp(TimeUnit::Nanosecond, None) => Ok(D::TIMESTAMP_NTZ),
        DataType::Timestamp(TimeUnit::Nanosecond, Some(_)) => Ok(D::TIMESTAMP),
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
        let hwm = extract_hwm_from_batch(&batch, "updated_at").unwrap();
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
        let hwm = extract_hwm_from_batch(&batch, "updated_at").unwrap();
        assert_eq!(hwm.last_id, 2);
    }

    #[test]
    fn extract_hwm_same_timestamp_picks_max_id() {
        let batch = make_batch_with_timestamps(
            vec![10, 50, 30],
            vec!["a", "b", "c"],
            vec![5000000i64, 5000000i64, 5000000i64],
        );
        let hwm = extract_hwm_from_batch(&batch, "updated_at").unwrap();
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
        let hwm = extract_hwm_from_batch(&batch, "updated_at").expect("Int32 id must produce a HWM");
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
        let hwm = extract_hwm_from_batch(&batch, "updated_at").expect("Utf8 timestamp must produce a HWM");
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

        assert!(extract_hwm_from_batch(&batch, "updated_at").is_none());
    }

    #[test]
    fn extract_hwm_missing_updated_at_returns_none() {
        let batch = make_batch_no_updated_at();
        assert!(extract_hwm_from_batch(&batch, "updated_at").is_none());
    }

    #[test]
    fn extract_hwm_missing_id_returns_none() {
        let batch = make_batch_no_id();
        assert!(extract_hwm_from_batch(&batch, "updated_at").is_none());
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

        let hwm = extract_hwm_from_batch(&batch, "updated_at").unwrap();
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

        let hwm = extract_hwm_from_batch(&batch, "updated_at").unwrap();
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

        let hwm = extract_hwm_from_batch(&batch, "updated_at").unwrap();
        assert_eq!(hwm.last_id, 1);
    }

    #[test]
    fn extract_hwm_descending_order() {
        let batch = make_batch_with_timestamps(
            vec![3, 2, 1],
            vec!["c", "b", "a"],
            vec![3000000i64, 2000000i64, 1000000i64],
        );
        let hwm = extract_hwm_from_batch(&batch, "updated_at").unwrap();
        assert_eq!(hwm.last_id, 3);
    }

    #[test]
    fn extract_hwm_same_ts_descending_id() {
        let batch = make_batch_with_timestamps(
            vec![30, 20, 10],
            vec!["c", "b", "a"],
            vec![5000000i64, 5000000i64, 5000000i64],
        );
        let hwm = extract_hwm_from_batch(&batch, "updated_at").unwrap();
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
        let r = arrow_type_to_delta(&DataType::Boolean);
        assert!(r.is_ok());
        assert_eq!(r.unwrap(), deltalake::kernel::DataType::BOOLEAN);

        let r = arrow_type_to_delta(&DataType::Int32);
        assert!(r.is_ok());
        assert_eq!(r.unwrap(), deltalake::kernel::DataType::INTEGER);

        let r = arrow_type_to_delta(&DataType::Int64);
        assert!(r.is_ok());
        assert_eq!(r.unwrap(), deltalake::kernel::DataType::LONG);

        let r = arrow_type_to_delta(&DataType::Float32);
        assert!(r.is_ok());
        assert_eq!(r.unwrap(), deltalake::kernel::DataType::FLOAT);

        let r = arrow_type_to_delta(&DataType::Float64);
        assert!(r.is_ok());
        assert_eq!(r.unwrap(), deltalake::kernel::DataType::DOUBLE);

        let r = arrow_type_to_delta(&DataType::Utf8);
        assert!(r.is_ok());
        assert_eq!(r.unwrap(), deltalake::kernel::DataType::STRING);

        let r = arrow_type_to_delta(&DataType::Date32);
        assert!(r.is_ok());
        assert_eq!(r.unwrap(), deltalake::kernel::DataType::DATE);

        let ts_result = arrow_type_to_delta(&DataType::Timestamp(TimeUnit::Microsecond, None));
        assert!(ts_result.is_ok());
        assert_eq!(ts_result.unwrap(), deltalake::kernel::DataType::TIMESTAMP_NTZ);
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

        let hwm = extract_hwm_from_batch(&batch, "updated_at").unwrap();
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

        let hwm = extract_hwm_from_batch(&batch, "updated_at").unwrap();
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

        let hwm = extract_hwm_from_batch(&batch, "updated_at").unwrap();
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

        assert!(extract_hwm_from_batch(&batch, "updated_at").is_none());
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

        let hwm = extract_hwm_from_batch(&batch, "updated_at").expect("Int32 id must produce a HWM");
        assert_eq!(hwm.last_id, 1);
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
    fn arrow_type_to_delta_timestamp_second() {
        let result = arrow_type_to_delta(&DataType::Timestamp(TimeUnit::Second, None));
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), deltalake::kernel::DataType::TIMESTAMP_NTZ);
    }

    #[test]
    fn arrow_type_to_delta_timestamp_micros() {
        let result = arrow_type_to_delta(&DataType::Timestamp(TimeUnit::Microsecond, None));
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), deltalake::kernel::DataType::TIMESTAMP_NTZ);
    }

    #[test]
    fn arrow_type_to_delta_timestamp_millis() {
        let result = arrow_type_to_delta(&DataType::Timestamp(TimeUnit::Millisecond, None));
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), deltalake::kernel::DataType::TIMESTAMP_NTZ);
    }


    #[test]
    fn arrow_type_to_delta_timestamp_nanos() {
        let result = arrow_type_to_delta(&DataType::Timestamp(TimeUnit::Nanosecond, None));
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), deltalake::kernel::DataType::TIMESTAMP_NTZ);
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

        let hwm = extract_hwm_from_batch(&batch, "updated_at").unwrap();
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

        let hwm = extract_hwm_from_batch(&batch, "completed_at").unwrap();
        assert_eq!(hwm.last_id, 3);
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

        let hwm = extract_hwm_from_batch(&batch, "updated_at").unwrap();
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

        assert!(extract_hwm_from_batch(&batch, "updated_at").is_none());
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

    #[tokio::test]
    async fn read_insert_hwm_nonexistent_table() {
        let temp = tempfile::tempdir().unwrap();
        let writer = DeltaWriter::new_local(temp.path().to_str().unwrap());
        let insert_hwm = writer.read_insert_hwm("nonexistent").await.unwrap();
        assert!(insert_hwm.is_none());
    }

    #[tokio::test]
    async fn read_insert_hwm_round_trip() {
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
            vec![Arc::new(Int64Array::from(vec![1i64, 2i64]))],
        )
        .unwrap();

        let update_hwm = Hwm {
            updated_at: "2026-03-28 10:00:00".to_string(),
            last_id: 5,
        };

        let table = writer.open_table("test_table").await.unwrap();
        table.write(vec![batch])
            .with_save_mode(SaveMode::Append)
            .with_commit_properties(build_two_stream_commit_properties(Some(42), Some(&update_hwm)))
            .await
            .unwrap();

        let insert_hwm = writer.read_insert_hwm("test_table").await.unwrap();
        assert_eq!(insert_hwm, Some(42));
    }

    #[tokio::test]
    async fn merge_batch_upsert_workflow() {
        use deltalake::arrow::array::StringViewArray;
        let temp = tempfile::tempdir().unwrap();
        let writer = DeltaWriter::new_local(temp.path().to_str().unwrap());
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Utf8, false),
        ]));

        writer
            .ensure_table("t", schema.clone())
            .await
            .unwrap();

        // First merge: insert (1,"a") and (2,"b")
        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1i64, 2i64])),
                Arc::new(StringArray::from(vec!["a", "b"])),
            ],
        )
        .unwrap();

        writer
            .merge_batch("t", vec![batch1], "id", Some(2), None)
            .await
            .unwrap();

        // Second merge: update (1,"A") and insert (3,"c")
        let batch2 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1i64, 3i64])),
                Arc::new(StringArray::from(vec!["A", "c"])),
            ],
        )
        .unwrap();

        writer
            .merge_batch("t", vec![batch2], "id", Some(3), None)
            .await
            .unwrap();

        // Read back and verify final state
        let t = writer.open_table("t").await.unwrap();
        let ctx = deltalake::datafusion::prelude::SessionContext::new();
        let provider = t.table_provider().await.unwrap();
        ctx.register_table("t", provider).unwrap();
        let batches = ctx
            .sql("SELECT id, value FROM t ORDER BY id")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 3);

        let id_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let value_col = batch.column(1);

        // Check id values
        assert_eq!(id_col.value(0), 1i64);
        assert_eq!(id_col.value(1), 2i64);
        assert_eq!(id_col.value(2), 3i64);

        // Check string values (may be StringArray or StringViewArray)
        let value_0 = if let Some(str_arr) = value_col.as_any().downcast_ref::<StringArray>() {
            str_arr.value(0).to_string()
        } else if let Some(str_view_arr) = value_col.as_any().downcast_ref::<StringViewArray>() {
            str_view_arr.value(0).to_string()
        } else {
            panic!("Unexpected value column type");
        };

        let value_1 = if let Some(str_arr) = value_col.as_any().downcast_ref::<StringArray>() {
            str_arr.value(1).to_string()
        } else if let Some(str_view_arr) = value_col.as_any().downcast_ref::<StringViewArray>() {
            str_view_arr.value(1).to_string()
        } else {
            panic!("Unexpected value column type");
        };

        let value_2 = if let Some(str_arr) = value_col.as_any().downcast_ref::<StringArray>() {
            str_arr.value(2).to_string()
        } else if let Some(str_view_arr) = value_col.as_any().downcast_ref::<StringViewArray>() {
            str_view_arr.value(2).to_string()
        } else {
            panic!("Unexpected value column type");
        };

        assert_eq!(value_0, "A");
        assert_eq!(value_1, "b");
        assert_eq!(value_2, "c");

        // Check insert hwm
        let insert_hwm = writer.read_insert_hwm("t").await.unwrap();
        assert_eq!(insert_hwm, Some(3));
    }

    #[tokio::test]
    async fn merge_batch_bounded_pool_preserves_correctness() {
        use deltalake::arrow::array::StringViewArray;
        let temp = tempfile::tempdir().unwrap();
        let spill = tempfile::tempdir().unwrap();
        let writer = DeltaWriter::new_local(temp.path().to_str().unwrap())
            .with_merge_limits(32, Some(spill.path().to_path_buf()));
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Utf8, false),
        ]));

        writer
            .ensure_table("t", schema.clone())
            .await
            .unwrap();

        // First merge: insert (1,"a") and (2,"b")
        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1i64, 2i64])),
                Arc::new(StringArray::from(vec!["a", "b"])),
            ],
        )
        .unwrap();

        writer
            .merge_batch("t", vec![batch1], "id", Some(2), None)
            .await
            .unwrap();

        // Second merge: update (1,"A") and insert (3,"c")
        let batch2 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1i64, 3i64])),
                Arc::new(StringArray::from(vec!["A", "c"])),
            ],
        )
        .unwrap();

        writer
            .merge_batch("t", vec![batch2], "id", Some(3), None)
            .await
            .unwrap();

        // Read back and verify final state
        let t = writer.open_table("t").await.unwrap();
        let ctx = deltalake::datafusion::prelude::SessionContext::new();
        let provider = t.table_provider().await.unwrap();
        ctx.register_table("t", provider).unwrap();
        let batches = ctx
            .sql("SELECT id, value FROM t ORDER BY id")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 3);

        let id_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let value_col = batch.column(1);

        // Check id values
        assert_eq!(id_col.value(0), 1i64);
        assert_eq!(id_col.value(1), 2i64);
        assert_eq!(id_col.value(2), 3i64);

        // Check string values (may be StringArray or StringViewArray)
        let value_0 = if let Some(str_arr) = value_col.as_any().downcast_ref::<StringArray>() {
            str_arr.value(0).to_string()
        } else if let Some(str_view_arr) = value_col.as_any().downcast_ref::<StringViewArray>() {
            str_view_arr.value(0).to_string()
        } else {
            panic!("Unexpected value column type");
        };

        let value_1 = if let Some(str_arr) = value_col.as_any().downcast_ref::<StringArray>() {
            str_arr.value(1).to_string()
        } else if let Some(str_view_arr) = value_col.as_any().downcast_ref::<StringViewArray>() {
            str_view_arr.value(1).to_string()
        } else {
            panic!("Unexpected value column type");
        };

        let value_2 = if let Some(str_arr) = value_col.as_any().downcast_ref::<StringArray>() {
            str_arr.value(2).to_string()
        } else if let Some(str_view_arr) = value_col.as_any().downcast_ref::<StringViewArray>() {
            str_view_arr.value(2).to_string()
        } else {
            panic!("Unexpected value column type");
        };

        assert_eq!(value_0, "A");
        assert_eq!(value_1, "b");
        assert_eq!(value_2, "c");

        // Check insert hwm
        let insert_hwm = writer.read_insert_hwm("t").await.unwrap();
        assert_eq!(insert_hwm, Some(3));
    }

    #[tokio::test]
    async fn merge_batch_empty_batches_noop() {
        let temp = tempfile::tempdir().unwrap();
        let writer = DeltaWriter::new_local(temp.path().to_str().unwrap());
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Utf8, false),
        ]));

        writer
            .ensure_table("t", schema.clone())
            .await
            .unwrap();

        // Merge with empty batch vector — should be no-op
        writer
            .merge_batch("t", vec![], "id", Some(1), None)
            .await
            .unwrap();

        let t = writer.open_table("t").await.unwrap();
        let ctx = deltalake::datafusion::prelude::SessionContext::new();
        let provider = t.table_provider().await.unwrap();
        ctx.register_table("t", provider).unwrap();
        let batches = ctx
            .sql("SELECT COUNT(*) as cnt FROM t")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        assert_eq!(batches.len(), 1);
        let cnt_col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<deltalake::arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(cnt_col.value(0), 0i64);
    }

    #[tokio::test]
    async fn merge_batch_dedup_duplicate_keys() {
        // Test that merge_batch deduplicates source keys, fixing cardinality violations.
        // Step F10.3a: Ensure MERGE never hits "matched a target row with multiple source rows".
        use deltalake::arrow::array::StringViewArray;
        let temp = tempfile::tempdir().unwrap();
        let writer = DeltaWriter::new_local(temp.path().to_str().unwrap());
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Utf8, false),
        ]));

        writer
            .ensure_table("t", schema.clone())
            .await
            .unwrap();

        // First merge: insert id=1 with value="a"
        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1i64])),
                Arc::new(StringArray::from(vec!["a"])),
            ],
        )
        .unwrap();

        writer
            .merge_batch("t", vec![batch1], "id", Some(1), None)
            .await
            .unwrap();

        // Second merge: source contains DUPLICATE keys: id=1 twice (values "X" and "Y") and id=2 once (value "b")
        // Before fix: MERGE would fail with "matched a target row with multiple source rows"
        // After fix (dedup): MERGE succeeds, keeping one row per key
        let batch2 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1i64, 1i64, 2i64])),
                Arc::new(StringArray::from(vec!["X", "Y", "b"])),
            ],
        )
        .unwrap();

        writer
            .merge_batch("t", vec![batch2], "id", Some(2), None)
            .await
            .unwrap();

        // Read back and verify
        let t = writer.open_table("t").await.unwrap();
        let ctx = deltalake::datafusion::prelude::SessionContext::new();
        let provider = t.table_provider().await.unwrap();
        ctx.register_table("t", provider).unwrap();
        let batches = ctx
            .sql("SELECT id, value FROM t ORDER BY id")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 2, "Should have 2 rows: id=1 and id=2");

        let id_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let value_col = batch.column(1);

        // Verify ids
        assert_eq!(id_col.value(0), 1i64);
        assert_eq!(id_col.value(1), 2i64);

        // Verify id=1 has been updated (dedup keeps one row deterministically)
        // and id=2 has been inserted
        let value_0 = if let Some(str_arr) = value_col.as_any().downcast_ref::<StringArray>() {
            str_arr.value(0).to_string()
        } else if let Some(str_view_arr) = value_col.as_any().downcast_ref::<StringViewArray>() {
            str_view_arr.value(0).to_string()
        } else {
            panic!("Unexpected value column type");
        };

        let value_1 = if let Some(str_arr) = value_col.as_any().downcast_ref::<StringArray>() {
            str_arr.value(1).to_string()
        } else if let Some(str_view_arr) = value_col.as_any().downcast_ref::<StringViewArray>() {
            str_view_arr.value(1).to_string()
        } else {
            panic!("Unexpected value column type");
        };

        // id=1 should have one of the deduped values (dedup keeps first by row order)
        assert!(value_0 == "X" || value_0 == "Y", "id=1 value should be X or Y, got {}", value_0);
        assert_eq!(value_1, "b", "id=2 should have value 'b'");
    }

    #[tokio::test]
    async fn append_two_stream_inserts_rows_with_both_watermarks() {
        let temp = tempfile::tempdir().unwrap();
        let writer = DeltaWriter::new_local(temp.path().to_str().unwrap());
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Utf8, false),
        ]));

        writer
            .ensure_table("t", schema.clone())
            .await
            .unwrap();

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1i64, 2i64])),
                Arc::new(StringArray::from(vec!["a", "b"])),
            ],
        )
        .unwrap();

        writer
            .append_two_stream("t", vec![batch], Some(2), None)
            .await
            .unwrap();

        // Verify insert_id is recorded
        let insert_hwm = writer.read_insert_hwm("t").await.unwrap();
        assert_eq!(insert_hwm, Some(2));

        // Verify rows are in the table
        let t = writer.open_table("t").await.unwrap();
        let ctx = deltalake::datafusion::prelude::SessionContext::new();
        let provider = t.table_provider().await.unwrap();
        ctx.register_table("t", provider).unwrap();
        let batches = ctx
            .sql("SELECT id FROM t ORDER BY id")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 2);

        let id_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(id_col.value(0), 1i64);
        assert_eq!(id_col.value(1), 2i64);
    }

    #[tokio::test]
    async fn append_two_stream_empty_batches_noop() {
        let temp = tempfile::tempdir().unwrap();
        let writer = DeltaWriter::new_local(temp.path().to_str().unwrap());
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Utf8, false),
        ]));

        writer
            .ensure_table("t", schema.clone())
            .await
            .unwrap();

        // Append with empty batch vector — should be no-op
        writer
            .append_two_stream("t", vec![], Some(1), None)
            .await
            .unwrap();

        let t = writer.open_table("t").await.unwrap();
        let ctx = deltalake::datafusion::prelude::SessionContext::new();
        let provider = t.table_provider().await.unwrap();
        ctx.register_table("t", provider).unwrap();
        let batches = ctx
            .sql("SELECT COUNT(*) as cnt FROM t")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        assert_eq!(batches.len(), 1);
        let cnt_col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<deltalake::arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(cnt_col.value(0), 0i64);
    }

    #[tokio::test]
    async fn delete_then_append_upsert_workflow() {
        use deltalake::arrow::array::StringViewArray;
        let temp = tempfile::tempdir().unwrap();
        let writer = DeltaWriter::new_local(temp.path().to_str().unwrap());
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Utf8, false),
        ]));

        writer
            .ensure_table("t", schema.clone())
            .await
            .unwrap();

        // First append: insert (1,"a") and (2,"b")
        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1i64, 2i64])),
                Arc::new(StringArray::from(vec!["a", "b"])),
            ],
        )
        .unwrap();

        writer
            .append_two_stream("t", vec![batch1], Some(2), None)
            .await
            .unwrap();

        // Second operation: delete-then-append with (1,"A") and (3,"c")
        // This deletes id=1 and appends id=1 with new value and id=3 as new row
        let batch2 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1i64, 3i64])),
                Arc::new(StringArray::from(vec!["A", "c"])),
            ],
        )
        .unwrap();

        writer
            .delete_then_append("t", vec![batch2], "id", Some(3), None)
            .await
            .unwrap();

        // Read back and verify final state
        let t = writer.open_table("t").await.unwrap();
        let ctx = deltalake::datafusion::prelude::SessionContext::new();
        let provider = t.table_provider().await.unwrap();
        ctx.register_table("t", provider).unwrap();
        let batches = ctx
            .sql("SELECT id, value FROM t ORDER BY id")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 3);

        let id_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let value_col = batch.column(1);

        // Check id values
        assert_eq!(id_col.value(0), 1i64);
        assert_eq!(id_col.value(1), 2i64);
        assert_eq!(id_col.value(2), 3i64);

        // Check string values (may be StringArray or StringViewArray)
        let value_0 = if let Some(str_arr) = value_col.as_any().downcast_ref::<StringArray>() {
            str_arr.value(0).to_string()
        } else if let Some(str_view_arr) = value_col.as_any().downcast_ref::<StringViewArray>() {
            str_view_arr.value(0).to_string()
        } else {
            panic!("Unexpected value column type");
        };

        let value_1 = if let Some(str_arr) = value_col.as_any().downcast_ref::<StringArray>() {
            str_arr.value(1).to_string()
        } else if let Some(str_view_arr) = value_col.as_any().downcast_ref::<StringViewArray>() {
            str_view_arr.value(1).to_string()
        } else {
            panic!("Unexpected value column type");
        };

        let value_2 = if let Some(str_arr) = value_col.as_any().downcast_ref::<StringArray>() {
            str_arr.value(2).to_string()
        } else if let Some(str_view_arr) = value_col.as_any().downcast_ref::<StringViewArray>() {
            str_view_arr.value(2).to_string()
        } else {
            panic!("Unexpected value column type");
        };

        assert_eq!(value_0, "A");
        assert_eq!(value_1, "b");
        assert_eq!(value_2, "c");

        // Check insert hwm
        let insert_hwm = writer.read_insert_hwm("t").await.unwrap();
        assert_eq!(insert_hwm, Some(3));
    }

    #[tokio::test]
    async fn delete_then_append_upsert_uint64_key() {
        use deltalake::arrow::array::StringViewArray;
        let temp = tempfile::tempdir().unwrap();
        let writer = DeltaWriter::new_local(temp.path().to_str().unwrap());
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::UInt64, false),
            Field::new("value", DataType::Utf8, false),
        ]));

        writer
            .ensure_table("t", schema.clone())
            .await
            .unwrap();

        // First append: insert (1,"a") and (2,"b")
        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from(vec![1u64, 2u64])),
                Arc::new(StringArray::from(vec!["a", "b"])),
            ],
        )
        .unwrap();

        writer
            .append_two_stream("t", vec![batch1], Some(2), None)
            .await
            .unwrap();

        // Second operation: delete-then-append with (1,"A") and (3,"c")
        // This deletes id=1 and appends id=1 with new value and id=3 as new row
        let batch2 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from(vec![1u64, 3u64])),
                Arc::new(StringArray::from(vec!["A", "c"])),
            ],
        )
        .unwrap();

        writer
            .delete_then_append("t", vec![batch2], "id", Some(3), None)
            .await
            .unwrap();

        // Read back and verify final state
        let t = writer.open_table("t").await.unwrap();
        let ctx = deltalake::datafusion::prelude::SessionContext::new();
        let provider = t.table_provider().await.unwrap();
        ctx.register_table("t", provider).unwrap();
        let batches = ctx
            .sql("SELECT id, value FROM t ORDER BY id")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 3);

        let id_col_raw = batch.column(0);
        let value_col = batch.column(1);

        // Check id values - handle both UInt64Array and Int64Array conversions
        let (id_0, id_1, id_2) = if let Some(arr) = id_col_raw.as_any().downcast_ref::<UInt64Array>() {
            (arr.value(0), arr.value(1), arr.value(2))
        } else if let Some(arr) = id_col_raw.as_any().downcast_ref::<Int64Array>() {
            (arr.value(0) as u64, arr.value(1) as u64, arr.value(2) as u64)
        } else {
            panic!("Unexpected id column type");
        };
        assert_eq!(id_0, 1u64);
        assert_eq!(id_1, 2u64);
        assert_eq!(id_2, 3u64);

        // Check string values (may be StringArray or StringViewArray)
        let value_0 = if let Some(str_arr) = value_col.as_any().downcast_ref::<StringArray>() {
            str_arr.value(0).to_string()
        } else if let Some(str_view_arr) = value_col.as_any().downcast_ref::<StringViewArray>() {
            str_view_arr.value(0).to_string()
        } else {
            panic!("Unexpected value column type");
        };

        let value_1 = if let Some(str_arr) = value_col.as_any().downcast_ref::<StringArray>() {
            str_arr.value(1).to_string()
        } else if let Some(str_view_arr) = value_col.as_any().downcast_ref::<StringViewArray>() {
            str_view_arr.value(1).to_string()
        } else {
            panic!("Unexpected value column type");
        };

        let value_2 = if let Some(str_arr) = value_col.as_any().downcast_ref::<StringArray>() {
            str_arr.value(2).to_string()
        } else if let Some(str_view_arr) = value_col.as_any().downcast_ref::<StringViewArray>() {
            str_view_arr.value(2).to_string()
        } else {
            panic!("Unexpected value column type");
        };

        assert_eq!(value_0, "A");
        assert_eq!(value_1, "b");
        assert_eq!(value_2, "c");

        // Check insert hwm
        let insert_hwm = writer.read_insert_hwm("t").await.unwrap();
        assert_eq!(insert_hwm, Some(3));
    }

    #[tokio::test]
    async fn delete_then_append_dedups_duplicate_keys() {
        use deltalake::arrow::array::StringViewArray;
        let temp = tempfile::tempdir().unwrap();
        let writer = DeltaWriter::new_local(temp.path().to_str().unwrap());
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Utf8, false),
        ]));

        writer
            .ensure_table("t", schema.clone())
            .await
            .unwrap();

        // Batch with DUPLICATE keys: id=1 appears twice (values "x" and "y"), id=2 once (value "b")
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1i64, 1i64, 2i64])),
                Arc::new(StringArray::from(vec!["x", "y", "b"])),
            ],
        )
        .unwrap();

        // delete_then_append should dedup and keep only one row per key
        writer
            .delete_then_append("t", vec![batch], "id", Some(2), None)
            .await
            .unwrap();

        // Read back and verify
        let t = writer.open_table("t").await.unwrap();
        let ctx = deltalake::datafusion::prelude::SessionContext::new();
        let provider = t.table_provider().await.unwrap();
        ctx.register_table("t", provider).unwrap();
        let batches = ctx
            .sql("SELECT id, value FROM t ORDER BY id")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        // After dedup: exactly 2 rows (id=1 once, id=2 once)
        assert_eq!(batch.num_rows(), 2, "Should have exactly 2 rows after dedup");

        let id_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let value_col = batch.column(1);

        // Check ids
        assert_eq!(id_col.value(0), 1i64);
        assert_eq!(id_col.value(1), 2i64);

        // Check that id=1 has one of the deduped values (either "x" or "y")
        let value_0 = if let Some(str_arr) = value_col.as_any().downcast_ref::<StringArray>() {
            str_arr.value(0).to_string()
        } else if let Some(str_view_arr) = value_col.as_any().downcast_ref::<StringViewArray>() {
            str_view_arr.value(0).to_string()
        } else {
            panic!("Unexpected value column type");
        };

        let value_1 = if let Some(str_arr) = value_col.as_any().downcast_ref::<StringArray>() {
            str_arr.value(1).to_string()
        } else if let Some(str_view_arr) = value_col.as_any().downcast_ref::<StringViewArray>() {
            str_view_arr.value(1).to_string()
        } else {
            panic!("Unexpected value column type");
        };

        // id=1 should have one of the deduplicated values
        assert!(
            value_0 == "x" || value_0 == "y",
            "id=1 value should be 'x' or 'y', got '{}'",
            value_0
        );
        // id=2 should have value "b"
        assert_eq!(value_1, "b", "id=2 should have value 'b'");
    }

    #[tokio::test]
    async fn delete_then_append_upsert_int32_key() {
        use deltalake::arrow::array::StringViewArray;
        let temp = tempfile::tempdir().unwrap();
        let writer = DeltaWriter::new_local(temp.path().to_str().unwrap());
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Utf8, false),
        ]));

        writer
            .ensure_table("t", schema.clone())
            .await
            .unwrap();

        // First append: insert (1,"a") and (2,"b")
        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1i32, 2i32])),
                Arc::new(StringArray::from(vec!["a", "b"])),
            ],
        )
        .unwrap();

        writer
            .append_two_stream("t", vec![batch1], Some(2), None)
            .await
            .unwrap();

        // Second operation: delete-then-append with (1,"A") and (3,"c")
        let batch2 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1i32, 3i32])),
                Arc::new(StringArray::from(vec!["A", "c"])),
            ],
        )
        .unwrap();

        writer
            .delete_then_append("t", vec![batch2], "id", Some(3), None)
            .await
            .unwrap();

        // Read back and verify final state
        let t = writer.open_table("t").await.unwrap();
        let ctx = deltalake::datafusion::prelude::SessionContext::new();
        let provider = t.table_provider().await.unwrap();
        ctx.register_table("t", provider).unwrap();
        let batches = ctx
            .sql("SELECT id, value FROM t ORDER BY id")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 3);

        let id_col_raw = batch.column(0);
        let value_col = batch.column(1);

        // Check id values - handle both Int32Array and Int64Array conversions
        let (id_0, id_1, id_2) = if let Some(arr) = id_col_raw.as_any().downcast_ref::<Int32Array>() {
            (arr.value(0) as i64, arr.value(1) as i64, arr.value(2) as i64)
        } else if let Some(arr) = id_col_raw.as_any().downcast_ref::<Int64Array>() {
            (arr.value(0), arr.value(1), arr.value(2))
        } else {
            panic!("Unexpected id column type");
        };
        assert_eq!(id_0, 1i64);
        assert_eq!(id_1, 2i64);
        assert_eq!(id_2, 3i64);

        // Check string values (may be StringArray or StringViewArray)
        let value_0 = if let Some(str_arr) = value_col.as_any().downcast_ref::<StringArray>() {
            str_arr.value(0).to_string()
        } else if let Some(str_view_arr) = value_col.as_any().downcast_ref::<StringViewArray>() {
            str_view_arr.value(0).to_string()
        } else {
            panic!("Unexpected value column type");
        };

        let value_1 = if let Some(str_arr) = value_col.as_any().downcast_ref::<StringArray>() {
            str_arr.value(1).to_string()
        } else if let Some(str_view_arr) = value_col.as_any().downcast_ref::<StringViewArray>() {
            str_view_arr.value(1).to_string()
        } else {
            panic!("Unexpected value column type");
        };

        let value_2 = if let Some(str_arr) = value_col.as_any().downcast_ref::<StringArray>() {
            str_arr.value(2).to_string()
        } else if let Some(str_view_arr) = value_col.as_any().downcast_ref::<StringViewArray>() {
            str_view_arr.value(2).to_string()
        } else {
            panic!("Unexpected value column type");
        };

        assert_eq!(value_0, "A");
        assert_eq!(value_1, "b");
        assert_eq!(value_2, "c");

        // Check insert hwm
        let insert_hwm = writer.read_insert_hwm("t").await.unwrap();
        assert_eq!(insert_hwm, Some(3));
    }

    #[tokio::test]
    async fn delete_then_append_upsert_uint32_key() {
        use deltalake::arrow::array::StringViewArray;
        let temp = tempfile::tempdir().unwrap();
        let writer = DeltaWriter::new_local(temp.path().to_str().unwrap());
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::UInt32, false),
            Field::new("value", DataType::Utf8, false),
        ]));

        writer
            .ensure_table("t", schema.clone())
            .await
            .unwrap();

        // First append: insert (1,"a") and (2,"b")
        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt32Array::from(vec![1u32, 2u32])),
                Arc::new(StringArray::from(vec!["a", "b"])),
            ],
        )
        .unwrap();

        writer
            .append_two_stream("t", vec![batch1], Some(2), None)
            .await
            .unwrap();

        // Second operation: delete-then-append with (1,"A") and (3,"c")
        let batch2 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt32Array::from(vec![1u32, 3u32])),
                Arc::new(StringArray::from(vec!["A", "c"])),
            ],
        )
        .unwrap();

        writer
            .delete_then_append("t", vec![batch2], "id", Some(3), None)
            .await
            .unwrap();

        // Read back and verify final state
        let t = writer.open_table("t").await.unwrap();
        let ctx = deltalake::datafusion::prelude::SessionContext::new();
        let provider = t.table_provider().await.unwrap();
        ctx.register_table("t", provider).unwrap();
        let batches = ctx
            .sql("SELECT id, value FROM t ORDER BY id")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 3);

        let id_col_raw = batch.column(0);
        let value_col = batch.column(1);

        // Check id values - handle UInt32/UInt64/Int32/Int64 array conversions
        let (id_0, id_1, id_2) = if let Some(arr) = id_col_raw.as_any().downcast_ref::<UInt32Array>() {
            (arr.value(0) as u64, arr.value(1) as u64, arr.value(2) as u64)
        } else if let Some(arr) = id_col_raw.as_any().downcast_ref::<UInt64Array>() {
            (arr.value(0), arr.value(1), arr.value(2))
        } else if let Some(arr) = id_col_raw.as_any().downcast_ref::<Int32Array>() {
            (arr.value(0) as u64, arr.value(1) as u64, arr.value(2) as u64)
        } else if let Some(arr) = id_col_raw.as_any().downcast_ref::<Int64Array>() {
            (arr.value(0) as u64, arr.value(1) as u64, arr.value(2) as u64)
        } else {
            panic!("Unexpected id column type");
        };
        assert_eq!(id_0, 1u64);
        assert_eq!(id_1, 2u64);
        assert_eq!(id_2, 3u64);

        // Check string values (may be StringArray or StringViewArray)
        let value_0 = if let Some(str_arr) = value_col.as_any().downcast_ref::<StringArray>() {
            str_arr.value(0).to_string()
        } else if let Some(str_view_arr) = value_col.as_any().downcast_ref::<StringViewArray>() {
            str_view_arr.value(0).to_string()
        } else {
            panic!("Unexpected value column type");
        };

        let value_1 = if let Some(str_arr) = value_col.as_any().downcast_ref::<StringArray>() {
            str_arr.value(1).to_string()
        } else if let Some(str_view_arr) = value_col.as_any().downcast_ref::<StringViewArray>() {
            str_view_arr.value(1).to_string()
        } else {
            panic!("Unexpected value column type");
        };

        let value_2 = if let Some(str_arr) = value_col.as_any().downcast_ref::<StringArray>() {
            str_arr.value(2).to_string()
        } else if let Some(str_view_arr) = value_col.as_any().downcast_ref::<StringViewArray>() {
            str_view_arr.value(2).to_string()
        } else {
            panic!("Unexpected value column type");
        };

        assert_eq!(value_0, "A");
        assert_eq!(value_1, "b");
        assert_eq!(value_2, "c");

        // Check insert hwm
        let insert_hwm = writer.read_insert_hwm("t").await.unwrap();
        assert_eq!(insert_hwm, Some(3));
    }

    #[tokio::test]
    async fn delete_then_append_empty_batch_noop() {
        let temp = tempfile::tempdir().unwrap();
        let writer = DeltaWriter::new_local(temp.path().to_str().unwrap());
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Utf8, false),
        ]));

        writer
            .ensure_table("t", schema.clone())
            .await
            .unwrap();

        // delete_then_append with an empty batch vector — should be a no-op
        let result = writer
            .delete_then_append("t", vec![], "id", None, None)
            .await;
        assert!(result.is_ok());

        let t = writer.open_table("t").await.unwrap();
        let ctx = deltalake::datafusion::prelude::SessionContext::new();
        let provider = t.table_provider().await.unwrap();
        ctx.register_table("t", provider).unwrap();
        let batches = ctx
            .sql("SELECT COUNT(*) as cnt FROM t")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        assert_eq!(batches.len(), 1);
        let cnt_col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<deltalake::arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(cnt_col.value(0), 0i64);
    }
}
