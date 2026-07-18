use std::collections::HashMap;

use anyhow::{Context, Result};
use deltalake::arrow::datatypes::SchemaRef;
#[cfg(test)]
use deltalake::arrow::array::{Int64Array, StringArray};
#[cfg(test)]
use deltalake::arrow::datatypes::{DataType, Schema as ArrowSchema};
use deltalake::arrow::record_batch::RecordBatch;
use deltalake::DeltaTable;
use deltalake::kernel::{Action, CommitInfo, MetadataExt, StructType};
use deltalake::kernel::transaction::CommitBuilder;
use deltalake::operations::write::SchemaMode;
use deltalake::protocol::{DeltaOperation, SaveMode};
use deltalake::writer::{DeltaWriter as _, RecordBatchWriter};
use tokio::sync::Mutex;
use tracing::{info, warn};
use url::Url;

mod datetime;
mod hwm;
mod schema;
mod two_stream;

use hwm::{build_commit_properties, build_two_stream_commit_properties};
pub use hwm::{extract_hwm_from_batch, extract_max_id, hwm_has_advanced};
use schema::*;

/// True only when the error means the Delta table does not exist yet (so "no HWM" is
/// the correct answer). Mirrors the classification `ensure_table` uses to decide it must
/// create the table. Any OTHER error (transient S3, auth, network) must NOT be treated
/// as "missing" — returning None there causes a from-scratch re-extract and duplicate rows.
pub(crate) fn is_missing_table_error(e: &anyhow::Error) -> bool {
    if let Some(dte) = e.downcast_ref::<deltalake::DeltaTableError>()
        && matches!(
            dte,
            deltalake::DeltaTableError::NotATable(_)
                | deltalake::DeltaTableError::InvalidTableLocation(_)
        )
    {
        return true;
    }
    let err_str = e.to_string();
    err_str.contains("does not exist") || err_str.contains("Invalid table location")
}

/// L7: how many of the most-recent commits `read_hwm`/`read_insert_hwm` scan backward through
/// looking for the last commit that actually carries the watermark key(s). Bounds the lookback
/// so a pathological history (many non-HWM commits in a row) can't make a read scan unboundedly
/// far back. Not finding the key(s) within this window falls back to today's existing safe
/// behavior: `None` (starts a from-scratch re-extract, exactly like a missing HWM always has).
pub(crate) const HWM_LOOKBACK_COMMITS: usize = 64;

/// L7: scan `history` (as returned by `DeltaTable::history`, which orders MOST-RECENT-FIRST) for
/// the newest commit whose `info` carries every one of `keys`. A Delta `OPTIMIZE` / `VACUUM` /
/// checkpoint housekeeping commit carries none of the `hwm_*` keys, so without this scan it would
/// silently SHADOW the real watermark stamped by an older commit — `read_hwm`/`read_insert_hwm`
/// would see the newest (keyless) commit and wrongly report "no HWM". Returns the matching commit
/// alongside how many newer commits were skipped to reach it (0 = the newest commit itself
/// carried the keys, the common case and today's pre-L7 behavior).
pub(crate) fn find_commit_with_keys<'a>(
    history: &'a [CommitInfo],
    keys: &[&str],
) -> Option<(&'a CommitInfo, usize)> {
    history
        .iter()
        .enumerate()
        .find(|(_, ci)| keys.iter().all(|k| ci.info.contains_key(*k)))
        .map(|(idx, ci)| (ci, idx))
}

#[derive(Debug, Clone)]
pub struct Hwm {
    pub updated_at: String,
    pub last_id: i64,
}

/// O2-r CP1: state for one in-flight staged-overwrite session on a single table. Holds the
/// `RecordBatchWriter` across multiple `stage_overwrite_chunk` calls (each `flush()` writes
/// buffered rows to parquet FILES without committing) and accumulates the resulting `Add`
/// actions until `commit_overwrite` folds them, plus every current file's `Remove`, into one
/// atomic commit.
struct OverwriteSession {
    writer: RecordBatchWriter,
    /// Loaded handle for the table being overwritten; used at commit time for the current
    /// snapshot's file list (the Removes) and the log store.
    table: DeltaTable,
    adds: Vec<deltalake::kernel::Add>,
    /// FA3: `Some(new_struct)` when `begin_overwrite` found the current source schema differs
    /// from the table's stored Delta schema — `commit_overwrite` then folds a `Metadata` action
    /// carrying `new_struct` into the same atomic commit, REPLACING the schema. `None` in the
    /// common (unchanged-schema) case, where behavior is byte-identical to before FA3.
    schema_change: Option<StructType>,
}

/// O2-r: coerce a batch to `RecordBatchWriter`'s exact target schema. Unlike the old
/// `table.write()` path (DataFusion, which casts + relabels the input to the table schema),
/// `RecordBatchWriter` validates each batch's schema STRICTLY — so a NOT NULL source column
/// (non-nullable Arrow field) or a narrower integer type is rejected. For each target field:
/// find its column in the batch by NAME, cast it to the target type when they differ (a safe
/// widening to the Delta type — e.g. Int16→Int32 as N5 intends), and rebuild under the target
/// schema (which also aligns field nullability). Errors if a target column is absent from the batch.
///
/// FA1: the cast uses `CastOptions { safe: false }` — arrow's kernel ERRORS on any value that
/// doesn't fit the target type instead of silently substituting NULL (`safe: true`, arrow's
/// default). Without this, a full-refresh table whose source type drifted narrower than its Delta
/// column (e.g. a value now exceeding the stored INTEGER range) would write silent NULLs, exit 0 —
/// the exact silent-corruption class N5's `align_batches_to_schema` (which also uses `safe: false`)
/// exists to prevent. A data-losing narrowing now fails the table loudly and actionably instead.
fn coerce_batch_to_schema(
    batch: &RecordBatch,
    target: &deltalake::arrow::datatypes::SchemaRef,
) -> Result<RecordBatch> {
    use deltalake::arrow::compute::{cast_with_options, CastOptions};
    let cast_opts = CastOptions { safe: false, ..Default::default() };
    let mut columns = Vec::with_capacity(target.fields().len());
    for field in target.fields() {
        let col = batch.column_by_name(field.name()).ok_or_else(|| {
            anyhow::anyhow!(
                "atomic overwrite: column `{}` expected by the Delta schema is missing from the extracted batch",
                field.name()
            )
        })?;
        let coerced = if col.data_type() == field.data_type() {
            col.clone()
        } else {
            cast_with_options(col, field.data_type(), &cast_opts).map_err(|e| {
                anyhow::anyhow!(
                    "atomic overwrite: column `{}` value does not fit the Delta column type {:?} \
                     (source type {:?}); a full refresh cannot narrow it without data loss — {e}",
                    field.name(),
                    field.data_type(),
                    col.data_type()
                )
            })?
        };
        columns.push(coerced);
    }
    Ok(RecordBatch::try_new(target.clone(), columns)?)
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
    /// P1: per-table DeltaTable handle reuse across the per-batch write loop, so each
    /// commit doesn't rebuild + full-`load()` the handle from the `_delta_log`. Arc so the
    /// writer can be shared; the handle is TAKEN on acquire and the post-commit handle
    /// STORED back (single-writer-per-table ⇒ always current, no incremental update needed).
    table_cache: std::sync::Arc<Mutex<HashMap<String, DeltaTable>>>,
    /// O2-r CP1: per-table staged-overwrite sessions (`begin_overwrite` / `stage_overwrite_chunk`
    /// / `commit_overwrite`). Additive; not yet driven by production code in this checkpoint.
    overwrite_sessions: std::sync::Arc<Mutex<HashMap<String, OverwriteSession>>>,
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
            table_cache: std::sync::Arc::new(Mutex::new(HashMap::new())),
            overwrite_sessions: std::sync::Arc::new(Mutex::new(HashMap::new())),
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
            table_cache: std::sync::Arc::new(Mutex::new(HashMap::new())),
            overwrite_sessions: std::sync::Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Override the MERGE memory budget + spill dir (called by the writer adapters from config).
    pub fn with_merge_limits(mut self, merge_memory_mb: u64, merge_spill_dir: Option<std::path::PathBuf>) -> Self {
        self.merge_memory_mb = merge_memory_mb;
        self.merge_spill_dir = merge_spill_dir;
        self
    }

    /// Memory budget (MB) for bounded datafusion sessions (MERGE and the verify probes).
    pub(crate) fn merge_memory_mb(&self) -> u64 {
        self.merge_memory_mb
    }

    /// Spill dir for bounded datafusion sessions; None = system temp.
    pub(crate) fn merge_spill_dir(&self) -> Option<&std::path::Path> {
        self.merge_spill_dir.as_deref()
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

    /// Whether the Delta table exists AND holds at least one data file. Used by the
    /// no-HWM guards (audit H-2026-07-11-1): an incremental / two-stream run that has
    /// no stored watermark must not re-extract from scratch with APPEND onto a table
    /// that already has data — that duplicates every row. A genuinely missing table
    /// (first run) is `false`, as is a freshly created empty one (ensure_table runs
    /// before extraction, so the first run sees zero files here).
    pub async fn has_data(&self, table_name: &str) -> Result<bool> {
        let table = match self.open_table(table_name).await {
            Ok(t) => t,
            Err(e) => {
                if is_missing_table_error(&e) {
                    return Ok(false);
                }
                return Err(e).context(format!(
                    "has_data: could not open Delta table `{table_name}`"
                ));
            }
        };
        Ok(table.get_file_uris()?.next().is_some())
    }

    pub async fn open_table(&self, table_name: &str) -> Result<DeltaTable> {
        let url = self.table_url(table_name)?;
        let mut table = deltalake::DeltaTableBuilder::from_url(url)?
            .with_storage_options(self.storage_options.clone())
            .build()?;
        table.load().await?;
        Ok(table)
    }

    /// P1: take the cached handle for `table_name` (removing it so a consuming write op can
    /// own it), or fresh-load one. Structured so the mutex guard is never held across the
    /// `open_table` await.
    async fn take_cached_table(&self, table_name: &str) -> Result<DeltaTable> {
        let cached = self.table_cache.lock().await.remove(table_name);
        match cached {
            Some(t) => Ok(t),
            None => self.open_table(table_name).await,
        }
    }

    /// P1: store the post-commit handle so the next write in the loop reuses it.
    async fn cache_store(&self, table_name: &str, table: DeltaTable) {
        self.table_cache.lock().await.insert(table_name.to_string(), table);
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

        let table = self.take_cached_table(table_name).await?;

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

        let table = table
            .write(batches)
            .with_save_mode(SaveMode::Append)
            // D1: additive schema evolution. `schema_evolution_check` guarantees this batch's
            // schema is a SUPERSET of the Delta table's (new extractable source columns are
            // included; drops and type changes still bail before we get here), so Merge grows
            // the table by any new column and old rows read that column back as NULL. When the
            // batch schema already equals the table's (the normal case) Merge is a no-op.
            .with_schema_mode(SchemaMode::Merge)
            .with_commit_properties(commit_properties)
            .await?;

        info!(
            table = table_name,
            rows = total_rows,
            hwm_updated_at = ?hwm.as_ref().map(|h| h.updated_at.as_str()),
            hwm_last_id = ?hwm.as_ref().map(|h| h.last_id),
            "batch committed"
        );
        self.cache_store(table_name, table).await;
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

    /// O2-r CP1 / FA3: start a staged-overwrite session for `table_name`. Loads the table's
    /// current handle fresh, then decides what schema the staged `RecordBatchWriter` targets:
    ///
    /// A full refresh rewrites 100% of the data, so it SHOULD always adopt the CURRENT source
    /// schema (`target_schema`) — a new source column appears, a dropped one disappears, and a
    /// type widening is adopted, all atomically. `target_schema` is converted to Delta's
    /// `StructType` and compared (in Delta type-space, so an N5 widening the table was already
    /// created with doesn't look like a change) against the table's CURRENTLY STORED schema:
    /// - unchanged: behavior is byte-identical to before FA3 — `RecordBatchWriter::for_table`
    ///   (targets the table's existing schema), no `Metadata` action at commit.
    /// - changed: the writer targets `target_schema` directly (`RecordBatchWriter::try_new`, not
    ///   `for_table`), and `commit_overwrite` will fold a `Metadata` action replacing the Delta
    ///   schema into the same atomic commit as the Remove/Add actions.
    ///
    /// The caller streams chunks through `stage_overwrite_chunk` (parquet files written, nothing
    /// committed) and finishes with `commit_overwrite` (one atomic commit swaps the whole
    /// snapshot). The table must already exist (callers run `ensure_table` first, as full-refresh
    /// does today). Replaces any stale session for this table (e.g. from a prior aborted run).
    pub async fn begin_overwrite(&self, table_name: &str, target_schema: SchemaRef) -> Result<()> {
        let table = self.open_table(table_name).await?;

        let target_struct = arrow_schema_to_delta(&target_schema)?;
        let current_struct = table.snapshot()?.schema().as_ref().clone();
        let schema_changed = target_struct != current_struct;

        let (writer, schema_change) = if schema_changed {
            let url = self.table_url(table_name)?;
            let w = RecordBatchWriter::try_new(
                url.as_str(),
                target_schema.clone(),
                None,
                Some(self.storage_options.clone()),
            )?;
            info!(
                table = table_name,
                "begin_overwrite: source schema differs from the stored Delta schema — this \
                 overwrite will REPLACE the schema (FA3)"
            );
            (w, Some(target_struct))
        } else {
            (RecordBatchWriter::for_table(&table)?, None)
        };

        self.overwrite_sessions.lock().await.insert(
            table_name.to_string(),
            OverwriteSession {
                writer,
                table,
                adds: Vec::new(),
                schema_change,
            },
        );

        info!(table = table_name, "begin_overwrite: staged-overwrite session started");
        Ok(())
    }

    /// O2-r CP1: write one chunk's rows to parquet FILES in the table's storage, WITHOUT
    /// committing — `flush()` returns the resulting `Add` actions, which are accumulated on the
    /// session for the final `commit_overwrite`. Requires a session started by `begin_overwrite`.
    pub async fn stage_overwrite_chunk(
        &self,
        table_name: &str,
        batches: Vec<RecordBatch>,
    ) -> Result<()> {
        let mut sessions = self.overwrite_sessions.lock().await;
        let session = sessions.get_mut(table_name).ok_or_else(|| {
            anyhow::anyhow!(
                "stage_overwrite_chunk: no overwrite session in progress for table `{table_name}` (call begin_overwrite first)"
            )
        })?;

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        // O2-r: coerce each batch to RecordBatchWriter's exact target schema (see
        // `coerce_batch_to_schema`) — the strict writer rejects a schema that the old tolerant
        // `table.write()` path would have cast/relabelled.
        let target_schema = session.writer.arrow_schema();
        for b in batches {
            let coerced = coerce_batch_to_schema(&b, &target_schema)?;
            session.writer.write(coerced).await?;
        }
        let adds = session.writer.flush().await?;
        session.adds.extend(adds);

        info!(
            table = table_name,
            rows = total_rows,
            "stage_overwrite_chunk: chunk written to parquet (not committed)"
        );
        Ok(())
    }

    /// O2-r CP1: finish the staged-overwrite session for `table_name` with ONE atomic commit —
    /// `Remove` actions for every file in the table's CURRENT snapshot, plus `Add` actions for
    /// every chunk staged since `begin_overwrite`. The Delta transaction log commit IS the
    /// atomic swap: the prior snapshot stays fully readable until this commit lands; an
    /// interruption before it leaves the staged parquet orphaned (vacuum-able) with the live
    /// table untouched. Evicts the P1 table_cache entry afterward — the overwrite advanced the
    /// version outside that cached handle.
    ///
    /// PS-H-B: `insert_id` is the two-stream insert-cursor watermark to stamp ALONGSIDE `hwm`
    /// (the two-stream update-cursor watermark) when a table reconcile's one-shot full snapshot
    /// re-seeds BOTH streams in this same atomic commit. When both are `Some`, the commit is
    /// stamped via `build_two_stream_commit_properties` (all three of `hwm_insert_id` /
    /// `hwm_updated_at` / `hwm_last_id`); otherwise this is byte-identical to before PS-H-B —
    /// `build_commit_properties(hwm)` alone (no `hwm_insert_id` key at all).
    pub async fn commit_overwrite(
        &self,
        table_name: &str,
        hwm: Option<&Hwm>,
        insert_id: Option<i64>,
    ) -> Result<()> {
        let session = {
            let mut sessions = self.overwrite_sessions.lock().await;
            sessions.remove(table_name).ok_or_else(|| {
                anyhow::anyhow!(
                    "commit_overwrite: no overwrite session in progress for table `{table_name}` (call begin_overwrite first)"
                )
            })?
        };
        let OverwriteSession { table, adds, schema_change, .. } = session;

        // Current file list, synchronously from the already-loaded (eager) snapshot — no new
        // I/O and no async stream needed. Every current file becomes a Remove; on a brand-new
        // table with zero files this is simply empty.
        let snapshot = table.snapshot()?;
        let mut actions: Vec<Action> = snapshot
            .log_data()
            .iter()
            .map(|file_view| Action::Remove(file_view.remove_action(true)))
            .collect();
        let removed_files = actions.len();

        // FA3: when `begin_overwrite` found the source schema differs from the table's stored
        // schema, fold a `Metadata` action (carrying the new schema) into this SAME atomic
        // commit, right alongside the Remove/Add actions below — this is the same bundling
        // delta-rs's own `table.write()` uses for `SchemaMode::Overwrite` (Metadata + Add/Remove
        // in one commit), so the schema replacement and the data rewrite land atomically together.
        let schema_replaced = schema_change.is_some();
        if let Some(new_struct) = &schema_change {
            let current_meta = snapshot.metadata().clone();
            let new_meta = current_meta.with_schema(new_struct).map_err(|e| {
                anyhow::anyhow!(
                    "commit_overwrite: failed to build Metadata with the new schema: {e}"
                )
            })?;
            actions.push(Action::Metadata(new_meta));
        }

        let added_files = adds.len();
        actions.extend(adds.into_iter().map(Action::Add));

        // PS-H-B: a reconcile commit carries BOTH watermarks (insert_id + hwm) in one atomic
        // commit — build_two_stream_commit_properties stamps all three keys a two-stream resume
        // needs. Any other combination (including a lone insert_id with no hwm, which reconcile
        // never produces) falls back to the pre-PS-H-B single-stream stamping, unchanged.
        let commit_properties = match (insert_id, hwm) {
            (Some(id), Some(h)) => build_two_stream_commit_properties(Some(id), Some(h)),
            _ => build_commit_properties(hwm),
        };
        let log_store = table.log_store();
        let finalized = CommitBuilder::from(commit_properties)
            .with_actions(actions)
            .build(
                Some(snapshot),
                log_store,
                DeltaOperation::Write {
                    mode: SaveMode::Overwrite,
                    partition_by: None,
                    predicate: None,
                },
            )
            .await?;

        // P1's per-table handle cache is now stale (the overwrite advanced the version behind
        // its back) — evict so the next write op re-loads instead of reusing it.
        self.table_cache.lock().await.remove(table_name);

        info!(
            table = table_name,
            version = finalized.version(),
            files_removed = removed_files,
            files_added = added_files,
            schema_replaced,
            hwm_insert_id = ?insert_id,
            hwm_updated_at = ?hwm.as_ref().map(|h| h.updated_at.as_str()),
            hwm_last_id = ?hwm.as_ref().map(|h| h.last_id),
            "commit_overwrite: atomic overwrite committed (single commit swaps entire snapshot)"
        );
        Ok(())
    }

    /// FA11: release a staged-overwrite session for `table_name` WITHOUT committing — for a
    /// full refresh that fails or is shut down after `begin_overwrite` has already started a
    /// session (so it isn't left resident in `overwrite_sessions` until process exit). Idempotent:
    /// a no-op when no session is in progress for this table (nothing begun, already committed, or
    /// already aborted). Does NOT touch any parquet already flushed by `stage_overwrite_chunk` —
    /// those files are simply never referenced by an `Add` action, so an abort after staging
    /// leaves them orphaned in object storage, reclaimable only by running VACUUM.
    pub async fn abort_overwrite(&self, table_name: &str) {
        let removed = self.overwrite_sessions.lock().await.remove(table_name);
        if removed.is_some() {
            info!(
                table = table_name,
                "abort_overwrite: staged-overwrite session released without committing \
                 (any staged parquet is orphaned pending VACUUM)"
            );
        }
    }

    pub async fn read_hwm(&self, table_name: &str) -> Result<Option<Hwm>> {
        let table = match self.open_table(table_name).await {
            Ok(t) => t,
            Err(e) => {
                if is_missing_table_error(&e) {
                    info!(table = table_name, "Delta table does not exist, no HWM");
                    return Ok(None);
                }
                return Err(e).context(format!(
                    "read HWM: could not open Delta table `{table_name}` (not treating a transient error as no-HWM, to avoid a from-scratch re-extract)"
                ));
            }
        };

        let history = table.history(Some(HWM_LOOKBACK_COMMITS)).await?.collect::<Vec<_>>();
        if history.is_empty() {
            warn!(table = table_name, "Delta table has no commits, no HWM");
            return Ok(None);
        }

        // L7: scan backward from the newest commit — a Delta OPTIMIZE/VACUUM/checkpoint
        // housekeeping commit between syncs carries no `hwm_*` keys and must not shadow an
        // older commit's real watermark.
        let (commit_info, skipped) =
            match find_commit_with_keys(&history, &["hwm_updated_at", "hwm_last_id"]) {
                Some(found) => found,
                None => {
                    warn!(
                        table = table_name,
                        lookback = history.len(),
                        "Delta table exists but no HWM in commitInfo within the last {} commit(s), starting from beginning",
                        history.len()
                    );
                    return Ok(None);
                }
            };

        let updated_at = commit_info.info.get("hwm_updated_at");
        let last_id = commit_info.info.get("hwm_last_id");

        match (updated_at, last_id) {
            (Some(serde_json::Value::String(ua)), Some(serde_json::Value::String(id))) => {
                let id: i64 = id.parse().context("invalid hwm_last_id in commitInfo")?;
                if skipped > 0 {
                    info!(
                        table = table_name,
                        hwm_updated_at = %ua,
                        hwm_last_id = id,
                        skipped_commits = skipped,
                        "read HWM recovered from an older commit — {skipped} newer commit(s) \
                         (e.g. OPTIMIZE/VACUUM/checkpoint) carried no HWM and were skipped"
                    );
                } else {
                    info!(
                        table = table_name,
                        hwm_updated_at = %ua,
                        hwm_last_id = id,
                        "read HWM from Delta log"
                    );
                }
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




#[cfg(test)]
mod tests {
    use super::*;
    
    
    use deltalake::arrow::datatypes::Field;
    use std::sync::Arc;


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

        assert!(!writer.storage_options.contains_key("AWS_ENDPOINT_URL"));
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
    async fn append_batch_multiple_calls_preserve_all_rows() {
        // P1: proves the per-table DeltaTable handle cache (take-on-acquire, store-back
        // post-commit) is coherent across a sequence of appends — three separate calls to
        // `append_batch`, each reusing the cached handle from the previous call, must neither
        // drop nor duplicate rows.
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

        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1i64, 2i64])),
                Arc::new(StringArray::from(vec!["a", "b"])),
            ],
        )
        .unwrap();
        let hwm1 = Hwm {
            updated_at: "2026-03-28 09:00:00".to_string(),
            last_id: 2,
        };
        writer
            .append_batch("test_table", vec![batch1], Some(&hwm1))
            .await
            .unwrap();

        let batch2 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![3i64, 4i64])),
                Arc::new(StringArray::from(vec!["c", "d"])),
            ],
        )
        .unwrap();
        let hwm2 = Hwm {
            updated_at: "2026-03-28 10:00:00".to_string(),
            last_id: 4,
        };
        writer
            .append_batch("test_table", vec![batch2], Some(&hwm2))
            .await
            .unwrap();

        let batch3 = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![5i64, 6i64])),
                Arc::new(StringArray::from(vec!["e", "f"])),
            ],
        )
        .unwrap();
        let hwm3 = Hwm {
            updated_at: "2026-03-28 11:00:00".to_string(),
            last_id: 6,
        };
        writer
            .append_batch("test_table", vec![batch3], Some(&hwm3))
            .await
            .unwrap();

        // Fresh-load (a brand new handle, bypassing the writer's cache) and verify all 6 rows
        // are present exactly once each.
        let table = writer.open_table("test_table").await.unwrap();
        let ctx = deltalake::datafusion::prelude::SessionContext::new();
        let provider = table.table_provider().await.unwrap();
        ctx.register_table("test_table", provider).unwrap();
        let result = ctx
            .sql("SELECT COUNT(*) AS c, COUNT(DISTINCT id) AS d FROM test_table")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let b = &result[0];
        let count = b
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        let distinct = b
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(count, 6, "all 3 cached appends' rows must be present");
        assert_eq!(distinct, 6, "no row should be duplicated across cached appends");
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

    /// L7: a Delta OPTIMIZE/VACUUM/checkpoint-style housekeeping commit (simulated here with a
    /// plain `hwm=None` append, which stamps NO `hwm_*` keys — the same shape as a housekeeping
    /// commit) landing AFTER a real HWM commit must not shadow the real watermark. `read_hwm` must
    /// scan backward and recover it from the older commit instead of reporting `None`.
    #[tokio::test]
    async fn read_hwm_recovers_past_a_shadowing_non_hwm_commit() {
        let temp = tempfile::tempdir().unwrap();
        let writer = DeltaWriter::new_local(temp.path().to_str().unwrap());
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
        ]));

        writer.ensure_table("test_table", schema.clone()).await.unwrap();

        let hwm = Hwm {
            updated_at: "2026-03-28 10:00:00".to_string(),
            last_id: 5,
        };
        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1i64]))],
        )
        .unwrap();
        writer
            .append_batch("test_table", vec![batch1], Some(&hwm))
            .await
            .unwrap();

        // Simulate one or more housekeeping commits (e.g. OPTIMIZE/VACUUM) landing after the
        // real HWM commit: they carry no `hwm_*` keys at all.
        let batch2 = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![2i64]))],
        )
        .unwrap();
        writer
            .append_batch("test_table", vec![batch2], None)
            .await
            .unwrap();
        let batch3 = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![3i64]))],
        )
        .unwrap();
        writer
            .append_batch("test_table", vec![batch3], None)
            .await
            .unwrap();

        let read_back = writer
            .read_hwm("test_table")
            .await
            .unwrap()
            .expect("HWM must be recovered from the older commit, not shadowed by the newer non-HWM commits");
        assert_eq!(read_back.updated_at, "2026-03-28 10:00:00");
        assert_eq!(read_back.last_id, 5);
    }

    /// L7: still `None` (today's safe fallback) when NO commit within the lookback window
    /// carries the HWM keys — proven here by a table whose only commit has none.
    #[tokio::test]
    async fn read_hwm_none_when_no_commit_in_window_carries_keys() {
        let temp = tempfile::tempdir().unwrap();
        let writer = DeltaWriter::new_local(temp.path().to_str().unwrap());
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
        ]));
        writer.ensure_table("test_table", schema.clone()).await.unwrap();
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![1i64]))],
        )
        .unwrap();
        writer.append_batch("test_table", vec![batch], None).await.unwrap();

        let hwm = writer.read_hwm("test_table").await.unwrap();
        assert!(hwm.is_none());
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

    /// L7: `find_commit_with_keys` is the pure scan `read_hwm`/`read_insert_hwm` use — unit-test
    /// it directly against a synthetic history so the "skip N shadowing commits, then find the
    /// one that carries the key(s)" and "None found in the whole window" cases are both covered
    /// without needing to fabricate a real 64-commit Delta history.
    #[test]
    fn find_commit_with_keys_skips_shadowing_commits() {
        fn commit_info_with(pairs: &[(&str, &str)]) -> CommitInfo {
            let mut info = HashMap::new();
            for (k, v) in pairs {
                info.insert((*k).to_string(), serde_json::Value::String((*v).to_string()));
            }
            CommitInfo { info, ..Default::default() }
        }

        // Most-recent-first, mirroring `DeltaTable::history`'s order: two shadowing
        // (no hwm_* keys) commits, then the real HWM commit further back.
        let history = vec![
            commit_info_with(&[]), // newest: OPTIMIZE-like, no keys
            commit_info_with(&[("some_other_key", "x")]), // VACUUM-like, no keys
            commit_info_with(&[("hwm_updated_at", "2026-01-01 00:00:00"), ("hwm_last_id", "7")]),
        ];

        let (found, skipped) =
            find_commit_with_keys(&history, &["hwm_updated_at", "hwm_last_id"])
                .expect("must find the older commit that carries both keys");
        assert_eq!(skipped, 2, "must report exactly 2 shadowing commits skipped");
        assert_eq!(
            found.info.get("hwm_last_id"),
            Some(&serde_json::Value::String("7".to_string()))
        );
    }

    #[test]
    fn find_commit_with_keys_none_when_absent_from_every_commit() {
        fn commit_info_with(pairs: &[(&str, &str)]) -> CommitInfo {
            let mut info = HashMap::new();
            for (k, v) in pairs {
                info.insert((*k).to_string(), serde_json::Value::String((*v).to_string()));
            }
            CommitInfo { info, ..Default::default() }
        }

        let history = vec![commit_info_with(&[]), commit_info_with(&[("unrelated", "1")])];
        assert!(find_commit_with_keys(&history, &["hwm_updated_at", "hwm_last_id"]).is_none());
    }

    /// FA11: `abort_overwrite` removes an in-progress session so a subsequent `begin_overwrite`
    /// (a retry of the same table) doesn't inherit stale state, and completes normally.
    #[tokio::test]
    async fn abort_overwrite_removes_session_and_allows_clean_retry() {
        let temp = tempfile::tempdir().unwrap();
        let writer = DeltaWriter::new_local(temp.path().to_str().unwrap());
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
        ]));
        writer.ensure_table("t", schema.clone()).await.unwrap();

        writer.begin_overwrite("t", schema.clone()).await.unwrap();
        let chunk = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![99i64]))],
        )
        .unwrap();
        writer.stage_overwrite_chunk("t", vec![chunk]).await.unwrap();

        // Abort instead of committing — the session (and its staged-but-uncommitted chunk)
        // must be gone.
        writer.abort_overwrite("t").await;

        // A fresh begin/stage/commit for the same table must work cleanly — proves no stale
        // session lingered to conflict with (or silently extend) this new one.
        writer.begin_overwrite("t", schema.clone()).await.unwrap();
        let chunk2 = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![1i64]))],
        )
        .unwrap();
        writer.stage_overwrite_chunk("t", vec![chunk2]).await.unwrap();
        writer.commit_overwrite("t", None, None).await.unwrap();

        let table = writer.open_table("t").await.unwrap();
        let ctx = deltalake::datafusion::prelude::SessionContext::new();
        let provider = table.table_provider().await.unwrap();
        ctx.register_table("t", provider).unwrap();
        let batches = ctx
            .sql("SELECT id FROM t")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let ids: Vec<i64> = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .to_vec();
        assert_eq!(
            ids,
            vec![1],
            "only the post-abort session's chunk must be present — the aborted one's chunk (99) must not leak in"
        );
    }

    /// FA11: `abort_overwrite` on a table with no in-progress session is a harmless no-op.
    #[tokio::test]
    async fn abort_overwrite_is_noop_when_no_session() {
        let temp = tempfile::tempdir().unwrap();
        let writer = DeltaWriter::new_local(temp.path().to_str().unwrap());
        writer.abort_overwrite("never_started").await;
    }

    #[tokio::test]
    async fn read_hwm_s3_error_propagates() {
        let writer = DeltaWriter::new(
            "nonexistent-bucket",
            "prefix",
            Some("http://localhost:1"),
            "us-east-1",
            "fake",
            "fake",
        );

        let result = writer.read_hwm("nonexistent").await;
        assert!(result.is_err());
    }

    /// O2-r CP1 de-risker: proves the staged-overwrite session is a single atomic commit, not
    /// per-chunk commits — begin/stage/stage/commit must advance the table version by EXACTLY
    /// 1 and leave EXACTLY the newly-staged rows (the prior snapshot's rows are gone).
    #[tokio::test]
    async fn atomic_overwrite_replaces_snapshot_in_one_commit() {
        let temp = tempfile::tempdir().unwrap();
        let writer = DeltaWriter::new_local(temp.path().to_str().unwrap());
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("val", DataType::Utf8, false),
        ]));

        writer.ensure_table("t", schema.clone()).await.unwrap();

        // Seed the initial snapshot: ids 1,2,3.
        let seed = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1i64, 2i64, 3i64])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap();
        writer.overwrite_table("t", vec![seed], None).await.unwrap();

        let version_before = writer.open_table("t").await.unwrap().version().unwrap();

        // Stage two chunks (ids 10,11 then 12,13) without ever committing per-chunk.
        writer.begin_overwrite("t", schema.clone()).await.unwrap();
        let chunk1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![10i64, 11i64])),
                Arc::new(StringArray::from(vec!["x", "y"])),
            ],
        )
        .unwrap();
        writer.stage_overwrite_chunk("t", vec![chunk1]).await.unwrap();

        let chunk2 = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![12i64, 13i64])),
                Arc::new(StringArray::from(vec!["z", "w"])),
            ],
        )
        .unwrap();
        writer.stage_overwrite_chunk("t", vec![chunk2]).await.unwrap();

        writer.commit_overwrite("t", None, None).await.unwrap();

        // Fresh-load (bypassing any cache) and verify the swap.
        let table = writer.open_table("t").await.unwrap();
        let version_after = table.version().unwrap();
        assert_eq!(
            version_after,
            version_before + 1,
            "commit_overwrite must be a SINGLE commit, not one per staged chunk"
        );

        let ctx = deltalake::datafusion::prelude::SessionContext::new();
        let provider = table.table_provider().await.unwrap();
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
        assert_eq!(batch.num_rows(), 4, "old rows [1,2,3] must be gone; only the 4 staged rows remain");
        let ids: Vec<i64> = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .to_vec();
        assert_eq!(ids, vec![10, 11, 12, 13]);
    }

    /// FA3: a full refresh must ADOPT the current source schema, not stay frozen at
    /// table-creation time — a NEW source column must appear in the Delta schema (not be
    /// silently dropped) after `begin_overwrite` is given a WIDER target schema, with its
    /// values present via a single atomic commit that folds in a `Metadata` action.
    #[tokio::test]
    async fn atomic_overwrite_adopts_new_column_schema() {
        let temp = tempfile::tempdir().unwrap();
        let writer = DeltaWriter::new_local(temp.path().to_str().unwrap());
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("val", DataType::Utf8, true),
        ]));
        writer.ensure_table("t", schema.clone()).await.unwrap();

        let seed = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1i64, 2i64])),
                Arc::new(StringArray::from(vec!["a", "b"])),
            ],
        )
        .unwrap();
        writer.overwrite_table("t", vec![seed], None).await.unwrap();

        // The current SOURCE schema now has a new `extra` column the Delta table (created
        // above) doesn't have yet — the exact FA3 scenario.
        let new_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("val", DataType::Utf8, true),
            Field::new("extra", DataType::Int64, true),
        ]));

        writer.begin_overwrite("t", new_schema.clone()).await.unwrap();
        let chunk = RecordBatch::try_new(
            new_schema,
            vec![
                Arc::new(Int64Array::from(vec![10i64, 11i64])),
                Arc::new(StringArray::from(vec!["x", "y"])),
                Arc::new(Int64Array::from(vec![100i64, 101i64])),
            ],
        )
        .unwrap();
        writer.stage_overwrite_chunk("t", vec![chunk]).await.unwrap();
        writer.commit_overwrite("t", None, None).await.unwrap();

        // Fresh-load (bypassing any cache) and verify the Delta schema now HAS `extra`.
        let table = writer.open_table("t").await.unwrap();
        let field_names: Vec<String> = table
            .snapshot()
            .unwrap()
            .schema()
            .fields()
            .map(|f| f.name().clone())
            .collect();
        assert!(
            field_names.contains(&"extra".to_string()),
            "the Delta schema must adopt the new source column `extra`, got: {field_names:?}"
        );

        let ctx = deltalake::datafusion::prelude::SessionContext::new();
        let provider = table.table_provider().await.unwrap();
        ctx.register_table("t", provider).unwrap();
        let batches = ctx
            .sql("SELECT id, extra FROM t ORDER BY id")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 2, "old rows must be replaced by the 2 newly staged rows");
        let extras: Vec<i64> = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .to_vec();
        assert_eq!(extras, vec![100, 101], "the new column's values must round-trip");
    }

    /// FA3: the common (unchanged-schema) case must remain behaviorally identical to before —
    /// `begin_overwrite` given the SAME schema the table already has takes the `for_table`
    /// path (no `Metadata` action folded into the commit), and the overwrite still works.
    #[tokio::test]
    async fn atomic_overwrite_unchanged_schema_no_metadata() {
        let temp = tempfile::tempdir().unwrap();
        let writer = DeltaWriter::new_local(temp.path().to_str().unwrap());
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("val", DataType::Utf8, true),
        ]));
        writer.ensure_table("t", schema.clone()).await.unwrap();

        let seed = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1i64, 2i64, 3i64])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap();
        writer.overwrite_table("t", vec![seed], None).await.unwrap();
        let version_before = writer.open_table("t").await.unwrap().version().unwrap();

        // begin_overwrite with the SAME schema the table already has — must take the
        // for_table/no-Metadata path, exactly like before FA3.
        writer.begin_overwrite("t", schema.clone()).await.unwrap();
        let chunk = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![10i64])),
                Arc::new(StringArray::from(vec!["z"])),
            ],
        )
        .unwrap();
        writer.stage_overwrite_chunk("t", vec![chunk]).await.unwrap();
        writer.commit_overwrite("t", None, None).await.unwrap();

        let table = writer.open_table("t").await.unwrap();
        assert_eq!(
            table.version().unwrap(),
            version_before + 1,
            "an unchanged-schema overwrite must still be a single atomic commit"
        );
        let field_names: Vec<String> = table
            .snapshot()
            .unwrap()
            .schema()
            .fields()
            .map(|f| f.name().clone())
            .collect();
        assert_eq!(
            field_names,
            vec!["id".to_string(), "val".to_string()],
            "the schema must be unchanged when the source schema didn't change"
        );

        let ctx = deltalake::datafusion::prelude::SessionContext::new();
        let provider = table.table_provider().await.unwrap();
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
        assert_eq!(batch.num_rows(), 1, "old rows [1,2,3] must be gone; only the new row remains");
    }

    #[tokio::test]
    async fn atomic_overwrite_coerces_non_nullable_source_batch() {
        // O2-r regression: a NOT NULL source column arrives as a NON-nullable Arrow field, but the
        // Delta table schema is all-nullable (column_info_to_v57_schema marks every column
        // nullable). RecordBatchWriter validates nullability strictly, so without coercion staging
        // rejects the batch ("RecordBatch schema does not match"). stage_overwrite_chunk coerces to
        // the writer's target schema, so this succeeds (a safe non-null → nullable widening).
        let temp = tempfile::tempdir().unwrap();
        let writer = DeltaWriter::new_local(temp.path().to_str().unwrap());
        // Table schema: all NULLABLE (mirrors column_info_to_v57_schema).
        let table_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("val", DataType::Utf8, true),
        ]));
        writer.ensure_table("t", table_schema.clone()).await.unwrap();

        writer.begin_overwrite("t", table_schema).await.unwrap();
        // Batch built with a NON-nullable schema, as a NOT NULL source column would produce.
        let non_nullable_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("val", DataType::Utf8, false),
        ]));
        let chunk = RecordBatch::try_new(
            non_nullable_schema,
            vec![
                Arc::new(Int64Array::from(vec![1i64, 2i64])),
                Arc::new(StringArray::from(vec!["a", "b"])),
            ],
        )
        .unwrap();
        writer
            .stage_overwrite_chunk("t", vec![chunk])
            .await
            .expect("staging a non-nullable batch into the nullable table must be coerced, not rejected");
        writer.commit_overwrite("t", None, None).await.unwrap();

        let table = writer.open_table("t").await.unwrap();
        let ctx = deltalake::datafusion::prelude::SessionContext::new();
        let provider = table.table_provider().await.unwrap();
        ctx.register_table("t", provider).unwrap();
        let batches = ctx
            .sql("SELECT id FROM t ORDER BY id")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let ids: Vec<i64> = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .to_vec();
        assert_eq!(ids, vec![1, 2], "the coerced non-nullable batch must persist");
    }

    #[test]
    fn coerce_batch_to_schema_errors_on_lossy_narrowing_not_silent_null() {
        // FA1: a widening cast (Int16 source -> Int32 Delta, the N5 case) succeeds; a NARROWING
        // cast whose value overflows the target (Int64 value > i32::MAX -> Int32 Delta) must ERROR
        // loudly, NOT silently substitute NULL (arrow's safe:true default). Guards the full-refresh
        // silent-corruption class.
        use deltalake::arrow::array::{Int16Array, Int64Array};
        use deltalake::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};

        let target = Arc::new(ArrowSchema::new(vec![Field::new("v", DataType::Int32, true)]));

        // Widening Int16 -> Int32: OK.
        let widen = RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![Field::new("v", DataType::Int16, true)])),
            vec![Arc::new(Int16Array::from(vec![7i16, 42]))],
        )
        .unwrap();
        let out = coerce_batch_to_schema(&widen, &target).expect("widening Int16->Int32 must succeed");
        assert_eq!(out.num_rows(), 2);

        // Narrowing Int64 -> Int32 with an out-of-range value: must ERROR (not NULL).
        let overflow = RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![Field::new("v", DataType::Int64, true)])),
            vec![Arc::new(Int64Array::from(vec![(i32::MAX as i64) + 1]))],
        )
        .unwrap();
        let err = coerce_batch_to_schema(&overflow, &target)
            .expect_err("a value exceeding the Delta Int32 range must fail, not become NULL");
        assert!(
            err.to_string().contains("does not fit the Delta column type"),
            "unexpected error: {err}"
        );
    }

    /// O2-r CP1 de-risker: an aborted staged-overwrite (staged but never committed) must leave
    /// the live table completely untouched — the staged parquet files are orphaned, not
    /// referenced by any commit, and readers still see the prior snapshot.
    #[tokio::test]
    async fn atomic_overwrite_abort_leaves_table_unchanged() {
        let temp = tempfile::tempdir().unwrap();
        let writer = DeltaWriter::new_local(temp.path().to_str().unwrap());
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("val", DataType::Utf8, false),
        ]));

        writer.ensure_table("t", schema.clone()).await.unwrap();

        let seed = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1i64, 2i64, 3i64])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap();
        writer.overwrite_table("t", vec![seed], None).await.unwrap();
        let version_before = writer.open_table("t").await.unwrap().version().unwrap();

        // Begin + stage, but NEVER commit — simulates an interruption mid-rewrite.
        writer.begin_overwrite("t", schema.clone()).await.unwrap();
        let chunk = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![99i64])),
                Arc::new(StringArray::from(vec!["orphan"])),
            ],
        )
        .unwrap();
        writer.stage_overwrite_chunk("t", vec![chunk]).await.unwrap();
        // ... abort here: no commit_overwrite call.

        // Fresh-load (bypassing any cache) and verify the prior snapshot is intact.
        let table = writer.open_table("t").await.unwrap();
        assert_eq!(
            table.version().unwrap(),
            version_before,
            "an uncommitted staged-overwrite must not advance the table version"
        );

        let ctx = deltalake::datafusion::prelude::SessionContext::new();
        let provider = table.table_provider().await.unwrap();
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
        assert_eq!(batch.num_rows(), 3, "prior snapshot [1,2,3] must be untouched by the aborted overwrite");
        let ids: Vec<i64> = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .to_vec();
        assert_eq!(ids, vec![1, 2, 3]);
    }

    /// PS-H-B: a table reconcile's final commit passes BOTH `hwm` (the two-stream update
    /// watermark) and `insert_id` (the two-stream insert watermark) to `commit_overwrite` in
    /// the SAME atomic commit — this must stamp all three `hwm_insert_id`/`hwm_updated_at`/
    /// `hwm_last_id` keys so a two-stream resume (`read_insert_hwm` + `read_hwm`) finds every
    /// key it needs, with no separate commit and no manual seed.
    #[tokio::test]
    async fn commit_overwrite_with_insert_id_stamps_two_stream_hwm() {
        let temp = tempfile::tempdir().unwrap();
        let writer = DeltaWriter::new_local(temp.path().to_str().unwrap());
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("val", DataType::Utf8, false),
        ]));
        writer.ensure_table("t", schema.clone()).await.unwrap();

        writer.begin_overwrite("t", schema.clone()).await.unwrap();
        let chunk = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1i64, 2i64, 3i64])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap();
        writer.stage_overwrite_chunk("t", vec![chunk]).await.unwrap();

        let hwm = Hwm { updated_at: "2026-07-01 00:00:00".to_string(), last_id: i64::MAX };
        writer
            .commit_overwrite("t", Some(&hwm), Some(3))
            .await
            .expect("reconcile commit must succeed");

        let insert_hwm = writer.read_insert_hwm("t").await.unwrap();
        assert_eq!(insert_hwm, Some(3), "hwm_insert_id must round-trip as the reconcile's max PK id");

        let update_hwm = writer.read_hwm("t").await.unwrap().expect("hwm_updated_at/hwm_last_id must be stamped");
        assert_eq!(update_hwm.updated_at, "2026-07-01 00:00:00");
        assert_eq!(update_hwm.last_id, i64::MAX, "reconcile's update watermark uses the D3 tie-break sentinel");
    }

    /// PS-H-B regression: the non-reconcile path (`insert_id: None`) must remain byte-identical
    /// to before — no `hwm_insert_id` key at all, even when a plain `hwm` is also stamped (the
    /// normal single-stream incremental overwrite case, not exercised by full_refresh today but
    /// guarded here since `commit_overwrite` is a shared, general-purpose method).
    #[tokio::test]
    async fn commit_overwrite_without_insert_id_stamps_no_insert_hwm_key() {
        let temp = tempfile::tempdir().unwrap();
        let writer = DeltaWriter::new_local(temp.path().to_str().unwrap());
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("val", DataType::Utf8, false),
        ]));
        writer.ensure_table("t", schema.clone()).await.unwrap();

        writer.begin_overwrite("t", schema.clone()).await.unwrap();
        let chunk = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1i64])),
                Arc::new(StringArray::from(vec!["a"])),
            ],
        )
        .unwrap();
        writer.stage_overwrite_chunk("t", vec![chunk]).await.unwrap();

        let hwm = Hwm { updated_at: "2026-07-01 00:00:00".to_string(), last_id: 1 };
        writer.commit_overwrite("t", Some(&hwm), None).await.unwrap();

        let insert_hwm = writer.read_insert_hwm("t").await.unwrap();
        assert_eq!(insert_hwm, None, "no insert_id passed means no hwm_insert_id key, unchanged from before PS-H-B");
        let update_hwm = writer.read_hwm("t").await.unwrap().expect("hwm_updated_at/hwm_last_id must still be stamped");
        assert_eq!(update_hwm.last_id, 1);
    }
}
