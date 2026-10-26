//! Archive mode (docs/design/archive-mode.md): one append-only commit per `(table, unit)`, with
//! every row stamped with its unit in `_archive_unit`.
//!
//! An archive table's rows are later purged from the source, so its Delta copy may become the only
//! copy. This writer therefore only ever adds files: a commit carries `Add` actions and never a
//! `Remove`. The unit is recorded in the data, not in the Delta log — delta-rs checkpoints every
//! 100 commits and, by default, deletes commit files older than 30 days afterwards, and
//! checkpoints do not keep `commitInfo`. `_archive_unit` is what `archive_unit_present` reads to
//! make a re-run skip a unit already archived.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use deltalake::DeltaTable;
use deltalake::arrow::array::Int64Array;
use deltalake::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use deltalake::arrow::record_batch::RecordBatch;
use deltalake::kernel::transaction::{CommitBuilder, CommitProperties};
use deltalake::kernel::{Action, Add, Transaction};
use deltalake::parquet::basic::{Compression, ZstdLevel};
use deltalake::parquet::file::properties::WriterProperties;
use deltalake::protocol::{DeltaOperation, SaveMode};
use deltalake::writer::{DeltaWriter as _, RecordBatchWriter};
use tracing::info;

use super::{DeltaWriter, coerce_batch_to_schema, is_missing_table_error};

/// The column every archived row carries: the id of the unit (e.g. the campaign) it belongs to.
pub const ARCHIVE_UNIT_COLUMN: &str = "_archive_unit";

/// The Delta transaction-marker (`txn`) application id of a unit's archive commits. Delta's
/// conflict check rejects a concurrent commit carrying the same id, so overlapping runs cannot
/// store a unit twice; markers are kept in checkpoints, unlike `commitInfo`.
pub(super) fn unit_app_id(unit: i64) -> String {
    format!("parket-archive:{unit}")
}

/// One in-flight `(table, unit)` append: files are written as chunks arrive and committed once.
pub(super) struct ArchiveSession {
    writer: RecordBatchWriter,
    table: DeltaTable,
    unit: i64,
    adds: Vec<Add>,
    rows: u64,
}

/// An archive is written once and read rarely, the trade ZSTD is made for: on campaign email data
/// it measured about 40 % smaller than delta-rs's Snappy default. Synced tables keep the default.
fn archive_writer_properties() -> WriterProperties {
    WriterProperties::builder()
        .set_created_by(format!("parket {}", env!("CARGO_PKG_VERSION")))
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .build()
}

/// The archive table's schema: the source columns plus a non-null `_archive_unit`. Refuses a
/// source that already has a column of that name, which the stamp would otherwise shadow.
pub fn with_archive_unit_column(source: &Schema) -> Result<SchemaRef> {
    if source.field_with_name(ARCHIVE_UNIT_COLUMN).is_ok() {
        bail!(
            "the source already has a `{ARCHIVE_UNIT_COLUMN}` column; archive mode reserves that \
             name for the unit each row belongs to"
        );
    }
    let mut fields: Vec<Field> = source.fields().iter().map(|f| f.as_ref().clone()).collect();
    fields.push(Field::new(ARCHIVE_UNIT_COLUMN, DataType::Int64, false));
    Ok(Arc::new(Schema::new_with_metadata(fields, source.metadata().clone())))
}

/// Append `_archive_unit = unit` to a source batch. The writer stamps every row itself, so a caller
/// cannot file rows under the wrong unit.
fn stamp_unit(batch: &RecordBatch, unit: i64) -> Result<RecordBatch> {
    let schema = with_archive_unit_column(batch.schema().as_ref())?;
    let mut columns = batch.columns().to_vec();
    columns.push(Arc::new(Int64Array::from(vec![unit; batch.num_rows()])));
    RecordBatch::try_new(schema, columns).context("stamping the archive unit onto a batch")
}

impl DeltaWriter {
    /// Whether `table_name` already holds rows of `unit`. A missing table holds nothing. Each
    /// archive file holds one unit, so the filter is answered from file statistics.
    pub async fn archive_unit_present(&self, table_name: &str, unit: i64) -> Result<bool> {
        let table = match self.open_table(table_name).await {
            Ok(t) => t,
            Err(e) if is_missing_table_error(&e) => return Ok(false),
            Err(e) => return Err(e).context(format!("archive: opening `{table_name}`")),
        };
        let ctx = deltalake::datafusion::prelude::SessionContext::new();
        ctx.register_table("t", table.table_provider().await?)?;
        let batches = ctx
            .sql(&format!("SELECT 1 FROM t WHERE `{ARCHIVE_UNIT_COLUMN}` = {unit} LIMIT 1"))
            .await?
            .collect()
            .await?;
        Ok(batches.iter().any(|b| b.num_rows() > 0))
    }

    /// Start appending one unit's rows to an existing archive table (one with `_archive_unit`).
    pub async fn begin_archive_unit(&self, table_name: &str, unit: i64) -> Result<()> {
        let table = self.open_table(table_name).await?;
        let marker = table.snapshot()?.transaction_version(table.log_store().as_ref(), unit_app_id(unit)).await?;
        if let Some(version) = marker {
            bail!(
                "unit {unit} of `{table_name}` is already archived (transaction marker version {version}); \
                 refusing to append it again"
            );
        }
        let writer = RecordBatchWriter::for_table(&table)?.with_writer_properties(archive_writer_properties());
        if writer.arrow_schema().field_with_name(ARCHIVE_UNIT_COLUMN).is_err() {
            bail!(
                "`{table_name}` has no `{ARCHIVE_UNIT_COLUMN}` column, so it was not created as an \
                 archive table; refusing to append archive rows to it"
            );
        }
        let mut sessions = self.archive_sessions.lock().await;
        if sessions.contains_key(table_name) {
            bail!("an archive session for `{table_name}` is already in progress");
        }
        sessions.insert(
            table_name.to_string(),
            ArchiveSession { writer, table, unit, adds: Vec::new(), rows: 0 },
        );
        Ok(())
    }

    /// Write one chunk of the unit's rows to parquet files, without committing.
    pub async fn stage_archive_chunk(&self, table_name: &str, batches: Vec<RecordBatch>) -> Result<()> {
        let mut sessions = self.archive_sessions.lock().await;
        let session = sessions.get_mut(table_name).with_context(|| {
            format!("stage_archive_chunk: no archive session in progress for `{table_name}`")
        })?;
        let target = session.writer.arrow_schema();
        for batch in batches {
            let stamped = stamp_unit(&batch, session.unit)?;
            session.rows += stamped.num_rows() as u64;
            session.writer.write(coerce_batch_to_schema(&stamped, &target)?).await?;
        }
        let adds = session.writer.flush().await?;
        session.adds.extend(adds);
        Ok(())
    }

    /// Commit the unit's staged files as ONE append-only commit — `Add` actions only, never a
    /// `Remove` — and return the rows it added. A unit with no rows gets no commit. `commitInfo`
    /// carries `archive_unit` and `archive_rows` as an audit trail; `_archive_unit` in the data is
    /// the record `archive_unit_present` relies on.
    pub async fn commit_archive_unit(&self, table_name: &str) -> Result<u64> {
        let session = self.archive_sessions.lock().await.remove(table_name).with_context(|| {
            format!("commit_archive_unit: no archive session in progress for `{table_name}`")
        })?;
        let ArchiveSession { table, unit, adds, rows, .. } = session;
        if adds.is_empty() {
            info!(table = table_name, unit, "archive: unit has no rows in this table; nothing to commit");
            return Ok(0);
        }
        let files = adds.len();
        let actions: Vec<Action> = adds.into_iter().map(Action::Add).collect();
        let metadata = HashMap::from([
            ("archive_unit".to_string(), serde_json::Value::String(unit.to_string())),
            ("archive_rows".to_string(), serde_json::Value::String(rows.to_string())),
        ]);
        let properties = CommitProperties::default()
            .with_metadata(metadata)
            .with_application_transaction(Transaction::new(unit_app_id(unit), 1));
        let finalized = CommitBuilder::from(properties)
            .with_actions(actions)
            .build(
                Some(table.snapshot()?),
                table.log_store(),
                DeltaOperation::Write { mode: SaveMode::Append, partition_by: None, predicate: None },
            )
            .await?;
        // The commit advanced the table behind any cached handle.
        self.table_cache.lock().await.remove(table_name);
        info!(table = table_name, unit, rows, files, version = finalized.version(), "archive: unit committed");
        Ok(rows)
    }

    /// Drop an in-flight archive session without committing. Any staged parquet is left
    /// unreferenced (reclaimable by `--vacuum`); the table is unchanged. Idempotent.
    pub async fn abort_archive_unit(&self, table_name: &str) {
        if self.archive_sessions.lock().await.remove(table_name).is_some() {
            info!(table = table_name, "archive: session aborted; nothing committed");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use deltalake::arrow::array::StringArray;

    fn source_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("subject", DataType::Utf8, true),
        ]))
    }

    fn rows(ids: std::ops::RangeInclusive<i64>) -> RecordBatch {
        let ids: Vec<i64> = ids.collect();
        let subjects: Vec<String> = ids.iter().map(|i| format!("subject {}", i % 3)).collect();
        RecordBatch::try_new(
            source_schema(),
            vec![Arc::new(Int64Array::from(ids)), Arc::new(StringArray::from(subjects))],
        )
        .unwrap()
    }

    async fn archive_table(dir: &std::path::Path) -> DeltaWriter {
        let writer = DeltaWriter::new_local(dir.to_str().unwrap());
        writer
            .ensure_table("messages", with_archive_unit_column(&source_schema()).unwrap())
            .await
            .unwrap();
        writer
    }

    async fn archive(writer: &DeltaWriter, unit: i64, chunks: Vec<RecordBatch>) -> u64 {
        writer.begin_archive_unit("messages", unit).await.unwrap();
        for chunk in chunks {
            writer.stage_archive_chunk("messages", vec![chunk]).await.unwrap();
        }
        writer.commit_archive_unit("messages").await.unwrap()
    }

    async fn query(writer: &DeltaWriter, sql: &str) -> Vec<RecordBatch> {
        let table = writer.open_table("messages").await.unwrap();
        let ctx = deltalake::datafusion::prelude::SessionContext::new();
        ctx.register_table("t", table.table_provider().await.unwrap()).unwrap();
        ctx.sql(sql).await.unwrap().collect().await.unwrap()
    }

    fn i64_cell(batches: &[RecordBatch], col: usize) -> i64 {
        batches[0].column(col).as_any().downcast_ref::<Int64Array>().unwrap().value(0)
    }

    #[tokio::test]
    async fn a_unit_lands_in_one_append_only_commit_with_every_row_stamped() {
        let dir = tempfile::tempdir().unwrap();
        let writer = archive_table(dir.path()).await;
        let before = writer.open_table("messages").await.unwrap().version();

        let added = archive(&writer, 909, vec![rows(1..=600), rows(601..=1000)]).await;
        assert_eq!(added, 1000);

        let table = writer.open_table("messages").await.unwrap();
        assert_eq!(table.version(), before.map(|v| v + 1), "two chunks, ONE commit");
        let history = table.history(Some(1)).await.unwrap().collect::<Vec<_>>();
        let info = &history[0].info;
        assert_eq!(info.get("archive_unit"), Some(&serde_json::json!("909")));
        assert_eq!(info.get("archive_rows"), Some(&serde_json::json!("1000")));
        assert_eq!(history[0].operation.as_deref(), Some("WRITE"));
        assert_eq!(
            history[0].operation_parameters.as_ref().and_then(|p| p.get("mode")),
            Some(&serde_json::json!("Append"))
        );

        let b = query(&writer, "SELECT count(*), count(DISTINCT `_archive_unit`), min(`_archive_unit`) FROM t").await;
        assert_eq!((i64_cell(&b, 0), i64_cell(&b, 1), i64_cell(&b, 2)), (1000, 1, 909));
    }

    #[tokio::test]
    async fn a_second_unit_only_adds_files_and_leaves_the_first_untouched() {
        let dir = tempfile::tempdir().unwrap();
        let writer = archive_table(dir.path()).await;
        archive(&writer, 909, vec![rows(1..=100)]).await;
        let first: Vec<String> = writer
            .open_table("messages")
            .await
            .unwrap()
            .get_file_uris()
            .unwrap()
            .collect();

        archive(&writer, 856, vec![rows(101..=150)]).await;
        let after: Vec<String> =
            writer.open_table("messages").await.unwrap().get_file_uris().unwrap().collect();
        assert!(first.iter().all(|f| after.contains(f)), "every file of unit 909 is still live");
        assert_eq!(after.len(), first.len() + 1);

        let b = query(&writer, "SELECT count(*) FROM t WHERE `_archive_unit` = 909").await;
        assert_eq!(i64_cell(&b, 0), 100);
    }

    #[tokio::test]
    async fn archive_unit_present_is_answered_from_the_data() {
        let dir = tempfile::tempdir().unwrap();
        let writer = DeltaWriter::new_local(dir.path().to_str().unwrap());
        assert!(!writer.archive_unit_present("messages", 909).await.unwrap(), "a missing table holds nothing");

        writer
            .ensure_table("messages", with_archive_unit_column(&source_schema()).unwrap())
            .await
            .unwrap();
        assert!(!writer.archive_unit_present("messages", 909).await.unwrap());
        archive(&writer, 909, vec![rows(1..=10)]).await;
        assert!(writer.archive_unit_present("messages", 909).await.unwrap());
        assert!(!writer.archive_unit_present("messages", 856).await.unwrap());
    }

    #[tokio::test]
    async fn archive_files_are_written_with_zstd() {
        let dir = tempfile::tempdir().unwrap();
        let writer = archive_table(dir.path()).await;
        archive(&writer, 909, vec![rows(1..=100)]).await;
        let table = writer.open_table("messages").await.unwrap();
        let uri = table.get_file_uris().unwrap().next().unwrap();
        let path = uri.strip_prefix("file://").unwrap_or(&uri);
        let file = std::fs::File::open(path).unwrap();
        let reader = deltalake::parquet::file::reader::SerializedFileReader::new(file).unwrap();
        use deltalake::parquet::file::reader::FileReader;
        let column = reader.metadata().row_group(0).column(0).compression();
        assert!(matches!(column, Compression::ZSTD(_)), "got {column:?}");
    }

    #[tokio::test]
    async fn a_unit_with_no_rows_makes_no_commit() {
        let dir = tempfile::tempdir().unwrap();
        let writer = archive_table(dir.path()).await;
        let before = writer.open_table("messages").await.unwrap().version();
        assert_eq!(archive(&writer, 909, vec![]).await, 0);
        assert_eq!(writer.open_table("messages").await.unwrap().version(), before);
    }

    #[tokio::test]
    async fn only_archive_tables_accept_archive_rows() {
        let dir = tempfile::tempdir().unwrap();
        let writer = DeltaWriter::new_local(dir.path().to_str().unwrap());
        writer.ensure_table("messages", source_schema()).await.unwrap();
        let err = writer.begin_archive_unit("messages", 909).await.unwrap_err().to_string();
        assert!(err.contains("not created as an archive table"), "got: {err}");
    }

    #[tokio::test]
    async fn two_concurrent_archives_of_a_unit_cannot_both_commit() {
        // Two runs over the same table (two writers), both past the presence check: the
        // transaction marker makes the second commit conflict instead of storing the unit twice.
        let dir = tempfile::tempdir().unwrap();
        let first = archive_table(dir.path()).await;
        let second = DeltaWriter::new_local(dir.path().to_str().unwrap());
        first.begin_archive_unit("messages", 909).await.unwrap();
        second.begin_archive_unit("messages", 909).await.unwrap();
        first.stage_archive_chunk("messages", vec![rows(1..=10)]).await.unwrap();
        second.stage_archive_chunk("messages", vec![rows(1..=10)]).await.unwrap();
        assert_eq!(first.commit_archive_unit("messages").await.unwrap(), 10);
        let err = format!("{:#}", second.commit_archive_unit("messages").await.unwrap_err());
        assert!(err.to_lowercase().contains("transaction") || err.contains("conflict"), "got: {err}");
        let b = query(&first, "SELECT count(*) FROM t").await;
        assert_eq!(i64_cell(&b, 0), 10, "the unit is stored once");
    }

    #[tokio::test]
    async fn an_archived_unit_cannot_be_appended_again() {
        let dir = tempfile::tempdir().unwrap();
        let writer = archive_table(dir.path()).await;
        archive(&writer, 909, vec![rows(1..=10)]).await;
        let err = writer.begin_archive_unit("messages", 909).await.unwrap_err().to_string();
        assert!(err.contains("already archived (transaction marker version 1)"), "got: {err}");
    }

    #[test]
    fn a_source_column_named_like_the_stamp_is_refused() {
        let schema = Schema::new(vec![Field::new(ARCHIVE_UNIT_COLUMN, DataType::Int64, true)]);
        let err = with_archive_unit_column(&schema).unwrap_err().to_string();
        assert!(err.contains("reserves that name"), "got: {err}");
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(Int64Array::from(vec![1]))]).unwrap();
        assert!(stamp_unit(&batch, 909).is_err());
    }

    #[tokio::test]
    async fn an_aborted_unit_leaves_the_table_unchanged() {
        let dir = tempfile::tempdir().unwrap();
        let writer = archive_table(dir.path()).await;
        let before = writer.open_table("messages").await.unwrap().version();
        writer.begin_archive_unit("messages", 909).await.unwrap();
        writer.stage_archive_chunk("messages", vec![rows(1..=10)]).await.unwrap();
        writer.abort_archive_unit("messages").await;
        assert_eq!(writer.open_table("messages").await.unwrap().version(), before);
        assert!(!writer.archive_unit_present("messages", 909).await.unwrap());
        // the session is gone, so a new one can start
        writer.begin_archive_unit("messages", 909).await.unwrap();
    }
}
