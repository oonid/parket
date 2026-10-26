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
use deltalake::kernel::{Action, Add, Remove};
use deltalake::parquet::basic::{Compression, ZstdLevel};
use deltalake::parquet::file::properties::WriterProperties;
use deltalake::protocol::{DeltaOperation, SaveMode};
use deltalake::writer::{DeltaWriter as _, RecordBatchWriter};
use tracing::info;

use super::{DeltaWriter, arrow_type_to_delta, coerce_batch_to_schema, is_missing_table_error};
use crate::archive::SOURCE_TIME_ZONE_PROPERTY;
use deltalake::kernel::{MetadataValue, StructField};

/// The column every archived row carries: the id of the unit (e.g. the campaign) it belongs to.
pub const ARCHIVE_UNIT_COLUMN: &str = "_archive_unit";

/// One in-flight `(table, unit)` append: files are written as chunks arrive and committed once.
pub(super) struct ArchiveSession {
    writer: RecordBatchWriter,
    table: DeltaTable,
    unit: i64,
    adds: Vec<Add>,
    rows: u64,
    /// `Some` for a unit-scoped replace: the unit's current files, removed by the same commit
    /// that adds the re-extracted ones.
    replacing: Option<Vec<Remove>>,
}

/// The file-path column the replace scan adds to find which files hold a unit.
const FILE_COLUMN: &str = "__parket_file";

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
    /// Create the archive table if it does not exist: `schema` (from `archive::archive_schema`)
    /// with every field's metadata kept — the sync path's `arrow_schema_to_delta` drops it — and
    /// the source time zone as a table property. An existing table is returned as is;
    /// `begin_archive_unit` refuses one that is not an archive table.
    pub async fn ensure_archive_table(
        &self,
        table_name: &str,
        schema: SchemaRef,
        source_time_zone: &str,
    ) -> Result<DeltaTable> {
        match self.open_table(table_name).await {
            Ok(table) => return Ok(table),
            Err(e) if is_missing_table_error(&e) => {}
            Err(e) => return Err(e).context(format!("archive: opening `{table_name}`")),
        }
        let fields = schema
            .fields()
            .iter()
            .map(|f| {
                let field = StructField::new(f.name(), arrow_type_to_delta(f.data_type())?, f.is_nullable());
                Ok(field.with_metadata(
                    f.metadata().iter().map(|(k, v)| (k.clone(), MetadataValue::String(v.clone()))),
                ))
            })
            .collect::<Result<Vec<StructField>>>()?;
        let table = deltalake::DeltaTableBuilder::from_url(self.table_url(table_name)?)?
            .with_storage_options(self.storage_options.clone())
            .build()?;
        let created = table
            .create()
            .with_columns(fields)
            .with_table_name(table_name)
            .with_configuration([(SOURCE_TIME_ZONE_PROPERTY, Some(source_time_zone))])
            // the property is parket's own, not a `delta.*` key
            .with_raise_if_key_not_exists(false)
            .await?;
        info!(table = table_name, source_time_zone, "archive: table created");
        Ok(created)
    }

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
        self.begin_session(table_name, table, unit, None).await
    }

    /// Start a **unit-scoped replace**: re-extracted rows of `unit` will replace exactly the files
    /// that hold it now, in one commit. Returns how many files that is. Refuses a file holding
    /// another unit as well — replacing it would drop that unit's rows. The caller must only
    /// replace a unit it has not handed over for deletion from the source
    /// (docs/design/archive-mode.md); `commit_archive_unit` refuses an empty new copy.
    pub async fn begin_replace_unit(&self, table_name: &str, unit: i64) -> Result<usize> {
        let table = self.open_table(table_name).await?;
        let removes = self.unit_files(&table, table_name, unit).await?;
        let files = removes.len();
        self.begin_session(table_name, table, unit, Some(removes)).await?;
        Ok(files)
    }

    /// The files of `table` that hold `unit`, as `Remove` actions. Found from the data (a file-path
    /// column), not from statistics, and checked: every such file must hold `unit` alone.
    async fn unit_files(&self, table: &DeltaTable, table_name: &str, unit: i64) -> Result<Vec<Remove>> {
        let ctx = deltalake::datafusion::prelude::SessionContext::new();
        ctx.register_table("t", table.table_provider().with_file_column(FILE_COLUMN).await?)?;
        let sql = format!(
            "SELECT {FILE_COLUMN}, min(`{ARCHIVE_UNIT_COLUMN}`), max(`{ARCHIVE_UNIT_COLUMN}`) FROM t \
             GROUP BY {FILE_COLUMN} \
             HAVING sum(CASE WHEN `{ARCHIVE_UNIT_COLUMN}` = {unit} THEN 1 ELSE 0 END) > 0"
        );
        let batches = ctx.sql(&sql).await?.collect().await?;
        let mut paths = Vec::new();
        for b in &batches {
            for row in 0..b.num_rows() {
                let path = deltalake::arrow::util::display::array_value_to_string(b.column(0), row)?;
                let bounds = (
                    deltalake::arrow::util::display::array_value_to_string(b.column(1), row)?,
                    deltalake::arrow::util::display::array_value_to_string(b.column(2), row)?,
                );
                if bounds != (unit.to_string(), unit.to_string()) {
                    bail!(
                        "`{table_name}`: file {path} holds units {} to {}, not unit {unit} alone; \
                         refusing a replace that would drop another unit's rows",
                        bounds.0, bounds.1
                    );
                }
                paths.push(path);
            }
        }
        let removes: Vec<Remove> = table
            .snapshot()?
            .log_data()
            .iter()
            .filter(|f| paths.iter().any(|p| p.ends_with(f.path().as_ref())))
            .map(|f| f.remove_action(true))
            .collect();
        if removes.len() != paths.len() {
            bail!(
                "`{table_name}`: unit {unit} is in {} file(s) but {} matched the table's file list",
                paths.len(),
                removes.len()
            );
        }
        Ok(removes)
    }

    async fn begin_session(
        &self,
        table_name: &str,
        table: DeltaTable,
        unit: i64,
        replacing: Option<Vec<Remove>>,
    ) -> Result<()> {
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
            ArchiveSession { writer, table, unit, adds: Vec::new(), rows: 0, replacing },
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

    /// Commit the unit's staged files as ONE commit and return the rows it added. An archive
    /// commit is append-only — `Add` actions only, never a `Remove`; a unit with no rows gets no
    /// commit. A replace commit also removes the unit's previous files, and is refused when the new
    /// copy is empty: with the source rows gone (purged), it would delete archived rows and put
    /// nothing back. `commitInfo` carries `archive_unit` and `archive_rows` as an audit trail;
    /// `_archive_unit` in the data is the record `archive_unit_present` relies on.
    pub async fn commit_archive_unit(&self, table_name: &str) -> Result<u64> {
        let session = self.archive_sessions.lock().await.remove(table_name).with_context(|| {
            format!("commit_archive_unit: no archive session in progress for `{table_name}`")
        })?;
        let ArchiveSession { table, unit, adds, rows, replacing, .. } = session;
        if adds.is_empty() {
            if let Some(removes) = replacing.filter(|r| !r.is_empty()) {
                bail!(
                    "refusing to replace unit {unit} of `{table_name}`: the source holds no rows of it \
                     now, so the replace would delete its {} archived file(s) and add nothing; the \
                     table is unchanged",
                    removes.len()
                );
            }
            info!(table = table_name, unit, "archive: unit has no rows in this table; nothing to commit");
            return Ok(0);
        }
        let files = adds.len();
        let (operation, mut actions) = match replacing {
            None => (DeltaOperation::Write { mode: SaveMode::Append, partition_by: None, predicate: None }, Vec::new()),
            Some(removes) => (
                DeltaOperation::Write {
                    mode: SaveMode::Overwrite,
                    partition_by: None,
                    predicate: Some(format!("{ARCHIVE_UNIT_COLUMN} = {unit}")),
                },
                removes.into_iter().map(Action::Remove).collect(),
            ),
        };
        actions.extend(adds.into_iter().map(Action::Add));
        let metadata = HashMap::from([
            ("archive_unit".to_string(), serde_json::Value::String(unit.to_string())),
            ("archive_rows".to_string(), serde_json::Value::String(rows.to_string())),
        ]);
        let finalized = CommitBuilder::from(CommitProperties::default().with_metadata(metadata))
            .with_actions(actions)
            .build(Some(table.snapshot()?), table.log_store(), operation)
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
    async fn an_archive_table_keeps_typed_utc_timestamps_and_the_source_zone() {
        use crate::archive::{SOURCE_TIME_ZONE_KEY, archive_schema, to_archive_batch};
        use crate::discovery::ColumnInfo;
        let col = |name: &str, dt: &str| ColumnInfo {
            name: name.into(),
            data_type: dt.into(),
            column_type: dt.into(),
            nullable: true,
        };
        let dir = tempfile::tempdir().unwrap();
        let writer = DeltaWriter::new_local(dir.path().to_str().unwrap());
        let schema = archive_schema(&[col("id", "bigint"), col("sent_at", "timestamp")], "Asia/Jakarta").unwrap();
        writer.ensure_archive_table("messages", schema.clone(), "Asia/Jakarta").await.unwrap();

        // The zone survives into the Delta schema and the table properties.
        let table = writer.open_table("messages").await.unwrap();
        let snapshot = table.snapshot().unwrap();
        let sent = snapshot.schema().field("sent_at").unwrap().clone();
        assert_eq!(
            sent.metadata().get(SOURCE_TIME_ZONE_KEY),
            Some(&MetadataValue::String("Asia/Jakarta".into()))
        );
        assert_eq!(
            snapshot.metadata().configuration().get(SOURCE_TIME_ZONE_PROPERTY).map(String::as_str),
            Some("Asia/Jakarta")
        );

        // Extracted UTC text becomes instants, archived as one unit.
        let extracted = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("sent_at", DataType::Utf8, true),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec![Some("2026-10-26 04:30:44"), None])),
            ],
        )
        .unwrap();
        let without_unit = Schema::new(
            schema.fields().iter().filter(|f| f.name() != ARCHIVE_UNIT_COLUMN).cloned().collect::<Vec<_>>(),
        );
        let typed = to_archive_batch(&extracted, &without_unit).unwrap();
        writer.begin_archive_unit("messages", 909).await.unwrap();
        writer.stage_archive_chunk("messages", vec![typed]).await.unwrap();
        assert_eq!(writer.commit_archive_unit("messages").await.unwrap(), 2);

        // Read back in WIB with `AT TIME ZONE`. (DataFusion's `execution.time_zone` session setting
        // does NOT change how a stored timestamp displays — checked, so readers must convert.)
        let table = writer.open_table("messages").await.unwrap();
        let ctx = deltalake::datafusion::prelude::SessionContext::new();
        ctx.register_table("t", table.table_provider().await.unwrap()).unwrap();
        let b = ctx
            .sql("SELECT cast(sent_at AT TIME ZONE 'Asia/Jakarta' AS varchar) FROM t WHERE id = 1")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let shown = deltalake::arrow::util::display::array_value_to_string(b[0].column(0), 0).unwrap();
        assert_eq!(shown, "2026-10-26T11:30:44+07:00", "04:30:44 UTC shown in WIB");
    }

    async fn file_uris(writer: &DeltaWriter) -> Vec<String> {
        writer.open_table("messages").await.unwrap().get_file_uris().unwrap().collect()
    }

    async fn replace(writer: &DeltaWriter, unit: i64, chunks: Vec<RecordBatch>) -> Result<u64> {
        writer.begin_replace_unit("messages", unit).await?;
        for chunk in chunks {
            writer.stage_archive_chunk("messages", vec![chunk]).await?;
        }
        writer.commit_archive_unit("messages").await
    }

    #[tokio::test]
    async fn a_replace_swaps_exactly_one_unit_in_one_commit() {
        let dir = tempfile::tempdir().unwrap();
        let writer = archive_table(dir.path()).await;
        archive(&writer, 909, vec![rows(1..=60), rows(61..=100)]).await;
        let unit_909_files = file_uris(&writer).await;
        archive(&writer, 856, vec![rows(101..=150)]).await;
        let other_files: Vec<String> =
            file_uris(&writer).await.into_iter().filter(|f| !unit_909_files.contains(f)).collect();
        assert_eq!(other_files.len(), 1);
        let before = writer.open_table("messages").await.unwrap().version();

        assert_eq!(writer.begin_replace_unit("messages", 909).await.unwrap(), 2, "unit 909 is in two files");
        writer.stage_archive_chunk("messages", vec![rows(1..=99)]).await.unwrap();
        assert_eq!(writer.commit_archive_unit("messages").await.unwrap(), 99);

        let table = writer.open_table("messages").await.unwrap();
        assert_eq!(table.version(), before.map(|v| v + 1), "one commit");
        let history = table.history(Some(1)).await.unwrap().collect::<Vec<_>>();
        let params = history[0].operation_parameters.clone().unwrap_or_default();
        assert_eq!(params.get("mode"), Some(&serde_json::json!("Overwrite")));
        assert_eq!(params.get("predicate"), Some(&serde_json::json!("_archive_unit = 909")));
        let live: Vec<String> = table.get_file_uris().unwrap().collect();
        assert!(other_files.iter().all(|f| live.contains(f)), "unit 856's file is untouched");
        assert!(unit_909_files.iter().all(|f| !live.contains(f)), "unit 909's old files are removed");
        let b = query(&writer, "SELECT count(*) FROM t WHERE `_archive_unit` = 909").await;
        assert_eq!(i64_cell(&b, 0), 99);
        let b = query(&writer, "SELECT count(*) FROM t WHERE `_archive_unit` = 856").await;
        assert_eq!(i64_cell(&b, 0), 50);
    }

    #[tokio::test]
    async fn a_replace_refuses_an_empty_new_copy() {
        let dir = tempfile::tempdir().unwrap();
        let writer = archive_table(dir.path()).await;
        archive(&writer, 909, vec![rows(1..=10)]).await;
        let before = writer.open_table("messages").await.unwrap().version();
        let err = replace(&writer, 909, vec![]).await.unwrap_err().to_string();
        assert!(err.contains("refusing to replace unit 909") && err.contains("no rows"), "got: {err}");
        assert_eq!(writer.open_table("messages").await.unwrap().version(), before, "table unchanged");
        let b = query(&writer, "SELECT count(*) FROM t WHERE `_archive_unit` = 909").await;
        assert_eq!(i64_cell(&b, 0), 10, "the archived rows are still there");
    }

    #[tokio::test]
    async fn a_replace_refuses_a_file_holding_another_unit_too() {
        let dir = tempfile::tempdir().unwrap();
        let writer = archive_table(dir.path()).await;
        // A file the archive writer would never produce: two units in one file.
        let mixed = RecordBatch::try_new(
            with_archive_unit_column(&source_schema()).unwrap(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["a", "b"])),
                Arc::new(Int64Array::from(vec![909, 856])),
            ],
        )
        .unwrap();
        let table = writer.open_table("messages").await.unwrap();
        table.write(vec![mixed]).with_save_mode(SaveMode::Append).await.unwrap();
        let err = writer.begin_replace_unit("messages", 909).await.unwrap_err().to_string();
        assert!(err.contains("not unit 909 alone"), "got: {err}");
    }

    #[tokio::test]
    async fn replacing_a_unit_never_archived_archives_it() {
        let dir = tempfile::tempdir().unwrap();
        let writer = archive_table(dir.path()).await;
        assert_eq!(replace(&writer, 909, vec![rows(1..=5)]).await.unwrap(), 5);
        assert!(writer.archive_unit_present("messages", 909).await.unwrap());
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
