use std::collections::HashMap;

use anyhow::{Context, Result};
use deltalake::arrow::datatypes::SchemaRef;
#[cfg(test)]
use deltalake::arrow::array::{Int64Array, StringArray};
#[cfg(test)]
use deltalake::arrow::datatypes::{DataType, Schema as ArrowSchema};
use deltalake::arrow::record_batch::RecordBatch;
use deltalake::DeltaTable;
use deltalake::protocol::SaveMode;
use tracing::{info, warn};
use url::Url;

mod datetime;
mod hwm;
mod schema;
mod two_stream;

use hwm::build_commit_properties;
pub use hwm::{extract_hwm_from_batch, extract_max_id};
use schema::*;

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
