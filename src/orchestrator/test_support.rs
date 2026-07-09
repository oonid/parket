use crate::orchestrator::*;
use crate::config::Config;
use crate::discovery::{ColumnInfo, IndexInfo};
use crate::extractor::Extraction;
use anyhow::Result;
use deltalake::arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use std::path::PathBuf;
use tokio::sync::watch;

/// M2: `Extract::extract` returns `Result<Extraction>` now (batches + a `truncated` flag for
/// the mid-stream memory circuit breaker). Most orchestrator/loop tests don't exercise
/// truncation at all — this helper keeps their mock `.returning(|_| ok_batches(vec![...]))`
/// bodies as close as possible to the pre-M2 `Ok(vec![...])` shape.
pub(crate) fn ok_batches(batches: Vec<RecordBatch>) -> Result<Extraction> {
    Ok(Extraction { batches, truncated: false })
}

pub(crate) fn make_columns() -> Vec<ColumnInfo> {
    vec![
        ColumnInfo {
            name: "id".to_string(),
            data_type: "bigint".to_string(),
            column_type: "bigint(20)".to_string(),
        },
        ColumnInfo {
            name: "name".to_string(),
            data_type: "varchar".to_string(),
            column_type: "varchar(255)".to_string(),
        },
        ColumnInfo {
            name: "updated_at".to_string(),
            data_type: "timestamp".to_string(),
            column_type: "timestamp".to_string(),
        },
    ]
}

pub(crate) fn make_config(tables: Vec<String>) -> Config {
    Config {
        database_url: "mysql://u:p@h/db".to_string(),
        s3_bucket: "bucket".to_string(),
        s3_access_key_id: "key".to_string(),
        s3_secret_access_key: "secret".to_string(),
        tables,
        target_memory_mb: 512,
        merge_memory_mb: 512,
        merge_spill_dir: None,
        s3_endpoint: None,
        s3_region: "us-east-1".to_string(),
        s3_prefix: "parket".to_string(),
        default_batch_size: 10000,
        rust_log: "info".to_string(),
        table_modes: HashMap::new(),
        table_initial_hwm: HashMap::new(),
        table_timestamp_col: HashMap::new(),
        table_insert_cursor: HashMap::new(),
        table_update_cursor: HashMap::new(),
    }
}

pub(crate) fn make_orchestrator(
    config: Config,
    schema_mock: MockSchemaInspect,
    extract_mock: MockExtract,
    writer_mock: MockDeltaWrite,
    state_mock: MockStateManage,
    state_path: PathBuf,
) -> Orchestrator<MockSchemaInspect, MockExtract, MockDeltaWrite, MockStateManage> {
    let (_tx, rx) = watch::channel(false);
    Orchestrator::new(
        config,
        schema_mock,
        extract_mock,
        writer_mock,
        state_mock,
        rx,
        state_path,
        false,
    )
}

pub(crate) fn setup_incremental_mocks(
    schema_mock: &mut MockSchemaInspect,
    extract_mock: &mut MockExtract,
    writer_mock: &mut MockDeltaWrite,
    state_mock: &mut MockStateManage,
) {
    schema_mock
        .expect_discover_columns()
        .returning(move |_| Ok(make_columns()));
    schema_mock
        .expect_discover_indexes()
        .returning(|_| Ok(make_full_refresh_indexes()));
    schema_mock
        .expect_get_avg_row_length()
        .returning(|_| Ok(Some(100)));
    extract_mock
        .expect_calculate_batch_size()
        .returning(|_| 10000);
    extract_mock
        .expect_batch_size()
        .returning(|| 10000);
    writer_mock
        .expect_ensure_table()
        .returning(|_, _| Ok(()));
    writer_mock
        .expect_get_schema()
        .returning(|_| Ok(None));
    writer_mock
        .expect_read_hwm()
        .returning(|_| Ok(None));
    extract_mock
        .expect_extract()
        .returning(|_| ok_batches(vec![]));
    state_mock
        .expect_load_or_default()
        .returning(|_| crate::state::AppState::default());
    state_mock
        .expect_update_table()
        .returning(|_, _, _| Ok(()));
}

pub(crate) fn make_full_refresh_indexes() -> Vec<IndexInfo> {
    vec![]
}

pub(crate) fn make_full_refresh_primary_key(key_col: &str) -> Vec<IndexInfo> {
    vec![IndexInfo {
        name: "PRIMARY".to_string(),
        unique: true,
        columns: vec![key_col.to_string()],
    }]
}

pub(crate) fn make_full_refresh_unique_key(key_col: &str) -> Vec<IndexInfo> {
    vec![IndexInfo {
        name: format!("{key_col}_uniq"),
        unique: true,
        columns: vec![key_col.to_string()],
    }]
}

/// N5: builds the Delta-schema `SchemaRef` a set of `ColumnInfo` would produce, for tests
/// that call `process_full_refresh`/`process_incremental`/`process_two_stream` directly
/// (bypassing `process_table`, which normally computes this and threads it through).
pub(crate) fn schema_from_columns(columns: &[ColumnInfo]) -> deltalake::arrow::datatypes::SchemaRef {
    use deltalake::arrow::datatypes::{Field, Schema};
    let fields: Vec<Field> = columns
        .iter()
        .map(|c| {
            let dt = super::schema::mariadb_type_to_arrow(&c.data_type, &c.column_type)
                .expect("test column type must be supported by mariadb_type_to_arrow");
            Field::new(&c.name, dt, true)
        })
        .collect();
    std::sync::Arc::new(Schema::new(fields))
}

pub(crate) fn make_full_refresh_columns() -> Vec<ColumnInfo> {
    vec![
        ColumnInfo {
            name: "id".to_string(),
            data_type: "bigint".to_string(),
            column_type: "bigint(20)".to_string(),
        },
        ColumnInfo {
            name: "name".to_string(),
            data_type: "varchar".to_string(),
            column_type: "varchar(255)".to_string(),
        },
    ]
}

pub(crate) fn make_config_with_full_refresh(tables: Vec<String>) -> Config {
    Config {
        database_url: "mysql://u:p@h/db".to_string(),
        s3_bucket: "bucket".to_string(),
        s3_access_key_id: "key".to_string(),
        s3_secret_access_key: "secret".to_string(),
        tables,
        target_memory_mb: 512,
        merge_memory_mb: 512,
        merge_spill_dir: None,
        s3_endpoint: None,
        s3_region: "us-east-1".to_string(),
        s3_prefix: "parket".to_string(),
        default_batch_size: 10000,
        rust_log: "info".to_string(),
        table_modes: HashMap::new(),
        table_initial_hwm: HashMap::new(),
        table_timestamp_col: HashMap::new(),
        table_insert_cursor: HashMap::new(),
        table_update_cursor: HashMap::new(),
    }
}
