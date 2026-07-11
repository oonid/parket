use std::collections::HashMap;
use std::path::PathBuf;

use aws_sdk_s3::config::Credentials;
use aws_sdk_s3::primitives::ByteStream;
use futures::StreamExt;
use parket::config::Config;
use parket::orchestrator::{
    DeltaWriterAdapter, ExitCode, ExtractorAdapter, Orchestrator, SchemaInspectorAdapter,
    SignalHandler, StateManageAdapter,
};
use parket::verify::{
    DeltaProbeAdapter, SourceProbeAdapter, TablePlan, VerifyCommand, VerifyMode, VerifyVerdict,
};
use parket::writer::DeltaWriter;
use sqlx::MySqlPool;
use tempfile::TempDir;
use testcontainers::runners::AsyncRunner;
use testcontainers::ImageExt;
use testcontainers_modules::mariadb::Mariadb;
use testcontainers_modules::minio::MinIO;
use tokio::sync::watch;

async fn create_minio_bucket(endpoint: &str, bucket: &str) {
    let s3_config = aws_sdk_s3::config::Builder::new()
        .endpoint_url(endpoint)
        .region(aws_sdk_s3::config::Region::new("us-east-1"))
        .credentials_provider(Credentials::new(
            "minioadmin",
            "minioadmin",
            None,
            None,
            "test",
        ))
        .force_path_style(true)
        .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
        .build();
    let client = aws_sdk_s3::Client::from_conf(s3_config);
    client
        .create_bucket()
        .bucket(bucket)
        .send()
        .await
        .expect("failed to create bucket");
}

fn make_config(db_url: &str, s3_endpoint: &str, tables: Vec<&str>) -> Config {
    Config {
        database_url: db_url.to_string(),
        s3_bucket: "test-bucket".to_string(),
        s3_access_key_id: "minioadmin".to_string(),
        s3_secret_access_key: "minioadmin".to_string(),
        tables: tables.into_iter().map(|s| s.to_string()).collect(),
        target_memory_mb: 64,
        merge_memory_mb: 64,
        merge_spill_dir: None,
        s3_endpoint: Some(s3_endpoint.to_string()),
        s3_region: "us-east-1".to_string(),
        s3_prefix: "parket".to_string(),
        default_batch_size: 10000,
        rust_log: "parket=debug".to_string(),
        table_modes: HashMap::new(),
        table_initial_hwm: HashMap::new(),
        table_timestamp_col: HashMap::new(),
        table_insert_cursor: HashMap::new(),
        table_update_cursor: HashMap::new(),
    }
}

#[allow(dead_code)]
struct TestEnv {
    db_url: String,
    s3_endpoint: String,
    config: Config,
    pool: MySqlPool,
    state_dir: TempDir,
    _db: testcontainers::ContainerAsync<Mariadb>,
    _storage: testcontainers::ContainerAsync<MinIO>,
}

impl TestEnv {
    async fn new(tables: Vec<&str>) -> Self {
        let db = Mariadb::default()
            .with_env_var("MARIADB_ROOT_PASSWORD", "testpwd")
            .with_env_var("MARIADB_DATABASE", "parket")
            .start()
            .await
            .expect("MariaDB container failed to start");

        let storage = MinIO::default()
            .with_env_var("MINIO_ROOT_USER", "minioadmin")
            .with_env_var("MINIO_ROOT_PASSWORD", "minioadmin")
            .start()
            .await
            .expect("MinIO container failed to start");

        let db_host = db.get_host().await.unwrap();
        let db_port = db.get_host_port_ipv4(3306).await.unwrap();
        let db_url = format!("mysql://root:testpwd@{db_host}:{db_port}/parket");

        let s3_host = storage.get_host().await.unwrap();
        let s3_port = storage.get_host_port_ipv4(9000).await.unwrap();
        let s3_endpoint = format!("http://{s3_host}:{s3_port}");

        create_minio_bucket(&s3_endpoint, "test-bucket").await;

        let pool = MySqlPool::connect(&db_url)
            .await
            .expect("failed to connect to MariaDB");

        let config = make_config(&db_url, &s3_endpoint, tables);
        let state_dir = TempDir::new().expect("failed to create temp dir");

        Self {
            db_url,
            s3_endpoint,
            config,
            pool,
            state_dir,
            _db: db,
            _storage: storage,
        }
    }

    fn state_path(&self) -> PathBuf {
        self.state_dir.path().join("state.json")
    }

    fn make_orchestrator(
        &self,
    ) -> Orchestrator<
        SchemaInspectorAdapter,
        ExtractorAdapter,
        DeltaWriterAdapter,
        StateManageAdapter,
    > {
        let (_signal_handler, shutdown_rx) = SignalHandler::new();
        self.make_orchestrator_with_shutdown(shutdown_rx)
    }

    fn make_orchestrator_with_shutdown(
        &self,
        shutdown_rx: watch::Receiver<bool>,
    ) -> Orchestrator<
        SchemaInspectorAdapter,
        ExtractorAdapter,
        DeltaWriterAdapter,
        StateManageAdapter,
    > {
        let database = "parket".to_string();
        let schema_inspect = SchemaInspectorAdapter::new(self.pool.clone(), database);
        let extractor = ExtractorAdapter::new(&self.config);
        let writer = DeltaWriterAdapter::new(&self.config);
        let state_mgr = StateManageAdapter::new();

        Orchestrator::new(
            self.config.clone(),
            schema_inspect,
            extractor,
            writer,
            state_mgr,
            shutdown_rx,
            self.state_path(),
            false,
        )
    }

    async fn open_delta_table(&self, table_name: &str) -> deltalake::DeltaTable {
        let writer = DeltaWriter::new(
            &self.config.s3_bucket,
            &self.config.s3_prefix,
            self.config.s3_endpoint.as_deref(),
            &self.config.s3_region,
            &self.config.s3_access_key_id,
            &self.config.s3_secret_access_key,
        );
        writer
            .open_table(table_name)
            .await
            .expect("failed to open delta table")
    }
}

#[tokio::test]
async fn smoke_mariadb_and_minio_containers_start() {
    let db = Mariadb::default()
        .with_env_var("MARIADB_ROOT_PASSWORD", "testpwd")
        .with_env_var("MARIADB_DATABASE", "parket")
        .start()
        .await
        .expect("MariaDB container failed to start");

    let storage = MinIO::default()
        .with_env_var("MINIO_ROOT_USER", "minioadmin")
        .with_env_var("MINIO_ROOT_PASSWORD", "minioadmin")
        .start()
        .await
        .expect("MinIO container failed to start");

    let db_host = db.get_host().await.unwrap();
    let db_port = db.get_host_port_ipv4(3306).await.unwrap();
    let db_url = format!("mysql://root:testpwd@{db_host}:{db_port}/parket");

    let s3_host = storage.get_host().await.unwrap();
    let s3_port = storage.get_host_port_ipv4(9000).await.unwrap();
    let s3_endpoint = format!("http://{s3_host}:{s3_port}");

    let pool = MySqlPool::connect(&db_url)
        .await
        .expect("failed to connect to MariaDB");

    sqlx::query("CREATE TABLE smoke_test (id BIGINT PRIMARY KEY, val VARCHAR(100))")
        .execute(&pool)
        .await
        .expect("failed to create table");

    sqlx::query("INSERT INTO smoke_test (id, val) VALUES (1, 'hello')")
        .execute(&pool)
        .await
        .expect("failed to insert row");

    let (row_val,) = sqlx::query_as::<_, (String,)>("SELECT val FROM smoke_test WHERE id = 1")
        .fetch_one(&pool)
        .await
        .expect("failed to query");
    assert_eq!(row_val, "hello");

    create_minio_bucket(&s3_endpoint, "test-bucket").await;

    let s3_conf = aws_sdk_s3::config::Builder::new()
        .endpoint_url(&s3_endpoint)
        .region(aws_sdk_s3::config::Region::new("us-east-1"))
        .credentials_provider(Credentials::new(
            "minioadmin",
            "minioadmin",
            None,
            None,
            "test",
        ))
        .force_path_style(true)
        .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
        .build();
    let s3_client = aws_sdk_s3::Client::from_conf(s3_conf);

    s3_client
        .put_object()
        .bucket("test-bucket")
        .key("test-key")
        .body(ByteStream::from(b"hello-s3".to_vec()))
        .send()
        .await
        .expect("failed to put object");

    let resp = s3_client
        .get_object()
        .bucket("test-bucket")
        .key("test-key")
        .send()
        .await
        .expect("failed to get object");
    let body = resp.body.collect().await.unwrap().into_bytes();
    assert_eq!(&body[..], b"hello-s3");

    pool.close().await;
}

#[tokio::test]
#[serial_test::serial]
async fn testenv_fixture_creates_containers_and_bucket() {
    let env = TestEnv::new(vec!["smoke_table"]).await;

    sqlx::query("CREATE TABLE smoke_table (id BIGINT PRIMARY KEY, val VARCHAR(100))")
        .execute(&env.pool)
        .await
        .expect("failed to create table");

    sqlx::query("INSERT INTO smoke_table (id, val) VALUES (1, 'fixture-works')")
        .execute(&env.pool)
        .await
        .expect("failed to insert row");

    let (row_val,) =
        sqlx::query_as::<_, (String,)>("SELECT val FROM smoke_table WHERE id = 1")
            .fetch_one(&env.pool)
            .await
            .expect("failed to query");
    assert_eq!(row_val, "fixture-works");

    assert!(env.state_path().parent().unwrap().exists());
    assert!(!env.state_path().exists());
}

async fn count_delta_rows(env: &TestEnv, table_name: &str) -> usize {
    let mut table = env.open_delta_table(table_name).await;
    table.load().await.expect("failed to load delta table");
    let stream = table.scan_table().await.expect("scan_table failed").1;
    futures::pin_mut!(stream);
    let mut total = 0;
    while let Some(batch) = stream.next().await {
        let batch = batch.expect("failed to read batch");
        total += batch.num_rows();
    }
    total
}

/// Count rows in a Delta table matching a SQL WHERE clause (via datafusion over the table provider).
async fn count_matching(env: &TestEnv, table_name: &str, where_clause: &str) -> i64 {
    let t = env.open_delta_table(table_name).await;
    let ctx = deltalake::datafusion::prelude::SessionContext::new();
    let provider = t.table_provider().await.expect("table_provider failed");
    ctx.register_table(table_name, provider).unwrap();
    let batches = ctx
        .sql(&format!("SELECT COUNT(*) AS c FROM {table_name} WHERE {where_clause}"))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<deltalake::arrow::array::Int64Array>()
        .expect("COUNT(*) should be Int64")
        .value(0)
}

#[tokio::test]
#[serial_test::serial]
async fn graceful_shutdown_signal_skips_all_tables() {
    let _guard = tracing_subscriber::fmt()
        .with_env_filter("parket=debug")
        .with_test_writer()
        .try_init();

    let env = TestEnv::new(vec!["orders", "products"]).await;

    sqlx::query(
        "CREATE TABLE orders (\
            id BIGINT AUTO_INCREMENT PRIMARY KEY, \
            name VARCHAR(255), \
            qty INT, \
            updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6)\
        )",
    )
    .execute(&env.pool)
    .await
    .expect("failed to create orders table");

    sqlx::query("CREATE INDEX idx_orders_updated_at ON orders (updated_at)")
        .execute(&env.pool)
        .await
        .expect("failed to create index");

    sqlx::query(
        "INSERT INTO orders (name, qty, updated_at) VALUES \
            ('widget', 10, '2026-01-01 10:00:00.000000')",
    )
    .execute(&env.pool)
        .await
        .expect("failed to insert orders");

    sqlx::query(
        "CREATE TABLE products (\
            sku VARCHAR(50) PRIMARY KEY, \
            description TEXT, \
            price DOUBLE\
        )",
    )
    .execute(&env.pool)
    .await
    .expect("failed to create products table");

    sqlx::query(
        "INSERT INTO products (sku, description, price) VALUES \
            ('W-001', 'Widget', 9.99), \
            ('G-001', 'Gadget', 19.99)"
    )
    .execute(&env.pool)
    .await
    .expect("failed to insert products");

    let (tx, shutdown_rx) = tokio::sync::watch::channel(false);
    tx.send(true).unwrap();

    let mut orchestrator = env.make_orchestrator_with_shutdown(shutdown_rx);
    let exit_code = orchestrator.run().await;

    // O2/R4: a run cut off by shutdown must not report clean success — it exits
    // PartialFailure so schedulers can tell it was interrupted.
    assert!(
        matches!(exit_code, ExitCode::PartialFailure),
        "expected PartialFailure exit code for an interrupted run, got {exit_code:?}"
    );

    let writer = DeltaWriter::new(
        &env.config.s3_bucket,
        &env.config.s3_prefix,
        env.config.s3_endpoint.as_deref(),
        &env.config.s3_region,
        &env.config.s3_access_key_id,
        &env.config.s3_secret_access_key,
    );
    let orders_exists = writer.open_table("orders").await.is_ok();
    let products_exists = writer.open_table("products").await.is_ok();

    assert!(
        !orders_exists,
        "no orders Delta table should exist (shutdown before processing)"
    );
    assert!(
        !products_exists,
        "no products Delta table should exist (shutdown before processing)"
    );
}

#[tokio::test]
#[serial_test::serial]
async fn full_refresh_extraction_creates_delta_table_with_all_rows() {
    let _guard = tracing_subscriber::fmt()
        .with_env_filter("parket=debug")
        .with_test_writer()
        .try_init();

    let env = TestEnv::new(vec!["products"]).await;

    sqlx::query(
        "CREATE TABLE products (\
            sku VARCHAR(50) PRIMARY KEY, \
            description TEXT, \
            price DOUBLE\
        )"
    )
    .execute(&env.pool)
    .await
    .expect("failed to create products table");

    sqlx::query(
        "INSERT INTO products (sku, description, price) VALUES \
            ('W-001', 'Widget', 9.99), \
            ('G-001', 'Gadget', 19.99)"
    )
    .execute(&env.pool)
    .await
    .expect("failed to insert products");

    let mut orchestrator = env.make_orchestrator();
    let exit_code = orchestrator.run().await;

    assert!(
        matches!(exit_code, ExitCode::Success),
        "expected Success exit code, got {exit_code:?}"
    );

    let row_count = count_delta_rows(&env, "products").await;
    assert_eq!(row_count, 2, "expected 2 rows in Delta table for products");
}

#[tokio::test]
#[serial_test::serial]
async fn incremental_extraction_creates_delta_table_with_hwm() {
    let _guard = tracing_subscriber::fmt()
        .with_env_filter("parket=debug")
        .with_test_writer()
        .try_init();

    let env = TestEnv::new(vec!["orders"]).await;

    sqlx::query(
        "CREATE TABLE orders (\
            id BIGINT AUTO_INCREMENT PRIMARY KEY, \
            name VARCHAR(255), \
            qty INT, \
            updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6)\
        )",
    )
    .execute(&env.pool)
    .await
    .expect("failed to create orders table");

    sqlx::query("CREATE INDEX idx_orders_updated_at ON orders (updated_at)")
        .execute(&env.pool)
        .await
        .expect("failed to create index");

    sqlx::query(
        "INSERT INTO orders (name, qty, updated_at) VALUES \
            ('widget', 10, '2026-01-01 10:00:00.000000'), \
            ('gadget', 5,  '2026-01-01 11:00:00.000000'), \
            ('doohickey', 3, '2026-01-02 09:00:00.000000')",
    )
    .execute(&env.pool)
        .await
        .expect("failed to insert orders");

    let mut orchestrator = env.make_orchestrator();
    let exit_code = orchestrator.run().await;

    assert!(
        matches!(exit_code, ExitCode::Success),
        "expected Success exit code, got {exit_code:?}"
    );

    let row_count = count_delta_rows(&env, "orders").await;
    assert_eq!(row_count, 3, "expected 3 rows in Delta table for orders");

    let writer = DeltaWriter::new(
        &env.config.s3_bucket,
        &env.config.s3_prefix,
        env.config.s3_endpoint.as_deref(),
        &env.config.s3_region,
        &env.config.s3_access_key_id,
        &env.config.s3_secret_access_key,
    );
    let hwm = writer.read_hwm("orders").await.expect("read_hwm failed");
    assert!(hwm.is_some(), "HWM should be set after incremental extraction");
    let hwm = hwm.unwrap();
    assert!(
        hwm.updated_at.starts_with("2026-01-02"),
        "HWM updated_at should be 2026-01-02, got: {}",
        hwm.updated_at,
    );
    assert_eq!(hwm.last_id, 3, "HWM last_id should be 3");
}

/// D2: an explicitly-configured `TABLE_MODE=incremental` on a NULLABLE cursor column is
/// honored (O3 decision b), but rows whose cursor is NULL are silently excluded by the
/// `WHERE updated_at IS NOT NULL` filter. This documents that known limitation end-to-end:
/// the run SUCCEEDS, the non-NULL rows sync, and the NULL-cursor rows are genuinely ABSENT
/// (the count_null probe warns about them but does not fail the table).
#[tokio::test]
#[serial_test::serial]
async fn incremental_nullable_cursor_excludes_null_rows_but_succeeds() {
    let _guard = tracing_subscriber::fmt()
        .with_env_filter("parket=debug")
        .with_test_writer()
        .try_init();

    let mut env = TestEnv::new(vec!["orders"]).await;
    // Explicit incremental override on a nullable cursor (the D2 scenario).
    env.config
        .table_modes
        .insert("orders".to_string(), parket::config::ExtractionMode::Incremental);

    sqlx::query(
        "CREATE TABLE orders (\
            id BIGINT AUTO_INCREMENT PRIMARY KEY, \
            name VARCHAR(255), \
            qty INT, \
            updated_at TIMESTAMP(6) NULL DEFAULT NULL\
        )",
    )
    .execute(&env.pool)
    .await
    .expect("failed to create orders table");

    // 3 rows with a non-NULL cursor + 2 rows whose updated_at IS NULL.
    sqlx::query(
        "INSERT INTO orders (name, qty, updated_at) VALUES \
            ('widget', 10, '2026-01-01 10:00:00.000000'), \
            ('gadget', 5,  '2026-01-01 11:00:00.000000'), \
            ('doohickey', 3, '2026-01-02 09:00:00.000000'), \
            ('orphan_a', 1, NULL), \
            ('orphan_b', 2, NULL)",
    )
    .execute(&env.pool)
    .await
    .expect("failed to insert orders");

    let mut orchestrator = env.make_orchestrator();
    let exit_code = orchestrator.run().await;

    // The NULL-cursor rows are a documented, warned-about limitation — NOT a failure.
    assert!(
        matches!(exit_code, ExitCode::Success),
        "expected Success (NULL-cursor loss is warned, not fatal), got {exit_code:?}"
    );

    // Only the 3 non-NULL-cursor rows synced; the 2 NULL-cursor rows are genuinely excluded.
    let row_count = count_delta_rows(&env, "orders").await;
    assert_eq!(
        row_count, 3,
        "expected only the 3 non-NULL-cursor rows in Delta (NULL-cursor rows excluded by D2)"
    );
    let null_named = count_matching(&env, "orders", "name = 'orphan_a' OR name = 'orphan_b'").await;
    assert_eq!(
        null_named, 0,
        "the NULL-cursor rows must be absent from Delta (documents the D2 limitation)"
    );
}

/// D3 STEP 1: validate the primitive itself. delta-rs 0.32.4 must be able to commit a
/// metadata-only (ZERO data-action) commit carrying the two-stream watermarks, and
/// `read_hwm`/`read_insert_hwm` must read them back. This is the mechanism
/// `process_two_stream` relies on to persist a first-run seed durably even when both
/// streams write nothing. If this fails, the HWM-only-commit approach is not viable on
/// this delta-rs version and the fix must fall back to a different persistence strategy.
#[tokio::test]
#[serial_test::serial]
async fn commit_hwm_only_round_trips_watermarks_on_fresh_table() {
    let _guard = tracing_subscriber::fmt()
        .with_env_filter("parket=debug")
        .with_test_writer()
        .try_init();

    let env = TestEnv::new(vec!["seedonly"]).await;

    let writer = DeltaWriter::new(
        &env.config.s3_bucket,
        &env.config.s3_prefix,
        env.config.s3_endpoint.as_deref(),
        &env.config.s3_region,
        &env.config.s3_access_key_id,
        &env.config.s3_secret_access_key,
    );

    let schema = std::sync::Arc::new(deltalake::arrow::datatypes::Schema::new(vec![
        deltalake::arrow::datatypes::Field::new("id", deltalake::arrow::datatypes::DataType::Int64, false),
        deltalake::arrow::datatypes::Field::new("updated_at", deltalake::arrow::datatypes::DataType::Utf8, false),
    ]));
    writer.ensure_table("seedonly", schema).await.expect("ensure_table failed");

    // Fresh empty table — no watermark of either kind yet.
    assert!(writer.read_hwm("seedonly").await.unwrap().is_none(), "fresh table: no update HWM");
    assert!(writer.read_insert_hwm("seedonly").await.unwrap().is_none(), "fresh table: no insert HWM");

    let seed = parket::writer::Hwm {
        updated_at: "2026-06-01T12:00:00.000000".to_string(),
        last_id: i64::MAX,
    };
    writer
        .commit_hwm_only("seedonly", Some(42), Some(&seed))
        .await
        .expect("commit_hwm_only must succeed on delta-rs 0.32.4 (zero-data-action commit)");

    // Both watermarks round-trip from the metadata-only commit.
    let insert_hwm = writer.read_insert_hwm("seedonly").await.unwrap();
    assert_eq!(insert_hwm, Some(42), "insert HWM must round-trip from the HWM-only commit");
    let update_hwm = writer
        .read_hwm("seedonly")
        .await
        .unwrap()
        .expect("update HWM must round-trip from the HWM-only commit");
    assert_eq!(update_hwm.updated_at, "2026-06-01T12:00:00.000000");
    assert_eq!(update_hwm.last_id, i64::MAX);

    // The commit carried no data actions — the table stays empty.
    assert_eq!(
        count_delta_rows(&env, "seedonly").await,
        0,
        "an HWM-only commit must not add any data rows"
    );
}

#[tokio::test]
#[serial_test::serial]
async fn crash_recovery_hwm_advances_and_only_new_rows_appended() {
    let _guard = tracing_subscriber::fmt()
        .with_env_filter("parket=debug")
        .with_test_writer()
        .try_init();

    let env = TestEnv::new(vec!["orders"]).await;

    sqlx::query(
        "CREATE TABLE orders (\
            id BIGINT AUTO_INCREMENT PRIMARY KEY, \
            name VARCHAR(255), \
            qty INT, \
            updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6)\
        )",
    )
    .execute(&env.pool)
    .await
    .expect("failed to create orders table");

    sqlx::query("CREATE INDEX idx_orders_updated_at ON orders (updated_at)")
        .execute(&env.pool)
        .await
        .expect("failed to create index");

    sqlx::query(
        "INSERT INTO orders (name, qty, updated_at) VALUES \
            ('widget', 10, '2026-01-01 10:00:00.000000'), \
            ('gadget', 5,  '2026-01-01 11:00:00.000000'), \
            ('doohickey', 3, '2026-01-02 09:00:00.000000')",
    )
    .execute(&env.pool)
    .await
    .expect("failed to insert initial orders");

    let mut orchestrator_run1 = env.make_orchestrator();
    let exit_code_run1 = orchestrator_run1.run().await;
    assert!(
        matches!(exit_code_run1, ExitCode::Success),
        "run 1: expected Success, got {exit_code_run1:?}"
    );

    let row_count_run1 = count_delta_rows(&env, "orders").await;
    assert_eq!(row_count_run1, 3, "run 1: expected 3 rows in Delta");

    let writer = DeltaWriter::new(
        &env.config.s3_bucket,
        &env.config.s3_prefix,
        env.config.s3_endpoint.as_deref(),
        &env.config.s3_region,
        &env.config.s3_access_key_id,
        &env.config.s3_secret_access_key,
    );
    let hwm_run1 = writer.read_hwm("orders").await.expect("run 1: read_hwm failed");
    assert!(hwm_run1.is_some(), "run 1: HWM should be set");
    let hwm1 = hwm_run1.unwrap();
    assert!(
        hwm1.updated_at.starts_with("2026-01-02"),
        "run 1: HWM updated_at should be 2026-01-02, got: {}",
        hwm1.updated_at,
    );
    assert_eq!(hwm1.last_id, 3, "run 1: HWM last_id should be 3");

    sqlx::query(
        "INSERT INTO orders (name, qty, updated_at) VALUES \
            ('thingamajig', 7, '2026-01-03 14:00:00.000000'), \
            ('whatchamacallit', 2, '2026-01-04 08:30:00.000000')",
    )
    .execute(&env.pool)
    .await
    .expect("failed to insert new orders");

    let mut orchestrator_run2 = env.make_orchestrator();
    let exit_code_run2 = orchestrator_run2.run().await;
    assert!(
        matches!(exit_code_run2, ExitCode::Success),
        "run 2: expected Success, got {exit_code_run2:?}"
    );

    let row_count_run2 = count_delta_rows(&env, "orders").await;
    assert_eq!(row_count_run2, 5, "run 2: expected 5 total rows (3 old + 2 new)");

    let hwm_run2 = writer.read_hwm("orders").await.expect("run 2: read_hwm failed");
    assert!(hwm_run2.is_some(), "run 2: HWM should be set");
    let hwm2 = hwm_run2.unwrap();
    assert!(
        hwm2.updated_at.starts_with("2026-01-04"),
        "run 2: HWM updated_at should be 2026-01-04, got: {}",
        hwm2.updated_at,
    );
    assert_eq!(hwm2.last_id, 5, "run 2: HWM last_id should be 5");
}

/// D3: a two-stream first run seeds the update watermark from MAX(completed_at). That seed
/// must be persisted durably so a completion arriving AFTER the run-1 seed (but before run 2)
/// is caught by the `completed_at > stored_seed` window instead of being skipped by a re-seed
/// to a newer MAX. Run 1 loads everything via the insert stream and derives the seed (the
/// HWM-only commit persists it before the streams run); run 2 then captures the late
/// completion. End-to-end correctness lock (the both-streams-write-nothing isolation is
/// covered by the `two_stream_seeds_update_hwm_when_none_stored` unit test + the STEP-1
/// round-trip test above).
#[tokio::test]
#[serial_test::serial]
async fn two_stream_first_run_seed_persists_so_later_completion_is_not_skipped() {
    let _guard = tracing_subscriber::fmt()
        .with_env_filter("parket=debug")
        .with_test_writer()
        .try_init();

    let mut env = TestEnv::new(vec!["tasks"]).await;
    env.config
        .table_insert_cursor
        .insert("tasks".to_string(), "id".to_string());
    env.config
        .table_update_cursor
        .insert("tasks".to_string(), "completed_at".to_string());

    sqlx::query(
        "CREATE TABLE tasks (\
            id BIGINT PRIMARY KEY, \
            name VARCHAR(255), \
            completed_at DATETIME(6) NULL\
        )",
    )
    .execute(&env.pool)
    .await
    .expect("failed to create tasks table");

    // Run 1: 3 already-completed rows; MAX(completed_at) = 2026-01-03 becomes the update seed and
    // the update stream finds nothing beyond it. The insert stream loads all 3.
    sqlx::query(
        "INSERT INTO tasks (id, name, completed_at) VALUES \
            (1, 'a', '2026-01-01 10:00:00.000000'), \
            (2, 'b', '2026-01-02 10:00:00.000000'), \
            (3, 'c', '2026-01-03 10:00:00.000000')",
    )
    .execute(&env.pool)
    .await
    .expect("failed to insert run-1 rows");

    let mut run1 = env.make_orchestrator();
    assert!(matches!(run1.run().await, ExitCode::Success), "run 1 must succeed");
    assert_eq!(count_delta_rows(&env, "tasks").await, 3, "run 1: insert stream loads all 3 rows");

    let writer = DeltaWriter::new(
        &env.config.s3_bucket,
        &env.config.s3_prefix,
        env.config.s3_endpoint.as_deref(),
        &env.config.s3_region,
        &env.config.s3_access_key_id,
        &env.config.s3_secret_access_key,
    );
    // D3: the run-1 seed is persisted — even though the update stream wrote nothing.
    let hwm1 = writer
        .read_hwm("tasks")
        .await
        .expect("run 1: read_hwm failed")
        .expect("run 1: the first-run update seed must be persisted");
    assert!(
        hwm1.updated_at.starts_with("2026-01-03"),
        "run 1: persisted seed should be MAX(completed_at)=2026-01-03, got {}",
        hwm1.updated_at
    );

    // A completion arrives after run 1's seed but before run 2: row 2 completes at 2026-01-04.
    sqlx::query(
        "UPDATE tasks SET name = 'b-done', completed_at = '2026-01-04 10:00:00.000000' WHERE id = 2",
    )
    .execute(&env.pool)
    .await
    .expect("failed to apply completion");

    let mut run2 = env.make_orchestrator();
    assert!(matches!(run2.run().await, ExitCode::Success), "run 2 must succeed");

    // The completion is captured (not skipped) and not duplicated: still 3 distinct rows, and
    // row 2 now reflects the mutation.
    assert_eq!(count_delta_rows(&env, "tasks").await, 3, "run 2: no duplication");
    assert_eq!(
        count_matching(&env, "tasks", "id = 2 AND name = 'b-done'").await,
        1,
        "run 2 must capture the late completion on row 2 (D3: seed persisted, window not skipped)"
    );
    // And the update watermark advanced to the completion's timestamp.
    let hwm2 = writer
        .read_hwm("tasks")
        .await
        .expect("run 2: read_hwm failed")
        .expect("run 2: HWM should be set");
    assert!(
        hwm2.updated_at.starts_with("2026-01-04"),
        "run 2: HWM should advance to the completion timestamp, got {}",
        hwm2.updated_at
    );
}

/// Two-stream (insert + update MERGE) end-to-end across two runs:
/// - insert stream appends new rows by PK `id`;
/// - update stream MERGEs mutations by the `completed_at` cursor;
/// - bootstrap seeds the update watermark so run 1 merges nothing redundant;
/// - run 2 captures: a mutated existing row, a NULL->set transition, new inserts, and
///   crucially does NOT duplicate a row caught by BOTH streams.
async fn run_two_stream_upsert_scenario(update_strategy: Option<&str>) {
    if let Some(s) = update_strategy {
        unsafe { std::env::set_var("UPDATE_STRATEGY", s); }
    }
    let _guard = tracing_subscriber::fmt()
        .with_env_filter("parket=debug")
        .with_test_writer()
        .try_init();

    let mut env = TestEnv::new(vec!["orders"]).await;
    // Enable two-stream for `orders`: insert cursor = id (PK), update cursor = completed_at.
    env.config
        .table_insert_cursor
        .insert("orders".to_string(), "id".to_string());
    env.config
        .table_update_cursor
        .insert("orders".to_string(), "completed_at".to_string());

    sqlx::query(
        "CREATE TABLE orders (\
            id BIGINT PRIMARY KEY, \
            name VARCHAR(255), \
            qty INT, \
            completed_at DATETIME(6) NULL\
        )",
    )
    .execute(&env.pool)
    .await
    .expect("failed to create orders table");

    sqlx::query("CREATE INDEX idx_orders_completed_at ON orders (completed_at, id)")
        .execute(&env.pool)
        .await
        .expect("failed to create index");

    // Run 1 seed: row 2 is not yet completed (completed_at NULL).
    sqlx::query(
        "INSERT INTO orders (id, name, qty, completed_at) VALUES \
            (1, 'widget', 10, '2026-01-01 10:00:00.000000'), \
            (2, 'gadget', 5, NULL), \
            (3, 'doohickey', 3, '2026-01-02 09:00:00.000000')",
    )
    .execute(&env.pool)
    .await
    .expect("failed to insert run-1 rows");

    let mut run1 = env.make_orchestrator();
    let exit1 = run1.run().await;
    assert!(matches!(exit1, ExitCode::Success), "run 1: expected Success, got {exit1:?}");

    // Insert stream appended all 3 rows in their current state (incl. the NULL-completed row 2).
    assert_eq!(count_delta_rows(&env, "orders").await, 3, "run 1: expected 3 rows");

    let writer = DeltaWriter::new(
        &env.config.s3_bucket,
        &env.config.s3_prefix,
        env.config.s3_endpoint.as_deref(),
        &env.config.s3_region,
        &env.config.s3_access_key_id,
        &env.config.s3_secret_access_key,
    );
    assert_eq!(
        writer.read_insert_hwm("orders").await.unwrap(),
        Some(3),
        "run 1: insert HWM should be max id = 3"
    );

    // --- between runs: mutate existing rows + insert new ones ---
    // Existing row 1 mutated (qty + completed_at advance) — must be captured by the update MERGE:
    sqlx::query(
        "UPDATE orders SET qty = 99, completed_at = '2026-01-06 09:00:00.000000' WHERE id = 1",
    )
    .execute(&env.pool)
    .await
    .expect("failed to mutate row 1");
    // Row 2 transitions NULL -> set (Feature E x two-stream): must now be captured:
    sqlx::query(
        "UPDATE orders SET completed_at = '2026-01-05 12:00:00.000000' WHERE id = 2",
    )
    .execute(&env.pool)
    .await
    .expect("failed to complete row 2");
    // New rows by id > 3. Row 5 is ALSO completed after the seed, so it is caught by BOTH the
    // insert stream (id > 3) AND the update stream (completed_at > seed) — must NOT duplicate.
    sqlx::query(
        "INSERT INTO orders (id, name, qty, completed_at) VALUES \
            (4, 'thingamajig', 7, NULL), \
            (5, 'gizmo', 8, '2026-01-03 14:00:00.000000')",
    )
    .execute(&env.pool)
    .await
    .expect("failed to insert run-2 rows");

    let mut run2 = env.make_orchestrator();
    let exit2 = run2.run().await;
    assert!(matches!(exit2, ExitCode::Success), "run 2: expected Success, got {exit2:?}");

    // CRITICAL: exactly 5 distinct rows — no duplicates despite row 5 being in both streams.
    assert_eq!(
        count_delta_rows(&env, "orders").await,
        5,
        "run 2: expected 5 distinct rows (no duplicates from the two streams)"
    );
    // Mutation captured by the update MERGE (qty 10 -> 99):
    assert_eq!(
        count_matching(&env, "orders", "id = 1 AND qty = 99").await,
        1,
        "run 2: row 1 mutation should be merged (qty=99)"
    );
    // NULL -> set transition captured:
    assert_eq!(
        count_matching(&env, "orders", "id = 2 AND completed_at IS NOT NULL").await,
        1,
        "run 2: row 2 completion should be merged (completed_at now set)"
    );
    // Insert watermark advanced past the new rows:
    assert_eq!(
        writer.read_insert_hwm("orders").await.unwrap(),
        Some(5),
        "run 2: insert HWM should advance to 5"
    );

    if update_strategy.is_some() {
        unsafe { std::env::remove_var("UPDATE_STRATEGY"); }
    }
}

#[tokio::test]
#[serial_test::serial]
async fn two_stream_inserts_and_delete_append_updates_across_runs() {
    // default strategy = delete_then_append
    run_two_stream_upsert_scenario(None).await;
}

#[tokio::test]
#[serial_test::serial]
async fn two_stream_inserts_and_merges_mutations_across_runs() {
    // opt-out: the legacy MERGE path
    run_two_stream_upsert_scenario(Some("merge")).await;
}

#[tokio::test]
#[serial_test::serial]
async fn schema_evolution_add_column_warns_and_skips() {
    let _guard = tracing_subscriber::fmt()
        .with_env_filter("parket=debug")
        .with_test_writer()
        .try_init();

    let env = TestEnv::new(vec!["orders"]).await;

    sqlx::query(
        "CREATE TABLE orders (\
            id BIGINT AUTO_INCREMENT PRIMARY KEY, \
            name VARCHAR(255), \
            qty INT, \
            updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6)\
        )",
    )
    .execute(&env.pool)
    .await
    .expect("failed to create orders table");

    sqlx::query("CREATE INDEX idx_orders_updated_at ON orders (updated_at)")
        .execute(&env.pool)
        .await
        .expect("failed to create index");

    sqlx::query(
        "INSERT INTO orders (name, qty, updated_at) VALUES \
            ('widget', 10, '2026-01-01 10:00:00.000000'), \
            ('gadget', 5,  '2026-01-01 11:00:00.000000'), \
            ('doohickey', 3, '2026-01-02 09:00:00.000000')",
    )
    .execute(&env.pool)
    .await
    .expect("failed to insert orders");

    let mut orchestrator_run1 = env.make_orchestrator();
    let exit_code_run1 = orchestrator_run1.run().await;
    assert!(
        matches!(exit_code_run1, ExitCode::Success),
        "run 1: expected Success, got {exit_code_run1:?}"
    );

    let row_count_run1 = count_delta_rows(&env, "orders").await;
    assert_eq!(row_count_run1, 3, "run 1: expected 3 rows in Delta");

    let mut table = env.open_delta_table("orders").await;
    table.load().await.expect("failed to load delta table");
    let kernel_schema = table.snapshot().unwrap().schema();
    let arrow_schema: deltalake::arrow::datatypes::Schema =
        deltalake::kernel::engine::arrow_conversion::TryIntoArrow::try_into_arrow(
            kernel_schema.as_ref(),
        )
        .expect("failed to convert schema");
    let field_names_run1: Vec<&str> = arrow_schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert!(
        !field_names_run1.contains(&"color"),
        "run 1: Delta schema should not contain 'color' yet"
    );

    sqlx::query("ALTER TABLE orders ADD COLUMN color VARCHAR(50) AFTER qty")
        .execute(&env.pool)
        .await
        .expect("failed to alter table");

    sqlx::query(
        "INSERT INTO orders (name, qty, color, updated_at) VALUES \
            ('thingamajig', 7, 'red', '2026-01-03 14:00:00.000000'), \
            ('whatchamacallit', 2, 'blue', '2026-01-04 08:30:00.000000')",
    )
    .execute(&env.pool)
    .await
    .expect("failed to insert new orders with color");

    let mut orchestrator_run2 = env.make_orchestrator();
    let exit_code_run2 = orchestrator_run2.run().await;
    assert!(
        matches!(exit_code_run2, ExitCode::Success),
        "run 2: expected Success (warn+skip), got {exit_code_run2:?}"
    );

    let row_count_run2 = count_delta_rows(&env, "orders").await;
    assert_eq!(row_count_run2, 5, "run 2: expected 5 total rows (3 old + 2 new)");

    let writer = DeltaWriter::new(
        &env.config.s3_bucket,
        &env.config.s3_prefix,
        env.config.s3_endpoint.as_deref(),
        &env.config.s3_region,
        &env.config.s3_access_key_id,
        &env.config.s3_secret_access_key,
    );
    let hwm = writer.read_hwm("orders").await.expect("run 2: read_hwm failed");
    assert!(hwm.is_some(), "run 2: HWM should be set");
    let hwm = hwm.unwrap();
    assert!(
        hwm.updated_at.starts_with("2026-01-04"),
        "run 2: HWM updated_at should be 2026-01-04, got: {}",
        hwm.updated_at,
    );
    assert_eq!(hwm.last_id, 5, "run 2: HWM last_id should be 5");

    let mut table = env.open_delta_table("orders").await;
    table.load().await.expect("failed to load delta table after run 2");
    let kernel_schema = table.snapshot().unwrap().schema();
    let arrow_schema: deltalake::arrow::datatypes::Schema =
        deltalake::kernel::engine::arrow_conversion::TryIntoArrow::try_into_arrow(
            kernel_schema.as_ref(),
        )
        .expect("failed to convert schema");
    let field_names_run2: Vec<&str> = arrow_schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert!(
        !field_names_run2.contains(&"color"),
        "run 2: Delta schema should still not contain 'color' (warn+skip)"
    );
}

#[tokio::test]
#[serial_test::serial]
async fn verify_value_aggregates_real_basic_match_across_type_families() {
    let _guard = tracing_subscriber::fmt()
        .with_env_filter("parket=debug")
        .with_test_writer()
        .try_init();

    let env = TestEnv::new(vec!["audit_values"]).await;

    sqlx::query(
        "CREATE TABLE audit_values (            id BIGINT PRIMARY KEY,             qty INT NOT NULL,             amount DECIMAL(18,2) NOT NULL,             ratio DECIMAL(20,12) NOT NULL,             happened_at DATETIME(6) NOT NULL,             due_date DATE,             note VARCHAR(255) CHARACTER SET utf8mb4        )",
    )
    .execute(&env.pool)
    .await
    .expect("failed to create audit_values table");

    sqlx::query(
        "INSERT INTO audit_values (id, qty, amount, ratio, happened_at, due_date, note) VALUES             (1, 10, 12.34, 0.123456789012, '2026-01-01 10:00:00.123456', '2026-01-05', 'alpha'),             (2, 25, 99.99, 3.000000000005, '2026-01-02 11:30:15.654321', '2026-01-06', 'bravo'),             (3, 7,  5.50,  0.999999999999, '2026-01-03 09:45:59.000001', '2026-01-07', 'charlie'),             (4, 3,  1.00,  0.000000000001, '2026-01-04 00:00:00.000000', NULL, NULL),             (5, 42, 8.88,  1.111111111111, '2026-01-05 06:06:06.000000', '2026-01-08', 'héllo 世界')",
    )
    .execute(&env.pool)
    .await
    .expect("failed to insert audit_values rows");

    let mut orchestrator = env.make_orchestrator();
    let exit_code = orchestrator.run().await;
    assert!(
        matches!(exit_code, ExitCode::Success),
        "expected Success exit code, got {exit_code:?}"
    );

    let source = SourceProbeAdapter::new(env.pool.clone());
    let delta = DeltaProbeAdapter::new(DeltaWriter::new(
        &env.config.s3_bucket,
        &env.config.s3_prefix,
        env.config.s3_endpoint.as_deref(),
        &env.config.s3_region,
        &env.config.s3_access_key_id,
        &env.config.s3_secret_access_key,
    ));
    let verdict = VerifyCommand::new(source, delta, vec!["audit_values".to_string()])
        .with_table_plans(vec![TablePlan {
            table: "audit_values".to_string(),
            mode: VerifyMode::Basic,
        }])
        .with_deep(true)
        .run()
        .await
        .expect("verify basic value aggregates should succeed");

    assert_eq!(verdict, VerifyVerdict::Clean);

    // T2: corrupt the source directly, without re-running the pipeline, and prove verify
    // detects the drift instead of always reporting Clean.
    sqlx::query(
        "UPDATE audit_values SET amount = amount + 0.01, ratio = ratio + 0.000000000001, note = CONCAT(note, 'x') WHERE id = 2",
    )
    .execute(&env.pool)
    .await
    .expect("failed to corrupt audit_values row directly");

    let source = SourceProbeAdapter::new(env.pool.clone());
    let delta = DeltaProbeAdapter::new(DeltaWriter::new(
        &env.config.s3_bucket,
        &env.config.s3_prefix,
        env.config.s3_endpoint.as_deref(),
        &env.config.s3_region,
        &env.config.s3_access_key_id,
        &env.config.s3_secret_access_key,
    ));
    let verdict_after_corruption =
        VerifyCommand::new(source, delta, vec!["audit_values".to_string()])
            .with_table_plans(vec![TablePlan {
                table: "audit_values".to_string(),
                mode: VerifyMode::Basic,
            }])
            .with_deep(true)
            .run()
            .await
            .expect("verify basic value aggregates should succeed after corruption");

    assert_eq!(verdict_after_corruption, VerifyVerdict::Discrepancy);
}

#[tokio::test]
#[serial_test::serial]
async fn verify_value_aggregates_real_incremental_hwm_scope_matches_latest_rows() {
    let _guard = tracing_subscriber::fmt()
        .with_env_filter("parket=debug")
        .with_test_writer()
        .try_init();

    let env = TestEnv::new(vec!["orders"]).await;

    sqlx::query(
        "CREATE TABLE orders (            id BIGINT AUTO_INCREMENT PRIMARY KEY,             qty INT NOT NULL,             amount DECIMAL(18,2) NOT NULL,             happened_at DATETIME(6) NOT NULL,             due_date DATE NOT NULL,             note VARCHAR(255) NOT NULL,             updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6)        )",
    )
    .execute(&env.pool)
    .await
    .expect("failed to create orders table for verify");

    sqlx::query("CREATE INDEX idx_orders_updated_at ON orders (updated_at)")
        .execute(&env.pool)
        .await
        .expect("failed to create orders updated_at index");

    sqlx::query(
        "INSERT INTO orders (qty, amount, happened_at, due_date, note, updated_at) VALUES             (10, 12.34, '2026-01-01 10:00:00.123456', '2026-01-05', 'alpha', '2026-01-01 10:00:00.123456'),             (20, 20.50, '2026-01-02 11:30:15.654321', '2026-01-06', 'bravo', '2026-01-02 11:30:15.654321')",
    )
    .execute(&env.pool)
    .await
    .expect("failed to insert initial orders rows");

    let mut run1 = env.make_orchestrator();
    let exit1 = run1.run().await;
    assert!(matches!(exit1, ExitCode::Success), "run 1 expected Success, got {exit1:?}");

    sqlx::query(
        "UPDATE orders             SET qty = 11,                 amount = 13.44,                 happened_at = '2026-01-03 12:15:30.222222',                 due_date = '2026-01-08',                 note = 'alpha-2',                 updated_at = '2026-01-03 12:15:30.222222'           WHERE id = 1",
    )
    .execute(&env.pool)
    .await
    .expect("failed to update existing order row");

    sqlx::query(
        "INSERT INTO orders (qty, amount, happened_at, due_date, note, updated_at) VALUES             (30, 31.75, '2026-01-04 08:45:00.999999', '2026-01-09', 'charlie', '2026-01-04 08:45:00.999999')",
    )
    .execute(&env.pool)
    .await
    .expect("failed to insert new incremental order row");

    let mut run2 = env.make_orchestrator();
    let exit2 = run2.run().await;
    assert!(matches!(exit2, ExitCode::Success), "run 2 expected Success, got {exit2:?}");

    let writer = DeltaWriter::new(
        &env.config.s3_bucket,
        &env.config.s3_prefix,
        env.config.s3_endpoint.as_deref(),
        &env.config.s3_region,
        &env.config.s3_access_key_id,
        &env.config.s3_secret_access_key,
    );
    let hwm = writer
        .read_hwm("orders")
        .await
        .expect("read_hwm failed for verify orders")
        .expect("incremental verify orders should have HWM");

    let source = SourceProbeAdapter::new(env.pool.clone());
    let delta = DeltaProbeAdapter::new(DeltaWriter::new(
        &env.config.s3_bucket,
        &env.config.s3_prefix,
        env.config.s3_endpoint.as_deref(),
        &env.config.s3_region,
        &env.config.s3_access_key_id,
        &env.config.s3_secret_access_key,
    ));
    let verdict = VerifyCommand::new(source, delta, vec!["orders".to_string()])
        .with_table_plans(vec![TablePlan {
            table: "orders".to_string(),
            mode: VerifyMode::Incremental {
                cursor_col: "updated_at".to_string(),
                hwm: Some(hwm.clone()),
            },
        }])
        .with_deep(true)
        .run()
        .await
        .expect("verify incremental value aggregates should succeed");

    assert_eq!(verdict, VerifyVerdict::Clean);

    // T3: insert a new source row with updated_at strictly AFTER the stored HWM, without
    // re-running the pipeline. The scope predicate must exclude this row entirely — verify
    // should still report Clean, proving the HWM scope is actually enforced.
    sqlx::query(
        "INSERT INTO orders (qty, amount, happened_at, due_date, note, updated_at) VALUES             (99, 50.00, '2026-01-05 00:00:00.000000', '2026-01-10', 'post-hwm', '2026-01-05 00:00:00.000000')",
    )
    .execute(&env.pool)
    .await
    .expect("failed to insert post-HWM order row");

    let source = SourceProbeAdapter::new(env.pool.clone());
    let delta = DeltaProbeAdapter::new(DeltaWriter::new(
        &env.config.s3_bucket,
        &env.config.s3_prefix,
        env.config.s3_endpoint.as_deref(),
        &env.config.s3_region,
        &env.config.s3_access_key_id,
        &env.config.s3_secret_access_key,
    ));
    let verdict_after_post_hwm_insert =
        VerifyCommand::new(source, delta, vec!["orders".to_string()])
            .with_table_plans(vec![TablePlan {
                table: "orders".to_string(),
                mode: VerifyMode::Incremental {
                    cursor_col: "updated_at".to_string(),
                    hwm: Some(hwm.clone()),
                },
            }])
            .with_deep(true)
            .run()
            .await
            .expect("verify incremental value aggregates should succeed after post-HWM insert");

    assert_eq!(verdict_after_post_hwm_insert, VerifyVerdict::Clean);

    // T2: corrupt a row INSIDE the HWM window directly, without re-running the pipeline, and
    // prove verify detects the drift. Re-assigning `updated_at` to its own current value keeps
    // the automatic ON UPDATE CURRENT_TIMESTAMP(6) from bumping it past the HWM, so the row
    // stays inside the already-scoped window.
    sqlx::query(
        "UPDATE orders             SET qty = qty + 1,                 amount = amount + 0.01,                 note = CONCAT(note, 'x'),                 updated_at = updated_at           WHERE id = 1",
    )
    .execute(&env.pool)
    .await
    .expect("failed to corrupt existing order row directly");

    let source = SourceProbeAdapter::new(env.pool.clone());
    let delta = DeltaProbeAdapter::new(DeltaWriter::new(
        &env.config.s3_bucket,
        &env.config.s3_prefix,
        env.config.s3_endpoint.as_deref(),
        &env.config.s3_region,
        &env.config.s3_access_key_id,
        &env.config.s3_secret_access_key,
    ));
    let verdict_after_corruption = VerifyCommand::new(source, delta, vec!["orders".to_string()])
        .with_table_plans(vec![TablePlan {
            table: "orders".to_string(),
            mode: VerifyMode::Incremental {
                cursor_col: "updated_at".to_string(),
                hwm: Some(hwm),
            },
        }])
        .with_deep(true)
        .run()
        .await
        .expect("verify incremental value aggregates should succeed after corruption");

    assert_eq!(verdict_after_corruption, VerifyVerdict::Discrepancy);
}

// N5: unsigned MariaDB integer columns. `mariadb_type_to_arrow` widens each unsigned
// width to the narrowest SIGNED Arrow/Delta type that holds its full range (Delta has no
// unsigned type), and `align_batch_to_schema` casts the UInt* batches connector_arrow
// emits to match, erroring (not corrupting) on any value that doesn't fit. These two
// Docker tests lock that in against real MariaDB + MinIO + delta-rs 0.32. No `updated_at`
// column → auto FullRefresh; `id BIGINT PRIMARY KEY` gives the keyset path a PK to page on.
//
// PROBE OBSERVATION (pre-fix, recorded for the record): writing the UInt* batches against
// the old SIGNED-narrower Delta schema, delta-rs 0.32 errored at write time; with the fix,
// in-range unsigned values round-trip exactly and a BIGINT UNSIGNED value > i64::MAX fails
// the table by name (see the two tests below).
#[tokio::test]
#[serial_test::serial]
async fn unsigned_columns_round_trip() {
    let _guard = tracing_subscriber::fmt()
        .with_env_filter("parket=debug")
        .with_test_writer()
        .try_init();

    let env = TestEnv::new(vec!["unsigned_probe"]).await;

    sqlx::query(
        "CREATE TABLE unsigned_probe (\
            id BIGINT PRIMARY KEY, \
            a TINYINT UNSIGNED NOT NULL, \
            b SMALLINT UNSIGNED NOT NULL, \
            c INT UNSIGNED NOT NULL, \
            d BIGINT UNSIGNED NOT NULL, \
            e MEDIUMINT UNSIGNED NOT NULL\
        )",
    )
    .execute(&env.pool)
    .await
    .expect("failed to create unsigned_probe table");

    // Row 1 small; row 2 pushes every column past its SIGNED counterpart's max
    // (a>i8::MAX 127, b>i16::MAX 32767, c>i32::MAX, e>mediumint-signed 8388607) — and d
    // large but still <= i64::MAX so BIGINT UNSIGNED round-trips within its supported range.
    sqlx::query(
        "INSERT INTO unsigned_probe (id, a, b, c, d, e) VALUES \
            (1, 1, 2, 3, 12345, 4), \
            (2, 200, 40000, 3000000000, 9000000000000000000, 10000000)",
    )
    .execute(&env.pool)
    .await
    .expect("failed to insert unsigned_probe rows");

    let mut orchestrator = env.make_orchestrator();
    let exit_code = orchestrator.run().await;
    assert!(
        matches!(exit_code, ExitCode::Success),
        "in-range unsigned columns must sync cleanly, got {exit_code:?}"
    );

    let mut table = env.open_delta_table("unsigned_probe").await;
    table.load().await.expect("failed to load delta table");

    // Delta stores Int8/16/32 (and their unsigned sources) all as INTEGER→Int32, and
    // Int64/UInt64 as LONG→Int64 (see writer::schema::arrow_type_to_delta). So the widened
    // columns read back as Int32/Int64 — the point is that no value was truncated to a type
    // too narrow to hold it (i8/i16/i32 for the above-max values), which the value checks below
    // confirm. `types_equivalent` (orchestrator::schema) treats these as matching the expected
    // signed types so a second run's schema-evolution check does NOT false-flag (asserted below).
    let kernel_schema = table.snapshot().unwrap().schema();
    let arrow_schema: deltalake::arrow::datatypes::Schema =
        deltalake::kernel::engine::arrow_conversion::TryIntoArrow::try_into_arrow(
            kernel_schema.as_ref(),
        )
        .expect("failed to convert schema");
    use deltalake::arrow::datatypes::DataType;
    let dt = |n: &str| arrow_schema.field_with_name(n).unwrap().data_type().clone();
    assert_eq!(dt("a"), DataType::Int32, "tinyint unsigned widened to Delta INTEGER");
    assert_eq!(dt("b"), DataType::Int32, "smallint unsigned widened to Delta INTEGER");
    assert_eq!(dt("c"), DataType::Int64, "int unsigned needs 64 bits (> i32::MAX)");
    assert_eq!(dt("d"), DataType::Int64, "bigint unsigned -> Delta LONG");
    assert_eq!(dt("e"), DataType::Int32, "mediumint unsigned -> Delta INTEGER");

    // Every above-signed-max value must survive exactly (read back as bigint via datafusion).
    let expected = vec![2i64, 200, 40000, 3_000_000_000, 9_000_000_000_000_000_000, 10_000_000];
    assert_eq!(
        read_unsigned_probe_maxima(&env).await,
        expected,
        "count + per-column maxima (a,b,c,d,e) must round-trip exactly"
    );

    // Second run: proves a full-refresh overwrite of the existing (already-widened,
    // INTEGER/LONG-typed) Delta table with freshly-extracted UInt* batches stays clean and
    // identical — i.e. align + overwrite are idempotent across runs. (FullRefresh mode does
    // not invoke schema_evolution_check; types_equivalent's Int32/Int64 acceptance for the
    // expected signed widths, which matters for the incremental/two-stream paths, is covered
    // by the orchestrator::schema lib unit tests.)
    let mut orchestrator2 = env.make_orchestrator();
    let exit_code2 = orchestrator2.run().await;
    assert!(
        matches!(exit_code2, ExitCode::Success),
        "second run over unsigned columns must stay clean (no schema-evolution false flag), got {exit_code2:?}"
    );
    assert_eq!(
        read_unsigned_probe_maxima(&env).await,
        expected,
        "values must be identical after the second full-refresh run"
    );
}

// count(*) + per-column max(cast(col as bigint)) over the unsigned_probe Delta table, in
// column order (count, a, b, c, d, e). Factored out so both runs assert the same values.
async fn read_unsigned_probe_maxima(env: &TestEnv) -> Vec<i64> {
    let mut t = env.open_delta_table("unsigned_probe").await;
    t.load().await.unwrap();
    let ctx = deltalake::datafusion::prelude::SessionContext::new();
    ctx.register_table("up", t.table_provider().await.unwrap())
        .unwrap();
    let b = ctx
        .sql(
            "SELECT count(*), max(cast(a as bigint)), max(cast(b as bigint)), \
             max(cast(c as bigint)), max(cast(d as bigint)), max(cast(e as bigint)) FROM up",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let r = &b[0];
    (0..6)
        .map(|i| {
            r.column(i)
                .as_any()
                .downcast_ref::<deltalake::arrow::array::Int64Array>()
                .unwrap()
                .value(0)
        })
        .collect()
}

// A BIGINT UNSIGNED value above i64::MAX has no signed 64-bit representation; the cast must
// fail the table loudly (by column name) rather than wrap negative or corrupt the write.
#[tokio::test]
#[serial_test::serial]
async fn bigint_unsigned_beyond_i64_fails_actionably() {
    let _guard = tracing_subscriber::fmt()
        .with_env_filter("parket=debug")
        .with_test_writer()
        .try_init();

    let env = TestEnv::new(vec!["unsigned_overflow"]).await;

    sqlx::query("CREATE TABLE unsigned_overflow (id BIGINT PRIMARY KEY, d BIGINT UNSIGNED NOT NULL)")
        .execute(&env.pool)
        .await
        .expect("failed to create unsigned_overflow table");

    // 9300000000000000000 > i64::MAX (9223372036854775807).
    sqlx::query("INSERT INTO unsigned_overflow (id, d) VALUES (1, 9300000000000000000)")
        .execute(&env.pool)
        .await
        .expect("failed to insert overflow row");

    let mut orchestrator = env.make_orchestrator();
    let exit_code = orchestrator.run().await;

    // Sole table, extraction/cast fails before any write → all-failed → Fatal. The
    // per-table error (logged above at ERROR) names column `d` and the i64::MAX ceiling.
    assert!(
        matches!(exit_code, ExitCode::Fatal),
        "a BIGINT UNSIGNED value above i64::MAX must fail the table (Fatal), got {exit_code:?}"
    );

    // ensure_table creates the (empty) Delta table before extraction, but the cast fails
    // before any batch is written — so the table exists yet holds ZERO rows: no negative-
    // wrapped / corrupt data ever lands.
    let mut table = env.open_delta_table("unsigned_overflow").await;
    table.load().await.expect("failed to load delta table");
    let ctx = deltalake::datafusion::prelude::SessionContext::new();
    ctx.register_table("t", table.table_provider().await.unwrap())
        .unwrap();
    let b = ctx
        .sql("SELECT count(*) FROM t")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let n = b[0]
        .column(0)
        .as_any()
        .downcast_ref::<deltalake::arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(n, 0, "the failed table must contain no partial/corrupt rows");
}

#[tokio::test]
#[serial_test::serial]
async fn verify_key_set_works_with_non_id_primary_key() {
    // V3: a table keyed by something other than a literal `id` column must still get a
    // real key-set verdict from `--verify`, not a silent Skipped-as-Clean. `code_id` is
    // the table's single-column integer PRIMARY key; there is no `id` column at all.
    let _guard = tracing_subscriber::fmt()
        .with_env_filter("parket=debug")
        .with_test_writer()
        .try_init();

    let env = TestEnv::new(vec!["keyed_by_code"]).await;

    sqlx::query(
        "CREATE TABLE keyed_by_code (            code_id BIGINT PRIMARY KEY,             amount INT NOT NULL        )",
    )
    .execute(&env.pool)
    .await
    .expect("failed to create keyed_by_code table");

    sqlx::query(
        "INSERT INTO keyed_by_code (code_id, amount) VALUES (100, 10), (200, 20), (300, 30)",
    )
    .execute(&env.pool)
    .await
    .expect("failed to insert keyed_by_code rows");

    // No TABLE_MODE configured for this table and no timestamp column present ⇒ auto-detect
    // resolves full_refresh.
    let mut orchestrator = env.make_orchestrator();
    let exit_code = orchestrator.run().await;
    assert!(
        matches!(exit_code, ExitCode::Success),
        "expected Success exit code, got {exit_code:?}"
    );

    let source = SourceProbeAdapter::new(env.pool.clone());
    let delta = DeltaProbeAdapter::new(DeltaWriter::new(
        &env.config.s3_bucket,
        &env.config.s3_prefix,
        env.config.s3_endpoint.as_deref(),
        &env.config.s3_region,
        &env.config.s3_access_key_id,
        &env.config.s3_secret_access_key,
    ));
    let verdict = VerifyCommand::new(source, delta, vec!["keyed_by_code".to_string()])
        .with_table_plans(vec![TablePlan {
            table: "keyed_by_code".to_string(),
            mode: VerifyMode::Basic,
        }])
        .with_deep(true)
        .run()
        .await
        .expect("verify should succeed against a non-id-keyed table");

    assert_eq!(
        verdict,
        VerifyVerdict::Clean,
        "a genuinely-synced non-id-keyed table must verify Clean, not Skipped-as-Clean by \
         accident — the assertion that matters is the corruption check below actually \
         flipping this to Discrepancy"
    );

    // Corrupt one source value directly, without re-running the pipeline, so the ONLY way
    // verify can catch it is by actually running the key-set/value machinery against
    // `code_id` — proving the fix, not just that Skipped happens to report Clean.
    sqlx::query("UPDATE keyed_by_code SET amount = amount + 1 WHERE code_id = 200")
        .execute(&env.pool)
        .await
        .expect("failed to corrupt keyed_by_code row directly");

    let source = SourceProbeAdapter::new(env.pool.clone());
    let delta = DeltaProbeAdapter::new(DeltaWriter::new(
        &env.config.s3_bucket,
        &env.config.s3_prefix,
        env.config.s3_endpoint.as_deref(),
        &env.config.s3_region,
        &env.config.s3_access_key_id,
        &env.config.s3_secret_access_key,
    ));
    let verdict_after_corruption =
        VerifyCommand::new(source, delta, vec!["keyed_by_code".to_string()])
            .with_table_plans(vec![TablePlan {
                table: "keyed_by_code".to_string(),
                mode: VerifyMode::Basic,
            }])
            .with_deep(true)
            .run()
            .await
            .expect("verify should succeed after corruption");

    assert_eq!(
        verdict_after_corruption,
        VerifyVerdict::Discrepancy,
        "post-corruption verify must catch the drift via the non-id key, proving the \
         machinery genuinely ran against `code_id` rather than being Skipped"
    );
}

#[tokio::test]
#[serial_test::serial]
async fn table_with_time_year_bit_columns_syncs_with_columns_skipped() {
    // N1/O8: TIME/YEAR/BIT are not in discovery::EXTRACTABLE_DATA_TYPES. Pre-fix, `t`
    // (TIME) and `y` (YEAR) would reach the vendored connector_arrow's `create_field`
    // unmapped and ABORT THE WHOLE PROCESS (exit 101, not this crate's 0/1/2 contract);
    // `b` (BIT) would fail via `mariadb_type_to_arrow`'s bail (whole-table failure, not a
    // process abort, but still worse than the geometry precedent). Post-fix, all three
    // are silently skipped with a warn — the table still syncs successfully with just
    // `id` + `name`, proving the panic/whole-table-failure path is sealed end-to-end
    // against a real MariaDB server, not just in unit tests that construct ColumnInfo by
    // hand.
    let _guard = tracing_subscriber::fmt()
        .with_env_filter("parket=debug")
        .with_test_writer()
        .try_init();

    let env = TestEnv::new(vec!["skippy"]).await;

    sqlx::query(
        "CREATE TABLE skippy (\
            id BIGINT PRIMARY KEY, \
            name VARCHAR(50), \
            t TIME, \
            y YEAR, \
            b BIT(8)\
        )",
    )
    .execute(&env.pool)
    .await
    .expect("failed to create skippy table");

    sqlx::query(
        "INSERT INTO skippy (id, name, t, y, b) VALUES \
            (1, 'alice', '12:34:56', 2026, b'00000001'), \
            (2, 'bob', '23:45:01', 2025, b'00000010')",
    )
    .execute(&env.pool)
    .await
    .expect("failed to insert skippy rows");

    let mut orchestrator = env.make_orchestrator();
    let exit_code = orchestrator.run().await;
    assert!(
        matches!(exit_code, ExitCode::Success),
        "expected Success exit code (columns skipped, not a process abort or table \
         failure), got {exit_code:?}"
    );

    let mut table = env.open_delta_table("skippy").await;
    table.load().await.expect("failed to load delta table");
    let kernel_schema = table.snapshot().unwrap().schema();
    let arrow_schema: deltalake::arrow::datatypes::Schema =
        deltalake::kernel::engine::arrow_conversion::TryIntoArrow::try_into_arrow(
            kernel_schema.as_ref(),
        )
        .expect("failed to convert schema");
    let mut field_names: Vec<&str> = arrow_schema.fields().iter().map(|f| f.name().as_str()).collect();
    field_names.sort_unstable();
    assert_eq!(
        field_names,
        vec!["id", "name"],
        "Delta schema must contain ONLY id + name — t/y/b must be skipped, not present as \
         (unmapped) columns"
    );

    let row_count = count_delta_rows(&env, "skippy").await;
    assert_eq!(row_count, 2, "expected 2 rows in Delta table for skippy");

    assert_eq!(
        count_matching(&env, "skippy", "id = 1 AND name = 'alice'").await,
        1,
        "row 1's surviving columns must round-trip intact"
    );
    assert_eq!(
        count_matching(&env, "skippy", "id = 2 AND name = 'bob'").await,
        1,
        "row 2's surviving columns must round-trip intact"
    );
}
