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
        s3_endpoint: Some(s3_endpoint.to_string()),
        s3_region: "us-east-1".to_string(),
        s3_prefix: "parket".to_string(),
        default_batch_size: 10000,
        rust_log: "parket=debug".to_string(),
        table_modes: HashMap::new(),
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

    assert!(
        matches!(exit_code, ExitCode::Success),
        "expected Success exit code, got {exit_code:?}"
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

