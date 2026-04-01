//! Standalone pipeline example — no MariaDB, no MinIO, no Docker.
//!
//! Translates the integration test scenarios into a single runnable application
//! that writes real Delta Lake tables to the local filesystem.
//!
//!     cargo run --example standalone_pipeline
//!
//! Output goes to `/tmp/parket-example/` (auto-cleaned if it already exists).
//! After running, inspect with:
//!
//!     ls -R /tmp/parket-example/
//!     python3 -c "import deltalake; dt=deltalake.DeltaTable('/tmp/parket-example/orders'); print(dt.to_pandas())"

use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use deltalake::arrow::array::{
    Float64Array, Int64Array, Int32Array, StringArray, TimestampMicrosecondArray,
};
use deltalake::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use deltalake::arrow::record_batch::RecordBatch;
use futures::StreamExt;

use parket::writer::DeltaWriter;

const BASE_DIR: &str = "/tmp/parket-example";

// ---------------------------------------------------------------------------
// "SQL data" embedded as Rust — mirrors what integration tests insert into MariaDB
//
// CREATE TABLE orders (
//     id           BIGINT PRIMARY KEY,
//     name         VARCHAR(255),
//     qty          INT,
//     updated_at   TIMESTAMP(6)
// );
//
// CREATE TABLE products (
//     sku          VARCHAR(50) PRIMARY KEY,
//     description  TEXT,
//     price        DOUBLE
// );
// ---------------------------------------------------------------------------

struct OrderRow {
    id: i64,
    name: &'static str,
    qty: i32,
    updated_at_micros: i64,
}

struct ProductRow {
    sku: &'static str,
    description: &'static str,
    price: f64,
}

const ORDERS_RUN1: &[OrderRow] = &[
    OrderRow { id: 1, name: "widget", qty: 10, updated_at_micros: 1735689600000000 },
    OrderRow { id: 2, name: "gadget", qty: 5,  updated_at_micros: 1735689660000000 },
    OrderRow { id: 3, name: "doohickey", qty: 2,  updated_at_micros: 1735776000000000 },
];

const ORDERS_RUN2_NEW: &[OrderRow] = &[
    OrderRow { id: 4, name: "thingamajig", qty: 7, updated_at_micros: 1735862400000000 },
    OrderRow { id: 5, name: "whatchamacallit", qty: 1, updated_at_micros: 1735948800000000 },
];

const PRODUCTS: &[ProductRow] = &[
    ProductRow { sku: "SKU-A", description: "Alpha widget", price: 9.99 },
    ProductRow { sku: "SKU-B", description: "Beta gadget", price: 19.99 },
    ProductRow { sku: "SKU-C", description: "Gamma doohickey", price: 29.99 },
];

// ---------------------------------------------------------------------------
// Schema + batch builders
// ---------------------------------------------------------------------------

fn orders_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("qty", DataType::Int32, false),
        Field::new("updated_at", DataType::Timestamp(TimeUnit::Microsecond, None), false),
    ]))
}

fn products_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("sku", DataType::Utf8, false),
        Field::new("description", DataType::Utf8, false),
        Field::new("price", DataType::Float64, false),
    ]))
}

fn build_orders_batch(rows: &[OrderRow]) -> RecordBatch {
    let ids = Int64Array::from(rows.iter().map(|r| r.id).collect::<Vec<_>>());
    let names = StringArray::from(rows.iter().map(|r| r.name).collect::<Vec<_>>());
    let qtys = Int32Array::from(rows.iter().map(|r| r.qty).collect::<Vec<_>>());
    let timestamps = TimestampMicrosecondArray::from(
        rows.iter().map(|r| r.updated_at_micros).collect::<Vec<_>>(),
    );
    RecordBatch::try_new(orders_schema(), vec![
        Arc::new(ids),
        Arc::new(names),
        Arc::new(qtys),
        Arc::new(timestamps),
    ]).unwrap()
}

fn build_products_batch(rows: &[ProductRow]) -> RecordBatch {
    let skus = StringArray::from(rows.iter().map(|r| r.sku).collect::<Vec<_>>());
    let descriptions = StringArray::from(rows.iter().map(|r| r.description).collect::<Vec<_>>());
    let prices = Float64Array::from(rows.iter().map(|r| r.price).collect::<Vec<_>>());
    RecordBatch::try_new(products_schema(), vec![
        Arc::new(skus),
        Arc::new(descriptions),
        Arc::new(prices),
    ]).unwrap()
}

// ---------------------------------------------------------------------------
// Verification helpers (same approach as tests/integration.rs)
// ---------------------------------------------------------------------------

async fn count_delta_rows(writer: &DeltaWriter, table_name: &str) -> usize {
    let mut table = writer.open_table(table_name).await.expect("open table");
    table.load().await.expect("load table");
    let stream = table.scan_table().await.expect("scan_table failed").1;
    futures::pin_mut!(stream);
    let mut total = 0;
    while let Some(batch) = stream.next().await {
        let batch = batch.expect("failed to read batch");
        total += batch.num_rows();
    }
    total
}

fn print_parquet_listing(table: &str) {
    let table_dir = format!("{}/{}", BASE_DIR, table);
    println!("  [files] {}/:", table_dir);
    if let Ok(entries) = std::fs::read_dir(&table_dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().is_some_and(|e| e == "parquet") {
                let meta = std::fs::metadata(&path).unwrap();
                println!("    {} ({} bytes)", path.display(), meta.len());
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Main pipeline
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<()> {
    println!("=== Parket Standalone Pipeline Example ===\n");

    if Path::new(BASE_DIR).exists() {
        std::fs::remove_dir_all(BASE_DIR)?;
        println!("[setup] Cleaned existing {}", BASE_DIR);
    }
    std::fs::create_dir_all(BASE_DIR)?;
    println!("[setup] Output directory: {}\n", BASE_DIR);

    let writer = DeltaWriter::new_local(BASE_DIR);

    // ------------------------------------------------------------------
    // Scenario 1: FullRefresh (products table — no updated_at)
    // ------------------------------------------------------------------
    println!("--- Scenario 1: FullRefresh (products) ---");

    let products_batch = build_products_batch(PRODUCTS);
    println!(
        "[run 1] Built products batch: {} rows, {} columns",
        products_batch.num_rows(),
        products_batch.num_columns(),
    );

    writer.ensure_table("products", products_schema()).await?;
    writer.overwrite_table("products", vec![products_batch], None).await?;
    println!("[run 1] Wrote products via Overwrite (FullRefresh mode)");

    let product_rows = count_delta_rows(&writer, "products").await;
    assert_eq!(product_rows, 3, "products should have 3 rows");
    println!("  [verify] products: 3 rows - OK");
    print_parquet_listing("products");

    // ------------------------------------------------------------------
    // Scenario 2: Incremental with HWM (orders table)
    // ------------------------------------------------------------------
    println!("\n--- Scenario 2: Incremental + HWM (orders) ---");

    // Run 1: first 3 orders
    let orders_batch1 = build_orders_batch(ORDERS_RUN1);
    println!("[run 1] Built orders batch: {} rows", orders_batch1.num_rows());

    writer.ensure_table("orders", orders_schema()).await?;

    let hwm1 = parket::writer::extract_hwm_from_batch(&orders_batch1)
        .expect("should extract HWM from orders batch");
    println!(
        "[run 1] Extracted HWM: updated_at={}, last_id={}",
        hwm1.updated_at, hwm1.last_id,
    );

    writer.append_batch("orders", vec![orders_batch1], Some(&hwm1)).await?;
    println!("[run 1] Wrote orders via Append (Incremental mode)");

    let orders_rows1 = count_delta_rows(&writer, "orders").await;
    assert_eq!(orders_rows1, 3, "orders should have 3 rows after run 1");
    println!("  [verify] orders: 3 rows - OK");

    let stored_hwm1 = writer.read_hwm("orders").await?.expect("HWM should exist after run 1");
    assert_eq!(stored_hwm1.last_id, 3, "HWM last_id should be 3 after run 1");
    println!(
        "  [verify] HWM after run 1: updated_at={}, last_id={} - OK",
        stored_hwm1.updated_at, stored_hwm1.last_id,
    );
    print_parquet_listing("orders");

    // Run 2: simulate restart — read HWM from Delta, append only new rows
    println!("\n  --- Simulating process restart ---");

    let hwm_read = writer.read_hwm("orders").await?.expect("HWM should persist across restarts");
    println!(
        "[run 2] Read persisted HWM: updated_at={}, last_id={}",
        hwm_read.updated_at, hwm_read.last_id,
    );

    let orders_batch2 = build_orders_batch(ORDERS_RUN2_NEW);
    println!(
        "[run 2] Built NEW orders batch: {} rows (only rows after HWM)",
        orders_batch2.num_rows(),
    );

    let hwm2 = parket::writer::extract_hwm_from_batch(&orders_batch2).unwrap();
    println!(
        "[run 2] Extracted new HWM: updated_at={}, last_id={}",
        hwm2.updated_at, hwm2.last_id,
    );

    writer.append_batch("orders", vec![orders_batch2], Some(&hwm2)).await?;
    println!("[run 2] Appended new orders");

    let orders_rows2 = count_delta_rows(&writer, "orders").await;
    assert_eq!(orders_rows2, 5, "orders should have 5 rows after run 2");
    println!("  [verify] orders: 5 rows - OK");

    let stored_hwm2 = writer.read_hwm("orders").await?.expect("HWM should exist after run 2");
    assert_eq!(stored_hwm2.last_id, 5, "HWM last_id should be 5 after run 2");
    assert_eq!(
        stored_hwm2.updated_at, hwm2.updated_at,
        "HWM updated_at should match latest batch",
    );
    println!(
        "  [verify] HWM after run 2: updated_at={}, last_id={} - OK",
        stored_hwm2.updated_at, stored_hwm2.last_id,
    );
    print_parquet_listing("orders");

    // ------------------------------------------------------------------
    // Summary
    // ------------------------------------------------------------------
    println!("\n=== Summary ===");
    println!("Output directory: {}/", BASE_DIR);
    println!("  products/  - Delta table (FullRefresh, 3 rows, 3 columns)");
    println!("  orders/    - Delta table (Incremental, 5 rows across 2 commits, HWM persisted)");
    println!();
    println!("Inspect with:");
    println!("  ls -R {}/", BASE_DIR);
    println!(
        "  python3 -c \"import deltalake; dt=deltalake.DeltaTable('{}/orders'); print(dt.to_pandas())\"",
        BASE_DIR,
    );
    println!();
    println!("All scenarios passed.");

    Ok(())
}
