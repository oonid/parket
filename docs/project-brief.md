# Project Brief: Parket - Universal MariaDB to Delta Lake Extractor

## 1. Project Overview
**Repository:** https://github.com/oonid/parket
**Description:** "Parket" is a smart, standalone Rust binary designed to autonomously extract data from MariaDB and load it into Delta Lake (S3/MinIO) using Apache Arrow. It acts as an intelligent Extract & Load (EL) tool that eliminates manual schema mapping through auto-discovery and manages its own memory footprint dynamically based on table metadata.

## 2. Core Objectives
* **Zero-Configuration Type Mapping:** Utilize `connector-x` to automatically infer MariaDB column types and map them directly to Apache Arrow memory formats without intermediate Rust structs.
* **Auto-Discovery:** Automatically inspect table schemas to determine the extraction mode: `Incremental` (if `updated_at` and `id` exist) or `FullRefresh`.
* **Dynamic Memory Management:** Calculate row batch limits on the fly based on a configured `TARGET_MEMORY_MB` and the specific table's `AVG_ROW_LENGTH` from `information_schema`.
* **Idempotent & Resilient Writes:** Write data to S3/MinIO using `delta-rs` (SaveMode::Append) with ACID guarantees. Track High Watermarks (HWM) locally, updating state *only* upon successful Delta Lake commits.

## 3. Technology Stack
* **Language:** Rust
* **Extraction Engine:** `connector-x` (SQL to Arrow)
* **Lakehouse Engine:** `delta-rs` (Arrow to Delta Lake/Parquet)
* **Schema Inspection:** `sqlx` (MySQL feature, for metadata and query building)
* **Async Runtime:** `tokio`
* **Configuration & State:** `dotenvy` (Env vars), `serde_json` (State management)

## 4. System Architecture & Development Milestones

### Milestone 1: Configuration & State Manager
* Load parameters from `.env`: `DATABASE_URL`, S3 credentials, `TARGET_MEMORY_MB`, and a list of target tables.
* Implement a state manager to read/write `state.json`. This tracks the High Watermark (`last_updated_at`, `last_id`) for each table.

### Milestone 2: Auto-Discovery & Memory Profiler
* Connect to MariaDB using `sqlx` to query `information_schema.columns` for detecting Incremental support.
* Query `information_schema.tables` to retrieve `AVG_ROW_LENGTH`.
* Calculate `dynamic_batch_size = (TARGET_MEMORY_MB * 1024 * 1024) / AVG_ROW_LENGTH`.

### Milestone 3: Dynamic Query Builder
* Construct safe, paginated SQL queries based on the extraction mode.
* For `Incremental` mode, utilize a precise windowing query:
  `WHERE (updated_at = ? AND id > ?) OR (updated_at > ?) ORDER BY updated_at ASC, id ASC LIMIT {dynamic_batch_size}`.

### Milestone 4: Core Extraction (connector-x)
* Execute the dynamic SQL string using `connector-x`.
* Retrieve the extracted dataset directly as an Apache Arrow `RecordBatch` residing in memory.

### Milestone 5: Delta Lake Integration (delta-rs)
* Extract the Arrow `Schema` from the `RecordBatch`.
* Ensure the Delta table exists in the target S3 path (create if missing using the extracted schema).
* Append the `RecordBatch` to the Delta table.
* Return a success signal to allow the state manager to update `state.json` with the latest HWM from the batch.

### Milestone 6: Resilient Orchestration Loop
* Implement the main application loop traversing the configured tables.
* For each table, run the extract-and-load cycle in batches until `connector-x` returns 0 rows.
* Ensure graceful error handling: network or S3 failures must panic or exit gracefully *without* updating `state.json`, ensuring the next run retries the exact same batch.