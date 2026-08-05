//! Delta-side key census — a read-only diagnostic over the **Delta copy only**.
//!
//! Answers "does this Delta table hold duplicate keys?" without touching the source
//! database at all. That distinction is the point: the equivalent question via
//! `--verify --verify-deep` drives four to five full scans of the source table (see
//! `docs/audit-findings.md` §8.5), which the runbook reserves for off-peak windows. This
//! reads only the key column out of the Delta parquet, so it is cheap enough to run any
//! time and costs the production DB nothing.
//!
//! Two registered uses:
//!  * **FA2-r2** — in two-stream mode Delta is a current-state mirror (one row per key), so
//!    `count > distinct` means duplicated rows (the FA2 class). `--verify` only reports this
//!    as a diagnostic; this tool measures it directly.
//!  * **§8.5 frontier parity** — pair `count` here with the source's
//!    `COUNT(*) WHERE key <= hwm_insert_id` to detect drift cheaply.
//!
//!     # from the directory holding .env (e.g. porter/)
//!     cargo run --example delta_key_census -- <table> [key_col]
//!
//! Reads `S3_BUCKET`/`S3_PREFIX`/`S3_ENDPOINT`/`S3_REGION`/`S3_ACCESS_KEY_ID`/
//! `S3_SECRET_ACCESS_KEY` (plus optional `MERGE_MEMORY_MB`/`MERGE_SPILL_DIR` to bound the
//! datafusion session — the same knobs the verify probes use).
//! `--local <dir>` censuses a local-filesystem table instead. `key_col` defaults to `id`.

use anyhow::{Context, Result};
use parket::verify::{DeltaProbe, DeltaProbeAdapter};
use parket::writer::DeltaWriter;

fn env_opt(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|v| !v.trim().is_empty())
}

fn env_req(key: &str) -> Result<String> {
    env_opt(key).with_context(|| format!("{key} must be set (or pass --local <dir>)"))
}

#[tokio::main]
async fn main() -> Result<()> {
    let _ = dotenvy::dotenv();
    deltalake::aws::register_handlers(None);

    let args: Vec<String> = std::env::args().skip(1).collect();
    let local_dir = args
        .iter()
        .position(|a| a == "--local")
        .and_then(|i| args.get(i + 1).cloned());
    let positional: Vec<&String> = args
        .iter()
        .filter(|a| a.as_str() != "--local")
        .filter(|a| Some(a.as_str()) != local_dir.as_deref())
        .collect();

    let table = match positional.first() {
        Some(t) => (*t).clone(),
        None => {
            eprintln!(
                "usage: cargo run --example delta_key_census -- <table> [key_col] [--local <dir>]"
            );
            std::process::exit(2);
        }
    };
    let key_col = positional
        .get(1)
        .map(|s| (*s).clone())
        .unwrap_or_else(|| "id".to_string());

    // Bound the datafusion session exactly like the verify probes do (VA3): the distinct
    // aggregate over a large key column spills to disk instead of growing unbounded.
    let merge_memory_mb: u64 = env_opt("MERGE_MEMORY_MB")
        .or_else(|| env_opt("TARGET_MEMORY_MB"))
        .map(|v| v.parse())
        .transpose()
        .context("MERGE_MEMORY_MB/TARGET_MEMORY_MB must be a positive integer")?
        .unwrap_or(512);
    let spill_dir = env_opt("MERGE_SPILL_DIR").map(std::path::PathBuf::from);

    // Local mode ONLY via an explicit `--local <dir>`. Deliberately does NOT fall back to
    // `DELTA_PATH`: that variable belongs to the separate `mct-export` tool, and in a real
    // deployment it points at a different (often stale) local directory — honouring it here
    // would silently census the wrong copy while appearing to report on the S3 table.
    let writer = match local_dir {
        Some(dir) => {
            println!("source: local filesystem `{dir}`");
            DeltaWriter::new_local(&dir)
        }
        None => {
            let bucket = env_req("S3_BUCKET")?;
            let prefix = env_opt("S3_PREFIX").unwrap_or_else(|| "parket".to_string());
            let region = env_opt("S3_REGION").unwrap_or_else(|| "us-east-1".to_string());
            println!("source: s3://{bucket}/{prefix}/{table}/");
            DeltaWriter::new(
                &bucket,
                &prefix,
                env_opt("S3_ENDPOINT").as_deref(),
                &region,
                &env_req("S3_ACCESS_KEY_ID")?,
                &env_req("S3_SECRET_ACCESS_KEY")?,
            )
        }
    }
    .with_merge_limits(merge_memory_mb, spill_dir);

    println!("table : {table}\nkey   : {key_col}\n(reading the key column only — the source database is NOT queried)\n");

    let probe = DeltaProbeAdapter::new(writer);
    let ks = probe
        .key_stats(&table, &key_col)
        .await
        .with_context(|| format!("delta key_stats for `{table}`.`{key_col}`"))?;

    println!("count          : {}", ks.count);
    println!("distinct keys  : {}", ks.distinct);
    println!("min / max      : {:?} / {:?}", ks.min, ks.max);
    println!("sum (distinct) : {}", ks.sum);
    println!("xor / distinct : {} / {}", ks.xor, ks.distinct_xor);
    println!();

    match ks.count.checked_sub(ks.distinct) {
        Some(surplus) if surplus > 0 => {
            println!(
                "DUPLICATE KEYS: {surplus} surplus row(s) — {} rows over {} distinct keys.",
                ks.count, ks.distinct
            );
            println!(
                "  Two-stream keeps exactly one row per key, so this is FA2-class duplication;\n  \
                 remediate with a TABLE_RECONCILE_<table>=true one-shot.\n  \
                 (For an INCREMENTAL append-log table a surplus is NORMAL — many versions per key.)"
            );
            std::process::exit(1);
        }
        _ => {
            println!("NO DUPLICATE KEYS: count == distinct ({} rows).", ks.count);
            println!(
                "  Consistent with a two-stream current-state mirror (one row per key).\n  \
                 Pair this count with the source's `COUNT(*) WHERE {key_col} <= hwm_insert_id`\n  \
                 for the §8.5 frontier-parity check."
            );
        }
    }
    Ok(())
}
