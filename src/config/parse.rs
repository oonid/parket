use std::collections::HashMap;

use anyhow::{bail, Context, Result};

use super::ExtractionMode;

pub(crate) fn env(key: &str) -> Result<String> {
    let val = std::env::var(key).with_context(|| format!("{key} is required"))?;
    if val.is_empty() {
        bail!("{key} is required");
    }
    Ok(val)
}

pub(crate) fn validate_database_url(url: &str) -> Result<()> {
    if url.starts_with("mysql://") {
        Ok(())
    } else {
        bail!("DATABASE_URL must start with mysql:// — unsupported scheme")
    }
}

pub(crate) fn parse_tables(raw: &str) -> Result<Vec<String>> {
    let tables: Vec<String> = raw
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    Ok(tables)
}

pub(crate) fn parse_table_modes(tables: &[String]) -> Result<HashMap<String, ExtractionMode>> {
    let mut modes = HashMap::new();
    for table in tables {
        let key = format!("TABLE_MODE_{table}");
        if let Ok(val) = std::env::var(&key) {
            let normalized = val.trim().to_lowercase();
            let mode = match normalized.as_str() {
                "auto" => ExtractionMode::Auto,
                "incremental" => ExtractionMode::Incremental,
                "full_refresh" => ExtractionMode::FullRefresh,
                "two_stream" => bail!(
                    "{key}={val}: two-stream mode is not selected via {key}; set \
                     TABLE_INSERT_CURSOR_{table} and TABLE_UPDATE_CURSOR_{table} instead \
                     (remove {key})"
                ),
                _ => bail!(
                    "{key}={val}: invalid extraction mode '{val}'; accepted values are \
                     auto, incremental, full_refresh (two-stream is selected via \
                     TABLE_INSERT_CURSOR_{table}/TABLE_UPDATE_CURSOR_{table}, not {key})"
                ),
            };
            modes.insert(table.clone(), mode);
        }
    }
    Ok(modes)
}

/// O5: two-stream cursor config (`TABLE_INSERT_CURSOR_<t>` + `TABLE_UPDATE_CURSOR_<t>`) and an
/// explicit, non-Auto `TABLE_MODE_<t>` are mutually exclusive ways of picking a table's
/// extraction mode. Without this check the orchestrator resolves the two-stream branch first
/// (see `orchestrator.rs`/`preflight.rs`) and silently discards the operator's explicit
/// `TABLE_MODE`. Called from both `Config::load` and `Config::load_local` so `--check` and a
/// real run agree.
pub(crate) fn validate_mode_conflicts(
    tables: &[String],
    table_modes: &HashMap<String, ExtractionMode>,
    table_insert_cursor: &HashMap<String, String>,
    table_update_cursor: &HashMap<String, String>,
) -> Result<()> {
    for table in tables {
        let has_cursors = table_insert_cursor.contains_key(table) && table_update_cursor.contains_key(table);
        if !has_cursors {
            continue;
        }
        if let Some(mode) = table_modes.get(table)
            && *mode != ExtractionMode::Auto
        {
            let mode_str = match mode {
                ExtractionMode::Auto => "auto",
                ExtractionMode::Incremental => "incremental",
                ExtractionMode::FullRefresh => "full_refresh",
                ExtractionMode::TwoStream => "two_stream",
            };
            bail!(
                "table '{table}' has both TABLE_INSERT_CURSOR_{table}/TABLE_UPDATE_CURSOR_{table} \
                 set AND TABLE_MODE_{table}={mode_str}; these conflict — remove \
                 TABLE_MODE_{table} to run '{table}' as two-stream, or remove the cursor \
                 vars to run it as {mode_str}"
            );
        }
    }
    Ok(())
}

pub(crate) fn parse_table_initial_hwm(tables: &[String]) -> Result<HashMap<String, (String, i64)>> {
    let mut map = HashMap::new();
    for table in tables {
        let key = format!("TABLE_HWM_{table}");
        if let Ok(val) = std::env::var(&key) {
            let val = val.trim();
            if val.is_empty() {
                continue;
            }
            let (ua, id_str) = val.split_once(',')
                .ok_or_else(|| anyhow::anyhow!("{key} must be '<updated_at>,<last_id>', got '{val}'"))?;
            let ua = ua.trim();
            let id_str = id_str.trim();
            if ua.is_empty() {
                bail!("{key}: updated_at must not be empty");
            }
            let last_id: i64 = id_str.parse()
                .with_context(|| format!("{key}: last_id '{id_str}' is not a valid i64"))?;
            map.insert(table.clone(), (ua.to_string(), last_id));
        }
    }
    Ok(map)
}

pub(crate) fn parse_table_timestamp_col(tables: &[String]) -> HashMap<String, String> {
    let mut map = HashMap::new();
    for table in tables {
        let key = format!("TABLE_TIMESTAMP_{table}");
        if let Ok(val) = std::env::var(&key) {
            let val = val.trim();
            if !val.is_empty() {
                map.insert(table.clone(), val.to_string());
            }
        }
    }
    map
}

pub(crate) fn parse_table_insert_cursor(tables: &[String]) -> HashMap<String, String> {
    let mut map = HashMap::new();
    for table in tables {
        let key = format!("TABLE_INSERT_CURSOR_{table}");
        if let Ok(val) = std::env::var(&key) {
            let val = val.trim();
            if !val.is_empty() {
                map.insert(table.clone(), val.to_string());
            }
        }
    }
    map
}

pub(crate) fn parse_table_update_cursor(tables: &[String]) -> HashMap<String, String> {
    let mut map = HashMap::new();
    for table in tables {
        let key = format!("TABLE_UPDATE_CURSOR_{table}");
        if let Ok(val) = std::env::var(&key) {
            let val = val.trim();
            if !val.is_empty() {
                map.insert(table.clone(), val.to_string());
            }
        }
    }
    map
}

/// Detect total physical RAM in MB by reading `/proc/meminfo` (`MemTotal:  N kB`).
/// Returns None on any failure or non-Linux platform, so callers never block when
/// RAM is unknowable. NOTE: in a container this reports the HOST total, not the
/// cgroup limit — acceptable for the VM target; cgroup-aware detection is a future nicety.
pub(crate) fn detect_total_ram_mb() -> Option<u64> {
    let contents = std::fs::read_to_string("/proc/meminfo").ok()?;
    for line in contents.lines() {
        if let Some(rest) = line.strip_prefix("MemTotal:") {
            let kb: u64 = rest.split_whitespace().next()?.parse().ok()?;
            return Some(kb / 1024);
        }
    }
    None
}

/// Reject a memory budget that cannot physically fit in RAM (M4). `total_ram_mb`
/// is passed in (None = undetectable → no check) so this stays unit-testable.
/// The circuit breaker admits up to 2x TARGET_MEMORY_MB resident (up to ~4x for
/// unsigned-heavy tables post-widening), so a budget near/over RAM OOMs at runtime.
pub(crate) fn validate_memory_budget(
    target_memory_mb: u64,
    merge_memory_mb: u64,
    total_ram_mb: Option<u64>,
) -> Result<()> {
    let Some(total) = total_ram_mb else { return Ok(()) };
    if target_memory_mb > total {
        bail!(
            "TARGET_MEMORY_MB ({target_memory_mb}) exceeds detected system RAM ({total} MB); \
             the extract memory budget cannot be larger than physical RAM. Set it to at most \
             half of RAM (~{} MB) to leave headroom for the 2x circuit-breaker ceiling.",
            total / 2
        );
    }
    if merge_memory_mb > total {
        bail!(
            "MERGE_MEMORY_MB ({merge_memory_mb}) exceeds detected system RAM ({total} MB); \
             the MERGE spill-pool budget cannot be larger than physical RAM."
        );
    }
    // Not impossible, but the breaker's 2x resident ceiling would exceed RAM → likely OOM.
    if target_memory_mb.saturating_mul(2) > total {
        tracing::warn!(
            target_memory_mb,
            total_ram_mb = total,
            "TARGET_MEMORY_MB is more than half of detected RAM; the circuit breaker admits up \
             to 2x this budget resident (up to ~4x for unsigned-heavy tables), so OOM is likely. \
             Consider a budget of ~{} MB or less.",
            total / 4
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_memory_budget_ok_when_under_ram() {
        assert!(validate_memory_budget(512, 512, Some(8192)).is_ok());
    }

    #[test]
    fn validate_memory_budget_ok_at_exactly_ram_boundary() {
        assert!(validate_memory_budget(8192, 8192, Some(8192)).is_ok());
    }

    #[test]
    fn validate_memory_budget_ok_when_ram_undetectable() {
        assert!(validate_memory_budget(65536, 65536, None).is_ok());
    }

    #[test]
    fn validate_memory_budget_bails_when_target_exceeds_ram() {
        let result = validate_memory_budget(65536, 512, Some(8192));
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("TARGET_MEMORY_MB"),
            "error should mention TARGET_MEMORY_MB, got: {err}"
        );
    }

    #[test]
    fn validate_memory_budget_bails_when_merge_exceeds_ram() {
        let result = validate_memory_budget(512, 65536, Some(8192));
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("MERGE_MEMORY_MB"),
            "error should mention MERGE_MEMORY_MB, got: {err}"
        );
    }
}
