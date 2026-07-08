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
