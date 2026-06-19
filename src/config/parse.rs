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

pub(crate) fn parse_table_modes(tables: &[String]) -> HashMap<String, ExtractionMode> {
    let mut modes = HashMap::new();
    for table in tables {
        let key = format!("TABLE_MODE_{table}");
        if let Ok(val) = std::env::var(&key) {
            let mode = match val.to_lowercase().as_str() {
                "incremental" => ExtractionMode::Incremental,
                "full_refresh" => ExtractionMode::FullRefresh,
                _ => ExtractionMode::Auto,
            };
            modes.insert(table.clone(), mode);
        }
    }
    modes
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
