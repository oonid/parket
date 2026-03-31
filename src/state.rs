use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::path::Path;

use anyhow::Result;
use serde::{Deserialize, Serialize};
use tracing::warn;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableState {
    pub last_run_at: Option<String>,
    pub last_run_status: Option<String>,
    pub last_run_rows: Option<u64>,
    pub last_run_duration_ms: Option<u64>,
    pub extraction_mode: Option<String>,
    pub schema_columns_hash: Option<String>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct AppState {
    #[serde(default)]
    pub tables: HashMap<String, TableState>,
}

impl AppState {
    pub fn load(path: &Path) -> Result<Self> {
        if !path.exists() {
            return Ok(Self::default());
        }

        let data = fs::read_to_string(path)?;
        let state: AppState = serde_json::from_str(&data)?;
        Ok(state)
    }

    pub fn load_or_warn(path: &Path) -> Self {
        match Self::load(path) {
            Ok(state) => state,
            Err(e) => {
                warn!("state.json is corrupted or invalid, treating as first run: {e}");
                Self::default()
            }
        }
    }

    pub fn update_table(&mut self, table_name: &str, state: TableState, path: &Path) -> Result<()> {
        self.tables.insert(table_name.to_string(), state);
        self.write_atomic(path)
    }

    fn write_atomic(&self, path: &Path) -> Result<()> {
        let json = serde_json::to_string_pretty(self)?;

        let tmp_path = path.with_extension("tmp");
        {
            let mut tmp = fs::File::create(&tmp_path)?;
            tmp.write_all(json.as_bytes())?;
            tmp.flush()?;
        }
        fs::rename(&tmp_path, path)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn state_path(dir: &TempDir) -> std::path::PathBuf {
        dir.path().join("state.json")
    }

    fn sample_table_state() -> TableState {
        TableState {
            last_run_at: Some("2026-03-28T10:00:00Z".to_string()),
            last_run_status: Some("success".to_string()),
            last_run_rows: Some(50000),
            last_run_duration_ms: Some(3200),
            extraction_mode: Some("incremental".to_string()),
            schema_columns_hash: Some("abc123".to_string()),
        }
    }

    #[test]
    fn load_missing_file_returns_default() {
        let dir = TempDir::new().unwrap();
        let path = state_path(&dir);

        let state = AppState::load(&path).unwrap();

        assert!(state.tables.is_empty());
    }

    #[test]
    fn load_valid_json() {
        let dir = TempDir::new().unwrap();
        let path = state_path(&dir);

        let json = r#"{"tables":{"orders":{"last_run_at":"2026-03-28T10:00:00Z","last_run_status":"success","last_run_rows":50000,"last_run_duration_ms":3200,"extraction_mode":"incremental","schema_columns_hash":"abc123"}}}"#;
        fs::write(&path, json).unwrap();

        let state = AppState::load(&path).unwrap();

        assert_eq!(state.tables.len(), 1);
        let ts = state.tables.get("orders").unwrap();
        assert_eq!(ts.last_run_status, Some("success".to_string()));
        assert_eq!(ts.last_run_rows, Some(50000));
        assert_eq!(ts.extraction_mode, Some("incremental".to_string()));
        assert_eq!(ts.schema_columns_hash, Some("abc123".to_string()));
    }

    #[test]
    fn load_corrupted_json_returns_error() {
        let dir = TempDir::new().unwrap();
        let path = state_path(&dir);

        fs::write(&path, "{not valid json}}}").unwrap();

        let result = AppState::load(&path);
        assert!(result.is_err());
    }

    #[test]
    fn load_or_warn_corrupted_json_returns_default() {
        let dir = TempDir::new().unwrap();
        let path = state_path(&dir);

        fs::write(&path, "{not valid json}}}").unwrap();

        let state = AppState::load_or_warn(&path);
        assert!(state.tables.is_empty());
    }

    #[test]
    fn load_or_warn_valid_json_returns_state() {
        let dir = TempDir::new().unwrap();
        let path = state_path(&dir);

        let json = r#"{"tables":{"orders":{"last_run_at":"2026-03-28T10:00:00Z","last_run_status":"success","last_run_rows":50000,"last_run_duration_ms":3200,"extraction_mode":"incremental","schema_columns_hash":"abc123"}}}"#;
        fs::write(&path, json).unwrap();

        let state = AppState::load_or_warn(&path);
        assert_eq!(state.tables.len(), 1);
        assert!(state.tables.contains_key("orders"));
    }

    #[test]
    fn load_or_warn_missing_file_returns_default() {
        let dir = TempDir::new().unwrap();
        let path = state_path(&dir);

        let state = AppState::load_or_warn(&path);
        assert!(state.tables.is_empty());
    }

    #[test]
    fn update_table_success() {
        let dir = TempDir::new().unwrap();
        let path = state_path(&dir);

        let mut state = AppState::default();
        state
            .update_table("orders", sample_table_state(), &path)
            .unwrap();

        let loaded = AppState::load(&path).unwrap();
        assert_eq!(loaded.tables.len(), 1);
        let ts = loaded.tables.get("orders").unwrap();
        assert_eq!(ts.last_run_status, Some("success".to_string()));
        assert_eq!(ts.last_run_rows, Some(50000));
    }

    #[test]
    fn update_table_overwrites_previous() {
        let dir = TempDir::new().unwrap();
        let path = state_path(&dir);

        let mut state = AppState::default();
        state
            .update_table("orders", sample_table_state(), &path)
            .unwrap();

        let failed_state = TableState {
            last_run_at: Some("2026-03-28T11:00:00Z".to_string()),
            last_run_status: Some("failed".to_string()),
            last_run_rows: None,
            last_run_duration_ms: None,
            extraction_mode: Some("incremental".to_string()),
            schema_columns_hash: Some("abc123".to_string()),
        };
        state.update_table("orders", failed_state, &path).unwrap();

        let loaded = AppState::load(&path).unwrap();
        assert_eq!(loaded.tables.len(), 1);
        let ts = loaded.tables.get("orders").unwrap();
        assert_eq!(ts.last_run_status, Some("failed".to_string()));
        assert_eq!(ts.last_run_rows, None);
    }

    #[test]
    fn update_multiple_tables() {
        let dir = TempDir::new().unwrap();
        let path = state_path(&dir);

        let mut state = AppState::default();
        state
            .update_table("orders", sample_table_state(), &path)
            .unwrap();

        let customer_state = TableState {
            last_run_at: Some("2026-03-28T10:05:00Z".to_string()),
            last_run_status: Some("success".to_string()),
            last_run_rows: Some(12000),
            last_run_duration_ms: Some(800),
            extraction_mode: Some("full_refresh".to_string()),
            schema_columns_hash: Some("def456".to_string()),
        };
        state
            .update_table("customers", customer_state, &path)
            .unwrap();

        let loaded = AppState::load(&path).unwrap();
        assert_eq!(loaded.tables.len(), 2);
        assert!(loaded.tables.contains_key("orders"));
        assert!(loaded.tables.contains_key("customers"));
    }

    #[test]
    fn atomic_write_uses_temp_then_rename() {
        let dir = TempDir::new().unwrap();
        let path = state_path(&dir);

        let mut state = AppState::default();
        state
            .update_table("orders", sample_table_state(), &path)
            .unwrap();

        assert!(path.exists());
        assert!(!path.with_extension("tmp").exists());
    }

    #[test]
    fn load_empty_file_returns_error() {
        let dir = TempDir::new().unwrap();
        let path = state_path(&dir);

        fs::write(&path, "").unwrap();

        let result = AppState::load(&path);
        assert!(result.is_err());
    }

    #[test]
    fn load_empty_json_object() {
        let dir = TempDir::new().unwrap();
        let path = state_path(&dir);

        fs::write(&path, "{}").unwrap();

        let state = AppState::load(&path).unwrap();
        assert!(state.tables.is_empty());
    }
}
