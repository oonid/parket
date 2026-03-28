use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableState {
    pub last_run_at: Option<String>,
    pub last_run_status: Option<String>,
    pub last_run_rows: Option<u64>,
    pub last_run_duration_ms: Option<u64>,
    pub extraction_mode: Option<String>,
    pub schema_columns_hash: Option<String>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct AppState {
    pub tables: std::collections::HashMap<String, TableState>,
}

impl AppState {
    pub fn load(_path: &std::path::Path) -> anyhow::Result<Self> {
        todo!()
    }

    pub fn update_table(
        &mut self,
        _table_name: &str,
        _state: TableState,
        _path: &std::path::Path,
    ) -> anyhow::Result<()> {
        todo!()
    }
}
