#[allow(dead_code)] // TODO: remove when SchemaInspector is implemented (Task 5)
pub struct SchemaInspector {
    pool: sqlx::MySqlPool,
}

impl SchemaInspector {
    pub fn new(pool: sqlx::MySqlPool) -> Self {
        Self { pool }
    }
}
