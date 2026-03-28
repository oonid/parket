pub struct QueryBuilder;

impl QueryBuilder {
    pub fn build_incremental_query(
        _table: &str,
        _columns: &[String],
        _hwm_updated_at: Option<&str>,
        _hwm_last_id: Option<i64>,
        _batch_size: u64,
    ) -> String {
        todo!()
    }

    pub fn build_full_refresh_query(_table: &str, _columns: &[String]) -> String {
        todo!()
    }
}
