fn backtick(ident: &str) -> String {
    format!("`{ident}`")
}

fn format_columns(columns: &[String]) -> String {
    columns
        .iter()
        .map(|c| backtick(c))
        .collect::<Vec<_>>()
        .join(", ")
}

pub struct QueryBuilder;

impl QueryBuilder {
    pub fn build_incremental_query(
        table: &str,
        columns: &[String],
        timestamp_col: &str,
        hwm_updated_at: Option<&str>,
        hwm_last_id: Option<i64>,
        batch_size: u64,
    ) -> String {
        let col_list = format_columns(columns);
        let quoted_table = backtick(table);
        let ts = backtick(timestamp_col);

        match (hwm_updated_at, hwm_last_id) {
            (Some(updated_at), Some(last_id)) => {
                format!(
                    "SELECT {col_list} FROM {quoted_table} WHERE ({ts} = '{updated_at}' AND `id` > {last_id}) OR ({ts} > '{updated_at}') ORDER BY {ts} ASC, `id` ASC LIMIT {batch_size}"
                )
            }
            _ => {
                format!(
                    "SELECT {col_list} FROM {quoted_table} ORDER BY {ts} ASC, `id` ASC LIMIT {batch_size}"
                )
            }
        }
    }

    pub fn build_full_refresh_query(table: &str, columns: &[String]) -> String {
        let col_list = format_columns(columns);
        let quoted_table = backtick(table);
        format!("SELECT {col_list} FROM {quoted_table}")
    }

    pub fn build_full_refresh_query_paged(
        table: &str,
        columns: &[String],
        batch_size: u64,
        offset: u64,
    ) -> String {
        let col_list = format_columns(columns);
        let quoted_table = backtick(table);
        format!("SELECT {col_list} FROM {quoted_table} LIMIT {batch_size} OFFSET {offset}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn incremental_with_hwm() {
        let sql = QueryBuilder::build_incremental_query(
            "orders",
            &[
                "id".to_string(),
                "name".to_string(),
                "updated_at".to_string(),
            ],
            "updated_at",
            Some("2026-03-28 09:00:00"),
            Some(500),
            10000,
        );

        assert!(sql.contains("SELECT `id`, `name`, `updated_at` FROM `orders`"));
        assert!(sql.contains("WHERE (`updated_at` = '2026-03-28 09:00:00' AND `id` > 500)"));
        assert!(sql.contains("OR (`updated_at` > '2026-03-28 09:00:00')"));
        assert!(sql.contains("ORDER BY `updated_at` ASC, `id` ASC"));
        assert!(sql.contains("LIMIT 10000"));
    }

    #[test]
    fn incremental_without_hwm_first_run() {
        let sql = QueryBuilder::build_incremental_query(
            "orders",
            &[
                "id".to_string(),
                "name".to_string(),
                "updated_at".to_string(),
            ],
            "updated_at",
            None,
            None,
            10000,
        );

        assert!(sql.contains("SELECT `id`, `name`, `updated_at` FROM `orders`"));
        assert!(!sql.contains("WHERE"));
        assert!(sql.contains("ORDER BY `updated_at` ASC, `id` ASC"));
        assert!(sql.contains("LIMIT 10000"));
    }

    #[test]
    fn full_refresh_query() {
        let sql = QueryBuilder::build_full_refresh_query(
            "customers",
            &["id".to_string(), "email".to_string()],
        );

        assert_eq!(sql, "SELECT `id`, `email` FROM `customers`");
    }

    #[test]
    fn full_refresh_no_limit_no_where() {
        let sql = QueryBuilder::build_full_refresh_query(
            "products",
            &["id".to_string(), "name".to_string()],
        );

        assert!(!sql.contains("WHERE"));
        assert!(!sql.contains("LIMIT"));
        assert!(!sql.contains("ORDER BY"));
    }

    #[test]
    fn backtick_quoting_table_name() {
        let sql = QueryBuilder::build_full_refresh_query("order", &["id".to_string()]);

        assert!(sql.contains("FROM `order`"));
    }

    #[test]
    fn backtick_quoting_column_names() {
        let sql = QueryBuilder::build_full_refresh_query(
            "orders",
            &["id".to_string(), "create date".to_string()],
        );

        assert!(sql.contains("`create date`"));
    }

    #[test]
    fn single_column_query() {
        let sql = QueryBuilder::build_full_refresh_query("orders", &["id".to_string()]);

        assert_eq!(sql, "SELECT `id` FROM `orders`");
    }

    #[test]
    fn incremental_hwm_values_interpolated_not_parameterized() {
        let sql = QueryBuilder::build_incremental_query(
            "orders",
            &["id".to_string()],
            "updated_at",
            Some("2026-01-01 00:00:00"),
            Some(42),
            5000,
        );

        assert!(
            sql.contains("'2026-01-01 00:00:00'"),
            "HWM timestamp should be single-quoted inline"
        );
        assert!(sql.contains("> 42"), "HWM id should be interpolated inline");
        assert!(!sql.contains("?"), "No parameterized placeholders");
    }

    #[test]
    fn incremental_partial_hwm_treated_as_no_hwm() {
        let sql = QueryBuilder::build_incremental_query(
            "orders",
            &["id".to_string()],
            "updated_at",
            Some("2026-01-01 00:00:00"),
            None,
            10000,
        );

        assert!(
            !sql.contains("WHERE"),
            "Partial HWM should be treated as no HWM"
        );
    }

    #[test]
    fn incremental_partial_hwm_no_updated_at_treated_as_no_hwm() {
        let sql = QueryBuilder::build_incremental_query(
            "orders",
            &["id".to_string()],
            "updated_at",
            None,
            Some(42),
            10000,
        );

        assert!(
            !sql.contains("WHERE"),
            "Partial HWM should be treated as no HWM"
        );
    }

    #[test]
    fn backtick_helper() {
        assert_eq!(backtick("orders"), "`orders`");
        assert_eq!(backtick("select"), "`select`");
    }

    #[test]
    fn format_columns_helper() {
        assert_eq!(
            format_columns(&["a".to_string(), "b".to_string(), "c".to_string()]),
            "`a`, `b`, `c`"
        );
        assert_eq!(format_columns(&["x".to_string()]), "`x`");
    }

    #[test]
    fn incremental_exact_expected_output() {
        let sql = QueryBuilder::build_incremental_query(
            "orders",
            &[
                "id".to_string(),
                "name".to_string(),
                "updated_at".to_string(),
            ],
            "updated_at",
            Some("2026-03-28 09:00:00"),
            Some(500),
            10000,
        );

        assert_eq!(
            sql,
            "SELECT `id`, `name`, `updated_at` FROM `orders` WHERE (`updated_at` = '2026-03-28 09:00:00' AND `id` > 500) OR (`updated_at` > '2026-03-28 09:00:00') ORDER BY `updated_at` ASC, `id` ASC LIMIT 10000"
        );
    }

    #[test]
    fn incremental_no_hwm_exact_output() {
        let sql =
            QueryBuilder::build_incremental_query("orders", &["id".to_string()], "updated_at", None, None, 5000);

        assert_eq!(
            sql,
            "SELECT `id` FROM `orders` ORDER BY `updated_at` ASC, `id` ASC LIMIT 5000"
        );
    }

    #[test]
    fn incremental_custom_timestamp_col() {
        let sql = QueryBuilder::build_incremental_query(
            "orders",
            &[
                "id".to_string(),
                "name".to_string(),
                "completed_at".to_string(),
            ],
            "completed_at",
            Some("2026-03-28 09:00:00"),
            Some(500),
            10000,
        );

        assert!(sql.contains("`completed_at` = '2026-03-28 09:00:00'"));
        assert!(sql.contains("`completed_at` > '2026-03-28 09:00:00'"));
        assert!(sql.contains("ORDER BY `completed_at` ASC, `id` ASC"));
    }

    #[test]
    fn full_refresh_paged_contains_limit_and_offset() {
        let sql = QueryBuilder::build_full_refresh_query_paged(
            "orders", &["id".to_string(), "name".to_string()], 5000, 0,
        );
        assert!(sql.contains("LIMIT 5000"));
        assert!(sql.contains("OFFSET 0"));
        assert!(!sql.contains("WHERE"));
        assert!(!sql.contains("ORDER BY"));
    }

    #[test]
    fn full_refresh_paged_second_page_offset() {
        let sql = QueryBuilder::build_full_refresh_query_paged(
            "orders", &["id".to_string()], 5000, 5000,
        );
        assert!(sql.contains("LIMIT 5000"));
        assert!(sql.contains("OFFSET 5000"));
    }

    #[test]
    fn full_refresh_paged_exact_output() {
        let sql = QueryBuilder::build_full_refresh_query_paged(
            "customers", &["id".to_string(), "email".to_string()], 1000, 2000,
        );
        assert_eq!(sql, "SELECT `id`, `email` FROM `customers` LIMIT 1000 OFFSET 2000");
    }

    #[test]
    fn full_refresh_paged_backtick_quoting() {
        let sql = QueryBuilder::build_full_refresh_query_paged(
            "order", &["id".to_string()], 100, 0,
        );
        assert!(sql.contains("FROM `order`"));
    }

    #[test]
    fn full_refresh_paged_zero_offset_first_page() {
        let sql = QueryBuilder::build_full_refresh_query_paged(
            "t", &["a".to_string()], 10000, 0,
        );
        assert!(sql.ends_with("LIMIT 10000 OFFSET 0"));
    }
}
