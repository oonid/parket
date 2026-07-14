/// Quote a MySQL/MariaDB identifier. A backtick is a legal character inside an
/// identifier and is escaped by doubling it (S2) so an embedded backtick cannot
/// break out of the quoting.
fn backtick(ident: &str) -> String {
    format!("`{}`", ident.replace('`', "``"))
}

/// Escape a value for inline single-quoted SQL string literals by doubling embedded
/// single quotes (S2). Used for HWM values interpolated into the WHERE clause.
fn sql_str_literal(v: &str) -> String {
    v.replace('\'', "''")
}

fn format_columns(columns: &[String]) -> String {
    columns
        .iter()
        .map(|c| backtick(c))
        .collect::<Vec<_>>()
        .join(", ")
}

/// One ORDER BY term for OFFSET-paged full refresh (N8). `binary` wraps the column in `BINARY`
/// so a case-insensitive collation can't order two distinct-but-collation-equal string values
/// arbitrarily across pages (which would skip/duplicate rows). Non-string columns and
/// unique-index columns use `binary = false` (plain value order; index-usable).
pub struct OrderTerm {
    pub column: String,
    pub binary: bool,
}

fn format_order_by(terms: &[OrderTerm]) -> String {
    terms
        .iter()
        .map(|t| {
            let c = backtick(&t.column);
            if t.binary { format!("BINARY {c}") } else { c }
        })
        .collect::<Vec<_>>()
        .join(", ")
}

pub struct QueryBuilder;

impl QueryBuilder {
    pub fn build_incremental_query(
        table: &str,
        columns: &[String],
        timestamp_col: &str,
        key_col: &str,
        hwm_updated_at: Option<&str>,
        hwm_last_id: Option<i64>,
        batch_size: u64,
    ) -> String {
        let col_list = format_columns(columns);
        let quoted_table = backtick(table);
        let ts = backtick(timestamp_col);
        let key = backtick(key_col);

        match (hwm_updated_at, hwm_last_id) {
            (Some(updated_at), Some(last_id)) => {
                let updated_at = sql_str_literal(updated_at);
                format!(
                    "SELECT {col_list} FROM {quoted_table} WHERE {ts} IS NOT NULL AND (({ts} = '{updated_at}' AND {key} > {last_id}) OR ({ts} > '{updated_at}')) ORDER BY {ts} ASC, {key} ASC LIMIT {batch_size}"
                )
            }
            _ => {
                format!(
                    "SELECT {col_list} FROM {quoted_table} WHERE {ts} IS NOT NULL ORDER BY {ts} ASC, {key} ASC LIMIT {batch_size}"
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
        order_terms: &[OrderTerm],
        batch_size: u64,
        offset: u64,
    ) -> String {
        let col_list = format_columns(columns);
        let quoted_table = backtick(table);
        let order_by = format_order_by(order_terms);
        format!(
            "SELECT {col_list} FROM {quoted_table} ORDER BY {order_by} LIMIT {batch_size} OFFSET {offset}"
        )
    }

    pub fn build_full_refresh_query_keyset(
        table: &str,
        columns: &[String],
        key_col: &str,
        last_key: Option<i64>,
        batch_size: u64,
    ) -> String {
        let col_list = format_columns(columns);
        let quoted_table = backtick(table);
        let key = backtick(key_col);

        match last_key {
            Some(last_key) => format!(
                "SELECT {col_list} FROM {quoted_table} WHERE {key} > {last_key} ORDER BY {key} ASC LIMIT {batch_size}"
            ),
            None => format!(
                "SELECT {col_list} FROM {quoted_table} ORDER BY {key} ASC LIMIT {batch_size}"
            ),
        }
    }

    /// Insert-stream query: rows with key greater than the watermark, ordered by key.
    /// `key_col` is the monotonic PK (e.g. `id`). First run (None) has no WHERE.
    pub fn build_insert_stream_query(
        table: &str,
        columns: &[String],
        key_col: &str,
        hwm_id: Option<i64>,
        batch_size: u64,
    ) -> String {
        let col_list = format_columns(columns);
        let quoted_table = backtick(table);
        let key = backtick(key_col);
        match hwm_id {
            Some(id) => format!(
                "SELECT {col_list} FROM {quoted_table} WHERE {key} > {id} ORDER BY {key} ASC LIMIT {batch_size}"
            ),
            None => format!(
                "SELECT {col_list} FROM {quoted_table} ORDER BY {key} ASC LIMIT {batch_size}"
            ),
        }
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
            "id",
            Some("2026-03-28 09:00:00"),
            Some(500),
            10000,
        );

        assert!(sql.contains("SELECT `id`, `name`, `updated_at` FROM `orders`"));
        assert!(sql.contains("WHERE `updated_at` IS NOT NULL AND"));
        assert!(sql.contains("(`updated_at` = '2026-03-28 09:00:00' AND `id` > 500)"));
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
            "id",
            None,
            None,
            10000,
        );

        assert!(sql.contains("SELECT `id`, `name`, `updated_at` FROM `orders`"));
        assert!(sql.contains("WHERE `updated_at` IS NOT NULL"));
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
    fn full_refresh_paged_query_uses_stable_order() {
        let sql = QueryBuilder::build_full_refresh_query_paged(
            "products",
            &["id".to_string(), "name".to_string()],
            &[
                OrderTerm { column: "id".to_string(), binary: false },
                OrderTerm { column: "name".to_string(), binary: false },
            ],
            100,
            200,
        );

        assert_eq!(
            sql,
            "SELECT `id`, `name` FROM `products` ORDER BY `id`, `name` LIMIT 100 OFFSET 200"
        );
    }

    #[test]
    fn full_refresh_keyset_first_page() {
        let sql = QueryBuilder::build_full_refresh_query_keyset(
            "products",
            &["id".to_string(), "name".to_string()],
            "id",
            None,
            100,
        );

        assert_eq!(
            sql,
            "SELECT `id`, `name` FROM `products` ORDER BY `id` ASC LIMIT 100"
        );
    }

    #[test]
    fn full_refresh_keyset_next_page() {
        let sql = QueryBuilder::build_full_refresh_query_keyset(
            "products",
            &["id".to_string(), "name".to_string()],
            "id",
            Some(42),
            100,
        );

        assert_eq!(
            sql,
            "SELECT `id`, `name` FROM `products` WHERE `id` > 42 ORDER BY `id` ASC LIMIT 100"
        );
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
            "id",
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
            "id",
            Some("2026-01-01 00:00:00"),
            None,
            10000,
        );

        assert!(
            sql.contains("WHERE `updated_at` IS NOT NULL"),
            "Partial HWM should be treated as no HWM (with NULL filter)"
        );
        assert!(!sql.contains("AND ("), "No conditional HWM clauses");
    }

    #[test]
    fn incremental_partial_hwm_no_updated_at_treated_as_no_hwm() {
        let sql = QueryBuilder::build_incremental_query(
            "orders",
            &["id".to_string()],
            "updated_at",
            "id",
            None,
            Some(42),
            10000,
        );

        assert!(
            sql.contains("WHERE `updated_at` IS NOT NULL"),
            "Partial HWM should be treated as no HWM (with NULL filter)"
        );
        assert!(!sql.contains("AND ("), "No conditional HWM clauses");
    }

    #[test]
    fn backtick_helper() {
        assert_eq!(backtick("orders"), "`orders`");
        assert_eq!(backtick("select"), "`select`");
    }

    // S2: an identifier containing a backtick (legal in MySQL/MariaDB) must not
    // break out of the quoting — the embedded backtick is doubled.
    #[test]
    fn backtick_escapes_embedded_backtick() {
        assert_eq!(backtick("a`b"), "`a``b`");
        // A lone backtick doubles to ``, wrapped in `…` → four backticks total.
        assert_eq!(backtick("`"), "````");
    }

    #[test]
    fn backtick_escaped_column_in_query() {
        let sql = QueryBuilder::build_full_refresh_query("weird`table", &["od`d".to_string()]);
        assert_eq!(sql, "SELECT `od``d` FROM `weird``table`");
    }

    // S2: the interpolated HWM `updated_at` value must have single quotes doubled so
    // a value containing a `'` cannot break out of the string literal.
    #[test]
    fn incremental_hwm_value_escapes_single_quote() {
        let sql = QueryBuilder::build_incremental_query(
            "orders",
            &["id".to_string()],
            "updated_at",
            "id",
            Some("2026-01-01' OR '1'='1"),
            Some(42),
            5000,
        );

        // The single quotes in the value are doubled, so the literal stays intact.
        assert!(
            sql.contains("= '2026-01-01'' OR ''1''=''1'"),
            "HWM single quotes must be doubled, got: {sql}"
        );
        // No lone (un-doubled) injection-breakout remains.
        assert!(
            !sql.contains("2026-01-01' OR '1'='1"),
            "raw un-escaped value must not appear, got: {sql}"
        );
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
            "id",
            Some("2026-03-28 09:00:00"),
            Some(500),
            10000,
        );

        assert_eq!(
            sql,
            "SELECT `id`, `name`, `updated_at` FROM `orders` WHERE `updated_at` IS NOT NULL AND ((`updated_at` = '2026-03-28 09:00:00' AND `id` > 500) OR (`updated_at` > '2026-03-28 09:00:00')) ORDER BY `updated_at` ASC, `id` ASC LIMIT 10000"
        );
    }

    #[test]
    fn incremental_no_hwm_exact_output() {
        let sql = QueryBuilder::build_incremental_query(
            "orders",
            &["id".to_string()],
            "updated_at",
            "id",
            None,
            None,
            5000,
        );

        assert_eq!(
            sql,
            "SELECT `id` FROM `orders` WHERE `updated_at` IS NOT NULL ORDER BY `updated_at` ASC, `id` ASC LIMIT 5000"
        );
    }

    #[test]
    fn incremental_custom_key_column_exact_output() {
        let sql = QueryBuilder::build_incremental_query(
            "orders",
            &["order_id".to_string(), "updated_at".to_string()],
            "updated_at",
            "order_id",
            Some("2026-03-28 09:00:00"),
            Some(500),
            10000,
        );

        assert_eq!(
            sql,
            "SELECT `order_id`, `updated_at` FROM `orders` WHERE `updated_at` IS NOT NULL AND ((`updated_at` = '2026-03-28 09:00:00' AND `order_id` > 500) OR (`updated_at` > '2026-03-28 09:00:00')) ORDER BY `updated_at` ASC, `order_id` ASC LIMIT 10000"
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
            "id",
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
            "orders",
            &["id".to_string(), "name".to_string()],
            &[
                OrderTerm { column: "id".to_string(), binary: false },
                OrderTerm { column: "name".to_string(), binary: false },
            ],
            5000,
            0,
        );
        assert!(sql.contains("LIMIT 5000"));
        assert!(sql.contains("OFFSET 0"));
        assert!(!sql.contains("WHERE"));
        assert!(sql.contains("ORDER BY `id`, `name`"));
    }

    #[test]
    fn full_refresh_paged_second_page_offset() {
        let sql = QueryBuilder::build_full_refresh_query_paged(
            "orders",
            &["id".to_string()],
            &[OrderTerm { column: "id".to_string(), binary: false }],
            5000,
            5000,
        );
        assert!(sql.contains("LIMIT 5000"));
        assert!(sql.contains("OFFSET 5000"));
    }

    #[test]
    fn full_refresh_paged_exact_output() {
        let sql = QueryBuilder::build_full_refresh_query_paged(
            "customers",
            &["id".to_string(), "email".to_string()],
            &[
                OrderTerm { column: "id".to_string(), binary: false },
                OrderTerm { column: "email".to_string(), binary: false },
            ],
            1000,
            2000,
        );
        assert_eq!(
            sql,
            "SELECT `id`, `email` FROM `customers` ORDER BY `id`, `email` LIMIT 1000 OFFSET 2000"
        );
    }

    #[test]
    fn full_refresh_paged_backtick_quoting() {
        let sql = QueryBuilder::build_full_refresh_query_paged(
            "order",
            &["id".to_string()],
            &[OrderTerm { column: "id".to_string(), binary: false }],
            100,
            0,
        );
        assert!(sql.contains("FROM `order`"));
    }

    #[test]
    fn full_refresh_paged_zero_offset_first_page() {
        let sql = QueryBuilder::build_full_refresh_query_paged(
            "t",
            &["a".to_string()],
            &[OrderTerm { column: "a".to_string(), binary: false }],
            10000,
            0,
        );
        assert!(sql.ends_with("LIMIT 10000 OFFSET 0"));
    }

    #[test]
    fn full_refresh_paged_order_by_binary_string_column() {
        let sql = QueryBuilder::build_full_refresh_query_paged(
            "customers",
            &["name".to_string()],
            &[OrderTerm { column: "name".to_string(), binary: true }],
            100,
            0,
        );
        assert!(
            sql.contains("ORDER BY BINARY `name`"),
            "expected BINARY-wrapped order term, got: {sql}"
        );
    }

    #[test]
    fn full_refresh_paged_order_by_mixed_binary_and_plain_terms() {
        let sql = QueryBuilder::build_full_refresh_query_paged(
            "customers",
            &["id".to_string(), "name".to_string()],
            &[
                OrderTerm { column: "id".to_string(), binary: false },
                OrderTerm { column: "name".to_string(), binary: true },
            ],
            100,
            0,
        );
        assert!(
            sql.contains("ORDER BY `id`, BINARY `name`"),
            "expected mixed plain/BINARY order terms, got: {sql}"
        );
    }

    #[test]
    fn incremental_null_filter_in_both_branches() {
        // Test with HWM
        let sql_with_hwm = QueryBuilder::build_incremental_query(
            "orders",
            &["id".to_string()],
            "updated_at",
            "id",
            Some("2026-03-28 09:00:00"),
            Some(500),
            10000,
        );
        assert!(sql_with_hwm.contains("WHERE `updated_at` IS NOT NULL AND"));

        // Test without HWM
        let sql_without_hwm = QueryBuilder::build_incremental_query(
            "orders",
            &["id".to_string()],
            "updated_at",
            "id",
            None,
            None,
            10000,
        );
        assert!(sql_without_hwm.contains("WHERE `updated_at` IS NOT NULL"));
    }

    #[test]
    fn insert_stream_with_hwm() {
        let sql = QueryBuilder::build_insert_stream_query(
            "orders",
            &["id".to_string(), "name".to_string()],
            "id",
            Some(1000),
            5000,
        );

        assert!(sql.contains("SELECT `id`, `name` FROM `orders`"));
        assert!(sql.contains("WHERE `id` > 1000"));
        assert!(sql.contains("ORDER BY `id` ASC"));
        assert!(sql.contains("LIMIT 5000"));
    }

    #[test]
    fn insert_stream_first_run_no_hwm() {
        let sql = QueryBuilder::build_insert_stream_query(
            "orders",
            &["id".to_string(), "name".to_string()],
            "id",
            None,
            5000,
        );

        assert!(sql.contains("SELECT `id`, `name` FROM `orders`"));
        assert!(!sql.contains("WHERE"));
        assert!(sql.contains("ORDER BY `id` ASC"));
        assert!(sql.contains("LIMIT 5000"));
    }

    #[test]
    fn insert_stream_custom_key_column() {
        let sql = QueryBuilder::build_insert_stream_query(
            "orders",
            &["id".to_string()],
            "order_id",
            Some(100),
            1000,
        );

        assert!(sql.contains("WHERE `order_id` > 100"));
        assert!(sql.contains("ORDER BY `order_id` ASC"));
    }

    #[test]
    fn insert_stream_exact_output_with_hwm() {
        let sql = QueryBuilder::build_insert_stream_query(
            "events",
            &["id".to_string(), "data".to_string()],
            "id",
            Some(500),
            10000,
        );

        assert_eq!(
            sql,
            "SELECT `id`, `data` FROM `events` WHERE `id` > 500 ORDER BY `id` ASC LIMIT 10000"
        );
    }

    #[test]
    fn insert_stream_exact_output_no_hwm() {
        let sql = QueryBuilder::build_insert_stream_query(
            "events",
            &["id".to_string()],
            "id",
            None,
            1000,
        );

        assert_eq!(
            sql,
            "SELECT `id` FROM `events` ORDER BY `id` ASC LIMIT 1000"
        );
    }
}
