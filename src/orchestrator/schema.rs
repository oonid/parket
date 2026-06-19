use anyhow::Result;
use deltalake::arrow::datatypes::{DataType, SchemaRef};
use tracing::{error, warn};

use crate::discovery::ColumnInfo;

pub(crate) fn mariadb_type_to_arrow(data_type: &str, column_type: &str) -> Result<DataType> {
    match data_type {
        "tinyint" => Ok(DataType::Int8),
        "smallint" => Ok(DataType::Int16),
        "int" | "mediumint" => Ok(DataType::Int32),
        "bigint" => Ok(DataType::Int64),
        "float" => Ok(DataType::Float32),
        "double" => Ok(DataType::Float64),
        "decimal" | "numeric" => Ok(DataType::Utf8),
        "varchar" | "char" | "text" | "tinytext" | "mediumtext" | "longtext" => Ok(DataType::Utf8),
        "json" | "enum" | "set" => Ok(DataType::Utf8),
        "date" | "datetime" | "timestamp" => Ok(DataType::Utf8),
        "boolean" | "bool" => Ok(DataType::Int8),
        "blob" | "tinyblob" | "mediumblob" | "longblob" | "binary" | "varbinary" => Ok(DataType::Binary),
        _ => anyhow::bail!(
            "unsupported MariaDB type for Delta schema: {data_type} ({column_type})"
        ),
    }
}

pub(crate) fn schema_evolution_check(
    mariadb_columns: &[ColumnInfo],
    delta_schema: &SchemaRef,
) -> Result<Vec<String>> {
    let delta_names: std::collections::HashSet<&str> = delta_schema
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();

    let mariadb_names: std::collections::HashSet<&str> = mariadb_columns
        .iter()
        .map(|c| c.name.as_str())
        .collect();

    let mut errors: Vec<String> = Vec::new();

    for delta_name in &delta_names {
        if !mariadb_names.contains(delta_name) {
            errors.push(format!(
                "column {delta_name} exists in Delta but not in MariaDB — table was dropped"
            ));
        }
    }

    for col in mariadb_columns {
        if let Ok(delta_field) = delta_schema.field_with_name(&col.name) {
            let expected_dt = mariadb_type_to_arrow(&col.data_type, &col.column_type);
            match expected_dt {
                Ok(dt) => {
                    if !types_equivalent(delta_field.data_type(), &dt) {
                        errors.push(format!(
                            "column {} type changed: Delta has {:?}, MariaDB has {:?}",
                            col.name,
                            delta_field.data_type(),
                            dt
                        ));
                    }
                }
                Err(_) => {
                    warn!(
                        column = %col.name,
                        data_type = %col.data_type,
                        "skipping unsupported MariaDB type in schema evolution check"
                    );
                }
            }
        }
    }

    if !errors.is_empty() {
        for e in &errors {
            error!("{e}");
        }
        anyhow::bail!("schema evolution error: {}", errors.join(", "));
    }

    let mut select_columns: Vec<String> = Vec::new();
    for col in mariadb_columns {
        if delta_names.contains(col.name.as_str()) {
            select_columns.push(col.name.clone());
        } else {
            warn!(
                column = %col.name,
                "column exists in MariaDB but not in Delta, excluding from SELECT"
            );
        }
    }

    Ok(select_columns)
}

fn types_equivalent(delta_dt: &DataType, mariadb_dt: &DataType) -> bool {
    match (delta_dt, mariadb_dt) {
        (DataType::Timestamp(_, tz_a), DataType::Timestamp(_, tz_b)) => {
            match (tz_a.as_deref(), tz_b.as_deref()) {
                (Some("UTC"), Some("UTC")) | (None, None) => true,
                (Some("UTC"), None) | (None, Some("UTC")) => true,
                (a, b) => a == b,
            }
        }
        // Delta stores Int8/Int16/Int32/UInt8/UInt16/UInt32 all as INTEGER,
        // which round-trips back as Int32. Accept any of those widths when
        // the Delta side shows Int32.
        (DataType::Int32, DataType::Int8 | DataType::Int16 | DataType::Int32
            | DataType::UInt8 | DataType::UInt16 | DataType::UInt32) => true,
        // Similarly Int64 and UInt64 both map to Delta LONG -> Int64.
        (DataType::Int64, DataType::Int64 | DataType::UInt64) => true,
        _ => delta_dt == mariadb_dt,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use deltalake::arrow::datatypes::{Field, Schema};
    use std::sync::Arc;

    #[test]
    fn schema_evolution_column_addition_warns_and_excludes() {
        let mariadb_cols = vec![
            ColumnInfo { name: "id".into(), data_type: "bigint".into(), column_type: "bigint(20)".into() },
            ColumnInfo { name: "name".into(), data_type: "varchar".into(), column_type: "varchar(255)".into() },
            ColumnInfo { name: "email".into(), data_type: "varchar".into(), column_type: "varchar(255)".into() },
        ];
        let delta_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let result = schema_evolution_check(&mariadb_cols, &delta_schema).unwrap();
        assert_eq!(result, vec!["id", "name"]);
    }

    #[test]
    fn schema_evolution_column_drop_errors() {
        let mariadb_cols = vec![
            ColumnInfo { name: "id".into(), data_type: "bigint".into(), column_type: "bigint(20)".into() },
        ];
        let delta_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let result = schema_evolution_check(&mariadb_cols, &delta_schema);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("column name exists in Delta but not in MariaDB"));
    }

    #[test]
    fn schema_evolution_no_changes() {
        let mariadb_cols = vec![
            ColumnInfo { name: "id".into(), data_type: "bigint".into(), column_type: "bigint(20)".into() },
            ColumnInfo { name: "name".into(), data_type: "varchar".into(), column_type: "varchar(255)".into() },
        ];
        let delta_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let result = schema_evolution_check(&mariadb_cols, &delta_schema).unwrap();
        assert_eq!(result, vec!["id", "name"]);
    }

    #[test]
    fn mariadb_type_to_arrow_conversions() {
        assert!(matches!(mariadb_type_to_arrow("bigint", "bigint(20)").unwrap(), DataType::Int64));
        assert!(matches!(mariadb_type_to_arrow("int", "int(11)").unwrap(), DataType::Int32));
        assert!(matches!(mariadb_type_to_arrow("varchar", "varchar(255)").unwrap(), DataType::Utf8));
        assert!(matches!(mariadb_type_to_arrow("timestamp", "timestamp").unwrap(), DataType::Utf8));
        assert!(matches!(mariadb_type_to_arrow("double", "double").unwrap(), DataType::Float64));
        assert!(matches!(mariadb_type_to_arrow("date", "date").unwrap(), DataType::Utf8));
        assert!(matches!(mariadb_type_to_arrow("mediumtext", "mediumtext").unwrap(), DataType::Utf8));
        assert!(matches!(mariadb_type_to_arrow("enum", "enum('a','b')").unwrap(), DataType::Utf8));
        assert!(mariadb_type_to_arrow("geometry", "geometry").is_err());
    }

    #[test]
    fn mariadb_type_to_arrow_tinyint() {
        assert!(matches!(
            mariadb_type_to_arrow("tinyint", "tinyint(1)").unwrap(),
            DataType::Int8
        ));
    }

    #[test]
    fn mariadb_type_to_arrow_smallint() {
        assert!(matches!(
            mariadb_type_to_arrow("smallint", "smallint(6)").unwrap(),
            DataType::Int16
        ));
    }

    #[test]
    fn mariadb_type_to_arrow_mediumint() {
        assert!(matches!(
            mariadb_type_to_arrow("mediumint", "mediumint(7)").unwrap(),
            DataType::Int32
        ));
    }

    #[test]
    fn mariadb_type_to_arrow_float() {
        assert!(matches!(
            mariadb_type_to_arrow("float", "float").unwrap(),
            DataType::Float32
        ));
    }

    #[test]
    fn mariadb_type_to_arrow_decimal() {
        assert!(matches!(
            mariadb_type_to_arrow("decimal", "decimal(10,2)").unwrap(),
            DataType::Utf8
        ));
    }

    #[test]
    fn mariadb_type_to_arrow_json() {
        assert!(matches!(
            mariadb_type_to_arrow("json", "json").unwrap(),
            DataType::Utf8
        ));
    }

    #[test]
    fn mariadb_type_to_arrow_char() {
        assert!(matches!(
            mariadb_type_to_arrow("char", "char(10)").unwrap(),
            DataType::Utf8
        ));
    }

    #[test]
    fn mariadb_type_to_arrow_text() {
        assert!(matches!(
            mariadb_type_to_arrow("text", "text").unwrap(),
            DataType::Utf8
        ));
    }

    #[test]
    fn mariadb_type_to_arrow_datetime() {
        assert!(matches!(
            mariadb_type_to_arrow("datetime", "datetime").unwrap(),
            DataType::Utf8
        ));
    }

    #[test]
    fn mariadb_type_to_arrow_bool() {
        assert!(matches!(
            mariadb_type_to_arrow("bool", "bool").unwrap(),
            DataType::Int8
        ));
    }

    #[test]
    fn mariadb_type_to_arrow_blob() {
        assert!(matches!(
            mariadb_type_to_arrow("blob", "blob").unwrap(),
            DataType::Binary
        ));
    }

    #[test]
    fn schema_evolution_type_change_errors() {
        let mariadb_cols = vec![
            ColumnInfo { name: "id".into(), data_type: "bigint".into(), column_type: "bigint(20)".into() },
            ColumnInfo { name: "age".into(), data_type: "bigint".into(), column_type: "bigint(20)".into() },
        ];
        let delta_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("age", DataType::Int32, false),
        ]));
        let result = schema_evolution_check(&mariadb_cols, &delta_schema);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("type changed"),
            "expected type change error, got: {err}"
        );
    }

    #[test]
    fn schema_evolution_unsupported_type_in_existing_column() {
        let mariadb_cols = vec![
            ColumnInfo { name: "id".into(), data_type: "bigint".into(), column_type: "bigint(20)".into() },
            ColumnInfo { name: "location".into(), data_type: "geometry".into(), column_type: "geometry".into() },
        ];
        let delta_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("location", DataType::Binary, false),
        ]));

        let result = schema_evolution_check(&mariadb_cols, &delta_schema).unwrap();
        assert_eq!(result, vec!["id".to_string(), "location".to_string()]);
    }

    #[tokio::test]
    async fn schema_evolution_integration_with_existing_delta_table() {
        // This test is in orchestrator.rs as it tests the full orchestrator flow
        // Placeholder kept for tracking that it exists in the main tests
    }
}
