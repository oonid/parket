use std::sync::Arc;

use anyhow::Result;
use deltalake::arrow::array::Array;
use deltalake::arrow::compute::{CastOptions, cast_with_options};
use deltalake::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use deltalake::arrow::record_batch::RecordBatch;
use tracing::{error, info, warn};

use crate::discovery::ColumnInfo;

/// N5: DATA_TYPE alone (`tinyint`, `int`, ...) can't tell a signed column from its
/// unsigned counterpart — that marker only shows up in COLUMN_TYPE (e.g.
/// `int(10) unsigned`). The vendored connector_arrow maps every unsigned MariaDB integer
/// column to the matching Arrow UInt* type (verified against real MariaDB via the
/// `unsigned_probe`/`unsigned_columns_round_trip` Docker tests in tests/integration.rs):
///   tinyint unsigned   (0..=255)                    -> Arrow UInt8
///   smallint unsigned  (0..=65535)                  -> Arrow UInt16
///   mediumint unsigned (0..=16777215)                -> Arrow UInt32
///   int unsigned       (0..=4294967295)              -> Arrow UInt32
///   bigint unsigned    (0..=18446744073709551615)    -> Arrow UInt64
///
/// Delta Lake has no unsigned integer type (see `writer::schema::arrow_type_to_delta`,
/// which only ever emits INTEGER/LONG), so this widens the *Delta schema* to the
/// narrowest SIGNED Arrow type that can hold every value of that unsigned width:
///   tinyint unsigned   -> Int16   (255 fits, i8 would truncate at 127)
///   smallint unsigned  -> Int32   (65535 fits, i16 would truncate at 32767)
///   mediumint unsigned -> Int32   (16777215 fits comfortably under i32::MAX)
///   int unsigned       -> Int64   (4294967295 exceeds i32::MAX, needs 64 bits)
///   bigint unsigned    -> Int64   (no signed 64-bit type is wide enough for the full
///                                  unsigned range; values above i64::MAX
///                                  (9223372036854775807) are UNSUPPORTED and error out
///                                  at batch-cast time in `align_batch_to_schema` below
///                                  rather than silently wrapping negative or corrupting)
///
/// The physical batches extracted off the wire still arrive as the original UInt* arrays
/// though — `align_batch_to_schema` casts them to match this widened schema before they
/// reach the Delta writer.
/// N1/O8: `discovery::filter_unsupported_columns` runs on every extraction path
/// (`Orchestrator::process_table`, `PreflightCheck::check_table`) BEFORE columns ever
/// reach this function or `column_info_to_v57_schema` — it keeps a column only if its
/// DATA_TYPE is in `discovery::EXTRACTABLE_DATA_TYPES`, which mirrors the match arms
/// below exactly (kept in sync by the
/// `mariadb_type_to_arrow_covers_exactly_the_extractable_allowlist` test). So the `_ =>
/// bail!` arm below should be unreachable from those callers in normal operation — it
/// remains as defense-in-depth (e.g. direct callers/tests that construct `ColumnInfo`
/// bypassing the filter, per `column_info_to_v57_schema_unsupported_type` below) rather
/// than the primary guard. Returning a graceful `Err` here (not a panic) is what makes a
/// column that somehow evades the allowlist fail just its own table instead of the
/// process — the vendored connector_arrow's own `create_field` has no such fallback and
/// `todo!()`s (process abort) on anything it doesn't recognize.
pub(crate) fn mariadb_type_to_arrow(data_type: &str, column_type: &str) -> Result<DataType> {
    let is_unsigned = column_type.to_ascii_lowercase().contains("unsigned");
    match data_type {
        "tinyint" => Ok(if is_unsigned { DataType::Int16 } else { DataType::Int8 }),
        "smallint" => Ok(if is_unsigned { DataType::Int32 } else { DataType::Int16 }),
        "int" => Ok(if is_unsigned { DataType::Int64 } else { DataType::Int32 }),
        "mediumint" => Ok(DataType::Int32),
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

/// N5: aligns one extracted batch's columns to the (possibly widened, see
/// `mariadb_type_to_arrow`) target Delta schema. connector_arrow always emits Arrow
/// UInt8/16/32/64 for an unsigned MariaDB column; `mariadb_type_to_arrow` picks a signed
/// target wide enough to hold every value of that unsigned width EXCEPT bigint unsigned
/// values above i64::MAX, which have no signed 64-bit representation. Casting with
/// `CastOptions { safe: false, .. }` means arrow's cast kernel ERRORS on any value that
/// doesn't fit the target type instead of nulling it out (safe: true) or wrapping/
/// truncating it — so an out-of-range BIGINT UNSIGNED value fails this table loudly and
/// by name rather than silently corrupting the write. Columns whose physical Arrow type
/// already matches the target (every signed column, the overwhelming common case) are
/// left untouched — no cast call, no allocation beyond the Arc-clone below — so an
/// all-signed batch pays zero extra cost.
pub(crate) fn align_batch_to_schema(
    batch: RecordBatch,
    target: &SchemaRef,
    table_name: &str,
) -> Result<RecordBatch> {
    let source_schema = batch.schema();
    let mut changed = false;
    let mut fields: Vec<Field> = Vec::with_capacity(batch.num_columns());
    let mut columns: Vec<Arc<dyn Array>> = Vec::with_capacity(batch.num_columns());

    for (i, field) in source_schema.fields().iter().enumerate() {
        let column = batch.column(i);

        let needs_cast = matches!(
            field.data_type(),
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64
        ) && target
            .field_with_name(field.name())
            .is_ok_and(|target_field| target_field.data_type() != field.data_type());

        if !needs_cast {
            fields.push(field.as_ref().clone());
            columns.push(Arc::clone(column));
            continue;
        }

        // Safe to unwrap: `needs_cast` only becomes true after a successful
        // `field_with_name` lookup above.
        let target_field = target.field_with_name(field.name()).unwrap();
        let opts = CastOptions { safe: false, ..Default::default() };
        let casted = cast_with_options(column, target_field.data_type(), &opts).map_err(|e| {
            anyhow::anyhow!(
                "table `{table_name}` column `{}`: unsigned {:?} value does not fit the \
                 signed {:?} Delta column (Delta Lake has no unsigned integer type; this can \
                 only happen for a value above the supported range — for BIGINT UNSIGNED \
                 that range tops out at i64::MAX = 9223372036854775807): {e}",
                field.name(),
                field.data_type(),
                target_field.data_type(),
            )
        })?;

        changed = true;
        fields.push(Field::new(field.name(), target_field.data_type().clone(), field.is_nullable()));
        columns.push(casted);
    }

    if !changed {
        return Ok(batch);
    }

    let new_schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(new_schema, columns).map_err(|e| {
        anyhow::anyhow!(
            "table `{table_name}`: failed to rebuild batch after widening unsigned columns: {e}"
        )
    })
}

/// Batch-vector convenience wrapper around `align_batch_to_schema` — see its doc comment.
pub(crate) fn align_batches_to_schema(
    batches: Vec<RecordBatch>,
    target: &SchemaRef,
    table_name: &str,
) -> Result<Vec<RecordBatch>> {
    batches
        .into_iter()
        .map(|b| align_batch_to_schema(b, target, table_name))
        .collect()
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

    // D1: additive schema evolution. A column present in Delta is selected as before. A
    // column that is NEW to the source (absent from Delta) is INCLUDED in the SELECT when it
    // is extractable — its `mariadb_type_to_arrow` succeeds, equivalently it is in the
    // discovery allowlist that already gatekeeps every path into this function. The append
    // writers issue the write with `SchemaMode::Merge`, so the batch (whose schema is then a
    // superset of the Delta table's) grows the Delta table by that column; pre-existing rows
    // read back NULL. A new column that is NON-extractable stays excluded and warned,
    // consistent with the allowlist: parket never captures a type it cannot map, and the
    // drop-column / type-change bails above have already fired for the only-bailing cases.
    let mut select_columns: Vec<String> = Vec::new();
    for col in mariadb_columns {
        if delta_names.contains(col.name.as_str()) {
            select_columns.push(col.name.clone());
        } else if mariadb_type_to_arrow(&col.data_type, &col.column_type).is_ok() {
            info!(
                column = %col.name,
                "new column will be added to Delta via schema merge"
            );
            select_columns.push(col.name.clone());
        } else {
            warn!(
                column = %col.name,
                data_type = %col.data_type,
                "new column has a non-extractable type, excluding from SELECT"
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
    fn schema_evolution_new_extractable_column_is_included() {
        // D1: a column new to the source (absent from Delta) whose type is extractable is
        // now INCLUDED in the SELECT so it gets captured; SchemaMode::Merge grows the Delta
        // table by it. (Before D1 this was silently excluded + warned.)
        let mariadb_cols = vec![
            ColumnInfo { name: "id".into(), data_type: "bigint".into(), column_type: "bigint(20)".into(), nullable: false },
            ColumnInfo { name: "name".into(), data_type: "varchar".into(), column_type: "varchar(255)".into(), nullable: false },
            ColumnInfo { name: "email".into(), data_type: "varchar".into(), column_type: "varchar(255)".into(), nullable: false },
        ];
        let delta_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let result = schema_evolution_check(&mariadb_cols, &delta_schema).unwrap();
        assert_eq!(result, vec!["id", "name", "email"]);
    }

    #[test]
    fn schema_evolution_new_non_extractable_column_is_excluded() {
        // D1: a NEW source column whose type parket cannot map stays excluded (and warned) —
        // it is not in the allowlist, so it never becomes part of the merged Delta schema.
        let mariadb_cols = vec![
            ColumnInfo { name: "id".into(), data_type: "bigint".into(), column_type: "bigint(20)".into(), nullable: false },
            ColumnInfo { name: "name".into(), data_type: "varchar".into(), column_type: "varchar(255)".into(), nullable: false },
            ColumnInfo { name: "shape".into(), data_type: "geometry".into(), column_type: "geometry".into(), nullable: false },
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
            ColumnInfo { name: "id".into(), data_type: "bigint".into(), column_type: "bigint(20)".into(), nullable: false },
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
            ColumnInfo { name: "id".into(), data_type: "bigint".into(), column_type: "bigint(20)".into(), nullable: false },
            ColumnInfo { name: "name".into(), data_type: "varchar".into(), column_type: "varchar(255)".into(), nullable: false },
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
    fn mariadb_type_to_arrow_tinyint_unsigned() {
        assert!(matches!(
            mariadb_type_to_arrow("tinyint", "tinyint(3) unsigned").unwrap(),
            DataType::Int16
        ));
    }

    #[test]
    fn mariadb_type_to_arrow_smallint_unsigned() {
        assert!(matches!(
            mariadb_type_to_arrow("smallint", "smallint(5) unsigned").unwrap(),
            DataType::Int32
        ));
    }

    #[test]
    fn mariadb_type_to_arrow_mediumint_unsigned() {
        // mediumint's unsigned max (16777215) still comfortably fits Int32, same as signed.
        assert!(matches!(
            mariadb_type_to_arrow("mediumint", "mediumint(8) unsigned").unwrap(),
            DataType::Int32
        ));
    }

    #[test]
    fn mariadb_type_to_arrow_int_unsigned() {
        // int unsigned's max (4294967295) exceeds i32::MAX, so it must widen to Int64 —
        // unlike every other unsigned width, this crosses Delta's INTEGER/LONG boundary.
        assert!(matches!(
            mariadb_type_to_arrow("int", "int(10) unsigned").unwrap(),
            DataType::Int64
        ));
    }

    #[test]
    fn mariadb_type_to_arrow_bigint_unsigned_stays_int64() {
        // No signed 64-bit type can hold the full bigint-unsigned range; Int64 is the
        // widest we have. Values above i64::MAX are rejected at cast time, not here.
        assert!(matches!(
            mariadb_type_to_arrow("bigint", "bigint(20) unsigned").unwrap(),
            DataType::Int64
        ));
    }

    #[test]
    fn mariadb_type_to_arrow_signed_widths_unchanged() {
        assert!(matches!(mariadb_type_to_arrow("tinyint", "tinyint(4)").unwrap(), DataType::Int8));
        assert!(matches!(mariadb_type_to_arrow("smallint", "smallint(6)").unwrap(), DataType::Int16));
        assert!(matches!(mariadb_type_to_arrow("int", "int(11)").unwrap(), DataType::Int32));
        assert!(matches!(mariadb_type_to_arrow("mediumint", "mediumint(9)").unwrap(), DataType::Int32));
        assert!(matches!(mariadb_type_to_arrow("bigint", "bigint(20)").unwrap(), DataType::Int64));
    }

    #[test]
    fn align_batch_to_schema_all_signed_passes_through_unchanged() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("qty", DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(deltalake::arrow::array::Int64Array::from(vec![1i64])),
                Arc::new(deltalake::arrow::array::Int32Array::from(vec![2i32])),
            ],
        )
        .unwrap();
        let aligned = align_batch_to_schema(batch, &schema, "t").unwrap();
        assert_eq!(aligned.column(0).data_type(), &DataType::Int64);
        assert_eq!(aligned.column(1).data_type(), &DataType::Int32);
    }

    #[test]
    fn align_batch_to_schema_widens_uint8_to_int16() {
        let target = Arc::new(Schema::new(vec![Field::new("a", DataType::Int16, false)]));
        let source_schema = Arc::new(Schema::new(vec![Field::new("a", DataType::UInt8, false)]));
        let batch = RecordBatch::try_new(
            source_schema,
            vec![Arc::new(deltalake::arrow::array::UInt8Array::from(vec![200u8]))],
        )
        .unwrap();
        let aligned = align_batch_to_schema(batch, &target, "unsigned_probe").unwrap();
        assert_eq!(aligned.column(0).data_type(), &DataType::Int16);
        let arr = aligned.column(0).as_any().downcast_ref::<deltalake::arrow::array::Int16Array>().unwrap();
        assert_eq!(arr.value(0), 200);
    }

    #[test]
    fn align_batch_to_schema_widens_uint32_to_int64_for_int_unsigned() {
        let target = Arc::new(Schema::new(vec![Field::new("c", DataType::Int64, false)]));
        let source_schema = Arc::new(Schema::new(vec![Field::new("c", DataType::UInt32, false)]));
        let batch = RecordBatch::try_new(
            source_schema,
            vec![Arc::new(deltalake::arrow::array::UInt32Array::from(vec![3_000_000_000u32]))],
        )
        .unwrap();
        let aligned = align_batch_to_schema(batch, &target, "unsigned_probe").unwrap();
        assert_eq!(aligned.column(0).data_type(), &DataType::Int64);
        let arr = aligned.column(0).as_any().downcast_ref::<deltalake::arrow::array::Int64Array>().unwrap();
        assert_eq!(arr.value(0), 3_000_000_000i64);
    }

    #[test]
    fn align_batch_to_schema_bigint_unsigned_above_i64_max_errors_actionably() {
        let target = Arc::new(Schema::new(vec![Field::new("d", DataType::Int64, false)]));
        let source_schema = Arc::new(Schema::new(vec![Field::new("d", DataType::UInt64, false)]));
        let huge = (i64::MAX as u64) + 1;
        let batch = RecordBatch::try_new(
            source_schema,
            vec![Arc::new(deltalake::arrow::array::UInt64Array::from(vec![huge]))],
        )
        .unwrap();
        let result = align_batch_to_schema(batch, &target, "unsigned_probe");
        let err = result.expect_err("value above i64::MAX must error, not wrap/corrupt").to_string();
        assert!(err.contains("unsigned_probe"), "error must name the table, got: {err}");
        assert!(err.contains('d'), "error must name the column, got: {err}");
        assert!(
            err.contains("9223372036854775807"),
            "error must state the i64::MAX boundary, got: {err}"
        );
    }

    #[test]
    fn align_batches_to_schema_maps_over_multiple_batches() {
        let target = Arc::new(Schema::new(vec![Field::new("a", DataType::Int16, false)]));
        let source_schema = Arc::new(Schema::new(vec![Field::new("a", DataType::UInt8, false)]));
        let b1 = RecordBatch::try_new(
            source_schema.clone(),
            vec![Arc::new(deltalake::arrow::array::UInt8Array::from(vec![1u8]))],
        )
        .unwrap();
        let b2 = RecordBatch::try_new(
            source_schema,
            vec![Arc::new(deltalake::arrow::array::UInt8Array::from(vec![200u8]))],
        )
        .unwrap();
        let aligned = align_batches_to_schema(vec![b1, b2], &target, "t").unwrap();
        assert_eq!(aligned.len(), 2);
        assert_eq!(aligned[0].column(0).data_type(), &DataType::Int16);
        assert_eq!(aligned[1].column(0).data_type(), &DataType::Int16);
    }

    #[test]
    fn schema_evolution_type_change_errors() {
        let mariadb_cols = vec![
            ColumnInfo { name: "id".into(), data_type: "bigint".into(), column_type: "bigint(20)".into(), nullable: false },
            ColumnInfo { name: "age".into(), data_type: "bigint".into(), column_type: "bigint(20)".into(), nullable: false },
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
            ColumnInfo { name: "id".into(), data_type: "bigint".into(), column_type: "bigint(20)".into(), nullable: false },
            ColumnInfo { name: "location".into(), data_type: "geometry".into(), column_type: "geometry".into(), nullable: false },
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

    #[test]
    fn mariadb_type_to_arrow_covers_exactly_the_extractable_allowlist() {
        // N1/O8: keeps discovery::EXTRACTABLE_DATA_TYPES and this function's match arms
        // in sync in both directions:
        //  1. every allowlisted type must be accepted here (else a column parket itself
        //     decided was safe would still bail the table);
        //  2. known-unmapped types must stay OUT of the allowlist and still be rejected
        //     here (else the allowlist would be stale and let an unmapped type through
        //     to the connector's todo!()).
        use crate::discovery::EXTRACTABLE_DATA_TYPES;

        for dt in EXTRACTABLE_DATA_TYPES {
            assert!(
                mariadb_type_to_arrow(dt, dt).is_ok(),
                "allowlisted type '{dt}' must be accepted by mariadb_type_to_arrow"
            );
        }

        for dt in ["time", "year", "bit", "uuid", "inet4", "inet6", "geometry", "point", "vector"] {
            assert!(
                !EXTRACTABLE_DATA_TYPES.contains(&dt),
                "'{dt}' must stay out of the extractable allowlist"
            );
            assert!(
                mariadb_type_to_arrow(dt, dt).is_err(),
                "'{dt}' unexpectedly accepted by mariadb_type_to_arrow — allowlist is stale"
            );
        }
    }
}
