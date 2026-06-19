use anyhow::{Context, Result};
use deltalake::arrow::datatypes::{DataType, Schema as ArrowSchema, TimeUnit};
use deltalake::kernel::StructType;

pub(crate) fn arrow_schema_to_delta(schema: &ArrowSchema) -> Result<StructType> {
    let fields: Vec<deltalake::kernel::StructField> = schema
        .fields()
        .iter()
        .map(|f| {
            let dt = arrow_type_to_delta(f.data_type())?;
            Ok(deltalake::kernel::StructField::new(
                f.name(),
                dt,
                f.is_nullable(),
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    StructType::try_new(fields).context("failed to create Delta schema")
}

pub(crate) fn arrow_type_to_delta(dt: &DataType) -> Result<deltalake::kernel::DataType> {
    use deltalake::kernel::DataType as D;

    match dt {
        DataType::Boolean => Ok(D::BOOLEAN),
        DataType::Int8 | DataType::Int16 | DataType::Int32 => Ok(D::INTEGER),
        DataType::Int64 => Ok(D::LONG),
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 => Ok(D::INTEGER),
        DataType::UInt64 => Ok(D::LONG),
        DataType::Float16 | DataType::Float32 => Ok(D::FLOAT),
        DataType::Float64 => Ok(D::DOUBLE),
        DataType::Utf8 | DataType::LargeUtf8 => Ok(D::STRING),
        DataType::Binary | DataType::LargeBinary => Ok(D::BINARY),
        DataType::Date32 | DataType::Date64 => Ok(D::DATE),
        DataType::Timestamp(TimeUnit::Microsecond, None) => Ok(D::TIMESTAMP_NTZ),
        DataType::Timestamp(TimeUnit::Microsecond, Some(_)) => Ok(D::TIMESTAMP),
        DataType::Timestamp(TimeUnit::Millisecond, None) => Ok(D::TIMESTAMP_NTZ),
        DataType::Timestamp(TimeUnit::Millisecond, Some(_)) => Ok(D::TIMESTAMP),
        DataType::Timestamp(TimeUnit::Second, None) => Ok(D::TIMESTAMP_NTZ),
        DataType::Timestamp(TimeUnit::Second, Some(_)) => Ok(D::TIMESTAMP),
        DataType::Timestamp(TimeUnit::Nanosecond, None) => Ok(D::TIMESTAMP_NTZ),
        DataType::Timestamp(TimeUnit::Nanosecond, Some(_)) => Ok(D::TIMESTAMP),
        DataType::Decimal128(p, s) | DataType::Decimal256(p, s) => {
            let scale_u8: u8 = (*s).try_into().context("invalid decimal scale")?;
            Ok(D::decimal(*p, scale_u8)?)
        }
        _ => anyhow::bail!("unsupported Arrow type for Delta: {dt:?}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use deltalake::arrow::datatypes::Field;
    use std::sync::Arc;

    #[test]
    fn arrow_schema_to_delta_basic() {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("price", DataType::Float64, true),
        ]));

        let delta_schema = arrow_schema_to_delta(&schema).unwrap();

        let field_names: Vec<String> = delta_schema.fields().map(|f| f.name().clone()).collect();
        assert_eq!(field_names, vec!["id", "name", "price"]);
    }

    #[test]
    fn arrow_type_to_delta_conversions() {
        let r = arrow_type_to_delta(&DataType::Boolean);
        assert!(r.is_ok());
        assert_eq!(r.unwrap(), deltalake::kernel::DataType::BOOLEAN);

        let r = arrow_type_to_delta(&DataType::Int32);
        assert!(r.is_ok());
        assert_eq!(r.unwrap(), deltalake::kernel::DataType::INTEGER);

        let r = arrow_type_to_delta(&DataType::Int64);
        assert!(r.is_ok());
        assert_eq!(r.unwrap(), deltalake::kernel::DataType::LONG);

        let r = arrow_type_to_delta(&DataType::Float32);
        assert!(r.is_ok());
        assert_eq!(r.unwrap(), deltalake::kernel::DataType::FLOAT);

        let r = arrow_type_to_delta(&DataType::Float64);
        assert!(r.is_ok());
        assert_eq!(r.unwrap(), deltalake::kernel::DataType::DOUBLE);

        let r = arrow_type_to_delta(&DataType::Utf8);
        assert!(r.is_ok());
        assert_eq!(r.unwrap(), deltalake::kernel::DataType::STRING);

        let r = arrow_type_to_delta(&DataType::Date32);
        assert!(r.is_ok());
        assert_eq!(r.unwrap(), deltalake::kernel::DataType::DATE);

        let ts_result = arrow_type_to_delta(&DataType::Timestamp(TimeUnit::Microsecond, None));
        assert!(ts_result.is_ok());
        assert_eq!(ts_result.unwrap(), deltalake::kernel::DataType::TIMESTAMP_NTZ);
    }

    #[test]
    fn arrow_type_to_delta_unsupported() {
        let result = arrow_type_to_delta(&DataType::List(Arc::new(Field::new(
            "item",
            DataType::Int32,
            true,
        ))));
        assert!(result.is_err());
    }

    #[test]
    fn arrow_type_to_delta_int8() {
        assert!(matches!(
            arrow_type_to_delta(&DataType::Int8),
            Ok(deltalake::kernel::DataType::INTEGER)
        ));
    }

    #[test]
    fn arrow_type_to_delta_int16() {
        assert!(matches!(
            arrow_type_to_delta(&DataType::Int16),
            Ok(deltalake::kernel::DataType::INTEGER)
        ));
    }

    #[test]
    fn arrow_type_to_delta_uint8() {
        assert!(matches!(
            arrow_type_to_delta(&DataType::UInt8),
            Ok(deltalake::kernel::DataType::INTEGER)
        ));
    }

    #[test]
    fn arrow_type_to_delta_uint16() {
        assert!(matches!(
            arrow_type_to_delta(&DataType::UInt16),
            Ok(deltalake::kernel::DataType::INTEGER)
        ));
    }

    #[test]
    fn arrow_type_to_delta_uint32() {
        assert!(matches!(
            arrow_type_to_delta(&DataType::UInt32),
            Ok(deltalake::kernel::DataType::INTEGER)
        ));
    }

    #[test]
    fn arrow_type_to_delta_uint64() {
        assert!(matches!(
            arrow_type_to_delta(&DataType::UInt64),
            Ok(deltalake::kernel::DataType::LONG)
        ));
    }

    #[test]
    fn arrow_type_to_delta_float16() {
        assert!(matches!(
            arrow_type_to_delta(&DataType::Float16),
            Ok(deltalake::kernel::DataType::FLOAT)
        ));
    }

    #[test]
    fn arrow_type_to_delta_large_utf8() {
        assert!(matches!(
            arrow_type_to_delta(&DataType::LargeUtf8),
            Ok(deltalake::kernel::DataType::STRING)
        ));
    }

    #[test]
    fn arrow_type_to_delta_binary() {
        assert!(matches!(
            arrow_type_to_delta(&DataType::Binary),
            Ok(deltalake::kernel::DataType::BINARY)
        ));
    }

    #[test]
    fn arrow_type_to_delta_large_binary() {
        assert!(matches!(
            arrow_type_to_delta(&DataType::LargeBinary),
            Ok(deltalake::kernel::DataType::BINARY)
        ));
    }

    #[test]
    fn arrow_type_to_delta_date64() {
        assert!(matches!(
            arrow_type_to_delta(&DataType::Date64),
            Ok(deltalake::kernel::DataType::DATE)
        ));
    }

    #[test]
    fn arrow_type_to_delta_timestamp_second() {
        let result = arrow_type_to_delta(&DataType::Timestamp(TimeUnit::Second, None));
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), deltalake::kernel::DataType::TIMESTAMP_NTZ);
    }

    #[test]
    fn arrow_type_to_delta_timestamp_micros() {
        let result = arrow_type_to_delta(&DataType::Timestamp(TimeUnit::Microsecond, None));
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), deltalake::kernel::DataType::TIMESTAMP_NTZ);
    }

    #[test]
    fn arrow_type_to_delta_timestamp_millis() {
        let result = arrow_type_to_delta(&DataType::Timestamp(TimeUnit::Millisecond, None));
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), deltalake::kernel::DataType::TIMESTAMP_NTZ);
    }

    #[test]
    fn arrow_type_to_delta_timestamp_nanos() {
        let result = arrow_type_to_delta(&DataType::Timestamp(TimeUnit::Nanosecond, None));
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), deltalake::kernel::DataType::TIMESTAMP_NTZ);
    }

    #[test]
    fn arrow_type_to_delta_decimal128() {
        let result = arrow_type_to_delta(&DataType::Decimal128(10, 2));
        assert!(result.is_ok());
    }

    #[test]
    fn arrow_type_to_delta_decimal256() {
        let result = arrow_type_to_delta(&DataType::Decimal256(10, 2));
        assert!(result.is_ok());
    }

    #[test]
    fn arrow_type_to_delta_decimal_invalid_precision() {
        let result = arrow_type_to_delta(&DataType::Decimal128(0, 0));
        assert!(result.is_err());
    }

    #[test]
    fn arrow_schema_to_delta_unsupported_type() {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("data", DataType::List(Arc::new(Field::new("item", DataType::Int32, true))), false),
        ]));
        let result = arrow_schema_to_delta(&schema);
        assert!(result.is_err());
    }
}
