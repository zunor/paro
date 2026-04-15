use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_parser::ast::TypeName;

/// Convert AST TypeName to LogicalType.
pub fn bind_logical_type(data_type: &TypeName) -> Result<LogicalType> {
    match data_type {
        TypeName::Boolean => Ok(LogicalType::Boolean),
        TypeName::Int8 => Ok(LogicalType::TinyInt),
        TypeName::Int16 => Ok(LogicalType::SmallInt),
        TypeName::Int32 => Ok(LogicalType::Integer),
        TypeName::Int64 => Ok(LogicalType::BigInt),
        TypeName::HugeInt => Ok(LogicalType::HugeInt),
        TypeName::UInt8 => Ok(LogicalType::UTinyInt),
        TypeName::UInt16 => Ok(LogicalType::USmallInt),
        TypeName::UInt32 => Ok(LogicalType::UInteger),
        TypeName::UInt64 => Ok(LogicalType::UBigInt),
        TypeName::UHugeInt => Ok(LogicalType::UHugeInt),
        TypeName::Float32 => Ok(LogicalType::Float),
        TypeName::Float64 => Ok(LogicalType::Double),
        TypeName::Decimal { precision, scale } => Ok(LogicalType::Decimal {
            precision: *precision,
            scale: *scale,
        }),
        TypeName::Binary => Ok(LogicalType::Blob),
        TypeName::Uuid => Ok(LogicalType::Uuid),
        TypeName::String | TypeName::Char => Ok(LogicalType::Varchar),
        TypeName::TsVector => Ok(LogicalType::TsVector),
        TypeName::TsQuery => Ok(LogicalType::TsQuery),
        TypeName::Json => Ok(LogicalType::Json),
        TypeName::Jsonb => Ok(LogicalType::Jsonb),
        TypeName::Date => Ok(LogicalType::Date),
        TypeName::Time => Ok(LogicalType::Time),
        TypeName::Interval => Ok(LogicalType::Interval),
        TypeName::Timestamp => Ok(LogicalType::Timestamp),
        TypeName::TimestampTz => Ok(LogicalType::TimestampTz),
        TypeName::Array(child) => Ok(LogicalType::List(Box::new(bind_logical_type(child)?))),
        TypeName::List(child) => Ok(LogicalType::List(Box::new(bind_logical_type(child)?))),
        TypeName::Tuple {
            fields_name,
            fields_type,
        } => {
            let mut fields = Vec::with_capacity(fields_type.len());
            for (idx, field_type) in fields_type.iter().enumerate() {
                let name = fields_name
                    .as_ref()
                    .and_then(|names| names.get(idx))
                    .map(|ident| ident.name.clone())
                    .unwrap_or_else(|| format!("field{}", idx));
                fields.push((name, bind_logical_type(field_type)?));
            }
            Ok(LogicalType::Struct(fields))
        }
        TypeName::Vector(dimensions) => Ok(LogicalType::Array(
            Box::new(LogicalType::Float),
            *dimensions as usize,
        )),
        _ => Err(paro_error::not_implemented(format!(
            "Data type not supported: {:?}",
            data_type
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::bind_logical_type;
    use paro_common::types::LogicalType;
    use paro_parser::ast::TypeName;

    #[test]
    fn bind_logical_type_maps_primitive_integers() {
        assert_eq!(
            bind_logical_type(&TypeName::Int8).unwrap(),
            LogicalType::TinyInt
        );
        assert_eq!(
            bind_logical_type(&TypeName::Int16).unwrap(),
            LogicalType::SmallInt
        );
        assert_eq!(
            bind_logical_type(&TypeName::HugeInt).unwrap(),
            LogicalType::HugeInt
        );
        assert_eq!(
            bind_logical_type(&TypeName::UInt8).unwrap(),
            LogicalType::UTinyInt
        );
        assert_eq!(
            bind_logical_type(&TypeName::UInt16).unwrap(),
            LogicalType::USmallInt
        );
        assert_eq!(
            bind_logical_type(&TypeName::UInt32).unwrap(),
            LogicalType::UInteger
        );
        assert_eq!(
            bind_logical_type(&TypeName::UInt64).unwrap(),
            LogicalType::UBigInt
        );
        assert_eq!(
            bind_logical_type(&TypeName::UHugeInt).unwrap(),
            LogicalType::UHugeInt
        );
        assert_eq!(
            bind_logical_type(&TypeName::Float32).unwrap(),
            LogicalType::Float
        );
        assert_eq!(
            bind_logical_type(&TypeName::Float64).unwrap(),
            LogicalType::Double
        );
        assert_eq!(
            bind_logical_type(&TypeName::Decimal {
                precision: 10,
                scale: 2,
            })
            .unwrap(),
            LogicalType::Decimal {
                precision: 10,
                scale: 2,
            }
        );
        assert_eq!(
            bind_logical_type(&TypeName::Binary).unwrap(),
            LogicalType::Blob
        );
        assert_eq!(
            bind_logical_type(&TypeName::Uuid).unwrap(),
            LogicalType::Uuid
        );
        assert_eq!(
            bind_logical_type(&TypeName::Interval).unwrap(),
            LogicalType::Interval
        );
        assert_eq!(
            bind_logical_type(&TypeName::Time).unwrap(),
            LogicalType::Time
        );
        assert_eq!(
            bind_logical_type(&TypeName::TsVector).unwrap(),
            LogicalType::TsVector
        );
        assert_eq!(
            bind_logical_type(&TypeName::TsQuery).unwrap(),
            LogicalType::TsQuery
        );
        assert_eq!(
            bind_logical_type(&TypeName::TimestampTz).unwrap(),
            LogicalType::TimestampTz
        );
        assert_eq!(
            bind_logical_type(&TypeName::Json).unwrap(),
            LogicalType::Json
        );
        assert_eq!(
            bind_logical_type(&TypeName::Jsonb).unwrap(),
            LogicalType::Jsonb
        );
    }
}
