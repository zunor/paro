use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;

pub(crate) fn decimal_storage_width(precision: u8) -> usize {
    if precision <= 18 {
        std::mem::size_of::<i64>()
    } else {
        std::mem::size_of::<i128>()
    }
}

pub(crate) fn list_child_is_varlen(child_type: &LogicalType) -> bool {
    matches!(
        child_type,
        LogicalType::Varchar
            | LogicalType::VarcharCollation(_)
            | LogicalType::TsVector
            | LogicalType::TsQuery
            | LogicalType::Json
            | LogicalType::Jsonb
            | LogicalType::Blob
    )
}

pub(crate) fn list_child_fixed_size(child_type: &LogicalType) -> Result<usize> {
    fixed_row_width(child_type)
}

pub(crate) fn struct_field_is_varlen(field_type: &LogicalType) -> bool {
    list_child_is_varlen(field_type)
}

pub(crate) fn struct_field_fixed_size(field_type: &LogicalType) -> Result<usize> {
    fixed_row_width(field_type)
}

pub(crate) fn fixed_row_width(ty: &LogicalType) -> Result<usize> {
    let width = match ty {
        LogicalType::Boolean | LogicalType::TinyInt | LogicalType::UTinyInt => 1,
        LogicalType::SmallInt | LogicalType::USmallInt => 2,
        LogicalType::Integer | LogicalType::UInteger | LogicalType::Date => 4,
        LogicalType::BigInt
        | LogicalType::UBigInt
        | LogicalType::Time
        | LogicalType::Timestamp
        | LogicalType::TimestampTz => 8,
        LogicalType::HugeInt
        | LogicalType::UHugeInt
        | LogicalType::Uuid
        | LogicalType::Interval => 16,
        LogicalType::Float => 4,
        LogicalType::Double => 8,
        LogicalType::Null => 1,
        LogicalType::Decimal { precision, .. } => decimal_storage_width(*precision),
        LogicalType::Array(inner, dim) if matches!(**inner, LogicalType::Float) => dim
            .checked_mul(std::mem::size_of::<f32>())
            .ok_or_else(|| paro_error::data_corrupted("Array row width overflow"))?,
        other => {
            return Err(paro_error::not_supported(format!(
                "Unsupported fixed-width type in storage layout: {:?}",
                other
            )))
        }
    };
    Ok(width)
}
