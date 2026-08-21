// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Key domains supported by direct-addressing aggregate hash tables.

use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;
use paro_common::types::{LogicalType, StringView};
use paro_common::vector::DecodedVectorRef;
use paro_storage::statistics::{BaseStatistics, StringStats};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PerfectHashKeyDomain {
    logical_type: LogicalType,
    codec: PerfectHashKeyCodec,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PerfectHashKeyCodec {
    Integer,
    SingleByteVarchar,
}

impl PerfectHashKeyDomain {
    pub(crate) fn try_new(logical_type: LogicalType) -> Option<Self> {
        let codec = if logical_type.is_integer() {
            PerfectHashKeyCodec::Integer
        } else if logical_type == LogicalType::Varchar {
            PerfectHashKeyCodec::SingleByteVarchar
        } else {
            return None;
        };
        Some(Self {
            logical_type,
            codec,
        })
    }

    pub(crate) fn logical_type(&self) -> &LogicalType {
        &self.logical_type
    }

    pub(crate) fn min_max_from_stats(
        &self,
        stats: Option<&BaseStatistics>,
    ) -> Option<(i128, i128)> {
        match self.codec {
            PerfectHashKeyCodec::Integer => stats
                .and_then(integer_min_max_from_stats)
                .or_else(|| integer_type_bounds(&self.logical_type)),
            PerfectHashKeyCodec::SingleByteVarchar => single_byte_varchar_min_max(stats?),
        }
    }

    pub(crate) fn encode_decoded(
        &self,
        group: &DecodedVectorRef<'_>,
        physical_idx: usize,
    ) -> Result<i128> {
        match &self.logical_type {
            LogicalType::TinyInt => {
                Ok(unsafe { *group.get_data::<i8>().add(physical_idx) } as i128)
            }
            LogicalType::SmallInt => {
                Ok(unsafe { *group.get_data::<i16>().add(physical_idx) } as i128)
            }
            LogicalType::Integer => {
                Ok(unsafe { *group.get_data::<i32>().add(physical_idx) } as i128)
            }
            LogicalType::BigInt => {
                Ok(unsafe { *group.get_data::<i64>().add(physical_idx) } as i128)
            }
            LogicalType::HugeInt => Ok(unsafe { *group.get_data::<i128>().add(physical_idx) }),
            LogicalType::UTinyInt => {
                Ok(unsafe { *group.get_data::<u8>().add(physical_idx) } as i128)
            }
            LogicalType::USmallInt => {
                Ok(unsafe { *group.get_data::<u16>().add(physical_idx) } as i128)
            }
            LogicalType::UInteger => {
                Ok(unsafe { *group.get_data::<u32>().add(physical_idx) } as i128)
            }
            LogicalType::UBigInt => {
                Ok(unsafe { *group.get_data::<u64>().add(physical_idx) } as i128)
            }
            LogicalType::UHugeInt => {
                let value = unsafe { *group.get_data::<u128>().add(physical_idx) };
                i128::try_from(value).map_err(|_| {
                    paro_error::internal(format!(
                        "UHUGEINT key exceeds the perfect-hash domain: {value}"
                    ))
                })
            }
            LogicalType::Varchar => {
                let value = unsafe { *group.get_data::<StringView>().add(physical_idx) };
                encode_single_byte_varchar(value.as_bytes()).ok_or_else(|| {
                    paro_error::internal(format!(
                        "Single-byte perfect-hash key received VARCHAR length {}",
                        value.len()
                    ))
                })
            }
            ty => Err(paro_error::internal(format!(
                "Unsupported perfect-hash key type: {ty:?}"
            ))),
        }
    }

    pub(crate) fn value_from_encoded(&self, value: i128) -> Result<Value> {
        match &self.logical_type {
            LogicalType::TinyInt => Ok(Value::TinyInt(checked_decode(value, "TINYINT")?)),
            LogicalType::SmallInt => Ok(Value::SmallInt(checked_decode(value, "SMALLINT")?)),
            LogicalType::Integer => Ok(Value::Integer(checked_decode(value, "INTEGER")?)),
            LogicalType::BigInt => Ok(Value::BigInt(checked_decode(value, "BIGINT")?)),
            LogicalType::HugeInt => Ok(Value::HugeInt(value)),
            LogicalType::UTinyInt => Ok(Value::UTinyInt(checked_decode(value, "UTINYINT")?)),
            LogicalType::USmallInt => Ok(Value::USmallInt(checked_decode(value, "USMALLINT")?)),
            LogicalType::UInteger => Ok(Value::UInteger(checked_decode(value, "UINTEGER")?)),
            LogicalType::UBigInt => Ok(Value::UBigInt(checked_decode(value, "UBIGINT")?)),
            LogicalType::UHugeInt => Ok(Value::UHugeInt(checked_decode(value, "UHUGEINT")?)),
            LogicalType::Varchar => decode_single_byte_varchar(value),
            ty => Err(paro_error::internal(format!(
                "Unsupported perfect-hash key type: {ty:?}"
            ))),
        }
    }
}

fn checked_decode<T>(value: i128, type_name: &str) -> Result<T>
where
    T: TryFrom<i128>,
{
    T::try_from(value).map_err(|_| {
        paro_error::internal(format!(
            "Decoded perfect-hash value is outside {type_name}: {value}"
        ))
    })
}

fn integer_min_max_from_stats(stats: &BaseStatistics) -> Option<(i128, i128)> {
    let min = stats.min_value().and_then(|value| integer_value(&value))?;
    let max = stats.max_value().and_then(|value| integer_value(&value))?;
    Some((min, max))
}

fn integer_value(value: &Value) -> Option<i128> {
    match value {
        Value::TinyInt(v) => Some(*v as i128),
        Value::SmallInt(v) => Some(*v as i128),
        Value::Integer(v) => Some(*v as i128),
        Value::BigInt(v) => Some(*v as i128),
        Value::HugeInt(v) => Some(*v),
        Value::UTinyInt(v) => Some(*v as i128),
        Value::USmallInt(v) => Some(*v as i128),
        Value::UInteger(v) => Some(*v as i128),
        Value::UBigInt(v) => Some(*v as i128),
        Value::UHugeInt(v) => i128::try_from(*v).ok(),
        _ => None,
    }
}

fn integer_type_bounds(ty: &LogicalType) -> Option<(i128, i128)> {
    match ty {
        LogicalType::TinyInt => Some((i8::MIN as i128, i8::MAX as i128)),
        LogicalType::SmallInt => Some((i16::MIN as i128, i16::MAX as i128)),
        LogicalType::Integer => Some((i32::MIN as i128, i32::MAX as i128)),
        LogicalType::BigInt => Some((i64::MIN as i128, i64::MAX as i128)),
        LogicalType::UTinyInt => Some((0, u8::MAX as i128)),
        LogicalType::USmallInt => Some((0, u16::MAX as i128)),
        LogicalType::UInteger => Some((0, u32::MAX as i128)),
        LogicalType::UBigInt => Some((0, u64::MAX as i128)),
        _ => None,
    }
}

fn single_byte_varchar_min_max(stats: &BaseStatistics) -> Option<(i128, i128)> {
    let string_stats = StringStats::get_data(stats)?;
    if string_stats.max_string_length()? > 1 {
        return None;
    }
    let min = encode_single_byte_varchar(string_stats.min_bytes())?;
    let max = encode_single_byte_varchar(string_stats.max_bytes())?;
    (min <= max).then_some((min, max))
}

fn encode_single_byte_varchar(value: &[u8]) -> Option<i128> {
    match value {
        [] => Some(0),
        [byte] => Some(i128::from(*byte) + 1),
        _ => None,
    }
}

fn decode_single_byte_varchar(value: i128) -> Result<Value> {
    match value {
        0 => Ok(Value::Varchar(String::new())),
        1..=256 => {
            let byte = u8::try_from(value - 1).map_err(|_| {
                paro_error::internal(format!(
                    "Decoded value is outside the single-byte VARCHAR domain: {value}"
                ))
            })?;
            let text = std::str::from_utf8(std::slice::from_ref(&byte)).map_err(|_| {
                paro_error::internal(format!(
                    "Decoded single-byte VARCHAR is not valid UTF-8: {byte}"
                ))
            })?;
            Ok(Value::Varchar(text.to_string()))
        }
        _ => Err(paro_error::internal(format!(
            "Decoded value is outside the single-byte VARCHAR domain: {value}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_byte_varchar_domain_roundtrips() {
        let domain = PerfectHashKeyDomain::try_new(LogicalType::Varchar).unwrap();
        assert_eq!(
            domain.value_from_encoded(0).unwrap(),
            Value::Varchar(String::new())
        );
        assert_eq!(
            domain.value_from_encoded(i128::from(b'R') + 1).unwrap(),
            Value::Varchar("R".to_string())
        );
        assert!(domain.value_from_encoded(257).is_err());
    }

    #[test]
    fn single_byte_varchar_stats_form_a_compact_domain() {
        let domain = PerfectHashKeyDomain::try_new(LogicalType::Varchar).unwrap();
        let mut stats = StringStats::create_empty(LogicalType::Varchar);
        StringStats::update(&mut stats, "A");
        StringStats::update(&mut stats, "R");
        assert_eq!(domain.min_max_from_stats(Some(&stats)), Some((66, 83)));

        StringStats::update(&mut stats, "AB");
        assert_eq!(domain.min_max_from_stats(Some(&stats)), None);
    }
}
