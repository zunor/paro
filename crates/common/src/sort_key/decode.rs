// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Reconstruction of SQL values from normalized sort keys.
//!
//! Sort keys are the authoritative row representation only when every field is
//! losslessly reversible. Floating-point keys deliberately canonicalize signed
//! zero and NaN, so callers must retain the original row for those types.

use crate::chunk::Chunk;
use crate::error::{self as paro_error, Result};
use crate::runtime_value::Value;
use crate::types::LogicalType;
use crate::vector::Vector;

use super::{fixed_value_len, SortKeyEncoding, SortKeyFieldEncoding};

/// Reusable decoder for one sort-key encoding and output projection.
#[derive(Debug)]
pub struct SortKeyDecoder<'a> {
    encoding: &'a SortKeyEncoding,
    field_offsets: Vec<usize>,
    output_columns: Vec<usize>,
    variable_scratch: Vec<u8>,
}

impl SortKeyEncoding {
    /// Whether normalized keys preserve every observable value bit needed to
    /// reconstruct projected SQL values.
    #[inline]
    pub fn can_reconstruct_values(&self) -> bool {
        self.fields
            .iter()
            .all(|field| !matches!(field.logical_type, LogicalType::Float | LogicalType::Double))
    }

    /// Prepare a decoder from sort-key field indexes to output column indexes.
    pub fn decoder<'a>(
        &'a self,
        output: &Chunk,
        projection: &[(usize, usize)],
    ) -> Result<SortKeyDecoder<'a>> {
        SortKeyDecoder::new(self, output, projection)
    }
}

impl<'a> SortKeyDecoder<'a> {
    fn new(
        encoding: &'a SortKeyEncoding,
        output: &Chunk,
        projection: &[(usize, usize)],
    ) -> Result<Self> {
        let field_count = encoding.fields.len();
        let mut field_counts = vec![0usize; field_count];
        let mut claimed_outputs = vec![false; output.column_count()];

        for &(field_idx, output_col_idx) in projection {
            let field = encoding.fields.get(field_idx).ok_or_else(|| {
                paro_error::internal(format!(
                    "sort-key decode field {field_idx} out of bounds {field_count}"
                ))
            })?;
            let output_vector = output.column(output_col_idx).ok_or_else(|| {
                paro_error::internal(format!(
                    "sort-key decode output column {output_col_idx} out of bounds {}",
                    output.column_count()
                ))
            })?;
            if output_vector.logical_type() != &field.logical_type {
                return Err(paro_error::internal(format!(
                    "sort-key decode type mismatch for field {field_idx}: encoded {:?}, output {:?}",
                    field.logical_type,
                    output_vector.logical_type()
                )));
            }
            if std::mem::replace(&mut claimed_outputs[output_col_idx], true) {
                return Err(paro_error::internal(format!(
                    "sort-key decode output column {output_col_idx} has multiple sources"
                )));
            }
            field_counts[field_idx] += 1;
        }

        let mut field_offsets = Vec::with_capacity(field_count + 1);
        field_offsets.push(0);
        for count in field_counts {
            field_offsets.push(field_offsets.last().copied().unwrap_or(0) + count);
        }

        let mut next_offsets = field_offsets[..field_count].to_vec();
        let mut output_columns = vec![0usize; projection.len()];
        for &(field_idx, output_col_idx) in projection {
            let offset = next_offsets[field_idx];
            output_columns[offset] = output_col_idx;
            next_offsets[field_idx] += 1;
        }

        Ok(Self {
            encoding,
            field_offsets,
            output_columns,
            variable_scratch: Vec::new(),
        })
    }

    /// Decode one complete normalized key into `output_position`.
    pub fn decode_row(
        &mut self,
        encoded: &[u8],
        output: &mut Chunk,
        output_position: usize,
    ) -> Result<()> {
        if output_position >= output.capacity() {
            return Err(paro_error::internal(format!(
                "sort-key decode output position {output_position} exceeds capacity {}",
                output.capacity()
            )));
        }

        let mut reader = KeyReader::new(encoded);
        for (field_idx, field) in self.encoding.fields.iter().enumerate() {
            let marker = reader.read_byte("field marker")?;
            let (null_marker, valid_marker) = if field.modifiers.nulls_first {
                (super::NULL_FIRST_BYTE, super::NULL_LAST_BYTE)
            } else {
                (super::NULL_LAST_BYTE, super::NULL_FIRST_BYTE)
            };

            if marker == null_marker {
                if let Some(width) = fixed_value_len(&field.logical_type)? {
                    let padding = reader.read_exact(width, "NULL field padding")?;
                    if padding.iter().any(|byte| *byte != 0) {
                        return Err(corrupt("fixed-width NULL field has non-zero padding"));
                    }
                }
                self.write_null(field_idx, output, output_position)?;
                continue;
            }
            if marker != valid_marker {
                return Err(corrupt(format!(
                    "invalid marker {marker} for sort-key field {field_idx}"
                )));
            }

            self.decode_value(field_idx, field, &mut reader, output, output_position)?;
        }

        if reader.remaining() != 0 {
            return Err(corrupt(format!(
                "sort key has {} trailing bytes after {} fields",
                reader.remaining(),
                self.encoding.fields.len()
            )));
        }
        Ok(())
    }

    fn decode_value(
        &mut self,
        field_idx: usize,
        field: &SortKeyFieldEncoding,
        reader: &mut KeyReader<'_>,
        output: &mut Chunk,
        output_position: usize,
    ) -> Result<()> {
        let ascending = field.modifiers.ascending;
        match &field.logical_type {
            LogicalType::Boolean => {
                let byte = read_ordered::<1>(reader, ascending)?[0];
                let value = match byte {
                    0 => false,
                    1 => true,
                    other => return Err(corrupt(format!("invalid encoded boolean {other}"))),
                };
                self.write(field_idx, output, |vector| {
                    vector.set_bool(output_position, value);
                    Ok(())
                })
            }
            LogicalType::TinyInt => {
                let value = (read_ordered::<1>(reader, ascending)?[0] ^ 0x80) as i8;
                self.write(field_idx, output, |vector| {
                    vector.set_i8(output_position, value);
                    Ok(())
                })
            }
            LogicalType::SmallInt => {
                let value = decode_i16(reader, ascending)?;
                self.write(field_idx, output, |vector| {
                    vector.set_i16(output_position, value);
                    Ok(())
                })
            }
            LogicalType::Integer | LogicalType::Date => {
                let value = decode_i32(reader, ascending)?;
                self.write(field_idx, output, |vector| {
                    vector.set_i32(output_position, value);
                    Ok(())
                })
            }
            LogicalType::BigInt
            | LogicalType::Time
            | LogicalType::Timestamp
            | LogicalType::TimestampTz => {
                let value = decode_i64(reader, ascending)?;
                self.write(field_idx, output, |vector| {
                    vector.set_i64(output_position, value);
                    Ok(())
                })
            }
            LogicalType::HugeInt => {
                let value = decode_i128(reader, ascending)?;
                self.write(field_idx, output, |vector| {
                    vector.set_i128(output_position, value);
                    Ok(())
                })
            }
            LogicalType::UTinyInt => {
                let value = read_ordered::<1>(reader, ascending)?[0];
                self.write(field_idx, output, |vector| {
                    vector.set_u8(output_position, value);
                    Ok(())
                })
            }
            LogicalType::USmallInt => {
                let value = u16::from_be_bytes(read_ordered(reader, ascending)?);
                self.write(field_idx, output, |vector| {
                    vector.set_u16(output_position, value);
                    Ok(())
                })
            }
            LogicalType::UInteger => {
                let value = u32::from_be_bytes(read_ordered(reader, ascending)?);
                self.write(field_idx, output, |vector| {
                    vector.set_u32(output_position, value);
                    Ok(())
                })
            }
            LogicalType::UBigInt => {
                let value = u64::from_be_bytes(read_ordered(reader, ascending)?);
                self.write(field_idx, output, |vector| {
                    vector.set_u64(output_position, value);
                    Ok(())
                })
            }
            LogicalType::UHugeInt | LogicalType::Uuid => {
                let value = decode_u128(reader, ascending)?;
                self.write(field_idx, output, |vector| {
                    vector.set_u128(output_position, value);
                    Ok(())
                })
            }
            LogicalType::Float => {
                let value = decode_f32(reader, ascending)?;
                self.write(field_idx, output, |vector| {
                    vector.set_f32(output_position, value);
                    Ok(())
                })
            }
            LogicalType::Double => {
                let value = decode_f64(reader, ascending)?;
                self.write(field_idx, output, |vector| {
                    vector.set_f64(output_position, value);
                    Ok(())
                })
            }
            LogicalType::Interval => {
                let value = Value::Interval(
                    decode_i32(reader, ascending)?,
                    decode_i32(reader, ascending)?,
                    decode_i64(reader, ascending)?,
                );
                self.write(field_idx, output, |vector| {
                    if vector.try_set_scalar_value(output_position, &value)? {
                        Ok(())
                    } else {
                        Err(paro_error::internal(
                            "interval unexpectedly required nested vector decoding",
                        ))
                    }
                })
            }
            LogicalType::Decimal { precision, .. } if *precision <= 18 => {
                let value = decode_i64(reader, ascending)?;
                self.write(field_idx, output, |vector| {
                    vector.set_i64(output_position, value);
                    Ok(())
                })
            }
            LogicalType::Decimal { .. } => {
                let value = decode_i128(reader, ascending)?;
                self.write(field_idx, output, |vector| {
                    vector.set_i128(output_position, value);
                    Ok(())
                })
            }
            LogicalType::Varchar
            | LogicalType::VarcharCollation(_)
            | LogicalType::TsVector
            | LogicalType::TsQuery
            | LogicalType::Json
            | LogicalType::Jsonb => {
                decode_string(reader, ascending, &mut self.variable_scratch)?;
                let value = std::str::from_utf8(&self.variable_scratch)
                    .map_err(|_| corrupt("decoded sort-key string is not UTF-8"))?;
                self.write(field_idx, output, |vector| {
                    vector.try_set_string(output_position, value)
                })
            }
            LogicalType::Blob => {
                decode_blob(reader, ascending, &mut self.variable_scratch)?;
                let value = &self.variable_scratch;
                self.write(field_idx, output, |vector| {
                    vector.try_set_blob(output_position, value)
                })
            }
            other => Err(paro_error::not_implemented(format!(
                "sort-key decoding not implemented for type {other:?}"
            ))),
        }
    }

    fn write_null(
        &self,
        field_idx: usize,
        output: &mut Chunk,
        output_position: usize,
    ) -> Result<()> {
        self.write(field_idx, output, |vector| {
            vector.try_set_null(output_position, true)
        })
    }

    fn write(
        &self,
        field_idx: usize,
        output: &mut Chunk,
        mut write: impl FnMut(&mut Vector) -> Result<()>,
    ) -> Result<()> {
        let start = self.field_offsets[field_idx];
        let end = self.field_offsets[field_idx + 1];
        for &output_col_idx in &self.output_columns[start..end] {
            let vector = output.column_mut(output_col_idx).ok_or_else(|| {
                paro_error::internal(format!(
                    "sort-key output column {output_col_idx} disappeared during decoding"
                ))
            })?;
            write(vector)?;
        }
        Ok(())
    }
}

struct KeyReader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> KeyReader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn read_byte(&mut self, context: &str) -> Result<u8> {
        Ok(self.read_exact(1, context)?[0])
    }

    fn read_exact(&mut self, len: usize, context: &str) -> Result<&'a [u8]> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or_else(|| corrupt("sort-key decode offset overflow"))?;
        let bytes = self.bytes.get(self.offset..end).ok_or_else(|| {
            corrupt(format!(
                "truncated sort key while reading {context}: need {len}, remaining {}",
                self.remaining()
            ))
        })?;
        self.offset = end;
        Ok(bytes)
    }

    fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.offset)
    }
}

fn read_ordered<const N: usize>(reader: &mut KeyReader<'_>, ascending: bool) -> Result<[u8; N]> {
    let mut bytes = [0u8; N];
    bytes.copy_from_slice(reader.read_exact(N, "fixed-width value")?);
    if !ascending {
        for byte in &mut bytes {
            *byte = !*byte;
        }
    }
    Ok(bytes)
}

fn decode_i16(reader: &mut KeyReader<'_>, ascending: bool) -> Result<i16> {
    Ok((u16::from_be_bytes(read_ordered(reader, ascending)?) ^ 0x8000) as i16)
}

fn decode_i32(reader: &mut KeyReader<'_>, ascending: bool) -> Result<i32> {
    Ok((u32::from_be_bytes(read_ordered(reader, ascending)?) ^ 0x8000_0000) as i32)
}

fn decode_i64(reader: &mut KeyReader<'_>, ascending: bool) -> Result<i64> {
    Ok((u64::from_be_bytes(read_ordered(reader, ascending)?) ^ 0x8000_0000_0000_0000) as i64)
}

fn decode_i128(reader: &mut KeyReader<'_>, ascending: bool) -> Result<i128> {
    let high = decode_i64(reader, ascending)? as i128;
    let low = u64::from_be_bytes(read_ordered(reader, ascending)?) as i128;
    Ok((high << 64) | low)
}

fn decode_u128(reader: &mut KeyReader<'_>, ascending: bool) -> Result<u128> {
    let high = u64::from_be_bytes(read_ordered(reader, ascending)?) as u128;
    let low = u64::from_be_bytes(read_ordered(reader, ascending)?) as u128;
    Ok((high << 64) | low)
}

fn decode_f32(reader: &mut KeyReader<'_>, ascending: bool) -> Result<f32> {
    let encoded = u32::from_be_bytes(read_ordered(reader, ascending)?);
    Ok(match encoded {
        0 => f32::NEG_INFINITY,
        u32::MAX => f32::NAN,
        value if value == u32::MAX - 1 => f32::INFINITY,
        0x8000_0000 => 0.0,
        value if value & 0x8000_0000 != 0 => f32::from_bits(value & 0x7fff_ffff),
        value => f32::from_bits(!value),
    })
}

fn decode_f64(reader: &mut KeyReader<'_>, ascending: bool) -> Result<f64> {
    let encoded = u64::from_be_bytes(read_ordered(reader, ascending)?);
    Ok(match encoded {
        0 => f64::NEG_INFINITY,
        u64::MAX => f64::NAN,
        value if value == u64::MAX - 1 => f64::INFINITY,
        0x8000_0000_0000_0000 => 0.0,
        value if value & 0x8000_0000_0000_0000 != 0 => {
            f64::from_bits(value & 0x7fff_ffff_ffff_ffff)
        }
        value => f64::from_bits(!value),
    })
}

fn decode_string(reader: &mut KeyReader<'_>, ascending: bool, output: &mut Vec<u8>) -> Result<()> {
    output.clear();
    loop {
        let byte = normalize_variable_byte(reader.read_byte("string byte")?, ascending);
        if byte == super::STRING_DELIMITER {
            return Ok(());
        }
        output.push(byte.wrapping_sub(1));
    }
}

fn decode_blob(reader: &mut KeyReader<'_>, ascending: bool, output: &mut Vec<u8>) -> Result<()> {
    output.clear();
    loop {
        let byte = normalize_variable_byte(reader.read_byte("blob byte")?, ascending);
        if byte == super::STRING_DELIMITER {
            return Ok(());
        }
        if byte == super::BLOB_ESCAPE_CHARACTER {
            let escaped = normalize_variable_byte(reader.read_byte("blob escape")?, ascending);
            if escaped > super::BLOB_ESCAPE_CHARACTER {
                return Err(corrupt(format!(
                    "invalid escaped blob byte {escaped} in sort key"
                )));
            }
            output.push(escaped);
        } else {
            output.push(byte);
        }
    }
}

#[inline]
fn normalize_variable_byte(byte: u8, ascending: bool) -> u8 {
    if ascending {
        byte
    } else {
        !byte
    }
}

fn corrupt(message: impl Into<String>) -> crate::error::ParoError {
    paro_error::data_corrupted(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sort_key::OrderModifiers;

    fn roundtrip(ty: LogicalType, value: Value, ascending: bool) -> Value {
        let mut input = crate::test_utils::test_vector_with_capacity(ty.clone(), 1);
        input.set_value(0, &value);
        input.set_count(1);
        let input = crate::test_utils::test_chunk_from_vectors(vec![input]);
        let encoding = SortKeyEncoding::new(
            vec![ty.clone()],
            vec![OrderModifiers::new(ascending, false)],
        )
        .unwrap();
        let key = encoding.encode_row(&input, 0, &[0]).unwrap();
        let mut output = crate::test_utils::test_chunk_with_capacity(&[ty], 1);
        let mut decoder = encoding.decoder(&output, &[(0, 0)]).unwrap();
        decoder.decode_row(&key, &mut output, 0).unwrap();
        output.set_cardinality(1);
        output.get_value(0, 0).unwrap()
    }

    #[test]
    fn roundtrips_lossless_fixed_width_types_in_both_orders() {
        let cases = [
            (LogicalType::Boolean, Value::Boolean(true)),
            (LogicalType::TinyInt, Value::TinyInt(-73)),
            (LogicalType::SmallInt, Value::SmallInt(-12_345)),
            (LogicalType::Integer, Value::Integer(-123_456_789)),
            (LogicalType::BigInt, Value::BigInt(-9_876_543_210)),
            (LogicalType::HugeInt, Value::HugeInt(-(1i128 << 100) + 17)),
            (LogicalType::UTinyInt, Value::UTinyInt(201)),
            (LogicalType::USmallInt, Value::USmallInt(54_321)),
            (LogicalType::UInteger, Value::UInteger(3_456_789_012)),
            (
                LogicalType::UBigInt,
                Value::UBigInt(15_000_000_000_000_000_000),
            ),
            (LogicalType::UHugeInt, Value::UHugeInt((1u128 << 120) + 91)),
            (LogicalType::Uuid, Value::Uuid((1u128 << 121) + 27)),
            (LogicalType::Date, Value::Date(-12_345)),
            (LogicalType::Time, Value::Time(45_678_901)),
            (LogicalType::Timestamp, Value::Timestamp(-98_765_432_100)),
            (LogicalType::TimestampTz, Value::TimestampTz(87_654_321_000)),
            (LogicalType::Interval, Value::Interval(-17, 23, -45_678_901)),
            (
                LogicalType::Decimal {
                    precision: 12,
                    scale: 3,
                },
                Value::Decimal(-987_654_321, 12, 3),
            ),
            (
                LogicalType::Decimal {
                    precision: 30,
                    scale: 7,
                },
                Value::Decimal(-(1i128 << 90) + 123, 30, 7),
            ),
        ];

        for ascending in [true, false] {
            for (ty, value) in &cases {
                assert_eq!(roundtrip(ty.clone(), value.clone(), ascending), *value);
            }
        }
    }

    #[test]
    fn roundtrips_strings_and_escaped_blobs_in_both_orders() {
        let cases = [
            (LogicalType::Varchar, Value::Varchar("héllo 世界".into())),
            (
                LogicalType::VarcharCollation("NOCASE".into()),
                Value::Varchar("MiXeD".into()),
            ),
            (
                LogicalType::TsVector,
                Value::Varchar("alpha:1 beta:2".into()),
            ),
            (LogicalType::TsQuery, Value::Varchar("alpha & beta".into())),
            (LogicalType::Json, Value::Varchar("{\"x\":1}".into())),
            (LogicalType::Jsonb, Value::Varchar("{\"x\":1}".into())),
            (LogicalType::Blob, Value::Blob(vec![0, 1, 2, 255, 1, 0])),
        ];

        for ascending in [true, false] {
            for (ty, value) in &cases {
                assert_eq!(roundtrip(ty.clone(), value.clone(), ascending), *value);
            }
        }
    }

    #[test]
    fn floating_point_decode_matches_key_canonicalization() {
        assert_eq!(
            roundtrip(LogicalType::Float, Value::Float(-0.0), true),
            Value::Float(0.0)
        );
        let decoded = roundtrip(
            LogicalType::Double,
            Value::Double(f64::from_bits(0x7ff8_0000_0000_1234)),
            false,
        );
        assert!(matches!(decoded, Value::Double(value) if value.is_nan()));

        let encoding = SortKeyEncoding::new(
            vec![LogicalType::Integer, LogicalType::Double],
            vec![OrderModifiers::new(true, false); 2],
        )
        .unwrap();
        assert!(!encoding.can_reconstruct_values());
    }

    #[test]
    fn preserves_nulls_and_duplicate_field_projections() {
        let ty = LogicalType::Integer;
        let mut input = crate::test_utils::test_vector_with_capacity(ty.clone(), 1);
        input.set_null(0, true);
        input.set_count(1);
        let input = crate::test_utils::test_chunk_from_vectors(vec![input]);
        let encoding =
            SortKeyEncoding::new(vec![ty.clone()], vec![OrderModifiers::new(false, true)]).unwrap();
        let key = encoding.encode_row(&input, 0, &[0]).unwrap();
        let mut output = crate::test_utils::test_chunk_with_capacity(&[ty.clone(), ty], 1);
        let mut decoder = encoding.decoder(&output, &[(0, 1), (0, 0)]).unwrap();
        decoder.decode_row(&key, &mut output, 0).unwrap();
        output.set_cardinality(1);
        assert!(output.column(0).unwrap().is_null(0));
        assert!(output.column(1).unwrap().is_null(0));
    }

    #[test]
    fn rejects_truncated_and_trailing_keys() {
        let ty = LogicalType::Varchar;
        let mut input = crate::test_utils::test_vector_with_capacity(ty.clone(), 1);
        input.set_string(0, "abc");
        input.set_count(1);
        let input = crate::test_utils::test_chunk_from_vectors(vec![input]);
        let encoding =
            SortKeyEncoding::new(vec![ty.clone()], vec![OrderModifiers::new(true, false)]).unwrap();
        let key = encoding.encode_row(&input, 0, &[0]).unwrap();
        let output = crate::test_utils::test_chunk_with_capacity(&[ty], 1);

        let mut decoder = encoding.decoder(&output, &[(0, 0)]).unwrap();
        assert!(decoder
            .decode_row(&key[..key.len() - 1], &mut output.clone(), 0)
            .is_err());
        let mut trailing = key;
        trailing.push(7);
        assert!(decoder
            .decode_row(&trailing, &mut output.clone(), 0)
            .is_err());
    }
}
