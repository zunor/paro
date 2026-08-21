// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Batched PostgreSQL `DataRow` wire encoding.

use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_execution::query_executor::compiled::ResultColumnDesc;
use paro_session::{encode_binary_value, FormatCode};
use pgwire::messages::data::MESSAGE_TYPE_BYTE_DATA_ROW;
use tokio_util::bytes::{BufMut, BytesMut};

use super::value_format::TextVectorEncoder;

const MESSAGE_HEADER_BYTES: usize = 1 + size_of::<i32>();
const FIELD_COUNT_BYTES: usize = size_of::<i16>();
const FIELD_LENGTH_BYTES: usize = size_of::<i32>();

/// A sequence of complete backend `DataRow` frames.
///
/// The codec accepts this as one sink item, avoiding an intermediate allocation
/// and codec dispatch for every row while preserving the PostgreSQL message
/// boundary for clients.
pub(crate) struct EncodedDataRows(BytesMut);

impl EncodedDataRows {
    #[cfg(test)]
    pub(crate) fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    pub(crate) fn into_inner(self) -> BytesMut {
        self.0
    }
}

struct DataRowBatchEncoder {
    data: BytesMut,
    field_count: i16,
}

impl DataRowBatchEncoder {
    fn new(row_count: usize, field_count: usize, encoded_value_hint: usize) -> Result<Self> {
        let field_count = i16::try_from(field_count).map_err(|_| {
            paro_error::internal(format!("PostgreSQL row has too many fields: {field_count}"))
        })?;
        let minimum_row_bytes = MESSAGE_HEADER_BYTES
            .checked_add(FIELD_COUNT_BYTES)
            .and_then(|size| {
                size.checked_add(usize::from(field_count as u16).saturating_mul(FIELD_LENGTH_BYTES))
            })
            .ok_or_else(|| paro_error::internal("PostgreSQL row batch size overflow"))?;
        let capacity = row_count
            .checked_mul(minimum_row_bytes)
            .and_then(|size| size.checked_add(encoded_value_hint))
            .ok_or_else(|| paro_error::internal("PostgreSQL row batch size overflow"))?;
        Ok(Self {
            data: BytesMut::with_capacity(capacity),
            field_count,
        })
    }

    fn begin_row(&mut self) -> usize {
        let row_start = self.data.len();
        self.data.put_u8(MESSAGE_TYPE_BYTE_DATA_ROW);
        self.data.put_i32(0);
        self.data.put_i16(self.field_count);
        row_start
    }

    fn append_null(&mut self) {
        self.data.put_i32(-1);
    }

    fn append_value(&mut self, encode: impl FnOnce(&mut BytesMut) -> Result<()>) -> Result<()> {
        let length_offset = self.data.len();
        self.data.put_i32(0);
        let value_start = self.data.len();
        encode(&mut self.data)?;
        let value_length = self.data.len() - value_start;
        patch_i32(&mut self.data, length_offset, value_length, "DataRow field")
    }

    fn finish_row(&mut self, row_start: usize) -> Result<()> {
        let body_length = self
            .data
            .len()
            .checked_sub(row_start + 1)
            .ok_or_else(|| paro_error::internal("invalid PostgreSQL DataRow boundary"))?;
        patch_i32(
            &mut self.data,
            row_start + 1,
            body_length,
            "DataRow message",
        )
    }

    fn finish(self) -> EncodedDataRows {
        EncodedDataRows(self.data)
    }
}

fn patch_i32(data: &mut BytesMut, offset: usize, value: usize, description: &str) -> Result<()> {
    let value = i32::try_from(value).map_err(|_| {
        paro_error::internal(format!("PostgreSQL {description} exceeds protocol limit"))
    })?;
    data[offset..offset + size_of::<i32>()].copy_from_slice(&value.to_be_bytes());
    Ok(())
}

pub(crate) fn encode_text_chunk_rows(chunk: &Chunk, field_count: usize) -> Result<EncodedDataRows> {
    let mut columns = Vec::with_capacity(field_count);
    let mut encoded_value_hint = 0usize;
    for col_idx in 0..field_count {
        let column = chunk
            .column(col_idx)
            .map(|vector| TextVectorEncoder::try_new(vector, chunk.size()))
            .transpose()?;
        if let Some(column) = &column {
            encoded_value_hint = encoded_value_hint
                .checked_add(column.encoded_bytes_hint(chunk.size()))
                .ok_or_else(|| paro_error::internal("PostgreSQL row batch size overflow"))?;
        }
        columns.push(column);
    }
    let mut encoder = DataRowBatchEncoder::new(chunk.size(), field_count, encoded_value_hint)?;
    for row_idx in 0..chunk.size() {
        let row_start = encoder.begin_row();
        for column in &mut columns {
            let Some(column) = column else {
                encoder.append_null();
                continue;
            };
            if column.is_null(row_idx) {
                encoder.append_null();
                continue;
            }
            encoder.append_value(|buffer| column.append_non_null(buffer, row_idx))?;
        }
        encoder.finish_row(row_start)?;
    }
    Ok(encoder.finish())
}

pub(crate) fn encode_chunk_rows(
    chunk: &Chunk,
    schema: &[ResultColumnDesc],
    format_codes: &[FormatCode],
) -> Result<EncodedDataRows> {
    let mut text_columns = Vec::with_capacity(schema.len());
    let mut encoded_value_hint = 0usize;
    for col_idx in 0..schema.len() {
        let format = format_codes.get(col_idx).unwrap_or(&FormatCode::Text);
        let column = if matches!(format, FormatCode::Text) {
            chunk
                .column(col_idx)
                .map(|vector| TextVectorEncoder::try_new(vector, chunk.size()))
                .transpose()?
        } else {
            None
        };
        if let Some(column) = &column {
            encoded_value_hint = encoded_value_hint
                .checked_add(column.encoded_bytes_hint(chunk.size()))
                .ok_or_else(|| paro_error::internal("PostgreSQL row batch size overflow"))?;
        }
        text_columns.push(column);
    }
    let mut encoder = DataRowBatchEncoder::new(chunk.size(), schema.len(), encoded_value_hint)?;
    for row_idx in 0..chunk.size() {
        let row_start = encoder.begin_row();
        for (col_idx, column) in schema.iter().enumerate() {
            let Some(vector) = chunk.column(col_idx) else {
                encoder.append_null();
                continue;
            };
            match format_codes.get(col_idx).unwrap_or(&FormatCode::Text) {
                FormatCode::Text => {
                    let column = text_columns[col_idx].as_mut().ok_or_else(|| {
                        paro_error::internal(format!(
                            "PostgreSQL text column encoder missing at index {col_idx}"
                        ))
                    })?;
                    if column.is_null(row_idx) {
                        encoder.append_null();
                    } else {
                        encoder.append_value(|buffer| column.append_non_null(buffer, row_idx))?;
                    }
                }
                FormatCode::Binary => {
                    if vector.is_null(row_idx) {
                        encoder.append_null();
                    } else {
                        encoder.append_value(|buffer| {
                            let value = vector.get_value(row_idx);
                            let payload = encode_binary_value(&value, &column.logical_type)?;
                            buffer.extend_from_slice(&payload);
                            Ok(())
                        })?;
                    }
                }
            }
        }
        encoder.finish_row(row_start)?;
    }
    Ok(encoder.finish())
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_common::test_utils::{
        test_chunk_from_vectors, test_i32_vector, test_i64_vector, test_nullable_string_vector,
    };

    fn decode_rows(encoded: &EncodedDataRows) -> Vec<Vec<Option<Vec<u8>>>> {
        let mut input = encoded.as_bytes();
        let mut rows = Vec::new();
        while !input.is_empty() {
            assert_eq!(input[0], MESSAGE_TYPE_BYTE_DATA_ROW);
            let message_length = i32::from_be_bytes(input[1..5].try_into().unwrap()) as usize;
            let message_end = 1 + message_length;
            let field_count = i16::from_be_bytes(input[5..7].try_into().unwrap()) as usize;
            let mut offset = 7;
            let mut row = Vec::with_capacity(field_count);
            for _ in 0..field_count {
                let length = i32::from_be_bytes(input[offset..offset + 4].try_into().unwrap());
                offset += 4;
                if length < 0 {
                    row.push(None);
                } else {
                    let length = length as usize;
                    row.push(Some(input[offset..offset + length].to_vec()));
                    offset += length;
                }
            }
            assert_eq!(offset, message_end);
            rows.push(row);
            input = &input[message_end..];
        }
        rows
    }

    #[test]
    fn text_batch_encodes_q16_scalar_shape_without_value_materialization() {
        let chunk = test_chunk_from_vectors(vec![
            test_nullable_string_vector(&[Some("Brand#11"), None]),
            test_nullable_string_vector(&[Some("STANDARD POLISHED"), Some("SMALL BRUSHED")]),
            test_i32_vector(&[3, 49]),
            test_i64_vector(&[17, 2]),
        ]);

        let rows = decode_rows(&encode_text_chunk_rows(&chunk, 4).unwrap());
        assert_eq!(
            rows,
            vec![
                vec![
                    Some(b"Brand#11".to_vec()),
                    Some(b"STANDARD POLISHED".to_vec()),
                    Some(b"3".to_vec()),
                    Some(b"17".to_vec()),
                ],
                vec![
                    None,
                    Some(b"SMALL BRUSHED".to_vec()),
                    Some(b"49".to_vec()),
                    Some(b"2".to_vec()),
                ],
            ]
        );
    }

    #[test]
    fn empty_chunk_produces_no_data_row_frames() {
        let chunk = test_chunk_from_vectors(Vec::new());
        assert!(encode_text_chunk_rows(&chunk, 0)
            .unwrap()
            .as_bytes()
            .is_empty());
    }
}
