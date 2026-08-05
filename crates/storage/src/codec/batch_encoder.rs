// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! ColumnBatch → columnar storage encoding (compaction / merge path).

use crate::codec::physical_layout;
use crate::rowset::column::ColumnBatch;
use crate::rowset::encoding::BinaryPlainPageDecoder;
use crate::rowset::ColumnData;
use crate::tablet::{ColumnId, TabletSchema};
use bytes::Bytes;

use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;

pub(crate) fn encode_batch(
    schema: &TabletSchema,
    batch: &[(ColumnId, ColumnBatch)],
    rows: usize,
) -> Result<Vec<ColumnData>> {
    let mut columns = Vec::with_capacity(schema.num_columns());
    let mut data_map = std::collections::HashMap::new();
    for (cid, batch) in batch {
        data_map.insert(*cid, batch);
    }

    for col in schema.columns() {
        let batch = data_map.get(&col.id).ok_or_else(|| {
            paro_error::data_corrupted(format!("Missing column {} in batch", col.id))
        })?;

        let data = materialize_storage_dictionary(col.logical_type.clone(), batch, rows)?;
        let column = if let Some(nulls) = batch.nulls.as_deref() {
            let packed = pack_nulls(nulls, rows)?;
            ColumnData::with_nulls(data, packed, rows as u32)
        } else {
            ColumnData::new(data, rows as u32)
        };
        columns.push(column);
    }

    Ok(columns)
}

fn materialize_storage_dictionary(
    logical_type: LogicalType,
    batch: &ColumnBatch,
    rows: usize,
) -> Result<Bytes> {
    if batch.storage_dictionary.is_none() {
        return Ok(batch.data.clone());
    }
    let fixed_width = if matches!(
        &logical_type,
        LogicalType::Varchar
            | LogicalType::VarcharCollation(_)
            | LogicalType::TsVector
            | LogicalType::TsQuery
            | LogicalType::Json
            | LogicalType::Jsonb
            | LogicalType::Blob
    ) {
        None
    } else {
        Some(
            physical_layout::fixed_row_width(&logical_type).map_err(|_| {
                paro_error::data_corrupted(format!(
                    "Storage dictionary batch has incompatible type {logical_type}"
                ))
            })?,
        )
    };

    let storage_dictionary = batch
        .storage_dictionary
        .as_ref()
        .expect("storage dictionary checked above");
    let mut decoder = BinaryPlainPageDecoder::new(storage_dictionary.dictionary.clone());
    decoder.init()?;
    if batch.nulls.as_ref().is_some_and(|nulls| nulls.len() < rows) {
        return Err(paro_error::data_corrupted(
            "Storage dictionary null map shorter than row count",
        ));
    }

    let mut data = Vec::new();
    for row in 0..rows {
        let is_null = batch.nulls.as_ref().is_some_and(|nulls| nulls[row] != 0);
        if is_null {
            if let Some(width) = fixed_width {
                data.resize(data.len() + width, 0);
            } else {
                data.extend_from_slice(&0_u32.to_le_bytes());
            }
            continue;
        }
        let code_offset = row
            .checked_mul(std::mem::size_of::<u32>())
            .ok_or_else(|| paro_error::data_corrupted("Storage dictionary row overflow"))?;
        let code_end = code_offset
            .checked_add(std::mem::size_of::<u32>())
            .ok_or_else(|| paro_error::data_corrupted("Storage dictionary code overflow"))?;
        let code_bytes = storage_dictionary
            .codes
            .get(code_offset..code_end)
            .ok_or_else(|| {
                paro_error::data_corrupted("Storage dictionary codes shorter than row count")
            })?;
        let code = u32::from_le_bytes(code_bytes.try_into().expect("u32 code slice"));
        let value = decoder.string_at(code).ok_or_else(|| {
            paro_error::data_corrupted(format!("Storage dictionary code {code} is out of range"))
        })?;
        if let Some(width) = fixed_width {
            if value.len() != width {
                return Err(paro_error::data_corrupted(format!(
                    "Storage dictionary value width {} does not match {logical_type} physical width {width}",
                    value.len(),
                )));
            }
        } else {
            let len = u32::try_from(value.len()).map_err(|_| {
                paro_error::out_of_range("Storage dictionary value exceeds u32 length")
            })?;
            data.extend_from_slice(&len.to_le_bytes());
        }
        data.extend_from_slice(value.as_ref());
    }
    Ok(Bytes::from(data))
}

fn pack_nulls(nulls: &[u8], rows: usize) -> Result<Bytes> {
    if nulls.len() < rows {
        return Err(paro_error::data_corrupted(
            "Null map shorter than expected row count",
        ));
    }
    let byte_len = rows.div_ceil(8);
    let mut packed = vec![0u8; byte_len];
    for (idx, &null_val) in nulls.iter().enumerate().take(rows) {
        if null_val != 0 {
            let byte_idx = idx / 8;
            let bit_idx = idx % 8;
            packed[byte_idx] |= 1u8 << bit_idx;
        }
    }
    Ok(Bytes::from(packed))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rowset::encoding::BinaryPlainPageBuilder;
    use crate::tablet::{KeysType, TabletColumn};
    use std::sync::Arc;

    #[test]
    fn encode_batch_materializes_storage_dictionary_codes_as_varlen_values() {
        let mut dictionary = BinaryPlainPageBuilder::new(1024);
        assert!(dictionary.add_slice(b"N"));
        assert!(dictionary.add_slice(b"R"));
        let dictionary = dictionary.finish().unwrap();
        let codes = [0_u32, 1, 0]
            .into_iter()
            .flat_map(u32::to_le_bytes)
            .collect::<Vec<_>>();
        let batch = ColumnBatch::with_storage_dictionary(
            dictionary,
            Bytes::from(codes),
            Some(Bytes::from(vec![0, 0, 1])),
        );
        let schema = Arc::new(
            TabletSchema::new(
                1,
                vec![TabletColumn::new(0, "flag", LogicalType::Varchar)],
                KeysType::DuplicateKeys,
            )
            .unwrap(),
        );

        let encoded = encode_batch(&schema, &[(0, batch)], 3).unwrap();

        assert_eq!(encoded.len(), 1);
        assert_eq!(encoded[0].num_values, 3);
        assert_eq!(encoded[0].null_flags.as_deref(), Some(&[0b0000_0100][..]));
        assert_eq!(
            encoded[0].data.as_ref(),
            &[1, 0, 0, 0, b'N', 1, 0, 0, 0, b'R', 0, 0, 0, 0,]
        );
    }

    #[test]
    fn encode_batch_materializes_storage_dictionary_codes_as_fixed_values() {
        let mut dictionary = BinaryPlainPageBuilder::new(1024);
        assert!(dictionary.add_slice(&7_i32.to_le_bytes()));
        assert!(dictionary.add_slice(&11_i32.to_le_bytes()));
        let dictionary = dictionary.finish().unwrap();
        let codes = [0_u32, 1, 0]
            .into_iter()
            .flat_map(u32::to_le_bytes)
            .collect::<Vec<_>>();
        let batch = ColumnBatch::with_storage_dictionary(
            dictionary,
            Bytes::from(codes),
            Some(Bytes::from(vec![0, 0, 1])),
        );
        let schema = Arc::new(
            TabletSchema::new(
                1,
                vec![TabletColumn::new(0, "key", LogicalType::Integer)],
                KeysType::DuplicateKeys,
            )
            .unwrap(),
        );

        let encoded = encode_batch(&schema, &[(0, batch)], 3).unwrap();

        assert_eq!(encoded.len(), 1);
        assert_eq!(encoded[0].num_values, 3);
        assert_eq!(encoded[0].null_flags.as_deref(), Some(&[0b0000_0100][..]));
        let expected = [7_i32, 11, 0]
            .into_iter()
            .flat_map(i32::to_le_bytes)
            .collect::<Vec<_>>();
        assert_eq!(encoded[0].data.as_ref(), expected);
    }
}
