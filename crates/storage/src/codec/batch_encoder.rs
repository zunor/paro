//! ColumnBatch → columnar storage encoding (compaction / merge path).

use crate::rowset::column::ColumnBatch;
use crate::rowset::ColumnData;
use crate::tablet::{ColumnId, TabletSchema};
use bytes::Bytes;

use paro_common::error::{self as paro_error, Result};

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

        let column = if let Some(nulls) = batch.nulls.as_deref() {
            let packed = pack_nulls(nulls, rows)?;
            ColumnData::with_nulls(batch.data.clone(), packed, rows as u32)
        } else {
            ColumnData::new(batch.data.clone(), rows as u32)
        };
        columns.push(column);
    }

    Ok(columns)
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
