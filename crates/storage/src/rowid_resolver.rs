use crate::codec::vector_decoder;
use crate::rowset::column::ColumnBatch;
use crate::rowset::{load_base_rowids_for_offsets, RowsetSharedPtr};
use crate::tablet::{ColumnId, Tablet};
use paro_common::allocator::Allocator;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_common::vector::Vector;
use std::sync::Arc;

struct GroupResult {
    rowset_id: u64,
    segment_id: u32,
    original_indices: Vec<usize>,
    row_count: usize,
    col_data: Vec<(ColumnId, ColumnBatch)>,
    resolved_missing: Option<Chunk>,
    missing_column_ids: Vec<ColumnId>,
}

pub(crate) fn scatter_vector_values(
    src: &Vector,
    indices: &[usize],
    dst: &mut Vector,
) -> Result<()> {
    for (src_idx, &dst_idx) in indices.iter().enumerate() {
        dst.copy_at(dst_idx, src, src_idx);
    }
    Ok(())
}

pub(crate) fn read_chunk_by_rowids_recursive<F>(
    tablet: &Tablet,
    column_ids: &[ColumnId],
    output_types: &[LogicalType],
    rowids: &[u64],
    allocator: Arc<dyn Allocator>,
    depth: usize,
    resolve_rowset: &F,
) -> Result<Chunk>
where
    F: Fn(u64) -> Result<RowsetSharedPtr>,
{
    if depth > 32 {
        return Err(paro_error::data_corrupted(
            "partial row base-row chain exceeds maximum depth",
        ));
    }
    if rowids.is_empty() || column_ids.is_empty() {
        return Ok(Chunk::with_allocator(allocator));
    }

    let schema = tablet
        .schema()
        .ok_or_else(|| paro_error::internal("Tablet schema not available"))?;

    let mut entries: Vec<(u64, u32, u32, usize)> = Vec::with_capacity(rowids.len());
    for (idx, &raw) in rowids.iter().enumerate() {
        let location = tablet.decode_row_id(crate::primary_key::RowID::from_raw(raw))?;
        entries.push((
            location.rowset_id,
            location.segment_id,
            location.row_offset,
            idx,
        ));
    }
    entries.sort_unstable_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)).then(a.2.cmp(&b.2)));

    let mut group_results = Vec::new();
    let mut group_start = 0;
    while group_start < entries.len() {
        let (rowset_id, segment_id, _, _) = entries[group_start];
        let mut group_end = group_start + 1;
        while group_end < entries.len()
            && entries[group_end].0 == rowset_id
            && entries[group_end].1 == segment_id
        {
            group_end += 1;
        }

        let group = &entries[group_start..group_end];
        let row_offsets: Vec<u32> = group.iter().map(|entry| entry.2).collect();
        let original_indices: Vec<usize> = group.iter().map(|entry| entry.3).collect();

        let rowset = resolve_rowset(rowset_id)?;
        rowset.load()?;
        let segment = rowset.get_segment(segment_id).ok_or_else(|| {
            paro_error::internal(format!(
                "segment {} not found in rowset {} while resolving row ids",
                segment_id, rowset_id
            ))
        })?;

        let present_column_ids: Vec<ColumnId> = column_ids
            .iter()
            .copied()
            .filter(|column_id| segment.get_column_meta(*column_id).is_some())
            .collect();
        let col_data = if present_column_ids.is_empty() {
            Vec::new()
        } else {
            segment.read_by_rowids(&present_column_ids, &row_offsets)?
        };
        let missing_column_ids: Vec<ColumnId> = column_ids
            .iter()
            .copied()
            .filter(|column_id| !col_data.iter().any(|(cid, _)| cid == column_id))
            .collect();
        let resolved_missing = if missing_column_ids.is_empty() {
            None
        } else if let Some(base_rowids) =
            load_base_rowids_for_offsets(rowset.rowset_path(), segment_id, &row_offsets)?
        {
            let base_rowids: Vec<u64> = base_rowids
                .into_iter()
                .map(|rowid| rowid.to_raw())
                .collect();
            let missing_output_types: Vec<LogicalType> = missing_column_ids
                .iter()
                .map(|column_id| {
                    schema
                        .column_by_id(*column_id)
                        .map(|column| column.logical_type.clone())
                        .ok_or_else(|| {
                            paro_error::invalid_input(format!(
                                "Column ID {} not found in schema",
                                column_id
                            ))
                        })
                })
                .collect::<Result<Vec<_>>>()?;
            Some(read_chunk_by_rowids_recursive(
                tablet,
                &missing_column_ids,
                &missing_output_types,
                &base_rowids,
                allocator.clone(),
                depth + 1,
                resolve_rowset,
            )?)
        } else {
            None
        };

        group_results.push(GroupResult {
            rowset_id,
            segment_id,
            original_indices,
            row_count: row_offsets.len(),
            col_data,
            resolved_missing,
            missing_column_ids,
        });
        group_start = group_end;
    }

    let mut output_vectors = Vec::with_capacity(column_ids.len());
    for (&column_id, output_type) in column_ids.iter().zip(output_types.iter()) {
        let mut vector = Vector::with_capacity_and_allocator(
            output_type.clone(),
            rowids.len(),
            allocator.clone(),
        );
        for group in &group_results {
            if let Some(batch) = group
                .col_data
                .iter()
                .find(|(cid, _)| *cid == column_id)
                .map(|(_, batch)| batch)
            {
                let temp = vector_decoder::decode_column_batch(
                    output_type,
                    batch,
                    group.row_count,
                    allocator.clone(),
                    batch.storage_dictionary.as_ref().map(|_| {
                        vector_decoder::storage_dictionary_provenance_id(
                            group.rowset_id,
                            group.segment_id,
                            column_id,
                        )
                    }),
                )?;
                scatter_vector_values(&temp, &group.original_indices, &mut vector)?;
                continue;
            }

            if let Some(resolved) = &group.resolved_missing {
                if let Some(idx) = group
                    .missing_column_ids
                    .iter()
                    .position(|missing_column_id| *missing_column_id == column_id)
                {
                    let temp = resolved.column(idx).ok_or_else(|| {
                        paro_error::internal("resolved partial row chunk missing requested column")
                    })?;
                    scatter_vector_values(temp, &group.original_indices, &mut vector)?;
                    continue;
                }
            }

            for &original_idx in &group.original_indices {
                vector.set_null(original_idx, true);
            }
        }

        vector.set_count(rowids.len());
        output_vectors.push(Arc::new(vector));
    }

    Ok(Chunk::from_arc_vectors(output_vectors))
}
