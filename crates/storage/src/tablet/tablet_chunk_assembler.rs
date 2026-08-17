// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::tablet_reader::TabletReader;
use crate::codec::vector_decoder;
use crate::rowset::load_base_rowids_for_offsets;
use crate::rowset::{BatchRowOrdinal, SegmentRowId};
use crate::tablet::{ColumnId, PhysicalRowRef, TabletRef};
use paro_common::allocator::Allocator;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_common::vector::{SelectionVector, ValidatedVectorSelection, Vector};
use std::sync::Arc;

impl TabletReader {
    pub(super) fn infer_row_count(
        &self,
        batch: &[(ColumnId, crate::rowset::column::ColumnBatch)],
        expected: usize,
    ) -> Result<usize> {
        if expected == 0 {
            return Ok(0);
        }
        if batch.is_empty() {
            return Ok(expected);
        }

        let (col_id, batch) = &batch[0];
        let ty = self.logical_type_for_column(*col_id)?;
        let inferred = if let Some(storage_dictionary) = &batch.storage_dictionary {
            let code_width = std::mem::size_of::<u32>();
            if storage_dictionary.codes.len() % code_width != 0 {
                return Err(paro_error::data_corrupted(
                    "Storage dictionary code count is not u32-aligned",
                ));
            }
            storage_dictionary.codes.len() / code_width
        } else {
            vector_decoder::infer_batch_row_count(&ty, &batch.data, expected)?
        };
        if inferred != expected {
            return Err(paro_error::data_corrupted(format!(
                "Row count mismatch: expected {}, inferred {}",
                expected, inferred
            )));
        }
        Ok(inferred)
    }

    pub(super) fn logical_type_for_column(&self, col_id: ColumnId) -> Result<LogicalType> {
        if let Some(idx) = self
            .projection
            .iter()
            .position(|column_id| *column_id == col_id)
        {
            return Ok(self.read_types[idx].clone());
        }
        if let Some(column) = self.schema.column_by_id(col_id) {
            return Ok(column.logical_type.clone());
        }
        Err(paro_error::internal("Column ID not found in schema"))
    }

    #[cfg(test)]
    pub(super) fn build_chunk(
        &self,
        batch: &[(ColumnId, crate::rowset::column::ColumnBatch)],
        rows: usize,
        rowids: &[u32],
        rowset_id: u64,
        segment_id: u32,
    ) -> Result<Chunk> {
        self.build_chunk_with_owned_selection(
            batch,
            rows,
            rows,
            None,
            SegmentRowId::from_raw_slice(rowids),
            rowset_id,
            segment_id,
        )
    }

    pub(super) fn build_chunk_with_owned_selection(
        &self,
        batch: &[(ColumnId, crate::rowset::column::ColumnBatch)],
        rows: usize,
        physical_rows: usize,
        selection: Option<Vec<BatchRowOrdinal>>,
        rowids: &[SegmentRowId],
        rowset_id: u64,
        segment_id: u32,
    ) -> Result<Chunk> {
        if rows == 0 {
            return Chunk::try_new(self.allocator.clone());
        }
        if selection
            .as_ref()
            .is_some_and(|selection| selection.len() != rows)
        {
            return Err(paro_error::data_corrupted(
                "Column batch selection length does not match logical rows",
            ));
        }
        let mut read_vectors: Vec<Arc<Vector>> = Vec::with_capacity(self.projection.len());
        let allocator = self.allocator.clone();
        let selection = selection
            .map(BatchRowOrdinal::into_raw_vec)
            .map(|indices| SelectionVector::try_from_owned_indices(indices, allocator.clone()))
            .transpose()?
            .map(|selection| {
                ValidatedVectorSelection::try_new(selection, physical_rows).map_err(|error| {
                    paro_error::data_corrupted(format!(
                        "invalid column batch selection for {physical_rows} rows: {error}"
                    ))
                })
            })
            .transpose()?;
        let mut batch_hint = 0usize;

        for (idx, col_id) in self.projection.iter().enumerate() {
            let ty = &self.read_types[idx];
            if let Some(col_batch) = find_column_batch(batch, *col_id, &mut batch_hint) {
                let storage_provenance = col_batch.storage_dictionary.as_ref().map(|_| {
                    vector_decoder::storage_dictionary_provenance_id(rowset_id, segment_id, *col_id)
                });
                read_vectors.push(Arc::new(vector_decoder::decode_column_batch_cached(
                    ty,
                    col_batch,
                    physical_rows,
                    allocator.clone(),
                    storage_provenance,
                    &self.storage_dictionary_cache,
                    u64::from(*col_id),
                )?));
                continue;
            }

            if let Some(vector) = self.schema_fill_vector(rowset_id, idx, physical_rows)? {
                read_vectors.push(vector);
                continue;
            }

            if let Some(base_rowids) = self.load_base_rowids(rowset_id, segment_id, rowids)? {
                let resolved = self.get_by_rowids_internal(&base_rowids, &[*col_id], 1)?;
                let resolved_vector = resolved.column(0).ok_or_else(|| {
                    paro_error::internal("resolved partial row scan chunk missing requested column")
                })?;
                read_vectors.push(resolved_vector.clone());
            } else {
                read_vectors.push(Arc::new(Vector::try_constant_null(
                    ty.clone(),
                    physical_rows,
                    allocator.clone(),
                )?));
            }
        }

        if let Some(selection) = &selection {
            for vector in &mut read_vectors {
                if vector.len() == physical_rows {
                    *vector = Arc::new(Vector::try_dictionary_from_validated(
                        vector.clone(),
                        selection.clone(),
                    )?);
                } else if vector.len() != rows {
                    return Err(paro_error::data_corrupted(format!(
                        "Selected column vector has {} rows, expected {rows} logical or {physical_rows} physical rows",
                        vector.len(),
                    )));
                }
            }
        }

        let mut output_vectors = Vec::with_capacity(self.output_to_read.len());
        for &read_idx in &self.output_to_read {
            let vector = read_vectors
                .get(read_idx)
                .ok_or_else(|| paro_error::internal("Output mapping out of range"))?
                .clone();
            output_vectors.push(vector);
        }

        if self.params.emit_row_id {
            if rowids.len() != rows {
                return Err(paro_error::data_corrupted(format!(
                    "RowId vector length mismatch: expected {}, got {}",
                    rows,
                    rowids.len()
                )));
            }
            output_vectors.push(Arc::new(Self::build_row_id_vector(
                &self.tablet,
                rows,
                rowids,
                rowset_id,
                segment_id,
                allocator,
            )?));
        }

        Chunk::try_from_arc_vectors_with_cardinality(output_vectors, rows, self.allocator.clone())
    }

    fn load_base_rowids(
        &self,
        rowset_id: u64,
        segment_id: u32,
        rowids: &[SegmentRowId],
    ) -> Result<Option<Vec<u64>>> {
        let rowset = self
            .rowsets
            .iter()
            .find(|rowset| rowset.rowset_id() == rowset_id)
            .cloned()
            .or_else(|| self.tablet.find_retained_rowset_by_id(rowset_id))
            .ok_or_else(|| {
                paro_error::internal(format!(
                    "rowset {} not found while resolving partial scan rows",
                    rowset_id
                ))
            })?;
        Ok(load_base_rowids_for_offsets(
            rowset.rowset_path(),
            segment_id,
            SegmentRowId::as_raw_slice(rowids),
        )?
        .map(|base_rowids| {
            base_rowids
                .into_iter()
                .map(|rowid| rowid.to_raw())
                .collect()
        }))
    }

    fn build_row_id_vector(
        tablet: &TabletRef,
        rows: usize,
        rowids: &[SegmentRowId],
        rowset_id: u64,
        segment_id: u32,
        allocator: Arc<dyn Allocator>,
    ) -> Result<Vector> {
        let mut vector = Vector::try_new(LogicalType::BigInt, rows, allocator)?;
        for (idx, row_offset) in rowids.iter().copied().enumerate() {
            let row_id = tablet
                .encode_row_location(PhysicalRowRef::new(rowset_id, segment_id, row_offset))?;
            vector.set_i64(idx, row_id.to_raw() as i64);
        }
        vector.set_count(rows);
        Ok(vector)
    }
}

fn find_column_batch<'a>(
    batch: &'a [(ColumnId, crate::rowset::column::ColumnBatch)],
    column_id: ColumnId,
    hint: &mut usize,
) -> Option<&'a crate::rowset::column::ColumnBatch> {
    if let Some((candidate, column_batch)) = batch.get(*hint) {
        if *candidate == column_id {
            *hint += 1;
            return Some(column_batch);
        }
    }

    let found = batch
        .iter()
        .position(|(candidate, _)| *candidate == column_id)?;
    *hint = found + 1;
    Some(&batch[found].1)
}
