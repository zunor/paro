// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::tablet_reader::TabletReader;
use crate::codec::vector_decoder;
use crate::rowset::encoding::BinaryPlainPageDecoder;
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
        let physical_selection = selection.as_deref();
        let mut stored_reads = vec![false; self.projection.len()];
        for (&read_idx, projection) in self.output_to_read.iter().zip(&self.value_projections) {
            if matches!(
                projection,
                super::tablet_reader::ColumnValueProjection::Stored
            ) {
                stored_reads[read_idx] = true;
            }
        }
        let mut read_vectors: Vec<Option<Arc<Vector>>> = Vec::with_capacity(self.projection.len());
        let mut raw_batches = Vec::with_capacity(self.projection.len());
        let allocator = self.allocator.clone();
        let selection = selection
            .clone()
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
                raw_batches.push(Some(col_batch));
                if !stored_reads[idx] {
                    read_vectors.push(None);
                    continue;
                }
                let storage_provenance = col_batch.storage_dictionary.as_ref().map(|_| {
                    vector_decoder::storage_dictionary_provenance_id(rowset_id, segment_id, *col_id)
                });
                read_vectors.push(Some(Arc::new(vector_decoder::decode_column_batch_cached(
                    ty,
                    col_batch,
                    physical_rows,
                    allocator.clone(),
                    storage_provenance,
                    &self.storage_dictionary_cache,
                    u64::from(*col_id),
                )?)));
                continue;
            }
            raw_batches.push(None);

            if let Some(vector) = self.schema_fill_vector(rowset_id, idx, physical_rows)? {
                read_vectors.push(Some(vector));
                continue;
            }

            if let Some(base_rowids) = self.load_base_rowids(rowset_id, segment_id, rowids)? {
                let resolved = self.get_by_rowids_internal(&base_rowids, &[*col_id], 1)?;
                let resolved_vector = resolved.column(0).ok_or_else(|| {
                    paro_error::internal("resolved partial row scan chunk missing requested column")
                })?;
                read_vectors.push(Some(resolved_vector.clone()));
            } else {
                read_vectors.push(Some(Arc::new(Vector::try_constant_null(
                    ty.clone(),
                    physical_rows,
                    allocator.clone(),
                )?)));
            }
        }

        if let Some(selection) = &selection {
            for vector in &mut read_vectors {
                let Some(vector) = vector else {
                    continue;
                };
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
        for (&read_idx, projection) in self.output_to_read.iter().zip(&self.value_projections) {
            let vector = match projection {
                super::tablet_reader::ColumnValueProjection::Stored => read_vectors
                    .get(read_idx)
                    .and_then(Option::as_ref)
                    .ok_or_else(|| paro_error::internal("Stored output mapping is unavailable"))?
                    .clone(),
                super::tablet_reader::ColumnValueProjection::MatchedUtf8Prefix { byte_width } => {
                    if let Some(batch) = raw_batches.get(read_idx).copied().flatten() {
                        Arc::new(decode_matched_utf8_prefix_batch(
                            batch,
                            physical_rows,
                            physical_selection,
                            rows,
                            *byte_width,
                            allocator.clone(),
                        )?)
                    } else {
                        let vector = read_vectors
                            .get(read_idx)
                            .and_then(Option::as_ref)
                            .ok_or_else(|| {
                                paro_error::internal("Matched-prefix source mapping is unavailable")
                            })?;
                        Arc::new(project_matched_utf8_prefix_vector(
                            vector,
                            rows,
                            *byte_width,
                            allocator.clone(),
                        )?)
                    }
                }
            };
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

fn decode_matched_utf8_prefix_batch(
    batch: &crate::rowset::column::ColumnBatch,
    physical_rows: usize,
    selection: Option<&[BatchRowOrdinal]>,
    rows: usize,
    byte_width: usize,
    allocator: Arc<dyn Allocator>,
) -> Result<Vector> {
    if byte_width == 0 || selection.is_some_and(|selection| selection.len() != rows) {
        return Err(paro_error::internal(
            "invalid matched UTF-8 prefix projection contract",
        ));
    }
    if batch
        .nulls
        .as_ref()
        .is_some_and(|nulls| nulls.len() < physical_rows)
    {
        return Err(paro_error::data_corrupted(
            "matched-prefix null bitmap is shorter than its row domain",
        ));
    }
    let physical_index = |output_index: usize| {
        selection.map_or(output_index, |selection| selection[output_index].index())
    };
    if (0..rows).any(|index| physical_index(index) >= physical_rows)
        || (1..rows).any(|index| physical_index(index - 1) >= physical_index(index))
    {
        return Err(paro_error::data_corrupted(
            "matched-prefix selection is not strictly increasing within the batch",
        ));
    }
    let mut output = Vector::try_new(LogicalType::Varchar, rows, allocator)?;
    let (entries, _validity, heap) = output.begin_varlen_write(rows);
    let validate_utf8 = !batch.has_verified_utf8();
    let mut write_value = |output_index: usize, value: &[u8]| -> Result<()> {
        if validate_utf8 {
            std::str::from_utf8(value)
                .map_err(|_| paro_error::data_corrupted("Invalid UTF-8 in string column"))?;
        }
        let prefix = value.get(..byte_width).ok_or_else(|| {
            paro_error::data_corrupted("matched string is shorter than its prefix witness")
        })?;
        if !prefix.is_ascii() {
            return Err(paro_error::data_corrupted(
                "matched string contradicts its ASCII prefix witness",
            ));
        }
        // SAFETY: the output vector owns `heap`, and begin_varlen_write exposed
        // exactly `rows` writable entries.
        let entry = unsafe { heap.try_add_blob(prefix) }?;
        unsafe { entries.add(output_index).write(entry) };
        Ok(())
    };
    let is_null = |row: usize| {
        batch
            .nulls
            .as_ref()
            .is_some_and(|nulls| nulls.get(row).is_some_and(|flag| *flag != 0))
    };

    if let Some(dictionary_batch) = &batch.storage_dictionary {
        if dictionary_batch.codes.len()
            != physical_rows
                .checked_mul(std::mem::size_of::<u32>())
                .ok_or_else(|| {
                    paro_error::data_corrupted("matched-prefix dictionary cardinality overflow")
                })?
        {
            return Err(paro_error::data_corrupted(
                "matched-prefix dictionary code cardinality is inconsistent",
            ));
        }
        let mut dictionary = BinaryPlainPageDecoder::new(dictionary_batch.dictionary.clone());
        dictionary.init()?;
        for output_index in 0..rows {
            let row = physical_index(output_index);
            if is_null(row) {
                return Err(paro_error::data_corrupted(
                    "NULL row escaped a matched-prefix predicate",
                ));
            }
            let code_offset = row * std::mem::size_of::<u32>();
            let code_end = code_offset + std::mem::size_of::<u32>();
            let code = u32::from_le_bytes(
                dictionary_batch
                    .codes
                    .get(code_offset..code_end)
                    .ok_or_else(|| paro_error::data_corrupted("dictionary code is truncated"))?
                    .try_into()
                    .expect("validated u32 code width"),
            );
            let value = dictionary.value_ref_at(code).ok_or_else(|| {
                paro_error::data_corrupted("matched-prefix dictionary code is out of range")
            })?;
            write_value(output_index, value)?;
        }
    } else if let Some(storage) = &batch.storage_binary_plain {
        if storage.rows() != physical_rows {
            return Err(paro_error::data_corrupted(
                "matched-prefix BinaryPlain cardinality is inconsistent",
            ));
        }
        for output_index in 0..rows {
            let row = physical_index(output_index);
            if is_null(row) {
                return Err(paro_error::data_corrupted(
                    "NULL row escaped a matched-prefix predicate",
                ));
            }
            let value = storage.row_value_ref(row).ok_or_else(|| {
                paro_error::data_corrupted("matched-prefix BinaryPlain row is missing")
            })?;
            write_value(output_index, value)?;
        }
    } else {
        let mut offset = 0usize;
        let mut output_index = 0usize;
        for row in 0..physical_rows {
            let length_end = offset
                .checked_add(std::mem::size_of::<u32>())
                .ok_or_else(|| {
                    paro_error::data_corrupted("matched-prefix length offset overflow")
                })?;
            let length = u32::from_le_bytes(
                batch
                    .data
                    .get(offset..length_end)
                    .ok_or_else(|| {
                        paro_error::data_corrupted("matched-prefix length is truncated")
                    })?
                    .try_into()
                    .expect("validated u32 length width"),
            ) as usize;
            let value_end = length_end.checked_add(length).ok_or_else(|| {
                paro_error::data_corrupted("matched-prefix value offset overflow")
            })?;
            let value = batch.data.get(length_end..value_end).ok_or_else(|| {
                paro_error::data_corrupted("matched-prefix value exceeds the column batch")
            })?;
            if output_index < rows && physical_index(output_index) == row {
                if is_null(row) {
                    return Err(paro_error::data_corrupted(
                        "NULL row escaped a matched-prefix predicate",
                    ));
                }
                write_value(output_index, value)?;
                output_index += 1;
            }
            offset = value_end;
        }
        if offset != batch.data.len() || output_index != rows {
            return Err(paro_error::data_corrupted(
                "matched-prefix column batch cardinality is inconsistent",
            ));
        }
    }
    output.set_count(rows);
    Ok(output)
}

fn project_matched_utf8_prefix_vector(
    input: &Vector,
    rows: usize,
    byte_width: usize,
    allocator: Arc<dyn Allocator>,
) -> Result<Vector> {
    let view = input.try_to_varlen_view(rows)?;
    let mut output = Vector::try_new(LogicalType::Varchar, rows, allocator)?;
    let (entries, _validity, heap) = output.begin_varlen_write(rows);
    for row in 0..rows {
        if !view.is_valid(row) {
            return Err(paro_error::data_corrupted(
                "NULL row escaped a matched-prefix predicate",
            ));
        }
        let prefix = view.bytes(row).get(..byte_width).ok_or_else(|| {
            paro_error::data_corrupted("matched string is shorter than its prefix witness")
        })?;
        if !prefix.is_ascii() {
            return Err(paro_error::data_corrupted(
                "matched string contradicts its ASCII prefix witness",
            ));
        }
        // SAFETY: the output vector owns the heap and exposes `rows` entries.
        let entry = unsafe { heap.try_add_blob(prefix) }?;
        unsafe { entries.add(row).write(entry) };
    }
    output.set_count(rows);
    Ok(output)
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
