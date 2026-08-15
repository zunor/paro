// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::predicate_column::PredicateColumnReuse;
use super::segment::{Segment, SegmentOptions};
use super::segment_predicate::PredicateEvaluator;
use super::segment_predicate_program::PredicateStageReadStats;
use crate::buffer::{BufferPool, Prefetcher};
use crate::index::{IndexEvaluator, PredicateResult, PredicateTree};
use crate::primary_key::DeleteVector;
use crate::rowset::column::{ColumnBatch, ColumnIterator, OrderedRowIds};
use crate::rowset::{BatchRowOrdinal, SegmentRowId};
use crate::tablet::ColumnId;
use bytes::Bytes;
use paro_common::allocator::MemoryTag;
use paro_common::error::{self as paro_error, Result};
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Debug)]
struct ColumnDataBytesTracker {
    buffer_pool: Option<Arc<BufferPool>>,
    bytes: i64,
}

impl ColumnDataBytesTracker {
    fn new(buffer_pool: Option<Arc<BufferPool>>) -> Self {
        Self {
            buffer_pool,
            bytes: 0,
        }
    }

    fn set(&mut self, bytes: usize) {
        let new_bytes = bytes as i64;
        if new_bytes == self.bytes {
            return;
        }
        if let Some(pool) = &self.buffer_pool {
            pool.update_used_memory(MemoryTag::ColumnData, new_bytes - self.bytes);
        }
        self.bytes = new_bytes;
    }

    fn reset(&mut self) {
        self.set(0);
    }
}

impl Drop for ColumnDataBytesTracker {
    fn drop(&mut self) {
        if self.bytes != 0 {
            if let Some(pool) = &self.buffer_pool {
                pool.update_used_memory(MemoryTag::ColumnData, -self.bytes);
            }
        }
        self.bytes = 0;
    }
}

/// Segment iterator for reading data.
pub struct SegmentIterator {
    segment_id: u32,
    file_path: PathBuf,
    column_iterators: Vec<(ColumnId, Box<dyn ColumnIterator + Send + Sync>)>,
    current_ordinal: u64,
    num_rows: u64,
    end_ordinal: u64,
    options: SegmentOptions,
    delete_vector: Option<DeleteVector>,
    pub(super) evaluated_selection: PredicateResult,
    predicate_guaranteed: PredicateResult,
    predicate_evaluator: Option<PredicateEvaluator>,
    late_materialization: Option<LateMaterializationState>,
    sparse_batch_streak: u8,
    dense_batch_streak: u8,
    eager_predicate_matches: Vec<BatchRowOrdinal>,
    selection_tracker: ColumnDataBytesTracker,
    rowid_tracker: ColumnDataBytesTracker,
    prefetcher: Option<Arc<Prefetcher>>,
    predicate_stage_read_stats: PredicateStageReadStats,
}

pub struct SegmentBatch {
    pub rowids: Vec<SegmentRowId>,
    pub rows: usize,
    /// Number of physical rows represented by every returned column batch.
    /// This differs from `rows` only when `selection` overlays an eager batch.
    pub physical_rows: usize,
    /// Shared logical-to-physical mapping for an eager predicate result.
    pub selection: Option<Vec<BatchRowOrdinal>>,
    pub columns: Vec<(ColumnId, ColumnBatch)>,
}

struct ReusedPredicateColumn {
    column_id: ColumnId,
    predicate_idx: usize,
    encoding: PredicateColumnReuse,
    state: PredicateColumnReuseState,
}

enum PredicateColumnReuseState {
    Collecting {
        data: Vec<u8>,
        nulls: Vec<u8>,
        row_ends: Vec<usize>,
        utf8_verified: bool,
    },
    Dictionary {
        dictionary: Bytes,
        codes: Vec<u8>,
        nulls: Vec<u8>,
        utf8_verified: bool,
    },
    Readback,
}

struct LateMaterializationState {
    rowids: Vec<SegmentRowId>,
    predicate_matches: Vec<BatchRowOrdinal>,
    reused_predicate_columns: Vec<ReusedPredicateColumn>,
}

fn contiguous_segment_rowids(start: u64, rows: usize) -> Result<Vec<SegmentRowId>> {
    let end = start
        .checked_add(rows as u64)
        .ok_or_else(|| paro_error::data_corrupted("segment row-id range overflow"))?;
    if end > u64::from(u32::MAX) + 1 {
        return Err(paro_error::data_corrupted(
            "segment row-id range exceeds the u32 domain",
        ));
    }
    Ok((start..end)
        .map(|ordinal| SegmentRowId::from_raw(ordinal as u32))
        .collect())
}

/// Return whether `start` belongs to a predicate-proof range and the next
/// ordinal at which that answer can change. Proof ranges are segment-relative.
fn predicate_proof_span(proof: &PredicateResult, start: u64, scan_end: u64) -> (bool, u64) {
    match proof {
        PredicateResult::AllMatch => (true, scan_end),
        PredicateResult::PageRanges(ranges) => {
            for range in ranges {
                let range_start = range.start_row as u64;
                let range_end = range.end_row as u64;
                if start < range_start {
                    // A false span never needs to be split at the next proof
                    // boundary: row-level evaluation remains correct across
                    // both unproven and proven pages. Only an active proof
                    // must stop where its truth value can change.
                    return (false, scan_end);
                }
                if start < range_end {
                    return (true, scan_end.min(range_end));
                }
            }
            (false, scan_end)
        }
        PredicateResult::NoneMatch | PredicateResult::Bitmap(_) | PredicateResult::Unknown => {
            (false, scan_end)
        }
    }
}

impl ReusedPredicateColumn {
    fn new(column_id: ColumnId, predicate_idx: usize, encoding: PredicateColumnReuse) -> Self {
        Self {
            column_id,
            predicate_idx,
            encoding,
            state: PredicateColumnReuseState::Collecting {
                data: Vec::new(),
                nulls: Vec::new(),
                row_ends: Vec::new(),
                utf8_verified: true,
            },
        }
    }

    fn append_rows(
        &mut self,
        batch: &super::predicate_column::PredicateColumnBatch,
        rows: &[BatchRowOrdinal],
    ) -> Result<()> {
        if let Some(dictionary_batch) = batch.storage_dictionary() {
            return self.append_dictionary_rows(dictionary_batch, rows);
        }
        if matches!(self.state, PredicateColumnReuseState::Dictionary { .. }) {
            self.state = PredicateColumnReuseState::Readback;
            return Ok(());
        }
        let PredicateColumnReuseState::Collecting {
            data,
            nulls,
            row_ends,
            utf8_verified,
        } = &mut self.state
        else {
            return Ok(());
        };
        if batch.append_reusable_rows(self.encoding, rows, data, nulls, row_ends)? {
            *utf8_verified &= batch.reusable_rows_have_verified_utf8();
        } else {
            self.state = PredicateColumnReuseState::Readback;
        }
        Ok(())
    }

    fn append_dictionary_rows(
        &mut self,
        batch: &super::predicate_column::StorageDictionaryPredicateBatch,
        rows: &[BatchRowOrdinal],
    ) -> Result<()> {
        let can_start_dictionary = matches!(
            &self.state,
            PredicateColumnReuseState::Collecting {
                data,
                nulls,
                row_ends,
                ..
            } if data.is_empty() && nulls.is_empty() && row_ends.is_empty()
        );
        if can_start_dictionary {
            self.state = PredicateColumnReuseState::Dictionary {
                dictionary: batch.encoded_dictionary().clone(),
                codes: Vec::with_capacity(rows.len().saturating_mul(std::mem::size_of::<u32>())),
                nulls: Vec::with_capacity(rows.len()),
                utf8_verified: true,
            };
        }

        let PredicateColumnReuseState::Dictionary {
            dictionary,
            codes,
            nulls,
            utf8_verified,
        } = &mut self.state
        else {
            self.state = PredicateColumnReuseState::Readback;
            return Ok(());
        };
        if dictionary != batch.encoded_dictionary() {
            self.state = PredicateColumnReuseState::Readback;
            return Ok(());
        }
        for &row_idx in rows {
            let row_idx = row_idx.index();
            codes.extend_from_slice(batch.encoded_code(row_idx));
            nulls.push(u8::from(batch.is_null(row_idx)));
        }
        *utf8_verified &= batch.has_verified_utf8();
        Ok(())
    }

    fn take_prefix(&mut self, rows: usize) -> Result<Option<ColumnBatch>> {
        if matches!(self.state, PredicateColumnReuseState::Dictionary { .. }) {
            return self.take_dictionary_prefix(rows).map(Some);
        }
        let PredicateColumnReuseState::Collecting {
            data,
            nulls,
            row_ends,
            utf8_verified,
        } = &mut self.state
        else {
            return Ok(None);
        };
        if rows > nulls.len()
            || (matches!(self.encoding, PredicateColumnReuse::Varlen) && rows > row_ends.len())
        {
            return Err(paro_error::internal(
                "Reusable predicate column is shorter than the staged selection",
            ));
        }
        let data_len = match self.encoding {
            PredicateColumnReuse::Fixed { width } => rows.checked_mul(width).ok_or_else(|| {
                paro_error::internal("Reusable fixed predicate prefix size overflow")
            })?,
            PredicateColumnReuse::Varlen => {
                rows.checked_sub(1).map_or(0, |last_row| row_ends[last_row])
            }
        };
        if data_len > data.len() {
            return Err(paro_error::internal(
                "Reusable predicate data is shorter than the staged selection",
            ));
        }

        let remaining_data = data.split_off(data_len);
        let data = std::mem::replace(data, remaining_data);
        let remaining_nulls = nulls.split_off(rows);
        let nulls = std::mem::replace(nulls, remaining_nulls);
        if matches!(self.encoding, PredicateColumnReuse::Varlen) {
            let mut remaining_ends = row_ends.split_off(rows);
            for end in &mut remaining_ends {
                *end = end.checked_sub(data_len).ok_or_else(|| {
                    paro_error::internal("Reusable predicate row boundary moved backwards")
                })?;
            }
            *row_ends = remaining_ends;
        }
        let nulls = nulls
            .iter()
            .any(|is_null| *is_null != 0)
            .then(|| Bytes::from(nulls));
        let batch = ColumnBatch::new(Bytes::from(data), nulls);
        Ok(Some(
            if matches!(self.encoding, PredicateColumnReuse::Varlen) && *utf8_verified {
                batch.with_verified_utf8()
            } else {
                batch
            },
        ))
    }

    fn take_dictionary_prefix(&mut self, rows: usize) -> Result<ColumnBatch> {
        let PredicateColumnReuseState::Dictionary {
            dictionary,
            codes,
            nulls,
            utf8_verified,
        } = &mut self.state
        else {
            return Err(paro_error::internal(
                "Reusable dictionary column lost its dictionary state",
            ));
        };
        if rows > nulls.len() {
            return Err(paro_error::internal(
                "Reusable dictionary column is shorter than the staged selection",
            ));
        }
        let code_bytes = rows
            .checked_mul(std::mem::size_of::<u32>())
            .ok_or_else(|| paro_error::internal("Reusable dictionary code size overflow"))?;
        if code_bytes > codes.len() {
            return Err(paro_error::internal(
                "Reusable dictionary codes are shorter than the staged selection",
            ));
        }

        let remaining_codes = codes.split_off(code_bytes);
        let prefix_codes = std::mem::replace(codes, remaining_codes);
        let remaining_nulls = nulls.split_off(rows);
        let prefix_nulls = std::mem::replace(nulls, remaining_nulls);
        let nulls = prefix_nulls
            .iter()
            .any(|is_null| *is_null != 0)
            .then(|| Bytes::from(prefix_nulls));
        let batch = ColumnBatch::with_storage_dictionary(
            dictionary.clone(),
            Bytes::from(prefix_codes),
            nulls,
        );
        Ok(if *utf8_verified {
            batch.with_verified_utf8()
        } else {
            batch
        })
    }

    fn clear(&mut self) {
        match &mut self.state {
            PredicateColumnReuseState::Collecting {
                data,
                nulls,
                row_ends,
                utf8_verified,
            } => {
                data.clear();
                nulls.clear();
                row_ends.clear();
                *utf8_verified = true;
            }
            PredicateColumnReuseState::Dictionary {
                codes,
                nulls,
                utf8_verified,
                ..
            } => {
                codes.clear();
                nulls.clear();
                *utf8_verified = true;
            }
            PredicateColumnReuseState::Readback => {}
        }
    }
}

impl LateMaterializationState {
    fn new(
        column_iterators: &[(ColumnId, Box<dyn ColumnIterator + Send + Sync>)],
        evaluator: &PredicateEvaluator,
    ) -> Self {
        let reused_predicate_columns = column_iterators
            .iter()
            .filter_map(|(column_id, _)| {
                evaluator
                    .reusable_column_info(*column_id)
                    .map(|(predicate_idx, encoding)| {
                        ReusedPredicateColumn::new(*column_id, predicate_idx, encoding)
                    })
            })
            .collect();
        Self {
            rowids: Vec::new(),
            predicate_matches: Vec::new(),
            reused_predicate_columns,
        }
    }

    fn clear(&mut self) {
        self.rowids.clear();
        self.predicate_matches.clear();
        for reused in &mut self.reused_predicate_columns {
            reused.clear();
        }
    }

    fn take_rowids(&mut self, max_rows: usize) -> Vec<SegmentRowId> {
        let rows = max_rows.min(self.rowids.len());
        let remaining = self.rowids.split_off(rows);
        std::mem::replace(&mut self.rowids, remaining)
    }
}

impl SegmentBatch {
    fn empty() -> Self {
        Self {
            rowids: Vec::new(),
            rows: 0,
            physical_rows: 0,
            selection: None,
            columns: Vec::new(),
        }
    }
}

impl SegmentIterator {
    pub(super) fn new(segment: &Segment, column_ids: Vec<ColumnId>) -> Result<Self> {
        Self::new_with_prefetcher(segment, column_ids, None)
    }

    fn new_with_prefetcher(
        segment: &Segment,
        column_ids: Vec<ColumnId>,
        prefetcher: Option<Arc<Prefetcher>>,
    ) -> Result<Self> {
        let mut column_iterators = Vec::with_capacity(column_ids.len());
        for col_id in &column_ids {
            let iter = segment.new_column_iterator_with_prefetcher(*col_id, prefetcher.clone())?;
            column_iterators.push((*col_id, iter));
        }

        let buffer_pool = segment
            .options
            .page_cache
            .as_ref()
            .map(|cache| cache.buffer_pool());

        Ok(Self {
            segment_id: segment.segment_id,
            file_path: segment.file_path.clone(),
            column_iterators,
            current_ordinal: 0,
            num_rows: segment.num_rows(),
            end_ordinal: segment.num_rows(),
            options: segment.options.clone(),
            delete_vector: None,
            evaluated_selection: PredicateResult::Unknown,
            predicate_guaranteed: PredicateResult::NoneMatch,
            predicate_evaluator: None,
            late_materialization: None,
            sparse_batch_streak: 0,
            dense_batch_streak: 0,
            eager_predicate_matches: Vec::new(),
            selection_tracker: ColumnDataBytesTracker::new(buffer_pool.clone()),
            rowid_tracker: ColumnDataBytesTracker::new(buffer_pool),
            prefetcher,
            predicate_stage_read_stats: PredicateStageReadStats::default(),
        })
    }

    pub fn new_with_delete_vector(
        segment: &Segment,
        column_ids: Vec<ColumnId>,
        delete_vector: Option<DeleteVector>,
    ) -> Result<Self> {
        let mut iter = Self::new(segment, column_ids)?;
        iter.delete_vector = delete_vector;
        Ok(iter)
    }

    pub fn new_with_delete_vector_and_predicate(
        segment: &Segment,
        column_ids: Vec<ColumnId>,
        delete_vector: Option<DeleteVector>,
        predicate_tree: Option<PredicateTree>,
    ) -> Result<Self> {
        let mut iter = Self::new(segment, column_ids)?;
        iter.delete_vector = delete_vector;
        iter.initialize_predicate(segment, predicate_tree, None)?;
        Ok(iter)
    }

    pub fn new_with_delete_vector_predicate_and_prefetcher(
        segment: &Segment,
        column_ids: Vec<ColumnId>,
        delete_vector: Option<DeleteVector>,
        predicate_tree: Option<PredicateTree>,
        prefetcher: Option<Arc<Prefetcher>>,
    ) -> Result<Self> {
        let mut iter = Self::new_with_prefetcher(segment, column_ids, prefetcher)?;
        iter.delete_vector = delete_vector;
        iter.initialize_predicate(segment, predicate_tree, None)?;
        Ok(iter)
    }

    pub fn new_with_delete_vector_predicate_and_prefetcher_late_materialize(
        segment: &Segment,
        column_ids: Vec<ColumnId>,
        predicate_columns: Vec<ColumnId>,
        delete_vector: Option<DeleteVector>,
        predicate_tree: Option<PredicateTree>,
        prefetcher: Option<Arc<Prefetcher>>,
    ) -> Result<Self> {
        let mut iter = Self::new_with_prefetcher(segment, column_ids, prefetcher)?;
        iter.delete_vector = delete_vector;
        iter.initialize_predicate(segment, predicate_tree, Some(predicate_columns))?;
        Ok(iter)
    }

    fn initialize_predicate(
        &mut self,
        segment: &Segment,
        predicate_tree: Option<PredicateTree>,
        explicit_predicate_columns: Option<Vec<ColumnId>>,
    ) -> Result<()> {
        if let Some(tree) = predicate_tree {
            let use_late_materialization = explicit_predicate_columns.is_some();
            let evaluator = IndexEvaluator::new(segment.predicate_indexes());
            let needs_row_level_eval =
                PredicateEvaluator::predicate_tree_requires_row_verification(&tree)
                    || PredicateEvaluator::requires_row_level_predicate_eval(&evaluator, &tree);
            let index_evaluation = evaluator.evaluate_with_proof(&tree);
            self.predicate_guaranteed = index_evaluation.guaranteed;
            self.evaluated_selection = index_evaluation.candidates;
            if needs_row_level_eval {
                if !matches!(
                    self.evaluated_selection,
                    PredicateResult::NoneMatch | PredicateResult::AllMatch
                ) {
                    self.evaluated_selection = PredicateResult::Unknown;
                }
            }
            self.update_selection_tracker();
            self.predicate_evaluator = PredicateEvaluator::new(
                segment,
                tree,
                &evaluator,
                self.prefetcher.clone(),
                explicit_predicate_columns,
            )?;
            self.late_materialization = self.predicate_evaluator.as_ref().and_then(|evaluator| {
                (use_late_materialization
                    || !evaluator.all_columns_projected(&self.column_iterators))
                .then(|| LateMaterializationState::new(&self.column_iterators, evaluator))
            });
        }
        Ok(())
    }

    fn update_selection_tracker(&mut self) {
        match &self.evaluated_selection {
            PredicateResult::Bitmap(bitmap) => self.selection_tracker.set(bitmap.serialized_size()),
            _ => self.selection_tracker.reset(),
        }
    }

    #[cfg(test)]
    pub(crate) fn uses_late_materialize(&self) -> bool {
        self.late_materialization.is_some()
    }

    #[cfg(test)]
    pub(super) fn predicate_stage_read_stats(&self) -> PredicateStageReadStats {
        self.predicate_stage_read_stats.clone()
    }

    pub fn segment_id(&self) -> u32 {
        self.segment_id
    }

    pub fn current_ordinal(&self) -> u64 {
        self.current_ordinal
    }

    pub fn num_rows(&self) -> u64 {
        self.num_rows
    }

    pub fn has_next(&self) -> bool {
        self.current_ordinal < self.end_ordinal
            || self
                .late_materialization
                .as_ref()
                .is_some_and(|state| !state.rowids.is_empty())
    }

    /// Restrict this iterator to an ownership-disjoint ordinal range.
    /// Predicate bitmaps, delete vectors, and emitted row ids remain expressed
    /// in segment-relative ordinals, so no downstream remapping is required.
    pub fn set_ordinal_range(&mut self, start: u64, end: u64) -> Result<()> {
        if start > end || end > self.num_rows {
            return Err(paro_error::out_of_range(format!(
                "Segment ordinal range [{start}, {end}) exceeds row count {}",
                self.num_rows
            )));
        }
        self.end_ordinal = end;
        self.seek_to_ordinal(start)
    }

    pub fn seek_to_ordinal(&mut self, ordinal: u64) -> Result<()> {
        if ordinal > self.num_rows {
            return Err(paro_error::out_of_range(format!(
                "Ordinal {} out of range (max: {})",
                ordinal, self.num_rows
            )));
        }

        if let Some(state) = &mut self.late_materialization {
            state.clear();
        }
        self.seek_columns_to_ordinal(ordinal)
    }

    fn seek_columns_to_ordinal(&mut self, ordinal: u64) -> Result<()> {
        for (_, iter) in &mut self.column_iterators {
            iter.seek_to_ordinal(ordinal)?;
        }
        if let Some(predicate) = &mut self.predicate_evaluator {
            predicate.seek_to_ordinal(ordinal)?;
        }

        self.current_ordinal = ordinal;
        Ok(())
    }

    pub fn next_batch(
        &mut self,
        batch_size: usize,
    ) -> Result<(Vec<SegmentRowId>, Vec<(ColumnId, ColumnBatch)>)> {
        // This compact API cannot represent a logical selection over physical
        // column batches. Force the materialized path before reading so its
        // row ids and column values always have identical cardinality.
        if self.predicate_evaluator.is_some() && self.late_materialization.is_none() {
            let evaluator = self
                .predicate_evaluator
                .as_ref()
                .expect("predicate evaluator was checked");
            self.late_materialization = Some(LateMaterializationState::new(
                &self.column_iterators,
                evaluator,
            ));
        }
        let batch = self.next_batch_with_rowid_policy(batch_size, true)?;
        Ok((batch.rowids, batch.columns))
    }

    pub fn next_batch_with_rowid_policy(
        &mut self,
        batch_size: usize,
        materialize_sequential_rowids: bool,
    ) -> Result<SegmentBatch> {
        if self.predicate_evaluator.is_some() && self.late_materialization.is_some() {
            return self.next_batch_late_materialize(batch_size, materialize_sequential_rowids);
        }
        if self.predicate_evaluator.is_some() {
            return self.next_batch_eager_predicate(batch_size, materialize_sequential_rowids);
        }

        loop {
            if !self.has_next() {
                self.rowid_tracker.reset();
                return Ok(SegmentBatch::empty());
            }

            if matches!(self.evaluated_selection, PredicateResult::NoneMatch) {
                self.rowid_tracker.reset();
                return Ok(SegmentBatch::empty());
            }

            if let PredicateResult::PageRanges(ranges) = &self.evaluated_selection {
                let mut found = false;
                for range in ranges {
                    if self.current_ordinal < range.end_row as u64 {
                        if self.current_ordinal < range.start_row as u64 {
                            self.seek_to_ordinal(range.start_row as u64)?;
                        }
                        found = true;
                        break;
                    }
                }
                if !found {
                    self.current_ordinal = self.end_ordinal;
                    self.rowid_tracker.reset();
                    return Ok(SegmentBatch::empty());
                }
            }

            let selection_bitmap = match &self.evaluated_selection {
                PredicateResult::Bitmap(bm) => Some(bm),
                _ => None,
            };

            if selection_bitmap.is_some() || self.delete_vector.is_some() {
                let mut rowids = Vec::with_capacity(batch_size);
                let mut max_rowid = self.end_ordinal;
                if let PredicateResult::PageRanges(ranges) = &self.evaluated_selection {
                    for range in ranges {
                        if self.current_ordinal < range.end_row as u64 {
                            max_rowid = self.end_ordinal.min(range.end_row as u64);
                            break;
                        }
                    }
                }

                while rowids.len() < batch_size && self.current_ordinal < max_rowid {
                    let ord = self.current_ordinal;
                    self.current_ordinal += 1;

                    let matches_predicate =
                        selection_bitmap.is_none_or(|bm| bm.contains(ord as u32));
                    let not_deleted = self
                        .delete_vector
                        .as_ref()
                        .is_none_or(|dv| !dv.is_deleted(ord as u32));

                    if matches_predicate && not_deleted {
                        rowids.push(SegmentRowId::try_from_ordinal(ord)?);
                    }
                }

                if rowids.is_empty() {
                    if self.current_ordinal >= max_rowid && self.current_ordinal < self.end_ordinal
                    {
                        self.rowid_tracker.reset();
                        continue;
                    }
                    self.rowid_tracker.reset();
                    return Ok(SegmentBatch::empty());
                }

                if self.column_iterators.is_empty() {
                    self.rowid_tracker
                        .set(rowids.capacity() * std::mem::size_of::<u32>());
                    return Ok(SegmentBatch {
                        rows: rowids.len(),
                        physical_rows: rowids.len(),
                        selection: None,
                        rowids,
                        columns: Vec::new(),
                    });
                }

                let ordered_rowids = OrderedRowIds::try_new(&rowids)?;
                let mut results = Vec::with_capacity(self.column_iterators.len());
                for (col_id, iter) in &mut self.column_iterators {
                    let batch = iter.read_by_ordered_rowids(&ordered_rowids)?;
                    results.push((*col_id, batch));
                }

                self.rowid_tracker
                    .set(rowids.capacity() * std::mem::size_of::<u32>());
                return Ok(SegmentBatch {
                    rows: rowids.len(),
                    physical_rows: rowids.len(),
                    selection: None,
                    rowids,
                    columns: results,
                });
            }

            let start_ordinal = self.current_ordinal;
            let mut effective_batch_size =
                batch_size.min((self.end_ordinal - self.current_ordinal) as usize);
            if let PredicateResult::PageRanges(ranges) = &self.evaluated_selection {
                for range in ranges {
                    if self.current_ordinal < range.end_row as u64 {
                        let remaining_in_range =
                            (range.end_row as u64 - self.current_ordinal) as usize;
                        effective_batch_size = batch_size.min(remaining_in_range);
                        break;
                    }
                }
            }

            if self.column_iterators.is_empty() {
                let remaining = (self.end_ordinal - self.current_ordinal) as usize;
                let to_read = effective_batch_size.min(remaining);
                let rowids = if to_read == 0 || !materialize_sequential_rowids {
                    Vec::new()
                } else {
                    contiguous_segment_rowids(start_ordinal, to_read)?
                };
                self.current_ordinal += to_read as u64;
                if rowids.is_empty() {
                    self.rowid_tracker.reset();
                } else {
                    self.rowid_tracker
                        .set(rowids.capacity() * std::mem::size_of::<u32>());
                }
                return Ok(SegmentBatch {
                    rowids,
                    rows: to_read,
                    physical_rows: to_read,
                    selection: None,
                    columns: Vec::new(),
                });
            }

            let mut results = Vec::with_capacity(self.column_iterators.len());
            let mut rows_read = 0usize;
            for (col_id, iter) in &mut self.column_iterators {
                let (count, batch) = iter.next_batch(effective_batch_size)?;
                if count > 0 {
                    results.push((*col_id, batch));
                    rows_read = count;
                }
            }
            if let Some((_, iter)) = self.column_iterators.first() {
                self.current_ordinal = iter.current_ordinal();
            } else {
                self.current_ordinal += rows_read as u64;
            }
            let rowids: Vec<SegmentRowId> = if materialize_sequential_rowids {
                contiguous_segment_rowids(start_ordinal, rows_read)?
            } else {
                Vec::new()
            };
            if rowids.is_empty() {
                self.rowid_tracker.reset();
            } else {
                self.rowid_tracker
                    .set(rowids.capacity() * std::mem::size_of::<u32>());
            }
            return Ok(SegmentBatch {
                rowids,
                rows: rows_read,
                physical_rows: rows_read,
                selection: None,
                columns: results,
            });
        }
    }

    fn next_batch_eager_predicate(
        &mut self,
        batch_size: usize,
        materialize_sequential_rowids: bool,
    ) -> Result<SegmentBatch> {
        if batch_size == 0 {
            return Ok(SegmentBatch::empty());
        }

        loop {
            if self.current_ordinal >= self.end_ordinal
                || matches!(self.evaluated_selection, PredicateResult::NoneMatch)
            {
                return Ok(SegmentBatch::empty());
            }

            if let PredicateResult::PageRanges(ranges) = &self.evaluated_selection {
                let Some(range) = ranges
                    .iter()
                    .find(|range| self.current_ordinal < range.end_row as u64)
                else {
                    self.current_ordinal = self.end_ordinal;
                    return Ok(SegmentBatch::empty());
                };
                if self.current_ordinal < range.start_row as u64 {
                    self.seek_columns_to_ordinal(range.start_row as u64)?;
                }
            }

            let start_ordinal = self.current_ordinal;
            let mut to_read = batch_size.min((self.end_ordinal - start_ordinal) as usize);
            if let PredicateResult::PageRanges(ranges) = &self.evaluated_selection {
                if let Some(range) = ranges
                    .iter()
                    .find(|range| start_ordinal < range.end_row as u64)
                {
                    to_read = to_read.min((range.end_row as u64 - start_ordinal) as usize);
                }
            }
            let (predicate_guaranteed, proof_end) =
                predicate_proof_span(&self.predicate_guaranteed, start_ordinal, self.end_ordinal);
            to_read = to_read.min((proof_end - start_ordinal) as usize);

            let mut columns = Vec::with_capacity(self.column_iterators.len());
            let mut rows_read = None;
            for (column_id, iterator) in &mut self.column_iterators {
                let (count, batch) = iterator.next_batch(to_read)?;
                if rows_read.is_some_and(|expected| expected != count) {
                    return Err(paro_error::data_corrupted(
                        "Eager predicate column row count mismatch",
                    ));
                }
                rows_read = Some(count);
                columns.push((*column_id, batch));
            }
            let mut predicate_batches_read = None;
            let rows_read = match rows_read {
                Some(rows_read) => rows_read,
                None if predicate_guaranteed => to_read,
                None => {
                    let (rows_read, batches) = self
                        .predicate_evaluator
                        .as_mut()
                        .expect("eager predicate mode requires an evaluator")
                        .read_next_batch(to_read)?;
                    predicate_batches_read = Some(batches);
                    rows_read
                }
            };
            if predicate_guaranteed {
                // Index proof replaces row-level evaluation, but the
                // independent predicate readers still have to cross the same
                // ordinal span. Seeking preserves alignment without decoding
                // values whose truth is already proven.
                self.predicate_evaluator
                    .as_mut()
                    .expect("eager predicate mode requires an evaluator")
                    .seek_to_ordinal(start_ordinal + rows_read as u64)?;
            }
            if rows_read == 0 {
                self.current_ordinal = start_ordinal + to_read as u64;
                continue;
            }
            self.current_ordinal = start_ordinal + rows_read as u64;

            if predicate_guaranteed {
                self.eager_predicate_matches.clear();
                self.eager_predicate_matches
                    .extend((0..rows_read).map(BatchRowOrdinal::from_index));
            } else {
                let evaluator = self
                    .predicate_evaluator
                    .as_ref()
                    .expect("eager predicate mode requires an evaluator");
                if let Some(predicate_batches) = predicate_batches_read.as_ref() {
                    evaluator.evaluate_batch(
                        predicate_batches,
                        rows_read,
                        &mut self.eager_predicate_matches,
                    )?;
                } else if let Some(predicate_batches) =
                    evaluator.prepare_projected_batches(&columns, rows_read)?
                {
                    evaluator.evaluate_batch(
                        &predicate_batches,
                        rows_read,
                        &mut self.eager_predicate_matches,
                    )?;
                } else {
                    let (predicate_rows, predicate_batches) = self
                        .predicate_evaluator
                        .as_mut()
                        .expect("eager predicate mode requires an evaluator")
                        .read_next_batch(rows_read)?;
                    if predicate_rows != rows_read {
                        return Err(paro_error::data_corrupted(
                            "Eager predicate reader row count mismatch",
                        ));
                    }
                    self.predicate_evaluator
                        .as_ref()
                        .expect("eager predicate mode requires an evaluator")
                        .evaluate_batch(
                            &predicate_batches,
                            rows_read,
                            &mut self.eager_predicate_matches,
                        )?;
                }
            }

            let selection_bitmap = match &self.evaluated_selection {
                PredicateResult::Bitmap(bitmap) => Some(bitmap),
                _ => None,
            };
            if selection_bitmap.is_some() || self.delete_vector.is_some() {
                self.eager_predicate_matches.retain(|&row_idx| {
                    let ordinal = start_ordinal + u64::from(row_idx.get());
                    selection_bitmap.is_none_or(|bitmap| bitmap.contains(ordinal as u32))
                        && self
                            .delete_vector
                            .as_ref()
                            .is_none_or(|deletes| !deletes.is_deleted(ordinal as u32))
                });
            }
            if self.eager_predicate_matches.is_empty() {
                continue;
            }

            let all_match = self.eager_predicate_matches.len() == rows_read;
            let sparse_batch = !materialize_sequential_rowids
                && !self
                    .options
                    .scan_access_cost
                    .sequential_materialization_is_cheaper(
                        self.eager_predicate_matches.len(),
                        rows_read,
                    );
            if sparse_batch {
                self.sparse_batch_streak = self.sparse_batch_streak.saturating_add(1);
                self.dense_batch_streak = 0;
            } else {
                self.sparse_batch_streak = 0;
            }
            if self.sparse_batch_streak >= 2 {
                let evaluator = self
                    .predicate_evaluator
                    .as_ref()
                    .expect("eager predicate mode requires an evaluator");
                self.late_materialization = Some(LateMaterializationState::new(
                    &self.column_iterators,
                    evaluator,
                ));
                // Eager evaluation may have reused projected predicate columns,
                // leaving the independent predicate readers at the previous
                // ordinal. Synchronize every reader before the next batch is
                // dispatched through the late path; seeking to the output
                // iterators' current ordinal does not decode the current batch
                // again.
                self.seek_columns_to_ordinal(self.current_ordinal)?;
                self.sparse_batch_streak = 0;
            }
            let selection = (!all_match).then(|| self.eager_predicate_matches.clone());
            let rowids = if materialize_sequential_rowids {
                self.eager_predicate_matches
                    .iter()
                    .map(|&row| {
                        SegmentRowId::try_from_ordinal(start_ordinal + u64::from(row.get()))
                    })
                    .collect::<Result<Vec<_>>>()?
            } else {
                Vec::new()
            };
            if rowids.is_empty() {
                self.rowid_tracker.reset();
            } else {
                self.rowid_tracker
                    .set(rowids.capacity() * std::mem::size_of::<u32>());
            }
            return Ok(SegmentBatch {
                rowids,
                rows: self.eager_predicate_matches.len(),
                physical_rows: rows_read,
                selection,
                columns,
            });
        }
    }

    fn next_batch_late_materialize(
        &mut self,
        batch_size: usize,
        materialize_sequential_rowids: bool,
    ) -> Result<SegmentBatch> {
        if batch_size == 0 {
            self.rowid_tracker.reset();
            return Ok(SegmentBatch::empty());
        }

        loop {
            if matches!(self.evaluated_selection, PredicateResult::NoneMatch) {
                self.current_ordinal = self.end_ordinal;
                if let Some(state) = &mut self.late_materialization {
                    state.clear();
                }
                self.rowid_tracker.reset();
                return Ok(SegmentBatch::empty());
            }

            let staged_rows = self
                .late_materialization
                .as_ref()
                .expect("late materialization requires selection state")
                .rowids
                .len();
            if staged_rows >= batch_size
                || (self.current_ordinal >= self.end_ordinal && staged_rows > 0)
            {
                return self
                    .materialize_staged_selection(batch_size, materialize_sequential_rowids);
            }
            if self.current_ordinal >= self.end_ordinal {
                self.rowid_tracker.reset();
                return Ok(SegmentBatch::empty());
            }

            if let PredicateResult::PageRanges(ranges) = &self.evaluated_selection {
                let mut found = false;
                for range in ranges {
                    if self.current_ordinal < range.end_row as u64 {
                        if self.current_ordinal < range.start_row as u64 {
                            self.seek_columns_to_ordinal(range.start_row as u64)?;
                        }
                        found = true;
                        break;
                    }
                }
                if !found {
                    self.current_ordinal = self.end_ordinal;
                    continue;
                }
            }

            let mut max_rowid = self.end_ordinal;
            if let PredicateResult::PageRanges(ranges) = &self.evaluated_selection {
                for range in ranges {
                    if self.current_ordinal < range.end_row as u64 {
                        max_rowid = self.end_ordinal.min(range.end_row as u64);
                        break;
                    }
                }
            }

            let start_ordinal = self.current_ordinal;
            let (predicate_guaranteed, proof_end) =
                predicate_proof_span(&self.predicate_guaranteed, start_ordinal, max_rowid);
            let remaining = (proof_end - start_ordinal) as usize;
            let to_read = batch_size.min(remaining);
            let staged_program = !predicate_guaranteed
                && self
                    .predicate_evaluator
                    .as_ref()
                    .expect("late materialization requires predicate evaluator")
                    .has_staged_program();
            let mut staged_matches = None;
            let (rows_read, predicate_batches) = if predicate_guaranteed {
                self.predicate_evaluator
                    .as_mut()
                    .expect("late materialization requires predicate evaluator")
                    .seek_to_ordinal(start_ordinal + to_read as u64)?;
                (to_read, Vec::new())
            } else if staged_program {
                let mut matches = std::mem::take(
                    &mut self
                        .late_materialization
                        .as_mut()
                        .expect("late materialization requires selection state")
                        .predicate_matches,
                );
                let rows_read = self
                    .predicate_evaluator
                    .as_mut()
                    .expect("late materialization requires predicate evaluator")
                    .evaluate_staged_batch(
                        start_ordinal,
                        to_read,
                        self.options.scan_access_cost,
                        &mut matches,
                        &mut self.predicate_stage_read_stats,
                    )?;
                staged_matches = Some(matches);
                (rows_read, Vec::new())
            } else {
                self.predicate_evaluator
                    .as_mut()
                    .expect("late materialization requires predicate evaluator")
                    .read_next_batch(to_read)?
            };
            if rows_read == 0 {
                self.current_ordinal = max_rowid;
                continue;
            }

            {
                let state = self
                    .late_materialization
                    .as_mut()
                    .expect("late materialization requires selection state");
                if predicate_guaranteed {
                    state.predicate_matches.clear();
                    state
                        .predicate_matches
                        .extend((0..rows_read).map(BatchRowOrdinal::from_index));
                } else if let Some(matches) = staged_matches.take() {
                    state.predicate_matches = matches;
                } else {
                    self.predicate_evaluator
                        .as_ref()
                        .expect("late materialization requires predicate evaluator")
                        .evaluate_batch(
                            &predicate_batches,
                            rows_read,
                            &mut state.predicate_matches,
                        )?;
                }
            }
            let selection_bitmap = match &self.evaluated_selection {
                PredicateResult::Bitmap(bitmap) => Some(bitmap),
                _ => None,
            };
            let state = self
                .late_materialization
                .as_mut()
                .expect("late materialization requires selection state");
            state.predicate_matches.retain(|&row_idx| {
                let ordinal = self.current_ordinal + u64::from(row_idx.get());
                !(selection_bitmap.is_some_and(|bitmap| !bitmap.contains(ordinal as u32))
                    || self
                        .delete_vector
                        .as_ref()
                        .is_some_and(|deletes| deletes.is_deleted(ordinal as u32)))
            });
            let dense_matches = (!materialize_sequential_rowids
                && state.rowids.is_empty()
                && self
                    .options
                    .scan_access_cost
                    .sequential_materialization_is_cheaper(
                        state.predicate_matches.len(),
                        rows_read,
                    ))
            .then(|| std::mem::take(&mut state.predicate_matches));
            if let Some(predicate_matches) = dense_matches {
                self.dense_batch_streak = self.dense_batch_streak.saturating_add(1);
                self.sparse_batch_streak = 0;
                let dense_batch =
                    self.materialize_dense_selection(start_ordinal, rows_read, &predicate_matches)?;
                self.current_ordinal += rows_read as u64;
                if self.dense_batch_streak >= 2 {
                    self.late_materialization = None;
                    self.dense_batch_streak = 0;
                }
                return Ok(dense_batch);
            }
            self.dense_batch_streak = 0;
            for reused in &mut state.reused_predicate_columns {
                let batch = predicate_batches
                    .get(reused.predicate_idx)
                    .ok_or_else(|| paro_error::internal("Reusable predicate batch missing"))?;
                reused.append_rows(batch, &state.predicate_matches)?;
            }
            state.rowids.reserve(state.predicate_matches.len());
            for &row_idx in &state.predicate_matches {
                let ordinal = self.current_ordinal + u64::from(row_idx.get());
                state.rowids.push(SegmentRowId::try_from_ordinal(ordinal)?);
            }
            self.current_ordinal += rows_read as u64;
        }
    }

    fn materialize_dense_selection(
        &mut self,
        start_ordinal: u64,
        physical_rows: usize,
        predicate_matches: &[BatchRowOrdinal],
    ) -> Result<SegmentBatch> {
        let mut columns = Vec::with_capacity(self.column_iterators.len());
        for (column_id, iterator) in &mut self.column_iterators {
            if iterator.current_ordinal() != start_ordinal {
                iterator.seek_to_ordinal(start_ordinal)?;
            }
            let (rows_read, batch) = iterator.next_batch(physical_rows)?;
            if rows_read != physical_rows {
                return Err(paro_error::data_corrupted(
                    "Dense predicate column row count mismatch",
                ));
            }
            columns.push((*column_id, batch));
        }

        self.rowid_tracker.reset();
        let selection =
            (predicate_matches.len() != physical_rows).then(|| predicate_matches.to_vec());
        Ok(SegmentBatch {
            rowids: Vec::new(),
            rows: predicate_matches.len(),
            physical_rows,
            selection,
            columns,
        })
    }

    fn materialize_staged_selection(
        &mut self,
        batch_size: usize,
        materialize_sequential_rowids: bool,
    ) -> Result<SegmentBatch> {
        let state = self
            .late_materialization
            .as_mut()
            .expect("late materialization requires selection state");
        let rowids = state.take_rowids(batch_size);
        let rows = rowids.len();
        self.rowid_tracker
            .set(rowids.capacity() * std::mem::size_of::<u32>());

        if self.column_iterators.is_empty() {
            let returned_rowids = if materialize_sequential_rowids {
                rowids
            } else {
                self.rowid_tracker.reset();
                Vec::new()
            };
            return Ok(SegmentBatch {
                rows,
                physical_rows: rows,
                selection: None,
                rowids: returned_rowids,
                columns: Vec::new(),
            });
        }

        let ordered_rowids = OrderedRowIds::try_new(&rowids)?;
        let mut columns = Vec::with_capacity(self.column_iterators.len());
        for (column_id, iter) in &mut self.column_iterators {
            let reused_batch = state
                .reused_predicate_columns
                .iter_mut()
                .find(|reused| reused.column_id == *column_id)
                .map(|reused| reused.take_prefix(rows))
                .transpose()?
                .flatten();
            let batch = match reused_batch {
                Some(batch) => batch,
                None => iter.read_by_ordered_rowids(&ordered_rowids)?,
            };
            columns.push((*column_id, batch));
        }

        let returned_rowids = if materialize_sequential_rowids {
            rowids
        } else {
            Vec::new()
        };
        if returned_rowids.is_empty() {
            self.rowid_tracker.reset();
        }
        Ok(SegmentBatch {
            rows,
            physical_rows: rows,
            selection: None,
            rowids: returned_rowids,
            columns,
        })
    }

    pub fn num_columns(&self) -> usize {
        self.column_iterators.len()
    }
}

impl std::fmt::Debug for SegmentIterator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SegmentIterator")
            .field("segment_id", &self.segment_id)
            .field("file_path", &self.file_path)
            .field("current_ordinal", &self.current_ordinal)
            .field("num_rows", &self.num_rows)
            .field("end_ordinal", &self.end_ordinal)
            .field("num_columns", &self.column_iterators.len())
            .field("late_materialize", &self.predicate_evaluator.is_some())
            .field("prefetcher", &self.prefetcher.is_some())
            .field("options", &self.options)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rowset::encoding::BinaryPlainPageBuilder;
    use crate::rowset::segment::predicate_column::{PredicateColumnAccess, PredicateColumnBatch};
    use crate::test_utils::{test_i32_vector, test_nullable_string_vector};

    #[test]
    fn unproven_spans_do_not_fragment_row_level_batches() {
        let proof = PredicateResult::PageRanges(vec![crate::index::PageRange::new(10, 20)]);
        assert_eq!(predicate_proof_span(&proof, 0, 30), (false, 30));
        assert_eq!(predicate_proof_span(&proof, 10, 30), (true, 20));
        assert_eq!(predicate_proof_span(&proof, 20, 30), (false, 30));
    }

    #[test]
    fn decoded_predicate_batch_disables_column_reuse_for_readback() {
        let mut reused = ReusedPredicateColumn::new(7, 0, PredicateColumnReuse::Fixed { width: 4 });
        let raw = PredicateColumnBatch::Raw(ColumnBatch::new(
            Bytes::copy_from_slice(&11_i32.to_le_bytes()),
            None,
        ));
        reused
            .append_rows(&raw, &[BatchRowOrdinal::from_index(0)])
            .unwrap();

        let dictionary_decoded = PredicateColumnBatch::Decoded(test_i32_vector(&[11]));
        reused
            .append_rows(&dictionary_decoded, &[BatchRowOrdinal::from_index(0)])
            .unwrap();

        assert!(reused.take_prefix(1).unwrap().is_none());
    }

    #[test]
    fn fixed_predicate_values_are_reused_across_output_batches() {
        let mut reused = ReusedPredicateColumn::new(7, 0, PredicateColumnReuse::Fixed { width: 4 });
        let values = [10_i32, 20, 30]
            .into_iter()
            .flat_map(i32::to_le_bytes)
            .collect::<Vec<_>>();
        let raw = PredicateColumnBatch::Raw(ColumnBatch::new(Bytes::from(values), None));
        reused
            .append_rows(&raw, &[2, 0, 1].map(BatchRowOrdinal::from_index))
            .unwrap();

        let first = reused.take_prefix(2).unwrap().expect("reused prefix");
        assert_eq!(
            first.data.as_ref(),
            &[30_i32.to_le_bytes(), 10_i32.to_le_bytes()].concat()
        );

        let second = reused.take_prefix(1).unwrap().expect("reused suffix");
        assert_eq!(second.data.as_ref(), 20_i32.to_le_bytes());
    }

    #[test]
    fn decoded_varlen_predicate_values_are_reused_across_output_batches() {
        let mut reused = ReusedPredicateColumn::new(7, 0, PredicateColumnReuse::Varlen);
        let decoded = PredicateColumnBatch::Decoded(test_nullable_string_vector(&[
            Some("alpha"),
            None,
            Some("a value longer than the inline string capacity"),
        ]));
        reused
            .append_rows(&decoded, &[0, 1, 2].map(BatchRowOrdinal::from_index))
            .unwrap();

        let first = reused.take_prefix(2).unwrap().expect("reused prefix");
        assert_eq!(
            first.varlen_row(0).unwrap().as_deref(),
            Some(b"alpha".as_slice())
        );
        assert_eq!(first.varlen_row(1).unwrap(), None);

        let second = reused.take_prefix(1).unwrap().expect("reused suffix");
        assert_eq!(
            second.varlen_row(0).unwrap().as_deref(),
            Some(b"a value longer than the inline string capacity".as_slice())
        );
    }

    #[test]
    fn storage_dictionary_predicate_reuse_preserves_dictionary_codes() {
        let mut dictionary = BinaryPlainPageBuilder::new(1024);
        for value in [b"alpha".as_slice(), b"beta", b"gamma"] {
            assert!(dictionary.add_slice(value));
        }
        let dictionary = dictionary.finish().unwrap();
        let codes = [2_u32, 0, 1]
            .into_iter()
            .flat_map(u32::to_le_bytes)
            .collect::<Vec<_>>();
        let batch = PredicateColumnBatch::prepare(
            &paro_common::types::LogicalType::Varchar,
            PredicateColumnAccess::Typed { raw_width: None },
            ColumnBatch::with_storage_dictionary(dictionary, Bytes::from(codes), None)
                .with_verified_utf8(),
            3,
            paro_common::test_utils::test_allocator(),
        )
        .unwrap();

        let mut reused = ReusedPredicateColumn::new(7, 0, PredicateColumnReuse::Varlen);
        reused
            .append_rows(&batch, &[2, 0, 2].map(BatchRowOrdinal::from_index))
            .unwrap();

        let first = reused.take_prefix(2).unwrap().expect("reused prefix");
        assert!(first.storage_dictionary.is_some());
        assert_eq!(
            first.varlen_row(0).unwrap().as_deref(),
            Some(b"beta".as_slice())
        );
        assert_eq!(
            first.varlen_row(1).unwrap().as_deref(),
            Some(b"gamma".as_slice())
        );

        let second = reused.take_prefix(1).unwrap().expect("reused suffix");
        assert!(second.storage_dictionary.is_some());
        assert_eq!(
            second.varlen_row(0).unwrap().as_deref(),
            Some(b"beta".as_slice())
        );
    }
}
