// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::segment::{Segment, SegmentOptions};
use super::segment_predicate::PredicateEvaluator;
use crate::buffer::{BufferPool, Prefetcher};
use crate::index::{IndexEvaluator, PredicateResult, PredicateTree};
use crate::primary_key::DeleteVector;
use crate::rowset::column::{ColumnBatch, ColumnIterator};
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
    predicate_evaluator: Option<PredicateEvaluator>,
    selection_tracker: ColumnDataBytesTracker,
    rowid_tracker: ColumnDataBytesTracker,
    prefetcher: Option<Arc<Prefetcher>>,
}

pub struct SegmentBatch {
    pub rowids: Vec<u32>,
    pub rows: usize,
    pub columns: Vec<(ColumnId, ColumnBatch)>,
}

struct ReusedPredicateColumn {
    column_id: ColumnId,
    predicate_idx: usize,
    width: usize,
    data: Vec<u8>,
    nulls: Vec<u8>,
}

impl ReusedPredicateColumn {
    fn new(column_id: ColumnId, predicate_idx: usize, width: usize, capacity: usize) -> Self {
        Self {
            column_id,
            predicate_idx,
            width,
            data: Vec::with_capacity(capacity.saturating_mul(width)),
            nulls: Vec::with_capacity(capacity),
        }
    }

    fn append(&mut self, batch: &ColumnBatch, row_idx: usize) -> Result<()> {
        let start = row_idx
            .checked_mul(self.width)
            .ok_or_else(|| paro_error::data_corrupted("Predicate row offset overflow"))?;
        let end = start
            .checked_add(self.width)
            .ok_or_else(|| paro_error::data_corrupted("Predicate row width overflow"))?;
        let value = batch.data.get(start..end).ok_or_else(|| {
            paro_error::data_corrupted("Predicate row exceeds the fixed-width batch")
        })?;
        self.data.extend_from_slice(value);
        self.nulls
            .push(batch.nulls.as_ref().map_or(0, |nulls| nulls[row_idx]));
        Ok(())
    }

    fn take_batch(&mut self) -> ColumnBatch {
        let nulls = std::mem::take(&mut self.nulls);
        let nulls = nulls
            .iter()
            .any(|is_null| *is_null != 0)
            .then(|| Bytes::from(nulls));
        ColumnBatch::new(Bytes::from(std::mem::take(&mut self.data)), nulls)
    }
}

impl SegmentBatch {
    fn empty() -> Self {
        Self {
            rowids: Vec::new(),
            rows: 0,
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
            predicate_evaluator: None,
            selection_tracker: ColumnDataBytesTracker::new(buffer_pool.clone()),
            rowid_tracker: ColumnDataBytesTracker::new(buffer_pool),
            prefetcher,
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
            let evaluator = IndexEvaluator::new(segment.predicate_indexes());
            let needs_row_level_eval =
                PredicateEvaluator::predicate_tree_requires_row_verification(&tree)
                    || PredicateEvaluator::requires_row_level_predicate_eval(&evaluator, &tree);
            self.evaluated_selection = evaluator.evaluate(&tree);
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
        self.predicate_evaluator.is_some()
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
    ) -> Result<(Vec<u32>, Vec<(ColumnId, ColumnBatch)>)> {
        let batch = self.next_batch_with_rowid_policy(batch_size, true)?;
        Ok((batch.rowids, batch.columns))
    }

    pub fn next_batch_with_rowid_policy(
        &mut self,
        batch_size: usize,
        materialize_sequential_rowids: bool,
    ) -> Result<SegmentBatch> {
        if self.predicate_evaluator.is_some() {
            return self.next_batch_late_materialize(batch_size);
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
                        rowids.push(ord as u32);
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
                        rowids,
                        columns: Vec::new(),
                    });
                }

                let rowids_u64: Vec<u64> = rowids.iter().map(|&id| id as u64).collect();
                let mut results = Vec::with_capacity(self.column_iterators.len());
                for (col_id, iter) in &mut self.column_iterators {
                    let batch = iter.read_by_rowids(&rowids_u64)?;
                    results.push((*col_id, batch));
                }

                self.rowid_tracker
                    .set(rowids.capacity() * std::mem::size_of::<u32>());
                return Ok(SegmentBatch {
                    rows: rowids.len(),
                    rowids,
                    columns: results,
                });
            }

            let start_ord = self.current_ordinal as u32;
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
                    (start_ord..start_ord + to_read as u32).collect()
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
            let rowids: Vec<u32> = if materialize_sequential_rowids {
                (start_ord..start_ord + rows_read as u32).collect()
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
                columns: results,
            });
        }
    }

    fn next_batch_late_materialize(&mut self, batch_size: usize) -> Result<SegmentBatch> {
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

            let mut max_rowid = self.end_ordinal;
            if let PredicateResult::PageRanges(ranges) = &self.evaluated_selection {
                for range in ranges {
                    if self.current_ordinal < range.end_row as u64 {
                        max_rowid = self.end_ordinal.min(range.end_row as u64);
                        break;
                    }
                }
            }

            let mut rowids = Vec::with_capacity(batch_size);
            let mut predicate_matches = Vec::with_capacity(batch_size);
            let mut reused_predicate_columns = self
                .column_iterators
                .iter()
                .filter_map(|(column_id, _)| {
                    self.predicate_evaluator
                        .as_ref()
                        .and_then(|evaluator| evaluator.raw_column_info(*column_id))
                        .map(|(predicate_idx, width)| {
                            ReusedPredicateColumn::new(*column_id, predicate_idx, width, batch_size)
                        })
                })
                .collect::<Vec<_>>();
            while rowids.len() < batch_size && self.current_ordinal < max_rowid {
                let remaining = (max_rowid - self.current_ordinal) as usize;
                let output_remaining = batch_size - rowids.len();
                let to_read = output_remaining.min(remaining);
                if to_read == 0 {
                    break;
                }

                let (rows_read, vectors_by_col) = self
                    .predicate_evaluator
                    .as_mut()
                    .expect("late materialization requires predicate evaluator")
                    .read_next_batch(to_read)?;
                if rows_read == 0 {
                    self.current_ordinal = max_rowid;
                    break;
                }

                let selection_bitmap = match &self.evaluated_selection {
                    PredicateResult::Bitmap(bm) => Some(bm),
                    _ => None,
                };

                self.predicate_evaluator
                    .as_ref()
                    .expect("late materialization requires predicate evaluator")
                    .evaluate_batch(&vectors_by_col, rows_read, &mut predicate_matches)?;

                for &row_idx in &predicate_matches {
                    let ord = self.current_ordinal + row_idx as u64;
                    let matches_index = selection_bitmap.is_none_or(|bm| bm.contains(ord as u32));
                    if !matches_index {
                        continue;
                    }
                    let not_deleted = self
                        .delete_vector
                        .as_ref()
                        .is_none_or(|dv| !dv.is_deleted(ord as u32));
                    if !not_deleted {
                        continue;
                    }

                    for reused in &mut reused_predicate_columns {
                        let batch = vectors_by_col
                            .get(reused.predicate_idx)
                            .and_then(|batch| batch.raw())
                            .ok_or_else(|| {
                                paro_error::internal(
                                    "Reusable predicate column was decoded unexpectedly",
                                )
                            })?;
                        reused.append(batch, row_idx)?;
                    }
                    rowids.push(ord as u32);
                    if rowids.len() >= batch_size {
                        break;
                    }
                }

                self.current_ordinal += rows_read as u64;
                if rows_read < to_read {
                    break;
                }
            }

            if rowids.is_empty() {
                if self.current_ordinal >= max_rowid && self.current_ordinal < self.end_ordinal {
                    self.rowid_tracker.reset();
                    continue;
                }
                self.rowid_tracker.reset();
                return Ok(SegmentBatch::empty());
            }

            self.rowid_tracker
                .set(rowids.capacity() * std::mem::size_of::<u32>());

            if self.column_iterators.is_empty() {
                return Ok(SegmentBatch {
                    rows: rowids.len(),
                    rowids,
                    columns: Vec::new(),
                });
            }

            let rowids_u64: Vec<u64> = rowids.iter().map(|&id| id as u64).collect();
            let mut results = Vec::with_capacity(self.column_iterators.len());
            for (col_id, iter) in &mut self.column_iterators {
                let batch = if let Some(reused) = reused_predicate_columns
                    .iter_mut()
                    .find(|reused| reused.column_id == *col_id)
                {
                    reused.take_batch()
                } else {
                    iter.read_by_rowids(&rowids_u64)?
                };
                results.push((*col_id, batch));
            }

            return Ok(SegmentBatch {
                rows: rowids.len(),
                rowids,
                columns: results,
            });
        }
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
