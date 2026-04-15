// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::segment::{Segment, SegmentOptions};
use super::segment_predicate::PredicateEvaluator;
use crate::buffer::{BufferPool, Prefetcher};
use crate::index::{IndexEvaluator, PredicateResult, PredicateTree};
use crate::primary_key::DeleteVector;
use crate::rowset::column::{ColumnBatch, ColumnIterator};
use crate::tablet::ColumnId;
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
    options: SegmentOptions,
    delete_vector: Option<DeleteVector>,
    pub(super) evaluated_selection: PredicateResult,
    predicate_evaluator: Option<PredicateEvaluator>,
    selection_tracker: ColumnDataBytesTracker,
    rowid_tracker: ColumnDataBytesTracker,
    prefetcher: Option<Arc<Prefetcher>>,
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
            self.evaluated_selection = evaluator.evaluate(&tree);
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
        self.current_ordinal < self.num_rows
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
        if self.predicate_evaluator.is_some() {
            return self.next_batch_late_materialize(batch_size);
        }

        loop {
            if !self.has_next() {
                self.rowid_tracker.reset();
                return Ok((Vec::new(), Vec::new()));
            }

            if matches!(self.evaluated_selection, PredicateResult::NoneMatch) {
                self.rowid_tracker.reset();
                return Ok((Vec::new(), Vec::new()));
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
                    self.current_ordinal = self.num_rows;
                    return Ok((Vec::new(), Vec::new()));
                }
            }

            let selection_bitmap = match &self.evaluated_selection {
                PredicateResult::Bitmap(bm) => Some(bm),
                _ => None,
            };

            if selection_bitmap.is_some() || self.delete_vector.is_some() {
                let mut rowids = Vec::with_capacity(batch_size);
                let mut max_rowid = self.num_rows;
                if let PredicateResult::PageRanges(ranges) = &self.evaluated_selection {
                    for range in ranges {
                        if self.current_ordinal < range.end_row as u64 {
                            max_rowid = range.end_row as u64;
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
                    if self.current_ordinal >= max_rowid && self.current_ordinal < self.num_rows {
                        self.rowid_tracker.reset();
                        continue;
                    }
                    self.rowid_tracker.reset();
                    return Ok((Vec::new(), Vec::new()));
                }

                if self.column_iterators.is_empty() {
                    self.rowid_tracker
                        .set(rowids.capacity() * std::mem::size_of::<u32>());
                    return Ok((rowids, Vec::new()));
                }

                let rowids_u64: Vec<u64> = rowids.iter().map(|&id| id as u64).collect();
                let mut results = Vec::with_capacity(self.column_iterators.len());
                for (col_id, iter) in &mut self.column_iterators {
                    let batch = iter.read_by_rowids(&rowids_u64)?;
                    results.push((*col_id, batch));
                }

                self.rowid_tracker
                    .set(rowids.capacity() * std::mem::size_of::<u32>());
                return Ok((rowids, results));
            }

            let start_ord = self.current_ordinal as u32;
            let mut effective_batch_size = batch_size;
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
                let remaining = (self.num_rows - self.current_ordinal) as usize;
                let to_read = effective_batch_size.min(remaining);
                let rowids = if to_read == 0 {
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
                return Ok((rowids, Vec::new()));
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
            let rowids: Vec<u32> = (start_ord..start_ord + rows_read as u32).collect();
            if rowids.is_empty() {
                self.rowid_tracker.reset();
            } else {
                self.rowid_tracker
                    .set(rowids.capacity() * std::mem::size_of::<u32>());
            }
            return Ok((rowids, results));
        }
    }

    fn next_batch_late_materialize(
        &mut self,
        batch_size: usize,
    ) -> Result<(Vec<u32>, Vec<(ColumnId, ColumnBatch)>)> {
        loop {
            if !self.has_next() {
                self.rowid_tracker.reset();
                return Ok((Vec::new(), Vec::new()));
            }

            if matches!(self.evaluated_selection, PredicateResult::NoneMatch) {
                self.rowid_tracker.reset();
                return Ok((Vec::new(), Vec::new()));
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
                    self.current_ordinal = self.num_rows;
                    self.rowid_tracker.reset();
                    return Ok((Vec::new(), Vec::new()));
                }
            }

            let mut max_rowid = self.num_rows;
            if let PredicateResult::PageRanges(ranges) = &self.evaluated_selection {
                for range in ranges {
                    if self.current_ordinal < range.end_row as u64 {
                        max_rowid = range.end_row as u64;
                        break;
                    }
                }
            }

            let mut rowids = Vec::with_capacity(batch_size);
            while rowids.len() < batch_size && self.current_ordinal < max_rowid {
                let remaining = (max_rowid - self.current_ordinal) as usize;
                let to_read = batch_size.min(remaining);
                if to_read == 0 {
                    break;
                }

                let (rows_read, values_by_col) = self
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

                for row_idx in 0..rows_read {
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

                    if self
                        .predicate_evaluator
                        .as_ref()
                        .expect("late materialization requires predicate evaluator")
                        .evaluate_row(&values_by_col, row_idx)?
                    {
                        rowids.push(ord as u32);
                        if rowids.len() >= batch_size {
                            break;
                        }
                    }
                }

                self.current_ordinal += rows_read as u64;
                if rows_read < to_read {
                    break;
                }
            }

            if rowids.is_empty() {
                if self.current_ordinal >= max_rowid && self.current_ordinal < self.num_rows {
                    self.rowid_tracker.reset();
                    continue;
                }
                self.rowid_tracker.reset();
                return Ok((Vec::new(), Vec::new()));
            }

            self.rowid_tracker
                .set(rowids.capacity() * std::mem::size_of::<u32>());

            if self.column_iterators.is_empty() {
                return Ok((rowids, Vec::new()));
            }

            let rowids_u64: Vec<u64> = rowids.iter().map(|&id| id as u64).collect();
            let mut results = Vec::with_capacity(self.column_iterators.len());
            for (col_id, iter) in &mut self.column_iterators {
                let batch = iter.read_by_rowids(&rowids_u64)?;
                results.push((*col_id, batch));
            }

            return Ok((rowids, results));
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
            .field("num_columns", &self.column_iterators.len())
            .field("late_materialize", &self.predicate_evaluator.is_some())
            .field("prefetcher", &self.prefetcher.is_some())
            .field("options", &self.options)
            .finish()
    }
}
