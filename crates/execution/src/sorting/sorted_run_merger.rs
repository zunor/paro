// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::{Arc, Mutex};

use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::sort_key::compare_keys;
use paro_common::vector::VECTOR_SIZE;

use crate::result_type::SourceResultType;

use super::sort_descriptor::Sort;
use super::sort_key_store::KeyCursor;
use super::sorted_run::{RunRowCursor, SortedRun};

const MERGE_KEY_SCRATCH_RETAIN_BYTES: usize = 64 * 1024;

#[derive(Debug, Clone)]
pub struct KWayMergeState {
    current_positions: Vec<u32>,
    end_positions: Vec<u32>,
}

#[derive(Debug, Clone, Copy)]
struct RunConsumption {
    run_idx: usize,
    sorted_start: u32,
    len: u32,
    output_range_start: usize,
    output_range_len: usize,
}

#[derive(Debug, Default)]
struct MergeOutputBatch {
    output_positions: Vec<u32>,
    consumptions: Vec<RunConsumption>,
}

#[derive(Debug, Eq, PartialEq)]
struct MergeHeapEntry {
    key: Vec<u8>,
    run_idx: usize,
    sorted_position: u32,
}

impl Ord for MergeHeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        compare_keys(&other.key, &self.key)
            .then_with(|| other.run_idx.cmp(&self.run_idx))
            .then_with(|| other.sorted_position.cmp(&self.sorted_position))
    }
}

impl PartialOrd for MergeHeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl MergeOutputBatch {
    fn clear(&mut self) {
        self.output_positions.clear();
        self.consumptions.clear();
    }

    fn len(&self) -> usize {
        self.output_positions.len()
    }

    fn push(&mut self, run_idx: usize, sorted_position: u32, output_position: u32) {
        let range_start = self.output_positions.len();
        self.output_positions.push(output_position);

        if let Some(last) = self.consumptions.last_mut() {
            if last.run_idx == run_idx
                && last.sorted_start + last.len == sorted_position
                && last.output_range_start + last.output_range_len == range_start
            {
                last.len += 1;
                last.output_range_len += 1;
                return;
            }
        }

        self.consumptions.push(RunConsumption {
            run_idx,
            sorted_start: sorted_position,
            len: 1,
            output_range_start: range_start,
            output_range_len: 1,
        });
    }
}

#[derive(Debug)]
pub struct SortedRunMerger {
    pub(crate) sort: Arc<Sort>,
    pub(crate) sorted_runs: Vec<SortedRun>,
    total_count: usize,
}

impl SortedRunMerger {
    pub fn new(sort: Arc<Sort>, sorted_runs: Vec<SortedRun>) -> Self {
        let total_count = sorted_runs.iter().map(SortedRun::count).sum();
        Self {
            sort,
            sorted_runs,
            total_count,
        }
    }

    #[inline]
    pub fn total_count(&self) -> usize {
        self.total_count
    }

    #[inline]
    pub fn run_count(&self) -> usize {
        self.sorted_runs.len()
    }

    pub fn get_data(
        &self,
        chunk: &mut Chunk,
        gstate: &SortedRunMergerGlobalState,
        lstate: &mut SortedRunMergerLocalState,
    ) -> Result<SourceResultType> {
        if self.sorted_runs.is_empty() || self.total_count == 0 {
            chunk.set_cardinality(0);
            return Ok(SourceResultType::Finished);
        }

        if lstate.merge_state.is_none() || lstate.batch_exhausted() {
            if !self.assign_partition(gstate, lstate)? {
                chunk.set_cardinality(0);
                return Ok(SourceResultType::Finished);
            }
        }

        let mut key_cursors = self
            .sorted_runs
            .iter()
            .map(|run| run.key_store().cursor_on_demand(1))
            .collect::<Vec<_>>();

        self.collect_output_batch(lstate, &mut key_cursors)?;
        if lstate.output_batch.len() == 0 {
            self.finish_partition(gstate, lstate)?;
            if gstate.all_partitions_scanned() {
                chunk.set_cardinality(0);
                return Ok(SourceResultType::Finished);
            }
            return self.get_data(chunk, gstate, lstate);
        }

        chunk.try_reset(chunk.allocator().clone())?;
        let mut key_row_cursors = self
            .sorted_runs
            .iter()
            .map(SortedRun::external_key_cursor)
            .collect::<Vec<Option<RunRowCursor<'_>>>>();
        let mut payload_row_cursors = self
            .sorted_runs
            .iter()
            .map(SortedRun::external_payload_cursor)
            .collect::<Vec<Option<RunRowCursor<'_>>>>();

        for consumption in &lstate.output_batch.consumptions {
            let positions = &lstate.output_batch.output_positions[consumption.output_range_start
                ..consumption.output_range_start + consumption.output_range_len];
            let run = &self.sorted_runs[consumption.run_idx];
            run.gather_sorted_range_projected(
                consumption.sorted_start,
                consumption.len,
                chunk,
                positions,
                self.sort.output_projection_columns(),
                key_row_cursors[consumption.run_idx].as_mut(),
                payload_row_cursors[consumption.run_idx].as_mut(),
            )?;
        }
        chunk.set_cardinality(lstate.output_batch.len());

        let mut result = SourceResultType::HaveMoreOutput;
        if lstate.batch_exhausted() {
            self.finish_partition(gstate, lstate)?;
            if gstate.all_partitions_scanned() {
                result = SourceResultType::Finished;
            }
        }
        Ok(result)
    }

    fn assign_partition(
        &self,
        gstate: &SortedRunMergerGlobalState,
        lstate: &mut SortedRunMergerLocalState,
    ) -> Result<bool> {
        let Some((partition_idx, batch_start, batch_end)) = gstate.assign_batch() else {
            return Ok(false);
        };

        let mut cursors = self
            .sorted_runs
            .iter()
            .map(|run| run.key_store().cursor_on_demand(1))
            .collect::<Vec<_>>();
        let start_positions = self.compute_partition_boundaries(
            batch_start as u32,
            lstate.prev_run_positions.as_deref(),
            &mut cursors,
        )?;
        let end_positions = self.compute_partition_boundaries(
            batch_end as u32,
            Some(&start_positions),
            &mut cursors,
        )?;

        lstate.batch_range = Some((batch_start, batch_end));
        lstate.current_position = batch_start;
        lstate.partition_idx = Some(partition_idx);
        lstate.prev_run_positions = Some(start_positions.clone());
        lstate.merge_state = Some(KWayMergeState {
            current_positions: start_positions,
            end_positions,
        });
        lstate.clear_merge_heap();
        Ok(true)
    }

    fn finish_partition(
        &self,
        gstate: &SortedRunMergerGlobalState,
        lstate: &mut SortedRunMergerLocalState,
    ) -> Result<()> {
        let Some(partition_idx) = lstate.partition_idx.take() else {
            return Ok(());
        };
        let Some(merge_state) = lstate.merge_state.take() else {
            return Ok(());
        };
        lstate.batch_range = None;
        gstate.mark_partition_scanned(self, partition_idx, &merge_state.end_positions)
    }

    fn collect_output_batch(
        &self,
        lstate: &mut SortedRunMergerLocalState,
        key_cursors: &mut [KeyCursor<'_>],
    ) -> Result<()> {
        lstate.output_batch.clear();
        if lstate.merge_state.is_none() {
            return Ok(());
        }

        let remaining = {
            let merge_state = lstate
                .merge_state
                .as_ref()
                .expect("merge state checked above");
            merge_state
                .end_positions
                .iter()
                .zip(merge_state.current_positions.iter())
                .map(|(end, current)| end.saturating_sub(*current) as usize)
                .sum::<usize>()
        };
        let batch_count = remaining.min(VECTOR_SIZE);
        if lstate.merge_heap.is_empty() && remaining > 0 {
            for run_idx in 0..self.sorted_runs.len() {
                let sorted_position = {
                    let merge_state = lstate
                        .merge_state
                        .as_ref()
                        .expect("merge state checked above");
                    (merge_state.current_positions[run_idx] < merge_state.end_positions[run_idx])
                        .then_some(merge_state.current_positions[run_idx])
                };
                let Some(sorted_position) = sorted_position else {
                    continue;
                };
                let mut entry = lstate.take_merge_heap_entry(run_idx, sorted_position);
                self.read_merge_heap_entry_key(&mut entry, key_cursors)?;
                lstate.merge_heap.push(entry);
            }
        }

        for output_idx in 0..batch_count {
            let Some(mut entry) = lstate.merge_heap.pop() else {
                break;
            };
            lstate
                .output_batch
                .push(entry.run_idx, entry.sorted_position, output_idx as u32);
            lstate.current_position += 1;
            let next_position = entry.sorted_position + 1;
            let end_position = {
                let merge_state = lstate
                    .merge_state
                    .as_mut()
                    .expect("merge state checked above");
                merge_state.current_positions[entry.run_idx] = next_position;
                merge_state.end_positions[entry.run_idx]
            };
            if next_position < end_position {
                entry.sorted_position = next_position;
                self.read_merge_heap_entry_key(&mut entry, key_cursors)?;
                lstate.merge_heap.push(entry);
            } else {
                lstate.recycle_merge_heap_entry(entry);
            }
        }

        Ok(())
    }

    fn read_merge_heap_entry_key(
        &self,
        entry: &mut MergeHeapEntry,
        key_cursors: &mut [KeyCursor<'_>],
    ) -> Result<()> {
        let ordinal = self.sorted_runs[entry.run_idx]
            .source_ordinal_at_sorted_position(entry.sorted_position)?;
        key_cursors[entry.run_idx].read_key_into(ordinal, &mut entry.key)?;
        Ok(())
    }

    fn compute_partition_boundaries(
        &self,
        batch_start: u32,
        prev_boundaries: Option<&[u32]>,
        key_cursors: &mut [KeyCursor<'_>],
    ) -> Result<Vec<u32>> {
        let num_runs = self.sorted_runs.len();
        let mut run_positions = prev_boundaries
            .map(|positions| positions.to_vec())
            .unwrap_or_else(|| vec![0; num_runs]);
        let mut total_remaining = batch_start;
        for position in &run_positions {
            total_remaining = total_remaining.saturating_sub(*position);
        }

        let mut active_runs: Vec<usize> = (0..num_runs)
            .filter(|&run_idx| run_positions[run_idx] < self.sorted_runs[run_idx].count() as u32)
            .collect();

        while total_remaining > 0 && !active_runs.is_empty() {
            let base_delta = total_remaining.div_ceil(active_runs.len() as u32);
            let mut best_run_idx = active_runs[0];
            let mut best_delta = base_delta
                .min(self.sorted_runs[best_run_idx].count() as u32 - run_positions[best_run_idx]);
            let mut best_position = run_positions[best_run_idx] + best_delta - 1;

            for &run_idx in &active_runs[1..] {
                let remaining = self.sorted_runs[run_idx].count() as u32 - run_positions[run_idx];
                let delta = base_delta.min(remaining);
                let position = run_positions[run_idx] + delta - 1;
                if self.compare_positions(
                    run_idx,
                    position,
                    best_run_idx,
                    best_position,
                    key_cursors,
                )? == Ordering::Less
                {
                    best_run_idx = run_idx;
                    best_delta = delta;
                    best_position = position;
                }
            }

            run_positions[best_run_idx] += best_delta;
            total_remaining -= best_delta;
            active_runs.retain(|&run_idx| {
                run_positions[run_idx] < self.sorted_runs[run_idx].count() as u32
            });
        }

        Ok(run_positions)
    }

    fn compare_positions(
        &self,
        left_run_idx: usize,
        left_position: u32,
        right_run_idx: usize,
        right_position: u32,
        key_cursors: &mut [KeyCursor<'_>],
    ) -> Result<Ordering> {
        let left_ordinal =
            self.sorted_runs[left_run_idx].source_ordinal_at_sorted_position(left_position)?;
        let right_ordinal =
            self.sorted_runs[right_run_idx].source_ordinal_at_sorted_position(right_position)?;
        if left_run_idx == right_run_idx {
            return key_cursors[left_run_idx].compare(left_ordinal, right_ordinal);
        }

        let (left_cursor, right_cursor) = split_two_mut(key_cursors, left_run_idx, right_run_idx);
        KeyCursor::compare_with(left_cursor, left_ordinal, right_cursor, right_ordinal)
    }
}

fn split_two_mut<T>(values: &mut [T], left: usize, right: usize) -> (&mut T, &mut T) {
    debug_assert_ne!(left, right);
    if left < right {
        let (head, tail) = values.split_at_mut(right);
        (&mut head[left], &mut tail[0])
    } else {
        let (head, tail) = values.split_at_mut(left);
        (&mut tail[0], &mut head[right])
    }
}

#[derive(Debug)]
pub struct SortedRunMergerLocalState {
    pub batch_range: Option<(usize, usize)>,
    pub current_position: usize,
    pub merge_state: Option<KWayMergeState>,
    pub prev_run_positions: Option<Vec<u32>>,
    pub partition_idx: Option<usize>,
    output_batch: MergeOutputBatch,
    merge_heap: BinaryHeap<MergeHeapEntry>,
    merge_entry_pool: Vec<MergeHeapEntry>,
}

impl SortedRunMergerLocalState {
    pub fn new() -> Self {
        Self {
            batch_range: None,
            current_position: 0,
            merge_state: None,
            prev_run_positions: None,
            partition_idx: None,
            output_batch: MergeOutputBatch::default(),
            merge_heap: BinaryHeap::new(),
            merge_entry_pool: Vec::new(),
        }
    }

    pub fn batch_exhausted(&self) -> bool {
        if let Some((_, batch_end)) = self.batch_range {
            self.current_position >= batch_end
        } else {
            true
        }
    }

    fn clear_merge_heap(&mut self) {
        while let Some(entry) = self.merge_heap.pop() {
            self.recycle_merge_heap_entry(entry);
        }
    }

    fn take_merge_heap_entry(&mut self, run_idx: usize, sorted_position: u32) -> MergeHeapEntry {
        if let Some(mut entry) = self.merge_entry_pool.pop() {
            entry.run_idx = run_idx;
            entry.sorted_position = sorted_position;
            entry.key.clear();
            entry
        } else {
            MergeHeapEntry {
                key: Vec::new(),
                run_idx,
                sorted_position,
            }
        }
    }

    fn recycle_merge_heap_entry(&mut self, mut entry: MergeHeapEntry) {
        if entry.key.capacity() > MERGE_KEY_SCRATCH_RETAIN_BYTES {
            entry.key = Vec::new();
        } else {
            entry.key.clear();
        }
        self.merge_entry_pool.push(entry);
    }

    #[cfg(test)]
    pub(crate) fn scratch_capacities(&self) -> (usize, usize, usize, usize) {
        let key_scratch_bytes = self
            .merge_heap
            .iter()
            .chain(self.merge_entry_pool.iter())
            .map(|entry| entry.key.capacity())
            .sum();
        (
            self.output_batch.output_positions.capacity(),
            self.output_batch.consumptions.capacity(),
            self.merge_heap.capacity(),
            key_scratch_bytes,
        )
    }
}

#[derive(Debug)]
pub struct SortedRunMergerGlobalState {
    pub(crate) total_count: usize,
    pub(crate) partition_size: usize,
    pub(crate) next_batch_index: Mutex<usize>,
    external: bool,
    num_threads: usize,
    scanned_partitions: Mutex<Vec<Option<Vec<u32>>>>,
    next_release_partition: Mutex<usize>,
}

impl SortedRunMergerGlobalState {
    pub fn new(
        total_count: usize,
        partition_size: usize,
        external: bool,
        num_threads: usize,
    ) -> Self {
        let num_partitions = if total_count == 0 {
            0
        } else {
            total_count.div_ceil(partition_size.max(1))
        };
        Self {
            total_count,
            partition_size: partition_size.max(1),
            next_batch_index: Mutex::new(0),
            external,
            num_threads: num_threads.max(1),
            scanned_partitions: Mutex::new(vec![None; num_partitions]),
            next_release_partition: Mutex::new(0),
        }
    }

    pub fn assign_batch(&self) -> Option<(usize, usize, usize)> {
        let mut next_batch = self.next_batch_index.lock().unwrap();
        let partition_idx = *next_batch;
        let batch_start = partition_idx * self.partition_size;
        if batch_start >= self.total_count {
            return None;
        }
        let batch_end = (batch_start + self.partition_size).min(self.total_count);
        *next_batch += 1;
        Some((partition_idx, batch_start, batch_end))
    }

    pub fn mark_partition_scanned(
        &self,
        merger: &SortedRunMerger,
        partition_idx: usize,
        run_end_positions: &[u32],
    ) -> Result<()> {
        if let Some(entry) = self
            .scanned_partitions
            .lock()
            .unwrap()
            .get_mut(partition_idx)
        {
            *entry = Some(run_end_positions.to_vec());
        }
        self.release_scanned_prefix(merger)
    }

    pub fn all_partitions_scanned(&self) -> bool {
        self.scanned_partitions
            .lock()
            .unwrap()
            .iter()
            .all(Option::is_some)
    }

    pub fn max_threads(&self) -> usize {
        if self.total_count == 0 {
            1
        } else {
            self.num_threads
                .min(self.total_count.div_ceil(self.partition_size))
                .max(1)
        }
    }

    fn release_scanned_prefix(&self, merger: &SortedRunMerger) -> Result<()> {
        if !self.external {
            return Ok(());
        }

        let scanned = self.scanned_partitions.lock().unwrap();
        let mut next_release_partition = self.next_release_partition.lock().unwrap();
        let mut contiguous = *next_release_partition;
        while contiguous < scanned.len() && scanned[contiguous].is_some() {
            contiguous += 1;
        }

        if contiguous <= self.num_threads {
            return Ok(());
        }
        let release_upto = contiguous - self.num_threads;
        if release_upto <= *next_release_partition {
            return Ok(());
        }

        let mut frontiers = vec![0u32; merger.run_count()];
        for partition_idx in *next_release_partition..release_upto {
            let run_end_positions = scanned[partition_idx]
                .as_ref()
                .ok_or_else(|| paro_error::internal("missing scanned partition frontier"))?;
            for (run_idx, &frontier) in run_end_positions.iter().enumerate() {
                frontiers[run_idx] = frontier;
            }
        }
        drop(scanned);

        for (run_idx, frontier) in frontiers.into_iter().enumerate() {
            merger.sorted_runs[run_idx].advance_release_frontier(frontier)?;
        }
        *next_release_partition = release_upto;
        Ok(())
    }
}
