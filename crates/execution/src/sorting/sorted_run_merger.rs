// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::cmp::Ordering;
use std::sync::{Arc, Mutex};

use paro_common::allocator::Allocator;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_common::vector::VECTOR_SIZE;

use crate::result_type::SourceResultType;

use super::sort_descriptor::Sort;
use super::sort_key_store::KeyCursor;
use super::sorted_run::{RunRowCursor, SortedRun};

const NO_MERGE_RUN: usize = usize::MAX;

#[derive(Debug, Clone)]
pub struct KWayMergeState {
    current_positions: Vec<u32>,
    end_positions: Vec<u32>,
}

#[derive(Debug, Default)]
struct RunOutputBatch {
    sorted_start: u32,
    output_positions: Vec<u32>,
}

#[derive(Debug, Default)]
struct MergeOutputBatch {
    runs: Vec<RunOutputBatch>,
    len: usize,
}

/// Reusable winner tree for k-way merge.
///
/// Leaves contain run indices and internal nodes contain the run whose current
/// row sorts first. Unlike `BinaryHeap<Vec<u8>>`, the tree never copies a sort
/// key: comparisons read directly from each run's key store.
#[derive(Debug, Default)]
struct MergeTournament {
    leaf_count: usize,
    winners: Vec<usize>,
}

impl MergeTournament {
    fn reset(&mut self) {
        self.leaf_count = 0;
        self.winners.clear();
    }

    fn is_initialized(&self) -> bool {
        self.leaf_count != 0
    }

    fn initialize(
        &mut self,
        run_count: usize,
        mut is_active: impl FnMut(usize) -> bool,
        mut compare: impl FnMut(usize, usize) -> Result<Ordering>,
    ) -> Result<()> {
        self.leaf_count = run_count.max(1).next_power_of_two();
        self.winners
            .resize(self.leaf_count.saturating_mul(2), NO_MERGE_RUN);
        self.winners.fill(NO_MERGE_RUN);
        for run_idx in 0..run_count {
            if is_active(run_idx) {
                self.winners[self.leaf_count + run_idx] = run_idx;
            }
        }
        for node_idx in (1..self.leaf_count).rev() {
            self.winners[node_idx] = choose_merge_winner(
                self.winners[node_idx * 2],
                self.winners[node_idx * 2 + 1],
                &mut compare,
            )?;
        }
        Ok(())
    }

    fn winner(&self) -> Option<usize> {
        self.winners
            .get(1)
            .copied()
            .filter(|run_idx| *run_idx != NO_MERGE_RUN)
    }

    fn update(
        &mut self,
        run_idx: usize,
        active: bool,
        mut compare: impl FnMut(usize, usize) -> Result<Ordering>,
    ) -> Result<()> {
        if self.leaf_count == 0 || run_idx >= self.leaf_count {
            return Err(paro_error::internal(format!(
                "merge tournament update is out of bounds: run={run_idx}, leaves={}",
                self.leaf_count
            )));
        }
        let leaf_idx = self.leaf_count + run_idx;
        self.winners[leaf_idx] = if active { run_idx } else { NO_MERGE_RUN };
        let mut node_idx = leaf_idx / 2;
        while node_idx > 0 {
            self.winners[node_idx] = choose_merge_winner(
                self.winners[node_idx * 2],
                self.winners[node_idx * 2 + 1],
                &mut compare,
            )?;
            node_idx /= 2;
        }
        Ok(())
    }
}

fn choose_merge_winner(
    left: usize,
    right: usize,
    compare: &mut impl FnMut(usize, usize) -> Result<Ordering>,
) -> Result<usize> {
    match (left, right) {
        (NO_MERGE_RUN, NO_MERGE_RUN) => Ok(NO_MERGE_RUN),
        (NO_MERGE_RUN, right) => Ok(right),
        (left, NO_MERGE_RUN) => Ok(left),
        (left, right) => Ok(if compare(left, right)? == Ordering::Greater {
            right
        } else {
            left
        }),
    }
}

impl MergeOutputBatch {
    fn clear(&mut self, run_count: usize) {
        self.runs.resize_with(run_count, RunOutputBatch::default);
        for run in &mut self.runs {
            run.output_positions.clear();
        }
        self.len = 0;
    }

    fn len(&self) -> usize {
        self.len
    }

    fn push(&mut self, run_idx: usize, sorted_position: u32, output_position: u32) {
        let run = self
            .runs
            .get_mut(run_idx)
            .expect("merge output run index must be valid");
        if run.output_positions.is_empty() {
            run.sorted_start = sorted_position;
        } else {
            debug_assert_eq!(
                run.sorted_start + run.output_positions.len() as u32,
                sorted_position,
                "a merge consumes each input run in sorted order"
            );
        }
        run.output_positions.push(output_position);
        self.len += 1;
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

    pub(crate) fn materialize_range(
        &self,
        start: usize,
        end: usize,
        output_types: &[LogicalType],
        allocator: Arc<dyn Allocator>,
    ) -> Result<Vec<Chunk>> {
        if start >= end || end > self.total_count {
            return Err(paro_error::internal(format!(
                "invalid sorted merge materialization range: start={start}, end={end}, total={}",
                self.total_count
            )));
        }
        let mut key_cursors = self
            .sorted_runs
            .iter()
            .map(|run| run.key_store().cursor_on_demand(1))
            .collect::<Vec<_>>();
        let start_positions = self.compute_partition_boundaries(start, None, &mut key_cursors)?;
        let end_positions =
            self.compute_partition_boundaries(end, Some(&start_positions), &mut key_cursors)?;
        let mut local = SortedRunMergerLocalState::new();
        local.batch_range = Some((start, end));
        local.current_position = start;
        local.merge_state = Some(KWayMergeState {
            current_positions: start_positions,
            end_positions,
        });

        let mut chunks = Vec::with_capacity((end - start).div_ceil(VECTOR_SIZE));
        let mut materialized_rows = 0usize;
        while !local.batch_exhausted() {
            self.collect_output_batch(&mut local, &mut key_cursors)?;
            let batch_len = local.output_batch.len();
            if batch_len == 0 {
                return Err(paro_error::internal(format!(
                    "sorted merge materialization stopped before its range ended: current={}, end={end}",
                    local.current_position
                )));
            }
            let mut chunk =
                Chunk::try_initialize(output_types, VECTOR_SIZE, Arc::clone(&allocator))?;
            self.gather_output_batch(&mut chunk, &local, &mut key_cursors)?;
            materialized_rows = materialized_rows.checked_add(chunk.size()).ok_or_else(|| {
                paro_error::internal("sorted merge materialized row count overflow")
            })?;
            chunks.push(chunk);
        }
        if materialized_rows != end - start {
            return Err(paro_error::internal(format!(
                "sorted merge materialization length mismatch: expected={}, actual={materialized_rows}",
                end - start,
            )));
        }
        Ok(chunks)
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

        self.gather_output_batch(chunk, lstate, &mut key_cursors)?;

        let mut result = SourceResultType::HaveMoreOutput;
        if lstate.batch_exhausted() {
            self.finish_partition(gstate, lstate)?;
            if gstate.all_partitions_scanned() {
                result = SourceResultType::Finished;
            }
        }
        Ok(result)
    }

    fn gather_output_batch(
        &self,
        chunk: &mut Chunk,
        lstate: &SortedRunMergerLocalState,
        sort_key_cursors: &mut [KeyCursor<'_>],
    ) -> Result<()> {
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

        for (run_idx, output_batch) in lstate.output_batch.runs.iter().enumerate() {
            if output_batch.output_positions.is_empty() {
                continue;
            }
            let run = &self.sorted_runs[run_idx];
            run.gather_sorted_range_projected(
                output_batch.sorted_start,
                output_batch.output_positions.len() as u32,
                chunk,
                &output_batch.output_positions,
                self.sort.output_projection_columns(),
                &mut sort_key_cursors[run_idx],
                key_row_cursors[run_idx].as_mut(),
                payload_row_cursors[run_idx].as_mut(),
            )?;
        }
        chunk.set_cardinality(lstate.output_batch.len());
        Ok(())
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
            batch_start,
            lstate.prev_run_positions.as_deref(),
            &mut cursors,
        )?;
        let end_positions =
            self.compute_partition_boundaries(batch_end, Some(&start_positions), &mut cursors)?;

        lstate.batch_range = Some((batch_start, batch_end));
        lstate.current_position = batch_start;
        lstate.partition_idx = Some(partition_idx);
        lstate.prev_run_positions = Some(start_positions.clone());
        lstate.merge_state = Some(KWayMergeState {
            current_positions: start_positions,
            end_positions,
        });
        lstate.merge_tournament.reset();
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
        lstate.output_batch.clear(self.sorted_runs.len());
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
        if !lstate.merge_tournament.is_initialized() && remaining > 0 {
            let merge_state = lstate
                .merge_state
                .as_ref()
                .expect("merge state checked above");
            let positions = &merge_state.current_positions;
            let ends = &merge_state.end_positions;
            lstate.merge_tournament.initialize(
                self.sorted_runs.len(),
                |run_idx| positions[run_idx] < ends[run_idx],
                |left, right| self.compare_run_heads(left, right, positions, key_cursors),
            )?;
        }

        for output_idx in 0..batch_count {
            let Some(run_idx) = lstate.merge_tournament.winner() else {
                break;
            };
            let sorted_position = lstate
                .merge_state
                .as_ref()
                .expect("merge state checked above")
                .current_positions[run_idx];
            lstate
                .output_batch
                .push(run_idx, sorted_position, output_idx as u32);
            lstate.current_position += 1;
            let active = {
                let merge_state = lstate
                    .merge_state
                    .as_mut()
                    .expect("merge state checked above");
                merge_state.current_positions[run_idx] = sorted_position + 1;
                merge_state.current_positions[run_idx] < merge_state.end_positions[run_idx]
            };
            let positions = &lstate
                .merge_state
                .as_ref()
                .expect("merge state checked above")
                .current_positions;
            lstate
                .merge_tournament
                .update(run_idx, active, |left, right| {
                    self.compare_run_heads(left, right, positions, key_cursors)
                })?;
        }

        Ok(())
    }

    fn compare_run_heads(
        &self,
        left_run_idx: usize,
        right_run_idx: usize,
        positions: &[u32],
        key_cursors: &mut [KeyCursor<'_>],
    ) -> Result<Ordering> {
        self.compare_positions(
            left_run_idx,
            positions[left_run_idx],
            right_run_idx,
            positions[right_run_idx],
            key_cursors,
        )
        .map(|ordering| ordering.then_with(|| left_run_idx.cmp(&right_run_idx)))
    }

    fn compute_partition_boundaries(
        &self,
        batch_start: usize,
        prev_boundaries: Option<&[u32]>,
        key_cursors: &mut [KeyCursor<'_>],
    ) -> Result<Vec<u32>> {
        let num_runs = self.sorted_runs.len();
        let mut run_positions = prev_boundaries
            .map(|positions| positions.to_vec())
            .unwrap_or_else(|| vec![0; num_runs]);
        let mut total_remaining = batch_start;
        for position in &run_positions {
            total_remaining = total_remaining.saturating_sub(*position as usize);
        }

        let mut active_runs: Vec<usize> = (0..num_runs)
            .filter(|&run_idx| run_positions[run_idx] < self.sorted_runs[run_idx].count() as u32)
            .collect();

        while total_remaining > 0 && !active_runs.is_empty() {
            let base_delta = total_remaining.div_ceil(active_runs.len());
            let mut best_run_idx = active_runs[0];
            let mut best_delta = base_delta
                .min(self.sorted_runs[best_run_idx].count() - run_positions[best_run_idx] as usize);
            let mut best_position =
                checked_sorted_position(run_positions[best_run_idx], best_delta)?;

            for &run_idx in &active_runs[1..] {
                let remaining = self.sorted_runs[run_idx].count() - run_positions[run_idx] as usize;
                let delta = base_delta.min(remaining);
                let position = checked_sorted_position(run_positions[run_idx], delta)?;
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

            run_positions[best_run_idx] = run_positions[best_run_idx]
                .checked_add(u32::try_from(best_delta).map_err(|_| {
                    paro_error::internal(format!(
                        "sorted merge boundary delta exceeds u32: {best_delta}"
                    ))
                })?)
                .ok_or_else(|| paro_error::internal("sorted merge boundary overflow"))?;
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

fn checked_sorted_position(start: u32, delta: usize) -> Result<u32> {
    let delta = u32::try_from(delta).map_err(|_| {
        paro_error::internal(format!("sorted merge boundary delta exceeds u32: {delta}"))
    })?;
    start
        .checked_add(delta)
        .and_then(|end| end.checked_sub(1))
        .ok_or_else(|| paro_error::internal("sorted merge boundary position overflow"))
}

#[derive(Debug)]
pub struct SortedRunMergerLocalState {
    pub batch_range: Option<(usize, usize)>,
    pub current_position: usize,
    pub merge_state: Option<KWayMergeState>,
    pub prev_run_positions: Option<Vec<u32>>,
    pub partition_idx: Option<usize>,
    output_batch: MergeOutputBatch,
    merge_tournament: MergeTournament,
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
            merge_tournament: MergeTournament::default(),
        }
    }

    pub fn batch_exhausted(&self) -> bool {
        if let Some((_, batch_end)) = self.batch_range {
            self.current_position >= batch_end
        } else {
            true
        }
    }

    #[cfg(test)]
    pub(crate) fn scratch_capacities(&self) -> (usize, usize, usize) {
        let output_position_capacity = self
            .output_batch
            .runs
            .iter()
            .map(|run| run.output_positions.capacity())
            .sum();
        (
            output_position_capacity,
            self.output_batch.runs.capacity(),
            self.merge_tournament.winners.capacity(),
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

#[cfg(test)]
mod merge_output_tests {
    use super::MergeOutputBatch;

    #[test]
    fn interleaved_merge_rows_are_coalesced_per_input_run() {
        let mut batch = MergeOutputBatch::default();
        batch.clear(2);
        batch.push(0, 10, 0);
        batch.push(1, 20, 1);
        batch.push(0, 11, 2);
        batch.push(1, 21, 3);
        batch.push(0, 12, 4);

        assert_eq!(batch.len(), 5);
        assert_eq!(batch.runs[0].sorted_start, 10);
        assert_eq!(batch.runs[0].output_positions, vec![0, 2, 4]);
        assert_eq!(batch.runs[1].sorted_start, 20);
        assert_eq!(batch.runs[1].output_positions, vec![1, 3]);
    }
}
