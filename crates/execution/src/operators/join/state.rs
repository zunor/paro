// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use parking_lot::Mutex;
use paro_common::chunk::Chunk;
use paro_common::types::LogicalType;
use paro_common::vector::{SelectionVector, Vector};

use crate::expression_executor::executor::ExpressionExecutor;
use crate::join_hashtable::scan_structure::ScanStructure;
use crate::join_hashtable::{FullOuterScanState, GroupedReductionExtrema, JoinHashTable};
use crate::memory_runtime::QueryMemoryPool;
use crate::operators::join::hash::residual::HashJoinResidualProbeState;
use crate::operators::join::hash::source_predicate::ReductionSourcePredicateState;
use crate::runtime::breaker::{
    JoinBuildSpillBuffer, JoinProbeSpillBuffer, JoinRuntimeFilterBuilder,
};

#[derive(Debug, Default)]
pub struct HashJoinProbeTransformLocal {
    pub scan_structure: Option<ScanStructure>,
    pub probe_keys: Option<Chunk>,
    pub probe_key_types: Box<[LogicalType]>,
    pub probe_key_executors: Box<[ExpressionExecutor]>,
    pub residual: Option<HashJoinResidualProbeState>,
    pub reduction_residuals: Box<[Option<HashJoinResidualProbeState>]>,
    pub reduction_source_predicates: Box<[ReductionSourcePredicateState]>,
    pub reduction_source_masks: Vec<u8>,
    pub reduction_channel_map: Option<Arc<[u8; 256]>>,
    pub reduction_selection: Option<SelectionVector>,
    pub(crate) reduction_mode: ReductionProbeMode,
    pub reduction_group_slots: Vec<usize>,
    pub probe_hashes: Option<Vector>,
    pub probe_spill_chunk: Option<Chunk>,
    pub probe_spill_buffer: Option<JoinProbeSpillBuffer>,
    pub probe_in_progress: bool,
}

#[derive(Debug, Default)]
pub(crate) enum ReductionProbeMode {
    #[default]
    Uninitialized,
    MatchMask,
    GroupedExtrema(Arc<GroupedReductionExtrema>),
}

#[derive(Debug, Default)]
pub struct NljUnmatchedSourceLocal {
    pub chunk_idx: usize,
    pub row_idx: usize,
    pub global_row_idx: usize,
}

#[derive(Debug, Default)]
pub struct CrossProductProbeTransformLocal {
    pub probe_row: usize,
    pub build_chunk: usize,
    pub build_row: usize,
    pub probe_in_progress: bool,
}

#[derive(Debug, Default)]
pub struct NestedLoopJoinProbeTransformLocal {
    pub probe_row: usize,
    pub build_chunk: usize,
    pub build_row: usize,
    pub build_global_idx: usize,
    pub probe_in_progress: bool,
    pub found_match: bool,
    pub saw_null: bool,
    pub single_match_found: bool,
    pub left_condition_executors: Vec<ExpressionExecutor>,
    pub right_condition_executors: Vec<ExpressionExecutor>,
    pub arbitrary_condition_executor: Option<ExpressionExecutor>,
    pub left_condition_results: Vec<Arc<Vector>>,
    pub right_condition_cache: Vec<Arc<Vector>>,
    pub right_cache_chunk_idx: Option<usize>,
    pub combined_chunk: Option<Chunk>,
    pub output_row: usize,
}

#[derive(Debug, Default)]
pub struct SortRangeJoinProbeTransformLocal {
    pub probe_row: usize,
    pub candidate_start: usize,
    pub candidate_end: usize,
    pub candidate_pos: usize,
    pub candidate_positions: Vec<usize>,
    pub candidate_source: SortRangeCandidateSource,
    pub probe_offsets: Vec<SortRangeProbeOffsets>,
    pub cached_candidate_ranges: Vec<SortRangeCandidateRange>,
    pub cached_candidate_positions: Vec<usize>,
    pub candidate_cache_probe_order: Vec<usize>,
    pub cached_candidates_ready: bool,
    pub secondary_candidate_bitmap: Vec<u64>,
    pub secondary_candidate_touched_words: Vec<usize>,
    pub primary_candidate_bitmap: Vec<u64>,
    pub primary_candidate_touched_words: Vec<usize>,
    pub candidate_ready: bool,
    pub probe_in_progress: bool,
    pub found_match: bool,
    pub saw_null: bool,
    pub single_match_found: bool,
    pub left_condition_executors: Vec<ExpressionExecutor>,
    pub left_condition_results: Vec<Arc<Vector>>,
    pub output_row: usize,
}

#[derive(Debug, Default)]
pub struct ClassicIeJoinSourceLocal;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum SortRangeCandidateSource {
    #[default]
    PrimaryRange,
    SparsePositions,
    CachedSparsePositions,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct SortRangeProbeOffsets {
    pub primary_start: usize,
    pub primary_end: usize,
    pub secondary_start: usize,
    pub secondary_end: usize,
    pub valid: bool,
    pub saw_null: bool,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct SortRangeCandidateRange {
    pub start: usize,
    pub end: usize,
}

impl SortRangeJoinProbeTransformLocal {
    pub fn reset_for_input(&mut self) {
        self.probe_row = 0;
        self.candidate_start = 0;
        self.candidate_end = 0;
        self.candidate_pos = 0;
        self.candidate_positions.clear();
        self.candidate_source = SortRangeCandidateSource::PrimaryRange;
        self.probe_offsets.clear();
        self.cached_candidate_ranges.clear();
        self.cached_candidate_positions.clear();
        self.candidate_cache_probe_order.clear();
        self.cached_candidates_ready = false;
        self.primary_candidate_bitmap.fill(0);
        self.primary_candidate_touched_words.clear();
        self.secondary_candidate_bitmap.fill(0);
        self.secondary_candidate_touched_words.clear();
        self.candidate_ready = false;
        self.probe_in_progress = true;
        self.found_match = false;
        self.saw_null = false;
        self.single_match_found = false;
        self.output_row = 0;
    }

    pub fn advance_probe_row(&mut self) {
        self.probe_row += 1;
        self.candidate_start = 0;
        self.candidate_end = 0;
        self.candidate_pos = 0;
        self.candidate_positions.clear();
        self.candidate_source = SortRangeCandidateSource::PrimaryRange;
        self.candidate_ready = false;
        self.found_match = false;
        self.saw_null = false;
        self.single_match_found = false;
    }
}

#[derive(Debug, Default)]
pub struct HashJoinSpillReplaySourceLocal {
    pub probe_key_types: Box<[LogicalType]>,
    pub probe_key_executors: Box<[ExpressionExecutor]>,
    pub residual: Option<HashJoinResidualProbeState>,
    pub reduction_residuals: Box<[Option<HashJoinResidualProbeState>]>,
    pub reduction_source_predicates: Box<[ReductionSourcePredicateState]>,
    pub reduction_source_masks: Vec<u8>,
    pub reduction_selection: Option<SelectionVector>,
    pub current: Option<HashJoinSpillReplayPartitionLocal>,
}

#[derive(Debug)]
pub struct HashJoinSpillReplayPartitionLocal {
    pub partition_idx: usize,
    pub hash_table: Arc<JoinHashTable>,
    pub probe_cursor: Option<paro_storage::row::ReclaimingRowScanCursor>,
    pub probe_spill_chunk: Chunk,
    pub probe_input: Option<Chunk>,
    pub probe_keys: Option<Chunk>,
    pub scan_structure: ScanStructure,
    pub probe_in_progress: bool,
    pub probe_exhausted: bool,
    pub unmatched_scan_state: Option<FullOuterScanState>,
}

#[derive(Debug, Default)]
pub struct HashJoinUnmatchedSourceLocal {
    pub scan_state: Option<FullOuterScanState>,
    pub reduction_channel_masks: Box<[u8]>,
}

#[derive(Debug, Default)]
pub struct HashJoinBuildSinkLocal {
    pub hash_table: Option<Arc<JoinHashTable>>,
    pub build_keys: Option<Chunk>,
    pub build_payload: Option<Chunk>,
    pub build_selection: Option<SelectionVector>,
    pub build_hashes: Vec<u64>,
    pub runtime_filter_builder: Option<JoinRuntimeFilterBuilder>,
    pub build_spill: Arc<Mutex<Option<JoinBuildSpillBuffer>>>,
    pub(crate) local_build_spill_reclaimer_name: Option<String>,
    pub(crate) query_memory: Option<Arc<QueryMemoryPool>>,
    pub build_key_types: Box<[LogicalType]>,
    pub build_key_executors: Box<[ExpressionExecutor]>,
    pub build_residual_types: Box<[LogicalType]>,
    pub build_residual_executors: Box<[ExpressionExecutor]>,
    pub build_residuals: Option<Chunk>,
    pub(crate) build_time_integer_builder:
        Option<Arc<crate::join_hashtable::table::BuildTimeIntegerIndexBuilder>>,
}

impl Drop for HashJoinBuildSinkLocal {
    fn drop(&mut self) {
        self.unregister_local_reclaimers();
    }
}

impl HashJoinBuildSinkLocal {
    pub(crate) fn unregister_local_reclaimers(&mut self) {
        if let Some(memory) = self.query_memory.as_ref() {
            if let Some(name) = self.local_build_spill_reclaimer_name.take() {
                memory.unregister_reclaimer_by_name(&name);
            }
        }
    }
}
