// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use paro_common::chunk::Chunk;
use paro_common::types::LogicalType;
use paro_common::vector::Vector;

use crate::expression_executor::executor::ExpressionExecutor;
use crate::join_hashtable::join_hashtable::{FullOuterScanState, JoinHashTable};
use crate::join_hashtable::scan_structure::ScanStructure;

#[derive(Debug, Default)]
pub struct HashJoinProbeTransformLocal {
    pub scan_structure: Option<ScanStructure>,
    pub probe_keys: Option<Chunk>,
    pub probe_key_types: Box<[LogicalType]>,
    pub probe_key_executors: Box<[ExpressionExecutor]>,
    pub probe_hashes: Option<Vector>,
    pub probe_spill_chunk: Option<Chunk>,
    pub probe_in_progress: bool,
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
pub struct HashJoinSpillReplaySourceLocal {
    pub probe_key_types: Box<[LogicalType]>,
    pub probe_key_executors: Box<[ExpressionExecutor]>,
    pub current: Option<HashJoinSpillReplayPartitionLocal>,
}

#[derive(Debug)]
pub struct HashJoinSpillReplayPartitionLocal {
    pub partition_idx: usize,
    pub hash_table: Arc<JoinHashTable>,
    pub probe_rows: paro_storage::row::RowStore,
    pub probe_scan_state: paro_storage::row::RowScanState,
    pub probe_spill_chunk: Chunk,
    pub probe_input: Option<Chunk>,
    pub probe_keys: Option<Chunk>,
    pub scan_structure: ScanStructure,
    pub probe_in_progress: bool,
}

#[derive(Debug, Default)]
pub struct HashJoinUnmatchedSourceLocal {
    pub scan_state: Option<FullOuterScanState>,
}

#[derive(Debug, Default)]
pub struct HashJoinBuildSinkLocal {
    pub hash_table: Option<Arc<JoinHashTable>>,
    pub build_keys: Option<Chunk>,
    pub build_payload: Option<Chunk>,
    pub build_key_types: Box<[LogicalType]>,
    pub build_key_executors: Box<[ExpressionExecutor]>,
}
