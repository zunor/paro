// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Sort-indexed sort-range join probe transform.
//!
//! The build side is materialized by the normal materialize breaker. The first
//! range predicate is converted into an ordered build-side index. For the common
//! two-inequality shape, the second predicate also gets a sorted build-side
//! permutation plus chunk-local probe permutations/offsets. The probe path
//! computes primary/secondary offsets by sweeping sorted probe keys over the
//! sorted build keys, then reuses a bounded incremental bitmap cache when the
//! second predicate range is monotonic. Each probe row intersects the two
//! ordered ranges by scanning the narrower side, so only rows that satisfy both
//! ordered predicates enter the final join-condition evaluation.

use std::cmp::Ordering;
use std::sync::{Arc, OnceLock};

use parking_lot::Mutex;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_common::vector::{Vector, VECTOR_SIZE};
use paro_planner::operator::join::{JoinComparisonType, JoinCondition, JoinType};

use crate::execution_context::ExecutionContext;
use crate::expression_executor::executor::ExpressionExecutor;
use crate::operators::join::nested_loop::runtime::compare_with_nulls;
use crate::operators::output::ensure_transform_output;
use crate::runtime::breaker::{FoundBits, HandleRef, MaterializedHandle, MaterializedReader};
use crate::runtime::context::{OperatorCallContext, OperatorFinishContext, PipelineInitContext};
use crate::runtime::source::SourcePoll;
use crate::runtime::state::{
    ClassicIeJoinSourceLocal, SortRangeCandidateRange, SortRangeCandidateSource,
    SortRangeJoinProbeTransformLocal, SortRangeProbeOffsets, SourceGlobal, SourceLocal,
    TransformGlobal, TransformLocal,
};
use crate::runtime::transform::{TransformFinishPoll, TransformFlushPoll, TransformPoll};

const SORT_RANGE_CANDIDATE_CACHE_LIMIT: usize = VECTOR_SIZE * 64;

#[derive(Debug, Clone)]
pub struct SortRangeJoinProbeTransformExec {
    pub handle: HandleRef<MaterializedHandle>,
    pub join_type: JoinType,
    pub conditions: Box<[JoinCondition]>,
    pub mark_null_condition_start: Option<usize>,
    pub left_projection: Box<[usize]>,
    pub right_projection: Box<[usize]>,
    pub right_output_types: Box<[LogicalType]>,
    pub output_types: Box<[LogicalType]>,
}

#[derive(Debug, Clone)]
pub struct ClassicIeJoinSourceExec {
    pub left_handle: HandleRef<MaterializedHandle>,
    pub right_handle: HandleRef<MaterializedHandle>,
    pub join_type: JoinType,
    pub conditions: Box<[JoinCondition]>,
    pub mark_null_condition_start: Option<usize>,
    pub left_projection: Box<[usize]>,
    pub right_projection: Box<[usize]>,
    pub right_output_types: Box<[LogicalType]>,
    pub output_types: Box<[LogicalType]>,
}

#[derive(Debug)]
pub struct SortRangeJoinProbeGlobal {
    reader: MaterializedReader,
    needs_found_bits: bool,
    found_bits: Mutex<Option<FoundBits>>,
    index: OnceLock<Arc<SortRangeJoinIndex>>,
}

#[derive(Debug)]
pub struct ClassicIeJoinSourceGlobal {
    left: MaterializedReader,
    right: MaterializedReader,
    cursor: Mutex<Option<ClassicIeJoinCursor>>,
}

impl SortRangeJoinProbeGlobal {
    fn ensure_found_bits(&self, build_chunks: &[Chunk]) {
        if !self.needs_found_bits {
            return;
        }
        let mut bits = self.found_bits.lock();
        if bits.is_none() {
            *bits = Some(self.reader.initialize_found_bits_for_chunks(build_chunks));
        }
    }

    fn found_bits(&self) -> Option<FoundBits> {
        self.found_bits.lock().clone()
    }
}

#[derive(Debug)]
pub(crate) struct SortRangeJoinIndex {
    entries: Vec<SortRangeJoinEntry>,
    secondary_entries: Vec<SortRangeJoinSecondaryEntry>,
    secondary_positions_by_primary_pos: Vec<usize>,
    right_vectors: Vec<Vec<Arc<Vector>>>,
    primary_key_kind: Option<SortRangeKeyKind>,
    secondary_key_kind: Option<SortRangeKeyKind>,
}

#[derive(Debug, Clone)]
struct SortRangeJoinEntry {
    key: Value,
    typed_key: Option<SortRangeKeyValue>,
    secondary_key: Option<Value>,
    secondary_typed_key: Option<SortRangeKeyValue>,
    row: BuildRowRef,
}

#[derive(Debug, Clone)]
struct SortRangeJoinSecondaryEntry {
    key: Value,
    typed_key: Option<SortRangeKeyValue>,
    primary_pos: usize,
}

#[derive(Debug, Clone, Copy)]
struct BuildRowRef {
    chunk_idx: usize,
    row_idx: usize,
    global_idx: usize,
}

#[derive(Debug)]
struct ClassicIeJoinOutput {
    left_chunks: Arc<[Chunk]>,
    right_chunks: Arc<[Chunk]>,
}

#[derive(Debug)]
struct ClassicIeJoinCursor {
    output: ClassicIeJoinOutput,
    left: ClassicIeJoinSideKeys,
    right: ClassicIeJoinSideKeys,
    left_rows: Vec<ClassicIeJoinLeftRow>,
    right_entries: Vec<SortRangeJoinEntry>,
    secondary_entries: Vec<SortRangeJoinSecondaryEntry>,
    offsets: Vec<SortRangeProbeOffsets>,
    match_counts: Vec<usize>,
    candidate_bitmap: Vec<u64>,
    touched_words: Vec<usize>,
    phase: ClassicIeJoinEmitPhase,
    left_cursor: usize,
    unmatched_cursor: usize,
    current: Option<ClassicIeJoinCurrentScan>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ClassicIeJoinEmitPhase {
    Matches,
    Unmatched,
    Done,
}

#[derive(Debug)]
struct ClassicIeJoinCurrentScan {
    left_row: ClassicIeJoinLeftRow,
    mode: ClassicIeJoinScanMode,
}

#[derive(Debug)]
enum ClassicIeJoinScanMode {
    Primary { pos: usize, end: usize },
    Secondary { pos: usize, end: usize },
}

#[derive(Debug, Clone, Copy)]
struct ClassicIeJoinOutputRow {
    left: BuildRowRef,
    right: Option<BuildRowRef>,
}

#[derive(Debug, Clone, Copy)]
struct ClassicIeJoinLeftRow {
    row: BuildRowRef,
    primary_key: SortRangeKeyValue,
    secondary_key: SortRangeKeyValue,
}

#[derive(Debug, Clone)]
struct ClassicIeJoinSideKeys {
    vectors: Vec<Vec<Arc<Vector>>>,
    rows: Vec<BuildRowRef>,
}

impl SortRangeJoinProbeTransformExec {
    pub(crate) fn create_global(&self, ctx: &mut PipelineInitContext) -> Result<TransformGlobal> {
        let handle = ctx.handles.get(self.handle)?;
        Ok(TransformGlobal::SortRangeJoinProbe(Arc::new(
            SortRangeJoinProbeGlobal {
                reader: MaterializedReader::new(handle, "sort-range join probe"),
                needs_found_bits: self.uses_found_bits(),
                found_bits: Mutex::new(None),
                index: OnceLock::new(),
            },
        )))
    }

    pub(crate) fn create_local(
        &self,
        ctx: &mut PipelineInitContext,
        _global: &TransformGlobal,
    ) -> Result<TransformLocal> {
        let left_condition_executors = self
            .conditions
            .iter()
            .map(|condition| {
                ExpressionExecutor::with_expressions_for_session(
                    std::slice::from_ref(&condition.left),
                    ctx.query.session.as_ref(),
                )
            })
            .collect();
        Ok(TransformLocal::SortRangeJoinProbe(
            SortRangeJoinProbeTransformLocal {
                left_condition_executors,
                ..Default::default()
            },
        ))
    }

    pub(crate) fn transform(
        &self,
        ctx: &mut OperatorCallContext,
        global: &TransformGlobal,
        local: &mut TransformLocal,
        input: &Chunk,
        output: &mut Chunk,
    ) -> Result<TransformPoll> {
        ctx.cancel.check()?;
        let TransformGlobal::SortRangeJoinProbe(global) = global else {
            return Err(paro_error::internal(
                "sort-range join probe global state mismatch",
            ));
        };
        let TransformLocal::SortRangeJoinProbe(local) = local else {
            return Err(paro_error::internal(
                "sort-range join probe local state mismatch",
            ));
        };
        let build_chunks = global.reader.sealed_chunks()?;

        if input.is_empty() {
            output.try_set_cardinality(0)?;
            return Ok(TransformPoll::NeedMoreInput);
        }

        if build_chunks.iter().all(Chunk::is_empty) {
            self.emit_empty_build_result(input, output)?;
            return Ok(if output.is_empty() {
                TransformPoll::NeedMoreInput
            } else {
                TransformPoll::Output
            });
        }

        let index = self.ensure_index(ctx, global.as_ref(), build_chunks.as_ref())?;
        if !local.probe_in_progress {
            global.ensure_found_bits(build_chunks.as_ref());
            local.reset_for_input();
            self.evaluate_left_conditions(ctx, input, local)?;
            self.prepare_probe_offsets(index.as_ref(), input.size(), local)?;
        }

        let found_bits = global.found_bits();
        self.probe_loop(
            input,
            build_chunks.as_ref(),
            index.as_ref(),
            found_bits.as_ref(),
            local,
            output,
        )
    }

    pub(crate) fn flush(
        &self,
        _ctx: &mut OperatorCallContext,
        _global: &TransformGlobal,
        _local: &mut TransformLocal,
        _output: &mut Chunk,
    ) -> Result<TransformFlushPoll> {
        Ok(TransformFlushPoll::Done)
    }

    pub(crate) fn finish_global(
        &self,
        _ctx: &mut OperatorFinishContext,
        _global: &TransformGlobal,
    ) -> Result<TransformFinishPoll> {
        Ok(TransformFinishPoll::Done)
    }

    fn uses_found_bits(&self) -> bool {
        matches!(
            self.join_type,
            JoinType::Right | JoinType::Outer | JoinType::RightSemi | JoinType::RightAnti
        )
    }

    fn ensure_index(
        &self,
        ctx: &OperatorCallContext,
        global: &SortRangeJoinProbeGlobal,
        build_chunks: &[Chunk],
    ) -> Result<Arc<SortRangeJoinIndex>> {
        if let Some(index) = global.index.get().map(Arc::clone) {
            return Ok(index);
        }
        if self.conditions.is_empty() {
            return Err(paro_error::internal(
                "sort-range join requires at least one range condition",
            ));
        }

        let exec_ctx = ExecutionContext::new(ctx.query.session.clone(), ctx.thread, None);
        let mut right_executors = self
            .conditions
            .iter()
            .map(|condition| {
                ExpressionExecutor::with_expressions_for_session(
                    std::slice::from_ref(&condition.right),
                    ctx.query.session.as_ref(),
                )
            })
            .collect::<Vec<_>>();
        let mut right_vectors = (0..self.conditions.len())
            .map(|_| Vec::with_capacity(build_chunks.len()))
            .collect::<Vec<_>>();
        let primary_key_kind = sort_range_key_kind_for_condition(&self.conditions[0]);
        let build_secondary_index = self.conditions.len() == 2;
        let secondary_key_kind = build_secondary_index
            .then(|| sort_range_key_kind_for_condition(&self.conditions[1]))
            .flatten();
        let mut entries = Vec::new();
        let mut global_idx = 0usize;
        for (chunk_idx, chunk) in build_chunks.iter().enumerate() {
            let mut chunk_values = Vec::with_capacity(self.conditions.len());
            for executor in &mut right_executors {
                chunk_values.push(executor.execute_expression(
                    0,
                    chunk,
                    None,
                    chunk.size(),
                    &exec_ctx,
                )?);
            }
            for (condition_idx, values) in chunk_values.iter().enumerate() {
                right_vectors[condition_idx].push(values.clone());
            }
            for row_idx in 0..chunk.size() {
                let key = vector_value(&chunk_values[0], row_idx);
                if !key.is_null() {
                    let typed_key = primary_key_kind.and_then(|kind| {
                        sort_range_key_value_from_vector(&chunk_values[0], row_idx, kind)
                    });
                    let secondary_key = if build_secondary_index {
                        let value = vector_value(&chunk_values[1], row_idx);
                        (!value.is_null()).then_some(value)
                    } else {
                        None
                    };
                    let secondary_typed_key = secondary_key.as_ref().and_then(|_| {
                        secondary_key_kind.and_then(|kind| {
                            sort_range_key_value_from_vector(&chunk_values[1], row_idx, kind)
                        })
                    });
                    entries.push(SortRangeJoinEntry {
                        key,
                        typed_key,
                        secondary_key,
                        secondary_typed_key,
                        row: BuildRowRef {
                            chunk_idx,
                            row_idx,
                            global_idx,
                        },
                    });
                }
                global_idx += 1;
            }
        }
        sort_range_entries(&mut entries, primary_key_kind)?;
        let secondary_entries = build_secondary_entries(&entries, secondary_key_kind)?;
        let secondary_positions_by_primary_pos =
            build_secondary_positions_by_primary_pos(entries.len(), &secondary_entries);
        let index = Arc::new(SortRangeJoinIndex {
            entries,
            secondary_entries,
            secondary_positions_by_primary_pos,
            right_vectors,
            primary_key_kind,
            secondary_key_kind,
        });
        match global.index.set(Arc::clone(&index)) {
            Ok(()) => Ok(index),
            Err(_) => Ok(Arc::clone(global.index.get().expect(
                "sort-range join index should be initialized after racing set",
            ))),
        }
    }

    fn evaluate_left_conditions(
        &self,
        ctx: &OperatorCallContext,
        input: &Chunk,
        local: &mut SortRangeJoinProbeTransformLocal,
    ) -> Result<()> {
        let exec_ctx = ExecutionContext::new(ctx.query.session.clone(), ctx.thread, None);
        local.left_condition_results.clear();
        local.left_condition_results.reserve(self.conditions.len());
        for executor in &mut local.left_condition_executors {
            local
                .left_condition_results
                .push(executor.execute_expression(0, input, None, input.size(), &exec_ctx)?);
        }
        Ok(())
    }

    fn prepare_probe_offsets(
        &self,
        index: &SortRangeJoinIndex,
        input_size: usize,
        local: &mut SortRangeJoinProbeTransformLocal,
    ) -> Result<()> {
        let first = self
            .conditions
            .first()
            .ok_or_else(|| paro_error::internal("sort-range join missing first condition"))?;
        let use_secondary_cache = self.uses_secondary_candidate_bitmap();
        let second = use_secondary_cache.then_some(&self.conditions[1]);

        local.probe_offsets.clear();
        local.probe_offsets.reserve(input_size);
        local.cached_candidate_ranges.clear();
        local.cached_candidate_positions.clear();
        local.cached_candidates_ready = false;

        local
            .probe_offsets
            .resize(input_size, SortRangeProbeOffsets::default());
        fill_probe_offsets_by_sweeping_probe_permutation(
            &index.entries,
            local,
            input_size,
            ProbeOffsetSweepSpec {
                dimension: ProbeOffsetDimension::Primary,
                condition_idx: 0,
                comparison: first.comparison,
                key_kind: index.primary_key_kind,
                null_is_unknown: self.mark_condition_null_is_unknown(0),
            },
        )?;

        if let Some(second) = second {
            fill_probe_offsets_by_sweeping_probe_permutation(
                &index.secondary_entries,
                local,
                input_size,
                ProbeOffsetSweepSpec {
                    dimension: ProbeOffsetDimension::Secondary,
                    condition_idx: 1,
                    comparison: second.comparison,
                    key_kind: index.secondary_key_kind,
                    null_is_unknown: self.mark_condition_null_is_unknown(1),
                },
            )?;
        }

        if let Some(second) = second {
            prepare_incremental_secondary_candidate_cache(index, local, second.comparison);
        }
        Ok(())
    }

    fn probe_loop(
        &self,
        input: &Chunk,
        build_chunks: &[Chunk],
        index: &SortRangeJoinIndex,
        found_bits: Option<&FoundBits>,
        local: &mut SortRangeJoinProbeTransformLocal,
        output: &mut Chunk,
    ) -> Result<TransformPoll> {
        ensure_transform_output(output, &self.output_types, VECTOR_SIZE)?;
        loop {
            if local.probe_row >= input.size() {
                local.probe_in_progress = false;
                if local.output_row > 0 {
                    self.flush_output(local, output)?;
                    return Ok(TransformPoll::Output);
                }
                output.try_set_cardinality(0)?;
                return Ok(TransformPoll::NeedMoreInput);
            }

            if !local.candidate_ready {
                self.prepare_probe_row(index, local)?;
            }

            if local.candidate_pos >= local.candidate_end {
                self.emit_probe_row_conclusion(input, local, output)?;
                local.advance_probe_row();
                if local.output_row >= VECTOR_SIZE {
                    self.flush_output(local, output)?;
                    return Ok(if local.probe_row >= input.size() {
                        local.probe_in_progress = false;
                        TransformPoll::Output
                    } else {
                        TransformPoll::OutputMore
                    });
                }
                continue;
            }

            let entry_idx = match local.candidate_source {
                SortRangeCandidateSource::PrimaryRange => local.candidate_pos,
                SortRangeCandidateSource::SparsePositions => {
                    local.candidate_positions[local.candidate_pos]
                }
                SortRangeCandidateSource::CachedSparsePositions => {
                    local.cached_candidate_positions[local.candidate_pos]
                }
            };
            let entry = &index.entries[entry_idx];
            local.candidate_pos += 1;
            let match_result = self.evaluate_candidate(index, local, entry.row)?;
            match match_result {
                RowMatch::Match => {
                    if let Some(bits) = found_bits {
                        bits.mark(entry.row.global_idx);
                    }
                    local.found_match = true;
                    match self.join_type {
                        JoinType::Semi => {
                            self.append_left_only(input, local, output)?;
                            local.candidate_pos = local.candidate_end;
                        }
                        JoinType::Anti => {
                            local.candidate_pos = local.candidate_end;
                        }
                        JoinType::Single => {
                            if local.single_match_found {
                                return Err(paro_error::internal(
                                    "scalar subquery produced more than one row",
                                ));
                            }
                            local.single_match_found = true;
                            self.append_join_match(
                                input,
                                &build_chunks[entry.row.chunk_idx],
                                entry.row,
                                local,
                                output,
                            )?;
                        }
                        JoinType::Mark => {
                            local.candidate_pos = local.candidate_end;
                        }
                        JoinType::RightSemi | JoinType::RightAnti => {}
                        _ => {
                            self.append_join_match(
                                input,
                                &build_chunks[entry.row.chunk_idx],
                                entry.row,
                                local,
                                output,
                            )?;
                        }
                    }
                }
                RowMatch::Null => {
                    local.saw_null = true;
                }
                RowMatch::NoMatch => {}
            }

            if local.output_row >= VECTOR_SIZE {
                self.flush_output(local, output)?;
                return Ok(TransformPoll::OutputMore);
            }
        }
    }

    fn prepare_probe_row(
        &self,
        index: &SortRangeJoinIndex,
        local: &mut SortRangeJoinProbeTransformLocal,
    ) -> Result<()> {
        local.found_match = false;
        local.saw_null = false;
        local.single_match_found = false;
        local.candidate_ready = true;
        local.candidate_source = SortRangeCandidateSource::PrimaryRange;
        local.candidate_positions.clear();

        if let Some(offsets) = local.probe_offsets.get(local.probe_row).copied() {
            local.saw_null = offsets.saw_null;
            if !offsets.valid {
                set_empty_candidates(local);
                return Ok(());
            }
            if self.uses_secondary_candidate_bitmap() {
                if local.cached_candidates_ready {
                    let range = local
                        .cached_candidate_ranges
                        .get(local.probe_row)
                        .copied()
                        .unwrap_or(SortRangeCandidateRange { start: 0, end: 0 });
                    local.candidate_start = range.start;
                    local.candidate_end = range.end;
                    local.candidate_pos = range.start;
                    local.candidate_source = SortRangeCandidateSource::CachedSparsePositions;
                    return Ok(());
                }
                prepare_secondary_candidates_from_offsets(
                    index,
                    local,
                    offsets.primary_start,
                    offsets.primary_end,
                    offsets.secondary_start,
                    offsets.secondary_end,
                );
                return Ok(());
            }
            local.candidate_start = offsets.primary_start;
            local.candidate_end = offsets.primary_end;
            local.candidate_pos = offsets.primary_start;
            return Ok(());
        }

        let first = self
            .conditions
            .first()
            .ok_or_else(|| paro_error::internal("sort-range join missing first condition"))?;
        let left_value = left_condition_value(local, 0, first);
        if left_value.is_null() {
            local.saw_null = self.mark_condition_null_is_unknown(0);
            set_empty_candidates(local);
            return Ok(());
        }

        let left_lookup = SortRangeLookupKey::Value(left_value);
        let (start, end) = range_for_comparison(
            &index.entries,
            &left_lookup,
            first.comparison,
            index.primary_key_kind,
        )?;
        if self.uses_secondary_candidate_bitmap() {
            let second = &self.conditions[1];
            let second_left = left_condition_value(local, 1, second);
            if second_left.is_null() {
                local.saw_null = self.mark_condition_null_is_unknown(1);
                set_empty_candidates(local);
                return Ok(());
            }
            prepare_secondary_candidates(
                index,
                local,
                start,
                end,
                SortRangeLookupKey::Value(second_left),
                second.comparison,
            )?;
            return Ok(());
        }
        local.candidate_start = start;
        local.candidate_end = end;
        local.candidate_pos = start;
        Ok(())
    }

    fn uses_secondary_candidate_bitmap(&self) -> bool {
        self.join_type != JoinType::Mark && self.conditions.len() == 2
    }

    fn evaluate_candidate(
        &self,
        index: &SortRangeJoinIndex,
        local: &SortRangeJoinProbeTransformLocal,
        row: BuildRowRef,
    ) -> Result<RowMatch> {
        let mut saw_null = false;
        for (condition_idx, condition) in self.conditions.iter().enumerate() {
            let left_value = left_condition_value(local, condition_idx, condition);
            let right_value = right_condition_value(index, condition_idx, condition, row);
            match compare_with_nulls(condition.comparison, &left_value, &right_value) {
                Some(true) => {}
                Some(false) => return Ok(RowMatch::NoMatch),
                None => {
                    if self.mark_condition_null_is_unknown(condition_idx) {
                        saw_null = true;
                    } else {
                        return Ok(RowMatch::NoMatch);
                    }
                }
            }
        }
        Ok(if saw_null {
            RowMatch::Null
        } else {
            RowMatch::Match
        })
    }

    fn mark_condition_null_is_unknown(&self, condition_idx: usize) -> bool {
        if self.join_type != JoinType::Mark {
            return true;
        }
        self.mark_null_condition_start
            .is_some_and(|start| condition_idx >= start)
    }

    fn emit_empty_build_result(&self, input: &Chunk, output: &mut Chunk) -> Result<()> {
        ensure_transform_output(output, &self.output_types, VECTOR_SIZE)?;
        match self.join_type {
            JoinType::Anti => emit_projected_left(input, &self.left_projection, output),
            JoinType::Left | JoinType::Outer | JoinType::Single => emit_left_with_null_right(
                input,
                &self.left_projection,
                &self.right_output_types,
                output,
            ),
            JoinType::Mark => emit_mark_all(input, &self.left_projection, Some(false), output),
            _ => {
                output.try_set_cardinality(0)?;
                Ok(())
            }
        }
    }

    fn emit_probe_row_conclusion(
        &self,
        input: &Chunk,
        local: &mut SortRangeJoinProbeTransformLocal,
        output: &mut Chunk,
    ) -> Result<()> {
        if local.found_match {
            if self.join_type == JoinType::Mark {
                self.append_mark_row(input, local, output, Some(true))?;
            }
            return Ok(());
        }
        match self.join_type {
            JoinType::Anti => self.append_left_only(input, local, output)?,
            JoinType::Left | JoinType::Outer | JoinType::Single => {
                self.append_left_null_right(input, local, output)?;
            }
            JoinType::Mark => {
                let marker = if local.saw_null { None } else { Some(false) };
                self.append_mark_row(input, local, output, marker)?;
            }
            _ => {}
        }
        Ok(())
    }

    fn append_join_match(
        &self,
        input: &Chunk,
        build_chunk: &Chunk,
        row_ref: BuildRowRef,
        local: &mut SortRangeJoinProbeTransformLocal,
        output: &mut Chunk,
    ) -> Result<()> {
        let row = local.output_row;
        copy_projected_row(
            input,
            local.probe_row,
            &self.left_projection,
            output,
            row,
            0,
        )?;
        let right_offset = self.left_projection.len();
        copy_projected_row(
            build_chunk,
            row_ref.row_idx,
            &self.right_projection,
            output,
            row,
            right_offset,
        )?;
        local.output_row += 1;
        Ok(())
    }

    fn append_left_only(
        &self,
        input: &Chunk,
        local: &mut SortRangeJoinProbeTransformLocal,
        output: &mut Chunk,
    ) -> Result<()> {
        let row = local.output_row;
        copy_projected_row(
            input,
            local.probe_row,
            &self.left_projection,
            output,
            row,
            0,
        )?;
        local.output_row += 1;
        Ok(())
    }

    fn append_left_null_right(
        &self,
        input: &Chunk,
        local: &mut SortRangeJoinProbeTransformLocal,
        output: &mut Chunk,
    ) -> Result<()> {
        let row = local.output_row;
        copy_projected_row(
            input,
            local.probe_row,
            &self.left_projection,
            output,
            row,
            0,
        )?;
        let right_offset = self.left_projection.len();
        for (out_col, typ) in self.right_output_types.iter().enumerate() {
            let target = output.column_mut(right_offset + out_col).unwrap();
            target.set_value(row, &Value::Null(typ.clone()));
            target.try_set_null(row, true)?;
        }
        local.output_row += 1;
        Ok(())
    }

    fn append_mark_row(
        &self,
        input: &Chunk,
        local: &mut SortRangeJoinProbeTransformLocal,
        output: &mut Chunk,
        marker: Option<bool>,
    ) -> Result<()> {
        let row = local.output_row;
        copy_projected_row(
            input,
            local.probe_row,
            &self.left_projection,
            output,
            row,
            0,
        )?;
        let mark_col = output.column_mut(self.left_projection.len()).unwrap();
        match marker {
            Some(value) => {
                mark_col.set_value(row, &Value::Boolean(value));
                mark_col.try_set_null(row, false)?;
            }
            None => {
                mark_col.set_value(row, &Value::Boolean(false));
                mark_col.try_set_null(row, true)?;
            }
        }
        local.output_row += 1;
        Ok(())
    }

    fn flush_output(
        &self,
        local: &mut SortRangeJoinProbeTransformLocal,
        output: &mut Chunk,
    ) -> Result<()> {
        output.try_set_cardinality(local.output_row)?;
        local.output_row = 0;
        Ok(())
    }
}

impl ClassicIeJoinSourceExec {
    pub(crate) fn create_global(&self, ctx: &mut PipelineInitContext) -> Result<SourceGlobal> {
        Ok(SourceGlobal::ClassicIeJoin(Arc::new(
            ClassicIeJoinSourceGlobal {
                left: MaterializedReader::new(
                    ctx.handles.get(self.left_handle)?,
                    "classic IE join left input",
                ),
                right: MaterializedReader::new(
                    ctx.handles.get(self.right_handle)?,
                    "classic IE join right input",
                ),
                cursor: Mutex::new(None),
            },
        )))
    }

    pub(crate) fn create_local(
        &self,
        _ctx: &mut PipelineInitContext,
        _global: &SourceGlobal,
    ) -> Result<SourceLocal> {
        Ok(SourceLocal::ClassicIeJoin(ClassicIeJoinSourceLocal))
    }

    pub(crate) fn poll_next(
        &self,
        ctx: &mut OperatorCallContext,
        global: &SourceGlobal,
        _local: &mut SourceLocal,
        output: &mut Chunk,
    ) -> Result<SourcePoll> {
        ctx.cancel.check()?;
        let SourceGlobal::ClassicIeJoin(global) = global else {
            return Err(paro_error::internal(
                "classic IE join source global state mismatch",
            ));
        };
        ensure_transform_output(output, &self.output_types, VECTOR_SIZE)?;
        let mut guard = global.cursor.lock();
        if guard.is_none() {
            *guard = Some(self.build_cursor(ctx, global.as_ref())?);
        }
        let cursor = guard
            .as_mut()
            .expect("classic IE join cursor initialized above");
        let mut out_row = 0usize;
        while out_row < VECTOR_SIZE {
            let Some(joined_row) = cursor.next_output_row(self)? else {
                break;
            };
            copy_classic_ie_join_output_row(
                &cursor.output,
                joined_row,
                &self.left_projection,
                &self.right_projection,
                &self.right_output_types,
                output,
                out_row,
            )?;
            out_row += 1;
        }
        if out_row == 0 {
            output.try_set_cardinality(0)?;
            return Ok(SourcePoll::Finished);
        }
        output.try_set_cardinality(out_row)?;
        Ok(SourcePoll::Output)
    }

    fn build_cursor(
        &self,
        ctx: &OperatorCallContext,
        global: &ClassicIeJoinSourceGlobal,
    ) -> Result<ClassicIeJoinCursor> {
        if self.conditions.len() != 2 {
            return Err(paro_error::internal(
                "classic IE join requires exactly two inequality conditions",
            ));
        }
        if !matches!(
            self.join_type,
            JoinType::Inner | JoinType::Left | JoinType::Semi | JoinType::Anti | JoinType::Single
        ) {
            return Err(paro_error::internal(format!(
                "classic IE join source does not support {} joins",
                self.join_type
            )));
        }

        let left_chunks = Arc::clone(global.left.sealed_chunks()?);
        let right_chunks = Arc::clone(global.right.sealed_chunks()?);
        let left_keys =
            evaluate_classic_ie_join_side_keys(ctx, &left_chunks, &self.conditions, true)?;
        let right_keys =
            evaluate_classic_ie_join_side_keys(ctx, &right_chunks, &self.conditions, false)?;
        build_classic_ie_join_cursor(self, left_chunks, right_chunks, left_keys, right_keys)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RowMatch {
    Match,
    NoMatch,
    Null,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SortRangeKeyKind {
    Signed,
    Unsigned,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SortRangeKeyValue {
    Signed(i128),
    Unsigned(u128),
}

impl SortRangeKeyValue {
    fn cmp_same_kind(self, other: Self) -> Option<Ordering> {
        match (self, other) {
            (SortRangeKeyValue::Signed(left), SortRangeKeyValue::Signed(right)) => {
                Some(left.cmp(&right))
            }
            (SortRangeKeyValue::Unsigned(left), SortRangeKeyValue::Unsigned(right)) => {
                Some(left.cmp(&right))
            }
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
enum SortRangeLookupKey {
    Typed(SortRangeKeyValue),
    Value(Value),
}

#[derive(Debug, Clone)]
struct SortRangeProbeKey {
    row_idx: usize,
    lookup: SortRangeLookupKey,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProbeOffsetDimension {
    Primary,
    Secondary,
}

#[derive(Debug, Clone, Copy)]
struct ProbeOffsetSweepSpec {
    dimension: ProbeOffsetDimension,
    condition_idx: usize,
    comparison: JoinComparisonType,
    key_kind: Option<SortRangeKeyKind>,
    null_is_unknown: bool,
}

trait SortRangeKey {
    fn sort_key(&self) -> &Value;
    fn typed_sort_key(&self) -> Option<SortRangeKeyValue>;
}

impl SortRangeKey for SortRangeJoinEntry {
    fn sort_key(&self) -> &Value {
        &self.key
    }

    fn typed_sort_key(&self) -> Option<SortRangeKeyValue> {
        self.typed_key
    }
}

impl SortRangeKey for SortRangeJoinSecondaryEntry {
    fn sort_key(&self) -> &Value {
        &self.key
    }

    fn typed_sort_key(&self) -> Option<SortRangeKeyValue> {
        self.typed_key
    }
}

fn left_condition_value(
    local: &SortRangeJoinProbeTransformLocal,
    condition_idx: usize,
    condition: &JoinCondition,
) -> Value {
    left_condition_value_at(local, local.probe_row, condition_idx, condition)
}

fn left_condition_value_at(
    local: &SortRangeJoinProbeTransformLocal,
    row_idx: usize,
    condition_idx: usize,
    condition: &JoinCondition,
) -> Value {
    local
        .left_condition_results
        .get(condition_idx)
        .map(|values| vector_value(values, row_idx))
        .unwrap_or(Value::Null(condition.left.return_type()))
}

fn left_condition_lookup_key_at(
    local: &SortRangeJoinProbeTransformLocal,
    row_idx: usize,
    condition_idx: usize,
    key_kind: Option<SortRangeKeyKind>,
) -> Option<SortRangeLookupKey> {
    let values = local.left_condition_results.get(condition_idx)?;
    if let Some(kind) = key_kind {
        return sort_range_key_value_from_vector(values, row_idx, kind)
            .map(SortRangeLookupKey::Typed);
    }

    let value = vector_value(values, row_idx);
    (!value.is_null()).then_some(SortRangeLookupKey::Value(value))
}

fn probe_key_at(
    local: &SortRangeJoinProbeTransformLocal,
    row_idx: usize,
    condition_idx: usize,
    key_kind: Option<SortRangeKeyKind>,
) -> Option<SortRangeProbeKey> {
    left_condition_lookup_key_at(local, row_idx, condition_idx, key_kind)
        .map(|lookup| SortRangeProbeKey { row_idx, lookup })
}

fn right_condition_value(
    index: &SortRangeJoinIndex,
    condition_idx: usize,
    condition: &JoinCondition,
    row: BuildRowRef,
) -> Value {
    index
        .right_vectors
        .get(condition_idx)
        .and_then(|chunks| chunks.get(row.chunk_idx))
        .map(|values| vector_value(values, row.row_idx))
        .unwrap_or_else(|| Value::Null(condition.right.return_type()))
}

fn vector_value(values: &Vector, row_idx: usize) -> Value {
    values.get_value(row_idx)
}

fn sort_range_key_kind_for_condition(condition: &JoinCondition) -> Option<SortRangeKeyKind> {
    let left = condition.left.return_type();
    let right = condition.right.return_type();
    let left_kind = sort_range_key_kind_for_type(&left)?;
    let right_kind = sort_range_key_kind_for_type(&right)?;
    (left_kind == right_kind).then_some(left_kind)
}

fn sort_range_key_kind_for_type(logical_type: &LogicalType) -> Option<SortRangeKeyKind> {
    match logical_type {
        LogicalType::TinyInt
        | LogicalType::SmallInt
        | LogicalType::Integer
        | LogicalType::BigInt
        | LogicalType::HugeInt
        | LogicalType::Date
        | LogicalType::Timestamp
        | LogicalType::TimestampTz
        | LogicalType::Time => Some(SortRangeKeyKind::Signed),
        LogicalType::UTinyInt
        | LogicalType::USmallInt
        | LogicalType::UInteger
        | LogicalType::UBigInt
        | LogicalType::UHugeInt
        | LogicalType::Uuid => Some(SortRangeKeyKind::Unsigned),
        _ => None,
    }
}

fn sort_range_key_value_from_vector(
    vector: &Vector,
    row_idx: usize,
    kind: SortRangeKeyKind,
) -> Option<SortRangeKeyValue> {
    match (kind, vector.logical_type()) {
        (SortRangeKeyKind::Signed, LogicalType::TinyInt) => vector
            .get_i8(row_idx)
            .map(|value| SortRangeKeyValue::Signed(value as i128)),
        (SortRangeKeyKind::Signed, LogicalType::SmallInt) => vector
            .get_i16(row_idx)
            .map(|value| SortRangeKeyValue::Signed(value as i128)),
        (SortRangeKeyKind::Signed, LogicalType::Integer | LogicalType::Date) => vector
            .get_i32(row_idx)
            .map(|value| SortRangeKeyValue::Signed(value as i128)),
        (
            SortRangeKeyKind::Signed,
            LogicalType::BigInt
            | LogicalType::Timestamp
            | LogicalType::TimestampTz
            | LogicalType::Time,
        ) => vector
            .get_i64(row_idx)
            .map(|value| SortRangeKeyValue::Signed(value as i128)),
        (SortRangeKeyKind::Signed, LogicalType::HugeInt) => {
            vector.get_i128(row_idx).map(SortRangeKeyValue::Signed)
        }
        (SortRangeKeyKind::Unsigned, LogicalType::UTinyInt) => vector
            .get_u8(row_idx)
            .map(|value| SortRangeKeyValue::Unsigned(value as u128)),
        (SortRangeKeyKind::Unsigned, LogicalType::USmallInt) => vector
            .get_u16(row_idx)
            .map(|value| SortRangeKeyValue::Unsigned(value as u128)),
        (SortRangeKeyKind::Unsigned, LogicalType::UInteger) => vector
            .get_u32(row_idx)
            .map(|value| SortRangeKeyValue::Unsigned(value as u128)),
        (SortRangeKeyKind::Unsigned, LogicalType::UBigInt) => vector
            .get_u64(row_idx)
            .map(|value| SortRangeKeyValue::Unsigned(value as u128)),
        (SortRangeKeyKind::Unsigned, LogicalType::UHugeInt | LogicalType::Uuid) => {
            vector.get_u128(row_idx).map(SortRangeKeyValue::Unsigned)
        }
        _ => None,
    }
}

fn build_secondary_entries(
    entries: &[SortRangeJoinEntry],
    key_kind: Option<SortRangeKeyKind>,
) -> Result<Vec<SortRangeJoinSecondaryEntry>> {
    let mut secondary_entries = entries
        .iter()
        .enumerate()
        .filter_map(|(primary_pos, entry)| {
            entry
                .secondary_key
                .as_ref()
                .map(|key| SortRangeJoinSecondaryEntry {
                    key: key.clone(),
                    typed_key: entry.secondary_typed_key,
                    primary_pos,
                })
        })
        .collect::<Vec<_>>();
    sort_range_entries(&mut secondary_entries, key_kind)?;
    Ok(secondary_entries)
}

fn build_secondary_positions_by_primary_pos(
    entry_count: usize,
    secondary_entries: &[SortRangeJoinSecondaryEntry],
) -> Vec<usize> {
    let mut positions = vec![usize::MAX; entry_count];
    for (secondary_pos, entry) in secondary_entries.iter().enumerate() {
        if let Some(position) = positions.get_mut(entry.primary_pos) {
            *position = secondary_pos;
        }
    }
    positions
}

fn set_empty_candidates(local: &mut SortRangeJoinProbeTransformLocal) {
    local.candidate_start = 0;
    local.candidate_end = 0;
    local.candidate_pos = 0;
    local.candidate_source = SortRangeCandidateSource::PrimaryRange;
}

fn clear_secondary_candidate_bitmap(
    local: &mut SortRangeJoinProbeTransformLocal,
    word_count: usize,
) {
    clear_candidate_bitmap(
        &mut local.secondary_candidate_bitmap,
        &mut local.secondary_candidate_touched_words,
        word_count,
    );
}

fn fill_probe_offsets_by_sweeping_probe_permutation<T: SortRangeKey>(
    entries: &[T],
    local: &mut SortRangeJoinProbeTransformLocal,
    input_size: usize,
    spec: ProbeOffsetSweepSpec,
) -> Result<()> {
    let mut probe_keys = Vec::with_capacity(input_size);
    for row_idx in 0..input_size {
        if spec.dimension == ProbeOffsetDimension::Secondary && !local.probe_offsets[row_idx].valid
        {
            continue;
        }
        let Some(key) = probe_key_at(local, row_idx, spec.condition_idx, spec.key_kind) else {
            let offsets = &mut local.probe_offsets[row_idx];
            offsets.valid = false;
            offsets.saw_null |= spec.null_is_unknown;
            continue;
        };
        probe_keys.push(key);
    }
    sort_probe_keys(&mut probe_keys, spec.key_kind)?;

    let mut lower_cursor = 0usize;
    let mut upper_cursor = 0usize;
    for probe_key in &probe_keys {
        while lower_cursor < entries.len()
            && compare_entry_with_lookup_key(
                &entries[lower_cursor],
                &probe_key.lookup,
                spec.key_kind,
            )?
            .is_lt()
        {
            lower_cursor += 1;
        }
        while upper_cursor < entries.len()
            && matches!(
                compare_entry_with_lookup_key(
                    &entries[upper_cursor],
                    &probe_key.lookup,
                    spec.key_kind
                )?,
                Ordering::Less | Ordering::Equal
            )
        {
            upper_cursor += 1;
        }

        let (start, end) =
            range_from_sweep_bounds(entries.len(), lower_cursor, upper_cursor, spec.comparison);
        let offsets = &mut local.probe_offsets[probe_key.row_idx];
        match spec.dimension {
            ProbeOffsetDimension::Primary => {
                offsets.primary_start = start;
                offsets.primary_end = end;
                offsets.valid = true;
            }
            ProbeOffsetDimension::Secondary => {
                offsets.secondary_start = start;
                offsets.secondary_end = end;
            }
        }
    }
    Ok(())
}

fn range_from_sweep_bounds(
    entry_count: usize,
    lower_bound: usize,
    upper_bound: usize,
    comparison: JoinComparisonType,
) -> (usize, usize) {
    match comparison {
        JoinComparisonType::LessThan => (upper_bound, entry_count),
        JoinComparisonType::LessThanOrEqual => (lower_bound, entry_count),
        JoinComparisonType::GreaterThan => (0, lower_bound),
        JoinComparisonType::GreaterThanOrEqual => (0, upper_bound),
        _ => (0, entry_count),
    }
}

fn sort_probe_keys(
    keys: &mut [SortRangeProbeKey],
    key_kind: Option<SortRangeKeyKind>,
) -> Result<()> {
    let mut incomparable: Option<(Value, Value)> = None;
    keys.sort_unstable_by(|left, right| {
        compare_probe_keys_for_sort(left, right, key_kind).unwrap_or_else(|| {
            if incomparable.is_none() {
                incomparable = Some((
                    lookup_debug_value(&left.lookup),
                    lookup_debug_value(&right.lookup),
                ));
            }
            Ordering::Equal
        })
    });
    if let Some((left, right)) = incomparable {
        return Err(incomparable_sort_range_join_key(&left, &right));
    }
    Ok(())
}

fn compare_probe_keys_for_sort(
    left: &SortRangeProbeKey,
    right: &SortRangeProbeKey,
    key_kind: Option<SortRangeKeyKind>,
) -> Option<Ordering> {
    match (&left.lookup, &right.lookup, key_kind) {
        (SortRangeLookupKey::Typed(left), SortRangeLookupKey::Typed(right), Some(_)) => {
            left.cmp_same_kind(*right)
        }
        (SortRangeLookupKey::Value(left), SortRangeLookupKey::Value(right), None) => {
            left.partial_cmp(right)
        }
        _ => None,
    }
}

fn lookup_debug_value(key: &SortRangeLookupKey) -> Value {
    match key {
        SortRangeLookupKey::Value(value) => value.clone(),
        SortRangeLookupKey::Typed(SortRangeKeyValue::Signed(value)) => Value::HugeInt(*value),
        SortRangeLookupKey::Typed(SortRangeKeyValue::Unsigned(value)) => Value::UHugeInt(*value),
    }
}

fn clear_primary_candidate_bitmap(local: &mut SortRangeJoinProbeTransformLocal, word_count: usize) {
    clear_candidate_bitmap(
        &mut local.primary_candidate_bitmap,
        &mut local.primary_candidate_touched_words,
        word_count,
    );
}

fn clear_candidate_bitmap(
    bitmap: &mut Vec<u64>,
    touched_words: &mut Vec<usize>,
    word_count: usize,
) {
    for word_idx in touched_words.drain(..) {
        if let Some(word) = bitmap.get_mut(word_idx) {
            *word = 0;
        }
    }
    if bitmap.len() < word_count {
        bitmap.resize(word_count, 0);
    }
}

fn bitmap_set(bitmap: &mut [u64], touched_words: &mut Vec<usize>, pos: usize) {
    let word_idx = pos / 64;
    let word = &mut bitmap[word_idx];
    if *word == 0 {
        touched_words.push(word_idx);
    }
    *word |= 1u64 << (pos % 64);
}

fn bitmap_contains(bitmap: &[u64], pos: usize) -> bool {
    bitmap
        .get(pos / 64)
        .is_some_and(|word| (word & (1u64 << (pos % 64))) != 0)
}

fn prepare_secondary_candidates(
    index: &SortRangeJoinIndex,
    local: &mut SortRangeJoinProbeTransformLocal,
    primary_start: usize,
    primary_end: usize,
    second_left: SortRangeLookupKey,
    comparison: JoinComparisonType,
) -> Result<()> {
    let (secondary_start, secondary_end) = range_for_comparison(
        &index.secondary_entries,
        &second_left,
        comparison,
        index.secondary_key_kind,
    )?;
    prepare_secondary_candidates_from_offsets(
        index,
        local,
        primary_start,
        primary_end,
        secondary_start,
        secondary_end,
    );
    Ok(())
}

fn prepare_secondary_candidates_from_offsets(
    index: &SortRangeJoinIndex,
    local: &mut SortRangeJoinProbeTransformLocal,
    primary_start: usize,
    primary_end: usize,
    secondary_start: usize,
    secondary_end: usize,
) {
    local.candidate_positions.clear();
    local.candidate_positions.reserve(candidate_range_reserve(
        primary_start,
        primary_end,
        secondary_start,
        secondary_end,
    ));
    if range_len(primary_start, primary_end) <= range_len(secondary_start, secondary_end) {
        append_candidates_by_primary_rank_scan(
            index,
            local,
            primary_start,
            primary_end,
            secondary_start,
            secondary_end,
        );
    } else {
        append_candidates_by_secondary_range_scan(
            index,
            local,
            primary_start,
            primary_end,
            secondary_start,
            secondary_end,
        );
    }

    local.candidate_start = 0;
    local.candidate_end = local.candidate_positions.len();
    local.candidate_pos = 0;
    local.candidate_source = SortRangeCandidateSource::SparsePositions;
}

fn prepare_incremental_secondary_candidate_cache(
    index: &SortRangeJoinIndex,
    local: &mut SortRangeJoinProbeTransformLocal,
    comparison: JoinComparisonType,
) {
    local.cached_candidates_ready = false;
    local.cached_candidate_ranges.clear();
    local.cached_candidate_positions.clear();

    let activation = match comparison {
        JoinComparisonType::LessThan | JoinComparisonType::LessThanOrEqual => {
            SecondaryActivation::Suffix
        }
        JoinComparisonType::GreaterThan | JoinComparisonType::GreaterThanOrEqual => {
            SecondaryActivation::Prefix
        }
        _ => return,
    };

    let row_count = local.probe_offsets.len();
    local
        .cached_candidate_ranges
        .resize(row_count, SortRangeCandidateRange::default());

    let mut probe_order = std::mem::take(&mut local.candidate_cache_probe_order);
    probe_order.clear();
    for row_idx in 0..row_count {
        let offsets = local.probe_offsets[row_idx];
        if offsets.valid
            && offsets.primary_start < offsets.primary_end
            && offsets.secondary_start < offsets.secondary_end
        {
            probe_order.push(row_idx);
        }
    }

    let word_count = (index.entries.len() + 63) / 64;
    clear_secondary_candidate_bitmap(local, word_count);

    let mut cache_completed = true;
    match activation {
        SecondaryActivation::Prefix => {
            probe_order.sort_unstable_by_key(|&row_idx| local.probe_offsets[row_idx].secondary_end);
            let mut active_end = 0usize;
            for row_idx in probe_order.iter().copied() {
                let offsets = local.probe_offsets[row_idx];
                if offsets.secondary_end > active_end {
                    let bitmap = &mut local.secondary_candidate_bitmap;
                    let touched_words = &mut local.secondary_candidate_touched_words;
                    for entry in &index.secondary_entries[active_end..offsets.secondary_end] {
                        bitmap_set(bitmap, touched_words, entry.primary_pos);
                    }
                    active_end = offsets.secondary_end;
                }
                if !append_cached_secondary_candidates(index, local, row_idx, offsets) {
                    cache_completed = false;
                    break;
                }
            }
        }
        SecondaryActivation::Suffix => {
            probe_order.sort_unstable_by(|&left, &right| {
                local.probe_offsets[right]
                    .secondary_start
                    .cmp(&local.probe_offsets[left].secondary_start)
            });
            let mut active_start = index.secondary_entries.len();
            for row_idx in probe_order.iter().copied() {
                let offsets = local.probe_offsets[row_idx];
                if offsets.secondary_start < active_start {
                    let bitmap = &mut local.secondary_candidate_bitmap;
                    let touched_words = &mut local.secondary_candidate_touched_words;
                    for entry in &index.secondary_entries[offsets.secondary_start..active_start] {
                        bitmap_set(bitmap, touched_words, entry.primary_pos);
                    }
                    active_start = offsets.secondary_start;
                }
                if !append_cached_secondary_candidates(index, local, row_idx, offsets) {
                    cache_completed = false;
                    break;
                }
            }
        }
    }

    clear_secondary_candidate_bitmap(local, word_count);
    probe_order.clear();
    local.candidate_cache_probe_order = probe_order;

    if cache_completed {
        local.cached_candidates_ready = true;
    } else {
        local.cached_candidate_ranges.clear();
        local.cached_candidate_positions.clear();
    }
}

#[derive(Debug, Clone, Copy)]
enum SecondaryActivation {
    Prefix,
    Suffix,
}

fn append_cached_secondary_candidates(
    index: &SortRangeJoinIndex,
    local: &mut SortRangeJoinProbeTransformLocal,
    row_idx: usize,
    offsets: SortRangeProbeOffsets,
) -> bool {
    let range_start = local.cached_candidate_positions.len();
    let cache_completed = if range_len(offsets.primary_start, offsets.primary_end)
        <= range_len(offsets.secondary_start, offsets.secondary_end)
    {
        append_cached_candidates_by_primary_scan(local, offsets)
    } else {
        let word_count = (index.entries.len() + 63) / 64;
        clear_primary_candidate_bitmap(local, word_count);
        append_cached_candidates_by_secondary_scan(index, local, offsets)
    };
    if !cache_completed {
        return false;
    }
    let range_end = local.cached_candidate_positions.len();
    if let Some(range) = local.cached_candidate_ranges.get_mut(row_idx) {
        *range = SortRangeCandidateRange {
            start: range_start,
            end: range_end,
        };
    }

    debug_assert!(local.cached_candidate_positions[range_start..range_end]
        .iter()
        .all(|&primary_pos| primary_pos < index.entries.len()));
    true
}

fn range_len(start: usize, end: usize) -> usize {
    end.saturating_sub(start)
}

fn candidate_range_reserve(
    primary_start: usize,
    primary_end: usize,
    secondary_start: usize,
    secondary_end: usize,
) -> usize {
    range_len(primary_start, primary_end).min(range_len(secondary_start, secondary_end))
}

fn append_candidates_by_primary_rank_scan(
    index: &SortRangeJoinIndex,
    local: &mut SortRangeJoinProbeTransformLocal,
    primary_start: usize,
    primary_end: usize,
    secondary_start: usize,
    secondary_end: usize,
) {
    for primary_pos in primary_start..primary_end {
        let secondary_pos = index
            .secondary_positions_by_primary_pos
            .get(primary_pos)
            .copied()
            .unwrap_or(usize::MAX);
        if (secondary_start..secondary_end).contains(&secondary_pos) {
            local.candidate_positions.push(primary_pos);
        }
    }
}

fn append_candidates_by_secondary_range_scan(
    index: &SortRangeJoinIndex,
    local: &mut SortRangeJoinProbeTransformLocal,
    primary_start: usize,
    primary_end: usize,
    secondary_start: usize,
    secondary_end: usize,
) {
    for entry in &index.secondary_entries[secondary_start..secondary_end] {
        if (primary_start..primary_end).contains(&entry.primary_pos) {
            local.candidate_positions.push(entry.primary_pos);
        }
    }
}

fn append_cached_candidates_by_primary_scan(
    local: &mut SortRangeJoinProbeTransformLocal,
    offsets: SortRangeProbeOffsets,
) -> bool {
    for primary_pos in offsets.primary_start..offsets.primary_end {
        if bitmap_contains(&local.secondary_candidate_bitmap, primary_pos) {
            if local.cached_candidate_positions.len() >= SORT_RANGE_CANDIDATE_CACHE_LIMIT {
                return false;
            }
            local.cached_candidate_positions.push(primary_pos);
        }
    }
    true
}

fn append_cached_candidates_by_secondary_scan(
    index: &SortRangeJoinIndex,
    local: &mut SortRangeJoinProbeTransformLocal,
    offsets: SortRangeProbeOffsets,
) -> bool {
    {
        let bitmap = &mut local.primary_candidate_bitmap;
        let touched_words = &mut local.primary_candidate_touched_words;
        for primary_pos in offsets.primary_start..offsets.primary_end {
            bitmap_set(bitmap, touched_words, primary_pos);
        }
    }
    for entry in &index.secondary_entries[offsets.secondary_start..offsets.secondary_end] {
        if bitmap_contains(&local.primary_candidate_bitmap, entry.primary_pos) {
            if local.cached_candidate_positions.len() >= SORT_RANGE_CANDIDATE_CACHE_LIMIT {
                return false;
            }
            local.cached_candidate_positions.push(entry.primary_pos);
        }
    }
    true
}

fn range_for_comparison<T: SortRangeKey>(
    entries: &[T],
    left: &SortRangeLookupKey,
    comparison: JoinComparisonType,
    key_kind: Option<SortRangeKeyKind>,
) -> Result<(usize, usize)> {
    match comparison {
        JoinComparisonType::LessThan => Ok((upper_bound(entries, left, key_kind)?, entries.len())),
        JoinComparisonType::LessThanOrEqual => {
            Ok((lower_bound(entries, left, key_kind)?, entries.len()))
        }
        JoinComparisonType::GreaterThan => Ok((0, lower_bound(entries, left, key_kind)?)),
        JoinComparisonType::GreaterThanOrEqual => Ok((0, upper_bound(entries, left, key_kind)?)),
        _ => Ok((0, entries.len())),
    }
}

fn lower_bound<T: SortRangeKey>(
    entries: &[T],
    key: &SortRangeLookupKey,
    key_kind: Option<SortRangeKeyKind>,
) -> Result<usize> {
    let mut start = 0usize;
    let mut end = entries.len();
    while start < end {
        let mid = start + (end - start) / 2;
        if compare_entry_with_lookup_key(&entries[mid], key, key_kind)?.is_lt() {
            start = mid + 1;
        } else {
            end = mid;
        }
    }
    Ok(start)
}

fn upper_bound<T: SortRangeKey>(
    entries: &[T],
    key: &SortRangeLookupKey,
    key_kind: Option<SortRangeKeyKind>,
) -> Result<usize> {
    let mut start = 0usize;
    let mut end = entries.len();
    while start < end {
        let mid = start + (end - start) / 2;
        if matches!(
            compare_entry_with_lookup_key(&entries[mid], key, key_kind)?,
            Ordering::Less | Ordering::Equal
        ) {
            start = mid + 1;
        } else {
            end = mid;
        }
    }
    Ok(start)
}

fn sort_range_entries<T: SortRangeKey>(
    entries: &mut [T],
    key_kind: Option<SortRangeKeyKind>,
) -> Result<()> {
    let mut incomparable: Option<(Value, Value)> = None;
    entries.sort_unstable_by(|left, right| {
        compare_entries_for_sort(left, right, key_kind).unwrap_or_else(|| {
            if incomparable.is_none() {
                incomparable = Some((left.sort_key().clone(), right.sort_key().clone()));
            }
            Ordering::Equal
        })
    });
    if let Some((left, right)) = incomparable {
        return Err(incomparable_sort_range_join_key(&left, &right));
    }
    Ok(())
}

fn compare_entries_for_sort<T: SortRangeKey>(
    left: &T,
    right: &T,
    key_kind: Option<SortRangeKeyKind>,
) -> Option<Ordering> {
    if key_kind.is_some() {
        return left
            .typed_sort_key()?
            .cmp_same_kind(right.typed_sort_key()?);
    }
    left.sort_key().partial_cmp(right.sort_key())
}

fn compare_entry_with_lookup_key<T: SortRangeKey>(
    entry: &T,
    key: &SortRangeLookupKey,
    key_kind: Option<SortRangeKeyKind>,
) -> Result<Ordering> {
    if key_kind.is_some() {
        let SortRangeLookupKey::Typed(typed_key) = key else {
            return Err(paro_error::internal(
                "sort-range join typed lookup key missing",
            ));
        };
        return entry
            .typed_sort_key()
            .and_then(|entry_key| entry_key.cmp_same_kind(*typed_key))
            .ok_or_else(|| {
                paro_error::invalid_input(
                    "sort-range join typed range key values are not comparable",
                )
            });
    }
    let SortRangeLookupKey::Value(value) = key else {
        return Err(paro_error::internal(
            "sort-range join generic lookup key missing",
        ));
    };
    compare_values_for_sort(entry.sort_key(), value)
}

fn compare_values_for_sort(left: &Value, right: &Value) -> Result<Ordering> {
    left.partial_cmp(right)
        .ok_or_else(|| incomparable_sort_range_join_key(left, right))
}

fn incomparable_sort_range_join_key(left: &Value, right: &Value) -> paro_common::error::ParoError {
    paro_error::invalid_input(format!(
        "sort-range join range key values are not comparable: left={left:?}, right={right:?}"
    ))
}

fn copy_projected_row(
    input: &Chunk,
    input_row: usize,
    projection: &[usize],
    output: &mut Chunk,
    output_row: usize,
    output_offset: usize,
) -> Result<()> {
    for (out_col, &in_col) in projection.iter().enumerate() {
        let source = input.column(in_col).ok_or_else(|| {
            paro_error::internal(format!(
                "sort-range join projection source column out of bounds: {in_col}"
            ))
        })?;
        let target_col = output_offset + out_col;
        let target = output.column_mut(target_col).ok_or_else(|| {
            paro_error::internal(format!(
                "sort-range join projection target column out of bounds: {target_col}"
            ))
        })?;
        target.try_copy_at(output_row, source, input_row)?;
    }
    Ok(())
}

fn emit_projected_left(input: &Chunk, left_projection: &[usize], output: &mut Chunk) -> Result<()> {
    for row in 0..input.size() {
        copy_projected_row(input, row, left_projection, output, row, 0)?;
    }
    output.try_set_cardinality(input.size())?;
    Ok(())
}

fn emit_left_with_null_right(
    input: &Chunk,
    left_projection: &[usize],
    right_output_types: &[LogicalType],
    output: &mut Chunk,
) -> Result<()> {
    let right_offset = left_projection.len();
    for row in 0..input.size() {
        copy_projected_row(input, row, left_projection, output, row, 0)?;
        for (out_col, typ) in right_output_types.iter().enumerate() {
            let target = output.column_mut(right_offset + out_col).unwrap();
            target.set_value(row, &Value::Null(typ.clone()));
            target.try_set_null(row, true)?;
        }
    }
    output.try_set_cardinality(input.size())?;
    Ok(())
}

fn emit_mark_all(
    input: &Chunk,
    left_projection: &[usize],
    marker: Option<bool>,
    output: &mut Chunk,
) -> Result<()> {
    let mark_col_idx = left_projection.len();
    for row in 0..input.size() {
        copy_projected_row(input, row, left_projection, output, row, 0)?;
        let mark_col = output.column_mut(mark_col_idx).unwrap();
        match marker {
            Some(value) => {
                mark_col.set_value(row, &Value::Boolean(value));
                mark_col.try_set_null(row, false)?;
            }
            None => {
                mark_col.set_value(row, &Value::Boolean(false));
                mark_col.try_set_null(row, true)?;
            }
        }
    }
    output.try_set_cardinality(input.size())?;
    Ok(())
}

fn evaluate_classic_ie_join_side_keys(
    ctx: &OperatorCallContext,
    chunks: &[Chunk],
    conditions: &[JoinCondition],
    left_side: bool,
) -> Result<ClassicIeJoinSideKeys> {
    let exec_ctx = ExecutionContext::new(ctx.query.session.clone(), ctx.thread, None);
    let mut executors = conditions
        .iter()
        .map(|condition| {
            let expression = if left_side {
                &condition.left
            } else {
                &condition.right
            };
            ExpressionExecutor::with_expressions_for_session(
                std::slice::from_ref(expression),
                ctx.query.session.as_ref(),
            )
        })
        .collect::<Vec<_>>();
    let mut vectors = (0..conditions.len())
        .map(|_| Vec::with_capacity(chunks.len()))
        .collect::<Vec<_>>();
    let mut rows = Vec::new();
    let mut global_idx = 0usize;
    for (chunk_idx, chunk) in chunks.iter().enumerate() {
        for (condition_idx, executor) in executors.iter_mut().enumerate() {
            vectors[condition_idx].push(executor.execute_expression(
                0,
                chunk,
                None,
                chunk.size(),
                &exec_ctx,
            )?);
        }
        for row_idx in 0..chunk.size() {
            rows.push(BuildRowRef {
                chunk_idx,
                row_idx,
                global_idx,
            });
            global_idx += 1;
        }
    }
    Ok(ClassicIeJoinSideKeys { vectors, rows })
}

fn build_classic_ie_join_cursor(
    exec: &ClassicIeJoinSourceExec,
    left_chunks: Arc<[Chunk]>,
    right_chunks: Arc<[Chunk]>,
    left: ClassicIeJoinSideKeys,
    right: ClassicIeJoinSideKeys,
) -> Result<ClassicIeJoinCursor> {
    let primary_kind = sort_range_key_kind_for_condition(&exec.conditions[0]).ok_or_else(|| {
        paro_error::internal("classic IE join primary condition is not a typed ordered range key")
    })?;
    let secondary_kind =
        sort_range_key_kind_for_condition(&exec.conditions[1]).ok_or_else(|| {
            paro_error::internal(
                "classic IE join secondary condition is not a typed ordered range key",
            )
        })?;

    let left_rows = classic_ie_join_left_rows(&left, primary_kind, secondary_kind);
    let mut right_entries = classic_ie_join_right_entries(&right, primary_kind, secondary_kind);
    sort_range_entries(&mut right_entries, Some(primary_kind))?;
    let secondary_entries = build_secondary_entries(&right_entries, Some(secondary_kind))?;

    let mut offsets = vec![SortRangeProbeOffsets::default(); left.rows.len()];
    fill_classic_ie_join_offsets(
        &right_entries,
        &left_rows,
        &mut offsets,
        ClassicIeJoinOffsetSpec {
            dimension: ProbeOffsetDimension::Primary,
            comparison: exec.conditions[0].comparison,
            key_kind: primary_kind,
        },
    )?;
    fill_classic_ie_join_offsets(
        &secondary_entries,
        &left_rows,
        &mut offsets,
        ClassicIeJoinOffsetSpec {
            dimension: ProbeOffsetDimension::Secondary,
            comparison: exec.conditions[1].comparison,
            key_kind: secondary_kind,
        },
    )?;

    let match_counts = vec![0usize; left.rows.len()];
    let word_count = (right_entries.len() + 63) / 64;
    Ok(ClassicIeJoinCursor {
        output: ClassicIeJoinOutput {
            left_chunks,
            right_chunks,
        },
        left,
        right,
        left_rows,
        right_entries,
        secondary_entries,
        offsets,
        match_counts,
        candidate_bitmap: vec![0u64; word_count],
        touched_words: Vec::new(),
        phase: ClassicIeJoinEmitPhase::Matches,
        left_cursor: 0,
        unmatched_cursor: 0,
        current: None,
    })
}

fn classic_ie_join_left_rows(
    side: &ClassicIeJoinSideKeys,
    primary_kind: SortRangeKeyKind,
    secondary_kind: SortRangeKeyKind,
) -> Vec<ClassicIeJoinLeftRow> {
    let mut rows = Vec::new();
    let Some(primary_vectors) = side.vectors.first() else {
        return rows;
    };
    let Some(secondary_vectors) = side.vectors.get(1) else {
        return rows;
    };
    for row in &side.rows {
        let Some(primary) = sort_range_key_value_from_vector(
            &primary_vectors[row.chunk_idx],
            row.row_idx,
            primary_kind,
        ) else {
            continue;
        };
        let Some(secondary) = sort_range_key_value_from_vector(
            &secondary_vectors[row.chunk_idx],
            row.row_idx,
            secondary_kind,
        ) else {
            continue;
        };
        rows.push(ClassicIeJoinLeftRow {
            row: *row,
            primary_key: primary,
            secondary_key: secondary,
        });
    }
    rows
}

fn classic_ie_join_right_entries(
    side: &ClassicIeJoinSideKeys,
    primary_kind: SortRangeKeyKind,
    secondary_kind: SortRangeKeyKind,
) -> Vec<SortRangeJoinEntry> {
    let mut entries = Vec::new();
    let Some(primary_vectors) = side.vectors.first() else {
        return entries;
    };
    let Some(secondary_vectors) = side.vectors.get(1) else {
        return entries;
    };
    for row in &side.rows {
        let Some(primary) = sort_range_key_value_from_vector(
            &primary_vectors[row.chunk_idx],
            row.row_idx,
            primary_kind,
        ) else {
            continue;
        };
        let Some(secondary) = sort_range_key_value_from_vector(
            &secondary_vectors[row.chunk_idx],
            row.row_idx,
            secondary_kind,
        ) else {
            continue;
        };
        entries.push(SortRangeJoinEntry {
            key: lookup_debug_value(&SortRangeLookupKey::Typed(primary)),
            typed_key: Some(primary),
            secondary_key: Some(lookup_debug_value(&SortRangeLookupKey::Typed(secondary))),
            secondary_typed_key: Some(secondary),
            row: *row,
        });
    }
    entries
}

#[derive(Debug, Clone, Copy)]
struct ClassicIeJoinOffsetSpec {
    dimension: ProbeOffsetDimension,
    comparison: JoinComparisonType,
    key_kind: SortRangeKeyKind,
}

fn fill_classic_ie_join_offsets<T: SortRangeKey>(
    right_entries: &[T],
    left_rows: &[ClassicIeJoinLeftRow],
    offsets: &mut [SortRangeProbeOffsets],
    spec: ClassicIeJoinOffsetSpec,
) -> Result<()> {
    let mut probe_keys = left_rows
        .iter()
        .map(|row| SortRangeProbeKey {
            row_idx: row.row.global_idx,
            lookup: SortRangeLookupKey::Typed(match spec.dimension {
                ProbeOffsetDimension::Primary => row.primary_key,
                ProbeOffsetDimension::Secondary => row.secondary_key,
            }),
        })
        .collect::<Vec<_>>();
    sort_probe_keys(&mut probe_keys, Some(spec.key_kind))?;

    let mut lower_cursor = 0usize;
    let mut upper_cursor = 0usize;
    for probe_key in &probe_keys {
        while lower_cursor < right_entries.len()
            && compare_entry_with_lookup_key(
                &right_entries[lower_cursor],
                &probe_key.lookup,
                Some(spec.key_kind),
            )?
            .is_lt()
        {
            lower_cursor += 1;
        }
        while upper_cursor < right_entries.len()
            && matches!(
                compare_entry_with_lookup_key(
                    &right_entries[upper_cursor],
                    &probe_key.lookup,
                    Some(spec.key_kind)
                )?,
                Ordering::Less | Ordering::Equal
            )
        {
            upper_cursor += 1;
        }

        let (start, end) = range_from_sweep_bounds(
            right_entries.len(),
            lower_cursor,
            upper_cursor,
            spec.comparison,
        );
        let offset = &mut offsets[probe_key.row_idx];
        match spec.dimension {
            ProbeOffsetDimension::Primary => {
                offset.primary_start = start;
                offset.primary_end = end;
                offset.valid = true;
            }
            ProbeOffsetDimension::Secondary => {
                offset.secondary_start = start;
                offset.secondary_end = end;
            }
        }
    }
    Ok(())
}

impl ClassicIeJoinCursor {
    fn next_output_row(
        &mut self,
        exec: &ClassicIeJoinSourceExec,
    ) -> Result<Option<ClassicIeJoinOutputRow>> {
        loop {
            match self.phase {
                ClassicIeJoinEmitPhase::Matches => {
                    if let Some(row) = self.next_match_row(exec)? {
                        return Ok(Some(row));
                    }
                    self.phase = ClassicIeJoinEmitPhase::Unmatched;
                }
                ClassicIeJoinEmitPhase::Unmatched => {
                    if let Some(row) = self.next_unmatched_row(exec)? {
                        return Ok(Some(row));
                    }
                    self.phase = ClassicIeJoinEmitPhase::Done;
                }
                ClassicIeJoinEmitPhase::Done => return Ok(None),
            }
        }
    }

    fn next_match_row(
        &mut self,
        exec: &ClassicIeJoinSourceExec,
    ) -> Result<Option<ClassicIeJoinOutputRow>> {
        loop {
            if self.current.is_none() && !self.prepare_next_left_row() {
                return Ok(None);
            }
            let next_candidate = {
                let Some(current) = self.current.as_mut() else {
                    continue;
                };
                current
                    .next_candidate(
                        &self.right_entries,
                        &self.secondary_entries,
                        &self.candidate_bitmap,
                    )
                    .map(|right_row| (current.left_row.row, right_row))
            };
            let Some((left_row, right_row)) = next_candidate else {
                self.current = None;
                self.clear_candidate_bitmap();
                continue;
            };
            let Some(output) = self.emit_match(exec, left_row, right_row)? else {
                continue;
            };
            if matches!(exec.join_type, JoinType::Semi) {
                self.current = None;
                self.clear_candidate_bitmap();
            }
            return Ok(Some(output));
        }
    }

    fn prepare_next_left_row(&mut self) -> bool {
        while self.left_cursor < self.left_rows.len() {
            let left_row = self.left_rows[self.left_cursor];
            self.left_cursor += 1;
            let Some(offset) = self.offsets.get(left_row.row.global_idx).copied() else {
                continue;
            };
            if !offset.valid
                || offset.primary_start >= offset.primary_end
                || offset.secondary_start >= offset.secondary_end
            {
                continue;
            }
            self.clear_candidate_bitmap();
            let mode = if range_len(offset.primary_start, offset.primary_end)
                <= range_len(offset.secondary_start, offset.secondary_end)
            {
                for primary_pos in offset.primary_start..offset.primary_end {
                    bitmap_set(
                        &mut self.candidate_bitmap,
                        &mut self.touched_words,
                        primary_pos,
                    );
                }
                ClassicIeJoinScanMode::Secondary {
                    pos: offset.secondary_start,
                    end: offset.secondary_end,
                }
            } else {
                for secondary in
                    &self.secondary_entries[offset.secondary_start..offset.secondary_end]
                {
                    bitmap_set(
                        &mut self.candidate_bitmap,
                        &mut self.touched_words,
                        secondary.primary_pos,
                    );
                }
                ClassicIeJoinScanMode::Primary {
                    pos: offset.primary_start,
                    end: offset.primary_end,
                }
            };
            self.current = Some(ClassicIeJoinCurrentScan { left_row, mode });
            return true;
        }
        false
    }

    fn emit_match(
        &mut self,
        exec: &ClassicIeJoinSourceExec,
        left_row: BuildRowRef,
        right_row: BuildRowRef,
    ) -> Result<Option<ClassicIeJoinOutputRow>> {
        if !classic_ie_join_candidate_matches(
            &exec.conditions,
            &self.left,
            &self.right,
            left_row,
            right_row,
        ) {
            return Ok(None);
        }
        let count = &mut self.match_counts[left_row.global_idx];
        *count += 1;
        match exec.join_type {
            JoinType::Inner | JoinType::Left => Ok(Some(ClassicIeJoinOutputRow {
                left: left_row,
                right: Some(right_row),
            })),
            JoinType::Semi => Ok(Some(ClassicIeJoinOutputRow {
                left: left_row,
                right: None,
            })),
            JoinType::Anti => Ok(None),
            JoinType::Single => {
                if *count > 1 {
                    return Err(paro_error::internal(
                        "scalar subquery produced more than one row",
                    ));
                }
                Ok(Some(ClassicIeJoinOutputRow {
                    left: left_row,
                    right: Some(right_row),
                }))
            }
            _ => Err(paro_error::internal(format!(
                "classic IE join source does not support {} joins",
                exec.join_type
            ))),
        }
    }

    fn next_unmatched_row(
        &mut self,
        exec: &ClassicIeJoinSourceExec,
    ) -> Result<Option<ClassicIeJoinOutputRow>> {
        if !matches!(
            exec.join_type,
            JoinType::Left | JoinType::Single | JoinType::Anti
        ) {
            return Ok(None);
        }
        while self.unmatched_cursor < self.left.rows.len() {
            let left_row = self.left.rows[self.unmatched_cursor];
            self.unmatched_cursor += 1;
            if self.match_counts[left_row.global_idx] == 0 {
                return Ok(Some(ClassicIeJoinOutputRow {
                    left: left_row,
                    right: None,
                }));
            }
        }
        Ok(None)
    }

    fn clear_candidate_bitmap(&mut self) {
        let word_count = self.candidate_bitmap.len();
        clear_candidate_bitmap(
            &mut self.candidate_bitmap,
            &mut self.touched_words,
            word_count,
        );
    }
}

impl ClassicIeJoinCurrentScan {
    fn next_candidate(
        &mut self,
        right_entries: &[SortRangeJoinEntry],
        secondary_entries: &[SortRangeJoinSecondaryEntry],
        candidate_bitmap: &[u64],
    ) -> Option<BuildRowRef> {
        match &mut self.mode {
            ClassicIeJoinScanMode::Primary { pos, end } => {
                while *pos < *end {
                    let primary_pos = *pos;
                    *pos += 1;
                    if bitmap_contains(candidate_bitmap, primary_pos) {
                        return Some(right_entries[primary_pos].row);
                    }
                }
                None
            }
            ClassicIeJoinScanMode::Secondary { pos, end } => {
                while *pos < *end {
                    let secondary = &secondary_entries[*pos];
                    *pos += 1;
                    if bitmap_contains(candidate_bitmap, secondary.primary_pos) {
                        return Some(right_entries[secondary.primary_pos].row);
                    }
                }
                None
            }
        }
    }
}

fn classic_ie_join_candidate_matches(
    conditions: &[JoinCondition],
    left: &ClassicIeJoinSideKeys,
    right: &ClassicIeJoinSideKeys,
    left_row: BuildRowRef,
    right_row: BuildRowRef,
) -> bool {
    for (condition_idx, condition) in conditions.iter().enumerate() {
        let Some(left_vectors) = left.vectors.get(condition_idx) else {
            return false;
        };
        let Some(right_vectors) = right.vectors.get(condition_idx) else {
            return false;
        };
        let left_value = vector_value(&left_vectors[left_row.chunk_idx], left_row.row_idx);
        let right_value = vector_value(&right_vectors[right_row.chunk_idx], right_row.row_idx);
        if compare_with_nulls(condition.comparison, &left_value, &right_value) != Some(true) {
            return false;
        }
    }
    true
}

fn copy_classic_ie_join_output_row(
    joined: &ClassicIeJoinOutput,
    row_ref: ClassicIeJoinOutputRow,
    left_projection: &[usize],
    right_projection: &[usize],
    right_output_types: &[LogicalType],
    output: &mut Chunk,
    output_row: usize,
) -> Result<()> {
    copy_projected_row(
        &joined.left_chunks[row_ref.left.chunk_idx],
        row_ref.left.row_idx,
        left_projection,
        output,
        output_row,
        0,
    )?;
    let right_offset = left_projection.len();
    if let Some(right) = row_ref.right {
        copy_projected_row(
            &joined.right_chunks[right.chunk_idx],
            right.row_idx,
            right_projection,
            output,
            output_row,
            right_offset,
        )?;
    } else {
        for out_col in 0..right_output_types.len() {
            let target = output.column_mut(right_offset + out_col).ok_or_else(|| {
                paro_error::internal("classic IE join null-right target column out of bounds")
            })?;
            target.try_set_null(output_row, true)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_planner::expression::{Expression, ReferenceExpression};

    fn entry(primary: i32, secondary: Option<i32>, global_idx: usize) -> SortRangeJoinEntry {
        SortRangeJoinEntry {
            key: Value::Integer(primary),
            typed_key: None,
            secondary_key: secondary.map(Value::Integer),
            secondary_typed_key: None,
            row: BuildRowRef {
                chunk_idx: 0,
                row_idx: global_idx,
                global_idx,
            },
        }
    }

    fn typed_entry(primary: i32, secondary: Option<i32>, global_idx: usize) -> SortRangeJoinEntry {
        SortRangeJoinEntry {
            key: Value::Varchar(format!("fallback-{primary}")),
            typed_key: Some(SortRangeKeyValue::Signed(primary as i128)),
            secondary_key: secondary.map(|value| Value::Varchar(format!("fallback-{value}"))),
            secondary_typed_key: secondary.map(|value| SortRangeKeyValue::Signed(value as i128)),
            row: BuildRowRef {
                chunk_idx: 0,
                row_idx: global_idx,
                global_idx,
            },
        }
    }

    #[test]
    fn secondary_candidate_intersection_scans_narrower_range() {
        let index = index_with_secondary_keys();

        let (primary_start, primary_end) = range_for_comparison(
            &index.entries,
            &SortRangeLookupKey::Value(Value::Integer(4)),
            JoinComparisonType::LessThan,
            index.primary_key_kind,
        )
        .expect("primary range");
        let mut local = SortRangeJoinProbeTransformLocal::default();
        prepare_secondary_candidates(
            &index,
            &mut local,
            primary_start,
            primary_end,
            SortRangeLookupKey::Value(Value::Integer(5)),
            JoinComparisonType::GreaterThan,
        )
        .expect("secondary candidates");

        assert_eq!(
            local.candidate_source,
            SortRangeCandidateSource::SparsePositions
        );
        let matched_global_rows = local
            .candidate_positions
            .iter()
            .map(|&pos| index.entries[pos].row.global_idx)
            .collect::<Vec<_>>();
        assert_eq!(matched_global_rows, vec![0, 1, 2]);

        prepare_secondary_candidates(
            &index,
            &mut local,
            primary_start,
            primary_end,
            SortRangeLookupKey::Value(Value::Integer(4)),
            JoinComparisonType::GreaterThan,
        )
        .expect("secondary candidates after bitmap reuse");
        let matched_global_rows = local
            .candidate_positions
            .iter()
            .map(|&pos| index.entries[pos].row.global_idx)
            .collect::<Vec<_>>();
        assert_eq!(matched_global_rows, vec![0, 1]);
    }

    #[test]
    fn typed_range_comparator_drives_primary_and_secondary_offsets() {
        let mut entries = vec![
            typed_entry(5, Some(0), 0),
            typed_entry(7, Some(3), 1),
            typed_entry(10, Some(4), 2),
            typed_entry(2, Some(9), 3),
            typed_entry(8, None, 4),
        ];
        let key_kind = Some(SortRangeKeyKind::Signed);
        sort_range_entries(&mut entries, key_kind).expect("sort typed primary entries");
        let secondary_entries =
            build_secondary_entries(&entries, key_kind).expect("sort typed secondary entries");
        let secondary_positions_by_primary_pos =
            build_secondary_positions_by_primary_pos(entries.len(), &secondary_entries);
        let index = SortRangeJoinIndex {
            entries,
            secondary_entries,
            secondary_positions_by_primary_pos,
            right_vectors: Vec::new(),
            primary_key_kind: key_kind,
            secondary_key_kind: key_kind,
        };

        let (primary_start, primary_end) = range_for_comparison(
            &index.entries,
            &SortRangeLookupKey::Typed(SortRangeKeyValue::Signed(4)),
            JoinComparisonType::LessThan,
            index.primary_key_kind,
        )
        .expect("typed primary range");
        let mut local = SortRangeJoinProbeTransformLocal::default();
        prepare_secondary_candidates(
            &index,
            &mut local,
            primary_start,
            primary_end,
            SortRangeLookupKey::Typed(SortRangeKeyValue::Signed(5)),
            JoinComparisonType::GreaterThan,
        )
        .expect("typed secondary candidates");

        let matched_global_rows = local
            .candidate_positions
            .iter()
            .map(|&pos| index.entries[pos].row.global_idx)
            .collect::<Vec<_>>();
        assert_eq!(matched_global_rows, vec![0, 1, 2]);
    }

    #[test]
    fn probe_offsets_are_generated_by_sorted_probe_sweep() {
        let index = index_with_secondary_keys();
        let mut local = SortRangeJoinProbeTransformLocal {
            left_condition_results: vec![
                Arc::new(paro_common::test_utils::test_i32_vector(&[4, 8, 1])),
                Arc::new(paro_common::test_utils::test_i32_vector(&[5, 2, 8])),
            ],
            ..Default::default()
        };
        local
            .probe_offsets
            .resize(3, SortRangeProbeOffsets::default());

        fill_probe_offsets_by_sweeping_probe_permutation(
            &index.entries,
            &mut local,
            3,
            ProbeOffsetSweepSpec {
                dimension: ProbeOffsetDimension::Primary,
                condition_idx: 0,
                comparison: JoinComparisonType::LessThan,
                key_kind: index.primary_key_kind,
                null_is_unknown: true,
            },
        )
        .expect("primary probe offset sweep");
        fill_probe_offsets_by_sweeping_probe_permutation(
            &index.secondary_entries,
            &mut local,
            3,
            ProbeOffsetSweepSpec {
                dimension: ProbeOffsetDimension::Secondary,
                condition_idx: 1,
                comparison: JoinComparisonType::GreaterThan,
                key_kind: index.secondary_key_kind,
                null_is_unknown: true,
            },
        )
        .expect("secondary probe offset sweep");

        assert_eq!(
            (
                local.probe_offsets[0].primary_start,
                local.probe_offsets[0].primary_end
            ),
            (1, 5)
        );
        assert_eq!(
            (
                local.probe_offsets[0].secondary_start,
                local.probe_offsets[0].secondary_end
            ),
            (0, 3)
        );
        assert_eq!(
            (
                local.probe_offsets[1].primary_start,
                local.probe_offsets[1].primary_end
            ),
            (4, 5)
        );
        assert_eq!(
            (
                local.probe_offsets[1].secondary_start,
                local.probe_offsets[1].secondary_end
            ),
            (0, 1)
        );
        assert_eq!(
            (
                local.probe_offsets[2].primary_start,
                local.probe_offsets[2].primary_end
            ),
            (0, 5)
        );
        assert_eq!(
            (
                local.probe_offsets[2].secondary_start,
                local.probe_offsets[2].secondary_end
            ),
            (0, 3)
        );
        assert!(local.probe_offsets.iter().all(|offsets| offsets.valid));
    }

    #[test]
    fn incremental_secondary_candidate_cache_uses_prefix_activation() {
        let index = index_with_secondary_keys();
        let mut local = SortRangeJoinProbeTransformLocal {
            probe_offsets: vec![
                SortRangeProbeOffsets {
                    primary_start: 1,
                    primary_end: 5,
                    secondary_start: 0,
                    secondary_end: 2,
                    valid: true,
                    saw_null: false,
                },
                SortRangeProbeOffsets {
                    primary_start: 1,
                    primary_end: 5,
                    secondary_start: 0,
                    secondary_end: 3,
                    valid: true,
                    saw_null: false,
                },
            ],
            ..Default::default()
        };

        prepare_incremental_secondary_candidate_cache(
            &index,
            &mut local,
            JoinComparisonType::GreaterThan,
        );

        assert!(local.cached_candidates_ready);
        assert_eq!(cached_global_rows(&index, &local, 0), vec![0, 1]);
        assert_eq!(cached_global_rows(&index, &local, 1), vec![0, 1, 2]);
    }

    #[test]
    fn incremental_secondary_candidate_cache_uses_suffix_activation() {
        let index = index_with_secondary_keys();
        let mut local = SortRangeJoinProbeTransformLocal {
            probe_offsets: vec![
                SortRangeProbeOffsets {
                    primary_start: 0,
                    primary_end: 5,
                    secondary_start: 2,
                    secondary_end: 4,
                    valid: true,
                    saw_null: false,
                },
                SortRangeProbeOffsets {
                    primary_start: 0,
                    primary_end: 5,
                    secondary_start: 1,
                    secondary_end: 4,
                    valid: true,
                    saw_null: false,
                },
            ],
            ..Default::default()
        };

        prepare_incremental_secondary_candidate_cache(
            &index,
            &mut local,
            JoinComparisonType::LessThan,
        );

        assert!(local.cached_candidates_ready);
        assert_eq!(cached_global_rows(&index, &local, 0), vec![2, 3]);
        assert_eq!(cached_global_rows(&index, &local, 1), vec![1, 2, 3]);
    }

    fn index_with_secondary_keys() -> SortRangeJoinIndex {
        let mut entries = vec![
            entry(5, Some(0), 0),
            entry(7, Some(3), 1),
            entry(10, Some(4), 2),
            entry(2, Some(9), 3),
            entry(8, None, 4),
        ];
        sort_range_entries(&mut entries, None).expect("sort primary entries");
        let secondary_entries =
            build_secondary_entries(&entries, None).expect("sort secondary entries");
        let secondary_positions_by_primary_pos =
            build_secondary_positions_by_primary_pos(entries.len(), &secondary_entries);
        SortRangeJoinIndex {
            entries,
            secondary_entries,
            secondary_positions_by_primary_pos,
            right_vectors: Vec::new(),
            primary_key_kind: None,
            secondary_key_kind: None,
        }
    }

    fn cached_global_rows(
        index: &SortRangeJoinIndex,
        local: &SortRangeJoinProbeTransformLocal,
        row_idx: usize,
    ) -> Vec<usize> {
        let range = local.cached_candidate_ranges[row_idx];
        local.cached_candidate_positions[range.start..range.end]
            .iter()
            .map(|&pos| index.entries[pos].row.global_idx)
            .collect()
    }

    fn build_classic_ie_join_rows(
        exec: &ClassicIeJoinSourceExec,
        left: &ClassicIeJoinSideKeys,
        right: &ClassicIeJoinSideKeys,
    ) -> Result<Vec<ClassicIeJoinOutputRow>> {
        let mut cursor = build_classic_ie_join_cursor(
            exec,
            Arc::from(Vec::<Chunk>::new().into_boxed_slice()),
            Arc::from(Vec::<Chunk>::new().into_boxed_slice()),
            left.clone(),
            right.clone(),
        )?;
        let mut rows = Vec::new();
        while let Some(row) = cursor.next_output_row(exec)? {
            rows.push(row);
        }
        Ok(rows)
    }

    #[test]
    fn classic_ie_join_uses_global_offsets_and_bitmap_intersection() {
        let left = classic_side(&[(1, 10), (4, 7), (8, 2)]);
        let right = classic_side(&[(2, 9), (5, 6), (9, 1)]);
        let exec = classic_exec(JoinType::Inner);

        let rows = build_classic_ie_join_rows(&exec, &left, &right).expect("classic IE rows");
        let mut pairs = rows
            .iter()
            .map(|row| {
                (
                    row.left.global_idx,
                    row.right.expect("inner join right row").global_idx,
                )
            })
            .collect::<Vec<_>>();
        pairs.sort_unstable();

        assert_eq!(pairs, vec![(0, 0), (0, 1), (0, 2), (1, 1), (1, 2), (2, 2)]);
    }

    #[test]
    fn classic_ie_join_left_variants_emit_unmatched_rows() {
        let left = classic_side(&[(1, 10), (20, 0)]);
        let right = classic_side(&[(2, 9), (5, 6)]);
        let exec = classic_exec(JoinType::Left);

        let rows = build_classic_ie_join_rows(&exec, &left, &right).expect("classic IE left rows");
        let unmatched = rows
            .iter()
            .filter(|row| row.left.global_idx == 1 && row.right.is_none())
            .count();

        assert_eq!(unmatched, 1);
        assert_eq!(rows.len(), 3);
    }

    fn classic_side(keys: &[(i32, i32)]) -> ClassicIeJoinSideKeys {
        let primary = keys.iter().map(|(value, _)| *value).collect::<Vec<_>>();
        let secondary = keys.iter().map(|(_, value)| *value).collect::<Vec<_>>();
        ClassicIeJoinSideKeys {
            vectors: vec![
                vec![Arc::new(paro_common::test_utils::test_i32_vector(&primary))],
                vec![Arc::new(paro_common::test_utils::test_i32_vector(
                    &secondary,
                ))],
            ],
            rows: (0..keys.len())
                .map(|idx| BuildRowRef {
                    chunk_idx: 0,
                    row_idx: idx,
                    global_idx: idx,
                })
                .collect(),
        }
    }

    fn classic_exec(join_type: JoinType) -> ClassicIeJoinSourceExec {
        ClassicIeJoinSourceExec {
            left_handle: HandleRef::new(crate::pipeline::handles::BreakerHandleId::new(0)),
            right_handle: HandleRef::new(crate::pipeline::handles::BreakerHandleId::new(1)),
            join_type,
            conditions: vec![
                classic_condition(0, 0, JoinComparisonType::LessThan),
                classic_condition(1, 1, JoinComparisonType::GreaterThan),
            ]
            .into_boxed_slice(),
            mark_null_condition_start: None,
            left_projection: vec![0, 1].into_boxed_slice(),
            right_projection: vec![0, 1].into_boxed_slice(),
            right_output_types: vec![LogicalType::Integer, LogicalType::Integer].into_boxed_slice(),
            output_types: vec![
                LogicalType::Integer,
                LogicalType::Integer,
                LogicalType::Integer,
                LogicalType::Integer,
            ]
            .into_boxed_slice(),
        }
    }

    fn classic_condition(
        left_idx: usize,
        right_idx: usize,
        comparison: JoinComparisonType,
    ) -> JoinCondition {
        JoinCondition::new(
            Expression::Reference(ReferenceExpression::new(left_idx, LogicalType::Integer)),
            Expression::Reference(ReferenceExpression::new(right_idx, LogicalType::Integer)),
            comparison,
        )
    }
}
