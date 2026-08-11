// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use paro_common::allocator::MemoryTag;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_common::vector::{SelectionVector, VECTOR_SIZE};
use paro_function::scalar::FunctionExecContext;
use paro_planner::operator::join::{AntiJoinMode, JoinCondition, JoinType};

use crate::expression_executor::executor::ExpressionExecutor;
use crate::join_hashtable::{FullOuterScanState, JoinHashTable, JoinHashTableConfig};
use crate::operators::join::hash::keys::{evaluate_join_keys_into, join_key_types, JoinKeySide};
use crate::operators::join::hash::memory::hash_join_memory_context;
use crate::operators::join::hash::probe_output::{
    emit_empty_build_probe_result, scan_hash_join_results,
};
use crate::operators::join::hash::residual::HashJoinResidualProbeState;
use crate::operators::join::hash::source_predicate::ReductionSourcePredicateState;
use crate::operators::join::hash::spill::probe_input_from_spill_chunk_into;
use crate::operators::join::join_result_helpers::{
    construct_right_outer_scan_result, construct_semi_join_result,
};
use crate::operators::output::ensure_source_output;
use crate::physical::specs::HashReductionCascadeSpec;
use crate::runtime::breaker::{HandleRef, JoinBuildHandle};
use crate::runtime::context::{OperatorCallContext, PipelineInitContext};
use crate::runtime::source::SourcePoll;
use crate::runtime::state::{
    BreakerHandleGlobal, HashJoinSpillReplayPartitionLocal, HashJoinSpillReplaySourceLocal,
    SourceGlobal, SourceLocal,
};

#[derive(Debug, Clone)]
pub struct HashJoinSpillReplaySourceExec {
    pub handle: HandleRef<JoinBuildHandle>,
    pub join_type: JoinType,
    pub anti_join_mode: AntiJoinMode,
    pub key_conditions: Box<[JoinCondition]>,
    pub residual_conditions: Box<[JoinCondition]>,
    pub probe_types: Box<[LogicalType]>,
    pub build_output_count: usize,
    pub build_payload_types: Box<[LogicalType]>,
    pub left_projection: Box<[usize]>,
    pub output_types: Box<[LogicalType]>,
    pub reduction_cascade: Option<HashReductionCascadeSpec>,
}

impl HashJoinSpillReplaySourceExec {
    pub(crate) fn create_global(&self, ctx: &mut PipelineInitContext) -> Result<SourceGlobal> {
        Ok(SourceGlobal::HashJoinSpillReplay(Arc::new(
            BreakerHandleGlobal {
                handle: ctx.handles.get(self.handle)?,
            },
        )))
    }

    pub(crate) fn create_local(
        &self,
        ctx: &mut PipelineInitContext,
        _global: &SourceGlobal,
    ) -> Result<SourceLocal> {
        Ok(SourceLocal::HashJoinSpillReplay(
            HashJoinSpillReplaySourceLocal {
                probe_key_types: join_key_types(&self.key_conditions, JoinKeySide::Probe),
                probe_key_executors: self
                    .key_conditions
                    .iter()
                    .map(|condition| {
                        ExpressionExecutor::with_expressions_for_session(
                            std::slice::from_ref(&condition.left),
                            ctx.query.session.as_ref(),
                        )
                    })
                    .collect::<Vec<_>>()
                    .into_boxed_slice(),
                residual: HashJoinResidualProbeState::new(
                    &self.residual_conditions,
                    ctx.query.session.as_ref(),
                ),
                reduction_residuals: self
                    .reduction_cascade
                    .as_ref()
                    .map(|cascade| {
                        cascade
                            .predicates
                            .iter()
                            .map(|predicate| {
                                HashJoinResidualProbeState::new_at_offset(
                                    std::slice::from_ref(&predicate.condition),
                                    predicate.build_residual_offset,
                                    ctx.query.session.as_ref(),
                                )
                            })
                            .collect::<Vec<_>>()
                            .into_boxed_slice()
                    })
                    .unwrap_or_default(),
                reduction_source_predicates: self
                    .reduction_cascade
                    .as_ref()
                    .map(|cascade| {
                        cascade
                            .source_predicates
                            .iter()
                            .map(|predicate| {
                                ReductionSourcePredicateState::new(
                                    &predicate.expression,
                                    predicate.predicate_mask,
                                    ctx.query.session.as_ref(),
                                )
                            })
                            .collect::<Vec<_>>()
                            .into_boxed_slice()
                    })
                    .unwrap_or_default(),
                reduction_source_masks: Vec::new(),
                reduction_selection: None,
                current: None,
            },
        ))
    }

    pub(crate) fn poll_next(
        &self,
        ctx: &mut OperatorCallContext,
        global: &SourceGlobal,
        local: &mut SourceLocal,
        output: &mut Chunk,
    ) -> Result<SourcePoll> {
        ctx.cancel.check()?;
        let SourceGlobal::HashJoinSpillReplay(global) = global else {
            return Err(paro_error::internal(
                "hash join spill replay source global state mismatch",
            ));
        };
        let SourceLocal::HashJoinSpillReplay(local) = local else {
            return Err(paro_error::internal(
                "hash join spill replay source local state mismatch",
            ));
        };
        ensure_source_output(output, &self.output_types, VECTOR_SIZE)?;
        if !global.handle.is_external() {
            return Ok(SourcePoll::Finished);
        }
        if self.anti_join_mode == AntiJoinMode::NullAware
            && global.handle.require_table()?.has_null_keys()
        {
            output.try_set_cardinality(0)?;
            return Ok(SourcePoll::Finished);
        }

        loop {
            if local.current.is_none() {
                local.current = self.prepare_next_partition(ctx, global.handle.as_ref())?;
                if local.current.is_none() {
                    output.try_set_cardinality(0)?;
                    return Ok(SourcePoll::Finished);
                }
            }

            let current = local
                .current
                .as_mut()
                .expect("hash join replay partition initialized");
            if current.probe_in_progress {
                let count = self.scan_current_probe(
                    ctx,
                    current,
                    local.residual.as_mut(),
                    &mut local.reduction_residuals,
                    &local.reduction_source_masks,
                    &mut local.reduction_selection,
                    output,
                )?;
                if count > 0 {
                    return Ok(SourcePoll::Output);
                }
                continue;
            }

            if current.probe_exhausted {
                let count = self.scan_current_unmatched(current, output)?;
                if count > 0 {
                    return Ok(SourcePoll::Output);
                }
                local.current = None;
                continue;
            }

            let Some(probe_cursor) = current.probe_cursor.as_mut() else {
                current.probe_exhausted = true;
                continue;
            };
            let scanned = probe_cursor.next_chunk(&mut current.probe_spill_chunk)?;
            if scanned == 0 {
                current.probe_exhausted = true;
                continue;
            }
            current.probe_spill_chunk.try_set_cardinality(scanned)?;
            probe_input_from_spill_chunk_into(
                &current.probe_spill_chunk,
                self.probe_types.len(),
                &mut current.probe_input,
            )?;
            let replay_input = current
                .probe_input
                .as_ref()
                .ok_or_else(|| paro_error::internal("hash join replay input chunk missing"))?;
            if current.hash_table.is_empty() && self.join_type != JoinType::Mark {
                let emitted = emit_empty_build_probe_result(
                    self.join_type,
                    &replay_input,
                    &self.left_projection,
                    &self.output_types,
                    output,
                )?;
                if emitted > 0 {
                    return Ok(SourcePoll::Output);
                }
                continue;
            }

            evaluate_join_keys_into(
                ctx,
                replay_input,
                &self.key_conditions,
                &mut local.probe_key_executors,
                &local.probe_key_types,
                JoinKeySide::Probe,
                &mut current.probe_keys,
            )?;
            if let Some(residual) = local.residual.as_mut() {
                residual.evaluate_probe(ctx, replay_input)?;
            }
            for residual in local.reduction_residuals.iter_mut().flatten() {
                residual.evaluate_probe(ctx, replay_input)?;
            }
            if local.reduction_selection.is_none() {
                local.reduction_selection = Some(SelectionVector::try_with_capacity(
                    VECTOR_SIZE,
                    output.allocator().clone(),
                )?);
            }
            let selection = local
                .reduction_selection
                .as_mut()
                .ok_or_else(|| paro_error::internal("hash reduction replay selection missing"))?;
            local.reduction_source_masks.resize(replay_input.size(), 0);
            local.reduction_source_masks.fill(0);
            for predicate in local.reduction_source_predicates.iter_mut() {
                predicate.evaluate_into(
                    replay_input,
                    ctx.query,
                    selection,
                    &mut local.reduction_source_masks,
                )?;
            }
            let probe_keys = current
                .probe_keys
                .as_ref()
                .ok_or_else(|| paro_error::internal("hash join replay probe keys missing"))?;
            current.hash_table.probe(
                &probe_keys,
                &mut current.scan_structure,
                None,
                probe_keys.size(),
            )?;
            current.probe_in_progress = true;
        }
    }

    fn prepare_next_partition(
        &self,
        ctx: &mut OperatorCallContext,
        handle: &JoinBuildHandle,
    ) -> Result<Option<HashJoinSpillReplayPartitionLocal>> {
        let Some(partition) = handle.spill.take_next_replay_partition()? else {
            return Ok(None);
        };
        let template = handle.require_table()?;
        let hash_table = Arc::new(JoinHashTable::new_with_memory_and_output_count(
            template.buffer_pool().clone(),
            ctx.query.allocator(MemoryTag::HashTable),
            self.key_conditions.to_vec(),
            self.build_payload_types.to_vec(),
            self.build_output_count,
            self.join_type,
            JoinHashTableConfig::default(),
            hash_join_memory_context(ctx.query),
        ));
        hash_table.set_has_null(template.has_null_keys());

        let partition_idx = partition.partition_idx;
        let mut build_cursor = partition.build_rows.into_reclaiming_scanner();
        let mut build_chunk = Chunk::try_new(ctx.query.allocator(MemoryTag::BaseTable))?;
        loop {
            let scanned = build_cursor.next_chunk(&mut build_chunk)?;
            if scanned == 0 {
                break;
            }
            build_chunk.try_set_cardinality(scanned)?;
            hash_table.get_build_store().append_chunk(&build_chunk)?;
        }
        hash_table.refresh_count_from_data_collection();
        hash_table.finalize()?;
        let scan_structure = hash_table.create_scan_structure()?;
        let probe_cursor = partition
            .probe_rows
            .map(|probe_rows| probe_rows.into_reclaiming_scanner());
        let probe_exhausted = probe_cursor.is_none();
        Ok(Some(HashJoinSpillReplayPartitionLocal {
            partition_idx,
            hash_table,
            probe_cursor,
            probe_spill_chunk: Chunk::try_new(ctx.query.allocator(MemoryTag::BaseTable))?,
            probe_input: None,
            probe_keys: None,
            scan_structure,
            probe_in_progress: false,
            probe_exhausted,
            unmatched_scan_state: None,
        }))
    }

    fn scan_current_probe(
        &self,
        ctx: &OperatorCallContext,
        current: &mut HashJoinSpillReplayPartitionLocal,
        residual: Option<&mut HashJoinResidualProbeState>,
        reduction_residuals: &mut [Option<HashJoinResidualProbeState>],
        reduction_source_masks: &[u8],
        reduction_selection: &mut Option<SelectionVector>,
        output: &mut Chunk,
    ) -> Result<usize> {
        ensure_source_output(output, &self.output_types, VECTOR_SIZE)?;
        let probe_keys = current
            .probe_keys
            .as_ref()
            .ok_or_else(|| paro_error::internal("hash join replay probe keys missing"))?;
        let probe_input = current
            .probe_input
            .as_ref()
            .ok_or_else(|| paro_error::internal("hash join replay probe input missing"))?;
        if let Some(cascade) = &self.reduction_cascade {
            if reduction_selection.is_none() {
                *reduction_selection = Some(SelectionVector::try_with_capacity(
                    VECTOR_SIZE,
                    output.allocator().clone(),
                )?);
            }
            let selection = reduction_selection
                .as_mut()
                .ok_or_else(|| paro_error::internal("hash reduction replay selection missing"))?;
            current.scan_structure.mark_right_matches_with_masks(
                probe_keys,
                current.hash_table.as_ref(),
                |lhs_sel, rhs_pointers, match_count, masks| {
                    for (predicate, residual) in cascade
                        .predicates
                        .iter()
                        .zip(reduction_residuals.iter_mut())
                    {
                        let accepted_count = if let Some(residual) = residual.as_mut() {
                            residual.select_matches(
                                ctx.query,
                                current.hash_table.as_ref(),
                                lhs_sel,
                                rhs_pointers,
                                match_count,
                                selection,
                            )?
                        } else {
                            selection.set_len(match_count);
                            for idx in 0..match_count {
                                selection.set(idx, idx);
                            }
                            match_count
                        };
                        for accepted_idx in 0..accepted_count {
                            masks[selection.get(accepted_idx)] |= predicate.predicate_mask;
                        }
                    }
                    for candidate_idx in 0..match_count {
                        masks[candidate_idx] |= reduction_source_masks[lhs_sel.get(candidate_idx)];
                    }
                    for candidate_mask in masks {
                        let accepted_predicates = *candidate_mask;
                        *candidate_mask = cascade
                            .steps
                            .iter()
                            .filter(|step| {
                                accepted_predicates & step.predicate_mask == step.predicate_mask
                            })
                            .fold(0u8, |mask, step| mask | step.match_mask);
                    }
                    Ok(())
                },
            )?;
            current.probe_in_progress = false;
            output.try_set_cardinality(0)?;
            return Ok(0);
        }
        let count = scan_hash_join_results(
            self.join_type,
            self.anti_join_mode,
            probe_keys,
            probe_input,
            output,
            current.hash_table.as_ref(),
            &mut current.scan_structure,
            &self.left_projection,
            residual,
            ctx.query,
        )?;
        if current.scan_structure.finished {
            current.probe_in_progress = false;
        }
        Ok(count)
    }

    fn scan_current_unmatched(
        &self,
        current: &mut HashJoinSpillReplayPartitionLocal,
        output: &mut Chunk,
    ) -> Result<usize> {
        if !matches!(
            self.join_type,
            JoinType::Right | JoinType::Outer | JoinType::RightSemi | JoinType::RightAnti
        ) {
            output.try_set_cardinality(0)?;
            return Ok(0);
        }

        ensure_source_output(output, &self.output_types, VECTOR_SIZE)?;
        let scan_state = current
            .unmatched_scan_state
            .get_or_insert_with(FullOuterScanState::new);
        let emit_found = matches!(self.join_type, JoinType::RightSemi);
        let mut build_chunk = Chunk::try_initialize(
            current.hash_table.build_output_types(),
            VECTOR_SIZE,
            output.allocator().clone(),
        )?;
        let count = if let Some(cascade) = &self.reduction_cascade {
            current.hash_table.scan_reduction_cascade(
                scan_state,
                cascade.required_mask,
                cascade.forbidden_mask,
                &mut build_chunk,
            )?
        } else {
            current
                .hash_table
                .scan_full_outer(scan_state, emit_found, &mut build_chunk)?
        };
        if count == 0 {
            output.try_set_cardinality(0)?;
            return Ok(0);
        }

        let build_sel = SelectionVector::try_incremental(count, output.allocator().clone())?;
        match self.join_type {
            JoinType::Right | JoinType::Outer => {
                let projection = (0..build_chunk.column_count()).collect::<Vec<_>>();
                construct_right_outer_scan_result(
                    &build_chunk,
                    &build_sel,
                    count,
                    &self.left_output_types(),
                    &projection,
                    output,
                )?
            }
            JoinType::RightSemi | JoinType::RightAnti => {
                let projection = (0..build_chunk.column_count()).collect::<Vec<_>>();
                construct_semi_join_result(&build_chunk, &build_sel, count, &projection, output)?
            }
            _ => unreachable!("checked build-propagating join types above"),
        }
        Ok(count)
    }

    fn left_output_types(&self) -> Vec<LogicalType> {
        self.left_projection
            .iter()
            .map(|idx| self.probe_types[*idx].clone())
            .collect()
    }
}
