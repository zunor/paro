// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_common::vector::VECTOR_SIZE;
use paro_planner::operator::join::{AntiJoinMode, JoinCondition, JoinType};

use crate::expression_executor::executor::ExpressionExecutor;
use crate::operators::join::hash::hashing::compute_hashes_for_keys_into;
use crate::operators::join::hash::keys::{evaluate_join_keys_into, join_key_types, JoinKeySide};
use crate::operators::join::hash::memory::hash_join_spill_memory_context;
use crate::operators::join::hash::probe_output::{
    emit_empty_build_probe_result, scan_hash_join_results,
};
use crate::operators::join::hash::residual::HashJoinResidualProbeState;
use crate::operators::join::hash::source_predicate::ReductionSourcePredicateState;
use crate::operators::join::hash::spill::build_probe_spill_chunk_into;
use crate::operators::join::state::ReductionProbeMode;
use crate::operators::output::ensure_transform_output;
use crate::physical::specs::HashReductionCascadeSpec;
use crate::runtime::breaker::{HandleRef, JoinBuildHandle, JoinProbeSpillBuffer};
use crate::runtime::context::{OperatorCallContext, OperatorFinishContext, PipelineInitContext};
use crate::runtime::state::{
    BreakerHandleGlobal, HashJoinProbeTransformLocal, TransformGlobal, TransformLocal,
};
use crate::runtime::transform::{TransformFinishPoll, TransformFlushPoll, TransformPoll};

use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct HashJoinProbeTransformExec {
    pub handle: HandleRef<JoinBuildHandle>,
    pub join_type: JoinType,
    pub anti_join_mode: AntiJoinMode,
    pub key_conditions: Box<[JoinCondition]>,
    pub build_residual_conditions: Box<[JoinCondition]>,
    pub probe_residual_count: usize,
    pub left_projection: Box<[usize]>,
    pub output_types: Box<[LogicalType]>,
    pub reduction_cascade: Option<HashReductionCascadeSpec>,
}

impl HashJoinProbeTransformExec {
    pub(crate) fn create_global(&self, ctx: &mut PipelineInitContext) -> Result<TransformGlobal> {
        Ok(TransformGlobal::HashJoinProbe(Arc::new(
            BreakerHandleGlobal {
                handle: ctx.handles.get(self.handle)?,
            },
        )))
    }

    pub(crate) fn create_local(
        &self,
        ctx: &mut PipelineInitContext,
        _global: &TransformGlobal,
    ) -> Result<TransformLocal> {
        let probe_residual_conditions = self
            .build_residual_conditions
            .get(..self.probe_residual_count)
            .ok_or_else(|| {
                paro_error::internal("hash join probe residual prefix exceeds build layout")
            })?;
        let reduction_channel_map = self
            .reduction_cascade
            .as_ref()
            .and_then(|cascade| cascade.grouped_extrema.as_ref())
            .map(|grouped| Arc::clone(&grouped.channel_map));
        Ok(TransformLocal::HashJoinProbe(HashJoinProbeTransformLocal {
            scan_structure: None,
            probe_keys: None,
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
                probe_residual_conditions,
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
                            let condition = self
                                .build_residual_conditions
                                .get(predicate.build_residual_offset)
                                .ok_or_else(|| {
                                    paro_error::internal(
                                        "reduction residual offset exceeds build layout",
                                    )
                                })?;
                            Ok(HashJoinResidualProbeState::new_at_offset(
                                std::slice::from_ref(condition),
                                predicate.build_residual_offset,
                                ctx.query.session.as_ref(),
                            ))
                        })
                        .collect::<Result<Vec<_>>>()
                        .map(Vec::into_boxed_slice)
                })
                .transpose()?
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
            reduction_channel_map,
            reduction_selection: None,
            reduction_mode: ReductionProbeMode::Uninitialized,
            reduction_group_slots: Vec::new(),
            probe_hashes: None,
            probe_spill_chunk: None,
            probe_spill_buffer: None,
            probe_in_progress: false,
        }))
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
        let TransformGlobal::HashJoinProbe(global) = global else {
            return Err(paro_error::internal(
                "hash join probe transform global state mismatch",
            ));
        };
        let TransformLocal::HashJoinProbe(local) = local else {
            return Err(paro_error::internal(
                "hash join probe transform local state mismatch",
            ));
        };
        if !global.handle.completion.is_complete() {
            return Err(paro_error::internal(
                "hash join probe scheduled before build handle finalized",
            ));
        }
        let hash_table = global.handle.require_table()?;
        ensure_transform_output(output, &self.output_types, VECTOR_SIZE)?;

        if self.anti_join_mode == AntiJoinMode::NullAware && hash_table.has_null_keys() {
            output.try_set_cardinality(0)?;
            return Ok(TransformPoll::NeedMoreInput);
        }

        if global.handle.is_external() {
            if input.is_empty() {
                output.try_set_cardinality(0)?;
                return Ok(TransformPoll::NeedMoreInput);
            }
            evaluate_join_keys_into(
                ctx,
                input,
                &self.key_conditions,
                &mut local.probe_key_executors,
                &local.probe_key_types,
                JoinKeySide::Probe,
                &mut local.probe_keys,
            )?;
            let probe_keys = local
                .probe_keys
                .as_ref()
                .ok_or_else(|| paro_error::internal("hash join probe keys missing"))?;
            compute_hashes_for_keys_into(hash_table.as_ref(), probe_keys, &mut local.probe_hashes)?;
            let hashes = local
                .probe_hashes
                .as_ref()
                .ok_or_else(|| paro_error::internal("hash join probe hashes missing"))?;
            build_probe_spill_chunk_into(input, hashes, &mut local.probe_spill_chunk)?;
            let radix_bits = global
                .handle
                .external_config()
                .map(|config| config.radix_bits as usize)
                .ok_or_else(|| {
                    paro_error::internal("hash join external mode has no replay config")
                })?;
            {
                let spill_chunk = local
                    .probe_spill_chunk
                    .as_ref()
                    .ok_or_else(|| paro_error::internal("hash join probe spill chunk missing"))?;
                if local.probe_spill_buffer.is_none() {
                    local.probe_spill_buffer = Some(JoinProbeSpillBuffer::new(
                        hash_table.buffer_pool().clone(),
                        radix_bits,
                        input.column_count(),
                        spill_chunk.types(),
                        hash_join_spill_memory_context(ctx.query),
                    )?);
                }
                local
                    .probe_spill_buffer
                    .as_mut()
                    .expect("hash join probe spill buffer initialized")
                    .append(spill_chunk)?;
            }
            if let Some(spill_chunk) = local.probe_spill_chunk.as_mut() {
                spill_chunk.data.clear();
            }
            output.try_set_cardinality(0)?;
            return Ok(TransformPoll::NeedMoreInput);
        }

        if hash_table.is_empty()
            && !(self.join_type == JoinType::Mark && hash_table.has_null_keys())
        {
            let emitted = emit_empty_build_probe_result(
                self.join_type,
                input,
                &self.left_projection,
                &self.output_types,
                output,
            )?;
            return if emitted > 0 {
                Ok(TransformPoll::Output)
            } else {
                Ok(TransformPoll::NeedMoreInput)
            };
        }

        if let Some(cascade) = &self.reduction_cascade {
            if input.is_empty() {
                output.try_set_cardinality(0)?;
                return Ok(TransformPoll::NeedMoreInput);
            }
            evaluate_join_keys_into(
                ctx,
                input,
                &self.key_conditions,
                &mut local.probe_key_executors,
                &local.probe_key_types,
                JoinKeySide::Probe,
                &mut local.probe_keys,
            )?;
            let selection_capacity = input.size().max(VECTOR_SIZE);
            if local
                .reduction_selection
                .as_ref()
                .is_none_or(|selection| selection.capacity() < selection_capacity)
            {
                local.reduction_selection =
                    Some(paro_common::vector::SelectionVector::try_with_capacity(
                        selection_capacity,
                        input.allocator().clone(),
                    )?);
            }
            let selection = local
                .reduction_selection
                .as_mut()
                .ok_or_else(|| paro_error::internal("hash reduction selection missing"))?;
            local.reduction_source_masks.resize(input.size(), 0);
            local.reduction_source_masks.fill(0);
            for predicate in local.reduction_source_predicates.iter_mut() {
                predicate.evaluate_into(
                    input,
                    ctx.query,
                    selection,
                    &mut local.reduction_source_masks,
                )?;
            }
            if let Some(grouped) = &cascade.grouped_extrema {
                if matches!(local.reduction_mode, ReductionProbeMode::Uninitialized) {
                    local.reduction_mode = hash_table.grouped_reduction_extrema().map_or(
                        ReductionProbeMode::MatchMask,
                        ReductionProbeMode::GroupedExtrema,
                    );
                }
                if let ReductionProbeMode::GroupedExtrema(extrema) = &local.reduction_mode {
                    let values = input.column(grouped.source_value_index).ok_or_else(|| {
                        paro_error::internal("reduction extrema source column missing")
                    })?;
                    if values.logical_type() != &LogicalType::BigInt {
                        return Err(paro_error::internal(
                            "reduction extrema source column must be BIGINT",
                        ));
                    }
                    let values_view = values.try_to_view(input.size())?;
                    let values_data = values_view.get_data::<i64>();
                    local.reduction_group_slots.resize(input.size(), usize::MAX);
                    let probe_keys = local
                        .probe_keys
                        .as_ref()
                        .ok_or_else(|| paro_error::internal("hash reduction probe keys missing"))?;
                    let key = probe_keys.column(0).ok_or_else(|| {
                        paro_error::internal("grouped reduction equality key missing")
                    })?;
                    if !hash_table.lookup_i64_group_slots(
                        key,
                        probe_keys.size(),
                        &mut local.reduction_group_slots,
                    )? {
                        return Err(paro_error::internal(
                            "grouped reduction mode lost its finalized ranked key index",
                        ));
                    }
                    let mut row_idx = 0usize;
                    while row_idx < input.size() {
                        let slot = local.reduction_group_slots[row_idx];
                        if slot == usize::MAX {
                            row_idx += 1;
                            continue;
                        }
                        let run_start = row_idx;
                        while row_idx < input.size() && local.reduction_group_slots[row_idx] == slot
                        {
                            row_idx += 1;
                        }
                        let mut minima = [0_i64; u8::BITS as usize];
                        let mut maxima = [0_i64; u8::BITS as usize];
                        minima[..grouped.channels.len()].fill(i64::MAX);
                        maxima[..grouped.channels.len()].fill(i64::MIN);
                        let mut seen_channels = 0u8;
                        for value_idx in run_start..row_idx {
                            if !values_view.is_valid(value_idx) {
                                continue;
                            }
                            let value = match values_data {
                                Some(data) => unsafe {
                                    // SAFETY: the BIGINT vector view validates
                                    // its storage and `value_idx` is in-batch.
                                    *data.add(values_view.physical_index(value_idx))
                                },
                                None => values_view.get_i64(value_idx),
                            };
                            let source_mask = local.reduction_source_masks[value_idx];
                            let eligible_channels =
                                local.reduction_channel_map.as_ref().ok_or_else(|| {
                                    paro_error::internal(
                                        "grouped reduction is missing its channel map",
                                    )
                                })?[source_mask as usize];
                            let mut remaining = eligible_channels;
                            while remaining != 0 {
                                let channel_idx = remaining.trailing_zeros() as usize;
                                minima[channel_idx] = minima[channel_idx].min(value);
                                maxima[channel_idx] = maxima[channel_idx].max(value);
                                remaining &= remaining - 1;
                            }
                            seen_channels |= eligible_channels;
                        }
                        for channel_idx in 0..grouped.channels.len() {
                            if seen_channels & (1u8 << channel_idx) != 0 {
                                extrema.update_i64_range(
                                    slot,
                                    channel_idx,
                                    minima[channel_idx],
                                    maxima[channel_idx],
                                )?;
                            }
                        }
                    }
                    output.try_set_cardinality(0)?;
                    return Ok(TransformPoll::NeedMoreInput);
                }
            }
            for residual in local.reduction_residuals.iter_mut().flatten() {
                residual.evaluate_probe(ctx, input)?;
            }
            if local.scan_structure.is_none() {
                local.scan_structure = Some(hash_table.create_scan_structure()?);
            }
            let probe_keys = local
                .probe_keys
                .as_ref()
                .ok_or_else(|| paro_error::internal("hash reduction probe keys missing"))?;
            let scan_structure = local
                .scan_structure
                .as_mut()
                .ok_or_else(|| paro_error::internal("hash reduction scan structure missing"))?;
            hash_table.probe(probe_keys, scan_structure, None, probe_keys.size())?;
            let residuals = &mut local.reduction_residuals;
            let source_masks = &local.reduction_source_masks;
            scan_structure.mark_right_matches_with_masks(
                probe_keys,
                hash_table.as_ref(),
                |lhs_sel, rhs_pointers, match_count, masks| {
                    for (predicate, residual) in cascade.predicates.iter().zip(residuals.iter_mut())
                    {
                        let accepted_count = if let Some(residual) = residual.as_mut() {
                            residual.select_matches(
                                ctx.query,
                                hash_table.as_ref(),
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
                        masks[candidate_idx] |= source_masks[lhs_sel.get(candidate_idx)];
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
            output.try_set_cardinality(0)?;
            return Ok(TransformPoll::NeedMoreInput);
        }

        if !local.probe_in_progress {
            if input.is_empty() {
                output.try_set_cardinality(0)?;
                return Ok(TransformPoll::NeedMoreInput);
            }
            evaluate_join_keys_into(
                ctx,
                input,
                &self.key_conditions,
                &mut local.probe_key_executors,
                &local.probe_key_types,
                JoinKeySide::Probe,
                &mut local.probe_keys,
            )?;
            if let Some(residual) = local.residual.as_mut() {
                residual.evaluate_probe(ctx, input)?;
            }
            if local.scan_structure.is_none() {
                local.scan_structure = Some(hash_table.create_scan_structure()?);
            }
            let probe_keys = local
                .probe_keys
                .as_ref()
                .ok_or_else(|| paro_error::internal("hash join probe keys missing"))?;
            let scan_structure = local
                .scan_structure
                .as_mut()
                .expect("hash join scan structure initialized");
            hash_table.probe(&probe_keys, scan_structure, None, probe_keys.size())?;
            local.probe_in_progress = true;
        }

        loop {
            ensure_transform_output(output, &self.output_types, VECTOR_SIZE)?;
            let (count, finished) = {
                let probe_keys = local
                    .probe_keys
                    .as_ref()
                    .ok_or_else(|| paro_error::internal("hash join probe keys missing"))?;
                let scan_structure = local
                    .scan_structure
                    .as_mut()
                    .ok_or_else(|| paro_error::internal("hash join scan structure missing"))?;
                let count = scan_hash_join_results(
                    self.join_type,
                    self.anti_join_mode,
                    probe_keys,
                    input,
                    output,
                    &hash_table,
                    scan_structure,
                    &self.left_projection,
                    local.residual.as_mut(),
                    ctx.query,
                )?;
                (count, scan_structure.finished)
            };
            if finished {
                local.probe_in_progress = false;
            }
            if count > 0 {
                return if local.probe_in_progress {
                    Ok(TransformPoll::OutputMore)
                } else {
                    Ok(TransformPoll::Output)
                };
            }
            if !local.probe_in_progress {
                return Ok(TransformPoll::NeedMoreInput);
            }
        }
    }

    pub(crate) fn flush(
        &self,
        _ctx: &mut OperatorCallContext,
        global: &TransformGlobal,
        local: &mut TransformLocal,
        _output: &mut Chunk,
    ) -> Result<TransformFlushPoll> {
        let TransformGlobal::HashJoinProbe(global) = global else {
            return Err(paro_error::internal(
                "hash join probe transform global state mismatch",
            ));
        };
        let TransformLocal::HashJoinProbe(local) = local else {
            return Err(paro_error::internal(
                "hash join probe transform local state mismatch",
            ));
        };
        if let Some(buffer) = local.probe_spill_buffer.take() {
            global.handle.spill.append_probe_buffer(buffer)?;
        }
        Ok(TransformFlushPoll::Done)
    }

    pub(crate) fn finish_global(
        &self,
        _ctx: &mut OperatorFinishContext,
        _global: &TransformGlobal,
    ) -> Result<TransformFinishPoll> {
        Ok(TransformFinishPoll::Done)
    }
}
