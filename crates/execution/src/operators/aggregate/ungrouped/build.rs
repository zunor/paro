// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Ungrouped aggregate build sink operator.

use std::sync::Arc;

use paro_common::allocator::MemoryTag;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_common::vector::{Vector, VECTOR_SIZE};
use paro_function::aggregate::{AggregateCombineType, AggregateInputData};
use paro_function::scalar::FunctionExecContext;

use crate::expression_executor::executor::{ExpressionExecutor, VectorKernelInput};
use crate::operators::aggregate::aggregate_kernel::{
    update_filtered_states, update_states, AggregatePayload,
};
use crate::operators::aggregate::build_helpers::{
    build_per_aggregate_filters, can_skip_regular_aggregate_sink, combine_ungrouped_states,
    create_ungrouped_runtime_state, destroy_ungrouped_local, fill_repeated_state_addresses,
    has_aggregate_distinct, has_aggregate_filters, has_aggregate_ordered, query_modifier_memory,
};
use crate::operators::aggregate::distinct_helpers::{
    collect_distinct_rows, finalize_ungrouped_distinct,
};
use crate::operators::aggregate::distinct_state::DistinctAggregateState;
use crate::operators::aggregate::ordered_helpers::{
    collect_ordered_rows, finalize_ordered_ungrouped, merge_ordered_collectors,
};
use crate::physical::specs::AggregateSpec;
use crate::runtime::breaker::{
    AggregateBuildCompactionReclaimer, AggregateFinalizedStateReclaimer, AggregateHandle,
    AggregateRuntimeState, HandleRef,
};
use crate::runtime::context::{OperatorCallContext, OperatorFinishContext, PipelineInitContext};
use crate::runtime::sink::{FinishPoll, FinishWork, MergePoll, PrepareFinishPoll, SinkPoll};
use crate::runtime::state::{
    BreakerHandleGlobal, SinkGlobal, SinkLocal, UngroupedAggregateSinkLocal,
};
use crate::runtime::ExpressionEvalInput;

/// Sink operator for aggregates without group keys (scalar aggregates).
#[derive(Debug, Clone)]
pub struct UngroupedAggregateSinkExec {
    pub handle: HandleRef<AggregateHandle>,
    pub spec: AggregateSpec,
}

impl UngroupedAggregateSinkExec {
    pub(crate) fn create_global(&self, ctx: &mut PipelineInitContext) -> Result<SinkGlobal> {
        let handle = ctx.handles.get(self.handle)?;
        let state = create_ungrouped_runtime_state(
            &self.spec,
            ctx.query.allocator(MemoryTag::HashTable),
            ctx.query.session.buffer_pool().clone(),
            query_modifier_memory(ctx.query),
        )?;
        handle.initialize(AggregateRuntimeState::Ungrouped(state))?;
        ctx.query.memory.register_reclaimer_once_by_name(Arc::new(
            AggregateBuildCompactionReclaimer::new(handle.clone()),
        ));
        ctx.query.memory.register_reclaimer_once_by_name(Arc::new(
            AggregateFinalizedStateReclaimer::for_query(
                handle.clone(),
                ctx.query.session.buffer_pool().clone(),
                ctx.query.memory.clone(),
            ),
        ));
        Ok(SinkGlobal::UngroupedAggregate(Arc::new(
            BreakerHandleGlobal { handle },
        )))
    }

    pub(crate) fn create_local(
        &self,
        ctx: &mut PipelineInitContext,
        _global: &SinkGlobal,
    ) -> Result<SinkLocal> {
        let state = create_ungrouped_runtime_state(
            &self.spec,
            ctx.query.allocator(MemoryTag::HashTable),
            ctx.query.session.buffer_pool().clone(),
            query_modifier_memory(ctx.query),
        )?;
        Ok(SinkLocal::UngroupedAggregate(UngroupedAggregateSinkLocal {
            aggregate_objects: Arc::clone(&state.aggregate_objects),
            layout: state.layout.clone(),
            aggregate_inputs: Arc::clone(&state.aggregate_inputs),
            projection_executor: (!self.spec.projection_exprs.is_empty()).then(|| {
                ExpressionExecutor::with_expressions_for_session(
                    &self.spec.projection_exprs,
                    ctx.query.session.as_ref(),
                )
            }),
            payload_chunk: Chunk::try_initialize(
                &self.spec.payload_types,
                VECTOR_SIZE,
                ctx.query.allocator(MemoryTag::BaseTable),
            )?,
            state_buffer: state.state_buffer,
            addresses: Vector::try_new(
                LogicalType::BigInt,
                VECTOR_SIZE,
                ctx.query.allocator(MemoryTag::HashTable),
            )?,
            ordered_collectors: state.ordered_collectors,
            arena_allocator: state.arena_allocator,
            destroyed: state.destroyed,
            modifier_memory: query_modifier_memory(ctx.query),
            distinct: DistinctAggregateState::new(state.aggregate_objects.len()),
        }))
    }

    pub(crate) fn consume(
        &self,
        ctx: &mut OperatorCallContext,
        _global: &SinkGlobal,
        local: &mut SinkLocal,
        input: &mut Chunk,
    ) -> Result<SinkPoll> {
        ctx.cancel.check()?;
        if input.is_empty() {
            return Ok(SinkPoll::NeedMoreInput);
        }
        let SinkLocal::UngroupedAggregate(local) = local else {
            return Err(paro_error::internal(
                "ungrouped aggregate sink local state mismatch",
            ));
        };
        if let Some(executor) = local.projection_executor.as_mut() {
            if local.payload_chunk.column_count() != self.spec.payload_types.len()
                || local.payload_chunk.capacity() < input.size()
            {
                local.payload_chunk = Chunk::try_initialize(
                    &self.spec.payload_types,
                    input.size().max(1),
                    ctx.query.allocator(MemoryTag::BaseTable),
                )?;
            }
            executor.execute_all_kernel(
                VectorKernelInput::from_eval_input(ExpressionEvalInput {
                    params: ctx.query.params.as_ref(),
                    columns: input,
                }),
                ctx.query,
                &mut local.payload_chunk,
            )?;
        }
        let payload = if local.projection_executor.is_some() {
            &local.payload_chunk
        } else {
            input
        };
        let has_distinct = has_aggregate_distinct(&self.spec);
        let has_filters = has_aggregate_filters(&self.spec);
        let has_ordered = has_aggregate_ordered(&self.spec);
        if has_distinct {
            // Ungrouped DISTINCT still needs a zero-column key prefix with the
            // same logical row count as the payload. Scan batches may exceed
            // VECTOR_SIZE, so its capacity must come from the batch rather
            // than Chunk::try_new's default.
            let mut groups =
                Chunk::try_initialize(&[], payload.size().max(1), payload.allocator().clone())?;
            groups.try_set_cardinality(payload.size())?;
            collect_distinct_rows(
                &self.spec,
                &local.aggregate_objects,
                payload,
                &groups,
                ctx.query.session.number_of_threads(),
                ctx.query.memory.capacity_bytes(),
                &local.modifier_memory,
                &mut local.distinct,
            )?;
        }
        if has_ordered {
            collect_ordered_rows(
                &self.spec,
                &local.aggregate_objects,
                payload,
                &[],
                &mut local.ordered_collectors,
            )?;
        }
        if has_filters || has_distinct || has_ordered {
            let filters = if has_filters {
                build_per_aggregate_filters(&self.spec, payload)?
            } else {
                local.aggregate_objects.iter().map(|_| None).collect()
            };
            let base_ptr = local.state_buffer.as_mut_ptr() as *mut u8;
            for (agg_idx, (object, filter)) in local
                .aggregate_objects
                .iter()
                .zip(filters.iter())
                .enumerate()
            {
                if object.is_distinct() || !object.order_bys.is_empty() {
                    continue;
                }
                let state_offset = local.layout.state_offset(agg_idx);
                let agg_ptr = unsafe { base_ptr.add(state_offset) };
                fill_repeated_state_addresses(&mut local.addresses, agg_ptr, payload.size())?;
                let payload_desc = AggregatePayload {
                    chunk: payload,
                    aggregate_inputs: &local.aggregate_inputs[agg_idx..agg_idx + 1],
                };
                let mut input_data = AggregateInputData::new(
                    object.bind_info.as_deref(),
                    &mut local.arena_allocator,
                    AggregateCombineType::PreserveInput,
                );
                if let Some(selection) = filter {
                    if !selection.is_empty() {
                        update_filtered_states(
                            std::slice::from_ref(object),
                            &mut input_data,
                            &payload_desc,
                            &local.addresses,
                            selection,
                            selection.len(),
                        )?;
                    }
                } else {
                    update_states(
                        std::slice::from_ref(object),
                        &mut input_data,
                        &payload_desc,
                        &local.addresses,
                        payload.size(),
                    )?;
                }
            }
        } else {
            fill_repeated_state_addresses(
                &mut local.addresses,
                local.state_buffer.as_mut_ptr() as *mut u8,
                payload.size(),
            )?;
            let payload_desc = AggregatePayload {
                chunk: payload,
                aggregate_inputs: &local.aggregate_inputs,
            };
            let mut input_data = AggregateInputData::new(
                None,
                &mut local.arena_allocator,
                AggregateCombineType::PreserveInput,
            );
            update_states(
                &local.aggregate_objects,
                &mut input_data,
                &payload_desc,
                &local.addresses,
                payload.size(),
            )?;
        }
        Ok(SinkPoll::NeedMoreInput)
    }

    pub(crate) fn merge_local(
        &self,
        _ctx: &mut OperatorCallContext,
        global: &SinkGlobal,
        local: &mut SinkLocal,
    ) -> Result<MergePoll> {
        let SinkGlobal::UngroupedAggregate(global) = global else {
            return Err(paro_error::internal(
                "ungrouped aggregate sink global state mismatch",
            ));
        };
        let SinkLocal::UngroupedAggregate(local) = local else {
            return Err(paro_error::internal(
                "ungrouped aggregate sink local state mismatch",
            ));
        };
        global.handle.with_state_mut(|state| {
            let AggregateRuntimeState::Ungrouped(global) = state else {
                return Err(paro_error::internal(
                    "aggregate handle does not contain ungrouped aggregate state",
                ));
            };
            global.distinct.merge_from(&mut local.distinct)?;
            if !can_skip_regular_aggregate_sink(&self.spec, &local.aggregate_objects) {
                combine_ungrouped_states(global, local)?;
            }
            merge_ordered_collectors(
                &mut global.ordered_collectors,
                &mut local.ordered_collectors,
            )?;
            destroy_ungrouped_local(local)
        })?;
        Ok(MergePoll::Done)
    }

    pub(crate) fn prepare_finish(
        &self,
        _ctx: &mut OperatorFinishContext,
        _global: &SinkGlobal,
    ) -> Result<PrepareFinishPoll> {
        Ok(PrepareFinishPoll::Done)
    }

    pub(crate) fn finish_work(
        &self,
        _ctx: &mut OperatorFinishContext,
        _global: &SinkGlobal,
    ) -> Result<FinishWork> {
        Ok(FinishWork::None)
    }

    pub(crate) fn finish(
        &self,
        ctx: &mut OperatorFinishContext,
        global: &SinkGlobal,
    ) -> Result<FinishPoll> {
        let SinkGlobal::UngroupedAggregate(global) = global else {
            return Err(paro_error::internal(
                "ungrouped aggregate sink global state mismatch",
            ));
        };
        global.handle.with_state_mut(|state| {
            let AggregateRuntimeState::Ungrouped(global) = state else {
                return Err(paro_error::internal(
                    "aggregate handle does not contain ungrouped aggregate state",
                ));
            };
            finalize_ungrouped_distinct(&self.spec, global)?;
            finalize_ordered_ungrouped(&self.spec, &query_modifier_memory(ctx.query), global)
        })?;
        global.handle.mark_finalized();
        global.handle.enable_state_reclaim();
        Ok(FinishPoll::Done)
    }
}
