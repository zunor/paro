// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::mem::size_of;
use std::sync::Arc;

use paro_common::allocator::{ArenaAllocator, MemoryTag};
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_common::vector::{SelectionVector, Vector, VECTOR_SIZE};
use paro_function::aggregate::{AggregateCombineType, AggregateInputData};
use paro_function::scalar::FunctionExecContext;

use crate::expression_executor::executor::{ExpressionExecutor, VectorKernelInput};
use crate::operators::aggregate::aggregate_kernel::{
    destroy_states, finalize_states, initialize_states, update_states, AggregatePayload,
};
use crate::operators::aggregate::aggregate_object::create_aggregate_objects;
use crate::operators::aggregate::aggregate_state::AggregateStateLayout;
use crate::physical::specs::AggregateSpec;
use crate::runtime::context::{OperatorCallContext, OperatorFinishContext, PipelineInitContext};
use crate::runtime::state::{
    StreamingAggregateTransformGlobal, StreamingAggregateTransformLocal, TransformGlobal,
    TransformLocal,
};
use crate::runtime::transform::{TransformFinishPoll, TransformFlushPoll, TransformPoll};
use crate::runtime::ExpressionEvalInput;

#[derive(Debug, Clone)]
pub struct StreamingAggregateTransformExec {
    pub spec: AggregateSpec,
}

impl StreamingAggregateTransformExec {
    pub(crate) fn create_global(&self, _ctx: &mut PipelineInitContext) -> Result<TransformGlobal> {
        let objects = create_aggregate_objects(&self.spec.aggregates)?;
        let layout = AggregateStateLayout::new(&objects)?;
        let aggregate_inputs = self
            .spec
            .aggregate_inputs
            .iter()
            .map(|inputs| inputs.to_vec())
            .collect::<Vec<_>>();
        Ok(TransformGlobal::StreamingAggregate(Arc::new(
            StreamingAggregateTransformGlobal {
                aggregate_objects: Arc::from(objects.into_boxed_slice()),
                layout,
                aggregate_inputs: Arc::from(aggregate_inputs.into_boxed_slice()),
            },
        )))
    }

    pub(crate) fn create_local(
        &self,
        ctx: &mut PipelineInitContext,
        global: &TransformGlobal,
    ) -> Result<TransformLocal> {
        let TransformGlobal::StreamingAggregate(global) = global else {
            return Err(paro_error::internal(
                "streaming aggregate transform global state mismatch",
            ));
        };
        let mut state_buffer = vec![0u64; state_buffer_words(global.layout.total_size())];
        let arena_allocator = ArenaAllocator::new(ctx.query.allocator(MemoryTag::HashTable));
        initialize_state_buffer(
            &global.layout,
            &global.aggregate_objects,
            &mut state_buffer,
            ctx.query.allocator(MemoryTag::HashTable),
        )?;
        if self.spec.having_filter.len() > 1 {
            return Err(paro_error::internal(
                "aggregate HAVING lowering requires one normalized predicate",
            ));
        }
        Ok(TransformLocal::StreamingAggregate(
            StreamingAggregateTransformLocal {
                aggregate_objects: Arc::clone(&global.aggregate_objects),
                layout: global.layout.clone(),
                aggregate_inputs: Arc::clone(&global.aggregate_inputs),
                projection_executor: (!self.spec.projection_exprs.is_empty()).then(|| {
                    ExpressionExecutor::with_expressions_for_session(
                        &self.spec.projection_exprs,
                        ctx.query.session.as_ref(),
                    )
                }),
                having_executor: (!self.spec.having_filter.is_empty()).then(|| {
                    ExpressionExecutor::with_expressions_for_session(
                        &self.spec.having_filter,
                        ctx.query.session.as_ref(),
                    )
                }),
                having_selection: (!self.spec.having_filter.is_empty())
                    .then(|| {
                        SelectionVector::try_with_capacity(
                            1,
                            ctx.query.allocator(MemoryTag::BaseTable),
                        )
                    })
                    .transpose()?,
                payload_chunk: Chunk::try_initialize(
                    &self.spec.payload_types,
                    VECTOR_SIZE,
                    ctx.query.allocator(MemoryTag::BaseTable),
                )?,
                state_buffer,
                arena_allocator,
                emitted: false,
                destroyed: false,
            },
        ))
    }

    pub(crate) fn transform(
        &self,
        ctx: &mut OperatorCallContext,
        _global: &TransformGlobal,
        local: &mut TransformLocal,
        input: &Chunk,
        output: &mut Chunk,
    ) -> Result<TransformPoll> {
        let TransformLocal::StreamingAggregate(local) = local else {
            return Err(paro_error::internal(
                "streaming aggregate transform local state mismatch",
            ));
        };
        output.try_set_cardinality(0)?;
        if input.is_empty() {
            return Ok(TransformPoll::NeedMoreInput);
        }

        if let Some(executor) = local.projection_executor.as_mut() {
            executor.execute_all_kernel(
                VectorKernelInput::from_eval_input(ExpressionEvalInput {
                    params: ctx.query.params.as_ref(),
                    columns: input,
                }),
                ctx.query,
                &mut local.payload_chunk,
            )?;
        } else {
            prepare_empty_payload(
                &mut local.payload_chunk,
                input.size(),
                ctx.query.allocator(MemoryTag::BaseTable),
            )?;
        }

        let addresses = repeated_state_addresses(
            local.state_buffer.as_mut_ptr() as *mut u8,
            input.size(),
            local.arena_allocator.get_allocator().clone(),
        )?;
        let payload = AggregatePayload {
            chunk: &local.payload_chunk,
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
            &payload,
            &addresses,
            input.size(),
        )?;
        Ok(TransformPoll::NeedMoreInput)
    }

    pub(crate) fn flush(
        &self,
        ctx: &mut OperatorCallContext,
        _global: &TransformGlobal,
        local: &mut TransformLocal,
        output: &mut Chunk,
    ) -> Result<TransformFlushPoll> {
        let TransformLocal::StreamingAggregate(local) = local else {
            return Err(paro_error::internal(
                "streaming aggregate transform local state mismatch",
            ));
        };
        if local.emitted {
            return Ok(TransformFlushPoll::Done);
        }
        if output.column_count() != self.spec.output_types.len() || output.capacity() < 1 {
            *output = Chunk::try_initialize(
                &self.spec.output_types,
                1,
                ctx.query.allocator(MemoryTag::BaseTable),
            )?;
        } else {
            output.try_reset(output.allocator().clone())?;
        }

        let addresses = single_state_addresses(
            local.state_buffer.as_mut_ptr() as *mut u8,
            local.arena_allocator.get_allocator().clone(),
        )?;
        let mut input_data = AggregateInputData::new(
            None,
            &mut local.arena_allocator,
            AggregateCombineType::PreserveInput,
        );
        finalize_states(
            &local.aggregate_objects,
            &mut input_data,
            &addresses,
            output,
            1,
        )?;
        let selected_count = if let (Some(executor), Some(selection)) = (
            local.having_executor.as_mut(),
            local.having_selection.as_mut(),
        ) {
            executor.select_kernel(
                0,
                VectorKernelInput::from_eval_input(ExpressionEvalInput {
                    params: ctx.query.params.as_ref(),
                    columns: output,
                })
                .with_count(1),
                ctx.query,
                selection,
            )?
        } else {
            1
        };
        destroy_aggregate_local(local)?;
        local.emitted = true;
        if selected_count == 0 {
            output.try_set_cardinality(0)?;
            Ok(TransformFlushPoll::Done)
        } else {
            Ok(TransformFlushPoll::Output)
        }
    }

    pub(crate) fn finish_global(
        &self,
        _ctx: &mut OperatorFinishContext,
        _global: &TransformGlobal,
    ) -> Result<TransformFinishPoll> {
        Ok(TransformFinishPoll::Done)
    }
}

fn state_buffer_words(total_size: usize) -> usize {
    total_size.div_ceil(size_of::<u64>()).max(1)
}

fn initialize_state_buffer(
    layout: &AggregateStateLayout,
    objects: &[crate::operators::aggregate::aggregate_object::AggregateObject],
    state_buffer: &mut Vec<u64>,
    allocator: Arc<dyn paro_common::allocator::Allocator>,
) -> Result<()> {
    let buffer_bytes = state_buffer
        .len()
        .checked_mul(size_of::<u64>())
        .ok_or_else(|| paro_error::internal("streaming aggregate state buffer size overflow"))?;
    if buffer_bytes < layout.total_size() {
        return Err(paro_error::internal(format!(
            "streaming aggregate state buffer too small: required={}, actual={}",
            layout.total_size(),
            buffer_bytes
        )));
    }
    let addresses = single_state_addresses(state_buffer.as_mut_ptr() as *mut u8, allocator)?;
    initialize_states(layout, objects, &addresses, 1)
}

fn single_state_addresses(
    base_ptr: *mut u8,
    allocator: Arc<dyn paro_common::allocator::Allocator>,
) -> Result<Vector> {
    repeated_state_addresses(base_ptr, 1, allocator)
}

fn repeated_state_addresses(
    base_ptr: *mut u8,
    count: usize,
    allocator: Arc<dyn paro_common::allocator::Allocator>,
) -> Result<Vector> {
    let mut addresses = Vector::try_new(LogicalType::BigInt, count.max(1), allocator)?;
    addresses.set_count(count);
    unsafe {
        let ptrs = addresses.flat_data_mut::<*mut u8>();
        for idx in 0..count {
            *ptrs.add(idx) = base_ptr;
        }
    }
    Ok(addresses)
}

fn prepare_empty_payload(
    payload: &mut Chunk,
    count: usize,
    allocator: Arc<dyn paro_common::allocator::Allocator>,
) -> Result<()> {
    if payload.column_count() != 0 || payload.capacity() < count.max(1) {
        *payload = Chunk::try_initialize(&[], count.max(1), allocator)?;
    }
    payload.try_set_cardinality(count)?;
    Ok(())
}

pub(crate) fn destroy_aggregate_local(local: &mut StreamingAggregateTransformLocal) -> Result<()> {
    if local.destroyed {
        return Ok(());
    }
    let addresses = single_state_addresses(
        local.state_buffer.as_mut_ptr() as *mut u8,
        local.arena_allocator.get_allocator().clone(),
    )?;
    let mut input_data = AggregateInputData::new(
        None,
        &mut local.arena_allocator,
        AggregateCombineType::PreserveInput,
    );
    destroy_states(&local.aggregate_objects, &mut input_data, &addresses, 1)?;
    local.destroyed = true;
    Ok(())
}

impl Drop for StreamingAggregateTransformLocal {
    fn drop(&mut self) {
        let _ = destroy_aggregate_local(self);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_streaming_aggregate_payload_grows_for_large_filtered_chunks() {
        let allocator = paro_common::test_utils::test_allocator();
        let mut payload = Chunk::try_initialize(&[], VECTOR_SIZE, allocator.clone())
            .expect("empty payload chunk");

        prepare_empty_payload(&mut payload, VECTOR_SIZE + 1, allocator)
            .expect("empty payload should grow beyond the default vector size");

        assert_eq!(payload.size(), VECTOR_SIZE + 1);
        assert!(payload.capacity() >= VECTOR_SIZE + 1);
        assert_eq!(payload.column_count(), 0);
    }
}
