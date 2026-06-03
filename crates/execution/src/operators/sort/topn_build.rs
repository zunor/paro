// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use paro_common::allocator::MemoryTag;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::memory::MemoryAccountingClass;
use paro_common::memory::MemoryAccountingContext;
use paro_common::types::LogicalType;
use paro_common::vector::VECTOR_SIZE;
use paro_function::scalar::FunctionExecContext;

use crate::expression_executor::executor::{ExpressionExecutor, VectorKernelInput};
use crate::operators::sort::topn_heap::{TopNBoundaryValue, TopNHeap};
use crate::physical::properties::{MemoryClass, RequiredProperties};
use crate::physical::specs::TopNSpec;
use crate::runtime::breaker::{HandleRef, TopNHandle, TopNRuntimeState};
use crate::runtime::context::{
    OperatorCallContext, OperatorFinishContext, PipelineInitContext, QueryRuntimeContext,
};
use crate::runtime::sink::{
    FinishPoll, FinishTaskGroupRunner, FinishWork, MergePoll, PrepareFinishPoll, SinkPoll,
};
use crate::runtime::state::{BreakerHandleGlobal, SinkGlobal, SinkLocal, TopNBuildSinkLocal};
use crate::runtime::ExpressionEvalInput;

// ---------------------------------------------------------------------------
// TopN build sink
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct TopNBuildSinkExec {
    pub handle: HandleRef<TopNHandle>,
    pub spec: TopNSpec,
    pub required: RequiredProperties,
}

impl TopNBuildSinkExec {
    pub(crate) fn create_global(&self, ctx: &mut PipelineInitContext) -> Result<SinkGlobal> {
        let handle = ctx.handles.get(self.handle)?;
        let boundary = Arc::new(TopNBoundaryValue::new());
        let heap = TopNHeap::new_with_memory(
            self.spec.output_types.to_vec(),
            &self.spec.orders,
            self.spec.limit,
            self.spec.offset,
            topn_memory_context(ctx.query),
        );
        handle.initialize(TopNRuntimeState { heap, boundary })?;
        Ok(SinkGlobal::TopNBuild(Arc::new(BreakerHandleGlobal {
            handle,
        })))
    }

    pub(crate) fn create_local(
        &self,
        ctx: &mut PipelineInitContext,
        global: &SinkGlobal,
    ) -> Result<SinkLocal> {
        let SinkGlobal::TopNBuild(global) = global else {
            return Err(paro_error::internal(
                "topn build sink global state mismatch",
            ));
        };
        let order_exprs = self
            .spec
            .orders
            .iter()
            .map(|order| order.expression.clone())
            .collect::<Vec<_>>();
        let order_types = topn_order_types(&self.spec).into_boxed_slice();
        Ok(SinkLocal::TopNBuild(TopNBuildSinkLocal {
            heap: TopNHeap::new_with_memory(
                self.spec.output_types.to_vec(),
                &self.spec.orders,
                self.spec.limit,
                self.spec.offset,
                topn_memory_context(ctx.query),
            ),
            boundary: global.handle.boundary()?,
            order_executor: ExpressionExecutor::with_expressions_for_session(
                &order_exprs,
                ctx.query.session.as_ref(),
            ),
            sort_chunk: Chunk::try_initialize(
                order_types.as_ref(),
                VECTOR_SIZE,
                ctx.query.allocator(MemoryTag::BaseTable),
            )?,
            order_types,
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
        if input.is_empty() || self.spec.limit == 0 {
            return Ok(SinkPoll::NeedMoreInput);
        }
        let SinkLocal::TopNBuild(local) = local else {
            return Err(paro_error::internal("topn build sink local state mismatch"));
        };
        if local.sort_chunk.capacity() < input.size()
            || local.sort_chunk.column_count() != self.spec.orders.len()
        {
            local.sort_chunk = Chunk::try_initialize(
                local.order_types.as_ref(),
                input.size().max(1),
                ctx.memory.accounted_allocator_for(
                    MemoryTag::BaseTable,
                    MemoryAccountingClass::NonRevocable,
                ),
            )?;
        } else {
            local
                .sort_chunk
                .try_reset(local.sort_chunk.allocator().clone())?;
        }
        local.order_executor.execute_all_kernel(
            VectorKernelInput::from_eval_input(ExpressionEvalInput {
                params: ctx.query.params.as_ref(),
                columns: input,
            }),
            ctx.query,
            &mut local.sort_chunk,
        )?;
        local
            .heap
            .sink_with_sort_chunk(input, &local.sort_chunk, Some(&local.boundary))?;
        local.heap.reduce()?;
        Ok(SinkPoll::NeedMoreInput)
    }

    pub(crate) fn merge_local(
        &self,
        _ctx: &mut OperatorCallContext,
        global: &SinkGlobal,
        local: &mut SinkLocal,
    ) -> Result<MergePoll> {
        let SinkGlobal::TopNBuild(global) = global else {
            return Err(paro_error::internal(
                "topn build sink global state mismatch",
            ));
        };
        let SinkLocal::TopNBuild(local) = local else {
            return Err(paro_error::internal("topn build sink local state mismatch"));
        };
        global.handle.with_state_mut(|state| {
            state.heap.combine(&mut local.heap)?;
            state.heap.reduce()
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
        global: &SinkGlobal,
    ) -> Result<FinishWork> {
        let SinkGlobal::TopNBuild(global) = global else {
            return Err(paro_error::internal(
                "topn build sink global state mismatch",
            ));
        };
        let handle = global.handle.clone();
        Ok(FinishWork::Parallel(FinishTaskGroupRunner::group(
            "topn_seal",
            MemoryClass::Blocking,
            move |_ctx| handle.seal(),
        )))
    }

    pub(crate) fn finish(
        &self,
        _ctx: &mut OperatorFinishContext,
        global: &SinkGlobal,
    ) -> Result<FinishPoll> {
        let SinkGlobal::TopNBuild(global) = global else {
            return Err(paro_error::internal(
                "topn build sink global state mismatch",
            ));
        };
        if !global.handle.is_sealed() {
            global.handle.seal()?;
        }
        Ok(FinishPoll::Done)
    }
}

// ---------------------------------------------------------------------------
// TopN helpers
// ---------------------------------------------------------------------------

pub(crate) fn topn_memory_context(query: &QueryRuntimeContext) -> MemoryAccountingContext {
    let owner: Arc<dyn paro_common::memory::MemoryOwner> = query.memory.clone();
    MemoryAccountingContext::from_owner(
        owner,
        paro_common::memory::MemoryDomain::Host,
        MemoryTag::OrderBy,
        paro_common::memory::MemoryAccountingClass::Revocable,
    )
}

pub(crate) fn topn_order_types(spec: &TopNSpec) -> Vec<LogicalType> {
    spec.orders
        .iter()
        .map(|order| order.expression.return_type())
        .collect()
}
