// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Perfect hash aggregate build sink operator.

use std::sync::Arc;

use paro_common::allocator::MemoryTag;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_common::vector::{SelectionVector, Vector, VECTOR_SIZE};
use paro_function::scalar::FunctionExecContext;

use crate::expression_executor::executor::ExpressionExecutor;
use crate::operators::aggregate::build_helpers::{
    create_perfect_aggregate_table, group_payload_refs, projected_payload_chunk,
    query_hash_table_memory, update_perfect_aggregate_table,
};
use crate::physical::properties::RequiredProperties;
use crate::physical::specs::AggregateSpec;
use crate::runtime::breaker::{
    AggregateHandle, AggregateRuntimeState, HandleRef, PerfectHashAggregateRuntimeState,
};
use crate::runtime::context::{OperatorCallContext, OperatorFinishContext, PipelineInitContext};
use crate::runtime::sink::{FinishPoll, FinishWork, MergePoll, PrepareFinishPoll, SinkPoll};
use crate::runtime::state::{
    BreakerHandleGlobal, PerfectHashAggregateSinkLocal, SinkGlobal, SinkLocal,
};

/// Sink operator that builds a perfect hash aggregate table.
#[derive(Debug, Clone)]
pub struct PerfectHashAggregateSinkExec {
    pub handle: HandleRef<AggregateHandle>,
    pub spec: AggregateSpec,
    pub required: RequiredProperties,
}

impl PerfectHashAggregateSinkExec {
    pub(crate) fn create_global(&self, ctx: &mut PipelineInitContext) -> Result<SinkGlobal> {
        let handle = ctx.handles.get(self.handle)?;
        let table = create_perfect_aggregate_table(
            &self.spec,
            ctx.query.allocator(MemoryTag::HashTable),
            query_hash_table_memory(ctx.query),
        )?;
        handle.initialize(AggregateRuntimeState::Perfect(
            PerfectHashAggregateRuntimeState { table },
        ))?;
        Ok(SinkGlobal::PerfectHashAggregate(Arc::new(
            BreakerHandleGlobal { handle },
        )))
    }

    pub(crate) fn create_local(
        &self,
        ctx: &mut PipelineInitContext,
        _global: &SinkGlobal,
    ) -> Result<SinkLocal> {
        Ok(SinkLocal::PerfectHashAggregate(
            PerfectHashAggregateSinkLocal {
                projection_executor: (!self.spec.projection_exprs.is_empty())
                    .then(|| ExpressionExecutor::with_expressions(&self.spec.projection_exprs)),
                payload_chunk: (!self.spec.projection_exprs.is_empty())
                    .then(|| {
                        Chunk::try_initialize(
                            &self.spec.payload_types,
                            VECTOR_SIZE,
                            ctx.query.allocator(MemoryTag::BaseTable),
                        )
                    })
                    .transpose()?,
                group_refs: group_payload_refs(&self.spec)?.into_boxed_slice(),
                addresses: Vector::try_new(
                    LogicalType::BigInt,
                    VECTOR_SIZE,
                    ctx.query.allocator(MemoryTag::HashTable),
                )?,
                new_groups: SelectionVector::try_with_capacity(
                    VECTOR_SIZE,
                    ctx.query.allocator(MemoryTag::HashTable),
                )?,
                table: Some(create_perfect_aggregate_table(
                    &self.spec,
                    ctx.query.allocator(MemoryTag::HashTable),
                    query_hash_table_memory(ctx.query),
                )?),
            },
        ))
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
        let SinkLocal::PerfectHashAggregate(local) = local else {
            return Err(paro_error::internal(
                "perfect aggregate sink local state mismatch",
            ));
        };
        let payload = if let Some(executor) = local.projection_executor.as_mut() {
            projected_payload_chunk(
                &self.spec,
                executor,
                &mut local.payload_chunk,
                input,
                ctx.query,
            )?
        } else {
            input
        };
        let table = local.table.as_mut().ok_or_else(|| {
            paro_error::internal("perfect aggregate local table was already merged")
        })?;
        update_perfect_aggregate_table(
            &self.spec,
            &local.group_refs,
            payload,
            table,
            &mut local.addresses,
            &mut local.new_groups,
        )?;
        Ok(SinkPoll::NeedMoreInput)
    }

    pub(crate) fn merge_local(
        &self,
        _ctx: &mut OperatorCallContext,
        global: &SinkGlobal,
        local: &mut SinkLocal,
    ) -> Result<MergePoll> {
        let SinkGlobal::PerfectHashAggregate(global) = global else {
            return Err(paro_error::internal(
                "perfect aggregate sink global state mismatch",
            ));
        };
        let SinkLocal::PerfectHashAggregate(local) = local else {
            return Err(paro_error::internal(
                "perfect aggregate sink local state mismatch",
            ));
        };
        let Some(mut local_table) = local.table.take() else {
            return Ok(MergePoll::Done);
        };
        global.handle.with_state_mut(|state| {
            let AggregateRuntimeState::Perfect(global) = state else {
                return Err(paro_error::internal(
                    "aggregate handle does not contain perfect aggregate state",
                ));
            };
            global.table.combine(&mut local_table)
        })?;
        local_table.destroy()?;
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
        _ctx: &mut OperatorFinishContext,
        global: &SinkGlobal,
    ) -> Result<FinishPoll> {
        let SinkGlobal::PerfectHashAggregate(global) = global else {
            return Err(paro_error::internal(
                "perfect aggregate sink global state mismatch",
            ));
        };
        global.handle.mark_finalized();
        Ok(FinishPoll::Done)
    }
}
