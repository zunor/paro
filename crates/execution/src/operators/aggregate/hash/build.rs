// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Hash aggregate build sink operator.

use std::sync::Arc;

use paro_common::allocator::MemoryTag;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_common::vector::{SelectionVector, Vector, VECTOR_SIZE};
use paro_function::scalar::FunctionExecContext;

use crate::expression_executor::executor::ExpressionExecutor;
use crate::operators::aggregate::build_helpers::{
    aggregate_objects, create_hash_aggregate_tables, group_payload_refs, has_aggregate_distinct,
    has_aggregate_ordered, normalized_grouping_sets, projected_payload_chunk,
    query_hash_table_memory, query_modifier_memory, update_hash_aggregate_tables,
};
use crate::operators::aggregate::distinct_helpers::{
    collect_distinct_rows, finalize_distinct_into_tables,
};
use crate::operators::aggregate::ordered_helpers::{
    collect_ordered_rows, empty_ordered_collectors, finalize_ordered_into_hash_tables,
    merge_ordered_collectors,
};
use crate::physical::properties::RequiredProperties;
use crate::physical::specs::AggregateSpec;
use crate::runtime::breaker::{
    AggregateHandle, AggregateRuntimeState, HandleRef, HashAggregateRuntimeState,
};
use crate::runtime::context::{OperatorCallContext, OperatorFinishContext, PipelineInitContext};
use crate::runtime::sink::{FinishPoll, FinishWork, MergePoll, PrepareFinishPoll, SinkPoll};
use crate::runtime::state::{
    BreakerHandleGlobal, HashAggregateBuildSinkLocal, SinkGlobal, SinkLocal,
};

/// Sink operator that builds hash aggregate tables (one per grouping set).
#[derive(Debug, Clone)]
pub struct HashAggregateBuildSinkExec {
    pub handle: HandleRef<AggregateHandle>,
    pub spec: AggregateSpec,
    pub required: RequiredProperties,
}

impl HashAggregateBuildSinkExec {
    pub(crate) fn create_global(&self, ctx: &mut PipelineInitContext) -> Result<SinkGlobal> {
        let handle = ctx.handles.get(self.handle)?;
        let tables = create_hash_aggregate_tables(
            &self.spec,
            ctx.query.allocator(MemoryTag::HashTable),
            query_hash_table_memory(ctx.query),
        )?;
        let group_refs = group_payload_refs(&self.spec)?;
        handle.initialize(AggregateRuntimeState::Hash(HashAggregateRuntimeState {
            tables,
            ordered_collectors: empty_ordered_collectors(&self.spec, &group_refs),
        }))?;
        Ok(SinkGlobal::HashAggregateBuild(Arc::new(
            BreakerHandleGlobal { handle },
        )))
    }

    pub(crate) fn create_local(
        &self,
        ctx: &mut PipelineInitContext,
        _global: &SinkGlobal,
    ) -> Result<SinkLocal> {
        let aggregate_objects = Arc::from(aggregate_objects(&self.spec)?.into_boxed_slice());
        Ok(SinkLocal::HashAggregateBuild(HashAggregateBuildSinkLocal {
            aggregate_objects: Arc::clone(&aggregate_objects),
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
            grouping_sets: normalized_grouping_sets(&self.spec)?
                .into_iter()
                .map(Vec::into_boxed_slice)
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            addresses: Vector::try_new(
                LogicalType::BigInt,
                VECTOR_SIZE,
                ctx.query.allocator(MemoryTag::HashTable),
            )?,
            new_groups: SelectionVector::try_with_capacity(
                VECTOR_SIZE,
                ctx.query.allocator(MemoryTag::HashTable),
            )?,
            tables: create_hash_aggregate_tables(
                &self.spec,
                ctx.query.allocator(MemoryTag::HashTable),
                query_hash_table_memory(ctx.query),
            )?,
            ordered_collectors: empty_ordered_collectors(
                &self.spec,
                &group_payload_refs(&self.spec)?,
            ),
            modifier_memory: query_modifier_memory(ctx.query),
            distinct_sets: aggregate_objects.iter().map(|_| None).collect(),
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
        let SinkLocal::HashAggregateBuild(local) = local else {
            return Err(paro_error::internal(
                "hash aggregate sink local state mismatch",
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
        if has_aggregate_distinct(&self.spec) {
            collect_distinct_rows(
                &self.spec,
                &local.aggregate_objects,
                payload,
                &local.group_refs,
                &local.modifier_memory,
                &mut local.distinct_sets,
            )?;
        }
        if has_aggregate_ordered(&self.spec) {
            collect_ordered_rows(
                &self.spec,
                &local.aggregate_objects,
                payload,
                &local.group_refs,
                &mut local.ordered_collectors,
            )?;
        }
        update_hash_aggregate_tables(
            &self.spec,
            &local.aggregate_objects,
            payload,
            &local.group_refs,
            &local.grouping_sets,
            &mut local.tables,
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
        let SinkGlobal::HashAggregateBuild(global) = global else {
            return Err(paro_error::internal(
                "hash aggregate sink global state mismatch",
            ));
        };
        let SinkLocal::HashAggregateBuild(local) = local else {
            return Err(paro_error::internal(
                "hash aggregate sink local state mismatch",
            ));
        };
        finalize_distinct_into_tables(
            &self.spec,
            &local.aggregate_objects,
            &local.group_refs,
            &local.grouping_sets,
            &local.modifier_memory,
            &mut local.distinct_sets,
            &mut local.tables,
        )?;
        global.handle.with_state_mut(|state| {
            let AggregateRuntimeState::Hash(global) = state else {
                return Err(paro_error::internal(
                    "aggregate handle does not contain hash aggregate state",
                ));
            };
            if global.tables.len() != local.tables.len() {
                return Err(paro_error::internal(format!(
                    "hash aggregate table count mismatch: global={} local={}",
                    global.tables.len(),
                    local.tables.len()
                )));
            }
            for (global_table, local_table) in global.tables.iter_mut().zip(local.tables.iter_mut())
            {
                global_table.combine(local_table)?;
            }
            merge_ordered_collectors(
                &mut global.ordered_collectors,
                &mut local.ordered_collectors,
            )?;
            Ok(())
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
        _ctx: &mut OperatorFinishContext,
        global: &SinkGlobal,
    ) -> Result<FinishPoll> {
        let SinkGlobal::HashAggregateBuild(global) = global else {
            return Err(paro_error::internal(
                "hash aggregate sink global state mismatch",
            ));
        };
        global.handle.with_state_mut(|state| {
            let AggregateRuntimeState::Hash(global) = state else {
                return Err(paro_error::internal(
                    "aggregate handle does not contain hash aggregate state",
                ));
            };
            let aggregate_objects = aggregate_objects(&self.spec)?;
            let group_refs = group_payload_refs(&self.spec)?;
            let grouping_sets = normalized_grouping_sets(&self.spec)?
                .into_iter()
                .map(Vec::into_boxed_slice)
                .collect::<Vec<_>>();
            finalize_ordered_into_hash_tables(
                &self.spec,
                &aggregate_objects,
                &group_refs,
                &grouping_sets,
                &mut global.ordered_collectors,
                &mut global.tables,
            )
        })?;
        global.handle.mark_finalized();
        Ok(FinishPoll::Done)
    }
}
