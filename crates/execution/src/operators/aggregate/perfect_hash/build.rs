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
    query_perfect_hash_memory, update_perfect_aggregate_table,
};
use crate::operators::aggregate::perfect_hash::finalize::prepare_parallel_perfect_merge;
use crate::operators::aggregate::post_reduction::{PostAggregateInputRollup, PostAggregateReducer};
use crate::physical::specs::AggregateSpec;
use crate::runtime::breaker::{
    AggregateBuildCompactionReclaimer, AggregateFinalizedStateReclaimer, AggregateHandle,
    AggregateRuntimeState, HandleRef, PerfectHashAggregateRuntimeState,
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
}

impl PerfectHashAggregateSinkExec {
    pub(crate) fn create_global(&self, ctx: &mut PipelineInitContext) -> Result<SinkGlobal> {
        self.spec.verify_post_reduction()?;
        let handle = ctx.handles.get(self.handle)?;
        handle.initialize(AggregateRuntimeState::Perfect(
            PerfectHashAggregateRuntimeState {
                build_table: None,
                finalized_table: None,
                pending_tables: Vec::new(),
                input_rollup: PostAggregateInputRollup::try_new(&self.spec, ctx.query)?,
            },
        ))?;
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
                projection_executor: (!self.spec.projection_exprs.is_empty()).then(|| {
                    ExpressionExecutor::with_expressions_for_session(
                        &self.spec.projection_exprs,
                        ctx.query.session.as_ref(),
                    )
                }),
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
                    query_perfect_hash_memory(ctx.query),
                )?),
                input_rollup: PostAggregateInputRollup::try_new(&self.spec, ctx.query)?,
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
            &*input
        };
        let table = local.table.as_mut().ok_or_else(|| {
            paro_error::internal("perfect aggregate local table was already merged")
        })?;
        if let Some(rollup) = local.input_rollup.as_mut() {
            rollup.update(payload)?;
        }
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
        let Some(local_table) = local.table.take() else {
            return Ok(MergePoll::Done);
        };
        let mut local_rollup = local.input_rollup.take();
        global.handle.with_state_mut(|state| {
            let AggregateRuntimeState::Perfect(global) = state else {
                return Err(paro_error::internal(
                    "aggregate handle does not contain perfect aggregate state",
                ));
            };
            if global.finalized_table.is_some() {
                return Err(paro_error::internal(
                    "perfect aggregate local table merged after finalization",
                ));
            }
            match (global.input_rollup.as_mut(), local_rollup.as_mut()) {
                (Some(target), Some(source)) => target.combine_from(source)?,
                (None, None) => {}
                _ => {
                    return Err(paro_error::internal(
                        "perfect aggregate input-rollup local/global state mismatch",
                    ));
                }
            }
            if global.build_table.is_none() {
                global.build_table = Some(local_table);
                return Ok(());
            }
            global.pending_tables.push(local_table);
            Ok(())
        })?;
        Ok(MergePoll::Done)
    }

    pub(crate) fn prepare_finish(
        &self,
        ctx: &mut OperatorFinishContext,
        global: &SinkGlobal,
    ) -> Result<PrepareFinishPoll> {
        let SinkGlobal::PerfectHashAggregate(global) = global else {
            return Err(paro_error::internal(
                "perfect aggregate sink global state mismatch",
            ));
        };
        let rollup = global.handle.with_state_mut(|state| {
            let AggregateRuntimeState::Perfect(state) = state else {
                return Err(paro_error::internal(
                    "aggregate handle does not contain perfect aggregate state",
                ));
            };
            Ok(state.input_rollup.take())
        })?;
        if let Some(rollup) = rollup {
            global
                .handle
                .set_post_reduction_values(rollup.finish(ctx.query)?)?;
        }
        Ok(PrepareFinishPoll::Done)
    }

    pub(crate) fn finish_work(
        &self,
        ctx: &mut OperatorFinishContext,
        global: &SinkGlobal,
    ) -> Result<FinishWork> {
        let SinkGlobal::PerfectHashAggregate(global) = global else {
            return Err(paro_error::internal(
                "perfect aggregate sink global state mismatch",
            ));
        };
        let post_state_filter = self
            .spec
            .post_reduction
            .as_ref()
            .and_then(|_| global.handle.post_reduction_values_if_ready())
            .map(|values| super::emit::compile_post_state_filter(&self.spec, values))
            .transpose()?
            .flatten();
        prepare_parallel_perfect_merge(
            global.handle.clone(),
            ctx.query.session.number_of_threads().max(1),
            if post_state_filter.is_some() {
                post_state_filter
            } else if self.spec.post_reduction.is_some() {
                // The reduction must observe every finalized group. A perfect
                // state preselection can omit non-matching source-only slots
                // during parallel merge, so it belongs exclusively to the
                // later emit pass when a post reduction is present.
                None
            } else {
                super::emit::compile_state_filter(&self.spec)?
            },
            query_perfect_hash_memory(ctx.query),
        )
    }

    pub(crate) fn finish(
        &self,
        ctx: &mut OperatorFinishContext,
        global: &SinkGlobal,
    ) -> Result<FinishPoll> {
        let SinkGlobal::PerfectHashAggregate(global) = global else {
            return Err(paro_error::internal(
                "perfect aggregate sink global state mismatch",
            ));
        };
        let values_ready = global.handle.post_reduction_values_if_ready().is_some();
        let mut post_reducer = if values_ready {
            None
        } else {
            self.spec
                .post_reduction
                .as_ref()
                .map(|spec| PostAggregateReducer::try_new(spec, ctx.query))
                .transpose()?
        };
        let mut scanned_reduction = false;
        global.handle.with_state_mut(|state| {
            let AggregateRuntimeState::Perfect(global) = state else {
                return Err(paro_error::internal(
                    "aggregate handle does not contain perfect aggregate state",
                ));
            };
            if !global.pending_tables.is_empty() {
                return Err(paro_error::internal(format!(
                    "perfect aggregate finish left {} local tables unmerged",
                    global.pending_tables.len()
                )));
            }
            if global.finalized_table.is_none() {
                let table = global.build_table.take().map_or_else(
                    || {
                        create_perfect_aggregate_table(
                            &self.spec,
                            ctx.query.allocator(MemoryTag::HashTable),
                            query_perfect_hash_memory(ctx.query),
                        )
                    },
                    Ok,
                )?;
                global.finalized_table = Some(
                    crate::operators::aggregate::perfect_aggregate_hashtable::FinalizedPerfectAggregateTable::complete(table),
                );
            }
            if global.build_table.is_some() {
                return Err(paro_error::internal(
                    "perfect aggregate finish retained a mutable table after finalization",
                ));
            }
            if let Some(reducer) = post_reducer.as_mut() {
                let table = global.finalized_table.as_mut().ok_or_else(|| {
                    paro_error::internal("post reduction has no finalized perfect aggregate table")
                })?;
                let mut scratch =
                    crate::operators::aggregate::perfect_aggregate_hashtable::PerfectAggregateScanScratch::try_new(
                        &self.spec.output_types[self.spec.grouping_key_count
                            ..self.spec.grouping_key_count + self.spec.aggregates.len()],
                        VECTOR_SIZE,
                        ctx.query.allocator(MemoryTag::BaseTable),
                    )?;
                table.visit_all_finalized_aggregates(&mut scratch, |aggregates| {
                    reducer.consume(aggregates)
                })?;
                scanned_reduction = true;
            }
            Ok(())
        })?;
        if scanned_reduction {
            let values = post_reducer
                .expect("scanned post reduction owns its reducer")
                .finish(ctx.query)?;
            if !values_ready {
                global.handle.set_post_reduction_values(values)?;
            }
        }
        global.handle.mark_finalized();
        global.handle.enable_state_reclaim();
        Ok(FinishPoll::Done)
    }
}
