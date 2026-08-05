// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::vector::VECTOR_SIZE;
use paro_function::scalar::FunctionExecContext;

use crate::expression_executor::executor::{ExpressionExecutor, VectorKernelInput};
use crate::operators::aggregate::output_filter::copy_selected_rows;
use crate::operators::output::ensure_source_output;
use crate::physical::specs::AggregateSpec;
use crate::runtime::breaker::{
    AggregateBuildCompactionReclaimer, AggregateFinalizedStateReclaimer, AggregateHandle,
    AggregateRuntimeState, HandleRef,
};
use crate::runtime::context::{OperatorCallContext, PipelineInitContext};
use crate::runtime::source::SourcePoll;
use crate::runtime::state::{
    BreakerHandleGlobal, PerfectHashAggregateEmitSourceLocal, SourceGlobal, SourceLocal,
};
use crate::runtime::ExpressionEvalInput;

#[derive(Debug, Clone)]
pub struct PerfectHashAggregateEmitSourceExec {
    pub handle: HandleRef<AggregateHandle>,
    pub spec: AggregateSpec,
}

impl PerfectHashAggregateEmitSourceExec {
    pub(crate) fn create_global(&self, ctx: &mut PipelineInitContext) -> Result<SourceGlobal> {
        Ok(SourceGlobal::PerfectHashAggregateEmit(Arc::new(
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
        let mut local = PerfectHashAggregateEmitSourceLocal::default();
        if !self.spec.having_filter.is_empty() {
            if self.spec.having_filter.len() != 1 {
                return Err(paro_error::internal(
                    "aggregate HAVING lowering requires one normalized predicate",
                ));
            }
            local.having_executor = Some(ExpressionExecutor::with_expressions_for_session(
                &self.spec.having_filter,
                ctx.query.session.as_ref(),
            ));
            local.having_selection = Some(paro_common::vector::SelectionVector::try_with_capacity(
                VECTOR_SIZE,
                ctx.query
                    .allocator(paro_common::allocator::MemoryTag::BaseTable),
            )?);
            local.having_columns = (self.spec.grouping_key_count
                ..self.spec.grouping_key_count + self.spec.aggregates.len())
                .collect();
        }
        Ok(SourceLocal::PerfectHashAggregateEmit(local))
    }

    pub(crate) fn poll_next(
        &self,
        ctx: &mut OperatorCallContext,
        global: &SourceGlobal,
        local: &mut SourceLocal,
        output: &mut Chunk,
    ) -> Result<SourcePoll> {
        ctx.cancel.check()?;
        let SourceGlobal::PerfectHashAggregateEmit(global) = global else {
            return Err(paro_error::internal(
                "perfect aggregate emit source global state mismatch",
            ));
        };
        if !global.handle.is_finalized() {
            return Err(paro_error::internal(
                "perfect aggregate emit source polled before handle was finalized",
            ));
        }
        let SourceLocal::PerfectHashAggregateEmit(local) = local else {
            return Err(paro_error::internal(
                "perfect aggregate emit source local state mismatch",
            ));
        };
        if local.table.is_none() {
            ctx.query.memory.unregister_reclaimer_by_name(
                &AggregateBuildCompactionReclaimer::name_for(&global.handle),
            );
            ctx.query.memory.unregister_reclaimer_by_name(
                &AggregateFinalizedStateReclaimer::name_for(&global.handle),
            );
            let Some(state) = global.handle.take_state()? else {
                return Ok(SourcePoll::Finished);
            };
            let AggregateRuntimeState::Perfect(state) = state else {
                return Err(paro_error::internal(
                    "aggregate handle does not contain perfect aggregate state",
                ));
            };
            local.table = Some(state.table);
        }
        ensure_source_output(output, &self.spec.output_types, VECTOR_SIZE)?;
        let table = local.table.as_mut().ok_or_else(|| {
            paro_error::internal("perfect aggregate emit source did not load table")
        })?;
        if let (Some(executor), Some(selection)) = (
            local.having_executor.as_mut(),
            local.having_selection.as_mut(),
        ) {
            let scratch = local.filtered_chunk.get_or_insert(Chunk::try_new(
                ctx.query
                    .allocator(paro_common::allocator::MemoryTag::BaseTable),
            )?);
            ensure_source_output(scratch, &self.spec.output_types, VECTOR_SIZE)?;
            while table.scan(&mut local.position, scratch)? {
                let aggregate_types = &self.spec.output_types[self.spec.grouping_key_count
                    ..self.spec.grouping_key_count + self.spec.aggregates.len()];
                let mut aggregate_view =
                    Chunk::try_init_empty(aggregate_types, scratch.allocator().clone())?;
                aggregate_view.reference_columns(scratch, &local.having_columns);
                let selected_count = executor.select_kernel(
                    0,
                    VectorKernelInput::from_eval_input(ExpressionEvalInput {
                        params: ctx.query.params.as_ref(),
                        columns: &aggregate_view,
                    })
                    .with_count(scratch.size()),
                    ctx.query,
                    selection,
                )?;
                if selected_count > 0 {
                    copy_selected_rows(scratch, output, selection, selected_count)?;
                    return Ok(SourcePoll::Output);
                }
            }
        } else if table.scan(&mut local.position, output)? {
            return Ok(SourcePoll::Output);
        }
        output.try_set_cardinality(0)?;
        Ok(SourcePoll::Finished)
    }
}
