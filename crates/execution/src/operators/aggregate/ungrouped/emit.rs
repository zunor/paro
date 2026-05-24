// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_function::aggregate::{AggregateCombineType, AggregateInputData};

use crate::operators::aggregate::aggregate_kernel::finalize_states;
use crate::operators::output::ensure_source_output;
use crate::physical::specs::AggregateSpec;
use crate::runtime::breaker::{
    single_state_addresses, AggregateHandle, AggregateRuntimeState, HandleRef,
};
use crate::runtime::context::{OperatorCallContext, PipelineInitContext};
use crate::runtime::source::SourcePoll;
use crate::runtime::state::{
    BreakerHandleGlobal, SourceGlobal, SourceLocal, UngroupedAggregateEmitSourceLocal,
};

#[derive(Debug, Clone)]
pub struct UngroupedAggregateEmitSourceExec {
    pub handle: HandleRef<AggregateHandle>,
    pub spec: AggregateSpec,
}

impl UngroupedAggregateEmitSourceExec {
    pub(crate) fn create_global(&self, ctx: &mut PipelineInitContext) -> Result<SourceGlobal> {
        Ok(SourceGlobal::UngroupedAggregateEmit(Arc::new(
            BreakerHandleGlobal {
                handle: ctx.handles.get(self.handle)?,
            },
        )))
    }

    pub(crate) fn create_local(
        &self,
        _ctx: &mut PipelineInitContext,
        _global: &SourceGlobal,
    ) -> Result<SourceLocal> {
        Ok(SourceLocal::UngroupedAggregateEmit(
            UngroupedAggregateEmitSourceLocal::default(),
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
        let SourceGlobal::UngroupedAggregateEmit(global) = global else {
            return Err(paro_error::internal(
                "ungrouped aggregate emit source global state mismatch",
            ));
        };
        if !global.handle.is_finalized() {
            return Err(paro_error::internal(
                "ungrouped aggregate emit source polled before handle was finalized",
            ));
        }
        let SourceLocal::UngroupedAggregateEmit(local) = local else {
            return Err(paro_error::internal(
                "ungrouped aggregate emit source local state mismatch",
            ));
        };
        if local.emitted {
            output.try_set_cardinality(0)?;
            return Ok(SourcePoll::Finished);
        }
        if local.state.is_none() {
            let Some(state) = global.handle.take_state()? else {
                return Ok(SourcePoll::Finished);
            };
            let AggregateRuntimeState::Ungrouped(state) = state else {
                return Err(paro_error::internal(
                    "aggregate handle does not contain ungrouped aggregate state",
                ));
            };
            local.state = Some(state);
        }
        ensure_source_output(output, &self.spec.output_types, 1)?;
        let state = local.state.as_mut().ok_or_else(|| {
            paro_error::internal("ungrouped aggregate emit source did not load state")
        })?;
        let addresses = single_state_addresses(
            state.state_buffer.as_mut_ptr() as *mut u8,
            state.arena_allocator.get_allocator().clone(),
        )?;
        let mut input_data = AggregateInputData::new(
            None,
            &mut state.arena_allocator,
            AggregateCombineType::PreserveInput,
        );
        finalize_states(
            &state.aggregate_objects,
            &mut input_data,
            &addresses,
            output,
            1,
        )?;
        state.destroy()?;
        local.emitted = true;
        Ok(SourcePoll::Output)
    }
}
