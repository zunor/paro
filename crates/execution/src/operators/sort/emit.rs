// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::vector::VECTOR_SIZE;

use crate::operators::output::ensure_source_output;
use crate::result_type::SourceResultType;
use crate::runtime::breaker::{HandleRef, SortHandle};
use crate::runtime::context::{OperatorCallContext, PipelineInitContext};
use crate::runtime::source::SourcePoll;
use crate::runtime::state::{BreakerHandleGlobal, SortEmitSourceLocal, SourceGlobal, SourceLocal};

#[derive(Debug, Clone)]
pub struct SortEmitSourceExec {
    pub handle: HandleRef<SortHandle>,
}

impl SortEmitSourceExec {
    pub(crate) fn create_global(&self, ctx: &mut PipelineInitContext) -> Result<SourceGlobal> {
        Ok(SourceGlobal::SortEmit(Arc::new(BreakerHandleGlobal {
            handle: ctx.handles.get(self.handle)?,
        })))
    }

    pub(crate) fn create_local(
        &self,
        _ctx: &mut PipelineInitContext,
        _global: &SourceGlobal,
    ) -> Result<SourceLocal> {
        Ok(SourceLocal::SortEmit(SortEmitSourceLocal::default()))
    }

    pub(crate) fn poll_next(
        &self,
        ctx: &mut OperatorCallContext,
        global: &SourceGlobal,
        local: &mut SourceLocal,
        output: &mut Chunk,
    ) -> Result<SourcePoll> {
        ctx.cancel.check()?;
        let SourceGlobal::SortEmit(global) = global else {
            return Err(paro_error::internal(
                "sort emit source global state mismatch",
            ));
        };
        let SourceLocal::SortEmit(local) = local else {
            return Err(paro_error::internal(
                "sort emit source local state mismatch",
            ));
        };
        if local.state.is_none() {
            local.state = Some(global.handle.sealed_state()?);
        }
        let state = local
            .state
            .as_ref()
            .expect("sort emit source state initialized");
        if state.total_count == 0 {
            output.try_set_cardinality(0)?;
            return Ok(SourcePoll::Finished);
        }

        ensure_source_output(output, &state.output_types, VECTOR_SIZE)?;
        if let Some(single_run) = state.single_run.as_ref() {
            if local.current_position >= state.total_count {
                output.try_set_cardinality(0)?;
                return Ok(SourcePoll::Finished);
            }
            single_run.scan(
                output,
                local.current_position,
                state.sort.output_projection_columns(),
            )?;
            local.current_position += output.size();
            if output.is_empty() {
                return Ok(SourcePoll::Finished);
            }
            return Ok(SourcePoll::Output);
        }

        let (Some(merger), Some(merger_gstate)) =
            (state.merger.as_ref(), state.merger_gstate.as_ref())
        else {
            output.try_set_cardinality(0)?;
            return Ok(SourcePoll::Finished);
        };
        match merger.get_data(output, merger_gstate, &mut local.merger_lstate)? {
            SourceResultType::HaveMoreOutput => Ok(SourcePoll::Output),
            SourceResultType::Finished if !output.is_empty() => Ok(SourcePoll::Output),
            SourceResultType::Finished => Ok(SourcePoll::Finished),
            SourceResultType::Blocked => Err(paro_error::internal(
                "sorted run merger returned blocked without a wake registration",
            )),
        }
    }
}
