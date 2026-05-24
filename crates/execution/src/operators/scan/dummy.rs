// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};

use crate::physical::specs::DummyScanSpec;
use crate::runtime::context::{OperatorCallContext, PipelineInitContext};
use crate::runtime::source::SourcePoll;
use crate::runtime::state::{EmptySourceGlobal, EmptySourceLocal, SourceGlobal, SourceLocal};

#[derive(Debug, Clone)]
pub struct DummySourceExec {
    pub spec: DummyScanSpec,
}

impl DummySourceExec {
    pub(crate) fn create_global(&self, _ctx: &mut PipelineInitContext) -> Result<SourceGlobal> {
        Ok(SourceGlobal::Dummy(Arc::new(EmptySourceGlobal)))
    }

    pub(crate) fn create_local(
        &self,
        _ctx: &mut PipelineInitContext,
        _global: &SourceGlobal,
    ) -> Result<SourceLocal> {
        Ok(SourceLocal::Dummy(EmptySourceLocal::default()))
    }

    pub(crate) fn poll_next(
        &self,
        _ctx: &mut OperatorCallContext,
        _global: &SourceGlobal,
        local: &mut SourceLocal,
        output: &mut Chunk,
    ) -> Result<SourcePoll> {
        let SourceLocal::Dummy(local) = local else {
            return Err(paro_error::internal("dummy source local state mismatch"));
        };
        if local.emitted {
            return Ok(SourcePoll::Finished);
        }
        output.try_set_cardinality(1)?;
        local.emitted = true;
        Ok(SourcePoll::Output)
    }
}
