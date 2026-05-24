// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use paro_common::chunk::Chunk;
use paro_common::error::Result;

use crate::physical::specs::EmptyResultSpec;
use crate::runtime::context::{OperatorCallContext, PipelineInitContext};
use crate::runtime::source::SourcePoll;
use crate::runtime::state::{EmptySourceGlobal, EmptySourceLocal, SourceGlobal, SourceLocal};

#[derive(Debug, Clone)]
pub struct EmptySourceExec {
    pub spec: EmptyResultSpec,
}

impl EmptySourceExec {
    pub(crate) fn create_global(&self, _ctx: &mut PipelineInitContext) -> Result<SourceGlobal> {
        Ok(SourceGlobal::Empty(Arc::new(EmptySourceGlobal)))
    }

    pub(crate) fn create_local(
        &self,
        _ctx: &mut PipelineInitContext,
        _global: &SourceGlobal,
    ) -> Result<SourceLocal> {
        Ok(SourceLocal::Empty(EmptySourceLocal::default()))
    }

    pub(crate) fn poll_next(
        &self,
        _ctx: &mut OperatorCallContext,
        _global: &SourceGlobal,
        _local: &mut SourceLocal,
        _output: &mut Chunk,
    ) -> Result<SourcePoll> {
        Ok(SourcePoll::Finished)
    }
}
