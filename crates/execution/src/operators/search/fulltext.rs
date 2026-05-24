// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_common::chunk::Chunk;
use paro_common::error::Result;

use crate::operators::search::source::{
    create_search_global, create_search_local, poll_search_next, SearchSourceSpecRef,
};
use crate::physical::specs::FullTextSearchSpec;
use crate::runtime::context::{OperatorCallContext, PipelineInitContext};
use crate::runtime::source::SourcePoll;
use crate::runtime::state::{SourceGlobal, SourceLocal};

#[derive(Debug, Clone)]
pub struct FullTextSearchSourceExec {
    pub spec: FullTextSearchSpec,
}

impl FullTextSearchSourceExec {
    pub(crate) fn create_global(&self, ctx: &mut PipelineInitContext) -> Result<SourceGlobal> {
        create_search_global(ctx, SearchSourceSpecRef::FullText(&self.spec))
    }

    pub(crate) fn create_local(
        &self,
        ctx: &mut PipelineInitContext,
        global: &SourceGlobal,
    ) -> Result<SourceLocal> {
        create_search_local(ctx, global, SearchSourceSpecRef::FullText(&self.spec))
    }

    pub(crate) fn poll_next(
        &self,
        ctx: &mut OperatorCallContext,
        global: &SourceGlobal,
        local: &mut SourceLocal,
        output: &mut Chunk,
    ) -> Result<SourcePoll> {
        poll_search_next(ctx, global, local, output)
    }
}
