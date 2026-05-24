// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::atomic::Ordering;
use std::sync::Arc;

use paro_common::chunk::Chunk;
use paro_common::error::Result;

use crate::physical::specs::ChunkScanSpec;
use crate::runtime::context::{OperatorCallContext, PipelineInitContext};
use crate::runtime::source::SourcePoll;
use crate::runtime::state::{ChunkSourceGlobal, ChunkSourceLocal, SourceGlobal, SourceLocal};

#[derive(Debug, Clone)]
pub struct ChunkSourceExec {
    pub spec: ChunkScanSpec,
}

impl ChunkSourceExec {
    pub(crate) fn create_global(&self, _ctx: &mut PipelineInitContext) -> Result<SourceGlobal> {
        Ok(SourceGlobal::Chunk(Arc::new(ChunkSourceGlobal {
            chunks: Arc::clone(&self.spec.chunks),
            next_chunk: Default::default(),
        })))
    }

    pub(crate) fn create_local(
        &self,
        _ctx: &mut PipelineInitContext,
        global: &SourceGlobal,
    ) -> Result<SourceLocal> {
        global.chunk()?;
        Ok(SourceLocal::Chunk(ChunkSourceLocal))
    }

    pub(crate) fn poll_next(
        &self,
        _ctx: &mut OperatorCallContext,
        global: &SourceGlobal,
        _local: &mut SourceLocal,
        output: &mut Chunk,
    ) -> Result<SourcePoll> {
        let global = global.chunk()?;
        let idx = global.next_chunk.fetch_add(1, Ordering::AcqRel);
        let Some(chunk) = global.chunks.get(idx) else {
            return Ok(SourcePoll::Finished);
        };
        output.reference(chunk);
        Ok(SourcePoll::Output)
    }
}
