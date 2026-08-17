// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};

use crate::physical::properties::PropertyRepairKind;
use crate::runtime::context::{OperatorCallContext, OperatorFinishContext, PipelineInitContext};
use crate::runtime::state::{TransformGlobal, TransformLocal};
use crate::runtime::transform::{TransformFinishPoll, TransformFlushPoll, TransformPoll};

#[derive(Debug, Clone)]
pub struct PropertyRepairTransformExec {
    kind: PropertyRepairKind,
}

impl PropertyRepairTransformExec {
    pub(crate) fn try_new(kind: PropertyRepairKind) -> Result<Self> {
        match kind {
            PropertyRepairKind::BatchIndexAdapter | PropertyRepairKind::SingleTaskFallback => {
                Ok(Self { kind })
            }
            PropertyRepairKind::Sort(_) => {
                Err(paro_error::internal(
                    "blocking property repair must be lowered into breaker pipelines before program build",
                ))
            }
        }
    }

    pub(crate) fn create_global(&self, _ctx: &mut PipelineInitContext) -> Result<TransformGlobal> {
        Ok(TransformGlobal::PropertyRepair)
    }

    pub(crate) fn create_local(
        &self,
        _ctx: &mut PipelineInitContext,
        _global: &TransformGlobal,
    ) -> Result<TransformLocal> {
        Ok(TransformLocal::PropertyRepair)
    }

    pub(crate) fn transform(
        &self,
        ctx: &mut OperatorCallContext,
        _global: &TransformGlobal,
        _local: &mut TransformLocal,
        input: &Chunk,
        output: &mut Chunk,
    ) -> Result<TransformPoll> {
        ctx.cancel.check()?;
        match &self.kind {
            PropertyRepairKind::BatchIndexAdapter | PropertyRepairKind::SingleTaskFallback => {
                output.reference(input);
                Ok(TransformPoll::Output)
            }
            PropertyRepairKind::Sort(_) => Err(paro_error::internal(
                "blocking property repair must be lowered into breaker pipelines before runtime",
            )),
        }
    }

    pub(crate) fn flush(
        &self,
        _ctx: &mut OperatorCallContext,
        _global: &TransformGlobal,
        _local: &mut TransformLocal,
        _output: &mut Chunk,
    ) -> Result<TransformFlushPoll> {
        Ok(TransformFlushPoll::Done)
    }

    pub(crate) fn finish_global(
        &self,
        _ctx: &mut OperatorFinishContext,
        _global: &TransformGlobal,
    ) -> Result<TransformFinishPoll> {
        Ok(TransformFinishPoll::Done)
    }
}
