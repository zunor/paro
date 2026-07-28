// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};

use crate::operators::scan::expression_rows::poll_expression_rows;
use crate::physical::specs::ValuesSpec;
use crate::runtime::context::{OperatorCallContext, PipelineInitContext};
use crate::runtime::source::SourcePoll;
use crate::runtime::state::{SourceGlobal, SourceLocal, ValuesSourceGlobal, ValuesSourceLocal};

#[derive(Debug, Clone)]
pub struct ValuesSourceExec {
    pub spec: ValuesSpec,
}

impl ValuesSourceExec {
    pub(crate) fn create_global(&self, _ctx: &mut PipelineInitContext) -> Result<SourceGlobal> {
        Ok(SourceGlobal::Values(Arc::new(ValuesSourceGlobal {
            row_count: self.spec.expressions.len(),
        })))
    }

    pub(crate) fn create_local(
        &self,
        ctx: &mut PipelineInitContext,
        global: &SourceGlobal,
    ) -> Result<SourceLocal> {
        global.values()?;
        Ok(SourceLocal::Values(ValuesSourceLocal::try_new(
            ctx,
            &self.spec.expressions,
            &self.spec.output_types,
        )?))
    }

    pub(crate) fn poll_next(
        &self,
        ctx: &mut OperatorCallContext,
        _global: &SourceGlobal,
        local: &mut SourceLocal,
        output: &mut Chunk,
    ) -> Result<SourcePoll> {
        let SourceLocal::Values(local) = local else {
            return Err(paro_error::internal("values source local state mismatch"));
        };
        poll_expression_rows(ctx, &self.spec.expressions, local, output)
    }
}
