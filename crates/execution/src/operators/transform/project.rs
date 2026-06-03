// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_planner::expression::Expression;

use crate::expression_executor::executor::{ExpressionExecutor, VectorKernelInput};
use crate::physical::specs::ProjectSpec;
use crate::runtime::context::{OperatorCallContext, OperatorFinishContext, PipelineInitContext};
use crate::runtime::state::{TransformGlobal, TransformLocal};
use crate::runtime::transform::{TransformFinishPoll, TransformFlushPoll, TransformPoll};
use crate::runtime::ExpressionEvalInput;

#[derive(Debug, Clone)]
pub struct ProjectTransformExec {
    pub spec: ProjectSpec,
}

#[derive(Debug, Default)]
pub struct ProjectTransformGlobal;

#[derive(Debug, Default)]
pub struct ProjectTransformLocal {
    executor: Option<ExpressionExecutor>,
}

impl ProjectTransformExec {
    pub(crate) fn create_global(&self, _ctx: &mut PipelineInitContext) -> Result<TransformGlobal> {
        Ok(TransformGlobal::Project(Arc::new(ProjectTransformGlobal)))
    }

    pub(crate) fn create_local(
        &self,
        ctx: &mut PipelineInitContext,
        _global: &TransformGlobal,
    ) -> Result<TransformLocal> {
        Ok(TransformLocal::Project(ProjectTransformLocal {
            executor: Some(ExpressionExecutor::with_expressions_for_session(
                &self.spec.expressions,
                ctx.query.session.as_ref(),
            )),
        }))
    }

    pub(crate) fn transform(
        &self,
        ctx: &mut OperatorCallContext,
        _global: &TransformGlobal,
        local: &mut TransformLocal,
        input: &Chunk,
        output: &mut Chunk,
    ) -> Result<TransformPoll> {
        if input.is_empty() {
            *output = Chunk::try_init_empty(
                &project_output_types(&self.spec),
                output.allocator().clone(),
            )?;
            return Ok(TransformPoll::NeedMoreInput);
        }

        let TransformLocal::Project(local) = local else {
            return Err(paro_error::internal(
                "project transform local state mismatch",
            ));
        };
        let executor = local
            .executor
            .as_mut()
            .ok_or_else(|| paro_error::internal("project expression executor missing"))?;
        executor.execute_all_kernel(
            VectorKernelInput::from_eval_input(ExpressionEvalInput {
                params: ctx.query.params.as_ref(),
                columns: input,
            }),
            ctx.query,
            output,
        )?;
        Ok(TransformPoll::Output)
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

fn project_output_types(spec: &ProjectSpec) -> Vec<LogicalType> {
    spec.expressions
        .iter()
        .map(Expression::return_type)
        .collect()
}
