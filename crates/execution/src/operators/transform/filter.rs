// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use paro_common::allocator::MemoryTag;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_common::vector::VectorSelection;
use paro_function::scalar::FunctionExecContext;

use crate::expression_executor::executor::{ExpressionExecutor, VectorKernelInput};
use crate::physical::specs::FilterSpec;
use crate::runtime::context::{OperatorCallContext, OperatorFinishContext, PipelineInitContext};
use crate::runtime::state::{TransformGlobal, TransformLocal};
use crate::runtime::transform::{TransformFinishPoll, TransformFlushPoll, TransformPoll};
use crate::runtime::ExpressionEvalInput;

#[derive(Debug, Clone)]
pub struct FilterTransformExec {
    pub spec: FilterSpec,
}

#[derive(Debug, Default)]
pub struct FilterTransformGlobal;

#[derive(Debug, Default)]
pub struct FilterTransformLocal {
    executor: Option<ExpressionExecutor>,
    projection: Box<[usize]>,
    output_types: Option<Box<[LogicalType]>>,
}

impl FilterTransformExec {
    pub(crate) fn create_global(&self, _ctx: &mut PipelineInitContext) -> Result<TransformGlobal> {
        Ok(TransformGlobal::Filter(Arc::new(FilterTransformGlobal)))
    }

    pub(crate) fn create_local(
        &self,
        ctx: &mut PipelineInitContext,
        _global: &TransformGlobal,
    ) -> Result<TransformLocal> {
        Ok(TransformLocal::Filter(FilterTransformLocal {
            executor: Some(ExpressionExecutor::with_expressions_for_session(
                &self.spec.expressions,
                ctx.query.session.as_ref(),
            )),
            projection: self.spec.projection_map.to_vec().into_boxed_slice(),
            output_types: None,
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
            output.try_set_cardinality(0)?;
            return Ok(TransformPoll::NeedMoreInput);
        }

        let TransformLocal::Filter(local) = local else {
            return Err(paro_error::internal(
                "filter transform local state mismatch",
            ));
        };
        ensure_filter_output_types(local, input)?;

        if self.spec.expressions.is_empty() {
            reference_filter_output(
                input,
                output,
                &local.projection,
                local
                    .output_types
                    .as_deref()
                    .expect("filter output types initialized"),
            )?;
            return Ok(TransformPoll::Output);
        }
        if self.spec.expressions.len() != 1 {
            return Err(paro_error::not_implemented(
                "multi-predicate filter lowering should produce a single conjunction expression",
            ));
        }

        let executor = local
            .executor
            .as_mut()
            .ok_or_else(|| paro_error::internal("filter expression executor missing"))?;
        let mut scratch = ctx.scratch.expr();
        let selection =
            scratch.selection(input.size(), ctx.query.allocator(MemoryTag::BaseTable))?;
        let selected_count = executor.select_kernel(
            0,
            VectorKernelInput::from_eval_input(ExpressionEvalInput {
                params: ctx.query.params.as_ref(),
                columns: input,
            }),
            ctx.query,
            selection,
        )?;

        if selected_count == 0 {
            output.try_set_cardinality(0)?;
            return Ok(TransformPoll::NeedMoreInput);
        }
        if selected_count == input.size() {
            reference_filter_output(
                input,
                output,
                &local.projection,
                local
                    .output_types
                    .as_deref()
                    .expect("filter output types initialized"),
            )?;
            return Ok(TransformPoll::Output);
        }

        output.try_reference_selection(
            input,
            &local.projection,
            VectorSelection::from(&*selection),
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

fn ensure_filter_output_types(local: &mut FilterTransformLocal, input: &Chunk) -> Result<()> {
    if local.output_types.is_some() {
        return Ok(());
    }
    let output_len = if local.projection.is_empty() {
        input.column_count()
    } else {
        local.projection.len()
    };
    let mut output_types = Vec::with_capacity(output_len);
    let input_types = input.types();
    for output_idx in 0..output_len {
        let input_idx = filter_input_column(&local.projection, output_idx);
        let Some(data_type) = input_types.get(input_idx) else {
            return Err(paro_error::internal(format!(
                "filter projection column {input_idx} is out of bounds for input with {} columns",
                input.column_count()
            )));
        };
        output_types.push(data_type.clone());
    }
    local.output_types = Some(output_types.into_boxed_slice());
    Ok(())
}

#[inline]
fn filter_input_column(projection: &[usize], output_idx: usize) -> usize {
    if projection.is_empty() {
        output_idx
    } else {
        projection[output_idx]
    }
}

fn reference_filter_output(
    input: &Chunk,
    output: &mut Chunk,
    projection: &[usize],
    output_types: &[LogicalType],
) -> Result<()> {
    if output.column_count() != output_types.len() {
        *output =
            Chunk::try_initialize(output_types, input.size().max(1), input.allocator().clone())?;
    }
    if projection.is_empty() {
        output.reference(input);
    } else {
        output.reference_columns(input, projection);
    }
    Ok(())
}
