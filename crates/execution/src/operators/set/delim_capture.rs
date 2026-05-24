// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use paro_common::allocator::MemoryTag;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_common::vector::{SelectionVector, VECTOR_SIZE};
use paro_function::scalar::FunctionExecContext;
use paro_planner::expression::Expression;

use crate::expression_executor::executor::ExpressionExecutor;
use crate::physical::properties::RequiredProperties;
use crate::runtime::breaker::{DelimHandle, HandleRef};
use crate::runtime::context::{OperatorCallContext, OperatorFinishContext, PipelineInitContext};
use crate::runtime::sink::{FinishPoll, FinishWork, MergePoll, PrepareFinishPoll, SinkPoll};
use crate::runtime::state::{DelimCaptureSinkGlobal, DelimCaptureSinkLocal, SinkGlobal, SinkLocal};
use crate::runtime::ExpressionEvalInput;

#[derive(Debug, Clone)]
pub struct DelimCaptureSinkExec {
    pub handle: HandleRef<DelimHandle>,
    pub duplicate_keys: Box<[Expression]>,
    pub cached_outer: Option<HandleRef<DelimHandle>>,
    pub required: RequiredProperties,
}

pub fn delim_key_types(expressions: &[Expression]) -> Vec<LogicalType> {
    expressions
        .iter()
        .map(Expression::return_type)
        .collect::<Vec<_>>()
}

impl DelimCaptureSinkExec {
    pub(crate) fn create_global(&self, ctx: &mut PipelineInitContext) -> Result<SinkGlobal> {
        Ok(SinkGlobal::DelimCapture(Arc::new(DelimCaptureSinkGlobal {
            values: ctx.handles.get(self.handle)?,
            cached_outer: self
                .cached_outer
                .map(|handle| ctx.handles.get(handle))
                .transpose()?,
        })))
    }

    pub(crate) fn create_local(
        &self,
        ctx: &mut PipelineInitContext,
        _global: &SinkGlobal,
    ) -> Result<SinkLocal> {
        let key_types = delim_key_types(&self.duplicate_keys);
        Ok(SinkLocal::DelimCapture(DelimCaptureSinkLocal {
            key_executor: ExpressionExecutor::with_expressions(&self.duplicate_keys),
            key_chunk: Chunk::try_initialize(
                &key_types,
                VECTOR_SIZE,
                ctx.query.allocator(MemoryTag::BaseTable),
            )?,
            value_chunks: Vec::new(),
            cached_outer_chunks: Vec::new(),
        }))
    }

    pub(crate) fn consume(
        &self,
        ctx: &mut OperatorCallContext,
        _global: &SinkGlobal,
        local: &mut SinkLocal,
        input: &mut Chunk,
    ) -> Result<SinkPoll> {
        ctx.cancel.check()?;
        if input.is_empty() {
            return Ok(SinkPoll::NeedMoreInput);
        }

        let SinkLocal::DelimCapture(local) = local else {
            return Err(paro_error::internal(
                "delim capture sink local state mismatch",
            ));
        };

        if self.duplicate_keys.is_empty() {
            local.key_chunk.try_set_cardinality(input.size())?;
        } else {
            local.key_executor.execute_all_into_with_input(
                ExpressionEvalInput {
                    params: ctx.query.params.as_ref(),
                    columns: input,
                },
                ctx.query,
                &mut local.key_chunk,
            )?;
        }

        let mut captured_values = local.key_chunk.clone();
        if self.cached_outer.is_some() {
            local
                .cached_outer_chunks
                .push(input.handoff_referencing_vectors());
        }
        local.value_chunks.push(captured_values.take_owned());
        Ok(SinkPoll::NeedMoreInput)
    }

    pub(crate) fn merge_local(
        &self,
        _ctx: &mut OperatorCallContext,
        global: &SinkGlobal,
        local: &mut SinkLocal,
    ) -> Result<MergePoll> {
        let SinkGlobal::DelimCapture(global) = global else {
            return Err(paro_error::internal(
                "delim capture sink global state mismatch",
            ));
        };
        let SinkLocal::DelimCapture(local) = local else {
            return Err(paro_error::internal(
                "delim capture sink local state mismatch",
            ));
        };

        let mut unique_values = Vec::with_capacity(local.value_chunks.len());
        for chunk in local.value_chunks.drain(..) {
            let selection = global.values.select_new_keys(&chunk)?;
            if selection.is_empty() {
                continue;
            }
            let mut unique = chunk;
            if selection.len() != unique.size() {
                let selected_count = selection.len();
                let selection =
                    SelectionVector::try_from_indices(selection, unique.allocator().clone())?;
                unique.try_slice(&selection, selected_count)?;
            }
            unique_values.push(unique);
        }

        let mut no_cached_outer = Vec::new();
        global
            .values
            .append_capture(&mut unique_values, &mut no_cached_outer)?;
        if let Some(cached_outer) = global.cached_outer.as_ref() {
            let mut empty = Vec::new();
            cached_outer.append_capture(&mut local.cached_outer_chunks, &mut empty)?;
        } else {
            local.cached_outer_chunks.clear();
        }
        Ok(MergePoll::Done)
    }

    pub(crate) fn prepare_finish(
        &self,
        _ctx: &mut OperatorFinishContext,
        _global: &SinkGlobal,
    ) -> Result<PrepareFinishPoll> {
        Ok(PrepareFinishPoll::Done)
    }

    pub(crate) fn finish_work(
        &self,
        _ctx: &mut OperatorFinishContext,
        _global: &SinkGlobal,
    ) -> Result<FinishWork> {
        Ok(FinishWork::None)
    }

    pub(crate) fn finish(
        &self,
        _ctx: &mut OperatorFinishContext,
        global: &SinkGlobal,
    ) -> Result<FinishPoll> {
        let SinkGlobal::DelimCapture(global) = global else {
            return Err(paro_error::internal(
                "delim capture sink global state mismatch",
            ));
        };
        global.values.seal_capture()?;
        if let Some(cached_outer) = global.cached_outer.as_ref() {
            cached_outer.seal_capture()?;
        }
        Ok(FinishPoll::Done)
    }
}
