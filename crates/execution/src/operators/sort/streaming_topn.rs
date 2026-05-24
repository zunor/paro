// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use paro_common::allocator::MemoryTag;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_function::scalar::FunctionExecContext;

use crate::expression_executor::executor::ExpressionExecutor;
use crate::operators::sort::topn_heap::TopNHeap;
use crate::physical::specs::TopNSpec;
use crate::runtime::context::{OperatorCallContext, OperatorFinishContext, PipelineInitContext};
use crate::runtime::state::{
    StreamingTopNTransformGlobal, StreamingTopNTransformLocal, TransformGlobal, TransformLocal,
};
use crate::runtime::transform::{TransformFinishPoll, TransformFlushPoll, TransformPoll};
use crate::runtime::ExpressionEvalInput;

#[derive(Debug, Clone)]
pub struct StreamingTopNTransformExec {
    pub spec: TopNSpec,
}

impl StreamingTopNTransformExec {
    pub(crate) fn create_global(&self, _ctx: &mut PipelineInitContext) -> Result<TransformGlobal> {
        if self.spec.orders.is_empty() && self.spec.limit != 0 {
            return Err(paro_error::not_implemented(
                "TOP N without ORDER BY should lower to streaming LIMIT",
            ));
        }
        Ok(TransformGlobal::StreamingTopN(Arc::new(
            StreamingTopNTransformGlobal,
        )))
    }

    pub(crate) fn create_local(
        &self,
        _ctx: &mut PipelineInitContext,
        _global: &TransformGlobal,
    ) -> Result<TransformLocal> {
        let order_exprs = self
            .spec
            .orders
            .iter()
            .map(|order| order.expression.clone())
            .collect::<Vec<_>>();
        Ok(TransformLocal::StreamingTopN(StreamingTopNTransformLocal {
            heap: TopNHeap::new(
                self.spec.output_types.to_vec(),
                &self.spec.orders,
                self.spec.limit,
                self.spec.offset,
            ),
            order_executor: ExpressionExecutor::with_expressions(&order_exprs),
            output_chunks: Default::default(),
            finalized: false,
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
        let TransformLocal::StreamingTopN(local) = local else {
            return Err(paro_error::internal(
                "streaming topn transform local state mismatch",
            ));
        };
        output.try_set_cardinality(0)?;
        if self.spec.limit == 0 {
            return Ok(TransformPoll::StopPipeline);
        }
        if input.is_empty() {
            return Ok(TransformPoll::NeedMoreInput);
        }

        let mut sort_chunk = Chunk::try_initialize(
            &topn_order_types(&self.spec),
            input.size().max(1),
            ctx.query.allocator(MemoryTag::BaseTable),
        )?;
        local.order_executor.execute_all_into_with_input(
            ExpressionEvalInput {
                params: ctx.query.params.as_ref(),
                columns: input,
            },
            ctx.query,
            &mut sort_chunk,
        )?;
        local.heap.sink_with_sort_chunk(input, &sort_chunk, None)?;
        Ok(TransformPoll::NeedMoreInput)
    }

    pub(crate) fn flush(
        &self,
        _ctx: &mut OperatorCallContext,
        _global: &TransformGlobal,
        local: &mut TransformLocal,
        output: &mut Chunk,
    ) -> Result<TransformFlushPoll> {
        let TransformLocal::StreamingTopN(local) = local else {
            return Err(paro_error::internal(
                "streaming topn transform local state mismatch",
            ));
        };
        if !local.finalized {
            local.output_chunks = local.heap.extract_results()?.into();
            local.finalized = true;
        }
        let Some(mut chunk) = local.output_chunks.pop_front() else {
            return Ok(TransformFlushPoll::Done);
        };
        output.move_from(&mut chunk);
        if local.output_chunks.is_empty() {
            Ok(TransformFlushPoll::Output)
        } else {
            Ok(TransformFlushPoll::OutputMore)
        }
    }

    pub(crate) fn finish_global(
        &self,
        _ctx: &mut OperatorFinishContext,
        _global: &TransformGlobal,
    ) -> Result<TransformFinishPoll> {
        Ok(TransformFinishPoll::Done)
    }
}

fn topn_order_types(spec: &TopNSpec) -> Vec<LogicalType> {
    spec.orders
        .iter()
        .map(|order| order.expression.return_type())
        .collect()
}
