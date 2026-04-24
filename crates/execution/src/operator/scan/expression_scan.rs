// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Physical Expression Scan Operator

use crate::execution_context::ExecutionContext;
use crate::expression_executor::executor::ExpressionExecutor;
use crate::operator::state::{GlobalSourceState, OperatorSourceInput};
use crate::operator::PhysicalOperator;
use crate::operator_type::PhysicalOperatorType;
use crate::result_type::SourceResultType;
use paro_common::chunk::Chunk;
use paro_common::error::Result;
use paro_common::types::LogicalType;
use paro_common::vector::Vector;
use paro_planner::expression::Expression;
use std::any::Any;
use std::sync::Arc;

#[derive(Debug)]
pub struct PhysicalExpressionScan {
    pub expressions: Vec<Vec<Expression>>,
    pub types: Vec<LogicalType>,
}

impl PhysicalExpressionScan {
    pub fn new(expressions: Vec<Vec<Expression>>, types: Vec<LogicalType>) -> Self {
        Self { expressions, types }
    }
}

#[derive(Debug, Default)]
pub struct ExpressionScanGlobalState {
    pub current_row: std::sync::atomic::AtomicUsize,
}

impl GlobalSourceState for ExpressionScanGlobalState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl PhysicalOperator for PhysicalExpressionScan {
    fn operator_type(&self) -> PhysicalOperatorType {
        PhysicalOperatorType::ExpressionScan
    }

    fn types(&self) -> &[LogicalType] {
        &self.types
    }

    fn is_source(&self) -> bool {
        true
    }

    /// Expression scan does not support parallel source.
    ///
    /// and don't benefit from parallelism.
    fn parallel_source(&self) -> bool {
        false
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn get_global_source_state(
        &self,
        _ctx: &ExecutionContext,
        _sink_state: Option<&dyn crate::operator::state::GlobalSinkState>,
    ) -> Result<Box<dyn GlobalSourceState>> {
        Ok(Box::new(ExpressionScanGlobalState {
            current_row: std::sync::atomic::AtomicUsize::new(0),
        }))
    }

    fn get_data(
        &self,
        ctx: &ExecutionContext,
        chunk: &mut Chunk,
        input: &mut OperatorSourceInput,
    ) -> Result<SourceResultType> {
        let gstate = input
            .global_state
            .as_any()
            .downcast_ref::<ExpressionScanGlobalState>()
            .unwrap();

        let start_row = gstate.current_row.fetch_add(
            paro_common::vector::VECTOR_SIZE,
            std::sync::atomic::Ordering::SeqCst,
        );
        if start_row >= self.expressions.len() {
            return Ok(SourceResultType::Finished);
        }

        let end_row = std::cmp::min(
            start_row + paro_common::vector::VECTOR_SIZE,
            self.expressions.len(),
        );
        let count = end_row - start_row;

        // Evaluate expressions row by row and fill chunk
        // We need a dummy chunk for executor
        let dummy_chunk =
            Chunk::try_new(ctx.allocator(paro_common::allocator::MemoryTag::BaseTable))?;

        for col_idx in 0..self.types.len() {
            let mut col_vector = Vector::try_new(
                self.types[col_idx].clone(),
                count,
                ctx.allocator(paro_common::allocator::MemoryTag::BaseTable),
            )?;
            col_vector.set_count(count);
            for row_idx in 0..count {
                let expr = &self.expressions[start_row + row_idx][col_idx];
                // Each cell stores its own expression, so expression_scan compiles per cell.
                let mut executor = ExpressionExecutor::new(expr);
                let result_vector = executor.execute_expression(0, &dummy_chunk, None, 1, ctx)?;
                // Copy value from result_vector(0) to col_vector(row_idx)
                col_vector.copy_at(row_idx, &result_vector, 0);
            }
            if col_idx < chunk.data.len() {
                chunk.data[col_idx] = Arc::new(col_vector);
            } else {
                chunk.data.push(Arc::new(col_vector));
            }
        }

        chunk.set_cardinality(count);

        // Always return HaveMoreOutput when we produced data
        // The caller will call us again and we'll return Finished then
        Ok(SourceResultType::HaveMoreOutput)
    }
}
