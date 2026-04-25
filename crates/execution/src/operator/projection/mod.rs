// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Physical projection operator.

pub mod table_in_out_function;

use std::any::Any;

use paro_common::chunk::Chunk;
use paro_common::error::Result;
use paro_common::types::LogicalType;
use paro_planner::expression::Expression;

use crate::execution_context::ExecutionContext;
use crate::explain::explain_node::format_bound_expression;
use crate::expression_executor::executor::ExpressionExecutor;
use crate::operator::state::{
    GlobalOperatorState, GlobalSinkState, GlobalSourceState, LocalSourceState, OperatorSourceInput,
    OperatorState,
};
use crate::operator::PhysicalOperator;
use crate::operator_type::PhysicalOperatorType;
use crate::result_type::{OperatorResultType, SourceResultType};
use std::sync::Arc;

/// Physical projection operator.
///
/// Evaluates a list of expressions and produces output columns.
#[derive(Debug)]
pub struct Projection {
    /// Output types
    output_types: Vec<LogicalType>,
    /// Projection expressions
    expressions: Vec<Expression>,
    /// Child operator
    child: Arc<dyn PhysicalOperator>,
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::execution_context::ExecutionContext;
    use crate::expression_executor::state::CompiledExpressionState;
    use crate::operator::scan::dummy_scan::PhysicalDummyScan;
    use crate::operator::state::EmptyGlobalOperatorState;
    use crate::operator::PhysicalOperator;
    use crate::thread_context::ThreadContext;
    use paro_common::runtime_value::Value;
    use paro_common::vector::Vector;
    use paro_context::{test_support::TestStatementContextBuilder, StatementContext};
    use paro_function::scalar::{FunctionExecContext, ScalarFunction};
    use paro_planner::expression::{Expression, FunctionExpression, ReferenceExpression};

    fn test_session() -> Arc<StatementContext> {
        TestStatementContextBuilder::minimal().build()
    }

    fn test_runtime(session: Arc<StatementContext>) -> ExecutionContext<'static> {
        let thread = Box::leak(Box::new(ThreadContext::single_threaded()));
        ExecutionContext::new(session, thread, None)
    }

    fn integer_chunk(values: &[i32]) -> Chunk {
        Chunk::from_vectors(
            vec![paro_common::test_utils::test_i32_vector_with_allocator(
                values,
                paro_common::test_utils::test_allocator(),
            )],
            paro_common::test_utils::test_allocator(),
        )
    }

    fn add_one_function(
        input: &Chunk,
        _runtime: &dyn FunctionExecContext,
        result: &mut Vector,
    ) -> Result<()> {
        let column = input
            .column(0)
            .expect("projection test input should have one column");
        for row_idx in 0..input.size() {
            result.set_i32(
                row_idx,
                column
                    .get_i32(row_idx)
                    .expect("projection test input should be non-null")
                    + 1,
            );
        }
        Ok(())
    }

    fn add_one_expr() -> Expression {
        let function = ScalarFunction::new(
            "projection_add_one".to_string(),
            vec![LogicalType::Integer],
            LogicalType::Integer,
            add_one_function,
        );
        Expression::Function(FunctionExpression::new(
            function,
            vec![Expression::Reference(ReferenceExpression::new(
                0,
                LogicalType::Integer,
            ))],
            LogicalType::Integer,
        ))
    }

    #[test]
    fn projection_operator_state_reuses_compiled_executor_and_output_chunk() {
        let session = test_session();
        let ctx = test_runtime(session);
        let child = Arc::new(PhysicalDummyScan::with_types(vec![LogicalType::Integer]))
            as Arc<dyn PhysicalOperator>;
        let projection = Projection::new(vec![add_one_expr()], child);
        let mut state = projection
            .get_operator_state(&ctx)
            .expect("projection state should be created");
        let mut output = Chunk::try_new(paro_common::test_utils::test_allocator())
            .expect("test chunk allocation failed");

        let first_input = integer_chunk(&[1, 2, 3]);
        projection
            .execute(
                &ctx,
                &first_input,
                &mut output,
                &EmptyGlobalOperatorState,
                state.as_mut(),
                crate::operator::state::test_operator_memory_scope(),
            )
            .expect("projection execute should succeed");
        let output_capacity = output.capacity();
        assert_eq!(output.get_value(0, 0), Some(Value::Integer(2)));

        let state = state
            .as_any_mut()
            .downcast_mut::<ProjectionOperatorState>()
            .expect("projection state downcast should succeed");
        let first_intermediate_capacity = match state.executor.compiled_state(0) {
            CompiledExpressionState::Function(function_state) => function_state
                .intermediate_chunk
                .as_ref()
                .expect("function projection should allocate an intermediate chunk")
                .capacity(),
            other => panic!("expected function state, got {other:?}"),
        };

        let second_input = integer_chunk(&[4, 5]);
        projection
            .execute(
                &ctx,
                &second_input,
                &mut output,
                &EmptyGlobalOperatorState,
                state,
                crate::operator::state::test_operator_memory_scope(),
            )
            .expect("second projection execute should succeed");
        assert_eq!(output.capacity(), output_capacity);
        assert_eq!(output.get_value(0, 0), Some(Value::Integer(5)));
        assert_eq!(output.get_value(0, 1), Some(Value::Integer(6)));

        let second_intermediate_capacity = match state.executor.compiled_state(0) {
            CompiledExpressionState::Function(function_state) => function_state
                .intermediate_chunk
                .as_ref()
                .expect("intermediate chunk should stay allocated")
                .capacity(),
            _ => unreachable!(),
        };
        assert_eq!(first_intermediate_capacity, second_intermediate_capacity);
    }

    #[test]
    fn projection_local_source_state_caches_executor() {
        let session = test_session();
        let ctx = test_runtime(session);
        let child = Arc::new(PhysicalDummyScan::with_types(vec![LogicalType::Integer]))
            as Arc<dyn PhysicalOperator>;
        let projection = Projection::new(vec![add_one_expr()], child);
        let gstate = projection
            .get_global_source_state(&ctx, None)
            .expect("projection global source state should be created");
        let lstate = projection
            .get_local_source_state(&ctx, gstate.as_ref())
            .expect("projection local source state should be created");
        let lstate = lstate
            .as_any()
            .downcast_ref::<ProjectionLocalSourceState>()
            .expect("projection local source state downcast should succeed");

        assert_eq!(lstate.executor.expression_count(), 1);
    }
}

/// State for projection operator.
#[derive(Debug)]
pub struct ProjectionOperatorState {
    executor: ExpressionExecutor,
}

#[derive(Debug)]
pub struct ProjectionLocalSourceState {
    child_local_state: Box<dyn LocalSourceState>,
    executor: ExpressionExecutor,
}

impl OperatorState for ProjectionOperatorState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl LocalSourceState for ProjectionLocalSourceState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl Projection {
    /// Create a new projection operator.
    pub fn new(expressions: Vec<Expression>, child: Arc<dyn PhysicalOperator>) -> Self {
        let output_types = expressions.iter().map(|expr| expr.return_type()).collect();

        Self {
            output_types,
            expressions,
            child,
        }
    }

    pub fn expressions(&self) -> &[Expression] {
        &self.expressions
    }

    /// Returns true if any projection expression needs input columns.
    fn expressions_require_input_columns(expressions: &[Expression]) -> bool {
        expressions
            .iter()
            .any(Self::expression_requires_input_columns)
    }

    fn expression_requires_input_columns(expr: &Expression) -> bool {
        match expr {
            Expression::ColumnRef(_) | Expression::Reference(_) => true,
            Expression::Function(function) => function
                .children
                .iter()
                .any(Self::expression_requires_input_columns),
            Expression::Cast(cast) => Self::expression_requires_input_columns(&cast.child),
            Expression::Conjunction(conjunction) => conjunction
                .children
                .iter()
                .any(Self::expression_requires_input_columns),
            Expression::Case(case_expr) => {
                Self::expression_requires_input_columns(&case_expr.check)
                    || Self::expression_requires_input_columns(&case_expr.result_if_true)
                    || Self::expression_requires_input_columns(&case_expr.result_if_false)
            }
            Expression::Comparison(comparison) => {
                Self::expression_requires_input_columns(&comparison.left)
                    || Self::expression_requires_input_columns(&comparison.right)
            }
            Expression::Operator(operator) => operator
                .children
                .iter()
                .any(Self::expression_requires_input_columns),
            Expression::Aggregate(aggregate) => {
                aggregate
                    .children
                    .iter()
                    .any(Self::expression_requires_input_columns)
                    || aggregate
                        .filter
                        .as_ref()
                        .is_some_and(|filter| Self::expression_requires_input_columns(filter))
                    || aggregate
                        .order_bys
                        .iter()
                        .any(|order| Self::expression_requires_input_columns(&order.expression))
            }
            Expression::Subquery(subquery) => subquery
                .children
                .iter()
                .any(Self::expression_requires_input_columns),
            Expression::Window(window) => {
                window
                    .children
                    .iter()
                    .any(Self::expression_requires_input_columns)
                    || window
                        .partitions
                        .iter()
                        .any(Self::expression_requires_input_columns)
                    || window
                        .orders
                        .iter()
                        .any(|order| Self::expression_requires_input_columns(&order.expression))
            }
            Expression::Constant(_) => false,
        }
    }
}

impl PhysicalOperator for Projection {
    fn operator_type(&self) -> PhysicalOperatorType {
        PhysicalOperatorType::Projection
    }

    fn types(&self) -> &[LogicalType] {
        &self.output_types
    }

    fn explain_params(&self) -> Vec<String> {
        if self.expressions.is_empty() {
            return vec![];
        }
        vec![format!(
            "Output: {}",
            self.expressions
                .iter()
                .map(format_bound_expression)
                .collect::<Vec<_>>()
                .join(", ")
        )]
    }

    fn children_count(&self) -> usize {
        1
    }

    fn child(&self, index: usize) -> Option<&dyn PhysicalOperator> {
        if index == 0 {
            Some(self.child.as_ref())
        } else {
            None
        }
    }

    fn child_arc(&self, index: usize) -> Option<Arc<dyn PhysicalOperator>> {
        if index == 0 {
            Some(self.child.clone())
        } else {
            None
        }
    }

    fn get_operator_state(&self, _ctx: &ExecutionContext) -> Result<Box<dyn OperatorState>> {
        Ok(Box::new(ProjectionOperatorState {
            executor: ExpressionExecutor::with_expressions(&self.expressions),
        }))
    }

    fn is_source(&self) -> bool {
        self.child.is_source()
    }

    fn get_global_source_state(
        &self,
        ctx: &ExecutionContext,
        sink_state: Option<&dyn GlobalSinkState>,
    ) -> Result<Box<dyn GlobalSourceState>> {
        self.child.get_global_source_state(ctx, sink_state)
    }

    fn get_local_source_state(
        &self,
        ctx: &ExecutionContext,
        gstate: &dyn GlobalSourceState,
    ) -> Result<Box<dyn LocalSourceState>> {
        Ok(Box::new(ProjectionLocalSourceState {
            child_local_state: self.child.get_local_source_state(ctx, gstate)?,
            executor: ExpressionExecutor::with_expressions(&self.expressions),
        }))
    }

    fn get_data(
        &self,
        ctx: &ExecutionContext,
        chunk: &mut Chunk,
        input: &mut OperatorSourceInput,
    ) -> Result<SourceResultType> {
        let lstate = input
            .local_state
            .as_any_mut()
            .downcast_mut::<ProjectionLocalSourceState>()
            .expect("Invalid local source state for Projection");

        // Create a temporary chunk to hold child data
        let mut child_chunk = Chunk::try_initialize(
            self.child.types(),
            chunk.capacity(),
            chunk.allocator().clone(),
        )?;

        let mut child_input = OperatorSourceInput::with_memory(
            input.global_state,
            lstate.child_local_state.as_mut(),
            input.interrupt_state,
            input.memory.child_scope(),
        );
        let result = self
            .child
            .get_data(ctx, &mut child_chunk, &mut child_input)?;

        if child_chunk.size() > 0 {
            if chunk.column_count() != self.output_types.len()
                || chunk.capacity() < child_chunk.size().max(1)
            {
                *chunk = Chunk::try_initialize(
                    &self.output_types,
                    child_chunk.size().max(1),
                    ctx.allocator(paro_common::allocator::MemoryTag::BaseTable),
                )?;
            } else {
                chunk.try_reset(chunk.allocator().clone())?;
            }
            lstate.executor.execute_all_into(&child_chunk, ctx, chunk)?;
        } else {
            // Prevent stale output propagation when child yields no rows.
            *chunk = Chunk::try_init_empty(&self.output_types, chunk.allocator().clone())?;
        }
        Ok(result)
    }

    fn execute(
        &self,
        ctx: &ExecutionContext,
        input: &Chunk,
        chunk: &mut Chunk,
        _gstate: &dyn GlobalOperatorState,
        state: &mut dyn OperatorState,
        _memory: crate::memory_runtime::OperatorMemoryScope<'_>,
    ) -> Result<OperatorResultType> {
        let state = state
            .as_any_mut()
            .downcast_mut::<ProjectionOperatorState>()
            .expect("Invalid state type for Projection");

        // A 0-column input can still carry rows (e.g. DummyScan for `SELECT 42`).
        // Only treat as empty when cardinality is zero.
        if input.is_empty() {
            *chunk = Chunk::try_init_empty(&self.output_types, chunk.allocator().clone())?;
            return Ok(OperatorResultType::NeedMoreInput);
        }
        if input.column_count() == 0 && Self::expressions_require_input_columns(&self.expressions) {
            // Optimizer can route empty-result branches through a 0-column chunk.
            // If expressions reference input columns, treat this as no rows.
            *chunk = Chunk::try_init_empty(&self.output_types, chunk.allocator().clone())?;
            return Ok(OperatorResultType::NeedMoreInput);
        }

        if chunk.column_count() != self.output_types.len() || chunk.capacity() < input.size().max(1)
        {
            *chunk = Chunk::try_initialize(
                &self.output_types,
                input.size().max(1),
                ctx.allocator(paro_common::allocator::MemoryTag::BaseTable),
            )?;
        } else {
            chunk.try_reset(chunk.allocator().clone())?;
        }
        state.executor.execute_all_into(input, ctx, chunk)?;

        Ok(OperatorResultType::NeedMoreInput)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}
