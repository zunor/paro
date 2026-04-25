// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Physical filter operator.

use std::any::Any;
use std::sync::Arc;

use paro_common::chunk::Chunk;
use paro_common::error::Result;
use paro_common::types::LogicalType;
use paro_common::vector::{SelectionVector, Vector, VECTOR_SIZE};
use paro_planner::expression::Expression;

use crate::execution_context::ExecutionContext;
use crate::explain::explain_node::format_bound_expression;
use crate::expression_executor::executor::ExpressionExecutor;
use crate::operator::state::{GlobalOperatorState, OperatorState};
use crate::operator::PhysicalOperator;
use crate::operator_type::PhysicalOperatorType;
use crate::result_type::OperatorResultType;

/// Physical filter operator.
///
#[derive(Debug)]
pub struct Filter {
    /// Filter predicate
    predicate: Expression,
    /// Projection map applied after filtering (empty = keep all columns).
    projection_map: Vec<usize>,
    /// Output types after applying the optional projection map.
    types: Vec<LogicalType>,
    /// Child operator
    child: Arc<dyn PhysicalOperator>,
}

/// Cached state for filter execution.
#[derive(Debug)]
pub struct FilterOperatorState {
    predicate_executor: ExpressionExecutor,
    selection: SelectionVector,
}

impl OperatorState for FilterOperatorState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::operator::scan::dummy_scan::PhysicalDummyScan;
    use crate::operator::state::EmptyGlobalOperatorState;
    use crate::operator::PhysicalOperator;
    use crate::thread_context::ThreadContext;
    use paro_common::runtime_value::Value;
    use paro_common::vector::VectorType;
    use paro_context::{test_support::TestStatementContextBuilder, StatementContext};
    use paro_planner::expression::{
        ComparisonExpression, ComparisonType, ConstantExpression, Expression, ReferenceExpression,
    };

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

    fn filter_predicate() -> Expression {
        Expression::Comparison(ComparisonExpression::new(
            ComparisonType::GreaterThan,
            Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
            Expression::Constant(ConstantExpression::new(
                Value::Integer(1),
                LogicalType::Integer,
            )),
        ))
    }

    #[test]
    fn filter_state_caches_executor_and_partial_output_uses_dictionary_overlay() {
        let session = test_session();
        let ctx = test_runtime(session);
        let child = Arc::new(PhysicalDummyScan::with_types(vec![LogicalType::Integer]))
            as Arc<dyn PhysicalOperator>;
        let filter = Filter::new(filter_predicate(), child);
        let mut state = filter
            .get_operator_state(&ctx)
            .expect("filter state should be created");
        let mut output = Chunk::try_new(paro_common::test_utils::test_allocator())
            .expect("test chunk allocation failed");
        let input = integer_chunk(&[1, 2, 3]);

        let result = filter
            .execute(
                &ctx,
                &input,
                &mut output,
                &EmptyGlobalOperatorState,
                state.as_mut(),
                crate::operator::state::test_operator_memory_scope(),
            )
            .expect("filter execute should succeed");
        assert!(matches!(result, OperatorResultType::NeedMoreInput));
        assert_eq!(output.size(), 2);
        let output_column = output.column(0).expect("output column should exist");
        assert_eq!(output_column.vector_type(), VectorType::Dictionary);
        assert!(Arc::ptr_eq(
            output_column
                .child()
                .expect("dictionary output should keep a child reference"),
            input.column(0).expect("input column should exist"),
        ));

        let state = state
            .as_any_mut()
            .downcast_mut::<FilterOperatorState>()
            .expect("filter state downcast should succeed");
        let selection_allocation = state.selection.allocation_identity();
        assert_eq!(
            output_column
                .sel_vector()
                .expect("dictionary selection should exist")
                .allocation_identity(),
            selection_allocation
        );
        assert_eq!(state.predicate_executor.expression_count(), 1);
        match state.predicate_executor.compiled_state(0) {
            crate::expression_executor::state::CompiledExpressionState::Comparison(comparison) => {
                assert!(comparison.result.as_ref().is_none());
            }
            other => panic!("expected comparison predicate, got {other:?}"),
        }

        let all_selected_input = integer_chunk(&[2, 4]);
        filter
            .execute(
                &ctx,
                &all_selected_input,
                &mut output,
                &EmptyGlobalOperatorState,
                state,
                crate::operator::state::test_operator_memory_scope(),
            )
            .expect("second filter execute should succeed");
        assert!(Arc::ptr_eq(
            output.column(0).expect("output column should exist"),
            all_selected_input
                .column(0)
                .expect("all-selected input column should exist"),
        ));
        assert_ne!(state.selection.allocation_identity(), selection_allocation);
    }
}

impl Filter {
    /// Create a new filter operator.
    pub fn new(predicate: Expression, child: Arc<dyn PhysicalOperator>) -> Self {
        let types = child.types().to_vec();
        Self {
            predicate,
            projection_map: Vec::new(),
            types,
            child,
        }
    }

    pub fn with_projection_map(
        predicate: Expression,
        projection_map: Vec<usize>,
        child: Arc<dyn PhysicalOperator>,
    ) -> Self {
        let child_types = child.types().to_vec();
        let types = if projection_map.is_empty() {
            child_types.clone()
        } else {
            projection_map
                .iter()
                .filter_map(|&idx| child_types.get(idx).cloned())
                .collect()
        };
        Self {
            predicate,
            projection_map,
            types,
            child,
        }
    }

    pub fn predicate(&self) -> &Expression {
        &self.predicate
    }
}

impl PhysicalOperator for Filter {
    fn operator_type(&self) -> PhysicalOperatorType {
        PhysicalOperatorType::Filter
    }

    fn types(&self) -> &[LogicalType] {
        &self.types
    }

    fn explain_params(&self) -> Vec<String> {
        vec![format!(
            "Filter: {}",
            format_bound_expression(&self.predicate)
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

    fn get_operator_state(&self, ctx: &ExecutionContext) -> Result<Box<dyn OperatorState>> {
        Ok(Box::new(FilterOperatorState {
            predicate_executor: ExpressionExecutor::new(&self.predicate),
            selection: SelectionVector::try_with_capacity(
                VECTOR_SIZE,
                ctx.allocator(paro_common::allocator::MemoryTag::BaseTable),
            )?,
        }))
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
        if input.is_empty() {
            return Ok(OperatorResultType::NeedMoreInput);
        }

        let state = state
            .as_any_mut()
            .downcast_mut::<FilterOperatorState>()
            .expect("Invalid state type for Filter");

        if state.selection.len() < input.size() {
            state.selection = SelectionVector::try_with_capacity(
                input.size(),
                ctx.allocator(paro_common::allocator::MemoryTag::BaseTable),
            )?;
        }
        state.selection.set_len(input.size());
        let selected_count = state.predicate_executor.select_into(
            0,
            input,
            input.size(),
            ctx,
            &mut state.selection,
        )?;

        // No rows selected
        if selected_count == 0 {
            chunk.set_cardinality(0);
            return Ok(OperatorResultType::NeedMoreInput);
        }

        // All rows selected - return input as-is (optimization)
        if selected_count == input.size() {
            if self.projection_map.is_empty() {
                if chunk.column_count() != input.column_count() {
                    *chunk = Chunk::try_initialize(
                        self.child.types(),
                        input.size().max(1),
                        ctx.allocator(paro_common::allocator::MemoryTag::BaseTable),
                    )?;
                }
                chunk.reference(input);
            } else {
                if chunk.column_count() != self.types.len() {
                    *chunk = Chunk::try_initialize(
                        &self.types,
                        input.size().max(1),
                        ctx.allocator(paro_common::allocator::MemoryTag::BaseTable),
                    )?;
                }
                chunk.reference_columns(input, &self.projection_map);
            }
            return Ok(OperatorResultType::NeedMoreInput);
        }

        if chunk.column_count() != self.types.len() || chunk.capacity() < selected_count.max(1) {
            *chunk = Chunk::try_initialize(
                &self.types,
                selected_count.max(1),
                ctx.allocator(paro_common::allocator::MemoryTag::BaseTable),
            )?;
        } else {
            chunk.try_reset(chunk.allocator().clone())?;
        }

        let projection_columns: Vec<usize> = if self.projection_map.is_empty() {
            (0..input.data.len()).collect()
        } else {
            self.projection_map.clone()
        };

        for (output_idx, input_idx) in projection_columns.into_iter().enumerate() {
            let dict_vec =
                Vector::try_dictionary(Arc::clone(&input.data[input_idx]), &state.selection)?;
            chunk.data[output_idx] = Arc::new(dict_vec);
        }
        chunk.set_cardinality(selected_count);

        Ok(OperatorResultType::NeedMoreInput)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}
