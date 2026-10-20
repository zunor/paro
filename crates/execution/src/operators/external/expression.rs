// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Evaluate external expressions inside their SQL row-selection boundary.

use std::sync::Arc;

use paro_common::allocator::{Allocator, MemoryTag};
use paro_common::chunk::Chunk;
use paro_common::error::{self as error, Result};
use paro_common::memory::MemoryAccountingClass;
use paro_common::vector::{SelectionVector, Vector};
use paro_planner::expression::{Expression, ExpressionIterator, ReferenceExpression};

use crate::expression_executor::executor::ExpressionExecutor;
use crate::memory_runtime::OperatorMemoryScope;
use crate::runtime::QueryRuntimeContext;

pub(super) fn evaluate(
    expression: &Expression,
    input: &Chunk,
    ctx: &QueryRuntimeContext,
    memory: &OperatorMemoryScope<'_>,
) -> Result<Vector> {
    Evaluator { ctx, memory }.evaluate(expression, input, 0)
}

struct Evaluator<'a, 'm> {
    ctx: &'a QueryRuntimeContext,
    memory: &'a OperatorMemoryScope<'m>,
}

impl Evaluator<'_, '_> {
    fn allocator(&self) -> Arc<dyn Allocator> {
        self.memory.accounted_allocator_for(
            MemoryTag::ExternalRuntimeHost,
            MemoryAccountingClass::NonRevocable,
        )
    }

    fn evaluate(&self, expression: &Expression, input: &Chunk, depth: usize) -> Result<Vector> {
        self.ctx.cancellation.check()?;
        if depth > 256 {
            return Err(error::invalid_input(
                "external expression nesting exceeds 256",
            ));
        }
        if input.is_empty() {
            return Vector::try_new(expression.return_type(), 0, self.allocator());
        }
        if let Expression::Case(case) = expression {
            let condition = self.evaluate(&case.check, input, depth + 1)?;
            let mut selected = [Vec::new(), Vec::new()];
            for row in 0..input.size() {
                let branch = usize::from(condition.get_bool(row) == Some(true));
                selected[branch].push(row as u32);
            }
            let mut result =
                Vector::try_new(case.return_type.clone(), input.size(), self.allocator())?;
            result.try_set_count(input.size())?;
            for (rows, branch) in selected
                .into_iter()
                .zip([case.result_if_false.as_ref(), case.result_if_true.as_ref()])
            {
                if rows.is_empty() {
                    continue;
                }
                let selection =
                    SelectionVector::try_from_owned_indices(rows.clone(), self.allocator())?;
                let mut active = Chunk::try_new(self.allocator())?;
                active.try_reference_selection(input, &[], selection.into())?;
                let values = self.evaluate(branch, &active, depth + 1)?;
                for (source, destination) in rows.into_iter().enumerate() {
                    result.try_copy_range(destination as usize, &values, source, 1)?;
                }
            }
            return Ok(result);
        }

        if !expression.contains_external_routine() {
            return self.native(expression, input);
        }

        // Materialize only external child occurrences, then use the existing
        // native expression executor for the enclosing expression. No user
        // function is registered as a native scalar implementation.
        let mut rewritten = expression.clone();
        let mut extended = input.clone_referencing_vectors();
        let mut failure = None;
        ExpressionIterator::enumerate_children_mut(&mut rewritten, |child| {
            if failure.is_some() || !child.contains_external_routine() {
                return;
            }
            let index = extended.column_count();
            match self
                .evaluate(child, input, depth + 1)
                .and_then(|value| extended.try_push_column(Arc::new(value), input.size()))
            {
                Ok(()) => {
                    *child = Expression::Reference(
                        ReferenceExpression::new(index, child.return_type()).into(),
                    )
                }
                Err(error) => failure = Some(error),
            }
        });
        if let Some(error) = failure {
            return Err(error);
        }
        if let Expression::Function(function) = &rewritten {
            if function.crosses_execution_boundary() {
                let spec = function
                    .routine_meta()
                    .and_then(|meta| meta.spec.as_ref())
                    .ok_or_else(|| {
                        error::internal("external expression has no routine definition")
                    })?;
                let arguments = function
                    .children
                    .iter()
                    .map(|child| self.native(child, &extended))
                    .collect::<Result<Vec<_>>>()?;
                let mut arguments = Chunk::from_vectors(arguments, self.allocator());
                arguments.try_set_cardinality(input.size())?;
                let mut result =
                    super::runtime_bridge::execute_python(self.ctx, spec, &arguments, self.memory)?;
                if result.len() != 1 {
                    return Err(error::contract_violation(
                        "scalar routine must return one column",
                    ));
                }
                return Ok(result.remove(0));
            }
        }
        self.native(&rewritten, &extended)
    }

    fn native(&self, expression: &Expression, input: &Chunk) -> Result<Vector> {
        let mut executor = ExpressionExecutor::with_expressions(std::slice::from_ref(expression));
        let value = executor.execute_expression(0, input, None, input.size(), self.ctx)?;
        let mut result = Vector::try_new(expression.return_type(), input.size(), self.allocator())?;
        result.try_copy_range(0, &value, 0, input.size())?;
        result.try_set_count(input.size())?;
        Ok(result)
    }
}
