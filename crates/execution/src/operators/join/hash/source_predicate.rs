// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Batch-local predicates attached to fused reduction sources.

use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::vector::SelectionVector;
use paro_context::StatementContext;
use paro_function::scalar::FunctionExecContext;
use paro_planner::expression::Expression;

use crate::expression_executor::executor::ExpressionExecutor;
use crate::operators::join::hash::comparison::{
    fixed_comparison_matches, FixedComparison, FixedKind,
};

#[derive(Debug)]
pub struct ReductionSourcePredicateState {
    predicate_mask: u8,
    evaluator: ReductionSourcePredicateEvaluator,
}

#[derive(Debug)]
enum ReductionSourcePredicateEvaluator {
    Fixed {
        left_index: usize,
        right_index: usize,
        comparison: FixedComparison,
        kind: FixedKind,
        fallback: ExpressionExecutor,
    },
    Generic(ExpressionExecutor),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FixedComparisonShape {
    left_index: usize,
    right_index: usize,
    comparison: FixedComparison,
    kind: FixedKind,
}

impl ReductionSourcePredicateState {
    pub(crate) fn new(
        expression: &Expression,
        predicate_mask: u8,
        session: &StatementContext,
    ) -> Self {
        let generic = || {
            ExpressionExecutor::with_expressions_for_session(
                std::slice::from_ref(expression),
                session,
            )
        };
        let evaluator = match compile_fixed_comparison(expression) {
            Some(shape) => ReductionSourcePredicateEvaluator::Fixed {
                left_index: shape.left_index,
                right_index: shape.right_index,
                comparison: shape.comparison,
                kind: shape.kind,
                fallback: generic(),
            },
            None => ReductionSourcePredicateEvaluator::Generic(generic()),
        };
        Self {
            predicate_mask,
            evaluator,
        }
    }

    pub(crate) fn evaluate_into(
        &mut self,
        input: &Chunk,
        runtime: &dyn FunctionExecContext,
        selection: &mut SelectionVector,
        masks: &mut [u8],
    ) -> Result<()> {
        if masks.len() < input.size() {
            return Err(paro_error::internal(
                "reduction source mask is smaller than its input",
            ));
        }
        match &mut self.evaluator {
            ReductionSourcePredicateEvaluator::Fixed {
                left_index,
                right_index,
                comparison,
                kind,
                fallback,
            } => {
                let left = input.column(*left_index).ok_or_else(|| {
                    paro_error::internal("reduction source predicate left column missing")
                })?;
                let right = input.column(*right_index).ok_or_else(|| {
                    paro_error::internal("reduction source predicate right column missing")
                })?;
                macro_rules! evaluate {
                    ($ty:ty) => {
                        evaluate_fixed::<$ty>(
                            left,
                            right,
                            input.size(),
                            *comparison,
                            self.predicate_mask,
                            masks,
                        )?
                    };
                }
                let evaluated = match kind {
                    FixedKind::I8 => evaluate!(i8),
                    FixedKind::I16 => evaluate!(i16),
                    FixedKind::I32 => evaluate!(i32),
                    FixedKind::I64 => {
                        evaluate_i64(
                            left,
                            right,
                            input.size(),
                            *comparison,
                            self.predicate_mask,
                            masks,
                        )?;
                        true
                    }
                    FixedKind::I128 => evaluate!(i128),
                    FixedKind::U8 => evaluate!(u8),
                    FixedKind::U16 => evaluate!(u16),
                    FixedKind::U32 => evaluate!(u32),
                    FixedKind::U64 => evaluate!(u64),
                    FixedKind::U128 => evaluate!(u128),
                    FixedKind::F32 => evaluate!(f32),
                    FixedKind::F64 => evaluate!(f64),
                };
                if !evaluated {
                    evaluate_generic(
                        fallback,
                        input,
                        runtime,
                        selection,
                        self.predicate_mask,
                        masks,
                    )?;
                }
            }
            ReductionSourcePredicateEvaluator::Generic(executor) => {
                evaluate_generic(
                    executor,
                    input,
                    runtime,
                    selection,
                    self.predicate_mask,
                    masks,
                )?;
            }
        }
        Ok(())
    }
}

fn compile_fixed_comparison(expression: &Expression) -> Option<FixedComparisonShape> {
    let Expression::Comparison(comparison_expression) = expression else {
        return None;
    };
    let left_index = column_index(&comparison_expression.left)?;
    let right_index = column_index(&comparison_expression.right)?;
    let left_type = comparison_expression.left.return_type().physical_type();
    if left_type != comparison_expression.right.return_type().physical_type() {
        return None;
    }
    let kind = FixedKind::from_physical_type(left_type)?;
    let comparison = FixedComparison::from(comparison_expression.comparison_type);
    if !kind.supports(comparison) {
        return None;
    }
    Some(FixedComparisonShape {
        left_index,
        right_index,
        comparison,
        kind,
    })
}

fn column_index(expression: &Expression) -> Option<usize> {
    match expression {
        Expression::Reference(reference) => Some(reference.index),
        _ => None,
    }
}

fn evaluate_fixed<T>(
    left: &paro_common::vector::Vector,
    right: &paro_common::vector::Vector,
    count: usize,
    comparison: FixedComparison,
    predicate_mask: u8,
    masks: &mut [u8],
) -> Result<bool>
where
    T: Copy + PartialEq + PartialOrd,
{
    let left = left.try_to_view(count)?;
    let right = right.try_to_view(count)?;
    let Some(left_data) = left.get_data::<T>() else {
        return Ok(false);
    };
    let Some(right_data) = right.get_data::<T>() else {
        return Ok(false);
    };
    for (row_idx, mask) in masks[..count].iter_mut().enumerate() {
        let left_value = left.is_valid(row_idx).then(|| unsafe {
            // SAFETY: the decoded view validates its physical storage and the
            // physical kind was compiled from the expression's logical type.
            *left_data.add(left.physical_index(row_idx))
        });
        let right_value = right
            .is_valid(row_idx)
            .then(|| unsafe { *right_data.add(right.physical_index(row_idx)) });
        if fixed_comparison_matches(left_value, right_value, comparison) {
            *mask |= predicate_mask;
        }
    }
    Ok(true)
}

fn evaluate_i64(
    left: &paro_common::vector::Vector,
    right: &paro_common::vector::Vector,
    count: usize,
    comparison: FixedComparison,
    predicate_mask: u8,
    masks: &mut [u8],
) -> Result<()> {
    let left = left.try_to_view(count)?;
    let right = right.try_to_view(count)?;
    for (row_idx, mask) in masks[..count].iter_mut().enumerate() {
        let left_value = left.is_valid(row_idx).then(|| left.get_i64(row_idx));
        let right_value = right.is_valid(row_idx).then(|| right.get_i64(row_idx));
        if fixed_comparison_matches(left_value, right_value, comparison) {
            *mask |= predicate_mask;
        }
    }
    Ok(())
}

fn evaluate_generic(
    executor: &mut ExpressionExecutor,
    input: &Chunk,
    runtime: &dyn FunctionExecContext,
    selection: &mut SelectionVector,
    predicate_mask: u8,
    masks: &mut [u8],
) -> Result<()> {
    let selected = executor.select_into(0, input, input.size(), runtime, selection)?;
    for selected_idx in 0..selected {
        masks[selection.get(selected_idx)] |= predicate_mask;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use paro_common::types::LogicalType;
    use paro_planner::expression::{
        ComparisonExpression, ComparisonType, Expression, ReferenceExpression,
    };

    use crate::operators::join::hash::comparison::{
        fixed_comparison_matches, FixedComparison, FixedKind,
    };

    use super::{compile_fixed_comparison, FixedComparisonShape};

    #[test]
    fn date_column_comparison_compiles_to_i32_kernel() {
        let expression = Expression::Comparison(ComparisonExpression::new(
            ComparisonType::GreaterThan,
            Expression::Reference(ReferenceExpression::new(0, LogicalType::Date)),
            Expression::Reference(ReferenceExpression::new(1, LogicalType::Date)),
        ));
        assert!(matches!(
            compile_fixed_comparison(&expression),
            Some(FixedComparisonShape {
                left_index: 0,
                right_index: 1,
                comparison: FixedComparison::GreaterThan,
                kind: FixedKind::I32,
            })
        ));
    }

    #[test]
    fn narrow_decimal_column_comparison_compiles_to_i64_kernel() {
        let decimal = LogicalType::Decimal {
            precision: 15,
            scale: 2,
        };
        let expression = Expression::Comparison(ComparisonExpression::new(
            ComparisonType::LessThan,
            Expression::Reference(ReferenceExpression::new(0, decimal.clone())),
            Expression::Reference(ReferenceExpression::new(1, decimal)),
        ));
        assert!(matches!(
            compile_fixed_comparison(&expression),
            Some(FixedComparisonShape {
                left_index: 0,
                right_index: 1,
                comparison: FixedComparison::LessThan,
                kind: FixedKind::I64,
            })
        ));
    }

    #[test]
    fn float_distinct_from_uses_generic_value_semantics() {
        let expression = Expression::Comparison(ComparisonExpression::new(
            ComparisonType::DistinctFrom,
            Expression::Reference(ReferenceExpression::new(0, LogicalType::Double)),
            Expression::Reference(ReferenceExpression::new(1, LogicalType::Double)),
        ));
        assert!(compile_fixed_comparison(&expression).is_none());
    }

    #[test]
    fn fixed_comparison_preserves_sql_null_semantics() {
        assert!(!fixed_comparison_matches::<i32>(
            None,
            Some(1),
            FixedComparison::GreaterThan
        ));
        assert!(fixed_comparison_matches::<i32>(
            None,
            Some(1),
            FixedComparison::DistinctFrom
        ));
        assert!(fixed_comparison_matches::<i32>(
            None,
            None,
            FixedComparison::NotDistinctFrom
        ));
    }
}
