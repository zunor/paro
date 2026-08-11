// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Batch-local predicates attached to fused reduction sources.

use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_common::vector::SelectionVector;
use paro_context::StatementContext;
use paro_function::scalar::FunctionExecContext;
use paro_planner::expression::{ComparisonType, Expression};

use crate::expression_executor::executor::ExpressionExecutor;

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
        comparison: ComparisonType,
        kind: FixedKind,
    },
    Generic(ExpressionExecutor),
}

#[derive(Debug, Clone, Copy)]
enum FixedKind {
    I8,
    I16,
    I32,
    I64,
    I128,
    U8,
    U16,
    U32,
    U64,
    U128,
    F32,
    F64,
}

impl ReductionSourcePredicateState {
    pub(crate) fn new(
        expression: &Expression,
        predicate_mask: u8,
        session: &StatementContext,
    ) -> Self {
        let evaluator = compile_fixed_comparison(expression).unwrap_or_else(|| {
            ReductionSourcePredicateEvaluator::Generic(
                ExpressionExecutor::with_expressions_for_session(
                    std::slice::from_ref(expression),
                    session,
                ),
            )
        });
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
                match kind {
                    FixedKind::I8 => evaluate!(i8),
                    FixedKind::I16 => evaluate!(i16),
                    FixedKind::I32 => evaluate!(i32),
                    FixedKind::I64 => evaluate_i64(
                        left,
                        right,
                        input.size(),
                        *comparison,
                        self.predicate_mask,
                        masks,
                    )?,
                    FixedKind::I128 => evaluate!(i128),
                    FixedKind::U8 => evaluate!(u8),
                    FixedKind::U16 => evaluate!(u16),
                    FixedKind::U32 => evaluate!(u32),
                    FixedKind::U64 => evaluate!(u64),
                    FixedKind::U128 => evaluate!(u128),
                    FixedKind::F32 => evaluate!(f32),
                    FixedKind::F64 => evaluate!(f64),
                }
            }
            ReductionSourcePredicateEvaluator::Generic(executor) => {
                let selected = executor.select_into(0, input, input.size(), runtime, selection)?;
                for selected_idx in 0..selected {
                    masks[selection.get(selected_idx)] |= self.predicate_mask;
                }
            }
        }
        Ok(())
    }
}

fn compile_fixed_comparison(expression: &Expression) -> Option<ReductionSourcePredicateEvaluator> {
    let Expression::Comparison(expression) = expression else {
        return None;
    };
    let left_index = column_index(&expression.left)?;
    let right_index = column_index(&expression.right)?;
    let left_type = expression.left.return_type();
    if left_type != expression.right.return_type() {
        return None;
    }
    let kind = match left_type {
        LogicalType::TinyInt => FixedKind::I8,
        LogicalType::SmallInt => FixedKind::I16,
        LogicalType::Integer | LogicalType::Date => FixedKind::I32,
        LogicalType::BigInt
        | LogicalType::Timestamp
        | LogicalType::TimestampTz
        | LogicalType::Time => FixedKind::I64,
        LogicalType::HugeInt | LogicalType::Decimal { .. } => FixedKind::I128,
        LogicalType::UTinyInt => FixedKind::U8,
        LogicalType::USmallInt => FixedKind::U16,
        LogicalType::UInteger => FixedKind::U32,
        LogicalType::UBigInt => FixedKind::U64,
        LogicalType::UHugeInt => FixedKind::U128,
        LogicalType::Float => FixedKind::F32,
        LogicalType::Double => FixedKind::F64,
        _ => return None,
    };
    Some(ReductionSourcePredicateEvaluator::Fixed {
        left_index,
        right_index,
        comparison: expression.comparison_type,
        kind,
    })
}

fn column_index(expression: &Expression) -> Option<usize> {
    match expression {
        Expression::Reference(reference) => Some(reference.index),
        Expression::ColumnRef(column) => Some(column.binding.column_index),
        _ => None,
    }
}

fn evaluate_fixed<T>(
    left: &paro_common::vector::Vector,
    right: &paro_common::vector::Vector,
    count: usize,
    comparison: ComparisonType,
    predicate_mask: u8,
    masks: &mut [u8],
) -> Result<()>
where
    T: Copy + PartialEq + PartialOrd,
{
    let left = left.try_to_view(count)?;
    let right = right.try_to_view(count)?;
    let Some(left_data) = left.get_data::<T>() else {
        return Err(paro_error::internal(
            "fixed reduction source column uses an unsupported sequence encoding",
        ));
    };
    let Some(right_data) = right.get_data::<T>() else {
        return Err(paro_error::internal(
            "fixed reduction source column uses an unsupported sequence encoding",
        ));
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
        if comparison_matches(left_value, right_value, comparison) {
            *mask |= predicate_mask;
        }
    }
    Ok(())
}

fn evaluate_i64(
    left: &paro_common::vector::Vector,
    right: &paro_common::vector::Vector,
    count: usize,
    comparison: ComparisonType,
    predicate_mask: u8,
    masks: &mut [u8],
) -> Result<()> {
    let left = left.try_to_view(count)?;
    let right = right.try_to_view(count)?;
    for (row_idx, mask) in masks[..count].iter_mut().enumerate() {
        let left_value = left.is_valid(row_idx).then(|| left.get_i64(row_idx));
        let right_value = right.is_valid(row_idx).then(|| right.get_i64(row_idx));
        if comparison_matches(left_value, right_value, comparison) {
            *mask |= predicate_mask;
        }
    }
    Ok(())
}

#[inline]
fn comparison_matches<T>(left: Option<T>, right: Option<T>, comparison: ComparisonType) -> bool
where
    T: PartialEq + PartialOrd,
{
    match comparison {
        ComparisonType::DistinctFrom => left != right,
        ComparisonType::NotDistinctFrom => left == right,
        ComparisonType::Equal => {
            matches!((left, right), (Some(left), Some(right)) if left == right)
        }
        ComparisonType::NotEqual => {
            matches!((left, right), (Some(left), Some(right)) if left != right)
        }
        ComparisonType::LessThan => {
            matches!((left, right), (Some(left), Some(right)) if left < right)
        }
        ComparisonType::LessThanOrEqual => {
            matches!((left, right), (Some(left), Some(right)) if left <= right)
        }
        ComparisonType::GreaterThan => {
            matches!((left, right), (Some(left), Some(right)) if left > right)
        }
        ComparisonType::GreaterThanOrEqual => {
            matches!((left, right), (Some(left), Some(right)) if left >= right)
        }
    }
}

#[cfg(test)]
mod tests {
    use paro_common::types::LogicalType;
    use paro_planner::expression::{
        ComparisonExpression, ComparisonType, Expression, ReferenceExpression,
    };

    use super::{
        comparison_matches, compile_fixed_comparison, FixedKind, ReductionSourcePredicateEvaluator,
    };

    #[test]
    fn date_column_comparison_compiles_to_i32_kernel() {
        let expression = Expression::Comparison(ComparisonExpression::new(
            ComparisonType::GreaterThan,
            Expression::Reference(ReferenceExpression::new(0, LogicalType::Date)),
            Expression::Reference(ReferenceExpression::new(1, LogicalType::Date)),
        ));
        assert!(matches!(
            compile_fixed_comparison(&expression),
            Some(ReductionSourcePredicateEvaluator::Fixed {
                left_index: 0,
                right_index: 1,
                comparison: ComparisonType::GreaterThan,
                kind: FixedKind::I32,
            })
        ));
    }

    #[test]
    fn fixed_comparison_preserves_sql_null_semantics() {
        assert!(!comparison_matches::<i32>(
            None,
            Some(1),
            ComparisonType::GreaterThan
        ));
        assert!(comparison_matches::<i32>(
            None,
            Some(1),
            ComparisonType::DistinctFrom
        ));
        assert!(comparison_matches::<i32>(
            None,
            None,
            ComparisonType::NotDistinctFrom
        ));
    }
}
