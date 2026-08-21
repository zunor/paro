// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Evaluation of bound, context-independent constant expressions.
//!
//! Scalar functions and casts are evaluated through their bound execution
//! kernels. This keeps optimizer-time folding on the same semantic path as
//! query execution instead of maintaining a second arithmetic implementation.
//! Structural boolean nodes are evaluated locally only after their children
//! have become typed `Value`s; they implement SQL three-valued control flow
//! and do not duplicate scalar function or cast kernels.

use paro_common::allocator::{default_allocator, Allocator, MemoryTag};
use paro_common::chunk::Chunk;
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_common::vector::Vector;
use paro_function::scalar::cast::{CastContextDependency, CastExecCtx};
use paro_function::scalar::{
    FunctionData, FunctionExecContext, FunctionLocalState, FunctionNullHandling,
};
use paro_planner::expression::{ComparisonType, ConjunctionType, Expression, OperatorType};
use std::sync::Arc;

/// Evaluate a bound expression that is independent of rows and runtime
/// session state. Errors deliberately decline folding: normal execution then
/// reports the error at the statement's ordinary evaluation point.
pub(super) fn evaluate_constant(expr: &Expression) -> Option<Value> {
    match expr {
        Expression::Constant(constant) => Some(constant.value.clone()),
        Expression::Cast(cast) => evaluate_cast(cast),
        Expression::Function(function) => evaluate_function(function),
        Expression::Comparison(comparison) => evaluate_comparison(
            &evaluate_constant(&comparison.left)?,
            &evaluate_constant(&comparison.right)?,
            comparison.comparison_type,
        ),
        Expression::Conjunction(conjunction) => evaluate_conjunction(
            conjunction.children.iter().map(evaluate_constant),
            conjunction.conjunction_type,
        ),
        Expression::Operator(operator) => evaluate_operator(operator),
        Expression::Case(case) => match evaluate_constant(&case.check)? {
            Value::Boolean(true) => evaluate_constant(&case.result_if_true),
            Value::Boolean(false) | Value::Null(_) => evaluate_constant(&case.result_if_false),
            _ => None,
        },
        Expression::ColumnRef(_)
        | Expression::Parameter(_)
        | Expression::Reference(_)
        | Expression::Aggregate(_)
        | Expression::Subquery(_)
        | Expression::Window(_) => None,
    }
}

fn evaluate_cast(cast: &paro_planner::expression::CastExpression) -> Option<Value> {
    if cast.cast_info.context_dependency() == CastContextDependency::Runtime {
        return None;
    }
    let value = evaluate_constant(cast.child.as_ref())?;
    let allocator: Arc<dyn Allocator> = Arc::new(default_allocator());
    let source = singleton_vector(cast.child.return_type(), &value, allocator.clone())?;
    let mut result = Vector::try_new(cast.target_type.clone(), 1, allocator).ok()?;
    let runtime = ConstantFunctionContext::default();
    let context = CastExecCtx {
        runtime: &runtime,
        try_cast: cast.try_cast,
        cast_data: cast.cast_info.cast_data.as_deref(),
    };
    cast.cast_info
        .execute(&source, &mut result, 1, &context)
        .ok()?;
    Some(result.get_value(0))
}

fn evaluate_function(expression: &paro_planner::expression::FunctionExpression) -> Option<Value> {
    if !expression.is_foldable_native() {
        return None;
    }
    let values = expression
        .children
        .iter()
        .map(evaluate_constant)
        .collect::<Option<Vec<_>>>()?;
    if expression.function.null_handling == FunctionNullHandling::DefaultNullHandling
        && values.iter().any(Value::is_null)
    {
        return Some(Value::Null(expression.return_type.clone()));
    }

    let allocator: Arc<dyn Allocator> = Arc::new(default_allocator());
    let vectors = expression
        .children
        .iter()
        .zip(values.iter())
        .map(|(child, value)| singleton_vector(child.return_type(), value, allocator.clone()))
        .collect::<Option<Vec<_>>>()?;
    let input = Chunk::from_vectors(vectors, allocator.clone());
    let mut result = Vector::try_new(expression.return_type.clone(), 1, allocator).ok()?;

    let bind_data = expression.function.bind_data.as_deref();
    let initial_context = ConstantFunctionContext {
        bind_data,
        local_state: None,
    };
    let local_state = match expression.function.init_local_state {
        Some(initialize) => Some(initialize(&initial_context, bind_data).ok()?),
        None => None,
    };
    let context = ConstantFunctionContext {
        bind_data,
        local_state: local_state.as_deref(),
    };
    expression
        .function
        .execute(&input, &context, &mut result)
        .ok()?;
    Some(result.get_value(0))
}

fn singleton_vector(
    logical_type: LogicalType,
    value: &Value,
    allocator: Arc<dyn Allocator>,
) -> Option<Vector> {
    let mut vector = Vector::try_new(logical_type, 1, allocator).ok()?;
    vector.set_count(1);
    vector.set_value(0, value);
    Some(vector)
}

fn evaluate_comparison(left: &Value, right: &Value, comparison: ComparisonType) -> Option<Value> {
    if left.is_null() || right.is_null() {
        return match comparison {
            ComparisonType::DistinctFrom => Some(Value::Boolean(left.is_null() != right.is_null())),
            ComparisonType::NotDistinctFrom => {
                Some(Value::Boolean(left.is_null() && right.is_null()))
            }
            _ => Some(Value::Null(LogicalType::Boolean)),
        };
    }
    let ordering = left.partial_cmp(right)?;
    let value = match comparison {
        ComparisonType::Equal => ordering.is_eq(),
        ComparisonType::NotEqual => !ordering.is_eq(),
        ComparisonType::LessThan => ordering.is_lt(),
        ComparisonType::LessThanOrEqual => ordering.is_le(),
        ComparisonType::GreaterThan => ordering.is_gt(),
        ComparisonType::GreaterThanOrEqual => ordering.is_ge(),
        ComparisonType::DistinctFrom => !ordering.is_eq(),
        ComparisonType::NotDistinctFrom => ordering.is_eq(),
    };
    Some(Value::Boolean(value))
}

fn evaluate_conjunction(
    values: impl IntoIterator<Item = Option<Value>>,
    conjunction: ConjunctionType,
) -> Option<Value> {
    let mut has_null = false;
    for value in values {
        match (conjunction, value?) {
            (ConjunctionType::And, Value::Boolean(false)) => {
                return Some(Value::Boolean(false));
            }
            (ConjunctionType::Or, Value::Boolean(true)) => return Some(Value::Boolean(true)),
            (_, Value::Boolean(_)) => {}
            (_, Value::Null(_)) => has_null = true,
            _ => return None,
        }
    }
    if has_null {
        Some(Value::Null(LogicalType::Boolean))
    } else {
        Some(Value::Boolean(conjunction == ConjunctionType::And))
    }
}

fn evaluate_operator(operator: &paro_planner::expression::OperatorExpression) -> Option<Value> {
    match operator.operator_type {
        OperatorType::Not => match evaluate_constant(operator.children.first()?)? {
            Value::Boolean(value) => Some(Value::Boolean(!value)),
            Value::Null(_) => Some(Value::Null(LogicalType::Boolean)),
            _ => None,
        },
        OperatorType::IsNull | OperatorType::IsNotNull => {
            let is_null = evaluate_constant(operator.children.first()?)?.is_null();
            Some(Value::Boolean(
                if operator.operator_type == OperatorType::IsNull {
                    is_null
                } else {
                    !is_null
                },
            ))
        }
        OperatorType::Coalesce => {
            for child in &operator.children {
                let value = evaluate_constant(child)?;
                if !value.is_null() {
                    return Some(value);
                }
            }
            Some(Value::Null(operator.return_type.clone()))
        }
        OperatorType::In | OperatorType::NotIn => evaluate_in(operator),
        OperatorType::Like
        | OperatorType::ILike
        | OperatorType::ArrayConstructor
        | OperatorType::StructConstructor
        | OperatorType::ArrayExtract
        | OperatorType::ErrorIfMultipleRows => None,
    }
}

fn evaluate_in(operator: &paro_planner::expression::OperatorExpression) -> Option<Value> {
    let (needle, haystack) = operator.children.split_first()?;
    let needle = evaluate_constant(needle)?;
    if needle.is_null() {
        return Some(Value::Null(LogicalType::Boolean));
    }
    let mut has_null = false;
    for candidate in haystack {
        let candidate = evaluate_constant(candidate)?;
        if candidate.is_null() {
            has_null = true;
        } else if candidate == needle {
            return Some(Value::Boolean(operator.operator_type == OperatorType::In));
        }
    }
    if has_null {
        Some(Value::Null(LogicalType::Boolean))
    } else {
        Some(Value::Boolean(
            operator.operator_type == OperatorType::NotIn,
        ))
    }
}

#[derive(Default)]
struct ConstantFunctionContext<'a> {
    bind_data: Option<&'a dyn FunctionData>,
    local_state: Option<&'a dyn FunctionLocalState>,
}

impl FunctionExecContext for ConstantFunctionContext<'_> {
    fn current_database(&self) -> Option<&str> {
        None
    }

    fn current_schema(&self) -> Option<&str> {
        None
    }

    fn current_user(&self) -> Option<&str> {
        None
    }

    fn allocator(&self, _tag: MemoryTag) -> Arc<dyn Allocator> {
        Arc::new(default_allocator())
    }

    fn bind_data(&self) -> Option<&dyn FunctionData> {
        self.bind_data
    }

    fn local_state(&self) -> Option<&dyn FunctionLocalState> {
        self.local_state
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_function::scalar::date::register_temporal_arithmetic_functions;
    use paro_function::scalar::{FunctionSideEffects, FunctionStability, ScalarFunctionSet};
    use paro_planner::expression::{ConstantExpression, FunctionExpression};

    fn constant(value: Value) -> Expression {
        let return_type = value.logical_type();
        Expression::Constant(ConstantExpression { value, return_type })
    }

    #[test]
    fn bound_temporal_kernel_folds_date_interval_arithmetic() {
        let mut functions = ScalarFunctionSet::new("-".to_string());
        register_temporal_arithmetic_functions(&mut functions);
        let (function, arguments) = functions
            .bind(&[LogicalType::Date, LogicalType::Interval])
            .unwrap();
        let function = function
            .bind(&paro_function::scalar::ScalarBindInput::new(
                arguments,
                vec![None, None],
            ))
            .unwrap();
        assert_eq!(function.stability, FunctionStability::Consistent);
        assert_eq!(function.side_effects, FunctionSideEffects::NoSideEffects);

        let expression = Expression::Function(FunctionExpression::new(
            function,
            vec![
                constant(Value::Date(10_000)),
                constant(Value::Interval(0, 90, 0)),
            ],
            LogicalType::Timestamp,
        ));
        assert_eq!(
            evaluate_constant(&expression),
            Some(Value::Timestamp((10_000_i64 - 90) * 86_400_000_000))
        );
    }
}
