// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::dispatcher::ExpressionBinder;
use crate::expression::{
    CastExpression, ComparisonExpression, ComparisonType, ConjunctionExpression, ConjunctionType,
    ConstantExpression, Expression, OperatorExpression, OperatorType,
};
use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_parser::ast::Expr;

/// Binds a comparison expression (e.g., a = b, a < b).
pub fn bind_comparison(
    binder: &mut ExpressionBinder,
    left: Expr,
    right: Expr,
    comparison_type: ComparisonType,
) -> Result<Expression> {
    let mut left = binder.bind_child(left)?;
    let mut right = binder.bind_child(right)?;
    let left_sql_type = left.get_expression_return_type();
    let right_sql_type = right.get_expression_return_type();
    let input_type = try_bind_comparison(&left_sql_type, &right_sql_type, comparison_type)?;
    let target_type = input_type.normalize_type();

    if left.return_type() != target_type {
        left = CastExpression::add_cast_if_needed(
            left,
            target_type.clone(),
            &binder.binder.cast_functions,
        )?;
    }
    if right.return_type() != target_type {
        right =
            CastExpression::add_cast_if_needed(right, target_type, &binder.binder.cast_functions)?;
    }

    Ok(Expression::Comparison(ComparisonExpression::new(
        comparison_type,
        left,
        right,
    )))
}

/// Try to bind a comparison and determine the common input type.
pub fn try_bind_comparison(
    left_type: &LogicalType,
    right_type: &LogicalType,
    _comparison_type: ComparisonType,
) -> Result<LogicalType> {
    let result_type = LogicalType::max_logical_type(left_type, right_type);

    let result_type = if matches!(&result_type, LogicalType::Varchar) {
        if !matches!(left_type, LogicalType::Varchar | LogicalType::StringLiteral)
            && switch_varchar_comparison(left_type)
        {
            left_type.normalize_type()
        } else if !matches!(
            right_type,
            LogicalType::Varchar | LogicalType::StringLiteral
        ) && switch_varchar_comparison(right_type)
        {
            right_type.normalize_type()
        } else {
            result_type
        }
    } else {
        result_type
    };

    if result_type == LogicalType::Unknown {
        return Err(paro_error::syntax(format!(
            "Cannot compare values of type {} and {} - an explicit cast is required",
            left_type, right_type
        )));
    }

    Ok(result_type)
}

fn switch_varchar_comparison(ty: &LogicalType) -> bool {
    matches!(
        ty,
        LogicalType::Boolean
            | LogicalType::TinyInt
            | LogicalType::SmallInt
            | LogicalType::Integer
            | LogicalType::BigInt
            | LogicalType::HugeInt
            | LogicalType::Float
            | LogicalType::Double
            | LogicalType::Decimal { .. }
            | LogicalType::UTinyInt
            | LogicalType::USmallInt
            | LogicalType::UInteger
            | LogicalType::UBigInt
            | LogicalType::UHugeInt
            | LogicalType::Date
            | LogicalType::Time
            | LogicalType::Timestamp
            | LogicalType::TimestampTz
            | LogicalType::Interval
            | LogicalType::IntegerLiteral(_)
            | LogicalType::Uuid
    )
}

/// Binds a conjunction expression (e.g., AND, OR).
pub fn bind_conjunction(
    binder: &mut ExpressionBinder,
    left: Expr,
    right: Expr,
    conjunction_type: ConjunctionType,
) -> Result<Expression> {
    let left = binder.bind_child(left)?;
    let right = binder.bind_child(right)?;

    Ok(Expression::Conjunction(ConjunctionExpression::new(
        conjunction_type,
        vec![left, right],
    )))
}

/// Binds a unary NOT expression.
pub fn bind_not(binder: &mut ExpressionBinder, expr: Expr) -> Result<Expression> {
    let bound_child = binder.bind_child(expr)?;
    Ok(Expression::Operator(OperatorExpression::new_unary(
        OperatorType::Not,
        bound_child,
        LogicalType::Boolean,
    )))
}

/// Binds a LIKE or NOT LIKE expression.
pub fn bind_like(
    binder: &mut ExpressionBinder,
    left: Expr,
    right: Expr,
    not: bool,
) -> Result<Expression> {
    let mut left = binder.bind_child(left)?;
    let mut right = binder.bind_child(right)?;

    if left.return_type() != LogicalType::Varchar {
        left = CastExpression::add_cast_if_needed(
            left,
            LogicalType::Varchar,
            &binder.binder.cast_functions,
        )?;
    }
    if right.return_type() != LogicalType::Varchar {
        right = CastExpression::add_cast_if_needed(
            right,
            LogicalType::Varchar,
            &binder.binder.cast_functions,
        )?;
    }

    let like = Expression::Operator(OperatorExpression::new(
        OperatorType::Like,
        vec![left, right],
        LogicalType::Boolean,
    ));

    if not {
        Ok(Expression::Operator(OperatorExpression::new_unary(
            OperatorType::Not,
            like,
            LogicalType::Boolean,
        )))
    } else {
        Ok(like)
    }
}

/// Binds an IS NULL or IS NOT NULL expression.
pub fn bind_is_null(binder: &mut ExpressionBinder, expr: Expr, not: bool) -> Result<Expression> {
    let bound_child = binder.bind_child(expr)?;
    let op_type = if not {
        OperatorType::IsNotNull
    } else {
        OperatorType::IsNull
    };
    Ok(Expression::Operator(OperatorExpression::new_unary(
        op_type,
        bound_child,
        LogicalType::Boolean,
    )))
}

/// Binds an IN list expression.
pub fn bind_in_list(
    binder: &mut ExpressionBinder,
    expr: Expr,
    list: Vec<Expr>,
    not: bool,
) -> Result<Expression> {
    let bound_left = binder.bind_child(expr)?;
    let mut children = vec![bound_left];
    for item in list {
        children.push(binder.bind_child(item)?);
    }
    let op_type = if not {
        OperatorType::NotIn
    } else {
        OperatorType::In
    };
    Ok(Expression::Operator(OperatorExpression::new(
        op_type,
        children,
        LogicalType::Boolean,
    )))
}

/// Binds a BETWEEN expression.
pub fn bind_between(
    binder: &mut ExpressionBinder,
    expr: Expr,
    low: Expr,
    high: Expr,
    not: bool,
) -> Result<Expression> {
    let bound_expr = binder.bind_child(expr)?;
    let bound_low = binder.bind_child(low)?;
    let bound_high = binder.bind_child(high)?;

    if not {
        let left = Expression::Comparison(ComparisonExpression::new(
            ComparisonType::LessThan,
            bound_expr.clone(),
            bound_low,
        ));
        let right = Expression::Comparison(ComparisonExpression::new(
            ComparisonType::GreaterThan,
            bound_expr,
            bound_high,
        ));
        Ok(Expression::Conjunction(ConjunctionExpression::new(
            ConjunctionType::Or,
            vec![left, right],
        )))
    } else {
        let left = Expression::Comparison(ComparisonExpression::new(
            ComparisonType::GreaterThanOrEqual,
            bound_expr.clone(),
            bound_low,
        ));
        let right = Expression::Comparison(ComparisonExpression::new(
            ComparisonType::LessThanOrEqual,
            bound_expr,
            bound_high,
        ));
        Ok(Expression::Conjunction(ConjunctionExpression::new(
            ConjunctionType::And,
            vec![left, right],
        )))
    }
}

/// Binds an ARRAY constructor expression.
pub fn bind_array(binder: &mut ExpressionBinder, exprs: Vec<Expr>) -> Result<Expression> {
    let mut bound_exprs = Vec::with_capacity(exprs.len());
    for expr in exprs {
        bound_exprs.push(binder.bind_child(expr)?);
    }

    let child_type = if bound_exprs.is_empty() {
        LogicalType::Null
    } else {
        let mut current_type = bound_exprs[0].return_type();
        for expr in bound_exprs.iter().skip(1) {
            current_type = LogicalType::max_logical_type(&current_type, &expr.return_type());
        }
        current_type
    };

    for expr in &mut bound_exprs {
        if expr.return_type() != child_type {
            *expr = CastExpression::add_cast_if_needed(
                expr.clone(),
                child_type.clone(),
                &binder.binder.cast_functions,
            )?;
        }
    }

    let array_size = bound_exprs.len();
    let return_type = LogicalType::Array(Box::new(child_type.clone()), array_size);
    let all_constants = bound_exprs
        .iter()
        .all(|expr| matches!(expr, Expression::Constant(_)));
    if all_constants {
        let mut values = Vec::with_capacity(bound_exprs.len());
        for expr in bound_exprs {
            if let Expression::Constant(constant) = expr {
                values.push(constant.value);
            }
        }
        Ok(Expression::Constant(ConstantExpression {
            value: Value::array(child_type, values),
            return_type,
        }))
    } else {
        Ok(Expression::Operator(OperatorExpression::new(
            OperatorType::ArrayConstructor,
            bound_exprs,
            return_type,
        )))
    }
}

/// Binds a STRUCT (tuple) constructor expression.
pub fn bind_tuple(binder: &mut ExpressionBinder, exprs: Vec<Expr>) -> Result<Expression> {
    let mut bound_exprs = Vec::with_capacity(exprs.len());
    for expr in exprs {
        bound_exprs.push(binder.bind_child(expr)?);
    }

    let mut fields = Vec::with_capacity(bound_exprs.len());
    for (idx, expr) in bound_exprs.iter().enumerate() {
        fields.push((format!("field{}", idx), expr.return_type()));
    }

    let return_type = LogicalType::Struct(fields.clone());
    let all_constants = bound_exprs
        .iter()
        .all(|expr| matches!(expr, Expression::Constant(_)));
    if all_constants {
        let mut values = Vec::with_capacity(bound_exprs.len());
        for expr in bound_exprs {
            if let Expression::Constant(constant) = expr {
                values.push(constant.value);
            }
        }
        Ok(Expression::Constant(ConstantExpression {
            value: Value::struct_value(fields, values),
            return_type,
        }))
    } else {
        Ok(Expression::Operator(OperatorExpression::new(
            OperatorType::StructConstructor,
            bound_exprs,
            return_type,
        )))
    }
}

/// Binds a COALESCE expression.
pub fn bind_coalesce(binder: &mut ExpressionBinder, args: Vec<Expr>) -> Result<Expression> {
    let mut bound_args = Vec::new();
    for arg in args {
        bound_args.push(binder.bind_child(arg)?);
    }

    if bound_args.is_empty() {
        return Err(paro_error::syntax("COALESCE needs at least one argument"));
    }

    let mut return_type = bound_args[0].return_type();
    for arg in bound_args.iter().skip(1) {
        return_type = LogicalType::max_logical_type(&return_type, &arg.return_type());
    }

    for arg in &mut bound_args {
        if arg.return_type() != return_type {
            *arg = CastExpression::add_cast_if_needed(
                arg.clone(),
                return_type.clone(),
                &binder.binder.cast_functions,
            )?;
        }
    }

    Ok(Expression::Operator(OperatorExpression::new(
        OperatorType::Coalesce,
        bound_args,
        return_type,
    )))
}

/// Binds a MAP/ARRAY access expression.
pub fn bind_map_access(
    binder: &mut ExpressionBinder,
    expr: Expr,
    accessor: paro_parser::ast::MapAccessor,
) -> Result<Expression> {
    let bound_child = binder.bind_child(expr)?;
    match accessor {
        paro_parser::ast::MapAccessor::Bracket { key } => {
            let bound_key = binder.bind_child(*key)?;
            let child_type = match bound_child.return_type() {
                LogicalType::Array(child_type, _) => *child_type,
                LogicalType::List(child_type) => *child_type,
                _ => {
                    return Err(paro_error::type_mismatch(format!(
                        "Cannot index into type: {}",
                        bound_child.return_type()
                    )))
                }
            };
            Ok(Expression::Operator(OperatorExpression::new(
                OperatorType::ArrayExtract,
                vec![bound_child, bound_key],
                child_type,
            )))
        }
        _ => Err(paro_error::not_implemented(format!(
            "Map accessor not supported: {:?}",
            accessor
        ))),
    }
}
