//! Bind CASE Expression
//!
//!

use crate::binder::bind::expr::ExpressionBinder;
use crate::expression::{CaseExpression, CastExpression, ConstantExpression, Expression};
use paro_common::error::Result;
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_parser::ast::Expr;

/// Binds a CASE expression.
pub fn bind_case(
    binder: &mut ExpressionBinder,
    operand: Option<Box<Expr>>,
    conditions: Vec<Expr>,
    results: Vec<Expr>,
    else_result: Option<Box<Expr>>,
) -> Result<Expression> {
    // Transform Simple CASE to Searched CASE at AST level if operand exists
    // Simple CASE: CASE x WHEN v1 THEN r1 WHEN v2 THEN r2 ELSE e END
    // Searched CASE: CASE WHEN x = v1 THEN r1 WHEN x = v2 THEN r2 ELSE e END
    let ast_conditions = if let Some(operand_expr) = operand {
        // Transform each WHEN condition to an equality comparison at AST level
        let mut transformed = Vec::with_capacity(conditions.len());
        for cond in conditions {
            // Create AST: operand = condition
            let comparison_expr = Expr::BinaryOp {
                span: None,
                op: paro_parser::ast::BinaryOperator::Eq,
                left: operand_expr.clone(),
                right: Box::new(cond),
            };
            transformed.push(comparison_expr);
        }
        transformed
    } else {
        // Searched CASE: use conditions as-is
        conditions
    };

    // 1. Bind all conditions and results
    let mut bound_conditions = Vec::with_capacity(ast_conditions.len());
    for cond in ast_conditions {
        bound_conditions.push(binder.bind_child(cond)?);
    }

    let mut bound_results = Vec::with_capacity(results.len());
    for res in results {
        bound_results.push(binder.bind_child(res)?);
    }

    let mut bound_else = if let Some(else_expr) = else_result {
        binder.bind_child(*else_expr)?
    } else {
        Expression::Constant(ConstantExpression {
            value: Value::Null(LogicalType::Null),
            return_type: LogicalType::Null,
        })
    };

    // 2. Determine common return type across all branches
    let mut return_type = bound_else.return_type();
    for res in &bound_results {
        return_type = LogicalType::max_logical_type(&return_type, &res.return_type());
    }

    // 3. Cast all result branches to the common return type
    for res in &mut bound_results {
        if res.return_type() != return_type {
            *res = CastExpression::add_cast_if_needed(
                res.clone(),
                return_type.clone(),
                &binder.binder.cast_functions,
            )?;
        }
    }
    if bound_else.return_type() != return_type {
        bound_else = CastExpression::add_cast_if_needed(
            bound_else,
            return_type.clone(),
            &binder.binder.cast_functions,
        )?;
    }

    // 4. Build the nested CASE expression tree from back to front
    let mut current_expr = bound_else;
    for (cond, res) in bound_conditions
        .into_iter()
        .zip(bound_results.into_iter())
        .rev()
    {
        current_expr = Expression::Case(CaseExpression::new(
            cond,
            res,
            current_expr,
            return_type.clone(),
        ));
    }

    Ok(current_expr)
}
