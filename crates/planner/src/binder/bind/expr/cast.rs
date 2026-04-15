//! Bind Cast Expression
//!
//!

use crate::binder::bind::expr::ExpressionBinder;
use crate::binder::bind::type_name::bind_logical_type;
use crate::expression::{CastExpression, Expression};
use paro_common::error::Result;
use paro_parser::ast::Expr;

/// Binds a CAST expression.
pub fn bind_cast(
    binder: &mut ExpressionBinder,
    expr: Expr,
    target_type: paro_parser::ast::TypeName,
) -> Result<Expression> {
    let bound_expr = binder.bind_child(expr)?;
    let target = bind_logical_type(&target_type)?;

    CastExpression::add_explicit_cast(bound_expr, target, &binder.binder.cast_functions, false)
}

/// Binds a TRY_CAST expression.
pub fn bind_try_cast(
    binder: &mut ExpressionBinder,
    expr: Expr,
    target_type: paro_parser::ast::TypeName,
) -> Result<Expression> {
    let bound_expr = binder.bind_child(expr)?;
    let target = bind_logical_type(&target_type)?;

    CastExpression::add_explicit_cast(bound_expr, target, &binder.binder.cast_functions, true)
}
