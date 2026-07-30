// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Binds window functions, partition/order, and frames. Named windows and `EXCLUDE` are not supported yet.

use crate::binder::bind::expr;
use crate::binder::Binder;
use crate::expression::{
    Expression, OrderByExpression, WindowExpression, WindowFrame, WindowFrameBound, WindowFrameType,
};
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_function::window::WindowFunction;
use paro_parser::ast::{
    Expr, OrderByExpr, Window, WindowFrame as AstWindowFrame,
    WindowFrameBound as AstWindowFrameBound, WindowFrameUnits,
};

/// Bind a window expression.
pub fn bind_window_expression(
    binder: &mut Binder,
    func_name: &str,
    args: Vec<Expr>,
    distinct: bool,
    filter: Option<Expr>,
    arg_order_bys: Vec<OrderByExpr>,
    over: Window,
    ignore_nulls: bool,
) -> Result<Expression> {
    if distinct || filter.is_some() || !arg_order_bys.is_empty() {
        return Err(paro_error::not_implemented(
            "Aggregate DISTINCT/FILTER/WITHIN GROUP on window functions",
        ));
    }

    // 1. Bind function arguments
    let mut bound_args = Vec::new();
    let mut arg_types = Vec::new();

    for arg in args {
        let bound = expr::bind_expression(binder, arg)?;
        arg_types.push(bound.return_type());
        bound_args.push(bound);
    }

    // 2. Resolve window function
    let window_func = resolve_window_function(func_name, &arg_types)?;

    // 3. Parse window specification
    let spec = match over {
        Window::WindowSpec(spec) => spec,
        Window::WindowReference(_name) => {
            return Err(paro_error::not_implemented(
                "Named windows not supported yet",
            ));
        }
    };

    // 4. Bind PARTITION BY
    let partitions = bind_partition_by(binder, spec.partition_by)?;

    // 5. Bind ORDER BY
    let orders = bind_order_by(binder, spec.order_by)?;

    // 6. Bind window frame
    let frame = bind_window_frame(binder, spec.window_frame, &window_func)?;

    let return_type = window_func.return_type.clone();

    Ok(Expression::Window(WindowExpression {
        function: window_func,
        children: bound_args,
        partitions,
        orders,
        frame,
        ignore_nulls,
        return_type,
    }))
}

/// Resolve window function by name and argument types.
fn resolve_window_function(name: &str, arg_types: &[LogicalType]) -> Result<WindowFunction> {
    let name_lower = name.to_lowercase();

    match name_lower.as_str() {
        // Ranking functions (no arguments)
        "row_number" => {
            if !arg_types.is_empty() {
                return Err(paro_error::syntax("ROW_NUMBER() takes no arguments"));
            }
            Ok(WindowFunction::row_number())
        }
        "rank" => {
            if !arg_types.is_empty() {
                return Err(paro_error::syntax("RANK() takes no arguments"));
            }
            Ok(WindowFunction::rank())
        }
        "dense_rank" => {
            if !arg_types.is_empty() {
                return Err(paro_error::syntax("DENSE_RANK() takes no arguments"));
            }
            Ok(WindowFunction::dense_rank())
        }
        "percent_rank" => {
            if !arg_types.is_empty() {
                return Err(paro_error::syntax("PERCENT_RANK() takes no arguments"));
            }
            Ok(WindowFunction::percent_rank())
        }
        "cume_dist" => {
            if !arg_types.is_empty() {
                return Err(paro_error::syntax("CUME_DIST() takes no arguments"));
            }
            Ok(WindowFunction::cume_dist())
        }
        "ntile" => {
            if arg_types.len() != 1 {
                return Err(paro_error::syntax("NTILE() requires exactly 1 argument"));
            }
            Ok(WindowFunction::ntile())
        }

        // Value functions
        "lead" => match arg_types.len() {
            1 => Ok(WindowFunction::lead(arg_types[0].clone())),
            2 => Ok(WindowFunction::lead_with_offset(arg_types[0].clone())),
            3 => Ok(WindowFunction::lead_with_default(arg_types[0].clone())),
            _ => Err(paro_error::syntax("LEAD() requires 1, 2, or 3 arguments")),
        },
        "lag" => match arg_types.len() {
            1 => Ok(WindowFunction::lag(arg_types[0].clone())),
            2 => Ok(WindowFunction::lag_with_offset(arg_types[0].clone())),
            3 => Ok(WindowFunction::lag_with_default(arg_types[0].clone())),
            _ => Err(paro_error::syntax("LAG() requires 1, 2, or 3 arguments")),
        },
        "first_value" => {
            if arg_types.len() != 1 {
                return Err(paro_error::syntax(
                    "FIRST_VALUE() requires exactly 1 argument",
                ));
            }
            Ok(WindowFunction::first_value(arg_types[0].clone()))
        }
        "last_value" => {
            if arg_types.len() != 1 {
                return Err(paro_error::syntax(
                    "LAST_VALUE() requires exactly 1 argument",
                ));
            }
            Ok(WindowFunction::last_value(arg_types[0].clone()))
        }
        "nth_value" => {
            if arg_types.len() != 2 {
                return Err(paro_error::syntax(
                    "NTH_VALUE() requires exactly 2 arguments",
                ));
            }
            Ok(WindowFunction::nth_value(arg_types[0].clone()))
        }

        _ => Err(paro_error::syntax(format!(
            "Unknown window function: {}",
            name
        ))),
    }
}

/// Bind PARTITION BY expressions.
fn bind_partition_by(binder: &mut Binder, exprs: Vec<Expr>) -> Result<Vec<Expression>> {
    let mut result = Vec::with_capacity(exprs.len());
    for expr in exprs {
        result.push(expr::bind_expression(binder, expr)?);
    }
    Ok(result)
}

/// Bind ORDER BY expressions.
fn bind_order_by(
    binder: &mut Binder,
    order_by: Vec<OrderByExpr>,
) -> Result<Vec<OrderByExpression>> {
    let mut result = Vec::with_capacity(order_by.len());

    for order in order_by {
        let expression = expr::bind_expression(binder, order.expr)?;

        let ascending = order.asc.unwrap_or(true);
        let nulls_first = order.nulls_first.unwrap_or(!ascending);

        result.push(OrderByExpression {
            expression,
            ascending,
            nulls_first,
        });
    }

    Ok(result)
}

/// Bind window frame specification.
fn bind_window_frame(
    binder: &mut Binder,
    frame: Option<AstWindowFrame>,
    func: &WindowFunction,
) -> Result<WindowFrame> {
    match frame {
        Some(frame) => {
            validate_window_frame_bounds(&frame)?;
            let frame_type = match frame.units {
                WindowFrameUnits::Rows => WindowFrameType::Rows,
                WindowFrameUnits::Range => WindowFrameType::Range,
            };

            let (start_bound, start_is_preceding) = bind_frame_bound(binder, &frame.start_bound)?;

            let (end_bound, end_is_preceding) = bind_frame_bound(binder, &frame.end_bound)?;

            Ok(WindowFrame {
                frame_type,
                start_bound,
                start_is_preceding,
                end_bound,
                end_is_preceding,
            })
        }
        None => {
            // Default frame depends on function type
            Ok(get_default_frame(func))
        }
    }
}

/// Reject structurally impossible frame extents before binding their offsets.
///
/// Offset values within the same direction can still produce an empty frame
/// (for example, `1 PRECEDING` through `2 PRECEDING`) and are intentionally
/// left to execution. These checks cover only orderings forbidden by SQL.
fn validate_window_frame_bounds(frame: &AstWindowFrame) -> Result<()> {
    use AstWindowFrameBound::{CurrentRow, Following, Preceding};

    if matches!(&frame.start_bound, Following(None)) {
        return Err(paro_error::windowing_error(
            "frame start cannot be UNBOUNDED FOLLOWING",
        ));
    }
    if matches!(&frame.end_bound, Preceding(None)) {
        return Err(paro_error::windowing_error(
            "frame end cannot be UNBOUNDED PRECEDING",
        ));
    }
    if matches!(&frame.start_bound, CurrentRow) && matches!(&frame.end_bound, Preceding(Some(_))) {
        return Err(paro_error::windowing_error(
            "frame starting from current row cannot have preceding rows",
        ));
    }
    if matches!(&frame.start_bound, Following(Some(_)))
        && matches!(&frame.end_bound, CurrentRow | Preceding(Some(_)))
    {
        return Err(paro_error::windowing_error(
            "frame starting from following row cannot have preceding rows",
        ));
    }
    Ok(())
}

/// Bind a single frame bound.
fn bind_frame_bound(
    binder: &mut Binder,
    bound: &AstWindowFrameBound,
) -> Result<(WindowFrameBound, bool)> {
    match bound {
        AstWindowFrameBound::CurrentRow => Ok((WindowFrameBound::CurrentRow, false)),
        AstWindowFrameBound::Preceding(None) => Ok((WindowFrameBound::Unbounded, true)),
        AstWindowFrameBound::Following(None) => Ok((WindowFrameBound::Unbounded, false)),
        AstWindowFrameBound::Preceding(Some(expr)) => {
            let bound_expr = expr::bind_expression(binder, (**expr).clone())?;
            Ok((WindowFrameBound::Offset(Box::new(bound_expr)), true))
        }
        AstWindowFrameBound::Following(Some(expr)) => {
            let bound_expr = expr::bind_expression(binder, (**expr).clone())?;
            Ok((WindowFrameBound::Offset(Box::new(bound_expr)), false))
        }
    }
}

/// Get default frame for a window function.
fn get_default_frame(func: &WindowFunction) -> WindowFrame {
    WindowFrame::get_default_frame(func)
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_parser::ast::Literal;

    fn offset_bound(following: bool) -> AstWindowFrameBound {
        let offset = Box::new(Expr::Literal {
            span: None,
            value: Literal::UInt64(1),
        });
        if following {
            AstWindowFrameBound::Following(Some(offset))
        } else {
            AstWindowFrameBound::Preceding(Some(offset))
        }
    }

    fn frame(start_bound: AstWindowFrameBound, end_bound: AstWindowFrameBound) -> AstWindowFrame {
        AstWindowFrame {
            units: WindowFrameUnits::Rows,
            start_bound,
            end_bound,
        }
    }

    #[test]
    fn rejects_structurally_invalid_window_frames() {
        let cases = [
            (
                frame(
                    AstWindowFrameBound::Following(None),
                    AstWindowFrameBound::Following(None),
                ),
                "frame start cannot be UNBOUNDED FOLLOWING",
            ),
            (
                frame(
                    AstWindowFrameBound::Preceding(None),
                    AstWindowFrameBound::Preceding(None),
                ),
                "frame end cannot be UNBOUNDED PRECEDING",
            ),
            (
                frame(AstWindowFrameBound::CurrentRow, offset_bound(false)),
                "frame starting from current row cannot have preceding rows",
            ),
            (
                frame(offset_bound(true), AstWindowFrameBound::CurrentRow),
                "frame starting from following row cannot have preceding rows",
            ),
            (
                frame(offset_bound(true), offset_bound(false)),
                "frame starting from following row cannot have preceding rows",
            ),
        ];

        for (frame, expected) in cases {
            let error = validate_window_frame_bounds(&frame).unwrap_err();
            assert!(error.to_string().contains(expected), "{error}");
        }
    }

    #[test]
    fn allows_offset_frames_that_can_be_empty_at_execution() {
        let frame = frame(offset_bound(false), offset_bound(false));
        validate_window_frame_bounds(&frame)
            .expect("same-direction offsets are structurally valid");
    }
}
