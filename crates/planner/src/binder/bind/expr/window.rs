// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Binds window functions, partition/order, and frames. Named windows and `EXCLUDE` are not supported yet.

use super::call::{apply_implicit_casts, bind_window_aggregate_invocation};
use crate::binder::bind::expr::{self, ExpressionBinder};
use crate::binder::Binder;
use crate::expression::{
    CastExpression, Expression, ExpressionIterator, ExpressionVisitDecision, OrderByExpression,
    WindowExpression, WindowFrame, WindowFrameBound, WindowFrameType,
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
    schema: Option<&str>,
    func_name: &str,
    args: Vec<Expr>,
    distinct: bool,
    filter: Option<Expr>,
    arg_order_bys: Vec<OrderByExpr>,
    over: Window,
    ignore_nulls: bool,
) -> Result<Expression> {
    // Parse the window specification before function binding so both native
    // and aggregate invocations share one clause-binding path.
    let spec = match over {
        Window::WindowSpec(spec) => spec,
        Window::WindowReference(_name) => {
            return Err(paro_error::not_implemented(
                "Named windows not supported yet",
            ));
        }
    };

    let partitions = bind_partition_by(binder, spec.partition_by)?;
    let orders = bind_order_by(binder, spec.order_by)?;

    let native_namespace = schema.is_none_or(|schema| schema.eq_ignore_ascii_case("pg_catalog"));
    if native_namespace && is_native_window_function(func_name) {
        if distinct || filter.is_some() || !arg_order_bys.is_empty() {
            return Err(paro_error::syntax(format!(
                "native window function '{}' does not accept aggregate modifiers",
                func_name
            )));
        }
        let mut bound_args = Vec::with_capacity(args.len());
        let mut arg_types = Vec::with_capacity(args.len());
        for arg in args {
            let bound = expr::bind_expression(binder, arg)?;
            arg_types.push(bound.return_type());
            bound_args.push(bound);
        }
        let window_func = resolve_native_window_function(func_name, &arg_types)?;
        apply_implicit_casts(
            &mut bound_args,
            &arg_types,
            &window_func.arguments,
            binder.cast_functions.as_ref(),
        )?;
        let default_frame = WindowFrame::get_default_frame(&window_func);
        let frame = bind_window_frame(binder, spec.window_frame, default_frame)?;
        return Ok(Expression::Window(WindowExpression::native(
            window_func,
            bound_args,
            partitions,
            orders,
            frame,
            ignore_nulls,
        )));
    }

    if ignore_nulls {
        return Err(paro_error::syntax(format!(
            "aggregate window function '{}' does not accept IGNORE NULLS",
            func_name
        )));
    }
    if distinct || !arg_order_bys.is_empty() {
        return Err(paro_error::not_implemented(
            "DISTINCT and argument ORDER BY on aggregate windows",
        ));
    }

    let aggregate = bind_window_aggregate_invocation(
        binder,
        schema,
        func_name,
        args,
        distinct,
        filter,
        arg_order_bys,
    )?;
    let frame = bind_window_frame(binder, spec.window_frame, WindowFrame::default())?;

    Ok(Expression::Window(WindowExpression::aggregate(
        aggregate, partitions, orders, frame,
    )))
}

fn is_native_window_function(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "row_number"
            | "rank"
            | "dense_rank"
            | "percent_rank"
            | "cume_dist"
            | "ntile"
            | "lead"
            | "lag"
            | "first_value"
            | "last_value"
            | "nth_value"
    )
}

/// Resolve a native window function by name and argument types.
fn resolve_native_window_function(name: &str, arg_types: &[LogicalType]) -> Result<WindowFunction> {
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
    default_frame: WindowFrame,
) -> Result<WindowFrame> {
    match frame {
        Some(frame) => {
            validate_window_frame_bounds(&frame)?;
            let frame_type = match frame.units {
                WindowFrameUnits::Rows => WindowFrameType::Rows,
                WindowFrameUnits::Range => WindowFrameType::Range,
            };

            let (start_bound, start_is_preceding) =
                bind_frame_bound(binder, &frame.start_bound, frame_type)?;

            let (end_bound, end_is_preceding) =
                bind_frame_bound(binder, &frame.end_bound, frame_type)?;

            Ok(WindowFrame {
                frame_type,
                start_bound,
                start_is_preceding,
                end_bound,
                end_is_preceding,
            })
        }
        None => Ok(default_frame),
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
    frame_type: WindowFrameType,
) -> Result<(WindowFrameBound, bool)> {
    match bound {
        AstWindowFrameBound::CurrentRow => Ok((WindowFrameBound::CurrentRow, false)),
        AstWindowFrameBound::Preceding(None) => Ok((WindowFrameBound::Unbounded, true)),
        AstWindowFrameBound::Following(None) => Ok((WindowFrameBound::Unbounded, false)),
        AstWindowFrameBound::Preceding(Some(expr)) => {
            let bound_expr = bind_frame_offset(binder, (**expr).clone(), frame_type)?;
            Ok((WindowFrameBound::Offset(Box::new(bound_expr)), true))
        }
        AstWindowFrameBound::Following(Some(expr)) => {
            let bound_expr = bind_frame_offset(binder, (**expr).clone(), frame_type)?;
            Ok((WindowFrameBound::Offset(Box::new(bound_expr)), false))
        }
    }
}

fn bind_frame_offset(
    binder: &mut Binder,
    expression: Expr,
    frame_type: WindowFrameType,
) -> Result<Expression> {
    let mut offset_binder = ExpressionBinder::new(binder);
    offset_binder.allow_aggregates = false;
    offset_binder.allow_window = false;
    offset_binder.allow_default = false;
    let mut bound = offset_binder.bind(expression)?;

    let mut row_dependent = offset_binder.has_bound_columns();
    drop(offset_binder);
    ExpressionIterator::visit(&bound, &mut |node| match node {
        Expression::ColumnRef(_)
        | Expression::Aggregate(_)
        | Expression::Window(_)
        | Expression::Subquery(_) => {
            row_dependent = true;
            ExpressionVisitDecision::SkipChildren
        }
        _ => ExpressionVisitDecision::Descend,
    });
    if row_dependent {
        return Err(paro_error::windowing_error(
            "window frame offset must not contain row-dependent variables",
        ));
    }

    if frame_type == WindowFrameType::Rows {
        bound = CastExpression::add_cast_if_needed(
            bound,
            LogicalType::BigInt,
            binder.cast_functions.as_ref(),
        )?;
    }
    Ok(bound)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binder::test_utils::test_binder_with_search_path;
    use crate::operator::LogicalOperator;
    use crate::plan::LogicalPlan;
    use paro_catalog::collection::InstallMode;
    use paro_catalog::entry::{AggregateFunctionCatalogEntry, CatalogEntryEnum, CatalogType};
    use paro_catalog::search_path::CatalogSearchEntry;
    use paro_function::aggregate::distributive::{
        count::get_count_star_function, minmax::get_min_function, sum::get_sum_function,
    };
    use paro_function::aggregate::AggregateFunctionSet;
    use paro_parser::ast::Literal;
    use std::sync::Arc;

    fn bind_window_plan(sql: &str) -> Result<LogicalPlan> {
        let mut binder =
            test_binder_with_search_path(vec![CatalogSearchEntry::schema_only("public")]);
        install_test_aggregate(&binder, get_min_function());
        install_test_aggregate(&binder, get_sum_function());
        let mut count_star = AggregateFunctionSet::new("count_star".to_string());
        count_star.add_function(get_count_star_function());
        install_test_aggregate(&binder, count_star);
        let statement = paro_parser::parse_one(sql)?.stmt;
        Ok(binder.bind(statement)?.plan)
    }

    fn bind_single_window(sql: &str) -> Result<WindowExpression> {
        let plan = bind_window_plan(sql)?;
        find_single_window(&plan.operator)
            .cloned()
            .ok_or_else(|| paro_error::internal("expected one planned window"))
    }

    fn find_single_window(operator: &LogicalOperator) -> Option<&WindowExpression> {
        if let LogicalOperator::Window(window) = operator {
            let [expression] = window.expressions.as_slice() else {
                return None;
            };
            return Some(expression);
        }
        operator
            .children()
            .into_iter()
            .find_map(|child| find_single_window(&child.operator))
    }

    fn install_test_aggregate(
        binder: &Binder,
        set: paro_function::aggregate::AggregateFunctionSet,
    ) {
        let txn = binder.catalog_txn_view();
        let schema = binder
            .catalog()
            .get_schema(&txn, "public")
            .expect("public schema should exist");
        let entry = Arc::new(AggregateFunctionCatalogEntry::new(
            schema.base.catalog.clone(),
            schema.base.name.clone(),
            set,
            schema.object_id_allocator().allocate(),
            0,
        ));
        let _ = schema
            .collection(CatalogType::AggregateFunction)
            .expect("aggregate function collection")
            .install_committed(
                Arc::new(CatalogEntryEnum::AggregateFunction(entry)),
                InstallMode::RejectExisting,
            );
    }

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

    #[test]
    fn binds_full_partition_aggregate_window_to_real_kernel() {
        let window = bind_single_window(
            "SELECT min(x) OVER (PARTITION BY g) \
             FROM (VALUES (1.25::DECIMAL(9,2), 1)) AS t(x, g)",
        )
        .expect("bind aggregate window");
        let aggregate = window.aggregate_invocation().expect("aggregate invocation");

        assert_eq!(aggregate.function.name, "min");
        assert!(aggregate
            .function
            .execution_semantics_equal(&aggregate.function));
        assert_eq!(aggregate.children.len(), 1);
        assert_eq!(
            aggregate.children[0].return_type(),
            LogicalType::Decimal {
                precision: 9,
                scale: 2,
            }
        );
        assert_eq!(window.partitions.len(), 1);
        assert!(window.orders.is_empty());
        assert!(window.frame.covers_whole_partition(false));
        window.verify_bound_contract().expect("bound contract");
    }

    #[test]
    fn aggregate_window_accepts_explicit_double_unbounded_rows_frame() {
        let window = bind_single_window(
            "SELECT sum(x) OVER (PARTITION BY g ROWS BETWEEN UNBOUNDED PRECEDING \
             AND UNBOUNDED FOLLOWING) FROM (VALUES (1, 1)) AS t(x, g)",
        )
        .expect("bind explicit full frame");
        assert!(window.aggregate_invocation().is_some());
        assert!(window.frame.covers_whole_partition(false));
    }

    #[test]
    fn count_star_window_uses_zero_argument_aggregate_kernel() {
        let window =
            bind_single_window("SELECT count(*) OVER (PARTITION BY g) FROM (VALUES (1)) AS t(g)")
                .expect("bind count-star window");
        let aggregate = window.aggregate_invocation().expect("aggregate invocation");

        assert_eq!(aggregate.function.name, "count_star");
        assert!(aggregate.children.is_empty());
        assert_eq!(window.return_type(), LogicalType::BigInt);
        window.verify_bound_contract().expect("bound contract");
    }

    #[test]
    fn aggregate_window_preserves_ordered_and_bounded_frames_for_the_sort_fallback() {
        for sql in [
            "SELECT sum(x) OVER (ORDER BY g) FROM (VALUES (1, 1)) AS t(x, g)",
            "SELECT sum(x) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) \
             FROM (VALUES (1)) AS t(x)",
        ] {
            let window = bind_single_window(sql).expect("bind aggregate window fallback");
            assert!(window.aggregate_invocation().is_some());
            assert!(!window
                .frame
                .covers_whole_partition(!window.orders.is_empty()));
            window.verify_bound_contract().expect("bound contract");
        }
    }

    #[test]
    fn aggregate_window_accepts_query_level_aggregate_inputs() {
        let plan = bind_window_plan(
            "SELECT sum(sum(x)) OVER (PARTITION BY g ORDER BY g) \
             FROM (VALUES (1, 1), (2, 1)) AS t(x, g) GROUP BY g",
        )
        .expect("bind grouped aggregate under window");
        let window = find_single_window(&plan.operator).expect("planned window");
        let aggregate = window.aggregate_invocation().expect("aggregate window");

        assert!(matches!(
            aggregate.children.as_slice(),
            [Expression::ColumnRef(_)]
        ));
        assert!(plan_contains_grouped_aggregate(&plan.operator));
        window.verify_bound_contract().expect("bound contract");
    }

    #[test]
    fn rows_frame_offsets_are_row_independent_bigints() {
        let window = bind_single_window(
            "SELECT sum(x) OVER (ORDER BY x ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) \
             FROM (VALUES (1), (2)) AS t(x)",
        )
        .expect("bind constant ROWS offset");
        let WindowFrameBound::Offset(offset) = &window.frame.start_bound else {
            panic!("expected ROWS frame offset");
        };
        assert_eq!(offset.return_type(), LogicalType::BigInt);

        let error = bind_window_plan(
            "SELECT sum(x) OVER (ORDER BY x ROWS BETWEEN x PRECEDING AND CURRENT ROW) \
             FROM (VALUES (1), (2)) AS t(x)",
        )
        .expect_err("row-dependent ROWS offset must be rejected");
        assert!(
            error
                .to_string()
                .contains("window frame offset must not contain row-dependent variables"),
            "{error}"
        );
    }

    fn plan_contains_grouped_aggregate(operator: &LogicalOperator) -> bool {
        matches!(operator, LogicalOperator::Aggregate(_))
            || operator
                .children()
                .into_iter()
                .any(|child| plan_contains_grouped_aggregate(&child.operator))
    }

    #[test]
    fn native_window_binding_remains_native() {
        let window = bind_single_window(
            "SELECT row_number() OVER (PARTITION BY g ORDER BY x) \
             FROM (VALUES (1, 1)) AS t(x, g)",
        )
        .expect("bind native window");
        let (function, arguments) = window.native_invocation().expect("native invocation");
        assert_eq!(function.name, "row_number");
        assert!(arguments.is_empty());
        assert_eq!(window.orders.len(), 1);
    }

    #[test]
    fn native_window_arguments_are_cast_to_the_bound_contract() {
        let window =
            bind_single_window("SELECT ntile(4) OVER (ORDER BY x) FROM (VALUES (1)) AS t(x)")
                .expect("bind ntile window");
        let (function, arguments) = window.native_invocation().expect("native invocation");

        assert_eq!(function.name, "ntile");
        assert_eq!(arguments.len(), 1);
        assert_eq!(arguments[0].return_type(), LogicalType::BigInt);
        window.verify_bound_contract().expect("bound contract");
    }
}
