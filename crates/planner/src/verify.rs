// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Planner verification helpers.
//!
//! These checks enforce the contract between planner/subquery-flattening and
//! the physical planner/expression executor.

use paro_common::error::{self as paro_error, Result};

use crate::expression::{Expression, ExpressionIterator};
use crate::operator::{Join, LogicalOperator};

/// Verify that a logical plan is ready for physical planning/execution.
///
/// The planner must fully flatten dependent joins and remove subquery
/// expressions before this point.
pub fn verify_physical_planner_invariants(plan: &LogicalOperator) -> Result<()> {
    verify_operator(plan)
}

fn verify_operator(op: &LogicalOperator) -> Result<()> {
    match op {
        LogicalOperator::DependentJoin(_) => {
            return Err(paro_error::internal(
                "Planner verify failed: DependentJoin remained after flattening".to_string(),
            ));
        }
        LogicalOperator::Filter(filter) => {
            for expr in &filter.expressions {
                verify_expression(expr)?;
            }
        }
        LogicalOperator::Projection(proj) => {
            for expr in &proj.expressions {
                verify_expression(expr)?;
            }
        }
        LogicalOperator::ExternalProject(project) => {
            for expr in &project.expressions {
                verify_expression(&expr.expression)?;
            }
        }
        LogicalOperator::ExternalTable(table) => {
            verify_expression(&table.call_expression)?;
        }
        LogicalOperator::Limit(limit) => {
            if let Some(expr) = &limit.limit {
                verify_expression(expr)?;
            }
            if let Some(expr) = &limit.offset {
                verify_expression(expr)?;
            }
        }
        LogicalOperator::Order(order) => {
            for order in &order.orders {
                verify_expression(&order.expression)?;
            }
        }
        LogicalOperator::TopN(topn) => {
            for order in &topn.orders {
                verify_expression(&order.expression)?;
            }
        }
        LogicalOperator::Aggregate(agg) => {
            for expr in &agg.groups {
                verify_expression(expr)?;
            }
            for expr in &agg.aggregates {
                verify_expression(expr)?;
            }
        }
        LogicalOperator::Update(update) => {
            for expr in &update.expressions {
                verify_expression(expr)?;
            }
        }
        LogicalOperator::ExpressionGet(get) => {
            for row in &get.expressions {
                for expr in row {
                    verify_expression(expr)?;
                }
            }
        }
        LogicalOperator::Join(join) => match join {
            Join::Comparison(comp_join) => {
                for expr in &comp_join.duplicate_eliminated_columns {
                    verify_expression(expr)?;
                }
                for cond in &comp_join.conditions {
                    verify_expression(&cond.left)?;
                    verify_expression(&cond.right)?;
                }
            }
            Join::Any(any_join) => {
                verify_expression(&any_join.condition)?;
            }
            Join::Cross(_) => {}
        },
        LogicalOperator::Distinct(distinct) => {
            for expr in &distinct.distinct_targets {
                verify_expression(expr)?;
            }
            if let Some(order_by) = &distinct.order_by {
                for order in order_by {
                    verify_expression(&order.expression)?;
                }
            }
        }
        LogicalOperator::Window(window) => {
            let mut result = Ok(());
            for expression in &window.expressions {
                ExpressionIterator::enumerate_window_children(expression, |child| {
                    if result.is_ok() {
                        result = verify_expression(child);
                    }
                });
            }
            result?;
        }
        LogicalOperator::CreateIndex(create_index) => {
            for expr in &create_index.expressions {
                verify_expression(expr)?;
            }
            for expr in &create_index.unbound_expressions {
                verify_expression(expr)?;
            }
        }
        LogicalOperator::TableFunctionGet(table_function) => {
            for expr in &table_function.arguments {
                verify_expression(expr)?;
            }
        }
        LogicalOperator::SearchScan(search) => {
            for expr in &search.projections {
                verify_expression(expr)?;
            }
            for expr in &search.absorbed_predicates {
                verify_expression(expr)?;
            }
            for expr in &search.residual_predicates {
                verify_expression(expr)?;
            }
            verify_expression(&search.score_expression)?;
        }
        LogicalOperator::FullTextFilterScan(scan) => {
            verify_expression(&scan.match_expression)?;
            for expr in &scan.other_predicates {
                verify_expression(expr)?;
            }
            for expr in &scan.residual_predicates {
                verify_expression(expr)?;
            }
        }
        _ => {}
    }

    for child in op.children() {
        verify_operator(&child.operator)?;
    }

    Ok(())
}

fn verify_expression(expr: &Expression) -> Result<()> {
    if let Expression::Subquery(subquery) = expr {
        return Err(paro_error::internal(format!(
            "Planner verify failed: Expression::Subquery remained after flattening (state={:?})",
            subquery.planning_state
        )));
    }

    let mut result = Ok(());
    ExpressionIterator::enumerate_children(expr, |child| {
        if result.is_ok() {
            result = verify_expression(child);
        }
    });
    result
}

#[cfg(test)]
mod tests {
    use super::verify_physical_planner_invariants;
    use std::sync::Arc;

    use crate::binder::context::BindContext;
    use crate::expression::{
        ColumnRefExpression, ComparisonType, ConstantExpression, Expression, SubqueryExpression,
        SubqueryPlanningState, SubqueryType, WindowExpression, WindowFrame, WindowFrameBound,
        WindowFrameType,
    };
    use crate::operator::projection::Projection;
    use crate::operator::{ColumnBinding, DependentJoin, ExpressionGet, LogicalOperator};
    use crate::plan::LogicalPlan;
    use crate::plan::PlannedStatement;
    use paro_common::runtime_value::Value;
    use paro_common::types::LogicalType;
    use paro_function::window::WindowFunction;

    fn expression_get(table_index: usize) -> LogicalOperator {
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            table_index,
            vec![],
            vec!["v".to_string()],
            vec![LogicalType::Integer],
        ))
    }

    fn wrap(ctx: &BindContext, op: LogicalOperator) -> LogicalPlan {
        LogicalPlan::new(ctx, op)
    }

    fn dummy_subquery_expr() -> Expression {
        Expression::Subquery(SubqueryExpression {
            subquery_type: SubqueryType::Scalar,
            subquery: Arc::new(PlannedStatement {
                types: vec![LogicalType::Integer],
                names: vec!["v".to_string()],
                plan: wrap(&BindContext::new(), expression_get(99)),
            }),
            children: vec![Expression::Constant(ConstantExpression {
                value: Value::Integer(1),
                return_type: LogicalType::Integer,
            })],
            child_types: vec![LogicalType::Integer],
            child_targets: vec![LogicalType::Integer],
            comparison_type: ComparisonType::Equal,
            return_type: LogicalType::Integer,
            correlated_columns: vec![],
            bind_snapshot: BindContext::new().snapshot(),
            planning_state: SubqueryPlanningState::Unplanned,
        })
    }

    #[test]
    fn verify_rejects_remaining_dependent_join() {
        let ctx = BindContext::new();
        let plan = LogicalOperator::DependentJoin(DependentJoin::scalar(
            wrap(&ctx, expression_get(0)),
            wrap(&ctx, expression_get(1)),
            vec![],
        ));

        let err = verify_physical_planner_invariants(&plan).expect_err("verify should fail");
        assert!(err.to_string().contains("DependentJoin"));
    }

    #[test]
    fn verify_rejects_remaining_subquery_expression() {
        let ctx = BindContext::new();
        let child = wrap(&ctx, expression_get(0));
        let plan =
            LogicalOperator::Projection(Projection::new(42, child, vec![dummy_subquery_expr()]));

        let err = verify_physical_planner_invariants(&plan).expect_err("verify should fail");
        assert!(err.to_string().contains("Expression::Subquery"));
    }

    #[test]
    fn verify_rejects_subquery_in_window_frame_offset() {
        let ctx = BindContext::new();
        let child = wrap(&ctx, expression_get(0));
        let window = Expression::Window(WindowExpression {
            function: WindowFunction::row_number(),
            children: vec![],
            partitions: vec![],
            orders: vec![],
            frame: WindowFrame {
                frame_type: WindowFrameType::Rows,
                start_bound: WindowFrameBound::Offset(Box::new(dummy_subquery_expr())),
                start_is_preceding: true,
                end_bound: WindowFrameBound::CurrentRow,
                end_is_preceding: false,
            },
            ignore_nulls: false,
            return_type: LogicalType::BigInt,
        });
        let plan = LogicalOperator::Projection(Projection::new(42, child, vec![window]));

        let err = verify_physical_planner_invariants(&plan).expect_err("verify should fail");
        assert!(err.to_string().contains("Expression::Subquery"));
    }

    #[test]
    fn verify_accepts_flattened_projection_plan() {
        let ctx = BindContext::new();
        let child = wrap(&ctx, expression_get(0));
        let plan = LogicalOperator::Projection(Projection::new(
            42,
            child,
            vec![Expression::ColumnRef(ColumnRefExpression::new(
                ColumnBinding::new(0, 0),
                LogicalType::Integer,
            ))],
        ));

        verify_physical_planner_invariants(&plan).expect("verify should pass");
    }
}
