// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Planner verification helpers.
//!
//! These checks enforce the contract between planner/subquery-flattening and
//! the physical planner/expression executor.

use paro_common::error::{self as paro_error, Result};

use crate::expression::{Expression, ExpressionIterator};
use crate::operator::{Join, LogicalOperator, LogicalOutputLayout};

/// Verify that a logical plan is ready for physical planning/execution.
///
/// The planner must fully flatten dependent joins and remove subquery
/// expressions before this point.
pub fn verify_physical_planner_invariants(plan: &LogicalOperator) -> Result<()> {
    plan.try_fold_ref_post_order(|operator, child_layouts| {
        verify_operator(operator, child_layouts)?;
        Ok(operator.output_layout_from_children(child_layouts))
    })
    .map(|_| ())
}

fn verify_operator(op: &LogicalOperator, child_layouts: &[LogicalOutputLayout]) -> Result<()> {
    match op {
        LogicalOperator::DependentJoin(_) => {
            return Err(paro_error::internal(
                "Planner verify failed: DependentJoin remained after flattening".to_string(),
            ));
        }
        LogicalOperator::Filter(filter) => {
            filter.projection_map.validate(child_layouts[0].len())?;
            for expr in &filter.expressions {
                verify_expression(expr)?;
            }
        }
        LogicalOperator::Projection(proj) => {
            for expr in &proj.expressions {
                verify_expression(expr)?;
            }
        }
        LogicalOperator::RowFetch(fetch) => {
            for source in &fetch.sources {
                verify_expression(&source.rowid)?;
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
            order.projection_map.validate(child_layouts[0].len())?;
            for order in &order.orders {
                verify_expression(&order.expression)?;
            }
        }
        LogicalOperator::TopN(topn) => {
            topn.projection_map.validate(child_layouts[0].len())?;
            for order in &topn.orders {
                verify_expression(&order.expression)?;
            }
        }
        LogicalOperator::Aggregate(agg) => {
            agg.verify_post_reduction()?;
            for expr in &agg.groups {
                verify_expression(expr)?;
            }
            for expr in &agg.aggregates {
                verify_expression(expr)?;
            }
            if let Some(reduction) = &agg.post_reduction {
                for reducer in &reduction.reducers {
                    verify_expression(reducer)?;
                }
                for scalar in &reduction.scalar_expressions {
                    verify_expression(scalar)?;
                }
                verify_expression(&reduction.predicate)?;
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
                comp_join
                    .left_projection_map
                    .validate(child_layouts[0].len())?;
                comp_join
                    .right_projection_map
                    .validate(child_layouts[1].len())?;
                for expr in &comp_join.duplicate_eliminated_columns {
                    verify_expression(expr)?;
                }
                for cond in &comp_join.conditions {
                    verify_expression(&cond.left)?;
                    verify_expression(&cond.right)?;
                    if !crate::expression::ComparisonExpression::operands_have_bound_input_contract(
                        &cond.left,
                        &cond.right,
                    ) {
                        return Err(paro_error::internal(format!(
                            "Planner verify failed: join comparison operands are not explicitly coerced to one normalized type (left={}, right={})",
                            cond.left.return_type(),
                            cond.right.return_type(),
                        )));
                    }
                }
            }
            Join::Any(any_join) => {
                any_join
                    .left_projection_map
                    .validate(child_layouts[0].len())?;
                any_join
                    .right_projection_map
                    .validate(child_layouts[1].len())?;
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
                expression.verify_bound_contract()?;
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
            scan.projection_map
                .validate(scan.get.returned_types.len())?;
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

    Ok(())
}

fn verify_expression(expr: &Expression) -> Result<()> {
    verify_expression_node(expr, expr)
}

fn verify_expression_node(expr: &Expression, root: &Expression) -> Result<()> {
    if let Expression::Window(window) = expr {
        window.verify_bound_contract()?;
    }
    if let Expression::Comparison(comparison) = expr {
        verify_comparison_input_contract(
            comparison.left.as_ref(),
            comparison.right.as_ref(),
            root,
        )?;
    }
    if let Expression::Subquery(subquery) = expr {
        return Err(paro_error::internal(format!(
            "Planner verify failed: Expression::Subquery remained after flattening (state={:?}) in {root:?}",
            subquery.planning_state,
        )));
    }
    if let Expression::ColumnRef(column) = expr {
        if column.depth != 0 {
            return Err(paro_error::internal(format!(
                "Planner verify failed: correlated column {:?} remained at depth {} after flattening in {root:?}",
                column.binding, column.depth,
            )));
        }
    }

    let mut result = Ok(());
    ExpressionIterator::enumerate_children(expr, |child| {
        if result.is_ok() {
            result = verify_expression_node(child, root);
        }
    });
    result
}

fn verify_comparison_input_contract(
    left: &Expression,
    right: &Expression,
    root: &Expression,
) -> Result<()> {
    if crate::expression::ComparisonExpression::operands_have_bound_input_contract(left, right) {
        return Ok(());
    }
    Err(paro_error::internal(format!(
        "Planner verify failed: comparison operands are not explicitly coerced to one normalized type (left={}, right={}) in {root:?}",
        left.return_type(),
        right.return_type(),
    )))
}

#[cfg(test)]
mod tests {
    use super::verify_physical_planner_invariants;
    use std::sync::Arc;

    use crate::binder::context::BindContext;
    use crate::expression::{
        AggregateExpression, ColumnRefExpression, ComparisonExpression, ComparisonType,
        ConstantExpression, Expression, SubqueryExpression, SubqueryPlanningState, SubqueryType,
        WindowExpression, WindowFrame, WindowFrameBound, WindowFrameType,
    };
    use crate::operator::projection::Projection;
    use crate::operator::{
        ColumnBinding, DependentJoin, ExpressionGet, Filter, LogicalOperator, TopN,
    };
    use crate::plan::OwnedLogicalPlan;
    use crate::plan::PlannedStatement;
    use paro_common::runtime_value::Value;
    use paro_common::types::LogicalType;
    use paro_function::aggregate::distributive::count::get_count_star_function;
    use paro_function::window::WindowFunction;

    fn expression_get(table_index: usize) -> LogicalOperator {
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            table_index,
            vec![],
            vec!["v".to_string()],
            vec![LogicalType::Integer],
        ))
    }

    fn wrap(ctx: &BindContext, op: LogicalOperator) -> OwnedLogicalPlan {
        OwnedLogicalPlan::new(ctx, op)
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
            None,
        ));

        let err = verify_physical_planner_invariants(&plan).expect_err("verify should fail");
        assert!(err.to_string().contains("DependentJoin"));
    }

    #[test]
    fn verify_rejects_an_invalid_topn_projection() {
        let ctx = BindContext::new();
        let mut topn = TopN::new(wrap(&ctx, expression_get(0)), vec![], 1, 0);
        topn.projection_map = vec![1].into();

        let error = verify_physical_planner_invariants(&LogicalOperator::TopN(topn))
            .expect_err("projection outside the child must fail verification");
        assert!(error.to_string().contains("outside child layout width 1"));
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
        let window = Expression::Window(WindowExpression::native(
            WindowFunction::row_number(),
            vec![],
            vec![],
            vec![],
            WindowFrame {
                frame_type: WindowFrameType::Rows,
                start_bound: WindowFrameBound::Offset(Box::new(dummy_subquery_expr())),
                start_is_preceding: true,
                end_bound: WindowFrameBound::CurrentRow,
                end_is_preceding: false,
            },
            false,
        ));
        let plan = LogicalOperator::Projection(Projection::new(42, child, vec![window]));

        let err = verify_physical_planner_invariants(&plan).expect_err("verify should fail");
        assert!(err.to_string().contains("Expression::Subquery"));
    }

    #[test]
    fn verify_rejects_aggregate_window_kernel_type_drift() {
        let ctx = BindContext::new();
        let child = wrap(&ctx, expression_get(0));
        let aggregate =
            AggregateExpression::new(get_count_star_function(), vec![], LogicalType::Integer);
        let window = Expression::Window(WindowExpression::aggregate(
            aggregate,
            vec![],
            vec![],
            WindowFrame::default(),
        ));
        let plan = LogicalOperator::Projection(Projection::new(42, child, vec![window]));

        let err = verify_physical_planner_invariants(&plan).expect_err("verify should fail");
        assert!(err.to_string().contains("return type mismatch"), "{err}");
    }

    #[test]
    fn verify_rejects_comparison_without_explicit_common_type() {
        let ctx = BindContext::new();
        let child = wrap(&ctx, expression_get(0));
        let comparison = Expression::Comparison(ComparisonExpression {
            left: Box::new(Expression::Constant(ConstantExpression::new(
                Value::Integer(1),
                LogicalType::Integer,
            ))),
            right: Box::new(Expression::Constant(ConstantExpression::new(
                Value::BigInt(1),
                LogicalType::BigInt,
            ))),
            comparison_type: ComparisonType::Equal,
        });
        let plan = LogicalOperator::Projection(Projection::new(42, child, vec![comparison]));

        let err = verify_physical_planner_invariants(&plan).expect_err("verify should fail");
        assert!(err.to_string().contains("explicitly coerced"), "{err}");
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

    #[test]
    fn verify_reuses_child_layouts_on_a_deep_filter_chain() {
        const DEPTH: usize = 10_000;
        const TEST_STACK_BYTES: usize = 512 * 1024;

        std::thread::Builder::new()
            .name("deep-plan-verifier".to_string())
            .stack_size(TEST_STACK_BYTES)
            .spawn(|| {
                let ctx = BindContext::new();
                let mut plan = wrap(&ctx, expression_get(0));
                for _ in 0..DEPTH {
                    plan = wrap(&ctx, LogicalOperator::Filter(Filter::new(plan, Vec::new())));
                }

                verify_physical_planner_invariants(&plan.operator)
                    .expect("deep valid plan should verify with a bounded native stack");
            })
            .expect("verifier thread should start")
            .join()
            .expect("verifier thread should not overflow its stack");
    }
}
