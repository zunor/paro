// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_planner::expression::{
    ComparisonExpression, ComparisonType, Expression, OperatorExpression, OperatorType,
    WindowFrameBound,
};
use paro_planner::operator::{Filter, Join, LogicalOperator, Projection};
use paro_planner::plan::LogicalPlan;

pub struct InClauseRewriter;

impl InClauseRewriter {
    pub fn new() -> Self {
        Self
    }

    pub fn rewrite(&mut self, plan: LogicalPlan) -> paro_common::error::Result<LogicalPlan> {
        let plan = plan.try_map_children(|child| self.rewrite(child))?;
        Ok(self.rewrite_current(plan))
    }

    fn rewrite_current(&mut self, plan: LogicalPlan) -> LogicalPlan {
        let LogicalPlan {
            id,
            stats,
            operator,
        } = plan;
        let operator = match operator {
            LogicalOperator::Filter(filter) => LogicalOperator::Filter(self.rewrite_filter(filter)),
            LogicalOperator::Projection(projection) => {
                LogicalOperator::Projection(self.rewrite_projection(projection))
            }
            LogicalOperator::Aggregate(mut aggregate) => {
                aggregate.groups = aggregate
                    .groups
                    .into_iter()
                    .map(|expr| self.rewrite_expression(expr))
                    .collect();
                aggregate.aggregates = aggregate
                    .aggregates
                    .into_iter()
                    .map(|expr| self.rewrite_expression(expr))
                    .collect();
                aggregate.recompute_returned_types();
                LogicalOperator::Aggregate(aggregate)
            }
            LogicalOperator::Join(join) => LogicalOperator::Join(self.rewrite_join(join)),
            LogicalOperator::Order(mut order) => {
                for order_by in &mut order.orders {
                    order_by.expression = self.rewrite_expression(order_by.expression.clone());
                }
                LogicalOperator::Order(order)
            }
            LogicalOperator::TopN(mut topn) => {
                for order_by in &mut topn.orders {
                    order_by.expression = self.rewrite_expression(order_by.expression.clone());
                }
                LogicalOperator::TopN(topn)
            }
            LogicalOperator::Distinct(mut distinct) => {
                distinct.distinct_targets = distinct
                    .distinct_targets
                    .into_iter()
                    .map(|expr| self.rewrite_expression(expr))
                    .collect();
                if let Some(order_by) = &mut distinct.order_by {
                    for order in order_by {
                        order.expression = self.rewrite_expression(order.expression.clone());
                    }
                }
                LogicalOperator::Distinct(distinct)
            }
            LogicalOperator::Window(mut window) => {
                for expr in &mut window.expressions {
                    expr.children = expr
                        .children
                        .drain(..)
                        .map(|child| self.rewrite_expression(child))
                        .collect();
                    expr.partitions = expr
                        .partitions
                        .drain(..)
                        .map(|child| self.rewrite_expression(child))
                        .collect();
                    for order in &mut expr.orders {
                        order.expression = self.rewrite_expression(order.expression.clone());
                    }
                    rewrite_window_frame_bounds(expr, &mut |bound_expr| {
                        self.rewrite_expression(bound_expr)
                    });
                }
                LogicalOperator::Window(window)
            }
            LogicalOperator::Update(mut update) => {
                update.expressions = update
                    .expressions
                    .into_iter()
                    .map(|expr| self.rewrite_expression(expr))
                    .collect();
                LogicalOperator::Update(update)
            }
            LogicalOperator::ExpressionGet(mut expr_get) => {
                for row in &mut expr_get.expressions {
                    for expr in row {
                        *expr = self.rewrite_expression(expr.clone());
                    }
                }
                LogicalOperator::ExpressionGet(expr_get)
            }
            LogicalOperator::SearchScan(mut search) => {
                search.projections = search
                    .projections
                    .into_iter()
                    .map(|expr| self.rewrite_expression(expr))
                    .collect();
                search.absorbed_predicates = search
                    .absorbed_predicates
                    .into_iter()
                    .map(|expr| self.rewrite_expression(expr))
                    .collect();
                search.residual_predicates = search
                    .residual_predicates
                    .into_iter()
                    .map(|expr| self.rewrite_expression(expr))
                    .collect();
                search.score_expression = self.rewrite_expression(search.score_expression);
                LogicalOperator::SearchScan(search)
            }
            LogicalOperator::FullTextFilterScan(mut scan) => {
                scan.match_expression = self.rewrite_expression(scan.match_expression);
                scan.other_predicates = scan
                    .other_predicates
                    .into_iter()
                    .map(|expr| self.rewrite_expression(expr))
                    .collect();
                scan.residual_predicates = scan
                    .residual_predicates
                    .into_iter()
                    .map(|expr| self.rewrite_expression(expr))
                    .collect();
                LogicalOperator::FullTextFilterScan(scan)
            }
            other => other,
        };
        LogicalPlan {
            id,
            stats,
            operator,
        }
    }

    fn rewrite_filter(&mut self, mut filter: Filter) -> Filter {
        filter.expressions = filter
            .expressions
            .into_iter()
            .map(|expr| self.rewrite_expression(expr))
            .collect();
        filter
    }

    fn rewrite_projection(&mut self, mut projection: Projection) -> Projection {
        projection.expressions = projection
            .expressions
            .into_iter()
            .map(|expr| self.rewrite_expression(expr))
            .collect();
        projection.returned_types = projection
            .expressions
            .iter()
            .map(Expression::return_type)
            .collect();
        projection
    }

    fn rewrite_join(&mut self, join: Join) -> Join {
        match join {
            Join::Comparison(mut comparison) => {
                for condition in &mut comparison.conditions {
                    condition.left = self.rewrite_expression(condition.left.clone());
                    condition.right = self.rewrite_expression(condition.right.clone());
                }
                Join::Comparison(comparison)
            }
            Join::Any(mut any) => {
                any.condition = self.rewrite_expression(any.condition);
                Join::Any(any)
            }
            Join::Cross(cross) => Join::Cross(cross),
        }
    }

    fn rewrite_expression(&mut self, expr: Expression) -> Expression {
        match expr {
            Expression::Function(mut function) => {
                function.children = function
                    .children
                    .into_iter()
                    .map(|child| self.rewrite_expression(child))
                    .collect();
                Expression::Function(function)
            }
            Expression::Cast(mut cast) => {
                cast.child = Box::new(self.rewrite_expression(*cast.child));
                Expression::Cast(cast)
            }
            Expression::Conjunction(mut conjunction) => {
                conjunction.children = conjunction
                    .children
                    .into_iter()
                    .map(|child| self.rewrite_expression(child))
                    .collect();
                Expression::Conjunction(conjunction)
            }
            Expression::Case(mut case) => {
                case.check = Box::new(self.rewrite_expression(*case.check));
                case.result_if_true = Box::new(self.rewrite_expression(*case.result_if_true));
                case.result_if_false = Box::new(self.rewrite_expression(*case.result_if_false));
                Expression::Case(case)
            }
            Expression::Comparison(mut comparison) => {
                comparison.left = Box::new(self.rewrite_expression(*comparison.left));
                comparison.right = Box::new(self.rewrite_expression(*comparison.right));
                Expression::Comparison(comparison)
            }
            Expression::Operator(mut operator) => {
                operator.children = operator
                    .children
                    .into_iter()
                    .map(|child| self.rewrite_expression(child))
                    .collect();
                rewrite_in_operator(operator)
            }
            Expression::Aggregate(mut aggregate) => {
                aggregate.children = aggregate
                    .children
                    .into_iter()
                    .map(|child| self.rewrite_expression(child))
                    .collect();
                aggregate.filter = aggregate
                    .filter
                    .map(|filter| Box::new(self.rewrite_expression(*filter)));
                for order in &mut aggregate.order_bys {
                    order.expression = self.rewrite_expression(order.expression.clone());
                }
                Expression::Aggregate(aggregate)
            }
            Expression::Window(mut window) => {
                window.children = window
                    .children
                    .into_iter()
                    .map(|child| self.rewrite_expression(child))
                    .collect();
                window.partitions = window
                    .partitions
                    .into_iter()
                    .map(|partition| self.rewrite_expression(partition))
                    .collect();
                for order in &mut window.orders {
                    order.expression = self.rewrite_expression(order.expression.clone());
                }
                rewrite_window_frame_bounds(&mut window, &mut |bound_expr| {
                    self.rewrite_expression(bound_expr)
                });
                Expression::Window(window)
            }
            Expression::Subquery(mut subquery) => {
                subquery.children = subquery
                    .children
                    .into_iter()
                    .map(|child| self.rewrite_expression(child))
                    .collect();
                Expression::Subquery(subquery)
            }
            leaf => leaf,
        }
    }
}

impl Default for InClauseRewriter {
    fn default() -> Self {
        Self::new()
    }
}

fn rewrite_in_operator(operator: OperatorExpression) -> Expression {
    if !matches!(
        operator.operator_type,
        OperatorType::In | OperatorType::NotIn
    ) || operator.children.len() != 2
    {
        return Expression::Operator(operator);
    }
    let negate = matches!(operator.operator_type, OperatorType::NotIn);
    build_single_item_in(operator.children, negate)
}

fn build_single_item_in(mut children: Vec<Expression>, negate: bool) -> Expression {
    let comparison_type = if negate {
        ComparisonType::NotEqual
    } else {
        ComparisonType::Equal
    };
    let rhs = children.pop().expect("single-item IN rhs");
    let lhs = children.pop().expect("single-item IN lhs");
    Expression::Comparison(ComparisonExpression::new(comparison_type, lhs, rhs))
}

fn rewrite_window_frame_bounds(
    window: &mut paro_planner::expression::WindowExpression,
    f: &mut impl FnMut(Expression) -> Expression,
) {
    if let WindowFrameBound::Offset(offset) = &mut window.frame.start_bound {
        *offset = Box::new(f((**offset).clone()));
    }
    if let WindowFrameBound::Offset(offset) = &mut window.frame.end_bound {
        *offset = Box::new(f((**offset).clone()));
    }
}

#[cfg(test)]
mod tests {
    use paro_common::runtime_value::Value;
    use paro_common::types::LogicalType;
    use paro_planner::binder::context::BindContext;
    use paro_planner::expression::ColumnRefExpression;
    use paro_planner::operator::{ColumnBinding, ExpressionGet, JoinType};

    use super::*;
    fn integer_get(bind_context: &BindContext, table_index: usize) -> LogicalPlan {
        LogicalPlan::new(
            bind_context,
            LogicalOperator::ExpressionGet(ExpressionGet::new(
                table_index,
                vec![],
                vec!["v".to_string()],
                vec![LogicalType::Integer],
            )),
        )
    }

    fn integer_column(table_index: usize, column_index: usize) -> Expression {
        Expression::ColumnRef(ColumnRefExpression::new(
            ColumnBinding::new(table_index, column_index),
            LogicalType::Integer,
        ))
    }

    fn int_constant(value: i32) -> Expression {
        Expression::Constant(paro_planner::expression::ConstantExpression::new(
            Value::Integer(value),
            LogicalType::Integer,
        ))
    }

    #[test]
    fn rewrites_single_item_in_to_comparison() {
        let bind_context = BindContext::new();
        let child = integer_get(&bind_context, 0);
        let expr = Expression::Operator(OperatorExpression::new(
            OperatorType::In,
            vec![integer_column(0, 0), int_constant(1)],
            LogicalType::Boolean,
        ));
        let plan = LogicalPlan::new(
            &bind_context,
            LogicalOperator::Filter(Filter::new(child, vec![expr])),
        );

        let rewritten = InClauseRewriter::new()
            .rewrite(plan)
            .expect("rewrite succeeds");

        let LogicalOperator::Filter(filter) = rewritten.operator else {
            panic!("expected filter");
        };
        assert!(matches!(
            &filter.expressions[0],
            Expression::Comparison(comparison)
                if matches!(comparison.comparison_type, ComparisonType::Equal)
        ));
    }

    #[test]
    fn preserves_large_constant_in_filter_for_execution_and_pushdown() {
        let bind_context = BindContext::new();
        let child = integer_get(&bind_context, 0);
        let expr = Expression::Operator(OperatorExpression::new(
            OperatorType::In,
            vec![
                integer_column(0, 0),
                int_constant(1),
                int_constant(2),
                int_constant(3),
                int_constant(4),
                int_constant(5),
            ],
            LogicalType::Boolean,
        ));
        let plan = LogicalPlan::new(
            &bind_context,
            LogicalOperator::Filter(Filter::new(child, vec![expr])),
        );

        let rewritten = InClauseRewriter::new()
            .rewrite(plan)
            .expect("rewrite succeeds");

        let LogicalOperator::Filter(filter) = rewritten.operator else {
            panic!("expected filter");
        };
        assert!(filter.projection_map.is_identity(1));
        let Expression::Operator(operator) = &filter.expressions[0] else {
            panic!("expected preserved IN operator");
        };
        assert_eq!(operator.operator_type, OperatorType::In);
        assert_eq!(operator.children.len(), 6);
        assert!(matches!(
            filter.child.operator,
            LogicalOperator::ExpressionGet(_)
        ));
    }

    #[test]
    fn preserves_large_in_outside_filter() {
        let bind_context = BindContext::new();
        let left = integer_get(&bind_context, 0);
        let right = integer_get(&bind_context, 1);
        let condition = Expression::Operator(OperatorExpression::new(
            OperatorType::In,
            vec![
                integer_column(0, 0),
                int_constant(1),
                int_constant(2),
                int_constant(3),
                int_constant(4),
                int_constant(5),
            ],
            LogicalType::Boolean,
        ));
        let join = Join::any(JoinType::Inner, left, right, condition);
        let plan = LogicalPlan::new(&bind_context, LogicalOperator::Join(join));

        let rewritten = InClauseRewriter::new()
            .rewrite(plan)
            .expect("rewrite succeeds");

        let LogicalOperator::Join(Join::Any(join)) = rewritten.operator else {
            panic!("expected any join");
        };
        let Expression::Operator(operator) = &join.condition else {
            panic!("expected preserved IN operator");
        };
        assert_eq!(operator.operator_type, OperatorType::In);
        assert_eq!(operator.children.len(), 6);
    }
}
