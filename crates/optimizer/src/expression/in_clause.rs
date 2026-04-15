use paro_common::types::LogicalType;
use paro_planner::expression::{
    ColumnRefExpression, ComparisonExpression, ComparisonType, ConjunctionExpression,
    ConjunctionType, Expression, OperatorExpression, OperatorType, WindowFrameBound,
};
use paro_planner::operator::ExpressionGet;
use paro_planner::operator::{
    ComparisonJoin, Filter, Join, JoinCondition, JoinType, LogicalOperator, Projection,
};
use paro_planner::plan::LogicalPlan;

use crate::context::OptimizationContext;

const MARK_JOIN_IN_THRESHOLD: usize = 6;

pub struct InClauseRewriter;

struct RootRewriteContext {
    child: Option<LogicalPlan>,
    filter_projection_map: Option<Vec<usize>>,
}

impl InClauseRewriter {
    pub fn new() -> Self {
        Self
    }

    pub fn rewrite(
        &mut self,
        plan: LogicalPlan,
        ctx: &mut OptimizationContext,
    ) -> paro_common::error::Result<LogicalPlan> {
        let plan = plan.try_map_children(|child| self.rewrite(child, ctx))?;
        Ok(self.rewrite_current(plan, ctx))
    }

    fn rewrite_current(&mut self, plan: LogicalPlan, ctx: &mut OptimizationContext) -> LogicalPlan {
        let LogicalPlan {
            id,
            stats,
            operator,
        } = plan;
        let operator = match operator {
            LogicalOperator::Filter(filter) => {
                LogicalOperator::Filter(self.rewrite_filter(filter, ctx))
            }
            LogicalOperator::Projection(projection) => {
                LogicalOperator::Projection(self.rewrite_projection(projection, ctx))
            }
            LogicalOperator::Aggregate(mut aggregate) => {
                aggregate.groups = aggregate
                    .groups
                    .into_iter()
                    .map(|expr| self.rewrite_expression_no_root(expr))
                    .collect();
                aggregate.aggregates = aggregate
                    .aggregates
                    .into_iter()
                    .map(|expr| self.rewrite_expression_no_root(expr))
                    .collect();
                aggregate.recompute_returned_types();
                LogicalOperator::Aggregate(aggregate)
            }
            LogicalOperator::Join(join) => LogicalOperator::Join(self.rewrite_join(join)),
            LogicalOperator::Order(mut order) => {
                for order_by in &mut order.orders {
                    order_by.expression =
                        self.rewrite_expression_no_root(order_by.expression.clone());
                }
                LogicalOperator::Order(order)
            }
            LogicalOperator::TopN(mut topn) => {
                for order_by in &mut topn.orders {
                    order_by.expression =
                        self.rewrite_expression_no_root(order_by.expression.clone());
                }
                LogicalOperator::TopN(topn)
            }
            LogicalOperator::Distinct(mut distinct) => {
                distinct.distinct_targets = distinct
                    .distinct_targets
                    .into_iter()
                    .map(|expr| self.rewrite_expression_no_root(expr))
                    .collect();
                if let Some(order_by) = &mut distinct.order_by {
                    for order in order_by {
                        order.expression =
                            self.rewrite_expression_no_root(order.expression.clone());
                    }
                }
                LogicalOperator::Distinct(distinct)
            }
            LogicalOperator::Window(mut window) => {
                for expr in &mut window.expressions {
                    expr.children = expr
                        .children
                        .drain(..)
                        .map(|child| self.rewrite_expression_no_root(child))
                        .collect();
                    expr.partitions = expr
                        .partitions
                        .drain(..)
                        .map(|child| self.rewrite_expression_no_root(child))
                        .collect();
                    for order in &mut expr.orders {
                        order.expression =
                            self.rewrite_expression_no_root(order.expression.clone());
                    }
                    rewrite_window_frame_bounds(expr, &mut |bound_expr| {
                        self.rewrite_expression_no_root(bound_expr)
                    });
                }
                LogicalOperator::Window(window)
            }
            LogicalOperator::Update(mut update) => {
                update.expressions = update
                    .expressions
                    .into_iter()
                    .map(|expr| self.rewrite_expression_no_root(expr))
                    .collect();
                LogicalOperator::Update(update)
            }
            LogicalOperator::ExpressionGet(mut expr_get) => {
                for row in &mut expr_get.expressions {
                    for expr in row {
                        *expr = self.rewrite_expression_no_root(expr.clone());
                    }
                }
                LogicalOperator::ExpressionGet(expr_get)
            }
            LogicalOperator::SearchScan(mut search) => {
                search.projections = search
                    .projections
                    .into_iter()
                    .map(|expr| self.rewrite_expression_no_root(expr))
                    .collect();
                search.absorbed_predicates = search
                    .absorbed_predicates
                    .into_iter()
                    .map(|expr| self.rewrite_expression_no_root(expr))
                    .collect();
                search.residual_predicates = search
                    .residual_predicates
                    .into_iter()
                    .map(|expr| self.rewrite_expression_no_root(expr))
                    .collect();
                search.score_expression = self.rewrite_expression_no_root(search.score_expression);
                LogicalOperator::SearchScan(search)
            }
            LogicalOperator::FullTextFilterScan(mut scan) => {
                scan.match_expression = self.rewrite_expression_no_root(scan.match_expression);
                scan.other_predicates = scan
                    .other_predicates
                    .into_iter()
                    .map(|expr| self.rewrite_expression_no_root(expr))
                    .collect();
                scan.residual_predicates = scan
                    .residual_predicates
                    .into_iter()
                    .map(|expr| self.rewrite_expression_no_root(expr))
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

    fn rewrite_filter(&mut self, filter: Filter, ctx: &mut OptimizationContext) -> Filter {
        let Filter {
            expressions,
            child,
            projection_map,
        } = filter;
        let mut root = RootRewriteContext {
            child: Some(*child),
            filter_projection_map: Some(projection_map),
        };
        let expressions = expressions
            .into_iter()
            .map(|expr| self.rewrite_expression_with_root(expr, &mut root, ctx))
            .collect();
        let (child, projection_map) = root.into_parts();
        Filter {
            expressions,
            child: Box::new(child),
            projection_map,
        }
    }

    fn rewrite_projection(
        &mut self,
        projection: Projection,
        ctx: &mut OptimizationContext,
    ) -> Projection {
        let Projection {
            table_index,
            expressions,
            output_names,
            child,
            ..
        } = projection;
        let mut root = RootRewriteContext {
            child: Some(*child),
            filter_projection_map: None,
        };
        let expressions: Vec<_> = expressions
            .into_iter()
            .map(|expr| self.rewrite_expression_with_root(expr, &mut root, ctx))
            .collect();
        let (child, _) = root.into_parts();
        let returned_types = expressions.iter().map(Expression::return_type).collect();
        Projection {
            table_index,
            expressions,
            output_names,
            child: Box::new(child),
            returned_types,
        }
    }

    fn rewrite_join(&mut self, join: Join) -> Join {
        match join {
            Join::Comparison(mut comparison) => {
                for condition in &mut comparison.conditions {
                    condition.left = self.rewrite_expression_no_root(condition.left.clone());
                    condition.right = self.rewrite_expression_no_root(condition.right.clone());
                }
                Join::Comparison(comparison)
            }
            Join::Any(mut any) => {
                any.condition = self.rewrite_expression_no_root(any.condition);
                Join::Any(any)
            }
            Join::Cross(cross) => Join::Cross(cross),
        }
    }

    fn rewrite_expression_with_root(
        &mut self,
        expr: Expression,
        root: &mut RootRewriteContext,
        ctx: &mut OptimizationContext,
    ) -> Expression {
        match expr {
            Expression::Function(mut function) => {
                function.children = function
                    .children
                    .into_iter()
                    .map(|child| self.rewrite_expression_with_root(child, root, ctx))
                    .collect();
                Expression::Function(function)
            }
            Expression::Cast(mut cast) => {
                cast.child = Box::new(self.rewrite_expression_with_root(*cast.child, root, ctx));
                Expression::Cast(cast)
            }
            Expression::Conjunction(mut conjunction) => {
                conjunction.children = conjunction
                    .children
                    .into_iter()
                    .map(|child| self.rewrite_expression_with_root(child, root, ctx))
                    .collect();
                Expression::Conjunction(conjunction)
            }
            Expression::Case(mut case) => {
                case.check = Box::new(self.rewrite_expression_with_root(*case.check, root, ctx));
                case.result_if_true =
                    Box::new(self.rewrite_expression_with_root(*case.result_if_true, root, ctx));
                case.result_if_false =
                    Box::new(self.rewrite_expression_with_root(*case.result_if_false, root, ctx));
                Expression::Case(case)
            }
            Expression::Comparison(mut comparison) => {
                comparison.left =
                    Box::new(self.rewrite_expression_with_root(*comparison.left, root, ctx));
                comparison.right =
                    Box::new(self.rewrite_expression_with_root(*comparison.right, root, ctx));
                Expression::Comparison(comparison)
            }
            Expression::Operator(mut operator) => {
                operator.children = operator
                    .children
                    .into_iter()
                    .map(|child| self.rewrite_expression_with_root(child, root, ctx))
                    .collect();
                self.rewrite_in_operator_with_root(operator, root, ctx)
            }
            Expression::Aggregate(mut aggregate) => {
                aggregate.children = aggregate
                    .children
                    .into_iter()
                    .map(|child| self.rewrite_expression_with_root(child, root, ctx))
                    .collect();
                aggregate.filter = aggregate
                    .filter
                    .map(|filter| Box::new(self.rewrite_expression_with_root(*filter, root, ctx)));
                for order in &mut aggregate.order_bys {
                    order.expression =
                        self.rewrite_expression_with_root(order.expression.clone(), root, ctx);
                }
                Expression::Aggregate(aggregate)
            }
            Expression::Window(mut window) => {
                window.children = window
                    .children
                    .into_iter()
                    .map(|child| self.rewrite_expression_with_root(child, root, ctx))
                    .collect();
                window.partitions = window
                    .partitions
                    .into_iter()
                    .map(|partition| self.rewrite_expression_with_root(partition, root, ctx))
                    .collect();
                for order in &mut window.orders {
                    order.expression =
                        self.rewrite_expression_with_root(order.expression.clone(), root, ctx);
                }
                rewrite_window_frame_bounds(&mut window, &mut |bound_expr| {
                    self.rewrite_expression_with_root(bound_expr, root, ctx)
                });
                Expression::Window(window)
            }
            Expression::Subquery(mut subquery) => {
                subquery.children = subquery
                    .children
                    .into_iter()
                    .map(|child| self.rewrite_expression_with_root(child, root, ctx))
                    .collect();
                Expression::Subquery(subquery)
            }
            leaf => leaf,
        }
    }

    fn rewrite_expression_no_root(&mut self, expr: Expression) -> Expression {
        match expr {
            Expression::Function(mut function) => {
                function.children = function
                    .children
                    .into_iter()
                    .map(|child| self.rewrite_expression_no_root(child))
                    .collect();
                Expression::Function(function)
            }
            Expression::Cast(mut cast) => {
                cast.child = Box::new(self.rewrite_expression_no_root(*cast.child));
                Expression::Cast(cast)
            }
            Expression::Conjunction(mut conjunction) => {
                conjunction.children = conjunction
                    .children
                    .into_iter()
                    .map(|child| self.rewrite_expression_no_root(child))
                    .collect();
                Expression::Conjunction(conjunction)
            }
            Expression::Case(mut case) => {
                case.check = Box::new(self.rewrite_expression_no_root(*case.check));
                case.result_if_true =
                    Box::new(self.rewrite_expression_no_root(*case.result_if_true));
                case.result_if_false =
                    Box::new(self.rewrite_expression_no_root(*case.result_if_false));
                Expression::Case(case)
            }
            Expression::Comparison(mut comparison) => {
                comparison.left = Box::new(self.rewrite_expression_no_root(*comparison.left));
                comparison.right = Box::new(self.rewrite_expression_no_root(*comparison.right));
                Expression::Comparison(comparison)
            }
            Expression::Operator(mut operator) => {
                operator.children = operator
                    .children
                    .into_iter()
                    .map(|child| self.rewrite_expression_no_root(child))
                    .collect();
                self.rewrite_in_operator_without_root(operator)
            }
            Expression::Aggregate(mut aggregate) => {
                aggregate.children = aggregate
                    .children
                    .into_iter()
                    .map(|child| self.rewrite_expression_no_root(child))
                    .collect();
                aggregate.filter = aggregate
                    .filter
                    .map(|filter| Box::new(self.rewrite_expression_no_root(*filter)));
                for order in &mut aggregate.order_bys {
                    order.expression = self.rewrite_expression_no_root(order.expression.clone());
                }
                Expression::Aggregate(aggregate)
            }
            Expression::Window(mut window) => {
                window.children = window
                    .children
                    .into_iter()
                    .map(|child| self.rewrite_expression_no_root(child))
                    .collect();
                window.partitions = window
                    .partitions
                    .into_iter()
                    .map(|partition| self.rewrite_expression_no_root(partition))
                    .collect();
                for order in &mut window.orders {
                    order.expression = self.rewrite_expression_no_root(order.expression.clone());
                }
                rewrite_window_frame_bounds(&mut window, &mut |bound_expr| {
                    self.rewrite_expression_no_root(bound_expr)
                });
                Expression::Window(window)
            }
            Expression::Subquery(mut subquery) => {
                subquery.children = subquery
                    .children
                    .into_iter()
                    .map(|child| self.rewrite_expression_no_root(child))
                    .collect();
                Expression::Subquery(subquery)
            }
            leaf => leaf,
        }
    }

    fn rewrite_in_operator_with_root(
        &mut self,
        operator: OperatorExpression,
        root: &mut RootRewriteContext,
        ctx: &mut OptimizationContext,
    ) -> Expression {
        if !matches!(
            operator.operator_type,
            OperatorType::In | OperatorType::NotIn
        ) {
            return Expression::Operator(operator);
        }
        rewrite_in_operator(operator, Some(root), ctx)
    }

    fn rewrite_in_operator_without_root(&mut self, operator: OperatorExpression) -> Expression {
        if !matches!(
            operator.operator_type,
            OperatorType::In | OperatorType::NotIn
        ) {
            return Expression::Operator(operator);
        }

        let negate = matches!(operator.operator_type, OperatorType::NotIn);
        build_in_fallback(operator.children, negate)
    }
}

impl Default for InClauseRewriter {
    fn default() -> Self {
        Self::new()
    }
}

impl RootRewriteContext {
    fn child(&self) -> &LogicalPlan {
        self.child.as_ref().expect("root child must exist")
    }

    fn into_parts(self) -> (LogicalPlan, Vec<usize>) {
        (
            self.child.expect("root child must exist"),
            self.filter_projection_map.unwrap_or_default(),
        )
    }

    fn introduce_mark_join(
        &mut self,
        lhs: Expression,
        rhs_constants: Vec<Expression>,
        negate: bool,
        ctx: &mut OptimizationContext,
    ) -> Expression {
        let old_output_len = self.child().types().len();
        let input_type = lhs.return_type();
        let rhs_table_index = ctx.bind_context.generate_table_index();
        let mark_index = ctx.bind_context.generate_table_index();
        let rhs_values = rhs_constants.into_iter().map(|value| vec![value]).collect();
        let rhs = LogicalPlan::new(
            &ctx.bind_context,
            LogicalOperator::ExpressionGet(ExpressionGet::new(
                rhs_table_index,
                rhs_values,
                vec!["in_value".to_string()],
                vec![input_type.clone()],
            )),
        );

        let left = self.child.take().expect("root child must exist");
        let mut join = ComparisonJoin::new(
            JoinType::Mark,
            left,
            rhs,
            vec![JoinCondition::new(
                lhs,
                Expression::ColumnRef(ColumnRefExpression::new(
                    paro_planner::operator::ColumnBinding::new(rhs_table_index, 0),
                    input_type,
                )),
                paro_planner::operator::JoinComparisonType::Equal,
            )],
        );
        join.mark_index = Some(mark_index);
        self.child = Some(LogicalPlan::new(
            &ctx.bind_context,
            LogicalOperator::Join(Join::Comparison(join)),
        ));

        if let Some(projection_map) = self.filter_projection_map.as_mut() {
            if projection_map.is_empty() {
                projection_map.extend(0..old_output_len);
            }
        }

        let mark_ref = Expression::ColumnRef(ColumnRefExpression::new(
            paro_planner::operator::ColumnBinding::new(mark_index, 0),
            LogicalType::Boolean,
        ));
        if negate {
            Expression::Operator(OperatorExpression::new_unary(
                OperatorType::Not,
                mark_ref,
                LogicalType::Boolean,
            ))
        } else {
            mark_ref
        }
    }
}

fn rewrite_in_operator(
    operator: OperatorExpression,
    root: Option<&mut RootRewriteContext>,
    ctx: &mut OptimizationContext,
) -> Expression {
    let negate = matches!(operator.operator_type, OperatorType::NotIn);
    if operator.children.len() < 2 {
        return Expression::Operator(operator);
    }

    if operator.children.len() == 2 {
        return build_single_item_in(operator.children, negate);
    }

    let mut children = operator.children;
    let lhs = children.remove(0);
    if children.len() + 1 >= MARK_JOIN_IN_THRESHOLD
        && children
            .iter()
            .all(|child| matches!(child, Expression::Constant(_)))
    {
        if let Some(root) = root {
            return root.introduce_mark_join(lhs, children, negate, ctx);
        }
    }

    let mut all_children = Vec::with_capacity(children.len() + 1);
    all_children.push(lhs);
    all_children.extend(children);
    build_in_fallback(all_children, negate)
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

fn build_in_fallback(children: Vec<Expression>, negate: bool) -> Expression {
    if children.len() < 2 {
        return Expression::Operator(OperatorExpression::new(
            if negate {
                OperatorType::NotIn
            } else {
                OperatorType::In
            },
            children,
            LogicalType::Boolean,
        ));
    }

    let lhs = children[0].clone();
    let comparison_type = if negate {
        ComparisonType::NotEqual
    } else {
        ComparisonType::Equal
    };
    let conjunction_type = if negate {
        ConjunctionType::And
    } else {
        ConjunctionType::Or
    };
    let comparisons: Vec<_> = children
        .into_iter()
        .skip(1)
        .map(|rhs| {
            Expression::Comparison(ComparisonExpression::new(comparison_type, lhs.clone(), rhs))
        })
        .collect();
    if comparisons.len() == 1 {
        comparisons.into_iter().next().expect("single comparison")
    } else {
        Expression::Conjunction(ConjunctionExpression::new(conjunction_type, comparisons))
    }
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
    use std::sync::Arc;

    use paro_common::runtime_value::Value;
    use paro_context::{test_support::TestStatementContextBuilder, StatementContext};
    use paro_planner::binder::context::BindContext;
    use paro_planner::operator::{ColumnBinding, JoinComparisonType};

    use super::*;
    use crate::context::OptimizationContext;

    fn make_test_session() -> Arc<StatementContext> {
        TestStatementContextBuilder::minimal().build()
    }

    fn make_ctx() -> (BindContext, OptimizationContext) {
        let bind_context = BindContext::new();
        let session = make_test_session();
        let ctx = OptimizationContext::new(session, bind_context.clone());
        (bind_context, ctx)
    }

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
        let (bind_context, mut ctx) = make_ctx();
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
            .rewrite(plan, &mut ctx)
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
    fn rewrites_large_constant_in_filter_to_mark_join() {
        let (bind_context, mut ctx) = make_ctx();
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
            .rewrite(plan, &mut ctx)
            .expect("rewrite succeeds");

        let LogicalOperator::Filter(filter) = rewritten.operator else {
            panic!("expected filter");
        };
        assert_eq!(filter.projection_map, vec![0]);
        let Expression::ColumnRef(mark_ref) = &filter.expressions[0] else {
            panic!("expected mark column reference");
        };
        let LogicalOperator::Join(Join::Comparison(join)) = &filter.child.operator else {
            panic!("expected mark join child");
        };
        assert_eq!(join.join_type, JoinType::Mark);
        assert_eq!(join.mark_index, Some(mark_ref.binding.table_index));
        assert_eq!(join.conditions.len(), 1);
        assert_eq!(join.conditions[0].comparison, JoinComparisonType::Equal);

        let LogicalOperator::ExpressionGet(expr_get) = &join.right.operator else {
            panic!("expected constant rhs");
        };
        assert_eq!(expr_get.expressions.len(), 5);
    }

    #[test]
    fn rewrites_large_in_outside_filter_projection_to_disjunction() {
        let (bind_context, mut ctx) = make_ctx();
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
            .rewrite(plan, &mut ctx)
            .expect("rewrite succeeds");

        let LogicalOperator::Join(Join::Any(join)) = rewritten.operator else {
            panic!("expected any join");
        };
        let Expression::Conjunction(conjunction) = &join.condition else {
            panic!("expected OR fallback");
        };
        assert_eq!(conjunction.conjunction_type, ConjunctionType::Or);
        assert_eq!(conjunction.children.len(), 5);
    }
}
