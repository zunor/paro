// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_common::error::Result;
use paro_planner::expression::Expression;
use paro_planner::operator::LogicalOperator;
use paro_planner::plan::OwnedLogicalPlan;

use crate::context::OptimizationContext;

pub struct ReorderFilter;

impl ReorderFilter {
    pub fn new() -> Self {
        Self
    }

    pub fn rewrite(&mut self, plan: OwnedLogicalPlan, ctx: &OptimizationContext) -> Result<OwnedLogicalPlan> {
        self.rewrite_with_change(plan, ctx).map(|(plan, _)| plan)
    }

    pub fn rewrite_with_change(
        &mut self,
        plan: OwnedLogicalPlan,
        ctx: &OptimizationContext,
    ) -> Result<(OwnedLogicalPlan, bool)> {
        let mut changed = false;
        let plan = plan.try_map_children(|child| {
            let (child, child_changed) = self.rewrite_with_change(child, ctx)?;
            changed |= child_changed;
            Ok(child)
        })?;
        let (plan, node_changed) = self.reorder_node(plan, ctx);
        Ok((plan, changed || node_changed))
    }

    /// Reorder predicates owned by this node without rewriting descendants.
    ///
    /// Memo exploration invokes transformations per equivalence group, so a
    /// rule applied to one Filter must not manufacture the Cartesian product
    /// of every independently reorderable descendant Filter.
    pub(crate) fn reorder_node(
        &mut self,
        plan: OwnedLogicalPlan,
        ctx: &OptimizationContext,
    ) -> (OwnedLogicalPlan, bool) {
        let (id, stats, operator) = plan.into_parts();
        let (operator, changed) = match operator {
            LogicalOperator::Filter(mut filter) => {
                let original = filter.expressions;
                let reordered = reorder_with_evaluation_fences(original.clone(), ctx);
                let changed = original
                    .iter()
                    .zip(&reordered)
                    .any(|(left, right)| !left.equals(right));
                filter.expressions = reordered;
                (LogicalOperator::Filter(filter), changed)
            }
            other => (other, false),
        };
        (
            OwnedLogicalPlan {
                id,
                stats,
                operator,
            },
            changed,
        )
    }
}

fn reorder_with_evaluation_fences(
    expressions: Vec<Expression>,
    ctx: &OptimizationContext,
) -> Vec<Expression> {
    let mut reordered = Vec::with_capacity(expressions.len());
    let mut native_segment = Vec::new();

    for expr in expressions {
        if expr.evaluation_properties().is_reorder_fence() {
            sort_filter_segment(&mut native_segment, ctx);
            reordered.append(&mut native_segment);
            reordered.push(expr);
        } else {
            native_segment.push(expr);
        }
    }

    sort_filter_segment(&mut native_segment, ctx);
    reordered.append(&mut native_segment);
    reordered
}

fn sort_filter_segment(expressions: &mut [Expression], ctx: &OptimizationContext) {
    expressions.sort_by(|left, right| {
        let left_sel = ctx.cost_model.estimate_selectivity(left, &ctx.column_stats);
        let right_sel = ctx
            .cost_model
            .estimate_selectivity(right, &ctx.column_stats);
        left_sel.total_cmp(&right_sel)
    });
}

impl Default for ReorderFilter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use paro_common::runtime_value::Value;
    use paro_common::types::LogicalType;
    use paro_context::{test_support::TestStatementContextBuilder, StatementContext};
    use paro_planner::binder::context::BindContext;
    use paro_planner::expression::{
        ColumnRefExpression, ComparisonExpression, ComparisonType, ConstantExpression, Expression,
        FunctionExpression,
    };
    use paro_planner::operator::{ColumnBinding, ExpressionGet, Filter, LogicalOperator};

    use super::*;
    use crate::context::{EmptyGraphStatsLoader, GraphStatsCache, OptimizationContext};
    use crate::cost_model::CostModel;
    use crate::profiler::OptimizerProfiler;

    fn make_test_session() -> Arc<StatementContext> {
        TestStatementContextBuilder::minimal().build()
    }

    fn integer_get(bind_context: &BindContext, table_index: usize) -> OwnedLogicalPlan {
        OwnedLogicalPlan::new(
            bind_context,
            LogicalOperator::ExpressionGet(ExpressionGet::new(
                table_index,
                vec![],
                vec!["v".to_string()],
                vec![LogicalType::Integer],
            )),
        )
    }

    fn int_column(table_index: usize, column_index: usize) -> Expression {
        Expression::ColumnRef(ColumnRefExpression::new(
            ColumnBinding::new(table_index, column_index),
            LogicalType::Integer,
        ))
    }

    fn int_constant(value: i32) -> Expression {
        Expression::Constant(ConstantExpression::new(
            Value::Integer(value),
            LogicalType::Integer,
        ))
    }

    fn comparison(
        comparison_type: ComparisonType,
        left: Expression,
        right: Expression,
    ) -> Expression {
        Expression::Comparison(ComparisonExpression::new(comparison_type, left, right))
    }

    fn volatile_expression() -> Expression {
        let function = paro_function::scalar::math::get_random_function()
            .functions
            .into_iter()
            .next()
            .expect("random overload");
        Expression::Function(FunctionExpression::new(
            function,
            vec![],
            LogicalType::Double,
        ))
    }

    #[test]
    fn reorders_filter_expressions_by_estimated_selectivity() {
        let bind_context = BindContext::new();
        let child = integer_get(&bind_context, 0);
        let range_predicate = comparison(
            ComparisonType::GreaterThan,
            int_column(0, 0),
            int_constant(10),
        );
        let equality_predicate =
            comparison(ComparisonType::Equal, int_column(0, 0), int_constant(42));
        let plan = OwnedLogicalPlan::new(
            &bind_context,
            LogicalOperator::Filter(Filter::new(
                child,
                vec![range_predicate.clone(), equality_predicate.clone()],
            )),
        );
        let ctx = OptimizationContext {
            session: make_test_session(),
            bind_context: bind_context.clone(),
            column_stats: Default::default(),
            graph_stats: GraphStatsCache::with_loader(Arc::new(EmptyGraphStatsLoader)),
            cost_model: CostModel::default(),
            verify_enabled: true,
            profiler: OptimizerProfiler::default(),
            invalidations: crate::context::OptimizerInvalidations::default(),
        };

        let rewritten = ReorderFilter::new()
            .rewrite(plan, &ctx)
            .expect("rewrite succeeds");

        let LogicalOperator::Filter(filter) = &rewritten.operator else {
            panic!("expected filter");
        };
        assert!(filter.expressions[0].equals(&equality_predicate));
        assert!(filter.expressions[1].equals(&range_predicate));
    }

    #[test]
    fn node_reordering_does_not_rewrite_descendant_filters() {
        let bind_context = BindContext::new();
        let range_predicate = comparison(
            ComparisonType::GreaterThan,
            int_column(0, 0),
            int_constant(10),
        );
        let equality_predicate =
            comparison(ComparisonType::Equal, int_column(0, 0), int_constant(42));
        let child = OwnedLogicalPlan::new(
            &bind_context,
            LogicalOperator::Filter(Filter::new(
                integer_get(&bind_context, 0),
                vec![range_predicate.clone(), equality_predicate.clone()],
            )),
        );
        let plan = OwnedLogicalPlan::new(
            &bind_context,
            LogicalOperator::Filter(Filter::new(child, vec![equality_predicate.clone()])),
        );
        let ctx = OptimizationContext {
            session: make_test_session(),
            bind_context: bind_context.clone(),
            column_stats: Default::default(),
            graph_stats: GraphStatsCache::with_loader(Arc::new(EmptyGraphStatsLoader)),
            cost_model: CostModel::default(),
            verify_enabled: true,
            profiler: OptimizerProfiler::default(),
            invalidations: crate::context::OptimizerInvalidations::default(),
        };

        let (rewritten, changed) = ReorderFilter::new().reorder_node(plan, &ctx);

        assert!(!changed);
        let LogicalOperator::Filter(parent) = &rewritten.operator else {
            panic!("expected parent filter");
        };
        let LogicalOperator::Filter(child) = &parent.child.operator else {
            panic!("expected child filter");
        };
        assert!(child.expressions[0].equals(&range_predicate));
        assert!(child.expressions[1].equals(&equality_predicate));
    }

    #[test]
    fn volatile_expression_fences_filter_reordering() {
        let bind_context = BindContext::new();
        let range_predicate = comparison(
            ComparisonType::GreaterThan,
            int_column(0, 0),
            int_constant(10),
        );
        let volatile = volatile_expression();
        let equality_predicate =
            comparison(ComparisonType::Equal, int_column(0, 0), int_constant(42));
        let plan = OwnedLogicalPlan::new(
            &bind_context,
            LogicalOperator::Filter(Filter::new(
                integer_get(&bind_context, 0),
                vec![
                    range_predicate.clone(),
                    volatile.clone(),
                    equality_predicate.clone(),
                ],
            )),
        );
        let ctx = OptimizationContext {
            session: make_test_session(),
            bind_context: bind_context.clone(),
            column_stats: Default::default(),
            graph_stats: GraphStatsCache::with_loader(Arc::new(EmptyGraphStatsLoader)),
            cost_model: CostModel::default(),
            verify_enabled: true,
            profiler: OptimizerProfiler::default(),
            invalidations: crate::context::OptimizerInvalidations::default(),
        };

        let rewritten = ReorderFilter::new()
            .rewrite(plan, &ctx)
            .expect("rewrite succeeds");

        let LogicalOperator::Filter(filter) = &rewritten.operator else {
            panic!("expected filter");
        };
        assert!(filter.expressions[0].equals(&range_predicate));
        assert!(filter.expressions[1].equals(&volatile));
        assert!(filter.expressions[2].equals(&equality_predicate));
    }
}
