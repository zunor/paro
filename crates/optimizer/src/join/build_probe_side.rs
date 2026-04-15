// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Build/probe-side optimizer.
//!
//! `build_probe_side_optimizer.cpp`. For Paro we only need enough cost
//! estimation to keep delim joins and regular comparison joins on the cheaper
//! build side, while preserving join semantics when children are swapped.

use std::sync::Arc;

use paro_common::types::LogicalType;
use paro_context::StatementContext;
use paro_planner::operator::{Join, JoinType, LogicalOperator};
use paro_planner::plan::LogicalPlan;

/// Choose a cheaper build side for joins.
pub struct BuildProbeSideOptimizer {
    session: Arc<StatementContext>,
}

impl BuildProbeSideOptimizer {
    pub fn new(session: Arc<StatementContext>) -> Self {
        Self { session }
    }

    #[cfg(test)]
    fn optimize(&mut self, plan: LogicalOperator) -> LogicalOperator {
        self.optimize_plan(LogicalPlan::synthetic(plan)).operator
    }

    pub fn optimize_plan(&mut self, plan: LogicalPlan) -> LogicalPlan {
        self.optimize_recursive_plan(plan)
    }

    fn optimize_recursive_plan(&mut self, plan: LogicalPlan) -> LogicalPlan {
        let plan = plan.map_children(|child| self.optimize_recursive_plan(child));
        let operator = match plan.operator {
            LogicalOperator::Join(join) => match join {
                Join::Comparison(mut comp) => {
                    self.try_flip_comparison_join(&mut comp);
                    LogicalOperator::Join(Join::Comparison(comp))
                }
                Join::Any(any) => LogicalOperator::Join(Join::Any(any)),
                Join::Cross(mut cross) => {
                    self.try_flip_cross_product(&mut cross);
                    LogicalOperator::Join(Join::Cross(cross))
                }
            },
            other => other,
        };
        LogicalPlan {
            id: plan.id,
            stats: plan.stats,
            operator,
        }
    }

    fn try_flip_comparison_join(&self, join: &mut paro_planner::operator::ComparisonJoin) {
        // Only INNER joins participate in build/probe side selection here.
        // Other join types keep their existing side contract.
        if join.join_type != JoinType::Inner {
            return;
        }

        let Some(inverse_type) = join.join_type.inverse() else {
            return;
        };

        let left_cost = self.build_cost(join.left.as_ref());
        let right_cost = self.build_cost(join.right.as_ref());
        if right_cost <= left_cost {
            return;
        }

        std::mem::swap(&mut join.left, &mut join.right);
        join.join_type = inverse_type;
        for cond in &mut join.conditions {
            std::mem::swap(&mut cond.left, &mut cond.right);
            cond.comparison = cond.comparison.flip();
        }
        std::mem::swap(
            &mut join.left_projection_map,
            &mut join.right_projection_map,
        );
        if !join.duplicate_eliminated_columns.is_empty() {
            join.delim_flipped = !join.delim_flipped;
        }
    }

    fn try_flip_cross_product(&self, join: &mut paro_planner::operator::CrossProduct) {
        let left_cost = self.build_cost(join.left.as_ref());
        let right_cost = self.build_cost(join.right.as_ref());
        if right_cost > left_cost {
            std::mem::swap(&mut join.left, &mut join.right);
        }
    }

    fn build_cost(&self, plan: &LogicalPlan) -> u128 {
        let cardinality = self.estimated_cardinality(plan) as u128;
        let row_width = self.estimate_row_width(&plan.types()) as u128;
        cardinality.saturating_mul(row_width.max(1))
    }

    fn estimated_cardinality(&self, plan: &LogicalPlan) -> usize {
        plan.stats
            .estimated_cardinality
            .map(|estimate| estimate.expected.min(usize::MAX as u64) as usize)
            .unwrap_or_else(|| self.default_cardinality())
    }

    fn default_cardinality(&self) -> usize {
        match self.session.get_setting("default_table_cardinality") {
            Some(paro_common::runtime_value::Value::BigInt(v)) if *v > 0 => *v as usize,
            Some(paro_common::runtime_value::Value::Integer(v)) if *v > 0 => *v as usize,
            _ => 1000,
        }
    }

    fn estimate_row_width(&self, types: &[LogicalType]) -> usize {
        let mut width = 8;
        for ty in types {
            width += ty.type_size();
            width += Self::type_penalty(ty);
        }
        width
    }

    fn type_penalty(ty: &LogicalType) -> usize {
        match ty {
            LogicalType::Varchar
            | LogicalType::VarcharCollation(_)
            | LogicalType::TsVector
            | LogicalType::TsQuery
            | LogicalType::Json
            | LogicalType::Jsonb
            | LogicalType::Blob => 8,
            LogicalType::List(child) => 32 + Self::type_penalty(child),
            LogicalType::Array(child, _) => 32 + Self::type_penalty(child),
            LogicalType::Struct(fields) => {
                16 + fields
                    .iter()
                    .map(|(_, ty)| Self::type_penalty(ty))
                    .sum::<usize>()
            }
            _ => 1,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::BuildProbeSideOptimizer;
    use paro_common::types::LogicalType;
    use paro_context::{test_support::TestStatementContextBuilder, StatementContext};
    use paro_planner::binder::context::BindContext;
    use paro_planner::expression::{ColumnRefExpression, Expression};
    use paro_planner::operator::{
        ColumnBinding, ComparisonJoin, ExpressionGet, Join, JoinComparisonType, JoinCondition,
        JoinType, LogicalOperator,
    };
    use paro_planner::plan::{CardinalityEstimate, LogicalPlan};
    use std::sync::Arc;

    fn plan_with_cardinality(
        ctx: &BindContext,
        op: LogicalOperator,
        estimated_rows: u64,
    ) -> LogicalPlan {
        let mut plan = LogicalPlan::new(ctx, op);
        plan.stats.estimated_cardinality = Some(CardinalityEstimate::exact(estimated_rows));
        plan
    }

    fn make_test_session() -> Arc<StatementContext> {
        TestStatementContextBuilder::minimal().build()
    }

    fn expression_get(
        table_index: usize,
        row_count: usize,
        types: Vec<LogicalType>,
    ) -> LogicalOperator {
        let expressions = (0..row_count)
            .map(|_| {
                types
                    .iter()
                    .enumerate()
                    .map(|(idx, ty)| {
                        Expression::ColumnRef(ColumnRefExpression::new(
                            ColumnBinding::new(table_index, idx),
                            ty.clone(),
                        ))
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            table_index,
            expressions,
            (0..types.len()).map(|idx| format!("c{idx}")).collect(),
            types,
        ))
    }

    #[test]
    fn build_probe_flips_inner_join_to_smaller_build_side() {
        let ctx = BindContext::new();
        let left = expression_get(0, 1, vec![LogicalType::Integer]);
        let right = expression_get(1, 64, vec![LogicalType::Varchar, LogicalType::Varchar]);
        let join = ComparisonJoin::new(
            JoinType::Inner,
            plan_with_cardinality(&ctx, left, 1),
            plan_with_cardinality(&ctx, right, 64),
            vec![JoinCondition::new(
                Expression::ColumnRef(ColumnRefExpression::new(
                    ColumnBinding::new(0, 0),
                    LogicalType::Integer,
                )),
                Expression::ColumnRef(ColumnRefExpression::new(
                    ColumnBinding::new(1, 0),
                    LogicalType::Varchar,
                )),
                JoinComparisonType::Equal,
            )],
        );

        let result = BuildProbeSideOptimizer::new(make_test_session())
            .optimize(LogicalOperator::Join(Join::Comparison(join)));

        match result {
            LogicalOperator::Join(Join::Comparison(join)) => {
                assert_eq!(join.join_type, JoinType::Inner);
                // HashJoin builds on the right child. After flip, right side should be
                // the smaller original left child.
                assert_eq!(join.right.types().len(), 1);
                match &join.conditions[0].left {
                    Expression::ColumnRef(col) => {
                        assert_eq!(col.binding.table_index, 1);
                    }
                    other => panic!("expected column ref, got {other:?}"),
                }
                match &join.conditions[0].right {
                    Expression::ColumnRef(col) => {
                        assert_eq!(col.binding.table_index, 0);
                    }
                    other => panic!("expected column ref, got {other:?}"),
                }
            }
            _ => panic!("expected comparison join"),
        }
    }

    #[test]
    fn build_probe_does_not_flip_left_join() {
        let ctx = BindContext::new();
        let left = expression_get(0, 1, vec![LogicalType::Integer]);
        let right = expression_get(1, 64, vec![LogicalType::Varchar, LogicalType::Varchar]);
        let join = ComparisonJoin::new(
            JoinType::Left,
            plan_with_cardinality(&ctx, left, 1),
            plan_with_cardinality(&ctx, right, 64),
            vec![JoinCondition::new(
                Expression::ColumnRef(ColumnRefExpression::new(
                    ColumnBinding::new(0, 0),
                    LogicalType::Integer,
                )),
                Expression::ColumnRef(ColumnRefExpression::new(
                    ColumnBinding::new(1, 0),
                    LogicalType::Varchar,
                )),
                JoinComparisonType::Equal,
            )],
        );

        let result = BuildProbeSideOptimizer::new(make_test_session())
            .optimize(LogicalOperator::Join(Join::Comparison(join)));

        match result {
            LogicalOperator::Join(Join::Comparison(join)) => {
                assert_eq!(join.join_type, JoinType::Left);
                match &join.conditions[0].left {
                    Expression::ColumnRef(col) => {
                        assert_eq!(col.binding.table_index, 0);
                    }
                    other => panic!("expected column ref, got {other:?}"),
                }
                match &join.conditions[0].right {
                    Expression::ColumnRef(col) => {
                        assert_eq!(col.binding.table_index, 1);
                    }
                    other => panic!("expected column ref, got {other:?}"),
                }
            }
            _ => panic!("expected comparison join"),
        }
    }

    #[test]
    fn build_probe_flips_delim_join_and_toggles_delim_flag() {
        let ctx = BindContext::new();
        let left = expression_get(0, 1, vec![LogicalType::Integer]);
        let right = expression_get(1, 64, vec![LogicalType::Varchar, LogicalType::Varchar]);
        let mut join = ComparisonJoin::new(
            JoinType::Inner,
            plan_with_cardinality(&ctx, left, 1),
            plan_with_cardinality(&ctx, right, 64),
            vec![JoinCondition::new(
                Expression::ColumnRef(ColumnRefExpression::new(
                    ColumnBinding::new(0, 0),
                    LogicalType::Integer,
                )),
                Expression::ColumnRef(ColumnRefExpression::new(
                    ColumnBinding::new(1, 0),
                    LogicalType::Varchar,
                )),
                JoinComparisonType::Equal,
            )],
        );
        join.duplicate_eliminated_columns = vec![Expression::ColumnRef(ColumnRefExpression::new(
            ColumnBinding::new(0, 0),
            LogicalType::Integer,
        ))];

        let result = BuildProbeSideOptimizer::new(make_test_session())
            .optimize(LogicalOperator::Join(Join::Comparison(join)));

        match result {
            LogicalOperator::Join(Join::Comparison(join)) => {
                assert!(join.delim_flipped);
                assert_eq!(join.join_type, JoinType::Inner);
                match &join.conditions[0].left {
                    Expression::ColumnRef(col) => {
                        assert_eq!(col.binding.table_index, 1);
                    }
                    other => panic!("expected column ref, got {other:?}"),
                }
            }
            _ => panic!("expected comparison join"),
        }
    }

    #[test]
    fn build_probe_uses_plan_stats_instead_of_expression_get_row_count() {
        let ctx = BindContext::new();
        let left = expression_get(0, 128, vec![LogicalType::Integer]);
        let right = expression_get(1, 1, vec![LogicalType::Varchar]);
        let join = ComparisonJoin::new(
            JoinType::Inner,
            plan_with_cardinality(&ctx, left, 8),
            plan_with_cardinality(&ctx, right, 1024),
            vec![JoinCondition::new(
                Expression::ColumnRef(ColumnRefExpression::new(
                    ColumnBinding::new(0, 0),
                    LogicalType::Integer,
                )),
                Expression::ColumnRef(ColumnRefExpression::new(
                    ColumnBinding::new(1, 0),
                    LogicalType::Varchar,
                )),
                JoinComparisonType::Equal,
            )],
        );

        let result = BuildProbeSideOptimizer::new(make_test_session())
            .optimize(LogicalOperator::Join(Join::Comparison(join)));

        match result {
            LogicalOperator::Join(Join::Comparison(join)) => {
                assert_eq!(
                    join.right.stats.estimated_cardinality,
                    Some(CardinalityEstimate::exact(8))
                );
                assert_eq!(
                    join.left.stats.estimated_cardinality,
                    Some(CardinalityEstimate::exact(1024))
                );
            }
            _ => panic!("expected comparison join"),
        }
    }
}
