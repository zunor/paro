// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Build/probe-side optimizer.
//!
//! estimation to keep delim joins and regular comparison joins on the cheaper
//! build side, while preserving join semantics when children are swapped.

use std::sync::Arc;

use paro_common::types::LogicalType;
use paro_context::StatementContext;
use paro_planner::operator::{Join, JoinType, LogicalOperator, ProjectionMap};
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
        // Reduction joins have explicit right-preserving inverses, so they can
        // choose the smaller build relation without changing multiplicity or
        // their one-side output contract. Outer joins retain their current
        // orientation because unmatched-row emission is a separate pipeline.
        if !matches!(
            join.join_type,
            JoinType::Inner
                | JoinType::Semi
                | JoinType::Anti
                | JoinType::RightSemi
                | JoinType::RightAnti
        ) {
            return;
        }

        let Some(inverse_type) = join.join_type.inverse() else {
            return;
        };

        let left_cost = self.comparison_build_cost(
            join.left.as_ref(),
            &join.left_projection_map,
            join.conditions.iter().map(|condition| &condition.left),
        );
        let right_cost = self.comparison_build_cost(
            join.right.as_ref(),
            &join.right_projection_map,
            join.conditions.iter().map(|condition| &condition.right),
        );
        let build_side = choose_hash_build_side(
            join.join_type,
            HashBuildCandidate {
                serialized_work: left_cost,
                contains_control_region: contains_control_region_boundary(join.left.as_ref()),
            },
            HashBuildCandidate {
                serialized_work: right_cost,
                contains_control_region: contains_control_region_boundary(join.right.as_ref()),
            },
        );
        if build_side == HashBuildSide::Right {
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
        let row_width = estimate_row_width(&plan.types()) as u128;
        cardinality.saturating_mul(row_width.max(1))
    }

    fn comparison_build_cost<'a>(
        &self,
        plan: &LogicalPlan,
        projection: &ProjectionMap,
        condition_expressions: impl Iterator<Item = &'a paro_planner::expression::Expression>,
    ) -> f64 {
        let cardinality = self.estimated_cardinality(plan) as f64;
        let plan_types = plan.types();
        let projected_types = projection
            .to_indices(plan_types.len())
            .into_iter()
            .filter_map(|index| plan_types.get(index).cloned());
        let condition_types = condition_expressions.map(|expression| expression.return_type());
        let projected_payload_width =
            estimate_row_payload_width(&projected_types.collect::<Vec<_>>());
        let condition_payload_width =
            estimate_row_payload_width(&condition_types.collect::<Vec<_>>());

        // Hash build rows store equality keys independently of the visible
        // payload, followed by hash, next-pointer, and match-mask state. The
        // former width-only estimate omitted these fixed bytes and therefore
        // preferred very large narrow build inputs over substantially smaller
        // relations. Charge the actual serialized shape used by
        // `BuildRowLayout`; projected key columns deliberately remain in the
        // payload because the join may expose them above the operator.
        let row_width =
            estimate_hash_build_row_width(projected_payload_width, condition_payload_width) as f64;
        cardinality * row_width.max(1.0)
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
}

pub(crate) fn contains_control_region_boundary(plan: &LogicalPlan) -> bool {
    let owns_region = matches!(
        &plan.operator,
        LogicalOperator::Join(Join::Comparison(join))
            if !join.duplicate_eliminated_columns.is_empty()
    ) || matches!(
        &plan.operator,
        LogicalOperator::MaterializedCTE(_) | LogicalOperator::RecursiveCTE(_)
    );
    owns_region
        || plan
            .children()
            .iter()
            .any(|child| contains_control_region_boundary(child))
}

/// Physical side selected as the hash-table build input.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HashBuildSide {
    Left,
    Right,
}

/// Inputs to the shared hash-build orientation policy.
#[derive(Debug, Clone, Copy)]
pub(crate) struct HashBuildCandidate {
    /// Cost of serializing this input into the physical build-row layout.
    pub serialized_work: f64,
    /// Whether moving this subtree to the probe side crosses a control region.
    pub contains_control_region: bool,
}

/// Choose the physical hash build side for one comparison join.
///
/// Both DP enumeration and final build/probe lowering call this function. A
/// reduction's filtering control region must remain a materialized build
/// producer; otherwise the smaller serialized build input wins, with the
/// current right-build convention breaking ties.
pub(crate) fn choose_hash_build_side(
    join_type: JoinType,
    left: HashBuildCandidate,
    right: HashBuildCandidate,
) -> HashBuildSide {
    let filtering_side = match join_type {
        JoinType::Semi | JoinType::Anti => Some(HashBuildSide::Right),
        JoinType::RightSemi | JoinType::RightAnti => Some(HashBuildSide::Left),
        _ => None,
    };
    if filtering_side == Some(HashBuildSide::Left) && left.contains_control_region {
        return HashBuildSide::Left;
    }
    if filtering_side == Some(HashBuildSide::Right) && right.contains_control_region {
        return HashBuildSide::Right;
    }
    if right.serialized_work <= left.serialized_work {
        HashBuildSide::Right
    } else {
        HashBuildSide::Left
    }
}

/// Estimate the bytes carried by one intermediate execution row.
///
/// Used by the final build/probe pass for cross products and tests.
pub(crate) fn estimate_row_width(types: &[LogicalType]) -> usize {
    8 + estimate_row_payload_width(types)
}

/// Estimate one serialized comparison-hash-build row from its visible payload
/// and independently stored condition values.
///
/// Join-order enumeration has relation payload widths rather than a completed
/// logical join. Sharing this byte model keeps its build-side choice aligned
/// with the final build/probe pass while allowing the latter to use the exact
/// projected and condition types.
pub(crate) fn estimate_hash_build_row_width(
    projected_payload_width: usize,
    condition_payload_width: usize,
) -> usize {
    const HASH_BUILD_RUNTIME_BYTES: usize =
        std::mem::size_of::<u64>() + std::mem::size_of::<usize>() + std::mem::size_of::<u8>();
    8usize
        .saturating_add(projected_payload_width)
        .saturating_add(condition_payload_width)
        .saturating_add(HASH_BUILD_RUNTIME_BYTES)
}

/// Estimate the schema-dependent bytes without a row-container header.
/// Join-order costing combines multiple base relations into one intermediate,
/// so the fixed header must be charged once for that intermediate rather than
/// once per contributing relation.
pub(crate) fn estimate_row_payload_width(types: &[LogicalType]) -> usize {
    let mut width = 0;
    for ty in types {
        width += ty.type_size();
        width += type_penalty(ty);
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
        LogicalType::List(child) => 32 + type_penalty(child),
        LogicalType::Array(child, _) => 32 + type_penalty(child),
        LogicalType::Struct(fields) => {
            16 + fields.iter().map(|(_, ty)| type_penalty(ty)).sum::<usize>()
        }
        _ => 1,
    }
}

#[cfg(test)]
mod tests {
    use super::{contains_control_region_boundary, BuildProbeSideOptimizer};
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
    fn build_probe_charges_hash_row_state_before_choosing_a_large_narrow_build() {
        let ctx = BindContext::new();
        let filtered_fact = expression_get(
            0,
            1,
            vec![
                LogicalType::BigInt,
                LogicalType::BigInt,
                LogicalType::BigInt,
                LogicalType::BigInt,
                LogicalType::Varchar,
            ],
        );
        let dimension = expression_get(1, 1, vec![LogicalType::BigInt, LogicalType::Date]);
        let mut join = ComparisonJoin::new(
            JoinType::Inner,
            plan_with_cardinality(&ctx, filtered_fact, 320_000),
            plan_with_cardinality(&ctx, dimension, 1_500_000),
            vec![JoinCondition::new(
                Expression::ColumnRef(ColumnRefExpression::new(
                    ColumnBinding::new(0, 0),
                    LogicalType::BigInt,
                )),
                Expression::ColumnRef(ColumnRefExpression::new(
                    ColumnBinding::new(1, 0),
                    LogicalType::BigInt,
                )),
                JoinComparisonType::Equal,
            )],
        );
        // Only the date payload is consumed above the join. The equality key
        // remains independently stored in the physical build row.
        join.right_projection_map = vec![1].into();

        let result = BuildProbeSideOptimizer::new(make_test_session())
            .optimize(LogicalOperator::Join(Join::Comparison(join)));
        let LogicalOperator::Join(Join::Comparison(join)) = result else {
            panic!("expected comparison join");
        };
        assert_eq!(
            join.right.stats.estimated_cardinality.unwrap().expected,
            320_000,
            "the smaller filtered input should become the hash build"
        );
        assert_eq!(join.right.types().len(), 5);
    }

    #[test]
    fn build_probe_flips_reduction_join_to_smaller_preserved_build_side() {
        let ctx = BindContext::new();
        let preserved = expression_get(0, 1, vec![LogicalType::Integer]);
        let filtering = expression_get(1, 64, vec![LogicalType::Integer]);
        let join = ComparisonJoin::new(
            JoinType::Semi,
            plan_with_cardinality(&ctx, preserved, 1),
            plan_with_cardinality(&ctx, filtering, 64),
            vec![JoinCondition::new(
                Expression::ColumnRef(ColumnRefExpression::new(
                    ColumnBinding::new(0, 0),
                    LogicalType::Integer,
                )),
                Expression::ColumnRef(ColumnRefExpression::new(
                    ColumnBinding::new(1, 0),
                    LogicalType::Integer,
                )),
                JoinComparisonType::Equal,
            )],
        );

        let result = BuildProbeSideOptimizer::new(make_test_session())
            .optimize(LogicalOperator::Join(Join::Comparison(join)));
        let LogicalOperator::Join(Join::Comparison(join)) = result else {
            panic!("expected comparison join");
        };
        assert_eq!(join.join_type, JoinType::RightSemi);
        assert_eq!(join.right.stats.estimated_cardinality.unwrap().expected, 1);
        assert!(join.left_projection_map.is_none());
        assert!(join.right_projection_map.is_all());
    }

    #[test]
    fn build_probe_keeps_control_region_off_reduction_probe_side() {
        let ctx = BindContext::new();
        let preserved = expression_get(0, 1, vec![LogicalType::Integer]);
        let dependent_left = expression_get(1, 64, vec![LogicalType::Integer]);
        let dependent_right = expression_get(2, 1, vec![LogicalType::Integer]);
        let mut dependent = ComparisonJoin::new(
            JoinType::Inner,
            plan_with_cardinality(&ctx, dependent_left, 64),
            plan_with_cardinality(&ctx, dependent_right, 1),
            vec![JoinCondition::new(
                Expression::ColumnRef(ColumnRefExpression::new(
                    ColumnBinding::new(1, 0),
                    LogicalType::Integer,
                )),
                Expression::ColumnRef(ColumnRefExpression::new(
                    ColumnBinding::new(2, 0),
                    LogicalType::Integer,
                )),
                JoinComparisonType::Equal,
            )],
        );
        dependent.duplicate_eliminated_columns = vec![Expression::ColumnRef(
            ColumnRefExpression::new(ColumnBinding::new(1, 0), LogicalType::Integer),
        )];
        let join = ComparisonJoin::new(
            JoinType::Semi,
            plan_with_cardinality(&ctx, preserved, 1),
            plan_with_cardinality(&ctx, LogicalOperator::Join(Join::Comparison(dependent)), 64),
            vec![JoinCondition::new(
                Expression::ColumnRef(ColumnRefExpression::new(
                    ColumnBinding::new(0, 0),
                    LogicalType::Integer,
                )),
                Expression::ColumnRef(ColumnRefExpression::new(
                    ColumnBinding::new(1, 0),
                    LogicalType::Integer,
                )),
                JoinComparisonType::Equal,
            )],
        );

        let result = BuildProbeSideOptimizer::new(make_test_session())
            .optimize(LogicalOperator::Join(Join::Comparison(join)));
        let LogicalOperator::Join(Join::Comparison(join)) = result else {
            panic!("expected comparison join");
        };
        assert_eq!(join.join_type, JoinType::Semi);
        assert_eq!(join.left.stats.estimated_cardinality.unwrap().expected, 1);
        assert_eq!(join.right.stats.estimated_cardinality.unwrap().expected, 64);
    }

    #[test]
    fn build_probe_normalizes_right_reduction_control_region_onto_build_side() {
        let ctx = BindContext::new();
        let dependent_left = expression_get(0, 64, vec![LogicalType::Integer]);
        let dependent_right = expression_get(1, 1, vec![LogicalType::Integer]);
        let mut dependent = ComparisonJoin::new(
            JoinType::Inner,
            plan_with_cardinality(&ctx, dependent_left, 64),
            plan_with_cardinality(&ctx, dependent_right, 1),
            vec![JoinCondition::new(
                Expression::ColumnRef(ColumnRefExpression::new(
                    ColumnBinding::new(0, 0),
                    LogicalType::Integer,
                )),
                Expression::ColumnRef(ColumnRefExpression::new(
                    ColumnBinding::new(1, 0),
                    LogicalType::Integer,
                )),
                JoinComparisonType::Equal,
            )],
        );
        dependent.duplicate_eliminated_columns = vec![Expression::ColumnRef(
            ColumnRefExpression::new(ColumnBinding::new(0, 0), LogicalType::Integer),
        )];
        let preserved = expression_get(2, 1, vec![LogicalType::Integer]);
        let join = ComparisonJoin::new(
            JoinType::RightSemi,
            plan_with_cardinality(&ctx, LogicalOperator::Join(Join::Comparison(dependent)), 64),
            plan_with_cardinality(&ctx, preserved, 1),
            vec![JoinCondition::new(
                Expression::ColumnRef(ColumnRefExpression::new(
                    ColumnBinding::new(0, 0),
                    LogicalType::Integer,
                )),
                Expression::ColumnRef(ColumnRefExpression::new(
                    ColumnBinding::new(2, 0),
                    LogicalType::Integer,
                )),
                JoinComparisonType::Equal,
            )],
        );

        let result = BuildProbeSideOptimizer::new(make_test_session())
            .optimize(LogicalOperator::Join(Join::Comparison(join)));
        let LogicalOperator::Join(Join::Comparison(join)) = result else {
            panic!("expected comparison join");
        };
        assert_eq!(join.join_type, JoinType::Semi);
        assert!(contains_control_region_boundary(join.right.as_ref()));
        assert_eq!(join.left.stats.estimated_cardinality.unwrap().expected, 1);
        assert_eq!(join.right.stats.estimated_cardinality.unwrap().expected, 64);
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
