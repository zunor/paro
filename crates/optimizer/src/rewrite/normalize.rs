// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Local canonical construction after global relational substitutions.
//! CTE demand and predicate routing remain ordered, nonlocal barriers. Do not
//! fold a new law into this boundary without proving it consumes canonical
//! children and commutes with the existing local laws.

use paro_common::error::Result;
use paro_planner::plan::OwnedLogicalPlan;

/// Complete predicate routing after a relational substitution. The second
/// routing pass is a dependency of scalar changes, not of a cache miss or of
/// visiting the pipeline again. A no-op scalar walk leaves the already routed
/// tree intact. A changed expression can expose a new domain or remove a fence,
/// so it conservatively reopens routing (including nonlocal join/CTE barriers).
pub(crate) fn predicates(
    plan: OwnedLogicalPlan,
    scalars: &mut crate::rewrite::expr::CanonicalScalars,
) -> OwnedLogicalPlan {
    let mut plan = crate::rewrite::predicate::pushdown::FilterPushdown::new().rewrite_plan(plan);
    if scalars.normalize_plan(&mut plan) {
        plan = crate::rewrite::predicate::pushdown::FilterPushdown::new().rewrite_plan(plan);
    }
    plan
}

/// Empty pullup preserves emptiness under transparent filter composition.
/// Both laws consume canonical children, so one postorder is sufficient.
pub(crate) fn finish(plan: OwnedLogicalPlan) -> Result<OwnedLogicalPlan> {
    let empty = crate::rewrite::subquery::empty_result::EmptyResultPullup::new();
    plan.try_map_post_order(|plan| {
        Ok(crate::cascades::planner::restriction::normalize_node(
            empty.normalize_node(plan),
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_common::{runtime_value::Value, types::LogicalType};
    use paro_planner::{
        expression::{ConstantExpression, Expression},
        operator::{EmptyResult, Filter, LogicalOperator},
    };

    #[test]
    fn predicate_construction_matches_separate_routing_oracle() {
        use crate::rewrite::predicate::pushdown::FilterPushdown;
        use paro_planner::expression::{
            ColumnRefExpression, ComparisonExpression, ComparisonType, ConjunctionExpression,
            ConjunctionType,
        };
        use paro_planner::operator::{ColumnBinding, ExpressionGet, Limit, Projection};
        fn integer(value: i32) -> Expression {
            Expression::Constant(
                ConstantExpression::new(Value::Integer(value), LogicalType::Integer).into(),
            )
        }
        fn column(table: usize) -> Expression {
            Expression::ColumnRef(
                ColumnRefExpression::new(ColumnBinding::new(table, 0), LogicalType::Integer).into(),
            )
        }
        for barrier in [false, true] {
            for nesting in 0..12 {
                let values = OwnedLogicalPlan::synthetic(LogicalOperator::ExpressionGet(
                    ExpressionGet::new(
                        1,
                        vec![vec![integer(1)], vec![integer(2)]],
                        vec!["k".into()],
                        vec![LogicalType::Integer],
                    ),
                ));
                let mut plan = OwnedLogicalPlan::synthetic(LogicalOperator::Projection(
                    Projection::new(2, values, vec![column(1)]),
                ));
                if barrier {
                    plan = OwnedLogicalPlan::synthetic(LogicalOperator::Limit(
                        Limit::new(plan, Some(integer(1)), None).into(),
                    ));
                }
                for _ in 0..nesting {
                    plan = OwnedLogicalPlan::synthetic(LogicalOperator::Filter(Filter::new(
                        plan,
                        vec![Expression::Conjunction(
                            ConjunctionExpression::new(
                                ConjunctionType::And,
                                vec![
                                    Expression::Comparison(
                                        ComparisonExpression::new(
                                            ComparisonType::Equal,
                                            column(2),
                                            integer(1),
                                        )
                                        .into(),
                                    ),
                                    Expression::Comparison(
                                        ComparisonExpression::new(
                                            ComparisonType::Equal,
                                            integer(1),
                                            integer(1),
                                        )
                                        .into(),
                                    ),
                                ],
                            )
                            .into(),
                        )],
                    )));
                }
                let copied = paro_planner::binder::deep_copy::duplicate_plan_preserving_indices(
                    &plan,
                    paro_planner::binder::context::BindContext::new()
                        .shared()
                        .as_ref(),
                );
                let mut expected = FilterPushdown::new().rewrite_plan(copied);
                crate::rewrite::expr::scalar_normalizer().rewrite_plan(&mut expected);
                let mut expected = FilterPushdown::new().rewrite_plan(expected);
                let mut actual =
                    predicates(plan, &mut crate::rewrite::expr::CanonicalScalars::default());
                for plan in [&mut actual, &mut expected] {
                    plan.visit_post_order_mut(|node| node.id = paro_planner::plan::PlanNodeId(0));
                }
                assert_eq!(
                    format!("{actual:?}"),
                    format!("{expected:?}"),
                    "barrier={barrier}, nesting={nesting}"
                );
                assert_eq!(actual.output_layout(), expected.output_layout());
            }
        }
    }

    #[test]
    fn local_construction_matches_separate_pass_oracle() {
        for empty_input in [false, true] {
            for depth in 0..12 {
                let mut plan = OwnedLogicalPlan::synthetic(LogicalOperator::DummyScan);
                if empty_input {
                    plan = OwnedLogicalPlan::synthetic(LogicalOperator::EmptyResult(
                        EmptyResult::new(plan),
                    ));
                }
                for i in 0..depth {
                    let predicate = Expression::Constant(
                        ConstantExpression::new(Value::Boolean(i % 3 != 0), LogicalType::Boolean)
                            .into(),
                    );
                    plan = OwnedLogicalPlan::synthetic(LogicalOperator::Filter(Filter::new(
                        plan,
                        vec![predicate],
                    )));
                }
                let mut copied = paro_planner::binder::deep_copy::duplicate_plan_preserving_indices(
                    &plan,
                    paro_planner::binder::context::BindContext::new()
                        .shared()
                        .as_ref(),
                );
                // The explicit occurrence copier assigns fresh node IDs;
                // this fixture uses synthetic IDs on both oracle arms.
                copied.visit_post_order_mut(|node| node.id = paro_planner::plan::PlanNodeId(0));
                let old = crate::rewrite::subquery::empty_result::EmptyResultPullup::new()
                    .optimize_plan(copied);
                let expected = crate::cascades::planner::restriction::normalize_tree(old).unwrap();
                let actual = finish(plan).unwrap();
                assert_eq!(
                    format!("{actual:?}"),
                    format!("{expected:?}"),
                    "empty={empty_input}, depth={depth}"
                );
                assert_eq!(actual.output_layout(), expected.output_layout());
            }
        }
    }
}
