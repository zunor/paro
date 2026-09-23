// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Local canonical construction after global relational substitutions.
//! CTE demand and predicate routing remain ordered, nonlocal barriers. Do not
//! fold a new law into this boundary without proving it consumes canonical
//! children and commutes with the existing local laws.

use paro_common::error::Result;
use paro_planner::plan::OwnedLogicalPlan;

/// Empty pullup preserves emptiness under transparent filter composition.
/// Both laws consume canonical children, so one postorder is sufficient.
pub(crate) fn finish(plan: OwnedLogicalPlan) -> Result<OwnedLogicalPlan> {
    let empty = crate::subquery::empty_result::EmptyResultPullup::new();
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
                let old =
                    crate::subquery::empty_result::EmptyResultPullup::new().optimize_plan(copied);
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
