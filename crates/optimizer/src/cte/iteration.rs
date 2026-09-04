// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Physical ownership rules for recursive CTE iterations.
//!
//! A recursive reference is the delta stream of the current iteration.  Hash,
//! nested-loop, and cross-product joins materialize their right input in a
//! query-lifetime breaker, so placing that stream on the right would retain an
//! earlier delta across iterations.  Canonicalize the recursive member once,
//! after join enumeration, so its single recursive reference remains on the
//! streaming (left/probe) side of every containing inner join.

use paro_common::error::{self as paro_error, Result};
use paro_planner::operator::{Join, JoinType, LogicalOperator};
use paro_planner::plan::LogicalPlan;

/// Normalize every recursive member's join ownership without affecting CTE
/// references that consume the completed result outside the iteration.
pub(crate) fn normalize_iteration_ownership(plan: LogicalPlan) -> Result<LogicalPlan> {
    plan.try_map_post_order(|plan| {
        plan.try_map_operator(|operator| match operator {
            LogicalOperator::RecursiveCTE(mut cte) => {
                cte.recursive = Box::new(orient_recursive_member(
                    *cte.recursive,
                    cte.cte_index,
                    &cte.cte_name,
                )?);
                Ok(LogicalOperator::RecursiveCTE(cte))
            }
            operator => Ok(operator),
        })
    })
}

fn orient_recursive_member(
    plan: LogicalPlan,
    cte_index: usize,
    cte_name: &str,
) -> Result<LogicalPlan> {
    let (plan, _) = plan.try_fold_post_order(|mut plan, children: Vec<bool>| {
        let contains_reference = matches!(
            &plan.operator,
            LogicalOperator::CTERef(reference) if reference.cte_index == cte_index
        ) || children.iter().any(|contains| *contains);

        if let LogicalOperator::Join(join) = &mut plan.operator {
            let left_contains_reference = children.first().copied().unwrap_or(false);
            let right_contains_reference = children.get(1).copied().unwrap_or(false);
            if left_contains_reference && right_contains_reference {
                return Err(paro_error::not_implemented(format!(
                    "recursive CTE '{cte_name}' may be referenced only once in its recursive member"
                )));
            }
            if right_contains_reference {
                move_recursive_input_to_probe(join, cte_name)?;
            }
            if left_contains_reference || right_contains_reference {
                // Every stateful implementation inside the loop must keep the
                // changing delta on its streaming side. This is a logical join
                // property, independent of predicate representation.
                join.set_build_side_constraint(
                    paro_planner::operator::JoinBuildSideConstraint::Right,
                );
            }
        }

        Ok((plan, contains_reference))
    })?;
    Ok(plan)
}

fn move_recursive_input_to_probe(join: &mut Join, cte_name: &str) -> Result<()> {
    match join {
        Join::Comparison(join) if join.join_type == JoinType::Inner => {
            std::mem::swap(&mut join.left, &mut join.right);
            for condition in &mut join.conditions {
                std::mem::swap(&mut condition.left, &mut condition.right);
                condition.comparison = condition.comparison.flip();
            }
            std::mem::swap(
                &mut join.left_projection_map,
                &mut join.right_projection_map,
            );
        }
        Join::Any(join) if join.join_type == JoinType::Inner => {
            std::mem::swap(&mut join.left, &mut join.right);
            std::mem::swap(
                &mut join.left_projection_map,
                &mut join.right_projection_map,
            );
        }
        Join::Cross(join) => std::mem::swap(&mut join.left, &mut join.right),
        _ => {
            return Err(paro_error::not_implemented(format!(
                "recursive CTE '{cte_name}' must be the preserved probe input of non-inner joins"
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use paro_common::types::LogicalType;
    use paro_planner::expression::{ColumnRefExpression, Expression};
    use paro_planner::operator::{
        AnyJoin, CTERef, ColumnBinding, ComparisonJoin, CrossProduct, ExpressionGet, Join,
        JoinBuildSideConstraint, JoinComparisonType, JoinCondition, JoinType, LogicalOperator,
        RecursiveCTE,
    };
    use paro_planner::plan::LogicalPlan;

    use super::normalize_iteration_ownership;

    fn values(table_index: usize) -> LogicalPlan {
        LogicalPlan::synthetic(LogicalOperator::ExpressionGet(ExpressionGet::new(
            table_index,
            vec![vec![Expression::Constant(
                paro_planner::expression::ConstantExpression::new(
                    paro_common::runtime_value::Value::Integer(1),
                    LogicalType::Integer,
                ),
            )]],
            vec!["value".to_string()],
            vec![LogicalType::Integer],
        )))
    }

    fn recursive_reference(cte_index: usize, table_index: usize) -> LogicalPlan {
        LogicalPlan::synthetic(LogicalOperator::CTERef(CTERef::new(
            cte_index,
            table_index,
            "delta".to_string(),
            vec!["value".to_string()],
            vec![LogicalType::Integer],
        )))
    }

    #[test]
    fn recursive_delta_is_the_probe_input_after_join_enumeration() {
        let cte_index = 7;
        let static_table_index = 11;
        let recursive_table_index = 12;
        let condition = JoinCondition::new(
            Expression::ColumnRef(ColumnRefExpression::new(
                ColumnBinding::new(static_table_index, 0),
                LogicalType::Integer,
            )),
            Expression::ColumnRef(ColumnRefExpression::new(
                ColumnBinding::new(recursive_table_index, 0),
                LogicalType::Integer,
            )),
            JoinComparisonType::LessThan,
        );
        let recursive = LogicalPlan::synthetic(LogicalOperator::Join(Join::Comparison(
            ComparisonJoin::new(
                JoinType::Inner,
                values(static_table_index),
                recursive_reference(cte_index, recursive_table_index),
                vec![condition],
            ),
        )));
        let plan = LogicalPlan::synthetic(LogicalOperator::RecursiveCTE(RecursiveCTE {
            cte_index,
            cte_name: "walk".to_string(),
            column_names: vec!["value".to_string()],
            column_types: vec![LogicalType::Integer],
            union_all: true,
            anchor: Box::new(values(10)),
            recursive: Box::new(recursive),
        }));

        let normalized = normalize_iteration_ownership(plan).expect("normalize recursive member");
        let LogicalOperator::RecursiveCTE(cte) = &normalized.operator else {
            panic!("expected recursive CTE");
        };
        let LogicalOperator::Join(Join::Comparison(join)) = &cte.recursive.operator else {
            panic!("expected comparison join");
        };
        assert!(matches!(
            &join.left.operator,
            LogicalOperator::CTERef(reference) if reference.cte_index == cte_index
        ));
        assert!(matches!(
            &join.right.operator,
            LogicalOperator::ExpressionGet(_)
        ));
        assert_eq!(
            join.conditions[0].comparison,
            JoinComparisonType::GreaterThan
        );
        let Expression::ColumnRef(left) = &join.conditions[0].left else {
            panic!("expected left column reference");
        };
        assert_eq!(left.binding.table_index, recursive_table_index);
        assert_eq!(join.build_side_constraint, JoinBuildSideConstraint::Right);
    }

    #[test]
    fn arbitrary_join_persists_recursive_build_ownership() {
        let cte_index = 7;
        let recursive =
            LogicalPlan::synthetic(LogicalOperator::Join(Join::Any(Box::new(AnyJoin::new(
                JoinType::Inner,
                values(11),
                recursive_reference(cte_index, 12),
                Expression::Constant(paro_planner::expression::ConstantExpression::new(
                    paro_common::runtime_value::Value::Boolean(true),
                    LogicalType::Boolean,
                )),
            )))));
        let plan = LogicalPlan::synthetic(LogicalOperator::RecursiveCTE(RecursiveCTE {
            cte_index,
            cte_name: "walk".to_string(),
            column_names: vec!["value".to_string()],
            column_types: vec![LogicalType::Integer],
            union_all: true,
            anchor: Box::new(values(10)),
            recursive: Box::new(recursive),
        }));

        let normalized = normalize_iteration_ownership(plan).unwrap();
        let LogicalOperator::RecursiveCTE(cte) = &normalized.operator else {
            panic!("expected recursive CTE");
        };
        let LogicalOperator::Join(Join::Any(join)) = &cte.recursive.operator else {
            panic!("expected arbitrary join");
        };
        assert!(matches!(join.left.operator, LogicalOperator::CTERef(_)));
        assert_eq!(join.build_side_constraint, JoinBuildSideConstraint::Right);
    }

    #[test]
    fn cross_product_persists_recursive_build_ownership() {
        let cte_index = 7;
        let recursive = LogicalPlan::synthetic(LogicalOperator::Join(Join::Cross(
            CrossProduct::new(values(11), recursive_reference(cte_index, 12)),
        )));
        let plan = LogicalPlan::synthetic(LogicalOperator::RecursiveCTE(RecursiveCTE {
            cte_index,
            cte_name: "walk".to_string(),
            column_names: vec!["value".to_string()],
            column_types: vec![LogicalType::Integer],
            union_all: true,
            anchor: Box::new(values(10)),
            recursive: Box::new(recursive),
        }));

        let normalized = normalize_iteration_ownership(plan).unwrap();
        let LogicalOperator::RecursiveCTE(cte) = &normalized.operator else {
            panic!("expected recursive CTE");
        };
        let LogicalOperator::Join(Join::Cross(join)) = &cte.recursive.operator else {
            panic!("expected cross product");
        };
        assert!(matches!(join.left.operator, LogicalOperator::CTERef(_)));
        assert_eq!(join.build_side_constraint, JoinBuildSideConstraint::Right);
    }
}
