// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Canonicalize null-safe join equalities under executable non-NULL proofs.

use paro_planner::expression::{ColumnRefExpression, ConjunctionType, Expression, OperatorType};
use paro_planner::operator::{Join, JoinComparisonType, LogicalOperator};
use paro_planner::plan::LogicalPlan;

/// Replace `IS NOT DISTINCT FROM` with strict equality when either input is
/// proven non-NULL at the join boundary.
///
/// This is an equivalence rewrite, not a selectivity assumption. It restores
/// the canonical equality after dependent-join flattening has represented an
/// original equality as a null-safe comparison plus an executable inner-side
/// `IS NOT NULL` predicate.
pub fn optimize_plan(plan: LogicalPlan) -> (LogicalPlan, bool) {
    let mut changed = false;
    let plan = plan.map_children(|child| {
        let (child, child_changed) = optimize_plan(child);
        changed |= child_changed;
        child
    });
    let plan = plan.map_operator(|operator| match operator {
        LogicalOperator::Join(Join::Comparison(mut join)) => {
            for condition in &mut join.conditions {
                if condition.comparison == JoinComparisonType::NotDistinctFrom
                    && (expression_is_proven_non_null_at(join.left.as_ref(), &condition.left)
                        || expression_is_proven_non_null_at(join.right.as_ref(), &condition.right))
                {
                    condition.comparison = JoinComparisonType::Equal;
                    changed = true;
                }
            }
            LogicalOperator::Join(Join::Comparison(join))
        }
        operator => operator,
    });
    (plan, changed)
}

/// Follow a direct value through relational operators that preserve its
/// non-NULL proof. Projections are substituted exactly, set operations require
/// both branches, and NULL-extending join sides stop the walk.
fn expression_is_proven_non_null_at(plan: &LogicalPlan, expression: &Expression) -> bool {
    if let Expression::Constant(constant) = expression {
        return !constant.value.is_null();
    }
    let Expression::ColumnRef(column) = expression else {
        return false;
    };
    if column.depth != 0 || !plan.get_column_bindings().contains(&column.binding) {
        return false;
    }

    match &plan.operator {
        LogicalOperator::Filter(filter) => {
            predicates_prove_non_null(&filter.expressions, expression)
                || expression_is_proven_non_null_at(filter.child.as_ref(), expression)
        }
        LogicalOperator::Projection(projection)
            if column.binding.table_index == projection.table_index =>
        {
            projection
                .expressions
                .get(column.binding.column_index)
                .is_some_and(|projected| {
                    expression_is_proven_non_null_at(projection.child.as_ref(), projected)
                })
        }
        LogicalOperator::Join(Join::Comparison(join)) => {
            let left_has = join.left.get_column_bindings().contains(&column.binding);
            let right_has = join.right.get_column_bindings().contains(&column.binding);
            if left_has == right_has {
                return false;
            }
            if left_has {
                join.join_type.preserves_left_values()
                    && expression_is_proven_non_null_at(join.left.as_ref(), expression)
            } else {
                join.join_type.preserves_right_values()
                    && expression_is_proven_non_null_at(join.right.as_ref(), expression)
            }
        }
        LogicalOperator::Join(Join::Cross(join)) => {
            if join.left.get_column_bindings().contains(&column.binding) {
                expression_is_proven_non_null_at(join.left.as_ref(), expression)
            } else {
                expression_is_proven_non_null_at(join.right.as_ref(), expression)
            }
        }
        LogicalOperator::SetOperation(setop)
            if column.binding.table_index == setop.table_index
                && column.binding.column_index < setop.column_count =>
        {
            let index = column.binding.column_index;
            let left_bindings = setop.left.get_column_bindings();
            let right_bindings = setop.right.get_column_bindings();
            let (Some(left_binding), Some(right_binding)) =
                (left_bindings.get(index), right_bindings.get(index))
            else {
                return false;
            };
            let return_type = expression.return_type();
            expression_is_proven_non_null_at(
                setop.left.as_ref(),
                &Expression::ColumnRef(ColumnRefExpression::new(
                    *left_binding,
                    return_type.clone(),
                )),
            ) && expression_is_proven_non_null_at(
                setop.right.as_ref(),
                &Expression::ColumnRef(ColumnRefExpression::new(*right_binding, return_type)),
            )
        }
        LogicalOperator::Order(order) => {
            expression_is_proven_non_null_at(order.child.as_ref(), expression)
        }
        LogicalOperator::TopN(topn) => {
            expression_is_proven_non_null_at(topn.child.as_ref(), expression)
        }
        LogicalOperator::Limit(limit) => {
            expression_is_proven_non_null_at(limit.child.as_ref(), expression)
        }
        LogicalOperator::Distinct(distinct) => {
            expression_is_proven_non_null_at(distinct.child.as_ref(), expression)
        }
        LogicalOperator::RowFetch(fetch) => {
            expression_is_proven_non_null_at(fetch.child.as_ref(), expression)
        }
        LogicalOperator::ExternalProject(project)
            if project
                .child
                .get_column_bindings()
                .contains(&column.binding) =>
        {
            expression_is_proven_non_null_at(project.child.as_ref(), expression)
        }
        // No row survives an empty relation, so every value property holds
        // vacuously at this boundary.
        LogicalOperator::EmptyResult(_) => true,
        _ => false,
    }
}

fn predicates_prove_non_null(predicates: &[Expression], target: &Expression) -> bool {
    predicates
        .iter()
        .flat_map(conjunction_terms)
        .any(|predicate| {
            matches!(
                predicate,
                Expression::Operator(operator)
                    if operator.operator_type == OperatorType::IsNotNull
                        && operator.children.len() == 1
                        && operator.children[0].equals(target)
            )
        })
}

fn conjunction_terms(expression: &Expression) -> Vec<&Expression> {
    match expression {
        Expression::Conjunction(conjunction)
            if conjunction.conjunction_type == ConjunctionType::And =>
        {
            conjunction
                .children
                .iter()
                .flat_map(conjunction_terms)
                .collect()
        }
        _ => vec![expression],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_common::types::LogicalType;
    use paro_planner::expression::{ColumnRefExpression, OperatorExpression};
    use paro_planner::operator::{
        ColumnBinding, ComparisonJoin, ExpressionGet, Filter, JoinCondition, JoinType, Projection,
    };

    fn column(table: usize) -> Expression {
        Expression::ColumnRef(ColumnRefExpression::new(
            ColumnBinding::new(table, 0),
            LogicalType::Integer,
        ))
    }

    fn values(table: usize) -> LogicalPlan {
        LogicalPlan::synthetic(LogicalOperator::ExpressionGet(ExpressionGet::new(
            table,
            vec![vec![column(table)]],
            vec!["v".to_string()],
            vec![LogicalType::Integer],
        )))
    }

    fn non_null_filter(table: usize) -> LogicalPlan {
        LogicalPlan::synthetic(LogicalOperator::Filter(Filter::new(
            values(table),
            vec![Expression::Operator(OperatorExpression::new_unary(
                OperatorType::IsNotNull,
                column(table),
                LogicalType::Boolean,
            ))],
        )))
    }

    #[test]
    fn restores_equality_through_projection() {
        let right = LogicalPlan::synthetic(LogicalOperator::Projection(Projection::new(
            2,
            non_null_filter(1),
            vec![column(1)],
        )));
        let plan = LogicalPlan::synthetic(LogicalOperator::Join(Join::Comparison(
            ComparisonJoin::new(
                JoinType::Semi,
                values(0),
                right,
                vec![JoinCondition::new(
                    column(0),
                    column(2),
                    JoinComparisonType::NotDistinctFrom,
                )],
            ),
        )));

        let (plan, changed) = optimize_plan(plan);
        assert!(changed);
        let LogicalOperator::Join(Join::Comparison(join)) = &plan.operator else {
            panic!("expected comparison join");
        };
        assert_eq!(join.conditions[0].comparison, JoinComparisonType::Equal);
    }

    #[test]
    fn does_not_cross_null_extending_side() {
        let nullable_right = LogicalPlan::synthetic(LogicalOperator::Join(Join::Comparison(
            ComparisonJoin::new(
                JoinType::Left,
                values(2),
                non_null_filter(1),
                vec![JoinCondition::new(
                    column(2),
                    column(1),
                    JoinComparisonType::Equal,
                )],
            ),
        )));
        let plan = LogicalPlan::synthetic(LogicalOperator::Join(Join::Comparison(
            ComparisonJoin::new(
                JoinType::Inner,
                values(0),
                nullable_right,
                vec![JoinCondition::new(
                    column(0),
                    column(1),
                    JoinComparisonType::NotDistinctFrom,
                )],
            ),
        )));

        let (plan, changed) = optimize_plan(plan);
        assert!(!changed);
        let LogicalOperator::Join(Join::Comparison(join)) = &plan.operator else {
            panic!("expected comparison join");
        };
        assert_eq!(
            join.conditions[0].comparison,
            JoinComparisonType::NotDistinctFrom
        );
    }
}
