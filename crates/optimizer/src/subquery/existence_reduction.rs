// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Remove multiplicity that an enclosing existence test cannot observe.
//!
//! A SEMI/ANTI join and a two-valued existence MARK consume only whether a
//! matching row exists on their filtering side. If an INNER join below that
//! boundary contributes demanded columns from only one input, the other input
//! is itself existential: `exists(A join B)` is `exists(A semi B)`. This is a
//! semantic reduction, independent of declared uniqueness or statistics.

use std::collections::HashSet;

use paro_common::error::Result;
use paro_planner::expression::{Expression, ExpressionIterator, ExpressionVisitDecision};
use paro_planner::operator::{
    AntiJoinMode, ColumnBinding, Join, JoinType, LogicalOperator, MarkJoinSemantics, ProjectionMap,
};
use paro_planner::plan::OwnedLogicalPlan;

pub(crate) fn optimize_plan(plan: OwnedLogicalPlan) -> Result<(OwnedLogicalPlan, bool)> {
    let mut changed = false;
    let plan = plan.try_map_post_order(|plan| {
        let (plan, local_changed) = reduce_existence_consumer(plan);
        changed |= local_changed;
        Ok(plan)
    })?;
    Ok((plan, changed))
}

fn reduce_existence_consumer(plan: OwnedLogicalPlan) -> (OwnedLogicalPlan, bool) {
    let LogicalOperator::Join(Join::Comparison(parent)) = &plan.operator else {
        return (plan, false);
    };
    let existence_only = (matches!(parent.join_type, JoinType::Semi | JoinType::Anti)
        && parent.anti_join_mode == AntiJoinMode::Regular)
        || (parent.join_type == JoinType::Mark
            && parent.mark_semantics == MarkJoinSemantics::TwoValued);
    if !existence_only
        || !parent.right_projection_map.is_none()
        || !parent.duplicate_eliminated_columns.is_empty()
        || parent.delim_flipped
    {
        return (plan, false);
    }

    let right_bindings = parent.right.get_column_bindings();
    let mut demanded = HashSet::new();
    if parent.conditions.is_empty()
        || parent.conditions.iter().any(|condition| {
            !collect_expression_bindings(&condition.right, &right_bindings, &mut demanded)
        })
        || demanded.is_empty()
    {
        return (plan, false);
    }

    let (id, stats, operator) = plan.into_parts();
    let LogicalOperator::Join(Join::Comparison(mut parent)) = operator else {
        unreachable!("existence consumer changed after recognition");
    };
    let (right, reduced) = reduce_input(*parent.right, &demanded);
    parent.right = Box::new(right);
    (
        OwnedLogicalPlan {
            id,
            stats,
            operator: LogicalOperator::Join(Join::Comparison(parent)),
        },
        reduced,
    )
}

fn reduce_input(
    plan: OwnedLogicalPlan,
    demanded: &HashSet<ColumnBinding>,
) -> (OwnedLogicalPlan, bool) {
    match &plan.operator {
        LogicalOperator::Projection(projection) => {
            let child_bindings = projection.child.get_column_bindings();
            let mut child_demanded = HashSet::new();
            if projection.expressions.iter().any(|expression| {
                !collect_expression_bindings(expression, &child_bindings, &mut child_demanded)
            }) {
                return (plan, false);
            }
            let (id, stats, operator) = plan.into_parts();
            let LogicalOperator::Projection(mut projection) = operator else {
                unreachable!();
            };
            let (child, changed) = reduce_input(*projection.child, &child_demanded);
            projection.child = Box::new(child);
            (
                OwnedLogicalPlan {
                    id,
                    stats,
                    operator: LogicalOperator::Projection(projection),
                },
                changed,
            )
        }
        LogicalOperator::Filter(filter) => {
            let child_bindings = filter.child.get_column_bindings();
            let mut child_demanded = demanded.clone();
            if filter.expressions.iter().any(|expression| {
                !collect_expression_bindings(expression, &child_bindings, &mut child_demanded)
            }) {
                return (plan, false);
            }
            let (id, stats, operator) = plan.into_parts();
            let LogicalOperator::Filter(mut filter) = operator else {
                unreachable!();
            };
            let (child, changed) = reduce_input(*filter.child, &child_demanded);
            filter.child = Box::new(child);
            (
                OwnedLogicalPlan {
                    id,
                    stats,
                    operator: LogicalOperator::Filter(filter),
                },
                changed,
            )
        }
        LogicalOperator::SetOperation(setop) if setop.is_union_all() => {
            let output_bindings = (0..setop.column_count)
                .map(|column_index| ColumnBinding::new(setop.table_index, column_index))
                .collect::<Vec<_>>();
            let demanded_indices = demanded
                .iter()
                .map(|binding| output_bindings.iter().position(|output| output == binding))
                .collect::<Option<Vec<_>>>();
            let Some(demanded_indices) = demanded_indices else {
                return (plan, false);
            };
            let left_bindings = setop.left.get_column_bindings();
            let right_bindings = setop.right.get_column_bindings();
            let Some((left_demanded, right_demanded)) = demanded_indices
                .iter()
                .map(|&index| Some((*left_bindings.get(index)?, *right_bindings.get(index)?)))
                .collect::<Option<Vec<_>>>()
                .map(|pairs| {
                    pairs.into_iter().fold(
                        (HashSet::new(), HashSet::new()),
                        |(mut left, mut right), (left_binding, right_binding)| {
                            left.insert(left_binding);
                            right.insert(right_binding);
                            (left, right)
                        },
                    )
                })
            else {
                return (plan, false);
            };

            let (id, stats, operator) = plan.into_parts();
            let LogicalOperator::SetOperation(mut setop) = operator else {
                unreachable!();
            };
            // A set operation owns a positional schema contract. A reduction
            // may change a join's width only below a Projection that restores
            // the branch width expected here.
            let (left, left_changed) = reduce_setop_branch(*setop.left, &left_demanded);
            let (right, right_changed) = reduce_setop_branch(*setop.right, &right_demanded);
            setop.left = Box::new(left);
            setop.right = Box::new(right);
            (
                OwnedLogicalPlan {
                    id,
                    stats,
                    operator: LogicalOperator::SetOperation(setop),
                },
                left_changed || right_changed,
            )
        }
        LogicalOperator::Join(Join::Comparison(join)) => {
            let eligible = join.join_type == JoinType::Inner
                && join.anti_join_mode == AntiJoinMode::Regular
                && join.mark_index.is_none()
                && join.mark_semantics == MarkJoinSemantics::NotMark
                && join.duplicate_eliminated_columns.is_empty()
                && !join.delim_flipped;
            if !eligible {
                return (plan, false);
            }
            let left_bindings = join.left.get_column_bindings();
            let right_bindings = join.right.get_column_bindings();
            let preserve_left = demanded
                .iter()
                .all(|binding| left_bindings.contains(binding));
            let preserve_right = demanded
                .iter()
                .all(|binding| right_bindings.contains(binding));
            if preserve_left == preserve_right {
                return (plan, false);
            }

            let (id, stats, operator) = plan.into_parts();
            let LogicalOperator::Join(Join::Comparison(mut join)) = operator else {
                unreachable!();
            };
            if preserve_left {
                join.join_type = JoinType::Semi;
                join.right_projection_map = ProjectionMap::none();
            } else {
                join.join_type = JoinType::RightSemi;
                join.left_projection_map = ProjectionMap::none();
            }
            (
                OwnedLogicalPlan {
                    id,
                    stats,
                    operator: LogicalOperator::Join(Join::Comparison(join)),
                },
                true,
            )
        }
        _ => (plan, false),
    }
}

fn reduce_setop_branch(
    plan: OwnedLogicalPlan,
    demanded: &HashSet<ColumnBinding>,
) -> (OwnedLogicalPlan, bool) {
    match &plan.operator {
        LogicalOperator::Projection(_) => reduce_input(plan, demanded),
        LogicalOperator::Filter(filter) => {
            let child_bindings = filter.child.get_column_bindings();
            if filter.projection_map.to_indices(child_bindings.len()).len() != child_bindings.len()
            {
                return (plan, false);
            }
            let mut child_demanded = demanded.clone();
            if filter.expressions.iter().any(|expression| {
                !collect_expression_bindings(expression, &child_bindings, &mut child_demanded)
            }) {
                return (plan, false);
            }
            let (id, stats, operator) = plan.into_parts();
            let LogicalOperator::Filter(mut filter) = operator else {
                unreachable!();
            };
            let (child, changed) = reduce_setop_branch(*filter.child, &child_demanded);
            filter.child = Box::new(child);
            (
                OwnedLogicalPlan {
                    id,
                    stats,
                    operator: LogicalOperator::Filter(filter),
                },
                changed,
            )
        }
        _ => (plan, false),
    }
}

fn collect_expression_bindings(
    expression: &Expression,
    input_bindings: &[ColumnBinding],
    bindings: &mut HashSet<ColumnBinding>,
) -> bool {
    let mut valid = true;
    ExpressionIterator::visit(expression, &mut |node| match node {
        Expression::ColumnRef(column) => {
            if column.depth != 0 || !input_bindings.contains(&column.binding) {
                valid = false;
            } else {
                bindings.insert(column.binding);
            }
            ExpressionVisitDecision::SkipChildren
        }
        // Changing an input join's width invalidates positional references.
        // Reduction is therefore confined to bound-column expressions; a
        // physical-layout rewrite needs a separate, explicit rebasing pass.
        Expression::Reference(_) => {
            valid = false;
            ExpressionVisitDecision::SkipChildren
        }
        _ => ExpressionVisitDecision::Descend,
    });
    valid
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_common::runtime_value::Value;
    use paro_common::types::LogicalType;
    use paro_planner::binder::context::BindContext;
    use paro_planner::expression::{ColumnRefExpression, ConstantExpression};
    use paro_planner::operator::{
        ComparisonJoin, ExpressionGet, JoinComparisonType, JoinCondition, Projection, SetOperation,
    };

    fn values(ctx: &BindContext, table: usize) -> OwnedLogicalPlan {
        OwnedLogicalPlan::new(
            ctx,
            LogicalOperator::ExpressionGet(ExpressionGet::new(
                table,
                vec![vec![Expression::Constant(ConstantExpression::new(
                    Value::Integer(1),
                    LogicalType::Integer,
                ))]],
                vec!["k".into()],
                vec![LogicalType::Integer],
            )),
        )
    }

    fn column(table: usize) -> Expression {
        Expression::ColumnRef(ColumnRefExpression::new(
            ColumnBinding::new(table, 0),
            LogicalType::Integer,
        ))
    }

    fn equality(left: usize, right: usize) -> JoinCondition {
        JoinCondition::new(column(left), column(right), JoinComparisonType::Equal)
    }

    #[test]
    fn inner_filtering_relation_becomes_semi_under_existence() {
        let ctx = BindContext::new();
        let inner = OwnedLogicalPlan::new(
            &ctx,
            LogicalOperator::Join(Join::Comparison(ComparisonJoin::new(
                JoinType::Inner,
                values(&ctx, 1),
                values(&ctx, 2),
                vec![equality(1, 2)],
            ))),
        );
        let outer = OwnedLogicalPlan::new(
            &ctx,
            LogicalOperator::Join(Join::Comparison(ComparisonJoin::new(
                JoinType::Semi,
                values(&ctx, 0),
                inner,
                vec![equality(0, 1)],
            ))),
        );

        let (result, changed) = optimize_plan(outer).unwrap();
        assert!(changed);
        let LogicalOperator::Join(Join::Comparison(outer)) = &result.operator else {
            panic!("expected outer semi join");
        };
        let LogicalOperator::Join(Join::Comparison(inner)) = &outer.right.operator else {
            panic!("expected reduced inner join");
        };
        assert_eq!(inner.join_type, JoinType::Semi);
        assert!(inner.right_projection_map.is_none());
    }

    #[test]
    fn inner_join_is_kept_when_both_sides_are_observed() {
        let ctx = BindContext::new();
        let inner = OwnedLogicalPlan::new(
            &ctx,
            LogicalOperator::Join(Join::Comparison(ComparisonJoin::new(
                JoinType::Inner,
                values(&ctx, 1),
                values(&ctx, 2),
                vec![equality(1, 2)],
            ))),
        );
        let outer = OwnedLogicalPlan::new(
            &ctx,
            LogicalOperator::Join(Join::Comparison(ComparisonJoin::new(
                JoinType::Semi,
                values(&ctx, 0),
                inner,
                vec![equality(0, 1), equality(0, 2)],
            ))),
        );

        let (_, changed) = optimize_plan(outer).unwrap();
        assert!(!changed);
    }

    #[test]
    fn existence_demand_reduces_each_union_all_branch() {
        let ctx = BindContext::new();
        let branch = |preserved, filtering, projection| {
            let inner = OwnedLogicalPlan::new(
                &ctx,
                LogicalOperator::Join(Join::Comparison(ComparisonJoin::new(
                    JoinType::Inner,
                    values(&ctx, preserved),
                    values(&ctx, filtering),
                    vec![equality(preserved, filtering)],
                ))),
            );
            OwnedLogicalPlan::new(
                &ctx,
                LogicalOperator::Projection(Projection::new(
                    projection,
                    inner,
                    vec![column(preserved)],
                )),
            )
        };
        let union = OwnedLogicalPlan::new(
            &ctx,
            LogicalOperator::SetOperation(SetOperation::union(
                7,
                branch(1, 2, 5),
                branch(3, 4, 6),
                true,
                vec![LogicalType::Integer],
            )),
        );
        let outer = OwnedLogicalPlan::new(
            &ctx,
            LogicalOperator::Join(Join::Comparison(ComparisonJoin::new(
                JoinType::Semi,
                values(&ctx, 0),
                union,
                vec![equality(0, 7)],
            ))),
        );

        let (result, changed) = optimize_plan(outer).unwrap();
        assert!(changed);
        let LogicalOperator::Join(Join::Comparison(outer)) = &result.operator else {
            panic!("expected outer semi join");
        };
        let LogicalOperator::SetOperation(union) = &outer.right.operator else {
            panic!("expected union-all input");
        };
        for branch in [union.left.as_ref(), union.right.as_ref()] {
            let LogicalOperator::Projection(projection) = &branch.operator else {
                panic!("expected branch projection");
            };
            let LogicalOperator::Join(Join::Comparison(join)) = &projection.child.operator else {
                panic!("expected reduced branch join");
            };
            assert_eq!(join.join_type, JoinType::Semi);
            assert!(join.right_projection_map.is_none());
        }
    }

    #[test]
    fn union_all_branch_schema_cannot_be_narrowed_in_place() {
        let ctx = BindContext::new();
        let branch = |left, right| {
            OwnedLogicalPlan::new(
                &ctx,
                LogicalOperator::Join(Join::Comparison(ComparisonJoin::new(
                    JoinType::Inner,
                    values(&ctx, left),
                    values(&ctx, right),
                    vec![equality(left, right)],
                ))),
            )
        };
        let union = OwnedLogicalPlan::new(
            &ctx,
            LogicalOperator::SetOperation(SetOperation::union(
                7,
                branch(1, 2),
                branch(3, 4),
                true,
                vec![LogicalType::Integer, LogicalType::Integer],
            )),
        );
        let outer = OwnedLogicalPlan::new(
            &ctx,
            LogicalOperator::Join(Join::Comparison(ComparisonJoin::new(
                JoinType::Semi,
                values(&ctx, 0),
                union,
                vec![equality(0, 7)],
            ))),
        );

        let (result, changed) = optimize_plan(outer).unwrap();
        assert!(!changed);
        let LogicalOperator::Join(Join::Comparison(outer)) = &result.operator else {
            panic!("expected outer semi join");
        };
        let LogicalOperator::SetOperation(union) = &outer.right.operator else {
            panic!("expected union-all input");
        };
        assert_eq!(union.column_count, 2);
        for branch in [union.left.as_ref(), union.right.as_ref()] {
            assert_eq!(branch.get_column_bindings().len(), 2);
            let LogicalOperator::Join(Join::Comparison(join)) = &branch.operator else {
                panic!("expected branch join");
            };
            assert_eq!(join.join_type, JoinType::Inner);
        }
    }
}
