// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Normalize a disjunction of sibling `EXISTS` markers into one reduction.
//!
//! Decorrelation represents `EXISTS(a) OR EXISTS(b)` as a chain of two-valued
//! MARK joins followed by a marker disjunction. Once column lifetime proves
//! that neither marker is observable above that filter, the equivalent
//! relational form is a SEMI join over `a UNION ALL b`. Besides removing the
//! marker columns, this exposes one preserved build relation whose exact
//! runtime membership filter can be shared by every union branch.

use std::collections::{HashMap, HashSet};

use paro_common::error::Result;
use paro_common::types::LogicalType;
use paro_planner::binder::context::BindContext;
use paro_planner::expression::{
    ColumnRefExpression, ConjunctionType, Expression, ExpressionIterator, ExpressionVisitDecision,
};
use paro_planner::operator::{
    ColumnBinding, ComparisonJoin, Join, JoinBuildSideConstraint, JoinComparisonType,
    JoinCondition, JoinType, LogicalOperator, MarkJoinSemantics, Projection, ProjectionMap,
    SetOperation,
};
use paro_planner::plan::LogicalPlan;
use paro_planner::visitor::LogicalOperatorVisitor;

pub(crate) fn optimize_plan(
    mut plan: LogicalPlan,
    bind_context: &BindContext,
) -> Result<(LogicalPlan, bool)> {
    let root_outputs = plan
        .get_column_bindings()
        .into_iter()
        .collect::<HashSet<_>>();
    let mut counter = BindingUseCounter::default();
    counter.visit_logical_plan(&mut plan);
    let mut changed = false;
    let plan = plan.try_map_post_order(|plan| {
        let Some(witness) = recognize(&plan, &counter.uses, &root_outputs) else {
            return Ok(plan);
        };
        changed = true;
        apply(plan, witness, bind_context)
    })?;
    Ok((plan, changed))
}

#[derive(Default)]
struct BindingUseCounter {
    uses: HashMap<ColumnBinding, usize>,
}

impl LogicalOperatorVisitor for BindingUseCounter {
    fn visit_replace_column_ref(
        &mut self,
        expression: &mut ColumnRefExpression,
    ) -> Option<Expression> {
        *self.uses.entry(expression.binding).or_default() += 1;
        None
    }
}

#[derive(Debug)]
struct DisjunctionWitness {
    filter_index: usize,
    markers: Vec<ColumnBinding>,
    output_bindings: Vec<ColumnBinding>,
    comparisons: Vec<JoinComparisonType>,
}

fn recognize(
    plan: &LogicalPlan,
    binding_uses: &HashMap<ColumnBinding, usize>,
    root_outputs: &HashSet<ColumnBinding>,
) -> Option<DisjunctionWitness> {
    let LogicalOperator::Filter(filter) = &plan.operator else {
        return None;
    };
    let (filter_index, markers) =
        filter
            .expressions
            .iter()
            .enumerate()
            .find_map(|(index, expression)| {
                marker_disjunction(expression).map(|markers| (index, markers))
            })?;
    if markers.len() < 2 {
        return None;
    }

    let marker_set = markers.iter().copied().collect::<HashSet<_>>();
    if marker_set.len() != markers.len() {
        return None;
    }
    if markers
        .iter()
        .any(|marker| binding_uses.get(marker).copied() != Some(1) || root_outputs.contains(marker))
    {
        return None;
    }
    let child_bindings = filter.child.get_column_bindings();
    let output_bindings = filter
        .projection_map
        .to_indices(child_bindings.len())
        .into_iter()
        .map(|index| child_bindings[index])
        .collect::<Vec<_>>();
    // Preserve the Filter node's complete output contract. Even when a parent
    // later drops a marker, changing this intermediate schema would invalidate
    // positional projection maps before the next schema-settling boundary.
    if output_bindings
        .iter()
        .any(|binding| marker_set.contains(binding))
    {
        return None;
    }

    let mut remaining = marker_set;
    let mut current = filter.child.as_ref();
    let mut common_left_keys: Option<Vec<&Expression>> = None;
    let mut common_comparisons: Option<Vec<JoinComparisonType>> = None;
    while !remaining.is_empty() {
        let LogicalOperator::Join(Join::Comparison(join)) = &current.operator else {
            return None;
        };
        let marker = marker_binding(join)?;
        if !remaining.remove(&marker) || !existence_branch_is_fusible(join) {
            return None;
        }
        let left_keys = join
            .conditions
            .iter()
            .map(|condition| &condition.left)
            .collect::<Vec<_>>();
        let comparisons = join
            .conditions
            .iter()
            .map(|condition| condition.comparison)
            .collect::<Vec<_>>();
        match &common_left_keys {
            None => common_left_keys = Some(left_keys),
            Some(common)
                if common.len() == left_keys.len()
                    && common
                        .iter()
                        .zip(left_keys)
                        .all(|(left, right)| left.equals(right)) => {}
            Some(_) => return None,
        }
        match &common_comparisons {
            None => common_comparisons = Some(comparisons),
            Some(common) if common == &comparisons => {}
            Some(_) => return None,
        }
        current = join.left.as_ref();
    }
    let base_bindings = current.get_column_bindings();
    if output_bindings
        .iter()
        .any(|binding| !base_bindings.contains(binding))
    {
        return None;
    }
    common_left_keys?
        .iter()
        .all(|expression| expression_bindings_belong_to(expression, &base_bindings))
        .then_some(DisjunctionWitness {
            filter_index,
            markers,
            output_bindings,
            comparisons: common_comparisons?,
        })
}

fn marker_disjunction(expression: &Expression) -> Option<Vec<ColumnBinding>> {
    let Expression::Conjunction(conjunction) = expression else {
        return None;
    };
    if conjunction.conjunction_type != ConjunctionType::Or {
        return None;
    }
    conjunction
        .children
        .iter()
        .map(|term| match term {
            Expression::ColumnRef(column)
                if column.depth == 0 && column.return_type == LogicalType::Boolean =>
            {
                Some(column.binding)
            }
            _ => None,
        })
        .collect()
}

fn marker_binding(join: &ComparisonJoin) -> Option<ColumnBinding> {
    let marker_index = join.mark_index?;
    (join.join_type == JoinType::Mark
        && join.mark_semantics == MarkJoinSemantics::TwoValued
        && join.anti_join_mode == paro_planner::operator::AntiJoinMode::Regular)
        .then(|| ColumnBinding::new(marker_index, 0))
}

fn existence_branch_is_fusible(join: &ComparisonJoin) -> bool {
    join.build_side_constraint == JoinBuildSideConstraint::Either
        && join.duplicate_eliminated_columns.is_empty()
        && !join.delim_flipped
        && !join.conditions.is_empty()
        && join.conditions.iter().all(|condition| {
            matches!(
                condition.comparison,
                JoinComparisonType::Equal | JoinComparisonType::NotDistinctFrom
            )
        })
}

fn expression_bindings_belong_to(expression: &Expression, bindings: &[ColumnBinding]) -> bool {
    let mut valid = true;
    ExpressionIterator::visit(expression, &mut |node| {
        if let Expression::ColumnRef(column) = node {
            if column.depth != 0 || !bindings.contains(&column.binding) {
                valid = false;
            }
            ExpressionVisitDecision::SkipChildren
        } else {
            ExpressionVisitDecision::Descend
        }
    });
    valid
}

fn apply(
    plan: LogicalPlan,
    witness: DisjunctionWitness,
    bind_context: &BindContext,
) -> Result<LogicalPlan> {
    let (id, stats, operator) = plan.into_parts();
    let LogicalOperator::Filter(mut filter) = operator else {
        unreachable!("existence-disjunction witness must belong to a filter");
    };
    let marker_set = witness.markers.into_iter().collect::<HashSet<_>>();
    let mut current = *filter.child;
    let mut branches = Vec::with_capacity(marker_set.len());
    let mut common_left_keys = None;

    loop {
        let (_, _, operator) = current.into_parts();
        let join = match operator {
            LogicalOperator::Join(Join::Comparison(join)) => join,
            _ => unreachable!("recognized marker chain changed before application"),
        };
        let marker = marker_binding(&join)
            .expect("recognized existence chain must retain its two-valued marker");
        if !marker_set.contains(&marker) {
            unreachable!("recognized marker chain changed before application");
        }
        if common_left_keys.is_none() {
            common_left_keys = Some(
                join.conditions
                    .iter()
                    .map(|condition| condition.left.clone())
                    .collect::<Vec<_>>(),
            );
        }
        let right_keys = join
            .conditions
            .iter()
            .map(|condition| condition.right.clone())
            .collect::<Vec<_>>();
        let projection_index = bind_context.generate_table_index();
        let projected = LogicalPlan::new(
            bind_context,
            LogicalOperator::Projection(
                Projection::new(projection_index, *join.right, right_keys).with_internal_outputs(),
            ),
        );
        branches.push(projected);
        current = *join.left;
        if branches.len() == marker_set.len() {
            break;
        }
    }

    let key_types = common_left_keys
        .as_ref()
        .expect("recognized existence chain must have keys")
        .iter()
        .map(|key| key.return_type())
        .collect::<Vec<_>>();
    let mut branches = branches.into_iter();
    let mut union = branches
        .next()
        .expect("recognized marker disjunction must have branches");
    let mut union_index = union
        .get_column_bindings()
        .first()
        .expect("existence branch must project at least one key")
        .table_index;
    for branch in branches {
        union_index = bind_context.generate_table_index();
        union = LogicalPlan::new(
            bind_context,
            LogicalOperator::SetOperation(SetOperation::union(
                union_index,
                union,
                branch,
                true,
                key_types.clone(),
            )),
        );
    }

    let base_bindings = current.get_column_bindings();
    let left_projection_map = witness
        .output_bindings
        .iter()
        .map(|binding| {
            base_bindings
                .iter()
                .position(|candidate| candidate == binding)
                .expect("recognized output binding must belong to preserved base")
        })
        .collect::<Vec<_>>();
    let conditions = common_left_keys
        .expect("recognized existence chain must have keys")
        .into_iter()
        .enumerate()
        .map(|(column_index, left)| {
            JoinCondition::new(
                left,
                Expression::ColumnRef(ColumnRefExpression::new(
                    ColumnBinding::new(union_index, column_index),
                    key_types[column_index].clone(),
                )),
                witness.comparisons[column_index],
            )
        })
        .collect::<Vec<_>>();
    let mut reduction = ComparisonJoin::new(JoinType::Semi, current, union, conditions);
    reduction.left_projection_map = ProjectionMap::new(left_projection_map);
    reduction.right_projection_map = ProjectionMap::none();
    let reduction = LogicalPlan::new(
        bind_context,
        LogicalOperator::Join(Join::Comparison(reduction)),
    );

    filter.expressions.remove(witness.filter_index);
    if filter.expressions.is_empty() {
        let (_, _, operator) = reduction.into_parts();
        return Ok(LogicalPlan {
            id,
            stats,
            operator,
        });
    }
    filter.child = Box::new(reduction);
    filter.projection_map = ProjectionMap::all();
    Ok(LogicalPlan {
        id,
        stats,
        operator: LogicalOperator::Filter(filter),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_common::runtime_value::Value;
    use paro_planner::expression::{ConjunctionExpression, ConstantExpression};
    use paro_planner::operator::{ExpressionGet, Filter, MarkJoinSemantics};

    fn value_plan(ctx: &BindContext, table_index: usize, width: usize) -> LogicalPlan {
        LogicalPlan::new(
            ctx,
            LogicalOperator::ExpressionGet(ExpressionGet::new(
                table_index,
                vec![vec![
                    Expression::Constant(ConstantExpression::new(
                        Value::Integer(1),
                        LogicalType::Integer,
                    ));
                    width
                ]],
                (0..width).map(|index| format!("c{index}")).collect(),
                vec![LogicalType::Integer; width],
            )),
        )
    }

    fn column(table_index: usize, column_index: usize, ty: LogicalType) -> Expression {
        Expression::ColumnRef(ColumnRefExpression::new(
            ColumnBinding::new(table_index, column_index),
            ty,
        ))
    }

    fn mark(
        ctx: &BindContext,
        left: LogicalPlan,
        right_table: usize,
        marker_table: usize,
    ) -> LogicalPlan {
        mark_with_comparison(
            ctx,
            left,
            right_table,
            marker_table,
            JoinComparisonType::Equal,
        )
    }

    fn mark_with_comparison(
        ctx: &BindContext,
        left: LogicalPlan,
        right_table: usize,
        marker_table: usize,
        comparison: JoinComparisonType,
    ) -> LogicalPlan {
        let mut join = ComparisonJoin::new(
            JoinType::Mark,
            left,
            value_plan(ctx, right_table, 1),
            vec![JoinCondition::new(
                column(0, 0, LogicalType::Integer),
                column(right_table, 0, LogicalType::Integer),
                comparison,
            )],
        );
        join.mark_index = Some(marker_table);
        join.mark_semantics = MarkJoinSemantics::TwoValued;
        LogicalPlan::new(ctx, LogicalOperator::Join(Join::Comparison(join)))
    }

    #[test]
    fn fuses_unobservable_exists_disjunction_into_union_semi_join() {
        let ctx = BindContext::new();
        let base = value_plan(&ctx, 0, 2);
        let inner = mark(&ctx, base, 1, 10);
        let outer = mark(&ctx, inner, 2, 11);
        let disjunction = Expression::Conjunction(ConjunctionExpression::new(
            ConjunctionType::Or,
            vec![
                column(10, 0, LogicalType::Boolean),
                column(11, 0, LogicalType::Boolean),
            ],
        ));
        let mut filter = Filter::new(outer, vec![disjunction]);
        filter.projection_map = ProjectionMap::new(vec![1]);
        let plan = LogicalPlan::new(&ctx, LogicalOperator::Filter(filter));

        let (result, changed) = optimize_plan(plan, &ctx).unwrap();
        assert!(changed);
        let LogicalOperator::Join(Join::Comparison(join)) = &result.operator else {
            panic!("marker disjunction should become a semi join");
        };
        assert_eq!(join.join_type, JoinType::Semi);
        assert_eq!(join.left_projection_map.as_columns(), Some(&[1][..]));
        assert!(join.right_projection_map.is_none());
        assert_eq!(result.get_column_bindings(), vec![ColumnBinding::new(0, 1)]);
        assert!(matches!(
            join.right.operator,
            LogicalOperator::SetOperation(_)
        ));
    }

    #[test]
    fn keeps_disjunction_when_a_marker_is_observable() {
        let ctx = BindContext::new();
        let base = value_plan(&ctx, 0, 2);
        let inner = mark(&ctx, base, 1, 10);
        let outer = mark(&ctx, inner, 2, 11);
        let disjunction = Expression::Conjunction(ConjunctionExpression::new(
            ConjunctionType::Or,
            vec![
                column(10, 0, LogicalType::Boolean),
                column(11, 0, LogicalType::Boolean),
            ],
        ));
        let mut filter = Filter::new(outer, vec![disjunction]);
        filter.projection_map = ProjectionMap::new(vec![1, 2]);
        let plan = LogicalPlan::new(&ctx, LogicalOperator::Filter(filter));

        let (_, changed) = optimize_plan(plan, &ctx).unwrap();
        assert!(!changed);
    }

    #[test]
    fn preserves_null_safe_comparison_semantics_when_fusing_markers() {
        let ctx = BindContext::new();
        let base = value_plan(&ctx, 0, 1);
        let inner = mark_with_comparison(&ctx, base, 1, 10, JoinComparisonType::NotDistinctFrom);
        let outer = mark_with_comparison(&ctx, inner, 2, 11, JoinComparisonType::NotDistinctFrom);
        let disjunction = Expression::Conjunction(ConjunctionExpression::new(
            ConjunctionType::Or,
            vec![
                column(10, 0, LogicalType::Boolean),
                column(11, 0, LogicalType::Boolean),
            ],
        ));
        let mut filter = Filter::new(outer, vec![disjunction]);
        filter.projection_map = ProjectionMap::new(vec![0]);

        let (result, changed) = optimize_plan(
            LogicalPlan::new(&ctx, LogicalOperator::Filter(filter)),
            &ctx,
        )
        .unwrap();
        assert!(changed);
        let LogicalOperator::Join(Join::Comparison(join)) = &result.operator else {
            panic!("marker disjunction should become a semi join");
        };
        assert!(join
            .conditions
            .iter()
            .all(|condition| condition.comparison == JoinComparisonType::NotDistinctFrom));
    }
}
