// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Decompose grouped DISTINCT aggregates into two spillable aggregation stages.
//!
//! A hash table that owns DISTINCT modifier state cannot currently externalize
//! that state.  When every aggregate has the same DISTINCT argument tuple, an
//! equivalent plan first groups by `(group keys, distinct arguments)` and then
//! evaluates the original functions without DISTINCT.  Both hash tables use
//! the ordinary spillable aggregation path, so a hard resource grant remains a
//! feasibility constraint instead of becoming a reason to reject the query.

use paro_common::error::Result;
use paro_planner::binder::context::BindContext;
#[cfg(test)]
use paro_planner::binder::deep_copy::fork_plan_preserving_indices;
use paro_planner::expression::{AggregateType, ColumnRefExpression, Expression};
use paro_planner::logical::operator::{Aggregate, ColumnBinding, LogicalOperator};
use paro_planner::logical::plan::{NodeStats, OwnedLogicalPlan};

/// Keep the baseline in place unless there is a legal resource-feasibility
/// alternative. The read-only admission and rewrite share the same predicate;
/// absence of DISTINCT must not require an owned copy of the query.
#[cfg(test)]
pub fn fork_candidate(
    plan: OwnedLogicalPlan,
    bind_context: &BindContext,
) -> Result<(OwnedLogicalPlan, Option<OwnedLogicalPlan>)> {
    let mut pending = vec![&plan];
    let mut eligible = false;
    while let Some(node) = pending.pop() {
        if matches!(&node.operator, LogicalOperator::Aggregate(aggregate)
            if common_distinct_arguments(aggregate).is_some())
        {
            eligible = true;
            break;
        }
        node.operator
            .visit_child_links(&mut |child| pending.push(child.as_ref()));
    }
    if !eligible {
        return Ok((plan, None));
    }
    let (baseline, candidate) = fork_plan_preserving_indices(plan, bind_context.shared().as_ref())?;
    Ok((baseline, Some(candidate)))
}

/// Rewrite every independently eligible grouped aggregate in post-order.
pub fn optimize_plan(
    plan: OwnedLogicalPlan,
    bind_context: &BindContext,
) -> Result<(OwnedLogicalPlan, bool)> {
    let mut changed = false;
    let plan = plan.try_map_post_order(|plan| {
        let (plan, node_changed) = rewrite_node(plan, bind_context);
        changed |= node_changed;
        Ok(plan)
    })?;
    Ok((plan, changed))
}

fn rewrite_node(
    mut plan: OwnedLogicalPlan,
    bind_context: &BindContext,
) -> (OwnedLogicalPlan, bool) {
    let LogicalOperator::Aggregate(aggregate) = &mut plan.operator else {
        return (plan, false);
    };
    let Some(distinct_arguments) = common_distinct_arguments(aggregate) else {
        return (plan, false);
    };
    let distinct_arguments = distinct_arguments.to_vec();
    let original_group_count = aggregate.groups.len();

    let inner_group_index = bind_context.generate_table_index();
    let inner_aggregate_index = bind_context.generate_table_index();
    let inner_groupings_index = bind_context.generate_table_index();
    let inner_groups = aggregate
        .groups
        .iter()
        .cloned()
        .chain(distinct_arguments.iter().cloned())
        .collect::<Vec<_>>();
    let child = std::mem::replace(
        &mut aggregate.child,
        Box::new(OwnedLogicalPlan::synthetic(LogicalOperator::DummyScan)),
    );
    let inner = OwnedLogicalPlan::new(
        bind_context,
        LogicalOperator::Aggregate(Box::new(Aggregate::new(
            inner_group_index,
            inner_aggregate_index,
            inner_groupings_index,
            *child,
            inner_groups,
            Vec::new(),
            Vec::new(),
            Vec::new(),
        ))),
    );

    aggregate.groups = aggregate
        .groups
        .iter()
        .enumerate()
        .map(|(ordinal, group)| {
            Expression::ColumnRef(
                ColumnRefExpression::new(
                    ColumnBinding::new(inner_group_index, ordinal),
                    group.return_type(),
                )
                .into(),
            )
        })
        .collect();
    aggregate.aggregates = aggregate
        .aggregates
        .drain(..)
        .map(|expression| {
            let Expression::Aggregate(mut aggregate) = expression else {
                unreachable!("common_distinct_arguments accepted a non-aggregate expression")
            };
            aggregate.aggr_type = AggregateType::NonDistinct;
            aggregate.children = distinct_arguments
                .iter()
                .enumerate()
                .map(|(ordinal, argument)| {
                    Expression::ColumnRef(
                        ColumnRefExpression::new(
                            ColumnBinding::new(inner_group_index, original_group_count + ordinal),
                            argument.return_type(),
                        )
                        .into(),
                    )
                })
                .collect();
            Expression::Aggregate(aggregate)
        })
        .collect();
    aggregate.child = Box::new(inner);
    aggregate.recompute_returned_types();
    plan.stats = NodeStats::default();
    (plan, true)
}

fn common_distinct_arguments(aggregate: &Aggregate) -> Option<&[Expression]> {
    if aggregate.groups.is_empty()
        || !aggregate.grouping_sets.is_empty()
        || !aggregate.grouping_functions.is_empty()
        || aggregate.post_reduction.is_some()
        || aggregate.aggregates.is_empty()
    {
        return None;
    }
    let Expression::Aggregate(first) = aggregate.aggregates.first()? else {
        return None;
    };
    if first.aggr_type != AggregateType::Distinct
        || first.children.is_empty()
        || first.filter.is_some()
        || !first.order_bys.is_empty()
    {
        return None;
    }
    let arguments = first.children.as_slice();
    aggregate
        .aggregates
        .iter()
        .all(|expression| {
            let Expression::Aggregate(candidate) = expression else {
                return false;
            };
            candidate.aggr_type == AggregateType::Distinct
                && candidate.filter.is_none()
                && candidate.order_bys.is_empty()
                && candidate.children.len() == arguments.len()
                && candidate
                    .children
                    .iter()
                    .zip(arguments)
                    .all(|(candidate, expected)| candidate.equals(expected))
        })
        .then_some(arguments)
}

#[cfg(test)]
mod tests {
    use paro_common::types::LogicalType;
    use paro_function::aggregate::distributive::count::get_count_function;
    use paro_planner::binder::context::BindContext;
    use paro_planner::expression::{
        AggregateExpression, AggregateType, ColumnRefExpression, Expression,
    };
    use paro_planner::logical::operator::{
        Aggregate, ColumnBinding, ExpressionGet, LogicalOperator,
    };
    use paro_planner::logical::plan::OwnedLogicalPlan;

    use super::{fork_candidate, optimize_plan};

    fn column(table: usize, ordinal: usize) -> Expression {
        Expression::ColumnRef(
            ColumnRefExpression::new(ColumnBinding::new(table, ordinal), LogicalType::BigInt)
                .into(),
        )
    }

    fn count_distinct(input: Expression) -> Expression {
        let (function, _) = get_count_function()
            .bind(&[LogicalType::BigInt])
            .expect("bind count(bigint)");
        Expression::Aggregate(
            AggregateExpression::new(function, vec![input], LogicalType::BigInt)
                .with_aggr_type(AggregateType::Distinct)
                .into(),
        )
    }

    #[test]
    fn grouped_distinct_becomes_two_ordinary_grouping_stages() {
        let bind_context = BindContext::new();
        for _ in 0..8 {
            bind_context.generate_table_index();
        }
        let child = OwnedLogicalPlan::new(
            &bind_context,
            LogicalOperator::ExpressionGet(ExpressionGet::new(
                0,
                Vec::new(),
                vec!["group".to_string(), "value".to_string()],
                vec![LogicalType::BigInt, LogicalType::BigInt],
            )),
        );
        let plan = OwnedLogicalPlan::new(
            &bind_context,
            LogicalOperator::Aggregate(Box::new(Aggregate::new(
                1,
                2,
                3,
                child,
                vec![column(0, 0)],
                Vec::new(),
                vec![count_distinct(column(0, 1))],
                Vec::new(),
            ))),
        );

        let (baseline, candidate) = fork_candidate(plan, &bind_context).expect("prepare");
        let candidate = candidate.expect("mandatory feasibility alternative");
        assert_eq!(
            baseline.get_column_bindings(),
            candidate.get_column_bindings()
        );
        let (rewritten, changed) = optimize_plan(candidate, &bind_context).expect("rewrite");
        assert!(changed);
        let LogicalOperator::Aggregate(outer) = &rewritten.operator else {
            panic!("expected outer aggregate")
        };
        assert_eq!(
            outer.get_column_bindings(),
            [ColumnBinding::new(1, 0), ColumnBinding::new(2, 0)]
        );
        let Expression::Aggregate(count) = &outer.aggregates[0] else {
            panic!("expected count")
        };
        assert_eq!(count.aggr_type, AggregateType::NonDistinct);
        let LogicalOperator::Aggregate(inner) = &outer.child.operator else {
            panic!("expected inner aggregate")
        };
        assert_eq!(inner.groups.len(), 2);
        assert!(inner.aggregates.is_empty());
        let Expression::ColumnRef(argument) = &count.children[0] else {
            panic!("expected inner distinct-key reference")
        };
        assert_eq!(argument.binding, ColumnBinding::new(inner.group_index, 1));
    }

    #[test]
    fn ineligible_aggregates_do_not_fork_or_rebuild_the_baseline() {
        for case in 0..4 {
            let bind = BindContext::new();
            let mut aggregates = vec![count_distinct(column(0, 1))];
            match case {
                0 => aggregates.clear(),
                1 => {
                    let Expression::Aggregate(aggregate) = &mut aggregates[0] else {
                        unreachable!()
                    };
                    aggregate.aggr_type = AggregateType::NonDistinct;
                }
                2 => aggregates.push(count_distinct(column(0, 2))),
                3 => {} // Ungrouped DISTINCT is deliberately not this rule.
                _ => unreachable!(),
            }
            let plan = OwnedLogicalPlan::new(
                &bind,
                LogicalOperator::Aggregate(Box::new(Aggregate::new(
                    1,
                    2,
                    3,
                    OwnedLogicalPlan::dummy_scan(&bind),
                    if case == 3 {
                        Vec::new()
                    } else {
                        vec![column(0, 0)]
                    },
                    Vec::new(),
                    aggregates,
                    Vec::new(),
                ))),
            );
            let LogicalOperator::Aggregate(aggregate) = &plan.operator else {
                unreachable!()
            };
            let address = aggregate.as_ref() as *const Aggregate;
            let (baseline, candidate) = fork_candidate(plan, &bind).unwrap();
            assert!(candidate.is_none(), "case {case}");
            let LogicalOperator::Aggregate(aggregate) = &baseline.operator else {
                unreachable!()
            };
            assert_eq!(address, aggregate.as_ref() as *const Aggregate);
            let (_, changed) = optimize_plan(baseline, &bind).unwrap();
            assert!(!changed, "admission and rewriting disagree for case {case}");
        }
    }
}
