// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! The shared column/domain transfer contract.
//!
//! Selected-path discovery and native domain rewriting must agree on the
//! namespace at every operator boundary.  This module owns that agreement:
//! callers provide predicates in the current operator's output namespace and
//! receive predicates in each child namespace, together with predicates that
//! must remain at the current boundary.  It deliberately does not create Memo
//! expressions or make a quality decision.

use super::*;
use crate::expression::traversal::visit_expression;
use crate::filter::pushdown::FilterPushdown;
use paro_planner::expression::{ConjunctionExpression, ConjunctionType};
#[cfg(test)]
#[path = "domain_transfer_tests.rs"]
mod tests;
use paro_planner::operator::{
    Aggregate, Join, JoinType, LogicalOutputLayout, Projection, SetOpType, SetOperation,
};

/// The result of routing a batch of predicates across one logical operator.
///
/// `child_predicates[i]` is already rebound to child `i`'s output namespace.
/// `remaining` stays in the operator's original output namespace.  A `None`
/// result means that the operator shape or its output contract is not safe to
/// cross; it is different from a valid operator for which one predicate is
/// simply not movable.
#[derive(Debug, Clone, Default)]
pub(super) struct OperatorDomainTransfer {
    pub(super) child_predicates: Box<[Box<[Expression]>]>,
    pub(super) remaining: Box<[Expression]>,
    /// Residuals whose necessary-domain coverage is not established. They
    /// must not be certified as a legal barrier merely because routing stops.
    pub(super) unsupported: bool,
}

impl OperatorDomainTransfer {
    pub(super) fn has_moved(&self) -> bool {
        self.child_predicates
            .iter()
            .any(|predicates| !predicates.is_empty())
    }
}

/// The conservative domain subset understood by the transfer contract.
///
/// In particular, a predicate without a column reference is not a useful
/// domain request, and evaluation fences or nested/correlated references are
/// never moved by this path.
pub(super) fn is_local_domain(expression: &Expression) -> bool {
    let mut has_column = false;
    let mut admitted = !expression.evaluation_properties().is_reorder_fence();
    visit_expression(expression, &mut |part| match part {
        Expression::ColumnRef(column) => {
            has_column = true;
            admitted &= column.depth == 0;
        }
        Expression::Constant(_) | Expression::Comparison(_) | Expression::Conjunction(_) => {}
        _ => admitted = false,
    });
    admitted && has_column
}

/// Whether every column in a predicate belongs to one child layout.
pub(super) fn predicate_is_local_to_layout(
    predicate: &Expression,
    layout: &LogicalOutputLayout,
) -> bool {
    let mut local = is_local_domain(predicate);
    visit_expression(predicate, &mut |part| {
        if let Expression::ColumnRef(column) = part {
            local &= layout.bindings().contains(&column.binding);
        }
    });
    local
}

fn valid_output_predicate(predicate: &Expression, table_index: usize, width: usize) -> bool {
    let mut valid = true;
    visit_expression(predicate, &mut |part| {
        if let Expression::ColumnRef(column) = part {
            // An operator output has one binding namespace.  A foreign or
            // correlated reference must not be rebound by ordinal: doing so
            // would turn a malformed/mixed predicate into a different local
            // predicate and make discovery disagree with the rewrite.
            valid &= column.depth == 0
                && column.binding.table_index == table_index
                && column.binding.column_index < width;
        }
    });
    valid
}

fn valid_aggregate_output(predicate: &Expression, aggregate: &Aggregate<impl Sized>) -> bool {
    let mut valid = true;
    visit_expression(predicate, &mut |part| {
        if let Expression::ColumnRef(column) = part {
            let valid_slot = if column.binding.table_index == aggregate.group_index {
                column.binding.column_index < aggregate.groups.len()
            } else if column.binding.table_index == aggregate.aggregate_index {
                column.binding.column_index < aggregate.aggregates.len()
            } else if column.binding.table_index == aggregate.groupings_index {
                column.binding.column_index < aggregate.grouping_functions.len()
            } else {
                false
            };
            valid &= column.depth == 0 && valid_slot;
        }
    });
    valid
}

fn projection_expression_is_transparent<Child>(projection: &Projection<Child>) -> bool {
    projection.expressions.iter().all(|expression| {
        matches!(expression, Expression::ColumnRef(column) if column.depth == 0)
            || matches!(expression, Expression::Constant(_))
    })
}

fn projection_predicate<Child>(
    predicate: &Expression,
    projection: &Projection<Child>,
    child_layout: &LogicalOutputLayout,
) -> Option<Expression> {
    if !is_local_domain(predicate)
        || !valid_output_predicate(
            predicate,
            projection.table_index,
            projection.expressions.len(),
        )
        || !projection_expression_is_transparent(projection)
        || FilterPushdown::has_evaluation_fence_through_projection(projection, predicate)
    {
        return None;
    }
    let mut mapped = true;
    visit_expression(predicate, &mut |part| {
        if let Expression::ColumnRef(column) = part {
            mapped &= projection
                .expressions
                .get(column.binding.column_index)
                .is_some_and(|expression| match expression {
                    Expression::ColumnRef(projected) => {
                        projected.depth == 0 && child_layout.bindings().contains(&projected.binding)
                    }
                    Expression::Constant(_) => true,
                    _ => false,
                });
        }
    });
    if !mapped {
        return None;
    }
    let mut mapped = predicate.clone().replace_column_ref(&|column| {
        projection
            .expressions
            .get(column.binding.column_index)
            .cloned()
    });
    crate::expression::scalar_normalizer()
        .rewrite_expression(&mut mapped, &LogicalOperator::DummyScan);
    Some(mapped)
}

/// Abstract a predicate to a necessary condition in the grouping-key domain.
/// An unavailable atom denotes TRUE (no restriction), never an empty domain.
/// AND may retain supported conjuncts; OR needs a condition from every arm.
/// This is linear in the expression tree and never distributes into DNF.
fn aggregate_necessary_predicate<Child>(
    predicate: &Expression,
    aggregate: &Aggregate<Child>,
    child_layout: &LogicalOutputLayout,
    depth: usize,
) -> Option<Expression> {
    if depth == 128 || !aggregate.has_plain_grouping_domain() || !is_local_domain(predicate) {
        return None;
    }
    if let Some(mapped) = aggregate_predicate(predicate, aggregate, child_layout) {
        return Some(mapped);
    }
    let Expression::Conjunction(conjunction) = predicate else {
        return None;
    };
    let mut children = Vec::new();
    for child in &conjunction.children {
        match aggregate_necessary_predicate(child, aggregate, child_layout, depth + 1) {
            Some(mapped) => children.push(mapped),
            None if conjunction.conjunction_type == ConjunctionType::And => {}
            None => return None,
        }
    }
    match children.len() {
        0 => None,
        1 => children.pop(),
        _ => Some(Expression::Conjunction(
            ConjunctionExpression::new(conjunction.conjunction_type, children).into(),
        )),
    }
}

fn aggregate_predicate<Child>(
    predicate: &Expression,
    aggregate: &Aggregate<Child>,
    child_layout: &LogicalOutputLayout,
) -> Option<Expression> {
    if !is_local_domain(predicate)
        || !aggregate.has_plain_grouping_domain()
        || !FilterPushdown::group_filter_can_move(aggregate, predicate)
    {
        return None;
    }
    let mapped = predicate
        .clone()
        .replace_column_ref(&|column| aggregate.groups.get(column.binding.column_index).cloned());
    predicate_is_local_to_layout(&mapped, child_layout).then_some(mapped)
}

fn union_predicates<Child>(
    predicate: &Expression,
    setop: &SetOperation<Child>,
    left: &LogicalOutputLayout,
    right: &LogicalOutputLayout,
) -> Option<(Expression, Expression)> {
    if setop.setop_type != SetOpType::Union
        || !setop.setop_all
        || left.len() != setop.column_count
        || right.len() != setop.column_count
        || left.types() != right.types()
        || left.types() != setop.types.as_slice()
        || !is_local_domain(predicate)
        || !valid_output_predicate(predicate, setop.table_index, setop.column_count)
    {
        return None;
    }
    let rebind = |layout: &LogicalOutputLayout| {
        predicate.clone().replace_column_ref(&|column| {
            let mut column = column.clone();
            column.binding = layout.bindings()[column.binding.column_index];
            Some(Expression::ColumnRef(column.into()))
        })
    };
    Some((rebind(left), rebind(right)))
}

fn join_shape_is_transferable<Child>(
    operator: &LogicalOperator<Child>,
    child_layouts: &[&LogicalOutputLayout],
) -> bool {
    if child_layouts.len() != 2 {
        return false;
    }
    match operator {
        LogicalOperator::Join(Join::Comparison(join)) => {
            join.join_type == JoinType::Inner
                && join.duplicate_eliminated_columns.is_empty()
                && !join.delim_flipped
                && !crate::expression::comparison_join_has_evaluation_fence(join)
        }
        LogicalOperator::Join(Join::Cross(_)) => true,
        _ => false,
    }
}

/// Route predicates through one exact logical operator.
pub(super) fn transfer_predicates<Child>(
    operator: &LogicalOperator<Child>,
    child_layouts: &[&LogicalOutputLayout],
    predicates: &[Expression],
) -> Option<OperatorDomainTransfer> {
    let child_count = match operator {
        LogicalOperator::Filter(filter) => {
            if child_layouts.len() != 1
                || !filter.projection_map.is_identity(child_layouts[0].len())
            {
                return None;
            }
            1
        }
        LogicalOperator::Projection(_) => {
            if child_layouts.len() != 1 {
                return None;
            }
            1
        }
        LogicalOperator::Aggregate(_) => {
            if child_layouts.len() != 1 {
                return None;
            }
            1
        }
        LogicalOperator::SetOperation(setop) => {
            let [left, right] = child_layouts else {
                return None;
            };
            if left.len() != setop.column_count
                || right.len() != setop.column_count
                || left.types() != right.types()
                || left.types() != setop.types.as_slice()
            {
                return None;
            }
            2
        }
        LogicalOperator::Join(_) if child_layouts.len() == 2 => 2,
        _ => return None,
    };

    let mut child_predicates = vec![Vec::new(); child_count];
    let mut remaining = Vec::new();
    let mut unsupported = false;
    for predicate in predicates {
        if predicate.evaluation_properties().is_reorder_fence() {
            remaining.push(predicate.clone());
            continue;
        }
        match operator {
            LogicalOperator::Filter(_) => {
                let LogicalOperator::Filter(filter) = operator else {
                    unreachable!()
                };
                if filter.expressions.iter().all(is_local_domain)
                    && !filter
                        .expressions
                        .iter()
                        .any(|expression| expression.evaluation_properties().is_reorder_fence())
                    && filter.expressions.iter().all(|expression| {
                        predicate_is_local_to_layout(expression, child_layouts[0])
                    })
                    && predicate_is_local_to_layout(predicate, child_layouts[0])
                {
                    child_predicates[0].push(predicate.clone());
                } else {
                    unsupported |= !filter
                        .expressions
                        .iter()
                        .any(|expression| expression.evaluation_properties().is_reorder_fence());
                    remaining.push(predicate.clone());
                }
            }
            LogicalOperator::Projection(projection) => {
                if !valid_output_predicate(
                    predicate,
                    projection.table_index,
                    projection.expressions.len(),
                ) {
                    return None;
                }
                if let Some(mapped) = projection_predicate(predicate, projection, child_layouts[0])
                {
                    child_predicates[0].push(mapped);
                } else {
                    unsupported |= !projection
                        .expressions
                        .iter()
                        .any(|expression| expression.evaluation_properties().is_reorder_fence());
                    remaining.push(predicate.clone());
                }
            }
            LogicalOperator::Aggregate(aggregate) => {
                if !valid_aggregate_output(predicate, aggregate) {
                    return None;
                }
                if let Some(mapped) = aggregate_predicate(predicate, aggregate, child_layouts[0]) {
                    child_predicates[0].push(mapped);
                } else {
                    if aggregate.has_plain_grouping_domain() {
                        unsupported |= !is_local_domain(predicate);
                        visit_expression(predicate, &mut |part| {
                            if let Expression::ColumnRef(column) = part {
                                if column.binding.table_index == aggregate.group_index {
                                    unsupported |= !aggregate
                                        .groups
                                        .get(column.binding.column_index)
                                        .is_some_and(|group| {
                                            predicate_is_local_to_layout(group, child_layouts[0])
                                        });
                                }
                            }
                        });
                        let mut depth_stack = vec![(predicate, 0usize)];
                        while let Some((part, depth)) = depth_stack.pop() {
                            if depth >= 128 {
                                unsupported = true;
                                break;
                            }
                            if let Expression::Conjunction(conjunction) = part {
                                depth_stack.extend(
                                    conjunction.children.iter().map(|child| (child, depth + 1)),
                                );
                            }
                        }
                    }
                    if let Some(necessary) =
                        aggregate_necessary_predicate(predicate, aggregate, child_layouts[0], 0)
                    {
                        child_predicates[0].push(necessary);
                    }
                    remaining.push(predicate.clone());
                }
            }
            LogicalOperator::SetOperation(setop) => {
                if setop.setop_type != SetOpType::Union || !setop.setop_all {
                    if !valid_output_predicate(predicate, setop.table_index, setop.column_count) {
                        return None;
                    }
                    remaining.push(predicate.clone());
                    continue;
                }
                if !valid_output_predicate(predicate, setop.table_index, setop.column_count) {
                    return None;
                }
                if let Some((left, right)) =
                    union_predicates(predicate, setop, child_layouts[0], child_layouts[1])
                {
                    child_predicates[0].push(left);
                    child_predicates[1].push(right);
                } else {
                    unsupported |= !matches!(predicate, Expression::Constant(_));
                    remaining.push(predicate.clone());
                }
            }
            LogicalOperator::Join(_) => {
                if !join_shape_is_transferable(operator, child_layouts) {
                    remaining.push(predicate.clone());
                    continue;
                }
                let left = predicate_is_local_to_layout(predicate, child_layouts[0]);
                let right = predicate_is_local_to_layout(predicate, child_layouts[1]);
                match (left, right) {
                    (true, false) => child_predicates[0].push(predicate.clone()),
                    (false, true) => child_predicates[1].push(predicate.clone()),
                    _ => remaining.push(predicate.clone()),
                }
            }
            _ => unreachable!("operator was validated before predicate routing"),
        }
    }
    Some(OperatorDomainTransfer {
        child_predicates: child_predicates
            .into_iter()
            .map(Vec::into_boxed_slice)
            .collect::<Vec<_>>()
            .into_boxed_slice(),
        remaining: remaining.into_boxed_slice(),
        unsupported,
    })
}
