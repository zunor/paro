// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Defer wide dimension payload until after a fact-side partial aggregate.
//!
//! A grouped analytical query often joins a small dimension only to group by a
//! descriptive string. Materializing that string for every fact row is much
//! more expensive than grouping the fact stream by the equality key, joining
//! the compact partials to the dimension, and merging partial aggregate
//! results by the original SQL group. The final merge is required even when a
//! key is declared unique: SQL groups by the payload value, and two different
//! dimension keys may legally carry the same payload.

use std::cell::Cell;
use std::collections::HashSet;

use paro_common::error::Result;
use paro_planner::binder::context::BindContext;
use paro_planner::expression::Expression;
use paro_planner::expression::{AggregateExpression, AggregateType, ColumnRefExpression};
use paro_planner::operator::{Aggregate, ColumnBinding};
use paro_planner::operator::{ComparisonJoin, Join, JoinComparisonType, JoinType, LogicalOperator};
use paro_planner::plan::{NodeStats, OwnedLogicalPlan, PlanNodeId};

use crate::expression::traversal::visit_expression;

pub(crate) mod join_region;

/// Necessary root-only eligibility shared by dispatch and both rewrite paths.
/// These fields belong to the immutable aggregate shell. In particular this
/// must not inspect the child: a later child alternative can expose a dimension
/// join even when the current input cannot be deferred.
pub(crate) fn root_eligible<Child>(aggregate: &Aggregate<Child>) -> bool {
    aggregate.post_reduction.is_none()
        && !aggregate.aggregates.is_empty()
        && aggregate.has_plain_grouping_domain()
}

/// Produce one root-local aggregate alternative. The caller owns traversal;
/// recursively rewriting descendants here would duplicate region decisions.
pub fn optimize_plan(
    plan: OwnedLogicalPlan,
    bind_context: &BindContext,
) -> Result<(OwnedLogicalPlan, bool)> {
    rewrite_node(plan, bind_context)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExpressionDomain {
    Constant,
    Fact,
    Dimension,
    Mixed,
    Invalid,
}

struct DimensionDeferral {
    projection_depth: usize,
    partial_groups: Vec<Expression>,
    outer_groups: Vec<DeferredOuterGroup>,
    partial_aggregates: Vec<Expression>,
    merge_functions: Vec<paro_function::aggregate::AggregateFunction>,
    fact_conditions: Vec<FactConditionRewrite>,
}

enum DeferredOuterGroup {
    Partial { ordinal: usize },
    Dimension(Box<Expression>),
}

struct FactConditionRewrite {
    key_ordinal: usize,
    fact_on_left: bool,
}

struct DimensionRewriteInput {
    root_id: PlanNodeId,
    root_stats: NodeStats,
    aggregate: Aggregate,
    join: ComparisonJoin,
}

fn rewrite_node(
    plan: OwnedLogicalPlan,
    bind_context: &BindContext,
) -> Result<(OwnedLogicalPlan, bool)> {
    let plan = join_region::isolate_widest_dimension(plan, bind_context)?;
    let Some(witness) = recognize(&plan) else {
        return Ok((plan, false));
    };
    let input = match DimensionRewriteInput::from_plan(plan, witness.projection_depth) {
        Ok(input) => input,
        Err(plan) => return Ok((*plan, false)),
    };
    Ok((apply(input, witness, bind_context), true))
}

/// Prove that the aggregate can be split into fact-side partial states and a
/// post-join merge. The dimension is moved, not copied: unmatched fact states
/// disappear at the final inner join, so an earlier existence join would be
/// both redundant and more expensive.
fn recognize(plan: &OwnedLogicalPlan) -> Option<DimensionDeferral> {
    let LogicalOperator::Aggregate(aggregate) = &plan.operator else {
        return None;
    };
    if !root_eligible(aggregate) {
        return None;
    }
    let mut projections = Vec::new();
    let mut child = aggregate.child.as_ref();
    while let LogicalOperator::Projection(projection) = &child.operator {
        projections.push(projection);
        child = projection.child.as_ref();
    }
    let LogicalOperator::Join(Join::Comparison(join)) = &child.operator else {
        return None;
    };
    if join.join_type != JoinType::Inner
        || join.build_side_constraint != paro_planner::operator::JoinBuildSideConstraint::Either
        || join.conditions.is_empty()
        || join.mark_index.is_some()
        || !join.duplicate_eliminated_columns.is_empty()
        || join.delim_flipped
        || join
            .conditions
            .iter()
            .any(|condition| condition.comparison != JoinComparisonType::Equal)
    {
        return None;
    }
    if !matches!(
        &join.right.operator,
        LogicalOperator::Get(_) | LogicalOperator::CTERef(_)
    ) {
        return None;
    }

    let fact_bindings = join
        .left
        .get_column_bindings()
        .into_iter()
        .collect::<HashSet<_>>();
    let dimension_bindings = join
        .right
        .get_column_bindings()
        .into_iter()
        .collect::<HashSet<_>>();
    if fact_bindings.is_empty() || dimension_bindings.is_empty() {
        return None;
    }

    let mut fact_join_keys: Vec<Expression> = Vec::with_capacity(join.conditions.len());
    let mut fact_conditions = Vec::with_capacity(join.conditions.len());
    for condition in &join.conditions {
        let (fact_key, dimension_key, fact_on_left) = match (
            expression_domain(&condition.left, &fact_bindings, &dimension_bindings),
            expression_domain(&condition.right, &fact_bindings, &dimension_bindings),
        ) {
            (ExpressionDomain::Fact, ExpressionDomain::Dimension) => {
                (&condition.left, &condition.right, true)
            }
            (ExpressionDomain::Dimension, ExpressionDomain::Fact) => {
                (&condition.right, &condition.left, false)
            }
            _ => return None,
        };
        if !expression_is_movable(fact_key) || !expression_is_movable(dimension_key) {
            return None;
        }
        let key_ordinal = fact_join_keys
            .iter()
            .position(|key| key.equals(fact_key))
            .unwrap_or_else(|| {
                let ordinal = fact_join_keys.len();
                fact_join_keys.push(fact_key.clone());
                ordinal
            });
        fact_conditions.push(FactConditionRewrite {
            key_ordinal,
            fact_on_left,
        });
    }
    let expanded_groups = aggregate
        .groups
        .iter()
        .map(|expression| inline_projections(expression, &projections))
        .collect::<Option<Vec<_>>>()?;
    let mut partial_groups = fact_join_keys;
    let mut outer_groups = Vec::with_capacity(expanded_groups.len());
    let mut has_deferred_payload = false;
    for group in expanded_groups {
        match expression_domain(&group, &fact_bindings, &dimension_bindings) {
            ExpressionDomain::Fact | ExpressionDomain::Constant => {
                if !expression_is_movable(&group) {
                    return None;
                }
                let ordinal = partial_groups
                    .iter()
                    .position(|existing| existing.equals(&group))
                    .unwrap_or_else(|| {
                        let ordinal = partial_groups.len();
                        partial_groups.push(group);
                        ordinal
                    });
                outer_groups.push(DeferredOuterGroup::Partial { ordinal });
            }
            ExpressionDomain::Dimension => {
                if !expression_is_movable(&group) {
                    return None;
                }
                has_deferred_payload = true;
                outer_groups.push(DeferredOuterGroup::Dimension(Box::new(group)));
            }
            ExpressionDomain::Mixed | ExpressionDomain::Invalid => return None,
        }
    }
    if !has_deferred_payload {
        return None;
    }

    // A partial grouping over an already-covered key cannot be justified by
    // further key-domain reduction. Keep the existing state boundary instead
    // of recursively introducing equivalent aggregate/merge layers.
    if crate::statistics::unique_keys::expressions_cover_unique_key(
        &join.left,
        &partial_groups.iter().collect::<Vec<_>>(),
    ) {
        return None;
    }

    let mut partial_aggregates = Vec::with_capacity(aggregate.aggregates.len());
    let mut merge_functions = Vec::with_capacity(aggregate.aggregates.len());
    for expression in &aggregate.aggregates {
        let expanded = inline_projections(expression, &projections)?;
        let Expression::Aggregate(partial) = &expanded else {
            return None;
        };
        if partial.aggr_type != AggregateType::NonDistinct || !partial.order_bys.is_empty() {
            return None;
        }
        if partial
            .children
            .iter()
            .any(|child| !expression_is_movable(child))
            || partial
                .filter
                .as_deref()
                .is_some_and(|filter| !expression_is_movable(filter))
        {
            return None;
        }
        if !matches!(
            expression_domain(&expanded, &fact_bindings, &dimension_bindings),
            ExpressionDomain::Fact | ExpressionDomain::Constant
        ) {
            return None;
        }
        let merge = partial.function.partial_merge_function()?;
        if merge.arguments != [partial.return_type.clone()]
            || merge.return_type != partial.return_type
        {
            return None;
        }
        partial_aggregates.push(expanded);
        merge_functions.push(merge);
    }

    Some(DimensionDeferral {
        projection_depth: projections.len(),
        partial_groups,
        outer_groups,
        partial_aggregates,
        merge_functions,
        fact_conditions,
    })
}

impl DimensionRewriteInput {
    /// Consume the exact operator spine recognized above without cloning it.
    /// A future recognizer drift reconstructs and returns the original plan;
    /// the successful representation contains no fallible shape decisions.
    fn from_plan(
        plan: OwnedLogicalPlan,
        projection_depth: usize,
    ) -> std::result::Result<Self, Box<OwnedLogicalPlan>> {
        let (root_id, root_stats, operator) = plan.into_parts();
        let LogicalOperator::Aggregate(mut aggregate) = operator else {
            return Err(Box::new(OwnedLogicalPlan {
                id: root_id,
                stats: root_stats,
                operator,
            }));
        };
        let child = *std::mem::replace(
            &mut aggregate.child,
            Box::new(OwnedLogicalPlan::synthetic(LogicalOperator::DummyScan)),
        );
        match take_join_below_projections(child, projection_depth) {
            Ok(join) => Ok(Self {
                root_id,
                root_stats,
                aggregate: *aggregate,
                join,
            }),
            Err(child) => {
                aggregate.child = child;
                Err(Box::new(OwnedLogicalPlan {
                    id: root_id,
                    stats: root_stats,
                    operator: LogicalOperator::Aggregate(aggregate),
                }))
            }
        }
    }
}

fn take_join_below_projections(
    plan: OwnedLogicalPlan,
    projection_depth: usize,
) -> std::result::Result<ComparisonJoin, Box<OwnedLogicalPlan>> {
    let (id, stats, operator) = plan.into_parts();
    if projection_depth == 0 {
        return match operator {
            LogicalOperator::Join(Join::Comparison(join)) => Ok(join),
            operator => Err(Box::new(OwnedLogicalPlan {
                id,
                stats,
                operator,
            })),
        };
    }
    let LogicalOperator::Projection(mut projection) = operator else {
        return Err(Box::new(OwnedLogicalPlan {
            id,
            stats,
            operator,
        }));
    };
    match take_join_below_projections(*projection.child, projection_depth - 1) {
        Ok(join) => Ok(join),
        Err(child) => {
            projection.child = child;
            Err(Box::new(OwnedLogicalPlan {
                id,
                stats,
                operator: LogicalOperator::Projection(projection),
            }))
        }
    }
}

fn apply(
    input: DimensionRewriteInput,
    witness: DimensionDeferral,
    bind_context: &BindContext,
) -> OwnedLogicalPlan {
    let DimensionRewriteInput {
        root_id,
        root_stats,
        mut aggregate,
        join,
    } = input;
    let DimensionDeferral {
        projection_depth: _,
        partial_groups,
        outer_groups,
        partial_aggregates,
        merge_functions,
        fact_conditions,
    } = witness;
    let partial_group_index = bind_context.generate_table_index();
    let partial_aggregate_index = bind_context.generate_table_index();
    let partial_groupings_index = bind_context.generate_table_index();
    let mut final_conditions = join.conditions.clone();
    debug_assert_eq!(final_conditions.len(), fact_conditions.len());
    for (condition, rewrite) in final_conditions.iter_mut().zip(&fact_conditions) {
        let fact_expression = if rewrite.fact_on_left {
            &mut condition.left
        } else {
            &mut condition.right
        };
        debug_assert!(partial_groups
            .get(rewrite.key_ordinal)
            .is_some_and(|group| fact_expression.equals(group)));
        *fact_expression = Expression::ColumnRef(
            ColumnRefExpression::new(
                ColumnBinding::new(partial_group_index, rewrite.key_ordinal),
                fact_expression.return_type(),
            )
            .into(),
        );
    }
    let outer_groups = outer_groups
        .into_iter()
        .map(|group| match group {
            DeferredOuterGroup::Dimension(expression) => *expression,
            DeferredOuterGroup::Partial { ordinal } => Expression::ColumnRef(
                ColumnRefExpression::new(
                    ColumnBinding::new(partial_group_index, ordinal),
                    partial_groups[ordinal].return_type(),
                )
                .into(),
            ),
        })
        .collect::<Vec<_>>();
    let partial = OwnedLogicalPlan::new(
        bind_context,
        LogicalOperator::Aggregate(Box::new(Aggregate::new(
            partial_group_index,
            partial_aggregate_index,
            partial_groupings_index,
            *join.left,
            partial_groups,
            vec![],
            partial_aggregates,
            vec![],
        ))),
    );
    // Unmatched partial states vanish here. Duplicate dimension rows are
    // intentionally retained, and the final merge reproduces their SQL join
    // multiplicity without carrying descriptive payload through the hot
    // fact-side aggregate.
    for condition in &mut final_conditions {
        std::mem::swap(&mut condition.left, &mut condition.right);
        condition.comparison = condition.comparison.flip();
    }
    let final_join = ComparisonJoin::new(JoinType::Inner, *join.right, partial, final_conditions);

    let outer_aggregates = merge_functions
        .into_iter()
        .enumerate()
        .map(|(aggregate_index, merge)| {
            let return_type = merge.return_type.clone();
            Expression::Aggregate(
                AggregateExpression::new(
                    merge,
                    vec![Expression::ColumnRef(
                        ColumnRefExpression::new(
                            ColumnBinding::new(partial_aggregate_index, aggregate_index),
                            return_type.clone(),
                        )
                        .into(),
                    )],
                    return_type,
                )
                .into(),
            )
        })
        .collect();

    aggregate.child = Box::new(OwnedLogicalPlan::new(
        bind_context,
        LogicalOperator::Join(Join::Comparison(final_join)),
    ));
    aggregate.groups = outer_groups;
    aggregate.aggregates = outer_aggregates;
    aggregate.grouping_sets.clear();
    aggregate.recompute_returned_types();
    OwnedLogicalPlan {
        id: root_id,
        stats: root_stats,
        operator: LogicalOperator::Aggregate(Box::new(aggregate)),
    }
}

fn inline_projection<Child>(
    expression: &Expression,
    projection: &paro_planner::operator::Projection<Child>,
) -> Option<Expression> {
    let invalid = Cell::new(false);
    let result = expression.clone().replace_column_ref(&|column| {
        if column.depth != 0 {
            invalid.set(true);
            return None;
        }
        if column.binding.table_index != projection.table_index {
            return None;
        }
        let Some(replacement) = projection.expressions.get(column.binding.column_index) else {
            invalid.set(true);
            return None;
        };
        Some(replacement.clone())
    });
    (!invalid.get()).then_some(result)
}

fn expression_is_movable(expression: &Expression) -> bool {
    let properties = expression.evaluation_properties();
    properties.can_share_evaluation() && !properties.is_reorder_fence()
}

pub(crate) fn inline_projections<Child>(
    expression: &Expression,
    projections: &[&paro_planner::operator::Projection<Child>],
) -> Option<Expression> {
    projections
        .iter()
        .try_fold(expression.clone(), |expression, projection| {
            inline_projection(&expression, projection)
        })
}

fn expression_domain(
    expression: &Expression,
    fact: &HashSet<ColumnBinding>,
    dimension: &HashSet<ColumnBinding>,
) -> ExpressionDomain {
    let mut domain = ExpressionDomain::Constant;
    visit_expression(expression, &mut |expression| {
        let Expression::ColumnRef(column) = expression else {
            return;
        };
        let current = if column.depth != 0 {
            ExpressionDomain::Invalid
        } else if fact.contains(&column.binding) {
            ExpressionDomain::Fact
        } else if dimension.contains(&column.binding) {
            ExpressionDomain::Dimension
        } else {
            ExpressionDomain::Invalid
        };
        domain = combine_domains(domain, current);
    });
    domain
}

fn combine_domains(left: ExpressionDomain, right: ExpressionDomain) -> ExpressionDomain {
    use ExpressionDomain::{Constant, Dimension, Fact, Invalid, Mixed};
    match (left, right) {
        (Invalid, _) | (_, Invalid) => Invalid,
        (Mixed, _) | (_, Mixed) => Mixed,
        (Constant, domain) | (domain, Constant) => domain,
        (Fact, Fact) => Fact,
        (Dimension, Dimension) => Dimension,
        (Fact, Dimension) | (Dimension, Fact) => Mixed,
    }
}
