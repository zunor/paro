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
use std::collections::{HashMap, HashSet};

use paro_common::error::{self as paro_error, Result};
use paro_planner::binder::context::BindContext;
use paro_planner::binder::deep_copy::deep_copy_plan;
use paro_planner::expression::{
    AggregateExpression, AggregateType, ColumnRefExpression, Expression,
};
use paro_planner::operator::{
    Aggregate, ColumnBinding, Join, JoinComparisonType, JoinType, LogicalOperator, ProjectionMap,
};
use paro_planner::plan::LogicalPlan;

use crate::cost_model::CostModel;
use crate::expression::traversal::visit_expression;
use crate::statistics::unique_keys::{declared_unique_keys, NullRejectedKeyProof};

/// Rewrite direct aggregate/projection/dimension-join shapes after cost-based
/// join ordering has selected the dimension boundary.
pub fn optimize_plan(
    plan: LogicalPlan,
    bind_context: &BindContext,
    cost_model: &CostModel,
) -> Result<(LogicalPlan, bool)> {
    let mut changed = false;
    let plan = plan.try_map_post_order(|plan| {
        let (plan, node_changed) = rewrite_node(plan, bind_context, cost_model)?;
        changed |= node_changed;
        Ok(plan)
    })?;
    Ok((plan, changed))
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
    expanded_groups: Vec<Expression>,
    partial_aggregates: Vec<Expression>,
    merge_functions: Vec<paro_function::aggregate::AggregateFunction>,
    fact_group_indices: Vec<usize>,
    dimension_group_indices: HashSet<usize>,
    fact_join_keys: Vec<Expression>,
    fact_condition_left: Vec<bool>,
}

fn rewrite_node(
    plan: LogicalPlan,
    bind_context: &BindContext,
    cost_model: &CostModel,
) -> Result<(LogicalPlan, bool)> {
    let Some(witness) = recognize(&plan, cost_model) else {
        return Ok((plan, false));
    };
    apply(plan, witness, bind_context).map(|plan| (plan, true))
}

fn recognize(plan: &LogicalPlan, cost_model: &CostModel) -> Option<DimensionDeferral> {
    let LogicalOperator::Aggregate(aggregate) = &plan.operator else {
        return None;
    };
    if aggregate.post_reduction.is_some()
        || aggregate.aggregates.is_empty()
        || !aggregate.has_plain_grouping_domain()
    {
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
    let LogicalOperator::Get(dimension_get) = &join.right.operator else {
        return None;
    };

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
    let mut fact_condition_left = Vec::with_capacity(join.conditions.len());
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
        if !fact_key.evaluation_properties().can_share_evaluation()
            || !dimension_key.evaluation_properties().can_share_evaluation()
        {
            return None;
        }
        if !fact_join_keys.iter().any(|key| key.equals(fact_key)) {
            fact_join_keys.push(fact_key.clone());
        }
        fact_condition_left.push(fact_on_left);
    }
    let canonical_conditions = join
        .conditions
        .iter()
        .zip(fact_condition_left.iter().copied())
        .map(|(condition, fact_on_left)| {
            if fact_on_left {
                paro_planner::operator::JoinCondition::equality(
                    condition.left.clone(),
                    condition.right.clone(),
                )
            } else {
                paro_planner::operator::JoinCondition::equality(
                    condition.right.clone(),
                    condition.left.clone(),
                )
            }
        })
        .collect::<Vec<_>>();
    let null_rejection = NullRejectedKeyProof::from_equal_right_keys(&canonical_conditions)?;
    if !declared_unique_keys(dimension_get)
        .iter()
        .any(|key| key.is_unique_with_nulls_rejected(&null_rejection))
    {
        return None;
    }
    let expanded_groups = aggregate
        .groups
        .iter()
        .map(|expression| inline_projections(expression, &projections))
        .collect::<Option<Vec<_>>>()?;
    let mut fact_group_indices = Vec::new();
    let mut dimension_group_indices = HashSet::new();
    let mut deferred_payload_types = Vec::new();
    for (group_index, group) in expanded_groups.iter().enumerate() {
        match expression_domain(group, &fact_bindings, &dimension_bindings) {
            ExpressionDomain::Fact | ExpressionDomain::Constant => {
                if !group.evaluation_properties().can_share_evaluation() {
                    return None;
                }
                fact_group_indices.push(group_index);
            }
            ExpressionDomain::Dimension => {
                if !group.evaluation_properties().can_share_evaluation() {
                    return None;
                }
                deferred_payload_types.push(group.return_type());
                dimension_group_indices.insert(group_index);
            }
            ExpressionDomain::Mixed | ExpressionDomain::Invalid => return None,
        }
    }
    if dimension_group_indices.is_empty() {
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
            .any(|child| !child.evaluation_properties().can_share_evaluation())
            || partial
                .filter
                .as_deref()
                .is_some_and(|filter| !filter.evaluation_properties().can_share_evaluation())
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

    // Join-graph estimates can be globally selective even when the physical
    // carrier still processes a large fact stream. Use that estimate together
    // with a work upper bound that does not look through fact-side reduction
    // boundaries. This gate is cost-only; every semantic precondition above is
    // independent of it.
    let estimated_partial_input_rows = child.stats.estimated_cardinality?.expected;
    let carrier_rows =
        estimated_partial_input_rows.max(carrier_work_upper_bound(join.left.as_ref()));
    let group_estimate = plan.stats.estimated_cardinality?.expected.max(1);
    let dimension_rows = join.right.stats.estimated_cardinality?.expected;
    cost_model.dimension_deferral_benefit(
        carrier_rows,
        group_estimate,
        dimension_rows,
        deferred_payload_types,
    )?;

    Some(DimensionDeferral {
        expanded_groups,
        partial_aggregates,
        merge_functions,
        fact_group_indices,
        dimension_group_indices,
        fact_join_keys,
        fact_condition_left,
    })
}

fn apply(
    plan: LogicalPlan,
    witness: DimensionDeferral,
    bind_context: &BindContext,
) -> Result<LogicalPlan> {
    let LogicalPlan {
        id,
        stats,
        operator: LogicalOperator::Aggregate(mut aggregate),
    } = plan
    else {
        return Err(paro_error::internal(
            "dimension deferral witness does not own an Aggregate root",
        ));
    };
    let mut join_plan = *aggregate.child;
    loop {
        join_plan = match join_plan.operator {
            LogicalOperator::Projection(projection) => *projection.child,
            _ => break,
        };
    }
    let LogicalPlan {
        id: join_id,
        stats: join_stats,
        operator: LogicalOperator::Join(Join::Comparison(mut join)),
    } = join_plan
    else {
        return Err(paro_error::internal(
            "dimension deferral witness does not own a comparison join",
        ));
    };

    let partial_group_index = bind_context.generate_table_index();
    let partial_aggregate_index = bind_context.generate_table_index();
    let partial_groupings_index = bind_context.generate_table_index();
    let mut partial_groups = witness.fact_join_keys.clone();
    let mut outer_fact_group_ordinals = Vec::with_capacity(witness.fact_group_indices.len());
    for &group_index in &witness.fact_group_indices {
        let group = witness.expanded_groups[group_index].clone();
        let ordinal = partial_groups
            .iter()
            .position(|existing| existing.equals(&group))
            .unwrap_or_else(|| {
                let ordinal = partial_groups.len();
                partial_groups.push(group);
                ordinal
            });
        outer_fact_group_ordinals.push((group_index, ordinal));
    }

    let mut final_conditions = join.conditions.clone();
    let old_dimension_bindings = join.right.get_column_bindings();
    let final_dimension = deep_copy_plan(join.right.as_ref(), bind_context.shared().as_ref());
    let new_dimension_bindings = final_dimension.get_column_bindings();
    if old_dimension_bindings.len() != new_dimension_bindings.len() {
        return Err(paro_error::internal(
            "dimension deferral copy changed the dimension output arity",
        ));
    }
    let dimension_binding_map = old_dimension_bindings
        .into_iter()
        .zip(new_dimension_bindings)
        .collect::<HashMap<_, _>>();
    if dimension_binding_map.iter().any(|(old, new)| old == new) {
        return Err(paro_error::internal(
            "dimension deferral copy reused the original binding namespace",
        ));
    }
    for condition in &mut final_conditions {
        condition.left = remap_bindings(condition.left.clone(), &dimension_binding_map);
        condition.right = remap_bindings(condition.right.clone(), &dimension_binding_map);
    }
    // Preserve the original inner join below the partial aggregate so fact
    // expressions retain their SQL error/evaluation domain. The declared key
    // proves that filtering join cannot multiply facts; its descriptive
    // payload is omitted until the compact final join.
    join.left_projection_map = ProjectionMap::all();
    join.right_projection_map = ProjectionMap::none();
    let filtered_fact = LogicalPlan {
        id: join_id,
        stats: join_stats.clone(),
        operator: LogicalOperator::Join(Join::Comparison(join)),
    };
    let partial = LogicalPlan::new(
        bind_context,
        LogicalOperator::Aggregate(Aggregate::new(
            partial_group_index,
            partial_aggregate_index,
            partial_groupings_index,
            filtered_fact,
            partial_groups.clone(),
            vec![],
            witness.partial_aggregates,
            vec![],
        )),
    );
    let mut final_join = paro_planner::operator::ComparisonJoin::new(
        JoinType::Inner,
        partial,
        final_dimension,
        final_conditions,
    );

    for (condition, fact_on_left) in final_join
        .conditions
        .iter_mut()
        .zip(witness.fact_condition_left.iter().copied())
    {
        let fact_expression = if fact_on_left {
            &mut condition.left
        } else {
            &mut condition.right
        };
        let key_ordinal = witness
            .fact_join_keys
            .iter()
            .position(|key| key.equals(fact_expression))
            .ok_or_else(|| {
                paro_error::internal("dimension deferral witness did not map a final equality key")
            })?;
        *fact_expression = Expression::ColumnRef(ColumnRefExpression::new(
            ColumnBinding::new(partial_group_index, key_ordinal),
            fact_expression.return_type(),
        ));
    }

    let mut outer_groups = Vec::with_capacity(witness.expanded_groups.len());
    for (group_index, group) in witness.expanded_groups.into_iter().enumerate() {
        if witness.dimension_group_indices.contains(&group_index) {
            outer_groups.push(remap_bindings(group, &dimension_binding_map));
            continue;
        }
        let ordinal = outer_fact_group_ordinals
            .iter()
            .find_map(|(old, ordinal)| (*old == group_index).then_some(*ordinal))
            .ok_or_else(|| {
                paro_error::internal("dimension deferral witness did not map a fact group")
            })?;
        outer_groups.push(Expression::ColumnRef(ColumnRefExpression::new(
            ColumnBinding::new(partial_group_index, ordinal),
            partial_groups[ordinal].return_type(),
        )));
    }
    let outer_aggregates = witness
        .merge_functions
        .into_iter()
        .enumerate()
        .map(|(aggregate_index, merge)| {
            let return_type = merge.return_type.clone();
            Expression::Aggregate(AggregateExpression::new(
                merge,
                vec![Expression::ColumnRef(ColumnRefExpression::new(
                    ColumnBinding::new(partial_aggregate_index, aggregate_index),
                    return_type.clone(),
                ))],
                return_type,
            ))
        })
        .collect();

    aggregate.child = Box::new(LogicalPlan::new(
        bind_context,
        LogicalOperator::Join(Join::Comparison(final_join)),
    ));
    aggregate.groups = outer_groups;
    aggregate.aggregates = outer_aggregates;
    aggregate.grouping_sets.clear();
    aggregate.recompute_returned_types();
    Ok(LogicalPlan {
        id,
        stats,
        operator: LogicalOperator::Aggregate(aggregate),
    })
}

fn inline_projection(
    expression: &Expression,
    projection: &paro_planner::operator::Projection,
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

fn remap_bindings(
    expression: Expression,
    bindings: &HashMap<ColumnBinding, ColumnBinding>,
) -> Expression {
    expression.replace_column_ref(&|column| {
        bindings.get(&column.binding).copied().map(|binding| {
            Expression::ColumnRef(ColumnRefExpression {
                binding,
                depth: column.depth,
                return_type: column.return_type.clone(),
            })
        })
    })
}

fn inline_projections(
    expression: &Expression,
    projections: &[&paro_planner::operator::Projection],
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

/// Conservative carrier-work bound that stops at an already-estimated row
/// reduction. Raw leaf cardinality would ignore a selective fact Filter, while
/// a join-graph output estimate can include reductions that physical runtime
/// filters have not applied before this carrier is materialized.
fn carrier_work_upper_bound(plan: &LogicalPlan) -> u64 {
    if matches!(
        plan.operator,
        LogicalOperator::Filter(_)
            | LogicalOperator::Limit(_)
            | LogicalOperator::TopN(_)
            | LogicalOperator::Aggregate(_)
            | LogicalOperator::Distinct(_)
            | LogicalOperator::EmptyResult(_)
    ) {
        return plan
            .stats
            .estimated_cardinality
            .map_or(0, |estimate| estimate.expected);
    }
    let children = plan.children();
    if children.is_empty() {
        return plan
            .stats
            .estimated_cardinality
            .map_or(0, |estimate| estimate.expected);
    }
    children
        .into_iter()
        .map(carrier_work_upper_bound)
        .max()
        .unwrap_or(0)
}
