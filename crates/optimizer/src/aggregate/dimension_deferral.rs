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

use paro_common::error::Result;
use paro_planner::binder::context::BindContext;
use paro_planner::binder::deep_copy::deep_copy_plan;
use paro_planner::expression::{
    AggregateExpression, AggregateType, ColumnRefExpression, Expression,
};
use paro_planner::operator::{
    Aggregate, ColumnBinding, ComparisonJoin, Join, JoinComparisonType, JoinType, LogicalOperator,
    ProjectionMap,
};
use paro_planner::plan::{CardinalityProvenance, LogicalPlan, NodeStats, PlanNodeId};

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
    projection_depth: usize,
    dimension: PreparedDimension,
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

struct PreparedDimension {
    plan: LogicalPlan,
    binding_map: HashMap<ColumnBinding, ColumnBinding>,
}

struct DimensionRewriteInput {
    root_id: PlanNodeId,
    root_stats: NodeStats,
    aggregate: Aggregate,
    join_id: PlanNodeId,
    join_stats: NodeStats,
    join: ComparisonJoin,
}

fn rewrite_node(
    plan: LogicalPlan,
    bind_context: &BindContext,
    cost_model: &CostModel,
) -> Result<(LogicalPlan, bool)> {
    let Some(witness) = recognize_and_prepare(&plan, bind_context, cost_model) else {
        return Ok((plan, false));
    };
    let input = match DimensionRewriteInput::from_plan(plan, witness.projection_depth) {
        Ok(input) => input,
        Err(plan) => return Ok((*plan, false)),
    };
    Ok((apply(input, witness, bind_context), true))
}

/// Prove the rewrite and prepare its independently-bound dimension copy only
/// after the cost gate accepts it. The resulting witness owns every resource
/// needed by the total mutation phase below.
fn recognize_and_prepare(
    plan: &LogicalPlan,
    bind_context: &BindContext,
    cost_model: &CostModel,
) -> Option<DimensionDeferral> {
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
        if !fact_key.evaluation_properties().can_share_evaluation()
            || !dimension_key.evaluation_properties().can_share_evaluation()
        {
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
    let canonical_conditions = join
        .conditions
        .iter()
        .zip(&fact_conditions)
        .map(|(condition, rewrite)| {
            if rewrite.fact_on_left {
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
    let fact_join_key_count = fact_join_keys.len();
    let mut partial_groups = fact_join_keys;
    let mut outer_groups = Vec::with_capacity(expanded_groups.len());
    let mut deferred_payload_types = Vec::new();
    for group in expanded_groups {
        match expression_domain(&group, &fact_bindings, &dimension_bindings) {
            ExpressionDomain::Fact | ExpressionDomain::Constant => {
                if !group.evaluation_properties().can_share_evaluation() {
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
                if !group.evaluation_properties().can_share_evaluation() {
                    return None;
                }
                deferred_payload_types.push(group.return_type());
                outer_groups.push(DeferredOuterGroup::Dimension(Box::new(group)));
            }
            ExpressionDomain::Mixed | ExpressionDomain::Invalid => return None,
        }
    }
    if deferred_payload_types.is_empty() {
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
    // Every final (payload, fact-group) tuple can represent up to one partial
    // per dimension key because SQL payloads need not be unique. Without
    // cross-column NDV statistics, use that conservative upper bound and cap
    // it at the carrier cardinality. This deliberately overprices, rather than
    // underprices, the compact join and final merge when fact groups exist.
    let partial_group_rows =
        partial_group_work_upper_bound(carrier_rows, group_estimate, dimension_rows);
    let key_types = partial_groups
        .iter()
        .take(fact_join_key_count)
        .map(Expression::return_type)
        .collect::<Vec<_>>();
    let aggregate_state_types = partial_aggregates
        .iter()
        .map(Expression::return_type)
        .collect::<Vec<_>>();
    if !cost_model.dimension_deferral_is_cheaper(
        carrier_rows,
        partial_group_rows,
        dimension_rows,
        &key_types,
        &deferred_payload_types,
        &aggregate_state_types,
    ) {
        return None;
    }

    let old_bindings = join.right.get_column_bindings();
    let copied_dimension = deep_copy_plan(join.right.as_ref(), bind_context.shared().as_ref());
    let new_bindings = copied_dimension.get_column_bindings();
    debug_assert_eq!(old_bindings.len(), new_bindings.len());
    let binding_map = old_bindings
        .into_iter()
        .zip(new_bindings)
        .collect::<HashMap<_, _>>();
    debug_assert!(binding_map.iter().all(|(old, new)| old != new));

    Some(DimensionDeferral {
        projection_depth: projections.len(),
        dimension: PreparedDimension {
            plan: copied_dimension,
            binding_map,
        },
        partial_groups,
        outer_groups,
        partial_aggregates,
        merge_functions,
        fact_conditions,
    })
}

pub(super) fn partial_group_work_upper_bound(
    carrier_rows: u64,
    final_group_rows: u64,
    dimension_rows: u64,
) -> u64 {
    carrier_rows.min(final_group_rows.saturating_mul(dimension_rows))
}

impl DimensionRewriteInput {
    /// Consume the exact operator spine recognized above without cloning it.
    /// A future recognizer drift reconstructs and returns the original plan;
    /// the successful representation contains no fallible shape decisions.
    fn from_plan(
        plan: LogicalPlan,
        projection_depth: usize,
    ) -> std::result::Result<Self, Box<LogicalPlan>> {
        let LogicalPlan {
            id: root_id,
            stats: root_stats,
            operator,
        } = plan;
        let LogicalOperator::Aggregate(mut aggregate) = operator else {
            return Err(Box::new(LogicalPlan {
                id: root_id,
                stats: root_stats,
                operator,
            }));
        };
        let child = *std::mem::replace(
            &mut aggregate.child,
            Box::new(LogicalPlan::synthetic(LogicalOperator::DummyScan)),
        );
        match take_join_below_projections(child, projection_depth) {
            Ok((join_id, join_stats, join)) => Ok(Self {
                root_id,
                root_stats,
                aggregate,
                join_id,
                join_stats,
                join,
            }),
            Err(child) => {
                aggregate.child = child;
                Err(Box::new(LogicalPlan {
                    id: root_id,
                    stats: root_stats,
                    operator: LogicalOperator::Aggregate(aggregate),
                }))
            }
        }
    }
}

fn take_join_below_projections(
    plan: LogicalPlan,
    projection_depth: usize,
) -> std::result::Result<(PlanNodeId, NodeStats, ComparisonJoin), Box<LogicalPlan>> {
    let LogicalPlan {
        id,
        stats,
        operator,
    } = plan;
    if projection_depth == 0 {
        return match operator {
            LogicalOperator::Join(Join::Comparison(join)) => Ok((id, stats, join)),
            operator => Err(Box::new(LogicalPlan {
                id,
                stats,
                operator,
            })),
        };
    }
    let LogicalOperator::Projection(mut projection) = operator else {
        return Err(Box::new(LogicalPlan {
            id,
            stats,
            operator,
        }));
    };
    match take_join_below_projections(*projection.child, projection_depth - 1) {
        Ok(join) => Ok(join),
        Err(child) => {
            projection.child = child;
            Err(Box::new(LogicalPlan {
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
) -> LogicalPlan {
    let DimensionRewriteInput {
        root_id,
        root_stats,
        mut aggregate,
        join_id,
        join_stats,
        mut join,
    } = input;
    let DimensionDeferral {
        projection_depth: _,
        dimension,
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
    for condition in &mut final_conditions {
        condition.left = remap_bindings(condition.left.clone(), &dimension.binding_map);
        condition.right = remap_bindings(condition.right.clone(), &dimension.binding_map);
    }
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
        *fact_expression = Expression::ColumnRef(ColumnRefExpression::new(
            ColumnBinding::new(partial_group_index, rewrite.key_ordinal),
            fact_expression.return_type(),
        ));
    }
    let outer_groups = outer_groups
        .into_iter()
        .map(|group| match group {
            DeferredOuterGroup::Dimension(expression) => {
                remap_bindings(*expression, &dimension.binding_map)
            }
            DeferredOuterGroup::Partial { ordinal } => {
                Expression::ColumnRef(ColumnRefExpression::new(
                    ColumnBinding::new(partial_group_index, ordinal),
                    partial_groups[ordinal].return_type(),
                ))
            }
        })
        .collect();
    // Preserve the original inner join below the partial aggregate so fact
    // expressions retain their SQL error/evaluation domain. The declared key
    // proves that filtering join cannot multiply facts; its descriptive
    // payload is omitted until the compact final join.
    join.left_projection_map = ProjectionMap::all();
    join.right_projection_map = ProjectionMap::none();
    let filtered_fact = LogicalPlan {
        id: join_id,
        stats: join_stats,
        operator: LogicalOperator::Join(Join::Comparison(join)),
    };
    let partial = LogicalPlan::new(
        bind_context,
        LogicalOperator::Aggregate(Aggregate::new(
            partial_group_index,
            partial_aggregate_index,
            partial_groupings_index,
            filtered_fact,
            partial_groups,
            vec![],
            partial_aggregates,
            vec![],
        )),
    );
    let final_join =
        ComparisonJoin::new(JoinType::Inner, partial, dimension.plan, final_conditions);

    let outer_aggregates = merge_functions
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
    LogicalPlan {
        id: root_id,
        stats: root_stats,
        operator: LogicalOperator::Aggregate(aggregate),
    }
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

/// Conservative carrier-work bound that trusts physically materialized
/// operator boundaries and looks through join-graph joins. A reordered join's
/// estimate can include runtime-filter reductions not applied before its
/// carrier is materialized. A Filter is different: even when join ordering
/// assigned its estimate, that physical operator executes the reduction before
/// the carrier reaches the aggregate.
pub(super) fn carrier_work_upper_bound(plan: &LogicalPlan) -> u64 {
    if plan.stats.cardinality_provenance != CardinalityProvenance::JoinGraph
        || !matches!(plan.operator, LogicalOperator::Join(_))
    {
        return plan
            .stats
            .estimated_cardinality
            .map_or(0, |estimate| estimate.expected);
    }
    plan.children()
        .into_iter()
        .map(carrier_work_upper_bound)
        .max()
        .unwrap_or(0)
}
