// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Fuse sibling scalar aggregates over one alpha-equivalent filtered input.
//!
//! Uncorrelated scalar subqueries are represented as one-row branches joined
//! into their consumer. When several branches aggregate the same deterministic
//! filtered relation, executing each branch independently repeats the entire
//! input. This pass builds an optional relational candidate whose sibling
//! aggregates share one scan and one filter while retaining independent
//! aggregate states and scalar result expressions.
//!
//! The proof is deliberately narrow: only the canonical scalar wrapper over a
//! plain ungrouped aggregate is admitted, cross-product regions must be
//! unconstrained, scan columns are matched by stable physical source, and
//! filter equality uses the proof-grade alpha-equivalence implementation.

use std::collections::{HashMap, HashSet};

use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_function::aggregate::distributive::count::get_count_star_function;
use paro_function::aggregate::distributive::first_last::get_first_function;
use paro_planner::binder::context::BindContext;
use paro_planner::expression::{
    AggregateExpression, AggregateType, ColumnRefExpression, Expression, ExpressionIterator,
    ExpressionVisitDecision, OperatorType,
};
use paro_planner::operator::{
    Aggregate, ColumnBinding, CrossProduct, Filter, Get, Join, JoinBuildSideConstraint,
    LogicalOperator, Projection,
};
use paro_planner::plan::OwnedLogicalPlan;

use crate::aggregate::post_reduction::alpha::AlphaBindings;
use crate::aggregate::semantic_kernels::aggregate_kernels_equal;

pub fn optimize_plan(
    plan: OwnedLogicalPlan,
    bind_context: &BindContext,
) -> Result<OwnedLogicalPlan> {
    optimize_plan_with_change(plan, bind_context).map(|(plan, _)| plan)
}

/// Clone-free structural prefilter for optional-candidate construction. Exact
/// source and expression equivalence remains the responsibility of
/// [`recognize`]; a false positive here only creates a declined candidate.
pub(crate) fn contains_candidate_root(plan: &OwnedLogicalPlan) -> bool {
    let mut pending = vec![plan];
    while let Some(candidate) = pending.pop() {
        if let LogicalOperator::Projection(projection) = &candidate.operator {
            let mut leaves = Vec::new();
            if collect_cross_leaves(projection.child.as_ref(), &mut leaves).is_some()
                && leaves
                    .into_iter()
                    .filter(|leaf| has_scalar_branch_shape(leaf))
                    .take(2)
                    .count()
                    == 2
            {
                return true;
            }
        }
        pending.extend(candidate.children());
    }
    false
}

fn has_scalar_branch_shape(plan: &OwnedLogicalPlan) -> bool {
    let LogicalOperator::Projection(wrapper) = &plan.operator else {
        return false;
    };
    let LogicalOperator::Aggregate(wrapper) = &wrapper.child.operator else {
        return false;
    };
    let LogicalOperator::Projection(scalar) = &wrapper.child.operator else {
        return false;
    };
    let LogicalOperator::Aggregate(reduction) = &scalar.child.operator else {
        return false;
    };
    match &reduction.child.operator {
        LogicalOperator::Get(_) => true,
        LogicalOperator::Filter(filter) => {
            matches!(filter.child.operator, LogicalOperator::Get(_))
        }
        _ => false,
    }
}

pub fn optimize_plan_with_change(
    plan: OwnedLogicalPlan,
    bind_context: &BindContext,
) -> Result<(OwnedLogicalPlan, bool)> {
    let mut changed = false;
    let plan = plan.try_map_post_order(|plan| {
        let Some(witness) = recognize(&plan) else {
            return Ok(plan);
        };
        changed = true;
        apply_rewrite(plan, witness, bind_context)
    })?;
    Ok((plan, changed))
}

#[derive(Clone)]
struct ScalarBranch {
    leaf_index: usize,
    wrapper_binding: ColumnBinding,
    wrapper_type: LogicalType,
    scalar_expression: Expression,
    scalar_source_binding: ColumnBinding,
    aggregate: AggregateExpression,
    filter_expressions: Vec<Expression>,
    source_get: Get,
}

struct FusionGroup {
    members: Vec<usize>,
    fused_get: Get,
    filter_expressions: Vec<Expression>,
}

struct FusionWitness {
    branches: Vec<ScalarBranch>,
    groups: Vec<FusionGroup>,
}

fn recognize(plan: &OwnedLogicalPlan) -> Option<FusionWitness> {
    let LogicalOperator::Projection(projection) = &plan.operator else {
        return None;
    };
    let mut leaves = Vec::new();
    collect_cross_leaves(projection.child.as_ref(), &mut leaves)?;
    if leaves.len() < 2 {
        return None;
    }

    let branches = leaves
        .iter()
        .enumerate()
        .filter_map(|(leaf_index, leaf)| peel_scalar_branch(leaf, leaf_index))
        .collect::<Vec<_>>();
    if branches.len() < 2 {
        return None;
    }

    let mut groups: Vec<FusionGroup> = Vec::new();
    for (branch_index, branch) in branches.iter().enumerate() {
        let mut matched = false;
        for group in &mut groups {
            let Some(merged_get) = merged_get(&group.fused_get, &branch.source_get) else {
                continue;
            };
            let Some(bindings) = AlphaBindings::match_gets(&merged_get, &branch.source_get) else {
                continue;
            };
            if !filters_equal(
                &group.filter_expressions,
                &branch.filter_expressions,
                &bindings,
            ) {
                continue;
            }
            group.fused_get = merged_get;
            group.members.push(branch_index);
            matched = true;
            break;
        }
        if !matched {
            groups.push(FusionGroup {
                members: vec![branch_index],
                fused_get: branch.source_get.clone(),
                filter_expressions: branch.filter_expressions.clone(),
            });
        }
    }
    groups.retain(|group| group.members.len() >= 2);
    (!groups.is_empty()).then_some(FusionWitness { branches, groups })
}

fn collect_cross_leaves<'a>(
    plan: &'a OwnedLogicalPlan,
    leaves: &mut Vec<&'a OwnedLogicalPlan>,
) -> Option<()> {
    let mut pending = vec![plan];
    while let Some(plan) = pending.pop() {
        if let LogicalOperator::Join(Join::Cross(join)) = &plan.operator {
            if join.build_side_constraint != JoinBuildSideConstraint::Either {
                return None;
            }
            pending.push(join.right.as_ref());
            pending.push(join.left.as_ref());
        } else {
            leaves.push(plan);
        }
    }
    Some(())
}

fn peel_scalar_branch(plan: &OwnedLogicalPlan, leaf_index: usize) -> Option<ScalarBranch> {
    let LogicalOperator::Projection(wrapper_projection) = &plan.operator else {
        return None;
    };
    if wrapper_projection.expressions.len() != 1
        || wrapper_projection.returned_types.len() != 1
        || wrapper_projection.visible_names.len() != 1
    {
        return None;
    }
    let Expression::Operator(checked) = &wrapper_projection.expressions[0] else {
        return None;
    };
    if checked.operator_type != OperatorType::ErrorIfMultipleRows || checked.children.len() != 2 {
        return None;
    }

    let LogicalOperator::Aggregate(wrapper) = &wrapper_projection.child.operator else {
        return None;
    };
    if !plain_scalar_wrapper(wrapper) {
        return None;
    }
    let [Expression::Aggregate(first), Expression::Aggregate(_)] = wrapper.aggregates.as_slice()
    else {
        return None;
    };
    let [Expression::ColumnRef(first_output), Expression::ColumnRef(count_output)] =
        checked.children.as_slice()
    else {
        return None;
    };
    if !is_column(
        first_output,
        ColumnBinding::new(wrapper.aggregate_index, 0),
        &first.return_type,
    ) || !is_column(
        count_output,
        ColumnBinding::new(wrapper.aggregate_index, 1),
        &LogicalType::BigInt,
    ) || checked.return_type != first.return_type
    {
        return None;
    }

    let LogicalOperator::Projection(scalar_projection) = &wrapper.child.operator else {
        return None;
    };
    if scalar_projection.expressions.len() != 1
        || scalar_projection.returned_types.len() != 1
        || scalar_projection.visible_names.len() != 1
    {
        return None;
    }
    let scalar_expression = &scalar_projection.expressions[0];
    if !is_movable(scalar_expression)
        || scalar_expression.return_type() != first.return_type
        || !is_column_expression(
            &first.children[0],
            ColumnBinding::new(scalar_projection.table_index, 0),
            &scalar_expression.return_type(),
        )
    {
        return None;
    }

    let LogicalOperator::Aggregate(reduction) = &scalar_projection.child.operator else {
        return None;
    };
    if !plain_ungrouped_aggregate(reduction)
        || !expression_uses_only_binding(
            scalar_expression,
            ColumnBinding::new(reduction.aggregate_index, 0),
        )
    {
        return None;
    }
    let Expression::Aggregate(aggregate) = &reduction.aggregates[0] else {
        return None;
    };
    if aggregate.aggr_type != AggregateType::NonDistinct
        || !aggregate.order_bys.is_empty()
        || !is_movable(&reduction.aggregates[0])
    {
        return None;
    }

    let (filter_expressions, source_get) = match &reduction.child.operator {
        LogicalOperator::Filter(filter) => {
            let LogicalOperator::Get(get) = &filter.child.operator else {
                return None;
            };
            if !filter.expressions.iter().all(is_movable) {
                return None;
            }
            (filter.expressions.clone(), get)
        }
        LogicalOperator::Get(get) => (Vec::new(), get),
        _ => return None,
    };
    if source_get.table.is_none()
        || source_get.scan_order.is_some()
        || !source_get.runtime_filter_expressions.is_empty()
    {
        return None;
    }

    Some(ScalarBranch {
        leaf_index,
        wrapper_binding: ColumnBinding::new(wrapper_projection.table_index, 0),
        wrapper_type: checked.return_type.clone(),
        scalar_expression: scalar_expression.clone(),
        scalar_source_binding: ColumnBinding::new(reduction.aggregate_index, 0),
        aggregate: aggregate.clone(),
        filter_expressions,
        source_get: *source_get.clone(),
    })
}

fn plain_scalar_wrapper(wrapper: &Aggregate) -> bool {
    if !wrapper.groups.is_empty()
        || !wrapper.grouping_sets.is_empty()
        || !wrapper.grouping_functions.is_empty()
        || wrapper.aggregates.len() != 2
        || wrapper.post_reduction.is_some()
    {
        return false;
    }
    let [Expression::Aggregate(first), Expression::Aggregate(count)] =
        wrapper.aggregates.as_slice()
    else {
        return false;
    };
    let Ok((canonical_first, _)) =
        get_first_function().bind(std::slice::from_ref(&first.return_type))
    else {
        return false;
    };
    let canonical_first =
        AggregateExpression::new(canonical_first, Vec::new(), first.return_type.clone());
    let canonical_count =
        AggregateExpression::new(get_count_star_function(), Vec::new(), LogicalType::BigInt);
    first.aggr_type == AggregateType::NonDistinct
        && first.filter.is_none()
        && first.order_bys.is_empty()
        && first.children.len() == 1
        && aggregate_kernels_equal(first, &canonical_first)
        && count.aggr_type == AggregateType::NonDistinct
        && count.filter.is_none()
        && count.order_bys.is_empty()
        && count.children.is_empty()
        && aggregate_kernels_equal(count, &canonical_count)
}

fn plain_ungrouped_aggregate(aggregate: &Aggregate) -> bool {
    aggregate.groups.is_empty()
        && aggregate.grouping_sets.is_empty()
        && aggregate.grouping_functions.is_empty()
        && aggregate.aggregates.len() == 1
        && aggregate.post_reduction.is_none()
}

fn merged_get(grouped: &Get, scalar: &Get) -> Option<Get> {
    if grouped.column_sources.len() != grouped.column_types.len()
        || grouped.column_sources.len() != grouped.returned_types.len()
        || grouped.column_sources.len() != grouped.names.len()
        || scalar.column_sources.len() != scalar.column_types.len()
        || scalar.column_sources.len() != scalar.returned_types.len()
        || scalar.column_sources.len() != scalar.names.len()
        || has_duplicate_sources(grouped)
        || has_duplicate_sources(scalar)
    {
        return None;
    }
    let mut merged = grouped.clone();
    for index in 0..scalar.column_sources.len() {
        let source = scalar.column_sources[index];
        if let Some(existing) = merged
            .column_sources
            .iter()
            .position(|candidate| *candidate == source)
        {
            if merged.column_types[existing] != scalar.column_types[index]
                || merged.returned_types[existing] != scalar.returned_types[index]
            {
                return None;
            }
            continue;
        }
        merged.append_output(
            scalar.names[index].clone(),
            scalar.returned_types[index].clone(),
            scalar.column_types[index].clone(),
            source,
        );
    }
    AlphaBindings::match_gets(&merged, scalar).map(|_| merged)
}

fn has_duplicate_sources(get: &Get) -> bool {
    get.column_sources
        .iter()
        .enumerate()
        .any(|(index, source)| get.column_sources[..index].contains(source))
}

fn filters_equal(grouped: &[Expression], scalar: &[Expression], bindings: &AlphaBindings) -> bool {
    if grouped.len() != scalar.len() {
        return false;
    }
    let mut matched = vec![false; scalar.len()];
    grouped.iter().all(|grouped_expression| {
        let Some(index) = scalar
            .iter()
            .enumerate()
            .position(|(index, scalar_expression)| {
                !matched[index] && bindings.expressions_equal(grouped_expression, scalar_expression)
            })
        else {
            return false;
        };
        matched[index] = true;
        true
    })
}

fn apply_rewrite(
    plan: OwnedLogicalPlan,
    witness: FusionWitness,
    bind_context: &BindContext,
) -> Result<OwnedLogicalPlan> {
    let (id, stats, operator) = plan.into_parts();
    let LogicalOperator::Projection(mut projection) = operator else {
        return Err(paro_error::internal(
            "scalar aggregate fusion witness no longer points to a projection",
        ));
    };
    let original_child = std::mem::replace(
        &mut *projection.child,
        OwnedLogicalPlan::synthetic(LogicalOperator::DummyScan),
    );
    let mut leaves = Vec::new();
    flatten_cross_owned(original_child, &mut leaves)?;

    let FusionWitness { branches, groups } = witness;
    let mut member_to_leader = HashMap::new();
    let mut leaders = HashMap::new();
    let mut replacements = HashMap::new();
    let mut eliminated_bindings = HashSet::new();
    for group in groups {
        let members = group.members.clone();
        let leader = branches[members[0]].leaf_index;
        let (fused, group_replacements) = build_fused_group(group, &branches, bind_context)?;
        for member in members {
            member_to_leader.insert(branches[member].leaf_index, leader);
        }
        for (old, new) in group_replacements {
            eliminated_bindings.insert(old);
            replacements.insert(old, new);
        }
        leaders.insert(leader, fused);
    }

    let mut retained = Vec::with_capacity(leaves.len());
    for (leaf_index, leaf) in leaves.into_iter().enumerate() {
        let Some(leader) = member_to_leader.get(&leaf_index).copied() else {
            retained.push(leaf);
            continue;
        };
        if leaf_index == leader {
            retained.push(leaders.remove(&leader).ok_or_else(|| {
                paro_error::internal("scalar aggregate fusion lost its leader plan")
            })?);
        }
    }
    let child = rebuild_cross(retained, bind_context)?;
    projection.child = Box::new(child);
    for expression in &mut projection.expressions {
        *expression = expression.clone().replace_column_ref(&|column| {
            (column.depth == 0)
                .then(|| replacements.get(&column.binding).copied())
                .flatten()
                .map(|binding| {
                    Expression::ColumnRef(ColumnRefExpression::new(
                        binding,
                        column.return_type.clone(),
                    ))
                })
        });
    }
    if projection
        .expressions
        .iter()
        .any(|expression| expression_mentions_any(expression, &eliminated_bindings))
    {
        return Err(paro_error::internal(
            "scalar aggregate fusion left a reference to an eliminated branch",
        ));
    }
    Ok(OwnedLogicalPlan {
        id,
        stats,
        operator: LogicalOperator::Projection(projection),
    })
}

fn flatten_cross_owned(plan: OwnedLogicalPlan, leaves: &mut Vec<OwnedLogicalPlan>) -> Result<()> {
    let mut pending = vec![plan];
    while let Some(plan) = pending.pop() {
        if !matches!(plan.operator, LogicalOperator::Join(Join::Cross(_))) {
            leaves.push(plan);
            continue;
        }
        let (_, _, operator) = plan.into_parts();
        let LogicalOperator::Join(Join::Cross(join)) = operator else {
            return Err(paro_error::internal(
                "scalar aggregate fusion cross-product witness changed before ownership transfer",
            ));
        };
        if join.build_side_constraint != JoinBuildSideConstraint::Either {
            return Err(paro_error::internal(
                "scalar aggregate fusion encountered a constrained cross product",
            ));
        }
        pending.push(*join.right);
        pending.push(*join.left);
    }
    Ok(())
}

fn build_fused_group(
    group: FusionGroup,
    branches: &[ScalarBranch],
    bind_context: &BindContext,
) -> Result<(OwnedLogicalPlan, Vec<(ColumnBinding, ColumnBinding)>)> {
    let get_plan = OwnedLogicalPlan::new(
        bind_context,
        LogicalOperator::Get(Box::new(group.fused_get.clone())),
    );
    let source = if group.filter_expressions.is_empty() {
        get_plan
    } else {
        OwnedLogicalPlan::new(
            bind_context,
            LogicalOperator::Filter(Filter::new(get_plan, group.filter_expressions)),
        )
    };
    let aggregate_index = bind_context.generate_table_index();
    let mut aggregates = Vec::with_capacity(group.members.len());
    let mut scalar_expressions = Vec::with_capacity(group.members.len());
    for (output_index, member) in group.members.iter().copied().enumerate() {
        let branch = &branches[member];
        let bindings = AlphaBindings::match_gets(&group.fused_get, &branch.source_get)
            .ok_or_else(|| paro_error::internal("fused scan no longer covers a scalar branch"))?;
        let Expression::Aggregate(aggregate) = bindings
            .rebase_scalar(&Expression::Aggregate(branch.aggregate.clone()))
            .ok_or_else(|| {
                paro_error::internal("scalar aggregate inputs escaped the fused scan")
            })?
        else {
            return Err(paro_error::internal(
                "scalar aggregate fusion produced a non-aggregate expression",
            ));
        };
        aggregates.push(Expression::Aggregate(aggregate));
        let aggregate_binding = ColumnBinding::new(aggregate_index, output_index);
        let scalar_expression = branch
            .scalar_expression
            .clone()
            .replace_column_ref(&|column| {
                (column.depth == 0 && column.binding == branch.scalar_source_binding).then(|| {
                    Expression::ColumnRef(ColumnRefExpression::new(
                        aggregate_binding,
                        branch.aggregate.return_type.clone(),
                    ))
                })
            });
        if !expression_uses_only_binding(&scalar_expression, aggregate_binding) {
            return Err(paro_error::internal(
                "scalar result expression escaped its fused aggregate output",
            ));
        }
        scalar_expressions.push(scalar_expression);
    }
    let aggregate = Aggregate::new(
        bind_context.generate_table_index(),
        aggregate_index,
        bind_context.generate_table_index(),
        source,
        Vec::new(),
        Vec::new(),
        aggregates,
        Vec::new(),
    );
    let aggregate_plan = OwnedLogicalPlan::new(
        bind_context,
        LogicalOperator::Aggregate(Box::new(aggregate)),
    );
    let projection_index = bind_context.generate_table_index();
    let projection = Projection::new(projection_index, aggregate_plan, scalar_expressions)
        .with_internal_outputs();
    let replacements = group
        .members
        .iter()
        .enumerate()
        .map(|(output_index, member)| {
            let branch = &branches[*member];
            debug_assert_eq!(branch.wrapper_type, projection.returned_types[output_index]);
            (
                branch.wrapper_binding,
                ColumnBinding::new(projection_index, output_index),
            )
        })
        .collect();
    Ok((
        OwnedLogicalPlan::new(bind_context, LogicalOperator::Projection(projection)),
        replacements,
    ))
}

fn rebuild_cross(
    leaves: Vec<OwnedLogicalPlan>,
    bind_context: &BindContext,
) -> Result<OwnedLogicalPlan> {
    let mut leaves = leaves.into_iter();
    let first = leaves
        .next()
        .ok_or_else(|| paro_error::internal("scalar aggregate fusion removed every input"))?;
    Ok(leaves.fold(first, |left, right| {
        OwnedLogicalPlan::new(
            bind_context,
            LogicalOperator::Join(Join::Cross(CrossProduct::new(left, right))),
        )
    }))
}

fn is_column_expression(expression: &Expression, binding: ColumnBinding, ty: &LogicalType) -> bool {
    matches!(expression, Expression::ColumnRef(column) if is_column(column, binding, ty))
}

fn is_column(column: &ColumnRefExpression, binding: ColumnBinding, ty: &LogicalType) -> bool {
    column.depth == 0 && column.binding == binding && &column.return_type == ty
}

fn is_movable(expression: &Expression) -> bool {
    let properties = expression.evaluation_properties();
    properties.can_share_evaluation() && !properties.is_reorder_fence()
}

fn expression_uses_only_binding(expression: &Expression, binding: ColumnBinding) -> bool {
    let mut saw_binding = false;
    let mut valid = true;
    ExpressionIterator::visit(expression, &mut |node| match node {
        Expression::ColumnRef(column) => {
            saw_binding |= column.depth == 0 && column.binding == binding;
            valid &= column.depth == 0 && column.binding == binding;
            ExpressionVisitDecision::SkipChildren
        }
        Expression::Aggregate(_)
        | Expression::Reference(_)
        | Expression::Subquery(_)
        | Expression::Window(_) => {
            valid = false;
            ExpressionVisitDecision::SkipChildren
        }
        _ => ExpressionVisitDecision::Descend,
    });
    valid && saw_binding
}

fn expression_mentions_any(expression: &Expression, bindings: &HashSet<ColumnBinding>) -> bool {
    let mut found = false;
    ExpressionIterator::visit(expression, &mut |node| {
        if matches!(node, Expression::ColumnRef(column) if column.depth == 0 && bindings.contains(&column.binding))
        {
            found = true;
            ExpressionVisitDecision::SkipChildren
        } else {
            ExpressionVisitDecision::Descend
        }
    });
    found
}

#[cfg(test)]
mod tests {
    use paro_planner::operator::LogicalOperator;
    use paro_planner::planner::Planner;

    use super::super::partition_aggregate_tests::setup_session;
    use crate::optimizer::Optimizer as TestOptimizer;

    #[test]
    fn sibling_scalar_aggregates_share_one_filtered_scan() {
        let session = setup_session();
        let statement = paro_parser::parse_one(
            "SELECT CASE \
                 WHEN (SELECT count(*) FROM customer WHERE c_nationkey BETWEEN 1 AND 5) > 1 \
                 THEN (SELECT avg(c_acctbal) FROM customer WHERE c_nationkey BETWEEN 1 AND 5) \
                 ELSE (SELECT min(c_acctbal) FROM customer WHERE c_nationkey BETWEEN 1 AND 5) \
             END \
             FROM nation WHERE n_nationkey = 1",
        )
        .expect("parse scalar aggregate fusion shape")
        .stmt;
        let mut planner = Planner::new(session.clone());
        planner
            .create_plan(statement)
            .expect("plan scalar aggregate fusion shape");
        let plan = planner.take_plan().expect("logical plan");
        let mut optimizer = TestOptimizer::new(planner.binder.clone(), session);
        let optimized = optimizer
            .scalar_reuse_frontier_for_test(plan)
            .expect("enumerate scalar aggregate fusion frontier");

        let mut customer_gets = 0;
        let mut fused_aggregates = 0;
        optimized
            .try_visit_pre_order(|plan| {
                match &plan.operator {
                    LogicalOperator::Get(get)
                        if get
                            .table
                            .as_ref()
                            .is_some_and(|table| table.base.base.name == "customer") =>
                    {
                        customer_gets += 1;
                    }
                    LogicalOperator::Aggregate(aggregate)
                        if aggregate.groups.is_empty() && aggregate.aggregates.len() == 3 =>
                    {
                        fused_aggregates += 1;
                    }
                    _ => {}
                }
                Ok(())
            })
            .expect("inspect fused scalar aggregate plan");

        assert_eq!(customer_gets, 1, "{optimized:#?}");
        assert_eq!(fused_aggregates, 1, "{optimized:#?}");
    }

    #[test]
    fn sibling_scalar_aggregates_keep_distinct_filtered_inputs() {
        let session = setup_session();
        let statement = paro_parser::parse_one(
            "SELECT CASE \
                 WHEN n_nationkey = 1 \
                 THEN (SELECT avg(c_acctbal) FROM customer WHERE c_nationkey BETWEEN 1 AND 5) \
                 ELSE (SELECT avg(c_acctbal) FROM customer WHERE c_nationkey BETWEEN 6 AND 10) \
             END \
             FROM nation WHERE n_nationkey = 1",
        )
        .expect("parse distinct scalar aggregate inputs")
        .stmt;
        let mut planner = Planner::new(session.clone());
        planner
            .create_plan(statement)
            .expect("plan distinct scalar aggregate inputs");
        let plan = planner.take_plan().expect("logical plan");
        let mut optimizer = TestOptimizer::new(planner.binder.clone(), session);
        let optimized = optimizer
            .scalar_reuse_frontier_for_test(plan)
            .expect("enumerate scalar aggregate frontier");

        let mut customer_gets = 0;
        optimized
            .try_visit_pre_order(|plan| {
                if matches!(
                    &plan.operator,
                    LogicalOperator::Get(get)
                        if get.table.as_ref().is_some_and(|table| table.base.base.name == "customer")
                ) {
                    customer_gets += 1;
                }
                Ok(())
            })
            .expect("inspect distinct scalar aggregate plan");

        assert_eq!(customer_gets, 2, "{optimized:#?}");
    }
}
