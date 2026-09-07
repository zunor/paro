// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Producer-side construction for shared CTE demand domains.

use std::collections::HashSet;

use paro_common::types::LogicalType;
use paro_planner::binder::context::BindContext;
use paro_planner::expression::{ColumnRefExpression, Expression};
use paro_planner::operator::{
    ColumnBinding, ComparisonJoin, Join, JoinComparisonType, JoinCondition, JoinType,
    LogicalOperator, Projection, SetOperation,
};
use paro_planner::plan::{OwnedLogicalPlan, NodeStats};

use super::MaterializedDemandInfo;

pub(super) fn build_demand_relation(
    info: MaterializedDemandInfo,
    bind_context: &BindContext,
) -> Option<(Vec<usize>, OwnedLogicalPlan)> {
    let first = info.joined_refs.first()?;
    let key_ordinals = first.key_ordinals.clone();
    let key_types = first.key_types.clone();
    if info
        .joined_refs
        .iter()
        .any(|demand| demand.key_ordinals != key_ordinals || demand.key_types != key_types)
    {
        return None;
    }

    let mut branches = info.joined_refs.into_iter().map(|demand| {
        let projection_index = bind_context.generate_table_index();
        OwnedLogicalPlan::new(
            bind_context,
            LogicalOperator::Projection(
                Projection::new(projection_index, demand.plan, demand.expressions)
                    .with_internal_outputs(),
            ),
        )
    });
    let mut relation = branches.next()?;
    for branch in branches {
        let set_index = bind_context.generate_table_index();
        relation = OwnedLogicalPlan::new(
            bind_context,
            LogicalOperator::SetOperation(SetOperation::union(
                set_index,
                relation,
                branch,
                true,
                key_types.clone(),
            )),
        );
    }
    Some((key_ordinals, relation))
}

pub(super) fn push_group_demand(
    producer: &mut OwnedLogicalPlan,
    key_ordinals: &[usize],
    demand: OwnedLogicalPlan,
    bind_context: &BindContext,
) -> bool {
    let output_bindings = producer.get_column_bindings();
    let output_types = producer.types();
    let mut keys = Vec::with_capacity(key_ordinals.len());
    for &ordinal in key_ordinals {
        let (Some(binding), Some(logical_type)) =
            (output_bindings.get(ordinal), output_types.get(ordinal))
        else {
            return false;
        };
        keys.push((*binding, logical_type.clone()));
    }
    push_group_demand_inner(producer, keys, demand, bind_context)
}

fn push_group_demand_inner(
    producer: &mut OwnedLogicalPlan,
    keys: Vec<(ColumnBinding, LogicalType)>,
    demand: OwnedLogicalPlan,
    bind_context: &BindContext,
) -> bool {
    match &mut producer.operator {
        LogicalOperator::Projection(projection) => {
            let mut child_keys = Vec::with_capacity(keys.len());
            for (binding, logical_type) in keys {
                if binding.table_index != projection.table_index {
                    return false;
                }
                let Some(Expression::ColumnRef(reference)) =
                    projection.expressions.get(binding.column_index)
                else {
                    return false;
                };
                if reference.depth != 0 || reference.return_type != logical_type {
                    return false;
                }
                child_keys.push((reference.binding, logical_type));
            }
            let changed = push_group_demand_inner(
                projection.child.as_mut(),
                child_keys,
                demand,
                bind_context,
            );
            if changed {
                producer.stats = NodeStats::default();
            }
            changed
        }
        LogicalOperator::Aggregate(aggregate) => {
            if !aggregate.grouping_sets.is_empty()
                || !aggregate.grouping_functions.is_empty()
                || aggregate.groups.is_empty()
            {
                return false;
            }
            let mut group_keys = Vec::with_capacity(keys.len());
            for (binding, logical_type) in keys {
                if binding.table_index != aggregate.group_index {
                    return false;
                }
                let Some(group) = aggregate.groups.get(binding.column_index) else {
                    return false;
                };
                if group.return_type() != logical_type
                    || group.evaluation_properties().is_reorder_fence()
                {
                    return false;
                }
                group_keys.push(group.clone());
            }
            let changed =
                push_to_key_owner(aggregate.child.as_mut(), &group_keys, demand, bind_context);
            if changed {
                producer.stats = NodeStats::default();
            }
            changed
        }
        _ => false,
    }
}

fn push_to_key_owner(
    producer: &mut OwnedLogicalPlan,
    key_expressions: &[Expression],
    demand: OwnedLogicalPlan,
    bind_context: &BindContext,
) -> bool {
    let mut key_bindings = Vec::new();
    for expression in key_expressions {
        crate::column::lifetime::ColumnLifetimeAnalyzer::extract_column_bindings(
            expression,
            &mut key_bindings,
        );
    }
    if key_bindings.is_empty() {
        return false;
    }

    if let LogicalOperator::Join(Join::Comparison(join)) = &mut producer.operator {
        if join.join_type == JoinType::Inner {
            let left_bindings = join
                .left
                .get_column_bindings()
                .into_iter()
                .collect::<HashSet<_>>();
            let right_bindings = join
                .right
                .get_column_bindings()
                .into_iter()
                .collect::<HashSet<_>>();
            let left_owns = key_bindings
                .iter()
                .all(|binding| left_bindings.contains(binding));
            let right_owns = key_bindings
                .iter()
                .all(|binding| right_bindings.contains(binding));
            if left_owns != right_owns {
                let changed = if left_owns {
                    push_to_key_owner(join.left.as_mut(), key_expressions, demand, bind_context)
                } else {
                    push_to_key_owner(join.right.as_mut(), key_expressions, demand, bind_context)
                };
                if changed {
                    // Join-graph estimates describe the complete old region.
                    // Once a child domain changes, retaining that provenance
                    // would price every ancestor with pre-rewrite rows.
                    producer.stats = NodeStats::default();
                }
                return changed;
            }
        }
    }

    let demand_bindings = demand.get_column_bindings();
    let demand_types = demand.types();
    if demand_bindings.len() != key_expressions.len() || demand_types.len() != key_expressions.len()
    {
        return false;
    }
    let conditions = key_expressions
        .iter()
        .cloned()
        .zip(demand_bindings.into_iter().zip(demand_types))
        .map(|(left, (binding, logical_type))| {
            JoinCondition::new(
                left,
                Expression::ColumnRef(ColumnRefExpression::new(binding, logical_type)),
                JoinComparisonType::Equal,
            )
        })
        .collect();
    let preserved = std::mem::replace(producer, OwnedLogicalPlan::synthetic(LogicalOperator::DummyScan));
    *producer = OwnedLogicalPlan::new(
        bind_context,
        LogicalOperator::Join(Join::Comparison(ComparisonJoin::new(
            JoinType::Semi,
            preserved,
            demand,
            conditions,
        ))),
    );
    true
}
