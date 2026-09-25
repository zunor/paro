// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Aggregate TopN construction over the exact selected shell. Admission is
//! shared with the owned rule; this adapter supplies evidence, not new policy.

use super::native_topn_payload::{
    append_rowid, column, node, occurrences, projection, push, source_get,
};
use super::staging::{NativeChild, NativeNode, NativeShell};
use super::PlannerTransformState;
use crate::physical::access::late_payload::{
    prove_aggregate_topn_inputs, prove_rowid_operator, RowIdPathPolicy,
};
use crate::rewrite::expr::traversal::visit_expression;
use paro_common::error::{self as error, Result};
use paro_common::types::LogicalType;
use paro_planner::expression::Expression;
#[cfg(test)]
use paro_planner::operator::aggregate::GroupInputMultiplicity;
use paro_planner::operator::{
    ColumnBinding, LogicalOperator, ProjectionMap, RowFetch, RowFetchSource,
};
use std::collections::{BTreeSet, HashSet};

pub(super) fn rewrite(
    shell: NativeShell,
    state: &PlannerTransformState,
) -> Result<Option<NativeShell>> {
    if super::native_shell_contains_control_boundary(&shell) {
        return Ok(None);
    }
    let root = shell.root;
    let original_layout = shell.root_layout()?;
    let LogicalOperator::TopN(mut topn) = shell.root_operator().clone() else {
        return Ok(None);
    };
    let Some(output_index) = node(&topn.child) else {
        return Ok(None);
    };
    let LogicalOperator::Projection(mut output) = shell.nodes[output_index].operator.clone() else {
        return Ok(None);
    };
    let Some(aggregate_index) = node(&output.child) else {
        return Ok(None);
    };
    let LogicalOperator::Aggregate(mut aggregate) = shell.nodes[aggregate_index].operator.clone()
    else {
        return Ok(None);
    };
    let Some(input) = node(&aggregate.child) else {
        return Ok(None);
    };
    let Some(candidate) = prove_aggregate_topn_inputs(
        crate::physical::access::late_payload::AggregateTopNInputs {
            total_rows: topn.limit.saturating_add(topn.offset),
            orders: &topn.orders,
            output: &output,
            child_operator: &shell.nodes[aggregate_index].operator,
            child_cardinality: shell.nodes[aggregate_index].stats.estimated_cardinality,
            aggregate_input_cardinality: shell.nodes[input].stats.estimated_cardinality,
        },
        |table| {
            if occurrences(&shell, &aggregate.child, table) != Some(1) {
                return None;
            }
            let index = source_get(&shell, &aggregate.child, table)?;
            match &shell.nodes[index].operator {
                LogicalOperator::Get(get) => Some(get.as_ref()),
                _ => None,
            }
        },
        |table| {
            if occurrences(&shell, &aggregate.child, table) != Some(1) {
                return None;
            }
            prove_rowid_operator(
                &shell.nodes[input].operator,
                table,
                RowIdPathPolicy::NonNull,
                &|child| shell.nodes.get(node(child)?).map(|n| &n.operator),
                &|child| occurrences(&shell, child, table),
            )
        },
        &state.cost_model,
        &mut None,
    ) else {
        return Ok(None);
    };
    let projected = match topn.projection_map.as_columns() {
        None => (0..output.expressions.len()).collect::<Vec<_>>(),
        Some(indices) if indices.iter().all(|&i| i < output.expressions.len()) => indices.to_vec(),
        _ => {
            return Err(error::internal(
                "aggregate TopN projection ordinal out of bounds",
            ));
        }
    };
    let visible_count = projected
        .iter()
        .take_while(|&&i| i < output.visible_count)
        .count();
    if projected[visible_count..]
        .iter()
        .any(|&i| i < output.visible_count)
    {
        return Err(error::internal(
            "aggregate TopN interleaves visible/internal outputs",
        ));
    }
    let visible_names = projected[..visible_count]
        .iter()
        .map(|&i| {
            output
                .visible_names
                .get(i)
                .cloned()
                .ok_or_else(|| error::internal("aggregate TopN visible name missing"))
        })
        .collect::<Result<Vec<_>>>()?;
    let dependent = aggregate
        .group_dependencies
        .get(candidate.dependency)
        .ok_or_else(|| error::internal("aggregate TopN dependency ordinal stale"))?
        .dependents
        .iter()
        .copied()
        .collect::<HashSet<_>>();
    let mut required = HashSet::new();
    for expr in aggregate.groups.iter().chain(&aggregate.aggregates) {
        visit_expression(expr, &mut |e| {
            if let Expression::ColumnRef(c) = e {
                required.insert(c.binding);
            }
        });
    }
    let mut nodes = shell.nodes.into_vec();
    let rowid = append_rowid(
        &mut nodes,
        input,
        &candidate.rowid_path,
        candidate.source_table_index,
        &required,
    )?;
    let mut old_to_new = vec![None; aggregate.groups.len()];
    let mut groups = Vec::new();
    let mut group_stats = Vec::new();
    // Match apply_rewrite's ordinal/stat pairing, including its ordering.
    for (old, (group, stats)) in aggregate
        .groups
        .drain(..)
        .zip(aggregate.group_stats.drain(..))
        .enumerate()
    {
        if dependent.contains(&old) {
            continue;
        }
        old_to_new[old] = Some(groups.len());
        groups.push(group);
        group_stats.push(stats);
    }
    let rowid_group = groups.len();
    groups.push(column(rowid, LogicalType::BigInt));
    group_stats.push(None);
    aggregate.groups = groups;
    aggregate.group_stats = group_stats;
    aggregate.grouping_sets.clear();
    aggregate.group_dependencies.clear();
    aggregate.recompute_returned_types();

    let carrier_table = state.bind_context.generate_table_index();
    let materialized_table = state.bind_context.generate_table_index();
    let mut carrier_expressions = Vec::new();
    let mut carrier_names = Vec::new();
    let mut output_to_carrier = vec![None; output.expressions.len()];
    let mut final_expressions = Vec::new();
    for (i, expr) in output.expressions.iter().enumerate() {
        let Expression::ColumnRef(c) = expr else {
            return Err(error::internal("aggregate TopN output proof mismatch"));
        };
        if c.binding.table_index == aggregate.group_index {
            let old = c.binding.column_index;
            if let Some(&catalog) = candidate.dependent_catalog_columns.get(&old) {
                final_expressions.push(column(
                    ColumnBinding::new(materialized_table, catalog),
                    c.return_type.clone(),
                ));
                continue;
            }
            let new = old_to_new
                .get(old)
                .copied()
                .flatten()
                .ok_or_else(|| error::internal("aggregate TopN incomplete group remap"))?;
            carrier_expressions.push(column(
                ColumnBinding::new(aggregate.group_index, new),
                c.return_type.clone(),
            ));
            carrier_names.push(format!("late_group_{new}"));
        } else if c.binding.table_index == aggregate.aggregate_index {
            carrier_expressions.push(expr.clone());
            carrier_names.push(format!("late_aggregate_{}", c.binding.column_index));
        } else {
            return Err(error::internal(
                "aggregate TopN output escaped proven namespace",
            ));
        }
        let index = carrier_expressions.len() - 1;
        output_to_carrier[i] = Some(index);
        final_expressions.push(column(
            ColumnBinding::new(carrier_table, index),
            c.return_type.clone(),
        ));
    }
    for order in &mut topn.orders {
        let Expression::ColumnRef(c) = &mut order.expression else {
            return Err(error::internal("aggregate TopN order proof mismatch"));
        };
        let index = output_to_carrier
            .get(c.binding.column_index)
            .copied()
            .flatten()
            .ok_or_else(|| error::internal("aggregate TopN order depends on delayed payload"))?;
        c.binding = ColumnBinding::new(carrier_table, index);
    }
    let rowid_carrier = carrier_expressions.len();
    carrier_expressions.push(column(
        ColumnBinding::new(aggregate.group_index, rowid_group),
        LogicalType::BigInt,
    ));
    carrier_names.push("__late_rowid".into());
    let needed = projected
        .iter()
        .filter_map(|&i| {
            let Expression::ColumnRef(c) = &output.expressions[i] else {
                return None;
            };
            (c.binding.table_index == aggregate.group_index)
                .then(|| {
                    candidate
                        .dependent_catalog_columns
                        .get(&c.binding.column_index)
                        .copied()
                })
                .flatten()
        })
        .collect::<BTreeSet<_>>();
    let mut topn_outputs = projected
        .iter()
        .filter_map(|&i| output_to_carrier[i])
        .collect::<Vec<_>>();
    if !needed.is_empty() {
        topn_outputs.push(rowid_carrier);
    }
    topn.projection_map = ProjectionMap::new(topn_outputs);
    nodes[aggregate_index].operator = LogicalOperator::Aggregate(aggregate);
    nodes[aggregate_index].source_proofs = Box::new([]);
    let carrier = push(
        &mut nodes,
        projection(
            carrier_table,
            aggregate_index,
            carrier_expressions,
            carrier_names,
        ),
        state,
    );
    topn.child = NativeChild::Node(carrier);
    nodes[root].operator = LogicalOperator::TopN(topn);
    nodes[root].source_proofs = Box::new([]);
    let child = if needed.is_empty() {
        root
    } else {
        push(
            &mut nodes,
            LogicalOperator::RowFetch(RowFetch {
                carrier_table_index: carrier_table,
                child: NativeChild::Node(root),
                sources: vec![RowFetchSource {
                    materialized_table_index: materialized_table,
                    rowid: column(
                        ColumnBinding::new(carrier_table, rowid_carrier),
                        LogicalType::BigInt,
                    ),
                    table: candidate.table,
                    needed_columns: needed.into_iter().collect::<Vec<_>>().into_boxed_slice(),
                }],
            }),
            state,
        )
    };
    output.child = NativeChild::Node(child);
    output.expressions = projected
        .iter()
        .map(|&i| final_expressions[i].clone())
        .collect();
    output.visible_count = visible_count;
    output.visible_names = visible_names;
    output.returned_types = output
        .expressions
        .iter()
        .map(Expression::return_type)
        .collect();
    let final_root = nodes.len();
    nodes.push(NativeNode {
        id: nodes[output_index].id,
        stats: nodes[root].stats.clone(),
        source_proofs: Box::new([]),
        operator: LogicalOperator::Projection(output),
    });
    let (result, final_layout) = super::compact_native_shell_with_layout(NativeShell {
        nodes: nodes.into_boxed_slice(),
        root: final_root,
    })?;
    if final_layout != original_layout {
        return Ok(None);
    }
    Ok(Some(result))
}

#[cfg(test)]
#[path = "native_aggregate_topn_payload_tests.rs"]
mod tests;
