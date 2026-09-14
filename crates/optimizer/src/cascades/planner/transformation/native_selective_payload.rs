// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Native ordinary-Projection late fetch. TopN lowering remains a separate
//! contract; neither prefix legality nor expected cardinality substitutes for it.

use std::collections::{BTreeSet, HashMap};

use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_planner::expression::{ColumnRefExpression, Expression};
use paro_planner::operator::{
    ColumnBinding, LogicalOperator, LogicalOutputLayout, Projection, RowFetch, RowFetchSource,
};
use paro_planner::plan::NodeStats;

use super::staging::{NativeChild, NativeNode, NativeShell};
use super::PlannerTransformState;
use crate::aggregate::late_payload::{
    prefix_unary_child, prefix_unary_child_mut, prove_rowid_operator, RowIdPathPolicy,
};
use crate::expression::traversal::visit_expression;

pub(super) fn rewrite(
    shell: NativeShell,
    mut layouts: Vec<LogicalOutputLayout>,
    state: &PlannerTransformState,
) -> Result<Option<NativeShell>> {
    let root = shell.root;
    let original_layout = layouts[root].clone();
    let LogicalOperator::Projection(mut output) = shell.root_operator().clone() else {
        return Ok(None);
    };
    if output
        .expressions
        .iter()
        .any(|expression| !expression.evaluation_properties().can_share_evaluation())
    {
        return Ok(None);
    }
    let NativeChild::Node(input) = output.child else {
        return Ok(None);
    };
    let Some(fetched_rows) = shell.nodes[input]
        .stats
        .estimated_cardinality
        .map(|estimate| estimate.max)
    else {
        return Ok(None);
    };

    // Ordinary selective fetching currently has no post-join locality proof.
    // Resolve the single unary source without constructing another tree.
    let mut cursor = input;
    let mut ancestors = Vec::new();
    let get_index = loop {
        let operator = &shell.nodes[cursor].operator;
        if matches!(operator, LogicalOperator::Get(_)) {
            break cursor;
        }
        let Some(NativeChild::Node(child)) = prefix_unary_child(operator) else {
            return Ok(None);
        };
        ancestors.push(cursor);
        cursor = *child;
    };
    let LogicalOperator::Get(mut get) = shell.nodes[get_index].operator.clone() else {
        unreachable!()
    };
    let resolve = |child: &NativeChild| match child {
        NativeChild::Node(index) => Some(&shell.nodes[*index].operator),
        _ => None,
    };
    // No Join was traversed above; the count callback is deliberately unknown
    // rather than claiming uniqueness for an unsupported branch.
    let Some(path) = prove_rowid_operator(
        &shell.nodes[input].operator,
        get.table_index,
        RowIdPathPolicy::RowPreserving,
        &resolve,
        &|_| None,
    ) else {
        return Ok(None);
    };
    if path.crosses_join() {
        return Ok(None);
    }
    let Some(table) = get
        .table
        .as_ref()
        .filter(|table| table.get_storage().is_some())
        .cloned()
    else {
        return Ok(None);
    };
    let mut delayed = HashMap::new();
    let mut valid = true;
    for expression in &output.expressions {
        visit_expression(expression, &mut |expression| {
            let Expression::ColumnRef(column) = expression else {
                return;
            };
            if column.depth != 0 {
                valid = false;
                return;
            }
            if column.binding.table_index != get.table_index {
                return;
            }
            let Some(catalog) = get.stored_column(column.binding.column_index) else {
                valid = false;
                return;
            };
            if table
                .columns
                .get(catalog)
                .is_none_or(|definition| definition.logical_type != column.return_type)
            {
                valid = false;
                return;
            }
            delayed.insert(column.binding, catalog);
        });
    }
    if !valid || delayed.is_empty() {
        return Ok(None);
    }
    let Some(carrier_rows) = shell.nodes[get_index]
        .stats
        .estimated_cardinality
        .map(|estimate| estimate.expected)
    else {
        return Ok(None);
    };
    if fetched_rows >= carrier_rows
        || state
            .cost_model
            .late_row_fetch_benefit(
                carrier_rows,
                fetched_rows,
                delayed
                    .values()
                    .map(|&column| table.columns[column].logical_type.clone()),
                path.stages(),
            )
            .is_none()
    {
        return Ok(None);
    }

    // All semantic and cost guards precede symbol allocation and mutation.
    let rowid = get.append_virtual_rowid("rowid");
    let materialized = state.bind_context.generate_table_index();
    let carrier = state.bind_context.generate_table_index();
    let mut nodes = shell.nodes.into_vec();
    nodes[get_index].operator = LogicalOperator::Get(get);
    nodes[get_index].source_proofs = Box::new([]);
    layouts[get_index] = nodes[get_index].operator.output_layout_from_child_refs(&[]);
    let mut child = get_index;
    for index in ancestors.into_iter().rev() {
        let ordinal = layouts[child]
            .bindings()
            .iter()
            .position(|binding| *binding == rowid)
            .ok_or_else(|| paro_error::internal("native rowid disappeared below carrier"))?;
        let (_, projection) = prefix_unary_child_mut(&mut nodes[index].operator)
            .ok_or_else(|| paro_error::internal("native rowid path changed operator"))?;
        if let Some(projection) = projection {
            projection.include(ordinal);
        }
        layouts[index] = nodes[index]
            .operator
            .output_layout_from_child_refs(&[&layouts[child]]);
        nodes[index].source_proofs = Box::new([]);
        child = index;
    }

    let mut ordinary = Vec::new();
    let mut ordinary_indices = HashMap::new();
    for expression in &output.expressions {
        visit_expression(expression, &mut |expression| {
            let Expression::ColumnRef(column) = expression else {
                return;
            };
            if column.depth == 0
                && !delayed.contains_key(&column.binding)
                && !ordinary_indices.contains_key(&column.binding)
            {
                ordinary_indices.insert(column.binding, ordinary.len());
                ordinary.push((column.binding, column.return_type.clone()));
            }
        });
    }
    let mut carrier_expressions = ordinary
        .iter()
        .map(|(binding, ty)| column(*binding, ty.clone()))
        .collect::<Vec<_>>();
    let mut names = (0..ordinary.len())
        .map(|index| format!("late_carrier_{index}"))
        .collect::<Vec<_>>();
    let rowid_index = carrier_expressions.len();
    carrier_expressions.push(column(rowid, LogicalType::BigInt));
    names.push(format!("__late_rowid_{}", rowid.table_index));
    for expression in &mut output.expressions {
        *expression = expression.clone().replace_column_ref(&|reference| {
            if reference.depth != 0 {
                return None;
            }
            if let Some(&catalog) = delayed.get(&reference.binding) {
                return Some(column(
                    ColumnBinding::new(materialized, catalog),
                    reference.return_type.clone(),
                ));
            }
            ordinary_indices.get(&reference.binding).map(|&index| {
                column(
                    ColumnBinding::new(carrier, index),
                    reference.return_type.clone(),
                )
            })
        });
    }
    output.returned_types = output
        .expressions
        .iter()
        .map(Expression::return_type)
        .collect();
    let carrier_node = nodes.len();
    nodes.push(NativeNode {
        id: state.bind_context.next_plan_id(),
        stats: nodes[input].stats.clone(),
        source_proofs: Box::new([]),
        operator: LogicalOperator::Projection(Projection {
            table_index: carrier,
            returned_types: carrier_expressions
                .iter()
                .map(Expression::return_type)
                .collect(),
            expressions: carrier_expressions,
            visible_names: names,
            visible_count: 0,
            visible_qualifier: None,
            child: NativeChild::Node(input),
        }),
    });
    let fetch_node = nodes.len();
    nodes.push(NativeNode {
        id: state.bind_context.next_plan_id(),
        stats: NodeStats::default(),
        source_proofs: Box::new([]),
        operator: LogicalOperator::RowFetch(RowFetch {
            carrier_table_index: carrier,
            child: NativeChild::Node(carrier_node),
            sources: vec![RowFetchSource {
                materialized_table_index: materialized,
                rowid: column(
                    ColumnBinding::new(carrier, rowid_index),
                    LogicalType::BigInt,
                ),
                table,
                needed_columns: delayed
                    .values()
                    .copied()
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect::<Vec<_>>()
                    .into_boxed_slice(),
            }],
        }),
    });
    output.child = NativeChild::Node(fetch_node);
    nodes[root].operator = LogicalOperator::Projection(output);
    nodes[root].source_proofs = Box::new([]);
    let (result, layout) = super::compact_native_shell_with_layout(NativeShell {
        nodes: nodes.into_boxed_slice(),
        root,
    })?;
    if layout != original_layout {
        return Err(paro_error::internal(
            "native selective fetch changed root output contract",
        ));
    }
    Ok(Some(result))
}

fn column(binding: ColumnBinding, ty: LogicalType) -> Expression {
    Expression::ColumnRef(ColumnRefExpression::new(binding, ty).into())
}
