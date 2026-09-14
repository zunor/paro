// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Detail TopN's two row-fetch frontiers, operating only on selected native nodes.

use super::staging::{NativeChild, NativeNode, NativeShell};
use super::PlannerTransformState;
use crate::aggregate::late_payload::{
    prove_rowid_operator, RowIdJoinSide, RowIdPath, RowIdPathPolicy,
};
use paro_catalog::entry::TableCatalogEntry;
use paro_common::error::{self as error, Result};
use paro_common::types::LogicalType;
use paro_planner::expression::{ColumnRefExpression, Expression};
use paro_planner::operator::{
    ColumnBinding, Join, LogicalOperator, Projection, ProjectionMap, RowFetch, RowFetchSource,
};
use paro_planner::plan::NodeStats;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;

struct Source {
    table_index: usize,
    table: Arc<TableCatalogEntry>,
    ordered: HashMap<usize, usize>,
    output: HashMap<usize, usize>,
    path: RowIdPath,
    rowid: ColumnBinding,
    ordered_table: Option<usize>,
    output_table: Option<usize>,
    narrow_rowid: usize,
    topn_rowid: Option<usize>,
}

pub(super) fn node(child: &NativeChild) -> Option<usize> {
    match child {
        NativeChild::Node(index) => Some(*index),
        _ => None,
    }
}

/// Memo templates canonicalize carrier maps. Recover the selected root's
/// declared output with the same binding contract used by owned extraction;
/// otherwise hidden ordering columns would become visible output columns.
pub(super) fn restore_root_output(
    shell: &mut NativeShell,
    binding: &super::PatternOperand,
    memo: &super::Memo,
    state: &PlannerTransformState,
) -> Result<()> {
    let super::PatternOperand::Expression { expression, .. } = binding else {
        return Err(error::internal("native TopN root is not an expression"));
    };
    let logical = memo
        .logical_expr(*expression)
        .ok_or_else(|| error::internal("native TopN lost its expression"))?;
    let metadata = state
        .metadata
        .get(&logical.payload)
        .ok_or_else(|| error::internal("native TopN lost its output metadata"))?;
    let layout = shell.root_layout()?;
    let map = super::super::semantic_plan::projection_for_bindings(
        layout.bindings(),
        layout.types(),
        &metadata.output_columns,
        &state.binding_ids,
    )?;
    let LogicalOperator::TopN(topn) = &mut shell.nodes[shell.root].operator else {
        return Err(error::internal("native TopN root changed operator"));
    };
    topn.projection_map = map;
    Ok(())
}

// Count edges/occurrences, not unique node IDs. Opaque branches cannot prove absence.
pub(super) fn occurrences(shell: &NativeShell, child: &NativeChild, table: usize) -> Option<usize> {
    let op = &shell.nodes.get(node(child)?)?.operator;
    let mut count = Some(usize::from(
        matches!(op, LogicalOperator::Get(get) if get.table_index == table),
    ));
    op.visit_child_links(&mut |child| {
        count = count
            .zip(occurrences(shell, child, table))
            .map(|(a, b)| a.saturating_add(b).min(2));
    });
    count
}

pub(super) fn source_get(shell: &NativeShell, child: &NativeChild, table: usize) -> Option<usize> {
    let index = node(child)?;
    let op = &shell.nodes.get(index)?.operator;
    if matches!(op, LogicalOperator::Get(get) if get.table_index == table) {
        return Some(index);
    }
    let mut found = None;
    op.visit_child_links(&mut |child| {
        if found.is_none() {
            found = source_get(shell, child, table);
        }
    });
    found
}

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
    let total_rows = topn.limit.saturating_add(topn.offset);
    if total_rows == 0 {
        return Ok(None);
    }
    let Some(output_node) = node(&topn.child) else {
        return Ok(None);
    };
    let LogicalOperator::Projection(mut output) = shell.nodes[output_node].operator.clone() else {
        return Ok(None);
    };
    let Some(input) = node(&output.child) else {
        return Ok(None);
    };
    let Some(candidate) = crate::aggregate::late_payload::prove_row_preserving_inputs(
        total_rows,
        &topn.orders,
        &topn.projection_map,
        &output,
        matches!(shell.nodes[input].operator, LogicalOperator::RowFetch(_)),
        shell.nodes[input].stats.estimated_cardinality,
        |table| {
            if occurrences(&shell, &output.child, table) != Some(1) {
                return None;
            }
            let index = source_get(&shell, &output.child, table)?;
            match &shell.nodes[index].operator {
                LogicalOperator::Get(get) => Some(get.as_ref()),
                _ => None,
            }
        },
        |table| {
            prove_rowid_operator(
                &shell.nodes[input].operator,
                table,
                RowIdPathPolicy::RowPreserving,
                &|child| shell.nodes.get(node(child)?).map(|n| &n.operator),
                &|child| occurrences(&shell, child, table),
            )
        },
        |table| {
            let index = source_get(&shell, &output.child, table)?;
            Some(shell.nodes[index].stats.estimated_cardinality?.expected)
        },
        &state.cost_model,
        &mut None,
    ) else {
        return Ok(None);
    };
    let projected = topn.projection_map.as_columns().map_or_else(
        || (0..output.expressions.len()).collect::<Vec<_>>(),
        |v| v.to_vec(),
    );
    let mut sources = candidate
        .sources
        .into_iter()
        .map(|source| Source {
            table_index: source.source_table_index,
            table: source.table,
            ordered: source.ordered_catalog_columns,
            output: source.output_catalog_columns,
            path: source.rowid_path,
            rowid: ColumnBinding::new(source.source_table_index, 0),
            ordered_table: None,
            output_table: None,
            narrow_rowid: 0,
            topn_rowid: None,
        })
        .collect::<Vec<_>>();
    // Same ordinal naming contract as Projection::name_at (owned-only API).
    let names = (0..output.expressions.len())
        .map(|i| {
            output
                .visible_names
                .get(i)
                .cloned()
                .unwrap_or_else(|| format!("__paro_hidden_{i}"))
        })
        .collect::<Vec<_>>();
    let visible = projected
        .iter()
        .take_while(|&&i| i < output.visible_count)
        .count();
    if projected[visible..]
        .iter()
        .any(|&i| i < output.visible_count)
    {
        return Err(error::internal(
            "native TopN interleaves visible/internal outputs",
        ));
    }
    let final_names = projected[..visible]
        .iter()
        .map(|&i| {
            output
                .visible_names
                .get(i)
                .cloned()
                .ok_or_else(|| error::internal("native TopN visible name missing"))
        })
        .collect::<Result<Vec<_>>>()?;

    let narrow_table = state.bind_context.generate_table_index();
    let topn_table = state.bind_context.generate_table_index();
    let mut nodes = shell.nodes.into_vec();
    for source in &mut sources {
        let required = layout(&nodes, input)?.bindings().iter().copied().collect();
        source.rowid = append_rowid(
            &mut nodes,
            input,
            &source.path,
            source.table_index,
            &required,
        )?;
        source.ordered_table =
            (!source.ordered.is_empty()).then(|| state.bind_context.generate_table_index());
        source.output_table =
            (!source.output.is_empty()).then(|| state.bind_context.generate_table_index());
    }
    let mut narrow_expr = Vec::new();
    let mut narrow_names = Vec::new();
    let mut to_narrow = vec![None; output.expressions.len()];
    for (i, expr) in output.expressions.iter().enumerate() {
        if sources
            .iter()
            .any(|s| s.ordered.contains_key(&i) || s.output.contains_key(&i))
        {
            continue;
        }
        to_narrow[i] = Some(narrow_expr.len());
        narrow_expr.push(expr.clone());
        narrow_names.push(names[i].clone());
    }
    for source in &mut sources {
        source.narrow_rowid = narrow_expr.len();
        narrow_expr.push(column(source.rowid, LogicalType::BigInt));
        narrow_names.push(format!("__late_rowid_{}", source.table_index));
    }
    let narrow = push(
        &mut nodes,
        projection(narrow_table, input, narrow_expr, narrow_names),
        state,
    );
    let before = fetch_sources(&sources, true, narrow_table);
    let before = if before.is_empty() {
        narrow
    } else {
        push(
            &mut nodes,
            LogicalOperator::RowFetch(RowFetch {
                carrier_table_index: narrow_table,
                sources: before,
                child: NativeChild::Node(narrow),
            }),
            state,
        )
    };
    let mut topn_expr = Vec::new();
    let mut topn_names = Vec::new();
    let mut to_topn = vec![None; output.expressions.len()];
    for (i, expr) in output.expressions.iter().enumerate() {
        let binding = if let Some(s) = sources.iter().find(|s| s.ordered.contains_key(&i)) {
            ColumnBinding::new(
                s.ordered_table.expect("proved ordered namespace"),
                s.ordered[&i],
            )
        } else {
            if sources.iter().any(|s| s.output.contains_key(&i)) {
                continue;
            }
            ColumnBinding::new(narrow_table, to_narrow[i].expect("ordinary narrow output"))
        };
        to_topn[i] = Some(topn_expr.len());
        topn_expr.push(column(binding, expr.return_type()));
        topn_names.push(names[i].clone());
    }
    for s in &mut sources {
        if s.output_table.is_none() {
            continue;
        }
        s.topn_rowid = Some(topn_expr.len());
        topn_expr.push(column(
            ColumnBinding::new(narrow_table, s.narrow_rowid),
            LogicalType::BigInt,
        ));
        topn_names.push(format!("__late_rowid_{}", s.table_index));
    }
    let carrier = push(
        &mut nodes,
        projection(topn_table, before, topn_expr, topn_names),
        state,
    );
    for order in &mut topn.orders {
        let Expression::ColumnRef(c) = &mut order.expression else {
            unreachable!()
        };
        c.binding = ColumnBinding::new(
            topn_table,
            to_topn[c.binding.column_index].expect("ordered output available before TopN"),
        );
    }
    let mut indices = projected
        .iter()
        .filter_map(|&i| to_topn[i])
        .collect::<Vec<_>>();
    indices.extend(sources.iter().filter_map(|s| s.topn_rowid));
    topn.projection_map = ProjectionMap::new(indices);
    topn.child = NativeChild::Node(carrier);
    nodes[root].operator = LogicalOperator::TopN(topn);
    nodes[root].source_proofs = Box::new([]);
    let after = fetch_sources(&sources, false, topn_table);
    let after = if after.is_empty() {
        root
    } else {
        push(
            &mut nodes,
            LogicalOperator::RowFetch(RowFetch {
                carrier_table_index: topn_table,
                sources: after,
                child: NativeChild::Node(root),
            }),
            state,
        )
    };
    output.expressions = projected
        .iter()
        .map(|&i| {
            let binding = if let Some(s) = sources.iter().find(|s| s.output.contains_key(&i)) {
                ColumnBinding::new(s.output_table.expect("output namespace"), s.output[&i])
            } else {
                ColumnBinding::new(topn_table, to_topn[i].expect("ordinary TopN output"))
            };
            column(binding, output.expressions[i].return_type())
        })
        .collect();
    output.visible_count = visible;
    output.visible_names = final_names;
    output.returned_types = output
        .expressions
        .iter()
        .map(Expression::return_type)
        .collect();
    output.child = NativeChild::Node(after);
    // The old Projection node cannot be reused: TopN originally points through it.
    let final_root = nodes.len();
    nodes.push(NativeNode {
        id: nodes[output_node].id,
        stats: nodes[root].stats.clone(),
        source_proofs: Box::new([]),
        operator: LogicalOperator::Projection(output),
    });
    let (result, result_layout) = super::compact_native_shell_with_layout(NativeShell {
        nodes: nodes.into_boxed_slice(),
        root: final_root,
    })?;
    if result_layout != original_layout {
        return Ok(None);
    }
    Ok(Some(result))
}

pub(super) fn column(binding: ColumnBinding, ty: LogicalType) -> Expression {
    Expression::ColumnRef(ColumnRefExpression::new(binding, ty).into())
}
pub(super) fn projection(
    table_index: usize,
    child: usize,
    expressions: Vec<Expression>,
    names: Vec<String>,
) -> LogicalOperator<NativeChild> {
    LogicalOperator::Projection(Projection {
        table_index,
        returned_types: expressions.iter().map(Expression::return_type).collect(),
        expressions,
        visible_names: names,
        visible_count: 0,
        visible_qualifier: None,
        child: NativeChild::Node(child),
    })
}
pub(super) fn push(
    nodes: &mut Vec<NativeNode>,
    operator: LogicalOperator<NativeChild>,
    state: &PlannerTransformState,
) -> usize {
    let index = nodes.len();
    nodes.push(NativeNode {
        id: state.bind_context.next_plan_id(),
        stats: NodeStats::default(),
        operator,
        source_proofs: Box::new([]),
    });
    index
}
fn fetch_sources(sources: &[Source], ordered: bool, carrier: usize) -> Vec<RowFetchSource> {
    sources
        .iter()
        .filter_map(|s| {
            let materialized_table_index = if ordered {
                s.ordered_table
            } else {
                s.output_table
            }?;
            let columns = if ordered { &s.ordered } else { &s.output };
            Some(RowFetchSource {
                materialized_table_index,
                rowid: column(
                    ColumnBinding::new(
                        carrier,
                        if ordered {
                            s.narrow_rowid
                        } else {
                            s.topn_rowid.expect("output rowid")
                        },
                    ),
                    LogicalType::BigInt,
                ),
                table: s.table.clone(),
                needed_columns: columns
                    .values()
                    .copied()
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect::<Vec<_>>()
                    .into_boxed_slice(),
            })
        })
        .collect()
}

pub(super) fn layout(
    nodes: &[NativeNode],
    index: usize,
) -> Result<paro_planner::operator::LogicalOutputLayout> {
    let mut inputs = Vec::new();
    let mut failure = None;
    nodes[index]
        .operator
        .visit_child_links(&mut |child| match child {
            NativeChild::Node(i) => match layout(nodes, *i) {
                Ok(value) => inputs.push(value),
                Err(e) => failure = Some(e),
            },
            NativeChild::Group { layout, .. } | NativeChild::MemoGroup { layout, .. } => {
                inputs.push(layout.clone())
            }
        });
    if let Some(error) = failure {
        return Err(error);
    }
    Ok(nodes[index]
        .operator
        .output_layout_from_child_refs(&inputs.iter().collect::<Vec<_>>()))
}

pub(super) fn append_rowid(
    nodes: &mut [NativeNode],
    index: usize,
    path: &RowIdPath,
    table: usize,
    required: &HashSet<ColumnBinding>,
) -> Result<ColumnBinding> {
    if matches!(path, RowIdPath::Get) {
        let LogicalOperator::Get(get) = &mut nodes[index].operator else {
            return Err(error::internal("rowid Get witness mismatch"));
        };
        if get.table_index != table {
            return Err(error::internal("rowid source mismatch"));
        }
        let binding = get.append_virtual_rowid("rowid");
        nodes[index].source_proofs = Box::new([]);
        return Ok(binding);
    }
    let (slot, tail) = match path {
        RowIdPath::Join { side, child, .. } => (
            usize::from(matches!(side, RowIdJoinSide::Right)),
            child.as_ref(),
        ),
        RowIdPath::Filter(p)
        | RowIdPath::Window(p)
        | RowIdPath::Order(p)
        | RowIdPath::Limit(p)
        | RowIdPath::EmptyResult(p) => (0, p.as_ref()),
        RowIdPath::Get => unreachable!(),
    };
    let mut children = Vec::new();
    nodes[index]
        .operator
        .visit_child_links(&mut |c| children.push(c.clone()));
    let child = children
        .get(slot)
        .and_then(node)
        .ok_or_else(|| error::internal("rowid path has opaque child"))?;
    let binding = append_rowid(nodes, child, tail, table, required)?;
    let layouts = children
        .iter()
        .map(|c| match c {
            NativeChild::Node(i) => layout(nodes, *i),
            NativeChild::Group { layout, .. } | NativeChild::MemoGroup { layout, .. } => {
                Ok(layout.clone())
            }
        })
        .collect::<Result<Vec<_>>>()?;
    let ordinal = layouts[slot]
        .bindings()
        .iter()
        .position(|&b| b == binding)
        .ok_or_else(|| error::internal("rowid not exposed"))?;
    match &mut nodes[index].operator {
        LogicalOperator::Filter(op) => op.projection_map.include(ordinal),
        LogicalOperator::Order(op) => op.projection_map.include(ordinal),
        LogicalOperator::Join(join) => {
            let maps = match join {
                Join::Comparison(j) => {
                    Some((&mut j.left_projection_map, &mut j.right_projection_map))
                }
                Join::Any(j) => Some((&mut j.left_projection_map, &mut j.right_projection_map)),
                Join::Cross(_) => None,
            };
            if let Some((left, right)) = maps {
                if slot == 0 {
                    left.include(ordinal);
                } else {
                    right.include(ordinal);
                }
                for (map, layout) in [(left, &layouts[0]), (right, &layouts[1])] {
                    for (i, b) in layout.bindings().iter().enumerate() {
                        if required.contains(b) {
                            map.include(i);
                        }
                    }
                }
            }
        }
        LogicalOperator::Window(_)
        | LogicalOperator::Limit(_)
        | LogicalOperator::EmptyResult(_) => {}
        _ => return Err(error::internal("rowid transport witness mismatch")),
    }
    nodes[index].source_proofs = Box::new([]);
    Ok(binding)
}

#[cfg(test)]
#[path = "native_topn_payload_tests.rs"]
mod tests;
