// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;
use paro_planner::logical::operator::graph_expand::graph_path_element_list_type;

pub(crate) fn is_graph_chain(plan: &PreparedNode) -> bool {
    matches!(
        &plan.operator,
        LogicalOperator::GraphScan(_) | LogicalOperator::GraphExpand(_)
    )
}

pub(crate) fn extract_graph_name_from_logical(plan: &PreparedNode) -> Option<String> {
    match &plan.operator {
        LogicalOperator::GraphScan(scan) => Some(scan.graph_name.clone()),
        LogicalOperator::GraphExpand(expand) => {
            extract_graph_name_from_logical(expand.child.as_ref())
        }
        _ => None,
    }
}

pub(crate) fn extract_schema_name_from_logical(plan: &PreparedNode) -> Option<String> {
    match &plan.operator {
        LogicalOperator::GraphScan(scan) => Some(scan.schema_name.clone()),
        LogicalOperator::GraphExpand(expand) => {
            extract_schema_name_from_logical(expand.child.as_ref())
        }
        _ => None,
    }
}

#[derive(Debug, Default, Clone)]
pub(crate) struct GraphChainLayout {
    pub(crate) width: usize,
    pub(crate) output_table_index: usize,
    pub(crate) local_id_cols: HashMap<usize, usize>,
    pub(crate) rowid_cols: HashMap<usize, usize>,
}

pub(crate) fn build_graph_chain_layout(plan: &PreparedNode) -> Result<GraphChainLayout> {
    match &plan.operator {
        LogicalOperator::GraphScan(scan) => {
            let mut layout = GraphChainLayout {
                width: scan.output_types.len(),
                output_table_index: scan.output_table_index,
                ..GraphChainLayout::default()
            };
            layout.local_id_cols.insert(scan.table_index, 0);
            layout.rowid_cols.insert(scan.table_index, 1);
            Ok(layout)
        }
        LogicalOperator::GraphExpand(expand) => {
            let mut layout = build_graph_chain_layout(expand.child.as_ref())?;
            if layout.output_table_index != expand.output_table_index {
                return Err(paro_error::internal(format!(
                    "GraphExpand carrier namespace changed within a graph chain: child={}, expand={}",
                    layout.output_table_index, expand.output_table_index
                )));
            }
            let base = layout.width;
            layout.rowid_cols.insert(expand.edge_table_index, base);
            layout
                .local_id_cols
                .insert(expand.target_table_index, base + 1);
            layout
                .rowid_cols
                .insert(expand.target_table_index, base + 2);
            layout.width = expand.output_types().len();
            if layout.width != base + 3 + usize::from(expand.has_path_functions) * 3 {
                return Err(paro_error::internal(
                    "GraphExpand logical carrier width is inconsistent with its child",
                ));
            }
            Ok(layout)
        }
        _ => Err(paro_error::internal(format!(
            "Unexpected operator in graph chain layout: {:?}",
            plan.operator.op_type()
        ))),
    }
}

pub(crate) fn build_rowid_mappings_from_logical(
    plan: &PreparedNode,
    schema_name: &str,
) -> Result<Vec<GraphRowFetchMapping>> {
    let layout = build_graph_chain_layout(plan)?;
    let mut mappings = Vec::new();
    collect_rowid_mappings_from_logical(plan, schema_name, &layout, &mut mappings)?;
    Ok(mappings)
}

pub(crate) fn collect_rowid_mappings_from_logical(
    plan: &PreparedNode,
    schema_name: &str,
    layout: &GraphChainLayout,
    mappings: &mut Vec<GraphRowFetchMapping>,
) -> Result<()> {
    match &plan.operator {
        LogicalOperator::GraphScan(scan) => {
            let rowid_col_idx = layout
                .rowid_cols
                .get(&scan.table_index)
                .copied()
                .ok_or_else(|| {
                    paro_error::internal(format!(
                        "Missing rowid column layout for graph scan table_index {}",
                        scan.table_index
                    ))
                })?;
            mappings.push(GraphRowFetchMapping {
                table_index: scan.table_index,
                rowid_col_idx,
                table_name: scan.vertex_info.table_name.clone(),
                schema_name: schema_name.to_string(),
            });
            Ok(())
        }
        LogicalOperator::GraphExpand(expand) => {
            collect_rowid_mappings_from_logical(
                expand.child.as_ref(),
                schema_name,
                layout,
                mappings,
            )?;

            let edge_rowid_col_idx = layout
                .rowid_cols
                .get(&expand.edge_table_index)
                .copied()
                .ok_or_else(|| {
                    paro_error::internal(format!(
                        "Missing rowid column layout for graph edge table_index {}",
                        expand.edge_table_index
                    ))
                })?;
            mappings.push(GraphRowFetchMapping {
                table_index: expand.edge_table_index,
                rowid_col_idx: edge_rowid_col_idx,
                table_name: expand.edge_info.table_name.clone(),
                schema_name: schema_name.to_string(),
            });

            let target_rowid_col_idx = layout
                .rowid_cols
                .get(&expand.target_table_index)
                .copied()
                .ok_or_else(|| {
                paro_error::internal(format!(
                    "Missing rowid column layout for graph target table_index {}",
                    expand.target_table_index
                ))
            })?;
            mappings.push(GraphRowFetchMapping {
                table_index: expand.target_table_index,
                rowid_col_idx: target_rowid_col_idx,
                table_name: expand.target_table_name.clone(),
                schema_name: schema_name.to_string(),
            });
            Ok(())
        }
        _ => Err(paro_error::internal(format!(
            "Unexpected operator in rowid mapping collection: {:?}",
            plan.operator.op_type()
        ))),
    }
}

pub(crate) fn collect_graph_filters_from_logical(plan: &PreparedNode) -> Vec<Expression> {
    let mut filters = Vec::new();
    collect_graph_filters_recursive(plan, &mut filters);
    filters
}

pub(crate) fn collect_graph_filters_recursive(plan: &PreparedNode, filters: &mut Vec<Expression>) {
    match &plan.operator {
        LogicalOperator::GraphScan(_) => {}
        LogicalOperator::GraphExpand(expand) => {
            collect_graph_filters_recursive(expand.child.as_ref(), filters);
            if let Some(filter) = &expand.edge_filter {
                filters.push(filter.clone());
            }
            if let Some(filter) = &expand.target_filter {
                filters.push(filter.clone());
            }
        }
        _ => {}
    }
}

pub(crate) fn graph_expand_output_row_type(
    child_output: RowType,
    has_path_functions: bool,
) -> (Vec<String>, Vec<LogicalType>) {
    let mut names = child_output.names.to_vec();
    names.extend([
        "edge_rowid".to_string(),
        "target_local_id".to_string(),
        "target_rowid".to_string(),
    ]);
    let mut types = child_output.types.to_vec();
    types.extend([
        LogicalType::UBigInt,
        LogicalType::UBigInt,
        LogicalType::UBigInt,
    ]);
    if has_path_functions {
        names.extend([
            "path_length".to_string(),
            "path_vertices".to_string(),
            "path_edges".to_string(),
        ]);
        types.extend([
            LogicalType::BigInt,
            graph_path_element_list_type(),
            graph_path_element_list_type(),
        ]);
    }
    (names, types)
}

pub(crate) fn graph_hop_range(expand: &LogicalGraphExpand<PreparedChild>) -> Result<(u64, u64)> {
    match &expand.quantifier {
        Some(paro_parser::ast::PathQuantifier::Bounded { lower, upper }) => {
            Ok((*lower, upper.unwrap_or(*lower)))
        }
        Some(paro_parser::ast::PathQuantifier::Plus) => Ok((1, u64::MAX)),
        Some(paro_parser::ast::PathQuantifier::Star) => Ok((0, u64::MAX)),
        None => Ok((1, 1)),
    }
}
