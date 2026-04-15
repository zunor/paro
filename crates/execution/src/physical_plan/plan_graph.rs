//! Plan Graph - Convert logical graph operators to physical graph operators.
//!
//! Handles:
//! - GraphScan → PhysicalGraphScan
//! - GraphExpand → PhysicalGraphExpand
//! - Projection over graph chain → PhysicalGraphProject

use paro_common::error::Result;
use paro_planner::operator::{GraphExpand, GraphScan, LogicalOperator, Projection};
use paro_planner::plan::LogicalPlan;
use std::collections::HashMap;
use std::sync::Arc;

use crate::operator::graph::graph_expand::PhysicalGraphExpand;
use crate::operator::graph::graph_path::PathRowRefSpec;
use crate::operator::graph::graph_path::{PathEmitSpec, PATH_HIDDEN_COLUMN_COUNT};
use crate::operator::graph::graph_project::PhysicalGraphProject;
use crate::operator::graph::graph_project::RowidMapping;
use crate::operator::graph::graph_scan::PhysicalGraphScan;
use crate::operator::graph::graph_shortest_path::PhysicalGraphShortestPath;
use crate::operator::PhysicalOperator;

use super::generator::PhysicalPlanGenerator;

impl PhysicalPlanGenerator {
    /// Create physical plan for GraphScan.
    ///
    /// Pass `scan.filter` to the physical operator so vertex predicates are
    /// evaluated at the scan level instead of being deferred to `GraphProject`.
    pub fn create_plan_graph_scan(&self, scan: &GraphScan) -> Result<Arc<dyn PhysicalOperator>> {
        Ok(Arc::new(PhysicalGraphScan::new(
            scan.graph_name.clone(),
            scan.vertex_info.clone(),
            scan.label.clone(),
            scan.filter.clone(),
            scan.schema_name.clone(),
        )))
    }

    /// Create physical plan for GraphExpand.
    pub fn create_plan_graph_expand(
        &self,
        expand: &GraphExpand,
        child: Arc<dyn PhysicalOperator>,
    ) -> Result<Arc<dyn PhysicalOperator>> {
        // Extract graph_name from the child chain (it's stored in GraphScan)
        let graph_name = extract_graph_name_from_logical(expand.child.as_ref()).unwrap_or_default();

        // Resolve hop range from quantifier (default: single-hop {1,1})
        let (min_hops, max_hops) = match &expand.quantifier {
            Some(paro_parser::ast::PathQuantifier::Bounded { lower, upper }) => {
                (*lower, upper.unwrap_or(*lower))
            }
            Some(paro_parser::ast::PathQuantifier::Plus) => (1, u64::MAX),
            Some(paro_parser::ast::PathQuantifier::Star) => (0, u64::MAX),
            None => (1, 1),
        };

        let emit_path_info = expand.has_path_functions;

        let child_layout = build_graph_chain_layout(expand.child.as_ref())?;
        let mut path_emit_spec = build_graph_path_prefix(expand.child.as_ref(), &child_layout)?;
        path_emit_spec.segment_vertex_table_oid = expand.target_table_oid;
        path_emit_spec.segment_edge_table_oid = expand.edge_info.table_oid;
        let source_local_col_idx = child_layout
            .local_id_cols
            .get(&expand.source_table_index)
            .copied()
            .ok_or_else(|| {
                paro_common::error::internal(format!(
                    "Missing source local_id column for graph table_index {}",
                    expand.source_table_index
                ))
            })?;
        let target_local_col_idx = child_layout
            .local_id_cols
            .get(&expand.target_table_index)
            .copied();

        if let Some(ref path_mode) = expand.path_mode {
            match path_mode {
                paro_parser::ast::PathMode::AnyShortest
                | paro_parser::ast::PathMode::AllShortest => {
                    let schema_name = extract_schema_name_from_logical(expand.child.as_ref())
                        .unwrap_or_else(|| "public".to_string());
                    return Ok(Arc::new(
                        PhysicalGraphShortestPath::with_path_info_and_filter(
                            graph_name,
                            expand.edge_info.clone(),
                            expand.direction,
                            expand.source_label.clone(),
                            expand.target_label.clone(),
                            source_local_col_idx,
                            target_local_col_idx,
                            path_mode.clone(),
                            min_hops,
                            max_hops,
                            emit_path_info,
                            path_emit_spec,
                            expand.target_filter.clone(),
                            expand.target_table_name.clone(),
                            schema_name,
                            child,
                        ),
                    ));
                }
                _ => {}
            }
        }

        // Pass edge_filter, target_filter, target_table_name, and schema_name
        // to the physical operator.
        // edge_filter is kept for future use (currently still applied in GraphProject).
        // target_filter is consumed by GraphExpand for BitSet pre-filtering.
        let schema_name = extract_schema_name_from_logical(expand.child.as_ref())
            .unwrap_or_else(|| "public".to_string());
        Ok(Arc::new(PhysicalGraphExpand::with_filters(
            graph_name,
            expand.edge_info.clone(),
            expand.direction,
            expand.source_label.clone(),
            expand.target_label.clone(),
            source_local_col_idx,
            target_local_col_idx,
            min_hops,
            max_hops,
            emit_path_info,
            path_emit_spec,
            expand.edge_filter.clone(),
            expand.target_filter.clone(),
            expand.target_table_name.clone(),
            schema_name,
            child,
        )))
    }

    /// Create physical plan for a graph projection (Projection over graph chain).
    ///
    /// Instead of using Projection (which can't do late materialization),
    /// we use PhysicalGraphProject which reads actual column values from tables
    /// using rowids from the expand chain.
    pub fn create_plan_graph_projection(
        &self,
        projection: &Projection,
    ) -> Result<Arc<dyn PhysicalOperator>> {
        // Extract graph_name and schema_name from the logical graph chain
        let _graph_name =
            extract_graph_name_from_logical(projection.child.as_ref()).unwrap_or_default();
        let schema_name = extract_schema_name_from_logical(projection.child.as_ref())
            .unwrap_or_else(|| "public".to_string());

        // Build the child physical plan (graph scan/expand chain)
        let child = self.create_graph_chain(projection.child.as_ref())?;

        // Build rowid mappings from the logical graph chain
        let rowid_mappings =
            build_rowid_mappings_from_logical(projection.child.as_ref(), &schema_name)?;

        let expressions = projection.expressions.clone();

        // Collect vertex/edge filters from the logical graph chain
        let filters = collect_filters_from_logical(projection.child.as_ref());

        Ok(Arc::new(PhysicalGraphProject::new(
            expressions,
            filters,
            rowid_mappings,
            child,
        )))
    }

    /// Recursively build the physical graph scan/expand chain.
    pub(crate) fn create_graph_chain(
        &self,
        plan: &LogicalPlan,
    ) -> Result<Arc<dyn PhysicalOperator>> {
        match &plan.operator {
            LogicalOperator::GraphScan(scan) => self.create_plan_graph_scan(scan),
            LogicalOperator::GraphExpand(expand) => {
                let child = self.create_graph_chain(expand.child.as_ref())?;
                self.create_plan_graph_expand(expand, child)
            }
            _ => Err(paro_common::error::internal(format!(
                "Unexpected operator in graph chain: {:?}",
                plan.operator.op_type()
            ))),
        }
    }
}

/// Extract graph_name from the logical graph chain (stored in GraphScan).
fn extract_graph_name_from_logical(plan: &LogicalPlan) -> Option<String> {
    match &plan.operator {
        LogicalOperator::GraphScan(scan) => Some(scan.graph_name.clone()),
        LogicalOperator::GraphExpand(expand) => {
            extract_graph_name_from_logical(expand.child.as_ref())
        }
        _ => None,
    }
}

/// Extract schema_name from the logical graph chain (stored in GraphScan).
fn extract_schema_name_from_logical(plan: &LogicalPlan) -> Option<String> {
    match &plan.operator {
        LogicalOperator::GraphScan(scan) => Some(scan.schema_name.clone()),
        LogicalOperator::GraphExpand(expand) => {
            extract_schema_name_from_logical(expand.child.as_ref())
        }
        _ => None,
    }
}

fn build_rowid_mappings_from_logical(
    plan: &LogicalPlan,
    schema_name: &str,
) -> Result<Vec<RowidMapping>> {
    let layout = build_graph_chain_layout(plan)?;
    let mut mappings = Vec::new();
    collect_rowid_mappings_from_logical(plan, schema_name, &layout, &mut mappings)?;
    Ok(mappings)
}

#[derive(Debug, Default, Clone)]
struct GraphChainLayout {
    width: usize,
    local_id_cols: HashMap<usize, usize>,
    rowid_cols: HashMap<usize, usize>,
}

fn build_graph_chain_layout(plan: &LogicalPlan) -> Result<GraphChainLayout> {
    match &plan.operator {
        LogicalOperator::GraphScan(scan) => {
            let mut layout = GraphChainLayout {
                width: scan.output_types.len(),
                ..GraphChainLayout::default()
            };
            layout.local_id_cols.insert(scan.table_index, 0);
            layout.rowid_cols.insert(scan.table_index, 1);
            Ok(layout)
        }
        LogicalOperator::GraphExpand(expand) => {
            let mut layout = build_graph_chain_layout(expand.child.as_ref())?;
            let base = layout.width;
            layout.rowid_cols.insert(expand.edge_table_index, base);
            layout
                .local_id_cols
                .insert(expand.target_table_index, base + 1);
            layout
                .rowid_cols
                .insert(expand.target_table_index, base + 2);
            layout.width += 3 + if expand.has_path_functions {
                PATH_HIDDEN_COLUMN_COUNT
            } else {
                0
            };
            Ok(layout)
        }
        _ => Err(paro_common::error::internal(format!(
            "Unexpected operator in graph chain layout: {:?}",
            plan.operator.op_type()
        ))),
    }
}

fn build_graph_path_prefix(plan: &LogicalPlan, layout: &GraphChainLayout) -> Result<PathEmitSpec> {
    let mut prefix = PathEmitSpec::default();
    collect_graph_path_prefix(plan, layout, &mut prefix)?;
    Ok(prefix)
}

fn collect_graph_path_prefix(
    plan: &LogicalPlan,
    layout: &GraphChainLayout,
    spec: &mut PathEmitSpec,
) -> Result<()> {
    match &plan.operator {
        LogicalOperator::GraphScan(scan) => {
            let rowid_col_idx = layout
                .rowid_cols
                .get(&scan.table_index)
                .copied()
                .ok_or_else(|| {
                    paro_common::error::internal(format!(
                        "Missing rowid column layout for graph scan table_index {}",
                        scan.table_index
                    ))
                })?;
            spec.prefix_vertices.push(PathRowRefSpec {
                table_oid: scan.vertex_info.table_oid,
                rowid_col_idx,
            });
            Ok(())
        }
        LogicalOperator::GraphExpand(expand) => {
            collect_graph_path_prefix(expand.child.as_ref(), layout, spec)?;

            let edge_rowid_col_idx = layout
                .rowid_cols
                .get(&expand.edge_table_index)
                .copied()
                .ok_or_else(|| {
                    paro_common::error::internal(format!(
                        "Missing edge rowid column layout for graph table_index {}",
                        expand.edge_table_index
                    ))
                })?;
            let target_rowid_col_idx = layout
                .rowid_cols
                .get(&expand.target_table_index)
                .copied()
                .ok_or_else(|| {
                paro_common::error::internal(format!(
                    "Missing target rowid column layout for graph table_index {}",
                    expand.target_table_index
                ))
            })?;

            spec.prefix_edges.push(PathRowRefSpec {
                table_oid: expand.edge_info.table_oid,
                rowid_col_idx: edge_rowid_col_idx,
            });
            spec.prefix_vertices.push(PathRowRefSpec {
                table_oid: expand.target_table_oid,
                rowid_col_idx: target_rowid_col_idx,
            });
            Ok(())
        }
        _ => Err(paro_common::error::internal(format!(
            "Unexpected operator in graph path prefix: {:?}",
            plan.operator.op_type()
        ))),
    }
}

fn collect_rowid_mappings_from_logical(
    plan: &LogicalPlan,
    schema_name: &str,
    layout: &GraphChainLayout,
    mappings: &mut Vec<RowidMapping>,
) -> Result<()> {
    match &plan.operator {
        LogicalOperator::GraphScan(scan) => {
            let rowid_col_idx = layout
                .rowid_cols
                .get(&scan.table_index)
                .copied()
                .ok_or_else(|| {
                    paro_common::error::internal(format!(
                        "Missing rowid column layout for graph scan table_index {}",
                        scan.table_index
                    ))
                })?;
            mappings.push(RowidMapping {
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
                    paro_common::error::internal(format!(
                        "Missing rowid column layout for graph edge table_index {}",
                        expand.edge_table_index
                    ))
                })?;
            mappings.push(RowidMapping {
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
                paro_common::error::internal(format!(
                    "Missing rowid column layout for graph target table_index {}",
                    expand.target_table_index
                ))
            })?;
            mappings.push(RowidMapping {
                table_index: expand.target_table_index,
                rowid_col_idx: target_rowid_col_idx,
                table_name: expand.target_table_name.clone(),
                schema_name: schema_name.to_string(),
            });
            Ok(())
        }
        _ => Err(paro_common::error::internal(format!(
            "Unexpected operator in rowid mapping collection: {:?}",
            plan.operator.op_type()
        ))),
    }
}

/// Collect filter expressions that must remain in GraphProject (not pushed down).
///
/// `scan.filter` is consumed by `PhysicalGraphScan`, so it is not collected
/// here to avoid double-evaluation.
///
/// `target_filter` is consumed by `PhysicalGraphExpand` via BitSet pre-filtering,
/// so it is not collected here to avoid double-evaluation.
///
/// Only `edge_filter` is collected for `GraphProject`, because edge attributes
/// require late materialization and are not available during expansion.
fn collect_filters_from_logical(plan: &LogicalPlan) -> Vec<paro_planner::expression::Expression> {
    let mut filters = Vec::new();
    collect_filters_recursive(plan, &mut filters);
    filters
}

fn collect_filters_recursive(
    plan: &LogicalPlan,
    filters: &mut Vec<paro_planner::expression::Expression>,
) {
    match &plan.operator {
        LogicalOperator::GraphScan(_scan) => {
            // `scan.filter` is passed directly to `PhysicalGraphScan`.
        }
        LogicalOperator::GraphExpand(expand) => {
            collect_filters_recursive(expand.child.as_ref(), filters);
            // `edge_filter` stays in `GraphProject` because edge attributes are
            // only available after rowid-based late materialization.
            if let Some(ref filter) = expand.edge_filter {
                filters.push(filter.clone());
            }
            // `target_filter` is consumed by `PhysicalGraphExpand` during
            // BitSet pre-filtering.
        }
        _ => {}
    }
}
