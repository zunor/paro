// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;

impl PhysicalPlanGenerator {
    pub(crate) fn lower_late_row_fetch_project(
        &mut self,
        project: &LogicalProjection,
    ) -> Result<(PhysicalNodeKind, Vec<PhysicalPlanNodeId>)> {
        let fetch = project.late_row_fetch.as_ref().ok_or_else(|| {
            paro_error::internal("late row-fetch projection is missing its fetch contract")
        })?;
        let child = self.generate_node(project.child.as_ref())?;
        let mut rowid_mappings = Vec::with_capacity(fetch.sources.len());
        for source in &fetch.sources {
            let Expression::Reference(rowid) = &source.rowid else {
                return Err(paro_error::internal(
                    "late row-fetch rowid was not resolved to a physical input reference",
                ));
            };
            rowid_mappings.push(RowFetchMapping {
                table_index: source.materialized_table_index,
                rowid_col_idx: rowid.index,
                table_name: source.table.base.base.name.clone(),
                schema_name: source.table.base.schema_name.clone(),
            });
        }
        let output_names = align_output_names(
            project.output_names.clone(),
            project.expressions.len(),
            "late row-fetch project output",
        )?;
        let output_types = project
            .expressions
            .iter()
            .map(Expression::return_type)
            .collect::<Vec<_>>();
        let spec = RowFetchProjectSpec {
            expressions: project.expressions.clone().into_boxed_slice(),
            filters: Box::new([]),
            carrier_table_index: fetch.carrier_table_index,
            rowid_mappings: rowid_mappings.into_boxed_slice(),
            output_names: output_names.into_boxed_slice(),
            output_types: output_types.into_boxed_slice(),
            coalesce_input: fetch.coalesce_input,
        };
        Ok((PhysicalNodeKind::RowFetchProject(spec), vec![child]))
    }

    pub(crate) fn lower_graph_scan(
        &mut self,
        scan: &LogicalGraphScan,
    ) -> (PhysicalNodeKind, Vec<PhysicalPlanNodeId>) {
        let spec = GraphScanSpec {
            vertex_info: scan.vertex_info.clone(),
            filter: scan.filter.clone(),
            table_index: scan.table_index,
            label: scan.label.clone(),
            graph_name: scan.graph_name.clone(),
            schema_name: scan.schema_name.clone(),
            output_types: scan.output_types.clone().into_boxed_slice(),
        };
        (PhysicalNodeKind::GraphScan(spec), Vec::new())
    }

    pub(crate) fn lower_graph_expand(
        &mut self,
        expand: &LogicalGraphExpand,
    ) -> Result<(PhysicalNodeKind, Vec<PhysicalPlanNodeId>)> {
        let (min_hops, max_hops) = graph_hop_range(expand)?;
        if (min_hops != 1 || max_hops != 1) && expand.source_label != expand.target_label {
            return self.unsupported_preserving_children(
                "GRAPH_EXPAND",
                "typed multi-hop graph expand currently requires source and target labels to match",
                &[expand.child.as_ref()],
            );
        }
        let child = self.generate_node(expand.child.as_ref())?;
        let child_output = self
            .arena
            .get(child)
            .map(|node| node.output.clone())
            .ok_or_else(|| paro_error::internal("GraphExpand child node is missing"))?;
        let graph_name = extract_graph_name_from_logical(expand.child.as_ref()).unwrap_or_default();
        let schema_name = extract_schema_name_from_logical(expand.child.as_ref())
            .unwrap_or_else(|| "public".to_string());
        let layout = build_graph_chain_layout(expand.child.as_ref())?;
        let source_local_col_idx = layout
            .local_id_cols
            .get(&expand.source_table_index)
            .copied()
            .ok_or_else(|| {
                paro_error::internal(format!(
                    "Missing source local_id column for graph table_index {}",
                    expand.source_table_index
                ))
            })?;
        let source_rowid_col_idx = layout
            .rowid_cols
            .get(&expand.source_table_index)
            .copied()
            .ok_or_else(|| {
                paro_error::internal(format!(
                    "Missing source rowid column for graph table_index {}",
                    expand.source_table_index
                ))
            })?;
        let target_local_col_idx = layout
            .local_id_cols
            .get(&expand.target_table_index)
            .copied();
        let (output_names, output_types) =
            graph_expand_output_row_type(child_output, expand.has_path_functions);

        if matches!(
            &expand.path_mode,
            Some(paro_parser::ast::PathMode::AnyShortest)
                | Some(paro_parser::ast::PathMode::AllShortest)
        ) {
            let spec = GraphShortestPathSpec {
                graph_name,
                edge_info: expand.edge_info.clone(),
                direction: expand.direction,
                source_label: expand.source_label.clone(),
                target_label: expand.target_label.clone(),
                source_local_col_idx,
                source_rowid_col_idx,
                target_local_col_idx,
                source_table_oid: expand.source_table_oid,
                target_table_oid: expand.target_table_oid,
                min_hops,
                max_hops,
                path_mode: expand.path_mode.clone(),
                target_filter: None,
                has_path_functions: expand.has_path_functions,
                target_table_name: expand.target_table_name.clone(),
                schema_name,
                output_names: output_names.into_boxed_slice(),
                output_types: output_types.into_boxed_slice(),
            };
            return Ok((PhysicalNodeKind::GraphShortestPath(spec), vec![child]));
        }

        let spec = GraphExpandSpec {
            graph_name,
            schema_name,
            edge_info: expand.edge_info.clone(),
            direction: expand.direction,
            source_label: expand.source_label.clone(),
            edge_filter: None,
            target_filter: None,
            source_table_index: expand.source_table_index,
            edge_table_index: expand.edge_table_index,
            target_table_index: expand.target_table_index,
            target_label: expand.target_label.clone(),
            source_local_col_idx,
            source_rowid_col_idx,
            min_hops,
            max_hops,
            source_table_oid: expand.source_table_oid,
            target_table_oid: expand.target_table_oid,
            target_table_name: expand.target_table_name.clone(),
            has_path_functions: expand.has_path_functions,
            output_names: output_names.into_boxed_slice(),
            output_types: output_types.into_boxed_slice(),
        };
        Ok((PhysicalNodeKind::GraphExpand(spec), vec![child]))
    }

    pub(crate) fn lower_graph_project(
        &mut self,
        project: &LogicalProjection,
    ) -> Result<(PhysicalNodeKind, Vec<PhysicalPlanNodeId>)> {
        let child = self.generate_node(project.child.as_ref())?;
        let schema_name = extract_schema_name_from_logical(project.child.as_ref())
            .unwrap_or_else(|| "public".to_string());
        let rowid_mappings =
            build_rowid_mappings_from_logical(project.child.as_ref(), &schema_name)?;
        let carrier_table_index =
            build_graph_chain_layout(project.child.as_ref())?.output_table_index;
        let filters = collect_graph_filters_from_logical(project.child.as_ref());
        let output_names = align_output_names(
            project.output_names.clone(),
            project.expressions.len(),
            "graph project output",
        )?;
        let output_types = project
            .expressions
            .iter()
            .map(Expression::return_type)
            .collect::<Vec<_>>();
        let spec = RowFetchProjectSpec {
            expressions: project.expressions.clone().into_boxed_slice(),
            filters: filters.into_boxed_slice(),
            carrier_table_index,
            rowid_mappings: rowid_mappings.into_boxed_slice(),
            output_names: output_names.into_boxed_slice(),
            output_types: output_types.into_boxed_slice(),
            coalesce_input: false,
        };
        Ok((PhysicalNodeKind::RowFetchProject(spec), vec![child]))
    }
}
