// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;

impl PhysicalPlanGenerator {
    pub(crate) fn lower_get(
        &mut self,
        get: &Get,
    ) -> Result<(PhysicalNodeKind, Vec<PhysicalPlanNodeId>)> {
        let Some(table) = get.table.clone() else {
            return Ok((
                self.unsupported("GET", "base table metadata is not available"),
                Vec::new(),
            ));
        };
        let table_column_count = table.columns.len();
        let mut column_ids = Vec::with_capacity(get.column_ids.len());
        let mut emit_row_id = false;
        for (idx, column_id) in get.column_ids.iter().copied().enumerate() {
            if column_id < table_column_count {
                column_ids.push(column_id);
            } else if column_id == table_column_count
                && get
                    .names
                    .get(idx)
                    .is_some_and(|name| name.eq_ignore_ascii_case("rowid"))
            {
                emit_row_id = true;
            } else {
                return Ok((
                    self.unsupported(
                        "GET",
                        format!(
                            "column id {column_id} is out of range for table with {table_column_count} columns"
                        ),
                    ),
                    Vec::new(),
                ));
            }
        }

        let spec = RowsetScanSpec {
            table_index: get.table_index,
            output_names: get.names.clone().into_boxed_slice(),
            returned_types: get.returned_types.clone().into_boxed_slice(),
            relation_name: get.relation_name.clone(),
            relation_alias: get.relation_alias.clone(),
            column_ids: column_ids.into_boxed_slice(),
            emit_row_id,
            column_types: get.column_types.clone().into_boxed_slice(),
            table,
            scan_order: get.scan_order.clone(),
            runtime_filter_expressions: get.runtime_filter_expressions.clone().into_boxed_slice(),
        };
        Ok((PhysicalNodeKind::RowsetScan(spec), Vec::new()))
    }

    pub(crate) fn lower_values(
        &mut self,
        values: &ExpressionGet,
    ) -> (PhysicalNodeKind, Vec<PhysicalPlanNodeId>) {
        let output_names = if values.names.len() == values.types.len() {
            values.names.clone()
        } else {
            (0..values.types.len())
                .map(|idx| format!("col{idx}"))
                .collect()
        };
        let expressions = values
            .expressions
            .iter()
            .cloned()
            .map(Vec::into_boxed_slice)
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let spec = ValuesSpec {
            table_index: values.table_index,
            expressions,
            output_names: output_names.into_boxed_slice(),
            output_types: values.types.clone().into_boxed_slice(),
        };
        (PhysicalNodeKind::Values(spec), Vec::new())
    }

    pub(crate) fn lower_empty_result(
        &mut self,
        empty: &LogicalEmptyResult,
    ) -> Result<(PhysicalNodeKind, Vec<PhysicalPlanNodeId>)> {
        let child = self.generate_node(empty.child.as_ref())?;
        Ok((PhysicalNodeKind::EmptyResult(EmptyResultSpec), vec![child]))
    }

    pub(crate) fn lower_filter(
        &mut self,
        filter: &LogicalFilter,
    ) -> Result<(PhysicalNodeKind, Vec<PhysicalPlanNodeId>)> {
        let child = self.generate_node(filter.child.as_ref())?;
        let expressions = if filter.expressions.len() <= 1 {
            filter.expressions.clone()
        } else {
            vec![Expression::Conjunction(ConjunctionExpression {
                conjunction_type: ConjunctionType::And,
                children: filter.expressions.clone(),
            })]
        };
        let spec = FilterSpec {
            expressions: expressions.into_boxed_slice(),
            projection_map: filter.projection_map.clone().into_boxed_slice(),
        };
        Ok((PhysicalNodeKind::Filter(spec), vec![child]))
    }

    pub(crate) fn lower_project(
        &mut self,
        project: &LogicalProjection,
    ) -> Result<(PhysicalNodeKind, Vec<PhysicalPlanNodeId>)> {
        let child = self.generate_node(project.child.as_ref())?;
        let spec = ProjectSpec {
            table_index: project.table_index,
            expressions: project.expressions.clone().into_boxed_slice(),
            output_names: align_output_names(
                project.output_names.clone(),
                project.expressions.len(),
                "project output",
            )?
            .into_boxed_slice(),
        };
        Ok((PhysicalNodeKind::Project(spec), vec![child]))
    }

    pub(crate) fn lower_limit(
        &mut self,
        limit: &LogicalLimit,
    ) -> Result<(PhysicalNodeKind, Vec<PhysicalPlanNodeId>)> {
        let child = self.generate_node(limit.child.as_ref())?;
        let spec = LimitSpec {
            limit: limit.limit.clone(),
            offset: limit.offset.clone(),
            hnsw_ef_hint: limit.hnsw_ef_hint,
        };
        Ok((PhysicalNodeKind::Limit(spec), vec![child]))
    }

    pub(crate) fn lower_topn(
        &mut self,
        topn: &LogicalTopN,
    ) -> Result<(PhysicalNodeKind, Vec<PhysicalPlanNodeId>)> {
        let child = self.generate_node(topn.child.as_ref())?;
        let output_types = topn.child.types();
        let output_names =
            align_output_names(topn.child.output_names(), output_types.len(), "topn output")?;
        let spec = TopNSpec {
            orders: topn.orders.clone().into_boxed_slice(),
            limit: topn.limit,
            offset: topn.offset,
            hnsw_ef_hint: topn.hnsw_ef_hint,
            output_names: output_names.into_boxed_slice(),
            output_types: output_types.into_boxed_slice(),
        };
        Ok((PhysicalNodeKind::TopN(spec), vec![child]))
    }

    pub(crate) fn lower_order(
        &mut self,
        order: &LogicalOrder,
    ) -> Result<(PhysicalNodeKind, Vec<PhysicalPlanNodeId>)> {
        let child = self.generate_node(order.child.as_ref())?;
        let child_types = order.child.types();
        let child_names = align_output_names(
            order.child.output_names(),
            child_types.len(),
            "order child output",
        )?;
        let output_names = project_by_index(&child_names, &order.projection_map, "order output")?;
        let output_types = project_by_index(&child_types, &order.projection_map, "order output")?;
        let spec = SortSpec {
            orders: order.orders.clone().into_boxed_slice(),
            projection_map: order.projection_map.clone().into_boxed_slice(),
            output_names: output_names.into_boxed_slice(),
            output_types: output_types.into_boxed_slice(),
        };
        Ok((PhysicalNodeKind::Sort(spec), vec![child]))
    }
}
