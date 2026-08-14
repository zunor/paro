// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Physical fusion for a projection over a logical row-fetch boundary.

use super::*;

impl PhysicalPlanGenerator {
    pub(crate) fn lower_row_fetch_project(
        &mut self,
        project: &LogicalProjection,
        fetch: &LogicalRowFetch,
    ) -> Result<(PhysicalNodeKind, Vec<PhysicalPlanNodeId>)> {
        let child = self.generate_node(fetch.child.as_ref())?;
        let mut rowid_mappings = Vec::with_capacity(fetch.sources.len());
        for source in &fetch.sources {
            let Expression::Reference(rowid) = &source.rowid else {
                return Err(paro_error::internal(
                    "row-fetch rowid was not resolved to a physical input reference",
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
            "row-fetch project output",
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
            // Relational row fetch remains parallel. Graph lowering may still
            // request coalescing through the shared physical operator.
            coalesce_input: false,
        };
        Ok((PhysicalNodeKind::RowFetchProject(spec), vec![child]))
    }
}
