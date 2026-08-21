// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Physical lowering for a logical row-fetch boundary.

use super::*;

impl PhysicalPlanGenerator {
    pub(crate) fn lower_row_fetch(
        &mut self,
        fetch: &LogicalRowFetch,
        project: Option<&LogicalProjection>,
    ) -> Result<(PhysicalNodeKind, Vec<PhysicalPlanNodeId>)> {
        let child = self.generate_node(fetch.child.as_ref())?;
        let mut mappings = Vec::with_capacity(fetch.sources.len());
        for source in &fetch.sources {
            let Expression::Reference(rowid) = &source.rowid else {
                return Err(paro_error::internal(
                    "row-fetch rowid was not resolved to a physical input reference",
                ));
            };
            let column_ids = source
                .needed_columns
                .iter()
                .map(|&column| {
                    u32::try_from(column).map_err(|_| {
                        paro_error::internal(format!(
                            "row-fetch catalog column {column} exceeds u32"
                        ))
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            mappings.push(RelationalRowFetchMapping {
                table_index: source.materialized_table_index,
                rowid_col_idx: rowid.index,
                table_name: source.table.base.base.name.clone(),
                schema_name: source.table.base.schema_name.clone(),
                column_ids: column_ids.into_boxed_slice(),
            });
        }
        let projection = project
            .map(|project| {
                let output_names = align_output_names(
                    project.visible_names.clone(),
                    project.expressions.len(),
                    "row-fetch project output",
                )?;
                Ok::<RowFetchProjectionSpec, paro_common::error::ParoError>(
                    RowFetchProjectionSpec {
                        expressions: project.expressions.clone().into_boxed_slice(),
                        output_names: output_names.into_boxed_slice(),
                        output_types: project
                            .expressions
                            .iter()
                            .map(Expression::return_type)
                            .collect::<Vec<_>>()
                            .into_boxed_slice(),
                    },
                )
            })
            .transpose()?;
        let spec = RowFetchSpec {
            mappings: mappings.into_boxed_slice(),
            raw_output_names: fetch.output_names().into_boxed_slice(),
            raw_output_types: fetch.output_types().into_boxed_slice(),
            projection,
        };
        Ok((PhysicalNodeKind::RowFetch(spec), vec![child]))
    }
}
