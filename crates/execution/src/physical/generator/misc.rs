// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;

impl PhysicalPlanGenerator {
    pub(crate) fn plan_node_output(&self, id: PhysicalPlanNodeId) -> RowType {
        self.arena
            .get(id)
            .expect("synthetic physical node must exist")
            .output
            .clone()
    }

    pub(crate) fn lower_window(
        &mut self,
        window: &LogicalWindow,
    ) -> Result<(PhysicalNodeKind, Vec<PhysicalPlanNodeId>)> {
        let child = self.generate_node(window.child.as_ref())?;
        let input_width = window.child.types().len();
        let mut output_names = align_output_names(
            window.child.output_names(),
            input_width,
            "window child output",
        )?;
        output_names.extend((0..window.expressions.len()).map(|idx| format!("window_{}", idx + 1)));
        let spec = WindowSpec {
            window_index: window.window_index,
            expressions: window.expressions.clone().into_boxed_slice(),
            input_width,
            output_names: output_names.into_boxed_slice(),
            output_types: window.get_types().into_boxed_slice(),
        };
        Ok((PhysicalNodeKind::Window(spec), vec![child]))
    }

    pub(crate) fn lower_table_function(
        &mut self,
        get: &LogicalTableFunctionGet,
    ) -> Result<(PhysicalNodeKind, Vec<PhysicalPlanNodeId>)> {
        if get.is_in_out_function() {
            return Ok((
                self.unsupported(
                    "TABLE_FUNCTION_GET",
                    "table-in-out functions lower as transforms in a later phase",
                ),
                Vec::new(),
            ));
        }
        let spec = TableFunctionScanSpec {
            function: get.function.clone(),
            bind_data: get.bind_data.clone(),
            table_index: get.table_index,
            arguments: get.arguments.clone().into_boxed_slice(),
            projection_ids: get
                .projection_ids
                .as_ref()
                .map(|ids| ids.clone().into_boxed_slice()),
            input_table_types: get.input_table_types.clone().into_boxed_slice(),
            input_table_names: get.input_table_names.clone().into_boxed_slice(),
            output_names: get.get_names().into_boxed_slice(),
            output_types: get.get_types().into_boxed_slice(),
            with_ordinality: get.with_ordinality,
        };
        Ok((PhysicalNodeKind::TableFunctionScan(spec), Vec::new()))
    }

    pub(crate) fn lower_delim_get(
        &mut self,
        get: &LogicalDelimGet,
    ) -> (PhysicalNodeKind, Vec<PhysicalPlanNodeId>) {
        let spec = DelimScanSpec {
            target: DelimScanTarget::Values {
                table_index: get.table_index,
            },
            output_names: (0..get.chunk_types.len())
                .map(|idx| format!("delim_{}", idx + 1))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            output_types: get.chunk_types.clone().into_boxed_slice(),
        };
        (PhysicalNodeKind::DelimScan(spec), Vec::new())
    }
}
