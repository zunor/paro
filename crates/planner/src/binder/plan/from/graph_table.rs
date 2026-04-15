use crate::binder::ir::BoundFromGraphTable;
use crate::binder::Binder;
use crate::operator::{GraphMatch, LogicalOperator};
use paro_common::error::Result;

impl Binder {
    pub(crate) fn plan_graph_table_ref(
        &mut self,
        graph_ref: BoundFromGraphTable,
    ) -> Result<LogicalOperator> {
        let output_types = graph_ref.output_types.clone();
        let path_mode = graph_ref.path_mode.clone();
        let has_path_functions = graph_ref.has_path_functions;
        Ok(LogicalOperator::GraphMatch(GraphMatch::new(
            graph_ref.graph_entry,
            graph_ref.bound_pattern,
            graph_ref.bound_columns,
            graph_ref.table_index,
            output_types,
            path_mode,
            has_path_functions,
        )))
    }
}
