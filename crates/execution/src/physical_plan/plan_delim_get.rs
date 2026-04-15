//! Physical plan generation for `DelimGet`.
//!
//!

use std::sync::Arc;

use paro_common::error::Result;
use paro_planner::operator::delim_get::DelimGet;

use super::generator::PhysicalPlanGenerator;
use crate::operator::scan::column_data_scan::PhysicalColumnDataScan;
use crate::operator::PhysicalOperator;

impl PhysicalPlanGenerator {
    pub fn create_plan_delim_get(&self, op: &DelimGet) -> Result<Arc<dyn PhysicalOperator>> {
        Ok(Arc::new(PhysicalColumnDataScan::new(
            op.chunk_types.clone(),
            Some(op.table_index),
        )))
    }
}

#[cfg(test)]
mod tests {
    use crate::operator::scan::column_data_scan::PhysicalColumnDataScan;
    use crate::physical_plan::generator::PhysicalPlanGenerator;
    use paro_common::types::LogicalType;
    use paro_context::{test_support::TestStatementContextBuilder, StatementContext};
    use paro_planner::operator::{DelimGet, LogicalOperator};
    use std::sync::Arc;

    fn test_session() -> Arc<StatementContext> {
        TestStatementContextBuilder::minimal().build()
    }

    #[test]
    fn logical_delim_get_plans_to_column_data_scan() {
        let generator = PhysicalPlanGenerator::new(test_session());
        let mut logical = LogicalOperator::DelimGet(DelimGet::new(99, vec![LogicalType::Integer]));

        let physical = generator
            .plan_operator(&mut logical)
            .expect("plan should succeed");
        assert_eq!(
            physical.operator_type(),
            crate::operator_type::PhysicalOperatorType::ColumnDataScan
        );

        let scan = physical
            .as_any()
            .downcast_ref::<PhysicalColumnDataScan>()
            .expect("expected column data scan");
        assert_eq!(scan.binding().dependency_id(), Some(99));
    }
}
