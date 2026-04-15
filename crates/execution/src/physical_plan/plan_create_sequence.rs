use super::generator::PhysicalPlanGenerator;
use crate::operator::ddl::create_sequence::CreateSequence;
use crate::operator::PhysicalOperator;
use paro_common::error::Result;
use paro_planner::operator::CreateSequence as LogicalCreateSequence;
use std::sync::Arc;

impl PhysicalPlanGenerator {
    pub fn create_plan_create_sequence(
        &self,
        op: &LogicalCreateSequence,
    ) -> Result<Arc<dyn PhysicalOperator>> {
        Ok(Arc::new(CreateSequence::new(op.info.clone())))
    }
}
