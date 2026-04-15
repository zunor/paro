use super::generator::PhysicalPlanGenerator;
use crate::operator::ddl::alter::Alter;
use crate::operator::PhysicalOperator;
use paro_common::error::Result;
use paro_planner::operator::Alter as LogicalAlter;
use std::sync::Arc;

impl PhysicalPlanGenerator {
    pub fn create_plan_alter(&self, op: &LogicalAlter) -> Result<Arc<dyn PhysicalOperator>> {
        Ok(Arc::new(Alter::new(op.info.clone())))
    }
}
