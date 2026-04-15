//! Plan Projection - Convert Projection to Projection
//!
//!

use super::generator::PhysicalPlanGenerator;
use crate::operator::projection::Projection;
use crate::operator::PhysicalOperator;
use paro_common::error::Result;
use paro_planner::operator::Projection as PlannerProjection;

use std::sync::Arc;

impl PhysicalPlanGenerator {
    /// Create physical plan for Projection.
    pub fn create_plan_projection(
        &self,
        projection: &PlannerProjection,
        child: Arc<dyn PhysicalOperator>,
    ) -> Result<Arc<dyn PhysicalOperator>> {
        let expressions = projection.expressions.clone();

        let physical_projection = Projection::new(expressions, child);
        Ok(Arc::new(physical_projection))
    }
}
