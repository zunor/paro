//! Physical Plan Generator for DROP operations
//!
//!
//! ## Dependencies Check
//! - Allocator: N/A (DDL operation)
//! - BufferManager: N/A (DDL operation)

use super::generator::PhysicalPlanGenerator;
use crate::operator::ddl::drop::Drop;
use crate::operator::PhysicalOperator;
use paro_common::error::Result;
use paro_planner::operator::Drop as LogicalDrop;
use std::sync::Arc;

impl PhysicalPlanGenerator {
    /// Create a physical plan for DROP operations.
    ///
    /// DROP TABLE, DROP SCHEMA, etc. are all handled by Drop.
    pub fn create_plan_drop(&self, op: &LogicalDrop) -> Result<Arc<dyn PhysicalOperator>> {
        Ok(Arc::new(Drop::new(op.info.clone())))
    }
}
