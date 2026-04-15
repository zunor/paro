use super::generator::PhysicalPlanGenerator;
use crate::operator::ddl::create_view::CreateView;
use crate::operator::PhysicalOperator;
use paro_common::error::Result;
use paro_planner::operator::CreateView as LogicalCreateView;
use std::sync::Arc;

impl PhysicalPlanGenerator {
    /// Create a physical plan for CREATE VIEW.
    ///
    /// This converts CreateView to CreateView.
    /// The physical operator will execute the DDL by calling Catalog::create_view().
    ///
    pub fn create_plan_create_view(
        &self,
        op: &LogicalCreateView,
    ) -> Result<Arc<dyn PhysicalOperator>> {
        Ok(Arc::new(CreateView::new(op.info.clone())))
    }
}
