use paro_common::error::Result;
use paro_planner::plan::LogicalPlan;

use crate::context::OptimizationContext;
use crate::optimizer_type::OptimizerType;

/// Unified optimizer pass interface.
pub trait Rewriter: Send {
    fn optimizer_type(&self) -> OptimizerType;
    fn rewrite(&mut self, plan: LogicalPlan, ctx: &mut OptimizationContext) -> Result<LogicalPlan>;
}
