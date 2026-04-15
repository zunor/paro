//! Plan Explain - Convert Explain to Explain

use super::generator::PhysicalPlanGenerator;
use crate::explain::explain_node::{
    build_explain_doc, render_explain_json_string, render_explain_text_lines,
};
use crate::operator::helper::explain::Explain;
use crate::operator::helper::explain_analyze::ExplainAnalyze;
use crate::operator::PhysicalOperator;
use paro_common::error::Result;
use paro_planner::operator::Explain as LogicalExplain;
use paro_planner::operator::{ExplainFormat, ExplainMode};
use std::sync::Arc;

impl PhysicalPlanGenerator {
    /// Create physical plan for Explain.
    ///
    /// For plain EXPLAIN, we plan the child query and render the physical plan
    /// into PostgreSQL-style text lines, then return a source operator that
    /// emits those lines as a single VARCHAR column.
    pub fn create_plan_explain(
        &self,
        explain: &LogicalExplain,
    ) -> Result<Arc<dyn PhysicalOperator>> {
        let explain_generator = self.for_explain(explain.spec);
        let child = explain_generator.create_plan_from_logical_plan(explain.child.as_ref())?;
        match explain.spec.mode {
            ExplainMode::Plan => {
                let doc = build_explain_doc(child.as_ref(), explain.spec);
                let plan_lines = match explain.spec.format {
                    ExplainFormat::Text => render_explain_text_lines(&doc),
                    ExplainFormat::Json => vec![render_explain_json_string(&doc)],
                };
                Ok(Arc::new(Explain::new(plan_lines)))
            }
            ExplainMode::Analyze => Ok(Arc::new(ExplainAnalyze::new(child, explain.spec))),
        }
    }
}
