//! Window functions over a child rowset. Aggregates as window functions are not supported yet.

use paro_common::types::LogicalType;

use crate::expression::WindowExpression;
use crate::plan::LogicalPlan;

/// Window represents a window function computation.
///
/// Window functions compute values over a set of rows related to the current row.
/// Examples: ROW_NUMBER(), RANK(), SUM() OVER (...)
#[derive(Debug)]
pub struct Window {
    /// Unique index for this window operator.
    pub window_index: usize,
    /// The window expressions to compute.
    pub expressions: Vec<WindowExpression>,
    /// The child operator providing input rows.
    pub child: Box<LogicalPlan>,
}

impl Window {
    /// Create a new Window operator.
    pub fn new(
        window_index: usize,
        expressions: Vec<WindowExpression>,
        child: LogicalPlan,
    ) -> Self {
        Self {
            window_index,
            expressions,
            child: Box::new(child),
        }
    }

    /// Get the output types.
    /// Window functions append their results to the child's output.
    pub fn get_types(&self) -> Vec<LogicalType> {
        let mut types = self.child.types();
        for expr in &self.expressions {
            types.push(expr.return_type());
        }
        types
    }

    /// Get the number of window expressions.
    pub fn expression_count(&self) -> usize {
        self.expressions.len()
    }

    /// Get the operator name.
    pub fn name(&self) -> &'static str {
        "WINDOW"
    }
}
