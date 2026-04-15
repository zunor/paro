//! Logical Limit Operator
//!
//!

use crate::expression::Expression;
use crate::plan::LogicalPlan;

/// Limit represents a LIMIT/OFFSET operation.
#[derive(Debug)]
pub struct Limit {
    pub limit: Option<Expression>,
    pub offset: Option<Expression>,
    /// Optional SQL hint `/*+ HNSW_EF(N) */` propagated to TopN/VectorScan.
    pub hnsw_ef_hint: Option<usize>,
    pub child: Box<LogicalPlan>,
}

impl Limit {
    pub fn new(child: LogicalPlan, limit: Option<Expression>, offset: Option<Expression>) -> Self {
        Self {
            limit,
            offset,
            hnsw_ef_hint: None,
            child: Box::new(child),
        }
    }

    pub fn with_hnsw_ef_hint(mut self, hnsw_ef_hint: Option<usize>) -> Self {
        self.hnsw_ef_hint = hnsw_ef_hint;
        self
    }
}
