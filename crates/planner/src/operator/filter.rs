// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Logical Filter Operator
//!
//!

use crate::expression::Expression;
use crate::plan::LogicalPlan;

/// Filter represents a filter operation (WHERE clause).
#[derive(Debug)]
pub struct Filter {
    pub expressions: Vec<Expression>,
    pub child: Box<LogicalPlan>,
    /// Projection map for column lifetime optimization.
    /// Empty vector means all columns are preserved.
    /// Non-empty vector contains indices of columns to keep.
    pub projection_map: Vec<usize>,
}

impl Filter {
    pub fn new(child: LogicalPlan, expressions: Vec<Expression>) -> Self {
        Self {
            expressions,
            child: Box::new(child),
            projection_map: Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::binder::context::BindContext;

    use super::*;

    #[test]
    fn test_filter_has_projection_map() {
        let ctx = BindContext::new();
        let filter = Filter::new(LogicalPlan::dummy_scan(&ctx), vec![]);
        assert!(filter.projection_map.is_empty());
    }

    #[test]
    fn test_filter_projection_map_can_be_set() {
        let ctx = BindContext::new();
        let mut filter = Filter::new(LogicalPlan::dummy_scan(&ctx), vec![]);
        filter.projection_map = vec![0, 2, 4];
        assert_eq!(filter.projection_map, vec![0, 2, 4]);
    }
}
