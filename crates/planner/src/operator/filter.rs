// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Logical Filter Operator
//!
//!

use crate::expression::Expression;
use crate::operator::ProjectionMap;
use crate::plan::LogicalPlan;

/// Filter represents a filter operation (WHERE clause).
#[derive(Debug)]
pub struct Filter {
    pub expressions: Vec<Expression>,
    pub child: Box<LogicalPlan>,
    /// Exact output projection derived by column lifetime analysis.
    pub projection_map: ProjectionMap,
}

impl Filter {
    pub fn new(child: LogicalPlan, expressions: Vec<Expression>) -> Self {
        let projection_map = ProjectionMap::all();
        Self {
            expressions,
            child: Box::new(child),
            projection_map,
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
        assert!(filter
            .projection_map
            .is_identity(filter.child.types().len()));
    }

    #[test]
    fn test_filter_projection_map_can_be_set() {
        let ctx = BindContext::new();
        let mut filter = Filter::new(LogicalPlan::dummy_scan(&ctx), vec![]);
        filter.projection_map = vec![0, 2, 4].into();
        assert_eq!(filter.projection_map.as_columns(), Some(&[0, 2, 4][..]));
    }
}
