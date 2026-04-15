// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Logical Order Operator
//!
//!

use crate::binder::ir::OrderByNode;
use crate::plan::LogicalPlan;

/// Order represents an ORDER BY operation.
#[derive(Debug)]
pub struct Order {
    pub orders: Vec<OrderByNode>,
    pub child: Box<LogicalPlan>,
    /// Projection map for column lifetime optimization.
    /// Empty vector means all columns are preserved.
    /// Non-empty vector contains indices of columns to keep.
    pub projection_map: Vec<usize>,
}

impl Order {
    pub fn new(child: LogicalPlan, orders: Vec<OrderByNode>) -> Self {
        Self {
            orders,
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
    fn test_order_has_projection_map() {
        let ctx = BindContext::new();
        let order = Order::new(LogicalPlan::dummy_scan(&ctx), vec![]);
        assert!(order.projection_map.is_empty());
    }

    #[test]
    fn test_order_projection_map_can_be_set() {
        let ctx = BindContext::new();
        let mut order = Order::new(LogicalPlan::dummy_scan(&ctx), vec![]);
        order.projection_map = vec![0, 1, 3];
        assert_eq!(order.projection_map, vec![0, 1, 3]);
    }
}
