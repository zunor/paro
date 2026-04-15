// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Plan Order - Convert Order to Order
//!
//!
//! ## Design Notes
//! - MVP uses simple in-memory sorting
//! - Empty ORDER BY returns child plan directly (optimization)

use super::generator::PhysicalPlanGenerator;
use crate::operator::helper::order::Order;
use crate::operator::PhysicalOperator;
use paro_common::error::Result;
use paro_planner::operator::Order as LogicalOrder;
use std::sync::Arc;

impl PhysicalPlanGenerator {
    /// Create physical plan for Order.
    ///
    /// Converts Order to Order.
    /// If ORDER BY is empty, returns the child plan directly.
    pub fn create_plan_order(
        &self,
        order: &LogicalOrder,
        child: Arc<dyn PhysicalOperator>,
    ) -> Result<Arc<dyn PhysicalOperator>> {
        // If no ORDER BY expressions, return child directly
        if order.orders.is_empty() {
            return Ok(child);
        }

        // Get output types from child
        let types = child.types().to_vec();

        // Get projection map from Order
        // Empty projection map means output all columns
        let projections = order.projection_map.clone();

        // Create physical order operator
        let physical_order = Order::new(
            types,
            order.orders.clone(),
            projections,
            child,
            false, // is_index_sort = false for regular ORDER BY
        )?;

        Ok(Arc::new(physical_order))
    }
}
