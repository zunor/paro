// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Plan Window - Convert Window to Window
//!
//!
//! ## Dependencies Check
//! - Window: ✅
//! - Window: ✅
//!
//! ## Implementation Notes
//! - MVP creates a single Window for all window expressions

use super::generator::PhysicalPlanGenerator;
use crate::operator::window::window_operator::Window;
use crate::operator::PhysicalOperator;
use paro_common::error::Result;
use paro_planner::operator::Window as LogicalWindow;

use std::sync::Arc;

impl PhysicalPlanGenerator {
    /// Create physical plan for Window.
    pub fn create_plan_window(&self, window: &LogicalWindow) -> Result<Arc<dyn PhysicalOperator>> {
        // Create plan for child
        let child = self.create_plan_from_logical_plan(window.child.as_ref())?;

        // Create Window with all expressions
        let physical_window = Window::new(window.expressions.clone(), child);

        Ok(Arc::new(physical_window))
    }
}
