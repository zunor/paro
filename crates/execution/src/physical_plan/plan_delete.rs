// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Plan Delete - Convert Delete to PhysicalDelete
//!
//!
//! ## Dependencies Check
//! - PhysicalDelete: ✅
//!
//! ## Implementation Notes
//! - The child operator produces rows with a row_id column
//! - The row_id column index is determined from the child's output types
//! - DELETE returns a single BIGINT with the count of deleted rows

use super::generator::PhysicalPlanGenerator;
use crate::operator::persistent::delete::PhysicalDelete;
use crate::operator::PhysicalOperator;
use paro_common::error::{self as paro_error, Result};
use paro_planner::operator::delete::Delete;

use std::sync::Arc;

impl PhysicalPlanGenerator {
    /// Create physical plan for Delete.
    ///
    /// The child operator should produce rows with a row_id column that identifies
    /// which rows to delete. The row_id is typically the last column in the output.
    pub fn create_plan_delete(&self, delete: &Delete) -> Result<Arc<dyn PhysicalOperator>> {
        // Create plan for child (typically a scan + filter)
        let child = self.create_plan_from_logical_plan(delete.child.as_ref())?;

        // The row_id column is typically the last column in the child's output
        // For MVP, we assume row_id is the last column
        let child_types = child.types();
        if child_types.is_empty() {
            return Err(paro_error::internal(
                "DELETE child operator has no output columns".to_string(),
            ));
        }

        // Find the row_id column index
        // Convention: row_id is the last column added by the scan
        let row_id_index = child_types.len() - 1;

        let physical_delete = PhysicalDelete::new(
            delete.table.clone(),
            row_id_index,
            child,
            delete.is_full_table_delete,
        );

        Ok(Arc::new(physical_delete))
    }
}
