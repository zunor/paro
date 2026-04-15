// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Plan Insert - Convert Insert to PhysicalInsert

use super::generator::PhysicalPlanGenerator;
use crate::operator::persistent::insert::PhysicalInsert;
use crate::operator::scan::table_function::PhysicalTableFunction;
use crate::operator::PhysicalOperator;
use paro_common::error::Result;
use paro_planner::operator::insert::Insert;

use std::sync::Arc;

impl PhysicalPlanGenerator {
    /// Create physical plan for Insert.
    pub fn create_plan_insert(
        &self,
        insert: &Insert,
        child: Arc<dyn PhysicalOperator>,
    ) -> Result<Arc<dyn PhysicalOperator>> {
        let copy_from_read_csv = child
            .as_any()
            .downcast_ref::<PhysicalTableFunction>()
            .is_some_and(|table_function| {
                table_function
                    .function_name()
                    .eq_ignore_ascii_case("read_csv")
            });

        let physical_insert = PhysicalInsert::new(
            insert.table.clone(),
            insert.column_index_map.clone(),
            insert.expected_types.clone(),
            insert.on_conflict.clone(),
            child,
            copy_from_read_csv,
        );
        Ok(Arc::new(physical_insert))
    }
}
