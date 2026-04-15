// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Physical plan generation for CTE operators.

use std::sync::Arc;

use paro_common::error::{self as paro_error, Result};
use paro_planner::operator::{
    CTERef as LogicalCTERef, MaterializedCTE as LogicalMaterializedCTE,
    RecursiveCTE as LogicalRecursiveCTE,
};

use super::generator::PhysicalPlanGenerator;
use crate::operator::set::cte::{CteScan, CteWorkingTable, CTE};
use crate::operator::set::recursive_cte::RecursiveCTE as PhysicalRecursiveCTE;
use crate::operator::PhysicalOperator;
use crate::physical_plan::generator::PlannedCteTable;

impl PhysicalPlanGenerator {
    pub fn create_plan_cte(
        &self,
        op: &LogicalMaterializedCTE,
    ) -> Result<Arc<dyn PhysicalOperator>> {
        let working_table = Arc::new(CteWorkingTable::new(op.column_types.clone()));
        let previous = {
            let mut context = self.plan_context.borrow_mut();
            context.cte_tables.insert(
                op.cte_index,
                PlannedCteTable {
                    working_table: Arc::clone(&working_table),
                    register_dependency: true,
                    cte_name: op.cte_name.clone(),
                },
            )
        };

        let cte_query = self.create_plan_from_logical_plan(op.cte_query.as_ref());
        let main_query = self.create_plan_from_logical_plan(op.child.as_ref());

        {
            let mut context = self.plan_context.borrow_mut();
            if let Some(previous) = previous {
                context.cte_tables.insert(op.cte_index, previous);
            } else {
                context.cte_tables.remove(&op.cte_index);
            }
        }

        let physical_cte = CTE::new(
            op.cte_name.clone(),
            op.cte_index,
            op.child.types(),
            op.materialized,
            op.ref_count,
            cte_query?,
            main_query?,
            working_table,
        );

        Ok(Arc::new(physical_cte))
    }

    pub fn create_plan_cte_ref(&self, op: &LogicalCTERef) -> Result<Arc<dyn PhysicalOperator>> {
        let working_table = {
            let context = self.plan_context.borrow();
            context.cte_tables.get(&op.cte_index).cloned()
        }
        .ok_or_else(|| {
            paro_error::internal(format!(
                "CTE working table not found for index {}",
                op.cte_index
            ))
        })?;

        Ok(Arc::new(CteScan::new(
            working_table.cte_name,
            op.cte_index,
            op.column_types.clone(),
            working_table.working_table,
            working_table.register_dependency,
        )))
    }

    pub fn create_plan_recursive_cte(
        &self,
        op: &LogicalRecursiveCTE,
    ) -> Result<Arc<dyn PhysicalOperator>> {
        let recursive_working_table = Arc::new(CteWorkingTable::new(op.column_types.clone()));
        let anchor = self.create_plan_from_logical_plan(op.anchor.as_ref())?;

        let previous = {
            let mut context = self.plan_context.borrow_mut();
            context.cte_tables.insert(
                op.cte_index,
                PlannedCteTable {
                    working_table: Arc::clone(&recursive_working_table),
                    register_dependency: false,
                    cte_name: op.cte_name.clone(),
                },
            )
        };

        let recursive = self.create_plan_from_logical_plan(op.recursive.as_ref());

        {
            let mut context = self.plan_context.borrow_mut();
            if let Some(previous) = previous {
                context.cte_tables.insert(op.cte_index, previous);
            } else {
                context.cte_tables.remove(&op.cte_index);
            }
        }

        Ok(Arc::new(PhysicalRecursiveCTE::new(
            op.cte_name.clone(),
            op.cte_index,
            op.column_types.clone(),
            op.union_all,
            anchor,
            recursive?,
            recursive_working_table,
        )))
    }
}
