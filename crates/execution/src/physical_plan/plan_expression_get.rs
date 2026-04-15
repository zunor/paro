// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Plan Expression Get - Convert ExpressionGet to PhysicalExpressionScan

use super::generator::PhysicalPlanGenerator;
use crate::operator::scan::expression_scan::PhysicalExpressionScan;
use crate::operator::PhysicalOperator;
use paro_common::error::Result;
use paro_planner::operator::expression_get::ExpressionGet;
use std::sync::Arc;

impl PhysicalPlanGenerator {
    pub fn create_plan_expression_get(
        &self,
        op: &ExpressionGet,
    ) -> Result<Arc<dyn PhysicalOperator>> {
        let physical_scan = PhysicalExpressionScan::new(op.expressions.clone(), op.types.clone());
        Ok(Arc::new(physical_scan))
    }
}
