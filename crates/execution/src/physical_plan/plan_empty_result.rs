// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Physical plan generation for `EmptyResult`.

use std::sync::Arc;

use paro_common::error::Result;
use paro_planner::operator::EmptyResult as LogicalEmptyResult;

use super::generator::PhysicalPlanGenerator;
use crate::operator::helper::empty_result::EmptyResult;
use crate::operator::PhysicalOperator;

impl PhysicalPlanGenerator {
    pub fn create_plan_empty_result(
        &self,
        op: &LogicalEmptyResult,
    ) -> Result<Arc<dyn PhysicalOperator>> {
        Ok(Arc::new(EmptyResult::new(op.get_types())))
    }
}
