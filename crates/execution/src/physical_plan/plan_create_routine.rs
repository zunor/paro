// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::generator::PhysicalPlanGenerator;
use crate::operator::ddl::create_routine::CreateRoutine;
use crate::operator::PhysicalOperator;
use paro_common::error::Result;
use paro_planner::operator::CreateRoutine as LogicalCreateRoutine;
use std::sync::Arc;

impl PhysicalPlanGenerator {
    pub fn create_plan_create_routine(
        &self,
        op: &LogicalCreateRoutine,
    ) -> Result<Arc<dyn PhysicalOperator>> {
        Ok(Arc::new(CreateRoutine::new(op.info.clone())))
    }
}
