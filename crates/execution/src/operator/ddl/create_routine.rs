// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Physical CREATE FUNCTION operator.

use crate::execution_context::ExecutionContext;
use crate::operator::state::OperatorSourceInput;
use crate::operator::PhysicalOperator;
use crate::operator_type::PhysicalOperatorType;
use crate::result_type::SourceResultType;
use paro_common::chunk::Chunk;
use paro_common::error::Result;
use paro_common::types::LogicalType;
use paro_planner::binder::ir::statement::BoundCreateRoutineInfo;
use std::any::Any;

#[derive(Debug)]
pub struct CreateRoutine {
    pub info: BoundCreateRoutineInfo,
}

impl CreateRoutine {
    pub fn new(info: BoundCreateRoutineInfo) -> Self {
        Self { info }
    }
}

impl PhysicalOperator for CreateRoutine {
    fn operator_type(&self) -> PhysicalOperatorType {
        PhysicalOperatorType::CreateRoutine
    }

    fn types(&self) -> &[LogicalType] {
        &[]
    }

    fn is_source(&self) -> bool {
        true
    }

    fn get_data(
        &self,
        ctx: &ExecutionContext,
        _chunk: &mut Chunk,
        _input: &mut OperatorSourceInput,
    ) -> Result<SourceResultType> {
        ctx.session
            .ddl()
            .expect("ddl context must exist inside transactions")
            .apply_create_routine(self.info.to_create_routine_info())?;
        Ok(SourceResultType::Finished)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}
