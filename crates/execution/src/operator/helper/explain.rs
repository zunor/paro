// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Physical EXPLAIN operator.
//!
//! Source-only operator that returns pre-rendered plan text lines as a
//! single VARCHAR column (`QUERY PLAN`).

use std::any::Any;

use paro_common::allocator::MemoryTag;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_common::vector::VECTOR_SIZE;

use crate::execution_context::ExecutionContext;
use crate::operator::state::{GlobalSourceState, LocalSourceState, OperatorSourceInput};
use crate::operator::PhysicalOperator;
use crate::operator_type::PhysicalOperatorType;
use crate::result_type::SourceResultType;

/// Explain emits one text row per rendered plan line.
#[derive(Debug)]
pub struct Explain {
    output_types: Vec<LogicalType>,
    plan_lines: Vec<String>,
}

impl Explain {
    pub fn new(plan_lines: Vec<String>) -> Self {
        Self {
            output_types: vec![LogicalType::Varchar],
            plan_lines,
        }
    }
}

impl PhysicalOperator for Explain {
    fn operator_type(&self) -> PhysicalOperatorType {
        PhysicalOperatorType::Explain
    }

    fn types(&self) -> &[LogicalType] {
        &self.output_types
    }

    fn is_source(&self) -> bool {
        true
    }

    fn parallel_source(&self) -> bool {
        false
    }

    fn get_local_source_state(
        &self,
        _ctx: &ExecutionContext,
        _gstate: &dyn GlobalSourceState,
    ) -> Result<Box<dyn LocalSourceState>> {
        Ok(Box::new(ExplainLocalSourceState::new()))
    }

    fn get_data(
        &self,
        ctx: &ExecutionContext,
        chunk: &mut Chunk,
        input: &mut OperatorSourceInput,
    ) -> Result<SourceResultType> {
        let lstate = input
            .local_state
            .as_any_mut()
            .downcast_mut::<ExplainLocalSourceState>()
            .ok_or_else(|| paro_error::internal("Invalid local state for Explain".to_string()))?;

        if lstate.next_line >= self.plan_lines.len() {
            return Ok(SourceResultType::Finished);
        }

        let remaining = self.plan_lines.len() - lstate.next_line;
        let output_count = remaining.min(VECTOR_SIZE);
        let allocator = ctx.allocator(MemoryTag::Allocator);
        let mut output_chunk =
            Chunk::initialize_with_allocator(&self.output_types, output_count, allocator);

        let output_vector = output_chunk.column_mut(0).ok_or_else(|| {
            paro_error::internal("Explain output chunk missing column".to_string())
        })?;

        for row_idx in 0..output_count {
            output_vector.set_value(
                row_idx,
                &Value::Varchar(self.plan_lines[lstate.next_line + row_idx].clone()),
            );
        }
        output_chunk.set_cardinality(output_count);
        lstate.next_line += output_count;
        *chunk = output_chunk;

        Ok(SourceResultType::HaveMoreOutput)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[derive(Debug, Default)]
struct ExplainLocalSourceState {
    next_line: usize,
}

impl ExplainLocalSourceState {
    fn new() -> Self {
        Self::default()
    }
}

impl LocalSourceState for ExplainLocalSourceState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}
