// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Physical Dummy Scan Operator
//!
//!
//! ## Dependencies Check
//! - Allocator: ✅ via ExecutionContext

use std::any::Any;

use paro_common::chunk::Chunk;
use paro_common::error::Result;
use paro_common::types::LogicalType;

use crate::execution_context::ExecutionContext;
use crate::operator::state::{GlobalSourceState, LocalSourceState, OperatorSourceInput};
use crate::operator::PhysicalOperator;
use crate::operator_type::PhysicalOperatorType;
use crate::result_type::SourceResultType;

/// Physical dummy scan operator.
///
/// Returns a single empty row. Used for queries like `SELECT 1` or `SELECT 1+2`
/// where no actual table data is needed.
#[derive(Debug)]
pub struct PhysicalDummyScan {
    /// Output types (usually empty for dummy scan)
    output_types: Vec<LogicalType>,
}

impl PhysicalDummyScan {
    /// Create a new dummy scan operator.
    pub fn new() -> Self {
        Self {
            output_types: vec![],
        }
    }

    /// Create a dummy scan with specific output types.
    pub fn with_types(output_types: Vec<LogicalType>) -> Self {
        Self { output_types }
    }
}

impl Default for PhysicalDummyScan {
    fn default() -> Self {
        Self::new()
    }
}

impl PhysicalOperator for PhysicalDummyScan {
    fn operator_type(&self) -> PhysicalOperatorType {
        PhysicalOperatorType::DummyScan
    }

    fn types(&self) -> &[LogicalType] {
        &self.output_types
    }

    fn is_source(&self) -> bool {
        true
    }

    /// Dummy scan does not support parallel source.
    ///
    fn parallel_source(&self) -> bool {
        false
    }

    fn get_data(
        &self,
        ctx: &ExecutionContext,
        chunk: &mut Chunk,
        _input: &mut OperatorSourceInput,
    ) -> Result<SourceResultType> {
        // Dummy scan returns one row, then finishes
        // Check if we've already returned data via local state
        let lstate = _input
            .local_state
            .as_any_mut()
            .downcast_mut::<DummyScanLocalState>()
            .ok_or_else(|| paro_common::error::internal("Invalid local state".to_string()))?;

        if lstate.finished {
            return Ok(SourceResultType::Finished);
        }

        // Return a single row with empty columns (or initialize with types)
        if self.output_types.is_empty() {
            // Completely empty row (for SELECT without columns)
            chunk.set_cardinality(1);
        } else {
            // Initialize vectors with proper types
            let allocator = ctx.allocator(paro_common::allocator::MemoryTag::BaseTable);
            let new_chunk = Chunk::try_initialize(&self.output_types, 1, allocator)?;
            *chunk = new_chunk;
            chunk.set_cardinality(1);
        }

        lstate.finished = true;
        Ok(SourceResultType::HaveMoreOutput)
    }

    fn get_local_source_state(
        &self,
        _ctx: &ExecutionContext,
        _gstate: &dyn GlobalSourceState,
    ) -> Result<Box<dyn LocalSourceState>> {
        Ok(Box::new(DummyScanLocalState::new()))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

/// Local state for dummy scan - tracks if we've returned our single row
#[derive(Debug)]
struct DummyScanLocalState {
    finished: bool,
}

impl DummyScanLocalState {
    fn new() -> Self {
        Self { finished: false }
    }
}

impl LocalSourceState for DummyScanLocalState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}
