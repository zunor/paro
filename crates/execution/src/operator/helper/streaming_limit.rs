// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Streaming LIMIT/OFFSET operator.

use std::any::Any;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use paro_common::chunk::Chunk;
use paro_common::error::Result;
use paro_common::types::LogicalType;

use crate::execution_context::ExecutionContext;
use crate::operator::state::{GlobalOperatorState, OperatorState};
use crate::operator::{OrderPreservationType, PhysicalOperator};
use crate::operator_type::PhysicalOperatorType;
use crate::result_type::OperatorResultType;

pub const MAX_LIMIT_VALUE: usize = 1 << 62;

/// Physical streaming limit operator.
///
/// Implements LIMIT/OFFSET in a streaming fashion without materializing all data.
/// This is the preferred implementation when:
/// - Insertion order doesn't need to be preserved, OR
/// - The source doesn't support batch index
#[derive(Debug)]
pub struct StreamingLimit {
    /// Output types (same as child)
    types: Vec<LogicalType>,
    /// Limit value (None means no limit)
    limit: Option<usize>,
    /// Offset value (None means 0)
    offset: Option<usize>,
    /// Whether to run in parallel mode
    parallel: bool,
    /// Child operator
    child: Arc<dyn PhysicalOperator>,
}

/// Thread-local state for streaming limit.
#[derive(Debug)]
pub struct StreamingLimitOperatorState {
    /// Current limit value (may be computed from expression)
    limit: usize,
    /// Current offset value (may be computed from expression)
    offset: usize,
}

impl OperatorState for StreamingLimitOperatorState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

/// Global state for streaming limit (tracks offset across threads).
#[derive(Debug)]
pub struct StreamingLimitGlobalState {
    /// Current offset across all threads (atomic for parallel execution)
    current_offset: AtomicUsize,
}

impl GlobalOperatorState for StreamingLimitGlobalState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl StreamingLimit {
    /// Create a new streaming limit operator.
    pub fn new(
        types: Vec<LogicalType>,
        limit: Option<usize>,
        offset: Option<usize>,
        parallel: bool,
        child: Arc<dyn PhysicalOperator>,
    ) -> Self {
        Self {
            types,
            limit,
            offset,
            parallel,
            child,
        }
    }

    /// Get the effective limit value.
    fn get_limit(&self) -> usize {
        self.limit.unwrap_or(MAX_LIMIT_VALUE)
    }

    /// Get the effective offset value.
    fn get_offset(&self) -> usize {
        self.offset.unwrap_or(0)
    }

    /// Handle offset logic and slice the input chunk.
    ///
    /// Returns true if the chunk should be output, false if it should be skipped.
    fn handle_offset(
        input: &Chunk,
        current_offset: usize,
        offset: usize,
        limit: usize,
    ) -> Option<(usize, usize)> {
        let max_element = limit.saturating_add(offset);
        let input_size = input.size();

        if current_offset < offset {
            // We are not yet at the offset point
            if current_offset + input_size > offset {
                // We will reach the offset in this chunk
                let start_position = offset - current_offset;
                let chunk_count = limit.min(input_size - start_position);
                Some((start_position, chunk_count))
            } else {
                // Skip entire chunk
                None
            }
        } else {
            // We are past the offset
            let chunk_count = if current_offset + input_size >= max_element {
                // Have to limit the count
                max_element - current_offset
            } else {
                // Copy entire chunk
                input_size
            };
            Some((0, chunk_count))
        }
    }
}

impl PhysicalOperator for StreamingLimit {
    fn operator_type(&self) -> PhysicalOperatorType {
        PhysicalOperatorType::StreamingLimit
    }

    fn types(&self) -> &[LogicalType] {
        &self.types
    }

    fn explain_params(&self) -> Vec<String> {
        let mut params = Vec::new();
        match self.limit {
            Some(limit) => params.push(format!("Limit: {limit}")),
            None => params.push("Limit: ALL".to_string()),
        }
        if let Some(offset) = self.offset {
            if offset > 0 {
                params.push(format!("Offset: {offset}"));
            }
        }
        params
    }

    fn children_count(&self) -> usize {
        1
    }

    fn child(&self, index: usize) -> Option<&dyn PhysicalOperator> {
        if index == 0 {
            Some(self.child.as_ref())
        } else {
            None
        }
    }

    fn child_arc(&self, index: usize) -> Option<Arc<dyn PhysicalOperator>> {
        if index == 0 {
            Some(self.child.clone())
        } else {
            None
        }
    }

    fn parallel_operator(&self) -> bool {
        self.parallel
    }

    fn operator_order(&self) -> OrderPreservationType {
        OrderPreservationType::FixedOrder
    }

    fn get_operator_state(&self, _ctx: &ExecutionContext) -> Result<Box<dyn OperatorState>> {
        Ok(Box::new(StreamingLimitOperatorState {
            limit: self.get_limit(),
            offset: self.get_offset(),
        }))
    }

    fn get_global_operator_state(&self) -> Result<Box<dyn GlobalOperatorState>> {
        Ok(Box::new(StreamingLimitGlobalState {
            current_offset: AtomicUsize::new(0),
        }))
    }

    fn execute(
        &self,
        _ctx: &ExecutionContext,
        input: &Chunk,
        chunk: &mut Chunk,
        gstate: &dyn GlobalOperatorState,
        state: &mut dyn OperatorState,
    ) -> Result<OperatorResultType> {
        let gstate = gstate
            .as_any()
            .downcast_ref::<StreamingLimitGlobalState>()
            .expect("Invalid global state type for StreamingLimit");

        let state = state
            .as_any_mut()
            .downcast_mut::<StreamingLimitOperatorState>()
            .expect("Invalid state type for StreamingLimit");

        let limit = state.limit;
        let offset = state.offset;
        let max_element = limit.saturating_add(offset);

        // Atomically fetch and add the input size to track global offset
        let current_offset = gstate
            .current_offset
            .fetch_add(input.size(), Ordering::SeqCst);

        // Check if we've already reached the limit
        if limit == 0 || current_offset >= max_element {
            chunk.set_cardinality(0);
            return Ok(OperatorResultType::Finished);
        }

        // Handle offset and compute slice parameters
        match Self::handle_offset(input, current_offset, offset, limit) {
            None => {
                // Skip this chunk entirely (still in offset region)
                chunk.set_cardinality(0);
                Ok(OperatorResultType::NeedMoreInput)
            }
            Some((start, count)) => {
                if count == 0 {
                    chunk.set_cardinality(0);
                    return Ok(OperatorResultType::NeedMoreInput);
                }

                if start == 0 && count == input.size() {
                    // No slicing needed, reference the entire input
                    *chunk = input.clone();
                } else {
                    // Need to slice the input - create dictionary vectors
                    use paro_common::vector::Vector;

                    // Create indices for the slice: [start, start+1, ..., start+count-1]
                    let indices: Vec<u32> = (start..(start + count)).map(|i| i as u32).collect();

                    let mut sliced_vectors = Vec::with_capacity(input.data.len());
                    for col in &input.data {
                        let dict_vec = Vector::dictionary(Arc::clone(col), indices.clone());
                        sliced_vectors.push(Arc::new(dict_vec));
                    }

                    let mut sliced = Chunk::from_arc_vectors(sliced_vectors);
                    sliced.set_cardinality(count);
                    *chunk = sliced;
                }

                // Returning FINISHED here would drop the terminal output chunk
                // in the pipeline executor.
                Ok(OperatorResultType::NeedMoreInput)
            }
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::thread_context::ThreadContext;
    use paro_common::vector::Vector;
    use paro_context::{test_support::TestStatementContextBuilder, StatementContext};
    use std::sync::Arc;

    fn test_session() -> Arc<StatementContext> {
        TestStatementContextBuilder::minimal().build()
    }

    #[test]
    fn streaming_limit_keeps_terminal_output_chunk() {
        let child = Arc::new(crate::operator::helper::empty_result::EmptyResult::new(
            vec![LogicalType::Integer],
        ));
        let op = StreamingLimit::new(vec![LogicalType::Integer], Some(1), None, false, child);
        let session = test_session();
        let thread = ThreadContext::new(0, 1);
        let ctx = ExecutionContext::new(session, &thread, None);
        let gstate = op
            .get_global_operator_state()
            .expect("global operator state");
        let mut state = op.get_operator_state(&ctx).expect("operator state");
        let input = Chunk::from_vectors(vec![Vector::from_i32(&[10, 20])]);
        let mut output = Chunk::initialize(&[LogicalType::Integer], 2);

        let result = op
            .execute(&ctx, &input, &mut output, gstate.as_ref(), state.as_mut())
            .expect("execute");

        assert_eq!(result, OperatorResultType::NeedMoreInput);
        assert_eq!(output.size(), 1);
        assert_eq!(output.column(0).unwrap().get_i32(0), Some(10));
    }
}
