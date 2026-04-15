// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Streaming result collection backed by [`BufferedData`].

use std::any::Any;
use std::sync::{Arc, Mutex};

use paro_common::allocator::Allocator;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;

use super::buffered_data::{AppendResult, BufferedData};
use crate::execution_context::ExecutionContext;
use crate::operator::state::{
    GlobalSinkState, LocalSinkState, OperatorSinkCombineInput, OperatorSinkInput,
};
use crate::operator::PhysicalOperator;
use crate::operator_type::PhysicalOperatorType;
use crate::result_type::{SinkCombineResultType, SinkFinalizeType, SinkResultType};

/// Default buffer capacity (number of chunks).
///
/// When the buffer reaches this capacity, the sink will block.
const DEFAULT_BUFFER_CAPACITY: usize = 50;

/// Physical operator that collects query results into a buffered stream.
///
/// This operator implements streaming execution by using a bounded buffer.
/// When the buffer is full, the sink operation returns `Blocked`, implementing
/// backpressure to prevent unbounded memory growth.
///
/// # Design
///
/// Unlike the old implementation that collected all results into a `Vec<Chunk>`,
/// this version uses `BufferedData` which provides:
/// - Bounded buffer with configurable capacity
/// - Backpressure when buffer is full
/// - Proper close semantics
/// - Error propagation
///
/// # Thread Safety
///
/// The buffer is wrapped in `Arc<Mutex<>>` to allow safe concurrent access
/// from multiple pipeline threads.
pub struct PhysicalResultCollector {
    buffer: Arc<Mutex<BufferedData>>,
    types: Vec<LogicalType>,
}

impl std::fmt::Debug for PhysicalResultCollector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PhysicalResultCollector")
            .field("types", &self.types)
            .field("buffer", &"<BufferedData>")
            .finish()
    }
}

impl PhysicalResultCollector {
    /// Create a new result collector with a shared buffer.
    ///
    /// # Arguments
    ///
    /// * `types` - Column types for the result
    /// * `buffer` - Shared buffer for collecting results
    pub fn new(types: Vec<LogicalType>, buffer: Arc<Mutex<BufferedData>>) -> Self {
        Self { buffer, types }
    }

    /// Create a result collector with a new buffer.
    ///
    /// Returns both the collector and a reference to the shared buffer,
    /// allowing the caller to read results from the buffer.
    ///
    /// # Arguments
    ///
    /// * `types` - Column types for the result
    /// * `allocator` - Allocator for memory management
    ///
    /// # Example
    ///
    /// ```ignore
    /// let (collector, buffer) = PhysicalResultCollector::with_shared_result(
    ///     types,
    ///     allocator,
    /// );
    /// // Use collector in pipeline
    /// // Read results from buffer
    /// ```
    pub fn with_shared_result(
        types: Vec<LogicalType>,
        allocator: Arc<dyn Allocator>,
    ) -> (Self, Arc<Mutex<BufferedData>>) {
        let buffer = Arc::new(Mutex::new(BufferedData::new(
            DEFAULT_BUFFER_CAPACITY,
            allocator,
        )));
        (Self::new(types, buffer.clone()), buffer)
    }

    /// Get a reference to the shared buffer.
    ///
    /// This allows external code to read results from the buffer.
    pub fn buffer(&self) -> &Arc<Mutex<BufferedData>> {
        &self.buffer
    }
}

/// Global sink state for the result collector.
///
/// Holds the shared buffer that all threads write to.
struct ResultCollectorGlobalSinkState {
    buffer: Arc<Mutex<BufferedData>>,
}

impl std::fmt::Debug for ResultCollectorGlobalSinkState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResultCollectorGlobalSinkState")
            .field("buffer", &"<BufferedData>")
            .finish()
    }
}

impl GlobalSinkState for ResultCollectorGlobalSinkState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

/// Local sink state for the result collector.
///
/// Each thread has its own local state for buffering chunks before
/// combining them into the global buffer.
#[derive(Debug, Default)]
struct ResultCollectorLocalSinkState {
    /// Buffered chunks waiting to be combined
    chunks: Vec<Chunk>,
    /// Pending chunk that could not be appended because the buffer was full.
    pending_chunk: Option<Chunk>,
}

impl LocalSinkState for ResultCollectorLocalSinkState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl PhysicalOperator for PhysicalResultCollector {
    fn operator_type(&self) -> PhysicalOperatorType {
        PhysicalOperatorType::ResultCollector
    }

    fn types(&self) -> &[LogicalType] {
        &self.types
    }

    fn is_sink(&self) -> bool {
        true
    }

    fn get_global_sink_state(&self, _ctx: &ExecutionContext) -> Result<Box<dyn GlobalSinkState>> {
        Ok(Box::new(ResultCollectorGlobalSinkState {
            buffer: self.buffer.clone(),
        }))
    }

    fn get_local_sink_state(&self, _ctx: &ExecutionContext) -> Result<Box<dyn LocalSinkState>> {
        Ok(Box::new(ResultCollectorLocalSinkState::default()))
    }

    fn sink(
        &self,
        _ctx: &ExecutionContext,
        chunk: &Chunk,
        input: &mut OperatorSinkInput,
    ) -> Result<SinkResultType> {
        let lstate = input
            .local_state
            .as_any_mut()
            .downcast_mut::<ResultCollectorLocalSinkState>()
            .ok_or_else(|| paro_error::internal("Invalid local sink state".to_string()))?;

        // Buffer chunk in local state
        lstate.chunks.push(chunk.clone());
        Ok(SinkResultType::NeedMoreInput)
    }

    fn combine(
        &self,
        _ctx: &ExecutionContext,
        input: &mut OperatorSinkCombineInput,
    ) -> Result<SinkCombineResultType> {
        let gstate = input
            .global_state
            .as_any()
            .downcast_ref::<ResultCollectorGlobalSinkState>()
            .ok_or_else(|| paro_error::internal("Invalid global sink state".to_string()))?;

        let lstate = input
            .local_state
            .as_any_mut()
            .downcast_mut::<ResultCollectorLocalSinkState>()
            .ok_or_else(|| paro_error::internal("Invalid local sink state".to_string()))?;

        let mut buffer = gstate
            .buffer
            .lock()
            .map_err(|e| paro_error::internal(format!("Failed to lock buffer: {}", e)))?;

        // First, try to append any previously blocked chunk.
        if let Some(pending) = lstate.pending_chunk.take() {
            match buffer.try_append(pending) {
                AppendResult::Success => {
                    // Successfully appended, continue with other chunks
                }
                AppendResult::Full(chunk) => {
                    // Still full, save it back and return Blocked
                    lstate.pending_chunk = Some(chunk);
                    return Ok(SinkCombineResultType::Blocked);
                }
                AppendResult::Closed => {
                    return Err(paro_error::internal("Cannot append to closed buffer"));
                }
            }
        }

        // Then append the newly produced chunks.
        let mut i = 0;
        while i < lstate.chunks.len() {
            let chunk = lstate.chunks[i].clone();
            match buffer.try_append(chunk) {
                AppendResult::Success => {
                    i += 1;
                }
                AppendResult::Full(chunk) => {
                    // Buffer is full, save pending chunk and return Blocked
                    lstate.pending_chunk = Some(chunk);
                    // Remove successfully appended chunks
                    lstate.chunks.drain(0..i);
                    return Ok(SinkCombineResultType::Blocked);
                }
                AppendResult::Closed => {
                    return Err(paro_error::internal("Cannot append to closed buffer"));
                }
            }
        }

        // All chunks appended successfully
        lstate.chunks.clear();
        Ok(SinkCombineResultType::Finished)
    }

    fn finalize(
        &self,
        _input: &crate::operator::state::OperatorSinkFinalizeInput,
    ) -> Result<SinkFinalizeType> {
        // Close the buffer to signal no more data will be appended
        let mut buffer = self
            .buffer
            .lock()
            .map_err(|e| paro_error::internal(format!("Failed to lock buffer: {}", e)))?;
        buffer.close();
        Ok(SinkFinalizeType::Ready)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}
