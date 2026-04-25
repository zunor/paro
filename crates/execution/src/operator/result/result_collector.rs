// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Streaming result collection backed by [`QueryOutputBuffer`].

use std::any::Any;
use std::sync::{Arc, Mutex};

use paro_common::allocator::{Allocator, MemoryTag};
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::memory::{MemoryAccountingClass, MemoryAccountingContext, MemoryDomain};
use paro_common::types::LogicalType;

use crate::execution_context::ExecutionContext;
use crate::memory_runtime::{
    OperatorMemoryAccount, OutputAppendResult, QueryMemoryPool, QueryOutputBuffer,
};
use crate::operator::state::{
    GlobalSinkState, LocalSinkState, OperatorSinkCombineInput, OperatorSinkInput,
};
use crate::operator::PhysicalOperator;
use crate::operator_type::PhysicalOperatorType;
use crate::result_type::{SinkCombineResultType, SinkFinalizeType, SinkResultType};

/// Default query output buffer capacity in bytes.
const DEFAULT_OUTPUT_BUFFER_BYTES: usize = 64 * 1024 * 1024;

/// Physical operator that collects query results into a buffered stream.
///
/// This operator implements streaming execution by using a bounded buffer.
/// When the buffer is full, the sink operation returns `Blocked`, implementing
/// backpressure to prevent unbounded memory growth.
///
/// # Design
///
/// Unlike the old implementation that collected all results into a `Vec<Chunk>`,
/// this version uses `QueryOutputBuffer` which provides:
/// - Bounded buffer with configurable byte capacity
/// - Backpressure when buffer is full
/// - Proper close semantics
/// - Error propagation
///
/// # Thread Safety
///
/// The buffer is wrapped in `Arc<Mutex<>>` to allow safe concurrent access
/// from multiple pipeline threads.
pub struct PhysicalResultCollector {
    buffer: Arc<Mutex<QueryOutputBuffer>>,
    types: Vec<LogicalType>,
}

impl std::fmt::Debug for PhysicalResultCollector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PhysicalResultCollector")
            .field("types", &self.types)
            .field("buffer", &"<QueryOutputBuffer>")
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
    pub fn new(types: Vec<LogicalType>, buffer: Arc<Mutex<QueryOutputBuffer>>) -> Self {
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
    ///     query_memory_pool,
    /// );
    /// // Use collector in pipeline
    /// // Read results from buffer
    /// ```
    pub fn with_shared_result(
        types: Vec<LogicalType>,
        allocator: Arc<dyn Allocator>,
        query_memory_pool: Arc<QueryMemoryPool>,
    ) -> (Self, Arc<Mutex<QueryOutputBuffer>>) {
        let account = Arc::new(OperatorMemoryAccount::new(query_memory_pool));
        let owner: Arc<dyn paro_common::memory::MemoryOwner> = account;
        let memory = MemoryAccountingContext::from_owner(
            owner,
            MemoryDomain::Host,
            MemoryTag::Allocator,
            MemoryAccountingClass::NonRevocable,
        );
        let buffer = Arc::new(Mutex::new(QueryOutputBuffer::new(
            DEFAULT_OUTPUT_BUFFER_BYTES,
            memory,
            allocator,
        )));
        (Self::new(types, buffer.clone()), buffer)
    }

    /// Get a reference to the shared buffer.
    ///
    /// This allows external code to read results from the buffer.
    pub fn buffer(&self) -> &Arc<Mutex<QueryOutputBuffer>> {
        &self.buffer
    }
}

/// Global sink state for the result collector.
///
/// Holds the shared buffer that all threads write to.
struct ResultCollectorGlobalSinkState {
    buffer: Arc<Mutex<QueryOutputBuffer>>,
}

impl std::fmt::Debug for ResultCollectorGlobalSinkState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResultCollectorGlobalSinkState")
            .field("buffer", &"<QueryOutputBuffer>")
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
#[derive(Debug, Default)]
struct ResultCollectorLocalSinkState {
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

        let next = lstate.pending_chunk.take().unwrap_or_else(|| chunk.clone());
        let mut buffer = gstate
            .buffer
            .lock()
            .map_err(|e| paro_error::internal(format!("Failed to lock buffer: {}", e)))?;
        match buffer.try_append(next)? {
            OutputAppendResult::Success => Ok(SinkResultType::NeedMoreInput),
            OutputAppendResult::Full(chunk) => {
                lstate.pending_chunk = Some(chunk);
                let _ = buffer.block_sink(input.interrupt_state.clone());
                Ok(SinkResultType::Blocked)
            }
            OutputAppendResult::Closed => {
                Err(paro_error::internal("Cannot append to closed buffer"))
            }
        }
    }

    fn combine(
        &self,
        _ctx: &ExecutionContext,
        input: &mut OperatorSinkCombineInput,
    ) -> Result<SinkCombineResultType> {
        let lstate = input
            .local_state
            .as_any_mut()
            .downcast_mut::<ResultCollectorLocalSinkState>()
            .ok_or_else(|| paro_error::internal("Invalid local sink state".to_string()))?;

        if lstate.pending_chunk.is_some() {
            return Ok(SinkCombineResultType::Blocked);
        }

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
