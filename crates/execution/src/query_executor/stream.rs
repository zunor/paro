// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Streaming result handling for query execution.

use std::sync::{Arc, Mutex};

use paro_common::allocator::Allocator;
use paro_common::chunk::Chunk;
use paro_common::error::Result;
use paro_common::types::LogicalType;
use paro_common::vector::VECTOR_SIZE;
use paro_context::StatementCancellation;
use paro_scheduler::coordinator::EventCoordinator;
use paro_scheduler::scheduler::TaskScheduler;

use crate::operator::result::buffered_data::BufferedData;

/// Result handler for streaming query execution.
///
/// This handler drives pipeline execution on-demand as chunks are fetched.
/// It holds a shared buffer with the pipeline sink and an event coordinator
/// that manages pipeline execution.
pub struct ResultHandler {
    /// Column names for the result.
    names: Vec<String>,
    /// Column types for the result.
    types: Vec<LogicalType>,
    /// Pre-allocated output chunk (reused across fetches).
    output_chunk: Chunk,
    /// Allocator for memory management.
    allocator: Arc<dyn Allocator>,
    /// Shared buffer with pipeline sink.
    buffer: Arc<Mutex<BufferedData>>,
    /// Event coordinator for pipeline execution.
    coordinator: Arc<EventCoordinator>,
    /// Statement-scoped cancellation state shared with the session.
    cancellation: StatementCancellation,
    /// Whether the handler is closed.
    closed: bool,
}

impl std::fmt::Debug for ResultHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResultHandler")
            .field("names", &self.names)
            .field("types", &self.types)
            .field("closed", &self.closed)
            .finish()
    }
}

impl ResultHandler {
    /// Create a result handler backed by a shared output buffer.
    pub fn new(
        names: Vec<String>,
        types: Vec<LogicalType>,
        buffer: Arc<Mutex<BufferedData>>,
        coordinator: Arc<EventCoordinator>,
        cancellation: StatementCancellation,
        allocator: Arc<dyn Allocator>,
    ) -> Self {
        let output_chunk = if types.is_empty() {
            Chunk::with_allocator(allocator.clone())
        } else {
            Chunk::initialize_with_allocator(&types, VECTOR_SIZE, allocator.clone())
        };

        Self {
            names,
            types,
            output_chunk,
            allocator,
            buffer,
            coordinator,
            cancellation,
            closed: false,
        }
    }

    /// Create an empty ResultHandler (for DDL/DML that return no rows).
    pub fn empty(allocator: Arc<dyn Allocator>) -> Self {
        let buffer = Arc::new(Mutex::new(BufferedData::new(1, allocator.clone())));
        buffer.lock().unwrap().close();

        let dummy_scheduler = Arc::new(TaskScheduler::new());
        let coordinator = Arc::new(EventCoordinator::new(dummy_scheduler));

        Self {
            names: Vec::new(),
            types: Vec::new(),
            output_chunk: Chunk::with_allocator(allocator.clone()),
            allocator,
            buffer,
            coordinator,
            cancellation: StatementCancellation::new(
                tokio_util::sync::CancellationToken::new(),
                None,
            ),
            closed: true,
        }
    }

    /// Fetch the next chunk of results.
    pub fn fetch(&mut self) -> Result<Option<&Chunk>> {
        if self.closed {
            return Ok(None);
        }

        // Replenish buffer by driving execution
        let execution_result = self.replenish_buffer()?;

        // Check execution result
        match execution_result {
            StreamExecutionResult::ChunkReady | StreamExecutionResult::Finished => {
                // Try to get a chunk from the buffer
                let mut buffer_guard = self.buffer.lock().map_err(|e| {
                    paro_common::error::internal(format!("Failed to lock buffer: {}", e))
                })?;

                if let Some(chunk) = buffer_guard.scan() {
                    drop(buffer_guard);
                    self.output_chunk = chunk;
                    if self.output_chunk.size() == 0 {
                        // Skip empty chunks, try again
                        return self.fetch();
                    }
                    return Ok(Some(&self.output_chunk));
                }

                // No chunk available and execution finished
                drop(buffer_guard);
                self.closed = true;
                Ok(None)
            }
            StreamExecutionResult::Cancelled => {
                self.closed = true;
                self.cancellation.check()?;
                Ok(None)
            }
            StreamExecutionResult::Error => {
                self.closed = true;
                // Try to get error from buffer first
                let buffer_guard = self.buffer.lock().map_err(|e| {
                    paro_common::error::internal(format!("Failed to lock buffer: {}", e))
                })?;
                if let Some(error) = buffer_guard.error() {
                    let error_msg = format!("{:?}", error);
                    drop(buffer_guard);
                    return Err(paro_common::error::internal(error_msg));
                }
                drop(buffer_guard);

                // If no error in buffer, try coordinator-local activation/finish
                // errors first, then fall back to scheduler-global task errors.
                if let Some(error) = self.coordinator.get_error() {
                    return Err(error);
                }
                if let Some(producer) = self.coordinator.producer_token() {
                    if let Some(error) = self
                        .coordinator
                        .scheduler()
                        .get_error_for_producer(&producer)
                    {
                        return Err(paro_common::error::internal(error.message));
                    }
                } else if let Some(error) = self.coordinator.scheduler().get_error() {
                    return Err(paro_common::error::internal(error.message));
                }

                // Last resort: unknown error
                Err(paro_common::error::internal(
                    "Unknown execution error".to_string(),
                ))
            }
            StreamExecutionResult::Blocked => {
                // This shouldn't happen in replenish_buffer, but handle it
                self.closed = true;
                Err(paro_common::error::internal(
                    "Unexpected blocked state".to_string(),
                ))
            }
            StreamExecutionResult::Running => {
                unreachable!("replenish_buffer should keep polling instead of returning Running")
            }
        }
    }

    /// Replenish the buffer by driving pipeline execution.
    ///
    /// - Executes tasks until a chunk is ready or execution finishes
    /// - Uses condition variables for efficient waiting (no busy-waiting)
    /// - Unblocks sinks when buffer has space
    /// - Returns the execution result status
    ///
    /// # Returns
    ///
    /// * `Ok(StreamExecutionResult)` - The execution status
    /// * `Err(ParoError)` - An error occurred
    fn replenish_buffer(&mut self) -> Result<StreamExecutionResult> {
        loop {
            if self.coordinator.is_cancelled() || self.cancellation.is_cancelled() {
                return Ok(StreamExecutionResult::Cancelled);
            }

            // Check for errors first
            if self.coordinator.has_error() {
                return Ok(StreamExecutionResult::Error);
            }

            // Execute a task and check the result
            let execution_result = self.execute_task_internal()?;

            // Check if we're done or have a chunk ready
            if Self::is_chunk_ready(execution_result) {
                return Ok(execution_result);
            }

            // If blocked, unblock sinks and wait for tasks
            if execution_result == StreamExecutionResult::Blocked {
                // Unblock sinks that might be waiting for buffer space.
                {
                    let mut buffer_guard = self.buffer.lock().map_err(|e| {
                        paro_common::error::internal(format!("Failed to lock buffer: {}", e))
                    })?;
                    buffer_guard.unblock_sinks();
                }

                // Wait for tasks to complete using a condition variable.
                self.wait_for_task();
            }
        }
    }

    /// Wait for a task to become ready.
    ///
    /// Uses condition variable instead of busy-waiting.
    fn wait_for_task(&self) {
        let _ = self.coordinator.wait_for_task();
    }

    /// Execute a single task and return the execution status.
    ///
    ///
    /// # Returns
    ///
    /// * `Ok(StreamExecutionResult)` - The execution status
    /// * `Err(ParoError)` - An error occurred
    fn execute_task_internal(&mut self) -> Result<StreamExecutionResult> {
        if self.coordinator.is_cancelled() || self.cancellation.is_cancelled() {
            return Ok(StreamExecutionResult::Cancelled);
        }

        // Check for errors first
        {
            let buffer_guard = self.buffer.lock().map_err(|e| {
                paro_common::error::internal(format!("Failed to lock buffer: {}", e))
            })?;

            if let Some(_error) = buffer_guard.error() {
                return Ok(StreamExecutionResult::Error);
            }

            // Check if buffer has data
            if !buffer_guard.is_empty() {
                return Ok(StreamExecutionResult::ChunkReady);
            }

            // Check if buffer is closed (execution finished)
            if buffer_guard.is_closed() {
                return Ok(StreamExecutionResult::Finished);
            }
        }

        // Check if coordinator is complete
        if self.coordinator.is_complete() {
            // Coordinator finished, check buffer one more time
            let mut buffer_guard = self.buffer.lock().map_err(|e| {
                paro_common::error::internal(format!("Failed to lock buffer: {}", e))
            })?;

            if !buffer_guard.is_empty() {
                return Ok(StreamExecutionResult::ChunkReady);
            }

            // Close buffer if not already closed
            if !buffer_guard.is_closed() {
                buffer_guard.close();
            }

            return Ok(StreamExecutionResult::Finished);
        }

        // Execute some tasks to drive execution
        if self.coordinator.is_cancelled() || self.cancellation.is_cancelled() {
            return Ok(StreamExecutionResult::Cancelled);
        }
        let tasks_executed = self.coordinator.execute_some_tasks(10);

        // Check buffer again after executing tasks
        {
            let buffer_guard = self.buffer.lock().map_err(|e| {
                paro_common::error::internal(format!("Failed to lock buffer: {}", e))
            })?;

            if !buffer_guard.is_empty() {
                return Ok(StreamExecutionResult::ChunkReady);
            }
        }

        if tasks_executed == 0 {
            Ok(StreamExecutionResult::Blocked)
        } else {
            Ok(StreamExecutionResult::Running)
        }
    }

    /// Check if the execution result indicates a chunk is ready or execution is complete.
    ///
    fn is_chunk_ready(result: StreamExecutionResult) -> bool {
        matches!(
            result,
            StreamExecutionResult::ChunkReady
                | StreamExecutionResult::Finished
                | StreamExecutionResult::Cancelled
                | StreamExecutionResult::Error
        )
    }

    /// Check if the handler is still open.
    pub fn is_open(&self) -> bool {
        !self.closed
    }

    /// Close the handler and release resources.
    ///
    /// This method:
    /// 1. Cancels pipeline execution
    /// 2. Closes the buffer
    /// 3. Marks the handler as closed
    pub fn close(&mut self) {
        if self.closed {
            return;
        }

        self.closed = true;

        // Cancel execution
        self.coordinator.cancel();

        // Close buffer
        if let Ok(mut buffer_guard) = self.buffer.lock() {
            buffer_guard.close();
        }
    }

    /// Get the column names.
    pub fn names(&self) -> &[String] {
        &self.names
    }

    /// Get the column types.
    pub fn types(&self) -> &[LogicalType] {
        &self.types
    }

    /// Get the allocator used by this handler.
    pub fn allocator(&self) -> &Arc<dyn Allocator> {
        &self.allocator
    }

    /// Get the number of columns.
    pub fn column_count(&self) -> usize {
        self.types.len()
    }

    /// Get the event coordinator.
    pub fn coordinator(&self) -> &Arc<EventCoordinator> {
        &self.coordinator
    }

    /// Get the shared buffer.
    pub fn buffer(&self) -> &Arc<Mutex<BufferedData>> {
        &self.buffer
    }

    /// Create a ResultHandler for testing with pre-materialized chunks.
    ///
    /// **Note**: This is only for testing. Production code should use streaming execution.
    pub fn from_materialized_for_test(
        names: Vec<String>,
        types: Vec<LogicalType>,
        chunks: Vec<Chunk>,
        allocator: Arc<dyn Allocator>,
    ) -> Self {
        // Create a buffer and fill it with chunks
        let buffer = Arc::new(Mutex::new(BufferedData::new(
            chunks.len() + 1,
            allocator.clone(),
        )));

        {
            let mut buffer_guard = buffer.lock().unwrap();
            for chunk in chunks {
                assert!(
                    matches!(
                        buffer_guard.try_append(chunk),
                        crate::operator::result::buffered_data::AppendResult::Success
                    ),
                    "test buffer should accept materialized chunks"
                );
            }
            buffer_guard.close();
        }

        // Create a dummy coordinator (already finished)
        let dummy_scheduler = Arc::new(TaskScheduler::new());
        let coordinator = Arc::new(EventCoordinator::new(dummy_scheduler));

        Self::new(
            names,
            types,
            buffer,
            coordinator,
            StatementCancellation::new(tokio_util::sync::CancellationToken::new(), None),
            allocator,
        )
    }
}

impl Drop for ResultHandler {
    fn drop(&mut self) {
        self.close();
    }
}

/// Execution result for streaming.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamExecutionResult {
    /// Chunk is ready to be fetched.
    ChunkReady,
    /// Execution made progress and should keep polling without sleeping.
    Running,
    /// Execution is blocked, waiting for tasks.
    Blocked,
    /// Execution finished successfully.
    Finished,
    /// Execution was cancelled.
    Cancelled,
    /// An error occurred during execution.
    Error,
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_common::allocator::default_allocator;
    use paro_context::{NoopStatementTimeoutDriver, StatementCancelReason};
    use paro_scheduler::event::Event;
    use std::sync::OnceLock;
    use std::time::{Duration, Instant};
    use tokio_util::sync::CancellationToken;

    #[test]
    fn test_result_handler_empty() {
        let allocator = Arc::new(default_allocator().clone());
        let handler = ResultHandler::empty(allocator);

        assert!(handler.closed);
        assert_eq!(handler.names().len(), 0);
        assert_eq!(handler.types().len(), 0);
    }

    #[test]
    fn test_result_handler_from_materialized() {
        let allocator = Arc::new(default_allocator().clone());
        let names = vec!["col1".to_string()];
        let types = vec![LogicalType::Integer];

        let mut handler =
            ResultHandler::from_materialized_for_test(names, types, vec![], allocator);

        // Should return None since no chunks were provided
        let result = handler.fetch();
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn test_wait_for_task_integration() {
        // This test verifies that wait_for_task() doesn't panic
        let allocator = Arc::new(default_allocator().clone());
        let buffer = Arc::new(Mutex::new(BufferedData::new(10, allocator.clone())));
        let scheduler = Arc::new(TaskScheduler::new());
        let coordinator = Arc::new(EventCoordinator::new(scheduler));

        let handler = ResultHandler::new(
            vec!["col1".to_string()],
            vec![LogicalType::Integer],
            buffer,
            coordinator,
            StatementCancellation::new(tokio_util::sync::CancellationToken::new(), None),
            allocator,
        );

        // Call wait_for_task - should timeout gracefully
        handler.wait_for_task();
    }

    #[test]
    fn blocked_fetch_returns_promptly_after_cancellation() {
        let allocator = Arc::new(default_allocator().clone());
        let buffer = Arc::new(Mutex::new(BufferedData::new(10, allocator.clone())));
        let scheduler = Arc::new(TaskScheduler::new());
        let coordinator = Arc::new(EventCoordinator::new(scheduler));
        coordinator.add_event(Event::new());

        let connection_token = CancellationToken::new();
        let statement_token = connection_token.child_token();
        let cancel_reason = Arc::new(OnceLock::new());
        let cancellation = StatementCancellation::from_parts(
            connection_token,
            statement_token.clone(),
            None,
            cancel_reason.clone(),
            Arc::new(NoopStatementTimeoutDriver),
        );

        let mut handler = ResultHandler::new(
            vec!["col1".to_string()],
            vec![LogicalType::Integer],
            buffer,
            coordinator.clone(),
            cancellation,
            allocator,
        );

        let cancel_thread = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(20));
            let _ = cancel_reason.set(StatementCancelReason::UserRequest);
            statement_token.cancel();
            coordinator.cancel();
        });

        let started = Instant::now();
        let err = handler
            .fetch()
            .expect_err("fetch should surface query cancellation");
        cancel_thread.join().expect("cancel thread should join");

        assert!(
            started.elapsed() < Duration::from_millis(500),
            "blocked fetch should wake promptly after cancellation"
        );
        assert!(err
            .message()
            .contains("canceling statement due to user request"));
    }
}
