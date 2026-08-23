// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Streaming result handling for typed query execution.

mod background_output;
mod completed_output;
mod typed_streaming;

use std::sync::Arc;

use paro_common::allocator::Allocator;
use paro_common::chunk::Chunk;
use paro_common::error::Result;
use paro_common::types::LogicalType;
use paro_common::vector::VECTOR_SIZE;
use paro_context::StatementCancellation;

use crate::memory_runtime::QueryMemoryPool;
use crate::query_executor::pipeline_driver::PipelineExecutionDriver;
use crate::query_executor::program_executor::{BackgroundExecutionDriver, ProgramExecution};
use crate::runtime::{CleanupReason, QueryOutputPort, QueryRuntimeContext};

enum ResultOutput {
    Closed,
    Completed(QueryOutputPort),
    FetchDriven {
        query: QueryRuntimeContext,
        output: QueryOutputPort,
        driver: PipelineExecutionDriver,
    },
    Background {
        output: QueryOutputPort,
        driver: BackgroundExecutionDriver,
    },
}

/// Non-blocking observation of a result stream.
pub enum FetchState<'a> {
    Ready(&'a Chunk),
    Pending,
    Exhausted,
}

/// Result handler for query execution.
///
/// Completed-output queries drain an already-populated typed output port.
/// Fetch-driven queries advance the typed pipeline scheduler on demand as the
/// client consumes chunks.
pub struct ResultHandler {
    /// Column names for the result.
    names: Vec<String>,
    /// Column types for the result.
    types: Vec<LogicalType>,
    /// Pre-allocated output chunk (reused across fetches).
    output_chunk: Chunk,
    /// Allocator for memory management.
    allocator: Arc<dyn Allocator>,
    /// Active typed output source.
    output: ResultOutput,
    /// Statement-scoped cancellation state shared with the session.
    cancellation: StatementCancellation,
    /// Active query pool registration, detached when execution is closed.
    query_memory_pool: Option<Arc<QueryMemoryPool>>,
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
    /// Create an empty ResultHandler (for DDL/DML that return no rows).
    pub fn empty(allocator: Arc<dyn Allocator>) -> Result<Self> {
        Ok(Self {
            names: Vec::new(),
            types: Vec::new(),
            output_chunk: Chunk::try_new(allocator.clone())?,
            allocator,
            output: ResultOutput::Closed,
            cancellation: StatementCancellation::new(
                tokio_util::sync::CancellationToken::new(),
                None,
            ),
            query_memory_pool: None,
            closed: true,
        })
    }

    /// Create a handler over typed program execution.
    pub fn from_program_execution(
        names: Vec<String>,
        types: Vec<LogicalType>,
        execution: ProgramExecution,
        allocator: Arc<dyn Allocator>,
        query_memory_pool: Option<Arc<QueryMemoryPool>>,
    ) -> Result<Self> {
        let output_chunk = Self::new_output_chunk(&types, allocator.clone())?;

        let cancellation = execution.query.cancellation.clone();
        let output = if let Some(driver) = execution.driver {
            ResultOutput::FetchDriven {
                output: execution.query.output.clone(),
                query: execution.query,
                driver,
            }
        } else if let Some(driver) = execution.background {
            ResultOutput::Background {
                output: execution.query.output.clone(),
                driver,
            }
        } else {
            ResultOutput::Completed(execution.query.output)
        };

        Ok(Self {
            names,
            types,
            output_chunk,
            allocator,
            output,
            cancellation,
            query_memory_pool,
            closed: false,
        })
    }

    fn new_output_chunk(types: &[LogicalType], allocator: Arc<dyn Allocator>) -> Result<Chunk> {
        if types.is_empty() {
            Chunk::try_new(allocator)
        } else {
            Chunk::try_initialize(types, VECTOR_SIZE, allocator)
        }
    }

    /// Fetch the next chunk of results.
    #[inline]
    pub fn fetch(&mut self) -> Result<Option<&Chunk>> {
        if self.closed {
            return Ok(None);
        }

        match &self.output {
            ResultOutput::Completed(_) => self.fetch_completed_output(),
            ResultOutput::FetchDriven { .. } => self.fetch_typed_streaming_output(),
            ResultOutput::Background { .. } => self.fetch_background_output(),
            ResultOutput::Closed => {
                self.mark_closed();
                Err(paro_common::error::internal(
                    "result handler has no output source",
                ))
            }
        }
    }

    /// Fetch without parking the calling thread on a background driver.
    ///
    /// Async front ends use this to keep receiving bounded COPY input while a
    /// blocking execution worker consumes it. Other result modes retain their
    /// existing fetch-driven behavior.
    pub fn try_fetch(&mut self) -> Result<FetchState<'_>> {
        if !matches!(self.output, ResultOutput::Background { .. }) {
            return Ok(match self.fetch()? {
                Some(chunk) => FetchState::Ready(chunk),
                None => FetchState::Exhausted,
            });
        }

        loop {
            if self.cancellation.is_cancelled() {
                let _ = self.cleanup_typed_driver(CleanupReason::Cancelled(
                    self.cancellation
                        .reason()
                        .unwrap_or(paro_context::StatementCancelReason::UserRequest),
                ));
                self.mark_closed();
                self.cancellation.check()?;
                return Ok(FetchState::Exhausted);
            }

            if let Some(chunk) = self.pop_background_output_chunk()? {
                self.output_chunk = chunk;
                if self.output_chunk.size() != 0 {
                    return Ok(FetchState::Ready(&self.output_chunk));
                }
                continue;
            }

            if self.background_driver_finished()? {
                let result = self.finish_background_driver();
                self.mark_closed();
                result?;
                return Ok(FetchState::Exhausted);
            }
            return Ok(FetchState::Pending);
        }
    }

    pub fn progress_port(&self) -> Option<QueryOutputPort> {
        match &self.output {
            ResultOutput::Background { output, .. } => Some(output.clone()),
            _ => None,
        }
    }

    /// Check if the handler is still open.
    pub fn is_open(&self) -> bool {
        !self.closed
    }

    /// Close the handler and release resources.
    pub fn close(&mut self) {
        if self.closed {
            self.detach_query_memory_pool();
            return;
        }

        if matches!(
            self.output,
            ResultOutput::FetchDriven { .. } | ResultOutput::Background { .. }
        ) {
            if let Err(error) = self.cleanup_typed_driver(CleanupReason::Cancelled(
                paro_context::StatementCancelReason::UserRequest,
            )) {
                tracing::warn!(
                    error = %error.message(),
                    "typed result handler cleanup failed during close"
                );
            }
        }
        self.mark_closed();
    }

    fn mark_closed(&mut self) {
        self.closed = true;
        self.output = ResultOutput::Closed;
        self.detach_query_memory_pool();
    }

    fn detach_query_memory_pool(&mut self) {
        if let Some(pool) = self.query_memory_pool.take() {
            pool.detach_registration();
        }
    }

    fn cleanup_typed_driver(&mut self, reason: CleanupReason) -> Result<()> {
        let output = std::mem::replace(&mut self.output, ResultOutput::Closed);
        match output {
            ResultOutput::FetchDriven {
                query, mut driver, ..
            } => driver.cleanup(&query, reason),
            ResultOutput::Background { mut driver, .. } => driver.join(),
            other => {
                self.output = other;
                Ok(())
            }
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
}

impl Drop for ResultHandler {
    fn drop(&mut self) {
        self.close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_common::allocator::default_allocator;
    use tokio_util::sync::CancellationToken;

    fn completed_handler(
        names: Vec<String>,
        types: Vec<LogicalType>,
        chunks: Vec<Chunk>,
        allocator: Arc<dyn Allocator>,
    ) -> ResultHandler {
        let output = QueryOutputPort::unbounded();
        for chunk in chunks {
            assert!(
                matches!(
                    output.try_push(chunk),
                    crate::runtime::QueryOutputWrite::Written
                ),
                "test output port should accept completed-output chunks"
            );
        }
        let output_chunk = ResultHandler::new_output_chunk(&types, allocator.clone())
            .expect("test result handler allocation failed");

        ResultHandler {
            names,
            types,
            output_chunk,
            allocator,
            output: ResultOutput::Completed(output),
            cancellation: StatementCancellation::new(CancellationToken::new(), None),
            query_memory_pool: None,
            closed: false,
        }
    }

    #[test]
    fn test_result_handler_empty() {
        let allocator = Arc::new(default_allocator().clone());
        let handler = ResultHandler::empty(allocator).expect("empty result handler");

        assert!(handler.closed);
        assert_eq!(handler.names().len(), 0);
        assert_eq!(handler.types().len(), 0);
    }

    #[test]
    fn test_result_handler_from_completed_output() {
        let allocator = Arc::new(default_allocator().clone());
        let names = vec!["col1".to_string()];
        let types = vec![LogicalType::Integer];

        let mut handler = completed_handler(names, types, vec![], allocator);

        let result = handler.fetch();
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }
}
