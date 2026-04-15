// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! CollectingSink Implementation
//!
//! A `ResultSink` implementation that collects all results in memory.
//! Primarily used for testing and CLI applications.
//!
//! # Example
//!
//! ```ignore
//! let mut sink = CollectingSink::new();
//! session.execute_simple_query("SELECT 1; SELECT 2", &mut sink).await?;
//!
//! assert_eq!(sink.results().len(), 2);
//! assert_eq!(sink.last_result().unwrap().rows, 1);
//! ```

use async_trait::async_trait;
use paro_common::chunk::Chunk;
use paro_common::error::Result;
use paro_common::types::LogicalType;

use super::sink::ResultSink;
use crate::completion::StatementCompletion;
use crate::copy_protocol::ProtocolResultSink;

/// A collected result from a single statement execution.
///
/// This holds all the data from a statement that returned results,
/// as well as the command tag and row count.
#[derive(Debug, Clone)]
pub struct CollectedResult {
    /// Column names for the result set (empty for non-query statements)
    pub names: Vec<String>,
    /// Column types for the result set (empty for non-query statements)
    pub types: Vec<LogicalType>,
    /// Data chunks containing the actual rows
    pub chunks: Vec<Chunk>,
    /// Completion payload indicating the type of command
    pub completion: StatementCompletion,
    /// Number of rows affected or returned
    pub rows: usize,
}

impl CollectedResult {
    /// Create a new empty collected result.
    pub fn new() -> Self {
        Self {
            names: Vec::new(),
            types: Vec::new(),
            chunks: Vec::new(),
            completion: StatementCompletion::Empty,
            rows: 0,
        }
    }

    /// Create a new collected result with schema information.
    pub fn with_schema(names: Vec<String>, types: Vec<LogicalType>) -> Self {
        Self {
            names,
            types,
            chunks: Vec::new(),
            completion: StatementCompletion::Select { rows: 0 },
            rows: 0,
        }
    }

    /// Check if this result has data (columns and rows).
    pub fn has_data(&self) -> bool {
        !self.names.is_empty()
    }

    /// Get the total number of rows across all chunks.
    pub fn total_rows(&self) -> usize {
        self.chunks.iter().map(|c| c.len()).sum()
    }

    /// Check if this is a query result (has columns).
    pub fn is_query(&self) -> bool {
        !self.names.is_empty()
    }
}

impl Default for CollectedResult {
    fn default() -> Self {
        Self::new()
    }
}

/// A sink that collects all execution results in memory.
///
/// This is primarily used for testing and CLI applications where
/// you need to access all results after execution completes.
///
/// # Thread Safety
///
/// CollectingSink is `Send` and can be passed across threads.
///
/// # Example
///
/// ```ignore
/// let mut sink = CollectingSink::new();
///
/// // Execute with push model
/// sink.start_result(&["id".to_string()], &[LogicalType::Integer]).await?;
/// sink.push_chunk(&chunk).await?;
/// sink.finish_result(&StatementCompletion::Select { rows: 1 }).await?;
///
/// // Access collected results
/// assert_eq!(sink.results().len(), 1);
/// let result = sink.last_result().unwrap();
/// assert_eq!(result.names[0], "id");
/// ```
/// Information about an error that occurred during execution.
#[derive(Debug, Clone)]
pub struct CollectedError {
    /// Error message
    pub message: String,
}

#[derive(Debug, Default)]
pub struct CollectingSink {
    /// Collected results from all executed statements
    results: Vec<CollectedResult>,
    /// Collected errors (if any)
    errors: Vec<CollectedError>,
    /// Flag indicating if we're currently building a result
    building_result: bool,
}

impl CollectingSink {
    /// Create a new empty collecting sink.
    pub fn new() -> Self {
        Self {
            results: Vec::new(),
            errors: Vec::new(),
            building_result: false,
        }
    }

    /// Get all collected results.
    pub fn results(&self) -> &[CollectedResult] {
        &self.results
    }

    /// Get mutable access to collected results.
    pub fn results_mut(&mut self) -> &mut Vec<CollectedResult> {
        &mut self.results
    }

    /// Get the last collected result, if any.
    pub fn last_result(&self) -> Option<&CollectedResult> {
        self.results.last()
    }

    /// Get a mutable reference to the last collected result.
    pub fn last_result_mut(&mut self) -> Option<&mut CollectedResult> {
        self.results.last_mut()
    }

    /// Get all collected errors.
    pub fn errors(&self) -> &[CollectedError] {
        &self.errors
    }

    /// Check if any errors occurred during execution.
    pub fn has_errors(&self) -> bool {
        !self.errors.is_empty()
    }

    /// Get the number of collected results.
    pub fn len(&self) -> usize {
        self.results.len()
    }

    /// Check if no results were collected.
    pub fn is_empty(&self) -> bool {
        self.results.is_empty()
    }

    /// Assert that exactly one result was collected and return it.
    ///
    /// # Panics
    ///
    /// Panics if the number of results is not exactly 1.
    pub fn assert_single_result(&self) -> &CollectedResult {
        assert_eq!(
            self.results.len(),
            1,
            "Expected exactly 1 result, got {}",
            self.results.len()
        );
        &self.results[0]
    }

    /// Take ownership of all collected results.
    pub fn take_results(&mut self) -> Vec<CollectedResult> {
        std::mem::take(&mut self.results)
    }

    /// Take ownership of all collected errors.
    pub fn take_errors(&mut self) -> Vec<CollectedError> {
        std::mem::take(&mut self.errors)
    }

    /// Clear all collected results and errors.
    pub fn clear(&mut self) {
        self.results.clear();
        self.errors.clear();
        self.building_result = false;
    }

    /// Get the total number of rows across all results.
    pub fn total_rows(&self) -> usize {
        self.results.iter().map(|r| r.rows).sum()
    }
}

#[async_trait]
impl ResultSink for CollectingSink {
    async fn start_result(&mut self, names: &[String], types: &[LogicalType]) -> Result<()> {
        self.results
            .push(CollectedResult::with_schema(names.to_vec(), types.to_vec()));
        self.building_result = true;
        Ok(())
    }

    async fn push_chunk(&mut self, chunk: &Chunk) -> Result<()> {
        if let Some(result) = self.results.last_mut() {
            result
                .chunks
                .push(chunk.deep_copy_with_allocator(chunk.allocator().clone()));
        }
        Ok(())
    }

    async fn finish_result(&mut self, completion: &StatementCompletion) -> Result<()> {
        if self.building_result {
            // Complete the current result being built
            if let Some(result) = self.results.last_mut() {
                result.completion = completion.clone();
                result.rows = completion
                    .row_count()
                    .unwrap_or_else(|| result.total_rows());
            }
        } else {
            // Pure command (no start_result was called)
            self.results.push(CollectedResult {
                names: Vec::new(),
                types: Vec::new(),
                chunks: Vec::new(),
                completion: completion.clone(),
                rows: completion.row_count().unwrap_or(0),
            });
        }
        self.building_result = false;
        Ok(())
    }

    async fn error(&mut self, err: &paro_common::error::ParoError) -> Result<()> {
        // Store error information (we can't clone ParoError, so we store the message)
        self.errors.push(CollectedError {
            message: err.to_string(),
        });
        self.building_result = false;
        Ok(())
    }
}

impl ProtocolResultSink for CollectingSink {}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_collecting_sink_new() {
        let sink = CollectingSink::new();
        assert!(sink.is_empty());
        assert_eq!(sink.len(), 0);
        assert!(!sink.has_errors());
    }

    #[tokio::test]
    async fn test_collecting_sink_query_result() {
        let mut sink = CollectingSink::new();

        // Simulate a query result
        sink.start_result(
            &["id".to_string(), "name".to_string()],
            &[LogicalType::Integer, LogicalType::Varchar],
        )
        .await
        .unwrap();

        sink.finish_result(&StatementCompletion::Select { rows: 5 })
            .await
            .unwrap();

        assert_eq!(sink.len(), 1);
        let result = sink.assert_single_result();
        assert_eq!(result.names.len(), 2);
        assert_eq!(result.names[0], "id");
        assert_eq!(result.names[1], "name");
        assert!(matches!(
            result.completion,
            StatementCompletion::Select { rows: 5 }
        ));
        assert_eq!(result.rows, 5);
        assert!(result.is_query());
    }

    #[tokio::test]
    async fn test_collecting_sink_command_result() {
        let mut sink = CollectingSink::new();

        // Simulate a command (INSERT) without start_result
        sink.finish_result(&StatementCompletion::Insert { rows: 3 })
            .await
            .unwrap();

        assert_eq!(sink.len(), 1);
        let result = sink.assert_single_result();
        assert!(result.names.is_empty());
        assert!(!result.is_query());
        assert!(matches!(
            result.completion,
            StatementCompletion::Insert { rows: 3 }
        ));
        assert_eq!(result.rows, 3);
    }

    #[tokio::test]
    async fn test_collecting_sink_multiple_results() {
        let mut sink = CollectingSink::new();

        // First query
        sink.start_result(&["a".to_string()], &[LogicalType::Integer])
            .await
            .unwrap();
        sink.finish_result(&StatementCompletion::Select { rows: 1 })
            .await
            .unwrap();

        // Second command
        sink.finish_result(&StatementCompletion::Insert { rows: 2 })
            .await
            .unwrap();

        // Third query
        sink.start_result(&["b".to_string()], &[LogicalType::Varchar])
            .await
            .unwrap();
        sink.finish_result(&StatementCompletion::Select { rows: 3 })
            .await
            .unwrap();

        assert_eq!(sink.len(), 3);
        assert_eq!(sink.total_rows(), 6); // 1 + 2 + 3

        assert_eq!(sink.results()[0].names[0], "a");
        assert!(sink.results()[1].names.is_empty());
        assert_eq!(sink.results()[2].names[0], "b");
    }

    #[tokio::test]
    async fn test_collecting_sink_clear() {
        let mut sink = CollectingSink::new();

        sink.finish_result(&StatementCompletion::Select { rows: 1 })
            .await
            .unwrap();
        assert_eq!(sink.len(), 1);

        sink.clear();
        assert!(sink.is_empty());
        assert!(!sink.has_errors());
    }

    #[tokio::test]
    async fn test_collecting_sink_take_results() {
        let mut sink = CollectingSink::new();

        sink.finish_result(&StatementCompletion::Select { rows: 1 })
            .await
            .unwrap();
        sink.finish_result(&StatementCompletion::Insert { rows: 2 })
            .await
            .unwrap();

        let results = sink.take_results();
        assert_eq!(results.len(), 2);
        assert!(sink.is_empty()); // Original sink should be empty now
    }

    #[tokio::test]
    async fn test_collected_result_default() {
        let result = CollectedResult::default();
        assert!(result.names.is_empty());
        assert!(result.types.is_empty());
        assert!(result.chunks.is_empty());
        assert!(!result.has_data());
        assert!(!result.is_query());
    }
}
