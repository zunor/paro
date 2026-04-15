// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! ResultSink Trait
//!
//! This module defines the `ResultSink` trait, which is the core abstraction for
//! outputting execution results from Session to protocol layers or test collectors.
//!
//! # Design Overview
//!
//! ResultSink is a push-based interface where the Session pushes results to the sink,
//! rather than the sink pulling results. This simplifies the execution model and
//! allows for better separation of concerns:
//!
//! - **Session**: Responsible for parsing, compiling, executing statements and
//!   driving the result stream. It knows when to start/finish results and what
//!   command tags to use.
//! - **ResultSink**: Responsible for "how to present results" - encoding to wire
//!   protocol, collecting for tests, formatting for CLI, etc.
//!
//! # Lifecycle
//!
//! ResultSink is NOT held by Session. It is passed as a parameter to execution methods.
//! This avoids lifetime conflicts since Session is long-lived (entire connection),
//! while ResultSink (especially server-side protocol sinks) holds socket references with shorter
//! lifetimes.
//!
//! # Call Sequence
//!
//! For each statement in a Simple Query batch:
//!
//! ## Statements with result sets (SELECT, SHOW, etc.):
//! ```text
//! sink.start_result(names, types)   // RowDescription
//! sink.push_chunk(chunk)            // DataRow (repeated)
//! sink.push_chunk(chunk)            // DataRow (repeated)
//! sink.finish_result(completion)    // CommandComplete
//! ```
//!
//! ## Statements without result sets (INSERT, CREATE, etc.):
//! ```text
//! sink.finish_result(completion)    // CommandComplete only
//! ```
//!
//! ## On error:
//! ```text
//! sink.error(err)                   // ErrorResponse
//! ```
//!
//! # Example Implementation
//!
//! ```ignore
//! struct CollectingSink {
//!     results: Vec<CollectedResult>,
//! }
//!
//! #[async_trait]
//! impl ResultSink for CollectingSink {
//!     async fn start_result(&mut self, names: &[String], types: &[LogicalType]) -> Result<()> {
//!         self.results.push(CollectedResult::new(names, types));
//!         Ok(())
//!     }
//!     // ... other methods
//! }
//! ```
//!
//! # References
//!
//! - Design document: `DESIGN-session-execution-v2.md`
//! - PostgreSQL Simple Query: Section "Extended Query" and "Simple Query" in protocol docs

use async_trait::async_trait;
use paro_common::chunk::Chunk;
use paro_common::error::{ParoError, Result};
use paro_common::types::LogicalType;

use crate::completion::StatementCompletion;

/// Trait for receiving execution results from Session.
///
/// This is the primary interface between the Session execution layer and
/// the result presentation layer (protocol, tests, CLI).
///
/// # Implementors
///
/// - `PgWireResultSink`: Encodes results to PostgreSQL wire protocol messages
/// - `CollectingSink`: Collects results for testing
/// - Future: CLI sink, HTTP response sink, etc.
#[async_trait]
pub trait ResultSink: Send {
    /// Called when a statement starts producing a result set.
    ///
    /// This is called for statements that return rows (SELECT, SHOW, EXPLAIN, etc.).
    /// For statements that don't return rows (INSERT, CREATE, etc.), this is NOT called.
    ///
    /// # Arguments
    ///
    /// * `names` - Column names for the result set
    /// * `types` - Column types for the result set
    ///
    /// # Protocol Mapping
    ///
    /// For pgwire: Sends `RowDescription` message
    async fn start_result(&mut self, names: &[String], types: &[LogicalType]) -> Result<()>;

    /// Called for each chunk of data in the result set.
    ///
    /// This is called zero or more times between `start_result` and `finish_result`.
    /// Each chunk may contain multiple rows of data.
    ///
    /// # Arguments
    ///
    /// * `chunk` - A chunk of rows to output
    ///
    /// # Protocol Mapping
    ///
    /// For pgwire: Sends one `DataRow` message per row in the chunk
    async fn push_chunk(&mut self, chunk: &Chunk) -> Result<()>;

    /// Called when a statement completes execution.
    ///
    /// This is called for ALL statements, whether or not they have result sets.
    ///
    /// # Arguments
    ///
    /// * `completion` - The completion semantic for the executed statement
    ///
    /// # Protocol Mapping
    ///
    /// For pgwire: Sends `CommandComplete` message with tag and row count
    async fn finish_result(&mut self, completion: &StatementCompletion) -> Result<()>;

    /// Called when an error occurs during execution.
    ///
    /// This is called instead of or after `finish_result` when an error occurs.
    /// The default implementation does nothing - implementations should override
    /// this to send error responses.
    ///
    /// # Arguments
    ///
    /// * `err` - The error that occurred
    ///
    /// # Protocol Mapping
    ///
    /// For pgwire: Sends `ErrorResponse` message
    async fn error(&mut self, _err: &ParoError) -> Result<()> {
        Ok(())
    }
}
