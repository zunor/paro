//! # StatementResult and ExecutorConfig
//!
//! Result types for statement execution.

use paro_common::chunk::Chunk;
use paro_common::types::LogicalType;

/// Result of executing a statement.
#[derive(Debug, Default)]
pub enum StatementResult {
    /// Query result with schema and data chunks
    Query {
        names: Vec<String>,
        types: Vec<LogicalType>,
        chunks: Vec<Chunk>,
    },
    /// DML result with number of rows affected
    RowsChanged(usize),
    /// DDL result (success with no data)
    #[default]
    Success,
}

impl StatementResult {
    /// Create a query result.
    pub fn query(names: Vec<String>, types: Vec<LogicalType>, chunks: Vec<Chunk>) -> Self {
        Self::Query {
            names,
            types,
            chunks,
        }
    }

    /// Create a rows changed result.
    pub fn rows_changed(count: usize) -> Self {
        Self::RowsChanged(count)
    }

    /// Create a success result.
    pub fn success() -> Self {
        Self::Success
    }

    /// Get the total number of rows in the result.
    pub fn total_rows(&self) -> usize {
        match self {
            Self::Query { chunks, .. } => chunks.iter().map(|c| c.size()).sum(),
            Self::RowsChanged(n) => *n,
            Self::Success => 0,
        }
    }

    /// Check if this is a query result.
    pub fn is_query(&self) -> bool {
        matches!(self, Self::Query { .. })
    }

    /// Get the column names (for query results).
    pub fn names(&self) -> Option<&Vec<String>> {
        match self {
            Self::Query { names, .. } => Some(names),
            _ => None,
        }
    }

    /// Get the column types (for query results).
    pub fn types(&self) -> Option<&Vec<LogicalType>> {
        match self {
            Self::Query { types, .. } => Some(types),
            _ => None,
        }
    }

    /// Get the data chunks (for query results).
    pub fn chunks(&self) -> Option<&Vec<Chunk>> {
        match self {
            Self::Query { chunks, .. } => Some(chunks),
            _ => None,
        }
    }
}

/// Configuration for the Executor.
#[derive(Debug, Clone)]
pub struct ExecutorConfig {
    /// Maximum number of threads for parallel execution.
    pub max_threads: usize,
}

impl Default for ExecutorConfig {
    fn default() -> Self {
        Self {
            max_threads: num_cpus::get().max(1),
        }
    }
}
