// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Streaming query results used by the extended-query path.

use paro_common::chunk::Chunk;
use paro_common::error::Result;
use paro_common::types::LogicalType;
use paro_execution::query_executor::stream::ResultHandler;

/// Streaming query result that releases memory as chunks are consumed.
///
/// This wraps a `ResultHandler` from the execution layer, providing
/// on-demand fetching of result chunks. Memory is released back to
/// the BufferPool as each chunk is consumed.
///
/// Note: For Simple Query, results are pushed directly to a `ResultSink`.
/// This type is primarily used for Extended Query (Prepared Statements)
/// and internal library APIs.
pub struct QueryResult {
    /// Column names for the result.
    pub names: Vec<String>,
    /// Column types for the result.
    pub types: Vec<LogicalType>,
    /// The underlying result handler.
    pub stream: ResultHandler,
    /// Number of rows affected (for INSERT/UPDATE/DELETE).
    pub rows_affected: usize,
}

impl std::fmt::Debug for QueryResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryResult")
            .field("names", &self.names)
            .field("types", &self.types)
            .field("rows_affected", &self.rows_affected)
            .finish()
    }
}

impl QueryResult {
    /// Create a new streaming query result.
    pub fn new(names: Vec<String>, types: Vec<LogicalType>, stream: ResultHandler) -> Self {
        Self {
            names,
            types,
            stream,
            rows_affected: 0,
        }
    }

    /// Create a result for rows affected (INSERT/UPDATE/DELETE).
    pub fn rows_changed(
        count: usize,
        allocator: std::sync::Arc<dyn paro_common::allocator::Allocator>,
    ) -> paro_common::error::Result<Self> {
        Ok(Self {
            names: Vec::new(),
            types: Vec::new(),
            stream: ResultHandler::empty(allocator)?,
            rows_affected: count,
        })
    }

    /// Create an empty result.
    pub fn empty(
        allocator: std::sync::Arc<dyn paro_common::allocator::Allocator>,
    ) -> paro_common::error::Result<Self> {
        Ok(Self {
            names: Vec::new(),
            types: Vec::new(),
            stream: ResultHandler::empty(allocator)?,
            rows_affected: 0,
        })
    }

    /// Get the number of columns.
    pub fn column_count(&self) -> usize {
        self.names.len()
    }

    /// Check if this is a query result (has columns).
    pub fn is_query(&self) -> bool {
        !self.names.is_empty()
    }

    /// Check if the stream is still open.
    pub fn is_open(&self) -> bool {
        self.stream.is_open()
    }

    /// Materialize all chunks from the stream.
    ///
    /// This consumes the underlying stream and collects all chunks into a vector.
    /// Primarily used for testing and CLI display.
    pub fn collect_all(&mut self) -> Result<Vec<Chunk>> {
        let mut chunks = Vec::new();
        while let Some(chunk) = self.stream.fetch()? {
            chunks.push(chunk.try_deep_copy(chunk.allocator().clone())?);
        }
        Ok(chunks)
    }

    /// Convert the streaming result to a simple row-based string representation.
    ///
    /// This consumes the underlying stream.
    pub fn fetch_to_string_rows(&mut self) -> Result<Vec<Vec<String>>> {
        use paro_common::runtime_value::Value;

        let mut rows = Vec::new();
        while let Some(chunk) = self.stream.fetch()? {
            for row_idx in 0..chunk.size() {
                let mut row = Vec::new();
                for col_idx in 0..chunk.column_count() {
                    if let Some(vector) = chunk.column(col_idx) {
                        let value = vector.get_value(row_idx);
                        match value {
                            Value::Varchar(s) => row.push(s),
                            _ => row.push(value.to_string()),
                        }
                    } else {
                        row.push("NULL".to_string());
                    }
                }
                rows.push(row);
            }
        }
        Ok(rows)
    }

    /// Convert the streaming result to a formatted table string.
    ///
    /// This consumes the underlying stream.
    pub fn fetch_to_table_string(&mut self) -> Result<String> {
        if self.names.is_empty() {
            return Ok(String::new());
        }

        let rows = self.fetch_to_string_rows()?;

        // Calculate column widths
        let mut widths: Vec<usize> = self.names.iter().map(|n| n.len()).collect();
        for row in &rows {
            for (i, cell) in row.iter().enumerate() {
                if i < widths.len() {
                    widths[i] = widths[i].max(cell.len());
                }
            }
        }

        let mut result = String::new();

        // Header
        for (i, name) in self.names.iter().enumerate() {
            if i > 0 {
                result.push_str(" | ");
            }
            result.push_str(&format!("{:width$}", name, width = widths[i]));
        }
        result.push('\n');

        // Separator
        for (i, width) in widths.iter().enumerate() {
            if i > 0 {
                result.push_str("-+-");
            }
            result.push_str(&"-".repeat(*width));
        }
        result.push('\n');

        // Data rows
        for row in &rows {
            for (i, cell) in row.iter().enumerate() {
                if i > 0 {
                    result.push_str(" | ");
                }
                if i < widths.len() {
                    result.push_str(&format!("{:width$}", cell, width = widths[i]));
                }
            }
            result.push('\n');
        }

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_common::allocator::default_allocator;
    use std::sync::Arc;

    #[test]
    fn test_empty_result() {
        let allocator = Arc::new(default_allocator().clone());
        let mut result = QueryResult::empty(allocator).unwrap();
        assert!(result.is_query() == false);
        assert_eq!(result.column_count(), 0);
        assert!(result.collect_all().unwrap().is_empty());
    }
}
