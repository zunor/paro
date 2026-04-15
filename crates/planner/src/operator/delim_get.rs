// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Logical Delim Get Operator
//!
//! Represents a duplicate-eliminated scan owned by a delim/dependent join.

use paro_common::types::LogicalType;

/// DelimGet represents a duplicate-eliminated scan belonging to a DelimJoin.
#[derive(Debug, Clone)]
pub struct DelimGet {
    /// The table index in the current bind context.
    pub table_index: usize,
    /// The types of the chunk produced by this delim scan.
    pub chunk_types: Vec<LogicalType>,
}

impl DelimGet {
    pub fn new(table_index: usize, chunk_types: Vec<LogicalType>) -> Self {
        Self {
            table_index,
            chunk_types,
        }
    }

    pub fn get_types(&self) -> Vec<LogicalType> {
        self.chunk_types.clone()
    }

    pub fn name(&self) -> &'static str {
        "DELIM_GET"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delim_get_preserves_table_index_and_types() {
        let op = DelimGet::new(42, vec![LogicalType::Integer, LogicalType::Boolean]);
        assert_eq!(op.table_index, 42);
        assert_eq!(
            op.get_types(),
            vec![LogicalType::Integer, LogicalType::Boolean]
        );
        assert_eq!(op.name(), "DELIM_GET");
    }
}
