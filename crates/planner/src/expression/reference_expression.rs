//! Bound Reference Expression
//!
//!

use paro_common::types::LogicalType;

/// A reference to a column in a Chunk by index.
/// Used in the physical layer.
#[derive(Debug, Clone)]
pub struct ReferenceExpression {
    /// Index of the column within the Chunk.
    pub index: usize,
    /// Resulting logical type of the column.
    pub return_type: LogicalType,
}

impl ReferenceExpression {
    pub fn new(index: usize, return_type: LogicalType) -> Self {
        Self { index, return_type }
    }
}
