//! Bound Column Reference Expression
//!
//!

use crate::operator::ColumnBinding;
use paro_common::types::LogicalType;

/// A reference to a column in a bound table.
///
/// The binding field directly stores the table_index and column_index.
#[derive(Debug, Clone)]
pub struct ColumnRefExpression {
    /// Column binding (table_index, column_index)
    pub binding: ColumnBinding,

    /// Resulting logical type of the column.
    pub return_type: LogicalType,

    /// The subquery depth (i.e. depth 0 = current query, depth 1 = parent query, etc.).
    /// This is only non-zero for correlated expressions inside subqueries.
    pub depth: usize,
}

impl ColumnRefExpression {
    /// Create a new ColumnRefExpression.
    pub fn new(binding: ColumnBinding, return_type: LogicalType) -> Self {
        Self {
            binding,
            return_type,
            depth: 0,
        }
    }

    /// Create a new ColumnRefExpression with depth for correlated subqueries.
    pub fn with_depth(binding: ColumnBinding, return_type: LogicalType, depth: usize) -> Self {
        Self {
            binding,
            return_type,
            depth,
        }
    }
}
