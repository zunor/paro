//! Column Binding
//!
//! Represents a binding to a specific column in a specific table.

/// ColumnBinding represents a binding to a specific column in a specific table (index).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct ColumnBinding {
    pub table_index: usize,
    pub column_index: usize,
}

impl ColumnBinding {
    pub fn new(table_index: usize, column_index: usize) -> Self {
        Self {
            table_index,
            column_index,
        }
    }
}
