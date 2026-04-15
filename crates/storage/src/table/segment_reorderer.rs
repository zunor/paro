//! # Segment Reorderer
//!
//! Optimization for ORDER BY and LIMIT by reordering segments based on statistics.

/// Statistics type used for ordering segments.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderByStatistics {
    /// Order by the minimum value in the segment.
    Min,
    /// Order by the maximum value in the segment.
    Max,
}

/// Direction of ordering.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SegmentOrderType {
    /// Ascending order.
    Asc,
    /// Descending order.
    Desc,
}

/// Type of column used for ordering.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderByColumnType {
    /// Numeric column (integer, float, etc.)
    Numeric,
    /// String column (varchar)
    String,
}

/// Options for reordering segments.
#[derive(Debug, Clone)]
pub struct SegmentOrderOptions {
    /// The index of the column to order by.
    pub column_idx: usize,
    /// The statistics type to use for ordering.
    pub order_by: OrderByStatistics,
    /// The ordering direction.
    pub order_type: SegmentOrderType,
    /// The column type.
    pub column_type: OrderByColumnType,
    /// Optional limit on the number of rows to scan.
    pub row_limit: Option<usize>,
    /// Optional offset on the number of rows to skip.
    pub row_offset: usize,
}

impl SegmentOrderOptions {
    /// Create new SegmentOrderOptions.
    pub fn new(
        column_idx: usize,
        order_by: OrderByStatistics,
        order_type: SegmentOrderType,
        column_type: OrderByColumnType,
        row_limit: Option<usize>,
        row_offset: usize,
    ) -> Self {
        Self {
            column_idx,
            order_by,
            order_type,
            column_type,
            row_limit,
            row_offset,
        }
    }
}
