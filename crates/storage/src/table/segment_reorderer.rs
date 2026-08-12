// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # Segment Reorderer
//!
//! Optimization for ORDER BY and LIMIT by reordering segments based on statistics.

use std::cmp::Ordering;

use crate::rowset::{RowsetSharedPtr, SegmentSharedPtr};
use paro_common::runtime_value::Value;

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
#[derive(Debug, Clone, PartialEq, Eq)]
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

/// Reorder visible rowset segments using segment-level min/max statistics.
///
/// This is a scan-order hint only: it never drops segments, because the upper
/// TopN/sort node still owns correctness. Segments without usable statistics
/// are kept after statistics-backed segments in their existing relative order.
pub fn reorder_segments(
    segments: &mut [(RowsetSharedPtr, SegmentSharedPtr)],
    options: &SegmentOrderOptions,
) {
    segments.sort_by(|left, right| {
        let left_key = segment_order_value(&left.1, options);
        let right_key = segment_order_value(&right.1, options);
        match (left_key, right_key) {
            (Some(left), Some(right)) => compare_segment_values(&left, &right, options.order_type),
            (Some(_), None) => Ordering::Less,
            (None, Some(_)) => Ordering::Greater,
            (None, None) => Ordering::Equal,
        }
    });
}

fn segment_order_value(segment: &SegmentSharedPtr, options: &SegmentOrderOptions) -> Option<Value> {
    let column = segment
        .statistics()
        .and_then(|stats| stats.column(options.column_idx as u32))?;
    match options.order_by {
        OrderByStatistics::Min => column.stats.statistics().min_value(),
        OrderByStatistics::Max => column.stats.statistics().max_value(),
    }
}

fn compare_segment_values(left: &Value, right: &Value, order_type: SegmentOrderType) -> Ordering {
    let ordering = left.partial_cmp(right).unwrap_or(Ordering::Equal);
    match order_type {
        SegmentOrderType::Asc => ordering,
        SegmentOrderType::Desc => ordering.reverse(),
    }
}
