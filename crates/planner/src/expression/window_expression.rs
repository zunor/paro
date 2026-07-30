// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Bound Window Expression
//!
//!

use super::Expression;
use paro_common::types::LogicalType;
use paro_function::window::{WindowBoundary, WindowFunction, WindowFunctionType};

/// Window frame type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum WindowFrameType {
    /// ROWS mode - physical row offsets.
    Rows,
    /// RANGE mode - logical value ranges.
    #[default]
    Range,
}

/// Window frame bound.
#[derive(Debug, Clone, Default)]
pub enum WindowFrameBound {
    /// UNBOUNDED PRECEDING/FOLLOWING.
    #[default]
    Unbounded,
    /// CURRENT ROW.
    CurrentRow,
    /// Expression offset (N PRECEDING/FOLLOWING).
    Offset(Box<Expression>),
}

/// Bound window frame specification.
#[derive(Debug, Clone)]
pub struct WindowFrame {
    /// Frame type (ROWS/RANGE/GROUPS).
    pub frame_type: WindowFrameType,
    /// Start boundary.
    pub start_bound: WindowFrameBound,
    /// Whether start is PRECEDING (true) or FOLLOWING (false).
    pub start_is_preceding: bool,
    /// End boundary.
    pub end_bound: WindowFrameBound,
    /// Whether end is PRECEDING (true) or FOLLOWING (false).
    pub end_is_preceding: bool,
}

impl Default for WindowFrame {
    fn default() -> Self {
        Self {
            frame_type: WindowFrameType::Range,
            start_bound: WindowFrameBound::Unbounded,
            start_is_preceding: true,
            end_bound: WindowFrameBound::CurrentRow,
            end_is_preceding: false,
        }
    }
}

/// Bound ORDER BY expression for window functions.
#[derive(Debug, Clone)]
pub struct OrderByExpression {
    /// The expression to order by.
    pub expression: Expression,
    /// Ascending (true) or descending (false).
    pub ascending: bool,
    /// NULLS FIRST (true) or NULLS LAST (false).
    pub nulls_first: bool,
}

/// A bound window expression.
#[derive(Debug, Clone)]
pub struct WindowExpression {
    /// The window function.
    pub function: WindowFunction,
    /// Function arguments.
    pub children: Vec<Expression>,
    /// PARTITION BY expressions.
    pub partitions: Vec<Expression>,
    /// ORDER BY expressions.
    pub orders: Vec<OrderByExpression>,
    /// Window frame specification.
    pub frame: WindowFrame,
    /// Whether to ignore NULLs.
    pub ignore_nulls: bool,
    /// The return type.
    pub return_type: LogicalType,
}

impl WindowExpression {
    pub fn return_type(&self) -> LogicalType {
        self.return_type.clone()
    }

    /// Whether two window expressions can share one physical partition/order layout.
    ///
    /// Function arguments, frames, and NULL treatment may differ without requiring another sort.
    /// Partition and order expressions, including sort direction and NULL placement, must match.
    pub fn has_same_layout(&self, other: &Self) -> bool {
        self.partitions.len() == other.partitions.len()
            && self
                .partitions
                .iter()
                .zip(&other.partitions)
                .all(|(left, right)| left.equals(right))
            && self.orders.len() == other.orders.len()
            && self.orders.iter().zip(&other.orders).all(|(left, right)| {
                left.ascending == right.ascending
                    && left.nulls_first == right.nulls_first
                    && left.expression.equals(&right.expression)
            })
    }
}

impl WindowFrame {
    /// Get the start boundary as WindowBoundary.
    pub fn start_boundary(&self) -> WindowBoundary {
        match (&self.start_bound, self.start_is_preceding, self.frame_type) {
            (WindowFrameBound::Unbounded, true, _) => WindowBoundary::UnboundedPreceding,
            (WindowFrameBound::Unbounded, false, _) => WindowBoundary::UnboundedFollowing,
            (WindowFrameBound::CurrentRow, _, WindowFrameType::Rows) => {
                WindowBoundary::CurrentRowRows
            }
            (WindowFrameBound::CurrentRow, _, WindowFrameType::Range) => {
                WindowBoundary::CurrentRowRange
            }
            (WindowFrameBound::Offset(_), true, WindowFrameType::Rows) => {
                WindowBoundary::ExprPrecedingRows
            }
            (WindowFrameBound::Offset(_), false, WindowFrameType::Rows) => {
                WindowBoundary::ExprFollowingRows
            }
            (WindowFrameBound::Offset(_), true, WindowFrameType::Range) => {
                WindowBoundary::ExprPrecedingRange
            }
            (WindowFrameBound::Offset(_), false, WindowFrameType::Range) => {
                WindowBoundary::ExprFollowingRange
            }
        }
    }

    /// Get the end boundary as WindowBoundary.
    pub fn end_boundary(&self) -> WindowBoundary {
        match (&self.end_bound, self.end_is_preceding, self.frame_type) {
            (WindowFrameBound::Unbounded, true, _) => WindowBoundary::UnboundedPreceding,
            (WindowFrameBound::Unbounded, false, _) => WindowBoundary::UnboundedFollowing,
            (WindowFrameBound::CurrentRow, _, WindowFrameType::Rows) => {
                WindowBoundary::CurrentRowRows
            }
            (WindowFrameBound::CurrentRow, _, WindowFrameType::Range) => {
                WindowBoundary::CurrentRowRange
            }
            (WindowFrameBound::Offset(_), true, WindowFrameType::Rows) => {
                WindowBoundary::ExprPrecedingRows
            }
            (WindowFrameBound::Offset(_), false, WindowFrameType::Rows) => {
                WindowBoundary::ExprFollowingRows
            }
            (WindowFrameBound::Offset(_), true, WindowFrameType::Range) => {
                WindowBoundary::ExprPrecedingRange
            }
            (WindowFrameBound::Offset(_), false, WindowFrameType::Range) => {
                WindowBoundary::ExprFollowingRange
            }
        }
    }

    /// Get default frame for a window function.
    pub fn get_default_frame(func: &WindowFunction) -> WindowFrame {
        match func.function_type {
            WindowFunctionType::RowNumber
            | WindowFunctionType::Rank
            | WindowFunctionType::DenseRank
            | WindowFunctionType::Ntile
            | WindowFunctionType::PercentRank
            | WindowFunctionType::CumeDist
            | WindowFunctionType::Lead
            | WindowFunctionType::Lag => WindowFrame {
                frame_type: WindowFrameType::Rows,
                start_bound: WindowFrameBound::Unbounded,
                start_is_preceding: true,
                end_bound: WindowFrameBound::Unbounded,
                end_is_preceding: false,
            },
            WindowFunctionType::FirstValue
            | WindowFunctionType::LastValue
            | WindowFunctionType::NthValue
            | WindowFunctionType::Aggregate => WindowFrame::default(),
        }
    }
}
