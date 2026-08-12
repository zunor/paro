// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Window Function Module
//!
//!
//!
//! ## Dependencies Check
//! - Chunk: ✅ `paro_common::chunk`
//! - LogicalType: ✅ `paro_common::types`
//! - Vector: ✅ `paro_common::vector`
//!
//! ## Overview
//! Window functions compute values across a set of rows related to the current row.
//! Unlike aggregate functions, window functions do not collapse rows.
//!
//! ## Key Components
//! - `WindowBoundary`: Frame boundary specification (UNBOUNDED, CURRENT ROW, etc.)
//! - `WindowExcludeMode`: EXCLUDE clause handling
//! - `WindowBounds`: Column indices for boundary values
//! - `WindowBoundariesState`: Computes frame boundaries for each row
//! - `WindowExecutor`: Base trait for window function execution
//!
//! ## Window Function Types
//! - Ranking: ROW_NUMBER, RANK, DENSE_RANK, NTILE, PERCENT_RANK, CUME_DIST
//! - Value: LEAD, LAG, FIRST_VALUE, LAST_VALUE, NTH_VALUE
//!
//! Aggregate windows use the aggregate-function kernel directly and are not
//! represented as native [`WindowFunction`] values.

use std::any::Any;
use std::fmt;

use paro_common::chunk::Chunk;
use paro_common::error::Result;
use paro_common::types::LogicalType;
use paro_common::vector::Vector;

// ============================================================================
// Window Boundary Types
// ============================================================================

/// Window frame boundary specification.
///
/// Defines how the window frame is bounded relative to the current row.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum WindowBoundary {
    /// Invalid boundary (uninitialized).
    #[default]
    Invalid,
    /// UNBOUNDED PRECEDING - start of partition.
    UnboundedPreceding,
    /// UNBOUNDED FOLLOWING - end of partition.
    UnboundedFollowing,
    /// CURRENT ROW for RANGE mode.
    CurrentRowRange,
    /// CURRENT ROW for ROWS mode.
    CurrentRowRows,
    /// N PRECEDING for ROWS mode.
    ExprPrecedingRows,
    /// N FOLLOWING for ROWS mode.
    ExprFollowingRows,
    /// N PRECEDING for RANGE mode.
    ExprPrecedingRange,
    /// N FOLLOWING for RANGE mode.
    ExprFollowingRange,
    /// CURRENT ROW for GROUPS mode.
    CurrentRowGroups,
    /// N PRECEDING for GROUPS mode.
    ExprPrecedingGroups,
    /// N FOLLOWING for GROUPS mode.
    ExprFollowingGroups,
}

impl fmt::Display for WindowBoundary {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid => write!(f, "INVALID"),
            Self::UnboundedPreceding => write!(f, "UNBOUNDED PRECEDING"),
            Self::UnboundedFollowing => write!(f, "UNBOUNDED FOLLOWING"),
            Self::CurrentRowRange => write!(f, "CURRENT ROW (RANGE)"),
            Self::CurrentRowRows => write!(f, "CURRENT ROW (ROWS)"),
            Self::ExprPrecedingRows => write!(f, "expr PRECEDING (ROWS)"),
            Self::ExprFollowingRows => write!(f, "expr FOLLOWING (ROWS)"),
            Self::ExprPrecedingRange => write!(f, "expr PRECEDING (RANGE)"),
            Self::ExprFollowingRange => write!(f, "expr FOLLOWING (RANGE)"),
            Self::CurrentRowGroups => write!(f, "CURRENT ROW (GROUPS)"),
            Self::ExprPrecedingGroups => write!(f, "expr PRECEDING (GROUPS)"),
            Self::ExprFollowingGroups => write!(f, "expr FOLLOWING (GROUPS)"),
        }
    }
}

impl WindowBoundary {
    /// Check if this is a ROWS mode boundary.
    pub fn is_rows(&self) -> bool {
        matches!(
            self,
            Self::CurrentRowRows | Self::ExprPrecedingRows | Self::ExprFollowingRows
        )
    }

    /// Check if this is a RANGE mode boundary.
    pub fn is_range(&self) -> bool {
        matches!(
            self,
            Self::CurrentRowRange | Self::ExprPrecedingRange | Self::ExprFollowingRange
        )
    }

    /// Check if this is a GROUPS mode boundary.
    pub fn is_groups(&self) -> bool {
        matches!(
            self,
            Self::CurrentRowGroups | Self::ExprPrecedingGroups | Self::ExprFollowingGroups
        )
    }

    /// Check if this is a PRECEDING boundary.
    pub fn is_preceding(&self) -> bool {
        matches!(
            self,
            Self::UnboundedPreceding
                | Self::ExprPrecedingRows
                | Self::ExprPrecedingRange
                | Self::ExprPrecedingGroups
        )
    }

    /// Check if this is a FOLLOWING boundary.
    pub fn is_following(&self) -> bool {
        matches!(
            self,
            Self::UnboundedFollowing
                | Self::ExprFollowingRows
                | Self::ExprFollowingRange
                | Self::ExprFollowingGroups
        )
    }

    /// Check if this is a CURRENT ROW boundary.
    pub fn is_current_row(&self) -> bool {
        matches!(
            self,
            Self::CurrentRowRange | Self::CurrentRowRows | Self::CurrentRowGroups
        )
    }

    /// Check if this is an UNBOUNDED boundary.
    pub fn is_unbounded(&self) -> bool {
        matches!(self, Self::UnboundedPreceding | Self::UnboundedFollowing)
    }
}

// ============================================================================
// Window Exclude Mode
// ============================================================================

/// Window EXCLUDE clause mode.
///
/// Specifies which rows to exclude from the window frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum WindowExcludeMode {
    /// EXCLUDE NO OTHERS (default) - include all rows in frame.
    #[default]
    NoOther,
    /// EXCLUDE CURRENT ROW - exclude the current row.
    CurrentRow,
    /// EXCLUDE GROUP - exclude current row and its peers.
    Group,
    /// EXCLUDE TIES - exclude peers of current row but not current row itself.
    Ties,
}

impl fmt::Display for WindowExcludeMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoOther => write!(f, "EXCLUDE NO OTHERS"),
            Self::CurrentRow => write!(f, "EXCLUDE CURRENT ROW"),
            Self::Group => write!(f, "EXCLUDE GROUP"),
            Self::Ties => write!(f, "EXCLUDE TIES"),
        }
    }
}

// ============================================================================
// Window Bounds Column Indices
// ============================================================================

/// Column indices for window bounds in the bounds Chunk.
///
/// The bounds chunk contains pre-computed boundary values for efficient access.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum WindowBounds {
    /// Start of the current partition.
    PartitionBegin = 0,
    /// End of the current partition (exclusive).
    PartitionEnd = 1,
    /// Start of the current peer group.
    PeerBegin = 2,
    /// End of the current peer group (exclusive).
    PeerEnd = 3,
    /// Start of valid rows (after IGNORE NULLS filtering).
    ValidBegin = 4,
    /// End of valid rows (exclusive).
    ValidEnd = 5,
    /// Start of the window frame.
    FrameBegin = 6,
    /// End of the window frame (exclusive).
    FrameEnd = 7,
}

impl WindowBounds {
    /// Get the column index as usize.
    pub fn index(&self) -> usize {
        *self as usize
    }

    /// Total number of bounds columns.
    pub const COUNT: usize = 8;
}

// ============================================================================
// Frame Bounds
// ============================================================================

/// A pair of frame boundaries (start, end).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct FrameBounds {
    /// Frame start (inclusive).
    pub start: usize,
    /// Frame end (exclusive).
    pub end: usize,
}

impl FrameBounds {
    /// Create new frame bounds.
    pub fn new(start: usize, end: usize) -> Self {
        Self { start, end }
    }

    /// Check if the frame is empty.
    pub fn is_empty(&self) -> bool {
        self.start >= self.end
    }

    /// Get the frame size.
    pub fn size(&self) -> usize {
        self.end.saturating_sub(self.start)
    }

    /// Check if a row index is within the frame.
    pub fn contains(&self, row_idx: usize) -> bool {
        row_idx >= self.start && row_idx < self.end
    }
}

// ============================================================================
// Window Function Type
// ============================================================================

/// Type of window function.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WindowFunctionType {
    /// ROW_NUMBER() - sequential row number within partition.
    RowNumber,
    /// RANK() - rank with gaps for ties.
    Rank,
    /// DENSE_RANK() - rank without gaps.
    DenseRank,
    /// NTILE(n) - divide partition into n buckets.
    Ntile,
    /// PERCENT_RANK() - relative rank as percentage.
    PercentRank,
    /// CUME_DIST() - cumulative distribution.
    CumeDist,
    /// LEAD(expr, offset, default) - value from following row.
    Lead,
    /// LAG(expr, offset, default) - value from preceding row.
    Lag,
    /// FIRST_VALUE(expr) - first value in frame.
    FirstValue,
    /// LAST_VALUE(expr) - last value in frame.
    LastValue,
    /// NTH_VALUE(expr, n) - nth value in frame.
    NthValue,
}

impl fmt::Display for WindowFunctionType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RowNumber => write!(f, "ROW_NUMBER"),
            Self::Rank => write!(f, "RANK"),
            Self::DenseRank => write!(f, "DENSE_RANK"),
            Self::Ntile => write!(f, "NTILE"),
            Self::PercentRank => write!(f, "PERCENT_RANK"),
            Self::CumeDist => write!(f, "CUME_DIST"),
            Self::Lead => write!(f, "LEAD"),
            Self::Lag => write!(f, "LAG"),
            Self::FirstValue => write!(f, "FIRST_VALUE"),
            Self::LastValue => write!(f, "LAST_VALUE"),
            Self::NthValue => write!(f, "NTH_VALUE"),
        }
    }
}

impl WindowFunctionType {
    /// Check if this is a ranking function.
    pub fn is_ranking(&self) -> bool {
        matches!(
            self,
            Self::RowNumber
                | Self::Rank
                | Self::DenseRank
                | Self::Ntile
                | Self::PercentRank
                | Self::CumeDist
        )
    }

    /// Check if this is a value function.
    pub fn is_value(&self) -> bool {
        matches!(
            self,
            Self::Lead | Self::Lag | Self::FirstValue | Self::LastValue | Self::NthValue
        )
    }

    /// Check if this function requires frame bounds.
    pub fn needs_frame(&self) -> bool {
        matches!(self, Self::FirstValue | Self::LastValue | Self::NthValue)
    }

    /// Check if this function requires peer bounds.
    pub fn needs_peers(&self) -> bool {
        matches!(
            self,
            Self::Rank | Self::DenseRank | Self::PercentRank | Self::CumeDist
        )
    }
}

// ============================================================================
// Window Executor State Traits
// ============================================================================

/// Global state for window function execution.
///
/// This state is shared across all threads working on the window function.
pub trait WindowExecutorGlobalState: Send + Sync {
    /// Downcast to concrete type.
    fn as_any(&self) -> &dyn Any;

    /// Downcast to mutable concrete type.
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

/// Local state for window function execution.
///
/// This state is thread-local and is initialized once per thread.
pub trait WindowExecutorLocalState: Send {
    /// Downcast to concrete type.
    fn as_any(&self) -> &dyn Any;

    /// Downcast to mutable concrete type.
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

// ============================================================================
// Window Executor Trait
// ============================================================================

/// Trait for window function executors.
///
/// Each window function type has its own executor implementation.
pub trait WindowExecutor: Send + Sync {
    /// Get the window function type.
    fn function_type(&self) -> WindowFunctionType;

    /// Get the return type of this window function.
    fn return_type(&self) -> LogicalType;

    /// Whether this function ignores NULL values.
    fn ignore_nulls(&self) -> bool {
        false
    }

    /// Create global state for this executor.
    fn create_global_state(&self) -> Result<Box<dyn WindowExecutorGlobalState>>;

    /// Create local state for this executor.
    fn create_local_state(
        &self,
        global_state: &dyn WindowExecutorGlobalState,
    ) -> Result<Box<dyn WindowExecutorLocalState>>;

    /// Evaluate the window function for a range of rows.
    ///
    /// # Arguments
    /// * `global_state` - Global state shared across threads
    /// * `local_state` - Thread-local state
    /// * `input` - Input data chunk
    /// * `result` - Output vector to fill
    /// * `row_idx` - Starting row index
    /// * `count` - Number of rows to process
    /// * `partition_start` - Start of current partition
    /// * `partition_end` - End of current partition (exclusive)
    /// * `peer_start` - Start of current peer group
    /// * `peer_end` - End of current peer group (exclusive)
    /// * `frame_start` - Start of window frame
    /// * `frame_end` - End of window frame (exclusive)
    fn evaluate(
        &self,
        global_state: &dyn WindowExecutorGlobalState,
        local_state: &mut dyn WindowExecutorLocalState,
        input: &Chunk,
        result: &mut Vector,
        row_idx: usize,
        count: usize,
        partition_start: usize,
        partition_end: usize,
        peer_start: usize,
        peer_end: usize,
        frame_start: usize,
        frame_end: usize,
    ) -> Result<()>;
}

// ============================================================================
// Window Function Definition
// ============================================================================

/// Definition of a window function.
///
/// This struct holds the metadata and executor for a window function.
#[derive(Clone)]
pub struct WindowFunction {
    /// Function name.
    pub name: String,
    /// Function type.
    pub function_type: WindowFunctionType,
    /// Argument types.
    pub arguments: Vec<LogicalType>,
    /// Return type.
    pub return_type: LogicalType,
}

impl fmt::Debug for WindowFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WindowFunction")
            .field("name", &self.name)
            .field("function_type", &self.function_type)
            .field("arguments", &self.arguments)
            .field("return_type", &self.return_type)
            .finish()
    }
}

impl WindowFunction {
    /// Create a new window function definition.
    pub fn new(
        name: impl Into<String>,
        function_type: WindowFunctionType,
        arguments: Vec<LogicalType>,
        return_type: LogicalType,
    ) -> Self {
        Self {
            name: name.into(),
            function_type,
            arguments,
            return_type,
        }
    }

    /// Create ROW_NUMBER() function.
    pub fn row_number() -> Self {
        Self::new(
            "row_number",
            WindowFunctionType::RowNumber,
            vec![],
            LogicalType::BigInt,
        )
    }

    /// Create RANK() function.
    pub fn rank() -> Self {
        Self::new(
            "rank",
            WindowFunctionType::Rank,
            vec![],
            LogicalType::BigInt,
        )
    }

    /// Create DENSE_RANK() function.
    pub fn dense_rank() -> Self {
        Self::new(
            "dense_rank",
            WindowFunctionType::DenseRank,
            vec![],
            LogicalType::BigInt,
        )
    }

    /// Create NTILE(n) function.
    pub fn ntile() -> Self {
        Self::new(
            "ntile",
            WindowFunctionType::Ntile,
            vec![LogicalType::BigInt],
            LogicalType::BigInt,
        )
    }

    /// Create PERCENT_RANK() function.
    pub fn percent_rank() -> Self {
        Self::new(
            "percent_rank",
            WindowFunctionType::PercentRank,
            vec![],
            LogicalType::Double,
        )
    }

    /// Create CUME_DIST() function.
    pub fn cume_dist() -> Self {
        Self::new(
            "cume_dist",
            WindowFunctionType::CumeDist,
            vec![],
            LogicalType::Double,
        )
    }

    /// Create LEAD(expr) function.
    pub fn lead(value_type: LogicalType) -> Self {
        Self::new(
            "lead",
            WindowFunctionType::Lead,
            vec![value_type.clone()],
            value_type,
        )
    }

    /// Create LEAD(expr, offset) function.
    pub fn lead_with_offset(value_type: LogicalType) -> Self {
        Self::new(
            "lead",
            WindowFunctionType::Lead,
            vec![value_type.clone(), LogicalType::BigInt],
            value_type,
        )
    }

    /// Create LEAD(expr, offset, default) function.
    pub fn lead_with_default(value_type: LogicalType) -> Self {
        Self::new(
            "lead",
            WindowFunctionType::Lead,
            vec![value_type.clone(), LogicalType::BigInt, value_type.clone()],
            value_type,
        )
    }

    /// Create LAG(expr) function.
    pub fn lag(value_type: LogicalType) -> Self {
        Self::new(
            "lag",
            WindowFunctionType::Lag,
            vec![value_type.clone()],
            value_type,
        )
    }

    /// Create LAG(expr, offset) function.
    pub fn lag_with_offset(value_type: LogicalType) -> Self {
        Self::new(
            "lag",
            WindowFunctionType::Lag,
            vec![value_type.clone(), LogicalType::BigInt],
            value_type,
        )
    }

    /// Create LAG(expr, offset, default) function.
    pub fn lag_with_default(value_type: LogicalType) -> Self {
        Self::new(
            "lag",
            WindowFunctionType::Lag,
            vec![value_type.clone(), LogicalType::BigInt, value_type.clone()],
            value_type,
        )
    }

    /// Create FIRST_VALUE(expr) function.
    pub fn first_value(value_type: LogicalType) -> Self {
        Self::new(
            "first_value",
            WindowFunctionType::FirstValue,
            vec![value_type.clone()],
            value_type,
        )
    }

    /// Create LAST_VALUE(expr) function.
    pub fn last_value(value_type: LogicalType) -> Self {
        Self::new(
            "last_value",
            WindowFunctionType::LastValue,
            vec![value_type.clone()],
            value_type,
        )
    }

    /// Create NTH_VALUE(expr, n) function.
    pub fn nth_value(value_type: LogicalType) -> Self {
        Self::new(
            "nth_value",
            WindowFunctionType::NthValue,
            vec![value_type.clone(), LogicalType::BigInt],
            value_type,
        )
    }
}

// ============================================================================
// Window Function Set
// ============================================================================

/// A set of window functions with the same name but different signatures.
#[derive(Clone, Debug)]
pub struct WindowFunctionSet {
    /// Function name.
    pub name: String,
    /// Function overloads.
    pub functions: Vec<WindowFunction>,
}

impl WindowFunctionSet {
    /// Create a new window function set.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            functions: Vec::new(),
        }
    }

    /// Add a function to the set.
    pub fn add_function(&mut self, function: WindowFunction) {
        self.functions.push(function);
    }

    /// Find the best matching function for the given arguments.
    pub fn bind(&self, arguments: &[LogicalType]) -> Result<&WindowFunction> {
        use paro_common::cast_rules::CastRules;

        let mut best_match: Option<(&WindowFunction, i64)> = None;

        for func in &self.functions {
            if func.arguments.len() != arguments.len() {
                continue;
            }

            let mut total_cost: i64 = 0;
            let mut valid = true;

            for (arg_type, param_type) in arguments.iter().zip(&func.arguments) {
                let cost = CastRules::implicit_cast_cost(arg_type, param_type);
                if cost < 0 {
                    valid = false;
                    break;
                }
                total_cost += cost;
            }

            if !valid {
                continue;
            }

            match &best_match {
                None => {
                    best_match = Some((func, total_cost));
                }
                Some((_, best_cost)) if total_cost < *best_cost => {
                    best_match = Some((func, total_cost));
                }
                _ => {}
            }
        }

        match best_match {
            Some((func, _)) => Ok(func),
            None => Err(paro_common::error::catalog(format!(
                "No matching window function found for {} with arguments {:?}",
                self.name, arguments
            ))),
        }
    }
}

// ============================================================================
// Empty State Implementations
// ============================================================================

/// Empty global state for window functions that don't need global state.
pub struct EmptyWindowGlobalState;

impl WindowExecutorGlobalState for EmptyWindowGlobalState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

/// Empty local state for window functions that don't need local state.
pub struct EmptyWindowLocalState;

impl WindowExecutorLocalState for EmptyWindowLocalState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_window_boundary_display() {
        assert_eq!(
            WindowBoundary::UnboundedPreceding.to_string(),
            "UNBOUNDED PRECEDING"
        );
        assert_eq!(
            WindowBoundary::CurrentRowRows.to_string(),
            "CURRENT ROW (ROWS)"
        );
        assert_eq!(
            WindowBoundary::ExprFollowingRange.to_string(),
            "expr FOLLOWING (RANGE)"
        );
    }

    #[test]
    fn test_window_boundary_predicates() {
        assert!(WindowBoundary::CurrentRowRows.is_rows());
        assert!(!WindowBoundary::CurrentRowRows.is_range());

        assert!(WindowBoundary::ExprPrecedingRange.is_range());
        assert!(WindowBoundary::ExprPrecedingRange.is_preceding());

        assert!(WindowBoundary::UnboundedFollowing.is_unbounded());
        assert!(WindowBoundary::UnboundedFollowing.is_following());
    }

    #[test]
    fn test_window_exclude_mode() {
        assert_eq!(WindowExcludeMode::default(), WindowExcludeMode::NoOther);
        assert_eq!(
            WindowExcludeMode::CurrentRow.to_string(),
            "EXCLUDE CURRENT ROW"
        );
    }

    #[test]
    fn test_window_bounds_index() {
        assert_eq!(WindowBounds::PartitionBegin.index(), 0);
        assert_eq!(WindowBounds::FrameEnd.index(), 7);
        assert_eq!(WindowBounds::COUNT, 8);
    }

    #[test]
    fn test_frame_bounds() {
        let frame = FrameBounds::new(5, 10);
        assert_eq!(frame.size(), 5);
        assert!(!frame.is_empty());
        assert!(frame.contains(5));
        assert!(frame.contains(9));
        assert!(!frame.contains(10));

        let empty = FrameBounds::new(10, 5);
        assert!(empty.is_empty());
        assert_eq!(empty.size(), 0);
    }

    #[test]
    fn test_window_function_type() {
        assert!(WindowFunctionType::RowNumber.is_ranking());
        assert!(!WindowFunctionType::RowNumber.is_value());

        assert!(WindowFunctionType::Lead.is_value());
        assert!(!WindowFunctionType::Lead.is_ranking());

        assert!(WindowFunctionType::FirstValue.needs_frame());
        assert!(WindowFunctionType::Rank.needs_peers());
    }

    #[test]
    fn test_window_function_constructors() {
        let row_num = WindowFunction::row_number();
        assert_eq!(row_num.name, "row_number");
        assert_eq!(row_num.function_type, WindowFunctionType::RowNumber);
        assert!(row_num.arguments.is_empty());
        assert_eq!(row_num.return_type, LogicalType::BigInt);

        let lead = WindowFunction::lead_with_default(LogicalType::Integer);
        assert_eq!(lead.name, "lead");
        assert_eq!(lead.arguments.len(), 3);
    }

    #[test]
    fn test_window_function_set_bind() {
        let mut set = WindowFunctionSet::new("lead");
        set.add_function(WindowFunction::lead(LogicalType::Integer));
        set.add_function(WindowFunction::lead_with_offset(LogicalType::Integer));
        set.add_function(WindowFunction::lead_with_default(LogicalType::Integer));

        // Bind with 1 argument
        let func = set.bind(&[LogicalType::Integer]).unwrap();
        assert_eq!(func.arguments.len(), 1);

        // Bind with 2 arguments
        let func = set
            .bind(&[LogicalType::Integer, LogicalType::BigInt])
            .unwrap();
        assert_eq!(func.arguments.len(), 2);

        // Bind with 3 arguments
        let func = set
            .bind(&[
                LogicalType::Integer,
                LogicalType::BigInt,
                LogicalType::Integer,
            ])
            .unwrap();
        assert_eq!(func.arguments.len(), 3);
    }

    #[test]
    fn test_window_function_set_bind_no_match() {
        let mut set = WindowFunctionSet::new("row_number");
        set.add_function(WindowFunction::row_number());

        // row_number takes no arguments
        let result = set.bind(&[LogicalType::Integer]);
        assert!(result.is_err());
    }

    #[test]
    fn test_empty_states() {
        let global = EmptyWindowGlobalState;
        let _ = global.as_any();

        let local = EmptyWindowLocalState;
        let _ = local.as_any();
    }
}
