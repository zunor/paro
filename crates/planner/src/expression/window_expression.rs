// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Bound Window Expression
//!
//!

use super::{AggregateExpression, Expression};
use paro_common::error::{self as paro_error, Result};
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

/// Bound function invocation evaluated by a window operator.
///
/// Native window functions and aggregate functions have different execution
/// contracts. Keeping that distinction in the IR prevents physical planning
/// from reconstructing an aggregate kernel from a display name.
#[derive(Debug, Clone)]
pub enum WindowInvocation {
    /// A native ranking or value window function and its arguments.
    Native {
        function: WindowFunction,
        arguments: Vec<Expression>,
    },
    /// A fully bound aggregate invocation, including bind data and modifiers.
    Aggregate(AggregateExpression),
}

impl WindowInvocation {
    pub fn name(&self) -> &str {
        match self {
            Self::Native { function, .. } => &function.name,
            Self::Aggregate(aggregate) => &aggregate.function.name,
        }
    }

    pub fn return_type(&self) -> LogicalType {
        match self {
            Self::Native { function, .. } => function.return_type.clone(),
            Self::Aggregate(aggregate) => aggregate.return_type.clone(),
        }
    }

    pub fn arguments(&self) -> &[Expression] {
        match self {
            Self::Native { arguments, .. } => arguments,
            Self::Aggregate(aggregate) => &aggregate.children,
        }
    }

    pub fn arguments_mut(&mut self) -> &mut Vec<Expression> {
        match self {
            Self::Native { arguments, .. } => arguments,
            Self::Aggregate(aggregate) => &mut aggregate.children,
        }
    }

    pub fn native(&self) -> Option<(&WindowFunction, &[Expression])> {
        match self {
            Self::Native {
                function,
                arguments,
            } => Some((function, arguments)),
            Self::Aggregate(_) => None,
        }
    }

    pub fn aggregate(&self) -> Option<&AggregateExpression> {
        match self {
            Self::Native { .. } => None,
            Self::Aggregate(aggregate) => Some(aggregate),
        }
    }
}

/// A bound window expression.
#[derive(Debug, Clone)]
pub struct WindowExpression {
    /// Bound native or aggregate function invocation.
    pub invocation: WindowInvocation,
    /// PARTITION BY expressions.
    pub partitions: Vec<Expression>,
    /// ORDER BY expressions.
    pub orders: Vec<OrderByExpression>,
    /// Window frame specification.
    pub frame: WindowFrame,
    /// Whether to ignore NULLs.
    pub ignore_nulls: bool,
}

impl WindowExpression {
    pub fn native(
        function: WindowFunction,
        arguments: Vec<Expression>,
        partitions: Vec<Expression>,
        orders: Vec<OrderByExpression>,
        frame: WindowFrame,
        ignore_nulls: bool,
    ) -> Self {
        Self {
            invocation: WindowInvocation::Native {
                function,
                arguments,
            },
            partitions,
            orders,
            frame,
            ignore_nulls,
        }
    }

    pub fn aggregate(
        aggregate: AggregateExpression,
        partitions: Vec<Expression>,
        orders: Vec<OrderByExpression>,
        frame: WindowFrame,
    ) -> Self {
        Self {
            invocation: WindowInvocation::Aggregate(aggregate),
            partitions,
            orders,
            frame,
            ignore_nulls: false,
        }
    }

    pub fn return_type(&self) -> LogicalType {
        self.invocation.return_type()
    }

    pub fn function_name(&self) -> &str {
        self.invocation.name()
    }

    pub fn arguments(&self) -> &[Expression] {
        self.invocation.arguments()
    }

    pub fn arguments_mut(&mut self) -> &mut Vec<Expression> {
        self.invocation.arguments_mut()
    }

    pub fn native_invocation(&self) -> Option<(&WindowFunction, &[Expression])> {
        self.invocation.native()
    }

    pub fn aggregate_invocation(&self) -> Option<&AggregateExpression> {
        self.invocation.aggregate()
    }

    /// Verify that the bound invocation is internally self-consistent.
    ///
    /// This is a correctness boundary: physical planning consumes executable
    /// aggregate hooks from the expression and must never infer them from a
    /// name or tolerate an argument layout that disagrees with the kernel.
    pub fn verify_bound_contract(&self) -> Result<()> {
        match &self.invocation {
            WindowInvocation::Native {
                function,
                arguments,
            } => {
                if function.arguments.len() != arguments.len() {
                    return Err(paro_error::internal(format!(
                        "native window '{}' expects {} arguments, found {}",
                        function.name,
                        function.arguments.len(),
                        arguments.len()
                    )));
                }
                for (idx, (expected, argument)) in
                    function.arguments.iter().zip(arguments).enumerate()
                {
                    let actual = argument.return_type();
                    if &actual != expected {
                        return Err(paro_error::internal(format!(
                            "native window '{}' argument {} type mismatch: expected {}, found {}",
                            function.name, idx, expected, actual
                        )));
                    }
                }
            }
            WindowInvocation::Aggregate(aggregate) => {
                if self.ignore_nulls {
                    return Err(paro_error::internal(
                        "aggregate window cannot carry native IGNORE NULLS semantics",
                    ));
                }
                if aggregate.return_type != aggregate.function.return_type {
                    return Err(paro_error::internal(format!(
                        "aggregate window '{}' return type mismatch: expression={}, kernel={}",
                        aggregate.function.name,
                        aggregate.return_type,
                        aggregate.function.return_type
                    )));
                }
                let fixed = aggregate.function.arguments.len();
                let valid_arity = if aggregate.function.varargs.is_some() {
                    aggregate.children.len() >= fixed
                } else {
                    aggregate.children.len() == fixed
                };
                if !valid_arity {
                    return Err(paro_error::internal(format!(
                        "aggregate window '{}' expects {}{} arguments, found {}",
                        aggregate.function.name,
                        fixed,
                        if aggregate.function.varargs.is_some() {
                            " or more"
                        } else {
                            ""
                        },
                        aggregate.children.len()
                    )));
                }
                for (idx, child) in aggregate.children.iter().enumerate() {
                    let expected = aggregate
                        .function
                        .arguments
                        .get(idx)
                        .or(aggregate.function.varargs.as_ref())
                        .ok_or_else(|| {
                            paro_error::internal(format!(
                                "aggregate window '{}' has no type for argument {}",
                                aggregate.function.name, idx
                            ))
                        })?;
                    let actual = child.return_type();
                    if &actual != expected {
                        return Err(paro_error::internal(format!(
                            "aggregate window '{}' argument {} type mismatch: expected {}, found {}",
                            aggregate.function.name, idx, expected, actual
                        )));
                    }
                }
                if let Some(filter) = &aggregate.filter {
                    if filter.return_type() != LogicalType::Boolean {
                        return Err(paro_error::internal(format!(
                            "aggregate window '{}' FILTER must be BOOLEAN",
                            aggregate.function.name
                        )));
                    }
                }
            }
        }
        Ok(())
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
    /// Whether this frame contains every row in a partition.
    ///
    /// With no ORDER BY all rows are peers, so RANGE CURRENT ROW also denotes
    /// the full partition. Explicit double-unbounded frames are independent of
    /// peer layout and cover the partition even when it is ordered.
    pub fn covers_whole_partition(&self, has_order: bool) -> bool {
        let starts_unbounded = matches!(
            (&self.start_bound, self.start_is_preceding),
            (WindowFrameBound::Unbounded, true)
        );
        let ends_unbounded = matches!(
            (&self.end_bound, self.end_is_preceding),
            (WindowFrameBound::Unbounded, false)
        );
        if starts_unbounded && ends_unbounded {
            return true;
        }
        if has_order || self.frame_type != WindowFrameType::Range {
            return false;
        }
        let starts_at_first_peer =
            starts_unbounded || matches!(&self.start_bound, WindowFrameBound::CurrentRow);
        let ends_at_last_peer =
            ends_unbounded || matches!(&self.end_bound, WindowFrameBound::CurrentRow);
        starts_at_first_peer && ends_at_last_peer
    }

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
            | WindowFunctionType::NthValue => WindowFrame::default(),
        }
    }
}
