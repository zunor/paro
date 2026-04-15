// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Result types used to drive operator, source, and sink execution flow.

use std::fmt;

/// Result type for regular operators (non-sink, non-source).
///
/// Controls data flow around physical operators:
/// - `NeedMoreInput`: Operator is done with current input, can consume more
/// - `HaveMoreOutput`: Operator not finished with current input, call again
/// - `Finished`: Pipeline is complete, no more processing needed
/// - `Blocked`: Operator is blocked (e.g., async I/O)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum OperatorResultType {
    /// Operator needs more input to continue
    NeedMoreInput = 0,
    /// Operator has more output from current input
    HaveMoreOutput,
    /// Operator and pipeline are finished
    Finished,
    /// Operator is currently blocked
    Blocked,
}

impl fmt::Display for OperatorResultType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NeedMoreInput => write!(f, "NEED_MORE_INPUT"),
            Self::HaveMoreOutput => write!(f, "HAVE_MORE_OUTPUT"),
            Self::Finished => write!(f, "FINISHED"),
            Self::Blocked => write!(f, "BLOCKED"),
        }
    }
}

/// Result type for finalize operations on operators.
///
/// Used when operators need to flush cached results:
/// - `HaveMoreOutput`: Contains more cached results
/// - `Finished`: All cached data has been flushed
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum OperatorFinalizeResultType {
    /// Operator has more cached output
    HaveMoreOutput = 0,
    /// Operator has finished flushing
    Finished,
}

impl fmt::Display for OperatorFinalizeResultType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::HaveMoreOutput => write!(f, "HAVE_MORE_OUTPUT"),
            Self::Finished => write!(f, "FINISHED"),
        }
    }
}

/// Result type for operator-level finalize calls in PipelineFinishTask.
///
/// Used when an intermediate operator needs a dedicated finalize step after all
/// pipeline tasks have completed:
/// - `Finished`: finalize completed
/// - `Blocked`: finalize is blocked and should be rescheduled
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum OperatorFinalResultType {
    /// Operator finalize completed
    Finished = 0,
    /// Operator finalize is currently blocked
    Blocked,
}

impl fmt::Display for OperatorFinalResultType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Finished => write!(f, "FINISHED"),
            Self::Blocked => write!(f, "BLOCKED"),
        }
    }
}

/// Result type for source operators (data producers).
///
/// Indicates the result of pulling data from a source:
/// - `HaveMoreOutput`: Source has more data available
/// - `Finished`: Source is exhausted
/// - `Blocked`: Source is blocked (e.g., async I/O)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum SourceResultType {
    /// Source has more output available
    HaveMoreOutput = 0,
    /// Source is exhausted
    Finished,
    /// Source is currently blocked
    Blocked,
}

impl SourceResultType {
    /// Check if the source is finished.
    #[inline]
    pub fn is_finished(&self) -> bool {
        matches!(self, Self::Finished)
    }

    /// Check if the source is blocked.
    #[inline]
    pub fn is_blocked(&self) -> bool {
        matches!(self, Self::Blocked)
    }

    /// Check if the source has more output.
    #[inline]
    pub fn has_more_output(&self) -> bool {
        matches!(self, Self::HaveMoreOutput)
    }
}

impl fmt::Display for SourceResultType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::HaveMoreOutput => write!(f, "HAVE_MORE_OUTPUT"),
            Self::Finished => write!(f, "FINISHED"),
            Self::Blocked => write!(f, "BLOCKED"),
        }
    }
}

/// Result type for sink operators (data consumers).
///
/// Indicates the result of pushing data into a sink:
/// - `NeedMoreInput`: Sink needs more input
/// - `Finished`: Sink is finished, no more input needed
/// - `Blocked`: Sink is blocked (e.g., async I/O)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum SinkResultType {
    /// Sink needs more input
    NeedMoreInput = 0,
    /// Sink is finished
    Finished,
    /// Sink is currently blocked
    Blocked,
    /// Sink was interrupted
    Interrupted,
}

impl SinkResultType {
    /// Check if the sink is finished.
    #[inline]
    pub fn is_finished(&self) -> bool {
        matches!(self, Self::Finished)
    }

    /// Check if the sink needs more input.
    #[inline]
    pub fn needs_more_input(&self) -> bool {
        matches!(self, Self::NeedMoreInput)
    }
}

impl fmt::Display for SinkResultType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NeedMoreInput => write!(f, "NEED_MORE_INPUT"),
            Self::Finished => write!(f, "FINISHED"),
            Self::Blocked => write!(f, "BLOCKED"),
            Self::Interrupted => write!(f, "INTERRUPTED"),
        }
    }
}

/// Result type for sink combine operations.
///
/// Used when combining thread-local sink states:
/// - `Finished`: Combine completed
/// - `Blocked`: Combine is blocked
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum SinkCombineResultType {
    /// Combine completed
    Finished = 0,
    /// Combine is blocked
    Blocked,
    /// Sink combine was interrupted
    Interrupted,
}

impl fmt::Display for SinkCombineResultType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Finished => write!(f, "FINISHED"),
            Self::Blocked => write!(f, "BLOCKED"),
            Self::Interrupted => write!(f, "INTERRUPTED"),
        }
    }
}

/// Result type for sink finalize operations.
///
/// Indicates the result of finalizing a sink:
/// - `Ready`: Sink is ready for further processing
/// - `NoOutputPossible`: Sink can never provide output
/// - `Blocked`: Finalize is blocked
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum SinkFinalizeType {
    /// Ready for further processing
    #[default]
    Ready = 0,
    /// No output possible from this sink
    NoOutputPossible,
    /// Finalize is blocked
    Blocked,
}

impl fmt::Display for SinkFinalizeType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ready => write!(f, "READY"),
            Self::NoOutputPossible => write!(f, "NO_OUTPUT_POSSIBLE"),
            Self::Blocked => write!(f, "BLOCKED"),
        }
    }
}

/// Result type for sink next batch operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum SinkNextBatchType {
    /// Ready for next batch
    #[default]
    Ready = 0,
    /// Next batch is blocked
    Blocked,
}

impl fmt::Display for SinkNextBatchType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ready => write!(f, "READY"),
            Self::Blocked => write!(f, "BLOCKED"),
        }
    }
}
