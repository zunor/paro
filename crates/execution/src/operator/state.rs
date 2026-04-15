// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Operator state types for physical operators.
//!
//! - Separate state types for different operator roles
//! - Thread-local states for parallel execution
//! - Global states for cross-thread coordination
//! - Input structs to bundle state references

use std::any::Any;
use std::fmt;

use crate::result_type::SinkFinalizeType;

// ========== Progress Data ==========

/// Progress information from source operators.
///
#[derive(Debug, Clone, Copy, Default)]
pub struct ProgressData {
    /// Percentage of work completed (0.0 to 1.0).
    pub percentage: f64,
    /// Number of rows scanned so far.
    pub rows_scanned: u64,
}

impl ProgressData {
    /// Create a new ProgressData.
    pub fn new(percentage: f64, rows_scanned: u64) -> Self {
        Self {
            percentage,
            rows_scanned,
        }
    }

    /// Create an invalid progress (used when progress is unknown).
    pub fn invalid() -> Self {
        Self {
            percentage: -1.0,
            rows_scanned: 0,
        }
    }

    /// Check if this progress data is valid.
    pub fn is_valid(&self) -> bool {
        self.percentage >= 0.0
    }

    ///
    /// Calculates the percentage as (scanned / total).
    pub fn set_progress(&mut self, scanned: usize, total: usize) {
        self.rows_scanned = scanned as u64;
        if total > 0 {
            self.percentage = (scanned as f64) / (total as f64);
        } else {
            self.percentage = 1.0;
        }
    }
}

/// Trait for downcasting operator states.
///
/// All operator states implement this to enable safe downcasting
/// to concrete state types in operator implementations.
pub trait OperatorStateDowncast: Any + Send + Sync {
    /// Downcast to Any for type checking.
    fn as_any(&self) -> &dyn Any;

    /// Downcast to mutable Any.
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

impl<T: Any + Send + Sync> OperatorStateDowncast for T {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

// ========== Operator State ==========

/// Thread-local state for regular operators.
///
/// Each thread maintains its own OperatorState during execution.
/// Override `finalize` to perform cleanup after execution.
pub trait OperatorState: Send + Sync + fmt::Debug {
    /// Finalize the state after execution completes.
    fn finalize(&mut self) {
        // Default: no-op
    }

    /// Downcast to concrete type.
    fn as_any(&self) -> &dyn Any;

    /// Downcast to mutable concrete type.
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

/// Default empty operator state.
#[derive(Debug, Default)]
pub struct EmptyOperatorState;

impl OperatorState for EmptyOperatorState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

// ========== Global Operator State ==========

/// Global (cross-thread) state for regular operators.
///
/// Shared across all threads executing an operator.
/// Use synchronization primitives for thread safety.
///
pub trait GlobalOperatorState: Send + Sync + fmt::Debug {
    /// Maximum number of threads for this operator.
    ///
    /// Returns the source's max threads by default.
    /// Override to limit parallelism for operators that have constraints.
    fn max_threads(&self, source_max_threads: usize) -> usize {
        source_max_threads
    }

    /// Downcast to concrete type.
    fn as_any(&self) -> &dyn Any;

    /// Downcast to mutable concrete type.
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

/// Default empty global operator state.
#[derive(Debug, Default)]
pub struct EmptyGlobalOperatorState;

impl GlobalOperatorState for EmptyGlobalOperatorState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

// ========== Source State ==========

/// Thread-local state for source operators.
///
/// Each thread has its own LocalSourceState when scanning.
pub trait LocalSourceState: Send + Sync + fmt::Debug {
    /// Downcast to concrete type.
    fn as_any(&self) -> &dyn Any;

    /// Downcast to mutable concrete type.
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

/// Default empty local source state.
#[derive(Debug, Default)]
pub struct EmptyLocalSourceState;

impl LocalSourceState for EmptyLocalSourceState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

/// Global state for source operators.
///
/// Coordinates work distribution across threads.
///
pub trait GlobalSourceState: Send + Sync + fmt::Debug {
    /// Maximum number of threads for this source.
    ///
    /// Returns the maximum number of threads that can be used to scan this source.
    /// Default is 1 (sequential). Override to return higher values for parallel sources.
    ///
    /// For table scans, this typically returns the number of scan partitions/segments.
    /// For value scans or single-row sources, this should return 1.
    fn max_threads(&self) -> usize {
        1
    }

    /// Downcast to concrete type.
    fn as_any(&self) -> &dyn Any;

    /// Downcast to mutable concrete type.
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

/// Default empty global source state.
#[derive(Debug, Default)]
pub struct EmptyGlobalSourceState;

impl GlobalSourceState for EmptyGlobalSourceState {
    fn max_threads(&self) -> usize {
        1
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

// ========== Sink State ==========

/// Thread-local state for sink operators.
///
/// Each thread has its own LocalSinkState when writing.
pub trait LocalSinkState: Send + Sync + fmt::Debug {
    /// Downcast to concrete type.
    fn as_any(&self) -> &dyn Any;

    /// Downcast to mutable concrete type.
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

/// Default empty local sink state.
#[derive(Debug, Default)]
pub struct EmptyLocalSinkState;

impl LocalSinkState for EmptyLocalSinkState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

/// Global state for sink operators.
///
/// Coordinates writes across threads.
///
pub trait GlobalSinkState: Send + Sync + fmt::Debug {
    /// Current finalize state.
    fn state(&self) -> SinkFinalizeType {
        SinkFinalizeType::Ready
    }

    /// Maximum number of threads for this sink.
    ///
    /// Returns the maximum number of threads that can be used for this sink.
    /// Default passes through the source's max threads.
    ///
    /// Override to limit parallelism for sinks that have constraints
    /// (e.g., single-threaded file writes).
    fn max_threads(&self, source_max_threads: usize) -> usize {
        source_max_threads
    }

    /// Downcast to concrete type.
    fn as_any(&self) -> &dyn Any;

    /// Downcast to mutable concrete type.
    fn as_any_mut(&mut self) -> &mut dyn Any;

    /// Name of the sink state.
    fn sink_state_name(&self) -> &str {
        "MISSING_IMPLEMENTATION"
    }
}

/// Default empty global sink state.
#[derive(Debug)]
pub struct EmptyGlobalSinkState {
    /// Current finalize state.
    pub finalize_state: SinkFinalizeType,
    /// Name for debugging
    pub _name: String,
}

impl Default for EmptyGlobalSinkState {
    fn default() -> Self {
        Self {
            finalize_state: SinkFinalizeType::Ready,
            _name: "EmptyGlobalSinkState".to_string(),
        }
    }
}

impl GlobalSinkState for EmptyGlobalSinkState {
    fn state(&self) -> SinkFinalizeType {
        self.finalize_state
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn sink_state_name(&self) -> &str {
        &self._name
    }
}

use paro_scheduler::task::InterruptState;

// ========== Input Structs ==========

/// Input for source operators.
///
/// Bundles references to global and local source states.
pub struct OperatorSourceInput<'a> {
    /// Global source state
    pub global_state: &'a dyn GlobalSourceState,
    /// Thread-local source state
    pub local_state: &'a mut dyn LocalSourceState,
    /// Interrupt state for blocking operations
    pub interrupt_state: &'a InterruptState,
}

impl<'a> OperatorSourceInput<'a> {
    /// Create a new source input.
    pub fn new(
        global_state: &'a dyn GlobalSourceState,
        local_state: &'a mut dyn LocalSourceState,
        interrupt_state: &'a InterruptState,
    ) -> Self {
        Self {
            global_state,
            local_state,
            interrupt_state,
        }
    }
}

/// Input for sink operators.
///
/// Bundles references to global and local sink states.
pub struct OperatorSinkInput<'a> {
    /// Global sink state
    pub global_state: &'a dyn GlobalSinkState,
    /// Thread-local sink state
    pub local_state: &'a mut dyn LocalSinkState,
    /// Interrupt state for blocking operations
    pub interrupt_state: &'a InterruptState,
}

impl<'a> OperatorSinkInput<'a> {
    /// Create a new sink input.
    pub fn new(
        global_state: &'a dyn GlobalSinkState,
        local_state: &'a mut dyn LocalSinkState,
        interrupt_state: &'a InterruptState,
    ) -> Self {
        Self {
            global_state,
            local_state,
            interrupt_state,
        }
    }
}

/// Input for sink combine operations.
pub struct OperatorSinkCombineInput<'a> {
    /// Global sink state
    pub global_state: &'a dyn GlobalSinkState,
    /// Thread-local sink state
    pub local_state: &'a mut dyn LocalSinkState,
    /// Interrupt state for blocking operations
    pub interrupt_state: &'a InterruptState,
}

impl<'a> OperatorSinkCombineInput<'a> {
    /// Create a new combine input.
    pub fn new(
        global_state: &'a dyn GlobalSinkState,
        local_state: &'a mut dyn LocalSinkState,
        interrupt_state: &'a InterruptState,
    ) -> Self {
        Self {
            global_state,
            local_state,
            interrupt_state,
        }
    }
}

/// Input for sink finalize operations.
pub struct OperatorSinkFinalizeInput<'a> {
    /// Global sink state
    pub global_state: &'a dyn GlobalSinkState,
    /// Interrupt state for blocking operations
    pub interrupt_state: &'a InterruptState,
}

impl<'a> OperatorSinkFinalizeInput<'a> {
    /// Create a new finalize input.
    pub fn new(global_state: &'a dyn GlobalSinkState, interrupt_state: &'a InterruptState) -> Self {
        Self {
            global_state,
            interrupt_state,
        }
    }
}

/// Input for operator finalize operations.
pub struct OperatorFinalizeInput<'a> {
    /// Global operator state
    pub global_state: &'a dyn GlobalOperatorState,
    /// Interrupt state for blocking operations
    pub interrupt_state: &'a InterruptState,
}

impl<'a> OperatorFinalizeInput<'a> {
    /// Create a new operator finalize input.
    pub fn new(
        global_state: &'a dyn GlobalOperatorState,
        interrupt_state: &'a InterruptState,
    ) -> Self {
        Self {
            global_state,
            interrupt_state,
        }
    }
}
