// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # Paro Function
//!
//! Function system for Paro database.
//!
//! ## Modules
//! - `scalar`: Scalar functions (arithmetic, string, date, etc.)
//! - `aggregate`: Aggregate functions (SUM, COUNT, AVG, etc.)
//! - `table`: Table functions (generate_series, etc.)
//! - `window`: Window functions (ROW_NUMBER, RANK, etc.)
//!
//! ## Key Types
//! - `ScalarFunction`: Scalar function definition
//! - `ScalarFunctionSet`: Set of overloaded scalar functions
//! - `AggregateFunction`: Aggregate function definition
//! - `AggregateFunctionSet`: Set of overloaded aggregate functions
//! - `TableFunction`: Table function definition
//! - `TableFunctionSet`: Set of overloaded table functions
//! - `FunctionStability`: Function stability (CONSISTENT, VOLATILE, etc.)
//! - `FunctionNullHandling`: How NULLs are handled
//! - `FunctionSideEffects`: Whether function has side effects
//! - `FunctionData`: Trait for bind-time function data

pub mod scalar;

// Re-export key types from scalar module
pub use scalar::{
    function_data_equals, BoundScalarFunction, DictionaryStrategy, ExpressionState, FunctionData,
    FunctionErrorMode, FunctionLocalState, FunctionNullHandling, FunctionSideEffects,
    FunctionStability, InitLocalStateFn, ScalarBindFn, ScalarBindInput, ScalarDispatch,
    ScalarFunction, ScalarFunctionFn, ScalarFunctionSet,
};

pub mod aggregate;

// Re-export key types from aggregate module
pub use aggregate::{
    AggregateCombineFn, AggregateDestructorFn, AggregateFinalizeFn, AggregateFunction,
    AggregateFunctionSet, AggregateInitializeFn, AggregateSimpleUpdateFn, AggregateUpdateFn,
};

pub mod table;

// Re-export key types from table module
pub use table::{
    EmptyBindData, EmptyGlobalState, EmptyLocalState, GlobalTableFunctionState,
    LocalTableFunctionState, TableFunction, TableFunctionBindData, TableFunctionBindFn,
    TableFunctionBindInput, TableFunctionCardinalityFn, TableFunctionFn, TableFunctionInitGlobalFn,
    TableFunctionInitInput, TableFunctionInitLocalFn, TableFunctionInput, TableFunctionResult,
    TableFunctionSet, MAX_THREADS,
};

pub mod copy;

pub use copy::{
    CopyFormat, CopyFunction, CopyFunctionBindData, CopyOptions, CopyToGlobalState,
    CopyToLocalState, ForceQuoteOption,
};

// Re-export runtime registration APIs for system table functions.
pub use table::system::{
    get_log_storage, get_system_buffer_manager, register_log_storage,
    register_system_buffer_manager,
};

pub mod window;

// Re-export key types from window module
pub use window::{
    EmptyWindowGlobalState, EmptyWindowLocalState, FrameBounds, WindowBoundary, WindowBounds,
    WindowExcludeMode, WindowExecutor, WindowExecutorGlobalState, WindowExecutorLocalState,
    WindowFunction, WindowFunctionSet, WindowFunctionType,
};
