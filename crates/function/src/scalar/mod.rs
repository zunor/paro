//! Scalar Function Module
//!
//! This module provides scalar function definitions and execution infrastructure.

mod expression_state;
mod function_data;
mod local_state;
mod scalar_function;

#[macro_use]
pub mod executor;
pub mod blob;
pub mod cast;
pub mod date;
pub mod fulltext;
pub mod math;
pub mod null_ops;
pub mod operators;
pub mod string;
pub mod system;
pub mod vector;

// Re-export public types
pub use expression_state::{ExpressionState, FunctionExecContext};
pub use function_data::{function_data_equals, FunctionData};
pub use local_state::FunctionLocalState;
pub use scalar_function::{
    BoundScalarFunction, DictionaryStrategy, FunctionErrorMode, FunctionNullHandling,
    FunctionSideEffects, FunctionStability, InitLocalStateFn, ScalarBindFn, ScalarBindInput,
    ScalarDispatch, ScalarFunction, ScalarFunctionFn, ScalarFunctionSet,
};
