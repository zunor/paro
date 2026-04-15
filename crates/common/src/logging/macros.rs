// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Convenience logging macros.
//!
//! These macros automatically use the module path as the log target,
//! making it easier to filter logs by module.
//!
//! # Example
//!
//! **Note:** Prefer `tracing::info!(target: targets::XXX, ...)` with explicit
//! `targets` constants from `paro_common::logging::targets`. These macros use
//! `module_path!()` which produces crate-prefixed targets (e.g. `paro_execution::...`)
//! that don't align with the `paro::` target convention.
//!
//! ```rust,ignore
//! use paro_common::{paro_info, paro_debug, paro_error};
//!
//! fn my_function() {
//!     paro_info!("Server started");
//!     paro_debug!(user_id = 123, "Processing request");
//!     paro_error!("Connection refused: {}", error);
//! }
//! ```

/// Log a trace-level message with automatic module path target.
#[macro_export]
macro_rules! paro_trace {
    ($($arg:tt)*) => {
        tracing::trace!(target: module_path!(), $($arg)*)
    };
}

/// Log a debug-level message with automatic module path target.
#[macro_export]
macro_rules! paro_debug {
    ($($arg:tt)*) => {
        tracing::debug!(target: module_path!(), $($arg)*)
    };
}

/// Log an info-level message with automatic module path target.
#[macro_export]
macro_rules! paro_info {
    ($($arg:tt)*) => {
        tracing::info!(target: module_path!(), $($arg)*)
    };
}

/// Log a warn-level message with automatic module path target.
#[macro_export]
macro_rules! paro_warn {
    ($($arg:tt)*) => {
        tracing::warn!(target: module_path!(), $($arg)*)
    };
}

/// Log an error-level message with automatic module path target.
#[macro_export]
macro_rules! paro_error {
    ($($arg:tt)*) => {
        tracing::error!(target: module_path!(), $($arg)*)
    };
}

#[cfg(test)]
mod tests {
    // These macros are tested via usage in other tests
    // since they're simple wrappers around tracing macros

    #[test]
    fn test_macros_compile() {
        // Just verify that the macros compile correctly
        // (they won't actually log since no subscriber is set up)
        let x = 42;
        paro_trace!("trace message");
        paro_debug!("debug message: {}", x);
        paro_info!(value = x, "info message");
        paro_warn!("warn message");
        paro_error!("error message");
    }
}
