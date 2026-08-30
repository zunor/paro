// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::any::Any;
use std::sync::Arc;

use paro_common::allocator::{Allocator, MemoryTag};

use super::function_data::FunctionData;
use super::local_state::FunctionLocalState;

/// Trait for runtime context access during scalar function execution.
pub trait FunctionExecContext: Send + Sync {
    fn current_database(&self) -> Option<&str>;

    fn current_schema(&self) -> Option<&str>;

    fn current_user(&self) -> Option<&str>;

    fn current_setting(&self, _key: &str) -> Option<String> {
        None
    }

    /// Transaction wall-clock timestamp in microseconds since the Unix epoch.
    ///
    /// Runtime contexts provide this from their frozen transaction snapshot. The default keeps
    /// lightweight function-only contexts source-compatible while allowing time functions to
    /// reject execution without an explicit temporal anchor.
    fn transaction_timestamp_micros(&self) -> Option<i64> {
        None
    }

    /// Draw from the session-owned SQL random sequence.
    fn next_random(&self) -> Option<f64> {
        None
    }

    fn is_interrupted(&self) -> bool {
        false
    }

    fn allocator(&self, _tag: MemoryTag) -> Arc<dyn Allocator> {
        Arc::new(paro_common::allocator::default_allocator())
    }

    fn has_session_context(&self) -> bool {
        self.current_database().is_some()
    }

    fn bind_data(&self) -> Option<&dyn FunctionData> {
        None
    }

    fn local_state(&self) -> Option<&dyn FunctionLocalState> {
        None
    }

    fn as_any(&self) -> &dyn Any
    where
        Self: 'static + Sized,
    {
        self
    }
}

pub use FunctionExecContext as ExpressionState;
