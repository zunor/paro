// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Runtime context adapters for scalar function execution.

use paro_function::scalar::{FunctionData, FunctionExecContext, FunctionLocalState};

#[derive(Clone, Copy)]
pub struct BoundFunctionContext<'a> {
    runtime: &'a dyn FunctionExecContext,
    bind_data: Option<&'a dyn FunctionData>,
    local_state: Option<&'a dyn FunctionLocalState>,
}

impl<'a> BoundFunctionContext<'a> {
    pub fn new(
        runtime: &'a dyn FunctionExecContext,
        bind_data: Option<&'a dyn FunctionData>,
        local_state: Option<&'a dyn FunctionLocalState>,
    ) -> Self {
        Self {
            runtime,
            bind_data,
            local_state,
        }
    }
}

impl FunctionExecContext for BoundFunctionContext<'_> {
    fn current_database(&self) -> Option<&str> {
        self.runtime.current_database()
    }

    fn current_schema(&self) -> Option<&str> {
        self.runtime.current_schema()
    }

    fn current_user(&self) -> Option<&str> {
        self.runtime.current_user()
    }

    fn current_setting(&self, key: &str) -> Option<String> {
        self.runtime.current_setting(key)
    }

    fn transaction_timestamp_micros(&self) -> Option<i64> {
        self.runtime.transaction_timestamp_micros()
    }

    fn next_random(&self) -> Option<f64> {
        self.runtime.next_random()
    }

    fn is_interrupted(&self) -> bool {
        self.runtime.is_interrupted()
    }

    fn allocator(
        &self,
        tag: paro_common::allocator::MemoryTag,
    ) -> std::sync::Arc<dyn paro_common::allocator::Allocator> {
        self.runtime.allocator(tag)
    }

    fn bind_data(&self) -> Option<&dyn FunctionData> {
        self.bind_data
    }

    fn local_state(&self) -> Option<&dyn FunctionLocalState> {
        self.local_state
    }
}
