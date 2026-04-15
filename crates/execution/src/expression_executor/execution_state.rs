//! Runtime context adapters for scalar function execution.

use paro_common::config::format_setting_value;
use paro_common::runtime_value::Value;
use paro_context::StatementContext;
use paro_function::scalar::{FunctionData, FunctionExecContext, FunctionLocalState};

use crate::execution_context::ExecutionContext;

fn setting_value_to_string(name: &str, value: &Value) -> String {
    format_setting_value(name, value)
}

fn current_setting_from_context(session: &StatementContext, key: &str) -> Option<String> {
    let normalized_key = key
        .trim()
        .trim_matches('\'')
        .trim_matches('"')
        .to_ascii_lowercase();

    session
        .get_setting(&normalized_key)
        .map(|value| setting_value_to_string(&normalized_key, value))
}

impl FunctionExecContext for ExecutionContext<'_> {
    fn current_database(&self) -> Option<&str> {
        Some(ExecutionContext::current_database(self))
    }

    fn current_schema(&self) -> Option<&str> {
        Some(ExecutionContext::current_schema(self))
    }

    fn current_user(&self) -> Option<&str> {
        Some(ExecutionContext::current_user(self))
    }

    fn current_setting(&self, key: &str) -> Option<String> {
        current_setting_from_context(self.session.as_ref(), key)
    }

    fn is_interrupted(&self) -> bool {
        ExecutionContext::is_interrupted(self)
    }

    fn allocator(
        &self,
        tag: paro_common::allocator::MemoryTag,
    ) -> std::sync::Arc<dyn paro_common::allocator::Allocator> {
        self.allocator(tag)
    }
}

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
