// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use paro_context::test_support::TestStatementContextBuilder;
use paro_execution::memory_runtime::QueryMemoryPool;
use paro_execution::runtime::{ParameterBindings, QueryOutputPort, QueryRuntimeContext};

pub fn query_runtime() -> QueryRuntimeContext {
    QueryRuntimeContext::new(
        TestStatementContextBuilder::minimal().build(),
        Arc::new(ParameterBindings::empty()),
        Arc::new(QueryMemoryPool::unbounded()),
        QueryOutputPort::discarding(),
    )
}
