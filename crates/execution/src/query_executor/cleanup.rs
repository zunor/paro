// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Terminal cleanup helpers for typed query execution.

use std::sync::Arc;

use paro_common::allocator::Allocator;
use paro_common::error::{ParoError, Result};
use paro_context::StatementCancelReason;

use crate::explain::profiler::OperatorProfiler;
use crate::runtime::{
    BreakerHandleRegistry, CleanupReason, OperatorCleanupContext, QueryRuntimeContext,
    TaskMemoryGrants,
};
use crate::thread_context::ThreadContext;

pub(super) fn cancelled_cleanup_reason(query: &QueryRuntimeContext) -> CleanupReason {
    CleanupReason::Cancelled(
        query
            .cancellation
            .reason()
            .unwrap_or(StatementCancelReason::UserRequest),
    )
}

pub(super) fn cleanup_reason_for_error(
    query: &QueryRuntimeContext,
    error: &ParoError,
) -> CleanupReason {
    if error.is_query_canceled() && query.cancellation.is_cancelled() {
        return cancelled_cleanup_reason(query);
    }
    CleanupReason::Failed(query.errors.record_root(error.clone()))
}

pub(super) fn cleanup_handles(
    handles: &BreakerHandleRegistry,
    query: &QueryRuntimeContext,
    allocator: Arc<dyn Allocator>,
    reason: CleanupReason,
) -> Result<()> {
    let thread = ThreadContext::single_threaded();
    let memory = TaskMemoryGrants::detached(allocator);
    let mut profiler = OperatorProfiler::disabled();
    let mut ctx = OperatorCleanupContext {
        query,
        pipeline: None,
        operator: None,
        thread: &thread,
        memory: memory.call_scope(),
        cancel: &query.cancellation,
        profiler: &mut profiler,
    };
    handles.cleanup_all(&mut ctx, reason)
}

pub(super) fn merge_execution_and_cleanup_result(
    result: Result<()>,
    cleanup_result: Result<()>,
) -> Result<()> {
    match (result, cleanup_result) {
        (Ok(()), Ok(())) => Ok(()),
        (Ok(()), Err(cleanup_error)) => Err(cleanup_error),
        (Err(error), Ok(())) | (Err(error), Err(_)) => Err(error),
    }
}
