// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Stack budget for the recursive planning pipeline.

// Unoptimized AST, bound-query, and logical-plan frames can exhaust the two MiB
// stack used by the test harness. Release frames are substantially smaller.
#[cfg(debug_assertions)]
const PLANNER_STACK_RED_ZONE: usize = 4 * 1024 * 1024;
#[cfg(not(debug_assertions))]
const PLANNER_STACK_RED_ZONE: usize = 256 * 1024;

#[cfg(debug_assertions)]
const PLANNER_STACK_GROW_SIZE: usize = 8 * 1024 * 1024;
#[cfg(not(debug_assertions))]
const PLANNER_STACK_GROW_SIZE: usize = 2 * 1024 * 1024;

/// Run a planning phase with enough stack for one unoptimized query level.
/// Recursive query entrypoints call this again before consuming another level.
#[inline(always)]
pub(crate) fn maybe_grow_planner_stack<R>(callback: impl FnOnce() -> R) -> R {
    stacker::maybe_grow(PLANNER_STACK_RED_ZONE, PLANNER_STACK_GROW_SIZE, callback)
}
