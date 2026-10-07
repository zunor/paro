// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Allocation-volume accounting for short-lived planning work.
//!
//! The counter is thread-local so concurrent sessions do not contaminate one
//! another. Binaries opt in by installing [`MetricsSystemAllocator`] as their
//! global allocator; library users and tests safely observe zero.

use std::alloc::{GlobalAlloc, Layout, System};
#[cfg(feature = "alloc-metrics")]
use std::cell::Cell;

#[cfg(feature = "alloc-metrics")]
thread_local! {
    static ALLOCATED_BYTES: Cell<u64> = const { Cell::new(0) };
    static METRICS_DEPTH: Cell<u32> = const { Cell::new(0) };
}

pub struct MetricsSystemAllocator;

/// Enables allocation-volume accounting on the current thread for one scoped
/// operation. Nested scopes share the monotonic counter and only change the
/// enable depth, so a subphase cannot disable its enclosing measurement.
pub struct AllocationMetricsGuard {
    _private: (),
}

impl Drop for AllocationMetricsGuard {
    fn drop(&mut self) {
        #[cfg(feature = "alloc-metrics")]
        METRICS_DEPTH.with(|depth| depth.set(depth.get().saturating_sub(1)));
    }
}

pub fn begin_allocation_metrics() -> AllocationMetricsGuard {
    #[cfg(feature = "alloc-metrics")]
    METRICS_DEPTH.with(|depth| depth.set(depth.get().saturating_add(1)));
    AllocationMetricsGuard { _private: () }
}

impl MetricsSystemAllocator {
    #[cfg(feature = "alloc-metrics")]
    fn record(size: usize) {
        METRICS_DEPTH.with(|depth| {
            if depth.get() != 0 {
                ALLOCATED_BYTES.with(|bytes| bytes.set(bytes.get().saturating_add(size as u64)));
            }
        });
    }
}

// SAFETY: every operation delegates to `System` with the original pointer and
// layout. Accounting has no effect on allocation identity or lifetime.
unsafe impl GlobalAlloc for MetricsSystemAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let result = unsafe { System.alloc(layout) };
        #[cfg(feature = "alloc-metrics")]
        if !result.is_null() {
            Self::record(layout.size());
        }
        result
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let result = unsafe { System.alloc_zeroed(layout) };
        #[cfg(feature = "alloc-metrics")]
        if !result.is_null() {
            Self::record(layout.size());
        }
        result
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let result = unsafe { System.realloc(ptr, layout, new_size) };
        #[cfg(feature = "alloc-metrics")]
        if !result.is_null() {
            // Account only for newly committed bytes.  Charging the complete
            // destination size double-counts Vec growth and made planner
            // allocation diagnostics systematically pessimistic.
            Self::record(new_size.saturating_sub(layout.size()));
        }
        result
    }
}

pub fn thread_allocated_bytes() -> u64 {
    #[cfg(feature = "alloc-metrics")]
    {
        ALLOCATED_BYTES.with(Cell::get)
    }
    #[cfg(not(feature = "alloc-metrics"))]
    {
        0
    }
}

pub fn allocated_bytes_since(snapshot: u64) -> u64 {
    thread_allocated_bytes().saturating_sub(snapshot)
}

#[cfg(all(test, feature = "alloc-metrics"))]
mod tests {
    use super::*;

    #[test]
    fn realloc_accounts_only_the_growth_delta() {
        let allocator = MetricsSystemAllocator;
        let layout = Layout::from_size_align(8, 8).unwrap();
        let _scope = begin_allocation_metrics();
        let before = thread_allocated_bytes();
        // SAFETY: the layout is valid and the returned allocation is used
        // only through the matching allocator methods below.
        let pointer = unsafe { allocator.alloc(layout) };
        assert!(!pointer.is_null());
        // SAFETY: `pointer` was allocated with `layout`; the destination is
        // retained and released with its new layout.
        let pointer = unsafe { allocator.realloc(pointer, layout, 16) };
        assert!(!pointer.is_null());
        let after = thread_allocated_bytes();
        // One initial allocation plus only the eight newly committed bytes.
        assert_eq!(after.saturating_sub(before), 16);
        // SAFETY: pointer/layout pair matches the successful realloc above.
        unsafe { allocator.dealloc(pointer, Layout::from_size_align(16, 8).unwrap()) };
    }
}
