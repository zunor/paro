// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Allocation-volume accounting for short-lived planning work.
//!
//! The counter is thread-local so concurrent sessions do not contaminate one
//! another. Binaries opt in by installing [`MetricsSystemAllocator`] as their
//! global allocator; library users and tests safely observe zero.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

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
        METRICS_DEPTH.with(|depth| depth.set(depth.get().saturating_sub(1)));
    }
}

pub fn begin_allocation_metrics() -> AllocationMetricsGuard {
    METRICS_DEPTH.with(|depth| depth.set(depth.get().saturating_add(1)));
    AllocationMetricsGuard { _private: () }
}

impl MetricsSystemAllocator {
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
        if !result.is_null() {
            Self::record(layout.size());
        }
        result
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let result = unsafe { System.alloc_zeroed(layout) };
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
        if !result.is_null() {
            Self::record(new_size);
        }
        result
    }
}

pub fn thread_allocated_bytes() -> u64 {
    ALLOCATED_BYTES.with(Cell::get)
}

pub fn allocated_bytes_since(snapshot: u64) -> u64 {
    thread_allocated_bytes().saturating_sub(snapshot)
}
