// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Allocator debug tracking.
//!
//! This is only enabled in debug builds and tracks allocator leak diagnostics:
//! - track outstanding bytes
//! - optionally track pointers + stack traces (`debug-allocation` feature)
//! - assert no outstanding allocations on drop

#[cfg(feature = "debug-allocation")]
use std::backtrace::Backtrace;
#[cfg(feature = "debug-allocation")]
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
#[cfg(feature = "debug-allocation")]
use std::sync::Mutex;

/// Debug allocation tracker for allocators.
#[derive(Debug)]
pub(crate) struct AllocatorDebugInfo {
    allocator_name: &'static str,
    /// Outstanding allocated bytes.
    outstanding_bytes: AtomicUsize,
    #[cfg(feature = "debug-allocation")]
    pointers: Mutex<HashMap<usize, (usize, String)>>,
}

impl AllocatorDebugInfo {
    pub(crate) fn new(allocator_name: &'static str) -> Self {
        Self {
            allocator_name,
            outstanding_bytes: AtomicUsize::new(0),
            #[cfg(feature = "debug-allocation")]
            pointers: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) fn record_allocate(&self, ptr: *mut u8, size: usize) {
        if ptr.is_null() || size == 0 {
            return;
        }
        self.outstanding_bytes.fetch_add(size, Ordering::AcqRel);

        #[cfg(feature = "debug-allocation")]
        {
            let mut pointers = self.pointers.lock().unwrap();
            pointers.insert(ptr as usize, (size, capture_stack_trace()));
        }
    }

    pub(crate) fn record_free(&self, ptr: *mut u8, size: usize) {
        if ptr.is_null() || size == 0 {
            return;
        }

        let old = self.outstanding_bytes.fetch_sub(size, Ordering::AcqRel);
        assert!(
            old >= size,
            "{}: free {} bytes exceeds outstanding {} bytes",
            self.allocator_name,
            size,
            old
        );

        #[cfg(feature = "debug-allocation")]
        {
            let mut pointers = self.pointers.lock().unwrap();
            let Some((tracked_size, _)) = pointers.remove(&(ptr as usize)) else {
                panic!(
                    "{}: free for unknown pointer {:p}",
                    self.allocator_name, ptr
                );
            };
            assert_eq!(
                tracked_size, size,
                "{}: free size mismatch for pointer {:p}, tracked={}, actual={}",
                self.allocator_name, ptr, tracked_size, size
            );
        }
    }

    pub(crate) fn record_reallocate(
        &self,
        old_ptr: *mut u8,
        new_ptr: *mut u8,
        old_size: usize,
        new_size: usize,
    ) {
        self.record_free(old_ptr, old_size);
        self.record_allocate(new_ptr, new_size);
    }

    pub(crate) fn outstanding_bytes(&self) -> usize {
        self.outstanding_bytes.load(Ordering::Acquire)
    }
}

impl Drop for AllocatorDebugInfo {
    fn drop(&mut self) {
        let outstanding = self.outstanding_bytes();
        if outstanding == 0 {
            return;
        }

        #[cfg(feature = "debug-allocation")]
        {
            let pointers = self.pointers.lock().unwrap();
            let mut leak_report = String::new();
            for (ptr, (size, trace)) in pointers.iter() {
                use std::fmt::Write;
                let _ = writeln!(
                    leak_report,
                    "LEAK: {} bytes at {:p}\n{}",
                    size, *ptr as *const u8, trace
                );
            }
            panic!(
                "AllocatorDebugInfo({}): {} outstanding bytes\n{}",
                self.allocator_name, outstanding, leak_report
            );
        }

        #[cfg(not(feature = "debug-allocation"))]
        panic!(
            "AllocatorDebugInfo({}): {} outstanding bytes",
            self.allocator_name, outstanding
        );
    }
}

#[cfg(feature = "debug-allocation")]
fn capture_stack_trace() -> String {
    format!("{:?}", Backtrace::force_capture())
}
