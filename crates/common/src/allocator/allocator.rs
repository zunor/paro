// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Allocator trait - unified memory allocation interface.

use crate::error::Result;

/// Maximum allocation size (281 TB).
pub const MAXIMUM_ALLOC_SIZE: usize = 281_474_976_710_656;

/// Unified memory allocator trait.
///
/// All memory allocation goes through this interface so the system can swap
/// allocator implementations without changing callers.
///
/// # Example
/// ```ignore
/// use paro_common::allocator::{Allocator, DefaultAllocator};
///
/// let allocator = DefaultAllocator::new();
/// let ptr = allocator.allocate(1024)?;
/// // ... use memory ...
/// allocator.free(ptr, 1024);
/// ```
pub trait Allocator: Send + Sync {
    /// Allocate memory of the given size.
    ///
    /// Returns a raw pointer to the allocated memory.
    /// The memory is NOT initialized (contains garbage).
    ///
    /// # Errors
    /// Returns an error if allocation fails.
    fn allocate(&self, size: usize) -> Result<*mut u8>;

    /// Allocate zeroed memory of the given size.
    ///
    /// Returns a raw pointer to the allocated memory.
    /// The memory is initialized to zero.
    ///
    /// # Errors
    /// Returns an error if allocation fails.
    fn allocate_zeroed(&self, size: usize) -> Result<*mut u8>;

    /// Free previously allocated memory.
    ///
    /// # Safety
    /// - `ptr` must have been allocated by this allocator
    /// - `size` must match the original allocation size
    /// - `ptr` must not be used after this call
    ///
    /// # Panics
    /// May panic if ptr is null or was not allocated by this allocator.
    fn free(&self, ptr: *mut u8, size: usize);

    /// Reallocate memory to a new size.
    ///
    /// If the new size is larger, the extra bytes are NOT initialized.
    /// The original content is preserved up to `min(old_size, new_size)`.
    ///
    /// # Safety
    /// - `ptr` must have been allocated by this allocator
    /// - `old_size` must match the original allocation size
    ///
    /// # Errors
    /// Returns an error if reallocation fails. In this case, the original
    /// allocation is still valid.
    fn reallocate(&self, ptr: *mut u8, old_size: usize, new_size: usize) -> Result<*mut u8>;

    /// Get the name of this allocator (for debugging).
    fn name(&self) -> &'static str {
        "Allocator"
    }

    /// Check if this allocator supports flushing thread-local caches.
    fn supports_flush(&self) -> bool {
        false
    }

    /// Flush thread-local caches of the allocator.
    ///
    /// # Arguments
    /// * `_background_threads` - Whether the allocator uses background threads
    /// * `_threshold` - Minimum bytes to flush
    /// * `_thread_count` - Approximate number of threads using this allocator
    fn thread_flush(&self, _background_threads: bool, _threshold: usize, _thread_count: usize) {}

    /// Notify the allocator that the current thread is idle.
    fn thread_idle(&self) {}

    /// Get the delay (in seconds) before the allocator decays thread-local caches.
    /// Returns None if no decay delay is specified.
    fn decay_delay(&self) -> Option<usize> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::allocator::DefaultAllocator;

    #[test]
    fn test_allocator_max_size() {
        // 281 TB
        assert_eq!(MAXIMUM_ALLOC_SIZE, 281_474_976_710_656);
    }

    #[test]
    fn test_allocator_trait_object() {
        let allocator: Box<dyn Allocator> = Box::new(DefaultAllocator::new());
        let ptr = allocator.allocate(64).unwrap();
        assert!(!ptr.is_null());
        allocator.free(ptr, 64);
    }
}
