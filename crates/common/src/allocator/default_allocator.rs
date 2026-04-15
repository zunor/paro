//! DefaultAllocator - standard system allocator.
//!
//! ## Usage Guidelines
//!
//! **For Tests Only**: This allocator is intended for unit tests and simple examples.
//! Production code should use `BufferAllocator` which integrates with the BufferPool
//! for proper memory tracking and management.
//!
//! **Why not use in production?**
//! - No memory tracking (BufferPool doesn't know about allocations)
//! - No memory limits enforcement
//! - No spill-to-disk support
//!
//! **Use BufferAllocator instead**:
//! ```ignore
//! // In Session/Executor context:
//! let allocator = ctx.allocator(MemoryTag::Operator);
//!
//! // Or directly:
//! let allocator = BufferAllocator::new(buffer_pool.clone(), MemoryTag::Operator);
//! ```

use std::alloc::{alloc, alloc_zeroed, dealloc, realloc, Layout};
#[cfg(debug_assertions)]
use std::sync::Arc;

use crate::error::{self as paro_error, Result};

#[cfg(debug_assertions)]
use super::debug_info::AllocatorDebugInfo;
use super::Allocator;

/// Default allocator using standard system allocation.
///
/// This is the simplest allocator that directly uses `std::alloc`.
/// It does NOT track memory usage or integrate with `BufferManager`.
///
/// # ⚠️ For Tests Only
///
/// This allocator should only be used in unit tests and simple examples.
/// Production code (Session, Executor, Connection) should use `BufferAllocator`
/// which integrates with the BufferPool for proper memory tracking.
#[derive(Debug, Clone)]
pub struct DefaultAllocator {
    #[cfg(debug_assertions)]
    debug_info: Arc<AllocatorDebugInfo>,
}

impl DefaultAllocator {
    /// Create a new default allocator.
    pub fn new() -> Self {
        Self {
            #[cfg(debug_assertions)]
            debug_info: Arc::new(AllocatorDebugInfo::new("DefaultAllocator")),
        }
    }
}

impl Default for DefaultAllocator {
    fn default() -> Self {
        Self::new()
    }
}

impl Allocator for DefaultAllocator {
    fn allocate(&self, size: usize) -> Result<*mut u8> {
        if size == 0 {
            return Ok(std::ptr::null_mut());
        }

        if size > super::allocator::MAXIMUM_ALLOC_SIZE {
            return Err(paro_error::out_of_memory(format!(
                "Allocation size {} exceeds maximum {}",
                size,
                super::allocator::MAXIMUM_ALLOC_SIZE
            )));
        }

        let layout = Layout::from_size_align(size, 8)
            .map_err(|e| paro_error::internal(format!("Invalid allocation layout: {}", e)))?;

        // SAFETY: layout is valid, checked above
        let ptr = unsafe { alloc(layout) };

        if ptr.is_null() {
            Err(paro_error::out_of_memory(format!(
                "Failed to allocate {} bytes",
                size
            )))
        } else {
            #[cfg(debug_assertions)]
            self.debug_info.record_allocate(ptr, size);
            Ok(ptr)
        }
    }

    fn allocate_zeroed(&self, size: usize) -> Result<*mut u8> {
        if size == 0 {
            return Ok(std::ptr::null_mut());
        }

        if size > super::allocator::MAXIMUM_ALLOC_SIZE {
            return Err(paro_error::out_of_memory(format!(
                "Allocation size {} exceeds maximum {}",
                size,
                super::allocator::MAXIMUM_ALLOC_SIZE
            )));
        }

        let layout = Layout::from_size_align(size, 8)
            .map_err(|e| paro_error::internal(format!("Invalid allocation layout: {}", e)))?;

        // SAFETY: layout is valid, checked above
        let ptr = unsafe { alloc_zeroed(layout) };

        if ptr.is_null() {
            Err(paro_error::out_of_memory(format!(
                "Failed to allocate {} bytes",
                size
            )))
        } else {
            #[cfg(debug_assertions)]
            self.debug_info.record_allocate(ptr, size);
            Ok(ptr)
        }
    }

    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn free(&self, ptr: *mut u8, size: usize) {
        if ptr.is_null() || size == 0 {
            return;
        }

        #[cfg(debug_assertions)]
        self.debug_info.record_free(ptr, size);

        // SAFETY: ptr was allocated by us with this layout
        unsafe {
            let layout = Layout::from_size_align_unchecked(size, 8);
            dealloc(ptr, layout);
        }
    }

    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn reallocate(&self, ptr: *mut u8, old_size: usize, new_size: usize) -> Result<*mut u8> {
        if new_size == 0 {
            self.free(ptr, old_size);
            return Ok(std::ptr::null_mut());
        }

        if ptr.is_null() || old_size == 0 {
            return self.allocate(new_size);
        }

        if new_size > super::allocator::MAXIMUM_ALLOC_SIZE {
            return Err(paro_error::out_of_memory(format!(
                "Reallocation size {} exceeds maximum {}",
                new_size,
                super::allocator::MAXIMUM_ALLOC_SIZE
            )));
        }

        // SAFETY: ptr was allocated by us, old layout is valid
        let new_ptr = unsafe {
            let old_layout = Layout::from_size_align_unchecked(old_size, 8);
            realloc(ptr, old_layout, new_size)
        };

        if new_ptr.is_null() {
            Err(paro_error::out_of_memory(format!(
                "Failed to reallocate from {} to {} bytes",
                old_size, new_size
            )))
        } else {
            #[cfg(debug_assertions)]
            self.debug_info
                .record_reallocate(ptr, new_ptr, old_size, new_size);
            Ok(new_ptr)
        }
    }

    fn name(&self) -> &'static str {
        "DefaultAllocator"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_allocator_allocate() {
        let allocator = DefaultAllocator::new();
        let ptr = allocator.allocate(64).unwrap();
        assert!(!ptr.is_null());
        allocator.free(ptr, 64);
    }

    #[test]
    fn test_default_allocator_allocate_zeroed() {
        let allocator = DefaultAllocator::new();
        let ptr = allocator.allocate_zeroed(64).unwrap();
        assert!(!ptr.is_null());

        // Check that memory is zeroed
        unsafe {
            let slice = std::slice::from_raw_parts(ptr, 64);
            for &byte in slice {
                assert_eq!(byte, 0);
            }
        }

        allocator.free(ptr, 64);
    }

    #[test]
    fn test_default_allocator_zero_size() {
        let allocator = DefaultAllocator::new();
        let ptr = allocator.allocate(0).unwrap();
        assert!(ptr.is_null());
    }

    #[test]
    fn test_default_allocator_reallocate() {
        let allocator = DefaultAllocator::new();

        // Allocate and write
        let ptr = allocator.allocate_zeroed(64).unwrap();
        unsafe {
            let slice = std::slice::from_raw_parts_mut(ptr, 64);
            slice[0] = 42;
            slice[63] = 100;
        }

        // Reallocate to larger
        let new_ptr = allocator.reallocate(ptr, 64, 128).unwrap();
        assert!(!new_ptr.is_null());

        // Check original data preserved
        unsafe {
            let slice = std::slice::from_raw_parts(new_ptr, 128);
            assert_eq!(slice[0], 42);
            assert_eq!(slice[63], 100);
        }

        allocator.free(new_ptr, 128);
    }

    #[test]
    fn test_default_allocator_reallocate_to_zero() {
        let allocator = DefaultAllocator::new();
        let ptr = allocator.allocate(64).unwrap();
        let new_ptr = allocator.reallocate(ptr, 64, 0).unwrap();
        assert!(new_ptr.is_null());
    }
}
