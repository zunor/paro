//! AllocatedData - RAII wrapper for allocated memory.

use std::sync::Arc;

use super::Allocator;

/// RAII wrapper for allocated memory.
///
/// Automatically frees memory when dropped.
/// Cannot be copied, only moved.
///
/// # Example
/// ```ignore
/// use paro_common::allocator::{AllocatedData, DefaultAllocator};
///
/// let allocator = Arc::new(DefaultAllocator::new());
/// let data = AllocatedData::new(allocator.clone(), 1024)?;
/// // Memory automatically freed when `data` goes out of scope
/// ```
pub struct AllocatedData {
    /// Pointer to allocated memory
    ptr: *mut u8,
    /// Size of allocation in bytes
    size: usize,
    /// Allocator that owns this memory (for deallocation)
    allocator: Arc<dyn Allocator>,
}

impl AllocatedData {
    /// Create a new allocated data block.
    ///
    /// Allocates `size` bytes of zeroed memory.
    pub fn new(allocator: Arc<dyn Allocator>, size: usize) -> crate::error::Result<Self> {
        let ptr = allocator.allocate_zeroed(size)?;
        Ok(Self {
            ptr,
            size,
            allocator,
        })
    }

    /// Create from an existing allocation.
    ///
    /// # Safety
    /// - `ptr` must be a valid allocation from `allocator`
    /// - `size` must match the allocation size
    pub unsafe fn from_raw(allocator: Arc<dyn Allocator>, ptr: *mut u8, size: usize) -> Self {
        Self {
            ptr,
            size,
            allocator,
        }
    }

    /// Get the raw pointer.
    #[inline]
    pub fn get(&self) -> *mut u8 {
        self.ptr
    }

    /// Get the raw pointer as const.
    #[inline]
    pub fn get_const(&self) -> *const u8 {
        self.ptr
    }

    /// Get the size of the allocation.
    #[inline]
    pub fn size(&self) -> usize {
        self.size
    }

    /// Check if the allocation is valid (non-null).
    #[inline]
    pub fn is_set(&self) -> bool {
        !self.ptr.is_null()
    }

    /// Get typed slice of the data.
    ///
    /// # Safety
    /// Caller must ensure T matches the actual stored data.
    #[inline]
    pub unsafe fn as_slice<T>(&self) -> &[T] {
        if self.ptr.is_null() {
            return &[];
        }
        let count = self.size / std::mem::size_of::<T>();
        std::slice::from_raw_parts(self.ptr as *const T, count)
    }

    /// Get mutable typed slice of the data.
    ///
    /// # Safety
    /// Caller must ensure T matches the actual stored data.
    #[inline]
    pub unsafe fn as_mut_slice<T>(&mut self) -> &mut [T] {
        if self.ptr.is_null() {
            return &mut [];
        }
        let count = self.size / std::mem::size_of::<T>();
        std::slice::from_raw_parts_mut(self.ptr as *mut T, count)
    }

    /// Reset the allocation (free and set to null).
    pub fn reset(&mut self) {
        if !self.ptr.is_null() {
            self.allocator.free(self.ptr, self.size);
            self.ptr = std::ptr::null_mut();
            self.size = 0;
        }
    }

    /// Take ownership of the raw pointer, preventing automatic deallocation.
    ///
    /// Returns (pointer, size). The caller is now responsible for freeing.
    pub fn take(mut self) -> (*mut u8, usize) {
        let ptr = self.ptr;
        let size = self.size;
        self.ptr = std::ptr::null_mut();
        self.size = 0;
        (ptr, size)
    }
}

impl Drop for AllocatedData {
    fn drop(&mut self) {
        if !self.ptr.is_null() {
            self.allocator.free(self.ptr, self.size);
        }
    }
}

// AllocatedData cannot be copied, only moved
// This is enforced by not implementing Clone

// SAFETY: AllocatedData owns its memory exclusively and the allocator is Send+Sync
unsafe impl Send for AllocatedData {}
unsafe impl Sync for AllocatedData {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::allocator::DefaultAllocator;

    #[test]
    fn test_allocated_data_new() {
        let allocator = Arc::new(DefaultAllocator::new());
        let data = AllocatedData::new(allocator, 64).unwrap();
        assert!(data.is_set());
        assert_eq!(data.size(), 64);
    }

    #[test]
    fn test_allocated_data_reset() {
        let allocator = Arc::new(DefaultAllocator::new());
        let mut data = AllocatedData::new(allocator, 64).unwrap();
        assert!(data.is_set());
        data.reset();
        assert!(!data.is_set());
        assert_eq!(data.size(), 0);
    }

    #[test]
    fn test_allocated_data_take() {
        let allocator = Arc::new(DefaultAllocator::new());
        let data = AllocatedData::new(allocator.clone(), 64).unwrap();
        let (ptr, size) = data.take();
        assert!(!ptr.is_null());
        assert_eq!(size, 64);
        // Manual cleanup
        allocator.free(ptr, size);
    }

    #[test]
    fn test_allocated_data_as_slice() {
        let allocator = Arc::new(DefaultAllocator::new());
        let mut data = AllocatedData::new(allocator, 32).unwrap();

        // Write some data
        unsafe {
            let slice = data.as_mut_slice::<i32>();
            slice[0] = 42;
            slice[1] = 100;
        }

        // Read it back
        unsafe {
            let slice = data.as_slice::<i32>();
            assert_eq!(slice[0], 42);
            assert_eq!(slice[1], 100);
        }
    }
}
