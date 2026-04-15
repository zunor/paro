use std::fmt;
use std::ptr::NonNull;
use std::sync::Arc;

use crate::allocator::{default_allocator, Allocator};

use super::AllocationSet;

/// Internal managed memory for the buffer.
struct RawBuffer {
    /// Pointer to data
    ptr: NonNull<u8>,
    /// Total size in bytes
    size: usize,
    /// Allocator used for this buffer
    allocator: Arc<dyn Allocator>,
}

impl Drop for RawBuffer {
    fn drop(&mut self) {
        // Use allocator to free memory
        self.allocator.free(self.ptr.as_ptr(), self.size);
    }
}

// RawBuffer is Send+Sync because Allocator is Send+Sync and it owns its memory.
unsafe impl Send for RawBuffer {}
unsafe impl Sync for RawBuffer {}

/// Raw buffer for vector data.
/// Manages memory allocation and provides shared typed access.
#[derive(Clone)]
pub(crate) struct VectorBuffer {
    /// Internal shared data
    inner: Option<Arc<RawBuffer>>,
    /// Size of each element in bytes
    element_size: usize,
    /// Number of elements allocated
    capacity: usize,
}

impl fmt::Debug for VectorBuffer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VectorBuffer")
            .field("element_size", &self.element_size)
            .field("capacity", &self.capacity)
            .field("shared", &self.inner.is_some())
            .finish()
    }
}

impl VectorBuffer {
    /// Create a new buffer with given element size and capacity using default allocator.
    ///
    /// NOTE: This convenience constructor uses `default_allocator()` and is mainly
    /// intended for tests or standalone utility code. Production paths should pass
    /// an explicit allocator via `with_allocator`.
    pub fn new(element_size: usize, capacity: usize) -> Self {
        Self::with_allocator(element_size, capacity, Arc::new(default_allocator()))
    }

    /// Create a new buffer with a custom allocator.
    pub fn with_allocator(
        element_size: usize,
        capacity: usize,
        allocator: Arc<dyn Allocator>,
    ) -> Self {
        if element_size == 0 || capacity == 0 {
            return Self {
                inner: None,
                element_size,
                capacity: 0,
            };
        }

        let size = element_size * capacity;

        // Use allocator to get memory
        let ptr = allocator.allocate_zeroed(size).expect("Allocation failed"); // We expect success locally for now

        Self {
            inner: Some(Arc::new(RawBuffer {
                ptr: NonNull::new(ptr).expect("out of memory or allocator bug: ptr is null"),
                size,
                allocator,
            })),
            element_size,
            capacity,
        }
    }

    /// Get the allocator used by this buffer.
    pub fn allocator(&self) -> &Arc<dyn Allocator> {
        static DEFAULT: std::sync::OnceLock<Arc<dyn Allocator>> = std::sync::OnceLock::new();
        self.inner
            .as_ref()
            .map(|i| &i.allocator)
            .unwrap_or_else(|| {
                DEFAULT.get_or_init(|| Arc::new(crate::allocator::DefaultAllocator::new()))
            })
    }

    /// Get raw data pointer.
    #[inline]
    pub fn data(&self) -> *mut u8 {
        self.inner
            .as_ref()
            .map(|i| i.ptr.as_ptr())
            .unwrap_or(std::ptr::null_mut())
    }

    /// Get the capacity (number of elements).
    #[inline]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Get the element size in bytes.
    #[inline]
    pub fn element_size(&self) -> usize {
        self.element_size
    }

    /// Get the total size in bytes.
    #[inline]
    #[allow(dead_code)]
    pub fn size(&self) -> usize {
        self.inner.as_ref().map(|i| i.size).unwrap_or(0)
    }

    pub(crate) fn allocation_identity(&self) -> Option<usize> {
        self.inner.as_ref().map(|inner| Arc::as_ptr(inner) as usize)
    }

    pub(crate) fn collect_allocation_size(&self, allocations: &mut AllocationSet) -> usize {
        match self.allocation_identity() {
            Some(identity) => allocations.add(identity, self.size()),
            None => 0,
        }
    }

    /// Get typed data slice.
    ///
    /// # Safety
    /// Caller must ensure T matches the actual stored type.
    #[inline]
    pub unsafe fn as_slice<T>(&self, len: usize) -> &[T] {
        if self.inner.is_none() {
            return &[];
        }
        std::slice::from_raw_parts(self.data() as *const T, len)
    }

    /// Get mutable typed data slice.
    ///
    /// # Safety
    /// Caller must ensure T matches the actual stored type and that
    /// this buffer is NOT SHARED.
    #[inline]
    pub unsafe fn as_mut_slice<T>(&mut self, len: usize) -> &mut [T] {
        if self.inner.is_none() {
            return &mut [];
        }
        // SAFETY: The caller must ensure exclusivity if they want a &mut slice.
        // We don't automatically trigger make_mut here to avoid hidden O(N) copies.
        std::slice::from_raw_parts_mut(self.data() as *mut T, len)
    }

    /// Create a deep copy of this buffer.
    pub fn deep_copy(&self) -> Self {
        if let Some(inner) = &self.inner {
            let new_buffer = Self::with_allocator(
                self.element_size,
                self.capacity,
                Arc::clone(&inner.allocator),
            );
            if let Some(new_inner) = &new_buffer.inner {
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        inner.ptr.as_ptr(),
                        new_inner.ptr.as_ptr(),
                        inner.size,
                    );
                }
            }
            new_buffer
        } else {
            Self::new(self.element_size, 0)
        }
    }

    /// Ensure the buffer is exclusively owned (Copy-on-Write).
    pub fn make_exclusive(&mut self) {
        if let Some(inner) = &self.inner {
            if Arc::strong_count(inner) > 1 {
                *self = self.deep_copy();
            }
        }
    }
}

// SAFETY: VectorBuffer owns its memory exclusively. The raw pointer is only accessed
// through the owning VectorBuffer, and Clone creates a deep copy. No shared mutable
// access occurs across threads. Allocator is Send+Sync.
unsafe impl Send for VectorBuffer {}
unsafe impl Sync for VectorBuffer {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::allocator::DefaultAllocator;

    #[test]
    fn test_vector_buffer_new() {
        let buf = VectorBuffer::new(8, 100);
        assert!(!buf.data().is_null());
        assert_eq!(buf.capacity(), 100);
        assert_eq!(buf.element_size(), 8);
        assert_eq!(buf.size(), 800);
    }

    #[test]
    fn test_vector_buffer_zero_size() {
        let buf = VectorBuffer::new(0, 100);
        assert!(buf.data().is_null());
        assert_eq!(buf.capacity(), 0);
    }

    #[test]
    fn test_vector_buffer_zero_capacity() {
        let buf = VectorBuffer::new(8, 0);
        assert!(buf.data().is_null());
        assert_eq!(buf.capacity(), 0);
    }

    #[test]
    fn test_vector_buffer_with_allocator() {
        let allocator = Arc::new(DefaultAllocator::new());
        let buf = VectorBuffer::with_allocator(4, 50, allocator);
        assert!(!buf.data().is_null());
        assert_eq!(buf.size(), 200);
    }

    #[test]
    fn test_vector_buffer_clone() {
        let mut buf = VectorBuffer::new(4, 10);

        // Write some data
        unsafe {
            let slice = buf.as_mut_slice::<i32>(10);
            slice[0] = 42;
        }

        // Clone (shallow)
        let cloned = buf.clone();

        // Verify data is shared
        unsafe {
            let slice = cloned.as_slice::<i32>(10);
            assert_eq!(slice[0], 42);
        }
    }

    #[test]
    fn test_vector_buffer_shallow_clone() {
        let mut buf = VectorBuffer::new(4, 10);
        unsafe {
            buf.as_mut_slice::<i32>(10)[0] = 1;
        }

        let cloned = buf.clone();

        // Modify original
        unsafe {
            buf.as_mut_slice::<i32>(10)[0] = 2;
        }

        // Cloned should see the change (shared)
        unsafe {
            assert_eq!(cloned.as_slice::<i32>(10)[0], 2);
        }
    }

    #[test]
    fn test_vector_buffer_make_exclusive() {
        let mut buf = VectorBuffer::new(4, 10);
        unsafe {
            buf.as_mut_slice::<i32>(10)[0] = 1;
        }

        let cloned = buf.clone();

        // Make original exclusive (should trigger deep copy)
        buf.make_exclusive();

        // Modify original
        unsafe {
            buf.as_mut_slice::<i32>(10)[0] = 2;
        }

        // Cloned should NOT see the change
        unsafe {
            assert_eq!(cloned.as_slice::<i32>(10)[0], 1);
            assert_eq!(buf.as_slice::<i32>(10)[0], 2);
        }
    }
}
