// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::fmt;
use std::ptr::NonNull;
use std::sync::Arc;

use bytes::Bytes;

use crate::allocator::Allocator;
use crate::error::{self as paro_error, Result};

use super::AllocationSet;

/// Internal managed memory for the buffer.
struct RawBuffer {
    /// Pointer to data
    ptr: NonNull<u8>,
    /// Total size in bytes
    size: usize,
    storage: RawBufferStorage,
}

enum RawBufferStorage {
    Allocated(Arc<dyn Allocator>),
    External { _owner: Bytes },
}

impl Drop for RawBuffer {
    fn drop(&mut self) {
        if let RawBufferStorage::Allocated(allocator) = &self.storage {
            allocator.free(self.ptr.as_ptr(), self.size);
        }
    }
}

impl RawBuffer {
    #[inline]
    fn is_mutable(&self) -> bool {
        matches!(self.storage, RawBufferStorage::Allocated(_))
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
    /// Allocator associated with this buffer, even for zero-capacity buffers.
    allocator: Arc<dyn Allocator>,
}

impl fmt::Debug for VectorBuffer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VectorBuffer")
            .field("element_size", &self.element_size)
            .field("capacity", &self.capacity)
            .field("allocator", &self.allocator.name())
            .field("shared", &self.inner.is_some())
            .finish()
    }
}

impl VectorBuffer {
    /// Create a new buffer with a custom allocator.
    pub fn try_with_allocator(
        element_size: usize,
        capacity: usize,
        allocator: Arc<dyn Allocator>,
    ) -> Result<Self> {
        if element_size == 0 || capacity == 0 {
            return Ok(Self {
                inner: None,
                element_size,
                capacity: 0,
                allocator,
            });
        }

        let size = element_size.checked_mul(capacity).ok_or_else(|| {
            paro_error::out_of_memory(format!(
                "vector buffer allocation size overflow: element_size={element_size}, capacity={capacity}"
            ))
        })?;

        // Use allocator to get memory
        let ptr = allocator.allocate_zeroed(size)?;

        Ok(Self {
            inner: Some(Arc::new(RawBuffer {
                ptr: NonNull::new(ptr).expect("out of memory or allocator bug: ptr is null"),
                size,
                storage: RawBufferStorage::Allocated(allocator.clone()),
            })),
            element_size,
            capacity,
            allocator,
        })
    }

    /// Adopt immutable, fixed-width vector storage without copying it.
    ///
    /// Mutation uses the normal vector copy-on-write path and therefore first
    /// materializes an allocator-owned buffer. Misaligned byte storage is
    /// copied eagerly because typed vector readers require physical alignment.
    pub fn try_from_bytes(
        element_size: usize,
        capacity: usize,
        bytes: Bytes,
        allocator: Arc<dyn Allocator>,
    ) -> Result<Self> {
        let expected_size = element_size.checked_mul(capacity).ok_or_else(|| {
            paro_error::out_of_memory(format!(
                "external vector buffer size overflow: element_size={element_size}, capacity={capacity}"
            ))
        })?;
        if bytes.len() != expected_size {
            return Err(paro_error::invalid_input(format!(
                "external vector buffer length mismatch: expected={expected_size}, actual={}",
                bytes.len()
            )));
        }
        if expected_size == 0 {
            return Ok(Self {
                inner: None,
                element_size,
                capacity: 0,
                allocator,
            });
        }

        let required_alignment = element_size.min(std::mem::align_of::<u128>()).max(1);
        if !(bytes.as_ptr() as usize).is_multiple_of(required_alignment) {
            let owned = Self::try_with_allocator(element_size, capacity, allocator)?;
            // SAFETY: both buffers were validated to contain `expected_size`
            // bytes and do not overlap because `owned` was freshly allocated.
            unsafe {
                std::ptr::copy_nonoverlapping(bytes.as_ptr(), owned.data(), expected_size);
            }
            return Ok(owned);
        }

        let ptr = NonNull::new(bytes.as_ptr() as *mut u8)
            .expect("non-empty Bytes must expose a non-null pointer");
        Ok(Self {
            inner: Some(Arc::new(RawBuffer {
                ptr,
                size: expected_size,
                storage: RawBufferStorage::External { _owner: bytes },
            })),
            element_size,
            capacity,
            allocator,
        })
    }

    /// Get the allocator used by this buffer.
    pub fn allocator(&self) -> &Arc<dyn Allocator> {
        &self.allocator
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

    #[inline]
    pub(crate) fn is_uniquely_owned(&self) -> bool {
        self.inner
            .as_ref()
            .is_none_or(|inner| inner.is_mutable() && Arc::strong_count(inner) == 1)
    }

    pub(crate) fn collect_allocation_size(&self, allocations: &mut AllocationSet) -> usize {
        match self.allocation_identity() {
            Some(identity) => allocations.add(identity, self.size()),
            None => 0,
        }
    }

    pub(crate) fn collect_allocation_entries(&self, entries: &mut Vec<(usize, usize)>) {
        if let Some(identity) = self.allocation_identity() {
            let size = self.size();
            if size > 0 {
                entries.push((identity, size));
            }
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
    pub fn try_deep_copy(&self) -> Result<Self> {
        if let Some(inner) = &self.inner {
            let new_buffer =
                Self::try_with_allocator(self.element_size, self.capacity, self.allocator.clone())?;
            if let Some(new_inner) = &new_buffer.inner {
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        inner.ptr.as_ptr(),
                        new_inner.ptr.as_ptr(),
                        inner.size,
                    );
                }
            }
            Ok(new_buffer)
        } else {
            Self::try_with_allocator(self.element_size, 0, self.allocator().clone())
        }
    }

    /// Ensure the buffer is exclusively owned (Copy-on-Write).
    pub fn try_make_exclusive(&mut self) -> Result<()> {
        if let Some(inner) = &self.inner {
            if !inner.is_mutable() || Arc::strong_count(inner) > 1 {
                *self = self.try_deep_copy()?;
            }
        }
        Ok(())
    }
}

// SAFETY: allocated storage follows the existing COW protocol before mutable
// vector access; external storage is retained by immutable `Bytes` and is also
// copied before mutation. The raw pointer stays valid for the lifetime of the
// `RawBuffer`, and the allocator is Send + Sync.
unsafe impl Send for VectorBuffer {}
unsafe impl Sync for VectorBuffer {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::allocator::DefaultAllocator;

    #[test]
    fn test_vector_buffer_new() {
        let buf =
            VectorBuffer::try_with_allocator(8, 100, Arc::new(DefaultAllocator::new())).unwrap();
        assert!(!buf.data().is_null());
        assert_eq!(buf.capacity(), 100);
        assert_eq!(buf.element_size(), 8);
        assert_eq!(buf.size(), 800);
    }

    #[test]
    fn test_vector_buffer_zero_size() {
        let buf =
            VectorBuffer::try_with_allocator(0, 100, Arc::new(DefaultAllocator::new())).unwrap();
        assert!(buf.data().is_null());
        assert_eq!(buf.capacity(), 0);
    }

    #[test]
    fn test_vector_buffer_zero_capacity() {
        let buf =
            VectorBuffer::try_with_allocator(8, 0, Arc::new(DefaultAllocator::new())).unwrap();
        assert!(buf.data().is_null());
        assert_eq!(buf.capacity(), 0);
    }

    #[test]
    fn test_vector_buffer_with_allocator() {
        let allocator = Arc::new(DefaultAllocator::new());
        let buf = VectorBuffer::try_with_allocator(4, 50, allocator).unwrap();
        assert!(!buf.data().is_null());
        assert_eq!(buf.size(), 200);
    }

    #[test]
    fn test_vector_buffer_clone() {
        let mut buf =
            VectorBuffer::try_with_allocator(4, 10, Arc::new(DefaultAllocator::new())).unwrap();

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
        let mut buf =
            VectorBuffer::try_with_allocator(4, 10, Arc::new(DefaultAllocator::new())).unwrap();
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
        let mut buf =
            VectorBuffer::try_with_allocator(4, 10, Arc::new(DefaultAllocator::new())).unwrap();
        unsafe {
            buf.as_mut_slice::<i32>(10)[0] = 1;
        }

        let cloned = buf.clone();

        // Make original exclusive (should trigger deep copy)
        buf.try_make_exclusive().unwrap();

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

    #[test]
    fn external_bytes_are_borrowed_until_mutation() {
        let bytes = Bytes::from(vec![1_u8, 2, 3, 4]);
        let source_ptr = bytes.as_ptr();
        let mut buffer = VectorBuffer::try_from_bytes(
            1,
            bytes.len(),
            bytes.clone(),
            Arc::new(DefaultAllocator::new()),
        )
        .unwrap();

        assert_eq!(buffer.data(), source_ptr as *mut u8);
        assert!(!buffer.is_uniquely_owned());
        buffer.try_make_exclusive().unwrap();
        assert_ne!(buffer.data(), source_ptr as *mut u8);
        unsafe { buffer.as_mut_slice::<u8>(4)[0] = 9 };
        assert_eq!(bytes[0], 1);
    }
}
