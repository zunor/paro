// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! String heap - arena allocator for strings.
//!
//! Uses an arena allocator to provide stable backing storage for out-of-line
//! strings referenced by `StringView`.

use std::sync::Arc;

use crate::allocator::{Allocator, ArenaAllocator, DefaultAllocator};
use crate::error::Result;
use crate::types::{StringView, INLINE_CAPACITY};

/// Arena allocator for string data.
///
/// Uses `ArenaAllocator` for memory management. Memory is never moved once
/// allocated, so pointers returned by `add_string` remain valid until the
/// `StringHeap` is destroyed.
#[derive(Debug)]
pub struct StringHeap {
    /// Arena allocator for string storage
    allocator: ArenaAllocator,
}

impl Default for StringHeap {
    fn default() -> Self {
        Self::new()
    }
}

impl StringHeap {
    /// Create a new empty string heap.
    pub fn new() -> Self {
        let allocator = Arc::new(DefaultAllocator::new());
        Self {
            allocator: ArenaAllocator::new(allocator),
        }
    }

    /// Create a string heap with pre-allocated capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        let allocator = Arc::new(DefaultAllocator::new());
        Self {
            allocator: ArenaAllocator::with_capacity(allocator, capacity),
        }
    }

    /// Create with custom allocator.
    pub fn with_allocator(capacity: usize, allocator: Arc<dyn Allocator>) -> Self {
        Self {
            allocator: ArenaAllocator::with_capacity(allocator, capacity),
        }
    }

    /// Add a string to the heap, returning an `StringView`.
    #[cfg(test)]
    #[inline]
    pub fn add_string(&mut self, s: &str) -> StringView {
        self.try_add_string(s)
            .expect("test string heap allocation failed")
    }

    /// Add a string to the heap, returning an `StringView`.
    #[inline]
    pub fn try_add_string(&mut self, s: &str) -> Result<StringView> {
        self.try_add_blob(s.as_bytes())
    }

    /// Add bytes directly to the heap, returning an `StringView`.
    #[cfg(test)]
    #[inline]
    pub fn add_blob(&mut self, bytes: &[u8]) -> StringView {
        self.try_add_blob(bytes)
            .expect("test string heap allocation failed")
    }

    /// Add bytes directly to the heap, returning an `StringView`.
    #[inline]
    pub fn try_add_blob(&mut self, bytes: &[u8]) -> Result<StringView> {
        let len = bytes.len();

        if len <= INLINE_CAPACITY {
            return Ok(StringView::from_bytes(bytes));
        }

        let result = self.try_alloc_uninit(len)?;

        unsafe {
            let ptr = result.get_data() as *mut u8;
            std::ptr::copy_nonoverlapping(bytes.as_ptr(), ptr, len);
        }

        let mut result = result;
        result.finalize();
        Ok(result)
    }

    /// Allocate uninitialized out-of-line string storage of the given length.
    ///
    /// Returns an StringView with uninitialized data. The caller must
    /// fill in the data and call `finalize()` on the result.
    ///
    /// # Panics
    /// Panics in debug builds if len <= INLINE_CAPACITY (use StringView::from_bytes
    /// directly for short strings).
    #[inline]
    pub fn try_alloc_uninit(&mut self, len: usize) -> Result<StringView> {
        debug_assert!(
            len > INLINE_CAPACITY,
            "try_alloc_uninit should only be called for strings > {} bytes",
            INLINE_CAPACITY
        );

        let ptr = self.allocator.allocate(len)?;

        Ok(unsafe { StringView::from_ptr(ptr, len as u32) })
    }

    /// Total bytes used in the heap.
    pub fn size(&self) -> usize {
        self.allocator.size_in_bytes()
    }

    /// Total allocation size (may be larger than size due to arena chunks).
    pub fn allocation_size(&self) -> usize {
        self.allocator.allocation_size()
    }

    pub fn allocation_identity(&self) -> usize {
        self as *const Self as usize
    }

    /// Check if heap is empty.
    pub fn is_empty(&self) -> bool {
        self.allocator.is_empty()
    }

    /// Clear the heap (reset arena, keep first chunk).
    pub fn clear(&mut self) {
        self.allocator.reset();
    }

    /// Destroy the heap, freeing all memory.
    pub fn destroy(&mut self) {
        self.allocator.destroy();
    }

    /// Move the contents of this heap to another heap.
    pub fn move_to(&mut self, other: &mut StringHeap) {
        self.allocator.move_to(&mut other.allocator);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::allocator::{Allocator, DefaultAllocator};
    use crate::error::{self as paro_error, Result};
    use std::sync::Arc;

    #[derive(Debug)]
    struct FailingAllocator;

    impl Allocator for FailingAllocator {
        fn allocate(&self, size: usize) -> Result<*mut u8> {
            Err(paro_error::out_of_memory(format!(
                "injected allocation failure: {size} bytes"
            )))
        }

        fn allocate_zeroed(&self, size: usize) -> Result<*mut u8> {
            self.allocate(size)
        }

        fn free(&self, _ptr: *mut u8, _size: usize) {}

        fn reallocate(&self, _ptr: *mut u8, _old_size: usize, new_size: usize) -> Result<*mut u8> {
            self.allocate(new_size)
        }

        fn name(&self) -> &'static str {
            "FailingAllocator"
        }
    }

    #[test]
    fn test_string_heap_new() {
        let heap = StringHeap::new();
        assert!(heap.is_empty());
        assert_eq!(heap.size(), 0);
    }

    #[test]
    fn test_string_heap_add_short_string() {
        let mut heap = StringHeap::new();

        // Short string should be inlined
        let s = heap.add_string("hello");
        assert!(s.is_inlined());
        assert_eq!(s.as_str(), "hello");

        // Heap should still be empty (no allocation for inlined strings)
        assert_eq!(heap.size(), 0);
    }

    #[test]
    fn test_string_heap_add_long_string() {
        let mut heap = StringHeap::new();

        // Long string should be stored in heap
        let long_str = "this is a very long string that exceeds inline length";
        let s = heap.add_string(long_str);
        assert!(!s.is_inlined());
        assert_eq!(s.as_str(), long_str);

        // Heap should have allocated memory
        assert!(heap.size() > 0);
    }

    #[test]
    fn test_string_heap_pointer_stability() {
        let mut heap = StringHeap::new();

        // Add multiple long strings
        let s1 = heap.add_string("first long string that needs heap allocation");
        let s2 = heap.add_string("second long string that needs heap allocation");
        let s3 = heap.add_string("third long string that needs heap allocation");

        // All pointers should still be valid
        assert_eq!(s1.as_str(), "first long string that needs heap allocation");
        assert_eq!(s2.as_str(), "second long string that needs heap allocation");
        assert_eq!(s3.as_str(), "third long string that needs heap allocation");
    }

    #[test]
    fn test_string_heap_add_blob() {
        let mut heap = StringHeap::new();

        // Short blob
        let short_blob = heap.add_blob(b"hello");
        assert!(short_blob.is_inlined());
        assert_eq!(short_blob.as_bytes(), b"hello");

        // Long blob
        let long_data = b"this is a very long blob that exceeds inline length";
        let long_blob = heap.add_blob(long_data);
        assert!(!long_blob.is_inlined());
        assert_eq!(long_blob.as_bytes(), long_data);
    }

    #[test]
    fn test_string_heap_try_alloc_uninit() {
        let mut heap = StringHeap::new();

        // Allocate uninitialized storage of specific length
        let len = 50;
        let mut s = heap.try_alloc_uninit(len).unwrap();

        // Fill with data
        unsafe {
            let ptr = s.get_data() as *mut u8;
            for i in 0..len {
                *ptr.add(i) = b'x';
            }
        }
        s.finalize();

        assert_eq!(s.len(), len);
        assert_eq!(s.as_str(), "x".repeat(len));
    }

    #[test]
    fn test_string_heap_try_add_short_string_no_allocation() {
        let mut heap = StringHeap::with_allocator(0, Arc::new(FailingAllocator));

        let s = heap.try_add_string("hello").unwrap();

        assert!(s.is_inlined());
        assert_eq!(s.as_str(), "hello");
    }

    #[test]
    fn test_string_heap_try_add_long_string_propagates_error() {
        let mut heap = StringHeap::with_allocator(0, Arc::new(FailingAllocator));

        let err = heap
            .try_add_string("this is a very long string that must allocate")
            .unwrap_err();

        assert!(err.to_string().contains("injected allocation failure"));
    }

    #[test]
    fn test_string_heap_try_add_blob_uses_allocator() {
        let mut heap = StringHeap::with_allocator(0, Arc::new(DefaultAllocator::new()));

        let data = b"this is a very long blob that exceeds inline length";
        let blob = heap.try_add_blob(data).unwrap();

        assert_eq!(blob.as_bytes(), data);
        assert!(heap.size() > 0);
    }

    #[test]
    fn test_string_heap_clear() {
        let mut heap = StringHeap::new();

        heap.add_string("long string that needs heap allocation here");
        assert!(heap.size() > 0);

        heap.clear();
        assert_eq!(heap.size(), 0);
    }

    #[test]
    fn test_string_heap_with_capacity() {
        let heap = StringHeap::with_capacity(8192);
        assert!(heap.is_empty());
    }

    #[test]
    fn test_string_heap_boundary_strings() {
        let mut heap = StringHeap::new();

        // Exactly at inline boundary (12 bytes)
        let s12 = heap.add_string("123456789012");
        assert!(s12.is_inlined());
        assert_eq!(s12.as_str(), "123456789012");

        // Just over inline boundary (13 bytes)
        let s13 = heap.add_string("1234567890123");
        assert!(!s13.is_inlined());
        assert_eq!(s13.as_str(), "1234567890123");
    }

    #[test]
    fn test_string_heap_many_allocations() {
        let mut heap = StringHeap::new();
        let mut strings = Vec::new();

        // Add many long strings to trigger multiple arena chunks
        for i in 0..100 {
            let s = format!("long string number {} that needs heap allocation", i);
            strings.push(heap.add_string(&s));
        }

        // Verify all strings are still valid
        for (i, s) in strings.iter().enumerate() {
            let expected = format!("long string number {} that needs heap allocation", i);
            assert_eq!(s.as_str(), expected);
        }
    }
}
