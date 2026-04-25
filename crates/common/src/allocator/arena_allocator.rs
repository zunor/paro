// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! ArenaAllocator - batch allocation with single deallocation.
//!
//! This allocator is optimized for batch allocation patterns where individual
//! frees are unnecessary and all memory can be reclaimed together.

use std::sync::Arc;

use super::{AllocatedData, Allocator};
use crate::error::{self as paro_error, Result};

/// Initial capacity for arena chunks (2KB).
pub(crate) const ARENA_ALLOCATOR_INITIAL_CAPACITY: usize = 2048;

/// Maximum capacity for arena chunks (16MB).
pub(crate) const ARENA_ALLOCATOR_MAX_CAPACITY: usize = 1 << 24;

/// Default alignment for allocations (8 bytes).
const DEFAULT_ALIGNMENT: usize = 8;

/// A chunk of memory in the arena.
///
/// Chunks are linked in a list, with new chunks added at the head.
/// Each chunk owns its memory via `AllocatedData`.
pub(crate) struct ArenaChunk {
    /// The allocated memory for this chunk.
    data: AllocatedData,
    /// Current position in the chunk (next allocation starts here).
    current_position: usize,
    /// Maximum size of this chunk.
    maximum_size: usize,
    /// Next chunk in the list (older chunks).
    next: Option<Box<ArenaChunk>>,
    /// Previous chunk pointer (for back-reference, raw pointer).
    prev: *mut ArenaChunk,
}

impl ArenaChunk {
    /// Create a new arena chunk with the given size.
    fn new(allocator: Arc<dyn Allocator>, size: usize) -> Result<Self> {
        debug_assert!(size > 0, "Arena chunk size must be positive");
        let data = AllocatedData::new(allocator, size)?;
        Ok(Self {
            data,
            current_position: 0,
            maximum_size: size,
            next: None,
            prev: std::ptr::null_mut(),
        })
    }

    /// Get the data pointer.
    #[inline]
    fn data_ptr(&self) -> *mut u8 {
        self.data.get()
    }
}

// SAFETY: ArenaChunk owns its memory and prev is only used for internal bookkeeping
unsafe impl Send for ArenaChunk {}
unsafe impl Sync for ArenaChunk {}

/// Arena allocator for batch allocation with single deallocation.
///
/// Allocates memory in chunks and uses bump-pointer allocation within each chunk.
///
/// # Usage
/// ```ignore
/// use paro_common::allocator::{ArenaAllocator, DefaultAllocator};
/// use std::sync::Arc;
///
/// let allocator = Arc::new(DefaultAllocator::new());
/// let mut arena = ArenaAllocator::new(allocator);
///
/// // Allocate memory (very fast, bump pointer)
/// let ptr1 = arena.allocate(64)?;
/// let ptr2 = arena.allocate(128)?;
///
/// // Reset frees all memory but keeps first chunk
/// arena.reset();
///
/// // Destroy frees all memory including chunks
/// arena.destroy();
/// ```
pub struct ArenaAllocator {
    /// The underlying allocator for chunk allocation.
    allocator: Arc<dyn Allocator>,
    /// Initial capacity for new arenas.
    initial_capacity: usize,
    /// Head of the chunk list (most recent chunk).
    head: Option<Box<ArenaChunk>>,
    /// Tail of the chunk list (oldest chunk).
    tail: *mut ArenaChunk,
    /// Total allocated size across all chunks.
    allocated_size: usize,
}

impl ArenaAllocator {
    /// Create a new arena allocator with default initial capacity.
    pub fn new(allocator: Arc<dyn Allocator>) -> Self {
        Self::with_capacity(allocator, ARENA_ALLOCATOR_INITIAL_CAPACITY)
    }

    /// Create a new arena allocator with specified initial capacity.
    pub fn with_capacity(allocator: Arc<dyn Allocator>, initial_capacity: usize) -> Self {
        Self {
            allocator,
            initial_capacity,
            head: None,
            tail: std::ptr::null_mut(),
            allocated_size: 0,
        }
    }

    /// Allocate memory of the given size.
    ///
    /// Returns a raw pointer to the allocated memory.
    /// The memory is NOT initialized.
    ///
    /// # Errors
    /// Returns an error if allocation fails.
    #[inline]
    pub fn allocate(&mut self, len: usize) -> Result<*mut u8> {
        self.allocate_with_alignment(len, 1)
    }

    /// Allocate memory with an explicit alignment requirement.
    ///
    /// The arena guarantees alignments up to 8 bytes, matching the underlying
    /// allocator contract used by Paro today.
    pub fn allocate_with_alignment(&mut self, len: usize, alignment: usize) -> Result<*mut u8> {
        validate_alignment(alignment)?;

        if len == 0 {
            return Ok(std::ptr::null_mut());
        }

        let aligned_len = if alignment == 1 {
            len
        } else {
            align_value_to(len, alignment)
        };

        // Check if we need a new block
        let need_new_block = match &self.head {
            None => true,
            Some(head) => {
                let aligned_position = align_value_to(head.current_position, alignment);
                aligned_position + aligned_len > head.maximum_size
            }
        };

        if need_new_block {
            self.allocate_new_block(aligned_len)?;
        }

        // Now we have a head with enough space
        let head = self
            .head
            .as_mut()
            .expect("head should exist after allocate_new_block");
        if alignment > 1 && !is_aligned_to(head.current_position, alignment) {
            head.current_position = align_value_to(head.current_position, alignment);
        }
        debug_assert!(
            head.current_position + aligned_len <= head.maximum_size,
            "Not enough space in head chunk"
        );

        let result = unsafe { head.data_ptr().add(head.current_position) };
        head.current_position += aligned_len;
        Ok(result)
    }

    /// Allocate aligned memory of the given size.
    ///
    /// Aligns both the current position and the allocation size to 8 bytes.
    pub fn allocate_aligned(&mut self, size: usize) -> Result<*mut u8> {
        self.allocate_with_alignment(size, DEFAULT_ALIGNMENT)
    }

    /// Reallocate memory to a new size.
    ///
    /// If the pointer is the head pointer and there's enough space,
    /// this is optimized to just adjust the position.
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    pub fn reallocate(
        &mut self,
        pointer: *mut u8,
        old_size: usize,
        new_size: usize,
    ) -> Result<*mut u8> {
        if old_size == new_size {
            return Ok(pointer);
        }

        if let Some(head) = &mut self.head {
            let head_ptr = unsafe { head.data_ptr().add(head.current_position - old_size) };
            let diff = new_size as isize - old_size as isize;
            let new_position = head.current_position as isize + diff;

            // Check if we can optimize: pointer is head pointer and fits
            if pointer == head_ptr
                && (new_size < old_size || new_position <= head.maximum_size as isize)
            {
                head.current_position = new_position as usize;
                return Ok(pointer);
            }
        }

        // Fallback: allocate new memory and copy
        let result = self.allocate(new_size)?;
        if !pointer.is_null() && old_size > 0 {
            let copy_size = old_size.min(new_size);
            // SAFETY: both pointers are valid and non-overlapping
            unsafe {
                std::ptr::copy_nonoverlapping(pointer, result, copy_size);
            }
        }
        Ok(result)
    }

    /// Reallocate aligned memory to a new size.
    pub fn reallocate_aligned(
        &mut self,
        pointer: *mut u8,
        old_size: usize,
        new_size: usize,
    ) -> Result<*mut u8> {
        self.align_next();
        self.reallocate(
            pointer,
            old_size,
            align_value_to(new_size, DEFAULT_ALIGNMENT),
        )
    }

    /// Align the next allocation to 8 bytes.
    ///
    /// Increments the internal cursor so the next allocation is guaranteed
    /// to be aligned to 8 bytes.
    pub fn align_next(&mut self) {
        if let Some(head) = &mut self.head {
            if !is_aligned_to(head.current_position, DEFAULT_ALIGNMENT) {
                head.current_position = align_value_to(head.current_position, DEFAULT_ALIGNMENT);
            }
        }
    }

    /// Shrink the last allocation.
    ///
    /// This can only be called after `allocate` with a size >= shrink_size.
    ///
    /// # Panics
    /// Panics if there's no head or if shrink_size > current_position.
    pub fn shrink_head(&mut self, shrink_size: usize) {
        let head = self.head.as_mut().expect("shrink_head called with no head");
        debug_assert!(
            head.current_position >= shrink_size,
            "shrink_size {} > current_position {}",
            shrink_size,
            head.current_position
        );
        head.current_position -= shrink_size;
    }

    /// Reset the arena, keeping the first chunk.
    ///
    /// Destroys all chunks except the current head, and resets the head's position.
    /// This is more efficient than `destroy()` if you plan to reuse the arena.
    pub fn reset(&mut self) {
        if let Some(head) = &mut self.head {
            // Destroy all chunks after head
            if head.next.is_some() {
                // Drop the chain by taking ownership
                let mut current = head.next.take();
                while let Some(mut chunk) = current {
                    current = chunk.next.take();
                    // chunk is dropped here
                }
            }

            // Reset tail to head
            self.tail = head.as_mut() as *mut ArenaChunk;

            // Reset head position
            head.current_position = 0;
            head.prev = std::ptr::null_mut();
            self.allocated_size = head.maximum_size;
        } else {
            self.allocated_size = 0;
        }
    }

    /// Destroy the arena, freeing all memory.
    pub fn destroy(&mut self) {
        self.head = None;
        self.tail = std::ptr::null_mut();
        self.allocated_size = 0;
    }

    /// Move the contents of this arena to another arena.
    ///
    /// The other arena must be empty.
    pub fn move_to(&mut self, other: &mut ArenaAllocator) {
        debug_assert!(other.head.is_none(), "Target arena must be empty");
        other.tail = self.tail;
        other.head = self.head.take();
        other.initial_capacity = self.initial_capacity;
        other.allocated_size = self.allocated_size;
        self.destroy();
    }

    /// Get the head chunk.
    #[allow(dead_code)]
    pub(crate) fn get_head(&self) -> Option<&ArenaChunk> {
        self.head.as_deref()
    }

    /// Get the tail chunk.
    #[allow(dead_code)]
    pub(crate) fn get_tail(&self) -> *mut ArenaChunk {
        self.tail
    }

    /// Check if the arena is empty (no chunks allocated).
    pub fn is_empty(&self) -> bool {
        self.head.is_none()
    }

    /// Get the total used size across all chunks.
    ///
    /// This iterates through all chunks to calculate the total.
    pub fn size_in_bytes(&self) -> usize {
        let mut total = 0;
        let mut current = self.head.as_deref();
        while let Some(chunk) = current {
            total += chunk.current_position;
            current = chunk.next.as_deref();
        }
        total
    }

    /// Get the total allocated size (cached value).
    pub fn allocation_size(&self) -> usize {
        debug_assert!(self.head.is_some() || self.allocated_size == 0);
        self.allocated_size
    }

    /// Return how many new backing bytes the next allocation would require.
    pub fn additional_capacity_for_allocation(
        &self,
        len: usize,
        alignment: usize,
    ) -> Result<usize> {
        validate_alignment(alignment)?;

        if len == 0 {
            return Ok(0);
        }

        let aligned_len = if alignment == 1 {
            len
        } else {
            align_value_to(len, alignment)
        };

        let need_new_block = match &self.head {
            None => true,
            Some(head) => {
                let aligned_position = align_value_to(head.current_position, alignment);
                aligned_position + aligned_len > head.maximum_size
            }
        };

        if need_new_block {
            Ok(self.next_block_capacity(aligned_len))
        } else {
            Ok(0)
        }
    }

    /// Get the underlying allocator.
    pub fn get_allocator(&self) -> &Arc<dyn Allocator> {
        &self.allocator
    }

    /// Allocate a new block with at least `min_size` capacity.
    fn allocate_new_block(&mut self, min_size: usize) -> Result<()> {
        let capacity = self.next_block_capacity(min_size);

        // Create new chunk
        let mut new_chunk = Box::new(ArenaChunk::new(self.allocator.clone(), capacity)?);

        // Link into list
        if let Some(mut old_head) = self.head.take() {
            old_head.prev = new_chunk.as_mut() as *mut ArenaChunk;
            new_chunk.next = Some(old_head);
        } else {
            self.tail = new_chunk.as_mut() as *mut ArenaChunk;
        }

        self.head = Some(new_chunk);
        self.allocated_size += capacity;

        Ok(())
    }

    fn next_block_capacity(&self, min_size: usize) -> usize {
        let mut capacity = if self.head.is_none() {
            self.initial_capacity
        } else {
            self.head
                .as_ref()
                .map(|h| h.maximum_size)
                .unwrap_or(self.initial_capacity)
        };
        capacity = capacity.max(1);

        // Cap at max capacity if we're above it
        if capacity > ARENA_ALLOCATOR_MAX_CAPACITY {
            capacity = ARENA_ALLOCATOR_MAX_CAPACITY;
        }

        // Double until we reach max capacity
        if capacity < ARENA_ALLOCATOR_MAX_CAPACITY {
            capacity *= 2;
        }

        // Keep doubling until we can fit min_size
        while capacity < min_size {
            if capacity >= ARENA_ALLOCATOR_MAX_CAPACITY {
                capacity = min_size;
                break;
            }
            capacity = (capacity * 2).min(ARENA_ALLOCATOR_MAX_CAPACITY);
        }
        capacity
    }
}

impl Drop for ArenaAllocator {
    fn drop(&mut self) {
        self.destroy();
    }
}

// SAFETY: ArenaAllocator owns all its memory
unsafe impl Send for ArenaAllocator {}
unsafe impl Sync for ArenaAllocator {}

impl std::fmt::Debug for ArenaAllocator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArenaAllocator")
            .field("initial_capacity", &self.initial_capacity)
            .field("allocated_size", &self.allocated_size)
            .field("has_head", &self.head.is_some())
            .finish()
    }
}

fn validate_alignment(alignment: usize) -> Result<()> {
    if alignment == 0 {
        return Err(paro_error::invalid_input(
            "ArenaAllocator alignment must be greater than 0",
        ));
    }
    if !alignment.is_power_of_two() {
        return Err(paro_error::invalid_input(format!(
            "ArenaAllocator alignment must be a power of two, got {alignment}"
        )));
    }
    if alignment > DEFAULT_ALIGNMENT {
        return Err(paro_error::invalid_input(format!(
            "ArenaAllocator alignment {alignment} exceeds supported max {DEFAULT_ALIGNMENT}"
        )));
    }
    Ok(())
}

/// Align a value to the requested power-of-two boundary.
#[inline]
fn align_value_to(value: usize, alignment: usize) -> usize {
    debug_assert!(alignment.is_power_of_two());
    (value + alignment - 1) & !(alignment - 1)
}

/// Check if a value is aligned to the requested power-of-two boundary.
#[inline]
fn is_aligned_to(value: usize, alignment: usize) -> bool {
    debug_assert!(alignment.is_power_of_two());
    value & (alignment - 1) == 0
}

#[cfg(test)]
#[inline]
fn align_value(value: usize) -> usize {
    align_value_to(value, DEFAULT_ALIGNMENT)
}

#[cfg(test)]
#[inline]
fn is_aligned(value: usize) -> bool {
    is_aligned_to(value, DEFAULT_ALIGNMENT)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::allocator::DefaultAllocator;

    fn create_arena() -> ArenaAllocator {
        let allocator = Arc::new(DefaultAllocator::new());
        ArenaAllocator::new(allocator)
    }

    #[test]
    fn test_arena_allocator_new() {
        let arena = create_arena();
        assert!(arena.is_empty());
        assert_eq!(arena.allocation_size(), 0);
        assert_eq!(arena.size_in_bytes(), 0);
    }

    #[test]
    fn test_arena_allocator_allocate() {
        let mut arena = create_arena();

        let ptr1 = arena.allocate(64).unwrap();
        assert!(!ptr1.is_null());
        assert!(!arena.is_empty());
        assert!(arena.allocation_size() > 0);

        let ptr2 = arena.allocate(128).unwrap();
        assert!(!ptr2.is_null());
        assert_ne!(ptr1, ptr2);
    }

    #[test]
    fn test_arena_allocator_allocate_zero() {
        let mut arena = create_arena();
        let ptr = arena.allocate(0).unwrap();
        assert!(ptr.is_null());
    }

    #[test]
    fn test_arena_allocator_zero_initial_capacity_still_allocates() {
        let allocator = Arc::new(DefaultAllocator::new());
        let mut arena = ArenaAllocator::with_capacity(allocator, 0);

        let ptr = arena.allocate(32).unwrap();
        assert!(!ptr.is_null());
        assert!(arena.allocation_size() >= 32);
    }

    #[test]
    fn test_arena_allocator_allocate_aligned() {
        let mut arena = create_arena();

        // Allocate unaligned size
        let ptr1 = arena.allocate(7).unwrap();
        assert!(!ptr1.is_null());

        // Allocate aligned
        let ptr2 = arena.allocate_aligned(64).unwrap();
        assert!(!ptr2.is_null());
        assert_eq!(ptr2 as usize % 8, 0);
    }

    #[test]
    fn test_arena_allocator_allocate_with_custom_alignment() {
        let mut arena = create_arena();

        let ptr = arena.allocate_with_alignment(13, 4).unwrap();
        assert!(!ptr.is_null());
        assert_eq!(ptr as usize % 4, 0);
    }

    #[test]
    fn test_arena_allocator_allocate_invalid_alignment() {
        let mut arena = create_arena();

        let err = arena.allocate_with_alignment(64, 3).unwrap_err();
        assert!(err.to_string().contains("power of two"));

        let err = arena.allocate_with_alignment(64, 16).unwrap_err();
        assert!(err.to_string().contains("exceeds supported max"));
    }

    #[test]
    fn test_arena_allocator_reallocate_grow() {
        let mut arena = create_arena();

        let ptr = arena.allocate(64).unwrap();
        unsafe {
            std::ptr::write_bytes(ptr, 0xAB, 64);
        }

        let new_ptr = arena.reallocate(ptr, 64, 128).unwrap();
        assert!(!new_ptr.is_null());

        // Check data preserved
        unsafe {
            let slice = std::slice::from_raw_parts(new_ptr, 64);
            for &byte in slice {
                assert_eq!(byte, 0xAB);
            }
        }
    }

    #[test]
    fn test_arena_allocator_reallocate_shrink() {
        let mut arena = create_arena();

        let ptr = arena.allocate(128).unwrap();
        let new_ptr = arena.reallocate(ptr, 128, 64).unwrap();

        // For shrink at head, pointer should be same
        assert_eq!(ptr, new_ptr);
    }

    #[test]
    fn test_arena_allocator_reallocate_same_size() {
        let mut arena = create_arena();

        let ptr = arena.allocate(64).unwrap();
        let new_ptr = arena.reallocate(ptr, 64, 64).unwrap();
        assert_eq!(ptr, new_ptr);
    }

    #[test]
    fn test_arena_allocator_shrink_head() {
        let mut arena = create_arena();

        arena.allocate(100).unwrap();
        let size_before = arena.size_in_bytes();

        arena.shrink_head(20);
        let size_after = arena.size_in_bytes();

        assert_eq!(size_before - size_after, 20);
    }

    #[test]
    fn test_arena_allocator_reset() {
        let mut arena = create_arena();

        // Allocate some memory
        arena.allocate(1024).unwrap();
        arena.allocate(2048).unwrap();
        assert!(!arena.is_empty());

        // Reset
        arena.reset();

        // Arena should still have head chunk but position reset
        assert!(!arena.is_empty());
        assert_eq!(arena.size_in_bytes(), 0);
        assert!(arena.allocation_size() > 0);
    }

    #[test]
    fn test_arena_allocator_destroy() {
        let mut arena = create_arena();

        arena.allocate(1024).unwrap();
        assert!(!arena.is_empty());

        arena.destroy();
        assert!(arena.is_empty());
        assert_eq!(arena.allocation_size(), 0);
    }

    #[test]
    fn test_arena_allocator_move_to() {
        let allocator = Arc::new(DefaultAllocator::new());
        let mut arena1 = ArenaAllocator::new(allocator.clone());
        let mut arena2 = ArenaAllocator::new(allocator);

        arena1.allocate(1024).unwrap();
        let size = arena1.allocation_size();

        arena1.move_to(&mut arena2);

        assert!(arena1.is_empty());
        assert!(!arena2.is_empty());
        assert_eq!(arena2.allocation_size(), size);
    }

    #[test]
    fn test_arena_allocator_large_allocation() {
        let mut arena = create_arena();

        // Allocate more than initial capacity
        let large_size = ARENA_ALLOCATOR_INITIAL_CAPACITY * 4;
        let ptr = arena.allocate(large_size).unwrap();
        assert!(!ptr.is_null());
        assert!(arena.allocation_size() >= large_size);
    }

    #[test]
    fn test_arena_allocator_multiple_chunks() {
        let allocator = Arc::new(DefaultAllocator::new());
        let mut arena = ArenaAllocator::with_capacity(allocator, 256);

        // Allocate enough to trigger multiple chunks
        for _ in 0..10 {
            let ptr = arena.allocate(200).unwrap();
            assert!(!ptr.is_null());
        }

        // Should have multiple chunks
        assert!(arena.allocation_size() > 256);
    }

    #[test]
    fn test_arena_allocator_size_in_bytes() {
        let mut arena = create_arena();

        arena.allocate(100).unwrap();
        assert_eq!(arena.size_in_bytes(), 100);

        arena.allocate(200).unwrap();
        assert_eq!(arena.size_in_bytes(), 300);
    }

    #[test]
    fn test_arena_allocator_with_capacity() {
        let allocator = Arc::new(DefaultAllocator::new());
        let mut arena = ArenaAllocator::with_capacity(allocator, 4096);

        arena.allocate(100).unwrap();
        // First allocation should use initial capacity
        assert!(arena.allocation_size() >= 4096);
    }

    #[test]
    fn test_align_value() {
        assert_eq!(align_value(0), 0);
        assert_eq!(align_value(1), 8);
        assert_eq!(align_value(7), 8);
        assert_eq!(align_value(8), 8);
        assert_eq!(align_value(9), 16);
        assert_eq!(align_value(15), 16);
        assert_eq!(align_value(16), 16);
    }

    #[test]
    fn test_is_aligned() {
        assert!(is_aligned(0));
        assert!(!is_aligned(1));
        assert!(!is_aligned(7));
        assert!(is_aligned(8));
        assert!(!is_aligned(9));
        assert!(is_aligned(16));
    }
}
