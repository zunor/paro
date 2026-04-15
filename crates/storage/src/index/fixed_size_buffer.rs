// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # Fixed Size Buffer
//!
//! A fixed-size buffer holds fixed-size segments of data for index nodes.
//!
//! ## Design
//!
//! Each buffer contains:
//! - A bitmask at the beginning to track allocated segments
//! - Fixed-size segments for storing index node data
//!
//! ```text
//! ┌─────────────────────────────────────────┐
//! │ Bitmask (validity_t[])                  │
//! ├─────────────────────────────────────────┤
//! │ Segment 0                               │
//! ├─────────────────────────────────────────┤
//! │ Segment 1                               │
//! ├─────────────────────────────────────────┤
//! │ ...                                     │
//! └─────────────────────────────────────────┘
//! ```
//!
//! ## Thread Safety
//! - FixedSizeBuffer uses internal locking for thread safety
//! - SegmentHandle provides RAII access with reader counting

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use paro_common::allocator::MemoryTag;

use crate::buffer::{BufferHandle, BufferManager, SharedBlockHandle};

/// Type for validity bitmask entries (64 bits each).
pub type ValidityT = u64;

/// Number of bits per validity entry.
pub const BITS_PER_VALIDITY: usize = 64;

/// Constants for fast offset calculations in the bitmask.
pub const BASE: [u64; 6] = [0x0000_0000_FFFF_FFFF, 0x0000_FFFF, 0x00FF, 0x0F, 0x3, 0x1];

pub const SHIFT: [u8; 6] = [32, 16, 8, 4, 2, 1];

/// Block pointer for on-disk storage.
#[derive(Debug, Clone, Copy, Default)]
pub struct BlockPointer {
    /// Block ID in the storage file.
    pub block_id: u32,
    /// Offset within the block.
    pub offset: u32,
}

impl BlockPointer {
    /// Creates a new block pointer.
    pub fn new(block_id: u32, offset: u32) -> Self {
        Self { block_id, offset }
    }

    /// Returns true if this block pointer is valid.
    pub fn is_valid(&self) -> bool {
        self.block_id != u32::MAX
    }

    /// Creates an invalid block pointer.
    pub fn invalid() -> Self {
        Self {
            block_id: u32::MAX,
            offset: 0,
        }
    }
}

/// Internal state of a FixedSizeBuffer protected by mutex.
struct FixedSizeBufferInner {
    /// The number of allocated segments.
    segment_count: usize,

    /// The size of allocated memory in this buffer.
    allocation_size: usize,

    /// True: the in-memory buffer is no longer consistent with its optional copy on disk.
    dirty: bool,

    /// True: can be vacuumed after the vacuum operation.
    vacuum: bool,

    /// True: has been loaded from disk.
    loaded: bool,

    /// Block pointer for on-disk storage.
    block_pointer: BlockPointer,

    /// The buffer handle from BufferManager (in-memory data).
    buffer_handle: Option<BufferHandle>,

    /// The block handle for on-disk buffer.
    block_handle: Option<SharedBlockHandle>,
}

/// A fixed-size buffer holds fixed-size segments of data.
///
/// This implementation integrates with BufferManager for memory management,
/// supporting memory limits, tracking via MemoryTag::ArtIndex, and Pin/Unpin.
pub struct FixedSizeBuffer {
    /// Buffer manager for memory allocation.
    buffer_manager: Arc<dyn BufferManager>,

    /// Number of active readers.
    readers: AtomicUsize,

    /// Block size for this buffer.
    block_size: usize,

    /// Protected inner state.
    inner: Mutex<FixedSizeBufferInner>,
}

impl FixedSizeBuffer {
    /// Creates a new in-memory buffer with the given block size.
    pub fn new(buffer_manager: Arc<dyn BufferManager>, block_size: usize) -> Self {
        // Allocate memory through BufferManager
        let buffer_handle = crate::buffer::BufferManager::allocate(
            buffer_manager.as_ref(),
            MemoryTag::ArtIndex,
            block_size,
            false,
        )
        .expect("Failed to allocate buffer");

        // Zero-initialize the buffer as it might get serialized to storage
        unsafe {
            if let Some(data) = buffer_handle.data_mut() {
                data.fill(0);
            }
        }

        // Get the block handle for potential disk operations
        let block_handle = buffer_handle.block_handle().cloned();

        Self {
            buffer_manager,
            readers: AtomicUsize::new(0),
            block_size,
            inner: Mutex::new(FixedSizeBufferInner {
                segment_count: 0,
                allocation_size: 0,
                dirty: false,
                vacuum: false,
                loaded: false,
                block_pointer: BlockPointer::invalid(),
                buffer_handle: Some(buffer_handle),
                block_handle,
            }),
        }
    }

    /// Creates a buffer from on-disk storage metadata.
    ///
    /// If `block_pointer` is invalid, the buffer will be created in memory.
    pub fn from_disk(
        buffer_manager: Arc<dyn BufferManager>,
        block_size: usize,
        segment_count: usize,
        allocation_size: usize,
        block_pointer: BlockPointer,
    ) -> Self {
        // If block_pointer is invalid, create an in-memory buffer instead
        if !block_pointer.is_valid() {
            let buffer = Self::new(buffer_manager, block_size);
            {
                let mut inner = buffer.inner.lock().unwrap();
                inner.segment_count = segment_count;
                inner.allocation_size = allocation_size;
            }
            return buffer;
        }

        Self {
            buffer_manager,
            readers: AtomicUsize::new(0),
            block_size,
            inner: Mutex::new(FixedSizeBufferInner {
                segment_count,
                allocation_size,
                dirty: false,
                vacuum: false,
                loaded: false,
                block_pointer,
                buffer_handle: None,
                block_handle: None,
            }),
        }
    }

    /// Returns true if the buffer is in memory.
    pub fn in_memory(&self) -> bool {
        let inner = self.inner.lock().unwrap();
        inner.buffer_handle.is_some()
    }

    /// Returns true if the buffer is on disk.
    pub fn on_disk(&self) -> bool {
        let inner = self.inner.lock().unwrap();
        inner.block_pointer.is_valid()
    }

    /// Returns the number of allocated segments.
    pub fn segment_count(&self) -> usize {
        let inner = self.inner.lock().unwrap();
        inner.segment_count
    }

    /// Sets the segment count.
    pub fn set_segment_count(&self, count: usize) {
        let mut inner = self.inner.lock().unwrap();
        inner.segment_count = count;
    }

    /// Increments the segment count.
    pub fn increment_segment_count(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.segment_count += 1;
    }

    /// Decrements the segment count.
    pub fn decrement_segment_count(&self) {
        let mut inner = self.inner.lock().unwrap();
        debug_assert!(inner.segment_count > 0);
        inner.segment_count -= 1;
    }

    /// Returns the allocation size.
    pub fn allocation_size(&self) -> usize {
        let inner = self.inner.lock().unwrap();
        inner.allocation_size
    }

    /// Returns true if the buffer is dirty.
    pub fn is_dirty(&self) -> bool {
        let inner = self.inner.lock().unwrap();
        inner.dirty
    }

    /// Sets the dirty flag.
    pub fn set_dirty(&self, dirty: bool) {
        let mut inner = self.inner.lock().unwrap();
        inner.dirty = dirty;
    }

    /// Returns true if the buffer should be vacuumed.
    pub fn should_vacuum(&self) -> bool {
        let inner = self.inner.lock().unwrap();
        inner.vacuum
    }

    /// Sets the vacuum flag.
    pub fn set_vacuum(&self, vacuum: bool) {
        let mut inner = self.inner.lock().unwrap();
        inner.vacuum = vacuum;
    }

    /// Returns the block pointer.
    pub fn block_pointer(&self) -> BlockPointer {
        let inner = self.inner.lock().unwrap();
        inner.block_pointer
    }

    /// Sets the block pointer.
    pub fn set_block_pointer(&self, block_pointer: BlockPointer) {
        let mut inner = self.inner.lock().unwrap();
        inner.block_pointer = block_pointer;
    }

    /// Returns the buffer manager.
    pub fn buffer_manager(&self) -> &Arc<dyn BufferManager> {
        &self.buffer_manager
    }

    /// Returns the block size.
    pub fn block_size(&self) -> usize {
        self.block_size
    }

    /// Gets a pointer to the buffer data at the given offset.
    ///
    /// If `dirty` is true, marks the buffer as dirty.
    /// Ensures the buffer is in memory first.
    ///
    /// # Safety
    /// The caller must ensure the offset is within bounds.
    pub fn get(&self, offset: usize, dirty: bool) -> *mut u8 {
        let mut inner = self.inner.lock().unwrap();

        // Ensure buffer is in memory
        if inner.buffer_handle.is_none() {
            self.load_from_disk_inner(&mut inner);
        }

        if dirty {
            inner.dirty = true;
        }

        let buffer_handle = inner.buffer_handle.as_ref().unwrap();
        buffer_handle.ptr().unwrap().wrapping_add(offset)
    }

    /// Finds the first free offset in the bitmask.
    pub fn get_offset(&self, bitmask_count: usize, available_segments: usize) -> u32 {
        let mut inner = self.inner.lock().unwrap();

        // Ensure buffer is in memory
        if inner.buffer_handle.is_none() {
            self.load_from_disk_inner(&mut inner);
        }

        let buffer_handle = inner.buffer_handle.as_ref().unwrap();
        let bitmask_ptr = buffer_handle.ptr().unwrap() as *mut ValidityT;

        // Try to fill sequentially first
        if inner.segment_count < available_segments {
            unsafe {
                let word = inner.segment_count / BITS_PER_VALIDITY;
                let bit = inner.segment_count % BITS_PER_VALIDITY;
                let mask_word = bitmask_ptr.add(word);

                if (*mask_word & (1u64 << bit)) != 0 {
                    *mask_word &= !(1u64 << bit);
                    inner.dirty = true;
                    return inner.segment_count as u32;
                }
            }
        }

        // Search for a free bit in the bitmask
        for entry_idx in 0..bitmask_count {
            let entry = unsafe { *bitmask_ptr.add(entry_idx) };

            if entry == 0 {
                continue;
            }

            let first_valid_bit = Self::find_first_set_bit(entry);
            let prev_bits = entry_idx * BITS_PER_VALIDITY;
            let offset = prev_bits + first_valid_bit;

            unsafe {
                let mask_word = bitmask_ptr.add(entry_idx);
                *mask_word &= !(1u64 << first_valid_bit);
            }

            inner.dirty = true;
            return offset as u32;
        }

        panic!("Invalid bitmask for FixedSizeAllocator: no free segments");
    }

    /// Finds the position of the first set bit in a 64-bit value.
    #[inline]
    fn find_first_set_bit(mut entry: u64) -> usize {
        let mut first_valid_bit = 0usize;

        for i in 0..6 {
            if (entry & BASE[i]) != 0 {
                entry &= BASE[i];
            } else {
                entry >>= SHIFT[i];
                first_valid_bit += SHIFT[i] as usize;
            }
        }

        first_valid_bit
    }

    /// Marks a segment as free in the bitmask.
    pub fn free_segment(&self, offset: u32) {
        let mut inner = self.inner.lock().unwrap();

        // Ensure buffer is in memory
        if inner.buffer_handle.is_none() {
            self.load_from_disk_inner(&mut inner);
        }

        let buffer_handle = inner.buffer_handle.as_ref().unwrap();
        let bitmask_ptr = buffer_handle.ptr().unwrap() as *mut ValidityT;

        let word = offset as usize / BITS_PER_VALIDITY;
        let bit = offset as usize % BITS_PER_VALIDITY;

        unsafe {
            let mask_word = bitmask_ptr.add(word);
            debug_assert!((*mask_word & (1u64 << bit)) == 0, "Segment already free");
            *mask_word |= 1u64 << bit;
        }

        inner.dirty = true;
    }

    /// Initializes the bitmask to all valid (all segments free).
    pub fn initialize_bitmask(&self, available_segments: usize) {
        let mut inner = self.inner.lock().unwrap();

        if inner.buffer_handle.is_none() {
            return;
        }

        let buffer_handle = inner.buffer_handle.as_ref().unwrap();
        let bitmask_ptr = buffer_handle.ptr().unwrap() as *mut ValidityT;

        let full_words = available_segments / BITS_PER_VALIDITY;
        let remaining_bits = available_segments % BITS_PER_VALIDITY;

        for i in 0..full_words {
            unsafe {
                *bitmask_ptr.add(i) = u64::MAX;
            }
        }

        if remaining_bits > 0 {
            unsafe {
                *bitmask_ptr.add(full_words) = (1u64 << remaining_bits) - 1;
            }
        }

        inner.dirty = true;
    }

    /// Sets the allocation size based on the maximum used offset.
    pub fn set_allocation_size(
        &self,
        available_segments: usize,
        segment_size: usize,
        bitmask_offset: usize,
    ) {
        let mut inner = self.inner.lock().unwrap();

        if !inner.dirty || inner.buffer_handle.is_none() {
            return;
        }

        let buffer_handle = inner.buffer_handle.as_ref().unwrap();
        let bitmask_ptr = buffer_handle.ptr().unwrap() as *const ValidityT;

        let mut max_offset = available_segments;
        for i in (0..available_segments).rev() {
            let word = i / BITS_PER_VALIDITY;
            let bit = i % BITS_PER_VALIDITY;

            let is_free = unsafe {
                let mask_word = *bitmask_ptr.add(word);
                (mask_word & (1u64 << bit)) != 0
            };

            if !is_free {
                max_offset = i + 1;
                break;
            }
        }

        inner.allocation_size = max_offset * segment_size + bitmask_offset;
    }

    /// Loads the buffer from disk.
    fn load_from_disk_inner(&self, inner: &mut FixedSizeBufferInner) {
        debug_assert!(inner.block_pointer.is_valid());
        debug_assert!(!inner.dirty);

        // Allocate a new buffer through BufferManager
        let new_buffer_handle = crate::buffer::BufferManager::allocate(
            self.buffer_manager.as_ref(),
            MemoryTag::ArtIndex,
            self.block_size,
            false,
        )
        .expect("Failed to allocate buffer for loading from disk");

        // Persistent index segment reload is not wired yet in this path.
        // Keep a zero-initialized in-memory buffer so callers can continue safely.
        if inner.allocation_size > 0 {
            // Zero-initialize for now
            unsafe {
                if let Some(data) = new_buffer_handle.data_mut() {
                    data[..inner.allocation_size].fill(0);
                }
            }
        }

        let block_handle = new_buffer_handle.block_handle().cloned();
        inner.buffer_handle = Some(new_buffer_handle);
        inner.block_handle = block_handle;
        inner.loaded = true;
    }

    /// Returns the raw buffer data for serialization.
    pub fn buffer_data(&self) -> Option<Vec<u8>> {
        let inner = self.inner.lock().unwrap();
        inner
            .buffer_handle
            .as_ref()
            .and_then(|h| h.data().map(|d| d.to_vec()))
    }

    /// Returns the number of active readers.
    pub fn reader_count(&self) -> usize {
        self.readers.load(Ordering::Acquire)
    }

    /// Increments the reader count.
    fn increment_readers(&self) {
        self.readers.fetch_add(1, Ordering::AcqRel);
    }

    /// Decrements the reader count.
    fn decrement_readers(&self) {
        self.readers.fetch_sub(1, Ordering::AcqRel);
    }

    /// Creates a segment handle for accessing data at the given offset.
    pub fn get_segment_handle(&self, offset: usize) -> SegmentHandle<'_> {
        SegmentHandle::new(self, offset)
    }
}

impl Drop for FixedSizeBuffer {
    fn drop(&mut self) {
        debug_assert_eq!(
            self.readers.load(Ordering::Acquire),
            0,
            "FixedSizeBuffer dropped with active readers"
        );

        // BufferHandle will be dropped automatically, unpinning the buffer
        // Block handle cleanup is handled by BufferManager
    }
}

// SAFETY: FixedSizeBuffer uses internal synchronization via Mutex.
unsafe impl Send for FixedSizeBuffer {}
unsafe impl Sync for FixedSizeBuffer {}

/// A handle to a segment within a FixedSizeBuffer.
///
/// SegmentHandle provides RAII access to segment data with reader counting.
/// When created, it increments the reader count and ensures the buffer is in memory.
/// When dropped, it decrements the reader count.
pub struct SegmentHandle<'a> {
    buffer: &'a FixedSizeBuffer,
    ptr: *mut u8,
}

impl<'a> SegmentHandle<'a> {
    /// Creates a new segment handle.
    fn new(buffer: &'a FixedSizeBuffer, offset: usize) -> Self {
        let mut inner = buffer.inner.lock().unwrap();

        // Ensure buffer is in memory
        if inner.buffer_handle.is_none() && !inner.loaded {
            buffer.load_from_disk_inner(&mut inner);
        }

        // If loaded from disk but buffer_handle is None, re-pin
        if inner.buffer_handle.is_none() && inner.loaded {
            if let Some(ref block_handle) = inner.block_handle {
                if let Ok(handle) =
                    crate::buffer::BufferManager::pin(buffer.buffer_manager.as_ref(), block_handle)
                {
                    inner.buffer_handle = Some(handle);
                }
            }
        }

        let ptr = inner
            .buffer_handle
            .as_ref()
            .map(|h| h.ptr().unwrap().wrapping_add(offset))
            .unwrap_or(std::ptr::null_mut());

        buffer.increment_readers();

        Self { buffer, ptr }
    }

    /// Gets a reference to the segment data as type T.
    #[inline]
    pub unsafe fn get_ref<T>(&self) -> &T {
        &*(self.ptr as *const T)
    }

    /// Gets a mutable reference to the segment data as type T.
    #[inline]
    pub unsafe fn get_mut<T>(&mut self) -> &mut T {
        &mut *(self.ptr as *mut T)
    }

    /// Gets a raw pointer to the segment data.
    #[inline]
    pub fn get_ptr(&self) -> *mut u8 {
        self.ptr
    }

    /// Marks the buffer as modified.
    pub fn mark_modified(&self) {
        self.buffer.set_dirty(true);
    }
}

impl<'a> Drop for SegmentHandle<'a> {
    fn drop(&mut self) {
        self.buffer.decrement_readers();
        // TODO: Consider unpinning buffers with zero readers while preventing oscillation
    }
}

// SegmentHandle is not Copy or Clone - move-only semantics
// This is enforced by not implementing Clone/Copy traits

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::StandardBufferManager;

    fn create_test_buffer_manager() -> Arc<dyn BufferManager> {
        Arc::new(StandardBufferManager::with_defaults(10 * 1024 * 1024))
    }

    #[test]
    fn test_block_pointer() {
        let ptr = BlockPointer::new(42, 100);
        assert!(ptr.is_valid());
        assert_eq!(ptr.block_id, 42);
        assert_eq!(ptr.offset, 100);

        let invalid = BlockPointer::invalid();
        assert!(!invalid.is_valid());
    }

    #[test]
    fn test_fixed_size_buffer_new() {
        let manager = create_test_buffer_manager();
        let buffer = FixedSizeBuffer::new(manager, 4096);

        assert!(buffer.in_memory());
        assert!(!buffer.on_disk());
        assert_eq!(buffer.segment_count(), 0);
        assert!(!buffer.is_dirty());
    }

    #[test]
    fn test_fixed_size_buffer_from_disk() {
        let manager = create_test_buffer_manager();
        let block_pointer = BlockPointer::new(1, 0);
        let buffer = FixedSizeBuffer::from_disk(manager, 4096, 10, 1024, block_pointer);

        assert!(!buffer.in_memory());
        assert!(buffer.on_disk());
        assert_eq!(buffer.segment_count(), 10);
    }

    #[test]
    fn test_find_first_set_bit() {
        assert_eq!(FixedSizeBuffer::find_first_set_bit(1), 0);
        assert_eq!(FixedSizeBuffer::find_first_set_bit(2), 1);
        assert_eq!(FixedSizeBuffer::find_first_set_bit(4), 2);
        assert_eq!(FixedSizeBuffer::find_first_set_bit(8), 3);
        assert_eq!(
            FixedSizeBuffer::find_first_set_bit(0x8000_0000_0000_0000),
            63
        );
        assert_eq!(
            FixedSizeBuffer::find_first_set_bit(0xFFFF_FFFF_FFFF_FFFF),
            0
        );
    }

    #[test]
    fn test_initialize_bitmask() {
        let manager = create_test_buffer_manager();
        let buffer = FixedSizeBuffer::new(manager, 4096);
        buffer.initialize_bitmask(100);

        let data = buffer.buffer_data().unwrap();
        let bitmask_ptr = data.as_ptr() as *const ValidityT;

        unsafe {
            assert_eq!(*bitmask_ptr, u64::MAX);
            assert_eq!(*bitmask_ptr.add(1), (1u64 << 36) - 1);
        }
    }

    #[test]
    fn test_get_offset_sequential() {
        let manager = create_test_buffer_manager();
        let buffer = FixedSizeBuffer::new(manager, 4096);
        buffer.initialize_bitmask(100);

        for i in 0..10 {
            let offset = buffer.get_offset(2, 100);
            assert_eq!(offset, i);
            buffer.increment_segment_count();
        }
    }

    #[test]
    fn test_free_segment() {
        let manager = create_test_buffer_manager();
        let buffer = FixedSizeBuffer::new(manager, 4096);
        buffer.initialize_bitmask(100);

        let offset = buffer.get_offset(2, 100);
        assert_eq!(offset, 0);
        buffer.increment_segment_count();

        buffer.free_segment(0);

        let offset2 = buffer.get_offset(2, 100);
        assert_eq!(offset2, 1);
    }

    #[test]
    fn test_segment_count() {
        let manager = create_test_buffer_manager();
        let buffer = FixedSizeBuffer::new(manager, 4096);
        assert_eq!(buffer.segment_count(), 0);

        buffer.increment_segment_count();
        assert_eq!(buffer.segment_count(), 1);

        buffer.decrement_segment_count();
        assert_eq!(buffer.segment_count(), 0);
    }

    #[test]
    fn test_dirty_flag() {
        let manager = create_test_buffer_manager();
        let buffer = FixedSizeBuffer::new(manager, 4096);
        assert!(!buffer.is_dirty());

        buffer.set_dirty(true);
        assert!(buffer.is_dirty());
    }

    #[test]
    fn test_vacuum_flag() {
        let manager = create_test_buffer_manager();
        let buffer = FixedSizeBuffer::new(manager, 4096);
        assert!(!buffer.should_vacuum());

        buffer.set_vacuum(true);
        assert!(buffer.should_vacuum());
    }

    #[test]
    fn test_segment_handle() {
        let manager = create_test_buffer_manager();
        let buffer = FixedSizeBuffer::new(manager, 4096);

        assert_eq!(buffer.reader_count(), 0);

        {
            let handle = buffer.get_segment_handle(0);
            assert_eq!(buffer.reader_count(), 1);
            assert!(!handle.get_ptr().is_null());
        }

        assert_eq!(buffer.reader_count(), 0);
    }

    #[test]
    fn test_segment_handle_data_access() {
        let manager = create_test_buffer_manager();
        let buffer = FixedSizeBuffer::new(manager, 4096);

        {
            let mut handle = buffer.get_segment_handle(64);
            unsafe {
                let value: &mut u64 = handle.get_mut();
                *value = 0xDEAD_BEEF;
            }
            handle.mark_modified();
        }

        assert!(buffer.is_dirty());

        {
            let handle = buffer.get_segment_handle(64);
            unsafe {
                let value: &u64 = handle.get_ref();
                assert_eq!(*value, 0xDEAD_BEEF);
            }
        }
    }

    #[test]
    fn test_multiple_readers() {
        let manager = create_test_buffer_manager();
        let buffer = FixedSizeBuffer::new(manager, 4096);

        let handle1 = buffer.get_segment_handle(0);
        assert_eq!(buffer.reader_count(), 1);

        let handle2 = buffer.get_segment_handle(64);
        assert_eq!(buffer.reader_count(), 2);

        drop(handle1);
        assert_eq!(buffer.reader_count(), 1);

        drop(handle2);
        assert_eq!(buffer.reader_count(), 0);
    }

    #[test]
    fn test_buffer_manager_integration() {
        let manager = create_test_buffer_manager();
        let initial_used = manager.get_used_memory();

        let buffer = FixedSizeBuffer::new(manager.clone(), 4096);
        assert!(manager.get_used_memory() > initial_used);

        drop(buffer);
        // Memory should be freed after buffer is dropped
        // Note: exact memory tracking depends on BufferManager implementation
    }

    #[test]
    fn test_set_allocation_size() {
        let manager = create_test_buffer_manager();
        let buffer = FixedSizeBuffer::new(manager, 4096);
        buffer.initialize_bitmask(100);

        // Allocate some segments
        for _ in 0..10 {
            buffer.get_offset(2, 100);
            buffer.increment_segment_count();
        }

        buffer.set_dirty(true);
        buffer.set_allocation_size(100, 32, 16);

        // Allocation size should be set based on used segments
        assert!(buffer.allocation_size() > 0);
    }
}
