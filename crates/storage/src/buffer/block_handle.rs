// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! BlockHandle - manages a single block in memory.
//!
//! - Reference counting via pin_count
//! - State machine: UNLOADED -> LOADED -> UNLOADED
//! - Memory tagged for tracking (e.g., hash tables, sorting)

use std::ptr::NonNull;
use std::sync::atomic::{AtomicI32, AtomicPtr, AtomicU64, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use paro_common::allocator::{Allocator, MemoryTag};
use paro_common::error::{self as paro_error, Result};

use super::FileBufferType;

/// Block identifier type.
pub type BlockId = i64;

/// Invalid eviction queue index.
const INVALID_INDEX: usize = usize::MAX;

/// Block state in the buffer pool.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum BlockState {
    /// Block data is not loaded
    Unloaded = 0,
    /// Block data is loaded in memory
    Loaded = 1,
}

impl From<u8> for BlockState {
    fn from(value: u8) -> Self {
        match value {
            0 => BlockState::Unloaded,
            _ => BlockState::Loaded,
        }
    }
}

/// Manages a single block of memory in the buffer pool.
///
/// BlockHandle tracks the state and pin count of a block.
/// When pin_count > 0, the block cannot be evicted.
///
/// # Thread Safety
/// - `state` and `pin_count` use atomics for lock-free operations
/// - Buffer pointer access requires external synchronization when unpinned
pub struct BlockHandle {
    /// Unique block identifier
    block_id: BlockId,
    /// Memory tag for tracking
    tag: MemoryTag,
    /// Current state (loaded/unloaded)
    state: AtomicU8,
    /// Number of active pins (readers)
    pin_count: AtomicI32,
    /// Serializes the two lifecycle transitions that cannot be expressed by
    /// independent `state` and `pin_count` atomics: publishing a pin against a
    /// loaded allocation, and detaching an unpinned allocation for eviction.
    lifecycle: Mutex<()>,
    /// Allocated memory size
    size: usize,
    /// Atomically published allocation address. Lifecycle transitions remain
    /// serialized by `lifecycle`; pinned readers only need an acquire load and
    /// never contend on that mutex.
    buffer: AtomicPtr<u8>,
    /// Allocator for memory management
    allocator: Arc<dyn Allocator>,
    /// Whether this block can be evicted when unpinned
    can_destroy: bool,

    // === Eviction Support ===
    /// Buffer type (determines eviction priority)
    buffer_type: FileBufferType,
    /// Eviction sequence number (incremented each time added to eviction queue)
    /// Used to detect stale eviction nodes when iterating the queue
    eviction_seq_num: AtomicU64,
    /// Last access timestamp in milliseconds (for LRU ordering)
    lru_timestamp: AtomicU64,
    /// Eviction queue index (for MANAGED_BUFFER only)
    /// INVALID_INDEX (usize::MAX) means not set
    eviction_queue_idx: AtomicUsize,
}

impl std::fmt::Debug for BlockHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlockHandle")
            .field("block_id", &self.block_id)
            .field("tag", &self.tag)
            .field("state", &self.state)
            .field("pin_count", &self.pin_count)
            .field("size", &self.size)
            .field("buffer", &self.buffer.load(Ordering::Acquire))
            .field("can_destroy", &self.can_destroy)
            .field("buffer_type", &self.buffer_type)
            .field("eviction_seq_num", &self.eviction_seq_num)
            .field("lru_timestamp", &self.lru_timestamp)
            .field("eviction_queue_idx", &self.eviction_queue_idx)
            .finish()
    }
}

impl BlockHandle {
    /// Create a new unloaded block handle.
    pub fn new(
        block_id: BlockId,
        tag: MemoryTag,
        size: usize,
        can_destroy: bool,
        allocator: Arc<dyn Allocator>,
        buffer_type: FileBufferType,
    ) -> Self {
        Self {
            block_id,
            tag,
            state: AtomicU8::new(BlockState::Unloaded as u8),
            pin_count: AtomicI32::new(0),
            size,
            lifecycle: Mutex::new(()),
            buffer: AtomicPtr::new(std::ptr::null_mut()),
            allocator,
            can_destroy,
            buffer_type,
            eviction_seq_num: AtomicU64::new(0),
            lru_timestamp: AtomicU64::new(0),
            eviction_queue_idx: AtomicUsize::new(INVALID_INDEX),
        }
    }

    /// Create and allocate a new block with memory.
    pub fn allocate(
        block_id: BlockId,
        tag: MemoryTag,
        size: usize,
        can_destroy: bool,
        allocator: Arc<dyn Allocator>,
        buffer_type: FileBufferType,
    ) -> Result<Self> {
        if size == 0 {
            return Err(paro_error::invalid_input("Block size cannot be zero"));
        }

        let ptr = allocator.allocate_zeroed(size)?;
        let ptr = NonNull::new(ptr).ok_or_else(|| {
            paro_error::out_of_memory(format!("Failed to allocate {} bytes for block", size))
        })?;

        Ok(Self {
            block_id,
            tag,
            state: AtomicU8::new(BlockState::Loaded as u8),
            pin_count: AtomicI32::new(1), // Start pinned
            size,
            lifecycle: Mutex::new(()),
            buffer: AtomicPtr::new(ptr.as_ptr()),
            allocator,
            can_destroy,
            buffer_type,
            eviction_seq_num: AtomicU64::new(0),
            lru_timestamp: AtomicU64::new(0),
            eviction_queue_idx: AtomicUsize::new(INVALID_INDEX),
        })
    }

    /// Get the block ID.
    #[inline]
    pub fn block_id(&self) -> BlockId {
        self.block_id
    }

    /// Get the memory tag.
    #[inline]
    pub fn tag(&self) -> MemoryTag {
        self.tag
    }

    /// Get the block size.
    #[inline]
    pub fn size(&self) -> usize {
        self.size
    }

    /// Check if this block can be destroyed when unpinned.
    #[inline]
    pub fn can_destroy(&self) -> bool {
        self.can_destroy
    }

    /// Get current block state.
    #[inline]
    pub fn state(&self) -> BlockState {
        BlockState::from(self.state.load(Ordering::Acquire))
    }

    /// Check if block is loaded in memory.
    #[inline]
    pub fn is_loaded(&self) -> bool {
        self.state() == BlockState::Loaded
    }

    /// Get current pin count.
    #[inline]
    pub fn pin_count(&self) -> i32 {
        self.pin_count.load(Ordering::Acquire)
    }

    /// Check if block is currently pinned.
    #[inline]
    pub fn is_pinned(&self) -> bool {
        self.pin_count() > 0
    }

    /// Try to publish one pin against the current loaded allocation.
    ///
    /// The lifecycle lock closes the check-then-pin race with eviction. A
    /// caller that observes `None` must load the block before retrying.
    pub(crate) fn try_pin(&self) -> Option<i32> {
        let _lifecycle = self.lifecycle.lock().unwrap();
        if self.state() != BlockState::Loaded {
            return None;
        }
        Some(self.pin_count.fetch_add(1, Ordering::AcqRel) + 1)
    }

    /// Increment pin count. Returns new count.
    ///
    /// # Panics
    /// Panics if block is not loaded. Buffer-pool callers should use
    /// [`Self::try_pin`] when eviction may race with acquisition.
    pub fn pin(&self) -> i32 {
        self.try_pin().expect("Cannot pin unloaded block")
    }

    /// Decrement pin count. Returns new count.
    ///
    /// # Panics
    /// Panics if pin count would go negative.
    pub fn unpin(&self) -> i32 {
        let old = self.pin_count.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(old > 0, "Pin count cannot go negative");
        old - 1
    }

    // LRU enhancement methods

    /// Get buffer type
    #[inline]
    pub fn buffer_type(&self) -> FileBufferType {
        self.buffer_type
    }

    /// Set eviction queue index (MANAGED_BUFFER only, can only be set once).
    ///
    /// # Panics
    /// - Panics if buffer_type is not MANAGED_BUFFER
    /// - Panics if eviction_queue_idx has already been set
    pub fn set_eviction_queue_idx(&self, idx: usize) {
        debug_assert!(
            self.buffer_type.supports_eviction_queue_idx(),
            "Only MANAGED_BUFFER supports eviction_queue_idx, got {:?}",
            self.buffer_type
        );
        debug_assert_eq!(
            self.eviction_queue_idx.load(Ordering::Acquire),
            INVALID_INDEX,
            "eviction_queue_idx can only be set once"
        );
        self.eviction_queue_idx.store(idx, Ordering::Release);
    }

    /// Get eviction queue index.
    ///
    /// Returns INVALID_INDEX (usize::MAX) if not set.
    #[inline]
    pub fn get_eviction_queue_idx(&self) -> usize {
        self.eviction_queue_idx.load(Ordering::Acquire)
    }

    /// Check if block must be written to disk before eviction
    #[inline]
    pub fn must_write_to_disk(&self) -> bool {
        self.buffer_type.must_write_to_disk()
    }

    /// Increment and return the next eviction sequence number.
    ///
    /// Called when adding this block to the eviction queue.
    /// Helps detect stale queue entries.
    #[inline]
    pub fn next_eviction_seq_num(&self) -> u64 {
        self.eviction_seq_num.fetch_add(1, Ordering::Relaxed) + 1
    }

    /// Get current eviction sequence number.
    #[inline]
    pub fn current_eviction_seq_num(&self) -> u64 {
        self.eviction_seq_num.load(Ordering::Acquire)
    }

    /// Set LRU timestamp (milliseconds since epoch).
    #[inline]
    pub fn set_lru_timestamp(&self, ts: u64) {
        self.lru_timestamp.store(ts, Ordering::Release);
    }

    /// Get LRU timestamp.
    #[inline]
    pub fn get_lru_timestamp(&self) -> u64 {
        self.lru_timestamp.load(Ordering::Acquire)
    }

    /// Check if this block can be unloaded (unpinned and can_destroy=true).
    ///
    /// Used by eviction logic to determine if block is a candidate for eviction.
    /// This is a simplified version that doesn't check for temporary directory availability.
    #[inline]
    pub fn can_unload(&self) -> bool {
        self.pin_count() == 0 && self.can_destroy && self.state() != BlockState::Unloaded
    }

    /// Get pointer to buffer data.
    ///
    /// # Safety
    /// Caller must ensure block is pinned before accessing.
    #[inline]
    pub fn data_ptr(&self) -> Option<*mut u8> {
        NonNull::new(self.buffer.load(Ordering::Acquire)).map(NonNull::as_ptr)
    }

    /// Get data as a mutable slice.
    ///
    /// # Safety
    /// Caller must ensure:
    /// - Block is pinned
    /// - No concurrent mutable access
    #[inline]
    #[allow(clippy::mut_from_ref)] // Interior mutability via raw pointer is intentional
    pub unsafe fn data_mut(&self) -> Option<&mut [u8]> {
        NonNull::new(self.buffer.load(Ordering::Acquire))
            .map(|p| std::slice::from_raw_parts_mut(p.as_ptr(), self.size))
    }

    /// Get data as an immutable slice.
    ///
    /// # Safety
    /// Caller must ensure block is pinned.
    #[inline]
    pub unsafe fn data(&self) -> Option<&[u8]> {
        NonNull::new(self.buffer.load(Ordering::Acquire))
            .map(|p| std::slice::from_raw_parts(p.as_ptr(), self.size))
    }

    /// Check if this block can be unloaded.
    ///
    /// A block can be unloaded if:
    /// - It is not already unloaded
    /// - It has no active readers (pin_count == 0)
    /// - If it needs to write to disk, a temporary directory is available
    pub fn can_unload_check(&self, has_temp_directory: bool) -> bool {
        if self.state() == BlockState::Unloaded {
            // Already unloaded
            return false;
        }
        if self.pin_count() > 0 {
            // There are active readers
            return false;
        }
        // Only check temp directory if this buffer type needs to write to disk
        if self.must_write_to_disk() && !has_temp_directory {
            // This block needs to be written to disk but no temp directory is available
            return false;
        }
        true
    }

    /// Unload the block and take ownership of the buffer.
    ///
    /// This method unloads the block from memory and returns the buffer.
    /// If the block needs to be persisted (temporary block that must write to disk),
    /// the caller is responsible for writing it to the temporary file manager.
    ///
    /// Returns None if the block is already unloaded.
    ///
    /// # Arguments
    /// * `write_to_temp_file` - Callback to write buffer to temporary file if needed.
    ///   Called with (block_id, buffer_data) and should return Result<()>.
    ///
    /// # Panics
    /// Panics if the block cannot be unloaded (has active readers or other constraints).
    pub fn unload_and_take_block<F>(
        &self,
        has_temp_directory: bool,
        mut write_to_temp_file: F,
    ) -> Result<Option<Vec<u8>>>
    where
        F: FnMut(BlockId, &[u8]) -> Result<()>,
    {
        let _lifecycle = self.lifecycle.lock().unwrap();
        if self.state() == BlockState::Unloaded {
            // Already unloaded: nothing to do
            return Ok(None);
        }

        // A queue node is only a hint. A pin may have been published after the
        // node was dequeued, so eligibility must be rechecked while holding the
        // same lifecycle lock used by `try_pin`.
        if self.pin_count() > 0 || !self.can_destroy {
            return Ok(None);
        }
        if self.must_write_to_disk() && !has_temp_directory {
            return Err(paro_error::internal(format!(
                "Cannot unload block {} without a temporary directory",
                self.block_id
            )));
        }

        let buffer = self.buffer.load(Ordering::Acquire);
        // If this is a temporary block that must be persisted, write it to disk
        if self.must_write_to_disk() {
            if let Some(ptr) = NonNull::new(buffer) {
                // SAFETY: The lifecycle lock excludes pin publication and the
                // zero pin count proves no reader can retain this allocation.
                let data = unsafe { std::slice::from_raw_parts(ptr.as_ptr(), self.size) };
                write_to_temp_file(self.block_id, data)?;
            }
        }

        // Take ownership of the buffer
        let detached = if let Some(ptr) =
            NonNull::new(self.buffer.swap(std::ptr::null_mut(), Ordering::AcqRel))
        {
            // SAFETY: The lifecycle lock and zero pin count give exclusive
            // ownership while the allocation is copied and released.
            let data = unsafe { std::slice::from_raw_parts(ptr.as_ptr(), self.size) };
            let vec = data.to_vec();
            // Free the original allocation
            self.allocator.free(ptr.as_ptr(), self.size);
            Some(vec)
        } else {
            None
        };

        // Update state
        self.state
            .store(BlockState::Unloaded as u8, Ordering::Release);

        Ok(detached)
    }

    /// Unload the block (deallocate memory).
    ///
    /// This is a convenience wrapper around `unload_and_take_block` that
    /// discards the buffer instead of returning it.
    pub fn unload<F>(&self, has_temp_directory: bool, write_to_temp_file: F) -> Result<()>
    where
        F: FnMut(BlockId, &[u8]) -> Result<()>,
    {
        self.unload_and_take_block(has_temp_directory, write_to_temp_file)?;
        Ok(())
    }

    /// Set the buffer data and mark the block as loaded.
    ///
    /// This method is used when loading a block from disk or temporary file.
    /// It takes ownership of the provided buffer and marks the block as loaded.
    ///
    /// # Arguments
    /// * `buffer` - The buffer data to set
    ///
    /// # Panics
    /// Panics if the buffer size doesn't match the block size.
    pub fn set_buffer(&self, buffer: Vec<u8>) -> Result<()> {
        if buffer.len() != self.size {
            return Err(paro_error::internal(format!(
                "Buffer size mismatch: expected {}, got {}",
                self.size,
                buffer.len()
            )));
        }

        // Allocate memory and copy data
        let ptr = self.allocator.allocate(self.size)?;
        unsafe {
            std::ptr::copy_nonoverlapping(buffer.as_ptr(), ptr, self.size);
        }

        // SAFETY: The allocator returned a valid allocation of `self.size`.
        self.install_buffer(unsafe { NonNull::new_unchecked(ptr) })
    }

    /// Reconstruct an evicted scratch block directly in its final allocation.
    /// This avoids a temporary `Vec` plus a second full-size copy for large
    /// query workspaces.
    pub(crate) fn reconstruct_zeroed(&self) -> Result<()> {
        let ptr = self.allocator.allocate_zeroed(self.size)?;
        // SAFETY: The allocator returned a valid zeroed allocation of
        // `self.size`, owned exclusively by this unloaded block.
        self.install_buffer(unsafe { NonNull::new_unchecked(ptr) })
    }

    fn install_buffer(&self, non_null: NonNull<u8>) -> Result<()> {
        let _lifecycle = self.lifecycle.lock().unwrap();
        if self.state() != BlockState::Unloaded
            || !self.buffer.load(Ordering::Acquire).is_null()
            || self.pin_count() != 0
        {
            self.allocator.free(non_null.as_ptr(), self.size);
            return Err(paro_error::internal(format!(
                "cannot install allocation into loaded block {}",
                self.block_id
            )));
        }
        self.buffer.store(non_null.as_ptr(), Ordering::Release);

        // Publish the pointer before the loaded state. `try_pin` takes the same
        // lifecycle lock and therefore cannot observe a half-published block.
        self.state
            .store(BlockState::Loaded as u8, Ordering::Release);
        Ok(())
    }
}

impl Drop for BlockHandle {
    fn drop(&mut self) {
        // Deallocate buffer if still loaded
        if let Some(ptr) = NonNull::new(*self.buffer.get_mut()) {
            self.allocator.free(ptr.as_ptr(), self.size);
        }
    }
}

/// Shared handle to a block (thread-safe reference counting).
pub type SharedBlockHandle = Arc<BlockHandle>;

#[cfg(test)]
mod tests {
    use super::*;
    use paro_common::allocator::default_allocator;

    #[test]
    fn test_block_handle_creation() {
        let handle = BlockHandle::new(
            1,
            MemoryTag::InMemoryTable,
            4096,
            true,
            Arc::new(default_allocator().clone()),
            FileBufferType::ManagedBuffer,
        );
        assert_eq!(handle.block_id(), 1);
        assert_eq!(handle.size(), 4096);
        assert!(!handle.is_loaded());
        assert!(!handle.is_pinned());
        assert_eq!(handle.buffer_type(), FileBufferType::ManagedBuffer);
        assert_eq!(handle.get_eviction_queue_idx(), INVALID_INDEX);
    }

    #[test]
    fn test_block_handle_allocate() {
        let handle = BlockHandle::allocate(
            1,
            MemoryTag::HashTable,
            1024,
            true,
            Arc::new(default_allocator().clone()),
            FileBufferType::Block,
        )
        .unwrap();
        assert!(handle.is_loaded());
        assert!(handle.is_pinned());
        assert_eq!(handle.pin_count(), 1);
        assert!(handle.data_ptr().is_some());
        assert_eq!(handle.buffer_type(), FileBufferType::Block);
    }

    #[test]
    fn test_pin_unpin() {
        let handle = BlockHandle::allocate(
            1,
            MemoryTag::InMemoryTable,
            1024,
            true,
            Arc::new(default_allocator().clone()),
            FileBufferType::ManagedBuffer,
        )
        .unwrap();
        assert_eq!(handle.pin_count(), 1);

        assert_eq!(handle.pin(), 2);
        assert_eq!(handle.pin(), 3);
        assert_eq!(handle.pin_count(), 3);

        assert_eq!(handle.unpin(), 2);
        assert_eq!(handle.unpin(), 1);
        assert_eq!(handle.unpin(), 0);
        assert!(!handle.is_pinned());
    }

    #[test]
    fn test_data_access() {
        let handle = BlockHandle::allocate(
            1,
            MemoryTag::InMemoryTable,
            1024,
            true,
            Arc::new(default_allocator().clone()),
            FileBufferType::ManagedBuffer,
        )
        .unwrap();

        // SAFETY: Block is pinned
        unsafe {
            let data = handle.data_mut().unwrap();
            data[0] = 42;
            data[1023] = 99;

            let data = handle.data().unwrap();
            assert_eq!(data[0], 42);
            assert_eq!(data[1023], 99);
        }
    }

    #[test]
    fn test_unload() {
        let handle = BlockHandle::allocate(
            1,
            MemoryTag::InMemoryTable,
            1024,
            true,
            Arc::new(default_allocator().clone()),
            FileBufferType::ManagedBuffer,
        )
        .unwrap();

        // Cannot unload while pinned
        assert!(!handle.can_unload_check(true));

        // Unpin first
        handle.unpin();
        assert!(handle.can_unload_check(true));

        // Unload with no-op write callback
        let result = handle.unload(true, |_block_id, _data| Ok(()));
        assert!(result.is_ok());
        assert!(!handle.is_loaded());
        assert!(handle.data_ptr().is_none());
    }

    #[test]
    fn test_unload_and_take_block() {
        let handle = BlockHandle::allocate(
            1,
            MemoryTag::InMemoryTable,
            1024,
            true,
            Arc::new(default_allocator().clone()),
            FileBufferType::ManagedBuffer,
        )
        .unwrap();

        // Write some data
        unsafe {
            let data = handle.data_mut().unwrap();
            data[0] = 42;
            data[100] = 99;
        }

        // Unpin
        handle.unpin();

        // Unload and take buffer
        let buffer = handle
            .unload_and_take_block(true, |_block_id, _data| Ok(()))
            .unwrap();
        assert!(buffer.is_some());
        let buffer = buffer.unwrap();
        assert_eq!(buffer.len(), 1024);
        assert_eq!(buffer[0], 42);
        assert_eq!(buffer[100], 99);

        // Block should be unloaded
        assert!(!handle.is_loaded());
        assert!(handle.data_ptr().is_none());
    }

    #[test]
    fn test_unload_with_temp_file_write() {
        let handle = BlockHandle::allocate(
            1,
            MemoryTag::OrderBy,
            1024,
            true,
            Arc::new(default_allocator().clone()),
            FileBufferType::ManagedBuffer, // Must write to disk
        )
        .unwrap();

        // Write some data
        unsafe {
            let data = handle.data_mut().unwrap();
            data[0] = 42;
        }

        // Unpin
        handle.unpin();

        // Track if write callback was called
        let mut write_called = false;
        let mut written_block_id = 0;
        let mut written_data = Vec::new();

        let result = handle.unload(true, |block_id, data| {
            write_called = true;
            written_block_id = block_id;
            written_data = data.to_vec();
            Ok(())
        });

        assert!(result.is_ok());
        assert!(write_called);
        assert_eq!(written_block_id, 1);
        assert_eq!(written_data.len(), 1024);
        assert_eq!(written_data[0], 42);
    }

    #[test]
    fn test_can_unload_check() {
        let handle = BlockHandle::allocate(
            1,
            MemoryTag::InMemoryTable,
            1024,
            true,
            Arc::new(default_allocator().clone()),
            FileBufferType::ManagedBuffer,
        )
        .unwrap();

        // Cannot unload while pinned
        assert!(!handle.can_unload_check(true));

        // Unpin
        handle.unpin();
        assert!(handle.can_unload_check(true));

        // Can unload even without temp directory if buffer doesn't need to write
        let handle2 = BlockHandle::allocate(
            2,
            MemoryTag::InMemoryTable,
            1024,
            true,
            Arc::new(default_allocator().clone()),
            FileBufferType::Block, // Doesn't need to write to disk
        )
        .unwrap();
        handle2.unpin();
        assert!(handle2.can_unload_check(false));

        // Cannot unload if needs temp directory but none available
        let handle3 = BlockHandle::allocate(
            3,
            MemoryTag::OrderBy,
            1024,
            true,
            Arc::new(default_allocator().clone()),
            FileBufferType::ManagedBuffer, // Needs to write to disk
        )
        .unwrap();
        handle3.unpin();
        assert!(!handle3.can_unload_check(false)); // No temp directory
        assert!(handle3.can_unload_check(true)); // With temp directory
    }

    #[test]
    fn test_eviction_queue_idx() {
        let handle = BlockHandle::allocate(
            1,
            MemoryTag::OrderBy,
            4096,
            true,
            Arc::new(default_allocator().clone()),
            FileBufferType::ManagedBuffer,
        )
        .unwrap();

        // Initially INVALID_INDEX
        assert_eq!(handle.get_eviction_queue_idx(), INVALID_INDEX);

        // Can set once
        handle.set_eviction_queue_idx(2);
        assert_eq!(handle.get_eviction_queue_idx(), 2);
    }

    #[test]
    #[should_panic(expected = "eviction_queue_idx can only be set once")]
    fn test_eviction_queue_idx_set_twice() {
        let handle = BlockHandle::allocate(
            1,
            MemoryTag::OrderBy,
            4096,
            true,
            Arc::new(default_allocator().clone()),
            FileBufferType::ManagedBuffer,
        )
        .unwrap();

        handle.set_eviction_queue_idx(2);
        handle.set_eviction_queue_idx(3); // Should panic
    }

    #[test]
    #[should_panic(expected = "Only MANAGED_BUFFER supports eviction_queue_idx")]
    fn test_eviction_queue_idx_wrong_type() {
        let handle = BlockHandle::allocate(
            1,
            MemoryTag::InMemoryTable,
            1024,
            true,
            Arc::new(default_allocator().clone()),
            FileBufferType::Block, // Not MANAGED_BUFFER
        )
        .unwrap();

        handle.set_eviction_queue_idx(0); // Should panic
    }

    #[test]
    fn test_buffer_type_methods() {
        let managed = BlockHandle::allocate(
            1,
            MemoryTag::OrderBy,
            1024,
            true,
            Arc::new(default_allocator().clone()),
            FileBufferType::ManagedBuffer,
        )
        .unwrap();
        assert!(managed.must_write_to_disk());

        let block = BlockHandle::allocate(
            2,
            MemoryTag::InMemoryTable,
            1024,
            true,
            Arc::new(default_allocator().clone()),
            FileBufferType::Block,
        )
        .unwrap();
        assert!(!block.must_write_to_disk());
    }

    #[test]
    fn test_eviction_seq_num() {
        let handle = BlockHandle::allocate(
            1,
            MemoryTag::OrderBy,
            1024,
            true,
            Arc::new(default_allocator().clone()),
            FileBufferType::ManagedBuffer,
        )
        .unwrap();

        assert_eq!(handle.current_eviction_seq_num(), 0);
        assert_eq!(handle.next_eviction_seq_num(), 1);
        assert_eq!(handle.current_eviction_seq_num(), 1);
        assert_eq!(handle.next_eviction_seq_num(), 2);
        assert_eq!(handle.current_eviction_seq_num(), 2);
    }

    #[test]
    fn test_memory_tag() {
        assert_eq!(MemoryTag::default(), MemoryTag::Allocator);
        assert_eq!(MemoryTag::HashTable as u8, 1);
        assert_eq!(MemoryTag::OrderBy.as_index(), 4);
    }

    #[test]
    fn test_shared_block_handle() {
        let handle: SharedBlockHandle = Arc::new(
            BlockHandle::allocate(
                1,
                MemoryTag::InMemoryTable,
                1024,
                true,
                Arc::new(default_allocator().clone()),
                FileBufferType::ManagedBuffer,
            )
            .unwrap(),
        );

        // Clone creates another reference
        let handle2 = handle.clone();
        assert_eq!(Arc::strong_count(&handle), 2);

        // Both can pin
        handle.pin();
        handle2.pin();
        assert_eq!(handle.pin_count(), 3); // 1 from allocate + 2 pins

        // Both can unpin
        handle.unpin();
        handle2.unpin();
        assert_eq!(handle.pin_count(), 1);
    }
}
