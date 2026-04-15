//! StandardBufferManager - Concrete implementation of BufferManager.
//!
//! - Wraps BufferPool with additional functionality
//! - Manages temporary spill path for spill-to-disk
//! - Provides block allocation size configuration

use std::sync::Arc;

use paro_common::allocator::{
    Allocator, BufferAllocator, BufferManager as CommonBufferManager, MemoryTag,
};
use paro_common::error::{self as paro_error, Result};

use super::buffer_handle::BufferHandle;
use super::buffer_manager::BufferManager;
use super::buffer_pool::BufferPool;
use super::file_buffer_type::FileBufferType;
use super::{BlockId, SharedBlockHandle, DEFAULT_BLOCK_ALLOC_SIZE};

/// Standard implementation of BufferManager.
///
/// StandardBufferManager wraps a BufferPool and provides:
/// - Configurable block allocation size
/// - Memory tracking and limits
/// - Integration with BufferPool spill/reload
///
/// # Example
/// ```ignore
/// let manager = StandardBufferManager::new(1024 * 1024 * 1024, 262144, 8);
/// let handle = manager.allocate_temp(MemoryTag::InMemoryTable, 4096)?;
/// ```
pub struct StandardBufferManager {
    /// The underlying buffer pool (wrapped in Arc for TempBufferPoolReservation)
    buffer_pool: Arc<BufferPool>,
    /// Shared buffer allocator
    buffer_allocator: Arc<BufferAllocator>,
    /// Block allocation size (including header)
    block_alloc_size: usize,
    /// Block header size
    block_header_size: usize,
}

impl StandardBufferManager {
    /// Create a StandardBufferManager that wraps an existing BufferPool.
    ///
    /// # Arguments
    /// * `buffer_pool` - Shared buffer pool
    /// * `block_alloc_size` - Block allocation size including header
    /// * `block_header_size` - Size of block header
    pub fn new_with_pool(
        buffer_pool: Arc<BufferPool>,
        block_alloc_size: usize,
        block_header_size: usize,
    ) -> Self {
        let buffer_allocator = Arc::new(BufferAllocator::new(
            buffer_pool.clone() as Arc<dyn CommonBufferManager>,
            MemoryTag::Allocator,
        ));

        Self {
            buffer_pool,
            buffer_allocator,
            block_alloc_size,
            block_header_size,
        }
    }

    /// Create a StandardBufferManager that creates its own BufferPool.
    ///
    /// # Arguments
    /// * `max_memory` - Memory limit for the internal pool
    /// * `block_alloc_size` - Block allocation size including header
    /// * `block_header_size` - Size of block header
    pub fn new(max_memory: usize, block_alloc_size: usize, block_header_size: usize) -> Self {
        let buffer_pool = BufferPool::new_arc(max_memory);
        Self::new_with_pool(buffer_pool, block_alloc_size, block_header_size)
    }

    /// Get the shared buffer allocator.
    pub fn get_buffer_allocator(&self) -> Arc<dyn Allocator> {
        self.buffer_allocator.clone() as Arc<dyn Allocator>
    }

    /// Create a StandardBufferManager with default block sizes.
    ///
    /// Uses DEFAULT_BLOCK_ALLOC_SIZE (256 KB) and 8-byte header.
    pub fn with_defaults(max_memory: usize) -> Self {
        Self::new(max_memory, DEFAULT_BLOCK_ALLOC_SIZE, 8)
    }

    /// Create a StandardBufferManager with default settings.
    ///
    /// Uses 1 GB memory limit and default block sizes.
    pub fn default_manager() -> Self {
        Self::with_defaults(1024 * 1024 * 1024)
    }

    /// Get the buffer pool statistics.
    pub fn stats(&self) -> &super::buffer_pool::BufferPoolStats {
        self.buffer_pool.stats()
    }

    /// Get the number of blocks in the pool.
    pub fn block_count(&self) -> usize {
        self.buffer_pool.block_count()
    }

    /// Clear all blocks from the pool.
    pub fn clear(&self) -> Result<()> {
        self.buffer_pool.clear()
    }
}

impl CommonBufferManager for StandardBufferManager {
    fn allocate(&self, tag: MemoryTag, size: usize) -> Result<*mut u8> {
        <BufferPool as CommonBufferManager>::allocate(&self.buffer_pool, tag, size)
    }

    fn free(&self, ptr: *mut u8, tag: MemoryTag, size: usize) {
        <BufferPool as CommonBufferManager>::free(&self.buffer_pool, ptr, tag, size)
    }

    fn reallocate(
        &self,
        ptr: *mut u8,
        tag: MemoryTag,
        old_size: usize,
        new_size: usize,
    ) -> Result<*mut u8> {
        <BufferPool as CommonBufferManager>::reallocate(
            &self.buffer_pool,
            ptr,
            tag,
            old_size,
            new_size,
        )
    }
}

impl BufferManager for StandardBufferManager {
    fn allocate(&self, tag: MemoryTag, size: usize, can_destroy: bool) -> Result<BufferHandle> {
        if can_destroy {
            self.buffer_pool
                .allocate(tag, FileBufferType::ManagedBuffer, size)
        } else {
            self.buffer_pool
                .allocate_persistent(tag, FileBufferType::ManagedBuffer, size)
        }
    }

    fn allocate_memory(
        &self,
        tag: MemoryTag,
        block_size: usize,
        can_destroy: bool,
    ) -> Result<SharedBlockHandle> {
        let handle = if can_destroy {
            self.buffer_pool
                .allocate(tag, FileBufferType::ManagedBuffer, block_size)?
        } else {
            self.buffer_pool
                .allocate_persistent(tag, FileBufferType::ManagedBuffer, block_size)?
        };

        // Get the block handle from the buffer handle
        let block_handle = handle
            .block_handle()
            .cloned()
            .ok_or_else(|| paro_error::internal("Failed to get block handle from buffer"))?;

        // Pin again before dropping BufferHandle, so the block stays pinned
        block_handle.pin();

        // BufferHandle will unpin on drop, but we've added an extra pin
        Ok(block_handle)
    }

    fn pin(&self, handle: &SharedBlockHandle) -> Result<BufferHandle> {
        let block_id = handle.block_id();
        self.buffer_pool.pin(block_id)
    }

    fn unpin(&self, handle: &SharedBlockHandle) {
        let block_id = handle.block_id();
        self.buffer_pool.unpin(block_id);
    }

    fn get_used_memory(&self) -> usize {
        self.buffer_pool.used_memory()
    }

    fn get_max_memory(&self) -> usize {
        self.buffer_pool.max_memory()
    }

    fn set_memory_limit(&self, limit: usize) -> Result<()> {
        self.buffer_pool.set_memory_limit(limit)
    }

    fn set_swap_limit(&self, limit: Option<usize>) -> Result<()> {
        self.buffer_pool.set_swap_limit(limit)
    }

    fn get_temporary_files(&self) -> Vec<super::TemporaryFileInfo> {
        self.buffer_pool.get_temporary_files()
    }

    fn get_temporary_spill_metrics(&self) -> super::TemporarySpillMetricsSnapshot {
        self.buffer_pool.get_temporary_spill_metrics()
    }

    fn set_temporary_directory(&self, path: String) -> Result<()> {
        self.buffer_pool.set_temporary_directory(path)
    }

    fn get_block_alloc_size(&self) -> usize {
        self.block_alloc_size
    }

    fn get_block_size(&self) -> usize {
        self.block_alloc_size - self.block_header_size
    }

    fn get_buffer_pool(&self) -> &BufferPool {
        &self.buffer_pool
    }

    fn pin_by_id(&self, block_id: BlockId) -> Result<BufferHandle> {
        self.buffer_pool.pin(block_id)
    }

    fn unpin_by_id(&self, block_id: BlockId) {
        self.buffer_pool.unpin(block_id);
    }

    fn evict(&self, target_bytes: usize) -> usize {
        let current_used = self.buffer_pool.used_memory();
        let memory_limit = current_used.saturating_sub(target_bytes);

        // Call evict_blocks with extra_memory=0 to avoid creating a reservation
        // We just want to free memory, not reserve it for allocation
        let result = self.buffer_pool.evict_blocks(
            MemoryTag::Allocator,
            0, // No reservation needed
            memory_limit,
            None,
        );

        if result.success {
            current_used.saturating_sub(self.buffer_pool.used_memory())
        } else {
            0
        }
    }

    fn get_buffer_allocator(&self) -> Arc<dyn Allocator> {
        self.buffer_allocator.clone() as Arc<dyn Allocator>
    }
}

impl std::fmt::Debug for StandardBufferManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StandardBufferManager")
            .field("max_memory", &self.get_max_memory())
            .field("used_memory", &self.get_used_memory())
            .field("block_alloc_size", &self.block_alloc_size)
            .field("block_header_size", &self.block_header_size)
            .field("block_count", &self.block_count())
            .finish()
    }
}

impl Default for StandardBufferManager {
    fn default() -> Self {
        Self::default_manager()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_standard_buffer_manager_creation() {
        let manager = StandardBufferManager::new(1024 * 1024, 262144, 8);
        assert_eq!(manager.get_max_memory(), 1024 * 1024);
        assert_eq!(manager.get_used_memory(), 0);
        assert_eq!(manager.get_block_alloc_size(), 262144);
        assert_eq!(manager.get_block_size(), 262144 - 8);
    }

    #[test]
    fn test_new_with_pool() {
        let pool = Arc::new(BufferPool::new(1024 * 1024));
        let manager = StandardBufferManager::new_with_pool(pool.clone(), 262144, 8);
        assert_eq!(manager.get_max_memory(), 1024 * 1024);
        // Verify they share the pool
        assert_eq!(
            manager.get_buffer_pool() as *const BufferPool,
            Arc::as_ptr(&pool)
        );
    }

    #[test]
    fn test_with_defaults() {
        let manager = StandardBufferManager::with_defaults(2 * 1024 * 1024);
        assert_eq!(manager.get_max_memory(), 2 * 1024 * 1024);
        assert_eq!(manager.get_block_alloc_size(), DEFAULT_BLOCK_ALLOC_SIZE);
    }

    #[test]
    fn test_default_manager() {
        let manager = StandardBufferManager::default_manager();
        assert_eq!(manager.get_max_memory(), 1024 * 1024 * 1024);
    }

    #[test]
    fn test_allocate_temp() {
        let manager = StandardBufferManager::with_defaults(1024 * 1024);

        let handle = manager
            .allocate_temp(MemoryTag::InMemoryTable, 4096)
            .unwrap();
        assert!(handle.is_valid());
        assert_eq!(handle.size(), 4096);
        assert!(manager.get_used_memory() >= 4096);
    }

    #[test]
    fn test_allocate_persistent() {
        let manager = StandardBufferManager::with_defaults(1024 * 1024);

        let handle = BufferManager::allocate(&manager, MemoryTag::HashTable, 8192, false).unwrap();
        assert!(handle.is_valid());
        assert_eq!(handle.size(), 8192);
    }

    #[test]
    fn test_allocate_memory() {
        let manager = StandardBufferManager::with_defaults(1024 * 1024);

        let block_handle = manager
            .allocate_memory(MemoryTag::OrderBy, 4096, true)
            .unwrap();
        assert!(block_handle.is_loaded());
        assert_eq!(block_handle.size(), 4096);
    }

    #[test]
    fn test_pin_unpin() {
        let manager = StandardBufferManager::with_defaults(1024 * 1024);

        let block_handle = manager
            .allocate_memory(MemoryTag::InMemoryTable, 4096, true)
            .unwrap();

        // Initial pin count is 1 from allocation
        assert_eq!(block_handle.pin_count(), 1);

        // Pin again
        let buffer_handle = BufferManager::pin(&manager, &block_handle).unwrap();
        assert!(buffer_handle.is_valid());
        assert_eq!(block_handle.pin_count(), 2);

        // Unpin via buffer handle drop
        drop(buffer_handle);
        assert_eq!(block_handle.pin_count(), 1);
    }

    #[test]
    fn test_pin_by_id() {
        let manager = StandardBufferManager::with_defaults(1024 * 1024);

        let handle = manager
            .allocate_temp(MemoryTag::InMemoryTable, 4096)
            .unwrap();
        let block_id = handle.block_handle().unwrap().block_id();

        // Pin by ID
        let handle2 = BufferManager::pin_by_id(&manager, block_id).unwrap();
        assert!(handle2.is_valid());
    }

    #[test]
    fn test_eviction() {
        let manager = StandardBufferManager::with_defaults(8192);

        // Set temporary directory for ManagedBuffer eviction
        let temp_dir = std::env::temp_dir().join("paro_test_eviction");
        manager
            .buffer_pool
            .set_temporary_directory(temp_dir.to_string_lossy().to_string())
            .unwrap();

        // Allocate a block
        let handle = manager
            .allocate_temp(MemoryTag::InMemoryTable, 2048)
            .unwrap();
        let block_id = handle.block_handle().unwrap().block_id();

        // Drop handle to make it evictable
        drop(handle);

        // Add to eviction queue
        manager.buffer_pool.add_to_eviction_queue(block_id);

        // Record memory before eviction
        let _used_before = manager.get_used_memory();

        // Evict
        let freed = manager.evict(2048);

        // After eviction, the block should be unloaded but BlockHandle still exists
        // So freed memory should be approximately 2048 (may be less due to reservation overhead)
        assert!(freed > 0, "Should have freed some memory");

        // Verify block is unloaded
        let block = manager.buffer_pool.get_block(block_id);
        assert!(block.is_some(), "BlockHandle should still exist");
        assert!(!block.unwrap().is_loaded(), "Block should be unloaded");
    }

    #[test]
    fn test_available_memory() {
        let manager = StandardBufferManager::with_defaults(10000);

        assert_eq!(manager.get_available_memory(), 10000);

        let _handle = manager
            .allocate_temp(MemoryTag::InMemoryTable, 3000)
            .unwrap();
        assert_eq!(manager.get_available_memory(), 7000);
    }

    #[test]
    fn test_data_access() {
        let manager = StandardBufferManager::with_defaults(1024 * 1024);

        let handle = manager
            .allocate_temp(MemoryTag::InMemoryTable, 256)
            .unwrap();

        // Write data
        unsafe {
            let data = handle.data_mut().unwrap();
            for i in 0..256 {
                data[i] = i as u8;
            }
        }

        // Read data
        let data = handle.data().unwrap();
        for i in 0..256 {
            assert_eq!(data[i], i as u8);
        }
    }

    #[test]
    fn test_debug_format() {
        let manager = StandardBufferManager::with_defaults(1024 * 1024);
        let debug_str = format!("{:?}", manager);
        assert!(debug_str.contains("StandardBufferManager"));
        assert!(debug_str.contains("max_memory"));
    }

    #[test]
    fn test_stats() {
        let manager = StandardBufferManager::with_defaults(1024 * 1024);

        let _h1 = manager
            .allocate_temp(MemoryTag::InMemoryTable, 1024)
            .unwrap();
        let _h2 = manager.allocate_temp(MemoryTag::HashTable, 2048).unwrap();

        let stats = manager.stats();
        assert_eq!(
            stats.allocations.load(std::sync::atomic::Ordering::Relaxed),
            2
        );
    }

    #[test]
    fn test_clear() {
        let manager = StandardBufferManager::with_defaults(1024 * 1024);

        let handle = manager
            .allocate_temp(MemoryTag::InMemoryTable, 1024)
            .unwrap();
        let _block_id = handle.block_handle().unwrap().block_id();
        drop(handle);

        assert_eq!(manager.block_count(), 1);

        manager.clear().unwrap();
        assert_eq!(manager.block_count(), 0);
    }

    #[test]
    fn test_set_memory_limit() {
        let manager = StandardBufferManager::with_defaults(1024 * 1024);
        manager.set_memory_limit(512 * 1024).unwrap();
        assert_eq!(manager.get_max_memory(), 512 * 1024);
    }

    #[test]
    fn test_set_swap_limit_and_temporary_directory() {
        let manager = StandardBufferManager::with_defaults(1024 * 1024);
        let temp_dir = std::env::temp_dir().join("paro_test_manager_temp_dir");

        manager
            .set_temporary_directory(temp_dir.to_string_lossy().to_string())
            .unwrap();
        manager.set_swap_limit(Some(64 * 1024)).unwrap();

        // No spill yet, so temporary files should be empty.
        assert!(manager.get_temporary_files().is_empty());
    }
}
