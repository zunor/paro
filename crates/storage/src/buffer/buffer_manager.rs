//! BufferManager - Abstract interface for buffer management.
//!
//! - Abstract BufferManager trait for different implementations
//! - StandardBufferManager as the concrete implementation
//! - Integration with BufferPool for managed allocation and spill
//! - Memory tracking via MemoryTag

use std::sync::Arc;

use paro_common::allocator::{Allocator, BufferManager as CommonBufferManager, MemoryTag};
use paro_common::error::Result;

use super::buffer_handle::BufferHandle;
use super::buffer_pool::BufferPool;
use super::{BlockId, SharedBlockHandle, TemporaryFileInfo, TemporarySpillMetricsSnapshot};

/// Abstract interface for buffer management.
///
/// BufferManager is responsible for:
/// - Allocating temporary memory
/// - Allocating block-based memory via BufferPool
/// - Pin/Unpin operations
/// - Memory usage tracking
///
/// # Thread Safety
/// All implementations must be thread-safe (Send + Sync).
pub trait BufferManager: CommonBufferManager + Send + Sync + std::fmt::Debug {
    /// Allocate temporary memory of the given size and return a pinned handle.
    ///
    /// # Arguments
    /// * `tag` - Memory tag for tracking
    /// * `size` - Size in bytes to allocate
    /// * `can_destroy` - Whether the buffer can be destroyed when unpinned
    ///
    /// # Returns
    /// A `BufferHandle` to the pinned block.
    fn allocate(&self, tag: MemoryTag, size: usize, can_destroy: bool) -> Result<BufferHandle>;

    /// Allocate temporary memory with default can_destroy=true.
    fn allocate_temp(&self, tag: MemoryTag, size: usize) -> Result<BufferHandle> {
        BufferManager::allocate(self, tag, size, true)
    }

    /// Allocate block-based memory using the given block size.
    ///
    /// # Arguments
    /// * `tag` - Memory tag for tracking
    /// * `block_size` - Size of the block to allocate
    /// * `can_destroy` - Whether the buffer can be destroyed when unpinned
    ///
    /// # Returns
    /// A shared handle to the allocated block.
    fn allocate_memory(
        &self,
        tag: MemoryTag,
        block_size: usize,
        can_destroy: bool,
    ) -> Result<SharedBlockHandle>;

    /// Pin a block handle and return a BufferHandle for access.
    ///
    /// # Arguments
    /// * `handle` - The block handle to pin
    ///
    /// # Returns
    /// A `BufferHandle` providing access to the pinned block.
    fn pin(&self, handle: &SharedBlockHandle) -> Result<BufferHandle>;

    /// Unpin a block handle.
    ///
    /// This is typically called automatically when BufferHandle is dropped.
    fn unpin(&self, handle: &SharedBlockHandle);

    /// Get current memory usage in bytes.
    fn get_used_memory(&self) -> usize;

    /// Get maximum memory limit in bytes.
    fn get_max_memory(&self) -> usize;

    /// Set maximum memory limit in bytes.
    fn set_memory_limit(&self, limit: usize) -> Result<()>;

    /// Set maximum swap limit in bytes (`None` means unlimited).
    fn set_swap_limit(&self, limit: Option<usize>) -> Result<()>;

    /// Get temporary spill files.
    fn get_temporary_files(&self) -> Vec<TemporaryFileInfo>;

    /// Get temporary spill metrics (read/write bytes, file count, swap usage, etc.).
    fn get_temporary_spill_metrics(&self) -> TemporarySpillMetricsSnapshot;

    /// Set temporary directory path for spill files.
    fn set_temporary_directory(&self, path: String) -> Result<()>;

    /// Get available memory in bytes.
    fn get_available_memory(&self) -> usize {
        let used = self.get_used_memory();
        let max = self.get_max_memory();
        if max == 0 {
            usize::MAX - used
        } else {
            max.saturating_sub(used)
        }
    }

    /// Get the block allocation size for buffer-managed blocks.
    fn get_block_alloc_size(&self) -> usize;

    /// Get the block size (alloc size minus header).
    fn get_block_size(&self) -> usize;

    /// Get the underlying buffer pool.
    fn get_buffer_pool(&self) -> &BufferPool;

    /// Pin a block by ID.
    fn pin_by_id(&self, block_id: BlockId) -> Result<BufferHandle>;

    /// Unpin a block by ID.
    fn unpin_by_id(&self, block_id: BlockId);

    /// Evict blocks to free memory.
    ///
    /// # Arguments
    /// * `target_bytes` - Amount of memory to free
    ///
    /// # Returns
    /// Amount of memory actually freed.
    fn evict(&self, target_bytes: usize) -> usize;

    /// Get the shared buffer allocator.
    fn get_buffer_allocator(&self) -> Arc<dyn Allocator>;
}

/// Shared reference to a BufferManager.
pub type SharedBufferManager = Arc<dyn BufferManager>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::StandardBufferManager;

    #[test]
    fn test_buffer_manager_trait_object() {
        let manager: Arc<dyn BufferManager> =
            Arc::new(StandardBufferManager::new(1024 * 1024, 262144, 8));

        assert_eq!(manager.get_max_memory(), 1024 * 1024);
        assert_eq!(manager.get_used_memory(), 0);
        assert_eq!(manager.get_block_alloc_size(), 262144);
    }

    #[test]
    fn test_buffer_manager_allocate() {
        let manager: Arc<dyn BufferManager> =
            Arc::new(StandardBufferManager::new(1024 * 1024, 262144, 8));

        let handle = manager
            .allocate_temp(MemoryTag::InMemoryTable, 4096)
            .unwrap();
        assert!(handle.is_valid());
        assert_eq!(handle.size(), 4096);
        assert!(manager.get_used_memory() >= 4096);
    }

    #[test]
    fn test_buffer_manager_pin_unpin() {
        let manager: Arc<dyn BufferManager> =
            Arc::new(StandardBufferManager::new(1024 * 1024, 262144, 8));

        let block_handle = manager
            .allocate_memory(MemoryTag::InMemoryTable, 4096, true)
            .unwrap();

        // Initial pin count is 1 from allocation
        assert_eq!(block_handle.pin_count(), 1);

        // Pin the block again
        let buffer_handle = manager.pin(&block_handle).unwrap();
        assert!(buffer_handle.is_valid());
        assert_eq!(block_handle.pin_count(), 2);

        // Unpin via buffer handle drop
        drop(buffer_handle);
        assert_eq!(block_handle.pin_count(), 1);
    }

    #[test]
    fn test_buffer_manager_runtime_limits_and_temp_dir() {
        let manager: Arc<dyn BufferManager> =
            Arc::new(StandardBufferManager::new(1024 * 1024, 262144, 8));

        manager.set_memory_limit(512 * 1024).unwrap();
        assert_eq!(manager.get_max_memory(), 512 * 1024);

        let temp_dir = std::env::temp_dir().join("paro_buffer_manager_runtime_limit");
        manager
            .set_temporary_directory(temp_dir.to_string_lossy().to_string())
            .unwrap();
        manager.set_swap_limit(Some(128 * 1024)).unwrap();
        assert!(manager.get_temporary_files().is_empty());
    }
}
