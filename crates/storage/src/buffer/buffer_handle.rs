// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! BufferHandle - RAII wrapper for pinned blocks.
//!
//! - RAII: automatically unpins on drop
//! - Move-only: no copy semantics
//! - Provides safe data access while pinned

use std::sync::{Arc, Weak};

use super::block_handle::BlockHandle;
use super::buffer_pool::BufferPool;

/// RAII wrapper for a pinned block.
///
/// When a BufferHandle is created, the underlying block is pinned.
/// When the BufferHandle is dropped, the block is automatically unpinned.
///
/// This ensures blocks are never leaked in a pinned state.
///
/// # Example
/// ```ignore
/// let pool = BufferPool::new(1024 * 1024); // 1 MB
/// let handle = pool.allocate(MemoryTag::InMemoryTable, 4096)?;
///
/// // Block is pinned, safe to access
/// let data = handle.data_mut();
/// data[0] = 42;
///
/// // Block automatically unpinned when handle goes out of scope
/// drop(handle);
/// ```
pub struct BufferHandle {
    /// The underlying block (shared ownership)
    block: Option<Arc<BlockHandle>>,
    /// Owning buffer pool used for proper unpin/eviction queue integration
    pool: Option<Weak<BufferPool>>,
}

impl BufferHandle {
    /// Create a new buffer handle for a pinned block.
    ///
    /// The block should already be pinned when this is called.
    pub fn new(block: Arc<BlockHandle>) -> Self {
        debug_assert!(block.is_pinned(), "Block must be pinned");
        Self {
            block: Some(block),
            pool: None,
        }
    }

    /// Create a new buffer handle that notifies the pool on drop.
    pub fn with_pool(block: Arc<BlockHandle>, pool: Weak<BufferPool>) -> Self {
        debug_assert!(block.is_pinned(), "Block must be pinned");
        Self {
            block: Some(block),
            pool: Some(pool),
        }
    }

    /// Create an invalid/empty buffer handle.
    pub fn invalid() -> Self {
        Self {
            block: None,
            pool: None,
        }
    }

    fn unpin_internal(&mut self) {
        if let Some(block) = self.block.take() {
            if let Some(pool_weak) = self.pool.take() {
                if let Some(pool) = pool_weak.upgrade() {
                    pool.unpin(block.block_id());
                    return;
                }
            }
            block.unpin();
        } else {
            self.pool = None;
        }
    }

    /// Check if this handle is valid.
    #[inline]
    pub fn is_valid(&self) -> bool {
        self.block.is_some()
    }

    /// Get a pointer to the buffer data.
    ///
    /// Returns None if handle is invalid.
    #[inline]
    pub fn ptr(&self) -> Option<*mut u8> {
        self.block.as_ref().and_then(|b| b.data_ptr())
    }

    /// Get the block's data as a mutable slice.
    ///
    /// # Safety
    /// Caller must ensure no concurrent mutable access.
    #[inline]
    #[allow(clippy::mut_from_ref)] // Interior mutability via raw pointer is intentional
    pub unsafe fn data_mut(&self) -> Option<&mut [u8]> {
        self.block.as_ref().and_then(|b| b.data_mut())
    }

    /// Get the block's data as an immutable slice.
    #[inline]
    pub fn data(&self) -> Option<&[u8]> {
        // SAFETY: Block is pinned via this handle
        self.block.as_ref().and_then(|b| unsafe { b.data() })
    }

    /// Get the size of the buffer.
    #[inline]
    pub fn size(&self) -> usize {
        self.block.as_ref().map(|b| b.size()).unwrap_or(0)
    }

    /// Get access to the underlying block handle.
    #[inline]
    pub fn block_handle(&self) -> Option<&Arc<BlockHandle>> {
        self.block.as_ref()
    }

    /// Destroy this handle, unpinning the block.
    pub fn destroy(&mut self) {
        self.unpin_internal();
    }
}

impl Drop for BufferHandle {
    fn drop(&mut self) {
        // Automatically unpin when handle is dropped.
        // If we know the pool, route through pool.unpin() so the block enters eviction queues.
        self.unpin_internal();
    }
}

// Move-only semantics (no Copy/Clone)
// This is intentional - we don't want multiple handles for the same pin

impl std::fmt::Debug for BufferHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.block {
            Some(b) => f
                .debug_struct("BufferHandle")
                .field("block_id", &b.block_id())
                .field("size", &b.size())
                .field("pin_count", &b.pin_count())
                .finish(),
            None => f
                .debug_struct("BufferHandle")
                .field("valid", &false)
                .finish(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::{FileBufferType, MemoryTag};

    #[test]
    fn test_buffer_handle_valid() {
        use paro_common::allocator::default_allocator;
        let block = Arc::new(
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
        let handle = BufferHandle::new(block.clone());

        assert!(handle.is_valid());
        assert_eq!(handle.size(), 1024);
        assert!(handle.ptr().is_some());
    }

    #[test]
    fn test_buffer_handle_invalid() {
        let handle = BufferHandle::invalid();
        assert!(!handle.is_valid());
        assert_eq!(handle.size(), 0);
        assert!(handle.ptr().is_none());
    }

    #[test]
    fn test_buffer_handle_data_access() {
        use paro_common::allocator::default_allocator;
        let block = Arc::new(
            BlockHandle::allocate(
                1,
                MemoryTag::InMemoryTable,
                256,
                true,
                Arc::new(default_allocator().clone()),
                FileBufferType::ManagedBuffer,
            )
            .unwrap(),
        );
        let handle = BufferHandle::new(block);

        // SAFETY: We have exclusive access
        unsafe {
            let data = handle.data_mut().unwrap();
            data[0] = 0xAB;
            data[255] = 0xCD;
        }

        let data = handle.data().unwrap();
        assert_eq!(data[0], 0xAB);
        assert_eq!(data[255], 0xCD);
    }

    #[test]
    fn test_buffer_handle_auto_unpin() {
        use paro_common::allocator::default_allocator;
        let block = Arc::new(
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

        // Initial pin count is 1 (from allocate)
        assert_eq!(block.pin_count(), 1);

        // Pin again for the handle
        block.pin();
        assert_eq!(block.pin_count(), 2);

        {
            let _handle = BufferHandle::new(block.clone());
            // Handle doesn't add a pin, it owns one
        }
        // After drop, should have unpinned
        assert_eq!(block.pin_count(), 1);
    }

    #[test]
    fn test_buffer_handle_destroy() {
        use paro_common::allocator::default_allocator;
        let block = Arc::new(
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
        assert_eq!(block.pin_count(), 1);

        block.pin();
        assert_eq!(block.pin_count(), 2);

        let mut handle = BufferHandle::new(block.clone());
        handle.destroy();

        // Should have unpinned
        assert_eq!(block.pin_count(), 1);
        assert!(!handle.is_valid());
    }

    #[test]
    fn test_buffer_handle_debug() {
        use paro_common::allocator::default_allocator;
        let block = Arc::new(
            BlockHandle::allocate(
                42,
                MemoryTag::HashTable,
                4096,
                true,
                Arc::new(default_allocator().clone()),
                FileBufferType::ManagedBuffer,
            )
            .unwrap(),
        );
        let handle = BufferHandle::new(block);

        let debug_str = format!("{:?}", handle);
        assert!(debug_str.contains("block_id"));
        assert!(debug_str.contains("42"));
    }
}
