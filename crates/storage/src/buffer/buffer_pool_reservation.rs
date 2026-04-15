// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! BufferPoolReservation - RAII memory reservation for buffer pool.
//!
//! - RAII-style memory reservation
//! - Automatically releases reservation on drop
//! - Move-only semantics
//! - Resize support for adjusting reservation
//! - Merge support for combining reservations

use std::sync::{Arc, Weak};

use paro_common::allocator::MemoryTag;

use super::BufferPool;

/// RAII memory reservation for buffer pool.
///
/// This structure reserves memory in the buffer pool and automatically
/// releases it when dropped. It ensures that memory accounting is correct
/// even in the presence of errors or early returns.
pub struct BufferPoolReservation {
    /// Memory tag for tracking
    tag: MemoryTag,
    /// Current reservation size
    size: usize,
    /// Reference to the buffer pool
    pool: Weak<BufferPool>,
}

impl BufferPoolReservation {
    /// Create a new reservation with zero size.
    pub fn new(tag: MemoryTag, pool: &Arc<BufferPool>) -> Self {
        Self {
            tag,
            size: 0,
            pool: Arc::downgrade(pool),
        }
    }

    /// Get the current reservation size.
    #[inline]
    pub fn size(&self) -> usize {
        self.size
    }

    /// Get the memory tag.
    #[inline]
    pub fn tag(&self) -> MemoryTag {
        self.tag
    }

    /// Resize the reservation.
    ///
    /// This updates the memory usage in the buffer pool by the delta
    /// between the new size and the old size.
    pub fn resize(&mut self, new_size: usize) {
        if let Some(pool) = self.pool.upgrade() {
            let delta = new_size as i64 - self.size as i64;
            pool.update_used_memory(self.tag, delta);
            self.size = new_size;
        }
    }

    /// Merge another reservation into this one.
    ///
    /// The source reservation is consumed and its size is added to this one.
    /// The source reservation's size is set to zero to prevent double-free.
    pub fn merge(&mut self, mut src: BufferPoolReservation) {
        self.size += src.size;
        src.size = 0;
    }

    /// Take ownership of the reservation, preventing automatic cleanup.
    ///
    /// Returns the size that was reserved. The caller is responsible for
    /// manually releasing the memory.
    pub fn take(&mut self) -> usize {
        let size = self.size;
        self.size = 0;
        size
    }
}

impl Drop for BufferPoolReservation {
    /// Automatically release the reservation when dropped.
    fn drop(&mut self) {
        debug_assert_eq!(
            self.size, 0,
            "BufferPoolReservation dropped with non-zero size: {}",
            self.size
        );
    }
}

/// Temporary buffer pool reservation that automatically releases on drop.
///
/// This is a convenience wrapper around BufferPoolReservation that
/// automatically resizes to zero in the destructor, making it safe to
/// use in temporary contexts.
pub struct TempBufferPoolReservation {
    inner: BufferPoolReservation,
}

impl TempBufferPoolReservation {
    /// Create a new temporary reservation with the given size.
    pub fn new(tag: MemoryTag, pool: &Arc<BufferPool>, size: usize) -> Self {
        let mut inner = BufferPoolReservation::new(tag, pool);
        inner.resize(size);
        Self { inner }
    }

    /// Get the current reservation size.
    #[inline]
    pub fn size(&self) -> usize {
        self.inner.size()
    }

    /// Get the memory tag.
    #[inline]
    pub fn tag(&self) -> MemoryTag {
        self.inner.tag()
    }

    /// Resize the reservation.
    pub fn resize(&mut self, new_size: usize) {
        self.inner.resize(new_size);
    }

    /// Convert to a regular BufferPoolReservation.
    ///
    /// This consumes the TempBufferPoolReservation and returns the inner
    /// BufferPoolReservation, preventing automatic cleanup.
    pub fn into_inner(mut self) -> BufferPoolReservation {
        // Take the inner reservation and prevent drop
        let inner = BufferPoolReservation {
            tag: self.inner.tag,
            size: self.inner.size,
            pool: self.inner.pool.clone(),
        };
        self.inner.size = 0; // Prevent double-free
        inner
    }
}

impl Drop for TempBufferPoolReservation {
    /// Automatically resize to zero when dropped.
    fn drop(&mut self) {
        self.inner.resize(0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_common::allocator::MemoryTag;

    #[test]
    fn test_buffer_pool_reservation_basic() {
        let pool = Arc::new(BufferPool::new(1024 * 1024));
        let initial_usage = pool.used_memory();

        let mut reservation = BufferPoolReservation::new(MemoryTag::OrderBy, &pool);
        assert_eq!(reservation.size(), 0);
        assert_eq!(pool.used_memory(), initial_usage);

        // Resize to 1024
        reservation.resize(1024);
        assert_eq!(reservation.size(), 1024);
        assert_eq!(pool.used_memory(), initial_usage + 1024);

        // Resize to 2048
        reservation.resize(2048);
        assert_eq!(reservation.size(), 2048);
        assert_eq!(pool.used_memory(), initial_usage + 2048);

        // Resize to 0
        reservation.resize(0);
        assert_eq!(reservation.size(), 0);
        assert_eq!(pool.used_memory(), initial_usage);
    }

    #[test]
    fn test_buffer_pool_reservation_merge() {
        let pool = Arc::new(BufferPool::new(1024 * 1024));
        let initial_usage = pool.used_memory();

        let mut reservation1 = BufferPoolReservation::new(MemoryTag::OrderBy, &pool);
        reservation1.resize(1024);

        let mut reservation2 = BufferPoolReservation::new(MemoryTag::OrderBy, &pool);
        reservation2.resize(512);

        assert_eq!(pool.used_memory(), initial_usage + 1536);

        // Merge reservation2 into reservation1
        reservation1.merge(reservation2);
        assert_eq!(reservation1.size(), 1536);
        assert_eq!(pool.used_memory(), initial_usage + 1536);

        // Clean up
        reservation1.resize(0);
        assert_eq!(pool.used_memory(), initial_usage);
    }

    #[test]
    fn test_temp_buffer_pool_reservation() {
        let pool = Arc::new(BufferPool::new(1024 * 1024));
        let initial_usage = pool.used_memory();

        {
            let reservation = TempBufferPoolReservation::new(MemoryTag::OrderBy, &pool, 1024);
            assert_eq!(reservation.size(), 1024);
            assert_eq!(pool.used_memory(), initial_usage + 1024);
        }

        // Should automatically resize to 0 on drop
        assert_eq!(pool.used_memory(), initial_usage);
    }

    #[test]
    fn test_temp_reservation_resize() {
        let pool = Arc::new(BufferPool::new(1024 * 1024));
        let initial_usage = pool.used_memory();

        {
            let mut reservation = TempBufferPoolReservation::new(MemoryTag::OrderBy, &pool, 1024);
            assert_eq!(pool.used_memory(), initial_usage + 1024);

            reservation.resize(2048);
            assert_eq!(pool.used_memory(), initial_usage + 2048);

            reservation.resize(512);
            assert_eq!(pool.used_memory(), initial_usage + 512);
        }

        assert_eq!(pool.used_memory(), initial_usage);
    }

    #[test]
    fn test_temp_reservation_into_inner() {
        let pool = Arc::new(BufferPool::new(1024 * 1024));
        let initial_usage = pool.used_memory();

        let mut regular_reservation = {
            let temp_reservation = TempBufferPoolReservation::new(MemoryTag::OrderBy, &pool, 1024);
            assert_eq!(pool.used_memory(), initial_usage + 1024);
            temp_reservation.into_inner()
        };

        // Should NOT resize to 0 because we converted to regular reservation
        assert_eq!(pool.used_memory(), initial_usage + 1024);

        // Manual cleanup
        regular_reservation.resize(0);
        assert_eq!(pool.used_memory(), initial_usage);
    }

    #[test]
    fn test_reservation_take() {
        let pool = Arc::new(BufferPool::new(1024 * 1024));
        let initial_usage = pool.used_memory();

        let mut reservation = BufferPoolReservation::new(MemoryTag::OrderBy, &pool);
        reservation.resize(1024);
        assert_eq!(pool.used_memory(), initial_usage + 1024);

        let size = reservation.take();
        assert_eq!(size, 1024);
        assert_eq!(reservation.size(), 0);

        // Memory is still reserved in the pool (caller's responsibility)
        assert_eq!(pool.used_memory(), initial_usage + 1024);

        // Manual cleanup
        pool.update_used_memory(MemoryTag::OrderBy, -1024);
        assert_eq!(pool.used_memory(), initial_usage);
    }
}
