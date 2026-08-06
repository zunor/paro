// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # Buffer Pool Management
//!
//! This module provides buffer pool management for Paro's storage engine.
//!
//! - Pin/Unpin reference counting for buffer management
//! - BufferHandle RAII wrapper for automatic unpinning
//! - BlockHandle for tracking individual blocks
//! - Memory limit enforcement with LRU eviction

mod block_handle;
mod buffer_handle;
mod buffer_manager;
mod buffer_pool;
mod buffer_pool_reservation;
mod eviction_queue;
mod file_buffer_type;
mod page_cache;
mod prefetch;
mod standard_buffer_manager;
mod temporary_file_manager;
mod write_buffer_reserve;

pub use block_handle::{BlockHandle, BlockId, SharedBlockHandle};
pub use buffer_handle::BufferHandle;
pub use buffer_manager::{BufferManager, SharedBufferManager};
pub use buffer_pool::{BufferPool, BufferPoolStats, EvictionResult};
pub use buffer_pool_reservation::{BufferPoolReservation, TempBufferPoolReservation};
pub use file_buffer_type::FileBufferType;
pub use page_cache::{
    PageCache, PageCacheHandle, PageCacheOptions, PageCacheStatsSnapshot, PageContentKind, PageKey,
};
pub use paro_common::allocator::MemoryTag;
pub use prefetch::{PrefetchBudget, PrefetchItem, PrefetchOptions, Prefetcher};
pub use standard_buffer_manager::StandardBufferManager;
pub use temporary_file_manager::{
    TemporaryBufferSize, TemporaryFileIdentifier, TemporaryFileIndex, TemporaryFileInfo,
    TemporaryFileManager, TemporarySpillMetricsSnapshot,
};
pub use write_buffer_reserve::{
    FixedWriteBufferReserve, WriteBufferReservation, WriteBufferReserve,
};

/// Default block allocation size (256 KB).
pub const DEFAULT_BLOCK_ALLOC_SIZE: usize = 262144;

/// Default block size (block allocation size minus header).
pub const DEFAULT_BLOCK_SIZE: usize = DEFAULT_BLOCK_ALLOC_SIZE - 8;
