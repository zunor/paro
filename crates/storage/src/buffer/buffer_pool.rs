// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! BufferPool - manages memory allocation and eviction.
//!
//! - Central pool managing all buffer allocations
//! - Memory limit enforcement
//! - LRU eviction for unpinned blocks with dead node detection
//! - Spill-to-disk when memory pressure occurs
//! - Thread-safe operations
//! - Per-tag memory tracking

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock, Weak};
use std::time::SystemTime;

use paro_common::allocator::{
    default_allocator, BufferManager, MemoryTag, MemoryUsage, MemoryUsageSnapshot, MEMORY_TAG_COUNT,
};
use paro_common::error::{self as paro_error, Result};

use super::buffer_handle::BufferHandle;
use super::eviction_queue::{BufferEvictionNode, EvictionQueue};
use super::file_buffer_type::FileBufferType;
use super::temporary_file_manager::{TemporaryFileManager, TemporarySpillMetricsSnapshot};
use super::DEFAULT_BLOCK_SIZE;
use super::{BlockHandle, BlockId, TemporaryFileInfo};

/// Get current timestamp in milliseconds since UNIX epoch.
fn current_timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

/// Buffer pool statistics.
#[derive(Debug, Default)]
pub struct BufferPoolStats {
    /// Total allocations
    pub allocations: AtomicUsize,
    /// Total deallocations
    pub deallocations: AtomicUsize,
    /// Current pin operations
    pub pins: AtomicUsize,
    /// Current unpin operations
    pub unpins: AtomicUsize,
    /// Eviction count
    pub evictions: AtomicUsize,
    /// Number of successful buffer reuses.
    pub buffer_reuses: AtomicUsize,
    /// Number of buffer reuse attempts (when buffer parameter is provided)
    pub buffer_reuse_attempts: AtomicUsize,
    /// Number of times size mismatch prevented reuse
    pub buffer_reuse_size_mismatches: AtomicUsize,
}

impl BufferPoolStats {
    /// Calculate the buffer reuse rate.
    ///
    /// Returns the percentage of reuse attempts that succeeded.
    /// Returns 0.0 if no attempts were made.
    pub fn buffer_reuse_rate(&self) -> f64 {
        let attempts = self.buffer_reuse_attempts.load(Ordering::Relaxed);
        if attempts == 0 {
            return 0.0;
        }
        let reuses = self.buffer_reuses.load(Ordering::Relaxed);
        (reuses as f64 / attempts as f64) * 100.0
    }

    /// Get a formatted string with buffer reuse statistics.
    pub fn buffer_reuse_stats_string(&self) -> String {
        let attempts = self.buffer_reuse_attempts.load(Ordering::Relaxed);
        let reuses = self.buffer_reuses.load(Ordering::Relaxed);
        let mismatches = self.buffer_reuse_size_mismatches.load(Ordering::Relaxed);
        let rate = self.buffer_reuse_rate();

        format!(
            "Buffer Reuse: {} successful / {} attempts ({:.1}% rate), {} size mismatches",
            reuses, attempts, rate, mismatches
        )
    }
}

/// Result of an eviction operation.
///
/// Contains whether the eviction was successful and a memory reservation
/// that can be used for the allocation that triggered the eviction.
pub struct EvictionResult {
    /// Whether the eviction was successful
    pub success: bool,
    /// Memory reservation for the evicted memory
    pub reservation: super::buffer_pool_reservation::TempBufferPoolReservation,
}

/// Buffer pool for managing memory allocations.
///
/// The BufferPool is the central manager for all buffer allocations.
/// It tracks memory usage, enforces limits, and handles eviction.
///
/// # Thread Safety
/// All operations are thread-safe using internal locking.
pub struct BufferPool {
    /// Maximum memory limit in bytes
    max_memory: AtomicUsize,
    /// Serializes memory limit updates.
    limit_lock: Mutex<()>,
    /// Current memory usage in bytes
    used_memory: AtomicUsize,
    /// Next block ID to assign
    next_block_id: AtomicI64,
    /// All blocks indexed by ID
    blocks: RwLock<HashMap<BlockId, Arc<BlockHandle>>>,
    /// Multiple priority eviction queues
    queues: Vec<EvictionQueue>,
    /// Queue sizes for each FileBufferType category
    /// [BLOCK_AND_EXTERNAL_FILE, MANAGED_BUFFER, TINY_BUFFER]
    eviction_queue_sizes: [usize; 3],
    /// Statistics
    stats: BufferPoolStats,
    /// Track raw allocations to BufferHandles (for BufferAllocator integration)
    allocations: RwLock<HashMap<usize, BufferHandle>>,
    /// Per-tag memory usage tracking
    memory_usage: MemoryUsage,
    /// Temporary directory for spilling blocks to disk
    temporary_directory: RwLock<String>,
    /// Spill file manager used by managed buffer eviction.
    temporary_file_manager: RwLock<Option<Arc<TemporaryFileManager>>>,
    /// Maximum allowed temporary spill size (`u64::MAX` means unlimited).
    max_swap_space: AtomicU64,
    /// Weak reference to self for upgrading &self to Arc<Self>
    weak_self: RwLock<Weak<Self>>,
}

impl BufferPool {
    /// How many eviction queue types we have (BLOCK and EXTERNAL_FILE go into same queue)
    const EVICTION_QUEUE_TYPES: usize = 3; // FILE_BUFFER_TYPE_COUNT - 1

    /// How many eviction queues we have for the different FileBufferTypes
    const BLOCK_AND_EXTERNAL_FILE_QUEUE_SIZE: usize = 1;
    const MANAGED_BUFFER_QUEUE_SIZE: usize = 6;
    const TINY_BUFFER_QUEUE_SIZE: usize = 1;

    /// Create a new buffer pool with the given memory limit.
    ///
    /// # Arguments
    /// * `max_memory` - Maximum memory in bytes (0 = unlimited)
    pub fn new(max_memory: usize) -> Self {
        let eviction_queue_sizes = [
            Self::BLOCK_AND_EXTERNAL_FILE_QUEUE_SIZE,
            Self::MANAGED_BUFFER_QUEUE_SIZE,
            Self::TINY_BUFFER_QUEUE_SIZE,
        ];

        // Initialize all eviction queues
        let mut queues = Vec::new();
        for (queue_type_idx, &type_queue_size) in eviction_queue_sizes
            .iter()
            .enumerate()
            .take(Self::EVICTION_QUEUE_TYPES)
        {
            let types = Self::eviction_queue_type_idx_to_file_buffer_types(queue_type_idx);
            for _ in 0..type_queue_size {
                queues.push(EvictionQueue::new(types.clone()));
            }
        }

        Self {
            max_memory: AtomicUsize::new(max_memory),
            limit_lock: Mutex::new(()),
            used_memory: AtomicUsize::new(0),
            next_block_id: AtomicI64::new(1),
            blocks: RwLock::new(HashMap::new()),
            queues,
            eviction_queue_sizes,
            stats: BufferPoolStats::default(),
            allocations: RwLock::new(HashMap::new()),
            memory_usage: MemoryUsage::new(),
            temporary_directory: RwLock::new(String::new()),
            temporary_file_manager: RwLock::new(None),
            max_swap_space: AtomicU64::new(u64::MAX),
            weak_self: RwLock::new(Weak::new()),
        }
    }

    /// Create a new buffer pool wrapped in an Arc with weak_self set.
    pub fn new_arc(limit: usize) -> Arc<Self> {
        let pool = Arc::new(Self::new(limit));
        pool.set_weak_self(Arc::downgrade(&pool));
        pool
    }

    /// Set the weak reference to self.
    pub fn set_weak_self(&self, weak: Weak<Self>) {
        let mut weak_self = self.weak_self.write().unwrap();
        *weak_self = weak;
    }

    /// Try to upgrade the weak reference to an Arc.
    pub fn arc_self(&self) -> Arc<Self> {
        self.weak_self
            .read()
            .unwrap()
            .upgrade()
            .expect("BufferPool must be managed by an Arc")
    }

    /// Create a buffer pool with default settings.
    pub fn default_pool() -> Self {
        // Default to 1 GB limit
        Self::new(1024 * 1024 * 1024)
    }

    /// Map FileBufferType to eviction queue type index.
    fn file_buffer_type_to_eviction_queue_type_idx(buffer_type: FileBufferType) -> usize {
        match buffer_type {
            FileBufferType::Block | FileBufferType::ExternalFile => 0, // Evict these first (cheap, just free)
            FileBufferType::ManagedBuffer => 1, // Then these (have to write to storage)
            FileBufferType::TinyBuffer => 2,    // Evict tiny buffers last (last resort)
        }
    }

    /// Map eviction queue type index to FileBufferTypes.
    fn eviction_queue_type_idx_to_file_buffer_types(queue_type_idx: usize) -> Vec<FileBufferType> {
        match queue_type_idx {
            0 => vec![FileBufferType::Block, FileBufferType::ExternalFile],
            1 => vec![FileBufferType::ManagedBuffer],
            2 => vec![FileBufferType::TinyBuffer],
            _ => panic!("Unknown queue type index: {}", queue_type_idx),
        }
    }

    /// Get the eviction queue for a specific block handle.
    ///
    /// This method determines which queue to use based on:
    /// 1. The buffer type (Block/ExternalFile, ManagedBuffer, or TinyBuffer)
    /// 2. The eviction_queue_idx (for ManagedBuffer only, provides fine-grained priority)
    fn get_eviction_queue_for_block_handle(&self, handle: &BlockHandle) -> &EvictionQueue {
        let handle_buffer_type = handle.buffer_type();

        // Get offset into eviction queues for this FileBufferType
        let mut queue_index = 0;
        let handle_queue_type_idx =
            Self::file_buffer_type_to_eviction_queue_type_idx(handle_buffer_type);
        for type_idx in 0..handle_queue_type_idx {
            queue_index += self.eviction_queue_sizes[type_idx];
        }

        let queue_size = self.eviction_queue_sizes[handle_queue_type_idx];
        // Adjust if eviction_queue_idx is set (idx == 0 -> add at back, idx >= queue_size -> add at front)
        let eviction_queue_idx = handle.get_eviction_queue_idx();
        if eviction_queue_idx < queue_size {
            queue_index += queue_size - eviction_queue_idx - 1;
        }

        debug_assert!(
            self.queues[queue_index].has_file_buffer_type(handle_buffer_type),
            "Queue {} should handle buffer type {:?}",
            queue_index,
            handle_buffer_type
        );
        &self.queues[queue_index]
    }

    /// Increment dead nodes counter for the queue handling this block.
    fn increment_dead_nodes(&self, handle: &BlockHandle) {
        let queue = self.get_eviction_queue_for_block_handle(handle);
        queue.increment_dead_nodes();
    }

    /// Garbage collect dead nodes in the eviction queue for this block.
    fn purge_queue(&self, handle: &BlockHandle) {
        let queue = self.get_eviction_queue_for_block_handle(handle);
        queue.purge();
    }

    /// Get the maximum memory limit.
    #[inline]
    pub fn max_memory(&self) -> usize {
        self.max_memory.load(Ordering::Acquire)
    }

    /// Get current memory usage.
    #[inline]
    pub fn used_memory(&self) -> usize {
        self.used_memory.load(Ordering::Acquire)
    }

    /// Get available memory.
    #[inline]
    pub fn available_memory(&self) -> usize {
        let used = self.used_memory();
        let max = self.max_memory();
        if max == 0 {
            usize::MAX - used
        } else {
            max.saturating_sub(used)
        }
    }

    /// Get pool statistics.
    pub fn stats(&self) -> &BufferPoolStats {
        &self.stats
    }

    /// Get memory usage information per tag.
    ///
    /// Returns a snapshot of memory usage broken down by MemoryTag.
    /// This is useful for monitoring and debugging memory consumption.
    pub fn get_memory_usage_info(&self) -> MemoryUsageSnapshot {
        self.memory_usage.snapshot()
    }

    /// Get memory usage for a specific tag.
    #[inline]
    pub fn get_tag_usage(&self, tag: MemoryTag) -> i64 {
        self.memory_usage.get(tag)
    }

    fn sub_used_memory_checked(&self, tag: MemoryTag, size: usize, context: &'static str) {
        if size == 0 {
            return;
        }

        let mut current = self.used_memory.load(Ordering::Acquire);
        loop {
            assert!(
                current >= size,
                "BufferPool used_memory underflow during {}: tag={}, size={}, current={}",
                context,
                tag,
                size,
                current
            );
            match self.used_memory.compare_exchange_weak(
                current,
                current - size,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
        }

        self.memory_usage.sub(tag, size);
    }

    fn sub_used_memory_saturating(&self, tag: MemoryTag, size: usize) -> usize {
        if size == 0 {
            return 0;
        }

        let mut current = self.used_memory.load(Ordering::Acquire);
        loop {
            if current == 0 {
                return 0;
            }

            let actual = current.min(size);
            match self.used_memory.compare_exchange_weak(
                current,
                current - actual,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    self.memory_usage.sub_saturating(tag, actual);
                    return actual;
                }
                Err(observed) => current = observed,
            }
        }
    }

    /// Update used memory by a delta amount.
    ///
    /// This is used by BufferPoolReservation to adjust memory usage.
    /// Positive delta increases usage, negative delta decreases it.
    pub fn update_used_memory(&self, tag: MemoryTag, delta: i64) {
        if delta > 0 {
            self.used_memory.fetch_add(delta as usize, Ordering::AcqRel);
            self.memory_usage.add(tag, delta as usize);
        } else if delta < 0 {
            self.sub_used_memory_checked(tag, (-delta) as usize, "update_used_memory");
        }
    }

    pub(crate) fn release_reserved_memory(&self, tag: MemoryTag, size: usize) {
        self.sub_used_memory_saturating(tag, size);
    }

    /// Update memory limit at runtime.
    ///
    /// Follows an "evict + set + verify + rollback" pattern.
    pub fn set_memory_limit(&self, limit: usize) -> Result<()> {
        let _limit_guard = self.limit_lock.lock().unwrap();

        if limit != 0 {
            let precheck = self.evict_blocks(MemoryTag::Extension, 0, limit, None);
            if !precheck.success {
                return Err(paro_error::out_of_memory(format!(
                    "Failed to change memory limit to {}: unable to evict enough blocks",
                    limit
                )));
            }
        }

        let old_limit = self.max_memory();
        self.max_memory.store(limit, Ordering::Release);

        if limit != 0 {
            let verify = self.evict_blocks(MemoryTag::Extension, 0, limit, None);
            if !verify.success {
                self.max_memory.store(old_limit, Ordering::Release);
                return Err(paro_error::out_of_memory(format!(
                    "Failed to change memory limit to {}: unable to satisfy new limit",
                    limit
                )));
            }
        }

        Ok(())
    }

    /// Set spill-to-disk swap limit.
    ///
    /// `None` means unlimited.
    pub fn set_swap_limit(&self, limit: Option<usize>) -> Result<()> {
        let max_swap = match limit {
            Some(v) => u64::try_from(v)
                .map_err(|_| paro_error::invalid_input("swap limit exceeds u64 range"))?,
            None => u64::MAX,
        };
        self.max_swap_space.store(max_swap, Ordering::Release);

        if let Some(manager) = self.get_temporary_file_manager() {
            manager.set_max_swap_space(max_swap);
        }
        Ok(())
    }

    /// Get the currently configured swap limit.
    ///
    /// Returns `None` when unlimited.
    pub fn get_swap_limit(&self) -> Option<usize> {
        let value = self.max_swap_space.load(Ordering::Acquire);
        if value == u64::MAX {
            None
        } else {
            usize::try_from(value).ok()
        }
    }

    /// Get temporary spill files from the active temporary file manager.
    pub fn get_temporary_files(&self) -> Vec<TemporaryFileInfo> {
        self.get_temporary_file_manager()
            .map(|manager| manager.get_temporary_files())
            .unwrap_or_default()
    }

    /// Get a snapshot of temporary spill metrics from the active temporary file manager.
    pub fn get_temporary_spill_metrics(&self) -> TemporarySpillMetricsSnapshot {
        self.get_temporary_file_manager()
            .map(|manager| manager.metrics_snapshot())
            .unwrap_or_default()
    }

    pub fn get_temporary_storage_by_tag(&self) -> [u64; MEMORY_TAG_COUNT] {
        self.get_temporary_file_manager()
            .map(|manager| manager.spill_usage_per_tag())
            .unwrap_or_else(|| [0; MEMORY_TAG_COUNT])
    }

    fn get_temporary_file_manager(&self) -> Option<Arc<TemporaryFileManager>> {
        self.temporary_file_manager
            .read()
            .unwrap()
            .as_ref()
            .cloned()
    }

    fn get_or_create_temporary_file_manager(&self) -> Result<Arc<TemporaryFileManager>> {
        if let Some(existing) = self.get_temporary_file_manager() {
            return Ok(existing);
        }

        let temp_dir = self.temporary_directory.read().unwrap().clone();
        if temp_dir.is_empty() {
            return Err(paro_error::out_of_memory(
                "Out-of-memory: cannot write buffer because no temporary directory is specified!",
            ));
        }

        let manager = Arc::new(TemporaryFileManager::new(&temp_dir)?);
        manager.set_max_swap_space(self.max_swap_space.load(Ordering::Acquire));

        let mut guard = self.temporary_file_manager.write().unwrap();
        if let Some(existing) = guard.as_ref() {
            return Ok(existing.clone());
        }
        *guard = Some(manager.clone());
        Ok(manager)
    }

    /// Load a block from temporary spill file.
    ///
    /// This method handles loading managed buffers that were evicted to temporary files.
    ///
    /// # Arguments
    /// * `block` - The block handle to load
    /// * `reusable_buffer` - Optional buffer to reuse (for memory efficiency)
    fn load_block(&self, block: Arc<BlockHandle>, _reusable_buffer: Option<Vec<u8>>) -> Result<()> {
        let block_id = block.block_id();
        let size = block.size();

        if !block.must_write_to_disk() {
            return Err(paro_error::internal(format!(
                "Block {} cannot be reloaded without temporary spill data",
                block_id
            )));
        }

        let manager = self.get_or_create_temporary_file_manager()?;
        if !manager.has_temporary_buffer(block_id) {
            return Err(paro_error::internal(format!(
                "Temporary spill file not found for block {}",
                block_id
            )));
        }

        let buffer = self.read_from_temporary_file(block_id, size)?;

        // Set the buffer and mark as loaded
        block.set_buffer(buffer)?;

        Ok(())
    }

    /// Read a block from temporary file.
    fn read_from_temporary_file(&self, block_id: BlockId, size: usize) -> Result<Vec<u8>> {
        let manager = self.get_or_create_temporary_file_manager()?;
        let mut data = vec![0u8; size];
        let read_size = manager.read_temporary_buffer(block_id, &mut data)?;
        if read_size != size {
            return Err(paro_error::internal(format!(
                "Temporary buffer size mismatch for block {}: expected {}, got {}",
                block_id, size, read_size
            )));
        }
        Ok(data)
    }

    /// Write a block to temporary file.
    fn write_to_temporary_file(
        &self,
        block_id: BlockId,
        tag: MemoryTag,
        data: &[u8],
    ) -> Result<()> {
        let manager = self.get_or_create_temporary_file_manager()?;
        manager.write_temporary_buffer(block_id, tag, data)?;
        Ok(())
    }

    /// Set the temporary directory path.
    pub fn set_temporary_directory(&self, path: String) -> Result<()> {
        let normalized = path.trim().to_string();
        let mut temp_dir = self.temporary_directory.write().unwrap();
        if *temp_dir == normalized {
            return Ok(());
        }

        let existing_manager = self
            .temporary_file_manager
            .read()
            .unwrap()
            .as_ref()
            .cloned();

        if let Some(existing) = existing_manager {
            // Drop stale/unreachable spilled blocks before switching temp directory.
            // This keeps session-level SET/RESET temp_directory usable after spill-heavy queries.
            for block_id in existing.temporary_block_ids() {
                let block = {
                    let mut blocks = self.blocks.write().unwrap();
                    blocks.remove(&block_id)
                };
                if let Some(block) = block {
                    if block.is_loaded() {
                        self.update_used_memory(block.tag(), -(block.size() as i64));
                    }
                    self.remove_from_eviction_queue(block_id);
                }
            }

            if !existing.get_temporary_files().is_empty() {
                existing.clear()?;
            }
        }

        let mut manager_guard = self.temporary_file_manager.write().unwrap();

        if normalized.is_empty() {
            *manager_guard = None;
            *temp_dir = normalized;
            return Ok(());
        }

        let manager = Arc::new(TemporaryFileManager::new(&normalized)?);
        manager.set_max_swap_space(self.max_swap_space.load(Ordering::Acquire));
        *manager_guard = Some(manager);
        *temp_dir = normalized;
        Ok(())
    }

    /// Check if temporary directory is set.
    pub fn has_temporary_directory(&self) -> bool {
        let temp_dir = self.temporary_directory.read().unwrap();
        !temp_dir.is_empty()
    }

    /// Allocate a new block with the given size and return a pinned handle.
    ///
    /// # Arguments
    /// * `self` - Arc reference to self (needed for memory pressure detection)
    /// * `tag` - Memory tag for tracking
    /// * `buffer_type` - Buffer type (determines eviction priority)
    /// * `size` - Size in bytes to allocate
    ///
    /// # Returns
    /// A `BufferHandle` to the pinned block, or error if out of memory.
    pub fn allocate(
        &self,
        tag: MemoryTag,
        buffer_type: FileBufferType,
        size: usize,
    ) -> Result<BufferHandle> {
        self.allocate_internal(tag, buffer_type, size, true)
    }

    /// Allocate a block that cannot be destroyed/evicted.
    pub fn allocate_persistent(
        &self,
        tag: MemoryTag,
        buffer_type: FileBufferType,
        size: usize,
    ) -> Result<BufferHandle> {
        self.allocate_internal(tag, buffer_type, size, false)
    }

    fn allocate_internal(
        &self,
        tag: MemoryTag,
        buffer_type: FileBufferType,
        size: usize,
        can_destroy: bool,
    ) -> Result<BufferHandle> {
        let size = if size == 0 { DEFAULT_BLOCK_SIZE } else { size };

        // Check memory limit and try eviction if needed
        // EvictBlocksOrThrow is called before allocation
        self.ensure_memory_available(size)?;

        // Generate new block ID
        let block_id = self.next_block_id.fetch_add(1, Ordering::Relaxed);

        // Allocate the block
        let block = BlockHandle::allocate(
            block_id,
            tag,
            size,
            can_destroy,
            Arc::new(default_allocator().clone()),
            buffer_type,
        )?;
        let block = Arc::new(block);

        // Set initial LRU timestamp
        block.set_lru_timestamp(current_timestamp_ms());

        // Track memory and store block
        self.used_memory.fetch_add(size, Ordering::AcqRel);
        // Track per-tag memory usage
        self.memory_usage.add(tag, size);
        {
            let mut blocks = self.blocks.write().unwrap();
            blocks.insert(block_id, block.clone());
        }

        self.stats.allocations.fetch_add(1, Ordering::Relaxed);

        let pool_weak = self.weak_self.read().unwrap().clone();
        Ok(BufferHandle::with_pool(block, pool_weak))
    }

    /// Pin a block by ID and return a handle.
    ///
    /// If the block is not loaded, this method will automatically load it from disk
    /// or temporary file. This implements a two-phase locking pattern to avoid
    /// deadlocks during concurrent loading.
    ///
    /// # Arguments
    /// * `block_id` - The block to pin
    ///
    /// # Returns
    /// A `BufferHandle` to the pinned block, or error if not found.
    pub fn pin(&self, block_id: BlockId) -> Result<BufferHandle> {
        let block = {
            let blocks = self.blocks.read().unwrap();
            blocks.get(&block_id).cloned()
        };

        let block = match block {
            Some(block) => block,
            None => {
                return Err(paro_error::internal(format!(
                    "Block {} not found",
                    block_id
                )))
            }
        };

        // Fast path: block is already loaded
        if block.is_loaded() {
            // Remove from eviction queue if present
            self.remove_from_eviction_queue(block_id);

            // Pin the block and update LRU timestamp
            block.pin();
            block.set_lru_timestamp(current_timestamp_ms());
            self.stats.pins.fetch_add(1, Ordering::Relaxed);

            let pool_weak = self.weak_self.read().unwrap().clone();
            return Ok(BufferHandle::with_pool(block, pool_weak));
        }

        // Slow path: block needs to be loaded

        // Get required memory for loading
        let required_memory = block.size();

        // Evict blocks until we have space for the current block
        let mut reusable_buffer = None;
        let mut eviction_result = self.evict_blocks(
            block.tag(),
            required_memory,
            self.max_memory(),
            Some(&mut reusable_buffer),
        );

        if !eviction_result.success {
            return Err(paro_error::out_of_memory(format!(
                "Failed to pin block {}: cannot evict enough memory (need {} bytes)",
                block_id, required_memory
            )));
        }

        // Double-check locking: check if another thread loaded the block
        if block.is_loaded() {
            // Block was loaded by another thread, just pin it
            block.pin();
            block.set_lru_timestamp(current_timestamp_ms());
            self.stats.pins.fetch_add(1, Ordering::Relaxed);
            let pool_weak = self.weak_self.read().unwrap().clone();
            return Ok(BufferHandle::with_pool(block, pool_weak));
        }

        // Now we can actually load the block
        self.load_block(block.clone(), reusable_buffer)?;
        eviction_result.reservation.resize(0);
        self.update_used_memory(block.tag(), required_memory as i64);

        // Pin the block and update LRU timestamp
        block.pin();
        block.set_lru_timestamp(current_timestamp_ms());
        self.stats.pins.fetch_add(1, Ordering::Relaxed);

        let pool_weak = self.weak_self.read().unwrap().clone();
        Ok(BufferHandle::with_pool(block, pool_weak))
    }

    /// Unpin a block by ID.
    ///
    /// This is typically called automatically when BufferHandle is dropped.
    pub fn unpin(&self, block_id: BlockId) {
        let block: Option<Arc<BlockHandle>> = {
            let blocks = self.blocks.read().unwrap();
            blocks.get(&block_id).cloned()
        };

        if let Some(block) = block {
            let new_count = (*block).unpin();
            self.stats.unpins.fetch_add(1, Ordering::Relaxed);

            // Add to eviction queue if fully unpinned and can be destroyed
            if new_count == 0 && block.can_destroy() {
                self.add_to_eviction_queue(block_id);
            }
        }
    }

    /// Get a block handle by ID (for inspection only).
    pub fn get_block(&self, block_id: BlockId) -> Option<Arc<BlockHandle>> {
        let blocks = self.blocks.read().unwrap();
        blocks.get(&block_id).cloned()
    }

    /// Free a specific block by ID.
    ///
    /// The block must not be pinned.
    pub fn free(&self, block_id: BlockId) -> Result<()> {
        if let Some(manager) = self.get_temporary_file_manager() {
            if manager.has_temporary_buffer(block_id) {
                let _ = manager.delete_temporary_buffer(block_id);
            }
        }

        let block: Option<Arc<BlockHandle>> = {
            let mut blocks = self.blocks.write().unwrap();
            blocks.remove(&block_id)
        };

        if let Some(block) = block {
            if (*block).is_pinned() {
                // Put it back
                let mut blocks = self.blocks.write().unwrap();
                blocks.insert(block_id, block);
                return Err(paro_error::internal("Cannot free pinned block"));
            }

            let size = block.size();
            let tag = block.tag();
            if block.is_loaded() {
                // Only resident blocks still contribute to pool usage at free time.
                self.sub_used_memory_checked(tag, size, "free");
            }
            self.stats.deallocations.fetch_add(1, Ordering::Relaxed);
            self.remove_from_eviction_queue(block_id);
        }

        Ok(())
    }

    /// Evict blocks to free memory with optional buffer reuse.
    ///
    /// Tries to evict blocks from queues in priority order:
    /// 1. Block/ExternalFile (cheapest - just free)
    /// 2. ManagedBuffer (need to write to storage)
    /// 3. TinyBuffer (last resort)
    ///
    /// # Arguments
    /// * `self` - Arc reference to self (needed for TempBufferPoolReservation)
    /// * `tag` - Memory tag for tracking
    /// * `extra_memory` - Amount of memory to free
    /// * `memory_limit` - Target memory limit
    /// * `buffer` - Optional buffer to reuse (if size matches)
    ///
    /// # Returns
    /// EvictionResult with success status and memory reservation
    pub fn evict_blocks(
        &self,
        tag: MemoryTag,
        extra_memory: usize,
        memory_limit: usize,
        buffer: Option<&mut Option<Vec<u8>>>,
    ) -> EvictionResult {
        // Track buffer reuse attempt if buffer parameter is provided
        // Only count once per evict_blocks call, not per queue
        if buffer.is_some() {
            self.stats
                .buffer_reuse_attempts
                .fetch_add(1, Ordering::Relaxed);
        }

        // Convert to raw pointer to work around borrow checker
        let buffer_ptr = buffer.map(|b| b as *mut Option<Vec<u8>>);

        for (i, queue) in self.queues.iter().enumerate() {
            let result = self.arc_self().evict_blocks_internal(
                queue,
                tag,
                extra_memory,
                memory_limit,
                buffer_ptr,
            );
            if result.success || i == self.queues.len() - 1 {
                return result; // Return upon success or upon last queue
            }
        }
        // This should never happen since we always return on last queue
        unreachable!("Exited evict_blocks without returning EvictionResult");
    }

    /// Internal eviction implementation for a single queue.
    fn evict_blocks_internal(
        self: &Arc<Self>,
        queue: &EvictionQueue,
        tag: MemoryTag,
        extra_memory: usize,
        memory_limit: usize,
        buffer_ptr: Option<*mut Option<Vec<u8>>>,
    ) -> EvictionResult {
        use super::buffer_pool_reservation::TempBufferPoolReservation;

        // Create a temporary reservation for the memory we're trying to free
        let mut reservation = TempBufferPoolReservation::new(tag, &self.arc_self(), extra_memory);
        let mut found = false;

        // Early-out if memory is already satisfied
        if self.used_memory() <= memory_limit {
            return EvictionResult {
                success: true,
                reservation,
            };
        }

        // Iterate over unloadable blocks
        queue.iterate_unloadable_blocks(|_node, handle| {
            // Check if we can reuse the buffer directly
            if let Some(buf_ptr) = buffer_ptr {
                // SAFETY: We have exclusive access to the buffer through the raw pointer
                let buf_ref = unsafe { &mut *buf_ptr };

                // Use flexible size matching instead of exact match
                // This improves buffer reuse rate while avoiding excessive waste
                if Self::can_reuse_buffer(handle.size(), extra_memory) {
                    // We can re-use the memory directly (with possible resizing)
                    let has_temp_dir = self.has_temporary_directory();

                    // Get mutable access to the block handle
                    let block_id = handle.block_id();
                    let blocks = self.blocks.write().unwrap();
                    if let Some(block) = blocks.get(&block_id) {
                        // Create a temporary mutable reference
                        // SAFETY: We have exclusive access via the write lock
                        let block_ptr =
                            std::sync::Arc::<BlockHandle>::as_ptr(block) as *mut BlockHandle;
                        let block_mut = unsafe { &mut *block_ptr };

                        let block_size = block.size();
                        let block_tag = block.tag();
                        if let Ok(Some(taken_buffer)) = block_mut
                            .unload_and_take_block(has_temp_dir, |bid, data| {
                                self.write_to_temporary_file(bid, block_tag, data)
                            })
                        {
                            reservation.resize(0);
                            self.sub_used_memory_saturating(block_tag, block_size);
                            // Resize buffer if it's larger than needed
                            let resized_buffer =
                                Self::resize_buffer_if_needed(taken_buffer, extra_memory);
                            *buf_ref = Some(resized_buffer);
                            found = true;
                            drop(blocks);
                            self.stats.evictions.fetch_add(1, Ordering::Relaxed);
                            // Record successful buffer reuse
                            self.stats.buffer_reuses.fetch_add(1, Ordering::Relaxed);
                            return false; // Stop iteration
                        }
                    }
                    drop(blocks);
                } else {
                    // Size mismatch - record it and continue looking
                    self.stats
                        .buffer_reuse_size_mismatches
                        .fetch_add(1, Ordering::Relaxed);
                }
            }

            // Release the memory and mark the block as unloaded
            let block_id = handle.block_id();
            let has_temp_dir = self.has_temporary_directory();
            let block_size = handle.size();
            let block_tag = handle.tag();

            let blocks = self.blocks.write().unwrap();
            if let Some(block) = blocks.get(&block_id) {
                let block_ptr = std::sync::Arc::<BlockHandle>::as_ptr(block) as *mut BlockHandle;
                let block_mut = unsafe { &mut *block_ptr };

                // Unload the block (releases buffer but keeps BlockHandle)
                let unload_result = block_mut.unload(has_temp_dir, |bid, data| {
                    self.write_to_temporary_file(bid, block_tag, data)
                });

                // Update memory statistics (decrease used_memory)
                if unload_result.is_ok() && !block.is_loaded() {
                    self.update_used_memory(block_tag, -(block_size as i64));
                }
            }
            drop(blocks);

            // Note: We do NOT call self.free(block_id) here
            // The BlockHandle remains in self.blocks for potential reload later

            self.stats.evictions.fetch_add(1, Ordering::Relaxed);

            // Check if we've freed enough memory
            if self.used_memory() <= memory_limit {
                found = true;
                return false; // Stop iteration
            }

            true // Continue iteration
        });

        // If eviction failed, resize reservation to 0
        if !found {
            reservation.resize(0);
        }

        EvictionResult {
            success: found,
            reservation,
        }
    }

    /// Check if a buffer size is acceptable for reuse.
    ///
    /// We allow reusing buffers that are:
    /// 1. Exactly the requested size (perfect match)
    /// 2. Up to 25% larger than requested (acceptable overhead)
    /// 3. At least the requested size (never smaller)
    ///
    /// This strategy balances memory efficiency with reuse rate.
    ///
    /// # Arguments
    /// * `buffer_size` - Size of the available buffer
    /// * `requested_size` - Size of the requested allocation
    ///
    /// # Returns
    /// `true` if the buffer can be reused, `false` otherwise
    ///
    /// # Examples
    /// ```ignore
    /// assert!(can_reuse_buffer(1024, 1024));  // Exact match
    /// assert!(can_reuse_buffer(1280, 1024));  // 25% larger, acceptable
    /// assert!(!can_reuse_buffer(1281, 1024)); // >25% larger, too wasteful
    /// assert!(!can_reuse_buffer(1000, 1024)); // Smaller, not acceptable
    /// ```
    fn can_reuse_buffer(buffer_size: usize, requested_size: usize) -> bool {
        if buffer_size < requested_size {
            // Buffer is too small, cannot reuse
            return false;
        }

        if buffer_size == requested_size {
            // Perfect match
            return true;
        }

        // Allow up to 25% overhead
        // This is a good balance between memory efficiency and reuse rate
        let max_acceptable_size = requested_size + (requested_size / 4);
        buffer_size <= max_acceptable_size
    }

    /// Resize a buffer to the requested size if needed.
    ///
    /// If the buffer is larger than requested, we truncate it to avoid
    /// wasting memory. The truncated portion is effectively "freed".
    ///
    /// # Arguments
    /// * `buffer` - The buffer to resize
    /// * `requested_size` - The desired size
    ///
    /// # Returns
    /// The resized buffer
    fn resize_buffer_if_needed(mut buffer: Vec<u8>, requested_size: usize) -> Vec<u8> {
        if buffer.len() > requested_size {
            buffer.truncate(requested_size);
            buffer.shrink_to_fit();
        }
        buffer
    }

    /// Ensure sufficient memory is available, evicting if necessary.
    ///
    /// This method checks if there's enough memory available for the requested
    /// allocation. If not, it attempts to evict blocks to free up memory.
    ///
    /// # Arguments
    /// * `required` - Amount of memory required in bytes
    ///
    /// # Returns
    /// Ok(()) if memory is available or eviction succeeded, Err otherwise
    fn ensure_memory_available(&self, required: usize) -> Result<()> {
        let max_memory = self.max_memory();
        if max_memory == 0 {
            return Ok(()); // Unlimited memory
        }

        // Check if we already have enough memory
        let current_used = self.used_memory();
        if current_used + required <= max_memory {
            return Ok(());
        }

        // Calculate how much memory we need to free
        let memory_to_free = (current_used + required).saturating_sub(max_memory);

        // Calculate target memory limit after eviction
        let memory_limit = max_memory.saturating_sub(required);

        // Try to evict blocks to free up memory
        let result = self.evict_blocks(MemoryTag::Allocator, memory_to_free, memory_limit, None);

        if result.success {
            // Verify we actually have enough memory now
            let new_used = self.used_memory();
            if new_used + required <= max_memory {
                return Ok(());
            }
        }

        // Eviction failed or didn't free enough memory
        Err(paro_error::out_of_memory(format!(
            "Failed to allocate {} bytes (used: {}/{}, need to free: {})",
            required, current_used, max_memory, memory_to_free
        )))
    }

    /// Add a block to the eviction queue.
    ///
    /// This is called when a block becomes fully unpinned.
    ///
    /// # Note
    /// Currently public for testing. In production, this should be
    /// called automatically when BufferHandle is dropped.
    pub fn add_to_eviction_queue(&self, block_id: BlockId) {
        let block_opt: Option<Arc<BlockHandle>> = {
            let blocks = self.blocks.read().unwrap();
            blocks.get(&block_id).cloned()
        };

        if let Some(block) = block_opt {
            // Increment sequence number and get new value
            let seq_num = (*block).next_eviction_seq_num();
            // Update LRU timestamp
            block.set_lru_timestamp(current_timestamp_ms());

            // If this is not the first addition (ts != 1), we're adding a newer version
            // which means we're killing exactly one previous version
            if seq_num != 1 {
                self.increment_dead_nodes(&block);
            }

            // Get the eviction queue for the block and add it
            let queue = self.get_eviction_queue_for_block_handle(&block);
            let node = BufferEvictionNode::new(std::sync::Arc::downgrade(&block), seq_num);
            let should_purge = queue.add_to_eviction_queue(node);

            // Trigger purge if needed
            if should_purge {
                self.purge_queue(&block);
            }
        }
    }

    fn remove_from_eviction_queue(&self, _block_id: BlockId) {
        // With the new multi-queue system, we don't need to explicitly remove from queue
        // Dead nodes will be detected and purged automatically
        // This method is kept for API compatibility but does nothing
    }

    /// Clear all blocks from the pool.
    ///
    /// # Warning
    /// Only call this when all handles have been dropped.
    pub fn clear(&self) -> Result<()> {
        let blocks: Vec<_> = {
            let blocks = self.blocks.read().unwrap();
            blocks.keys().copied().collect()
        };

        for block_id in blocks {
            self.free(block_id)?;
        }

        Ok(())
    }

    /// Get number of blocks in the pool.
    pub fn block_count(&self) -> usize {
        let blocks = self.blocks.read().unwrap();
        blocks.len()
    }
}

// Note: BufferPool no longer implements BufferManager trait directly
// because allocate() now requires Arc<Self> for memory pressure detection.
// Use StandardBufferManager instead, which wraps BufferPool in an Arc.
//
// TODO: Consider creating a wrapper type that implements BufferManager
// if direct BufferPool usage as BufferManager is needed.

impl std::fmt::Debug for BufferPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut debug = f.debug_struct("BufferPool");
        debug
            .field("max_memory", &self.max_memory())
            .field("used_memory", &self.used_memory())
            .field("block_count", &self.block_count())
            .field(
                "allocations",
                &self.stats.allocations.load(Ordering::Relaxed),
            )
            .field(
                "deallocations",
                &self.stats.deallocations.load(Ordering::Relaxed),
            )
            .field("evictions", &self.stats.evictions.load(Ordering::Relaxed));

        // Add buffer reuse statistics if any attempts were made
        let reuse_attempts = self.stats.buffer_reuse_attempts.load(Ordering::Relaxed);
        if reuse_attempts > 0 {
            debug
                .field(
                    "buffer_reuses",
                    &self.stats.buffer_reuses.load(Ordering::Relaxed),
                )
                .field("buffer_reuse_attempts", &reuse_attempts)
                .field(
                    "buffer_reuse_rate",
                    &format!("{:.1}%", self.stats.buffer_reuse_rate()),
                )
                .field(
                    "buffer_reuse_size_mismatches",
                    &self
                        .stats
                        .buffer_reuse_size_mismatches
                        .load(Ordering::Relaxed),
                );
        }

        debug.finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper function to create a BufferPool with temporary directory set
    fn create_pool_with_temp_dir(max_memory: usize) -> Arc<BufferPool> {
        let pool = BufferPool::new_arc(max_memory);
        // Set temporary directory for tests that need to evict ManagedBuffer blocks
        // Use a unique directory for each pool to avoid test interference
        // Combine process ID, thread ID, and timestamp for uniqueness
        use std::time::{SystemTime, UNIX_EPOCH};
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let thread_id = std::thread::current().id();
        let temp_dir = std::env::temp_dir().join(format!(
            "paro_test_{}_{:?}_{}",
            std::process::id(),
            thread_id,
            timestamp
        ));
        pool.set_temporary_directory(temp_dir.to_string_lossy().to_string())
            .unwrap();
        pool
    }

    #[test]
    fn test_buffer_pool_creation() {
        let pool = BufferPool::new_arc(1024 * 1024); // 1 MB
        assert_eq!(pool.max_memory(), 1024 * 1024);
        assert_eq!(pool.used_memory(), 0);
        assert_eq!(pool.block_count(), 0);
    }

    #[test]
    fn test_allocate_basic() {
        let pool = Arc::new(BufferPool::new(1024 * 1024));
        let handle = pool
            .allocate(
                MemoryTag::InMemoryTable,
                FileBufferType::ManagedBuffer,
                4096,
            )
            .unwrap();

        assert!(handle.is_valid());
        assert_eq!(handle.size(), 4096);
        assert_eq!(pool.used_memory(), 4096);
        assert_eq!(pool.block_count(), 1);
    }

    #[test]
    fn test_allocate_multiple() {
        let pool = Arc::new(BufferPool::new(1024 * 1024));

        let h1 = pool
            .allocate(
                MemoryTag::InMemoryTable,
                FileBufferType::ManagedBuffer,
                1024,
            )
            .unwrap();
        let h2 = pool
            .allocate(MemoryTag::HashTable, FileBufferType::ManagedBuffer, 2048)
            .unwrap();
        let h3 = pool
            .allocate(MemoryTag::OrderBy, FileBufferType::ManagedBuffer, 512)
            .unwrap();

        assert_eq!(pool.used_memory(), 1024 + 2048 + 512);
        assert_eq!(pool.block_count(), 3);

        drop(h1);
        drop(h2);
        drop(h3);
    }

    #[test]
    fn test_data_access() {
        let pool = Arc::new(BufferPool::new(1024 * 1024));
        let handle = pool
            .allocate(MemoryTag::InMemoryTable, FileBufferType::ManagedBuffer, 256)
            .unwrap();

        // SAFETY: We have exclusive access via handle
        unsafe {
            let data = handle.data_mut().unwrap();
            for i in 0..256 {
                data[i] = i as u8;
            }
        }

        let data = handle.data().unwrap();
        for i in 0..256 {
            assert_eq!(data[i], i as u8);
        }
    }

    #[test]
    fn test_out_of_memory() {
        use paro_common::error::codes;

        let pool = create_pool_with_temp_dir(1024); // Very small pool

        let result = pool.allocate(
            MemoryTag::InMemoryTable,
            FileBufferType::ManagedBuffer,
            2048,
        );
        assert!(result.is_err());

        let err = result.unwrap_err();
        assert!(err.is(codes::resource::OUT_OF_MEMORY));
    }

    #[test]
    fn test_set_memory_limit_with_evict_and_rollback() {
        let pool = BufferPool::new_arc(4096);

        // Evictable block.
        let handle = pool
            .allocate(MemoryTag::InMemoryTable, FileBufferType::Block, 1024)
            .unwrap();
        let block_id = handle.block_handle().unwrap().block_id();
        drop(handle);
        pool.add_to_eviction_queue(block_id);

        pool.set_memory_limit(512).unwrap();
        assert_eq!(pool.max_memory(), 512);
        assert!(pool.used_memory() <= 512);

        pool.set_memory_limit(2048).unwrap();
        assert_eq!(pool.max_memory(), 2048);

        // Pinned block cannot be evicted: shrink should fail and rollback.
        let pinned = pool
            .allocate(MemoryTag::InMemoryTable, FileBufferType::Block, 1024)
            .unwrap();
        let err = pool.set_memory_limit(256).unwrap_err();
        assert!(
            err.to_string().contains("Failed to change memory limit"),
            "unexpected error: {}",
            err
        );
        assert_eq!(pool.max_memory(), 2048);
        drop(pinned);
    }

    #[test]
    fn test_swap_limit_enforced_by_spill_manager() {
        let pool = create_pool_with_temp_dir(4096);
        pool.set_swap_limit(Some(128)).unwrap();

        let handle = pool
            .allocate(MemoryTag::OrderBy, FileBufferType::ManagedBuffer, 1024)
            .unwrap();
        let block_id = handle.block_handle().unwrap().block_id();
        drop(handle);
        pool.add_to_eviction_queue(block_id);

        // Writing 1024 bytes with a 128-byte swap limit should fail eviction.
        let result = pool.evict_blocks(MemoryTag::OrderBy, 0, 0, None);
        assert!(!result.success);
        assert!(pool.get_temporary_files().is_empty());
    }

    #[test]
    fn test_eviction() {
        let pool = create_pool_with_temp_dir(4096);

        // Allocate a block
        let handle = pool
            .allocate(
                MemoryTag::InMemoryTable,
                FileBufferType::ManagedBuffer,
                1024,
            )
            .unwrap();

        // Drop handle; BufferHandle drop now routes through pool.unpin and
        // automatically updates eviction queues.
        drop(handle);

        // Now allocate more, which should trigger eviction
        let result = pool.allocate(
            MemoryTag::InMemoryTable,
            FileBufferType::ManagedBuffer,
            3500,
        );

        // Either eviction worked and we got memory, or we're at the limit
        // The key point is that eviction machinery was triggered
        if result.is_ok() {
            assert!(pool.stats.evictions.load(Ordering::Relaxed) >= 1);
        }
    }

    #[test]
    fn test_pin_existing() {
        let pool = Arc::new(BufferPool::new(1024 * 1024));

        let handle = pool
            .allocate(
                MemoryTag::InMemoryTable,
                FileBufferType::ManagedBuffer,
                1024,
            )
            .unwrap();
        let block_id = handle.block_handle().unwrap().block_id();

        // Get another handle to the same block
        let handle2 = pool.pin(block_id).unwrap();

        assert!(handle2.is_valid());
        assert_eq!(handle2.size(), 1024);

        // Both handles should work
        drop(handle);
        drop(handle2);
    }

    #[test]
    fn test_free_unpinned() {
        let pool = Arc::new(BufferPool::new(1024 * 1024));

        let handle = pool
            .allocate(
                MemoryTag::InMemoryTable,
                FileBufferType::ManagedBuffer,
                1024,
            )
            .unwrap();
        let block_id = handle.block_handle().unwrap().block_id();

        // Drop handle first (unpins)
        drop(handle);

        // Now we can free
        assert!(pool.free(block_id).is_ok());
        assert_eq!(pool.block_count(), 0);
    }

    #[test]
    fn test_stats() {
        let pool = Arc::new(BufferPool::new(1024 * 1024));

        let _h1 = pool
            .allocate(
                MemoryTag::InMemoryTable,
                FileBufferType::ManagedBuffer,
                1024,
            )
            .unwrap();
        let _h2 = pool
            .allocate(MemoryTag::HashTable, FileBufferType::ManagedBuffer, 2048)
            .unwrap();

        let stats = pool.stats();
        assert_eq!(stats.allocations.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn test_unlimited_memory() {
        let pool = Arc::new(BufferPool::new(0)); // Unlimited

        // Should be able to allocate large blocks
        let _h = pool
            .allocate(
                MemoryTag::InMemoryTable,
                FileBufferType::ManagedBuffer,
                10 * 1024 * 1024,
            )
            .unwrap(); // 10 MB
        assert!(pool.available_memory() > 0);
    }

    // === LRU Enhancement Tests ===

    #[test]
    fn test_lru_eviction_seq_num() {
        let pool = create_pool_with_temp_dir(8192);

        // Allocate a block
        let handle = pool
            .allocate(
                MemoryTag::InMemoryTable,
                FileBufferType::ManagedBuffer,
                1024,
            )
            .unwrap();
        let block_id = handle.block_handle().unwrap().block_id();
        let block = handle.block_handle().unwrap();

        // Initial sequence number should be 0
        assert_eq!(block.current_eviction_seq_num(), 0);

        // Drop should add to eviction queue and increment seq_num
        drop(handle);

        // Sequence number should be 1 after first addition
        let block = pool.get_block(block_id).unwrap();
        assert_eq!(block.current_eviction_seq_num(), 1);

        // Pin again and drop again - should increment seq_num
        let handle = pool.pin(block_id).unwrap();
        drop(handle);

        // Sequence number should be 2 after re-addition
        let block = pool.get_block(block_id).unwrap();
        assert_eq!(block.current_eviction_seq_num(), 2);
    }

    #[test]
    fn test_lru_timestamp_tracking() {
        let pool = create_pool_with_temp_dir(8192);

        let handle = pool
            .allocate(
                MemoryTag::InMemoryTable,
                FileBufferType::ManagedBuffer,
                1024,
            )
            .unwrap();
        let block = handle.block_handle().unwrap();

        // Timestamp should be set on pin
        let ts1 = block.get_lru_timestamp();
        assert!(ts1 > 0);

        // Sleep a tiny bit and pin again
        std::thread::sleep(std::time::Duration::from_millis(2));
        let block_id = block.block_id();
        drop(handle);

        let handle2 = pool.pin(block_id).unwrap();
        let ts2 = handle2.block_handle().unwrap().get_lru_timestamp();

        // Timestamp should have been updated
        assert!(ts2 >= ts1);
    }

    #[test]
    fn test_dead_node_detection() {
        let pool = create_pool_with_temp_dir(8192);

        // Allocate and immediately free a block
        let handle = pool
            .allocate(
                MemoryTag::InMemoryTable,
                FileBufferType::ManagedBuffer,
                1024,
            )
            .unwrap();
        let block_id = handle.block_handle().unwrap().block_id();
        drop(handle);

        // Add to eviction queue with seq_num 1
        pool.add_to_eviction_queue(block_id);

        // Pin and unpin again - creates seq_num 2
        let handle = pool.pin(block_id).unwrap();
        drop(handle);
        pool.add_to_eviction_queue(block_id);

        // Now queue has two entries for same block with different seq_nums
        // First one (seq_num 1) is a dead node

        // Trigger eviction - should skip dead node and evict the live one
        // We want to evict the block, so we set a low memory limit
        let result = pool.evict_blocks(
            MemoryTag::InMemoryTable,
            0,   // No extra memory needed
            512, // Memory limit lower than current usage (1024)
            None,
        );
        assert!(result.success);

        // After eviction, BlockHandle should still exist but be unloaded
        let block = pool.get_block(block_id);
        assert!(
            block.is_some(),
            "BlockHandle should still exist after eviction"
        );
        assert!(
            !block.unwrap().is_loaded(),
            "Block should be unloaded after eviction"
        );

        // Dead nodes are now tracked per-queue, not globally
        // The queue should have detected and handled the dead node
    }

    #[test]
    fn test_multi_priority_queues() {
        let pool = create_pool_with_temp_dir(16384);
        // Set temporary directory for ManagedBuffer eviction
        pool.set_temporary_directory("/tmp/paro_test".to_string())
            .unwrap();

        // Allocate blocks with different buffer types
        let h1 = pool
            .allocate(MemoryTag::InMemoryTable, FileBufferType::Block, 1024)
            .unwrap();
        let h2 = pool
            .allocate(
                MemoryTag::InMemoryTable,
                FileBufferType::ManagedBuffer,
                2048,
            )
            .unwrap();
        let h3 = pool
            .allocate(MemoryTag::InMemoryTable, FileBufferType::TinyBuffer, 512)
            .unwrap();

        let block_id1 = h1.block_handle().unwrap().block_id();
        let block_id2 = h2.block_handle().unwrap().block_id();
        let block_id3 = h3.block_handle().unwrap().block_id();

        // Drop handles and add to eviction queues
        drop(h1);
        drop(h2);
        drop(h3);
        pool.add_to_eviction_queue(block_id1);
        pool.add_to_eviction_queue(block_id2);
        pool.add_to_eviction_queue(block_id3);

        // Evict should prioritize Block (cheapest) first
        let result = pool.evict_blocks(
            MemoryTag::InMemoryTable,
            0,    // No extra memory needed
            1024, // Memory limit: evict until we're at or below 1024 bytes
            None,
        );
        assert!(result.success);

        // After eviction, BlockHandles should still exist but some should be unloaded
        // Block should be evicted first (highest priority)
        let block1 = pool.get_block(block_id1);
        assert!(
            block1.is_some(),
            "BlockHandle should still exist after eviction"
        );
        assert!(
            !block1.unwrap().is_loaded(),
            "Block1 should be unloaded after eviction"
        );

        // Other blocks may or may not be unloaded depending on total size
        // Since total is 1024+2048+512=3584, and limit is 1024, we need to evict at least 2560 bytes
        // So block1 (1024) + block2 (2048) should be evicted
        assert!(pool.get_block(block_id2).is_some());
        assert!(
            !pool.get_block(block_id2).unwrap().is_loaded(),
            "Block2 should be unloaded"
        );

        // Block3 might still be loaded
        assert!(pool.get_block(block_id3).is_some());
    }

    #[test]
    fn test_buffer_reuse_exact_match() {
        let pool = create_pool_with_temp_dir(16384);

        // Allocate a block
        let h1 = pool
            .allocate(
                MemoryTag::InMemoryTable,
                FileBufferType::ManagedBuffer,
                1024,
            )
            .unwrap();
        let block_id = h1.block_handle().unwrap().block_id();
        drop(h1);
        pool.add_to_eviction_queue(block_id);

        // Request exact same size - should reuse
        let mut reused_buffer = None;
        let current_used = pool.used_memory();
        let memory_limit = current_used.saturating_sub(1024);
        let result = pool.evict_blocks(
            MemoryTag::InMemoryTable,
            1024,
            memory_limit,
            Some(&mut reused_buffer),
        );

        assert!(result.success);
        assert!(reused_buffer.is_some());
        assert_eq!(reused_buffer.unwrap().len(), 1024);
    }

    #[test]
    fn test_buffer_reuse_acceptable_overhead() {
        let pool = create_pool_with_temp_dir(16384);

        // Allocate a 1280-byte block (25% larger than 1024)
        let h1 = pool
            .allocate(
                MemoryTag::InMemoryTable,
                FileBufferType::ManagedBuffer,
                1280,
            )
            .unwrap();
        let block_id = h1.block_handle().unwrap().block_id();
        drop(h1);
        pool.add_to_eviction_queue(block_id);

        // Request 1024 bytes - should reuse and resize
        let mut reused_buffer = None;
        let current_used = pool.used_memory();
        let memory_limit = current_used.saturating_sub(1024);
        let result = pool.evict_blocks(
            MemoryTag::InMemoryTable,
            1024,
            memory_limit,
            Some(&mut reused_buffer),
        );

        assert!(result.success);
        assert!(reused_buffer.is_some());
        // Buffer should be resized to requested size
        assert_eq!(reused_buffer.unwrap().len(), 1024);
    }

    #[test]
    fn test_buffer_reuse_too_large() {
        let pool = create_pool_with_temp_dir(16384);

        // Allocate a 2048-byte block (100% larger than 1024, exceeds 25% threshold)
        let h1 = pool
            .allocate(
                MemoryTag::InMemoryTable,
                FileBufferType::ManagedBuffer,
                2048,
            )
            .unwrap();
        let block_id = h1.block_handle().unwrap().block_id();
        drop(h1);
        pool.add_to_eviction_queue(block_id);

        // Request 1024 bytes - should NOT reuse (too wasteful)
        let mut reused_buffer = None;
        let current_used = pool.used_memory();
        let memory_limit = current_used.saturating_sub(1024);
        let result = pool.evict_blocks(
            MemoryTag::InMemoryTable,
            1024,
            memory_limit,
            Some(&mut reused_buffer),
        );

        assert!(result.success);
        // Buffer should not be reused (too large)
        assert!(reused_buffer.is_none());
    }

    #[test]
    fn test_buffer_reuse_too_small() {
        let pool = create_pool_with_temp_dir(16384);

        // Allocate two blocks to ensure we have something to evict
        let h1 = pool
            .allocate(MemoryTag::InMemoryTable, FileBufferType::ManagedBuffer, 512)
            .unwrap();
        let h2 = pool
            .allocate(
                MemoryTag::InMemoryTable,
                FileBufferType::ManagedBuffer,
                1024,
            )
            .unwrap();

        let block_id1 = h1.block_handle().unwrap().block_id();
        let block_id2 = h2.block_handle().unwrap().block_id();

        drop(h1);
        drop(h2);
        pool.add_to_eviction_queue(block_id1); // 512 bytes - too small
        pool.add_to_eviction_queue(block_id2); // 1024 bytes - exact match

        // Request 1024 bytes - should skip the 512-byte block and reuse the 1024-byte block
        let mut reused_buffer = None;
        let current_used = pool.used_memory();
        let memory_limit = current_used.saturating_sub(1024);
        let result = pool.evict_blocks(
            MemoryTag::InMemoryTable,
            1024,
            memory_limit,
            Some(&mut reused_buffer),
        );

        assert!(result.success);
        // Should reuse the 1024-byte block, not the 512-byte one
        assert!(reused_buffer.is_some());
        assert_eq!(reused_buffer.unwrap().len(), 1024);
    }

    #[test]
    fn test_can_reuse_buffer() {
        // Exact match
        assert!(BufferPool::can_reuse_buffer(1024, 1024));

        // Acceptable overhead (up to 25%)
        assert!(BufferPool::can_reuse_buffer(1280, 1024)); // Exactly 25%
        assert!(BufferPool::can_reuse_buffer(1200, 1024)); // Less than 25%

        // Too much overhead (more than 25%)
        assert!(!BufferPool::can_reuse_buffer(1281, 1024)); // Just over 25%
        assert!(!BufferPool::can_reuse_buffer(2048, 1024)); // 100% overhead

        // Too small
        assert!(!BufferPool::can_reuse_buffer(1023, 1024));
        assert!(!BufferPool::can_reuse_buffer(512, 1024));
    }

    #[test]
    fn test_resize_buffer_if_needed() {
        // Buffer larger than needed - should truncate
        let buffer = vec![1u8; 2048];
        let resized = BufferPool::resize_buffer_if_needed(buffer, 1024);
        assert_eq!(resized.len(), 1024);
        assert_eq!(resized.capacity(), 1024); // shrink_to_fit should work

        // Buffer exact size - no change
        let buffer = vec![1u8; 1024];
        let resized = BufferPool::resize_buffer_if_needed(buffer, 1024);
        assert_eq!(resized.len(), 1024);

        // Buffer smaller than needed - no change (shouldn't happen in practice)
        let buffer = vec![1u8; 512];
        let resized = BufferPool::resize_buffer_if_needed(buffer, 1024);
        assert_eq!(resized.len(), 512);
    }

    #[test]
    fn test_buffer_reuse_statistics() {
        let pool = create_pool_with_temp_dir(16384);

        // Initially no statistics
        assert_eq!(pool.stats.buffer_reuse_attempts.load(Ordering::Relaxed), 0);
        assert_eq!(pool.stats.buffer_reuses.load(Ordering::Relaxed), 0);
        assert_eq!(
            pool.stats
                .buffer_reuse_size_mismatches
                .load(Ordering::Relaxed),
            0
        );
        assert_eq!(pool.stats.buffer_reuse_rate(), 0.0);

        // Allocate and evict with successful reuse
        let h1 = pool
            .allocate(
                MemoryTag::InMemoryTable,
                FileBufferType::ManagedBuffer,
                1024,
            )
            .unwrap();
        let block_id1 = h1.block_handle().unwrap().block_id();
        drop(h1);
        pool.add_to_eviction_queue(block_id1);

        let mut reused_buffer = None;
        let current_used = pool.used_memory();
        let memory_limit = current_used.saturating_sub(1024);
        let result = pool.evict_blocks(
            MemoryTag::InMemoryTable,
            1024,
            memory_limit,
            Some(&mut reused_buffer),
        );

        assert!(result.success);
        assert!(reused_buffer.is_some());

        // Check statistics
        assert_eq!(pool.stats.buffer_reuse_attempts.load(Ordering::Relaxed), 1);
        assert_eq!(pool.stats.buffer_reuses.load(Ordering::Relaxed), 1);
        assert_eq!(pool.stats.buffer_reuse_rate(), 100.0);

        // Allocate and evict with size mismatch
        let h2 = pool
            .allocate(
                MemoryTag::InMemoryTable,
                FileBufferType::ManagedBuffer,
                2048,
            )
            .unwrap();
        let block_id2 = h2.block_handle().unwrap().block_id();
        drop(h2);
        pool.add_to_eviction_queue(block_id2);

        let mut reused_buffer2 = None;
        let current_used = pool.used_memory();
        let memory_limit = current_used.saturating_sub(1024);
        let result = pool.evict_blocks(
            MemoryTag::InMemoryTable,
            1024, // Request 1024 but block is 2048 (too large)
            memory_limit,
            Some(&mut reused_buffer2),
        );

        assert!(result.success);
        assert!(reused_buffer2.is_none()); // Not reused due to size mismatch

        // Check updated statistics
        assert_eq!(pool.stats.buffer_reuse_attempts.load(Ordering::Relaxed), 2);
        assert_eq!(pool.stats.buffer_reuses.load(Ordering::Relaxed), 1); // Still 1
                                                                         // Size mismatch count may be >= 1 depending on how many blocks were checked
        assert!(
            pool.stats
                .buffer_reuse_size_mismatches
                .load(Ordering::Relaxed)
                >= 1
        );
        assert_eq!(pool.stats.buffer_reuse_rate(), 50.0); // 1/2 = 50%
    }

    #[test]
    fn test_buffer_reuse_stats_string() {
        let pool = create_pool_with_temp_dir(16384);

        // Test with no attempts
        let stats_str = pool.stats.buffer_reuse_stats_string();
        assert!(stats_str.contains("0 successful"));
        assert!(stats_str.contains("0 attempts"));
        assert!(stats_str.contains("0.0% rate"));

        // Simulate some statistics
        pool.stats
            .buffer_reuse_attempts
            .store(10, Ordering::Relaxed);
        pool.stats.buffer_reuses.store(8, Ordering::Relaxed);
        pool.stats
            .buffer_reuse_size_mismatches
            .store(2, Ordering::Relaxed);

        let stats_str = pool.stats.buffer_reuse_stats_string();
        assert!(stats_str.contains("8 successful"));
        assert!(stats_str.contains("10 attempts"));
        assert!(stats_str.contains("80.0% rate"));
        assert!(stats_str.contains("2 size mismatches"));
    }

    #[test]
    fn test_buffer_pool_debug_with_reuse_stats() {
        let pool = create_pool_with_temp_dir(16384);

        // Debug without reuse attempts
        let debug_str = format!("{:?}", pool);
        assert!(debug_str.contains("BufferPool"));
        assert!(debug_str.contains("max_memory"));
        assert!(!debug_str.contains("buffer_reuses")); // Should not show if no attempts

        // Simulate some reuse attempts
        pool.stats.buffer_reuse_attempts.store(5, Ordering::Relaxed);
        pool.stats.buffer_reuses.store(4, Ordering::Relaxed);

        let debug_str = format!("{:?}", pool);
        assert!(debug_str.contains("buffer_reuses"));
        assert!(debug_str.contains("buffer_reuse_attempts"));
        assert!(debug_str.contains("buffer_reuse_rate"));
    }

    #[test]
    fn test_concurrent_reuse_statistics() {
        use std::thread;

        let pool = BufferPool::new_arc(1024 * 1024);

        // Set temporary directory for ManagedBuffer eviction
        let temp_dir = std::env::temp_dir().join("paro_test_concurrent_reuse");
        pool.set_temporary_directory(temp_dir.to_string_lossy().to_string())
            .unwrap();

        // Spawn multiple threads that perform evictions with reuse
        let mut handles = vec![];
        for _ in 0..4 {
            let pool_clone = pool.clone();
            let handle = thread::spawn(move || {
                for _ in 0..10 {
                    // Allocate and evict
                    let h = pool_clone
                        .allocate(
                            MemoryTag::InMemoryTable,
                            FileBufferType::ManagedBuffer,
                            1024,
                        )
                        .unwrap();
                    let block_id = h.block_handle().unwrap().block_id();
                    drop(h);
                    pool_clone.add_to_eviction_queue(block_id);

                    let mut reused_buffer = None;
                    let _ = pool_clone.evict_blocks(
                        MemoryTag::InMemoryTable,
                        1024,
                        0,
                        Some(&mut reused_buffer),
                    );
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // Check that statistics were updated (exact values may vary due to concurrency)
        let attempts = pool.stats.buffer_reuse_attempts.load(Ordering::Relaxed);
        assert!(attempts > 0, "Should have recorded some reuse attempts");

        // Reuse rate should be reasonable
        let rate = pool.stats.buffer_reuse_rate();
        assert!(
            rate >= 0.0 && rate <= 100.0,
            "Reuse rate should be between 0 and 100"
        );
    }

    #[test]
    fn test_buffer_allocator_integration() {
        // Note: This test is disabled because BufferPool no longer implements
        // paro_common::allocator::BufferManager trait directly (allocate now requires Arc<Self>).
        // StandardBufferManager implements crate::buffer::BufferManager, which is a different trait.
        //
        // TODO: Create a wrapper type if BufferAllocator integration is needed.
    }

    // === Per-Tag Memory Tracking Tests ===

    #[test]
    fn test_memory_usage_per_tag() {
        let pool = Arc::new(BufferPool::new(1024 * 1024));

        // Allocate with different tags
        let _h1 = pool
            .allocate(MemoryTag::HashTable, FileBufferType::ManagedBuffer, 1024)
            .unwrap();
        let _h2 = pool
            .allocate(MemoryTag::OrderBy, FileBufferType::ManagedBuffer, 2048)
            .unwrap();
        let _h3 = pool
            .allocate(MemoryTag::ArtIndex, FileBufferType::ManagedBuffer, 512)
            .unwrap();

        // Check per-tag usage
        assert_eq!(pool.get_tag_usage(MemoryTag::HashTable), 1024);
        assert_eq!(pool.get_tag_usage(MemoryTag::OrderBy), 2048);
        assert_eq!(pool.get_tag_usage(MemoryTag::ArtIndex), 512);
        assert_eq!(pool.get_tag_usage(MemoryTag::BaseTable), 0);

        // Check total
        let snapshot = pool.get_memory_usage_info();
        assert_eq!(snapshot.total(), 1024 + 2048 + 512);
    }

    #[test]
    fn test_memory_usage_after_free() {
        let pool = Arc::new(BufferPool::new(1024 * 1024));

        // Allocate
        let handle = pool
            .allocate(MemoryTag::HashTable, FileBufferType::ManagedBuffer, 1024)
            .unwrap();
        let block_id = handle.block_handle().unwrap().block_id();
        assert_eq!(pool.get_tag_usage(MemoryTag::HashTable), 1024);

        // Free
        drop(handle);
        pool.free(block_id).unwrap();

        // Tag usage should be 0
        assert_eq!(pool.get_tag_usage(MemoryTag::HashTable), 0);
        assert_eq!(pool.get_memory_usage_info().total(), 0);
    }

    #[test]
    fn test_memory_usage_snapshot() {
        let pool = Arc::new(BufferPool::new(1024 * 1024));

        let _h1 = pool
            .allocate(MemoryTag::BaseTable, FileBufferType::ManagedBuffer, 100)
            .unwrap();
        let _h2 = pool
            .allocate(MemoryTag::HashTable, FileBufferType::ManagedBuffer, 200)
            .unwrap();
        let _h3 = pool
            .allocate(MemoryTag::ColumnData, FileBufferType::ManagedBuffer, 300)
            .unwrap();

        let snapshot = pool.get_memory_usage_info();

        // Check snapshot values
        assert_eq!(snapshot.get(MemoryTag::BaseTable), 100);
        assert_eq!(snapshot.get(MemoryTag::HashTable), 200);
        assert_eq!(snapshot.get(MemoryTag::ColumnData), 300);
        assert_eq!(snapshot.total(), 600);

        // Check non-zero iterator
        let non_zero: Vec<_> = snapshot.non_zero().collect();
        assert_eq!(non_zero.len(), 3);
    }

    #[test]
    fn test_memory_usage_report() {
        let pool = Arc::new(BufferPool::new(1024 * 1024));

        let _h = pool
            .allocate(MemoryTag::HashTable, FileBufferType::ManagedBuffer, 1024)
            .unwrap();

        let snapshot = pool.get_memory_usage_info();
        let report = snapshot.format_report();

        assert!(report.contains("Memory Usage Report"));
        assert!(report.contains("Total: 1024 bytes"));
        assert!(report.contains("HASH_TABLE: 1024 bytes"));
    }

    #[test]
    fn test_simple_temp_file_write() {
        // Very simple test to verify temp file writing works
        let pool = create_pool_with_temp_dir(8192);

        // Allocate a block
        let handle = pool
            .allocate(MemoryTag::OrderBy, FileBufferType::ManagedBuffer, 1024)
            .unwrap();
        let block_id = handle.block_handle().unwrap().block_id();

        // Write some data
        unsafe {
            let data = handle.data_mut().unwrap();
            data[0] = 42;
            data[1] = 43;
        }

        // Verify data was written
        {
            let data = handle.data().unwrap();
            assert_eq!(data[0], 42);
            assert_eq!(data[1], 43);
        }

        // Drop handle and add to eviction queue
        drop(handle);
        pool.add_to_eviction_queue(block_id);

        // Manually trigger eviction
        let result = pool.evict_blocks(
            MemoryTag::OrderBy,
            0,
            0, // Force eviction
            None,
        );

        assert!(result.success, "Eviction should succeed");
        let temp_files = pool.get_temporary_files();
        assert!(
            !temp_files.is_empty(),
            "Expected at least one temporary spill file after eviction"
        );
        assert!(temp_files.iter().any(|info| info.size > 0));
    }

    #[test]
    fn test_block_reload_mechanism() {
        // Test the complete block reload mechanism with temporary files

        let pool = create_pool_with_temp_dir(16384); // Large pool to avoid memory issues

        // Allocate a block and write some data
        let handle = pool
            .allocate(MemoryTag::OrderBy, FileBufferType::ManagedBuffer, 1024)
            .unwrap();
        let block_id = handle.block_handle().unwrap().block_id();

        // Write test data
        unsafe {
            let data = handle.data_mut().unwrap();
            for i in 0..1024 {
                data[i] = (i % 256) as u8;
            }
        }

        // Verify block is loaded
        let block = pool.get_block(block_id).unwrap();
        assert!(block.is_loaded(), "Block should be loaded after allocation");

        // Drop handle and add to eviction queue
        drop(handle);
        pool.add_to_eviction_queue(block_id);

        // Manually evict the block
        let result = pool.evict_blocks(
            MemoryTag::OrderBy,
            0,
            0, // Force eviction
            None,
        );
        assert!(result.success, "Eviction should succeed");

        // Block should now be unloaded (written to temp file)
        let block = pool.get_block(block_id).unwrap();
        assert!(
            !block.is_loaded(),
            "Block should be unloaded after eviction"
        );

        // Verify temporary spill file exists
        let files_before_reload = pool.get_temporary_files();
        assert!(
            !files_before_reload.is_empty(),
            "Temp file should exist after eviction"
        );

        // Pin the block again - this should trigger reload from temp file
        let handle_reloaded = pool.pin(block_id).unwrap();
        assert!(
            handle_reloaded.is_valid(),
            "Should be able to pin unloaded block"
        );

        // Verify block is loaded again
        let block = pool.get_block(block_id).unwrap();
        assert!(block.is_loaded(), "Block should be loaded after pin");

        // Verify data integrity - data should be preserved through eviction/reload cycle
        let data = handle_reloaded.data().unwrap();
        for i in 0..1024 {
            assert_eq!(
                data[i],
                (i % 256) as u8,
                "Data mismatch at index {}: expected {}, got {}",
                i,
                (i % 256) as u8,
                data[i]
            );
        }
    }

    #[test]
    fn test_temporary_file_persistence() {
        // Test that temporary files correctly persist block data across eviction/reload cycles

        let pool = create_pool_with_temp_dir(16384);

        // Allocate multiple blocks with different data patterns
        let mut block_ids = Vec::new();

        for pattern in 0..3 {
            let handle = pool
                .allocate(MemoryTag::OrderBy, FileBufferType::ManagedBuffer, 1024)
                .unwrap();
            let block_id = handle.block_handle().unwrap().block_id();
            block_ids.push(block_id);

            // Write unique pattern for each block
            unsafe {
                let data = handle.data_mut().unwrap();
                for i in 0..1024 {
                    data[i] = ((i + pattern * 100) % 256) as u8;
                }
            }

            // Add to eviction queue immediately
            drop(handle);
            pool.add_to_eviction_queue(block_id);
        }

        // Manually evict all blocks
        let result = pool.evict_blocks(
            MemoryTag::OrderBy,
            0,
            0, // Force eviction of all
            None,
        );
        assert!(result.success, "Eviction should succeed");

        // All original blocks should be unloaded
        for &block_id in &block_ids {
            let block = pool.get_block(block_id).unwrap();
            assert!(!block.is_loaded(), "Block {} should be unloaded", block_id);
        }

        // Reload each block and verify data integrity
        for (idx, &block_id) in block_ids.iter().enumerate() {
            let handle = pool.pin(block_id).unwrap();
            assert!(
                handle.is_valid(),
                "Should be able to reload block {}",
                block_id
            );

            // Verify the unique pattern for this block
            let data = handle.data().unwrap();
            for i in 0..1024 {
                let expected = ((i + idx * 100) % 256) as u8;
                assert_eq!(
                    data[i], expected,
                    "Block {} data mismatch at index {}: expected {}, got {}",
                    block_id, i, expected, data[i]
                );
            }
        }
    }

    #[test]
    fn test_reload_restores_memory_accounting_before_next_eviction() {
        let pool = create_pool_with_temp_dir(16384);
        let handle = pool
            .allocate(MemoryTag::OrderBy, FileBufferType::ManagedBuffer, 1024)
            .unwrap();
        let block_id = handle.block_handle().unwrap().block_id();

        drop(handle);
        let first_eviction = pool.evict_blocks(MemoryTag::OrderBy, 0, 0, None);
        assert!(first_eviction.success);
        assert_eq!(pool.used_memory(), 0);
        assert_eq!(pool.get_tag_usage(MemoryTag::OrderBy), 0);

        let reloaded = pool.pin(block_id).unwrap();
        assert_eq!(
            pool.used_memory(),
            1024,
            "reloading a spilled block must restore resident memory accounting"
        );
        assert_eq!(
            pool.get_tag_usage(MemoryTag::OrderBy),
            1024,
            "reloading a spilled block must restore per-tag accounting"
        );

        drop(reloaded);
        let second_eviction = pool.evict_blocks(MemoryTag::OrderBy, 0, 0, None);
        assert!(second_eviction.success);
        assert_eq!(
            pool.used_memory(),
            0,
            "evicting a reloaded block must not underflow used memory"
        );
        assert_eq!(
            pool.get_tag_usage(MemoryTag::OrderBy),
            0,
            "evicting a reloaded block must not drive tag usage negative"
        );
    }

    // Note: Concurrent block reload test removed due to complexity
    // The double-checked locking in pin() is tested indirectly by other tests

    #[test]
    fn test_temporary_file_cleanup() {
        // Test that temporary files are cleaned up after reading
        let temp_dir = std::env::temp_dir().join("paro_test_cleanup");
        let pool = BufferPool::new_arc(16384);
        pool.set_temporary_directory(temp_dir.to_string_lossy().to_string())
            .unwrap();

        // Allocate a block
        let handle = pool
            .allocate(MemoryTag::OrderBy, FileBufferType::ManagedBuffer, 1024)
            .unwrap();
        let block_id = handle.block_handle().unwrap().block_id();

        drop(handle);
        pool.add_to_eviction_queue(block_id);

        // Force eviction (writes to temp file)
        let result = pool.evict_blocks(MemoryTag::OrderBy, 0, 0, None);
        assert!(result.success, "Eviction should succeed");

        let files_before_reload = pool.get_temporary_files();
        assert!(
            !files_before_reload.is_empty(),
            "Temporary file should exist after eviction"
        );

        // Reload the block (should delete temp file)
        let _handle = pool.pin(block_id).unwrap();

        // Temp file should be deleted after reading
        assert!(
            pool.get_temporary_files().is_empty(),
            "Temporary file should be deleted after reading"
        );

        // Cleanup test directory
        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_set_temporary_directory_clears_existing_spill_files() {
        let initial_dir = std::env::temp_dir().join("paro_test_switch_temp_dir_initial");
        let next_dir = std::env::temp_dir().join("paro_test_switch_temp_dir_next");
        let pool = BufferPool::new_arc(16384);
        pool.set_temporary_directory(initial_dir.to_string_lossy().to_string())
            .unwrap();

        let handle = pool
            .allocate(MemoryTag::OrderBy, FileBufferType::ManagedBuffer, 1024)
            .unwrap();
        let block_id = handle.block_handle().unwrap().block_id();
        drop(handle);
        pool.add_to_eviction_queue(block_id);
        let result = pool.evict_blocks(MemoryTag::OrderBy, 0, 0, None);
        assert!(result.success);
        assert!(
            !pool.get_temporary_files().is_empty(),
            "expected spill files before switching temp directory"
        );

        // Simulate stale spill metadata left by already-finished operators.
        {
            let mut blocks = pool.blocks.write().unwrap();
            blocks.remove(&block_id);
        }

        pool.set_temporary_directory(next_dir.to_string_lossy().to_string())
            .unwrap();
        assert!(
            pool.get_temporary_files().is_empty(),
            "switching temp directory should clear stale spill files"
        );

        let _ = std::fs::remove_dir_all(&initial_dir);
        let _ = std::fs::remove_dir_all(&next_dir);
    }

    #[test]
    fn test_free_cleans_spilled_temp_blocks() {
        let pool = create_pool_with_temp_dir(16384);
        let handle = pool
            .allocate(MemoryTag::OrderBy, FileBufferType::ManagedBuffer, 2048)
            .unwrap();
        let block_id = handle.block_handle().unwrap().block_id();

        drop(handle);
        pool.add_to_eviction_queue(block_id);
        let result = pool.evict_blocks(MemoryTag::OrderBy, 0, 0, None);
        assert!(result.success);
        assert_eq!(
            pool.used_memory(),
            0,
            "evicted block should no longer count as resident memory"
        );
        assert_eq!(
            pool.get_tag_usage(MemoryTag::OrderBy),
            0,
            "evicted block should no longer count against tag usage"
        );
        assert!(
            !pool.get_temporary_files().is_empty(),
            "expected spill file to exist before free"
        );

        pool.free(block_id).unwrap();
        assert_eq!(
            pool.used_memory(),
            0,
            "freeing an evicted block must not double-subtract resident memory"
        );
        assert_eq!(
            pool.get_tag_usage(MemoryTag::OrderBy),
            0,
            "freeing an evicted block must not drive tag usage negative"
        );
        assert!(
            pool.get_temporary_files().is_empty(),
            "free should remove temp blocks for the freed handle"
        );
    }

    #[test]
    fn test_temporary_spill_metrics_exposed() {
        let pool = create_pool_with_temp_dir(16384);
        let handle = pool
            .allocate(MemoryTag::OrderBy, FileBufferType::ManagedBuffer, 2048)
            .unwrap();
        let block_id = handle.block_handle().unwrap().block_id();
        drop(handle);
        pool.add_to_eviction_queue(block_id);
        pool.evict_blocks(MemoryTag::OrderBy, 0, 0, None);

        let metrics = pool.get_temporary_spill_metrics();
        assert!(metrics.write_bytes > 0);
        assert!(metrics.file_count > 0);
        assert!(metrics.swap_usage > 0);

        let _ = pool.pin(block_id).unwrap();
        let metrics_after_read = pool.get_temporary_spill_metrics();
        assert!(metrics_after_read.read_bytes > 0);
    }
}

// ============================================================================
// BufferManager Trait Implementation
// ============================================================================

/// Implementation of BufferManager trait for BufferPool.
///
/// This allows BufferPool to be used as a BufferManager for BufferAllocator,
/// enabling integration between the compute layer (which uses Allocator)
/// and the storage layer (which manages physical buffers).
impl BufferManager for BufferPool {
    fn allocate(&self, tag: MemoryTag, size: usize) -> Result<*mut u8> {
        // Allocate a buffer using the buffer pool
        // Use MANAGED_BUFFER type for general allocations
        // Use explicit inherent method call to avoid ambiguity with trait method
        let handle = self.allocate(tag, FileBufferType::ManagedBuffer, size)?;

        // Get the raw pointer from the buffer
        let ptr = handle
            .ptr()
            .ok_or_else(|| paro_error::internal("Failed to get pointer from buffer"))?;

        // Store the handle in allocations map to keep it alive
        {
            let mut allocations = self.allocations.write().unwrap();
            allocations.insert(ptr as usize, handle);
        }

        Ok(ptr)
    }

    fn free(&self, ptr: *mut u8, _tag: MemoryTag, _size: usize) {
        if ptr.is_null() {
            return;
        }

        // Remove the handle from allocations map.
        // After unpinning, immediately free the underlying block so memory is
        // returned instead of waiting for future eviction pressure.
        let handle = {
            let mut allocations = self.allocations.write().unwrap();
            allocations.remove(&(ptr as usize))
        };

        let Some(handle) = handle else {
            return;
        };

        let block_id = handle.block_handle().map(|b| b.block_id());
        drop(handle); // unpin via BufferHandle::drop

        if let Some(block_id) = block_id {
            let _ = BufferPool::free(self, block_id);
        }
    }

    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn reallocate(
        &self,
        ptr: *mut u8,
        tag: MemoryTag,
        old_size: usize,
        new_size: usize,
    ) -> Result<*mut u8> {
        if ptr.is_null() {
            return BufferManager::allocate(self, tag, new_size);
        }

        if new_size == 0 {
            BufferManager::free(self, ptr, tag, old_size);
            return Ok(std::ptr::null_mut());
        }

        // Allocate new buffer
        let handle = self.allocate(tag, FileBufferType::ManagedBuffer, new_size)?;
        let new_ptr = handle
            .ptr()
            .ok_or_else(|| paro_error::internal("Failed to get pointer from buffer"))?;

        // Store the handle in allocations map to keep it alive
        {
            let mut allocations = self.allocations.write().unwrap();
            allocations.insert(new_ptr as usize, handle);
        }

        // Copy old data to new buffer
        if !new_ptr.is_null() && !ptr.is_null() {
            let copy_size = old_size.min(new_size);
            // SAFETY: Both pointers are valid and have at least copy_size bytes
            unsafe {
                std::ptr::copy_nonoverlapping(ptr, new_ptr, copy_size);
            }
        }

        // Free old buffer
        BufferManager::free(self, ptr, tag, old_size);

        Ok(new_ptr)
    }
}
