//! # Fixed Size Allocator
//!
//! Provides pointers to fixed-size memory segments of pre-allocated memory buffers.
//!
//! ## Design
//!
//! The FixedSizeAllocator manages multiple FixedSizeBuffers, each containing
//! fixed-size segments. It provides:
//!
//! - `new_segment()` - Allocate a new segment, returns IndexPointer
//! - `free(ptr)` - Free a segment
//! - `get(ptr)` - Get a pointer to segment data
//! - `initialize_vacuum()` / `finalize_vacuum()` - Compact fragmented buffers
//! - `merge(other)` - Merge two allocators
//!
//! ## Thread Safety
//!
//! FixedSizeAllocator is NOT internally synchronized. The caller (typically ART)
//! is responsible for providing thread safety when needed.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::buffer::{BufferManager, StandardBufferManager};

use super::fixed_size_buffer::{BlockPointer, FixedSizeBuffer, ValidityT, BITS_PER_VALIDITY};
use super::index_storage_info::IndexBufferInfo;
use super::IndexPointer;

/// Default block size (256 KB).
pub const DEFAULT_BLOCK_SIZE: usize = 262144;

/// Vacuum threshold percentage (10%).
pub const VACUUM_THRESHOLD: u8 = 10;

/// Information about a fixed-size allocator for serialization.
#[derive(Debug, Clone, Default)]
pub struct FixedSizeAllocatorInfo {
    /// Segment size in bytes.
    pub segment_size: usize,
    /// Buffer IDs.
    pub buffer_ids: Vec<u32>,
    /// Block pointers for each buffer.
    pub block_pointers: Vec<BlockPointer>,
    /// Segment counts for each buffer.
    pub segment_counts: Vec<usize>,
    /// Allocation sizes for each buffer.
    pub allocation_sizes: Vec<usize>,
    /// Buffer IDs with free space.
    pub buffers_with_free_space: Vec<u32>,
}

/// The FixedSizeAllocator provides pointers to fixed-size memory segments.
///
/// The pointers are IndexPointers, and the leftmost byte (metadata) must always be zero.
pub struct FixedSizeAllocator {
    /// Buffer manager for memory allocation.
    buffer_manager: Arc<dyn BufferManager>,

    /// Allocation size of one segment in a buffer.
    segment_size: usize,

    /// Block size for buffers.
    block_size: usize,

    /// Number of validity_t values in the bitmask.
    bitmask_count: usize,

    /// First starting byte of the payload (segments).
    bitmask_offset: usize,

    /// Number of possible segment allocations per buffer.
    available_segments_per_buffer: usize,

    /// Total number of allocated segments in all buffers.
    total_segment_count: usize,

    /// Buffers containing the segments.
    buffers: HashMap<u32, FixedSizeBuffer>,

    /// Buffers with free space.
    buffers_with_free_space: HashSet<u32>,

    /// Cached buffer ID with free space for consistent filling.
    buffer_with_free_space: Option<u32>,

    /// Buffers qualifying for vacuum.
    vacuum_buffers: HashSet<u32>,
}

impl FixedSizeAllocator {
    /// Creates a new fixed-size allocator with the given segment size.
    pub fn new(segment_size: usize) -> Self {
        Self::with_block_size(segment_size, DEFAULT_BLOCK_SIZE)
    }

    /// Creates a new fixed-size allocator with custom block size.
    pub fn with_block_size(segment_size: usize, block_size: usize) -> Self {
        let buffer_manager = Arc::new(StandardBufferManager::default_manager());
        Self::with_buffer_manager(segment_size, block_size, buffer_manager)
    }

    /// Creates a new fixed-size allocator with a custom buffer manager.
    pub fn with_buffer_manager(
        segment_size: usize,
        block_size: usize,
        buffer_manager: Arc<dyn BufferManager>,
    ) -> Self {
        assert!(
            segment_size <= block_size - std::mem::size_of::<ValidityT>(),
            "Segment size {} exceeds maximum {} for block size {}",
            segment_size,
            block_size - std::mem::size_of::<ValidityT>(),
            block_size
        );

        let (bitmask_count, bitmask_offset, available_segments_per_buffer) =
            Self::calculate_buffer_layout(segment_size, block_size);

        Self {
            buffer_manager,
            segment_size,
            block_size,
            bitmask_count,
            bitmask_offset,
            available_segments_per_buffer,
            total_segment_count: 0,
            buffers: HashMap::new(),
            buffers_with_free_space: HashSet::new(),
            buffer_with_free_space: None,
            vacuum_buffers: HashSet::new(),
        }
    }

    /// Calculates the buffer layout for the given segment and block sizes.
    fn calculate_buffer_layout(segment_size: usize, block_size: usize) -> (usize, usize, usize) {
        let bits_per_value = BITS_PER_VALIDITY;
        let mut byte_count = 0usize;
        let mut bitmask_count = 0usize;
        let mut available_segments = 0usize;

        while byte_count < block_size {
            if bitmask_count == 0
                || (bitmask_count * bits_per_value).is_multiple_of(available_segments)
            {
                bitmask_count += 1;
                byte_count += std::mem::size_of::<ValidityT>();
            }

            let remaining_bytes = block_size.saturating_sub(byte_count);
            let remaining_segments = std::cmp::min(remaining_bytes / segment_size, bits_per_value);

            if remaining_segments == 0 {
                break;
            }

            available_segments += remaining_segments;
            byte_count += remaining_segments * segment_size;
        }

        let bitmask_offset = bitmask_count * std::mem::size_of::<ValidityT>();
        (bitmask_count, bitmask_offset, available_segments)
    }

    /// Allocates a new segment and returns an IndexPointer to it.
    pub fn new_segment(&mut self) -> IndexPointer {
        if self.buffer_with_free_space.is_none() {
            let buffer_id = self.get_available_buffer_id();
            let buffer = FixedSizeBuffer::new(self.buffer_manager.clone(), self.block_size);
            buffer.initialize_bitmask(self.available_segments_per_buffer);
            self.buffers.insert(buffer_id, buffer);
            self.buffers_with_free_space.insert(buffer_id);
            self.buffer_with_free_space = Some(buffer_id);
        }

        let buffer_id = self.buffer_with_free_space.unwrap();
        let buffer = self.buffers.get(&buffer_id).unwrap();

        let offset = buffer.get_offset(self.bitmask_count, self.available_segments_per_buffer);
        buffer.increment_segment_count();
        self.total_segment_count += 1;

        if buffer.segment_count() == self.available_segments_per_buffer {
            self.buffers_with_free_space.remove(&buffer_id);
            self.next_buffer_with_free_space();
        }

        IndexPointer::with_buffer_and_offset(buffer_id, offset)
    }

    /// Frees the segment at the given IndexPointer.
    pub fn free(&mut self, ptr: IndexPointer) {
        let buffer_id = ptr.get_buffer_id();
        let offset = ptr.get_offset();

        let buffer = self
            .buffers
            .get(&buffer_id)
            .expect("Buffer not found for IndexPointer");

        buffer.free_segment(offset);
        buffer.decrement_segment_count();
        self.total_segment_count -= 1;

        if buffer.segment_count() == 0 {
            // Keep one empty buffer to prevent fluctuation
            if self.buffers_with_free_space.len() == 1 {
                return;
            }

            if self.buffer_with_free_space == Some(buffer_id) {
                self.buffer_with_free_space = None;
            }
            self.buffers_with_free_space.remove(&buffer_id);
            self.buffers.remove(&buffer_id);
            self.next_buffer_with_free_space();
        } else {
            self.buffers_with_free_space.insert(buffer_id);
            if self.buffer_with_free_space.is_none() {
                self.buffer_with_free_space = Some(buffer_id);
            }
        }
    }

    /// Gets a pointer to the segment data at the given IndexPointer.
    pub fn get(&self, ptr: IndexPointer, dirty: bool) -> *mut u8 {
        debug_assert!(ptr.get_offset() < self.available_segments_per_buffer as u32);

        let buffer_id = ptr.get_buffer_id();
        let buffer = self
            .buffers
            .get(&buffer_id)
            .expect("Buffer not found for IndexPointer");

        let offset = ptr.get_offset() as usize * self.segment_size + self.bitmask_offset;
        buffer.get(offset, dirty)
    }

    /// Gets a typed reference to the segment data.
    ///
    /// # Safety
    /// The caller must ensure T matches the actual stored type.
    #[inline]
    pub unsafe fn get_ref<T>(&self, ptr: IndexPointer) -> &T {
        let data_ptr = self.get(ptr, false);
        &*(data_ptr as *const T)
    }

    /// Gets a mutable typed reference to the segment data.
    ///
    /// # Safety
    /// The caller must ensure T matches the actual stored type.
    #[inline]
    pub unsafe fn get_mut<T>(&mut self, ptr: IndexPointer) -> &mut T {
        let data_ptr = self.get(ptr, true);
        &mut *(data_ptr as *mut T)
    }

    /// Returns true if the segment has been loaded from storage.
    pub fn loaded_from_storage(&self, ptr: IndexPointer) -> bool {
        let buffer_id = ptr.get_buffer_id();
        self.buffers
            .get(&buffer_id)
            .map(|b| b.in_memory())
            .unwrap_or(false)
    }

    /// Resets the allocator, freeing all buffers.
    pub fn reset(&mut self) {
        self.buffers.clear();
        self.buffers_with_free_space.clear();
        self.buffer_with_free_space = None;
        self.total_segment_count = 0;
    }

    /// Returns the in-memory size in bytes.
    pub fn get_in_memory_size(&self) -> usize {
        self.buffers.values().filter(|b| b.in_memory()).count() * self.block_size
    }

    /// Returns the segment size.
    #[inline]
    pub fn segment_size(&self) -> usize {
        self.segment_size
    }

    /// Returns the total segment count.
    #[inline]
    pub fn total_segment_count(&self) -> usize {
        self.total_segment_count
    }

    /// Returns the number of buffers.
    #[inline]
    pub fn buffer_count(&self) -> usize {
        self.buffers.len()
    }

    /// Returns true if the allocator is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.total_segment_count == 0
    }

    /// Returns the upper bound of buffer IDs.
    pub fn get_upper_bound_buffer_id(&self) -> u32 {
        self.buffers
            .keys()
            .copied()
            .max()
            .map(|id| id + 1)
            .unwrap_or(0)
    }

    /// Returns the buffer manager.
    pub fn buffer_manager(&self) -> &Arc<dyn BufferManager> {
        &self.buffer_manager
    }

    /// Merges another allocator into this one.
    pub fn merge(&mut self, other: &mut FixedSizeAllocator) {
        assert_eq!(
            self.segment_size, other.segment_size,
            "Cannot merge allocators with different segment sizes"
        );

        let upper_bound_id = self.get_upper_bound_buffer_id();

        for (buffer_id, buffer) in other.buffers.drain() {
            let new_id = buffer_id + upper_bound_id;
            self.buffers.insert(new_id, buffer);
        }

        for buffer_id in other.buffers_with_free_space.drain() {
            self.buffers_with_free_space
                .insert(buffer_id + upper_bound_id);
        }
        self.next_buffer_with_free_space();

        self.total_segment_count += other.total_segment_count;
        other.total_segment_count = 0;
    }

    /// Initializes a vacuum operation. Returns true if vacuuming is needed.
    pub fn initialize_vacuum(&mut self) -> bool {
        if self.total_segment_count == 0 {
            self.reset();
            return false;
        }

        self.remove_empty_buffers();

        let mut available_segments_in_memory = 0usize;
        let mut buffer_free_space: Vec<(usize, u32)> = Vec::new();

        for (&buffer_id, buffer) in &self.buffers {
            buffer.set_vacuum(false);
            if buffer.in_memory() {
                let available = self.available_segments_per_buffer - buffer.segment_count();
                available_segments_in_memory += available;
                buffer_free_space.push((available, buffer_id));
            }
        }

        if buffer_free_space.is_empty() {
            return false;
        }

        let excess_buffer_count = available_segments_in_memory / self.available_segments_per_buffer;
        if excess_buffer_count == 0 {
            return false;
        }

        let memory_usage = self.get_in_memory_size();
        let excess_memory = excess_buffer_count * self.block_size;
        let excess_percentage = (excess_memory as f64 / memory_usage as f64) * 100.0;

        if excess_percentage < VACUUM_THRESHOLD as f64 {
            return false;
        }

        buffer_free_space.sort_by(|a, b| b.0.cmp(&a.0));

        self.vacuum_buffers.clear();
        for (_, buffer_id) in buffer_free_space.iter().take(excess_buffer_count) {
            if let Some(buffer) = self.buffers.get(buffer_id) {
                buffer.set_vacuum(true);
                self.buffers_with_free_space.remove(buffer_id);
            }
            self.vacuum_buffers.insert(*buffer_id);
        }

        self.next_buffer_with_free_space();
        true
    }

    /// Finalizes a vacuum operation by freeing all vacuumed buffers.
    pub fn finalize_vacuum(&mut self) {
        for buffer_id in self.vacuum_buffers.drain() {
            self.buffers.remove(&buffer_id);
        }
    }

    /// Returns true if the IndexPointer qualifies for vacuum.
    #[inline]
    pub fn needs_vacuum(&self, ptr: IndexPointer) -> bool {
        self.vacuum_buffers.contains(&ptr.get_buffer_id())
    }

    /// Vacuums an IndexPointer by moving its data to a new location.
    pub fn vacuum_pointer(&mut self, old_ptr: IndexPointer) -> IndexPointer {
        let new_ptr = self.new_segment();
        self.total_segment_count -= 1;

        let old_data = self.get(old_ptr, false);
        let new_data = self.get(new_ptr, true);

        unsafe {
            std::ptr::copy_nonoverlapping(old_data, new_data, self.segment_size);
        }

        new_ptr
    }

    /// Returns allocator information for serialization.
    pub fn get_info(&self) -> FixedSizeAllocatorInfo {
        let mut info = FixedSizeAllocatorInfo {
            segment_size: self.segment_size,
            buffer_ids: Vec::with_capacity(self.buffers.len()),
            block_pointers: Vec::with_capacity(self.buffers.len()),
            segment_counts: Vec::with_capacity(self.buffers.len()),
            allocation_sizes: Vec::with_capacity(self.buffers.len()),
            buffers_with_free_space: Vec::with_capacity(self.buffers_with_free_space.len()),
        };

        for (&buffer_id, buffer) in &self.buffers {
            info.buffer_ids.push(buffer_id);
            info.block_pointers.push(buffer.block_pointer());
            info.segment_counts.push(buffer.segment_count());
            info.allocation_sizes.push(buffer.allocation_size());
        }

        for &buffer_id in &self.buffers_with_free_space {
            info.buffers_with_free_space.push(buffer_id);
        }

        info
    }

    /// Initializes the allocator from serialized information.
    pub fn init(&mut self, info: &FixedSizeAllocatorInfo) {
        self.segment_size = info.segment_size;
        self.total_segment_count = 0;

        let (bitmask_count, bitmask_offset, available_segments) =
            Self::calculate_buffer_layout(self.segment_size, self.block_size);
        self.bitmask_count = bitmask_count;
        self.bitmask_offset = bitmask_offset;
        self.available_segments_per_buffer = available_segments;

        for i in 0..info.buffer_ids.len() {
            let buffer_id = info.buffer_ids[i];
            let block_pointer = info.block_pointers[i];
            let segment_count = info.segment_counts[i];
            let allocation_size = info.allocation_sizes[i];

            let buffer = FixedSizeBuffer::from_disk(
                self.buffer_manager.clone(),
                self.block_size,
                segment_count,
                allocation_size,
                block_pointer,
            );

            self.buffers.insert(buffer_id, buffer);
            self.total_segment_count += segment_count;
        }

        for &buffer_id in &info.buffers_with_free_space {
            self.buffers_with_free_space.insert(buffer_id);
        }
        self.next_buffer_with_free_space();
    }

    /// Prepares buffers for WAL serialization.
    pub fn init_serialization_to_wal(&self) -> Vec<IndexBufferInfo> {
        let mut buffer_infos = Vec::with_capacity(self.buffers.len());

        for buffer in self.buffers.values() {
            buffer.set_allocation_size(
                self.available_segments_per_buffer,
                self.segment_size,
                self.bitmask_offset,
            );

            if let Some(data) = buffer.buffer_data() {
                buffer_infos.push(IndexBufferInfo {
                    data,
                    size: buffer.allocation_size(),
                });
            }
        }

        buffer_infos
    }

    /// Removes empty buffers.
    pub fn remove_empty_buffers(&mut self) {
        let empty_ids: Vec<u32> = self
            .buffers
            .iter()
            .filter(|(_, b)| b.segment_count() == 0)
            .map(|(&id, _)| id)
            .collect();

        for id in empty_ids {
            self.buffers_with_free_space.remove(&id);
            self.buffers.remove(&id);
        }

        self.next_buffer_with_free_space();
    }

    /// Verifies that there is at most one empty buffer.
    pub fn verify_buffers(&self) -> bool {
        let empty_count = self
            .buffers
            .values()
            .filter(|b| b.segment_count() == 0)
            .count();
        empty_count <= 1
    }

    /// Gets an available buffer ID.
    fn get_available_buffer_id(&self) -> u32 {
        // Start from 1 to ensure IndexPointer is always valid (data != 0)
        let mut buffer_id = (self.buffers.len() as u32).max(1);
        while self.buffers.contains_key(&buffer_id) {
            buffer_id = buffer_id.wrapping_add(1);
        }
        buffer_id
    }

    /// Caches the next buffer with free space.
    fn next_buffer_with_free_space(&mut self) {
        self.buffer_with_free_space = self.buffers_with_free_space.iter().next().copied();
    }

    /// Returns the available segments per buffer.
    #[inline]
    pub fn available_segments_per_buffer(&self) -> usize {
        self.available_segments_per_buffer
    }

    /// Returns the bitmask offset.
    #[inline]
    pub fn bitmask_offset(&self) -> usize {
        self.bitmask_offset
    }
}

// SAFETY: FixedSizeAllocator does not use internal synchronization.
// Thread safety must be provided by the caller.
unsafe impl Send for FixedSizeAllocator {}
unsafe impl Sync for FixedSizeAllocator {}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_buffer_manager() -> Arc<dyn BufferManager> {
        Arc::new(StandardBufferManager::with_defaults(100 * 1024 * 1024))
    }

    #[test]
    fn test_allocator_new() {
        let allocator = FixedSizeAllocator::new(64);
        assert_eq!(allocator.segment_size(), 64);
        assert_eq!(allocator.total_segment_count(), 0);
        assert!(allocator.is_empty());
    }

    #[test]
    fn test_allocator_with_block_size() {
        let allocator = FixedSizeAllocator::with_block_size(32, 4096);
        assert_eq!(allocator.segment_size(), 32);
        assert!(allocator.available_segments_per_buffer() > 0);
    }

    #[test]
    fn test_allocator_with_buffer_manager() {
        let manager = create_test_buffer_manager();
        let allocator = FixedSizeAllocator::with_buffer_manager(64, 4096, manager);
        assert_eq!(allocator.segment_size(), 64);
    }

    #[test]
    fn test_new_segment() {
        let mut allocator = FixedSizeAllocator::new(64);

        let ptr1 = allocator.new_segment();
        assert!(ptr1.is_valid());
        assert_eq!(allocator.total_segment_count(), 1);

        let ptr2 = allocator.new_segment();
        assert!(ptr2.is_valid());
        assert_eq!(allocator.total_segment_count(), 2);

        assert_ne!(ptr1.get(), ptr2.get());
    }

    #[test]
    fn test_free_segment() {
        let mut allocator = FixedSizeAllocator::new(64);

        let ptr = allocator.new_segment();
        assert_eq!(allocator.total_segment_count(), 1);

        allocator.free(ptr);
        assert_eq!(allocator.total_segment_count(), 0);
    }

    #[test]
    fn test_get_segment_data() {
        let mut allocator = FixedSizeAllocator::new(64);
        let ptr = allocator.new_segment();

        let data_ptr = allocator.get(ptr, true);
        unsafe {
            *data_ptr = 42;
        }

        let data_ptr = allocator.get(ptr, false);
        unsafe {
            assert_eq!(*data_ptr, 42);
        }
    }

    #[test]
    fn test_typed_access() {
        let mut allocator = FixedSizeAllocator::new(std::mem::size_of::<u64>());
        let ptr = allocator.new_segment();

        unsafe {
            let value: &mut u64 = allocator.get_mut(ptr);
            *value = 0xDEAD_BEEF_CAFE_BABE;

            let value: &u64 = allocator.get_ref(ptr);
            assert_eq!(*value, 0xDEAD_BEEF_CAFE_BABE);
        }
    }

    #[test]
    fn test_multiple_allocations() {
        let mut allocator = FixedSizeAllocator::with_block_size(64, 4096);
        let mut pointers = Vec::new();

        for i in 0..100 {
            let ptr = allocator.new_segment();
            let data_ptr = allocator.get(ptr, true);
            unsafe {
                *(data_ptr as *mut u32) = i;
            }
            pointers.push(ptr);
        }

        assert_eq!(allocator.total_segment_count(), 100);

        for (i, &ptr) in pointers.iter().enumerate() {
            let data_ptr = allocator.get(ptr, false);
            unsafe {
                assert_eq!(*(data_ptr as *const u32), i as u32);
            }
        }
    }

    #[test]
    fn test_free_and_reuse() {
        let mut allocator = FixedSizeAllocator::new(64);

        let ptr1 = allocator.new_segment();
        allocator.free(ptr1);

        let ptr2 = allocator.new_segment();
        assert!(ptr2.is_valid());
        assert_eq!(allocator.total_segment_count(), 1);
    }

    #[test]
    fn test_reset() {
        let mut allocator = FixedSizeAllocator::new(64);

        for _ in 0..10 {
            allocator.new_segment();
        }
        assert_eq!(allocator.total_segment_count(), 10);

        allocator.reset();
        assert_eq!(allocator.total_segment_count(), 0);
        assert!(allocator.is_empty());
    }

    #[test]
    fn test_merge() {
        let manager = create_test_buffer_manager();
        let mut allocator1 = FixedSizeAllocator::with_buffer_manager(64, 4096, manager.clone());
        let mut allocator2 = FixedSizeAllocator::with_buffer_manager(64, 4096, manager);

        for _ in 0..5 {
            allocator1.new_segment();
        }
        for _ in 0..3 {
            allocator2.new_segment();
        }

        allocator1.merge(&mut allocator2);

        assert_eq!(allocator1.total_segment_count(), 8);
        assert_eq!(allocator2.total_segment_count(), 0);
    }

    #[test]
    fn test_vacuum() {
        let mut allocator = FixedSizeAllocator::with_block_size(64, 1024);

        let mut pointers = Vec::new();
        for _ in 0..50 {
            pointers.push(allocator.new_segment());
        }

        for ptr in pointers.iter().take(25) {
            allocator.free(*ptr);
        }

        let needs_vacuum = allocator.initialize_vacuum();
        if needs_vacuum {
            allocator.finalize_vacuum();
        }

        assert_eq!(allocator.total_segment_count(), 25);
    }

    #[test]
    fn test_get_info() {
        let mut allocator = FixedSizeAllocator::new(64);

        for _ in 0..10 {
            allocator.new_segment();
        }

        let info = allocator.get_info();
        assert_eq!(info.segment_size, 64);
        assert!(!info.buffer_ids.is_empty());
    }

    #[test]
    fn test_init_from_info() {
        let manager = create_test_buffer_manager();
        let mut allocator1 = FixedSizeAllocator::with_buffer_manager(64, 4096, manager.clone());

        for _ in 0..10 {
            allocator1.new_segment();
        }

        let info = allocator1.get_info();

        let mut allocator2 = FixedSizeAllocator::with_buffer_manager(64, 4096, manager);
        allocator2.init(&info);

        assert_eq!(allocator2.segment_size(), info.segment_size);
        assert_eq!(allocator2.total_segment_count(), 10);
    }

    #[test]
    fn test_verify_buffers() {
        let mut allocator = FixedSizeAllocator::new(64);

        assert!(allocator.verify_buffers());

        let ptr = allocator.new_segment();
        allocator.free(ptr);

        assert!(allocator.verify_buffers());
    }

    #[test]
    fn test_in_memory_size() {
        let mut allocator = FixedSizeAllocator::with_block_size(64, 4096);

        assert_eq!(allocator.get_in_memory_size(), 0);

        allocator.new_segment();
        assert_eq!(allocator.get_in_memory_size(), 4096);
    }

    #[test]
    fn test_buffer_layout_calculation() {
        let (bitmask_count, bitmask_offset, available) =
            FixedSizeAllocator::calculate_buffer_layout(64, 4096);

        assert!(bitmask_count > 0);
        assert!(bitmask_offset > 0);
        assert!(available > 0);

        let total_size = bitmask_offset + available * 64;
        assert!(total_size <= 4096);
    }

    #[test]
    fn test_upper_bound_buffer_id() {
        let mut allocator = FixedSizeAllocator::new(64);

        assert_eq!(allocator.get_upper_bound_buffer_id(), 0);

        allocator.new_segment();
        assert!(allocator.get_upper_bound_buffer_id() > 0);
    }

    #[test]
    fn test_buffer_manager_memory_tracking() {
        let manager = create_test_buffer_manager();
        let initial_used = manager.get_used_memory();

        let mut allocator = FixedSizeAllocator::with_buffer_manager(64, 4096, manager.clone());
        allocator.new_segment();

        // Memory should be allocated through BufferManager
        assert!(manager.get_used_memory() > initial_used);
    }

    #[test]
    fn test_init_serialization_to_wal() {
        let mut allocator = FixedSizeAllocator::new(64);

        for _ in 0..5 {
            allocator.new_segment();
        }

        let buffer_infos = allocator.init_serialization_to_wal();
        assert!(!buffer_infos.is_empty());
        assert!(buffer_infos[0].size > 0);
    }
}
