// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Raw block allocator backing execution-time row storage.

use std::sync::{Arc, Mutex};

use crate::buffer::{
    BlockHandle, BufferHandle, BufferPool, FileBufferType, MemoryTag, DEFAULT_BLOCK_SIZE,
};
use paro_common::error::{self as paro_error, Result};
use paro_common::memory::{MemoryAccountingClass, MemoryAccountingContext, MemoryReleaseHandle};

use super::segment::RawRowChunkPart;
use super::{RawRowChunkState, RawRowLayout, RawRowPinProperties, RawRowPinState};
use paro_common::types::LogicalType;

/// A block of memory for storing raw row data.
///
/// Wraps a BlockHandle with capacity/size tracking.
#[derive(Debug)]
pub struct RawRowBlock {
    /// Handle to the allocated block (shared ownership)
    ///
    /// Uses `Arc<BlockHandle>` for shared ownership.
    pub handle: Option<Arc<BlockHandle>>,
    /// Total capacity in bytes
    pub capacity: usize,
    /// Currently used size in bytes
    pub size: usize,
    /// Logical memory ownership for this physical block.
    release: MemoryReleaseHandle,
}

impl RawRowBlock {
    /// Create a new row-data block from a buffer handle.
    fn new(buffer_handle: BufferHandle, capacity: usize, release: MemoryReleaseHandle) -> Self {
        // Extract the BlockHandle from the BufferHandle
        let handle = buffer_handle.block_handle().map(Arc::clone);
        Self {
            handle,
            capacity,
            size: 0,
            release,
        }
    }

    /// Get remaining capacity in bytes.
    #[inline]
    pub fn remaining_capacity(&self) -> usize {
        self.capacity.saturating_sub(self.size)
    }

    /// Get remaining capacity in rows.
    #[inline]
    pub fn remaining_rows(&self, row_width: usize) -> usize {
        if row_width == 0 {
            return 0;
        }
        self.remaining_capacity() / row_width
    }

    /// Get the capacity.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Get the current size.
    #[inline]
    pub fn size(&self) -> usize {
        self.size
    }

    /// Add to the used size.
    #[inline]
    pub fn add_size(&mut self, bytes: usize) {
        self.size += bytes;
        debug_assert!(self.size <= self.capacity);
    }

    #[inline]
    fn release_memory(&self) {
        self.release.release();
    }
}

/// Allocator for raw row blocks.
///
/// Manages allocation of row blocks (for fixed-size data) and
/// heap blocks (for variable-length data like VARCHAR).
#[derive(Debug)]
pub struct RawRowAllocator {
    /// Buffer pool for memory allocation
    buffer_pool: Arc<BufferPool>,
    /// Layout of the rows
    layout: Arc<RawRowLayout>,
    /// Memory tag for tracking
    tag: MemoryTag,
    /// Logical memory owner for block allocations.
    memory: MemoryAccountingContext,
    /// Row blocks (fixed-size row data)
    row_blocks: Vec<RawRowBlock>,
    /// Heap blocks (variable-length data)
    heap_blocks: Vec<RawRowBlock>,
}

impl RawRowAllocator {
    /// Create a new raw row allocator.
    ///
    /// # Arguments
    /// * `buffer_pool` - Buffer pool for memory allocation
    /// * `layout` - Layout of the rows to store
    /// * `tag` - Memory tag for tracking
    pub fn new(buffer_pool: Arc<BufferPool>, layout: Arc<RawRowLayout>, tag: MemoryTag) -> Self {
        let memory =
            MemoryAccountingContext::detached(tag, MemoryAccountingClass::default_for_tag(tag));
        Self::new_with_memory(buffer_pool, layout, tag, memory)
    }

    pub fn new_with_memory(
        buffer_pool: Arc<BufferPool>,
        layout: Arc<RawRowLayout>,
        tag: MemoryTag,
        memory: MemoryAccountingContext,
    ) -> Self {
        Self {
            buffer_pool,
            layout,
            tag,
            memory,
            row_blocks: Vec::new(),
            heap_blocks: Vec::new(),
        }
    }

    /// Get the buffer pool.
    #[inline]
    pub fn buffer_pool(&self) -> &Arc<BufferPool> {
        &self.buffer_pool
    }

    /// Get the row layout.
    #[inline]
    pub fn layout(&self) -> &RawRowLayout {
        &self.layout
    }

    /// Get the layout as Arc.
    #[inline]
    pub fn layout_ptr(&self) -> Arc<RawRowLayout> {
        Arc::clone(&self.layout)
    }

    /// Get the number of row blocks.
    #[inline]
    pub fn row_block_count(&self) -> usize {
        self.row_blocks.len()
    }

    /// Get the number of heap blocks.
    #[inline]
    pub fn heap_block_count(&self) -> usize {
        self.heap_blocks.len()
    }

    /// Get a row block by index.
    #[inline]
    pub fn get_row_block(&self, index: usize) -> Option<&RawRowBlock> {
        self.row_blocks.get(index)
    }

    /// Get a heap block by index.
    #[inline]
    pub fn get_heap_block(&self, index: usize) -> Option<&RawRowBlock> {
        self.heap_blocks.get(index)
    }

    /// Create a new row block.
    ///
    /// Allocates a new block from the buffer pool for storing rows.
    pub fn create_row_block(&mut self) -> Result<usize> {
        let release = self.memory.retain(DEFAULT_BLOCK_SIZE)?;
        let handle = match self.buffer_pool.allocate(
            self.tag,
            FileBufferType::ManagedBuffer,
            DEFAULT_BLOCK_SIZE,
        ) {
            Ok(handle) => handle,
            Err(err) => {
                release.release();
                return Err(err);
            }
        };
        let capacity = handle.size();
        self.row_blocks
            .push(RawRowBlock::new(handle, capacity, release));
        Ok(self.row_blocks.len() - 1)
    }

    /// Create a new heap block with the specified minimum size.
    ///
    /// # Arguments
    /// * `min_size` - Minimum size needed (may allocate larger)
    pub fn create_heap_block(&mut self, min_size: usize) -> Result<usize> {
        let size = min_size.max(DEFAULT_BLOCK_SIZE);
        let release = self.memory.retain(size)?;
        let handle = match self
            .buffer_pool
            .allocate(self.tag, FileBufferType::ManagedBuffer, size)
        {
            Ok(handle) => handle,
            Err(err) => {
                release.release();
                return Err(err);
            }
        };
        let capacity = handle.size();
        self.heap_blocks
            .push(RawRowBlock::new(handle, capacity, release));
        Ok(self.heap_blocks.len() - 1)
    }

    /// Allocate space for rows and return allocation info.
    ///
    /// # Arguments
    /// * `count` - Number of rows to allocate
    /// * `heap_sizes` - Heap size needed for each row (for variable-length data)
    ///
    /// # Returns
    /// `RowAllocation` with block indices and offsets.
    pub fn allocate_rows(
        &mut self,
        count: usize,
        heap_sizes: Option<&[usize]>,
    ) -> Result<RowAllocation> {
        if count == 0 {
            return Ok(RowAllocation::empty());
        }

        let row_width = self.layout.get_row_width();
        let all_constant = self.layout.all_constant();

        // Ensure we have a row block with space
        let row_block_index = self.ensure_row_block_space(row_width)?;

        // Get block info before any other mutable borrows
        let row_block_offset = self.row_blocks[row_block_index].size;
        let rows_that_fit = self.row_blocks[row_block_index]
            .remaining_rows(row_width)
            .min(count);

        // Handle heap allocation for variable-length data
        let heap_info = if !all_constant {
            if let Some(sizes) = heap_sizes {
                self.allocate_heap_space(rows_that_fit, &sizes[..rows_that_fit])?
            } else {
                None
            }
        } else {
            None
        };

        // Update row block size
        let row_bytes = rows_that_fit * row_width;
        self.row_blocks[row_block_index].add_size(row_bytes);

        Ok(RowAllocation {
            row_block_index,
            row_block_offset,
            count: rows_that_fit,
            heap_info,
        })
    }

    /// Ensure there's a row block with space for at least one row.
    fn ensure_row_block_space(&mut self, row_width: usize) -> Result<usize> {
        if let Some(last) = self.row_blocks.last() {
            if last.remaining_capacity() >= row_width {
                return Ok(self.row_blocks.len() - 1);
            }
        }
        self.create_row_block()
    }

    /// Allocate heap space for variable-length data.
    fn allocate_heap_space(
        &mut self,
        count: usize,
        heap_sizes: &[usize],
    ) -> Result<Option<HeapAllocation>> {
        let total_heap_size: usize = heap_sizes.iter().sum();
        if total_heap_size == 0 {
            return Ok(None);
        }

        // Find or create a heap block with enough space
        let first_size = heap_sizes.first().copied().unwrap_or(0);
        let heap_block_index = self.ensure_heap_block_space(first_size.max(total_heap_size))?;
        let heap_block = &mut self.heap_blocks[heap_block_index];
        let heap_block_offset = heap_block.size;

        // Check how much actually fits
        let heap_remaining = heap_block.remaining_capacity();
        let (actual_count, actual_heap_size) = if total_heap_size <= heap_remaining {
            (count, total_heap_size)
        } else {
            // Not everything fits - find how many rows we can fit
            let mut accumulated = 0usize;
            let mut fitting_count = 0usize;
            for &size in heap_sizes {
                if accumulated + size > heap_remaining {
                    break;
                }
                accumulated += size;
                fitting_count += 1;
            }
            (fitting_count, accumulated)
        };

        if actual_count == 0 {
            return Ok(None);
        }

        heap_block.add_size(actual_heap_size);

        Ok(Some(HeapAllocation {
            heap_block_index,
            heap_block_offset,
            total_heap_size: actual_heap_size,
            count: actual_count,
        }))
    }

    /// Ensure there's a heap block with space for the given size.
    fn ensure_heap_block_space(&mut self, min_size: usize) -> Result<usize> {
        if let Some(last) = self.heap_blocks.last() {
            if last.remaining_capacity() >= min_size {
                return Ok(self.heap_blocks.len() - 1);
            }
        }
        self.create_heap_block(min_size)
    }

    /// Get total data size across all blocks.
    pub fn total_data_size(&self) -> usize {
        let row_size: usize = self.row_blocks.iter().map(|b| b.size).sum();
        let heap_size: usize = self.heap_blocks.iter().map(|b| b.size).sum();
        row_size + heap_size
    }

    /// Pin a row block and return a buffer handle.
    ///
    /// # Arguments
    /// * `pin_state` - Pin state to store the handle
    /// * `block_index` - Index of the row block to pin
    ///
    /// # Returns
    /// Reference to the pinned buffer handle
    pub fn pin_row_block<'a>(
        &self,
        pin_state: &'a mut RawRowPinState,
        block_index: usize,
    ) -> Result<&'a BufferHandle> {
        // Get the block
        let block = self.row_blocks.get(block_index).ok_or_else(|| {
            paro_error::internal(format!("Invalid row block index: {}", block_index))
        })?;

        let handle = block
            .handle
            .as_ref()
            .ok_or_else(|| paro_error::internal("Block handle is None"))?;
        let block_id = handle.block_id();

        // Check if already pinned.
        if pin_state.row_handles.find(block_id).is_some() {
            return Ok(pin_state.row_handles.find(block_id).unwrap());
        }

        // Pin the block
        let buffer_handle = self.buffer_pool.pin(block_id)?;

        // Store in pin state
        pin_state.row_handles.insert(block_id, buffer_handle);
        Ok(pin_state.row_handles.find(block_id).unwrap())
    }

    /// Pin a heap block and return a buffer handle.
    ///
    /// # Arguments
    /// * `pin_state` - Pin state to store the handle
    /// * `block_index` - Index of the heap block to pin
    ///
    /// # Returns
    /// Reference to the pinned buffer handle
    pub fn pin_heap_block<'a>(
        &self,
        pin_state: &'a mut RawRowPinState,
        block_index: usize,
    ) -> Result<&'a BufferHandle> {
        // Get the block
        let block = self.heap_blocks.get(block_index).ok_or_else(|| {
            paro_error::internal(format!("Invalid heap block index: {}", block_index))
        })?;

        let handle = block
            .handle
            .as_ref()
            .ok_or_else(|| paro_error::internal("Block handle is None"))?;
        let block_id = handle.block_id();

        // Check if already pinned.
        if pin_state.heap_handles.find(block_id).is_some() {
            return Ok(pin_state.heap_handles.find(block_id).unwrap());
        }

        // Pin the block
        let buffer_handle = self.buffer_pool.pin(block_id)?;

        // Store in pin state
        pin_state.heap_handles.insert(block_id, buffer_handle);
        Ok(pin_state.heap_handles.find(block_id).unwrap())
    }

    /// Get a row pointer from a pinned block.
    ///
    /// # Arguments
    /// * `pin_state` - Pin state containing pinned handles
    /// * `block_index` - Index of the row block
    /// * `offset` - Byte offset within the block
    ///
    /// # Returns
    /// Pointer to the row data
    pub fn get_row_pointer_pinned(
        &self,
        pin_state: &mut RawRowPinState,
        block_index: usize,
        offset: usize,
    ) -> Result<*mut u8> {
        let handle = self.pin_row_block(pin_state, block_index)?;
        let ptr = handle
            .ptr()
            .ok_or_else(|| paro_error::internal("Failed to get buffer pointer"))?;

        // SAFETY: offset is assumed to be within bounds (caller's responsibility)
        Ok(unsafe { ptr.add(offset) })
    }

    /// Get a heap pointer from a pinned block.
    ///
    /// # Arguments
    /// * `pin_state` - Pin state containing pinned handles
    /// * `block_index` - Index of the heap block
    /// * `offset` - Byte offset within the block
    ///
    /// # Returns
    /// Pointer to the heap data
    pub fn get_heap_pointer_pinned(
        &self,
        pin_state: &mut RawRowPinState,
        block_index: usize,
        offset: usize,
    ) -> Result<*mut u8> {
        let handle = self.pin_heap_block(pin_state, block_index)?;
        let ptr = handle
            .ptr()
            .ok_or_else(|| paro_error::internal("Failed to get buffer pointer"))?;

        // SAFETY: offset is assumed to be within bounds (caller's responsibility)
        Ok(unsafe { ptr.add(offset) })
    }

    /// Get a row pointer from a pinned block using a chunk part.
    ///
    pub fn get_row_pointer(
        &self,
        pin_state: &mut RawRowPinState,
        part: &RawRowChunkPart,
    ) -> Result<*mut u8> {
        self.get_row_pointer_pinned(
            pin_state,
            part.row_block_index as usize,
            part.row_block_offset as usize,
        )
    }

    /// Get a base heap pointer from a pinned block using a chunk part.
    pub fn get_heap_pointer(
        &self,
        pin_state: &mut RawRowPinState,
        part: &RawRowChunkPart,
    ) -> Result<*mut u8> {
        self.get_heap_pointer_pinned(
            pin_state,
            part.heap_block_index as usize,
            part.heap_block_offset as usize,
        )
    }

    /// Release or store handles in the pin state.
    ///
    /// If pin properties are KeepEverythingPinned, handles are moved to the segment.
    /// Otherwise, they are simply cleared (and thus unpinned).
    pub fn release_or_store_handles(
        &self,
        pin_state: &mut RawRowPinState,
        pinned_handles: &Mutex<super::segment::SegmentPinnedHandles>,
    ) {
        match pin_state.properties {
            RawRowPinProperties::KeepEverythingPinned => {
                if let Ok(mut handles) = pinned_handles.lock() {
                    // Move handles from pin_state to segment.
                    for (_, handle) in pin_state.row_handles.handles.drain(..) {
                        handles.row_handles.push(handle);
                    }
                    for (_, handle) in pin_state.heap_handles.handles.drain(..) {
                        handles.heap_handles.push(handle);
                    }
                }
            }
            RawRowPinProperties::DestroyAfterDone => {
                // Emulate DestroyBufferUpon::UNPIN behavior:
                // drop all pins first, then free the corresponding blocks eagerly.
                let mut blocks_to_free = Vec::new();

                for (_, handle) in pin_state.row_handles.handles.drain(..) {
                    if let Some(block) = handle.block_handle() {
                        blocks_to_free.push(block.block_id());
                    }
                    drop(handle);
                }
                for (_, handle) in pin_state.heap_handles.handles.drain(..) {
                    if let Some(block) = handle.block_handle() {
                        blocks_to_free.push(block.block_id());
                    }
                    drop(handle);
                }

                for block_id in blocks_to_free {
                    if self.buffer_pool.free(block_id).is_ok() {
                        self.release_block_accounting(block_id);
                    }
                }
            }
            RawRowPinProperties::UnpinAfterDone
            | RawRowPinProperties::AlreadyPinned
            | RawRowPinProperties::Invalid => {}
        }
        // Always reset the pin state
        pin_state.reset();
    }

    /// Recompute heap pointers when a block has moved (swizzling).
    ///
    /// # Safety
    /// row_locations must contain valid pointers to row data.
    pub unsafe fn recompute_heap_pointers(
        old_heap_ptr: *mut u8,
        new_heap_ptr: *mut u8,
        row_locations: &[*mut u8],
        offset: usize,
        count: usize,
        layout: &RawRowLayout,
        base_col_offset: usize,
    ) {
        let diff = new_heap_ptr.offset_from(old_heap_ptr);
        if diff == 0 {
            return;
        }

        for &col_idx in layout.get_variable_columns() {
            let col_offset = layout.get_offsets()[col_idx];
            let typ = &layout.get_types()[col_idx];

            match typ {
                LogicalType::Varchar | LogicalType::Blob => {
                    Self::recompute_string_heap_pointers(
                        diff,
                        row_locations,
                        offset,
                        count,
                        base_col_offset + col_offset,
                    );
                }
                // TODO: Handle nested types (List, Struct, Array)
                _ => {}
            }
        }
    }

    unsafe fn recompute_string_heap_pointers(
        diff: isize,
        row_locations: &[*mut u8],
        offset: usize,
        count: usize,
        string_location_offset: usize,
    ) {
        for i in 0..count {
            let row_ptr = row_locations[offset + i];
            if row_ptr.is_null() {
                continue;
            }

            let string_location = row_ptr.add(string_location_offset);

            // Read length (first 4 bytes)
            let len_ptr = string_location as *const u32;
            let len = std::ptr::read_unaligned(len_ptr);

            if len > 12 {
                // Heap string: last 8 bytes is the pointer
                // string_t size is 16 bytes.
                // [len:4][prefix:4][ptr:8]
                let ptr_location = string_location.add(8) as *mut *mut u8;
                let current_ptr = std::ptr::read_unaligned(ptr_location);

                let new_target = current_ptr.offset(diff);
                std::ptr::write_unaligned(ptr_location, new_target);
            }
        }
    }

    fn release_blocks(buffer_pool: &Arc<BufferPool>, blocks: &mut Vec<RawRowBlock>) {
        for block in blocks.iter_mut() {
            let Some(handle) = block.handle.take() else {
                continue;
            };

            let block_id = handle.block_id();
            drop(handle);
            if buffer_pool.free(block_id).is_ok() {
                block.release_memory();
            }
        }
        blocks.clear();
    }

    fn release_block_range(
        buffer_pool: &Arc<BufferPool>,
        blocks: &mut [RawRowBlock],
        begin: usize,
        end: usize,
    ) {
        for block in blocks.iter_mut().take(end).skip(begin) {
            let Some(handle) = block.handle.take() else {
                continue;
            };

            let block_id = handle.block_id();
            drop(handle);
            if buffer_pool.free(block_id).is_ok() {
                block.release_memory();
            }
        }
    }

    fn release_block_accounting(&self, block_id: crate::buffer::BlockId) {
        for block in self.row_blocks.iter().chain(self.heap_blocks.iter()) {
            let Some(handle) = &block.handle else {
                continue;
            };
            if handle.block_id() == block_id {
                block.release_memory();
                return;
            }
        }
    }
    /// Initialize chunk state for reading/writing.
    ///
    /// Sets up pointers and handles pinning.
    /// Also handles pointer swizzling if blocks have moved.
    ///
    /// Implements pointer swizzling by recomputing heap pointers when a block
    /// is reloaded at a different address.
    #[allow(clippy::too_many_arguments)]
    pub fn initialize_chunk_state(
        &self,
        pin_state: &mut RawRowPinState,
        chunk_state: &mut RawRowChunkState,
        offset: usize,
        recompute: bool,
        init_heap_pointers: bool,
        init_heap_sizes: bool,
        parts: &[&RawRowChunkPart],
    ) -> Result<()> {
        let mut current_offset = offset;

        // Access row_locations as u64 array (pointers stored as integers)
        let row_locations_slice = unsafe { chunk_state.row_locations.flat_data_mut::<u64>() };

        for part in parts {
            let next_count = part.count as usize;

            // Setup row locations
            let row_width = self.layout.get_row_width();
            let base_row_ptr = self.get_row_pointer(pin_state, part)?;

            for i in 0..next_count {
                // Store pointer as u64
                unsafe {
                    let row_ptr = base_row_ptr.add(i * row_width);
                    *row_locations_slice.add(current_offset + i) = row_ptr as u64;
                }
            }

            if self.layout.all_constant() {
                current_offset += next_count;
                continue;
            }

            // TODO: InitializeHeapSizes support if needed
            if init_heap_sizes {
                // Would initialize heap_sizes vector here
                // Not implemented yet as it's not required for current use cases
            }

            // Recompute row-local heap pointers when the block address changes.
            if part.total_heap_size > 0 {
                // Only recompute if:
                // 1. recompute flag is true (not first initialization)
                // 2. blocks are not already pinned (would have same address)
                if recompute && pin_state.properties != RawRowPinProperties::AlreadyPinned {
                    // Get the new base heap pointer after pinning
                    let new_base_heap_ptr = self.get_heap_pointer(pin_state, part)?;

                    // Acquire lock for thread-safe access to base_heap_ptr
                    let mut _lock = part.lock.lock().unwrap();

                    // Check if pointer changed (block was evicted and reloaded at different address)
                    if part.base_heap_ptr != Some(new_base_heap_ptr) {
                        // Only recompute if we have an old pointer (not first load)
                        if let Some(old_base_heap_ptr) = part.base_heap_ptr {
                            // Convert u64 pointers back to *mut u8 for recompute_heap_pointers
                            let mut row_ptrs: Vec<*mut u8> = Vec::with_capacity(next_count);
                            for i in 0..next_count {
                                unsafe {
                                    let ptr_val = *row_locations_slice.add(current_offset + i);
                                    row_ptrs.push(ptr_val as *mut u8);
                                }
                            }

                            // Recompute all heap pointers in the rows
                            // This updates VARCHAR pointers, LIST pointers, etc.
                            unsafe {
                                Self::recompute_heap_pointers(
                                    old_base_heap_ptr.add(part.heap_block_offset as usize),
                                    new_base_heap_ptr.add(part.heap_block_offset as usize),
                                    &row_ptrs,
                                    current_offset,
                                    next_count,
                                    &self.layout,
                                    0,
                                );
                            }
                        }
                        // Update the base pointer in the part
                        // SAFETY: We have the lock, and we're updating through a raw pointer
                        // because part is passed as & reference but needs mutation
                        let part_ptr = *part as *const RawRowChunkPart;
                        let part_mut = part_ptr as *mut RawRowChunkPart;
                        unsafe {
                            (*part_mut).base_heap_ptr = Some(new_base_heap_ptr);
                        }
                    }
                } else if part.base_heap_ptr.is_none() {
                    // First time loading this part - initialize base_heap_ptr
                    let new_base_heap_ptr = self.get_heap_pointer(pin_state, part)?;
                    let _lock = part.lock.lock().unwrap();
                    let part_ptr = *part as *const RawRowChunkPart;
                    let part_mut = part_ptr as *mut RawRowChunkPart;
                    unsafe {
                        (*part_mut).base_heap_ptr = Some(new_base_heap_ptr);
                    }
                }

                // Initialize heap pointers if requested
                if init_heap_pointers {
                    // Would set up heap_locations vector here
                    // Not implemented yet as it's not required for current use cases
                }
            }

            current_offset += next_count;
        }

        Ok(())
    }

    pub fn release_row_blocks_range(&mut self, begin: usize, end: usize) {
        let end = end.min(self.row_blocks.len());
        if begin >= end {
            return;
        }
        Self::release_block_range(&self.buffer_pool, &mut self.row_blocks, begin, end);
    }

    pub fn release_heap_blocks_range(&mut self, begin: usize, end: usize) {
        let end = end.min(self.heap_blocks.len());
        if begin >= end {
            return;
        }
        Self::release_block_range(&self.buffer_pool, &mut self.heap_blocks, begin, end);
    }
}

impl Drop for RawRowAllocator {
    fn drop(&mut self) {
        Self::release_blocks(&self.buffer_pool, &mut self.row_blocks);
        Self::release_blocks(&self.buffer_pool, &mut self.heap_blocks);
    }
}

/// Result of allocating rows.
#[derive(Debug, Clone)]
pub struct RowAllocation {
    /// Index of the row block
    pub row_block_index: usize,
    /// Offset within the row block
    pub row_block_offset: usize,
    /// Number of rows allocated
    pub count: usize,
    /// Heap allocation info (if any)
    pub heap_info: Option<HeapAllocation>,
}

impl RowAllocation {
    /// Create an empty allocation.
    pub fn empty() -> Self {
        Self {
            row_block_index: 0,
            row_block_offset: 0,
            count: 0,
            heap_info: None,
        }
    }

    /// Check if this allocation is empty.
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }
}

/// Result of allocating heap space.
#[derive(Debug, Clone)]
pub struct HeapAllocation {
    /// Index of the heap block
    pub heap_block_index: usize,
    /// Offset within the heap block
    pub heap_block_offset: usize,
    /// Total heap size allocated
    pub total_heap_size: usize,
    /// Number of rows this heap allocation covers
    pub count: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::row::raw::RawRowValidityType;
    use paro_common::types::LogicalType;

    fn create_test_allocator(types: Vec<LogicalType>) -> RawRowAllocator {
        let pool = Arc::new(BufferPool::new(10 * 1024 * 1024)); // 10 MB
        let mut layout = RawRowLayout::new();
        layout.initialize(types, RawRowValidityType::CanHaveNullValues);
        RawRowAllocator::new(pool, Arc::new(layout), MemoryTag::HashTable)
    }

    #[test]
    fn test_allocator_creation() {
        let allocator = create_test_allocator(vec![LogicalType::Integer, LogicalType::BigInt]);
        assert_eq!(allocator.row_block_count(), 0);
        assert_eq!(allocator.heap_block_count(), 0);
    }

    #[test]
    fn test_create_row_block() {
        let mut allocator = create_test_allocator(vec![LogicalType::Integer]);
        let idx = allocator.create_row_block().unwrap();
        assert_eq!(idx, 0);
        assert_eq!(allocator.row_block_count(), 1);

        let block = allocator.get_row_block(0).unwrap();
        assert!(block.capacity() > 0);
        assert_eq!(block.size(), 0);
    }

    #[test]
    fn test_create_heap_block() {
        let mut allocator = create_test_allocator(vec![LogicalType::Varchar]);
        let idx = allocator.create_heap_block(1024).unwrap();
        assert_eq!(idx, 0);
        assert_eq!(allocator.heap_block_count(), 1);

        let block = allocator.get_heap_block(0).unwrap();
        assert!(block.capacity() >= 1024);
    }

    #[test]
    fn test_allocate_fixed_rows() {
        let mut allocator = create_test_allocator(vec![LogicalType::Integer, LogicalType::BigInt]);
        let row_width = allocator.layout().get_row_width();

        let alloc = allocator.allocate_rows(100, None).unwrap();
        assert_eq!(alloc.count, 100);
        assert_eq!(alloc.row_block_index, 0);
        assert_eq!(alloc.row_block_offset, 0);
        assert!(alloc.heap_info.is_none());

        // Verify block was updated
        let block = allocator.get_row_block(0).unwrap();
        assert_eq!(block.size(), 100 * row_width);
    }

    #[test]
    fn test_allocate_variable_rows() {
        let mut allocator = create_test_allocator(vec![LogicalType::Integer, LogicalType::Varchar]);

        // Allocate with heap sizes
        let heap_sizes = vec![32, 64, 128];
        let alloc = allocator.allocate_rows(3, Some(&heap_sizes)).unwrap();

        assert_eq!(alloc.count, 3);
        assert!(alloc.heap_info.is_some());

        let heap_info = alloc.heap_info.unwrap();
        assert_eq!(heap_info.total_heap_size, 32 + 64 + 128);
        assert_eq!(heap_info.count, 3);
    }

    #[test]
    fn test_get_row_pointer_pinned() {
        let mut allocator = create_test_allocator(vec![LogicalType::Integer]);
        allocator.create_row_block().unwrap();

        let mut pin_state =
            RawRowPinState::new(crate::row::raw::RawRowPinProperties::KeepEverythingPinned);

        let ptr = allocator.get_row_pointer_pinned(&mut pin_state, 0, 0);
        assert!(ptr.is_ok());

        let ptr = allocator.get_row_pointer_pinned(&mut pin_state, 0, 100);
        assert!(ptr.is_ok());

        // Invalid block index
        let ptr = allocator.get_row_pointer_pinned(&mut pin_state, 99, 0);
        assert!(ptr.is_err());
    }

    #[test]
    fn test_multiple_allocations() {
        let mut allocator = create_test_allocator(vec![LogicalType::BigInt]);
        let row_width = allocator.layout().get_row_width();

        // First allocation
        let alloc1 = allocator.allocate_rows(50, None).unwrap();
        assert_eq!(alloc1.row_block_offset, 0);

        // Second allocation should continue in same block
        let alloc2 = allocator.allocate_rows(50, None).unwrap();
        assert_eq!(alloc2.row_block_index, 0);
        assert_eq!(alloc2.row_block_offset, 50 * row_width);

        // Verify total size
        let block = allocator.get_row_block(0).unwrap();
        assert_eq!(block.size(), 100 * row_width);
    }

    #[test]
    fn test_release_or_store_handles_destroy_after_done() {
        let mut allocator = create_test_allocator(vec![LogicalType::Integer]);
        allocator.create_row_block().unwrap();

        let mut pin_state = RawRowPinState::new(RawRowPinProperties::DestroyAfterDone);
        let _ = allocator.pin_row_block(&mut pin_state, 0).unwrap();
        assert_eq!(allocator.buffer_pool().block_count(), 1);

        let pinned_handles = Mutex::new(super::super::segment::SegmentPinnedHandles::default());
        allocator.release_or_store_handles(&mut pin_state, &pinned_handles);

        assert_eq!(allocator.buffer_pool().block_count(), 0);
        assert!(pin_state.row_handles.is_empty());
        assert!(pin_state.heap_handles.is_empty());
    }

    #[test]
    fn test_total_data_size() {
        let mut allocator = create_test_allocator(vec![LogicalType::Integer, LogicalType::Varchar]);

        let heap_sizes = vec![100, 200];
        allocator.allocate_rows(2, Some(&heap_sizes)).unwrap();

        let total = allocator.total_data_size();
        let row_width = allocator.layout().get_row_width();
        assert_eq!(total, 2 * row_width + 300);
    }
}
