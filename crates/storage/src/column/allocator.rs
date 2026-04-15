//! ColumnDataAllocator - allocation/pinning helpers for column data storage.

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Arc;

use crate::buffer::{BlockId, BufferHandle, BufferPool, FileBufferType, MemoryTag};
use paro_common::error::{self as paro_error, Result};

/// Allocation mode for [`ColumnDataAllocator`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnDataAllocatorType {
    /// Allocate via BufferPool managed blocks.
    BufferManagerAllocator,
    /// Allocate purely in-memory objects.
    InMemoryAllocator,
    /// Allocate vectors via BufferPool but allow in-memory shortcuts for small data.
    Hybrid,
}

/// Per-scan chunk management state.
///
/// Holds pinned block handles and releases them automatically on drop.
#[derive(Debug, Default)]
pub struct ChunkManagementState {
    pub handles: HashMap<BlockId, BufferHandle>,
}

impl ChunkManagementState {
    pub fn new() -> Self {
        Self {
            handles: HashMap::new(),
        }
    }

    pub fn clear(&mut self) {
        self.handles.clear();
    }
}

/// Allocator facade for column data collections.
#[derive(Debug)]
pub struct ColumnDataAllocator {
    allocator_type: ColumnDataAllocatorType,
    buffer_pool: Option<Arc<BufferPool>>,
    memory_tag: MemoryTag,
}

impl ColumnDataAllocator {
    pub fn in_memory() -> Self {
        Self {
            allocator_type: ColumnDataAllocatorType::InMemoryAllocator,
            buffer_pool: None,
            memory_tag: MemoryTag::ColumnData,
        }
    }

    pub fn buffer_manager(buffer_pool: Arc<BufferPool>, memory_tag: MemoryTag) -> Self {
        Self {
            allocator_type: ColumnDataAllocatorType::BufferManagerAllocator,
            buffer_pool: Some(buffer_pool),
            memory_tag,
        }
    }

    pub fn hybrid(buffer_pool: Arc<BufferPool>, memory_tag: MemoryTag) -> Self {
        Self {
            allocator_type: ColumnDataAllocatorType::Hybrid,
            buffer_pool: Some(buffer_pool),
            memory_tag,
        }
    }

    pub fn allocator_type(&self) -> ColumnDataAllocatorType {
        self.allocator_type
    }

    pub fn memory_tag(&self) -> MemoryTag {
        self.memory_tag
    }

    pub fn buffer_pool(&self) -> Option<&Arc<BufferPool>> {
        self.buffer_pool.as_ref()
    }

    pub fn is_buffer_managed(&self) -> bool {
        !matches!(
            self.allocator_type,
            ColumnDataAllocatorType::InMemoryAllocator
        )
    }

    pub fn allocate_serialized_chunk(&self, bytes: &[u8]) -> Result<BlockId> {
        if !self.is_buffer_managed() {
            return Err(paro_error::invalid_input(
                "allocate_serialized_chunk requires a buffer-managed allocator",
            ));
        }
        let pool = self.buffer_pool.as_ref().ok_or_else(|| {
            paro_error::internal("buffer-managed allocator missing BufferPool".to_string())
        })?;

        let handle = pool.allocate(
            self.memory_tag,
            FileBufferType::ManagedBuffer,
            bytes.len().max(1),
        )?;
        let block_id = handle
            .block_handle()
            .ok_or_else(|| paro_error::internal("buffer handle missing block handle".to_string()))?
            .block_id();

        let dst = unsafe {
            handle
                .data_mut()
                .ok_or_else(|| paro_error::internal("failed to access block bytes".to_string()))?
        };
        dst[..bytes.len()].copy_from_slice(bytes);
        if dst.len() > bytes.len() {
            dst[bytes.len()..].fill(0);
        }

        drop(handle);
        Ok(block_id)
    }

    pub fn pin_block<'a>(
        &self,
        block_id: BlockId,
        state: &'a mut ChunkManagementState,
    ) -> Result<&'a BufferHandle> {
        if !self.is_buffer_managed() {
            return Err(paro_error::invalid_input(
                "pin_block requires a buffer-managed allocator",
            ));
        }

        if let Entry::Vacant(e) = state.handles.entry(block_id) {
            let pool = self.buffer_pool.as_ref().ok_or_else(|| {
                paro_error::internal("buffer-managed allocator missing BufferPool".to_string())
            })?;
            let handle = pool.pin(block_id)?;
            e.insert(handle);
        }

        state.handles.get(&block_id).ok_or_else(|| {
            paro_error::internal(format!(
                "pinned block handle {} missing from state",
                block_id
            ))
        })
    }

    pub fn read_block_bytes(
        &self,
        block_id: BlockId,
        byte_len: usize,
        state: &mut ChunkManagementState,
    ) -> Result<Vec<u8>> {
        let handle = self.pin_block(block_id, state)?;
        let data = handle
            .data()
            .ok_or_else(|| paro_error::internal("failed to read pinned block bytes".to_string()))?;
        if byte_len > data.len() {
            return Err(paro_error::internal(format!(
                "serialized chunk length {} exceeds block size {}",
                byte_len,
                data.len()
            )));
        }
        Ok(data[..byte_len].to_vec())
    }

    pub fn free_block(&self, block_id: BlockId) -> Result<()> {
        if let Some(pool) = &self.buffer_pool {
            pool.free(block_id)?;
        }
        Ok(())
    }
}
