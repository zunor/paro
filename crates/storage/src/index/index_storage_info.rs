//! # Index Storage Info
//!
//! Storage information for serializing indexes to disk.
//!
//! ## Design
//! - IndexStorageInfo contains all information needed to serialize/deserialize an index
//! - For ART: root node pointer, allocator infos, and options
//! - Supports both checkpoint (disk) and WAL serialization

use std::collections::HashMap;

use paro_common::runtime_value::Value;

use super::fixed_size_allocator::FixedSizeAllocatorInfo;
use super::fixed_size_buffer::BlockPointer;
use super::IndexPointer;
use crate::buffer::BlockId;

/// Information about index storage for serialization.
///
/// This structure contains all the information needed to serialize
/// an index to disk and restore it later.
#[derive(Debug, Clone, Default)]
pub struct IndexStorageInfo {
    /// Name of the index
    pub name: String,

    /// Root node pointer (for ART: the tree root)
    pub root: IndexPointer,

    /// Root block pointer (for backwards compatibility with older storage format)
    pub root_block_ptr: BlockPointer,

    /// Allocator information for each node type
    pub allocator_infos: Vec<FixedSizeAllocatorInfo>,

    /// Buffer data for WAL serialization
    pub buffers: Vec<Vec<IndexBufferInfo>>,

    /// Index-specific options
    pub options: HashMap<String, Value>,

    /// Whether the index is valid and can be used
    pub is_valid: bool,
}

/// Information about a buffer for WAL serialization.
#[derive(Debug, Clone)]
pub struct IndexBufferInfo {
    /// Buffer data.
    pub data: Vec<u8>,
    /// Size of the buffer data.
    pub size: usize,
}

impl IndexStorageInfo {
    /// Creates a new empty IndexStorageInfo.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            root: IndexPointer::new(),
            root_block_ptr: BlockPointer::invalid(),
            allocator_infos: Vec::new(),
            buffers: Vec::new(),
            options: HashMap::new(),
            is_valid: true,
        }
    }

    /// Creates an invalid IndexStorageInfo (for indexes that failed to serialize).
    pub fn invalid(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            root: IndexPointer::new(),
            root_block_ptr: BlockPointer::invalid(),
            allocator_infos: Vec::new(),
            buffers: Vec::new(),
            options: HashMap::new(),
            is_valid: false,
        }
    }

    /// Returns true if this storage info is valid.
    pub fn is_valid(&self) -> bool {
        self.is_valid
    }

    /// Sets the root node pointer.
    pub fn with_root(mut self, root: IndexPointer) -> Self {
        self.root = root;
        self
    }

    /// Sets the root block pointer (for backwards compatibility).
    pub fn with_root_block_ptr(mut self, ptr: BlockPointer) -> Self {
        self.root_block_ptr = ptr;
        self
    }

    /// Adds allocator info.
    pub fn with_allocator_info(mut self, info: FixedSizeAllocatorInfo) -> Self {
        self.allocator_infos.push(info);
        self
    }

    /// Adds buffer data for WAL serialization.
    pub fn with_buffers(mut self, buffers: Vec<IndexBufferInfo>) -> Self {
        self.buffers.push(buffers);
        self
    }

    /// Sets an option value.
    pub fn with_option(mut self, key: impl Into<String>, value: Value) -> Self {
        self.options.insert(key.into(), value);
        self
    }

    /// Returns the root block ID (for backwards compatibility).
    pub fn root_block(&self) -> Option<BlockId> {
        if self.root_block_ptr.is_valid() {
            Some(self.root_block_ptr.block_id as BlockId)
        } else {
            None
        }
    }

    /// Returns allocator block IDs (for backwards compatibility).
    pub fn allocator_block_ids(&self) -> Vec<BlockId> {
        self.allocator_infos
            .iter()
            .flat_map(|info| {
                info.block_pointers
                    .iter()
                    .filter(|bp| bp.is_valid())
                    .map(|bp| bp.block_id as BlockId)
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_storage_info() {
        let info = IndexStorageInfo::new("test_index");
        assert_eq!(info.name, "test_index");
        assert!(info.root_block().is_none());
        assert!(info.allocator_block_ids().is_empty());
        assert!(info.is_valid());
    }

    #[test]
    fn test_invalid_storage_info() {
        let info = IndexStorageInfo::invalid("failed_index");
        assert_eq!(info.name, "failed_index");
        assert!(!info.is_valid());
    }

    #[test]
    fn test_builder_pattern() {
        let info = IndexStorageInfo::new("my_index")
            .with_root_block_ptr(BlockPointer::new(42, 0))
            .with_option("compression", Value::Varchar("lz4".to_string()));

        assert_eq!(info.name, "my_index");
        assert_eq!(info.root_block(), Some(42));
        assert!(info.options.contains_key("compression"));
    }
}
