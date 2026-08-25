// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! FileBufferType - Buffer type classification for eviction priority.

/// Buffer type determines eviction priority and handling.
///
/// - BLOCK and EXTERNAL_FILE: Cheap to evict (just free memory)
/// - MANAGED_BUFFER: Must write to disk before eviction
/// - TINY_BUFFER: Last resort (small allocations)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[repr(u8)]
pub enum FileBufferType {
    /// Persistent block (cheap to evict - just free memory)
    /// Eviction priority: HIGHEST (evict first)
    Block = 1,

    /// Temporary managed buffer (must write to disk before evict)
    /// Eviction priority: MEDIUM
    /// Supports eviction_queue_idx for fine-grained priority control
    #[default]
    ManagedBuffer = 2,

    /// Tiny buffer (last resort eviction)
    /// Eviction priority: LOWEST (evict last)
    TinyBuffer = 3,

    /// External file block (cheap to evict - just free memory)
    /// Eviction priority: HIGHEST (same as Block)
    ExternalFile = 4,

    /// Reconstructible query/build scratch memory.
    ///
    /// Scratch blocks are discarded under pressure and recreated as zeroed
    /// memory when pinned again. They must never be spilled: their contents
    /// have no value outside the borrower that currently pins the block.
    Scratch = 5,
}

impl FileBufferType {
    /// Get eviction queue type index (0 = highest priority).
    ///
    /// Maps FileBufferType to eviction queue type:
    /// - 0: BLOCK and EXTERNAL_FILE (evict first)
    /// - 1: MANAGED_BUFFER (evict second)
    /// - 2: TINY_BUFFER (evict last)
    pub fn eviction_queue_type_idx(self) -> usize {
        match self {
            FileBufferType::Block | FileBufferType::ExternalFile | FileBufferType::Scratch => 0,
            FileBufferType::ManagedBuffer => 1,
            FileBufferType::TinyBuffer => 2,
        }
    }

    /// Check if this buffer type needs to be written to disk before eviction
    pub fn must_write_to_disk(self) -> bool {
        matches!(self, FileBufferType::ManagedBuffer)
    }

    /// Whether an evicted block can be reconstructed without durable bytes.
    pub fn is_reconstructible(self) -> bool {
        matches!(self, FileBufferType::Scratch)
    }

    /// Check if this buffer type supports eviction queue index
    ///
    /// Only MANAGED_BUFFER supports fine-grained priority control via eviction_queue_idx
    pub fn supports_eviction_queue_idx(self) -> bool {
        matches!(self, FileBufferType::ManagedBuffer)
    }

    /// Get all buffer types for a given queue type index.
    pub fn from_queue_type_idx(queue_type_idx: usize) -> &'static [FileBufferType] {
        match queue_type_idx {
            0 => &[
                FileBufferType::Block,
                FileBufferType::ExternalFile,
                FileBufferType::Scratch,
            ],
            1 => &[FileBufferType::ManagedBuffer],
            2 => &[FileBufferType::TinyBuffer],
            _ => panic!("Invalid queue type index: {}", queue_type_idx),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_eviction_priority() {
        // Block and ExternalFile have highest priority (evict first)
        assert_eq!(FileBufferType::Block.eviction_queue_type_idx(), 0);
        assert_eq!(FileBufferType::ExternalFile.eviction_queue_type_idx(), 0);
        assert_eq!(FileBufferType::Scratch.eviction_queue_type_idx(), 0);

        // ManagedBuffer has medium priority
        assert_eq!(FileBufferType::ManagedBuffer.eviction_queue_type_idx(), 1);

        // TinyBuffer has lowest priority (evict last)
        assert_eq!(FileBufferType::TinyBuffer.eviction_queue_type_idx(), 2);
    }

    #[test]
    fn test_write_to_disk() {
        assert!(!FileBufferType::Block.must_write_to_disk());
        assert!(FileBufferType::ManagedBuffer.must_write_to_disk());
        assert!(!FileBufferType::TinyBuffer.must_write_to_disk());
        assert!(!FileBufferType::ExternalFile.must_write_to_disk());
        assert!(!FileBufferType::Scratch.must_write_to_disk());
    }

    #[test]
    fn test_eviction_queue_idx_support() {
        assert!(!FileBufferType::Block.supports_eviction_queue_idx());
        assert!(FileBufferType::ManagedBuffer.supports_eviction_queue_idx());
        assert!(!FileBufferType::TinyBuffer.supports_eviction_queue_idx());
        assert!(!FileBufferType::ExternalFile.supports_eviction_queue_idx());
        assert!(!FileBufferType::Scratch.supports_eviction_queue_idx());
    }

    #[test]
    fn test_from_queue_type_idx() {
        let types_0 = FileBufferType::from_queue_type_idx(0);
        assert_eq!(types_0.len(), 3);
        assert!(types_0.contains(&FileBufferType::Block));
        assert!(types_0.contains(&FileBufferType::ExternalFile));
        assert!(types_0.contains(&FileBufferType::Scratch));

        let types_1 = FileBufferType::from_queue_type_idx(1);
        assert_eq!(types_1.len(), 1);
        assert_eq!(types_1[0], FileBufferType::ManagedBuffer);

        let types_2 = FileBufferType::from_queue_type_idx(2);
        assert_eq!(types_2.len(), 1);
        assert_eq!(types_2[0], FileBufferType::TinyBuffer);
    }

    #[test]
    fn test_default() {
        assert_eq!(FileBufferType::default(), FileBufferType::ManagedBuffer);
    }

    #[test]
    fn test_enum_values() {
        // Verify enum values
        assert_eq!(FileBufferType::Block as u8, 1);
        assert_eq!(FileBufferType::ManagedBuffer as u8, 2);
        assert_eq!(FileBufferType::TinyBuffer as u8, 3);
        assert_eq!(FileBufferType::ExternalFile as u8, 4);
        assert_eq!(FileBufferType::Scratch as u8, 5);
    }
}
