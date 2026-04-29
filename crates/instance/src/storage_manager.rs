// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Storage Manager - Abstract Storage Interface
//!
//! StorageManager is responsible for managing the physical storage of a database.
//! It encapsulates:
//! - Write-ahead log (WAL) for durability
//! - Metadata/tablet management

use paro_common::error::{self as paro_error, Result};
use paro_common::logging::targets;
use paro_journal::wal::wal_entry::WalHeaderMetadata;
use paro_journal::wal::write_ahead_log::WriteAheadLog;
use paro_storage::meta::{MetadataStore, TabletMetaManager};
use std::sync::Arc;

/// Suffix for the main WAL file.
pub const MAIN_WAL_SUFFIX: &str = ".wal";
/// Build a WAL-related file path from a database path and a suffix.
///
pub fn wal_path_with_suffix(db_path: &str, suffix: &str) -> String {
    let question_mark_pos = if db_path.starts_with("\\\\?\\") {
        None
    } else {
        db_path.find('?')
    };

    if let Some(pos) = question_mark_pos {
        let mut result = db_path.to_string();
        result.insert_str(pos, suffix);
        result
    } else {
        format!("{}{}", db_path, suffix)
    }
}

/// Storage commit state for managing transaction commits.
///
///
/// This trait manages the commit state for storage operations.
/// Destruction without calling `flush_commit()` will roll back changes.
pub trait StorageCommitState: Send + Sync {
    /// Revert the commit.
    ///
    fn revert_commit(&mut self);

    /// Make the commit persistent.
    ///
    fn flush_commit(&mut self) -> Result<()>;

    /// Check if there is any data to commit.
    fn has_data(&self) -> bool {
        false
    }
}

/// Single file storage commit state implementation.
///
/// This is a simplified implementation that tracks whether a commit
/// has been flushed to prevent rollback on drop.
pub struct SingleFileStorageCommitState {
    /// Whether the commit has been flushed
    flushed: bool,
    /// WAL reference for writing commit records
    wal: Option<Arc<WriteAheadLog>>,
    /// Transaction ID
    transaction_id: u64,
}

impl SingleFileStorageCommitState {
    /// Create a new storage commit state.
    pub fn new(wal: Option<Arc<WriteAheadLog>>, transaction_id: u64) -> Self {
        Self {
            flushed: false,
            wal,
            transaction_id,
        }
    }
}

impl StorageCommitState for SingleFileStorageCommitState {
    fn revert_commit(&mut self) {
        if self.flushed {
            tracing::warn!(
                target: targets::STORAGE,
                transaction_id = %self.transaction_id,
                "Attempted to revert already flushed commit"
            );
            return;
        }

        // Mark as reverted (no flush will happen)
        self.flushed = true;
    }

    fn flush_commit(&mut self) -> Result<()> {
        if self.flushed {
            return Ok(());
        }

        // If we have a WAL, flush it
        if let Some(wal) = &self.wal {
            wal.flush()?;
        }

        self.flushed = true;
        Ok(())
    }

    fn has_data(&self) -> bool {
        self.wal.is_some()
    }
}

impl Drop for SingleFileStorageCommitState {
    fn drop(&mut self) {
        if !self.flushed {
            tracing::warn!(
                target: targets::STORAGE,
                transaction_id = %self.transaction_id,
                "StorageCommitState dropped without flush - rolling back"
            );
            self.revert_commit();
        }
    }
}

/// Database size information.
///
#[derive(Debug, Clone, Default)]
pub struct DatabaseSize {
    /// Total bytes used
    pub bytes: u64,
    /// Block size
    pub block_size: u64,
    /// Total blocks
    pub total_blocks: u64,
    /// Used blocks
    pub used_blocks: u64,
    /// Free blocks
    pub free_blocks: u64,
    /// WAL size in bytes
    pub wal_size: u64,
}

/// Metadata block information.
#[derive(Debug, Clone)]
pub struct MetadataBlockInfo {
    /// Block ID
    pub block_id: u64,
    /// Block type
    pub block_type: String,
    /// Number of entries
    pub entry_count: u64,
}

/// Storage manager trait for managing database storage.
///
///
/// This trait encapsulates:
/// - Write-ahead log (WAL)
/// - Metadata / tablet management
pub trait StorageManager: Send + Sync + std::fmt::Debug {
    /// Get the database path.
    ///
    fn get_path(&self) -> &str;

    /// Check if the database is in-memory.
    ///
    fn in_memory(&self) -> bool;

    /// Check if the database is read-only.
    fn is_read_only(&self) -> bool;

    /// Check if loading is complete.
    ///
    fn is_loaded(&self) -> bool;

    // --- WAL Methods ---

    /// Get the WAL, if any.
    ///
    fn get_wal(&self) -> Option<&WriteAheadLog>;

    /// Get mutable reference to WAL, if any.
    fn get_wal_mut(&mut self) -> Option<&mut WriteAheadLog>;

    /// Get the WAL as Arc (for sharing).
    fn get_wal_arc(&self) -> Option<Arc<WriteAheadLog>>;

    /// Get the durable WAL header metadata associated with the active WAL stream.
    fn wal_header_metadata(&self) -> Option<WalHeaderMetadata> {
        self.get_wal_arc().map(|wal| wal.header_metadata())
    }

    /// Replace the active WAL handle with a recovered instance.
    ///
    /// Recovery returns a WAL with the correct post-replay init state
    /// (e.g. requiring truncation on first write after torn tail detection).
    /// Storage managers that keep an internal WAL handle should adopt it here.
    fn replace_wal(&mut self, _wal: Arc<WriteAheadLog>) -> Result<()> {
        Err(paro_error::internal(
            "StorageManager does not support WAL handle replacement",
        ))
    }

    /// Check if WAL exists.
    ///
    fn has_wal(&self) -> bool {
        self.get_wal().is_some()
    }

    /// Get the WAL size in bytes.
    ///
    fn wal_size(&self) -> u64;

    /// Add to the estimated WAL size.
    ///
    fn add_wal_size(&self, size: u64);

    /// Set the WAL size.
    ///
    fn set_wal_size(&self, size: u64);

    /// Retention lower bound for WAL truncation/reclamation.
    ///
    /// Default `u64::MAX` means "no retention pin".
    fn wal_keep_from(&self) -> u64 {
        u64::MAX
    }

    /// Update retention lower bound for WAL truncation/reclamation.
    fn set_wal_keep_from(&self, _keep_from: u64) {}

    /// Get the WAL path.
    ///
    fn get_wal_path(&self) -> String {
        wal_path_with_suffix(self.get_path(), MAIN_WAL_SUFFIX)
    }

    /// Get metadata store provider used by catalog/tablet metadata paths.
    fn get_metadata_store(&self) -> Option<&dyn MetadataStore>;

    /// Get metadata store provider as Arc.
    fn get_metadata_store_arc(&self) -> Option<Arc<dyn MetadataStore>>;

    /// Get tablet metadata manager.
    fn get_tablet_meta_manager(&self) -> Option<Arc<TabletMetaManager>>;

    /// Generate a storage commit state for a transaction.
    ///
    fn gen_storage_commit_state(&self, transaction_id: u64) -> Box<dyn StorageCommitState>;

    /// Get database size information.
    ///
    fn get_database_size(&self) -> DatabaseSize;

    /// Get metadata info.
    ///
    fn get_metadata_info(&self) -> Vec<MetadataBlockInfo>;

    /// Initialize the storage manager.
    ///
    fn initialize(&mut self) -> Result<()>;

    /// Destroy the storage manager and clean up resources.
    ///
    fn destroy(&mut self);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_database_size_default() {
        let size = DatabaseSize::default();
        assert_eq!(size.bytes, 0);
        assert_eq!(size.wal_size, 0);
    }

    #[test]
    fn test_storage_commit_state_flush() {
        let mut state = SingleFileStorageCommitState::new(None, 1);
        assert!(!state.has_data());

        // Flush should succeed
        assert!(state.flush_commit().is_ok());

        // Second flush should be no-op
        assert!(state.flush_commit().is_ok());
    }

    #[test]
    fn test_storage_commit_state_revert() {
        let mut state = SingleFileStorageCommitState::new(None, 2);

        // Revert the commit
        state.revert_commit();

        // Flush after revert should be no-op
        assert!(state.flush_commit().is_ok());
    }

    #[test]
    fn test_storage_commit_state_drop_without_flush() {
        // This test verifies that dropping without flush triggers revert
        let state = SingleFileStorageCommitState::new(None, 3);
        // Drop happens here - should log warning and revert
        drop(state);
    }

    #[test]
    fn test_wal_path_with_suffix_plain_path() {
        let path = wal_path_with_suffix("/tmp/test.db", MAIN_WAL_SUFFIX);
        assert_eq!(path, "/tmp/test.db.wal");
    }

    #[test]
    fn test_wal_path_with_suffix_query_path() {
        let path = wal_path_with_suffix("/tmp/test.db?token=abc", MAIN_WAL_SUFFIX);
        assert_eq!(path, "/tmp/test.db.wal?token=abc");
    }
}
