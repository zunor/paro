// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Main WAL handle used by transactions and checkpoint coordination.

use crate::wal::wal_entry::WalHeaderMetadata;
use crate::wal::wal_write_state::WalWriteState;
use crate::wal::wal_writer::{WalInitState, WalWriter};
use paro_common::error::{self as paro_error, Result};
use paro_common::logging::targets;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

const MAIN_WAL_EXTENSION: &str = "wal";
const CHECKPOINT_WAL_EXTENSION: &str = "checkpoint.wal";
const RECOVERY_WAL_EXTENSION: &str = "recovery.wal";

fn replace_or_append_wal_suffix(
    main_wal_path: &Path,
    replacement_extension: &str,
    fallback_suffix: &str,
) -> PathBuf {
    let mut derived = main_wal_path.to_path_buf();

    if main_wal_path
        .extension()
        .and_then(|extension| extension.to_str())
        == Some(MAIN_WAL_EXTENSION)
    {
        derived.set_extension(replacement_extension);
    } else {
        let file_name = main_wal_path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("db");
        derived.set_file_name(format!("{}{}", file_name, fallback_suffix));
    }

    derived
}

/// Build checkpoint WAL path from main WAL path (`*.wal` -> `*.checkpoint.wal`).
pub(crate) fn checkpoint_wal_path_from_main(main_wal_path: &Path) -> PathBuf {
    replace_or_append_wal_suffix(main_wal_path, CHECKPOINT_WAL_EXTENSION, ".checkpoint.wal")
}

/// Build recovery WAL path from main WAL path (`*.wal` -> `*.recovery.wal`).
pub(crate) fn recovery_wal_path_from_main(main_wal_path: &Path) -> PathBuf {
    replace_or_append_wal_suffix(main_wal_path, RECOVERY_WAL_EXTENSION, ".recovery.wal")
}

/// Write-Ahead Log for durability.
///
/// The WAL provides durability by logging all changes before they are
/// applied to the database. On crash recovery, the WAL is replayed to
/// restore the database to a consistent state.
///
/// ## Checkpoint Coordination
///
/// During checkpoint, the WAL uses a two-file mechanism:
/// 1. Main WAL (`.wal`) - Contains entries up to the checkpoint
/// 2. Checkpoint WAL (`.checkpoint.wal`) - Receives new entries during checkpoint
///
/// After checkpoint completes:
/// - If checkpoint WAL is empty, main WAL is deleted
/// - If checkpoint WAL has entries, it replaces the main WAL
///
/// # Usage
/// ```ignore
/// let wal = WriteAheadLog::new("data/db.wal")?;
/// // Journal records are appended as binary `JournalRecord` WAL entries
/// // and closed with `WalWriter::flush`.
/// ```
pub struct WriteAheadLog {
    /// The underlying WAL writer
    writer: Arc<WalWriter>,
    /// Path to the WAL file
    path: PathBuf,
    /// Lock for coordinating writes and checkpoint operations
    write_lock: Mutex<()>,
    /// Whether we're currently in checkpoint mode (writing to checkpoint.wal)
    checkpoint_mode: Mutex<bool>,
    /// Dedicated checkpoint WAL writer while checkpoint mode is active.
    checkpoint_writer: Mutex<Option<Arc<WalWriter>>>,
    /// File-level metadata to embed in WAL headers.
    header_metadata: WalHeaderMetadata,
}

impl WriteAheadLog {
    /// Create a new WAL at the specified path.
    ///
    /// If the file exists, it will be opened for appending.
    /// If it doesn't exist, it will be created.
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::new_with_header_metadata(path, WalHeaderMetadata::default())
    }

    /// Create a new WAL with explicit header metadata.
    pub fn new_with_header_metadata<P: AsRef<Path>>(
        path: P,
        header_metadata: WalHeaderMetadata,
    ) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let writer = Arc::new(WalWriter::with_header_metadata(
            &path,
            WalInitState::Uninitialized,
            header_metadata,
        ));

        Ok(Self {
            writer,
            path,
            write_lock: Mutex::new(()),
            checkpoint_mode: Mutex::new(false),
            checkpoint_writer: Mutex::new(None),
            header_metadata,
        })
    }

    /// Create a WAL with a specific initial state.
    pub fn with_state<P: AsRef<Path>>(path: P, state: WalInitState) -> Result<Self> {
        Self::with_state_and_header_metadata(path, state, WalHeaderMetadata::default())
    }

    /// Create a WAL with explicit state and header metadata.
    pub fn with_state_and_header_metadata<P: AsRef<Path>>(
        path: P,
        state: WalInitState,
        header_metadata: WalHeaderMetadata,
    ) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let writer = Arc::new(WalWriter::with_header_metadata(
            &path,
            state,
            header_metadata,
        ));

        Ok(Self {
            writer,
            path,
            write_lock: Mutex::new(()),
            checkpoint_mode: Mutex::new(false),
            checkpoint_writer: Mutex::new(None),
            header_metadata,
        })
    }

    /// Get the path to the checkpoint WAL file.
    fn checkpoint_wal_path(&self) -> PathBuf {
        checkpoint_wal_path_from_main(&self.path)
    }

    /// Start checkpoint mode.
    ///
    /// This is called at the beginning of a checkpoint operation:
    /// 1. Writes a checkpoint marker to the main WAL
    /// 2. Flushes and closes the main WAL
    /// 3. Creates a new checkpoint WAL for concurrent transactions
    ///
    /// # Arguments
    /// * `checkpoint_marker` - Logical checkpoint marker written in metadata and WAL
    ///
    /// # Returns
    /// * `Ok(true)` - Checkpoint mode started, WAL had content
    /// * `Ok(false)` - No WAL content, checkpoint mode not needed
    pub fn start_checkpoint(&self, checkpoint_marker: u64) -> Result<bool> {
        let _lock = self.write_lock.lock().unwrap();

        if self.is_checkpoint_mode() {
            return Err(paro_error::internal(
                "Cannot start checkpoint while checkpoint mode is already active",
            ));
        }

        // Check if WAL has any content
        if self.file_size() == 0 {
            return Ok(false);
        }

        // Write checkpoint marker to main WAL
        self.writer.write_checkpoint_start(checkpoint_marker)?;
        self.writer.flush()?;

        // Remove any existing checkpoint WAL
        let checkpoint_path = self.checkpoint_wal_path();
        if checkpoint_path.exists() {
            fs::remove_file(&checkpoint_path).map_err(|e| {
                paro_error::io_error(format!("Failed to remove old checkpoint WAL: {}", e))
            })?;
        }

        // Switch writes to checkpoint WAL.
        let checkpoint_metadata = WalHeaderMetadata::new(
            self.header_metadata.db_identifier,
            self.header_metadata.checkpoint_iteration.saturating_add(1),
        );
        let checkpoint_writer = Arc::new(WalWriter::with_header_metadata(
            &checkpoint_path,
            WalInitState::NoWal,
            checkpoint_metadata,
        ));
        *self.checkpoint_writer.lock().unwrap() = Some(checkpoint_writer);

        // Mark that we're in checkpoint mode
        *self.checkpoint_mode.lock().unwrap() = true;

        tracing::info!(
            target: targets::CHECKPOINT,
            checkpoint_marker = checkpoint_marker,
            checkpoint_wal_path = %checkpoint_path.display(),
            "Started checkpoint mode"
        );

        Ok(true)
    }

    /// Finish checkpoint mode.
    ///
    /// This is called after a successful checkpoint:
    /// 1. If checkpoint WAL has content, it replaces the main WAL
    /// 2. If checkpoint WAL is empty, the main WAL is deleted
    /// 3. A new main WAL is created for future transactions
    pub fn finish_checkpoint(&self) -> Result<()> {
        let _lock = self.write_lock.lock().unwrap();

        let checkpoint_path = self.checkpoint_wal_path();
        // Leave checkpoint mode and drop active checkpoint writer first.
        *self.checkpoint_mode.lock().unwrap() = false;
        self.checkpoint_writer.lock().unwrap().take();

        // Check if checkpoint WAL exists and has content
        let checkpoint_has_content = checkpoint_path.exists()
            && checkpoint_path
                .metadata()
                .map(|m| m.len() > 0)
                .unwrap_or(false);

        if checkpoint_has_content {
            // Checkpoint WAL has content - replace main WAL with it
            tracing::info!(
                target: targets::CHECKPOINT,
                main_wal_path = %self.path.display(),
                checkpoint_wal_path = %checkpoint_path.display(),
                "Checkpoint WAL has content, replacing main WAL"
            );

            // Remove main WAL
            if self.path.exists() {
                fs::remove_file(&self.path).map_err(|e| {
                    paro_error::io_error(format!("Failed to remove main WAL: {}", e))
                })?;
            }

            // Move checkpoint WAL to main WAL
            fs::rename(&checkpoint_path, &self.path).map_err(|e| {
                paro_error::io_error(format!("Failed to rename checkpoint WAL: {}", e))
            })?;
        } else {
            // Checkpoint WAL is empty - just remove main WAL
            tracing::info!(
                target: targets::CHECKPOINT,
                main_wal_path = %self.path.display(),
                checkpoint_wal_path = %checkpoint_path.display(),
                "Checkpoint WAL is empty, removing main WAL"
            );

            if self.path.exists() {
                fs::remove_file(&self.path).map_err(|e| {
                    paro_error::io_error(format!("Failed to remove main WAL: {}", e))
                })?;
            }

            // Remove checkpoint WAL if it exists
            if checkpoint_path.exists() {
                fs::remove_file(&checkpoint_path).ok();
            }
        }

        tracing::info!(
            target: targets::CHECKPOINT,
            main_wal_path = %self.path.display(),
            checkpoint_wal_path = %checkpoint_path.display(),
            checkpoint_has_content = checkpoint_has_content,
            "Finished checkpoint mode"
        );

        Ok(())
    }

    /// Check if we're currently in checkpoint mode.
    pub fn is_checkpoint_mode(&self) -> bool {
        *self.checkpoint_mode.lock().unwrap()
    }

    /// Get the active WAL writer.
    ///
    /// During checkpoint mode, this returns a writer for the checkpoint WAL.
    /// Otherwise, it returns the main WAL writer.
    fn get_active_writer(&self) -> Arc<WalWriter> {
        if self.is_checkpoint_mode() {
            let mut checkpoint_writer_guard = self.checkpoint_writer.lock().unwrap();
            if let Some(writer) = checkpoint_writer_guard.as_ref() {
                return Arc::clone(writer);
            }

            // Fallback for recoveries/tests where checkpoint_mode may be set manually.
            let checkpoint_path = self.checkpoint_wal_path();
            let checkpoint_metadata = WalHeaderMetadata::new(
                self.header_metadata.db_identifier,
                self.header_metadata.checkpoint_iteration.saturating_add(1),
            );
            let writer = Arc::new(WalWriter::with_header_metadata(
                &checkpoint_path,
                WalInitState::NoWal,
                checkpoint_metadata,
            ));
            *checkpoint_writer_guard = Some(Arc::clone(&writer));
            writer
        } else {
            Arc::clone(&self.writer)
        }
    }

    /// Write a RowsetCommit entry (tablet-level publish of a rowset version).
    pub fn write_rowset_commit(
        &self,
        tablet_id: u64,
        rowset_id: u64,
        start_version: i64,
        end_version: i64,
        rowset_path: &str,
    ) -> Result<()> {
        let writer = self.get_active_writer();
        writer.write_rowset_commit(
            tablet_id,
            rowset_id,
            start_version,
            end_version,
            rowset_path,
        )
    }

    /// Begin a WAL write session.
    ///
    /// Returns a `WalWriteState` that can be used to write WAL entries.
    /// The state tracks the current table context for optimization.
    ///
    /// During checkpoint mode, writes go to the checkpoint WAL.
    pub fn begin_write(&self) -> WalWriteState {
        WalWriteState::new(self.get_active_writer())
    }

    /// Flush and sync the WAL to disk.
    ///
    /// This should be called at transaction commit to ensure durability.
    pub fn flush(&self) -> Result<()> {
        self.get_active_writer().flush()
    }

    /// Write a checkpoint marker.
    ///
    /// Called after a successful checkpoint to mark the WAL as truncatable.
    pub fn write_checkpoint(&self, checkpoint_marker: u64) -> Result<()> {
        self.writer.write_checkpoint(checkpoint_marker)
    }

    /// Truncate the WAL to a specific size.
    ///
    /// Called after checkpoint to reclaim space.
    pub fn truncate(&self, size: u64) -> Result<()> {
        self.writer.truncate(size)
    }

    /// Get the current WAL file size.
    pub fn file_size(&self) -> u64 {
        self.writer.file_size()
    }

    /// Get the total bytes written to the WAL.
    pub fn total_written(&self) -> u64 {
        self.writer.total_written()
    }

    /// Get the WAL file path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Check if the WAL is initialized.
    pub fn is_initialized(&self) -> bool {
        self.writer.is_initialized()
    }

    /// Get the underlying writer (for advanced use).
    pub fn writer(&self) -> &Arc<WalWriter> {
        &self.writer
    }

    /// Get WAL header metadata.
    pub fn header_metadata(&self) -> WalHeaderMetadata {
        self.header_metadata
    }

    /// Get the checkpoint WAL path.
    pub fn get_checkpoint_wal_path(&self) -> PathBuf {
        self.checkpoint_wal_path()
    }

    /// Check if a checkpoint WAL exists.
    pub fn has_checkpoint_wal(&self) -> bool {
        self.checkpoint_wal_path().exists()
    }

    /// Get the recovery WAL path derived from main WAL path.
    pub fn get_recovery_wal_path(&self) -> PathBuf {
        recovery_wal_path_from_main(&self.path)
    }
}

impl std::fmt::Debug for WriteAheadLog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WriteAheadLog")
            .field("path", &self.path)
            .field("initialized", &self.is_initialized())
            .field("file_size", &self.file_size())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::test_support::write_flushed_create_schema_txn;
    use tempfile::tempdir;

    #[test]
    fn test_write_ahead_log_create() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.wal");

        let wal = WriteAheadLog::new(&path).unwrap();
        assert!(!wal.is_initialized()); // Not initialized until first write
        assert_eq!(wal.file_size(), 0);
    }

    #[test]
    fn test_write_ahead_log_write() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.wal");

        let wal = WriteAheadLog::new(&path).unwrap();

        write_flushed_create_schema_txn(wal.writer().as_ref(), "default", "test_schema", 1, 100)
            .unwrap();

        assert!(wal.is_initialized());
        assert!(wal.file_size() > 0);
    }

    #[test]
    fn test_write_ahead_log_checkpoint() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.wal");

        let wal = WriteAheadLog::new(&path).unwrap();

        write_flushed_create_schema_txn(wal.writer().as_ref(), "default", "test_schema", 1, 100)
            .unwrap();

        // Write checkpoint
        wal.write_checkpoint(42).unwrap();
        wal.flush().unwrap();

        assert!(wal.file_size() > 0);
    }

    #[test]
    fn test_checkpoint_wal_path() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.wal");

        let wal = WriteAheadLog::new(&path).unwrap();
        let checkpoint_path = wal.get_checkpoint_wal_path();

        assert_eq!(checkpoint_path, dir.path().join("test.checkpoint.wal"));
    }

    #[test]
    fn test_recovery_wal_path() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.wal");

        let wal = WriteAheadLog::new(&path).unwrap();
        let recovery_path = wal.get_recovery_wal_path();

        assert_eq!(recovery_path, dir.path().join("test.recovery.wal"));
    }

    #[test]
    fn test_start_checkpoint_empty_wal() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.wal");

        let wal = WriteAheadLog::new(&path).unwrap();

        // Start checkpoint on empty WAL should return false
        let result = wal.start_checkpoint(42).unwrap();
        assert!(!result);
        assert!(!wal.is_checkpoint_mode());
    }

    #[test]
    fn test_start_checkpoint_with_content() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.wal");

        let wal = WriteAheadLog::new(&path).unwrap();

        write_flushed_create_schema_txn(wal.writer().as_ref(), "default", "test_schema", 1, 100)
            .unwrap();

        // Start checkpoint should succeed
        let result = wal.start_checkpoint(42).unwrap();
        assert!(result);
        assert!(wal.is_checkpoint_mode());
    }

    #[test]
    fn test_finish_checkpoint_empty() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.wal");

        let wal = WriteAheadLog::new(&path).unwrap();

        write_flushed_create_schema_txn(wal.writer().as_ref(), "default", "test_schema", 1, 100)
            .unwrap();

        let size_before = wal.file_size();
        assert!(size_before > 0);

        // Start and finish checkpoint (no concurrent writes)
        wal.start_checkpoint(42).unwrap();
        wal.finish_checkpoint().unwrap();

        // Main WAL should be removed (empty checkpoint WAL)
        assert!(!path.exists());
        assert!(!wal.is_checkpoint_mode());
    }

    #[test]
    fn test_checkpoint_mode_writes() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.wal");

        let wal = WriteAheadLog::new(&path).unwrap();

        write_flushed_create_schema_txn(wal.writer().as_ref(), "default", "schema1", 1, 100)
            .unwrap();

        // Start checkpoint
        wal.start_checkpoint(42).unwrap();
        assert!(wal.is_checkpoint_mode());

        let cp_session = wal.begin_write();
        let cp_writer = cp_session.wal().as_ref();
        write_flushed_create_schema_txn(cp_writer, "default", "schema2", 2, 101).unwrap();

        // Checkpoint WAL should exist
        assert!(wal.has_checkpoint_wal());

        // Finish checkpoint
        wal.finish_checkpoint().unwrap();

        // Main WAL should now contain the checkpoint WAL content
        assert!(path.exists());
        assert!(!wal.is_checkpoint_mode());
    }

    #[test]
    fn test_checkpoint_mode_flush_uses_checkpoint_wal() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.wal");

        let wal = WriteAheadLog::new(&path).unwrap();

        write_flushed_create_schema_txn(wal.writer().as_ref(), "default", "schema1", 1, 100)
            .unwrap();

        wal.start_checkpoint(42).unwrap();
        let main_size_before = std::fs::metadata(&path).unwrap().len();

        // Flush while checkpoint mode is active should target checkpoint WAL.
        wal.flush().unwrap();

        let main_size_after = std::fs::metadata(&path).unwrap().len();
        assert_eq!(main_size_after, main_size_before);
        assert!(wal.has_checkpoint_wal());
    }
}
