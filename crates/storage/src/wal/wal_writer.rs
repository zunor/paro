//! Buffered WAL file writing with checksums and checkpoint-aware file handling.

use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use crate::compaction::publish::record::CompactionPublishRecord;
use crate::wal::checksum::compute_wal_checksum;
use crate::wal::wal_entry::WalHeaderMetadata;
use crate::wal::wal_type::WalType;
use paro_common::error::{self as paro_error, Result};
pub const WAL_BUFFER_SIZE: usize = 4096;

/// WAL version number.
pub const WAL_VERSION_NUMBER: u64 = 3;

/// Buffered file writer for WAL operations.
///
/// Provides buffered I/O with explicit flush and sync operations.
/// All writes are accumulated in memory until flush is called.
pub(crate) struct BufferedFileWriter {
    /// Path to the WAL file
    #[allow(dead_code)]
    path: PathBuf,
    /// Buffered writer wrapping the file
    writer: BufWriter<File>,
    /// Current file size
    file_size: u64,
    /// Total bytes written since creation
    total_written: AtomicU64,
}

impl BufferedFileWriter {
    /// Create a new buffered file writer.
    ///
    /// Opens the file in append mode, creating it if it doesn't exist.
    ///
    /// # Arguments
    /// * `path` - Path to the WAL file
    ///
    /// # Returns
    /// A new BufferedFileWriter instance
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref().to_path_buf();

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(|e| paro_error::io_error(format!("Failed to open WAL file: {}", e)))?;

        let file_size = file
            .metadata()
            .map_err(|e| paro_error::io_error(format!("Failed to get file metadata: {}", e)))?
            .len();

        Ok(Self {
            path,
            writer: BufWriter::with_capacity(WAL_BUFFER_SIZE, file),
            file_size,
            total_written: AtomicU64::new(0),
        })
    }

    /// Write data to the buffer.
    pub fn write_data(&mut self, data: &[u8]) -> Result<()> {
        self.writer
            .write_all(data)
            .map_err(|e| paro_error::io_error(format!("WAL write failed: {}", e)))?;
        self.total_written
            .fetch_add(data.len() as u64, Ordering::Relaxed);
        Ok(())
    }

    /// Write a u64 value in little-endian format.
    pub fn write_u64(&mut self, value: u64) -> Result<()> {
        self.write_data(&value.to_le_bytes())
    }

    /// Write a u32 value in little-endian format.
    #[allow(dead_code)]
    pub fn write_u32(&mut self, value: u32) -> Result<()> {
        self.write_data(&value.to_le_bytes())
    }

    /// Write a u8 value.
    pub fn write_u8(&mut self, value: u8) -> Result<()> {
        self.write_data(&[value])
    }

    /// Flush the buffer to the file (without sync).
    pub fn flush(&mut self) -> Result<()> {
        self.writer
            .flush()
            .map_err(|e| paro_error::io_error(format!("WAL flush failed: {}", e)))?;
        // Update file size after flush
        if let Ok(metadata) = self.writer.get_ref().metadata() {
            self.file_size = metadata.len();
        }
        Ok(())
    }

    /// Flush and sync the file to disk.
    pub fn sync(&mut self) -> Result<()> {
        self.flush()?;
        self.writer
            .get_ref()
            .sync_all()
            .map_err(|e| paro_error::io_error(format!("WAL sync failed: {}", e)))?;
        Ok(())
    }

    /// Get the current file size.
    pub fn file_size(&self) -> u64 {
        self.file_size
    }

    /// Get total bytes written since creation.
    pub fn total_written(&self) -> u64 {
        self.total_written.load(Ordering::Relaxed)
    }

    /// Get the file path.
    #[allow(dead_code)]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Truncate the file to a specific size.
    pub fn truncate(&mut self, size: u64) -> Result<()> {
        self.flush()?;

        let file = self.writer.get_mut();
        file.set_len(size)
            .map_err(|e| paro_error::io_error(format!("WAL truncate failed: {}", e)))?;
        file.seek(SeekFrom::End(0))
            .map_err(|e| paro_error::io_error(format!("WAL seek failed: {}", e)))?;

        self.file_size = size;
        Ok(())
    }
}

/// WAL initialization state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalInitState {
    /// No WAL file
    NoWal,
    /// WAL exists but not initialized
    Uninitialized,
    /// WAL needs truncation before use
    UninitializedRequiresTruncate,
    /// WAL is fully initialized
    Initialized,
}

/// Write-Ahead Log writer.
///
/// Handles WAL entry serialization with checksum computation.
/// Each entry is written with:
/// 1. Entry size (u64)
/// 2. Checksum (u64)
/// 3. Entry data (variable length)
pub struct WalWriter {
    /// Underlying buffered file writer
    writer: Mutex<Option<BufferedFileWriter>>,
    /// Path to the WAL file
    wal_path: PathBuf,
    /// Current initialization state
    init_state: Mutex<WalInitState>,
    /// Whether header has been written
    header_written: Mutex<bool>,
    /// File-level metadata written in WAL header.
    header_metadata: WalHeaderMetadata,
}

impl WalWriter {
    /// Create a new WAL writer.
    ///
    /// # Arguments
    /// * `wal_path` - Path to the WAL file
    /// * `init_state` - Initial state of the WAL
    pub fn new<P: AsRef<Path>>(wal_path: P, init_state: WalInitState) -> Self {
        Self::with_header_metadata(wal_path, init_state, WalHeaderMetadata::default())
    }

    /// Create a WAL writer with explicit header metadata.
    pub fn with_header_metadata<P: AsRef<Path>>(
        wal_path: P,
        init_state: WalInitState,
        header_metadata: WalHeaderMetadata,
    ) -> Self {
        Self {
            writer: Mutex::new(None),
            wal_path: wal_path.as_ref().to_path_buf(),
            init_state: Mutex::new(init_state),
            header_written: Mutex::new(false),
            header_metadata,
        }
    }

    /// Get metadata written in the WAL header.
    pub fn header_metadata(&self) -> WalHeaderMetadata {
        self.header_metadata
    }

    /// Check if the WAL is initialized.
    pub fn is_initialized(&self) -> bool {
        *self.init_state.lock().unwrap() == WalInitState::Initialized
    }

    /// Initialize the WAL writer.
    ///
    /// Creates the file writer if not already created.
    pub fn initialize(&self) -> Result<()> {
        let mut writer_guard = self.writer.lock().unwrap();
        if writer_guard.is_none() {
            let mut writer = BufferedFileWriter::new(&self.wal_path)?;

            let mut init_state = self.init_state.lock().unwrap();
            if *init_state == WalInitState::UninitializedRequiresTruncate {
                // Truncate to previous known good size
                // For now, truncate to 0 (full recovery will handle this)
                writer.truncate(0)?;
            }
            *init_state = WalInitState::Initialized;
            *writer_guard = Some(writer);
        }
        Ok(())
    }

    /// Write the WAL header.
    ///
    /// The header contains:
    /// - WAL type marker (WAL_VERSION)
    /// - Version number
    /// - Database identifier
    /// - Checkpoint iteration
    pub fn write_header(&self) -> Result<()> {
        let mut header_written = self.header_written.lock().unwrap();
        if *header_written {
            return Ok(());
        }

        self.initialize()?;

        let mut writer_guard = self.writer.lock().unwrap();
        let writer = writer_guard
            .as_mut()
            .ok_or_else(|| paro_error::io_error("WAL writer not initialized"))?;

        // Only write header if file is empty
        if writer.file_size() > 0 {
            *header_written = true;
            return Ok(());
        }
        // Format: [wal_type: u8][version: u64][db_identifier: [u8;16]][checkpoint_iteration: u64]
        writer.write_u8(WalType::WalVersion as u8)?;
        writer.write_u64(WAL_VERSION_NUMBER)?;
        writer.write_data(&self.header_metadata.db_identifier)?;
        writer.write_u64(self.header_metadata.checkpoint_iteration)?;
        writer.flush()?;

        *header_written = true;
        Ok(())
    }

    /// Write a WAL entry with checksum.
    ///
    /// Entry format:
    /// - Size (u64): Size of the entry data
    /// - Checksum (u64): Checksum of the entry data
    /// - Data: The serialized entry
    ///
    /// # Arguments
    /// * `wal_type` - Type of the WAL entry
    /// * `data` - Serialized entry data (excluding type byte)
    pub fn write_entry(&self, wal_type: WalType, data: &[u8]) -> Result<()> {
        self.write_header()?;

        let mut writer_guard = self.writer.lock().unwrap();
        let writer = writer_guard
            .as_mut()
            .ok_or_else(|| paro_error::io_error("WAL writer not initialized"))?;

        // Build complete entry: [wal_type: u8][data]
        let mut entry_data = Vec::with_capacity(1 + data.len());
        entry_data.push(wal_type as u8);
        entry_data.extend_from_slice(data);

        // Compute checksum over the complete entry
        let checksum = compute_wal_checksum(&entry_data);

        // Write: [size: u64][checksum: u64][entry_data]
        writer.write_u64(entry_data.len() as u64)?;
        writer.write_u64(checksum)?;
        writer.write_data(&entry_data)?;

        Ok(())
    }

    /// Write a checkpoint marker.
    ///
    /// This writes a checkpoint entry to the WAL containing a checkpoint marker.
    /// During recovery, this marker is used to verify if the checkpoint completed:
    /// - If the metadata-store marker matches this WAL marker, checkpoint succeeded
    /// - If they don't match, the checkpoint was incomplete and WAL replay is needed
    ///
    /// # Arguments
    /// * `checkpoint_marker` - Logical marker persisted alongside checkpoint metadata.
    pub fn write_checkpoint(&self, checkpoint_marker: u64) -> Result<()> {
        let mut data = Vec::with_capacity(8);
        data.extend_from_slice(&checkpoint_marker.to_le_bytes());
        self.write_entry(WalType::Checkpoint, &data)
    }

    /// Write a RowsetCommit entry.
    ///
    /// Format:
    /// [tablet_id: u64][rowset_id: u64][start_version: i64][end_version: i64][path_len: u32][path_bytes]
    pub fn write_rowset_commit(
        &self,
        tablet_id: u64,
        rowset_id: u64,
        start_version: i64,
        end_version: i64,
        rowset_path: &str,
    ) -> Result<()> {
        let mut data = Vec::new();
        data.extend_from_slice(&tablet_id.to_le_bytes());
        data.extend_from_slice(&rowset_id.to_le_bytes());
        data.extend_from_slice(&start_version.to_le_bytes());
        data.extend_from_slice(&end_version.to_le_bytes());
        let path_bytes = rowset_path.as_bytes();
        data.extend_from_slice(&(path_bytes.len() as u32).to_le_bytes());
        data.extend_from_slice(path_bytes);
        self.write_entry(WalType::RowsetCommit, &data)
    }

    /// Write a CompactionPublish entry.
    pub fn write_compaction_publish(&self, record: &CompactionPublishRecord) -> Result<()> {
        let entry = crate::wal::wal_entry::WalEntry::CompactionPublish {
            tablet_id: record.tablet_id,
            plan_id: record.plan_id.0,
            job_id: record.job_id.0,
            output_rowset_id: record.output_rowset_id,
            output_start_version: record.output_version.start,
            output_end_version: record.output_version.end,
            cumulative_point_action: record.cumulative_point_action,
            output_rowset_path: record.output_rowset_path.clone(),
            replaced_inputs: record.replaced_inputs.clone(),
        };
        self.write_entry(WalType::CompactionPublish, &entry.serialize_data())
    }

    /// Write a checkpoint start marker.
    ///
    /// This is written at the beginning of a checkpoint operation to indicate
    /// that a checkpoint is in progress. If recovery finds this marker without
    /// a corresponding checkpoint completion, it knows the checkpoint failed.
    ///
    /// # Arguments
    /// * `checkpoint_marker` - Expected checkpoint marker for this checkpoint cycle.
    pub fn write_checkpoint_start(&self, checkpoint_marker: u64) -> Result<()> {
        // Write checkpoint marker - same format as write_checkpoint
        // The distinction is in how it's used during recovery
        self.write_checkpoint(checkpoint_marker)
    }

    /// Write a flush marker and sync to disk.
    pub fn flush(&self) -> Result<()> {
        // Write flush marker
        self.write_entry(WalType::WalFlush, &[])?;

        // Sync to disk
        let mut writer_guard = self.writer.lock().unwrap();
        if let Some(writer) = writer_guard.as_mut() {
            writer.sync()?;
        }
        Ok(())
    }

    /// Truncate the WAL to a specific size.
    pub fn truncate(&self, size: u64) -> Result<()> {
        let mut writer_guard = self.writer.lock().unwrap();
        if let Some(writer) = writer_guard.as_mut() {
            writer.truncate(size)?;
        } else {
            // Mark for truncation on next initialize
            let mut init_state = self.init_state.lock().unwrap();
            *init_state = WalInitState::UninitializedRequiresTruncate;
        }
        Ok(())
    }

    /// Get the current WAL file size.
    pub fn file_size(&self) -> u64 {
        let writer_guard = self.writer.lock().unwrap();
        writer_guard.as_ref().map(|w| w.file_size()).unwrap_or(0)
    }

    /// Get total bytes written.
    pub fn total_written(&self) -> u64 {
        let writer_guard = self.writer.lock().unwrap();
        writer_guard
            .as_ref()
            .map(|w| w.total_written())
            .unwrap_or(0)
    }

    /// Get the WAL file path.
    pub fn path(&self) -> &Path {
        &self.wal_path
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::wal_entry::WalEntry;
    use tempfile::tempdir;

    #[test]
    fn test_buffered_file_writer_create() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.wal");

        let writer = BufferedFileWriter::new(&path).unwrap();
        assert_eq!(writer.file_size(), 0);
        assert_eq!(writer.total_written(), 0);
    }

    #[test]
    fn test_buffered_file_writer_write() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.wal");

        let mut writer = BufferedFileWriter::new(&path).unwrap();
        writer.write_data(b"Hello, WAL!").unwrap();
        writer.flush().unwrap();

        assert!(writer.file_size() > 0);
        assert_eq!(writer.total_written(), 11);
    }

    #[test]
    fn test_wal_writer_header() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.wal");

        let wal = WalWriter::new(&path, WalInitState::Uninitialized);
        wal.write_header().unwrap();

        assert!(wal.is_initialized());
        assert!(wal.file_size() > 0);
    }

    #[test]
    fn test_wal_writer_entry() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.wal");

        let wal = WalWriter::new(&path, WalInitState::Uninitialized);
        wal.write_entry(WalType::Checkpoint, &42u64.to_le_bytes())
            .unwrap();

        assert!(wal.file_size() > 0);
    }

    #[test]
    fn test_wal_writer_flush() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.wal");

        let wal = WalWriter::new(&path, WalInitState::Uninitialized);
        let entry = WalEntry::RowsetCommit {
            tablet_id: 1,
            rowset_id: 2,
            start_version: 0,
            end_version: 0,
            rowset_path: "rs".to_string(),
        };
        wal.write_entry(entry.wal_type(), &entry.serialize_data())
            .unwrap();
        wal.flush().unwrap();

        // File should be synced
        assert!(wal.file_size() > 0);
    }
}
