//! Buffered WAL reading with checksum verification and torn-write detection.

use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use crate::wal::checksum::compute_wal_checksum;
use crate::wal::wal_entry::{WalEntry, WalHeaderMetadata, WAL_DB_IDENTIFIER_LEN};
use crate::wal::wal_type::WalType;
use crate::wal::wal_writer::WAL_VERSION_NUMBER;
use paro_common::error::{self as paro_error, Result};

/// Default buffer size for WAL reads (4KB).
pub const WAL_READ_BUFFER_SIZE: usize = 4096;

/// Result of reading a WAL entry.
#[derive(Debug)]
pub(crate) enum ReadEntryResult {
    /// Successfully read an entry
    Entry(WalEntry),
    /// End of file reached (clean)
    EndOfFile,
    /// Torn write detected - incomplete header
    TornWriteIncompleteHeader {
        /// Position where the torn write was detected
        position: u64,
        /// Bytes available (less than required for header)
        #[allow(dead_code)]
        bytes_available: u64,
    },
    /// Torn write detected - incomplete data
    TornWriteIncompleteData {
        /// Position where the torn write was detected
        position: u64,
        /// Expected size from header
        expected_size: u64,
        /// Actual bytes available
        #[allow(dead_code)]
        bytes_available: u64,
    },
    /// Torn write detected - checksum mismatch
    TornWriteChecksumMismatch {
        /// Position where the torn write was detected
        position: u64,
        /// Stored checksum
        stored_checksum: u64,
        /// Computed checksum
        computed_checksum: u64,
    },
}

impl ReadEntryResult {
    /// Position where tail corruption was detected, if this result indicates corruption.
    pub fn tail_corruption_position(&self) -> Option<u64> {
        match self {
            ReadEntryResult::TornWriteIncompleteHeader { position, .. }
            | ReadEntryResult::TornWriteIncompleteData { position, .. }
            | ReadEntryResult::TornWriteChecksumMismatch { position, .. } => Some(*position),
            _ => None,
        }
    }
}

/// Buffered file reader for WAL operations.
pub(crate) struct BufferedFileReader {
    /// Path to the WAL file
    path: PathBuf,
    /// Buffered reader wrapping the file
    reader: BufReader<File>,
    /// Total file size
    file_size: u64,
    /// Current read position
    current_offset: u64,
}

impl BufferedFileReader {
    /// Create a new buffered file reader.
    ///
    /// # Arguments
    /// * `path` - Path to the WAL file
    ///
    /// # Returns
    /// A new BufferedFileReader instance, or None if file doesn't exist
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Option<Self>> {
        let path = path.as_ref().to_path_buf();

        if !path.exists() {
            return Ok(None);
        }

        let file = File::open(&path)
            .map_err(|e| paro_error::io_error(format!("Failed to open WAL file: {}", e)))?;

        let file_size = file
            .metadata()
            .map_err(|e| paro_error::io_error(format!("Failed to get file metadata: {}", e)))?
            .len();

        Ok(Some(Self {
            path,
            reader: BufReader::with_capacity(WAL_READ_BUFFER_SIZE, file),
            file_size,
            current_offset: 0,
        }))
    }

    /// Check if we've reached the end of the file.
    pub fn finished(&self) -> bool {
        self.current_offset >= self.file_size
    }

    /// Get the current read offset.
    pub fn current_offset(&self) -> u64 {
        self.current_offset
    }

    /// Get the total file size.
    pub fn file_size(&self) -> u64 {
        self.file_size
    }

    /// Get the file path.
    #[allow(dead_code)]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Get remaining bytes in the file.
    pub fn remaining(&self) -> u64 {
        self.file_size.saturating_sub(self.current_offset)
    }

    /// Reset the reader to the beginning of the file.
    pub fn reset(&mut self) -> Result<()> {
        self.reader
            .seek(SeekFrom::Start(0))
            .map_err(|e| paro_error::io_error(format!("Failed to seek WAL file: {}", e)))?;
        self.current_offset = 0;
        Ok(())
    }

    /// Seek to a specific position in the file.
    pub fn seek(&mut self, position: u64) -> Result<()> {
        self.reader
            .seek(SeekFrom::Start(position))
            .map_err(|e| paro_error::io_error(format!("Failed to seek WAL file: {}", e)))?;
        self.current_offset = position;
        Ok(())
    }

    /// Read exactly `n` bytes from the file.
    pub fn read_exact(&mut self, buf: &mut [u8]) -> Result<()> {
        self.reader
            .read_exact(buf)
            .map_err(|e| paro_error::io_error(format!("Failed to read WAL file: {}", e)))?;
        self.current_offset += buf.len() as u64;
        Ok(())
    }

    /// Try to read exactly `n` bytes, returning how many were actually read.
    /// This is useful for detecting torn writes.
    pub fn try_read_exact(&mut self, buf: &mut [u8]) -> Result<usize> {
        let mut total_read = 0;
        while total_read < buf.len() {
            match self.reader.read(&mut buf[total_read..]) {
                Ok(0) => break, // EOF
                Ok(n) => {
                    total_read += n;
                    self.current_offset += n as u64;
                }
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(e) => {
                    return Err(paro_error::io_error(format!(
                        "Failed to read WAL file: {}",
                        e
                    )))
                }
            }
        }
        Ok(total_read)
    }

    /// Read a u8 value.
    pub fn read_u8(&mut self) -> Result<u8> {
        let mut buf = [0u8; 1];
        self.read_exact(&mut buf)?;
        Ok(buf[0])
    }

    /// Read a u64 value in little-endian format.
    pub fn read_u64(&mut self) -> Result<u64> {
        let mut buf = [0u8; 8];
        self.read_exact(&mut buf)?;
        Ok(u64::from_le_bytes(buf))
    }

    /// Read data into a buffer.
    #[allow(dead_code)]
    pub fn read_data(&mut self, buf: &mut [u8]) -> Result<()> {
        self.read_exact(buf)
    }

    /// Refresh file metadata after external changes (e.g. truncation during recovery).
    pub fn refresh_file_size(&mut self) -> Result<()> {
        let file_size = std::fs::metadata(&self.path)
            .map_err(|e| paro_error::io_error(format!("Failed to stat WAL file: {}", e)))?
            .len();
        self.file_size = file_size;

        if self.current_offset > self.file_size {
            self.seek(self.file_size)?;
        }

        Ok(())
    }
}

/// WAL Reader for recovery operations.
///
/// Reads WAL entries sequentially, verifying checksums and handling
/// torn writes gracefully.
pub struct WalReader {
    /// Underlying buffered file reader
    reader: BufferedFileReader,
    /// WAL version detected from header
    wal_version: u64,
    /// Whether the header has been read
    header_read: bool,
    /// File-level metadata parsed from WAL header (version >= 3).
    header_metadata: WalHeaderMetadata,
    /// Last successful read offset (for truncation on torn writes)
    last_successful_offset: u64,
    /// Last flush marker offset (for safe truncation point)
    last_flush_offset: u64,
}

/// Size of the entry header: size (u64) + checksum (u64)
const ENTRY_HEADER_SIZE: u64 = 16;

impl WalReader {
    /// Open a WAL file for reading.
    ///
    /// Returns None if the file doesn't exist.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Option<Self>> {
        let reader = BufferedFileReader::open(path)?;

        Ok(reader.map(|r| Self {
            reader: r,
            wal_version: 1, // Default to version 1 until header is read
            header_read: false,
            header_metadata: WalHeaderMetadata::default(),
            last_successful_offset: 0,
            last_flush_offset: 0,
        }))
    }

    /// Check if we've reached the end of the WAL.
    pub fn finished(&self) -> bool {
        self.reader.finished()
    }

    /// Get the current read offset.
    pub fn current_offset(&self) -> u64 {
        self.reader.current_offset()
    }

    /// Get the file size.
    pub fn file_size(&self) -> u64 {
        self.reader.file_size()
    }

    /// Get the last successful read offset.
    ///
    /// This is useful for truncating torn writes during recovery.
    pub fn last_successful_offset(&self) -> u64 {
        self.last_successful_offset
    }

    /// Get the last flush marker offset.
    ///
    /// This represents the last known safe truncation point where
    /// a complete transaction was committed.
    pub fn last_flush_offset(&self) -> u64 {
        self.last_flush_offset
    }

    /// Get the WAL version.
    pub fn wal_version(&self) -> u64 {
        self.wal_version
    }

    /// Get header metadata parsed from the WAL header.
    pub fn header_metadata(&self) -> WalHeaderMetadata {
        self.header_metadata
    }

    /// Ensure WAL header has been parsed.
    pub fn ensure_header_read(&mut self) -> Result<()> {
        self.read_header()
    }

    /// Reset the reader to the beginning.
    pub fn reset(&mut self) -> Result<()> {
        self.reader.reset()?;
        self.header_read = false;
        self.header_metadata = WalHeaderMetadata::default();
        self.last_successful_offset = 0;
        self.last_flush_offset = 0;
        Ok(())
    }

    /// Refresh file metadata after external WAL changes (e.g. truncation).
    pub fn refresh_file_size(&mut self) -> Result<()> {
        self.reader.refresh_file_size()
    }

    /// Read the WAL header.
    ///
    /// The header contains:
    /// - WAL type marker (WAL_VERSION)
    /// - Version number
    fn read_header(&mut self) -> Result<()> {
        if self.header_read {
            return Ok(());
        }

        if self.reader.finished() {
            return Err(paro_error::serialization_error("Empty WAL file"));
        }

        // Read WAL type (should be WAL_VERSION)
        let wal_type_byte = self.reader.read_u8()?;
        let wal_type = WalType::try_from(wal_type_byte)?;

        if wal_type != WalType::WalVersion {
            return Err(paro_error::serialization_error(format!(
                "Invalid WAL header: expected WAL_VERSION, got {:?}",
                wal_type
            )));
        }

        // Read version number
        self.wal_version = self.reader.read_u64()?;

        if self.wal_version > WAL_VERSION_NUMBER {
            return Err(paro_error::serialization_error(format!(
                "Unsupported WAL version: {} (max supported: {})",
                self.wal_version, WAL_VERSION_NUMBER
            )));
        }

        if self.wal_version >= 3 {
            let mut db_identifier = [0u8; WAL_DB_IDENTIFIER_LEN];
            self.reader.read_exact(&mut db_identifier)?;
            let checkpoint_iteration = self.reader.read_u64()?;
            self.header_metadata = WalHeaderMetadata::new(db_identifier, checkpoint_iteration);
        } else {
            self.header_metadata = WalHeaderMetadata::default();
        }

        self.header_read = true;
        self.last_successful_offset = self.reader.current_offset();
        Ok(())
    }

    /// Read the next WAL entry.
    ///
    /// Returns:
    /// - `Ok(Some(entry))` - Successfully read an entry
    /// - `Ok(None)` - End of file reached
    /// - `Err(...)` - Error reading or validating entry
    pub fn read_entry(&mut self) -> Result<Option<WalEntry>> {
        match self.read_entry_with_torn_detection()? {
            ReadEntryResult::Entry(entry) => Ok(Some(entry)),
            ReadEntryResult::EndOfFile => Ok(None),
            ReadEntryResult::TornWriteIncompleteHeader { position, .. } => {
                Err(paro_error::serialization_error(format!(
                    "Torn write detected: incomplete header at offset {}",
                    position
                )))
            }
            ReadEntryResult::TornWriteIncompleteData {
                position,
                expected_size,
                bytes_available,
            } => Err(paro_error::serialization_error(format!(
                "Torn write detected: incomplete data at offset {} (expected {} bytes, got {})",
                position, expected_size, bytes_available
            ))),
            ReadEntryResult::TornWriteChecksumMismatch {
                position,
                stored_checksum,
                computed_checksum,
            } => Err(paro_error::serialization_error(format!(
                "Torn write detected: checksum mismatch at offset {} (stored: {}, computed: {})",
                position, stored_checksum, computed_checksum
            ))),
        }
    }

    /// Read the next WAL entry with detailed torn write detection.
    ///
    /// This method provides detailed information about torn writes,
    /// allowing the caller to decide how to handle them.
    pub(crate) fn read_entry_with_torn_detection(&mut self) -> Result<ReadEntryResult> {
        // Ensure header is read first
        if !self.header_read {
            self.read_header()?;
        }

        if self.reader.finished() {
            return Ok(ReadEntryResult::EndOfFile);
        }

        let entry_start = self.reader.current_offset();

        // For WAL version 2+, entries have size and checksum
        if self.wal_version >= 2 {
            self.read_checksummed_entry_with_detection(entry_start)
        } else {
            // Version 1: no checksums (legacy format)
            self.read_legacy_entry(entry_start)
        }
    }

    /// Read a checksummed entry with torn write detection (WAL version 2+).
    fn read_checksummed_entry_with_detection(
        &mut self,
        entry_start: u64,
    ) -> Result<ReadEntryResult> {
        let remaining = self.reader.remaining();

        // Check if we have enough bytes for the header
        if remaining < ENTRY_HEADER_SIZE {
            if remaining == 0 {
                return Ok(ReadEntryResult::EndOfFile);
            }
            return Ok(ReadEntryResult::TornWriteIncompleteHeader {
                position: entry_start,
                bytes_available: remaining,
            });
        }

        // Read header: [size: u64][checksum: u64]
        let mut header_buf = [0u8; 16];
        let bytes_read = self.reader.try_read_exact(&mut header_buf)?;
        if bytes_read < 16 {
            return Ok(ReadEntryResult::TornWriteIncompleteHeader {
                position: entry_start,
                bytes_available: bytes_read as u64,
            });
        }

        let size = u64::from_le_bytes(header_buf[0..8].try_into().unwrap());
        let stored_checksum = u64::from_le_bytes(header_buf[8..16].try_into().unwrap());

        // Validate size - check for obviously corrupt values
        if size > 1024 * 1024 * 1024 {
            // > 1GB is suspicious
            return Ok(ReadEntryResult::TornWriteChecksumMismatch {
                position: entry_start,
                stored_checksum,
                computed_checksum: 0,
            });
        }

        // Check if we have enough bytes for the data
        let data_remaining = self.reader.remaining();
        if size > data_remaining {
            return Ok(ReadEntryResult::TornWriteIncompleteData {
                position: entry_start,
                expected_size: size,
                bytes_available: data_remaining,
            });
        }

        // Read entry data
        let mut data = vec![0u8; size as usize];
        let bytes_read = self.reader.try_read_exact(&mut data)?;
        if (bytes_read as u64) < size {
            return Ok(ReadEntryResult::TornWriteIncompleteData {
                position: entry_start,
                expected_size: size,
                bytes_available: bytes_read as u64,
            });
        }

        // Verify checksum
        let computed_checksum = compute_wal_checksum(&data);
        if stored_checksum != computed_checksum {
            return Ok(ReadEntryResult::TornWriteChecksumMismatch {
                position: entry_start,
                stored_checksum,
                computed_checksum,
            });
        }

        // Deserialize entry
        let entry = WalEntry::deserialize(&data)?;

        // Update last successful offset
        self.last_successful_offset = self.reader.current_offset();

        // Track flush markers for safe truncation points
        if matches!(entry, WalEntry::Flush) {
            self.last_flush_offset = self.last_successful_offset;
        }

        Ok(ReadEntryResult::Entry(entry))
    }

    /// Read a legacy entry (WAL version 1, no checksums).
    fn read_legacy_entry(&mut self, _entry_start: u64) -> Result<ReadEntryResult> {
        // Version 1 format: just [wal_type: u8][data...]
        // This is a simplified implementation - full version would need
        // to know entry sizes from the type
        let wal_type_byte = match self.reader.read_u8() {
            Ok(b) => b,
            Err(_) => return Ok(ReadEntryResult::EndOfFile),
        };

        let wal_type = WalType::try_from(wal_type_byte)?;

        // For legacy format, we'd need type-specific parsing
        // For now, return an error as we don't support version 1
        Err(paro_error::serialization_error(format!(
            "WAL version 1 not fully supported, found entry type {:?}",
            wal_type
        )))
    }

    /// Iterate over all entries in the WAL.
    ///
    /// This is a convenience method that yields entries until EOF or error.
    pub fn entries(&mut self) -> WalEntryIterator<'_> {
        WalEntryIterator { reader: self }
    }

    /// Scan the WAL to find the safe truncation point.
    ///
    /// This reads through the entire WAL, tracking:
    /// - Last successful entry offset
    /// - Last flush marker offset (safe truncation point)
    /// - Any torn writes detected
    ///
    /// Returns (last_successful_offset, last_flush_offset, torn_write_position)
    pub(crate) fn scan_for_truncation_point(&mut self) -> Result<TruncationScanResult> {
        self.reset()?;

        let mut result = TruncationScanResult {
            last_successful_offset: 0,
            last_flush_offset: 0,
            torn_write_position: None,
            has_unflushed_tail: false,
            entries_scanned: 0,
        };
        let mut has_entries_since_flush = false;

        loop {
            let read_result = self.read_entry_with_torn_detection()?;
            if let Some(position) = read_result.tail_corruption_position() {
                result.torn_write_position = Some(position);
                break;
            }

            match read_result {
                ReadEntryResult::Entry(entry) => {
                    result.entries_scanned += 1;
                    result.last_successful_offset = self.last_successful_offset;
                    has_entries_since_flush = true;

                    if matches!(entry, WalEntry::Flush) {
                        result.last_flush_offset = self.last_successful_offset;
                        has_entries_since_flush = false;
                    }
                }
                ReadEntryResult::EndOfFile => {
                    if has_entries_since_flush {
                        result.has_unflushed_tail = true;
                    }
                    break;
                }
                _ => unreachable!("tail corruption results are handled above"),
            }
        }

        Ok(result)
    }
}

/// Iterator over WAL entries.
pub struct WalEntryIterator<'a> {
    reader: &'a mut WalReader,
}

impl<'a> Iterator for WalEntryIterator<'a> {
    type Item = Result<WalEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.reader.read_entry() {
            Ok(Some(entry)) => Some(Ok(entry)),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}

/// Result of scanning WAL for truncation point.
#[derive(Debug, Clone)]
pub(crate) struct TruncationScanResult {
    /// Last successfully read entry offset
    pub last_successful_offset: u64,
    /// Last flush marker offset (safe truncation point)
    pub last_flush_offset: u64,
    /// Position where torn write was detected (if any)
    pub torn_write_position: Option<u64>,
    /// Whether WAL ends with entries after the last flush boundary.
    pub has_unflushed_tail: bool,
    /// Number of entries scanned
    pub entries_scanned: u64,
}

impl TruncationScanResult {
    /// Get the recommended truncation point.
    ///
    /// If recovery detects tail corruption or unflushed tail entries, returns
    /// the last flush offset (the last known safe transaction boundary).
    /// Otherwise, returns the last successful offset.
    pub fn recommended_truncation_point(&self) -> u64 {
        if self.needs_truncation() {
            // Corrupt/uncommitted tail detected - truncate to last flush (safe point)
            self.last_flush_offset
        } else {
            // No torn write - can keep everything
            self.last_successful_offset
        }
    }

    /// Check if truncation is needed.
    pub fn needs_truncation(&self) -> bool {
        self.torn_write_position.is_some() || self.has_unflushed_tail
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::test_support::{
        append_open_create_schema_txn, write_flushed_create_schema_txn,
    };
    use crate::wal::wal_entry::WalEntry;
    use crate::wal::wal_writer::{WalInitState, WalWriter};
    use paro_common::ddl::DdlChange;
    use std::fs::OpenOptions;
    use std::io::Write;
    use tempfile::tempdir;

    fn create_test_wal_with_entries() -> PathBuf {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.wal");

        let writer = WalWriter::new(&path, WalInitState::Uninitialized);
        write_flushed_create_schema_txn(&writer, "default", "test_schema", 1, 100).unwrap();

        // Keep tempdir alive
        std::mem::forget(dir);
        path
    }

    #[test]
    fn test_wal_reader_open_nonexistent() {
        let result = WalReader::open("/nonexistent/path/wal.log").unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_wal_reader_read_entries() {
        let path = create_test_wal_with_entries();

        let mut reader = WalReader::open(&path).unwrap().unwrap();
        assert!(!reader.finished());

        let mut saw_schema = false;
        let mut saw_flush = false;
        while let Some(entry) = reader.read_entry().unwrap() {
            match entry {
                WalEntry::TxnCatalogOp { op, .. } => {
                    if matches!(op.change.change, DdlChange::CreateSchema(_)) {
                        assert_eq!(op.change.key.name, "test_schema");
                        saw_schema = true;
                    }
                }
                WalEntry::Flush => saw_flush = true,
                _ => {}
            }
        }
        assert!(saw_schema && saw_flush);
        assert!(reader.finished());
    }

    #[test]
    fn test_wal_reader_reset() {
        let path = create_test_wal_with_entries();

        let mut reader = WalReader::open(&path).unwrap().unwrap();

        // Read all entries
        while reader.read_entry().unwrap().is_some() {}
        assert!(reader.finished());

        // Reset and read again
        reader.reset().unwrap();
        assert!(!reader.finished());

        let entry = reader.read_entry().unwrap();
        assert!(entry.is_some());
    }

    #[test]
    fn test_wal_reader_iterator() {
        let path = create_test_wal_with_entries();

        let mut reader = WalReader::open(&path).unwrap().unwrap();
        let entries: Vec<_> = reader.entries().collect();

        assert!(entries.len() >= 4, "expected journal + flush entries");
        assert!(entries.iter().all(|e| e.is_ok()));
    }

    #[test]
    fn test_torn_write_incomplete_header() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("torn.wal");

        {
            let writer = WalWriter::new(&path, WalInitState::Uninitialized);
            write_flushed_create_schema_txn(&writer, "default", "test", 1, 100).unwrap();
        }

        // Append incomplete header (only 4 bytes instead of 16)
        {
            let mut file = OpenOptions::new().append(true).open(&path).unwrap();
            file.write_all(&[0x01, 0x02, 0x03, 0x04]).unwrap();
        }

        let mut reader = WalReader::open(&path).unwrap().unwrap();
        let result = reader.scan_for_truncation_point().unwrap();

        assert!(result.needs_truncation());
        assert!(result.torn_write_position.is_some());
        assert!(!result.has_unflushed_tail);
        assert!(result.last_flush_offset > 0);
    }

    #[test]
    fn test_torn_write_incomplete_data() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("torn_data.wal");

        {
            let writer = WalWriter::new(&path, WalInitState::Uninitialized);
            write_flushed_create_schema_txn(&writer, "default", "test", 1, 100).unwrap();
        }

        // Append header claiming 100 bytes but only write 10
        {
            let mut file = OpenOptions::new().append(true).open(&path).unwrap();
            // Size: 100
            file.write_all(&100u64.to_le_bytes()).unwrap();
            // Checksum: dummy
            file.write_all(&0u64.to_le_bytes()).unwrap();
            // Only 10 bytes of data
            file.write_all(&[0u8; 10]).unwrap();
        }

        let mut reader = WalReader::open(&path).unwrap().unwrap();
        let result = reader.scan_for_truncation_point().unwrap();

        assert!(result.needs_truncation());
        assert!(result.torn_write_position.is_some());
        assert!(!result.has_unflushed_tail);
    }

    #[test]
    fn test_torn_write_checksum_mismatch() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("torn_checksum.wal");

        {
            let writer = WalWriter::new(&path, WalInitState::Uninitialized);
            write_flushed_create_schema_txn(&writer, "default", "test", 1, 100).unwrap();
        }

        // Append entry with wrong checksum
        {
            let mut file = OpenOptions::new().append(true).open(&path).unwrap();
            let data = [0x01, 0x02, 0x03, 0x04, 0x05];
            // Size: 5
            file.write_all(&5u64.to_le_bytes()).unwrap();
            // Wrong checksum
            file.write_all(&12345u64.to_le_bytes()).unwrap();
            // Data
            file.write_all(&data).unwrap();
        }

        let mut reader = WalReader::open(&path).unwrap().unwrap();
        let result = reader.scan_for_truncation_point().unwrap();

        assert!(result.needs_truncation());
        assert!(result.torn_write_position.is_some());
        assert!(!result.has_unflushed_tail);
    }

    #[test]
    fn test_scan_for_truncation_point_unflushed_tail() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("unflushed_tail.wal");

        {
            let writer = WalWriter::new(&path, WalInitState::Uninitialized);
            write_flushed_create_schema_txn(&writer, "default", "committed", 1, 100).unwrap();
            append_open_create_schema_txn(&writer, "default", "uncommitted", 2).unwrap();
        }

        let mut reader = WalReader::open(&path).unwrap().unwrap();
        let result = reader.scan_for_truncation_point().unwrap();

        assert!(result.needs_truncation());
        assert!(result.torn_write_position.is_none());
        assert!(result.has_unflushed_tail);
        assert_eq!(
            result.recommended_truncation_point(),
            result.last_flush_offset
        );
    }

    #[test]
    fn test_scan_for_truncation_point_clean() {
        let path = create_test_wal_with_entries();

        let mut reader = WalReader::open(&path).unwrap().unwrap();
        let result = reader.scan_for_truncation_point().unwrap();

        assert!(!result.needs_truncation());
        assert!(result.torn_write_position.is_none());
        assert!(!result.has_unflushed_tail);
        assert!(result.entries_scanned >= 4);
        assert!(result.last_flush_offset > 0);
        assert_eq!(
            result.last_successful_offset,
            result.recommended_truncation_point()
        );
    }

    #[test]
    fn test_last_flush_offset_tracking() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("multi_flush.wal");

        {
            let writer = WalWriter::new(&path, WalInitState::Uninitialized);
            write_flushed_create_schema_txn(&writer, "default", "schema1", 1, 100).unwrap();
            write_flushed_create_schema_txn(&writer, "default", "schema2", 2, 101).unwrap();
        }

        let mut reader = WalReader::open(&path).unwrap().unwrap();
        let mut flush_offsets = Vec::new();
        while let Some(entry) = reader.read_entry().unwrap() {
            if matches!(entry, WalEntry::Flush) {
                flush_offsets.push(reader.last_flush_offset());
            }
        }
        assert!(flush_offsets.len() >= 2);
        assert!(flush_offsets[1] > flush_offsets[0]);
    }
}
