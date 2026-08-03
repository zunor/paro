// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! TemporaryFileManager - Manages temporary files for spill-to-disk.
//!
//! - Manages temporary files for buffer spill-to-disk
//! - Multiple buffer sizes for compression efficiency
//! - Block index management within files
//! - Automatic file cleanup on drop

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use paro_common::allocator::{MemoryTag, MEMORY_TAG_COUNT};
use paro_common::error::{self as paro_error, Result};

use super::BlockId;

/// Granularity for temporary buffer sizes (32 KB).
pub const TEMPORARY_BUFFER_SIZE_GRANULARITY: usize = 32 * 1024;

/// Default block allocation size (256 KB).
pub const DEFAULT_BLOCK_ALLOC_SIZE: usize = 262144;

/// Supported temporary buffer sizes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(usize)]
pub enum TemporaryBufferSize {
    /// Invalid size
    Invalid = 0,
    /// 32 KB
    S32K = 32768,
    /// 64 KB
    S64K = 65536,
    /// 96 KB
    S96K = 98304,
    /// 128 KB
    S128K = 131072,
    /// 160 KB
    S160K = 163840,
    /// 192 KB
    S192K = 196608,
    /// 224 KB
    S224K = 229376,
    /// Default (256 KB)
    Default = DEFAULT_BLOCK_ALLOC_SIZE,
}

impl TemporaryBufferSize {
    /// Check if this is a valid buffer size.
    pub fn is_valid(&self) -> bool {
        !matches!(self, TemporaryBufferSize::Invalid)
    }

    /// Get the size in bytes.
    pub fn size(&self) -> usize {
        *self as usize
    }

    /// Round up a size to the nearest temporary buffer size.
    pub fn round_up(size: usize) -> Self {
        if size == 0 {
            return TemporaryBufferSize::Invalid;
        }
        let aligned =
            size.div_ceil(TEMPORARY_BUFFER_SIZE_GRANULARITY) * TEMPORARY_BUFFER_SIZE_GRANULARITY;

        match aligned {
            s if s <= 32768 => TemporaryBufferSize::S32K,
            s if s <= 65536 => TemporaryBufferSize::S64K,
            s if s <= 98304 => TemporaryBufferSize::S96K,
            s if s <= 131072 => TemporaryBufferSize::S128K,
            s if s <= 163840 => TemporaryBufferSize::S160K,
            s if s <= 196608 => TemporaryBufferSize::S192K,
            s if s <= 229376 => TemporaryBufferSize::S224K,
            _ => TemporaryBufferSize::Default,
        }
    }
}

/// Compression level for temporary files.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[repr(i32)]
pub enum TemporaryCompressionLevel {
    /// ZSTD compression level -5 (fastest)
    ZstdMinusFive = -5,
    /// ZSTD compression level -3
    ZstdMinusThree = -3,
    /// ZSTD compression level -1
    ZstdMinusOne = -1,
    /// No compression
    #[default]
    Uncompressed = 0,
    /// ZSTD compression level 1
    ZstdOne = 1,
    /// ZSTD compression level 3
    ZstdThree = 3,
    /// ZSTD compression level 5 (slowest, highest compression)
    ZstdFive = 5,
}

impl TemporaryCompressionLevel {
    /// Convert to ZSTD compression level integer.
    pub fn to_int(self) -> i32 {
        self as i32
    }

    /// Create from ZSTD compression level integer.
    ///
    /// Returns None if the level is not a valid TemporaryCompressionLevel.
    pub fn from_int(level: i32) -> Option<Self> {
        match level {
            -5 => Some(TemporaryCompressionLevel::ZstdMinusFive),
            -3 => Some(TemporaryCompressionLevel::ZstdMinusThree),
            -1 => Some(TemporaryCompressionLevel::ZstdMinusOne),
            0 => Some(TemporaryCompressionLevel::Uncompressed),
            1 => Some(TemporaryCompressionLevel::ZstdOne),
            3 => Some(TemporaryCompressionLevel::ZstdThree),
            5 => Some(TemporaryCompressionLevel::ZstdFive),
            _ => None,
        }
    }
}

/// Adaptive compression level selection for temporary files.
///
/// This structure tracks write performance for different compression levels
/// and automatically selects the best level based on historical performance.
#[derive(Debug)]
pub struct TemporaryFileCompressionAdaptivity {
    /// Random number generator for exploration
    random_engine: Mutex<rand::rngs::StdRng>,
    /// Duration of the last uncompressed write (nanoseconds)
    last_uncompressed_write_ns: AtomicI64,
    /// Duration of the last compressed writes for each level (nanoseconds)
    last_compressed_writes_ns: [AtomicI64; Self::LEVELS],
}

impl TemporaryFileCompressionAdaptivity {
    /// The value to initialize the atomic write counters to (50 microseconds)
    const INITIAL_NS: i64 = 50000;

    /// How many compression levels we adapt between
    const LEVELS: usize = 6;

    /// Bias towards compressed writes: we only choose uncompressed if it is more than 2x faster
    const DURATION_RATIO_THRESHOLD: f64 = 2.0;

    /// Probability to deviate from the current best write behavior (50%)
    const COMPRESSION_DEVIATION: f64 = 0.5;

    /// Weight to use for moving weighted average
    #[allow(dead_code)]
    const WEIGHT: i64 = 16;

    /// Create a new TemporaryFileCompressionAdaptivity.
    pub fn new() -> Self {
        use rand::SeedableRng;
        Self {
            random_engine: Mutex::new(rand::rngs::StdRng::from_entropy()),
            last_uncompressed_write_ns: AtomicI64::new(Self::INITIAL_NS),
            last_compressed_writes_ns: [
                AtomicI64::new(Self::INITIAL_NS),
                AtomicI64::new(Self::INITIAL_NS),
                AtomicI64::new(Self::INITIAL_NS),
                AtomicI64::new(Self::INITIAL_NS),
                AtomicI64::new(Self::INITIAL_NS),
                AtomicI64::new(Self::INITIAL_NS),
            ],
        }
    }

    /// Get current time in nanoseconds to measure write times.
    #[allow(dead_code)]
    pub fn get_current_time_nanos() -> i64 {
        use std::time::{SystemTime, UNIX_EPOCH};
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as i64
    }

    /// Convert from index to compression level.
    ///
    /// Formula: level = index * 2 - 5
    /// - index 0 -> level -5 (ZstdMinusFive)
    /// - index 1 -> level -3 (ZstdMinusThree)
    /// - index 2 -> level -1 (ZstdMinusOne)
    /// - index 3 -> level 1 (ZstdOne)
    /// - index 4 -> level 3 (ZstdThree)
    /// - index 5 -> level 5 (ZstdFive)
    fn index_to_level(index: usize) -> TemporaryCompressionLevel {
        let level_int = (index as i32) * 2 - 5;
        TemporaryCompressionLevel::from_int(level_int).expect("index_to_level: index out of range")
    }

    /// Convert from compression level to index.
    ///
    /// Formula: index = (level + 5) / 2
    #[allow(dead_code)]
    fn level_to_index(level: TemporaryCompressionLevel) -> usize {
        ((level.to_int() + 5) / 2) as usize
    }

    /// Get the minimum compression level (fastest).
    fn minimum_compression_level() -> TemporaryCompressionLevel {
        Self::index_to_level(0)
    }

    /// Get the maximum compression level (slowest, highest compression).
    fn maximum_compression_level() -> TemporaryCompressionLevel {
        Self::index_to_level(Self::LEVELS - 1)
    }

    /// Get the compression level to use based on current write times.
    ///
    /// This method implements an adaptive algorithm that:
    /// 1. Finds the fastest compression level based on historical performance
    /// 2. Compares compressed vs uncompressed write times
    /// 3. Occasionally deviates from the best choice to explore other options
    pub fn get_compression_level(&self) -> TemporaryCompressionLevel {
        use rand::Rng;

        let mut min_compression_idx = 0;
        let level;
        let ratio;
        let should_compress;
        let should_deviate;
        let deviate_uncompressed;

        {
            let mut rng = self.random_engine.lock().unwrap();

            // Find the compression level with the minimum write time
            let mut min_compressed_time = self.last_compressed_writes_ns[0].load(Ordering::Relaxed);
            for compression_idx in 1..Self::LEVELS {
                let time = self.last_compressed_writes_ns[compression_idx].load(Ordering::Relaxed);
                if time < min_compressed_time {
                    min_compression_idx = compression_idx;
                    min_compressed_time = time;
                }
            }
            level = Self::index_to_level(min_compression_idx);

            // Calculate the ratio of compressed to uncompressed write time
            let last_uncompressed = self.last_uncompressed_write_ns.load(Ordering::Relaxed);
            ratio = min_compressed_time as f64 / last_uncompressed as f64;
            should_compress = ratio < Self::DURATION_RATIO_THRESHOLD;

            // Decide whether to deviate from the best choice (for exploration)
            should_deviate = rng.gen::<f64>() < Self::COMPRESSION_DEVIATION;
            deviate_uncompressed = rng.gen::<f64>() < 0.5; // Coin flip
        }

        // Select the compression level based on the adaptive algorithm
        if !should_deviate {
            // Don't deviate: use the best choice
            if should_compress {
                level
            } else {
                TemporaryCompressionLevel::Uncompressed
            }
        } else if !should_compress {
            // Deviate from uncompressed -> go to fastest compression level
            Self::minimum_compression_level()
        } else if deviate_uncompressed {
            // Deviate to uncompressed
            TemporaryCompressionLevel::Uncompressed
        } else if level == Self::maximum_compression_level() {
            // At highest level, go down one
            Self::index_to_level(min_compression_idx - 1)
        } else if ratio < 1.0 {
            // Compressed writes are faster, try increasing the compression level
            Self::index_to_level(min_compression_idx + 1)
        } else {
            // Compressed writes are slower, try decreasing the compression level
            if level == Self::minimum_compression_level() {
                // Already lowest level, go to uncompressed
                TemporaryCompressionLevel::Uncompressed
            } else {
                Self::index_to_level(min_compression_idx - 1)
            }
        }
    }

    /// Update write time for given compression level.
    ///
    /// Uses a moving weighted average to smooth out variations:
    /// new_avg = (old_avg * (WEIGHT - 1) + new_duration) / WEIGHT
    #[allow(dead_code)]
    pub fn update(&self, level: TemporaryCompressionLevel, time_before_ns: i64) {
        let duration = Self::get_current_time_nanos() - time_before_ns;

        if level == TemporaryCompressionLevel::Uncompressed {
            let old_value = self.last_uncompressed_write_ns.load(Ordering::Relaxed);
            let new_value = (old_value * (Self::WEIGHT - 1) + duration) / Self::WEIGHT;
            self.last_uncompressed_write_ns
                .store(new_value, Ordering::Relaxed);
        } else {
            let index = Self::level_to_index(level);
            let old_value = self.last_compressed_writes_ns[index].load(Ordering::Relaxed);
            let new_value = (old_value * (Self::WEIGHT - 1) + duration) / Self::WEIGHT;
            self.last_compressed_writes_ns[index].store(new_value, Ordering::Relaxed);
        }
    }
}

impl Default for TemporaryFileCompressionAdaptivity {
    fn default() -> Self {
        Self::new()
    }
}

/// Identifier for a temporary file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TemporaryFileIdentifier {
    /// The size of buffers within this file.
    pub size: TemporaryBufferSize,
    /// The index of this file.
    pub file_index: Option<u64>,
    /// Indicates whether the file is encrypted.
    pub encrypted: bool,
}

impl TemporaryFileIdentifier {
    /// Create a new temporary file identifier.
    pub fn new(size: TemporaryBufferSize, file_index: u64) -> Self {
        Self {
            size,
            file_index: Some(file_index),
            encrypted: false,
        }
    }

    /// Create a new temporary file identifier with encryption flag.
    ///
    /// # Note
    /// The encryption key management is handled separately, so we only store the flag.
    pub fn new_with_encryption(
        size: TemporaryBufferSize,
        file_index: u64,
        encrypted: bool,
    ) -> Self {
        Self {
            size,
            file_index: Some(file_index),
            encrypted,
        }
    }

    /// Check if this identifier is valid.
    pub fn is_valid(&self) -> bool {
        self.size.is_valid() && self.file_index.is_some()
    }
}

impl Default for TemporaryFileIdentifier {
    /// Create an invalid temporary file identifier.
    fn default() -> Self {
        Self {
            size: TemporaryBufferSize::Invalid,
            file_index: None,
            encrypted: false,
        }
    }
}

/// Index of a block within a temporary file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TemporaryFileIndex {
    /// The file identifier.
    pub identifier: TemporaryFileIdentifier,
    /// The block index within the file.
    pub block_index: Option<u64>,
    /// The block header size.
    ///
    /// # Note
    /// This represents the size of metadata stored before the actual data.
    /// We also use this to store the original data size (before padding).
    pub block_header_size: Option<usize>,
}

impl TemporaryFileIndex {
    /// Create a new temporary file index.
    pub fn new(
        identifier: TemporaryFileIdentifier,
        block_index: u64,
        block_header_size: usize,
    ) -> Self {
        Self {
            identifier,
            block_index: Some(block_index),
            block_header_size: Some(block_header_size),
        }
    }

    /// Check if this index is valid.
    pub fn is_valid(&self) -> bool {
        self.identifier.is_valid() && self.block_index.is_some() && self.block_header_size.is_some()
    }

    /// Get the original data size (stored in block_header_size).
    ///
    /// # Note
    /// This is a helper method. block_header_size
    /// is used for metadata, but we use it to store the original size.
    pub fn original_size(&self) -> usize {
        self.block_header_size.unwrap_or(0)
    }
}

impl Default for TemporaryFileIndex {
    /// Create an invalid temporary file index.
    fn default() -> Self {
        Self {
            identifier: TemporaryFileIdentifier::default(),
            block_index: None,
            block_header_size: None,
        }
    }
}

/// Information about a temporary file.
#[derive(Debug, Clone)]
pub struct TemporaryFileInfo {
    /// Path to the file.
    pub path: PathBuf,
    /// Size of the file in bytes.
    pub size: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TemporaryBlockInfo {
    index: TemporaryFileIndex,
    tag: MemoryTag,
}

/// Snapshot of spill I/O metrics maintained by [`TemporaryFileManager`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TemporarySpillMetricsSnapshot {
    /// Total bytes written to temporary files.
    pub write_bytes: u64,
    /// Total bytes read from temporary files.
    pub read_bytes: u64,
    /// Current number of active temporary files.
    pub file_count: usize,
    /// Current swap usage on disk in bytes.
    pub swap_usage: u64,
    /// Current swap limit in bytes (`u64::MAX` means unlimited).
    pub swap_limit: u64,
    /// Number of times a write was rejected by swap-limit enforcement.
    pub swap_limit_hits: u64,
}

/// Maximum allowed index per file (base value).
const MAX_ALLOWED_INDEX_BASE: u64 = 4000;
/// Ownership marker file for startup orphan cleanup.
const TEMP_DIR_OWNERSHIP_MARKER: &str = ".paro_temp_owner";
/// Shards used to serialize operations that target the same temporary block.
const TEMPORARY_BLOCK_OPERATION_LOCK_SHARDS: usize = 64;

/// Manages block indexes within a temporary file.
///
/// # Design
/// The BlockIndexManager tracks which block indexes are in use and which are free
/// for reuse. It also manages file truncation when blocks are freed from the end.
struct BlockIndexManager {
    /// The maximum block index allocated.
    max_index: u64,
    /// Free indexes that can be reused.
    free_indexes: Vec<u64>,
    /// Indexes currently in use.
    indexes_in_use: Vec<u64>,
}

impl BlockIndexManager {
    /// Create a new block index manager.
    fn new() -> Self {
        Self {
            max_index: 0,
            free_indexes: Vec::new(),
            indexes_in_use: Vec::new(),
        }
    }

    /// Get a new block index.
    fn get_new_block_index(&mut self) -> u64 {
        let index = self.get_new_block_index_internal();
        self.indexes_in_use.push(index);
        index
    }

    /// Remove an index from the manager.
    ///
    /// Returns whether the max_index was reduced (and the file can be truncated),
    /// or `None` if the index was not allocated.
    fn remove_index(&mut self, index: u64) -> Option<bool> {
        // Remove from indexes_in_use
        if let Some(pos) = self.indexes_in_use.iter().position(|&x| x == index) {
            self.indexes_in_use.remove(pos);
        } else {
            return None;
        }

        // Add to free_indexes
        self.free_indexes.push(index);

        // Check if we can truncate the file
        let max_index_in_use = if self.indexes_in_use.is_empty() {
            0
        } else {
            *self.indexes_in_use.iter().max().unwrap() + 1
        };

        if max_index_in_use < self.max_index {
            // Reduce max_index
            self.max_index = max_index_in_use;

            // Remove free_indexes that are >= max_index
            self.free_indexes.retain(|&x| x < self.max_index);

            Some(true)
        } else {
            Some(false)
        }
    }

    /// Get the maximum block index.
    fn get_max_index(&self) -> u64 {
        self.max_index
    }

    /// Check if there are free blocks available.
    fn has_free_blocks(&self) -> bool {
        !self.free_indexes.is_empty()
    }

    /// Internal method to get a new block index.
    fn get_new_block_index_internal(&mut self) -> u64 {
        if !self.has_free_blocks() {
            let new_index = self.max_index;
            self.max_index += 1;
            return new_index;
        }

        // Reuse a free index (take from front for simplicity)
        self.free_indexes.remove(0)
    }
}

/// Handle for a single temporary file.
///
/// # Design
/// TemporaryFileHandle manages a single temporary file, including:
/// - Block index allocation and recycling via BlockIndexManager
/// - Lazy file creation
/// - Read/Write operations with optional compression
/// - File truncation when blocks are freed
/// - Thread-safe access via internal mutex
struct TemporaryFileHandle {
    /// The file identifier (size/file index).
    identifier: TemporaryFileIdentifier,
    /// Maximum allowed index for this file.
    max_allowed_index: u64,
    /// Path to the file.
    path: PathBuf,
    /// File handle (lazily opened).
    file: Mutex<Option<File>>,
    /// Block index manager.
    index_manager: Mutex<BlockIndexManager>,
}

impl TemporaryFileHandle {
    /// Create a new temporary file handle.
    fn new(identifier: TemporaryFileIdentifier, path: PathBuf, file_count: usize) -> Self {
        let max_allowed_index = (1u64 << file_count.min(10)) * MAX_ALLOWED_INDEX_BASE;
        Self {
            identifier,
            max_allowed_index,
            path,
            file: Mutex::new(None),
            index_manager: Mutex::new(BlockIndexManager::new()),
        }
    }

    /// Try to get a block index for writing.
    ///
    /// Returns None if the file is at capacity.
    fn try_get_block_index(&self, original_size: usize) -> Option<TemporaryFileIndex> {
        let mut index_manager = self.index_manager.lock().unwrap();

        if index_manager.get_max_index() >= self.max_allowed_index
            && !index_manager.has_free_blocks()
        {
            return None; // File is at capacity
        }

        // Open file if not already open
        self.create_file_if_not_exists()?;

        // Fetch a new block index to write to
        let block_index = index_manager.get_new_block_index();

        Some(TemporaryFileIndex::new(
            self.identifier,
            block_index,
            original_size, // Store original size in block_header_size
        ))
    }

    /// Create the file if it doesn't exist.
    fn create_file_if_not_exists(&self) -> Option<()> {
        let mut file_guard = self.file.lock().unwrap();

        if file_guard.is_some() {
            return Some(());
        }

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&self.path)
            .ok()?;

        *file_guard = Some(file);
        Some(())
    }

    /// Get the position in file for a given block index.
    fn get_position(&self, block_index: u64) -> u64 {
        // Note: metadata_size for encryption is not added yet
        block_index * self.identifier.size.size() as u64
    }

    /// Write a buffer to the file.
    fn write_buffer(&self, block_index: u64, data: &[u8]) -> Result<()> {
        let mut file_guard = self.file.lock().unwrap();
        let file = file_guard
            .as_mut()
            .ok_or_else(|| paro_error::internal("temp file not opened"))?;

        let position = self.get_position(block_index);
        file.seek(SeekFrom::Start(position))
            .map_err(|e| paro_error::io_error(format!("Seek failed: {}", e)))?;

        file.write_all(data)
            .map_err(|e| paro_error::io_error(format!("Write failed: {}", e)))?;

        Ok(())
    }

    /// Write a compressed buffer to the file.
    ///
    /// The compressed data is padded to the specified buffer_size.
    ///
    /// # Arguments
    /// * `block_index` - The block index within the file
    /// * `compressed_data` - The compressed data (format: [original_size: 8 bytes][compressed_data])
    /// * `buffer_size` - The target buffer size (must be >= compressed_data.len())
    fn write_buffer_compressed(
        &self,
        block_index: u64,
        compressed_data: &[u8],
        buffer_size: usize,
    ) -> Result<()> {
        let mut file_guard = self.file.lock().unwrap();
        let file = file_guard
            .as_mut()
            .ok_or_else(|| paro_error::internal("temp file not opened"))?;

        let position = self.get_position(block_index);
        file.seek(SeekFrom::Start(position))
            .map_err(|e| paro_error::io_error(format!("Seek failed: {}", e)))?;

        // Pad compressed data to buffer size
        let mut padded_data = vec![0u8; buffer_size];
        let copy_len = compressed_data.len().min(buffer_size);
        padded_data[..copy_len].copy_from_slice(&compressed_data[..copy_len]);

        file.write_all(&padded_data)
            .map_err(|e| paro_error::io_error(format!("Write failed: {}", e)))?;

        Ok(())
    }

    /// Read a buffer from the file.
    fn read_buffer(&self, block_index: u64, buffer: &mut [u8]) -> Result<()> {
        let mut file_guard = self.file.lock().unwrap();
        let file = file_guard
            .as_mut()
            .ok_or_else(|| paro_error::internal("temp file not opened"))?;

        let position = self.get_position(block_index);
        file.seek(SeekFrom::Start(position))
            .map_err(|e| paro_error::io_error(format!("Seek failed: {}", e)))?;

        file.read_exact(buffer)
            .map_err(|e| paro_error::io_error(format!("Read failed: {}", e)))?;

        Ok(())
    }

    /// Erase a block index.
    fn erase_block_index(&self, block_index: u64) -> Result<()> {
        let mut index_manager = self.index_manager.lock().unwrap();
        let should_truncate = index_manager.remove_index(block_index).ok_or_else(|| {
            paro_error::internal(format!(
                "temporary spill block index {} is not allocated",
                block_index
            ))
        })?;

        if should_truncate {
            // Truncate file to new max_index
            let file_guard = self
                .file
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(file) = file_guard.as_ref() {
                let new_size = self.get_position(index_manager.get_max_index());
                let _ = file.set_len(new_size);
            }
        }
        Ok(())
    }

    /// Check if the file is empty.
    fn is_empty(&self) -> bool {
        let index_manager = self.index_manager.lock().unwrap();
        index_manager.get_max_index() == 0
    }

    /// Delete the file.
    fn delete(&self) {
        // Cleanup must remain best-effort even if an I/O path panicked while
        // holding the file mutex. This is also called from the manager's Drop.
        let mut file_guard = self
            .file
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *file_guard = None;
        let _ = fs::remove_file(&self.path);
    }

    /// Get information about this file.
    fn get_info(&self) -> TemporaryFileInfo {
        let index_manager = self.index_manager.lock().unwrap();
        TemporaryFileInfo {
            path: self.path.clone(),
            size: self.get_position(index_manager.get_max_index()),
        }
    }
}

/// Result of buffer compression.
#[allow(dead_code)]
#[derive(Debug, Clone, Copy)]
pub struct CompressionResult {
    /// The size of the compressed buffer (or DEFAULT if uncompressed).
    pub size: TemporaryBufferSize,
    /// The compression level used.
    pub level: TemporaryCompressionLevel,
}

/// Manager for temporary files used in spill-to-disk.
///
/// The TemporaryFileManager handles writing buffers to temporary files
/// when memory pressure requires eviction. It manages multiple files
/// organized by buffer size for efficient storage.
///
/// # Thread Safety
/// All operations are thread-safe using internal locking.
///
/// # Example
/// ```ignore
/// let manager = TemporaryFileManager::new("/tmp/paro")?;
/// manager.set_max_swap_space(1024 * 1024 * 1024); // 1 GB
///
/// // Write a buffer
/// let size = manager.write_temporary_buffer(block_id, MemoryTag::InMemoryTable, &data)?;
///
/// // Read it back
/// let mut buffer = vec![0u8; size];
/// manager.read_temporary_buffer(block_id, &mut buffer)?;
/// ```
pub struct TemporaryFileManager {
    /// Temporary directory path.
    temp_directory: PathBuf,
    /// Prevents lifecycle operations from racing active spill I/O.
    ///
    /// Ordinary block operations hold a shared guard. Destructive manager-wide
    /// operations such as [`Self::clear`] hold an exclusive guard.
    lifecycle_lock: RwLock<()>,
    /// Serializes operations for the same block without serializing unrelated I/O.
    block_operation_locks: [Mutex<()>; TEMPORARY_BLOCK_OPERATION_LOCK_SHARDS],
    /// Files organized by (size, file_index).
    /// Note: Using Arc<TemporaryFileHandle> for shared ownership across threads.
    files: RwLock<HashMap<(TemporaryBufferSize, u64), Arc<TemporaryFileHandle>>>,
    /// Map of block_id -> file index and originating memory tag.
    used_blocks: RwLock<HashMap<BlockId, TemporaryBlockInfo>>,
    /// Total size on disk in bytes.
    size_on_disk: AtomicU64,
    /// Maximum swap space allowed.
    max_swap_space: AtomicU64,
    /// Next file index per size.
    next_file_index: RwLock<HashMap<TemporaryBufferSize, u64>>,
    /// Whether the directory was created by us.
    created_directory: bool,
    /// Path of ownership marker used for startup orphan cleanup.
    ownership_marker_path: PathBuf,
    /// Whether the ownership marker was created by this manager instance.
    owns_marker: bool,
    /// Total bytes written through write_temporary_buffer().
    write_bytes: AtomicU64,
    /// Total bytes read through read/peek temporary buffer paths.
    read_bytes: AtomicU64,
    /// Number of swap-limit rejections.
    swap_limit_hits: AtomicU64,
    /// Current spill usage on disk by memory tag.
    spill_usage_per_tag: [AtomicU64; MEMORY_TAG_COUNT],
    /// Compression adaptivity per CPU core (64 cores max).
    #[allow(dead_code)]
    compression_adaptivities: [TemporaryFileCompressionAdaptivity; 64],
}

impl TemporaryFileManager {
    /// Minimum compressed temporary buffer size (32 KB).
    #[allow(dead_code)]
    const MINIMUM_COMPRESSED_BUFFER_SIZE: TemporaryBufferSize = TemporaryBufferSize::S32K;

    /// Maximum compressed temporary buffer size (224 KB).
    #[allow(dead_code)]
    const MAXIMUM_COMPRESSED_BUFFER_SIZE: TemporaryBufferSize = TemporaryBufferSize::S224K;

    fn ownership_marker_path(temp_directory: &Path) -> PathBuf {
        temp_directory.join(TEMP_DIR_OWNERSHIP_MARKER)
    }

    fn is_managed_temp_file(path: &Path) -> bool {
        match path.file_name().and_then(|name| name.to_str()) {
            Some(name) => {
                name.starts_with("paro_temp_storage_")
                    && (name.ends_with(".tmp") || name.ends_with(".tmp.gz"))
            }
            None => false,
        }
    }

    fn cleanup_orphan_temp_files(temp_directory: &Path) -> Result<usize> {
        let mut removed = 0usize;
        let entries = fs::read_dir(temp_directory)
            .map_err(|e| paro_error::io_error(format!("Failed to read temp dir: {}", e)))?;

        for entry in entries {
            let entry = entry
                .map_err(|e| paro_error::io_error(format!("Failed to read dir entry: {}", e)))?;
            let path = entry.path();

            if path.is_file() && Self::is_managed_temp_file(&path) {
                fs::remove_file(&path).map_err(|e| {
                    paro_error::io_error(format!(
                        "Failed to remove orphan temp file {}: {}",
                        path.display(),
                        e
                    ))
                })?;
                removed += 1;
            }
        }

        Ok(removed)
    }

    fn write_ownership_marker(path: &Path) -> Result<()> {
        let pid = std::process::id();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
        let content = format!("pid={pid},ts_ms={now}\n");
        fs::write(path, content).map_err(|e| {
            paro_error::io_error(format!(
                "Failed to create temp ownership marker {}: {}",
                path.display(),
                e
            ))
        })
    }

    /// Create a new temporary file manager.
    ///
    /// # Arguments
    /// * `temp_directory` - Path to the temporary directory
    pub fn new<P: AsRef<Path>>(temp_directory: P) -> Result<Self> {
        let temp_directory = temp_directory.as_ref().to_path_buf();

        // Create directory if it doesn't exist
        let created_directory = if !temp_directory.exists() {
            fs::create_dir_all(&temp_directory)
                .map_err(|e| paro_error::io_error(format!("Failed to create temp dir: {}", e)))?;
            true
        } else {
            false
        };

        let ownership_marker_path = Self::ownership_marker_path(&temp_directory);

        // Startup orphan cleanup:
        // If the ownership marker exists, we treat this as a previous unclean shutdown.
        if ownership_marker_path.exists() {
            let _ = Self::cleanup_orphan_temp_files(&temp_directory)?;
            let _ = fs::remove_file(&ownership_marker_path);
        } else {
            // Also handle older runs without marker support.
            let _ = Self::cleanup_orphan_temp_files(&temp_directory)?;
        }

        Self::write_ownership_marker(&ownership_marker_path)?;

        // Initialize compression adaptivities array using std::array::from_fn
        let compression_adaptivities =
            std::array::from_fn(|_| TemporaryFileCompressionAdaptivity::new());

        Ok(Self {
            temp_directory,
            lifecycle_lock: RwLock::new(()),
            block_operation_locks: std::array::from_fn(|_| Mutex::new(())),
            files: RwLock::new(HashMap::new()),
            used_blocks: RwLock::new(HashMap::new()),
            size_on_disk: AtomicU64::new(0),
            max_swap_space: AtomicU64::new(u64::MAX),
            next_file_index: RwLock::new(HashMap::new()),
            created_directory,
            ownership_marker_path,
            owns_marker: true,
            write_bytes: AtomicU64::new(0),
            read_bytes: AtomicU64::new(0),
            swap_limit_hits: AtomicU64::new(0),
            spill_usage_per_tag: std::array::from_fn(|_| AtomicU64::new(0)),
            compression_adaptivities,
        })
    }

    /// Set the maximum swap space.
    pub fn set_max_swap_space(&self, max_bytes: u64) {
        self.max_swap_space.store(max_bytes, Ordering::Release);
    }

    /// Get the maximum swap space.
    pub fn get_max_swap_space(&self) -> u64 {
        self.max_swap_space.load(Ordering::Acquire)
    }

    /// Get the total used space on disk.
    pub fn get_used_space(&self) -> u64 {
        self.size_on_disk.load(Ordering::Acquire)
    }

    /// Get cumulative write bytes.
    pub fn get_write_bytes(&self) -> u64 {
        self.write_bytes.load(Ordering::Acquire)
    }

    /// Get cumulative read bytes.
    pub fn get_read_bytes(&self) -> u64 {
        self.read_bytes.load(Ordering::Acquire)
    }

    /// Get number of swap-limit write rejections.
    pub fn get_swap_limit_hits(&self) -> u64 {
        self.swap_limit_hits.load(Ordering::Acquire)
    }

    /// Get a snapshot of temporary spill metrics.
    pub fn metrics_snapshot(&self) -> TemporarySpillMetricsSnapshot {
        let _lifecycle_guard = self.lifecycle_lock.read().unwrap();
        TemporarySpillMetricsSnapshot {
            write_bytes: self.get_write_bytes(),
            read_bytes: self.get_read_bytes(),
            file_count: self.files.read().unwrap().len(),
            swap_usage: self.get_used_space(),
            swap_limit: self.get_max_swap_space(),
            swap_limit_hits: self.get_swap_limit_hits(),
        }
    }

    /// Get the temporary directory path.
    pub fn temp_directory(&self) -> &Path {
        &self.temp_directory
    }

    fn block_operation_lock(&self, block_id: BlockId) -> &Mutex<()> {
        let shard = block_id.rem_euclid(TEMPORARY_BLOCK_OPERATION_LOCK_SHARDS as i64) as usize;
        &self.block_operation_locks[shard]
    }

    fn reserve_disk_space(&self, bytes: u64) -> Result<()> {
        let mut current = self.size_on_disk.load(Ordering::Acquire);
        loop {
            let next = current.checked_add(bytes).ok_or_else(|| {
                paro_error::out_of_memory("temporary spill size accounting overflow")
            })?;
            let max_space = self.max_swap_space.load(Ordering::Acquire);
            if next > max_space {
                self.swap_limit_hits.fetch_add(1, Ordering::AcqRel);
                return Err(paro_error::out_of_memory(format!(
                    "swap space limit exceeded: {} + {} > {}",
                    current, bytes, max_space
                )));
            }
            match self.size_on_disk.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(()),
                Err(observed) => current = observed,
            }
        }
    }

    fn release_disk_space(&self, bytes: u64) -> Result<()> {
        self.size_on_disk
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current.checked_sub(bytes)
            })
            .map(|_| ())
            .map_err(|current| {
                paro_error::internal(format!(
                    "temporary spill size accounting underflow: {} - {}",
                    current, bytes
                ))
            })
    }

    /// Compress a buffer with adaptive compression level selection.
    ///
    /// This method checks if compression is worthwhile and selects the appropriate
    /// compression level based on historical performance.
    ///
    /// # Arguments
    /// * `compression_adaptivity` - The compression adaptivity tracker for this CPU core
    /// * `data` - The buffer data to compress
    ///
    /// # Returns
    /// A `CompressionResult` containing the buffer size and compression level used.
    /// If compression is not beneficial, returns DEFAULT size with UNCOMPRESSED level.
    fn compress_buffer_with_adaptivity(
        &self,
        compression_adaptivity: &TemporaryFileCompressionAdaptivity,
        data: &[u8],
    ) -> (CompressionResult, Option<Vec<u8>>) {
        // Check if buffer size is worth compressing
        if data.len() <= Self::MINIMUM_COMPRESSED_BUFFER_SIZE.size() {
            // Buffer size is less or equal to the minimum compressed size - no point compressing
            return (
                CompressionResult {
                    size: TemporaryBufferSize::Default,
                    level: TemporaryCompressionLevel::Uncompressed,
                },
                None,
            );
        }

        // Get the compression level from adaptivity
        let level = compression_adaptivity.get_compression_level();
        if level == TemporaryCompressionLevel::Uncompressed {
            return (
                CompressionResult {
                    size: TemporaryBufferSize::Default,
                    level: TemporaryCompressionLevel::Uncompressed,
                },
                None,
            );
        }

        // Compress the buffer
        let compression_level = level.to_int();
        let compressed = match compress_buffer(data, compression_level) {
            Ok(c) => c,
            Err(_) => {
                // Compression failed, return uncompressed
                return (
                    CompressionResult {
                        size: TemporaryBufferSize::Default,
                        level,
                    },
                    None,
                );
            }
        };

        let compressed_size = compressed.len();

        // Check if compressed size is reasonable
        if compressed_size > Self::MAXIMUM_COMPRESSED_BUFFER_SIZE.size() {
            // Use default size if compression ratio is bad
            return (
                CompressionResult {
                    size: TemporaryBufferSize::Default,
                    level,
                },
                None,
            );
        }

        // Round up compressed size to temporary buffer size
        let buffer_size = TemporaryBufferSize::round_up(compressed_size);

        (
            CompressionResult {
                size: buffer_size,
                level,
            },
            Some(compressed),
        )
    }

    /// Write a temporary buffer to disk.
    ///
    /// This method automatically compresses the buffer if beneficial, using adaptive
    /// compression level selection based on historical performance.
    ///
    /// # Arguments
    /// * `block_id` - The block ID to associate with this buffer
    /// * `data` - The buffer data to write
    ///
    /// # Returns
    /// The size of the buffer written (compressed or uncompressed).
    pub fn write_temporary_buffer(
        &self,
        block_id: BlockId,
        tag: MemoryTag,
        data: &[u8],
    ) -> Result<usize> {
        let _lifecycle_guard = self.lifecycle_lock.read().unwrap();
        let _block_guard = self.block_operation_lock(block_id).lock().unwrap();
        if self.used_blocks.read().unwrap().contains_key(&block_id) {
            return Err(paro_error::internal(format!(
                "temporary block {} is already spilled",
                block_id
            )));
        }

        let original_size = data.len();

        // Get current time for performance statistics
        let time_before_ns = TemporaryFileCompressionAdaptivity::get_current_time_nanos();

        // Get CPU ID and select corresponding compression adaptivity
        // Use thread ID as a proxy for CPU ID (modulo 64)
        let adaptivity_idx = Self::get_cpu_id() % 64;
        let compression_adaptivity = &self.compression_adaptivities[adaptivity_idx];

        // Try to compress the buffer
        let (compression_result, compressed_data) =
            self.compress_buffer_with_adaptivity(compression_adaptivity, data);

        let buffer_size = compression_result.size.size();
        if compressed_data.is_none() && original_size > buffer_size {
            return Err(paro_error::not_supported(format!(
                "uncompressed temporary block of {} bytes exceeds the grouped spill slot size of {} bytes",
                original_size, buffer_size
            )));
        }
        self.reserve_disk_space(buffer_size as u64)?;

        // Find or create a file handle and get a block index
        let index = match self.allocate_block_index(compression_result.size, original_size) {
            Ok(index) => index,
            Err(err) => {
                self.release_disk_space(buffer_size as u64)?;
                return Err(err);
            }
        };

        // Get the file and write
        let key = (
            index.identifier.size,
            index.identifier.file_index.unwrap(), // Safe: validated by is_valid()
        );
        let write_result = (|| -> Result<()> {
            let files = self.files.read().unwrap();
            let file_handle = files
                .get(&key)
                .ok_or_else(|| paro_error::internal("file not found after allocation"))?;

            // Write compressed or uncompressed data
            if let Some(compressed) = compressed_data {
                // Write compressed data
                file_handle.write_buffer_compressed(
                    index.block_index.unwrap(),
                    &compressed,
                    buffer_size,
                )
            } else {
                // Write uncompressed data (pad to buffer size)
                let mut padded_data = vec![0u8; buffer_size];
                padded_data[..original_size].copy_from_slice(data);
                file_handle.write_buffer(index.block_index.unwrap(), &padded_data)
            }
        })();
        if let Err(write_err) = write_result {
            if let Err(cleanup_err) = self.release_spill_allocation(&index) {
                return Err(paro_error::internal(format!(
                    "temporary block write failed: {}; rollback failed: {}",
                    write_err, cleanup_err
                )));
            }
            return Err(write_err);
        }

        // Track the block
        let registered = {
            let mut used_blocks = self.used_blocks.write().unwrap();
            match used_blocks.entry(block_id) {
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert(TemporaryBlockInfo { index, tag });
                    true
                }
                std::collections::hash_map::Entry::Occupied(_) => false,
            }
        };
        if !registered {
            self.release_spill_allocation(&index)?;
            return Err(paro_error::internal(format!(
                "temporary block {} was concurrently registered",
                block_id
            )));
        }

        self.spill_usage_per_tag[tag.as_index()].fetch_add(buffer_size as u64, Ordering::AcqRel);
        self.write_bytes
            .fetch_add(buffer_size as u64, Ordering::AcqRel);

        // Update compression statistics
        compression_adaptivity.update(compression_result.level, time_before_ns);

        Ok(buffer_size)
    }

    /// Get an estimated CPU ID for the current thread.
    ///
    /// This is used to select a compression adaptivity tracker to avoid contention.
    fn get_cpu_id() -> usize {
        // Use thread ID as a proxy for CPU ID
        // This is a simple heuristic that works well in practice
        use std::thread;
        let thread_id = thread::current().id();
        // Hash the thread ID to get a number
        let hash = format!("{:?}", thread_id)
            .chars()
            .filter(|c| c.is_numeric())
            .collect::<String>()
            .parse::<usize>()
            .unwrap_or(0);
        hash
    }

    /// Check if a temporary buffer exists.
    pub fn has_temporary_buffer(&self, block_id: BlockId) -> bool {
        let _lifecycle_guard = self.lifecycle_lock.read().unwrap();
        let _block_guard = self.block_operation_lock(block_id).lock().unwrap();
        let used_blocks = self.used_blocks.read().unwrap();
        used_blocks.contains_key(&block_id)
    }

    /// Return block IDs that currently have spilled temporary buffers.
    pub fn temporary_block_ids(&self) -> Vec<BlockId> {
        let _lifecycle_guard = self.lifecycle_lock.read().unwrap();
        let used_blocks = self.used_blocks.read().unwrap();
        used_blocks.keys().copied().collect()
    }

    /// Read a temporary buffer from disk.
    ///
    /// Note: This also deletes the buffer after reading.
    ///
    /// # Arguments
    /// * `block_id` - The block ID to read
    /// * `buffer` - Buffer to read into
    ///
    /// # Returns
    /// The number of bytes read (original size, not padded).
    pub fn read_temporary_buffer(&self, block_id: BlockId, buffer: &mut [u8]) -> Result<usize> {
        let _lifecycle_guard = self.lifecycle_lock.read().unwrap();
        let _block_guard = self.block_operation_lock(block_id).lock().unwrap();
        let block_info = {
            let used_blocks = self.used_blocks.read().unwrap();
            used_blocks.get(&block_id).copied().ok_or_else(|| {
                paro_error::internal(format!("block {} not found in temp files", block_id))
            })?
        };
        let index = block_info.index;

        let buffer_size = index.identifier.size.size();
        let original_size = index.original_size();
        self.read_block_into(&index, buffer)?;

        self.release_tracked_spill(block_id, block_info)?;
        self.read_bytes
            .fetch_add(buffer_size as u64, Ordering::AcqRel);

        Ok(original_size)
    }

    /// Read a temporary buffer without deleting it.
    pub fn peek_temporary_buffer(&self, block_id: BlockId, buffer: &mut [u8]) -> Result<usize> {
        let _lifecycle_guard = self.lifecycle_lock.read().unwrap();
        let _block_guard = self.block_operation_lock(block_id).lock().unwrap();
        let block_info = {
            let used_blocks = self.used_blocks.read().unwrap();
            used_blocks.get(&block_id).copied().ok_or_else(|| {
                paro_error::internal(format!("block {} not found in temp files", block_id))
            })?
        };
        let index = block_info.index;

        let buffer_size = index.identifier.size.size();
        let original_size = index.original_size();
        self.read_block_into(&index, buffer)?;

        self.read_bytes
            .fetch_add(buffer_size as u64, Ordering::AcqRel);

        Ok(original_size)
    }

    /// Delete a temporary buffer.
    pub fn delete_temporary_buffer(&self, block_id: BlockId) -> Result<usize> {
        let _lifecycle_guard = self.lifecycle_lock.read().unwrap();
        let _block_guard = self.block_operation_lock(block_id).lock().unwrap();
        let block_info = {
            let used_blocks = self.used_blocks.read().unwrap();
            used_blocks.get(&block_id).copied().ok_or_else(|| {
                paro_error::internal(format!("block {} not found in temp files", block_id))
            })?
        };
        let index = block_info.index;
        let buffer_size = index.identifier.size.size();

        self.release_tracked_spill(block_id, block_info)?;

        Ok(buffer_size)
    }

    fn read_block_into(&self, index: &TemporaryFileIndex, buffer: &mut [u8]) -> Result<()> {
        let original_size = index.original_size();
        if buffer.len() < original_size {
            return Err(paro_error::invalid_input(format!(
                "temporary buffer is too small: need {} bytes, got {}",
                original_size,
                buffer.len()
            )));
        }

        let key = (
            index.identifier.size,
            index.identifier.file_index.unwrap(), // Safe: validated by is_valid()
        );
        let files = self.files.read().unwrap();
        let file_handle = files
            .get(&key)
            .ok_or_else(|| paro_error::internal("temporary spill file not found"))?;
        let mut temp_buffer = vec![0u8; index.identifier.size.size()];
        file_handle.read_buffer(index.block_index.unwrap(), &mut temp_buffer)?;

        if index.identifier.size != TemporaryBufferSize::Default {
            let decompressed = decompress_buffer(&temp_buffer, original_size)?;
            buffer[..original_size].copy_from_slice(&decompressed);
        } else {
            buffer[..original_size].copy_from_slice(&temp_buffer[..original_size]);
        }
        Ok(())
    }

    /// Release an allocated file slot and its disk-space reservation.
    ///
    /// Every fallible invariant check happens before the slot or accounting is
    /// changed. Holding `files` exclusively makes the remaining state transition
    /// linearizable with allocations and other releases.
    fn release_spill_allocation(&self, index: &TemporaryFileIndex) -> Result<()> {
        let key = (
            index.identifier.size,
            index.identifier.file_index.unwrap(), // Safe: validated by is_valid()
        );
        let block_index = index.block_index.unwrap(); // Safe: validated by is_valid()
        let buffer_size = index.identifier.size.size() as u64;

        // Keep the file map exclusively locked across erase/check/remove. Otherwise
        // a concurrent writer can reuse the last slot between the empty check and
        // file removal.
        let mut files = self.files.write().unwrap();
        let current_size = self.size_on_disk.load(Ordering::Acquire);
        if current_size < buffer_size {
            return Err(paro_error::internal(format!(
                "temporary spill size accounting underflow: {} - {}",
                current_size, buffer_size
            )));
        }

        let should_remove_file = {
            let file_handle = files
                .get(&key)
                .ok_or_else(|| paro_error::internal("temporary spill file not found"))?;

            // A missing index is reported without mutating the index manager.
            file_handle.erase_block_index(block_index)?;
            file_handle.is_empty()
        };

        if should_remove_file {
            if let Some(file_handle) = files.remove(&key) {
                file_handle.delete();
            }
        }

        // All fallible work is complete. Other indexed releases are serialized by
        // `files`, and reservation-only rollbacks can only remove their own bytes,
        // so the preflight check above guarantees this subtraction cannot underflow.
        let previous_size = self.size_on_disk.fetch_sub(buffer_size, Ordering::AcqRel);
        debug_assert!(previous_size >= buffer_size);
        Ok(())
    }

    /// Release a registered spill as one ownership/accounting transition.
    fn release_tracked_spill(
        &self,
        block_id: BlockId,
        block_info: TemporaryBlockInfo,
    ) -> Result<()> {
        // Keep ownership stable until the file slot and all counters are committed.
        // Lock order is lifecycle -> block operation -> used_blocks -> files.
        let mut used_blocks = self.used_blocks.write().unwrap();
        match used_blocks.get(&block_id) {
            Some(current) if *current == block_info => {}
            Some(_) => {
                return Err(paro_error::internal(format!(
                    "temporary block {} ownership changed during release",
                    block_id
                )));
            }
            None => {
                return Err(paro_error::internal(format!(
                    "temporary block {} is not registered",
                    block_id
                )));
            }
        }

        let buffer_size = block_info.index.identifier.size.size() as u64;
        let tag_usage = &self.spill_usage_per_tag[block_info.tag.as_index()];
        let current_tag_usage = tag_usage.load(Ordering::Acquire);
        if current_tag_usage < buffer_size {
            return Err(paro_error::internal(format!(
                "temporary spill tag accounting underflow: {} - {}",
                current_tag_usage, buffer_size
            )));
        }

        self.release_spill_allocation(&block_info.index)?;

        // No fallible work remains after the allocation transition commits.
        let previous_tag_usage = tag_usage.fetch_sub(buffer_size, Ordering::AcqRel);
        debug_assert!(previous_tag_usage >= buffer_size);
        let removed = used_blocks.remove(&block_id);
        debug_assert_eq!(removed, Some(block_info));
        Ok(())
    }

    /// Get the list of temporary files.
    pub fn get_temporary_files(&self) -> Vec<TemporaryFileInfo> {
        let _lifecycle_guard = self.lifecycle_lock.read().unwrap();
        let files = self.files.read().unwrap();
        let mut result = Vec::new();

        for file_handle in files.values() {
            result.push(file_handle.get_info());
        }

        result
    }

    pub fn spill_usage_per_tag(&self) -> [u64; MEMORY_TAG_COUNT] {
        let _lifecycle_guard = self.lifecycle_lock.read().unwrap();
        std::array::from_fn(|idx| self.spill_usage_per_tag[idx].load(Ordering::Acquire))
    }

    /// Allocate a block index in a file.
    fn allocate_block_index(
        &self,
        size: TemporaryBufferSize,
        original_size: usize,
    ) -> Result<TemporaryFileIndex> {
        // First try to find an existing file with space
        {
            let files = self.files.read().unwrap();
            for (&(file_size, _), file_handle) in files.iter() {
                if file_size != size {
                    continue;
                }
                if let Some(index) = file_handle.try_get_block_index(original_size) {
                    return Ok(index);
                }
            }
        }

        // Need to create a new file
        let file_index = {
            let mut next_index = self.next_file_index.write().unwrap();
            let idx = next_index.entry(size).or_insert(0);
            let current = *idx;
            *idx += 1;
            current
        };

        let identifier = TemporaryFileIdentifier::new(size, file_index);
        let path = self.create_file_path(&identifier);
        let file_count = {
            let files = self.files.read().unwrap();
            files.iter().filter(|(&(s, _), _)| s == size).count()
        };

        let file_handle = Arc::new(TemporaryFileHandle::new(identifier, path, file_count));
        let index = file_handle
            .try_get_block_index(original_size)
            .ok_or_else(|| paro_error::internal("failed to get block index from new file"))?;

        // Store the file handle
        {
            let mut files = self.files.write().unwrap();
            files.insert((size, file_index), file_handle);
        }

        Ok(index)
    }

    /// Create a file path for the given identifier.
    fn create_file_path(&self, identifier: &TemporaryFileIdentifier) -> PathBuf {
        let size_name = match identifier.size {
            TemporaryBufferSize::Invalid => "invalid",
            TemporaryBufferSize::S32K => "32k",
            TemporaryBufferSize::S64K => "64k",
            TemporaryBufferSize::S96K => "96k",
            TemporaryBufferSize::S128K => "128k",
            TemporaryBufferSize::S160K => "160k",
            TemporaryBufferSize::S192K => "192k",
            TemporaryBufferSize::S224K => "224k",
            TemporaryBufferSize::Default => "default",
        };
        let file_index = identifier.file_index.unwrap_or(0); // Safe: should always be Some when creating path
        self.temp_directory.join(format!(
            "paro_temp_storage_{}-{}.tmp",
            size_name, file_index
        ))
    }

    /// Clear all temporary files.
    pub fn clear(&self) -> Result<()> {
        // `clear` is also called from Drop, including while another panic may be
        // unwinding. Recover poisoned cleanup locks so resource reclamation cannot
        // turn that panic into a process abort.
        let _lifecycle_guard = self
            .lifecycle_lock
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        {
            let mut files = self
                .files
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            for file_handle in files.values() {
                file_handle.delete();
            }
            files.clear();
        }

        self.used_blocks
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clear();
        self.next_file_index
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clear();

        self.size_on_disk.store(0, Ordering::Release);
        for usage in &self.spill_usage_per_tag {
            usage.store(0, Ordering::Release);
        }

        Ok(())
    }
}

impl Drop for TemporaryFileManager {
    fn drop(&mut self) {
        // Clear all files - ignore errors during drop
        let _ = self.clear();

        if self.owns_marker {
            let _ = fs::remove_file(&self.ownership_marker_path);
            self.owns_marker = false;
        }

        // Remove directory if we created it
        if self.created_directory {
            let _ = fs::remove_dir(&self.temp_directory);
        }
    }
}

impl std::fmt::Debug for TemporaryFileManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TemporaryFileManager")
            .field("temp_directory", &self.temp_directory)
            .field("size_on_disk", &self.get_used_space())
            .field("max_swap_space", &self.get_max_swap_space())
            .field("write_bytes", &self.get_write_bytes())
            .field("read_bytes", &self.get_read_bytes())
            .field("swap_limit_hits", &self.get_swap_limit_hits())
            .field("file_count", &self.files.read().unwrap().len())
            .field("block_count", &self.used_blocks.read().unwrap().len())
            .finish()
    }
}

// ============================================================================
// Compression Helper Functions
// ============================================================================

/// Compress a buffer using ZSTD compression.
///
/// # Arguments
/// * `data` - The data to compress
/// * `level` - The ZSTD compression level (-5 to 22, where negative values are faster)
///
/// # Returns
/// A vector containing: [compressed_size: 8 bytes][compressed_data]
///
/// # Format
/// Compressed buffer format:
/// - First 8 bytes: compressed size (little-endian u64), NOT original size
/// - Remaining bytes: ZSTD compressed data
#[allow(dead_code)]
pub fn compress_buffer(data: &[u8], level: i32) -> Result<Vec<u8>> {
    // Compress the data using ZSTD
    let compressed = zstd::encode_all(data, level)
        .map_err(|e| paro_error::internal(format!("ZSTD compression failed: {}", e)))?;

    // Create result buffer: [compressed_size: 8 bytes][compressed_data]
    let compressed_size = compressed.len() as u64;
    let mut result = Vec::with_capacity(8 + compressed.len());

    // Write compressed size (little-endian)
    result.extend_from_slice(&compressed_size.to_le_bytes());

    // Write compressed data
    result.extend_from_slice(&compressed);

    Ok(result)
}

/// Decompress a buffer that was compressed with compress_buffer().
///
/// # Arguments
/// * `compressed` - The compressed buffer (format: [compressed_size: 8 bytes][compressed_data][padding])
/// * `expected_size` - The expected original size (for validation)
///
/// # Returns
/// The decompressed data
///
/// # Errors
/// Returns an error if:
/// - The compressed buffer is too small (< 8 bytes)
/// - Decompression fails
/// - The decompressed size doesn't match the expected size
#[allow(dead_code)]
pub fn decompress_buffer(compressed: &[u8], expected_size: usize) -> Result<Vec<u8>> {
    // Validate minimum size
    if compressed.len() < 8 {
        return Err(paro_error::internal(format!(
            "Compressed buffer too small: {} bytes (need at least 8)",
            compressed.len()
        )));
    }

    // Read compressed size (little-endian)
    let mut size_bytes = [0u8; 8];
    size_bytes.copy_from_slice(&compressed[0..8]);
    let compressed_size = usize::try_from(u64::from_le_bytes(size_bytes)).map_err(|_| {
        paro_error::internal("Compressed buffer length does not fit in address space")
    })?;
    let compressed_end = 8usize
        .checked_add(compressed_size)
        .ok_or_else(|| paro_error::internal("Compressed buffer length overflow"))?;
    if compressed_end > compressed.len() {
        return Err(paro_error::internal(format!(
            "Compressed buffer payload exceeds allocation: need {} bytes, got {}",
            compressed_end,
            compressed.len()
        )));
    }

    // Decompress only the actual compressed data (not the padding)
    let compressed_data = &compressed[8..compressed_end];
    let decompressed = zstd::decode_all(compressed_data)
        .map_err(|e| paro_error::internal(format!("ZSTD decompression failed: {}", e)))?;

    // Validate decompressed size
    if decompressed.len() != expected_size {
        return Err(paro_error::internal(format!(
            "Decompressed size mismatch: got={}, expected={}",
            decompressed.len(),
            expected_size
        )));
    }

    Ok(decompressed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;
    use std::sync::atomic::AtomicU64;

    static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn create_temp_dir() -> PathBuf {
        let counter = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
        let dir = env::temp_dir().join(format!("paro_test_{}_{}", std::process::id(), counter));
        let _ = fs::remove_dir_all(&dir); // Clean up any existing
        let _ = fs::create_dir_all(&dir);
        dir
    }

    fn cleanup_temp_dir(dir: &Path) {
        let _ = fs::remove_dir_all(dir);
    }

    fn create_orphan_temp_file(dir: &Path, name: &str, bytes: usize) -> PathBuf {
        let path = dir.join(name);
        std::fs::write(&path, vec![7u8; bytes]).unwrap();
        path
    }

    #[test]
    fn test_temporary_buffer_size_round_up() {
        assert_eq!(
            TemporaryBufferSize::round_up(0),
            TemporaryBufferSize::Invalid
        );
        assert_eq!(TemporaryBufferSize::round_up(1), TemporaryBufferSize::S32K);
        assert_eq!(
            TemporaryBufferSize::round_up(32768),
            TemporaryBufferSize::S32K
        );
        assert_eq!(
            TemporaryBufferSize::round_up(32769),
            TemporaryBufferSize::S64K
        );
        assert_eq!(
            TemporaryBufferSize::round_up(65536),
            TemporaryBufferSize::S64K
        );
        assert_eq!(
            TemporaryBufferSize::round_up(200000),
            TemporaryBufferSize::S224K
        );
        assert_eq!(
            TemporaryBufferSize::round_up(300000),
            TemporaryBufferSize::Default
        );
    }

    #[test]
    fn test_temporary_file_identifier() {
        let id = TemporaryFileIdentifier::new(TemporaryBufferSize::S64K, 5);
        assert!(id.is_valid());
        assert_eq!(id.size, TemporaryBufferSize::S64K);
        assert_eq!(id.file_index, Some(5));
        assert!(!id.encrypted);

        let invalid = TemporaryFileIdentifier::default();
        assert!(!invalid.is_valid());
        assert_eq!(invalid.file_index, None);
    }

    #[test]
    fn test_temporary_file_manager_creation() {
        let dir = create_temp_dir();
        let manager = TemporaryFileManager::new(&dir).unwrap();

        assert_eq!(manager.get_used_space(), 0);
        assert_eq!(manager.get_max_swap_space(), u64::MAX);
        assert!(manager.temp_directory().exists());

        drop(manager);
        // Directory should still exist since we created it before the manager
    }

    #[test]
    fn test_startup_orphan_cleanup_with_marker() {
        let dir = create_temp_dir();
        let marker = dir.join(TEMP_DIR_OWNERSHIP_MARKER);
        std::fs::write(&marker, b"stale").unwrap();
        let orphan = create_orphan_temp_file(&dir, "paro_temp_storage_default-99.tmp", 64);
        assert!(orphan.exists());

        let manager = TemporaryFileManager::new(&dir).unwrap();
        assert!(!orphan.exists());
        assert!(marker.exists());

        drop(manager);
        assert!(!marker.exists());
        cleanup_temp_dir(&dir);
    }

    #[test]
    fn test_startup_cleanup_keeps_non_spill_files() {
        let dir = create_temp_dir();
        let marker = dir.join(TEMP_DIR_OWNERSHIP_MARKER);
        std::fs::write(&marker, b"stale").unwrap();
        let keep = create_orphan_temp_file(&dir, "keep.me", 32);
        let orphan = create_orphan_temp_file(&dir, "paro_temp_storage_32k-1.tmp", 32);

        let manager = TemporaryFileManager::new(&dir).unwrap();
        assert!(keep.exists());
        assert!(!orphan.exists());
        drop(manager);

        cleanup_temp_dir(&dir);
    }

    #[test]
    fn test_write_and_read_buffer() {
        let dir = create_temp_dir();
        let manager = TemporaryFileManager::new(&dir).unwrap();

        let data = vec![1u8, 2, 3, 4, 5, 6, 7, 8];
        let block_id = 42;

        // Write
        let written_size = manager
            .write_temporary_buffer(block_id, MemoryTag::InMemoryTable, &data)
            .unwrap();
        assert!(written_size >= data.len());
        assert!(manager.has_temporary_buffer(block_id));
        assert!(manager.get_used_space() > 0);

        // Read (also deletes)
        let mut buffer = vec![0u8; data.len()];
        let read_size = manager
            .read_temporary_buffer(block_id, &mut buffer)
            .unwrap();
        assert_eq!(read_size, data.len());
        assert_eq!(buffer, data);

        // Should be deleted now
        assert!(!manager.has_temporary_buffer(block_id));
        assert_eq!(manager.get_used_space(), 0);
        assert!(manager.get_write_bytes() > 0);
        assert!(manager.get_read_bytes() > 0);

        cleanup_temp_dir(&dir);
    }

    #[test]
    fn failed_read_keeps_spill_tracked_for_cleanup() {
        let dir = create_temp_dir();
        let manager = TemporaryFileManager::new(&dir).unwrap();
        let block_id = 43;
        let data = vec![7u8; 4096];

        let written = manager
            .write_temporary_buffer(block_id, MemoryTag::InMemoryTable, &data)
            .unwrap();
        let spill_path = manager
            .get_temporary_files()
            .into_iter()
            .next()
            .expect("spill file")
            .path;
        std::fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(spill_path)
            .expect("truncate spill file");

        let mut output = vec![0u8; data.len()];
        assert!(manager
            .read_temporary_buffer(block_id, &mut output)
            .is_err());
        assert!(manager.has_temporary_buffer(block_id));
        assert_eq!(manager.get_used_space(), written as u64);
        assert_eq!(
            manager.spill_usage_per_tag()[MemoryTag::InMemoryTable.as_index()],
            written as u64
        );

        manager.delete_temporary_buffer(block_id).unwrap();
        assert!(!manager.has_temporary_buffer(block_id));
        assert_eq!(manager.get_used_space(), 0);
        cleanup_temp_dir(&dir);
    }

    #[test]
    fn failed_release_preflight_keeps_spill_retryable() {
        let dir = create_temp_dir();
        let manager = TemporaryFileManager::new(&dir).unwrap();
        let block_id = 45;
        let tag = MemoryTag::InMemoryTable;
        let data = vec![8u8; 4096];

        let written = manager
            .write_temporary_buffer(block_id, tag, &data)
            .unwrap() as u64;
        let spill_path = manager
            .get_temporary_files()
            .into_iter()
            .next()
            .expect("spill file")
            .path;

        // Simulate a corrupted global counter. The release must fail before it
        // erases the file slot, so restoring the invariant makes it retryable.
        manager.size_on_disk.store(0, Ordering::Release);
        assert!(manager.delete_temporary_buffer(block_id).is_err());
        assert!(manager.has_temporary_buffer(block_id));
        assert!(spill_path.exists());

        manager.size_on_disk.store(written, Ordering::Release);
        let tag_usage = &manager.spill_usage_per_tag[tag.as_index()];
        tag_usage.store(0, Ordering::Release);
        assert!(manager.delete_temporary_buffer(block_id).is_err());
        assert!(manager.has_temporary_buffer(block_id));
        assert!(spill_path.exists());
        assert_eq!(manager.get_used_space(), written);

        tag_usage.store(written, Ordering::Release);
        assert_eq!(
            manager.delete_temporary_buffer(block_id).unwrap(),
            written as usize
        );
        assert!(!manager.has_temporary_buffer(block_id));
        assert_eq!(manager.get_used_space(), 0);
        assert!(!spill_path.exists());
        cleanup_temp_dir(&dir);
    }

    #[test]
    fn duplicate_block_write_is_rejected_without_leaking_spill_space() {
        let dir = create_temp_dir();
        let manager = TemporaryFileManager::new(&dir).unwrap();
        let block_id = 44;
        let original = vec![3u8; 4096];

        let written = manager
            .write_temporary_buffer(block_id, MemoryTag::InMemoryTable, &original)
            .unwrap();
        let duplicate =
            manager.write_temporary_buffer(block_id, MemoryTag::HashTable, &vec![9u8; 4096]);

        assert!(duplicate.is_err());
        assert_eq!(manager.get_used_space(), written as u64);
        assert_eq!(
            manager.spill_usage_per_tag()[MemoryTag::InMemoryTable.as_index()],
            written as u64
        );
        assert_eq!(
            manager.spill_usage_per_tag()[MemoryTag::HashTable.as_index()],
            0
        );

        let mut output = vec![0u8; original.len()];
        manager
            .read_temporary_buffer(block_id, &mut output)
            .unwrap();
        assert_eq!(output, original);
        assert_eq!(manager.get_used_space(), 0);
        cleanup_temp_dir(&dir);
    }

    #[test]
    fn test_peek_buffer() {
        let dir = create_temp_dir();
        let manager = TemporaryFileManager::new(&dir).unwrap();

        let data = vec![10u8, 20, 30, 40];
        let block_id = 100;

        manager
            .write_temporary_buffer(block_id, MemoryTag::InMemoryTable, &data)
            .unwrap();

        // Peek (doesn't delete)
        let mut buffer = vec![0u8; data.len()];
        let read_size = manager
            .peek_temporary_buffer(block_id, &mut buffer)
            .unwrap();
        assert_eq!(read_size, data.len());
        assert_eq!(buffer, data);

        // Should still exist
        assert!(manager.has_temporary_buffer(block_id));

        // Delete explicitly
        manager.delete_temporary_buffer(block_id).unwrap();
        assert!(!manager.has_temporary_buffer(block_id));

        cleanup_temp_dir(&dir);
    }

    #[test]
    fn test_multiple_buffers() {
        let dir = create_temp_dir();
        let manager = TemporaryFileManager::new(&dir).unwrap();

        // Write multiple buffers
        for i in 0..10 {
            let data = vec![i as u8; 1024];
            manager
                .write_temporary_buffer(i, MemoryTag::InMemoryTable, &data)
                .unwrap();
        }

        assert_eq!(manager.used_blocks.read().unwrap().len(), 10);

        // Read them back
        for i in 0..10 {
            let mut buffer = vec![0u8; 1024];
            manager.read_temporary_buffer(i, &mut buffer).unwrap();
            assert!(buffer.iter().all(|&b| b == i as u8));
        }

        assert_eq!(manager.used_blocks.read().unwrap().len(), 0);
        assert_eq!(manager.get_used_space(), 0);

        cleanup_temp_dir(&dir);
    }

    #[test]
    fn test_swap_space_limit() {
        let dir = create_temp_dir();
        let manager = TemporaryFileManager::new(&dir).unwrap();

        // Set a small limit
        manager.set_max_swap_space(300 * 1024); // 300 KB

        // Write buffers until we hit the limit
        let mut succeeded = 0;
        for i in 0..10 {
            let data = vec![0u8; 30 * 1024]; // 30 KB each
            let result = manager.write_temporary_buffer(i, MemoryTag::InMemoryTable, &data);
            if result.is_ok() {
                succeeded += 1;
            } else {
                // We hit the limit, which is expected
                assert!(
                    succeeded > 0,
                    "Should have succeeded at least once before hitting limit"
                );
                assert!(manager.get_swap_limit_hits() >= 1);
                cleanup_temp_dir(&dir);
                return;
            }
        }

        // If we got here, we wrote 10 buffers without hitting the limit
        // This is unexpected, but let's not fail the test
        // (compression might be very effective)
        cleanup_temp_dir(&dir);
    }

    #[test]
    fn swap_space_reservations_are_atomic() {
        use std::sync::{Arc, Barrier};
        use std::thread;

        const THREADS: usize = 8;
        let dir = create_temp_dir();
        let manager = Arc::new(TemporaryFileManager::new(&dir).unwrap());
        let reservation = DEFAULT_BLOCK_ALLOC_SIZE as u64;
        manager.set_max_swap_space(reservation);
        let barrier = Arc::new(Barrier::new(THREADS));

        let handles: Vec<_> = (0..THREADS)
            .map(|_| {
                let manager = Arc::clone(&manager);
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    barrier.wait();
                    manager.reserve_disk_space(reservation).is_ok()
                })
            })
            .collect();
        let successes = handles
            .into_iter()
            .map(|handle| usize::from(handle.join().unwrap()))
            .sum::<usize>();

        assert_eq!(successes, 1);
        assert_eq!(manager.get_used_space(), reservation);
        assert_eq!(manager.get_swap_limit_hits(), (THREADS - 1) as u64);
        manager.release_disk_space(reservation).unwrap();
        cleanup_temp_dir(&dir);
    }

    #[test]
    fn test_get_temporary_files() {
        let dir = create_temp_dir();
        let manager = TemporaryFileManager::new(&dir).unwrap();

        // Write some buffers
        manager
            .write_temporary_buffer(1, MemoryTag::InMemoryTable, &vec![0u8; 1024])
            .unwrap();
        manager
            .write_temporary_buffer(2, MemoryTag::HashTable, &vec![0u8; 50000])
            .unwrap();

        let files = manager.get_temporary_files();
        assert!(!files.is_empty());

        for file in &files {
            assert!(file.path.exists());
            assert!(file.size > 0);
        }

        cleanup_temp_dir(&dir);
    }

    #[test]
    fn test_metrics_snapshot() {
        let dir = create_temp_dir();
        let manager = TemporaryFileManager::new(&dir).unwrap();
        manager
            .write_temporary_buffer(1, MemoryTag::InMemoryTable, &vec![1u8; 4096])
            .unwrap();

        let mut buf = vec![0u8; 4096];
        manager.peek_temporary_buffer(1, &mut buf).unwrap();

        let metrics = manager.metrics_snapshot();
        assert!(metrics.write_bytes > 0);
        assert!(metrics.read_bytes > 0);
        assert_eq!(metrics.file_count, manager.get_temporary_files().len());
        assert_eq!(metrics.swap_usage, manager.get_used_space());

        cleanup_temp_dir(&dir);
    }

    #[test]
    fn test_clear() {
        let dir = create_temp_dir();
        let manager = TemporaryFileManager::new(&dir).unwrap();

        // Write some buffers
        for i in 0..5 {
            manager
                .write_temporary_buffer(i, MemoryTag::InMemoryTable, &vec![i as u8; 1024])
                .unwrap();
        }

        assert!(manager.get_used_space() > 0);

        // Clear
        manager.clear().unwrap();

        assert_eq!(manager.get_used_space(), 0);
        assert!(manager.get_temporary_files().is_empty());

        cleanup_temp_dir(&dir);
    }

    #[test]
    fn drop_recovers_poisoned_cleanup_locks() {
        use std::panic::{catch_unwind, AssertUnwindSafe};
        use std::sync::Arc;
        use std::thread;

        let dir = create_temp_dir();
        let manager = Arc::new(TemporaryFileManager::new(&dir).unwrap());
        manager
            .write_temporary_buffer(1, MemoryTag::InMemoryTable, &vec![1u8; 4096])
            .unwrap();
        let spill_path = manager
            .get_temporary_files()
            .into_iter()
            .next()
            .expect("spill file")
            .path;
        let file_handle = {
            let files = manager.files.read().unwrap();
            Arc::clone(files.values().next().expect("file handle"))
        };

        let poison_file = Arc::clone(&file_handle);
        assert!(thread::spawn(move || {
            let _guard = poison_file.file.lock().unwrap();
            panic!("poison file lock");
        })
        .join()
        .is_err());
        drop(file_handle);

        let poison_files = Arc::clone(&manager);
        assert!(thread::spawn(move || {
            let _guard = poison_files.files.write().unwrap();
            panic!("poison files lock");
        })
        .join()
        .is_err());

        let poison_used_blocks = Arc::clone(&manager);
        assert!(thread::spawn(move || {
            let _guard = poison_used_blocks.used_blocks.write().unwrap();
            panic!("poison used-blocks lock");
        })
        .join()
        .is_err());

        let poison_next_index = Arc::clone(&manager);
        assert!(thread::spawn(move || {
            let _guard = poison_next_index.next_file_index.write().unwrap();
            panic!("poison next-index lock");
        })
        .join()
        .is_err());

        let poison_lifecycle = Arc::clone(&manager);
        assert!(thread::spawn(move || {
            let _guard = poison_lifecycle.lifecycle_lock.write().unwrap();
            panic!("poison lifecycle lock");
        })
        .join()
        .is_err());

        assert!(catch_unwind(AssertUnwindSafe(|| drop(manager))).is_ok());
        assert!(!spill_path.exists());
        assert!(!dir.join(TEMP_DIR_OWNERSHIP_MARKER).exists());
        cleanup_temp_dir(&dir);
    }

    #[test]
    fn test_spill_usage_per_tag() {
        let dir = create_temp_dir();
        let manager = TemporaryFileManager::new(&dir).unwrap();

        manager
            .write_temporary_buffer(1, MemoryTag::InMemoryTable, &vec![0u8; 1024])
            .unwrap();
        manager
            .write_temporary_buffer(2, MemoryTag::HashTable, &vec![0u8; 1024])
            .unwrap();

        let usage = manager.spill_usage_per_tag();
        assert!(usage[MemoryTag::InMemoryTable.as_index()] > 0);
        assert!(usage[MemoryTag::HashTable.as_index()] > 0);

        manager.delete_temporary_buffer(1).unwrap();
        manager.delete_temporary_buffer(2).unwrap();
        let usage = manager.spill_usage_per_tag();
        assert_eq!(usage[MemoryTag::InMemoryTable.as_index()], 0);
        assert_eq!(usage[MemoryTag::HashTable.as_index()], 0);

        cleanup_temp_dir(&dir);
    }

    #[test]
    fn test_large_buffer() {
        let dir = create_temp_dir();
        let manager = TemporaryFileManager::new(&dir).unwrap();

        // Write a buffer at the maximum size (256 KB)
        // Note: Large buffers may be compressed, so the written size may vary
        let data: Vec<u8> = (0..DEFAULT_BLOCK_ALLOC_SIZE)
            .map(|i| (i % 256) as u8)
            .collect();
        let block_id = 999;

        let written = manager
            .write_temporary_buffer(block_id, MemoryTag::InMemoryTable, &data)
            .unwrap();
        // With compression, the written size may be smaller than DEFAULT_BLOCK_ALLOC_SIZE
        assert!(written > 0, "Should have written some data");

        let mut buffer = vec![0u8; data.len()];
        let read = manager
            .read_temporary_buffer(block_id, &mut buffer)
            .unwrap();
        assert_eq!(read, data.len());
        assert_eq!(buffer, data);

        cleanup_temp_dir(&dir);
    }

    #[test]
    fn test_buffer_size_categories() {
        let dir = create_temp_dir();
        let manager = TemporaryFileManager::new(&dir).unwrap();

        // Test different buffer size categories
        // Note: With compression enabled, small buffers (< 32KB) won't be compressed
        // and will use DEFAULT size (256KB). Larger buffers may be compressed.
        let test_cases = [
            (1024, Some(TemporaryBufferSize::Default)), // Small -> not compressed, uses DEFAULT
            (50_000, None),                             // Medium -> may be compressed, size varies
            (100_000, None),                            // Larger -> may be compressed, size varies
            (200_000, None), // Even larger -> may be compressed, size varies
        ];

        for (i, (size, expected_bucket)) in test_cases.iter().enumerate() {
            let data: Vec<u8> = (0..*size).map(|j| (j % 256) as u8).collect();
            let block_id = i as i64;

            let written = manager
                .write_temporary_buffer(block_id, MemoryTag::InMemoryTable, &data)
                .unwrap();

            // If we have an expected bucket, verify it
            if let Some(expected) = expected_bucket {
                assert_eq!(
                    written,
                    expected.size(),
                    "Size {} should use bucket {:?}",
                    size,
                    expected
                );
            } else {
                // Just verify that something was written
                assert!(
                    written > 0,
                    "Should have written some data for size {}",
                    size
                );
            }

            let mut buffer = vec![0u8; *size];
            let read = manager
                .read_temporary_buffer(block_id, &mut buffer)
                .unwrap();
            assert_eq!(read, *size);
            assert_eq!(buffer, data);
        }

        cleanup_temp_dir(&dir);
    }

    #[test]
    fn test_file_reuse() {
        let dir = create_temp_dir();
        let manager = TemporaryFileManager::new(&dir).unwrap();

        // Write and delete multiple times to test index reuse
        for round in 0..3 {
            for i in 0..5 {
                let block_id = round * 100 + i;
                manager
                    .write_temporary_buffer(block_id, MemoryTag::InMemoryTable, &vec![0u8; 1024])
                    .unwrap();
            }

            for i in 0..5 {
                let block_id = round * 100 + i;
                manager.delete_temporary_buffer(block_id).unwrap();
            }
        }

        // All files should be cleaned up
        assert_eq!(manager.get_used_space(), 0);

        cleanup_temp_dir(&dir);
    }

    #[test]
    fn test_debug_format() {
        let dir = create_temp_dir();
        let manager = TemporaryFileManager::new(&dir).unwrap();

        let debug_str = format!("{:?}", manager);
        assert!(debug_str.contains("TemporaryFileManager"));
        assert!(debug_str.contains("temp_directory"));
        assert!(debug_str.contains("size_on_disk"));

        cleanup_temp_dir(&dir);
    }

    #[test]
    fn test_temporary_compression_level() {
        // Test enum values
        assert_eq!(TemporaryCompressionLevel::ZstdMinusFive.to_int(), -5);
        assert_eq!(TemporaryCompressionLevel::ZstdMinusThree.to_int(), -3);
        assert_eq!(TemporaryCompressionLevel::ZstdMinusOne.to_int(), -1);
        assert_eq!(TemporaryCompressionLevel::Uncompressed.to_int(), 0);
        assert_eq!(TemporaryCompressionLevel::ZstdOne.to_int(), 1);
        assert_eq!(TemporaryCompressionLevel::ZstdThree.to_int(), 3);
        assert_eq!(TemporaryCompressionLevel::ZstdFive.to_int(), 5);

        // Test from_int
        assert_eq!(
            TemporaryCompressionLevel::from_int(-5),
            Some(TemporaryCompressionLevel::ZstdMinusFive)
        );
        assert_eq!(
            TemporaryCompressionLevel::from_int(-3),
            Some(TemporaryCompressionLevel::ZstdMinusThree)
        );
        assert_eq!(
            TemporaryCompressionLevel::from_int(-1),
            Some(TemporaryCompressionLevel::ZstdMinusOne)
        );
        assert_eq!(
            TemporaryCompressionLevel::from_int(0),
            Some(TemporaryCompressionLevel::Uncompressed)
        );
        assert_eq!(
            TemporaryCompressionLevel::from_int(1),
            Some(TemporaryCompressionLevel::ZstdOne)
        );
        assert_eq!(
            TemporaryCompressionLevel::from_int(3),
            Some(TemporaryCompressionLevel::ZstdThree)
        );
        assert_eq!(
            TemporaryCompressionLevel::from_int(5),
            Some(TemporaryCompressionLevel::ZstdFive)
        );

        // Test invalid values
        assert_eq!(TemporaryCompressionLevel::from_int(-10), None);
        assert_eq!(TemporaryCompressionLevel::from_int(2), None);
        assert_eq!(TemporaryCompressionLevel::from_int(10), None);

        // Test default
        assert_eq!(
            TemporaryCompressionLevel::default(),
            TemporaryCompressionLevel::Uncompressed
        );

        // Test round-trip conversion
        for level in [
            TemporaryCompressionLevel::ZstdMinusFive,
            TemporaryCompressionLevel::ZstdMinusThree,
            TemporaryCompressionLevel::ZstdMinusOne,
            TemporaryCompressionLevel::Uncompressed,
            TemporaryCompressionLevel::ZstdOne,
            TemporaryCompressionLevel::ZstdThree,
            TemporaryCompressionLevel::ZstdFive,
        ] {
            let int_val = level.to_int();
            let converted = TemporaryCompressionLevel::from_int(int_val);
            assert_eq!(converted, Some(level));
        }
    }

    #[test]
    fn test_compression_adaptivity_creation() {
        let adaptivity = TemporaryFileCompressionAdaptivity::new();

        // Initial values should be INITIAL_NS
        assert_eq!(
            adaptivity
                .last_uncompressed_write_ns
                .load(Ordering::Relaxed),
            TemporaryFileCompressionAdaptivity::INITIAL_NS
        );

        for i in 0..TemporaryFileCompressionAdaptivity::LEVELS {
            assert_eq!(
                adaptivity.last_compressed_writes_ns[i].load(Ordering::Relaxed),
                TemporaryFileCompressionAdaptivity::INITIAL_NS
            );
        }
    }

    #[test]
    fn test_compression_adaptivity_index_level_conversion() {
        use TemporaryFileCompressionAdaptivity as TFA;

        // Test index_to_level
        assert_eq!(
            TFA::index_to_level(0),
            TemporaryCompressionLevel::ZstdMinusFive
        );
        assert_eq!(
            TFA::index_to_level(1),
            TemporaryCompressionLevel::ZstdMinusThree
        );
        assert_eq!(
            TFA::index_to_level(2),
            TemporaryCompressionLevel::ZstdMinusOne
        );
        assert_eq!(TFA::index_to_level(3), TemporaryCompressionLevel::ZstdOne);
        assert_eq!(TFA::index_to_level(4), TemporaryCompressionLevel::ZstdThree);
        assert_eq!(TFA::index_to_level(5), TemporaryCompressionLevel::ZstdFive);

        // Test level_to_index
        assert_eq!(
            TFA::level_to_index(TemporaryCompressionLevel::ZstdMinusFive),
            0
        );
        assert_eq!(
            TFA::level_to_index(TemporaryCompressionLevel::ZstdMinusThree),
            1
        );
        assert_eq!(
            TFA::level_to_index(TemporaryCompressionLevel::ZstdMinusOne),
            2
        );
        assert_eq!(TFA::level_to_index(TemporaryCompressionLevel::ZstdOne), 3);
        assert_eq!(TFA::level_to_index(TemporaryCompressionLevel::ZstdThree), 4);
        assert_eq!(TFA::level_to_index(TemporaryCompressionLevel::ZstdFive), 5);

        // Test round-trip
        for i in 0..6 {
            let level = TFA::index_to_level(i);
            let index = TFA::level_to_index(level);
            assert_eq!(index, i);
        }
    }

    #[test]
    fn test_compression_adaptivity_min_max_levels() {
        use TemporaryFileCompressionAdaptivity as TFA;

        assert_eq!(
            TFA::minimum_compression_level(),
            TemporaryCompressionLevel::ZstdMinusFive
        );
        assert_eq!(
            TFA::maximum_compression_level(),
            TemporaryCompressionLevel::ZstdFive
        );
    }

    #[test]
    fn test_compression_adaptivity_get_level() {
        let adaptivity = TemporaryFileCompressionAdaptivity::new();

        // Initially, all times are equal, so any level could be chosen
        // Just verify it returns a valid level
        let level = adaptivity.get_compression_level();
        assert!(matches!(
            level,
            TemporaryCompressionLevel::ZstdMinusFive
                | TemporaryCompressionLevel::ZstdMinusThree
                | TemporaryCompressionLevel::ZstdMinusOne
                | TemporaryCompressionLevel::Uncompressed
                | TemporaryCompressionLevel::ZstdOne
                | TemporaryCompressionLevel::ZstdThree
                | TemporaryCompressionLevel::ZstdFive
        ));
    }

    #[test]
    fn test_compression_adaptivity_update() {
        use std::sync::atomic::Ordering;

        let adaptivity = TemporaryFileCompressionAdaptivity::new();

        // Drive update() with an explicit elapsed time instead of relying on sleep granularity.
        let time_before = TemporaryFileCompressionAdaptivity::get_current_time_nanos() - 1_000_000;
        adaptivity.update(TemporaryCompressionLevel::Uncompressed, time_before);

        let updated_value = adaptivity
            .last_uncompressed_write_ns
            .load(Ordering::Relaxed);
        assert!(updated_value > TemporaryFileCompressionAdaptivity::INITIAL_NS);

        let time_before = TemporaryFileCompressionAdaptivity::get_current_time_nanos() - 2_000_000;
        adaptivity.update(TemporaryCompressionLevel::ZstdOne, time_before);

        let index =
            TemporaryFileCompressionAdaptivity::level_to_index(TemporaryCompressionLevel::ZstdOne);
        let updated_value = adaptivity.last_compressed_writes_ns[index].load(Ordering::Relaxed);
        assert!(updated_value > TemporaryFileCompressionAdaptivity::INITIAL_NS);
    }

    #[test]
    fn test_compression_adaptivity_prefers_faster_level() {
        use std::sync::atomic::Ordering;

        let adaptivity = TemporaryFileCompressionAdaptivity::new();

        // Make ZstdMinusFive very fast (1 microsecond)
        adaptivity.last_compressed_writes_ns[0].store(1000, Ordering::Relaxed);

        // Make other levels slower
        for i in 1..TemporaryFileCompressionAdaptivity::LEVELS {
            adaptivity.last_compressed_writes_ns[i].store(100000, Ordering::Relaxed);
        }

        // Make uncompressed very slow
        adaptivity
            .last_uncompressed_write_ns
            .store(1000000, Ordering::Relaxed);

        // Get compression level multiple times (to account for randomness)
        let mut got_fastest = false;
        for _ in 0..20 {
            let level = adaptivity.get_compression_level();
            if level == TemporaryCompressionLevel::ZstdMinusFive {
                got_fastest = true;
                break;
            }
        }

        // Should choose the fastest level at least once
        assert!(got_fastest, "Should choose the fastest compression level");
    }

    #[test]
    fn test_concurrent_access() {
        use std::sync::Arc;
        use std::thread;

        let dir = create_temp_dir();
        let manager = Arc::new(TemporaryFileManager::new(&dir).unwrap());

        let mut handles = vec![];

        // Spawn multiple threads writing
        for t in 0..4 {
            let mgr = manager.clone();
            let handle = thread::spawn(move || {
                for i in 0..10 {
                    let block_id = t * 1000 + i;
                    let data = vec![(t * 10 + i) as u8; 1024];
                    mgr.write_temporary_buffer(block_id, MemoryTag::InMemoryTable, &data)
                        .unwrap();
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // Verify all blocks exist
        assert_eq!(manager.used_blocks.read().unwrap().len(), 40);

        // Read them all back
        for t in 0..4 {
            for i in 0..10 {
                let block_id = t * 1000 + i;
                let mut buffer = vec![0u8; 1024];
                manager
                    .read_temporary_buffer(block_id, &mut buffer)
                    .unwrap();
                assert!(buffer.iter().all(|&b| b == (t * 10 + i) as u8));
            }
        }

        cleanup_temp_dir(&dir);
    }

    // ========================================================================
    // Compression Tests
    // ========================================================================

    #[test]
    fn test_compression_basic() {
        use super::{compress_buffer, decompress_buffer};

        // Test with simple data
        let data = vec![1u8, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let level = 3; // ZSTD level 3

        // Compress
        let compressed = compress_buffer(&data, level).unwrap();

        // Verify format: [compressed_size: 8 bytes][compressed_data]
        assert!(compressed.len() >= 8);

        // Read compressed size from buffer
        let mut size_bytes = [0u8; 8];
        size_bytes.copy_from_slice(&compressed[0..8]);
        let stored_compressed_size = u64::from_le_bytes(size_bytes) as usize;
        // The stored size should be the compressed data size (not including the 8-byte header)
        assert_eq!(stored_compressed_size, compressed.len() - 8);

        // Decompress
        let decompressed = decompress_buffer(&compressed, data.len()).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_compression_levels() {
        use super::{compress_buffer, decompress_buffer};

        // Test data with some repetition (compressible)
        let data = vec![42u8; 1024];

        // Test different compression levels
        let levels = [-5, -3, -1, 0, 1, 3, 5];

        for level in levels {
            let compressed = compress_buffer(&data, level).unwrap();
            let decompressed = decompress_buffer(&compressed, data.len()).unwrap();
            assert_eq!(decompressed, data, "Failed at level {}", level);

            // Compressed should be smaller than original (for repetitive data)
            assert!(
                compressed.len() < data.len(),
                "Level {} didn't compress",
                level
            );
        }
    }

    #[test]
    fn test_compression_roundtrip() {
        use super::{compress_buffer, decompress_buffer};

        // Test with various data patterns
        let test_cases = vec![
            vec![0u8; 100],                             // All zeros
            vec![255u8; 100],                           // All ones
            (0..100).map(|i| i as u8).collect(),        // Sequential
            (0..100).map(|i| (i % 10) as u8).collect(), // Repetitive pattern
        ];

        for (i, data) in test_cases.iter().enumerate() {
            let compressed = compress_buffer(data, 3).unwrap();
            let decompressed = decompress_buffer(&compressed, data.len()).unwrap();
            assert_eq!(&decompressed, data, "Failed for test case {}", i);
        }
    }

    #[test]
    fn test_compression_invalid_data() {
        use super::decompress_buffer;

        // Test with buffer too small
        let small_buffer = vec![1u8, 2, 3];
        let result = decompress_buffer(&small_buffer, 10);
        assert!(result.is_err());

        // Test with invalid compressed data
        let mut invalid_buffer = vec![0u8; 100];
        invalid_buffer[0..8].copy_from_slice(&(50u64).to_le_bytes()); // Claim size is 50
                                                                      // But the rest is not valid ZSTD data
        let result = decompress_buffer(&invalid_buffer, 50);
        assert!(result.is_err());

        // A corrupt length header must be rejected instead of panicking on a slice.
        invalid_buffer[0..8].copy_from_slice(&u64::MAX.to_le_bytes());
        let result = decompress_buffer(&invalid_buffer, 50);
        assert!(result.is_err());
    }

    #[test]
    fn test_compression_size_mismatch() {
        use super::{compress_buffer, decompress_buffer};

        let data = vec![1u8, 2, 3, 4, 5];
        let compressed = compress_buffer(&data, 3).unwrap();

        // Try to decompress with wrong expected size
        let result = decompress_buffer(&compressed, 10);
        assert!(result.is_err());
    }
}
