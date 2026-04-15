//! BufferAllocator - Allocator that integrates with BufferManager.
//!
//! This allocator wraps a `BufferManager` so callers can allocate tracked
//! memory without depending on storage-layer types directly.

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, RwLock};

#[cfg(debug_assertions)]
use super::debug_info::AllocatorDebugInfo;
use super::Allocator;
use crate::error::Result;

/// Number of memory tags (must match MemoryTag enum variants).
pub const MEMORY_TAG_COUNT: usize = 20;

/// Memory tag for tracking allocation purposes.
///
/// Each allocation is tagged with a category to enable fine-grained
/// memory tracking and reporting.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
#[derive(Default)]
pub enum MemoryTag {
    /// Base table data (rowsets, segments, column segments)
    BaseTable = 0,
    /// Hash table for joins/aggregates
    HashTable = 1,
    /// Parquet reader buffers
    ParquetReader = 2,
    /// CSV reader buffers
    CsvReader = 3,
    /// Sorting buffers (ORDER BY)
    OrderBy = 4,
    /// ART index nodes
    ArtIndex = 5,
    /// Column data (decompressed pages, late-mat rowids/bitmaps)
    ColumnData = 6,
    /// Metadata (page cache index, catalog/schema)
    Metadata = 7,
    /// Overflow strings (out-of-line string data)
    OverflowStrings = 8,
    /// In-memory tables (MemTable / write buffer)
    InMemoryTable = 9,
    /// General allocator usage
    #[default]
    Allocator = 10,
    /// Extension memory
    Extension = 11,
    /// Transaction state
    Transaction = 12,
    /// External file cache (compressed pages, prefetch buffers)
    ExternalFileCache = 13,
    /// Window function state
    Window = 14,
    /// Storage page cache (compressed/decompressed cached pages)
    PageCache = 15,
    /// Compaction working set
    Compaction = 16,
    /// Vector index buffers (HNSW/Sparse)
    VectorIndex = 17,
    /// Write-ahead log buffers
    Wal = 18,
    /// MemTable/write-buffer working set
    MemTable = 19,
}

impl MemoryTag {
    /// Get the tag name as a string.
    pub fn name(&self) -> &'static str {
        match self {
            MemoryTag::BaseTable => "BASE_TABLE",
            MemoryTag::HashTable => "HASH_TABLE",
            MemoryTag::ParquetReader => "PARQUET_READER",
            MemoryTag::CsvReader => "CSV_READER",
            MemoryTag::OrderBy => "ORDER_BY",
            MemoryTag::ArtIndex => "ART_INDEX",
            MemoryTag::ColumnData => "COLUMN_DATA",
            MemoryTag::Metadata => "METADATA",
            MemoryTag::OverflowStrings => "OVERFLOW_STRINGS",
            MemoryTag::InMemoryTable => "IN_MEMORY_TABLE",
            MemoryTag::Allocator => "ALLOCATOR",
            MemoryTag::Extension => "EXTENSION",
            MemoryTag::Transaction => "TRANSACTION",
            MemoryTag::ExternalFileCache => "EXTERNAL_FILE_CACHE",
            MemoryTag::Window => "WINDOW",
            MemoryTag::PageCache => "PAGE_CACHE",
            MemoryTag::Compaction => "COMPACTION",
            MemoryTag::VectorIndex => "VECTOR_INDEX",
            MemoryTag::Wal => "WAL",
            MemoryTag::MemTable => "MEM_TABLE",
        }
    }

    /// Get all memory tags.
    pub fn all() -> &'static [MemoryTag] {
        &[
            MemoryTag::BaseTable,
            MemoryTag::HashTable,
            MemoryTag::ParquetReader,
            MemoryTag::CsvReader,
            MemoryTag::OrderBy,
            MemoryTag::ArtIndex,
            MemoryTag::ColumnData,
            MemoryTag::Metadata,
            MemoryTag::OverflowStrings,
            MemoryTag::InMemoryTable,
            MemoryTag::Allocator,
            MemoryTag::Extension,
            MemoryTag::Transaction,
            MemoryTag::ExternalFileCache,
            MemoryTag::Window,
            MemoryTag::PageCache,
            MemoryTag::Compaction,
            MemoryTag::VectorIndex,
            MemoryTag::Wal,
            MemoryTag::MemTable,
        ]
    }

    /// Convert tag to index for array access.
    #[inline]
    pub fn as_index(&self) -> usize {
        *self as usize
    }

    /// Try to convert from index to tag.
    pub fn from_index(index: usize) -> Option<MemoryTag> {
        match index {
            0 => Some(MemoryTag::BaseTable),
            1 => Some(MemoryTag::HashTable),
            2 => Some(MemoryTag::ParquetReader),
            3 => Some(MemoryTag::CsvReader),
            4 => Some(MemoryTag::OrderBy),
            5 => Some(MemoryTag::ArtIndex),
            6 => Some(MemoryTag::ColumnData),
            7 => Some(MemoryTag::Metadata),
            8 => Some(MemoryTag::OverflowStrings),
            9 => Some(MemoryTag::InMemoryTable),
            10 => Some(MemoryTag::Allocator),
            11 => Some(MemoryTag::Extension),
            12 => Some(MemoryTag::Transaction),
            13 => Some(MemoryTag::ExternalFileCache),
            14 => Some(MemoryTag::Window),
            15 => Some(MemoryTag::PageCache),
            16 => Some(MemoryTag::Compaction),
            17 => Some(MemoryTag::VectorIndex),
            18 => Some(MemoryTag::Wal),
            19 => Some(MemoryTag::MemTable),
            _ => None,
        }
    }
}

impl std::fmt::Display for MemoryTag {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

/// Memory usage tracking per tag.
///
/// Tracks memory usage for each `MemoryTag` category using atomic counters.
/// This enables thread-safe memory tracking without locks.
#[derive(Debug)]
pub struct MemoryUsage {
    /// Memory usage per tag (in bytes)
    usage_per_tag: [AtomicI64; MEMORY_TAG_COUNT],
    /// Total memory usage (cached for fast access)
    total_usage: AtomicI64,
}

impl MemoryUsage {
    /// Create a new memory usage tracker.
    pub fn new() -> Self {
        // Initialize all counters to 0
        Self {
            usage_per_tag: std::array::from_fn(|_| AtomicI64::new(0)),
            total_usage: AtomicI64::new(0),
        }
    }

    /// Add memory usage for a specific tag.
    #[inline]
    pub fn add(&self, tag: MemoryTag, size: usize) {
        let size = size as i64;
        self.usage_per_tag[tag.as_index()].fetch_add(size, Ordering::AcqRel);
        self.total_usage.fetch_add(size, Ordering::AcqRel);
    }

    /// Subtract memory usage for a specific tag.
    #[inline]
    pub fn sub(&self, tag: MemoryTag, size: usize) {
        let size = size as i64;
        self.usage_per_tag[tag.as_index()].fetch_sub(size, Ordering::AcqRel);
        self.total_usage.fetch_sub(size, Ordering::AcqRel);
    }

    /// Get memory usage for a specific tag.
    #[inline]
    pub fn get(&self, tag: MemoryTag) -> i64 {
        self.usage_per_tag[tag.as_index()].load(Ordering::Acquire)
    }

    /// Get total memory usage across all tags.
    #[inline]
    pub fn total(&self) -> i64 {
        self.total_usage.load(Ordering::Acquire)
    }

    /// Get memory usage for all tags as a snapshot.
    pub fn snapshot(&self) -> MemoryUsageSnapshot {
        let mut usage = [0i64; MEMORY_TAG_COUNT];
        for (i, counter) in self.usage_per_tag.iter().enumerate() {
            usage[i] = counter.load(Ordering::Acquire);
        }
        MemoryUsageSnapshot {
            usage_per_tag: usage,
            total_usage: self.total_usage.load(Ordering::Acquire),
        }
    }

    /// Reset all counters to zero.
    pub fn reset(&self) {
        for counter in &self.usage_per_tag {
            counter.store(0, Ordering::Release);
        }
        self.total_usage.store(0, Ordering::Release);
    }
}

impl Default for MemoryUsage {
    fn default() -> Self {
        Self::new()
    }
}

/// Snapshot of memory usage at a point in time.
///
/// This is a non-atomic copy of MemoryUsage for reporting purposes.
#[derive(Debug, Clone)]
pub struct MemoryUsageSnapshot {
    /// Memory usage per tag (in bytes)
    pub usage_per_tag: [i64; MEMORY_TAG_COUNT],
    /// Total memory usage
    pub total_usage: i64,
}

impl MemoryUsageSnapshot {
    /// Get memory usage for a specific tag.
    #[inline]
    pub fn get(&self, tag: MemoryTag) -> i64 {
        self.usage_per_tag[tag.as_index()]
    }

    /// Get total memory usage.
    #[inline]
    pub fn total(&self) -> i64 {
        self.total_usage
    }

    /// Iterate over all tags with their usage.
    pub fn iter(&self) -> impl Iterator<Item = (MemoryTag, i64)> + '_ {
        MemoryTag::all()
            .iter()
            .map(|&tag| (tag, self.usage_per_tag[tag.as_index()]))
    }

    /// Get only non-zero entries.
    pub fn non_zero(&self) -> impl Iterator<Item = (MemoryTag, i64)> + '_ {
        self.iter().filter(|(_, usage)| *usage != 0)
    }

    /// Format as a human-readable string.
    pub fn format_report(&self) -> String {
        let mut report = String::new();
        report.push_str("Memory Usage Report:\n");
        report.push_str(&format!("  Total: {} bytes\n", self.total_usage));
        report.push_str("  By Tag:\n");
        for (tag, usage) in self.non_zero() {
            report.push_str(&format!("    {}: {} bytes\n", tag.name(), usage));
        }
        report
    }
}

/// Trait implemented by the memory manager (typically BufferPool in storage)
/// to provide tagged memory allocation.
///
/// This serves as the bridge between the compute layer (which uses `Allocator`)
/// and the storage layer (which manages physical buffers).
pub trait BufferManager: Send + Sync {
    /// Allocate memory with a specific tag.
    ///
    /// # Arguments
    /// * `tag` - Memory tag for tracking
    /// * `size` - Size in bytes to allocate
    ///
    /// # Returns
    /// A raw pointer to the allocated memory.
    fn allocate(&self, tag: MemoryTag, size: usize) -> Result<*mut u8>;

    /// Free memory previously allocated via this manager.
    ///
    /// # Arguments
    /// * `ptr` - Pointer to the memory to free
    /// * `tag` - Memory tag (must match allocation)
    /// * `size` - Size in bytes (must match allocation)
    fn free(&self, ptr: *mut u8, tag: MemoryTag, size: usize);

    /// Reallocate memory to a new size.
    ///
    /// # Arguments
    /// * `ptr` - Pointer to the memory to reallocate
    /// * `tag` - Memory tag (must match allocation)
    /// * `old_size` - Original size in bytes
    /// * `new_size` - New size in bytes
    ///
    /// # Returns
    /// A raw pointer to the reallocated memory.
    fn reallocate(
        &self,
        ptr: *mut u8,
        tag: MemoryTag,
        old_size: usize,
        new_size: usize,
    ) -> Result<*mut u8>;
}

/// Allocation tracking entry.
///
/// Stores metadata about an allocation for proper cleanup.
#[derive(Debug, Clone, Copy)]
struct AllocationEntry {
    /// Size of the allocation
    size: usize,
    /// Memory tag used for this allocation
    tag: MemoryTag,
}

/// Allocator implementation that delegates to a `BufferManager`.
///
/// This allows compute-layer components (Vectors, Chunks) to use
/// memory managed by the storage layer while keeping allocation tracking local.
///
/// # Example
/// ```ignore
/// use paro_common::allocator::{BufferAllocator, MemoryTag};
///
/// // Get allocator from execution context
/// let allocator = ctx.allocator(MemoryTag::HashTable);
///
/// // Allocate memory (tracked by BufferPool)
/// let ptr = allocator.allocate(1024)?;
///
/// // Memory is automatically tracked and can be evicted if needed
/// ```
pub struct BufferAllocator {
    /// The underlying buffer manager
    manager: Arc<dyn BufferManager>,
    /// Memory tag for all allocations through this allocator
    tag: MemoryTag,
    /// Track allocations for proper cleanup
    /// Maps pointer address to allocation metadata
    allocations: RwLock<HashMap<usize, AllocationEntry>>,
    /// Debug allocation tracking (debug builds only)
    #[cfg(debug_assertions)]
    debug_info: Arc<AllocatorDebugInfo>,
}

impl BufferAllocator {
    /// Create a new buffer allocator with the given manager and tag.
    ///
    /// # Arguments
    /// * `manager` - The BufferManager to delegate allocations to
    /// * `tag` - Memory tag for all allocations through this allocator
    pub fn new(manager: Arc<dyn BufferManager>, tag: MemoryTag) -> Self {
        Self {
            manager,
            tag,
            allocations: RwLock::new(HashMap::new()),
            #[cfg(debug_assertions)]
            debug_info: Arc::new(AllocatorDebugInfo::new("BufferAllocator")),
        }
    }

    /// Get the associated memory tag.
    #[inline]
    pub fn tag(&self) -> MemoryTag {
        self.tag
    }

    /// Get the underlying buffer manager.
    #[inline]
    pub fn manager(&self) -> &Arc<dyn BufferManager> {
        &self.manager
    }

    /// Get the number of active allocations.
    pub fn allocation_count(&self) -> usize {
        self.allocations.read().unwrap().len()
    }

    /// Get the total allocated size.
    pub fn allocated_size(&self) -> usize {
        self.allocations
            .read()
            .unwrap()
            .values()
            .map(|e| e.size)
            .sum()
    }
}

impl Allocator for BufferAllocator {
    fn allocate(&self, size: usize) -> Result<*mut u8> {
        if size == 0 {
            return Ok(std::ptr::null_mut());
        }

        let ptr = self.manager.allocate(self.tag, size)?;

        // Track the allocation
        let mut allocations = self.allocations.write().unwrap();
        allocations.insert(
            ptr as usize,
            AllocationEntry {
                size,
                tag: self.tag,
            },
        );

        #[cfg(debug_assertions)]
        self.debug_info.record_allocate(ptr, size);

        Ok(ptr)
    }

    fn allocate_zeroed(&self, size: usize) -> Result<*mut u8> {
        let ptr = self.allocate(size)?;
        if !ptr.is_null() && size > 0 {
            // SAFETY: ptr is valid and size bytes are allocated
            unsafe {
                std::ptr::write_bytes(ptr, 0, size);
            }
        }
        Ok(ptr)
    }

    fn free(&self, ptr: *mut u8, size: usize) {
        if ptr.is_null() {
            return;
        }

        // Remove from tracking
        let entry = {
            let mut allocations = self.allocations.write().unwrap();
            allocations.remove(&(ptr as usize))
        };

        // Use tracked size if available, otherwise use provided size
        let actual_size = entry.map(|e| e.size).unwrap_or(size);
        let actual_tag = entry.map(|e| e.tag).unwrap_or(self.tag);

        #[cfg(debug_assertions)]
        self.debug_info.record_free(ptr, actual_size);

        self.manager.free(ptr, actual_tag, actual_size);
    }

    fn reallocate(&self, ptr: *mut u8, old_size: usize, new_size: usize) -> Result<*mut u8> {
        if ptr.is_null() {
            return self.allocate(new_size);
        }

        if new_size == 0 {
            self.free(ptr, old_size);
            return Ok(std::ptr::null_mut());
        }

        // Get the actual old size from tracking
        let actual_old_size = {
            let allocations = self.allocations.read().unwrap();
            allocations
                .get(&(ptr as usize))
                .map(|e| e.size)
                .unwrap_or(old_size)
        };

        let new_ptr = self
            .manager
            .reallocate(ptr, self.tag, actual_old_size, new_size)?;

        #[cfg(debug_assertions)]
        self.debug_info
            .record_reallocate(ptr, new_ptr, actual_old_size, new_size);

        // Update tracking
        {
            let mut allocations = self.allocations.write().unwrap();
            allocations.remove(&(ptr as usize));
            allocations.insert(
                new_ptr as usize,
                AllocationEntry {
                    size: new_size,
                    tag: self.tag,
                },
            );
        }

        Ok(new_ptr)
    }

    fn name(&self) -> &'static str {
        "BufferAllocator"
    }
}

impl std::fmt::Debug for BufferAllocator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BufferAllocator")
            .field("tag", &self.tag)
            .field("allocation_count", &self.allocation_count())
            .field("allocated_size", &self.allocated_size())
            .finish()
    }
}

// SAFETY: BufferAllocator uses internal synchronization (RwLock)
// and the underlying BufferManager is required to be Send + Sync
unsafe impl Send for BufferAllocator {}
unsafe impl Sync for BufferAllocator {}

impl Drop for BufferAllocator {
    fn drop(&mut self) {
        // Free any remaining allocations
        let allocations = std::mem::take(self.allocations.get_mut().unwrap());
        for (ptr, entry) in allocations {
            #[cfg(debug_assertions)]
            self.debug_info.record_free(ptr as *mut u8, entry.size);
            self.manager.free(ptr as *mut u8, entry.tag, entry.size);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error as paro_error;

    struct TestBufferManager {
        usage: MemoryUsage,
    }

    impl TestBufferManager {
        fn new() -> Self {
            Self {
                usage: MemoryUsage::new(),
            }
        }
    }

    impl BufferManager for TestBufferManager {
        fn allocate(&self, tag: MemoryTag, size: usize) -> Result<*mut u8> {
            self.usage.add(tag, size);
            // Use standard allocation for testing
            let layout = std::alloc::Layout::from_size_align(size, 8).unwrap();
            // SAFETY: layout is valid
            let ptr = unsafe { std::alloc::alloc(layout) };
            if ptr.is_null() {
                return Err(paro_error::out_of_memory(format!(
                    "Failed to allocate {} bytes",
                    size
                )));
            }
            Ok(ptr)
        }

        fn free(&self, ptr: *mut u8, tag: MemoryTag, size: usize) {
            if ptr.is_null() {
                return;
            }
            self.usage.sub(tag, size);
            let layout = std::alloc::Layout::from_size_align(size, 8).unwrap();
            // SAFETY: ptr was allocated with this layout
            unsafe { std::alloc::dealloc(ptr, layout) };
        }

        fn reallocate(
            &self,
            ptr: *mut u8,
            tag: MemoryTag,
            old_size: usize,
            new_size: usize,
        ) -> Result<*mut u8> {
            if ptr.is_null() {
                return self.allocate(tag, new_size);
            }
            if new_size == 0 {
                self.free(ptr, tag, old_size);
                return Ok(std::ptr::null_mut());
            }

            let old_layout = std::alloc::Layout::from_size_align(old_size, 8).unwrap();
            let new_layout = std::alloc::Layout::from_size_align(new_size, 8).unwrap();

            // SAFETY: ptr was allocated with old_layout
            let new_ptr = unsafe { std::alloc::realloc(ptr, old_layout, new_layout.size()) };
            if new_ptr.is_null() {
                return Err(paro_error::out_of_memory(format!(
                    "Failed to reallocate {} bytes",
                    new_size
                )));
            }

            // Update usage tracking
            self.usage.sub(tag, old_size);
            self.usage.add(tag, new_size);

            Ok(new_ptr)
        }
    }

    #[test]
    fn test_memory_tag_count() {
        assert_eq!(MEMORY_TAG_COUNT, 20);
        assert_eq!(MemoryTag::all().len(), MEMORY_TAG_COUNT);
    }

    #[test]
    fn test_memory_tag_as_index() {
        assert_eq!(MemoryTag::BaseTable.as_index(), 0);
        assert_eq!(MemoryTag::HashTable.as_index(), 1);
        assert_eq!(MemoryTag::Window.as_index(), 14);
        assert_eq!(MemoryTag::MemTable.as_index(), 19);
    }

    #[test]
    fn test_memory_tag_from_index() {
        assert_eq!(MemoryTag::from_index(0), Some(MemoryTag::BaseTable));
        assert_eq!(MemoryTag::from_index(14), Some(MemoryTag::Window));
        assert_eq!(MemoryTag::from_index(15), Some(MemoryTag::PageCache));
        assert_eq!(MemoryTag::from_index(19), Some(MemoryTag::MemTable));
        assert_eq!(MemoryTag::from_index(20), None);
        assert_eq!(MemoryTag::from_index(100), None);
    }

    #[test]
    fn test_memory_tag_name() {
        assert_eq!(MemoryTag::BaseTable.name(), "BASE_TABLE");
        assert_eq!(MemoryTag::HashTable.name(), "HASH_TABLE");
        assert_eq!(MemoryTag::ArtIndex.name(), "ART_INDEX");
    }

    #[test]
    fn test_memory_tag_display() {
        assert_eq!(format!("{}", MemoryTag::BaseTable), "BASE_TABLE");
        assert_eq!(format!("{}", MemoryTag::Window), "WINDOW");
    }

    #[test]
    fn test_memory_tag_default() {
        assert_eq!(MemoryTag::default(), MemoryTag::Allocator);
    }

    #[test]
    fn test_memory_usage_new() {
        let usage = MemoryUsage::new();
        assert_eq!(usage.total(), 0);
        for tag in MemoryTag::all() {
            assert_eq!(usage.get(*tag), 0);
        }
    }

    #[test]
    fn test_memory_usage_add_sub() {
        let usage = MemoryUsage::new();

        usage.add(MemoryTag::HashTable, 1024);
        assert_eq!(usage.get(MemoryTag::HashTable), 1024);
        assert_eq!(usage.total(), 1024);

        usage.add(MemoryTag::OrderBy, 2048);
        assert_eq!(usage.get(MemoryTag::OrderBy), 2048);
        assert_eq!(usage.total(), 3072);

        usage.sub(MemoryTag::HashTable, 512);
        assert_eq!(usage.get(MemoryTag::HashTable), 512);
        assert_eq!(usage.total(), 2560);
    }

    #[test]
    fn test_memory_usage_snapshot() {
        let usage = MemoryUsage::new();
        usage.add(MemoryTag::BaseTable, 100);
        usage.add(MemoryTag::HashTable, 200);
        usage.add(MemoryTag::ArtIndex, 300);

        let snapshot = usage.snapshot();
        assert_eq!(snapshot.get(MemoryTag::BaseTable), 100);
        assert_eq!(snapshot.get(MemoryTag::HashTable), 200);
        assert_eq!(snapshot.get(MemoryTag::ArtIndex), 300);
        assert_eq!(snapshot.total(), 600);
    }

    #[test]
    fn test_memory_usage_snapshot_non_zero() {
        let usage = MemoryUsage::new();
        usage.add(MemoryTag::BaseTable, 100);
        usage.add(MemoryTag::HashTable, 200);

        let snapshot = usage.snapshot();
        let non_zero: Vec<_> = snapshot.non_zero().collect();

        assert_eq!(non_zero.len(), 2);
        assert!(non_zero.contains(&(MemoryTag::BaseTable, 100)));
        assert!(non_zero.contains(&(MemoryTag::HashTable, 200)));
    }

    #[test]
    fn test_memory_usage_reset() {
        let usage = MemoryUsage::new();
        usage.add(MemoryTag::BaseTable, 100);
        usage.add(MemoryTag::HashTable, 200);

        usage.reset();

        assert_eq!(usage.total(), 0);
        assert_eq!(usage.get(MemoryTag::BaseTable), 0);
        assert_eq!(usage.get(MemoryTag::HashTable), 0);
    }

    #[test]
    fn test_memory_usage_format_report() {
        let usage = MemoryUsage::new();
        usage.add(MemoryTag::HashTable, 1024);

        let snapshot = usage.snapshot();
        let report = snapshot.format_report();

        assert!(report.contains("Memory Usage Report"));
        assert!(report.contains("Total: 1024 bytes"));
        assert!(report.contains("HASH_TABLE: 1024 bytes"));
    }

    #[test]
    fn test_memory_usage_thread_safety() {
        use std::thread;

        let usage = Arc::new(MemoryUsage::new());
        let mut handles = vec![];

        // Spawn multiple threads adding memory
        for _ in 0..10 {
            let usage = usage.clone();
            handles.push(thread::spawn(move || {
                for _ in 0..100 {
                    usage.add(MemoryTag::HashTable, 10);
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // 10 threads * 100 iterations * 10 bytes = 10000
        assert_eq!(usage.get(MemoryTag::HashTable), 10000);
        assert_eq!(usage.total(), 10000);
    }

    #[test]
    fn test_buffer_allocator_new() {
        let manager = Arc::new(TestBufferManager::new());
        let allocator = BufferAllocator::new(manager, MemoryTag::HashTable);

        assert_eq!(allocator.tag(), MemoryTag::HashTable);
        assert_eq!(allocator.allocation_count(), 0);
        assert_eq!(allocator.allocated_size(), 0);
    }

    #[test]
    fn test_buffer_allocator_allocate() {
        let manager = Arc::new(TestBufferManager::new());
        let allocator = BufferAllocator::new(manager.clone(), MemoryTag::HashTable);

        let ptr = allocator.allocate(1024).unwrap();
        assert!(!ptr.is_null());
        assert_eq!(allocator.allocation_count(), 1);
        assert_eq!(allocator.allocated_size(), 1024);
        assert_eq!(manager.usage.get(MemoryTag::HashTable), 1024);

        allocator.free(ptr, 1024);
        assert_eq!(allocator.allocation_count(), 0);
        assert_eq!(allocator.allocated_size(), 0);
        assert_eq!(manager.usage.get(MemoryTag::HashTable), 0);
    }

    #[test]
    fn test_buffer_allocator_allocate_zeroed() {
        let manager = Arc::new(TestBufferManager::new());
        let allocator = BufferAllocator::new(manager, MemoryTag::HashTable);

        let ptr = allocator.allocate_zeroed(256).unwrap();
        assert!(!ptr.is_null());

        // Check that memory is zeroed
        // SAFETY: ptr is valid and 256 bytes are allocated
        unsafe {
            let slice = std::slice::from_raw_parts(ptr, 256);
            assert!(slice.iter().all(|&b| b == 0));
        }

        allocator.free(ptr, 256);
    }

    #[test]
    fn test_buffer_allocator_reallocate() {
        let manager = Arc::new(TestBufferManager::new());
        let allocator = BufferAllocator::new(manager.clone(), MemoryTag::HashTable);

        let ptr = allocator.allocate(512).unwrap();
        assert_eq!(allocator.allocated_size(), 512);
        assert_eq!(manager.usage.get(MemoryTag::HashTable), 512);

        // Write some data
        // SAFETY: ptr is valid
        unsafe {
            std::ptr::write_bytes(ptr, 0xAB, 512);
        }

        // Reallocate to larger size
        let new_ptr = allocator.reallocate(ptr, 512, 1024).unwrap();
        assert!(!new_ptr.is_null());
        assert_eq!(allocator.allocation_count(), 1);
        assert_eq!(allocator.allocated_size(), 1024);
        assert_eq!(manager.usage.get(MemoryTag::HashTable), 1024);

        // Check that original data is preserved
        // SAFETY: new_ptr is valid
        unsafe {
            let slice = std::slice::from_raw_parts(new_ptr, 512);
            assert!(slice.iter().all(|&b| b == 0xAB));
        }

        allocator.free(new_ptr, 1024);
    }

    #[test]
    fn test_buffer_allocator_reallocate_null() {
        let manager = Arc::new(TestBufferManager::new());
        let allocator = BufferAllocator::new(manager, MemoryTag::HashTable);

        // Reallocate null pointer should allocate new memory
        let ptr = allocator.reallocate(std::ptr::null_mut(), 0, 256).unwrap();
        assert!(!ptr.is_null());
        assert_eq!(allocator.allocation_count(), 1);

        allocator.free(ptr, 256);
    }

    #[test]
    fn test_buffer_allocator_reallocate_to_zero() {
        let manager = Arc::new(TestBufferManager::new());
        let allocator = BufferAllocator::new(manager, MemoryTag::HashTable);

        let ptr = allocator.allocate(256).unwrap();
        assert_eq!(allocator.allocation_count(), 1);

        // Reallocate to zero should free
        let new_ptr = allocator.reallocate(ptr, 256, 0).unwrap();
        assert!(new_ptr.is_null());
        assert_eq!(allocator.allocation_count(), 0);
    }

    #[test]
    fn test_buffer_allocator_multiple_allocations() {
        let manager = Arc::new(TestBufferManager::new());
        let allocator = BufferAllocator::new(manager.clone(), MemoryTag::HashTable);

        let ptr1 = allocator.allocate(100).unwrap();
        let ptr2 = allocator.allocate(200).unwrap();
        let ptr3 = allocator.allocate(300).unwrap();

        assert_eq!(allocator.allocation_count(), 3);
        assert_eq!(allocator.allocated_size(), 600);
        assert_eq!(manager.usage.get(MemoryTag::HashTable), 600);

        allocator.free(ptr2, 200);
        assert_eq!(allocator.allocation_count(), 2);
        assert_eq!(allocator.allocated_size(), 400);

        allocator.free(ptr1, 100);
        allocator.free(ptr3, 300);
        assert_eq!(allocator.allocation_count(), 0);
        assert_eq!(allocator.allocated_size(), 0);
    }

    #[test]
    fn test_buffer_allocator_drop_cleanup() {
        let manager = Arc::new(TestBufferManager::new());

        {
            let allocator = BufferAllocator::new(manager.clone(), MemoryTag::HashTable);
            let _ptr1 = allocator.allocate(100).unwrap();
            let _ptr2 = allocator.allocate(200).unwrap();

            assert_eq!(manager.usage.get(MemoryTag::HashTable), 300);
            // allocator dropped here
        }

        // Memory should be freed on drop
        assert_eq!(manager.usage.get(MemoryTag::HashTable), 0);
    }

    #[test]
    fn test_buffer_allocator_thread_safety() {
        use std::thread;

        let manager = Arc::new(TestBufferManager::new());
        let allocator = Arc::new(BufferAllocator::new(manager.clone(), MemoryTag::HashTable));

        let mut handles = vec![];

        // Spawn multiple threads allocating and freeing
        for _ in 0..4 {
            let alloc = allocator.clone();
            handles.push(thread::spawn(move || {
                for _ in 0..100 {
                    let ptr = alloc.allocate(64).unwrap();
                    alloc.free(ptr, 64);
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // All allocations should be freed
        assert_eq!(allocator.allocation_count(), 0);
        assert_eq!(manager.usage.get(MemoryTag::HashTable), 0);
    }

    #[test]
    fn test_buffer_allocator_zero_size() {
        let manager = Arc::new(TestBufferManager::new());
        let allocator = BufferAllocator::new(manager, MemoryTag::HashTable);

        // Zero-size allocation should return null
        let ptr = allocator.allocate(0).unwrap();
        assert!(ptr.is_null());
        assert_eq!(allocator.allocation_count(), 0);
    }

    #[test]
    fn test_buffer_allocator_free_null() {
        let manager = Arc::new(TestBufferManager::new());
        let allocator = BufferAllocator::new(manager, MemoryTag::HashTable);

        // Freeing null should be a no-op
        allocator.free(std::ptr::null_mut(), 0);
        assert_eq!(allocator.allocation_count(), 0);
    }
}
