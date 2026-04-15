//! UndoBuffer Implementation
//!
//! - `UndoBuffer` uses `UndoBufferAllocator` for memory management
//! - Each entry has a header (UndoFlags + length) followed by payload
//! - Supports forward iteration (Commit) and reverse iteration (Rollback)
//!
//! The UndoBuffer uses a linked list of blocks for memory management:
//! - Each block has a fixed capacity (default 4KB, grows to block_size)
//! - Entries are allocated sequentially within blocks
//! - When a block is full, a new block is allocated and linked
//! - Supports both high-level API (push_*) and low-level API (create_entry)

use crate::transaction::commit_state::CommitState;
use crate::transaction::rollback_state::RollbackState;
use crate::transaction::txn::Transaction;
use paro_common::allocator::{Allocator, DefaultAllocator};
use std::sync::Arc;

// ============================================================================
// Memory Management Constants
// ============================================================================

/// Size of the undo entry header: UndoFlags (4 bytes) + length (4 bytes).
///
/// ```cpp
/// constexpr uint32_t UNDO_ENTRY_HEADER_SIZE = sizeof(UndoFlags) + sizeof(uint32_t);
/// ```
pub const UNDO_ENTRY_HEADER_SIZE: usize = 8;

/// Default initial block capacity (4KB).
pub const DEFAULT_INITIAL_BLOCK_CAPACITY: usize = 4096;

/// Default block size for subsequent allocations (256KB).
pub const DEFAULT_BLOCK_SIZE: usize = 262144;

/// Align a value to the given alignment (must be power of 2).
///
/// ```cpp
/// template <class T>
/// T AlignValue(T n, T alignment) {
///     return (n + (alignment - 1)) & ~(alignment - 1);
/// }
/// ```
#[inline]
pub fn align_value(n: usize, alignment: usize) -> usize {
    debug_assert!(alignment.is_power_of_two());
    (n + (alignment - 1)) & !(alignment - 1)
}

// ============================================================================
// UndoBufferBlock - Memory Block for Undo Entries
// ============================================================================

/// A single memory block in the undo buffer chain.
///
/// ```cpp
/// struct UndoBufferEntry {
///     BufferManager &buffer_manager;
///     shared_ptr<BlockHandle> block;
///     idx_t position = 0;
///     idx_t capacity = 0;
///     unique_ptr<UndoBufferEntry> next;
///     optional_ptr<UndoBufferEntry> prev;
/// };
/// ```
///
/// In Paro, we use a simpler design with raw memory managed by Allocator.
#[derive(Debug)]
pub struct UndoBufferBlock {
    /// Raw memory pointer allocated by the allocator
    data: *mut u8,
    /// Current write position within the block
    position: usize,
    /// Total capacity of the block
    capacity: usize,
}

impl UndoBufferBlock {
    /// Create a new block with the given allocator and capacity.
    ///
    /// # Errors
    /// Returns an error if memory allocation fails.
    pub fn new(allocator: &dyn Allocator, capacity: usize) -> paro_common::error::Result<Self> {
        let data = allocator.allocate_zeroed(capacity)?;
        Ok(Self {
            data,
            position: 0,
            capacity,
        })
    }

    /// Returns the remaining space in this block.
    #[inline]
    pub fn remaining(&self) -> usize {
        self.capacity.saturating_sub(self.position)
    }

    /// Returns true if this block can accommodate an allocation of the given size.
    #[inline]
    pub fn can_allocate(&self, size: usize) -> bool {
        self.remaining() >= size
    }

    /// Allocate space within this block, returning a pointer to the allocated region.
    ///
    /// # Safety
    /// Caller must ensure `size <= self.remaining()`.
    ///
    /// # Returns
    /// A tuple of (pointer to allocated region, offset within block).
    pub fn allocate(&mut self, size: usize) -> (*mut u8, usize) {
        debug_assert!(
            self.can_allocate(size),
            "Block cannot accommodate allocation"
        );
        let offset = self.position;
        // SAFETY: We verified there's enough space
        let ptr = unsafe { self.data.add(offset) };
        self.position += size;
        (ptr, offset)
    }

    /// Get a pointer to data at the given offset.
    ///
    /// # Safety
    /// Caller must ensure offset is within bounds.
    #[inline]
    pub unsafe fn ptr_at(&self, offset: usize) -> *mut u8 {
        debug_assert!(offset < self.capacity);
        self.data.add(offset)
    }

    /// Get the current position (bytes used).
    #[inline]
    pub fn position(&self) -> usize {
        self.position
    }

    /// Get the capacity of this block.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Get the raw data pointer.
    #[inline]
    pub fn data(&self) -> *mut u8 {
        self.data
    }
}

// SAFETY: UndoBufferBlock is Send because it owns its memory and doesn't share it.
unsafe impl Send for UndoBufferBlock {}

// ============================================================================
// UndoBufferAllocator - Manages Linked Blocks
// ============================================================================

/// Allocator for undo buffer entries using linked blocks.
///
/// ```cpp
/// struct UndoBufferAllocator {
///     mutex lock;
///     BufferManager &buffer_manager;
///     unique_ptr<UndoBufferEntry> head;
///     optional_ptr<UndoBufferEntry> tail;
/// };
/// ```
///
/// The allocator maintains a linked list of blocks:
/// - `head` points to the most recently allocated block (for new allocations)
/// - `tail` points to the oldest block (for forward iteration)
/// - Blocks are linked via indices in a Vec (simpler than raw pointers)
pub struct UndoBufferAllocator {
    /// The underlying allocator for memory management
    allocator: Arc<dyn Allocator>,
    /// List of allocated blocks (index 0 = tail, last = head)
    blocks: Vec<UndoBufferBlock>,
    /// Block size for new allocations
    block_size: usize,
}

impl std::fmt::Debug for UndoBufferAllocator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UndoBufferAllocator")
            .field("allocator", &self.allocator.name())
            .field("blocks", &self.blocks)
            .field("block_size", &self.block_size)
            .finish()
    }
}

impl UndoBufferAllocator {
    /// Create a new undo buffer allocator with the default allocator.
    pub fn new() -> Self {
        Self::with_allocator(Arc::new(DefaultAllocator::new()))
    }

    /// Create a new undo buffer allocator with a custom allocator.
    pub fn with_allocator(allocator: Arc<dyn Allocator>) -> Self {
        Self {
            allocator,
            blocks: Vec::new(),
            block_size: DEFAULT_BLOCK_SIZE,
        }
    }

    /// Set the block size for new allocations.
    pub fn set_block_size(&mut self, size: usize) {
        self.block_size = size;
    }

    /// Allocate space for an entry of the given size.
    ///
    /// ```cpp
    /// UndoBufferReference UndoBufferAllocator::Allocate(idx_t alloc_len) {
    ///     if (!head || head->position + alloc_len > head->capacity) {
    ///         // allocate new block
    ///     }
    ///     // allocate from head
    /// }
    /// ```
    ///
    /// # Returns
    /// An `UndoBufferReference` pointing to the allocated space.
    ///
    /// # Errors
    /// Returns an error if memory allocation fails.
    pub fn allocate(
        &mut self,
        alloc_len: usize,
    ) -> paro_common::error::Result<UndoBufferReference> {
        // Check if we need a new block
        let need_new_block = self.blocks.is_empty()
            || !self
                .blocks
                .last()
                .map_or(false, |b| b.can_allocate(alloc_len));

        if need_new_block {
            // Determine capacity for new block
            let capacity = if self.blocks.is_empty() && alloc_len <= DEFAULT_INITIAL_BLOCK_CAPACITY
            {
                DEFAULT_INITIAL_BLOCK_CAPACITY
            } else {
                self.block_size
            };

            // Ensure capacity is sufficient
            let capacity = if capacity < alloc_len {
                alloc_len.next_power_of_two()
            } else {
                capacity
            };

            let block = UndoBufferBlock::new(self.allocator.as_ref(), capacity)?;
            self.blocks.push(block);
        }

        // Allocate from the head (last) block
        let block_idx = self.blocks.len() - 1;
        let block = &mut self.blocks[block_idx];
        let (ptr, offset) = block.allocate(alloc_len);

        Ok(UndoBufferReference {
            block_idx,
            offset,
            ptr,
            len: alloc_len,
        })
    }

    /// Returns true if any allocations have been made.
    #[inline]
    pub fn has_allocations(&self) -> bool {
        !self.blocks.is_empty()
    }

    /// Get the total estimated size of all allocations.
    pub fn estimated_size(&self) -> usize {
        self.blocks.iter().map(|b| b.position()).sum()
    }

    /// Get the number of blocks allocated.
    #[inline]
    pub fn block_count(&self) -> usize {
        self.blocks.len()
    }

    /// Get a reference to a block by index.
    #[inline]
    pub fn block(&self, idx: usize) -> Option<&UndoBufferBlock> {
        self.blocks.get(idx)
    }

    /// Free all allocated blocks.
    pub fn clear(&mut self) {
        for block in self.blocks.drain(..) {
            self.allocator.free(block.data, block.capacity);
        }
    }

    /// Get the tail block index (oldest block, for forward iteration).
    /// Returns None if no blocks exist.
    #[inline]
    pub fn tail_idx(&self) -> Option<usize> {
        if self.blocks.is_empty() {
            None
        } else {
            Some(0)
        }
    }

    /// Get the head block index (newest block, for reverse iteration).
    /// Returns None if no blocks exist.
    #[inline]
    pub fn head_idx(&self) -> Option<usize> {
        if self.blocks.is_empty() {
            None
        } else {
            Some(self.blocks.len() - 1)
        }
    }

    /// Get the next block index in forward direction (tail -> head).
    /// Returns None if at the end.
    #[inline]
    pub fn next_block(&self, current: usize) -> Option<usize> {
        let next = current + 1;
        if next < self.blocks.len() {
            Some(next)
        } else {
            None
        }
    }

    /// Get the previous block index in reverse direction (head -> tail).
    /// Returns None if at the beginning.
    #[inline]
    pub fn prev_block(&self, current: usize) -> Option<usize> {
        if current > 0 {
            Some(current - 1)
        } else {
            None
        }
    }
}

impl Default for UndoBufferAllocator {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for UndoBufferAllocator {
    fn drop(&mut self) {
        self.clear();
    }
}

// ============================================================================
// UndoBufferReference - Handle to Allocated Entry
// ============================================================================

/// A reference to an allocated entry in the undo buffer.
///
/// ```cpp
/// struct UndoBufferReference {
///     optional_ptr<UndoBufferEntry> entry;
///     BufferHandle handle;
///     idx_t position;
///     data_ptr_t Ptr() { return handle.Ptr() + position; }
/// };
/// ```
///
/// This provides a handle to write data into the allocated space.
#[derive(Debug)]
pub struct UndoBufferReference {
    /// Index of the block containing this entry
    block_idx: usize,
    /// Offset within the block
    offset: usize,
    /// Direct pointer to the allocated memory
    ptr: *mut u8,
    /// Length of the allocation
    len: usize,
}

impl UndoBufferReference {
    /// Get a raw pointer to the allocated memory.
    #[inline]
    pub fn ptr(&self) -> *mut u8 {
        self.ptr
    }

    /// Get the block index.
    #[inline]
    pub fn block_idx(&self) -> usize {
        self.block_idx
    }

    /// Get the offset within the block.
    #[inline]
    pub fn offset(&self) -> usize {
        self.offset
    }

    /// Get the length of the allocation.
    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns true if this is a zero-length allocation.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Write bytes to the allocated memory.
    ///
    /// # Safety
    /// Caller must ensure `data.len() <= self.len`.
    pub unsafe fn write(&self, data: &[u8]) {
        debug_assert!(data.len() <= self.len);
        std::ptr::copy_nonoverlapping(data.as_ptr(), self.ptr, data.len());
    }

    /// Get a mutable slice to the allocated memory.
    ///
    /// # Safety
    /// Caller must ensure the memory is valid and not aliased.
    pub unsafe fn as_mut_slice(&mut self) -> &mut [u8] {
        std::slice::from_raw_parts_mut(self.ptr, self.len)
    }
}

// SAFETY: UndoBufferReference is Send because it's just a pointer to owned memory.
unsafe impl Send for UndoBufferReference {}

// ============================================================================
// UndoBufferPointer - Lightweight Handle for Iteration
// ============================================================================

/// A lightweight pointer to an entry in the undo buffer (for iteration).
///
/// ```cpp
/// struct UndoBufferPointer {
///     UndoBufferEntry *entry;
///     idx_t position;
/// };
/// ```
#[derive(Debug, Clone, Copy)]
pub struct UndoBufferPointer {
    /// Index of the block containing this entry
    pub block_idx: usize,
    /// Offset within the block
    pub offset: usize,
}

impl UndoBufferPointer {
    /// Create a new pointer.
    pub fn new(block_idx: usize, offset: usize) -> Self {
        Self { block_idx, offset }
    }
}

// ============================================================================
// ============================================================================

/// State for iterating over undo buffer entries.
///
/// ```cpp
/// struct IteratorState {
///     BufferHandle handle;
///     optional_ptr<UndoBufferEntry> current;
///     data_ptr_t start;
///     data_ptr_t end;
///     bool started = false;
/// };
/// ```
///
/// In Paro, we use block indices instead of raw pointers for safety.
#[derive(Debug, Clone, Default)]
pub struct IteratorState {
    /// Current block index being iterated
    pub current_block: usize,
    /// Current position within the block
    pub position: usize,
    /// End position within the current block
    pub end_position: usize,
    /// Whether iteration has started
    pub started: bool,
}

impl IteratorState {
    /// Create a new iterator state.
    pub fn new() -> Self {
        Self::default()
    }

    /// Check if the iterator has started.
    #[inline]
    pub fn is_started(&self) -> bool {
        self.started
    }
}
///
/// ```cpp
/// enum class UndoFlags : uint32_t {
///     EMPTY_ENTRY = 0,
///     CATALOG_ENTRY = 1,
///     INSERT_TUPLE = 2,
///     DELETE_TUPLE = 3,
///     UPDATE_TUPLE = 4,
///     SEQUENCE_VALUE = 5,
///     DATABASE_ATTACH = 6
/// };
/// ```
/// State tracking for active transactions.
///
/// ```cpp
/// enum class ActiveTransactionState { UNSET, OTHER_TRANSACTIONS, NO_OTHER_TRANSACTIONS };
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ActiveTransactionState {
    #[default]
    Unset,
    OtherTransactions,
    NoOtherTransactions,
}

/// Mode for committing or reverting a partially committed transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitMode {
    Commit,
    RevertCommit,
}

impl TryFrom<u32> for ActiveTransactionState {
    type Error = &'static str;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Unset),
            1 => Ok(Self::OtherTransactions),
            2 => Ok(Self::NoOtherTransactions),
            _ => Err("Invalid ActiveTransactionState"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum UndoFlags {
    /// Empty/invalid entry (used as sentinel)
    EmptyEntry = 0,
    /// Tuple insertion (INSERT)
    InsertTuple = 1,
    /// Tuple deletion (DELETE)
    DeleteTuple = 2,
    /// Tuple update (UPDATE)
    UpdateTuple = 3,
    /// Sequence value change
    SequenceValue = 4,
    /// Attached database modification
    DatabaseAttach = 5,
}

impl UndoFlags {
    /// Returns the string representation of this flag.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::EmptyEntry => "EMPTY_ENTRY",
            Self::InsertTuple => "INSERT_TUPLE",
            Self::DeleteTuple => "DELETE_TUPLE",
            Self::UpdateTuple => "UPDATE_TUPLE",
            Self::SequenceValue => "SEQUENCE_VALUE",
            Self::DatabaseAttach => "DATABASE_ATTACH",
        }
    }

    /// Returns true if this is a data modification entry (INSERT/DELETE/UPDATE).
    pub fn is_data_modification(&self) -> bool {
        matches!(
            self,
            Self::InsertTuple | Self::DeleteTuple | Self::UpdateTuple
        )
    }

    /// Returns true if this is a catalog modification entry.
    pub fn is_catalog_modification(&self) -> bool {
        matches!(self, Self::DatabaseAttach)
    }
}

impl std::fmt::Display for UndoFlags {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl TryFrom<u32> for UndoFlags {
    type Error = &'static str;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::EmptyEntry),
            1 => Ok(Self::InsertTuple),
            2 => Ok(Self::DeleteTuple),
            3 => Ok(Self::UpdateTuple),
            4 => Ok(Self::SequenceValue),
            5 => Ok(Self::DatabaseAttach),
            _ => Err("invalid UndoFlags value"),
        }
    }
}

/// Payload data for tuple append (insert) undo operations.
///
/// ```cpp
/// struct AppendInfo {
///     table_t table_id;
///     idx_t start_row;
///     idx_t count;
/// };
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UndoAppendInfo {
    /// Table OID being modified
    pub table_id: u64,
    /// Starting row ID of the inserted rows
    pub start_row: u64,
    /// Number of rows inserted
    pub count: u64,
}

/// Payload data for tuple delete undo operations.
///
/// ```cpp
/// struct DeleteInfo {
///     RowVersionManager *version_info;
///     idx_t vector_idx;
///     table_t table_id;
///     idx_t count;
///     idx_t base_row;
///     bool is_consecutive;
///     // followed by row identifiers if not consecutive
/// };
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UndoDeleteInfo {
    /// Table OID being modified
    pub table_id: u64,
    /// Base row ID for the delete operation
    pub base_row: u64,
    /// Number of rows deleted
    pub count: u64,
    /// Whether the deleted rows are consecutive
    pub is_consecutive: bool,
    /// Row IDs being deleted (only populated if not consecutive)
    pub row_ids: Vec<u64>,
}

/// Payload data for tuple update undo operations.
///
/// ```cpp
/// struct UpdateInfo {
///     table_t table_id;
///     transaction_t transaction_id;
///     // ... row identifiers
///     // ... column data and old values
/// };
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UndoUpdateInfo {
    /// Table OID being modified
    pub table_id: u64,
    /// Transaction ID that made the update
    pub transaction_id: u64,
    /// Row IDs being updated
    pub row_ids: Vec<u64>,
}

/// Payload data for sequence value undo operations.
///
/// ```cpp
/// struct SequenceValue {
///     SequenceCatalogEntry *entry;
///     idx_t usage_count;
///     int64_t counter;
/// };
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UndoSequenceValueInfo {
    /// Sequence OID
    pub sequence_id: u64,
    /// Usage count before modification
    pub usage_count: u64,
    /// Counter value before modification
    pub counter: i64,
}

/// A single undo entry with type flag and payload.
///
/// - `UndoFlags type` (4 bytes)
/// - `uint32_t length` (4 bytes)
/// - followed by payload data
///
/// Paro uses a Rust enum for type safety instead of raw bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UndoEntry {
    /// The type of this undo entry
    pub flags: UndoFlags,
    /// The payload data (type-specific)
    pub payload: UndoPayload,
}

/// Payload variants for different undo entry types.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UndoPayload {
    /// Empty entry (no payload)
    Empty,
    /// Tuple insertion
    Insert(UndoAppendInfo),
    /// Tuple deletion
    Delete(UndoDeleteInfo),
    /// Tuple update
    Update(UndoUpdateInfo),
    /// Sequence value change
    Sequence(UndoSequenceValueInfo),
    /// Attached database modification (schema + database name)
    DatabaseAttach { schema: String, database: String },
}

impl UndoEntry {
    /// Create a new insert (append) undo record.
    pub fn insert(table_id: u64, start_row: u64, count: u64) -> Self {
        Self {
            flags: UndoFlags::InsertTuple,
            payload: UndoPayload::Insert(UndoAppendInfo {
                table_id,
                start_row,
                count,
            }),
        }
    }

    /// Create a new delete undo record with consecutive rows.
    pub fn delete_consecutive(table_id: u64, base_row: u64, count: u64) -> Self {
        Self {
            flags: UndoFlags::DeleteTuple,
            payload: UndoPayload::Delete(UndoDeleteInfo {
                table_id,
                base_row,
                count,
                is_consecutive: true,
                row_ids: Vec::new(),
            }),
        }
    }

    /// Create a new delete undo record with non-consecutive rows.
    pub fn delete_rows(table_id: u64, row_ids: Vec<u64>) -> Self {
        let count = row_ids.len() as u64;
        let base_row = row_ids.first().copied().unwrap_or(0);
        Self {
            flags: UndoFlags::DeleteTuple,
            payload: UndoPayload::Delete(UndoDeleteInfo {
                table_id,
                base_row,
                count,
                is_consecutive: false,
                row_ids,
            }),
        }
    }

    /// Create a new update undo record.
    pub fn update(table_id: u64, transaction_id: u64, row_ids: Vec<u64>) -> Self {
        Self {
            flags: UndoFlags::UpdateTuple,
            payload: UndoPayload::Update(UndoUpdateInfo {
                table_id,
                transaction_id,
                row_ids,
            }),
        }
    }

    /// Create a new sequence value undo record.
    pub fn sequence_value(sequence_id: u64, usage_count: u64, counter: i64) -> Self {
        Self {
            flags: UndoFlags::SequenceValue,
            payload: UndoPayload::Sequence(UndoSequenceValueInfo {
                sequence_id,
                usage_count,
                counter,
            }),
        }
    }

    /// Returns the flags for this entry.
    pub fn flags(&self) -> UndoFlags {
        self.flags
    }

    /// Returns true if this is a data modification entry.
    pub fn is_data_modification(&self) -> bool {
        self.flags.is_data_modification()
    }

    /// Returns true if this is a catalog modification entry.
    pub fn is_catalog_modification(&self) -> bool {
        self.flags.is_catalog_modification()
    }
}

/// Buffer for storing undo information.
///
/// ```cpp
/// class UndoBuffer {
///     UndoBufferAllocator allocator;
///     ActiveTransactionState active_transaction_state;
/// };
/// ```
///
/// The UndoBuffer now uses `UndoBufferAllocator` for memory management:
/// - High-level API: `push_*` methods for type-safe entry creation
/// - Low-level API: `create_entry()` for raw memory allocation
///
/// Both APIs can be used together. The high-level API internally uses
/// the Vec-based storage for simplicity, while `create_entry()` uses
/// the allocator for raw memory management.
#[derive(Debug)]
pub struct UndoBuffer {
    /// List of undo entries in insertion order (high-level API)
    entries: Vec<UndoEntry>,
    /// Memory allocator for raw entries (low-level API)
    allocator: UndoBufferAllocator,
    /// Number of raw entries created via create_entry()
    raw_entry_count: usize,
}

impl Default for UndoBuffer {
    fn default() -> Self {
        Self::new()
    }
}

impl UndoBuffer {
    /// Create a new empty undo buffer.
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
            allocator: UndoBufferAllocator::new(),
            raw_entry_count: 0,
        }
    }

    /// Create a new undo buffer with a custom allocator.
    pub fn with_allocator(allocator: Arc<dyn Allocator>) -> Self {
        Self {
            entries: Vec::new(),
            allocator: UndoBufferAllocator::with_allocator(allocator),
            raw_entry_count: 0,
        }
    }

    /// Create a raw entry in the undo buffer, returning a writable reference.
    ///
    /// ```cpp
    /// UndoBufferReference UndoBuffer::CreateEntry(UndoFlags type, idx_t len) {
    ///     idx_t alloc_len = AlignValue<idx_t>(len + UNDO_ENTRY_HEADER_SIZE);
    ///     auto handle = allocator.Allocate(alloc_len);
    ///     // write header
    ///     Store<UndoFlags>(type, data);
    ///     Store<uint32_t>(len, data + sizeof(UndoFlags));
    ///     handle.position += UNDO_ENTRY_HEADER_SIZE;
    ///     return handle;
    /// }
    /// ```
    ///
    /// # Arguments
    /// * `entry_type` - The type of undo entry
    /// * `payload_len` - Length of the payload data (excluding header)
    ///
    /// # Returns
    /// An `UndoBufferReference` pointing to the payload area (after header).
    /// The header is already written.
    ///
    /// # Errors
    /// Returns an error if memory allocation fails.
    pub fn create_entry(
        &mut self,
        entry_type: UndoFlags,
        payload_len: usize,
    ) -> paro_common::error::Result<UndoBufferReference> {
        // Align allocation to 8 bytes
        let alloc_len = align_value(payload_len + UNDO_ENTRY_HEADER_SIZE, 8);
        let reference = self.allocator.allocate(alloc_len)?;

        // Write header: UndoFlags (4 bytes) + length (4 bytes)
        // SAFETY: We just allocated this memory
        unsafe {
            let ptr = reference.ptr();
            // Write UndoFlags as u32
            std::ptr::write(ptr as *mut u32, entry_type as u32);
            // Write payload length as u32
            std::ptr::write(ptr.add(4) as *mut u32, payload_len as u32);
        }

        // Adjust reference to point past the header
        let payload_ptr = unsafe { reference.ptr().add(UNDO_ENTRY_HEADER_SIZE) };
        self.raw_entry_count += 1;

        Ok(UndoBufferReference {
            block_idx: reference.block_idx(),
            offset: reference.offset() + UNDO_ENTRY_HEADER_SIZE,
            ptr: payload_ptr,
            len: payload_len,
        })
    }

    /// Add an undo entry to the buffer.
    ///
    /// ```cpp
    /// UndoBufferReference UndoBuffer::CreateEntry(UndoFlags type, idx_t len) {
    ///     idx_t alloc_len = AlignValue<idx_t>(len + UNDO_ENTRY_HEADER_SIZE);
    ///     auto handle = allocator.Allocate(alloc_len);
    ///     // write header and return reference
    /// }
    /// ```
    pub fn push(&mut self, entry: UndoEntry) {
        self.entries.push(entry);
    }

    /// Push an insert (append) operation.
    pub fn push_insert(&mut self, table_id: u64, start_row: u64, count: u64) {
        self.push(UndoEntry::insert(table_id, start_row, count));
    }

    /// Push a delete operation with consecutive rows.
    pub fn push_delete_consecutive(&mut self, table_id: u64, base_row: u64, count: u64) {
        self.push(UndoEntry::delete_consecutive(table_id, base_row, count));
    }

    /// Push a delete operation with specific row IDs.
    pub fn push_delete_rows(&mut self, table_id: u64, row_ids: Vec<u64>) {
        self.push(UndoEntry::delete_rows(table_id, row_ids));
    }

    /// Push an update operation.
    pub fn push_update(&mut self, table_id: u64, transaction_id: u64, row_ids: Vec<u64>) {
        self.push(UndoEntry::update(table_id, transaction_id, row_ids));
    }

    /// Push a sequence value change.
    pub fn push_sequence_value(&mut self, sequence_id: u64, usage_count: u64, counter: i64) {
        self.push(UndoEntry::sequence_value(sequence_id, usage_count, counter));
    }

    /// Returns true if any changes have been made (entries exist in the buffer).
    ///
    /// ```cpp
    /// bool UndoBuffer::ChangesMade() {
    ///     return allocator.head.get();
    /// }
    /// ```
    pub fn changes_made(&self) -> bool {
        !self.entries.is_empty() || self.allocator.has_allocations()
    }

    /// Returns the number of high-level entries in the buffer.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns the total number of entries (high-level + raw).
    pub fn total_entry_count(&self) -> usize {
        self.entries.len() + self.raw_entry_count
    }

    /// Returns the number of raw entries created via create_entry().
    pub fn raw_entry_count(&self) -> usize {
        self.raw_entry_count
    }

    /// Returns true if the buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty() && !self.allocator.has_allocations()
    }

    /// Returns the estimated memory size of the buffer.
    pub fn estimated_size(&self) -> usize {
        let entries_size = self.entries.len() * std::mem::size_of::<UndoEntry>();
        let allocator_size = self.allocator.estimated_size();
        entries_size + allocator_size
    }

    /// Returns the number of memory blocks allocated.
    pub fn block_count(&self) -> usize {
        self.allocator.block_count()
    }

    /// Get a reference to the underlying allocator.
    pub fn allocator(&self) -> &UndoBufferAllocator {
        &self.allocator
    }

    /// Returns an iterator over entries in insertion order (for commit).
    pub fn iter(&self) -> impl Iterator<Item = &UndoEntry> {
        self.entries.iter()
    }

    /// Returns an iterator over entries in reverse order (for rollback).
    pub fn iter_reverse(&self) -> impl Iterator<Item = &UndoEntry> {
        self.entries.iter().rev()
    }

    /// Commit the entries in the buffer.
    ///
    /// ```cpp
    /// void UndoBuffer::Commit(UndoBuffer::IteratorState &iterator_state, CommitInfo &info) {
    ///     IterateEntries(iterator_state, [&](UndoFlags type, data_ptr_t data) {
    ///         CommitState::CommitEntry(type, data, commit_info);
    ///     });
    /// }
    /// ```
    ///
    /// # Arguments
    /// * `transaction` - The transaction owning this buffer
    /// * `commit_id` - The transaction commit ID to mark entries with
    ///
    /// # Returns
    /// An `IteratorState` that can be used for partial commit/revert operations.
    pub fn commit(&self, transaction: &Transaction, commit_id: u64) -> IteratorState {
        let mut state = CommitState::new(
            transaction,
            commit_id,
            transaction.active_transaction_state(),
            CommitMode::Commit,
        );

        // Commit high-level entries in forward order
        for entry in self.iter() {
            state.commit_high_level_entry(entry);
        }

        // Commit raw entries via forward iteration
        self.iterate_entries(|flags, ptr| {
            state.commit_entry(flags, ptr);
        })
    }

    /// Rollback the entries in the buffer.
    ///
    /// ```cpp
    /// void UndoBuffer::Rollback() {
    ///     // rollback needs to be performed in reverse
    ///     RollbackState state(transaction);
    ///     ReverseIterateEntries([&](UndoFlags type, data_ptr_t data) {
    ///         state.RollbackEntry(type, data);
    ///     });
    /// }
    /// ```
    ///
    /// Rollback must be performed in reverse order (LIFO) to properly
    /// undo nested operations.
    pub fn rollback(&mut self, transaction: &Transaction) {
        let mut state = RollbackState::new(transaction);

        // Rollback raw entries via reverse iteration first
        // (they may have been created after high-level entries)
        self.reverse_iterate_entries(|flags, ptr| {
            state.rollback_entry(flags, ptr);
        });

        // Rollback high-level entries in reverse order (LIFO)
        for entry in self.entries.iter().rev() {
            state.rollback_high_level_entry(entry);
        }

        // Clear all entries after rollback
        self.entries.clear();
        self.allocator.clear();
        self.raw_entry_count = 0;
    }

    /// Cleanup committed entries that are no longer needed.
    ///
    /// ```cpp
    /// void UndoBuffer::Cleanup(transaction_t lowest_active_transaction) {
    ///     CleanupState state(transaction, lowest_active_transaction, active_transaction_state);
    ///     IterateEntries(iterator_state, [&](UndoFlags type, data_ptr_t data) {
    ///         state.CleanupEntry(type, data);
    ///     });
    /// }
    /// ```
    ///
    /// Cleanup should only be called after:
    /// 1. The transaction has successfully committed
    /// 2. There is no active transaction with start_id < commit_id
    ///
    /// # Arguments
    /// * `lowest_active_transaction` - The lowest active transaction ID
    /// * `transaction_state` - The state of other active transactions
    pub fn cleanup(
        &mut self,
        lowest_active_transaction: u64,
        transaction_state: ActiveTransactionState,
    ) {
        use crate::transaction::cleanup_state::CleanupState;

        let mut state = CleanupState::new(lowest_active_transaction, transaction_state);

        // Cleanup high-level entries in forward order
        for entry in &self.entries {
            state.cleanup_high_level_entry(entry);
        }

        // Cleanup raw entries via forward iteration
        self.iterate_entries(|flags, ptr| {
            state.cleanup_entry(flags, ptr);
        });

        // Flush any pending cleanup operations
        state.flush();

        // Clear entries after cleanup
        self.entries.clear();
        self.allocator.clear();
        self.raw_entry_count = 0;
    }

    /// Revert a partial commit up to a given state.
    ///
    /// ```cpp
    /// void UndoBuffer::RevertCommit(UndoBuffer::IteratorState &end_state, transaction_t transaction_id) {
    ///     CommitState state(transaction, transaction_id, active_transaction_state, CommitMode::REVERT_COMMIT);
    ///     IterateEntries(start_state, end_state, [&](UndoFlags type, data_ptr_t data) {
    ///         state.RevertCommit(type, data);
    ///     });
    /// }
    /// ```
    ///
    /// Used when a commit fails partway through and needs to be reverted.
    ///
    /// # Arguments
    /// * `end_state` - The iterator state marking where to stop reverting
    /// * `transaction_id` - The transaction ID to revert to
    pub fn revert_commit(
        &self,
        transaction: &Transaction,
        end_state: &IteratorState,
        transaction_id: u64,
    ) {
        let mut state = CommitState::new(
            transaction,
            transaction_id,
            transaction.active_transaction_state(),
            CommitMode::RevertCommit,
        );

        let start_state = IteratorState::new();
        self.iterate_entries_range(&start_state, end_state, |flags, ptr| {
            state.commit_entry(flags, ptr);
        });
    }

    /// Get properties of the undo buffer (for checkpoint decisions).
    ///
    /// ```cpp
    /// UndoBufferProperties UndoBuffer::GetProperties() {
    ///     // iterates entries to determine has_updates, has_deletes, etc.
    /// }
    /// ```
    pub fn get_properties(&self) -> UndoBufferProperties {
        let mut props = UndoBufferProperties::default();

        // Account for high-level entries
        for entry in &self.entries {
            props.estimated_size += std::mem::size_of::<UndoEntry>();

            match &entry.payload {
                UndoPayload::Update(_) => props.has_updates = true,
                UndoPayload::Delete(info) => {
                    props.has_deletes = true;
                    if !info.is_consecutive {
                        props.estimated_size += info.row_ids.len() * std::mem::size_of::<u64>();
                    }
                }
                UndoPayload::DatabaseAttach { .. } => {
                    props.has_catalog_changes = true;
                }
                _ => {}
            }
        }

        // Account for raw allocator memory
        props.estimated_size += self.allocator.estimated_size();

        props
    }

    // ========================================================================
    // ========================================================================

    /// Iterate over raw entries in insertion order (tail to head).
    /// Used for Commit operations.
    ///
    /// ```cpp
    /// template <class T>
    /// void UndoBuffer::IterateEntries(UndoBuffer::IteratorState &state, T &&callback) {
    ///     // iterate in insertion order: start with the tail
    ///     state.current = allocator.tail.get();
    ///     state.started = true;
    ///     while (state.current) {
    ///         state.handle = allocator.buffer_manager.Pin(state.current->block);
    ///         state.start = state.handle.Ptr();
    ///         state.end = state.start + state.current->position;
    ///         while (state.start < state.end) {
    ///             UndoFlags type = Load<UndoFlags>(state.start);
    ///             state.start += sizeof(UndoFlags);
    ///             uint32_t len = Load<uint32_t>(state.start);
    ///             state.start += sizeof(uint32_t);
    ///             callback(type, state.start);
    ///             state.start += len;
    ///         }
    ///         state.current = state.current->prev;
    ///     }
    /// }
    /// ```
    ///
    /// # Arguments
    /// * `callback` - Function called for each entry with (UndoFlags, *const u8 payload)
    ///
    /// # Returns
    /// The final iterator state (can be used for partial iteration).
    pub fn iterate_entries<F>(&self, mut callback: F) -> IteratorState
    where
        F: FnMut(UndoFlags, *const u8),
    {
        let mut state = IteratorState::new();
        self.iterate_entries_with_state(&mut state, &mut callback);
        state
    }

    /// Iterate over raw entries with an existing state.
    /// Allows resuming iteration from a previous position.
    ///
    /// # Arguments
    /// * `state` - Mutable iterator state to track position
    /// * `callback` - Function called for each entry
    pub fn iterate_entries_with_state<F>(&self, state: &mut IteratorState, callback: &mut F)
    where
        F: FnMut(UndoFlags, *const u8),
    {
        // Start from tail (oldest block) for forward iteration
        let Some(mut block_idx) = self.allocator.tail_idx() else {
            return;
        };

        state.started = true;

        loop {
            let Some(block) = self.allocator.block(block_idx) else {
                break;
            };

            state.current_block = block_idx;
            state.position = 0;
            state.end_position = block.position();

            // Iterate entries within this block
            while state.position < state.end_position {
                // Read header: UndoFlags (4 bytes) + length (4 bytes)
                // SAFETY: We verified position is within bounds
                let (flags, payload_len, payload_ptr) = unsafe {
                    let ptr = block.ptr_at(state.position);
                    let flags_raw = std::ptr::read(ptr as *const u32);
                    let len = std::ptr::read(ptr.add(4) as *const u32);
                    let payload = ptr.add(UNDO_ENTRY_HEADER_SIZE);
                    (flags_raw, len as usize, payload as *const u8)
                };

                // Convert flags
                let undo_flags = UndoFlags::try_from(flags).unwrap_or(UndoFlags::EmptyEntry);

                // Call the callback with payload pointer
                callback(undo_flags, payload_ptr);

                // Move to next entry (aligned)
                let entry_size = align_value(UNDO_ENTRY_HEADER_SIZE + payload_len, 8);
                state.position += entry_size;
            }

            // Move to next block (toward head)
            match self.allocator.next_block(block_idx) {
                Some(next) => block_idx = next,
                None => break,
            }
        }
    }

    /// Iterate over raw entries in reverse insertion order (head to tail, entries reversed).
    /// Used for Rollback operations.
    ///
    /// ```cpp
    /// template <class T>
    /// void UndoBuffer::ReverseIterateEntries(T &&callback) {
    ///     // iterate in reverse insertion order: start with the head
    ///     auto current = allocator.head.get();
    ///     while (current) {
    ///         auto handle = allocator.buffer_manager.Pin(current->block);
    ///         data_ptr_t start = handle.Ptr();
    ///         data_ptr_t end = start + current->position;
    ///         // create a vector with all nodes in this chunk
    ///         vector<pair<UndoFlags, data_ptr_t>> nodes;
    ///         while (start < end) {
    ///             auto type = Load<UndoFlags>(start);
    ///             start += sizeof(UndoFlags);
    ///             auto len = Load<uint32_t>(start);
    ///             start += sizeof(uint32_t);
    ///             nodes.emplace_back(type, start);
    ///             start += len;
    ///         }
    ///         // iterate over it in reverse order
    ///         for (idx_t i = nodes.size(); i > 0; i--) {
    ///             callback(nodes[i - 1].first, nodes[i - 1].second);
    ///         }
    ///         current = current->next.get();
    ///     }
    /// }
    /// ```
    ///
    /// # Arguments
    /// * `callback` - Function called for each entry with (UndoFlags, *const u8 payload)
    pub fn reverse_iterate_entries<F>(&self, mut callback: F)
    where
        F: FnMut(UndoFlags, *const u8),
    {
        // Start from head (newest block) for reverse iteration
        let Some(mut block_idx) = self.allocator.head_idx() else {
            return;
        };

        loop {
            let Some(block) = self.allocator.block(block_idx) else {
                break;
            };

            // Collect all entries in this block
            let mut entries: Vec<(UndoFlags, *const u8)> = Vec::new();
            let mut position = 0usize;
            let end_position = block.position();

            while position < end_position {
                // Read header
                // SAFETY: We verified position is within bounds
                let (flags, payload_len, payload_ptr) = unsafe {
                    let ptr = block.ptr_at(position);
                    let flags_raw = std::ptr::read(ptr as *const u32);
                    let len = std::ptr::read(ptr.add(4) as *const u32);
                    let payload = ptr.add(UNDO_ENTRY_HEADER_SIZE);
                    (flags_raw, len as usize, payload as *const u8)
                };

                let undo_flags = UndoFlags::try_from(flags).unwrap_or(UndoFlags::EmptyEntry);
                entries.push((undo_flags, payload_ptr));

                // Move to next entry
                let entry_size = align_value(UNDO_ENTRY_HEADER_SIZE + payload_len, 8);
                position += entry_size;
            }

            // Iterate entries in reverse order within this block
            for (flags, payload_ptr) in entries.into_iter().rev() {
                callback(flags, payload_ptr);
            }

            // Move to previous block (toward tail)
            match self.allocator.prev_block(block_idx) {
                Some(prev) => block_idx = prev,
                None => break,
            }
        }
    }

    /// Iterate over raw entries between two states (for partial commit/revert).
    ///
    /// ```cpp
    /// template <class T>
    /// void UndoBuffer::IterateEntries(IteratorState &state, IteratorState &end_state, T &&callback);
    /// ```
    ///
    /// # Arguments
    /// * `start_state` - Starting iterator state
    /// * `end_state` - Ending iterator state (exclusive)
    /// * `callback` - Function called for each entry
    pub fn iterate_entries_range<F>(
        &self,
        start_state: &IteratorState,
        end_state: &IteratorState,
        mut callback: F,
    ) where
        F: FnMut(UndoFlags, *const u8),
    {
        if !end_state.started {
            return;
        }

        let Some(mut block_idx) = self.allocator.tail_idx() else {
            return;
        };

        // Skip to start block if specified
        if start_state.started {
            block_idx = start_state.current_block;
        }

        loop {
            let Some(block) = self.allocator.block(block_idx) else {
                break;
            };

            let start_pos = if block_idx == start_state.current_block && start_state.started {
                start_state.position
            } else {
                0
            };

            let end_pos = if block_idx == end_state.current_block {
                end_state.position
            } else {
                block.position()
            };

            let mut position = start_pos;

            while position < end_pos {
                // Read header
                // SAFETY: We verified position is within bounds
                let (flags, payload_len, payload_ptr) = unsafe {
                    let ptr = block.ptr_at(position);
                    let flags_raw = std::ptr::read(ptr as *const u32);
                    let len = std::ptr::read(ptr.add(4) as *const u32);
                    let payload = ptr.add(UNDO_ENTRY_HEADER_SIZE);
                    (flags_raw, len as usize, payload as *const u8)
                };

                let undo_flags = UndoFlags::try_from(flags).unwrap_or(UndoFlags::EmptyEntry);
                callback(undo_flags, payload_ptr);

                let entry_size = align_value(UNDO_ENTRY_HEADER_SIZE + payload_len, 8);
                position += entry_size;
            }

            // Check if we've reached the end state
            if block_idx == end_state.current_block {
                break;
            }

            match self.allocator.next_block(block_idx) {
                Some(next) => block_idx = next,
                None => break,
            }
        }
    }
}

/// Properties of an undo buffer for checkpoint decisions.
///
/// ```cpp
/// struct UndoBufferProperties {
///     idx_t estimated_size = 0;
///     bool has_updates = false;
///     bool has_deletes = false;
///     bool has_index_deletes = false;
///     bool has_catalog_changes = false;
///     bool has_dropped_entries = false;
/// };
/// ```
#[derive(Debug, Clone, Default)]
pub struct UndoBufferProperties {
    /// Estimated memory size of the buffer
    pub estimated_size: usize,
    /// Whether the buffer contains update entries
    pub has_updates: bool,
    /// Whether the buffer contains delete entries
    pub has_deletes: bool,
    /// Whether the buffer contains index delete entries
    pub has_index_deletes: bool,
    /// Whether the buffer contains catalog changes
    pub has_catalog_changes: bool,
    /// Whether the buffer contains dropped entries
    pub has_dropped_entries: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    // ==================== UndoFlags Tests ====================

    #[test]
    fn test_undo_flags_values() {
        assert_eq!(UndoFlags::EmptyEntry as u32, 0);
        assert_eq!(UndoFlags::InsertTuple as u32, 1);
        assert_eq!(UndoFlags::DeleteTuple as u32, 2);
        assert_eq!(UndoFlags::UpdateTuple as u32, 3);
        assert_eq!(UndoFlags::SequenceValue as u32, 4);
        assert_eq!(UndoFlags::DatabaseAttach as u32, 5);
    }

    #[test]
    fn test_undo_flags_try_from() {
        assert_eq!(UndoFlags::try_from(0), Ok(UndoFlags::EmptyEntry));
        assert_eq!(UndoFlags::try_from(1), Ok(UndoFlags::InsertTuple));
        assert_eq!(UndoFlags::try_from(2), Ok(UndoFlags::DeleteTuple));
        assert_eq!(UndoFlags::try_from(3), Ok(UndoFlags::UpdateTuple));
        assert_eq!(UndoFlags::try_from(4), Ok(UndoFlags::SequenceValue));
        assert_eq!(UndoFlags::try_from(5), Ok(UndoFlags::DatabaseAttach));
        assert!(UndoFlags::try_from(6).is_err());
        assert!(UndoFlags::try_from(100).is_err());
    }

    #[test]
    fn test_undo_flags_is_data_modification() {
        assert!(!UndoFlags::EmptyEntry.is_data_modification());
        assert!(UndoFlags::InsertTuple.is_data_modification());
        assert!(UndoFlags::DeleteTuple.is_data_modification());
        assert!(UndoFlags::UpdateTuple.is_data_modification());
        assert!(!UndoFlags::SequenceValue.is_data_modification());
        assert!(!UndoFlags::DatabaseAttach.is_data_modification());
    }

    #[test]
    fn test_undo_flags_is_catalog_modification() {
        assert!(!UndoFlags::EmptyEntry.is_catalog_modification());
        assert!(!UndoFlags::InsertTuple.is_catalog_modification());
        assert!(!UndoFlags::DeleteTuple.is_catalog_modification());
        assert!(!UndoFlags::UpdateTuple.is_catalog_modification());
        assert!(!UndoFlags::SequenceValue.is_catalog_modification());
        assert!(UndoFlags::DatabaseAttach.is_catalog_modification());
    }

    // ==================== UndoEntry Tests ====================

    #[test]
    fn test_undo_entry_insert() {
        let entry = UndoEntry::insert(42, 100, 5);
        assert_eq!(entry.flags, UndoFlags::InsertTuple);
        assert!(entry.is_data_modification());
        assert!(!entry.is_catalog_modification());

        if let UndoPayload::Insert(info) = &entry.payload {
            assert_eq!(info.table_id, 42);
            assert_eq!(info.start_row, 100);
            assert_eq!(info.count, 5);
        } else {
            panic!("Expected Insert payload");
        }
    }

    #[test]
    fn test_undo_entry_delete_consecutive() {
        let entry = UndoEntry::delete_consecutive(42, 100, 10);
        assert_eq!(entry.flags, UndoFlags::DeleteTuple);

        if let UndoPayload::Delete(info) = &entry.payload {
            assert_eq!(info.table_id, 42);
            assert_eq!(info.base_row, 100);
            assert_eq!(info.count, 10);
            assert!(info.is_consecutive);
            assert!(info.row_ids.is_empty());
        } else {
            panic!("Expected Delete payload");
        }
    }

    #[test]
    fn test_undo_entry_delete_rows() {
        let entry = UndoEntry::delete_rows(42, vec![10, 20, 30]);
        assert_eq!(entry.flags, UndoFlags::DeleteTuple);

        if let UndoPayload::Delete(info) = &entry.payload {
            assert_eq!(info.table_id, 42);
            assert_eq!(info.base_row, 10);
            assert_eq!(info.count, 3);
            assert!(!info.is_consecutive);
            assert_eq!(info.row_ids, vec![10, 20, 30]);
        } else {
            panic!("Expected Delete payload");
        }
    }

    #[test]
    fn test_undo_entry_update() {
        let entry = UndoEntry::update(42, 999, vec![5, 6, 7]);
        assert_eq!(entry.flags, UndoFlags::UpdateTuple);

        if let UndoPayload::Update(info) = &entry.payload {
            assert_eq!(info.table_id, 42);
            assert_eq!(info.transaction_id, 999);
            assert_eq!(info.row_ids, vec![5, 6, 7]);
        } else {
            panic!("Expected Update payload");
        }
    }

    #[test]
    fn test_undo_entry_sequence() {
        let entry = UndoEntry::sequence_value(123, 10, 42);
        assert_eq!(entry.flags, UndoFlags::SequenceValue);

        if let UndoPayload::Sequence(info) = &entry.payload {
            assert_eq!(info.sequence_id, 123);
            assert_eq!(info.usage_count, 10);
            assert_eq!(info.counter, 42);
        } else {
            panic!("Expected Sequence payload");
        }
    }

    // ==================== UndoBuffer Tests ====================

    #[test]
    fn test_undo_buffer_new_is_empty() {
        let buffer = UndoBuffer::new();
        assert!(buffer.is_empty());
        assert_eq!(buffer.len(), 0);
        assert!(!buffer.changes_made());
    }

    #[test]
    fn test_undo_buffer_push_and_changes_made() {
        let mut buffer = UndoBuffer::new();

        assert!(!buffer.changes_made());

        buffer.push_insert(1, 0, 1);
        assert!(buffer.changes_made());
        assert_eq!(buffer.len(), 1);

        buffer.push_insert(1, 1, 1);
        assert!(buffer.changes_made());
        assert_eq!(buffer.len(), 2);
    }

    #[test]
    fn test_undo_buffer_rollback_clears_entries() {
        let mut buffer = UndoBuffer::new();
        buffer.push_insert(1, 0, 1);
        buffer.push_insert(1, 1, 1);
        buffer.push(UndoEntry::delete_consecutive(1, 2, 1));

        assert_eq!(buffer.len(), 3);
        assert!(buffer.changes_made());

        let transaction = Transaction::new(1, 100);
        buffer.rollback(&transaction);

        assert_eq!(buffer.len(), 0);
        assert!(!buffer.changes_made());
        assert!(buffer.is_empty());
    }

    #[test]
    fn test_undo_buffer_all_entry_types() {
        let mut buffer = UndoBuffer::new();

        buffer.push_insert(1, 100, 5);
        buffer.push_delete_consecutive(1, 200, 3);
        buffer.push_update(1, 999, vec![300]);
        buffer.push_sequence_value(10, 5, 42);
        buffer.push(UndoEntry {
            flags: UndoFlags::DatabaseAttach,
            payload: UndoPayload::DatabaseAttach {
                schema: "public".into(),
                database: "users".into(),
            },
        });

        assert_eq!(buffer.len(), 5);
        assert!(buffer.changes_made());
    }

    #[test]
    fn test_undo_buffer_get_properties() {
        let mut buffer = UndoBuffer::new();

        buffer.push_insert(1, 0, 5);
        buffer.push_delete_rows(1, vec![10, 20, 30]);
        buffer.push_update(1, 999, vec![5]);
        buffer.push(UndoEntry {
            flags: UndoFlags::DatabaseAttach,
            payload: UndoPayload::DatabaseAttach {
                schema: "public".into(),
                database: "users".into(),
            },
        });

        let props = buffer.get_properties();
        assert!(props.has_catalog_changes);
        assert!(props.has_deletes);
        assert!(props.has_updates);
        assert!(props.estimated_size > 0);
    }

    #[test]
    fn test_undo_buffer_iteration() {
        let mut buffer = UndoBuffer::new();
        buffer.push_insert(1, 0, 1);
        buffer.push_insert(1, 1, 1);
        buffer.push_insert(1, 2, 1);

        // Forward iteration
        let entries: Vec<_> = buffer.iter().collect();
        assert_eq!(entries.len(), 3);

        // Reverse iteration
        let rev_entries: Vec<_> = buffer.iter_reverse().collect();
        assert_eq!(rev_entries.len(), 3);
    }

    // ==================== Error/Edge Case Tests ====================

    #[test]
    fn test_undo_buffer_rollback_empty_buffer() {
        // Rollback on empty buffer should not panic
        let mut buffer = UndoBuffer::new();
        assert!(!buffer.changes_made());

        let transaction = Transaction::new(1, 100);
        buffer.rollback(&transaction);

        assert!(!buffer.changes_made());
        assert!(buffer.is_empty());
    }

    #[test]
    fn test_undo_buffer_multiple_rollbacks() {
        // Multiple rollbacks should be safe
        let mut buffer = UndoBuffer::new();
        buffer.push_insert(1, 0, 1);

        let transaction = Transaction::new(1, 100);
        buffer.rollback(&transaction);
        assert!(!buffer.changes_made());

        // Second rollback on empty buffer
        buffer.rollback(&transaction);
        assert!(!buffer.changes_made());
    }

    #[test]
    fn test_undo_entry_delete_empty_rows() {
        // Edge case: delete with empty row list
        let entry = UndoEntry::delete_rows(42, vec![]);
        if let UndoPayload::Delete(info) = &entry.payload {
            assert_eq!(info.count, 0);
            assert_eq!(info.base_row, 0); // Default when empty
            assert!(!info.is_consecutive);
        } else {
            panic!("Expected Delete payload");
        }
    }

    #[test]
    fn test_align_value() {
        // Test alignment to 8 bytes
        assert_eq!(align_value(0, 8), 0);
        assert_eq!(align_value(1, 8), 8);
        assert_eq!(align_value(7, 8), 8);
        assert_eq!(align_value(8, 8), 8);
        assert_eq!(align_value(9, 8), 16);
        assert_eq!(align_value(15, 8), 16);
        assert_eq!(align_value(16, 8), 16);
    }

    #[test]
    fn test_undo_buffer_allocator_new() {
        let allocator = UndoBufferAllocator::new();
        assert!(!allocator.has_allocations());
        assert_eq!(allocator.block_count(), 0);
        assert_eq!(allocator.estimated_size(), 0);
    }

    #[test]
    fn test_undo_buffer_allocator_single_allocation() {
        let mut allocator = UndoBufferAllocator::new();

        let reference = allocator.allocate(64).unwrap();
        assert!(!reference.ptr().is_null());
        assert_eq!(reference.len(), 64);
        assert_eq!(reference.block_idx(), 0);
        assert_eq!(reference.offset(), 0);

        assert!(allocator.has_allocations());
        assert_eq!(allocator.block_count(), 1);
        assert_eq!(allocator.estimated_size(), 64);
    }

    #[test]
    fn test_undo_buffer_allocator_multiple_allocations_same_block() {
        let mut allocator = UndoBufferAllocator::new();

        // Multiple small allocations should fit in one block
        let ref1 = allocator.allocate(100).unwrap();
        let ref2 = allocator.allocate(200).unwrap();
        let ref3 = allocator.allocate(300).unwrap();

        assert_eq!(allocator.block_count(), 1);
        assert_eq!(allocator.estimated_size(), 600);

        // All should be in the same block
        assert_eq!(ref1.block_idx(), 0);
        assert_eq!(ref2.block_idx(), 0);
        assert_eq!(ref3.block_idx(), 0);

        // Offsets should be sequential
        assert_eq!(ref1.offset(), 0);
        assert_eq!(ref2.offset(), 100);
        assert_eq!(ref3.offset(), 300);
    }

    #[test]
    fn test_undo_buffer_allocator_chained_blocks() {
        let mut allocator = UndoBufferAllocator::new();
        // Set small block size to force chaining
        allocator.set_block_size(256);

        // First allocation uses initial block (4KB by default for small allocs)
        let ref1 = allocator.allocate(200).unwrap();
        assert_eq!(allocator.block_count(), 1);

        // Fill up the first block to force a new one
        // Initial block is 4KB, so we need to allocate more
        for _ in 0..20 {
            allocator.allocate(200).unwrap();
        }

        // Now allocate more - should trigger new block since we set block_size to 256
        // and the current block should be nearly full
        let initial_blocks = allocator.block_count();

        // Keep allocating until we get a new block
        let mut ref2 = allocator.allocate(200).unwrap();
        while ref2.block_idx() == ref1.block_idx() {
            ref2 = allocator.allocate(200).unwrap();
        }

        // Should have more blocks now
        assert!(allocator.block_count() > initial_blocks);
        assert!(ref2.block_idx() > ref1.block_idx());
    }

    #[test]
    fn test_undo_buffer_allocator_large_allocation() {
        let mut allocator = UndoBufferAllocator::new();

        // Allocation larger than default block size
        let large_size = DEFAULT_BLOCK_SIZE + 1000;
        let reference = allocator.allocate(large_size).unwrap();

        assert!(!reference.ptr().is_null());
        assert_eq!(reference.len(), large_size);
        assert_eq!(allocator.block_count(), 1);

        // Block capacity should be next power of 2
        let block = allocator.block(0).unwrap();
        assert!(block.capacity() >= large_size);
        assert!(block.capacity().is_power_of_two());
    }

    #[test]
    fn test_undo_buffer_allocator_clear() {
        let mut allocator = UndoBufferAllocator::new();

        allocator.allocate(100).unwrap();
        allocator.allocate(200).unwrap();
        assert!(allocator.has_allocations());
        assert_eq!(allocator.block_count(), 1);

        allocator.clear();
        assert!(!allocator.has_allocations());
        assert_eq!(allocator.block_count(), 0);
        assert_eq!(allocator.estimated_size(), 0);
    }

    #[test]
    fn test_undo_buffer_reference_write() {
        let mut allocator = UndoBufferAllocator::new();
        let mut reference = allocator.allocate(8).unwrap();

        // Write data to the reference
        let data = [1u8, 2, 3, 4, 5, 6, 7, 8];
        unsafe {
            reference.write(&data);
        }

        // Verify data was written
        unsafe {
            let slice = reference.as_mut_slice();
            assert_eq!(slice, &data);
        }
    }

    #[test]
    fn test_undo_buffer_create_entry() {
        let mut buffer = UndoBuffer::new();

        // Create a raw entry
        let reference = buffer.create_entry(UndoFlags::InsertTuple, 24).unwrap();

        assert!(!reference.ptr().is_null());
        assert_eq!(reference.len(), 24);
        assert!(buffer.changes_made());
        assert_eq!(buffer.raw_entry_count(), 1);
        assert_eq!(buffer.total_entry_count(), 1);
        assert_eq!(buffer.block_count(), 1);
    }

    #[test]
    fn test_undo_buffer_create_entry_writes_header() {
        let mut buffer = UndoBuffer::new();

        let reference = buffer.create_entry(UndoFlags::DeleteTuple, 16).unwrap();

        // Verify header was written correctly
        // Header is at (reference.ptr - UNDO_ENTRY_HEADER_SIZE)
        unsafe {
            let header_ptr = reference.ptr().sub(UNDO_ENTRY_HEADER_SIZE);
            let flags = std::ptr::read(header_ptr as *const u32);
            let len = std::ptr::read(header_ptr.add(4) as *const u32);

            assert_eq!(flags, UndoFlags::DeleteTuple as u32);
            assert_eq!(len, 16);
        }
    }

    #[test]
    fn test_undo_buffer_create_entry_multiple() {
        let mut buffer = UndoBuffer::new();

        // Create multiple raw entries
        let ref1 = buffer.create_entry(UndoFlags::InsertTuple, 24).unwrap();
        let ref2 = buffer.create_entry(UndoFlags::DeleteTuple, 32).unwrap();
        let ref3 = buffer.create_entry(UndoFlags::UpdateTuple, 16).unwrap();

        assert_eq!(buffer.raw_entry_count(), 3);
        assert_eq!(buffer.total_entry_count(), 3);

        // Verify each entry has correct length
        assert_eq!(ref1.len(), 24);
        assert_eq!(ref2.len(), 32);
        assert_eq!(ref3.len(), 16);
    }

    #[test]
    fn test_undo_buffer_mixed_api() {
        let mut buffer = UndoBuffer::new();

        // Use both high-level and low-level APIs
        buffer.push_insert(1, 0, 10);
        let _raw_ref = buffer.create_entry(UndoFlags::DeleteTuple, 24).unwrap();
        buffer.push(UndoEntry::sequence_value(7, 1, 99));

        assert_eq!(buffer.len(), 2); // High-level entries
        assert_eq!(buffer.raw_entry_count(), 1); // Raw entries
        assert_eq!(buffer.total_entry_count(), 3); // Total
        assert!(buffer.changes_made());
    }

    #[test]
    fn test_undo_buffer_rollback_clears_allocator() {
        let mut buffer = UndoBuffer::new();

        // Create some raw entries
        buffer.create_entry(UndoFlags::InsertTuple, 24).unwrap();
        buffer.create_entry(UndoFlags::DeleteTuple, 32).unwrap();
        buffer.push_insert(1, 0, 5);

        assert!(buffer.changes_made());
        assert_eq!(buffer.block_count(), 1);

        let transaction = Transaction::new(1, 100);
        buffer.rollback(&transaction);

        assert!(!buffer.changes_made());
        assert_eq!(buffer.block_count(), 0);
        assert_eq!(buffer.raw_entry_count(), 0);
        assert_eq!(buffer.len(), 0);
        assert!(buffer.is_empty());
    }

    #[test]
    fn test_undo_buffer_estimated_size_includes_allocator() {
        let mut buffer = UndoBuffer::new();

        // Add high-level entry
        buffer.push_insert(1, 0, 5);
        let size_with_entry = buffer.estimated_size();

        // Add raw entry
        buffer.create_entry(UndoFlags::DeleteTuple, 100).unwrap();
        let size_with_raw = buffer.estimated_size();

        // Size should increase
        assert!(size_with_raw > size_with_entry);

        // Properties should also reflect allocator size
        let props = buffer.get_properties();
        assert!(props.estimated_size > 0);
    }

    #[test]
    fn test_undo_buffer_with_custom_allocator() {
        let allocator = Arc::new(DefaultAllocator::new());
        let mut buffer = UndoBuffer::with_allocator(allocator);

        buffer.create_entry(UndoFlags::InsertTuple, 64).unwrap();
        buffer.push_insert(1, 0, 10);

        assert!(buffer.changes_made());
        assert_eq!(buffer.total_entry_count(), 2);
    }

    // ==================== Error Case Tests ====================

    #[test]
    fn test_undo_buffer_pointer() {
        let ptr = UndoBufferPointer::new(5, 100);
        assert_eq!(ptr.block_idx, 5);
        assert_eq!(ptr.offset, 100);
    }

    #[test]
    fn test_iterator_state_default() {
        let state = IteratorState::new();
        assert_eq!(state.current_block, 0);
        assert_eq!(state.position, 0);
        assert_eq!(state.end_position, 0);
        assert!(!state.started);
        assert!(!state.is_started());
    }

    #[test]
    fn test_allocator_block_navigation() {
        let mut allocator = UndoBufferAllocator::new();
        allocator.set_block_size(128); // Small blocks to force multiple

        // No blocks initially
        assert!(allocator.tail_idx().is_none());
        assert!(allocator.head_idx().is_none());

        // Allocate to create first block
        allocator.allocate(64).unwrap();
        assert_eq!(allocator.tail_idx(), Some(0));
        assert_eq!(allocator.head_idx(), Some(0));
        assert!(allocator.next_block(0).is_none());
        assert!(allocator.prev_block(0).is_none());

        // Force second block by filling first
        for _ in 0..100 {
            allocator.allocate(64).unwrap();
        }

        // Should have multiple blocks now
        let block_count = allocator.block_count();
        assert!(block_count > 1);
        assert_eq!(allocator.tail_idx(), Some(0));
        assert_eq!(allocator.head_idx(), Some(block_count - 1));

        // Test navigation
        assert_eq!(allocator.next_block(0), Some(1));
        assert!(allocator.prev_block(0).is_none());
        assert_eq!(allocator.prev_block(1), Some(0));
    }

    #[test]
    fn test_iterate_entries_empty_buffer() {
        let buffer = UndoBuffer::new();
        let mut count = 0;
        buffer.iterate_entries(|_flags, _ptr| {
            count += 1;
        });
        assert_eq!(count, 0);
    }

    #[test]
    fn test_iterate_entries_single_entry() {
        let mut buffer = UndoBuffer::new();

        // Create a single raw entry
        let reference = buffer.create_entry(UndoFlags::InsertTuple, 24).unwrap();

        // Write some test data to the payload
        unsafe {
            let data = [1u8, 2, 3, 4, 5, 6, 7, 8];
            std::ptr::copy_nonoverlapping(data.as_ptr(), reference.ptr(), 8);
        }

        let mut entries_found = Vec::new();
        buffer.iterate_entries(|flags, ptr| {
            entries_found.push((flags, ptr));
        });

        assert_eq!(entries_found.len(), 1);
        assert_eq!(entries_found[0].0, UndoFlags::InsertTuple);
        assert!(!entries_found[0].1.is_null());
    }

    #[test]
    fn test_iterate_entries_multiple_entries() {
        let mut buffer = UndoBuffer::new();

        // Create multiple raw entries with different types
        buffer.create_entry(UndoFlags::InsertTuple, 16).unwrap();
        buffer.create_entry(UndoFlags::DeleteTuple, 24).unwrap();
        buffer.create_entry(UndoFlags::UpdateTuple, 32).unwrap();
        buffer.create_entry(UndoFlags::DatabaseAttach, 8).unwrap();

        let mut flags_found = Vec::new();
        buffer.iterate_entries(|flags, _ptr| {
            flags_found.push(flags);
        });

        // Should iterate in insertion order
        assert_eq!(flags_found.len(), 4);
        assert_eq!(flags_found[0], UndoFlags::InsertTuple);
        assert_eq!(flags_found[1], UndoFlags::DeleteTuple);
        assert_eq!(flags_found[2], UndoFlags::UpdateTuple);
        assert_eq!(flags_found[3], UndoFlags::DatabaseAttach);
    }

    #[test]
    fn test_iterate_entries_returns_state() {
        let mut buffer = UndoBuffer::new();
        buffer.create_entry(UndoFlags::InsertTuple, 16).unwrap();

        let state = buffer.iterate_entries(|_flags, _ptr| {});

        assert!(state.started);
    }

    #[test]
    fn test_reverse_iterate_entries_empty_buffer() {
        let buffer = UndoBuffer::new();
        let mut count = 0;
        buffer.reverse_iterate_entries(|_flags, _ptr| {
            count += 1;
        });
        assert_eq!(count, 0);
    }

    #[test]
    fn test_reverse_iterate_entries_single_entry() {
        let mut buffer = UndoBuffer::new();
        buffer.create_entry(UndoFlags::DeleteTuple, 16).unwrap();

        let mut entries_found = Vec::new();
        buffer.reverse_iterate_entries(|flags, ptr| {
            entries_found.push((flags, ptr));
        });

        assert_eq!(entries_found.len(), 1);
        assert_eq!(entries_found[0].0, UndoFlags::DeleteTuple);
    }

    #[test]
    fn test_reverse_iterate_entries_multiple_entries() {
        let mut buffer = UndoBuffer::new();

        // Create entries in order: A, B, C, D
        buffer.create_entry(UndoFlags::InsertTuple, 16).unwrap();
        buffer.create_entry(UndoFlags::DeleteTuple, 24).unwrap();
        buffer.create_entry(UndoFlags::UpdateTuple, 32).unwrap();
        buffer.create_entry(UndoFlags::DatabaseAttach, 8).unwrap();

        let mut flags_found = Vec::new();
        buffer.reverse_iterate_entries(|flags, _ptr| {
            flags_found.push(flags);
        });

        // Should iterate in reverse order: D, C, B, A
        assert_eq!(flags_found.len(), 4);
        assert_eq!(flags_found[0], UndoFlags::DatabaseAttach);
        assert_eq!(flags_found[1], UndoFlags::UpdateTuple);
        assert_eq!(flags_found[2], UndoFlags::DeleteTuple);
        assert_eq!(flags_found[3], UndoFlags::InsertTuple);
    }

    #[test]
    fn test_iterate_vs_reverse_iterate_order() {
        let mut buffer = UndoBuffer::new();

        // Create entries
        for i in 0..5 {
            buffer
                .create_entry(
                    UndoFlags::try_from(i % 5 + 1).unwrap_or(UndoFlags::EmptyEntry),
                    16,
                )
                .unwrap();
        }

        let mut forward_flags = Vec::new();
        buffer.iterate_entries(|flags, _ptr| {
            forward_flags.push(flags);
        });

        let mut reverse_flags = Vec::new();
        buffer.reverse_iterate_entries(|flags, _ptr| {
            reverse_flags.push(flags);
        });

        // Reverse should be the opposite of forward
        reverse_flags.reverse();
        assert_eq!(forward_flags, reverse_flags);
    }

    #[test]
    fn test_iterate_entries_across_multiple_blocks() {
        let mut buffer = UndoBuffer::new();

        // Use small block size to force multiple blocks
        // We need to access the allocator directly for this
        // Instead, create many entries to fill blocks

        // Create many entries to potentially span multiple blocks
        let entry_count = 100;
        for i in 0..entry_count {
            buffer
                .create_entry(
                    UndoFlags::try_from((i % 5) as u32 + 1).unwrap_or(UndoFlags::EmptyEntry),
                    64, // Larger payload
                )
                .unwrap();
        }

        let mut count = 0;
        buffer.iterate_entries(|_flags, _ptr| {
            count += 1;
        });

        assert_eq!(count, entry_count);
    }

    #[test]
    fn test_reverse_iterate_entries_across_multiple_blocks() {
        let mut buffer = UndoBuffer::new();

        let entry_count = 100;
        let mut expected_flags = Vec::new();

        for i in 0..entry_count {
            let flags = UndoFlags::try_from((i % 5) as u32 + 1).unwrap_or(UndoFlags::EmptyEntry);
            expected_flags.push(flags);
            buffer.create_entry(flags, 64).unwrap();
        }

        let mut reverse_flags = Vec::new();
        buffer.reverse_iterate_entries(|flags, _ptr| {
            reverse_flags.push(flags);
        });

        assert_eq!(reverse_flags.len(), entry_count);

        // Verify reverse order
        expected_flags.reverse();
        assert_eq!(reverse_flags, expected_flags);
    }

    #[test]
    fn test_iterate_entries_payload_data_integrity() {
        let mut buffer = UndoBuffer::new();

        // Create entry and write known data
        let reference = buffer.create_entry(UndoFlags::InsertTuple, 8).unwrap();
        let test_value: u64 = 0xDEADBEEF_CAFEBABE;
        unsafe {
            std::ptr::write(reference.ptr() as *mut u64, test_value);
        }

        // Verify data through iteration
        buffer.iterate_entries(|flags, ptr| {
            assert_eq!(flags, UndoFlags::InsertTuple);
            let value = unsafe { std::ptr::read(ptr as *const u64) };
            assert_eq!(value, test_value);
        });
    }

    #[test]
    fn test_iterate_entries_range_empty() {
        let buffer = UndoBuffer::new();
        let start = IteratorState::new();
        let end = IteratorState::new();

        let mut count = 0;
        buffer.iterate_entries_range(&start, &end, |_flags, _ptr| {
            count += 1;
        });

        // end_state.started is false, so no iteration
        assert_eq!(count, 0);
    }

    #[test]
    fn test_iterate_entries_with_state() {
        let mut buffer = UndoBuffer::new();

        buffer.create_entry(UndoFlags::InsertTuple, 16).unwrap();
        buffer.create_entry(UndoFlags::DeleteTuple, 16).unwrap();

        let mut state = IteratorState::new();
        let mut flags_found = Vec::new();

        buffer.iterate_entries_with_state(&mut state, &mut |flags, _ptr| {
            flags_found.push(flags);
        });

        assert!(state.started);
        assert_eq!(flags_found.len(), 2);
    }

    // ==================== Error/Edge Case Tests for Iterators ====================

    #[test]
    fn test_iterate_entries_after_rollback() {
        let mut buffer = UndoBuffer::new();

        buffer.create_entry(UndoFlags::InsertTuple, 16).unwrap();
        buffer.create_entry(UndoFlags::DeleteTuple, 16).unwrap();

        let transaction = Transaction::new(1, 100);
        buffer.rollback(&transaction);

        // After rollback, iteration should find nothing
        let mut count = 0;
        buffer.iterate_entries(|_flags, _ptr| {
            count += 1;
        });
        assert_eq!(count, 0);

        let mut reverse_count = 0;
        buffer.reverse_iterate_entries(|_flags, _ptr| {
            reverse_count += 1;
        });
        assert_eq!(reverse_count, 0);
    }

    #[test]
    fn test_iterate_entries_variable_payload_sizes() {
        let mut buffer = UndoBuffer::new();

        // Create entries with varying payload sizes
        let sizes = [8, 16, 32, 64, 128, 7, 13, 29]; // Mix of aligned and unaligned
        for (i, &size) in sizes.iter().enumerate() {
            buffer
                .create_entry(
                    UndoFlags::try_from((i % 5) as u32 + 1).unwrap_or(UndoFlags::EmptyEntry),
                    size,
                )
                .unwrap();
        }

        let mut count = 0;
        buffer.iterate_entries(|_flags, _ptr| {
            count += 1;
        });

        assert_eq!(count, sizes.len());
    }

    #[test]
    fn test_commit_empty_buffer() {
        let buffer = UndoBuffer::new();
        let transaction = Transaction::new(1, 100);
        let state = buffer.commit(&transaction, 1000);
        // Empty buffer commit should complete without error
        assert!(!state.started); // No entries to iterate
    }

    #[test]
    fn test_commit_with_high_level_entries() {
        let mut buffer = UndoBuffer::new();

        // Add various high-level entries
        buffer.push_insert(1, 0, 10);
        buffer.push_delete_consecutive(1, 100, 5);
        buffer.push_update(1, 999, vec![200, 201, 202]);
        buffer.push_sequence_value(7, 1, 99);

        let transaction = Transaction::new(1, 100);
        let state = buffer.commit(&transaction, 1000);

        // Commit should complete and return a valid state
        // High-level entries are still present (not cleared by commit)
        assert_eq!(buffer.len(), 4);
        assert!(buffer.changes_made());
        // State may or may not be started depending on raw entries
        let _ = state;
    }

    #[test]
    fn test_commit_with_raw_entries() {
        let mut buffer = UndoBuffer::new();

        // Create raw entries
        buffer.create_entry(UndoFlags::InsertTuple, 24).unwrap();
        buffer.create_entry(UndoFlags::DeleteTuple, 32).unwrap();
        buffer.create_entry(UndoFlags::UpdateTuple, 16).unwrap();

        let transaction = Transaction::new(1, 100);
        let state = buffer.commit(&transaction, 1000);

        // Commit should iterate through all raw entries
        assert!(state.started);
        assert_eq!(buffer.raw_entry_count(), 3);
    }

    #[test]
    fn test_commit_with_mixed_entries() {
        let mut buffer = UndoBuffer::new();

        // Mix of high-level and raw entries
        buffer.push_insert(1, 0, 5);
        buffer.create_entry(UndoFlags::DeleteTuple, 24).unwrap();
        buffer.push_sequence_value(8, 1, 100);
        buffer.create_entry(UndoFlags::UpdateTuple, 16).unwrap();

        let transaction = Transaction::new(1, 100);
        let state = buffer.commit(&transaction, 2000);

        // Both types should be processed
        assert_eq!(buffer.len(), 2);
        assert_eq!(buffer.raw_entry_count(), 2);
        assert!(state.started);
    }

    #[test]
    fn test_rollback_clears_all_entries() {
        let mut buffer = UndoBuffer::new();

        // Add entries
        buffer.push_insert(1, 0, 10);
        buffer.push_delete_consecutive(1, 100, 5);
        buffer.create_entry(UndoFlags::UpdateTuple, 24).unwrap();

        assert!(buffer.changes_made());
        assert_eq!(buffer.len(), 2);
        assert_eq!(buffer.raw_entry_count(), 1);

        let transaction = Transaction::new(1, 100);
        buffer.rollback(&transaction);

        // All entries should be cleared
        assert!(!buffer.changes_made());
        assert_eq!(buffer.len(), 0);
        assert_eq!(buffer.raw_entry_count(), 0);
        assert_eq!(buffer.block_count(), 0);
        assert!(buffer.is_empty());
    }

    #[test]
    fn test_rollback_processes_in_reverse_order() {
        let mut buffer = UndoBuffer::new();

        // Create entries in specific order
        buffer.create_entry(UndoFlags::InsertTuple, 16).unwrap();
        buffer.create_entry(UndoFlags::DeleteTuple, 16).unwrap();
        buffer.create_entry(UndoFlags::UpdateTuple, 16).unwrap();

        // Track the order entries are processed during rollback
        // (We can't directly observe this, but we verify the buffer is cleared)
        let transaction = Transaction::new(1, 100);
        buffer.rollback(&transaction);

        assert!(buffer.is_empty());
    }

    #[test]
    fn test_rollback_with_sequence_entries() {
        let mut buffer = UndoBuffer::new();

        buffer.push_sequence_value(1, 10, 100);
        buffer.push_sequence_value(2, 20, 200);

        let transaction = Transaction::new(1, 100);
        buffer.rollback(&transaction);

        assert!(buffer.is_empty());
    }

    #[test]
    fn test_cleanup_clears_committed_entries() {
        let mut buffer = UndoBuffer::new();

        // Add entries
        buffer.push_insert(1, 0, 10);
        buffer.push_delete_consecutive(1, 100, 5);
        buffer.create_entry(UndoFlags::UpdateTuple, 24).unwrap();

        // Cleanup should clear all entries
        buffer.cleanup(500, ActiveTransactionState::NoOtherTransactions);

        assert!(!buffer.changes_made());
        assert_eq!(buffer.len(), 0);
        assert_eq!(buffer.raw_entry_count(), 0);
        assert!(buffer.is_empty());
    }

    #[test]
    fn test_cleanup_empty_buffer() {
        let mut buffer = UndoBuffer::new();

        // Cleanup on empty buffer should not panic
        buffer.cleanup(1000, ActiveTransactionState::NoOtherTransactions);

        assert!(buffer.is_empty());
    }

    #[test]
    fn test_cleanup_with_database_attach_entries() {
        let mut buffer = UndoBuffer::new();

        buffer.push(UndoEntry {
            flags: UndoFlags::DatabaseAttach,
            payload: UndoPayload::DatabaseAttach {
                schema: "public".into(),
                database: "users".into(),
            },
        });
        buffer.push(UndoEntry {
            flags: UndoFlags::DatabaseAttach,
            payload: UndoPayload::DatabaseAttach {
                schema: "main".into(),
                database: "orders".into(),
            },
        });

        buffer.cleanup(1000, ActiveTransactionState::NoOtherTransactions);

        assert!(buffer.is_empty());
    }

    #[test]
    fn test_revert_commit_empty_state() {
        let buffer = UndoBuffer::new();
        let end_state = IteratorState::new();

        // Revert with empty state should not panic
        let transaction = Transaction::new(1, 100);
        buffer.revert_commit(&transaction, &end_state, 1000);
    }

    #[test]
    fn test_revert_commit_with_entries() {
        let mut buffer = UndoBuffer::new();

        // Create entries
        buffer.create_entry(UndoFlags::InsertTuple, 16).unwrap();
        buffer.create_entry(UndoFlags::DeleteTuple, 16).unwrap();

        // Get state after partial iteration
        let mut end_state = IteratorState::new();
        let mut count = 0;
        buffer.iterate_entries_with_state(&mut end_state, &mut |_flags, _ptr| {
            count += 1;
        });

        // Revert commit up to the end state
        let transaction = Transaction::new(1, 100);
        buffer.revert_commit(&transaction, &end_state, 500);

        // Buffer should still have entries (revert doesn't clear)
        assert_eq!(buffer.raw_entry_count(), 2);
    }

    #[test]
    fn test_commit_then_cleanup_sequence() {
        let mut buffer = UndoBuffer::new();

        // Add entries
        buffer.push_insert(1, 0, 10);
        buffer.push_update(1, 999, vec![5, 6, 7]);
        buffer.create_entry(UndoFlags::DeleteTuple, 24).unwrap();

        // Commit
        let transaction = Transaction::new(1, 100);
        let _state = buffer.commit(&transaction, 1000);

        // Entries still exist after commit
        assert!(buffer.changes_made());

        // Cleanup removes them
        buffer.cleanup(500, ActiveTransactionState::NoOtherTransactions);

        assert!(buffer.is_empty());
    }

    #[test]
    fn test_rollback_idempotent() {
        let mut buffer = UndoBuffer::new();

        buffer.push_insert(1, 0, 10);
        buffer.create_entry(UndoFlags::UpdateTuple, 16).unwrap();

        let transaction = Transaction::new(1, 100);
        // First rollback
        buffer.rollback(&transaction);
        assert!(buffer.is_empty());

        // Second rollback should be safe
        buffer.rollback(&transaction);
        assert!(buffer.is_empty());

        // Third rollback
        buffer.rollback(&transaction);
        assert!(buffer.is_empty());
    }

    #[test]
    fn test_cleanup_idempotent() {
        let mut buffer = UndoBuffer::new();

        buffer.push_insert(1, 0, 10);

        // First cleanup
        buffer.cleanup(1000, ActiveTransactionState::NoOtherTransactions);
        assert!(buffer.is_empty());

        // Second cleanup should be safe
        buffer.cleanup(2000, ActiveTransactionState::NoOtherTransactions);
        assert!(buffer.is_empty());
    }

    #[test]
    fn test_commit_preserves_entry_order() {
        let mut buffer = UndoBuffer::new();

        // Create entries in specific order
        buffer.create_entry(UndoFlags::DatabaseAttach, 8).unwrap();
        buffer.create_entry(UndoFlags::InsertTuple, 16).unwrap();
        buffer.create_entry(UndoFlags::DeleteTuple, 24).unwrap();
        buffer.create_entry(UndoFlags::UpdateTuple, 32).unwrap();

        let mut commit_order = Vec::new();
        buffer.iterate_entries(|flags, _ptr| {
            commit_order.push(flags);
        });

        // Verify forward order (as used by commit)
        assert_eq!(commit_order.len(), 4);
        assert_eq!(commit_order[0], UndoFlags::DatabaseAttach);
        assert_eq!(commit_order[1], UndoFlags::InsertTuple);
        assert_eq!(commit_order[2], UndoFlags::DeleteTuple);
        assert_eq!(commit_order[3], UndoFlags::UpdateTuple);
    }

    #[test]
    fn test_rollback_reverse_order() {
        let mut buffer = UndoBuffer::new();

        // Create entries in specific order
        buffer.create_entry(UndoFlags::DatabaseAttach, 8).unwrap();
        buffer.create_entry(UndoFlags::InsertTuple, 16).unwrap();
        buffer.create_entry(UndoFlags::DeleteTuple, 24).unwrap();
        buffer.create_entry(UndoFlags::UpdateTuple, 32).unwrap();

        let mut rollback_order = Vec::new();
        buffer.reverse_iterate_entries(|flags, _ptr| {
            rollback_order.push(flags);
        });

        // Verify reverse order (as used by rollback)
        assert_eq!(rollback_order.len(), 4);
        assert_eq!(rollback_order[0], UndoFlags::UpdateTuple);
        assert_eq!(rollback_order[1], UndoFlags::DeleteTuple);
        assert_eq!(rollback_order[2], UndoFlags::InsertTuple);
        assert_eq!(rollback_order[3], UndoFlags::DatabaseAttach);
    }
}
