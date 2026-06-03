// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! JoinHashTable - Main hash table structure for join operations.
//!
//!
//! ```text
//! Storage Layout:
//! [BUILD ROW][HASH][NEXT POINTER][FOUND FLAG]
//! [BUILD ROW][HASH][NEXT POINTER][FOUND FLAG]
//! ...
//!
//! Hash Table (separate):
//! [POINTER] -> points to first row with this hash
//! [POINTER]
//! [POINTER]
//! ```
//!
//! ## Implementation Notes
//! - Uses HashBuildStore for build-side row storage
//! - Linear probing with salt comparison
//! - Pointer chaining for collision handling

use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use paro_common::allocator::Allocator;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::memory::{MemoryAccountingClass, MemoryAccountingContext};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_common::vector::SelectionVector;
use paro_planner::operator::join::{JoinComparisonType, JoinCondition, JoinType};
use paro_storage::buffer::BufferPool;
use paro_storage::buffer::MemoryTag;
use paro_storage::row::RowLayout;

use super::build_store::{BuildRowLayout, BuildStoreScanState, HashBuildStore};
use super::hash_kernel::JoinKeyLayout;
use super::ht_entry::HtEntry;
use super::scan_structure::ScanStructure;

#[derive(Debug, Default)]
struct HtEntryTable {
    buffer: Option<paro_common::memory::GrantBuffer>,
    len: usize,
}

impl HtEntryTable {
    fn try_new(
        len: usize,
        allocator: Arc<dyn Allocator>,
        memory: &MemoryAccountingContext,
    ) -> Result<Self> {
        if len == 0 {
            return Ok(Self::default());
        }
        let bytes = len.saturating_mul(std::mem::size_of::<HtEntry>());
        let buffer = memory.allocate_zeroed_buffer(allocator, bytes)?;
        Ok(Self {
            buffer: Some(buffer),
            len,
        })
    }

    fn size_in_bytes(&self) -> usize {
        self.buffer
            .as_ref()
            .map(|buffer| buffer.size())
            .unwrap_or(0)
    }

    fn as_ptr(&self) -> *const HtEntry {
        let Some(buffer) = &self.buffer else {
            return ptr::null();
        };
        buffer.as_ptr() as *const HtEntry
    }

    fn as_mut_slice(&mut self) -> &mut [HtEntry] {
        let Some(buffer) = &self.buffer else {
            return &mut [];
        };
        unsafe { std::slice::from_raw_parts_mut(buffer.as_ptr() as *mut HtEntry, self.len) }
    }
}

/// Configuration for JoinHashTable.
#[derive(Debug, Clone)]
pub struct JoinHashTableConfig {
    /// Initial radix bits for partitioning (for external joins).
    pub initial_radix_bits: usize,
    /// Whether to use salt for large hash tables.
    pub use_salt_threshold: usize,
}

impl Default for JoinHashTableConfig {
    fn default() -> Self {
        Self {
            initial_radix_bits: 4,
            use_salt_threshold: 8192,
        }
    }
}

/// JoinHashTable - Hash table for implementing join operations.
///
/// This is a linear probing hash table optimized for join operations:
/// - Build phase: Insert rows from the build side
/// - Finalize phase: Construct the pointer table
/// - Probe phase: Look up rows from the probe side
///
/// # Memory Layout
/// Rows are stored in [`HashBuildStore`] with the following format:
/// ```text
/// [key columns][payload columns][matched flag (optional)][hash value][next pointer]
/// ```
pub struct JoinHashTable {
    /// Buffer pool for memory allocation.
    buffer_pool: Arc<BufferPool>,

    /// Allocator used for execution-side chunks and selection vectors.
    allocator: Arc<dyn Allocator>,

    /// Join type.
    pub join_type: JoinType,

    /// Join conditions.
    pub conditions: Vec<JoinCondition>,

    /// Types of equality key columns.
    pub equality_types: Vec<LogicalType>,

    /// Whether NULL values are considered equal for each equality key.
    pub null_values_are_equal: Vec<bool>,

    /// Equality comparison type for each hash key.
    equality_comparisons: Vec<JoinComparisonType>,

    /// Typed hash/equality dispatch for equality keys.
    key_layout: JoinKeyLayout,

    /// Types of build-side payload columns.
    pub build_types: Vec<LogicalType>,

    /// Column index of the found flag inside the build-side row layout.
    pub found_flag_column_index: Option<usize>,

    /// Layout for build rows and spill chunks.
    build_row_layout: BuildRowLayout,

    /// Build-side retained row ownership.
    build_memory: MemoryAccountingContext,

    /// Pointer table ownership.
    pointer_memory: MemoryAccountingContext,

    /// Layout exposed to spill/reload paths (`keys + payload + found? + hash`).
    spill_layout: Arc<RowLayout>,

    /// Build-side rows with stable pointer addresses for hash chains.
    build_store: Mutex<HashBuildStore>,

    /// Hash table entries (pointer table).
    entries: Mutex<HtEntryTable>,

    /// Lock-free read pointer published after finalize.
    probe_entries: AtomicPtr<HtEntry>,

    /// Capacity of the hash table (power of 2).
    capacity: AtomicUsize,

    /// Bitmask for hash -> index (capacity - 1).
    bitmask: AtomicUsize,

    /// Whether the hash table has been finalized.
    pub finalized: AtomicBool,

    /// Whether any NULL keys were encountered.
    pub has_null: AtomicBool,

    /// Offset to the next pointer in each row.
    pub pointer_offset: usize,

    /// Offset to the hash value in each row.
    pub hash_offset: usize,

    /// Offset to the optional found flag in each row.
    pub found_flag_offset: Option<usize>,

    /// Whether chains longer than one exist.
    pub chains_longer_than_one: AtomicBool,

    /// Configuration.
    config: JoinHashTableConfig,

    /// Row count.
    count: AtomicUsize,
}

impl std::fmt::Debug for JoinHashTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JoinHashTable")
            .field("buffer_pool", &self.buffer_pool)
            .field("allocator", &self.allocator.name())
            .field("join_type", &self.join_type)
            .field("conditions", &self.conditions)
            .field("equality_types", &self.equality_types)
            .field("build_types", &self.build_types)
            .field("capacity", &self.capacity)
            .field("finalized", &self.finalized)
            .field("config", &self.config)
            .finish()
    }
}

impl JoinHashTable {
    /// Create a new JoinHashTable.
    pub fn new(
        buffer_pool: Arc<BufferPool>,
        allocator: Arc<dyn Allocator>,
        conditions: Vec<JoinCondition>,
        build_types: Vec<LogicalType>,
        join_type: JoinType,
        config: JoinHashTableConfig,
    ) -> Self {
        let memory = MemoryAccountingContext::detached(
            MemoryTag::HashTable,
            MemoryAccountingClass::Revocable,
        );
        Self::new_with_memory(
            buffer_pool,
            allocator,
            conditions,
            build_types,
            join_type,
            config,
            memory,
        )
    }

    pub fn new_with_memory(
        buffer_pool: Arc<BufferPool>,
        allocator: Arc<dyn Allocator>,
        conditions: Vec<JoinCondition>,
        build_types: Vec<LogicalType>,
        join_type: JoinType,
        config: JoinHashTableConfig,
        memory: MemoryAccountingContext,
    ) -> Self {
        // Extract equality types from conditions
        let mut equality_types = Vec::new();
        let mut null_values_are_equal = Vec::new();
        let mut equality_comparisons = Vec::new();

        for cond in &conditions {
            if cond.comparison == JoinComparisonType::Equal
                || cond.comparison == JoinComparisonType::NotDistinctFrom
            {
                equality_types.push(cond.left.return_type().clone());
                null_values_are_equal.push(matches!(
                    cond.comparison,
                    JoinComparisonType::NotDistinctFrom
                ));
                equality_comparisons.push(cond.comparison);
            }
        }

        let found_flag_column_index = if Self::propagates_build_side(join_type) {
            Some(equality_types.len() + build_types.len())
        } else {
            None
        };
        let build_row_layout = BuildRowLayout::new(
            equality_types.clone(),
            build_types.clone(),
            found_flag_column_index.is_some(),
        );
        let key_layout = JoinKeyLayout::new(&equality_types);
        let spill_layout = Arc::new(RowLayout::from_types(
            build_row_layout.spill_types().to_vec(),
            paro_storage::row::RowValidityType::CanHaveNullValues,
        ));
        let build_memory = memory.with_class(MemoryAccountingClass::Revocable);
        let pointer_memory = memory.with_class(MemoryAccountingClass::NonRevocable);
        let build_store = HashBuildStore::new_with_memory(
            buffer_pool.clone(),
            allocator.clone(),
            build_row_layout.clone(),
            MemoryTag::HashTable,
            build_memory.clone(),
        );
        let pointer_offset = build_row_layout.next_offset();
        let hash_offset = build_row_layout.hash_offset();
        let found_flag_offset = found_flag_column_index.map(|_| build_row_layout.found_offset());

        Self {
            buffer_pool,
            allocator,
            join_type,
            conditions,
            equality_types,
            null_values_are_equal,
            equality_comparisons,
            key_layout,
            build_types,
            found_flag_column_index,
            build_row_layout,
            build_memory,
            pointer_memory,
            spill_layout,
            build_store: Mutex::new(build_store),
            entries: Mutex::new(HtEntryTable::default()),
            probe_entries: AtomicPtr::new(ptr::null_mut()),
            capacity: AtomicUsize::new(0),
            bitmask: AtomicUsize::new(0),
            finalized: AtomicBool::new(false),
            has_null: AtomicBool::new(false),
            pointer_offset,
            hash_offset,
            found_flag_offset,
            chains_longer_than_one: AtomicBool::new(false),
            config,
            count: AtomicUsize::new(0),
        }
    }

    /// Check if the join type propagates build side (FULL, RIGHT, RIGHT_ANTI, RIGHT_SEMI).
    pub fn propagates_build_side(join_type: JoinType) -> bool {
        matches!(
            join_type,
            JoinType::Outer | JoinType::Right | JoinType::RightAnti | JoinType::RightSemi
        )
    }

    /// Get the spill/reload row layout.
    pub fn layout(&self) -> &RowLayout {
        &self.spill_layout
    }

    pub fn key_count(&self) -> usize {
        self.build_row_layout.key_count()
    }

    pub fn spill_types(&self) -> &[LogicalType] {
        self.build_row_layout.spill_types()
    }

    /// Get the buffer pool.
    pub fn buffer_pool(&self) -> &Arc<BufferPool> {
        &self.buffer_pool
    }

    /// Get the execution allocator used for scratch chunks and selections.
    pub fn allocator(&self) -> &Arc<dyn Allocator> {
        &self.allocator
    }

    /// Get the number of rows in the hash table.
    pub fn count(&self) -> usize {
        self.count.load(Ordering::Relaxed)
    }

    /// Add to the row count.
    pub fn add_count(&self, delta: usize) {
        self.count.fetch_add(delta, Ordering::Relaxed);
    }

    /// Check if the hash table is empty.
    pub fn is_empty(&self) -> bool {
        self.count() == 0
    }

    /// Get the size in bytes.
    pub fn size_in_bytes(&self) -> usize {
        let data_size = self.build_store.lock().unwrap().size_in_bytes();
        let entries_size = self.entries.lock().unwrap().size_in_bytes();
        data_size + entries_size
    }

    /// Estimate pointer-table size for a given row count.
    pub fn pointer_table_size_for_count(count: usize) -> usize {
        Self::calculate_capacity(count) * std::mem::size_of::<HtEntry>()
    }

    /// Estimate total hash-table size from a data payload + row count.
    pub fn estimate_total_size(data_size: usize, row_count: usize) -> usize {
        data_size.saturating_add(Self::pointer_table_size_for_count(row_count))
    }

    /// Get mutable access to the build store.
    pub fn get_build_store(&self) -> std::sync::MutexGuard<'_, HashBuildStore> {
        self.build_store.lock().unwrap()
    }

    pub fn has_found_flag(&self) -> bool {
        self.found_flag_offset.is_some()
    }

    pub fn read_equality_value(&self, row_ptr: usize, key_idx: usize) -> Value {
        self.build_row_layout
            .read_value(row_ptr as *const u8, key_idx)
    }

    pub fn read_build_value(&self, row_ptr: usize, build_idx: usize) -> Value {
        self.build_row_layout.read_value(
            row_ptr as *const u8,
            self.build_row_layout.payload_base_col_idx(build_idx),
        )
    }

    pub fn scan_spill_chunk(
        &self,
        state: &mut BuildStoreScanState,
        output: &mut Chunk,
    ) -> Result<usize> {
        self.build_store
            .lock()
            .unwrap()
            .scan_spill_chunk(state, output)
    }

    pub fn build_rows_size_in_bytes(&self) -> usize {
        self.build_store.lock().unwrap().size_in_bytes()
    }

    pub fn try_build_rows_size_in_bytes(&self) -> Option<usize> {
        self.build_store
            .try_lock()
            .ok()
            .map(|store| store.size_in_bytes())
    }

    pub fn all_build_row_ptrs(&self) -> Vec<usize> {
        self.build_store.lock().unwrap().all_row_ptrs()
    }

    pub fn drain_build_store_spill_chunks<F>(&self, visitor: F) -> Result<()>
    where
        F: FnMut(&Chunk) -> Result<()>,
    {
        let empty_store = HashBuildStore::new_with_memory(
            self.buffer_pool.clone(),
            self.allocator.clone(),
            self.build_row_layout.clone(),
            MemoryTag::HashTable,
            self.build_memory.clone(),
        );
        let drained_store = {
            let mut store = self.build_store.lock().unwrap();
            std::mem::replace(&mut *store, empty_store)
        };
        self.count.store(0, Ordering::Relaxed);
        drained_store.drain_spill_chunks(visitor)
    }

    pub fn try_drain_build_store_spill_chunks<F>(&self, visitor: F) -> Result<Option<usize>>
    where
        F: FnMut(&Chunk) -> Result<()>,
    {
        let mut store = match self.build_store.try_lock() {
            Ok(store) => store,
            Err(std::sync::TryLockError::WouldBlock) => return Ok(None),
            Err(std::sync::TryLockError::Poisoned(_)) => {
                return Err(paro_error::internal(
                    "hash join build store mutex poisoned during spill drain",
                ))
            }
        };
        let drained_bytes = store.size_in_bytes();
        if drained_bytes == 0 {
            return Ok(Some(0));
        }
        let empty_store = HashBuildStore::new_with_memory(
            self.buffer_pool.clone(),
            self.allocator.clone(),
            self.build_row_layout.clone(),
            MemoryTag::HashTable,
            self.build_memory.clone(),
        );
        let drained_store = std::mem::replace(&mut *store, empty_store);
        drop(store);
        self.count.store(0, Ordering::Relaxed);
        drained_store.drain_spill_chunks(visitor)?;
        Ok(Some(drained_bytes))
    }

    fn prepare_keys(
        &self,
        keys: &paro_common::chunk::Chunk,
        input_sel: Option<&SelectionVector>,
        selected_count: usize,
        build_side: bool,
        output_sel: &mut SelectionVector,
    ) -> Result<usize> {
        let input_count = selected_count.min(keys.size());
        if output_sel.capacity() < input_count {
            return Err(paro_common::error::internal(format!(
                "hash join selection capacity too small: capacity={}, required={input_count}",
                output_sel.capacity()
            )));
        }
        output_sel.set_len(input_count);
        let input_rows = input_sel.map(SelectionVector::as_slice);
        let output_rows = output_sel.as_mut_slice();

        if build_side && Self::propagates_build_side(self.join_type) {
            for idx in 0..input_count {
                let row_idx = input_rows.map_or(idx, |rows| rows[idx] as usize);
                output_rows[idx] = row_idx as u32;
            }
            return Ok(output_sel.len());
        }

        let mut output_count = 0usize;
        for idx in 0..input_count {
            let row_idx = input_rows.map_or(idx, |rows| rows[idx] as usize);
            let mut keep = true;
            for (col_idx, nulls_equal) in self.null_values_are_equal.iter().enumerate() {
                if !*nulls_equal && keys.data[col_idx].is_null(row_idx) {
                    keep = false;
                    break;
                }
            }
            if keep {
                output_rows[output_count] = row_idx as u32;
                output_count += 1;
            }
        }
        output_sel.set_len(output_count);
        Ok(output_count)
    }

    pub(crate) fn key_values_match_build_row(
        &self,
        keys: &paro_common::chunk::Chunk,
        probe_row_idx: usize,
        row_ptr: usize,
    ) -> bool {
        self.key_layout.keys_match_build_row(
            keys,
            probe_row_idx,
            &self.build_row_layout,
            row_ptr,
            &self.equality_comparisons,
        )
    }

    pub fn set_build_side_found(&self, row_ptr: usize, found: bool) -> bool {
        if self.found_flag_offset.is_none() {
            return false;
        }
        self.build_row_layout.set_found(row_ptr as *mut u8, found);
        true
    }

    pub fn build_side_found(&self, row_ptr: usize) -> Option<bool> {
        self.found_flag_offset
            .map(|_| self.build_row_layout.found(row_ptr as *const u8))
    }

    pub fn hash_column_index(&self) -> usize {
        self.build_row_layout.hash_input_col_idx()
    }

    pub fn set_has_null(&self, has_null: bool) {
        self.has_null.store(has_null, Ordering::Relaxed);
    }

    pub fn has_null_keys(&self) -> bool {
        self.has_null.load(Ordering::Relaxed)
    }

    pub fn reset_runtime_state(&self) {
        self.finalized.store(false, Ordering::Release);
        self.probe_entries.store(ptr::null_mut(), Ordering::Release);
        *self.entries.lock().unwrap() = HtEntryTable::default();
        self.capacity.store(0, Ordering::Relaxed);
        self.bitmask.store(0, Ordering::Relaxed);
        self.chains_longer_than_one.store(false, Ordering::Relaxed);
    }

    pub fn reset_data_collection(&self) {
        self.build_store.lock().unwrap().reset();
        self.count.store(0, Ordering::Relaxed);
    }

    pub fn refresh_count_from_data_collection(&self) {
        let count = self.build_store.lock().unwrap().count() as usize;
        self.count.store(count, Ordering::Relaxed);
    }

    /// Build by appending keys and payload to the build store.
    ///
    /// Production build sinks should call [`Self::build_with_scratch`] and reuse
    /// the selection/hash buffers across chunks. This wrapper stays small for
    /// tests and one-off callers.
    pub fn build(
        &self,
        keys: &paro_common::chunk::Chunk,
        payload: &paro_common::chunk::Chunk,
    ) -> Result<()> {
        let mut selection =
            SelectionVector::try_with_capacity(keys.size(), keys.allocator().clone())?;
        let mut hashes = Vec::with_capacity(keys.size());
        self.build_with_scratch(keys, payload, &mut selection, &mut hashes)
            .map(|_| ())
    }

    pub fn build_with_scratch(
        &self,
        keys: &paro_common::chunk::Chunk,
        payload: &paro_common::chunk::Chunk,
        build_sel: &mut SelectionVector,
        hashes: &mut Vec<u64>,
    ) -> Result<usize> {
        debug_assert!(!self.finalized.load(Ordering::Relaxed));
        debug_assert_eq!(keys.size(), payload.size());

        if keys.size() == 0 {
            return Ok(0);
        }

        if build_sel.capacity() < keys.size() {
            *build_sel = SelectionVector::try_with_capacity(keys.size(), keys.allocator().clone())?;
        }
        let appended_count = self.prepare_keys(keys, None, keys.size(), true, build_sel)?;
        if appended_count < keys.size() {
            self.has_null.store(true, Ordering::Relaxed);
        }
        if appended_count == 0 {
            return Ok(0);
        }

        hashes.resize(appended_count, 0);
        self.key_layout
            .hash_selected_into(keys, build_sel, appended_count, hashes);
        let appended_count = self.build_store.lock().unwrap().append_key_payload_chunk(
            keys,
            payload,
            build_sel,
            appended_count,
            hashes,
            false,
        )?;

        // Update count
        self.count.fetch_add(appended_count, Ordering::Relaxed);

        Ok(appended_count)
    }

    /// Allocate the pointer table.
    fn allocate_pointer_table(&self) -> Result<()> {
        let count = self.count();
        let capacity = Self::calculate_capacity(count);

        let mut entries = self.entries.lock().unwrap();
        *entries = HtEntryTable::try_new(capacity, self.allocator.clone(), &self.pointer_memory)?;

        self.capacity.store(capacity, Ordering::Relaxed);
        self.bitmask.store(capacity - 1, Ordering::Relaxed);
        Ok(())
    }

    /// Calculate capacity for the given count (next power of 2, with load factor).
    fn calculate_capacity(count: usize) -> usize {
        if count == 0 {
            return 16; // Minimum
        }

        // Load factor of ~0.5
        let min_capacity = count * 2;

        // Round up to power of 2
        let mut capacity = 16;
        while capacity < min_capacity {
            capacity *= 2;
        }

        capacity
    }

    /// Initialize the pointer table with empty entries.
    fn initialize_pointer_table(&self) {
        let mut entries = self.entries.lock().unwrap();
        for entry in entries.as_mut_slice().iter_mut() {
            *entry = HtEntry::empty();
        }
    }

    /// Finalize the hash table after build phase.
    ///
    /// This constructs the pointer table from the stored rows.
    pub fn finalize(&self) -> Result<()> {
        if self.finalized.load(Ordering::Acquire) {
            return Ok(());
        }

        if self.count() == 0 {
            self.probe_entries.store(ptr::null_mut(), Ordering::Release);
            self.finalized.store(true, Ordering::Release);
            return Ok(());
        }

        // Step 1: Allocate and initialize pointer table
        self.allocate_pointer_table()?;
        self.initialize_pointer_table();

        let mut entries = self.entries.lock().unwrap();
        let bitmask = self.bitmask.load(Ordering::Relaxed);
        let has_long_chains = self
            .build_store
            .lock()
            .unwrap()
            .build_pointer_chains(entries.as_mut_slice(), bitmask);
        let entries_ptr = entries.as_ptr() as *mut HtEntry;
        self.chains_longer_than_one
            .store(has_long_chains, Ordering::Relaxed);

        self.probe_entries.store(entries_ptr, Ordering::Release);
        self.finalized.store(true, Ordering::Release);
        Ok(())
    }

    /// Check if salt should be used for probing.
    pub fn use_salt(&self) -> bool {
        self.capacity.load(Ordering::Relaxed) > self.config.use_salt_threshold
    }

    /// Create a scan structure for probing.
    pub fn create_scan_structure(&self) -> Result<ScanStructure> {
        let mut ss = ScanStructure::try_new(self.pointer_offset, self.allocator.clone())?;
        ss.has_long_chains = self.chains_longer_than_one.load(Ordering::Relaxed);
        Ok(ss)
    }

    /// Merge another hash table into this one.
    ///
    /// Used for parallel build: each thread builds a local hash table,
    /// then they are merged into the global one.
    pub fn merge(&self, other: Arc<JoinHashTable>) -> Result<()> {
        debug_assert!(!self.finalized.load(Ordering::Relaxed));
        let empty_other = HashBuildStore::new_with_memory(
            other.buffer_pool().clone(),
            self.allocator.clone(),
            other.build_row_layout.clone(),
            MemoryTag::HashTable,
            other.build_memory.clone(),
        );
        let mut self_store = self.build_store.lock().unwrap();
        let mut other_store = other.build_store.lock().unwrap();
        let moved = std::mem::replace(&mut *other_store, empty_other);
        self_store.merge(moved)?;

        // Merge has_null
        if other.has_null.load(Ordering::Relaxed) {
            self.has_null.store(true, Ordering::Relaxed);
        }

        // Count is updated by combine
        let self_count = self_store.count() as usize;
        self.count.store(self_count, Ordering::Relaxed);

        Ok(())
    }

    /// Probe the hash table with a batch of keys.
    ///
    /// # Arguments
    /// * `keys` - Chunk containing the join keys
    /// * `scan_structure` - Structure to store the probe result and state for iteration
    /// * `sel` - Optional selection vector for filtering probe keys
    pub fn probe(
        &self,
        keys: &paro_common::chunk::Chunk,
        scan_structure: &mut ScanStructure,
        sel: Option<&SelectionVector>,
        selected_count: usize,
    ) -> Result<()> {
        if !self.finalized.load(Ordering::Acquire) {
            panic!("Cannot probe non-finalized hash table");
        }

        if selected_count == 0 || keys.size() == 0 {
            scan_structure.reset();
            return Ok(());
        }

        scan_structure.ensure_capacity(keys.size())?;
        scan_structure.reset();
        scan_structure.has_long_chains = self.chains_longer_than_one.load(Ordering::Relaxed);

        if scan_structure.probe_sel.capacity() < selected_count.min(keys.size()) {
            scan_structure.probe_sel = SelectionVector::try_with_capacity(
                selected_count.min(keys.size()),
                keys.allocator().clone(),
            )?;
        }
        let filtered_count = self.prepare_keys(
            keys,
            sel,
            selected_count,
            false,
            &mut scan_structure.probe_sel,
        )?;
        scan_structure.has_null_value_filter = filtered_count < selected_count;
        if filtered_count == 0 {
            scan_structure.count = 0;
            scan_structure.sel_vector.set_len(0);
            return Ok(());
        }

        if scan_structure.hashes.len() < filtered_count {
            scan_structure.hashes.resize(filtered_count, 0);
        }
        self.key_layout.hash_selected_into(
            keys,
            &scan_structure.probe_sel,
            filtered_count,
            &mut scan_structure.hashes,
        );

        let capacity = self.capacity.load(Ordering::Relaxed);
        let bitmask = self.bitmask.load(Ordering::Relaxed);
        let entries_ptr = self.probe_entries.load(Ordering::Acquire);
        if entries_ptr.is_null() || capacity == 0 {
            scan_structure.count = 0;
            scan_structure.sel_vector.set_len(0);
            return Ok(());
        }
        let entries =
            unsafe { std::slice::from_raw_parts(entries_ptr as *const HtEntry, capacity) };
        let mut matched_count = 0usize;
        scan_structure.sel_vector.set_len(keys.size());
        let matched_rows = scan_structure.sel_vector.as_mut_slice();
        let prepared_rows = scan_structure.probe_sel.as_slice();

        for prepared_idx in 0..filtered_count {
            let hash = scan_structure.hashes[prepared_idx];
            let row_idx = prepared_rows[prepared_idx] as usize;
            let mut entry_idx = (hash as usize) & bitmask;
            let row_salt = hash & HtEntry::SALT_MASK;

            loop {
                let entry = entries[entry_idx];
                if !entry.is_occupied() {
                    break;
                }
                if entry.get_salt_bits() == row_salt {
                    scan_structure.pointers[row_idx] = entry.get_pointer() as usize;
                    matched_rows[matched_count] = row_idx as u32;
                    matched_count += 1;
                    break;
                }
                super::ht_entry::increment_and_wrap(&mut entry_idx, bitmask);
            }
        }

        scan_structure.count = matched_count;
        scan_structure.sel_vector.set_len(matched_count);
        Ok(())
    }

    /// Compute hash values with the same key hashing path used by probe().
    pub(crate) fn compute_key_hashes(
        &self,
        keys: &paro_common::chunk::Chunk,
        hashes: &mut paro_common::vector::Vector,
    ) -> Result<()> {
        hashes.try_set_count(keys.size())?;
        if keys.is_empty() {
            return Ok(());
        }
        self.key_layout.hash_keys_into(keys, hashes)?;
        Ok(())
    }

    /// Reset the hash table for reuse.
    pub fn reset(&self) {
        self.reset_runtime_state();
    }

    pub fn create_build_scan_state(&self) -> BuildStoreScanState {
        BuildStoreScanState::default()
    }

    pub fn create_full_outer_scan_state(&self) -> FullOuterScanState {
        FullOuterScanState::new()
    }

    pub fn scan_full_outer(
        &self,
        state: &mut FullOuterScanState,
        emit_found: bool,
        result: &mut Chunk,
    ) -> Result<usize> {
        if self.found_flag_column_index.is_none() {
            result.set_cardinality(0);
            return Ok(0);
        }
        self.build_store.lock().unwrap().scan_payload_rows(
            &mut state.scan_state,
            emit_found,
            &self.build_types,
            result,
        )
    }
}

/// State for scanning unmatched build rows (FULL/RIGHT outer joins).
#[derive(Debug)]
pub struct FullOuterScanState {
    pub scan_state: BuildStoreScanState,
}

impl FullOuterScanState {
    pub fn new() -> Self {
        Self {
            scan_state: BuildStoreScanState::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_common::allocator::MemoryTag;
    use paro_common::memory::{MemoryDomain, MemoryOwner};
    use paro_common::vector::VECTOR_SIZE;
    use paro_planner::expression::{ConstantExpression, Expression};
    use paro_storage::buffer::BufferPool;

    use crate::join_hashtable::hash_kernel::JoinKeyLayout;
    use crate::join_hashtable::ht_entry::HtEntry;
    use crate::memory_runtime::QueryMemoryPool;

    fn create_test_buffer_pool() -> Arc<BufferPool> {
        BufferPool::new_arc(64 * 1024 * 1024) // 64MB
    }

    fn equality_condition() -> JoinCondition {
        JoinCondition::new(
            Expression::Constant(ConstantExpression::new(
                Value::Integer(1),
                LogicalType::Integer,
            )),
            Expression::Constant(ConstantExpression::new(
                Value::Integer(1),
                LogicalType::Integer,
            )),
            JoinComparisonType::Equal,
        )
    }

    fn not_distinct_condition() -> JoinCondition {
        JoinCondition::new(
            Expression::Constant(ConstantExpression::new(
                Value::Integer(1),
                LogicalType::Integer,
            )),
            Expression::Constant(ConstantExpression::new(
                Value::Integer(1),
                LogicalType::Integer,
            )),
            JoinComparisonType::NotDistinctFrom,
        )
    }

    fn chunk_from_optional_i32(values: &[Option<i32>]) -> Chunk {
        let mut chunk = paro_common::test_utils::test_chunk_with_capacity(
            &[LogicalType::Integer],
            values.len(),
        );
        for (row_idx, value) in values.iter().enumerate() {
            let column = chunk.column_mut(0).expect("column must exist");
            match value {
                Some(value) => column.set_value(row_idx, &Value::Integer(*value)),
                None => column.set_value(row_idx, &Value::Null(LogicalType::Integer)),
            }
        }
        chunk.set_cardinality(values.len());
        chunk
    }

    #[test]
    fn join_hash_table_build_store_respects_query_quota() {
        let pool = Arc::new(QueryMemoryPool::new(1));
        let owner: Arc<dyn MemoryOwner> = pool;
        let memory = MemoryAccountingContext::from_owner(
            owner,
            MemoryDomain::Host,
            MemoryTag::HashTable,
            MemoryAccountingClass::Revocable,
        );
        let table = JoinHashTable::new_with_memory(
            create_test_buffer_pool(),
            paro_common::test_utils::test_allocator(),
            vec![equality_condition()],
            vec![LogicalType::Integer],
            JoinType::Inner,
            JoinHashTableConfig::default(),
            memory,
        );
        let keys = chunk_from_optional_i32(&[Some(1), Some(2)]);
        let payload = chunk_from_optional_i32(&[Some(10), Some(20)]);

        let err = table
            .build(&keys, &payload)
            .expect_err("tiny query quota must reject hash join build storage");
        assert!(err.to_string().contains("quota"));
    }

    fn find_linear_probe_collision_pair() -> (i32, i32) {
        let layout = JoinKeyLayout::new(&[LogicalType::Integer]);
        let values = (0..10_000).collect::<Vec<i32>>();
        let keys = Chunk::from_arc_vectors(
            vec![Arc::new(
                paro_common::test_utils::test_i32_vector_with_allocator(
                    &values,
                    paro_common::test_utils::test_allocator(),
                ),
            )],
            paro_common::test_utils::test_allocator(),
        );
        let hashes = (0..values.len())
            .map(|row_idx| layout.hash_key_at(&keys, row_idx))
            .collect::<Vec<_>>();
        for left in 0..values.len() {
            let left_hash = hashes[left];
            for right in (left + 1)..values.len() {
                let right_hash = hashes[right];
                if (left_hash as usize & 15) == (right_hash as usize & 15)
                    && (left_hash & HtEntry::SALT_MASK) != (right_hash & HtEntry::SALT_MASK)
                {
                    return (values[left], values[right]);
                }
            }
        }
        panic!("failed to find collision pair with different salts");
    }

    #[test]
    fn test_join_hash_table_new() {
        let buffer_pool = create_test_buffer_pool();
        let conditions = vec![];
        let build_types = vec![LogicalType::Integer, LogicalType::Varchar];

        let ht = JoinHashTable::new(
            buffer_pool,
            paro_common::test_utils::test_allocator(),
            conditions,
            build_types,
            JoinType::Inner,
            JoinHashTableConfig::default(),
        );

        assert_eq!(ht.count(), 0);
        assert!(ht.is_empty());
        assert!(!ht.finalized.load(Ordering::Relaxed));
    }

    #[test]
    fn test_calculate_capacity() {
        assert_eq!(JoinHashTable::calculate_capacity(0), 16);
        assert_eq!(JoinHashTable::calculate_capacity(10), 32);
        assert_eq!(JoinHashTable::calculate_capacity(100), 256);
        assert_eq!(JoinHashTable::calculate_capacity(1000), 2048);
    }

    #[test]
    fn test_propagates_build_side() {
        assert!(!JoinHashTable::propagates_build_side(JoinType::Inner));
        assert!(!JoinHashTable::propagates_build_side(JoinType::Left));
        assert!(JoinHashTable::propagates_build_side(JoinType::Right));
        assert!(JoinHashTable::propagates_build_side(JoinType::Outer));
        assert!(!JoinHashTable::propagates_build_side(JoinType::Semi));
        assert!(!JoinHashTable::propagates_build_side(JoinType::Anti));
    }

    #[test]
    fn test_finalize() {
        let buffer_pool = create_test_buffer_pool();
        let ht = JoinHashTable::new(
            buffer_pool,
            paro_common::test_utils::test_allocator(),
            vec![],
            vec![LogicalType::Integer],
            JoinType::Inner,
            JoinHashTableConfig::default(),
        );

        assert!(!ht.finalized.load(Ordering::Relaxed));
        ht.finalize().unwrap();
        assert!(ht.finalized.load(Ordering::Relaxed));
    }

    #[test]
    fn test_right_join_layout_tracks_found_flag_offset() {
        let ht = JoinHashTable::new(
            create_test_buffer_pool(),
            paro_common::test_utils::test_allocator(),
            vec![equality_condition()],
            vec![LogicalType::Integer],
            JoinType::Right,
            JoinHashTableConfig::default(),
        );

        assert!(ht.has_found_flag());
        assert_eq!(ht.found_flag_column_index, Some(2));
        assert!(ht.found_flag_offset.is_some());
    }

    #[test]
    fn test_scan_full_outer_uses_found_flag_filter() {
        let ht = JoinHashTable::new(
            create_test_buffer_pool(),
            paro_common::test_utils::test_allocator(),
            vec![equality_condition()],
            vec![LogicalType::Integer],
            JoinType::Right,
            JoinHashTableConfig::default(),
        );

        let keys = Chunk::from_arc_vectors(
            vec![Arc::new(
                paro_common::test_utils::test_i32_vector_with_allocator(
                    &[1, 2, 3],
                    paro_common::test_utils::test_allocator(),
                ),
            )],
            paro_common::test_utils::test_allocator(),
        );
        let payload = Chunk::from_arc_vectors(
            vec![Arc::new(
                paro_common::test_utils::test_i32_vector_with_allocator(
                    &[10, 20, 30],
                    paro_common::test_utils::test_allocator(),
                ),
            )],
            paro_common::test_utils::test_allocator(),
        );

        ht.build(&keys, &payload).unwrap();
        ht.finalize().unwrap();

        let row_ptrs = ht.all_build_row_ptrs();
        assert_eq!(row_ptrs.len(), 3);

        for (row_ptr, found) in row_ptrs.iter().copied().zip([false, true, false]) {
            ht.set_build_side_found(row_ptr, found);
            let stored = ht.build_side_found(row_ptr).unwrap();
            assert_eq!(stored, found);
        }

        let mut unmatched_state = ht.create_full_outer_scan_state();
        let mut unmatched = Chunk::try_new(paro_common::test_utils::test_allocator())
            .expect("test chunk allocation failed");
        let unmatched_count = ht
            .scan_full_outer(&mut unmatched_state, false, &mut unmatched)
            .unwrap();
        assert_eq!(unmatched_count, 2);
        assert_eq!(unmatched.data[0].get_value(0).to_string(), "10");
        assert_eq!(unmatched.data[0].get_value(1).to_string(), "30");

        let mut matched_state = ht.create_full_outer_scan_state();
        let mut matched = Chunk::try_new(paro_common::test_utils::test_allocator())
            .expect("test chunk allocation failed");
        let matched_count = ht
            .scan_full_outer(&mut matched_state, true, &mut matched)
            .unwrap();
        assert_eq!(matched_count, 1);
        assert_eq!(matched.data[0].get_value(0).to_string(), "20");
    }

    #[test]
    fn test_build_filters_null_keys_for_equal_conditions() {
        let ht = JoinHashTable::new(
            create_test_buffer_pool(),
            paro_common::test_utils::test_allocator(),
            vec![equality_condition()],
            vec![LogicalType::Integer],
            JoinType::Inner,
            JoinHashTableConfig::default(),
        );

        let keys = chunk_from_optional_i32(&[Some(1), None, Some(2)]);
        let payload = Chunk::from_arc_vectors(
            vec![Arc::new(
                paro_common::test_utils::test_i32_vector_with_allocator(
                    &[10, 20, 30],
                    paro_common::test_utils::test_allocator(),
                ),
            )],
            paro_common::test_utils::test_allocator(),
        );

        ht.build(&keys, &payload).unwrap();

        assert_eq!(ht.count(), 2);
        assert!(ht.has_null.load(Ordering::Relaxed));
    }

    #[test]
    fn test_not_distinct_from_keeps_null_keys_and_probe_matches_them() {
        let ht = JoinHashTable::new(
            create_test_buffer_pool(),
            paro_common::test_utils::test_allocator(),
            vec![not_distinct_condition()],
            vec![LogicalType::Integer],
            JoinType::Inner,
            JoinHashTableConfig::default(),
        );

        let build_keys = chunk_from_optional_i32(&[None]);
        let build_payload = Chunk::from_arc_vectors(
            vec![Arc::new(
                paro_common::test_utils::test_i32_vector_with_allocator(
                    &[99],
                    paro_common::test_utils::test_allocator(),
                ),
            )],
            paro_common::test_utils::test_allocator(),
        );

        ht.build(&build_keys, &build_payload).unwrap();
        ht.finalize().unwrap();

        assert_eq!(ht.count(), 1);
        assert!(!ht.has_null.load(Ordering::Relaxed));

        let probe_keys = chunk_from_optional_i32(&[None]);
        let left = probe_keys.clone();
        let mut scan = ht
            .create_scan_structure()
            .expect("test scan structure allocation failed");
        ht.probe(&probe_keys, &mut scan, None, probe_keys.size())
            .unwrap();

        let mut result = paro_common::test_utils::test_chunk_with_capacity(
            &[LogicalType::Integer, LogicalType::Integer],
            VECTOR_SIZE,
        );
        let count = scan
            .next_inner_join(&probe_keys, &left, &mut result, &ht, &[], &[])
            .unwrap();

        assert_eq!(count, 1);
        assert!(result.data[0].is_null(0));
        assert_eq!(result.data[1].get_value(0).to_string(), "99");
    }

    #[test]
    fn test_probe_respects_selected_count() {
        let ht = JoinHashTable::new(
            create_test_buffer_pool(),
            paro_common::test_utils::test_allocator(),
            vec![equality_condition()],
            vec![LogicalType::Integer],
            JoinType::Inner,
            JoinHashTableConfig::default(),
        );

        let keys = Chunk::from_arc_vectors(
            vec![Arc::new(
                paro_common::test_utils::test_i32_vector_with_allocator(
                    &[3],
                    paro_common::test_utils::test_allocator(),
                ),
            )],
            paro_common::test_utils::test_allocator(),
        );
        let payload = Chunk::from_arc_vectors(
            vec![Arc::new(
                paro_common::test_utils::test_i32_vector_with_allocator(
                    &[30],
                    paro_common::test_utils::test_allocator(),
                ),
            )],
            paro_common::test_utils::test_allocator(),
        );
        ht.build(&keys, &payload).unwrap();
        ht.finalize().unwrap();

        let probe_keys = Chunk::from_arc_vectors(
            vec![Arc::new(
                paro_common::test_utils::test_i32_vector_with_allocator(
                    &[3, 3],
                    paro_common::test_utils::test_allocator(),
                ),
            )],
            paro_common::test_utils::test_allocator(),
        );
        let probe_sel = paro_common::test_utils::test_selection(vec![1, 0]);
        let mut scan = ht
            .create_scan_structure()
            .expect("test scan structure allocation failed");

        ht.probe(&probe_keys, &mut scan, Some(&probe_sel), 1)
            .unwrap();

        assert_eq!(scan.count, 1);
        assert_eq!(scan.sel_vector.get(0), 1);
    }

    #[test]
    fn test_probe_linear_probing_finds_rows_behind_salt_mismatch() {
        let (first_key, second_key) = find_linear_probe_collision_pair();
        let ht = JoinHashTable::new(
            create_test_buffer_pool(),
            paro_common::test_utils::test_allocator(),
            vec![equality_condition()],
            vec![LogicalType::Integer],
            JoinType::Inner,
            JoinHashTableConfig::default(),
        );

        let build_keys = Chunk::from_arc_vectors(
            vec![Arc::new(
                paro_common::test_utils::test_i32_vector_with_allocator(
                    &[first_key, second_key],
                    paro_common::test_utils::test_allocator(),
                ),
            )],
            paro_common::test_utils::test_allocator(),
        );
        let build_payload = Chunk::from_arc_vectors(
            vec![Arc::new(
                paro_common::test_utils::test_i32_vector_with_allocator(
                    &[11, 22],
                    paro_common::test_utils::test_allocator(),
                ),
            )],
            paro_common::test_utils::test_allocator(),
        );
        ht.build(&build_keys, &build_payload).unwrap();
        ht.finalize().unwrap();

        let probe_keys = Chunk::from_arc_vectors(
            vec![Arc::new(
                paro_common::test_utils::test_i32_vector_with_allocator(
                    &[second_key],
                    paro_common::test_utils::test_allocator(),
                ),
            )],
            paro_common::test_utils::test_allocator(),
        );
        let left = probe_keys.clone();
        let mut scan = ht
            .create_scan_structure()
            .expect("test scan structure allocation failed");
        ht.probe(&probe_keys, &mut scan, None, probe_keys.size())
            .unwrap();

        let mut result = paro_common::test_utils::test_chunk_with_capacity(
            &[LogicalType::Integer, LogicalType::Integer],
            VECTOR_SIZE,
        );
        let count = scan
            .next_inner_join(&probe_keys, &left, &mut result, &ht, &[], &[])
            .unwrap();

        assert_eq!(count, 1);
        assert_eq!(
            result.data[0].get_value(0).to_string(),
            second_key.to_string()
        );
        assert_eq!(result.data[1].get_value(0).to_string(), "22");
    }
}
