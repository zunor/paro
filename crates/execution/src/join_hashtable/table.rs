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

use arc_swap::ArcSwapOption;
use paro_common::allocator::Allocator;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::memory::{MemoryAccountingClass, MemoryAccountingContext};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_common::vector::{SelectionVector, Vector};
use paro_planner::operator::join::{JoinComparisonType, JoinCondition, JoinType};
use paro_storage::buffer::BufferPool;
use paro_storage::buffer::MemoryTag;
use paro_storage::row::RowLayout;

use super::build_store::{BuildRowLayout, BuildStoreScanState, HashBuildStore};
use super::hash_kernel::{JoinKeyLayout, PreparedProbeKeys};
use super::ht_entry::HtEntry;
pub(crate) use super::integer_index::ConcurrentBuildTimeIntegerIndexBuilder as BuildTimeIntegerIndexBuilder;
use super::integer_index::{
    ConcurrentBuildTimeIntegerIndexBuilder, ExactIntegerJoinIndex, IntegerKeyKind,
};
use super::pair_integer_index::ExactI64PairJoinIndex;
use super::scan_structure::ScanStructure;

#[path = "integer_finalize.rs"]
mod integer_finalize;
use integer_finalize::BuiltIntegerIndex;
pub(crate) use integer_finalize::ParallelDirectIntegerIndexBuild;
#[path = "pair_integer_finalize.rs"]
mod pair_integer_finalize;
#[path = "table_grouped_reduction.rs"]
mod table_grouped_reduction;
use table_grouped_reduction::GroupedReductionExtremaState;

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
    /// The complete equality-key tuple is proven unique by the logical plan.
    pub build_keys_unique: bool,
    /// Build-time direct artifact shared by local tables. It is absent from the
    /// merged table so finish can take sole ownership and publish it.
    pub(crate) build_time_integer_builder: Option<Arc<ConcurrentBuildTimeIntegerIndexBuilder>>,
}

impl Default for JoinHashTableConfig {
    fn default() -> Self {
        Self {
            initial_radix_bits: 4,
            use_salt_threshold: 8192,
            build_keys_unique: false,
            build_time_integer_builder: None,
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
    /// Visible prefix of `build_types`; remaining columns are execution-only.
    build_output_count: usize,

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

    /// Optional exact index owner for bounded unique integer equality keys.
    integer_index: ArcSwapOption<ExactIntegerJoinIndex>,

    /// Optional exact index for two-column BIGINT equality keys.
    pair_integer_index: ArcSwapOption<ExactI64PairJoinIndex>,

    /// Lazily allocated compact per-key summaries for fused reductions.
    grouped_reduction_extrema: Mutex<GroupedReductionExtremaState>,

    /// Build-time bounds accumulated from hot key vectors. Keeping this local
    /// to each parallel table removes a full serialized-row pass at finalize.
    integer_index_build_stats: Mutex<IntegerIndexBuildStats>,

    /// Physical key kind retained even if later input makes the exact index
    /// ineligible. It is used to materialize hashes for generic fallback.
    integer_key_kind: Option<IntegerKeyKind>,

    /// Whether any build rows were appended without computing their hash.
    /// Hashing is deferred while a bounded exact integer index is viable.
    deferred_hashes: AtomicBool,

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

#[derive(Debug, Clone, Copy)]
enum IntegerIndexBuildStats {
    Ineligible,
    Empty {
        kind: IntegerKeyKind,
    },
    Bounded {
        kind: IntegerKeyKind,
        minimum: u128,
        maximum: u128,
        count: usize,
    },
}

impl IntegerIndexBuildStats {
    fn new(
        join_type: JoinType,
        equality_types: &[LogicalType],
        comparisons: &[JoinComparisonType],
    ) -> Self {
        if join_type == JoinType::Invalid
            || equality_types.len() != 1
            || comparisons
                .iter()
                .any(|comparison| *comparison != JoinComparisonType::Equal)
        {
            return Self::Ineligible;
        }
        IntegerKeyKind::from_logical_type(&equality_types[0])
            .map_or(Self::Ineligible, |kind| Self::Empty { kind })
    }

    fn add_batch(&mut self, vector: &Vector, logical_count: usize, selected: &[u32]) -> Result<()> {
        let kind = match *self {
            Self::Empty { kind } | Self::Bounded { kind, .. } => kind,
            Self::Ineligible => return Ok(()),
        };
        let Some((minimum, maximum)) = kind.selected_bounds(vector, logical_count, selected)?
        else {
            *self = Self::Ineligible;
            return Ok(());
        };
        *self = match *self {
            Self::Empty { .. } => Self::Bounded {
                kind,
                minimum,
                maximum,
                count: selected.len(),
            },
            Self::Bounded {
                minimum: current_minimum,
                maximum: current_maximum,
                count,
                ..
            } => Self::Bounded {
                kind,
                minimum: current_minimum.min(minimum),
                maximum: current_maximum.max(maximum),
                count: count.saturating_add(selected.len()),
            },
            Self::Ineligible => unreachable!("ineligible stats returned above"),
        };
        Ok(())
    }

    fn exact_index_candidate(self) -> bool {
        !matches!(self, Self::Ineligible)
    }

    fn merge(&mut self, incoming: Self) {
        *self = match (*self, incoming) {
            (Self::Ineligible, _) | (_, Self::Ineligible) => Self::Ineligible,
            (
                current @ Self::Empty { kind },
                Self::Empty {
                    kind: incoming_kind,
                },
            ) if kind == incoming_kind => current,
            (
                Self::Empty { kind },
                bounded @ Self::Bounded {
                    kind: incoming_kind,
                    ..
                },
            ) if kind == incoming_kind => bounded,
            (
                bounded @ Self::Bounded { kind, .. },
                Self::Empty {
                    kind: incoming_kind,
                },
            ) if kind == incoming_kind => bounded,
            (
                Self::Bounded {
                    kind,
                    minimum,
                    maximum,
                    count,
                },
                Self::Bounded {
                    kind: incoming_kind,
                    minimum: incoming_minimum,
                    maximum: incoming_maximum,
                    count: incoming_count,
                },
            ) if kind == incoming_kind => Self::Bounded {
                kind,
                minimum: minimum.min(incoming_minimum),
                maximum: maximum.max(incoming_maximum),
                count: count.saturating_add(incoming_count),
            },
            _ => Self::Ineligible,
        };
    }
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
        let build_output_count = build_types.len();
        Self::new_with_memory_and_output_count(
            buffer_pool,
            allocator,
            conditions,
            build_types,
            build_output_count,
            join_type,
            config,
            memory,
        )
    }

    pub fn new_with_memory_and_output_count(
        buffer_pool: Arc<BufferPool>,
        allocator: Arc<dyn Allocator>,
        conditions: Vec<JoinCondition>,
        build_types: Vec<LogicalType>,
        build_output_count: usize,
        join_type: JoinType,
        config: JoinHashTableConfig,
        memory: MemoryAccountingContext,
    ) -> Self {
        assert!(
            build_output_count <= build_types.len(),
            "visible hash-join payload cannot exceed stored payload"
        );
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

        let build_keys_may_be_null = Self::propagates_build_side(join_type);
        let found_flag_column_index = if build_keys_may_be_null {
            Some(equality_types.len() + build_types.len())
        } else {
            None
        };
        let build_row_layout = BuildRowLayout::new(
            equality_types.clone(),
            build_types.clone(),
            found_flag_column_index.is_some(),
        );
        let key_layout = JoinKeyLayout::new(
            &equality_types,
            &equality_comparisons,
            build_keys_may_be_null,
        );
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
        let integer_index_build_stats =
            IntegerIndexBuildStats::new(join_type, &equality_types, &equality_comparisons);
        let integer_key_kind = match integer_index_build_stats {
            IntegerIndexBuildStats::Empty { kind }
            | IntegerIndexBuildStats::Bounded { kind, .. } => Some(kind),
            IntegerIndexBuildStats::Ineligible => None,
        };

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
            build_output_count,
            found_flag_column_index,
            build_row_layout,
            build_memory,
            pointer_memory,
            spill_layout,
            build_store: Mutex::new(build_store),
            entries: Mutex::new(HtEntryTable::default()),
            integer_index: ArcSwapOption::empty(),
            pair_integer_index: ArcSwapOption::empty(),
            grouped_reduction_extrema: Mutex::new(GroupedReductionExtremaState::Unconfigured),
            integer_index_build_stats: Mutex::new(integer_index_build_stats),
            integer_key_kind,
            deferred_hashes: AtomicBool::new(false),
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

    pub fn build_output_count(&self) -> usize {
        self.build_output_count
    }

    pub fn build_output_types(&self) -> &[LogicalType] {
        &self.build_types[..self.build_output_count]
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
        let integer_index_size = self
            .integer_index
            .load()
            .as_ref()
            .map(|index| index.size_in_bytes())
            .unwrap_or(0);
        data_size + entries_size + integer_index_size + self.pair_integer_index_size()
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

    /// Gather one build-side payload column for matched row pointers.
    ///
    /// # Safety
    /// Every non-zero entry in `row_ptrs` must come from this hash table's build
    /// store and remain live for this call.
    pub unsafe fn gather_build_column(
        &self,
        row_ptrs: &[usize],
        build_idx: usize,
        output: &mut Vector,
    ) -> Result<()> {
        // SAFETY: forwarded from this method's caller.
        unsafe {
            self.build_row_layout
                .gather_payload_column(row_ptrs, build_idx, output)
        }
    }

    /// Read one fixed-width build payload cell directly from its row.
    ///
    /// # Safety
    /// `row_ptr` must come from this table and remain live, and `T` must match
    /// the payload column's physical representation.
    pub unsafe fn read_build_payload_fixed<T: Copy>(
        &self,
        row_ptr: usize,
        build_idx: usize,
    ) -> Option<T> {
        // SAFETY: forwarded from this method's caller.
        unsafe {
            self.build_row_layout
                .read_payload_fixed(row_ptr as *const u8, build_idx)
        }
    }

    pub fn scan_spill_chunk(
        &self,
        state: &mut BuildStoreScanState,
        output: &mut Chunk,
    ) -> Result<usize> {
        self.materialize_deferred_hashes();
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
        self.materialize_deferred_hashes();
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
        *self.integer_index_build_stats.lock().unwrap() = IntegerIndexBuildStats::new(
            self.join_type,
            &self.equality_types,
            &self.equality_comparisons,
        );
        self.deferred_hashes.store(false, Ordering::Release);
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
        if self.deferred_hashes.swap(false, Ordering::AcqRel) {
            let kind = self
                .integer_key_kind
                .expect("deferred integer hashes require a physical key kind");
            let layout = Arc::clone(self.build_row_layout.base());
            store.materialize_deferred_hashes(|row_ptr| kind.row_hash(layout.as_ref(), row_ptr, 0));
        }
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
        *self.integer_index_build_stats.lock().unwrap() = IntegerIndexBuildStats::new(
            self.join_type,
            &self.equality_types,
            &self.equality_comparisons,
        );
        self.deferred_hashes.store(false, Ordering::Release);
        drained_store.drain_spill_chunks(visitor)?;
        Ok(Some(drained_bytes))
    }

    fn materialize_deferred_hashes(&self) {
        let mut store = self.build_store.lock().unwrap();
        if self.deferred_hashes.swap(false, Ordering::AcqRel) {
            let kind = self
                .integer_key_kind
                .expect("deferred integer hashes require a physical key kind");
            let layout = Arc::clone(self.build_row_layout.base());
            store.materialize_deferred_hashes(|row_ptr| kind.row_hash(layout.as_ref(), row_ptr, 0));
        }
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

        let preserve_all_rows = build_side && Self::propagates_build_side(self.join_type);
        let mut all_null_sensitive_keys_valid = true;
        if !preserve_all_rows {
            for (column_idx, nulls_equal) in self.null_values_are_equal.iter().enumerate() {
                if !*nulls_equal
                    && !keys.data[column_idx]
                        .try_to_view(keys.size())?
                        .validity()
                        .all_valid()
                {
                    all_null_sensitive_keys_valid = false;
                    break;
                }
            }
        }

        if preserve_all_rows || all_null_sensitive_keys_valid {
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

    pub(crate) fn prepare_probe_keys<'a>(
        &self,
        keys: &'a paro_common::chunk::Chunk,
    ) -> Result<PreparedProbeKeys<'a>> {
        self.key_layout.prepare_probe_keys(keys)
    }

    pub(crate) fn key_values_match_build_row(
        &self,
        keys: &PreparedProbeKeys<'_>,
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
        self.build_row_layout
            .set_match_mask(row_ptr as *mut u8, u8::from(found));
        true
    }

    pub fn mark_build_side_match_mask(&self, row_ptr: usize, mask: u8) -> bool {
        if self.found_flag_offset.is_none() {
            return false;
        }
        self.build_row_layout
            .mark_match_mask(row_ptr as *mut u8, mask);
        true
    }

    pub fn build_side_found(&self, row_ptr: usize) -> Option<bool> {
        self.found_flag_offset
            .map(|_| self.build_row_layout.found(row_ptr as *const u8))
    }

    pub fn build_side_match_mask(&self, row_ptr: usize) -> Option<u8> {
        self.found_flag_offset
            .map(|_| self.build_row_layout.match_mask(row_ptr as *const u8))
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
        self.integer_index.store(None);
        self.pair_integer_index.store(None);
        *self.entries.lock().unwrap() = HtEntryTable::default();
        self.reset_grouped_reduction_extrema();
        self.capacity.store(0, Ordering::Relaxed);
        self.bitmask.store(0, Ordering::Relaxed);
        self.chains_longer_than_one.store(false, Ordering::Relaxed);
    }

    pub(crate) fn lookup_i64_group_slots(
        &self,
        vector: &Vector,
        vector_count: usize,
        output_slots: &mut [usize],
    ) -> Result<bool> {
        let index = self.integer_index.load();
        let Some(index) = index.as_ref() else {
            return Ok(false);
        };
        index.lookup_i64_group_slots(vector, vector_count, output_slots)
    }

    fn group_slot_for_build_row(&self, build_row: usize, row_ptr: *const u8) -> Option<usize> {
        self.integer_index.load().as_ref().and_then(|index| {
            index.group_slot_for_build_row(build_row, row_ptr, self.build_row_layout.base())
        })
    }

    pub fn reset_data_collection(&self) {
        self.reset_runtime_state();
        self.build_store.lock().unwrap().reset();
        self.count.store(0, Ordering::Relaxed);
        self.deferred_hashes.store(false, Ordering::Release);
        *self.integer_index_build_stats.lock().unwrap() = IntegerIndexBuildStats::new(
            self.join_type,
            &self.equality_types,
            &self.equality_comparisons,
        );
    }

    pub fn refresh_count_from_data_collection(&self) {
        let count = self.build_store.lock().unwrap().count() as usize;
        self.count.store(count, Ordering::Relaxed);
        // Callers that populate the row store directly bypass vector-domain
        // collection. The optional exact index must decline rather than trust
        // incomplete bounds.
        *self.integer_index_build_stats.lock().unwrap() = IntegerIndexBuildStats::Ineligible;
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

        let defer_hashes = if self.config.build_time_integer_builder.is_some() {
            // Storage bounds and the producer lifecycle already prove this
            // artifact's complete domain. Avoid repeating a min/max pass over
            // every hot key vector merely to rediscover runtime bounds.
            true
        } else {
            let mut integer_stats = self.integer_index_build_stats.lock().unwrap();
            if integer_stats.exact_index_candidate() {
                let key = keys.column(0).ok_or_else(|| {
                    paro_error::internal("hash join build has no first equality-key column")
                })?;
                integer_stats.add_batch(
                    key,
                    keys.size(),
                    &build_sel.as_slice()[..appended_count],
                )?;
            }
            integer_stats.exact_index_candidate()
        };

        if !defer_hashes {
            hashes.resize(appended_count, 0);
            self.key_layout
                .hash_selected_into(keys, build_sel, appended_count, hashes)?;
        }

        let mut store = self.build_store.lock().unwrap();
        let build_time_integer_builder = self.config.build_time_integer_builder.as_ref();
        let build_time_integer_reservation =
            build_time_integer_builder.and_then(|builder| builder.reserve_batch(appended_count));
        let direct_key = build_time_integer_builder
            .map(|_| {
                keys.column(0)
                    .ok_or_else(|| paro_error::internal("hash join build has no direct-index key"))
            })
            .transpose()?;
        let direct_key_view = direct_key
            .as_ref()
            .map(|key| key.try_to_view(keys.size()))
            .transpose()?;
        let appended_count = store.append_key_payload_chunk_with(
            keys,
            payload,
            build_sel,
            appended_count,
            (!defer_hashes).then_some(hashes.as_slice()),
            false,
            |output_idx, source_row_idx, row_ptr| {
                if let (Some(builder), Some(reservation), Some(key)) = (
                    build_time_integer_builder,
                    build_time_integer_reservation.as_ref(),
                    direct_key_view.as_ref(),
                ) {
                    builder.insert_reserved_vector_row_with_link(
                        reservation,
                        output_idx,
                        key,
                        source_row_idx,
                        row_ptr,
                        self.config.build_keys_unique,
                        |row_ptr, previous| {
                            self.build_row_layout
                                .set_next(row_ptr as *mut u8, previous as *const u8);
                        },
                    )?;
                }
                Ok(())
            },
        )?;
        if let (Some(builder), Some(reservation)) =
            (build_time_integer_builder, build_time_integer_reservation)
        {
            builder.publish_batch(reservation);
        }
        if defer_hashes {
            // Publish the deferred state while holding the same store lock that
            // made the zero hash fields visible to spill/finalize readers.
            self.deferred_hashes.store(true, Ordering::Release);
        }
        drop(store);

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
            self.finalize_grouped_reduction_extrema()?;
            self.probe_entries.store(ptr::null_mut(), Ordering::Release);
            self.finalized.store(true, Ordering::Release);
            return Ok(());
        }

        let direct_index = self.try_build_direct_integer_index()?;
        let integer_index = match direct_index {
            Some(index) => Some(index),
            None => self.try_build_ranked_integer_index()?,
        };
        if let Some(index) = integer_index {
            self.publish_integer_index(index)?;
            return Ok(());
        }
        if self.try_build_pair_integer_index()? {
            return Ok(());
        }

        // Exact-index admission is speculative. If bounds or uniqueness make
        // it unsuitable, initialize the canonical hash field once here before
        // constructing generic pointer chains.
        self.materialize_deferred_hashes();

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
        self.finalize_grouped_reduction_extrema()?;
        self.finalized.store(true, Ordering::Release);
        Ok(())
    }

    fn publish_integer_index(&self, built: BuiltIntegerIndex) -> Result<()> {
        self.chains_longer_than_one
            .store(built.has_long_chains, Ordering::Relaxed);
        self.integer_index.store(Some(Arc::new(built.index)));
        self.finalize_grouped_reduction_extrema()?;
        self.finalized.store(true, Ordering::Release);
        Ok(())
    }

    pub(crate) fn publish_build_time_integer_builder(
        &self,
        builder: ConcurrentBuildTimeIntegerIndexBuilder,
    ) -> Result<bool> {
        let Some((index, has_long_chains)) = builder.finish(|row_ptr, previous| {
            self.build_row_layout
                .set_next(row_ptr as *mut u8, previous as *const u8);
        })?
        else {
            return Ok(false);
        };
        self.publish_integer_index(BuiltIntegerIndex {
            index,
            has_long_chains,
        })?;
        Ok(true)
    }

    #[cfg(test)]
    fn has_integer_index(&self) -> bool {
        self.integer_index.load().is_some()
    }

    #[cfg(test)]
    fn has_pair_integer_index(&self) -> bool {
        self.pair_integer_index.load().is_some()
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
        let incoming_stats = *other.integer_index_build_stats.lock().unwrap();
        self.integer_index_build_stats
            .lock()
            .unwrap()
            .merge(incoming_stats);
        if other.deferred_hashes.load(Ordering::Acquire) {
            self.deferred_hashes.store(true, Ordering::Release);
        }
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

        let integer_index = self.integer_index.load();
        if let Some(index) = integer_index.as_ref() {
            Self::probe_exact_integer_index(index, keys, scan_structure, filtered_count)?;
            return Ok(());
        }
        if self.probe_pair_integer_index(keys, scan_structure, filtered_count)? {
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
        )?;

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

    fn probe_exact_integer_index(
        index: &ExactIntegerJoinIndex,
        keys: &Chunk,
        scan_structure: &mut ScanStructure,
        filtered_count: usize,
    ) -> Result<()> {
        let key = keys.column(0).ok_or_else(|| {
            paro_error::internal("integer join index probe is missing its key column")
        })?;
        let prepared_rows = scan_structure.probe_sel.as_slice();
        scan_structure.sel_vector.set_len(keys.size());
        let matched_rows = scan_structure.sel_vector.as_mut_slice();
        let matched_count = index.lookup_vector_rows(
            key.as_ref(),
            keys.size(),
            &prepared_rows[..filtered_count],
            &mut scan_structure.pointers,
            matched_rows,
        )?;
        scan_structure.count = matched_count;
        scan_structure.sel_vector.set_len(matched_count);
        scan_structure.exact_key_matches = true;
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

    pub fn create_full_outer_scan_state_for_block(&self, block_idx: usize) -> FullOuterScanState {
        let row_offset = self.build_store.lock().unwrap().block_row_offset(block_idx);
        FullOuterScanState::for_block(block_idx, row_offset)
    }

    pub fn build_block_count(&self) -> usize {
        self.build_store.lock().unwrap().block_count()
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
            self.build_output_types(),
            result,
        )
    }

    pub fn scan_reduction_cascade(
        &self,
        state: &mut FullOuterScanState,
        required_mask: u8,
        forbidden_mask: u8,
        result: &mut Chunk,
    ) -> Result<usize> {
        if self.found_flag_column_index.is_none() {
            result.set_cardinality(0);
            return Ok(0);
        }
        self.build_store.lock().unwrap().scan_payload_rows_by_mask(
            &mut state.scan_state,
            required_mask,
            forbidden_mask,
            self.build_output_types(),
            result,
        )
    }

    pub(crate) fn scan_grouped_reduction_extrema(
        &self,
        state: &mut FullOuterScanState,
        build_residual_offset: usize,
        channel_match_masks: &[u8],
        required_mask: u8,
        forbidden_mask: u8,
        result: &mut Chunk,
    ) -> Result<usize> {
        let Some(extrema) = self.grouped_reduction_extrema() else {
            return self.scan_reduction_cascade(state, required_mask, forbidden_mask, result);
        };
        let build_idx = self.build_output_count + build_residual_offset;
        let store = self.build_store.lock().unwrap();
        store.scan_payload_rows_where_indexed(
            &mut state.scan_state,
            self.build_output_types(),
            result,
            |build_row, row_ptr| {
                let group_slot = self.group_slot_for_build_row(build_row, row_ptr);
                let build_value =
                    unsafe { self.read_build_payload_fixed::<i64>(row_ptr as usize, build_idx) };
                let mut match_mask = 0u8;
                if let (Some(group_slot), Some(build_value)) = (group_slot, build_value) {
                    for (channel, &channel_mask) in channel_match_masks.iter().enumerate() {
                        if extrema.contains_unequal_i64(group_slot, channel, build_value)? {
                            match_mask |= channel_mask;
                        }
                    }
                }
                Ok(match_mask & required_mask == required_mask && match_mask & forbidden_mask == 0)
            },
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

    pub fn for_block(block_idx: usize, global_row_idx: usize) -> Self {
        Self {
            scan_state: BuildStoreScanState::for_block(block_idx, global_row_idx),
        }
    }
}

#[cfg(test)]
#[path = "table_tests.rs"]
mod tests;
