// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Flat linear-probing grouped aggregate hash table.

use std::mem::size_of;
use std::sync::Arc;

use paro_common::allocator::{Allocator, ArenaAllocator, MemoryTag};
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::memory::{AccountedVec, MemoryAccountingClass, MemoryAccountingContext};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_common::vector::{SelectionVector, Vector, VECTOR_SIZE};
use paro_function::aggregate::{AggregateCombineType, AggregateInputData, AggregateStateInput};

use super::aggregate_kernel::{
    combine_states, destroy_states, filtered_input_vectors_for_aggregate, finalize_states,
    initialize_states, input_vectors_for_aggregate, serialize_aggregate_state_blob,
    update_filtered_states, update_states, with_aggregate_input_data, AggregatePayload,
};
use super::aggregate_object::AggregateObject;
use super::aggregate_state::AggregateStateLayout;
use super::group_hash::hash_group_columns;
use super::tuple_layout::{TupleLayout, TupleScatterSource, VarlenHeap};

const MIN_CAPACITY: usize = 8;
const LOAD_FACTOR_NUMERATOR: usize = 3; // 0.6
const LOAD_FACTOR_DENOMINATOR: usize = 5;
const INLINE_KEY_MAX_BYTES: usize = 8;

/// Soft upper bound for eager hash-table allocation.
///
/// Cardinality estimates are useful for avoiding repeated rehashing, but they
/// are not trustworthy enough to reserve unbounded memory. Constructors use
/// both fields and fall back to the minimum table when the byte budget cannot
/// accommodate the estimated row count.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct HashTableCapacityHint {
    pub expected_rows: usize,
    pub max_fixed_bytes: usize,
}

impl HashTableCapacityHint {
    pub(crate) fn divided_across(self, partitions: usize) -> Self {
        if partitions == 0 {
            return Self::default();
        }
        Self {
            expected_rows: self.expected_rows.div_ceil(partitions),
            max_fixed_bytes: self.max_fixed_bytes / partitions,
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct AggregateHTEntry {
    value: u64,
}

impl AggregateHTEntry {
    const SALT_MASK: u64 = 0xFFFF_0000_0000_0000;
    const ROW_INDEX_MASK: u64 = 0x0000_FFFF_FFFF_FFFF;

    #[inline]
    fn empty() -> Self {
        Self { value: 0 }
    }

    #[inline]
    fn is_occupied(self) -> bool {
        self.value != 0
    }

    #[inline]
    fn salt_bits(self) -> u64 {
        self.value & Self::SALT_MASK
    }

    #[inline]
    fn matches_hash(self, hash: u64) -> bool {
        self.salt_bits() == Self::hash_salt_bits(hash)
    }

    #[inline]
    fn hash_salt_bits(hash: u64) -> u64 {
        hash & Self::SALT_MASK
    }

    fn from_hash_and_row(hash: u64, row_idx: usize) -> Result<Self> {
        let encoded = row_idx.checked_add(1).ok_or_else(|| {
            paro_error::internal(format!(
                "Row index overflow when encoding hash table entry: {row_idx}"
            ))
        })? as u64;
        if encoded > Self::ROW_INDEX_MASK {
            return Err(paro_error::internal(format!(
                "Row index exceeds hash table entry addressable range: row_idx={row_idx}"
            )));
        }
        Ok(Self {
            value: Self::hash_salt_bits(hash) | encoded,
        })
    }

    #[inline]
    fn row_idx(self) -> usize {
        debug_assert!(self.is_occupied());
        ((self.value & Self::ROW_INDEX_MASK) - 1) as usize
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct InlineKey {
    bits: u64,
    null_mask: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct InlineKeyLayout {
    group_types: Vec<LogicalType>,
    byte_offsets: Vec<usize>,
    total_width: usize,
}

impl InlineKeyLayout {
    fn try_new(group_types: &[LogicalType]) -> Option<Self> {
        if group_types.is_empty() {
            return None;
        }
        let mut byte_offsets = Vec::with_capacity(group_types.len());
        let mut width = 0usize;
        for group_type in group_types {
            let component_width = inline_key_component_width(group_type)?;
            let next_width = width.checked_add(component_width)?;
            if next_width > INLINE_KEY_MAX_BYTES {
                return None;
            }
            byte_offsets.push(width);
            width = next_width;
        }
        Some(Self {
            group_types: group_types.to_vec(),
            byte_offsets,
            total_width: width,
        })
    }

    fn encode_row(&self, groups: &Chunk, row_idx: usize) -> Result<InlineKey> {
        if row_idx >= groups.size() {
            return Err(paro_error::internal(format!(
                "Inline key row index out of bounds: row_idx={row_idx}, rows={}",
                groups.size()
            )));
        }
        if groups.column_count() != self.group_types.len() {
            return Err(paro_error::internal(format!(
                "Inline key group width mismatch: expected={}, actual={}",
                self.group_types.len(),
                groups.column_count()
            )));
        }
        if self.group_types.len() > u64::BITS as usize {
            return Err(paro_error::internal(format!(
                "Inline key group count exceeds null-mask capacity: group_count={}",
                self.group_types.len()
            )));
        }

        let mut key_bytes = [0u8; INLINE_KEY_MAX_BYTES];
        let mut null_mask = 0u64;
        for group_idx in 0..self.group_types.len() {
            let column = groups.column(group_idx).ok_or_else(|| {
                paro_error::internal(format!(
                    "Missing group column while encoding inline key: group_idx={group_idx}"
                ))
            })?;
            if column.is_null(row_idx) {
                null_mask |= 1u64 << group_idx;
                continue;
            }
            let offset = self.byte_offsets[group_idx];
            write_inline_component_bytes(
                &mut key_bytes,
                offset,
                column.as_ref(),
                row_idx,
                &self.group_types[group_idx],
            )?;
        }
        Ok(InlineKey {
            bits: u64::from_le_bytes(key_bytes),
            null_mask,
        })
    }

    /// Encode an inline key directly from an aggregate tuple row.
    ///
    /// # Safety
    /// `row_ptr` must point to an initialized row encoded by `layout`.
    unsafe fn encode_serialized_row(
        &self,
        layout: &TupleLayout,
        row_ptr: *const u8,
    ) -> Result<InlineKey> {
        if self.group_types != layout.group_types {
            return Err(paro_error::internal(format!(
                "Inline serialized key layout mismatch: expected={:?}, actual={:?}",
                self.group_types, layout.group_types
            )));
        }

        let mut key_bytes = [0u8; INLINE_KEY_MAX_BYTES];
        let mut null_mask = 0u64;
        for group_idx in 0..self.group_types.len() {
            if !unsafe { layout.serialized_group_is_valid(row_ptr, group_idx) } {
                null_mask |= 1u64 << group_idx;
                continue;
            }
            let source = unsafe { row_ptr.add(layout.group_offsets[group_idx]) };
            write_serialized_inline_component(
                &mut key_bytes,
                self.byte_offsets[group_idx],
                source,
                &self.group_types[group_idx],
            )?;
        }
        Ok(InlineKey {
            bits: u64::from_le_bytes(key_bytes),
            null_mask,
        })
    }
}

/// Scan cursor for [`GroupedAggregateHashTable::scan`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct HTScanPosition {
    pub offset: usize,
}

/// Flat grouped aggregate hash table.
#[derive(Debug)]
pub struct GroupedAggregateHashTable {
    entries: AccountedVec<AggregateHTEntry>,
    // Only narrow fixed-width groups allocate this sidecar. Keeping optional
    // inline keys out of the primary probe array makes ordinary entries one
    // cache-friendly u64 instead of imposing the inline fast path's metadata
    // on every aggregate table.
    inline_keys: Option<AccountedVec<InlineKey>>,
    // Keep row storage 8-byte aligned so aggregate states can be safely cast to typed pointers.
    data: AccountedVec<u64>,
    memory: MemoryAccountingContext,
    layout: TupleLayout,
    state_layout: AggregateStateLayout,
    aggregate_objects: Vec<AggregateObject>,
    aggregate_inputs: Vec<Vec<usize>>,
    aggregate_return_types: Vec<LogicalType>,
    varlen_heap: VarlenHeap,
    aggregate_allocator: ArenaAllocator,
    inline_key_layout: Option<InlineKeyLayout>,
    count: usize,
    capacity: usize,
    bitmask: usize,
}

impl GroupedAggregateHashTable {
    pub fn new(
        group_types: Vec<LogicalType>,
        aggregate_objects: Vec<AggregateObject>,
        aggregate_inputs: Vec<Vec<usize>>,
        allocator: Arc<dyn Allocator>,
    ) -> Result<Self> {
        Self::new_with_memory(
            group_types,
            aggregate_objects,
            aggregate_inputs,
            allocator,
            MemoryAccountingContext::detached(
                MemoryTag::HashTable,
                MemoryAccountingClass::Revocable,
            ),
        )
    }

    pub fn new_with_memory(
        group_types: Vec<LogicalType>,
        aggregate_objects: Vec<AggregateObject>,
        aggregate_inputs: Vec<Vec<usize>>,
        allocator: Arc<dyn Allocator>,
        memory: MemoryAccountingContext,
    ) -> Result<Self> {
        Self::with_capacity(
            group_types,
            aggregate_objects,
            aggregate_inputs,
            MIN_CAPACITY,
            allocator,
            memory,
        )
    }

    pub(crate) fn new_with_memory_capacity_hint(
        group_types: Vec<LogicalType>,
        aggregate_objects: Vec<AggregateObject>,
        aggregate_inputs: Vec<Vec<usize>>,
        allocator: Arc<dyn Allocator>,
        memory: MemoryAccountingContext,
        capacity_hint: HashTableCapacityHint,
    ) -> Result<Self> {
        let initial_capacity =
            initial_capacity_for_hint(&group_types, &aggregate_objects, capacity_hint)?;
        Self::with_capacity(
            group_types,
            aggregate_objects,
            aggregate_inputs,
            initial_capacity,
            allocator,
            memory,
        )
    }

    pub fn inline_key_width_for_types(group_types: &[LogicalType]) -> Option<usize> {
        InlineKeyLayout::try_new(group_types).map(|layout| layout.total_width)
    }

    pub fn inline_key_width(&self) -> Option<usize> {
        self.inline_key_layout
            .as_ref()
            .map(|layout| layout.total_width)
    }

    pub fn scan_output_types(&self) -> Vec<LogicalType> {
        let mut output_types = self.layout.group_types.clone();
        output_types.extend(self.aggregate_return_types.clone());
        output_types
    }

    pub(crate) fn group_types(&self) -> &[LogicalType] {
        &self.layout.group_types
    }

    pub fn aggregate_count(&self) -> usize {
        self.aggregate_return_types.len()
    }

    pub fn with_capacity(
        group_types: Vec<LogicalType>,
        aggregate_objects: Vec<AggregateObject>,
        aggregate_inputs: Vec<Vec<usize>>,
        initial_capacity: usize,
        allocator: Arc<dyn Allocator>,
        memory: MemoryAccountingContext,
    ) -> Result<Self> {
        validate_aggregate_inputs(&aggregate_objects, &aggregate_inputs)?;
        let layout = TupleLayout::build(&group_types, &aggregate_objects)?;
        let state_layout = AggregateStateLayout::new(&aggregate_objects)?;
        let aggregate_return_types = aggregate_objects
            .iter()
            .map(|object| object.return_type.clone())
            .collect::<Vec<_>>();
        let inline_key_layout = InlineKeyLayout::try_new(&group_types);

        let capacity = normalize_capacity(initial_capacity)?;
        let bitmask = capacity - 1;
        let mut entries = accounted_vec_for_context(
            &memory.with_class(MemoryAccountingClass::Metadata),
            MemoryTag::HashTable,
            MemoryAccountingClass::Metadata,
        )?;
        entries.try_resize_with(capacity, AggregateHTEntry::empty)?;
        let inline_keys = if inline_key_layout.is_some() {
            let mut keys = accounted_vec_for_context(
                &memory.with_class(MemoryAccountingClass::Metadata),
                MemoryTag::HashTable,
                MemoryAccountingClass::Metadata,
            )?;
            keys.try_resize_with(capacity, InlineKey::default)?;
            Some(keys)
        } else {
            None
        };
        let reserve_rows = resize_threshold(capacity).max(1);
        let reserve_bytes = layout.row_width.checked_mul(reserve_rows).ok_or_else(|| {
            paro_error::internal(format!(
                "Hash table row storage reserve overflow: row_width={} reserve_rows={reserve_rows}",
                layout.row_width
            ))
        })?;
        let reserve_words = bytes_to_words(reserve_bytes)?;
        let mut data = accounted_vec_for_context(
            &memory,
            MemoryTag::HashTable,
            MemoryAccountingClass::Revocable,
        )?;
        data.try_reserve(reserve_words)?;

        Ok(Self {
            entries,
            inline_keys,
            data,
            memory: memory.clone(),
            layout,
            state_layout,
            aggregate_objects,
            aggregate_inputs,
            aggregate_return_types,
            varlen_heap: VarlenHeap::new_with_memory(
                memory.with_class(MemoryAccountingClass::Revocable),
            ),
            aggregate_allocator: ArenaAllocator::new(allocator),
            inline_key_layout,
            count: 0,
            capacity,
            bitmask,
        })
    }

    pub fn count(&self) -> usize {
        self.count
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Reserve lookup and tuple storage for a known upper bound of future
    /// insertions. Bulk combiners use this once before merging several tables
    /// so existing rows are not repeatedly rehashed between fragments.
    pub(crate) fn reserve_for_insertions(&mut self, incoming_rows: usize) -> Result<()> {
        self.ensure_lookup_storage_available()?;
        self.ensure_capacity_for(incoming_rows)?;
        self.ensure_row_storage_capacity(incoming_rows)
    }

    pub fn allocator(&self) -> Arc<dyn Allocator> {
        self.aggregate_allocator.get_allocator().clone()
    }

    /// Hash grouped keys using Paro vector hash implementation.
    pub fn hash_groups(&self, groups: &Chunk) -> Result<Vector> {
        self.validate_group_chunk(groups)?;
        hash_group_columns(groups)
    }

    /// Probe and insert grouped keys, returning state addresses for each input row.
    pub fn find_or_create_groups(
        &mut self,
        groups: &Chunk,
        hashes: &Vector,
        addresses: &mut Vector,
        new_groups: &mut SelectionVector,
    ) -> Result<usize> {
        self.validate_group_chunk(groups)?;
        validate_hashes(hashes, groups.size())?;
        let hash_format = hashes.try_decode_ref(groups.size())?;
        let hash_data = hash_format.get_data::<u64>();
        self.find_or_create_groups_with(
            groups,
            groups.size(),
            |input_idx| input_idx,
            |_, source_row| {
                let hash_idx = hash_format.physical_index(source_row);
                if !hash_format.validity().is_valid(hash_idx) {
                    return Err(paro_error::internal(format!(
                        "Group hash contains NULL at row {source_row}"
                    )));
                }
                Ok(unsafe { *hash_data.add(hash_idx) })
            },
            addresses,
            new_groups,
        )
    }

    /// Probe a subset of `groups` without wrapping every input column in a
    /// dictionary vector.
    ///
    /// `source_rows` maps each contiguous hash to its row in `groups`. State
    /// addresses and new-group indices are written at those original row
    /// ordinals, allowing radix routing to pass its reusable row permutation
    /// straight through to the flat partitions.
    pub(crate) fn find_or_create_groups_selected(
        &mut self,
        groups: &Chunk,
        source_rows: &[u32],
        hashes: &[u64],
        addresses: &mut Vector,
        new_groups: &mut SelectionVector,
    ) -> Result<usize> {
        self.validate_group_chunk(groups)?;
        if source_rows.len() != hashes.len() {
            return Err(paro_error::internal(format!(
                "Selected group/hash size mismatch: rows={}, hashes={}",
                source_rows.len(),
                hashes.len()
            )));
        }
        for (selection_idx, &source_row) in source_rows.iter().enumerate() {
            if source_row as usize >= groups.size() {
                return Err(paro_error::internal(format!(
                    "Selected group row out of bounds: selection[{selection_idx}]={source_row}, groups={}",
                    groups.size()
                )));
            }
        }
        self.find_or_create_groups_with(
            groups,
            source_rows.len(),
            |input_idx| source_rows[input_idx] as usize,
            |input_idx, _| Ok(hashes[input_idx]),
            addresses,
            new_groups,
        )
    }

    fn find_or_create_groups_with(
        &mut self,
        groups: &Chunk,
        input_row_count: usize,
        source_row_at: impl Fn(usize) -> usize,
        hash_at: impl Fn(usize, usize) -> Result<u64>,
        addresses: &mut Vector,
        new_groups: &mut SelectionVector,
    ) -> Result<usize> {
        validate_addresses_vector(addresses, groups.size())?;
        addresses.try_set_count(groups.size())?;
        if input_row_count == 0 {
            new_groups.set_len(0);
            return Ok(0);
        }
        self.ensure_lookup_storage_available()?;
        let scatter_source = self.layout.prepare_scatter(groups)?;

        self.ensure_capacity_for(input_row_count)?;
        self.ensure_row_storage_capacity(input_row_count)?;

        let address_data = unsafe { addresses.flat_data_mut::<*mut u8>() };
        let mut new_state_ptrs = Vec::new();
        if new_groups.capacity() < input_row_count {
            *new_groups =
                SelectionVector::try_with_capacity(input_row_count, groups.allocator().clone())?;
        }
        new_groups.set_len(input_row_count);
        let new_group_data = new_groups.as_mut_slice().as_mut_ptr();
        let mut new_group_count = 0usize;
        let inline_key_layout = self.inline_key_layout.clone();
        if let Some(inline_layout) = inline_key_layout {
            let inline_key_data = self.inline_key_storage_mut_ptr()?;
            for input_idx in 0..input_row_count {
                let row_idx = source_row_at(input_idx);
                let hash = hash_at(input_idx, row_idx)?;
                let inline_key = inline_layout.encode_row(groups, row_idx)?;
                let mut slot = self.slot_for_hash(hash);
                loop {
                    let entry = self.entries[slot];
                    if !entry.is_occupied() {
                        let new_row_idx = self.append_group_row(&scatter_source, row_idx, hash)?;
                        self.entries[slot] =
                            AggregateHTEntry::from_hash_and_row(hash, new_row_idx)?;
                        // SAFETY: lookup storage was validated and reserved before
                        // taking the pointer, and no operation in this loop resizes it.
                        unsafe {
                            *inline_key_data.add(slot) = inline_key;
                        }
                        self.count += 1;
                        let state_ptr = self.state_ptr(new_row_idx);
                        unsafe {
                            *address_data.add(row_idx) = state_ptr;
                        }
                        // SAFETY: `new_groups` was sized to the input cardinality and
                        // `new_group_count` advances at most once per input row.
                        unsafe {
                            *new_group_data.add(new_group_count) = row_idx as u32;
                        }
                        new_group_count += 1;
                        if !self.aggregate_objects.is_empty() {
                            new_state_ptrs.push(state_ptr);
                        }
                        break;
                    }

                    // SAFETY: `slot` is masked by the lookup capacity, and the
                    // sidecar has exactly the same length as the primary entries.
                    let stored_inline_key = unsafe { *inline_key_data.add(slot) };
                    if entry.matches_hash(hash) && stored_inline_key == inline_key {
                        unsafe {
                            *address_data.add(row_idx) = self.state_ptr(entry.row_idx());
                        }
                        break;
                    }

                    slot = (slot + 1) & self.bitmask;
                }
            }
        } else {
            for input_idx in 0..input_row_count {
                let row_idx = source_row_at(input_idx);
                let hash = hash_at(input_idx, row_idx)?;
                let mut slot = self.slot_for_hash(hash);
                loop {
                    let entry = self.entries[slot];
                    if !entry.is_occupied() {
                        let new_row_idx = self.append_group_row(&scatter_source, row_idx, hash)?;
                        self.entries[slot] =
                            AggregateHTEntry::from_hash_and_row(hash, new_row_idx)?;
                        self.count += 1;
                        let state_ptr = self.state_ptr(new_row_idx);
                        unsafe {
                            *address_data.add(row_idx) = state_ptr;
                        }
                        // SAFETY: `new_groups` was sized to the input cardinality and
                        // `new_group_count` advances at most once per input row.
                        unsafe {
                            *new_group_data.add(new_group_count) = row_idx as u32;
                        }
                        new_group_count += 1;
                        if !self.aggregate_objects.is_empty() {
                            new_state_ptrs.push(state_ptr);
                        }
                        break;
                    }

                    if entry.matches_hash(hash)
                        && self.layout.compare_prepared_groups(
                            self.row_ptr(entry.row_idx()),
                            &scatter_source,
                            row_idx,
                            &self.varlen_heap,
                        )?
                    {
                        unsafe {
                            *address_data.add(row_idx) = self.state_ptr(entry.row_idx());
                        }
                        break;
                    }

                    slot = (slot + 1) & self.bitmask;
                }
            }
        }

        if !new_state_ptrs.is_empty() {
            let new_addresses = pointer_vector_from_slice(&new_state_ptrs, self.allocator())?;
            initialize_states(
                &self.state_layout,
                &self.aggregate_objects,
                &new_addresses,
                new_state_ptrs.len(),
            )?;
        }

        new_groups.set_len(new_group_count);
        Ok(new_groups.len())
    }

    /// Update aggregate states for a batch of input payload rows.
    pub fn update_aggregates(
        &mut self,
        payload: &Chunk,
        addresses: &Vector,
        filter: Option<&SelectionVector>,
    ) -> Result<()> {
        if payload.size() == 0 || self.aggregate_objects.is_empty() {
            return Ok(());
        }
        if addresses.len() < payload.size() {
            return Err(paro_error::internal(format!(
                "Address vector too small for aggregate update: addresses={} payload_rows={}",
                addresses.len(),
                payload.size()
            )));
        }
        if let Some(selection) = filter {
            validate_filter(selection, payload.size())?;
        }
        #[cfg(debug_assertions)]
        self.validate_state_addresses(addresses, payload.size())?;

        let payload_desc = AggregatePayload {
            chunk: payload,
            aggregate_inputs: &self.aggregate_inputs,
        };
        let mut input_data = AggregateInputData::new(
            None,
            &mut self.aggregate_allocator,
            AggregateCombineType::PreserveInput,
        );
        if let Some(selection) = filter {
            update_filtered_states(
                &self.aggregate_objects,
                &mut input_data,
                &payload_desc,
                addresses,
                selection,
                selection.len(),
            )?;
        } else {
            update_states(
                &self.aggregate_objects,
                &mut input_data,
                &payload_desc,
                addresses,
                payload.size(),
            )?;
        }
        Ok(())
    }

    pub fn update_aggregates_per_filter(
        &mut self,
        payload: &Chunk,
        addresses: &Vector,
        filters: &[Option<SelectionVector>],
    ) -> Result<()> {
        if payload.size() == 0 || self.aggregate_objects.is_empty() {
            return Ok(());
        }
        #[cfg(debug_assertions)]
        self.validate_state_addresses(addresses, payload.size())?;

        for (agg_idx, (object, filter)) in self
            .aggregate_objects
            .iter()
            .zip(filters.iter())
            .enumerate()
        {
            if let Some(selection) = filter {
                if selection.is_empty() {
                    continue;
                }
                let payload_desc = AggregatePayload {
                    chunk: payload,
                    aggregate_inputs: &self.aggregate_inputs[agg_idx..agg_idx + 1],
                };
                let inputs = filtered_input_vectors_for_aggregate(
                    &payload_desc,
                    0,
                    selection,
                    selection.len(),
                )?;
                let input_refs: Vec<&Vector> = inputs.iter().collect();
                let states = AggregateStateInput::try_new(
                    addresses,
                    self.state_layout.state_offset(agg_idx),
                    Some(selection),
                    selection.len(),
                )?;
                let mut input_data = AggregateInputData::new(
                    object.bind_info.as_deref(),
                    &mut self.aggregate_allocator,
                    AggregateCombineType::PreserveInput,
                );
                with_aggregate_input_data(object, &mut input_data, |aggr_input| unsafe {
                    (object.function.update)(&input_refs, &aggr_input, &states, selection.len());
                });
            } else {
                let payload_desc = AggregatePayload {
                    chunk: payload,
                    aggregate_inputs: &self.aggregate_inputs[agg_idx..agg_idx + 1],
                };
                let inputs = input_vectors_for_aggregate(&payload_desc, 0)?;
                let states = AggregateStateInput::try_new(
                    addresses,
                    self.state_layout.state_offset(agg_idx),
                    None,
                    payload.size(),
                )?;
                let mut input_data = AggregateInputData::new(
                    object.bind_info.as_deref(),
                    &mut self.aggregate_allocator,
                    AggregateCombineType::PreserveInput,
                );
                with_aggregate_input_data(object, &mut input_data, |aggr_input| unsafe {
                    (object.function.update)(&inputs, &aggr_input, &states, payload.size());
                });
            }
        }
        Ok(())
    }

    /// Scan grouped keys + finalized aggregate values into `result`.
    ///
    /// Returns `true` if output rows were produced, `false` when scan is complete.
    pub fn scan(&mut self, position: &mut HTScanPosition, result: &mut Chunk) -> Result<bool> {
        let group_count = self.layout.group_count();
        let aggregate_count = self.layout.aggregate_count();
        debug_assert_eq!(aggregate_count, self.aggregate_objects.len());
        let required_columns = group_count + aggregate_count;
        if result.column_count() < required_columns {
            return Err(paro_error::internal(format!(
                "Result chunk has insufficient columns for hash table scan: required={required_columns}, actual={}",
                result.column_count()
            )));
        }
        if position.offset >= self.count {
            result.try_set_cardinality(0)?;
            return Ok(false);
        }

        let batch_size = (self.count - position.offset).min(result.capacity());
        result.try_set_cardinality(batch_size)?;
        let row_base = self.row_ptr(position.offset);

        for group_idx in 0..group_count {
            let result_vector = result.column_mut(group_idx).ok_or_else(|| {
                paro_error::internal(format!(
                    "Missing group output column {group_idx} while scanning aggregate hash table"
                ))
            })?;
            // SAFETY: row_base starts at position.offset, batch_size is bounded
            // by self.count, and every row uses this table's layout stride.
            unsafe {
                self.layout.gather_group_column(
                    row_base,
                    self.layout.row_width,
                    batch_size,
                    group_idx,
                    &self.varlen_heap,
                    result_vector,
                )?;
            }
        }

        if aggregate_count > 0 {
            let aggregate_chunk = self.finalize_aggregate_range(
                position.offset,
                batch_size,
                result.allocator().clone(),
            )?;
            for agg_idx in 0..aggregate_count {
                let source = aggregate_chunk.column(agg_idx).ok_or_else(|| {
                    paro_error::internal(format!(
                        "Missing finalized aggregate column {agg_idx} in temporary scan chunk"
                    ))
                })?;
                let target = result.column_mut(group_count + agg_idx).ok_or_else(|| {
                    paro_error::internal(format!(
                        "Missing aggregate output column {} while scanning hash table",
                        group_count + agg_idx
                    ))
                })?;
                target.try_copy_range(0, source.as_ref(), 0, batch_size)?;
            }
        }

        position.offset += batch_size;
        Ok(true)
    }

    /// Scan groups whose finalized aggregate values satisfy a filter.
    ///
    /// The filter is evaluated before group keys are deserialized, which makes
    /// selective HAVING clauses cheap even for high-cardinality group sets.
    pub fn scan_with_aggregate_filter(
        &mut self,
        position: &mut HTScanPosition,
        result: &mut Chunk,
        selection: &mut SelectionVector,
        mut select: impl FnMut(&Chunk, usize, &mut SelectionVector) -> Result<usize>,
    ) -> Result<bool> {
        let group_count = self.layout.group_count();
        let aggregate_count = self.layout.aggregate_count();
        if result.column_count() < group_count + aggregate_count {
            return Err(paro_error::internal(
                "Result chunk has insufficient columns for filtered aggregate scan",
            ));
        }

        while position.offset < self.count {
            let start = position.offset;
            let batch_size = (self.count - start).min(result.capacity());
            let aggregate_chunk =
                self.finalize_aggregate_range(start, batch_size, result.allocator().clone())?;
            let selected_count = select(&aggregate_chunk, batch_size, selection)?;
            position.offset += batch_size;
            if selected_count == 0 {
                continue;
            }
            result.try_set_cardinality(selected_count)?;

            for group_idx in 0..group_count {
                let result_vector = result.column_mut(group_idx).ok_or_else(|| {
                    paro_error::internal(format!(
                        "Missing group output column {group_idx} in filtered aggregate scan"
                    ))
                })?;
                result_vector.try_set_count(selected_count)?;
                for output_row in 0..selected_count {
                    let source_row = selection.get(output_row);
                    let value = self.layout.deserialize_group_value(
                        self.row_ptr(start + source_row),
                        group_idx,
                        &self.varlen_heap,
                    )?;
                    result_vector.set_value(output_row, &value);
                }
            }

            let vector_selection = paro_common::vector::VectorSelection::from(&*selection);
            for agg_idx in 0..aggregate_count {
                let source = aggregate_chunk.column(agg_idx).ok_or_else(|| {
                    paro_error::internal(format!(
                        "Missing finalized aggregate column {agg_idx} in filtered scan"
                    ))
                })?;
                let target = result.column_mut(group_count + agg_idx).ok_or_else(|| {
                    paro_error::internal(format!(
                        "Missing aggregate output column {} in filtered scan",
                        group_count + agg_idx
                    ))
                })?;
                target.try_copy_selection(0, source.as_ref(), &vector_selection, selected_count)?;
            }
            return Ok(true);
        }

        result.try_set_cardinality(0)?;
        Ok(false)
    }

    fn finalize_aggregate_range(
        &mut self,
        start: usize,
        count: usize,
        allocator: Arc<dyn Allocator>,
    ) -> Result<Chunk> {
        let mut state_addresses = Vector::try_new(LogicalType::BigInt, count, allocator.clone())?;
        state_addresses.try_set_count(count)?;
        // SAFETY: the BIGINT vector owns `count` pointer-width slots on supported
        // 64-bit targets, and every state pointer addresses an initialized table row.
        unsafe {
            let address_data = state_addresses.flat_data_mut::<*mut u8>();
            for row in 0..count {
                *address_data.add(row) = self.state_ptr(start + row);
            }
        }

        let mut aggregate_chunk =
            Chunk::try_initialize(&self.aggregate_return_types, count, allocator)?;
        let mut input_data = AggregateInputData::new(
            None,
            &mut self.aggregate_allocator,
            AggregateCombineType::PreserveInput,
        );
        finalize_states(
            &self.aggregate_objects,
            &mut input_data,
            &state_addresses,
            &mut aggregate_chunk,
            count,
        )?;
        Ok(aggregate_chunk)
    }

    /// Scan group keys plus the raw aggregate state block.
    ///
    /// This is only valid for aggregate functions whose state can be byte-copied
    /// and later fed to `combine`. Callers are responsible for enforcing that
    /// ABI contract before using the raw state blob.
    pub fn scan_state_rows(
        &self,
        position: &mut HTScanPosition,
        result: &mut Chunk,
    ) -> Result<bool> {
        let group_count = self.layout.group_count();
        let required_columns = 1 + group_count + 1;
        if result.column_count() < required_columns {
            return Err(paro_error::internal(format!(
                "Result chunk has insufficient columns for aggregate state scan: required={required_columns}, actual={}",
                result.column_count()
            )));
        }
        if position.offset >= self.count {
            result.try_set_cardinality(0)?;
            return Ok(false);
        }

        let batch_size = (self.count - position.offset).min(result.capacity());
        result.try_set_cardinality(batch_size)?;

        for row in 0..batch_size {
            let source_ptr = self.row_ptr(position.offset + row);
            result
                .column_mut(0)
                .ok_or_else(|| paro_error::internal("Missing aggregate state hash column"))?
                .set_value(row, &Value::UBigInt(self.layout.load_hash(source_ptr)));
            for group_idx in 0..group_count {
                let value = self.layout.deserialize_group_value(
                    source_ptr,
                    group_idx,
                    &self.varlen_heap,
                )?;
                result
                    .column_mut(1 + group_idx)
                    .ok_or_else(|| {
                        paro_error::internal(format!(
                            "Missing aggregate state group column {group_idx}"
                        ))
                    })?
                    .set_value(row, &value);
            }
            let state_ptr = self.state_ptr(position.offset + row);
            let state_bytes =
                unsafe { std::slice::from_raw_parts(state_ptr, self.state_layout.total_size()) };
            result
                .column_mut(1 + group_count)
                .ok_or_else(|| paro_error::internal("Missing aggregate state blob column"))?
                .set_value(row, &Value::Blob(state_bytes.to_vec()));
        }

        position.offset += batch_size;
        Ok(true)
    }

    /// Scan group keys plus a serialized aggregate state blob.
    ///
    /// This path is used for aggregate functions with explicit state
    /// serialize/deserialize hooks. The output schema matches
    /// [`scan_state_rows`], but the final blob is an ABI-framed serialized
    /// state rather than a raw byte copy.
    pub fn scan_serialized_state_rows(
        &self,
        position: &mut HTScanPosition,
        result: &mut Chunk,
    ) -> Result<bool> {
        let group_count = self.layout.group_count();
        let required_columns = 1 + group_count + 1;
        if result.column_count() < required_columns {
            return Err(paro_error::internal(format!(
                "Result chunk has insufficient columns for aggregate serialized state scan: required={required_columns}, actual={}",
                result.column_count()
            )));
        }
        if position.offset >= self.count {
            result.try_set_cardinality(0)?;
            return Ok(false);
        }

        let batch_size = (self.count - position.offset).min(result.capacity());
        result.try_set_cardinality(batch_size)?;
        let mut serialize_allocator = ArenaAllocator::new(self.allocator());
        let mut input_data = AggregateInputData::new(
            None,
            &mut serialize_allocator,
            AggregateCombineType::PreserveInput,
        );

        for row in 0..batch_size {
            let source_ptr = self.row_ptr(position.offset + row);
            result
                .column_mut(0)
                .ok_or_else(|| {
                    paro_error::internal("Missing aggregate serialized state hash column")
                })?
                .set_value(row, &Value::UBigInt(self.layout.load_hash(source_ptr)));
            for group_idx in 0..group_count {
                let value = self.layout.deserialize_group_value(
                    source_ptr,
                    group_idx,
                    &self.varlen_heap,
                )?;
                result
                    .column_mut(1 + group_idx)
                    .ok_or_else(|| {
                        paro_error::internal(format!(
                            "Missing aggregate serialized state group column {group_idx}"
                        ))
                    })?
                    .set_value(row, &value);
            }
            let state_blob = serialize_aggregate_state_blob(
                &self.aggregate_objects,
                &self.state_layout,
                self.state_ptr(position.offset + row),
                &mut input_data,
            )?;
            result
                .column_mut(1 + group_count)
                .ok_or_else(|| {
                    paro_error::internal("Missing aggregate serialized state blob column")
                })?
                .set_value(row, &Value::Blob(state_blob));
        }

        position.offset += batch_size;
        Ok(true)
    }

    pub fn destroy(&mut self) -> Result<()> {
        if self.count > 0 {
            let mut addresses = Vector::try_new(LogicalType::BigInt, self.count, self.allocator())?;
            addresses.try_set_count(self.count)?;
            unsafe {
                let address_data = addresses.flat_data_mut::<*mut u8>();
                for row_idx in 0..self.count {
                    *address_data.add(row_idx) = self.state_ptr(row_idx);
                }
            }
            let mut input_data = AggregateInputData::new(
                None,
                &mut self.aggregate_allocator,
                AggregateCombineType::PreserveInput,
            );
            destroy_states(
                &self.aggregate_objects,
                &mut input_data,
                &addresses,
                self.count,
            )?;
        }

        self.data.clear();
        self.data.shrink_to_fit_and_refund();
        self.entries.clear();
        self.entries.shrink_to_fit_and_refund();
        if let Some(inline_keys) = &mut self.inline_keys {
            inline_keys.clear();
            inline_keys.shrink_to_fit_and_refund();
        }
        self.varlen_heap.reset();
        self.varlen_heap.shrink_to_fit_and_refund();
        self.varlen_heap.release_dedup_cache();
        self.aggregate_allocator.reset();
        self.count = 0;
        self.capacity = 0;
        self.bitmask = 0;
        Ok(())
    }

    pub fn memory_usage(&self) -> usize {
        self.external_accounted_memory_usage() + self.aggregate_allocator.allocation_size()
    }

    fn lookup_memory_usage(&self) -> usize {
        self.entries.capacity() * size_of::<AggregateHTEntry>()
            + self
                .inline_keys
                .as_ref()
                .map_or(0, |keys| keys.capacity() * size_of::<InlineKey>())
    }

    pub fn external_accounted_memory_usage(&self) -> usize {
        self.lookup_memory_usage()
            + self.data.capacity() * size_of::<u64>()
            + self.varlen_heap.capacity()
            + self.varlen_heap.dedup_cache_memory_usage()
    }

    pub fn reclaimable_finalized_memory(&self) -> usize {
        self.lookup_memory_usage()
            + self.data.capacity().saturating_sub(self.data.len()) * size_of::<u64>()
            + self.varlen_heap.spare_capacity()
            + self.varlen_heap.dedup_cache_memory_usage()
    }

    pub fn reclaimable_build_memory(&self) -> usize {
        self.data.capacity().saturating_sub(self.data.len()) * size_of::<u64>()
            + self.varlen_heap.spare_capacity()
            + self.varlen_heap.dedup_cache_memory_usage()
    }

    pub fn reclaim_build_memory(&mut self, target_bytes: usize) -> usize {
        if target_bytes == 0 {
            return 0;
        }
        let before = self.external_accounted_memory_usage();
        if self.data.capacity() > self.data.len() {
            self.data.shrink_to_fit_and_refund();
        }
        if self.varlen_heap.capacity() > self.varlen_heap.len() {
            self.varlen_heap.shrink_to_fit_and_refund();
        }
        self.varlen_heap.release_dedup_cache();
        before.saturating_sub(self.external_accounted_memory_usage())
    }

    pub fn reclaim_finalized_memory(&mut self, target_bytes: usize) -> usize {
        if target_bytes == 0 {
            return 0;
        }
        let before = self.external_accounted_memory_usage();
        self.release_finalized_lookup_storage();
        if self.data.capacity() > self.data.len() {
            self.data.shrink_to_fit_and_refund();
        }
        if self.varlen_heap.capacity() > self.varlen_heap.len() {
            self.varlen_heap.shrink_to_fit_and_refund();
        }
        self.varlen_heap.release_dedup_cache();
        before.saturating_sub(self.external_accounted_memory_usage())
    }

    pub fn resize(&mut self, new_capacity: usize) -> Result<()> {
        let new_capacity = normalize_capacity(new_capacity)?;
        if new_capacity <= self.capacity {
            return Ok(());
        }

        let mut new_entries = accounted_vec_for_context(
            &self.memory.with_class(MemoryAccountingClass::Metadata),
            MemoryTag::HashTable,
            MemoryAccountingClass::Metadata,
        )?;
        new_entries.try_resize_with(new_capacity, AggregateHTEntry::empty)?;
        let mut new_inline_keys = if self.inline_key_layout.is_some() {
            let mut keys = accounted_vec_for_context(
                &self.memory.with_class(MemoryAccountingClass::Metadata),
                MemoryTag::HashTable,
                MemoryAccountingClass::Metadata,
            )?;
            keys.try_resize_with(new_capacity, InlineKey::default)?;
            Some(keys)
        } else {
            None
        };
        if self.inline_key_layout.is_some() != self.inline_keys.is_some() {
            return Err(paro_error::internal(
                "Aggregate inline-key layout and storage disagree",
            ));
        }
        let new_bitmask = new_capacity - 1;
        for (old_slot, old_entry) in self.entries.iter().copied().enumerate() {
            if !old_entry.is_occupied() {
                continue;
            }
            let row_idx = old_entry.row_idx();
            let hash = self.layout.load_hash(self.row_ptr(row_idx));
            let mut slot = (hash as usize) & new_bitmask;
            loop {
                if !new_entries[slot].is_occupied() {
                    new_entries[slot] = AggregateHTEntry::from_hash_and_row(hash, row_idx)?;
                    if let (Some(old_keys), Some(new_keys)) =
                        (self.inline_keys.as_ref(), new_inline_keys.as_mut())
                    {
                        new_keys[slot] = old_keys[old_slot];
                    }
                    break;
                }
                slot = (slot + 1) & new_bitmask;
            }
        }
        self.entries = new_entries;
        self.inline_keys = new_inline_keys;
        self.capacity = new_capacity;
        self.bitmask = new_bitmask;
        Ok(())
    }

    fn validate_group_chunk(&self, groups: &Chunk) -> Result<()> {
        if groups.column_count() != self.layout.group_count() {
            return Err(paro_error::internal(format!(
                "Group key column count mismatch: expected={}, actual={}",
                self.layout.group_count(),
                groups.column_count()
            )));
        }
        for group_idx in 0..self.layout.group_count() {
            let group_type = groups
                .column(group_idx)
                .ok_or_else(|| {
                    paro_error::internal(format!("Missing group column at index {group_idx}"))
                })?
                .logical_type()
                .clone();
            if group_type != self.layout.group_types[group_idx] {
                return Err(paro_error::internal(format!(
                    "Group key type mismatch at index {group_idx}: expected={:?}, actual={:?}",
                    self.layout.group_types[group_idx], group_type
                )));
            }
        }
        Ok(())
    }

    fn ensure_compatible(&self, other: &Self) -> Result<()> {
        if self.layout.group_types != other.layout.group_types {
            return Err(paro_error::internal(format!(
                "Cannot combine hash tables with different group types: left={:?}, right={:?}",
                self.layout.group_types, other.layout.group_types
            )));
        }
        if self.inline_key_layout != other.inline_key_layout {
            return Err(paro_error::internal(format!(
                "Cannot combine hash tables with different key modes: left_inline_width={:?}, right_inline_width={:?}",
                self.inline_key_width(),
                other.inline_key_width()
            )));
        }
        if self.aggregate_objects.len() != other.aggregate_objects.len() {
            return Err(paro_error::internal(format!(
                "Cannot combine hash tables with different aggregate counts: left={}, right={}",
                self.aggregate_objects.len(),
                other.aggregate_objects.len()
            )));
        }
        for (idx, (left, right)) in self
            .aggregate_objects
            .iter()
            .zip(other.aggregate_objects.iter())
            .enumerate()
        {
            if left.payload_size != right.payload_size
                || left.child_count != right.child_count
                || left.return_type != right.return_type
            {
                return Err(paro_error::internal(format!(
                    "Aggregate object mismatch at index {idx}: \
payload_size {}/{} child_count {}/{} return_type {:?}/{:?}",
                    left.payload_size,
                    right.payload_size,
                    left.child_count,
                    right.child_count,
                    left.return_type,
                    right.return_type
                )));
            }
        }
        if self.layout.row_width != other.layout.row_width
            || self.layout.agg_state_offset != other.layout.agg_state_offset
        {
            return Err(paro_error::internal(format!(
                "Tuple layout mismatch while combining hash tables: \
row_width {}/{} agg_state_offset {}/{}",
                self.layout.row_width,
                other.layout.row_width,
                self.layout.agg_state_offset,
                other.layout.agg_state_offset
            )));
        }
        Ok(())
    }

    #[cfg(debug_assertions)]
    fn validate_state_addresses(&self, addresses: &Vector, row_count: usize) -> Result<()> {
        if row_count > addresses.len() {
            return Err(paro_error::internal(format!(
                "Address vector too small during state validation: rows={row_count}, addresses={}",
                addresses.len()
            )));
        }

        let data_base = self.data.as_ptr() as usize;
        let data_bytes = self
            .data
            .len()
            .checked_mul(size_of::<u64>())
            .ok_or_else(|| {
                paro_error::internal(format!(
                    "Hash table data byte-size overflow during state validation: words={}",
                    self.data.len()
                ))
            })?;
        let state_base = data_base
            .checked_add(self.layout.agg_state_offset)
            .ok_or_else(|| {
                paro_error::internal(format!(
                    "Hash table state base overflow during state validation: base=0x{data_base:x}, agg_state_offset={}",
                    self.layout.agg_state_offset
                ))
            })?;
        let data_end = data_base.checked_add(data_bytes).ok_or_else(|| {
            paro_error::internal(format!(
                "Hash table data end overflow during state validation: base=0x{data_base:x}, bytes={data_bytes}"
            ))
        })?;

        let address_format = addresses.try_decode_ref(addresses.len())?;
        let address_data = address_format.get_data::<*mut u8>();
        for row_idx in 0..row_count {
            let physical_idx = address_format.physical_index(row_idx);
            if !address_format.validity().is_valid(physical_idx) {
                return Err(paro_error::internal(format!(
                    "Address vector contains NULL at row {row_idx} during state validation"
                )));
            }
            let ptr = unsafe { *address_data.add(physical_idx) };
            if ptr.is_null() {
                return Err(paro_error::internal(format!(
                    "Address vector contains NULL pointer at row {row_idx} during state validation"
                )));
            }

            let addr = ptr as usize;
            if addr % 8 != 0 {
                return Err(paro_error::internal(format!(
                    "Misaligned aggregate state pointer: ptr=0x{addr:x}, row={row_idx}, row_width={}, agg_state_offset={}, count={}, data_base=0x{data_base:x}, data_bytes={data_bytes}",
                    self.layout.row_width, self.layout.agg_state_offset, self.count
                )));
            }
            if addr < state_base || addr >= data_end {
                return Err(paro_error::internal(format!(
                    "Aggregate state pointer out of bounds: ptr=0x{addr:x}, row={row_idx}, state_base=0x{state_base:x}, data_end=0x{data_end:x}, row_width={}, agg_state_offset={}, count={}",
                    self.layout.row_width, self.layout.agg_state_offset, self.count
                )));
            }

            let rel = addr - state_base;
            if rel % self.layout.row_width != 0 {
                return Err(paro_error::internal(format!(
                    "Aggregate state pointer does not map to row boundary: ptr=0x{addr:x}, row={row_idx}, rel={rel}, row_width={}, agg_state_offset={}, count={}, state_base=0x{state_base:x}",
                    self.layout.row_width, self.layout.agg_state_offset, self.count
                )));
            }
        }
        Ok(())
    }

    fn ensure_lookup_storage_available(&self) -> Result<()> {
        if self.capacity == 0 || self.entries.len() != self.capacity {
            return Err(paro_error::internal(
                "Aggregate hash table lookup storage was released after finalize".to_string(),
            ));
        }
        match (&self.inline_key_layout, &self.inline_keys) {
            (Some(_), Some(keys)) if keys.len() == self.capacity => {}
            (None, None) => {}
            _ => {
                return Err(paro_error::internal(
                    "Aggregate inline-key lookup storage is inconsistent",
                ));
            }
        }
        Ok(())
    }

    fn inline_key_storage_mut_ptr(&mut self) -> Result<*mut InlineKey> {
        let capacity = self.capacity;
        let keys = self.inline_keys.as_mut().ok_or_else(|| {
            paro_error::internal("Aggregate inline-key sidecar is not initialized")
        })?;
        if keys.len() != capacity {
            return Err(paro_error::internal(format!(
                "Aggregate inline-key sidecar length mismatch: keys={}, capacity={capacity}",
                keys.len()
            )));
        }
        Ok(keys.as_mut_ptr())
    }

    fn ensure_capacity_for(&mut self, incoming_rows: usize) -> Result<()> {
        if incoming_rows == 0 {
            return Ok(());
        }
        let target_count = self.count.checked_add(incoming_rows).ok_or_else(|| {
            paro_error::internal(format!(
                "Hash table count overflow: count={} incoming={incoming_rows}",
                self.count
            ))
        })?;
        if target_count <= resize_threshold(self.capacity) {
            return Ok(());
        }

        let mut target_capacity = self.capacity;
        while target_count > resize_threshold(target_capacity) {
            target_capacity = target_capacity.checked_mul(2).ok_or_else(|| {
                paro_error::internal(format!(
                    "Hash table capacity overflow when growing from {}",
                    target_capacity
                ))
            })?;
        }
        self.resize(target_capacity)
    }

    fn ensure_row_storage_capacity(&mut self, incoming_rows: usize) -> Result<()> {
        let target_rows = self.count.checked_add(incoming_rows).ok_or_else(|| {
            paro_error::internal(format!(
                "Hash table row count overflow for row storage reserve: count={} incoming={incoming_rows}",
                self.count
            ))
        })?;
        let target_bytes = target_rows
            .checked_mul(self.layout.row_width)
            .ok_or_else(|| {
                paro_error::internal(format!(
                    "Hash table row storage size overflow: rows={target_rows}, row_width={}",
                    self.layout.row_width
                ))
            })?;
        let target_words = bytes_to_words(target_bytes)?;
        if target_words > self.data.capacity() {
            if self.data.capacity() > self.data.len() {
                self.data.shrink_to_fit_and_refund();
            }
            let additional = target_words.saturating_sub(self.data.len());
            self.data.try_reserve(additional)?;
        }
        Ok(())
    }

    fn release_finalized_lookup_storage(&mut self) {
        self.entries.clear();
        self.entries.shrink_to_fit_and_refund();
        if let Some(inline_keys) = &mut self.inline_keys {
            inline_keys.clear();
            inline_keys.shrink_to_fit_and_refund();
        }
        self.capacity = 0;
        self.bitmask = 0;
    }

    fn append_group_row(
        &mut self,
        source: &TupleScatterSource<'_>,
        source_row_idx: usize,
        hash: u64,
    ) -> Result<usize> {
        let row_idx = self.count;
        let row_words = self.row_width_words();
        let old_len = self.data.len();
        let new_len = old_len.checked_add(row_words).ok_or_else(|| {
            paro_error::internal(format!(
                "Hash table row storage overflow: old_len={old_len}, row_width={}",
                self.layout.row_width
            ))
        })?;
        self.data.try_resize_with(new_len, || 0)?;

        let row_ptr =
            unsafe { (self.data.as_mut_ptr() as *mut u8).add(old_len * size_of::<u64>()) };
        if let Err(err) = self.layout.scatter_prepared_groups(
            row_ptr,
            source,
            source_row_idx,
            &mut self.varlen_heap,
        ) {
            self.data.truncate(old_len);
            return Err(err);
        }
        self.layout.store_hash(row_ptr, hash);
        Ok(row_idx)
    }

    #[inline]
    fn slot_for_hash(&self, hash: u64) -> usize {
        (hash as usize) & self.bitmask
    }

    #[inline]
    fn row_ptr(&self, row_idx: usize) -> *const u8 {
        debug_assert!(row_idx < self.count);
        let offset = row_idx * self.layout.row_width;
        unsafe { (self.data.as_ptr() as *const u8).add(offset) }
    }

    #[inline]
    fn state_ptr(&self, row_idx: usize) -> *mut u8 {
        unsafe { self.row_ptr(row_idx).add(self.layout.agg_state_offset) as *mut u8 }
    }

    #[inline]
    fn row_width_words(&self) -> usize {
        debug_assert_eq!(self.layout.row_width % size_of::<u64>(), 0);
        self.layout.row_width / size_of::<u64>()
    }
}

impl Drop for GroupedAggregateHashTable {
    fn drop(&mut self) {
        let _ = self.destroy();
    }
}

fn validate_aggregate_inputs(
    aggregate_objects: &[AggregateObject],
    aggregate_inputs: &[Vec<usize>],
) -> Result<()> {
    if aggregate_objects.len() != aggregate_inputs.len() {
        return Err(paro_error::internal(format!(
            "Aggregate input mapping count mismatch: objects={} mappings={}",
            aggregate_objects.len(),
            aggregate_inputs.len()
        )));
    }
    for (idx, (object, inputs)) in aggregate_objects
        .iter()
        .zip(aggregate_inputs.iter())
        .enumerate()
    {
        if inputs.len() != object.child_count {
            return Err(paro_error::internal(format!(
                "Aggregate input mapping arity mismatch at index {idx}: expected={} actual={}",
                object.child_count,
                inputs.len()
            )));
        }
    }
    Ok(())
}

fn bytes_to_words(bytes: usize) -> Result<usize> {
    let word = size_of::<u64>();
    let words = bytes
        .checked_add(word - 1)
        .ok_or_else(|| paro_error::internal(format!("Row storage bytes overflow: {bytes}")))?;
    Ok(words / word)
}

fn accounted_vec_for_context<T>(
    memory: &MemoryAccountingContext,
    tag: MemoryTag,
    class: MemoryAccountingClass,
) -> Result<AccountedVec<T>> {
    Ok(AccountedVec::new_with_accounting(
        memory.grant()?,
        tag,
        class,
    ))
}

fn inline_key_component_width(logical_type: &LogicalType) -> Option<usize> {
    match logical_type {
        LogicalType::TinyInt | LogicalType::UTinyInt => Some(1),
        LogicalType::SmallInt | LogicalType::USmallInt => Some(2),
        LogicalType::Integer | LogicalType::UInteger | LogicalType::Date => Some(4),
        LogicalType::BigInt | LogicalType::UBigInt => Some(8),
        _ => None,
    }
}

fn write_inline_component_bytes(
    key_bytes: &mut [u8; INLINE_KEY_MAX_BYTES],
    offset: usize,
    column: &Vector,
    row_idx: usize,
    logical_type: &LogicalType,
) -> Result<()> {
    let width = inline_key_component_width(logical_type).ok_or_else(|| {
        paro_error::internal(format!(
            "Unsupported inline key group type: {logical_type:?}"
        ))
    })?;
    let end = offset.checked_add(width).ok_or_else(|| {
        paro_error::internal(format!(
            "Inline key byte offset overflow: offset={offset}, width={width}"
        ))
    })?;
    if end > INLINE_KEY_MAX_BYTES {
        return Err(paro_error::internal(format!(
            "Inline key component out of bounds: offset={offset}, width={width}"
        )));
    }

    match logical_type {
        LogicalType::TinyInt => {
            let value = column.get_i8(row_idx).ok_or_else(|| {
                paro_error::internal(format!("Expected non-null TINYINT at row {row_idx}"))
            })?;
            key_bytes[offset] = value as u8;
        }
        LogicalType::UTinyInt => {
            let value = column.get_u8(row_idx).ok_or_else(|| {
                paro_error::internal(format!("Expected non-null UTINYINT at row {row_idx}"))
            })?;
            key_bytes[offset] = value;
        }
        LogicalType::SmallInt => {
            let value = column.get_i16(row_idx).ok_or_else(|| {
                paro_error::internal(format!("Expected non-null SMALLINT at row {row_idx}"))
            })?;
            key_bytes[offset..end].copy_from_slice(&value.to_le_bytes());
        }
        LogicalType::USmallInt => {
            let value = column.get_u16(row_idx).ok_or_else(|| {
                paro_error::internal(format!("Expected non-null USMALLINT at row {row_idx}"))
            })?;
            key_bytes[offset..end].copy_from_slice(&value.to_le_bytes());
        }
        LogicalType::Integer | LogicalType::Date => {
            let value = column.get_i32(row_idx).ok_or_else(|| {
                paro_error::internal(format!("Expected non-null INT32/DATE at row {row_idx}"))
            })?;
            key_bytes[offset..end].copy_from_slice(&value.to_le_bytes());
        }
        LogicalType::UInteger => {
            let value = column.get_u32(row_idx).ok_or_else(|| {
                paro_error::internal(format!("Expected non-null UINTEGER at row {row_idx}"))
            })?;
            key_bytes[offset..end].copy_from_slice(&value.to_le_bytes());
        }
        LogicalType::BigInt => {
            let value = column.get_i64(row_idx).ok_or_else(|| {
                paro_error::internal(format!("Expected non-null BIGINT at row {row_idx}"))
            })?;
            key_bytes[offset..end].copy_from_slice(&value.to_le_bytes());
        }
        LogicalType::UBigInt => {
            let value = column.get_u64(row_idx).ok_or_else(|| {
                paro_error::internal(format!("Expected non-null UBIGINT at row {row_idx}"))
            })?;
            key_bytes[offset..end].copy_from_slice(&value.to_le_bytes());
        }
        _ => {
            return Err(paro_error::internal(format!(
                "Unsupported inline key group type: {logical_type:?}"
            )));
        }
    }
    Ok(())
}

fn write_serialized_inline_component(
    key_bytes: &mut [u8; INLINE_KEY_MAX_BYTES],
    offset: usize,
    source: *const u8,
    logical_type: &LogicalType,
) -> Result<()> {
    let width = inline_key_component_width(logical_type).ok_or_else(|| {
        paro_error::internal(format!(
            "Unsupported serialized inline key group type: {logical_type:?}"
        ))
    })?;
    let end = offset.checked_add(width).ok_or_else(|| {
        paro_error::internal(format!(
            "Serialized inline key byte offset overflow: offset={offset}, width={width}"
        ))
    })?;
    if end > INLINE_KEY_MAX_BYTES {
        return Err(paro_error::internal(format!(
            "Serialized inline key component out of bounds: offset={offset}, width={width}"
        )));
    }

    macro_rules! write_le {
        ($ty:ty) => {{
            let value = unsafe { std::ptr::read_unaligned(source as *const $ty) };
            key_bytes[offset..end].copy_from_slice(&value.to_le_bytes());
        }};
    }
    match logical_type {
        LogicalType::TinyInt => key_bytes[offset] = unsafe { *source } as u8,
        LogicalType::UTinyInt => key_bytes[offset] = unsafe { *source },
        LogicalType::SmallInt => write_le!(i16),
        LogicalType::USmallInt => write_le!(u16),
        LogicalType::Integer | LogicalType::Date => write_le!(i32),
        LogicalType::UInteger => write_le!(u32),
        LogicalType::BigInt => write_le!(i64),
        LogicalType::UBigInt => write_le!(u64),
        _ => {
            return Err(paro_error::internal(format!(
                "Unsupported serialized inline key group type: {logical_type:?}"
            )));
        }
    }
    Ok(())
}

fn validate_hashes(hashes: &Vector, row_count: usize) -> Result<()> {
    if hashes.logical_type() != &LogicalType::UBigInt {
        return Err(paro_error::internal(format!(
            "Hash vector type must be UBigInt, found {:?}",
            hashes.logical_type()
        )));
    }
    if hashes.len() < row_count {
        return Err(paro_error::internal(format!(
            "Hash vector too small: required={row_count}, actual={}",
            hashes.len()
        )));
    }
    Ok(())
}

fn validate_addresses_vector(addresses: &Vector, row_count: usize) -> Result<()> {
    if addresses.capacity() < row_count {
        return Err(paro_error::internal(format!(
            "Address vector capacity too small: required={row_count}, capacity={}",
            addresses.capacity()
        )));
    }
    Ok(())
}

fn validate_filter(filter: &SelectionVector, payload_rows: usize) -> Result<()> {
    for idx in 0..filter.len() {
        let row = filter.get(idx);
        if row >= payload_rows {
            return Err(paro_error::internal(format!(
                "Filter selection index out of bounds: selection[{idx}]={row}, payload_rows={payload_rows}"
            )));
        }
    }
    Ok(())
}

fn pointer_vector_from_slice(ptrs: &[*mut u8], allocator: Arc<dyn Allocator>) -> Result<Vector> {
    let mut result = Vector::try_new(LogicalType::BigInt, ptrs.len(), allocator)?;
    result.set_count(ptrs.len());
    unsafe {
        let result_data = result.flat_data_mut::<*mut u8>();
        for (idx, ptr) in ptrs.iter().enumerate() {
            *result_data.add(idx) = *ptr;
        }
    }
    Ok(result)
}

fn normalize_capacity(capacity: usize) -> Result<usize> {
    let normalized = capacity.max(MIN_CAPACITY).next_power_of_two();
    if normalized == 0 {
        return Err(paro_error::internal(
            "Invalid hash table capacity after normalization".to_string(),
        ));
    }
    Ok(normalized)
}

fn resize_threshold(capacity: usize) -> usize {
    ((capacity * LOAD_FACTOR_NUMERATOR) / LOAD_FACTOR_DENOMINATOR).max(1)
}

fn initial_capacity_for_hint(
    group_types: &[LogicalType],
    aggregate_objects: &[AggregateObject],
    hint: HashTableCapacityHint,
) -> Result<usize> {
    if hint.expected_rows == 0 || hint.max_fixed_bytes == 0 {
        return Ok(MIN_CAPACITY);
    }

    let layout = TupleLayout::build(group_types, aggregate_objects)?;
    let has_inline_keys = InlineKeyLayout::try_new(group_types).is_some();
    let mut capacity = MIN_CAPACITY;
    while hint.expected_rows > resize_threshold(capacity) {
        capacity = capacity.checked_mul(2).ok_or_else(|| {
            paro_error::internal(format!(
                "Hash table capacity hint overflow: expected_rows={}",
                hint.expected_rows
            ))
        })?;
    }
    while capacity > MIN_CAPACITY
        && fixed_allocation_bytes(capacity, layout.row_width, has_inline_keys)?
            > hint.max_fixed_bytes
    {
        capacity /= 2;
    }
    Ok(capacity)
}

fn fixed_allocation_bytes(
    capacity: usize,
    row_width: usize,
    has_inline_keys: bool,
) -> Result<usize> {
    let lookup_bytes = capacity
        .checked_mul(size_of::<AggregateHTEntry>())
        .ok_or_else(|| paro_error::internal("Hash table lookup size overflow"))?;
    let inline_bytes = if has_inline_keys {
        capacity
            .checked_mul(size_of::<InlineKey>())
            .ok_or_else(|| paro_error::internal("Hash table inline-key size overflow"))?
    } else {
        0
    };
    let row_bytes = resize_threshold(capacity)
        .checked_mul(row_width)
        .ok_or_else(|| paro_error::internal("Hash table row reserve size overflow"))?;
    lookup_bytes
        .checked_add(inline_bytes)
        .and_then(|bytes| bytes.checked_add(row_bytes))
        .ok_or_else(|| paro_error::internal("Hash table fixed allocation size overflow"))
}

#[path = "grouped_aggregate_hashtable_merge.rs"]
mod merge;

#[path = "grouped_aggregate_hashtable_lookup.rs"]
mod lookup;
pub(crate) use lookup::SerializedGroupLookup;

#[path = "grouped_aggregate_hashtable_projection.rs"]
mod projection;
pub(crate) use projection::SerializedSourceRows;

#[cfg(test)]
#[path = "grouped_aggregate_hashtable_tests.rs"]
mod tests;
