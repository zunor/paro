// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Flat linear-probing grouped aggregate hash table.

use std::mem::size_of;
use std::sync::Arc;

use paro_common::allocator::{default_allocator, ArenaAllocator};
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_common::vector::VectorOperations;
use paro_common::vector::{SelectionVector, Vector, VECTOR_SIZE};
use paro_function::aggregate::{AggregateCombineType, AggregateInputData};

use super::aggregate_kernel::{
    combine_states, destroy_states, finalize_states, initialize_states, update_filtered_states,
    update_states, AggregatePayload,
};
use super::aggregate_object::AggregateObject;
use super::aggregate_state::AggregateStateLayout;
use super::tuple_layout::{TupleLayout, VarlenHeap};

const MIN_CAPACITY: usize = 8;
const LOAD_FACTOR_NUMERATOR: usize = 3; // 0.6
const LOAD_FACTOR_DENOMINATOR: usize = 5;
const HASH_MIX_MULTIPLIER: u64 = 0xd6e8_feb8_6659_fd93;
const EMPTY_GROUP_HASH: u64 = 0x9e37_79b9_7f4a_7c15;
const INLINE_KEY_MAX_BYTES: usize = 8;

#[derive(Debug, Clone, Copy, Default)]
struct AggregateHTEntry {
    value: u64,
    inline_key: u64,
    inline_meta: u64,
}

impl AggregateHTEntry {
    const SALT_MASK: u64 = 0xFFFF_0000_0000_0000;
    const ROW_INDEX_MASK: u64 = 0x0000_FFFF_FFFF_FFFF;

    #[inline]
    fn empty() -> Self {
        Self {
            value: 0,
            inline_key: 0,
            inline_meta: 0,
        }
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
            inline_key: 0,
            inline_meta: 0,
        })
    }

    #[inline]
    fn row_idx(self) -> usize {
        debug_assert!(self.is_occupied());
        ((self.value & Self::ROW_INDEX_MASK) - 1) as usize
    }

    #[inline]
    fn set_inline_key(&mut self, key: InlineKey) {
        self.inline_key = key.bits;
        self.inline_meta = key.null_mask;
    }

    #[inline]
    fn inline_key(self) -> InlineKey {
        InlineKey {
            bits: self.inline_key,
            null_mask: self.inline_meta,
        }
    }

    #[inline]
    fn matches_inline_key(self, key: InlineKey) -> bool {
        self.inline_key == key.bits && self.inline_meta == key.null_mask
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
}

/// Scan cursor for [`GroupedAggregateHashTable::scan`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct HTScanPosition {
    pub offset: usize,
}

/// Flat grouped aggregate hash table.
#[derive(Debug)]
pub struct GroupedAggregateHashTable {
    entries: Vec<AggregateHTEntry>,
    // Keep row storage 8-byte aligned so aggregate states can be safely cast to typed pointers.
    data: Vec<u64>,
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
    ) -> Result<Self> {
        Self::with_capacity(
            group_types,
            aggregate_objects,
            aggregate_inputs,
            MIN_CAPACITY,
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

    pub fn with_capacity(
        group_types: Vec<LogicalType>,
        aggregate_objects: Vec<AggregateObject>,
        aggregate_inputs: Vec<Vec<usize>>,
        initial_capacity: usize,
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
        let entries = vec![AggregateHTEntry::empty(); capacity];
        let reserve_rows = resize_threshold(capacity).max(1);
        let reserve_bytes = layout.row_width.checked_mul(reserve_rows).ok_or_else(|| {
            paro_error::internal(format!(
                "Hash table row storage reserve overflow: row_width={} reserve_rows={reserve_rows}",
                layout.row_width
            ))
        })?;
        let reserve_words = bytes_to_words(reserve_bytes)?;
        let data = Vec::with_capacity(reserve_words);

        Ok(Self {
            entries,
            data,
            layout,
            state_layout,
            aggregate_objects,
            aggregate_inputs,
            aggregate_return_types,
            varlen_heap: VarlenHeap::new(),
            aggregate_allocator: ArenaAllocator::new(Arc::new(default_allocator())),
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

    /// Hash grouped keys using Paro vector hash implementation.
    pub fn hash_groups(&self, groups: &Chunk) -> Result<Vector> {
        self.validate_group_chunk(groups)?;
        let count = groups.size();
        let mut hashes = Vector::with_capacity(LogicalType::UBigInt, count);
        hashes.set_count(count);
        if count == 0 {
            return Ok(hashes);
        }

        if self.layout.group_count() == 0 {
            for row_idx in 0..count {
                hashes.set_u64(row_idx, EMPTY_GROUP_HASH);
            }
            return Ok(hashes);
        }

        let first = groups.column(0).ok_or_else(|| {
            paro_error::internal("Missing first group key column while hashing".to_string())
        })?;
        VectorOperations::hash(first.as_ref(), &mut hashes, count);

        let mut column_hashes = Vector::with_capacity(LogicalType::UBigInt, count);
        for group_idx in 1..self.layout.group_count() {
            let group_column = groups.column(group_idx).ok_or_else(|| {
                paro_error::internal(format!(
                    "Missing group key column while hashing at index {group_idx}"
                ))
            })?;
            VectorOperations::hash(group_column.as_ref(), &mut column_hashes, count);
            for row_idx in 0..count {
                let left = hashes.get_u64(row_idx).ok_or_else(|| {
                    paro_error::internal(format!("Missing hash value at row {row_idx}"))
                })?;
                let right = column_hashes.get_u64(row_idx).ok_or_else(|| {
                    paro_error::internal(format!(
                        "Missing combined hash value at row {row_idx}, column {group_idx}"
                    ))
                })?;
                hashes.set_u64(row_idx, combine_hash_scalar(left, right));
            }
        }
        Ok(hashes)
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
        validate_addresses_vector(addresses, groups.size())?;

        if groups.size() == 0 {
            addresses.set_count(0);
            *new_groups = SelectionVector::from_indices(Vec::new());
            return Ok(0);
        }

        self.ensure_capacity_for(groups.size())?;
        self.ensure_row_storage_capacity(groups.size())?;

        let hash_format = hashes.decode(groups.size());
        let hash_data = hash_format.get_data::<u64>();

        addresses.set_count(groups.size());
        let address_data = unsafe { addresses.flat_data_mut::<*mut u8>() };

        let mut new_group_rows = Vec::new();
        let mut new_state_ptrs = Vec::new();
        let inline_key_layout = self.inline_key_layout.clone();
        if let Some(inline_layout) = inline_key_layout {
            for row_idx in 0..groups.size() {
                let hash_idx = hash_format.sel().get(row_idx);
                if !hash_format.validity().is_valid(hash_idx) {
                    return Err(paro_error::internal(format!(
                        "Group hash contains NULL at row {row_idx}"
                    )));
                }
                let hash = unsafe { *hash_data.add(hash_idx) };
                let inline_key = inline_layout.encode_row(groups, row_idx)?;
                let mut slot = self.slot_for_hash(hash);
                loop {
                    let entry = self.entries[slot];
                    if !entry.is_occupied() {
                        let new_row_idx = self.append_group_row(groups, row_idx, hash)?;
                        let mut new_entry = AggregateHTEntry::from_hash_and_row(hash, new_row_idx)?;
                        new_entry.set_inline_key(inline_key);
                        self.entries[slot] = new_entry;
                        self.count += 1;
                        let state_ptr = self.state_ptr(new_row_idx);
                        unsafe {
                            *address_data.add(row_idx) = state_ptr;
                        }
                        new_group_rows.push(row_idx as u32);
                        new_state_ptrs.push(state_ptr);
                        break;
                    }

                    if entry.matches_hash(hash) && entry.matches_inline_key(inline_key) {
                        unsafe {
                            *address_data.add(row_idx) = self.state_ptr(entry.row_idx());
                        }
                        break;
                    }

                    slot = (slot + 1) & self.bitmask;
                }
            }
        } else {
            for row_idx in 0..groups.size() {
                let hash_idx = hash_format.sel().get(row_idx);
                if !hash_format.validity().is_valid(hash_idx) {
                    return Err(paro_error::internal(format!(
                        "Group hash contains NULL at row {row_idx}"
                    )));
                }
                let hash = unsafe { *hash_data.add(hash_idx) };
                let mut slot = self.slot_for_hash(hash);
                loop {
                    let entry = self.entries[slot];
                    if !entry.is_occupied() {
                        let new_row_idx = self.append_group_row(groups, row_idx, hash)?;
                        self.entries[slot] =
                            AggregateHTEntry::from_hash_and_row(hash, new_row_idx)?;
                        self.count += 1;
                        let state_ptr = self.state_ptr(new_row_idx);
                        unsafe {
                            *address_data.add(row_idx) = state_ptr;
                        }
                        new_group_rows.push(row_idx as u32);
                        new_state_ptrs.push(state_ptr);
                        break;
                    }

                    if entry.matches_hash(hash)
                        && self.layout.compare_groups(
                            self.row_ptr(entry.row_idx()),
                            groups,
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
            let new_addresses = pointer_vector_from_slice(&new_state_ptrs);
            initialize_states(
                &self.state_layout,
                &self.aggregate_objects,
                &new_addresses,
                new_state_ptrs.len(),
            )?;
        }

        *new_groups = SelectionVector::from_indices(new_group_rows);
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

    /// Combine aggregate states from another hash table into this table.
    pub fn combine(&mut self, other: &mut Self) -> Result<()> {
        self.ensure_compatible(other)?;
        if other.count == 0 {
            return Ok(());
        }

        let mut row_offset = 0usize;
        while row_offset < other.count {
            let batch_size = (other.count - row_offset).min(VECTOR_SIZE);

            let groups = other.materialize_groups(row_offset, batch_size)?;
            let mut hashes = Vector::with_capacity(LogicalType::UBigInt, batch_size);
            hashes.set_count(batch_size);
            let mut source_addresses = Vector::with_capacity(LogicalType::BigInt, batch_size);
            source_addresses.set_count(batch_size);
            unsafe {
                let hash_data = hashes.flat_data_mut::<u64>();
                let source_data = source_addresses.flat_data_mut::<*mut u8>();
                for idx in 0..batch_size {
                    let source_row_idx = row_offset + idx;
                    *hash_data.add(idx) = other.layout.load_hash(other.row_ptr(source_row_idx));
                    *source_data.add(idx) = other.state_ptr(source_row_idx);
                }
            }

            let mut target_addresses = Vector::with_capacity(LogicalType::BigInt, batch_size);
            let mut new_groups = SelectionVector::with_capacity(batch_size);
            self.find_or_create_groups(&groups, &hashes, &mut target_addresses, &mut new_groups)?;

            if !self.aggregate_objects.is_empty() {
                let mut input_data = AggregateInputData::new(
                    None,
                    &mut self.aggregate_allocator,
                    AggregateCombineType::AllowDestructive,
                );
                combine_states(
                    &self.aggregate_objects,
                    &mut input_data,
                    &source_addresses,
                    &target_addresses,
                    batch_size,
                )?;
            }
            row_offset += batch_size;
        }
        Ok(())
    }

    /// Scan grouped keys + finalized aggregate values into `result`.
    ///
    /// Returns `true` if output rows were produced, `false` when scan is complete.
    pub fn scan(&mut self, position: &mut HTScanPosition, result: &mut Chunk) -> Result<bool> {
        let group_count = self.layout.group_count();
        let aggregate_count = self.aggregate_objects.len();
        let required_columns = group_count + aggregate_count;
        if result.column_count() < required_columns {
            return Err(paro_error::internal(format!(
                "Result chunk has insufficient columns for hash table scan: required={required_columns}, actual={}",
                result.column_count()
            )));
        }
        if position.offset >= self.count {
            result.set_cardinality(0);
            return Ok(false);
        }

        let batch_size = (self.count - position.offset).min(result.capacity());
        result.set_cardinality(batch_size);

        for group_idx in 0..group_count {
            let result_vector = result.column_mut(group_idx).ok_or_else(|| {
                paro_error::internal(format!(
                    "Missing group output column {group_idx} while scanning aggregate hash table"
                ))
            })?;
            result_vector.set_count(batch_size);
            for row in 0..batch_size {
                let row_ptr = self.row_ptr(position.offset + row);
                let value =
                    self.layout
                        .deserialize_group_value(row_ptr, group_idx, &self.varlen_heap)?;
                result_vector.set_value(row, &value);
            }
        }

        if aggregate_count > 0 {
            let mut state_addresses = Vector::with_capacity(LogicalType::BigInt, batch_size);
            state_addresses.set_count(batch_size);
            unsafe {
                let address_data = state_addresses.flat_data_mut::<*mut u8>();
                for row in 0..batch_size {
                    *address_data.add(row) = self.state_ptr(position.offset + row);
                }
            }

            let mut aggregate_chunk = Chunk::initialize(&self.aggregate_return_types, batch_size);
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
                batch_size,
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
                target.set_count(batch_size);
                for row in 0..batch_size {
                    target.copy_at(row, source.as_ref(), row);
                }
            }
        }

        position.offset += batch_size;
        Ok(true)
    }

    pub fn destroy(&mut self) -> Result<()> {
        if self.count == 0 {
            return Ok(());
        }

        let mut addresses = Vector::with_capacity(LogicalType::BigInt, self.count);
        addresses.set_count(self.count);
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

        self.entries.fill(AggregateHTEntry::empty());
        self.data.clear();
        self.varlen_heap.reset();
        self.aggregate_allocator.reset();
        self.count = 0;
        Ok(())
    }

    pub fn memory_usage(&self) -> usize {
        self.entries.capacity() * size_of::<AggregateHTEntry>()
            + self.data.capacity() * size_of::<u64>()
            + self.varlen_heap.len()
            + self.aggregate_allocator.allocation_size()
    }

    pub fn resize(&mut self, new_capacity: usize) -> Result<()> {
        let new_capacity = normalize_capacity(new_capacity)?;
        if new_capacity <= self.capacity {
            return Ok(());
        }

        let mut new_entries = vec![AggregateHTEntry::empty(); new_capacity];
        let new_bitmask = new_capacity - 1;
        for old_entry in self.entries.iter().copied() {
            if !old_entry.is_occupied() {
                continue;
            }
            let row_idx = old_entry.row_idx();
            let hash = self.layout.load_hash(self.row_ptr(row_idx));
            let mut slot = (hash as usize) & new_bitmask;
            loop {
                if !new_entries[slot].is_occupied() {
                    let mut new_entry = AggregateHTEntry::from_hash_and_row(hash, row_idx)?;
                    if self.inline_key_layout.is_some() {
                        new_entry.set_inline_key(old_entry.inline_key());
                    }
                    new_entries[slot] = new_entry;
                    break;
                }
                slot = (slot + 1) & new_bitmask;
            }
        }
        self.entries = new_entries;
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

        let address_format = addresses.decode(addresses.len());
        let address_data = address_format.get_data::<*mut u8>();
        for row_idx in 0..row_count {
            let physical_idx = address_format.sel().get(row_idx);
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

    fn ensure_capacity_for(&mut self, incoming_rows: usize) -> Result<()> {
        if incoming_rows == 0 {
            return Ok(());
        }
        while self.count.checked_add(incoming_rows).ok_or_else(|| {
            paro_error::internal(format!(
                "Hash table count overflow: count={} incoming={incoming_rows}",
                self.count
            ))
        })? > resize_threshold(self.capacity)
        {
            let new_capacity = self.capacity.checked_mul(2).ok_or_else(|| {
                paro_error::internal(format!(
                    "Hash table capacity overflow when growing from {}",
                    self.capacity
                ))
            })?;
            self.resize(new_capacity)?;
        }
        Ok(())
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
            let additional = target_words.saturating_sub(self.data.len());
            self.data.reserve(additional);
        }
        Ok(())
    }

    fn append_group_row(
        &mut self,
        groups: &Chunk,
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
        self.data.resize(new_len, 0);

        let row_ptr =
            unsafe { (self.data.as_mut_ptr() as *mut u8).add(old_len * size_of::<u64>()) };
        if let Err(err) =
            self.layout
                .scatter_groups(row_ptr, groups, source_row_idx, &mut self.varlen_heap)
        {
            self.data.truncate(old_len);
            return Err(err);
        }
        self.layout.store_hash(row_ptr, hash);
        Ok(row_idx)
    }

    fn materialize_groups(&self, start_row: usize, count: usize) -> Result<Chunk> {
        let mut groups = Chunk::initialize(&self.layout.group_types, count);
        groups.set_cardinality(count);
        for row_idx in 0..count {
            let source_ptr = self.row_ptr(start_row + row_idx);
            for group_idx in 0..self.layout.group_count() {
                let value = self.layout.deserialize_group_value(
                    source_ptr,
                    group_idx,
                    &self.varlen_heap,
                )?;
                let group_col = groups.column_mut(group_idx).ok_or_else(|| {
                    paro_error::internal(format!(
                        "Missing materialized group column {group_idx} for combine"
                    ))
                })?;
                group_col.set_value(row_idx, &value);
            }
        }
        Ok(groups)
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

fn pointer_vector_from_slice(ptrs: &[*mut u8]) -> Vector {
    let mut result = Vector::with_capacity(LogicalType::BigInt, ptrs.len());
    result.set_count(ptrs.len());
    unsafe {
        let result_data = result.flat_data_mut::<*mut u8>();
        for (idx, ptr) in ptrs.iter().enumerate() {
            *result_data.add(idx) = *ptr;
        }
    }
    result
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

#[inline]
fn combine_hash_scalar(mut left: u64, right: u64) -> u64 {
    left ^= left >> 32;
    left = left.wrapping_mul(HASH_MIX_MULTIPLIER);
    left ^ right
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;
    use std::collections::HashMap;
    use std::mem::size_of;
    use std::thread_local;

    use paro_common::runtime_value::Value;
    use paro_function::aggregate::AggregateFunction;
    use paro_planner::expression::{
        AggregateExpression, AggregateType, Expression, ReferenceExpression,
    };

    thread_local! {
        static DESTRUCTOR_CALLS: Cell<usize> = const { Cell::new(0) };
    }

    fn reset_destructor_calls() {
        DESTRUCTOR_CALLS.with(|calls| calls.set(0));
    }

    fn record_destructor_calls(count: usize) {
        DESTRUCTOR_CALLS.with(|calls| calls.set(calls.get() + count));
    }

    fn destructor_calls() -> usize {
        DESTRUCTOR_CALLS.with(Cell::get)
    }

    unsafe fn sum_initialize(state: *mut u8) {
        *(state as *mut i64) = 0;
    }

    unsafe fn sum_update(
        inputs: &[&Vector],
        _input_data: &AggregateInputData,
        states: &Vector,
        count: usize,
    ) {
        let input = inputs[0].decode(count);
        let input_data = input.get_data::<i64>();
        let state = states.decode(count);
        let state_data = state.get_data::<*mut u8>();
        for row in 0..count {
            let input_row = input.sel().get(row);
            if !input.validity().is_valid(input_row) {
                continue;
            }
            let state_row = state.sel().get(row);
            let state_ptr = *state_data.add(state_row) as *mut i64;
            *state_ptr += *input_data.add(input_row);
        }
    }

    unsafe fn sum_combine(
        source: &Vector,
        target: &Vector,
        _input_data: &AggregateInputData,
        count: usize,
    ) {
        let source_format = source.decode(count);
        let target_format = target.decode(count);
        let source_data = source_format.get_data::<*mut u8>();
        let target_data = target_format.get_data::<*mut u8>();
        for row in 0..count {
            let source_idx = source_format.sel().get(row);
            let target_idx = target_format.sel().get(row);
            let source_ptr = *source_data.add(source_idx) as *const i64;
            let target_ptr = *target_data.add(target_idx) as *mut i64;
            *target_ptr += *source_ptr;
        }
    }

    unsafe fn sum_finalize(
        states: &Vector,
        _input_data: &AggregateInputData,
        result: &mut Vector,
        count: usize,
    ) {
        let state = states.decode(count);
        let state_data = state.get_data::<*mut u8>();
        let result_data = result.flat_data_mut::<i64>();
        for row in 0..count {
            let state_idx = state.sel().get(row);
            let state_ptr = *state_data.add(state_idx) as *const i64;
            *result_data.add(row) = *state_ptr;
        }
    }

    unsafe fn sum_destructor(_states: &Vector, _input_data: &AggregateInputData, count: usize) {
        record_destructor_calls(count);
    }

    fn make_sum_object() -> AggregateObject {
        let function = AggregateFunction::new(
            "test_sum".to_string(),
            vec![LogicalType::BigInt],
            LogicalType::BigInt,
            size_of::<i64>(),
            sum_initialize,
            sum_update,
            sum_combine,
            sum_finalize,
            None,
            Some(sum_destructor),
        );
        let bound = AggregateExpression::new(
            function,
            vec![Expression::Reference(ReferenceExpression::new(
                0,
                LogicalType::BigInt,
            ))],
            LogicalType::BigInt,
        )
        .with_aggr_type(AggregateType::NonDistinct);
        AggregateObject::from_bound(&bound).expect("aggregate object")
    }

    fn collect_scan_rows(table: &mut GroupedAggregateHashTable) -> Vec<Vec<Value>> {
        let mut types = table.layout.group_types.clone();
        types.extend(table.aggregate_return_types.clone());
        let mut position = HTScanPosition::default();
        let mut chunk = Chunk::initialize(&types, VECTOR_SIZE);
        let mut rows = Vec::new();
        while table
            .scan(&mut position, &mut chunk)
            .expect("scan result chunk")
        {
            for row in 0..chunk.size() {
                let mut values = Vec::with_capacity(chunk.column_count());
                for col in 0..chunk.column_count() {
                    values.push(chunk.column(col).expect("result column").get_value(row));
                }
                rows.push(values);
            }
        }
        rows
    }

    fn build_map_from_scan(rows: Vec<Vec<Value>>) -> HashMap<i32, i64> {
        let mut result = HashMap::new();
        for row in rows {
            let key = match row.first().expect("group key value") {
                Value::Integer(v) => *v,
                other => panic!("unexpected key value in scan output: {other:?}"),
            };
            let value = match row.get(1).expect("aggregate value") {
                Value::BigInt(v) => *v,
                other => panic!("unexpected aggregate value in scan output: {other:?}"),
            };
            result.insert(key, value);
        }
        result
    }

    #[test]
    fn grouped_hash_table_find_create_and_update() {
        let mut table = GroupedAggregateHashTable::with_capacity(
            vec![LogicalType::Integer],
            vec![make_sum_object()],
            vec![vec![0]],
            8,
        )
        .expect("create grouped hash table");

        let groups = Chunk::from_vectors(vec![Vector::from_i32(&[1, 2, 1, 3, 2])]);
        let hashes = table.hash_groups(&groups).expect("hash groups");
        let mut addresses = Vector::with_capacity(LogicalType::BigInt, groups.size());
        let mut new_groups = SelectionVector::with_capacity(groups.size());
        let new_group_count = table
            .find_or_create_groups(&groups, &hashes, &mut addresses, &mut new_groups)
            .expect("find/create groups");
        assert_eq!(new_group_count, 3);
        assert_eq!(new_groups.as_slice(), &[0, 1, 3]);

        let payload = Chunk::from_vectors(vec![Vector::from_i64(&[10, 20, 5, 7, 8])]);
        table
            .update_aggregates(&payload, &addresses, None)
            .expect("update aggregates");
        assert_eq!(table.count(), 3);

        let actual = build_map_from_scan(collect_scan_rows(&mut table));
        let expected = HashMap::from([(1, 15), (2, 28), (3, 7)]);
        assert_eq!(actual, expected);
    }

    #[test]
    fn grouped_hash_table_update_with_filter() {
        let mut table = GroupedAggregateHashTable::new(
            vec![LogicalType::Integer],
            vec![make_sum_object()],
            vec![vec![0]],
        )
        .expect("create grouped hash table");

        let groups = Chunk::from_vectors(vec![Vector::from_i32(&[1, 2, 3])]);
        let hashes = table.hash_groups(&groups).expect("hash groups");
        let mut addresses = Vector::with_capacity(LogicalType::BigInt, groups.size());
        let mut new_groups = SelectionVector::with_capacity(groups.size());
        table
            .find_or_create_groups(&groups, &hashes, &mut addresses, &mut new_groups)
            .expect("find/create");

        let payload = Chunk::from_vectors(vec![Vector::from_i64(&[10, 20, 30])]);
        let filter = SelectionVector::from_indices(vec![0, 2]);
        table
            .update_aggregates(&payload, &addresses, Some(&filter))
            .expect("filtered update");

        let actual = build_map_from_scan(collect_scan_rows(&mut table));
        let expected = HashMap::from([(1, 10), (2, 0), (3, 30)]);
        assert_eq!(actual, expected);
    }

    #[test]
    fn grouped_hash_table_varlen_and_null_group_keys() {
        let mut table = GroupedAggregateHashTable::new(
            vec![LogicalType::Integer, LogicalType::Varchar],
            vec![make_sum_object()],
            vec![vec![0]],
        )
        .expect("create grouped hash table");

        let mut strings = Vector::from_strings(&["a", "n", "a", "b", "b"]);
        strings.set_null(1, true);
        let groups = Chunk::from_vectors(vec![Vector::from_i32(&[1, 1, 1, 2, 2]), strings]);
        let hashes = table.hash_groups(&groups).expect("hash groups");
        let mut addresses = Vector::with_capacity(LogicalType::BigInt, groups.size());
        let mut new_groups = SelectionVector::with_capacity(groups.size());
        table
            .find_or_create_groups(&groups, &hashes, &mut addresses, &mut new_groups)
            .expect("find/create");
        let payload = Chunk::from_vectors(vec![Vector::from_i64(&[1, 2, 3, 4, 5])]);
        table
            .update_aggregates(&payload, &addresses, None)
            .expect("update");

        let rows = collect_scan_rows(&mut table);
        let mut actual: HashMap<(i32, Option<String>), i64> = HashMap::new();
        for row in rows {
            let key0 = match &row[0] {
                Value::Integer(v) => *v,
                other => panic!("unexpected integer group key: {other:?}"),
            };
            let key1 = match &row[1] {
                Value::Varchar(v) => Some(v.clone()),
                Value::Null(_) => None,
                other => panic!("unexpected varchar group key: {other:?}"),
            };
            let sum = match &row[2] {
                Value::BigInt(v) => *v,
                other => panic!("unexpected aggregate sum: {other:?}"),
            };
            actual.insert((key0, key1), sum);
        }
        let expected = HashMap::from([
            ((1, Some("a".to_string())), 4),
            ((1, None), 2),
            ((2, Some("b".to_string())), 9),
        ]);
        assert_eq!(actual, expected);
    }

    #[test]
    fn grouped_hash_table_resizes_and_reuses_entries() {
        let mut table =
            GroupedAggregateHashTable::with_capacity(vec![LogicalType::Integer], vec![], vec![], 8)
                .expect("create grouped hash table");

        let values = (0..50).map(|v| v as i32).collect::<Vec<_>>();
        let groups = Chunk::from_vectors(vec![Vector::from_i32(&values)]);
        let hashes = table.hash_groups(&groups).expect("hash groups");
        let mut addresses = Vector::with_capacity(LogicalType::BigInt, groups.size());
        let mut new_groups = SelectionVector::with_capacity(groups.size());
        let new_group_count = table
            .find_or_create_groups(&groups, &hashes, &mut addresses, &mut new_groups)
            .expect("first insertion");
        assert_eq!(new_group_count, 50);
        assert_eq!(table.count(), 50);
        assert!(table.capacity() > 8);

        let base_memory = table.memory_usage();
        let mut probe_addresses = Vector::with_capacity(LogicalType::BigInt, groups.size());
        let mut probe_new_groups = SelectionVector::with_capacity(groups.size());
        let second_new = table
            .find_or_create_groups(
                &groups,
                &hashes,
                &mut probe_addresses,
                &mut probe_new_groups,
            )
            .expect("second probe");
        assert_eq!(second_new, 0);
        assert_eq!(table.count(), 50);
        assert_eq!(probe_new_groups.len(), 0);
        assert!(table.memory_usage() >= base_memory);
    }

    #[test]
    fn grouped_hash_table_combines_other_table() {
        let mut left = GroupedAggregateHashTable::new(
            vec![LogicalType::Integer],
            vec![make_sum_object()],
            vec![vec![0]],
        )
        .expect("left table");
        let mut right = GroupedAggregateHashTable::new(
            vec![LogicalType::Integer],
            vec![make_sum_object()],
            vec![vec![0]],
        )
        .expect("right table");

        let left_groups = Chunk::from_vectors(vec![Vector::from_i32(&[1, 2])]);
        let left_hashes = left.hash_groups(&left_groups).expect("left hashes");
        let mut left_addresses = Vector::with_capacity(LogicalType::BigInt, left_groups.size());
        let mut left_new_groups = SelectionVector::with_capacity(left_groups.size());
        left.find_or_create_groups(
            &left_groups,
            &left_hashes,
            &mut left_addresses,
            &mut left_new_groups,
        )
        .expect("left find/create");
        let left_payload = Chunk::from_vectors(vec![Vector::from_i64(&[10, 20])]);
        left.update_aggregates(&left_payload, &left_addresses, None)
            .expect("left update");

        let right_groups = Chunk::from_vectors(vec![Vector::from_i32(&[2, 3, 2])]);
        let right_hashes = right.hash_groups(&right_groups).expect("right hashes");
        let mut right_addresses = Vector::with_capacity(LogicalType::BigInt, right_groups.size());
        let mut right_new_groups = SelectionVector::with_capacity(right_groups.size());
        right
            .find_or_create_groups(
                &right_groups,
                &right_hashes,
                &mut right_addresses,
                &mut right_new_groups,
            )
            .expect("right find/create");
        let right_payload = Chunk::from_vectors(vec![Vector::from_i64(&[7, 8, 1])]);
        right
            .update_aggregates(&right_payload, &right_addresses, None)
            .expect("right update");

        left.combine(&mut right)
            .expect("combine grouped hash tables");
        let actual = build_map_from_scan(collect_scan_rows(&mut left));
        let expected = HashMap::from([(1, 10), (2, 28), (3, 8)]);
        assert_eq!(actual, expected);
    }

    #[test]
    fn grouped_hash_table_destroy_calls_destructor() {
        reset_destructor_calls();

        let mut table = GroupedAggregateHashTable::new(
            vec![LogicalType::Integer],
            vec![make_sum_object()],
            vec![vec![0]],
        )
        .expect("create grouped hash table");

        let groups = Chunk::from_vectors(vec![Vector::from_i32(&[1, 2, 3])]);
        let hashes = table.hash_groups(&groups).expect("hash groups");
        let mut addresses = Vector::with_capacity(LogicalType::BigInt, groups.size());
        let mut new_groups = SelectionVector::with_capacity(groups.size());
        table
            .find_or_create_groups(&groups, &hashes, &mut addresses, &mut new_groups)
            .expect("find/create groups");
        let payload = Chunk::from_vectors(vec![Vector::from_i64(&[4, 5, 6])]);
        table
            .update_aggregates(&payload, &addresses, None)
            .expect("update aggregates");

        let before_destroy = table.memory_usage();
        table.destroy().expect("destroy hash table");
        assert_eq!(table.count(), 0);
        assert_eq!(destructor_calls(), 3);
        assert!(table.memory_usage() <= before_destroy);
    }
}
