// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Perfect-hash aggregate hash table with direct array indexing.

use std::mem::{size_of, MaybeUninit};
use std::sync::Arc;

use paro_common::allocator::{Allocator, ArenaAllocator, MemoryTag};
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::memory::{AccountedVec, MemoryAccountingClass, MemoryAccountingContext};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_common::vector::{DecodedVectorRef, SelectionVector, Vector};
use paro_function::aggregate::{
    AggregateCombineType, AggregateComparison, AggregateInputData, AggregateStateInput,
};

use super::aggregate_kernel::{
    combine_states, destroy_states, filtered_input_vectors_for_aggregate, finalize_states,
    initialize_state_at_address, input_vectors_for_aggregate, update_filtered_states,
    update_states, with_aggregate_input_data, AggregatePayload,
};
use super::aggregate_object::AggregateObject;
use super::aggregate_state::AggregateStateLayout;
use super::perfect_hash_key::PerfectHashKeyDomain;

/// Scan cursor for a perfect aggregate hash table.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PerfectHTScanPosition {
    pub offset: usize,
}

#[derive(Debug)]
pub(crate) struct PerfectAggregateScanScratch {
    slots: Vec<usize>,
    state_addresses: Vector,
    aggregates: Chunk,
}

#[derive(Debug, Clone)]
pub(crate) struct PerfectAggregateStateFilter {
    pub aggregate_index: usize,
    pub comparison: AggregateComparison,
    pub constant: Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PerfectHashSlotLayout {
    cardinalities: Vec<usize>,
    strides: Vec<usize>,
    slot_count: usize,
}

impl PerfectHashSlotLayout {
    fn try_new(cardinalities: Vec<usize>) -> Result<Self> {
        let mut strides = vec![0; cardinalities.len()];
        let mut slot_count = 1usize;
        for group_idx in (0..cardinalities.len()).rev() {
            let cardinality = cardinalities[group_idx];
            if cardinality < 2 {
                return Err(paro_error::internal(format!(
                    "Invalid perfect aggregate key cardinality: group_idx={group_idx}, cardinality={cardinality}"
                )));
            }
            strides[group_idx] = slot_count;
            slot_count = slot_count.checked_mul(cardinality).ok_or_else(|| {
                paro_error::internal(format!(
                    "Perfect aggregate slot count overflow: slots={slot_count}, cardinality={cardinality}"
                ))
            })?;
        }
        Ok(Self {
            cardinalities,
            strides,
            slot_count,
        })
    }

    fn add_component(&self, slot: usize, group_idx: usize, encoded: usize) -> Result<usize> {
        let cardinality = self.cardinalities[group_idx];
        if encoded >= cardinality {
            return Err(paro_error::internal(format!(
                "Perfect aggregate encoded key exceeds domain: encoded={encoded}, cardinality={cardinality}, group_idx={group_idx}"
            )));
        }
        slot.checked_add(
            encoded
                .checked_mul(self.strides[group_idx])
                .ok_or_else(|| {
                    paro_error::internal(format!(
                        "Perfect aggregate slot component overflow: encoded={encoded}, stride={}, group_idx={group_idx}",
                        self.strides[group_idx]
                    ))
                })?,
        )
        .ok_or_else(|| {
            paro_error::internal(format!(
                "Perfect aggregate slot overflow: slot={slot}, group_idx={group_idx}"
            ))
        })
    }

    fn decode_component(&self, slot: usize, group_idx: usize) -> usize {
        (slot / self.strides[group_idx]) % self.cardinalities[group_idx]
    }
}

impl PerfectAggregateScanScratch {
    pub(crate) fn try_new(
        aggregate_types: &[LogicalType],
        capacity: usize,
        allocator: Arc<dyn Allocator>,
    ) -> Result<Self> {
        Ok(Self {
            slots: Vec::with_capacity(capacity),
            state_addresses: Vector::try_new(LogicalType::BigInt, capacity, allocator.clone())?,
            aggregates: Chunk::try_initialize(aggregate_types, capacity, allocator)?,
        })
    }
}

/// Direct-addressing aggregate table used by perfect-hash GROUP BY.
#[derive(Debug)]
pub struct PerfectAggregateHashTable {
    group_domains: Vec<PerfectHashKeyDomain>,
    group_minima: Vec<i128>,
    slot_layout: PerfectHashSlotLayout,
    state_layout: AggregateStateLayout,
    aggregate_objects: Vec<AggregateObject>,
    aggregate_inputs: Vec<Vec<usize>>,
    // 0 = empty, 1 = occupied
    occupancy: AccountedVec<u8>,
    // Keep row storage 8-byte aligned so aggregate states can be safely cast to typed pointers.
    // Slots are initialized lazily when their occupancy bit transitions from
    // empty to occupied. MaybeUninit makes that lifecycle explicit and avoids
    // touching the entire direct-addressing domain up front.
    data: AccountedVec<MaybeUninit<u64>>,
    row_width: usize,
    aggregate_allocator: ArenaAllocator,
    count: usize,
}

impl PerfectAggregateHashTable {
    pub fn new(
        group_types: Vec<LogicalType>,
        aggregate_objects: Vec<AggregateObject>,
        aggregate_inputs: Vec<Vec<usize>>,
        group_minima: Vec<i128>,
        group_cardinalities: Vec<usize>,
        allocator: Arc<dyn Allocator>,
    ) -> Result<Self> {
        Self::new_with_memory(
            group_types,
            aggregate_objects,
            aggregate_inputs,
            group_minima,
            group_cardinalities,
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
        group_minima: Vec<i128>,
        group_cardinalities: Vec<usize>,
        allocator: Arc<dyn Allocator>,
        memory: MemoryAccountingContext,
    ) -> Result<Self> {
        if group_types.is_empty() {
            return Err(paro_error::internal(
                "PerfectAggregateHashTable requires at least one group key".to_string(),
            ));
        }
        if group_types.len() != group_minima.len() || group_types.len() != group_cardinalities.len()
        {
            return Err(paro_error::internal(format!(
                "PerfectAggregateHashTable group metadata mismatch: types={} minima={} cardinalities={}",
                group_types.len(),
                group_minima.len(),
                group_cardinalities.len()
            )));
        }
        let group_domains = group_types
            .into_iter()
            .map(|ty| {
                PerfectHashKeyDomain::try_new(ty.clone()).ok_or_else(|| {
                    paro_error::internal(format!(
                        "Unsupported perfect aggregate group key type: {ty:?}"
                    ))
                })
            })
            .collect::<Result<Vec<_>>>()?;
        validate_aggregate_inputs(&aggregate_objects, &aggregate_inputs)?;

        let slot_layout = PerfectHashSlotLayout::try_new(group_cardinalities)?;
        let total_groups = slot_layout.slot_count;

        let state_layout = AggregateStateLayout::new(&aggregate_objects)?;
        let row_width = state_layout.total_size().max(1);
        let total_bytes = row_width.checked_mul(total_groups).ok_or_else(|| {
            paro_error::internal(format!(
                "Perfect aggregate row storage overflow: row_width={row_width}, groups={total_groups}"
            ))
        })?;
        let total_words = bytes_to_words(total_bytes)?;
        let mut data = accounted_vec_for_context(
            &memory,
            MemoryTag::HashTable,
            MemoryAccountingClass::Revocable,
        )?;
        data.try_resize_with(total_words, MaybeUninit::uninit)?;
        let mut occupancy = accounted_vec_for_context(
            &memory.with_class(MemoryAccountingClass::Metadata),
            MemoryTag::HashTable,
            MemoryAccountingClass::Metadata,
        )?;
        occupancy.try_resize_with(total_groups, || 0)?;
        Ok(Self {
            group_domains,
            group_minima,
            slot_layout,
            state_layout,
            aggregate_objects,
            aggregate_inputs,
            occupancy,
            data,
            row_width,
            aggregate_allocator: ArenaAllocator::new(allocator),
            count: 0,
        })
    }

    pub fn count(&self) -> usize {
        self.count
    }

    pub fn total_groups(&self) -> usize {
        self.slot_layout.slot_count
    }

    pub fn allocator(&self) -> Arc<dyn Allocator> {
        self.aggregate_allocator.get_allocator().clone()
    }

    /// Probe and insert grouped keys, returning state addresses for each input row.
    pub fn find_or_create_groups(
        &mut self,
        groups: &Chunk,
        addresses: &mut Vector,
        new_groups: &mut SelectionVector,
    ) -> Result<usize> {
        self.validate_group_chunk(groups)?;
        validate_addresses_vector(addresses, groups.size())?;

        if groups.size() == 0 {
            addresses.try_set_count(0)?;
            new_groups.set_len(0);
            return Ok(0);
        }

        let decoded_groups = (0..self.group_domains.len())
            .map(|group_idx| {
                groups
                    .column(group_idx)
                    .ok_or_else(|| {
                        paro_error::internal(format!(
                            "Missing group key column for perfect hash table: group_idx={group_idx}"
                        ))
                    })
                    .and_then(|column| column.try_decode_ref(groups.size()))
            })
            .collect::<Result<Vec<_>>>()?;

        addresses.try_set_count(groups.size())?;
        let address_data = unsafe { addresses.flat_data_mut::<*mut u8>() };
        if new_groups.capacity() < groups.size() {
            *new_groups =
                SelectionVector::try_with_capacity(groups.size(), groups.allocator().clone())?;
        }
        new_groups.set_len(groups.size());
        let mut new_group_count = 0usize;

        for row_idx in 0..groups.size() {
            let slot = self.compute_slot_from_decoded(&decoded_groups, row_idx)?;
            let state_ptr = self.state_ptr(slot);
            unsafe {
                *address_data.add(row_idx) = state_ptr;
            }
            if self.occupancy[slot] == 0 {
                self.initialize_state(state_ptr);
                self.occupancy[slot] = 1;
                self.count += 1;
                new_groups.try_set(new_group_count, row_idx)?;
                new_group_count += 1;
            }
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
                "Address vector too small for perfect aggregate update: addresses={} payload_rows={}",
                addresses.len(),
                payload.size()
            )));
        }
        if let Some(selection) = filter {
            validate_filter(selection, payload.size())?;
        }

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

    /// Combine aggregate states from another perfect hash table into this table.
    pub fn combine(&mut self, other: &mut Self) -> Result<()> {
        self.ensure_compatible(other)?;
        if other.count == 0 {
            return Ok(());
        }

        let mut source_ptrs =
            Vec::with_capacity(self.total_groups().min(paro_common::vector::VECTOR_SIZE));
        let mut target_ptrs =
            Vec::with_capacity(self.total_groups().min(paro_common::vector::VECTOR_SIZE));

        for slot in 0..self.total_groups() {
            if other.occupancy[slot] == 0 {
                continue;
            }
            if self.occupancy[slot] == 0 {
                self.initialize_state(self.state_ptr(slot));
                self.occupancy[slot] = 1;
                self.count += 1;
            }
            source_ptrs.push(other.state_ptr(slot));
            target_ptrs.push(self.state_ptr(slot));

            if source_ptrs.len() == paro_common::vector::VECTOR_SIZE {
                self.combine_pointer_batch(&source_ptrs, &target_ptrs)?;
                source_ptrs.clear();
                target_ptrs.clear();
            }
        }

        if !source_ptrs.is_empty() {
            self.combine_pointer_batch(&source_ptrs, &target_ptrs)?;
        }
        Ok(())
    }

    pub(crate) fn scan_with_scratch(
        &mut self,
        position: &mut PerfectHTScanPosition,
        result: &mut Chunk,
        scratch: &mut PerfectAggregateScanScratch,
    ) -> Result<bool> {
        if !self.collect_occupied_slots(position, result.capacity(), &mut scratch.slots) {
            result.try_set_cardinality(0)?;
            return Ok(false);
        }
        self.finalize_slots(scratch)?;
        self.write_selected_slots(result, scratch, None)?;
        Ok(true)
    }

    pub(crate) fn scan_with_aggregate_filter(
        &mut self,
        position: &mut PerfectHTScanPosition,
        result: &mut Chunk,
        scratch: &mut PerfectAggregateScanScratch,
        selection: &mut SelectionVector,
        mut select: impl FnMut(&Chunk, usize, &mut SelectionVector) -> Result<usize>,
    ) -> Result<bool> {
        loop {
            if !self.collect_occupied_slots(position, result.capacity(), &mut scratch.slots) {
                result.try_set_cardinality(0)?;
                return Ok(false);
            }
            self.finalize_slots(scratch)?;
            let selected_count = select(&scratch.aggregates, scratch.slots.len(), selection)?;
            if selected_count == 0 {
                continue;
            }
            self.write_selected_slots(result, scratch, Some(selection))?;
            return Ok(true);
        }
    }

    pub(crate) fn scan_with_state_filter(
        &mut self,
        position: &mut PerfectHTScanPosition,
        result: &mut Chunk,
        scratch: &mut PerfectAggregateScanScratch,
        selection: &mut SelectionVector,
        filter: &PerfectAggregateStateFilter,
    ) -> Result<bool> {
        let object = self
            .aggregate_objects
            .get(filter.aggregate_index)
            .ok_or_else(|| {
                paro_error::internal(format!(
                    "perfect aggregate state-filter index out of bounds: index={}, aggregates={}",
                    filter.aggregate_index,
                    self.aggregate_objects.len()
                ))
            })?;
        let state_filter = object.function.state_filter.ok_or_else(|| {
            paro_error::internal(format!(
                "aggregate {} does not implement state filtering",
                object.function.name
            ))
        })?;
        loop {
            if !self.collect_occupied_slots(position, result.capacity(), &mut scratch.slots) {
                result.try_set_cardinality(0)?;
                return Ok(false);
            }
            self.populate_state_addresses(scratch)?;
            let states = AggregateStateInput::try_new(
                &scratch.state_addresses,
                self.state_layout.state_offset(filter.aggregate_index),
                None,
                scratch.slots.len(),
            )?;
            let mut input_data = AggregateInputData::new(
                object.bind_info.as_deref(),
                &mut self.aggregate_allocator,
                AggregateCombineType::PreserveInput,
            );
            let selected = unsafe {
                state_filter(
                    &states,
                    &input_data,
                    filter.comparison,
                    &filter.constant,
                    selection,
                    scratch.slots.len(),
                )?
            };
            if selected == 0 {
                continue;
            }
            compact_state_addresses(&mut scratch.state_addresses, selection, selected)?;
            finalize_states(
                &self.aggregate_objects,
                &mut input_data,
                &scratch.state_addresses,
                &mut scratch.aggregates,
                selected,
            )?;
            self.write_state_filtered_slots(result, scratch, selection, selected)?;
            return Ok(true);
        }
    }

    fn collect_occupied_slots(
        &self,
        position: &mut PerfectHTScanPosition,
        capacity: usize,
        slots: &mut Vec<usize>,
    ) -> bool {
        slots.clear();
        let mut cursor = position.offset;
        while cursor < self.total_groups() && slots.len() < capacity {
            if self.occupancy[cursor] != 0 {
                slots.push(cursor);
            }
            cursor += 1;
        }
        position.offset = cursor;
        !slots.is_empty()
    }

    fn finalize_slots(&mut self, scratch: &mut PerfectAggregateScanScratch) -> Result<()> {
        let count = scratch.slots.len();
        self.populate_state_addresses(scratch)?;
        let mut input_data = AggregateInputData::new(
            None,
            &mut self.aggregate_allocator,
            AggregateCombineType::PreserveInput,
        );
        finalize_states(
            &self.aggregate_objects,
            &mut input_data,
            &scratch.state_addresses,
            &mut scratch.aggregates,
            count,
        )
    }

    fn populate_state_addresses(&self, scratch: &mut PerfectAggregateScanScratch) -> Result<()> {
        scratch.state_addresses.try_set_count(scratch.slots.len())?;
        // SAFETY: the address vector has one pointer-width slot per selected
        // group and every occupied slot owns initialized aggregate state.
        unsafe {
            let addresses = scratch.state_addresses.flat_data_mut::<*mut u8>();
            for (row_idx, &slot) in scratch.slots.iter().enumerate() {
                *addresses.add(row_idx) = self.state_ptr(slot);
            }
        }
        Ok(())
    }

    fn write_selected_slots(
        &self,
        result: &mut Chunk,
        scratch: &PerfectAggregateScanScratch,
        selection: Option<&SelectionVector>,
    ) -> Result<()> {
        let count = selection.map_or(scratch.slots.len(), SelectionVector::len);
        result.try_set_cardinality(count)?;
        for group_idx in 0..self.group_domains.len() {
            let target = result.column_mut(group_idx).ok_or_else(|| {
                paro_error::internal(format!(
                    "Missing group output column {group_idx} in perfect aggregate scan"
                ))
            })?;
            target.try_set_count(count)?;
            for output_row in 0..count {
                let source_row = selection.map_or(output_row, |sel| sel.get(output_row));
                let slot = scratch.slots[source_row];
                match self.decode_group_value(slot, group_idx)? {
                    Some(value) => target.set_value(output_row, &value),
                    None => target.try_set_null(output_row, true)?,
                }
            }
        }
        for aggregate_idx in 0..self.aggregate_objects.len() {
            let source = scratch.aggregates.column(aggregate_idx).ok_or_else(|| {
                paro_error::internal(format!(
                    "Missing finalized aggregate scratch column {aggregate_idx}"
                ))
            })?;
            let target_idx = self.group_domains.len() + aggregate_idx;
            let target = result.column_mut(target_idx).ok_or_else(|| {
                paro_error::internal(format!(
                    "Missing aggregate output column {target_idx} in perfect aggregate scan"
                ))
            })?;
            if let Some(selection) = selection {
                target.try_copy_selection(
                    0,
                    source.as_ref(),
                    &paro_common::vector::VectorSelection::from(selection),
                    count,
                )?;
            } else {
                target.try_copy_range(0, source.as_ref(), 0, count)?;
            }
        }
        Ok(())
    }

    fn write_state_filtered_slots(
        &self,
        result: &mut Chunk,
        scratch: &PerfectAggregateScanScratch,
        selection: &SelectionVector,
        count: usize,
    ) -> Result<()> {
        result.try_set_cardinality(count)?;
        for group_idx in 0..self.group_domains.len() {
            let target = result.column_mut(group_idx).ok_or_else(|| {
                paro_error::internal(format!(
                    "Missing group output column {group_idx} in perfect aggregate state filter"
                ))
            })?;
            target.try_set_count(count)?;
            for output_row in 0..count {
                let slot = scratch.slots[selection.get(output_row)];
                match self.decode_group_value(slot, group_idx)? {
                    Some(value) => target.set_value(output_row, &value),
                    None => target.try_set_null(output_row, true)?,
                }
            }
        }
        for aggregate_idx in 0..self.aggregate_objects.len() {
            let source = scratch.aggregates.column(aggregate_idx).ok_or_else(|| {
                paro_error::internal(format!(
                    "Missing state-filter aggregate scratch column {aggregate_idx}"
                ))
            })?;
            let target_idx = self.group_domains.len() + aggregate_idx;
            result
                .column_mut(target_idx)
                .ok_or_else(|| {
                    paro_error::internal(format!(
                        "Missing state-filter aggregate output column {target_idx}"
                    ))
                })?
                .try_copy_range(0, source.as_ref(), 0, count)?;
        }
        Ok(())
    }

    pub fn destroy(&mut self) -> Result<()> {
        if self.count == 0 || self.aggregate_objects.is_empty() {
            self.occupancy.as_mut_slice().fill(0);
            self.count = 0;
            self.aggregate_allocator.reset();
            return Ok(());
        }

        let mut ptrs = Vec::with_capacity(self.count);
        for slot in 0..self.total_groups() {
            if self.occupancy[slot] != 0 {
                ptrs.push(self.state_ptr(slot));
            }
        }

        if !ptrs.is_empty() {
            let addresses =
                pointer_vector_from_slice(&ptrs, self.aggregate_allocator.get_allocator().clone())?;
            let mut input_data = AggregateInputData::new(
                None,
                &mut self.aggregate_allocator,
                AggregateCombineType::PreserveInput,
            );
            destroy_states(
                &self.aggregate_objects,
                &mut input_data,
                &addresses,
                ptrs.len(),
            )?;
        }

        self.occupancy.as_mut_slice().fill(0);
        self.count = 0;
        self.aggregate_allocator.reset();
        Ok(())
    }

    pub fn memory_usage(&self) -> usize {
        self.external_accounted_memory_usage() + self.aggregate_allocator.allocation_size()
    }

    pub fn external_accounted_memory_usage(&self) -> usize {
        self.data.capacity() * size_of::<MaybeUninit<u64>>()
            + self.occupancy.capacity() * size_of::<u8>()
    }

    pub fn reclaimable_finalized_memory(&self) -> usize {
        0
    }

    pub fn reclaim_finalized_memory(&mut self, _target_bytes: usize) -> usize {
        0
    }

    fn combine_pointer_batch(
        &mut self,
        source_ptrs: &[*mut u8],
        target_ptrs: &[*mut u8],
    ) -> Result<()> {
        debug_assert_eq!(source_ptrs.len(), target_ptrs.len());
        if source_ptrs.is_empty() {
            return Ok(());
        }
        let source = pointer_vector_from_slice(
            source_ptrs,
            self.aggregate_allocator.get_allocator().clone(),
        )?;
        let target = pointer_vector_from_slice(
            target_ptrs,
            self.aggregate_allocator.get_allocator().clone(),
        )?;
        let mut input_data = AggregateInputData::new(
            None,
            &mut self.aggregate_allocator,
            AggregateCombineType::AllowDestructive,
        );
        combine_states(
            &self.aggregate_objects,
            &mut input_data,
            &source,
            &target,
            source_ptrs.len(),
        )
    }

    fn compute_slot_from_decoded(
        &self,
        decoded_groups: &[DecodedVectorRef<'_>],
        row_idx: usize,
    ) -> Result<usize> {
        let mut slot = 0usize;
        for group_idx in 0..self.group_domains.len() {
            let encoded =
                self.encoded_group_value(&decoded_groups[group_idx], row_idx, group_idx)?;
            slot = self.slot_layout.add_component(slot, group_idx, encoded)?;
        }
        if slot >= self.total_groups() {
            return Err(paro_error::internal(format!(
                "Perfect aggregate slot out of bounds: slot={slot}, total_groups={}",
                self.total_groups()
            )));
        }
        Ok(slot)
    }

    fn encoded_group_value(
        &self,
        decoded_group: &DecodedVectorRef<'_>,
        row_idx: usize,
        group_idx: usize,
    ) -> Result<usize> {
        let physical_idx = decoded_group.physical_index(row_idx);
        if !decoded_group.validity().is_valid(physical_idx) {
            return Ok(0);
        }

        let value = self.group_domains[group_idx].encode_decoded(decoded_group, physical_idx)?;
        let min_value = self.group_minima[group_idx];
        let adjusted = value
            .checked_sub(min_value)
            .and_then(|delta| delta.checked_add(1))
            .ok_or_else(|| {
                paro_error::internal(format!(
                    "Perfect aggregate adjusted key overflow: value={value}, min={min_value}, group_idx={group_idx}"
                ))
            })?;
        if adjusted <= 0 {
            return Err(paro_error::internal(format!(
                "Perfect aggregate key smaller than expected minimum: value={value}, min={min_value}, group_idx={group_idx}"
            )));
        }
        let cardinality = self.slot_layout.cardinalities[group_idx];
        let adjusted_u128 = u128::try_from(adjusted).map_err(|_| {
            paro_error::internal(format!(
                "Perfect aggregate adjusted key conversion failed: adjusted={adjusted}, group_idx={group_idx}"
            ))
        })?;
        if adjusted_u128 >= cardinality as u128 {
            return Err(paro_error::internal(format!(
                "Perfect aggregate key exceeds planned range: adjusted={adjusted_u128}, cardinality={cardinality}, group_idx={group_idx}"
            )));
        }
        usize::try_from(adjusted_u128).map_err(|_| {
            paro_error::internal(format!(
                "Perfect aggregate adjusted key exceeds usize: adjusted={adjusted_u128}, group_idx={group_idx}"
            ))
        })
    }

    fn decode_group_value(&self, slot: usize, group_idx: usize) -> Result<Option<Value>> {
        let encoded = self.slot_layout.decode_component(slot, group_idx);
        if encoded == 0 {
            return Ok(None);
        }

        let value = self.group_minima[group_idx]
            .checked_add(i128::try_from(encoded).map_err(|_| {
                paro_error::internal(format!(
                    "Failed to decode perfect aggregate key: encoded={encoded}, group_idx={group_idx}"
                ))
            })?)
            .and_then(|v| v.checked_sub(1))
            .ok_or_else(|| {
                paro_error::internal(format!(
                    "Perfect aggregate decoded value overflow: encoded={encoded}, min={}, group_idx={group_idx}",
                    self.group_minima[group_idx]
                ))
            })?;
        self.group_domains[group_idx]
            .value_from_encoded(value)
            .map(Some)
    }

    fn initialize_state(&self, state_ptr: *mut u8) {
        // SAFETY: callers only pass an unoccupied slot backed by one full
        // `state_layout` row, before publishing occupancy to consumers.
        unsafe {
            initialize_state_at_address(&self.state_layout, &self.aggregate_objects, state_ptr)
        };
    }

    fn validate_group_chunk(&self, groups: &Chunk) -> Result<()> {
        if groups.column_count() != self.group_domains.len() {
            return Err(paro_error::internal(format!(
                "Group key column count mismatch for perfect aggregate table: expected={}, actual={}",
                self.group_domains.len(),
                groups.column_count()
            )));
        }
        for group_idx in 0..self.group_domains.len() {
            let group_type = groups
                .column(group_idx)
                .ok_or_else(|| {
                    paro_error::internal(format!("Missing group column at index {group_idx}"))
                })?
                .logical_type()
                .clone();
            if &group_type != self.group_domains[group_idx].logical_type() {
                return Err(paro_error::internal(format!(
                    "Group key type mismatch for perfect aggregate table at index {group_idx}: expected={:?}, actual={:?}",
                    self.group_domains[group_idx].logical_type(), group_type
                )));
            }
        }
        Ok(())
    }

    fn ensure_compatible(&self, other: &Self) -> Result<()> {
        if self.group_domains != other.group_domains
            || self.group_minima != other.group_minima
            || self.slot_layout != other.slot_layout
            || self.state_layout.total_size() != other.state_layout.total_size()
        {
            return Err(paro_error::internal(
                "Cannot combine incompatible perfect aggregate hash tables".to_string(),
            ));
        }
        if self.aggregate_objects.len() != other.aggregate_objects.len() {
            return Err(paro_error::internal(format!(
                "Cannot combine perfect aggregate hash tables with different aggregate counts: left={}, right={}",
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
                    "Aggregate object mismatch at index {idx} while combining perfect hash tables"
                )));
            }
        }
        Ok(())
    }

    #[inline]
    fn state_ptr(&self, slot: usize) -> *mut u8 {
        debug_assert!(slot < self.total_groups());
        let offset = slot * self.row_width;
        // SAFETY: `data` reserves `total_groups * row_width` bytes (rounded up
        // to u64 words), and `slot` is in range. The returned MaybeUninit
        // storage is written by every aggregate initializer before an occupied
        // slot can be consumed, combined, finalized, or destroyed.
        unsafe { (self.data.as_ptr() as *mut u8).add(offset) }
    }
}

impl Drop for PerfectAggregateHashTable {
    fn drop(&mut self) {
        let _ = self.destroy();
    }
}

fn compact_state_addresses(
    addresses: &mut Vector,
    selection: &SelectionVector,
    count: usize,
) -> Result<()> {
    if count > selection.len() || selection.len() > addresses.len() {
        return Err(paro_error::internal(format!(
            "invalid aggregate state-filter compaction: selected={count}, selection={}, addresses={}",
            selection.len(),
            addresses.len()
        )));
    }
    // Selection indices are monotonically increasing, so forward in-place
    // compaction never overwrites a source pointer before it is read.
    unsafe {
        let data = addresses.flat_data_mut::<*mut u8>();
        for output_row in 0..count {
            *data.add(output_row) = *data.add(selection.get(output_row));
        }
    }
    addresses.try_set_count(count)
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

fn pointer_vector_from_slice(
    ptrs: &[*mut u8],
    allocator: Arc<dyn paro_common::allocator::Allocator>,
) -> Result<Vector> {
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

fn bytes_to_words(bytes: usize) -> Result<usize> {
    let word = size_of::<u64>();
    let words = bytes.checked_add(word - 1).ok_or_else(|| {
        paro_error::internal(format!(
            "Perfect aggregate row storage byte-size overflow: bytes={bytes}"
        ))
    })?;
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

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::PerfectHashSlotLayout;

    #[test]
    fn mixed_radix_layout_is_dense_and_round_trips_every_component() {
        let layout = PerfectHashSlotLayout::try_new(vec![3, 5, 2]).expect("layout");
        assert_eq!(layout.strides, vec![10, 2, 1]);
        assert_eq!(layout.slot_count, 30);

        let mut slots = HashSet::new();
        for first in 0..3 {
            for second in 0..5 {
                for third in 0..2 {
                    let mut slot = 0;
                    for (group_idx, encoded) in [first, second, third].into_iter().enumerate() {
                        slot = layout
                            .add_component(slot, group_idx, encoded)
                            .expect("component");
                    }
                    assert!(slots.insert(slot));
                    assert_eq!(layout.decode_component(slot, 0), first);
                    assert_eq!(layout.decode_component(slot, 1), second);
                    assert_eq!(layout.decode_component(slot, 2), third);
                }
            }
        }
        assert_eq!(slots.len(), layout.slot_count);
    }

    #[test]
    fn mixed_radix_layout_rejects_invalid_domains_and_components() {
        assert!(PerfectHashSlotLayout::try_new(vec![1]).is_err());
        let layout = PerfectHashSlotLayout::try_new(vec![3]).expect("layout");
        assert!(layout.add_component(0, 0, 3).is_err());
    }
}
