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
use paro_common::vector::{SelectionVector, Vector};
use paro_function::aggregate::{
    AggregateCombineType, AggregateComparison, AggregateInputData, AggregateStateInput,
    DirectGroupedAggregateProgram, DirectGroupedAggregateScratch, ValidatedDirectGroupSlots,
};

use super::aggregate_kernel::{
    combine_states, destroy_states, filtered_input_vectors_for_aggregate, finalize_states,
    initialize_state_at_address, input_vectors_for_aggregate, update_filtered_states,
    update_states, with_aggregate_input_data, AggregatePayload,
};
use super::aggregate_object::AggregateObject;
use super::aggregate_state::AggregateStateLayout;
use super::perfect_hash_key::PerfectHashKeyDomain;

mod key_encoding;
mod parallel_merge;
mod slot_bitmap;
mod support;

use key_encoding::{
    compute_slot_from_prepared, decode_group_batch, prepare_slot_encoding, PerfectHashSlotLayout,
};
#[cfg(test)]
use key_encoding::{PreparedDictionaryKey, PreparedSlotEncoding};
pub(crate) use parallel_merge::ParallelPerfectAggregateMerge;
use slot_bitmap::SlotBitmap;
pub(crate) use support::compile_direct_update_program;
use support::{
    accounted_vec_from_reservation, bytes_to_words, compact_state_addresses,
    direct_update_scratch_bytes, pointer_vector_from_slice, validate_addresses_vector,
    validate_aggregate_inputs, validate_filter,
};

pub(crate) fn perfect_hash_occupancy_bytes(slots: usize) -> Option<usize> {
    SlotBitmap::storage_bytes(slots).ok()
}

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PerfectAggregateStateFilter {
    pub aggregate_index: usize,
    pub comparison: AggregateComparison,
    pub constant: Value,
}

#[derive(Debug)]
struct PerfectAggregatePreselection {
    filter: PerfectAggregateStateFilter,
    slots: AccountedVec<usize>,
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
    direct_update_program: Option<DirectGroupedAggregateProgram>,
    direct_update_scratch: Option<DirectGroupedAggregateScratch>,
    direct_slots: Option<AccountedVec<usize>>,
    occupancy: SlotBitmap,
    // Keep row storage 8-byte aligned so aggregate states can be safely cast to typed pointers.
    // Slots are initialized lazily when their occupancy bit transitions from
    // empty to occupied. MaybeUninit makes that lifecycle explicit and avoids
    // touching the entire direct-addressing domain up front.
    data: AccountedVec<MaybeUninit<u64>>,
    row_width: usize,
    aggregate_allocator: ArenaAllocator,
    count: usize,
    preselection: Option<PerfectAggregatePreselection>,
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
        let direct_update_program =
            compile_direct_update_program(&aggregate_objects, &aggregate_inputs, &state_layout);
        let direct_update_program = direct_update_program
            .has_updates()
            .then_some(direct_update_program);
        let row_width = state_layout.total_size().max(1);
        let total_bytes = row_width.checked_mul(total_groups).ok_or_else(|| {
            paro_error::internal(format!(
                "Perfect aggregate row storage overflow: row_width={row_width}, groups={total_groups}"
            ))
        })?;
        let total_words = bytes_to_words(total_bytes)?;
        let direct_scratch_bytes =
            direct_update_scratch_bytes(direct_update_program.as_ref(), total_groups).ok_or_else(
                || paro_error::internal("perfect aggregate scratch byte-size overflow"),
            )?;
        let occupancy_bytes = SlotBitmap::storage_bytes(total_groups)?;
        let reserved_bytes = total_words
            .checked_mul(size_of::<u64>())
            .and_then(|bytes| bytes.checked_add(occupancy_bytes))
            .and_then(|bytes| bytes.checked_add(direct_scratch_bytes))
            .ok_or_else(|| paro_error::internal("perfect aggregate reservation overflow"))?;
        let reservation = memory.reserve_grant(reserved_bytes)?;
        let direct_update_scratch = match direct_update_program.as_ref() {
            Some(program) => program.try_create_scratch_with_grant(
                total_groups,
                &reservation,
                memory.tag(),
                memory.accounting_class(),
            )?,
            None => None,
        };
        let direct_slots = match direct_update_program.as_ref() {
            Some(program) if program.handles_all() => Some(accounted_vec_from_reservation(
                &reservation,
                paro_common::vector::VECTOR_SIZE,
                MemoryTag::HashTable,
                MemoryAccountingClass::Metadata,
            )?),
            _ => None,
        };
        let mut data = accounted_vec_from_reservation(
            &reservation,
            total_words,
            MemoryTag::HashTable,
            memory.accounting_class(),
        )?;
        data.try_resize_with(total_words, MaybeUninit::uninit)?;
        let occupancy = SlotBitmap::try_from_reservation(
            total_groups,
            &reservation,
            MemoryTag::HashTable,
            MemoryAccountingClass::Metadata,
        )?;
        debug_assert_eq!(reservation.available_bytes(), 0);
        Ok(Self {
            group_domains,
            group_minima,
            slot_layout,
            state_layout,
            aggregate_objects,
            aggregate_inputs,
            direct_update_program,
            direct_update_scratch,
            direct_slots,
            occupancy,
            data,
            row_width,
            aggregate_allocator: ArenaAllocator::new(allocator),
            count: 0,
            preselection: None,
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
        addresses.try_set_count(groups.size())?;
        let address_data = unsafe { addresses.flat_data_mut::<*mut u8>() };
        if new_groups.capacity() < groups.size() {
            *new_groups =
                SelectionVector::try_with_capacity(groups.size(), groups.allocator().clone())?;
        }
        new_groups.set_len(groups.size());
        let new_group_count = self.find_or_create_groups_inner(groups, address_data, new_groups)?;
        new_groups.set_len(new_group_count);
        Ok(new_group_count)
    }

    /// Update a fully supported batch directly from perfect-hash slots.
    ///
    /// Returning `false` means no state was changed and the caller must use
    /// the address-producing aggregate ABI.
    pub fn try_update_direct_groups(&mut self, groups: &Chunk, payload: &Chunk) -> Result<bool> {
        self.validate_group_chunk(groups)?;
        if groups.size() != payload.size() {
            return Err(paro_error::internal(format!(
                "perfect aggregate group/payload cardinality mismatch: groups={}, payload={}",
                groups.size(),
                payload.size()
            )));
        }
        let Some(program) = self.direct_update_program.as_ref() else {
            return Ok(false);
        };
        if !program.handles_all() {
            return Ok(false);
        }
        let Some(prepared_input) = program.prepare_input(payload) else {
            return Ok(false);
        };

        let decoded_groups = decode_group_batch(groups, self.group_domains.len())?;
        let mut slot_encoding = prepare_slot_encoding(
            &self.group_domains,
            &self.group_minima,
            &self.slot_layout,
            &decoded_groups,
            groups.size(),
        )?;
        let direct_slots = self
            .direct_slots
            .as_mut()
            .expect("complete direct program must own slot materialization scratch");
        direct_slots.clear();
        for row in 0..groups.size() {
            direct_slots.try_push(compute_slot_from_prepared(
                &self.group_domains,
                &self.group_minima,
                &self.slot_layout,
                &decoded_groups,
                &mut slot_encoding,
                row,
            )?)?;
        }
        // SAFETY: the canonical encoder validated every materialized slot
        // against this table's immutable mixed-radix domain.
        let slots = unsafe {
            ValidatedDirectGroupSlots::from_validated_prefix(
                direct_slots,
                groups.size(),
                self.slot_layout.slot_count,
            )
        };
        let state_base = self.data.as_mut_ptr().cast::<u8>();
        let program = self
            .direct_update_program
            .as_ref()
            .expect("direct program was validated");
        let direct_update_scratch = &mut self.direct_update_scratch;
        let occupancy = &mut self.occupancy;
        let state_layout = &self.state_layout;
        let aggregate_objects = &self.aggregate_objects;
        let group_count = &mut self.count;
        let mut initialize_slot = |slot, state_ptr| {
            if !occupancy.is_set(slot) {
                // SAFETY: materialized slots prove domain membership and this
                // callback runs before reduced state is consumed.
                unsafe {
                    initialize_state_at_address(state_layout, aggregate_objects, state_ptr);
                }
                occupancy.set(slot);
                *group_count += 1;
            }
        };
        let executed = if let Some(scratch) = direct_update_scratch.as_mut() {
            unsafe {
                program.execute_reduced_slots_prepared_with_initializer(
                    &prepared_input,
                    &slots,
                    scratch,
                    state_base,
                    self.row_width,
                    &mut initialize_slot,
                )?
            }
        } else {
            unsafe {
                program.execute_run_reduced_slots_prepared(
                    &prepared_input,
                    &slots,
                    state_base,
                    self.row_width,
                    &mut initialize_slot,
                )?
            }
        };
        if !executed {
            return Err(paro_error::internal(
                "validated direct perfect aggregate batch declined execution",
            ));
        }
        Ok(true)
    }

    fn find_or_create_groups_inner(
        &mut self,
        groups: &Chunk,
        address_data: *mut *mut u8,
        new_groups: &mut SelectionVector,
    ) -> Result<usize> {
        if groups.size() == 0 {
            return Ok(0);
        }
        let decoded_groups = decode_group_batch(groups, self.group_domains.len())?;
        let mut slot_encoding = prepare_slot_encoding(
            &self.group_domains,
            &self.group_minima,
            &self.slot_layout,
            &decoded_groups,
            groups.size(),
        )?;
        let mut new_group_count = 0usize;
        for row_idx in 0..groups.size() {
            let slot = compute_slot_from_prepared(
                &self.group_domains,
                &self.group_minima,
                &self.slot_layout,
                &decoded_groups,
                &mut slot_encoding,
                row_idx,
            )?;
            let state_ptr = self.state_ptr(slot);
            unsafe { *address_data.add(row_idx) = state_ptr };
            if !self.occupancy.is_set(slot) {
                self.initialize_state(state_ptr);
                self.occupancy.set(slot);
                self.count += 1;
                new_groups.try_set(new_group_count, row_idx)?;
                new_group_count += 1;
            }
        }
        Ok(new_group_count)
    }

    /// Update aggregate states for a batch of input payload rows.
    pub fn update_aggregates(
        &mut self,
        payload: &Chunk,
        addresses: &Vector,
        filter: Option<&SelectionVector>,
    ) -> Result<()> {
        self.update_aggregates_inner(payload, addresses, filter)
    }

    fn update_aggregates_inner(
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

        if let Some(selection) = filter {
            let payload_desc = AggregatePayload {
                chunk: payload,
                aggregate_inputs: &self.aggregate_inputs,
            };
            let mut input_data = AggregateInputData::new(
                None,
                &mut self.aggregate_allocator,
                AggregateCombineType::PreserveInput,
            );
            update_filtered_states(
                &self.aggregate_objects,
                &mut input_data,
                &payload_desc,
                addresses,
                selection,
                selection.len(),
            )?;
        } else {
            if let Some(program) = self.direct_update_program.as_ref() {
                let executed = unsafe { program.execute(payload, addresses, payload.size())? };
                if executed {
                    let payload_desc = AggregatePayload {
                        chunk: payload,
                        aggregate_inputs: &self.aggregate_inputs,
                    };
                    let mut input_data = AggregateInputData::new(
                        None,
                        &mut self.aggregate_allocator,
                        AggregateCombineType::PreserveInput,
                    );
                    let program = self
                        .direct_update_program
                        .as_ref()
                        .expect("program was checked");
                    for (agg_idx, object) in self.aggregate_objects.iter().enumerate() {
                        if program.handles(agg_idx) {
                            continue;
                        }
                        let inputs = input_vectors_for_aggregate(&payload_desc, agg_idx)?;
                        let states = AggregateStateInput::try_new(
                            addresses,
                            self.state_layout.state_offset(agg_idx),
                            None,
                            payload.size(),
                        )?;
                        with_aggregate_input_data(object, &mut input_data, |aggr_input| unsafe {
                            (object.function.update)(&inputs, &aggr_input, &states, payload.size());
                        });
                    }
                    return Ok(());
                }
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
        self.preselection = None;
        self.ensure_compatible(other)?;
        if other.count == 0 {
            return Ok(());
        }

        let mut source_ptrs =
            Vec::with_capacity(self.total_groups().min(paro_common::vector::VECTOR_SIZE));
        let mut target_ptrs =
            Vec::with_capacity(self.total_groups().min(paro_common::vector::VECTOR_SIZE));
        let direct_combine_program = self
            .direct_update_program
            .as_ref()
            .filter(|program| program.supports_direct_combine())
            .cloned();

        for slot in other.occupancy.set_bits(0..self.total_groups()) {
            let source_state = other.state_ptr(slot);
            if !self.occupancy.is_set(slot) {
                let target_state = self.state_ptr(slot);
                if direct_combine_program
                    .as_ref()
                    .is_some_and(|program| program.supports_trivial_state_copy())
                {
                    // SAFETY: direct admission proves every readable byte in
                    // the row belongs to fixed-width, non-owning state. Use
                    // MaybeUninit bytes so copying struct padding is valid.
                    unsafe {
                        std::ptr::copy_nonoverlapping(
                            source_state.cast::<MaybeUninit<u8>>(),
                            target_state.cast::<MaybeUninit<u8>>(),
                            self.row_width,
                        );
                    }
                    self.occupancy.set(slot);
                    self.count += 1;
                    continue;
                }
                self.initialize_state(target_state);
                self.occupancy.set(slot);
                self.count += 1;
            }
            if let Some(program) = direct_combine_program.as_ref() {
                let combined = unsafe {
                    program.combine_direct_rows(source_state.cast_const(), self.state_ptr(slot))
                };
                debug_assert!(combined, "direct combine program declined after admission");
                continue;
            }
            source_ptrs.push(source_state);
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
        if self
            .preselection
            .as_ref()
            .is_some_and(|preselection| preselection.filter == *filter)
        {
            return self.scan_with_scratch(position, result, scratch);
        }
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
        if let Some(preselection) = self.preselection.as_ref() {
            let start = position.offset.min(preselection.slots.len());
            let end = start.saturating_add(capacity).min(preselection.slots.len());
            slots.extend_from_slice(&preselection.slots[start..end]);
            position.offset = end;
            return start != end;
        }
        let total_groups = self.total_groups();
        let mut occupied = self.occupancy.set_bits(position.offset..total_groups);
        while slots.len() < capacity {
            let Some(slot) = occupied.next() else {
                position.offset = total_groups;
                return !slots.is_empty();
            };
            slots.push(slot);
            position.offset = slot + 1;
        }
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
        self.destroy_initialized_states()?;
        self.occupancy.clear();
        self.count = 0;
        self.preselection = None;
        self.aggregate_allocator.reset();
        Ok(())
    }

    fn destroy_initialized_states(&mut self) -> Result<()> {
        if self.count == 0
            || self
                .aggregate_objects
                .iter()
                .all(|object| object.function.destructor.is_none())
        {
            return Ok(());
        }

        let mut ptrs = Vec::with_capacity(self.count);
        for slot in self.occupancy.set_bits(0..self.total_groups()) {
            ptrs.push(self.state_ptr(slot));
        }
        if ptrs.is_empty() {
            return Ok(());
        }
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
        )
    }

    pub fn memory_usage(&self) -> usize {
        self.external_accounted_memory_usage() + self.aggregate_allocator.allocation_size()
    }

    pub fn external_accounted_memory_usage(&self) -> usize {
        self.data.capacity() * size_of::<MaybeUninit<u64>>()
            + self.occupancy.capacity_bytes()
            + self.preselection.as_ref().map_or(0, |preselection| {
                preselection.slots.capacity() * size_of::<usize>()
            })
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

    fn install_preselection(
        &mut self,
        filter: PerfectAggregateStateFilter,
        slots: AccountedVec<usize>,
    ) {
        debug_assert!(slots
            .iter()
            .all(|slot| *slot < self.total_groups() && self.occupancy.is_set(*slot)));
        self.preselection = Some(PerfectAggregatePreselection { filter, slots });
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
        // Final release does not need to clear direct-address buffers or reset
        // their arena: field drops reclaim both immediately. Only aggregates
        // with an explicit destructor require a state-domain scan.
        let _ = self.destroy_initialized_states();
    }
}

#[cfg(test)]
mod tests;
