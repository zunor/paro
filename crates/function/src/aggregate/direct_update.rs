// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Precompiled grouped aggregate update programs.

use paro_common::allocator::MemoryTag;
use paro_common::chunk::Chunk;
use paro_common::error::Result;
use paro_common::memory::{AccountedVec, MemoryAccountingClass, MemoryAccountingContext};
use paro_common::vector::{DataRef, DecodedVectorRef, SelectionRef, Vector, VECTOR_SIZE};
use smallvec::SmallVec;

use super::distributive::decimal::{DecimalAverageState, DecimalNarrowState};
use super::{AggregateDirectUpdate, AggregateStateInput};

const DIRECT_SLOT_SCAN_LIMIT: usize = 64;

#[derive(Debug, Clone)]
struct DirectDecimalInputUpdates {
    input_index: usize,
    sums: Vec<usize>,
    averages: Vec<usize>,
}

/// Fused update program compiled from explicit aggregate capabilities.
///
/// The program resolves a group state row once, loads each shared input once,
/// and applies compatible updates before advancing. Unsupported vector shapes
/// decline before state mutation so the normal aggregate ABI remains the
/// complete fallback.
#[derive(Debug, Clone)]
pub struct DirectGroupedAggregateProgram {
    decimal_inputs: Vec<DirectDecimalInputUpdates>,
    count_star_offsets: Vec<usize>,
    handled: Vec<bool>,
    update_count: usize,
}

/// Batch-local accumulators for a small direct-addressing group domain.
///
/// The scratch is fully grant-accounted and bounded to one vector of group
/// slots. Larger domains keep the row-at-a-time direct update path rather than
/// paying to initialize sparse temporary state.
#[derive(Debug)]
pub struct DirectGroupedAggregateScratch {
    slot_count: usize,
    source_count: usize,
    totals: AccountedVec<i128>,
    narrow_totals: AccountedVec<i64>,
    row_counts: AccountedVec<usize>,
    first_rows: AccountedVec<usize>,
    touched_slots: AccountedVec<usize>,
}

impl DirectGroupedAggregateProgram {
    pub fn new(aggregate_count: usize) -> Self {
        Self {
            decimal_inputs: Vec::new(),
            count_star_offsets: Vec::new(),
            handled: vec![false; aggregate_count],
            update_count: 0,
        }
    }

    pub fn try_add(
        &mut self,
        aggregate_index: usize,
        update: Option<AggregateDirectUpdate>,
        state_offset: usize,
        input_index: Option<usize>,
    ) -> bool {
        let Some(update) = update else {
            return false;
        };
        let Some(handled) = self.handled.get_mut(aggregate_index) else {
            return false;
        };
        if *handled {
            return false;
        }
        match update {
            AggregateDirectUpdate::CountStar if input_index.is_none() => {
                self.count_star_offsets.push(state_offset);
            }
            AggregateDirectUpdate::DecimalSumI64 | AggregateDirectUpdate::DecimalAverageI64 => {
                let Some(input_index) = input_index else {
                    return false;
                };
                let source = if let Some(source) = self
                    .decimal_inputs
                    .iter_mut()
                    .find(|source| source.input_index == input_index)
                {
                    source
                } else {
                    self.decimal_inputs.push(DirectDecimalInputUpdates {
                        input_index,
                        sums: Vec::new(),
                        averages: Vec::new(),
                    });
                    self.decimal_inputs
                        .last_mut()
                        .expect("source was just inserted")
                };
                match update {
                    AggregateDirectUpdate::DecimalSumI64 => source.sums.push(state_offset),
                    AggregateDirectUpdate::DecimalAverageI64 => source.averages.push(state_offset),
                    AggregateDirectUpdate::CountStar => return false,
                }
            }
            AggregateDirectUpdate::CountStar => return false,
        }
        *handled = true;
        self.update_count += 1;
        true
    }

    pub fn is_worthwhile(&self) -> bool {
        self.update_count >= 2
    }

    pub fn handles(&self, aggregate_index: usize) -> bool {
        self.handled.get(aggregate_index).copied().unwrap_or(false)
    }

    pub fn handles_all(&self) -> bool {
        self.update_count == self.handled.len()
    }

    /// Validate and decode every direct input before grouped state is mutated.
    ///
    /// Execution can retain this opaque view across group lookup, avoiding a
    /// second decode while preserving the decline-before-mutation contract.
    pub fn prepare_input<'a>(
        &self,
        payload: &'a Chunk,
    ) -> Option<PreparedDirectGroupedAggregateInput<'a>> {
        self.prepare_inputs(payload)
    }

    /// Upper bound used by perfect-hash admission before the exact direct
    /// update program has been constructed.
    pub fn conservative_scratch_bytes(aggregate_count: usize, slot_count: usize) -> Option<usize> {
        if slot_count > VECTOR_SIZE {
            return None;
        }
        let bytes_per_slot = aggregate_count
            .checked_mul(std::mem::size_of::<i128>().checked_add(std::mem::size_of::<i64>())?)?
            .checked_add(2 * std::mem::size_of::<usize>())?;
        bytes_per_slot
            .checked_mul(slot_count)?
            .checked_add(slot_count.checked_mul(std::mem::size_of::<usize>())?)
    }

    pub fn try_create_scratch(
        &self,
        slot_count: usize,
        memory: &MemoryAccountingContext,
    ) -> Result<Option<DirectGroupedAggregateScratch>> {
        if slot_count > VECTOR_SIZE || self.decimal_inputs.is_empty() {
            return Ok(None);
        }
        let source_count = self.decimal_inputs.len();
        let total_count = source_count.checked_mul(slot_count).ok_or_else(|| {
            paro_common::error::internal(format!(
                "direct aggregate scratch size overflow: sources={source_count}, slots={slot_count}"
            ))
        })?;
        let mut totals = accounted_scratch_vec::<i128>(memory)?;
        totals.try_resize_with(total_count, || 0)?;
        let mut narrow_totals = accounted_scratch_vec::<i64>(memory)?;
        narrow_totals.try_resize_with(total_count, || 0)?;
        let mut row_counts = accounted_scratch_vec::<usize>(memory)?;
        row_counts.try_resize_with(slot_count, || 0)?;
        let mut first_rows = accounted_scratch_vec::<usize>(memory)?;
        first_rows.try_resize_with(slot_count, || 0)?;
        let mut touched_slots = accounted_scratch_vec::<usize>(memory)?;
        touched_slots.try_reserve(slot_count)?;
        Ok(Some(DirectGroupedAggregateScratch {
            slot_count,
            source_count,
            totals,
            narrow_totals,
            row_counts,
            first_rows,
            touched_slots,
        }))
    }

    /// Execute over initialized grouped aggregate states.
    ///
    /// Returns `false` without modifying states when an input does not have
    /// the direct all-valid i64 DECIMAL shape required by the program.
    ///
    /// # Safety
    ///
    /// Every address must identify a live state row whose offsets match those
    /// used to compile the program.
    pub unsafe fn execute(
        &self,
        payload: &Chunk,
        addresses: &Vector,
        count: usize,
    ) -> Result<bool> {
        let Some(states) = AggregateStateInput::try_new(addresses, 0, None, count)?.direct_cursor()
        else {
            return Ok(false);
        };
        let Some(inputs) = self.prepare_inputs(payload) else {
            return Ok(false);
        };

        for row in 0..count {
            let base = unsafe { states.state_ptr(row) };
            let shared_physical_row = inputs.shared_physical_row(row);
            for &state_offset in &self.count_star_offsets {
                unsafe { *base.add(state_offset).cast::<i64>() += 1 };
            }
            for (source_idx, source) in self.decimal_inputs.iter().enumerate() {
                let value = unsafe { inputs.value(source_idx, row, shared_physical_row) };
                for &state_offset in &source.sums {
                    let state =
                        unsafe { &mut *base.add(state_offset).cast::<DecimalNarrowState>() };
                    state.add_i64(value);
                }
                for &state_offset in &source.averages {
                    let state =
                        unsafe { &mut *base.add(state_offset).cast::<DecimalAverageState>() };
                    state.update_direct_i64(value);
                }
            }
        }
        Ok(true)
    }

    /// Collapse a batch by perfect-hash slot before touching aggregate state.
    ///
    /// This is profitable only for small direct-addressing domains: each
    /// input value is accumulated in an i128 batch cell and each aggregate
    /// state is updated once per touched slot.
    ///
    /// # Safety
    ///
    /// Every address must identify a live state row whose offsets match those
    /// used to compile the program. Each slot must be within `scratch`.
    pub unsafe fn execute_reduced(
        &self,
        payload: &Chunk,
        addresses: &Vector,
        slots: &[usize],
        count: usize,
        scratch: &mut DirectGroupedAggregateScratch,
    ) -> Result<bool> {
        if scratch.source_count != self.decimal_inputs.len() || slots.len() < count {
            return Ok(false);
        }
        let Some(states) = AggregateStateInput::try_new(addresses, 0, None, count)?.direct_cursor()
        else {
            return Ok(false);
        };
        let Some(inputs) = self.prepare_inputs(payload) else {
            return Ok(false);
        };
        let narrow = self.reduce_inputs::<true>(&inputs, slots, count, scratch)?;
        for touched_idx in 0..scratch.touched_slots.len() {
            let slot = scratch.touched_slots[touched_idx];
            let base = unsafe { states.state_ptr(scratch.first_rows[slot]) };
            self.apply_reduced_slot(base, slot, scratch, narrow);
        }
        scratch.touched_slots.clear();
        Ok(true)
    }

    /// Collapse a batch by slot and update states in direct-addressing storage.
    ///
    /// # Safety
    ///
    /// `state_base + slot * state_stride` must identify initialized aggregate
    /// state rows for every supplied slot. Every value in `slots[..count]`
    /// must be smaller than `scratch.slot_count`.
    pub unsafe fn execute_reduced_slots_prepared(
        &self,
        inputs: &PreparedDirectGroupedAggregateInput<'_>,
        slots: &[usize],
        count: usize,
        scratch: &mut DirectGroupedAggregateScratch,
        state_base: *mut u8,
        state_stride: usize,
    ) -> Result<bool> {
        if scratch.source_count != self.decimal_inputs.len() || slots.len() < count {
            return Ok(false);
        }
        let scan_direct_domain = scratch.slot_count <= DIRECT_SLOT_SCAN_LIMIT;
        let narrow = if scan_direct_domain {
            self.reduce_inputs::<false>(inputs, slots, count, scratch)?
        } else {
            self.reduce_inputs::<true>(inputs, slots, count, scratch)?
        };
        if scan_direct_domain {
            for slot in 0..scratch.slot_count {
                if scratch.row_counts[slot] != 0 {
                    let base = unsafe { state_base.add(slot * state_stride) };
                    self.apply_reduced_slot(base, slot, scratch, narrow);
                }
            }
        } else {
            for touched_idx in 0..scratch.touched_slots.len() {
                let slot = scratch.touched_slots[touched_idx];
                let base = unsafe { state_base.add(slot * state_stride) };
                self.apply_reduced_slot(base, slot, scratch, narrow);
            }
        }
        scratch.touched_slots.clear();
        Ok(true)
    }

    fn reduce_inputs<const TRACK_TOUCHED: bool>(
        &self,
        inputs: &PreparedDirectGroupedAggregateInput<'_>,
        slots: &[usize],
        count: usize,
        scratch: &mut DirectGroupedAggregateScratch,
    ) -> Result<bool> {
        let mut overflowed = 0u64;
        for (row, &slot) in slots.iter().take(count).enumerate() {
            if TRACK_TOUCHED && slot >= scratch.slot_count {
                return Err(paro_common::error::internal(format!(
                    "direct aggregate slot out of bounds: slot={slot}, slots={}",
                    scratch.slot_count
                )));
            }
            if TRACK_TOUCHED && scratch.row_counts[slot] == 0 {
                scratch.first_rows[slot] = row;
                scratch.touched_slots.try_push(slot)?;
            }
            scratch.row_counts[slot] += 1;
            let shared_physical_row = inputs.shared_physical_row(row);
            match inputs.len() {
                1 => unsafe {
                    overflowed |=
                        accumulate_narrow_input(inputs, 0, row, shared_physical_row, slot, scratch)
                },
                2 => unsafe {
                    overflowed |=
                        accumulate_narrow_input(inputs, 0, row, shared_physical_row, slot, scratch);
                    overflowed |=
                        accumulate_narrow_input(inputs, 1, row, shared_physical_row, slot, scratch);
                },
                3 => unsafe {
                    overflowed |=
                        accumulate_narrow_input(inputs, 0, row, shared_physical_row, slot, scratch);
                    overflowed |=
                        accumulate_narrow_input(inputs, 1, row, shared_physical_row, slot, scratch);
                    overflowed |=
                        accumulate_narrow_input(inputs, 2, row, shared_physical_row, slot, scratch);
                },
                4 => unsafe {
                    overflowed |=
                        accumulate_narrow_input(inputs, 0, row, shared_physical_row, slot, scratch);
                    overflowed |=
                        accumulate_narrow_input(inputs, 1, row, shared_physical_row, slot, scratch);
                    overflowed |=
                        accumulate_narrow_input(inputs, 2, row, shared_physical_row, slot, scratch);
                    overflowed |=
                        accumulate_narrow_input(inputs, 3, row, shared_physical_row, slot, scratch);
                },
                5 => unsafe {
                    overflowed |=
                        accumulate_narrow_input(inputs, 0, row, shared_physical_row, slot, scratch);
                    overflowed |=
                        accumulate_narrow_input(inputs, 1, row, shared_physical_row, slot, scratch);
                    overflowed |=
                        accumulate_narrow_input(inputs, 2, row, shared_physical_row, slot, scratch);
                    overflowed |=
                        accumulate_narrow_input(inputs, 3, row, shared_physical_row, slot, scratch);
                    overflowed |=
                        accumulate_narrow_input(inputs, 4, row, shared_physical_row, slot, scratch);
                },
                _ => {
                    for source_idx in 0..inputs.len() {
                        overflowed |= unsafe {
                            accumulate_narrow_input(
                                inputs,
                                source_idx,
                                row,
                                shared_physical_row,
                                slot,
                                scratch,
                            )
                        };
                    }
                }
            }
        }
        if overflowed == 0 {
            return Ok(true);
        }

        if TRACK_TOUCHED {
            for &slot in scratch.touched_slots.iter() {
                for source in 0..inputs.len() {
                    scratch.narrow_totals[source * scratch.slot_count + slot] = 0;
                }
            }
        } else {
            for slot in 0..scratch.slot_count {
                if scratch.row_counts[slot] != 0 {
                    for source in 0..inputs.len() {
                        scratch.narrow_totals[source * scratch.slot_count + slot] = 0;
                    }
                }
            }
        }
        for (row, &slot) in slots.iter().take(count).enumerate() {
            let shared_physical_row = inputs.shared_physical_row(row);
            for source in 0..inputs.len() {
                let total = source * scratch.slot_count + slot;
                scratch.totals[total] +=
                    i128::from(unsafe { inputs.value(source, row, shared_physical_row) });
            }
        }
        Ok(false)
    }

    fn prepare_inputs<'a>(
        &self,
        payload: &'a Chunk,
    ) -> Option<PreparedDirectGroupedAggregateInput<'a>> {
        let mut decoded = SmallVec::<[DecodedVectorRef<'a>; 8]>::new();
        for source in &self.decimal_inputs {
            let vector = payload.column(source.input_index)?;
            let input = vector.try_decode_ref(payload.size()).ok()?;
            if !input.validity().all_valid() || !matches!(input.data(), DataRef::Ptr(_)) {
                return None;
            }
            decoded.push(input);
        }
        Some(PreparedDirectGroupedAggregateInput::new(decoded))
    }

    fn apply_reduced_slot(
        &self,
        base: *mut u8,
        slot: usize,
        scratch: &mut DirectGroupedAggregateScratch,
        narrow: bool,
    ) {
        let row_count = scratch.row_counts[slot];
        for &state_offset in &self.count_star_offsets {
            unsafe { *base.add(state_offset).cast::<i64>() += row_count as i64 };
        }
        for (source_idx, source) in self.decimal_inputs.iter().enumerate() {
            let total_idx = source_idx * scratch.slot_count + slot;
            if narrow {
                let value = scratch.narrow_totals[total_idx];
                for &state_offset in &source.sums {
                    let state =
                        unsafe { &mut *base.add(state_offset).cast::<DecimalNarrowState>() };
                    state.add_i64(value);
                }
                for &state_offset in &source.averages {
                    let state =
                        unsafe { &mut *base.add(state_offset).cast::<DecimalAverageState>() };
                    state.update_direct_i64_sum(value, row_count as u64);
                }
                scratch.narrow_totals[total_idx] = 0;
            } else {
                let value = scratch.totals[total_idx];
                for &state_offset in &source.sums {
                    let state =
                        unsafe { &mut *base.add(state_offset).cast::<DecimalNarrowState>() };
                    state.add_direct_i128(value);
                }
                for &state_offset in &source.averages {
                    let state =
                        unsafe { &mut *base.add(state_offset).cast::<DecimalAverageState>() };
                    state.update_direct_i128(value, row_count as u64);
                }
                scratch.totals[total_idx] = 0;
            }
        }
        scratch.row_counts[slot] = 0;
    }
}

struct PreparedDecimalInput<'a> {
    decoded: DecodedVectorRef<'a>,
    data: *const i64,
    direct: bool,
    uses_shared_selection: bool,
}

pub struct PreparedDirectGroupedAggregateInput<'a> {
    inputs: SmallVec<[PreparedDecimalInput<'a>; 8]>,
    shared_selection_input: Option<usize>,
    shared_selection_data: *const u32,
}

impl<'a> PreparedDirectGroupedAggregateInput<'a> {
    fn new(decoded: SmallVec<[DecodedVectorRef<'a>; 8]>) -> Self {
        let shared_identity = decoded
            .iter()
            .find_map(|input| input.sel().allocation_identity());
        let shared_selection_input = shared_identity.and_then(|identity| {
            decoded
                .iter()
                .position(|input| input.sel().allocation_identity() == Some(identity))
        });
        let inputs: SmallVec<[PreparedDecimalInput<'a>; 8]> = decoded
            .into_iter()
            .map(|decoded| PreparedDecimalInput {
                data: decoded.get_data::<i64>(),
                direct: matches!(decoded.sel(), SelectionRef::Incremental { .. }),
                uses_shared_selection: shared_identity
                    .is_some_and(|identity| decoded.sel().allocation_identity() == Some(identity)),
                decoded,
            })
            .collect();
        let shared_selection_data = shared_selection_input
            .and_then(|input| decoded_selection_data(&inputs[input].decoded))
            .unwrap_or(std::ptr::null());
        Self {
            inputs,
            shared_selection_input,
            shared_selection_data,
        }
    }

    fn len(&self) -> usize {
        self.inputs.len()
    }

    #[inline(always)]
    fn shared_physical_row(&self, row: usize) -> usize {
        if !self.shared_selection_data.is_null() {
            return unsafe { *self.shared_selection_data.add(row) as usize };
        }
        self.shared_selection_input
            .map_or(row, |input| self.inputs[input].decoded.physical_index(row))
    }

    /// # Safety
    ///
    /// `source`, `row`, and `shared_physical_row` must identify rows within the
    /// prepared input batch.
    #[inline(always)]
    unsafe fn value(&self, source: usize, row: usize, shared_physical_row: usize) -> i64 {
        let input = unsafe { self.inputs.get_unchecked(source) };
        if input.uses_shared_selection {
            unsafe { *input.data.add(shared_physical_row) }
        } else if input.direct {
            unsafe { *input.data.add(row) }
        } else {
            unsafe { *input.data.add(input.decoded.physical_index(row)) }
        }
    }
}

fn decoded_selection_data(decoded: &DecodedVectorRef<'_>) -> Option<*const u32> {
    decoded.sel().materialized_indices().map(<[u32]>::as_ptr)
}

/// Accumulate one prepared input into its slot-local batch state.
///
/// # Safety
///
/// The source, logical row, shared physical row, slot, and scratch dimensions
/// must all have been validated by `reduce_inputs` and `prepare_inputs`.
#[inline(always)]
unsafe fn accumulate_narrow_input(
    inputs: &PreparedDirectGroupedAggregateInput<'_>,
    source: usize,
    row: usize,
    shared_physical_row: usize,
    slot: usize,
    scratch: &mut DirectGroupedAggregateScratch,
) -> u64 {
    let total = source * scratch.slot_count + slot;
    let value = unsafe { inputs.value(source, row, shared_physical_row) };
    let target = unsafe {
        scratch
            .narrow_totals
            .as_mut_slice()
            .get_unchecked_mut(total)
    };
    let (sum, overflowed) = target.overflowing_add(value);
    *target = sum;
    u64::from(overflowed)
}

fn accounted_scratch_vec<T>(memory: &MemoryAccountingContext) -> Result<AccountedVec<T>> {
    Ok(AccountedVec::new_with_accounting(
        memory.grant()?,
        MemoryTag::HashTable,
        MemoryAccountingClass::Revocable,
    ))
}
