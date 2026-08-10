// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Precompiled grouped aggregate update programs.

use ethnum::i256;
use paro_common::allocator::MemoryTag;
use paro_common::chunk::Chunk;
use paro_common::error::Result;
use paro_common::memory::{AccountedVec, MemoryAccountingClass, MemoryAccountingContext};
use paro_common::types::LogicalType;
use paro_common::vector::{DataRef, DecodedVectorRef, SelectionRef, Vector, VECTOR_SIZE};
use smallvec::SmallVec;

use super::distributive::decimal::{DecimalAverageState, DecimalNarrowState, DecimalSumState};
use super::{AggregateDirectUpdate, AggregateStateInput, DecimalDirectUpdate};

#[derive(Debug, Clone)]
struct DirectDecimalInputUpdates {
    input_index: usize,
    width: DirectDecimalWidth,
    narrow_sums: Vec<usize>,
    wide_sums: Vec<usize>,
    averages: Vec<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DirectDecimalWidth {
    I64,
    I128,
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
    wide_totals: AccountedVec<i256>,
    row_counts: AccountedVec<usize>,
    touched_slots: AccountedVec<usize>,
}

/// Perfect-hash slots whose domain membership has been validated once before
/// entering unchecked aggregate reduction loops.
pub struct ValidatedDirectGroupSlots<'a> {
    slots: &'a [usize],
    slot_count: usize,
}

impl<'a> ValidatedDirectGroupSlots<'a> {
    pub fn try_new(slots: &'a [usize], count: usize, slot_count: usize) -> Result<Self> {
        let slots = slots.get(..count).ok_or_else(|| {
            paro_common::error::internal(format!(
                "direct aggregate slot batch is too short: slots={}, rows={count}",
                slots.len()
            ))
        })?;
        if let Some(slot) = slots.iter().copied().find(|slot| *slot >= slot_count) {
            return Err(paro_common::error::internal(format!(
                "direct aggregate slot out of bounds: slot={slot}, slots={slot_count}"
            )));
        }
        Ok(Self { slots, slot_count })
    }

    fn as_slice(&self) -> &'a [usize] {
        self.slots
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReducedSlotTraversal {
    DenseDomain,
    SparseTouched,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReducedDecimalWidth {
    I64,
    I128,
    I256,
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
            AggregateDirectUpdate::CountStar => {
                if input_index.is_some() {
                    return false;
                }
                self.count_star_offsets.push(state_offset);
            }
            AggregateDirectUpdate::Decimal(decimal_update) => {
                let Some(input_index) = input_index else {
                    return false;
                };
                let width = match decimal_update {
                    DecimalDirectUpdate::NarrowSumI64 | DecimalDirectUpdate::AverageI64 => {
                        DirectDecimalWidth::I64
                    }
                    DecimalDirectUpdate::WideSumI128 | DecimalDirectUpdate::AverageI128 => {
                        DirectDecimalWidth::I128
                    }
                };
                let source = if let Some(source) = self
                    .decimal_inputs
                    .iter_mut()
                    .find(|source| source.input_index == input_index)
                {
                    if source.width != width {
                        return false;
                    }
                    source
                } else {
                    self.decimal_inputs.push(DirectDecimalInputUpdates {
                        input_index,
                        width,
                        narrow_sums: Vec::new(),
                        wide_sums: Vec::new(),
                        averages: Vec::new(),
                    });
                    self.decimal_inputs
                        .last_mut()
                        .expect("source was just inserted")
                };
                match decimal_update {
                    DecimalDirectUpdate::NarrowSumI64 => source.narrow_sums.push(state_offset),
                    DecimalDirectUpdate::WideSumI128 => source.wide_sums.push(state_offset),
                    DecimalDirectUpdate::AverageI64 | DecimalDirectUpdate::AverageI128 => {
                        source.averages.push(state_offset)
                    }
                }
            }
        }
        *handled = true;
        self.update_count += 1;
        true
    }

    pub fn has_updates(&self) -> bool {
        self.update_count != 0
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
            .checked_mul(
                std::mem::size_of::<i128>()
                    .checked_add(std::mem::size_of::<i64>())?
                    .checked_add(std::mem::size_of::<i256>())?,
            )?
            .checked_add(std::mem::size_of::<usize>())?;
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
        let mut wide_totals = accounted_scratch_vec::<i256>(memory)?;
        wide_totals.try_resize_with(total_count, || i256::ZERO)?;
        let mut row_counts = accounted_scratch_vec::<usize>(memory)?;
        row_counts.try_resize_with(slot_count, || 0)?;
        let mut touched_slots = accounted_scratch_vec::<usize>(memory)?;
        touched_slots.try_reserve(slot_count)?;
        Ok(Some(DirectGroupedAggregateScratch {
            slot_count,
            source_count,
            totals,
            narrow_totals,
            wide_totals,
            row_counts,
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
                let value = unsafe { inputs.value_i128(source_idx, row, shared_physical_row) };
                for &state_offset in &source.narrow_sums {
                    let state =
                        unsafe { &mut *base.add(state_offset).cast::<DecimalNarrowState>() };
                    state.add_i64(value as i64);
                }
                for &state_offset in &source.wide_sums {
                    let state = unsafe { &mut *base.add(state_offset).cast::<DecimalSumState>() };
                    state.add_direct_i128(value);
                }
                for &state_offset in &source.averages {
                    let state =
                        unsafe { &mut *base.add(state_offset).cast::<DecimalAverageState>() };
                    match source.width {
                        DirectDecimalWidth::I64 => state.update_direct_i64(value as i64),
                        DirectDecimalWidth::I128 => state.update_direct_i128(value, 1),
                    }
                }
            }
        }
        Ok(true)
    }

    /// Collapse a batch by slot and update states in direct-addressing storage.
    ///
    /// # Safety
    ///
    /// `state_base + slot * state_stride` must identify initialized aggregate
    /// state rows for every validated slot.
    pub unsafe fn execute_reduced_slots_prepared(
        &self,
        inputs: &PreparedDirectGroupedAggregateInput<'_>,
        slots: &ValidatedDirectGroupSlots<'_>,
        scratch: &mut DirectGroupedAggregateScratch,
        state_base: *mut u8,
        state_stride: usize,
    ) -> Result<bool> {
        if scratch.source_count != self.decimal_inputs.len()
            || slots.slot_count != scratch.slot_count
        {
            return Ok(false);
        }
        let traversal = if scratch.slot_count <= slots.as_slice().len() {
            ReducedSlotTraversal::DenseDomain
        } else {
            ReducedSlotTraversal::SparseTouched
        };
        let widths = self.reduce_inputs(inputs, slots, scratch, traversal)?;
        match traversal {
            ReducedSlotTraversal::DenseDomain => {
                for slot in 0..scratch.slot_count {
                    if scratch.row_counts[slot] == 0 {
                        continue;
                    }
                    let base = unsafe { state_base.add(slot * state_stride) };
                    self.apply_reduced_slot(base, slot, scratch, &widths);
                }
            }
            ReducedSlotTraversal::SparseTouched => {
                for touched_idx in 0..scratch.touched_slots.len() {
                    let slot = scratch.touched_slots[touched_idx];
                    let base = unsafe { state_base.add(slot * state_stride) };
                    self.apply_reduced_slot(base, slot, scratch, &widths);
                }
            }
        }
        scratch.touched_slots.clear();
        Ok(true)
    }

    fn reduce_inputs(
        &self,
        inputs: &PreparedDirectGroupedAggregateInput<'_>,
        slots: &ValidatedDirectGroupSlots<'_>,
        scratch: &mut DirectGroupedAggregateScratch,
        traversal: ReducedSlotTraversal,
    ) -> Result<SmallVec<[ReducedDecimalWidth; 8]>> {
        let slots = slots.as_slice();
        let mut overflowed: SmallVec<[bool; 8]> =
            std::iter::repeat_n(false, inputs.len()).collect();
        for (row, &slot) in slots.iter().enumerate() {
            if traversal == ReducedSlotTraversal::SparseTouched && scratch.row_counts[slot] == 0 {
                scratch.touched_slots.try_push(slot)?;
            }
            scratch.row_counts[slot] += 1;
            let shared_physical_row = inputs.shared_physical_row(row);
            for (source_idx, source) in self.decimal_inputs.iter().enumerate() {
                let source_overflowed = match source.width {
                    DirectDecimalWidth::I64 => unsafe {
                        accumulate_i64_input(
                            inputs,
                            source_idx,
                            row,
                            shared_physical_row,
                            slot,
                            scratch,
                        )
                    },
                    DirectDecimalWidth::I128 => unsafe {
                        accumulate_i128_input(
                            inputs,
                            source_idx,
                            row,
                            shared_physical_row,
                            slot,
                            scratch,
                        )
                    },
                };
                if source_overflowed {
                    overflowed[source_idx] = true;
                }
            }
        }

        let mut widths = SmallVec::with_capacity(inputs.len());
        for (source_idx, source) in self.decimal_inputs.iter().enumerate() {
            if !overflowed[source_idx] {
                widths.push(match source.width {
                    DirectDecimalWidth::I64 => ReducedDecimalWidth::I64,
                    DirectDecimalWidth::I128 => ReducedDecimalWidth::I128,
                });
                continue;
            }
            clear_reduced_source(source_idx, scratch, traversal);
            let width = match source.width {
                DirectDecimalWidth::I64 => {
                    for (row, &slot) in slots.iter().enumerate() {
                        let physical_row = inputs.shared_physical_row(row);
                        let total = source_idx * scratch.slot_count + slot;
                        scratch.totals[total] +=
                            i128::from(unsafe { inputs.value_i64(source_idx, row, physical_row) });
                    }
                    ReducedDecimalWidth::I128
                }
                DirectDecimalWidth::I128 => {
                    for (row, &slot) in slots.iter().enumerate() {
                        let physical_row = inputs.shared_physical_row(row);
                        let total = source_idx * scratch.slot_count + slot;
                        scratch.wide_totals[total] +=
                            i256::from(unsafe { inputs.value_i128(source_idx, row, physical_row) });
                    }
                    ReducedDecimalWidth::I256
                }
            };
            widths.push(width);
        }
        Ok(widths)
    }

    fn apply_reduced_slot(
        &self,
        base: *mut u8,
        slot: usize,
        scratch: &mut DirectGroupedAggregateScratch,
        widths: &[ReducedDecimalWidth],
    ) {
        let row_count = scratch.row_counts[slot];
        for &state_offset in &self.count_star_offsets {
            unsafe { *base.add(state_offset).cast::<i64>() += row_count as i64 };
        }
        for (source_idx, source) in self.decimal_inputs.iter().enumerate() {
            let total_idx = source_idx * scratch.slot_count + slot;
            match widths[source_idx] {
                ReducedDecimalWidth::I64 => {
                    let value = scratch.narrow_totals[total_idx];
                    for &state_offset in &source.narrow_sums {
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
                }
                ReducedDecimalWidth::I128 => {
                    let value = scratch.totals[total_idx];
                    for &state_offset in &source.narrow_sums {
                        let state =
                            unsafe { &mut *base.add(state_offset).cast::<DecimalNarrowState>() };
                        state.add_direct_i128(value);
                    }
                    for &state_offset in &source.wide_sums {
                        let state =
                            unsafe { &mut *base.add(state_offset).cast::<DecimalSumState>() };
                        state.add_direct_i128(value);
                    }
                    for &state_offset in &source.averages {
                        let state =
                            unsafe { &mut *base.add(state_offset).cast::<DecimalAverageState>() };
                        state.update_direct_i128(value, row_count as u64);
                    }
                    scratch.totals[total_idx] = 0;
                }
                ReducedDecimalWidth::I256 => {
                    let value = scratch.wide_totals[total_idx];
                    for &state_offset in &source.wide_sums {
                        let state =
                            unsafe { &mut *base.add(state_offset).cast::<DecimalSumState>() };
                        state.add_direct_i256(value);
                    }
                    for &state_offset in &source.averages {
                        let state =
                            unsafe { &mut *base.add(state_offset).cast::<DecimalAverageState>() };
                        state.update_direct_i256(value, row_count as u64);
                    }
                    scratch.wide_totals[total_idx] = i256::ZERO;
                }
            }
        }
        scratch.row_counts[slot] = 0;
    }

    fn prepare_inputs<'a>(
        &self,
        payload: &'a Chunk,
    ) -> Option<PreparedDirectGroupedAggregateInput<'a>> {
        let mut decoded = SmallVec::<[(DecodedVectorRef<'a>, DirectDecimalWidth); 8]>::new();
        for source in &self.decimal_inputs {
            let vector = payload.column(source.input_index)?;
            let width = match vector.logical_type() {
                LogicalType::Decimal { precision, .. } if *precision <= 18 => {
                    DirectDecimalWidth::I64
                }
                LogicalType::Decimal { .. } => DirectDecimalWidth::I128,
                _ => return None,
            };
            if width != source.width {
                return None;
            }
            let input = vector.try_decode_ref(payload.size()).ok()?;
            if !input.validity().all_valid() || !matches!(input.data(), DataRef::Ptr(_)) {
                return None;
            }
            decoded.push((input, width));
        }
        Some(PreparedDirectGroupedAggregateInput::new(decoded))
    }
}

#[derive(Clone, Copy)]
enum PreparedDecimalData {
    I64(*const i64),
    I128(*const i128),
}

struct PreparedDecimalInput<'a> {
    decoded: DecodedVectorRef<'a>,
    data: PreparedDecimalData,
    direct: bool,
    uses_shared_selection: bool,
}

pub struct PreparedDirectGroupedAggregateInput<'a> {
    inputs: SmallVec<[PreparedDecimalInput<'a>; 8]>,
    shared_selection_input: Option<usize>,
    shared_selection_data: *const u32,
}

impl<'a> PreparedDirectGroupedAggregateInput<'a> {
    fn new(decoded: SmallVec<[(DecodedVectorRef<'a>, DirectDecimalWidth); 8]>) -> Self {
        let shared_identity = decoded
            .iter()
            .find_map(|(input, _)| input.sel().allocation_identity());
        let shared_selection_input = shared_identity.and_then(|identity| {
            decoded
                .iter()
                .position(|(input, _)| input.sel().allocation_identity() == Some(identity))
        });
        let inputs: SmallVec<[PreparedDecimalInput<'a>; 8]> = decoded
            .into_iter()
            .map(|(decoded, width)| PreparedDecimalInput {
                data: match width {
                    DirectDecimalWidth::I64 => PreparedDecimalData::I64(decoded.get_data::<i64>()),
                    DirectDecimalWidth::I128 => {
                        PreparedDecimalData::I128(decoded.get_data::<i128>())
                    }
                },
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
    unsafe fn physical_row(
        &self,
        input: &PreparedDecimalInput<'_>,
        row: usize,
        shared_physical_row: usize,
    ) -> usize {
        if input.uses_shared_selection {
            shared_physical_row
        } else if input.direct {
            row
        } else {
            input.decoded.physical_index(row)
        }
    }

    /// # Safety
    ///
    /// The prepared source must be an i64 DECIMAL and all row indices must be
    /// within the prepared input batch.
    #[inline(always)]
    unsafe fn value_i64(&self, source: usize, row: usize, shared_physical_row: usize) -> i64 {
        let input = unsafe { self.inputs.get_unchecked(source) };
        let physical_row = unsafe { self.physical_row(input, row, shared_physical_row) };
        let PreparedDecimalData::I64(data) = input.data else {
            unreachable!("validated direct i64 source has a non-i64 physical representation")
        };
        unsafe { *data.add(physical_row) }
    }

    /// # Safety
    ///
    /// `source`, `row`, and `shared_physical_row` must identify rows within the
    /// prepared input batch.
    #[inline(always)]
    unsafe fn value_i128(&self, source: usize, row: usize, shared_physical_row: usize) -> i128 {
        let input = unsafe { self.inputs.get_unchecked(source) };
        let physical_row = unsafe { self.physical_row(input, row, shared_physical_row) };
        match input.data {
            PreparedDecimalData::I64(data) => i128::from(unsafe { *data.add(physical_row) }),
            PreparedDecimalData::I128(data) => unsafe { *data.add(physical_row) },
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
unsafe fn accumulate_i64_input(
    inputs: &PreparedDirectGroupedAggregateInput<'_>,
    source: usize,
    row: usize,
    shared_physical_row: usize,
    slot: usize,
    scratch: &mut DirectGroupedAggregateScratch,
) -> bool {
    let total = source * scratch.slot_count + slot;
    let value = unsafe { inputs.value_i64(source, row, shared_physical_row) };
    let target = unsafe {
        scratch
            .narrow_totals
            .as_mut_slice()
            .get_unchecked_mut(total)
    };
    let (sum, overflowed) = target.overflowing_add(value);
    *target = sum;
    overflowed
}

/// Accumulate one physical i128 input into its slot-local batch state.
///
/// # Safety
///
/// The source, logical row, shared physical row, slot, and scratch dimensions
/// must all have been validated by `reduce_inputs` and `prepare_inputs`.
#[inline(always)]
unsafe fn accumulate_i128_input(
    inputs: &PreparedDirectGroupedAggregateInput<'_>,
    source: usize,
    row: usize,
    shared_physical_row: usize,
    slot: usize,
    scratch: &mut DirectGroupedAggregateScratch,
) -> bool {
    let total = source * scratch.slot_count + slot;
    let value = unsafe { inputs.value_i128(source, row, shared_physical_row) };
    let target = unsafe { scratch.totals.as_mut_slice().get_unchecked_mut(total) };
    let (sum, overflowed) = target.overflowing_add(value);
    *target = sum;
    overflowed
}

fn clear_reduced_source(
    source: usize,
    scratch: &mut DirectGroupedAggregateScratch,
    traversal: ReducedSlotTraversal,
) {
    let mut clear_slot = |slot: usize| {
        let index = source * scratch.slot_count + slot;
        scratch.narrow_totals[index] = 0;
        scratch.totals[index] = 0;
        scratch.wide_totals[index] = i256::ZERO;
    };
    match traversal {
        ReducedSlotTraversal::DenseDomain => {
            for slot in 0..scratch.slot_count {
                if scratch.row_counts[slot] != 0 {
                    clear_slot(slot);
                }
            }
        }
        ReducedSlotTraversal::SparseTouched => {
            for touched_idx in 0..scratch.touched_slots.len() {
                clear_slot(scratch.touched_slots[touched_idx]);
            }
        }
    }
}

fn accounted_scratch_vec<T>(memory: &MemoryAccountingContext) -> Result<AccountedVec<T>> {
    Ok(AccountedVec::new_with_accounting(
        memory.grant()?,
        MemoryTag::HashTable,
        MemoryAccountingClass::Revocable,
    ))
}
