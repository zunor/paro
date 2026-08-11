// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Precompiled grouped aggregate update programs.

use ethnum::i256;
use paro_common::allocator::MemoryTag;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::memory::{
    AccountedVec, MemoryAccountingClass, MemoryAccountingContext, MemoryGrant,
};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_common::vector::{DataRef, DecodedVectorRef, SelectionRef, Vector, VECTOR_SIZE};
use smallvec::SmallVec;

use super::distributive::decimal::{DecimalAverageState, DecimalNarrowState, DecimalSumState};
use super::{
    AggregateComparison, AggregateDirectUpdate, AggregateFunction, AggregateStateInput,
    DecimalDirectUpdate,
};

/// Comparison compiled once for direct-address aggregate state traversal.
///
/// This capability is intentionally narrower than [`super::AggregateStateFilterFn`]:
/// it admits only fixed-width states whose validation and comparison can be
/// performed from one state address without vector materialization.
#[derive(Debug, Clone)]
pub struct PreparedDirectAggregateStatePredicate {
    comparison: AggregateComparison,
    constant: i128,
    output_limit: i128,
    output_precision: u8,
}

impl PreparedDirectAggregateStatePredicate {
    pub(super) fn decimal_narrow_sum(
        comparison: AggregateComparison,
        constant: i128,
        output_limit: i128,
        output_precision: u8,
    ) -> Self {
        Self {
            comparison,
            constant,
            output_limit,
            output_precision,
        }
    }

    /// Evaluate one initialized aggregate state.
    ///
    /// # Safety
    ///
    /// `state` must point to the bound fixed-width state used to prepare this
    /// predicate and remain live for the duration of the call.
    #[inline(always)]
    pub unsafe fn matches(&self, state: *const u8) -> Result<bool> {
        let state = unsafe { &*state.cast::<DecimalNarrowState>() };
        if !state.is_set() {
            return Ok(false);
        }
        if state.overflowed() {
            return Err(paro_error::out_of_range("Decimal SUM aggregate overflow"));
        }
        let value = state.value();
        if value.unsigned_abs() >= self.output_limit as u128 {
            return Err(paro_error::out_of_range(format!(
                "Decimal SUM result exceeds precision {}",
                self.output_precision
            )));
        }
        Ok(match self.comparison {
            AggregateComparison::Equal => value == self.constant,
            AggregateComparison::NotEqual => value != self.constant,
            AggregateComparison::LessThan => value < self.constant,
            AggregateComparison::GreaterThan => value > self.constant,
            AggregateComparison::LessThanOrEqual => value <= self.constant,
            AggregateComparison::GreaterThanOrEqual => value >= self.constant,
        })
    }
}

pub fn prepare_direct_state_predicate(
    function: &AggregateFunction,
    comparison: AggregateComparison,
    constant: &Value,
) -> Result<Option<PreparedDirectAggregateStatePredicate>> {
    super::distributive::decimal::prepare_direct_state_predicate(function, comparison, constant)
}

#[derive(Debug, Clone)]
struct DirectDecimalInputUpdates {
    input_index: usize,
    width: DirectDecimalWidth,
    sums: DirectDecimalSums,
    averages: Vec<usize>,
}

#[derive(Debug, Clone)]
enum DirectDecimalSums {
    None,
    Narrow(Vec<usize>),
    Wide(Vec<usize>),
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
/// slots. Larger domains use the allocation-free run reducer rather than
/// paying to initialize sparse domain-sized temporary state.
#[derive(Debug)]
pub struct DirectGroupedAggregateScratch {
    slot_count: usize,
    decimal_sources: Vec<DirectDecimalScratch>,
    row_counts: AccountedVec<usize>,
    touched_slots: AccountedVec<usize>,
}

#[derive(Debug)]
enum DirectDecimalScratch {
    I64 {
        primary: AccountedVec<i64>,
        fallback: AccountedVec<i128>,
    },
    I128 {
        primary: AccountedVec<i128>,
        fallback: AccountedVec<i256>,
    },
}

/// Perfect-hash slots whose domain membership has been validated once before
/// entering unchecked aggregate reduction loops.
pub struct ValidatedDirectGroupSlots<'a> {
    slots: &'a [usize],
    slot_count: usize,
}

/// Replayable source of perfect-hash group slots for a direct reducer.
///
/// # Safety
///
/// Every successful `slot_at(row)` call must return a value smaller than
/// `slot_count()`, and repeated calls for the same row must return the same
/// value. The reducer uses those proofs for unchecked scratch access and may
/// replay the source when narrow accumulation overflows.
pub unsafe trait DirectGroupSlotSource {
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    fn slot_count(&self) -> usize;
    fn slot_at(&mut self, row: usize) -> Result<usize>;
}

struct ValidatedSlotSource<'a> {
    slots: &'a [usize],
    slot_count: usize,
}

// SAFETY: `ValidatedDirectGroupSlots` validates the complete slice before this
// adapter is constructed, and slice indexing is deterministic.
unsafe impl DirectGroupSlotSource for ValidatedSlotSource<'_> {
    fn len(&self) -> usize {
        self.slots.len()
    }

    fn slot_count(&self) -> usize {
        self.slot_count
    }

    fn slot_at(&mut self, row: usize) -> Result<usize> {
        Ok(unsafe { *self.slots.get_unchecked(row) })
    }
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

    /// Construct a slot view whose domain was proven by the producer.
    ///
    /// # Safety
    ///
    /// `slots[..count]` must exist and every value in that prefix must be
    /// strictly smaller than `slot_count`.
    pub unsafe fn from_validated_prefix(
        slots: &'a [usize],
        count: usize,
        slot_count: usize,
    ) -> Self {
        debug_assert!(count <= slots.len());
        debug_assert!(slots[..count].iter().all(|slot| *slot < slot_count));
        Self {
            slots: unsafe { slots.get_unchecked(..count) },
            slot_count,
        }
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
                        sums: DirectDecimalSums::None,
                        averages: Vec::new(),
                    });
                    self.decimal_inputs
                        .last_mut()
                        .expect("source was just inserted")
                };
                match decimal_update {
                    DecimalDirectUpdate::NarrowSumI64 => match &mut source.sums {
                        DirectDecimalSums::None => {
                            source.sums = DirectDecimalSums::Narrow(vec![state_offset]);
                        }
                        DirectDecimalSums::Narrow(offsets) => offsets.push(state_offset),
                        DirectDecimalSums::Wide(_) => return false,
                    },
                    DecimalDirectUpdate::WideSumI128 => match &mut source.sums {
                        DirectDecimalSums::None => {
                            source.sums = DirectDecimalSums::Wide(vec![state_offset]);
                        }
                        DirectDecimalSums::Wide(offsets) => offsets.push(state_offset),
                        DirectDecimalSums::Narrow(_) => return false,
                    },
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

    /// Whether every compiled aggregate can combine two same-slot state rows
    /// without materializing address vectors through the generic ABI.
    pub fn supports_direct_combine(&self) -> bool {
        self.handles_all()
            && self
                .decimal_inputs
                .iter()
                .all(|source| source.averages.is_empty())
    }

    /// Whether a complete initialized state row can be byte-copied into
    /// uninitialized storage without creating shared ownership.
    ///
    /// The admitted states are fixed-width counts and DECIMAL sums. They own
    /// no allocator-backed payload and their combine implementation already
    /// covers every aggregate in the row.
    pub fn supports_trivial_state_copy(&self) -> bool {
        self.supports_direct_combine()
    }

    /// Combine one pair of same-layout aggregate state rows directly.
    ///
    /// Returning `false` means no state was modified and the caller must use
    /// the generic aggregate combine ABI.
    ///
    /// # Safety
    ///
    /// `source` and `target` must point to initialized rows compiled for this
    /// program. They must not overlap and the caller must own `target`.
    #[inline(always)]
    pub unsafe fn combine_direct_rows(&self, source: *const u8, target: *mut u8) -> bool {
        if !self.supports_direct_combine() {
            return false;
        }
        for &offset in &self.count_star_offsets {
            let source = unsafe { *source.add(offset).cast::<i64>() };
            let target = unsafe { &mut *target.add(offset).cast::<i64>() };
            *target += source;
        }
        for decimal in &self.decimal_inputs {
            match &decimal.sums {
                DirectDecimalSums::Narrow(offsets) => {
                    for &offset in offsets {
                        let source = unsafe { &*source.add(offset).cast::<DecimalNarrowState>() };
                        let target =
                            unsafe { &mut *target.add(offset).cast::<DecimalNarrowState>() };
                        if source.overflowed() {
                            target.mark_overflowed();
                        } else if source.is_set() {
                            target.add(source.value());
                        }
                    }
                }
                DirectDecimalSums::Wide(offsets) => {
                    for &offset in offsets {
                        let source = unsafe { &*source.add(offset).cast::<DecimalSumState>() };
                        let target = unsafe { &mut *target.add(offset).cast::<DecimalSumState>() };
                        target.add_state(source);
                    }
                }
                DirectDecimalSums::None => {}
            }
        }
        true
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

    /// Exact peak scratch bytes for this compiled update program.
    pub fn scratch_bytes(&self, slot_count: usize) -> Option<usize> {
        if slot_count > VECTOR_SIZE || self.decimal_inputs.is_empty() {
            return Some(0);
        }
        let decimal_bytes_per_slot =
            self.decimal_inputs
                .iter()
                .try_fold(0usize, |total, source| {
                    let source_bytes =
                        match source.width {
                            DirectDecimalWidth::I64 => std::mem::size_of::<i64>()
                                .checked_add(std::mem::size_of::<i128>())?,
                            DirectDecimalWidth::I128 => std::mem::size_of::<i128>()
                                .checked_add(std::mem::size_of::<i256>())?,
                        };
                    total.checked_add(source_bytes)
                })?;
        decimal_bytes_per_slot
            .checked_add(std::mem::size_of::<usize>())?
            .checked_mul(slot_count)?
            .checked_add(slot_count.checked_mul(std::mem::size_of::<usize>())?)
    }

    /// Batch-local storage required to materialize a canonical slot stream.
    ///
    /// A complete direct program consumes each encoded group slot during both
    /// occupancy handling and aggregate reduction. Materializing once avoids
    /// replaying potentially expensive key codecs while keeping the storage
    /// bounded by vector capacity rather than the group domain.
    pub fn materialized_slot_bytes(&self) -> Option<usize> {
        if self.handles_all() {
            VECTOR_SIZE.checked_mul(std::mem::size_of::<usize>())
        } else {
            Some(0)
        }
    }

    pub fn try_create_scratch(
        &self,
        slot_count: usize,
        memory: &MemoryAccountingContext,
    ) -> Result<Option<DirectGroupedAggregateScratch>> {
        let bytes = self.scratch_bytes(slot_count).ok_or_else(|| {
            paro_error::internal("direct grouped aggregate scratch byte-size overflow")
        })?;
        let grant = memory.reserve_grant(bytes)?;
        self.try_create_scratch_with_grant(
            slot_count,
            &grant,
            memory.tag(),
            memory.accounting_class(),
        )
    }

    pub fn try_create_scratch_with_grant(
        &self,
        slot_count: usize,
        grant: &MemoryGrant,
        tag: MemoryTag,
        class: MemoryAccountingClass,
    ) -> Result<Option<DirectGroupedAggregateScratch>> {
        if slot_count > VECTOR_SIZE || self.decimal_inputs.is_empty() {
            return Ok(None);
        }
        let mut decimal_sources = Vec::new();
        decimal_sources
            .try_reserve_exact(self.decimal_inputs.len())
            .map_err(|_| {
                paro_error::out_of_memory(format!(
                    "failed to allocate {} direct decimal scratch descriptors",
                    self.decimal_inputs.len()
                ))
            })?;
        for source in &self.decimal_inputs {
            let scratch = match source.width {
                DirectDecimalWidth::I64 => {
                    let mut primary = accounted_scratch_vec::<i64>(grant, slot_count, tag, class)?;
                    primary.try_resize_with(slot_count, || 0)?;
                    let mut fallback =
                        accounted_scratch_vec::<i128>(grant, slot_count, tag, class)?;
                    fallback.try_resize_with(slot_count, || 0)?;
                    DirectDecimalScratch::I64 { primary, fallback }
                }
                DirectDecimalWidth::I128 => {
                    let mut primary = accounted_scratch_vec::<i128>(grant, slot_count, tag, class)?;
                    primary.try_resize_with(slot_count, || 0)?;
                    let mut fallback =
                        accounted_scratch_vec::<i256>(grant, slot_count, tag, class)?;
                    fallback.try_resize_with(slot_count, || i256::ZERO)?;
                    DirectDecimalScratch::I128 { primary, fallback }
                }
            };
            decimal_sources.push(scratch);
        }
        let mut row_counts = accounted_scratch_vec::<usize>(grant, slot_count, tag, class)?;
        row_counts.try_resize_with(slot_count, || 0)?;
        let touched_slots = accounted_scratch_vec::<usize>(grant, slot_count, tag, class)?;
        Ok(Some(DirectGroupedAggregateScratch {
            slot_count,
            decimal_sources,
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
                match source.width {
                    DirectDecimalWidth::I64 => {
                        let value =
                            unsafe { inputs.value_i64(source_idx, row, shared_physical_row) };
                        if let DirectDecimalSums::Narrow(offsets) = &source.sums {
                            for &state_offset in offsets {
                                let state = unsafe {
                                    &mut *base.add(state_offset).cast::<DecimalNarrowState>()
                                };
                                state.add_i64(value);
                            }
                        }
                        for &state_offset in &source.averages {
                            let state = unsafe {
                                &mut *base.add(state_offset).cast::<DecimalAverageState>()
                            };
                            state.update_direct_i64(value);
                        }
                    }
                    DirectDecimalWidth::I128 => {
                        let value =
                            unsafe { inputs.value_i128(source_idx, row, shared_physical_row) };
                        if let DirectDecimalSums::Wide(offsets) = &source.sums {
                            for &state_offset in offsets {
                                let state = unsafe {
                                    &mut *base.add(state_offset).cast::<DecimalSumState>()
                                };
                                state.add_direct_i128(value);
                            }
                        }
                        for &state_offset in &source.averages {
                            let state = unsafe {
                                &mut *base.add(state_offset).cast::<DecimalAverageState>()
                            };
                            state.update_direct_i128(value, 1);
                        }
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
        let mut source = ValidatedSlotSource {
            slots: slots.as_slice(),
            slot_count: slots.slot_count,
        };
        unsafe {
            self.execute_reduced_slot_source_prepared(
                inputs,
                &mut source,
                scratch,
                state_base,
                state_stride,
                |_, _| {},
            )
        }
    }

    /// Collapse a materialized slot batch and initialize each touched state
    /// before its first reduced update.
    ///
    /// # Safety
    ///
    /// The state allocation and initializer must satisfy the contract of
    /// `execute_reduced_slot_source_prepared`.
    pub unsafe fn execute_reduced_slots_prepared_with_initializer<F>(
        &self,
        inputs: &PreparedDirectGroupedAggregateInput<'_>,
        slots: &ValidatedDirectGroupSlots<'_>,
        scratch: &mut DirectGroupedAggregateScratch,
        state_base: *mut u8,
        state_stride: usize,
        initialize_slot: F,
    ) -> Result<bool>
    where
        F: FnMut(usize, *mut u8),
    {
        let mut source = ValidatedSlotSource {
            slots: slots.as_slice(),
            slot_count: slots.slot_count,
        };
        unsafe {
            self.execute_reduced_slot_source_prepared(
                inputs,
                &mut source,
                scratch,
                state_base,
                state_stride,
                initialize_slot,
            )
        }
    }

    /// Collapse a replayable slot stream and update direct-addressing states.
    ///
    /// `initialize_slot` runs exactly once for every non-empty reduced slot,
    /// before any aggregate state at that address is read or updated.
    ///
    /// # Safety
    ///
    /// `state_base + slot * state_stride` must provide one writable state row
    /// for the complete source domain. `initialize_slot` must establish a
    /// valid state at `base` when it is not already initialized.
    #[inline]
    pub unsafe fn execute_reduced_slot_source_prepared<S, F>(
        &self,
        inputs: &PreparedDirectGroupedAggregateInput<'_>,
        slot_source: &mut S,
        scratch: &mut DirectGroupedAggregateScratch,
        state_base: *mut u8,
        state_stride: usize,
        mut initialize_slot: F,
    ) -> Result<bool>
    where
        S: DirectGroupSlotSource,
        F: FnMut(usize, *mut u8),
    {
        if scratch.decimal_sources.len() != self.decimal_inputs.len()
            || slot_source.slot_count() != scratch.slot_count
        {
            return Ok(false);
        }
        let traversal = if scratch.slot_count <= slot_source.len() {
            ReducedSlotTraversal::DenseDomain
        } else {
            ReducedSlotTraversal::SparseTouched
        };
        let widths = self.reduce_inputs(inputs, slot_source, scratch, traversal)?;
        match traversal {
            ReducedSlotTraversal::DenseDomain => {
                for slot in 0..scratch.slot_count {
                    if scratch.row_counts[slot] == 0 {
                        continue;
                    }
                    let base = unsafe { state_base.add(slot * state_stride) };
                    initialize_slot(slot, base);
                    self.apply_reduced_slot(base, slot, scratch, &widths);
                }
            }
            ReducedSlotTraversal::SparseTouched => {
                for touched_idx in 0..scratch.touched_slots.len() {
                    let slot = scratch.touched_slots[touched_idx];
                    let base = unsafe { state_base.add(slot * state_stride) };
                    initialize_slot(slot, base);
                    self.apply_reduced_slot(base, slot, scratch, &widths);
                }
            }
        }
        scratch.touched_slots.clear();
        Ok(true)
    }

    /// Reduce consecutive equal slots without allocating domain-sized scratch.
    ///
    /// This is the complete direct-update path for large perfect domains. It
    /// preserves arbitrary input order—non-clustered keys simply form runs of
    /// length one—while clustered analytical keys write aggregate state once
    /// per run instead of once per row.
    ///
    /// # Safety
    ///
    /// `state_base + slot * state_stride` must provide one writable state row
    /// for the complete source domain. `initialize_slot` must establish valid
    /// state before the callback returns when the slot was previously empty.
    pub unsafe fn execute_run_reduced_slot_source_prepared<S, F>(
        &self,
        inputs: &PreparedDirectGroupedAggregateInput<'_>,
        slot_source: &mut S,
        state_base: *mut u8,
        state_stride: usize,
        mut initialize_slot: F,
    ) -> Result<bool>
    where
        S: DirectGroupSlotSource,
        F: FnMut(usize, *mut u8),
    {
        if !self.handles_all() || inputs.len() != self.decimal_inputs.len() {
            return Ok(false);
        }
        if slot_source.is_empty() {
            return Ok(true);
        }

        let mut run_start = 0usize;
        let mut run_slot = slot_source.slot_at(0)?;
        for row in 1..=slot_source.len() {
            let next_slot = if row < slot_source.len() {
                Some(slot_source.slot_at(row)?)
            } else {
                None
            };
            if next_slot == Some(run_slot) {
                continue;
            }
            let base = unsafe { state_base.add(run_slot * state_stride) };
            initialize_slot(run_slot, base);
            unsafe { self.apply_direct_run(inputs, run_start..row, base) };
            if let Some(slot) = next_slot {
                run_start = row;
                run_slot = slot;
            }
        }
        Ok(true)
    }

    /// Run-reduce a materialized, already validated slot batch.
    ///
    /// # Safety
    ///
    /// The state allocation and initializer must satisfy the same contract as
    /// `execute_run_reduced_slot_source_prepared`.
    pub unsafe fn execute_run_reduced_slots_prepared<F>(
        &self,
        inputs: &PreparedDirectGroupedAggregateInput<'_>,
        slots: &ValidatedDirectGroupSlots<'_>,
        state_base: *mut u8,
        state_stride: usize,
        initialize_slot: F,
    ) -> Result<bool>
    where
        F: FnMut(usize, *mut u8),
    {
        let mut source = ValidatedSlotSource {
            slots: slots.as_slice(),
            slot_count: slots.slot_count,
        };
        unsafe {
            self.execute_run_reduced_slot_source_prepared(
                inputs,
                &mut source,
                state_base,
                state_stride,
                initialize_slot,
            )
        }
    }

    unsafe fn apply_direct_run(
        &self,
        inputs: &PreparedDirectGroupedAggregateInput<'_>,
        rows: std::ops::Range<usize>,
        base: *mut u8,
    ) {
        let row_count = rows.len();
        for &state_offset in &self.count_star_offsets {
            unsafe { *base.add(state_offset).cast::<i64>() += row_count as i64 };
        }
        for (source_idx, source) in self.decimal_inputs.iter().enumerate() {
            match source.width {
                DirectDecimalWidth::I64 => {
                    let mut sum = 0_i128;
                    for row in rows.clone() {
                        let physical_row = inputs.shared_physical_row(row);
                        sum +=
                            i128::from(unsafe { inputs.value_i64(source_idx, row, physical_row) });
                    }
                    if let DirectDecimalSums::Narrow(offsets) = &source.sums {
                        for &state_offset in offsets {
                            let state = unsafe {
                                &mut *base.add(state_offset).cast::<DecimalNarrowState>()
                            };
                            state.add_direct_i128(sum);
                        }
                    }
                    for &state_offset in &source.averages {
                        let state =
                            unsafe { &mut *base.add(state_offset).cast::<DecimalAverageState>() };
                        state.update_direct_i128(sum, row_count as u64);
                    }
                }
                DirectDecimalWidth::I128 => {
                    let mut sum = i256::ZERO;
                    for row in rows.clone() {
                        let physical_row = inputs.shared_physical_row(row);
                        sum +=
                            i256::from(unsafe { inputs.value_i128(source_idx, row, physical_row) });
                    }
                    if let DirectDecimalSums::Wide(offsets) = &source.sums {
                        for &state_offset in offsets {
                            let state =
                                unsafe { &mut *base.add(state_offset).cast::<DecimalSumState>() };
                            state.add_direct_i256(sum);
                        }
                    }
                    for &state_offset in &source.averages {
                        let state =
                            unsafe { &mut *base.add(state_offset).cast::<DecimalAverageState>() };
                        state.update_direct_i256(sum, row_count as u64);
                    }
                }
            }
        }
    }

    #[inline]
    fn reduce_inputs<S: DirectGroupSlotSource>(
        &self,
        inputs: &PreparedDirectGroupedAggregateInput<'_>,
        slot_source: &mut S,
        scratch: &mut DirectGroupedAggregateScratch,
        traversal: ReducedSlotTraversal,
    ) -> Result<SmallVec<[ReducedDecimalWidth; 8]>> {
        let overflowed = if let Some(execution) =
            try_reduce_fixed_inputs(inputs, slot_source, scratch, traversal)
        {
            execution?
        } else {
            self.reduce_inputs_generic(inputs, slot_source, scratch, traversal)?
        };

        let mut widths = SmallVec::with_capacity(inputs.len());
        for (source_idx, source) in self.decimal_inputs.iter().enumerate() {
            if !overflowed[source_idx] {
                widths.push(match source.width {
                    DirectDecimalWidth::I64 => ReducedDecimalWidth::I64,
                    DirectDecimalWidth::I128 => ReducedDecimalWidth::I128,
                });
                continue;
            }
            clear_reduced_source(
                &mut scratch.decimal_sources[source_idx],
                &scratch.row_counts,
                &scratch.touched_slots,
                traversal,
            );
            let width = match (&mut scratch.decimal_sources[source_idx], source.width) {
                (DirectDecimalScratch::I64 { fallback, .. }, DirectDecimalWidth::I64) => {
                    for row in 0..slot_source.len() {
                        let slot = slot_source.slot_at(row)?;
                        let physical_row = inputs.shared_physical_row(row);
                        fallback[slot] +=
                            i128::from(unsafe { inputs.value_i64(source_idx, row, physical_row) });
                    }
                    ReducedDecimalWidth::I128
                }
                (DirectDecimalScratch::I128 { fallback, .. }, DirectDecimalWidth::I128) => {
                    for row in 0..slot_source.len() {
                        let slot = slot_source.slot_at(row)?;
                        let physical_row = inputs.shared_physical_row(row);
                        fallback[slot] +=
                            i256::from(unsafe { inputs.value_i128(source_idx, row, physical_row) });
                    }
                    ReducedDecimalWidth::I256
                }
                _ => unreachable!("compiled decimal source and scratch width disagree"),
            };
            widths.push(width);
        }
        Ok(widths)
    }

    #[inline]
    fn reduce_inputs_generic<S: DirectGroupSlotSource>(
        &self,
        inputs: &PreparedDirectGroupedAggregateInput<'_>,
        source: &mut S,
        scratch: &mut DirectGroupedAggregateScratch,
        traversal: ReducedSlotTraversal,
    ) -> Result<SmallVec<[bool; 8]>> {
        let mut overflowed: SmallVec<[bool; 8]> =
            std::iter::repeat_n(false, inputs.len()).collect();
        for row in 0..source.len() {
            let slot = source.slot_at(row)?;
            if traversal == ReducedSlotTraversal::SparseTouched && scratch.row_counts[slot] == 0 {
                scratch.touched_slots.try_push(slot)?;
            }
            scratch.row_counts[slot] += 1;
            let shared_physical_row = inputs.shared_physical_row(row);
            for (source_idx, source) in self.decimal_inputs.iter().enumerate() {
                let source_scratch =
                    unsafe { scratch.decimal_sources.get_unchecked_mut(source_idx) };
                let source_overflowed = match (source.width, source_scratch) {
                    (DirectDecimalWidth::I64, DirectDecimalScratch::I64 { primary, .. }) => {
                        let value =
                            unsafe { inputs.value_i64(source_idx, row, shared_physical_row) };
                        let target = unsafe { primary.as_mut_slice().get_unchecked_mut(slot) };
                        let (sum, overflowed) = target.overflowing_add(value);
                        *target = sum;
                        overflowed
                    }
                    (DirectDecimalWidth::I128, DirectDecimalScratch::I128 { primary, .. }) => {
                        let value =
                            unsafe { inputs.value_i128(source_idx, row, shared_physical_row) };
                        let target = unsafe { primary.as_mut_slice().get_unchecked_mut(slot) };
                        let (sum, overflowed) = target.overflowing_add(value);
                        *target = sum;
                        overflowed
                    }
                    _ => unreachable!("compiled decimal source and scratch width disagree"),
                };
                overflowed[source_idx] |= source_overflowed;
            }
        }
        Ok(overflowed)
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
            match widths[source_idx] {
                ReducedDecimalWidth::I64 => {
                    let DirectDecimalScratch::I64 { primary, .. } =
                        &mut scratch.decimal_sources[source_idx]
                    else {
                        unreachable!("i64 reduction has non-i64 scratch")
                    };
                    let value = primary[slot];
                    if let DirectDecimalSums::Narrow(offsets) = &source.sums {
                        for &state_offset in offsets {
                            let state = unsafe {
                                &mut *base.add(state_offset).cast::<DecimalNarrowState>()
                            };
                            state.add_i64(value);
                        }
                    }
                    for &state_offset in &source.averages {
                        let state =
                            unsafe { &mut *base.add(state_offset).cast::<DecimalAverageState>() };
                        state.update_direct_i64_sum(value, row_count as u64);
                    }
                    primary[slot] = 0;
                }
                ReducedDecimalWidth::I128 => {
                    let value = match &mut scratch.decimal_sources[source_idx] {
                        DirectDecimalScratch::I64 { fallback, .. } => {
                            let value = fallback[slot];
                            fallback[slot] = 0;
                            value
                        }
                        DirectDecimalScratch::I128 { primary, .. } => {
                            let value = primary[slot];
                            primary[slot] = 0;
                            value
                        }
                    };
                    match &source.sums {
                        DirectDecimalSums::Narrow(offsets) => {
                            for &state_offset in offsets {
                                let state = unsafe {
                                    &mut *base.add(state_offset).cast::<DecimalNarrowState>()
                                };
                                state.add_direct_i128(value);
                            }
                        }
                        DirectDecimalSums::Wide(offsets) => {
                            for &state_offset in offsets {
                                let state = unsafe {
                                    &mut *base.add(state_offset).cast::<DecimalSumState>()
                                };
                                state.add_direct_i128(value);
                            }
                        }
                        DirectDecimalSums::None => {}
                    }
                    for &state_offset in &source.averages {
                        let state =
                            unsafe { &mut *base.add(state_offset).cast::<DecimalAverageState>() };
                        state.update_direct_i128(value, row_count as u64);
                    }
                }
                ReducedDecimalWidth::I256 => {
                    let DirectDecimalScratch::I128 { fallback, .. } =
                        &mut scratch.decimal_sources[source_idx]
                    else {
                        unreachable!("i256 reduction has non-i128 scratch")
                    };
                    let value = fallback[slot];
                    if let DirectDecimalSums::Wide(offsets) = &source.sums {
                        for &state_offset in offsets {
                            let state =
                                unsafe { &mut *base.add(state_offset).cast::<DecimalSumState>() };
                            state.add_direct_i256(value);
                        }
                    }
                    for &state_offset in &source.averages {
                        let state =
                            unsafe { &mut *base.add(state_offset).cast::<DecimalAverageState>() };
                        state.update_direct_i256(value, row_count as u64);
                    }
                    fallback[slot] = i256::ZERO;
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

struct FixedReductionInputs<const N: usize> {
    input_data: [*const u8; N],
    scratch_data: [*mut u8; N],
    direct_mask: u8,
    shared_selection: *const u32,
}

#[inline]
fn try_reduce_fixed_inputs<S: DirectGroupSlotSource>(
    inputs: &PreparedDirectGroupedAggregateInput<'_>,
    source: &mut S,
    scratch: &mut DirectGroupedAggregateScratch,
    traversal: ReducedSlotTraversal,
) -> Option<Result<SmallVec<[bool; 8]>>> {
    if !(1..=5).contains(&inputs.len()) {
        return None;
    }
    let width_mask = inputs
        .inputs
        .iter()
        .enumerate()
        .fold(0_u8, |mask, (idx, input)| {
            mask | (u8::from(matches!(input.data, PreparedDecimalData::I128(_))) << idx)
        });
    macro_rules! dispatch_masks {
        ($count:literal; $($mask:literal),+ $(,)?) => {
            match width_mask {
                $($mask => run_fixed_reduction::<$count, $mask, _>(
                    inputs,
                    source,
                    scratch,
                    traversal,
                ),)+
                _ => None,
            }
        };
    }
    match inputs.len() {
        1 => dispatch_masks!(1; 0, 1),
        2 => dispatch_masks!(2; 0, 1, 2, 3),
        3 => dispatch_masks!(3; 0, 1, 2, 3, 4, 5, 6, 7),
        4 => dispatch_masks!(4; 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15),
        5 => dispatch_masks!(5;
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
            16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
        ),
        _ => None,
    }
}

#[inline]
fn run_fixed_reduction<const N: usize, const WIDTH_MASK: u8, S: DirectGroupSlotSource>(
    inputs: &PreparedDirectGroupedAggregateInput<'_>,
    source: &mut S,
    scratch: &mut DirectGroupedAggregateScratch,
    traversal: ReducedSlotTraversal,
) -> Option<Result<SmallVec<[bool; 8]>>> {
    let prepared = prepare_fixed_reduction::<N, WIDTH_MASK>(inputs, scratch)?;
    Some(
        reduce_fixed_inputs::<N, WIDTH_MASK, S>(prepared, source, scratch, traversal)
            .map(|overflowed| overflowed.into_iter().collect()),
    )
}

#[inline]
fn prepare_fixed_reduction<const N: usize, const WIDTH_MASK: u8>(
    inputs: &PreparedDirectGroupedAggregateInput<'_>,
    scratch: &mut DirectGroupedAggregateScratch,
) -> Option<FixedReductionInputs<N>> {
    if inputs.inputs.len() != N || scratch.decimal_sources.len() != N {
        return None;
    }
    let mut input_data = [std::ptr::null(); N];
    let mut scratch_data = [std::ptr::null_mut(); N];
    let mut direct_mask = 0_u8;
    for source_idx in 0..N {
        let input = &inputs.inputs[source_idx];
        let expects_i128 = WIDTH_MASK & (1_u8 << source_idx) != 0;
        input_data[source_idx] = match (expects_i128, input.data) {
            (false, PreparedDecimalData::I64(data)) => data.cast(),
            (true, PreparedDecimalData::I128(data)) => data.cast(),
            _ => return None,
        };
        scratch_data[source_idx] = match (expects_i128, &mut scratch.decimal_sources[source_idx]) {
            (false, DirectDecimalScratch::I64 { primary, .. }) => {
                primary.as_mut_slice().as_mut_ptr().cast()
            }
            (true, DirectDecimalScratch::I128 { primary, .. }) => {
                primary.as_mut_slice().as_mut_ptr().cast()
            }
            _ => return None,
        };
        if input.direct {
            direct_mask |= 1_u8 << source_idx;
        } else if !input.uses_shared_selection {
            return None;
        }
    }
    if direct_mask != ((1_u16 << N) - 1) as u8 && inputs.shared_selection_data.is_null() {
        return None;
    }
    Some(FixedReductionInputs {
        input_data,
        scratch_data,
        direct_mask,
        shared_selection: inputs.shared_selection_data,
    })
}

#[inline]
fn reduce_fixed_inputs<const N: usize, const WIDTH_MASK: u8, S: DirectGroupSlotSource>(
    prepared: FixedReductionInputs<N>,
    source: &mut S,
    scratch: &mut DirectGroupedAggregateScratch,
    traversal: ReducedSlotTraversal,
) -> Result<[bool; N]> {
    let mut overflowed = [false; N];
    for row in 0..source.len() {
        let slot = source.slot_at(row)?;
        if traversal == ReducedSlotTraversal::SparseTouched && scratch.row_counts[slot] == 0 {
            scratch.touched_slots.try_push(slot)?;
        }
        scratch.row_counts[slot] += 1;
        let shared_physical_row = if prepared.direct_mask == ((1_u16 << N) - 1) as u8 {
            row
        } else {
            unsafe { *prepared.shared_selection.add(row) as usize }
        };
        for source_idx in 0..N {
            let physical_row = if prepared.direct_mask & (1_u8 << source_idx) != 0 {
                row
            } else {
                shared_physical_row
            };
            if WIDTH_MASK & (1_u8 << source_idx) == 0 {
                let value = unsafe {
                    *prepared.input_data[source_idx]
                        .cast::<i64>()
                        .add(physical_row)
                };
                let target =
                    unsafe { &mut *prepared.scratch_data[source_idx].cast::<i64>().add(slot) };
                let (sum, did_overflow) = target.overflowing_add(value);
                *target = sum;
                overflowed[source_idx] |= did_overflow;
            } else {
                let value = unsafe {
                    *prepared.input_data[source_idx]
                        .cast::<i128>()
                        .add(physical_row)
                };
                let target =
                    unsafe { &mut *prepared.scratch_data[source_idx].cast::<i128>().add(slot) };
                let (sum, did_overflow) = target.overflowing_add(value);
                *target = sum;
                overflowed[source_idx] |= did_overflow;
            }
        }
    }
    Ok(overflowed)
}

fn decoded_selection_data(decoded: &DecodedVectorRef<'_>) -> Option<*const u32> {
    decoded.sel().materialized_indices().map(<[u32]>::as_ptr)
}

fn clear_reduced_source(
    source: &mut DirectDecimalScratch,
    row_counts: &[usize],
    touched_slots: &[usize],
    traversal: ReducedSlotTraversal,
) {
    let mut clear_slot = |slot: usize| match source {
        DirectDecimalScratch::I64 { primary, fallback } => {
            primary[slot] = 0;
            fallback[slot] = 0;
        }
        DirectDecimalScratch::I128 { primary, fallback } => {
            primary[slot] = 0;
            fallback[slot] = i256::ZERO;
        }
    };
    match traversal {
        ReducedSlotTraversal::DenseDomain => {
            for (slot, count) in row_counts.iter().copied().enumerate() {
                if count != 0 {
                    clear_slot(slot);
                }
            }
        }
        ReducedSlotTraversal::SparseTouched => {
            for &slot in touched_slots {
                clear_slot(slot);
            }
        }
    }
}

fn accounted_scratch_vec<T>(
    reservation: &MemoryGrant,
    capacity: usize,
    tag: MemoryTag,
    class: MemoryAccountingClass,
) -> Result<AccountedVec<T>> {
    let bytes = capacity
        .checked_mul(std::mem::size_of::<T>())
        .ok_or_else(|| paro_error::internal("direct aggregate scratch capacity overflow"))?;
    let mut result = AccountedVec::new_with_accounting(reservation.split(bytes)?, tag, class);
    result.try_reserve(capacity)?;
    Ok(result)
}
