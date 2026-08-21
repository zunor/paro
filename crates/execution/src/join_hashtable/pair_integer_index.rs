// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Exact open-addressed index for two-column BIGINT equality joins.
//!
//! A composite integer foreign key is common in analytical schemas. The
//! generic hash table stores salts and then repeats exact key comparison while
//! walking hash chains. This index stores one build-row pointer per occupied
//! key, resolves hash collisions during lookup, and links only true duplicate
//! keys through the build row's existing `next` field.

use std::sync::Arc;

use paro_common::allocator::Allocator;
use paro_common::error::{self as paro_error, Result};
use paro_common::memory::{GrantBuffer, MemoryAccountingContext};
use paro_common::vector::{Vector, VectorView};
use paro_storage::row::codec::unsafe_api;
use paro_storage::row::RowLayout;

const MAX_PAIR_INDEX_SLOTS: usize = 4 * 1024 * 1024;
const MAX_FAST_PAIR_AVERAGE_PROBES: usize = 2;
const MAX_FAST_PAIR_PROBE_CHAIN: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PairHashMode {
    /// One multiply on the usually low-cardinality right component. This is
    /// nearly collision-free for dense `(id, ordinal)` keys.
    LowLatency,
    /// Independent multiplication makes low-bit strides on either component
    /// reach the table mask before open addressing.
    StrideResistant,
}

#[derive(Debug)]
pub(super) struct ExactI64PairJoinIndex {
    slots: GrantBuffer,
    capacity: usize,
    mask: usize,
    hash_mode: PairHashMode,
    inserted_rows: usize,
    total_probe_steps: usize,
    max_probe_steps: usize,
}

impl ExactI64PairJoinIndex {
    pub(super) fn try_new(
        build_count: usize,
        allocator: Arc<dyn Allocator>,
        memory: &MemoryAccountingContext,
    ) -> Result<Option<Self>> {
        let Some(minimum_slots) = build_count.checked_mul(2) else {
            return Ok(None);
        };
        let capacity = minimum_slots.max(16).checked_next_power_of_two();
        let Some(capacity) = capacity.filter(|capacity| *capacity <= MAX_PAIR_INDEX_SLOTS) else {
            return Ok(None);
        };
        let Some(bytes) = capacity.checked_mul(std::mem::size_of::<usize>()) else {
            return Ok(None);
        };
        Ok(Some(Self {
            slots: memory.allocate_zeroed_buffer(allocator, bytes)?,
            capacity,
            mask: capacity - 1,
            hash_mode: PairHashMode::LowLatency,
            inserted_rows: 0,
            total_probe_steps: 0,
            max_probe_steps: 0,
        }))
    }

    pub(super) fn size_in_bytes(&self) -> usize {
        self.slots.size()
    }

    /// Insert a build row and return the previous head for a duplicate key.
    pub(super) fn insert(&mut self, layout: &RowLayout, row_ptr: usize) -> Result<Option<usize>> {
        if row_ptr == 0
            || row_key_is_null(layout, row_ptr, 0)
            || row_key_is_null(layout, row_ptr, 1)
        {
            return Err(paro_error::internal(
                "pair integer index received a NULL or invalid build row",
            ));
        }
        let left = read_row_i64(layout, row_ptr, 0);
        let right = read_row_i64(layout, row_ptr, 1);
        let mut slot = pair_hash(self.hash_mode, left, right) as usize & self.mask;
        for probe_steps in 1..=self.capacity {
            let slot_ptr = unsafe { self.slots.as_ptr().cast::<usize>().add(slot) };
            let existing = unsafe { std::ptr::read(slot_ptr) };
            if existing == 0 {
                unsafe { std::ptr::write(slot_ptr, row_ptr) };
                self.record_probe_steps(probe_steps);
                return Ok(None);
            }
            if read_row_i64(layout, existing, 0) == left
                && read_row_i64(layout, existing, 1) == right
            {
                unsafe { std::ptr::write(slot_ptr, row_ptr) };
                self.record_probe_steps(probe_steps);
                return Ok(Some(existing));
            }
            slot = (slot + 1) & self.mask;
        }
        Err(paro_error::internal(
            "pair integer join index exceeded its admitted load factor",
        ))
    }

    /// Switch once from the fast placement to the stride-resistant placement
    /// when the actual build keys prove that the fast assumption is false.
    /// Finalization owns every build row, so this one-time rebuild stays off
    /// the probe path and the selected mode becomes immutable at publication.
    pub(super) fn strengthen_hash_if_clustered(&mut self) -> bool {
        let average_is_clustered = self
            .inserted_rows
            .checked_mul(MAX_FAST_PAIR_AVERAGE_PROBES)
            .is_some_and(|budget| self.total_probe_steps > budget);
        if self.hash_mode != PairHashMode::LowLatency
            || (!average_is_clustered && self.max_probe_steps <= MAX_FAST_PAIR_PROBE_CHAIN)
        {
            return false;
        }
        // SAFETY: `slots` owns exactly `size()` writable bytes for the entire
        // mutable lifetime of this unpublished index.
        unsafe { std::ptr::write_bytes(self.slots.as_ptr(), 0, self.slots.size()) };
        self.hash_mode = PairHashMode::StrideResistant;
        self.inserted_rows = 0;
        self.total_probe_steps = 0;
        self.max_probe_steps = 0;
        true
    }

    #[cfg(test)]
    pub(super) fn uses_stride_resistant_hash(&self) -> bool {
        self.hash_mode == PairHashMode::StrideResistant
    }

    #[inline]
    fn record_probe_steps(&mut self, probe_steps: usize) {
        self.inserted_rows += 1;
        self.total_probe_steps = self.total_probe_steps.saturating_add(probe_steps);
        self.max_probe_steps = self.max_probe_steps.max(probe_steps);
    }

    pub(super) fn lookup_vector_rows(
        &self,
        left: &Vector,
        right: &Vector,
        vector_count: usize,
        probe_rows: &[u32],
        output_pointers: &mut [usize],
        matched_rows: &mut [u32],
        build_layout: &RowLayout,
    ) -> Result<usize> {
        let left = left.try_to_view(vector_count)?;
        let right = right.try_to_view(vector_count)?;
        let mut matched_count = 0usize;
        for &row in probe_rows {
            let row = row as usize;
            if !left.is_valid(row) || !right.is_valid(row) {
                continue;
            }
            let left_value = read_vector_i64(&left, row);
            let right_value = read_vector_i64(&right, row);
            let Some(pointer) = self.lookup(build_layout, left_value, right_value) else {
                continue;
            };
            output_pointers[row] = pointer;
            matched_rows[matched_count] = row as u32;
            matched_count += 1;
        }
        Ok(matched_count)
    }

    fn lookup(&self, layout: &RowLayout, left: i64, right: i64) -> Option<usize> {
        let mut slot = pair_hash(self.hash_mode, left, right) as usize & self.mask;
        for _ in 0..self.capacity {
            let pointer = unsafe { std::ptr::read(self.slots.as_ptr().cast::<usize>().add(slot)) };
            if pointer == 0 {
                return None;
            }
            if read_row_i64(layout, pointer, 0) == left && read_row_i64(layout, pointer, 1) == right
            {
                return Some(pointer);
            }
            slot = (slot + 1) & self.mask;
        }
        None
    }
}

#[inline]
fn pair_hash(mode: PairHashMode, left: i64, right: i64) -> u64 {
    let mixed = match mode {
        PairHashMode::LowLatency => {
            (left as u64).wrapping_add((right as u64).wrapping_mul(0x9e37_79b1_85eb_ca87))
        }
        PairHashMode::StrideResistant => {
            (left as u64).wrapping_mul(0x9e37_79b1_85eb_ca87)
                ^ (right as u64).wrapping_mul(0xff51_afd7_ed55_8ccd)
        }
    };
    mixed ^ (mixed >> 32)
}

#[inline]
fn read_vector_i64(view: &VectorView<'_>, row: usize) -> i64 {
    if let Some(data) = view.get_data::<i64>() {
        unsafe { *data.add(view.physical_index(row)) }
    } else {
        view.get_i64(row)
    }
}

#[inline]
fn row_key_is_null(layout: &RowLayout, row_ptr: usize, column: usize) -> bool {
    !layout.all_valid() && !unsafe { unsafe_api::row_is_valid(row_ptr as *const u8, column) }
}

#[inline]
fn read_row_i64(layout: &RowLayout, row_ptr: usize, column: usize) -> i64 {
    unsafe {
        std::ptr::read_unaligned(
            (row_ptr as *const u8)
                .add(layout.offsets()[column])
                .cast::<i64>(),
        )
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::{pair_hash, PairHashMode};

    #[test]
    fn pair_placement_breaks_aligned_low_bit_strides_on_both_keys() {
        const CAPACITY: u64 = 512;
        const KEY_COUNT: usize = 256;
        for pairs in [
            (0..KEY_COUNT)
                .map(|value| ((value * 256) as i64, 7))
                .collect::<Vec<_>>(),
            (0..KEY_COUNT)
                .map(|value| (3, (value * 86_400) as i64))
                .collect::<Vec<_>>(),
        ] {
            let occupied = pairs
                .into_iter()
                .map(|(left, right)| {
                    pair_hash(PairHashMode::StrideResistant, left, right) & (CAPACITY - 1)
                })
                .collect::<HashSet<_>>()
                .len();
            assert!(
                occupied >= KEY_COUNT * 3 / 4,
                "aligned composite keys occupied only {occupied} initial slots"
            );
        }
    }
}
