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

#[derive(Debug)]
pub(super) struct ExactI64PairJoinIndex {
    slots: GrantBuffer,
    capacity: usize,
    mask: usize,
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
        let mut slot = pair_hash(left, right) as usize & self.mask;
        for _ in 0..self.capacity {
            let slot_ptr = unsafe { self.slots.as_ptr().cast::<usize>().add(slot) };
            let existing = unsafe { std::ptr::read(slot_ptr) };
            if existing == 0 {
                unsafe { std::ptr::write(slot_ptr, row_ptr) };
                return Ok(None);
            }
            if read_row_i64(layout, existing, 0) == left
                && read_row_i64(layout, existing, 1) == right
            {
                unsafe { std::ptr::write(slot_ptr, row_ptr) };
                return Ok(Some(existing));
            }
            slot = (slot + 1) & self.mask;
        }
        Err(paro_error::internal(
            "pair integer join index exceeded its admitted load factor",
        ))
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
        let mut slot = pair_hash(left, right) as usize & self.mask;
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
fn pair_hash(left: i64, right: i64) -> u64 {
    // Power-of-two open addressing consumes the low bits, so both physical
    // keys must reach that domain before masking. One odd multiplicative mix
    // plus a high-half fold has substantially less dependency latency than a
    // general 128-bit hash combiner; exact row-key comparison still resolves
    // every collision, so this is a placement function rather than an
    // equality witness.
    let mixed = (left as u64).wrapping_add((right as u64).wrapping_mul(0x9e37_79b1_85eb_ca87));
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
