// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Per-equality-group extrema used by fused existential reductions.

use std::ptr;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;

use paro_common::allocator::Allocator;
use paro_common::error::{self as paro_error, Result};
use paro_common::memory::{GrantBuffer, MemoryAccountingContext};

/// Concurrent `i64` extrema indexed by `(channel, equality-group slot)`.
///
/// Each source row updates only the channels whose source-local predicates
/// hold. After the source pipeline drains, a preserved row can decide
/// `EXISTS(value <> preserved_value)` from the channel's minimum and maximum.
pub(crate) struct GroupedReductionExtrema {
    slot_count: usize,
    channel_count: usize,
    minima: GrantBuffer,
    maxima: GrantBuffer,
    seen: GrantBuffer,
}

impl std::fmt::Debug for GroupedReductionExtrema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GroupedReductionExtrema")
            .field("slot_count", &self.slot_count)
            .field("channel_count", &self.channel_count)
            .finish()
    }
}

impl GroupedReductionExtrema {
    pub(crate) fn try_new(
        slot_count: usize,
        channel_count: usize,
        allocator: Arc<dyn Allocator>,
        memory: &MemoryAccountingContext,
    ) -> Result<Self> {
        if slot_count == 0 || channel_count == 0 {
            return Err(paro_error::internal(
                "grouped reduction extrema requires non-empty dimensions",
            ));
        }
        let cell_count = slot_count
            .checked_mul(channel_count)
            .ok_or_else(|| paro_error::out_of_range("grouped reduction extrema size overflow"))?;
        let value_bytes = cell_count
            .checked_mul(std::mem::size_of::<u64>())
            .ok_or_else(|| paro_error::out_of_range("grouped reduction extrema byte overflow"))?;
        let minima = memory.allocate_buffer(allocator.clone(), value_bytes)?;
        unsafe {
            // SAFETY: the allocation owns exactly `value_bytes` writable bytes
            // and is not shared until this constructor returns.
            ptr::write_bytes(minima.as_ptr(), 0xff, value_bytes);
        }
        Ok(Self {
            slot_count,
            channel_count,
            minima,
            maxima: memory.allocate_zeroed_buffer(allocator.clone(), value_bytes)?,
            seen: memory.allocate_zeroed_buffer(allocator, cell_count)?,
        })
    }

    #[inline]
    pub(crate) fn channel_count(&self) -> usize {
        self.channel_count
    }

    #[inline]
    fn cell(&self, slot: usize, channel: usize) -> Result<usize> {
        if slot >= self.slot_count || channel >= self.channel_count {
            return Err(paro_error::internal(
                "grouped reduction extrema index is out of bounds",
            ));
        }
        Ok(channel * self.slot_count + slot)
    }

    #[inline]
    pub(crate) fn update_i64_range(
        &self,
        slot: usize,
        channel: usize,
        minimum: i64,
        maximum: i64,
    ) -> Result<()> {
        let cell = self.cell(slot, channel)?;
        debug_assert!(minimum <= maximum);
        let minimum = encode_i64(minimum);
        let maximum = encode_i64(maximum);
        unsafe {
            // SAFETY: `cell` was checked against both allocator-backed arrays;
            // allocations are naturally aligned for atomic integer access.
            (&*self.minima.as_ptr().cast::<AtomicU64>().add(cell))
                .fetch_min(minimum, Ordering::Relaxed);
            (&*self.maxima.as_ptr().cast::<AtomicU64>().add(cell))
                .fetch_max(maximum, Ordering::Relaxed);
            (&*self.seen.as_ptr().cast::<AtomicU8>().add(cell)).store(1, Ordering::Relaxed);
        }
        Ok(())
    }

    #[inline]
    pub(crate) fn contains_unequal_i64(
        &self,
        slot: usize,
        channel: usize,
        value: i64,
    ) -> Result<bool> {
        let cell = self.cell(slot, channel)?;
        let seen = unsafe {
            // SAFETY: `cell` is within the seen array. The source pipeline's
            // completion barrier joins every writer before the unmatched-build
            // pipeline reads these relaxed atomics; `seen` is only an occupancy
            // flag and is not itself the publication mechanism.
            (&*self.seen.as_ptr().cast::<AtomicU8>().add(cell)).load(Ordering::Relaxed)
        };
        if seen == 0 {
            return Ok(false);
        }
        let ordinal = encode_i64(value);
        let minimum = unsafe {
            (&*self.minima.as_ptr().cast::<AtomicU64>().add(cell)).load(Ordering::Relaxed)
        };
        let maximum = unsafe {
            (&*self.maxima.as_ptr().cast::<AtomicU64>().add(cell)).load(Ordering::Relaxed)
        };
        Ok(minimum != ordinal || maximum != ordinal)
    }
}

#[inline]
fn encode_i64(value: i64) -> u64 {
    (value as u64) ^ (1_u64 << 63)
}

#[cfg(test)]
mod tests {
    use paro_common::allocator::MemoryTag;
    use paro_common::memory::{MemoryAccountingClass, MemoryAccountingContext};

    use super::GroupedReductionExtrema;

    #[test]
    fn extrema_decides_exists_not_equal_per_channel() {
        let memory = MemoryAccountingContext::detached(
            MemoryTag::HashTable,
            MemoryAccountingClass::NonRevocable,
        );
        let extrema = GroupedReductionExtrema::try_new(
            2,
            2,
            paro_common::test_utils::test_allocator(),
            &memory,
        )
        .expect("extrema");
        extrema.update_i64_range(0, 0, 7, 7).unwrap();
        extrema.update_i64_range(0, 0, 9, 9).unwrap();
        extrema.update_i64_range(0, 1, 7, 7).unwrap();

        assert!(extrema.contains_unequal_i64(0, 0, 7).unwrap());
        assert!(!extrema.contains_unequal_i64(0, 1, 7).unwrap());
        assert!(!extrema.contains_unequal_i64(1, 0, 7).unwrap());
    }
}
