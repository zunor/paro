// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Compact occupancy map for direct-addressing aggregate slots.

use std::mem::size_of;
use std::ops::Range;

use paro_common::allocator::MemoryTag;
use paro_common::error::{self as paro_error, Result};
use paro_common::memory::{AccountedVec, MemoryAccountingClass, MemoryGrant};

use super::support::accounted_vec_from_reservation;

pub(super) const SLOT_WORD_BITS: usize = u64::BITS as usize;

#[derive(Debug)]
pub(super) struct SlotBitmap {
    words: AccountedVec<u64>,
    slots: usize,
}

impl SlotBitmap {
    pub(super) fn storage_bytes(slots: usize) -> Result<usize> {
        word_count(slots)
            .checked_mul(size_of::<u64>())
            .ok_or_else(|| paro_error::internal("perfect aggregate occupancy byte-size overflow"))
    }

    pub(super) fn try_from_reservation(
        slots: usize,
        reservation: &MemoryGrant,
        tag: MemoryTag,
        class: MemoryAccountingClass,
    ) -> Result<Self> {
        let words = word_count(slots);
        let mut storage = accounted_vec_from_reservation(reservation, words, tag, class)?;
        storage.try_resize_with(words, || 0)?;
        Ok(Self {
            words: storage,
            slots,
        })
    }

    #[inline(always)]
    pub(super) fn is_set(&self, slot: usize) -> Result<bool> {
        if slot >= self.slots {
            return Err(paro_error::internal(format!(
                "perfect aggregate occupancy slot out of bounds: slot={slot}, slots={}",
                self.slots
            )));
        }
        let mask = 1_u64 << (slot % SLOT_WORD_BITS);
        let value = self.words.get(slot / SLOT_WORD_BITS).ok_or_else(|| {
            paro_error::internal("perfect aggregate occupancy storage is inconsistent")
        })?;
        Ok(*value & mask != 0)
    }

    /// Mark a slot and return whether it transitioned from empty.
    #[inline(always)]
    pub(super) fn set(&mut self, slot: usize) -> Result<bool> {
        if slot >= self.slots {
            return Err(paro_error::internal(format!(
                "perfect aggregate occupancy slot out of bounds: slot={slot}, slots={}",
                self.slots
            )));
        }
        let word = slot / SLOT_WORD_BITS;
        let mask = 1_u64 << (slot % SLOT_WORD_BITS);
        let value = self.words.get_mut(word).ok_or_else(|| {
            paro_error::internal("perfect aggregate occupancy storage is inconsistent")
        })?;
        let inserted = *value & mask == 0;
        *value |= mask;
        Ok(inserted)
    }

    pub(super) fn clear(&mut self) {
        self.words.as_mut_slice().fill(0);
    }

    pub(super) fn capacity_bytes(&self) -> usize {
        self.words.capacity() * size_of::<u64>()
    }

    pub(super) fn words_ptr(&self) -> *const u64 {
        self.words.as_ptr()
    }

    pub(super) fn words_mut_ptr(&mut self) -> *mut u64 {
        self.words.as_mut_ptr()
    }

    pub(super) fn word_count(&self) -> usize {
        self.words.len()
    }

    pub(super) fn set_bits(&self, range: Range<usize>) -> SetBits<'_> {
        debug_assert!(range.start <= range.end);
        debug_assert!(range.end <= self.slots);
        SetBits::new(self.words.as_slice(), range)
    }
}

pub(super) struct SetBits<'a> {
    words: &'a [u64],
    word: usize,
    end_word: usize,
    current: u64,
    range: Range<usize>,
}

impl<'a> SetBits<'a> {
    fn new(words: &'a [u64], range: Range<usize>) -> Self {
        let word = range.start / SLOT_WORD_BITS;
        let end_word = range.end.div_ceil(SLOT_WORD_BITS);
        let mut result = Self {
            words,
            word,
            end_word,
            current: 0,
            range,
        };
        result.load_word();
        result
    }

    fn load_word(&mut self) {
        if self.word >= self.end_word {
            self.current = 0;
            return;
        }
        let mut bits = self.words[self.word];
        let word_start = self.word * SLOT_WORD_BITS;
        if self.range.start > word_start {
            bits &= u64::MAX << (self.range.start - word_start);
        }
        let word_end = word_start + SLOT_WORD_BITS;
        if self.range.end < word_end {
            bits &= u64::MAX >> (word_end - self.range.end);
        }
        self.current = bits;
    }
}

impl Iterator for SetBits<'_> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.current != 0 {
                let bit = self.current.trailing_zeros() as usize;
                self.current &= self.current - 1;
                return Some(self.word * SLOT_WORD_BITS + bit);
            }
            self.word += 1;
            if self.word >= self.end_word {
                return None;
            }
            self.load_word();
        }
    }
}

fn word_count(slots: usize) -> usize {
    slots.div_ceil(SLOT_WORD_BITS)
}

#[cfg(test)]
mod tests {
    use paro_common::allocator::MemoryTag;
    use paro_common::memory::{MemoryAccountingClass, MemoryAccountingContext};

    use super::SlotBitmap;

    #[test]
    fn set_bit_iteration_honors_unaligned_range_edges() {
        let memory = MemoryAccountingContext::detached(
            MemoryTag::HashTable,
            MemoryAccountingClass::Revocable,
        );
        let grant = memory
            .reserve_grant(SlotBitmap::storage_bytes(130).unwrap())
            .unwrap();
        let mut bitmap = SlotBitmap::try_from_reservation(
            130,
            &grant,
            MemoryTag::HashTable,
            MemoryAccountingClass::Metadata,
        )
        .unwrap();
        for slot in [0, 1, 63, 64, 65, 127, 128, 129] {
            assert!(bitmap.set(slot).unwrap());
            assert!(!bitmap.set(slot).unwrap());
        }

        assert_eq!(
            bitmap.set_bits(1..129).collect::<Vec<_>>(),
            vec![1, 63, 64, 65, 127, 128]
        );
        bitmap.clear();
        assert!(bitmap.set_bits(0..130).next().is_none());
    }
}
