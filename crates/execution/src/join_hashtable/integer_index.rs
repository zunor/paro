// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Exact indexes for unique, bounded integer join keys.
//!
//! Compact domains use a direct pointer array. Sparse domains use a bitset
//! plus rank metadata, keeping the probe path exact without paying one pointer
//! per absent key.

use std::sync::Arc;

use paro_common::allocator::Allocator;
use paro_common::error::{self as paro_error, Result};
use paro_common::hash::{combine_hash, hash_i64, hash_u128, hash_u64, HASH_SEED, NULL_HASH};
use paro_common::memory::{GrantBuffer, MemoryAccountingContext};
#[cfg(test)]
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_common::vector::{Vector, VectorView};
use paro_storage::row::codec::unsafe_api;
use paro_storage::row::RowLayout;

const MAX_INTEGER_JOIN_SLOTS: usize = 64 * 1024 * 1024;
const MAX_SLOTS_PER_BUILD_ROW: usize = 256;

const MAX_DIRECT_JOIN_SLOTS: usize = 1_048_576;
const MAX_DIRECT_SLOTS_PER_BUILD_ROW: usize = 8;
const MAX_RANKED_INDEX_BYTES: usize = 32 * 1024 * 1024;

const SIGNED_ORDINAL_MASK: u128 = 1_u128 << 127;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum IntegerKeyKind {
    TinyInt,
    SmallInt,
    Integer,
    BigInt,
    HugeInt,
    UTinyInt,
    USmallInt,
    UInteger,
    UBigInt,
    UHugeInt,
    Date,
    Timestamp,
    TimestampTz,
    Time,
}

impl IntegerKeyKind {
    pub(super) fn from_logical_type(ty: &LogicalType) -> Option<Self> {
        match ty {
            LogicalType::TinyInt => Some(Self::TinyInt),
            LogicalType::SmallInt => Some(Self::SmallInt),
            LogicalType::Integer => Some(Self::Integer),
            LogicalType::BigInt => Some(Self::BigInt),
            LogicalType::HugeInt => Some(Self::HugeInt),
            LogicalType::UTinyInt => Some(Self::UTinyInt),
            LogicalType::USmallInt => Some(Self::USmallInt),
            LogicalType::UInteger => Some(Self::UInteger),
            LogicalType::UBigInt => Some(Self::UBigInt),
            LogicalType::UHugeInt => Some(Self::UHugeInt),
            LogicalType::Date => Some(Self::Date),
            LogicalType::Timestamp => Some(Self::Timestamp),
            LogicalType::TimestampTz => Some(Self::TimestampTz),
            LogicalType::Time => Some(Self::Time),
            _ => None,
        }
    }

    #[cfg(test)]
    pub(super) fn value_ordinal(self, value: &Value) -> Option<u128> {
        match (self, value) {
            (Self::TinyInt, Value::TinyInt(value)) => Some(signed_ordinal(*value as i128)),
            (Self::SmallInt, Value::SmallInt(value)) => Some(signed_ordinal(*value as i128)),
            (Self::Integer, Value::Integer(value)) => Some(signed_ordinal(*value as i128)),
            (Self::BigInt, Value::BigInt(value)) => Some(signed_ordinal(*value as i128)),
            (Self::HugeInt, Value::HugeInt(value)) => Some(signed_ordinal(*value)),
            (Self::UTinyInt, Value::UTinyInt(value)) => Some(*value as u128),
            (Self::USmallInt, Value::USmallInt(value)) => Some(*value as u128),
            (Self::UInteger, Value::UInteger(value)) => Some(*value as u128),
            (Self::UBigInt, Value::UBigInt(value)) => Some(*value as u128),
            (Self::UHugeInt, Value::UHugeInt(value)) => Some(*value),
            (Self::Date, Value::Date(value)) => Some(signed_ordinal(*value as i128)),
            (Self::Timestamp, Value::Timestamp(value)) => Some(signed_ordinal(*value as i128)),
            (Self::TimestampTz, Value::TimestampTz(value)) => Some(signed_ordinal(*value as i128)),
            (Self::Time, Value::Time(value)) => Some(signed_ordinal(*value as i128)),
            _ => None,
        }
    }

    /// Read a key directly from its serialized build row.
    ///
    /// Integer-index finalization visits every build row at least once. Keeping
    /// this conversion in the physical domain avoids materializing a boxed
    /// [`Value`] merely to recover the integer that is already in the row.
    pub(super) fn row_ordinal(
        self,
        layout: &RowLayout,
        row_ptr: *const u8,
        column_idx: usize,
    ) -> Option<u128> {
        if !layout.all_valid() && !unsafe { unsafe_api::row_is_valid(row_ptr, column_idx) } {
            return None;
        }
        let value_ptr = unsafe { row_ptr.add(layout.offsets()[column_idx]) };
        macro_rules! read {
            ($ty:ty) => {{
                unsafe { std::ptr::read_unaligned(value_ptr as *const $ty) }
            }};
        }
        Some(match self {
            Self::TinyInt => signed_ordinal(read!(i8) as i128),
            Self::SmallInt => signed_ordinal(read!(i16) as i128),
            Self::Integer | Self::Date => signed_ordinal(read!(i32) as i128),
            Self::BigInt | Self::Timestamp | Self::TimestampTz | Self::Time => {
                signed_ordinal(read!(i64) as i128)
            }
            Self::HugeInt => signed_ordinal(read!(i128)),
            Self::UTinyInt => u128::from(read!(u8)),
            Self::USmallInt => u128::from(read!(u16)),
            Self::UInteger => u128::from(read!(u32)),
            Self::UBigInt => u128::from(read!(u64)),
            Self::UHugeInt => read!(u128),
        })
    }

    /// Hash one serialized build key with the same physical kernel used by
    /// [`super::hash_kernel::JoinKeyLayout`]. Adaptive exact-index builds call
    /// this only when they must fall back to the generic hash table or spill.
    pub(super) fn row_hash(self, layout: &RowLayout, row_ptr: *const u8, column_idx: usize) -> u64 {
        if !layout.all_valid() && !unsafe { unsafe_api::row_is_valid(row_ptr, column_idx) } {
            return combine_hash(HASH_SEED, NULL_HASH);
        }
        let value_ptr = unsafe { row_ptr.add(layout.offsets()[column_idx]) };
        macro_rules! read {
            ($ty:ty) => {{
                unsafe { std::ptr::read_unaligned(value_ptr.cast::<$ty>()) }
            }};
        }
        let value_hash = match self {
            Self::TinyInt => hash_i64(read!(i8) as i64),
            Self::SmallInt => hash_i64(read!(i16) as i64),
            Self::Integer | Self::Date => hash_i64(read!(i32) as i64),
            Self::BigInt | Self::Timestamp | Self::TimestampTz | Self::Time => hash_i64(read!(i64)),
            Self::HugeInt => hash_u128(read!(i128) as u128),
            Self::UTinyInt => hash_u64(read!(u8) as u64),
            Self::USmallInt => hash_u64(read!(u16) as u64),
            Self::UInteger => hash_u64(read!(u32) as u64),
            Self::UBigInt => hash_u64(read!(u64)),
            Self::UHugeInt => hash_u128(read!(u128)),
        };
        combine_hash(HASH_SEED, value_hash)
    }

    /// Compute one build batch's physical domain while its key vector is hot.
    /// This moves min/max work out of the serial finalize phase.
    pub(super) fn selected_bounds(
        self,
        vector: &Vector,
        logical_count: usize,
        selected: &[u32],
    ) -> Result<Option<(u128, u128)>> {
        if selected.is_empty() {
            return Ok(None);
        }
        let view = vector.try_to_view(logical_count)?;
        let mut minimum = u128::MAX;
        let mut maximum = 0_u128;

        macro_rules! bounds {
            ($ty:ty, $ordinal:expr) => {{
                let Some(data) = view.get_data::<$ty>() else {
                    return self.selected_sequence_bounds(&view, selected);
                };
                let to_ordinal = $ordinal;
                for &row in selected {
                    let row = row as usize;
                    if !view.is_valid(row) {
                        return Ok(None);
                    }
                    // SAFETY: the vector view validates its physical storage
                    // and selected rows are bounded by the build selection.
                    let value = unsafe { *data.add(view.physical_index(row)) };
                    let ordinal = to_ordinal(value);
                    minimum = minimum.min(ordinal);
                    maximum = maximum.max(ordinal);
                }
            }};
        }

        match self {
            Self::TinyInt => bounds!(i8, |value| signed_ordinal(value as i128)),
            Self::SmallInt => bounds!(i16, |value| signed_ordinal(value as i128)),
            Self::Integer | Self::Date => bounds!(i32, |value| signed_ordinal(value as i128)),
            Self::BigInt | Self::Timestamp | Self::TimestampTz | Self::Time => {
                bounds!(i64, |value| signed_ordinal(value as i128))
            }
            Self::HugeInt => bounds!(i128, signed_ordinal),
            Self::UTinyInt => bounds!(u8, u128::from),
            Self::USmallInt => bounds!(u16, u128::from),
            Self::UInteger => bounds!(u32, u128::from),
            Self::UBigInt => bounds!(u64, u128::from),
            Self::UHugeInt => bounds!(u128, |value| value),
        }
        Ok(Some((minimum, maximum)))
    }

    fn selected_sequence_bounds(
        self,
        view: &VectorView<'_>,
        selected: &[u32],
    ) -> Result<Option<(u128, u128)>> {
        if matches!(
            self,
            Self::UTinyInt | Self::USmallInt | Self::UInteger | Self::UBigInt | Self::UHugeInt
        ) {
            return Ok(None);
        }
        let mut minimum = u128::MAX;
        let mut maximum = 0_u128;
        for &row in selected {
            let row = row as usize;
            if !view.is_valid(row) {
                return Ok(None);
            }
            let ordinal = signed_ordinal(view.get_i64(row) as i128);
            minimum = minimum.min(ordinal);
            maximum = maximum.max(ordinal);
        }
        Ok(Some((minimum, maximum)))
    }
}

#[derive(Debug, Clone, Copy)]
struct IntegerIndexDomain {
    kind: IntegerKeyKind,
    min_ordinal: u128,
    len: usize,
}

impl IntegerIndexDomain {
    #[inline]
    fn index_for_ordinal(self, ordinal: u128) -> Option<usize> {
        ordinal
            .checked_sub(self.min_ordinal)
            .and_then(|offset| usize::try_from(offset).ok())
            .filter(|index| *index < self.len)
    }
}

/// Mutable construction state for an exact integer join index.
///
/// This type cannot be published to a probe pipeline. [`Self::finish`] consumes
/// it and returns the immutable [`ExactIntegerJoinIndex`] only after every
/// ranked pointer has been initialized.
pub(super) struct ExactIntegerJoinIndexBuilder {
    domain: IntegerIndexDomain,
    storage: IntegerJoinBuildStorage,
    allocator: Arc<dyn Allocator>,
    memory: MemoryAccountingContext,
}

#[derive(Debug)]
enum IntegerJoinBuildStorage {
    Direct {
        bits: GrantBuffer,
        pointers: GrantBuffer,
        count: usize,
        inserted: usize,
    },
    Ranked {
        bits: GrantBuffer,
        domain_indices: Option<GrantBuffer>,
        pointers: GrantBuffer,
        count: usize,
        inserted: usize,
        monotonic: bool,
        last_index: Option<usize>,
    },
}

/// Immutable after publication by [`super::table::JoinHashTable::finalize`].
#[derive(Debug)]
pub(super) struct ExactIntegerJoinIndex {
    domain: IntegerIndexDomain,
    storage: IntegerJoinStorage,
}

#[derive(Debug)]
enum IntegerJoinStorage {
    Direct {
        bits: GrantBuffer,
        pointers: GrantBuffer,
    },
    Ranked {
        bits: GrantBuffer,
        ranks: GrantBuffer,
        pointers: GrantBuffer,
        count: usize,
    },
}

impl ExactIntegerJoinIndexBuilder {
    pub(super) fn try_new(
        kind: IntegerKeyKind,
        min_ordinal: u128,
        max_ordinal: u128,
        build_count: usize,
        allocator: Arc<dyn Allocator>,
        memory: &MemoryAccountingContext,
    ) -> Result<Option<Self>> {
        let range = max_ordinal.checked_sub(min_ordinal).ok_or_else(|| {
            paro_error::internal("integer join key range is not monotonically ordered")
        })?;
        let len = usize::try_from(range)
            .ok()
            .and_then(|range| range.checked_add(1))
            .ok_or_else(|| paro_error::internal("integer join key range exceeds address space"))?;
        if len > MAX_INTEGER_JOIN_SLOTS || len > build_count.saturating_mul(MAX_SLOTS_PER_BUILD_ROW)
        {
            return Ok(None);
        }
        let direct_pointer_bytes = len
            .checked_mul(std::mem::size_of::<usize>())
            .ok_or_else(|| paro_error::internal("direct join index byte size overflow"))?;
        let direct_word_count = len.div_ceil(u64::BITS as usize);
        let direct_bit_bytes = direct_word_count
            .checked_mul(std::mem::size_of::<u64>())
            .ok_or_else(|| paro_error::internal("direct join bitmap byte size overflow"))?;
        let storage = if len <= MAX_DIRECT_JOIN_SLOTS
            && len <= build_count.saturating_mul(MAX_DIRECT_SLOTS_PER_BUILD_ROW)
        {
            IntegerJoinBuildStorage::Direct {
                bits: memory.allocate_zeroed_buffer(allocator.clone(), direct_bit_bytes)?,
                pointers: memory.allocate_buffer(allocator.clone(), direct_pointer_bytes)?,
                count: build_count,
                inserted: 0,
            }
        } else {
            let word_count = len.div_ceil(u64::BITS as usize);
            let bit_bytes = word_count
                .checked_mul(std::mem::size_of::<u64>())
                .ok_or_else(|| paro_error::internal("ranked join bitset size overflow"))?;
            let rank_bytes = word_count
                .checked_mul(std::mem::size_of::<u32>())
                .ok_or_else(|| paro_error::internal("ranked join rank size overflow"))?;
            let pointer_bytes = build_count
                .checked_mul(std::mem::size_of::<usize>())
                .ok_or_else(|| paro_error::internal("ranked join pointer size overflow"))?;
            let build_pointer_bytes = build_count
                .checked_mul(std::mem::size_of::<usize>())
                .ok_or_else(|| paro_error::internal("ranked join build-pointer size overflow"))?;
            let final_bytes = bit_bytes
                .checked_add(rank_bytes)
                .and_then(|bytes| bytes.checked_add(pointer_bytes))
                .ok_or_else(|| paro_error::internal("ranked join index size overflow"))?;
            if final_bytes > MAX_RANKED_INDEX_BYTES {
                return Ok(None);
            }
            IntegerJoinBuildStorage::Ranked {
                bits: memory.allocate_zeroed_buffer(allocator.clone(), bit_bytes)?,
                domain_indices: None,
                pointers: memory.allocate_buffer(allocator.clone(), build_pointer_bytes)?,
                count: build_count,
                inserted: 0,
                monotonic: true,
                last_index: None,
            }
        };
        Ok(Some(Self {
            domain: IntegerIndexDomain {
                kind,
                min_ordinal,
                len,
            },
            storage,
            allocator,
            memory: memory.clone(),
        }))
    }

    /// Insert a unique build key. Returns `false` when the slot was occupied.
    pub(super) fn insert(&mut self, ordinal: u128, row_ptr: usize) -> Result<bool> {
        if row_ptr == 0 {
            return Err(paro_error::internal(
                "integer join index cannot store a null build-row pointer",
            ));
        }
        let Some(index) = self.domain.index_for_ordinal(ordinal) else {
            return Err(paro_error::internal(
                "integer join build key fell outside its measured domain",
            ));
        };
        self.insert_index(index, row_ptr)
    }

    fn insert_index(&mut self, index: usize, row_ptr: usize) -> Result<bool> {
        debug_assert!(index < self.domain.len);
        match &mut self.storage {
            IntegerJoinBuildStorage::Direct {
                bits,
                pointers,
                count,
                inserted,
            } => {
                let words = unsafe {
                    grant_slice_mut::<u64>(bits, self.domain.len.div_ceil(u64::BITS as usize))
                };
                let target_word = index / u64::BITS as usize;
                let mask = 1_u64 << (index % u64::BITS as usize);
                if words[target_word] & mask != 0 {
                    return Ok(false);
                }
                if *inserted >= *count {
                    return Err(paro_error::internal(
                        "direct join index received more rows than planned",
                    ));
                }
                let target = unsafe { pointers.as_ptr().cast::<usize>().add(index) };
                unsafe {
                    // SAFETY: `index` is inside the allocated pointer domain,
                    // the occupancy bit proves this slot has not been written,
                    // and the builder is exclusively owned until publication.
                    std::ptr::write(target, row_ptr);
                }
                words[target_word] |= mask;
                *inserted += 1;
            }
            IntegerJoinBuildStorage::Ranked {
                bits,
                domain_indices,
                pointers,
                count,
                inserted,
                monotonic,
                last_index,
            } => {
                let words = unsafe {
                    grant_slice_mut::<u64>(bits, self.domain.len.div_ceil(u64::BITS as usize))
                };
                let target_word = index / u64::BITS as usize;
                let mask = 1_u64 << (index % u64::BITS as usize);
                if words[target_word] & mask != 0 {
                    return Ok(false);
                }
                if *inserted >= *count {
                    return Err(paro_error::internal(
                        "ranked join index received more rows than planned",
                    ));
                }
                let compact_index = u32::try_from(index)
                    .map_err(|_| paro_error::internal("ranked join domain index exceeds u32"))?;
                let remains_monotonic = last_index.is_none_or(|previous| previous < index);
                if *monotonic && !remains_monotonic {
                    let bytes = count
                        .checked_mul(std::mem::size_of::<u32>())
                        .ok_or_else(|| {
                            paro_error::internal("ranked join domain-index size overflow")
                        })?;
                    let indices = self.memory.allocate_buffer(self.allocator.clone(), bytes)?;
                    let indices_ptr = indices.as_ptr().cast::<u32>();
                    let mut output = 0usize;
                    for (word_idx, word) in words.iter().copied().enumerate() {
                        let mut remaining = word;
                        while remaining != 0 {
                            let bit = remaining.trailing_zeros() as usize;
                            let previous = word_idx * u64::BITS as usize + bit;
                            unsafe {
                                // SAFETY: the occupancy bitmap contains exactly
                                // `inserted` previous unique keys. Their strict
                                // monotonicity makes bitmap order identical to
                                // the existing pointer stream order.
                                std::ptr::write(indices_ptr.add(output), previous as u32);
                            }
                            output += 1;
                            remaining &= remaining - 1;
                        }
                    }
                    debug_assert_eq!(output, *inserted);
                    *domain_indices = Some(indices);
                    *monotonic = false;
                }
                words[target_word] |= mask;
                unsafe {
                    // SAFETY: `inserted` is below the allocated pointer stream;
                    // the optional domain stream has the same capacity and is
                    // exclusively owned until `finish` verifies it is full.
                    if let Some(domain_indices) = domain_indices {
                        std::ptr::write(
                            domain_indices.as_ptr().cast::<u32>().add(*inserted),
                            compact_index,
                        );
                    }
                    std::ptr::write(pointers.as_ptr().cast::<usize>().add(*inserted), row_ptr);
                }
                *last_index = Some(index);
                *inserted += 1;
            }
        }
        Ok(true)
    }

    /// Consume the mutable builder and return a probe-safe immutable index.
    ///
    /// Direct indexes are already fully initialized after insertion. Ranked
    /// indexes retain separate domain-index and pointer streams while validating
    /// uniqueness. A monotonically increasing build stream is already in rank
    /// order and its pointer buffer can be published directly; arbitrary input
    /// order falls back to a rank scatter without revisiting the row store.
    pub(super) fn finish(
        self,
        allocator: Arc<dyn Allocator>,
        memory: &MemoryAccountingContext,
    ) -> Result<ExactIntegerJoinIndex> {
        let Self {
            domain,
            storage,
            allocator: _,
            memory: _,
        } = self;
        let (bits, domain_indices, build_pointers, count, inserted, monotonic) = match storage {
            IntegerJoinBuildStorage::Direct {
                bits,
                pointers,
                count,
                inserted,
            } => {
                if inserted != count {
                    return Err(paro_error::internal(format!(
                        "direct join index row count mismatch: expected={count}, actual={inserted}",
                    )));
                }
                return Ok(ExactIntegerJoinIndex {
                    domain,
                    storage: IntegerJoinStorage::Direct { bits, pointers },
                });
            }
            IntegerJoinBuildStorage::Ranked {
                bits,
                domain_indices,
                pointers,
                count,
                inserted,
                monotonic,
                last_index: _,
            } => (bits, domain_indices, pointers, count, inserted, monotonic),
        };
        if inserted != count {
            return Err(paro_error::internal(format!(
                "ranked join index row count mismatch: expected={count}, actual={inserted}",
            )));
        }

        let word_count = domain.len.div_ceil(u64::BITS as usize);
        let rank_bytes = word_count
            .checked_mul(std::mem::size_of::<u32>())
            .ok_or_else(|| paro_error::internal("ranked join rank size overflow"))?;
        let pointer_bytes = count
            .checked_mul(std::mem::size_of::<usize>())
            .ok_or_else(|| paro_error::internal("ranked join pointer size overflow"))?;
        let ranks = memory.allocate_buffer(allocator.clone(), rank_bytes)?;
        let words = unsafe { grant_slice::<u64>(&bits, word_count) };
        let ranks_ptr = ranks.as_ptr().cast::<u32>();
        let mut running = 0_u32;
        for (word_idx, word) in words.iter().copied().enumerate() {
            unsafe {
                // SAFETY: `word_idx` is inside the allocated rank buffer and
                // each slot is initialized once before any shared reference is formed.
                std::ptr::write(ranks_ptr.add(word_idx), running);
            }
            running = running
                .checked_add(word.count_ones())
                .ok_or_else(|| paro_error::internal("ranked join cardinality overflow"))?;
        }
        if running as usize != count {
            return Err(paro_error::internal(
                "ranked join bitset cardinality does not match build rows",
            ));
        }

        let pointers = if monotonic {
            build_pointers
        } else {
            let pointers = memory.allocate_buffer(allocator, pointer_bytes)?;
            let rank_values = unsafe { grant_slice::<u32>(&ranks, word_count) };
            let domain_indices = domain_indices.ok_or_else(|| {
                paro_error::internal("non-monotonic ranked join index has no domain stream")
            })?;
            let build_indices = unsafe { grant_slice::<u32>(&domain_indices, count) };
            let source_pointers = unsafe { grant_slice::<usize>(&build_pointers, count) };
            let pointers_ptr = pointers.as_ptr().cast::<usize>();
            for (&domain_index, &row_ptr) in build_indices.iter().zip(source_pointers) {
                let slot = ranked_slot(words, rank_values, domain_index as usize);
                debug_assert!(slot < count);
                // Every domain index was admitted through a unique occupancy
                // bit. Rank therefore maps entries bijectively to `0..count`.
                unsafe {
                    // SAFETY: `slot` is inside the pointer buffer and the rank
                    // bijection guarantees that no two entries alias it.
                    std::ptr::write(pointers_ptr.add(slot), row_ptr);
                }
            }
            pointers
        };
        Ok(ExactIntegerJoinIndex {
            domain,
            storage: IntegerJoinStorage::Ranked {
                bits,
                ranks,
                pointers,
                count,
            },
        })
    }
}

impl ExactIntegerJoinIndex {
    pub(super) fn size_in_bytes(&self) -> usize {
        match &self.storage {
            IntegerJoinStorage::Direct { bits, pointers } => bits.size() + pointers.size(),
            IntegerJoinStorage::Ranked {
                bits,
                ranks,
                pointers,
                ..
            } => bits.size() + ranks.size() + pointers.size(),
        }
    }

    /// Probe a prepared selection of one integer vector.
    ///
    /// The physical type and index representation are dispatched once per
    /// batch. This avoids repeating vector decoding, logical-type dispatch,
    /// and index-representation dispatch for every probe row.
    pub(super) fn lookup_vector_rows(
        &self,
        vector: &Vector,
        vector_count: usize,
        probe_rows: &[u32],
        output_pointers: &mut [usize],
        matched_rows: &mut [u32],
    ) -> Result<usize> {
        let view = vector.try_to_view(vector_count)?;
        match &self.storage {
            IntegerJoinStorage::Direct { bits, pointers } => {
                let words = unsafe {
                    grant_slice::<u64>(bits, self.domain.len.div_ceil(u64::BITS as usize))
                };
                self.lookup_vector_rows_with(
                    &view,
                    probe_rows,
                    output_pointers,
                    matched_rows,
                    |ordinal| {
                        let index = self.domain.index_for_ordinal(ordinal)?;
                        let word = words[index / u64::BITS as usize];
                        let mask = 1_u64 << (index % u64::BITS as usize);
                        if word & mask == 0 {
                            return None;
                        }
                        let pointer = unsafe {
                            // SAFETY: an occupied bit is only published after
                            // the exclusive builder initializes this slot.
                            std::ptr::read(pointers.as_ptr().cast::<usize>().add(index))
                        };
                        Some(pointer)
                    },
                )
            }
            IntegerJoinStorage::Ranked {
                bits,
                ranks,
                pointers,
                count,
            } => {
                let words = unsafe {
                    grant_slice::<u64>(bits, self.domain.len.div_ceil(u64::BITS as usize))
                };
                let rank_values = unsafe { grant_slice::<u32>(ranks, words.len()) };
                let pointer_values = unsafe { grant_slice::<usize>(pointers, *count) };
                self.lookup_vector_rows_with(
                    &view,
                    probe_rows,
                    output_pointers,
                    matched_rows,
                    |ordinal| {
                        let index = self.domain.index_for_ordinal(ordinal)?;
                        let word = words[index / u64::BITS as usize];
                        let mask = 1_u64 << (index % u64::BITS as usize);
                        if word & mask == 0 {
                            return None;
                        }
                        let slot = ranked_slot(words, rank_values, index);
                        Some(pointer_values[slot])
                    },
                )
            }
        }
    }

    fn lookup_vector_rows_with(
        &self,
        view: &VectorView<'_>,
        probe_rows: &[u32],
        output_pointers: &mut [usize],
        matched_rows: &mut [u32],
        lookup: impl Fn(u128) -> Option<usize>,
    ) -> Result<usize> {
        macro_rules! probe {
            ($ty:ty, $ordinal:expr) => {{
                let Some(data) = view.get_data::<$ty>() else {
                    return self.lookup_vector_rows_fallback(
                        view,
                        probe_rows,
                        output_pointers,
                        matched_rows,
                        lookup,
                    );
                };
                let to_ordinal = $ordinal;
                let mut matched_count = 0usize;
                for &row_idx in probe_rows {
                    let row_idx = row_idx as usize;
                    debug_assert!(view.is_valid(row_idx));
                    // SAFETY: try_to_view validates the physical vector and
                    // probe_rows were bounded and NULL-filtered by prepare_keys.
                    let value = unsafe { *data.add(view.physical_index(row_idx)) };
                    if let Some(pointer) = lookup(to_ordinal(value)) {
                        output_pointers[row_idx] = pointer;
                        matched_rows[matched_count] = row_idx as u32;
                        matched_count += 1;
                    }
                }
                Ok(matched_count)
            }};
        }

        match self.domain.kind {
            IntegerKeyKind::TinyInt => probe!(i8, |value| signed_ordinal(value as i128)),
            IntegerKeyKind::SmallInt => probe!(i16, |value| signed_ordinal(value as i128)),
            IntegerKeyKind::Integer | IntegerKeyKind::Date => {
                probe!(i32, |value| signed_ordinal(value as i128))
            }
            IntegerKeyKind::BigInt
            | IntegerKeyKind::Timestamp
            | IntegerKeyKind::TimestampTz
            | IntegerKeyKind::Time => probe!(i64, |value| signed_ordinal(value as i128)),
            IntegerKeyKind::HugeInt => probe!(i128, signed_ordinal),
            IntegerKeyKind::UTinyInt => probe!(u8, u128::from),
            IntegerKeyKind::USmallInt => probe!(u16, u128::from),
            IntegerKeyKind::UInteger => probe!(u32, u128::from),
            IntegerKeyKind::UBigInt => probe!(u64, u128::from),
            IntegerKeyKind::UHugeInt => probe!(u128, |value| value),
        }
    }

    fn lookup_vector_rows_fallback(
        &self,
        view: &VectorView<'_>,
        probe_rows: &[u32],
        output_pointers: &mut [usize],
        matched_rows: &mut [u32],
        lookup: impl Fn(u128) -> Option<usize>,
    ) -> Result<usize> {
        if matches!(
            self.domain.kind,
            IntegerKeyKind::UTinyInt
                | IntegerKeyKind::USmallInt
                | IntegerKeyKind::UInteger
                | IntegerKeyKind::UBigInt
                | IntegerKeyKind::UHugeInt
        ) {
            return Err(paro_error::internal(
                "unsigned integer join sequence has no unsigned physical representation",
            ));
        }
        let mut matched_count = 0usize;
        for &row_idx in probe_rows {
            let row_idx = row_idx as usize;
            debug_assert!(view.is_valid(row_idx));
            let ordinal = signed_ordinal(view.get_i64(row_idx) as i128);
            if let Some(pointer) = lookup(ordinal) {
                output_pointers[row_idx] = pointer;
                matched_rows[matched_count] = row_idx as u32;
                matched_count += 1;
            }
        }
        Ok(matched_count)
    }
}

#[inline]
fn ranked_slot(words: &[u64], ranks: &[u32], index: usize) -> usize {
    let word_idx = index / u64::BITS as usize;
    let bit_idx = index % u64::BITS as usize;
    let lower_mask = (1_u64 << bit_idx).wrapping_sub(1);
    ranks[word_idx] as usize + (words[word_idx] & lower_mask).count_ones() as usize
}

unsafe fn grant_slice<T>(buffer: &GrantBuffer, len: usize) -> &[T] {
    debug_assert!(len
        .checked_mul(std::mem::size_of::<T>())
        .is_some_and(|bytes| bytes <= buffer.size()));
    debug_assert!((buffer.as_ptr() as usize).is_multiple_of(std::mem::align_of::<T>()));
    // SAFETY: every caller supplies the element type and length used to size
    // this allocator-backed buffer. Production allocators guarantee at least
    // eight-byte alignment, which covers all index element types.
    unsafe { std::slice::from_raw_parts(buffer.as_ptr().cast::<T>(), len) }
}

unsafe fn grant_slice_mut<T>(buffer: &mut GrantBuffer, len: usize) -> &mut [T] {
    debug_assert!(len
        .checked_mul(std::mem::size_of::<T>())
        .is_some_and(|bytes| bytes <= buffer.size()));
    debug_assert!((buffer.as_ptr() as usize).is_multiple_of(std::mem::align_of::<T>()));
    // SAFETY: see `grant_slice`; the mutable borrow of the owning buffer also
    // guarantees exclusive access to the returned slice.
    unsafe { std::slice::from_raw_parts_mut(buffer.as_ptr().cast::<T>(), len) }
}

#[inline]
fn signed_ordinal(value: i128) -> u128 {
    (value as u128) ^ SIGNED_ORDINAL_MASK
}

#[cfg(test)]
mod tests {
    use paro_common::allocator::MemoryTag;
    use paro_common::memory::{MemoryAccountingClass, MemoryAccountingContext};
    use paro_common::runtime_value::Value;

    use super::{ExactIntegerJoinIndexBuilder, IntegerJoinBuildStorage, IntegerKeyKind};

    #[test]
    fn signed_ordinals_preserve_order_across_zero() {
        let kind = IntegerKeyKind::Integer;
        let negative = kind
            .value_ordinal(&Value::Integer(-1))
            .expect("negative ordinal");
        let zero = kind
            .value_ordinal(&Value::Integer(0))
            .expect("zero ordinal");
        let positive = kind
            .value_ordinal(&Value::Integer(1))
            .expect("positive ordinal");
        assert_eq!(zero - negative, 1);
        assert_eq!(positive - zero, 1);
        let minimum = kind
            .value_ordinal(&Value::Integer(i32::MIN))
            .expect("minimum ordinal");
        let maximum = kind
            .value_ordinal(&Value::Integer(i32::MAX))
            .expect("maximum ordinal");
        assert_eq!(
            maximum
                .checked_sub(minimum)
                .and_then(|offset| usize::try_from(offset).ok()),
            Some(u32::MAX as usize)
        );
    }

    #[test]
    fn exact_index_rejects_duplicate_build_keys() {
        let allocator = paro_common::test_utils::test_allocator();
        let memory = MemoryAccountingContext::detached(
            MemoryTag::HashTable,
            MemoryAccountingClass::NonRevocable,
        );
        let min = IntegerKeyKind::Integer
            .value_ordinal(&Value::Integer(10))
            .expect("min");
        let max = IntegerKeyKind::Integer
            .value_ordinal(&Value::Integer(20))
            .expect("max");
        let mut index = ExactIntegerJoinIndexBuilder::try_new(
            IntegerKeyKind::Integer,
            min,
            max,
            1,
            allocator,
            &memory,
        )
        .expect("integer index allocation")
        .expect("eligible integer index");
        let ordinal = IntegerKeyKind::Integer
            .value_ordinal(&Value::Integer(12))
            .expect("ordinal");
        assert!(index.insert(ordinal, 1).expect("first insert"));
        assert!(!index.insert(ordinal, 2).expect("duplicate insert"));
    }

    #[test]
    fn direct_index_cannot_publish_before_all_rows_are_inserted() {
        let allocator = paro_common::test_utils::test_allocator();
        let memory = MemoryAccountingContext::detached(
            MemoryTag::HashTable,
            MemoryAccountingClass::NonRevocable,
        );
        let kind = IntegerKeyKind::Integer;
        let min = kind.value_ordinal(&Value::Integer(10)).expect("minimum");
        let max = kind.value_ordinal(&Value::Integer(13)).expect("maximum");
        let mut index =
            ExactIntegerJoinIndexBuilder::try_new(kind, min, max, 2, allocator.clone(), &memory)
                .expect("direct index allocation")
                .expect("eligible direct index");
        assert!(index.insert(min, 100).expect("insert first row"));

        let error = index
            .finish(allocator, &memory)
            .expect_err("incomplete direct index must not be published");
        assert!(error.to_string().contains("row count mismatch"));
    }

    #[test]
    fn ranked_index_maps_sparse_keys_to_compact_pointer_slots() {
        let allocator = paro_common::test_utils::test_allocator();
        let memory = MemoryAccountingContext::detached(
            MemoryTag::HashTable,
            MemoryAccountingClass::NonRevocable,
        );
        let kind = IntegerKeyKind::Integer;
        let min = kind.value_ordinal(&Value::Integer(10)).expect("minimum");
        let max = kind.value_ordinal(&Value::Integer(18)).expect("maximum");
        let mut index =
            ExactIntegerJoinIndexBuilder::try_new(kind, min, max, 1, allocator.clone(), &memory)
                .expect("ranked index allocation")
                .expect("eligible ranked index");
        let ordinal = kind
            .value_ordinal(&Value::Integer(14))
            .expect("key ordinal");
        assert!(index.insert(ordinal, 123).expect("insert sparse key"));
        assert!(matches!(
            &index.storage,
            IntegerJoinBuildStorage::Ranked {
                domain_indices: None,
                monotonic: true,
                ..
            }
        ));
        let index = index
            .finish(allocator.clone(), &memory)
            .expect("finish ranked index");

        let vector = paro_common::test_utils::test_i32_vector_with_allocator(&[14, 15], allocator);
        let mut pointers = [0; 2];
        let mut matched = [0; 2];
        let count = index
            .lookup_vector_rows(&vector, 2, &[0, 1], &mut pointers, &mut matched)
            .expect("probe ranked index");
        assert_eq!(count, 1);
        assert_eq!(matched[0], 0);
        assert_eq!(pointers[0], 123);
    }

    #[test]
    fn ranked_index_scatters_an_unordered_build_stream() {
        let allocator = paro_common::test_utils::test_allocator();
        let memory = MemoryAccountingContext::detached(
            MemoryTag::HashTable,
            MemoryAccountingClass::NonRevocable,
        );
        let kind = IntegerKeyKind::Integer;
        let min = kind.value_ordinal(&Value::Integer(10)).expect("minimum");
        let max = kind.value_ordinal(&Value::Integer(30)).expect("maximum");
        let mut index =
            ExactIntegerJoinIndexBuilder::try_new(kind, min, max, 2, allocator.clone(), &memory)
                .expect("ranked index allocation")
                .expect("eligible ranked index");
        let high = kind.value_ordinal(&Value::Integer(25)).expect("high key");
        let low = kind.value_ordinal(&Value::Integer(12)).expect("low key");
        assert!(index.insert(high, 250).expect("insert high key"));
        assert!(index.insert(low, 120).expect("insert low key"));
        assert!(matches!(
            &index.storage,
            IntegerJoinBuildStorage::Ranked {
                domain_indices: Some(_),
                monotonic: false,
                ..
            }
        ));
        let index = index
            .finish(allocator.clone(), &memory)
            .expect("finish ranked index");

        let vector =
            paro_common::test_utils::test_i32_vector_with_allocator(&[12, 25, 13], allocator);
        let mut pointers = [0; 3];
        let mut matched = [0; 3];
        let count = index
            .lookup_vector_rows(&vector, 3, &[0, 1, 2], &mut pointers, &mut matched)
            .expect("probe ranked index");
        assert_eq!(count, 2);
        assert_eq!(&matched[..count], &[0, 1]);
        assert_eq!(pointers[0], 120);
        assert_eq!(pointers[1], 250);
    }

    #[test]
    fn direct_index_probes_sequence_vectors_without_materializing() {
        let allocator = paro_common::test_utils::test_allocator();
        let memory = MemoryAccountingContext::detached(
            MemoryTag::HashTable,
            MemoryAccountingClass::NonRevocable,
        );
        let kind = IntegerKeyKind::BigInt;
        let min = kind.value_ordinal(&Value::BigInt(12)).expect("minimum");
        let max = kind.value_ordinal(&Value::BigInt(15)).expect("maximum");
        let mut index =
            ExactIntegerJoinIndexBuilder::try_new(kind, min, max, 2, allocator.clone(), &memory)
                .expect("direct index allocation")
                .expect("eligible direct index");
        assert!(index.insert(min, 120).expect("insert first key"));
        let second = kind.value_ordinal(&Value::BigInt(14)).expect("second key");
        assert!(index.insert(second, 140).expect("insert second key"));
        let index = index
            .finish(allocator.clone(), &memory)
            .expect("finish direct index");

        let vector = paro_common::test_utils::test_sequence_with_allocator(12, 1, 4, allocator);
        let mut pointers = [0; 4];
        let mut matched = [0; 4];
        let count = index
            .lookup_vector_rows(&vector, 4, &[0, 1, 2, 3], &mut pointers, &mut matched)
            .expect("probe sequence vector");
        assert_eq!(count, 2);
        assert_eq!(&matched[..count], &[0, 2]);
        assert_eq!(pointers[0], 120);
        assert_eq!(pointers[2], 140);
    }
}
