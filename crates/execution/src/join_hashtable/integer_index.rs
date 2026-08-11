// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Exact indexes for unique, bounded integer join keys.
//!
//! Compact domains use a direct pointer array. Sparse domains use a bitset
//! plus rank metadata, keeping the probe path exact without paying one pointer
//! per absent key.

use std::sync::atomic::{AtomicUsize, Ordering};
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

const MAX_DIRECT_JOIN_SLOTS: usize = 2_097_152;
const MAX_DIRECT_SLOTS_PER_BUILD_ROW: usize = 24;
const MAX_RANKED_INDEX_PEAK_BYTES: usize = 32 * 1024 * 1024;

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

/// Shared direct-address builder used by parallel hash-join finalization.
///
/// Each domain slot is a zero-initialized atomic chain head. Build blocks can
/// therefore publish disjoint keys without coordination, while duplicate keys
/// are linked by the previous head returned from `swap`.
pub(super) struct ConcurrentDirectIntegerIndexBuilder {
    domain: IntegerIndexDomain,
    pointers: GrantBuffer,
    expected_rows: usize,
    inserted_rows: AtomicUsize,
}

impl ConcurrentDirectIntegerIndexBuilder {
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
        if len > MAX_DIRECT_JOIN_SLOTS
            || len > build_count.saturating_mul(MAX_DIRECT_SLOTS_PER_BUILD_ROW)
        {
            return Ok(None);
        }
        let pointer_bytes = len
            .checked_mul(std::mem::size_of::<usize>())
            .ok_or_else(|| paro_error::internal("direct join index byte size overflow"))?;
        Ok(Some(Self {
            domain: IntegerIndexDomain {
                kind,
                min_ordinal,
                len,
            },
            pointers: memory.allocate_zeroed_buffer(allocator, pointer_bytes)?,
            expected_rows: build_count,
            inserted_rows: AtomicUsize::new(0),
        }))
    }

    pub(super) fn insert(&self, ordinal: u128, row_ptr: usize) -> Result<Option<usize>> {
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
        let inserted = self.inserted_rows.fetch_add(1, Ordering::Relaxed);
        if inserted >= self.expected_rows {
            return Err(paro_error::internal(
                "direct join index received more rows than planned",
            ));
        }
        let target = unsafe { self.pointers.as_ptr().cast::<AtomicUsize>().add(index) };
        let previous = unsafe { &*target }.swap(row_ptr, Ordering::Relaxed);
        Ok((previous != 0).then_some(previous))
    }

    pub(super) fn finish(self) -> Result<ExactIntegerJoinIndex> {
        let inserted = self.inserted_rows.load(Ordering::Acquire);
        if inserted != self.expected_rows {
            return Err(paro_error::internal(format!(
                "direct join index row count mismatch: expected={}, actual={inserted}",
                self.expected_rows
            )));
        }
        Ok(ExactIntegerJoinIndex {
            domain: self.domain,
            storage: IntegerJoinStorage::Direct {
                pointers: self.pointers,
            },
        })
    }
}

/// First phase of a staged compact-domain index build.
///
/// Each build row records its domain offset. Once all rows are recorded, the
/// occupancy bitmap is frozen and
/// [`Self::prepare_scatter`] freezes the bitmap, constructs its rank directory,
/// and allocates the compact chain-head array.
pub(super) struct StagedRankedIntegerIndexBuilder {
    domain: IntegerIndexDomain,
    bits: GrantBuffer,
    row_domain_indices: GrantBuffer,
    expected_rows: usize,
    recorded_rows: usize,
    allocator: Arc<dyn Allocator>,
    memory: MemoryAccountingContext,
}

impl StagedRankedIntegerIndexBuilder {
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

        let word_count = len.div_ceil(u64::BITS as usize);
        let bit_bytes = word_count
            .checked_mul(std::mem::size_of::<u64>())
            .ok_or_else(|| paro_error::internal("ranked join bitset size overflow"))?;
        let rank_bytes = word_count
            .checked_mul(std::mem::size_of::<u32>())
            .ok_or_else(|| paro_error::internal("ranked join rank size overflow"))?;
        let row_index_bytes = build_count
            .checked_mul(std::mem::size_of::<u32>())
            .ok_or_else(|| paro_error::internal("ranked join row-index size overflow"))?;
        let pointer_bytes = build_count
            .checked_mul(std::mem::size_of::<usize>())
            .ok_or_else(|| paro_error::internal("ranked join pointer size overflow"))?;
        let peak_bytes = bit_bytes
            .checked_add(rank_bytes)
            .and_then(|bytes| bytes.checked_add(row_index_bytes))
            .and_then(|bytes| bytes.checked_add(pointer_bytes))
            .ok_or_else(|| paro_error::internal("ranked join index size overflow"))?;
        if peak_bytes > MAX_RANKED_INDEX_PEAK_BYTES {
            return Ok(None);
        }

        Ok(Some(Self {
            domain: IntegerIndexDomain {
                kind,
                min_ordinal,
                len,
            },
            bits: memory.allocate_zeroed_buffer(allocator.clone(), bit_bytes)?,
            row_domain_indices: memory.allocate_buffer(allocator.clone(), row_index_bytes)?,
            expected_rows: build_count,
            recorded_rows: 0,
            allocator,
            memory: memory.clone(),
        }))
    }

    /// Record one row's domain offset.
    ///
    pub(super) fn record_at(&mut self, row_slot: usize, ordinal: u128) -> Result<()> {
        if row_slot != self.recorded_rows {
            return Err(paro_error::internal(
                "ranked join rows must be recorded in build-store order",
            ));
        }
        if row_slot >= self.expected_rows {
            return Err(paro_error::internal(
                "ranked join row slot exceeds declared build count",
            ));
        }
        let Some(domain_index) = self.domain.index_for_ordinal(ordinal) else {
            return Err(paro_error::internal(
                "integer join build key fell outside its measured domain",
            ));
        };
        let domain_index = u32::try_from(domain_index)
            .map_err(|_| paro_error::internal("ranked join domain index exceeds u32"))?;
        unsafe {
            // SAFETY: exclusive builder ownership initializes each slot once.
            std::ptr::write(
                self.row_domain_indices.as_ptr().cast::<u32>().add(row_slot),
                domain_index,
            );
        }
        let word_idx = domain_index as usize / u64::BITS as usize;
        let bit = 1_u64 << (domain_index as usize % u64::BITS as usize);
        unsafe {
            let word = self.bits.as_ptr().cast::<u64>().add(word_idx);
            *word |= bit;
        }
        self.recorded_rows += 1;
        Ok(())
    }

    pub(super) fn prepare_scatter(self) -> Result<StagedRankedIntegerIndexScatter> {
        let recorded = self.recorded_rows;
        if recorded != self.expected_rows {
            return Err(paro_error::internal(format!(
                "ranked join row count mismatch: expected={}, actual={recorded}",
                self.expected_rows
            )));
        }
        let word_count = self.domain.len.div_ceil(u64::BITS as usize);
        let rank_bytes = word_count
            .checked_mul(std::mem::size_of::<u32>())
            .ok_or_else(|| paro_error::internal("ranked join rank size overflow"))?;
        let ranks = self
            .memory
            .allocate_buffer(self.allocator.clone(), rank_bytes)?;
        let words = unsafe { grant_slice::<u64>(&self.bits, word_count) };
        let ranks_ptr = ranks.as_ptr().cast::<u32>();
        let mut distinct_count = 0_u32;
        for (word_idx, word) in words.iter().copied().enumerate() {
            unsafe {
                // SAFETY: every rank slot is initialized exactly once before
                // the scatter builder is shared with workers.
                std::ptr::write(ranks_ptr.add(word_idx), distinct_count);
            }
            distinct_count = distinct_count
                .checked_add(word.count_ones())
                .ok_or_else(|| paro_error::internal("ranked join cardinality overflow"))?;
        }
        let distinct_count = distinct_count as usize;
        let pointer_bytes = distinct_count
            .checked_mul(std::mem::size_of::<usize>())
            .ok_or_else(|| paro_error::internal("ranked join pointer size overflow"))?;
        let pointers = self
            .memory
            .allocate_zeroed_buffer(self.allocator, pointer_bytes)?;
        Ok(StagedRankedIntegerIndexScatter {
            domain: self.domain,
            bits: self.bits,
            ranks,
            pointers,
            distinct_count,
            row_domain_indices: self.row_domain_indices,
            expected_rows: self.expected_rows,
            scattered_rows: 0,
        })
    }
}

/// Second phase of a staged ranked-index build.
///
/// Rank maps each recorded domain offset to one compact chain head without a
/// temporary hash directory.
pub(super) struct StagedRankedIntegerIndexScatter {
    domain: IntegerIndexDomain,
    bits: GrantBuffer,
    ranks: GrantBuffer,
    pointers: GrantBuffer,
    distinct_count: usize,
    row_domain_indices: GrantBuffer,
    expected_rows: usize,
    scattered_rows: usize,
}

impl StagedRankedIntegerIndexScatter {
    /// Publish one build-row pointer and return the preceding chain head.
    ///
    pub(super) fn insert_at(&mut self, row_slot: usize, row_ptr: usize) -> Result<Option<usize>> {
        if row_slot != self.scattered_rows {
            return Err(paro_error::internal(
                "ranked join rows must be scattered in build-store order",
            ));
        }
        if row_slot >= self.expected_rows {
            return Err(paro_error::internal(
                "ranked join scatter received an invalid build row",
            ));
        }
        if row_ptr == 0 {
            return Err(paro_error::internal(
                "ranked join index cannot store a null build-row pointer",
            ));
        }
        let domain_index = unsafe {
            // SAFETY: guaranteed by the method contract.
            std::ptr::read(self.row_domain_indices.as_ptr().cast::<u32>().add(row_slot)) as usize
        };
        let words =
            unsafe { grant_slice::<u64>(&self.bits, self.domain.len.div_ceil(u64::BITS as usize)) };
        let ranks = unsafe { grant_slice::<u32>(&self.ranks, words.len()) };
        let pointer_slot = ranked_slot(words, ranks, domain_index);
        if pointer_slot >= self.distinct_count {
            return Err(paro_error::internal(
                "ranked join scatter produced an invalid pointer slot",
            ));
        }
        let pointer_slot_u32 = u32::try_from(pointer_slot)
            .map_err(|_| paro_error::internal("ranked join pointer slot exceeds u32"))?;
        unsafe {
            // SAFETY: the domain offset for this build row is dead after its
            // rank is known. Reusing the same initialized slot keeps a stable
            // build-row-to-group mapping without another allocation.
            std::ptr::write(
                self.row_domain_indices.as_ptr().cast::<u32>().add(row_slot),
                pointer_slot_u32,
            );
        }
        let target = unsafe { self.pointers.as_ptr().cast::<usize>().add(pointer_slot) };
        let previous = unsafe { std::ptr::replace(target, row_ptr) };
        self.scattered_rows += 1;
        Ok((previous != 0).then_some(previous))
    }

    pub(super) fn finish(self) -> Result<ExactIntegerJoinIndex> {
        let scattered = self.scattered_rows;
        if scattered != self.expected_rows {
            return Err(paro_error::internal(format!(
                "ranked join scatter count mismatch: expected={}, actual={scattered}",
                self.expected_rows
            )));
        }
        Ok(ExactIntegerJoinIndex {
            domain: self.domain,
            storage: IntegerJoinStorage::Ranked {
                bits: self.bits,
                ranks: self.ranks,
                pointers: self.pointers,
                count: self.distinct_count,
                build_group_slots: self.row_domain_indices,
                build_row_count: self.expected_rows,
            },
        })
    }
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
        pointers: GrantBuffer,
    },
    Ranked {
        bits: GrantBuffer,
        ranks: GrantBuffer,
        pointers: GrantBuffer,
        count: usize,
        build_group_slots: GrantBuffer,
        build_row_count: usize,
    },
}

impl ExactIntegerJoinIndex {
    pub(super) fn size_in_bytes(&self) -> usize {
        match &self.storage {
            IntegerJoinStorage::Direct { pointers } => pointers.size(),
            IntegerJoinStorage::Ranked {
                bits,
                ranks,
                pointers,
                build_group_slots,
                ..
            } => bits.size() + ranks.size() + pointers.size() + build_group_slots.size(),
        }
    }

    pub(super) fn ranked_group_count(&self) -> Option<usize> {
        match &self.storage {
            IntegerJoinStorage::Ranked { count, .. } => Some(*count),
            IntegerJoinStorage::Direct { .. } => None,
        }
    }

    /// Resolve every non-NULL `BIGINT` probe row to its compact ranked slot.
    /// Returns `false` when this index representation cannot provide compact
    /// group slots, allowing callers to retain their generic hash-chain path.
    pub(super) fn lookup_i64_group_slots(
        &self,
        vector: &Vector,
        vector_count: usize,
        output_slots: &mut [usize],
    ) -> Result<bool> {
        if self.domain.kind != IntegerKeyKind::BigInt || output_slots.len() < vector_count {
            return Ok(false);
        }
        let IntegerJoinStorage::Ranked { bits, ranks, .. } = &self.storage else {
            return Ok(false);
        };
        let words =
            unsafe { grant_slice::<u64>(bits, self.domain.len.div_ceil(u64::BITS as usize)) };
        let rank_values = unsafe { grant_slice::<u32>(ranks, words.len()) };
        output_slots[..vector_count].fill(usize::MAX);
        let view = vector.try_to_view(vector_count)?;
        if let Some(data) = view.get_data::<i64>() {
            for (row_idx, output) in output_slots[..vector_count].iter_mut().enumerate() {
                if !view.is_valid(row_idx) {
                    continue;
                }
                let value = unsafe { *data.add(view.physical_index(row_idx)) };
                *output = self
                    .ranked_group_slot_for_ordinal_in(
                        words,
                        rank_values,
                        signed_ordinal(value as i128),
                    )
                    .unwrap_or(usize::MAX);
            }
        } else {
            for (row_idx, output) in output_slots[..vector_count].iter_mut().enumerate() {
                if !view.is_valid(row_idx) {
                    continue;
                }
                *output = self
                    .ranked_group_slot_for_ordinal_in(
                        words,
                        rank_values,
                        signed_ordinal(view.get_i64(row_idx) as i128),
                    )
                    .unwrap_or(usize::MAX);
            }
        }
        Ok(true)
    }

    /// Return the compact group slot recorded for one row in build-store order.
    ///
    /// Staged ranked construction visits the immutable build store in this
    /// exact order, so the mapping is valid for the index's complete lifetime.
    #[inline]
    pub(super) fn ranked_group_slot_for_build_row(&self, build_row: usize) -> Option<usize> {
        let IntegerJoinStorage::Ranked {
            build_group_slots,
            build_row_count,
            ..
        } = &self.storage
        else {
            return None;
        };
        if build_row >= *build_row_count {
            return None;
        }
        Some(unsafe {
            // SAFETY: every build-row slot is initialized during scatter and
            // `build_row` is bounded by the retained stream's element count.
            std::ptr::read(build_group_slots.as_ptr().cast::<u32>().add(build_row)) as usize
        })
    }

    #[inline]
    fn ranked_group_slot_for_ordinal_in(
        &self,
        words: &[u64],
        ranks: &[u32],
        ordinal: u128,
    ) -> Option<usize> {
        let index = self.domain.index_for_ordinal(ordinal)?;
        let word = words[index / u64::BITS as usize];
        let mask = 1_u64 << (index % u64::BITS as usize);
        (word & mask != 0).then(|| ranked_slot(words, ranks, index))
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
            IntegerJoinStorage::Direct { pointers } => {
                self.lookup_vector_rows_with(
                    &view,
                    probe_rows,
                    output_pointers,
                    matched_rows,
                    |ordinal| {
                        let index = self.domain.index_for_ordinal(ordinal)?;
                        let pointer = unsafe {
                            // SAFETY: the zero-initialized pointer domain is
                            // immutable after the finalized index is published.
                            std::ptr::read(pointers.as_ptr().cast::<usize>().add(index))
                        };
                        (pointer != 0).then_some(pointer)
                    },
                )
            }
            IntegerJoinStorage::Ranked {
                bits,
                ranks,
                pointers,
                count,
                ..
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

#[inline]
fn signed_ordinal(value: i128) -> u128 {
    (value as u128) ^ SIGNED_ORDINAL_MASK
}

#[cfg(test)]
mod tests {
    use paro_common::allocator::MemoryTag;
    use paro_common::memory::{MemoryAccountingClass, MemoryAccountingContext};
    use paro_common::runtime_value::Value;

    use super::{
        ConcurrentDirectIntegerIndexBuilder, IntegerKeyKind, StagedRankedIntegerIndexBuilder,
    };

    fn memory() -> MemoryAccountingContext {
        MemoryAccountingContext::detached(MemoryTag::HashTable, MemoryAccountingClass::NonRevocable)
    }

    fn ordinal(kind: IntegerKeyKind, value: i32) -> u128 {
        kind.value_ordinal(&Value::Integer(value))
            .expect("integer ordinal")
    }

    #[test]
    fn signed_ordinals_preserve_order_across_zero() {
        let kind = IntegerKeyKind::Integer;
        let negative = ordinal(kind, -1);
        let zero = ordinal(kind, 0);
        let positive = ordinal(kind, 1);
        assert_eq!(zero - negative, 1);
        assert_eq!(positive - zero, 1);
        let minimum = ordinal(kind, i32::MIN);
        let maximum = ordinal(kind, i32::MAX);
        assert_eq!(
            maximum
                .checked_sub(minimum)
                .and_then(|offset| usize::try_from(offset).ok()),
            Some(u32::MAX as usize)
        );
    }

    #[test]
    fn direct_index_returns_previous_duplicate_head() {
        let allocator = paro_common::test_utils::test_allocator();
        let kind = IntegerKeyKind::Integer;
        let builder = ConcurrentDirectIntegerIndexBuilder::try_new(
            kind,
            ordinal(kind, 10),
            ordinal(kind, 20),
            2,
            allocator,
            &memory(),
        )
        .expect("direct index allocation")
        .expect("eligible direct index");
        let key = ordinal(kind, 12);
        assert_eq!(builder.insert(key, 100).unwrap(), None);
        assert_eq!(builder.insert(key, 200).unwrap(), Some(100));
        builder.finish().expect("complete direct index");
    }

    #[test]
    fn direct_index_rejects_incomplete_publication() {
        let allocator = paro_common::test_utils::test_allocator();
        let kind = IntegerKeyKind::Integer;
        let builder = ConcurrentDirectIntegerIndexBuilder::try_new(
            kind,
            ordinal(kind, 10),
            ordinal(kind, 13),
            2,
            allocator,
            &memory(),
        )
        .unwrap()
        .unwrap();
        builder.insert(ordinal(kind, 10), 100).unwrap();
        let error = builder
            .finish()
            .expect_err("incomplete direct index must not publish");
        assert!(error.to_string().contains("row count mismatch"));
    }

    #[test]
    fn ranked_index_maps_sparse_unordered_keys_and_build_rows() {
        let allocator = paro_common::test_utils::test_allocator();
        let kind = IntegerKeyKind::Integer;
        let minimum = ordinal(kind, 10);
        let maximum = ordinal(kind, 89);
        let keys = [ordinal(kind, 45), ordinal(kind, 12), ordinal(kind, 45)];
        let mut builder = StagedRankedIntegerIndexBuilder::try_new(
            kind,
            minimum,
            maximum,
            keys.len(),
            allocator.clone(),
            &memory(),
        )
        .unwrap()
        .unwrap();
        for (build_row, key) in keys.into_iter().enumerate() {
            builder.record_at(build_row, key).unwrap();
        }
        let mut scatter = builder.prepare_scatter().unwrap();
        assert_eq!(scatter.insert_at(0, 450).unwrap(), None);
        assert_eq!(scatter.insert_at(1, 120).unwrap(), None);
        assert_eq!(scatter.insert_at(2, 451).unwrap(), Some(450));
        let index = scatter.finish().unwrap();

        let vector =
            paro_common::test_utils::test_i32_vector_with_allocator(&[12, 45, 13], allocator);
        let mut pointers = [0; 3];
        let mut matched = [0; 3];
        let count = index
            .lookup_vector_rows(&vector, 3, &[0, 1, 2], &mut pointers, &mut matched)
            .unwrap();
        assert_eq!(count, 2);
        assert_eq!(&matched[..count], &[0, 1]);
        assert_eq!(pointers[0], 120);
        assert_eq!(pointers[1], 451);
        assert_eq!(index.ranked_group_slot_for_build_row(0), Some(1));
        assert_eq!(index.ranked_group_slot_for_build_row(1), Some(0));
        assert_eq!(index.ranked_group_slot_for_build_row(2), Some(1));
        assert_eq!(index.ranked_group_slot_for_build_row(3), None);
    }

    #[test]
    fn ranked_index_requires_build_store_order_in_both_phases() {
        let allocator = paro_common::test_utils::test_allocator();
        let kind = IntegerKeyKind::Integer;
        let mut builder = StagedRankedIntegerIndexBuilder::try_new(
            kind,
            ordinal(kind, 10),
            ordinal(kind, 40),
            2,
            allocator,
            &memory(),
        )
        .unwrap()
        .unwrap();
        assert!(builder.record_at(1, ordinal(kind, 20)).is_err());
        builder.record_at(0, ordinal(kind, 20)).unwrap();
        builder.record_at(1, ordinal(kind, 30)).unwrap();
        let mut scatter = builder.prepare_scatter().unwrap();
        assert!(scatter.insert_at(1, 200).is_err());
        scatter.insert_at(0, 200).unwrap();
        scatter.insert_at(1, 300).unwrap();
        scatter.finish().unwrap();
    }

    #[test]
    fn direct_index_probes_sequence_vectors_without_materializing() {
        let allocator = paro_common::test_utils::test_allocator();
        let kind = IntegerKeyKind::BigInt;
        let minimum = kind.value_ordinal(&Value::BigInt(12)).expect("minimum");
        let maximum = kind.value_ordinal(&Value::BigInt(15)).expect("maximum");
        let builder = ConcurrentDirectIntegerIndexBuilder::try_new(
            kind,
            minimum,
            maximum,
            2,
            allocator.clone(),
            &memory(),
        )
        .unwrap()
        .unwrap();
        builder.insert(minimum, 120).unwrap();
        builder.insert(minimum + 2, 140).unwrap();
        let index = builder.finish().unwrap();

        let vector = paro_common::test_utils::test_sequence_with_allocator(12, 1, 4, allocator);
        let mut pointers = [0; 4];
        let mut matched = [0; 4];
        let count = index
            .lookup_vector_rows(&vector, 4, &[0, 1, 2, 3], &mut pointers, &mut matched)
            .unwrap();
        assert_eq!(count, 2);
        assert_eq!(&matched[..count], &[0, 2]);
        assert_eq!(pointers[0], 120);
        assert_eq!(pointers[2], 140);
    }
}
