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
}

/// Immutable after publication by [`super::table::JoinHashTable::finalize`].
#[derive(Debug)]
pub(super) struct ExactIntegerJoinIndex {
    kind: IntegerKeyKind,
    min_ordinal: u128,
    len: usize,
    storage: IntegerJoinStorage,
}

#[derive(Debug)]
enum IntegerJoinStorage {
    Empty,
    Direct {
        pointers: GrantBuffer,
    },
    RankedBuilding {
        bits: GrantBuffer,
        offsets: GrantBuffer,
        input_pointers: GrantBuffer,
        count: usize,
        inserted: usize,
    },
    Ranked {
        bits: GrantBuffer,
        ranks: GrantBuffer,
        pointers: GrantBuffer,
        count: usize,
    },
}

impl ExactIntegerJoinIndex {
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
        let storage = if len <= MAX_DIRECT_JOIN_SLOTS
            && len <= build_count.saturating_mul(MAX_DIRECT_SLOTS_PER_BUILD_ROW)
        {
            let bytes = len
                .checked_mul(std::mem::size_of::<usize>())
                .ok_or_else(|| paro_error::internal("direct join index byte size overflow"))?;
            IntegerJoinStorage::Direct {
                pointers: memory.allocate_zeroed_buffer(allocator, bytes)?,
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
            let final_bytes = bit_bytes
                .checked_add(rank_bytes)
                .and_then(|bytes| bytes.checked_add(pointer_bytes))
                .ok_or_else(|| paro_error::internal("ranked join index size overflow"))?;
            if final_bytes > MAX_RANKED_INDEX_BYTES {
                return Ok(None);
            }
            let offset_bytes = build_count
                .checked_mul(std::mem::size_of::<u32>())
                .ok_or_else(|| paro_error::internal("ranked join offset size overflow"))?;
            IntegerJoinStorage::RankedBuilding {
                bits: memory.allocate_zeroed_buffer(allocator.clone(), bit_bytes)?,
                offsets: memory.allocate_buffer(allocator.clone(), offset_bytes)?,
                input_pointers: memory.allocate_buffer(allocator, pointer_bytes)?,
                count: build_count,
                inserted: 0,
            }
        };
        Ok(Some(Self {
            kind,
            min_ordinal,
            len,
            storage,
        }))
    }

    pub(super) fn size_in_bytes(&self) -> usize {
        match &self.storage {
            IntegerJoinStorage::Empty => 0,
            IntegerJoinStorage::Direct { pointers } => pointers.size(),
            IntegerJoinStorage::RankedBuilding {
                bits,
                offsets,
                input_pointers,
                ..
            } => bits.size() + offsets.size() + input_pointers.size(),
            IntegerJoinStorage::Ranked {
                bits,
                ranks,
                pointers,
                ..
            } => bits.size() + ranks.size() + pointers.size(),
        }
    }

    /// Insert a unique build key. Returns `false` when the slot was occupied.
    pub(super) fn insert(&mut self, ordinal: u128, row_ptr: usize) -> Result<bool> {
        if row_ptr == 0 {
            return Err(paro_error::internal(
                "integer join index cannot store a null build-row pointer",
            ));
        }
        let Some(index) = self.index_for_ordinal(ordinal) else {
            return Err(paro_error::internal(
                "integer join build key fell outside its measured domain",
            ));
        };
        self.insert_index(index, row_ptr)
    }

    fn insert_index(&mut self, index: usize, row_ptr: usize) -> Result<bool> {
        debug_assert!(index < self.len);
        match &mut self.storage {
            IntegerJoinStorage::Direct { pointers } => {
                let slots = unsafe { grant_slice_mut::<usize>(pointers, self.len) };
                let slot = &mut slots[index];
                if *slot != 0 {
                    return Ok(false);
                }
                *slot = row_ptr;
            }
            IntegerJoinStorage::RankedBuilding {
                bits,
                offsets,
                input_pointers,
                count,
                inserted,
            } => {
                let words =
                    unsafe { grant_slice_mut::<u64>(bits, self.len.div_ceil(u64::BITS as usize)) };
                let word = &mut words[index / u64::BITS as usize];
                let mask = 1_u64 << (index % u64::BITS as usize);
                if *word & mask != 0 {
                    return Ok(false);
                }
                if *inserted >= *count {
                    return Err(paro_error::internal(
                        "ranked join index received more rows than planned",
                    ));
                }
                *word |= mask;
                let offset = u32::try_from(index).map_err(|_| {
                    paro_error::internal("ranked join key offset exceeds its physical format")
                })?;
                let offset_values = unsafe { grant_slice_mut::<u32>(offsets, *count) };
                offset_values[*inserted] = offset;
                let pointer_values = unsafe { grant_slice_mut::<usize>(input_pointers, *count) };
                pointer_values[*inserted] = row_ptr;
                *inserted += 1;
            }
            IntegerJoinStorage::Ranked { .. } => {
                return Err(paro_error::internal(
                    "cannot insert into a finalized ranked join index",
                ));
            }
            IntegerJoinStorage::Empty => {
                return Err(paro_error::internal(
                    "cannot insert into an empty join index",
                ));
            }
        }
        Ok(true)
    }

    pub(super) fn finish(
        &mut self,
        allocator: Arc<dyn Allocator>,
        memory: &MemoryAccountingContext,
    ) -> Result<()> {
        let storage = std::mem::replace(&mut self.storage, IntegerJoinStorage::Empty);
        let (bits, offsets, input_pointers, count, inserted) = match storage {
            IntegerJoinStorage::RankedBuilding {
                bits,
                offsets,
                input_pointers,
                count,
                inserted,
            } => (bits, offsets, input_pointers, count, inserted),
            other => {
                self.storage = other;
                return Ok(());
            }
        };
        if inserted != count {
            return Err(paro_error::internal(format!(
                "ranked join index row count mismatch: expected={count}, actual={inserted}",
            )));
        }

        let word_count = self.len.div_ceil(u64::BITS as usize);
        let rank_bytes = word_count
            .checked_mul(std::mem::size_of::<u32>())
            .ok_or_else(|| paro_error::internal("ranked join rank size overflow"))?;
        let pointer_bytes = count
            .checked_mul(std::mem::size_of::<usize>())
            .ok_or_else(|| paro_error::internal("ranked join pointer size overflow"))?;
        let mut ranks = memory.allocate_buffer(allocator.clone(), rank_bytes)?;
        let mut pointers = memory.allocate_zeroed_buffer(allocator, pointer_bytes)?;
        let words = unsafe { grant_slice::<u64>(&bits, word_count) };
        let rank_values = unsafe { grant_slice_mut::<u32>(&mut ranks, word_count) };
        let mut running = 0_u32;
        for (word_idx, word) in words.iter().copied().enumerate() {
            rank_values[word_idx] = running;
            running = running
                .checked_add(word.count_ones())
                .ok_or_else(|| paro_error::internal("ranked join cardinality overflow"))?;
        }
        if running as usize != count {
            return Err(paro_error::internal(
                "ranked join bitset cardinality does not match build rows",
            ));
        }

        let source_offsets = unsafe { grant_slice::<u32>(&offsets, count) };
        let source_pointers = unsafe { grant_slice::<usize>(&input_pointers, count) };
        let ranked_pointers = unsafe { grant_slice_mut::<usize>(&mut pointers, count) };
        for (&offset, &row_ptr) in source_offsets.iter().zip(source_pointers) {
            let slot = ranked_slot(words, rank_values, offset as usize);
            if ranked_pointers[slot] != 0 {
                return Err(paro_error::internal(
                    "ranked join index produced a duplicate rank",
                ));
            }
            ranked_pointers[slot] = row_ptr;
        }

        self.storage = IntegerJoinStorage::Ranked {
            bits,
            ranks,
            pointers,
            count,
        };
        Ok(())
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
                let pointer_values = unsafe { grant_slice::<usize>(pointers, self.len) };
                self.lookup_vector_rows_with(
                    &view,
                    probe_rows,
                    output_pointers,
                    matched_rows,
                    |ordinal| {
                        let index = self.index_for_ordinal(ordinal)?;
                        let pointer = pointer_values[index];
                        (pointer != 0).then_some(pointer)
                    },
                )
            }
            IntegerJoinStorage::Ranked {
                bits,
                ranks,
                pointers,
                count,
            } => {
                let words =
                    unsafe { grant_slice::<u64>(bits, self.len.div_ceil(u64::BITS as usize)) };
                let rank_values = unsafe { grant_slice::<u32>(ranks, words.len()) };
                let pointer_values = unsafe { grant_slice::<usize>(pointers, *count) };
                self.lookup_vector_rows_with(
                    &view,
                    probe_rows,
                    output_pointers,
                    matched_rows,
                    |ordinal| {
                        let index = self.index_for_ordinal(ordinal)?;
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
            IntegerJoinStorage::RankedBuilding { .. } | IntegerJoinStorage::Empty => Err(
                paro_error::internal("integer join index was published before finalization"),
            ),
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

        match self.kind {
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
        if !matches!(
            self.kind,
            IntegerKeyKind::BigInt
                | IntegerKeyKind::Timestamp
                | IntegerKeyKind::TimestampTz
                | IntegerKeyKind::Time
        ) {
            return Err(paro_error::internal(
                "integer join vector has no fixed physical data",
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

    #[inline]
    fn index_for_ordinal(&self, ordinal: u128) -> Option<usize> {
        ordinal
            .checked_sub(self.min_ordinal)
            .and_then(|offset| usize::try_from(offset).ok())
            .filter(|index| *index < self.len)
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

    use super::{ExactIntegerJoinIndex, IntegerKeyKind};

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
        let mut index = ExactIntegerJoinIndex::try_new(
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
            ExactIntegerJoinIndex::try_new(kind, min, max, 1, allocator.clone(), &memory)
                .expect("ranked index allocation")
                .expect("eligible ranked index");
        let ordinal = kind
            .value_ordinal(&Value::Integer(14))
            .expect("key ordinal");
        assert!(index.insert(ordinal, 123).expect("insert sparse key"));
        index
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
            ExactIntegerJoinIndex::try_new(kind, min, max, 2, allocator.clone(), &memory)
                .expect("direct index allocation")
                .expect("eligible direct index");
        assert!(index.insert(min, 120).expect("insert first key"));
        let second = kind.value_ordinal(&Value::BigInt(14)).expect("second key");
        assert!(index.insert(second, 140).expect("insert second key"));

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
