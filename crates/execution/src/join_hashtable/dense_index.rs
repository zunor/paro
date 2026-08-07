// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Direct-address index for unique, bounded integer join keys.

use std::sync::Arc;

use paro_common::allocator::Allocator;
use paro_common::error::{self as paro_error, Result};
use paro_common::memory::{GrantBuffer, MemoryAccountingContext};
#[cfg(test)]
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_common::vector::Vector;
use paro_storage::row::codec::unsafe_api;
use paro_storage::row::RowLayout;

pub(super) const MAX_DENSE_JOIN_SLOTS: usize = 1_048_576;
pub(super) const MAX_SLOTS_PER_BUILD_ROW: usize = 8;

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

    pub(super) fn vector_ordinal(self, vector: &Vector, row_idx: usize) -> Option<u128> {
        match self {
            Self::TinyInt => vector
                .get_i8(row_idx)
                .map(|value| signed_ordinal(value as i128)),
            Self::SmallInt => vector
                .get_i16(row_idx)
                .map(|value| signed_ordinal(value as i128)),
            Self::Integer | Self::Date => vector
                .get_i32(row_idx)
                .map(|value| signed_ordinal(value as i128)),
            Self::BigInt | Self::Timestamp | Self::TimestampTz | Self::Time => vector
                .get_i64(row_idx)
                .map(|value| signed_ordinal(value as i128)),
            Self::HugeInt => vector.get_i128(row_idx).map(signed_ordinal),
            Self::UTinyInt => vector.get_u8(row_idx).map(u128::from),
            Self::USmallInt => vector.get_u16(row_idx).map(u128::from),
            Self::UInteger => vector.get_u32(row_idx).map(u128::from),
            Self::UBigInt => vector.get_u64(row_idx).map(u128::from),
            Self::UHugeInt => vector.get_u128(row_idx),
        }
    }

    /// Read a key directly from its serialized build row.
    ///
    /// Dense-index finalization visits every build row at least once. Keeping
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
pub(super) struct DenseJoinIndex {
    kind: IntegerKeyKind,
    min_ordinal: u128,
    pointers: GrantBuffer,
    len: usize,
}

impl DenseJoinIndex {
    pub(super) fn try_new(
        kind: IntegerKeyKind,
        min_ordinal: u128,
        max_ordinal: u128,
        allocator: Arc<dyn Allocator>,
        memory: &MemoryAccountingContext,
    ) -> Result<Self> {
        let range = max_ordinal.checked_sub(min_ordinal).ok_or_else(|| {
            paro_error::internal("dense join key range is not monotonically ordered")
        })?;
        let len = usize::try_from(range)
            .ok()
            .and_then(|range| range.checked_add(1))
            .ok_or_else(|| paro_error::internal("dense join key range exceeds address space"))?;
        let bytes = len
            .checked_mul(std::mem::size_of::<usize>())
            .ok_or_else(|| paro_error::internal("dense join index byte size overflow"))?;
        let pointers = memory.allocate_zeroed_buffer(allocator, bytes)?;
        Ok(Self {
            kind,
            min_ordinal,
            pointers,
            len,
        })
    }

    pub(super) fn size_in_bytes(&self) -> usize {
        self.pointers.size()
    }

    /// Insert a unique build key. Returns `false` when the slot was occupied.
    pub(super) fn insert(&mut self, ordinal: u128, row_ptr: usize) -> Result<bool> {
        if row_ptr == 0 {
            return Err(paro_error::internal(
                "dense join index cannot store a null build-row pointer",
            ));
        }
        let Some(index) = self.index_for_ordinal(ordinal) else {
            return Err(paro_error::internal(
                "dense join build key fell outside its measured range",
            ));
        };
        let slot = &mut self.pointers_mut()[index];
        if *slot != 0 {
            return Ok(false);
        }
        *slot = row_ptr;
        Ok(true)
    }

    #[inline]
    pub(super) fn lookup_vector_row(&self, vector: &Vector, row_idx: usize) -> Option<usize> {
        let ordinal = self.kind.vector_ordinal(vector, row_idx)?;
        let index = self.index_for_ordinal(ordinal)?;
        let pointer = self.pointers()[index];
        (pointer != 0).then_some(pointer)
    }

    fn index_for_ordinal(&self, ordinal: u128) -> Option<usize> {
        let offset = ordinal.checked_sub(self.min_ordinal)?;
        let index = usize::try_from(offset).ok()?;
        (index < self.len).then_some(index)
    }

    fn pointers(&self) -> &[usize] {
        unsafe { std::slice::from_raw_parts(self.pointers.as_ptr() as *const usize, self.len) }
    }

    fn pointers_mut(&mut self) -> &mut [usize] {
        unsafe { std::slice::from_raw_parts_mut(self.pointers.as_ptr() as *mut usize, self.len) }
    }
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

    use super::{DenseJoinIndex, IntegerKeyKind};

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
    }

    #[test]
    fn dense_index_rejects_duplicate_build_keys() {
        let allocator = paro_common::test_utils::test_allocator();
        let memory = MemoryAccountingContext::detached(
            MemoryTag::HashTable,
            MemoryAccountingClass::NonRevocable,
        );
        let mut index = DenseJoinIndex::try_new(
            IntegerKeyKind::Integer,
            IntegerKeyKind::Integer
                .value_ordinal(&Value::Integer(10))
                .expect("min"),
            IntegerKeyKind::Integer
                .value_ordinal(&Value::Integer(20))
                .expect("max"),
            allocator,
            &memory,
        )
        .expect("dense index");
        let ordinal = IntegerKeyKind::Integer
            .value_ordinal(&Value::Integer(12))
            .expect("ordinal");
        assert!(index.insert(ordinal, 1).expect("first insert"));
        assert!(!index.insert(ordinal, 2).expect("duplicate insert"));
    }
}
