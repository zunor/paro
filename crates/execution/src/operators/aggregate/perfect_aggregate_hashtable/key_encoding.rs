// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Batch-local group-key preparation and canonical mixed-radix slot encoding.

use paro_common::allocator::MemoryTag;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::memory::{AccountedVec, MemoryAccountingClass, MemoryAccountingContext};
use paro_common::types::LogicalType;
use paro_common::vector::{DataRef, DecodedVectorRef, SelectionRef};
use smallvec::SmallVec;

use crate::operators::aggregate::perfect_hash_key::PerfectHashKeyDomain;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct PerfectHashSlotLayout {
    pub(super) cardinalities: Vec<usize>,
    pub(super) strides: Vec<usize>,
    pub(super) slot_count: usize,
}

impl PerfectHashSlotLayout {
    pub(super) fn try_new(cardinalities: Vec<usize>) -> Result<Self> {
        let mut strides = vec![0; cardinalities.len()];
        let mut slot_count = 1usize;
        for group_idx in (0..cardinalities.len()).rev() {
            let cardinality = cardinalities[group_idx];
            if cardinality < 2 {
                return Err(paro_error::internal(format!(
                    "Invalid perfect aggregate key cardinality: group_idx={group_idx}, cardinality={cardinality}"
                )));
            }
            strides[group_idx] = slot_count;
            slot_count = slot_count.checked_mul(cardinality).ok_or_else(|| {
                paro_error::internal(format!(
                    "Perfect aggregate slot count overflow: slots={slot_count}, cardinality={cardinality}"
                ))
            })?;
        }
        Ok(Self {
            cardinalities,
            strides,
            slot_count,
        })
    }

    #[cfg(test)]
    pub(super) fn add_component(
        &self,
        slot: usize,
        group_idx: usize,
        encoded: usize,
    ) -> Result<usize> {
        let cardinality = self.cardinalities[group_idx];
        if encoded >= cardinality {
            return Err(paro_error::internal(format!(
                "Perfect aggregate encoded key exceeds domain: encoded={encoded}, cardinality={cardinality}, group_idx={group_idx}"
            )));
        }
        slot.checked_add(
            encoded
                .checked_mul(self.strides[group_idx])
                .ok_or_else(|| {
                    paro_error::internal(format!(
                        "Perfect aggregate slot component overflow: encoded={encoded}, stride={}, group_idx={group_idx}",
                        self.strides[group_idx]
                    ))
                })?,
        )
        .ok_or_else(|| {
            paro_error::internal(format!(
                "Perfect aggregate slot overflow: slot={slot}, group_idx={group_idx}"
            ))
        })
    }

    pub(super) fn decode_component(&self, slot: usize, group_idx: usize) -> usize {
        (slot / self.strides[group_idx]) % self.cardinalities[group_idx]
    }

    /// Encode the common two-key shape through the canonical mixed-radix
    /// layout. Keeping this arithmetic beside `decode_component` prevents hot
    /// paths from silently choosing a different component order.
    #[inline(always)]
    fn encode_pair(&self, first: usize, second: usize) -> usize {
        debug_assert_eq!(self.cardinalities.len(), 2);
        debug_assert!(first < self.cardinalities[0]);
        debug_assert!(second < self.cardinalities[1]);
        // `try_new` proved the complete mixed-radix domain fits in usize.
        first * self.strides[0] + second * self.strides[1]
    }
}

/// Batch-local codec for a repeated VARCHAR perfect-hash key.
///
/// Codes are compiled only for physical dictionary entries referenced by the
/// logical batch. Dictionary vectors intentionally retain unused child rows;
/// validating those rows would make an unobserved value affect query
/// semantics. The codec is key-column generic and composes through the normal
/// mixed-radix slot layout, so one, two, and wider key tuples share one path.
/// Flat inputs decline when their physical domain is not smaller than the
/// logical batch; the ordinary row-wise encoder is already optimal there.
pub(super) struct PreparedDictionaryKey<'a> {
    domain: &'a PerfectHashKeyDomain,
    group: &'a DecodedVectorRef<'a>,
    selection: &'a SelectionRef<'a>,
    selection_data: *const u32,
    physical_count: usize,
    minimum: i128,
    cardinality: usize,
    group_idx: usize,
    codes: AccountedVec<usize>,
}

impl<'a> PreparedDictionaryKey<'a> {
    const UNPREPARED: usize = usize::MAX;

    pub(super) fn try_new(
        domain: &'a PerfectHashKeyDomain,
        group: &'a DecodedVectorRef<'a>,
        rows: usize,
        minimum: i128,
        cardinality: usize,
        group_idx: usize,
        memory: &MemoryAccountingContext,
    ) -> Result<Option<Self>> {
        if domain.logical_type() != &LogicalType::Varchar || !group.validity().all_valid() {
            return Ok(None);
        }
        let physical_count = group.physical_count();
        if physical_count >= rows {
            return Ok(None);
        }
        let mut codes = AccountedVec::new_with_accounting(
            memory.reserve_grant(0)?,
            MemoryTag::HashTable,
            MemoryAccountingClass::Metadata,
        );
        codes.try_resize_with(physical_count, || Self::UNPREPARED)?;
        Ok(Some(Self {
            domain,
            group,
            selection: group.sel(),
            selection_data: group
                .sel()
                .materialized_indices()
                .map_or(std::ptr::null(), <[u32]>::as_ptr),
            physical_count,
            minimum,
            cardinality,
            group_idx,
            codes,
        }))
    }

    #[inline(always)]
    pub(super) fn encoded(&mut self, row: usize) -> Result<usize> {
        let physical_idx = if self.selection_data.is_null() {
            self.selection.get(row)
        } else {
            // SAFETY: materialized selection indices cover the logical batch.
            unsafe { *self.selection_data.add(row) as usize }
        };
        // Dictionary construction validates the complete logical-to-physical
        // mapping once. Rechecking it for every grouping consumer would turn a
        // vector invariant into a row-loop tax.
        debug_assert!(physical_idx < self.physical_count);
        // SAFETY: the decoded selection proves the physical index is in range.
        let code = unsafe { self.codes.get_unchecked_mut(physical_idx) };
        if *code == Self::UNPREPARED {
            let value = self.domain.encode_decoded(self.group, physical_idx)?;
            *code = adjust_group_value(value, self.minimum, self.cardinality, self.group_idx)?;
        }
        Ok(*code)
    }
}

#[derive(Clone, Copy)]
enum PreparedIntegerData {
    I8(*const i8),
    I16(*const i16),
    I32(*const i32),
    I64(*const i64),
    I128(*const i128),
    U8(*const u8),
    U16(*const u16),
    U32(*const u32),
    U64(*const u64),
    U128(*const u128),
    SequenceI64 { start: i64, increment: i64 },
}

pub(super) struct PreparedIntegerKey<'a> {
    data: PreparedIntegerData,
    selection: &'a SelectionRef<'a>,
    selection_data: *const u32,
    minimum: i128,
    cardinality: usize,
    group_idx: usize,
}

impl<'a> PreparedIntegerKey<'a> {
    fn try_new(
        domain: &'a PerfectHashKeyDomain,
        group: &'a DecodedVectorRef<'a>,
        minimum: i128,
        cardinality: usize,
        group_idx: usize,
    ) -> Option<Self> {
        if !group.validity().all_valid() {
            return None;
        }
        let data = match (domain.logical_type(), group.data()) {
            (LogicalType::TinyInt, DataRef::Ptr(ptr)) => PreparedIntegerData::I8(ptr.cast()),
            (LogicalType::SmallInt, DataRef::Ptr(ptr)) => PreparedIntegerData::I16(ptr.cast()),
            (LogicalType::Integer, DataRef::Ptr(ptr)) => PreparedIntegerData::I32(ptr.cast()),
            (LogicalType::BigInt, DataRef::Ptr(ptr)) => PreparedIntegerData::I64(ptr.cast()),
            (LogicalType::HugeInt, DataRef::Ptr(ptr)) => PreparedIntegerData::I128(ptr.cast()),
            (LogicalType::UTinyInt, DataRef::Ptr(ptr)) => PreparedIntegerData::U8(ptr.cast()),
            (LogicalType::USmallInt, DataRef::Ptr(ptr)) => PreparedIntegerData::U16(ptr.cast()),
            (LogicalType::UInteger, DataRef::Ptr(ptr)) => PreparedIntegerData::U32(ptr.cast()),
            (LogicalType::UBigInt, DataRef::Ptr(ptr)) => PreparedIntegerData::U64(ptr.cast()),
            (LogicalType::UHugeInt, DataRef::Ptr(ptr)) => PreparedIntegerData::U128(ptr.cast()),
            (LogicalType::BigInt, DataRef::SequenceI64 { start, increment }) => {
                PreparedIntegerData::SequenceI64 { start, increment }
            }
            _ => return None,
        };
        Some(Self {
            data,
            selection: group.sel(),
            selection_data: group
                .sel()
                .materialized_indices()
                .map_or(std::ptr::null(), <[u32]>::as_ptr),
            minimum,
            cardinality,
            group_idx,
        })
    }

    #[inline(always)]
    fn encoded(&self, row: usize) -> Result<usize> {
        let physical = if self.selection_data.is_null() {
            self.selection.get(row)
        } else {
            // SAFETY: materialized selection indices cover the logical batch.
            unsafe { *self.selection_data.add(row) as usize }
        };
        // SAFETY: decoded vector data and its selection remain borrowed for
        // this prepared key's lifetime; `physical` is a validated row index.
        let value = unsafe { self.physical_value(physical)? };
        adjust_group_value(value, self.minimum, self.cardinality, self.group_idx)
    }

    /// # Safety
    ///
    /// `physical` must identify a row in the prepared decoded vector.
    #[inline(always)]
    unsafe fn physical_value(&self, physical: usize) -> Result<i128> {
        Ok(match self.data {
            PreparedIntegerData::I8(data) => unsafe { *data.add(physical) as i128 },
            PreparedIntegerData::I16(data) => unsafe { *data.add(physical) as i128 },
            PreparedIntegerData::I32(data) => unsafe { *data.add(physical) as i128 },
            PreparedIntegerData::I64(data) => unsafe { *data.add(physical) as i128 },
            PreparedIntegerData::I128(data) => unsafe { *data.add(physical) },
            PreparedIntegerData::U8(data) => unsafe { *data.add(physical) as i128 },
            PreparedIntegerData::U16(data) => unsafe { *data.add(physical) as i128 },
            PreparedIntegerData::U32(data) => unsafe { *data.add(physical) as i128 },
            PreparedIntegerData::U64(data) => i128::from(unsafe { *data.add(physical) }),
            PreparedIntegerData::U128(data) => i128::try_from(unsafe { *data.add(physical) })
                .map_err(|_| {
                    paro_error::internal("UHUGEINT key exceeds the perfect-hash domain")
                })?,
            PreparedIntegerData::SequenceI64 { start, increment } => i128::from(
                start
                    .checked_add(
                        i64::try_from(physical)
                            .ok()
                            .and_then(|physical| physical.checked_mul(increment))
                            .ok_or_else(|| {
                                paro_error::out_of_range("perfect aggregate sequence key overflow")
                            })?,
                    )
                    .ok_or_else(|| {
                        paro_error::out_of_range("perfect aggregate sequence key overflow")
                    })?,
            ),
        })
    }
}

pub(super) enum PreparedGroupKey<'a> {
    Dictionary(PreparedDictionaryKey<'a>),
    Integer(PreparedIntegerKey<'a>),
}

impl PreparedGroupKey<'_> {
    #[inline(always)]
    fn encoded(&mut self, row: usize) -> Result<usize> {
        match self {
            Self::Dictionary(key) => key.encoded(row),
            Self::Integer(key) => key.encoded(row),
        }
    }
}

/// Batch-local execution strategy for perfect-hash group keys.
///
/// Single and two-key analytical groups dominate direct-address aggregation.
/// Compiling physical integer reads and dictionary caches once removes logical
/// type, key-count, and optional-codec dispatch from the row loop while the
/// generic mixed-radix representation remains the complete fallback.
pub(super) enum PreparedSlotEncoding<'a> {
    Single(PreparedGroupKey<'a>),
    Pair {
        first: PreparedGroupKey<'a>,
        second: PreparedGroupKey<'a>,
    },
    Generic(SmallVec<[Option<PreparedGroupKey<'a>>; 4]>),
}

impl<'a> PreparedSlotEncoding<'a> {
    fn new(mut keys: SmallVec<[Option<PreparedGroupKey<'a>>; 4]>) -> Self {
        if keys.len() == 1 && keys[0].is_some() {
            return Self::Single(keys.pop().flatten().expect("key was checked"));
        }
        if keys.len() == 2 && keys[0].is_some() && keys[1].is_some() {
            let second = keys.pop().flatten().expect("second key was checked");
            let first = keys.pop().flatten().expect("first key was checked");
            Self::Pair { first, second }
        } else {
            Self::Generic(keys)
        }
    }
}

pub(super) fn decode_group_batch<'a>(
    groups: &'a Chunk,
    group_count: usize,
) -> Result<SmallVec<[DecodedVectorRef<'a>; 4]>> {
    (0..group_count)
        .map(|group_idx| {
            groups
                .column(group_idx)
                .ok_or_else(|| {
                    paro_error::internal(format!(
                        "Missing group key column for perfect hash table: group_idx={group_idx}"
                    ))
                })
                .and_then(|column| column.try_decode_ref(groups.size()))
        })
        .collect()
}

pub(super) fn prepare_slot_encoding<'a>(
    group_domains: &'a [PerfectHashKeyDomain],
    group_minima: &[i128],
    slot_layout: &PerfectHashSlotLayout,
    decoded_groups: &'a [DecodedVectorRef<'a>],
    rows: usize,
    memory: &MemoryAccountingContext,
) -> Result<PreparedSlotEncoding<'a>> {
    let keys = decoded_groups
        .iter()
        .enumerate()
        .map(|(group_idx, group)| {
            let dictionary = PreparedDictionaryKey::try_new(
                &group_domains[group_idx],
                group,
                rows,
                group_minima[group_idx],
                slot_layout.cardinalities[group_idx],
                group_idx,
                memory,
            )?;
            Ok(dictionary.map(PreparedGroupKey::Dictionary).or_else(|| {
                PreparedIntegerKey::try_new(
                    &group_domains[group_idx],
                    group,
                    group_minima[group_idx],
                    slot_layout.cardinalities[group_idx],
                    group_idx,
                )
                .map(PreparedGroupKey::Integer)
            }))
        })
        .collect::<Result<SmallVec<[_; 4]>>>()?;
    Ok(PreparedSlotEncoding::new(keys))
}

#[inline(always)]
pub(super) fn compute_slot_from_prepared(
    group_domains: &[PerfectHashKeyDomain],
    group_minima: &[i128],
    slot_layout: &PerfectHashSlotLayout,
    decoded_groups: &[DecodedVectorRef<'_>],
    slot_encoding: &mut PreparedSlotEncoding<'_>,
    row_idx: usize,
) -> Result<usize> {
    match slot_encoding {
        PreparedSlotEncoding::Single(key) => return key.encoded(row_idx),
        PreparedSlotEncoding::Pair { first, second } => {
            let slot = slot_layout.encode_pair(first.encoded(row_idx)?, second.encoded(row_idx)?);
            debug_assert!(slot < slot_layout.slot_count);
            return Ok(slot);
        }
        PreparedSlotEncoding::Generic(_) => {}
    }
    let PreparedSlotEncoding::Generic(prepared_keys) = slot_encoding else {
        unreachable!("specialized encoding returned above")
    };
    compute_generic_slot_from_prepared(
        group_domains,
        group_minima,
        slot_layout,
        decoded_groups,
        prepared_keys,
        row_idx,
    )
}

#[inline(always)]
fn compute_generic_slot_from_prepared(
    group_domains: &[PerfectHashKeyDomain],
    group_minima: &[i128],
    slot_layout: &PerfectHashSlotLayout,
    decoded_groups: &[DecodedVectorRef<'_>],
    prepared_keys: &mut [Option<PreparedGroupKey<'_>>],
    row_idx: usize,
) -> Result<usize> {
    if prepared_keys.len() != group_domains.len() {
        return Err(paro_error::internal(
            "perfect aggregate prepared key count mismatch",
        ));
    }
    let slot = match group_domains.len() {
        1 => encoded_group_component(
            group_domains,
            group_minima,
            slot_layout,
            decoded_groups,
            prepared_keys,
            row_idx,
            0,
        )?,
        2 => {
            let first = encoded_group_component(
                group_domains,
                group_minima,
                slot_layout,
                decoded_groups,
                prepared_keys,
                row_idx,
                0,
            )?;
            let second = encoded_group_component(
                group_domains,
                group_minima,
                slot_layout,
                decoded_groups,
                prepared_keys,
                row_idx,
                1,
            )?;
            slot_layout.encode_pair(first, second)
        }
        _ => {
            let mut slot = 0usize;
            for group_idx in 0..group_domains.len() {
                let encoded = encoded_group_component(
                    group_domains,
                    group_minima,
                    slot_layout,
                    decoded_groups,
                    prepared_keys,
                    row_idx,
                    group_idx,
                )?;
                // Construction validated the mixed-radix product and every
                // component is range-checked by `adjust_group_value`.
                slot += encoded * slot_layout.strides[group_idx];
            }
            slot
        }
    };
    debug_assert!(slot < slot_layout.slot_count);
    Ok(slot)
}

#[inline(always)]
fn encoded_group_component(
    group_domains: &[PerfectHashKeyDomain],
    group_minima: &[i128],
    slot_layout: &PerfectHashSlotLayout,
    decoded_groups: &[DecodedVectorRef<'_>],
    prepared_keys: &mut [Option<PreparedGroupKey<'_>>],
    row_idx: usize,
    group_idx: usize,
) -> Result<usize> {
    match &mut prepared_keys[group_idx] {
        Some(prepared) => prepared.encoded(row_idx),
        None => {
            let decoded_group = &decoded_groups[group_idx];
            let physical_idx = decoded_group.physical_index(row_idx);
            if !decoded_group.validity().is_valid(physical_idx) {
                return Ok(0);
            }
            let value = group_domains[group_idx].encode_decoded(decoded_group, physical_idx)?;
            adjust_group_value(
                value,
                group_minima[group_idx],
                slot_layout.cardinalities[group_idx],
                group_idx,
            )
        }
    }
}

fn adjust_group_value(
    value: i128,
    minimum: i128,
    cardinality: usize,
    group_idx: usize,
) -> Result<usize> {
    let adjusted = value
        .checked_sub(minimum)
        .and_then(|delta| delta.checked_add(1))
        .ok_or_else(|| {
            paro_error::internal(format!(
                "Perfect aggregate adjusted key overflow: value={value}, min={minimum}, group_idx={group_idx}"
            ))
        })?;
    if adjusted <= 0 {
        return Err(paro_error::internal(format!(
            "Perfect aggregate key smaller than expected minimum: value={value}, min={minimum}, group_idx={group_idx}"
        )));
    }
    let adjusted = usize::try_from(adjusted).map_err(|_| {
        paro_error::internal(format!(
            "Perfect aggregate adjusted key exceeds usize: adjusted={adjusted}, group_idx={group_idx}"
        ))
    })?;
    if adjusted >= cardinality {
        return Err(paro_error::internal(format!(
            "Perfect aggregate key exceeds planned range: adjusted={adjusted}, cardinality={cardinality}, group_idx={group_idx}"
        )));
    }
    Ok(adjusted)
}
