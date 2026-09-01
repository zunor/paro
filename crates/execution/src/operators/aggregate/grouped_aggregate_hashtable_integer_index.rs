// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Runtime-observed exact index for compact integer aggregate groups.
//!
//! The ordinary hash table remains the canonical representation and receives
//! every group. This sidecar only bypasses hashing and linear probing for keys
//! inside a bounded domain observed from execution batches. It may expand or
//! disappear at any time without changing correctness, so cached plans never
//! depend on snapshot-local bounds.

use paro_common::hash::{hash_i64, hash_u64, NULL_HASH};
use paro_common::types::LogicalType;
use paro_common::vector::{DataRef, VectorView};

use super::*;

const MAX_ADAPTIVE_INTEGER_GROUP_SLOTS: usize = 4_096;
const MIN_ADAPTIVE_INTEGER_GROUP_SLOTS: usize = 64;
const MAX_SLOTS_PER_OBSERVED_ROW: usize = 8;
const SIGNED_ORDINAL_MASK: u128 = 1_u128 << 127;

#[derive(Debug)]
pub(super) enum AdaptiveIntegerGroupIndexState {
    Candidate,
    Active(AdaptiveIntegerGroupIndex),
    Disabled,
}

impl AdaptiveIntegerGroupIndexState {
    pub(super) fn memory_usage(&self) -> usize {
        match self {
            Self::Active(index) => index.rows.capacity() * size_of::<u64>(),
            Self::Candidate | Self::Disabled => 0,
        }
    }
}

#[derive(Debug)]
pub(super) struct AdaptiveIntegerGroupIndex {
    kind: IntegerGroupKind,
    base: u128,
    rows: AccountedVec<u64>,
    null_row: u64,
    mapped_count: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IntegerGroupKind {
    TinyInt,
    SmallInt,
    Integer,
    BigInt,
    UTinyInt,
    USmallInt,
    UInteger,
    UBigInt,
    Date,
}

#[derive(Debug, Clone, Copy)]
enum DirectSlot {
    Dense(usize),
    Null,
}

impl IntegerGroupKind {
    fn from_logical_type(logical_type: &LogicalType) -> Option<Self> {
        match logical_type {
            LogicalType::TinyInt => Some(Self::TinyInt),
            LogicalType::SmallInt => Some(Self::SmallInt),
            LogicalType::Integer => Some(Self::Integer),
            LogicalType::BigInt => Some(Self::BigInt),
            LogicalType::UTinyInt => Some(Self::UTinyInt),
            LogicalType::USmallInt => Some(Self::USmallInt),
            LogicalType::UInteger => Some(Self::UInteger),
            LogicalType::UBigInt => Some(Self::UBigInt),
            LogicalType::Date => Some(Self::Date),
            _ => None,
        }
    }

    #[inline]
    fn ordinal(self, view: &VectorView<'_>, row_idx: usize) -> Option<u128> {
        if !view.is_valid(row_idx) {
            return None;
        }
        let physical_idx = view.physical_index(row_idx);
        let DataRef::Ptr(data) = view.data() else {
            return (self == Self::BigInt).then(|| signed_ordinal(view.get_i64(row_idx) as i128));
        };
        macro_rules! read {
            ($ty:ty) => {{
                unsafe { *(data as *const $ty).add(physical_idx) }
            }};
        }
        Some(match self {
            Self::TinyInt => signed_ordinal(read!(i8) as i128),
            Self::SmallInt => signed_ordinal(read!(i16) as i128),
            Self::Integer | Self::Date => signed_ordinal(read!(i32) as i128),
            Self::BigInt => signed_ordinal(read!(i64) as i128),
            Self::UTinyInt => u128::from(read!(u8)),
            Self::USmallInt => u128::from(read!(u16)),
            Self::UInteger => u128::from(read!(u32)),
            Self::UBigInt => u128::from(read!(u64)),
        })
    }

    #[inline]
    fn hash(self, view: &VectorView<'_>, row_idx: usize) -> u64 {
        if !view.is_valid(row_idx) {
            return NULL_HASH;
        }
        let physical_idx = view.physical_index(row_idx);
        let DataRef::Ptr(data) = view.data() else {
            debug_assert_eq!(self, Self::BigInt);
            return hash_i64(view.get_i64(row_idx));
        };
        macro_rules! read {
            ($ty:ty) => {{
                unsafe { *(data as *const $ty).add(physical_idx) }
            }};
        }
        match self {
            Self::TinyInt => hash_i64(read!(i8) as i64),
            Self::SmallInt => hash_i64(read!(i16) as i64),
            Self::Integer | Self::Date => hash_i64(read!(i32) as i64),
            Self::BigInt => hash_i64(read!(i64)),
            Self::UTinyInt => hash_u64(read!(u8) as u64),
            Self::USmallInt => hash_u64(read!(u16) as u64),
            Self::UInteger => hash_u64(read!(u32) as u64),
            Self::UBigInt => hash_u64(read!(u64)),
        }
    }
}

impl AdaptiveIntegerGroupIndex {
    fn try_new(
        kind: IntegerGroupKind,
        view: &VectorView<'_>,
        row_count: usize,
        memory: &MemoryAccountingContext,
    ) -> Result<Option<Self>> {
        let mut rows = accounted_vec_for_context(
            &memory.with_class(MemoryAccountingClass::Metadata),
            MemoryTag::HashTable,
            MemoryAccountingClass::Metadata,
        )?;
        let bounds = batch_bounds(kind, view, row_count);
        let (base, slot_count) = match bounds {
            Some((minimum, maximum)) => {
                let Some(slot_count) = domain_size(minimum, maximum) else {
                    return Ok(None);
                };
                if !domain_is_bounded(slot_count, row_count) {
                    return Ok(None);
                }
                (minimum, slot_count)
            }
            None => (0, 0),
        };
        if rows.try_resize_with(slot_count, || 0).is_err() {
            return Ok(None);
        }
        Ok(Some(Self {
            kind,
            base,
            rows,
            null_row: 0,
            mapped_count: 0,
        }))
    }

    /// Expand the observed domain before mutating either representation.
    /// Allocation failure or an overly sparse domain is a clean decline.
    fn prepare_batch(
        &mut self,
        view: &VectorView<'_>,
        row_count: usize,
        existing_groups: usize,
    ) -> bool {
        let Some((batch_minimum, batch_maximum)) = batch_bounds(self.kind, view, row_count) else {
            return true;
        };
        let (new_base, new_maximum) = if self.rows.is_empty() {
            (batch_minimum, batch_maximum)
        } else {
            let current_maximum = self.base + self.rows.len() as u128 - 1;
            (
                self.base.min(batch_minimum),
                current_maximum.max(batch_maximum),
            )
        };
        let Some(new_slot_count) = domain_size(new_base, new_maximum) else {
            return false;
        };
        if !domain_is_bounded(new_slot_count, existing_groups.saturating_add(row_count)) {
            return false;
        }
        if new_slot_count == self.rows.len() && new_base == self.base {
            return true;
        }

        let old_len = self.rows.len();
        let base_shift = self
            .base
            .checked_sub(new_base)
            .and_then(|shift| usize::try_from(shift).ok())
            .unwrap_or(0);
        if self.rows.try_resize_with(new_slot_count, || 0).is_err() {
            return false;
        }
        if old_len > 0 && base_shift > 0 {
            self.rows.copy_within(0..old_len, base_shift);
            self.rows[..base_shift].fill(0);
        }
        self.base = new_base;
        true
    }

    #[inline]
    fn slot(&self, view: &VectorView<'_>, row_idx: usize) -> DirectSlot {
        match self.kind.ordinal(view, row_idx) {
            Some(ordinal) => {
                let offset = usize::try_from(ordinal - self.base)
                    .expect("prepared integer group domain must contain every row");
                DirectSlot::Dense(offset)
            }
            None => DirectSlot::Null,
        }
    }

    #[inline]
    fn mapped_row(&self, slot: DirectSlot) -> Option<usize> {
        let encoded = match slot {
            DirectSlot::Dense(slot) => self.rows[slot],
            DirectSlot::Null => self.null_row,
        };
        (encoded != 0).then(|| encoded as usize - 1)
    }

    #[inline]
    fn map_row(&mut self, slot: DirectSlot, row_idx: usize) -> Result<()> {
        let encoded = u64::try_from(row_idx)
            .ok()
            .and_then(|row| row.checked_add(1))
            .ok_or_else(|| paro_error::internal("Adaptive integer aggregate row index overflow"))?;
        match slot {
            DirectSlot::Dense(slot) => self.rows[slot] = encoded,
            DirectSlot::Null => self.null_row = encoded,
        }
        self.mapped_count += 1;
        Ok(())
    }

    fn maximum_new_groups(&self, input_rows: usize) -> usize {
        input_rows.min(
            self.rows
                .len()
                .saturating_add(1)
                .saturating_sub(self.mapped_count),
        )
    }
}

impl GroupedAggregateHashTable {
    /// Try an exact runtime index for a single compact integer grouping key.
    ///
    /// `true` means addresses were produced. `false` is a no-mutation decline
    /// and the caller must execute ordinary vector hashing and probing.
    pub(crate) fn try_find_or_create_adaptive_integer_groups(
        &mut self,
        groups: &Chunk,
        addresses: &mut Vector,
        new_groups: &mut SelectionVector,
    ) -> Result<bool> {
        self.validate_group_chunk(groups)?;
        validate_addresses_vector(addresses, groups.size())?;
        addresses.try_set_count(groups.size())?;
        if groups.size() == 0 {
            new_groups.set_len(0);
            return Ok(true);
        }

        let candidate_kind = (groups.column_count() == 1)
            .then(|| IntegerGroupKind::from_logical_type(groups.column(0)?.logical_type()))
            .flatten();
        let Some(kind) = candidate_kind else {
            self.adaptive_integer_index = AdaptiveIntegerGroupIndexState::Disabled;
            return Ok(false);
        };
        let column = groups
            .column(0)
            .ok_or_else(|| paro_error::internal("Missing adaptive integer aggregate key"))?;
        let view = column.try_to_view(groups.size())?;

        let state = std::mem::replace(
            &mut self.adaptive_integer_index,
            AdaptiveIntegerGroupIndexState::Disabled,
        );
        let mut index = match state {
            AdaptiveIntegerGroupIndexState::Candidate if self.count == 0 => {
                let Some(index) =
                    AdaptiveIntegerGroupIndex::try_new(kind, &view, groups.size(), &self.memory)?
                else {
                    return Ok(false);
                };
                index
            }
            AdaptiveIntegerGroupIndexState::Active(index) if index.kind == kind => index,
            AdaptiveIntegerGroupIndexState::Candidate
            | AdaptiveIntegerGroupIndexState::Active(_)
            | AdaptiveIntegerGroupIndexState::Disabled => return Ok(false),
        };
        if !index.prepare_batch(&view, groups.size(), self.count) {
            return Ok(false);
        }

        let result = self.find_or_create_adaptive_integer_groups_inner(
            groups, &view, &mut index, addresses, new_groups,
        );
        self.adaptive_integer_index = AdaptiveIntegerGroupIndexState::Active(index);
        result.map(|_| true)
    }

    fn find_or_create_adaptive_integer_groups_inner(
        &mut self,
        groups: &Chunk,
        view: &VectorView<'_>,
        index: &mut AdaptiveIntegerGroupIndex,
        addresses: &mut Vector,
        new_groups: &mut SelectionVector,
    ) -> Result<usize> {
        self.ensure_lookup_storage_available()?;
        let scatter_source = self.layout.prepare_scatter(groups)?;
        let possible_new_groups = index.maximum_new_groups(groups.size());
        self.ensure_capacity_for(possible_new_groups)?;
        self.ensure_row_storage_capacity(possible_new_groups)?;

        if new_groups.capacity() < groups.size() {
            *new_groups =
                SelectionVector::try_with_capacity(groups.size(), groups.allocator().clone())?;
        }
        new_groups.set_len(groups.size());
        let new_group_data = new_groups.as_mut_slice().as_mut_ptr();
        let address_data = unsafe { addresses.flat_data_mut::<*mut u8>() };
        let inline_key_layout = self.inline_key_layout.clone().ok_or_else(|| {
            paro_error::internal("Adaptive integer aggregate requires inline key storage")
        })?;
        let inline_key_data = self.inline_key_storage_mut_ptr()?;
        let mut new_state_ptrs = Vec::with_capacity(possible_new_groups);
        let mut new_group_count = 0usize;

        for row_idx in 0..groups.size() {
            let direct_slot = index.slot(view, row_idx);
            if let Some(group_row_idx) = index.mapped_row(direct_slot) {
                unsafe {
                    *address_data.add(row_idx) = self.state_ptr(group_row_idx);
                }
                continue;
            }

            let hash = index.kind.hash(view, row_idx);
            let inline_key = inline_key_layout.encode_row(groups, row_idx)?;
            let mut hash_slot = self.slot_for_hash(hash);
            while self.entries[hash_slot].is_occupied() {
                hash_slot = (hash_slot + 1) & self.bitmask;
            }
            let group_row_idx = self.append_group_row(&scatter_source, row_idx, hash)?;
            self.entries[hash_slot] = AggregateHTEntry::from_hash_and_row(hash, group_row_idx)?;
            unsafe {
                *inline_key_data.add(hash_slot) = inline_key;
            }
            self.count += 1;
            index.map_row(direct_slot, group_row_idx)?;
            let state_ptr = self.state_ptr(group_row_idx);
            unsafe {
                *address_data.add(row_idx) = state_ptr;
                *new_group_data.add(new_group_count) = row_idx as u32;
            }
            new_group_count += 1;
            if !self.aggregate_objects.is_empty() {
                new_state_ptrs.push(state_ptr);
            }
        }

        if !new_state_ptrs.is_empty() {
            let new_addresses = pointer_vector_from_slice(&new_state_ptrs, self.allocator())?;
            initialize_states(
                &self.state_layout,
                &self.aggregate_objects,
                &new_addresses,
                new_state_ptrs.len(),
            )?;
        }
        new_groups.set_len(new_group_count);
        Ok(new_group_count)
    }
}

fn batch_bounds(
    kind: IntegerGroupKind,
    view: &VectorView<'_>,
    row_count: usize,
) -> Option<(u128, u128)> {
    let mut minimum = u128::MAX;
    let mut maximum = 0;
    let mut found = false;
    for row_idx in 0..row_count {
        let Some(ordinal) = kind.ordinal(view, row_idx) else {
            continue;
        };
        minimum = minimum.min(ordinal);
        maximum = maximum.max(ordinal);
        found = true;
    }
    found.then_some((minimum, maximum))
}

fn domain_size(minimum: u128, maximum: u128) -> Option<usize> {
    maximum
        .checked_sub(minimum)?
        .checked_add(1)
        .and_then(|slots| usize::try_from(slots).ok())
}

fn domain_is_bounded(slot_count: usize, observed_rows: usize) -> bool {
    let density_limit = observed_rows
        .saturating_mul(MAX_SLOTS_PER_OBSERVED_ROW)
        .max(MIN_ADAPTIVE_INTEGER_GROUP_SLOTS);
    slot_count <= MAX_ADAPTIVE_INTEGER_GROUP_SLOTS && slot_count <= density_limit
}

#[inline]
fn signed_ordinal(value: i128) -> u128 {
    (value as u128) ^ SIGNED_ORDINAL_MASK
}
