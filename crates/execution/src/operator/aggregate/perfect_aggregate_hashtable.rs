// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Perfect-hash aggregate hash table with direct array indexing.

use std::mem::size_of;
use std::sync::Arc;

use paro_common::allocator::{Allocator, ArenaAllocator};
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_common::vector::{DecodedVectorOwned, SelectionVector, Vector};
use paro_function::aggregate::{AggregateCombineType, AggregateInputData};

use super::aggregate_kernel::{
    combine_states, destroy_states, finalize_states, update_filtered_states, update_states,
    AggregatePayload,
};
use super::aggregate_object::AggregateObject;
use super::aggregate_state::AggregateStateLayout;

/// Scan cursor for [`PerfectAggregateHashTable::scan`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PerfectHTScanPosition {
    pub offset: usize,
}

/// Direct-addressing aggregate table used by perfect-hash GROUP BY.
#[derive(Debug)]
pub struct PerfectAggregateHashTable {
    group_types: Vec<LogicalType>,
    group_minima: Vec<i128>,
    required_bits: Vec<usize>,
    bit_offsets: Vec<usize>,
    total_groups: usize,
    state_layout: AggregateStateLayout,
    aggregate_objects: Vec<AggregateObject>,
    aggregate_inputs: Vec<Vec<usize>>,
    aggregate_return_types: Vec<LogicalType>,
    // 0 = empty, 1 = occupied
    occupancy: Vec<u8>,
    // Keep row storage 8-byte aligned so aggregate states can be safely cast to typed pointers.
    data: Vec<u64>,
    row_width: usize,
    aggregate_allocator: ArenaAllocator,
    count: usize,
}

impl PerfectAggregateHashTable {
    pub fn new(
        group_types: Vec<LogicalType>,
        aggregate_objects: Vec<AggregateObject>,
        aggregate_inputs: Vec<Vec<usize>>,
        group_minima: Vec<i128>,
        required_bits: Vec<usize>,
        allocator: Arc<dyn Allocator>,
    ) -> Result<Self> {
        if group_types.is_empty() {
            return Err(paro_error::internal(
                "PerfectAggregateHashTable requires at least one group key".to_string(),
            ));
        }
        if group_types.len() != group_minima.len() || group_types.len() != required_bits.len() {
            return Err(paro_error::internal(format!(
                "PerfectAggregateHashTable group metadata mismatch: types={} minima={} bits={}",
                group_types.len(),
                group_minima.len(),
                required_bits.len()
            )));
        }
        validate_aggregate_inputs(&aggregate_objects, &aggregate_inputs)?;

        let mut bit_offsets = Vec::with_capacity(required_bits.len());
        let mut total_bits = 0usize;
        for &bits in &required_bits {
            if bits == 0 || bits >= usize::BITS as usize {
                return Err(paro_error::internal(format!(
                    "Invalid required bits for perfect aggregate hash table: bits={bits}"
                )));
            }
            total_bits = total_bits.checked_add(bits).ok_or_else(|| {
                paro_error::internal(format!(
                    "Perfect aggregate bit width overflow: total_bits={total_bits} + bits={bits}"
                ))
            })?;
        }
        if total_bits >= usize::BITS as usize {
            return Err(paro_error::internal(format!(
                "Perfect aggregate bit width exceeds pointer width: total_bits={total_bits}"
            )));
        }

        let mut shift = total_bits;
        for &bits in &required_bits {
            shift -= bits;
            bit_offsets.push(shift);
        }

        let total_groups = 1usize.checked_shl(total_bits as u32).ok_or_else(|| {
            paro_error::internal(format!(
                "Perfect aggregate slot count overflow for total_bits={total_bits}"
            ))
        })?;
        if total_groups == 0 {
            return Err(paro_error::internal(
                "Perfect aggregate slot count resolved to zero".to_string(),
            ));
        }

        let state_layout = AggregateStateLayout::new(&aggregate_objects)?;
        let row_width = state_layout.total_size().max(1);
        let total_bytes = row_width.checked_mul(total_groups).ok_or_else(|| {
            paro_error::internal(format!(
                "Perfect aggregate row storage overflow: row_width={row_width}, groups={total_groups}"
            ))
        })?;
        let total_words = bytes_to_words(total_bytes)?;
        let data = vec![0u64; total_words];
        let occupancy = vec![0u8; total_groups];
        let aggregate_return_types = aggregate_objects
            .iter()
            .map(|obj| obj.return_type.clone())
            .collect::<Vec<_>>();

        Ok(Self {
            group_types,
            group_minima,
            required_bits,
            bit_offsets,
            total_groups,
            state_layout,
            aggregate_objects,
            aggregate_inputs,
            aggregate_return_types,
            occupancy,
            data,
            row_width,
            aggregate_allocator: ArenaAllocator::new(allocator),
            count: 0,
        })
    }

    pub fn count(&self) -> usize {
        self.count
    }

    pub fn total_groups(&self) -> usize {
        self.total_groups
    }

    pub fn allocator(&self) -> Arc<dyn Allocator> {
        self.aggregate_allocator.get_allocator().clone()
    }

    /// Probe and insert grouped keys, returning state addresses for each input row.
    pub fn find_or_create_groups(
        &mut self,
        groups: &Chunk,
        addresses: &mut Vector,
        new_groups: &mut SelectionVector,
    ) -> Result<usize> {
        self.validate_group_chunk(groups)?;
        validate_addresses_vector(addresses, groups.size())?;

        if groups.size() == 0 {
            addresses.set_count(0);
            *new_groups =
                SelectionVector::try_from_indices(Vec::new(), groups.allocator().clone())?;
            return Ok(0);
        }

        let decoded_groups = (0..self.group_types.len())
            .map(|group_idx| {
                groups
                    .column(group_idx)
                    .ok_or_else(|| {
                        paro_error::internal(format!(
                            "Missing group key column for perfect hash table: group_idx={group_idx}"
                        ))
                    })
                    .and_then(|column| column.try_decode(groups.size()))
            })
            .collect::<Result<Vec<_>>>()?;

        addresses.set_count(groups.size());
        let address_data = unsafe { addresses.flat_data_mut::<*mut u8>() };
        let mut new_group_rows = Vec::new();

        for row_idx in 0..groups.size() {
            let slot = self.compute_slot_from_decoded(&decoded_groups, row_idx)?;
            let state_ptr = self.state_ptr(slot);
            unsafe {
                *address_data.add(row_idx) = state_ptr;
            }
            if self.occupancy[slot] == 0 {
                self.occupancy[slot] = 1;
                self.count += 1;
                self.initialize_state(state_ptr);
                new_group_rows.push(row_idx as u32);
            }
        }

        *new_groups =
            SelectionVector::try_from_indices(new_group_rows, groups.allocator().clone())?;
        Ok(new_groups.len())
    }

    /// Update aggregate states for a batch of input payload rows.
    pub fn update_aggregates(
        &mut self,
        payload: &Chunk,
        addresses: &Vector,
        filter: Option<&SelectionVector>,
    ) -> Result<()> {
        if payload.size() == 0 || self.aggregate_objects.is_empty() {
            return Ok(());
        }
        if addresses.len() < payload.size() {
            return Err(paro_error::internal(format!(
                "Address vector too small for perfect aggregate update: addresses={} payload_rows={}",
                addresses.len(),
                payload.size()
            )));
        }
        if let Some(selection) = filter {
            validate_filter(selection, payload.size())?;
        }

        let payload_desc = AggregatePayload {
            chunk: payload,
            aggregate_inputs: &self.aggregate_inputs,
        };
        let mut input_data = AggregateInputData::new(
            None,
            &mut self.aggregate_allocator,
            AggregateCombineType::PreserveInput,
        );
        if let Some(selection) = filter {
            update_filtered_states(
                &self.aggregate_objects,
                &mut input_data,
                &payload_desc,
                addresses,
                selection,
                selection.len(),
            )?;
        } else {
            update_states(
                &self.aggregate_objects,
                &mut input_data,
                &payload_desc,
                addresses,
                payload.size(),
            )?;
        }
        Ok(())
    }

    /// Combine aggregate states from another perfect hash table into this table.
    pub fn combine(&mut self, other: &mut Self) -> Result<()> {
        self.ensure_compatible(other)?;
        if other.count == 0 {
            return Ok(());
        }

        let mut source_ptrs =
            Vec::with_capacity(self.total_groups.min(paro_common::vector::VECTOR_SIZE));
        let mut target_ptrs =
            Vec::with_capacity(self.total_groups.min(paro_common::vector::VECTOR_SIZE));

        for slot in 0..self.total_groups {
            if other.occupancy[slot] == 0 {
                continue;
            }
            if self.occupancy[slot] == 0 {
                self.occupancy[slot] = 1;
                self.count += 1;
                self.initialize_state(self.state_ptr(slot));
            }
            source_ptrs.push(other.state_ptr(slot));
            target_ptrs.push(self.state_ptr(slot));

            if source_ptrs.len() == paro_common::vector::VECTOR_SIZE {
                self.combine_pointer_batch(&source_ptrs, &target_ptrs)?;
                source_ptrs.clear();
                target_ptrs.clear();
            }
        }

        if !source_ptrs.is_empty() {
            self.combine_pointer_batch(&source_ptrs, &target_ptrs)?;
        }
        Ok(())
    }

    /// Scan grouped keys + finalized aggregate values into `result`.
    ///
    /// Returns `true` if output rows were produced, `false` when scan is complete.
    pub fn scan(
        &mut self,
        position: &mut PerfectHTScanPosition,
        result: &mut Chunk,
    ) -> Result<bool> {
        let group_count = self.group_types.len();
        let aggregate_count = self.aggregate_objects.len();
        let required_columns = group_count + aggregate_count;
        if result.column_count() < required_columns {
            return Err(paro_error::internal(format!(
                "Result chunk has insufficient columns for perfect aggregate scan: required={required_columns}, actual={}",
                result.column_count()
            )));
        }
        if position.offset >= self.total_groups {
            result.set_cardinality(0);
            return Ok(false);
        }

        let mut slots = Vec::with_capacity(result.capacity());
        let mut cursor = position.offset;
        while cursor < self.total_groups && slots.len() < result.capacity() {
            if self.occupancy[cursor] != 0 {
                slots.push(cursor);
            }
            cursor += 1;
        }
        position.offset = cursor;

        if slots.is_empty() {
            result.set_cardinality(0);
            return Ok(false);
        }

        result.set_cardinality(slots.len());

        for group_idx in 0..group_count {
            let result_vector = result.column_mut(group_idx).ok_or_else(|| {
                paro_error::internal(format!(
                    "Missing group output column {group_idx} while scanning perfect aggregate hash table"
                ))
            })?;
            result_vector.set_count(slots.len());
            for (row_idx, &slot) in slots.iter().enumerate() {
                match self.decode_group_value(slot, group_idx)? {
                    Some(value) => result_vector.set_value(row_idx, &value),
                    None => result_vector.set_null(row_idx, true),
                }
            }
        }

        if aggregate_count > 0 {
            let mut state_addresses =
                Vector::try_new(LogicalType::BigInt, slots.len(), result.allocator().clone())?;
            state_addresses.set_count(slots.len());
            unsafe {
                let address_data = state_addresses.flat_data_mut::<*mut u8>();
                for (row_idx, &slot) in slots.iter().enumerate() {
                    *address_data.add(row_idx) = self.state_ptr(slot);
                }
            }

            let mut aggregate_chunk = Chunk::try_initialize(
                &self.aggregate_return_types,
                slots.len(),
                result.allocator().clone(),
            )?;
            let mut input_data = AggregateInputData::new(
                None,
                &mut self.aggregate_allocator,
                AggregateCombineType::PreserveInput,
            );
            finalize_states(
                &self.aggregate_objects,
                &mut input_data,
                &state_addresses,
                &mut aggregate_chunk,
                slots.len(),
            )?;

            for agg_idx in 0..aggregate_count {
                let source = aggregate_chunk.column(agg_idx).ok_or_else(|| {
                    paro_error::internal(format!(
                        "Missing finalized aggregate column {agg_idx} in perfect aggregate scan chunk"
                    ))
                })?;
                let target = result.column_mut(group_count + agg_idx).ok_or_else(|| {
                    paro_error::internal(format!(
                        "Missing aggregate output column {} while scanning perfect aggregate hash table",
                        group_count + agg_idx
                    ))
                })?;
                target.set_count(slots.len());
                for row in 0..slots.len() {
                    target.copy_at(row, source.as_ref(), row);
                }
            }
        }

        Ok(true)
    }

    pub fn destroy(&mut self) -> Result<()> {
        if self.count == 0 || self.aggregate_objects.is_empty() {
            self.occupancy.fill(0);
            self.count = 0;
            self.aggregate_allocator.reset();
            return Ok(());
        }

        let mut ptrs = Vec::with_capacity(self.count);
        for slot in 0..self.total_groups {
            if self.occupancy[slot] != 0 {
                ptrs.push(self.state_ptr(slot));
            }
        }

        if !ptrs.is_empty() {
            let addresses =
                pointer_vector_from_slice(&ptrs, self.aggregate_allocator.get_allocator().clone())?;
            let mut input_data = AggregateInputData::new(
                None,
                &mut self.aggregate_allocator,
                AggregateCombineType::PreserveInput,
            );
            destroy_states(
                &self.aggregate_objects,
                &mut input_data,
                &addresses,
                ptrs.len(),
            )?;
        }

        self.occupancy.fill(0);
        self.count = 0;
        self.aggregate_allocator.reset();
        Ok(())
    }

    pub fn memory_usage(&self) -> usize {
        self.external_accounted_memory_usage() + self.aggregate_allocator.allocation_size()
    }

    pub fn external_accounted_memory_usage(&self) -> usize {
        self.data.capacity() * size_of::<u64>() + self.occupancy.capacity() * size_of::<u8>()
    }

    fn combine_pointer_batch(
        &mut self,
        source_ptrs: &[*mut u8],
        target_ptrs: &[*mut u8],
    ) -> Result<()> {
        debug_assert_eq!(source_ptrs.len(), target_ptrs.len());
        if source_ptrs.is_empty() {
            return Ok(());
        }
        let source = pointer_vector_from_slice(
            source_ptrs,
            self.aggregate_allocator.get_allocator().clone(),
        )?;
        let target = pointer_vector_from_slice(
            target_ptrs,
            self.aggregate_allocator.get_allocator().clone(),
        )?;
        let mut input_data = AggregateInputData::new(
            None,
            &mut self.aggregate_allocator,
            AggregateCombineType::AllowDestructive,
        );
        combine_states(
            &self.aggregate_objects,
            &mut input_data,
            &source,
            &target,
            source_ptrs.len(),
        )
    }

    fn compute_slot_from_decoded(
        &self,
        decoded_groups: &[DecodedVectorOwned],
        row_idx: usize,
    ) -> Result<usize> {
        let mut slot = 0usize;
        for group_idx in 0..self.group_types.len() {
            let encoded =
                self.encoded_group_value(&decoded_groups[group_idx], row_idx, group_idx)?;
            slot |= encoded << self.bit_offsets[group_idx];
        }
        if slot >= self.total_groups {
            return Err(paro_error::internal(format!(
                "Perfect aggregate slot out of bounds: slot={slot}, total_groups={}",
                self.total_groups
            )));
        }
        Ok(slot)
    }

    fn encoded_group_value(
        &self,
        decoded_group: &DecodedVectorOwned,
        row_idx: usize,
        group_idx: usize,
    ) -> Result<usize> {
        let physical_idx = decoded_group.sel().get(row_idx);
        if !decoded_group.validity().is_valid(physical_idx) {
            return Ok(0);
        }

        let value =
            read_group_value_as_i128(decoded_group, &self.group_types[group_idx], physical_idx)?;
        let min_value = self.group_minima[group_idx];
        let adjusted = value
            .checked_sub(min_value)
            .and_then(|delta| delta.checked_add(1))
            .ok_or_else(|| {
                paro_error::internal(format!(
                    "Perfect aggregate adjusted key overflow: value={value}, min={min_value}, group_idx={group_idx}"
                ))
            })?;
        if adjusted <= 0 {
            return Err(paro_error::internal(format!(
                "Perfect aggregate key smaller than expected minimum: value={value}, min={min_value}, group_idx={group_idx}"
            )));
        }
        let max_encoded = max_encoded_for_bits(self.required_bits[group_idx])?;
        let adjusted_u128 = u128::try_from(adjusted).map_err(|_| {
            paro_error::internal(format!(
                "Perfect aggregate adjusted key conversion failed: adjusted={adjusted}, group_idx={group_idx}"
            ))
        })?;
        if adjusted_u128 > max_encoded {
            return Err(paro_error::internal(format!(
                "Perfect aggregate key exceeds planned range: adjusted={adjusted_u128}, max={max_encoded}, group_idx={group_idx}"
            )));
        }
        usize::try_from(adjusted_u128).map_err(|_| {
            paro_error::internal(format!(
                "Perfect aggregate adjusted key exceeds usize: adjusted={adjusted_u128}, group_idx={group_idx}"
            ))
        })
    }

    fn decode_group_value(&self, slot: usize, group_idx: usize) -> Result<Option<Value>> {
        let bits = self.required_bits[group_idx];
        let shift = self.bit_offsets[group_idx];
        let mask = ((1usize << bits) - 1) as u128;
        let encoded = ((slot >> shift) as u128) & mask;
        if encoded == 0 {
            return Ok(None);
        }

        let value = self.group_minima[group_idx]
            .checked_add(i128::try_from(encoded).map_err(|_| {
                paro_error::internal(format!(
                    "Failed to decode perfect aggregate key: encoded={encoded}, group_idx={group_idx}"
                ))
            })?)
            .and_then(|v| v.checked_sub(1))
            .ok_or_else(|| {
                paro_error::internal(format!(
                    "Perfect aggregate decoded value overflow: encoded={encoded}, min={}, group_idx={group_idx}",
                    self.group_minima[group_idx]
                ))
            })?;
        i128_to_value(value, &self.group_types[group_idx]).map(Some)
    }

    fn initialize_state(&self, state_ptr: *mut u8) {
        for (agg_idx, object) in self.aggregate_objects.iter().enumerate() {
            let offset = self.state_layout.state_offset(agg_idx);
            unsafe {
                (object.function.initialize)(state_ptr.add(offset));
            }
        }
    }

    fn validate_group_chunk(&self, groups: &Chunk) -> Result<()> {
        if groups.column_count() != self.group_types.len() {
            return Err(paro_error::internal(format!(
                "Group key column count mismatch for perfect aggregate table: expected={}, actual={}",
                self.group_types.len(),
                groups.column_count()
            )));
        }
        for group_idx in 0..self.group_types.len() {
            let group_type = groups
                .column(group_idx)
                .ok_or_else(|| {
                    paro_error::internal(format!("Missing group column at index {group_idx}"))
                })?
                .logical_type()
                .clone();
            if group_type != self.group_types[group_idx] {
                return Err(paro_error::internal(format!(
                    "Group key type mismatch for perfect aggregate table at index {group_idx}: expected={:?}, actual={:?}",
                    self.group_types[group_idx], group_type
                )));
            }
        }
        Ok(())
    }

    fn ensure_compatible(&self, other: &Self) -> Result<()> {
        if self.group_types != other.group_types
            || self.group_minima != other.group_minima
            || self.required_bits != other.required_bits
            || self.total_groups != other.total_groups
            || self.state_layout.total_size() != other.state_layout.total_size()
        {
            return Err(paro_error::internal(
                "Cannot combine incompatible perfect aggregate hash tables".to_string(),
            ));
        }
        if self.aggregate_objects.len() != other.aggregate_objects.len() {
            return Err(paro_error::internal(format!(
                "Cannot combine perfect aggregate hash tables with different aggregate counts: left={}, right={}",
                self.aggregate_objects.len(),
                other.aggregate_objects.len()
            )));
        }
        for (idx, (left, right)) in self
            .aggregate_objects
            .iter()
            .zip(other.aggregate_objects.iter())
            .enumerate()
        {
            if left.payload_size != right.payload_size
                || left.child_count != right.child_count
                || left.return_type != right.return_type
            {
                return Err(paro_error::internal(format!(
                    "Aggregate object mismatch at index {idx} while combining perfect hash tables"
                )));
            }
        }
        Ok(())
    }

    #[inline]
    fn state_ptr(&self, slot: usize) -> *mut u8 {
        debug_assert!(slot < self.total_groups);
        let offset = slot * self.row_width;
        unsafe { (self.data.as_ptr() as *mut u8).add(offset) }
    }
}

impl Drop for PerfectAggregateHashTable {
    fn drop(&mut self) {
        let _ = self.destroy();
    }
}

fn validate_aggregate_inputs(
    aggregate_objects: &[AggregateObject],
    aggregate_inputs: &[Vec<usize>],
) -> Result<()> {
    if aggregate_objects.len() != aggregate_inputs.len() {
        return Err(paro_error::internal(format!(
            "Aggregate input mapping count mismatch: objects={} mappings={}",
            aggregate_objects.len(),
            aggregate_inputs.len()
        )));
    }
    for (idx, (object, inputs)) in aggregate_objects
        .iter()
        .zip(aggregate_inputs.iter())
        .enumerate()
    {
        if inputs.len() != object.child_count {
            return Err(paro_error::internal(format!(
                "Aggregate input mapping arity mismatch at index {idx}: expected={} actual={}",
                object.child_count,
                inputs.len()
            )));
        }
    }
    Ok(())
}

fn read_group_value_as_i128(
    group: &DecodedVectorOwned,
    ty: &LogicalType,
    physical_idx: usize,
) -> Result<i128> {
    match ty {
        LogicalType::TinyInt => Ok(unsafe { *group.get_data::<i8>().add(physical_idx) } as i128),
        LogicalType::SmallInt => Ok(unsafe { *group.get_data::<i16>().add(physical_idx) } as i128),
        LogicalType::Integer => Ok(unsafe { *group.get_data::<i32>().add(physical_idx) } as i128),
        LogicalType::BigInt => Ok(unsafe { *group.get_data::<i64>().add(physical_idx) } as i128),
        LogicalType::HugeInt => Ok(unsafe { *group.get_data::<i128>().add(physical_idx) }),
        LogicalType::UTinyInt => Ok(unsafe { *group.get_data::<u8>().add(physical_idx) } as i128),
        LogicalType::USmallInt => Ok(unsafe { *group.get_data::<u16>().add(physical_idx) } as i128),
        LogicalType::UInteger => Ok(unsafe { *group.get_data::<u32>().add(physical_idx) } as i128),
        LogicalType::UBigInt => Ok(unsafe { *group.get_data::<u64>().add(physical_idx) } as i128),
        LogicalType::UHugeInt => {
            let value = unsafe { *group.get_data::<u128>().add(physical_idx) };
            i128::try_from(value).map_err(|_| {
                paro_error::internal(format!(
                    "UHUGEINT group key exceeds i128 range in perfect aggregate: {value}"
                ))
            })
        }
        _ => Err(paro_error::internal(format!(
            "Unsupported group key type for perfect aggregate table: {:?}",
            ty
        ))),
    }
}

fn i128_to_value(value: i128, ty: &LogicalType) -> Result<Value> {
    match ty {
        LogicalType::TinyInt => Ok(Value::TinyInt(i8::try_from(value).map_err(|_| {
            paro_error::internal(format!("Decoded value out of TINYINT range: {value}"))
        })?)),
        LogicalType::SmallInt => Ok(Value::SmallInt(i16::try_from(value).map_err(|_| {
            paro_error::internal(format!("Decoded value out of SMALLINT range: {value}"))
        })?)),
        LogicalType::Integer => Ok(Value::Integer(i32::try_from(value).map_err(|_| {
            paro_error::internal(format!("Decoded value out of INTEGER range: {value}"))
        })?)),
        LogicalType::BigInt => Ok(Value::BigInt(i64::try_from(value).map_err(|_| {
            paro_error::internal(format!("Decoded value out of BIGINT range: {value}"))
        })?)),
        LogicalType::HugeInt => Ok(Value::HugeInt(value)),
        LogicalType::UTinyInt => Ok(Value::UTinyInt(u8::try_from(value).map_err(|_| {
            paro_error::internal(format!("Decoded value out of UTINYINT range: {value}"))
        })?)),
        LogicalType::USmallInt => Ok(Value::USmallInt(u16::try_from(value).map_err(|_| {
            paro_error::internal(format!("Decoded value out of USMALLINT range: {value}"))
        })?)),
        LogicalType::UInteger => Ok(Value::UInteger(u32::try_from(value).map_err(|_| {
            paro_error::internal(format!("Decoded value out of UINTEGER range: {value}"))
        })?)),
        LogicalType::UBigInt => Ok(Value::UBigInt(u64::try_from(value).map_err(|_| {
            paro_error::internal(format!("Decoded value out of UBIGINT range: {value}"))
        })?)),
        LogicalType::UHugeInt => Ok(Value::UHugeInt(u128::try_from(value).map_err(|_| {
            paro_error::internal(format!("Decoded value out of UHUGEINT range: {value}"))
        })?)),
        _ => Err(paro_error::internal(format!(
            "Unsupported group key type while decoding perfect aggregate output: {:?}",
            ty
        ))),
    }
}

fn validate_addresses_vector(addresses: &Vector, row_count: usize) -> Result<()> {
    if addresses.capacity() < row_count {
        return Err(paro_error::internal(format!(
            "Address vector capacity too small: required={row_count}, capacity={}",
            addresses.capacity()
        )));
    }
    Ok(())
}

fn validate_filter(filter: &SelectionVector, payload_rows: usize) -> Result<()> {
    for idx in 0..filter.len() {
        let row = filter.get(idx);
        if row >= payload_rows {
            return Err(paro_error::internal(format!(
                "Filter selection index out of bounds: selection[{idx}]={row}, payload_rows={payload_rows}"
            )));
        }
    }
    Ok(())
}

fn pointer_vector_from_slice(
    ptrs: &[*mut u8],
    allocator: Arc<dyn paro_common::allocator::Allocator>,
) -> Result<Vector> {
    let mut result = Vector::try_new(LogicalType::BigInt, ptrs.len(), allocator)?;
    result.set_count(ptrs.len());
    unsafe {
        let result_data = result.flat_data_mut::<*mut u8>();
        for (idx, ptr) in ptrs.iter().enumerate() {
            *result_data.add(idx) = *ptr;
        }
    }
    Ok(result)
}

fn bytes_to_words(bytes: usize) -> Result<usize> {
    let word = size_of::<u64>();
    let words = bytes.checked_add(word - 1).ok_or_else(|| {
        paro_error::internal(format!(
            "Perfect aggregate row storage byte-size overflow: bytes={bytes}"
        ))
    })?;
    Ok(words / word)
}

fn max_encoded_for_bits(bits: usize) -> Result<u128> {
    if bits == 0 || bits >= 128 {
        return Err(paro_error::internal(format!(
            "Invalid bit width for perfect aggregate group key: bits={bits}"
        )));
    }
    Ok((1u128 << bits) - 1)
}
