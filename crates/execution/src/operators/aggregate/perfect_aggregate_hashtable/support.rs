// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::mem::size_of;
use std::sync::Arc;

use paro_common::allocator::{Allocator, MemoryTag};
use paro_common::error::{self as paro_error, Result};
use paro_common::memory::{AccountedVec, MemoryAccountingClass, MemoryGrant};
use paro_common::types::LogicalType;
use paro_common::vector::{SelectionVector, Vector};
use paro_function::aggregate::{AggregateDirectUpdate, DirectGroupedAggregateProgram};

use super::{AggregateObject, AggregateStateLayout};

pub(super) fn compact_state_addresses(
    addresses: &mut Vector,
    selection: &SelectionVector,
    count: usize,
) -> Result<()> {
    if count > selection.len() || selection.len() > addresses.len() {
        return Err(paro_error::internal(format!(
            "invalid aggregate state-filter compaction: selected={count}, selection={}, addresses={}",
            selection.len(),
            addresses.len()
        )));
    }
    // Selection indices are monotonically increasing, so forward in-place
    // compaction never overwrites a source pointer before it is read.
    unsafe {
        let data = addresses.flat_data_mut::<*mut u8>();
        for output_row in 0..count {
            *data.add(output_row) = *data.add(selection.get(output_row));
        }
    }
    addresses.try_set_count(count)
}

pub(super) fn validate_aggregate_inputs(
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

fn direct_payload_input(object: &AggregateObject, inputs: &[usize]) -> Option<usize> {
    if object.function.direct_update == Some(AggregateDirectUpdate::CountStar) {
        return None;
    }
    inputs.first().copied()
}

pub(crate) fn compile_direct_update_program(
    aggregate_objects: &[AggregateObject],
    aggregate_inputs: &[Vec<usize>],
    state_layout: &AggregateStateLayout,
) -> DirectGroupedAggregateProgram {
    let mut program = DirectGroupedAggregateProgram::new(aggregate_objects.len());
    for (aggregate_index, object) in aggregate_objects.iter().enumerate() {
        let Some(inputs) = aggregate_inputs.get(aggregate_index) else {
            continue;
        };
        if object.is_distinct() || object.filter.is_some() || !object.order_bys.is_empty() {
            continue;
        }
        program.try_add(
            aggregate_index,
            object.function.direct_update,
            state_layout.state_offset(aggregate_index),
            direct_payload_input(object, inputs),
            object.function.state_is_trivially_copyable(),
        );
    }
    program
}

pub(super) fn validate_addresses_vector(addresses: &Vector, row_count: usize) -> Result<()> {
    if addresses.capacity() < row_count {
        return Err(paro_error::internal(format!(
            "Address vector capacity too small: required={row_count}, capacity={}",
            addresses.capacity()
        )));
    }
    Ok(())
}

pub(super) fn validate_filter(filter: &SelectionVector, payload_rows: usize) -> Result<()> {
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

pub(super) fn pointer_vector_from_slice(
    ptrs: &[*mut u8],
    allocator: Arc<dyn Allocator>,
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

pub(super) fn bytes_to_words(bytes: usize) -> Result<usize> {
    let word = size_of::<u64>();
    let words = bytes.checked_add(word - 1).ok_or_else(|| {
        paro_error::internal(format!(
            "Perfect aggregate row storage byte-size overflow: bytes={bytes}"
        ))
    })?;
    Ok(words / word)
}

pub(super) fn direct_update_scratch_bytes(
    program: Option<&DirectGroupedAggregateProgram>,
    slot_count: usize,
) -> Option<usize> {
    let Some(program) = program else {
        return Some(0);
    };
    program
        .scratch_bytes(slot_count)?
        .checked_add(program.materialized_slot_bytes()?)
}

pub(super) fn accounted_vec_from_reservation<T>(
    reservation: &MemoryGrant,
    capacity: usize,
    tag: MemoryTag,
    class: MemoryAccountingClass,
) -> Result<AccountedVec<T>> {
    let bytes = capacity.checked_mul(size_of::<T>()).ok_or_else(|| {
        paro_error::internal("perfect aggregate vector reservation byte-size overflow")
    })?;
    let mut result = AccountedVec::new_with_accounting(reservation.split(bytes)?, tag, class);
    result.try_reserve(capacity)?;
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::direct_update_scratch_bytes;

    #[test]
    fn aggregate_without_direct_updates_requires_no_scratch() {
        assert_eq!(direct_update_scratch_bytes(None, usize::MAX), Some(0));
    }
}
