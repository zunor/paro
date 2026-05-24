// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Shared aggregate kernels for grouped/ungrouped execution paths.

use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_common::vector::{SelectionVector, Vector, VectorSelection};
use paro_function::aggregate::AggregateInputData;

use super::aggregate_object::AggregateObject;
use super::aggregate_state::AggregateStateLayout;

/// Payload chunk + input mapping required by aggregate updates.
pub struct AggregatePayload<'a> {
    /// Extracted payload chunk (already computed by projection/extraction).
    pub chunk: &'a Chunk,
    /// Per-aggregate input column indexes into `chunk`.
    pub aggregate_inputs: &'a [Vec<usize>],
}

/// Initialize aggregate states for a batch of row addresses.
pub fn initialize_states(
    layout: &AggregateStateLayout,
    objects: &[AggregateObject],
    addresses: &Vector,
    count: usize,
) -> Result<()> {
    validate_layout(layout, objects)?;
    let address_format = addresses.try_decode_ref(addresses.len())?;
    let address_data = address_format.get_data::<*mut u8>();
    for row in 0..count {
        let base = base_address(&address_format, address_data, addresses, row)?;
        for (agg_idx, object) in objects.iter().enumerate() {
            let state_ptr = unsafe { base.add(layout.state_offset(agg_idx)) };
            unsafe {
                (object.function.initialize)(state_ptr);
            }
        }
    }
    Ok(())
}

/// Update aggregate states for a batch of row addresses.
pub fn update_states(
    objects: &[AggregateObject],
    input_data: &mut AggregateInputData<'_>,
    payload: &AggregatePayload<'_>,
    addresses: &Vector,
    count: usize,
) -> Result<()> {
    let layout = AggregateStateLayout::new(objects)?;
    validate_payload_mapping(objects, payload)?;
    for (agg_idx, object) in objects.iter().enumerate() {
        let inputs = input_vectors_for_aggregate(payload, agg_idx)?;
        let states = build_state_vector(addresses, &layout, agg_idx, None, count)?;
        with_aggregate_input_data(object, input_data, |aggr_input| unsafe {
            (object.function.update)(&inputs, &aggr_input, &states, count);
        });
    }
    Ok(())
}

/// Update aggregate states for selected rows only.
pub fn update_filtered_states(
    objects: &[AggregateObject],
    input_data: &mut AggregateInputData<'_>,
    payload: &AggregatePayload<'_>,
    addresses: &Vector,
    filter: &SelectionVector,
    count: usize,
) -> Result<()> {
    if count > filter.len() {
        return Err(paro_error::internal(format!(
            "Filtered update count exceeds selection length: count={count}, selection={}",
            filter.len()
        )));
    }
    let layout = AggregateStateLayout::new(objects)?;
    validate_payload_mapping(objects, payload)?;
    for (agg_idx, object) in objects.iter().enumerate() {
        let filtered_inputs =
            filtered_input_vectors_for_aggregate(payload, agg_idx, filter, count)?;
        let filtered_input_refs: Vec<&Vector> = filtered_inputs.iter().collect();
        let states = build_state_vector(addresses, &layout, agg_idx, Some(filter), count)?;
        with_aggregate_input_data(object, input_data, |aggr_input| unsafe {
            (object.function.update)(&filtered_input_refs, &aggr_input, &states, count);
        });
    }
    Ok(())
}

/// Combine source states into target states.
pub fn combine_states(
    objects: &[AggregateObject],
    input_data: &mut AggregateInputData<'_>,
    source_addrs: &Vector,
    target_addrs: &Vector,
    count: usize,
) -> Result<()> {
    let layout = AggregateStateLayout::new(objects)?;
    for (agg_idx, object) in objects.iter().enumerate() {
        let source_states = build_state_vector(source_addrs, &layout, agg_idx, None, count)?;
        let target_states = build_state_vector(target_addrs, &layout, agg_idx, None, count)?;
        with_aggregate_input_data(object, input_data, |aggr_input| unsafe {
            (object.function.combine)(&source_states, &target_states, &aggr_input, count);
        });
    }
    Ok(())
}

/// Finalize aggregate states into result vectors.
pub fn finalize_states(
    objects: &[AggregateObject],
    input_data: &mut AggregateInputData<'_>,
    addresses: &Vector,
    result: &mut Chunk,
    count: usize,
) -> Result<()> {
    let layout = AggregateStateLayout::new(objects)?;
    if result.column_count() < objects.len() {
        return Err(paro_error::internal(format!(
            "Result chunk has insufficient columns: required={}, actual={}",
            objects.len(),
            result.column_count()
        )));
    }
    if result.capacity() < count {
        return Err(paro_error::internal(format!(
            "Result chunk capacity too small: required={count}, actual={}",
            result.capacity()
        )));
    }

    result.try_set_cardinality(count)?;
    for (agg_idx, object) in objects.iter().enumerate() {
        let states = build_state_vector(addresses, &layout, agg_idx, None, count)?;
        let result_vector = result
            .column_mut(agg_idx)
            .expect("result column validated above");
        result_vector.try_set_count(count)?;
        with_aggregate_input_data(object, input_data, |aggr_input| unsafe {
            (object.function.finalize)(&states, &aggr_input, result_vector, count);
        });
    }
    Ok(())
}

/// Destroy aggregate states (for aggregates with explicit destructors).
pub fn destroy_states(
    objects: &[AggregateObject],
    input_data: &mut AggregateInputData<'_>,
    addresses: &Vector,
    count: usize,
) -> Result<()> {
    let layout = AggregateStateLayout::new(objects)?;
    for (agg_idx, object) in objects.iter().enumerate() {
        let Some(destructor) = object.function.destructor else {
            continue;
        };
        let states = build_state_vector(addresses, &layout, agg_idx, None, count)?;
        with_aggregate_input_data(object, input_data, |aggr_input| unsafe {
            destructor(&states, &aggr_input, count);
        });
    }
    Ok(())
}

fn validate_layout(layout: &AggregateStateLayout, objects: &[AggregateObject]) -> Result<()> {
    if layout.aggregate_count() != objects.len() {
        return Err(paro_error::internal(format!(
            "AggregateStateLayout/object count mismatch: layout={} objects={}",
            layout.aggregate_count(),
            objects.len()
        )));
    }
    Ok(())
}

fn validate_payload_mapping(
    objects: &[AggregateObject],
    payload: &AggregatePayload<'_>,
) -> Result<()> {
    if payload.aggregate_inputs.len() != objects.len() {
        return Err(paro_error::internal(format!(
            "Aggregate payload mapping mismatch: objects={} aggregate_inputs={}",
            objects.len(),
            payload.aggregate_inputs.len()
        )));
    }
    for (agg_idx, object) in objects.iter().enumerate() {
        let actual = payload.aggregate_inputs[agg_idx].len();
        if actual != object.child_count {
            return Err(paro_error::internal(format!(
                "Aggregate input arity mismatch at index {agg_idx}: expected={} actual={actual}",
                object.child_count
            )));
        }
    }
    Ok(())
}

fn base_address(
    format: &paro_common::vector::DecodedVectorRef<'_>,
    address_data: *const *mut u8,
    addresses: &Vector,
    row_idx: usize,
) -> Result<*mut u8> {
    if row_idx >= addresses.len() {
        return Err(paro_error::internal(format!(
            "Address index out of bounds: row={row_idx}, len={}",
            addresses.len()
        )));
    }
    let physical_idx = format.physical_index(row_idx);
    if !format.validity().is_valid(physical_idx) {
        return Err(paro_error::internal(format!(
            "Address vector contains NULL at row {row_idx}"
        )));
    }
    let base = unsafe { *address_data.add(physical_idx) };
    if base.is_null() {
        return Err(paro_error::internal(format!(
            "Address vector contains NULL pointer at row {row_idx}"
        )));
    }
    Ok(base)
}

pub(crate) fn build_state_vector(
    addresses: &Vector,
    layout: &AggregateStateLayout,
    agg_idx: usize,
    selection: Option<&SelectionVector>,
    count: usize,
) -> Result<Vector> {
    if agg_idx >= layout.aggregate_count() {
        return Err(paro_error::internal(format!(
            "Aggregate index out of bounds for layout: agg_idx={agg_idx}, count={}",
            layout.aggregate_count()
        )));
    }
    if let Some(sel) = selection {
        if count > sel.len() {
            return Err(paro_error::internal(format!(
                "Selection count exceeds length: count={count}, selection={}",
                sel.len()
            )));
        }
    } else if count > addresses.len() {
        return Err(paro_error::internal(format!(
            "State count exceeds address vector length: count={count}, addresses={}",
            addresses.len()
        )));
    }

    let mut states = Vector::try_new(LogicalType::BigInt, count, addresses.allocator().clone())?;
    states.try_set_count(count)?;
    let state_ptrs = unsafe { states.flat_data_mut::<*mut u8>() };

    let address_format = addresses.try_decode_ref(addresses.len())?;
    let address_data = address_format.get_data::<*mut u8>();
    let state_offset = layout.state_offset(agg_idx);

    for i in 0..count {
        let source_row = selection.map(|sel| sel.get(i)).unwrap_or(i);
        let base = base_address(&address_format, address_data, addresses, source_row)?;
        unsafe {
            *state_ptrs.add(i) = base.add(state_offset);
        }
    }
    Ok(states)
}

pub(crate) fn input_vectors_for_aggregate<'a>(
    payload: &'a AggregatePayload<'_>,
    agg_idx: usize,
) -> Result<Vec<&'a Vector>> {
    let input_indices = payload.aggregate_inputs.get(agg_idx).ok_or_else(|| {
        paro_error::internal(format!(
            "Aggregate payload mapping index out of bounds: agg_idx={agg_idx}"
        ))
    })?;
    let mut inputs = Vec::with_capacity(input_indices.len());
    for &input_idx in input_indices {
        let vector = payload.chunk.column(input_idx).ok_or_else(|| {
            paro_error::internal(format!(
                "Aggregate payload column not found: agg_idx={agg_idx}, column={input_idx}"
            ))
        })?;
        inputs.push(vector.as_ref());
    }
    Ok(inputs)
}

pub(crate) fn filtered_input_vectors_for_aggregate(
    payload: &AggregatePayload<'_>,
    agg_idx: usize,
    filter: &SelectionVector,
    count: usize,
) -> Result<Vec<Vector>> {
    let input_indices = payload.aggregate_inputs.get(agg_idx).ok_or_else(|| {
        paro_error::internal(format!(
            "Aggregate payload mapping index out of bounds: agg_idx={agg_idx}"
        ))
    })?;
    let mut filtered = Vec::with_capacity(input_indices.len());
    for &input_idx in input_indices {
        let vector = payload.chunk.column(input_idx).ok_or_else(|| {
            paro_error::internal(format!(
                "Aggregate payload column not found: agg_idx={agg_idx}, column={input_idx}"
            ))
        })?;
        filtered.push(materialize_filtered_vector(vector, filter, count)?);
    }
    Ok(filtered)
}

fn materialize_filtered_vector(
    source: &Vector,
    filter: &SelectionVector,
    count: usize,
) -> Result<Vector> {
    if count > filter.len() {
        return Err(paro_error::internal(format!(
            "Filtered materialization count exceeds selection length: count={count}, selection={}",
            filter.len()
        )));
    }

    let mut materialized = Vector::try_new(
        source.logical_type().clone(),
        count,
        source.allocator().clone(),
    )?;
    let selection = if count == filter.len() {
        VectorSelection::Materialized(filter.clone())
    } else {
        VectorSelection::Materialized(filter.try_slice_range(0, count)?)
    };
    materialized.try_copy_selection(0, source, &selection, count)?;
    Ok(materialized)
}

pub(crate) fn with_aggregate_input_data<F>(
    object: &AggregateObject,
    input_data: &mut AggregateInputData<'_>,
    f: F,
) where
    F: FnOnce(AggregateInputData<'_>),
{
    let default_bind_data = input_data.bind_data;
    let combine_type = input_data.combine_type;
    let allocator = &mut *input_data.allocator;
    let bind_data = object.bind_info.as_deref().or(default_bind_data);
    let aggr_input = AggregateInputData::new(bind_data, allocator, combine_type);
    f(aggr_input);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::mem::size_of;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use paro_common::allocator::{default_allocator, ArenaAllocator};
    use paro_function::aggregate::{AggregateCombineType, AggregateFunction};
    use paro_planner::expression::{
        AggregateExpression, AggregateType, Expression, ReferenceExpression,
    };

    static DESTRUCTOR_CALLS: AtomicUsize = AtomicUsize::new(0);

    unsafe fn test_initialize(state: *mut u8) {
        *(state as *mut i64) = 0;
    }

    unsafe fn test_update(
        inputs: &[&Vector],
        _input_data: &AggregateInputData,
        states: &Vector,
        count: usize,
    ) {
        let input = inputs[0].try_decode_ref(count).unwrap();
        let state = states.try_decode_ref(count).unwrap();
        let input_data = input.get_data::<i64>();
        let state_data = state.get_data::<*mut u8>();
        for row in 0..count {
            let input_idx = input.sel().get(row);
            if !input.validity().is_valid(input_idx) {
                continue;
            }
            let state_idx = state.sel().get(row);
            let state_ptr = *state_data.add(state_idx) as *mut i64;
            *state_ptr += *input_data.add(input_idx);
        }
    }

    unsafe fn test_combine(
        source: &Vector,
        target: &Vector,
        _input_data: &AggregateInputData,
        count: usize,
    ) {
        let source_fmt = source.try_decode_ref(count).unwrap();
        let target_fmt = target.try_decode_ref(count).unwrap();
        let source_data = source_fmt.get_data::<*mut u8>();
        let target_data = target_fmt.get_data::<*mut u8>();
        for row in 0..count {
            let source_idx = source_fmt.sel().get(row);
            let target_idx = target_fmt.sel().get(row);
            let source_ptr = *source_data.add(source_idx) as *const i64;
            let target_ptr = *target_data.add(target_idx) as *mut i64;
            *target_ptr += *source_ptr;
        }
    }

    unsafe fn test_finalize(
        states: &Vector,
        _input_data: &AggregateInputData,
        result: &mut Vector,
        count: usize,
    ) {
        let state = states.try_decode_ref(count).unwrap();
        let state_data = state.get_data::<*mut u8>();
        let result_data = result.flat_data_mut::<i64>();
        for row in 0..count {
            let state_idx = state.sel().get(row);
            let state_ptr = *state_data.add(state_idx) as *const i64;
            *result_data.add(row) = *state_ptr;
        }
    }

    unsafe fn test_destructor(_states: &Vector, _input_data: &AggregateInputData, count: usize) {
        DESTRUCTOR_CALLS.fetch_add(count, Ordering::Relaxed);
    }

    fn make_sum_object() -> AggregateObject {
        let function = AggregateFunction::new(
            "test_sum".to_string(),
            vec![LogicalType::BigInt],
            LogicalType::BigInt,
            size_of::<i64>(),
            test_initialize,
            test_update,
            test_combine,
            test_finalize,
            None,
            Some(test_destructor),
        );
        let bound = AggregateExpression::new(
            function,
            vec![Expression::Reference(ReferenceExpression::new(
                0,
                LogicalType::BigInt,
            ))],
            LogicalType::BigInt,
        )
        .with_aggr_type(AggregateType::NonDistinct);
        AggregateObject::from_bound(&bound).expect("aggregate object")
    }

    fn make_address_vector(rows: &mut [Vec<u8>]) -> Vector {
        let mut addresses =
            paro_common::test_utils::test_vector_with_capacity(LogicalType::BigInt, rows.len());
        addresses.set_count(rows.len());
        unsafe {
            let address_data = addresses.flat_data_mut::<*mut u8>();
            for (row_idx, row) in rows.iter_mut().enumerate() {
                *address_data.add(row_idx) = row.as_mut_ptr();
            }
        }
        addresses
    }

    fn read_state_i64(
        rows: &[Vec<u8>],
        layout: &AggregateStateLayout,
        row_idx: usize,
        agg_idx: usize,
    ) -> i64 {
        unsafe { *((rows[row_idx].as_ptr().add(layout.state_offset(agg_idx))) as *const i64) }
    }

    #[test]
    fn kernel_roundtrip_initialize_update_combine_finalize_destroy() {
        DESTRUCTOR_CALLS.store(0, Ordering::Relaxed);

        let objects = vec![make_sum_object()];
        let layout = AggregateStateLayout::new(&objects).expect("layout");

        let mut source_rows = vec![vec![0u8; layout.total_size()]];
        let mut target_rows = vec![vec![0u8; layout.total_size()]];
        let source_addresses = make_address_vector(&mut source_rows);
        let target_addresses = make_address_vector(&mut target_rows);

        initialize_states(&layout, &objects, &source_addresses, 1).expect("init source");
        initialize_states(&layout, &objects, &target_addresses, 1).expect("init target");
        assert_eq!(read_state_i64(&source_rows, &layout, 0, 0), 0);
        assert_eq!(read_state_i64(&target_rows, &layout, 0, 0), 0);

        let source_payload_chunk = Chunk::from_vectors(
            vec![paro_common::test_utils::test_i64_vector_with_allocator(
                &[3],
                paro_common::test_utils::test_allocator(),
            )],
            paro_common::test_utils::test_allocator(),
        );
        let source_inputs = vec![vec![0]];
        let source_payload = AggregatePayload {
            chunk: &source_payload_chunk,
            aggregate_inputs: &source_inputs,
        };
        let target_payload_chunk = Chunk::from_vectors(
            vec![paro_common::test_utils::test_i64_vector_with_allocator(
                &[7],
                paro_common::test_utils::test_allocator(),
            )],
            paro_common::test_utils::test_allocator(),
        );
        let target_inputs = vec![vec![0]];
        let target_payload = AggregatePayload {
            chunk: &target_payload_chunk,
            aggregate_inputs: &target_inputs,
        };

        let mut arena = ArenaAllocator::new(Arc::new(default_allocator()));
        let mut preserve_input =
            AggregateInputData::new(None, &mut arena, AggregateCombineType::PreserveInput);

        update_states(
            &objects,
            &mut preserve_input,
            &source_payload,
            &source_addresses,
            1,
        )
        .expect("update source");
        update_states(
            &objects,
            &mut preserve_input,
            &target_payload,
            &target_addresses,
            1,
        )
        .expect("update target");
        assert_eq!(read_state_i64(&source_rows, &layout, 0, 0), 3);
        assert_eq!(read_state_i64(&target_rows, &layout, 0, 0), 7);

        let mut combine_input =
            AggregateInputData::new(None, &mut arena, AggregateCombineType::AllowDestructive);
        combine_states(
            &objects,
            &mut combine_input,
            &source_addresses,
            &target_addresses,
            1,
        )
        .expect("combine");
        assert_eq!(read_state_i64(&target_rows, &layout, 0, 0), 10);

        let mut result =
            paro_common::test_utils::test_chunk_with_capacity(&[LogicalType::BigInt], 1);
        let mut finalize_input =
            AggregateInputData::new(None, &mut arena, AggregateCombineType::PreserveInput);
        finalize_states(
            &objects,
            &mut finalize_input,
            &target_addresses,
            &mut result,
            1,
        )
        .expect("finalize");
        assert_eq!(result.size(), 1);
        assert_eq!(
            result
                .column(0)
                .expect("result column")
                .get_i64(0)
                .expect("result value"),
            10
        );

        let mut destroy_input =
            AggregateInputData::new(None, &mut arena, AggregateCombineType::PreserveInput);
        destroy_states(&objects, &mut destroy_input, &source_addresses, 1).expect("destroy source");
        destroy_states(&objects, &mut destroy_input, &target_addresses, 1).expect("destroy target");
        assert_eq!(DESTRUCTOR_CALLS.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn kernel_filtered_update_respects_selection() {
        let objects = vec![make_sum_object()];
        let layout = AggregateStateLayout::new(&objects).expect("layout");

        let mut rows = vec![vec![0u8; layout.total_size()]; 3];
        let addresses = make_address_vector(&mut rows);
        initialize_states(&layout, &objects, &addresses, 3).expect("initialize");

        let payload_chunk = Chunk::from_vectors(
            vec![paro_common::test_utils::test_i64_vector_with_allocator(
                &[10, 20, 30],
                paro_common::test_utils::test_allocator(),
            )],
            paro_common::test_utils::test_allocator(),
        );
        let aggregate_inputs = vec![vec![0]];
        let payload = AggregatePayload {
            chunk: &payload_chunk,
            aggregate_inputs: &aggregate_inputs,
        };
        let filter = paro_common::test_utils::test_selection(vec![1, 2]);

        let mut arena = ArenaAllocator::new(Arc::new(default_allocator()));
        let mut input =
            AggregateInputData::new(None, &mut arena, AggregateCombineType::PreserveInput);
        update_filtered_states(
            &objects,
            &mut input,
            &payload,
            &addresses,
            &filter,
            filter.len(),
        )
        .expect("filtered update");

        let mut result =
            paro_common::test_utils::test_chunk_with_capacity(&[LogicalType::BigInt], 3);
        finalize_states(&objects, &mut input, &addresses, &mut result, 3).expect("finalize");
        assert_eq!(result.column(0).expect("result col").get_i64(0), Some(0));
        assert_eq!(result.column(0).expect("result col").get_i64(1), Some(20));
        assert_eq!(result.column(0).expect("result col").get_i64(2), Some(30));
    }
}
