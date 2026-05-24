// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Shared helper functions used by all aggregate build sinks (hash, ungrouped, perfect hash).

use std::mem::size_of;
use std::sync::Arc;

use paro_common::allocator::{ArenaAllocator, MemoryTag};
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::memory::{MemoryAccountingClass, MemoryAccountingContext, MemoryDomain};
use paro_common::types::LogicalType;
use paro_common::vector::{SelectionVector, Vector};
use paro_function::aggregate::{AggregateCombineType, AggregateInputData};
use paro_function::scalar::FunctionExecContext;
use paro_planner::expression::Expression;

use crate::expression_executor::executor::ExpressionExecutor;
use crate::operators::aggregate::accounted_rows::aggregate_modifier_memory_context;
use crate::operators::aggregate::aggregate_kernel::{
    combine_states, destroy_states, initialize_states,
};
use crate::operators::aggregate::aggregate_object::{create_aggregate_objects, AggregateObject};
use crate::operators::aggregate::aggregate_state::AggregateStateLayout;
use crate::operators::aggregate::grouped_aggregate_data::reference_index;
use crate::operators::aggregate::ordered_helpers::empty_ordered_collectors;
use crate::operators::aggregate::perfect_aggregate_hashtable::PerfectAggregateHashTable;
use crate::operators::aggregate::radix_partitioned_aggregate_hashtable::AggregateHashTable;
use crate::physical::specs::AggregateSpec;
use crate::runtime::breaker::{single_state_addresses, UngroupedAggregateRuntimeState};
use crate::runtime::state::UngroupedAggregateSinkLocal;
use crate::runtime::ExpressionEvalInput;

/// Create aggregate objects from the spec, validating length consistency.
pub(crate) fn aggregate_objects(spec: &AggregateSpec) -> Result<Vec<AggregateObject>> {
    let objects = create_aggregate_objects(&spec.aggregates)?;
    if objects.len() != spec.aggregate_inputs.len()
        || objects.len() != spec.aggregate_filters.len()
        || objects.len() != spec.aggregate_orders.len()
    {
        return Err(paro_error::internal(format!(
            "aggregate descriptor length mismatch: objects={} inputs={} filters={} orders={}",
            objects.len(),
            spec.aggregate_inputs.len(),
            spec.aggregate_filters.len(),
            spec.aggregate_orders.len()
        )));
    }
    for (idx, object) in objects.iter().enumerate() {
        if object.filter != spec.aggregate_filters[idx] {
            return Err(paro_error::internal(format!(
                "aggregate filter mapping mismatch: aggregate_idx={idx} object={:?} spec={:?}",
                object.filter, spec.aggregate_filters[idx]
            )));
        }
        if object.order_bys != spec.aggregate_orders[idx].to_vec() {
            return Err(paro_error::internal(format!(
                "aggregate order mapping mismatch: aggregate_idx={idx} object={:?} spec={:?}",
                object.order_bys, spec.aggregate_orders[idx]
            )));
        }
    }
    Ok(objects)
}

/// Extract query-level modifier memory accounting context.
pub(crate) fn query_modifier_memory(
    query: &crate::runtime::context::QueryRuntimeContext,
) -> MemoryAccountingContext {
    let owner: Arc<dyn paro_common::memory::MemoryOwner> = query.memory.clone();
    aggregate_modifier_memory_context(owner)
}

pub(crate) fn query_hash_table_memory(
    query: &crate::runtime::context::QueryRuntimeContext,
) -> MemoryAccountingContext {
    let owner: Arc<dyn paro_common::memory::MemoryOwner> = query.memory.clone();
    MemoryAccountingContext::from_owner(
        owner,
        MemoryDomain::Host,
        MemoryTag::HashTable,
        MemoryAccountingClass::Revocable,
    )
}

/// Map aggregate inputs to Vec<Vec<usize>>.
pub(crate) fn aggregate_inputs(spec: &AggregateSpec) -> Vec<Vec<usize>> {
    spec.aggregate_inputs
        .iter()
        .map(|inputs| inputs.to_vec())
        .collect()
}

/// Resolve group expression references to payload column indices.
pub(crate) fn group_payload_refs(spec: &AggregateSpec) -> Result<Vec<usize>> {
    spec.groups.iter().map(reference_index).collect()
}

/// Extract logical types from group expressions.
pub(crate) fn group_types(spec: &AggregateSpec) -> Vec<LogicalType> {
    spec.groups.iter().map(Expression::return_type).collect()
}

/// Normalize grouping sets, deduplicating indices within each set.
pub(crate) fn normalized_grouping_sets(spec: &AggregateSpec) -> Result<Vec<Vec<usize>>> {
    if spec.grouping_sets.is_empty() {
        return Ok(vec![(0..spec.grouping_key_count).collect()]);
    }
    let mut normalized = Vec::with_capacity(spec.grouping_sets.len());
    for set in &spec.grouping_sets {
        let mut seen = vec![false; spec.grouping_key_count];
        let mut current = Vec::with_capacity(set.len());
        for &group_idx in set.iter() {
            if group_idx >= spec.grouping_key_count {
                return Err(paro_error::internal(format!(
                    "grouping set index out of bounds: group_idx={group_idx}, group_count={}",
                    spec.grouping_key_count
                )));
            }
            if !seen[group_idx] {
                seen[group_idx] = true;
                current.push(group_idx);
            }
        }
        normalized.push(current);
    }
    Ok(normalized)
}

/// Whether any aggregate has a filter expression.
pub(crate) fn has_aggregate_filters(spec: &AggregateSpec) -> bool {
    spec.aggregate_filters.iter().any(Option::is_some)
}

/// Whether any aggregate uses DISTINCT.
pub(crate) fn has_aggregate_distinct(spec: &AggregateSpec) -> bool {
    spec.aggregates.iter().any(|expr| {
        matches!(
            expr,
            Expression::Aggregate(agg) if agg.aggr_type == paro_planner::expression::AggregateType::Distinct
        )
    })
}

/// Whether any aggregate has an ORDER BY modifier.
pub(crate) fn has_aggregate_ordered(spec: &AggregateSpec) -> bool {
    spec.aggregate_orders
        .iter()
        .any(|orders| !orders.is_empty())
}

/// Build per-aggregate filter selection vectors from the payload chunk.
pub(crate) fn build_per_aggregate_filters(
    spec: &AggregateSpec,
    payload: &Chunk,
) -> Result<Vec<Option<SelectionVector>>> {
    let row_count = payload.size();
    spec.aggregate_filters
        .iter()
        .map(|filter_opt| {
            let Some(&filter_idx) = filter_opt.as_ref() else {
                return Ok(None);
            };
            let filter_vec = payload.column(filter_idx).ok_or_else(|| {
                paro_error::internal(format!(
                    "aggregate filter payload column not found: payload_idx={filter_idx}"
                ))
            })?;
            let format = filter_vec.try_decode_ref(row_count)?;
            let data = format.get_data::<bool>();
            let mut selected = Vec::with_capacity(row_count);
            for row_idx in 0..row_count {
                let physical_idx = format.physical_index(row_idx);
                if !format.validity().is_valid(physical_idx) {
                    continue;
                }
                if unsafe { *data.add(physical_idx) } {
                    selected.push(row_idx as u32);
                }
            }
            Ok(Some(SelectionVector::try_from_indices(
                selected,
                payload.allocator().clone(),
            )?))
        })
        .collect()
}

/// Execute projection expressions and return a reference to the payload chunk.
pub(crate) fn projected_payload_chunk<'a>(
    spec: &AggregateSpec,
    executor: &mut ExpressionExecutor,
    slot: &'a mut Option<Chunk>,
    input: &Chunk,
    query: &crate::runtime::context::QueryRuntimeContext,
) -> Result<&'a Chunk> {
    if slot.is_none() {
        *slot = Some(Chunk::try_initialize(
            &spec.payload_types,
            input.size().max(1),
            query.allocator(MemoryTag::BaseTable),
        )?);
    }
    let payload = slot.as_mut().expect("payload chunk initialized");
    if payload.column_count() != spec.payload_types.len() || payload.capacity() < input.size() {
        *payload = Chunk::try_initialize(
            &spec.payload_types,
            input.size().max(1),
            query.allocator(MemoryTag::BaseTable),
        )?;
    }
    executor.execute_all_into_with_input(
        ExpressionEvalInput {
            params: query.params.as_ref(),
            columns: input,
        },
        query,
        payload,
    )?;
    Ok(payload)
}

/// Ensure address and new_groups scratch vectors are large enough.
pub(crate) fn ensure_group_update_scratch(
    addresses: &mut Vector,
    new_groups: &mut SelectionVector,
    row_count: usize,
    allocator: Arc<dyn paro_common::allocator::Allocator>,
) -> Result<()> {
    let required_capacity = row_count.max(1);
    if addresses.logical_type() != &LogicalType::BigInt || addresses.capacity() < required_capacity
    {
        *addresses = Vector::try_new(LogicalType::BigInt, required_capacity, allocator.clone())?;
    }
    if new_groups.capacity() < required_capacity {
        *new_groups = SelectionVector::try_with_capacity(required_capacity, allocator)?;
    }
    new_groups.set_len(0);
    Ok(())
}

/// Build a chunk containing only the group columns extracted from the payload.
pub(crate) fn build_groups_chunk(payload: &Chunk, group_refs: &[usize]) -> Result<Chunk> {
    let mut vectors = Vec::with_capacity(group_refs.len());
    for (group_idx, &payload_idx) in group_refs.iter().enumerate() {
        vectors.push(Arc::clone(payload.column(payload_idx).ok_or_else(|| {
            paro_error::internal(format!(
                "aggregate group payload column not found: group_idx={group_idx}, payload_idx={payload_idx}"
            ))
        })?));
    }
    let mut groups = Chunk::from_arc_vectors(vectors, payload.allocator().clone());
    groups.try_set_cardinality(payload.size())?;
    Ok(groups)
}

/// Build a groups chunk for a specific grouping set, nulling absent columns.
pub(crate) fn build_groups_chunk_for_set(
    all_groups: &Chunk,
    grouping_set: &[usize],
    group_count: usize,
) -> Result<Chunk> {
    if grouping_set.len() == group_count {
        return Ok(all_groups.clone());
    }
    let mut present = vec![false; group_count];
    for &group_idx in grouping_set {
        if group_idx >= group_count {
            return Err(paro_error::internal(format!(
                "grouping set index out of bounds: group_idx={group_idx}, group_count={group_count}"
            )));
        }
        present[group_idx] = true;
    }
    let mut groups = all_groups.clone();
    for (group_idx, is_present) in present.into_iter().enumerate() {
        if is_present {
            continue;
        }
        let row_count = groups.size();
        let column = groups.column_mut(group_idx).ok_or_else(|| {
            paro_error::internal(format!(
                "missing grouping column while applying grouping set: group_idx={group_idx}"
            ))
        })?;
        column.validity_mut().try_set_all_invalid(row_count)?;
    }
    Ok(groups)
}

/// Calculate how many u64 words are needed for an aggregate state buffer.
pub(crate) fn state_buffer_words(total_size: usize) -> usize {
    total_size.div_ceil(size_of::<u64>()).max(1)
}

/// Initialize aggregate state buffer with starting state values.
pub(crate) fn initialize_state_buffer(
    layout: &AggregateStateLayout,
    aggregate_objects: &[AggregateObject],
    state_buffer: &mut Vec<u64>,
    allocator: Arc<dyn paro_common::allocator::Allocator>,
) -> Result<()> {
    let buffer_bytes = state_buffer
        .len()
        .checked_mul(size_of::<u64>())
        .ok_or_else(|| paro_error::internal("aggregate state buffer size overflow"))?;
    if buffer_bytes < layout.total_size() {
        return Err(paro_error::internal(format!(
            "aggregate state buffer too small: required={} actual={}",
            layout.total_size(),
            buffer_bytes
        )));
    }
    let addresses = single_state_addresses(state_buffer.as_mut_ptr() as *mut u8, allocator)?;
    initialize_states(layout, aggregate_objects, &addresses, 1)
}

/// Fill an address vector with the same pointer repeated `count` times.
pub(crate) fn fill_repeated_state_addresses(
    addresses: &mut Vector,
    base_ptr: *mut u8,
    count: usize,
) -> Result<()> {
    let required_capacity = count.max(1);
    if addresses.logical_type() != &LogicalType::BigInt || addresses.capacity() < required_capacity
    {
        let allocator = addresses.allocator().clone();
        *addresses = Vector::try_new(LogicalType::BigInt, required_capacity, allocator)?;
    }
    addresses.try_set_count(count)?;
    unsafe {
        let ptrs = addresses.flat_data_mut::<*mut u8>();
        for idx in 0..count {
            *ptrs.add(idx) = base_ptr;
        }
    }
    Ok(())
}

/// Combine local ungrouped aggregate states into the global state.
pub(crate) fn combine_ungrouped_states(
    global: &mut UngroupedAggregateRuntimeState,
    local: &mut UngroupedAggregateSinkLocal,
) -> Result<()> {
    let source = single_state_addresses(
        local.state_buffer.as_mut_ptr() as *mut u8,
        local.arena_allocator.get_allocator().clone(),
    )?;
    let target = single_state_addresses(
        global.state_buffer.as_mut_ptr() as *mut u8,
        global.arena_allocator.get_allocator().clone(),
    )?;
    let mut input_data = AggregateInputData::new(
        None,
        &mut global.arena_allocator,
        AggregateCombineType::AllowDestructive,
    );
    combine_states(
        &global.aggregate_objects,
        &mut input_data,
        &source,
        &target,
        1,
    )
}

/// Destroy local ungrouped aggregate states.
pub(crate) fn destroy_ungrouped_local(local: &mut UngroupedAggregateSinkLocal) -> Result<()> {
    if local.destroyed {
        return Ok(());
    }
    let addresses = single_state_addresses(
        local.state_buffer.as_mut_ptr() as *mut u8,
        local.arena_allocator.get_allocator().clone(),
    )?;
    let mut input_data = AggregateInputData::new(
        None,
        &mut local.arena_allocator,
        AggregateCombineType::PreserveInput,
    );
    destroy_states(&local.aggregate_objects, &mut input_data, &addresses, 1)?;
    local.destroyed = true;
    Ok(())
}

/// Create ungrouped aggregate runtime state from spec.
pub(crate) fn create_ungrouped_runtime_state(
    spec: &AggregateSpec,
    allocator: Arc<dyn paro_common::allocator::Allocator>,
) -> Result<UngroupedAggregateRuntimeState> {
    if spec.grouping_key_count != 0 {
        return Err(paro_error::internal(
            "ungrouped aggregate state cannot have group keys",
        ));
    }
    let objects = aggregate_objects(spec)?;
    let layout = AggregateStateLayout::new(&objects)?;
    let ordered_collectors = empty_ordered_collectors(spec, &[]);
    let aggregate_objects = Arc::from(objects.into_boxed_slice());
    let aggregate_inputs = Arc::from(aggregate_inputs(spec).into_boxed_slice());
    let mut state_buffer = vec![0u64; state_buffer_words(layout.total_size())];
    initialize_state_buffer(
        &layout,
        &aggregate_objects,
        &mut state_buffer,
        allocator.clone(),
    )?;
    Ok(UngroupedAggregateRuntimeState {
        aggregate_objects,
        layout,
        aggregate_inputs,
        ordered_collectors,
        state_buffer,
        arena_allocator: ArenaAllocator::new(allocator),
        destroyed: false,
    })
}

/// Create hash aggregate tables (one per grouping set).
pub(crate) fn create_hash_aggregate_tables(
    spec: &AggregateSpec,
    allocator: Arc<dyn paro_common::allocator::Allocator>,
    memory: MemoryAccountingContext,
) -> Result<Vec<AggregateHashTable>> {
    if spec.grouping_key_count == 0 {
        return Err(paro_error::internal(
            "hash aggregate table requires at least one group key",
        ));
    }
    let objects = aggregate_objects(spec)?;
    let inputs = aggregate_inputs(spec);
    let group_types = group_types(spec);
    normalized_grouping_sets(spec)?
        .iter()
        .map(|_| {
            AggregateHashTable::new_flat_with_memory(
                group_types.clone(),
                objects.clone(),
                inputs.clone(),
                allocator.clone(),
                memory.clone(),
            )
        })
        .collect()
}

/// Create a perfect hash aggregate table.
pub(crate) fn create_perfect_aggregate_table(
    spec: &AggregateSpec,
    allocator: Arc<dyn paro_common::allocator::Allocator>,
    memory: MemoryAccountingContext,
) -> Result<PerfectAggregateHashTable> {
    let Some(perfect) = &spec.perfect_hash else {
        return Err(paro_error::internal(
            "perfect aggregate sink requires perfect hash planning metadata",
        ));
    };
    PerfectAggregateHashTable::new_with_memory(
        group_types(spec),
        aggregate_objects(spec)?,
        aggregate_inputs(spec),
        perfect.group_minima.to_vec(),
        perfect.required_bits.to_vec(),
        allocator,
        memory,
    )
}

/// Update hash aggregate tables with new payload rows.
pub(crate) fn update_hash_aggregate_tables(
    spec: &AggregateSpec,
    aggregate_objects: &[AggregateObject],
    payload: &Chunk,
    group_refs: &[usize],
    grouping_sets: &[Box<[usize]>],
    tables: &mut [AggregateHashTable],
    addresses: &mut Vector,
    new_groups: &mut SelectionVector,
) -> Result<()> {
    if grouping_sets.len() != tables.len() {
        return Err(paro_error::internal(format!(
            "hash aggregate grouping table count mismatch: grouping_sets={} tables={}",
            grouping_sets.len(),
            tables.len()
        )));
    }
    let has_distinct = aggregate_objects.iter().any(AggregateObject::is_distinct);
    let has_ordered = aggregate_objects
        .iter()
        .any(|object| !object.order_bys.is_empty());
    let use_per_filter = has_aggregate_filters(spec) || has_distinct || has_ordered;
    let filters = if use_per_filter {
        let mut filters = build_per_aggregate_filters(spec, payload)?;
        if has_distinct || has_ordered {
            for (idx, obj) in aggregate_objects.iter().enumerate() {
                if obj.is_distinct() || !obj.order_bys.is_empty() {
                    filters[idx] = Some(SelectionVector::try_from_indices(
                        vec![],
                        payload.allocator().clone(),
                    )?);
                }
            }
        }
        Some(filters)
    } else {
        None
    };
    let all_groups = build_groups_chunk(payload, group_refs)?;
    for (table, grouping_set) in tables.iter_mut().zip(grouping_sets.iter()) {
        let groups = build_groups_chunk_for_set(
            &all_groups,
            grouping_set.as_ref(),
            spec.grouping_key_count,
        )?;
        let hashes = table.hash_groups(&groups)?;
        ensure_group_update_scratch(
            addresses,
            new_groups,
            payload.size(),
            payload.allocator().clone(),
        )?;
        table.find_or_create_groups(&groups, &hashes, addresses, new_groups)?;
        if let Some(filters) = &filters {
            table.update_aggregates_per_filter(payload, addresses, filters)?;
        } else {
            table.update_aggregates(payload, Some(&hashes), addresses, None)?;
        }
    }
    Ok(())
}

/// Update a perfect hash aggregate table with new payload rows.
pub(crate) fn update_perfect_aggregate_table(
    spec: &AggregateSpec,
    group_refs: &[usize],
    payload: &Chunk,
    table: &mut PerfectAggregateHashTable,
    addresses: &mut Vector,
    new_groups: &mut SelectionVector,
) -> Result<()> {
    let groups = build_groups_chunk(payload, group_refs)?;
    ensure_group_update_scratch(
        addresses,
        new_groups,
        payload.size(),
        payload.allocator().clone(),
    )?;
    table.find_or_create_groups(&groups, addresses, new_groups)?;
    if has_aggregate_filters(spec) {
        let filters = build_per_aggregate_filters(spec, payload)?;
        table.update_aggregates_per_filter(payload, addresses, &filters)
    } else {
        table.update_aggregates(payload, addresses, None)
    }
}
