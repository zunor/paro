// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! DISTINCT aggregate helpers shared by hash and ungrouped build sinks.

use std::sync::Arc;

use paro_common::allocator::ArenaAllocator;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::memory::{
    AccountedHashSet, MemoryAccountingClass, MemoryAccountingContext, PrecomputedHashBuildHasher,
};
use paro_common::types::LogicalType;
use paro_common::vector::{SelectionVector, Vector, VECTOR_SIZE};
use paro_function::aggregate::{AggregateCombineType, AggregateInputData};
use paro_storage::buffer::BufferPool;
use paro_storage::row::{Ordering, PinnedRows};

use crate::operators::aggregate::accounted_rows::{
    AccountedDistinctKey, DistinctRowSet, DistinctRows,
};
use crate::operators::aggregate::aggregate_kernel::{
    build_state_vector, update_states, AggregatePayload,
};
use crate::operators::aggregate::aggregate_object::AggregateObject;
use crate::operators::aggregate::aggregate_state::AggregateStateLayout;
use crate::operators::aggregate::build_helpers::{
    build_per_aggregate_filters, ensure_group_update_scratch, fill_repeated_state_addresses,
    group_types, has_aggregate_filters,
};
use crate::operators::aggregate::radix_partitioned_aggregate_hashtable::AggregateHashTable;
use crate::physical::specs::AggregateSpec;
use crate::runtime::state::UngroupedAggregateSinkLocal;

/// Collect distinct rows from the payload into per-aggregate row sets.
pub(crate) fn collect_distinct_rows(
    spec: &AggregateSpec,
    aggregate_objects: &[AggregateObject],
    payload: &Chunk,
    group_refs: &[usize],
    buffer_pool: &Arc<BufferPool>,
    modifier_memory: &MemoryAccountingContext,
    distinct_sets: &mut [Option<DistinctRowSet>],
) -> Result<()> {
    let row_count = payload.size();
    let filter_selections = if has_aggregate_filters(spec) {
        Some(build_per_aggregate_filters(spec, payload)?)
    } else {
        None
    };
    for (agg_idx, object) in aggregate_objects.iter().enumerate() {
        if !object.is_distinct() || !object.order_bys.is_empty() {
            continue;
        }
        let input_refs = &spec.aggregate_inputs[agg_idx];
        let row_refs = distinct_row_refs(group_refs, input_refs);
        validate_distinct_row_refs(payload, &row_refs, agg_idx)?;
        let row_types = distinct_row_types(spec, &row_refs, agg_idx)?;
        if distinct_sets[agg_idx].is_none() {
            distinct_sets[agg_idx] = Some(DistinctRowSet::new(
                Arc::clone(buffer_pool),
                row_types,
                modifier_memory.clone(),
            ));
        }
        let seen = distinct_sets[agg_idx]
            .as_mut()
            .expect("distinct set initialized");
        let mut projected_payload =
            Chunk::try_init_empty(seen.row_types(), payload.allocator().clone())?;
        projected_payload.reference_columns(payload, &row_refs);
        let filter = filter_selections.as_ref().and_then(|f| f[agg_idx].as_ref());
        let mut selection =
            SelectionVector::try_with_capacity(row_count, payload.allocator().clone())?;
        selection.set_len(row_count);
        let mut selected_count = 0usize;
        let mut key_scratch = Vec::new();
        let mut insert_row = |row_idx: usize| -> Result<()> {
            if seen.try_insert_key_from_chunk(&projected_payload, row_idx, &mut key_scratch)? {
                selection.try_set(selected_count, row_idx)?;
                selected_count += 1;
            }
            Ok(())
        };
        if let Some(sel) = filter {
            for i in 0..sel.len() {
                insert_row(sel.get(i))?;
            }
        } else {
            for row_idx in 0..row_count {
                insert_row(row_idx)?;
            }
        }
        selection.set_len(selected_count);
        seen.append_selected_rows(&projected_payload, &selection, selected_count)?;
    }
    Ok(())
}

fn distinct_row_refs(group_refs: &[usize], input_refs: &[usize]) -> Vec<usize> {
    group_refs
        .iter()
        .chain(input_refs.iter())
        .copied()
        .collect()
}

fn validate_distinct_row_refs(payload: &Chunk, row_refs: &[usize], agg_idx: usize) -> Result<()> {
    for &col_idx in row_refs {
        if col_idx >= payload.column_count() {
            return Err(paro_error::internal(format!(
                "distinct payload column not found for aggregate {agg_idx}: idx={col_idx}, column_count={}",
                payload.column_count()
            )));
        }
    }
    Ok(())
}

fn distinct_row_types(
    spec: &AggregateSpec,
    row_refs: &[usize],
    agg_idx: usize,
) -> Result<Vec<LogicalType>> {
    row_refs
        .iter()
        .map(|&idx| {
            spec.payload_types.get(idx).cloned().ok_or_else(|| {
                paro_error::internal(format!(
                    "distinct payload type not found for aggregate {agg_idx}: idx={idx}, type_count={}",
                    spec.payload_types.len()
                ))
            })
        })
        .collect()
}

/// Build a bitmask indicating which groups are present in a grouping set.
pub(crate) fn grouping_set_present_mask(
    group_count: usize,
    grouping_set: &[usize],
    agg_idx: usize,
) -> Result<Vec<bool>> {
    let mut present = vec![false; group_count];
    for &group_idx in grouping_set {
        if group_idx >= group_count {
            return Err(paro_error::internal(format!(
                "grouping set index out of bounds while finalizing DISTINCT aggregate {agg_idx}: group_idx={group_idx}, group_count={group_count}"
            )));
        }
        present[group_idx] = true;
    }
    Ok(present)
}

/// Allocate an accounted hash set for projected distinct deduplication.
pub(crate) fn new_projected_distinct_set(
    modifier_memory: &MemoryAccountingContext,
    capacity: usize,
    agg_idx: usize,
) -> Result<AccountedHashSet<AccountedDistinctKey, PrecomputedHashBuildHasher>> {
    let metadata_memory = modifier_memory.with_class(MemoryAccountingClass::Metadata);
    let mut seen = AccountedHashSet::new_with_accounting_and_hasher(
        metadata_memory
            .grant()
            .map_err(|e| paro_error::out_of_memory(format!("distinct aggregate {agg_idx}: {e}")))?,
        metadata_memory.tag(),
        metadata_memory.accounting_class(),
        PrecomputedHashBuildHasher,
    );
    seen.try_reserve(capacity)
        .map_err(|e| paro_error::out_of_memory(format!("distinct aggregate {agg_idx}: {e}")))?;
    Ok(seen)
}

/// Populate an input chunk from pinned distinct rows.
pub(crate) fn populate_distinct_input_chunk(
    chunk: &mut Chunk,
    pinned: &PinnedRows<'_>,
    output_positions: &[u32],
    value_offset: usize,
    input_count: usize,
    agg_idx: usize,
) -> Result<()> {
    if chunk.column_count() < input_count {
        return Err(paro_error::internal(format!(
            "missing input columns while finalizing DISTINCT aggregate {agg_idx}: required={input_count}, actual={}",
            chunk.column_count()
        )));
    }
    let projections = (0..input_count)
        .map(|input_idx| (value_offset + input_idx, input_idx))
        .collect::<Vec<_>>();
    if projections.is_empty() {
        chunk.try_set_cardinality(pinned.len())?;
        return Ok(());
    }
    pinned.gather_columns_projected(&projections, chunk, output_positions)?;
    Ok(())
}

/// Populate a group chunk from pinned distinct rows, applying present mask.
pub(crate) fn populate_distinct_group_chunk(
    groups: &mut Chunk,
    pinned: &PinnedRows<'_>,
    output_positions: &[u32],
    present_groups: &[bool],
    agg_idx: usize,
) -> Result<()> {
    if groups.column_count() < present_groups.len() {
        return Err(paro_error::internal(format!(
            "missing group columns while finalizing DISTINCT aggregate {agg_idx}: required={}, actual={}",
            present_groups.len(),
            groups.column_count()
        )));
    }
    groups.try_set_cardinality(pinned.len())?;
    let projections = present_groups
        .iter()
        .copied()
        .enumerate()
        .filter_map(|(group_idx, is_present)| is_present.then_some((group_idx, group_idx)))
        .collect::<Vec<_>>();
    if !projections.is_empty() {
        pinned.gather_columns_projected(&projections, groups, output_positions)?;
    }
    for (group_idx, is_present) in present_groups.iter().copied().enumerate() {
        if !is_present {
            let col = groups.column_mut(group_idx).ok_or_else(|| {
                paro_error::internal(format!(
                    "missing group column {group_idx} while finalizing DISTINCT aggregate {agg_idx}"
                ))
            })?;
            col.validity_mut().try_set_all_invalid(pinned.len())?;
        }
    }
    Ok(())
}

fn reset_identity_output_positions(positions: &mut Vec<u32>, count: usize) -> Result<()> {
    let count_u32 = u32::try_from(count).map_err(|_| {
        paro_error::internal(format!(
            "distinct batch too large for output positions: count={count}"
        ))
    })?;
    positions.clear();
    positions
        .try_reserve(count)
        .map_err(|_| paro_error::out_of_memory("distinct output position allocation failed"))?;
    positions.extend(0..count_u32);
    Ok(())
}

fn projected_distinct_key_types(
    group_types: &[LogicalType],
    input_types: &[LogicalType],
    present_groups: &[bool],
) -> Vec<LogicalType> {
    let mut types = Vec::with_capacity(
        present_groups.iter().filter(|&&present| present).count() + input_types.len(),
    );
    for (group_idx, is_present) in present_groups.iter().copied().enumerate() {
        if is_present {
            types.push(group_types[group_idx].clone());
        }
    }
    types.extend(input_types.iter().cloned());
    types
}

fn projected_distinct_key_projections(
    group_count: usize,
    input_count: usize,
    present_groups: &[bool],
) -> Vec<(usize, usize)> {
    let mut output_idx = 0usize;
    let mut projections =
        Vec::with_capacity(present_groups.iter().filter(|&&present| present).count() + input_count);
    for (group_idx, is_present) in present_groups.iter().copied().enumerate() {
        if is_present {
            projections.push((group_idx, output_idx));
            output_idx += 1;
        }
    }
    for input_idx in 0..input_count {
        projections.push((group_count + input_idx, output_idx));
        output_idx += 1;
    }
    projections
}

/// Finalize distinct rows into hash aggregate tables after all input is consumed.
pub(crate) fn finalize_distinct_into_tables(
    spec: &AggregateSpec,
    aggregate_objects: &[AggregateObject],
    group_refs: &[usize],
    grouping_sets: &[Box<[usize]>],
    modifier_memory: &MemoryAccountingContext,
    distinct_sets: &mut [Option<DistinctRowSet>],
    tables: &mut [AggregateHashTable],
) -> Result<()> {
    let group_count = group_refs.len();
    let group_types = group_types(spec);
    if grouping_sets.len() != tables.len() {
        return Err(paro_error::internal(format!(
            "hash aggregate grouping table count mismatch while finalizing DISTINCT: grouping_sets={} tables={}",
            grouping_sets.len(),
            tables.len()
        )));
    }
    for (agg_idx, object) in aggregate_objects.iter().enumerate() {
        if !object.is_distinct() || !object.order_bys.is_empty() {
            continue;
        }
        let rows = match distinct_sets[agg_idx].take() {
            Some(set) => set.into_rows()?,
            None => continue,
        };
        if rows.is_empty() {
            continue;
        }
        let input_refs = &spec.aggregate_inputs[agg_idx];
        let input_count = input_refs.len();
        let expected_len = group_count + input_count;
        if rows.row_width() != expected_len {
            return Err(paro_error::internal(format!(
                "distinct row width mismatch at aggregate {agg_idx}: expected={expected_len}, actual={}",
                rows.row_width()
            )));
        }
        let allocator = tables
            .first()
            .expect("hash aggregate has at least one table")
            .allocator();
        let input_types: Vec<LogicalType> = input_refs
            .iter()
            .filter_map(|&idx| spec.payload_types.get(idx))
            .cloned()
            .collect();
        let full_layout = AggregateStateLayout::new(aggregate_objects)?;
        let single_input = vec![(0..input_count).collect::<Vec<usize>>()];
        for (table, grouping_set) in tables.iter_mut().zip(grouping_sets.iter()) {
            let present_groups = grouping_set_present_mask(group_count, grouping_set, agg_idx)?;
            let table_allocator = table.allocator();
            let capacity = rows.len().min(VECTOR_SIZE).max(1);
            let mut updater = DistinctTableBatchUpdater {
                table,
                object,
                full_layout: &full_layout,
                single_input: &single_input,
                present_groups: &present_groups,
                group_count,
                input_count,
                agg_idx,
                groups: Chunk::try_initialize(&group_types, capacity, allocator.clone())?,
                input_chunk: Chunk::try_initialize(&input_types, capacity, allocator.clone())?,
                addresses: Vector::try_new(LogicalType::BigInt, capacity, allocator.clone())?,
                new_groups: SelectionVector::try_with_capacity(capacity, allocator.clone())?,
                arena: ArenaAllocator::new(table_allocator),
                allocator: allocator.clone(),
                output_positions: Vec::new(),
            };
            let needs_projected_dedup = present_groups.iter().any(|present| !present);
            if !needs_projected_dedup {
                for batch in rows.ordinals().chunks(VECTOR_SIZE) {
                    updater.flush(&rows, batch)?;
                }
                continue;
            }

            let mut projected_seen =
                new_projected_distinct_set(modifier_memory, rows.len(), agg_idx)?;
            let projected_key_memory = modifier_memory.with_class(MemoryAccountingClass::Metadata);
            let projected_key_types =
                projected_distinct_key_types(&group_types, &input_types, &present_groups);
            let projected_key_projections =
                projected_distinct_key_projections(group_count, input_count, &present_groups);
            let mut projected_key_chunk =
                Chunk::try_initialize(&projected_key_types, capacity, allocator.clone())?;
            let mut projected_positions = Vec::new();
            let mut key_scratch = Vec::new();
            let mut batch = Vec::with_capacity(rows.len().min(VECTOR_SIZE));
            for candidate_ordinals in rows.ordinals().chunks(VECTOR_SIZE) {
                reset_identity_output_positions(
                    &mut projected_positions,
                    candidate_ordinals.len(),
                )?;
                let pinned = rows.pin_ordinals(candidate_ordinals, Ordering::Sequential)?;
                if projected_key_projections.is_empty() {
                    projected_key_chunk.try_set_cardinality(candidate_ordinals.len())?;
                } else {
                    pinned.gather_columns_projected(
                        &projected_key_projections,
                        &mut projected_key_chunk,
                        &projected_positions,
                    )?;
                }
                for (row_idx, &ordinal) in candidate_ordinals.iter().enumerate() {
                    let key = AccountedDistinctKey::from_chunk_row(
                        &projected_key_memory,
                        &projected_key_chunk,
                        row_idx,
                        &mut key_scratch,
                    )?;
                    let inserted = projected_seen.try_insert(key).map_err(|e| {
                        paro_error::out_of_memory(format!("distinct aggregate {agg_idx}: {e}"))
                    })?;
                    if !inserted {
                        continue;
                    }
                    batch.push(ordinal);
                    if batch.len() < VECTOR_SIZE {
                        continue;
                    }
                    updater.flush(&rows, &batch)?;
                    batch.clear();
                }
            }
            if !batch.is_empty() {
                updater.flush(&rows, &batch)?;
            }
        }
    }
    Ok(())
}

/// Batch updater for flushing distinct rows into an aggregate hash table.
pub(crate) struct DistinctTableBatchUpdater<'a> {
    table: &'a mut AggregateHashTable,
    object: &'a AggregateObject,
    full_layout: &'a AggregateStateLayout,
    single_input: &'a [Vec<usize>],
    present_groups: &'a [bool],
    group_count: usize,
    input_count: usize,
    agg_idx: usize,
    groups: Chunk,
    input_chunk: Chunk,
    addresses: Vector,
    new_groups: SelectionVector,
    arena: ArenaAllocator,
    allocator: Arc<dyn paro_common::allocator::Allocator>,
    output_positions: Vec<u32>,
}

impl DistinctTableBatchUpdater<'_> {
    pub(crate) fn flush(&mut self, rows: &DistinctRows, batch: &[u64]) -> Result<()> {
        let pinned = rows.pin_ordinals(batch, Ordering::Sequential)?;
        reset_identity_output_positions(&mut self.output_positions, batch.len())?;
        populate_distinct_group_chunk(
            &mut self.groups,
            &pinned,
            &self.output_positions,
            self.present_groups,
            self.agg_idx,
        )?;
        populate_distinct_input_chunk(
            &mut self.input_chunk,
            &pinned,
            &self.output_positions,
            self.group_count,
            self.input_count,
            self.agg_idx,
        )?;
        let hashes = self.table.hash_groups(&self.groups)?;
        ensure_group_update_scratch(
            &mut self.addresses,
            &mut self.new_groups,
            batch.len(),
            self.allocator.clone(),
        )?;
        self.table.find_or_create_groups(
            &self.groups,
            &hashes,
            &mut self.addresses,
            &mut self.new_groups,
        )?;
        let payload_desc = AggregatePayload {
            chunk: &self.input_chunk,
            aggregate_inputs: self.single_input,
        };
        let states = build_state_vector(
            &self.addresses,
            self.full_layout,
            self.agg_idx,
            None,
            batch.len(),
        )?;
        let mut input_data = AggregateInputData::new(
            self.object.bind_info.as_deref(),
            &mut self.arena,
            AggregateCombineType::PreserveInput,
        );
        update_states(
            std::slice::from_ref(self.object),
            &mut input_data,
            &payload_desc,
            &states,
            batch.len(),
        )
    }
}

/// Finalize ungrouped DISTINCT aggregates from collected rows.
pub(crate) fn finalize_ungrouped_distinct(
    spec: &AggregateSpec,
    local: &mut UngroupedAggregateSinkLocal,
) -> Result<()> {
    for (agg_idx, object) in local.aggregate_objects.iter().enumerate() {
        if !object.is_distinct() || !object.order_bys.is_empty() {
            continue;
        }
        let rows = match local.distinct_sets[agg_idx].take() {
            Some(set) => set.into_rows()?,
            None => continue,
        };
        if rows.is_empty() {
            continue;
        }
        let input_refs = &spec.aggregate_inputs[agg_idx];
        let input_count = input_refs.len();
        let input_types: Vec<LogicalType> = input_refs
            .iter()
            .filter_map(|&idx| spec.payload_types.get(idx))
            .cloned()
            .collect();
        let allocator = local.arena_allocator.get_allocator().clone();
        let state_offset = local.layout.state_offset(agg_idx);
        let agg_ptr = unsafe { (local.state_buffer.as_mut_ptr() as *mut u8).add(state_offset) };
        let single_input = vec![(0..input_count).collect::<Vec<usize>>()];
        let mut input_chunk = Chunk::try_initialize(
            &input_types,
            rows.len().min(VECTOR_SIZE).max(1),
            allocator.clone(),
        )?;
        let mut output_positions = Vec::new();
        for batch in rows.ordinals().chunks(VECTOR_SIZE) {
            let pinned = rows.pin_ordinals(batch, Ordering::Sequential)?;
            reset_identity_output_positions(&mut output_positions, batch.len())?;
            populate_distinct_input_chunk(
                &mut input_chunk,
                &pinned,
                &output_positions,
                0,
                input_count,
                agg_idx,
            )?;
            fill_repeated_state_addresses(&mut local.addresses, agg_ptr, batch.len())?;
            let payload_desc = AggregatePayload {
                chunk: &input_chunk,
                aggregate_inputs: &single_input,
            };
            let mut input_data = AggregateInputData::new(
                object.bind_info.as_deref(),
                &mut local.arena_allocator,
                AggregateCombineType::PreserveInput,
            );
            update_states(
                std::slice::from_ref(object),
                &mut input_data,
                &payload_desc,
                &local.addresses,
                batch.len(),
            )?;
        }
    }
    Ok(())
}
