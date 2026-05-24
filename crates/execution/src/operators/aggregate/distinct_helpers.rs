// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! DISTINCT aggregate helpers shared by hash and ungrouped build sinks.

use std::hash::{Hash, Hasher};
use std::sync::Arc;

use paro_common::allocator::ArenaAllocator;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::memory::{AccountedHashSet, MemoryAccountingClass, MemoryAccountingContext};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_common::vector::{SelectionVector, Vector, VECTOR_SIZE};
use paro_function::aggregate::{AggregateCombineType, AggregateInputData};

use crate::operators::aggregate::accounted_rows::{AccountedValueRow, AccountedValueRowSet};
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
    modifier_memory: &MemoryAccountingContext,
    distinct_sets: &mut [Option<AccountedValueRowSet>],
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
        let seen = distinct_sets[agg_idx]
            .get_or_insert_with(|| AccountedValueRowSet::new(modifier_memory.clone()));
        let filter = filter_selections.as_ref().and_then(|f| f[agg_idx].as_ref());
        let mut insert_row = |row_idx: usize| -> Result<()> {
            let mut key = Vec::with_capacity(group_refs.len() + input_refs.len());
            for &col_idx in group_refs.iter().chain(input_refs.iter()) {
                let col = payload.column(col_idx).ok_or_else(|| {
                    paro_error::internal(format!(
                        "distinct payload column not found: idx={col_idx}"
                    ))
                })?;
                key.push(col.get_value(row_idx));
            }
            seen.insert(key)
                .map_err(|e| paro_error::out_of_memory(format!("distinct aggregate: {e}")))?;
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
    }
    Ok(())
}

/// Trait for accessing values from a distinct row.
pub(crate) trait DistinctRowValues {
    fn values(&self) -> &[Value];
}

impl DistinctRowValues for AccountedValueRow {
    fn values(&self) -> &[Value] {
        &self[..]
    }
}

impl DistinctRowValues for &AccountedValueRow {
    fn values(&self) -> &[Value] {
        &(*self)[..]
    }
}

/// A projected view of a distinct row for grouping-set deduplication.
#[derive(Clone, Copy)]
pub(crate) struct ProjectedDistinctRow<'a> {
    pub row: &'a AccountedValueRow,
    pub present_groups: &'a [bool],
    pub group_count: usize,
}

impl PartialEq for ProjectedDistinctRow<'_> {
    fn eq(&self, other: &Self) -> bool {
        if self.group_count != other.group_count
            || self.present_groups != other.present_groups
            || self.row.len().saturating_sub(self.group_count)
                != other.row.len().saturating_sub(other.group_count)
        {
            return false;
        }
        for group_idx in 0..self.group_count {
            if self.present_groups[group_idx] && self.row[group_idx] != other.row[group_idx] {
                return false;
            }
        }
        self.row[self.group_count..] == other.row[other.group_count..]
    }
}

impl Eq for ProjectedDistinctRow<'_> {}

impl Hash for ProjectedDistinctRow<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.group_count.hash(state);
        self.present_groups.hash(state);
        for group_idx in 0..self.group_count {
            if self.present_groups[group_idx] {
                self.row[group_idx].hash(state);
            }
        }
        self.row[self.group_count..].hash(state);
    }
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
pub(crate) fn new_projected_distinct_set<'a>(
    modifier_memory: &MemoryAccountingContext,
    capacity: usize,
    agg_idx: usize,
) -> Result<AccountedHashSet<ProjectedDistinctRow<'a>>> {
    let metadata_memory = modifier_memory.with_class(MemoryAccountingClass::Metadata);
    let mut seen = AccountedHashSet::new_with_accounting(
        metadata_memory
            .grant()
            .map_err(|e| paro_error::out_of_memory(format!("distinct aggregate {agg_idx}: {e}")))?,
        metadata_memory.tag(),
        metadata_memory.accounting_class(),
    );
    seen.try_reserve(capacity)
        .map_err(|e| paro_error::out_of_memory(format!("distinct aggregate {agg_idx}: {e}")))?;
    Ok(seen)
}

/// Populate an input chunk with values from distinct rows.
pub(crate) fn populate_distinct_input_chunk<R: DistinctRowValues>(
    chunk: &mut Chunk,
    rows: &[R],
    value_offset: usize,
    input_count: usize,
    agg_idx: usize,
) -> Result<()> {
    chunk.try_set_cardinality(rows.len())?;
    for input_idx in 0..input_count {
        let col = chunk.column_mut(input_idx).ok_or_else(|| {
            paro_error::internal(format!(
                "missing input column {input_idx} while finalizing DISTINCT aggregate {agg_idx}"
            ))
        })?;
        for (row_idx, row) in rows.iter().enumerate() {
            col.set_value(row_idx, &row.values()[value_offset + input_idx]);
        }
    }
    Ok(())
}

/// Populate a group chunk with values from distinct rows, applying present mask.
pub(crate) fn populate_distinct_group_chunk<R: DistinctRowValues>(
    groups: &mut Chunk,
    rows: &[R],
    present_groups: &[bool],
    agg_idx: usize,
) -> Result<()> {
    groups.try_set_cardinality(rows.len())?;
    for (group_idx, is_present) in present_groups.iter().copied().enumerate() {
        let col = groups.column_mut(group_idx).ok_or_else(|| {
            paro_error::internal(format!(
                "missing group column {group_idx} while finalizing DISTINCT aggregate {agg_idx}"
            ))
        })?;
        if is_present {
            for (row_idx, row) in rows.iter().enumerate() {
                col.set_value(row_idx, &row.values()[group_idx]);
            }
        } else {
            col.validity_mut().try_set_all_invalid(rows.len())?;
        }
    }
    Ok(())
}

/// Finalize distinct rows into hash aggregate tables after all input is consumed.
pub(crate) fn finalize_distinct_into_tables(
    spec: &AggregateSpec,
    aggregate_objects: &[AggregateObject],
    group_refs: &[usize],
    grouping_sets: &[Box<[usize]>],
    modifier_memory: &MemoryAccountingContext,
    distinct_sets: &mut [Option<AccountedValueRowSet>],
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
            Some(set) => set.into_rows(),
            None => continue,
        };
        if rows.is_empty() {
            continue;
        }
        let input_refs = &spec.aggregate_inputs[agg_idx];
        let input_count = input_refs.len();
        let expected_len = group_count + input_count;
        for row in &rows {
            if row.len() != expected_len {
                return Err(paro_error::internal(format!(
                    "distinct row width mismatch at aggregate {agg_idx}: expected={expected_len}, actual={}",
                    row.len()
                )));
            }
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
            };
            let mut batch = Vec::with_capacity(rows.len().min(VECTOR_SIZE));
            let needs_projected_dedup = present_groups.iter().any(|present| !present);
            let mut projected_seen = if needs_projected_dedup {
                Some(new_projected_distinct_set(
                    modifier_memory,
                    rows.len(),
                    agg_idx,
                )?)
            } else {
                None
            };
            for row in &rows {
                if let Some(seen) = projected_seen.as_mut() {
                    let inserted = seen
                        .try_insert(ProjectedDistinctRow {
                            row,
                            present_groups: &present_groups,
                            group_count,
                        })
                        .map_err(|e| {
                            paro_error::out_of_memory(format!("distinct aggregate {agg_idx}: {e}"))
                        })?;
                    if !inserted {
                        continue;
                    }
                }
                batch.push(row);
                if batch.len() < VECTOR_SIZE {
                    continue;
                }
                updater.flush(&batch)?;
                batch.clear();
            }
            if !batch.is_empty() {
                updater.flush(&batch)?;
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
}

impl DistinctTableBatchUpdater<'_> {
    pub(crate) fn flush(&mut self, batch: &[&AccountedValueRow]) -> Result<()> {
        populate_distinct_group_chunk(&mut self.groups, batch, self.present_groups, self.agg_idx)?;
        populate_distinct_input_chunk(
            &mut self.input_chunk,
            batch,
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
            Some(set) => set.into_rows(),
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
        for batch in rows.chunks(VECTOR_SIZE) {
            populate_distinct_input_chunk(&mut input_chunk, batch, 0, input_count, agg_idx)?;
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
