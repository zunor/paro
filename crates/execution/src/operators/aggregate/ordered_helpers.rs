// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Ordered aggregate collection and replay.
//!
//! Build sinks keep ordered aggregate rows out of the normal per-chunk update
//! path. At finish time, each ordered aggregate sorts its own collected rows
//! and replays them into the regular aggregate state kernel.
//!
//! Rows are collected into an operator-owned typed row store. This avoids the
//! old fixed-stride value arena while still keeping ORDER BY replay
//! isolated from the normal aggregate update path.

use std::cmp::Ordering;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use paro_common::allocator::{Allocator, ArenaAllocator};
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::memory::{
    AccountedHashSet, MemoryAccountingClass, MemoryAccountingContext, PrecomputedHashBuildHasher,
};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_common::vector::{SelectionVector, Vector, VectorSelection, VECTOR_SIZE};
use paro_function::aggregate::{AggregateCombineType, AggregateInputData};
use paro_planner::expression::{Expression, OrderByExpression};
use paro_storage::buffer::{BufferPool, MemoryTag as StorageMemoryTag};
use paro_storage::row::{RowLayout, RowStoreBuilder, RowValidityType};

use crate::operators::aggregate::accounted_rows::{hash_value, mix_row_hash};
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
use crate::runtime::breaker::UngroupedAggregateRuntimeState;

/// Typed row arena storing `[group values... | order values... | input values...]`.
#[derive(Debug)]
pub(crate) struct OrderedAggregateCollector {
    rows: RowStoreBuilder,
    row_types: Arc<[LogicalType]>,
    layout: Arc<RowLayout>,
    buffer_pool: Arc<BufferPool>,
    memory: MemoryAccountingContext,
    group_width: usize,
    order_width: usize,
    input_width: usize,
}

impl OrderedAggregateCollector {
    pub(crate) fn new(
        buffer_pool: Arc<BufferPool>,
        row_types: Vec<LogicalType>,
        memory: MemoryAccountingContext,
        group_width: usize,
        order_width: usize,
        input_width: usize,
    ) -> Self {
        let row_types: Arc<[LogicalType]> = Arc::from(row_types.into_boxed_slice());
        let layout = Arc::new(RowLayout::from_types(
            row_types.iter().cloned().collect(),
            RowValidityType::CanHaveNullValues,
        ));
        let rows = RowStoreBuilder::new_with_memory(
            Arc::clone(&buffer_pool),
            Arc::clone(&layout),
            StorageMemoryTag::HashTable,
            memory.clone(),
        );
        Self {
            rows,
            row_types,
            layout,
            buffer_pool,
            memory,
            group_width,
            order_width,
            input_width,
        }
    }

    #[inline]
    fn empty_builder(&self) -> RowStoreBuilder {
        RowStoreBuilder::new_with_memory(
            Arc::clone(&self.buffer_pool),
            Arc::clone(&self.layout),
            StorageMemoryTag::HashTable,
            self.memory.clone(),
        )
    }

    #[inline]
    pub(crate) fn row_count(&self) -> usize {
        usize::try_from(self.rows.count()).unwrap_or(usize::MAX)
    }

    #[inline]
    pub(crate) fn is_empty(&self) -> bool {
        self.rows.count() == 0
    }

    fn append_rows(
        &mut self,
        payload: &Chunk,
        group_refs: &[usize],
        order_refs: &[usize],
        input_refs: &[usize],
        selection: Option<&SelectionVector>,
    ) -> Result<()> {
        validate_ordered_row_refs(payload, group_refs, order_refs, input_refs)?;
        let row_refs = ordered_row_refs(group_refs, order_refs, input_refs);
        let mut projected = Chunk::try_init_empty(&self.row_types, payload.allocator().clone())?;
        projected.reference_columns(payload, &row_refs);
        let appended = if let Some(selection) = selection {
            self.rows
                .append_selected(&projected, selection, selection.len())?
        } else {
            self.rows.append(&projected)?
        };
        let expected = selection.map_or(payload.size(), SelectionVector::len);
        if appended != expected {
            return Err(paro_error::internal(format!(
                "ordered aggregate row append count mismatch: expected={expected}, appended={appended}"
            )));
        }
        Ok(())
    }

    fn append(&mut self, other: &mut Self) -> Result<()> {
        if self.group_width != other.group_width
            || self.order_width != other.order_width
            || self.input_width != other.input_width
            || self.row_types.as_ref() != other.row_types.as_ref()
        {
            return Err(paro_error::internal(format!(
                "ordered aggregate collector shape mismatch: target=({},{},{}) source=({},{},{})",
                self.group_width,
                self.order_width,
                self.input_width,
                other.group_width,
                other.order_width,
                other.input_width
            )));
        }
        let replacement = other.empty_builder();
        let source_rows = std::mem::replace(&mut other.rows, replacement);
        self.rows.try_absorb(source_rows)?;
        Ok(())
    }

    fn take_rows(&mut self, allocator: Arc<dyn Allocator>) -> Result<OrderedRows> {
        let count = self.row_count();
        let replacement = self.empty_builder();
        let rows = std::mem::replace(&mut self.rows, replacement);
        let store = rows.try_seal()?;
        let mut chunk = Chunk::try_initialize(self.row_types.as_ref(), count.max(1), allocator)?;
        if count > 0 {
            let pinned = store.pin_ordinal_range(0, count as u64)?;
            let positions = identity_output_positions(count)?;
            let projections = (0..self.row_types.len())
                .map(|idx| (idx, idx))
                .collect::<Vec<_>>();
            pinned.gather_columns_projected(&projections, &mut chunk, &positions)?;
            chunk.try_set_cardinality(count)?;
        }
        Ok(OrderedRows {
            chunk,
            group_width: self.group_width,
            order_width: self.order_width,
            input_width: self.input_width,
        })
    }
}

pub(crate) fn empty_ordered_collectors_with_memory(
    spec: &AggregateSpec,
    group_refs: &[usize],
    buffer_pool: Arc<BufferPool>,
    memory: MemoryAccountingContext,
) -> Result<Vec<OrderedAggregateCollector>> {
    let group_width = group_refs.len();
    spec.aggregate_orders
        .iter()
        .enumerate()
        .map(|(agg_idx, order_refs)| {
            if order_refs.is_empty() {
                return Ok(OrderedAggregateCollector::new(
                    Arc::clone(&buffer_pool),
                    Vec::new(),
                    memory.clone(),
                    0,
                    0,
                    0,
                ));
            }
            let order_width = order_refs.len();
            let input_width = spec.aggregate_inputs[agg_idx].len();
            let mut row_types = ordered_row_types(
                spec,
                group_refs,
                order_refs,
                &spec.aggregate_inputs[agg_idx],
            )?;
            if row_types.len() != group_width + order_width + input_width {
                return Err(paro_error::internal(format!(
                    "ordered aggregate row type width mismatch: aggregate={agg_idx}"
                )));
            }
            Ok(OrderedAggregateCollector::new(
                Arc::clone(&buffer_pool),
                std::mem::take(&mut row_types),
                memory.clone(),
                group_width,
                order_width,
                input_width,
            ))
        })
        .collect()
}

pub(crate) fn collect_ordered_rows(
    spec: &AggregateSpec,
    aggregate_objects: &[AggregateObject],
    payload: &Chunk,
    group_refs: &[usize],
    collectors: &mut [OrderedAggregateCollector],
) -> Result<()> {
    if collectors.len() != aggregate_objects.len() {
        return Err(paro_error::internal(format!(
            "ordered aggregate collector count mismatch: collectors={} aggregates={}",
            collectors.len(),
            aggregate_objects.len()
        )));
    }
    let filter_selections = if has_aggregate_filters(spec) {
        Some(build_per_aggregate_filters(spec, payload)?)
    } else {
        None
    };
    for (agg_idx, object) in aggregate_objects.iter().enumerate() {
        if object.order_bys.is_empty() {
            continue;
        }
        let input_refs = &spec.aggregate_inputs[agg_idx];
        let order_refs = &spec.aggregate_orders[agg_idx];
        let filter = filter_selections.as_ref().and_then(|f| f[agg_idx].as_ref());
        let collector = &mut collectors[agg_idx];
        collector.append_rows(payload, group_refs, order_refs, input_refs, filter)?;
    }
    Ok(())
}

pub(crate) fn merge_ordered_collectors(
    target: &mut [OrderedAggregateCollector],
    source: &mut [OrderedAggregateCollector],
) -> Result<()> {
    if target.len() != source.len() {
        return Err(paro_error::internal(format!(
            "ordered aggregate merge count mismatch: target={} source={}",
            target.len(),
            source.len()
        )));
    }
    for (target, source) in target.iter_mut().zip(source.iter_mut()) {
        if source.is_empty() {
            continue;
        }
        target.append(source)?;
    }
    Ok(())
}

pub(crate) fn finalize_ordered_into_hash_tables(
    spec: &AggregateSpec,
    aggregate_objects: &[AggregateObject],
    group_refs: &[usize],
    grouping_sets: &[Box<[usize]>],
    modifier_memory: &MemoryAccountingContext,
    ordered_collectors: &mut [OrderedAggregateCollector],
    tables: &mut [AggregateHashTable],
) -> Result<()> {
    if grouping_sets.len() != tables.len() {
        return Err(paro_error::internal(format!(
            "hash aggregate grouping table count mismatch while finalizing ordered aggregates: grouping_sets={} tables={}",
            grouping_sets.len(),
            tables.len()
        )));
    }
    let group_count = group_refs.len();
    let group_types = group_types(spec)?;
    let full_layout = AggregateStateLayout::new(aggregate_objects)?;
    for (agg_idx, object) in aggregate_objects.iter().enumerate() {
        let collector = &mut ordered_collectors[agg_idx];
        if object.order_bys.is_empty() || collector.is_empty() {
            continue;
        }
        let allocator = tables
            .first()
            .map(AggregateHashTable::allocator)
            .ok_or_else(|| {
                paro_error::internal(
                    "hash aggregate has no tables while finalizing ordered aggregates",
                )
            })?;
        let ordered_rows = collector.take_rows(allocator)?;
        let input_count = spec.aggregate_inputs[agg_idx].len();
        let input_types = aggregate_input_types(spec, agg_idx)?;
        let orders = aggregate_orders(spec, agg_idx)?;
        for (table, grouping_set) in tables.iter_mut().zip(grouping_sets.iter()) {
            let present_groups = grouping_set_present_mask(group_count, grouping_set, agg_idx)?;
            let mut indices: Vec<usize> = (0..ordered_rows.row_count()).collect();
            indices.sort_by(|&left, &right| {
                compare_projected_groups_rows(&ordered_rows, left, right, &present_groups)
                    .then_with(|| compare_order_values_rows(&ordered_rows, left, right, orders))
                    .then_with(|| left.cmp(&right))
            });
            let allocator = table.allocator();
            let single_input = vec![(0..input_count).collect::<Vec<usize>>()];
            let batch_cap = indices.len().min(VECTOR_SIZE).max(1);
            let mut updater = OrderedTableBatchUpdater {
                table,
                object,
                full_layout: &full_layout,
                single_input: &single_input,
                present_groups: &present_groups,
                input_count,
                agg_idx,
                groups: Chunk::try_initialize(&group_types, batch_cap, allocator.clone())?,
                input_chunk: Chunk::try_initialize(&input_types, batch_cap, allocator.clone())?,
                addresses: Vector::try_new(LogicalType::BigInt, batch_cap, allocator.clone())?,
                new_groups: SelectionVector::try_with_capacity(batch_cap, allocator.clone())?,
                arena: ArenaAllocator::new(allocator.clone()),
                allocator,
            };
            let mut batch = Vec::with_capacity(batch_cap);
            let mut distinct_seen = if object.is_distinct() {
                Some(new_ordered_distinct_set(
                    modifier_memory,
                    indices.len(),
                    agg_idx,
                )?)
            } else {
                None
            };
            for &row_idx in &indices {
                if let Some(seen) = distinct_seen.as_mut() {
                    let key = OrderedDistinctRow::new(&ordered_rows, row_idx, &present_groups);
                    if !seen.try_insert(key).map_err(|e| {
                        paro_error::out_of_memory(format!("ordered aggregate {agg_idx}: {e}"))
                    })? {
                        continue;
                    }
                }
                batch.push(row_idx);
                if batch.len() < VECTOR_SIZE {
                    continue;
                }
                updater.flush_rows(&ordered_rows, &batch)?;
                batch.clear();
            }
            if !batch.is_empty() {
                updater.flush_rows(&ordered_rows, &batch)?;
            }
        }
    }
    Ok(())
}

pub(crate) fn finalize_ordered_ungrouped(
    spec: &AggregateSpec,
    modifier_memory: &MemoryAccountingContext,
    state: &mut UngroupedAggregateRuntimeState,
) -> Result<()> {
    let aggregate_count = state.aggregate_objects.len();
    if state.ordered_collectors.len() != aggregate_count {
        return Err(paro_error::internal(format!(
            "ungrouped ordered collector count mismatch: collectors={} aggregates={aggregate_count}",
            state.ordered_collectors.len()
        )));
    }
    let objects = Arc::clone(&state.aggregate_objects);
    for agg_idx in 0..aggregate_count {
        let object = &objects[agg_idx];
        let collector = &mut state.ordered_collectors[agg_idx];
        if object.order_bys.is_empty() || collector.is_empty() {
            continue;
        }
        let allocator = state.arena_allocator.get_allocator().clone();
        let ordered_rows = collector.take_rows(allocator.clone())?;
        let input_count = spec.aggregate_inputs[agg_idx].len();
        let input_types = aggregate_input_types(spec, agg_idx)?;
        let orders = aggregate_orders(spec, agg_idx)?;
        let mut indices: Vec<usize> = (0..ordered_rows.row_count()).collect();
        indices.sort_by(|&left, &right| {
            compare_order_values_rows(&ordered_rows, left, right, orders)
                .then_with(|| left.cmp(&right))
        });

        let state_offset = state.layout.state_offset(agg_idx);
        let agg_ptr = unsafe { (state.state_buffer.as_mut_ptr() as *mut u8).add(state_offset) };
        let single_input = vec![(0..input_count).collect::<Vec<usize>>()];
        let batch_cap = indices.len().min(VECTOR_SIZE).max(1);
        let mut input_chunk = Chunk::try_initialize(&input_types, batch_cap, allocator.clone())?;
        let mut addresses = Vector::try_new(LogicalType::BigInt, batch_cap, allocator)?;
        let mut distinct_seen = if object.is_distinct() {
            Some(new_ordered_distinct_set(
                modifier_memory,
                indices.len(),
                agg_idx,
            )?)
        } else {
            None
        };
        let mut batch = Vec::with_capacity(batch_cap);
        for &row_idx in &indices {
            if let Some(seen) = distinct_seen.as_mut() {
                let key = OrderedDistinctRow::new(&ordered_rows, row_idx, &[]);
                if !seen.try_insert(key).map_err(|e| {
                    paro_error::out_of_memory(format!("ordered aggregate {agg_idx}: {e}"))
                })? {
                    continue;
                }
            }
            batch.push(row_idx);
            if batch.len() < VECTOR_SIZE {
                continue;
            }
            flush_ungrouped_ordered_batch_arena(
                object,
                &single_input,
                &mut state.arena_allocator,
                &mut addresses,
                agg_ptr,
                &mut input_chunk,
                &ordered_rows,
                &batch,
            )?;
            batch.clear();
        }
        if !batch.is_empty() {
            flush_ungrouped_ordered_batch_arena(
                object,
                &single_input,
                &mut state.arena_allocator,
                &mut addresses,
                agg_ptr,
                &mut input_chunk,
                &ordered_rows,
                &batch,
            )?;
        }
        drop(distinct_seen);
    }
    Ok(())
}

struct OrderedTableBatchUpdater<'a> {
    table: &'a mut AggregateHashTable,
    object: &'a AggregateObject,
    full_layout: &'a AggregateStateLayout,
    single_input: &'a [Vec<usize>],
    present_groups: &'a [bool],
    input_count: usize,
    agg_idx: usize,
    groups: Chunk,
    input_chunk: Chunk,
    addresses: Vector,
    new_groups: SelectionVector,
    arena: ArenaAllocator,
    allocator: Arc<dyn paro_common::allocator::Allocator>,
}

impl OrderedTableBatchUpdater<'_> {
    fn flush_rows(&mut self, rows: &OrderedRows, batch: &[usize]) -> Result<()> {
        populate_ordered_group_chunk_rows(
            &mut self.groups,
            rows,
            batch,
            self.present_groups,
            self.agg_idx,
        )?;
        populate_ordered_input_chunk_rows(
            &mut self.input_chunk,
            rows,
            batch,
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
        let states = build_state_vector(
            &self.addresses,
            self.full_layout,
            self.agg_idx,
            None,
            batch.len(),
        )?;
        let payload_desc = AggregatePayload {
            chunk: &self.input_chunk,
            aggregate_inputs: self.single_input,
        };
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

#[allow(clippy::too_many_arguments)]
fn flush_ungrouped_ordered_batch_arena(
    object: &AggregateObject,
    single_input: &[Vec<usize>],
    arena: &mut ArenaAllocator,
    addresses: &mut Vector,
    agg_ptr: *mut u8,
    input_chunk: &mut Chunk,
    rows: &OrderedRows,
    batch: &[usize],
) -> Result<()> {
    populate_ordered_input_chunk_rows(input_chunk, rows, batch, single_input[0].len(), 0)?;
    fill_repeated_state_addresses(addresses, agg_ptr, batch.len())?;
    let payload_desc = AggregatePayload {
        chunk: input_chunk,
        aggregate_inputs: single_input,
    };
    let mut input_data = AggregateInputData::new(
        object.bind_info.as_deref(),
        arena,
        AggregateCombineType::PreserveInput,
    );
    update_states(
        std::slice::from_ref(object),
        &mut input_data,
        &payload_desc,
        addresses,
        batch.len(),
    )
}

struct OrderedRows {
    chunk: Chunk,
    group_width: usize,
    order_width: usize,
    input_width: usize,
}

impl OrderedRows {
    #[inline]
    fn row_count(&self) -> usize {
        self.chunk.size()
    }

    #[inline]
    fn input_base(&self) -> usize {
        self.group_width + self.order_width
    }

    fn value(&self, column_idx: usize, row_idx: usize) -> Value {
        self.chunk
            .column(column_idx)
            .expect("ordered aggregate row column should exist")
            .get_value(row_idx)
    }

    fn values_equal(&self, column_idx: usize, left: usize, right: usize) -> bool {
        self.value(column_idx, left) == self.value(column_idx, right)
    }
}

fn populate_ordered_input_chunk_rows(
    chunk: &mut Chunk,
    rows: &OrderedRows,
    batch: &[usize],
    input_count: usize,
    agg_idx: usize,
) -> Result<()> {
    chunk.try_set_cardinality(batch.len())?;
    if input_count > rows.input_width {
        return Err(paro_error::internal(format!(
            "ordered aggregate input width mismatch: aggregate={agg_idx}, expected={input_count}, stored={}",
            rows.input_width
        )));
    }
    let selection = batch_selection(batch, chunk.allocator().clone())?;
    let selection = VectorSelection::materialized(selection);
    for input_idx in 0..input_count {
        let source_col_idx = rows.input_base() + input_idx;
        let source_col = rows.chunk.column(source_col_idx).ok_or_else(|| {
            paro_error::internal(format!(
                "missing ordered aggregate stored input column {input_idx} at aggregate {agg_idx}"
            ))
        })?;
        let target_col = chunk.column_mut(input_idx).ok_or_else(|| {
            paro_error::internal(format!(
                "missing ordered aggregate input column {input_idx} at aggregate {agg_idx}"
            ))
        })?;
        target_col.try_copy_selection(0, source_col, &selection, batch.len())?;
    }
    Ok(())
}

fn populate_ordered_group_chunk_rows(
    groups: &mut Chunk,
    rows: &OrderedRows,
    batch: &[usize],
    present_groups: &[bool],
    agg_idx: usize,
) -> Result<()> {
    groups.try_set_cardinality(batch.len())?;
    if present_groups.len() > rows.group_width {
        return Err(paro_error::internal(format!(
            "ordered aggregate group width mismatch: aggregate={agg_idx}, expected={}, stored={}",
            present_groups.len(),
            rows.group_width
        )));
    }
    let selection = batch_selection(batch, groups.allocator().clone())?;
    let selection = VectorSelection::materialized(selection);
    for (group_idx, is_present) in present_groups.iter().copied().enumerate() {
        let target_col = groups.column_mut(group_idx).ok_or_else(|| {
            paro_error::internal(format!(
                "missing ordered aggregate group column {group_idx} at aggregate {agg_idx}"
            ))
        })?;
        if is_present {
            let source_col = rows.chunk.column(group_idx).ok_or_else(|| {
                paro_error::internal(format!(
                    "missing ordered aggregate stored group column {group_idx} at aggregate {agg_idx}"
                ))
            })?;
            target_col.try_copy_selection(0, source_col, &selection, batch.len())?;
        } else {
            target_col.validity_mut().try_set_all_invalid(batch.len())?;
        }
    }
    Ok(())
}

fn grouping_set_present_mask(
    group_count: usize,
    grouping_set: &[usize],
    agg_idx: usize,
) -> Result<Vec<bool>> {
    let mut present = vec![false; group_count];
    for &group_idx in grouping_set {
        if group_idx >= group_count {
            return Err(paro_error::internal(format!(
                "grouping set index out of bounds while finalizing ordered aggregate {agg_idx}: group_idx={group_idx}, group_count={group_count}"
            )));
        }
        present[group_idx] = true;
    }
    Ok(present)
}

#[derive(Clone, Copy)]
struct OrderedDistinctRow<'a> {
    rows: &'a OrderedRows,
    row_idx: usize,
    present_groups: &'a [bool],
    hash: u64,
}

impl<'a> OrderedDistinctRow<'a> {
    fn new(rows: &'a OrderedRows, row_idx: usize, present_groups: &'a [bool]) -> Self {
        let mut hash = mix_row_hash(0xa6d3_75f9_1452_2c29, present_groups.len() as u64);
        for (group_idx, present) in present_groups.iter().copied().enumerate() {
            if present {
                hash = mix_row_hash(hash, hash_value(&rows.value(group_idx, row_idx)));
            }
        }
        for input_idx in 0..rows.input_width {
            hash = mix_row_hash(
                hash,
                hash_value(&rows.value(rows.input_base() + input_idx, row_idx)),
            );
        }
        Self {
            rows,
            row_idx,
            present_groups,
            hash,
        }
    }
}

impl PartialEq for OrderedDistinctRow<'_> {
    fn eq(&self, other: &Self) -> bool {
        if self.present_groups != other.present_groups {
            return false;
        }
        for (group_idx, present) in self.present_groups.iter().copied().enumerate() {
            if present
                && !self
                    .rows
                    .values_equal(group_idx, self.row_idx, other.row_idx)
            {
                return false;
            }
        }
        for input_idx in 0..self.rows.input_width {
            let source_col_idx = self.rows.input_base() + input_idx;
            if !self
                .rows
                .values_equal(source_col_idx, self.row_idx, other.row_idx)
            {
                return false;
            }
        }
        true
    }
}

impl Eq for OrderedDistinctRow<'_> {}

impl Hash for OrderedDistinctRow<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u64(self.hash);
    }
}

fn new_ordered_distinct_set<'a>(
    modifier_memory: &MemoryAccountingContext,
    capacity: usize,
    agg_idx: usize,
) -> Result<AccountedHashSet<OrderedDistinctRow<'a>, PrecomputedHashBuildHasher>> {
    let metadata_memory = modifier_memory.with_class(MemoryAccountingClass::Metadata);
    let mut seen = AccountedHashSet::new_with_accounting_and_hasher(
        metadata_memory
            .grant()
            .map_err(|e| paro_error::out_of_memory(format!("ordered aggregate {agg_idx}: {e}")))?,
        metadata_memory.tag(),
        metadata_memory.accounting_class(),
        PrecomputedHashBuildHasher,
    );
    seen.try_reserve(capacity)
        .map_err(|e| paro_error::out_of_memory(format!("ordered aggregate {agg_idx}: {e}")))?;
    Ok(seen)
}

fn aggregate_input_types(spec: &AggregateSpec, agg_idx: usize) -> Result<Vec<LogicalType>> {
    spec.aggregate_inputs
        .get(agg_idx)
        .ok_or_else(|| {
            paro_error::internal(format!(
                "aggregate input mapping missing for ordered aggregate {agg_idx}"
            ))
        })?
        .iter()
        .map(|&idx| {
            spec.payload_types.get(idx).cloned().ok_or_else(|| {
                paro_error::internal(format!(
                    "ordered aggregate payload type index out of bounds: aggregate={agg_idx}, payload={idx}"
                ))
            })
        })
        .collect()
}

fn aggregate_orders(spec: &AggregateSpec, agg_idx: usize) -> Result<&[OrderByExpression]> {
    let aggregate = spec.aggregates.get(agg_idx).ok_or_else(|| {
        paro_error::internal(format!("ordered aggregate index out of bounds: {agg_idx}"))
    })?;
    match aggregate {
        Expression::Aggregate(bound) => Ok(bound.order_bys.as_slice()),
        _ => Err(paro_error::internal(format!(
            "expected aggregate expression at index {agg_idx}"
        ))),
    }
}

fn ordered_row_refs(
    group_refs: &[usize],
    order_refs: &[usize],
    input_refs: &[usize],
) -> Vec<usize> {
    group_refs
        .iter()
        .chain(order_refs.iter())
        .chain(input_refs.iter())
        .copied()
        .collect()
}

fn ordered_row_types(
    spec: &AggregateSpec,
    group_refs: &[usize],
    order_refs: &[usize],
    input_refs: &[usize],
) -> Result<Vec<LogicalType>> {
    ordered_row_refs(group_refs, order_refs, input_refs)
        .into_iter()
        .map(|idx| {
            spec.payload_types.get(idx).cloned().ok_or_else(|| {
                paro_error::internal(format!(
                    "ordered aggregate payload type index out of bounds: idx={idx}, type_count={}",
                    spec.payload_types.len()
                ))
            })
        })
        .collect()
}

fn validate_ordered_row_refs(
    payload: &Chunk,
    group_refs: &[usize],
    order_refs: &[usize],
    input_refs: &[usize],
) -> Result<()> {
    for col_idx in ordered_row_refs(group_refs, order_refs, input_refs) {
        if col_idx >= payload.column_count() {
            return Err(paro_error::internal(format!(
                "ordered aggregate payload column not found: idx={col_idx}, column_count={}",
                payload.column_count()
            )));
        }
    }
    Ok(())
}

fn identity_output_positions(count: usize) -> Result<Vec<u32>> {
    let count_u32 = u32::try_from(count).map_err(|_| {
        paro_error::internal(format!(
            "ordered aggregate row count exceeds vector selection domain: count={count}"
        ))
    })?;
    Ok((0..count_u32).collect())
}

fn batch_selection(batch: &[usize], allocator: Arc<dyn Allocator>) -> Result<SelectionVector> {
    let mut selection = SelectionVector::try_with_capacity(batch.len(), allocator)?;
    selection.set_len(batch.len());
    for (out_idx, &row_idx) in batch.iter().enumerate() {
        let row_idx = u32::try_from(row_idx).map_err(|_| {
            paro_error::internal(format!(
                "ordered aggregate row index exceeds vector selection domain: row_idx={row_idx}"
            ))
        })?;
        selection.try_set(out_idx, row_idx as usize)?;
    }
    Ok(selection)
}

fn compare_projected_groups_rows(
    rows: &OrderedRows,
    left: usize,
    right: usize,
    present_groups: &[bool],
) -> Ordering {
    for (group_idx, present) in present_groups.iter().copied().enumerate() {
        if !present {
            continue;
        }
        let cmp = compare_values(
            &rows.value(group_idx, left),
            &rows.value(group_idx, right),
            true,
            true,
        );
        if cmp != Ordering::Equal {
            return cmp;
        }
    }
    Ordering::Equal
}

fn compare_order_values_rows(
    rows: &OrderedRows,
    left: usize,
    right: usize,
    orders: &[OrderByExpression],
) -> Ordering {
    for (idx, order) in orders.iter().enumerate() {
        let col_idx = rows.group_width + idx;
        let cmp = compare_values(
            &rows.value(col_idx, left),
            &rows.value(col_idx, right),
            order.ascending,
            order.nulls_first,
        );
        if cmp != Ordering::Equal {
            return cmp;
        }
    }
    Ordering::Equal
}

fn compare_values(left: &Value, right: &Value, ascending: bool, nulls_first: bool) -> Ordering {
    match (left.is_null(), right.is_null()) {
        (true, true) => Ordering::Equal,
        (true, false) => {
            if nulls_first {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        }
        (false, true) => {
            if nulls_first {
                Ordering::Greater
            } else {
                Ordering::Less
            }
        }
        (false, false) => {
            let cmp = left.partial_cmp(right).unwrap_or(Ordering::Equal);
            if ascending {
                cmp
            } else {
                cmp.reverse()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use paro_common::allocator::DefaultAllocator;
    use paro_storage::buffer::BufferPool;

    use crate::memory_runtime::QueryMemoryPool;
    use crate::operators::aggregate::accounted_rows::aggregate_modifier_memory_context;

    use super::*;

    fn modifier_memory() -> MemoryAccountingContext {
        let owner: Arc<dyn paro_common::memory::MemoryOwner> =
            Arc::new(QueryMemoryPool::new(8 * 1024 * 1024));
        aggregate_modifier_memory_context(owner)
    }

    fn collector_with_group_order_input_rows(
        rows: &[(i32, i32, i32, i32)],
    ) -> OrderedAggregateCollector {
        let allocator = Arc::new(DefaultAllocator::new());
        let types = vec![
            LogicalType::Integer,
            LogicalType::Integer,
            LogicalType::Integer,
            LogicalType::Integer,
        ];
        let mut chunk = Chunk::try_initialize(&types, rows.len().max(1), allocator).expect("chunk");
        chunk.try_set_cardinality(rows.len()).expect("cardinality");
        for (row_idx, &(group0, group1, order, input)) in rows.iter().enumerate() {
            chunk
                .set_value(0, row_idx, &Value::Integer(group0))
                .expect("group0");
            chunk
                .set_value(1, row_idx, &Value::Integer(group1))
                .expect("group1");
            chunk
                .set_value(2, row_idx, &Value::Integer(order))
                .expect("order");
            chunk
                .set_value(3, row_idx, &Value::Integer(input))
                .expect("input");
        }
        let mut collector = OrderedAggregateCollector::new(
            BufferPool::new_arc(1024 * 1024),
            types,
            modifier_memory(),
            2,
            1,
            1,
        );
        collector
            .append_rows(&chunk, &[0, 1], &[2], &[3], None)
            .expect("append rows");
        collector
    }

    #[test]
    fn ordered_distinct_row_deduplicates_without_boxed_value_key() {
        let mut collector = collector_with_group_order_input_rows(&[
            (1, 10, 2, 100),
            (1, 20, 1, 100),
            (1, 20, 3, 200),
        ]);
        let rows = collector
            .take_rows(Arc::new(DefaultAllocator::new()))
            .expect("ordered rows");
        let present_groups = [true, false];
        let mut seen = new_ordered_distinct_set(&modifier_memory(), rows.row_count(), 0)
            .expect("distinct set");

        assert!(seen
            .try_insert(OrderedDistinctRow::new(&rows, 0, &present_groups))
            .expect("insert first projected row"));
        assert!(!seen
            .try_insert(OrderedDistinctRow::new(&rows, 1, &present_groups))
            .expect("dedupe duplicate projected row"));
        assert!(seen
            .try_insert(OrderedDistinctRow::new(&rows, 2, &present_groups))
            .expect("insert second input"));
    }

    #[test]
    fn ungrouped_ordered_distinct_row_uses_only_input_values() {
        let mut collector = collector_with_group_order_input_rows(&[
            (1, 10, 2, 100),
            (9, 90, 1, 100),
            (9, 90, 3, 200),
        ]);
        let rows = collector
            .take_rows(Arc::new(DefaultAllocator::new()))
            .expect("ordered rows");
        let mut seen = new_ordered_distinct_set(&modifier_memory(), rows.row_count(), 0)
            .expect("distinct set");

        assert!(seen
            .try_insert(OrderedDistinctRow::new(&rows, 0, &[]))
            .expect("insert first input"));
        assert!(!seen
            .try_insert(OrderedDistinctRow::new(&rows, 1, &[]))
            .expect("dedupe duplicate input"));
        assert!(seen
            .try_insert(OrderedDistinctRow::new(&rows, 2, &[]))
            .expect("insert second input"));
    }
}
