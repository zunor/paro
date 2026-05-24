// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Ordered aggregate collection and replay.
//!
//! Build sinks keep ordered aggregate rows out of the normal per-chunk update
//! path. At finish time, each ordered aggregate sorts its own collected rows
//! and replays them into the regular aggregate state kernel.
//!
//! Values are stored in a flat arena (`Vec<Value>`) with fixed stride per row,
//! eliminating per-row heap allocations on the consume hot path.

use std::cmp::Ordering;
use std::collections::HashSet;
use std::sync::Arc;

use paro_common::allocator::ArenaAllocator;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_common::vector::{SelectionVector, Vector, VECTOR_SIZE};
use paro_function::aggregate::{AggregateCombineType, AggregateInputData};
use paro_planner::expression::{Expression, OrderByExpression};

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

/// Flat arena storing ordered aggregate rows without per-row heap allocations.
///
/// Each row occupies `stride()` contiguous slots in `values`:
/// `[group_values... | order_values... | input_values...]`
///
/// Sequence (insertion order) is implicit: row N is at offset `N * stride`.
#[derive(Debug, Clone)]
pub(crate) struct OrderedAggregateCollector {
    values: Vec<Value>,
    group_width: usize,
    order_width: usize,
    input_width: usize,
}

impl OrderedAggregateCollector {
    pub(crate) fn new(group_width: usize, order_width: usize, input_width: usize) -> Self {
        Self {
            values: Vec::new(),
            group_width,
            order_width,
            input_width,
        }
    }

    #[inline]
    fn stride(&self) -> usize {
        self.group_width + self.order_width + self.input_width
    }

    #[inline]
    fn reserve_rows(&mut self, rows: usize) {
        self.values.reserve(rows.saturating_mul(self.stride()));
    }

    #[inline]
    pub(crate) fn row_count(&self) -> usize {
        let s = self.stride();
        if s == 0 {
            return 0;
        }
        self.values.len() / s
    }

    #[inline]
    pub(crate) fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    #[inline]
    fn group_values(&self, row: usize) -> &[Value] {
        let base = row * self.stride();
        &self.values[base..base + self.group_width]
    }

    #[inline]
    fn order_values(&self, row: usize) -> &[Value] {
        let base = row * self.stride() + self.group_width;
        &self.values[base..base + self.order_width]
    }

    #[inline]
    fn input_values(&self, row: usize) -> &[Value] {
        let base = row * self.stride() + self.group_width + self.order_width;
        &self.values[base..base + self.input_width]
    }

    #[inline]
    fn push_row(
        &mut self,
        payload: &Chunk,
        group_refs: &[usize],
        order_refs: &[usize],
        input_refs: &[usize],
        row_idx: usize,
    ) -> Result<()> {
        for &col_idx in group_refs {
            self.values.push(payload_value(payload, col_idx, row_idx)?);
        }
        for &col_idx in order_refs {
            self.values.push(payload_value(payload, col_idx, row_idx)?);
        }
        for &col_idx in input_refs {
            self.values.push(payload_value(payload, col_idx, row_idx)?);
        }
        Ok(())
    }

    pub(crate) fn clear(&mut self) {
        self.values.clear();
    }

    fn append(&mut self, other: &mut Self) -> Result<()> {
        if self.group_width != other.group_width
            || self.order_width != other.order_width
            || self.input_width != other.input_width
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
        self.values.append(&mut other.values);
        Ok(())
    }
}

pub(crate) fn empty_ordered_collectors(
    spec: &AggregateSpec,
    group_refs: &[usize],
) -> Vec<OrderedAggregateCollector> {
    let group_width = group_refs.len();
    spec.aggregate_orders
        .iter()
        .enumerate()
        .map(|(agg_idx, order_refs)| {
            let order_width = order_refs.len();
            let input_width = spec.aggregate_inputs[agg_idx].len();
            OrderedAggregateCollector::new(group_width, order_width, input_width)
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
        if let Some(selection) = filter {
            collector.reserve_rows(selection.len());
            for idx in 0..selection.len() {
                collector.push_row(
                    payload,
                    group_refs,
                    order_refs,
                    input_refs,
                    selection.get(idx),
                )?;
            }
        } else {
            collector.reserve_rows(payload.size());
            for row_idx in 0..payload.size() {
                collector.push_row(payload, group_refs, order_refs, input_refs, row_idx)?;
            }
        }
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
    let group_types = group_types(spec);
    let full_layout = AggregateStateLayout::new(aggregate_objects)?;
    for (agg_idx, object) in aggregate_objects.iter().enumerate() {
        let collector = &ordered_collectors[agg_idx];
        if object.order_bys.is_empty() || collector.is_empty() {
            continue;
        }
        let input_count = spec.aggregate_inputs[agg_idx].len();
        let input_types = aggregate_input_types(spec, agg_idx)?;
        let orders = aggregate_orders(spec, agg_idx)?;
        for (table, grouping_set) in tables.iter_mut().zip(grouping_sets.iter()) {
            let present_groups = grouping_set_present_mask(group_count, grouping_set, agg_idx)?;
            let mut indices: Vec<usize> = (0..collector.row_count()).collect();
            indices.sort_by(|&left, &right| {
                compare_projected_groups_arena(collector, left, right, &present_groups)
                    .then_with(|| compare_order_values_arena(collector, left, right, orders))
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
            let mut distinct_seen = object.is_distinct().then(HashSet::<Box<[Value]>>::new);
            for &row_idx in &indices {
                if let Some(seen) = distinct_seen.as_mut() {
                    let key = projected_distinct_key_arena(collector, row_idx, &present_groups);
                    if !seen.insert(key) {
                        continue;
                    }
                }
                batch.push(row_idx);
                if batch.len() < VECTOR_SIZE {
                    continue;
                }
                updater.flush_arena(collector, &batch)?;
                batch.clear();
            }
            if !batch.is_empty() {
                updater.flush_arena(collector, &batch)?;
            }
        }
        ordered_collectors[agg_idx].clear();
    }
    Ok(())
}

pub(crate) fn finalize_ordered_ungrouped(
    spec: &AggregateSpec,
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
        let collector = &state.ordered_collectors[agg_idx];
        if object.order_bys.is_empty() || collector.is_empty() {
            continue;
        }
        let input_count = spec.aggregate_inputs[agg_idx].len();
        let input_types = aggregate_input_types(spec, agg_idx)?;
        let orders = aggregate_orders(spec, agg_idx)?;
        let mut indices: Vec<usize> = (0..collector.row_count()).collect();
        indices.sort_by(|&left, &right| {
            compare_order_values_arena(collector, left, right, orders)
                .then_with(|| left.cmp(&right))
        });

        let allocator = state.arena_allocator.get_allocator().clone();
        let state_offset = state.layout.state_offset(agg_idx);
        let agg_ptr = unsafe { (state.state_buffer.as_mut_ptr() as *mut u8).add(state_offset) };
        let single_input = vec![(0..input_count).collect::<Vec<usize>>()];
        let batch_cap = indices.len().min(VECTOR_SIZE).max(1);
        let mut input_chunk = Chunk::try_initialize(&input_types, batch_cap, allocator.clone())?;
        let mut addresses = Vector::try_new(LogicalType::BigInt, batch_cap, allocator)?;
        let mut distinct_seen = object.is_distinct().then(HashSet::<Box<[Value]>>::new);
        let mut batch = Vec::with_capacity(batch_cap);
        for &row_idx in &indices {
            if let Some(seen) = distinct_seen.as_mut() {
                let key = collector.input_values(row_idx).into();
                if !seen.insert(key) {
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
                collector,
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
                collector,
                &batch,
            )?;
        }
        state.ordered_collectors[agg_idx].clear();
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
    fn flush_arena(
        &mut self,
        collector: &OrderedAggregateCollector,
        batch: &[usize],
    ) -> Result<()> {
        populate_ordered_group_chunk_arena(
            &mut self.groups,
            collector,
            batch,
            self.present_groups,
            self.agg_idx,
        )?;
        populate_ordered_input_chunk_arena(
            &mut self.input_chunk,
            collector,
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
    collector: &OrderedAggregateCollector,
    batch: &[usize],
) -> Result<()> {
    populate_ordered_input_chunk_arena(input_chunk, collector, batch, single_input[0].len(), 0)?;
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

fn populate_ordered_input_chunk_arena(
    chunk: &mut Chunk,
    collector: &OrderedAggregateCollector,
    batch: &[usize],
    input_count: usize,
    agg_idx: usize,
) -> Result<()> {
    chunk.try_set_cardinality(batch.len())?;
    for input_idx in 0..input_count {
        let col = chunk.column_mut(input_idx).ok_or_else(|| {
            paro_error::internal(format!(
                "missing ordered aggregate input column {input_idx} at aggregate {agg_idx}"
            ))
        })?;
        for (out_idx, &row_idx) in batch.iter().enumerate() {
            let inputs = collector.input_values(row_idx);
            col.set_value(out_idx, &inputs[input_idx]);
        }
    }
    Ok(())
}

fn populate_ordered_group_chunk_arena(
    groups: &mut Chunk,
    collector: &OrderedAggregateCollector,
    batch: &[usize],
    present_groups: &[bool],
    agg_idx: usize,
) -> Result<()> {
    groups.try_set_cardinality(batch.len())?;
    for (group_idx, is_present) in present_groups.iter().copied().enumerate() {
        let col = groups.column_mut(group_idx).ok_or_else(|| {
            paro_error::internal(format!(
                "missing ordered aggregate group column {group_idx} at aggregate {agg_idx}"
            ))
        })?;
        if is_present {
            for (out_idx, &row_idx) in batch.iter().enumerate() {
                let gv = collector.group_values(row_idx);
                col.set_value(out_idx, &gv[group_idx]);
            }
        } else {
            col.validity_mut().try_set_all_invalid(batch.len())?;
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

fn projected_distinct_key_arena(
    collector: &OrderedAggregateCollector,
    row: usize,
    present_groups: &[bool],
) -> Box<[Value]> {
    let groups = collector.group_values(row);
    let inputs = collector.input_values(row);
    let mut key = Vec::with_capacity(
        present_groups.iter().filter(|present| **present).count() + inputs.len(),
    );
    for (group_idx, present) in present_groups.iter().copied().enumerate() {
        if present {
            key.push(groups[group_idx].clone());
        }
    }
    key.extend(inputs.iter().cloned());
    key.into_boxed_slice()
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

#[inline]
fn payload_value(payload: &Chunk, col_idx: usize, row_idx: usize) -> Result<Value> {
    payload
        .column(col_idx)
        .map(|col| col.get_value(row_idx))
        .ok_or_else(|| {
            paro_error::internal(format!(
                "ordered aggregate payload column not found: idx={col_idx}"
            ))
        })
}

fn compare_projected_groups_arena(
    collector: &OrderedAggregateCollector,
    left: usize,
    right: usize,
    present_groups: &[bool],
) -> Ordering {
    let left_groups = collector.group_values(left);
    let right_groups = collector.group_values(right);
    for (group_idx, present) in present_groups.iter().copied().enumerate() {
        if !present {
            continue;
        }
        let cmp = compare_values(
            &left_groups[group_idx],
            &right_groups[group_idx],
            true,
            true,
        );
        if cmp != Ordering::Equal {
            return cmp;
        }
    }
    Ordering::Equal
}

fn compare_order_values_arena(
    collector: &OrderedAggregateCollector,
    left: usize,
    right: usize,
    orders: &[OrderByExpression],
) -> Ordering {
    let left_orders = collector.order_values(left);
    let right_orders = collector.order_values(right);
    for (idx, order) in orders.iter().enumerate() {
        let cmp = compare_values(
            &left_orders[idx],
            &right_orders[idx],
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
