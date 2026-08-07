// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! DISTINCT aggregate helpers shared by hash and ungrouped build sinks.

use std::sync::Arc;

use paro_common::allocator::{ArenaAllocator, MemoryTag};
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::hash::hash_u64;
use paro_common::memory::{AccountedVec, MemoryAccountingClass, MemoryAccountingContext};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_common::vector::{SelectionVector, Vector, VECTOR_SIZE};
use paro_function::aggregate::{AggregateCombineType, AggregateInputData, AggregateStateInput};

use crate::operators::aggregate::aggregate_kernel::{
    build_state_vector, update_states, AggregatePayload,
};
use crate::operators::aggregate::aggregate_object::AggregateObject;
use crate::operators::aggregate::aggregate_state::AggregateStateLayout;
use crate::operators::aggregate::build_helpers::{
    build_per_aggregate_filters, fill_repeated_state_addresses, group_types, has_aggregate_filters,
};
use crate::operators::aggregate::distinct_state::{DistinctAggregateState, DistinctKeyTable};
use crate::operators::aggregate::grouped_aggregate_hashtable::{
    GroupedAggregateHashTable, HashTableCapacityHint, SerializedGroupLookup, SerializedSourceRows,
};
use crate::operators::aggregate::radix_partitioned_aggregate_hashtable::{
    AggregateHTScanPosition, AggregateHashTable,
};
use crate::physical::specs::AggregateSpec;
use crate::runtime::breaker::UngroupedAggregateRuntimeState;

const DISTINCT_EAGER_RESERVATION_MEMORY_DIVISOR: usize = 8;

/// Collect unordered DISTINCT inputs into vectorized per-aggregate key tables.
pub(crate) fn collect_distinct_rows(
    spec: &AggregateSpec,
    aggregate_objects: &[AggregateObject],
    payload: &Chunk,
    groups: &Chunk,
    parallelism: usize,
    query_capacity_bytes: usize,
    modifier_memory: &MemoryAccountingContext,
    distinct: &mut DistinctAggregateState,
) -> Result<()> {
    let capacity_hint =
        distinct_capacity_hint(spec, aggregate_objects, parallelism, query_capacity_bytes);
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
        let input_types = aggregate_input_types(spec, input_refs, agg_idx)?;
        let mut key_types = groups.types();
        key_types.extend(input_types);
        let table = distinct.get_or_create(
            agg_idx,
            key_types.clone(),
            groups.column_count(),
            payload.allocator().clone(),
            modifier_memory.clone(),
            parallelism,
            capacity_hint,
        )?;
        let mut vectors = groups.data.clone();
        for &payload_idx in input_refs.iter() {
            vectors.push(Arc::clone(payload.column(payload_idx).ok_or_else(|| {
                paro_error::internal(format!(
                    "distinct payload column not found for aggregate {agg_idx}: index={payload_idx}, column_count={}",
                    payload.column_count()
                ))
            })?));
        }
        let mut keys = Chunk::from_arc_vectors(vectors, payload.allocator().clone());
        keys.try_set_cardinality(payload.size())?;
        if let Some(selection) = filter_selections
            .as_ref()
            .and_then(|filters| filters[agg_idx].as_ref())
        {
            if selection.is_empty() {
                continue;
            }
            keys.try_slice(selection, selection.len())?;
        }
        table.insert(&keys)?;
    }
    Ok(())
}

fn distinct_capacity_hint(
    spec: &AggregateSpec,
    aggregate_objects: &[AggregateObject],
    parallelism: usize,
    query_capacity_bytes: usize,
) -> HashTableCapacityHint {
    let parallelism = parallelism.max(1);
    let distinct_table_count = aggregate_objects
        .iter()
        .filter(|object| object.is_distinct() && object.order_bys.is_empty())
        .count()
        .max(1);
    let expected_rows = spec
        .estimated_input_rows
        .map(|rows| rows.div_ceil(parallelism as u64))
        .and_then(|rows| usize::try_from(rows).ok())
        .unwrap_or(0);
    let max_fixed_bytes = query_capacity_bytes
        .checked_div(DISTINCT_EAGER_RESERVATION_MEMORY_DIVISOR)
        .unwrap_or(0)
        .checked_div(parallelism)
        .unwrap_or(0)
        .checked_div(distinct_table_count)
        .unwrap_or(0);
    HashTableCapacityHint {
        expected_rows,
        max_fixed_bytes,
    }
}

fn aggregate_input_types(
    spec: &AggregateSpec,
    input_refs: &[usize],
    agg_idx: usize,
) -> Result<Vec<LogicalType>> {
    input_refs
        .iter()
        .map(|&column_idx| {
            spec.payload_types.get(column_idx).cloned().ok_or_else(|| {
                paro_error::internal(format!(
                    "distinct input type not found for aggregate {agg_idx}: index={column_idx}, type_count={}",
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

fn project_grouping_set_keys(
    keys: &Chunk,
    group_types: &[LogicalType],
    present_groups: &[bool],
) -> Result<Chunk> {
    let mut projected = keys.clone_referencing_vectors();
    for (group_idx, present) in present_groups.iter().copied().enumerate() {
        if present {
            continue;
        }
        let group_type = group_types.get(group_idx).cloned().ok_or_else(|| {
            paro_error::internal(format!(
                "distinct grouping type not found: group_idx={group_idx}, group_count={}",
                group_types.len()
            ))
        })?;
        projected.data[group_idx] = Arc::new(Vector::try_constant_from_value(
            group_type.clone(),
            Value::Null(group_type),
            keys.size(),
            keys.allocator().clone(),
        )?);
    }
    projected.try_set_cardinality(keys.size())?;
    Ok(projected)
}

fn project_distinct_tables(
    source: &mut DistinctKeyTable,
    group_types: &[LogicalType],
    present_groups: &[Vec<bool>],
    modifier_memory: &MemoryAccountingContext,
) -> Result<Vec<DistinctKeyTable>> {
    let key_types = source.key_types().to_vec();
    let allocator = source.allocator();
    let mut projected = present_groups
        .iter()
        .map(|_| {
            DistinctKeyTable::try_new(
                key_types.clone(),
                group_types.len(),
                allocator.clone(),
                modifier_memory.clone(),
                1,
                HashTableCapacityHint::default(),
            )
        })
        .collect::<Result<Vec<_>>>()?;
    let capacity = source.count().min(VECTOR_SIZE).max(1);
    let mut keys = Chunk::try_initialize(&key_types, capacity, allocator)?;
    let mut position = AggregateHTScanPosition::default();
    while source.scan(&mut position, &mut keys)? {
        for (table, present) in projected.iter_mut().zip(present_groups.iter()) {
            if present.iter().all(|present| *present) {
                table.insert(&keys)?;
            } else {
                table.insert(&project_grouping_set_keys(&keys, group_types, present)?)?;
            }
        }
    }
    Ok(projected)
}

/// Finalize global DISTINCT keys into the main grouped aggregate tables.
pub(crate) fn finalize_distinct_into_tables(
    spec: &AggregateSpec,
    aggregate_objects: &[AggregateObject],
    group_refs: &[usize],
    grouping_sets: &[Box<[usize]>],
    modifier_memory: &MemoryAccountingContext,
    distinct: &mut DistinctAggregateState,
    tables: &mut [AggregateHashTable],
) -> Result<()> {
    let group_count = group_refs.len();
    let group_types = group_types(spec)?;
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
        let Some(mut key_table) = distinct.take_coalesced(agg_idx)? else {
            continue;
        };
        if key_table.count() == 0 {
            continue;
        }
        let input_refs = &spec.aggregate_inputs[agg_idx];
        let input_types = aggregate_input_types(spec, input_refs, agg_idx)?;
        let expected_width = group_count + input_types.len();
        if key_table.key_types().len() != expected_width {
            return Err(paro_error::internal(format!(
                "distinct key width mismatch at aggregate {agg_idx}: expected={expected_width}, actual={}",
                key_table.key_types().len()
            )));
        }
        let full_layout = AggregateStateLayout::new(aggregate_objects)?;
        let single_input = vec![(0..input_types.len()).collect::<Vec<_>>()];
        if grouping_sets.len() == 1 && grouping_sets[0].len() == group_count {
            finalize_distinct_partition_into_table(
                spec,
                aggregate_objects,
                group_refs,
                agg_idx,
                &key_table,
                &mut tables[0],
            )?;
            continue;
        }

        let present_groups = grouping_sets
            .iter()
            .map(|set| grouping_set_present_mask(group_count, set, agg_idx))
            .collect::<Result<Vec<_>>>()?;
        let projected_tables = project_distinct_tables(
            &mut key_table,
            &group_types,
            &present_groups,
            modifier_memory,
        )?;
        for (projected, table) in projected_tables.into_iter().zip(tables.iter_mut()) {
            flush_distinct_table(
                &projected,
                table,
                object,
                &full_layout,
                &single_input,
                &group_types,
                &input_types,
                group_count,
                agg_idx,
            )?;
        }
    }
    Ok(())
}

/// Apply one globally unique DISTINCT-key partition to a regular aggregate
/// table. Callers may execute independent partitions concurrently and combine
/// their resulting regular tables afterwards.
pub(crate) fn finalize_distinct_partition_into_table(
    spec: &AggregateSpec,
    aggregate_objects: &[AggregateObject],
    group_refs: &[usize],
    agg_idx: usize,
    keys: &DistinctKeyTable,
    table: &mut AggregateHashTable,
) -> Result<()> {
    let inputs =
        validate_distinct_finalize_inputs(spec, aggregate_objects, group_refs, agg_idx, keys)?;
    flush_distinct_table(
        keys,
        table,
        inputs.object,
        &inputs.full_layout,
        &inputs.single_input,
        &inputs.group_types,
        &inputs.input_types,
        group_refs.len(),
        agg_idx,
    )
}

fn flush_distinct_table(
    keys: &DistinctKeyTable,
    table: &mut AggregateHashTable,
    object: &AggregateObject,
    full_layout: &AggregateStateLayout,
    single_input: &[Vec<usize>],
    group_types: &[LogicalType],
    input_types: &[LogicalType],
    group_count: usize,
    agg_idx: usize,
) -> Result<()> {
    let mut updater = SerializedDistinctTableUpdater::try_new(
        table,
        object,
        full_layout,
        single_input,
        group_types,
        input_types,
        group_count,
        agg_idx,
        keys.count(),
    )?;
    keys.visit_flat_partitions(|source| updater.flush(source))
}

/// Finalize worker-local DISTINCT fragments without first copying their keys
/// into another hash table.
///
/// Every fragment is already unique internally. A compact hash filter rejects
/// almost all keys that cannot have appeared in an earlier fragment; possible
/// matches are verified against those immutable hash tables, preserving exact
/// SQL semantics even for hash collisions and Bloom false positives.
pub(crate) fn finalize_distinct_fragments_into_table(
    spec: &AggregateSpec,
    aggregate_objects: &[AggregateObject],
    group_refs: &[usize],
    agg_idx: usize,
    fragments: &[DistinctKeyTable],
    modifier_memory: &MemoryAccountingContext,
    table: &mut AggregateHashTable,
) -> Result<()> {
    let mut fragments = fragments
        .iter()
        .filter(|fragment| fragment.count() > 0)
        .collect::<Vec<_>>();
    if fragments.is_empty() {
        return Ok(());
    }
    fragments.sort_unstable_by_key(|fragment| std::cmp::Reverse(fragment.count()));
    let row_upper_bound = fragments.iter().try_fold(0usize, |total, fragment| {
        total
            .checked_add(fragment.count())
            .ok_or_else(|| paro_error::internal("DISTINCT fragment row-count overflow"))
    })?;

    let object = validate_distinct_finalize_inputs(
        spec,
        aggregate_objects,
        group_refs,
        agg_idx,
        fragments[0],
    )?;
    for fragment in fragments.iter().skip(1) {
        validate_distinct_key_width(
            fragment,
            group_refs.len(),
            object.input_types.len(),
            agg_idx,
        )?;
        if fragment.key_types() != fragments[0].key_types() {
            return Err(paro_error::internal(format!(
                "DISTINCT fragment schema mismatch at aggregate {agg_idx}: expected={:?}, actual={:?}",
                fragments[0].key_types(),
                fragment.key_types()
            )));
        }
    }

    let mut updater = SerializedDistinctTableUpdater::try_new(
        table,
        object.object,
        &object.full_layout,
        &object.single_input,
        &object.group_types,
        &object.input_types,
        group_refs.len(),
        agg_idx,
        row_upper_bound,
    )?;
    let mut filter = FragmentHashFilter::try_new(row_upper_bound, modifier_memory)?;
    let mut previous: Vec<&GroupedAggregateHashTable> = Vec::with_capacity(fragments.len());
    for fragment in fragments {
        let source = fragment.flat_partition()?;
        let lookups = previous
            .iter()
            .map(|target| SerializedGroupLookup::try_new(target, source))
            .collect::<Result<Vec<_>>>()?;
        updater.flush_unique(source, &lookups, &mut filter)?;
        previous.push(source);
    }
    Ok(())
}

struct DistinctFinalizeInputs<'a> {
    object: &'a AggregateObject,
    full_layout: AggregateStateLayout,
    single_input: Vec<Vec<usize>>,
    group_types: Vec<LogicalType>,
    input_types: Vec<LogicalType>,
}

fn validate_distinct_finalize_inputs<'a>(
    spec: &AggregateSpec,
    aggregate_objects: &'a [AggregateObject],
    group_refs: &[usize],
    agg_idx: usize,
    keys: &DistinctKeyTable,
) -> Result<DistinctFinalizeInputs<'a>> {
    let object = aggregate_objects.get(agg_idx).ok_or_else(|| {
        paro_error::internal(format!(
            "distinct aggregate object not found: index={agg_idx}, count={}",
            aggregate_objects.len()
        ))
    })?;
    if !object.is_distinct() || !object.order_bys.is_empty() {
        return Err(paro_error::internal(format!(
            "aggregate {agg_idx} is not an unordered DISTINCT aggregate"
        )));
    }
    let input_refs = spec.aggregate_inputs.get(agg_idx).ok_or_else(|| {
        paro_error::internal(format!(
            "distinct aggregate input mapping not found: index={agg_idx}, count={}",
            spec.aggregate_inputs.len()
        ))
    })?;
    let input_types = aggregate_input_types(spec, input_refs, agg_idx)?;
    validate_distinct_key_width(keys, group_refs.len(), input_types.len(), agg_idx)?;
    Ok(DistinctFinalizeInputs {
        object,
        full_layout: AggregateStateLayout::new(aggregate_objects)?,
        single_input: vec![(0..input_types.len()).collect()],
        group_types: group_types(spec)?,
        input_types,
    })
}

fn validate_distinct_key_width(
    keys: &DistinctKeyTable,
    group_count: usize,
    input_count: usize,
    agg_idx: usize,
) -> Result<()> {
    let expected_width = group_count
        .checked_add(input_count)
        .ok_or_else(|| paro_error::internal("DISTINCT key width overflow during finalization"))?;
    if keys.key_types().len() != expected_width {
        return Err(paro_error::internal(format!(
            "distinct key width mismatch at aggregate {agg_idx}: expected={expected_width}, actual={}",
            keys.key_types().len()
        )));
    }
    Ok(())
}

/// Existing full-key hashes form a cheap negative filter for fragmented
/// DISTINCT finalization. Exact source-table probes resolve every positive.
struct FragmentHashFilter {
    words: AccountedVec<u64>,
    bit_mask: usize,
}

impl FragmentHashFilter {
    const BITS_PER_ROW: usize = 8;

    fn try_new(expected_rows: usize, memory: &MemoryAccountingContext) -> Result<Self> {
        let requested_bits = expected_rows
            .max(1)
            .checked_mul(Self::BITS_PER_ROW)
            .ok_or_else(|| paro_error::internal("DISTINCT hash-filter size overflow"))?;
        let bit_count = requested_bits
            .max(u64::BITS as usize)
            .checked_next_power_of_two()
            .ok_or_else(|| paro_error::internal("DISTINCT hash-filter capacity overflow"))?;
        let word_count = bit_count / u64::BITS as usize;
        let mut words = AccountedVec::new_with_accounting(
            memory.with_class(MemoryAccountingClass::Metadata).grant()?,
            MemoryTag::HashTable,
            MemoryAccountingClass::Metadata,
        );
        words.try_resize_with(word_count, || 0)?;
        Ok(Self {
            words,
            bit_mask: bit_count - 1,
        })
    }

    #[inline]
    fn contains(&self, hash: u64) -> bool {
        self.test(hash as usize & self.bit_mask)
            && self.test(hash_u64(hash) as usize & self.bit_mask)
    }

    #[inline]
    fn insert(&mut self, hash: u64) {
        self.set(hash as usize & self.bit_mask);
        self.set(hash_u64(hash) as usize & self.bit_mask);
    }

    #[inline]
    fn test(&self, bit_idx: usize) -> bool {
        self.words[bit_idx / u64::BITS as usize] & (1_u64 << (bit_idx % u64::BITS as usize)) != 0
    }

    #[inline]
    fn set(&mut self, bit_idx: usize) {
        self.words[bit_idx / u64::BITS as usize] |= 1_u64 << (bit_idx % u64::BITS as usize);
    }
}

/// Apply serialized, globally unique DISTINCT rows to one aggregate table.
struct SerializedDistinctTableUpdater<'a> {
    table: &'a mut AggregateHashTable,
    object: &'a AggregateObject,
    full_layout: &'a AggregateStateLayout,
    single_input: &'a [Vec<usize>],
    group_count: usize,
    input_count: usize,
    agg_idx: usize,
    inputs: Chunk,
    run_starts: SelectionVector,
    hashes: Vector,
    addresses: Vector,
    arena: ArenaAllocator,
    input_columns: Vec<usize>,
}

impl<'a> SerializedDistinctTableUpdater<'a> {
    #[allow(clippy::too_many_arguments)]
    fn try_new(
        table: &'a mut AggregateHashTable,
        object: &'a AggregateObject,
        full_layout: &'a AggregateStateLayout,
        single_input: &'a [Vec<usize>],
        group_types: &[LogicalType],
        input_types: &[LogicalType],
        group_count: usize,
        agg_idx: usize,
        row_upper_bound: usize,
    ) -> Result<Self> {
        if group_types.len() != group_count {
            return Err(paro_error::internal(format!(
                "distinct group schema width mismatch at aggregate {agg_idx}: types={}, groups={group_count}",
                group_types.len()
            )));
        }
        let allocator = table.allocator();
        let capacity = row_upper_bound.min(VECTOR_SIZE).max(1);
        Ok(Self {
            table,
            object,
            full_layout,
            single_input,
            group_count,
            input_count: input_types.len(),
            agg_idx,
            inputs: Chunk::try_initialize(input_types, capacity, allocator.clone())?,
            run_starts: SelectionVector::try_with_capacity(capacity, allocator.clone())?,
            hashes: Vector::try_new(LogicalType::UBigInt, capacity, allocator.clone())?,
            addresses: Vector::try_new(LogicalType::BigInt, capacity, allocator.clone())?,
            arena: ArenaAllocator::new(allocator),
            input_columns: (group_count..group_count + input_types.len()).collect(),
        })
    }

    fn flush(&mut self, source: &GroupedAggregateHashTable) -> Result<()> {
        self.validate_source(source)?;
        self.flush_range(source, 0, source.count())
    }

    fn flush_unique(
        &mut self,
        source: &GroupedAggregateHashTable,
        previous: &[SerializedGroupLookup<'_>],
        filter: &mut FragmentHashFilter,
    ) -> Result<()> {
        self.validate_source(source)?;
        if previous.is_empty() {
            self.flush_range(source, 0, source.count())?;
            for row_idx in 0..source.count() {
                filter.insert(source.serialized_group_hash(row_idx)?);
            }
            return Ok(());
        }

        let mut unique_run_start = 0usize;
        for row_idx in 0..source.count() {
            let hash = source.serialized_group_hash(row_idx)?;
            let duplicate = if filter.contains(hash) {
                let mut found = false;
                for lookup in previous {
                    if lookup.contains(row_idx)? {
                        found = true;
                        break;
                    }
                }
                found
            } else {
                false
            };
            filter.insert(hash);
            if duplicate {
                self.flush_range(source, unique_run_start, row_idx - unique_run_start)?;
                unique_run_start = row_idx + 1;
            }
        }
        self.flush_range(source, unique_run_start, source.count() - unique_run_start)
    }

    fn validate_source(&self, source: &GroupedAggregateHashTable) -> Result<()> {
        let expected_width = self.group_count + self.input_count;
        if source.group_types().len() != expected_width {
            return Err(paro_error::internal(format!(
                "distinct key width mismatch while updating aggregate {}: expected={expected_width}, actual={}",
                self.agg_idx,
                source.group_types().len()
            )));
        }
        Ok(())
    }

    fn flush_range(
        &mut self,
        source: &GroupedAggregateHashTable,
        start: usize,
        count: usize,
    ) -> Result<()> {
        let end = start
            .checked_add(count)
            .ok_or_else(|| paro_error::internal("DISTINCT serialized flush range overflow"))?;
        if end > source.count() {
            return Err(paro_error::internal(format!(
                "DISTINCT serialized flush range out of bounds: start={start}, count={count}, rows={}",
                source.count()
            )));
        }
        let mut offset = start;
        while offset < end {
            let count = (end - offset).min(self.inputs.capacity());
            let run_count = source.project_serialized_group_prefix_runs(
                offset,
                count,
                self.group_count,
                &mut self.run_starts,
                &mut self.hashes,
            )?;
            self.table.find_or_create_serialized_group_prefix(
                source,
                SerializedSourceRows::new(offset, self.run_starts.as_slice()),
                &self.hashes,
                &mut self.addresses,
            )?;
            source.gather_serialized_group_columns(
                offset,
                count,
                &self.input_columns,
                &mut self.inputs,
            )?;
            let mut input_data = AggregateInputData::new(
                self.object.bind_info.as_deref(),
                &mut self.arena,
                AggregateCombineType::PreserveInput,
            );
            if let Some(update_runs) = self.object.function.distinct_run_update {
                let states = AggregateStateInput::try_new(
                    &self.addresses,
                    self.full_layout.state_offset(self.agg_idx),
                    None,
                    run_count,
                )?;
                let inputs = self.inputs.data.iter().map(Arc::as_ref).collect::<Vec<_>>();
                unsafe {
                    update_runs(
                        &inputs,
                        &input_data,
                        &states,
                        self.run_starts.as_slice(),
                        count,
                    );
                }
            } else {
                expand_prefix_run_addresses(
                    &mut self.addresses,
                    self.run_starts.as_slice(),
                    run_count,
                    count,
                )?;
                let payload = AggregatePayload {
                    chunk: &self.inputs,
                    aggregate_inputs: self.single_input,
                };
                let states = build_state_vector(
                    &self.addresses,
                    self.full_layout,
                    self.agg_idx,
                    None,
                    count,
                )?;
                update_states(
                    std::slice::from_ref(self.object),
                    &mut input_data,
                    &payload,
                    &states,
                    count,
                )?;
            }
            offset += count;
        }
        Ok(())
    }
}

fn expand_prefix_run_addresses(
    addresses: &mut Vector,
    run_starts: &[u32],
    run_count: usize,
    row_count: usize,
) -> Result<()> {
    if run_starts.len() != run_count || addresses.len() != run_count {
        return Err(paro_error::internal(format!(
            "DISTINCT prefix run lookup size mismatch: starts={}, addresses={}, runs={run_count}",
            run_starts.len(),
            addresses.len()
        )));
    }
    if row_count == 0 {
        if run_count != 0 {
            return Err(paro_error::internal(format!(
                "DISTINCT empty prefix expansion has {run_count} runs"
            )));
        }
        addresses.try_set_count(0)?;
        return Ok(());
    }
    if run_count == 0 || run_starts.first().copied() != Some(0) {
        return Err(paro_error::internal(
            "DISTINCT non-empty prefix expansion must start with row zero",
        ));
    }
    let mut previous = None;
    for &start in run_starts {
        let start = start as usize;
        if start >= row_count || previous.is_some_and(|previous| start <= previous) {
            return Err(paro_error::internal(format!(
                "DISTINCT prefix runs are invalid: starts={run_starts:?}, rows={row_count}"
            )));
        }
        previous = Some(start);
    }

    addresses.try_set_count(row_count)?;
    let data = unsafe { addresses.flat_data_mut::<*mut u8>() };
    // Expand backwards: compact run addresses occupy slots `0..run_count`,
    // while every run starts at or after its compact slot. Processing the last
    // run first prevents expansion from overwriting an unread address.
    for run_idx in (0..run_count).rev() {
        let state = unsafe { *data.add(run_idx) };
        let start = run_starts[run_idx] as usize;
        let end = run_starts
            .get(run_idx + 1)
            .map_or(row_count, |next| *next as usize);
        for row_idx in start..end {
            unsafe {
                *data.add(row_idx) = state;
            }
        }
    }
    Ok(())
}

/// Finalize global ungrouped DISTINCT keys into the single aggregate state.
pub(crate) fn finalize_ungrouped_distinct(
    spec: &AggregateSpec,
    state: &mut UngroupedAggregateRuntimeState,
) -> Result<()> {
    let aggregate_objects = Arc::clone(&state.aggregate_objects);
    let allocator = state.arena_allocator.get_allocator().clone();
    let mut addresses = Vector::try_new(LogicalType::BigInt, VECTOR_SIZE, allocator.clone())?;
    for (agg_idx, object) in aggregate_objects.iter().enumerate() {
        if !object.is_distinct() || !object.order_bys.is_empty() {
            continue;
        }
        let Some(mut keys) = state.distinct.take_coalesced(agg_idx)? else {
            continue;
        };
        if keys.count() == 0 {
            continue;
        }
        let input_refs = &spec.aggregate_inputs[agg_idx];
        let input_types = aggregate_input_types(spec, input_refs, agg_idx)?;
        if keys.key_types() != input_types.as_slice() {
            return Err(paro_error::internal(format!(
                "ungrouped distinct key schema mismatch at aggregate {agg_idx}: expected={input_types:?}, actual={:?}",
                keys.key_types()
            )));
        }
        let state_offset = state.layout.state_offset(agg_idx);
        let aggregate_state =
            unsafe { (state.state_buffer.as_mut_ptr() as *mut u8).add(state_offset) };
        let single_input = vec![(0..input_types.len()).collect::<Vec<_>>()];
        let mut input_chunk = Chunk::try_initialize(
            &input_types,
            keys.count().min(VECTOR_SIZE),
            allocator.clone(),
        )?;
        let mut position = AggregateHTScanPosition::default();
        while keys.scan(&mut position, &mut input_chunk)? {
            fill_repeated_state_addresses(&mut addresses, aggregate_state, input_chunk.size())?;
            let payload = AggregatePayload {
                chunk: &input_chunk,
                aggregate_inputs: &single_input,
            };
            let mut input_data = AggregateInputData::new(
                object.bind_info.as_deref(),
                &mut state.arena_allocator,
                AggregateCombineType::PreserveInput,
            );
            update_states(
                std::slice::from_ref(object),
                &mut input_data,
                &payload,
                &addresses,
                input_chunk.size(),
            )?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefix_run_addresses_expand_in_place_without_clobbering_later_runs() {
        let allocator = paro_common::test_utils::test_allocator();
        let mut addresses =
            Vector::try_new(LogicalType::BigInt, 12, allocator).expect("address vector");
        addresses.try_set_count(3).expect("compact addresses");
        let markers = [11_u8, 22, 33];
        let marker_addresses = markers
            .iter()
            .map(|marker| marker as *const u8 as *mut u8)
            .collect::<Vec<_>>();
        let data = unsafe { addresses.flat_data_mut::<*mut u8>() };
        for (run_idx, &address) in marker_addresses.iter().enumerate() {
            unsafe {
                *data.add(run_idx) = address;
            }
        }

        expand_prefix_run_addresses(&mut addresses, &[0, 1, 10], 3, 12)
            .expect("expand run addresses");

        let actual = addresses.as_slice::<*mut u8>();
        assert_eq!(actual[0], marker_addresses[0]);
        assert!(actual[1..10]
            .iter()
            .all(|&address| address == marker_addresses[1]));
        assert!(actual[10..12]
            .iter()
            .all(|&address| address == marker_addresses[2]));
    }
}
