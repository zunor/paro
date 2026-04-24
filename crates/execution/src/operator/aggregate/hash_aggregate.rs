// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Physical grouped hash aggregate operator.

use std::any::Any;
use std::cmp::Ordering;
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex};

use paro_common::allocator::{Allocator, ArenaAllocator};
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::memory::{
    MemoryAccountingClass, MemoryAccountingContext, MemoryDomain, MemoryOwner,
};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_common::vector::{SelectionVector, Vector};
use paro_function::aggregate::{AggregateCombineType, AggregateInputData};
use paro_storage::buffer::MemoryTag;
use paro_storage::row::{
    RadixPartitionedRows, RadixPartitionedRowsBuilder, RowLayout, RowStore, RowValidityType,
};

use crate::execution_context::ExecutionContext;
use crate::explain::explain_node::format_bound_expression;
use crate::explain::types::ExplainRuntimeStats;
use crate::memory_runtime::{
    LocalExternalMemoryTracker, LocalMemoryGrant, OperatorExternalMemoryTracker,
    OperatorMemoryAccount, QueryMemoryPool, ReclaimStats, Reclaimer, SpillCost,
};
use crate::operator::aggregate::accounted_rows::{
    aggregate_modifier_memory_context, AccountedValueRow, AccountedValueRowSet, AccountedValueRows,
};
use crate::operator::aggregate::aggregate_kernel::{
    update_filtered_states, update_states, AggregatePayload,
};
use crate::operator::aggregate::aggregate_object::{
    create_validated_aggregate_objects, AggregateObject,
};
use crate::operator::aggregate::aggregate_state::AggregateStateLayout;
use crate::operator::aggregate::grouped_aggregate_data::{reference_index, GroupedAggregateData};
use crate::operator::aggregate::grouped_aggregate_hashtable::GroupedAggregateHashTable;
use crate::operator::aggregate::radix_partitioned_aggregate_hashtable::{
    AggregateHTScanPosition, AggregateHashTable,
};
use crate::operator::state::{
    GlobalSinkState, GlobalSourceState, LocalSinkState, LocalSourceState, OperatorSinkCombineInput,
    OperatorSinkInput, OperatorSourceInput,
};
use crate::operator::PhysicalOperator;
use crate::operator_type::PhysicalOperatorType;
use crate::result_type::{SinkCombineResultType, SinkResultType, SourceResultType};

type DistinctRows = Option<AccountedValueRowSet>;
type OrderedRows = AccountedValueRows;
const AGGREGATE_EXTERNAL_RADIX_BITS: usize = 8;
const AGGREGATE_REPARTITION_GROWTH_FACTOR: usize = 2;
const FORCE_EXTERNAL_AGGREGATE_RADIX_BITS_FALLBACK: usize = 2;
const USE_NEW_AGG_SPILL_SETTING: &str = "use_new_agg_spill";
const HASH_AGGREGATE_MEMORY_TAG: MemoryTag = MemoryTag::HashTable;
const HASH_AGGREGATE_MEMORY_CLASS: MemoryAccountingClass = MemoryAccountingClass::Revocable;

pub struct HashAggregate {
    pub aggregate_data: GroupedAggregateData,
    pub aggregate_objects: Vec<AggregateObject>,
    pub child: Arc<dyn PhysicalOperator>,
    pub types: Vec<LogicalType>,
    layout: AggregateStateLayout,
    group_payload_refs: Vec<usize>,
    group_types: Vec<LogicalType>,
    grouping_sets: Vec<Vec<usize>>,
    has_distinct: bool,
    has_ordered: bool,
    has_aggregate_modifiers: bool,
    spill_enabled: bool,
    inline_key_width: Option<usize>,
    radix_partition_bits: Option<usize>,
    spill_payload_types_with_hash: Vec<LogicalType>,
    spill_hash_col_idx: usize,
    shared: Arc<HashAggregateShared>,
    sink_state: Mutex<Option<Arc<dyn GlobalSinkState>>>,
}

impl HashAggregate {
    fn normalize_grouping_sets(
        group_count: usize,
        grouping_sets: &[Vec<usize>],
    ) -> Result<Vec<Vec<usize>>> {
        if grouping_sets.is_empty() {
            return Ok(vec![(0..group_count).collect()]);
        }

        let mut normalized = Vec::with_capacity(grouping_sets.len());
        for set in grouping_sets {
            let mut seen = vec![false; group_count];
            let mut normalized_set = Vec::with_capacity(set.len());
            for &group_idx in set {
                if group_idx >= group_count {
                    return Err(paro_error::internal(format!(
                        "Grouping set index out of bounds: group_idx={group_idx}, group_count={group_count}"
                    )));
                }
                if !seen[group_idx] {
                    seen[group_idx] = true;
                    normalized_set.push(group_idx);
                }
            }
            normalized.push(normalized_set);
        }
        Ok(normalized)
    }

    pub fn new(
        aggregate_data: GroupedAggregateData,
        types: Vec<LogicalType>,
        child: Arc<dyn PhysicalOperator>,
    ) -> Result<Self> {
        let aggregate_objects = create_validated_aggregate_objects(&aggregate_data)?;
        let mut group_payload_refs = Vec::with_capacity(aggregate_data.groups.len());
        let mut group_types = Vec::with_capacity(aggregate_data.groups.len());
        for group_expr in &aggregate_data.groups {
            group_payload_refs.push(reference_index(group_expr)?);
            group_types.push(group_expr.return_type());
        }
        let grouping_sets = Self::normalize_grouping_sets(
            aggregate_data.groups.len(),
            &aggregate_data.grouping_sets,
        )?;
        let inline_key_width = GroupedAggregateHashTable::inline_key_width_for_types(&group_types);

        let expected_types = group_payload_refs.len()
            + aggregate_objects.len()
            + aggregate_data.grouping_functions.len();
        if types.len() != expected_types {
            return Err(paro_error::internal(format!(
                "HashAggregate output type mismatch: expected={expected_types}, actual={}",
                types.len()
            )));
        }

        let has_distinct = aggregate_objects.iter().any(AggregateObject::is_distinct);
        let has_ordered = aggregate_objects
            .iter()
            .any(|object| !object.order_bys.is_empty());
        for (agg_idx, object) in aggregate_objects.iter().enumerate() {
            if object.is_distinct() && !object.order_bys.is_empty() {
                return Err(paro_error::not_implemented(format!(
                    "DISTINCT ordered aggregate is not implemented yet: agg_idx={agg_idx}"
                )));
            }
        }
        let has_aggregate_modifiers = aggregate_objects.iter().any(|object| {
            object.filter.is_some() || object.is_distinct() || !object.order_bys.is_empty()
        });
        let spill_enabled = !has_aggregate_modifiers;
        let radix_partition_bits =
            if should_use_radix_partitioning(&group_types, &grouping_sets, has_aggregate_modifiers)
            {
                Some(2)
            } else {
                None
            };
        let layout = AggregateStateLayout::new(&aggregate_objects)?;
        let spill_hash_col_idx = aggregate_data.payload_types.len();
        let mut spill_payload_types_with_hash = aggregate_data.payload_types.clone();
        spill_payload_types_with_hash.push(LogicalType::UBigInt);
        let grouping_count = grouping_sets.len();

        Ok(Self {
            aggregate_data,
            aggregate_objects,
            child,
            types,
            layout,
            group_payload_refs,
            group_types,
            grouping_sets,
            has_distinct,
            has_ordered,
            has_aggregate_modifiers,
            spill_enabled,
            inline_key_width,
            radix_partition_bits,
            spill_payload_types_with_hash,
            spill_hash_col_idx,
            shared: Arc::new(HashAggregateShared::new(grouping_count)),
            sink_state: Mutex::new(None),
        })
    }

    pub fn runtime_memory_stats(&self) -> ExplainRuntimeStats {
        if !self.shared.has_runtime_execution() {
            return ExplainRuntimeStats::default();
        }
        let mut peak_memory_bytes = 0u64;
        if let Some(sink_state) = self.sink_state() {
            if let Some(state) = sink_state
                .as_any()
                .downcast_ref::<HashAggregateGlobalState>()
            {
                peak_memory_bytes = state.source_memory.peak_bytes().unwrap_or(0) as u64;
            }
        }
        ExplainRuntimeStats {
            spilled: Some(self.shared.externalized()),
            peak_memory_bytes: Some(peak_memory_bytes),
            temp_storage_bytes: None,
            ..Default::default()
        }
    }

    pub fn inline_key_width(&self) -> Option<usize> {
        self.inline_key_width
    }

    pub fn radix_partition_count(&self) -> Option<usize> {
        self.radix_partition_bits.map(|bits| 1usize << bits)
    }

    fn build_groups_chunk(&self, payload: &Chunk) -> Result<Chunk> {
        if self.group_payload_refs.is_empty() {
            let mut groups = Chunk::try_init_empty(&[], payload.allocator().clone())?;
            groups.set_cardinality(payload.size());
            return Ok(groups);
        }

        let mut group_vectors = Vec::with_capacity(self.group_payload_refs.len());
        for (group_idx, payload_idx) in self.group_payload_refs.iter().enumerate() {
            let group_vector = Arc::clone(payload.column(*payload_idx).ok_or_else(|| {
                paro_error::internal(format!(
                    "Group payload column not found: group_idx={group_idx}, payload_idx={payload_idx}"
                ))
            })?);
            group_vectors.push(group_vector);
        }
        let mut groups = Chunk::from_arc_vectors(group_vectors, payload.allocator().clone());
        groups.set_cardinality(payload.size());
        Ok(groups)
    }

    fn build_groups_chunk_for_set(
        &self,
        all_groups: &Chunk,
        grouping_set: &[usize],
    ) -> Result<Chunk> {
        if all_groups.column_count() != self.group_types.len() {
            return Err(paro_error::internal(format!(
                "Grouping chunk width mismatch: expected={}, actual={}",
                self.group_types.len(),
                all_groups.column_count()
            )));
        }
        if grouping_set.len() == self.group_types.len() {
            return Ok(all_groups.clone());
        }

        let mut present = vec![false; self.group_types.len()];
        for &group_idx in grouping_set {
            if group_idx >= self.group_types.len() {
                return Err(paro_error::internal(format!(
                    "Grouping set index out of bounds while building groups chunk: group_idx={group_idx}, group_count={}",
                    self.group_types.len()
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
            let group_column = groups.column_mut(group_idx).ok_or_else(|| {
                paro_error::internal(format!(
                    "Missing group column while applying grouping set: group_idx={group_idx}"
                ))
            })?;
            for row_idx in 0..row_count {
                group_column.set_null(row_idx, true);
            }
        }
        Ok(groups)
    }

    fn grouping_value(grouping_set: &[usize], grouping_function: &[usize]) -> i64 {
        let mut value = 0i64;
        for (arg_idx, &group_idx) in grouping_function.iter().enumerate() {
            if !grouping_set.contains(&group_idx) {
                let bit = (grouping_function.len() - 1 - arg_idx) as i64;
                value |= 1_i64 << bit;
            }
        }
        value
    }

    fn populate_grouping_columns(&self, chunk: &mut Chunk, grouping_set: &[usize]) -> Result<()> {
        if self.aggregate_data.grouping_functions.is_empty() || chunk.size() == 0 {
            return Ok(());
        }
        let group_count = self.group_types.len();
        let aggregate_count = self.aggregate_objects.len();
        let grouping_offset = group_count + aggregate_count;
        let row_count = chunk.size();
        for (func_idx, grouping_fn) in self.aggregate_data.grouping_functions.iter().enumerate() {
            let output_idx = grouping_offset + func_idx;
            let grouping_col = chunk.column_mut(output_idx).ok_or_else(|| {
                paro_error::internal(format!(
                    "Missing GROUPING() output column at index {output_idx}"
                ))
            })?;
            let grouping_value = Value::BigInt(Self::grouping_value(grouping_set, grouping_fn));
            for row_idx in 0..row_count {
                grouping_col.set_value(row_idx, &grouping_value);
            }
        }
        Ok(())
    }

    fn build_filter_selection(filter_vec: &Vector, row_count: usize) -> Result<SelectionVector> {
        let filter_format = filter_vec.try_decode(row_count)?;
        let filter_data = filter_format.get_data::<bool>();
        let mut selected_rows = Vec::with_capacity(row_count);
        for row_idx in 0..row_count {
            let physical_idx = filter_format.sel().get(row_idx);
            if !filter_format.validity().is_valid(physical_idx) {
                continue;
            }
            let passed = unsafe { *filter_data.add(physical_idx) };
            if passed {
                selected_rows.push(row_idx as u32);
            }
        }
        SelectionVector::try_from_indices(selected_rows, filter_vec.allocator().clone())
    }

    fn compare_order_values(
        lhs: &Value,
        rhs: &Value,
        ascending: bool,
        nulls_first: bool,
    ) -> Ordering {
        let lhs_null = matches!(lhs, Value::Null(_));
        let rhs_null = matches!(rhs, Value::Null(_));
        match (lhs_null, rhs_null) {
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
                let cmp = lhs.partial_cmp(rhs).unwrap_or(Ordering::Equal);
                if ascending {
                    cmp
                } else {
                    cmp.reverse()
                }
            }
        }
    }

    fn filter_selection_for_aggregate(
        &self,
        agg_idx: usize,
        payload: &Chunk,
    ) -> Result<Option<SelectionVector>> {
        let aggregate = self.aggregate_objects.get(agg_idx).ok_or_else(|| {
            paro_error::internal(format!("Aggregate index out of bounds: {agg_idx}"))
        })?;
        let filter_ref = self
            .aggregate_data
            .aggregate_filters
            .get(agg_idx)
            .ok_or_else(|| {
                paro_error::internal(format!(
                    "Aggregate filter mapping index out of bounds: {agg_idx}"
                ))
            })?;
        if *filter_ref != aggregate.filter {
            return Err(paro_error::internal(format!(
                "Aggregate filter mismatch at index {agg_idx}: object={:?} plan={:?}",
                aggregate.filter, filter_ref
            )));
        }
        let Some(filter_idx) = *filter_ref else {
            return Ok(None);
        };
        let filter_vec = payload.column(filter_idx).ok_or_else(|| {
            paro_error::internal(format!(
                "Aggregate filter payload column not found: agg_idx={agg_idx}, payload_idx={filter_idx}"
            ))
        })?;
        if filter_vec.logical_type() != &LogicalType::Boolean {
            return Err(paro_error::internal(format!(
                "Aggregate filter payload type mismatch at index {agg_idx}: expected=BOOLEAN actual={:?}",
                filter_vec.logical_type()
            )));
        }
        Ok(Some(Self::build_filter_selection(
            filter_vec,
            payload.size(),
        )?))
    }

    fn collect_distinct_rows_for_aggregate(
        &self,
        agg_idx: usize,
        groups: &Chunk,
        payload: &Chunk,
        distinct_rows: &mut DistinctRows,
        modifier_memory: &MemoryAccountingContext,
    ) -> Result<()> {
        let aggregate = self.aggregate_objects.get(agg_idx).ok_or_else(|| {
            paro_error::internal(format!("Aggregate index out of bounds: {agg_idx}"))
        })?;
        if !aggregate.is_distinct() {
            return Ok(());
        }
        let input_refs = self
            .aggregate_data
            .aggregate_inputs
            .get(agg_idx)
            .ok_or_else(|| {
                paro_error::internal(format!(
                    "Aggregate payload mapping index out of bounds: {agg_idx}"
                ))
            })?;
        let group_columns = (0..self.group_types.len())
            .map(|group_idx| {
                groups.column(group_idx).ok_or_else(|| {
                    paro_error::internal(format!(
                        "Group chunk column not found for DISTINCT key: group_idx={group_idx}"
                    ))
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let input_columns = input_refs
            .iter()
            .enumerate()
            .map(|(arg_idx, payload_idx)| {
                payload.column(*payload_idx).ok_or_else(|| {
                    paro_error::internal(format!(
                        "Aggregate payload column not found: agg_idx={agg_idx}, arg_idx={arg_idx}, payload_idx={payload_idx}"
                    ))
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let filter_selection = self.filter_selection_for_aggregate(agg_idx, payload)?;
        let seen =
            distinct_rows.get_or_insert_with(|| AccountedValueRowSet::new(modifier_memory.clone()));

        let mut append_row = |row_idx: usize| {
            let mut distinct_key =
                Vec::with_capacity(group_columns.len().saturating_add(input_columns.len()));
            for group_vec in &group_columns {
                distinct_key.push(group_vec.get_value(row_idx));
            }
            for input_vec in &input_columns {
                distinct_key.push(input_vec.get_value(row_idx));
            }
            seen.insert(distinct_key)?;
            Ok::<(), paro_common::memory::MemoryError>(())
        };

        if let Some(selection) = filter_selection {
            for idx in 0..selection.len() {
                append_row(selection.get(idx))?;
            }
        } else {
            for row_idx in 0..payload.size() {
                append_row(row_idx)?;
            }
        }

        Ok(())
    }

    fn collect_ordered_rows_for_aggregate(
        &self,
        agg_idx: usize,
        groups: &Chunk,
        payload: &Chunk,
        ordered_rows: &mut OrderedRows,
    ) -> Result<()> {
        let aggregate = self.aggregate_objects.get(agg_idx).ok_or_else(|| {
            paro_error::internal(format!("Aggregate index out of bounds: {agg_idx}"))
        })?;
        if aggregate.order_bys.is_empty() {
            return Ok(());
        }
        let input_refs = self
            .aggregate_data
            .aggregate_inputs
            .get(agg_idx)
            .ok_or_else(|| {
                paro_error::internal(format!(
                    "Aggregate payload mapping index out of bounds: {agg_idx}"
                ))
            })?;
        let order_refs = self
            .aggregate_data
            .aggregate_orders
            .get(agg_idx)
            .ok_or_else(|| {
                paro_error::internal(format!(
                    "Aggregate ORDER BY payload mapping index out of bounds: {agg_idx}"
                ))
            })?;
        if order_refs != &aggregate.order_bys {
            return Err(paro_error::internal(format!(
                "Aggregate ORDER BY mapping mismatch at index {agg_idx}: object={:?} plan={:?}",
                aggregate.order_bys, order_refs
            )));
        }

        let group_columns = (0..self.group_types.len())
            .map(|group_idx| {
                groups.column(group_idx).ok_or_else(|| {
                    paro_error::internal(format!(
                        "Group chunk column not found for ordered aggregate: group_idx={group_idx}"
                    ))
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let input_columns = input_refs
            .iter()
            .enumerate()
            .map(|(arg_idx, payload_idx)| {
                payload.column(*payload_idx).ok_or_else(|| {
                    paro_error::internal(format!(
                        "Aggregate payload column not found: agg_idx={agg_idx}, arg_idx={arg_idx}, payload_idx={payload_idx}"
                    ))
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let order_columns = order_refs
            .iter()
            .enumerate()
            .map(|(order_idx, payload_idx)| {
                payload.column(*payload_idx).ok_or_else(|| {
                    paro_error::internal(format!(
                        "Ordered aggregate ORDER BY payload column not found: agg_idx={agg_idx}, order_idx={order_idx}, payload_idx={payload_idx}"
                    ))
                })
            })
            .collect::<Result<Vec<_>>>()?;

        let filter_selection = self.filter_selection_for_aggregate(agg_idx, payload)?;
        let expected_width = group_columns.len() + input_columns.len() + order_columns.len();
        let mut append_row = |row_idx: usize| {
            let mut row_values = Vec::with_capacity(expected_width);
            for group in &group_columns {
                row_values.push(group.get_value(row_idx));
            }
            for input in &input_columns {
                row_values.push(input.get_value(row_idx));
            }
            for order in &order_columns {
                row_values.push(order.get_value(row_idx));
            }
            ordered_rows.push(row_values)?;
            Ok::<(), paro_common::memory::MemoryError>(())
        };

        if let Some(selection) = filter_selection {
            for idx in 0..selection.len() {
                append_row(selection.get(idx))?;
            }
        } else {
            for row_idx in 0..payload.size() {
                append_row(row_idx)?;
            }
        }
        Ok(())
    }

    fn update_non_distinct_aggregate(
        &self,
        agg_idx: usize,
        payload: &Chunk,
        aggregate_states: &Vector,
        filter_selection: Option<&SelectionVector>,
        arena: &mut ArenaAllocator,
    ) -> Result<()> {
        let aggregate = self.aggregate_objects.get(agg_idx).ok_or_else(|| {
            paro_error::internal(format!("Aggregate index out of bounds: {agg_idx}"))
        })?;
        let aggregate_inputs = self
            .aggregate_data
            .aggregate_inputs
            .get(agg_idx)
            .ok_or_else(|| {
                paro_error::internal(format!(
                    "Aggregate payload mapping index out of bounds: {agg_idx}"
                ))
            })?;
        let payload_desc = AggregatePayload {
            chunk: payload,
            aggregate_inputs: std::slice::from_ref(aggregate_inputs),
        };
        let mut input_data = AggregateInputData::new(
            aggregate.bind_info.as_deref(),
            arena,
            AggregateCombineType::PreserveInput,
        );

        if let Some(selection) = filter_selection {
            if selection.is_empty() {
                return Ok(());
            }
            update_filtered_states(
                std::slice::from_ref(aggregate),
                &mut input_data,
                &payload_desc,
                aggregate_states,
                selection,
                selection.len(),
            )
        } else {
            update_states(
                std::slice::from_ref(aggregate),
                &mut input_data,
                &payload_desc,
                aggregate_states,
                payload.size(),
            )
        }
    }

    fn update_aggregates_with_modifiers(
        &self,
        payload: &Chunk,
        groups: &Chunk,
        addresses: &Vector,
        distinct_rows: &mut [DistinctRows],
        ordered_rows: &mut [OrderedRows],
        modifier_memory: &MemoryAccountingContext,
        arena: &mut ArenaAllocator,
    ) -> Result<()> {
        let all_rows =
            SelectionVector::try_incremental(payload.size(), payload.allocator().clone())?;
        for agg_idx in 0..self.aggregate_objects.len() {
            let aggregate = self.aggregate_objects.get(agg_idx).ok_or_else(|| {
                paro_error::internal(format!("Aggregate index out of bounds: {agg_idx}"))
            })?;
            if !aggregate.order_bys.is_empty() {
                self.collect_ordered_rows_for_aggregate(
                    agg_idx,
                    groups,
                    payload,
                    &mut ordered_rows[agg_idx],
                )?;
                continue;
            }
            if aggregate.is_distinct() {
                self.collect_distinct_rows_for_aggregate(
                    agg_idx,
                    groups,
                    payload,
                    &mut distinct_rows[agg_idx],
                    modifier_memory,
                )?;
                continue;
            }

            let state_addresses = self.selected_state_addresses(addresses, &all_rows, agg_idx)?;
            let filter_selection = self.filter_selection_for_aggregate(agg_idx, payload)?;
            self.update_non_distinct_aggregate(
                agg_idx,
                payload,
                &state_addresses,
                filter_selection.as_ref(),
                arena,
            )?;
        }
        Ok(())
    }

    fn finalize_ordered_aggregates(
        &self,
        lstate: &mut HashAggregateGroupingLocalState,
    ) -> Result<()> {
        if lstate.ordered_finalized {
            return Ok(());
        }

        let mut arena = ArenaAllocator::new(lstate.hash_table.allocator());
        let group_count = self.group_types.len();

        for agg_idx in 0..self.aggregate_objects.len() {
            let aggregate = &self.aggregate_objects[agg_idx];
            if aggregate.order_bys.is_empty() {
                continue;
            }

            let mut rows = lstate.ordered_rows[agg_idx].take();
            if rows.is_empty() {
                continue;
            }

            let input_count = self.aggregate_data.aggregate_inputs[agg_idx].len();
            let order_count = self.aggregate_data.aggregate_orders[agg_idx].len();
            if order_count != aggregate.order_bys.len() {
                return Err(paro_error::internal(format!(
                    "Ordered aggregate ORDER BY width mismatch at index {agg_idx}: object={} plan={order_count}",
                    aggregate.order_bys.len()
                )));
            }
            let expected_len = group_count + input_count + order_count;
            for row in &rows {
                if row.len() != expected_len {
                    return Err(paro_error::internal(format!(
                        "Ordered row width mismatch at aggregate {agg_idx}: expected={expected_len}, actual={}",
                        row.len()
                    )));
                }
            }

            let order_bys = self
                .aggregate_data
                .aggregate_expr(agg_idx)?
                .order_bys
                .clone();
            rows.sort_by(|lhs, rhs| {
                for (order_idx, order_by) in order_bys.iter().enumerate() {
                    let lhs_value = &lhs[group_count + input_count + order_idx];
                    let rhs_value = &rhs[group_count + input_count + order_idx];
                    let cmp = Self::compare_order_values(
                        lhs_value,
                        rhs_value,
                        order_by.ascending,
                        order_by.nulls_first,
                    );
                    if cmp != Ordering::Equal {
                        return cmp;
                    }
                }
                Ordering::Equal
            });

            let mut groups = Chunk::try_initialize(
                &self.group_types,
                rows.len(),
                lstate.hash_table.allocator(),
            )?;
            groups.set_cardinality(rows.len());
            for group_idx in 0..group_count {
                let group_col = groups.column_mut(group_idx).ok_or_else(|| {
                    paro_error::internal(format!(
                        "Missing group column {group_idx} while finalizing ordered aggregate {agg_idx}"
                    ))
                })?;
                for (row_idx, row) in rows.iter().enumerate() {
                    group_col.set_value(row_idx, &row[group_idx]);
                }
            }

            let input_types = self
                .aggregate_data
                .aggregate_expr(agg_idx)?
                .children
                .iter()
                .map(|child| child.return_type())
                .collect::<Vec<_>>();
            let mut input_chunk =
                Chunk::try_initialize(&input_types, rows.len(), lstate.hash_table.allocator())?;
            input_chunk.set_cardinality(rows.len());
            for input_idx in 0..input_count {
                let input_col = input_chunk.column_mut(input_idx).ok_or_else(|| {
                    paro_error::internal(format!(
                        "Missing input column {input_idx} while finalizing ordered aggregate {agg_idx}"
                    ))
                })?;
                for (row_idx, row) in rows.iter().enumerate() {
                    input_col.set_value(row_idx, &row[group_count + input_idx]);
                }
            }

            let hashes = lstate.hash_table.hash_groups(&groups)?;
            let mut addresses = Vector::try_new(
                LogicalType::BigInt,
                rows.len(),
                lstate.hash_table.allocator(),
            )?;
            let mut new_groups =
                SelectionVector::try_with_capacity(rows.len(), lstate.hash_table.allocator())?;
            lstate.hash_table.find_or_create_groups(
                &groups,
                &hashes,
                &mut addresses,
                &mut new_groups,
            )?;

            let selection =
                SelectionVector::try_incremental(rows.len(), lstate.hash_table.allocator())?;
            let selected_states = self.selected_state_addresses(&addresses, &selection, agg_idx)?;
            let input_refs = (0..input_count)
                .map(|input_idx| {
                    input_chunk
                        .column(input_idx)
                        .ok_or_else(|| {
                            paro_error::internal(format!(
                                "Missing finalized ordered input column {input_idx} for aggregate {agg_idx}"
                            ))
                        })
                        .map(|v| v.as_ref())
                })
                .collect::<Result<Vec<_>>>()?;
            let input_data = AggregateInputData::new(
                aggregate.bind_info.as_deref(),
                &mut arena,
                AggregateCombineType::PreserveInput,
            );
            unsafe {
                (aggregate.function.update)(&input_refs, &input_data, &selected_states, rows.len());
            }
        }

        lstate.ordered_finalized = true;
        Ok(())
    }

    fn finalize_distinct_aggregates(
        &self,
        lstate: &mut HashAggregateGroupingLocalState,
    ) -> Result<()> {
        if lstate.distinct_finalized {
            return Ok(());
        }

        let mut arena = ArenaAllocator::new(lstate.hash_table.allocator());
        let group_count = self.group_types.len();

        for agg_idx in 0..self.aggregate_objects.len() {
            let aggregate = &self.aggregate_objects[agg_idx];
            if !aggregate.is_distinct() {
                continue;
            }
            let distinct_rows = lstate.distinct_rows[agg_idx].take();
            let Some(rows) = distinct_rows else {
                continue;
            };
            if rows.is_empty() {
                continue;
            }

            let row_values = rows
                .into_rows()
                .into_iter()
                .map(AccountedValueRow::into_values)
                .collect::<Vec<_>>();
            let input_count = self.aggregate_data.aggregate_inputs[agg_idx].len();
            let expected_len = group_count + input_count;
            for row in &row_values {
                if row.len() != expected_len {
                    return Err(paro_error::internal(format!(
                        "Distinct row width mismatch at aggregate {agg_idx}: expected={expected_len}, actual={}",
                        row.len()
                    )));
                }
            }

            let mut groups = Chunk::try_initialize(
                &self.group_types,
                row_values.len(),
                lstate.hash_table.allocator(),
            )?;
            groups.set_cardinality(row_values.len());
            for group_idx in 0..group_count {
                let group_col = groups.column_mut(group_idx).ok_or_else(|| {
                    paro_error::internal(format!(
                        "Missing group column {group_idx} while finalizing DISTINCT aggregate {agg_idx}"
                    ))
                })?;
                for (row_idx, row) in row_values.iter().enumerate() {
                    group_col.set_value(row_idx, &row[group_idx]);
                }
            }

            let input_types = self
                .aggregate_data
                .aggregate_expr(agg_idx)?
                .children
                .iter()
                .map(|child| child.return_type())
                .collect::<Vec<_>>();
            let mut input_chunk = Chunk::try_initialize(
                &input_types,
                row_values.len(),
                lstate.hash_table.allocator(),
            )?;
            input_chunk.set_cardinality(row_values.len());
            for input_idx in 0..input_count {
                let input_col = input_chunk.column_mut(input_idx).ok_or_else(|| {
                    paro_error::internal(format!(
                        "Missing input column {input_idx} while finalizing DISTINCT aggregate {agg_idx}"
                    ))
                })?;
                for (row_idx, row) in row_values.iter().enumerate() {
                    input_col.set_value(row_idx, &row[group_count + input_idx]);
                }
            }

            let hashes = lstate.hash_table.hash_groups(&groups)?;
            let mut addresses = Vector::try_new(
                LogicalType::BigInt,
                row_values.len(),
                lstate.hash_table.allocator(),
            )?;
            let mut new_groups = SelectionVector::try_with_capacity(
                row_values.len(),
                lstate.hash_table.allocator(),
            )?;
            lstate.hash_table.find_or_create_groups(
                &groups,
                &hashes,
                &mut addresses,
                &mut new_groups,
            )?;

            let selection =
                SelectionVector::try_incremental(row_values.len(), lstate.hash_table.allocator())?;
            let selected_states = self.selected_state_addresses(&addresses, &selection, agg_idx)?;
            let input_refs = (0..input_count)
                .map(|input_idx| {
                    input_chunk
                        .column(input_idx)
                        .ok_or_else(|| {
                            paro_error::internal(format!(
                                "Missing finalized DISTINCT input column {input_idx} for aggregate {agg_idx}"
                            ))
                        })
                        .map(|v| v.as_ref())
                })
                .collect::<Result<Vec<_>>>()?;
            let input_data = AggregateInputData::new(
                aggregate.bind_info.as_deref(),
                &mut arena,
                AggregateCombineType::PreserveInput,
            );
            unsafe {
                (aggregate.function.update)(
                    &input_refs,
                    &input_data,
                    &selected_states,
                    row_values.len(),
                );
            }
        }

        lstate.distinct_finalized = true;
        Ok(())
    }

    fn selected_state_addresses(
        &self,
        addresses: &Vector,
        selection: &SelectionVector,
        agg_idx: usize,
    ) -> Result<Vector> {
        if agg_idx >= self.layout.aggregate_count() {
            return Err(paro_error::internal(format!(
                "Aggregate index out of bounds for state layout: agg_idx={agg_idx}, count={}",
                self.layout.aggregate_count()
            )));
        }
        let state_offset = self.layout.state_offset(agg_idx);

        let mut selected = Vector::try_new(
            LogicalType::BigInt,
            selection.len(),
            addresses.allocator().clone(),
        )?;
        selected.set_count(selection.len());
        let selected_data = unsafe { selected.flat_data_mut::<*mut u8>() };

        let address_format = addresses.try_decode(addresses.len())?;
        let address_data = address_format.get_data::<*mut u8>();
        for idx in 0..selection.len() {
            let row_idx = selection.get(idx);
            if row_idx >= addresses.len() {
                return Err(paro_error::internal(format!(
                    "Address selection index out of bounds: row_idx={row_idx}, addresses={}",
                    addresses.len()
                )));
            }
            let physical_idx = address_format.sel().get(row_idx);
            if !address_format.validity().is_valid(physical_idx) {
                return Err(paro_error::internal(format!(
                    "Address vector contains NULL at selected row {row_idx}"
                )));
            }
            let state_ptr = unsafe { *address_data.add(physical_idx) };
            if state_ptr.is_null() {
                return Err(paro_error::internal(format!(
                    "Address vector contains NULL pointer at selected row {row_idx}"
                )));
            }
            unsafe {
                *selected_data.add(idx) = state_ptr.add(state_offset);
            }
        }
        Ok(selected)
    }

    fn parse_bool_setting(value: &Value) -> Option<bool> {
        match value {
            Value::Boolean(v) => Some(*v),
            Value::Varchar(v) => {
                let normalized = v.trim().to_ascii_lowercase();
                match normalized.as_str() {
                    "true" | "on" | "1" | "yes" => Some(true),
                    "false" | "off" | "0" | "no" => Some(false),
                    _ => None,
                }
            }
            _ => None,
        }
    }

    fn use_new_agg_spill_enabled(&self, ctx: &ExecutionContext) -> bool {
        ctx.session
            .get_setting(USE_NEW_AGG_SPILL_SETTING)
            .and_then(Self::parse_bool_setting)
            .unwrap_or(true)
    }

    fn force_external_spill_radix_bits(&self) -> usize {
        self.radix_partition_bits
            .unwrap_or(FORCE_EXTERNAL_AGGREGATE_RADIX_BITS_FALLBACK)
            .max(1)
    }

    fn create_external_spill_state(
        &self,
        ctx: &ExecutionContext,
        radix_bits: usize,
    ) -> Result<HashAggregateExternalSinkState> {
        let layout = Arc::new(RowLayout::from_types(
            self.spill_payload_types_with_hash.clone(),
            RowValidityType::CanHaveNullValues,
        ));
        let data = RadixPartitionedRowsBuilder::new(
            Arc::clone(ctx.buffer_pool()),
            layout,
            MemoryTag::HashTable,
            radix_bits,
            self.spill_hash_col_idx,
        )?;
        Ok(HashAggregateExternalSinkState { data })
    }

    fn build_spill_chunk(&self, payload: &Chunk, hashes: &Vector) -> Result<Chunk> {
        if payload.column_count() != self.aggregate_data.payload_types.len() {
            return Err(paro_error::internal(format!(
                "Aggregate spill payload width mismatch: expected={}, actual={}",
                self.aggregate_data.payload_types.len(),
                payload.column_count()
            )));
        }
        if hashes.logical_type() != &LogicalType::UBigInt {
            return Err(paro_error::internal(format!(
                "Aggregate spill hash type mismatch: expected=UBigInt actual={:?}",
                hashes.logical_type()
            )));
        }
        if hashes.len() < payload.size() {
            return Err(paro_error::internal(format!(
                "Aggregate spill hash vector too small: rows={} hashes={}",
                payload.size(),
                hashes.len()
            )));
        }

        let mut vectors = Vec::with_capacity(payload.column_count() + 1);
        for col_idx in 0..payload.column_count() {
            let column = payload.column(col_idx).ok_or_else(|| {
                paro_error::internal(format!(
                    "Missing payload column while building spill chunk: column_idx={col_idx}"
                ))
            })?;
            vectors.push(Arc::clone(column));
        }
        vectors.push(Arc::new(hashes.clone()));

        let mut spill_chunk = Chunk::from_arc_vectors(vectors, payload.allocator().clone());
        spill_chunk.set_cardinality(payload.size());
        Ok(spill_chunk)
    }

    fn total_local_hash_table_memory_usage(
        &self,
        grouping_states: &[HashAggregateGroupingLocalState],
    ) -> usize {
        grouping_states
            .iter()
            .map(|state| {
                state
                    .hash_table
                    .external_accounted_memory_usage()
                    .saturating_add(
                        state
                            .external_state
                            .as_ref()
                            .map(|external| external.data.size_in_bytes())
                            .unwrap_or(0),
                    )
            })
            .sum()
    }

    fn update_memory_tracker(
        &self,
        grouping_states: &[HashAggregateGroupingLocalState],
        memory: &mut LocalExternalMemoryTracker,
    ) -> Result<()> {
        let usage = self.total_local_hash_table_memory_usage(grouping_states);
        Ok(memory.set_accounted_bytes(usage)?)
    }

    fn maybe_externalize_grouping_state(
        &self,
        ctx: &ExecutionContext,
        memory: &mut LocalExternalMemoryTracker,
        grouping_state: &mut HashAggregateGroupingLocalState,
        use_new_agg_spill: bool,
    ) -> Result<bool> {
        if !use_new_agg_spill || !self.spill_enabled || self.has_aggregate_modifiers {
            return Ok(false);
        }
        if grouping_state.external_state.is_some() {
            return Ok(true);
        }

        let force_external = ctx.force_external();
        let has_temporary_directory = ctx.has_temporary_directory();
        if force_external {
            if !has_temporary_directory {
                return Err(paro_error::invalid_input(
                    "force_external requires a temporary directory (SET temp_directory)"
                        .to_string(),
                ));
            }
            // Keep force_external spill fanout small so parallel local states do not
            // pre-allocate hundreds of per-partition row buffers and exhaust memory
            // before combine/source has a chance to process the spilled data.
            grouping_state.external_state = Some(
                self.create_external_spill_state(ctx, self.force_external_spill_radix_bits())?,
            );
            self.shared.mark_externalized();
            return Ok(true);
        }

        if !has_temporary_directory {
            return Ok(false);
        }

        let total_size = grouping_state.hash_table.external_accounted_memory_usage();
        if total_size == 0 {
            return Ok(false);
        }

        let num_threads = ctx.num_threads().max(1);
        let reservation = memory.reservation_bytes();
        let mut thread_limit = reservation / num_threads;
        if thread_limit == 0 {
            thread_limit = reservation;
        }
        if total_size <= thread_limit {
            return Ok(false);
        }

        if !grouping_state.reservation_boosted {
            grouping_state.reservation_boosted = true;
            let min_boost = total_size.saturating_mul(num_threads);
            let new_minimum = memory.minimum_reservation_bytes().saturating_add(min_boost);
            memory.set_minimum_reservation_bytes(new_minimum);

            let remaining_size = memory
                .accounted_bytes()
                .max(total_size.saturating_mul(num_threads));
            let enlarged = remaining_size
                .saturating_mul(AGGREGATE_REPARTITION_GROWTH_FACTOR)
                .max(1);
            memory.set_accounted_bytes(enlarged)?;
            let new_reservation = memory.reservation_bytes();
            let per_thread = new_reservation / num_threads;
            thread_limit = if per_thread == 0 {
                new_reservation
            } else {
                per_thread
            };
        }

        if total_size > thread_limit {
            grouping_state.external_state =
                Some(self.create_external_spill_state(ctx, AGGREGATE_EXTERNAL_RADIX_BITS)?);
            self.shared.mark_externalized();
            return Ok(true);
        }

        Ok(false)
    }

    fn append_to_external_state(
        &self,
        payload: &Chunk,
        hashes: &Vector,
        grouping_state: &mut HashAggregateGroupingLocalState,
    ) -> Result<()> {
        let external_state = grouping_state.external_state.as_mut().ok_or_else(|| {
            paro_error::internal(
                "missing aggregate external state while appending spill chunk".to_string(),
            )
        })?;
        let spill_chunk = self.build_spill_chunk(payload, hashes)?;
        external_state.data.append(&spill_chunk)?;
        Ok(())
    }

    fn merge_local_external_state_into_global(
        &self,
        grouping_idx: usize,
        grouping_state: &mut HashAggregateGroupingLocalState,
        sink_state: &HashAggregateGlobalState,
    ) -> Result<()> {
        let Some(external_state) = grouping_state.external_state.take() else {
            return Ok(());
        };

        if external_state.data.count() == 0 {
            return Ok(());
        }

        let mut spill_slot = sink_state
            .spill_data
            .get(grouping_idx)
            .ok_or_else(|| {
                paro_error::internal(format!(
                    "Global aggregate spill slot index out of bounds during combine: grouping_idx={grouping_idx}"
                ))
            })?
            .lock()
            .map_err(|e| paro_error::internal(e.to_string()))?;
        if let Some(global_spill_data) = spill_slot.as_mut() {
            global_spill_data.absorb(external_state.data);
        } else {
            *spill_slot = Some(external_state.data);
        }

        self.shared.mark_externalized();
        Ok(())
    }

    fn replay_spilled_partition_into_hash_table(
        &self,
        grouping_set: &[usize],
        partition: &RowStore,
        hash_table: &mut AggregateHashTable,
    ) -> Result<()> {
        if partition.count() == 0 {
            return Ok(());
        }

        let mut scanner = partition.scanner();
        let mut spill_chunk = Chunk::try_initialize(
            &self.spill_payload_types_with_hash,
            paro_common::vector::VECTOR_SIZE,
            hash_table.allocator(),
        )?;
        loop {
            let fetched = scanner.next_chunk(&mut spill_chunk)?;
            if fetched == 0 {
                break;
            }

            let mut payload_vectors = Vec::with_capacity(self.aggregate_data.payload_types.len());
            for col_idx in 0..self.aggregate_data.payload_types.len() {
                let column = spill_chunk.column(col_idx).ok_or_else(|| {
                    paro_error::internal(format!(
                        "Missing spill payload column while replaying aggregate partition: column_idx={col_idx}"
                    ))
                })?;
                payload_vectors.push(Arc::clone(column));
            }
            let mut payload =
                Chunk::from_arc_vectors(payload_vectors, spill_chunk.allocator().clone());
            payload.set_cardinality(fetched);

            let all_groups = self.build_groups_chunk(&payload)?;
            let groups = self.build_groups_chunk_for_set(&all_groups, grouping_set)?;
            let hashes = hash_table.hash_groups(&groups)?;
            let mut addresses =
                Vector::try_new(LogicalType::BigInt, payload.size(), hash_table.allocator())?;
            let mut new_groups =
                SelectionVector::try_with_capacity(payload.size(), hash_table.allocator())?;
            hash_table.find_or_create_groups(&groups, &hashes, &mut addresses, &mut new_groups)?;
            hash_table.update_aggregates(&payload, Some(&hashes), &addresses, None)?;
        }
        Ok(())
    }

    fn replay_global_spill_data(
        &self,
        ctx: &ExecutionContext,
        sink_state: &HashAggregateGlobalState,
        spill_data: Vec<Option<RadixPartitionedRows>>,
    ) -> Result<()> {
        let mut max_partition_size = 0usize;
        for spill_data in &spill_data {
            if let Some(spill_data) = spill_data.as_ref() {
                for partition in spill_data.partitions() {
                    if partition.count() > 0 {
                        max_partition_size = max_partition_size.max(partition.size_in_bytes());
                    }
                }
            }
        }

        if max_partition_size == 0 {
            sink_state.source_memory.clear();
            return Ok(());
        }

        sink_state
            .source_memory
            .set_minimum_reservation_bytes(max_partition_size)?;
        sink_state.source_memory.set_accounted_bytes(0)?;
        let max_threads = ctx.num_threads().max(1);
        sink_state
            .source_memory
            .set_accounted_bytes(max_partition_size.saturating_mul(max_threads))?;

        for (grouping_idx, spill_data) in spill_data.into_iter().enumerate() {
            let Some(spill_data) = spill_data else {
                continue;
            };

            let grouping_set = self.grouping_sets.get(grouping_idx).ok_or_else(|| {
                paro_error::internal(format!(
                    "Grouping set index out of bounds during spill replay: grouping_idx={grouping_idx}"
                ))
            })?;

            let ght_mutex = sink_state
                .shared
                .hash_tables
                .get(grouping_idx)
                .ok_or_else(|| {
                    paro_error::internal(format!(
                        "Global hash table index out of bounds during spill replay: grouping_idx={grouping_idx}"
                    ))
                })?;
            let mut ght = ght_mutex
                .lock()
                .map_err(|e| paro_error::internal(e.to_string()))?;
            let ght = ght.as_mut().ok_or_else(|| {
                paro_error::internal(format!(
                    "Global hash table was not initialized during spill replay: grouping_idx={grouping_idx}"
                ))
            })?;

            for partition in spill_data.partitions() {
                if partition.count() == 0 {
                    continue;
                }
                let partition_size = partition.size_in_bytes().max(max_partition_size);
                sink_state
                    .source_memory
                    .set_accounted_bytes(partition_size)?;
                self.replay_spilled_partition_into_hash_table(grouping_set, partition, ght)?;
            }
        }

        sink_state.source_memory.clear();
        Ok(())
    }

    fn summarize_spill_data(spill_data: &[Option<RadixPartitionedRows>]) -> (bool, usize) {
        let mut has_spill_data = false;
        let mut max_partition_size = 0usize;
        for spill_data in spill_data {
            if let Some(spill_data) = spill_data.as_ref() {
                for partition in spill_data.partitions() {
                    if partition.count() == 0 {
                        continue;
                    }
                    has_spill_data = true;
                    max_partition_size = max_partition_size.max(partition.size_in_bytes());
                }
            }
        }
        (has_spill_data, max_partition_size)
    }

    fn global_hash_tables_are_empty(&self, sink_state: &HashAggregateGlobalState) -> Result<bool> {
        for grouping_idx in 0..self.grouping_sets.len() {
            let ght = sink_state
                .shared
                .hash_tables
                .get(grouping_idx)
                .ok_or_else(|| {
                    paro_error::internal(format!(
                        "Global hash table index out of bounds while checking external aggregate source mode: grouping_idx={grouping_idx}"
                    ))
                })?
                .lock()
                .map_err(|e| paro_error::internal(e.to_string()))?;
            let ght = ght.as_ref().ok_or_else(|| {
                paro_error::internal(format!(
                    "Global hash table was not initialized while checking source mode: grouping_idx={grouping_idx}"
                ))
            })?;
            if ght.count() != 0 {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn take_global_spill_data_for_source(
        &self,
        sink_state: &HashAggregateGlobalState,
    ) -> Result<Vec<Option<RadixPartitionedRows>>> {
        let mut spill_data = Vec::with_capacity(self.grouping_sets.len());
        for grouping_idx in 0..self.grouping_sets.len() {
            let mut spill_slot = sink_state
                .spill_data
                .get(grouping_idx)
                .ok_or_else(|| {
                    paro_error::internal(format!(
                        "Global aggregate spill slot index out of bounds while transferring source spill data: grouping_idx={grouping_idx}"
                    ))
                })?
                .lock()
                .map_err(|e| paro_error::internal(e.to_string()))?;
            spill_data.push(spill_slot.take().map(RadixPartitionedRowsBuilder::seal));
        }
        Ok(spill_data)
    }

    fn get_data_from_external_spill(
        &self,
        ctx: &ExecutionContext,
        chunk: &mut Chunk,
        gstate: &HashAggregateGlobalSourceState,
        lstate: &mut HashAggregateLocalSourceState,
    ) -> Result<SourceResultType> {
        while lstate.grouping_idx < self.grouping_sets.len() {
            let grouping_idx = lstate.grouping_idx;
            let grouping_set = self.grouping_sets.get(grouping_idx).ok_or_else(|| {
                paro_error::internal(format!(
                    "Grouping set index out of bounds during external aggregate source: grouping_idx={grouping_idx}"
                ))
            })?;
            let Some(spill_data) = gstate
                .external_spill_data
                .get(grouping_idx)
                .and_then(|slot| slot.as_ref())
            else {
                lstate.grouping_idx += 1;
                lstate.external_partition_idx = 0;
                continue;
            };

            while lstate.external_partition_idx < spill_data.partition_count() {
                let partition_idx = lstate.external_partition_idx;
                let partition = spill_data.partitions().get(partition_idx).ok_or_else(|| {
                    paro_error::internal(format!(
                        "External aggregate spill partition index out of bounds: grouping_idx={grouping_idx}, partition_idx={partition_idx}"
                    ))
                })?;
                if partition.count() == 0 {
                    lstate.external_partition_idx += 1;
                    continue;
                }

                if lstate.external_hash_table.is_none() {
                    let partition_size = partition.size_in_bytes().max(1);
                    gstate.source_memory.set_accounted_bytes(partition_size)?;
                    let hash_table_allocator = gstate
                        .source_memory
                        .accounted_allocator(ctx.allocator(HASH_AGGREGATE_MEMORY_TAG));
                    let mut hash_table = self.new_hash_table(hash_table_allocator)?;
                    self.replay_spilled_partition_into_hash_table(
                        grouping_set,
                        partition,
                        &mut hash_table,
                    )?;
                    lstate.external_hash_table = Some(hash_table);
                    lstate.external_position = AggregateHTScanPosition::default();
                }

                let hash_table = lstate.external_hash_table.as_mut().ok_or_else(|| {
                    paro_error::internal(
                        "missing external aggregate hash table while scanning source".to_string(),
                    )
                })?;
                if hash_table.scan(&mut lstate.external_position, chunk)? {
                    self.populate_grouping_columns(chunk, grouping_set)?;
                    return Ok(SourceResultType::HaveMoreOutput);
                }

                if let Some(mut hash_table) = lstate.external_hash_table.take() {
                    hash_table.destroy()?;
                }
                lstate.external_position = AggregateHTScanPosition::default();
                lstate.external_partition_idx += 1;
            }

            lstate.grouping_idx += 1;
            lstate.external_partition_idx = 0;
        }

        gstate.source_memory.clear();
        chunk.set_cardinality(0);
        Ok(SourceResultType::Finished)
    }

    fn new_hash_table(&self, allocator: Arc<dyn Allocator>) -> Result<AggregateHashTable> {
        match self.radix_partition_bits {
            Some(bits) => AggregateHashTable::new_radix(
                self.group_types.clone(),
                self.aggregate_objects.clone(),
                self.aggregate_data.aggregate_inputs.clone(),
                bits,
                allocator,
            ),
            None => AggregateHashTable::new_flat(
                self.group_types.clone(),
                self.aggregate_objects.clone(),
                self.aggregate_data.aggregate_inputs.clone(),
                allocator,
            ),
        }
    }

    fn new_global_hash_tables(
        &self,
        allocator: Arc<dyn Allocator>,
    ) -> Result<Vec<AggregateHashTable>> {
        (0..self.grouping_sets.len())
            .map(|_| self.new_hash_table(allocator.clone()))
            .collect()
    }
}

fn should_use_radix_partitioning(
    group_types: &[LogicalType],
    grouping_sets: &[Vec<usize>],
    has_aggregate_modifiers: bool,
) -> bool {
    if has_aggregate_modifiers || grouping_sets.len() > 1 || group_types.len() < 2 {
        return false;
    }
    group_types
        .iter()
        .all(|ty| ty.is_integer() || matches!(ty, LogicalType::Date))
}

impl fmt::Debug for HashAggregate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HashAggregate")
            .field("types", &self.types)
            .field("group_types", &self.group_types)
            .field("aggregate_count", &self.aggregate_objects.len())
            .finish()
    }
}

#[derive(Debug)]
struct HashAggregateShared {
    hash_tables: Vec<Mutex<Option<AggregateHashTable>>>,
    externalized: AtomicBool,
    execution_seen: AtomicBool,
    destroyed: AtomicBool,
}

impl HashAggregateShared {
    fn new(grouping_count: usize) -> Self {
        Self {
            hash_tables: (0..grouping_count).map(|_| Mutex::new(None)).collect(),
            externalized: AtomicBool::new(false),
            execution_seen: AtomicBool::new(false),
            destroyed: AtomicBool::new(false),
        }
    }

    fn mark_execution_seen(&self) {
        self.execution_seen.store(true, AtomicOrdering::Release);
    }

    fn has_runtime_execution(&self) -> bool {
        self.execution_seen.load(AtomicOrdering::Acquire)
    }

    fn mark_externalized(&self) {
        self.externalized.store(true, AtomicOrdering::Release);
    }

    fn externalized(&self) -> bool {
        self.externalized.load(AtomicOrdering::Acquire)
    }

    fn destroy_once(&self) {
        if self.destroyed.swap(true, AtomicOrdering::AcqRel) {
            return;
        }
        for hash_table in &self.hash_tables {
            if let Ok(mut guard) = hash_table.lock() {
                if let Some(hash_table) = guard.as_mut() {
                    let _ = hash_table.destroy();
                }
            }
        }
    }
}

#[derive(Debug)]
struct HashAggregateReclaimer {
    source_memory: Arc<OperatorExternalMemoryTracker>,
    shared: Arc<HashAggregateShared>,
}

impl Reclaimer for HashAggregateReclaimer {
    fn name(&self) -> &str {
        "hash_aggregate_spill"
    }

    fn reclaimable_bytes(&self) -> usize {
        if self.shared.externalized() {
            self.source_memory.accounted_bytes().unwrap_or(0)
        } else {
            0
        }
    }

    fn reclaim_sync(&self, target_bytes: usize) -> paro_common::memory::MemoryResult<ReclaimStats> {
        if !self.shared.externalized() {
            return Ok(ReclaimStats::empty(target_bytes));
        }
        let reclaimed = self.source_memory.reclaim_accounted_bytes(target_bytes)?;
        Ok(ReclaimStats::new(target_bytes, reclaimed, reclaimed))
    }

    fn spill_cost(&self) -> SpillCost {
        SpillCost::AccountingRelease
    }
}

fn query_memory_pool_for_context(ctx: &ExecutionContext) -> Arc<QueryMemoryPool> {
    ctx.pipeline
        .map(|pipeline| pipeline.query_memory_pool())
        .unwrap_or_else(|| Arc::new(QueryMemoryPool::unbounded()))
}

fn new_hash_aggregate_account(ctx: &ExecutionContext) -> Arc<OperatorMemoryAccount> {
    Arc::new(OperatorMemoryAccount::new(query_memory_pool_for_context(
        ctx,
    )))
}

fn new_hash_aggregate_local_memory_tracker(
    ctx: &ExecutionContext,
) -> Result<LocalExternalMemoryTracker> {
    let account = new_hash_aggregate_account(ctx);
    let owner: Arc<dyn MemoryOwner> = account;
    let grant = LocalMemoryGrant::new(
        owner,
        0,
        HASH_AGGREGATE_MEMORY_TAG,
        HASH_AGGREGATE_MEMORY_CLASS,
        ctx.allocator(HASH_AGGREGATE_MEMORY_TAG),
    )?;
    Ok(LocalExternalMemoryTracker::new(
        grant,
        HASH_AGGREGATE_MEMORY_TAG,
        HASH_AGGREGATE_MEMORY_CLASS,
    ))
}

fn new_hash_aggregate_source_memory_tracker(
    ctx: &ExecutionContext,
) -> Arc<OperatorExternalMemoryTracker> {
    Arc::new(OperatorExternalMemoryTracker::new(
        new_hash_aggregate_account(ctx),
        MemoryDomain::Host,
        HASH_AGGREGATE_MEMORY_TAG,
        HASH_AGGREGATE_MEMORY_CLASS,
    ))
}

fn new_hash_aggregate_modifier_memory_context(ctx: &ExecutionContext) -> MemoryAccountingContext {
    let owner: Arc<dyn MemoryOwner> = ctx.operator_memory_account();
    aggregate_modifier_memory_context(owner)
}

#[derive(Debug)]
struct HashAggregateGlobalState {
    shared: Arc<HashAggregateShared>,
    use_new_agg_spill: bool,
    source_memory: Arc<OperatorExternalMemoryTracker>,
    source_memory_transferred: AtomicBool,
    spill_data: Vec<Mutex<Option<RadixPartitionedRowsBuilder>>>,
}

impl GlobalSinkState for HashAggregateGlobalState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn sink_state_name(&self) -> &str {
        "HashAggregateGlobalState"
    }
}

#[derive(Debug)]
struct HashAggregateGlobalSourceState {
    shared: Arc<HashAggregateShared>,
    source_memory: Arc<OperatorExternalMemoryTracker>,
    external_spill_data: Vec<Option<RadixPartitionedRows>>,
}

impl GlobalSourceState for HashAggregateGlobalSourceState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl Drop for HashAggregateGlobalSourceState {
    fn drop(&mut self) {
        self.source_memory.clear();
        self.shared.destroy_once();
    }
}

#[derive(Debug)]
struct HashAggregateExternalSinkState {
    data: RadixPartitionedRowsBuilder,
}

#[derive(Debug)]
struct HashAggregateGroupingLocalState {
    hash_table: AggregateHashTable,
    external_state: Option<HashAggregateExternalSinkState>,
    reservation_boosted: bool,
    modifier_memory: MemoryAccountingContext,
    distinct_rows: Vec<DistinctRows>,
    ordered_rows: Vec<OrderedRows>,
    ordered_finalized: bool,
    distinct_finalized: bool,
}

#[derive(Debug)]
struct HashAggregateLocalSinkState {
    memory: LocalExternalMemoryTracker,
    grouping_states: Vec<HashAggregateGroupingLocalState>,
}

impl LocalSinkState for HashAggregateLocalSinkState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl Drop for HashAggregateLocalSinkState {
    fn drop(&mut self) {
        for grouping_state in &mut self.grouping_states {
            let _ = grouping_state.hash_table.destroy();
            grouping_state.external_state = None;
        }
        self.memory.clear();
    }
}

impl Drop for HashAggregateGlobalState {
    fn drop(&mut self) {
        for spill_slot in &self.spill_data {
            if let Ok(mut guard) = spill_slot.lock() {
                *guard = None;
            }
        }
        if !self.source_memory_transferred.load(AtomicOrdering::Acquire) {
            self.source_memory.clear();
        }
    }
}

#[derive(Debug)]
struct HashAggregateLocalSourceState {
    grouping_idx: usize,
    positions: Vec<AggregateHTScanPosition>,
    external_partition_idx: usize,
    external_position: AggregateHTScanPosition,
    external_hash_table: Option<AggregateHashTable>,
}

impl LocalSourceState for HashAggregateLocalSourceState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl Drop for HashAggregateLocalSourceState {
    fn drop(&mut self) {
        if let Some(mut hash_table) = self.external_hash_table.take() {
            let _ = hash_table.destroy();
        }
    }
}

impl PhysicalOperator for HashAggregate {
    fn operator_type(&self) -> PhysicalOperatorType {
        PhysicalOperatorType::HashGroupBy
    }

    fn runtime_memory_stats(&self) -> ExplainRuntimeStats {
        HashAggregate::runtime_memory_stats(self)
    }

    fn types(&self) -> &[LogicalType] {
        &self.types
    }

    fn explain_params(&self) -> Vec<String> {
        let mut params = Vec::new();

        if !self.aggregate_data.groups.is_empty() {
            params.push(format!(
                "Group Key: {}",
                self.aggregate_data
                    .groups
                    .iter()
                    .map(format_bound_expression)
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }

        if !self.aggregate_data.aggregates.is_empty() {
            let aggregates = self
                .aggregate_data
                .aggregates
                .iter()
                .map(format_bound_expression)
                .collect::<Vec<_>>()
                .join(", ");
            params.push(format!("Aggregates: {aggregates}"));
        }

        if let Some(width) = self.inline_key_width {
            params.push(format!("Key Mode: INLINE_KEY_{width}B"));
        }
        if let Some(bits) = self.radix_partition_bits {
            params.push(format!("Partitioning: RADIX_PARTITIONS={}", 1usize << bits));
        }

        // Only emit runtime externalization status when this operator has executed.
        if self.shared.has_runtime_execution() {
            params.push(format!("External: {}", self.shared.externalized()));
        }

        params
    }

    fn child(&self, index: usize) -> Option<&dyn PhysicalOperator> {
        if index == 0 {
            Some(self.child.as_ref())
        } else {
            None
        }
    }

    fn child_arc(&self, index: usize) -> Option<Arc<dyn PhysicalOperator>> {
        if index == 0 {
            Some(self.child.clone())
        } else {
            None
        }
    }

    fn children_count(&self) -> usize {
        1
    }

    fn is_source(&self) -> bool {
        true
    }

    fn is_sink(&self) -> bool {
        true
    }

    fn parallel_sink(&self) -> bool {
        !self.has_distinct && !self.has_ordered
    }

    fn set_sink_state(&self, state: Arc<dyn GlobalSinkState>) {
        let mut lock = self.sink_state.lock().unwrap();
        *lock = Some(state);
    }

    fn sink_state(&self) -> Option<Arc<dyn GlobalSinkState>> {
        self.sink_state.lock().unwrap().clone()
    }

    fn get_global_sink_state(&self, ctx: &ExecutionContext) -> Result<Box<dyn GlobalSinkState>> {
        let use_new_agg_spill = self.use_new_agg_spill_enabled(ctx);
        let source_memory = new_hash_aggregate_source_memory_tracker(ctx);
        let global_allocator =
            source_memory.accounted_allocator(ctx.allocator(HASH_AGGREGATE_MEMORY_TAG));
        let global_hash_tables = self.new_global_hash_tables(global_allocator)?;
        self.shared.destroyed.store(false, AtomicOrdering::Release);
        self.shared
            .externalized
            .store(false, AtomicOrdering::Release);
        for (slot, hash_table) in self.shared.hash_tables.iter().zip(global_hash_tables) {
            *slot
                .lock()
                .map_err(|err| paro_error::internal(err.to_string()))? = Some(hash_table);
        }
        let reclaimer: Arc<dyn Reclaimer> = Arc::new(HashAggregateReclaimer {
            source_memory: source_memory.clone(),
            shared: self.shared.clone(),
        });
        ctx.query_memory_pool().register_reclaimer(reclaimer);
        Ok(Box::new(HashAggregateGlobalState {
            shared: self.shared.clone(),
            use_new_agg_spill,
            source_memory,
            source_memory_transferred: AtomicBool::new(false),
            spill_data: (0..self.grouping_sets.len())
                .map(|_| Mutex::new(None))
                .collect(),
        }))
    }

    fn get_local_sink_state(&self, ctx: &ExecutionContext) -> Result<Box<dyn LocalSinkState>> {
        let memory = new_hash_aggregate_local_memory_tracker(ctx)?;
        let modifier_memory = new_hash_aggregate_modifier_memory_context(ctx);
        let hash_table_allocator = memory.accounted_allocator();
        let mut grouping_states = Vec::with_capacity(self.grouping_sets.len());
        for _ in 0..self.grouping_sets.len() {
            grouping_states.push(HashAggregateGroupingLocalState {
                hash_table: self.new_hash_table(hash_table_allocator.clone())?,
                external_state: None,
                reservation_boosted: false,
                modifier_memory: modifier_memory.clone(),
                distinct_rows: (0..self.aggregate_objects.len()).map(|_| None).collect(),
                ordered_rows: (0..self.aggregate_objects.len())
                    .map(|_| AccountedValueRows::new(modifier_memory.clone()))
                    .collect(),
                ordered_finalized: false,
                distinct_finalized: false,
            });
        }
        Ok(Box::new(HashAggregateLocalSinkState {
            memory,
            grouping_states,
        }))
    }

    fn sink(
        &self,
        ctx: &ExecutionContext,
        chunk: &Chunk,
        input: &mut OperatorSinkInput,
    ) -> Result<SinkResultType> {
        if chunk.size() == 0 {
            return Ok(SinkResultType::NeedMoreInput);
        }
        self.shared.mark_execution_seen();

        let lstate = input
            .local_state
            .as_any_mut()
            .downcast_mut::<HashAggregateLocalSinkState>()
            .ok_or_else(|| paro_error::internal("Invalid local sink state".to_string()))?;
        let gstate = input
            .global_state
            .as_any()
            .downcast_ref::<HashAggregateGlobalState>()
            .ok_or_else(|| paro_error::internal("Invalid global sink state".to_string()))?;

        let memory = &mut lstate.memory;
        let grouping_states = &mut lstate.grouping_states;
        self.update_memory_tracker(grouping_states.as_slice(), memory)?;

        let all_groups = self.build_groups_chunk(chunk)?;
        for (grouping_idx, grouping_set) in self.grouping_sets.iter().enumerate() {
            let groups = self.build_groups_chunk_for_set(&all_groups, grouping_set)?;
            let grouping_state = grouping_states.get_mut(grouping_idx).ok_or_else(|| {
                paro_error::internal(format!(
                    "Grouping local state index out of bounds: grouping_idx={grouping_idx}"
                ))
            })?;
            let hashes = grouping_state.hash_table.hash_groups(&groups)?;

            let external_mode = self.maybe_externalize_grouping_state(
                ctx,
                memory,
                grouping_state,
                gstate.use_new_agg_spill,
            )?;
            if external_mode {
                self.append_to_external_state(chunk, &hashes, grouping_state)?;
                continue;
            }

            let mut addresses =
                Vector::try_new(LogicalType::BigInt, chunk.size(), chunk.allocator().clone())?;
            let mut new_groups =
                SelectionVector::try_with_capacity(chunk.size(), chunk.allocator().clone())?;
            grouping_state.hash_table.find_or_create_groups(
                &groups,
                &hashes,
                &mut addresses,
                &mut new_groups,
            )?;

            if !self.has_aggregate_modifiers {
                grouping_state.hash_table.update_aggregates(
                    chunk,
                    Some(&hashes),
                    &addresses,
                    None,
                )?;
                continue;
            }

            let mut arena = ctx.arena_allocator();
            self.update_aggregates_with_modifiers(
                chunk,
                &groups,
                &addresses,
                grouping_state.distinct_rows.as_mut_slice(),
                grouping_state.ordered_rows.as_mut_slice(),
                &grouping_state.modifier_memory,
                &mut arena,
            )?;
        }

        self.update_memory_tracker(grouping_states.as_slice(), memory)?;

        Ok(SinkResultType::NeedMoreInput)
    }

    fn combine(
        &self,
        _ctx: &ExecutionContext,
        input: &mut OperatorSinkCombineInput,
    ) -> Result<SinkCombineResultType> {
        let gstate = input
            .global_state
            .as_any()
            .downcast_ref::<HashAggregateGlobalState>()
            .ok_or_else(|| paro_error::internal("Invalid global sink state".to_string()))?;
        let lstate = input
            .local_state
            .as_any_mut()
            .downcast_mut::<HashAggregateLocalSinkState>()
            .ok_or_else(|| paro_error::internal("Invalid local sink state".to_string()))?;
        for grouping_idx in 0..self.grouping_sets.len() {
            let grouping_state = lstate.grouping_states.get_mut(grouping_idx).ok_or_else(|| {
                paro_error::internal(format!(
                    "Grouping local state index out of bounds during combine: grouping_idx={grouping_idx}"
                ))
            })?;
            self.finalize_ordered_aggregates(grouping_state)?;
            self.finalize_distinct_aggregates(grouping_state)?;

            let ght_mutex = gstate
                .shared
                .hash_tables
                .get(grouping_idx)
                .ok_or_else(|| {
                    paro_error::internal(format!(
                        "Global hash table index out of bounds during combine: grouping_idx={grouping_idx}"
                    ))
                })?;
            let mut ght = ght_mutex
                .lock()
                .map_err(|e| paro_error::internal(e.to_string()))?;
            let ght = ght.as_mut().ok_or_else(|| {
                paro_error::internal(format!(
                    "Global hash table was not initialized during combine: grouping_idx={grouping_idx}"
                ))
            })?;
            ght.combine(&mut grouping_state.hash_table)?;
            self.merge_local_external_state_into_global(grouping_idx, grouping_state, gstate)?;
        }
        lstate.memory.clear();
        Ok(SinkCombineResultType::Finished)
    }

    fn get_global_source_state(
        &self,
        ctx: &ExecutionContext,
        sink_state: Option<&dyn GlobalSinkState>,
    ) -> Result<Box<dyn GlobalSourceState>> {
        let mut source_memory = new_hash_aggregate_source_memory_tracker(ctx);
        let mut external_spill_data = Vec::with_capacity(self.grouping_sets.len());
        external_spill_data.resize_with(self.grouping_sets.len(), || None);

        let stored_sink_state = self.sink_state();
        let sink_state = sink_state.or(stored_sink_state.as_deref());

        if let Some(sink_state) = sink_state {
            let sink_state = sink_state
                .as_any()
                .downcast_ref::<HashAggregateGlobalState>()
                .ok_or_else(|| {
                    paro_error::internal("Invalid sink state for hash aggregate source".to_string())
                })?;
            source_memory = Arc::clone(&sink_state.source_memory);
            if sink_state.use_new_agg_spill {
                let sealed_spill_data = self.take_global_spill_data_for_source(sink_state)?;
                let (has_spill_data, max_partition_size) =
                    Self::summarize_spill_data(&sealed_spill_data);
                if has_spill_data && self.global_hash_tables_are_empty(sink_state)? {
                    if max_partition_size > 0 {
                        sink_state
                            .source_memory
                            .set_minimum_reservation_bytes(max_partition_size)?;
                        sink_state
                            .source_memory
                            .set_accounted_bytes(max_partition_size)?;
                    } else {
                        sink_state.source_memory.clear();
                    }
                    external_spill_data = sealed_spill_data;
                } else {
                    self.replay_global_spill_data(ctx, sink_state, sealed_spill_data)?;
                }
            } else {
                sink_state.source_memory.clear();
            }
            sink_state
                .source_memory_transferred
                .store(true, AtomicOrdering::Release);
        }

        Ok(Box::new(HashAggregateGlobalSourceState {
            shared: self.shared.clone(),
            source_memory,
            external_spill_data,
        }))
    }

    fn get_local_source_state(
        &self,
        _ctx: &ExecutionContext,
        _gstate: &dyn GlobalSourceState,
    ) -> Result<Box<dyn LocalSourceState>> {
        Ok(Box::new(HashAggregateLocalSourceState {
            grouping_idx: 0,
            positions: vec![AggregateHTScanPosition::default(); self.grouping_sets.len()],
            external_partition_idx: 0,
            external_position: AggregateHTScanPosition::default(),
            external_hash_table: None,
        }))
    }

    fn get_data(
        &self,
        ctx: &ExecutionContext,
        chunk: &mut Chunk,
        input: &mut OperatorSourceInput,
    ) -> Result<SourceResultType> {
        let gstate = input
            .global_state
            .as_any()
            .downcast_ref::<HashAggregateGlobalSourceState>()
            .ok_or_else(|| paro_error::internal("Invalid global source state".to_string()))?;
        let lstate = input
            .local_state
            .as_any_mut()
            .downcast_mut::<HashAggregateLocalSourceState>()
            .ok_or_else(|| paro_error::internal("Invalid local source state".to_string()))?;

        if gstate.external_spill_data.iter().any(|slot| slot.is_some()) {
            return self.get_data_from_external_spill(ctx, chunk, gstate, lstate);
        }

        while lstate.grouping_idx < self.grouping_sets.len() {
            let grouping_idx = lstate.grouping_idx;
            let ght_mutex = gstate
                .shared
                .hash_tables
                .get(grouping_idx)
                .ok_or_else(|| {
                    paro_error::internal(format!(
                        "Global hash table index out of bounds during source: grouping_idx={grouping_idx}"
                    ))
                })?;
            let mut ght = ght_mutex
                .lock()
                .map_err(|e| paro_error::internal(e.to_string()))?;
            let ght = ght.as_mut().ok_or_else(|| {
                paro_error::internal(format!(
                    "Global hash table was not initialized during source: grouping_idx={grouping_idx}"
                ))
            })?;
            let position = lstate.positions.get_mut(grouping_idx).ok_or_else(|| {
                paro_error::internal(format!(
                    "Local source position index out of bounds: grouping_idx={grouping_idx}"
                ))
            })?;
            if ght.scan(position, chunk)? {
                let grouping_set = self.grouping_sets.get(grouping_idx).ok_or_else(|| {
                    paro_error::internal(format!(
                        "Grouping set index out of bounds during source: grouping_idx={grouping_idx}"
                    ))
                })?;
                self.populate_grouping_columns(chunk, grouping_set)?;
                return Ok(SourceResultType::HaveMoreOutput);
            }
            lstate.grouping_idx += 1;
        }
        chunk.set_cardinality(0);
        Ok(SourceResultType::Finished)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::mem::size_of;
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    use paro_common::chunk::Chunk;
    use paro_common::runtime_value::Value;
    use paro_common::types::LogicalType;
    use paro_common::vector::{Vector, VECTOR_SIZE};
    use paro_context::{RuntimeLimits, StatementContext, TestStatementContextBuilder};
    use paro_function::aggregate::{AggregateFunction, AggregateInputData};
    use paro_planner::expression::{
        AggregateExpression, AggregateType, Expression, ReferenceExpression,
    };
    use paro_scheduler::task::InterruptState;
    use paro_storage::buffer::{BufferPool, MemoryTag};
    use paro_storage::row::{RadixPartitionedRowsBuilder, RowLayout, RowValidityType};

    use crate::execution_context::ExecutionContext;
    use crate::operator::scan::dummy_scan::PhysicalDummyScan;
    use crate::operator::state::{
        OperatorSinkCombineInput, OperatorSinkInput, OperatorSourceInput,
    };
    use crate::operator::PhysicalOperator;
    use crate::thread_context::ThreadContext;

    use super::*;

    unsafe fn sum_initialize(state: *mut u8) {
        *(state as *mut i64) = 0;
    }

    unsafe fn sum_update(
        inputs: &[&Vector],
        _input_data: &AggregateInputData,
        states: &Vector,
        count: usize,
    ) {
        let input = inputs[0].decode(count);
        let input_data = input.get_data::<i64>();
        let state = states.decode(count);
        let state_data = state.get_data::<*mut u8>();
        for row in 0..count {
            let input_row = input.sel().get(row);
            if !input.validity().is_valid(input_row) {
                continue;
            }
            let state_row = state.sel().get(row);
            let state_ptr = *state_data.add(state_row) as *mut i64;
            *state_ptr += *input_data.add(input_row);
        }
    }

    unsafe fn sum_combine(
        source: &Vector,
        target: &Vector,
        _input_data: &AggregateInputData,
        count: usize,
    ) {
        let source_format = source.decode(count);
        let target_format = target.decode(count);
        let source_data = source_format.get_data::<*mut u8>();
        let target_data = target_format.get_data::<*mut u8>();
        for row in 0..count {
            let source_idx = source_format.sel().get(row);
            let target_idx = target_format.sel().get(row);
            let source_ptr = *source_data.add(source_idx) as *const i64;
            let target_ptr = *target_data.add(target_idx) as *mut i64;
            *target_ptr += *source_ptr;
        }
    }

    unsafe fn sum_finalize(
        states: &Vector,
        _input_data: &AggregateInputData,
        result: &mut Vector,
        count: usize,
    ) {
        let state = states.decode(count);
        let state_data = state.get_data::<*mut u8>();
        let result_data = result.flat_data_mut::<i64>();
        for row in 0..count {
            let state_idx = state.sel().get(row);
            let state_ptr = *state_data.add(state_idx) as *const i64;
            *result_data.add(row) = *state_ptr;
        }
    }

    fn build_sum_aggregate() -> Expression {
        let function = AggregateFunction::new(
            "test_sum".to_string(),
            vec![LogicalType::BigInt],
            LogicalType::BigInt,
            size_of::<i64>(),
            sum_initialize,
            sum_update,
            sum_combine,
            sum_finalize,
            None,
            None,
        );
        Expression::Aggregate(
            AggregateExpression::new(
                function,
                vec![Expression::Reference(ReferenceExpression::new(
                    2,
                    LogicalType::BigInt,
                ))],
                LogicalType::BigInt,
            )
            .with_aggr_type(AggregateType::NonDistinct),
        )
    }

    fn build_test_operator() -> HashAggregate {
        let group0 = Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer));
        let group1 = Expression::Reference(ReferenceExpression::new(1, LogicalType::Integer));
        let aggregate_data = GroupedAggregateData {
            projection_exprs: Vec::new(),
            payload_types: vec![
                LogicalType::Integer,
                LogicalType::Integer,
                LogicalType::BigInt,
            ],
            groups: vec![group0.clone(), group1.clone()],
            grouping_sets: vec![vec![0, 1]],
            aggregates: vec![build_sum_aggregate()],
            grouping_functions: Vec::new(),
            aggregate_inputs: vec![vec![2]],
            aggregate_filters: vec![None],
            aggregate_orders: vec![Vec::new()],
        };

        HashAggregate::new(
            aggregate_data,
            vec![
                LogicalType::Integer,
                LogicalType::Integer,
                LogicalType::BigInt,
            ],
            Arc::new(PhysicalDummyScan::new()),
        )
        .expect("create physical hash aggregate")
    }

    fn make_session(memory_limit: usize, force_external: bool) -> (Arc<StatementContext>, String) {
        let temp_dir = create_test_temp_dir("paro_hash_agg_external");
        let session = TestStatementContextBuilder::minimal()
            .with_limits(RuntimeLimits {
                max_threads: 1,
                max_memory: memory_limit,
                use_temporary_directory: true,
                temporary_directory: temp_dir.clone(),
                max_temp_directory_size: None,
                force_external,
            })
            .build();
        session
            .buffer_pool()
            .set_memory_limit(memory_limit)
            .unwrap();
        session
            .buffer_pool()
            .set_temporary_directory(temp_dir.clone())
            .expect("temp directory should be configured");
        (session, temp_dir)
    }

    fn create_test_temp_dir(prefix: &str) -> String {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock should be after unix epoch")
            .as_nanos();
        let path = std::env::temp_dir().join(format!("{prefix}_{}_{}", std::process::id(), suffix));
        fs::create_dir_all(&path).expect("test temp directory should be created");
        path.to_string_lossy().to_string()
    }

    fn create_external_grouping_state(operator: &HashAggregate) -> HashAggregateGroupingLocalState {
        create_external_grouping_state_with_buffer_pool(
            operator,
            BufferPool::new_arc(128 * 1024 * 1024),
        )
    }

    fn create_external_grouping_state_with_buffer_pool(
        operator: &HashAggregate,
        buffer_pool: Arc<BufferPool>,
    ) -> HashAggregateGroupingLocalState {
        let layout = Arc::new(RowLayout::from_types(
            operator.spill_payload_types_with_hash.clone(),
            RowValidityType::CanHaveNullValues,
        ));
        let data = RadixPartitionedRowsBuilder::new(
            buffer_pool,
            layout,
            MemoryTag::HashTable,
            AGGREGATE_EXTERNAL_RADIX_BITS,
            operator.spill_hash_col_idx,
        )
        .expect("create external spill data");

        let modifier_memory = MemoryAccountingContext::detached(
            HASH_AGGREGATE_MEMORY_TAG,
            HASH_AGGREGATE_MEMORY_CLASS,
        );

        HashAggregateGroupingLocalState {
            hash_table: operator
                .new_hash_table(paro_common::test_utils::test_allocator())
                .expect("create local hash table"),
            external_state: Some(HashAggregateExternalSinkState { data }),
            reservation_boosted: false,
            modifier_memory: modifier_memory.clone(),
            distinct_rows: (0..operator.aggregate_objects.len())
                .map(|_| None)
                .collect(),
            ordered_rows: (0..operator.aggregate_objects.len())
                .map(|_| AccountedValueRows::new(modifier_memory.clone()))
                .collect(),
            ordered_finalized: false,
            distinct_finalized: false,
        }
    }

    fn build_payload_chunk(start: usize, end: usize) -> Chunk {
        let k1 = (start..end).map(|v| v as i32).collect::<Vec<_>>();
        let k2 = (start..end).map(|v| (v % 257) as i32).collect::<Vec<_>>();
        let values = vec![1i64; end - start];
        Chunk::from_vectors(
            vec![
                paro_common::test_utils::test_i32_vector_with_allocator(
                    &k1,
                    paro_common::test_utils::test_allocator(),
                ),
                paro_common::test_utils::test_i32_vector_with_allocator(
                    &k2,
                    paro_common::test_utils::test_allocator(),
                ),
                paro_common::test_utils::test_i64_vector_with_allocator(
                    &values,
                    paro_common::test_utils::test_allocator(),
                ),
            ],
            paro_common::test_utils::test_allocator(),
        )
    }

    #[test]
    fn external_hash_aggregate_replay_preserves_all_groups() {
        let operator = build_test_operator();
        let mut grouping_state = create_external_grouping_state(&operator);

        let row_count = 200_000usize;
        let mut start = 1usize;
        while start <= row_count {
            let end = (start + VECTOR_SIZE).min(row_count + 1);
            let payload = build_payload_chunk(start, end);
            let all_groups = operator.build_groups_chunk(&payload).expect("build groups");
            let groups = operator
                .build_groups_chunk_for_set(&all_groups, &operator.grouping_sets[0])
                .expect("build grouping set");
            let hashes = grouping_state
                .hash_table
                .hash_groups(&groups)
                .expect("hash groups");
            operator
                .append_to_external_state(&payload, &hashes, &mut grouping_state)
                .expect("append to external state");
            start = end;
        }

        let external_data = grouping_state
            .external_state
            .take()
            .expect("external state should exist")
            .data
            .seal();
        let mut spilled_rows = 0usize;
        let mut spilled_sum_k1 = 0i64;
        let mut seen_k1 = vec![false; row_count + 1];
        for partition in external_data.partitions() {
            let mut scanner = partition.scanner();
            let mut chunk = paro_common::test_utils::test_chunk_with_capacity(
                &operator.spill_payload_types_with_hash,
                VECTOR_SIZE,
            );
            loop {
                let fetched = scanner
                    .next_chunk(&mut chunk)
                    .expect("fetch spill partition chunk");
                if fetched == 0 {
                    break;
                }
                for row_idx in 0..fetched {
                    spilled_rows += 1;
                    let k1 = chunk.column(0).unwrap().get_i32(row_idx).unwrap() as usize;
                    assert!(k1 <= row_count, "unexpected k1 value in spilled data: {k1}");
                    assert!(!seen_k1[k1], "duplicate k1 value in spilled data: {k1}");
                    seen_k1[k1] = true;
                    spilled_sum_k1 += k1 as i64;
                }
            }
        }
        assert_eq!(spilled_rows, row_count);
        let expected_sum = (row_count as i64) * ((row_count as i64) + 1) / 2;
        assert_eq!(spilled_sum_k1, expected_sum);
        assert!(seen_k1.iter().skip(1).all(|seen| *seen));

        let mut manual_hash_table = operator
            .new_hash_table(paro_common::test_utils::test_allocator())
            .expect("create manual hash table");
        for partition in external_data.partitions() {
            let mut scanner = partition.scanner();
            let mut spill_chunk = paro_common::test_utils::test_chunk_with_capacity(
                &operator.spill_payload_types_with_hash,
                VECTOR_SIZE,
            );
            loop {
                let fetched = scanner
                    .next_chunk(&mut spill_chunk)
                    .expect("fetch spill partition chunk for manual replay");
                if fetched == 0 {
                    break;
                }

                let mut payload_vectors =
                    Vec::with_capacity(operator.aggregate_data.payload_types.len());
                for col_idx in 0..operator.aggregate_data.payload_types.len() {
                    payload_vectors.push(Arc::clone(
                        spill_chunk.column(col_idx).expect("payload column"),
                    ));
                }
                let mut payload = Chunk::from_arc_vectors(
                    payload_vectors,
                    paro_common::test_utils::test_allocator(),
                );
                payload.set_cardinality(fetched);
                let all_groups = operator.build_groups_chunk(&payload).expect("build groups");
                let groups = operator
                    .build_groups_chunk_for_set(&all_groups, &operator.grouping_sets[0])
                    .expect("build grouping set");
                let hashes = manual_hash_table.hash_groups(&groups).expect("hash groups");
                let mut addresses = paro_common::test_utils::test_vector_with_capacity(
                    LogicalType::BigInt,
                    payload.size(),
                );
                let mut new_groups =
                    paro_common::test_utils::test_selection_with_capacity(payload.size());
                let created = manual_hash_table
                    .find_or_create_groups(&groups, &hashes, &mut addresses, &mut new_groups)
                    .expect("find or create groups");
                assert_eq!(created, fetched, "manual replay should create every group");
                manual_hash_table
                    .update_aggregates(&payload, Some(&hashes), &addresses, None)
                    .expect("update aggregates");
            }
        }
        let mut manual_position = AggregateHTScanPosition::default();
        let mut manual_chunk =
            paro_common::test_utils::test_chunk_with_capacity(&operator.types, VECTOR_SIZE);
        let mut manual_seen_rows = 0usize;
        while manual_hash_table
            .scan(&mut manual_position, &mut manual_chunk)
            .expect("scan manual hash table")
        {
            manual_seen_rows += manual_chunk.size();
        }
        assert_eq!(manual_seen_rows, row_count);
    }

    #[test]
    fn force_external_hash_aggregate_combine_keeps_global_ht_empty() {
        let (session, temp_dir) = make_session(32 * 1024 * 1024, true);
        let thread = ThreadContext::single_threaded();
        let ctx = ExecutionContext::new(session, &thread, None);
        let operator = build_test_operator();
        let global_sink_state = operator
            .get_global_sink_state(&ctx)
            .expect("create global sink state");
        let mut local_sink_state = operator
            .get_local_sink_state(&ctx)
            .expect("create local sink state");
        let interrupt = InterruptState::new();

        let row_count = 50_000usize;
        let mut start = 1usize;
        while start <= row_count {
            let end = (start + VECTOR_SIZE).min(row_count + 1);
            let payload = build_payload_chunk(start, end);
            let mut sink_input = OperatorSinkInput::new(
                global_sink_state.as_ref(),
                local_sink_state.as_mut(),
                &interrupt,
            );
            operator
                .sink(&ctx, &payload, &mut sink_input)
                .expect("sink payload chunk");
            start = end;
        }

        let mut combine_input = OperatorSinkCombineInput::new(
            global_sink_state.as_ref(),
            local_sink_state.as_mut(),
            &interrupt,
        );
        operator
            .combine(&ctx, &mut combine_input)
            .expect("combine sink states");

        let global_hash_state = global_sink_state
            .as_any()
            .downcast_ref::<HashAggregateGlobalState>()
            .expect("hash aggregate global sink state");
        let ght = global_hash_state.shared.hash_tables[0]
            .lock()
            .expect("lock global hash table");
        assert_eq!(ght.as_ref().expect("global hash table").count(), 0);
        drop(ght);

        let spill_count = global_hash_state.spill_data[0]
            .lock()
            .expect("lock spill slot")
            .as_ref()
            .expect("force_external combine should keep spill data")
            .count();
        assert_eq!(spill_count as usize, row_count);

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[test]
    fn force_external_hash_aggregate_uses_low_overhead_spill_partitioning() {
        let (session, temp_dir) = make_session(32 * 1024 * 1024, true);
        let thread = ThreadContext::single_threaded();
        let ctx = ExecutionContext::new(session, &thread, None);
        let operator = build_test_operator();
        let global_sink_state = operator
            .get_global_sink_state(&ctx)
            .expect("create global sink state");
        let mut local_sink_state = operator
            .get_local_sink_state(&ctx)
            .expect("create local sink state");
        let interrupt = InterruptState::new();

        let payload = build_payload_chunk(1, VECTOR_SIZE.min(4096));
        let mut sink_input = OperatorSinkInput::new(
            global_sink_state.as_ref(),
            local_sink_state.as_mut(),
            &interrupt,
        );
        operator
            .sink(&ctx, &payload, &mut sink_input)
            .expect("sink payload chunk");

        let local_hash_state = local_sink_state
            .as_any_mut()
            .downcast_mut::<HashAggregateLocalSinkState>()
            .expect("hash aggregate local sink state");
        let partition_count = local_hash_state.grouping_states[0]
            .external_state
            .as_ref()
            .expect("force_external should create spill state")
            .data
            .partition_count();
        assert_eq!(
            partition_count,
            1usize << operator.force_external_spill_radix_bits()
        );
        assert_eq!(partition_count, 4);

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[test]
    fn external_hash_aggregate_low_memory_execution_preserves_all_groups() {
        let (session, temp_dir) = make_session(32 * 1024 * 1024, true);
        let thread = ThreadContext::single_threaded();
        let ctx = ExecutionContext::new(session, &thread, None);
        let operator = build_test_operator();
        let global_sink_state = operator
            .get_global_sink_state(&ctx)
            .expect("create global sink state");
        let mut local_sink_state = operator
            .get_local_sink_state(&ctx)
            .expect("create local sink state");
        let interrupt = InterruptState::new();

        let row_count = 200_000usize;
        let mut start = 1usize;
        while start <= row_count {
            let end = (start + VECTOR_SIZE).min(row_count + 1);
            let payload = build_payload_chunk(start, end);
            let mut sink_input = OperatorSinkInput::new(
                global_sink_state.as_ref(),
                local_sink_state.as_mut(),
                &interrupt,
            );
            operator
                .sink(&ctx, &payload, &mut sink_input)
                .expect("sink payload chunk");
            start = end;
        }

        let local_hash_state = local_sink_state
            .as_any_mut()
            .downcast_mut::<HashAggregateLocalSinkState>()
            .expect("hash aggregate local sink state");
        let external_state = local_hash_state.grouping_states[0]
            .external_state
            .as_ref()
            .expect("external state should exist under force_external");
        assert_eq!(external_state.data.count() as usize, row_count);
        let (_, partition_counts) = external_state.data.get_sizes_and_counts();
        assert_eq!(
            partition_counts
                .into_iter()
                .map(|count| count as usize)
                .sum::<usize>(),
            row_count
        );

        let mut combine_input = OperatorSinkCombineInput::new(
            global_sink_state.as_ref(),
            local_sink_state.as_mut(),
            &interrupt,
        );
        operator
            .combine(&ctx, &mut combine_input)
            .expect("combine sink states");

        let global_hash_state = global_sink_state
            .as_any()
            .downcast_ref::<HashAggregateGlobalState>()
            .expect("hash aggregate global sink state");
        let ght = global_hash_state.shared.hash_tables[0]
            .lock()
            .expect("lock global hash table");
        assert_eq!(ght.as_ref().expect("global hash table").count(), 0);
        drop(ght);
        let spill_count = global_hash_state.spill_data[0]
            .lock()
            .expect("lock spill slot")
            .as_ref()
            .expect("force_external combine should keep spill data")
            .count();
        assert_eq!(spill_count as usize, row_count);

        let global_source_state = operator
            .get_global_source_state(&ctx, Some(global_sink_state.as_ref()))
            .expect("create global source state");
        let source_state = global_source_state
            .as_any()
            .downcast_ref::<HashAggregateGlobalSourceState>()
            .expect("hash aggregate global source state");
        let source_spill_count = source_state.external_spill_data[0]
            .as_ref()
            .expect("global source state should own spill data in force_external mode")
            .count();
        assert_eq!(source_spill_count as usize, row_count);
        assert!(
            global_hash_state.spill_data[0]
                .lock()
                .expect("lock sink spill slot after source init")
                .is_none(),
            "source initialization should transfer spill ownership out of sink state"
        );

        let mut local_source_state = operator
            .get_local_source_state(&ctx, global_source_state.as_ref())
            .expect("create local source state");

        let mut chunk =
            paro_common::test_utils::test_chunk_with_capacity(&operator.types, VECTOR_SIZE);
        let mut seen_rows = 0usize;
        let mut sum_k1 = 0i64;
        loop {
            let mut source_input = OperatorSourceInput::new(
                global_source_state.as_ref(),
                local_source_state.as_mut(),
                &interrupt,
            );
            if !matches!(
                operator
                    .get_data(&ctx, &mut chunk, &mut source_input)
                    .expect("source output"),
                crate::result_type::SourceResultType::HaveMoreOutput
            ) {
                break;
            }
            for row_idx in 0..chunk.size() {
                seen_rows += 1;
                match chunk.column(0).unwrap().get_value(row_idx) {
                    Value::Integer(v) => sum_k1 += v as i64,
                    other => panic!("unexpected group key value: {other:?}"),
                }
                assert_eq!(chunk.column(2).unwrap().get_i64(row_idx), Some(1));
            }
        }

        let expected_sum = (row_count as i64) * ((row_count as i64) + 1) / 2;
        assert_eq!(seen_rows, row_count);
        assert_eq!(sum_k1, expected_sum);

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[test]
    fn external_hash_aggregate_multi_local_force_external_preserves_all_groups() {
        let (session, temp_dir) = make_session(32 * 1024 * 1024, true);
        let thread = ThreadContext::single_threaded();
        let ctx = ExecutionContext::new(session, &thread, None);
        let operator = build_test_operator();
        let global_sink_state = operator
            .get_global_sink_state(&ctx)
            .expect("create global sink state");
        let interrupt = InterruptState::new();

        let mut local_sink_states = (0..8)
            .map(|_| {
                operator
                    .get_local_sink_state(&ctx)
                    .expect("create local sink state")
            })
            .collect::<Vec<_>>();

        let row_count = 50_000usize;
        let mut start = 1usize;
        let mut state_idx = 0usize;
        while start <= row_count {
            let end = (start + VECTOR_SIZE).min(row_count + 1);
            let payload = build_payload_chunk(start, end);
            let local_state = local_sink_states
                .get_mut(state_idx)
                .expect("local sink state should exist");
            let mut sink_input = OperatorSinkInput::new(
                global_sink_state.as_ref(),
                local_state.as_mut(),
                &interrupt,
            );
            operator
                .sink(&ctx, &payload, &mut sink_input)
                .expect("sink payload chunk");
            start = end;
            state_idx = (state_idx + 1) % local_sink_states.len();
        }

        for local_state in &mut local_sink_states {
            let mut combine_input = OperatorSinkCombineInput::new(
                global_sink_state.as_ref(),
                local_state.as_mut(),
                &interrupt,
            );
            operator
                .combine(&ctx, &mut combine_input)
                .expect("combine sink states");
        }

        let global_hash_state = global_sink_state
            .as_any()
            .downcast_ref::<HashAggregateGlobalState>()
            .expect("hash aggregate global sink state");
        let spill_count = global_hash_state.spill_data[0]
            .lock()
            .expect("lock spill slot")
            .as_ref()
            .expect("combined multi-local force_external state should spill")
            .count();
        assert_eq!(spill_count as usize, row_count);

        let global_source_state = operator
            .get_global_source_state(&ctx, Some(global_sink_state.as_ref()))
            .expect("create global source state");
        let mut local_source_state = operator
            .get_local_source_state(&ctx, global_source_state.as_ref())
            .expect("create local source state");

        let mut chunk =
            paro_common::test_utils::test_chunk_with_capacity(&operator.types, VECTOR_SIZE);
        let mut seen_rows = 0usize;
        loop {
            let mut source_input = OperatorSourceInput::new(
                global_source_state.as_ref(),
                local_source_state.as_mut(),
                &interrupt,
            );
            if !matches!(
                operator
                    .get_data(&ctx, &mut chunk, &mut source_input)
                    .expect("source output"),
                crate::result_type::SourceResultType::HaveMoreOutput
            ) {
                break;
            }
            seen_rows += chunk.size();
        }

        assert_eq!(seen_rows, row_count);

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[test]
    fn external_hash_aggregate_source_falls_back_to_stored_sink_state() {
        let (session, temp_dir) = make_session(32 * 1024 * 1024, true);
        let thread = ThreadContext::single_threaded();
        let ctx = ExecutionContext::new(session, &thread, None);
        let operator = build_test_operator();
        let global_sink_state: Arc<dyn GlobalSinkState> = Arc::from(
            operator
                .get_global_sink_state(&ctx)
                .expect("create global sink state"),
        );
        let mut local_sink_state = operator
            .get_local_sink_state(&ctx)
            .expect("create local sink state");
        let interrupt = InterruptState::new();

        let row_count = 50_000usize;
        let mut start = 1usize;
        while start <= row_count {
            let end = (start + VECTOR_SIZE).min(row_count + 1);
            let payload = build_payload_chunk(start, end);
            let mut sink_input = OperatorSinkInput::new(
                global_sink_state.as_ref(),
                local_sink_state.as_mut(),
                &interrupt,
            );
            operator
                .sink(&ctx, &payload, &mut sink_input)
                .expect("sink payload chunk");
            start = end;
        }

        let mut combine_input = OperatorSinkCombineInput::new(
            global_sink_state.as_ref(),
            local_sink_state.as_mut(),
            &interrupt,
        );
        operator
            .combine(&ctx, &mut combine_input)
            .expect("combine sink states");

        operator.set_sink_state(global_sink_state.clone());

        let global_source_state = operator
            .get_global_source_state(&ctx, None)
            .expect("create global source state from stored sink");
        let mut local_source_state = operator
            .get_local_source_state(&ctx, global_source_state.as_ref())
            .expect("create local source state");

        let mut chunk =
            paro_common::test_utils::test_chunk_with_capacity(&operator.types, VECTOR_SIZE);
        let mut seen_rows = 0usize;
        loop {
            let mut source_input = OperatorSourceInput::new(
                global_source_state.as_ref(),
                local_source_state.as_mut(),
                &interrupt,
            );
            if !matches!(
                operator
                    .get_data(&ctx, &mut chunk, &mut source_input)
                    .expect("source output"),
                crate::result_type::SourceResultType::HaveMoreOutput
            ) {
                break;
            }
            seen_rows += chunk.size();
        }

        assert_eq!(seen_rows, row_count);

        let _ = fs::remove_dir_all(temp_dir);
    }
}
