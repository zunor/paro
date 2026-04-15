// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Physical ungrouped aggregate operator.

use std::any::Any;
use std::cmp::Ordering;
use std::collections::HashSet;
use std::fmt;
use std::mem::size_of;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex};

use paro_common::allocator::{default_allocator, ArenaAllocator};
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_common::vector::{SelectionVector, Vector};
use paro_function::aggregate::{AggregateCombineType, AggregateInputData};

use crate::execution_context::ExecutionContext;
use crate::explain::explain_node::format_bound_expression;
use crate::explain::types::ExplainRuntimeStats;
use crate::operator::aggregate::aggregate_kernel::{
    combine_states, destroy_states, finalize_states, initialize_states, update_filtered_states,
    update_states, AggregatePayload,
};
use crate::operator::aggregate::aggregate_object::{
    create_validated_aggregate_objects, AggregateObject,
};
use crate::operator::aggregate::aggregate_state::AggregateStateLayout;
use crate::operator::aggregate::grouped_aggregate_data::GroupedAggregateData;
use crate::operator::state::{
    GlobalSinkState, GlobalSourceState, LocalSinkState, LocalSourceState, OperatorSinkCombineInput,
    OperatorSinkInput, OperatorSourceInput,
};
use crate::operator::PhysicalOperator;
use crate::operator_type::PhysicalOperatorType;
use crate::result_type::{SinkCombineResultType, SinkResultType, SourceResultType};

pub struct UngroupedAggregate {
    pub aggregate_data: GroupedAggregateData,
    pub aggregate_objects: Vec<AggregateObject>,
    pub child: Arc<dyn PhysicalOperator>,
    pub types: Vec<LogicalType>,
    layout: AggregateStateLayout,
    has_distinct: bool,
    has_ordered: bool,
    shared: Arc<UngroupedAggregateShared>,
}

impl UngroupedAggregate {
    pub fn new(
        aggregate_data: GroupedAggregateData,
        types: Vec<LogicalType>,
        child: Arc<dyn PhysicalOperator>,
    ) -> Result<Self> {
        let aggregate_objects = create_validated_aggregate_objects(&aggregate_data)?;
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
        let layout = AggregateStateLayout::new(&aggregate_objects)?;
        let shared = Arc::new(UngroupedAggregateShared::new(
            &layout,
            &aggregate_objects,
            ArenaAllocator::new(Arc::new(default_allocator())),
        )?);
        Ok(Self {
            aggregate_data,
            aggregate_objects,
            child,
            types,
            layout,
            has_distinct,
            has_ordered,
            shared,
        })
    }

    fn build_filter_selection(filter_vec: &Vector, row_count: usize) -> SelectionVector {
        let filter_format = filter_vec.decode(row_count);
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
        SelectionVector::from_indices(selected_rows)
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
        chunk: &Chunk,
    ) -> Result<Option<SelectionVector>> {
        let aggregate = &self.aggregate_objects[agg_idx];
        let filter_index = self.aggregate_data.aggregate_filters[agg_idx];
        if filter_index != aggregate.filter {
            return Err(paro_error::internal(format!(
                "Aggregate filter mismatch at index {agg_idx}: object={:?} plan={:?}",
                aggregate.filter, filter_index
            )));
        }
        let Some(filter_idx) = filter_index else {
            return Ok(None);
        };
        let filter_vec = chunk.column(filter_idx).ok_or_else(|| {
            paro_error::internal(format!(
                "Aggregate filter column not found in payload chunk: {filter_idx}"
            ))
        })?;
        if filter_vec.logical_type() != &LogicalType::Boolean {
            return Err(paro_error::internal(format!(
                "Aggregate filter payload type mismatch at index {agg_idx}: expected=BOOLEAN actual={:?}",
                filter_vec.logical_type()
            )));
        }
        Ok(Some(Self::build_filter_selection(filter_vec, chunk.size())))
    }

    fn collect_distinct_rows_for_aggregate(
        &self,
        agg_idx: usize,
        chunk: &Chunk,
        distinct_rows: &mut Option<HashSet<Vec<Value>>>,
    ) -> Result<()> {
        let aggregate = &self.aggregate_objects[agg_idx];
        if !aggregate.is_distinct() {
            return Ok(());
        }
        let input_indices = &self.aggregate_data.aggregate_inputs[agg_idx];
        let filter_selection = self.filter_selection_for_aggregate(agg_idx, chunk)?;
        let distinct = distinct_rows.get_or_insert_with(HashSet::new);

        let mut append_row = |row_idx: usize| -> Result<()> {
            let mut key = Vec::with_capacity(input_indices.len());
            for &input_idx in input_indices {
                let input_col = chunk.column(input_idx).ok_or_else(|| {
                    paro_error::internal(format!(
                        "Aggregate DISTINCT input column not found in payload chunk: {input_idx}"
                    ))
                })?;
                key.push(input_col.get_value(row_idx));
            }
            distinct.insert(key);
            Ok(())
        };

        if let Some(selection) = filter_selection {
            for idx in 0..selection.len() {
                append_row(selection.get(idx))?;
            }
        } else {
            for row_idx in 0..chunk.size() {
                append_row(row_idx)?;
            }
        }

        Ok(())
    }

    fn collect_ordered_rows_for_aggregate(
        &self,
        agg_idx: usize,
        chunk: &Chunk,
        ordered_rows: &mut Vec<Vec<Value>>,
    ) -> Result<()> {
        let aggregate = &self.aggregate_objects[agg_idx];
        if aggregate.order_bys.is_empty() {
            return Ok(());
        }
        let input_indices = &self.aggregate_data.aggregate_inputs[agg_idx];
        let order_indices = &self.aggregate_data.aggregate_orders[agg_idx];
        if order_indices != &aggregate.order_bys {
            return Err(paro_error::internal(format!(
                "Aggregate ORDER BY mapping mismatch at index {agg_idx}: object={:?} plan={:?}",
                aggregate.order_bys, order_indices
            )));
        }
        let filter_selection = self.filter_selection_for_aggregate(agg_idx, chunk)?;
        let expected_width = input_indices.len() + order_indices.len();

        let mut append_row = |row_idx: usize| -> Result<()> {
            let mut row_values = Vec::with_capacity(expected_width);
            for &input_idx in input_indices {
                let input_col = chunk.column(input_idx).ok_or_else(|| {
                    paro_error::internal(format!(
                        "Ordered aggregate input column not found in payload chunk: {input_idx}"
                    ))
                })?;
                row_values.push(input_col.get_value(row_idx));
            }
            for &order_idx in order_indices {
                let order_col = chunk.column(order_idx).ok_or_else(|| {
                    paro_error::internal(format!(
                        "Ordered aggregate ORDER BY column not found in payload chunk: {order_idx}"
                    ))
                })?;
                row_values.push(order_col.get_value(row_idx));
            }
            ordered_rows.push(row_values);
            Ok(())
        };

        if let Some(selection) = filter_selection {
            for idx in 0..selection.len() {
                append_row(selection.get(idx))?;
            }
        } else {
            for row_idx in 0..chunk.size() {
                append_row(row_idx)?;
            }
        }

        Ok(())
    }

    fn state_addresses_for_aggregate(
        &self,
        state: &mut UngroupedAggregateState,
        agg_idx: usize,
        row_count: usize,
    ) -> Result<Vector> {
        if agg_idx >= self.layout.aggregate_count() {
            return Err(paro_error::internal(format!(
                "Aggregate index out of bounds for state layout: agg_idx={agg_idx}, count={}",
                self.layout.aggregate_count()
            )));
        }

        let state_offset = self.layout.state_offset(agg_idx);
        let state_ptr = unsafe { state.base_ptr().add(state_offset) };
        let mut states = Vector::with_capacity(LogicalType::BigInt, row_count);
        states.set_count(row_count);
        let state_ptrs = unsafe { states.flat_data_mut::<*mut u8>() };
        for row_idx in 0..row_count {
            unsafe {
                *state_ptrs.add(row_idx) = state_ptr;
            }
        }
        Ok(states)
    }

    fn update_non_distinct_aggregate(
        &self,
        agg_idx: usize,
        chunk: &Chunk,
        state_addresses: &Vector,
        filter_selection: Option<&SelectionVector>,
        arena: &mut ArenaAllocator,
    ) -> Result<()> {
        let aggregate = &self.aggregate_objects[agg_idx];
        let aggregate_inputs = &self.aggregate_data.aggregate_inputs[agg_idx];
        let payload_desc = AggregatePayload {
            chunk,
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
                state_addresses,
                selection,
                selection.len(),
            )
        } else {
            update_states(
                std::slice::from_ref(aggregate),
                &mut input_data,
                &payload_desc,
                state_addresses,
                chunk.size(),
            )
        }
    }

    fn finalize_distinct_aggregates(
        &self,
        lstate: &mut UngroupedAggregateLocalSinkState,
    ) -> Result<()> {
        if lstate.distinct_finalized {
            return Ok(());
        }

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

            let row_values = rows.into_iter().collect::<Vec<_>>();
            let input_count = self.aggregate_data.aggregate_inputs[agg_idx].len();
            for row in &row_values {
                if row.len() != input_count {
                    return Err(paro_error::internal(format!(
                        "Ungrouped DISTINCT row width mismatch at aggregate {agg_idx}: expected={input_count}, actual={}",
                        row.len()
                    )));
                }
            }

            let input_types = self
                .aggregate_data
                .aggregate_expr(agg_idx)?
                .children
                .iter()
                .map(|child| child.return_type())
                .collect::<Vec<_>>();
            let mut input_vectors = input_types
                .into_iter()
                .map(|ty| Vector::with_capacity(ty, row_values.len()))
                .collect::<Vec<_>>();
            for input_vector in &mut input_vectors {
                input_vector.set_count(row_values.len());
            }
            for (row_idx, row) in row_values.iter().enumerate() {
                for input_idx in 0..input_count {
                    input_vectors[input_idx].set_value(row_idx, &row[input_idx]);
                }
            }
            let input_refs: Vec<&Vector> = input_vectors.iter().collect();

            let offset = self.layout.state_offset(agg_idx);
            let state_ptr = unsafe { lstate.state.base_ptr().add(offset) };
            let input_data = AggregateInputData::new(
                aggregate.bind_info.as_deref(),
                &mut lstate.state.arena_allocator,
                AggregateCombineType::PreserveInput,
            );
            unsafe {
                if let Some(simple_update) = aggregate.function.simple_update {
                    simple_update(&input_refs, &input_data, state_ptr, row_values.len());
                } else {
                    let mut states = Vector::new(LogicalType::BigInt);
                    states.set_count(row_values.len());
                    let state_ptrs = states.flat_data_mut::<*mut u8>();
                    for row_idx in 0..row_values.len() {
                        *state_ptrs.add(row_idx) = state_ptr;
                    }
                    (aggregate.function.update)(
                        &input_refs,
                        &input_data,
                        &states,
                        row_values.len(),
                    );
                }
            }
        }

        lstate.distinct_finalized = true;
        Ok(())
    }

    fn finalize_ordered_aggregates(
        &self,
        lstate: &mut UngroupedAggregateLocalSinkState,
    ) -> Result<()> {
        if lstate.ordered_finalized {
            return Ok(());
        }

        for agg_idx in 0..self.aggregate_objects.len() {
            let aggregate = &self.aggregate_objects[agg_idx];
            if aggregate.order_bys.is_empty() {
                continue;
            }

            let mut rows = std::mem::take(&mut lstate.ordered_rows[agg_idx]);
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
            let expected_len = input_count + order_count;
            for row in &rows {
                if row.len() != expected_len {
                    return Err(paro_error::internal(format!(
                        "Ungrouped ordered row width mismatch at aggregate {agg_idx}: expected={expected_len}, actual={}",
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
                    let lhs_value = &lhs[input_count + order_idx];
                    let rhs_value = &rhs[input_count + order_idx];
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

            let input_types = self
                .aggregate_data
                .aggregate_expr(agg_idx)?
                .children
                .iter()
                .map(|child| child.return_type())
                .collect::<Vec<_>>();
            let mut input_vectors = input_types
                .into_iter()
                .map(|ty| Vector::with_capacity(ty, rows.len()))
                .collect::<Vec<_>>();
            for input_vector in &mut input_vectors {
                input_vector.set_count(rows.len());
            }
            for (row_idx, row) in rows.iter().enumerate() {
                for input_idx in 0..input_count {
                    input_vectors[input_idx].set_value(row_idx, &row[input_idx]);
                }
            }
            let input_refs: Vec<&Vector> = input_vectors.iter().collect();

            let offset = self.layout.state_offset(agg_idx);
            let state_ptr = unsafe { lstate.state.base_ptr().add(offset) };
            let input_data = AggregateInputData::new(
                aggregate.bind_info.as_deref(),
                &mut lstate.state.arena_allocator,
                AggregateCombineType::PreserveInput,
            );
            unsafe {
                if let Some(simple_update) = aggregate.function.simple_update {
                    simple_update(&input_refs, &input_data, state_ptr, rows.len());
                } else {
                    let mut states = Vector::new(LogicalType::BigInt);
                    states.set_count(rows.len());
                    let state_ptrs = states.flat_data_mut::<*mut u8>();
                    for row_idx in 0..rows.len() {
                        *state_ptrs.add(row_idx) = state_ptr;
                    }
                    (aggregate.function.update)(&input_refs, &input_data, &states, rows.len());
                }
            }
        }

        lstate.ordered_finalized = true;
        Ok(())
    }
}

impl fmt::Debug for UngroupedAggregate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UngroupedAggregate")
            .field("types", &self.types)
            .field("layout", &self.layout)
            .finish()
    }
}

#[derive(Debug)]
struct UngroupedAggregateShared {
    state: Mutex<UngroupedAggregateState>,
    peak_memory_bytes: AtomicUsize,
}

impl UngroupedAggregateShared {
    fn new(
        layout: &AggregateStateLayout,
        aggregate_objects: &[AggregateObject],
        arena_allocator: ArenaAllocator,
    ) -> Result<Self> {
        let mut buffer = allocate_state_buffer(layout.total_size())?;
        initialize_state_buffer(layout, aggregate_objects, &mut buffer)?;
        let state = UngroupedAggregateState {
            state_buffer: buffer,
            arena_allocator,
            destroyed: false,
        };
        Ok(Self {
            peak_memory_bytes: AtomicUsize::new(state.memory_usage_bytes()),
            state: Mutex::new(state),
        })
    }

    fn record_peak(&self, bytes: usize) {
        self.peak_memory_bytes
            .fetch_max(bytes, AtomicOrdering::AcqRel);
    }

    fn peak_memory_bytes(&self) -> usize {
        self.peak_memory_bytes.load(AtomicOrdering::Acquire)
    }
}

#[derive(Debug)]
struct UngroupedAggregateGlobalState {
    shared: Arc<UngroupedAggregateShared>,
    aggregate_objects: Vec<AggregateObject>,
    destroy_on_drop: bool,
}

impl GlobalSinkState for UngroupedAggregateGlobalState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl GlobalSourceState for UngroupedAggregateGlobalState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[derive(Debug)]
struct UngroupedAggregateLocalSinkState {
    state: UngroupedAggregateState,
    aggregate_objects: Vec<AggregateObject>,
    ordered_rows: Vec<Vec<Vec<Value>>>,
    ordered_finalized: bool,
    distinct_rows: Vec<Option<HashSet<Vec<Value>>>>,
    distinct_finalized: bool,
}

impl LocalSinkState for UngroupedAggregateLocalSinkState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[derive(Debug)]
struct UngroupedAggregateLocalSourceState {
    finished: bool,
}

impl LocalSourceState for UngroupedAggregateLocalSourceState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

struct UngroupedAggregateState {
    // Keep aggregate states naturally aligned for typed state access (e.g. i64 in count/sum).
    state_buffer: Vec<u64>,
    arena_allocator: ArenaAllocator,
    destroyed: bool,
}

impl fmt::Debug for UngroupedAggregateState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UngroupedAggregateState")
            .field("state_bytes", &(self.state_buffer.len() * size_of::<u64>()))
            .field("destroyed", &self.destroyed)
            .finish()
    }
}

impl UngroupedAggregateState {
    fn memory_usage_bytes(&self) -> usize {
        self.state_buffer.capacity() * size_of::<u64>() + self.arena_allocator.allocation_size()
    }

    fn base_ptr(&mut self) -> *mut u8 {
        self.state_buffer.as_mut_ptr() as *mut u8
    }

    fn destroy_once(&mut self, aggregate_objects: &[AggregateObject]) -> Result<()> {
        if self.destroyed {
            return Ok(());
        }
        let addresses = single_state_addresses(self.base_ptr());
        let mut input_data = AggregateInputData::new(
            None,
            &mut self.arena_allocator,
            AggregateCombineType::PreserveInput,
        );
        destroy_states(aggregate_objects, &mut input_data, &addresses, 1)?;
        self.destroyed = true;
        Ok(())
    }
}

impl Drop for UngroupedAggregateLocalSinkState {
    fn drop(&mut self) {
        let _ = self.state.destroy_once(&self.aggregate_objects);
    }
}

impl Drop for UngroupedAggregateGlobalState {
    fn drop(&mut self) {
        if !self.destroy_on_drop {
            return;
        }
        if let Ok(mut guard) = self.shared.state.lock() {
            let _ = guard.destroy_once(&self.aggregate_objects);
        }
    }
}

fn single_state_addresses(base_ptr: *mut u8) -> Vector {
    let mut addresses = Vector::new(LogicalType::BigInt);
    addresses.set_count(1);
    unsafe {
        *addresses.flat_data_mut::<*mut u8>() = base_ptr;
    }
    addresses
}

fn values_memory_usage(values: &Vec<Value>) -> usize {
    values.capacity() * size_of::<Value>()
        + values.iter().map(Value::allocation_size).sum::<usize>()
}

fn nested_values_memory_usage(rows: &Vec<Vec<Value>>) -> usize {
    rows.capacity() * size_of::<Vec<Value>>()
        + rows
            .iter()
            .map(|row| values_memory_usage(row))
            .sum::<usize>()
}

fn distinct_rows_memory_usage(rows: &Vec<Option<HashSet<Vec<Value>>>>) -> usize {
    rows.capacity() * size_of::<Option<HashSet<Vec<Value>>>>()
        + rows
            .iter()
            .map(|row_set| {
                row_set
                    .as_ref()
                    .map(|set| {
                        set.iter()
                            .map(|row| values_memory_usage(row))
                            .sum::<usize>()
                            + set.len() * size_of::<Vec<Value>>()
                    })
                    .unwrap_or(0)
            })
            .sum::<usize>()
}

impl UngroupedAggregateLocalSinkState {
    fn memory_usage_bytes(&self) -> usize {
        self.state.memory_usage_bytes()
            + self.ordered_rows.capacity() * size_of::<Vec<Vec<Value>>>()
            + self
                .ordered_rows
                .iter()
                .map(|rows| nested_values_memory_usage(rows))
                .sum::<usize>()
            + distinct_rows_memory_usage(&self.distinct_rows)
    }
}

fn initialize_state_buffer(
    layout: &AggregateStateLayout,
    aggregate_objects: &[AggregateObject],
    state_buffer: &mut Vec<u64>,
) -> Result<()> {
    let buffer_bytes = state_buffer
        .len()
        .checked_mul(size_of::<u64>())
        .ok_or_else(|| {
            paro_error::internal(format!(
                "Ungrouped aggregate state buffer byte size overflow: words={}",
                state_buffer.len()
            ))
        })?;
    if buffer_bytes < layout.total_size() {
        return Err(paro_error::internal(format!(
            "Ungrouped aggregate state buffer too small: required={}, actual={}",
            layout.total_size(),
            buffer_bytes
        )));
    }
    let addresses = single_state_addresses(state_buffer.as_mut_ptr() as *mut u8);
    initialize_states(layout, aggregate_objects, &addresses, 1)
}

impl PhysicalOperator for UngroupedAggregate {
    fn operator_type(&self) -> PhysicalOperatorType {
        PhysicalOperatorType::UngroupedAggregate
    }

    fn runtime_memory_stats(&self) -> ExplainRuntimeStats {
        ExplainRuntimeStats {
            spilled: None,
            peak_memory_bytes: Some(self.shared.peak_memory_bytes() as u64),
            temp_storage_bytes: None,
        }
    }

    fn types(&self) -> &[LogicalType] {
        &self.types
    }

    fn explain_params(&self) -> Vec<String> {
        if self.aggregate_data.aggregates.is_empty() {
            return vec![];
        }

        let aggregates = self
            .aggregate_data
            .aggregates
            .iter()
            .map(format_bound_expression)
            .collect::<Vec<_>>()
            .join(", ");
        vec![format!("Aggregates: {aggregates}")]
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

    fn get_global_sink_state(&self, ctx: &ExecutionContext) -> Result<Box<dyn GlobalSinkState>> {
        {
            let mut state = self
                .shared
                .state
                .lock()
                .map_err(|e| paro_error::internal(e.to_string()))?;
            state.arena_allocator = ctx.arena_allocator();
            state.destroyed = false;
            initialize_state_buffer(
                &self.layout,
                &self.aggregate_objects,
                &mut state.state_buffer,
            )?;
        }
        Ok(Box::new(UngroupedAggregateGlobalState {
            shared: self.shared.clone(),
            aggregate_objects: self.aggregate_objects.clone(),
            destroy_on_drop: false,
        }))
    }

    fn get_local_sink_state(&self, ctx: &ExecutionContext) -> Result<Box<dyn LocalSinkState>> {
        let mut state_buffer = allocate_state_buffer(self.layout.total_size())?;
        initialize_state_buffer(&self.layout, &self.aggregate_objects, &mut state_buffer)?;

        Ok(Box::new(UngroupedAggregateLocalSinkState {
            state: UngroupedAggregateState {
                state_buffer,
                arena_allocator: ctx.arena_allocator(),
                destroyed: false,
            },
            aggregate_objects: self.aggregate_objects.clone(),
            ordered_rows: vec![Vec::new(); self.aggregate_objects.len()],
            ordered_finalized: false,
            distinct_rows: (0..self.aggregate_objects.len()).map(|_| None).collect(),
            distinct_finalized: false,
        }))
    }

    fn sink(
        &self,
        _ctx: &ExecutionContext,
        chunk: &Chunk,
        input: &mut OperatorSinkInput,
    ) -> Result<SinkResultType> {
        if chunk.size() == 0 {
            return Ok(SinkResultType::NeedMoreInput);
        }

        let lstate = input
            .local_state
            .as_any_mut()
            .downcast_mut::<UngroupedAggregateLocalSinkState>()
            .ok_or_else(|| paro_error::internal("Invalid local sink state".to_string()))?;

        for (agg_idx, aggregate) in self.aggregate_objects.iter().enumerate() {
            if !aggregate.order_bys.is_empty() {
                self.collect_ordered_rows_for_aggregate(
                    agg_idx,
                    chunk,
                    &mut lstate.ordered_rows[agg_idx],
                )?;
                continue;
            }
            if aggregate.is_distinct() {
                self.collect_distinct_rows_for_aggregate(
                    agg_idx,
                    chunk,
                    &mut lstate.distinct_rows[agg_idx],
                )?;
                continue;
            }

            let filter_selection = self.filter_selection_for_aggregate(agg_idx, chunk)?;
            let state_addresses =
                self.state_addresses_for_aggregate(&mut lstate.state, agg_idx, chunk.size())?;
            self.update_non_distinct_aggregate(
                agg_idx,
                chunk,
                &state_addresses,
                filter_selection.as_ref(),
                &mut lstate.state.arena_allocator,
            )?;
        }

        self.shared.record_peak(lstate.memory_usage_bytes());

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
            .downcast_ref::<UngroupedAggregateGlobalState>()
            .ok_or_else(|| paro_error::internal("Invalid global sink state".to_string()))?;
        let lstate = input
            .local_state
            .as_any_mut()
            .downcast_mut::<UngroupedAggregateLocalSinkState>()
            .ok_or_else(|| paro_error::internal("Invalid local sink state".to_string()))?;
        self.finalize_ordered_aggregates(lstate)?;
        self.finalize_distinct_aggregates(lstate)?;

        let mut gstate_guard = gstate
            .shared
            .state
            .lock()
            .map_err(|e| paro_error::internal(e.to_string()))?;
        let source_states = single_state_addresses(lstate.state.base_ptr());
        let target_states = single_state_addresses(gstate_guard.base_ptr());
        let mut input_data = AggregateInputData::new(
            None,
            &mut gstate_guard.arena_allocator,
            AggregateCombineType::AllowDestructive,
        );
        combine_states(
            &self.aggregate_objects,
            &mut input_data,
            &source_states,
            &target_states,
            1,
        )?;
        self.shared.record_peak(gstate_guard.memory_usage_bytes());
        lstate.state.destroy_once(&self.aggregate_objects)?;

        Ok(SinkCombineResultType::Finished)
    }

    fn get_global_source_state(
        &self,
        _ctx: &ExecutionContext,
        _sink_state: Option<&dyn GlobalSinkState>,
    ) -> Result<Box<dyn GlobalSourceState>> {
        Ok(Box::new(UngroupedAggregateGlobalState {
            shared: self.shared.clone(),
            aggregate_objects: self.aggregate_objects.clone(),
            destroy_on_drop: true,
        }))
    }

    fn get_local_source_state(
        &self,
        _ctx: &ExecutionContext,
        _gstate: &dyn GlobalSourceState,
    ) -> Result<Box<dyn LocalSourceState>> {
        Ok(Box::new(UngroupedAggregateLocalSourceState {
            finished: false,
        }))
    }

    fn get_data(
        &self,
        _ctx: &ExecutionContext,
        chunk: &mut Chunk,
        input: &mut OperatorSourceInput,
    ) -> Result<SourceResultType> {
        let gstate = input
            .global_state
            .as_any()
            .downcast_ref::<UngroupedAggregateGlobalState>()
            .ok_or_else(|| paro_error::internal("Invalid global source state".to_string()))?;
        let lstate = input
            .local_state
            .as_any_mut()
            .downcast_mut::<UngroupedAggregateLocalSourceState>()
            .ok_or_else(|| paro_error::internal("Invalid local source state".to_string()))?;

        if lstate.finished {
            chunk.set_cardinality(0);
            return Ok(SourceResultType::Finished);
        }

        let mut gstate_guard = gstate
            .shared
            .state
            .lock()
            .map_err(|e| paro_error::internal(e.to_string()))?;
        let state_addresses = single_state_addresses(gstate_guard.base_ptr());
        let mut input_data = AggregateInputData::new(
            None,
            &mut gstate_guard.arena_allocator,
            AggregateCombineType::PreserveInput,
        );
        finalize_states(
            &self.aggregate_objects,
            &mut input_data,
            &state_addresses,
            chunk,
            1,
        )?;
        gstate_guard.destroy_once(&self.aggregate_objects)?;

        lstate.finished = true;
        Ok(SourceResultType::HaveMoreOutput)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

fn allocate_state_buffer(total_bytes: usize) -> Result<Vec<u64>> {
    let word = size_of::<u64>();
    let words = total_bytes.checked_add(word - 1).ok_or_else(|| {
        paro_error::internal(format!(
            "Ungrouped aggregate state allocation overflow: total_bytes={total_bytes}"
        ))
    })? / word;
    Ok(vec![0u64; words])
}
