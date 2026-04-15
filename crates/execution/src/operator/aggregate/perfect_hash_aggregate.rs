//! Physical grouped perfect-hash aggregate operator.

use std::any::Any;
use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use paro_common::allocator::ArenaAllocator;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_common::vector::{SelectionVector, Vector};
use paro_function::aggregate::{AggregateCombineType, AggregateInputData};

use crate::execution_context::ExecutionContext;
use crate::explain::explain_node::format_bound_expression;
use crate::explain::types::ExplainRuntimeStats;
use crate::operator::aggregate::aggregate_kernel::{
    update_filtered_states, update_states, AggregatePayload,
};
use crate::operator::aggregate::aggregate_object::{
    create_validated_aggregate_objects, AggregateObject,
};
use crate::operator::aggregate::aggregate_state::AggregateStateLayout;
use crate::operator::aggregate::grouped_aggregate_data::{reference_index, GroupedAggregateData};
use crate::operator::aggregate::perfect_aggregate_hashtable::{
    PerfectAggregateHashTable, PerfectHTScanPosition,
};
use crate::operator::state::{
    GlobalSinkState, GlobalSourceState, LocalSinkState, LocalSourceState, OperatorSinkCombineInput,
    OperatorSinkInput, OperatorSourceInput,
};
use crate::operator::PhysicalOperator;
use crate::operator_type::PhysicalOperatorType;
use crate::result_type::{SinkCombineResultType, SinkResultType, SourceResultType};

pub struct PerfectHashAggregate {
    pub aggregate_data: GroupedAggregateData,
    pub aggregate_objects: Vec<AggregateObject>,
    pub child: Arc<dyn PhysicalOperator>,
    pub types: Vec<LogicalType>,
    layout: AggregateStateLayout,
    group_payload_refs: Vec<usize>,
    group_types: Vec<LogicalType>,
    has_filters: bool,
    group_minima: Vec<i128>,
    required_bits: Vec<usize>,
    shared: Arc<PerfectHashAggregateShared>,
}

impl PerfectHashAggregate {
    pub fn new(
        aggregate_data: GroupedAggregateData,
        types: Vec<LogicalType>,
        child: Arc<dyn PhysicalOperator>,
        group_minima: Vec<i128>,
        required_bits: Vec<usize>,
    ) -> Result<Self> {
        let aggregate_objects = create_validated_aggregate_objects(&aggregate_data)?;
        for (agg_idx, object) in aggregate_objects.iter().enumerate() {
            if object.is_distinct() {
                return Err(paro_error::internal(format!(
                    "Perfect hash aggregate does not support DISTINCT aggregate: agg_idx={agg_idx}"
                )));
            }
            if !object.order_bys.is_empty() {
                return Err(paro_error::internal(format!(
                    "Perfect hash aggregate does not support ordered aggregate: agg_idx={agg_idx}"
                )));
            }
        }
        if !aggregate_data.grouping_functions.is_empty() {
            return Err(paro_error::internal(
                "Perfect hash aggregate does not support GROUPING() functions".to_string(),
            ));
        }
        if aggregate_data.grouping_sets.len() > 1 {
            return Err(paro_error::internal(
                "Perfect hash aggregate does not support multiple grouping sets".to_string(),
            ));
        }

        let mut group_payload_refs = Vec::with_capacity(aggregate_data.groups.len());
        let mut group_types = Vec::with_capacity(aggregate_data.groups.len());
        for group_expr in &aggregate_data.groups {
            group_payload_refs.push(reference_index(group_expr)?);
            group_types.push(group_expr.return_type());
        }
        if group_types.len() != group_minima.len() || group_types.len() != required_bits.len() {
            return Err(paro_error::internal(format!(
                "Perfect hash group metadata mismatch: groups={} minima={} bits={}",
                group_types.len(),
                group_minima.len(),
                required_bits.len()
            )));
        }

        let expected_types = group_payload_refs.len()
            + aggregate_objects.len()
            + aggregate_data.grouping_functions.len();
        if types.len() != expected_types {
            return Err(paro_error::internal(format!(
                "PerfectHashAggregate output type mismatch: expected={expected_types}, actual={}",
                types.len()
            )));
        }

        let layout = AggregateStateLayout::new(&aggregate_objects)?;
        let has_filters = aggregate_objects.iter().any(|obj| obj.filter.is_some());
        let global_hash_table = PerfectAggregateHashTable::new(
            group_types.clone(),
            aggregate_objects.clone(),
            aggregate_data.aggregate_inputs.clone(),
            group_minima.clone(),
            required_bits.clone(),
        )?;

        Ok(Self {
            aggregate_data,
            aggregate_objects,
            child,
            types,
            layout,
            group_payload_refs,
            group_types,
            has_filters,
            group_minima,
            required_bits,
            shared: Arc::new(PerfectHashAggregateShared::new(global_hash_table)),
        })
    }

    fn build_groups_chunk(&self, payload: &Chunk) -> Result<Chunk> {
        let mut group_vectors = Vec::with_capacity(self.group_payload_refs.len());
        for (group_idx, payload_idx) in self.group_payload_refs.iter().enumerate() {
            let group_vector = Arc::clone(payload.column(*payload_idx).ok_or_else(|| {
                paro_error::internal(format!(
                    "Group payload column not found: group_idx={group_idx}, payload_idx={payload_idx}"
                ))
            })?);
            group_vectors.push(group_vector);
        }
        let mut groups = Chunk::from_arc_vectors(group_vectors);
        groups.set_cardinality(payload.size());
        Ok(groups)
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
        )))
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

        let mut selected = Vector::with_capacity(LogicalType::BigInt, selection.len());
        selected.set_count(selection.len());
        let selected_data = unsafe { selected.flat_data_mut::<*mut u8>() };

        let address_format = addresses.decode(addresses.len());
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

    fn update_aggregates_with_filters(
        &self,
        payload: &Chunk,
        addresses: &Vector,
        arena: &mut ArenaAllocator,
    ) -> Result<()> {
        let all_rows = SelectionVector::incremental(payload.size());
        for agg_idx in 0..self.aggregate_objects.len() {
            let aggregate_states = self.selected_state_addresses(addresses, &all_rows, agg_idx)?;
            let filter_selection = self.filter_selection_for_aggregate(agg_idx, payload)?;
            self.update_non_distinct_aggregate(
                agg_idx,
                payload,
                &aggregate_states,
                filter_selection.as_ref(),
                arena,
            )?;
        }
        Ok(())
    }

    fn new_hash_table(&self) -> Result<PerfectAggregateHashTable> {
        PerfectAggregateHashTable::new(
            self.group_types.clone(),
            self.aggregate_objects.clone(),
            self.aggregate_data.aggregate_inputs.clone(),
            self.group_minima.clone(),
            self.required_bits.clone(),
        )
    }
}

impl fmt::Debug for PerfectHashAggregate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PerfectHashAggregate")
            .field("types", &self.types)
            .field("group_types", &self.group_types)
            .field("aggregate_count", &self.aggregate_objects.len())
            .finish()
    }
}

#[derive(Debug)]
struct PerfectHashAggregateShared {
    hash_table: Mutex<PerfectAggregateHashTable>,
    peak_memory_bytes: AtomicUsize,
}

impl PerfectHashAggregateShared {
    fn new(hash_table: PerfectAggregateHashTable) -> Self {
        let initial = hash_table.memory_usage();
        Self {
            hash_table: Mutex::new(hash_table),
            peak_memory_bytes: AtomicUsize::new(initial),
        }
    }

    fn record_peak(&self, bytes: usize) {
        self.peak_memory_bytes.fetch_max(bytes, Ordering::AcqRel);
    }

    fn peak_memory_bytes(&self) -> usize {
        self.peak_memory_bytes.load(Ordering::Acquire)
    }
}

#[derive(Debug)]
struct PerfectHashAggregateGlobalState {
    shared: Arc<PerfectHashAggregateShared>,
}

impl GlobalSinkState for PerfectHashAggregateGlobalState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn sink_state_name(&self) -> &str {
        "PerfectHashAggregateGlobalState"
    }
}

impl GlobalSourceState for PerfectHashAggregateGlobalState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[derive(Debug)]
struct PerfectHashAggregateLocalSinkState {
    hash_table: PerfectAggregateHashTable,
}

impl LocalSinkState for PerfectHashAggregateLocalSinkState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[derive(Debug)]
struct PerfectHashAggregateLocalSourceState {
    position: PerfectHTScanPosition,
}

impl LocalSourceState for PerfectHashAggregateLocalSourceState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl PhysicalOperator for PerfectHashAggregate {
    fn operator_type(&self) -> PhysicalOperatorType {
        PhysicalOperatorType::PerfectHashGroupBy
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

    fn get_global_sink_state(&self, _ctx: &ExecutionContext) -> Result<Box<dyn GlobalSinkState>> {
        Ok(Box::new(PerfectHashAggregateGlobalState {
            shared: self.shared.clone(),
        }))
    }

    fn get_local_sink_state(&self, _ctx: &ExecutionContext) -> Result<Box<dyn LocalSinkState>> {
        Ok(Box::new(PerfectHashAggregateLocalSinkState {
            hash_table: self.new_hash_table()?,
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

        let lstate = input
            .local_state
            .as_any_mut()
            .downcast_mut::<PerfectHashAggregateLocalSinkState>()
            .ok_or_else(|| paro_error::internal("Invalid local sink state".to_string()))?;

        let groups = self.build_groups_chunk(chunk)?;
        let mut addresses = Vector::with_capacity(LogicalType::BigInt, chunk.size());
        let mut new_groups = SelectionVector::with_capacity(chunk.size());
        lstate
            .hash_table
            .find_or_create_groups(&groups, &mut addresses, &mut new_groups)?;

        if !self.has_filters {
            lstate
                .hash_table
                .update_aggregates(chunk, &addresses, None)?;
            self.shared.record_peak(lstate.hash_table.memory_usage());
            return Ok(SinkResultType::NeedMoreInput);
        }

        let mut arena = ctx.arena_allocator();
        self.update_aggregates_with_filters(chunk, &addresses, &mut arena)?;
        self.shared.record_peak(lstate.hash_table.memory_usage());
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
            .downcast_ref::<PerfectHashAggregateGlobalState>()
            .ok_or_else(|| paro_error::internal("Invalid global sink state".to_string()))?;
        let lstate = input
            .local_state
            .as_any_mut()
            .downcast_mut::<PerfectHashAggregateLocalSinkState>()
            .ok_or_else(|| paro_error::internal("Invalid local sink state".to_string()))?;

        let mut ght = gstate
            .shared
            .hash_table
            .lock()
            .map_err(|e| paro_error::internal(e.to_string()))?;
        ght.combine(&mut lstate.hash_table)?;
        gstate.shared.record_peak(ght.memory_usage());
        Ok(SinkCombineResultType::Finished)
    }

    fn get_global_source_state(
        &self,
        _ctx: &ExecutionContext,
        _sink_state: Option<&dyn GlobalSinkState>,
    ) -> Result<Box<dyn GlobalSourceState>> {
        Ok(Box::new(PerfectHashAggregateGlobalState {
            shared: self.shared.clone(),
        }))
    }

    fn get_local_source_state(
        &self,
        _ctx: &ExecutionContext,
        _gstate: &dyn GlobalSourceState,
    ) -> Result<Box<dyn LocalSourceState>> {
        Ok(Box::new(PerfectHashAggregateLocalSourceState {
            position: PerfectHTScanPosition::default(),
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
            .downcast_ref::<PerfectHashAggregateGlobalState>()
            .ok_or_else(|| paro_error::internal("Invalid global source state".to_string()))?;
        let lstate = input
            .local_state
            .as_any_mut()
            .downcast_mut::<PerfectHashAggregateLocalSourceState>()
            .ok_or_else(|| paro_error::internal("Invalid local source state".to_string()))?;

        let mut ght = gstate
            .shared
            .hash_table
            .lock()
            .map_err(|e| paro_error::internal(e.to_string()))?;
        if ght.scan(&mut lstate.position, chunk)? {
            return Ok(SourceResultType::HaveMoreOutput);
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
