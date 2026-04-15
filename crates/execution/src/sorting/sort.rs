use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::sort_key::{OrderModifiers, SortKeyEncoding};
use paro_common::types::LogicalType;
use paro_common::vector::VECTOR_SIZE;
use paro_planner::binder::ir::OrderByNode;
use paro_planner::expression::Expression;
use paro_storage::meta::DEFAULT_SORT_PARTITION_SIZE;
use paro_storage::row::{RowLayout, RowValidityType};

use crate::execution_context::ExecutionContext;
use crate::operator::state::{
    GlobalSinkState, GlobalSourceState, LocalSinkState, LocalSourceState,
};
use crate::result_type::{
    SinkCombineResultType, SinkFinalizeType, SinkResultType, SourceResultType,
};

use super::sort_projection_column::SortProjectionColumn;
use super::sorted_run::{RunBuilder, SortedRun};
use super::sorted_run_merger::{
    SortedRunMerger, SortedRunMergerGlobalState, SortedRunMergerLocalState,
};

#[derive(Debug)]
pub struct Sort {
    orders: Vec<OrderByNode>,
    sort_key_modifiers: Vec<OrderModifiers>,
    sort_key_encoding: Arc<SortKeyEncoding>,
    key_layout: Arc<RowLayout>,
    payload_layout: Arc<RowLayout>,
    input_projection_map: Vec<usize>,
    output_projection_columns: Vec<SortProjectionColumn>,
    is_index_sort: bool,
}

impl Sort {
    pub fn new(
        orders: Vec<OrderByNode>,
        input_types: Vec<LogicalType>,
        projection_map: Vec<usize>,
        is_index_sort: bool,
    ) -> Result<Self> {
        let projection_map = if projection_map.is_empty() {
            (0..input_types.len()).collect()
        } else {
            projection_map
        };

        let sort_key_modifiers: Vec<OrderModifiers> = orders
            .iter()
            .map(|order| OrderModifiers::new(order.ascending, order.nulls_first))
            .collect();

        let mut input_column_to_key = HashMap::new();
        for (key_idx, order) in orders.iter().enumerate() {
            if let Expression::ColumnRef(col_ref) = &order.expression {
                input_column_to_key.insert(col_ref.binding.column_index, key_idx);
            } else if let Expression::Reference(reference) = &order.expression {
                input_column_to_key.insert(reference.index, key_idx);
            }
        }

        let key_types = orders
            .iter()
            .map(|order| Self::get_expression_type(&order.expression))
            .collect::<Result<Vec<_>>>()?;
        let sort_key_encoding = Arc::new(SortKeyEncoding::new(
            key_types.clone(),
            sort_key_modifiers.clone(),
        )?);

        let mut payload_types = Vec::new();
        let mut input_projection_map = Vec::new();
        let mut output_projection_columns = Vec::new();
        for (output_col_idx, &input_col_idx) in projection_map.iter().enumerate() {
            if let Some(&key_idx) = input_column_to_key.get(&input_col_idx) {
                output_projection_columns.push(SortProjectionColumn::new(
                    false,
                    key_idx,
                    output_col_idx,
                ));
            } else {
                output_projection_columns.push(SortProjectionColumn::new(
                    true,
                    payload_types.len(),
                    output_col_idx,
                ));
                payload_types.push(input_types[input_col_idx].clone());
                input_projection_map.push(input_col_idx);
            }
        }
        output_projection_columns
            .sort_by(|left, right| left.output_col_idx.cmp(&right.output_col_idx));

        Ok(Self {
            orders,
            sort_key_modifiers,
            sort_key_encoding,
            key_layout: Arc::new(RowLayout::from_types(
                key_types,
                RowValidityType::CanHaveNullValues,
            )),
            payload_layout: Arc::new(RowLayout::from_types(
                payload_types,
                RowValidityType::CanHaveNullValues,
            )),
            input_projection_map,
            output_projection_columns,
            is_index_sort,
        })
    }

    fn get_expression_type(expr: &Expression) -> Result<LogicalType> {
        Ok(expr.return_type())
    }

    pub fn orders(&self) -> &[OrderByNode] {
        &self.orders
    }

    pub fn sort_key_modifiers(&self) -> &[OrderModifiers] {
        &self.sort_key_modifiers
    }

    pub fn sort_key_encoding(&self) -> &Arc<SortKeyEncoding> {
        &self.sort_key_encoding
    }

    pub fn key_layout(&self) -> &Arc<RowLayout> {
        &self.key_layout
    }

    pub fn payload_layout(&self) -> &Arc<RowLayout> {
        &self.payload_layout
    }

    pub fn input_projection_map(&self) -> &[usize] {
        &self.input_projection_map
    }

    pub fn output_projection_columns(&self) -> &[SortProjectionColumn] {
        &self.output_projection_columns
    }

    fn output_types(&self) -> Vec<LogicalType> {
        self.output_projection_columns
            .iter()
            .map(|projection| {
                let types = if projection.is_payload {
                    self.payload_layout.types()
                } else {
                    self.key_layout.types()
                };
                types[projection.layout_col_idx].clone()
            })
            .collect()
    }

    fn prepare_output_chunk(&self, chunk: &mut Chunk) {
        let output_types = self.output_types();
        let chunk_types = chunk.types();
        let needs_reinit = chunk.column_count() != output_types.len()
            || chunk.capacity() < VECTOR_SIZE
            || chunk_types != output_types;

        if needs_reinit {
            let allocator = chunk.allocator().clone();
            *chunk = Chunk::initialize_with_allocator(&output_types, VECTOR_SIZE, allocator);
        } else {
            chunk.reset();
        }
    }

    pub fn is_index_sort(&self) -> bool {
        self.is_index_sort
    }

    pub fn get_local_sink_state(&self, _ctx: &ExecutionContext) -> Result<Box<dyn LocalSinkState>> {
        Ok(Box::new(SortLocalSinkState::new(self)))
    }

    pub fn get_global_sink_state(
        &self,
        ctx: &ExecutionContext,
    ) -> Result<Box<dyn GlobalSinkState>> {
        let temp_memory_mgr = ctx.temporary_memory_manager();
        let temp_memory_state = temp_memory_mgr.register();
        let config = temp_memory_mgr.current_config();
        let external = config.force_external;
        if external && !config.has_temporary_directory {
            return Err(paro_error::out_of_memory(
                "force_external requires a temporary directory (SET temp_directory)",
            ));
        }

        Ok(Box::new(SortGlobalSinkState::new(
            ctx.num_threads(),
            temp_memory_state,
            external,
        )))
    }

    pub fn sink(
        &self,
        ctx: &ExecutionContext,
        chunk: &Chunk,
        global_state: &dyn GlobalSinkState,
        local_state: &mut dyn LocalSinkState,
    ) -> Result<SinkResultType> {
        let gstate = global_state
            .as_any()
            .downcast_ref::<SortGlobalSinkState>()
            .expect("invalid sort global sink state");
        let lstate = local_state
            .as_any_mut()
            .downcast_mut::<SortLocalSinkState>()
            .expect("invalid sort local sink state");

        if lstate.run_builder.is_none() {
            lstate.run_builder = Some(RunBuilder::new(
                Arc::clone(ctx.buffer_pool()),
                Arc::clone(&self.key_layout),
                Arc::clone(&self.payload_layout),
                Arc::clone(&self.sort_key_encoding),
            ));
            gstate.update_local_state(lstate);
        }

        lstate.key_chunk = build_key_chunk(chunk, &self.orders);
        lstate.payload_chunk = build_payload_chunk(chunk, &self.input_projection_map);

        if let Some(run_builder) = lstate.run_builder.as_mut() {
            run_builder.sink(&lstate.key_chunk, &lstate.payload_chunk)?;
            self.try_finish_sink(ctx, gstate, lstate)?;
        }

        Ok(SinkResultType::NeedMoreInput)
    }

    fn try_finish_sink(
        &self,
        ctx: &ExecutionContext,
        gstate: &SortGlobalSinkState,
        lstate: &mut SortLocalSinkState,
    ) -> Result<bool> {
        let Some(run_builder) = lstate.run_builder.as_ref() else {
            return Ok(false);
        };
        let run_size = run_builder.size_in_bytes();
        if let Some(state) = gstate.temporary_memory_state() {
            state.set_remaining_size(run_size);
        }
        if run_size < lstate.maximum_run_size {
            return Ok(true);
        }

        if !gstate.try_increase_reservation(lstate) && lstate.external {
            gstate.mark_external();
            if let Some(run_builder) = lstate.run_builder.take() {
                let run = run_builder.finish(true)?;
                gstate.add_sorted_run(run);
            }
            lstate.run_builder = Some(RunBuilder::new(
                Arc::clone(ctx.buffer_pool()),
                Arc::clone(&self.key_layout),
                Arc::clone(&self.payload_layout),
                Arc::clone(&self.sort_key_encoding),
            ));
            return Ok(true);
        }

        gstate.update_local_state(lstate);
        Ok(false)
    }

    pub fn combine(
        &self,
        _ctx: &ExecutionContext,
        global_state: &dyn GlobalSinkState,
        local_state: &mut dyn LocalSinkState,
    ) -> Result<SinkCombineResultType> {
        let gstate = global_state
            .as_any()
            .downcast_ref::<SortGlobalSinkState>()
            .expect("invalid sort global sink state");
        let lstate = local_state
            .as_any_mut()
            .downcast_mut::<SortLocalSinkState>()
            .expect("invalid sort local sink state");

        if let Some(run_builder) = lstate.run_builder.take() {
            gstate.set_any_combined();
            gstate.add_sorted_run(run_builder.finish(gstate.is_external())?);
        }

        Ok(SinkCombineResultType::Finished)
    }

    pub fn finalize(&self, global_state: &dyn GlobalSinkState) -> Result<SinkFinalizeType> {
        let gstate = global_state
            .as_any()
            .downcast_ref::<SortGlobalSinkState>()
            .expect("invalid sort global sink state");

        if gstate.sorted_runs_count() == 0 {
            return Ok(SinkFinalizeType::NoOutputPossible);
        }

        let total_count = {
            let runs = gstate.sorted_runs.lock().unwrap();
            runs.iter().map(SortedRun::count).sum::<usize>()
        };
        *gstate.total_count.lock().unwrap() = total_count;
        *gstate.partition_size.lock().unwrap() = if gstate.num_threads <= 1 {
            VECTOR_SIZE
        } else {
            total_count.min(DEFAULT_SORT_PARTITION_SIZE)
        };

        Ok(SinkFinalizeType::Ready)
    }

    pub fn get_local_source_state(
        &self,
        _ctx: &ExecutionContext,
        _global_state: &dyn GlobalSourceState,
    ) -> Result<Box<dyn LocalSourceState>> {
        Ok(Box::new(SortLocalSourceState::new()))
    }

    pub fn get_global_source_state(
        &self,
        _ctx: &ExecutionContext,
        sink_state: &dyn GlobalSinkState,
    ) -> Result<Box<dyn GlobalSourceState>> {
        let gstate = sink_state
            .as_any()
            .downcast_ref::<SortGlobalSinkState>()
            .expect("invalid sort global sink state");

        let runs = {
            let mut locked_runs = gstate.sorted_runs.lock().unwrap();
            std::mem::take(&mut *locked_runs)
        };

        let count = *gstate.total_count.lock().unwrap();
        let external = gstate.is_external();
        let partition_size = *gstate.partition_size.lock().unwrap();
        let temporary_memory_state = gstate.take_temporary_memory_state();
        if let Some(state) = temporary_memory_state.as_ref() {
            state.set_zero();
        }

        let mut merger = None;
        let mut merger_gstate = None;
        let mut single_run = None;
        if !runs.is_empty() {
            let sort_arc = Arc::new(Self {
                orders: self.orders.clone(),
                sort_key_modifiers: self.sort_key_modifiers.clone(),
                sort_key_encoding: Arc::clone(&self.sort_key_encoding),
                key_layout: Arc::clone(&self.key_layout),
                payload_layout: Arc::clone(&self.payload_layout),
                input_projection_map: self.input_projection_map.clone(),
                output_projection_columns: self.output_projection_columns.clone(),
                is_index_sort: self.is_index_sort,
            });
            if !external && runs.len() == 1 {
                single_run = Some(Arc::new(runs.into_iter().next().expect("single run")));
            } else {
                merger = Some(Arc::new(SortedRunMerger::new(
                    Arc::clone(&sort_arc),
                    runs,
                    partition_size,
                    external,
                )));
                merger_gstate = Some(Arc::new(SortedRunMergerGlobalState::new(
                    count,
                    partition_size,
                    external,
                    gstate.num_threads,
                )));
            }
        }

        Ok(Box::new(SortGlobalSourceState::new(
            single_run,
            merger,
            merger_gstate,
            count,
            temporary_memory_state,
        )))
    }

    pub fn get_data(
        &self,
        _ctx: &ExecutionContext,
        chunk: &mut Chunk,
        global_state: &dyn GlobalSourceState,
        local_state: &mut dyn LocalSourceState,
    ) -> Result<SourceResultType> {
        let gstate = global_state
            .as_any()
            .downcast_ref::<SortGlobalSourceState>()
            .expect("invalid sort global source state");

        if gstate.total_count() == 0 {
            gstate.release_temporary_memory_state();
            return Ok(SourceResultType::Finished);
        }

        if let Some(single_run) = gstate.single_run() {
            let lstate = local_state
                .as_any_mut()
                .downcast_mut::<SortLocalSourceState>()
                .expect("invalid sort local source state");
            self.prepare_output_chunk(chunk);
            single_run.scan(
                chunk,
                lstate.current_position,
                self.output_projection_columns(),
            )?;
            lstate.current_position += chunk.size();
            if lstate.current_position >= gstate.total_count() {
                gstate.release_temporary_memory_state();
                return Ok(SourceResultType::Finished);
            }
            return Ok(SourceResultType::HaveMoreOutput);
        }

        if let (Some(merger), Some(merger_gstate)) = (gstate.merger(), gstate.merger_gstate()) {
            let lstate = local_state
                .as_any_mut()
                .downcast_mut::<SortLocalSourceState>()
                .expect("invalid sort local source state");
            self.prepare_output_chunk(chunk);
            let result = merger.get_data(chunk, &merger_gstate, &mut lstate.merger_lstate)?;
            if result == SourceResultType::Finished {
                gstate.release_temporary_memory_state();
            }
            return Ok(result);
        }

        chunk.set_cardinality(0);
        Ok(SourceResultType::Finished)
    }
}

#[derive(Debug)]
pub struct SortLocalSinkState {
    run_builder: Option<RunBuilder>,
    maximum_run_size: usize,
    external: bool,
    key_chunk: Chunk,
    payload_chunk: Chunk,
}

impl SortLocalSinkState {
    pub fn new(sort: &Sort) -> Self {
        Self {
            run_builder: None,
            maximum_run_size: 0,
            external: false,
            key_chunk: Chunk::init_empty(sort.key_layout.types()),
            payload_chunk: Chunk::init_empty(sort.payload_layout.types()),
        }
    }
}

impl LocalSinkState for SortLocalSinkState {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

pub struct SortGlobalSinkState {
    sorted_runs: Arc<Mutex<Vec<SortedRun>>>,
    sorted_tuples: Mutex<usize>,
    external: AtomicBool,
    any_combined: AtomicBool,
    temporary_memory_state: Mutex<Option<Arc<paro_storage::buffer::TemporaryMemoryState>>>,
    num_threads: usize,
    pub(crate) total_count: Mutex<usize>,
    pub(crate) partition_size: Mutex<usize>,
}

impl std::fmt::Debug for SortGlobalSinkState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SortGlobalSinkState")
            .field("external", &self.is_external())
            .field("num_threads", &self.num_threads)
            .finish()
    }
}

impl SortGlobalSinkState {
    pub fn new(
        num_threads: usize,
        temporary_memory_state: Arc<paro_storage::buffer::TemporaryMemoryState>,
        external: bool,
    ) -> Self {
        Self {
            sorted_runs: Arc::new(Mutex::new(Vec::new())),
            sorted_tuples: Mutex::new(0),
            external: AtomicBool::new(external),
            any_combined: AtomicBool::new(false),
            temporary_memory_state: Mutex::new(Some(temporary_memory_state)),
            num_threads,
            total_count: Mutex::new(0),
            partition_size: Mutex::new(DEFAULT_SORT_PARTITION_SIZE),
        }
    }

    fn temporary_memory_state(&self) -> Option<Arc<paro_storage::buffer::TemporaryMemoryState>> {
        self.temporary_memory_state
            .lock()
            .unwrap()
            .as_ref()
            .map(Arc::clone)
    }

    fn take_temporary_memory_state(
        &self,
    ) -> Option<Arc<paro_storage::buffer::TemporaryMemoryState>> {
        self.temporary_memory_state.lock().unwrap().take()
    }

    pub fn current_reservation(&self) -> usize {
        self.temporary_memory_state()
            .map(|state| state.get_reservation())
            .unwrap_or(0)
    }

    pub fn peak_reservation(&self) -> usize {
        self.temporary_memory_state()
            .map(|state| state.get_peak_reservation())
            .unwrap_or(0)
    }

    pub fn is_external(&self) -> bool {
        self.external.load(Ordering::Acquire)
    }

    pub fn any_combined(&self) -> bool {
        self.any_combined.load(Ordering::Acquire)
    }

    fn mark_external(&self) {
        self.external.store(true, Ordering::Release);
    }

    pub fn update_local_state(&self, lstate: &mut SortLocalSinkState) {
        let reservation = self.current_reservation();
        lstate.maximum_run_size = if self.num_threads > 0 {
            reservation / self.num_threads
        } else {
            reservation
        };
        lstate.external = self.is_external();
    }

    pub fn try_increase_reservation(&self, lstate: &mut SortLocalSinkState) -> bool {
        if lstate.external || self.is_external() {
            lstate.external = true;
            self.mark_external();
            return false;
        }

        let Some(temp_state) = self.temporary_memory_state() else {
            lstate.external = true;
            self.mark_external();
            return false;
        };

        if temp_state.get_reservation() < temp_state.get_remaining_size() {
            if !self.any_combined() {
                lstate.external = true;
                self.mark_external();
            }
            return false;
        }

        let mut request = temp_state.get_remaining_size().saturating_mul(2);
        if request == 0 {
            request = 1;
        }
        temp_state.set_remaining_size_and_update_reservation(request);
        let got_enough = temp_state.get_reservation() >= temp_state.get_remaining_size();
        if !got_enough && !self.any_combined() {
            lstate.external = true;
            self.mark_external();
        }
        got_enough
    }

    pub fn add_sorted_run(&self, run: SortedRun) {
        let count = run.count();
        self.sorted_runs.lock().unwrap().push(run);
        *self.sorted_tuples.lock().unwrap() += count;
    }

    pub fn sorted_runs_count(&self) -> usize {
        self.sorted_runs.lock().unwrap().len()
    }

    pub fn set_any_combined(&self) {
        self.any_combined.store(true, Ordering::Release);
    }
}

impl GlobalSinkState for SortGlobalSinkState {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn sink_state_name(&self) -> &str {
        "SortGlobalSinkState"
    }
}

#[derive(Debug)]
pub struct SortLocalSourceState {
    current_position: usize,
    pub(crate) merger_lstate: SortedRunMergerLocalState,
}

impl SortLocalSourceState {
    pub fn new() -> Self {
        Self {
            current_position: 0,
            merger_lstate: SortedRunMergerLocalState::new(),
        }
    }
}

impl LocalSourceState for SortLocalSourceState {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

#[derive(Debug)]
pub struct SortGlobalSourceState {
    pub(crate) single_run: Option<Arc<SortedRun>>,
    pub(crate) merger: Option<Arc<SortedRunMerger>>,
    pub(crate) merger_gstate: Option<Arc<SortedRunMergerGlobalState>>,
    pub(crate) total_count: usize,
    temporary_memory_state: Mutex<Option<Arc<paro_storage::buffer::TemporaryMemoryState>>>,
}

impl SortGlobalSourceState {
    pub fn new(
        single_run: Option<Arc<SortedRun>>,
        merger: Option<Arc<SortedRunMerger>>,
        merger_gstate: Option<Arc<SortedRunMergerGlobalState>>,
        total_count: usize,
        temporary_memory_state: Option<Arc<paro_storage::buffer::TemporaryMemoryState>>,
    ) -> Self {
        Self {
            single_run,
            merger,
            merger_gstate,
            total_count,
            temporary_memory_state: Mutex::new(temporary_memory_state),
        }
    }

    pub fn single_run(&self) -> Option<Arc<SortedRun>> {
        self.single_run.as_ref().map(Arc::clone)
    }

    pub fn merger(&self) -> Option<Arc<SortedRunMerger>> {
        self.merger.as_ref().map(Arc::clone)
    }

    pub fn merger_gstate(&self) -> Option<Arc<SortedRunMergerGlobalState>> {
        self.merger_gstate.as_ref().map(Arc::clone)
    }

    pub fn total_count(&self) -> usize {
        self.total_count
    }

    fn release_temporary_memory_state(&self) {
        if let Some(state) = self.temporary_memory_state.lock().unwrap().take() {
            state.set_zero();
        }
    }
}

impl Drop for SortGlobalSourceState {
    fn drop(&mut self) {
        if let Some(state) = self.temporary_memory_state.get_mut().unwrap().take() {
            state.set_zero();
        }
    }
}

impl GlobalSourceState for SortGlobalSourceState {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn max_threads(&self) -> usize {
        if self.single_run.is_some() {
            1
        } else if let Some(merger_gstate) = self.merger_gstate() {
            merger_gstate.max_threads()
        } else {
            1
        }
    }
}

fn build_key_chunk(chunk: &Chunk, orders: &[OrderByNode]) -> Chunk {
    let mut vectors = Vec::with_capacity(orders.len());
    for order in orders {
        if let Expression::ColumnRef(col_ref) = &order.expression {
            if let Some(vector) = chunk.column(col_ref.binding.column_index) {
                vectors.push(Arc::clone(vector));
            }
        } else if let Expression::Reference(reference) = &order.expression {
            if let Some(vector) = chunk.column(reference.index) {
                vectors.push(Arc::clone(vector));
            }
        }
    }
    Chunk::from_arc_vectors(vectors)
}

fn build_payload_chunk(chunk: &Chunk, projection_map: &[usize]) -> Chunk {
    let vectors = projection_map
        .iter()
        .filter_map(|&column_idx| chunk.column(column_idx).map(Arc::clone))
        .collect::<Vec<_>>();
    Chunk::from_arc_vectors(vectors)
}
