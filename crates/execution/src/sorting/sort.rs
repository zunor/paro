// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use paro_common::allocator::MemoryTag;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::memory::{
    MemoryAccountingClass, MemoryAccountingContext, MemoryDomain, MemoryOwner,
};
use paro_common::sort_key::{OrderModifiers, SortKeyEncoding};
use paro_common::types::LogicalType;
use paro_common::vector::VECTOR_SIZE;
use paro_planner::binder::ir::OrderByNode;
use paro_planner::expression::Expression;
use paro_storage::buffer::DEFAULT_BLOCK_SIZE;
use paro_storage::meta::DEFAULT_SORT_PARTITION_SIZE;
use paro_storage::row::{RowLayout, RowValidityType};

use crate::execution_context::ExecutionContext;
use crate::operator_state::{GlobalSinkState, GlobalSourceState, LocalSinkState, LocalSourceState};
use crate::result_type::{
    SinkCombineResultType, SinkFinalizeType, SinkResultType, SourceResultType,
};

use super::sort_projection_column::SortProjectionColumn;
use super::sorted_run::{RunBuilder, SortedRun};
use super::sorted_run_merger::{
    SortedRunMerger, SortedRunMergerGlobalState, SortedRunMergerLocalState,
};
use crate::memory_runtime::{OperatorExternalMemoryTracker, ReclaimStats, Reclaimer, SpillCost};

const SORT_MEMORY_TAG: MemoryTag = MemoryTag::OrderBy;
const SORT_MEMORY_CLASS: MemoryAccountingClass = MemoryAccountingClass::Revocable;
const DEFAULT_SORT_RUN_TARGET_BYTES: usize = 64 * 1024 * 1024;

fn sort_run_target_bytes(
    query_max_memory: usize,
    memory_limit: usize,
    has_temporary_directory: bool,
    num_threads: usize,
    force_external: bool,
) -> usize {
    if force_external {
        return 1;
    }
    if !has_temporary_directory {
        return usize::MAX / 4;
    }

    let query_cap = query_max_memory.min(memory_limit);
    let target = if query_cap >= usize::MAX / 8 {
        DEFAULT_SORT_RUN_TARGET_BYTES
    } else {
        query_cap / num_threads.max(1)
    };
    target.max(DEFAULT_BLOCK_SIZE)
}

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

    fn prepare_output_chunk(&self, chunk: &mut Chunk) -> Result<()> {
        let output_types = self.output_types();
        let chunk_types = chunk.types();
        let needs_reinit = chunk.column_count() != output_types.len()
            || chunk.capacity() < VECTOR_SIZE
            || chunk_types != output_types;

        if needs_reinit {
            let allocator = chunk.allocator().clone();
            *chunk = Chunk::try_initialize(&output_types, VECTOR_SIZE, allocator)?;
        } else {
            chunk.try_reset(chunk.allocator().clone())?;
        }
        Ok(())
    }

    pub fn is_index_sort(&self) -> bool {
        self.is_index_sort
    }

    pub fn get_local_sink_state(&self, ctx: &ExecutionContext) -> Result<Box<dyn LocalSinkState>> {
        Ok(Box::new(SortLocalSinkState::new(
            self,
            ctx.allocator(MemoryTag::OrderBy),
        )?))
    }

    pub fn get_global_sink_state(
        &self,
        ctx: &ExecutionContext,
    ) -> Result<Box<dyn GlobalSinkState>> {
        let external = ctx.force_external();
        let has_temporary_directory = ctx.has_temporary_directory();
        if external && !has_temporary_directory {
            return Err(paro_error::out_of_memory(
                "force_external requires a temporary directory (SET temp_directory)",
            ));
        }

        let memory_tracker = Arc::new(OperatorExternalMemoryTracker::new(
            ctx.operator_memory_account(),
            MemoryDomain::Host,
            SORT_MEMORY_TAG,
            SORT_MEMORY_CLASS,
        ));
        let gstate = SortGlobalSinkState::new(
            ctx.num_threads(),
            memory_tracker.clone(),
            external,
            has_temporary_directory,
            sort_run_target_bytes(
                ctx.query_max_memory(),
                ctx.session.limits.max_memory,
                has_temporary_directory,
                ctx.num_threads().max(1),
                external,
            ),
        );
        let reclaimer: Arc<dyn Reclaimer> = Arc::new(SortRunReclaimer::new(
            Arc::clone(&gstate.sorted_runs),
            memory_tracker,
            ctx.buffer_pool().clone(),
            gstate.external.clone(),
        ));
        ctx.query_memory_pool().register_reclaimer(reclaimer);
        Ok(Box::new(gstate))
    }

    pub fn sink(
        &self,
        ctx: &ExecutionContext,
        chunk: &Chunk,
        global_state: &dyn GlobalSinkState,
        local_state: &mut dyn LocalSinkState,
    ) -> Result<SinkResultType> {
        ctx.check_cancelled()?;
        let gstate = global_state
            .as_any()
            .downcast_ref::<SortGlobalSinkState>()
            .expect("invalid sort global sink state");
        let lstate = local_state
            .as_any_mut()
            .downcast_mut::<SortLocalSinkState>()
            .expect("invalid sort local sink state");

        if lstate.run_builder.is_none() {
            lstate.run_builder = Some(RunBuilder::new_with_memory(
                Arc::clone(ctx.buffer_pool()),
                Arc::clone(&self.key_layout),
                Arc::clone(&self.payload_layout),
                Arc::clone(&self.sort_key_encoding),
                gstate.memory_context(),
            ));
            gstate.update_local_state(lstate);
        }

        build_key_chunk_in_place(chunk, &self.orders, &mut lstate.key_chunk)?;
        build_payload_chunk_in_place(chunk, &self.input_projection_map, &mut lstate.payload_chunk)?;

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
        ctx.check_cancelled()?;
        let Some(run_builder) = lstate.run_builder.as_ref() else {
            return Ok(false);
        };
        let run_size = run_builder.size_in_bytes();
        if run_size < lstate.maximum_run_size {
            return Ok(true);
        }

        if gstate.has_temporary_directory {
            lstate.external = true;
        }
        if lstate.external {
            gstate.mark_external();
            if let Some(run_builder) = lstate.run_builder.take() {
                let run = run_builder.finish(true)?;
                gstate.add_sorted_run(run);
            }
            lstate.run_builder = Some(RunBuilder::new_with_memory(
                Arc::clone(ctx.buffer_pool()),
                Arc::clone(&self.key_layout),
                Arc::clone(&self.payload_layout),
                Arc::clone(&self.sort_key_encoding),
                gstate.memory_context(),
            ));
            gstate.update_local_state(lstate);
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
        let memory_tracker = gstate.take_memory_tracker();

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
            memory_tracker,
        )))
    }

    pub fn get_data(
        &self,
        ctx: &ExecutionContext,
        chunk: &mut Chunk,
        global_state: &dyn GlobalSourceState,
        local_state: &mut dyn LocalSourceState,
    ) -> Result<SourceResultType> {
        ctx.check_cancelled()?;
        let gstate = global_state
            .as_any()
            .downcast_ref::<SortGlobalSourceState>()
            .expect("invalid sort global source state");

        if gstate.total_count() == 0 {
            gstate.release_memory_tracker();
            return Ok(SourceResultType::Finished);
        }

        if let Some(single_run) = gstate.single_run() {
            let lstate = local_state
                .as_any_mut()
                .downcast_mut::<SortLocalSourceState>()
                .expect("invalid sort local source state");
            self.prepare_output_chunk(chunk)?;
            single_run.scan(
                chunk,
                lstate.current_position,
                self.output_projection_columns(),
            )?;
            lstate.current_position += chunk.size();
            if lstate.current_position >= gstate.total_count() {
                gstate.release_memory_tracker();
                return Ok(SourceResultType::Finished);
            }
            return Ok(SourceResultType::HaveMoreOutput);
        }

        if let (Some(merger), Some(merger_gstate)) = (gstate.merger(), gstate.merger_gstate()) {
            let lstate = local_state
                .as_any_mut()
                .downcast_mut::<SortLocalSourceState>()
                .expect("invalid sort local source state");
            self.prepare_output_chunk(chunk)?;
            let result = merger.get_data(chunk, &merger_gstate, &mut lstate.merger_lstate)?;
            if result == SourceResultType::Finished {
                gstate.release_memory_tracker();
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
    pub fn new(sort: &Sort, allocator: Arc<dyn paro_common::allocator::Allocator>) -> Result<Self> {
        Ok(Self {
            run_builder: None,
            maximum_run_size: 0,
            external: false,
            key_chunk: Chunk::try_init_empty(sort.key_layout.types(), allocator.clone())?,
            payload_chunk: Chunk::try_init_empty(sort.payload_layout.types(), allocator)?,
        })
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
    external: Arc<AtomicBool>,
    any_combined: AtomicBool,
    memory_tracker: Mutex<Option<Arc<OperatorExternalMemoryTracker>>>,
    has_temporary_directory: bool,
    num_threads: usize,
    run_target_bytes: usize,
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
        memory_tracker: Arc<OperatorExternalMemoryTracker>,
        external: bool,
        has_temporary_directory: bool,
        run_target_bytes: usize,
    ) -> Self {
        Self {
            sorted_runs: Arc::new(Mutex::new(Vec::new())),
            sorted_tuples: Mutex::new(0),
            external: Arc::new(AtomicBool::new(external)),
            any_combined: AtomicBool::new(false),
            memory_tracker: Mutex::new(Some(memory_tracker)),
            has_temporary_directory,
            num_threads,
            run_target_bytes,
            total_count: Mutex::new(0),
            partition_size: Mutex::new(DEFAULT_SORT_PARTITION_SIZE),
        }
    }

    fn memory_tracker(&self) -> Option<Arc<OperatorExternalMemoryTracker>> {
        self.memory_tracker.lock().unwrap().as_ref().map(Arc::clone)
    }

    fn take_memory_tracker(&self) -> Option<Arc<OperatorExternalMemoryTracker>> {
        self.memory_tracker.lock().unwrap().take()
    }

    pub(crate) fn memory_context(&self) -> MemoryAccountingContext {
        self.memory_tracker()
            .map(|tracker| {
                let owner: Arc<dyn MemoryOwner> = tracker;
                MemoryAccountingContext::from_owner(
                    owner,
                    MemoryDomain::Host,
                    SORT_MEMORY_TAG,
                    SORT_MEMORY_CLASS,
                )
            })
            .unwrap_or_else(|| {
                MemoryAccountingContext::detached(SORT_MEMORY_TAG, SORT_MEMORY_CLASS)
            })
    }

    pub fn current_reservation(&self) -> usize {
        self.memory_tracker()
            .and_then(|tracker| tracker.reservation_bytes().ok())
            .unwrap_or(0)
    }

    pub fn peak_reservation(&self) -> usize {
        self.memory_tracker()
            .and_then(|tracker| tracker.peak_bytes().ok())
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
        lstate.maximum_run_size = self.run_target_bytes;
        lstate.external = self.is_external();
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

#[derive(Debug)]
struct SortRunReclaimer {
    name: String,
    sorted_runs: Arc<Mutex<Vec<SortedRun>>>,
    memory_tracker: Arc<OperatorExternalMemoryTracker>,
    buffer_pool: Arc<paro_storage::buffer::BufferPool>,
    external: Arc<AtomicBool>,
}

impl SortRunReclaimer {
    fn new(
        sorted_runs: Arc<Mutex<Vec<SortedRun>>>,
        memory_tracker: Arc<OperatorExternalMemoryTracker>,
        buffer_pool: Arc<paro_storage::buffer::BufferPool>,
        external: Arc<AtomicBool>,
    ) -> Self {
        Self {
            name: "sort_runs".to_string(),
            sorted_runs,
            memory_tracker,
            buffer_pool,
            external,
        }
    }

    fn memory_context(&self) -> MemoryAccountingContext {
        let owner: Arc<dyn MemoryOwner> = self.memory_tracker.clone();
        MemoryAccountingContext::from_owner(
            owner,
            MemoryDomain::Host,
            SORT_MEMORY_TAG,
            SORT_MEMORY_CLASS,
        )
    }
}

impl Reclaimer for SortRunReclaimer {
    fn name(&self) -> &str {
        &self.name
    }

    fn reclaimable_bytes(&self) -> usize {
        let completed_run_bytes = self
            .sorted_runs
            .lock()
            .map(|runs| runs.iter().map(SortedRun::size_in_bytes).sum::<usize>())
            .unwrap_or(0);
        completed_run_bytes.min(self.memory_tracker.accounted_bytes().unwrap_or(0))
    }

    fn reclaim_sync(&self, target_bytes: usize) -> paro_common::memory::MemoryResult<ReclaimStats> {
        if target_bytes == 0 {
            return Ok(ReclaimStats::empty(0));
        }

        self.external.store(true, Ordering::Release);

        let reclaimable = self.reclaimable_bytes();
        let target_bytes = target_bytes.min(reclaimable);
        if target_bytes == 0 {
            return Ok(ReclaimStats::empty(0));
        }

        let mut externalized_bytes = 0usize;
        let memory = self.memory_context();
        let mut runs = self.sorted_runs.lock().unwrap();
        if runs.iter().any(|run| !run.is_external()) {
            let old_runs = std::mem::take(&mut *runs);
            let mut new_runs = Vec::with_capacity(old_runs.len());
            for run in old_runs {
                if externalized_bytes >= target_bytes || run.is_external() {
                    new_runs.push(run);
                    continue;
                }
                let before = run.size_in_bytes();
                let (external_run, reclaimed) = run
                    .into_external(self.buffer_pool.clone(), memory.clone())
                    .map_err(|err| {
                        paro_common::memory::MemoryError::reclaim_failed(err.to_string())
                    })?;
                externalized_bytes = externalized_bytes
                    .saturating_add(reclaimed)
                    .saturating_add(before.saturating_sub(external_run.size_in_bytes()));
                new_runs.push(external_run);
            }
            *runs = new_runs;
        }
        drop(runs);

        let released = self.memory_tracker.reclaim_accounted_bytes(target_bytes)?;
        Ok(ReclaimStats::new(
            target_bytes,
            released.max(externalized_bytes),
            released,
        ))
    }

    fn spill_cost(&self) -> SpillCost {
        SpillCost::SpillToDisk
    }
}

impl GlobalSinkState for SortGlobalSinkState {
    fn as_any(&self) -> &dyn std::any::Any {
        self
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
    memory_tracker: Mutex<Option<Arc<OperatorExternalMemoryTracker>>>,
}

impl SortGlobalSourceState {
    pub fn new(
        single_run: Option<Arc<SortedRun>>,
        merger: Option<Arc<SortedRunMerger>>,
        merger_gstate: Option<Arc<SortedRunMergerGlobalState>>,
        total_count: usize,
        memory_tracker: Option<Arc<OperatorExternalMemoryTracker>>,
    ) -> Self {
        Self {
            single_run,
            merger,
            merger_gstate,
            total_count,
            memory_tracker: Mutex::new(memory_tracker),
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

    fn release_memory_tracker(&self) {
        let _ = self.memory_tracker.lock().unwrap().take();
    }
}

impl Drop for SortGlobalSourceState {
    fn drop(&mut self) {
        let _ = self.memory_tracker.get_mut().unwrap().take();
    }
}

impl GlobalSourceState for SortGlobalSourceState {
    fn as_any(&self) -> &dyn std::any::Any {
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

pub(crate) fn build_key_chunk_into<'a>(
    chunk: &'a Chunk,
    orders: &[OrderByNode],
    slot: &'a mut Option<Chunk>,
) -> Result<&'a Chunk> {
    let output = projected_metadata_chunk(chunk, orders.len(), slot)?;
    build_key_chunk_in_place(chunk, orders, output)?;
    Ok(output)
}

pub(crate) fn build_payload_chunk_into<'a>(
    chunk: &'a Chunk,
    projection_map: &[usize],
    slot: &'a mut Option<Chunk>,
) -> Result<&'a Chunk> {
    let output = projected_metadata_chunk(chunk, projection_map.len(), slot)?;
    build_payload_chunk_in_place(chunk, projection_map, output)?;
    Ok(output)
}

pub(crate) fn build_key_chunk_in_place(
    chunk: &Chunk,
    orders: &[OrderByNode],
    output: &mut Chunk,
) -> Result<()> {
    output.data.clear();
    output.data.reserve(orders.len());
    output.set_capacity(chunk.size().max(1));
    for order in orders {
        let column_idx = match &order.expression {
            Expression::ColumnRef(col_ref) => col_ref.binding.column_index,
            Expression::Reference(reference) => reference.index,
            other => {
                return Err(paro_error::internal(format!(
                    "sort key expression was not lowered to a column reference: {other:?}"
                )));
            }
        };
        output
            .data
            .push(Arc::clone(chunk.column(column_idx).ok_or_else(|| {
                paro_error::internal(format!("sort key column out of bounds: {column_idx}"))
            })?));
    }
    output.try_set_cardinality(chunk.size())?;
    Ok(())
}

pub(crate) fn build_payload_chunk_in_place(
    chunk: &Chunk,
    projection_map: &[usize],
    output: &mut Chunk,
) -> Result<()> {
    output.data.clear();
    output.data.reserve(projection_map.len());
    output.set_capacity(chunk.size().max(1));
    for &column_idx in projection_map {
        output
            .data
            .push(Arc::clone(chunk.column(column_idx).ok_or_else(|| {
                paro_error::internal(format!("sort payload column out of bounds: {column_idx}"))
            })?));
    }
    output.try_set_cardinality(chunk.size())?;
    Ok(())
}

fn projected_metadata_chunk<'a>(
    input: &'a Chunk,
    column_count: usize,
    slot: &'a mut Option<Chunk>,
) -> Result<&'a mut Chunk> {
    if slot.is_none() {
        *slot = Some(Chunk::try_new(input.allocator().clone())?);
    }
    let output = slot
        .as_mut()
        .expect("sort projected metadata chunk was initialized above");
    output.data.clear();
    output.data.reserve(column_count);
    output.set_capacity(input.size().max(1));
    Ok(output)
}
