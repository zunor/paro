//! Pipeline graph node: source, optional intermediate operators, and optional sink.

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};

use parking_lot::Mutex;

use crate::execution_context::ExecutionContext;
use crate::explain::profiler::ExplainProfiler;
use crate::operator::state::{
    GlobalOperatorState, GlobalSinkState, GlobalSourceState, LocalSinkState, LocalSourceState,
    OperatorState,
};
use crate::operator::{OperatorPartitionInfo, PhysicalOperator};
use crate::thread_context::ThreadContext;
use paro_scheduler::event::Event;
use paro_scheduler::scheduler::TaskScheduler;
use paro_scheduler::task::ProducerToken;
use paro_scheduler::task::Task;

/// PipelineGlobalStates represents the global execution state of a pipeline.
/// It is shared across all worker threads executing the same pipeline.
pub struct PipelineGlobalStates {
    /// Shared state for the source operator
    pub source: Mutex<Option<Arc<dyn GlobalSourceState>>>,
    /// Shared states for intermediate operators
    pub operators: Mutex<Option<Vec<Arc<dyn GlobalOperatorState>>>>,
    /// Shared state for the sink operator (if any)
    pub sink: Mutex<Option<Arc<dyn GlobalSinkState>>>,
    pub client: Arc<paro_context::StatementContext>,
}

impl std::fmt::Debug for PipelineGlobalStates {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PipelineGlobalStates")
            .field("source", &self.source)
            .field("operators", &self.operators)
            .field("sink", &self.sink)
            .finish()
    }
}

impl PipelineGlobalStates {
    /// Create new empty global states for a pipeline.
    pub fn empty(client: Arc<paro_context::StatementContext>) -> Self {
        Self {
            source: Mutex::new(None),
            operators: Mutex::new(None),
            sink: Mutex::new(None),
            client,
        }
    }
}

#[derive(Debug)]
pub struct PipelineLocalStates {
    /// Local state for the source operator
    pub source: Box<dyn LocalSourceState>,
    /// Local states for intermediate operators
    pub operators: Vec<Box<dyn OperatorState>>,
    /// Local state for the sink operator (if any)
    pub sink: Option<Box<dyn LocalSinkState>>,
}

impl PipelineLocalStates {
    pub fn new(
        ctx: &ExecutionContext,
        pipeline: &Pipeline,
        gstates: &PipelineGlobalStates,
    ) -> paro_common::error::Result<Self> {
        let source_gstate_guard = gstates.source.lock();
        let source_gstate = source_gstate_guard.as_ref().ok_or_else(|| {
            paro_common::error::internal("Source global state not initialized".to_string())
        })?;
        let source = pipeline
            .source()
            .unwrap()
            .get_local_source_state(ctx, source_gstate.as_ref())?;

        let ops_gstates_guard = gstates.operators.lock();
        let _ops_gstates = ops_gstates_guard.as_ref().ok_or_else(|| {
            paro_common::error::internal("Operator global states not initialized".to_string())
        })?;

        let operators_list = pipeline.get_operators();
        let mut operators = Vec::with_capacity(operators_list.len());
        for op in &operators_list {
            operators.push(op.get_operator_state(ctx)?);
        }

        let sink = if let Some(sink) = pipeline.get_sink() {
            Some(sink.get_local_sink_state(ctx)?)
        } else {
            None
        };

        Ok(Self {
            source,
            operators,
            sink,
        })
    }
}

/// Pipeline represents a single pipeline of execution.
/// A pipeline consists of a source operator, zero or more intermediate operators,
/// and an optional sink operator.
pub struct Pipeline {
    /// The source operator for this pipeline
    source: Mutex<Option<Arc<dyn PhysicalOperator>>>,
    /// Intermediate operators in the pipeline
    operators: Mutex<Vec<Arc<dyn PhysicalOperator>>>,
    /// The sink operator for this pipeline
    sink: Mutex<Option<Arc<dyn PhysicalOperator>>>,

    /// Parent pipelines that depend on this one
    parents: Mutex<Vec<Weak<Pipeline>>>,
    /// Dependency pipelines that this one depends on
    dependencies: Mutex<Vec<Weak<Pipeline>>>,

    /// Whether the pipeline is ready to be scheduled
    ready: AtomicBool,
    /// Whether the pipeline has been initialized
    initialized: AtomicBool,

    /// Global states for the pipeline
    global_states: Mutex<Option<Arc<PipelineGlobalStates>>>,

    /// Batch index for ordering
    batch_index: AtomicUsize,
    /// Active batch indexes used to track the minimum in-flight batch.
    active_batch_indexes: Mutex<BTreeSet<usize>>,
}

impl Pipeline {
    /// Create a new pipeline.
    pub fn new() -> Self {
        Self {
            source: Mutex::new(None),
            operators: Mutex::new(Vec::new()),
            sink: Mutex::new(None),
            parents: Mutex::new(Vec::new()),
            dependencies: Mutex::new(Vec::new()),
            ready: AtomicBool::new(false),
            initialized: AtomicBool::new(false),
            global_states: Mutex::new(None),
            batch_index: AtomicUsize::new(0),
            active_batch_indexes: Mutex::new(BTreeSet::new()),
        }
    }

    /// Create a new pipeline with a sink and batch index.
    pub fn new_with_sink(sink: Option<Arc<dyn PhysicalOperator>>, batch_index: usize) -> Self {
        let p = Self::new();
        if let Some(s) = sink {
            p.set_sink(s);
        }
        p.batch_index.store(batch_index, Ordering::SeqCst);
        p
    }

    /// Set the source operator for the pipeline.
    pub fn set_source(&self, source: Arc<dyn PhysicalOperator>) {
        assert!(source.is_source(), "Operator must be a source");
        *self.source.lock() = Some(source);
    }

    /// Add an intermediate operator to the pipeline.
    pub fn add_operator(&self, operator: Arc<dyn PhysicalOperator>) {
        self.operators.lock().push(operator);
    }

    /// Set all operators for the pipeline.
    pub fn set_operators(&self, operators: Vec<Arc<dyn PhysicalOperator>>) {
        *self.operators.lock() = operators;
    }

    /// Set the sink operator for the pipeline.
    pub fn set_sink(&self, sink: Arc<dyn PhysicalOperator>) {
        assert!(sink.is_sink(), "Operator must be a sink");
        *self.sink.lock() = Some(sink);
    }

    /// Clear the sink operator.
    pub fn clear_sink(&self) {
        *self.sink.lock() = None;
    }

    /// Get the source operator.
    pub fn source(&self) -> Option<Arc<dyn PhysicalOperator>> {
        self.source.lock().clone()
    }

    /// Get the intermediate operators.
    pub fn get_operators(&self) -> Vec<Arc<dyn PhysicalOperator>> {
        self.operators.lock().clone()
    }

    /// Get the sink operator.
    pub fn get_sink(&self) -> Option<Arc<dyn PhysicalOperator>> {
        self.sink.lock().clone()
    }

    pub fn explain_profiler(&self) -> Option<Arc<ExplainProfiler>> {
        if let Some(source) = self.source() {
            if let Some(profiler) = source.explain_profiler() {
                return Some(profiler);
            }
        }
        for operator in self.get_operators() {
            if let Some(profiler) = operator.explain_profiler() {
                return Some(profiler);
            }
        }
        self.get_sink().and_then(|sink| sink.explain_profiler())
    }

    /// Get the sink operator as Arc.
    pub fn sink_arc(&self) -> Arc<dyn PhysicalOperator> {
        self.sink.lock().clone().expect("Pipeline has no sink")
    }

    /// Check if the pipeline has a source.
    pub fn has_source(&self) -> bool {
        self.source.lock().is_some()
    }

    /// Get the number of operators.
    pub fn operator_count(&self) -> usize {
        self.operators.lock().len()
    }

    /// Set the batch index.
    pub fn set_batch_index(&self, index: usize) {
        self.batch_index.store(index, Ordering::SeqCst);
    }

    /// Get the batch index.
    pub fn batch_index(&self) -> usize {
        self.batch_index.load(Ordering::SeqCst)
    }

    /// Whether this pipeline requires input order preservation.
    ///
    pub fn is_order_dependent(&self) -> bool {
        if let Some(source) = self.source() {
            match source.source_order() {
                crate::operator::OrderPreservationType::FixedOrder => return true,
                crate::operator::OrderPreservationType::NoOrder => return false,
                crate::operator::OrderPreservationType::InsertionOrder => {}
            }
        }

        for op in self.get_operators() {
            match op.operator_order() {
                crate::operator::OrderPreservationType::NoOrder => return false,
                crate::operator::OrderPreservationType::FixedOrder => return true,
                crate::operator::OrderPreservationType::InsertionOrder => {}
            }
        }

        if let Some(sink) = self.get_sink() {
            if sink.sink_order_dependent() {
                return true;
            }
        }

        false
    }

    /// Register a new in-flight batch index.
    ///
    /// Returns the minimum currently active batch index after registration.
    pub fn register_new_batch_index(&self) -> usize {
        let mut active = self.active_batch_indexes.lock();
        let minimum = active
            .iter()
            .next()
            .copied()
            .unwrap_or_else(|| self.batch_index());
        active.insert(minimum);
        minimum
    }

    /// Update an in-flight batch index to a newer value.
    ///
    /// Returns the minimum active batch index after update.
    pub fn update_batch_index(
        &self,
        old_index: usize,
        new_index: usize,
    ) -> paro_common::error::Result<usize> {
        let mut active = self.active_batch_indexes.lock();
        let current_min = active.iter().next().copied().ok_or_else(|| {
            paro_common::error::internal(
                "No active batch index registered before update".to_string(),
            )
        })?;

        if new_index < current_min {
            return Err(paro_common::error::internal(format!(
                "Processing batch index {}, but previous min batch index was {}",
                new_index, current_min
            )));
        }

        if !active.remove(&old_index) {
            return Err(paro_common::error::internal(format!(
                "Batch index {} was not found in active batch indexes",
                old_index
            )));
        }

        active.insert(new_index);
        Ok(active.iter().next().copied().unwrap_or(new_index))
    }

    /// Schedule the pipeline for execution.
    ///
    /// This method determines whether to use parallel or sequential execution
    pub fn schedule(
        self: &Arc<Self>,
        event: &Arc<Event>,
        scheduler: &Arc<TaskScheduler>,
        producer: Option<&ProducerToken>,
    ) -> paro_common::error::Result<()> {
        if !self.ready.load(Ordering::SeqCst) {
            return Err(paro_common::error::internal(
                "Pipeline is not ready".to_string(),
            ));
        }

        // state right before scheduling. This avoids sharing a base initialize
        // event with non-base pipelines that only hold empty per-pipeline state.
        let gstates = self.get_global_states().ok_or_else(|| {
            paro_common::error::internal(
                "Pipeline global states must be initialized before scheduling".to_string(),
            )
        })?;
        let thread = ThreadContext::single_threaded();
        let exec_ctx = ExecutionContext::new(gstates.client.clone(), &thread, Some(self.as_ref()));
        let _ = self.reset(&exec_ctx)?;

        // Try to schedule parallel execution
        let is_parallel = self.schedule_parallel(event, scheduler, producer)?;

        if !is_parallel {
            // Could not parallelize: push a sequential task instead
            self.schedule_sequential(event, scheduler, producer);
        }

        Ok(())
    }

    /// Try to schedule parallel execution.
    ///
    /// Returns true if parallel scheduling succeeded, false if sequential is needed.
    fn schedule_parallel(
        self: &Arc<Self>,
        event: &Arc<Event>,
        scheduler: &Arc<TaskScheduler>,
        producer: Option<&ProducerToken>,
    ) -> paro_common::error::Result<bool> {
        // Check source supports parallelism
        let source = match self.source() {
            Some(s) => s,
            None => return Ok(false),
        };

        if !source.parallel_source() {
            return Ok(false);
        }

        // Check sink supports parallelism
        if let Some(sink) = self.get_sink() {
            if !sink.parallel_sink() {
                return Ok(false);
            }

            let partition_info = sink.required_partition_info();
            if partition_info.requires_batch_index()
                && !source.supports_partitioning(&OperatorPartitionInfo::batch_index())
            {
                return Err(paro_common::error::internal(
                    "Attempting to schedule a pipeline where the sink requires batch index but source does not support it".to_string(),
                ));
            }
        }

        // Check all intermediate operators support parallelism
        for op in self.get_operators() {
            if !op.parallel_operator() {
                return Ok(false);
            }
        }

        // Get max threads from source state
        let mut max_threads = self.compute_max_threads();

        // Get active threads from scheduler
        let active_threads = scheduler.number_of_threads().max(1) as usize;
        if max_threads > active_threads {
            max_threads = active_threads;
        }

        // Only launch parallel tasks if we have more than 1 thread
        if max_threads <= 1 {
            return Ok(false);
        }

        self.launch_scan_tasks(event, max_threads, scheduler, producer);
        Ok(true)
    }

    fn schedule_sequential(
        self: &Arc<Self>,
        event: &Arc<Event>,
        scheduler: &Arc<TaskScheduler>,
        producer: Option<&ProducerToken>,
    ) {
        self.launch_scan_tasks(event, 1, scheduler, producer);
    }

    /// Launch scan tasks for parallel execution.
    ///
    fn launch_scan_tasks(
        self: &Arc<Self>,
        event: &Arc<Event>,
        max_threads: usize,
        scheduler: &Arc<TaskScheduler>,
        producer: Option<&ProducerToken>,
    ) {
        let num_tasks = max_threads.max(1);

        let mut tasks: Vec<Arc<Mutex<dyn Task>>> = Vec::with_capacity(num_tasks);
        for i in 0..num_tasks {
            let task = super::task::PipelineTask::new(self.clone(), event.clone(), i);
            tasks.push(Arc::new(Mutex::new(task)));
        }

        // Schedule to TaskScheduler
        if let Some(producer) = producer {
            event.schedule_tasks_to_scheduler_with_producer(tasks, scheduler, producer);
        } else {
            event.schedule_tasks_to_scheduler(tasks, scheduler);
        }
    }

    /// Mark the pipeline as ready for execution.
    pub fn set_ready(&self) {
        self.ready.store(true, Ordering::SeqCst);
    }

    /// Check if the pipeline is ready.
    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::SeqCst)
    }

    /// Check if the pipeline has been initialized.
    pub fn is_initialized(&self) -> bool {
        self.initialized.load(Ordering::SeqCst)
    }

    /// Get the global states for the pipeline.
    pub fn get_global_states(&self) -> Option<Arc<PipelineGlobalStates>> {
        self.global_states.lock().clone()
    }

    /// Reset the pipeline, initializing all global states.
    pub fn reset(
        &self,
        ctx: &ExecutionContext,
    ) -> paro_common::error::Result<Arc<PipelineGlobalStates>> {
        self.reset_sink(ctx)?;
        self.reset_operators(ctx)?;
        self.reset_source(ctx, false)?;

        let states = self.get_global_states().expect("States should exist");
        self.initialized.store(true, Ordering::SeqCst);
        Ok(states)
    }

    /// Initialize the pipeline states if not already done.
    pub fn initialize(&self, ctx: &ExecutionContext) -> paro_common::error::Result<()> {
        let mut guard = self.global_states.lock();
        if guard.is_none() {
            *guard = Some(Arc::new(PipelineGlobalStates::empty(ctx.session.clone())));
        }
        Ok(())
    }

    /// Initialize the sink state.
    pub fn reset_sink(
        &self,
        ctx: &ExecutionContext,
    ) -> paro_common::error::Result<Option<Arc<dyn GlobalSinkState>>> {
        self.initialize(ctx)?;
        let gstates = self.get_global_states().unwrap();

        if let Some(sink) = self.get_sink() {
            let mut sink_guard = gstates.sink.lock();
            if sink_guard.is_none() {
                if let Some(existing) = sink.sink_state() {
                    *sink_guard = Some(existing.clone());
                    return Ok(Some(existing));
                }

                let state_box = sink.get_global_sink_state(ctx)?;
                let state: Arc<dyn GlobalSinkState> = Arc::from(state_box);
                *sink_guard = Some(state.clone());

                // Store the shared sink state in the operator so sibling pipelines that
                // feed the same sink can reuse it instead of allocating independent state.
                sink.set_sink_state(state.clone());

                return Ok(Some(state));
            }
            return Ok(sink_guard.clone());
        }
        Ok(None)
    }

    /// Initialize intermediate operator states.
    pub fn reset_operators(&self, ctx: &ExecutionContext) -> paro_common::error::Result<()> {
        self.initialize(ctx)?;
        let gstates = self.get_global_states().unwrap();

        let mut ops_guard = gstates.operators.lock();
        if ops_guard.is_none() {
            let operators_list = self.get_operators();
            let mut operators = Vec::with_capacity(operators_list.len());
            for op in &operators_list {
                operators.push(Arc::from(op.get_global_operator_state()?));
            }
            *ops_guard = Some(operators);
        }
        Ok(())
    }

    /// Initialize the source state.
    pub fn reset_source(
        &self,
        ctx: &ExecutionContext,
        force: bool,
    ) -> paro_common::error::Result<()> {
        self.initialize(ctx)?;
        let gstates = self.get_global_states().unwrap();

        let mut source_guard = gstates.source.lock();
        if force || source_guard.is_none() {
            let source_op = self
                .source()
                .ok_or_else(|| paro_common::error::internal("No source"))?;

            let mut sink_state_from_dep: Option<Arc<dyn GlobalSinkState>> = None;
            for dep in self.get_dependencies() {
                if let Some(dep_sink_op) = dep.get_sink() {
                    if Arc::ptr_eq(&dep_sink_op, &source_op) {
                        if let Some(dep_states) = dep.get_global_states() {
                            let dep_sink_guard = dep_states.sink.lock();
                            if let Some(dep_sink_state) = dep_sink_guard.clone() {
                                sink_state_from_dep = Some(dep_sink_state);
                                break;
                            }
                        }
                    }
                }
            }

            let s = {
                let sink_guard = gstates.sink.lock();

                // Decide which sink state to provide to the source
                let sink_state_to_use = if let Some(ref dep_sink) = sink_state_from_dep {
                    Some(dep_sink.as_ref())
                } else {
                    // Only use our OWN sink state if our SINK operator is the SAME as our SOURCE operator
                    let mut use_own = false;
                    if let Some(own_sink) = self.get_sink() {
                        if Arc::ptr_eq(&own_sink, &source_op) {
                            use_own = true;
                        }
                    }

                    if use_own {
                        sink_guard.as_ref().map(|s| s.as_ref())
                    } else {
                        // Let the operator handle the lack of sink state (e.g. by using its internal storage)
                        None
                    }
                };

                Arc::from(source_op.get_global_source_state(ctx, sink_state_to_use)?)
            };
            *source_guard = Some(s);
        }

        Ok(())
    }

    /// Compute the maximum number of threads for this pipeline.
    pub fn compute_max_threads(&self) -> usize {
        let mut max_threads = usize::MAX;
        let Some(gstates) = self.get_global_states() else {
            return 1;
        };

        if let Some(source_state) = gstates.source.lock().as_ref() {
            max_threads = std::cmp::min(max_threads, source_state.max_threads());
        }

        if let Some(operator_states) = gstates.operators.lock().as_ref() {
            for operator_state in operator_states {
                let op_max_threads = operator_state.max_threads(max_threads);
                max_threads = std::cmp::min(max_threads, op_max_threads.max(1));
            }
        }

        if let Some(sink_state) = gstates.sink.lock().as_ref() {
            max_threads = std::cmp::min(max_threads, sink_state.max_threads(max_threads));
        }

        if max_threads == usize::MAX {
            return 1;
        }
        max_threads.max(1)
    }

    /// Add a dependency to this pipeline.
    pub fn add_dependency(self: &Arc<Self>, dependency: Arc<Pipeline>) {
        dependency.parents.lock().push(Arc::downgrade(self));
        self.dependencies.lock().push(Arc::downgrade(&dependency));
    }

    /// Clear the source of the pipeline.
    pub fn clear_source(&self) {
        self.active_batch_indexes.lock().clear();
        let guard = self.global_states.lock();
        if let Some(states) = guard.as_ref() {
            let mut source_guard = states.source.lock();
            *source_guard = None;
        }
    }

    /// Clear all runtime states so this pipeline can be scheduled again.
    ///
    /// This is required by recursive CTE execution, where the same pipeline
    /// graph is repeatedly re-scheduled for each recursion step.
    pub fn clear_runtime_states(&self) {
        self.active_batch_indexes.lock().clear();
        self.initialized.store(false, Ordering::SeqCst);

        if let Some(sink) = self.get_sink() {
            sink.clear_sink_state();
        }

        let guard = self.global_states.lock();
        if let Some(states) = guard.as_ref() {
            *states.source.lock() = None;
            *states.operators.lock() = None;
            *states.sink.lock() = None;
        }
    }

    /// Get active dependencies.
    pub fn get_dependencies(&self) -> Vec<Arc<Pipeline>> {
        self.dependencies
            .lock()
            .iter()
            .filter_map(|w| w.upgrade())
            .collect()
    }

    /// Get active parents.
    pub fn get_parents(&self) -> Vec<Arc<Pipeline>> {
        self.parents
            .lock()
            .iter()
            .filter_map(|w| w.upgrade())
            .collect()
    }

    /// Get progress information for this pipeline.
    ///
    ///
    /// Returns progress data from the source operator, normalized by estimated cardinality.
    /// If the pipeline is not initialized, returns 0% progress.
    ///
    /// # Returns
    /// - `Some(ProgressData)` if progress is available
    /// - `None` if progress cannot be determined
    pub fn get_progress(&self) -> Option<crate::operator::state::ProgressData> {
        use crate::operator::state::ProgressData;

        let source = self.source()?;

        let source_cardinality = source.estimated_cardinality().min(1 << 48).max(1);

        if !self.is_initialized() {
            return Some(ProgressData::new(0.0, 0));
        }

        // Get progress from source
        let gstates = self.get_global_states()?;
        let source_guard = gstates.source.lock();
        let source_state = source_guard.as_ref()?;

        let mut progress = source.get_progress(source_state.as_ref());
        drop(source_guard);

        // Normalize progress by estimated cardinality
        if progress.is_valid() && source_cardinality > 0 {
            // Clamp percentage to [0, 1]
            progress.percentage = progress.percentage.clamp(0.0, 1.0);
        }

        if let Some(sink) = self.get_sink() {
            let sink_guard = gstates.sink.lock();
            if let Some(sink_state) = sink_guard.as_ref() {
                progress = sink.get_sink_progress(sink_state.as_ref(), progress);
            }
        }

        Some(progress)
    }
}

impl std::fmt::Display for Pipeline {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(source) = self.source() {
            write!(f, "Pipeline({})", source.name())?;
        } else {
            write!(f, "Pipeline(<no source>)")?;
        }
        for op in self.get_operators() {
            write!(f, " -> {}", op.name())?;
        }
        if let Some(sink) = self.get_sink() {
            write!(f, " -> {}", sink.name())?;
        }
        Ok(())
    }
}

impl std::fmt::Debug for Pipeline {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Pipeline")
            .field("source", &self.source())
            .field("operators", &self.get_operators())
            .field("sink", &self.get_sink())
            .field("ready", &self.ready)
            .field("initialized", &self.initialized)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::Pipeline;
    use std::any::Any;
    use std::sync::Arc;

    use paro_common::types::LogicalType;
    use paro_context::{test_support::TestStatementContextBuilder, StatementContext};

    use crate::execution_context::ExecutionContext;
    use crate::operator::state::{
        GlobalOperatorState, GlobalSinkState, GlobalSourceState, ProgressData,
    };
    use crate::operator::{OperatorPartitionInfo, OrderPreservationType, PhysicalOperator};
    use crate::operator_type::PhysicalOperatorType;
    use crate::thread_context::ThreadContext;

    fn test_session() -> Arc<StatementContext> {
        TestStatementContextBuilder::minimal().build()
    }

    #[derive(Debug)]
    struct MockSourceOperator {
        types: Vec<LogicalType>,
        source_order: OrderPreservationType,
        progress: ProgressData,
        supports_batch_index: bool,
    }

    impl PhysicalOperator for MockSourceOperator {
        fn operator_type(&self) -> PhysicalOperatorType {
            PhysicalOperatorType::RowsetScan
        }

        fn types(&self) -> &[LogicalType] {
            &self.types
        }

        fn is_source(&self) -> bool {
            true
        }

        fn source_order(&self) -> OrderPreservationType {
            self.source_order
        }

        fn get_progress(&self, _gstate: &dyn GlobalSourceState) -> ProgressData {
            self.progress
        }

        fn supports_partitioning(&self, partition_info: &OperatorPartitionInfo) -> bool {
            if partition_info.requires_batch_index() {
                return self.supports_batch_index;
            }
            true
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn as_any_mut(&mut self) -> &mut dyn Any {
            self
        }
    }

    #[derive(Debug)]
    struct MockIntermediateOperator {
        types: Vec<LogicalType>,
        order: OrderPreservationType,
    }

    impl PhysicalOperator for MockIntermediateOperator {
        fn operator_type(&self) -> PhysicalOperatorType {
            PhysicalOperatorType::Projection
        }

        fn types(&self) -> &[LogicalType] {
            &self.types
        }

        fn operator_order(&self) -> OrderPreservationType {
            self.order
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn as_any_mut(&mut self) -> &mut dyn Any {
            self
        }
    }

    #[derive(Debug)]
    struct MockSinkOperator {
        types: Vec<LogicalType>,
        order_dependent: bool,
        aggregated_percentage: f64,
        require_batch_index: bool,
    }

    impl PhysicalOperator for MockSinkOperator {
        fn operator_type(&self) -> PhysicalOperatorType {
            PhysicalOperatorType::ResultCollector
        }

        fn types(&self) -> &[LogicalType] {
            &self.types
        }

        fn is_sink(&self) -> bool {
            true
        }

        fn sink_order_dependent(&self) -> bool {
            self.order_dependent
        }

        fn get_sink_progress(
            &self,
            _gstate: &dyn GlobalSinkState,
            source_progress: ProgressData,
        ) -> ProgressData {
            ProgressData::new(self.aggregated_percentage, source_progress.rows_scanned)
        }

        fn required_partition_info(&self) -> OperatorPartitionInfo {
            if self.require_batch_index {
                OperatorPartitionInfo::batch_index()
            } else {
                OperatorPartitionInfo::no_partition_info()
            }
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn as_any_mut(&mut self) -> &mut dyn Any {
            self
        }
    }

    #[derive(Debug)]
    struct MockSourceState {
        max_threads: usize,
    }

    impl GlobalSourceState for MockSourceState {
        fn max_threads(&self) -> usize {
            self.max_threads
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn as_any_mut(&mut self) -> &mut dyn Any {
            self
        }
    }

    #[derive(Debug)]
    struct MockOperatorState {
        max_threads: usize,
    }

    impl GlobalOperatorState for MockOperatorState {
        fn max_threads(&self, source_max_threads: usize) -> usize {
            std::cmp::min(self.max_threads, source_max_threads)
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn as_any_mut(&mut self) -> &mut dyn Any {
            self
        }
    }

    #[derive(Debug)]
    struct MockSinkState {
        max_threads: usize,
    }

    impl GlobalSinkState for MockSinkState {
        fn max_threads(&self, source_max_threads: usize) -> usize {
            std::cmp::min(self.max_threads, source_max_threads)
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn as_any_mut(&mut self) -> &mut dyn Any {
            self
        }

        fn sink_state_name(&self) -> &str {
            "MockSinkState"
        }
    }

    fn source_with_order(order: OrderPreservationType) -> Arc<dyn PhysicalOperator> {
        Arc::new(MockSourceOperator {
            types: vec![LogicalType::Integer],
            source_order: order,
            progress: ProgressData::new(0.25, 10),
            supports_batch_index: true,
        })
    }

    #[test]
    fn pipeline_order_dependency_respects_source_operator_and_sink() {
        let pipeline = Pipeline::new();
        pipeline.set_source(source_with_order(OrderPreservationType::FixedOrder));
        assert!(pipeline.is_order_dependent());

        let pipeline = Pipeline::new();
        pipeline.set_source(source_with_order(OrderPreservationType::NoOrder));
        pipeline.add_operator(Arc::new(MockIntermediateOperator {
            types: vec![LogicalType::Integer],
            order: OrderPreservationType::FixedOrder,
        }));
        assert!(!pipeline.is_order_dependent());

        let pipeline = Pipeline::new();
        pipeline.set_source(source_with_order(OrderPreservationType::InsertionOrder));
        pipeline.add_operator(Arc::new(MockIntermediateOperator {
            types: vec![LogicalType::Integer],
            order: OrderPreservationType::FixedOrder,
        }));
        assert!(pipeline.is_order_dependent());

        let pipeline = Pipeline::new();
        pipeline.set_source(source_with_order(OrderPreservationType::InsertionOrder));
        pipeline.add_operator(Arc::new(MockIntermediateOperator {
            types: vec![LogicalType::Integer],
            order: OrderPreservationType::NoOrder,
        }));
        assert!(!pipeline.is_order_dependent());

        let pipeline = Pipeline::new();
        pipeline.set_source(source_with_order(OrderPreservationType::InsertionOrder));
        pipeline.set_sink(Arc::new(MockSinkOperator {
            types: vec![LogicalType::Integer],
            order_dependent: true,
            aggregated_percentage: 0.8,
            require_batch_index: false,
        }));
        assert!(pipeline.is_order_dependent());
    }

    #[test]
    fn batch_index_update_tracks_minimum_and_validates_state() {
        let pipeline = Pipeline::new();
        pipeline.set_batch_index(10);

        let initial = pipeline.register_new_batch_index();
        assert_eq!(initial, 10);

        let minimum = pipeline
            .update_batch_index(initial, 12)
            .expect("batch update should succeed");
        assert_eq!(minimum, 12);

        assert!(pipeline.update_batch_index(99, 100).is_err());
        assert!(pipeline.update_batch_index(12, 11).is_err());

        pipeline.clear_source();
        assert_eq!(pipeline.register_new_batch_index(), 10);
    }

    #[test]
    fn schedule_fails_when_sink_requires_batch_index_but_source_cannot_provide_it() {
        let session = test_session();
        let scheduler = session.scheduler().clone();
        let event = paro_scheduler::event::Event::new();
        let thread_ctx = ThreadContext::single_threaded();

        let pipeline = Arc::new(Pipeline::new());
        pipeline.set_source(Arc::new(MockSourceOperator {
            types: vec![LogicalType::Integer],
            source_order: OrderPreservationType::InsertionOrder,
            progress: ProgressData::new(0.0, 0),
            supports_batch_index: false,
        }));
        pipeline.set_sink(Arc::new(MockSinkOperator {
            types: vec![LogicalType::Integer],
            order_dependent: false,
            aggregated_percentage: 0.0,
            require_batch_index: true,
        }));
        pipeline.set_ready();
        let exec_ctx = ExecutionContext::new(session, &thread_ctx, Some(pipeline.as_ref()));
        pipeline
            .initialize(&exec_ctx)
            .expect("pipeline initialize should succeed");

        let result = pipeline.schedule(&event, &scheduler, None);
        assert!(result.is_err());
    }

    #[test]
    fn schedule_populates_per_pipeline_global_states_after_empty_initialize() {
        let session = test_session();
        let scheduler = session.scheduler().clone();
        let event = paro_scheduler::event::Event::new();
        let thread_ctx = ThreadContext::single_threaded();

        let pipeline = Arc::new(Pipeline::new());
        pipeline.set_source(source_with_order(OrderPreservationType::InsertionOrder));
        pipeline.add_operator(Arc::new(MockIntermediateOperator {
            types: vec![LogicalType::Integer],
            order: OrderPreservationType::InsertionOrder,
        }));
        pipeline.set_sink(Arc::new(MockSinkOperator {
            types: vec![LogicalType::Integer],
            order_dependent: false,
            aggregated_percentage: 0.0,
            require_batch_index: false,
        }));
        pipeline.set_ready();

        let exec_ctx = ExecutionContext::new(session, &thread_ctx, Some(pipeline.as_ref()));
        pipeline
            .initialize(&exec_ctx)
            .expect("pipeline initialize should create empty state holders");

        let gstates = pipeline
            .get_global_states()
            .expect("pipeline global states should exist after initialize");
        assert!(gstates.source.lock().is_none());
        assert!(gstates.operators.lock().is_none());
        assert!(gstates.sink.lock().is_none());

        pipeline
            .schedule(&event, &scheduler, None)
            .expect("schedule should populate source/operator/sink states");

        let gstates = pipeline
            .get_global_states()
            .expect("pipeline global states should still exist");
        assert!(gstates.source.lock().is_some());
        assert!(gstates.operators.lock().is_some());
        assert!(gstates.sink.lock().is_some());
    }

    #[test]
    fn get_progress_aggregates_sink_progress() {
        let session = test_session();
        let thread_ctx = ThreadContext::single_threaded();

        let pipeline = Pipeline::new();
        pipeline.set_source(Arc::new(MockSourceOperator {
            types: vec![LogicalType::Integer],
            source_order: OrderPreservationType::InsertionOrder,
            progress: ProgressData::new(0.4, 40),
            supports_batch_index: true,
        }));
        pipeline.set_sink(Arc::new(MockSinkOperator {
            types: vec![LogicalType::Integer],
            order_dependent: false,
            aggregated_percentage: 0.9,
            require_batch_index: false,
        }));

        let exec_ctx = ExecutionContext::new(session, &thread_ctx, Some(&pipeline));
        pipeline
            .reset(&exec_ctx)
            .expect("pipeline reset should initialize states");

        let progress = pipeline
            .get_progress()
            .expect("progress should be available");
        assert!((progress.percentage - 0.9).abs() < f64::EPSILON);
        assert_eq!(progress.rows_scanned, 40);
    }

    #[test]
    fn compute_max_threads_applies_intermediate_operator_limits() {
        let session = test_session();
        let thread_ctx = ThreadContext::single_threaded();
        let pipeline = Pipeline::new();
        let exec_ctx = ExecutionContext::new(session, &thread_ctx, Some(&pipeline));

        pipeline
            .initialize(&exec_ctx)
            .expect("pipeline initialize should succeed");
        let gstates = pipeline
            .get_global_states()
            .expect("pipeline global states should exist");

        *gstates.source.lock() = Some(Arc::new(MockSourceState { max_threads: 8 }));
        *gstates.operators.lock() = Some(vec![
            Arc::new(MockOperatorState { max_threads: 6 }),
            Arc::new(MockOperatorState { max_threads: 3 }),
        ]);
        *gstates.sink.lock() = Some(Arc::new(MockSinkState { max_threads: 5 }));

        assert_eq!(pipeline.compute_max_threads(), 3);
    }
}
