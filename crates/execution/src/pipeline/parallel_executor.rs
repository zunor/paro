//! Parallel pipeline execution helpers built on the task scheduler.

use crate::execution_context::ExecutionContext;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use super::executor::PipelineExecuteResult;
use crate::pipeline::pipeline::Pipeline;
use paro_common::error::Result;

/// Configuration for parallel pipeline execution.
#[derive(Debug, Clone)]
pub struct ParallelConfig {
    /// Maximum number of worker threads to use.
    /// If 0, uses the default number from TaskScheduler.
    pub max_threads: usize,
    /// Whether to execute remaining work on the main thread after scheduling.
    pub execute_on_main_thread: bool,
}

impl Default for ParallelConfig {
    fn default() -> Self {
        Self {
            max_threads: 0,
            execute_on_main_thread: true,
        }
    }
}

/// Executes a pipeline using the TaskScheduler for parallel execution.
///
/// This executor creates multiple PipelineTask instances for parallel processing
/// and submits them to the TaskScheduler.
pub struct ParallelPipelineExecutor {
    /// Configuration for parallel execution
    config: ParallelConfig,
    /// Whether execution was interrupted
    interrupted: Arc<AtomicBool>,
}

impl ParallelPipelineExecutor {
    /// Create a new parallel pipeline executor with default config.
    pub fn new() -> Self {
        Self {
            config: ParallelConfig::default(),
            interrupted: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Create a new parallel pipeline executor with specified config.
    pub fn with_config(config: ParallelConfig) -> Self {
        Self {
            config,
            interrupted: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Get the interrupt flag reference for external interruption.
    pub fn interrupted(&self) -> &Arc<AtomicBool> {
        &self.interrupted
    }

    /// Interrupt the execution.
    pub fn interrupt(&self) {
        self.interrupted.store(true, Ordering::SeqCst);
    }

    /// Check if execution was interrupted.
    pub fn is_interrupted(&self) -> bool {
        self.interrupted.load(Ordering::SeqCst)
    }

    /// Execute a pipeline using the TaskScheduler.
    ///
    /// This method:
    /// 1. Initializes pipeline global states (including source state)
    /// 3. Executes tasks using the scheduler
    pub fn execute<'a>(
        &self,
        ctx: &'a ExecutionContext<'a>,
        pipeline: Arc<Pipeline>,
    ) -> Result<PipelineExecuteResult> {
        let scheduler = ctx.session.scheduler();
        let scheduler_threads = scheduler.number_of_threads().max(0) as usize;

        // Initialize global states first so source max_threads is available.
        // Without this, compute_max_threads() often falls back to 1.
        let _gstates = pipeline.reset(ctx)?;
        pipeline.reset_source(ctx, false)?;

        // Get number of threads to use
        let mut num_threads = if self.config.max_threads > 0 {
            self.config.max_threads
        } else {
            scheduler_threads.max(1)
        };

        // Limit by pipeline's max parallelism
        let pipeline_max = pipeline.compute_max_threads().max(1);
        if num_threads > pipeline_max {
            num_threads = pipeline_max;
        }
        // If main thread is not allowed to execute tasks, we need worker threads.
        if !self.config.execute_on_main_thread && scheduler_threads == 0 {
            return self.execute_single_threaded(ctx, pipeline);
        }

        if num_threads <= 1 {
            // Single-threaded execution
            return self.execute_single_threaded(ctx, pipeline);
        }

        // Multi-threaded implementation
        // 1. Wrap in an event for completion tracking
        let event = paro_scheduler::event::Event::new();

        // 2. Create PipelineTasks
        let mut tasks: Vec<Arc<parking_lot::Mutex<dyn paro_scheduler::task::Task>>> =
            Vec::with_capacity(num_threads);
        for i in 0..num_threads {
            tasks.push(Arc::new(parking_lot::Mutex::new(
                crate::pipeline::task::PipelineTask::new(pipeline.clone(), event.clone(), i),
            )));
        }

        // 3. Schedule tasks to scheduler
        event.schedule_tasks_to_scheduler(tasks, scheduler);

        // 4. Wait for completion. Main thread can optionally help execute tasks.
        let task_marker = AtomicBool::new(true);
        while !event.is_finished() {
            if self.is_interrupted() {
                // Cancellation is not wired to the scheduler interrupt path yet.
                break;
            }

            let completed = if self.config.execute_on_main_thread {
                // Execute at most one task on the main thread
                scheduler.execute_tasks(&task_marker, 1)
            } else {
                0
            };
            if completed == 0 && !event.is_finished() {
                // If no tasks available to execute on main thread, wait or yield
                if !scheduler.wait_for_task() {
                    // Timeout, just yield a bit
                    std::thread::yield_now();
                }
            }
        }

        if event.is_finished() {
            Ok(PipelineExecuteResult::Finished)
        } else {
            Ok(PipelineExecuteResult::Blocked)
        }
    }

    /// Execute pipeline on a single thread.
    fn execute_single_threaded<'a>(
        &self,
        ctx: &'a ExecutionContext<'a>,
        pipeline: Arc<Pipeline>,
    ) -> Result<PipelineExecuteResult> {
        use super::executor::PipelineExecutor;

        // Create PipelineExecutor with session Arc from ExecutionContext
        let mut executor = PipelineExecutor::new(
            ctx.session.clone(), // Clone the Arc
            0,                   // thread_id: single-threaded, use 0
            1,                   // total_threads: single-threaded
            pipeline,
        )?;

        loop {
            if self.is_interrupted() {
                return Ok(PipelineExecuteResult::Blocked);
            }

            let result = executor.execute()?;

            match result {
                PipelineExecuteResult::Finished => return Ok(PipelineExecuteResult::Finished),
                PipelineExecuteResult::Blocked => {
                    // In single-threaded mode, blocked means we can't proceed
                    return Ok(PipelineExecuteResult::Blocked);
                }
                PipelineExecuteResult::Interrupted | PipelineExecuteResult::NotFinished => {
                    // Continue execution
                }
            }
        }
    }

    /// Execute tasks using the scheduler and wait for completion.
    ///
    /// This is a helper for environments where the main thread should
    /// participate in work while waiting.
    pub fn execute_with_scheduler(&self, ctx: &ExecutionContext, max_tasks: usize) -> usize {
        let scheduler = ctx.session.scheduler();
        scheduler.execute_tasks(&self.interrupted, max_tasks)
    }
}

impl Default for ParallelPipelineExecutor {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use paro_common::chunk::Chunk;
    use paro_common::error::{self as paro_error, Result};
    use paro_common::types::LogicalType;

    use crate::operator::state::{
        GlobalSourceState, LocalSourceState, OperatorSinkInput, OperatorSourceInput,
    };

    use crate::operator::PhysicalOperator;
    use crate::operator_type::PhysicalOperatorType;
    use crate::result_type::{SinkResultType, SourceResultType};
    use crate::thread_context::ThreadContext;

    use super::*;

    fn test_session() -> Arc<paro_context::StatementContext> {
        paro_context::test_support::TestStatementContextBuilder::minimal().build()
    }

    #[derive(Debug)]
    struct TestSource {
        types: Vec<LogicalType>,
        total_rows: usize,
        chunk_size: usize,
        max_threads: usize,
        local_state_inits: Arc<AtomicUsize>,
    }

    #[derive(Debug)]
    struct TestSourceGlobalState {
        total_rows: usize,
        next_row: AtomicUsize,
        max_threads: usize,
    }

    impl GlobalSourceState for TestSourceGlobalState {
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

    #[derive(Debug, Default)]
    struct TestSourceLocalState;

    impl LocalSourceState for TestSourceLocalState {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn as_any_mut(&mut self) -> &mut dyn Any {
            self
        }
    }

    impl PhysicalOperator for TestSource {
        fn operator_type(&self) -> PhysicalOperatorType {
            PhysicalOperatorType::RowsetScan
        }

        fn types(&self) -> &[LogicalType] {
            &self.types
        }

        fn is_source(&self) -> bool {
            true
        }

        fn parallel_source(&self) -> bool {
            true
        }

        fn estimated_cardinality(&self) -> usize {
            self.total_rows
        }

        fn get_global_source_state(
            &self,
            _ctx: &ExecutionContext,
            _sink_state: Option<&dyn crate::operator::state::GlobalSinkState>,
        ) -> Result<Box<dyn GlobalSourceState>> {
            Ok(Box::new(TestSourceGlobalState {
                total_rows: self.total_rows,
                next_row: AtomicUsize::new(0),
                max_threads: self.max_threads,
            }))
        }

        fn get_local_source_state(
            &self,
            _ctx: &ExecutionContext,
            _gstate: &dyn GlobalSourceState,
        ) -> Result<Box<dyn LocalSourceState>> {
            self.local_state_inits.fetch_add(1, Ordering::SeqCst);
            Ok(Box::new(TestSourceLocalState))
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
                .downcast_ref::<TestSourceGlobalState>()
                .ok_or_else(|| paro_error::internal("invalid source global state"))?;

            let start = gstate.next_row.fetch_add(self.chunk_size, Ordering::SeqCst);
            if start >= gstate.total_rows {
                return Ok(SourceResultType::Finished);
            }

            let count = (gstate.total_rows - start).min(self.chunk_size);
            chunk.reset();
            let col = chunk
                .column_mut(0)
                .ok_or_else(|| paro_error::internal("missing output column"))?;
            for i in 0..count {
                col.set_i32(i, (start + i) as i32);
            }
            chunk.set_cardinality(count);
            Ok(SourceResultType::HaveMoreOutput)
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn as_any_mut(&mut self) -> &mut dyn Any {
            self
        }
    }

    #[derive(Debug)]
    struct CountingSink {
        types: Vec<LogicalType>,
        rows_seen: Arc<AtomicUsize>,
    }

    impl PhysicalOperator for CountingSink {
        fn operator_type(&self) -> PhysicalOperatorType {
            PhysicalOperatorType::ResultCollector
        }

        fn types(&self) -> &[LogicalType] {
            &self.types
        }

        fn is_sink(&self) -> bool {
            true
        }

        fn sink(
            &self,
            _ctx: &ExecutionContext,
            chunk: &Chunk,
            _input: &mut OperatorSinkInput,
        ) -> Result<SinkResultType> {
            self.rows_seen.fetch_add(chunk.size(), Ordering::SeqCst);
            Ok(SinkResultType::NeedMoreInput)
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn as_any_mut(&mut self) -> &mut dyn Any {
            self
        }
    }

    fn run_parallel_executor(config: ParallelConfig) -> (PipelineExecuteResult, usize, usize) {
        let local_state_inits = Arc::new(AtomicUsize::new(0));
        let rows_seen = Arc::new(AtomicUsize::new(0));
        let total_rows = 8192;

        let source: Arc<dyn PhysicalOperator> = Arc::new(TestSource {
            types: vec![LogicalType::Integer],
            total_rows,
            chunk_size: 64,
            max_threads: 4,
            local_state_inits: local_state_inits.clone(),
        });
        let sink: Arc<dyn PhysicalOperator> = Arc::new(CountingSink {
            types: vec![LogicalType::Integer],
            rows_seen: rows_seen.clone(),
        });

        let pipeline = Arc::new(Pipeline::new());
        pipeline.set_source(source);
        pipeline.set_sink(sink);

        let session = test_session();
        let thread = ThreadContext::single_threaded();
        let ctx = ExecutionContext::new(session, &thread, Some(pipeline.as_ref()));

        let executor = ParallelPipelineExecutor::with_config(config);
        let result = executor
            .execute(&ctx, pipeline.clone())
            .expect("parallel execute failed");

        (
            result,
            local_state_inits.load(Ordering::SeqCst),
            rows_seen.load(Ordering::SeqCst),
        )
    }

    #[test]
    fn execute_uses_multiple_tasks_when_source_allows_parallelism() {
        let config = ParallelConfig {
            max_threads: 4,
            execute_on_main_thread: true,
        };
        let (result, local_states, rows_seen) = run_parallel_executor(config);
        assert_eq!(result, PipelineExecuteResult::Finished);
        assert!(
            local_states > 1,
            "expected parallel local states, got {local_states}"
        );
        assert_eq!(rows_seen, 8192);
    }

    #[test]
    fn execute_runs_when_main_thread_disabled() {
        let config = ParallelConfig {
            max_threads: 4,
            execute_on_main_thread: false,
        };
        let (result, local_states, rows_seen) = run_parallel_executor(config);
        assert_eq!(result, PipelineExecuteResult::Finished);
        assert!(local_states >= 1, "expected at least one local state");
        assert_eq!(rows_seen, 8192);
    }
}
