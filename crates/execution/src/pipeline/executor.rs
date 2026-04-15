// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Low-level pipeline execution engine.

use crate::execution_context::ExecutionContext;
use crate::operator::state::{OperatorSinkCombineInput, OperatorSinkInput, OperatorSourceInput};
use crate::operator::PhysicalOperator;
use crate::result_type::{
    OperatorResultType, SinkCombineResultType, SinkResultType, SourceResultType,
};
use crate::thread_context::ThreadContext;
use paro_common::allocator::MemoryTag;
use paro_common::chunk::Chunk;
use paro_common::vector::VECTOR_SIZE;
use paro_context::StatementContext;
use paro_scheduler::task::InterruptState;

use paro_common::error::{self as paro_error, Result};
use std::sync::Arc;

use super::pipeline::{Pipeline, PipelineGlobalStates, PipelineLocalStates};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PipelineExecuteResult {
    NotFinished,
    Blocked,
    Finished,
    Interrupted,
}

#[derive(Debug, Clone, Copy)]
pub struct ExecutionBudget {
    pub max_chunks: usize,
    pub chunks_processed: usize,
}

impl ExecutionBudget {
    pub fn new(max_chunks: usize) -> Self {
        Self {
            max_chunks,
            chunks_processed: 0,
        }
    }

    pub fn can_continue(&self) -> bool {
        self.chunks_processed < self.max_chunks
    }

    pub fn increment(&mut self) {
        self.chunks_processed += 1;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FinalizeState {
    Idle,
    Combine,
    Done,
}

pub struct PipelineExecutor {
    session: Arc<StatementContext>,
    thread: ThreadContext,
    pipeline: Arc<Pipeline>,
    gstates: Arc<PipelineGlobalStates>,
    lstates: PipelineLocalStates,
    intermediate_chunks: Vec<Chunk>,
    in_process: Vec<usize>,
    pending_input: Option<Chunk>,
    pending_start_idx: usize,
    pending_sink_chunk: Option<Chunk>,
    source_done: bool,
    flush_done: bool,
    finalize_state: FinalizeState,
    pub interrupt_state: InterruptState,
    pub budget: Option<ExecutionBudget>,
}

impl PipelineExecutor {
    pub fn new(
        session: Arc<StatementContext>,
        thread_id: usize,
        total_threads: usize,
        pipeline: Arc<Pipeline>,
    ) -> Result<Self> {
        let thread =
            ThreadContext::new_with_profiler(thread_id, total_threads, pipeline.explain_profiler());
        let ctx = ExecutionContext::new(session.clone(), &thread, Some(pipeline.as_ref()));
        let gstates = pipeline.reset(&ctx)?;
        Self::with_global_states_internal(session, thread, pipeline, gstates)
    }

    pub fn with_global_states(
        session: Arc<StatementContext>,
        thread_id: usize,
        total_threads: usize,
        pipeline: Arc<Pipeline>,
        gstates: Arc<PipelineGlobalStates>,
    ) -> Result<Self> {
        let thread =
            ThreadContext::new_with_profiler(thread_id, total_threads, pipeline.explain_profiler());
        Self::with_global_states_internal(session, thread, pipeline, gstates)
    }

    pub fn with_interrupt_state(
        session: Arc<StatementContext>,
        thread_id: usize,
        total_threads: usize,
        pipeline: Arc<Pipeline>,
        gstates: Arc<PipelineGlobalStates>,
        interrupt_state: InterruptState,
    ) -> Result<Self> {
        let thread =
            ThreadContext::new_with_profiler(thread_id, total_threads, pipeline.explain_profiler());
        Self::with_interrupt_state_internal(session, thread, pipeline, gstates, interrupt_state)
    }

    fn with_global_states_internal(
        session: Arc<StatementContext>,
        thread: ThreadContext,
        pipeline: Arc<Pipeline>,
        gstates: Arc<PipelineGlobalStates>,
    ) -> Result<Self> {
        Self::with_interrupt_state_internal(
            session,
            thread,
            pipeline,
            gstates,
            InterruptState::new(),
        )
    }

    fn with_interrupt_state_internal(
        session: Arc<StatementContext>,
        thread: ThreadContext,
        pipeline: Arc<Pipeline>,
        gstates: Arc<PipelineGlobalStates>,
        interrupt_state: InterruptState,
    ) -> Result<Self> {
        let ctx = ExecutionContext::new(session.clone(), &thread, Some(pipeline.as_ref()));
        // Ensure source state is initialized (respecting dependencies)
        pipeline.reset_source(&ctx, false)?;
        let lstates = PipelineLocalStates::new(&ctx, &pipeline, &gstates)?;

        let allocator = ctx.allocator(MemoryTag::Allocator);
        let operators = pipeline.get_operators();
        let mut intermediate_chunks = Vec::with_capacity(operators.len());
        for op in &operators {
            intermediate_chunks.push(Chunk::initialize_with_allocator(
                op.types(),
                VECTOR_SIZE,
                allocator.clone(),
            ));
        }

        Ok(Self {
            session,
            thread,
            pipeline,
            gstates,
            lstates,
            intermediate_chunks,
            in_process: Vec::new(),
            pending_input: None,
            pending_start_idx: 0,
            pending_sink_chunk: None,
            source_done: false,
            flush_done: false,
            finalize_state: FinalizeState::Idle,
            interrupt_state,
            budget: None,
        })
    }

    pub fn set_budget(&mut self, budget: ExecutionBudget) {
        self.budget = Some(budget);
    }

    pub fn execute(&mut self) -> Result<PipelineExecuteResult> {
        let sink_op = self
            .pipeline
            .get_sink()
            .ok_or_else(|| paro_error::internal("Pipeline has no sink".to_string()))?;

        if let Some(chunk) = self.pending_sink_chunk.take() {
            let sink_result = self.sink_chunk(sink_op.as_ref(), &chunk)?;
            match sink_result {
                SinkResultType::NeedMoreInput => {
                    // chunk consumed
                }
                SinkResultType::Finished => {
                    self.source_done = true;
                    return self.advance_post_processing(sink_op.as_ref());
                }
                SinkResultType::Blocked => {
                    self.pending_sink_chunk = Some(chunk);
                    return Ok(PipelineExecuteResult::Blocked);
                }
                SinkResultType::Interrupted => {
                    self.pending_sink_chunk = Some(chunk);
                    return Ok(PipelineExecuteResult::Interrupted);
                }
            }
        }

        if let Some(chunk) = self.pending_input.take() {
            let result = self.push_input_from(&chunk, self.pending_start_idx, sink_op.as_ref())?;
            match result {
                PipelineExecuteResult::NotFinished => {
                    // chunk consumed
                }
                PipelineExecuteResult::Blocked => {
                    self.pending_input = Some(chunk);
                    return Ok(PipelineExecuteResult::Blocked);
                }
                PipelineExecuteResult::Interrupted => {
                    self.pending_input = Some(chunk);
                    return Ok(PipelineExecuteResult::Interrupted);
                }
                PipelineExecuteResult::Finished => {
                    self.source_done = true;
                    return self.advance_post_processing(sink_op.as_ref());
                }
            }
        }

        if self.source_done {
            return self.advance_post_processing(sink_op.as_ref());
        }

        let source = self
            .pipeline
            .source()
            .ok_or_else(|| paro_error::internal("Pipeline has no source".to_string()))?;

        loop {
            let allocator = self.session.buffer_allocator();
            let mut source_chunk =
                Chunk::initialize_with_allocator(source.types(), VECTOR_SIZE, allocator);

            let source_result = {
                self.start_profile(source.as_ref());
                let ctx = ExecutionContext::new(
                    self.session.clone(),
                    &self.thread,
                    Some(self.pipeline.as_ref()),
                );
                let mut source_guard = self.gstates.source.lock();
                let gstate = source_guard
                    .as_mut()
                    .ok_or_else(|| paro_error::internal("Source state missing"))?;
                let mut source_input = OperatorSourceInput::new(
                    gstate.as_ref(),
                    self.lstates.source.as_mut(),
                    &self.interrupt_state,
                );
                let result = source.get_data(&ctx, &mut source_chunk, &mut source_input)?;
                self.end_profile(source.as_ref(), source_chunk.size());
                result
            };

            match source_result {
                SourceResultType::HaveMoreOutput => {}
                SourceResultType::Finished => {
                    if source_chunk.size() == 0 {
                        self.source_done = true;
                        return self.advance_post_processing(sink_op.as_ref());
                    }
                }
                SourceResultType::Blocked => {
                    return Ok(PipelineExecuteResult::Blocked);
                }
            }

            if source_chunk.size() == 0 {
                continue;
            }

            if let Some(budget) = self.budget.as_mut() {
                budget.increment();
                if !budget.can_continue() {
                    self.pending_input = Some(source_chunk);
                    self.pending_start_idx = 0;
                    return Ok(PipelineExecuteResult::Interrupted);
                }
            }

            self.in_process.clear();
            match self.push_input_from(&source_chunk, 0, sink_op.as_ref())? {
                PipelineExecuteResult::NotFinished => {}
                PipelineExecuteResult::Blocked => {
                    self.pending_input = Some(source_chunk);
                    self.pending_start_idx = 0;
                    return Ok(PipelineExecuteResult::Blocked);
                }
                PipelineExecuteResult::Interrupted => {
                    self.pending_input = Some(source_chunk);
                    self.pending_start_idx = 0;
                    return Ok(PipelineExecuteResult::Interrupted);
                }
                PipelineExecuteResult::Finished => {
                    self.source_done = true;
                    return self.advance_post_processing(sink_op.as_ref());
                }
            }
        }
    }

    fn advance_post_processing(
        &mut self,
        sink: &dyn PhysicalOperator,
    ) -> Result<PipelineExecuteResult> {
        if self.pending_sink_chunk.is_some() || self.pending_input.is_some() {
            return Ok(PipelineExecuteResult::Blocked);
        }

        if !self.flush_done {
            match self.flush_final_execute(sink)? {
                PipelineExecuteResult::Blocked => return Ok(PipelineExecuteResult::Blocked),
                PipelineExecuteResult::Interrupted => {
                    return Ok(PipelineExecuteResult::Interrupted)
                }
                _ => {
                    self.flush_done = true;
                }
            }
        }

        if self.finalize_state == FinalizeState::Idle {
            self.finalize_state = FinalizeState::Combine;
        }
        if self.finalize_state != FinalizeState::Done {
            match self.advance_finalize(sink)? {
                PipelineExecuteResult::Blocked => return Ok(PipelineExecuteResult::Blocked),
                PipelineExecuteResult::Interrupted => {
                    return Ok(PipelineExecuteResult::Interrupted)
                }
                _ => {}
            }
        }

        self.thread.flush_profiler();
        Ok(PipelineExecuteResult::Finished)
    }

    fn push_input_from(
        &mut self,
        input: &Chunk,
        start_idx: usize,
        sink: &dyn PhysicalOperator,
    ) -> Result<PipelineExecuteResult> {
        let operators = self.pipeline.get_operators();
        if start_idx >= operators.len() {
            let sink_result = self.sink_chunk(sink, input)?;
            return Ok(match sink_result {
                SinkResultType::NeedMoreInput => PipelineExecuteResult::NotFinished,
                SinkResultType::Finished => PipelineExecuteResult::Finished,
                SinkResultType::Blocked => PipelineExecuteResult::Blocked,
                SinkResultType::Interrupted => PipelineExecuteResult::Interrupted,
            });
        }

        loop {
            let out_idx = self.produce_next_output_from(input, start_idx)?;
            let Some(idx) = out_idx else {
                return Ok(PipelineExecuteResult::NotFinished);
            };

            let out_chunk = self.intermediate_chunks[idx].clone();
            let sink_result = self.sink_chunk(sink, &out_chunk)?;
            match sink_result {
                SinkResultType::NeedMoreInput => {}
                SinkResultType::Finished => return Ok(PipelineExecuteResult::Finished),
                SinkResultType::Blocked => return Ok(PipelineExecuteResult::Blocked),
                SinkResultType::Interrupted => return Ok(PipelineExecuteResult::Interrupted),
            }
            if self.in_process.is_empty() {
                return Ok(PipelineExecuteResult::NotFinished);
            }
        }
    }

    fn produce_next_output_from(
        &mut self,
        input: &Chunk,
        start_idx: usize,
    ) -> Result<Option<usize>> {
        let operators = self.pipeline.get_operators();
        let op_count = operators.len();
        if op_count == 0 {
            return Err(paro_error::internal("No operators"));
        }

        let mut current_idx = self.in_process.pop().unwrap_or(start_idx);

        loop {
            let op = operators[current_idx].clone();
            let node_id = op.explain_node_id();
            let op_result = {
                if let Some(node_id) = node_id {
                    self.thread.start_operator(node_id);
                }
                let ctx = ExecutionContext::new(
                    self.session.clone(),
                    &self.thread,
                    Some(self.pipeline.as_ref()),
                );
                let mut ops_guard = self.gstates.operators.lock();
                let ops_gstates = ops_guard
                    .as_mut()
                    .ok_or_else(|| paro_error::internal("Ops states missing"))?;
                let gstate = ops_gstates[current_idx].as_ref();
                let lstate = self.lstates.operators[current_idx].as_mut();

                // Use indices to avoid double borrow of self
                let intermediate_chunks = &mut self.intermediate_chunks;
                let (prev_chunk, out_chunk) = if current_idx == start_idx {
                    (input, &mut intermediate_chunks[current_idx])
                } else {
                    let (prev_part, out_part) = intermediate_chunks.split_at_mut(current_idx);
                    (&prev_part[current_idx - 1], &mut out_part[0])
                };
                out_chunk.reset();

                let result = op.execute(&ctx, prev_chunk, out_chunk, gstate, lstate)?;
                if let Some(node_id) = node_id {
                    self.thread.end_operator(node_id, out_chunk.size() as u64);
                }
                result
            };

            match op_result {
                OperatorResultType::HaveMoreOutput => {
                    self.in_process.push(current_idx);
                }
                OperatorResultType::NeedMoreInput => {}
                OperatorResultType::Finished => {
                    return Ok(None);
                }
                OperatorResultType::Blocked => {
                    self.in_process.push(current_idx);
                    return Ok(None);
                }
            }

            if self.intermediate_chunks[current_idx].size() == 0 {
                current_idx = match self.in_process.pop() {
                    Some(next_idx) => next_idx,
                    None => return Ok(None),
                };
                continue;
            }

            current_idx += 1;
            if current_idx >= op_count {
                return Ok(Some(op_count - 1));
            }
        }
    }

    fn flush_final_execute(
        &mut self,
        sink: &dyn PhysicalOperator,
    ) -> Result<PipelineExecuteResult> {
        let operators = self.pipeline.get_operators();
        let op_count = operators.len();
        if op_count == 0 {
            return Ok(PipelineExecuteResult::Finished);
        }

        for (idx, op) in operators.iter().enumerate() {
            if !op.requires_final_execute() {
                continue;
            }

            loop {
                let allocator = self.session.buffer_allocator();
                let mut flush_chunk =
                    Chunk::initialize_with_allocator(op.types(), VECTOR_SIZE, allocator);

                let finalize_result = {
                    self.start_profile(op.as_ref());
                    let ctx = ExecutionContext::new(
                        self.session.clone(),
                        &self.thread,
                        Some(self.pipeline.as_ref()),
                    );
                    let mut ops_guard = self.gstates.operators.lock();
                    let ops_gstates = ops_guard
                        .as_mut()
                        .ok_or_else(|| paro_error::internal("Ops states missing"))?;
                    let gstate = ops_gstates[idx].as_ref();

                    let result = op.final_execute(
                        &ctx,
                        &mut flush_chunk,
                        gstate,
                        self.lstates.operators[idx].as_mut(),
                    )?;
                    self.end_profile(op.as_ref(), flush_chunk.size());
                    result
                };

                if flush_chunk.size() > 0 {
                    match self.push_input_from(&flush_chunk, idx + 1, sink)? {
                        PipelineExecuteResult::NotFinished => {}
                        res => return Ok(res),
                    }
                }

                match finalize_result {
                    crate::result_type::OperatorFinalizeResultType::HaveMoreOutput => continue,
                    crate::result_type::OperatorFinalizeResultType::Finished => break,
                }
            }
        }

        Ok(PipelineExecuteResult::Finished)
    }

    fn sink_chunk(&mut self, sink: &dyn PhysicalOperator, chunk: &Chunk) -> Result<SinkResultType> {
        let node_id = sink.explain_node_id();
        if let Some(node_id) = node_id {
            self.thread.start_operator(node_id);
        }
        let sink_guard = self.gstates.sink.lock();
        let gstate = sink_guard
            .as_ref()
            .ok_or_else(|| paro_error::internal("Sink state missing"))?;
        let lstate = self
            .lstates
            .sink
            .as_mut()
            .ok_or_else(|| paro_error::internal("Local sink state missing"))?;

        let mut sink_input =
            OperatorSinkInput::new(gstate.as_ref(), lstate.as_mut(), &self.interrupt_state);
        let ctx = ExecutionContext::new(
            self.session.clone(),
            &self.thread,
            Some(self.pipeline.as_ref()),
        );
        let sink_result = sink.sink(&ctx, chunk, &mut sink_input)?;
        if let Some(node_id) = node_id {
            self.thread.end_operator(node_id, chunk.size() as u64);
        }

        Ok(sink_result)
    }

    fn advance_finalize(&mut self, sink: &dyn PhysicalOperator) -> Result<PipelineExecuteResult> {
        {
            let sink_guard = self.gstates.sink.lock();
            if sink_guard.is_none() {
                self.finalize_state = FinalizeState::Done;
                return Ok(PipelineExecuteResult::Finished);
            }
        }
        let lstate = self
            .lstates
            .sink
            .as_mut()
            .ok_or_else(|| paro_error::internal("Local sink state missing"))?;

        loop {
            match self.finalize_state {
                FinalizeState::Idle => self.finalize_state = FinalizeState::Combine,
                FinalizeState::Combine => {
                    let sink_guard = self.gstates.sink.lock();
                    let gstate = sink_guard
                        .as_ref()
                        .ok_or_else(|| paro_error::internal("Sink state missing"))?;
                    let mut combine_input = OperatorSinkCombineInput::new(
                        gstate.as_ref(),
                        lstate.as_mut(),
                        &self.interrupt_state,
                    );
                    let ctx = ExecutionContext::new(
                        self.session.clone(),
                        &self.thread,
                        Some(self.pipeline.as_ref()),
                    );
                    match sink.combine(&ctx, &mut combine_input)? {
                        SinkCombineResultType::Finished => {
                            self.finalize_state = FinalizeState::Done;
                        }
                        SinkCombineResultType::Blocked => {
                            return Ok(PipelineExecuteResult::Blocked)
                        }
                        SinkCombineResultType::Interrupted => {
                            return Ok(PipelineExecuteResult::Interrupted)
                        }
                    }
                }
                FinalizeState::Done => return Ok(PipelineExecuteResult::Finished),
            }
        }
    }

    fn start_profile(&self, operator: &dyn PhysicalOperator) {
        if let Some(node_id) = operator.explain_node_id() {
            self.thread.start_operator(node_id);
        }
    }

    fn end_profile(&self, operator: &dyn PhysicalOperator, output_rows: usize) {
        if let Some(node_id) = operator.explain_node_id() {
            self.thread.end_operator(node_id, output_rows as u64);
        }
    }
}

impl Drop for PipelineExecutor {
    fn drop(&mut self) {
        self.thread.flush_profiler();
    }
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    use paro_common::chunk::Chunk;
    use paro_common::error::Result;
    use paro_common::types::LogicalType;
    use paro_context::{test_support::TestStatementContextBuilder, StatementContext};

    use super::{PipelineExecuteResult, PipelineExecutor};
    use crate::execution_context::ExecutionContext;
    use crate::explain::profiler::ExplainProfiler;
    use crate::operator::state::OperatorSourceInput;
    use crate::operator::PhysicalOperator;
    use crate::operator_type::PhysicalOperatorType;
    use crate::pipeline::pipeline::Pipeline;
    use crate::result_type::{SinkResultType, SourceResultType};

    fn test_session() -> Arc<StatementContext> {
        TestStatementContextBuilder::minimal().build()
    }

    #[derive(Debug)]
    struct ProfilingSource {
        emitted: AtomicBool,
        profiler: Arc<ExplainProfiler>,
        types: Vec<LogicalType>,
    }

    impl ProfilingSource {
        fn new(profiler: Arc<ExplainProfiler>) -> Self {
            Self {
                emitted: AtomicBool::new(false),
                profiler,
                types: vec![LogicalType::Integer],
            }
        }
    }

    impl PhysicalOperator for ProfilingSource {
        fn operator_type(&self) -> PhysicalOperatorType {
            PhysicalOperatorType::RowsetScan
        }

        fn types(&self) -> &[LogicalType] {
            &self.types
        }

        fn is_source(&self) -> bool {
            true
        }

        fn explain_node_id(&self) -> Option<u64> {
            Some(1)
        }

        fn explain_profiler(&self) -> Option<Arc<ExplainProfiler>> {
            Some(self.profiler.clone())
        }

        fn get_data(
            &self,
            _ctx: &ExecutionContext,
            chunk: &mut Chunk,
            _input: &mut OperatorSourceInput,
        ) -> Result<SourceResultType> {
            if self.emitted.swap(true, Ordering::SeqCst) {
                chunk.set_cardinality(0);
                return Ok(SourceResultType::Finished);
            }

            let output = chunk.column_mut(0).expect("profiling source output column");
            output.set_i32(0, 42);
            chunk.set_cardinality(1);
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
    struct PassthroughSink {
        types: Vec<LogicalType>,
    }

    impl PassthroughSink {
        fn new() -> Self {
            Self { types: Vec::new() }
        }
    }

    impl PhysicalOperator for PassthroughSink {
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
            _chunk: &Chunk,
            _input: &mut crate::operator::state::OperatorSinkInput,
        ) -> Result<SinkResultType> {
            Ok(SinkResultType::NeedMoreInput)
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn as_any_mut(&mut self) -> &mut dyn Any {
            self
        }
    }

    #[test]
    fn flushes_explain_profiler_before_executor_returns_finished() {
        let session = test_session();
        let profiler = ExplainProfiler::new();
        let source = Arc::new(ProfilingSource::new(profiler.clone())) as Arc<dyn PhysicalOperator>;
        let sink = Arc::new(PassthroughSink::new()) as Arc<dyn PhysicalOperator>;
        let pipeline = Arc::new(Pipeline::new());
        pipeline.set_source(source);
        pipeline.set_sink(sink);

        let mut executor =
            PipelineExecutor::new(session, 0, 1, pipeline).expect("create pipeline executor");

        loop {
            match executor.execute().expect("execute pipeline") {
                PipelineExecuteResult::Finished => break,
                PipelineExecuteResult::Blocked => panic!("test pipeline should not block"),
                PipelineExecuteResult::Interrupted => {
                    panic!("test pipeline should not be interrupted")
                }
                PipelineExecuteResult::NotFinished => {}
            }
        }

        let stats = profiler
            .node_stats(1)
            .expect("profiler stats should be flushed before drop");
        assert_eq!(stats.output_rows, 1);
        assert_eq!(stats.loops, 2);
    }
}
