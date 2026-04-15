//! # PipelineFinishEvent
//!
//! Event for finalizing pipeline execution.

use crate::operator::state::{OperatorFinalizeInput, OperatorSinkFinalizeInput};
use crate::result_type::{OperatorFinalResultType, SinkFinalizeType};
use parking_lot::Mutex;
use paro_scheduler::event::Event;
use paro_scheduler::task::Task;
use paro_scheduler::task::TaskExecutionMode;
use paro_scheduler::task::TaskExecutionResult;
use std::sync::Arc;

use super::event_base::BasePipelineEvent;
use crate::pipeline::pipeline::{Pipeline, PipelineGlobalStates};
use paro_common::error::Result;

/// PipelineFinishEvent finalizes the pipeline after execution.
pub struct PipelineFinishEvent {
    /// Base event with pipeline reference
    base: BasePipelineEvent,
    /// Global states from the execution phase
    global_states: Arc<PipelineGlobalStates>,
}

impl PipelineFinishEvent {
    /// Create a new PipelineFinishEvent.
    pub fn new(pipeline: Arc<Pipeline>, global_states: Arc<PipelineGlobalStates>) -> Arc<Self> {
        Arc::new(Self {
            base: BasePipelineEvent::new(pipeline),
            global_states,
        })
    }

    /// Get the underlying event for dependency management.
    pub fn event(&self) -> &Arc<Event> {
        self.base.event()
    }

    /// Get the pipeline.
    pub fn pipeline(&self) -> &Arc<Pipeline> {
        self.base.pipeline()
    }

    /// Get the global states.
    pub fn global_states(&self) -> &Arc<PipelineGlobalStates> {
        &self.global_states
    }

    /// Create the finalization task for external scheduling.
    pub fn create_task(self: &Arc<Self>) -> Arc<Mutex<dyn Task>> {
        let task = PipelineFinishTask::new(self.clone());
        self.base.set_tasks(1);
        Arc::new(Mutex::new(task))
    }

    /// Check if the event is finished.
    pub fn is_finished(&self) -> bool {
        self.base.is_finished()
    }

    /// Add a dependency on another event.
    pub fn add_dependency(&self, dependency: &Arc<Event>) {
        self.base.add_dependency(dependency);
    }

    /// Set the number of tasks for this event (for testing).
    #[cfg(test)]
    pub fn set_tasks(&self, count: usize) {
        self.base.set_tasks(count);
    }
}

impl std::fmt::Debug for PipelineFinishEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PipelineFinishEvent")
            .field("is_finished", &self.is_finished())
            .finish()
    }
}

/// Task that finalizes the pipeline.
pub struct PipelineFinishTask {
    /// Reference to the parent event
    event: Arc<PipelineFinishEvent>,
    /// Current operator index being finalized
    operator_idx: usize,
    /// Whether we've started sink finalization
    sink_finalize_started: bool,
    /// Whether the task has completed
    finished: bool,
}

impl PipelineFinishTask {
    /// Create a new PipelineFinishTask.
    pub fn new(event: Arc<PipelineFinishEvent>) -> Self {
        Self {
            event,
            operator_idx: 0,
            sink_finalize_started: false,
            finished: false,
        }
    }

    /// Check if the task has finished.
    pub fn is_finished(&self) -> bool {
        self.finished
    }
}

impl Task for PipelineFinishTask {
    fn execute(&mut self, _mode: TaskExecutionMode) -> Result<TaskExecutionResult> {
        if self.finished {
            return Ok(TaskExecutionResult::Finished);
        }

        let pipeline = self.event.pipeline();
        let global_states = self.event.global_states();
        let interrupt_state = paro_scheduler::task::InterruptState::new();

        let operators = pipeline.get_operators();
        while self.operator_idx < operators.len() {
            let op = &operators[self.operator_idx];
            if !op.requires_operator_finalize() {
                self.operator_idx += 1;
                continue;
            }

            let finalize_result = {
                let ops_guard = global_states.operators.lock();
                let ops_states = ops_guard.as_ref().ok_or_else(|| {
                    paro_common::error::internal(
                        "Operator global states missing during finalize".to_string(),
                    )
                })?;
                let op_gstate = ops_states.get(self.operator_idx).ok_or_else(|| {
                    paro_common::error::internal(format!(
                        "Operator global state missing at index {}",
                        self.operator_idx
                    ))
                })?;
                let finalize_input =
                    OperatorFinalizeInput::new(op_gstate.as_ref(), &interrupt_state);
                op.operator_finalize(&finalize_input)?
            };

            match finalize_result {
                OperatorFinalResultType::Finished => {
                    self.operator_idx += 1;
                }
                OperatorFinalResultType::Blocked => {
                    return Ok(TaskExecutionResult::Blocked);
                }
            }
        }

        if !self.sink_finalize_started {
            self.sink_finalize_started = true;
        }

        if let Some(sink) = pipeline.get_sink() {
            let sink_guard = global_states.sink.lock();
            if let Some(sink_state) = sink_guard.as_ref() {
                let finalize_input =
                    OperatorSinkFinalizeInput::new(sink_state.as_ref(), &interrupt_state);
                let result = sink.finalize(&finalize_input)?;

                match result {
                    SinkFinalizeType::Ready | SinkFinalizeType::NoOutputPossible => {
                        // Finalization complete
                    }
                    SinkFinalizeType::Blocked => {
                        return Ok(TaskExecutionResult::Blocked);
                    }
                }
            }
        }

        self.finished = true;
        Ok(TaskExecutionResult::Finished)
    }

    fn task_type(&self) -> &str {
        "PipelineFinishTask"
    }
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use paro_common::types::LogicalType;
    use paro_context::{test_support::TestStatementContextBuilder, StatementContext};
    use paro_scheduler::task::Task;
    use paro_scheduler::task::TaskExecutionMode;
    use paro_scheduler::task::TaskExecutionResult;

    use crate::operator::state::{EmptyGlobalOperatorState, OperatorFinalizeInput};

    use crate::operator::PhysicalOperator;
    use crate::operator_type::PhysicalOperatorType;
    use crate::pipeline::pipeline::{Pipeline, PipelineGlobalStates};
    use crate::result_type::OperatorFinalResultType;

    use super::{PipelineFinishEvent, PipelineFinishTask};

    fn test_session() -> Arc<StatementContext> {
        TestStatementContextBuilder::minimal().build()
    }

    #[derive(Debug)]
    struct BlockingOnceFinalizeOperator {
        types: Vec<LogicalType>,
        finalize_calls: AtomicUsize,
    }

    impl BlockingOnceFinalizeOperator {
        fn new() -> Self {
            Self {
                types: vec![],
                finalize_calls: AtomicUsize::new(0),
            }
        }
    }

    impl PhysicalOperator for BlockingOnceFinalizeOperator {
        fn operator_type(&self) -> PhysicalOperatorType {
            PhysicalOperatorType::Projection
        }

        fn types(&self) -> &[LogicalType] {
            &self.types
        }

        fn requires_operator_finalize(&self) -> bool {
            true
        }

        fn operator_finalize(
            &self,
            _input: &OperatorFinalizeInput,
        ) -> paro_common::error::Result<OperatorFinalResultType> {
            let current = self.finalize_calls.fetch_add(1, Ordering::SeqCst);
            if current == 0 {
                Ok(OperatorFinalResultType::Blocked)
            } else {
                Ok(OperatorFinalResultType::Finished)
            }
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn as_any_mut(&mut self) -> &mut dyn Any {
            self
        }
    }

    #[test]
    fn finish_task_reschedules_when_operator_finalize_blocks() {
        let session: Arc<StatementContext> = test_session();
        let global_states = Arc::new(PipelineGlobalStates::empty(session));
        *global_states.operators.lock() = Some(vec![Arc::new(EmptyGlobalOperatorState)
            as Arc<dyn crate::operator::state::GlobalOperatorState>]);

        let pipeline = Arc::new(Pipeline::new());
        let op = Arc::new(BlockingOnceFinalizeOperator::new());
        pipeline.add_operator(op.clone() as Arc<dyn PhysicalOperator>);

        let event = PipelineFinishEvent::new(pipeline, global_states);
        let mut task = PipelineFinishTask::new(event);

        let first = Task::execute(&mut task, TaskExecutionMode::ProcessAll).unwrap();
        assert_eq!(first, TaskExecutionResult::Blocked);
        assert!(!task.is_finished());
        assert_eq!(op.finalize_calls.load(Ordering::SeqCst), 1);

        let second = Task::execute(&mut task, TaskExecutionMode::ProcessAll).unwrap();
        assert_eq!(second, TaskExecutionResult::Finished);
        assert!(task.is_finished());
        assert_eq!(op.finalize_calls.load(Ordering::SeqCst), 2);
    }
}
