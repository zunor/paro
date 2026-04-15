//! Event graph: dependencies, activation, and completion callbacks.

use parking_lot::Mutex;
use paro_common::error as paro_error;
use std::panic::{self, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};

type ScheduleCallback = Box<dyn FnOnce() -> paro_error::Result<()> + Send + Sync>;
type FinishCallback = Box<dyn FnOnce() + Send + Sync>;
type EventErrorHandler = Arc<dyn Fn(paro_error::ParoError) + Send + Sync>;

fn panic_payload_to_string(payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_string()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "unknown panic payload".to_string()
    }
}

/// Event represents a unit of work that can have dependencies on other events.
///
/// Events activate only after all dependencies are complete. Once an event finishes,
/// it notifies all parent events that depend on it.
pub struct Event {
    /// The current number of finished tasks.
    finished_tasks: AtomicUsize,
    /// The total number of tasks for this event.
    total_tasks: AtomicUsize,
    /// The number of completed dependencies.
    finished_dependencies: AtomicUsize,
    /// The total number of dependencies.
    total_dependencies: AtomicUsize,
    /// Whether the event has been activated.
    activated: AtomicBool,
    /// Events that depend on this event (parent events).
    parents: Mutex<Vec<Weak<Event>>>,
    /// Whether the event has finished.
    finished: AtomicBool,
    /// Callback to execute when the event is activated.
    schedule_callback: Mutex<Option<ScheduleCallback>>,
    /// Error handler shared by activation and finish failures.
    error_handler: Mutex<Option<EventErrorHandler>>,
    /// Callback to execute when the event finishes.
    finish_callback: Mutex<Option<FinishCallback>>,
}

impl Event {
    /// Create a new event.
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            finished_tasks: AtomicUsize::new(0),
            total_tasks: AtomicUsize::new(0),
            finished_dependencies: AtomicUsize::new(0),
            total_dependencies: AtomicUsize::new(0),
            activated: AtomicBool::new(false),
            parents: Mutex::new(Vec::new()),
            finished: AtomicBool::new(false),
            schedule_callback: Mutex::new(None),
            error_handler: Mutex::new(None),
            finish_callback: Mutex::new(None),
        })
    }

    /// Set the callback to execute when the event is activated.
    ///
    /// Invariant: all schedule callbacks must be installed before activation starts.
    pub fn set_schedule_callback<F>(self: &Arc<Self>, callback: F)
    where
        F: FnOnce() -> paro_error::Result<()> + Send + Sync + 'static,
    {
        if self.activated.load(Ordering::SeqCst) || self.finished.load(Ordering::SeqCst) {
            panic!("Cannot set a schedule callback after the event has activated or finished");
        }
        *self.schedule_callback.lock() = Some(Box::new(callback));
    }

    /// Set the callback to execute when the event finishes.
    pub fn set_finish_callback<F>(self: &Arc<Self>, callback: F)
    where
        F: FnOnce() + Send + Sync + 'static,
    {
        *self.finish_callback.lock() = Some(Box::new(callback));
    }

    /// Set the error handler used by activation and finish failures.
    pub(crate) fn set_error_handler<F>(&self, handler: F)
    where
        F: Fn(paro_error::ParoError) + Send + Sync + 'static,
    {
        *self.error_handler.lock() = Some(Arc::new(handler));
    }

    /// Activate the event through the synchronous root kickoff path.
    pub(crate) fn try_activate(self: &Arc<Self>) -> paro_error::Result<()> {
        if self
            .activated
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return Ok(());
        }

        let result = self.run_schedule_callback();
        if result.is_ok() && self.total_tasks.load(Ordering::SeqCst) == 0 {
            self.finish();
        }

        result
    }

    /// Activate the event through the dependency-driven path.
    pub(crate) fn activate(self: &Arc<Self>) {
        if self
            .activated
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }

        match self.run_schedule_callback() {
            Ok(()) => {
                if self.total_tasks.load(Ordering::SeqCst) == 0 {
                    self.finish();
                }
            }
            Err(error) => self.report_error(error),
        }
    }

    fn run_schedule_callback(&self) -> paro_error::Result<()> {
        let callback = self.schedule_callback.lock().take();
        let Some(callback) = callback else {
            return Ok(());
        };

        match panic::catch_unwind(AssertUnwindSafe(callback)) {
            Ok(result) => result,
            Err(payload) => Err(paro_error::internal(format!(
                "Event schedule callback panicked: {}",
                panic_payload_to_string(payload)
            ))),
        }
    }

    fn report_error(&self, error: paro_error::ParoError) {
        if let Some(handler) = self.error_handler.lock().clone() {
            handler(error);
            return;
        }

        panic!("Event error but no error handler is installed: {}", error);
    }

    /// Add a dependency on another event.
    ///
    /// Invariant: all dependencies must be added during graph construction,
    /// before the event is activated or finished.
    pub fn add_dependency(self: &Arc<Self>, dependency: &Arc<Event>) {
        if self.activated.load(Ordering::SeqCst) || self.finished.load(Ordering::SeqCst) {
            panic!("Cannot add dependencies after the event has activated or finished");
        }
        self.total_dependencies.fetch_add(1, Ordering::SeqCst);
        dependency.parents.lock().push(Arc::downgrade(self));
    }

    /// Check if this event has any dependencies.
    pub fn has_dependencies(&self) -> bool {
        self.total_dependencies.load(Ordering::SeqCst) != 0
    }

    /// Get the total number of dependencies.
    pub fn get_total_dependencies(&self) -> usize {
        self.total_dependencies.load(Ordering::SeqCst)
    }

    /// Complete a dependency.
    ///
    /// When all dependencies are complete, the event is automatically activated.
    pub fn complete_dependency(self: &Arc<Self>) {
        let current_finished = self.finished_dependencies.fetch_add(1, Ordering::SeqCst) + 1;
        let total = self.total_dependencies.load(Ordering::SeqCst);

        debug_assert!(
            current_finished <= total,
            "Finished dependencies ({}) exceeded total ({})",
            current_finished,
            total
        );

        if current_finished == total {
            // Dependencies are immutable after activation, so this read is stable.
            self.activate();
        }
    }

    /// Set the tasks for this event.
    ///
    /// The event will finish when all tasks complete.
    pub fn set_tasks(&self, task_count: usize) {
        debug_assert_eq!(self.total_tasks.load(Ordering::SeqCst), 0);
        debug_assert!(task_count > 0, "Task count must be greater than 0");
        self.total_tasks.store(task_count, Ordering::SeqCst);
    }

    /// Schedule tasks to a TaskScheduler and bind them to this event.
    ///
    /// This method:
    /// 1. Sets the total task count on this event
    /// 2. Wraps each task with EventTask for automatic finish_task() notification
    /// 3. Schedules the wrapped tasks to the TaskScheduler
    ///
    /// When all tasks complete, the event will automatically finish.
    pub fn schedule_tasks_to_scheduler(
        self: &Arc<Self>,
        tasks: Vec<Arc<parking_lot::Mutex<dyn crate::task::Task>>>,
        scheduler: &Arc<crate::scheduler::TaskScheduler>,
    ) {
        let producer = scheduler.create_producer();
        self.schedule_tasks_to_scheduler_with_producer(tasks, scheduler, &producer);
    }

    /// Schedule tasks using a caller-provided producer token.
    ///
    /// This enables per-query task isolation and producer-specific cancellation.
    pub fn schedule_tasks_to_scheduler_with_producer(
        self: &Arc<Self>,
        tasks: Vec<Arc<parking_lot::Mutex<dyn crate::task::Task>>>,
        scheduler: &Arc<crate::scheduler::TaskScheduler>,
        producer: &crate::task::ProducerToken,
    ) {
        debug_assert!(!tasks.is_empty(), "Cannot schedule empty task list");
        debug_assert_eq!(
            self.total_tasks.load(Ordering::SeqCst),
            0,
            "Tasks have already been set for this event"
        );

        self.total_tasks.store(tasks.len(), Ordering::SeqCst);

        let wrapped_tasks: Vec<Arc<parking_lot::Mutex<dyn crate::task::Task>>> = tasks
            .into_iter()
            .map(|task| crate::event_task::BoxedEventTask::from_arc_mutex(task, self.clone()))
            .collect();

        scheduler.schedule_tasks_with_token(producer, wrapped_tasks);
    }

    /// Mark a task as finished.
    ///
    /// When all tasks are finished, the event is automatically completed.
    pub fn finish_task(self: &Arc<Self>) {
        let current_tasks = self.total_tasks.load(Ordering::SeqCst);
        let current_finished = self.finished_tasks.fetch_add(1, Ordering::SeqCst) + 1;

        debug_assert!(
            current_finished <= current_tasks,
            "Finished tasks ({}) exceeded total ({})",
            current_finished,
            current_tasks
        );

        if current_finished == current_tasks {
            self.finish();
        }
    }

    /// Finish the event.
    ///
    /// `is_finished()` becomes true as soon as the CAS below succeeds. The finish
    /// callback may still be running at that point.
    fn finish(self: &Arc<Self>) {
        if self
            .finished
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }

        if let Some(callback) = self.finish_callback.lock().take() {
            if let Err(payload) = panic::catch_unwind(AssertUnwindSafe(callback)) {
                let message = panic_payload_to_string(payload);
                self.report_error(paro_error::internal(format!(
                    "Event finish_callback panicked: {}",
                    message
                )));
                return;
            }
        }

        let parents = self.parents.lock().clone();
        for parent_weak in parents {
            if let Some(parent) = parent_weak.upgrade() {
                parent.complete_dependency();
            }
        }
    }

    /// Check if the event is finished.
    pub fn is_finished(&self) -> bool {
        self.finished.load(Ordering::SeqCst)
    }

    /// Get the number of finished tasks.
    pub fn finished_task_count(&self) -> usize {
        self.finished_tasks.load(Ordering::SeqCst)
    }

    /// Get the total number of tasks.
    pub fn total_task_count(&self) -> usize {
        self.total_tasks.load(Ordering::SeqCst)
    }

    /// Get the number of parent events.
    pub fn parent_count(&self) -> usize {
        self.parents.lock().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scheduler::TaskScheduler;
    use crate::task::{Task, TaskExecutionMode, TaskExecutionResult};
    use parking_lot::Mutex;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Arc;

    #[test]
    fn test_event_basic() {
        let event = Event::new();
        assert!(!event.is_finished());
        assert_eq!(event.total_task_count(), 0);
        assert_eq!(event.finished_task_count(), 0);
    }

    #[test]
    fn test_event_single_task() {
        let event = Event::new();
        event.set_tasks(1);

        assert_eq!(event.total_task_count(), 1);
        assert!(!event.is_finished());

        event.finish_task();
        assert!(event.is_finished());
        assert_eq!(event.finished_task_count(), 1);
    }

    #[test]
    fn test_event_multiple_tasks() {
        let event = Event::new();
        event.set_tasks(3);

        event.finish_task();
        assert!(!event.is_finished());
        assert_eq!(event.finished_task_count(), 1);

        event.finish_task();
        assert!(!event.is_finished());
        assert_eq!(event.finished_task_count(), 2);

        event.finish_task();
        assert!(event.is_finished());
        assert_eq!(event.finished_task_count(), 3);
    }

    #[test]
    fn test_event_dependency() {
        let child = Event::new();
        let parent = Event::new();

        parent.add_dependency(&child);

        assert!(parent.has_dependencies());
        assert_eq!(parent.get_total_dependencies(), 1);
        assert_eq!(child.parent_count(), 1);
    }

    #[test]
    fn test_event_dependency_completion() {
        let child = Event::new();
        let parent = Event::new();
        let parent_activated = Arc::new(AtomicBool::new(false));
        let parent_activated_clone = parent_activated.clone();

        parent.set_schedule_callback(move || {
            parent_activated_clone.store(true, Ordering::SeqCst);
            Ok(())
        });
        parent.add_dependency(&child);

        assert!(!parent_activated.load(Ordering::SeqCst));

        child.set_tasks(1);
        child.finish_task();

        assert!(parent_activated.load(Ordering::SeqCst));
        assert!(parent.is_finished());
    }

    #[test]
    fn test_event_multiple_dependencies() {
        let child1 = Event::new();
        let child2 = Event::new();
        let parent = Event::new();
        let parent_activated = Arc::new(AtomicBool::new(false));
        let parent_activated_clone = parent_activated.clone();

        parent.set_schedule_callback(move || {
            parent_activated_clone.store(true, Ordering::SeqCst);
            Ok(())
        });
        parent.add_dependency(&child1);
        parent.add_dependency(&child2);

        assert_eq!(parent.get_total_dependencies(), 2);

        child1.set_tasks(1);
        child1.finish_task();
        assert!(!parent_activated.load(Ordering::SeqCst));

        child2.set_tasks(1);
        child2.finish_task();
        assert!(parent_activated.load(Ordering::SeqCst));
        assert!(parent.is_finished());
    }

    #[test]
    fn test_event_finish_callback() {
        let event = Event::new();
        let finished = Arc::new(AtomicBool::new(false));
        let finished_clone = finished.clone();

        event.set_finish_callback(move || {
            finished_clone.store(true, Ordering::SeqCst);
        });

        event.set_tasks(1);
        assert!(!finished.load(Ordering::SeqCst));

        event.finish_task();
        assert!(finished.load(Ordering::SeqCst));
    }

    #[test]
    fn test_event_chain() {
        let event1 = Event::new();
        let event2 = Event::new();
        let event3 = Event::new();
        let counter = Arc::new(AtomicUsize::new(0));

        let counter1 = counter.clone();
        event1.set_finish_callback(move || {
            counter1.fetch_add(1, Ordering::SeqCst);
        });

        let counter2 = counter.clone();
        event2.set_finish_callback(move || {
            counter2.fetch_add(10, Ordering::SeqCst);
        });

        let counter3 = counter.clone();
        event3.set_finish_callback(move || {
            counter3.fetch_add(100, Ordering::SeqCst);
        });

        let event2_clone = event2.clone();
        event2.set_schedule_callback(move || {
            event2_clone.set_tasks(1);
            Ok(())
        });

        let event3_clone = event3.clone();
        event3.set_schedule_callback(move || {
            event3_clone.set_tasks(1);
            Ok(())
        });

        event2.add_dependency(&event1);
        event3.add_dependency(&event2);

        event1.set_tasks(1);
        event1.finish_task();
        assert_eq!(counter.load(Ordering::SeqCst), 1);

        event2.finish_task();
        assert_eq!(counter.load(Ordering::SeqCst), 11);

        event3.finish_task();
        assert_eq!(counter.load(Ordering::SeqCst), 111);
    }

    #[test]
    fn test_root_event_activate_triggers_callback() {
        let event = Event::new();
        let activated = Arc::new(AtomicBool::new(false));
        let activated_clone = activated.clone();

        event.set_schedule_callback(move || {
            activated_clone.store(true, Ordering::SeqCst);
            Ok(())
        });

        event.try_activate().unwrap();

        assert!(activated.load(Ordering::SeqCst));
        assert!(event.is_finished());
    }

    #[test]
    fn test_root_event_without_tasks_auto_finishes() {
        let event = Event::new();

        event.try_activate().unwrap();

        assert!(event.is_finished());
    }

    #[test]
    fn test_activation_error_does_not_finish_event() {
        let event = Event::new();
        event.set_schedule_callback(|| Err(paro_error::internal("boom")));

        let result = event.try_activate();

        assert!(result.is_err());
        assert!(!event.is_finished());
    }

    #[test]
    fn test_activation_panic_reports_error() {
        let event = Event::new();
        let error_message = Arc::new(Mutex::new(None));
        let error_message_clone = error_message.clone();

        event.set_error_handler(move |error| {
            *error_message_clone.lock() = Some(error.to_string());
        });
        event.set_schedule_callback(|| panic!("panic in callback"));

        event.activate();

        let error = error_message.lock().clone().expect("activation error");
        assert!(error.contains("Event schedule callback panicked"));
        assert!(!event.is_finished());
    }

    #[test]
    fn test_activate_is_idempotent() {
        let event = Event::new();
        let activation_count = Arc::new(AtomicUsize::new(0));
        let activation_count_clone = activation_count.clone();

        event.set_schedule_callback(move || {
            activation_count_clone.fetch_add(1, Ordering::SeqCst);
            Ok(())
        });

        event.try_activate().unwrap();
        event.try_activate().unwrap();

        assert_eq!(activation_count.load(Ordering::SeqCst), 1);
        assert!(event.is_finished());
    }

    #[test]
    fn test_finish_is_idempotent() {
        let event = Event::new();
        let finish_count = Arc::new(AtomicUsize::new(0));
        let finish_count_clone = finish_count.clone();

        event.set_finish_callback(move || {
            finish_count_clone.fetch_add(1, Ordering::SeqCst);
        });

        event.finish();
        event.finish();

        assert_eq!(finish_count.load(Ordering::SeqCst), 1);
        assert!(event.is_finished());
    }

    #[test]
    fn test_finish_callback_panic_stops_parent_propagation() {
        let child = Event::new();
        let parent = Event::new();
        let parent_activated = Arc::new(AtomicBool::new(false));
        let parent_activated_clone = parent_activated.clone();
        let error_message = Arc::new(Mutex::new(None));
        let error_message_clone = error_message.clone();

        parent.add_dependency(&child);
        parent.set_schedule_callback(move || {
            parent_activated_clone.store(true, Ordering::SeqCst);
            Ok(())
        });

        child.set_error_handler(move |error| {
            *error_message_clone.lock() = Some(error.to_string());
        });
        child.set_finish_callback(|| panic!("finish callback panic"));
        child.set_tasks(1);

        child.finish_task();

        let error = error_message.lock().clone().expect("finish callback error");
        assert!(error.contains("Event finish_callback panicked"));
        assert!(!parent_activated.load(Ordering::SeqCst));
        assert!(!parent.is_finished());
    }

    #[test]
    #[should_panic(expected = "Cannot add dependencies after the event has activated or finished")]
    fn test_add_dependency_after_activate_panics() {
        let event = Event::new();
        let dependency = Event::new();

        event.try_activate().unwrap();
        event.add_dependency(&dependency);
    }

    #[test]
    #[should_panic(
        expected = "Cannot set a schedule callback after the event has activated or finished"
    )]
    fn test_set_schedule_callback_after_activate_panics() {
        let event = Event::new();

        event.try_activate().unwrap();
        event.set_schedule_callback(|| Ok(()));
    }

    #[test]
    #[should_panic(
        expected = "Cannot set a schedule callback after the event has activated or finished"
    )]
    fn test_set_schedule_callback_after_finish_panics() {
        let event = Event::new();

        event.finish();
        event.set_schedule_callback(|| Ok(()));
    }

    #[test]
    fn test_event_schedule_tasks_to_scheduler() {
        struct SimpleTask {
            executed: Arc<AtomicBool>,
        }

        impl Task for SimpleTask {
            fn execute(
                &mut self,
                _mode: TaskExecutionMode,
            ) -> paro_common::error::Result<TaskExecutionResult> {
                self.executed.store(true, Ordering::SeqCst);
                Ok(TaskExecutionResult::Finished)
            }

            fn task_type(&self) -> &str {
                "SimpleTask"
            }
        }

        let scheduler = Arc::new(TaskScheduler::new());
        let event = Event::new();
        let executed1 = Arc::new(AtomicBool::new(false));
        let executed2 = Arc::new(AtomicBool::new(false));

        let tasks: Vec<Arc<Mutex<dyn Task>>> = vec![
            Arc::new(Mutex::new(SimpleTask {
                executed: executed1.clone(),
            })),
            Arc::new(Mutex::new(SimpleTask {
                executed: executed2.clone(),
            })),
        ];

        event.schedule_tasks_to_scheduler(tasks, &scheduler);

        assert_eq!(event.total_task_count(), 2);
        assert!(!event.is_finished());
        assert_eq!(scheduler.pending_tasks(), 2);

        let marker = AtomicBool::new(true);
        scheduler.execute_tasks(&marker, 10);

        assert!(executed1.load(Ordering::SeqCst));
        assert!(executed2.load(Ordering::SeqCst));
        assert!(event.is_finished());
        assert_eq!(event.finished_task_count(), 2);
    }
}
