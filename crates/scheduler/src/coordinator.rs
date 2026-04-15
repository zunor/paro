// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Root event activation and completion waiting on a shared [`TaskScheduler`].

use crate::event::Event;
use crate::scheduler::TaskScheduler;
use crate::task::ProducerToken;
use parking_lot::Mutex;
use paro_common::error as paro_error;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// EventCoordinator manages the lifecycle of events during query execution.
///
/// It coordinates root activation, tracks completion, and owns coordinator-local
/// error state so event callback failures do not leak across shared schedulers.
pub struct EventCoordinator {
    /// The task scheduler used for task execution.
    scheduler: Arc<TaskScheduler>,
    /// Optional producer token used for per-query task isolation.
    producer: Option<ProducerToken>,
    /// List of all events being coordinated.
    events: Mutex<Vec<Arc<Event>>>,
    /// Number of completed events.
    completed_events: Arc<AtomicUsize>,
    /// Total number of events registered.
    total_events: AtomicUsize,
    /// Whether execution has been cancelled.
    cancelled: AtomicBool,
    /// Whether a coordinator-local error has occurred.
    has_error: Arc<AtomicBool>,
    /// The first coordinator-local error, if any.
    error_store: Arc<Mutex<Option<paro_error::ParoError>>>,
}

impl EventCoordinator {
    /// Create a new event coordinator with the given scheduler.
    pub fn new(scheduler: Arc<TaskScheduler>) -> Self {
        Self::with_producer_token(scheduler, None)
    }

    /// Create a new event coordinator bound to a producer token.
    pub fn with_producer(scheduler: Arc<TaskScheduler>, producer: ProducerToken) -> Self {
        Self::with_producer_token(scheduler, Some(producer))
    }

    fn with_producer_token(scheduler: Arc<TaskScheduler>, producer: Option<ProducerToken>) -> Self {
        Self {
            scheduler,
            producer,
            events: Mutex::new(Vec::new()),
            completed_events: Arc::new(AtomicUsize::new(0)),
            total_events: AtomicUsize::new(0),
            cancelled: AtomicBool::new(false),
            has_error: Arc::new(AtomicBool::new(false)),
            error_store: Arc::new(Mutex::new(None)),
        }
    }

    fn record_error(&self, error: paro_error::ParoError) {
        let mut store = self.error_store.lock();
        if store.is_none() {
            *store = Some(error);
        }
        drop(store);
        self.has_error.store(true, Ordering::SeqCst);
    }

    fn current_error(&self) -> Option<paro_error::ParoError> {
        self.get_error().or_else(|| {
            if let Some(producer) = &self.producer {
                self.scheduler
                    .get_error_for_producer(producer)
                    .map(|error| paro_error::internal(error.message))
            } else {
                self.scheduler
                    .get_error()
                    .map(|error| paro_error::internal(error.message))
            }
        })
    }

    /// Add an event to be coordinated.
    pub fn add_event(&self, event: Arc<Event>) {
        if self.cancelled.load(Ordering::SeqCst) {
            return;
        }

        let completed_events = self.completed_events.clone();
        event.set_finish_callback(move || {
            completed_events.fetch_add(1, Ordering::SeqCst);
        });

        let has_error = self.has_error.clone();
        let error_store = self.error_store.clone();
        let scheduler = self.scheduler.clone();
        let producer = self.producer.clone();
        event.set_error_handler(move |error| {
            let mut store = error_store.lock();
            if store.is_none() {
                *store = Some(error);
            }
            drop(store);

            has_error.store(true, Ordering::SeqCst);
            if let Some(ref token) = producer {
                let _ = scheduler.cancel_tasks_for_producer(token);
            } else {
                scheduler.cancel_tasks();
            }
            scheduler.signal_task_rescheduled();
        });

        self.events.lock().push(event);
        self.total_events.fetch_add(1, Ordering::SeqCst);
    }

    /// Activate a single root event through the synchronous kickoff path.
    fn activate_event(&self, event: &Arc<Event>) -> paro_error::Result<()> {
        if let Err(error) = event.try_activate() {
            self.record_error(error.clone());
            self.cancel();
            return Err(error);
        }
        Ok(())
    }

    /// Activate all root events that have no dependencies.
    pub fn activate_ready_events(&self) -> paro_error::Result<()> {
        let ready_events: Vec<Arc<Event>> = {
            let events = self.events.lock();
            events
                .iter()
                .filter(|event| !event.has_dependencies())
                .cloned()
                .collect()
        };

        for event in ready_events {
            self.activate_event(&event)?;
        }

        Ok(())
    }

    /// Execute some scheduled tasks through the bound scheduler.
    ///
    /// If a producer token is configured, only tasks from that producer are executed.
    pub fn execute_some_tasks(&self, max_tasks: usize) -> usize {
        let marker = AtomicBool::new(true);
        if let Some(producer) = &self.producer {
            self.scheduler
                .execute_tasks_for_producer(producer, &marker, max_tasks)
        } else {
            self.scheduler.execute_tasks(&marker, max_tasks)
        }
    }

    /// Wait for tasks to become available.
    ///
    /// If a producer token is configured, waits only for that producer's tasks.
    pub fn wait_for_task(&self) -> bool {
        if let Some(producer) = &self.producer {
            self.scheduler.wait_for_task_for_producer(producer)
        } else {
            self.scheduler.wait_for_task()
        }
    }

    /// Wait for all events to complete.
    pub fn wait_for_completion(&self) -> paro_error::Result<()> {
        let total = self.total_events.load(Ordering::SeqCst);

        while self.completed_events.load(Ordering::SeqCst) < total {
            if self.has_error() {
                self.cancel();
                return Err(self
                    .current_error()
                    .unwrap_or_else(|| paro_error::internal("Unknown error")));
            }

            if self.cancelled.load(Ordering::SeqCst) {
                return Err(paro_error::internal("Execution cancelled"));
            }

            if self.execute_some_tasks(10) == 0 {
                let _ = self.wait_for_task();
            }
        }

        if self.has_error() {
            return Err(self
                .current_error()
                .unwrap_or_else(|| paro_error::internal("Unknown error")));
        }

        Ok(())
    }

    /// Wait for completion with a timeout.
    ///
    /// Returns `Ok(true)` if completed, `Ok(false)` if timed out.
    pub fn wait_for_completion_timeout(&self, timeout: Duration) -> paro_error::Result<bool> {
        let total = self.total_events.load(Ordering::SeqCst);
        let start = std::time::Instant::now();

        while self.completed_events.load(Ordering::SeqCst) < total {
            if start.elapsed() > timeout {
                return Ok(false);
            }

            if self.has_error() {
                self.cancel();
                return Err(self
                    .current_error()
                    .unwrap_or_else(|| paro_error::internal("Unknown error")));
            }

            if self.cancelled.load(Ordering::SeqCst) {
                return Err(paro_error::internal("Execution cancelled"));
            }

            if self.execute_some_tasks(10) == 0 {
                let _ = self.wait_for_task();
            }
        }

        if self.has_error() {
            return Err(self
                .current_error()
                .unwrap_or_else(|| paro_error::internal("Unknown error")));
        }

        Ok(true)
    }

    /// Cancel all pending work owned by this coordinator.
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::SeqCst);
        if let Some(producer) = &self.producer {
            let _ = self.scheduler.cancel_tasks_for_producer(producer);
        } else {
            self.scheduler.cancel_tasks();
        }
        self.scheduler.signal_task_rescheduled();
    }

    /// Check if execution has completed (all events finished).
    pub fn is_complete(&self) -> bool {
        self.completed_events.load(Ordering::SeqCst) >= self.total_events.load(Ordering::SeqCst)
    }

    /// Check if any local or scheduler-global error has occurred.
    pub fn has_error(&self) -> bool {
        self.has_error.load(Ordering::SeqCst)
            || self
                .producer
                .as_ref()
                .map(|producer| self.scheduler.has_error_for_producer(producer))
                .unwrap_or_else(|| self.scheduler.has_error())
    }

    /// Get the first coordinator-local error, if any.
    pub fn get_error(&self) -> Option<paro_error::ParoError> {
        self.error_store.lock().clone()
    }

    /// Get the number of completed events.
    pub fn completed_count(&self) -> usize {
        self.completed_events.load(Ordering::SeqCst)
    }

    /// Get the total number of events.
    pub fn total_count(&self) -> usize {
        self.total_events.load(Ordering::SeqCst)
    }

    /// Get a reference to the scheduler.
    pub fn scheduler(&self) -> &Arc<TaskScheduler> {
        &self.scheduler
    }

    /// Get the producer token bound to this coordinator, if any.
    pub fn producer_token(&self) -> Option<ProducerToken> {
        self.producer.clone()
    }

    /// Reset the coordinator for reuse.
    pub fn reset(&self) {
        self.events.lock().clear();
        self.completed_events.store(0, Ordering::SeqCst);
        self.total_events.store(0, Ordering::SeqCst);
        self.cancelled.store(false, Ordering::SeqCst);
        self.has_error.store(false, Ordering::SeqCst);
        *self.error_store.lock() = None;
        if let Some(producer) = &self.producer {
            self.scheduler.reset_errors_for_producer(producer);
        } else {
            self.scheduler.reset_errors();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task::{Task, TaskExecutionMode, TaskExecutionResult};
    use parking_lot::Mutex;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Arc;

    struct IncrementTask {
        counter: Arc<AtomicUsize>,
    }

    impl Task for IncrementTask {
        fn execute(
            &mut self,
            _mode: TaskExecutionMode,
        ) -> paro_common::error::Result<TaskExecutionResult> {
            self.counter.fetch_add(1, Ordering::SeqCst);
            Ok(TaskExecutionResult::Finished)
        }

        fn task_type(&self) -> &str {
            "IncrementTask"
        }
    }

    #[test]
    fn test_coordinator_basic() {
        let scheduler = Arc::new(TaskScheduler::new());
        let coordinator = EventCoordinator::new(scheduler);

        assert_eq!(coordinator.total_count(), 0);
        assert_eq!(coordinator.completed_count(), 0);
        assert!(coordinator.is_complete());
    }

    #[test]
    fn test_root_event_callback_activated_by_coordinator() {
        let scheduler = Arc::new(TaskScheduler::new());
        let coordinator = EventCoordinator::new(scheduler);
        let activated = Arc::new(AtomicBool::new(false));
        let activated_clone = activated.clone();

        let event = Event::new();
        event.set_schedule_callback(move || {
            activated_clone.store(true, Ordering::SeqCst);
            Ok(())
        });
        coordinator.add_event(event);

        coordinator.activate_ready_events().unwrap();

        assert!(activated.load(Ordering::SeqCst));
        assert_eq!(coordinator.completed_count(), 1);
        assert!(coordinator.is_complete());
    }

    #[test]
    fn test_root_event_without_tasks_auto_finishes() {
        let scheduler = Arc::new(TaskScheduler::new());
        let coordinator = EventCoordinator::new(scheduler);
        let event = Event::new();

        coordinator.add_event(event);
        coordinator.activate_ready_events().unwrap();

        assert_eq!(coordinator.completed_count(), 1);
        assert!(coordinator.is_complete());
    }

    #[test]
    fn test_coordinator_with_tasks() {
        let scheduler = Arc::new(TaskScheduler::new());
        let coordinator = EventCoordinator::new(scheduler.clone());
        let counter = Arc::new(AtomicUsize::new(0));
        let event = Event::new();
        let counter_clone = counter.clone();
        let scheduler_clone = scheduler.clone();
        let event_for_callback = event.clone();

        event.set_schedule_callback(move || {
            let tasks: Vec<Arc<parking_lot::Mutex<dyn Task>>> = vec![
                Arc::new(parking_lot::Mutex::new(IncrementTask {
                    counter: counter_clone.clone(),
                })),
                Arc::new(parking_lot::Mutex::new(IncrementTask {
                    counter: counter_clone.clone(),
                })),
            ];
            event_for_callback.schedule_tasks_to_scheduler(tasks, &scheduler_clone);
            Ok(())
        });
        coordinator.add_event(event);

        coordinator.activate_ready_events().unwrap();
        let result = coordinator.wait_for_completion_timeout(Duration::from_secs(5));

        assert!(result.is_ok());
        assert!(result.unwrap());
        assert_eq!(counter.load(Ordering::SeqCst), 2);
        assert!(coordinator.is_complete());
    }

    #[test]
    fn test_coordinator_event_chain() {
        let scheduler = Arc::new(TaskScheduler::new());
        let coordinator = EventCoordinator::new(scheduler.clone());
        let counter = Arc::new(AtomicUsize::new(0));
        let event1 = Event::new();
        let event2 = Event::new();

        event2.add_dependency(&event1);
        coordinator.add_event(event1.clone());
        coordinator.add_event(event2.clone());

        let counter1 = counter.clone();
        let scheduler1 = scheduler.clone();
        let event1_for_callback = event1.clone();
        event1.set_schedule_callback(move || {
            let tasks: Vec<Arc<parking_lot::Mutex<dyn Task>>> =
                vec![Arc::new(parking_lot::Mutex::new(IncrementTask {
                    counter: counter1.clone(),
                }))];
            event1_for_callback.schedule_tasks_to_scheduler(tasks, &scheduler1);
            Ok(())
        });

        let counter2 = counter.clone();
        let scheduler2 = scheduler.clone();
        let event2_for_callback = event2.clone();
        event2.set_schedule_callback(move || {
            let tasks: Vec<Arc<parking_lot::Mutex<dyn Task>>> =
                vec![Arc::new(parking_lot::Mutex::new(IncrementTask {
                    counter: counter2.clone(),
                }))];
            event2_for_callback.schedule_tasks_to_scheduler(tasks, &scheduler2);
            Ok(())
        });

        coordinator.activate_ready_events().unwrap();
        let result = coordinator.wait_for_completion_timeout(Duration::from_secs(5));

        assert!(result.is_ok());
        assert!(result.unwrap());
        assert_eq!(counter.load(Ordering::SeqCst), 2);
        assert!(coordinator.is_complete());
    }

    #[test]
    fn test_activate_ready_events_fail_fast() {
        let scheduler = Arc::new(TaskScheduler::new());
        let coordinator = EventCoordinator::new(scheduler);
        let second_activated = Arc::new(AtomicBool::new(false));
        let second_activated_clone = second_activated.clone();

        let first = Event::new();
        first.set_schedule_callback(|| Err(paro_error::internal("first root failed")));
        coordinator.add_event(first);

        let second = Event::new();
        second.set_schedule_callback(move || {
            second_activated_clone.store(true, Ordering::SeqCst);
            Ok(())
        });
        coordinator.add_event(second);

        let error = coordinator.activate_ready_events().unwrap_err();
        assert!(error.to_string().contains("first root failed"));
        assert!(!second_activated.load(Ordering::SeqCst));
        assert!(coordinator.has_error());
        assert!(coordinator.get_error().is_some());
    }

    #[test]
    fn test_repeated_kickoff_does_not_repeat_completion() {
        let scheduler = Arc::new(TaskScheduler::new());
        let coordinator = EventCoordinator::new(scheduler);
        let event = Event::new();

        coordinator.add_event(event);
        coordinator.activate_ready_events().unwrap();
        coordinator.activate_ready_events().unwrap();

        assert_eq!(coordinator.completed_count(), 1);
    }

    #[test]
    fn test_callback_panic_marks_coordinator_error() {
        let scheduler = Arc::new(TaskScheduler::new());
        let coordinator = EventCoordinator::new(scheduler);
        let event = Event::new();

        event.set_schedule_callback(|| panic!("panic in root callback"));
        coordinator.add_event(event);

        let error = coordinator.activate_ready_events().unwrap_err();
        assert!(error
            .to_string()
            .contains("Event schedule callback panicked"));
        assert!(coordinator.has_error());
    }

    #[test]
    fn test_wait_for_completion_returns_coordinator_local_error() {
        let scheduler = Arc::new(TaskScheduler::new());
        let coordinator = EventCoordinator::new(scheduler.clone());
        let root = Event::new();
        let dependent = Event::new();

        dependent.add_dependency(&root);
        coordinator.add_event(root.clone());
        coordinator.add_event(dependent.clone());

        let scheduler_clone = scheduler.clone();
        let root_for_callback = root.clone();
        root.set_schedule_callback(move || {
            let tasks: Vec<Arc<parking_lot::Mutex<dyn Task>>> =
                vec![Arc::new(parking_lot::Mutex::new(IncrementTask {
                    counter: Arc::new(AtomicUsize::new(0)),
                }))];
            root_for_callback.schedule_tasks_to_scheduler(tasks, &scheduler_clone);
            Ok(())
        });
        dependent
            .set_schedule_callback(|| Err(paro_error::internal("dependent activation failed")));

        coordinator.activate_ready_events().unwrap();
        let error = coordinator.wait_for_completion().unwrap_err();

        assert!(error.to_string().contains("dependent activation failed"));
        assert!(coordinator.has_error());
        assert_eq!(coordinator.completed_count(), 1);
    }

    #[test]
    fn test_error_handler_only_cancels_bound_producer() {
        let scheduler = Arc::new(TaskScheduler::new());
        let producer_a = scheduler.create_producer();
        let producer_b = scheduler.create_producer();
        let coordinator = EventCoordinator::with_producer(scheduler.clone(), producer_a.clone());
        let counter_b = Arc::new(AtomicUsize::new(0));

        let task_a: Arc<Mutex<dyn Task>> = Arc::new(Mutex::new(IncrementTask {
            counter: Arc::new(AtomicUsize::new(0)),
        }));
        let task_b: Arc<Mutex<dyn Task>> = Arc::new(Mutex::new(IncrementTask {
            counter: counter_b.clone(),
        }));
        scheduler.schedule_task_with_token(&producer_a, task_a);
        scheduler.schedule_task_with_token(&producer_b, task_b);

        let event = Event::new();
        event.set_schedule_callback(|| Err(paro_error::internal("root failure")));
        coordinator.add_event(event);

        coordinator.activate_ready_events().unwrap_err();

        assert_eq!(scheduler.pending_tasks_for_producer(&producer_a), 0);
        assert_eq!(scheduler.pending_tasks_for_producer(&producer_b), 1);

        let marker = AtomicBool::new(true);
        scheduler.execute_tasks_for_producer(&producer_b, &marker, 10);
        assert_eq!(counter_b.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_coordinator_local_error_does_not_leak_to_other_coordinators() {
        let scheduler = Arc::new(TaskScheduler::new());
        let coordinator_a =
            EventCoordinator::with_producer(scheduler.clone(), scheduler.create_producer());
        let coordinator_b =
            EventCoordinator::with_producer(scheduler.clone(), scheduler.create_producer());

        let event = Event::new();
        event.set_schedule_callback(|| Err(paro_error::internal("query a failed")));
        coordinator_a.add_event(event);

        coordinator_a.activate_ready_events().unwrap_err();

        assert!(coordinator_a.has_error());
        assert!(!coordinator_b.has_error());
    }

    #[test]
    fn test_coordinator_cancel() {
        let scheduler = Arc::new(TaskScheduler::new());
        let coordinator = EventCoordinator::new(scheduler);
        let event = Event::new();

        coordinator.add_event(event);
        coordinator.cancel();

        let result = coordinator.wait_for_completion_timeout(Duration::from_millis(100));
        assert!(result.is_err());
    }

    #[test]
    fn test_coordinator_reset_clears_errors() {
        let scheduler = Arc::new(TaskScheduler::new());
        let coordinator = EventCoordinator::new(scheduler);
        let event = Event::new();

        event.set_schedule_callback(|| Err(paro_error::internal("root failure")));
        coordinator.add_event(event);
        coordinator.activate_ready_events().unwrap_err();

        assert!(coordinator.has_error());
        assert!(coordinator.get_error().is_some());

        coordinator.reset();

        assert_eq!(coordinator.total_count(), 0);
        assert_eq!(coordinator.completed_count(), 0);
        assert!(coordinator.is_complete());
        assert!(!coordinator.has_error());
        assert!(coordinator.get_error().is_none());
    }
}
