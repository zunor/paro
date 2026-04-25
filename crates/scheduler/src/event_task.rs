// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Wraps a [`Task`] and notifies the bound [`Event`] when the inner task finishes successfully.

use crate::event::Event;
use crate::task::{ProducerToken, Task, TaskExecutionMode, TaskExecutionResult};
use parking_lot::Mutex;
use paro_common::error::Result;
use std::sync::Arc;

/// EventTask wraps any Task and automatically notifies the Event when complete.
///
/// When the inner task finishes (returns `TaskExecutionResult::Finished`),
/// EventTask automatically calls `event.finish_task()` to notify the event.
///
/// # Example
/// ```ignore
/// let event = Event::new();
/// let my_task = MyTask::new();
/// let event_task = EventTask::new(my_task, event.clone());
/// // When event_task.execute() returns Finished, event.finish_task() is called
/// ```
pub struct EventTask<T: Task> {
    /// The wrapped inner task
    inner: T,
    /// The event this task belongs to
    event: Arc<Event>,
    /// Whether this task has called finish_task on the event
    event_notified: bool,
    /// Producer token for rescheduling
    token: Option<ProducerToken>,
}

impl<T: Task> EventTask<T> {
    /// Create a new EventTask wrapping an inner task and bound to an event.
    pub fn new(inner: T, event: Arc<Event>) -> Self {
        Self {
            inner,
            event,
            event_notified: false,
            token: None,
        }
    }

    /// Get a reference to the inner task.
    pub fn inner(&self) -> &T {
        &self.inner
    }

    /// Get a mutable reference to the inner task.
    pub fn inner_mut(&mut self) -> &mut T {
        &mut self.inner
    }

    /// Get a reference to the event.
    pub fn event(&self) -> &Arc<Event> {
        &self.event
    }
}

impl<T: Task> Task for EventTask<T> {
    fn execute(&mut self, mode: TaskExecutionMode) -> Result<TaskExecutionResult> {
        // Execute the inner task
        let result = self.inner.execute(mode)?;

        // If the task finished, notify the event (only once)
        if result == TaskExecutionResult::Finished && !self.event_notified {
            self.inner.clear_interrupt_state();
            self.event_notified = true;
            self.event.finish_task();
        }

        Ok(result)
    }

    fn set_token(&mut self, token: ProducerToken) {
        self.token = Some(token.clone());
        self.inner.set_token(token);
    }

    fn set_interrupt_state(&mut self, interrupt_state: crate::task::InterruptState) {
        self.inner.set_interrupt_state(interrupt_state);
    }

    fn clear_interrupt_state(&mut self) {
        self.inner.clear_interrupt_state();
    }

    fn get_token(&self) -> Option<ProducerToken> {
        self.token.clone().or_else(|| self.inner.get_token())
    }

    fn deschedule(&mut self) -> Result<()> {
        self.inner.deschedule()
    }

    fn reschedule(&mut self) -> Result<()> {
        self.inner.reschedule()
    }

    fn task_blocked_on_result(&self) -> bool {
        self.inner.task_blocked_on_result()
    }

    fn task_type(&self) -> &str {
        self.inner.task_type()
    }
}

/// Boxed EventTask for use with dyn Task.
///
/// This allows wrapping any `dyn Task` with event notification.
pub struct BoxedEventTask {
    /// The wrapped inner task (boxed)
    inner: Box<dyn Task>,
    /// The event this task belongs to
    event: Arc<Event>,
    /// Whether this task has called finish_task on the event
    event_notified: bool,
    /// Producer token for rescheduling
    token: Option<ProducerToken>,
}

impl BoxedEventTask {
    /// Create a new BoxedEventTask wrapping a boxed task and bound to an event.
    pub fn new(inner: Box<dyn Task>, event: Arc<Event>) -> Self {
        Self {
            inner,
            event,
            event_notified: false,
            token: None,
        }
    }

    /// Create from an Arc<Mutex<dyn Task>> (extracts and wraps).
    ///
    /// Note: This consumes the Arc and returns a new BoxedEventTask.
    /// The original task is moved into the BoxedEventTask.
    pub fn from_arc_mutex(task: Arc<Mutex<dyn Task>>, event: Arc<Event>) -> Arc<Mutex<dyn Task>> {
        let inner_task = task.clone();
        let wrapped: Arc<Mutex<dyn Task>> = Arc::new(Mutex::new(BoxedEventTask {
            inner: Box::new(ArcMutexTaskWrapper { task }),
            event,
            event_notified: false,
            token: None,
        }));

        let callback_task = wrapped.clone();
        let callback = Arc::new(move || {
            let mut task_lock = callback_task.lock();
            task_lock.reschedule()?;
            if let Some(token) = task_lock.get_token() {
                drop(task_lock);
                token.schedule_task(callback_task.clone());
            }
            Ok(())
        });
        inner_task
            .lock()
            .set_interrupt_state(crate::task::InterruptState::with_callback(callback));

        wrapped
    }

    /// Get a reference to the event.
    pub fn event(&self) -> &Arc<Event> {
        &self.event
    }
}

impl Task for BoxedEventTask {
    fn execute(&mut self, mode: TaskExecutionMode) -> Result<TaskExecutionResult> {
        let result = self.inner.execute(mode)?;

        if result == TaskExecutionResult::Finished && !self.event_notified {
            self.inner.clear_interrupt_state();
            self.event_notified = true;
            self.event.finish_task();
        }

        Ok(result)
    }

    fn set_token(&mut self, token: ProducerToken) {
        self.token = Some(token.clone());
        self.inner.set_token(token);
    }

    fn set_interrupt_state(&mut self, interrupt_state: crate::task::InterruptState) {
        self.inner.set_interrupt_state(interrupt_state);
    }

    fn clear_interrupt_state(&mut self) {
        self.inner.clear_interrupt_state();
    }

    fn get_token(&self) -> Option<ProducerToken> {
        self.token.clone().or_else(|| self.inner.get_token())
    }

    fn deschedule(&mut self) -> Result<()> {
        self.inner.deschedule()
    }

    fn reschedule(&mut self) -> Result<()> {
        self.inner.reschedule()
    }

    fn task_blocked_on_result(&self) -> bool {
        self.inner.task_blocked_on_result()
    }

    fn task_type(&self) -> &str {
        self.inner.task_type()
    }
}

/// Wrapper to use Arc<Mutex<dyn Task>> as a Task.
struct ArcMutexTaskWrapper {
    task: Arc<Mutex<dyn Task>>,
}

impl Task for ArcMutexTaskWrapper {
    fn execute(&mut self, mode: TaskExecutionMode) -> Result<TaskExecutionResult> {
        self.task.lock().execute(mode)
    }

    fn set_token(&mut self, token: ProducerToken) {
        self.task.lock().set_token(token);
    }

    fn set_interrupt_state(&mut self, interrupt_state: crate::task::InterruptState) {
        self.task.lock().set_interrupt_state(interrupt_state);
    }

    fn clear_interrupt_state(&mut self) {
        self.task.lock().clear_interrupt_state();
    }

    fn get_token(&self) -> Option<ProducerToken> {
        self.task.lock().get_token()
    }

    fn deschedule(&mut self) -> Result<()> {
        self.task.lock().deschedule()
    }

    fn reschedule(&mut self) -> Result<()> {
        self.task.lock().reschedule()
    }

    fn task_blocked_on_result(&self) -> bool {
        self.task.lock().task_blocked_on_result()
    }

    fn task_type(&self) -> &str {
        // We can't easily return &str from locked mutex, so use a default
        "WrappedTask"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use parking_lot::Mutex as ParkingMutex;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct CountingTask {
        counter: Arc<AtomicUsize>,
        target: usize,
        current: usize,
    }

    impl Task for CountingTask {
        fn execute(&mut self, mode: TaskExecutionMode) -> Result<TaskExecutionResult> {
            match mode {
                TaskExecutionMode::ProcessAll => {
                    self.counter
                        .fetch_add(self.target - self.current, Ordering::SeqCst);
                    self.current = self.target;
                    Ok(TaskExecutionResult::Finished)
                }
                TaskExecutionMode::ProcessPartial => {
                    self.counter.fetch_add(1, Ordering::SeqCst);
                    self.current += 1;
                    if self.current >= self.target {
                        Ok(TaskExecutionResult::Finished)
                    } else {
                        Ok(TaskExecutionResult::NotFinished)
                    }
                }
            }
        }

        fn task_type(&self) -> &str {
            "CountingTask"
        }
    }

    #[test]
    fn test_event_task_finishes_event() {
        let event = Event::new();
        let counter = Arc::new(AtomicUsize::new(0));

        let task = CountingTask {
            counter: counter.clone(),
            target: 5,
            current: 0,
        };

        event.set_tasks(1);
        let mut event_task = EventTask::new(task, event.clone());

        assert!(!event.is_finished());

        let result = event_task.execute(TaskExecutionMode::ProcessAll).unwrap();
        assert_eq!(result, TaskExecutionResult::Finished);
        assert_eq!(counter.load(Ordering::SeqCst), 5);

        // Event should now be finished
        assert!(event.is_finished());
    }

    #[test]
    fn test_event_task_partial_execution() {
        let event = Event::new();
        let counter = Arc::new(AtomicUsize::new(0));

        let task = CountingTask {
            counter: counter.clone(),
            target: 3,
            current: 0,
        };

        event.set_tasks(1);
        let mut event_task = EventTask::new(task, event.clone());

        // First partial execution
        let r1 = event_task
            .execute(TaskExecutionMode::ProcessPartial)
            .unwrap();
        assert_eq!(r1, TaskExecutionResult::NotFinished);
        assert!(!event.is_finished());

        // Second partial execution
        let r2 = event_task
            .execute(TaskExecutionMode::ProcessPartial)
            .unwrap();
        assert_eq!(r2, TaskExecutionResult::NotFinished);
        assert!(!event.is_finished());

        // Third partial execution - finishes
        let r3 = event_task
            .execute(TaskExecutionMode::ProcessPartial)
            .unwrap();
        assert_eq!(r3, TaskExecutionResult::Finished);
        assert!(event.is_finished());
    }

    #[test]
    fn test_event_with_multiple_tasks() {
        let event = Event::new();
        let counter = Arc::new(AtomicUsize::new(0));

        event.set_tasks(3);

        for i in 0..3 {
            let task = CountingTask {
                counter: counter.clone(),
                target: 1,
                current: 0,
            };
            let mut event_task = EventTask::new(task, event.clone());

            if i < 2 {
                let result = event_task.execute(TaskExecutionMode::ProcessAll).unwrap();
                assert_eq!(result, TaskExecutionResult::Finished);
                assert!(
                    !event.is_finished(),
                    "Event should not be finished after {} tasks",
                    i + 1
                );
            } else {
                let result = event_task.execute(TaskExecutionMode::ProcessAll).unwrap();
                assert_eq!(result, TaskExecutionResult::Finished);
                assert!(
                    event.is_finished(),
                    "Event should be finished after all tasks"
                );
            }
        }

        assert_eq!(counter.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn test_boxed_event_task() {
        let event = Event::new();
        let counter = Arc::new(AtomicUsize::new(0));

        let task: Box<dyn Task> = Box::new(CountingTask {
            counter: counter.clone(),
            target: 5,
            current: 0,
        });

        event.set_tasks(1);
        let mut boxed_task = BoxedEventTask::new(task, event.clone());

        assert!(!event.is_finished());

        let result = boxed_task.execute(TaskExecutionMode::ProcessAll).unwrap();
        assert_eq!(result, TaskExecutionResult::Finished);
        assert!(event.is_finished());
    }

    #[test]
    fn test_event_task_only_notifies_once() {
        let event = Event::new();

        struct AlwaysFinishedTask;
        impl Task for AlwaysFinishedTask {
            fn execute(&mut self, _mode: TaskExecutionMode) -> Result<TaskExecutionResult> {
                Ok(TaskExecutionResult::Finished)
            }
            fn task_type(&self) -> &str {
                "AlwaysFinishedTask"
            }
        }

        event.set_tasks(2); // Expect 2 tasks
        let mut task1 = EventTask::new(AlwaysFinishedTask, event.clone());

        // First task finishes
        task1.execute(TaskExecutionMode::ProcessAll).unwrap();
        assert!(!event.is_finished()); // Still waiting for second task
        assert_eq!(event.finished_task_count(), 1);

        // Execute again - should not increment finished count
        task1.execute(TaskExecutionMode::ProcessAll).unwrap();
        assert!(!event.is_finished());
        assert_eq!(event.finished_task_count(), 1);

        // Second task finishes
        let mut task2 = EventTask::new(AlwaysFinishedTask, event.clone());
        task2.execute(TaskExecutionMode::ProcessAll).unwrap();
        assert!(event.is_finished());
        assert_eq!(event.finished_task_count(), 2);
    }

    #[test]
    fn boxed_event_task_reschedules_the_wrapped_task_via_interrupt_state() {
        struct InterruptAwareBlockingTask {
            state: Arc<ParkingMutex<Option<crate::task::InterruptState>>>,
            token: Option<ProducerToken>,
            blocked_once: bool,
        }

        impl Task for InterruptAwareBlockingTask {
            fn execute(&mut self, _mode: TaskExecutionMode) -> Result<TaskExecutionResult> {
                if !self.blocked_once {
                    self.blocked_once = true;
                    Ok(TaskExecutionResult::Blocked)
                } else {
                    Ok(TaskExecutionResult::Finished)
                }
            }

            fn set_token(&mut self, token: ProducerToken) {
                self.token = Some(token);
            }

            fn get_token(&self) -> Option<ProducerToken> {
                self.token.clone()
            }

            fn set_interrupt_state(&mut self, interrupt_state: crate::task::InterruptState) {
                *self.state.lock() = Some(interrupt_state);
            }

            fn deschedule(&mut self) -> Result<()> {
                Ok(())
            }

            fn reschedule(&mut self) -> Result<()> {
                Ok(())
            }

            fn task_type(&self) -> &str {
                "InterruptAwareBlockingTask"
            }
        }

        let event = Event::new();
        event.set_tasks(1);

        let scheduler = Arc::new(crate::scheduler::TaskScheduler::new());
        let token = scheduler.create_producer();
        let interrupt_state = Arc::new(ParkingMutex::new(None));
        let inner: Arc<Mutex<dyn Task>> = Arc::new(Mutex::new(InterruptAwareBlockingTask {
            state: interrupt_state.clone(),
            token: None,
            blocked_once: false,
        }));
        let wrapped = BoxedEventTask::from_arc_mutex(inner, event.clone());

        scheduler.schedule_task_with_token(&token, wrapped);
        let marker = AtomicBool::new(true);

        scheduler.execute_tasks_for_producer(&token, &marker, 1);
        assert!(!event.is_finished());

        let callback = interrupt_state
            .lock()
            .take()
            .expect("wrapped task should receive a scheduler-linked interrupt state");
        callback
            .callback()
            .expect("interrupt callback should reschedule wrapped task");

        scheduler.execute_tasks_for_producer(&token, &marker, 1);
        assert!(event.is_finished());
    }

    #[test]
    fn boxed_event_task_interrupt_callback_does_not_keep_wrapped_task_alive() {
        struct InterruptAwareFinishedTask {
            state: Arc<ParkingMutex<Option<crate::task::InterruptState>>>,
        }
        impl Task for InterruptAwareFinishedTask {
            fn execute(&mut self, _mode: TaskExecutionMode) -> Result<TaskExecutionResult> {
                Ok(TaskExecutionResult::Finished)
            }

            fn set_interrupt_state(&mut self, interrupt_state: crate::task::InterruptState) {
                *self.state.lock() = Some(interrupt_state);
            }

            fn clear_interrupt_state(&mut self) {
                *self.state.lock() = None;
            }

            fn task_type(&self) -> &str {
                "InterruptAwareFinishedTask"
            }
        }

        let event = Event::new();
        event.set_tasks(1);
        let interrupt_state = Arc::new(ParkingMutex::new(None));
        let inner: Arc<Mutex<dyn Task>> = Arc::new(Mutex::new(InterruptAwareFinishedTask {
            state: interrupt_state.clone(),
        }));
        let wrapped = BoxedEventTask::from_arc_mutex(inner.clone(), event);
        let weak_wrapped = Arc::downgrade(&wrapped);

        assert!(
            interrupt_state.lock().is_some(),
            "from_arc_mutex should inject a scheduler-linked interrupt state"
        );
        wrapped
            .lock()
            .execute(TaskExecutionMode::ProcessAll)
            .expect("finished task should execute");
        assert!(
            interrupt_state.lock().is_none(),
            "finished task should clear its interrupt state"
        );

        drop(wrapped);
        drop(inner);

        assert!(
            weak_wrapped.upgrade().is_none(),
            "interrupt callback must not create a strong reference cycle"
        );
    }
}
