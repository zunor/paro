// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! [`Task`] trait, producer tokens, and interrupt / blocking helpers.

use parking_lot::{Condvar, Mutex as ParkingMutex};
use paro_common::error as paro_error;
use paro_common::error::Result;
use std::fmt;
use std::sync::{Arc, Weak};

/// ProducerToken is used to schedule tasks into the scheduler.
#[derive(Clone)]
pub struct ProducerToken {
    pub(crate) scheduler: Arc<crate::scheduler::TaskScheduler>,
    pub(crate) producer_id: usize,
    pub(crate) priority: i32,
}

impl ProducerToken {
    /// Get the producer id associated with this token.
    pub fn producer_id(&self) -> usize {
        self.producer_id
    }

    /// Get the scheduling priority associated with this token.
    pub fn priority(&self) -> i32 {
        self.priority
    }

    /// Schedule a single task.
    pub fn schedule_task(&self, task: Arc<parking_lot::Mutex<dyn Task>>) {
        self.scheduler.schedule_task_with_token(self, task);
    }

    /// Schedule multiple tasks.
    pub fn schedule_tasks(&self, tasks: Vec<Arc<parking_lot::Mutex<dyn Task>>>) {
        self.scheduler.schedule_tasks_with_token(self, tasks);
    }
}

/// InterruptMode specifies how operators should block/unblock.
///
/// This will happen transparently to the operator, as the operator only needs to return
/// a BLOCKED result and call the callback using the InterruptState.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InterruptMode {
    /// No blocking mode is specified, an error will be thrown when the operator blocks.
    /// Should only be used when manually calling operators of which is known they will never block.
    NoInterrupts,
    /// A weak pointer to a task is provided. On the callback, this task will be rescheduled.
    /// If the Task has been deleted, this callback becomes a NOP.
    /// This is the preferred way to await blocked pipelines.
    Task,
    /// The caller has blocked awaiting some synchronization primitive to wait for the callback.
    /// Used for code paths without Task.
    Blocking,
    /// A callback closure is provided and owns the reschedule logic.
    Callback,
}

/// Synchronization primitive used to await a callback in InterruptMode::Blocking.
#[derive(Clone)]
pub struct InterruptDoneSignalState {
    inner: Arc<InterruptDoneSignalStateInner>,
}

struct InterruptDoneSignalStateInner {
    lock: ParkingMutex<bool>,
    cv: Condvar,
}

impl InterruptDoneSignalState {
    /// Create a new signal state.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(InterruptDoneSignalStateInner {
                lock: ParkingMutex::new(false),
                cv: Condvar::new(),
            }),
        }
    }

    /// Called by the callback to signal the interrupt is over.
    pub fn signal(&self) {
        let mut done = self.inner.lock.lock();
        *done = true;
        self.inner.cv.notify_all();
    }

    /// Await the callback signalling the interrupt is over.
    pub fn await_signal(&self) {
        let mut done = self.inner.lock.lock();
        while !*done {
            self.inner.cv.wait(&mut done);
        }
        // Reset after signal received
        *done = false;
    }

    /// Create a weak reference to this signal state.
    pub fn downgrade(&self) -> WeakInterruptDoneSignalState {
        WeakInterruptDoneSignalState {
            inner: Arc::downgrade(&self.inner),
        }
    }
}

impl Default for InterruptDoneSignalState {
    fn default() -> Self {
        Self::new()
    }
}

/// Weak reference to InterruptDoneSignalState.
#[derive(Clone)]
pub struct WeakInterruptDoneSignalState {
    inner: Weak<InterruptDoneSignalStateInner>,
}

impl WeakInterruptDoneSignalState {
    /// Upgrade to a strong reference if the signal state still exists.
    pub fn upgrade(&self) -> Option<InterruptDoneSignalState> {
        self.inner
            .upgrade()
            .map(|inner| InterruptDoneSignalState { inner })
    }
}

/// State required to make the callback after some asynchronous operation.
///
/// This is used within operator sources/sinks to handle blocking operations.
#[derive(Clone)]
pub struct InterruptState {
    mode: InterruptMode,
    task: Option<Weak<parking_lot::Mutex<dyn Task>>>,
    signal_state: Option<WeakInterruptDoneSignalState>,
    callback: Option<Arc<dyn Fn() -> Result<()> + Send + Sync>>,
}

impl InterruptState {
    /// Create a new default interrupt state (NoInterrupts).
    pub fn new() -> Self {
        Self {
            mode: InterruptMode::NoInterrupts,
            task: None,
            signal_state: None,
            callback: None,
        }
    }

    /// Create a new interrupt state with a task (preferred mode).
    pub fn with_task(task: Weak<parking_lot::Mutex<dyn Task>>) -> Self {
        Self {
            mode: InterruptMode::Task,
            task: Some(task),
            signal_state: None,
            callback: None,
        }
    }

    /// Create a new interrupt state with a signal state (blocking mode).
    pub fn with_signal(signal_state: WeakInterruptDoneSignalState) -> Self {
        Self {
            mode: InterruptMode::Blocking,
            task: None,
            signal_state: Some(signal_state),
            callback: None,
        }
    }

    /// Create a new interrupt state with a callback closure.
    pub fn with_callback(callback: Arc<dyn Fn() -> Result<()> + Send + Sync>) -> Self {
        Self {
            mode: InterruptMode::Callback,
            task: None,
            signal_state: None,
            callback: Some(callback),
        }
    }

    /// Perform the callback to indicate the interrupt is over.
    pub fn callback(&self) -> Result<()> {
        match self.mode {
            InterruptMode::NoInterrupts => Err(paro_error::internal(
                "Callback made on InterruptState without valid interrupt mode specified",
            )),
            InterruptMode::Task => {
                if let Some(weak_task) = &self.task {
                    if let Some(task) = weak_task.upgrade() {
                        let mut task_lock = task.lock();
                        // First notify the task it's been rescheduled
                        task_lock.reschedule()?;

                        // Then actually schedule it if we have a token
                        if let Some(token) = task_lock.get_token() {
                            drop(task_lock); // Drop before scheduling to avoid nested locks if needed
                            token.schedule_task(task);
                        }
                    }
                }
                Ok(())
            }
            InterruptMode::Blocking => {
                if let Some(weak_signal) = &self.signal_state {
                    if let Some(signal) = weak_signal.upgrade() {
                        signal.signal();
                    }
                }
                Ok(())
            }
            InterruptMode::Callback => self
                .callback
                .as_ref()
                .ok_or_else(|| paro_error::internal("Interrupt callback closure missing"))?(
            ),
        }
    }
}

impl Default for InterruptState {
    fn default() -> Self {
        Self::new()
    }
}

/// Container for managing blocked tasks in operators.
///
/// This is used by operators (sources/sinks) that may need to block and later unblock tasks.
/// Operators can add tasks to the blocked list, and later unblock all of them at once.
pub struct StateWithBlockableTasks {
    /// Whether we can block tasks
    can_block: parking_lot::Mutex<bool>,
    /// Tasks that are currently blocked
    blocked_tasks: parking_lot::Mutex<Vec<InterruptState>>,
}

impl StateWithBlockableTasks {
    /// Create a new state with blockable tasks.
    pub fn new() -> Self {
        Self {
            can_block: parking_lot::Mutex::new(true),
            blocked_tasks: parking_lot::Mutex::new(Vec::new()),
        }
    }

    /// Prevent any further blocking of tasks.
    pub fn prevent_blocking(&self) {
        *self.can_block.lock() = false;
    }

    /// Check if tasks can be blocked.
    pub fn can_block(&self) -> bool {
        *self.can_block.lock()
    }

    /// Add a task to 'blocked_tasks' before returning BLOCKED result.
    ///
    /// Returns true if the task was blocked, false if blocking is prevented.
    pub fn block_task(&self, interrupt_state: InterruptState) -> bool {
        if *self.can_block.lock() {
            self.blocked_tasks.lock().push(interrupt_state);
            true
        } else {
            false
        }
    }

    /// Unblock all tasks by calling their callbacks.
    ///
    /// Returns true if any tasks were unblocked, false if the list was empty.
    pub fn unblock_tasks(&self) -> bool {
        let mut blocked = self.blocked_tasks.lock();
        if blocked.is_empty() {
            return false;
        }
        for entry in blocked.drain(..) {
            let _ = entry.callback(); // Ignore errors during callback
        }
        true
    }

    /// Get the number of currently blocked tasks.
    pub fn blocked_count(&self) -> usize {
        self.blocked_tasks.lock().len()
    }
}

impl Default for StateWithBlockableTasks {
    fn default() -> Self {
        Self::new()
    }
}

/// TaskExecutionMode determines how much work a task should perform in one call to Execute.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskExecutionMode {
    /// Finish all work before returning.
    ProcessAll,
    /// Process a partial amount of work and potentially return NotFinished.
    ProcessPartial,
}

/// TaskExecutionResult represents the result of executing a task.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskExecutionResult {
    /// The task has finished all its work.
    Finished,
    /// The task has more work to do (only possible in ProcessPartial mode).
    NotFinished,
    /// The task hit an error.
    Error,
    /// The task is blocked and should be descheduled.
    Blocked,
}

/// Generic parallel task trait.
///
/// Tasks are the unit of work scheduled by TaskScheduler.
/// Each task can be executed in ProcessAll or ProcessPartial mode.
pub trait Task: Send + Sync {
    /// Execute the task.
    /// Returns the result of the execution.
    fn execute(&mut self, mode: TaskExecutionMode) -> Result<TaskExecutionResult>;

    /// Set the producer token for the task.
    fn set_token(&mut self, _token: ProducerToken) {}

    /// Inject the interrupt state that should be used when this task blocks.
    fn set_interrupt_state(&mut self, _interrupt_state: InterruptState) {}

    /// Get the producer token for the task.
    fn get_token(&self) -> Option<ProducerToken> {
        None
    }

    /// Deschedule the task (called when task is blocked).
    fn deschedule(&mut self) -> Result<()> {
        Err(paro_error::internal(
            "Cannot deschedule task of base Task class",
        ))
    }

    /// Reschedule the task (called when blocking condition is resolved).
    fn reschedule(&mut self) -> Result<()> {
        Err(paro_error::internal(
            "Cannot reschedule task of base Task class",
        ))
    }

    /// Check if the task is blocked on a result.
    fn task_blocked_on_result(&self) -> bool {
        false
    }

    /// Get the type of the task for debugging.
    fn task_type(&self) -> &str {
        "UnnamedTask"
    }
}

impl fmt::Debug for dyn Task {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Task({})", self.task_type())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct CountingTask {
        counter: usize,
        target: usize,
    }

    impl Task for CountingTask {
        fn execute(&mut self, mode: TaskExecutionMode) -> Result<TaskExecutionResult> {
            match mode {
                TaskExecutionMode::ProcessAll => {
                    self.counter = self.target;
                    Ok(TaskExecutionResult::Finished)
                }
                TaskExecutionMode::ProcessPartial => {
                    self.counter += 1;
                    if self.counter >= self.target {
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
    fn test_task_process_all() {
        let mut task = CountingTask {
            counter: 0,
            target: 10,
        };
        let result = task.execute(TaskExecutionMode::ProcessAll).unwrap();
        assert_eq!(result, TaskExecutionResult::Finished);
        assert_eq!(task.counter, 10);
    }

    #[test]
    fn test_task_process_partial() {
        let mut task = CountingTask {
            counter: 0,
            target: 3,
        };

        let r1 = task.execute(TaskExecutionMode::ProcessPartial).unwrap();
        assert_eq!(r1, TaskExecutionResult::NotFinished);
        assert_eq!(task.counter, 1);

        let r2 = task.execute(TaskExecutionMode::ProcessPartial).unwrap();
        assert_eq!(r2, TaskExecutionResult::NotFinished);
        assert_eq!(task.counter, 2);

        let r3 = task.execute(TaskExecutionMode::ProcessPartial).unwrap();
        assert_eq!(r3, TaskExecutionResult::Finished);
        assert_eq!(task.counter, 3);
    }

    #[test]
    fn test_task_type() {
        let task = CountingTask {
            counter: 0,
            target: 1,
        };
        assert_eq!(task.task_type(), "CountingTask");
    }

    #[test]
    fn test_deschedule_error() {
        let mut task = CountingTask {
            counter: 0,
            target: 1,
        };
        assert!(task.deschedule().is_err());
    }

    struct BlockableTask {
        blocked: bool,
        finished: bool,
        token: Option<ProducerToken>,
    }

    impl Task for BlockableTask {
        fn execute(&mut self, _mode: TaskExecutionMode) -> Result<TaskExecutionResult> {
            if self.finished {
                return Ok(TaskExecutionResult::Finished);
            }
            if self.blocked {
                self.finished = true;
                Ok(TaskExecutionResult::Finished)
            } else {
                self.blocked = true;
                Ok(TaskExecutionResult::Blocked)
            }
        }
        fn set_token(&mut self, token: ProducerToken) {
            self.token = Some(token);
        }
        fn get_token(&self) -> Option<ProducerToken> {
            self.token.clone()
        }
        fn deschedule(&mut self) -> Result<()> {
            Ok(())
        }
        fn reschedule(&mut self) -> Result<()> {
            Ok(())
        }
    }

    #[test]
    fn test_interrupt_state_callback() {
        let task: Arc<parking_lot::Mutex<dyn Task>> =
            Arc::new(parking_lot::Mutex::new(BlockableTask {
                blocked: false,
                finished: false,
                token: None,
            }));

        // Mock a scheduler and token
        let scheduler = Arc::new(crate::scheduler::TaskScheduler::new());
        let token = scheduler.create_producer();
        task.lock().set_token(token);

        let interrupt_state = InterruptState::with_task(Arc::downgrade(&task));

        // Initial execution: returns Blocked
        let result = task.lock().execute(TaskExecutionMode::ProcessAll).unwrap();
        assert_eq!(result, TaskExecutionResult::Blocked);

        // Callback should reschedule it
        interrupt_state.callback().unwrap();

        // Check scheduler has 1 task
        assert_eq!(scheduler.pending_tasks(), 1);

        // Dequeue and execute
        let dequeued_task = scheduler.shared_task_queue().try_dequeue().unwrap();
        let result = dequeued_task
            .lock()
            .execute(TaskExecutionMode::ProcessAll)
            .unwrap();
        assert_eq!(result, TaskExecutionResult::Finished);
    }

    #[test]
    fn test_interrupt_done_signal_state() {
        let signal = InterruptDoneSignalState::new();
        let weak_signal = signal.downgrade();

        // Spawn a thread that will signal after a delay
        let signal_clone = signal.clone();
        let handle = std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(50));
            signal_clone.signal();
        });

        // Await the signal
        signal.await_signal();

        handle.join().unwrap();

        // Verify weak reference still works
        assert!(weak_signal.upgrade().is_some());
    }

    #[test]
    fn test_interrupt_state_blocking_mode() {
        let signal = InterruptDoneSignalState::new();
        let interrupt_state = InterruptState::with_signal(signal.downgrade());

        // Spawn a thread that will trigger the callback
        let interrupt_clone = interrupt_state.clone();
        let handle = std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(50));
            interrupt_clone.callback().unwrap();
        });

        // Await the signal
        signal.await_signal();

        handle.join().unwrap();
    }

    #[test]
    fn test_state_with_blockable_tasks() {
        let state = StateWithBlockableTasks::new();
        assert!(state.can_block());
        assert_eq!(state.blocked_count(), 0);

        // Create some interrupt states
        let signal1 = InterruptDoneSignalState::new();
        let signal2 = InterruptDoneSignalState::new();

        let int1 = InterruptState::with_signal(signal1.downgrade());
        let int2 = InterruptState::with_signal(signal2.downgrade());

        // Block tasks
        assert!(state.block_task(int1));
        assert!(state.block_task(int2));
        assert_eq!(state.blocked_count(), 2);

        // Unblock all
        assert!(state.unblock_tasks());
        assert_eq!(state.blocked_count(), 0);

        // Verify signals were triggered
        // (In real usage, the signals would have been awaited by other threads)
    }

    #[test]
    fn test_state_prevent_blocking() {
        let state = StateWithBlockableTasks::new();

        // Prevent blocking
        state.prevent_blocking();
        assert!(!state.can_block());

        // Try to block a task - should fail
        let signal = InterruptDoneSignalState::new();
        let int_state = InterruptState::with_signal(signal.downgrade());
        assert!(!state.block_task(int_state));
        assert_eq!(state.blocked_count(), 0);
    }

    #[test]
    fn test_interrupt_mode_variants() {
        // Test NoInterrupts mode
        let int1 = InterruptState::new();
        assert!(int1.callback().is_err());

        // Test Task mode with a task that has a token
        let scheduler = Arc::new(crate::scheduler::TaskScheduler::new());
        let token = scheduler.create_producer();
        let task: Arc<parking_lot::Mutex<dyn Task>> =
            Arc::new(parking_lot::Mutex::new(BlockableTask {
                blocked: false,
                finished: false,
                token: Some(token),
            }));
        let int2 = InterruptState::with_task(Arc::downgrade(&task));
        // Callback with token should work
        assert!(int2.callback().is_ok());

        // Test Blocking mode
        let signal = InterruptDoneSignalState::new();
        let int3 = InterruptState::with_signal(signal.downgrade());
        assert!(int3.callback().is_ok());
    }
}
