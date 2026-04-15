// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Worker thread pool and task execution loop.

use crate::error_manager::{TaskError, TaskErrorRegistry};
use crate::queue::ConcurrentTaskQueue;
use crate::task::{ProducerToken, Task, TaskExecutionMode, TaskExecutionResult};
use parking_lot::Mutex;
use paro_common::allocator::Allocator;
use paro_common::error as paro_error;
use paro_common::error::Result;
use std::panic;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex as StdMutex};
use std::thread;
use std::time::Duration;
#[cfg(target_os = "linux")]
use std::{mem, os::unix::thread::JoinHandleExt};

/// Default timeout for worker threads waiting for tasks (in milliseconds).
const TASK_TIMEOUT_MS: u64 = 100;

/// Initial wait time before flushing (in milliseconds).
const INITIAL_FLUSH_WAIT_MS: u64 = 500;

/// Default allocator flush threshold (128 KB).
const DEFAULT_ALLOCATOR_FLUSH_THRESHOLD: usize = 128 * 1024;

/// Wait time for task reschedule condition variable (in milliseconds).
const WAIT_FOR_TASK_MS: u64 = 1;

const THREAD_PIN_THRESHOLD: usize = 64;

/// Thread affinity mode for worker threads.
///
/// - `Off`: never pin worker threads.
/// - `On`: always attempt to pin worker threads.
/// - `Auto`: pin only on high-core-count systems (> 64 cores).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ThreadAffinityMode {
    Off,
    On,
    #[default]
    Auto,
}

impl ThreadAffinityMode {
    fn as_u8(self) -> u8 {
        match self {
            Self::Off => 0,
            Self::On => 1,
            Self::Auto => 2,
        }
    }

    fn from_u8(value: u8) -> Self {
        match value {
            0 => Self::Off,
            1 => Self::On,
            _ => Self::Auto,
        }
    }
}

fn requeue_task_with_existing_token(queue: &ConcurrentTaskQueue, task: Arc<Mutex<dyn Task>>) {
    let token = { task.lock().get_token() };
    if let Some(token) = token {
        queue.enqueue(token.producer_id(), token.priority(), task);
    } else {
        // Fallback for tasks that don't implement token propagation.
        queue.enqueue(0, 0, task);
    }
}

fn signal_task_rescheduled_raw(lock: &StdMutex<bool>, cv: &Condvar) {
    {
        let mut guard = lock.lock().unwrap();
        *guard = true;
    }
    cv.notify_one();
}

struct WorkerLoopContext {
    queue: Arc<ConcurrentTaskQueue>,
    shutdown: Arc<AtomicBool>,
    threshold: Arc<AtomicUsize>,
    bg_threads: Arc<AtomicBool>,
    thread_count: Arc<AtomicI32>,
    error_registry: Arc<TaskErrorRegistry>,
    task_reschedule_lock: Arc<StdMutex<bool>>,
    task_reschedule_cv: Arc<Condvar>,
}

/// TaskScheduler manages a pool of worker threads and a task queue.
///
/// # Example
/// ```ignore
/// let scheduler = Arc::new(TaskScheduler::new());
/// scheduler.set_threads(4)?;
///
/// let token = scheduler.create_producer();
/// token.schedule_task(Box::new(MyTask::new()));
/// ```
pub struct TaskScheduler {
    /// Shared task queue
    queue: Arc<ConcurrentTaskQueue>,
    /// Worker thread handles
    threads: Mutex<Vec<thread::JoinHandle<()>>>,
    /// Requested number of threads
    requested_thread_count: Arc<AtomicI32>,
    /// Current number of running threads
    current_thread_count: AtomicI32,
    /// Shutdown signal
    shutdown: Arc<AtomicBool>,
    /// Threshold (in bytes) for allocator flushing
    allocator_flush_threshold: Arc<AtomicUsize>,
    /// Whether allocator uses background threads for flushing
    allocator_background_threads: Arc<AtomicBool>,
    /// Monotonic id generator for producer tokens.
    next_producer_id: AtomicUsize,
    /// Global and producer-scoped task errors.
    error_registry: Arc<TaskErrorRegistry>,
    /// Condition variable for waiting on task reschedule
    task_reschedule_lock: Arc<StdMutex<bool>>,
    task_reschedule_cv: Arc<Condvar>,
    /// Thread affinity mode for worker threads.
    thread_affinity_mode: Arc<AtomicUsize>,
}

impl std::fmt::Debug for TaskScheduler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskScheduler")
            .field("queue_size", &self.queue.size())
            .field(
                "requested_threads",
                &self.requested_thread_count.load(Ordering::SeqCst),
            )
            .field(
                "current_threads",
                &self.current_thread_count.load(Ordering::SeqCst),
            )
            .finish()
    }
}

impl Default for TaskScheduler {
    fn default() -> Self {
        Self::new()
    }
}

impl TaskScheduler {
    /// Join all worker handles, skipping the current thread to avoid self-join panic.
    fn join_worker_handles(handles: &mut Vec<thread::JoinHandle<()>>) {
        let current_id = thread::current().id();
        while let Some(handle) = handles.pop() {
            if handle.thread().id() == current_id {
                // Dropping the current thread's JoinHandle detaches it. This avoids
                // EDEADLK ("Resource deadlock avoided") when shutdown is triggered
                // from a worker thread context.
                continue;
            }
            let _ = handle.join();
        }
    }

    /// Create a new TaskScheduler with no worker threads.
    pub fn new() -> Self {
        Self {
            queue: Arc::new(ConcurrentTaskQueue::new()),
            threads: Mutex::new(Vec::new()),
            requested_thread_count: Arc::new(AtomicI32::new(0)),
            current_thread_count: AtomicI32::new(0),
            shutdown: Arc::new(AtomicBool::new(false)),
            allocator_flush_threshold: Arc::new(AtomicUsize::new(
                DEFAULT_ALLOCATOR_FLUSH_THRESHOLD,
            )),
            allocator_background_threads: Arc::new(AtomicBool::new(false)),
            next_producer_id: AtomicUsize::new(1),
            error_registry: Arc::new(TaskErrorRegistry::new()),
            task_reschedule_lock: Arc::new(StdMutex::new(false)),
            task_reschedule_cv: Arc::new(Condvar::new()),
            thread_affinity_mode: Arc::new(AtomicUsize::new(
                ThreadAffinityMode::default().as_u8() as usize
            )),
        }
    }

    /// Create a producer token for submitting tasks.
    pub fn create_producer(self: &Arc<Self>) -> ProducerToken {
        self.create_producer_with_priority(0)
    }

    /// Create a producer token with a custom priority.
    ///
    /// Higher values mean higher scheduling priority.
    pub fn create_producer_with_priority(self: &Arc<Self>, priority: i32) -> ProducerToken {
        let producer_id = self.next_producer_id.fetch_add(1, Ordering::SeqCst);
        ProducerToken {
            scheduler: self.clone(),
            producer_id,
            priority,
        }
    }

    /// Set the number of worker threads.
    ///
    /// If reducing threads, existing threads will be signaled to stop.
    /// If increasing threads, new threads will be spawned.
    pub fn set_threads(&self, total_threads: usize) -> Result<()> {
        let n = total_threads as i32;
        self.requested_thread_count.store(n, Ordering::SeqCst);
        self.relaunch_threads(total_threads)
    }

    /// Set worker thread affinity mode.
    ///
    /// Existing workers are relaunched to apply the new affinity policy.
    pub fn set_thread_affinity_mode(&self, mode: ThreadAffinityMode) -> Result<()> {
        self.thread_affinity_mode
            .store(mode.as_u8() as usize, Ordering::SeqCst);

        let requested = self.requested_thread_count.load(Ordering::SeqCst).max(0) as usize;
        let current = self.number_of_threads().max(0) as usize;
        if requested > 0 && current == requested {
            // Relaunch current workers so the new affinity policy takes effect.
            self.relaunch_threads(0)?;
        }
        self.relaunch_threads(requested)
    }

    /// Get current worker thread affinity mode.
    pub fn thread_affinity_mode(&self) -> ThreadAffinityMode {
        let mode = self.thread_affinity_mode.load(Ordering::SeqCst) as u8;
        ThreadAffinityMode::from_u8(mode)
    }

    /// Get the current number of worker threads.
    pub fn number_of_threads(&self) -> i32 {
        self.current_thread_count.load(Ordering::SeqCst)
    }

    /// Get the number of pending tasks in the queue.
    pub fn pending_tasks(&self) -> usize {
        self.queue.size()
    }

    /// Get the number of pending tasks for a specific producer.
    pub fn pending_tasks_for_producer(&self, token: &ProducerToken) -> usize {
        self.queue.task_count_for_producer(token.producer_id())
    }

    /// Set the allocator flush threshold.
    pub fn set_allocator_flush_threshold(&self, threshold: usize) {
        self.allocator_flush_threshold
            .store(threshold, Ordering::SeqCst);
    }

    /// Set whether the allocator uses background threads for flushing.
    pub fn set_allocator_background_threads(&self, enable: bool) {
        self.allocator_background_threads
            .store(enable, Ordering::SeqCst);
    }

    fn has_unscoped_error(&self) -> bool {
        self.error_registry.has_global_error()
    }

    fn task_token(task: &Arc<Mutex<dyn Task>>) -> Option<ProducerToken> {
        task.lock().get_token()
    }

    fn record_task_error(&self, token: Option<&ProducerToken>, error: TaskError) {
        if let Some(token) = token {
            self.error_registry
                .push_producer_error(token.producer_id(), error);
        } else {
            self.error_registry.push_global_error(error);
        }
    }

    fn cancel_tasks_for_error(&self, token: Option<&ProducerToken>) {
        if let Some(token) = token {
            let _ = self.cancel_tasks_for_producer(token);
        } else {
            self.cancel_tasks();
        }
        self.signal_task_rescheduled();
    }

    /// Check if any task errors have occurred.
    pub fn has_error(&self) -> bool {
        self.error_registry.has_any_error()
    }

    /// Get the first error that occurred during task execution.
    pub fn get_error(&self) -> Option<TaskError> {
        self.error_registry.get_any_error()
    }

    /// Check whether a specific producer has an error.
    pub fn has_error_for_producer(&self, token: &ProducerToken) -> bool {
        self.error_registry
            .has_error_for_producer(token.producer_id())
    }

    /// Get the first error recorded for a producer.
    pub fn get_error_for_producer(&self, token: &ProducerToken) -> Option<TaskError> {
        self.error_registry
            .get_error_for_producer(token.producer_id())
    }

    /// Get all errors that occurred during task execution.
    pub fn get_all_errors(&self) -> Vec<TaskError> {
        self.error_registry.get_all_errors()
    }

    /// Reset tracked task errors, clearing global and producer-scoped state.
    pub fn reset_errors(&self) {
        self.error_registry.reset_all();
    }

    /// Reset errors for a specific producer.
    pub fn reset_errors_for_producer(&self, token: &ProducerToken) {
        self.error_registry.reset_producer(token.producer_id());
    }

    /// Cancel all pending tasks (used when an error occurs).
    ///
    /// This drains the task queue without executing the tasks.
    pub fn cancel_tasks(&self) {
        let _ = self.queue.drain_all();
    }

    /// Cancel pending tasks for a specific producer.
    ///
    /// Returns the number of cancelled tasks.
    pub fn cancel_tasks_for_producer(&self, token: &ProducerToken) -> usize {
        self.queue.cancel_producer(token.producer_id())
    }

    /// Try to fetch one task from a specific producer.
    pub fn get_task_from_producer(&self, token: &ProducerToken) -> Option<Arc<Mutex<dyn Task>>> {
        self.queue.try_dequeue_from_producer(token.producer_id())
    }

    /// Block until work may be available (queue non-empty, error posted, or wakeup), or timeout.
    pub fn wait_for_task(&self) -> bool {
        if self.queue.size() > 0 || self.has_error() {
            return true;
        }

        let mut guard = self.task_reschedule_lock.lock().unwrap();

        if *guard {
            *guard = false;
            return true;
        }

        let mut result = self
            .task_reschedule_cv
            .wait_timeout(guard, Duration::from_millis(WAIT_FOR_TASK_MS))
            .unwrap();

        let signaled = *result.0;
        if signaled {
            *result.0 = false;
        }

        signaled || !result.1.timed_out() || self.queue.size() > 0 || self.has_error()
    }

    /// Wait for a task to become ready for a specific producer.
    ///
    /// Returns `true` only when that producer has pending tasks.
    pub fn wait_for_task_for_producer(&self, token: &ProducerToken) -> bool {
        if self.pending_tasks_for_producer(token) > 0
            || self.has_error_for_producer(token)
            || self.has_unscoped_error()
        {
            return true;
        }

        let guard = self.task_reschedule_lock.lock().unwrap();
        let mut result = self
            .task_reschedule_cv
            .wait_timeout(guard, Duration::from_millis(WAIT_FOR_TASK_MS))
            .unwrap();

        if *result.0 {
            *result.0 = false;
        }

        self.pending_tasks_for_producer(token) > 0
            || self.has_error_for_producer(token)
            || self.has_unscoped_error()
    }

    /// Signal that a task has been rescheduled.
    ///
    /// This wakes up threads waiting in wait_for_task().
    /// Called by InterruptState::callback() when a blocked task is ready.
    pub fn signal_task_rescheduled(&self) {
        signal_task_rescheduled_raw(
            self.task_reschedule_lock.as_ref(),
            self.task_reschedule_cv.as_ref(),
        );
    }

    /// Shared task queue (tests that need direct dequeue access).
    #[cfg(test)]
    pub(crate) fn shared_task_queue(&self) -> &Arc<ConcurrentTaskQueue> {
        &self.queue
    }

    /// Schedule a single task for execution using a specific producer token.
    pub fn schedule_task_with_token(
        self: &Arc<Self>,
        token: &ProducerToken,
        task: Arc<Mutex<dyn Task>>,
    ) {
        task.lock().set_token(token.clone());
        self.queue
            .enqueue(token.producer_id(), token.priority(), task);
        self.signal_task_rescheduled();
    }

    /// Schedule multiple tasks for execution using a specific producer token.
    pub fn schedule_tasks_with_token(
        self: &Arc<Self>,
        token: &ProducerToken,
        tasks: Vec<Arc<Mutex<dyn Task>>>,
    ) {
        for task in &tasks {
            task.lock().set_token(token.clone());
        }
        self.queue
            .enqueue_bulk(token.producer_id(), token.priority(), tasks);
        self.signal_task_rescheduled();
    }

    /// Schedule a single task for execution.
    pub fn schedule_task(self: &Arc<Self>, task: Arc<Mutex<dyn Task>>) {
        let token = self.create_producer();
        self.schedule_task_with_token(&token, task);
    }

    /// Schedule multiple tasks for execution.
    pub fn schedule_tasks(self: &Arc<Self>, tasks: Vec<Arc<Mutex<dyn Task>>>) {
        let token = self.create_producer();
        self.schedule_tasks_with_token(&token, tasks);
    }

    /// Execute tasks on the current thread until marker is false or max_tasks reached.
    ///
    /// Returns the number of tasks completed.
    ///
    /// # Panic Safety
    /// This method catches panics from task execution and converts them to errors.
    pub fn execute_tasks(&self, marker: &AtomicBool, max_tasks: usize) -> usize {
        let mut completed = 0;
        let allocator = paro_common::allocator::default_allocator();

        while marker.load(Ordering::SeqCst) && completed < max_tasks {
            // Only unscoped/global errors should stop all execution.
            if self.has_unscoped_error() {
                break;
            }

            if let Some(task) = self.queue.try_dequeue() {
                let token = Self::task_token(&task);
                let result = panic::catch_unwind(panic::AssertUnwindSafe(|| {
                    let mut task_lock = task.lock();
                    task_lock.execute(TaskExecutionMode::ProcessAll)
                }));

                match result {
                    Ok(Ok(TaskExecutionResult::Finished)) | Ok(Ok(TaskExecutionResult::Error)) => {
                        completed += 1;
                    }
                    Ok(Ok(TaskExecutionResult::NotFinished)) => {
                        requeue_task_with_existing_token(self.queue.as_ref(), task);
                        completed += 1;
                    }
                    Ok(Ok(TaskExecutionResult::Blocked)) => {
                        let _ = task.lock().deschedule();
                        completed += 1;
                    }
                    Ok(Err(e)) => {
                        completed += 1;
                        self.record_task_error(token.as_ref(), TaskError::from_paro_error(e));
                        self.cancel_tasks_for_error(token.as_ref());
                        if token.is_none() {
                            break;
                        }
                    }
                    Err(panic_payload) => {
                        completed += 1;
                        self.record_task_error(
                            token.as_ref(),
                            TaskError::from_panic(panic_payload),
                        );
                        self.cancel_tasks_for_error(token.as_ref());
                        if token.is_none() {
                            break;
                        }
                    }
                }
            } else {
                if allocator.supports_flush() {
                    allocator.thread_flush(
                        self.allocator_background_threads.load(Ordering::SeqCst),
                        self.allocator_flush_threshold.load(Ordering::SeqCst),
                        self.requested_thread_count.load(Ordering::SeqCst) as usize,
                    );
                }
                allocator.thread_idle();
                break;
            }
        }
        completed
    }

    /// Execute tasks from a specific producer on the current thread.
    ///
    /// Returns the number of tasks completed.
    pub fn execute_tasks_for_producer(
        &self,
        token: &ProducerToken,
        marker: &AtomicBool,
        max_tasks: usize,
    ) -> usize {
        let mut completed = 0;
        let allocator = paro_common::allocator::default_allocator();

        while marker.load(Ordering::SeqCst) && completed < max_tasks {
            if self.has_unscoped_error() || self.has_error_for_producer(token) {
                break;
            }

            if let Some(task) = self.get_task_from_producer(token) {
                let result = panic::catch_unwind(panic::AssertUnwindSafe(|| {
                    let mut task_lock = task.lock();
                    task_lock.execute(TaskExecutionMode::ProcessAll)
                }));

                match result {
                    Ok(Ok(TaskExecutionResult::Finished)) | Ok(Ok(TaskExecutionResult::Error)) => {
                        completed += 1;
                    }
                    Ok(Ok(TaskExecutionResult::NotFinished)) => {
                        requeue_task_with_existing_token(self.queue.as_ref(), task);
                        completed += 1;
                    }
                    Ok(Ok(TaskExecutionResult::Blocked)) => {
                        let _ = task.lock().deschedule();
                        completed += 1;
                    }
                    Ok(Err(e)) => {
                        completed += 1;
                        self.record_task_error(Some(token), TaskError::from_paro_error(e));
                        self.cancel_tasks_for_error(Some(token));
                        break;
                    }
                    Err(panic_payload) => {
                        completed += 1;
                        self.record_task_error(Some(token), TaskError::from_panic(panic_payload));
                        self.cancel_tasks_for_error(Some(token));
                        break;
                    }
                }
            } else {
                if allocator.supports_flush() {
                    allocator.thread_flush(
                        self.allocator_background_threads.load(Ordering::SeqCst),
                        self.allocator_flush_threshold.load(Ordering::SeqCst),
                        self.requested_thread_count.load(Ordering::SeqCst) as usize,
                    );
                }
                allocator.thread_idle();
                break;
            }
        }
        completed
    }

    fn relaunch_threads(&self, n: usize) -> Result<()> {
        let mut threads = self.threads.lock();
        let current = threads.len();

        if current == n {
            return Ok(());
        }

        // If reducing threads, shutdown all and restart
        if current > n {
            self.shutdown.store(true, Ordering::SeqCst);
            self.queue.signal_all();
            Self::join_worker_handles(&mut threads);
            self.shutdown.store(false, Ordering::SeqCst);
            self.current_thread_count.store(0, Ordering::SeqCst);
        }

        // Spawn new threads
        let to_create = n - threads.len();
        let pin_threads = self.should_pin_threads();
        for _ in 0..to_create {
            let ctx = WorkerLoopContext {
                queue: self.queue.clone(),
                shutdown: self.shutdown.clone(),
                threshold: self.allocator_flush_threshold.clone(),
                bg_threads: self.allocator_background_threads.clone(),
                thread_count: self.requested_thread_count.clone(),
                error_registry: self.error_registry.clone(),
                task_reschedule_lock: self.task_reschedule_lock.clone(),
                task_reschedule_cv: self.task_reschedule_cv.clone(),
            };

            let thread_id = self.current_thread_count.fetch_add(1, Ordering::SeqCst) + 1;

            let handle = thread::Builder::new()
                .name(format!("paro-worker-{}", thread_id))
                .spawn(move || {
                    Self::worker_loop(ctx);
                })
                .map_err(|e| paro_error::internal(format!("Failed to spawn thread: {}", e)))?;

            if pin_threads {
                Self::set_thread_affinity(&handle, threads.len());
            }

            threads.push(handle);
        }

        Ok(())
    }

    fn worker_loop(ctx: WorkerLoopContext) {
        let WorkerLoopContext {
            queue,
            shutdown,
            threshold,
            bg_threads,
            thread_count,
            error_registry,
            task_reschedule_lock,
            task_reschedule_cv,
        } = ctx;
        let allocator = paro_common::allocator::default_allocator();

        while !shutdown.load(Ordering::SeqCst) {
            if error_registry.has_global_error() {
                break;
            }

            if queue.wait_for_task(Duration::from_millis(TASK_TIMEOUT_MS)) {
                if let Some(task) = queue.try_dequeue() {
                    let token = Self::task_token(&task);
                    let result = panic::catch_unwind(panic::AssertUnwindSafe(|| {
                        let mut task_lock = task.lock();
                        task_lock.execute(TaskExecutionMode::ProcessAll)
                    }));

                    match result {
                        Ok(Ok(TaskExecutionResult::Finished))
                        | Ok(Ok(TaskExecutionResult::Error)) => {}
                        Ok(Ok(TaskExecutionResult::NotFinished)) => {
                            requeue_task_with_existing_token(queue.as_ref(), task);
                        }
                        Ok(Ok(TaskExecutionResult::Blocked)) => {
                            let _ = task.lock().deschedule();
                        }
                        Ok(Err(e)) => {
                            let task_error = TaskError::from_paro_error(e);
                            if let Some(token) = token.as_ref() {
                                error_registry.push_producer_error(token.producer_id(), task_error);
                                let _ = queue.cancel_producer(token.producer_id());
                                signal_task_rescheduled_raw(
                                    task_reschedule_lock.as_ref(),
                                    task_reschedule_cv.as_ref(),
                                );
                            } else {
                                error_registry.push_global_error(task_error);
                                shutdown.store(true, Ordering::SeqCst);
                                queue.signal_all();
                                signal_task_rescheduled_raw(
                                    task_reschedule_lock.as_ref(),
                                    task_reschedule_cv.as_ref(),
                                );
                            }
                        }
                        Err(panic_payload) => {
                            let task_error = TaskError::from_panic(panic_payload);
                            if let Some(token) = token.as_ref() {
                                error_registry.push_producer_error(token.producer_id(), task_error);
                                let _ = queue.cancel_producer(token.producer_id());
                                signal_task_rescheduled_raw(
                                    task_reschedule_lock.as_ref(),
                                    task_reschedule_cv.as_ref(),
                                );
                            } else {
                                error_registry.push_global_error(task_error);
                                shutdown.store(true, Ordering::SeqCst);
                                queue.signal_all();
                                signal_task_rescheduled_raw(
                                    task_reschedule_lock.as_ref(),
                                    task_reschedule_cv.as_ref(),
                                );
                            }
                        }
                    }
                }
            } else {
                // Idle - allocator flushing
                if allocator.supports_flush() {
                    if !queue.wait_for_task(Duration::from_millis(INITIAL_FLUSH_WAIT_MS)) {
                        allocator.thread_flush(
                            bg_threads.load(Ordering::SeqCst),
                            threshold.load(Ordering::SeqCst),
                            thread_count.load(Ordering::SeqCst) as usize,
                        );

                        if let Some(decay) = allocator.decay_delay() {
                            let wait_ms = (decay as u64) * 1000;
                            if !queue.wait_for_task(Duration::from_millis(wait_ms)) {
                                allocator.thread_idle();
                            }
                        } else {
                            allocator.thread_idle();
                        }
                    }
                } else {
                    allocator.thread_idle();
                }
            }
        }

        // Thread exiting - final flush
        if allocator.supports_flush() {
            allocator.thread_flush(
                bg_threads.load(Ordering::SeqCst),
                0,
                thread_count.load(Ordering::SeqCst) as usize,
            );
            allocator.thread_idle();
        }
    }

    fn should_pin_threads(&self) -> bool {
        match self.thread_affinity_mode() {
            ThreadAffinityMode::Off => false,
            ThreadAffinityMode::On => true,
            ThreadAffinityMode::Auto => std::thread::available_parallelism()
                .map(|n| n.get() > THREAD_PIN_THRESHOLD)
                .unwrap_or(false),
        }
    }

    #[cfg(target_os = "linux")]
    fn set_thread_affinity(handle: &thread::JoinHandle<()>, worker_idx: usize) {
        let cpu_count = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1)
            .max(1);
        let cpu_id = worker_idx % cpu_count;

        if cpu_id >= libc::CPU_SETSIZE as usize {
            return;
        }

        // SAFETY:
        // - `cpuset` is initialized before use.
        // - `handle.as_pthread_t()` is a valid pthread handle for the spawned worker.
        // - Setting affinity failure is non-fatal and intentionally ignored.
        unsafe {
            let mut cpuset: libc::cpu_set_t = mem::zeroed();
            libc::CPU_ZERO(&mut cpuset);
            libc::CPU_SET(cpu_id, &mut cpuset);
            let _ = libc::pthread_setaffinity_np(
                handle.as_pthread_t(),
                mem::size_of::<libc::cpu_set_t>(),
                &cpuset,
            );
        }
    }

    #[cfg(not(target_os = "linux"))]
    fn set_thread_affinity(_handle: &thread::JoinHandle<()>, _worker_idx: usize) {}
}

impl Drop for TaskScheduler {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::SeqCst);
        self.queue.signal_all();
        let mut threads = self.threads.lock();
        Self::join_worker_handles(&mut threads);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use std::sync::Mutex as StdMutex;

    struct IncrementTask {
        counter: Arc<AtomicUsize>,
    }

    impl Task for IncrementTask {
        fn execute(&mut self, _mode: TaskExecutionMode) -> Result<TaskExecutionResult> {
            self.counter.fetch_add(1, Ordering::SeqCst);
            Ok(TaskExecutionResult::Finished)
        }

        fn task_type(&self) -> &str {
            "IncrementTask"
        }
    }

    #[test]
    fn test_scheduler_creation() {
        let scheduler = TaskScheduler::new();
        assert_eq!(scheduler.number_of_threads(), 0);
        assert_eq!(scheduler.pending_tasks(), 0);
    }

    #[test]
    fn test_scheduler_set_threads() {
        let scheduler = TaskScheduler::new();
        scheduler.set_threads(4).unwrap();
        assert_eq!(scheduler.number_of_threads(), 4);

        scheduler.set_threads(2).unwrap();
        assert_eq!(scheduler.number_of_threads(), 2);
    }

    #[test]
    fn test_thread_affinity_mode_defaults_to_auto() {
        let scheduler = TaskScheduler::new();
        assert_eq!(scheduler.thread_affinity_mode(), ThreadAffinityMode::Auto);
    }

    #[test]
    fn test_set_thread_affinity_mode_keeps_worker_count() {
        let scheduler = TaskScheduler::new();
        scheduler.set_threads(2).unwrap();
        assert_eq!(scheduler.number_of_threads(), 2);

        scheduler
            .set_thread_affinity_mode(ThreadAffinityMode::On)
            .unwrap();
        assert_eq!(scheduler.thread_affinity_mode(), ThreadAffinityMode::On);
        assert_eq!(scheduler.number_of_threads(), 2);

        scheduler
            .set_thread_affinity_mode(ThreadAffinityMode::Off)
            .unwrap();
        assert_eq!(scheduler.thread_affinity_mode(), ThreadAffinityMode::Off);
        assert_eq!(scheduler.number_of_threads(), 2);
    }

    #[test]
    fn test_scheduler_schedule_and_execute() {
        let scheduler = Arc::new(TaskScheduler::new());
        let counter = Arc::new(AtomicUsize::new(0));

        // Schedule tasks
        for _ in 0..10 {
            scheduler.schedule_task(Arc::new(Mutex::new(IncrementTask {
                counter: counter.clone(),
            })));
        }

        assert_eq!(scheduler.pending_tasks(), 10);

        // Execute on current thread
        let marker = AtomicBool::new(true);
        let completed = scheduler.execute_tasks(&marker, 100);

        assert_eq!(completed, 10);
        assert_eq!(counter.load(Ordering::SeqCst), 10);
        assert_eq!(scheduler.pending_tasks(), 0);
    }

    #[test]
    fn test_scheduler_with_worker_threads() {
        let scheduler = Arc::new(TaskScheduler::new());
        scheduler.set_threads(2).unwrap();

        let counter = Arc::new(AtomicUsize::new(0));

        // Schedule tasks
        for _ in 0..20 {
            scheduler.schedule_task(Arc::new(Mutex::new(IncrementTask {
                counter: counter.clone(),
            })));
        }

        // Wait for completion
        let start = std::time::Instant::now();
        while counter.load(Ordering::SeqCst) < 20 && start.elapsed() < Duration::from_secs(5) {
            thread::sleep(Duration::from_millis(10));
        }

        assert_eq!(counter.load(Ordering::SeqCst), 20);
    }

    #[test]
    fn test_producer_token() {
        let scheduler = Arc::new(TaskScheduler::new());
        let token = scheduler.create_producer();

        let counter = Arc::new(AtomicUsize::new(0));
        token.schedule_task(Arc::new(Mutex::new(IncrementTask {
            counter: counter.clone(),
        })));

        assert_eq!(scheduler.pending_tasks(), 1);
    }

    #[test]
    fn test_error_propagation() {
        let scheduler = Arc::new(TaskScheduler::new());

        struct FailingTask;
        impl Task for FailingTask {
            fn execute(&mut self, _mode: TaskExecutionMode) -> Result<TaskExecutionResult> {
                Err(paro_error::internal("Task failed"))
            }
            fn task_type(&self) -> &str {
                "FailingTask"
            }
        }

        scheduler.schedule_task(Arc::new(Mutex::new(FailingTask)));

        let marker = AtomicBool::new(true);
        let completed = scheduler.execute_tasks(&marker, 10);

        assert_eq!(completed, 1);
        assert!(scheduler.has_error());

        let error = scheduler.get_error().unwrap();
        assert!(error.message.contains("Task failed"));
    }

    #[test]
    fn test_panic_catching() {
        let scheduler = Arc::new(TaskScheduler::new());

        struct PanickingTask;
        impl Task for PanickingTask {
            fn execute(&mut self, _mode: TaskExecutionMode) -> Result<TaskExecutionResult> {
                panic!("Task panicked!");
            }
            fn task_type(&self) -> &str {
                "PanickingTask"
            }
        }

        scheduler.schedule_task(Arc::new(Mutex::new(PanickingTask)));

        let marker = AtomicBool::new(true);
        let completed = scheduler.execute_tasks(&marker, 10);

        assert_eq!(completed, 1);
        assert!(scheduler.has_error());

        let error = scheduler.get_error().unwrap();
        assert!(error.message.contains("panicked"));
    }

    #[test]
    fn test_cancel_tasks_on_error() {
        let scheduler = Arc::new(TaskScheduler::new());
        let counter = Arc::new(AtomicUsize::new(0));

        struct FailingTask;
        impl Task for FailingTask {
            fn execute(&mut self, _mode: TaskExecutionMode) -> Result<TaskExecutionResult> {
                Err(paro_error::internal("Task failed"))
            }
            fn task_type(&self) -> &str {
                "FailingTask"
            }
        }

        // Schedule a failing task first
        scheduler.schedule_task(Arc::new(Mutex::new(FailingTask)));

        // Schedule more tasks that should be cancelled
        for _ in 0..5 {
            scheduler.schedule_task(Arc::new(Mutex::new(IncrementTask {
                counter: counter.clone(),
            })));
        }

        assert_eq!(scheduler.pending_tasks(), 6);

        let marker = AtomicBool::new(true);
        let completed = scheduler.execute_tasks(&marker, 100);

        // Only the first task should have executed
        assert_eq!(completed, 1);
        assert!(scheduler.has_error());
        // Counter should not have been incremented (tasks were cancelled)
        assert_eq!(counter.load(Ordering::SeqCst), 0);
        // Queue should be empty (tasks were cancelled)
        assert_eq!(scheduler.pending_tasks(), 0);
    }

    #[test]
    fn test_error_stays_scoped_to_failing_producer() {
        let scheduler = Arc::new(TaskScheduler::new());
        let token_a = scheduler.create_producer_with_priority(0);
        let token_b = scheduler.create_producer_with_priority(0);
        let counter_b = Arc::new(AtomicUsize::new(0));

        token_a.schedule_task(Arc::new(Mutex::new(TokenAwareFailingTask { token: None })));
        token_b.schedule_task(Arc::new(Mutex::new(TokenAwareIncrementTask {
            counter: counter_b.clone(),
            token: None,
        })));

        let marker = AtomicBool::new(true);
        let completed = scheduler.execute_tasks(&marker, 10);

        assert_eq!(completed, 2);
        assert!(scheduler.has_error());
        assert!(scheduler.has_error_for_producer(&token_a));
        assert!(!scheduler.has_error_for_producer(&token_b));
        assert!(scheduler.get_error_for_producer(&token_b).is_none());
        assert_eq!(counter_b.load(Ordering::SeqCst), 1);
        assert_eq!(scheduler.pending_tasks(), 0);
    }

    #[test]
    fn test_worker_threads_continue_other_producers_after_scoped_error() {
        let scheduler = Arc::new(TaskScheduler::new());
        scheduler.set_threads(1).unwrap();

        let token_a = scheduler.create_producer_with_priority(0);
        let token_b = scheduler.create_producer_with_priority(0);
        let counter_b = Arc::new(AtomicUsize::new(0));

        token_a.schedule_task(Arc::new(Mutex::new(TokenAwareFailingTask { token: None })));
        token_b.schedule_task(Arc::new(Mutex::new(TokenAwareIncrementTask {
            counter: counter_b.clone(),
            token: None,
        })));

        let start = std::time::Instant::now();
        while counter_b.load(Ordering::SeqCst) == 0 && start.elapsed() < Duration::from_secs(5) {
            thread::sleep(Duration::from_millis(10));
        }

        assert_eq!(counter_b.load(Ordering::SeqCst), 1);
        assert!(scheduler.has_error_for_producer(&token_a));
        assert!(!scheduler.has_error_for_producer(&token_b));
    }

    #[test]
    fn test_reset_errors() {
        let scheduler = Arc::new(TaskScheduler::new());

        struct FailingTask;
        impl Task for FailingTask {
            fn execute(&mut self, _mode: TaskExecutionMode) -> Result<TaskExecutionResult> {
                Err(paro_error::internal("Task failed"))
            }
            fn task_type(&self) -> &str {
                "FailingTask"
            }
        }

        scheduler.schedule_task(Arc::new(Mutex::new(FailingTask)));

        let marker = AtomicBool::new(true);
        scheduler.execute_tasks(&marker, 10);

        assert!(scheduler.has_error());

        scheduler.reset_errors();
        assert!(!scheduler.has_error());
        assert!(scheduler.get_error().is_none());
    }

    #[test]
    fn test_wait_for_task_with_existing_tasks() {
        let scheduler = Arc::new(TaskScheduler::new());
        let counter = Arc::new(AtomicUsize::new(0));

        scheduler.schedule_task(Arc::new(Mutex::new(IncrementTask {
            counter: counter.clone(),
        })));

        let start = std::time::Instant::now();
        assert!(scheduler.wait_for_task());
        assert!(start.elapsed() < Duration::from_millis(50));
    }

    #[test]
    fn test_wait_for_task_timeout() {
        let scheduler = Arc::new(TaskScheduler::new());

        // No tasks scheduled, should timeout
        let start = std::time::Instant::now();
        let result = scheduler.wait_for_task();
        let elapsed = start.elapsed();

        // Should have waited approximately WAIT_FOR_TASK_MS
        assert!(!result); // Should timeout
        assert!(elapsed >= Duration::from_millis(WAIT_FOR_TASK_MS.saturating_sub(50)));
        assert!(elapsed <= Duration::from_millis(WAIT_FOR_TASK_MS + 200));
    }

    #[test]
    fn test_signal_task_rescheduled() {
        let scheduler = Arc::new(TaskScheduler::new());
        let scheduler_clone = scheduler.clone();

        // Spawn a thread that will signal immediately (before wait starts)
        // This tests that the signal flag is properly set and checked
        let handle = std::thread::spawn(move || {
            // Signal immediately - the flag should be set before wait_for_task checks it
            scheduler_clone.signal_task_rescheduled();
        });

        // Give the signal thread time to complete
        handle.join().unwrap();

        // Now wait - should return immediately because signal flag is set
        let start = std::time::Instant::now();
        let result = scheduler.wait_for_task();
        let elapsed = start.elapsed();

        // Should have returned quickly because signal was already sent
        assert!(result);
        // Should be very fast since flag was already set
        assert!(elapsed < Duration::from_millis(50));
    }

    #[test]
    fn test_execute_tasks_for_producer_isolated() {
        let scheduler = Arc::new(TaskScheduler::new());
        let token_a = scheduler.create_producer_with_priority(0);
        let token_b = scheduler.create_producer_with_priority(0);

        let counter_a = Arc::new(AtomicUsize::new(0));
        let counter_b = Arc::new(AtomicUsize::new(0));

        token_a.schedule_task(Arc::new(Mutex::new(IncrementTask {
            counter: counter_a.clone(),
        })));
        token_b.schedule_task(Arc::new(Mutex::new(IncrementTask {
            counter: counter_b.clone(),
        })));

        let marker = AtomicBool::new(true);
        let completed = scheduler.execute_tasks_for_producer(&token_a, &marker, 10);

        assert_eq!(completed, 1);
        assert_eq!(counter_a.load(Ordering::SeqCst), 1);
        assert_eq!(counter_b.load(Ordering::SeqCst), 0);
        assert_eq!(scheduler.pending_tasks_for_producer(&token_a), 0);
        assert_eq!(scheduler.pending_tasks_for_producer(&token_b), 1);
    }

    #[test]
    fn test_cancel_tasks_for_specific_producer() {
        let scheduler = Arc::new(TaskScheduler::new());
        let token_a = scheduler.create_producer_with_priority(0);
        let token_b = scheduler.create_producer_with_priority(0);
        let counter = Arc::new(AtomicUsize::new(0));

        for _ in 0..3 {
            token_a.schedule_task(Arc::new(Mutex::new(IncrementTask {
                counter: counter.clone(),
            })));
        }
        for _ in 0..2 {
            token_b.schedule_task(Arc::new(Mutex::new(IncrementTask {
                counter: counter.clone(),
            })));
        }

        let removed = scheduler.cancel_tasks_for_producer(&token_a);
        assert_eq!(removed, 3);
        assert_eq!(scheduler.pending_tasks_for_producer(&token_a), 0);
        assert_eq!(scheduler.pending_tasks_for_producer(&token_b), 2);
        assert_eq!(scheduler.pending_tasks(), 2);
    }

    struct OrderTask {
        label: &'static str,
        order: Arc<StdMutex<Vec<&'static str>>>,
    }

    impl Task for OrderTask {
        fn execute(&mut self, _mode: TaskExecutionMode) -> Result<TaskExecutionResult> {
            let mut order = self.order.lock().expect("order lock");
            order.push(self.label);
            Ok(TaskExecutionResult::Finished)
        }

        fn task_type(&self) -> &str {
            "OrderTask"
        }
    }

    struct TokenAwareIncrementTask {
        counter: Arc<AtomicUsize>,
        token: Option<ProducerToken>,
    }

    impl Task for TokenAwareIncrementTask {
        fn execute(&mut self, _mode: TaskExecutionMode) -> Result<TaskExecutionResult> {
            self.counter.fetch_add(1, Ordering::SeqCst);
            Ok(TaskExecutionResult::Finished)
        }

        fn set_token(&mut self, token: ProducerToken) {
            self.token = Some(token);
        }

        fn get_token(&self) -> Option<ProducerToken> {
            self.token.clone()
        }

        fn task_type(&self) -> &str {
            "TokenAwareIncrementTask"
        }
    }

    struct TokenAwareFailingTask {
        token: Option<ProducerToken>,
    }

    impl Task for TokenAwareFailingTask {
        fn execute(&mut self, _mode: TaskExecutionMode) -> Result<TaskExecutionResult> {
            Err(paro_error::internal("producer a failed"))
        }

        fn set_token(&mut self, token: ProducerToken) {
            self.token = Some(token);
        }

        fn get_token(&self) -> Option<ProducerToken> {
            self.token.clone()
        }

        fn task_type(&self) -> &str {
            "TokenAwareFailingTask"
        }
    }

    #[test]
    fn test_global_execute_respects_producer_priority() {
        let scheduler = Arc::new(TaskScheduler::new());
        let high = scheduler.create_producer_with_priority(10);
        let low = scheduler.create_producer_with_priority(0);

        let order = Arc::new(StdMutex::new(Vec::new()));

        low.schedule_task(Arc::new(Mutex::new(OrderTask {
            label: "low",
            order: order.clone(),
        })));
        high.schedule_task(Arc::new(Mutex::new(OrderTask {
            label: "high",
            order: order.clone(),
        })));

        let marker = AtomicBool::new(true);
        let completed = scheduler.execute_tasks(&marker, 1);
        assert_eq!(completed, 1);

        let execution_order = order.lock().expect("order lock").clone();
        assert_eq!(execution_order, vec!["high"]);
    }
}
