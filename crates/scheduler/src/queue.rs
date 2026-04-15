// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Priority-aware per-producer task queue with worker wakeups.

use crate::task::Task;
use parking_lot::{Condvar, Mutex};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

type TaskRef = Arc<parking_lot::Mutex<dyn Task>>;

#[derive(Default)]
struct ProducerTasks {
    priority: i32,
    tasks: VecDeque<TaskRef>,
}

#[derive(Default)]
struct QueueState {
    producers: HashMap<usize, ProducerTasks>,
    ready_producers: VecDeque<usize>,
}

/// ConcurrentTaskQueue manages a thread-safe queue of tasks with signaling.
///
/// Features:
/// - Per-producer task isolation
/// - Priority-aware global dequeue
/// - Condition variable for efficient worker wake-up
/// - Bulk enqueue for batch task submission
pub struct ConcurrentTaskQueue {
    /// Per-producer task queues and global ready-producer order.
    state: Mutex<QueueState>,
    /// Number of tasks currently in the queue
    tasks_in_queue: AtomicUsize,
    /// Mutex for the condvar
    lock: Mutex<()>,
    /// Condvar to wake up sleeping workers
    cv: Condvar,
}

impl ConcurrentTaskQueue {
    /// Create a new empty task queue.
    pub fn new() -> Self {
        Self {
            state: Mutex::new(QueueState::default()),
            tasks_in_queue: AtomicUsize::new(0),
            lock: Mutex::new(()),
            cv: Condvar::new(),
        }
    }

    /// Enqueue a task for a specific producer and signal one worker.
    pub fn enqueue(&self, producer_id: usize, priority: i32, task: TaskRef) {
        let mut state = self.state.lock();
        let producer = state
            .producers
            .entry(producer_id)
            .or_insert_with(|| ProducerTasks {
                priority,
                tasks: VecDeque::new(),
            });
        producer.priority = priority;
        let was_empty = producer.tasks.is_empty();
        producer.tasks.push_back(task);
        if was_empty {
            state.ready_producers.push_back(producer_id);
        }
        drop(state);

        self.tasks_in_queue.fetch_add(1, Ordering::SeqCst);
        self.cv.notify_one();
    }

    /// Enqueue multiple tasks for a specific producer and signal workers.
    pub fn enqueue_bulk(&self, producer_id: usize, priority: i32, tasks: Vec<TaskRef>) {
        let count = tasks.len();
        if count == 0 {
            return;
        }

        let mut state = self.state.lock();
        let producer = state
            .producers
            .entry(producer_id)
            .or_insert_with(|| ProducerTasks {
                priority,
                tasks: VecDeque::new(),
            });
        producer.priority = priority;
        let was_empty = producer.tasks.is_empty();
        producer.tasks.extend(tasks);
        if was_empty {
            state.ready_producers.push_back(producer_id);
        }
        drop(state);

        self.tasks_in_queue.fetch_add(count, Ordering::SeqCst);
        if count == 1 {
            self.cv.notify_one();
        } else {
            self.cv.notify_all();
        }
    }

    /// Try to dequeue a task without blocking.
    ///
    /// Selection policy:
    /// - Choose the highest-priority producer that currently has tasks.
    /// - For producers with the same priority, apply round-robin fairness.
    pub fn try_dequeue(&self) -> Option<TaskRef> {
        let mut state = self.state.lock();

        let mut best_idx: Option<usize> = None;
        let mut best_priority = i32::MIN;
        for (idx, producer_id) in state.ready_producers.iter().enumerate() {
            let Some(producer) = state.producers.get(producer_id) else {
                continue;
            };
            if producer.tasks.is_empty() {
                continue;
            }
            if producer.priority > best_priority {
                best_priority = producer.priority;
                best_idx = Some(idx);
            }
        }

        let best_idx = best_idx?;
        let producer_id = state
            .ready_producers
            .remove(best_idx)
            .expect("ready producer index should be valid");

        let task = {
            let producer = state
                .producers
                .get_mut(&producer_id)
                .expect("ready producer must exist");
            let task = producer
                .tasks
                .pop_front()
                .expect("ready producer must have task");
            if producer.tasks.is_empty() {
                state.producers.remove(&producer_id);
            } else {
                state.ready_producers.push_back(producer_id);
            }
            task
        };

        self.tasks_in_queue.fetch_sub(1, Ordering::SeqCst);
        Some(task)
    }

    /// Try to dequeue a task from a specific producer without blocking.
    pub fn try_dequeue_from_producer(&self, producer_id: usize) -> Option<TaskRef> {
        let mut state = self.state.lock();
        let producer = state.producers.get_mut(&producer_id)?;
        let task = producer.tasks.pop_front()?;

        if producer.tasks.is_empty() {
            state.producers.remove(&producer_id);
            state.ready_producers.retain(|id| *id != producer_id);
        }

        self.tasks_in_queue.fetch_sub(1, Ordering::SeqCst);
        Some(task)
    }

    /// Cancel all tasks for a specific producer.
    ///
    /// Returns the number of tasks removed.
    pub fn cancel_producer(&self, producer_id: usize) -> usize {
        let mut state = self.state.lock();
        let removed = state
            .producers
            .remove(&producer_id)
            .map(|producer| producer.tasks.len())
            .unwrap_or(0);
        if removed > 0 {
            state.ready_producers.retain(|id| *id != producer_id);
            self.tasks_in_queue.fetch_sub(removed, Ordering::SeqCst);
        }
        removed
    }

    /// Drain all tasks in the queue.
    ///
    /// Returns the number of tasks removed.
    pub fn drain_all(&self) -> usize {
        let mut state = self.state.lock();
        let removed: usize = state
            .producers
            .values()
            .map(|producer| producer.tasks.len())
            .sum();
        if removed > 0 {
            state.producers.clear();
            state.ready_producers.clear();
            self.tasks_in_queue.fetch_sub(removed, Ordering::SeqCst);
        }
        removed
    }

    /// Get the number of producers currently having queued tasks.
    pub fn producer_count(&self) -> usize {
        self.state.lock().producers.len()
    }

    /// Get the number of queued tasks for a specific producer.
    pub fn task_count_for_producer(&self, producer_id: usize) -> usize {
        self.state
            .lock()
            .producers
            .get(&producer_id)
            .map(|producer| producer.tasks.len())
            .unwrap_or(0)
    }

    /// Wait for a task to be available or timeout.
    ///
    /// Returns true if a task might be available, false if timed out.
    pub fn wait_for_task(&self, timeout: Duration) -> bool {
        let mut guard = self.lock.lock();
        if self.tasks_in_queue.load(Ordering::SeqCst) > 0 {
            return true;
        }
        !self.cv.wait_for(&mut guard, timeout).timed_out()
    }

    /// Signal all waiting workers.
    pub fn signal_all(&self) {
        self.cv.notify_all();
    }

    /// Get the current number of tasks in the queue.
    pub fn size(&self) -> usize {
        self.tasks_in_queue.load(Ordering::SeqCst)
    }

    /// Check if the queue is empty.
    pub fn is_empty(&self) -> bool {
        self.size() == 0
    }
}

impl Default for ConcurrentTaskQueue {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for ConcurrentTaskQueue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConcurrentTaskQueue")
            .field("size", &self.size())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task::{TaskExecutionMode, TaskExecutionResult};
    use paro_common::error::Result;
    use std::sync::Arc;
    use std::thread;

    struct SimpleTask;

    impl Task for SimpleTask {
        fn execute(&mut self, _mode: TaskExecutionMode) -> Result<TaskExecutionResult> {
            Ok(TaskExecutionResult::Finished)
        }

        fn task_type(&self) -> &str {
            "SimpleTask"
        }
    }

    #[test]
    fn test_queue_enqueue_dequeue() {
        let queue = ConcurrentTaskQueue::new();
        assert!(queue.is_empty());

        queue.enqueue(1, 0, Arc::new(parking_lot::Mutex::new(SimpleTask)));
        assert_eq!(queue.size(), 1);

        let task = queue.try_dequeue();
        assert!(task.is_some());
        assert!(queue.is_empty());
    }

    #[test]
    fn test_queue_bulk_enqueue() {
        let queue = ConcurrentTaskQueue::new();

        let tasks: Vec<Arc<parking_lot::Mutex<dyn Task>>> = (0..5)
            .map(|_| Arc::new(parking_lot::Mutex::new(SimpleTask)) as _)
            .collect();
        queue.enqueue_bulk(1, 0, tasks);

        assert_eq!(queue.size(), 5);

        for _ in 0..5 {
            assert!(queue.try_dequeue().is_some());
        }
        assert!(queue.is_empty());
    }

    #[test]
    fn test_queue_try_dequeue_empty() {
        let queue = ConcurrentTaskQueue::new();
        assert!(queue.try_dequeue().is_none());
    }

    #[test]
    fn test_queue_concurrent_access() {
        let queue = Arc::new(ConcurrentTaskQueue::new());
        let mut handles = vec![];

        // Producer threads
        for i in 0..4 {
            let q = queue.clone();
            handles.push(thread::spawn(move || {
                for _ in 0..10 {
                    q.enqueue(i, 0, Arc::new(parking_lot::Mutex::new(SimpleTask)));
                }
            }));
        }

        // Consumer thread
        let q = queue.clone();
        let consumer = thread::spawn(move || {
            let mut count = 0;
            while count < 40 {
                if q.try_dequeue().is_some() {
                    count += 1;
                } else {
                    thread::yield_now();
                }
            }
            count
        });

        for h in handles {
            h.join().unwrap();
        }

        let consumed = consumer.join().unwrap();
        assert_eq!(consumed, 40);
    }

    #[test]
    fn test_queue_dequeue_from_specific_producer() {
        let queue = ConcurrentTaskQueue::new();

        queue.enqueue(1, 0, Arc::new(parking_lot::Mutex::new(SimpleTask)));
        queue.enqueue(2, 0, Arc::new(parking_lot::Mutex::new(SimpleTask)));

        let task = queue
            .try_dequeue_from_producer(2)
            .expect("producer 2 should have task");
        assert_eq!(task.lock().task_type(), "SimpleTask");
        assert_eq!(queue.size(), 1);
        assert_eq!(queue.task_count_for_producer(1), 1);
        assert_eq!(queue.task_count_for_producer(2), 0);
    }

    #[test]
    fn test_queue_priority_scheduling() {
        let queue = ConcurrentTaskQueue::new();

        queue.enqueue(1, 0, Arc::new(parking_lot::Mutex::new(SimpleTask)));
        queue.enqueue(2, 10, Arc::new(parking_lot::Mutex::new(SimpleTask)));

        let _ = queue.try_dequeue().expect("task should exist");
        assert_eq!(queue.task_count_for_producer(2), 0);
        assert_eq!(queue.task_count_for_producer(1), 1);

        let _ = queue.try_dequeue().expect("task should exist");
        assert!(queue.is_empty());
    }

    #[test]
    fn test_cancel_producer_tasks() {
        let queue = ConcurrentTaskQueue::new();

        queue.enqueue(1, 0, Arc::new(parking_lot::Mutex::new(SimpleTask)));
        queue.enqueue(1, 0, Arc::new(parking_lot::Mutex::new(SimpleTask)));
        queue.enqueue(2, 0, Arc::new(parking_lot::Mutex::new(SimpleTask)));

        let removed = queue.cancel_producer(1);
        assert_eq!(removed, 2);
        assert_eq!(queue.size(), 1);
        assert_eq!(queue.task_count_for_producer(1), 0);
        assert_eq!(queue.task_count_for_producer(2), 1);
    }
}
