//! Thread-local execution state shared across operators in a worker.

use std::sync::Arc;

use parking_lot::Mutex;

use crate::explain::profiler::{ExplainProfiler, OperatorProfiler};

/// Thread-local context for parallel execution.
///
/// Each worker thread gets one instance for its lifetime within a pipeline run.
pub struct ThreadContext {
    /// The thread ID within this execution (0-based).
    pub thread_id: usize,

    /// Total number of threads in this execution.
    pub total_threads: usize,
    /// Optional thread-local operator profiler for EXPLAIN ANALYZE.
    profiler: Option<Mutex<OperatorProfiler>>,
}

impl ThreadContext {
    /// Create a new ThreadContext for parallel execution.
    ///
    /// # Arguments
    ///
    /// * `thread_id` - The thread ID within this execution (0-based)
    /// * `total_threads` - Total number of threads in this execution
    pub fn new(thread_id: usize, total_threads: usize) -> Self {
        Self::new_with_profiler(thread_id, total_threads, None)
    }

    pub fn new_with_profiler(
        thread_id: usize,
        total_threads: usize,
        profiler: Option<Arc<ExplainProfiler>>,
    ) -> Self {
        Self {
            thread_id,
            total_threads,
            profiler: profiler.map(OperatorProfiler::new).map(Mutex::new),
        }
    }

    /// Create a ThreadContext for single-threaded execution.
    ///
    /// This is a convenience method for the common case of single-threaded execution.
    pub fn single_threaded() -> Self {
        Self::new(0, 1)
    }

    #[inline]
    pub fn start_operator(&self, node_id: u64) {
        if let Some(profiler) = &self.profiler {
            profiler.lock().start_operator(node_id);
        }
    }

    #[inline]
    pub fn end_operator(&self, node_id: u64, output_rows: u64) {
        if let Some(profiler) = &self.profiler {
            profiler.lock().end_operator(node_id, output_rows);
        }
    }

    #[inline]
    pub fn flush_profiler(&self) {
        if let Some(profiler) = &self.profiler {
            profiler.lock().flush();
        }
    }

    /// Get the thread ID.
    #[inline]
    pub fn thread_id(&self) -> usize {
        self.thread_id
    }

    /// Get the total number of threads.
    #[inline]
    pub fn total_threads(&self) -> usize {
        self.total_threads
    }

    /// Check if this is the first thread.
    #[inline]
    pub fn is_first_thread(&self) -> bool {
        self.thread_id == 0
    }

    /// Check if this is the last thread.
    #[inline]
    pub fn is_last_thread(&self) -> bool {
        self.thread_id == self.total_threads - 1
    }
}
