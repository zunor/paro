// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! State for the query currently executing in a session.

use std::sync::Arc;
use std::time::Instant;

use paro_execution::query_executor::executor::Executor;
use paro_scheduler::coordinator::EventCoordinator;

/// Progress information for a running query.
///
/// This tracks the execution progress of a query, including percentage
/// completion and rows processed.
///
#[derive(Debug, Clone, Default)]
pub struct QueryProgress {
    /// Percentage of query completion (0.0 to 100.0)
    pub percentage: f64,
    /// Number of rows processed so far
    pub rows_processed: u64,
    /// Total number of rows to process (if known)
    pub total_rows_to_process: u64,
    /// Whether the query is currently running
    pub is_running: bool,
}

impl QueryProgress {
    /// Creates a new QueryProgress with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Initializes/resets the progress to default state.
    pub fn initialize(&mut self) {
        self.percentage = 0.0;
        self.rows_processed = 0;
        self.total_rows_to_process = 0;
        self.is_running = false;
    }

    /// Updates the progress with new values.
    pub fn update(&mut self, rows_processed: u64, total_rows: u64) {
        self.rows_processed = rows_processed;
        self.total_rows_to_process = total_rows;
        if total_rows > 0 {
            self.percentage = (rows_processed as f64 / total_rows as f64) * 100.0;
        }
    }

    /// Marks the query as running.
    pub fn start(&mut self) {
        self.is_running = true;
    }

    /// Marks the query as finished.
    pub fn finish(&mut self) {
        self.is_running = false;
        self.percentage = 100.0;
    }

    /// Returns true if the query is currently running.
    pub fn is_running(&self) -> bool {
        self.is_running
    }

    /// Returns the completion percentage.
    pub fn get_percentage(&self) -> f64 {
        self.percentage
    }
}

/// Context for the currently executing query.
///
/// This structure holds all state related to the query that is currently
/// being executed in a session. It tracks the query string, prepared
/// statement data, coordinator, and progress information.
///
///
/// # Lifecycle
/// 1. Created when a query begins (`Session::begin_query_internal`)
/// 2. Holds coordinator and progress bar during execution
/// 3. Destroyed when query ends (`Session::end_query_internal`)
///
/// # Thread Safety
/// This structure is not thread-safe by itself. Access should be
/// synchronized through the owning Session's lock.
///
/// The executor is created per query and owned by `ActiveQueryContext`.
pub struct ActiveQueryContext {
    /// The query string currently being executed.
    query: String,

    /// Prepared statement name (if this query originated from a prepared statement).
    prepared_name: Option<String>,

    /// Query start time for timing.
    start_time: Instant,

    /// The currently open result (raw pointer for tracking).
    /// This is used to check if a result is still active.
    open_result_id: Option<u64>,

    /// Event coordinator for the running pipeline.
    coordinator: Option<Arc<EventCoordinator>>,

    /// The executor is created per-query and destroyed when the query ends.
    executor: Option<Executor>,
}

// Manual Debug implementation because EventCoordinator and Executor don't implement Debug
impl std::fmt::Debug for ActiveQueryContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ActiveQueryContext")
            .field("query", &self.query)
            .field("prepared_name", &self.prepared_name)
            .field("start_time", &self.start_time)
            .field("open_result_id", &self.open_result_id)
            .field("has_coordinator", &self.coordinator.is_some())
            .field("has_executor", &self.executor.is_some())
            .finish()
    }
}

impl ActiveQueryContext {
    /// Creates a new ActiveQueryContext for the given query.
    pub fn new(query: impl Into<String>) -> Self {
        Self {
            query: query.into(),
            prepared_name: None,
            start_time: Instant::now(),
            open_result_id: None,
            coordinator: None,
            executor: None,
        }
    }

    /// Creates a context that already owns an executor.
    pub fn with_executor(query: impl Into<String>, executor: Executor) -> Self {
        Self {
            query: query.into(),
            prepared_name: None,
            start_time: Instant::now(),
            open_result_id: None,
            coordinator: None,
            executor: Some(executor),
        }
    }

    /// Creates a new ActiveQueryContext with a prepared statement name.
    pub fn with_prepared(query: impl Into<String>, prepared_name: impl Into<String>) -> Self {
        Self {
            query: query.into(),
            prepared_name: Some(prepared_name.into()),
            start_time: Instant::now(),
            open_result_id: None,
            coordinator: None,
            executor: None,
        }
    }

    /// Returns the query string.
    #[inline]
    pub fn query(&self) -> &str {
        &self.query
    }

    /// Returns the prepared statement name, if any.
    #[inline]
    pub fn prepared_name(&self) -> Option<&str> {
        self.prepared_name.as_deref()
    }

    /// Returns the query start time.
    #[inline]
    pub fn start_time(&self) -> Instant {
        self.start_time
    }

    /// Returns the elapsed time since query start.
    #[inline]
    pub fn elapsed(&self) -> std::time::Duration {
        self.start_time.elapsed()
    }

    /// Sets the open result ID.
    ///
    /// This is used to track which result is currently open/active.
    pub fn set_open_result(&mut self, result_id: u64) {
        self.open_result_id = Some(result_id);
    }

    /// Checks if the given result ID matches the open result.
    pub fn is_open_result(&self, result_id: u64) -> bool {
        self.open_result_id == Some(result_id)
    }

    /// Returns true if there is an open result.
    pub fn has_open_result(&self) -> bool {
        self.open_result_id.is_some()
    }

    /// Clears the open result.
    pub fn clear_open_result(&mut self) {
        self.open_result_id = None;
    }

    /// Sets the event coordinator for the currently executing query.
    ///
    /// This should be called when query execution starts, to enable
    /// cancellation and status tracking.
    ///
    /// # Arguments
    /// * `coordinator` - The event coordinator managing pipeline execution
    pub fn set_coordinator(&mut self, coordinator: Arc<EventCoordinator>) {
        self.coordinator = Some(coordinator);
    }

    /// Returns a reference to the event coordinator, if any.
    ///
    /// Returns `None` if no query is currently executing or if the
    /// coordinator has not been set.
    pub fn coordinator(&self) -> Option<&Arc<EventCoordinator>> {
        self.coordinator.as_ref()
    }

    /// Cancels the currently executing query.
    ///
    /// This method cancels all pending pipeline tasks and marks the
    /// execution as cancelled. If no coordinator is set, this is a no-op.
    ///
    /// # Thread Safety
    /// This method is safe to call from any thread, as the coordinator
    /// handles synchronization internally.
    pub fn cancel(&mut self) {
        if let Some(ref coordinator) = self.coordinator {
            coordinator.cancel();
        }
    }

    /// Checks if a query is currently executing.
    ///
    /// Returns `true` if:
    /// - A coordinator is set, AND
    /// - The coordinator has not yet completed all events
    ///
    /// Returns `false` if:
    /// - No coordinator is set, OR
    /// - The coordinator has completed all events
    pub fn is_executing(&self) -> bool {
        self.coordinator
            .as_ref()
            .map(|c| !c.is_complete())
            .unwrap_or(false)
    }

    /// Clears the coordinator reference.
    ///
    /// This should be called when query execution finishes (either
    /// successfully or with an error).
    pub fn clear_coordinator(&mut self) {
        self.coordinator = None;
    }

    // ========================================================================
    // ========================================================================

    ///
    /// This should be called when the query begins execution.
    pub fn set_executor(&mut self, executor: Executor) {
        self.executor = Some(executor);
    }

    /// Returns a reference to the executor.
    ///
    /// # Panics
    ///
    /// Panics if the executor has not been set. Use `has_executor()` to check first.
    ///
    pub fn executor(&self) -> &Executor {
        self.executor.as_ref().expect("Executor not initialized")
    }

    /// Returns a mutable reference to the executor.
    ///
    /// # Panics
    ///
    /// Panics if the executor has not been set.
    pub fn executor_mut(&mut self) -> &mut Executor {
        self.executor.as_mut().expect("Executor not initialized")
    }

    /// Returns true if an executor has been set for this query.
    #[inline]
    pub fn has_executor(&self) -> bool {
        self.executor.is_some()
    }

    /// Takes the executor out of this context, if present.
    ///
    /// This can be used to move the executor to another location.
    pub fn take_executor(&mut self) -> Option<Executor> {
        self.executor.take()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // ------------------------------------------------------------------------
    // QueryProgress Tests
    // ------------------------------------------------------------------------

    #[test]
    fn test_query_progress_new() {
        let progress = QueryProgress::new();
        assert_eq!(progress.percentage, 0.0);
        assert_eq!(progress.rows_processed, 0);
        assert!(!progress.is_running);
    }

    #[test]
    fn test_query_progress_update() {
        let mut progress = QueryProgress::new();
        progress.update(50, 100);
        assert_eq!(progress.percentage, 50.0);
        assert_eq!(progress.rows_processed, 50);
        assert_eq!(progress.total_rows_to_process, 100);
    }

    #[test]
    fn test_query_progress_lifecycle() {
        let mut progress = QueryProgress::new();

        progress.start();
        assert!(progress.is_running());

        progress.update(25, 100);
        assert_eq!(progress.get_percentage(), 25.0);

        progress.finish();
        assert!(!progress.is_running());
        assert_eq!(progress.get_percentage(), 100.0);
    }

    #[test]
    fn test_query_progress_initialize() {
        let mut progress = QueryProgress::new();
        progress.update(50, 100);
        progress.start();

        progress.initialize();
        assert_eq!(progress.percentage, 0.0);
        assert_eq!(progress.rows_processed, 0);
        assert!(!progress.is_running);
    }

    #[test]
    fn test_active_query_context_new() {
        let ctx = ActiveQueryContext::new("SELECT 1");
        assert_eq!(ctx.query(), "SELECT 1");
        assert!(ctx.prepared_name().is_none());
        assert!(!ctx.has_open_result());
    }

    #[test]
    fn test_active_query_context_with_prepared() {
        let ctx = ActiveQueryContext::with_prepared("SELECT 1", "stmt1");

        assert_eq!(ctx.query(), "SELECT 1");
        assert_eq!(ctx.prepared_name(), Some("stmt1"));
    }

    #[test]
    fn test_active_query_context_elapsed() {
        let ctx = ActiveQueryContext::new("SELECT 1");
        std::thread::sleep(std::time::Duration::from_millis(10));
        assert!(ctx.elapsed().as_millis() >= 10);
    }

    #[test]
    fn test_active_query_context_open_result() {
        let mut ctx = ActiveQueryContext::new("SELECT 1");

        assert!(!ctx.has_open_result());
        assert!(!ctx.is_open_result(42));

        ctx.set_open_result(42);
        assert!(ctx.has_open_result());
        assert!(ctx.is_open_result(42));
        assert!(!ctx.is_open_result(99));

        ctx.clear_open_result();
        assert!(!ctx.has_open_result());
    }

    // ------------------------------------------------------------------------
    // Coordinator integration
    // ------------------------------------------------------------------------

    #[test]
    fn test_active_query_context_coordinator() {
        use paro_scheduler::scheduler::TaskScheduler;

        let mut ctx = ActiveQueryContext::new("SELECT 1");

        // Initially no coordinator
        assert!(ctx.coordinator().is_none());
        assert!(!ctx.is_executing());

        // Set coordinator
        let scheduler = Arc::new(TaskScheduler::new());
        let coordinator = Arc::new(EventCoordinator::new(scheduler));
        ctx.set_coordinator(coordinator.clone());

        // Now has coordinator
        assert!(ctx.coordinator().is_some());
        assert!(Arc::ptr_eq(ctx.coordinator().unwrap(), &coordinator));

        // Clear coordinator
        ctx.clear_coordinator();
        assert!(ctx.coordinator().is_none());
    }

    #[test]
    fn test_active_query_context_is_executing() {
        use paro_scheduler::event::Event;
        use paro_scheduler::scheduler::TaskScheduler;

        let mut ctx = ActiveQueryContext::new("SELECT 1");

        // Not executing initially
        assert!(!ctx.is_executing());

        // Set up coordinator with an event
        let scheduler = Arc::new(TaskScheduler::new());
        let coordinator = Arc::new(EventCoordinator::new(scheduler));
        let event = Event::new();
        coordinator.add_event(event.clone());

        ctx.set_coordinator(coordinator);

        // Now executing (event not complete)
        assert!(ctx.is_executing());

        // Complete the event
        event.set_tasks(1);
        event.finish_task();

        // Still has coordinator but execution is complete
        assert!(!ctx.is_executing());
    }

    #[test]
    fn test_active_query_context_cancel() {
        use paro_scheduler::scheduler::TaskScheduler;

        let mut ctx = ActiveQueryContext::new("SELECT 1");

        // Cancel without coordinator is a no-op
        ctx.cancel();

        // Set coordinator
        let scheduler = Arc::new(TaskScheduler::new());
        let coordinator = Arc::new(EventCoordinator::new(scheduler));
        ctx.set_coordinator(coordinator.clone());

        // Cancel should work
        ctx.cancel();

        // Coordinator should be cancelled (we can't directly check this,
        // but we can verify the method doesn't panic)
        assert!(ctx.coordinator().is_some());
    }

    #[test]
    fn test_active_query_context_debug() {
        let ctx = ActiveQueryContext::new("SELECT 1");
        let debug_str = format!("{:?}", ctx);
        assert!(debug_str.contains("ActiveQueryContext"));
        assert!(debug_str.contains("SELECT 1"));
    }
}
