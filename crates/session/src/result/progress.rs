// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Progress tracking and optional terminal rendering for long-running queries.

use std::io::{self, Write};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use paro_scheduler::coordinator::EventCoordinator;

// ============================================================================
// ProgressBarDisplay trait
// ============================================================================

/// Display interface for progress bar rendering.
///
/// Implement this trait to customize how progress is displayed.
///
pub trait ProgressBarDisplay: Send + Sync {
    /// Updates the display with the current progress percentage.
    ///
    /// # Arguments
    /// * `percentage` - Progress percentage (0.0 to 100.0)
    fn update(&self, percentage: f64);

    /// Finishes the progress display.
    fn finish(&self);
}

// ============================================================================
// TerminalProgressBarDisplay
// ============================================================================

/// Default terminal-based progress bar display.
///
/// Renders a progress bar to stderr with percentage and a visual bar.
#[derive(Debug)]
pub struct TerminalProgressBarDisplay {
    /// Width of the progress bar in characters
    bar_width: usize,
    /// Whether the display has been finished
    finished: AtomicBool,
}

impl Default for TerminalProgressBarDisplay {
    fn default() -> Self {
        Self::new()
    }
}

impl TerminalProgressBarDisplay {
    /// Creates a new terminal progress bar display.
    pub fn new() -> Self {
        Self {
            bar_width: 40,
            finished: AtomicBool::new(false),
        }
    }

    /// Creates a new terminal progress bar display with custom width.
    pub fn with_width(bar_width: usize) -> Self {
        Self {
            bar_width,
            finished: AtomicBool::new(false),
        }
    }

    /// Renders the progress bar string.
    fn render_bar(&self, percentage: f64) -> String {
        let filled = ((percentage / 100.0) * self.bar_width as f64) as usize;
        let empty = self.bar_width.saturating_sub(filled);

        let bar: String = std::iter::repeat_n('█', filled)
            .chain(std::iter::repeat_n('░', empty))
            .collect();

        format!("\r[{}] {:>5.1}%", bar, percentage)
    }
}

impl ProgressBarDisplay for TerminalProgressBarDisplay {
    fn update(&self, percentage: f64) {
        if self.finished.load(Ordering::Relaxed) {
            return;
        }
        let output = self.render_bar(percentage.clamp(0.0, 100.0));
        let _ = io::stderr().write_all(output.as_bytes());
        let _ = io::stderr().flush();
    }

    fn finish(&self) {
        if self.finished.swap(true, Ordering::Relaxed) {
            return; // Already finished
        }
        let _ = io::stderr().write_all(b"\r");
        // Clear the line
        let clear: String = std::iter::repeat_n(' ', self.bar_width + 10).collect();
        let _ = io::stderr().write_all(clear.as_bytes());
        let _ = io::stderr().write_all(b"\r");
        let _ = io::stderr().flush();
    }
}

// ============================================================================
// NoOpProgressBarDisplay
// ============================================================================

/// A no-op progress bar display that does nothing.
///
/// Useful for testing or when progress display is disabled.
#[derive(Debug, Default)]
pub struct NoOpProgressBarDisplay;

impl ProgressBarDisplay for NoOpProgressBarDisplay {
    fn update(&self, _percentage: f64) {}
    fn finish(&self) {}
}

// ============================================================================
// QueryProgress (atomic version for ProgressBar)
// ============================================================================

/// Thread-safe query progress tracking.
///
/// This is an atomic version of QueryProgress for use with ProgressBar,
/// allowing safe updates from multiple threads.
#[derive(Debug)]
pub struct AtomicQueryProgress {
    /// Progress percentage (stored as fixed-point: value * 100)
    percentage_fixed: AtomicU64,
    /// Number of rows processed
    rows_processed: AtomicU64,
    /// Total rows to process
    total_rows_to_process: AtomicU64,
}

impl Default for AtomicQueryProgress {
    fn default() -> Self {
        Self::new()
    }
}

impl AtomicQueryProgress {
    /// Creates a new atomic query progress.
    pub fn new() -> Self {
        Self {
            percentage_fixed: AtomicU64::new(0),
            rows_processed: AtomicU64::new(0),
            total_rows_to_process: AtomicU64::new(0),
        }
    }

    /// Initializes/resets the progress.
    pub fn initialize(&self) {
        self.percentage_fixed.store(0, Ordering::Relaxed);
        self.rows_processed.store(0, Ordering::Relaxed);
        self.total_rows_to_process.store(0, Ordering::Relaxed);
    }

    /// Gets the current percentage.
    pub fn get_percentage(&self) -> f64 {
        let fixed = self.percentage_fixed.load(Ordering::Relaxed);
        (fixed as f64) / 100.0
    }

    /// Sets the percentage.
    pub fn set_percentage(&self, percentage: f64) {
        let fixed = (percentage * 100.0) as u64;
        self.percentage_fixed.store(fixed, Ordering::Relaxed);
    }

    /// Gets the rows processed.
    pub fn get_rows_processed(&self) -> u64 {
        self.rows_processed.load(Ordering::Relaxed)
    }

    /// Gets the total rows to process.
    pub fn get_total_rows_to_process(&self) -> u64 {
        self.total_rows_to_process.load(Ordering::Relaxed)
    }

    /// Updates the progress with new values.
    pub fn update(&self, rows_processed: u64, total_rows: u64) {
        self.rows_processed.store(rows_processed, Ordering::Relaxed);
        self.total_rows_to_process
            .store(total_rows, Ordering::Relaxed);
        if total_rows > 0 {
            let percentage = (rows_processed as f64 / total_rows as f64) * 100.0;
            self.set_percentage(percentage);
        }
    }
}

// ============================================================================
// ProgressBar
// ============================================================================

/// Progress bar for tracking query execution progress.
///
/// The ProgressBar monitors query execution and displays progress to the user.
/// It only shows the progress bar after a configurable delay to avoid
/// flickering for fast queries.
///
///
/// # Example
/// ```ignore
/// let display = Arc::new(TerminalProgressBarDisplay::new());
/// let mut progress_bar = ProgressBar::new(display, Duration::from_millis(500));
/// progress_bar.start();
/// // ... during execution ...
/// progress_bar.update(50.0, false);
/// progress_bar.update(100.0, true);
/// ```
pub struct ProgressBar {
    /// The display used to render progress
    display: Arc<dyn ProgressBarDisplay>,
    /// Time after which to start showing progress
    show_progress_after: Duration,
    /// When the progress bar was started
    start_time: Option<Instant>,
    /// Current query progress
    query_progress: AtomicQueryProgress,
    /// Whether the progress bar is supported for current query
    supported: bool,
    /// Whether the progress bar has finished
    finished: bool,
    /// Whether the progress bar has been shown yet
    shown: bool,
}

impl ProgressBar {
    /// Creates a new progress bar with the given display and delay.
    ///
    /// # Arguments
    /// * `display` - The display implementation to use
    /// * `show_progress_after` - Delay before showing progress bar
    pub fn new(display: Arc<dyn ProgressBarDisplay>, show_progress_after: Duration) -> Self {
        Self {
            display,
            show_progress_after,
            start_time: None,
            query_progress: AtomicQueryProgress::new(),
            supported: true,
            finished: false,
            shown: false,
        }
    }

    /// Creates a new progress bar with default terminal display.
    pub fn with_terminal_display(show_progress_after: Duration) -> Self {
        Self::new(
            Arc::new(TerminalProgressBarDisplay::new()),
            show_progress_after,
        )
    }

    /// Creates a new progress bar with no-op display (for testing).
    pub fn disabled() -> Self {
        Self::new(Arc::new(NoOpProgressBarDisplay), Duration::ZERO)
    }

    /// Starts the progress bar timer.
    pub fn start(&mut self) {
        self.start_time = Some(Instant::now());
        self.query_progress.initialize();
        self.finished = false;
        self.shown = false;
    }

    /// Updates the progress bar.
    ///
    /// # Arguments
    /// * `percentage` - Current progress percentage (0.0 to 100.0)
    /// * `is_final` - Whether this is the final update
    pub fn update(&mut self, percentage: f64, is_final: bool) {
        if self.finished || !self.supported {
            return;
        }

        self.query_progress.set_percentage(percentage);

        if self.should_print(is_final) {
            self.print_progress(percentage);
        }

        if is_final {
            self.finish();
        }
    }

    pub fn update_rows(&mut self, rows_processed: u64, total_rows: u64, is_final: bool) {
        self.query_progress.update(rows_processed, total_rows);
        let percentage = self.query_progress.get_percentage();
        self.update(percentage, is_final);
    }

    /// Returns whether the progress bar should be printed.
    fn should_print(&self, is_final: bool) -> bool {
        if !self.supported {
            return false;
        }

        // Always print final update if we've shown progress
        if is_final && self.shown {
            return true;
        }

        // Check if enough time has passed
        if let Some(start) = self.start_time {
            if start.elapsed() >= self.show_progress_after {
                return true;
            }
        }

        false
    }

    /// Prints the current progress.
    fn print_progress(&mut self, percentage: f64) {
        self.shown = true;
        self.display.update(percentage);
    }

    /// Finishes the progress bar.
    pub fn finish(&mut self) {
        if self.finished {
            return;
        }
        self.finished = true;
        if self.shown {
            self.display.finish();
        }
    }

    /// Returns the current query progress.
    pub fn get_progress(&self) -> (f64, u64, u64) {
        (
            self.query_progress.get_percentage(),
            self.query_progress.get_rows_processed(),
            self.query_progress.get_total_rows_to_process(),
        )
    }

    /// Returns whether the progress bar is supported.
    pub fn is_supported(&self) -> bool {
        self.supported
    }

    /// Sets whether the progress bar is supported.
    pub fn set_supported(&mut self, supported: bool) {
        self.supported = supported;
    }

    /// Returns whether the progress bar has finished.
    pub fn is_finished(&self) -> bool {
        self.finished
    }

    /// Updates the progress bar from an EventCoordinator.
    ///
    /// events vs total events in the coordinator.
    ///
    /// # Arguments
    /// * `coordinator` - The event coordinator managing pipeline execution
    ///
    /// # Progress Calculation
    /// Progress is calculated as: (completed_events / total_events) * 100
    ///
    /// If the coordinator has completed all events, this is treated as
    /// the final update and the progress bar is finished.
    pub fn update_from_coordinator(&mut self, coordinator: &EventCoordinator) {
        if self.finished || !self.supported {
            return;
        }

        let total = coordinator.total_count();
        let completed = coordinator.completed_count();

        // Calculate percentage
        let percentage = if total > 0 {
            (completed as f64 / total as f64) * 100.0
        } else {
            0.0
        };

        // Check if execution is complete
        let is_final = coordinator.is_complete();

        // Update progress
        self.update(percentage, is_final);
    }
}

impl std::fmt::Debug for ProgressBar {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProgressBar")
            .field("show_progress_after", &self.show_progress_after)
            .field("supported", &self.supported)
            .field("finished", &self.finished)
            .field("shown", &self.shown)
            .finish_non_exhaustive()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicU32;

    // A test display that records updates
    struct TestProgressBarDisplay {
        update_count: AtomicU32,
        last_percentage: std::sync::Mutex<f64>,
        finished: AtomicBool,
    }

    impl TestProgressBarDisplay {
        fn new() -> Self {
            Self {
                update_count: AtomicU32::new(0),
                last_percentage: std::sync::Mutex::new(0.0),
                finished: AtomicBool::new(false),
            }
        }

        fn get_update_count(&self) -> u32 {
            self.update_count.load(Ordering::Relaxed)
        }

        fn get_last_percentage(&self) -> f64 {
            *self.last_percentage.lock().unwrap()
        }

        fn is_finished(&self) -> bool {
            self.finished.load(Ordering::Relaxed)
        }
    }

    impl ProgressBarDisplay for TestProgressBarDisplay {
        fn update(&self, percentage: f64) {
            self.update_count.fetch_add(1, Ordering::Relaxed);
            *self.last_percentage.lock().unwrap() = percentage;
        }

        fn finish(&self) {
            self.finished.store(true, Ordering::Relaxed);
        }
    }

    #[test]
    fn test_terminal_display_render() {
        let display = TerminalProgressBarDisplay::with_width(10);
        let bar = display.render_bar(50.0);
        assert!(bar.contains("█████░░░░░"));
        assert!(bar.contains("50.0%"));
    }

    #[test]
    fn test_atomic_query_progress() {
        let progress = AtomicQueryProgress::new();
        assert_eq!(progress.get_percentage(), 0.0);

        progress.update(50, 100);
        assert_eq!(progress.get_percentage(), 50.0);
        assert_eq!(progress.get_rows_processed(), 50);
        assert_eq!(progress.get_total_rows_to_process(), 100);

        progress.initialize();
        assert_eq!(progress.get_percentage(), 0.0);
    }

    #[test]
    fn test_progress_bar_delayed_show() {
        let display = Arc::new(TestProgressBarDisplay::new());
        let mut bar = ProgressBar::new(display.clone(), Duration::from_millis(100));

        bar.start();
        bar.update(25.0, false);

        // Should not show yet (delay not passed)
        assert_eq!(display.get_update_count(), 0);

        // Wait for delay
        std::thread::sleep(Duration::from_millis(150));
        bar.update(50.0, false);

        // Should show now
        assert_eq!(display.get_update_count(), 1);
        assert_eq!(display.get_last_percentage(), 50.0);
    }

    #[test]
    fn test_progress_bar_immediate_show() {
        let display = Arc::new(TestProgressBarDisplay::new());
        let mut bar = ProgressBar::new(display.clone(), Duration::ZERO);

        bar.start();
        bar.update(25.0, false);

        // Should show immediately (no delay)
        assert_eq!(display.get_update_count(), 1);
        assert_eq!(display.get_last_percentage(), 25.0);
    }

    #[test]
    fn test_progress_bar_finish() {
        let display = Arc::new(TestProgressBarDisplay::new());
        let mut bar = ProgressBar::new(display.clone(), Duration::ZERO);

        bar.start();
        bar.update(50.0, false);
        bar.update(100.0, true);

        assert!(bar.is_finished());
        assert!(display.is_finished());
    }

    #[test]
    fn test_progress_bar_disabled() {
        let mut bar = ProgressBar::disabled();
        bar.start();
        bar.update(50.0, false);
        bar.finish();
        // Should not crash
        assert!(bar.is_finished());
    }

    #[test]
    fn test_progress_bar_update_rows() {
        let display = Arc::new(TestProgressBarDisplay::new());
        let mut bar = ProgressBar::new(display.clone(), Duration::ZERO);

        bar.start();
        bar.update_rows(25, 100, false);

        let (percentage, rows, total) = bar.get_progress();
        assert_eq!(percentage, 25.0);
        assert_eq!(rows, 25);
        assert_eq!(total, 100);
    }

    #[test]
    fn test_progress_bar_unsupported() {
        let display = Arc::new(TestProgressBarDisplay::new());
        let mut bar = ProgressBar::new(display.clone(), Duration::ZERO);

        bar.set_supported(false);
        bar.start();
        bar.update(50.0, false);

        // Should not update when unsupported
        assert_eq!(display.get_update_count(), 0);
    }

    // ------------------------------------------------------------------------
    // Coordinator integration
    // ------------------------------------------------------------------------

    #[test]
    fn test_progress_bar_from_coordinator() {
        use paro_scheduler::event::Event;
        use paro_scheduler::scheduler::TaskScheduler;

        let display = Arc::new(TestProgressBarDisplay::new());
        let mut bar = ProgressBar::new(display.clone(), Duration::ZERO);

        // Create coordinator with 4 events
        let scheduler = Arc::new(TaskScheduler::new());
        let coordinator = EventCoordinator::new(scheduler);

        let events: Vec<_> = (0..4).map(|_| Event::new()).collect();
        for event in &events {
            coordinator.add_event(event.clone());
        }

        bar.start();

        // Initially 0% (no events completed)
        bar.update_from_coordinator(&coordinator);
        assert_eq!(display.get_last_percentage(), 0.0);

        // Complete 2 events (50%)
        for event in events.iter().take(2) {
            event.set_tasks(1);
            event.finish_task();
        }
        bar.update_from_coordinator(&coordinator);
        assert_eq!(display.get_last_percentage(), 50.0);

        // Complete all events (100%)
        for event in events.iter().skip(2) {
            event.set_tasks(1);
            event.finish_task();
        }
        bar.update_from_coordinator(&coordinator);
        assert_eq!(display.get_last_percentage(), 100.0);
        assert!(bar.is_finished());
    }

    #[test]
    fn test_progress_bar_from_coordinator_empty() {
        use paro_scheduler::scheduler::TaskScheduler;

        let display = Arc::new(TestProgressBarDisplay::new());
        let mut bar = ProgressBar::new(display.clone(), Duration::ZERO);

        // Create coordinator with no events
        let scheduler = Arc::new(TaskScheduler::new());
        let coordinator = EventCoordinator::new(scheduler);

        bar.start();
        bar.update_from_coordinator(&coordinator);

        // Should handle empty coordinator gracefully
        assert_eq!(display.get_last_percentage(), 0.0);
    }

    #[test]
    fn test_progress_bar_from_coordinator_incremental() {
        use paro_scheduler::event::Event;
        use paro_scheduler::scheduler::TaskScheduler;

        let display = Arc::new(TestProgressBarDisplay::new());
        let mut bar = ProgressBar::new(display.clone(), Duration::ZERO);

        let scheduler = Arc::new(TaskScheduler::new());
        let coordinator = EventCoordinator::new(scheduler);

        // Add 10 events
        let events: Vec<_> = (0..10).map(|_| Event::new()).collect();
        for event in &events {
            coordinator.add_event(event.clone());
        }

        bar.start();

        // Complete events one by one
        for (i, event) in events.iter().enumerate() {
            event.set_tasks(1);
            event.finish_task();
            bar.update_from_coordinator(&coordinator);

            let expected_percentage = ((i + 1) as f64 / 10.0) * 100.0;
            assert_eq!(display.get_last_percentage(), expected_percentage);
        }

        assert!(bar.is_finished());
    }
}
