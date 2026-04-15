// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Session-level configuration knobs used while planning and executing queries.

use std::collections::HashMap;

use paro_common::runtime_value::Value;
use paro_context::ExplainOutputType;

// ============================================================
// Enums
// ============================================================

/// Format for printing query profiling information.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ProfilerPrintFormat {
    /// Print as a query tree (default)
    #[default]
    QueryTree,
    /// Print as JSON
    Json,
    /// Print as query tree with optimizer info
    QueryTreeOptimizer,
    /// No output
    NoOutput,
}

/// Profiling coverage level.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ProfilingCoverage {
    /// Profile SELECT statements only (default)
    #[default]
    Select,
    /// Profile all statement types
    All,
}

// ============================================================
// SessionConfig
// ============================================================

/// Session configuration options.
///
/// This structure holds all configuration options that can be set on a per-session
///
/// # Example
///
/// ```
/// use paro_session::SessionConfig;
///
/// let mut config = SessionConfig::default();
/// config.enable_profiler = true;
/// config.enable_progress_bar = true;
/// ```
#[derive(Debug, Clone)]
pub struct SessionConfig {
    // ============================================================
    // General Settings
    // ============================================================
    /// The home directory used by the system (if any).
    pub home_directory: Option<String>,

    // ============================================================
    // Profiler Settings
    // ============================================================
    /// If the query profiler is enabled or not.
    pub enable_profiler: bool,

    /// If detailed query profiling is enabled.
    ///
    /// When enabled, includes more granular timing information.
    pub enable_detailed_profiling: bool,

    /// The format to print query profiling information in.
    pub profiler_print_format: ProfilerPrintFormat,

    /// The file to save query profiling information to.
    ///
    /// If `None`, profiling output is printed to the console.
    pub profiler_save_location: Option<String>,

    /// Allows suppressing profiler output, even if enabled.
    ///
    /// Useful for test runs where profiling is enabled but output is not wanted.
    pub emit_profiler_output: bool,

    /// The profiling coverage level.
    pub profiling_coverage: ProfilingCoverage,

    // ============================================================
    // Progress Bar Settings
    // ============================================================
    /// If the progress bar is enabled or not.
    pub enable_progress_bar: bool,

    /// If the print of the progress bar is enabled.
    ///
    /// When false, progress is tracked but not displayed.
    pub print_progress_bar: bool,

    /// The wait time before showing the progress bar (in milliseconds).
    ///
    /// The progress bar is only shown if the query takes longer than this.
    pub progress_bar_wait_time_ms: u32,

    // ============================================================
    // Query Execution Settings
    // ============================================================
    /// The maximum expression depth limit in the parser.
    ///
    /// Prevents stack overflow from deeply nested expressions.
    pub max_expression_depth: usize,

    /// Enable the running of optimizers.
    pub enable_optimizer: bool,

    /// Enable caching operators.
    ///
    /// When enabled, operators can cache intermediate results.
    pub enable_caching_operators: bool,

    /// The maximum amount of memory to keep buffered in a streaming query result.
    ///
    /// Default: 1MB (1_000_000 bytes).
    pub streaming_buffer_size: usize,

    /// The explain output type used when none is specified.
    pub explain_output_type: ExplainOutputType,

    // ============================================================
    // ============================================================
    /// Maximum number of threads for query execution.
    ///
    /// If `None`, uses the system default (CPU core count).
    /// Can be changed at runtime via `SET threads = N`.
    ///
    pub threads: Option<usize>,

    // ============================================================
    // Verification Settings (for testing)
    // ============================================================
    /// Whether or not aggressive query verification is enabled.
    ///
    /// Used for testing to verify query correctness.
    pub query_verification_enabled: bool,

    /// Force parallelism of small tables.
    ///
    /// Used for testing parallel execution.
    pub verify_parallelism: bool,

    /// Force out-of-core computation for operators that support it.
    ///
    /// Used for testing spill-to-disk behavior.
    pub force_external: bool,

    // ============================================================
    // Error Handling
    // ============================================================
    /// Output error messages as structured JSON instead of as a raw string.
    pub errors_as_json: bool,

    // ============================================================
    // Settings
    // ============================================================
    /// Registry-backed PostgreSQL-style session settings.
    pub settings: HashMap<String, Value>,
}

impl Default for SessionConfig {
    fn default() -> Self {
        Self {
            // General
            home_directory: None,

            // Profiler
            enable_profiler: false,
            enable_detailed_profiling: false,
            profiler_print_format: ProfilerPrintFormat::default(),
            profiler_save_location: None,
            emit_profiler_output: true,
            profiling_coverage: ProfilingCoverage::default(),

            // Progress bar
            enable_progress_bar: false,
            print_progress_bar: true,
            progress_bar_wait_time_ms: 2000,

            // Query execution
            max_expression_depth: 1000,
            enable_optimizer: true,
            enable_caching_operators: true,
            streaming_buffer_size: 1_000_000, // 1MB

            explain_output_type: ExplainOutputType::default(),

            threads: None, // Use system default

            // Verification (testing)
            query_verification_enabled: false,
            verify_parallelism: false,
            force_external: false,

            // Error handling
            errors_as_json: false,

            // Settings
            settings: HashMap::new(),
        }
    }
}

impl SessionConfig {
    /// Creates a new session config with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns whether any verification mode is enabled.
    pub fn any_verification(&self) -> bool {
        self.query_verification_enabled || self.verify_parallelism || self.force_external
    }

    // ============================================================
    // Session Settings
    // ============================================================

    /// Sets a registry-backed session setting.
    pub fn set_setting(&mut self, name: impl Into<String>, value: Value) {
        self.settings.insert(name.into().to_lowercase(), value);
    }

    /// Gets a session setting by name.
    pub fn get_setting(&self, name: &str) -> Option<&Value> {
        self.settings.get(&name.to_lowercase())
    }

    /// Resets (removes) a session setting entry.
    pub fn reset_setting(&mut self, name: &str) {
        self.settings.remove(&name.to_lowercase());
    }

    /// Clears all stored session settings.
    pub fn clear_settings(&mut self) {
        self.settings.clear();
    }

    // ============================================================
    // Convenience Methods
    // ============================================================

    /// Enables the query profiler.
    pub fn enable_profiling(&mut self) {
        self.enable_profiler = true;
    }

    /// Disables the query profiler.
    pub fn disable_profiling(&mut self) {
        self.enable_profiler = false;
    }

    /// Enables the progress bar with default settings.
    pub fn enable_progress(&mut self) {
        self.enable_progress_bar = true;
        self.print_progress_bar = true;
    }

    /// Disables the progress bar.
    pub fn disable_progress(&mut self) {
        self.enable_progress_bar = false;
    }

    // ============================================================
    // ============================================================

    /// Sets the maximum number of threads for query execution.
    ///
    /// # Arguments
    /// * `n` - Number of threads. Must be >= 1.
    ///
    /// - `ThreadsSetting::SetGlobal()` in `custom_settings.cpp`
    pub fn set_threads(&mut self, n: usize) {
        self.threads = Some(n.max(1));
    }

    /// Gets the configured number of threads.
    ///
    /// Returns `None` if using system default.
    pub fn get_threads(&self) -> Option<usize> {
        self.threads
    }

    /// Resets threads to system default.
    pub fn reset_threads(&mut self) {
        self.threads = None;
    }
}

// ============================================================
// Tests
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_session_config_default() {
        let config = SessionConfig::default();

        assert!(config.home_directory.is_none());
        assert!(!config.enable_profiler);
        assert!(!config.enable_detailed_profiling);
        assert_eq!(config.profiler_print_format, ProfilerPrintFormat::QueryTree);
        assert!(config.profiler_save_location.is_none());
        assert!(config.emit_profiler_output);

        assert!(!config.enable_progress_bar);
        assert!(config.print_progress_bar);
        assert_eq!(config.progress_bar_wait_time_ms, 2000);

        assert_eq!(config.max_expression_depth, 1000);
        assert!(config.enable_optimizer);
        assert!(config.enable_caching_operators);
        assert_eq!(config.streaming_buffer_size, 1_000_000);

        assert!(!config.query_verification_enabled);
        assert!(!config.verify_parallelism);
        assert!(!config.force_external);

        assert!(!config.errors_as_json);
        assert!(config.settings.is_empty());
    }

    #[test]
    fn test_session_config_new() {
        let config = SessionConfig::new();
        assert_eq!(config.max_expression_depth, 1000);
    }

    #[test]
    fn test_any_verification() {
        let mut config = SessionConfig::default();
        assert!(!config.any_verification());

        config.query_verification_enabled = true;
        assert!(config.any_verification());

        config.query_verification_enabled = false;
        config.verify_parallelism = true;
        assert!(config.any_verification());

        config.verify_parallelism = false;
        config.force_external = true;
        assert!(config.any_verification());
    }

    #[test]
    fn test_session_settings() {
        let mut config = SessionConfig::default();

        config.set_setting("threads", Value::Integer(4));
        assert_eq!(config.get_setting("threads"), Some(&Value::Integer(4)));
        assert_eq!(config.get_setting("THREADS"), Some(&Value::Integer(4)));

        config.reset_setting("threads");
        assert!(config.get_setting("threads").is_none());

        config.set_setting("copy_buffer_size", Value::Integer(8192));
        config.set_setting("force_external", Value::Boolean(true));
        assert_eq!(config.settings.len(), 2);

        config.clear_settings();
        assert!(config.settings.is_empty());
    }

    #[test]
    fn test_profiling_convenience_methods() {
        let mut config = SessionConfig::default();

        assert!(!config.enable_profiler);
        config.enable_profiling();
        assert!(config.enable_profiler);

        config.disable_profiling();
        assert!(!config.enable_profiler);
    }

    #[test]
    fn test_progress_convenience_methods() {
        let mut config = SessionConfig::default();

        assert!(!config.enable_progress_bar);
        config.enable_progress();
        assert!(config.enable_progress_bar);
        assert!(config.print_progress_bar);

        config.disable_progress();
        assert!(!config.enable_progress_bar);
    }

    #[test]
    fn test_explain_output_type_default() {
        assert_eq!(
            ExplainOutputType::default(),
            ExplainOutputType::PhysicalOnly
        );
    }

    #[test]
    fn test_profiler_print_format_default() {
        assert_eq!(
            ProfilerPrintFormat::default(),
            ProfilerPrintFormat::QueryTree
        );
    }

    #[test]
    fn test_profiling_coverage_default() {
        assert_eq!(ProfilingCoverage::default(), ProfilingCoverage::Select);
    }

    #[test]
    fn test_threads_default() {
        let config = SessionConfig::default();
        assert!(config.threads.is_none());
        assert!(config.get_threads().is_none());
    }

    #[test]
    fn test_threads_set_and_get() {
        let mut config = SessionConfig::default();

        config.set_threads(4);
        assert_eq!(config.get_threads(), Some(4));

        config.set_threads(8);
        assert_eq!(config.get_threads(), Some(8));
    }

    #[test]
    fn test_threads_minimum_value() {
        let mut config = SessionConfig::default();

        // Setting 0 should be clamped to 1
        config.set_threads(0);
        assert_eq!(config.get_threads(), Some(1));
    }

    #[test]
    fn test_threads_reset() {
        let mut config = SessionConfig::default();

        config.set_threads(4);
        assert_eq!(config.get_threads(), Some(4));

        config.reset_threads();
        assert!(config.get_threads().is_none());
    }
}
