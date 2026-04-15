//! Query profiling metrics and rendering helpers.

use std::collections::HashMap;
use std::fmt;
use std::time::{Duration, Instant};

use crate::config::ProfilerPrintFormat;

/// Types of metrics that can be collected during query profiling.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MetricType {
    /// Total CPU time
    CpuTime,
    /// Query latency (wall clock time)
    Latency,
    /// Planner
    Planner,
    /// Planner binding
    PlannerBinding,
    /// Physical planner
    PhysicalPlanner,
    /// All optimizers combined
    AllOptimizers,

    /// Operator execution timing
    OperatorTiming,
    /// Operator cardinality (rows returned)
    OperatorCardinality,
    /// Rows scanned by operator
    OperatorRowsScanned,

    // Result metrics
    /// Total rows returned
    RowsReturned,
    /// Result set size in bytes
    ResultSetSize,
}

impl MetricType {
    /// Returns the display name for this metric type.
    pub fn name(&self) -> &'static str {
        match self {
            Self::CpuTime => "CPU Time",
            Self::Latency => "Latency",
            Self::Planner => "Planner",
            Self::PlannerBinding => "Planner Binding",
            Self::PhysicalPlanner => "Physical Planner",
            Self::AllOptimizers => "All Optimizers",
            Self::OperatorTiming => "Operator Timing",
            Self::OperatorCardinality => "Operator Cardinality",
            Self::OperatorRowsScanned => "Operator Rows Scanned",
            Self::RowsReturned => "Rows Returned",
            Self::ResultSetSize => "Result Set Size",
        }
    }

    /// True for planner / optimizer pipeline timing buckets (not row or operator stats).
    pub fn is_compiler_pipeline_metric(&self) -> bool {
        matches!(
            self,
            Self::Planner | Self::PlannerBinding | Self::PhysicalPlanner | Self::AllOptimizers
        )
    }
}

impl fmt::Display for MetricType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name())
    }
}

/// One timed segment (nested stack) for a given [`MetricType`].
#[derive(Debug, Clone)]
pub struct ProfileSegment {
    pub metric_type: MetricType,
    pub start_time: Instant,
    pub end_time: Option<Instant>,
}

impl ProfileSegment {
    pub fn new(metric_type: MetricType) -> Self {
        Self {
            metric_type,
            start_time: Instant::now(),
            end_time: None,
        }
    }

    pub fn end(&mut self) {
        self.end_time = Some(Instant::now());
    }

    pub fn duration(&self) -> Duration {
        match self.end_time {
            Some(end) => end.duration_since(self.start_time),
            None => self.start_time.elapsed(),
        }
    }

    pub fn is_ended(&self) -> bool {
        self.end_time.is_some()
    }
}

/// Collected metrics for a query execution.
#[derive(Debug, Clone, Default)]
pub struct QueryMetrics {
    pub cpu_time: Duration,
    pub latency: Duration,
    pub rows_returned: u64,
    pub result_set_size: u64,
    /// Wall time per [`MetricType`] (summed for stacked segments).
    pub stage_durations: HashMap<MetricType, Duration>,
}

impl QueryMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_duration(&mut self, metric_type: MetricType, duration: Duration) {
        self.stage_durations
            .entry(metric_type)
            .and_modify(|d| *d += duration)
            .or_insert(duration);
    }

    pub fn duration_for(&self, metric_type: MetricType) -> Duration {
        self.stage_durations
            .get(&metric_type)
            .copied()
            .unwrap_or_default()
    }
}

/// Query profiler for collecting execution metrics (planner/optimizer segments, latency, rows).
///
/// # Example
/// ```ignore
/// let mut profiler = QueryProfiler::new();
/// profiler.start_query("SELECT * FROM t", false);
/// profiler.begin_stage(MetricType::Planner);
/// // ... planning ...
/// profiler.end_stage();
/// profiler.end_query();
/// let rendered = profiler.to_string();
/// assert!(!rendered.is_empty());
/// ```
#[derive(Debug)]
pub struct QueryProfiler {
    /// Whether profiling is enabled
    enabled: bool,
    /// Whether detailed profiling is enabled
    detailed: bool,
    /// Whether the profiler is currently running
    running: bool,
    /// The query being profiled
    query: String,
    /// Whether this is an EXPLAIN ANALYZE query
    is_explain_analyze: bool,
    /// Query start time
    start_time: Option<Instant>,
    /// Query end time
    end_time: Option<Instant>,
    segment_stack: Vec<ProfileSegment>,
    /// Collected metrics
    metrics: QueryMetrics,
    /// Print format
    print_format: ProfilerPrintFormat,
    /// Save location for profiling output
    save_location: Option<String>,
}

impl QueryProfiler {
    /// Creates a new disabled query profiler.
    pub fn new() -> Self {
        Self {
            enabled: false,
            detailed: false,
            running: false,
            query: String::new(),
            is_explain_analyze: false,
            start_time: None,
            end_time: None,
            segment_stack: Vec::new(),
            metrics: QueryMetrics::new(),
            print_format: ProfilerPrintFormat::QueryTree,
            save_location: None,
        }
    }

    /// Creates a new enabled query profiler.
    pub fn enabled() -> Self {
        let mut profiler = Self::new();
        profiler.enabled = true;
        profiler
    }

    /// Returns whether profiling is enabled.
    #[inline]
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Returns whether detailed profiling is enabled.
    #[inline]
    pub fn is_detailed_enabled(&self) -> bool {
        self.detailed
    }

    /// Returns whether the profiler is currently running.
    #[inline]
    pub fn is_running(&self) -> bool {
        self.running
    }

    /// Enables profiling.
    pub fn enable(&mut self) {
        self.enabled = true;
    }

    /// Disables profiling.
    pub fn disable(&mut self) {
        self.enabled = false;
    }

    /// Enables detailed profiling.
    pub fn enable_detailed(&mut self) {
        self.detailed = true;
    }

    /// Sets the print format.
    pub fn set_print_format(&mut self, format: ProfilerPrintFormat) {
        self.print_format = format;
    }

    /// Gets the print format.
    pub fn get_print_format(&self) -> ProfilerPrintFormat {
        self.print_format
    }

    /// Sets the save location.
    pub fn set_save_location(&mut self, location: impl Into<String>) {
        self.save_location = Some(location.into());
    }

    /// Gets the save location.
    pub fn get_save_location(&self) -> Option<&str> {
        self.save_location.as_deref()
    }

    /// Starts profiling a query.
    ///
    /// # Arguments
    /// * `query` - The SQL query string
    /// * `is_explain_analyze` - Whether this is an EXPLAIN ANALYZE query
    pub fn start_query(&mut self, query: &str, is_explain_analyze: bool) {
        if !self.enabled && !is_explain_analyze {
            return;
        }
        self.reset();
        self.running = true;
        self.query = query.to_string();
        self.is_explain_analyze = is_explain_analyze;
        self.start_time = Some(Instant::now());
    }

    /// Ends the current query profiling.
    pub fn end_query(&mut self) {
        if !self.running {
            return;
        }
        self.end_time = Some(Instant::now());
        self.running = false;

        // Calculate total latency
        if let (Some(start), Some(end)) = (self.start_time, self.end_time) {
            self.metrics.latency = end.duration_since(start);
            self.metrics.cpu_time = self.metrics.latency; // Simplified: CPU time = latency
        }

        for seg in &mut self.segment_stack {
            if !seg.is_ended() {
                seg.end();
            }
        }
    }

    /// Resets the profiler state.
    pub fn reset(&mut self) {
        self.running = false;
        self.query.clear();
        self.is_explain_analyze = false;
        self.start_time = None;
        self.end_time = None;
        self.segment_stack.clear();
        self.metrics = QueryMetrics::new();
    }

    pub fn begin_stage(&mut self, metric_type: MetricType) {
        if !self.running {
            return;
        }
        self.segment_stack.push(ProfileSegment::new(metric_type));
    }

    pub fn end_stage(&mut self) {
        if !self.running {
            return;
        }
        if let Some(mut seg) = self.segment_stack.pop() {
            seg.end();
            self.metrics
                .record_duration(seg.metric_type, seg.duration());
        }
    }

    pub fn get_timing(&self, metric_type: MetricType) -> Duration {
        self.metrics.duration_for(metric_type)
    }

    /// Sets the number of rows returned.
    pub fn set_rows_returned(&mut self, rows: u64) {
        self.metrics.rows_returned = rows;
    }

    /// Sets the result set size.
    pub fn set_result_set_size(&mut self, size: u64) {
        self.metrics.result_set_size = size;
    }

    /// Returns the query string.
    pub fn query(&self) -> &str {
        &self.query
    }

    /// Returns the collected metrics.
    pub fn metrics(&self) -> &QueryMetrics {
        &self.metrics
    }

    /// Returns the total latency.
    pub fn latency(&self) -> Duration {
        self.metrics.latency
    }

    /// Formats the profiler output as a string.
    pub fn to_string_format(&self, format: ProfilerPrintFormat) -> String {
        match format {
            ProfilerPrintFormat::QueryTree | ProfilerPrintFormat::QueryTreeOptimizer => {
                self.to_tree_string()
            }
            ProfilerPrintFormat::Json => self.to_json(),
            ProfilerPrintFormat::NoOutput => String::new(),
        }
    }

    /// Formats the profiler output as a tree string.
    fn to_tree_string(&self) -> String {
        let mut output = String::new();
        output.push_str(
            "┌─────────────────────────────────────────────────────────────────────────┐\n",
        );
        output.push_str(
            "│                              Query Profiler                             │\n",
        );
        output.push_str(
            "├─────────────────────────────────────────────────────────────────────────┤\n",
        );

        // Query
        let query_display = if self.query.len() > 60 {
            format!("{}...", &self.query[..57])
        } else {
            self.query.clone()
        };
        output.push_str(&format!("│ Query: {:<64} │\n", query_display));
        output.push_str(
            "├─────────────────────────────────────────────────────────────────────────┤\n",
        );

        // Timing
        output.push_str(&format!(
            "│ Total Latency: {:>54.3?} │\n",
            self.metrics.latency
        ));
        output.push_str(&format!(
            "│ Rows Returned: {:>54} │\n",
            self.metrics.rows_returned
        ));

        if !self.metrics.stage_durations.is_empty() {
            output.push_str(
                "├─────────────────────────────────────────────────────────────────────────┤\n",
            );
            output.push_str(
                "│ Stage durations:                                                        │\n",
            );
            for (metric, duration) in &self.metrics.stage_durations {
                output.push_str(&format!("│   {}: {:>50.3?} │\n", metric.name(), duration));
            }
        }

        output.push_str(
            "└─────────────────────────────────────────────────────────────────────────┘\n",
        );
        output
    }

    /// Formats the profiler output as JSON.
    fn to_json(&self) -> String {
        let mut json = String::from("{\n");
        json.push_str(&format!(
            "  \"query\": \"{}\",\n",
            self.query.replace('\"', "\\\"")
        ));
        json.push_str(&format!(
            "  \"latency_ms\": {:.3},\n",
            self.metrics.latency.as_secs_f64() * 1000.0
        ));
        json.push_str(&format!(
            "  \"rows_returned\": {},\n",
            self.metrics.rows_returned
        ));
        json.push_str(&format!(
            "  \"result_set_size\": {},\n",
            self.metrics.result_set_size
        ));

        json.push_str("  \"stage_durations\": {\n");
        let timings: Vec<_> = self.metrics.stage_durations.iter().collect();
        for (i, (metric, duration)) in timings.iter().enumerate() {
            let comma = if i < timings.len() - 1 { "," } else { "" };
            json.push_str(&format!(
                "    \"{}\": {:.3}{}\n",
                metric.name(),
                duration.as_secs_f64() * 1000.0,
                comma
            ));
        }
        json.push_str("  }\n");
        json.push_str("}\n");
        json
    }
}

impl Default for QueryProfiler {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for QueryProfiler {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_string_format(self.print_format))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metric_type_name() {
        assert_eq!(MetricType::CpuTime.name(), "CPU Time");
        assert_eq!(MetricType::Planner.name(), "Planner");
        assert!(MetricType::Planner.is_compiler_pipeline_metric());
        assert!(!MetricType::RowsReturned.is_compiler_pipeline_metric());
    }

    #[test]
    fn test_profile_segment() {
        let mut seg = ProfileSegment::new(MetricType::Planner);
        assert!(!seg.is_ended());

        std::thread::sleep(std::time::Duration::from_millis(10));
        seg.end();

        assert!(seg.is_ended());
        assert!(seg.duration().as_millis() >= 10);
    }

    #[test]
    fn test_query_metrics() {
        let mut metrics = QueryMetrics::new();
        metrics.record_duration(MetricType::Planner, Duration::from_millis(100));
        metrics.record_duration(MetricType::Planner, Duration::from_millis(50));

        assert_eq!(
            metrics.duration_for(MetricType::Planner),
            Duration::from_millis(150)
        );
        assert_eq!(
            metrics.duration_for(MetricType::PhysicalPlanner),
            Duration::ZERO
        );
    }

    #[test]
    fn test_query_profiler_disabled() {
        let mut profiler = QueryProfiler::new();
        assert!(!profiler.is_enabled());

        profiler.start_query("SELECT 1", false);
        assert!(!profiler.is_running()); // Should not start when disabled
    }

    #[test]
    fn test_query_profiler_enabled() {
        let mut profiler = QueryProfiler::enabled();
        assert!(profiler.is_enabled());

        profiler.start_query("SELECT 1", false);
        assert!(profiler.is_running());
        assert_eq!(profiler.query(), "SELECT 1");

        profiler.end_query();
        assert!(!profiler.is_running());
    }

    #[test]
    fn test_query_profiler_explain_analyze() {
        let mut profiler = QueryProfiler::new();
        assert!(!profiler.is_enabled());

        // EXPLAIN ANALYZE should enable profiling even when disabled
        profiler.start_query("EXPLAIN ANALYZE SELECT 1", true);
        assert!(profiler.is_running());
    }

    #[test]
    fn test_query_profiler_stacked_stages() {
        let mut profiler = QueryProfiler::enabled();
        profiler.start_query("SELECT 1", false);

        profiler.begin_stage(MetricType::Planner);
        std::thread::sleep(std::time::Duration::from_millis(10));
        profiler.end_stage();

        profiler.begin_stage(MetricType::PhysicalPlanner);
        std::thread::sleep(std::time::Duration::from_millis(10));
        profiler.end_stage();

        profiler.end_query();

        assert!(profiler.get_timing(MetricType::Planner).as_millis() >= 10);
        assert!(profiler.get_timing(MetricType::PhysicalPlanner).as_millis() >= 10);
    }

    #[test]
    fn test_query_profiler_reset() {
        let mut profiler = QueryProfiler::enabled();
        profiler.start_query("SELECT 1", false);
        profiler.set_rows_returned(100);
        profiler.end_query();

        profiler.reset();
        assert!(profiler.query().is_empty());
        assert_eq!(profiler.metrics().rows_returned, 0);
    }

    #[test]
    fn test_query_profiler_to_json() {
        let mut profiler = QueryProfiler::enabled();
        profiler.start_query("SELECT 1", false);
        profiler.set_rows_returned(42);
        profiler.end_query();

        let json = profiler.to_string_format(ProfilerPrintFormat::Json);
        assert!(json.contains("\"query\": \"SELECT 1\""));
        assert!(json.contains("\"rows_returned\": 42"));
    }
}
