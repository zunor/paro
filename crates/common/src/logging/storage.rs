// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! In-memory log storage for SQL querying.
//!
//! Provides a ring buffer for storing log entries that can be queried
//! via SQL.

use crate::config::LogLevel;
use parking_lot::RwLock;
use std::collections::VecDeque;
use std::time::{SystemTime, UNIX_EPOCH};

/// A single log entry.
#[derive(Debug, Clone)]
pub struct LogEntry {
    /// Timestamp in Unix milliseconds.
    pub timestamp: u64,
    /// Log level.
    pub level: LogLevel,
    /// Log target (module path or custom target).
    pub target: String,
    /// Log message.
    pub message: String,
    /// Structured fields as key-value pairs.
    pub fields: Vec<(String, String)>,
    /// Span context (nested spans).
    pub spans: Vec<SpanInfo>,
}

impl LogEntry {
    /// Create a new log entry with the current timestamp.
    pub fn new(level: LogLevel, target: String, message: String) -> Self {
        Self {
            timestamp: current_timestamp_millis(),
            level,
            target,
            message,
            fields: Vec::new(),
            spans: Vec::new(),
        }
    }

    /// Add a structured field.
    pub fn with_field(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.fields.push((key.into(), value.into()));
        self
    }

    /// Add span context.
    pub fn with_span(mut self, span: SpanInfo) -> Self {
        self.spans.push(span);
        self
    }
}

/// Span information for context tracking.
#[derive(Debug, Clone)]
pub struct SpanInfo {
    /// Span name.
    pub name: String,
    /// Span fields.
    pub fields: Vec<(String, String)>,
}

impl SpanInfo {
    /// Create a new span info.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            fields: Vec::new(),
        }
    }

    /// Add a field to the span.
    pub fn with_field(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.fields.push((key.into(), value.into()));
        self
    }
}

/// In-memory log storage with a fixed-size ring buffer.
///
/// Thread-safe storage for log entries that automatically evicts
/// oldest entries when the buffer is full.
///
/// # Example
///
/// ```rust,ignore
/// use paro_common::logging::MemoryLogStorage;
///
/// let storage = MemoryLogStorage::new(1000);
/// storage.push(LogEntry::new(LogLevel::Info, "test".into(), "Hello".into()));
///
/// let entries = storage.query(&LogQueryFilter::default());
/// ```
pub struct MemoryLogStorage {
    entries: RwLock<VecDeque<LogEntry>>,
    max_entries: usize,
}

impl MemoryLogStorage {
    /// Create a new memory log storage with the specified capacity.
    pub fn new(max_entries: usize) -> Self {
        Self {
            entries: RwLock::new(VecDeque::with_capacity(max_entries)),
            max_entries,
        }
    }

    /// Add a log entry to the storage.
    ///
    /// If the storage is full, the oldest entry is removed.
    pub fn push(&self, entry: LogEntry) {
        let mut entries = self.entries.write();
        if entries.len() >= self.max_entries {
            entries.pop_front();
        }
        entries.push_back(entry);
    }

    /// Query log entries with a filter.
    pub fn query(&self, filter: &LogQueryFilter) -> Vec<LogEntry> {
        let entries = self.entries.read();
        let mut results: Vec<LogEntry> = entries
            .iter()
            .filter(|e| filter.matches(e))
            .cloned()
            .collect();

        // Apply limit if specified
        if let Some(limit) = filter.limit {
            results.truncate(limit);
        }

        results
    }

    /// Get all entries (no filtering).
    pub fn all(&self) -> Vec<LogEntry> {
        self.entries.read().iter().cloned().collect()
    }

    /// Clear all log entries.
    pub fn clear(&self) {
        self.entries.write().clear();
    }

    /// Get the number of stored entries.
    pub fn len(&self) -> usize {
        self.entries.read().len()
    }

    /// Check if the storage is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.read().is_empty()
    }

    /// Get the maximum capacity.
    pub fn capacity(&self) -> usize {
        self.max_entries
    }
}

/// Filter for querying log entries.
#[derive(Debug, Default, Clone)]
pub struct LogQueryFilter {
    /// Minimum log level (inclusive).
    pub min_level: Option<LogLevel>,
    /// Target prefix to match.
    pub target_prefix: Option<String>,
    /// Start timestamp (inclusive, Unix milliseconds).
    pub start_time: Option<u64>,
    /// End timestamp (inclusive, Unix milliseconds).
    pub end_time: Option<u64>,
    /// Message substring to match.
    pub message_contains: Option<String>,
    /// Maximum number of entries to return.
    pub limit: Option<usize>,
}

impl LogQueryFilter {
    /// Create a new empty filter (matches all entries).
    pub fn new() -> Self {
        Self::default()
    }

    /// Filter by minimum level.
    pub fn with_min_level(mut self, level: LogLevel) -> Self {
        self.min_level = Some(level);
        self
    }

    /// Filter by target prefix.
    pub fn with_target_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.target_prefix = Some(prefix.into());
        self
    }

    /// Filter by time range.
    pub fn with_time_range(mut self, start: u64, end: u64) -> Self {
        self.start_time = Some(start);
        self.end_time = Some(end);
        self
    }

    /// Filter by message content.
    pub fn with_message_contains(mut self, substring: impl Into<String>) -> Self {
        self.message_contains = Some(substring.into());
        self
    }

    /// Limit the number of results.
    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Check if an entry matches this filter.
    fn matches(&self, entry: &LogEntry) -> bool {
        // Check minimum level
        if let Some(min_level) = &self.min_level {
            if !entry.level.is_at_least(*min_level) {
                return false;
            }
        }

        // Check target prefix
        if let Some(prefix) = &self.target_prefix {
            if !entry.target.starts_with(prefix) {
                return false;
            }
        }

        // Check start time
        if let Some(start) = self.start_time {
            if entry.timestamp < start {
                return false;
            }
        }

        // Check end time
        if let Some(end) = self.end_time {
            if entry.timestamp > end {
                return false;
            }
        }

        // Check message contains
        if let Some(contains) = &self.message_contains {
            if !entry.message.contains(contains) {
                return false;
            }
        }

        true
    }
}

/// Get the current timestamp in Unix milliseconds.
fn current_timestamp_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memory_storage_basic() {
        let storage = MemoryLogStorage::new(10);
        assert!(storage.is_empty());

        storage.push(LogEntry::new(LogLevel::Info, "test".into(), "Hello".into()));
        assert_eq!(storage.len(), 1);

        let entries = storage.all();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].message, "Hello");
    }

    #[test]
    fn test_memory_storage_eviction() {
        let storage = MemoryLogStorage::new(3);

        for i in 0..5 {
            storage.push(LogEntry::new(
                LogLevel::Info,
                "test".into(),
                format!("Message {}", i),
            ));
        }

        assert_eq!(storage.len(), 3);
        let entries = storage.all();
        // Should have messages 2, 3, 4 (oldest evicted)
        assert_eq!(entries[0].message, "Message 2");
        assert_eq!(entries[1].message, "Message 3");
        assert_eq!(entries[2].message, "Message 4");
    }

    #[test]
    fn test_query_filter_level() {
        let storage = MemoryLogStorage::new(10);
        storage.push(LogEntry::new(
            LogLevel::Debug,
            "test".into(),
            "Debug msg".into(),
        ));
        storage.push(LogEntry::new(
            LogLevel::Info,
            "test".into(),
            "Info msg".into(),
        ));
        storage.push(LogEntry::new(
            LogLevel::Error,
            "test".into(),
            "Error msg".into(),
        ));

        let filter = LogQueryFilter::new().with_min_level(LogLevel::Info);
        let results = storage.query(&filter);

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].message, "Info msg");
        assert_eq!(results[1].message, "Error msg");
    }

    #[test]
    fn test_query_filter_target() {
        let storage = MemoryLogStorage::new(10);
        storage.push(LogEntry::new(
            LogLevel::Info,
            "paro::query".into(),
            "Query msg".into(),
        ));
        storage.push(LogEntry::new(
            LogLevel::Info,
            "paro::storage".into(),
            "Storage msg".into(),
        ));
        storage.push(LogEntry::new(
            LogLevel::Info,
            "other::module".into(),
            "Other msg".into(),
        ));

        let filter = LogQueryFilter::new().with_target_prefix("paro::");
        let results = storage.query(&filter);

        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_query_filter_message() {
        let storage = MemoryLogStorage::new(10);
        storage.push(LogEntry::new(
            LogLevel::Info,
            "test".into(),
            "Connection established".into(),
        ));
        storage.push(LogEntry::new(
            LogLevel::Error,
            "test".into(),
            "Connection failed".into(),
        ));
        storage.push(LogEntry::new(
            LogLevel::Info,
            "test".into(),
            "Query executed".into(),
        ));

        let filter = LogQueryFilter::new().with_message_contains("Connection");
        let results = storage.query(&filter);

        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_query_filter_limit() {
        let storage = MemoryLogStorage::new(10);
        for i in 0..5 {
            storage.push(LogEntry::new(
                LogLevel::Info,
                "test".into(),
                format!("Message {}", i),
            ));
        }

        let filter = LogQueryFilter::new().with_limit(2);
        let results = storage.query(&filter);

        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_log_entry_builder() {
        let entry = LogEntry::new(LogLevel::Info, "test".into(), "Hello".into())
            .with_field("user_id", "123")
            .with_field("query_id", "456")
            .with_span(SpanInfo::new("process_query").with_field("table", "users"));

        assert_eq!(entry.fields.len(), 2);
        assert_eq!(entry.spans.len(), 1);
        assert_eq!(entry.spans[0].name, "process_query");
    }
}
