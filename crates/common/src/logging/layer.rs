// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Custom tracing layer for capturing logs to memory storage.

use crate::config::LogLevel;
use crate::logging::storage::{LogEntry, MemoryLogStorage, SpanInfo};
use std::sync::Arc;
use tracing::field::{Field, Visit};
use tracing::{Event, Subscriber};
use tracing_subscriber::layer::Context;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::Layer;

/// A tracing layer that captures log events to an in-memory storage.
///
/// This allows log entries to be queried via SQL.
pub(crate) struct MemoryLayer {
    storage: Arc<MemoryLogStorage>,
}

impl MemoryLayer {
    /// Create a new memory layer with the given storage.
    pub fn new(storage: Arc<MemoryLogStorage>) -> Self {
        Self { storage }
    }
}

impl<S> Layer<S> for MemoryLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        // Extract level
        let level = LogLevel::from(*event.metadata().level());

        // Extract target
        let target = event.metadata().target().to_string();

        // Extract message and fields
        let mut visitor = FieldVisitor::new();
        event.record(&mut visitor);

        // Get message - either explicit or constructed from fields
        let message = match visitor.message.take() {
            Some(msg) => msg,
            None => visitor.fields_string(),
        };

        // Collect span context
        let spans = collect_span_context(&ctx);

        // Create and store the entry
        let mut entry = LogEntry::new(level, target, message);
        entry.fields = visitor.fields;
        entry.spans = spans;

        self.storage.push(entry);
    }
}

/// Visitor that extracts fields from a tracing event.
struct FieldVisitor {
    message: Option<String>,
    fields: Vec<(String, String)>,
}

impl FieldVisitor {
    fn new() -> Self {
        Self {
            message: None,
            fields: Vec::new(),
        }
    }

    /// Build a message from fields if no explicit message was provided.
    fn fields_string(&self) -> String {
        self.fields
            .iter()
            .map(|(k, v)| format!("{}={}", k, v))
            .collect::<Vec<_>>()
            .join(" ")
    }
}

impl Visit for FieldVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        let value_str = format!("{:?}", value);
        if field.name() == "message" {
            // Strip surrounding quotes from debug-formatted strings
            let cleaned = value_str
                .strip_prefix('"')
                .and_then(|s| s.strip_suffix('"'))
                .map(|s| s.to_string())
                .unwrap_or(value_str);
            self.message = Some(cleaned);
        } else {
            self.fields.push((field.name().to_string(), value_str));
        }
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "message" {
            self.message = Some(value.to_string());
        } else {
            self.fields
                .push((field.name().to_string(), value.to_string()));
        }
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.fields
            .push((field.name().to_string(), value.to_string()));
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.fields
            .push((field.name().to_string(), value.to_string()));
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.fields
            .push((field.name().to_string(), value.to_string()));
    }
}

/// Collect span context from the current context.
fn collect_span_context<S>(ctx: &Context<'_, S>) -> Vec<SpanInfo>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    let mut spans = Vec::new();

    // Get the current span and traverse up to collect context
    if let Some(current) = ctx.lookup_current() {
        // Collect this span and all its parents
        let mut span_ref = Some(current);
        while let Some(span) = span_ref {
            let span_info = SpanInfo::new(span.name());
            spans.push(span_info);
            span_ref = span.parent();
        }
    }

    // Reverse to get outer-to-inner order (root first)
    spans.reverse();
    spans
}

#[cfg(test)]
mod tests {
    use super::*;
    use tracing_subscriber::layer::SubscriberExt;

    // Note: These tests use a local subscriber to avoid conflicts with global state

    #[test]
    fn test_memory_layer_captures_events() {
        let storage = Arc::new(MemoryLogStorage::new(100));
        let layer = MemoryLayer::new(Arc::clone(&storage));

        let subscriber = tracing_subscriber::registry().with(layer);

        tracing::subscriber::with_default(subscriber, || {
            tracing::info!("Test message");
            tracing::error!(user_id = 123, "Error occurred");
        });

        assert_eq!(storage.len(), 2);

        let entries = storage.all();
        assert_eq!(entries[0].message, "Test message");
        assert_eq!(entries[0].level, LogLevel::Info);

        assert_eq!(entries[1].level, LogLevel::Error);
        assert!(entries[1].fields.iter().any(|(k, _)| k == "user_id"));
    }

    #[test]
    fn test_memory_layer_captures_target() {
        let storage = Arc::new(MemoryLogStorage::new(100));
        let layer = MemoryLayer::new(Arc::clone(&storage));

        let subscriber = tracing_subscriber::registry().with(layer);

        tracing::subscriber::with_default(subscriber, || {
            tracing::info!(target: "paro::query", "Query started");
        });

        let entries = storage.all();
        assert_eq!(entries[0].target, "paro::query");
    }
}
