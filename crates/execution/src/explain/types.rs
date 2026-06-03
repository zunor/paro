// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_planner::operator::{ExplainMode, ExplainSpec};
use paro_planner::plan::CardinalityEstimate;
use serde_json::{Map, Value as JsonValue};

pub type ExplainNodeId = u64;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExplainSchema {
    pub output_names: Vec<String>,
    pub relation_name: Option<String>,
    pub relation_alias: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct ExplainSearchInfo {
    pub summary: String,
    pub confidence: Option<String>,
    pub candidates: Vec<String>,
}

#[derive(Debug, Clone, Default)]
pub struct ExplainLogicalInfo {
    pub estimated_cardinality: Option<CardinalityEstimate>,
    pub search: Option<ExplainSearchInfo>,
}

#[derive(Debug, Clone, Default)]
pub struct ExplainRuntimeStats {
    pub spilled: Option<bool>,
    pub peak_memory_bytes: Option<u64>,
    pub temp_storage_bytes: Option<u64>,
    pub data_plane_bytes: Option<u64>,
    pub memory_tag_bytes: Vec<ExplainMemoryTagStat>,
    pub domain_memory_tag_bytes: Vec<ExplainMemoryDomainTagStat>,
    pub leaked_grant_bytes: Option<u64>,
    pub local_refill_count: Option<u64>,
    pub local_refill_bytes: Option<u64>,
    pub reclaim_attempt_count: Option<u64>,
    pub reclaimed_bytes: Option<u64>,
    pub spilled_bytes: Option<u64>,
    pub reclaim_latency_us: Option<u64>,
    pub spill_latency_us: Option<u64>,
    pub allocator_tracking_event_count: Option<u64>,
    pub selection_materialization_count: Option<u64>,
    pub scheduler_worker_count: Option<u64>,
    pub scheduler_morsel_count: Option<u64>,
    pub scheduler_blocked_count: Option<u64>,
    pub scheduler_wake_count: Option<u64>,
    pub scheduler_ready_time_us: Option<u64>,
    pub scheduler_wait_time_us: Option<u64>,
    pub scheduler_wake_coalesce_count: Option<u64>,
    pub output_backpressure_count: Option<u64>,
    pub runtime_filter_installed_count: Option<u64>,
    /// Reserved for runtime-filter producers that intentionally let probes run
    /// without waiting for a late filter. Current rowset runtime filters still
    /// require the build-side filter to be installed before scan init.
    pub runtime_filter_no_wait_count: Option<u64>,
    pub grant_bytes: Option<u64>,
    pub revoked_bytes: Option<u64>,
    pub yield_latency_us: Option<u64>,
    pub repartition_depth: Option<u64>,
    pub peak_rss_bytes: Option<u64>,
    pub output_buffer_bytes: Option<u64>,
    pub session_retained_bytes: Option<u64>,
}

impl ExplainRuntimeStats {
    pub fn has_any(&self) -> bool {
        self.spilled.is_some()
            || self.peak_memory_bytes.is_some()
            || self.temp_storage_bytes.is_some()
            || self.data_plane_bytes.is_some()
            || !self.memory_tag_bytes.is_empty()
            || !self.domain_memory_tag_bytes.is_empty()
            || self.leaked_grant_bytes.is_some()
            || self.local_refill_count.is_some()
            || self.local_refill_bytes.is_some()
            || self.reclaim_attempt_count.is_some()
            || self.reclaimed_bytes.is_some()
            || self.spilled_bytes.is_some()
            || self.reclaim_latency_us.is_some()
            || self.spill_latency_us.is_some()
            || self.allocator_tracking_event_count.is_some()
            || self.selection_materialization_count.is_some()
            || self.scheduler_worker_count.is_some()
            || self.scheduler_morsel_count.is_some()
            || self.scheduler_blocked_count.is_some()
            || self.scheduler_wake_count.is_some()
            || self.scheduler_ready_time_us.is_some()
            || self.scheduler_wait_time_us.is_some()
            || self.scheduler_wake_coalesce_count.is_some()
            || self.output_backpressure_count.is_some()
            || self.runtime_filter_installed_count.is_some()
            || self.runtime_filter_no_wait_count.is_some()
            || self.grant_bytes.is_some()
            || self.revoked_bytes.is_some()
            || self.yield_latency_us.is_some()
            || self.repartition_depth.is_some()
            || self.peak_rss_bytes.is_some()
            || self.output_buffer_bytes.is_some()
            || self.session_retained_bytes.is_some()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExplainMemoryTagStat {
    pub tag: String,
    pub bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExplainMemoryDomainTagStat {
    pub domain: String,
    pub tag: String,
    pub bytes: u64,
}

#[derive(Debug, Clone, Default)]
pub struct ExplainActualStats {
    pub output_rows: u64,
    pub loops: u64,
    pub startup_time_ms: Option<f64>,
    pub total_time_ms: Option<f64>,
    pub runtime: ExplainRuntimeStats,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExplainControlRegionStats {
    RecursiveCte(ExplainRecursiveCteStats),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExplainRecursiveCteStats {
    pub region_id: usize,
    pub iterations: usize,
    pub termination: ExplainRecursiveCteTermination,
    pub iteration_stats: Vec<ExplainRecursiveCteIterationStats>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExplainRecursiveCteTermination {
    EmptyDelta,
    MaxIterations,
    Cancelled,
}

impl ExplainRecursiveCteTermination {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::EmptyDelta => "empty_delta",
            Self::MaxIterations => "max_iterations",
            Self::Cancelled => "cancelled",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExplainRecursiveCteIterationStats {
    pub iteration: usize,
    pub delta_rows: u64,
    pub working_rows: u64,
}

#[derive(Debug, Clone)]
pub enum ExplainValue {
    String(String),
    Integer(i64),
    Unsigned(u64),
    Float(f64),
    Bool(bool),
    Bytes(u64),
    List(Vec<ExplainValue>),
}

impl ExplainValue {
    pub fn to_text(&self) -> String {
        match self {
            Self::String(value) => value.clone(),
            Self::Integer(value) => value.to_string(),
            Self::Unsigned(value) => value.to_string(),
            Self::Float(value) => format!("{value:.3}"),
            Self::Bool(value) => value.to_string(),
            Self::Bytes(value) => format_bytes(*value),
            Self::List(values) => values
                .iter()
                .map(Self::to_text)
                .collect::<Vec<_>>()
                .join(", "),
        }
    }

    pub fn to_json(&self) -> JsonValue {
        match self {
            Self::String(value) => JsonValue::String(value.clone()),
            Self::Integer(value) => JsonValue::from(*value),
            Self::Unsigned(value) => JsonValue::from(*value),
            Self::Float(value) => JsonValue::from(*value),
            Self::Bool(value) => JsonValue::from(*value),
            Self::Bytes(value) => JsonValue::from(*value),
            Self::List(values) => JsonValue::Array(values.iter().map(Self::to_json).collect()),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ExplainProperty {
    pub label: String,
    pub value: ExplainValue,
}

impl ExplainProperty {
    pub fn new(label: impl Into<String>, value: ExplainValue) -> Self {
        Self {
            label: label.into(),
            value,
        }
    }

    pub fn text_line(&self) -> String {
        format!("{}: {}", self.label, self.value.to_text())
    }

    pub fn to_json_entry(&self) -> (String, JsonValue) {
        (self.label.clone(), self.value.to_json())
    }
}

#[derive(Debug, Clone)]
pub struct ExplainNode {
    pub node_id: Option<ExplainNodeId>,
    pub operator_name: String,
    pub relation_name: Option<String>,
    pub relation_alias: Option<String>,
    pub output_names: Vec<String>,
    pub estimated_cardinality: Option<CardinalityEstimate>,
    pub actual: Option<ExplainActualStats>,
    pub properties: Vec<ExplainProperty>,
    pub children: Vec<ExplainNode>,
}

impl ExplainNode {
    pub fn relation_label(&self) -> Option<String> {
        let relation = self.relation_name.as_ref()?;
        match &self.relation_alias {
            Some(alias) => Some(format!("{relation} {alias}")),
            None => Some(relation.clone()),
        }
    }

    pub fn header_text(&self, spec: &ExplainSpec) -> String {
        let mut header = self.operator_name.clone();
        if let Some(relation) = self.relation_label() {
            header.push_str(" on ");
            header.push_str(&relation);
        }
        if let Some(estimate) = self.estimated_cardinality {
            header.push_str(&format!("  (rows={})", estimate.expected));
        }
        if matches!(spec.mode, ExplainMode::Analyze) {
            if let Some(actual) = &self.actual {
                if spec.detail.timing {
                    let start = actual.startup_time_ms.unwrap_or(0.0);
                    let end = actual.total_time_ms.unwrap_or(start);
                    header.push_str(&format!(
                        " (actual time={start:.3}..{end:.3} rows={} loops={})",
                        actual.output_rows,
                        actual.loops.max(1)
                    ));
                } else {
                    header.push_str(&format!(
                        " (actual rows={} loops={})",
                        actual.output_rows,
                        actual.loops.max(1)
                    ));
                }
            }
        }
        header
    }

    pub fn to_json(&self, spec: &ExplainSpec) -> JsonValue {
        let mut object = Map::new();
        if let Some(node_id) = self.node_id {
            object.insert("node_id".to_string(), JsonValue::from(node_id));
        }
        object.insert(
            "operator".to_string(),
            JsonValue::String(self.operator_name.clone()),
        );
        if let Some(relation_name) = &self.relation_name {
            object.insert(
                "relation".to_string(),
                JsonValue::String(relation_name.clone()),
            );
        }
        if let Some(relation_alias) = &self.relation_alias {
            object.insert(
                "alias".to_string(),
                JsonValue::String(relation_alias.clone()),
            );
        }
        if let Some(estimated_cardinality) = self.estimated_cardinality {
            object.insert(
                "estimated_rows".to_string(),
                JsonValue::from(estimated_cardinality.expected),
            );
            let mut estimate = Map::new();
            estimate.insert(
                "min".to_string(),
                JsonValue::from(estimated_cardinality.min),
            );
            estimate.insert(
                "expected".to_string(),
                JsonValue::from(estimated_cardinality.expected),
            );
            estimate.insert(
                "max".to_string(),
                JsonValue::from(estimated_cardinality.max),
            );
            object.insert(
                "estimated_cardinality".to_string(),
                JsonValue::Object(estimate),
            );
        }
        if !self.output_names.is_empty() {
            object.insert(
                "output_names".to_string(),
                JsonValue::Array(
                    self.output_names
                        .iter()
                        .cloned()
                        .map(JsonValue::String)
                        .collect(),
                ),
            );
        }
        if let Some(actual) = &self.actual {
            let mut actual_object = Map::new();
            actual_object.insert("rows".to_string(), JsonValue::from(actual.output_rows));
            actual_object.insert("loops".to_string(), JsonValue::from(actual.loops.max(1)));
            if spec.detail.timing {
                if let Some(value) = actual.startup_time_ms {
                    actual_object.insert("startup_time_ms".to_string(), JsonValue::from(value));
                }
                if let Some(value) = actual.total_time_ms {
                    actual_object.insert("total_time_ms".to_string(), JsonValue::from(value));
                }
            }
            if spec.detail.memory {
                if let Some(value) = actual.runtime.spilled {
                    actual_object.insert("spilled".to_string(), JsonValue::from(value));
                }
                if let Some(value) = actual.runtime.peak_memory_bytes {
                    actual_object.insert("peak_memory_bytes".to_string(), JsonValue::from(value));
                }
                if let Some(value) = actual.runtime.temp_storage_bytes {
                    actual_object.insert("temp_storage_bytes".to_string(), JsonValue::from(value));
                }
                if let Some(value) = actual.runtime.data_plane_bytes {
                    actual_object.insert("data_plane_bytes".to_string(), JsonValue::from(value));
                }
                if !actual.runtime.memory_tag_bytes.is_empty() {
                    actual_object.insert(
                        "memory_tags".to_string(),
                        JsonValue::Array(
                            actual
                                .runtime
                                .memory_tag_bytes
                                .iter()
                                .map(|entry| {
                                    let mut object = Map::new();
                                    object.insert(
                                        "tag".to_string(),
                                        JsonValue::String(entry.tag.clone()),
                                    );
                                    object
                                        .insert("bytes".to_string(), JsonValue::from(entry.bytes));
                                    JsonValue::Object(object)
                                })
                                .collect(),
                        ),
                    );
                }
                if !actual.runtime.domain_memory_tag_bytes.is_empty() {
                    actual_object.insert(
                        "domain_memory_tags".to_string(),
                        JsonValue::Array(
                            actual
                                .runtime
                                .domain_memory_tag_bytes
                                .iter()
                                .map(|entry| {
                                    let mut object = Map::new();
                                    object.insert(
                                        "domain".to_string(),
                                        JsonValue::String(entry.domain.clone()),
                                    );
                                    object.insert(
                                        "tag".to_string(),
                                        JsonValue::String(entry.tag.clone()),
                                    );
                                    object
                                        .insert("bytes".to_string(), JsonValue::from(entry.bytes));
                                    JsonValue::Object(object)
                                })
                                .collect(),
                        ),
                    );
                }
                insert_optional_u64(
                    &mut actual_object,
                    "leaked_grant_bytes",
                    actual.runtime.leaked_grant_bytes,
                );
                insert_optional_u64(
                    &mut actual_object,
                    "local_refill_count",
                    actual.runtime.local_refill_count,
                );
                insert_optional_u64(
                    &mut actual_object,
                    "local_refill_bytes",
                    actual.runtime.local_refill_bytes,
                );
                insert_optional_u64(
                    &mut actual_object,
                    "reclaim_attempt_count",
                    actual.runtime.reclaim_attempt_count,
                );
                insert_optional_u64(
                    &mut actual_object,
                    "reclaimed_bytes",
                    actual.runtime.reclaimed_bytes,
                );
                insert_optional_u64(
                    &mut actual_object,
                    "spilled_bytes",
                    actual.runtime.spilled_bytes,
                );
                insert_optional_u64(
                    &mut actual_object,
                    "reclaim_latency_us",
                    actual.runtime.reclaim_latency_us,
                );
                insert_optional_u64(
                    &mut actual_object,
                    "spill_latency_us",
                    actual.runtime.spill_latency_us,
                );
                insert_optional_u64(
                    &mut actual_object,
                    "allocator_tracking_event_count",
                    actual.runtime.allocator_tracking_event_count,
                );
                insert_optional_u64(
                    &mut actual_object,
                    "selection_materialization_count",
                    actual.runtime.selection_materialization_count,
                );
                insert_optional_u64(
                    &mut actual_object,
                    "scheduler_worker_count",
                    actual.runtime.scheduler_worker_count,
                );
                insert_optional_u64(
                    &mut actual_object,
                    "scheduler_morsel_count",
                    actual.runtime.scheduler_morsel_count,
                );
                insert_optional_u64(
                    &mut actual_object,
                    "scheduler_blocked_count",
                    actual.runtime.scheduler_blocked_count,
                );
                insert_optional_u64(
                    &mut actual_object,
                    "scheduler_wake_count",
                    actual.runtime.scheduler_wake_count,
                );
                insert_optional_u64(
                    &mut actual_object,
                    "scheduler_ready_time_us",
                    actual.runtime.scheduler_ready_time_us,
                );
                insert_optional_u64(
                    &mut actual_object,
                    "scheduler_wait_time_us",
                    actual.runtime.scheduler_wait_time_us,
                );
                insert_optional_u64(
                    &mut actual_object,
                    "scheduler_wake_coalesce_count",
                    actual.runtime.scheduler_wake_coalesce_count,
                );
                insert_optional_u64(
                    &mut actual_object,
                    "output_backpressure_count",
                    actual.runtime.output_backpressure_count,
                );
                insert_optional_u64(
                    &mut actual_object,
                    "runtime_filter_installed_count",
                    actual.runtime.runtime_filter_installed_count,
                );
                insert_optional_u64(
                    &mut actual_object,
                    "runtime_filter_no_wait_count",
                    actual.runtime.runtime_filter_no_wait_count,
                );
                insert_optional_u64(
                    &mut actual_object,
                    "grant_bytes",
                    actual.runtime.grant_bytes,
                );
                insert_optional_u64(
                    &mut actual_object,
                    "revoked_bytes",
                    actual.runtime.revoked_bytes,
                );
                insert_optional_u64(
                    &mut actual_object,
                    "yield_latency_us",
                    actual.runtime.yield_latency_us,
                );
                insert_optional_u64(
                    &mut actual_object,
                    "repartition_depth",
                    actual.runtime.repartition_depth,
                );
                insert_optional_u64(
                    &mut actual_object,
                    "peak_rss_bytes",
                    actual.runtime.peak_rss_bytes,
                );
                insert_optional_u64(
                    &mut actual_object,
                    "output_buffer_bytes",
                    actual.runtime.output_buffer_bytes,
                );
                insert_optional_u64(
                    &mut actual_object,
                    "session_retained_bytes",
                    actual.runtime.session_retained_bytes,
                );
            }
            object.insert("actual".to_string(), JsonValue::Object(actual_object));
        }
        if !self.properties.is_empty() {
            let mut properties = Map::new();
            for property in &self.properties {
                if !spec.detail.verbose && property.label == "Output Schema" {
                    continue;
                }
                let (label, value) = property.to_json_entry();
                properties.insert(label, value);
            }
            if !properties.is_empty() {
                object.insert("properties".to_string(), JsonValue::Object(properties));
            }
        }
        if !self.children.is_empty() {
            object.insert(
                "children".to_string(),
                JsonValue::Array(
                    self.children
                        .iter()
                        .map(|child| child.to_json(spec))
                        .collect(),
                ),
            );
        }
        JsonValue::Object(object)
    }
}

fn insert_optional_u64(object: &mut Map<String, JsonValue>, key: &str, value: Option<u64>) {
    if let Some(value) = value {
        object.insert(key.to_string(), JsonValue::from(value));
    }
}

#[derive(Debug, Clone)]
pub struct ExplainDoc {
    pub format_version: u32,
    pub spec: ExplainSpec,
    pub root: ExplainNode,
    pub summary: Vec<ExplainProperty>,
}

impl ExplainDoc {
    pub fn to_json(&self) -> JsonValue {
        let mut object = Map::new();
        object.insert(
            "format_version".to_string(),
            JsonValue::from(self.format_version),
        );
        object.insert(
            "mode".to_string(),
            JsonValue::String(format!("{:?}", self.spec.mode).to_ascii_lowercase()),
        );
        object.insert(
            "format".to_string(),
            JsonValue::String(format!("{:?}", self.spec.format).to_ascii_lowercase()),
        );
        let mut detail = Map::new();
        detail.insert(
            "verbose".to_string(),
            JsonValue::from(self.spec.detail.verbose),
        );
        detail.insert(
            "summary".to_string(),
            JsonValue::from(self.spec.detail.summary),
        );
        detail.insert(
            "timing".to_string(),
            JsonValue::from(self.spec.detail.timing),
        );
        detail.insert(
            "memory".to_string(),
            JsonValue::from(self.spec.detail.memory),
        );
        object.insert("detail".to_string(), JsonValue::Object(detail));
        object.insert("plan".to_string(), self.root.to_json(&self.spec));
        if self.spec.detail.summary && !self.summary.is_empty() {
            let mut summary = Map::new();
            for property in &self.summary {
                let (label, value) = property.to_json_entry();
                summary.insert(label, value);
            }
            object.insert("summary".to_string(), JsonValue::Object(summary));
        }
        JsonValue::Object(object)
    }
}

pub fn format_bytes(value: u64) -> String {
    const UNITS: [&str; 5] = ["B", "kB", "MB", "GB", "TB"];
    let mut value_f = value as f64;
    let mut unit_idx = 0usize;
    while value_f >= 1024.0 && unit_idx + 1 < UNITS.len() {
        value_f /= 1024.0;
        unit_idx += 1;
    }
    if unit_idx == 0 {
        format!("{} {}", value, UNITS[unit_idx])
    } else {
        format!("{value_f:.1} {}", UNITS[unit_idx])
    }
}
