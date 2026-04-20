// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::time::Duration;

use crate::rowset::RowsetId;

use super::capability::CoverageState;
use super::capability::SearchIndexKind;
use super::stats::{BuildEpoch, SearchDefinitionId, SearchGenerationId, SegmentId};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryTelemetryEvent {
    pub kind: SearchIndexKind,
    pub segments_searched: usize,
    pub candidates_produced: usize,
    pub rows_returned: usize,
    pub peak_heap_items: usize,
    pub degraded_segments: usize,
    pub elapsed: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentTelemetryEvent {
    pub kind: SearchIndexKind,
    pub rowset_id: RowsetId,
    pub segment_id: SegmentId,
    pub candidates_produced: usize,
    pub degraded: bool,
    pub elapsed: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GenerationTelemetryEvent {
    pub kind: SearchIndexKind,
    pub definition_id: SearchDefinitionId,
    pub generation_id: SearchGenerationId,
    pub build_epoch: BuildEpoch,
    pub coverage: CoverageState,
    pub artifact_count: usize,
}

pub trait SearchTelemetryCollector: Send + Sync {
    fn record_query(&self, _event: QueryTelemetryEvent) {}

    fn record_segment_search(&self, _event: SegmentTelemetryEvent) {}

    fn record_generation(&self, _event: GenerationTelemetryEvent) {}
}

#[derive(Debug, Default)]
pub struct NoopSearchTelemetryCollector;

impl SearchTelemetryCollector for NoopSearchTelemetryCollector {}
