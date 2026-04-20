// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;
use std::sync::Arc;

use crate::rowset::{RowsetId, SegmentSharedPtr};
use crate::tablet::{TabletReadGuard, TabletRef};
use paro_common::error::{self as paro_error, Result};

use super::budget::{ResourceBudget, SearchBatchConfig};
use super::capability::{CoverageState, SearchArtifactRef};
use super::request::NormalizedSearchRequest;
use super::stats::{
    BuildEpoch, GenerationMaintenanceState, GenerationStats, SearchDefinitionId,
    SearchGenerationId, SearchSourceId, SegmentId, TableId,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableReadSnapshot {
    pub table_id: TableId,
    pub tablet_id: u64,
    pub visible_version: i64,
}

#[derive(Debug, Clone)]
pub(crate) struct VisibleSegment {
    pub(crate) rowset_id: RowsetId,
    pub(crate) segment_id: SegmentId,
    pub(crate) segment: SegmentSharedPtr,
}

impl VisibleSegment {
    pub(crate) const fn key(&self) -> (RowsetId, SegmentId) {
        (self.rowset_id, self.segment_id)
    }
}

#[derive(Clone)]
pub struct TableReadLease {
    pub table_id: TableId,
    pub tablet_id: u64,
    pub visible_version: i64,
    guard: Arc<TabletReadGuard>,
    visible_segments: Arc<Vec<VisibleSegment>>,
    segment_index: Arc<HashMap<(RowsetId, SegmentId), SegmentSharedPtr>>,
}

impl TableReadLease {
    pub(crate) fn open(
        tablet: &TabletRef,
        table_id: TableId,
        visible_version: i64,
    ) -> Result<(TableReadSnapshot, Arc<Self>)> {
        let guard = Arc::new(TabletReadGuard::pin(tablet, visible_version));
        let rowsets = tablet.capture_consistent_rowsets(visible_version)?;
        let mut visible_segments = Vec::new();
        let mut segment_index = HashMap::new();

        for rowset in rowsets {
            rowset.load()?;
            let rowset_id = rowset.rowset_id();
            for segment in rowset.segments() {
                let segment_id = segment.segment_id();
                let entry = VisibleSegment {
                    rowset_id,
                    segment_id,
                    segment,
                };
                segment_index.insert(entry.key(), entry.segment.clone());
                visible_segments.push(entry);
            }
        }

        let snapshot = TableReadSnapshot {
            table_id,
            tablet_id: tablet.tablet_id(),
            visible_version,
        };
        let lease = Arc::new(Self {
            table_id,
            tablet_id: tablet.tablet_id(),
            visible_version,
            guard,
            visible_segments: Arc::new(visible_segments),
            segment_index: Arc::new(segment_index),
        });
        Ok((snapshot, lease))
    }

    pub(crate) fn visible_segments(&self) -> &[VisibleSegment] {
        self.visible_segments.as_slice()
    }

    pub fn visible_segment_count(&self) -> usize {
        self.visible_segments.len()
    }

    pub fn resolve_segment(&self, row: PhysicalRowRef) -> Result<SegmentSharedPtr> {
        self.segment_index
            .get(&row.segment_key())
            .cloned()
            .ok_or_else(|| {
                paro_error::internal(format!(
                    "Search snapshot missing segment ({}, {})",
                    row.rowset_id, row.segment_id
                ))
            })
    }

    pub fn pinned_visible_version(&self) -> i64 {
        self.guard.visible_version()
    }
}

impl std::fmt::Debug for TableReadLease {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TableReadLease")
            .field("table_id", &self.table_id)
            .field("tablet_id", &self.tablet_id)
            .field("visible_version", &self.visible_version)
            .field("pinned_visible_version", &self.guard.visible_version())
            .field("visible_segment_count", &self.visible_segments.len())
            .finish()
    }
}

impl PartialEq for TableReadLease {
    fn eq(&self, other: &Self) -> bool {
        self.table_id == other.table_id
            && self.tablet_id == other.tablet_id
            && self.visible_version == other.visible_version
            && self.visible_segments.len() == other.visible_segments.len()
    }
}

impl Eq for TableReadLease {}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct GenerationArtifactSet {
    pub artifacts: Vec<SearchArtifactRef>,
}

#[derive(Clone)]
pub struct GenerationReadLease {
    pub definition_id: SearchDefinitionId,
    pub generation_id: SearchGenerationId,
    pub build_epoch: BuildEpoch,
    artifacts: Arc<GenerationArtifactSet>,
}

impl GenerationReadLease {
    pub fn from_snapshot(snapshot: &GenerationReadSnapshot) -> Arc<Self> {
        Arc::new(Self {
            definition_id: snapshot.definition_id,
            generation_id: snapshot.generation_id,
            build_epoch: snapshot.build_epoch,
            artifacts: snapshot.artifacts.clone(),
        })
    }

    pub fn artifact_count(&self) -> usize {
        self.artifacts.artifacts.len()
    }
}

impl std::fmt::Debug for GenerationReadLease {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GenerationReadLease")
            .field("definition_id", &self.definition_id)
            .field("generation_id", &self.generation_id)
            .field("build_epoch", &self.build_epoch)
            .field("artifact_count", &self.artifacts.artifacts.len())
            .finish()
    }
}

impl PartialEq for GenerationReadLease {
    fn eq(&self, other: &Self) -> bool {
        self.definition_id == other.definition_id
            && self.generation_id == other.generation_id
            && self.build_epoch == other.build_epoch
            && Arc::ptr_eq(&self.artifacts, &other.artifacts)
    }
}

impl Eq for GenerationReadLease {}

#[derive(Debug, Clone, PartialEq)]
pub struct GenerationReadSnapshot {
    pub definition_id: SearchDefinitionId,
    pub generation_id: SearchGenerationId,
    pub build_epoch: BuildEpoch,
    pub build_snapshot_version: i64,
    pub coverage: CoverageState,
    pub generation_stats: GenerationStats,
    pub maintenance_state: GenerationMaintenanceState,
    pub artifacts: Arc<GenerationArtifactSet>,
}

#[derive(Debug, Clone)]
pub struct SearchReadSnapshot {
    pub table: TableReadSnapshot,
    pub generation: GenerationReadSnapshot,
    pub table_lease: Arc<TableReadLease>,
    pub generation_lease: Arc<GenerationReadLease>,
}

impl SearchReadSnapshot {
    pub fn new(
        table: TableReadSnapshot,
        generation: GenerationReadSnapshot,
        table_lease: Arc<TableReadLease>,
        generation_lease: Arc<GenerationReadLease>,
    ) -> Self {
        Self {
            table,
            generation,
            table_lease,
            generation_lease,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PhysicalRowRef {
    pub rowset_id: RowsetId,
    pub segment_id: SegmentId,
    pub row_id: u32,
}

impl PhysicalRowRef {
    pub const fn new(rowset_id: RowsetId, segment_id: SegmentId, row_id: u32) -> Self {
        Self {
            rowset_id,
            segment_id,
            row_id,
        }
    }

    pub const fn segment_key(self) -> (RowsetId, SegmentId) {
        (self.rowset_id, self.segment_id)
    }
}

impl From<crate::tablet::PhysicalRowRef> for PhysicalRowRef {
    fn from(value: crate::tablet::PhysicalRowRef) -> Self {
        Self::new(value.rowset_id, value.segment_id, value.row_offset)
    }
}

impl From<PhysicalRowRef> for crate::tablet::PhysicalRowRef {
    fn from(value: PhysicalRowRef) -> Self {
        Self::new(value.rowset_id, value.segment_id, value.row_id)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SearchRowHandle {
    pub source_id: SearchSourceId,
    pub row: PhysicalRowRef,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct CandidateBatch {
    pub rows: Vec<PhysicalRowRef>,
    pub scores: Vec<f32>,
}

impl CandidateBatch {
    pub fn try_new(rows: Vec<PhysicalRowRef>, scores: Vec<f32>) -> Result<Self> {
        if !scores.is_empty() && scores.len() != rows.len() {
            return Err(paro_error::invalid_input(format!(
                "CandidateBatch rows/scores length mismatch ({} vs {})",
                rows.len(),
                scores.len()
            )));
        }
        Ok(Self { rows, scores })
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum SearchBatchState {
    Ready(CandidateBatch),
    Exhausted,
}

pub struct OpenedSearchCursor {
    pub snapshot: SearchReadSnapshot,
    pub cursor: Box<dyn SearchCursor>,
}

impl std::fmt::Debug for OpenedSearchCursor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OpenedSearchCursor")
            .field("snapshot", &self.snapshot)
            .field("cursor", &"<dyn SearchCursor>")
            .finish()
    }
}

pub trait SearchCursor: Send {
    fn next_batch(
        &mut self,
        batch: &SearchBatchConfig,
        budget: &mut ResourceBudget,
    ) -> Result<SearchBatchState>;
}

pub trait SearchProvider: Send + Sync {
    fn open_cursor(
        &self,
        request: &NormalizedSearchRequest,
        snapshot: SearchReadSnapshot,
    ) -> Result<Box<dyn SearchCursor>>;
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{
        CandidateBatch, GenerationArtifactSet, GenerationReadLease, GenerationReadSnapshot,
        PhysicalRowRef, SearchReadSnapshot, TableReadLease,
    };
    use crate::search::capability::CoverageState;
    use crate::search::stats::{GenerationMaintenanceState, GenerationStats};
    use crate::table::table_factory::TableFactory;
    use paro_common::types::LogicalType;

    #[test]
    fn physical_row_ref_round_trips_with_existing_tablet_type() {
        let row = PhysicalRowRef::new(7, 3, 11);
        let tablet_row: crate::tablet::PhysicalRowRef = row.into();
        assert_eq!(tablet_row.rowset_id, 7);
        assert_eq!(tablet_row.segment_id, 3);
        assert_eq!(tablet_row.row_offset, 11);

        let restored = PhysicalRowRef::from(tablet_row);
        assert_eq!(restored, row);
        assert_eq!(restored.segment_key(), (7, 3));
    }

    #[test]
    fn candidate_batch_rejects_misaligned_scores() {
        let err = CandidateBatch::try_new(vec![PhysicalRowRef::new(1, 0, 0)], vec![0.1, 0.2]);
        assert!(err.is_err());

        let filter_batch = CandidateBatch::try_new(vec![PhysicalRowRef::new(1, 0, 0)], vec![]);
        assert!(filter_batch.is_ok());
        assert_eq!(filter_batch.unwrap().len(), 1);
    }

    #[test]
    fn search_read_snapshot_uses_real_leases() {
        let table = TableFactory::default()
            .create_table(&[LogicalType::Integer])
            .expect("create table");
        let visible_version = table.max_version();
        let (table_snapshot, table_lease) =
            TableReadLease::open(&table.tablet(), table.tablet_id(), visible_version)
                .expect("open table lease");
        let generation = GenerationReadSnapshot {
            definition_id: 5,
            generation_id: 6,
            build_epoch: 7,
            build_snapshot_version: visible_version,
            coverage: CoverageState::Complete,
            generation_stats: GenerationStats::default(),
            maintenance_state: GenerationMaintenanceState::default(),
            artifacts: Arc::new(GenerationArtifactSet::default()),
        };
        let generation_lease = GenerationReadLease::from_snapshot(&generation);

        let snapshot =
            SearchReadSnapshot::new(table_snapshot, generation, table_lease, generation_lease);
        assert_eq!(snapshot.table_lease.table_id, table.tablet_id());
        assert_eq!(
            snapshot.table_lease.pinned_visible_version(),
            visible_version
        );
        assert_eq!(snapshot.generation_lease.definition_id, 5);
        assert_eq!(snapshot.generation_lease.generation_id, 6);
        assert_eq!(snapshot.generation_lease.build_epoch, 7);
        assert_eq!(snapshot.generation_lease.artifact_count(), 0);
    }
}
