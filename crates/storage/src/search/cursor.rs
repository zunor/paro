// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;
use std::sync::Arc;

use crate::rowset::{RowsetId, RowsetSharedPtr, SegmentSharedPtr};
use crate::tablet::{TabletReadGuard, TabletRef};
use crate::transaction::overlay_reader::OverlayDeleteVectorMap;
use paro_common::error::{self as paro_error, Result};
use paro_transaction::{DerivedLagLease, RetentionLeaseInfo};

use super::budget::{ResourceBudget, SearchBatchConfig};
use super::capability::{CoverageState, SearchArtifactRef, SearchIndexKind};
use super::request::NormalizedSearchRequest;
use super::stats::{
    BuildEpoch, GenerationMaintenanceState, GenerationStats, SearchDefinitionId,
    SearchGenerationId, SearchSourceId, SegmentId, TableId,
};
use super::tail::exact_merge::TailWindow;

pub use crate::rowset::PhysicalRowRef;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableReadSnapshot {
    pub table_id: TableId,
    pub tablet_id: u64,
    pub visible_version: i64,
}

#[derive(Debug, Clone)]
pub(crate) struct VisibleSegment {
    pub(crate) rowset: RowsetSharedPtr,
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
    overlay_rowset_ids: Arc<Vec<RowsetId>>,
}

impl TableReadLease {
    pub(crate) fn open(
        tablet: &TabletRef,
        table_id: TableId,
        visible_version: i64,
    ) -> Result<(TableReadSnapshot, Arc<Self>)> {
        let guard = Arc::new(TabletReadGuard::pin(tablet, visible_version)?);
        let rowsets = tablet.capture_consistent_rowsets(visible_version)?;
        Self::from_pinned_rowsets(
            tablet,
            table_id,
            visible_version,
            guard,
            rowsets,
            Vec::new(),
        )
    }

    pub(crate) fn open_with_overlay_rowsets(
        tablet: &TabletRef,
        table_id: TableId,
        visible_version: i64,
        overlay_rowsets: Vec<RowsetSharedPtr>,
    ) -> Result<(TableReadSnapshot, Arc<Self>)> {
        let guard = Arc::new(TabletReadGuard::pin(tablet, visible_version)?);
        let mut rowsets = tablet.capture_consistent_rowsets(visible_version)?;
        let mut overlay_rowset_ids: Vec<_> = overlay_rowsets
            .iter()
            .map(|rowset| rowset.rowset_id())
            .collect();
        overlay_rowset_ids.sort_unstable();
        overlay_rowset_ids.dedup();
        rowsets.extend(overlay_rowsets);
        Self::from_pinned_rowsets(
            tablet,
            table_id,
            visible_version,
            guard,
            rowsets,
            overlay_rowset_ids,
        )
    }

    fn from_pinned_rowsets(
        tablet: &TabletRef,
        table_id: TableId,
        visible_version: i64,
        guard: Arc<TabletReadGuard>,
        rowsets: Vec<RowsetSharedPtr>,
        overlay_rowset_ids: Vec<RowsetId>,
    ) -> Result<(TableReadSnapshot, Arc<Self>)> {
        let mut visible_segments = Vec::new();
        let mut segment_index = HashMap::new();
        for rowset in rowsets {
            rowset.load()?;
            let rowset_id = rowset.rowset_id();
            for segment in rowset.segments() {
                let segment_id = segment.segment_id();
                let entry = VisibleSegment {
                    rowset: rowset.clone(),
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
            overlay_rowset_ids: Arc::new(overlay_rowset_ids),
        });
        Ok((snapshot, lease))
    }

    pub(crate) fn visible_segments(&self) -> &[VisibleSegment] {
        self.visible_segments.as_slice()
    }

    pub fn visible_segment_count(&self) -> usize {
        self.visible_segments.len()
    }

    pub(crate) fn is_overlay_rowset(&self, rowset_id: RowsetId) -> bool {
        self.overlay_rowset_ids.binary_search(&rowset_id).is_ok()
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
            .field("overlay_rowset_count", &self.overlay_rowset_ids.len())
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
    pub indexed_through_ts: u64,
    pub coverage: CoverageState,
    pub generation_stats: GenerationStats,
    pub maintenance_state: GenerationMaintenanceState,
    pub artifacts: Arc<GenerationArtifactSet>,
}

#[derive(Debug, Clone)]
pub struct SearchReadSnapshot {
    pub table: TableReadSnapshot,
    pub provider_kind: SearchIndexKind,
    pub generation: GenerationReadSnapshot,
    pub table_lease: Arc<TableReadLease>,
    pub generation_lease: Arc<GenerationReadLease>,
    derived_lag_lease: Option<Arc<DerivedLagLease>>,
    overlay_delete_vectors: Option<Arc<OverlayDeleteVectorMap>>,
    tail_window: TailWindow,
}

impl SearchReadSnapshot {
    pub fn new(
        table: TableReadSnapshot,
        provider_kind: SearchIndexKind,
        generation: GenerationReadSnapshot,
        table_lease: Arc<TableReadLease>,
        generation_lease: Arc<GenerationReadLease>,
    ) -> Self {
        let tail_window = TailWindow::from_segments(
            generation.indexed_through_ts,
            table.visible_version,
            table_lease.visible_segments(),
            |rowset_id| table_lease.is_overlay_rowset(rowset_id),
        );
        Self {
            table,
            provider_kind,
            generation,
            table_lease,
            generation_lease,
            derived_lag_lease: None,
            overlay_delete_vectors: None,
            tail_window,
        }
    }

    pub fn with_derived_lag_lease(mut self, lease: Option<Arc<DerivedLagLease>>) -> Self {
        self.derived_lag_lease = lease;
        self
    }

    pub fn derived_lag_lease_info(&self) -> Result<Option<RetentionLeaseInfo>> {
        self.derived_lag_lease
            .as_ref()
            .map(|lease| lease.info())
            .transpose()
            .map_err(|err| paro_error::internal(format!("derived lag lease info: {err}")))
    }

    pub(crate) fn with_overlay_delete_vectors(
        mut self,
        delete_vectors: Option<Arc<OverlayDeleteVectorMap>>,
    ) -> Self {
        self.overlay_delete_vectors = delete_vectors;
        self
    }

    #[inline]
    pub(crate) fn has_overlay_delete_vectors(&self) -> bool {
        self.overlay_delete_vectors.is_some()
    }

    pub fn tail_window(&self) -> TailWindow {
        self.tail_window
    }

    pub(crate) fn artifact_for_segment(
        &self,
        kind: SearchIndexKind,
        column_id: u32,
        segment: &VisibleSegment,
    ) -> Option<&SearchArtifactRef> {
        self.generation.artifacts.artifacts.iter().find(|artifact| {
            artifact.kind == kind
                && artifact.column_id == column_id
                && artifact.segment.rowset_id == segment.rowset_id
                && artifact.segment.segment_id == segment.segment_id
        })
    }

    pub(crate) fn is_tail_segment(&self, segment: &VisibleSegment) -> bool {
        if self.table_lease.is_overlay_rowset(segment.rowset_id) {
            return true;
        }
        let rowset_end = segment.rowset.end_version();
        rowset_end >= 0
            && (rowset_end as u64) > self.generation.indexed_through_ts
            && (rowset_end as u64) <= self.table.visible_version.max(0) as u64
    }

    #[inline]
    pub(crate) fn is_overlay_deleted(&self, row: PhysicalRowRef) -> bool {
        self.overlay_delete_vectors
            .as_ref()
            .and_then(|delete_vectors| delete_vectors.get(&(row.rowset_id, row.segment_id)))
            .is_some_and(|delete_vector| delete_vector.is_deleted(row.row_offset))
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

#[derive(Debug, Clone, PartialEq)]
pub enum OpenSearchCursorResult<T> {
    Opened(T),
    CapabilityTokenStale,
    NotQueryable,
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
    use crate::search::capability::{CoverageState, SearchIndexKind};
    use crate::search::stats::{GenerationMaintenanceState, GenerationStats};
    use crate::table::table_factory::TableFactory;
    use paro_common::types::LogicalType;
    use paro_transaction::{CommitTs, RetentionLeaseKind, RetentionRegistry};

    #[test]
    fn physical_row_ref_round_trips_with_existing_tablet_type() {
        let row = PhysicalRowRef::new(7, 3, crate::rowset::SegmentRowId::from_raw(11));
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
        let err = CandidateBatch::try_new(
            vec![PhysicalRowRef::new(
                1,
                0,
                crate::rowset::SegmentRowId::from_raw(0),
            )],
            vec![0.1, 0.2],
        );
        assert!(err.is_err());

        let filter_batch = CandidateBatch::try_new(
            vec![PhysicalRowRef::new(
                1,
                0,
                crate::rowset::SegmentRowId::from_raw(0),
            )],
            vec![],
        );
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
            indexed_through_ts: visible_version.max(0) as u64,
            coverage: CoverageState::Complete,
            generation_stats: GenerationStats::default(),
            maintenance_state: GenerationMaintenanceState::default(),
            artifacts: Arc::new(GenerationArtifactSet::default()),
        };
        let generation_lease = GenerationReadLease::from_snapshot(&generation);

        let registry = RetentionRegistry::with_capacity(1, 2);
        let derived_lag_target = visible_version.max(3) as u64;
        let derived_lag_lease = Arc::new(
            registry
                .lease_derived_lag_range(CommitTs::new(3), CommitTs::new(derived_lag_target))
                .expect("lease derived lag"),
        );
        let snapshot = SearchReadSnapshot::new(
            table_snapshot,
            SearchIndexKind::Hnsw,
            generation,
            table_lease,
            generation_lease,
        )
        .with_derived_lag_lease(Some(derived_lag_lease));
        assert_eq!(snapshot.table_lease.table_id, table.tablet_id());
        assert_eq!(
            snapshot.table_lease.pinned_visible_version(),
            visible_version
        );
        assert_eq!(snapshot.generation_lease.definition_id, 5);
        assert_eq!(snapshot.generation_lease.generation_id, 6);
        assert_eq!(snapshot.generation_lease.build_epoch, 7);
        assert_eq!(snapshot.generation_lease.artifact_count(), 0);
        let lease_info = snapshot
            .derived_lag_lease_info()
            .expect("lease info")
            .expect("derived lag lease");
        assert_eq!(lease_info.kind, RetentionLeaseKind::DerivedLag);
        assert_eq!(lease_info.commit_ts_floor, Some(CommitTs::new(3)));
        assert_eq!(
            lease_info.commit_ts_ceiling,
            Some(CommitTs::new(derived_lag_target))
        );
    }
}
