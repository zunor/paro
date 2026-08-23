// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::compaction::plan::types::{CompactionReason, CumulativePointAction, PolicyKind};
use crate::rowset::RowsetSharedPtr;
use crate::tablet::Tablet;
use paro_common::error::{self as paro_error, Result};

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CompactionDecision {
    pub score: f64,
    pub policy_kind: PolicyKind,
    pub cumulative_point_action: CumulativePointAction,
    pub reason: CompactionReason,
}

pub trait CompactionPolicy: Send + Sync {
    fn select(&self, tablet: &Tablet) -> Result<Option<CompactionDecision>>;

    fn pick_rowsets(&self, tablet: &Tablet) -> Result<Vec<RowsetSharedPtr>>;
}

pub struct SizeTieredCompactionPolicy {
    min_segments: u32,
    level_multiple: f64,
    min_level_size: u64,
}

impl SizeTieredCompactionPolicy {
    pub fn new() -> Self {
        Self {
            min_segments: 2,
            level_multiple: 5.0,
            min_level_size: 64 * 1024 * 1024,
        }
    }

    fn calculate_score(&self, rowsets: &[RowsetSharedPtr]) -> f64 {
        if rowsets.is_empty() {
            return 0.0;
        }

        let mut segment_num = 0;
        let mut total_size = 0;
        let mut deleted_rows = 0;
        let mut total_rows = 0;

        for rs in rowsets {
            let meta = rs.rowset_meta();
            segment_num += meta.num_segments();
            total_size += meta.total_disk_size();
            deleted_rows += meta.num_deleted_rows();
            total_rows += meta.num_rows();
        }

        let mut score = segment_num as f64;
        if total_rows > 0 {
            let delete_ratio = deleted_rows as f64 / total_rows as f64;
            if delete_ratio > 0.1 {
                score += delete_ratio * 50.0;
            }
        }

        let size_mb = total_size as f64 / (1024.0 * 1024.0);
        if size_mb < 256.0 {
            score += (1.0 - size_mb / 256.0) * 10.0;
        }

        score
    }
}

impl CompactionPolicy for SizeTieredCompactionPolicy {
    fn select(&self, tablet: &Tablet) -> Result<Option<CompactionDecision>> {
        let rowsets = self.pick_rowsets(tablet)?;
        if rowsets.is_empty() {
            return Ok(None);
        }

        let score = self.calculate_score(&rowsets);
        if score < self.min_segments as f64 {
            return Ok(None);
        }

        let cumulative_point_action = if rowsets
            .last()
            .is_some_and(|rowset| rowset.end_version() < tablet.cumulative_point())
        {
            CumulativePointAction::Preserve
        } else {
            CumulativePointAction::AdvanceToOutputEndExclusive
        };

        Ok(Some(CompactionDecision {
            score,
            policy_kind: PolicyKind::SizeTiered,
            cumulative_point_action,
            reason: CompactionReason::SizeTieredPolicy,
        }))
    }

    fn pick_rowsets(&self, tablet: &Tablet) -> Result<Vec<RowsetSharedPtr>> {
        let mut rowsets = tablet.capture_consistent_rowsets(tablet.max_version())?;
        if rowsets.len() < self.min_segments as usize {
            return Ok(Vec::new());
        }

        rowsets.sort_by_key(|rs| rs.start_version());

        let mut current_group = Vec::new();
        let mut current_level_size = 0u64;

        for rs in &rowsets {
            let rs_size = rs.total_disk_size();
            let effective_size = rs_size.max(self.min_level_size / 10);

            if current_group.is_empty() {
                current_group.push(rs.clone());
                current_level_size = effective_size;
                continue;
            }

            if rs_size as f64 > current_level_size as f64 * self.level_multiple {
                if current_group.len() >= self.min_segments as usize {
                    break;
                }

                current_group.clear();
                current_group.push(rs.clone());
                current_level_size = effective_size;
            } else {
                current_group.push(rs.clone());
                current_level_size = current_level_size.max(effective_size);
            }
        }

        if current_group.len() >= self.min_segments as usize {
            Ok(current_group)
        } else {
            Ok(Vec::new())
        }
    }
}

pub struct BaseCompactionPolicy {
    min_newer_rowsets: usize,
    newer_bytes_escape: u64,
}

impl BaseCompactionPolicy {
    pub fn new() -> Self {
        Self {
            min_newer_rowsets: 4,
            newer_bytes_escape: 256 * 1024 * 1024,
        }
    }
}

impl CompactionPolicy for BaseCompactionPolicy {
    fn select(&self, tablet: &Tablet) -> Result<Option<CompactionDecision>> {
        let rowsets = self.pick_rowsets(tablet)?;
        if rowsets.is_empty() {
            return Ok(None);
        }

        Ok(Some(CompactionDecision {
            score: rowsets.len() as f64,
            policy_kind: PolicyKind::Base,
            cumulative_point_action: CumulativePointAction::Preserve,
            reason: CompactionReason::BasePolicy,
        }))
    }

    fn pick_rowsets(&self, tablet: &Tablet) -> Result<Vec<RowsetSharedPtr>> {
        let mut rowsets = tablet.capture_consistent_rowsets(tablet.max_version())?;
        if rowsets.is_empty() {
            return Ok(Vec::new());
        }

        rowsets.sort_by_key(|rs| rs.start_version());
        let cumulative_point = tablet.cumulative_point();
        let mut candidates = Vec::new();
        for rs in rowsets {
            if rs.end_version() < cumulative_point {
                candidates.push(rs);
            } else {
                break;
            }
        }

        if candidates.len() <= 1 {
            return Ok(Vec::new());
        }

        // A prior base output owns the earliest visible tablet prefix; global
        // commit versions do not require that prefix to begin at zero. Rate-limit
        // its next full rewrite using deterministic version-space progress,
        // not wall time. A byte escape prevents large deltas from waiting on a
        // rowset-count threshold.
        if candidates[0].rowset_meta().is_compaction_output() {
            let newer = &candidates[1..];
            let newer_bytes = newer.iter().fold(0_u64, |total, rowset| {
                total.saturating_add(rowset.total_disk_size())
            });
            if newer.len() < self.min_newer_rowsets && newer_bytes < self.newer_bytes_escape {
                return Ok(Vec::new());
            }
        }
        Ok(candidates)
    }
}

pub struct CumulativeCompactionPolicy {
    min_delta_rowsets: usize,
    max_delta_rowsets: usize,
}

impl CumulativeCompactionPolicy {
    pub fn new() -> Self {
        Self {
            // Two contiguous deltas are useful merge work. Base compaction is
            // independently rate-limited, so advancing the cumulative point
            // here cannot trigger an unbounded full-base rewrite loop.
            min_delta_rowsets: 2,
            max_delta_rowsets: 1000,
        }
    }
}

impl CompactionPolicy for CumulativeCompactionPolicy {
    fn select(&self, tablet: &Tablet) -> Result<Option<CompactionDecision>> {
        let rowsets = self.pick_rowsets(tablet)?;
        if rowsets.is_empty() {
            return Ok(None);
        }

        let score: f64 = rowsets
            .iter()
            .map(|rs| rs.rowset_meta().get_compaction_score())
            .sum();
        Ok(Some(CompactionDecision {
            score,
            policy_kind: PolicyKind::Cumulative,
            cumulative_point_action: CumulativePointAction::AdvanceToOutputEndExclusive,
            reason: CompactionReason::CumulativePolicy,
        }))
    }

    fn pick_rowsets(&self, tablet: &Tablet) -> Result<Vec<RowsetSharedPtr>> {
        let mut rowsets = tablet.capture_consistent_rowsets(tablet.max_version())?;
        if rowsets.is_empty() {
            return Ok(Vec::new());
        }

        rowsets.sort_by_key(|rs| rs.start_version());

        let cumulative_point = tablet.cumulative_point();
        let mut candidates = Vec::new();
        // -1 is the durable sentinel used before the first cumulative
        // compaction. Global commit versions may contain holes where this
        // tablet had no write, so a sentinel does not imply that its first
        // rowset starts at zero.
        let mut output_end_exclusive = cumulative_point.max(0);

        for rs in rowsets {
            if rs.end_version() < cumulative_point {
                continue;
            }
            // Never skip a delete-bearing or otherwise inconvenient rowset and
            // then move the cumulative point beyond it. Numeric commit-version
            // holes are valid when this tablet had no write in those commits.
            if cumulative_point >= 0
                && rs.start_version() < cumulative_point
                && rs.end_version() >= cumulative_point
            {
                return Err(paro_error::data_corrupted(format!(
                    "rowset version [{}, {}] crosses the cumulative point {cumulative_point}",
                    rs.start_version(),
                    rs.end_version(),
                )));
            }
            if !candidates.is_empty() && rs.start_version() < output_end_exclusive {
                return Err(paro_error::data_corrupted(format!(
                    "rowset version [{}, {}] overlaps an earlier cumulative candidate ending at {}",
                    rs.start_version(),
                    rs.end_version(),
                    output_end_exclusive - 1,
                )));
            }
            if candidates.len() >= self.max_delta_rowsets {
                break;
            }

            output_end_exclusive = match rs.end_version().checked_add(1) {
                Some(next) => next,
                None => break,
            };
            candidates.push(rs);
        }

        // Cumulative compaction merges delta rowsets. A rowset's segment/size
        // priority score must never turn one freshly published rowset into a
        // self-rewrite; intra-rowset layout is a writer responsibility.
        if candidates.len() >= self.min_delta_rowsets {
            Ok(candidates)
        } else {
            Ok(Vec::new())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use paro_common::types::LogicalType;

    use super::*;
    use crate::rowset::{Rowset, RowsetMeta};
    use crate::tablet::{KeysType, TabletColumn, TabletSchema, Version};

    fn test_tablet() -> (tempfile::TempDir, Tablet) {
        let temp = tempfile::tempdir().unwrap();
        let schema = Arc::new(
            TabletSchema::new(
                1,
                vec![TabletColumn::key(0, "id", LogicalType::BigInt)],
                KeysType::PrimaryKeys,
            )
            .unwrap(),
        );
        let tablet = Tablet::new(1, 1, 0, schema, temp.path(), None).unwrap();
        (temp, tablet)
    }

    fn add_rowset(tablet: &Tablet, id: u64, version: i64, deleted_rows: u64) {
        let mut meta = RowsetMeta::new(id, tablet.tablet_id(), Version::singleton(version));
        if deleted_rows > 0 {
            meta.set_delete_info(1, deleted_rows);
        }
        let rowset = Rowset::create(
            tablet.schema().expect("test tablet schema"),
            meta,
            tablet.data_dir().join(format!("rowset-{id}")),
        )
        .unwrap();
        tablet.add_rowset(Arc::new(rowset)).unwrap();
    }

    #[test]
    fn cumulative_policy_normalizes_initial_sentinel_and_keeps_delete_versions() {
        let (_temp, tablet) = test_tablet();
        assert_eq!(tablet.cumulative_point(), -1);
        for version in 0..5 {
            add_rowset(&tablet, version as u64 + 1, version, (version == 2) as u64);
        }

        let selected = CumulativeCompactionPolicy::new()
            .pick_rowsets(&tablet)
            .unwrap();
        assert_eq!(
            selected
                .iter()
                .map(|rowset| rowset.start_version())
                .collect::<Vec<_>>(),
            vec![0, 1, 2, 3, 4]
        );
        assert_eq!(selected[2].rowset_meta().num_deleted_rows(), 1);
    }

    #[test]
    fn base_rewrite_waits_for_deterministic_version_progress() {
        let (_temp, tablet) = test_tablet();
        let mut base = RowsetMeta::new(1, tablet.tablet_id(), Version::new(4, 5));
        base.set_compaction_output(vec![10, 11]);
        tablet
            .add_rowset(Arc::new(
                Rowset::create(
                    tablet.schema().expect("test tablet schema"),
                    base,
                    tablet.data_dir().join("base-output"),
                )
                .unwrap(),
            ))
            .unwrap();
        for (id, version) in [(2, 7), (3, 9), (4, 12)] {
            add_rowset(&tablet, id, version, 0);
        }
        tablet.set_cumulative_point(13);

        assert!(BaseCompactionPolicy::new()
            .pick_rowsets(&tablet)
            .unwrap()
            .is_empty());

        add_rowset(&tablet, 5, 14, 0);
        tablet.set_cumulative_point(15);
        assert_eq!(
            BaseCompactionPolicy::new()
                .pick_rowsets(&tablet)
                .unwrap()
                .len(),
            5
        );
    }

    #[test]
    fn cumulative_policy_rejects_rowset_crossing_cumulative_point() {
        let (_temp, tablet) = test_tablet();
        let meta = RowsetMeta::new(1, tablet.tablet_id(), Version::new(0, 2));
        tablet
            .add_rowset(Arc::new(
                Rowset::create(
                    tablet.schema().expect("test tablet schema"),
                    meta,
                    tablet.data_dir().join("crossing-output"),
                )
                .unwrap(),
            ))
            .unwrap();
        tablet.set_cumulative_point(1);

        let error = CumulativeCompactionPolicy::new()
            .pick_rowsets(&tablet)
            .unwrap_err();
        assert!(error.to_string().contains("crosses the cumulative point"));
    }

    #[test]
    fn cumulative_policy_accepts_commit_versions_without_tablet_writes() {
        let (_temp, tablet) = test_tablet();
        add_rowset(&tablet, 1, 4, 0);
        add_rowset(&tablet, 2, 9, 0);

        let selected = CumulativeCompactionPolicy::new()
            .pick_rowsets(&tablet)
            .unwrap();
        assert_eq!(
            selected
                .iter()
                .map(|rowset| rowset.start_version())
                .collect::<Vec<_>>(),
            vec![4, 9]
        );
    }

    #[test]
    fn size_tiered_action_is_relative_to_cumulative_point_not_version_zero() {
        let (_temp, base_tablet) = test_tablet();
        add_rowset(&base_tablet, 1, 4, 0);
        add_rowset(&base_tablet, 2, 7, 0);
        base_tablet.set_cumulative_point(10);
        let base_decision = SizeTieredCompactionPolicy::new()
            .select(&base_tablet)
            .unwrap()
            .unwrap();
        assert_eq!(
            base_decision.cumulative_point_action,
            CumulativePointAction::Preserve
        );

        let (_temp, delta_tablet) = test_tablet();
        add_rowset(&delta_tablet, 1, 4, 0);
        add_rowset(&delta_tablet, 2, 7, 0);
        let delta_decision = SizeTieredCompactionPolicy::new()
            .select(&delta_tablet)
            .unwrap()
            .unwrap();
        assert_eq!(
            delta_decision.cumulative_point_action,
            CumulativePointAction::AdvanceToOutputEndExclusive
        );
    }
}
