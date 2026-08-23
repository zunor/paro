// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::compaction::plan::types::{CompactionReason, CumulativePointAction, PolicyKind};
use crate::rowset::RowsetSharedPtr;
use crate::tablet::Tablet;
use paro_common::error::Result;
use std::time::{SystemTime, UNIX_EPOCH};

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

        let cumulative_point_action = if rowsets.first().map(|rs| rs.start_version()) == Some(0) {
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
    min_interval_seconds: u64,
}

impl BaseCompactionPolicy {
    pub fn new() -> Self {
        Self {
            min_interval_seconds: 86400,
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
        if rowsets[0].start_version() != 0 {
            return Ok(Vec::new());
        }

        let cumulative_point = tablet.cumulative_point();
        let mut candidates = Vec::new();
        for rs in rowsets {
            if rs.end_version() < cumulative_point {
                candidates.push(rs);
            } else {
                break;
            }
        }

        if candidates.len() > 1 {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_or(0, |duration| duration.as_secs());
            let newest_compaction_output = candidates
                .iter()
                .map(|rowset| rowset.rowset_meta())
                .filter(|meta| meta.is_compaction_output())
                .map(|meta| meta.creation_time().max(0) as u64)
                .max();
            if newest_compaction_output
                .is_some_and(|created| now.saturating_sub(created) < self.min_interval_seconds)
            {
                return Ok(Vec::new());
            }
            Ok(candidates)
        } else {
            Ok(Vec::new())
        }
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
        // compaction. Data versions themselves start at zero.
        let mut expected_start = cumulative_point.max(0);

        for rs in rowsets {
            if rs.end_version() < cumulative_point {
                continue;
            }
            // A compaction output is a contiguous version interval. Never skip
            // a delete-bearing or otherwise inconvenient rowset and then move
            // the cumulative point beyond the resulting hole.
            if rs.start_version() != expected_start {
                break;
            }
            if candidates.len() >= self.max_delta_rowsets {
                break;
            }

            expected_start = match rs.end_version().checked_add(1) {
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
    fn fresh_base_compaction_output_is_rate_limited() {
        let (_temp, tablet) = test_tablet();
        add_rowset(&tablet, 1, 0, 0);
        let mut recent = RowsetMeta::new(2, tablet.tablet_id(), Version::singleton(1));
        recent.set_compaction_output(vec![10, 11]);
        tablet
            .add_rowset(Arc::new(
                Rowset::create(
                    tablet.schema().expect("test tablet schema"),
                    recent,
                    tablet.data_dir().join("recent-output"),
                )
                .unwrap(),
            ))
            .unwrap();
        tablet.set_cumulative_point(2);

        assert!(BaseCompactionPolicy::new()
            .pick_rowsets(&tablet)
            .unwrap()
            .is_empty());
    }
}
