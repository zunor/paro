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

pub struct CompactionSelection {
    pub decision: CompactionDecision,
    pub rowsets: Vec<RowsetSharedPtr>,
}

/// One immutable, validated view of the visible rowset graph used by every
/// compaction policy in a planning attempt.
///
/// Global commit versions may contain holes when a tablet received no write,
/// but visible rowset ranges must never overlap or straddle the durable
/// cumulative boundary. Capturing and validating once prevents policies from
/// silently disagreeing about the same tablet state.
pub struct CompactionRowsetSet {
    rowsets: Vec<RowsetSharedPtr>,
    cumulative_point: i64,
}

impl CompactionRowsetSet {
    pub fn capture(tablet: &Tablet) -> Result<Self> {
        let mut rowsets = tablet.capture_consistent_rowsets(tablet.max_version())?;
        rowsets.sort_by_key(|rowset| rowset.start_version());
        let cumulative_point = tablet.cumulative_point();

        let mut previous_end = None;
        for rowset in &rowsets {
            if let Some(end) = previous_end {
                if rowset.start_version() <= end {
                    return Err(paro_error::data_corrupted(format!(
                        "rowset version [{}, {}] overlaps an earlier visible rowset ending at {end}",
                        rowset.start_version(),
                        rowset.end_version(),
                    )));
                }
            }
            if cumulative_point >= 0
                && rowset.start_version() < cumulative_point
                && rowset.end_version() >= cumulative_point
            {
                return Err(paro_error::data_corrupted(format!(
                    "rowset version [{}, {}] crosses the cumulative point {cumulative_point}",
                    rowset.start_version(),
                    rowset.end_version(),
                )));
            }
            previous_end = Some(rowset.end_version());
        }

        Ok(Self {
            rowsets,
            cumulative_point,
        })
    }

    pub fn rowsets(&self) -> &[RowsetSharedPtr] {
        &self.rowsets
    }

    pub const fn cumulative_point(&self) -> i64 {
        self.cumulative_point
    }
}

pub trait CompactionPolicy: Send + Sync {
    fn select(&self, rowsets: &CompactionRowsetSet) -> Result<Option<CompactionSelection>>;
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

    fn pick_rowsets(&self, input: &CompactionRowsetSet) -> Result<Vec<RowsetSharedPtr>> {
        if input.rowsets().len() < self.min_segments as usize {
            return Ok(Vec::new());
        }

        let mut groups = Vec::new();
        let mut current_group = Vec::new();
        let mut current_level_size = 0u64;
        let mut current_is_base = None;
        for rs in input.rowsets() {
            let rs_size = rs.total_disk_size();
            let effective_size = rs_size.max(self.min_level_size / 10);
            let is_base = rs.end_version() < input.cumulative_point();

            if !current_group.is_empty() {
                let larger = current_level_size.max(effective_size);
                let smaller = current_level_size.min(effective_size).max(1);
                if current_is_base != Some(is_base)
                    || larger as f64 > smaller as f64 * self.level_multiple
                {
                    groups.push(std::mem::take(&mut current_group));
                    current_level_size = 0;
                }
            }
            current_group.push(rs.clone());
            current_level_size = current_level_size.max(effective_size);
            current_is_base = Some(is_base);
        }
        if !current_group.is_empty() {
            groups.push(current_group);
        }

        // Multiple eligible levels may coexist. Prefer the level with the
        // highest benefit score; for equal scores prefer the newer level so a
        // large established base does not win merely by appearing first.
        Ok(groups
            .into_iter()
            .filter(|group| group.len() >= self.min_segments as usize)
            .max_by(|left, right| {
                self.calculate_score(left)
                    .total_cmp(&self.calculate_score(right))
                    .then_with(|| left[0].start_version().cmp(&right[0].start_version()))
            })
            .unwrap_or_default())
    }
}

impl CompactionPolicy for SizeTieredCompactionPolicy {
    fn select(&self, input: &CompactionRowsetSet) -> Result<Option<CompactionSelection>> {
        let rowsets = self.pick_rowsets(input)?;
        if rowsets.is_empty() {
            return Ok(None);
        }

        let score = self.calculate_score(&rowsets);
        if score < self.min_segments as f64 {
            return Ok(None);
        }

        let cumulative_point_action = if rowsets
            .last()
            .is_some_and(|rowset| rowset.end_version() < input.cumulative_point())
        {
            CumulativePointAction::Preserve
        } else {
            CumulativePointAction::AdvanceToOutputEndExclusive
        };

        Ok(Some(CompactionSelection {
            decision: CompactionDecision {
                score,
                policy_kind: PolicyKind::SizeTiered,
                cumulative_point_action,
                reason: CompactionReason::SizeTieredPolicy,
            },
            rowsets,
        }))
    }
}

pub struct BaseCompactionPolicy;

impl BaseCompactionPolicy {
    pub fn new() -> Self {
        Self
    }

    fn pick_rowsets(&self, input: &CompactionRowsetSet) -> Result<Vec<RowsetSharedPtr>> {
        if input.rowsets().is_empty() {
            return Ok(Vec::new());
        }

        let mut candidates = Vec::new();
        for rs in input.rowsets() {
            if rs.end_version() < input.cumulative_point() {
                candidates.push(rs.clone());
            } else {
                break;
            }
        }

        if candidates.len() <= 1 {
            return Ok(Vec::new());
        }

        Ok(candidates)
    }
}

impl CompactionPolicy for BaseCompactionPolicy {
    fn select(&self, input: &CompactionRowsetSet) -> Result<Option<CompactionSelection>> {
        let rowsets = self.pick_rowsets(input)?;
        if rowsets.is_empty() {
            return Ok(None);
        }

        Ok(Some(CompactionSelection {
            decision: CompactionDecision {
                score: rowsets.len() as f64,
                policy_kind: PolicyKind::Base,
                cumulative_point_action: CumulativePointAction::Preserve,
                reason: CompactionReason::BasePolicy,
            },
            rowsets,
        }))
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

    fn pick_rowsets(&self, input: &CompactionRowsetSet) -> Result<Vec<RowsetSharedPtr>> {
        if input.rowsets().is_empty() {
            return Ok(Vec::new());
        }

        let mut candidates = Vec::new();
        // -1 is the durable sentinel used before the first cumulative
        // compaction. Global commit versions may contain holes where this
        // tablet had no write, so a sentinel does not imply that its first
        // rowset starts at zero.
        for rs in input.rowsets() {
            if rs.end_version() < input.cumulative_point() {
                continue;
            }
            if candidates.len() >= self.max_delta_rowsets {
                break;
            }
            candidates.push(rs.clone());
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

impl CompactionPolicy for CumulativeCompactionPolicy {
    fn select(&self, input: &CompactionRowsetSet) -> Result<Option<CompactionSelection>> {
        let rowsets = self.pick_rowsets(input)?;
        if rowsets.is_empty() {
            return Ok(None);
        }

        let score = rowsets
            .iter()
            .map(|rs| rs.rowset_meta().get_compaction_score())
            .sum();
        Ok(Some(CompactionSelection {
            decision: CompactionDecision {
                score,
                policy_kind: PolicyKind::Cumulative,
                cumulative_point_action: CumulativePointAction::AdvanceToOutputEndExclusive,
                reason: CompactionReason::CumulativePolicy,
            },
            rowsets,
        }))
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

    fn add_sized_rowset(
        tablet: &Tablet,
        id: u64,
        version: i64,
        bytes: u64,
        compaction_output: bool,
    ) {
        let mut meta = RowsetMeta::new(id, tablet.tablet_id(), Version::singleton(version));
        meta.set_disk_sizes(bytes, 0);
        if compaction_output {
            meta.set_compaction_output(vec![id + 100]);
        }
        let rowset = Rowset::create(
            tablet.schema().expect("test tablet schema"),
            meta,
            tablet.data_dir().join(format!("rowset-{id}")),
        )
        .unwrap();
        tablet.add_rowset(Arc::new(rowset)).unwrap();
    }

    fn captured(tablet: &Tablet) -> CompactionRowsetSet {
        CompactionRowsetSet::capture(tablet).unwrap()
    }

    #[test]
    fn cumulative_policy_normalizes_initial_sentinel_and_keeps_delete_versions() {
        let (_temp, tablet) = test_tablet();
        assert_eq!(tablet.cumulative_point(), -1);
        for version in 0..5 {
            add_rowset(&tablet, version as u64 + 1, version, (version == 2) as u64);
        }

        let selected = CumulativeCompactionPolicy::new()
            .pick_rowsets(&captured(&tablet))
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
    fn base_policy_returns_the_complete_base_prefix() {
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

        assert_eq!(
            BaseCompactionPolicy::new()
                .pick_rowsets(&captured(&tablet))
                .unwrap()
                .len(),
            4
        );

        add_rowset(&tablet, 5, 14, 0);
        tablet.set_cumulative_point(15);
        assert_eq!(
            BaseCompactionPolicy::new()
                .pick_rowsets(&captured(&tablet))
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

        let error = CompactionRowsetSet::capture(&tablet)
            .err()
            .expect("crossing cumulative point must be rejected");
        assert!(error.to_string().contains("crosses the cumulative point"));
    }

    #[test]
    fn cumulative_policy_accepts_commit_versions_without_tablet_writes() {
        let (_temp, tablet) = test_tablet();
        add_rowset(&tablet, 1, 4, 0);
        add_rowset(&tablet, 2, 9, 0);

        let selected = CumulativeCompactionPolicy::new()
            .pick_rowsets(&captured(&tablet))
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
        let base_selection = SizeTieredCompactionPolicy::new()
            .select(&captured(&base_tablet))
            .unwrap()
            .unwrap();
        assert_eq!(
            base_selection.decision.cumulative_point_action,
            CumulativePointAction::Preserve
        );

        let (_temp, delta_tablet) = test_tablet();
        add_rowset(&delta_tablet, 1, 4, 0);
        add_rowset(&delta_tablet, 2, 7, 0);
        let delta_selection = SizeTieredCompactionPolicy::new()
            .select(&captured(&delta_tablet))
            .unwrap()
            .unwrap();
        assert_eq!(
            delta_selection.decision.cumulative_point_action,
            CumulativePointAction::AdvanceToOutputEndExclusive
        );
    }

    #[test]
    fn size_tiered_keeps_large_base_out_of_small_delta_level() {
        let (_temp, tablet) = test_tablet();
        add_sized_rowset(&tablet, 1, 1, 100 * 1024 * 1024 * 1024, true);
        add_sized_rowset(&tablet, 2, 2, 4 * 1024 * 1024, false);
        add_sized_rowset(&tablet, 3, 3, 4 * 1024 * 1024, false);
        tablet.set_cumulative_point(2);

        let selected = SizeTieredCompactionPolicy::new()
            .pick_rowsets(&captured(&tablet))
            .unwrap();
        assert_eq!(
            selected
                .iter()
                .map(|rowset| rowset.rowset_id())
                .collect::<Vec<_>>(),
            vec![2, 3]
        );
    }
}
