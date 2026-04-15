use crate::compaction::plan::types::{CompactionReason, CumulativePointAction, PolicyKind};
use crate::rowset::RowsetSharedPtr;
use crate::tablet::Tablet;
use paro_common::error::Result;

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
    _min_interval_seconds: u64,
}

impl BaseCompactionPolicy {
    pub fn new() -> Self {
        Self {
            _min_interval_seconds: 86400,
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
            Ok(candidates)
        } else {
            Ok(Vec::new())
        }
    }
}

pub struct CumulativeCompactionPolicy {
    min_deltas: usize,
    max_deltas: usize,
}

impl CumulativeCompactionPolicy {
    pub fn new() -> Self {
        Self {
            min_deltas: 5,
            max_deltas: 1000,
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
        if score < self.min_deltas as f64 {
            return Ok(None);
        }

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
        let mut score = 0.0;

        for rs in rowsets {
            if rs.start_version() < cumulative_point {
                continue;
            }
            if rs.rowset_meta().num_deleted_rows() > 0 {
                if !candidates.is_empty() {
                    break;
                }
                continue;
            }
            if candidates.len() >= self.max_deltas {
                break;
            }

            score += rs.rowset_meta().get_compaction_score();
            candidates.push(rs);
        }

        if score >= self.min_deltas as f64 {
            Ok(candidates)
        } else {
            Ok(Vec::new())
        }
    }
}
