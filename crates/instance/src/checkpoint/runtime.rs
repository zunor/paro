// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::view::CheckpointCut;
use parking_lot::{Condvar, Mutex};
use paro_common::checkpoint::{CheckpointFrontier, RecoverySummary};
use std::collections::BTreeMap;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Per-record watermark payload folded into the published-prefix recovery
/// summary.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RecordWatermarks {
    pub commit_id: u64,
    pub maintenance_id: u64,
    pub catalog_commit_id: u64,
    pub max_seen_object_id: u64,
}

impl RecordWatermarks {
    pub const fn transaction(
        commit_id: u64,
        catalog_commit_id: u64,
        max_seen_object_id: u64,
    ) -> Self {
        Self {
            commit_id,
            maintenance_id: 0,
            catalog_commit_id,
            max_seen_object_id,
        }
    }
}

/// Durable-but-not-yet-folded published-prefix record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ApplyRequest {
    pub lsn: u64,
    pub watermarks: RecordWatermarks,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExactPrefixTimeout {
    pub target_lsn: u64,
    pub published_lsn: u64,
    pub durable_lsn: u64,
}

#[derive(Debug, Clone)]
struct ApplyRuntimeState {
    next_lsn: u64,
    next_maintenance_id: u64,
    durable_lsn: u64,
    published_lsn: u64,
    published_summary: RecoverySummary,
    pending_by_lsn: BTreeMap<u64, RecordWatermarks>,
    exact_prefix_waiters: BTreeMap<u64, Option<RecoverySummary>>,
}

impl Default for ApplyRuntimeState {
    fn default() -> Self {
        Self {
            next_lsn: 1,
            next_maintenance_id: 1,
            durable_lsn: 0,
            published_lsn: 0,
            published_summary: RecoverySummary::default(),
            pending_by_lsn: BTreeMap::new(),
            exact_prefix_waiters: BTreeMap::new(),
        }
    }
}

/// Database-scoped published-prefix tracker shared by commit, maintenance, and
/// checkpoint cut/drain.
#[derive(Debug, Default)]
pub struct PublishedPrefixTracker {
    state: Mutex<ApplyRuntimeState>,
    changed: Condvar,
}

pub fn frontier_from_summary(summary: &RecoverySummary) -> CheckpointFrontier {
    CheckpointFrontier {
        checkpoint_lsn: summary.max_lsn,
        checkpoint_commit_id: summary.max_commit_id,
        checkpoint_maintenance_id: summary.max_maintenance_id,
    }
}

fn now_micros() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_micros() as u64)
        .unwrap_or(0)
}

fn fold_record(summary: &mut RecoverySummary, lsn: u64, watermarks: RecordWatermarks) {
    summary.max_lsn = lsn;
    summary.max_commit_id = summary.max_commit_id.max(watermarks.commit_id);
    summary.max_maintenance_id = summary.max_maintenance_id.max(watermarks.maintenance_id);
    summary.max_catalog_commit_id = summary
        .max_catalog_commit_id
        .max(watermarks.catalog_commit_id);
    summary.max_seen_object_id = summary
        .max_seen_object_id
        .max(watermarks.max_seen_object_id);
}

impl PublishedPrefixTracker {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn bootstrap(&self, summary: RecoverySummary) {
        let mut state = self.state.lock();
        state.next_lsn = summary.max_lsn.saturating_add(1);
        state.next_maintenance_id = summary.max_maintenance_id.saturating_add(1).max(1);
        state.durable_lsn = summary.max_lsn;
        state.published_lsn = summary.max_lsn;
        state.published_summary = summary;
        state.pending_by_lsn.clear();
        state.exact_prefix_waiters.clear();
        self.changed.notify_all();
    }

    pub fn published_summary(&self) -> RecoverySummary {
        self.state.lock().published_summary.clone()
    }

    pub fn begin_apply(&self, watermarks: RecordWatermarks) -> ApplyRequest {
        let mut state = self.state.lock();
        let lsn = state.next_lsn;
        state.next_lsn = state.next_lsn.saturating_add(1);
        state.durable_lsn = lsn;
        let request = ApplyRequest { lsn, watermarks };
        self.changed.notify_all();
        request
    }

    pub fn begin_maintenance_apply(&self) -> ApplyRequest {
        let mut state = self.state.lock();
        let maintenance_id = state.next_maintenance_id;
        state.next_maintenance_id = state.next_maintenance_id.saturating_add(1);
        let lsn = state.next_lsn;
        state.next_lsn = state.next_lsn.saturating_add(1);
        state.durable_lsn = lsn;
        let request = ApplyRequest {
            lsn,
            watermarks: RecordWatermarks {
                maintenance_id,
                ..RecordWatermarks::default()
            },
        };
        self.changed.notify_all();
        request
    }

    pub fn publish_completed(&self, request: ApplyRequest) -> RecoverySummary {
        let mut state = self.state.lock();
        state.pending_by_lsn.insert(request.lsn, request.watermarks);
        Self::advance_published_locked(&mut state);
        let summary = state.published_summary.clone();
        self.changed.notify_all();
        summary
    }

    pub fn publish_immediately(&self, watermarks: RecordWatermarks) -> RecoverySummary {
        let request = self.begin_apply(watermarks);
        self.publish_completed(request)
    }

    pub fn capture_durable_prefix(&self) -> CheckpointCut {
        let mut state = self.state.lock();
        let target_lsn = state.durable_lsn;
        if target_lsn > state.published_lsn {
            state.exact_prefix_waiters.entry(target_lsn).or_insert(None);
        }
        CheckpointCut {
            target_lsn,
            issued_at_micros: now_micros(),
        }
    }

    pub fn wait_for_exact_prefix(
        &self,
        target_lsn: u64,
        timeout: Duration,
    ) -> Result<RecoverySummary, ExactPrefixTimeout> {
        let start = Instant::now();
        let mut state = self.state.lock();

        loop {
            if target_lsn == 0 {
                return Ok(RecoverySummary::default());
            }

            if state.published_lsn == target_lsn {
                return Ok(state.published_summary.clone());
            }

            if let Some(summary) = state
                .exact_prefix_waiters
                .get_mut(&target_lsn)
                .and_then(Option::take)
            {
                state.exact_prefix_waiters.remove(&target_lsn);
                return Ok(summary);
            }

            let Some(remaining) = timeout.checked_sub(start.elapsed()) else {
                return Err(ExactPrefixTimeout {
                    target_lsn,
                    published_lsn: state.published_lsn,
                    durable_lsn: state.durable_lsn,
                });
            };

            if self.changed.wait_for(&mut state, remaining).timed_out() {
                return Err(ExactPrefixTimeout {
                    target_lsn,
                    published_lsn: state.published_lsn,
                    durable_lsn: state.durable_lsn,
                });
            }
        }
    }

    fn advance_published_locked(state: &mut ApplyRuntimeState) {
        loop {
            let next_lsn = state.published_lsn.saturating_add(1);
            let Some(watermarks) = state.pending_by_lsn.remove(&next_lsn) else {
                break;
            };
            fold_record(&mut state.published_summary, next_lsn, watermarks);
            state.published_lsn = next_lsn;

            if let Some(waiter) = state.exact_prefix_waiters.get_mut(&next_lsn) {
                *waiter = Some(state.published_summary.clone());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_prefix_waiter_captures_target_before_later_lsn_advances() {
        let tracker = PublishedPrefixTracker::new();
        tracker.publish_immediately(RecordWatermarks::transaction(1, 1, 10));

        let second = tracker.begin_apply(RecordWatermarks::transaction(2, 0, 0));
        let cut = tracker.capture_durable_prefix();
        let third = tracker.begin_apply(RecordWatermarks::transaction(3, 3, 11));

        tracker.publish_completed(third);
        tracker.publish_completed(second);

        let summary = tracker
            .wait_for_exact_prefix(cut.target_lsn, Duration::from_millis(50))
            .expect("target lsn should resolve");
        assert_eq!(cut.target_lsn, 2);
        assert_eq!(summary.max_lsn, 2);
        assert_eq!(summary.max_commit_id, 2);
        assert_eq!(summary.max_catalog_commit_id, 1);
        assert_eq!(summary.max_seen_object_id, 10);
    }

    #[test]
    fn bootstrap_floor_survives_dml_only_publish() {
        let tracker = PublishedPrefixTracker::new();
        tracker.bootstrap(RecoverySummary {
            max_lsn: 7,
            max_commit_id: 11,
            max_maintenance_id: 3,
            max_catalog_commit_id: 5,
            max_seen_object_id: 99,
        });

        let summary = tracker.publish_immediately(RecordWatermarks::transaction(12, 0, 0));
        assert_eq!(summary.max_lsn, 8);
        assert_eq!(summary.max_commit_id, 12);
        assert_eq!(summary.max_catalog_commit_id, 5);
        assert_eq!(summary.max_seen_object_id, 99);
    }

    #[test]
    fn exact_prefix_waiter_times_out_when_target_never_publishes() {
        let tracker = PublishedPrefixTracker::new();
        tracker.publish_immediately(RecordWatermarks::transaction(1, 1, 10));

        let pending = tracker.begin_apply(RecordWatermarks::transaction(2, 0, 0));
        let cut = tracker.capture_durable_prefix();

        let err = tracker
            .wait_for_exact_prefix(cut.target_lsn, Duration::from_millis(20))
            .expect_err("target lsn should time out while publish is stalled");
        assert_eq!(cut.target_lsn, pending.lsn);
        assert_eq!(err.target_lsn, 2);
        assert_eq!(err.published_lsn, 1);
        assert_eq!(err.durable_lsn, 2);
    }
}
