// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::view::CheckpointCut;
use parking_lot::{Condvar, Mutex};
use paro_common::checkpoint::{CheckpointFrontier, RecoverySummary};
pub use paro_common::journal::JournalPublicationWatermarks as RecordWatermarks;
use paro_common::{error as paro_error, error::Result};
use paro_journal::JournalPublicationObserver;
use std::collections::BTreeMap;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Default)]
struct PublishedPrefixState {
    durable_lsn: u64,
    published_summary: RecoverySummary,
    exact_prefix_waiters: BTreeMap<u64, Option<RecoverySummary>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExactPrefixTimeout {
    pub target_lsn: u64,
    pub published_lsn: u64,
    pub durable_lsn: u64,
}

/// Database-scoped exact published-prefix tracker.
///
/// The ordered journal apply runtime is the only producer. This tracker never
/// allocates or infers LSNs: every record is folded at the LSN embedded in its
/// durable journal frame. The checkpoint replay boundary therefore shares the
/// WAL ordering domain, including maintenance records between transactions.
#[derive(Debug, Default)]
pub struct PublishedPrefixTracker {
    state: Mutex<PublishedPrefixState>,
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
        state.durable_lsn = summary.max_lsn;
        state.published_summary = summary;
        state.exact_prefix_waiters.clear();
        self.changed.notify_all();
    }

    pub fn published_summary(&self) -> RecoverySummary {
        self.state.lock().published_summary.clone()
    }

    /// Atomically capture both the replay boundary and its aggregate
    /// watermarks. Later publications cannot change the captured summary.
    pub fn capture_durable_prefix(&self) -> CheckpointCut {
        let mut state = self.state.lock();
        let target_lsn = state.durable_lsn;
        if target_lsn > state.published_summary.max_lsn {
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
    ) -> std::result::Result<RecoverySummary, ExactPrefixTimeout> {
        let started_at = Instant::now();
        let mut state = self.state.lock();
        loop {
            if target_lsn == 0 {
                return Ok(RecoverySummary::default());
            }
            if state.published_summary.max_lsn == target_lsn {
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
            let Some(remaining) = timeout.checked_sub(started_at.elapsed()) else {
                return Err(Self::timeout(&state, target_lsn));
            };
            if self.changed.wait_for(&mut state, remaining).timed_out() {
                return Err(Self::timeout(&state, target_lsn));
            }
        }
    }

    fn timeout(state: &PublishedPrefixState, target_lsn: u64) -> ExactPrefixTimeout {
        ExactPrefixTimeout {
            target_lsn,
            published_lsn: state.published_summary.max_lsn,
            durable_lsn: state.durable_lsn,
        }
    }

    fn record_durable_inner(&self, durable_lsn: u64) {
        let mut state = self.state.lock();
        state.durable_lsn = state.durable_lsn.max(durable_lsn);
        self.changed.notify_all();
    }

    fn record_published_inner(&self, lsn: u64, watermarks: RecordWatermarks) -> Result<()> {
        let mut state = self.state.lock();
        let expected_lsn = state.published_summary.max_lsn.saturating_add(1);
        if lsn != expected_lsn {
            return Err(paro_error::internal(format!(
                "journal publication prefix expected lsn {} but observed {}",
                expected_lsn, lsn
            )));
        }
        fold_record(&mut state.published_summary, lsn, watermarks);
        let published_summary = state.published_summary.clone();
        if let Some(waiter) = state.exact_prefix_waiters.get_mut(&lsn) {
            *waiter = Some(published_summary);
        }
        self.changed.notify_all();
        Ok(())
    }
}

impl JournalPublicationObserver for PublishedPrefixTracker {
    fn record_durable(&self, durable_lsn: u64) {
        self.record_durable_inner(durable_lsn);
    }

    fn record_published(&self, lsn: u64, watermarks: RecordWatermarks) -> Result<()> {
        self.record_published_inner(lsn, watermarks)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_cut_uses_real_durable_lsn_and_survives_later_publish() {
        let tracker = PublishedPrefixTracker::new();
        tracker.record_durable_inner(3);
        tracker
            .record_published_inner(1, RecordWatermarks::transaction(1, 1, 10))
            .unwrap();
        let cut = tracker.capture_durable_prefix();
        tracker
            .record_published_inner(2, RecordWatermarks::maintenance(7))
            .unwrap();
        tracker
            .record_published_inner(3, RecordWatermarks::transaction(2, 0, 0))
            .unwrap();
        tracker.record_durable_inner(4);
        tracker
            .record_published_inner(4, RecordWatermarks::maintenance(8))
            .unwrap();
        let summary = tracker
            .wait_for_exact_prefix(cut.target_lsn, Duration::from_millis(50))
            .unwrap();

        assert_eq!(cut.target_lsn, 3);
        assert_eq!(summary.max_lsn, 3);
        assert_eq!(summary.max_commit_id, 2);
        assert_eq!(summary.max_catalog_commit_id, 1);
        assert_eq!(summary.max_seen_object_id, 10);
        assert_eq!(summary.max_maintenance_id, 7);
        assert_eq!(tracker.published_summary().max_maintenance_id, 8);
    }

    #[test]
    fn rejects_inferred_or_gapped_lsn() {
        let tracker = PublishedPrefixTracker::new();
        let err = tracker
            .record_published_inner(2, RecordWatermarks::transaction(1, 0, 0))
            .expect_err("publication cannot skip the durable lsn domain");
        assert!(err.to_string().contains("expected lsn 1"));
    }

    #[test]
    fn bootstrap_floor_survives_dml_and_maintenance_publish() {
        let tracker = PublishedPrefixTracker::new();
        tracker.bootstrap(RecoverySummary {
            max_lsn: 7,
            max_commit_id: 11,
            max_maintenance_id: 3,
            max_catalog_commit_id: 5,
            max_seen_object_id: 99,
        });

        tracker.record_durable_inner(9);
        tracker
            .record_published_inner(8, RecordWatermarks::maintenance(4))
            .unwrap();
        tracker
            .record_published_inner(9, RecordWatermarks::transaction(12, 0, 0))
            .unwrap();
        let summary = tracker.published_summary();
        assert_eq!(summary.max_lsn, 9);
        assert_eq!(summary.max_commit_id, 12);
        assert_eq!(summary.max_maintenance_id, 4);
        assert_eq!(summary.max_catalog_commit_id, 5);
        assert_eq!(summary.max_seen_object_id, 99);
    }

    #[test]
    fn exact_cut_times_out_while_real_durable_record_is_unpublished() {
        let tracker = PublishedPrefixTracker::new();
        tracker.record_durable_inner(1);
        let cut = tracker.capture_durable_prefix();
        let err = tracker
            .wait_for_exact_prefix(cut.target_lsn, Duration::from_millis(5))
            .expect_err("unpublished durable record must block the exact cut");
        assert_eq!(err.target_lsn, 1);
        assert_eq!(err.published_lsn, 0);
        assert_eq!(err.durable_lsn, 1);
    }
}
