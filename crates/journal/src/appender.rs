// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::codec::encode_record;
use paro_common::effect::{DeletePatchRef, StorageCommitOp, TabletMutation};
use paro_common::error as paro_error;
use paro_common::error::{ParoError, Result};
use paro_common::journal::JournalRecord;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{sync_channel, SyncSender};
use std::sync::{Arc, Condvar, Mutex, Weak};
use std::thread;
use std::time::Instant;

/// Append target used by [`JournalAppender`].
pub trait JournalSink: Send + Sync + 'static {
    fn append_batch(&self, frames: &[Vec<u8>]) -> Result<()>;
}

/// Append result returned to each waiter after a batch is durable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppendResult {
    pub lsn: u64,
    pub durable_batch_lsn: u64,
    pub durable_batch_size: u64,
    pub durable_batch_bytes: u64,
    pub sync_latency_micros: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct JournalAppenderMetricsSnapshot {
    pub commit_bytes_total: u64,
    pub group_count: u64,
    pub group_size_last: u64,
    pub group_size_peak: u64,
    pub sync_latency_micros_total: u64,
    pub sync_latency_micros_peak: u64,
    pub inline_delete_patch_count: u64,
    pub delete_patch_count: u64,
}

impl JournalAppenderMetricsSnapshot {
    pub fn inline_patch_ratio(self) -> f64 {
        if self.delete_patch_count == 0 {
            0.0
        } else {
            self.inline_delete_patch_count as f64 / self.delete_patch_count as f64
        }
    }
}

pub struct JournalAppender {
    inner: Arc<JournalAppenderInner>,
}

struct JournalAppenderInner {
    sink: Arc<dyn JournalSink>,
    state: Mutex<AppenderState>,
    wake_worker: Condvar,
    metrics: AppenderMetrics,
}

struct AppenderState {
    next_lsn: u64,
    queue: VecDeque<PendingAppendGroup>,
    poisoned: Option<ParoError>,
    shutdown: bool,
}

struct PendingAppendGroup {
    records: Vec<JournalRecord>,
    response: SyncSender<std::result::Result<Vec<AppendResult>, ParoError>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
struct BatchObservation {
    group_size: u64,
    batch_bytes: u64,
    inline_delete_patch_count: u64,
    delete_patch_count: u64,
}

#[derive(Debug, Default)]
struct AppenderMetrics {
    commit_bytes_total: AtomicU64,
    group_count: AtomicU64,
    group_size_last: AtomicU64,
    group_size_peak: AtomicU64,
    sync_latency_micros_total: AtomicU64,
    sync_latency_micros_peak: AtomicU64,
    inline_delete_patch_count: AtomicU64,
    delete_patch_count: AtomicU64,
}

impl JournalAppender {
    pub fn new(sink: Arc<dyn JournalSink>) -> Self {
        Self::new_with_next_lsn(sink, 1)
    }

    pub fn new_with_next_lsn(sink: Arc<dyn JournalSink>, next_lsn: u64) -> Self {
        let inner = Arc::new(JournalAppenderInner {
            sink,
            state: Mutex::new(AppenderState {
                next_lsn: next_lsn.max(1),
                queue: VecDeque::new(),
                poisoned: None,
                shutdown: false,
            }),
            wake_worker: Condvar::new(),
            metrics: AppenderMetrics::default(),
        });

        let worker_inner = Arc::downgrade(&inner);
        thread::Builder::new()
            .name("paro-journal-appender".to_string())
            .spawn(move || run_worker(worker_inner))
            .expect("spawn journal appender worker");

        Self { inner }
    }

    pub fn metrics(&self) -> JournalAppenderMetricsSnapshot {
        self.inner.metrics.snapshot()
    }

    pub fn append_record(&self, record: &JournalRecord) -> Result<AppendResult> {
        let mut results = self.append_records(std::slice::from_ref(record))?;
        results
            .pop()
            .ok_or_else(|| paro_error::internal("journal appender returned no append result"))
    }

    pub fn append_records(&self, records: &[JournalRecord]) -> Result<Vec<AppendResult>> {
        if records.is_empty() {
            return Ok(Vec::new());
        }

        let (tx, rx) = sync_channel(1);
        let mut state = self.inner.state.lock().unwrap();
        if let Some(err) = state.poisoned.clone() {
            return Err(err);
        }
        if state.shutdown {
            return Err(paro_error::internal(
                "journal appender is shutting down and cannot accept new records",
            ));
        }
        state.queue.push_back(PendingAppendGroup {
            records: records.to_vec(),
            response: tx,
        });
        drop(state);
        self.inner.wake_worker.notify_one();

        rx.recv()
            .map_err(|_| paro_error::internal("journal appender worker exited before responding"))?
    }
}

impl Clone for JournalAppender {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl Drop for JournalAppender {
    fn drop(&mut self) {
        if Arc::strong_count(&self.inner) == 1 {
            let mut state = self.inner.state.lock().unwrap();
            state.shutdown = true;
            self.inner.wake_worker.notify_all();
        }
    }
}

fn run_worker(inner: Weak<JournalAppenderInner>) {
    loop {
        let Some(inner) = inner.upgrade() else {
            return;
        };
        let (batch, poison, next_lsn) = {
            let mut state = inner.state.lock().unwrap();
            while state.queue.is_empty() && !state.shutdown {
                state = inner.wake_worker.wait(state).unwrap();
            }

            if state.queue.is_empty() && state.shutdown {
                return;
            }

            let mut batch = Vec::with_capacity(state.queue.len());
            while let Some(pending) = state.queue.pop_front() {
                batch.push(pending);
            }
            (batch, state.poisoned.clone(), state.next_lsn)
        };

        if let Some(err) = poison {
            reject_batch(batch, err);
            continue;
        }

        let encoded = match encode_batch(&batch, next_lsn) {
            Ok(encoded) => encoded,
            Err(err) => {
                poison_appender(&inner, err.clone());
                reject_batch(batch, err);
                continue;
            }
        };
        let observation = observe_batch(&batch, &encoded);

        let frames: Vec<Vec<u8>> = encoded
            .iter()
            .flat_map(|pending| pending.frames.iter().cloned())
            .collect();

        let sync_started_at = Instant::now();
        match inner.sink.append_batch(&frames) {
            Ok(()) => {
                let sync_latency_micros =
                    sync_started_at.elapsed().as_micros().min(u64::MAX as u128) as u64;
                inner
                    .metrics
                    .observe_batch(observation, sync_latency_micros);
                let durable_batch_lsn = encoded
                    .iter()
                    .filter_map(|pending| pending.lsns.last().copied())
                    .next_back()
                    .unwrap_or(0);
                {
                    let mut state = inner.state.lock().unwrap();
                    state.next_lsn = durable_batch_lsn.saturating_add(1);
                }
                for pending in encoded {
                    let result = pending
                        .lsns
                        .iter()
                        .map(|&lsn| AppendResult {
                            lsn,
                            durable_batch_lsn,
                            durable_batch_size: observation.group_size,
                            durable_batch_bytes: observation.batch_bytes,
                            sync_latency_micros,
                        })
                        .collect::<Vec<_>>();
                    let _ = pending.response.send(Ok(result));
                }
            }
            Err(err) => {
                let poisoned = paro_error::internal(format!(
                    "journal appender halted after append failure: {}",
                    err
                ));
                poison_appender(&inner, poisoned.clone());
                for pending in encoded {
                    let _ = pending.response.send(Err(poisoned.clone()));
                }
            }
        }
    }
}

struct EncodedPendingGroup {
    lsns: Vec<u64>,
    frames: Vec<Vec<u8>>,
    response: SyncSender<std::result::Result<Vec<AppendResult>, ParoError>>,
}

impl AppenderMetrics {
    fn snapshot(&self) -> JournalAppenderMetricsSnapshot {
        JournalAppenderMetricsSnapshot {
            commit_bytes_total: self.commit_bytes_total.load(Ordering::Relaxed),
            group_count: self.group_count.load(Ordering::Relaxed),
            group_size_last: self.group_size_last.load(Ordering::Relaxed),
            group_size_peak: self.group_size_peak.load(Ordering::Relaxed),
            sync_latency_micros_total: self.sync_latency_micros_total.load(Ordering::Relaxed),
            sync_latency_micros_peak: self.sync_latency_micros_peak.load(Ordering::Relaxed),
            inline_delete_patch_count: self.inline_delete_patch_count.load(Ordering::Relaxed),
            delete_patch_count: self.delete_patch_count.load(Ordering::Relaxed),
        }
    }

    fn observe_batch(&self, observation: BatchObservation, sync_latency_micros: u64) {
        self.group_count.fetch_add(1, Ordering::Relaxed);
        self.commit_bytes_total
            .fetch_add(observation.batch_bytes, Ordering::Relaxed);
        self.group_size_last
            .store(observation.group_size, Ordering::Relaxed);
        update_peak(&self.group_size_peak, observation.group_size);
        self.sync_latency_micros_total
            .fetch_add(sync_latency_micros, Ordering::Relaxed);
        update_peak(&self.sync_latency_micros_peak, sync_latency_micros);
        self.inline_delete_patch_count
            .fetch_add(observation.inline_delete_patch_count, Ordering::Relaxed);
        self.delete_patch_count
            .fetch_add(observation.delete_patch_count, Ordering::Relaxed);
    }
}

fn observe_batch(
    batch: &[PendingAppendGroup],
    encoded: &[EncodedPendingGroup],
) -> BatchObservation {
    let mut observation = BatchObservation {
        group_size: encoded
            .iter()
            .map(|pending| pending.lsns.len() as u64)
            .sum(),
        batch_bytes: encoded
            .iter()
            .flat_map(|pending| pending.frames.iter())
            .map(|frame| frame.len() as u64)
            .sum(),
        ..BatchObservation::default()
    };

    for pending in batch {
        for record in &pending.records {
            observe_record(record, &mut observation);
        }
    }

    observation
}

fn observe_record(record: &JournalRecord, observation: &mut BatchObservation) {
    let storage_ops = match record {
        JournalRecord::Commit(record) => &record.storage_ops,
        JournalRecord::Maintenance(record) => &record.storage_ops,
        JournalRecord::CheckpointFence(_) => return,
    };
    observe_storage_ops(storage_ops, observation);
}

fn observe_storage_ops(storage_ops: &[StorageCommitOp], observation: &mut BatchObservation) {
    for op in storage_ops {
        let StorageCommitOp::Tablet(tablet) = op;
        for mutation in &tablet.mutations {
            if let TabletMutation::ApplyDeletePatch { patch, .. } = mutation {
                observation.delete_patch_count = observation.delete_patch_count.saturating_add(1);
                if matches!(patch, DeletePatchRef::Inline(_)) {
                    observation.inline_delete_patch_count =
                        observation.inline_delete_patch_count.saturating_add(1);
                }
            }
        }
    }
}

fn update_peak(peak: &AtomicU64, value: u64) {
    let mut current = peak.load(Ordering::Relaxed);
    while value > current {
        match peak.compare_exchange(current, value, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => return,
            Err(next) => current = next,
        }
    }
}

fn encode_batch(batch: &[PendingAppendGroup], first_lsn: u64) -> Result<Vec<EncodedPendingGroup>> {
    let mut next_lsn = first_lsn;
    let mut encoded = Vec::with_capacity(batch.len());
    for pending in batch {
        let mut lsns = Vec::with_capacity(pending.records.len());
        let mut frames = Vec::with_capacity(pending.records.len());
        for record in &pending.records {
            let lsn = next_lsn;
            next_lsn = next_lsn.saturating_add(1);
            lsns.push(lsn);
            frames.push(encode_record(record, lsn)?);
        }
        encoded.push(EncodedPendingGroup {
            lsns,
            frames,
            response: pending.response.clone(),
        });
    }
    Ok(encoded)
}

fn reject_batch(batch: Vec<PendingAppendGroup>, err: ParoError) {
    for pending in batch {
        let _ = pending.response.send(Err(err.clone()));
    }
}

fn poison_appender(inner: &Arc<JournalAppenderInner>, err: ParoError) {
    let mut state = inner.state.lock().unwrap();
    if state.poisoned.is_none() {
        state.poisoned = Some(err);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use parking_lot::Mutex as ParkingMutex;
    use paro_common::effect::{
        DeletePatchInline, DeletePatchRef, StorageCommitOp, TabletApplyOp, TabletMutation,
    };
    use paro_common::journal::{CheckpointFence, JournalRecord};

    #[derive(Default)]
    struct RecordingSink {
        batches: ParkingMutex<Vec<Vec<Vec<u8>>>>,
        fail_once: ParkingMutex<bool>,
    }

    impl JournalSink for RecordingSink {
        fn append_batch(&self, frames: &[Vec<u8>]) -> Result<()> {
            if *self.fail_once.lock() {
                *self.fail_once.lock() = false;
                return Err(paro_error::io_error("injected append failure"));
            }
            self.batches.lock().push(frames.to_vec());
            Ok(())
        }
    }

    #[test]
    fn appender_returns_monotonic_lsns() {
        let sink = Arc::new(RecordingSink::default());
        let appender = JournalAppender::new(sink);

        let first = appender
            .append_record(&JournalRecord::CheckpointFence(CheckpointFence {
                checkpoint_marker: 1,
            }))
            .unwrap();
        let second = appender
            .append_record(&JournalRecord::CheckpointFence(CheckpointFence {
                checkpoint_marker: 2,
            }))
            .unwrap();

        assert_eq!(first.lsn, 1);
        assert_eq!(second.lsn, 2);
        assert!(second.durable_batch_lsn >= second.lsn);
    }

    #[test]
    fn appender_batches_explicit_record_groups() {
        let sink = Arc::new(RecordingSink::default());
        let appender = JournalAppender::new(sink.clone());

        let results = appender
            .append_records(&[
                JournalRecord::CheckpointFence(CheckpointFence {
                    checkpoint_marker: 1,
                }),
                JournalRecord::CheckpointFence(CheckpointFence {
                    checkpoint_marker: 2,
                }),
            ])
            .unwrap();

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].lsn, 1);
        assert_eq!(results[1].lsn, 2);
        assert_eq!(results[0].durable_batch_lsn, 2);
        assert_eq!(results[1].durable_batch_lsn, 2);
        assert_eq!(results[0].durable_batch_size, 2);
        assert!(results[0].durable_batch_bytes > 0);
        assert_eq!(sink.batches.lock().len(), 1);
    }

    #[test]
    fn appender_bootstraps_next_lsn() {
        let sink = Arc::new(RecordingSink::default());
        let appender = JournalAppender::new_with_next_lsn(sink, 9);

        let result = appender
            .append_record(&JournalRecord::CheckpointFence(CheckpointFence {
                checkpoint_marker: 7,
            }))
            .unwrap();

        assert_eq!(result.lsn, 9);
    }

    #[test]
    fn append_failure_poison_prevents_lsn_holes() {
        let sink = Arc::new(RecordingSink::default());
        *sink.fail_once.lock() = true;
        let appender = JournalAppender::new(sink);

        let err = appender
            .append_record(&JournalRecord::CheckpointFence(CheckpointFence {
                checkpoint_marker: 1,
            }))
            .expect_err("append should fail");
        assert!(err
            .to_string()
            .contains("journal appender halted after append failure"));

        let err = appender
            .append_record(&JournalRecord::CheckpointFence(CheckpointFence {
                checkpoint_marker: 2,
            }))
            .expect_err("poisoned appender should reject new work");
        assert!(err
            .to_string()
            .contains("journal appender halted after append failure"));
    }

    #[test]
    fn appender_metrics_track_batch_bytes_and_inline_patch_ratio() {
        let sink = Arc::new(RecordingSink::default());
        let appender = JournalAppender::new(sink);

        appender
            .append_record(&JournalRecord::Commit(paro_common::journal::CommitRecord {
                txn_id: 7,
                start_time: 11,
                commit_id: 13,
                catalog_ops: Vec::new(),
                storage_ops: vec![StorageCommitOp::Tablet(TabletApplyOp {
                    tablet_id: 41,
                    mutations: vec![TabletMutation::ApplyDeletePatch {
                        patch: DeletePatchRef::Inline(DeletePatchInline {
                            encoding:
                                paro_common::effect::DeletePatchEncoding::GroupedRowOffsetDeltaV1,
                            row_count: 0,
                            groups: Vec::new(),
                        }),
                        deleted_row_count: 0,
                    }],
                })],
                apply_descriptors: Vec::new(),
                deferred_tasks: Vec::new(),
            }))
            .unwrap();

        let metrics = appender.metrics();
        assert_eq!(metrics.group_count, 1);
        assert_eq!(metrics.group_size_last, 1);
        assert_eq!(metrics.group_size_peak, 1);
        assert!(metrics.commit_bytes_total > 0);
        assert_eq!(metrics.delete_patch_count, 1);
        assert_eq!(metrics.inline_delete_patch_count, 1);
        assert_eq!(metrics.inline_patch_ratio(), 1.0);
    }
}
