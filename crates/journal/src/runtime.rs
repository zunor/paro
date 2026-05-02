// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::appender::{AppendResult, JournalAppender};
use crate::apply::JournalApplyRuntime;
use paro_common::durability::PreparedMaintenancePlan;
use paro_common::error as paro_error;
use paro_common::error::Result;
use paro_common::journal::{JournalRecord, MaintenanceRecord};
use paro_common::logging::targets;
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaintenanceAppendContext {
    pub maintenance_id: u64,
    pub lsn: u64,
    pub durable_batch_lsn: u64,
    pub durable_batch_size: u64,
    pub durable_batch_bytes: u64,
    pub sync_latency_micros: u64,
    pub record: MaintenanceRecord,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct JournalFrontierSnapshot {
    pub durable_lsn: u64,
    pub applied_lsn: u64,
    pub published_lsn: u64,
}

pub struct JournalCoordinator {
    inner: Arc<JournalCoordinatorInner>,
}

impl std::fmt::Debug for JournalCoordinator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JournalCoordinator")
            .field("frontiers", &self.frontiers())
            .finish()
    }
}

struct JournalCoordinatorInner {
    appender: Option<Arc<JournalAppender>>,
    apply_runtime: Mutex<Option<Arc<JournalApplyRuntime>>>,
    next_maintenance_id: Mutex<u64>,
    state: Mutex<CoordinatorState>,
}

#[derive(Default)]
struct CoordinatorState {
    fallback_frontiers: JournalFrontierSnapshot,
    shutdown: bool,
}

impl JournalCoordinator {
    pub fn new(appender: Option<Arc<JournalAppender>>) -> Self {
        Self {
            inner: Arc::new(JournalCoordinatorInner {
                appender,
                apply_runtime: Mutex::new(None),
                next_maintenance_id: Mutex::new(1),
                state: Mutex::new(CoordinatorState::default()),
            }),
        }
    }

    pub fn bind_apply_runtime(&self, runtime: Arc<JournalApplyRuntime>) {
        *self.inner.apply_runtime.lock().unwrap() = Some(runtime);
    }

    pub fn frontiers(&self) -> JournalFrontierSnapshot {
        self.inner
            .apply_runtime
            .lock()
            .unwrap()
            .as_ref()
            .map(|runtime| runtime.frontiers())
            .unwrap_or_else(|| self.inner.state.lock().unwrap().fallback_frontiers)
    }

    pub fn sync_maintenance_id_with(&self, min_maintenance_id: u64) {
        bump_mutex_min(
            &self.inner.next_maintenance_id,
            min_maintenance_id.saturating_add(1),
        );
    }

    pub fn append_records(&self, records: &[JournalRecord]) -> Result<Vec<AppendResult>> {
        if records.is_empty() {
            return Ok(Vec::new());
        }
        self.ensure_open()?;

        let results = match self.inner.appender.as_ref() {
            Some(appender) => appender.append_records(records)?,
            None => vec![
                AppendResult {
                    lsn: 0,
                    durable_batch_lsn: 0,
                    durable_batch_size: records.len() as u64,
                    durable_batch_bytes: 0,
                    sync_latency_micros: 0,
                };
                records.len()
            ],
        };

        if let Some(last) = results.last().copied() {
            update_durable_frontier(&self.inner, last.durable_batch_lsn);
            tracing::info!(
                target: targets::WAL,
                first_lsn = results.first().map(|result| result.lsn).unwrap_or(0),
                durable_batch_lsn = last.durable_batch_lsn,
                group_size = last.durable_batch_size,
                batch_bytes = last.durable_batch_bytes,
                sync_latency_micros = last.sync_latency_micros,
                commit_records = records
                    .iter()
                    .filter(|record| matches!(record, JournalRecord::Commit(_)))
                    .count(),
                maintenance_records = records
                    .iter()
                    .filter(|record| matches!(record, JournalRecord::Maintenance(_)))
                    .count(),
                "Durable journal records appended"
            );
        }
        Ok(results)
    }

    pub fn submit_maintenance(
        &self,
        plan: PreparedMaintenancePlan,
    ) -> Result<MaintenanceAppendContext> {
        self.ensure_open()?;

        let mut next_maintenance_id = self.inner.next_maintenance_id.lock().unwrap();
        let maintenance_id = *next_maintenance_id;
        let record = plan.into_record(maintenance_id);
        let results = self.append_records(&[JournalRecord::Maintenance(record.clone())])?;
        let result = results.first().copied().ok_or_else(|| {
            paro_error::internal("journal coordinator returned no append result for maintenance")
        })?;
        *next_maintenance_id = maintenance_id.saturating_add(1);

        Ok(MaintenanceAppendContext {
            maintenance_id,
            lsn: result.lsn,
            durable_batch_lsn: result.durable_batch_lsn,
            durable_batch_size: result.durable_batch_size,
            durable_batch_bytes: result.durable_batch_bytes,
            sync_latency_micros: result.sync_latency_micros,
            record,
        })
    }

    fn ensure_open(&self) -> Result<()> {
        let state = self.inner.state.lock().unwrap();
        if state.shutdown {
            return Err(paro_error::internal(
                "journal coordinator is shutting down and cannot append records",
            ));
        }
        Ok(())
    }
}

impl Clone for JournalCoordinator {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl Drop for JournalCoordinator {
    fn drop(&mut self) {
        if Arc::strong_count(&self.inner) == 1 {
            self.inner.state.lock().unwrap().shutdown = true;
        }
    }
}

fn update_durable_frontier(inner: &Arc<JournalCoordinatorInner>, durable_lsn: u64) {
    if let Some(runtime) = inner.apply_runtime.lock().unwrap().as_ref() {
        runtime.note_durable_append(durable_lsn);
        return;
    }

    let mut state = inner.state.lock().unwrap();
    state.fallback_frontiers.durable_lsn = state.fallback_frontiers.durable_lsn.max(durable_lsn);
}

fn bump_mutex_min(mutex: &Mutex<u64>, min_value: u64) -> u64 {
    let mut guard = mutex.lock().unwrap();
    if *guard < min_value {
        *guard = min_value;
    }
    *guard
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::appender::JournalSink;
    use crate::{ApplyRequest, JournalApplyRuntime, TabletApplyPart, WaitMode};
    use parking_lot::Mutex as ParkingMutex;
    use paro_common::durability::PreparedCommitPlan;
    use paro_common::journal::MaintenanceKind;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Condvar, Mutex as StdMutex};
    use std::thread;
    use std::time::Duration;

    #[derive(Default)]
    struct RecordingSink {
        batch_sizes: ParkingMutex<Vec<usize>>,
    }

    impl JournalSink for RecordingSink {
        fn append_batch(&self, frames: &[Vec<u8>]) -> Result<()> {
            self.batch_sizes.lock().push(frames.len());
            Ok(())
        }
    }

    struct FailOnceSink {
        should_fail: AtomicBool,
    }

    impl FailOnceSink {
        fn new() -> Self {
            Self {
                should_fail: AtomicBool::new(true),
            }
        }
    }

    impl JournalSink for FailOnceSink {
        fn append_batch(&self, _frames: &[Vec<u8>]) -> Result<()> {
            if self.should_fail.swap(false, Ordering::AcqRel) {
                return Err(paro_error::internal("injected append failure"));
            }
            Ok(())
        }
    }

    fn empty_commit_plan(txn_id: u64) -> PreparedCommitPlan {
        PreparedCommitPlan {
            txn_id,
            start_time: txn_id,
            catalog_ops: Vec::new(),
            storage_ops: Vec::new(),
            apply_descriptors: Vec::new(),
            deferred_tasks: Vec::new(),
            tablets: Vec::new(),
        }
    }

    fn empty_maintenance_plan() -> PreparedMaintenancePlan {
        PreparedMaintenancePlan {
            kind: MaintenanceKind::Compaction,
            catalog_ops: Vec::new(),
            storage_ops: Vec::new(),
            apply_descriptors: Vec::new(),
            deferred_tasks: Vec::new(),
            tablets: Vec::new(),
        }
    }

    #[test]
    fn append_records_updates_durable_frontier_without_publish_callbacks() {
        let sink = Arc::new(RecordingSink::default());
        let appender = Arc::new(JournalAppender::new(sink.clone()));
        let coordinator = JournalCoordinator::new(Some(appender));
        let record = JournalRecord::Commit(empty_commit_plan(7).into_record(12));

        let results = coordinator.append_records(&[record]).unwrap();

        assert_eq!(*sink.batch_sizes.lock(), vec![1]);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].lsn, 1);
        let frontiers = coordinator.frontiers();
        assert_eq!(frontiers.durable_lsn, 1);
        assert_eq!(frontiers.published_lsn, 0);
    }

    #[test]
    fn submit_maintenance_allocates_after_durable_append() {
        let sink = Arc::new(RecordingSink::default());
        let appender = Arc::new(JournalAppender::new(sink.clone()));
        let coordinator = JournalCoordinator::new(Some(appender));
        coordinator.sync_maintenance_id_with(9);

        let ctx = coordinator
            .submit_maintenance(empty_maintenance_plan())
            .unwrap();

        assert_eq!(*sink.batch_sizes.lock(), vec![1]);
        assert_eq!(ctx.maintenance_id, 10);
        assert_eq!(ctx.record.maintenance_id, 10);
        assert_eq!(ctx.lsn, 1);
    }

    #[test]
    fn append_failure_does_not_consume_maintenance_id() {
        let appender = Arc::new(JournalAppender::new(Arc::new(FailOnceSink::new())));
        let coordinator = JournalCoordinator::new(Some(appender));
        coordinator.sync_maintenance_id_with(9);

        let err = coordinator
            .submit_maintenance(empty_maintenance_plan())
            .unwrap_err();
        assert!(err.to_string().contains("journal appender halted"));

        assert_eq!(*coordinator.inner.next_maintenance_id.lock().unwrap(), 10);
    }

    #[test]
    fn frontiers_proxy_bound_apply_runtime_without_crossing_apply_gap() {
        let sink = Arc::new(RecordingSink::default());
        let appender = Arc::new(JournalAppender::new(sink));
        let coordinator = JournalCoordinator::new(Some(appender));
        let runtime = Arc::new(JournalApplyRuntime::new());
        coordinator.bind_apply_runtime(Arc::clone(&runtime));

        let first_record = JournalRecord::Commit(empty_commit_plan(1).into_record(1));
        let first_append = coordinator.append_records(&[first_record]).unwrap()[0];
        let slow_part_started = Arc::new(AtomicBool::new(false));
        let release_slow_part = Arc::new((StdMutex::new(false), Condvar::new()));
        let runtime_first = Arc::clone(&runtime);
        let slow_part_started_first = Arc::clone(&slow_part_started);
        let release_slow_part_first = Arc::clone(&release_slow_part);

        let first = thread::spawn(move || {
            runtime_first
                .submit(ApplyRequest {
                    lsn: first_append.lsn,
                    durable_batch_lsn: first_append.durable_batch_lsn,
                    commit_id: Some(1),
                    wait_mode: WaitMode::Published,
                    catalog_serial: false,
                    catalog_pre: Box::new(|| Ok(())),
                    tablet_parts: vec![TabletApplyPart {
                        tablet_id: 41,
                        apply: Box::new(move || {
                            slow_part_started_first.store(true, Ordering::Release);
                            let (lock, wake) = &*release_slow_part_first;
                            let mut released = lock.lock().unwrap();
                            while !*released {
                                released = wake.wait(released).unwrap();
                            }
                            Ok(())
                        }),
                    }],
                    descriptor_phase: Box::new(|| Ok(())),
                    catalog_post: Box::new(|| Ok(())),
                    on_published: Box::new(|| Ok(())),
                })
                .unwrap();
        });

        for _ in 0..20 {
            if slow_part_started.load(Ordering::Acquire) {
                break;
            }
            thread::sleep(Duration::from_millis(10));
        }
        assert!(slow_part_started.load(Ordering::Acquire));

        let second_record = JournalRecord::Commit(empty_commit_plan(2).into_record(2));
        let second_append = coordinator.append_records(&[second_record]).unwrap()[0];
        let stalled = coordinator.frontiers();
        assert_eq!(stalled.durable_lsn, second_append.lsn);
        assert_eq!(stalled.applied_lsn, 0);
        assert_eq!(stalled.published_lsn, 0);

        let (lock, wake) = &*release_slow_part;
        *lock.lock().unwrap() = true;
        wake.notify_all();
        first.join().unwrap();

        runtime
            .submit(ApplyRequest {
                lsn: second_append.lsn,
                durable_batch_lsn: second_append.durable_batch_lsn,
                commit_id: Some(2),
                wait_mode: WaitMode::Published,
                catalog_serial: false,
                catalog_pre: Box::new(|| Ok(())),
                tablet_parts: Vec::new(),
                descriptor_phase: Box::new(|| Ok(())),
                catalog_post: Box::new(|| Ok(())),
                on_published: Box::new(|| Ok(())),
            })
            .unwrap();

        let frontiers = coordinator.frontiers();
        assert_eq!(frontiers.applied_lsn, second_append.lsn);
        assert_eq!(frontiers.published_lsn, second_append.lsn);
    }
}
