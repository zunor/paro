// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

use divan::{black_box, Bencher};
use paro_common::durability::{PreparedCommitPlan, PreparedMaintenancePlan};
use paro_common::effect::{
    ArtifactNamespace, ArtifactRef, DeletePatchEncoding, DeletePatchGroup, DeletePatchInline,
    DeletePatchRef, DeletePatchSegment,
};
use paro_common::error::Result;
use paro_common::journal::{CommitRecord, JournalRecord, MaintenanceKind, MaintenanceRecord};
use paro_journal::{
    encode_record, ApplyRequest, JournalAppender, JournalAppenderMetricsSnapshot,
    JournalApplyRuntime, JournalCoordinator, JournalSink, TabletApplyPart, WaitMode,
};

const GROUP_COMMIT_THREADS: [usize; 3] = [4, 16, 64];
const MIXED_BATCH_REQUESTS: [usize; 3] = [4, 16, 64];
const DELETE_PATCH_ROWS: [usize; 3] = [64, 512, 4096];
const APPLY_EXECUTOR_TABLETS: [usize; 3] = [32, 128, 512];
const ADAPTIVE_BATCH_ARGS: [(usize, u64); 3] = [(16, 0), (16, 50), (16, 250)];
const FAKE_FSYNC_MICROS: u64 = 200;
const APPLY_WORK_MICROS: u64 = 100;

fn main() {
    divan::main();
}

#[derive(Default)]
struct BenchSink {
    fsync_delay: Duration,
}

impl BenchSink {
    fn with_delay(delay: Duration) -> Self {
        Self { fsync_delay: delay }
    }
}

impl JournalSink for BenchSink {
    fn append_batch(&self, _frames: &[Vec<u8>]) -> Result<()> {
        if !self.fsync_delay.is_zero() {
            thread::sleep(self.fsync_delay);
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

fn empty_maintenance_plan(kind: MaintenanceKind) -> PreparedMaintenancePlan {
    PreparedMaintenancePlan {
        kind,
        catalog_ops: Vec::new(),
        storage_ops: Vec::new(),
        apply_descriptors: Vec::new(),
        deferred_tasks: Vec::new(),
        tablets: Vec::new(),
    }
}

fn make_inline_patch(row_count: usize) -> DeletePatchRef {
    let mut remaining = row_count as u32;
    let mut rowset_id = 100u64;
    let mut groups = Vec::new();
    while remaining > 0 {
        let segment_rows = remaining.min(64);
        groups.push(DeletePatchGroup {
            rowset_id,
            segments: vec![DeletePatchSegment {
                segment_id: 0,
                row_offsets_delta: (0..segment_rows).map(|row| row + 1).collect(),
            }],
        });
        rowset_id += 1;
        remaining -= segment_rows;
    }

    DeletePatchRef::Inline(DeletePatchInline {
        encoding: DeletePatchEncoding::GroupedRowOffsetDeltaV1,
        row_count: row_count as u32,
        groups,
    })
}

fn make_delete_patch_record(row_count: usize, artifact_backed: bool) -> JournalRecord {
    let patch = if artifact_backed {
        DeletePatchRef::Artifact(ArtifactRef {
            namespace: ArtifactNamespace::DeletePatch,
            locator: vec!["bench".to_string(), format!("delete_patch_{row_count}.bin")],
        })
    } else {
        make_inline_patch(row_count)
    };

    JournalRecord::Commit(CommitRecord {
        txn_id: 7,
        start_time: 11,
        commit_id: 13,
        catalog_ops: Vec::new(),
        storage_ops: vec![paro_common::effect::StorageCommitOp::Tablet(
            paro_common::effect::TabletApplyOp {
                tablet_id: 41,
                mutations: vec![paro_common::effect::TabletMutation::ApplyDeletePatch {
                    patch,
                    deleted_row_count: row_count as u32,
                }],
            },
        )],
        apply_descriptors: Vec::new(),
        deferred_tasks: Vec::new(),
    })
}

fn run_commit_burst(
    thread_count: usize,
    follower_gap_micros: u64,
) -> JournalAppenderMetricsSnapshot {
    let appender = Arc::new(JournalAppender::new(Arc::new(BenchSink::with_delay(
        Duration::from_micros(FAKE_FSYNC_MICROS),
    ))));
    let coordinator = Arc::new(JournalCoordinator::new(Some(Arc::clone(&appender))));
    let barrier = Arc::new(Barrier::new(thread_count));
    let mut handles = Vec::with_capacity(thread_count);

    for tid in 0..thread_count {
        let coordinator = Arc::clone(&coordinator);
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            barrier.wait();
            if follower_gap_micros != 0 && tid != 0 {
                thread::sleep(Duration::from_micros(follower_gap_micros));
            }
            let ctx = coordinator
                .submit_commit_context(empty_commit_plan(tid as u64 + 1), |_| Ok(()))
                .unwrap();
            black_box(ctx.lsn);
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }

    appender.metrics()
}

fn run_mixed_batch(request_count: usize) -> JournalAppenderMetricsSnapshot {
    let appender = Arc::new(JournalAppender::new(Arc::new(BenchSink::with_delay(
        Duration::from_micros(FAKE_FSYNC_MICROS),
    ))));
    let coordinator = Arc::new(JournalCoordinator::new(Some(Arc::clone(&appender))));
    let barrier = Arc::new(Barrier::new(request_count));
    let mut handles = Vec::with_capacity(request_count);

    for idx in 0..request_count {
        let coordinator = Arc::clone(&coordinator);
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            barrier.wait();
            if idx % 2 == 0 {
                let ctx = coordinator
                    .submit_commit_context(empty_commit_plan(idx as u64 + 1), |_| Ok(()))
                    .unwrap();
                black_box(ctx.lsn);
            } else {
                let ctx = coordinator
                    .submit_maintenance_context(
                        empty_maintenance_plan(MaintenanceKind::Compaction),
                        |_| Ok(()),
                    )
                    .unwrap();
                black_box(ctx.lsn);
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }

    appender.metrics()
}

fn run_apply_executor_round(tablet_count: usize) -> u64 {
    let runtime = Arc::new(JournalApplyRuntime::new());
    let barrier = Arc::new(Barrier::new(tablet_count));
    let mut handles = Vec::with_capacity(tablet_count);

    for idx in 0..tablet_count {
        let runtime = Arc::clone(&runtime);
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            barrier.wait();
            runtime
                .submit(ApplyRequest {
                    lsn: idx as u64 + 1,
                    durable_batch_lsn: idx as u64 + 1,
                    commit_id: Some(idx as u64 + 1),
                    wait_mode: WaitMode::Published,
                    catalog_serial: false,
                    catalog_pre: Box::new(|| Ok(())),
                    tablet_parts: vec![TabletApplyPart {
                        tablet_id: idx as u64 + 1,
                        apply: Box::new(|| {
                            thread::sleep(Duration::from_micros(APPLY_WORK_MICROS));
                            Ok(())
                        }),
                    }],
                    descriptor_phase: Box::new(|| Ok(())),
                    catalog_post: Box::new(|| Ok(())),
                    on_published: Box::new(|| Ok(())),
                })
                .unwrap();
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let peak = runtime.metrics().active_workers_peak;
    assert!(peak <= 16);
    peak
}

#[divan::bench(args = GROUP_COMMIT_THREADS, sample_count = 10, sample_size = 1)]
fn group_commit_benchmark(bencher: Bencher, thread_count: usize) {
    bencher.counter(thread_count).bench_local(|| {
        let metrics = run_commit_burst(thread_count, 0);
        black_box((metrics.group_count, metrics.group_size_peak));
    });
}

#[divan::bench(args = GROUP_COMMIT_THREADS, sample_count = 10, sample_size = 1)]
fn batch_queue_leader_follower_benchmark(bencher: Bencher, thread_count: usize) {
    bencher.counter(thread_count).bench_local(|| {
        let metrics = run_commit_burst(thread_count, 50);
        black_box((metrics.group_count, metrics.group_size_last));
    });
}

#[divan::bench(args = ADAPTIVE_BATCH_ARGS, sample_count = 10, sample_size = 1)]
fn adaptive_batching_latency_throughput_benchmark(
    bencher: Bencher,
    (thread_count, follower_gap_micros): (usize, u64),
) {
    bencher.counter(thread_count).bench_local(|| {
        let metrics = run_commit_burst(thread_count, follower_gap_micros);
        black_box((
            metrics.group_count,
            metrics.group_size_peak,
            metrics.sync_latency_micros_peak,
        ));
    });
}

#[divan::bench(args = MIXED_BATCH_REQUESTS, sample_count = 10, sample_size = 1)]
fn mixed_commit_maintenance_batch_benchmark(bencher: Bencher, request_count: usize) {
    bencher.counter(request_count).bench_local(|| {
        let metrics = run_mixed_batch(request_count);
        black_box((metrics.group_count, metrics.group_size_peak));
    });
}

#[divan::bench(args = DELETE_PATCH_ROWS, sample_count = 10)]
fn delete_patch_encoding_inline_benchmark(bencher: Bencher, row_count: usize) {
    let record = make_delete_patch_record(row_count, false);
    bencher.counter(row_count).bench_local(|| {
        let encoded = encode_record(&record, 1).unwrap();
        black_box(encoded.len());
    });
}

#[divan::bench(args = DELETE_PATCH_ROWS, sample_count = 10)]
fn delete_patch_encoding_artifact_benchmark(bencher: Bencher, row_count: usize) {
    let record = make_delete_patch_record(row_count, true);
    bencher.counter(row_count).bench_local(|| {
        let encoded = encode_record(&record, 1).unwrap();
        black_box(encoded.len());
    });
}

#[divan::bench(args = APPLY_EXECUTOR_TABLETS, sample_count = 10, sample_size = 1)]
fn apply_executor_scaling_benchmark(bencher: Bencher, tablet_count: usize) {
    bencher.counter(tablet_count).bench_local(|| {
        let peak_workers = run_apply_executor_round(tablet_count);
        black_box(peak_workers);
    });
}

#[allow(dead_code)]
fn _mixed_record_example() -> MaintenanceRecord {
    MaintenanceRecord {
        maintenance_id: 1,
        kind: MaintenanceKind::Compaction,
        catalog_ops: Vec::new(),
        storage_ops: Vec::new(),
        apply_descriptors: Vec::new(),
        deferred_tasks: Vec::new(),
    }
}
