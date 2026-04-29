// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use divan::{black_box, Bencher};
use paro_transaction::{
    CommitAckPolicy, CommitRequest, CommitSequencer, CommitSequencerOptions, CommitSequencingPlan,
    CommitTs, DatabaseId, FrozenLockSet, LockMode, LockNamespace, LockRequest, LockResource,
    ReadTs, TableId, TransactionView, TxnId,
};
use std::sync::atomic::{AtomicU64, Ordering};

const BATCH_ARGS: [usize; 3] = [16, 64, 256];

fn main() {
    divan::main();
}

fn namespace() -> LockNamespace {
    LockNamespace::single_tenant(DatabaseId::new(1))
}

fn pk(key_hash: u64) -> LockResource {
    LockResource::primary_key(namespace(), TableId::new(10), 20, key_hash)
}

fn plan(txn_id: u64, read_ts: u64, key_hash: u64) -> CommitSequencingPlan {
    let resource = pk(key_hash);
    let request = CommitRequest::new(
        DatabaseId::new(1),
        TxnId::new(txn_id),
        TransactionView::autocommit(ReadTs::new(read_ts)),
        CommitAckPolicy::RequiredPublished,
        FrozenLockSet::from_locks(vec![LockRequest::new(resource.clone(), LockMode::X)]),
        Vec::new(),
    );
    CommitSequencingPlan::new(request.commit_plan(), vec![resource])
}

fn plans(batch_size: usize, base_txn: u64) -> Vec<CommitSequencingPlan> {
    (0..batch_size)
        .map(|idx| {
            let id = base_txn + idx as u64;
            plan(id, id.saturating_sub(1), id.wrapping_mul(1_000_003))
        })
        .collect()
}

#[divan::bench(args = BATCH_ARGS, sample_count = 10)]
fn sequence_batch_no_conflicts(bencher: Bencher, batch_size: usize) {
    let sequencer = CommitSequencer::new(
        CommitTs::new(1),
        CommitSequencerOptions {
            max_group_commit_batch_size: batch_size,
            ..CommitSequencerOptions::default()
        },
    );
    let next_txn = AtomicU64::new(1);
    bencher.bench_local(|| {
        let base = next_txn.fetch_add(batch_size as u64, Ordering::Relaxed);
        let batch = sequencer
            .sequence_batch(plans(batch_size, base), |_| Ok::<_, ()>(()))
            .unwrap();
        black_box(batch.accepted.len());
    });
}

#[divan::bench(args = BATCH_ARGS, sample_count = 10)]
fn sequence_batch_with_in_batch_conflicts(bencher: Bencher, batch_size: usize) {
    let sequencer = CommitSequencer::new(
        CommitTs::new(1),
        CommitSequencerOptions {
            max_group_commit_batch_size: batch_size,
            ..CommitSequencerOptions::default()
        },
    );
    let next_txn = AtomicU64::new(1);
    bencher.bench_local(|| {
        let base = next_txn.fetch_add(batch_size as u64, Ordering::Relaxed);
        let plans = (0..batch_size)
            .map(|idx| {
                let id = base + idx as u64;
                plan(id, id.saturating_sub(1), (idx % 8) as u64)
            })
            .collect::<Vec<_>>();
        let batch = sequencer
            .sequence_batch(plans, |_| Ok::<_, ()>(()))
            .unwrap();
        black_box((batch.accepted.len(), batch.rejected.len()));
    });
}
