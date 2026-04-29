// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use divan::{black_box, Bencher};
use paro_transaction::{
    ActiveRwTxnHandle, ActiveTxnRegistry, BackfillLease, CheckpointLease, CommitTs,
    DerivedLagLease, LayoutEpoch, LayoutEpochLease, ReadSnapshotLease, ReadTs, RetentionRegistry,
    TxnId, WriteConflictLease,
};

const SHARD_ARGS: [usize; 3] = [32, 64, 128];
const ACTIVE_LEASES: usize = 10_000;

fn main() {
    divan::main();
}

#[derive(Debug)]
struct LegacyTxn {
    txn_id: u64,
    read_ts: u64,
    start_ts: u64,
    read_write: bool,
}

#[derive(Default)]
struct LegacyActiveList {
    active: RwLock<Vec<Arc<LegacyTxn>>>,
    next_txn: AtomicU64,
}

impl LegacyActiveList {
    fn begin_rw(&self) -> Arc<LegacyTxn> {
        let id = self.next_txn.fetch_add(1, Ordering::Relaxed) + 1;
        let txn = Arc::new(LegacyTxn {
            txn_id: id,
            read_ts: id,
            start_ts: id,
            read_write: true,
        });
        self.active.write().unwrap().push(txn.clone());
        txn
    }

    fn end(&self, txn: &Arc<LegacyTxn>) {
        self.active
            .write()
            .unwrap()
            .retain(|active| !Arc::ptr_eq(active, txn));
    }

    fn sweep_rw_min(&self) -> (u64, u64, u64) {
        let active = self.active.read().unwrap();
        let mut read_ts = u64::MAX;
        let mut start_ts = u64::MAX;
        let mut count = 0_u64;
        for txn in active.iter() {
            if txn.read_write {
                read_ts = read_ts.min(txn.read_ts);
                start_ts = start_ts.min(txn.start_ts);
                count += 1;
            }
            black_box(txn.txn_id);
        }
        (read_ts, start_ts, count)
    }
}

fn slots_per_shard(shards: usize) -> usize {
    (ACTIVE_LEASES / shards).saturating_add(128)
}

fn build_active_registry(shards: usize) -> (ActiveTxnRegistry, Vec<ActiveRwTxnHandle>) {
    let registry = ActiveTxnRegistry::with_capacity(shards, slots_per_shard(shards));
    let mut handles = Vec::with_capacity(ACTIVE_LEASES);
    for idx in 0..ACTIVE_LEASES {
        handles.push(
            registry
                .register_read_write(
                    TxnId::new(idx as u64 + 1),
                    ReadTs::new(idx as u64 + 1),
                    ReadTs::new(idx as u64 + 1),
                )
                .unwrap(),
        );
    }
    registry.refresh_watermarks();
    (registry, handles)
}

fn build_legacy_active_list() -> (LegacyActiveList, Vec<Arc<LegacyTxn>>) {
    let legacy = LegacyActiveList::default();
    let mut handles = Vec::with_capacity(ACTIVE_LEASES);
    for _ in 0..ACTIVE_LEASES {
        handles.push(legacy.begin_rw());
    }
    (legacy, handles)
}

enum HeldRetentionLease {
    Read(ReadSnapshotLease),
    Layout(LayoutEpochLease),
    Backfill(BackfillLease),
    Derived(DerivedLagLease),
    Checkpoint(CheckpointLease),
    Conflict(WriteConflictLease),
}

impl HeldRetentionLease {
    fn lease_id_raw(&self) -> u64 {
        match self {
            Self::Read(lease) => lease.lease_id().unwrap().into_raw(),
            Self::Layout(lease) => lease.lease_id().unwrap().into_raw(),
            Self::Backfill(lease) => lease.lease_id().unwrap().into_raw(),
            Self::Derived(lease) => lease.lease_id().unwrap().into_raw(),
            Self::Checkpoint(lease) => lease.lease_id().unwrap().into_raw(),
            Self::Conflict(lease) => lease.lease_id().unwrap().into_raw(),
        }
    }
}

fn build_retention_registry(shards: usize) -> (RetentionRegistry, Vec<HeldRetentionLease>) {
    let registry = RetentionRegistry::with_capacity(shards, slots_per_shard(shards));
    let mut leases = Vec::with_capacity(ACTIVE_LEASES);
    for idx in 0..ACTIVE_LEASES {
        let ts = idx as u64 + 1;
        let lease = match idx % 6 {
            0 => HeldRetentionLease::Read(registry.lease_read_snapshot(ReadTs::new(ts)).unwrap()),
            1 => HeldRetentionLease::Layout(
                registry.lease_layout_epoch(LayoutEpoch::new(ts)).unwrap(),
            ),
            2 => HeldRetentionLease::Backfill(registry.lease_backfill(CommitTs::new(ts)).unwrap()),
            3 => {
                HeldRetentionLease::Derived(registry.lease_derived_lag(CommitTs::new(ts)).unwrap())
            }
            4 => HeldRetentionLease::Checkpoint(
                registry.lease_checkpoint(CommitTs::new(ts)).unwrap(),
            ),
            _ => HeldRetentionLease::Conflict(
                registry.lease_write_conflict(CommitTs::new(ts)).unwrap(),
            ),
        };
        leases.push(lease);
    }
    registry.refresh_watermarks();
    (registry, leases)
}

#[divan::bench(sample_count = 10)]
fn legacy_rwlock_vec_begin_end(bencher: Bencher) {
    let legacy = LegacyActiveList::default();
    bencher.bench_local(|| {
        let txn = legacy.begin_rw();
        black_box(txn.read_ts);
        legacy.end(&txn);
    });
}

#[divan::bench(sample_count = 10)]
fn legacy_rwlock_vec_begin_end_with_10k_active(bencher: Bencher) {
    let (legacy, handles) = build_legacy_active_list();
    bencher.bench_local(|| {
        let txn = legacy.begin_rw();
        black_box((txn.read_ts, handles.len()));
        legacy.end(&txn);
    });
}

#[divan::bench(args = SHARD_ARGS, sample_count = 10)]
fn active_registry_begin_end(bencher: Bencher, shards: usize) {
    let registry = ActiveTxnRegistry::with_capacity(shards, slots_per_shard(shards));
    let next_txn = AtomicU64::new(1);
    bencher.bench_local(|| {
        let id = next_txn.fetch_add(1, Ordering::Relaxed);
        let handle = registry
            .register_read_write(TxnId::new(id), ReadTs::new(id), ReadTs::new(id))
            .unwrap();
        black_box(handle.info().unwrap().read_ts);
        handle.release().unwrap();
    });
}

#[divan::bench(args = SHARD_ARGS, sample_count = 10)]
fn active_registry_begin_end_with_10k_active(bencher: Bencher, shards: usize) {
    let (registry, handles) = build_active_registry(shards);
    let next_txn = AtomicU64::new(ACTIVE_LEASES as u64 + 1);
    bencher.bench_local(|| {
        let id = next_txn.fetch_add(1, Ordering::Relaxed);
        let handle = registry
            .register_read_write(TxnId::new(id), ReadTs::new(id), ReadTs::new(id))
            .unwrap();
        black_box((handle.info().unwrap().read_ts, handles.len()));
        handle.release().unwrap();
    });
}

#[divan::bench(sample_count = 10, sample_size = 1)]
fn legacy_pointer_chasing_sweep_10k(bencher: Bencher) {
    let (legacy, _handles) = build_legacy_active_list();
    bencher.bench_local(|| black_box(legacy.sweep_rw_min()));
}

#[divan::bench(args = SHARD_ARGS, sample_count = 10, sample_size = 1)]
fn active_registry_aggregator_sweep_10k(bencher: Bencher, shards: usize) {
    let (registry, _handles) = build_active_registry(shards);
    bencher.bench_local(|| black_box(registry.refresh_watermarks()));
}

#[divan::bench(args = SHARD_ARGS, sample_count = 10, sample_size = 1)]
fn active_registry_confirmed_barrier_10k(bencher: Bencher, shards: usize) {
    let (registry, _handles) = build_active_registry(shards);
    bencher.bench_local(|| black_box(registry.confirmed_watermarks()));
}

#[divan::bench(args = SHARD_ARGS, sample_count = 10)]
fn retention_registry_six_lease_begin_end(bencher: Bencher, shards: usize) {
    let registry = RetentionRegistry::with_capacity(shards, slots_per_shard(shards));
    let next_ts = AtomicU64::new(1);
    bencher.bench_local(|| {
        let ts = next_ts.fetch_add(6, Ordering::Relaxed);
        let read = registry.lease_read_snapshot(ReadTs::new(ts)).unwrap();
        let layout = registry
            .lease_layout_epoch(LayoutEpoch::new(ts + 1))
            .unwrap();
        let backfill = registry.lease_backfill(CommitTs::new(ts + 2)).unwrap();
        let derived = registry.lease_derived_lag(CommitTs::new(ts + 3)).unwrap();
        let checkpoint = registry.lease_checkpoint(CommitTs::new(ts + 4)).unwrap();
        let conflict = registry
            .lease_write_conflict(CommitTs::new(ts + 5))
            .unwrap();
        black_box(conflict.info().unwrap().lease_id);
        drop((read, layout, backfill, derived, checkpoint, conflict));
    });
}

#[divan::bench(args = SHARD_ARGS, sample_count = 10, sample_size = 1)]
fn retention_registry_aggregator_sweep_10k(bencher: Bencher, shards: usize) {
    let (registry, leases) = build_retention_registry(shards);
    let lease_id_sum: u64 = leases.iter().map(HeldRetentionLease::lease_id_raw).sum();
    bencher.bench_local(|| {
        let watermarks = registry.refresh_watermarks();
        black_box((watermarks, leases.len(), lease_id_sum))
    });
}

#[divan::bench(args = SHARD_ARGS, sample_count = 10, sample_size = 1)]
fn retention_registry_confirmed_barrier_10k(bencher: Bencher, shards: usize) {
    let (registry, leases) = build_retention_registry(shards);
    let lease_id_sum: u64 = leases.iter().map(HeldRetentionLease::lease_id_raw).sum();
    bencher.bench_local(|| {
        let watermarks = registry.confirmed_watermarks();
        black_box((watermarks, leases.len(), lease_id_sum))
    });
}
