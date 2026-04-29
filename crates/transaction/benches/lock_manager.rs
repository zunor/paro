// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::atomic::{AtomicU64, Ordering};

use divan::{black_box, Bencher};
use paro_transaction::{
    DatabaseId, LockMode, LockNamespace, LockRequest, LockResource, ShardedLockManager, TableId,
    TxnId, TxnLockSet,
};

const SHARD_ARGS: [usize; 3] = [32, 64, 128];
const ACTIVE_FINE_LOCKS: usize = 10_000;

fn main() {
    divan::main();
}

fn ns() -> LockNamespace {
    LockNamespace::single_tenant(DatabaseId::new(1))
}

fn pk(key_hash: u64) -> LockResource {
    LockResource::primary_key(ns(), TableId::new(10), 20, key_hash)
}

fn table() -> LockResource {
    LockResource::Table {
        namespace: ns(),
        table_id: TableId::new(10),
    }
}

struct HeldLockManager {
    manager: ShardedLockManager,
    _locks: Vec<TxnLockSet>,
}

fn build_manager_with_fine_shared_locks(shards: usize) -> HeldLockManager {
    let manager = ShardedLockManager::with_shards(shards);
    let mut locks = Vec::with_capacity(ACTIVE_FINE_LOCKS);
    for idx in 0..ACTIVE_FINE_LOCKS {
        let txn_id = TxnId::new(idx as u64 + 1);
        let key_hash = idx as u64 + 1;
        let lock = manager.lock_one(txn_id, pk(key_hash), LockMode::S).unwrap();
        locks.push(lock);
    }
    HeldLockManager {
        manager,
        _locks: locks,
    }
}

#[divan::bench(args = SHARD_ARGS, sample_count = 10)]
fn fine_key_lock_unlock_without_coarse_gate(bencher: Bencher, shards: usize) {
    let manager = ShardedLockManager::with_shards(shards);
    let next_txn = AtomicU64::new(1);
    bencher.bench_local(|| {
        let txn_id = next_txn.fetch_add(1, Ordering::Relaxed);
        let lock = manager
            .lock_one(TxnId::new(txn_id), pk(txn_id), LockMode::X)
            .unwrap();
        black_box(lock.len());
        drop(lock);
    });
}

#[divan::bench(args = SHARD_ARGS, sample_count = 10)]
fn coarse_table_lock_unlock_without_replication(bencher: Bencher, shards: usize) {
    let manager = ShardedLockManager::with_shards(shards);
    let next_txn = AtomicU64::new(1);
    bencher.bench_local(|| {
        let txn_id = next_txn.fetch_add(1, Ordering::Relaxed);
        let lock = manager
            .lock_one(TxnId::new(txn_id), table(), LockMode::X)
            .unwrap();
        black_box(lock.len());
        drop(lock);
    });
}

#[divan::bench(args = SHARD_ARGS, sample_count = 10, sample_size = 1)]
fn coarse_table_conflict_scan_with_10k_fine_holders(bencher: Bencher, shards: usize) {
    let held = build_manager_with_fine_shared_locks(shards);
    bencher.bench_local(|| {
        let err = held
            .manager
            .lock_one(
                TxnId::new(ACTIVE_FINE_LOCKS as u64 + 1),
                table(),
                LockMode::X,
            )
            .unwrap_err();
        black_box(err);
    });
}

#[divan::bench(args = SHARD_ARGS, sample_count = 10, sample_size = 1)]
fn lock_many_primary_key_batch_without_coarse_gate(bencher: Bencher, shards: usize) {
    let manager = ShardedLockManager::with_shards(shards);
    let next_txn = AtomicU64::new(1);
    bencher.bench_local(|| {
        let txn_id = next_txn.fetch_add(1, Ordering::Relaxed);
        let requests = (0..64).map(|idx| {
            LockRequest::new(
                pk(txn_id.wrapping_mul(1_000).wrapping_add(idx)),
                LockMode::X,
            )
        });
        let lock = manager.lock_many(TxnId::new(txn_id), requests).unwrap();
        black_box(lock.len());
        drop(lock);
    });
}
