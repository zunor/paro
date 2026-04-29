// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;
use std::thread;

use divan::{black_box, Bencher};
use paro_storage::transaction::version_info::VersionInfo;

const ROWS: usize = 16_384;
const SCAN_ITERS: usize = 128;
const DELETE_ITERS: usize = 64;
const READER_THREADS: usize = 4;

fn main() {
    divan::main();
}

fn build_versions() -> Arc<Vec<VersionInfo>> {
    Arc::new((0..ROWS).map(|_| VersionInfo::new(1)).collect())
}

#[divan::bench]
fn visible_snapshot_single_thread(bencher: Bencher) {
    let versions = build_versions();
    bencher.bench_local(|| {
        let mut visible = 0usize;
        for _ in 0..SCAN_ITERS {
            for version in versions.iter() {
                if version.is_visible(0, 2) {
                    visible += 1;
                }
            }
        }
        black_box(visible);
    });
}

#[divan::bench]
fn concurrent_visibility_with_delete_publish(bencher: Bencher) {
    let versions = build_versions();
    bencher.bench_local(|| {
        let mut readers = Vec::with_capacity(READER_THREADS);
        for _ in 0..READER_THREADS {
            let versions = versions.clone();
            readers.push(thread::spawn(move || {
                let mut visible = 0usize;
                for _ in 0..SCAN_ITERS {
                    for version in versions.iter() {
                        if version.is_visible(0, 2) {
                            visible += 1;
                        }
                    }
                }
                visible
            }));
        }

        for txn_id in 3..(3 + DELETE_ITERS as u64) {
            let idx = (txn_id as usize) % ROWS;
            black_box(versions[idx].try_delete(txn_id));
        }

        let visible: usize = readers
            .into_iter()
            .map(|reader| reader.join().expect("reader joins"))
            .sum();
        black_box(visible);
    });
}
