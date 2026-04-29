// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::{Arc, RwLock};
use std::thread;

use divan::{black_box, Bencher};
use paro_storage::tablet::versioned_rowset_catalog::{
    RowsetCatalogDescriptor, RowsetCatalogFlags, VersionedRowsetCatalog,
};
use paro_storage::tablet::Version;

const READER_THREADS: usize = 4;
const READ_ITERS: usize = 128;

fn main() {
    divan::main();
}

fn descriptor(rowset_id: u64, version: Version) -> RowsetCatalogDescriptor {
    RowsetCatalogDescriptor {
        rowset_id,
        version,
        schema_version: 1,
        physical_schema_token: 1,
        delete_vector_catalog_token: 0,
        artifact_id: rowset_id,
        flags: RowsetCatalogFlags::empty(),
        cold_meta_id: 0,
    }
}

fn build_live_catalog(rowsets: usize) -> VersionedRowsetCatalog {
    let descriptors = (0..rowsets)
        .map(|idx| descriptor(idx as u64 + 1, Version::singleton(idx as i64)))
        .collect::<Vec<_>>();
    VersionedRowsetCatalog::rebuild_from_live(descriptors, 1, rowsets as i64 - 1).unwrap()
}

fn build_history_catalog(rowsets: usize, delta_events: usize) -> VersionedRowsetCatalog {
    let mut catalog = build_live_catalog(rowsets);
    for idx in 0..delta_events {
        catalog
            .publish_delete_vector(2 + idx as u64, rowsets as i64 - 1)
            .unwrap();
    }
    catalog
}

#[divan::bench(args = [64usize, 1024], sample_count = 10)]
fn latest_fast_path(bencher: Bencher, rowsets: usize) {
    let catalog = build_live_catalog(rowsets);
    bencher.bench_local(|| {
        let cut = catalog
            .capture_entry_ids(rowsets as i64 - 1, catalog.latest_layout_epoch())
            .unwrap();
        black_box(cut.entry_ids);
    });
}

#[divan::bench(sample_count = 10)]
fn history_cut_near_fence(bencher: Bencher) {
    let catalog = build_history_catalog(64, 64);
    let target_epoch = catalog.latest_layout_epoch();
    bencher.bench_local(|| {
        let cut = catalog.capture_entry_ids(63, target_epoch).unwrap();
        black_box(cut.entry_ids);
    });
}

#[divan::bench(sample_count = 10)]
fn history_cut_worst_delta(bencher: Bencher) {
    let catalog = build_history_catalog(64, 512);
    let target_epoch = catalog.latest_layout_epoch();
    bencher.bench_local(|| {
        let cut = catalog.capture_entry_ids(63, target_epoch).unwrap();
        black_box(cut.entry_ids);
    });
}

#[divan::bench(sample_count = 10)]
fn full_ap_cut(bencher: Bencher) {
    let catalog = build_live_catalog(4096);
    bencher.bench_local(|| {
        let cut = catalog
            .capture_entry_ids(4095, catalog.latest_layout_epoch())
            .unwrap();
        black_box(cut.entry_ids);
    });
}

#[divan::bench(sample_count = 10)]
fn publish_compaction_delta(bencher: Bencher) {
    let base = build_live_catalog(8);
    bencher.bench_local(|| {
        let mut catalog = base.clone();
        let output = RowsetCatalogDescriptor {
            flags: RowsetCatalogFlags::COMPACTION_OUTPUT,
            ..descriptor(100, Version::new(0, 7))
        };
        catalog
            .publish_compaction(&[1, 2, 3, 4, 5, 6, 7, 8], output, 2, 7)
            .unwrap();
        black_box(catalog.latest_rowset_ids().len());
    });
}

#[divan::bench(sample_count = 10)]
fn concurrent_latest_and_history(bencher: Bencher) {
    let catalog = Arc::new(build_history_catalog(1024, 256));
    bencher.bench_local(|| {
        let mut readers = Vec::with_capacity(READER_THREADS);
        for thread_id in 0..READER_THREADS {
            let catalog = catalog.clone();
            readers.push(thread::spawn(move || {
                let mut total = 0usize;
                for _ in 0..READ_ITERS {
                    let layout_epoch = if thread_id % 2 == 0 {
                        catalog.latest_layout_epoch()
                    } else {
                        1
                    };
                    total += catalog
                        .capture_entry_ids(1023, layout_epoch)
                        .unwrap()
                        .entry_ids
                        .len();
                }
                total
            }));
        }
        let total: usize = readers
            .into_iter()
            .map(|reader| reader.join().unwrap())
            .sum();
        black_box(total);
    });
}

#[divan::bench(sample_count = 10)]
fn concurrent_latest_read_and_foreground_publish(bencher: Bencher) {
    bencher.bench_local(|| {
        let catalog = Arc::new(RwLock::new(build_live_catalog(1024)));
        let mut readers = Vec::with_capacity(READER_THREADS);
        for _ in 0..READER_THREADS {
            let catalog = catalog.clone();
            readers.push(thread::spawn(move || {
                let mut total = 0usize;
                for _ in 0..READ_ITERS {
                    let guard = catalog.read().unwrap();
                    total += guard
                        .capture_entry_ids(1023, guard.latest_layout_epoch())
                        .unwrap()
                        .entry_ids
                        .len();
                }
                total
            }));
        }
        {
            let mut guard = catalog.write().unwrap();
            guard.publish_delete_vector(2, 1024).unwrap();
        }
        let total: usize = readers
            .into_iter()
            .map(|reader| reader.join().unwrap())
            .sum();
        black_box(total);
    });
}
