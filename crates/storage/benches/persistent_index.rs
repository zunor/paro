// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::OnceLock;

use divan::Bencher;
use paro_storage::primary_key::{
    ImmutableIndexReader, ImmutableIndexWriter, PersistentIndex, PrimaryIndexVersion, RowID,
};
use tempfile::TempDir;

const ENTRY_COUNT: u32 = 200_000;
const FLUSH_ENTRY_COUNT: usize = 40_000;
const COMPACTION_ENTRY_COUNT: usize = 10_000;
const COMPACTION_FLUSH_ROUNDS: usize = 6;

fn make_key(seed: u32) -> Vec<u8> {
    format!("persistent-index-bench-key-{seed:08x}").into_bytes()
}

fn make_entries(start: u32, count: usize) -> Vec<(Vec<u8>, RowID)> {
    (0..count)
        .map(|offset| {
            let row = start + offset as u32;
            (
                make_key(row),
                RowID::new(1, paro_storage::rowset::SegmentRowId::from_raw(row)),
            )
        })
        .collect()
}

struct BenchState {
    _dir: TempDir,
    immutable_path: std::path::PathBuf,
    persistent_dir: std::path::PathBuf,
    hit_key: Vec<u8>,
    miss_key: Vec<u8>,
}

impl BenchState {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let immutable_path = dir.path().join("immutable.idx");
        let persistent_dir = dir.path().join("persistent");

        let entries: Vec<_> = (0..ENTRY_COUNT)
            .map(|i| {
                (
                    make_key(i),
                    RowID::new(1, paro_storage::rowset::SegmentRowId::from_raw(i)),
                )
            })
            .collect();
        let immutable_entries: Vec<_> = entries
            .iter()
            .map(|(key, row_id)| (key.clone(), PrimaryIndexVersion::live(*row_id, 1)))
            .collect();
        ImmutableIndexWriter::default()
            .write_entries(&immutable_path, &immutable_entries)
            .unwrap();

        let mut persistent = PersistentIndex::new(&persistent_dir).unwrap();
        let empty = paro_storage::primary_key::PrimaryIndex::new();
        for chunk in entries.chunks(40_000) {
            persistent.apply_upserts(chunk).unwrap();
            persistent.flush_l0(&empty, true).unwrap();
        }

        Self {
            _dir: dir,
            immutable_path,
            persistent_dir,
            hit_key: make_key(ENTRY_COUNT / 2),
            miss_key: b"persistent-index-miss".to_vec(),
        }
    }
}

fn state() -> &'static BenchState {
    static STATE: OnceLock<BenchState> = OnceLock::new();
    STATE.get_or_init(BenchState::new)
}

fn main() {
    divan::main();
}

#[divan::bench]
fn immutable_index_hit() {
    let state = state();
    let reader = ImmutableIndexReader::open_cached(&state.immutable_path).unwrap();
    divan::black_box(reader.get(&state.hit_key).unwrap());
}

#[divan::bench]
fn immutable_index_miss() {
    let state = state();
    let reader = ImmutableIndexReader::open_cached(&state.immutable_path).unwrap();
    divan::black_box(reader.get(&state.miss_key).unwrap());
}

#[divan::bench]
fn persistent_index_hit() {
    let state = state();
    let persistent = PersistentIndex::new(&state.persistent_dir).unwrap();
    divan::black_box(persistent.get(&state.hit_key).unwrap());
}

#[divan::bench]
fn persistent_index_miss() {
    let state = state();
    let persistent = PersistentIndex::new(&state.persistent_dir).unwrap();
    divan::black_box(persistent.get(&state.miss_key).unwrap());
}

#[divan::bench(sample_count = 10)]
fn persistent_index_flush_l0(bencher: Bencher) {
    let entries = make_entries(0, FLUSH_ENTRY_COUNT);
    bencher.counter(FLUSH_ENTRY_COUNT).bench_local(|| {
        let dir = tempfile::tempdir().unwrap();
        let mut persistent = PersistentIndex::new(dir.path()).unwrap();
        let empty = paro_storage::primary_key::PrimaryIndex::new();
        persistent.apply_upserts(&entries).unwrap();
        persistent.flush_l0(&empty, true).unwrap();
        divan::black_box(persistent.get(&entries[entries.len() / 2].0).unwrap());
    });
}

#[divan::bench(sample_count = 10)]
fn persistent_index_minor_compaction(bencher: Bencher) {
    bencher
        .counter(COMPACTION_ENTRY_COUNT * COMPACTION_FLUSH_ROUNDS)
        .bench_local(|| {
            let dir = tempfile::tempdir().unwrap();
            let mut persistent = PersistentIndex::new(dir.path()).unwrap();
            let empty = paro_storage::primary_key::PrimaryIndex::new();
            for round in 0..COMPACTION_FLUSH_ROUNDS {
                let entries = make_entries(
                    (round * COMPACTION_ENTRY_COUNT) as u32,
                    COMPACTION_ENTRY_COUNT,
                );
                persistent.apply_upserts(&entries).unwrap();
                persistent.flush_l0(&empty, true).unwrap();
            }
            let reopened = PersistentIndex::new(dir.path()).unwrap();
            divan::black_box(
                reopened
                    .get(&make_key((COMPACTION_ENTRY_COUNT / 2) as u32))
                    .unwrap(),
            );
        });
}
