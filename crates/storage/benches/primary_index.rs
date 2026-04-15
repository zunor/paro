use std::sync::{Arc, Barrier, OnceLock};
use std::thread;

use divan::Bencher;
use paro_common::chunk::Chunk;
use paro_common::types::LogicalType;
use paro_common::vector::Vector;
use paro_storage::primary_key::{PrimaryIndex, PrimaryKeySerializer, RowID};
use paro_storage::tablet::tablet_schema::{KeysType, TabletColumn, TabletSchema};

const PRELOAD_KEYS: u32 = 200_000;
const LOOKUP_BATCH: usize = 16_384;
const MIXED_OPS_PER_THREAD: usize = 40_000;
const MIXED_THREADS: usize = 4;
const ENCODE_BATCH_ROWS: usize = 8_192;
const H4_LOOKUP_SCALES: [usize; 3] = [1_000_000, 10_000_000, 100_000_000];
const H4_UPSERT_KEYS: usize = 200_000;
const H4_MULTI_THREAD_UPSERT_KEYS_PER_THREAD: usize = 50_000;

fn main() {
    divan::main();
}

fn make_fixed_key(seed: u32) -> Vec<u8> {
    seed.to_be_bytes().repeat(2)
}

fn make_long_key(seed: u32) -> Vec<u8> {
    format!("primary-index-bench-long-key-{seed:08x}-payload").into_bytes()
}

struct BenchState {
    fixed: Arc<PrimaryIndex>,
    long: Arc<PrimaryIndex>,
    fixed_lookup: Vec<Vec<u8>>,
    long_lookup: Vec<Vec<u8>>,
}

impl BenchState {
    fn new() -> Self {
        let fixed = Arc::new(PrimaryIndex::with_options(16, usize::MAX / 2));
        let long = Arc::new(PrimaryIndex::with_options(16, usize::MAX / 2));

        for i in 0..PRELOAD_KEYS {
            fixed.upsert(make_fixed_key(i), RowID::new(1, i));
            long.upsert(make_long_key(i), RowID::new(1, i));
        }

        let fixed_lookup = (0..LOOKUP_BATCH)
            .map(|i| make_fixed_key((i as u32) % PRELOAD_KEYS))
            .collect();
        let long_lookup = (0..LOOKUP_BATCH)
            .map(|i| make_long_key((i as u32) % PRELOAD_KEYS))
            .collect();

        Self {
            fixed,
            long,
            fixed_lookup,
            long_lookup,
        }
    }
}

fn state() -> &'static BenchState {
    static STATE: OnceLock<BenchState> = OnceLock::new();
    STATE.get_or_init(BenchState::new)
}

struct EncodeBenchState {
    serializer: PrimaryKeySerializer,
    chunk: Chunk,
}

impl EncodeBenchState {
    fn new() -> Self {
        let schema = Arc::new(
            TabletSchema::new(
                42,
                vec![
                    TabletColumn::key(0, "id", LogicalType::Integer),
                    TabletColumn::key(1, "code", LogicalType::Varchar),
                    TabletColumn::new(2, "payload", LogicalType::Integer),
                ],
                KeysType::PrimaryKeys,
            )
            .unwrap(),
        );
        let serializer = PrimaryKeySerializer::from_schema_ref(&schema).unwrap();
        let ids: Vec<i32> = (0..ENCODE_BATCH_ROWS as i32).collect();
        let codes: Vec<String> = (0..ENCODE_BATCH_ROWS)
            .map(|idx| format!("pk-code-{idx:08}"))
            .collect();
        let payloads: Vec<i32> = (0..ENCODE_BATCH_ROWS as i32).map(|v| v + 100).collect();
        let chunk = Chunk::from_vectors(vec![
            Vector::from_i32(&ids),
            Vector::from_strings(&codes.iter().map(String::as_str).collect::<Vec<_>>()),
            Vector::from_i32(&payloads),
        ]);
        Self { serializer, chunk }
    }
}

fn encode_state() -> &'static EncodeBenchState {
    static STATE: OnceLock<EncodeBenchState> = OnceLock::new();
    STATE.get_or_init(EncodeBenchState::new)
}

struct LookupScaleState {
    index: PrimaryIndex,
    lookup_keys: Vec<Vec<u8>>,
}

impl LookupScaleState {
    fn new(key_count: usize) -> Self {
        let index = PrimaryIndex::with_options(16, usize::MAX / 2);
        for i in 0..key_count {
            let row = i as u32;
            index.upsert(make_fixed_key(row), RowID::new(1, row));
        }
        let lookup_keys = (0..LOOKUP_BATCH)
            .map(|i| make_fixed_key((i % key_count) as u32))
            .collect();
        Self { index, lookup_keys }
    }
}

#[divan::bench]
fn fixed_key_multi_get() {
    let state = state();
    let keys: Vec<&[u8]> = state.fixed_lookup.iter().map(Vec::as_slice).collect();
    let out = state.fixed.multi_get(keys);
    divan::black_box(out.len());
}

#[divan::bench]
fn long_key_multi_get() {
    let state = state();
    let keys: Vec<&[u8]> = state.long_lookup.iter().map(Vec::as_slice).collect();
    let out = state.long.multi_get(keys);
    divan::black_box(out.len());
}

#[divan::bench]
fn fixed_key_point_lookups() {
    let state = state();
    for key in &state.fixed_lookup {
        divan::black_box(state.fixed.get(key));
    }
}

#[divan::bench]
fn long_key_point_lookups() {
    let state = state();
    for key in &state.long_lookup {
        divan::black_box(state.long.get(key));
    }
}

#[divan::bench]
fn mixed_read_write_fixed_keys() {
    let index = Arc::new(PrimaryIndex::with_options(16, usize::MAX / 2));
    for i in 0..PRELOAD_KEYS {
        index.upsert(make_fixed_key(i), RowID::new(1, i));
    }

    let barrier = Arc::new(Barrier::new(MIXED_THREADS));
    let mut handles = Vec::with_capacity(MIXED_THREADS);
    for tid in 0..MIXED_THREADS {
        let index = index.clone();
        let barrier = barrier.clone();
        handles.push(thread::spawn(move || {
            barrier.wait();
            for i in 0..MIXED_OPS_PER_THREAD {
                let key = make_fixed_key((i as u32) % PRELOAD_KEYS);
                if i % 4 == 0 {
                    index.upsert(key, RowID::new((tid + 2) as u32, i as u32));
                } else {
                    divan::black_box(index.get(&key));
                }
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }
}

#[divan::bench(sample_count = 10)]
fn fixed_key_upsert_single_thread(bencher: Bencher) {
    let keys: Vec<_> = (0..H4_UPSERT_KEYS as u32).map(make_fixed_key).collect();
    bencher.counter(H4_UPSERT_KEYS).bench_local(|| {
        let index = PrimaryIndex::with_options(16, usize::MAX / 2);
        for (row_offset, key) in keys.iter().enumerate() {
            index.upsert(key.clone(), RowID::new(1, row_offset as u32));
        }
        divan::black_box(index.len());
    });
}

#[divan::bench(sample_count = 10)]
fn fixed_key_upsert_multi_thread(bencher: Bencher) {
    let thread_batches: Vec<Vec<Vec<u8>>> = (0..MIXED_THREADS)
        .map(|tid| {
            (0..H4_MULTI_THREAD_UPSERT_KEYS_PER_THREAD)
                .map(|offset| {
                    let seed = (tid * H4_MULTI_THREAD_UPSERT_KEYS_PER_THREAD + offset) as u32;
                    make_fixed_key(seed)
                })
                .collect()
        })
        .collect();

    bencher
        .counter(MIXED_THREADS * H4_MULTI_THREAD_UPSERT_KEYS_PER_THREAD)
        .bench_local(|| {
            let index = Arc::new(PrimaryIndex::with_options(16, usize::MAX / 2));
            let mut handles = Vec::with_capacity(MIXED_THREADS);
            for (tid, batch) in thread_batches.iter().cloned().enumerate() {
                let index = index.clone();
                handles.push(thread::spawn(move || {
                    for (row_offset, key) in batch.into_iter().enumerate() {
                        index.upsert(key, RowID::new((tid + 1) as u32, row_offset as u32));
                    }
                }));
            }
            for handle in handles {
                handle.join().unwrap();
            }
            divan::black_box(index.len());
        });
}

#[divan::bench(args = H4_LOOKUP_SCALES, sample_count = 10)]
#[ignore = "manual scale benchmark; run filtered for the target key count"]
fn fixed_key_lookup_scale(bencher: Bencher, key_count: usize) {
    let state = LookupScaleState::new(key_count);
    let lookup_keys: Vec<_> = state.lookup_keys.iter().map(Vec::as_slice).collect();
    bencher.counter(lookup_keys.len()).bench(|| {
        divan::black_box(state.index.multi_get(lookup_keys.iter().copied()));
    });
}

#[divan::bench]
fn pk_encode_row_loop() {
    let state = encode_state();
    let encoded: Vec<_> = (0..state.chunk.size())
        .map(|row_idx| state.serializer.encode_row(&state.chunk, row_idx).unwrap())
        .collect();
    divan::black_box(encoded.len());
}

#[divan::bench]
fn pk_encode_chunk_batch() {
    let state = encode_state();
    let encoded = state.serializer.encode_chunk(&state.chunk).unwrap();
    divan::black_box(encoded.len());
}
