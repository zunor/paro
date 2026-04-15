use std::sync::{Arc, OnceLock};

use paro_storage::buffer::{BufferPool, PageCache, PageContentKind, PageKey};

const PAGE_SIZE: usize = 8 * 1024;
const WARM_PAGE_COUNT: usize = 4096;
const WARM_SCAN_ITERS: usize = 8192;
const PRESSURE_PAGE_COUNT: usize = 32768;
const PRESSURE_SCAN_ITERS: usize = 8192;

fn main() {
    divan::main();
}

struct BenchState {
    cache: Arc<PageCache>,
    keys: Vec<PageKey>,
    random_indices: Vec<usize>,
    payload: Vec<u8>,
}

impl BenchState {
    fn new(memory_limit: usize, page_count: usize, random_iters: usize, warmup: bool) -> Self {
        let pool = BufferPool::new_arc(memory_limit);
        let cache = Arc::new(PageCache::new(pool));
        let payload = vec![7u8; PAGE_SIZE];

        let mut keys = Vec::with_capacity(page_count);
        for idx in 0..page_count {
            keys.push(PageKey::new(
                1,
                100,
                1,
                0,
                (idx * PAGE_SIZE) as u64,
                PAGE_SIZE as u32,
            ));
        }

        if warmup {
            for key in &keys {
                let handle = cache
                    .get_or_load(*key, PageContentKind::Compressed, || Ok(payload.clone()))
                    .unwrap();
                divan::black_box(handle.size());
            }
        }

        let random_indices = build_random_indices(random_iters, page_count, 0x5eed_cafe_u64);
        Self {
            cache,
            keys,
            random_indices,
            payload,
        }
    }
}

fn build_random_indices(len: usize, modulo: usize, mut seed: u64) -> Vec<usize> {
    let mut out = Vec::with_capacity(len);
    for _ in 0..len {
        seed = seed
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        out.push((seed as usize) % modulo);
    }
    out
}

fn warm_state() -> &'static BenchState {
    static STATE: OnceLock<BenchState> = OnceLock::new();
    STATE.get_or_init(|| BenchState::new(512 * 1024 * 1024, WARM_PAGE_COUNT, WARM_SCAN_ITERS, true))
}

fn pressure_state() -> &'static BenchState {
    static STATE: OnceLock<BenchState> = OnceLock::new();
    // 8MB cap with 8KB pages: only ~1024 pages fit, while keyspace is much larger.
    STATE.get_or_init(|| {
        BenchState::new(
            8 * 1024 * 1024,
            PRESSURE_PAGE_COUNT,
            PRESSURE_SCAN_ITERS,
            false,
        )
    })
}

#[divan::bench]
fn sequential_scan_warm_cache() {
    let state = warm_state();
    for key in &state.keys {
        let handle = state
            .cache
            .lookup(key, PageContentKind::Compressed)
            .expect("warm cache should hit");
        divan::black_box(handle.data().map(|d| d[0]));
    }
}

#[divan::bench]
fn random_scan_warm_cache() {
    let state = warm_state();
    for idx in &state.random_indices {
        let key = state.keys[*idx];
        let handle = state
            .cache
            .lookup(&key, PageContentKind::Compressed)
            .expect("warm cache should hit");
        divan::black_box(handle.size());
    }
}

#[divan::bench]
fn random_scan_memory_pressure() {
    let state = pressure_state();
    for idx in &state.random_indices {
        let key = state.keys[*idx];
        let handle = state
            .cache
            .get_or_load(key, PageContentKind::Compressed, || {
                Ok(state.payload.clone())
            })
            .unwrap();
        divan::black_box(handle.size());
    }
}
