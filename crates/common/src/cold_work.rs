// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Opt-in, bounded E1-COLD scalar ledger. Process scope is deliberate: only an
//! isolated server is attributable. Concurrent measurement windows invalidate it.
//! Timers sum exclusive synchronous worker elapsed time, NOT critical-path wall.

use std::cell::Cell;
use std::marker::PhantomData;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::Mutex;
use std::sync::OnceLock;
use std::time::Instant;

pub const MAX_OPERATORS: usize = 1024;
const KINDS: usize = 12;
static ACTIVE: AtomicBool = AtomicBool::new(false);
static IN_FLIGHT: AtomicU64 = AtomicU64::new(0);
static EPOCH: AtomicU64 = AtomicU64::new(0);
static OVERLAPS: AtomicU64 = AtomicU64::new(0);
static OVERFLOW: AtomicU64 = AtomicU64::new(0);
static COUNTS: [AtomicU64; KINDS] = [const { AtomicU64::new(0) }; KINDS];
static BYTES: [AtomicU64; KINDS] = [const { AtomicU64::new(0) }; KINDS];
static NANOS: [AtomicU64; KINDS] = [const { AtomicU64::new(0) }; KINDS];
static OP_NANOS: [AtomicU64; MAX_OPERATORS] = [const { AtomicU64::new(0) }; MAX_OPERATORS];
static OP_COUNTS: [AtomicU64; MAX_OPERATORS] = [const { AtomicU64::new(0) }; MAX_OPERATORS];
thread_local! { static ACCOUNTED_NS: Cell<u64> = const { Cell::new(0) }; }
thread_local! { static CURRENT_KIND: Cell<Option<(usize, u64)>> = const { Cell::new(None) }; }
#[cfg(any(test, feature = "test-support"))]
thread_local! { static TEST_ENABLED: Cell<bool> = const { Cell::new(false) }; }
#[cfg(any(test, feature = "test-support"))]
static TEST_SERIAL: Mutex<()> = Mutex::new(());
#[cfg(test)]
static BEFORE_DROP_LOCK: Mutex<Option<std::sync::Arc<std::sync::Barrier>>> = Mutex::new(None);
// Fixed scalar occupancy histogram, not an event trace. Bin0 is uninstrumented
// wall; other bins identify simultaneous activity, counted once regardless of DOP.
struct Occupancy {
    active: [u32; KINDS],
    nanos: [u64; 1 << KINDS],
    last: Instant,
}
impl Occupancy {
    fn new() -> Self {
        Self {
            active: [0; KINDS],
            nanos: [0; 1 << KINDS],
            last: Instant::now(),
        }
    }
    fn settle(&mut self) {
        self.settle_at(Instant::now());
    }
    fn settle_at(&mut self, now: Instant) {
        let mask = self
            .active
            .iter()
            .enumerate()
            .fold(0, |mask, (i, n)| mask | (usize::from(*n > 0) << i));
        self.nanos[mask] =
            self.nanos[mask].saturating_add(now.duration_since(self.last).as_nanos() as u64);
        self.last = now;
    }
    fn transition(&mut self, from: Option<usize>, to: Option<usize>) {
        self.transition_at(from, to, Instant::now());
    }
    fn transition_at(&mut self, from: Option<usize>, to: Option<usize>, now: Instant) {
        self.settle_at(now);
        if let Some(i) = from {
            self.active[i] = self.active[i].saturating_sub(1);
        }
        if let Some(i) = to {
            self.active[i] += 1;
        }
    }
}
fn occupancy() -> &'static Mutex<Occupancy> {
    static CLOCK: OnceLock<Mutex<Occupancy>> = OnceLock::new();
    CLOCK.get_or_init(|| Mutex::new(Occupancy::new()))
}

pub fn enabled() -> bool {
    #[cfg(any(test, feature = "test-support"))]
    if TEST_ENABLED.with(Cell::get) {
        return true;
    }
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| std::env::var("PARO_COLD_WORK_EVIDENCE").is_ok_and(|v| v == "1"))
}

#[derive(Clone, Copy)]
pub enum Kind {
    BufferFill,
    Decoder,
    BitShuffleDecompress,
    BitShuffleUnshuffle,
    BitShuffleMaterialize,
    Dictionary,
    ZoneMap,
    GlobalInit,
    LocalInit,
    // Append only: activity masks from earlier ledger schemas retain their bits.
    // Reserve/initialize byte counters record requested destination bytes;
    // core records compressed payload bytes excluding the four-byte prefix.
    Lz4Reserve,
    Lz4Initialize,
    Lz4Core,
}
const NAMES: [&str; KINDS] = [
    "buffer_fill",
    "decoder",
    "bitshuffle_decompress",
    "bitshuffle_unshuffle",
    "bitshuffle_materialize",
    "dictionary",
    "zone_map",
    "global_init",
    "local_init",
    "lz4_reserve",
    "lz4_initialize",
    "lz4_core",
];

pub struct WorkScope {
    started: Instant,
    accounted: u64,
    kind: usize,
    bytes: u64,
    operator: Option<usize>,
    epoch: u64,
    parent: Option<(usize, u64)>,
    occupancy_entered: bool,
    // A synchronous scope must not migrate between worker-local nesting clocks.
    _not_send: PhantomData<Rc<()>>,
}
impl WorkScope {
    pub fn new(kind: Kind, bytes: usize) -> Option<Self> {
        if !ACTIVE.load(Relaxed) || !enabled() {
            return None;
        }
        let epoch = EPOCH.load(Relaxed);
        let parent = CURRENT_KIND.with(Cell::get);
        // A canceled synchronous worker may still be inside an old scope.
        // Its nested work cannot borrow the next execution's global epoch.
        if parent.is_some_and(|(_, e)| e != epoch) {
            return None;
        }
        Self::enter(kind, bytes, epoch, parent)
    }
    /// Split only the active bitshuffle decompression worker scope. Ordinary
    /// page LZ4 calls remain uninstrumented. When the window is disabled this
    /// returns before consulting TLS, a clock, a lock or any allocation.
    pub fn bitshuffle_decompress_child(kind: Kind, bytes: usize) -> Option<Self> {
        if !ACTIVE.load(Relaxed) || !enabled() {
            return None;
        }
        if !matches!(kind, Kind::Lz4Reserve | Kind::Lz4Initialize | Kind::Lz4Core) {
            return None;
        }
        let epoch = EPOCH.load(Relaxed);
        let parent = CURRENT_KIND.with(Cell::get);
        if parent != Some((Kind::BitShuffleDecompress as usize, epoch)) {
            return None;
        }
        Self::enter(kind, bytes, epoch, parent)
    }
    fn enter(kind: Kind, bytes: usize, epoch: u64, parent: Option<(usize, u64)>) -> Option<Self> {
        let mut clock = occupancy().lock().unwrap();
        // The optimistic checks can race with cancellation and the next
        // window. Reset, transitions and counters share this synchronization.
        if !ACTIVE.load(Relaxed) || epoch != EPOCH.load(Relaxed) {
            return None;
        }
        CURRENT_KIND.with(|current| current.set(Some((kind as usize, epoch))));
        clock.transition(parent.map(|(kind, _)| kind), Some(kind as usize));
        Some(Self {
            started: Instant::now(),
            accounted: ACCOUNTED_NS.with(Cell::get),
            kind: kind as usize,
            bytes: bytes as u64,
            operator: None,
            epoch,
            parent,
            occupancy_entered: true,
            _not_send: PhantomData,
        })
    }
    pub fn operator(kind: Kind, id: usize) -> Option<Self> {
        let mut scope = Self::new(kind, 0)?;
        scope.operator = Some(id);
        Some(scope)
    }
}
impl Drop for WorkScope {
    fn drop(&mut self) {
        if self.occupancy_entered {
            CURRENT_KIND.with(|current| {
                // An abandoned scope may outlive its window. Its drop must not
                // overwrite the nesting state of a later measurement.
                if current.get() == Some((self.kind, self.epoch)) {
                    current.set(self.parent);
                }
            });
        }
        if !ACTIVE.load(Relaxed) || self.epoch != EPOCH.load(Relaxed) {
            return;
        }
        // The new narrow phases stop before the occupancy lock, so exit-lock
        // contention is charged to their parent rather than to LZ4 work. Keep
        // the original stop order for existing coarse kinds.
        let child_total = (Kind::Lz4Reserve as usize..=Kind::Lz4Core as usize)
            .contains(&self.kind)
            .then(|| self.started.elapsed().as_nanos().min(u64::MAX as u128) as u64);
        #[cfg(test)]
        {
            let barrier = BEFORE_DROP_LOCK.lock().unwrap().clone();
            if let Some(barrier) = barrier {
                barrier.wait();
                barrier.wait();
            }
        }
        let mut clock = occupancy().lock().unwrap();
        if !ACTIVE.load(Relaxed) || self.epoch != EPOCH.load(Relaxed) {
            return;
        }
        if self.occupancy_entered {
            clock.transition(Some(self.kind), self.parent.map(|(kind, _)| kind));
        }
        let total = child_total
            .unwrap_or_else(|| self.started.elapsed().as_nanos().min(u64::MAX as u128) as u64);
        let own = exclusive_worker_ns(total, self.accounted);
        COUNTS[self.kind].fetch_add(1, Relaxed);
        BYTES[self.kind].fetch_add(self.bytes, Relaxed);
        NANOS[self.kind].fetch_add(own, Relaxed);
        if let Some(id) = self.operator {
            if id < MAX_OPERATORS {
                OP_NANOS[id].fetch_add(own, Relaxed);
                OP_COUNTS[id].fetch_add(1, Relaxed);
            } else {
                OVERFLOW.fetch_add(1, Relaxed);
            }
        }
    }
}

fn exclusive_worker_ns(total: u64, accounted: u64) -> u64 {
    ACCOUNTED_NS.with(|clock| {
        let nested = clock.get().saturating_sub(accounted);
        let own = total.saturating_sub(nested);
        clock.set(clock.get().saturating_add(own));
        own
    })
}

#[derive(Default, Clone, Copy)]
struct Usage {
    minor: u64,
    major: u64,
    max_rss: u64,
    valid: bool,
}
fn usage() -> Usage {
    #[cfg(unix)]
    {
        let mut value = std::mem::MaybeUninit::<libc::rusage>::uninit();
        // SAFETY: getrusage initializes the structure on success; no pointer escapes.
        if unsafe { libc::getrusage(libc::RUSAGE_SELF, value.as_mut_ptr()) } != 0 {
            return Usage::default();
        }
        let value = unsafe { value.assume_init() };
        let scale = if cfg!(target_os = "macos") { 1 } else { 1024 };
        Usage {
            minor: value.ru_minflt.max(0) as u64,
            major: value.ru_majflt.max(0) as u64,
            max_rss: (value.ru_maxrss.max(0) as u64).saturating_mul(scale),
            valid: true,
        }
    }
    #[cfg(not(unix))]
    {
        Usage::default()
    }
}

/// One actual execution occurrence, excluding parse/compiler. maxRSS is the
/// process lifetime high-water at these endpoints, not a resettable statement peak.
pub struct Window {
    before: Usage,
    overlaps: u64,
    started: Instant,
    owner: bool,
}
#[derive(Debug, Clone)]
pub struct Snapshot {
    pub valid: bool,
    pub elapsed_us: u64,
    pub minor: u64,
    pub major: u64,
    pub rss_before: u64,
    pub rss_after: u64,
    pub overflow: u64,
    pub metrics: [[u64; 3]; KINDS],
    pub operators: Vec<(usize, u64, u64)>,
    pub occupancy_nanos: [u64; 1 << KINDS],
}
impl Snapshot {
    /// Naming/serialization happens only when the post-timer side channel is read.
    pub fn rows(&self) -> Vec<(String, u64)> {
        let mut rows = vec![
            ("ledger_schema_version".into(), 2),
            ("activity_kind_count".into(), KINDS as u64),
            ("activity_mask_count".into(), (1 << KINDS) as u64),
            ("activity_mask_missing_is_zero".into(), 1),
            ("valid_isolated_window".into(), u64::from(self.valid)),
            ("execution_elapsed_us".into(), self.elapsed_us),
            ("minor_faults".into(), self.minor),
            ("major_faults".into(), self.major),
            ("process_max_rss_before_bytes".into(), self.rss_before),
            ("process_max_rss_after_bytes".into(), self.rss_after),
            ("operator_overflow_count".into(), self.overflow),
        ];
        for (i, name) in NAMES.iter().enumerate() {
            rows.push((format!("activity_{name}_bit_index"), i as u64));
            rows.push((format!("{name}_count"), self.metrics[i][0]));
            rows.push((format!("{name}_input_bytes"), self.metrics[i][1]));
            rows.push((format!("{name}_exclusive_worker_ns"), self.metrics[i][2]));
        }
        // Existing counters remain exclusive. This subtree sum reconstructs
        // the older unsplit decompression worker time, not critical-path wall.
        let decompression = [
            Kind::BitShuffleDecompress,
            Kind::Lz4Reserve,
            Kind::Lz4Initialize,
            Kind::Lz4Core,
        ]
        .into_iter()
        .fold(0u64, |sum, kind| {
            sum.saturating_add(self.metrics[kind as usize][2])
        });
        rows.push((
            "bitshuffle_decompress_subtree_worker_ns".into(),
            decompression,
        ));
        for &(id, count, nanos) in &self.operators {
            rows.push((format!("operator_{id}_init_count"), count));
            rows.push((format!("operator_{id}_init_exclusive_worker_ns"), nanos));
        }
        for (mask, nanos) in self.occupancy_nanos.iter().enumerate() {
            if *nanos > 0 {
                rows.push((format!("activity_mask_{mask}_wall_ns"), *nanos));
            }
        }
        rows
    }
}
impl Window {
    pub fn begin() -> Option<Self> {
        if !enabled() {
            return None;
        }
        Some(Self::begin_enabled())
    }
    fn begin_enabled() -> Self {
        let mut clock = occupancy().lock().unwrap();
        let owner = IN_FLIGHT.fetch_add(1, Relaxed) == 0;
        if !owner {
            OVERLAPS.fetch_add(1, Relaxed);
        }
        if owner {
            EPOCH.fetch_add(1, Relaxed);
            ACTIVE.store(true, Relaxed);
            for values in [
                &COUNTS[..],
                &BYTES[..],
                &NANOS[..],
                &OP_NANOS[..],
                &OP_COUNTS[..],
            ] {
                for value in values {
                    value.store(0, Relaxed);
                }
            }
            OVERFLOW.store(0, Relaxed);
        }
        let before = usage();
        let started = Instant::now();
        if owner {
            *clock = Occupancy {
                active: [0; KINDS],
                nanos: [0; 1 << KINDS],
                last: started,
            };
        }
        Self {
            before,
            overlaps: OVERLAPS.load(Relaxed),
            started,
            owner,
        }
    }
    pub fn finish(self) -> Snapshot {
        let after = usage();
        let mut clock = occupancy().lock().unwrap();
        clock.settle();
        let occupancy_nanos = clock.nanos;
        let valid = self.owner
            && self.overlaps == OVERLAPS.load(Relaxed)
            && self.before.valid
            && after.valid
            && OVERFLOW.load(Relaxed) == 0
            && clock.active.iter().all(|count| *count == 0);
        let mut operators = Vec::new();
        for id in 0..MAX_OPERATORS {
            let count = OP_COUNTS[id].load(Relaxed);
            if count > 0 {
                operators.push((id, count, OP_NANOS[id].load(Relaxed)));
            }
        }
        let snapshot = Snapshot {
            valid,
            elapsed_us: self.started.elapsed().as_micros() as u64,
            minor: after.minor.saturating_sub(self.before.minor),
            major: after.major.saturating_sub(self.before.major),
            rss_before: self.before.max_rss,
            rss_after: after.max_rss,
            overflow: OVERFLOW.load(Relaxed),
            metrics: std::array::from_fn(|i| {
                [
                    COUNTS[i].load(Relaxed),
                    BYTES[i].load(Relaxed),
                    NANOS[i].load(Relaxed),
                ]
            }),
            operators,
            occupancy_nanos,
        };
        drop(clock);
        snapshot
    }
}
impl Drop for Window {
    fn drop(&mut self) {
        let _clock = occupancy().lock().unwrap();
        if self.owner {
            ACTIVE.store(false, Relaxed);
        }
        IN_FLIGHT.fetch_sub(1, Relaxed);
    }
}

/// Scoped fixture for storage tests, without mutating the process environment
/// or the production enablement cache. All ledger fixtures share this lock.
#[cfg(any(test, feature = "test-support"))]
#[doc(hidden)]
pub fn with_test_window<T>(run: impl FnOnce() -> T) -> (T, Snapshot) {
    let _serial = TEST_SERIAL.lock().unwrap();
    struct EnableGuard(bool);
    impl Drop for EnableGuard {
        fn drop(&mut self) {
            TEST_ENABLED.with(|value| value.set(self.0));
        }
    }
    let _enabled = EnableGuard(TEST_ENABLED.with(|value| value.replace(true)));
    let window = Window::begin_enabled();
    let result = run();
    (result, window.finish())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn stale_entry_does_not_change_current_window_or_worker_parent() {
        let (old_epoch, _) = with_test_window(|| EPOCH.load(Relaxed));
        let (_, record) = with_test_window(|| {
            let parent = CURRENT_KIND.with(Cell::get);
            assert!(WorkScope::enter(Kind::Lz4Core, 99, old_epoch, None).is_none());
            assert_eq!(CURRENT_KIND.with(Cell::get), parent);
        });
        assert!(record.valid);
        assert_eq!(record.metrics[Kind::Lz4Core as usize], [0; 3]);
    }

    #[test]
    fn stale_drop_rechecks_epoch_after_waiting_to_change_occupancy() {
        let _serial = TEST_SERIAL.lock().unwrap();
        let old = Window::begin_enabled();
        let epoch = EPOCH.load(Relaxed);
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
        *BEFORE_DROP_LOCK.lock().unwrap() = Some(barrier.clone());
        let worker = std::thread::spawn(move || {
            let parent = WorkScope::enter(Kind::BitShuffleDecompress, 100, epoch, None).unwrap();
            let mut child = WorkScope::enter(
                Kind::Lz4Core,
                99,
                epoch,
                Some((Kind::BitShuffleDecompress as usize, epoch)),
            )
            .unwrap();
            child.operator = Some(0);
            drop(child);
            // The child restored this thread's old parent. Neither nested
            // work nor the eventual parent drop may join the fresh window.
            TEST_ENABLED.with(|value| value.set(true));
            assert!(WorkScope::new(Kind::Decoder, 77).is_none());
            TEST_ENABLED.with(|value| value.set(false));
            drop(parent);
        });
        barrier.wait();
        drop(old);
        let fresh = Window::begin_enabled();
        barrier.wait();
        worker.join().unwrap();
        *BEFORE_DROP_LOCK.lock().unwrap() = None;
        let record = fresh.finish();
        assert!(record.valid);
        assert_eq!(record.metrics[Kind::Lz4Core as usize], [0; 3]);
        assert!(record.operators.is_empty());
        assert!(record.occupancy_nanos.iter().skip(1).all(|ns| *ns == 0));
    }

    #[test]
    fn live_abandoned_scope_invalidates_a_finished_window() {
        let (scope, record) = with_test_window(|| WorkScope::new(Kind::Decoder, 1).unwrap());
        assert!(!record.valid);
        drop(scope);
    }
    #[test]
    fn occupancy_oracle_counts_parallel_overlap_once_and_preserves_uncovered_time() {
        let mut clock = Occupancy::new();
        let zero = clock.last;
        let t = |n| zero + std::time::Duration::from_nanos(n);
        clock.transition_at(None, Some(0), t(10));
        clock.transition_at(None, Some(1), t(20));
        clock.transition_at(Some(0), None, t(40));
        clock.transition_at(Some(1), None, t(50));
        clock.settle_at(t(70));
        assert_eq!(clock.nanos[0], 30);
        assert_eq!(clock.nanos[1], 10);
        assert_eq!(clock.nanos[2], 10);
        assert_eq!(clock.nanos[3], 20);
        assert_eq!(clock.nanos.iter().sum::<u64>(), 70);
        assert_eq!(clock.nanos.iter().skip(1).sum::<u64>(), 40);
    }
    #[test]
    fn decompression_children_parallel_union_oracle_preserves_parent_activity() {
        let mut clock = Occupancy::new();
        let zero = clock.last;
        let t = |n| zero + std::time::Duration::from_nanos(n);
        let parent = Kind::BitShuffleDecompress as usize;
        let core = Kind::Lz4Core as usize;
        clock.transition_at(None, Some(parent), t(10));
        clock.transition_at(None, Some(parent), t(20));
        clock.transition_at(Some(parent), Some(core), t(30));
        clock.transition_at(Some(core), Some(parent), t(50));
        clock.transition_at(Some(parent), None, t(60));
        clock.transition_at(Some(parent), None, t(70));
        clock.settle_at(t(80));
        assert_eq!(clock.nanos[0], 20);
        assert_eq!(clock.nanos[1 << parent], 40);
        assert_eq!(clock.nanos[(1 << parent) | (1 << core)], 20);
        assert_eq!(clock.nanos.iter().sum::<u64>(), 80);
        let family = (1 << parent) | (1 << core);
        let union: u64 = clock
            .nanos
            .iter()
            .enumerate()
            .filter(|(mask, _)| *mask & family != 0)
            .map(|(_, nanos)| *nanos)
            .sum();
        assert_eq!(union, 60);
    }
    #[test]
    fn nested_exclusive_worker_oracle_recomposes_unsplit_time() {
        // Grandchild 20, child 50 (including grandchild), parent 100.
        ACCOUNTED_NS.with(|clock| clock.set(0));
        let grandchild = exclusive_worker_ns(20, 0);
        let child = exclusive_worker_ns(50, 0);
        let parent = exclusive_worker_ns(100, 0);
        assert_eq!((grandchild, child, parent), (20, 30, 50));
        assert_eq!(grandchild + child + parent, 100);
        assert_eq!(ACCOUNTED_NS.with(Cell::get), 100);
    }
    #[test]
    fn disabled_children_do_not_touch_nesting_or_the_occupancy_lock() {
        let _serial = TEST_SERIAL.lock().unwrap();
        assert!(!ACTIVE.load(Relaxed));
        let _occupancy = occupancy().lock().unwrap();
        let parent = CURRENT_KIND.with(Cell::get);
        for kind in [Kind::Lz4Reserve, Kind::Lz4Initialize, Kind::Lz4Core] {
            assert!(WorkScope::bitshuffle_decompress_child(kind, 99).is_none());
            assert_eq!(CURRENT_KIND.with(Cell::get), parent);
        }
    }
    #[test]
    fn children_require_a_matching_live_parent_and_ignore_old_window_scopes() {
        let (old, _) = with_test_window(|| {
            assert!(WorkScope::bitshuffle_decompress_child(Kind::Lz4Core, 1).is_none());
            WorkScope::new(Kind::BitShuffleDecompress, 10).unwrap()
        });
        let (_, record) = with_test_window(|| {
            // The TLS parent survived the previous window, but its epoch did not.
            assert!(WorkScope::bitshuffle_decompress_child(Kind::Lz4Core, 1).is_none());
            assert!(WorkScope::new(Kind::Decoder, 99).is_none());
            // Force an independent fresh scope to check that a stale drop
            // cannot overwrite its TLS nesting identity.
            let parent =
                WorkScope::enter(Kind::BitShuffleDecompress, 20, EPOCH.load(Relaxed), None)
                    .unwrap();
            drop(old);
            let child = WorkScope::bitshuffle_decompress_child(Kind::Lz4Core, 7).unwrap();
            assert!(WorkScope::bitshuffle_decompress_child(Kind::Lz4Reserve, 1).is_none());
            drop(child);
            assert!(WorkScope::bitshuffle_decompress_child(Kind::Decoder, 1).is_none());
            drop(parent);
            let unrelated = WorkScope::new(Kind::Decoder, 1).unwrap();
            assert!(WorkScope::bitshuffle_decompress_child(Kind::Lz4Core, 1).is_none());
            drop(unrelated);
        });
        assert!(record.valid);
        assert_eq!(
            record.metrics[Kind::BitShuffleDecompress as usize][..2],
            [1, 20]
        );
        assert_eq!(record.metrics[Kind::Lz4Core as usize][..2], [1, 7]);
        assert_eq!(record.metrics[Kind::Lz4Reserve as usize][0], 0);
        assert_eq!(CURRENT_KIND.with(Cell::get), None);
    }
    #[test]
    fn ledger_schema_preserves_old_bits_and_serializes_sparse_masks() {
        let (_, mut record) = with_test_window(|| {});
        record.occupancy_nanos = [0; 1 << KINDS];
        record.occupancy_nanos[(1 << 2) | (1 << 11)] = 123;
        record.metrics[2][2] = 10;
        record.metrics[9][2] = 20;
        record.metrics[10][2] = 30;
        record.metrics[11][2] = 40;
        let rows = record.rows();
        let value = |name: &str| rows.iter().find(|(key, _)| key == name).map(|(_, n)| *n);
        assert_eq!(value("ledger_schema_version"), Some(2));
        assert_eq!(value("activity_mask_count"), Some(4096));
        assert_eq!(value("activity_mask_missing_is_zero"), Some(1));
        assert_eq!(value("activity_bitshuffle_decompress_bit_index"), Some(2));
        assert_eq!(value("activity_local_init_bit_index"), Some(8));
        assert_eq!(value("activity_lz4_reserve_bit_index"), Some(9));
        assert_eq!(value("activity_lz4_initialize_bit_index"), Some(10));
        assert_eq!(value("activity_lz4_core_bit_index"), Some(11));
        assert_eq!(value("activity_mask_2052_wall_ns"), Some(123));
        assert_eq!(value("activity_mask_0_wall_ns"), None);
        assert_eq!(value("bitshuffle_decompress_exclusive_worker_ns"), Some(10));
        assert_eq!(value("bitshuffle_decompress_subtree_worker_ns"), Some(100));
    }
    #[test]
    fn worker_nesting_excludes_children_without_changing_wall_semantics() {
        let _serial = TEST_SERIAL.lock().unwrap();
        let window = Window::begin_enabled();
        ACCOUNTED_NS.with(|v| v.set(0));
        let outer = WorkScope {
            started: Instant::now(),
            accounted: 0,
            kind: 4,
            bytes: 0,
            operator: None,
            epoch: EPOCH.load(Relaxed),
            parent: None,
            occupancy_entered: false,
            _not_send: PhantomData,
        };
        let inner = WorkScope {
            started: Instant::now(),
            accounted: 0,
            kind: 1,
            bytes: 17,
            operator: None,
            epoch: EPOCH.load(Relaxed),
            parent: None,
            occupancy_entered: false,
            _not_send: PhantomData,
        };
        drop(inner);
        drop(outer);
        assert_eq!(BYTES[1].load(Relaxed), 17);
        assert!(ACCOUNTED_NS.with(Cell::get) >= NANOS[1].load(Relaxed));
        assert!(window.finish().valid);
    }
    #[test]
    fn overlapping_or_canceled_windows_cannot_certify_or_contaminate_next_run() {
        let _serial = TEST_SERIAL.lock().unwrap();
        let first = Window::begin_enabled();
        let second = Window::begin_enabled();
        assert!(!first.finish().valid);
        let third = Window::begin_enabled();
        assert!(!third.finish().valid);
        assert!(!second.finish().valid);
        let aborted = Window::begin_enabled();
        let old = WorkScope {
            started: Instant::now(),
            accounted: ACCOUNTED_NS.with(Cell::get),
            kind: 0,
            bytes: 99,
            operator: None,
            epoch: EPOCH.load(Relaxed),
            parent: None,
            occupancy_entered: false,
            _not_send: PhantomData,
        };
        drop(aborted);
        let fresh = Window::begin_enabled();
        drop(old);
        let record = fresh.finish();
        assert!(record.valid);
        assert_eq!(record.metrics[0][0], 0);
    }
}
