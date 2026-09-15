// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Opt-in, bounded E1-COLD scalar ledger. Process scope is deliberate: only an
//! isolated server is attributable. Concurrent measurement windows invalidate it.
//! Timers sum exclusive synchronous worker elapsed time, NOT critical-path wall.

use std::cell::Cell;
use std::marker::PhantomData;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::OnceLock;
use std::sync::Mutex;
use std::time::Instant;

pub const MAX_OPERATORS: usize = 1024;
const KINDS: usize = 9;
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
thread_local! { static CURRENT_KIND: Cell<Option<usize>> = const { Cell::new(None) }; }
// Fixed scalar occupancy histogram, not an event trace. Bin0 is uninstrumented
// wall; other bins identify simultaneous activity, counted once regardless of DOP.
struct Occupancy {
    active: [u32; KINDS],
    nanos: [u64; 1 << KINDS],
    last: Instant,
}
impl Occupancy {
    fn new() -> Self { Self { active:[0; KINDS], nanos:[0; 1 << KINDS], last:Instant::now() } }
    fn settle(&mut self) {
        self.settle_at(Instant::now());
    }
    fn settle_at(&mut self, now: Instant) {
        let mask = self.active.iter().enumerate().fold(0, |mask,(i,n)| mask | (usize::from(*n > 0) << i));
        self.nanos[mask] = self.nanos[mask].saturating_add(now.duration_since(self.last).as_nanos() as u64);
        self.last = now;
    }
    fn transition(&mut self, from: Option<usize>, to: Option<usize>) {
        self.transition_at(from, to, Instant::now());
    }
    fn transition_at(&mut self, from: Option<usize>, to: Option<usize>, now: Instant) {
        self.settle_at(now);
        if let Some(i) = from { self.active[i] = self.active[i].saturating_sub(1); }
        if let Some(i) = to { self.active[i] += 1; }
    }
}
fn occupancy() -> &'static Mutex<Occupancy> {
    static CLOCK: OnceLock<Mutex<Occupancy>> = OnceLock::new();
    CLOCK.get_or_init(|| Mutex::new(Occupancy::new()))
}

pub fn enabled() -> bool {
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
];

pub struct WorkScope {
    started: Instant,
    accounted: u64,
    kind: usize,
    bytes: u64,
    operator: Option<usize>,
    epoch: u64,
    parent: Option<usize>,
    occupancy_entered: bool,
    // A synchronous scope must not migrate between worker-local nesting clocks.
    _not_send: PhantomData<Rc<()>>,
}
impl WorkScope {
    pub fn new(kind: Kind, bytes: usize) -> Option<Self> {
        if !enabled() || !ACTIVE.load(Relaxed) { return None; }
        let parent = CURRENT_KIND.with(|current| current.replace(Some(kind as usize)));
        occupancy().lock().unwrap().transition(parent, Some(kind as usize));
        Some(Self { started: Instant::now(), accounted: ACCOUNTED_NS.with(Cell::get),
            kind: kind as usize, bytes: bytes as u64, operator: None, epoch: EPOCH.load(Relaxed),
            parent, occupancy_entered:true, _not_send: PhantomData })
    }
    pub fn operator(kind: Kind, id: usize) -> Option<Self> {
        let mut scope = Self::new(kind, 0)?;
        scope.operator = Some(id);
        Some(scope)
    }
}
impl Drop for WorkScope {
    fn drop(&mut self) {
        if self.occupancy_entered { CURRENT_KIND.with(|current| current.set(self.parent)); }
        if !ACTIVE.load(Relaxed) || self.epoch != EPOCH.load(Relaxed) { return; }
        if self.occupancy_entered { occupancy().lock().unwrap().transition(Some(self.kind), self.parent); }
        let total = self.started.elapsed().as_nanos().min(u64::MAX as u128) as u64;
        let own = ACCOUNTED_NS.with(|clock| {
            let nested = clock.get().saturating_sub(self.accounted);
            let own = total.saturating_sub(nested);
            clock.set(clock.get().saturating_add(own));
            own
        });
        COUNTS[self.kind].fetch_add(1, Relaxed);
        BYTES[self.kind].fetch_add(self.bytes, Relaxed);
        NANOS[self.kind].fetch_add(own, Relaxed);
        if let Some(id) = self.operator {
            if id < MAX_OPERATORS {
                OP_NANOS[id].fetch_add(own, Relaxed);
                OP_COUNTS[id].fetch_add(1, Relaxed);
            } else { OVERFLOW.fetch_add(1, Relaxed); }
        }
    }
}

#[derive(Default, Clone, Copy)]
struct Usage { minor: u64, major: u64, max_rss: u64, valid: bool }
fn usage() -> Usage {
    #[cfg(unix)] {
        let mut value = std::mem::MaybeUninit::<libc::rusage>::uninit();
        // SAFETY: getrusage initializes the structure on success; no pointer escapes.
        if unsafe { libc::getrusage(libc::RUSAGE_SELF, value.as_mut_ptr()) } != 0 { return Usage::default(); }
        let value = unsafe { value.assume_init() };
        let scale = if cfg!(target_os = "macos") { 1 } else { 1024 };
        Usage { minor: value.ru_minflt.max(0) as u64, major: value.ru_majflt.max(0) as u64,
            max_rss: (value.ru_maxrss.max(0) as u64).saturating_mul(scale), valid: true }
    }
    #[cfg(not(unix))] { Usage::default() }
}

/// One actual execution occurrence, excluding parse/compiler. maxRSS is the
/// process lifetime high-water at these endpoints, not a resettable statement peak.
pub struct Window { before: Usage, overlaps: u64, started: Instant, owner: bool }
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
            ("valid_isolated_window".into(), u64::from(self.valid)),
            ("execution_elapsed_us".into(), self.elapsed_us),
            ("minor_faults".into(), self.minor), ("major_faults".into(), self.major),
            ("process_max_rss_before_bytes".into(), self.rss_before),
            ("process_max_rss_after_bytes".into(), self.rss_after),
            ("operator_overflow_count".into(), self.overflow),
        ];
        for (i, name) in NAMES.iter().enumerate() {
            rows.push((format!("{name}_count"), self.metrics[i][0]));
            rows.push((format!("{name}_input_bytes"), self.metrics[i][1]));
            rows.push((format!("{name}_exclusive_worker_ns"), self.metrics[i][2]));
        }
        for &(id, count, nanos) in &self.operators {
            rows.push((format!("operator_{id}_init_count"), count));
            rows.push((format!("operator_{id}_init_exclusive_worker_ns"), nanos));
        }
        for (mask, nanos) in self.occupancy_nanos.iter().enumerate() {
            rows.push((format!("activity_mask_{mask}_wall_ns"), *nanos));
        }
        rows
    }
}
impl Window {
    pub fn begin() -> Option<Self> {
        if !enabled() { return None; }
        Some(Self::begin_enabled())
    }
    fn begin_enabled() -> Self {
        let owner = IN_FLIGHT.fetch_add(1, Relaxed) == 0;
        if !owner { OVERLAPS.fetch_add(1, Relaxed); }
        if owner {
            EPOCH.fetch_add(1, Relaxed);
            ACTIVE.store(true, Relaxed);
            for values in [&COUNTS[..], &BYTES[..], &NANOS[..], &OP_NANOS[..], &OP_COUNTS[..]] {
                for value in values { value.store(0, Relaxed); }
            }
            OVERFLOW.store(0, Relaxed);
        }
        let before = usage();
        let started = Instant::now();
        if owner { *occupancy().lock().unwrap() = Occupancy { active:[0; KINDS], nanos:[0; 1 << KINDS], last:started }; }
        Self { before, overlaps: OVERLAPS.load(Relaxed), started, owner }
    }
    pub fn finish(self) -> Snapshot {
        let after = usage();
        let occupancy_nanos = { let mut clock = occupancy().lock().unwrap(); clock.settle(); clock.nanos };
        let valid = self.owner && self.overlaps == OVERLAPS.load(Relaxed) && self.before.valid && after.valid && OVERFLOW.load(Relaxed) == 0;
        let mut operators = Vec::new();
        for id in 0..MAX_OPERATORS {
            let count = OP_COUNTS[id].load(Relaxed);
            if count > 0 {
                operators.push((id, count, OP_NANOS[id].load(Relaxed)));
            }
        }
        Snapshot { valid, elapsed_us:self.started.elapsed().as_micros() as u64,
            minor:after.minor.saturating_sub(self.before.minor), major:after.major.saturating_sub(self.before.major),
            rss_before:self.before.max_rss, rss_after:after.max_rss, overflow:OVERFLOW.load(Relaxed),
            metrics:std::array::from_fn(|i| [COUNTS[i].load(Relaxed), BYTES[i].load(Relaxed), NANOS[i].load(Relaxed)]), operators, occupancy_nanos }
    }
}
impl Drop for Window { fn drop(&mut self) {
    if self.owner { ACTIVE.store(false, Relaxed); }
    IN_FLIGHT.fetch_sub(1, Relaxed);
} }

#[cfg(test)]
mod tests {
    use super::*;
    static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());
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
    fn worker_nesting_excludes_children_without_changing_wall_semantics() {
        let _serial = SERIAL.lock().unwrap();
        let window = Window::begin_enabled();
        ACCOUNTED_NS.with(|v| v.set(0));
        let outer = WorkScope { started: Instant::now(), accounted: 0, kind: 4, bytes:0,
            operator: None, epoch:EPOCH.load(Relaxed), parent:None, occupancy_entered:false, _not_send: PhantomData };
        let inner = WorkScope { started: Instant::now(), accounted: 0, kind: 1, bytes:17,
            operator: None, epoch:EPOCH.load(Relaxed), parent:None, occupancy_entered:false, _not_send: PhantomData };
        drop(inner);
        drop(outer);
        assert_eq!(BYTES[1].load(Relaxed), 17);
        assert!(ACCOUNTED_NS.with(Cell::get) >= NANOS[1].load(Relaxed));
        assert!(window.finish().valid);
    }
    #[test]
    fn overlapping_or_canceled_windows_cannot_certify_or_contaminate_next_run() {
        let _serial = SERIAL.lock().unwrap();
        let first = Window::begin_enabled();
        let second = Window::begin_enabled();
        assert!(!first.finish().valid);
        let third = Window::begin_enabled();
        assert!(!third.finish().valid);
        assert!(!second.finish().valid);
        let aborted = Window::begin_enabled();
        let old = WorkScope { started: Instant::now(), accounted: ACCOUNTED_NS.with(Cell::get),
            kind:0, bytes:99, operator:None, epoch:EPOCH.load(Relaxed), parent:None, occupancy_entered:false, _not_send:PhantomData };
        drop(aborted);
        let fresh = Window::begin_enabled();
        drop(old);
        let record = fresh.finish();
        assert!(record.valid);
        assert_eq!(record.metrics[0][0], 0);
    }
}
