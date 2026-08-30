// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::{HnswBuildContract, HnswFilterBlocks, HnswIndex, VectorStorage};
use paro_common::error::{self as paro_error, Result};
use rayon::ThreadPool;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::time::{Duration, Instant};

/// Runtime scheduling policy for an HNSW build.
///
/// This policy is deliberately absent from [`HnswBuildContract`]: it controls
/// how many workers may execute immutable proposal/publish partitions, never
/// wave membership or durable topology. The frozen-wave builder is required
/// to produce identical bytes for every granted width.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum HnswBuildExecutionPolicy {
    /// User-requested index construction may consume the complete shared pool.
    #[default]
    Foreground,
    /// Background catch-up and generation compaction preserve foreground CPU.
    Maintenance,
}

impl HnswBuildExecutionPolicy {
    fn granted_parallelism(self, pool_width: usize) -> usize {
        match self {
            Self::Foreground => pool_width.max(1),
            // An idle process should retire freshness debt at full speed.
            // Foreground reservations are applied dynamically at immutable
            // proposal-wave barriers below; imposing a permanent half-width
            // cap makes sustained ingest converge at only half the builder's
            // service rate even when no query exists.
            Self::Maintenance => pool_width.max(1),
        }
    }

    fn preparation_parallelism(self, pool_width: usize) -> usize {
        match self {
            Self::Foreground => pool_width.max(1),
            // Vector preparation currently has two long immutable passes and
            // no wave boundary at which its active Rayon job set can shrink.
            // Keep maintenance preparation preemptible by making those passes
            // serial; foreground CREATE INDEX may use the complete shared
            // pool. Graph construction remains dynamically governed per wave.
            Self::Maintenance => 1,
        }
    }

    fn parallelism_under_foreground_pressure(self, granted: usize) -> usize {
        match self {
            Self::Foreground => granted.max(1),
            // Maintenance priority belongs to admission, retry, and write
            // backpressure. It must not grant a second, oversubscribing CPU
            // budget after a job has entered the process-owned build pool.
            // One lane guarantees progress under sustained reads; immutable
            // wave barriers restore the complete grant as soon as the
            // foreground reservation expires.
            Self::Maintenance => 1,
        }
    }
}

struct HnswBuildPool {
    threads: usize,
    pool: ThreadPool,
}

static HNSW_BUILD_THREADS: AtomicUsize = AtomicUsize::new(0);
static HNSW_BUILD_POOL: OnceLock<std::result::Result<HnswBuildPool, String>> = OnceLock::new();
static HNSW_ACTIVE_FOREGROUND_QUERIES: AtomicUsize = AtomicUsize::new(0);
static HNSW_SCHEDULER_EPOCH: OnceLock<Instant> = OnceLock::new();
static HNSW_FOREGROUND_PRESSURE_UNTIL_MICROS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
static HNSW_FOREGROUND_PRESSURE_CHANGED: OnceLock<(Mutex<()>, Condvar)> = OnceLock::new();
/// Hold the foreground reservation across protocol, planning, and artifact
/// fan-out gaps between consecutive queries. Maintenance still receives one
/// lane, so sustained read traffic bounds interference without starving
/// bounded-lag catch-up.
const HNSW_FOREGROUND_PRESSURE_COOLDOWN: Duration = Duration::from_millis(250);
const HNSW_MAINTENANCE_FOREGROUND_BACKOFF: Duration = Duration::from_millis(25);

fn scheduler_micros() -> u64 {
    let micros = HNSW_SCHEDULER_EPOCH
        .get_or_init(Instant::now)
        .elapsed()
        .as_micros();
    micros.min(u128::from(u64::MAX)) as u64
}

fn extend_foreground_pressure() {
    let cooldown = HNSW_FOREGROUND_PRESSURE_COOLDOWN.as_micros() as u64;
    HNSW_FOREGROUND_PRESSURE_UNTIL_MICROS.fetch_max(
        scheduler_micros().saturating_add(cooldown),
        Ordering::AcqRel,
    );
}

pub(crate) fn hnsw_foreground_pressure_active() -> bool {
    foreground_pressure_active_at(
        scheduler_micros(),
        HNSW_ACTIVE_FOREGROUND_QUERIES.load(Ordering::Acquire),
        HNSW_FOREGROUND_PRESSURE_UNTIL_MICROS.load(Ordering::Acquire),
    )
}

/// Per-definition foreground activity used by optional residency work.
///
/// Integrity authentication may rotate past a hot definition without coupling
/// unrelated tenants through the process-wide query census. Required freshness
/// and levelled graph compaction do not wait for an idle window: their HNSW
/// execution policy instead shrinks at deterministic wave barriers.
#[derive(Debug, Default)]
pub(crate) struct HnswQueryActivity {
    active_queries: AtomicUsize,
    last_query_micros_plus_one: AtomicU64,
    changed_state: Mutex<()>,
    changed: Condvar,
}

impl HnswQueryActivity {
    pub(crate) fn enter(self: &Arc<Self>) -> HnswQueryActivityGuard {
        self.last_query_micros_plus_one
            .store(scheduler_micros().saturating_add(1), Ordering::Release);
        self.active_queries.fetch_add(1, Ordering::AcqRel);
        self.changed.notify_all();
        HnswQueryActivityGuard {
            activity: Arc::clone(self),
        }
    }

    /// Optional residency work is admitted per definition. A hot tenant or
    /// table must not prevent unrelated definitions from authenticating their
    /// immutable artifacts.
    pub(crate) fn quiet_for(&self, minimum_idle: Duration) -> bool {
        if self.active_queries.load(Ordering::Acquire) != 0 {
            return false;
        }
        let last_plus_one = self.last_query_micros_plus_one.load(Ordering::Acquire);
        if last_plus_one == 0 {
            return true;
        }
        scheduler_micros().saturating_sub(last_plus_one - 1)
            >= minimum_idle.as_micros().min(u128::from(u64::MAX)) as u64
    }

    /// Wait on this definition only. Instance-wide integrity work can rotate
    /// past a hot definition, while the single-job case parks without polling
    /// or coupling unrelated tenants through the global HNSW query census.
    pub(crate) fn wait_for_quiet(&self, minimum_idle: Duration, max_wait: Duration) -> bool {
        if self.quiet_for(minimum_idle) {
            return true;
        }
        let deadline = Instant::now() + max_wait;
        let mut state = self
            .changed_state
            .lock()
            .expect("HNSW definition activity lock poisoned");
        while !self.quiet_for(minimum_idle) {
            let now = Instant::now();
            if now >= deadline {
                return false;
            }
            let (next, timeout) = self
                .changed
                .wait_timeout(state, deadline.saturating_duration_since(now))
                .expect("HNSW definition activity lock poisoned");
            state = next;
            if timeout.timed_out() && !self.quiet_for(minimum_idle) {
                return false;
            }
        }
        true
    }
}

pub(crate) struct HnswQueryActivityGuard {
    activity: Arc<HnswQueryActivity>,
}

impl Drop for HnswQueryActivityGuard {
    fn drop(&mut self) {
        self.activity
            .last_query_micros_plus_one
            .store(scheduler_micros().saturating_add(1), Ordering::Release);
        self.activity.active_queries.fetch_sub(1, Ordering::AcqRel);
        self.activity.changed.notify_all();
    }
}

/// Number of foreground HNSW queries currently inside provider execution.
///
/// Search and maintenance use the same process-width contract but execute in
/// separate fixed worker pools. Exposing the query census lets every HNSW
/// query divide the search pool by actual demand: one query can use the idle
/// machine, while concurrent queries receive a fair share without baking a
/// benchmark-specific lane count into the provider.
pub(crate) fn hnsw_active_foreground_queries() -> usize {
    HNSW_ACTIVE_FOREGROUND_QUERIES
        .load(Ordering::Acquire)
        .max(1)
}

fn foreground_pressure_active_at(
    now_micros: u64,
    active_queries: usize,
    until_micros: u64,
) -> bool {
    active_queries != 0 || now_micros < until_micros
}

/// Process-level cooperative HNSW workload governor.
///
/// Construction advances through deterministic proposal waves. Each barrier
/// is a natural preemption point: background builds use their complete grant
/// while idle and shrink to one worker as soon as a foreground query appears.
/// Small delta graphs bound the duration of that serial progress share; query
/// latency remains the priority while the immutable wave barriers guarantee
/// graph bytes are independent of execution width.
pub(crate) struct HnswForegroundQueryGuard;

impl HnswForegroundQueryGuard {
    pub(crate) fn enter() -> Self {
        let (state, changed) =
            HNSW_FOREGROUND_PRESSURE_CHANGED.get_or_init(|| (Mutex::new(()), Condvar::new()));
        let _state = state
            .lock()
            .expect("HNSW foreground-pressure lock poisoned");
        extend_foreground_pressure();
        HNSW_ACTIVE_FOREGROUND_QUERIES.fetch_add(1, Ordering::AcqRel);
        changed.notify_all();
        Self
    }
}

impl Drop for HnswForegroundQueryGuard {
    fn drop(&mut self) {
        let (state, changed) =
            HNSW_FOREGROUND_PRESSURE_CHANGED.get_or_init(|| (Mutex::new(()), Condvar::new()));
        let _state = state
            .lock()
            .expect("HNSW foreground-pressure lock poisoned");
        // Publish the trailing reservation before removing the active count;
        // a build wave can therefore never observe an unreserved gap between
        // the two states.
        extend_foreground_pressure();
        HNSW_ACTIVE_FOREGROUND_QUERIES.fetch_sub(1, Ordering::AcqRel);
        changed.notify_all();
    }
}

/// Wait for the HNSW foreground reservation without consuming scheduler CPU.
///
/// Integrity verification is optional background work and shares memory
/// bandwidth with graph traversal. A condition variable lets its one
/// low-priority task park while queries are active; the bounded wait also
/// observes expiry of the trailing cooldown when no guard transition remains
/// to send another notification.
pub(crate) fn hnsw_wait_for_foreground_quiet(max_wait: Duration) -> bool {
    if !hnsw_foreground_pressure_active() {
        return true;
    }
    let (state, changed) =
        HNSW_FOREGROUND_PRESSURE_CHANGED.get_or_init(|| (Mutex::new(()), Condvar::new()));
    let deadline = Instant::now() + max_wait;
    let mut state = state
        .lock()
        .expect("HNSW foreground-pressure lock poisoned");
    while hnsw_foreground_pressure_active() {
        let now = Instant::now();
        if now >= deadline {
            return false;
        }
        let (next, timeout) = changed
            .wait_timeout(state, deadline.saturating_duration_since(now))
            .expect("HNSW foreground-pressure lock poisoned");
        state = next;
        if timeout.timed_out() && hnsw_foreground_pressure_active() {
            return false;
        }
    }
    true
}

/// Bound the memory-bandwidth share of required catch-up under sustained
/// reads. Maintenance still completes one deterministic proposal quantum
/// before reaching this point, so progress is guaranteed; the timed park
/// prevents that one lane from becoming a continuous random-memory stream.
pub(crate) fn hnsw_yield_maintenance_to_foreground(policy: HnswBuildExecutionPolicy) {
    if policy == HnswBuildExecutionPolicy::Maintenance {
        let _ = hnsw_wait_for_foreground_quiet(HNSW_MAINTENANCE_FOREGROUND_BACKOFF);
    }
}

pub(crate) fn hnsw_current_build_parallelism(
    policy: HnswBuildExecutionPolicy,
    granted: usize,
) -> usize {
    if !hnsw_foreground_pressure_active() {
        return granted.max(1);
    }
    policy.parallelism_under_foreground_pressure(granted)
}

fn create_build_pool(threads: usize) -> std::result::Result<HnswBuildPool, String> {
    let threads = threads.max(1);
    let pool = rayon::ThreadPoolBuilder::new()
        .thread_name(|idx| format!("paro-hnsw-build-{idx}"))
        .num_threads(threads)
        .build()
        .map_err(|error| format!("create process HNSW build pool: {error}"))?;
    Ok(HnswBuildPool { threads, pool })
}

/// Configure the process-owned HNSW build pool before the first parallel build.
///
/// Every inline, catch-up, and rebuild job installs work into this one pool, so
/// concurrent jobs share workers through Rayon work stealing instead of each
/// creating a private pool. The first process runtime owns the width; later
/// calls report the effective width and never create an oversubscribing pool.
pub fn configure_hnsw_build_threads(threads: usize) -> usize {
    let requested = threads.max(1);
    let configured = match HNSW_BUILD_THREADS.compare_exchange(
        0,
        requested,
        Ordering::AcqRel,
        Ordering::Acquire,
    ) {
        Ok(_) => requested,
        Err(configured) => configured,
    };
    let effective = HNSW_BUILD_POOL
        .get()
        .and_then(|runtime| runtime.as_ref().ok())
        .map_or(configured, |runtime| runtime.threads);
    if effective != requested {
        tracing::warn!(
            requested_threads = requested,
            effective_threads = effective,
            "HNSW build pool is already configured for this process"
        );
    }
    effective
}

pub(crate) fn hnsw_build_pool() -> Result<(&'static ThreadPool, usize)> {
    let configured = HNSW_BUILD_THREADS.load(Ordering::Acquire);
    let threads = if configured == 0 {
        std::thread::available_parallelism()
            .map(usize::from)
            .unwrap_or(1)
    } else {
        configured
    };
    match HNSW_BUILD_POOL.get_or_init(|| create_build_pool(threads)) {
        Ok(runtime) => Ok((&runtime.pool, runtime.threads)),
        Err(error) => Err(paro_error::internal(error.clone())),
    }
}

pub(crate) fn hnsw_build_thread_count() -> usize {
    HNSW_BUILD_POOL
        .get()
        .and_then(|runtime| runtime.as_ref().ok())
        .map(|runtime| runtime.threads)
        .unwrap_or_else(|| {
            let configured = HNSW_BUILD_THREADS.load(Ordering::Acquire);
            if configured == 0 {
                std::thread::available_parallelism()
                    .map(usize::from)
                    .unwrap_or(1)
            } else {
                configured
            }
        })
}

/// Cooperative stop-check used by long-running HNSW build tasks.
#[derive(Clone)]
pub struct HnswBuildStopCheck(Arc<dyn Fn() -> bool + Send + Sync + 'static>);

impl HnswBuildStopCheck {
    pub fn new<F>(check: F) -> Self
    where
        F: Fn() -> bool + Send + Sync + 'static,
    {
        Self(Arc::new(check))
    }

    pub fn should_stop(&self) -> bool {
        (self.0)()
    }
}

impl<F> From<F> for HnswBuildStopCheck
where
    F: Fn() -> bool + Send + Sync + 'static,
{
    fn from(value: F) -> Self {
        Self::new(value)
    }
}

impl fmt::Debug for HnswBuildStopCheck {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("HnswBuildStopCheck(..)")
    }
}

/// Shared HNSW build entrypoint used by all storage-layer HNSW materialization paths.
#[derive(Clone, Debug, Default)]
pub struct HnswBuilder {
    stop_check: Option<HnswBuildStopCheck>,
    workspace_dir: Option<PathBuf>,
    execution_policy: HnswBuildExecutionPolicy,
}

impl HnswBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_stop_check(mut self, stop_check: HnswBuildStopCheck) -> Self {
        self.stop_check = Some(stop_check);
        self
    }

    /// Place large build-only routing workspaces beside the artifact staging
    /// files. This keeps them on a governed, capacity-checked filesystem and
    /// makes cancellation cleanup deterministic.
    pub fn with_workspace_dir(mut self, workspace_dir: impl AsRef<Path>) -> Self {
        self.workspace_dir = Some(workspace_dir.as_ref().to_path_buf());
        self
    }

    pub fn with_execution_policy(mut self, execution_policy: HnswBuildExecutionPolicy) -> Self {
        self.execution_policy = execution_policy;
        self
    }

    pub fn build(
        &self,
        storage: Arc<dyn VectorStorage>,
        build_contract: HnswBuildContract,
    ) -> Result<HnswIndex> {
        let (pool, pool_width) = hnsw_build_pool()?;
        HnswIndex::build_with_controls_and_filter_blocks_in_workspace_with_parallelism(
            storage,
            build_contract,
            HnswFilterBlocks::default(),
            Some(pool),
            self.execution_policy.granted_parallelism(pool_width),
            self.execution_policy.preparation_parallelism(pool_width),
            self.execution_policy,
            self.stop_check.as_ref(),
            self.workspace_dir.as_deref(),
        )
    }

    pub fn build_with_filter_blocks(
        &self,
        storage: Arc<dyn VectorStorage>,
        build_contract: HnswBuildContract,
        filter_blocks: HnswFilterBlocks,
    ) -> Result<HnswIndex> {
        let (pool, pool_width) = hnsw_build_pool()?;
        HnswIndex::build_with_controls_and_filter_blocks_in_workspace_with_parallelism(
            storage,
            build_contract,
            filter_blocks,
            Some(pool),
            self.execution_policy.granted_parallelism(pool_width),
            self.execution_policy.preparation_parallelism(pool_width),
            self.execution_policy,
            self.stop_check.as_ref(),
            self.workspace_dir.as_deref(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::hnsw::{DistanceMetric, HnswConfig, InMemoryVectorStorage};
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    #[serial_test::serial]
    fn maintenance_parallelism_holds_foreground_reservation_across_query_gaps() {
        {
            let _query = HnswForegroundQueryGuard::enter();
            assert_eq!(
                hnsw_current_build_parallelism(HnswBuildExecutionPolicy::Maintenance, 8),
                1
            );
        }
        assert_eq!(
            hnsw_current_build_parallelism(HnswBuildExecutionPolicy::Maintenance, 8),
            1
        );
    }

    #[test]
    #[serial_test::serial]
    fn maintenance_priority_cannot_oversubscribe_foreground_queries() {
        let _query = HnswForegroundQueryGuard::enter();
        assert_eq!(
            hnsw_current_build_parallelism(HnswBuildExecutionPolicy::Maintenance, 10),
            1
        );
    }

    #[test]
    #[serial_test::serial]
    fn optional_compaction_idle_window_tracks_query_exit() {
        let activity = Arc::new(HnswQueryActivity::default());
        let unrelated_definition = Arc::new(HnswQueryActivity::default());
        assert!(activity.quiet_for(Duration::ZERO));
        {
            let _query = activity.enter();
            assert!(!activity.quiet_for(Duration::from_secs(1)));
            assert!(unrelated_definition.quiet_for(Duration::from_secs(1)));
        }
        assert!(!activity.quiet_for(Duration::from_secs(1)));
    }

    #[test]
    fn foreground_pressure_deadline_has_a_precise_trailing_edge() {
        assert!(foreground_pressure_active_at(10, 1, 0));
        assert!(foreground_pressure_active_at(10, 0, 11));
        assert!(!foreground_pressure_active_at(11, 0, 11));
    }

    #[test]
    fn builder_stop_check_can_cancel_build() {
        let dim = 8;
        let points = 2048usize;
        let mut flat = Vec::with_capacity(points * dim);
        for i in 0..points {
            for j in 0..dim {
                flat.push((i * (j + 1)) as f32);
            }
        }

        let check_count = Arc::new(AtomicUsize::new(0));
        let stop_check = {
            let check_count = Arc::clone(&check_count);
            HnswBuildStopCheck::new(move || check_count.fetch_add(1, Ordering::Relaxed) > 0)
        };
        let builder = HnswBuilder::new().with_stop_check(stop_check);
        let result = builder.build(
            Arc::new(InMemoryVectorStorage::new(flat, dim)),
            HnswConfig::new(8, 50).build_contract(DistanceMetric::Euclidean),
        );
        let err = match result {
            Ok(_) => panic!("expected HNSW build to be cancelled by stop-check"),
            Err(err) => err,
        };
        assert!(err.is_query_canceled());
    }
}
