// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::{HnswBuildContract, HnswFilterBlocks, HnswIndex, VectorStorage};
use paro_common::error::{self as paro_error, Result};
use rayon::ThreadPool;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

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
            // A maintenance build must make progress without monopolizing the
            // process. Reserve half of the shared pool for foreground work;
            // no private pool or extra OS thread is made. This also keeps a
            // large catch-up inside the bounded-lag maintenance envelope.
            Self::Maintenance => pool_width.div_ceil(2).max(1),
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
static HNSW_ACTIVE_MAINTENANCE_BUILDS: AtomicUsize = AtomicUsize::new(0);
const HNSW_MAINTENANCE_FOREGROUND_WORKERS: usize = 1;

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
        HNSW_ACTIVE_FOREGROUND_QUERIES.fetch_add(1, Ordering::AcqRel);
        Self
    }
}

impl Drop for HnswForegroundQueryGuard {
    fn drop(&mut self) {
        HNSW_ACTIVE_FOREGROUND_QUERIES.fetch_sub(1, Ordering::AcqRel);
    }
}

struct HnswMaintenanceBuildGuard;

impl HnswMaintenanceBuildGuard {
    fn enter(policy: HnswBuildExecutionPolicy) -> Option<Self> {
        (policy == HnswBuildExecutionPolicy::Maintenance).then(|| {
            HNSW_ACTIVE_MAINTENANCE_BUILDS.fetch_add(1, Ordering::AcqRel);
            Self
        })
    }
}

impl Drop for HnswMaintenanceBuildGuard {
    fn drop(&mut self) {
        HNSW_ACTIVE_MAINTENANCE_BUILDS.fetch_sub(1, Ordering::AcqRel);
    }
}

pub(crate) fn hnsw_current_build_parallelism(granted: usize) -> usize {
    if HNSW_ACTIVE_MAINTENANCE_BUILDS.load(Ordering::Acquire) != 0
        && HNSW_ACTIVE_FOREGROUND_QUERIES.load(Ordering::Acquire) != 0
    {
        granted.clamp(1, HNSW_MAINTENANCE_FOREGROUND_WORKERS)
    } else {
        granted.max(1)
    }
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
        let _maintenance_guard = HnswMaintenanceBuildGuard::enter(self.execution_policy);
        HnswIndex::build_with_controls_and_filter_blocks_in_workspace_with_parallelism(
            storage,
            build_contract,
            HnswFilterBlocks::default(),
            Some(pool),
            self.execution_policy.granted_parallelism(pool_width),
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
        let _maintenance_guard = HnswMaintenanceBuildGuard::enter(self.execution_policy);
        HnswIndex::build_with_controls_and_filter_blocks_in_workspace_with_parallelism(
            storage,
            build_contract,
            filter_blocks,
            Some(pool),
            self.execution_policy.granted_parallelism(pool_width),
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
    fn maintenance_parallelism_yields_at_wave_boundaries_to_foreground_queries() {
        let maintenance =
            HnswMaintenanceBuildGuard::enter(HnswBuildExecutionPolicy::Maintenance).unwrap();
        {
            let _query = HnswForegroundQueryGuard::enter();
            assert_eq!(hnsw_current_build_parallelism(8), 1);
        }
        drop(maintenance);
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
