// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::{HnswBuildContract, HnswIndex, VectorStorage};
use paro_common::error::{self as paro_error, Result};
use rayon::ThreadPool;
use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

struct HnswBuildPool {
    threads: usize,
    pool: ThreadPool,
}

static HNSW_BUILD_THREADS: AtomicUsize = AtomicUsize::new(0);
static HNSW_BUILD_POOL: OnceLock<std::result::Result<HnswBuildPool, String>> = OnceLock::new();

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
}

impl HnswBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_stop_check(mut self, stop_check: HnswBuildStopCheck) -> Self {
        self.stop_check = Some(stop_check);
        self
    }

    pub fn build(
        &self,
        storage: Arc<dyn VectorStorage>,
        build_contract: HnswBuildContract,
    ) -> Result<HnswIndex> {
        let (pool, _) = hnsw_build_pool()?;
        HnswIndex::build_with_controls(
            storage,
            build_contract,
            Some(pool),
            self.stop_check.as_ref(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::hnsw::{DistanceMetric, HnswConfig, InMemoryVectorStorage};
    use std::sync::atomic::{AtomicUsize, Ordering};

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
