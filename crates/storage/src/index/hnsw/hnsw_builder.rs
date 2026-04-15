// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::{DistanceMetric, HnswConfig, HnswIndex, VectorStorage};
use paro_common::error::Result;
use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

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

/// Shared budget limiting how many concurrent HNSW builders may use rayon-parallel construction.
#[derive(Debug)]
pub struct HnswBuildConcurrencyBudget {
    max_parallel_builds: usize,
    active_parallel_builds: AtomicUsize,
}

impl HnswBuildConcurrencyBudget {
    pub fn new(max_parallel_builds: usize) -> Self {
        Self {
            max_parallel_builds,
            active_parallel_builds: AtomicUsize::new(0),
        }
    }

    pub fn max_parallel_builds(&self) -> usize {
        self.max_parallel_builds
    }

    fn try_acquire(self: &Arc<Self>) -> Option<HnswBuildParallelPermit> {
        if self.max_parallel_builds == 0 {
            return None;
        }

        let mut active = self.active_parallel_builds.load(Ordering::Acquire);
        loop {
            if active >= self.max_parallel_builds {
                return None;
            }
            match self.active_parallel_builds.compare_exchange_weak(
                active,
                active + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Some(HnswBuildParallelPermit {
                        budget: Arc::clone(self),
                    })
                }
                Err(observed) => active = observed,
            }
        }
    }
}

#[derive(Debug)]
struct HnswBuildParallelPermit {
    budget: Arc<HnswBuildConcurrencyBudget>,
}

impl Drop for HnswBuildParallelPermit {
    fn drop(&mut self) {
        self.budget
            .active_parallel_builds
            .fetch_sub(1, Ordering::AcqRel);
    }
}

/// Shared HNSW build entrypoint used by all storage-layer HNSW materialization paths.
#[derive(Clone, Debug, Default)]
pub struct HnswBuilder {
    stop_check: Option<HnswBuildStopCheck>,
    concurrency_budget: Option<Arc<HnswBuildConcurrencyBudget>>,
}

impl HnswBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_stop_check(mut self, stop_check: HnswBuildStopCheck) -> Self {
        self.stop_check = Some(stop_check);
        self
    }

    pub fn with_concurrency_budget(
        mut self,
        concurrency_budget: Arc<HnswBuildConcurrencyBudget>,
    ) -> Self {
        self.concurrency_budget = Some(concurrency_budget);
        self
    }

    pub fn build(
        &self,
        storage: Arc<dyn VectorStorage>,
        config: HnswConfig,
        distance: DistanceMetric,
    ) -> Result<HnswIndex> {
        let permit = self
            .concurrency_budget
            .as_ref()
            .and_then(|budget| budget.try_acquire());
        let use_parallel = self.concurrency_budget.is_none() || permit.is_some();
        let result = HnswIndex::build_with_controls(
            storage,
            config,
            distance,
            use_parallel,
            self.stop_check.as_ref(),
        );
        drop(permit);
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::hnsw::InMemoryVectorStorage;

    #[test]
    fn concurrency_budget_limits_parallel_slots() {
        let budget = Arc::new(HnswBuildConcurrencyBudget::new(1));
        let first = budget.try_acquire();
        assert!(first.is_some());
        assert!(budget.try_acquire().is_none());
        drop(first);
        assert!(budget.try_acquire().is_some());
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
            HnswConfig::new(8, 50),
            DistanceMetric::Euclidean,
        );
        let err = match result {
            Ok(_) => panic!("expected HNSW build to be cancelled by stop-check"),
            Err(err) => err,
        };
        assert!(err.is_query_canceled());
    }
}
