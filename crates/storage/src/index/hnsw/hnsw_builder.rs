// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::{HnswBuildContract, HnswIndex, VectorStorage};
use paro_common::error::Result;
use std::fmt;
use std::sync::Arc;

/// Query-independent execution policy for constructing one immutable HNSW artifact.
///
/// This is deliberately not part of [`HnswBuildContract`]: changing worker counts
/// must not invalidate an artifact or alter its topology. The frozen-wave epoch
/// is part of the builder algorithm, while this policy only controls how many workers
/// execute that algorithm. A fixed build contract therefore produces a byte-identical
/// graph at every worker count.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HnswBuildExecutionPolicy {
    max_threads: usize,
}

impl HnswBuildExecutionPolicy {
    // Topology constants belong to the versioned build algorithm, not runtime
    // scheduling. Keep epochs small enough that proposals do not observe a
    // materially stale graph while still exposing ample point-level parallelism.
    const PROPOSAL_WAVE_SIZE: usize = 64;
    const SERIAL_PREFIX_SIZE: usize = 4_096;

    pub const fn serial() -> Self {
        Self { max_threads: 1 }
    }

    pub fn parallel(max_threads: usize) -> Self {
        let max_threads = max_threads.max(1);
        if max_threads == 1 {
            return Self::serial();
        }
        Self { max_threads }
    }

    pub const fn max_threads(self) -> usize {
        self.max_threads
    }

    pub const fn proposal_wave_size(self) -> usize {
        Self::PROPOSAL_WAVE_SIZE
    }

    pub const fn serial_prefix_size(self) -> usize {
        Self::SERIAL_PREFIX_SIZE
    }

    pub const fn is_parallel(self) -> bool {
        self.max_threads > 1
    }
}

impl Default for HnswBuildExecutionPolicy {
    fn default() -> Self {
        Self::serial()
    }
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
    execution: HnswBuildExecutionPolicy,
}

impl HnswBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_stop_check(mut self, stop_check: HnswBuildStopCheck) -> Self {
        self.stop_check = Some(stop_check);
        self
    }

    pub fn with_execution_policy(mut self, execution: HnswBuildExecutionPolicy) -> Self {
        self.execution = execution;
        self
    }

    pub fn build(
        &self,
        storage: Arc<dyn VectorStorage>,
        build_contract: HnswBuildContract,
    ) -> Result<HnswIndex> {
        HnswIndex::build_with_controls(
            storage,
            build_contract,
            self.execution,
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
