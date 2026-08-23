// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::Instant;

use paro_common::error::{self as paro_error, Result};
use rayon::prelude::*;
use rayon::ThreadPool;

use super::capability::SearchIndexKind;
use super::cursor::VisibleSegment;
use super::telemetry::{SearchTelemetryCollector, SegmentTelemetryEvent};

static SEGMENT_DISPATCH_POOLS: LazyLock<Mutex<HashMap<usize, Arc<ThreadPool>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

#[derive(Debug)]
pub(crate) struct SegmentDispatchResult<T> {
    pub(crate) output: T,
    pub(crate) candidates_produced: usize,
    pub(crate) degraded: bool,
}

pub(crate) fn dispatch_segments<T, F>(
    kind: SearchIndexKind,
    segments: &[VisibleSegment],
    parallelism_slots: usize,
    telemetry: &dyn SearchTelemetryCollector,
    execute: F,
) -> Result<Vec<T>>
where
    T: Send,
    F: Fn(usize, &VisibleSegment) -> Result<SegmentDispatchResult<T>> + Send + Sync,
{
    let execute_one = |(index, segment): (usize, &VisibleSegment)| {
        let started_at = Instant::now();
        let result = execute(index, segment);
        let elapsed = started_at.elapsed();
        result
            .map_err(|err| {
                err.context(format!(
                    "{kind:?} segment dispatch on rowset {} segment {}",
                    segment.rowset_id, segment.segment_id
                ))
            })
            .map(|outcome| {
                telemetry.record_segment_search(SegmentTelemetryEvent {
                    kind,
                    rowset_id: segment.rowset_id,
                    segment_id: segment.segment_id,
                    candidates_produced: outcome.candidates_produced,
                    degraded: outcome.degraded,
                    elapsed,
                });
                outcome.output
            })
    };

    if parallelism_slots > 1 && segments.len() > 1 {
        let pool = dispatch_pool(parallelism_slots.min(segments.len()))?;
        pool.install(|| {
            segments
                .par_iter()
                .enumerate()
                .map(execute_one)
                .collect::<Result<Vec<_>>>()
        })
    } else {
        segments.iter().enumerate().map(execute_one).collect()
    }
}

/// Apply an ordered preparation step to visible segments on the same governed
/// dispatch pool used by search. The returned vector remains index-aligned
/// with `segments`, so callers do not need a hash lookup or an impossible
/// "missing prepared segment" branch in the search hot path.
pub(crate) fn prepare_segments<T, F>(
    kind: SearchIndexKind,
    segments: &[VisibleSegment],
    parallelism_slots: usize,
    prepare: F,
) -> Result<Vec<T>>
where
    T: Send,
    F: Fn(usize, &VisibleSegment) -> Result<T> + Send + Sync,
{
    let prepare_one = |(index, segment): (usize, &VisibleSegment)| {
        prepare(index, segment).map_err(|err| {
            err.context(format!(
                "{kind:?} segment preparation on rowset {} segment {}",
                segment.rowset_id, segment.segment_id
            ))
        })
    };

    if parallelism_slots > 1 && segments.len() > 1 {
        let pool = dispatch_pool(parallelism_slots.min(segments.len()))?;
        pool.install(|| {
            segments
                .par_iter()
                .enumerate()
                .map(prepare_one)
                .collect::<Result<Vec<_>>>()
        })
    } else {
        segments.iter().enumerate().map(prepare_one).collect()
    }
}

fn dispatch_pool(thread_count: usize) -> Result<Arc<ThreadPool>> {
    let mut guard = SEGMENT_DISPATCH_POOLS
        .lock()
        .map_err(|_| paro_error::internal("lock segment dispatch pool cache"))?;
    if let Some(pool) = guard.get(&thread_count) {
        return Ok(Arc::clone(pool));
    }
    let pool = Arc::new(
        rayon::ThreadPoolBuilder::new()
            .num_threads(thread_count.max(1))
            .build()
            .map_err(|err| paro_error::internal(format!("build segment dispatch pool: {err}")))?,
    );
    guard.insert(thread_count, Arc::clone(&pool));
    Ok(pool)
}

#[cfg(test)]
mod tests {
    use super::dispatch_pool;
    use std::sync::Arc;

    #[test]
    fn dispatch_pool_is_cached_per_width() {
        let first = dispatch_pool(2).expect("first pool");
        let second = dispatch_pool(2).expect("second pool");
        let third = dispatch_pool(3).expect("third pool");

        assert!(Arc::ptr_eq(&first, &second));
        assert!(!Arc::ptr_eq(&first, &third));
    }
}
