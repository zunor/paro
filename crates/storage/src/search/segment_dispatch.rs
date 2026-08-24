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

    map_segments(segments, parallelism_slots, execute_one)
}

/// Execute an ordered segment phase in the shared query-governed pool.
///
/// Providers with an exact preparation phase use the same executor for
/// predicate evaluation and search. Keeping this primitive below telemetry
/// avoids reporting predicate preparation as a completed segment search, and
/// nested provider work reuses the pool without creating another executor.
pub(crate) fn map_segments<T, F>(
    segments: &[VisibleSegment],
    parallelism_slots: usize,
    execute: F,
) -> Result<Vec<T>>
where
    T: Send,
    F: Fn((usize, &VisibleSegment)) -> Result<T> + Send + Sync,
{
    if parallelism_slots > 1 && !segments.is_empty() {
        install_search_pool(parallelism_slots, || {
            segments
                .par_iter()
                .enumerate()
                .map(execute)
                .collect::<Result<Vec<_>>>()
        })
    } else {
        segments.iter().enumerate().map(execute).collect()
    }
}

/// Run provider work in the process-level search pool selected by the query's
/// execution grant.
///
/// This is the shared nesting boundary for segment dispatch and finer-grained
/// provider partitions. Rayon can work-steal nested jobs in the same pool, so
/// a query never creates a private executor or oversubscribes the granted
/// width when one large segment exposes more parallel work than its siblings.
pub(crate) fn install_search_pool<T, F>(parallelism_slots: usize, execute: F) -> Result<T>
where
    T: Send,
    F: FnOnce() -> Result<T> + Send,
{
    dispatch_pool(parallelism_slots.max(1))?.install(execute)
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
