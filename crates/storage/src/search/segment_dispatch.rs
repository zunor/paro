// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock, Mutex, RwLock};
use std::time::Instant;

use paro_common::error::{self as paro_error, Result};
use rayon::ThreadPool;

use super::capability::SearchIndexKind;
use super::cursor::VisibleSegment;
use super::telemetry::{SearchTelemetryCollector, SegmentTelemetryEvent};

static SEGMENT_DISPATCH_POOLS: LazyLock<RwLock<HashMap<usize, Arc<ThreadPool>>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

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
    map_search_tasks(segments, parallelism_slots, |index, segment| {
        execute((index, segment))
    })
}

/// Map borrowed search work in deterministic input order while keeping the
/// calling query thread as one execution lane.
///
/// The process pool owns only `parallelism_slots - 1` workers. `in_place_scope`
/// executes one lane on the query thread and lends the remaining work to those
/// workers, so a grant of four means four runnable lanes rather than four
/// Rayon workers plus a blocked executor thread. Nested exact-scan phases use
/// the same registry and remain work-stealable without oversubscription.
pub(crate) fn map_search_tasks<I, T, F>(
    items: &[I],
    parallelism_slots: usize,
    execute: F,
) -> Result<Vec<T>>
where
    I: Sync,
    T: Send,
    F: Fn(usize, &I) -> Result<T> + Sync,
{
    if items.is_empty() {
        return Ok(Vec::new());
    }
    let lane_count = parallelism_slots.max(1).min(items.len());
    if lane_count == 1 {
        return items
            .iter()
            .enumerate()
            .map(|(index, item)| execute(index, item))
            .collect();
    }

    let pool = dispatch_pool(parallelism_slots.max(1))?;
    // Reserve the first item for the query thread. Without this reservation a
    // newly-awakened worker can consume a short queue before the caller gets
    // scheduled, turning the caller back into a waiter and exceeding the
    // execution grant by one runnable thread.
    let next = AtomicUsize::new(1);
    let results = (0..items.len())
        .map(|_| Mutex::new(None))
        .collect::<Vec<Mutex<Option<Result<T>>>>>();
    let execute_index = |index: usize| {
        let result = execute(index, &items[index]);
        *results[index]
            .lock()
            .expect("search task result lock poisoned") = Some(result);
    };
    let run_lane = || loop {
        let index = next.fetch_add(1, Ordering::Relaxed);
        if index >= items.len() {
            break;
        }
        execute_index(index);
    };

    pool.in_place_scope(|scope| {
        for _ in 1..lane_count {
            let run_lane = &run_lane;
            scope.spawn(move |_| run_lane());
        }
        execute_index(0);
        run_lane();
    });

    results
        .into_iter()
        .enumerate()
        .map(|(index, result)| {
            result
                .into_inner()
                .map_err(|_| paro_error::internal("search task result lock poisoned"))?
                .ok_or_else(|| {
                    paro_error::internal(format!(
                        "search task {index} completed without publishing a result"
                    ))
                })?
        })
        .collect()
}

fn dispatch_pool(parallelism_slots: usize) -> Result<Arc<ThreadPool>> {
    if let Some(pool) = SEGMENT_DISPATCH_POOLS
        .read()
        .map_err(|_| paro_error::internal("read segment dispatch pool cache"))?
        .get(&parallelism_slots)
        .cloned()
    {
        return Ok(pool);
    }
    let pool = Arc::new(
        rayon::ThreadPoolBuilder::new()
            .num_threads(parallelism_slots.saturating_sub(1).max(1))
            .build()
            .map_err(|err| paro_error::internal(format!("build segment dispatch pool: {err}")))?,
    );
    let mut guard = SEGMENT_DISPATCH_POOLS
        .write()
        .map_err(|_| paro_error::internal("publish segment dispatch pool"))?;
    Ok(guard
        .entry(parallelism_slots)
        .or_insert_with(|| Arc::clone(&pool))
        .clone())
}

#[cfg(test)]
mod tests {
    use super::{dispatch_pool, map_search_tasks};
    use paro_common::error as paro_error;
    use std::sync::{Arc, Barrier, Mutex};

    #[test]
    fn dispatch_pool_is_cached_per_width() {
        let first = dispatch_pool(2).expect("first pool");
        let second = dispatch_pool(2).expect("second pool");
        let third = dispatch_pool(3).expect("third pool");

        assert!(Arc::ptr_eq(&first, &second));
        assert!(!Arc::ptr_eq(&first, &third));
        assert_eq!(first.current_num_threads(), 1);
        assert_eq!(third.current_num_threads(), 2);
    }

    #[test]
    fn map_search_tasks_preserves_order_and_uses_the_query_thread() {
        let caller = std::thread::current().id();
        let observed = Mutex::new(Vec::new());
        let values = [3usize, 1, 2, 0];

        let mapped = map_search_tasks(&values, 3, |index, value| {
            observed
                .lock()
                .expect("record executing thread")
                .push((index, std::thread::current().id()));
            Ok(index * 10 + value)
        })
        .expect("map search tasks");

        assert_eq!(mapped, vec![3, 11, 22, 30]);
        assert!(observed
            .into_inner()
            .expect("read executing threads")
            .contains(&(0, caller)));
    }

    #[test]
    fn nested_phase_on_pool_worker_does_not_deadlock() {
        let barrier = Arc::new(Barrier::new(2));
        let items = [0usize, 1];
        let mapped = map_search_tasks(&items, 2, |index, _| {
            barrier.wait();
            if index == 1 {
                let inner = map_search_tasks(&[2usize, 3], 2, |_, value| Ok(*value))?;
                Ok(inner.into_iter().sum())
            } else {
                Ok(1)
            }
        })
        .expect("nested search phase");

        assert_eq!(mapped, vec![1, 5]);
    }

    #[test]
    fn map_search_tasks_returns_the_first_input_order_error() {
        let error = map_search_tasks(&[0usize, 1, 2], 3, |index, _| {
            if index == 1 {
                Err(paro_error::internal("first ordered failure"))
            } else if index == 2 {
                Err(paro_error::internal("later ordered failure"))
            } else {
                Ok(index)
            }
        })
        .expect_err("map should fail");

        assert!(error.to_string().contains("first ordered failure"));
    }
}
