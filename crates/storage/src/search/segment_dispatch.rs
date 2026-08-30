// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::Instant;

use paro_common::error::{self as paro_error, Result};
use rayon::ThreadPool;

use super::capability::SearchIndexKind;
use super::cursor::VisibleSegment;
use super::telemetry::{SearchTelemetryCollector, SegmentTelemetryEvent};

struct SearchDispatchRuntime {
    threads: usize,
    pool: ThreadPool,
}

static SEARCH_DISPATCH_THREADS: AtomicUsize = AtomicUsize::new(0);
static SEARCH_DISPATCH_RUNTIME: OnceLock<std::result::Result<SearchDispatchRuntime, String>> =
    OnceLock::new();

/// Configure the process-owned search executor before the first query.
///
/// All providers and concurrent queries share one fixed-width executor. Query
/// protocol threads submit work and wait instead of becoming untracked lanes
/// beside provider-private pools, so total search CPU is bounded by the
/// instance runtime rather than by `connections * requested_parallelism`.
pub fn configure_search_dispatch_threads(threads: usize) -> usize {
    let requested = threads.max(1);
    let configured = match SEARCH_DISPATCH_THREADS.compare_exchange(
        0,
        requested,
        Ordering::AcqRel,
        Ordering::Acquire,
    ) {
        Ok(_) => requested,
        Err(configured) => configured,
    };
    let effective = SEARCH_DISPATCH_RUNTIME
        .get()
        .and_then(|runtime| runtime.as_ref().ok())
        .map_or(configured, |runtime| runtime.threads);
    if effective != requested {
        tracing::warn!(
            requested_threads = requested,
            effective_threads = effective,
            "search dispatch executor was already configured by another runtime"
        );
    }
    effective
}

fn create_search_dispatch_runtime(
    threads: usize,
) -> std::result::Result<SearchDispatchRuntime, String> {
    let threads = threads.max(1);
    rayon::ThreadPoolBuilder::new()
        .thread_name(|index| format!("paro-search-{index}"))
        // Connection threads submit and wait; only this process-owned pool
        // executes provider work. Counting every caller as a lane makes an
        // advertised width of ten expand to `pool + concurrent_connections`
        // and turns wide exact-tail scans into a memory-bandwidth storm.
        .num_threads(threads)
        .build()
        .map(|pool| SearchDispatchRuntime { threads, pool })
        .map_err(|error| format!("create process search dispatch executor: {error}"))
}

fn search_dispatch_runtime(requested_slots: usize) -> Result<&'static SearchDispatchRuntime> {
    let configured = SEARCH_DISPATCH_THREADS.load(Ordering::Acquire);
    let threads = if configured == 0 {
        // Direct storage embeddings and unit tests do not construct an
        // instance runtime. The server path configures this value before any
        // query is admitted.
        requested_slots.max(1)
    } else {
        configured
    };
    SEARCH_DISPATCH_RUNTIME
        .get_or_init(|| create_search_dispatch_runtime(threads))
        .as_ref()
        .map_err(|error| paro_error::internal(error.clone()))
}

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

/// Dispatch only the selected visible segments while preserving their index
/// in the immutable table read lease.
///
/// Generation artifacts normally cover most or all visible segments. Sending
/// every covered segment through the executor merely to return an empty result
/// turns rowset fan-out into scheduler work and can starve the much smaller set
/// of graph tasks under concurrent queries. Coverage routing is deterministic,
/// so compile it before submission and expose only the exact tail here.
pub(crate) fn dispatch_segment_indices<T, F>(
    kind: SearchIndexKind,
    segments: &[VisibleSegment],
    indices: &[usize],
    parallelism_slots: usize,
    telemetry: &dyn SearchTelemetryCollector,
    execute: F,
) -> Result<Vec<T>>
where
    T: Send,
    F: Fn(usize, &VisibleSegment) -> Result<SegmentDispatchResult<T>> + Send + Sync,
{
    map_search_tasks(indices, parallelism_slots, |_, visible_index| {
        let segment = segments.get(*visible_index).ok_or_else(|| {
            paro_error::internal("selected search segment index is out of bounds")
        })?;
        let started_at = Instant::now();
        let result = execute(*visible_index, segment);
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
    })
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

/// Map borrowed search work in deterministic input order while sharing the
/// process-owned executor.
///
/// Every query publishes its independent shard tasks to one fixed-width
/// process executor. Protocol/connection threads never execute provider work,
/// so query concurrency cannot manufacture unaccounted CPU lanes. Avoid
/// slicing the workers into static per-query grants: HNSW shards are
/// random-memory workloads with unequal completion times, and the global
/// work-stealing queue naturally shares capacity between queries.
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
    let runtime = search_dispatch_runtime(parallelism_slots)?;
    map_search_tasks_on_runtime(runtime, items, parallelism_slots, execute)
}

fn spawn_search_lane_continuation<'scope, I, T, F>(
    scope: &rayon::ScopeFifo<'scope>,
    next: &'scope AtomicUsize,
    items: &'scope [I],
    results: &'scope [Mutex<Option<Result<T>>>],
    execute: &'scope F,
) where
    I: Sync + 'scope,
    T: Send + 'scope,
    F: Fn(usize, &I) -> Result<T> + Sync + 'scope,
{
    scope.spawn_fifo(move |scope| {
        let index = next.fetch_add(1, Ordering::Relaxed);
        if index >= items.len() {
            return;
        }
        let result = execute(index, &items[index]);
        *results[index]
            .lock()
            .expect("search task result lock poisoned") = Some(result);
        // Return to the global FIFO after every shard. At most one
        // continuation per granted lane exists, so this preserves the query's
        // parallelism ceiling without letting a long-lived lane monopolize a
        // worker across multiple provider shards.
        spawn_search_lane_continuation(scope, next, items, results, execute);
    });
}

fn map_search_tasks_on_runtime<I, T, F>(
    runtime: &SearchDispatchRuntime,
    items: &[I],
    parallelism_slots: usize,
    execute: F,
) -> Result<Vec<T>>
where
    I: Sync,
    T: Send,
    F: Fn(usize, &I) -> Result<T> + Sync,
{
    let lane_count = runtime
        .threads
        .min(parallelism_slots.max(1))
        .min(items.len());
    if lane_count == 1 {
        return runtime.pool.install(|| {
            items
                .iter()
                .enumerate()
                .map(|(index, item)| execute(index, item))
                .collect()
        });
    }

    let next = AtomicUsize::new(0);
    let results = (0..items.len())
        .map(|_| Mutex::new(None))
        .collect::<Vec<Mutex<Option<Result<T>>>>>();
    // A continuation per granted lane preserves the caller's parallelism
    // ceiling without inserting barriers between uneven shards. Completing a
    // shard requeues that lane behind already-runnable work, so the fixed
    // Rayon FIFO remains the sole fair scheduler shared by HNSW, sparse, and
    // full-text queries.
    runtime.pool.scope_fifo(|scope| {
        for _ in 0..lane_count {
            spawn_search_lane_continuation(scope, &next, items, &results, &execute);
        }
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

#[cfg(test)]
mod tests {
    use super::{create_search_dispatch_runtime, map_search_tasks_on_runtime};
    use paro_common::error as paro_error;
    use std::sync::{Arc, Barrier, Condvar, Mutex};
    use std::time::Duration;

    #[test]
    fn dispatch_runtime_owns_the_complete_process_width() {
        let runtime = create_search_dispatch_runtime(3).expect("search runtime");
        assert_eq!(runtime.threads, 3);
        assert_eq!(runtime.pool.current_num_threads(), 3);
    }

    #[test]
    fn map_search_tasks_preserves_order_without_using_the_query_thread() {
        let runtime = create_search_dispatch_runtime(3).expect("search runtime");
        let caller = std::thread::current().id();
        let observed = Mutex::new(Vec::new());
        let values = [3usize, 1, 2, 0];

        let mapped = map_search_tasks_on_runtime(&runtime, &values, 3, |index, value| {
            observed
                .lock()
                .expect("record executing thread")
                .push((index, std::thread::current().id()));
            Ok(index * 10 + value)
        })
        .expect("map search tasks");

        assert_eq!(mapped, vec![3, 11, 22, 30]);
        assert!(!observed
            .into_inner()
            .expect("read executing threads")
            .iter()
            .any(|(_, thread)| *thread == caller));
    }

    #[test]
    fn nested_phase_on_pool_worker_does_not_deadlock() {
        let runtime = create_search_dispatch_runtime(2).expect("search runtime");
        let barrier = std::sync::Arc::new(Barrier::new(2));
        let items = [0usize, 1];
        let mapped = map_search_tasks_on_runtime(&runtime, &items, 2, |index, _| {
            barrier.wait();
            if index == 1 {
                let inner =
                    map_search_tasks_on_runtime(&runtime, &[2usize, 3], 2, |_, value| Ok(*value))?;
                Ok(inner.into_iter().sum())
            } else {
                Ok(1)
            }
        })
        .expect("nested search phase");

        assert_eq!(mapped, vec![1, 5]);
    }

    #[test]
    fn completed_lane_pulls_next_uneven_shard_without_a_batch_barrier() {
        let runtime = create_search_dispatch_runtime(2).expect("search runtime");
        let third_started = Arc::new((Mutex::new(false), Condvar::new()));
        let mapped = map_search_tasks_on_runtime(&runtime, &[0usize, 1, 2], 2, |index, _| {
            match index {
                1 => {
                    let (started, changed) = third_started.as_ref();
                    let observed = started.lock().expect("third-shard state");
                    let (observed, timeout) = changed
                        .wait_timeout_while(observed, Duration::from_secs(2), |value| !*value)
                        .expect("wait for third shard");
                    assert!(*observed, "third shard remained behind a batch barrier");
                    assert!(!timeout.timed_out(), "third shard did not start promptly");
                }
                2 => {
                    let (started, changed) = third_started.as_ref();
                    *started.lock().expect("third-shard state") = true;
                    changed.notify_all();
                }
                _ => {}
            }
            Ok(index)
        })
        .expect("map uneven search tasks");

        assert_eq!(mapped, vec![0, 1, 2]);
    }

    #[test]
    fn map_search_tasks_returns_the_first_input_order_error() {
        let runtime = create_search_dispatch_runtime(3).expect("search runtime");
        let error = map_search_tasks_on_runtime(&runtime, &[0usize, 1, 2], 3, |index, _| {
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
