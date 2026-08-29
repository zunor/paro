// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Condvar, Mutex, OnceLock};
use std::time::Instant;

use paro_common::error::{self as paro_error, Result};
use rayon::ThreadPool;

use super::capability::SearchIndexKind;
use super::cursor::VisibleSegment;
use super::telemetry::{SearchTelemetryCollector, SegmentTelemetryEvent};

struct SearchDispatchRuntime {
    threads: usize,
    pool: ThreadPool,
    admission: SearchDispatchAdmission,
}

#[derive(Debug)]
struct SearchDispatchAdmission {
    state: Mutex<SearchDispatchAdmissionState>,
    changed: Condvar,
}

#[derive(Debug)]
struct SearchDispatchAdmissionState {
    available_lanes: usize,
    next_ticket: u64,
    serving_ticket: u64,
}

/// Query-wide ownership of a subset of the process search lanes.
///
/// A fixed worker pool bounds physical threads, but without admission every
/// concurrent query can still construct a full-width scoped fork. Those roots
/// occupy workers while their children wait, producing severe head-of-line
/// latency once `queries * graph_shards` exceeds the process width. A fair
/// ticketed lease makes the resource contract explicit and lets each admitted
/// query size its shard fan-out to the lanes it actually owns.
pub(crate) struct SearchDispatchLease<'runtime> {
    runtime: &'runtime SearchDispatchRuntime,
    lanes: usize,
}

impl SearchDispatchLease<'_> {
    pub(crate) const fn lanes(&self) -> usize {
        self.lanes
    }
}

impl Drop for SearchDispatchLease<'_> {
    fn drop(&mut self) {
        let mut state = self
            .runtime
            .admission
            .state
            .lock()
            .expect("search dispatch admission lock poisoned");
        state.available_lanes = state
            .available_lanes
            .saturating_add(self.lanes)
            .min(self.runtime.threads);
        self.runtime.admission.changed.notify_all();
    }
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
        .map(|pool| SearchDispatchRuntime {
            threads,
            pool,
            admission: SearchDispatchAdmission {
                state: Mutex::new(SearchDispatchAdmissionState {
                    available_lanes: threads,
                    next_ticket: 0,
                    serving_ticket: 0,
                }),
                changed: Condvar::new(),
            },
        })
        .map_err(|error| format!("create process search dispatch executor: {error}"))
}

/// Fairly acquire process search lanes for one complete provider query.
///
/// The head waiter accepts the currently available width instead of waiting
/// for its ideal width. This keeps every physical worker useful while FIFO
/// tickets prevent a stream of narrow queries from starving a wide one.
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

impl SearchDispatchRuntime {
    fn acquire_lanes(
        &self,
        requested_slots: usize,
        task_count: usize,
    ) -> Result<SearchDispatchLease<'_>> {
        let requested = requested_slots
            .max(1)
            .min(task_count.max(1))
            .min(self.threads);
        let mut state = self
            .admission
            .state
            .lock()
            .map_err(|_| paro_error::internal("search dispatch admission lock poisoned"))?;
        let ticket = state.next_ticket;
        state.next_ticket = state.next_ticket.wrapping_add(1);
        while ticket != state.serving_ticket || state.available_lanes == 0 {
            state = self
                .admission
                .changed
                .wait(state)
                .map_err(|_| paro_error::internal("search dispatch admission lock poisoned"))?;
        }
        let lanes = requested.min(state.available_lanes).max(1);
        state.available_lanes -= lanes;
        state.serving_ticket = state.serving_ticket.wrapping_add(1);
        self.admission.changed.notify_all();
        drop(state);
        Ok(SearchDispatchLease {
            runtime: self,
            lanes,
        })
    }
}

pub(crate) fn acquire_search_dispatch_lanes(
    requested_slots: usize,
    task_count: usize,
) -> Result<SearchDispatchLease<'static>> {
    search_dispatch_runtime(requested_slots)?.acquire_lanes(requested_slots, task_count)
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
    runtime.pool.install(|| {
        rayon::scope(|scope| {
            for _ in 1..lane_count {
                let run_lane = &run_lane;
                scope.spawn(move |_| run_lane());
            }
            run_lane();
        })
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
    use std::sync::{Barrier, Mutex};

    #[test]
    fn dispatch_runtime_owns_the_complete_process_width() {
        let runtime = create_search_dispatch_runtime(3).expect("search runtime");
        assert_eq!(runtime.threads, 3);
        assert_eq!(runtime.pool.current_num_threads(), 3);
    }

    #[test]
    fn admission_conserves_the_process_lane_budget() {
        let runtime = create_search_dispatch_runtime(3).expect("search runtime");
        let wide = runtime.acquire_lanes(3, 2).expect("wide lease");
        let remainder = runtime.acquire_lanes(3, 2).expect("remainder lease");
        assert_eq!(wide.lanes(), 2);
        assert_eq!(remainder.lanes(), 1);
        assert_eq!(
            runtime
                .admission
                .state
                .lock()
                .expect("admission state")
                .available_lanes,
            0
        );
        drop(wide);
        drop(remainder);
        assert_eq!(
            runtime
                .admission
                .state
                .lock()
                .expect("admission state")
                .available_lanes,
            3
        );
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
