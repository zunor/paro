// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::runtime::PipelineReadyPriority;
use crate::{
    memory_runtime::QueryMemoryPool,
    physical::{properties::PipelineProperties, specs::EmptyResultSpec, RowType},
    pipeline::{
        graph::{
            ClientResultSpec, DependencyKind, PipelineDependency, PipelineGraph, PipelineRoot,
            PipelineSpec, SinkSharing, SinkSpec, SourceSpec,
        },
        handles::BreakerHandleCatalog,
        PipelineIdMap, PipelineProgramBuilder, PipelineProgramIndex,
    },
    runtime::{ParameterBindings, QueryOutputPort},
};
use paro_context::TestStatementContextBuilder;

use super::*;

#[test]
fn ready_heap_uses_policy_priority() {
    let mut heap = BinaryHeap::new();
    heap.push(ReadyEntry {
        priority: PipelineReadyPriority::new(1),
        seq: 0,
        payload: PipelineId::new(0),
    });
    heap.push(ReadyEntry {
        priority: PipelineReadyPriority::new(10),
        seq: 1,
        payload: PipelineId::new(1),
    });
    assert_eq!(heap.pop().unwrap().payload, PipelineId::new(1));
}

#[test]
fn source_work_batches_many_morsels_into_bounded_contiguous_ranges() {
    let assignments = SourceWork::Chunks { count: 1_024 }.into_task_assignments(4);

    assert_eq!(assignments.len(), 4 * DATA_TASKS_PER_THREAD);
    assert_eq!(
        assignments.first(),
        Some(&SourceTaskAssignment::ChunkRange { start: 0, end: 64 })
    );
    assert_eq!(
        assignments.last(),
        Some(&SourceTaskAssignment::ChunkRange {
            start: 960,
            end: 1_024
        })
    );
    assert_eq!(
        assignments
            .iter()
            .copied()
            .map(|assignment| assignment.morsel_count().expect("morsel assignment"))
            .sum::<usize>(),
        1_024
    );
    assert!(assignments.windows(2).all(|pair| match pair {
        [
            SourceTaskAssignment::ChunkRange { end, .. },
            SourceTaskAssignment::ChunkRange { start, .. },
        ] => end == start,
        _ => false,
    }));

    assert!(SourceWork::Chunks { count: 0 }
        .into_task_assignments(4)
        .is_empty());
}

#[test]
fn empty_source_work_has_no_data_assignment() {
    assert_eq!(SourceWork::Empty.work_unit_count(), 0);
    assert!(SourceWork::Empty.into_task_assignments(4).is_empty());
}

#[test]
fn shared_queue_sources_spawn_workers_without_fake_morsels() {
    let assignments = SourceWork::SharedWorkers {
        count: 64,
        worker: SharedSourceWorker::HashAggregateEmit,
    }
    .into_task_assignments(4);

    assert_eq!(assignments.len(), 4);
    assert!(assignments.iter().all(|assignment| {
        *assignment == SourceTaskAssignment::SharedWorker(SharedSourceWorker::HashAggregateEmit)
            && assignment.morsel_count().is_none()
    }));

    let materialized = SourceWork::SharedWorkers {
        count: 64,
        worker: SharedSourceWorker::Materialized,
    }
    .into_task_assignments(4);
    assert_eq!(materialized.len(), 4);
    assert!(materialized.iter().all(|assignment| {
        *assignment == SourceTaskAssignment::SharedWorker(SharedSourceWorker::Materialized)
            && assignment.morsel_count().is_none()
    }));

    let rowset_assignments = SourceWork::RowsetScan { count: 19 }.into_task_assignments(4);
    assert_eq!(rowset_assignments.len(), 4);
    assert!(rowset_assignments.iter().all(|assignment| {
        *assignment == SourceTaskAssignment::SharedWorker(SharedSourceWorker::RowsetScan)
            && assignment.morsel_count().is_none()
    }));
}

#[test]
fn worker_coordinator_cancel_queued_releases_pending_slots() {
    let coordinator = PipelineWorkerCoordinator::new(3);
    coordinator.cancel_queued(2);
    assert_eq!(coordinator.remaining(), 1);
    coordinator.finish(Ok(()));
    assert_eq!(coordinator.snapshot().unwrap(), None);
}

#[test]
fn waiter_registry_wakes_registered_work_units_once() {
    let wake = PendingWakeRegistration {
        task_id: PipelineTaskId(7),
        source: WakeSource::Memory,
        token: crate::runtime::WakeToken(11),
        generation: crate::runtime::WakeGeneration(3),
    };
    let unit = WorkUnitId(99);
    let mut registry = WaiterRegistry::default();

    registry.register(wake, unit);
    registry.register(wake, unit);

    assert_eq!(registry.wake(wake.key()), vec![unit]);
    assert!(registry.wake(wake.key()).is_empty());
}

#[test]
fn waiter_registry_moves_unit_when_wake_key_changes() {
    let old_wake = PendingWakeRegistration {
        task_id: PipelineTaskId(7),
        source: WakeSource::Memory,
        token: crate::runtime::WakeToken(11),
        generation: crate::runtime::WakeGeneration(3),
    };
    let new_wake = PendingWakeRegistration {
        task_id: PipelineTaskId(7),
        source: WakeSource::Spill,
        token: crate::runtime::WakeToken(12),
        generation: crate::runtime::WakeGeneration(3),
    };
    let unit = WorkUnitId(99);
    let mut registry = WaiterRegistry::default();

    registry.register(old_wake, unit);
    registry.register(new_wake, unit);

    assert!(registry.wake(old_wake.key()).is_empty());
    assert_eq!(registry.wake(new_wake.key()), vec![unit]);
}

#[test]
fn completion_wave_publishes_and_drains_independent_siblings_before_error() {
    let output = RowType::new(Vec::new(), Vec::new());
    let pipelines = (0..4)
        .map(|index| PipelineSpec {
            id: PipelineId::new(index),
            source: SourceSpec::Empty(EmptyResultSpec),
            transforms: Vec::new(),
            sink: SinkSpec::ClientResult(ClientResultSpec),
            sink_sharing: SinkSharing::Exclusive,
            properties: PipelineProperties::default(),
            output: output.clone(),
        })
        .collect();
    let graph = PipelineGraph {
        pipelines,
        dependencies: vec![
            PipelineDependency {
                producer: PipelineId::new(0),
                consumer: PipelineId::new(2),
                kind: DependencyKind::MaterializeBeforeRead,
            },
            PipelineDependency {
                producer: PipelineId::new(1),
                consumer: PipelineId::new(3),
                kind: DependencyKind::MaterializeBeforeRead,
            },
        ],
        handles: BreakerHandleCatalog::default(),
        control_regions: Vec::new(),
        root: PipelineRoot::Pipeline(PipelineId::new(3)),
    };
    let mut programs = PipelineProgramBuilder::default()
        .build_program_set(&graph)
        .expect("pipeline programs");

    // Inject one bad continuation without disturbing the independent sibling.
    // The program arena remains dense so scheduler state still covers all ids;
    // only the id lookup contract for pipeline 2 is deliberately absent.
    let mut by_pipeline_id = PipelineIdMap::new(4);
    for index in [0, 1, 3] {
        by_pipeline_id
            .insert(PipelineId::new(index), PipelineProgramIndex::new(index))
            .expect("valid test pipeline id");
    }
    programs.by_pipeline_id = by_pipeline_id;

    let handles =
        Arc::new(BreakerHandleRegistry::from_catalog(&graph.handles).expect("breaker registry"));
    let query = QueryRuntimeContext::new(
        TestStatementContextBuilder::minimal().build(),
        Arc::new(ParameterBindings::empty()),
        Arc::new(QueryMemoryPool::unbounded()),
        QueryOutputPort::discarding(),
    );
    let mut scheduler = PipelineScheduler::new(
        &graph,
        &programs,
        handles,
        query,
        paro_common::test_utils::test_allocator(),
    )
    .expect("scheduler");

    let error = scheduler
        .finish_completed_pipelines([PipelineId::new(0), PipelineId::new(1)], true)
        .expect_err("missing continuation program must fail");

    assert!(error.to_string().contains("pipeline program missing"));
    assert_eq!(scheduler.finished, vec![true, true, false, true]);
    assert_eq!(scheduler.finished_count, 3);
}

#[test]
fn completed_pipeline_opens_consumers_while_independent_data_is_running() {
    let graph = PipelineGraph {
        pipelines: (0..3)
            .map(|index| PipelineSpec {
                id: PipelineId::new(index),
                source: SourceSpec::Empty(EmptyResultSpec),
                transforms: Vec::new(),
                sink: SinkSpec::ClientResult(ClientResultSpec),
                sink_sharing: SinkSharing::Exclusive,
                properties: PipelineProperties::default(),
                output: RowType::new(Vec::new(), Vec::new()),
            })
            .collect(),
        dependencies: vec![PipelineDependency {
            producer: PipelineId::new(0),
            consumer: PipelineId::new(2),
            kind: DependencyKind::MaterializeBeforeRead,
        }],
        handles: BreakerHandleCatalog::default(),
        control_regions: Vec::new(),
        root: PipelineRoot::Pipeline(PipelineId::new(2)),
    };
    let programs = PipelineProgramBuilder::default()
        .build_program_set(&graph)
        .unwrap();
    let handles = Arc::new(BreakerHandleRegistry::from_catalog(&graph.handles).unwrap());
    let query = QueryRuntimeContext::new(
        TestStatementContextBuilder::minimal().build(),
        Arc::new(ParameterBindings::empty()),
        Arc::new(QueryMemoryPool::unbounded()),
        QueryOutputPort::discarding(),
    );
    let mut scheduler = PipelineScheduler::new(
        &graph,
        &programs,
        handles,
        query,
        paro_common::test_utils::test_allocator(),
    )
    .unwrap();
    scheduler.ready.clear();
    // Model a completed local-merge barrier and an independent data worker
    // still in flight. Run the real finish task and dependency publication.
    let mut active = (0..2)
        .map(|index| {
            let runtime = scheduler.runtime(PipelineId::new(index)).unwrap();
            let workers = scheduler.query.session.scheduler().clone();
            ScheduledPipelineExecution {
                runtime,
                total_threads: 1,
                data: ScheduledDataTasks {
                    producer: workers.create_producer(),
                    scheduler: workers,
                    group: Arc::new(PipelineWorkerCoordinator::new(index)),
                    query: scheduler.query.clone(),
                },
                finish: None,
                query: scheduler.query.clone(),
                allocator: scheduler.allocator.clone(),
            }
        })
        .collect::<Vec<_>>();
    // Put the unfinished sibling first to catch ordered waits as well as waves.
    active.swap(0, 1);
    let deadline = Instant::now() + std::time::Duration::from_secs(5);
    while !scheduler.finished[0] {
        scheduler.poll_completed_pipelines(&mut active).unwrap();
        assert!(
            Instant::now() < deadline,
            "completed sibling remained gated"
        );
        std::thread::yield_now();
    }
    assert_eq!(scheduler.finished, vec![true, false, false]);
    assert!(scheduler.gates.is_ready(PipelineId::new(2)));
    assert!(scheduler
        .ready
        .iter()
        .any(|entry| entry.payload == PipelineId::new(2)));
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].data.group.remaining(), 1);
    active[0].data.group.finish(Ok(()));
    while !active.is_empty() {
        scheduler.poll_completed_pipelines(&mut active).unwrap();
        assert!(Instant::now() < deadline);
        std::thread::yield_now();
    }
    scheduler.run().unwrap();
    assert_eq!(scheduler.finished_count, 3);
}

#[test]
fn priority_admission_backfills_spare_slots_without_narrowing_wide_sources() {
    use crate::physical::specs::ChunkScanSpec;
    use paro_common::chunk::Chunk;

    let graph = PipelineGraph {
        pipelines: [2, 4, 1, 2, 1]
            .into_iter()
            .enumerate()
            .map(|(index, width)| {
                let mut properties = PipelineProperties::default();
                properties.capabilities.parallelism = Parallelism::bounded(width);
                PipelineSpec {
                    id: PipelineId::new(index),
                    source: if index == 4 {
                        SourceSpec::Empty(EmptyResultSpec)
                    } else {
                        SourceSpec::Chunk(ChunkScanSpec {
                            chunks: (0..4).map(|_| Chunk::default()).collect::<Vec<_>>().into(),
                            output_names: Box::new([]),
                            output_types: Box::new([]),
                        })
                    },
                    transforms: Vec::new(),
                    sink: SinkSpec::ClientResult(ClientResultSpec),
                    sink_sharing: SinkSharing::Exclusive,
                    properties,
                    output: RowType::new(Vec::new(), Vec::new()),
                }
            })
            .collect(),
        dependencies: vec![PipelineDependency {
            producer: PipelineId::new(0),
            consumer: PipelineId::new(4),
            kind: DependencyKind::MaterializeBeforeRead,
        }],
        handles: BreakerHandleCatalog::default(),
        control_regions: Vec::new(),
        root: PipelineRoot::Pipeline(PipelineId::new(3)),
    };
    let programs = PipelineProgramBuilder::default()
        .build_program_set(&graph)
        .unwrap();
    let handles = Arc::new(BreakerHandleRegistry::from_catalog(&graph.handles).unwrap());
    let memory = Arc::new(QueryMemoryPool::unbounded());
    memory.task_permits().set_max_permits(4);
    let query = QueryRuntimeContext::new(
        TestStatementContextBuilder::minimal()
            .with_limits(paro_context::RuntimeLimits {
                max_threads: 4,
                parallel_scheduler: true,
                ..Default::default()
            })
            .build(),
        Arc::new(ParameterBindings::empty()),
        memory,
        QueryOutputPort::discarding(),
    );
    let mut scheduler = PipelineScheduler::new(
        &graph,
        &programs,
        handles,
        query,
        paro_common::test_utils::test_allocator(),
    )
    .unwrap();
    scheduler.ready.clear();
    for index in 0..4 {
        scheduler.ready.push(ReadyEntry {
            priority: PipelineReadyPriority::new(if index == 1 {
                100_000
            } else {
                100 - index as i64
            }),
            seq: if index == 1 { 10 } else { index as u64 },
            payload: PipelineId::new(index),
        });
    }
    let pending = |scheduler: &mut PipelineScheduler<'_>, pipeline, total_threads| {
        let workers = scheduler.query.session.scheduler().clone();
        ScheduledPipelineExecution {
            runtime: scheduler.runtime(pipeline).unwrap(),
            total_threads,
            data: ScheduledDataTasks {
                producer: workers.create_producer(),
                scheduler: workers,
                group: Arc::new(PipelineWorkerCoordinator::new(1)),
                query: scheduler.query.clone(),
            },
            finish: None,
            query: scheduler.query.clone(),
            allocator: scheduler.allocator.clone(),
        }
    };
    let mut active = Vec::new();
    // P1 is newly ready and highest priority. It cannot take the whole budget
    // ahead of older narrow work; subsequent admission still uses priority.
    assert_eq!(
        scheduler.next_admitted_pipeline(&active).unwrap(),
        Some((PipelineId::new(0), 2))
    );
    active.push(pending(&mut scheduler, PipelineId::new(0), 2));
    // P1 requires four slots. P2 may fill one spare slot, without pinning P1
    // to reduced parallelism for the rest of its execution.
    assert_eq!(
        scheduler.next_admitted_pipeline(&active).unwrap(),
        Some((PipelineId::new(2), 1))
    );
    active.push(pending(&mut scheduler, PipelineId::new(2), 1));
    assert!(scheduler.next_admitted_pipeline(&active).unwrap().is_none());
    active[1].data.group.finish(Ok(()));
    let deadline = Instant::now() + std::time::Duration::from_secs(5);
    while !scheduler.finished[2] {
        scheduler.poll_completed_pipelines(&mut active).unwrap();
        assert!(Instant::now() < deadline);
        std::thread::yield_now();
    }
    // P3 starts while P0 is still unfinished: no wave completion barrier.
    assert_eq!(
        scheduler.next_admitted_pipeline(&active).unwrap(),
        Some((PipelineId::new(3), 2))
    );
    assert!(!scheduler.finished[0]);
    active.push(pending(&mut scheduler, PipelineId::new(3), 2));
    active[0].data.group.finish(Ok(()));
    while !scheduler.finished[0] {
        scheduler.poll_completed_pipelines(&mut active).unwrap();
        assert!(Instant::now() < deadline);
        std::thread::yield_now();
    }
    // The newly ready empty continuation owns real finish work. Admit it
    // before P1's higher-priority data, reserving only the two spare slots.
    assert_eq!(
        scheduler.next_admitted_pipeline(&active).unwrap(),
        Some((PipelineId::new(4), 2))
    );
    let finish = schedule_pipeline_data_tasks(
        scheduler.runtime(PipelineId::new(4)).unwrap(),
        Parallelism::single(),
        2,
        scheduler.query.clone(),
        scheduler.allocator.clone(),
    )
    .unwrap();
    assert_eq!(finish.total_threads, 2);
    active.push(finish);
    assert!(scheduler.next_admitted_pipeline(&active).unwrap().is_none());
    while !scheduler.finished[4] {
        scheduler.poll_completed_pipelines(&mut active).unwrap();
        assert!(Instant::now() < deadline);
        std::thread::yield_now();
    }
    assert!(!scheduler.finished[3]);
    active[0].data.group.finish(Ok(()));
    while !active.is_empty() {
        scheduler.poll_completed_pipelines(&mut active).unwrap();
        assert!(Instant::now() < deadline);
        std::thread::yield_now();
    }
    assert_eq!(
        scheduler.next_admitted_pipeline(&active).unwrap(),
        Some((PipelineId::new(1), 4))
    );
}

#[test]
fn empty_continuation_finish_obeys_permits_and_cancels_while_blocked() {
    for cancel in [false, true] {
        let graph = PipelineGraph {
            pipelines: vec![PipelineSpec {
                id: PipelineId::new(0),
                source: SourceSpec::Empty(EmptyResultSpec),
                transforms: Vec::new(),
                sink: SinkSpec::ClientResult(ClientResultSpec),
                sink_sharing: SinkSharing::Exclusive,
                properties: PipelineProperties::default(),
                output: RowType::new(Vec::new(), Vec::new()),
            }],
            dependencies: Vec::new(),
            handles: BreakerHandleCatalog::default(),
            control_regions: Vec::new(),
            root: PipelineRoot::Pipeline(PipelineId::new(0)),
        };
        let programs = PipelineProgramBuilder::default()
            .build_program_set(&graph)
            .unwrap();
        let handles = Arc::new(BreakerHandleRegistry::from_catalog(&graph.handles).unwrap());
        let memory = Arc::new(QueryMemoryPool::unbounded());
        memory.task_permits().set_max_permits(1);
        let query = Arc::new(QueryRuntimeContext::new(
            TestStatementContextBuilder::minimal().build(),
            Arc::new(ParameterBindings::empty()),
            memory.clone(),
            QueryOutputPort::discarding(),
        ));
        let runtime = Arc::new(
            PipelineRuntime::with_registry(
                programs.get(PipelineId::new(0)).unwrap().clone(),
                handles,
                query.params.clone(),
                query.as_ref(),
            )
            .unwrap(),
        );
        let held = memory.task_permits().try_acquire_available().unwrap();
        let mut execution = schedule_pipeline_data_tasks(
            runtime,
            Parallelism::single(),
            1,
            query,
            paro_common::test_utils::test_allocator(),
        )
        .unwrap();
        assert_eq!(execution.data.group.remaining(), 0);
        assert!(!execution.poll_complete().unwrap());
        let deadline = Instant::now() + std::time::Duration::from_secs(5);
        while execution
            .finish
            .as_ref()
            .unwrap()
            .group
            .inner
            .lock()
            .unwrap()
            .blocked
            .is_empty()
        {
            assert!(
                Instant::now() < deadline,
                "finish did not enter permit wait"
            );
            std::thread::yield_now();
        }
        assert!(!execution.poll_complete().unwrap());
        assert_eq!(memory.task_permits().used_permits(), 1);
        if cancel {
            execution.cancel();
            assert_eq!(execution.finish.as_ref().unwrap().group.remaining(), 0);
        }
        drop(held);
        while !execution.poll_complete().unwrap() {
            assert!(
                Instant::now() < deadline,
                "finish did not resume after permit release"
            );
            std::thread::yield_now();
        }
        assert_eq!(memory.task_permits().used_permits(), 0);
    }
}
