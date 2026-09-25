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
        .finish_completed_pipelines([PipelineId::new(0), PipelineId::new(1)])
        .expect_err("missing continuation program must fail");

    assert!(error.to_string().contains("pipeline program missing"));
    assert_eq!(scheduler.finished, vec![true, true, false, true]);
    assert_eq!(scheduler.finished_count, 3);
}
