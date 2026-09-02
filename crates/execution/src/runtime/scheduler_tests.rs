// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::runtime::PipelineReadyPriority;

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
