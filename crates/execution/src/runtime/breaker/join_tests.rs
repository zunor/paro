// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use paro_common::chunk::Chunk;
use paro_common::memory::MemoryAccountingClass;
use paro_common::runtime_value::Value;
use paro_common::test_utils::{
    test_allocator, test_chunk_from_vectors, test_i32_vector_with_allocator,
    test_vector_with_capacity,
};
use paro_common::types::LogicalType;
use paro_common::vector::SelectionVector;
use paro_context::test_support::TestStatementContextBuilder;
use paro_planner::expression::{Expression, ReferenceExpression};
use paro_planner::operator::join::JoinComparisonType;
use paro_storage::buffer::{BufferPool, MemoryTag};
use paro_storage::index::{Predicate, PredicateTree};
use paro_storage::row::RowValidityType;

use crate::explain::profiler::OperatorProfiler;
use crate::memory_runtime::QueryMemoryPool;
use crate::physical::properties::PipelineProperties;
use crate::physical::row_type::RowType;
use crate::pipeline::handles::{BreakerHandleId, BreakerHandleKind};
use crate::runtime::context::{OperatorCleanupContext, QueryRuntimeContext};
use crate::runtime::parameter::ParameterBindings;
use crate::runtime::scratch::TaskMemoryGrants;
use crate::runtime::QueryOutputPort;
use crate::thread_context::ThreadContext;

use super::*;

fn metadata() -> BreakerHandleMetadata {
    metadata_with_consumers(&[])
}

fn metadata_with_consumers(consumers: &[PipelineId]) -> BreakerHandleMetadata {
    BreakerHandleMetadata {
        id: BreakerHandleId::new(0),
        kind: BreakerHandleKind::HashJoinBuild,
        row_type: RowType::new(vec!["a".to_string()], vec![LogicalType::Integer]),
        producer: None,
        consumers: consumers.to_vec().into_boxed_slice(),
        properties: PipelineProperties::default(),
    }
}

#[test]
fn duplicate_consumer_completion_cannot_release_join_build_early() {
    let first = PipelineId::new(1);
    let second = PipelineId::new(2);
    let handle = JoinBuildHandle::new(metadata_with_consumers(&[first, second]));
    handle
        .initialize_table(
            Arc::new(BufferPool::new(16 * 1024 * 1024)),
            test_allocator(),
            vec![JoinCondition::new(
                Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
                Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
                JoinComparisonType::Equal,
            )],
            vec![LogicalType::Integer],
            JoinType::Inner,
            MemoryAccountingContext::detached(
                paro_common::allocator::MemoryTag::HashTable,
                MemoryAccountingClass::Revocable,
            ),
        )
        .expect("initialize hash table");

    assert!(!handle.consumer_finished(first));
    assert!(handle.table().is_some());
    assert!(!handle.consumer_finished(first));
    assert!(handle.table().is_some());

    assert!(handle.consumer_finished(second));
    assert!(handle.table().is_none());
    assert!(!handle.consumer_finished(second));
}

fn query_context() -> QueryRuntimeContext {
    QueryRuntimeContext::new(
        TestStatementContextBuilder::minimal().build(),
        Arc::new(ParameterBindings::empty()),
        Arc::new(QueryMemoryPool::unbounded()),
        QueryOutputPort::unbounded(),
    )
}

fn with_cleanup_context<R>(
    query: &QueryRuntimeContext,
    f: impl FnOnce(&mut OperatorCleanupContext<'_>) -> R,
) -> R {
    let thread = ThreadContext::single_threaded();
    let memory = TaskMemoryGrants::detached(test_allocator());
    let mut profiler = OperatorProfiler::disabled();
    let mut ctx = OperatorCleanupContext {
        query,
        pipeline: None,
        operator: None,
        thread: &thread,
        memory: memory.call_scope(),
        cancel: &query.cancellation,
        profiler: &mut profiler,
    };
    f(&mut ctx)
}

fn radix_input() -> Chunk {
    radix_input_with_rows(&[0, 1 << 63, 0, 1 << 63], &[10, 20, 30, 40])
}

fn radix_input_with_rows(hashes_input: &[u64], payload_input: &[i32]) -> Chunk {
    assert_eq!(hashes_input.len(), payload_input.len());
    let count = hashes_input.len();
    let mut hashes = test_vector_with_capacity(LogicalType::UBigInt, count);
    for (idx, hash) in hashes_input.iter().copied().enumerate() {
        hashes.set_u64(idx, hash);
    }
    hashes.set_count(count);

    let mut payload = test_vector_with_capacity(LogicalType::Integer, count);
    for (idx, value) in payload_input.iter().copied().enumerate() {
        payload.set_i32(idx, value);
    }
    payload.set_count(count);

    test_chunk_from_vectors(vec![hashes, payload])
}

fn partitioned_rows() -> RadixPartitionedRows {
    let pool = Arc::new(BufferPool::new(16 * 1024 * 1024));
    let layout = Arc::new(RowLayout::from_types(
        vec![LogicalType::UBigInt, LogicalType::Integer],
        RowValidityType::CanHaveNullValues,
    ));
    let mut builder = RadixPartitionedRowsBuilder::new(pool, layout, MemoryTag::HashTable, 1, 0)
        .expect("radix builder");
    builder.append(&radix_input()).expect("append radix input");
    builder.seal()
}

#[test]
fn join_build_mode_uses_atomic_discriminant_and_once_external_config() {
    let handle = JoinBuildHandle::new(metadata());
    assert_eq!(handle.mode(), JoinBuildMode::InMemory);
    assert!(!handle.is_external());
    assert!(handle.external_config().is_none());

    handle
        .set_external_mode(JoinExternalModeConfig {
            radix_bits: 4,
            build_partitions: JoinPartitionSet { partition_count: 8 },
            probe_partitions: ProbeSpillSet { partition_count: 8 },
        })
        .expect("external mode should be set once");

    assert_eq!(handle.mode(), JoinBuildMode::External);
    assert!(handle.is_external());
    assert_eq!(
        handle
            .external_config()
            .expect("external config")
            .build_partitions
            .partition_count,
        8
    );
    assert!(handle
        .set_external_mode(JoinExternalModeConfig {
            radix_bits: 5,
            build_partitions: JoinPartitionSet {
                partition_count: 16
            },
            probe_partitions: ProbeSpillSet {
                partition_count: 16
            },
        })
        .is_err());
}

#[test]
fn join_build_finalize_publishes_exact_runtime_filter() {
    let allocator = test_allocator();
    let handle = JoinBuildHandle::new(metadata());
    let table = handle
        .initialize_table(
            Arc::new(BufferPool::new(16 * 1024 * 1024)),
            allocator.clone(),
            vec![JoinCondition::new(
                Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
                Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
                JoinComparisonType::Equal,
            )],
            vec![LogicalType::Integer],
            JoinType::Inner,
            MemoryAccountingContext::detached(
                paro_common::allocator::MemoryTag::HashTable,
                MemoryAccountingClass::Revocable,
            ),
        )
        .expect("initialize hash table");
    let keys = Chunk::from_arc_vectors(
        vec![Arc::new(test_i32_vector_with_allocator(
            &[10, 20, 30],
            allocator.clone(),
        ))],
        allocator.clone(),
    );
    let payload = Chunk::from_arc_vectors(
        vec![Arc::new(test_i32_vector_with_allocator(
            &[100, 200, 300],
            allocator.clone(),
        ))],
        allocator.clone(),
    );
    table.build(&keys, &payload).expect("build hash table");
    let selection = SelectionVector::try_incremental(3, allocator.clone()).expect("selection");
    let mut sketch = JoinRuntimeFilterBuilder::empty(&[LogicalType::Integer]);
    sketch
        .add_key_chunk(&keys, &selection, 3)
        .expect("update runtime filter sketch");
    handle
        .merge_runtime_filter_builder(Some(sketch))
        .expect("merge runtime filter sketch");

    handle.finalize_in_memory().expect("finalize build");

    assert_eq!(
        handle.runtime_filter_predicate(0, 7).expect("predicate"),
        PredicateTree::leaf(Predicate::FixedIn {
            column_id: 7,
            values: paro_storage::index::FixedMembership::i32(vec![10, 20, 30]),
        })
    );
}

#[test]
fn oversized_join_runtime_filter_falls_back_to_min_max() {
    let allocator = test_allocator();
    let exact_value_limit = 3;
    let value_count = exact_value_limit + 1;
    let mut values = test_vector_with_capacity(LogicalType::Integer, value_count);
    for value in 0..value_count {
        values.set_i32(value, value as i32);
    }
    values.set_count(value_count);
    let keys = Chunk::from_arc_vectors(vec![Arc::new(values)], allocator.clone());
    let selection = SelectionVector::try_incremental(value_count, allocator).unwrap();
    let mut sketch = JoinRuntimeFilterBuilder::empty_with_exact_value_limit(
        &[LogicalType::Integer],
        exact_value_limit,
    );
    sketch
        .add_key_chunk(&keys, &selection, value_count)
        .expect("update oversized runtime filter sketch");
    let filter = sketch.freeze();

    assert_eq!(
        filter.predicate_for_column(0, 7).expect("predicate"),
        PredicateTree::leaf(Predicate::Range {
            column_id: 7,
            lower: Value::Integer(0),
            upper: Value::Integer(exact_value_limit as i32),
        })
    );
}

#[test]
fn hash_join_build_spill_reclaimer_externalizes_after_finish_enable() {
    let allocator = test_allocator();
    let handle = Arc::new(JoinBuildHandle::new(metadata()));
    let memory = MemoryAccountingContext::detached(
        paro_common::allocator::MemoryTag::HashTable,
        MemoryAccountingClass::Revocable,
    );
    let table = handle
        .initialize_table(
            Arc::new(BufferPool::new(16 * 1024 * 1024)),
            allocator.clone(),
            vec![JoinCondition::new(
                Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
                Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
                JoinComparisonType::Equal,
            )],
            vec![LogicalType::Integer],
            JoinType::Inner,
            memory.clone(),
        )
        .expect("initialize hash table");
    let keys = Chunk::from_arc_vectors(
        vec![Arc::new(test_i32_vector_with_allocator(
            &[10, 20, 30, 40],
            allocator.clone(),
        ))],
        allocator.clone(),
    );
    let payload = Chunk::from_arc_vectors(
        vec![Arc::new(test_i32_vector_with_allocator(
            &[100, 200, 300, 400],
            allocator.clone(),
        ))],
        allocator.clone(),
    );
    table.build(&keys, &payload).expect("build hash table");
    let reclaimer = HashJoinBuildSpillReclaimer::new(handle.clone(), memory, 16 * 1024 * 1024);

    assert_eq!(reclaimer.reclaimable_bytes(), 0);
    handle.enable_build_reclaim();
    let before = table.build_rows_size_in_bytes();
    assert!(before > 0);
    assert_eq!(reclaimer.reclaimable_bytes(), before);

    let stats = reclaimer.reclaim_sync(1).expect("reclaim hash join build");
    assert_eq!(stats.requested_bytes, 1);
    assert_eq!(stats.reclaimed_bytes, before);
    assert_eq!(stats.spilled_bytes, before);
    assert!(handle.is_external());
    assert!(handle.completion.is_complete());
    assert!(handle.runtime_filter_ready());
    assert_eq!(table.build_rows_size_in_bytes(), 0);
    assert_eq!(handle.spill.partition_counts().0, 2);
    assert_eq!(reclaimer.reclaimable_bytes(), 0);
}

#[test]
fn hash_join_local_build_spill_reclaimer_buffers_unmerged_build_rows() {
    let allocator = test_allocator();
    let handle = Arc::new(JoinBuildHandle::new(metadata()));
    let memory = MemoryAccountingContext::detached(
        paro_common::allocator::MemoryTag::HashTable,
        MemoryAccountingClass::Revocable,
    );
    handle
        .initialize_table(
            Arc::new(BufferPool::new(16 * 1024 * 1024)),
            allocator.clone(),
            vec![JoinCondition::new(
                Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
                Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
                JoinComparisonType::Equal,
            )],
            vec![LogicalType::Integer],
            JoinType::Inner,
            memory.clone(),
        )
        .expect("initialize hash table");
    let local_table = Arc::new(JoinHashTable::new_with_memory(
        Arc::new(BufferPool::new(16 * 1024 * 1024)),
        allocator.clone(),
        vec![JoinCondition::new(
            Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
            Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
            JoinComparisonType::Equal,
        )],
        vec![LogicalType::Integer],
        JoinType::Inner,
        JoinHashTableConfig::default(),
        memory.clone(),
    ));
    let keys = Chunk::from_arc_vectors(
        vec![Arc::new(test_i32_vector_with_allocator(
            &[10, 20, 30, 40],
            allocator.clone(),
        ))],
        allocator.clone(),
    );
    let payload = Chunk::from_arc_vectors(
        vec![Arc::new(test_i32_vector_with_allocator(
            &[100, 200, 300, 400],
            allocator.clone(),
        ))],
        allocator.clone(),
    );
    local_table
        .build(&keys, &payload)
        .expect("build local hash table");
    let build_spill = Arc::new(Mutex::new(None));
    let reclaimer = HashJoinLocalBuildSpillReclaimer::new(
        handle.clone(),
        HashJoinLocalBuildSpillReclaimer::next_local_id(),
        local_table.clone(),
        build_spill.clone(),
        memory.clone(),
        16 * 1024 * 1024,
    );

    let before = local_table.build_rows_size_in_bytes();
    assert!(before > 0);
    assert_eq!(reclaimer.reclaimable_bytes(), before);
    let stats = reclaimer
        .reclaim_sync(1)
        .expect("reclaim local hash join build");

    assert_eq!(stats.requested_bytes, 1);
    assert_eq!(stats.reclaimed_bytes, before);
    assert!(stats.spilled_bytes > 0);
    assert_eq!(local_table.build_rows_size_in_bytes(), 0);
    assert!(!handle.is_external());
    assert!(!handle.completion.is_complete());

    let buffer = build_spill.lock().take().expect("local build spill buffer");
    handle
        .spill
        .append_build_buffer(buffer)
        .expect("append local build spill to handle");
    assert!(handle.has_build_spill());
    assert_eq!(handle.spill.partition_counts().0, 2);

    handle
        .spill_build_for_reclaim(usize::MAX, 16 * 1024 * 1024, memory)
        .expect("finish external hash join from local spill");
    assert!(handle.is_external());
    assert!(handle.completion.is_complete());
    assert_eq!(handle.spill.partition_counts().0, 2);
}

#[test]
fn join_spill_cleanup_releases_partitions_and_resets_replay_state() {
    let spill = JoinSpillState::default();
    let pool = Arc::new(BufferPool::new(16 * 1024 * 1024));
    spill
        .install_build_partitions(partitioned_rows())
        .expect("install build partitions");
    let input = radix_input();
    let mut probe_buffer = JoinProbeSpillBuffer::new(
        pool,
        1,
        0,
        input.types(),
        MemoryAccountingContext::detached(
            paro_common::allocator::MemoryTag::HashTable,
            paro_common::memory::MemoryAccountingClass::Revocable,
        ),
    )
    .expect("probe spill buffer");
    probe_buffer
        .append(&input)
        .expect("append probe partition chunk");
    spill
        .append_probe_buffer(probe_buffer)
        .expect("append probe partition buffer");
    assert_eq!(spill.partition_counts(), (2, 2));
    let stats = spill.stats();
    assert_eq!(stats.build_rows, 4);
    assert_eq!(stats.probe_rows, 4);
    assert!(stats.build_bytes > 0);
    assert!(stats.probe_bytes > 0);
    assert!(stats.max_partition_bytes > 0);

    let first_partition = spill
        .take_next_replay_partition()
        .expect("first replay partition")
        .expect("partition");
    assert_eq!(first_partition.partition_idx, 0);
    assert!(spill.is_sealed());

    let query = query_context();
    with_cleanup_context(&query, |ctx| {
        spill
            .cleanup(
                ctx,
                CleanupReason::Cancelled(paro_context::StatementCancelReason::UserRequest),
            )
            .expect("cleanup spill");
    });

    assert_eq!(spill.partition_counts(), (0, 0));
    assert!(!spill.is_sealed());
    assert_eq!(spill.cleanup_status(), CleanupStatus::Cancelled);
    assert!(spill
        .take_next_replay_partition()
        .expect("cleanup should leave replay in an empty state")
        .is_none());
}

#[test]
fn join_spill_replay_partitions_use_reclaiming_row_scanners() {
    let spill = JoinSpillState::default();
    let pool = Arc::new(BufferPool::new(16 * 1024 * 1024));
    spill
        .install_build_partitions(partitioned_rows())
        .expect("install build partitions");
    let input = radix_input();
    let mut probe_buffer = JoinProbeSpillBuffer::new(
        pool,
        1,
        0,
        input.types(),
        MemoryAccountingContext::detached(
            paro_common::allocator::MemoryTag::HashTable,
            paro_common::memory::MemoryAccountingClass::Revocable,
        ),
    )
    .expect("probe spill buffer");
    probe_buffer
        .append(&input)
        .expect("append probe partition chunk");
    spill
        .append_probe_buffer(probe_buffer)
        .expect("append probe partition buffer");

    let partition = spill
        .take_next_replay_partition()
        .expect("replay partition")
        .expect("partition");
    let mut build_cursor = partition.build_rows.into_reclaiming_scanner();
    let mut probe_cursor = partition
        .probe_rows
        .expect("probe rows")
        .into_reclaiming_scanner();
    let expected_build_rows = build_cursor.count() as usize;
    let expected_probe_rows = probe_cursor.count() as usize;
    let mut chunk = Chunk::try_new(test_allocator()).expect("scan chunk");

    let mut build_rows = 0usize;
    loop {
        let scanned = build_cursor.next_chunk(&mut chunk).expect("scan build");
        if scanned == 0 {
            break;
        }
        build_rows += scanned;
    }
    assert_eq!(build_rows, expected_build_rows);
    assert!(build_rows > 0);

    let mut probe_rows = 0usize;
    loop {
        let scanned = probe_cursor.next_chunk(&mut chunk).expect("scan probe");
        if scanned == 0 {
            break;
        }
        probe_rows += scanned;
    }
    assert_eq!(probe_rows, expected_probe_rows);
    assert!(probe_rows > 0);
}

#[test]
fn join_spill_replay_preserves_build_partition_without_probe_rows() {
    let spill = JoinSpillState::default();
    let pool = Arc::new(BufferPool::new(16 * 1024 * 1024));
    spill
        .install_build_partitions(partitioned_rows())
        .expect("install build partitions");
    let input = radix_input_with_rows(&[0], &[100]);
    let mut probe_buffer = JoinProbeSpillBuffer::new(
        pool,
        1,
        0,
        input.types(),
        MemoryAccountingContext::detached(
            paro_common::allocator::MemoryTag::HashTable,
            paro_common::memory::MemoryAccountingClass::Revocable,
        ),
    )
    .expect("probe spill buffer");
    probe_buffer
        .append(&input)
        .expect("append one-sided probe partition chunk");
    spill
        .append_probe_buffer(probe_buffer)
        .expect("append probe partition buffer");

    let first = spill
        .take_next_replay_partition()
        .expect("first replay partition")
        .expect("first partition");
    assert_eq!(first.partition_idx, 0);
    assert!(first.probe_rows.is_some());
    let second = spill
        .take_next_replay_partition()
        .expect("second replay partition")
        .expect("second partition");
    assert_eq!(second.partition_idx, 1);
    assert!(second.build_rows.count() > 0);
    assert!(second.probe_rows.is_none());
}

#[test]
fn join_spill_replay_preserves_build_partitions_when_probe_never_spilled() {
    let spill = JoinSpillState::default();
    spill
        .install_build_partitions(partitioned_rows())
        .expect("install build partitions");

    let first = spill
        .take_next_replay_partition()
        .expect("first replay partition")
        .expect("first partition");
    assert_eq!(first.partition_idx, 0);
    assert!(first.build_rows.count() > 0);
    assert!(first.probe_rows.is_none());
}

#[test]
fn join_probe_spill_buffers_batch_global_appends() {
    let spill = JoinSpillState::default();
    let pool = Arc::new(BufferPool::new(16 * 1024 * 1024));
    let memory = MemoryAccountingContext::detached(
        paro_common::allocator::MemoryTag::HashTable,
        paro_common::memory::MemoryAccountingClass::Revocable,
    );
    let input = radix_input();

    let mut first = JoinProbeSpillBuffer::new(pool.clone(), 1, 0, input.types(), memory.clone())
        .expect("first probe spill buffer");
    first.append(&input).expect("append first probe chunk");
    let mut second = JoinProbeSpillBuffer::new(pool, 1, 0, input.types(), memory)
        .expect("second probe spill buffer");
    second.append(&input).expect("append second probe chunk");

    spill
        .append_probe_buffer(first)
        .expect("append first probe buffer");
    spill
        .append_probe_buffer(second)
        .expect("append second probe buffer");

    assert_eq!(spill.partition_counts(), (0, 2));
    let stats = spill.stats();
    assert_eq!(stats.probe_rows, 8);
    assert!(stats.probe_bytes > 0);
}

#[test]
fn hash_join_radix_bits_scale_with_build_bytes_and_query_cap() {
    assert_eq!(choose_hash_join_radix_bits(0, 1024 * 1024), 1);
    assert_eq!(
        choose_hash_join_radix_bits(2 * 1024 * 1024, 4 * 1024 * 1024),
        1
    );
    assert!(choose_hash_join_radix_bits(256 * 1024 * 1024, 16 * 1024 * 1024) >= 6);
    assert_eq!(
        choose_hash_join_radix_bits(usize::MAX / 2, usize::MAX / 2),
        HASH_JOIN_SPILL_MAX_RADIX_BITS
    );
}
