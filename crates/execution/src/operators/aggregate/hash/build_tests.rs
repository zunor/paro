// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;

use paro_common::memory::{MemoryAccountingClass, MemoryAccountingContext};
use paro_common::runtime_value::Value;
use paro_context::test_support::TestStatementContextBuilder;
use paro_function::aggregate::distributive::count::get_count_star_function;
use paro_function::aggregate::distributive::string_agg::get_string_agg_function;
use paro_planner::expression::{AggregateExpression, Expression, ReferenceExpression};
use paro_storage::row::RowSpillReader;

use crate::explain::profiler::OperatorProfiler;
use crate::memory_runtime::QueryMemoryPool;
use crate::operators::aggregate::radix_partitioned_aggregate_hashtable::AggregateHTScanPosition;
use crate::physical::specs::GroupKeyEncoding;
use crate::pipeline::graph::PipelineId;
use crate::runtime::parameter::ParameterBindings;
use crate::runtime::scratch::TaskMemoryGrants;
use crate::runtime::{
    OperatorWakeScope, PipelineTaskId, QueryOutputPort, QueryRuntimeContext, RuntimeOperatorId,
    WakeGeneration,
};
use crate::thread_context::ThreadContext;

use crate::physical::properties::PipelineProperties;
use crate::physical::row_type::RowType;
use crate::pipeline::handles::{BreakerHandleCatalogBuilder, BreakerHandleKind};
use crate::runtime::breaker::BreakerHandleRegistry;
use crate::runtime::context::OperatorScratchScope;
use crate::runtime::scratch::ExpressionScratchArena;
use crate::runtime::sink::NextFinishTask;

fn build_grouping_merge_fixture(
    query: &QueryRuntimeContext,
    spec: AggregateSpec,
    batches: Vec<Vec<Chunk>>,
) -> (HashAggregateBuildSinkExec, SinkGlobal) {
    let properties = PipelineProperties::default();
    let mut catalog = BreakerHandleCatalogBuilder::default();
    let id = catalog.register(
        BreakerHandleKind::Aggregate,
        RowType::new(spec.output_names.to_vec(), spec.output_types.to_vec()),
        properties.clone(),
    );
    let handles = BreakerHandleRegistry::from_catalog(&catalog.finish()).unwrap();
    let exec = HashAggregateBuildSinkExec {
        handle: HandleRef::new(id),
        spec,
    };
    let mut init = PipelineInitContext {
        query,
        pipeline: PipelineId::new(0),
        operator: RuntimeOperatorId::new(0),
        params: &query.params,
        handles: &handles,
        properties: &properties,
    };
    let global = exec.create_global(&mut init).unwrap();
    let thread = ThreadContext::single_threaded();
    let memory = TaskMemoryGrants::detached(paro_common::test_utils::test_allocator());
    let wake = OperatorWakeScope {
        task_id: PipelineTaskId(61),
        generation: WakeGeneration(0),
    };
    let mut profiler = OperatorProfiler::disabled();
    let mut scratch = ExpressionScratchArena::default();
    for batch in batches {
        let mut local = exec.create_local(&mut init, &global).unwrap();
        let mut ctx = OperatorCallContext {
            query,
            pipeline: PipelineId::new(0),
            operator: RuntimeOperatorId::new(0),
            thread: &thread,
            memory: memory.call_scope(),
            scratch: OperatorScratchScope::from_expression(&mut scratch),
            cancel: &query.cancellation,
            wake: &wake,
            profiler: &mut profiler,
        };
        for mut chunk in batch {
            exec.consume(&mut ctx, &global, &mut local, &mut chunk)
                .unwrap();
        }
        exec.merge_local(&mut ctx, &global, &mut local).unwrap();
    }
    (exec, global)
}

fn grouping_merge_rows(global: &SinkGlobal, spec: &AggregateSpec) -> Vec<Vec<Vec<Value>>> {
    let SinkGlobal::HashAggregateBuild(global) = global else {
        unreachable!()
    };
    assert!(global.handle.is_finalized());
    global
        .handle
        .with_state_mut(|state| {
            let AggregateRuntimeState::Hash(state) = state else {
                unreachable!()
            };
            if let Some(outputs) = state.spilled_outputs.take() {
                return outputs
                    .into_iter()
                    .map(|output| {
                        let mut rows = Vec::new();
                        if let Some(output) = output {
                            let mut reader = output.into_reader();
                            let mut chunk = Chunk::try_initialize(
                                &spec.output_types,
                                VECTOR_SIZE,
                                paro_common::test_utils::test_allocator(),
                            )?;
                            while reader.read_next(&mut chunk)? > 0 {
                                for row in 0..chunk.size() {
                                    rows.push(
                                        chunk
                                            .data
                                            .iter()
                                            .map(|column| column.get_value(row))
                                            .collect(),
                                    );
                                }
                            }
                        }
                        rows.sort_by_key(|row| format!("{row:?}"));
                        Ok(rows)
                    })
                    .collect();
            }
            let mut domains = Vec::new();
            for table in &mut state.tables {
                let mut position = AggregateHTScanPosition::default();
                let mut output = Chunk::try_initialize(
                    &spec.output_types,
                    VECTOR_SIZE,
                    paro_common::test_utils::test_allocator(),
                )?;
                let mut rows = Vec::new();
                while table.scan(&mut position, &mut output)? {
                    for row in 0..output.size() {
                        rows.push(
                            output
                                .data
                                .iter()
                                .map(|column| column.get_value(row))
                                .collect(),
                        );
                    }
                }
                rows.sort_by_key(|row| format!("{row:?}"));
                domains.push(rows);
            }
            Ok(domains)
        })
        .unwrap()
}

fn finish_grouping_merge_fixture(
    query: &QueryRuntimeContext,
    exec: &HashAggregateBuildSinkExec,
    global: &SinkGlobal,
    serial: bool,
) -> usize {
    let thread = ThreadContext::single_threaded();
    let memory = TaskMemoryGrants::detached(paro_common::test_utils::test_allocator());
    let wake = OperatorWakeScope {
        task_id: PipelineTaskId(62),
        generation: WakeGeneration(0),
    };
    let mut profiler = OperatorProfiler::disabled();
    let mut ctx = OperatorFinishContext {
        query,
        pipeline: PipelineId::new(0),
        operator: RuntimeOperatorId::new(0),
        finish_task: None,
        thread: &thread,
        memory: memory.call_scope(),
        cancel: &query.cancellation,
        wake: &wake,
        profiler: &mut profiler,
    };
    if serial {
        let SinkGlobal::HashAggregateBuild(global) = global else {
            unreachable!()
        };
        global
            .handle
            .with_state_mut(|state| {
                let AggregateRuntimeState::Hash(state) = state else {
                    unreachable!()
                };
                merge_pending_radix_tables(state)
            })
            .unwrap();
    }
    exec.prepare_finish(&mut ctx, global).unwrap();
    let task_count = match exec.finish_work(&mut ctx, global).unwrap() {
        FinishWork::None => 0,
        FinishWork::Parallel(group) => {
            let mut tasks = Vec::new();
            while let NextFinishTask::Task(task) = group.driver.next_task(&mut ctx).unwrap() {
                tasks.push(task);
            }
            assert!(group.driver.finish_group(&mut ctx).is_err());
            std::thread::scope(|scope| {
                let joins = tasks
                    .into_iter()
                    .rev()
                    .map(|task| {
                        let driver = group.driver.clone();
                        scope.spawn(move || {
                            let thread = ThreadContext::single_threaded();
                            let memory = TaskMemoryGrants::detached(
                                paro_common::test_utils::test_allocator(),
                            );
                            let mut profiler = OperatorProfiler::disabled();
                            let wake = OperatorWakeScope {
                                task_id: PipelineTaskId(62),
                                generation: WakeGeneration(task.0 as u64),
                            };
                            let mut ctx = OperatorFinishContext {
                                query,
                                pipeline: PipelineId::new(0),
                                operator: RuntimeOperatorId::new(0),
                                finish_task: Some(task),
                                thread: &thread,
                                memory: memory.call_scope(),
                                cancel: &query.cancellation,
                                wake: &wake,
                                profiler: &mut profiler,
                            };
                            driver.run_task(task, &mut ctx).unwrap();
                            assert!(driver.run_task(task, &mut ctx).is_err());
                        })
                    })
                    .collect::<Vec<_>>();
                for join in joins {
                    join.join().unwrap();
                }
            });
            group.driver.finish_group(&mut ctx).unwrap();
            group.task_count
        }
    };
    exec.finish(&mut ctx, global).unwrap();
    task_count
}

#[test]
fn grouping_finish_parallel_domains_keep_nulls_duplicates_and_empty_identity() {
    let query = query_context();
    let allocator = paro_common::test_utils::test_allocator();
    let mut spec = grouped_count_spec();
    spec.grouping_sets = Box::new([Box::new([0]), Box::new([]), Box::new([0]), Box::new([])]);
    let mut nullable = int_payload(&[1, 1, 2], allocator.clone());
    nullable
        .set_value(0, 2, &Value::Null(LogicalType::Integer))
        .unwrap();
    let (exec, global) = build_grouping_merge_fixture(
        &query,
        spec.clone(),
        vec![
            vec![],
            vec![nullable],
            vec![int_payload(&[1, 2], allocator)],
        ],
    );
    let SinkGlobal::HashAggregateBuild(handle) = &global else {
        unreachable!()
    };
    handle
        .handle
        .with_state_mut(|state| {
            let AggregateRuntimeState::Hash(state) = state else {
                unreachable!()
            };
            assert!(state.tables.is_empty());
            assert_eq!(state.pending_radix_merges.len(), 3);
            Ok(())
        })
        .unwrap();
    assert_eq!(
        finish_grouping_merge_fixture(&query, &exec, &global, false),
        4
    );
    let domains = grouping_merge_rows(&global, &spec);
    assert_eq!(domains[0], domains[2]);
    assert_eq!(domains[1], domains[3]);
    assert_eq!(domains[0].len(), 3);
    assert!(domains[0].contains(&vec![Value::Integer(1), Value::BigInt(3)]));
    assert!(domains[0].contains(&vec![Value::Integer(2), Value::BigInt(1)]));
    assert!(domains[0].contains(&vec![Value::Null(LogicalType::Integer), Value::BigInt(1)]));
    assert_eq!(
        domains[1],
        vec![vec![Value::Null(LogicalType::Integer), Value::BigInt(5)]]
    );

    let query = query_context();
    let (exec, global) = build_grouping_merge_fixture(&query, spec.clone(), vec![vec![], vec![]]);
    assert_eq!(
        finish_grouping_merge_fixture(&query, &exec, &global, false),
        4
    );
    let domains = grouping_merge_rows(&global, &spec);
    assert!(domains[0].is_empty());
    assert!(domains[2].is_empty());
    assert_eq!(
        domains[1],
        vec![vec![Value::Null(LogicalType::Integer), Value::BigInt(0)]]
    );
    assert_eq!(domains[1], domains[3]);
}

#[test]
fn grouping_finish_preserves_local_float_combine_order_bitwise() {
    let (sum, _) = paro_function::aggregate::distributive::sum::get_sum_function()
        .bind(&[LogicalType::Double])
        .unwrap();
    let mut spec = grouped_count_spec();
    spec.payload_types = Box::new([LogicalType::Integer, LogicalType::Double]);
    spec.grouping_sets = Box::new([Box::new([0]), Box::new([]), Box::new([])]);
    spec.aggregates = Box::new([Expression::Aggregate(
        AggregateExpression::new(
            sum,
            vec![reference(1, LogicalType::Double)],
            LogicalType::Double,
        )
        .into(),
    )]);
    spec.aggregate_inputs = Box::new([Box::new([1])]);
    spec.output_types = Box::new([LogicalType::Integer, LogicalType::Double]);
    let mut outputs = Vec::new();
    for serial in [true, false] {
        let query = query_context();
        let batches = [1e16, -1e16, 1.0]
            .into_iter()
            .map(|value| {
                let mut payload = Chunk::try_initialize(
                    &spec.payload_types,
                    1,
                    paro_common::test_utils::test_allocator(),
                )
                .unwrap();
                payload.try_set_cardinality(1).unwrap();
                payload.set_value(0, 0, &Value::Integer(1)).unwrap();
                payload.set_value(1, 0, &Value::Double(value)).unwrap();
                vec![payload]
            })
            .collect();
        let (exec, global) = build_grouping_merge_fixture(&query, spec.clone(), batches);
        assert_eq!(
            finish_grouping_merge_fixture(&query, &exec, &global, serial),
            if serial { 0 } else { 3 }
        );
        outputs.push(grouping_merge_rows(&global, &spec));
    }
    // Value::Double equality compares to_bits (common/runtime_value.rs), so
    // this comparison covers the exact binary64 state-combine result.
    assert_eq!(outputs[0], outputs[1]);
    for domain in &outputs[1] {
        assert_eq!(domain[0][1], Value::Double(1.0));
    }
}

#[test]
fn grouping_finish_distinct_and_ordered_modifiers_keep_their_finalize_owner() {
    use paro_planner::expression::{AggregateType, OrderByExpression};
    for ordered in [false, true] {
        let query = query_context();
        let (count, _) = paro_function::aggregate::distributive::count::get_count_function()
            .bind(&[LogicalType::Integer])
            .unwrap();
        let mut spec = grouped_count_spec();
        spec.grouping_sets = Box::new([Box::new([0]), Box::new([])]);
        let mut aggregate = AggregateExpression::new(
            count,
            vec![reference(0, LogicalType::Integer)],
            LogicalType::BigInt,
        );
        if ordered {
            aggregate = aggregate.with_order_bys(vec![OrderByExpression {
                expression: reference(0, LogicalType::Integer),
                ascending: true,
                nulls_first: false,
            }]);
            spec.aggregate_orders = Box::new([Box::new([0])]);
        } else {
            aggregate = aggregate.with_aggr_type(AggregateType::Distinct);
        }
        spec.aggregates = Box::new([Expression::Aggregate(aggregate.into())]);
        spec.aggregate_inputs = Box::new([Box::new([0])]);
        let batches = vec![
            vec![int_payload(
                &[1, 1, 2],
                paro_common::test_utils::test_allocator(),
            )],
            vec![int_payload(
                &[1, 2, 2],
                paro_common::test_utils::test_allocator(),
            )],
        ];
        let (exec, global) = build_grouping_merge_fixture(&query, spec.clone(), batches);
        let SinkGlobal::HashAggregateBuild(handle) = &global else {
            unreachable!()
        };
        handle
            .handle
            .with_state_mut(|state| {
                let AggregateRuntimeState::Hash(state) = state else {
                    unreachable!()
                };
                assert!(state.pending_radix_merges.is_empty());
                Ok(())
            })
            .unwrap();
        assert_eq!(
            finish_grouping_merge_fixture(&query, &exec, &global, false),
            0
        );
        let rows = grouping_merge_rows(&global, &spec);
        assert_eq!(
            rows[0],
            vec![
                vec![
                    Value::Integer(1),
                    Value::BigInt(if ordered { 3 } else { 1 })
                ],
                vec![
                    Value::Integer(2),
                    Value::BigInt(if ordered { 3 } else { 1 })
                ],
            ]
        );
        assert_eq!(
            rows[1],
            vec![vec![
                Value::Null(LogicalType::Integer),
                Value::BigInt(if ordered { 6 } else { 2 })
            ]]
        );
    }
}

#[test]
fn grouping_finish_rejects_illegal_mixed_raw_and_pending_state_and_refunds() {
    let query = query_context();
    let initial_bytes = query.memory.published_used_bytes();
    let allocator = paro_common::test_utils::test_allocator();
    let mut spec = grouped_count_spec();
    spec.grouping_sets = Box::new([Box::new([0]), Box::new([])]);
    let (exec, global) = build_grouping_merge_fixture(
        &query,
        spec.clone(),
        vec![
            vec![int_payload(&[1, 1], allocator.clone())],
            vec![int_payload(&[2], allocator.clone())],
        ],
    );
    let payload = int_payload(&[1, 3], allocator);
    let all_groups = build_groups_chunk(&payload, &group_payload_refs(&spec).unwrap()).unwrap();
    let SinkGlobal::HashAggregateBuild(handle) = &global else {
        unreachable!()
    };
    handle
        .handle
        .with_state_mut(|state| {
            let AggregateRuntimeState::Hash(state) = state else {
                unreachable!()
            };
            assert_eq!(state.pending_radix_merges.len(), 2);
            for (domain, grouping_set) in normalized_grouping_sets(&spec)?.iter().enumerate() {
                let groups = build_groups_chunk_for_set(&all_groups, grouping_set, 1)?;
                let mut spill = AggregatePayloadSpillBuffer::new(
                    query.session.buffer_pool().clone(),
                    payload.types(),
                    aggregate_spill_radix_bits(1, usize::MAX),
                    query_hash_table_memory(&query),
                )?;
                spill.append_payload(&payload, &hash_group_columns(&groups)?)?;
                state.spilled_payloads.push(spill.seal_for_grouping(domain));
            }
            Ok(())
        })
        .unwrap();
    // Raw grouping-set spill must start before accepting any input state.
    // Deliberately injecting it after completed locals is illegal, just as it
    // was before domains were deferred. Keep the owner's rejection contract.
    let thread = ThreadContext::single_threaded();
    let memory = TaskMemoryGrants::detached(paro_common::test_utils::test_allocator());
    let wake = OperatorWakeScope {
        task_id: PipelineTaskId(64),
        generation: WakeGeneration(0),
    };
    let mut profiler = OperatorProfiler::disabled();
    let mut ctx = OperatorFinishContext {
        query: &query,
        pipeline: PipelineId::new(0),
        operator: RuntimeOperatorId::new(0),
        finish_task: None,
        thread: &thread,
        memory: memory.call_scope(),
        cancel: &query.cancellation,
        wake: &wake,
        profiler: &mut profiler,
    };
    exec.prepare_finish(&mut ctx, &global).unwrap();
    assert!(matches!(
        exec.finish_work(&mut ctx, &global).unwrap(),
        FinishWork::None
    ));
    let error = exec.finish(&mut ctx, &global).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("mixed raw payload with in-memory state"),
        "{error}"
    );
    assert!(!handle.handle.is_finalized());
    drop(handle.handle.take_state().unwrap());
    assert_eq!(query.memory.published_used_bytes(), initial_bytes);
}

#[test]
fn grouping_finish_forced_external_locals_keep_raw_domain_owner() {
    let temporary = tempfile::tempdir().unwrap();
    let query = QueryRuntimeContext::new(
        TestStatementContextBuilder::minimal()
            .with_limits(paro_context::RuntimeLimits {
                max_threads: 1,
                max_memory: 64 * 1024 * 1024,
                use_temporary_directory: true,
                temporary_directory: temporary.path().to_str().unwrap().to_owned(),
                max_temp_directory_size: None,
                force_external: true,
                rowset_scan_pushdown: true,
                parallel_scheduler: false,
            })
            .build(),
        Arc::new(ParameterBindings::empty()),
        Arc::new(QueryMemoryPool::unbounded()),
        QueryOutputPort::unbounded(),
    );
    let mut spec = grouped_count_spec();
    spec.grouping_sets = Box::new([Box::new([0]), Box::new([]), Box::new([])]);
    spec.spill_policy = SpillExecutionPolicy::ForcedExternal;
    let (exec, global) = build_grouping_merge_fixture(
        &query,
        spec.clone(),
        vec![
            vec![int_payload(
                &[1, 1],
                paro_common::test_utils::test_allocator(),
            )],
            vec![int_payload(&[2], paro_common::test_utils::test_allocator())],
        ],
    );
    let SinkGlobal::HashAggregateBuild(handle) = &global else {
        unreachable!()
    };
    handle
        .handle
        .with_state_mut(|state| {
            let AggregateRuntimeState::Hash(state) = state else {
                unreachable!()
            };
            assert!(state.tables.is_empty());
            assert!(state.pending_radix_merges.is_empty());
            assert_eq!(state.spilled_payloads.len(), 6);
            Ok(())
        })
        .unwrap();
    assert_eq!(
        finish_grouping_merge_fixture(&query, &exec, &global, false),
        0
    );
    let domains = grouping_merge_rows(&global, &spec);
    assert_eq!(
        domains[0],
        vec![
            vec![Value::Integer(1), Value::BigInt(2)],
            vec![Value::Integer(2), Value::BigInt(1)],
        ]
    );
    assert_eq!(
        domains[1],
        vec![vec![Value::Null(LogicalType::Integer), Value::BigInt(3)]]
    );
    assert_eq!(domains[1], domains[2]);
}

#[test]
fn adaptive_grouping_late_growth_failure_does_not_drop_consumed_local_state() {
    let temporary = tempfile::tempdir().unwrap();
    let query = QueryRuntimeContext::new(
        TestStatementContextBuilder::minimal()
            .with_limits(paro_context::RuntimeLimits {
                max_threads: 1,
                max_memory: 64 * 1024 * 1024,
                use_temporary_directory: true,
                temporary_directory: temporary.path().to_str().unwrap().to_owned(),
                max_temp_directory_size: None,
                force_external: false,
                rowset_scan_pushdown: true,
                parallel_scheduler: false,
            })
            .build(),
        Arc::new(ParameterBindings::empty()),
        Arc::new(QueryMemoryPool::new(64 * 1024 * 1024)),
        QueryOutputPort::unbounded(),
    );
    let initial_bytes = query.memory.published_used_bytes();
    let mut spec = grouped_count_spec();
    spec.grouping_sets = Box::new([Box::new([0]), Box::new([])]);
    assert!(!hash_aggregate_preemptive_payload_spill_enabled(&query));
    assert!(hash_aggregate_payload_spill_supported(&spec));
    assert!(!hash_aggregate_state_spill_supported(
        &spec,
        &aggregate_objects(&spec).unwrap()
    ));
    let properties = PipelineProperties::default();
    let mut catalog = BreakerHandleCatalogBuilder::default();
    let id = catalog.register(
        BreakerHandleKind::Aggregate,
        RowType::new(spec.output_names.to_vec(), spec.output_types.to_vec()),
        properties.clone(),
    );
    let handles = BreakerHandleRegistry::from_catalog(&catalog.finish()).unwrap();
    let exec = HashAggregateBuildSinkExec {
        handle: HandleRef::new(id),
        spec: spec.clone(),
    };
    let mut init = PipelineInitContext {
        query: &query,
        pipeline: PipelineId::new(0),
        operator: RuntimeOperatorId::new(0),
        params: &query.params,
        handles: &handles,
        properties: &properties,
    };
    let global = exec.create_global(&mut init).unwrap();
    let mut local = exec.create_local(&mut init, &global).unwrap();
    let thread = ThreadContext::single_threaded();
    let memory = TaskMemoryGrants::detached(paro_common::test_utils::test_allocator());
    let wake = OperatorWakeScope {
        task_id: PipelineTaskId(65),
        generation: WakeGeneration(0),
    };
    let mut profiler = OperatorProfiler::disabled();
    let mut scratch = ExpressionScratchArena::default();
    let mut ctx = OperatorCallContext {
        query: &query,
        pipeline: PipelineId::new(0),
        operator: RuntimeOperatorId::new(0),
        thread: &thread,
        memory: memory.call_scope(),
        scratch: OperatorScratchScope::from_expression(&mut scratch),
        cancel: &query.cancellation,
        wake: &wake,
        profiler: &mut profiler,
    };
    let mut first = int_payload(&[-1], paro_common::test_utils::test_allocator());
    exec.consume(&mut ctx, &global, &mut local, &mut first)
        .unwrap();
    let SinkLocal::HashAggregateBuild(state) = &local else {
        unreachable!()
    };
    assert!(!state.raw_payload_spill_enabled());
    assert!(state.tables.lock().iter().all(|table| table.count() == 1));
    // The query began above the preemptive threshold. Force a later real
    // growth admission failure; Spill-class streams can still allocate because
    // the buffer pool, rather than the working-set cap, owns their budget.
    query.memory.set_capacity_bytes(1);
    let values = (0..VECTOR_SIZE as i32).collect::<Vec<_>>();
    let mut second = int_payload(&values, paro_common::test_utils::test_allocator());
    let result = exec.consume(&mut ctx, &global, &mut local, &mut second);
    if result.is_ok() {
        // This branch makes the old bug a concrete failing regression witness:
        // late raw fallback used to succeed, discard the first local state at
        // merge, then emit a grand total containing only the second batch.
        query.memory.set_capacity_bytes(64 * 1024 * 1024);
        exec.merge_local(&mut ctx, &global, &mut local).unwrap();
        drop(local);
        finish_grouping_merge_fixture(&query, &exec, &global, false);
        let domains = grouping_merge_rows(&global, &spec);
        assert_eq!(
            domains[1][0][1],
            Value::BigInt((VECTOR_SIZE + 1) as i64),
            "late raw fallback lost the previously consumed first batch"
        );
        panic!("populated local must keep the original growth admission error");
    }
    let error = result.unwrap_err();
    assert!(error.to_string().contains("memory"), "{error}");
    let SinkLocal::HashAggregateBuild(state) = &local else {
        unreachable!()
    };
    assert!(!state.raw_payload_spill_enabled());
    assert!(state.payload_spills.iter().all(Option::is_none));
    assert!(state.state_spill.lock().is_none());
    assert!(state.tables.lock().iter().all(|table| table.count() == 1));
    let SinkGlobal::HashAggregateBuild(handle) = &global else {
        unreachable!()
    };
    assert!(!handle.handle.is_finalized());
    drop(local);
    drop(handle.handle.take_state().unwrap());
    assert_eq!(query.memory.published_used_bytes(), initial_bytes);
}

#[test]
fn grouping_finish_cancel_and_merge_allocation_failure_never_publish_and_refund() {
    for cancel in [true, false] {
        let mut query = query_context();
        let token = tokio_util::sync::CancellationToken::new();
        query.cancellation = paro_context::StatementCancellation::new(token.clone(), None);
        let initial_bytes = query.memory.published_used_bytes();
        let mut spec = grouped_count_spec();
        spec.grouping_sets = Box::new([Box::new([0]), Box::new([]), Box::new([0])]);
        let batches = (0..3)
            .map(|local| {
                let values = (0..VECTOR_SIZE as i32)
                    .map(|key| key + local * VECTOR_SIZE as i32)
                    .collect::<Vec<_>>();
                vec![int_payload(
                    &values,
                    paro_common::test_utils::test_allocator(),
                )]
            })
            .collect();
        let (exec, global) = build_grouping_merge_fixture(&query, spec, batches);
        assert!(query.memory.published_used_bytes() > initial_bytes);
        let thread = ThreadContext::single_threaded();
        let memory = TaskMemoryGrants::detached(paro_common::test_utils::test_allocator());
        let wake = OperatorWakeScope {
            task_id: PipelineTaskId(63),
            generation: WakeGeneration(0),
        };
        let mut profiler = OperatorProfiler::disabled();
        let mut ctx = OperatorFinishContext {
            query: &query,
            pipeline: PipelineId::new(0),
            operator: RuntimeOperatorId::new(0),
            finish_task: None,
            thread: &thread,
            memory: memory.call_scope(),
            cancel: &query.cancellation,
            wake: &wake,
            profiler: &mut profiler,
        };
        exec.prepare_finish(&mut ctx, &global).unwrap();
        let FinishWork::Parallel(group) = exec.finish_work(&mut ctx, &global).unwrap() else {
            panic!("expected independent domain tasks")
        };
        // A completed empty-set task is retained privately until every domain
        // succeeds. The next keyed domain must grow for disjoint source keys.
        group
            .driver
            .run_task(crate::runtime::context::FinishTaskId(1), &mut ctx)
            .unwrap();
        if cancel {
            token.cancel();
        } else {
            query.memory.set_capacity_bytes(1);
        }
        let error = group
            .driver
            .run_task(crate::runtime::context::FinishTaskId(0), &mut ctx)
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains(if cancel { "cancel" } else { "memory" }),
            "{error}"
        );
        assert!(group.driver.finish_group(&mut ctx).is_err());
        let SinkGlobal::HashAggregateBuild(handle) = &global else {
            unreachable!()
        };
        assert!(!handle.handle.is_finalized());
        handle
            .handle
            .with_state_mut(|state| {
                let AggregateRuntimeState::Hash(state) = state else {
                    unreachable!()
                };
                assert!(state.tables.is_empty());
                assert!(state.pending_radix_merges.is_empty());
                Ok(())
            })
            .unwrap();
        drop(group);
        assert_eq!(query.memory.published_used_bytes(), initial_bytes);
    }
}

fn reference(index: usize, ty: LogicalType) -> Expression {
    Expression::Reference(ReferenceExpression::new(index, ty).into())
}

fn grouped_count_spec() -> AggregateSpec {
    AggregateSpec {
        grouping_key_count: 1,
        initial_lookup_hash_key_count: 1,
        state_output_projection: Box::new([]),
        estimated_input_rows: None,
        projection_exprs: Box::new([]),
        payload_types: Box::new([LogicalType::Integer]),
        groups: Box::new([reference(0, LogicalType::Integer)]),
        group_key_encodings: Box::new([GroupKeyEncoding::Identity]),
        grouping_sets: Box::new([]),
        aggregates: Box::new([Expression::Aggregate(
            AggregateExpression::new(get_count_star_function(), vec![], LogicalType::BigInt).into(),
        )]),
        grouping_functions: Box::new([]),
        aggregate_inputs: Box::new([Box::new([])]),
        aggregate_filters: Box::new([None]),
        aggregate_orders: Box::new([Box::new([])]),
        post_reduction: None,
        having_filter: Box::new([]),
        spill_policy: crate::physical::specs::SpillExecutionPolicy::Adaptive,
        perfect_hash: None,
        output_names: Box::new(["k".to_string(), "count".to_string()]),
        output_types: Box::new([LogicalType::Integer, LogicalType::BigInt]),
    }
}

fn grouped_string_agg_spec() -> AggregateSpec {
    let (string_agg, _) = get_string_agg_function()
        .bind(&[LogicalType::Varchar])
        .expect("bind string_agg");
    AggregateSpec {
        grouping_key_count: 1,
        initial_lookup_hash_key_count: 1,
        state_output_projection: Box::new([]),
        estimated_input_rows: None,
        projection_exprs: Box::new([]),
        payload_types: Box::new([LogicalType::Integer, LogicalType::Varchar]),
        groups: Box::new([reference(0, LogicalType::Integer)]),
        group_key_encodings: Box::new([GroupKeyEncoding::Identity]),
        grouping_sets: Box::new([]),
        aggregates: Box::new([Expression::Aggregate(
            AggregateExpression::new(
                string_agg,
                vec![reference(1, LogicalType::Varchar)],
                LogicalType::Varchar,
            )
            .into(),
        )]),
        grouping_functions: Box::new([]),
        aggregate_inputs: Box::new([Box::new([1])]),
        aggregate_filters: Box::new([None]),
        aggregate_orders: Box::new([Box::new([])]),
        post_reduction: None,
        having_filter: Box::new([]),
        spill_policy: crate::physical::specs::SpillExecutionPolicy::Adaptive,
        perfect_hash: None,
        output_names: Box::new(["k".to_string(), "items".to_string()]),
        output_types: Box::new([LogicalType::Integer, LogicalType::Varchar]),
    }
}

fn int_payload(values: &[i32], allocator: Arc<dyn paro_common::allocator::Allocator>) -> Chunk {
    let mut payload =
        Chunk::try_initialize(&[LogicalType::Integer], values.len(), allocator).expect("payload");
    payload.set_cardinality(values.len());
    for (row_idx, value) in values.iter().enumerate() {
        payload
            .column_mut(0)
            .expect("payload column")
            .set_value(row_idx, &Value::Integer(*value));
    }
    payload
}

fn pair_payload(
    values: &[(i32, i32)],
    allocator: Arc<dyn paro_common::allocator::Allocator>,
) -> Chunk {
    let mut payload = Chunk::try_initialize(
        &[LogicalType::Integer, LogicalType::Integer],
        values.len(),
        allocator,
    )
    .expect("payload");
    payload.set_cardinality(values.len());
    for (row_idx, (left, right)) in values.iter().enumerate() {
        payload
            .column_mut(0)
            .expect("left group")
            .set_value(row_idx, &Value::Integer(*left));
        payload
            .column_mut(1)
            .expect("right group")
            .set_value(row_idx, &Value::Integer(*right));
    }
    payload
}

fn string_agg_payload(
    rows: &[(i32, &str)],
    allocator: Arc<dyn paro_common::allocator::Allocator>,
) -> Chunk {
    let mut payload = Chunk::try_initialize(
        &[LogicalType::Integer, LogicalType::Varchar],
        rows.len(),
        allocator,
    )
    .expect("payload");
    payload.set_cardinality(rows.len());
    for (row_idx, (key, value)) in rows.iter().enumerate() {
        payload
            .column_mut(0)
            .expect("key column")
            .set_value(row_idx, &Value::Integer(*key));
        payload
            .column_mut(1)
            .expect("value column")
            .set_value(row_idx, &Value::Varchar((*value).to_string()));
    }
    payload
}

#[test]
fn hash_prefix_collisions_still_compare_the_complete_group_key() {
    let allocator = paro_common::test_utils::test_allocator();
    let mut spec = grouped_count_spec();
    spec.grouping_key_count = 2;
    spec.initial_lookup_hash_key_count = 1;
    spec.payload_types = Box::new([LogicalType::Integer, LogicalType::Integer]);
    spec.groups = Box::new([
        reference(0, LogicalType::Integer),
        reference(1, LogicalType::Integer),
    ]);
    spec.group_key_encodings = Box::new([GroupKeyEncoding::Identity, GroupKeyEncoding::Identity]);
    spec.output_names = Box::new(["left".into(), "right".into(), "count".into()]);
    spec.output_types = Box::new([
        LogicalType::Integer,
        LogicalType::Integer,
        LogicalType::BigInt,
    ]);

    let aggregate_objects = aggregate_objects(&spec).expect("aggregate objects");
    let group_refs = group_payload_refs(&spec).expect("group refs");
    let grouping_sets = normalized_grouping_sets(&spec)
        .expect("grouping sets")
        .into_iter()
        .map(Vec::into_boxed_slice)
        .collect::<Vec<_>>();
    let table_memory =
        MemoryAccountingContext::detached(MemoryTag::HashTable, MemoryAccountingClass::Revocable);
    let mut tables =
        create_hash_aggregate_tables(&spec, allocator.clone(), table_memory, 1).expect("tables");
    let payload = pair_payload(&[(7, 10), (7, 20), (7, 10)], allocator);
    let groups = build_groups_chunk(&payload, &group_refs).expect("groups");
    let mut addresses =
        paro_common::test_utils::test_vector_with_capacity(LogicalType::BigInt, VECTOR_SIZE);
    let mut new_groups = paro_common::test_utils::test_selection_with_capacity(VECTOR_SIZE);

    update_hash_aggregate_tables(
        &spec,
        &aggregate_objects,
        &payload,
        &groups,
        &grouping_sets,
        &mut tables,
        &mut addresses,
        &mut new_groups,
    )
    .expect("build aggregate table");

    assert_eq!(tables[0].count(), 2);
}

fn query_context() -> QueryRuntimeContext {
    QueryRuntimeContext::new(
        TestStatementContextBuilder::minimal().build(),
        Arc::new(ParameterBindings::empty()),
        Arc::new(QueryMemoryPool::unbounded()),
        QueryOutputPort::unbounded(),
    )
}

#[test]
fn grouping_lookup_rejects_group_payload_cardinality_mismatch_before_updates() {
    let allocator = paro_common::test_utils::test_allocator();
    let spec = grouped_count_spec();
    let objects = aggregate_objects(&spec).expect("objects");
    let mut tables = create_hash_aggregate_tables(
        &spec,
        allocator.clone(),
        MemoryAccountingContext::detached(MemoryTag::HashTable, MemoryAccountingClass::Revocable),
        1,
    )
    .expect("tables");
    let payload = int_payload(&[1, 2, 3], allocator.clone());
    let wrong_groups = int_payload(&[1, 2], allocator);
    let mut addresses = paro_common::test_utils::test_vector_with_capacity(LogicalType::BigInt, 4);
    let mut new_groups = paro_common::test_utils::test_selection_with_capacity(4);
    let error = update_hash_aggregate_tables(
        &spec,
        &objects,
        &payload,
        &wrong_groups,
        &[Box::new([0])],
        &mut tables,
        &mut addresses,
        &mut new_groups,
    )
    .expect_err("mismatched chunks must fail before insertion");
    assert!(error.to_string().contains("cardinality mismatch"));
    assert_eq!(tables[0].count(), 0);
}

#[test]
fn empty_grouping_domain_broadcasts_lookup_and_preserves_null_and_duplicate_domains() {
    let allocator = paro_common::test_utils::test_allocator();
    let mut spec = grouped_count_spec();
    spec.grouping_sets = Box::new([Box::new([0]), Box::new([]), Box::new([])]);
    let objects = aggregate_objects(&spec).expect("objects");
    let group_refs = group_payload_refs(&spec).expect("references");
    let grouping_sets = normalized_grouping_sets(&spec)
        .expect("grouping sets")
        .into_iter()
        .map(Vec::into_boxed_slice)
        .collect::<Vec<_>>();
    let memory =
        MemoryAccountingContext::detached(MemoryTag::HashTable, MemoryAccountingClass::Revocable);
    let mut global =
        create_hash_aggregate_tables(&spec, allocator.clone(), memory.clone(), 4).expect("global");
    let mut addresses = paro_common::test_utils::test_vector_with_capacity(LogicalType::BigInt, 8);
    let mut new_groups = paro_common::test_utils::test_selection_with_capacity(8);
    let mut scratch = GroupHashScratch::try_new(8, allocator.clone()).expect("scratch");

    // Two local tables, each with two batches, exercise address reuse and
    // merge. Real NULL groups must remain separate from both grand totals.
    for _ in 0..2 {
        let mut local = create_hash_aggregate_tables(&spec, allocator.clone(), memory.clone(), 4)
            .expect("local");
        for _ in 0..2 {
            let mut payload = int_payload(&[7, 7, 9], allocator.clone());
            payload
                .column_mut(0)
                .unwrap()
                .set_value(2, &Value::Null(LogicalType::Integer));
            let groups = build_groups_chunk(&payload, &group_refs).expect("groups");
            let mut plans = local
                .iter_mut()
                .zip(&grouping_sets)
                .map(|(table, grouping_set)| {
                    let lookup_groups = build_groups_chunk_for_set_lookup(
                        &groups,
                        grouping_set,
                        spec.grouping_key_count,
                        table,
                    )
                    .expect("lookup groups");
                    assert_eq!(
                        lookup_groups.size(),
                        if grouping_set.is_empty() { 1 } else { 3 }
                    );
                    table
                        .growth_plan(&lookup_groups, &mut scratch)
                        .expect("growth")
                        .0
                })
                .collect::<Vec<_>>();
            update_hash_aggregate_tables_with_growth_plans(
                &spec,
                &objects,
                &payload,
                &groups,
                &grouping_sets,
                &mut local,
                &mut scratch,
                &mut plans,
                &mut addresses,
                &mut new_groups,
            )
            .expect("update");
            assert_eq!(addresses.len(), payload.size());
            assert!(addresses.as_slice::<*mut u8>()[..3]
                .windows(2)
                .all(|pair| pair[0] == pair[1]));
            assert_eq!(local[0].count(), 2);
            assert_eq!(local[1].count(), 1);
            assert_eq!(local[2].count(), 1);
        }
        merge_local_tables(&mut global, &mut local).expect("merge locals");
    }
    let mut actual = Vec::new();
    let mut output = Chunk::try_initialize(&spec.output_types, 8, allocator).expect("output");
    for (domain, table) in global.iter_mut().enumerate() {
        let mut position = AggregateHTScanPosition::default();
        while table.scan(&mut position, &mut output).expect("scan") {
            actual.extend((0..output.size()).map(|row| {
                (
                    domain,
                    output.column(0).unwrap().get_value(row),
                    output.column(1).unwrap().get_i64(row).unwrap(),
                )
            }));
        }
    }
    actual.sort_by_key(|row| (row.0, row.1.to_string()));
    let mut expected = vec![
        (0, Value::Integer(7), 8),
        (0, Value::Null(LogicalType::Integer), 4),
        (1, Value::Null(LogicalType::Integer), 12),
        (2, Value::Null(LogicalType::Integer), 12),
    ];
    expected.sort_by_key(|row| (row.0, row.1.to_string()));
    assert_eq!(actual, expected);
}

#[test]
fn empty_grouping_domain_applies_filter_to_full_payload_after_single_lookup() {
    let allocator = paro_common::test_utils::test_allocator();
    let mut spec = grouped_count_spec();
    spec.payload_types = Box::new([LogicalType::Integer, LogicalType::Boolean]);
    spec.grouping_sets = Box::new([Box::new([])]);
    spec.aggregates = Box::new([Expression::Aggregate(
        AggregateExpression::new(get_count_star_function(), vec![], LogicalType::BigInt)
            .with_filter(Some(reference(1, LogicalType::Boolean)))
            .into(),
    )]);
    spec.aggregate_filters = Box::new([Some(1)]);
    let objects = aggregate_objects(&spec).expect("objects");
    let mut tables = create_hash_aggregate_tables(
        &spec,
        allocator.clone(),
        MemoryAccountingContext::detached(MemoryTag::HashTable, MemoryAccountingClass::Revocable),
        4,
    )
    .expect("tables");
    let mut payload =
        Chunk::try_initialize(&spec.payload_types, 4, allocator.clone()).expect("payload");
    payload.set_cardinality(4);
    for row in 0..4 {
        payload
            .column_mut(0)
            .unwrap()
            .set_value(row, &Value::Integer(row as i32));
        payload
            .column_mut(1)
            .unwrap()
            .set_value(row, &Value::Boolean(row > 1));
    }
    payload
        .column_mut(1)
        .unwrap()
        .set_value(0, &Value::Null(LogicalType::Boolean));
    let groups = build_groups_chunk(&payload, &[0]).expect("groups");
    let mut addresses = paro_common::test_utils::test_vector_with_capacity(LogicalType::BigInt, 4);
    let mut new_groups = paro_common::test_utils::test_selection_with_capacity(4);
    update_hash_aggregate_tables(
        &spec,
        &objects,
        &payload,
        &groups,
        &[Box::new([])],
        &mut tables,
        &mut addresses,
        &mut new_groups,
    )
    .expect("update filtered grand total");
    let mut output = Chunk::try_initialize(&spec.output_types, 4, allocator).expect("output");
    assert!(tables[0]
        .scan(&mut AggregateHTScanPosition::default(), &mut output)
        .expect("scan"));
    assert_eq!(output.size(), 1);
    assert_eq!(
        output.column(0).unwrap().get_value(0),
        Value::Null(LogicalType::Integer)
    );
    assert_eq!(output.column(1).unwrap().get_value(0), Value::BigInt(2));
}

#[test]
fn grouping_set_spill_partitions_each_domain_by_its_own_keys() {
    let allocator = paro_common::test_utils::test_allocator();
    let query = query_context();
    let thread = ThreadContext::single_threaded();
    let memory = TaskMemoryGrants::detached(allocator.clone());
    let wake = OperatorWakeScope {
        task_id: PipelineTaskId(41),
        generation: WakeGeneration(0),
    };
    let mut profiler = OperatorProfiler::disabled();
    let mut ctx = OperatorFinishContext {
        query: &query,
        pipeline: PipelineId::new(0),
        operator: RuntimeOperatorId::new(0),
        finish_task: None,
        thread: &thread,
        memory: memory.call_scope(),
        cancel: &query.cancellation,
        wake: &wake,
        profiler: &mut profiler,
    };
    let mut spec = grouped_count_spec();
    spec.grouping_key_count = 2;
    spec.payload_types = Box::new([LogicalType::Integer, LogicalType::Integer]);
    spec.groups = Box::new([
        reference(0, LogicalType::Integer),
        reference(1, LogicalType::Integer),
    ]);
    spec.group_key_encodings = Box::new([GroupKeyEncoding::Identity, GroupKeyEncoding::Identity]);
    spec.grouping_sets = Box::new([Box::new([0]), Box::new([1]), Box::new([])]);
    spec.output_names = Box::new(["a".into(), "b".into(), "count".into()]);
    spec.output_types = Box::new([
        LogicalType::Integer,
        LogicalType::Integer,
        LogicalType::BigInt,
    ]);

    let payload = pair_payload(&[(1, 10), (1, 20), (2, 10)], allocator.clone());
    let group_refs = group_payload_refs(&spec).expect("group refs");
    let all_groups = build_groups_chunk(&payload, &group_refs).expect("all groups");
    let grouping_sets = normalized_grouping_sets(&spec)
        .expect("grouping sets")
        .into_iter()
        .map(Vec::into_boxed_slice)
        .collect::<Vec<_>>();
    let table_memory =
        MemoryAccountingContext::detached(MemoryTag::HashTable, MemoryAccountingClass::Revocable);
    let mut spilled_payloads = Vec::new();
    for (grouping_idx, grouping_set) in grouping_sets.iter().enumerate() {
        let groups =
            build_groups_chunk_for_set(&all_groups, grouping_set, 2).expect("domain groups");
        let hashes = hash_group_columns(&groups).expect("domain hashes");
        let mut spill = AggregatePayloadSpillBuffer::new(
            query.session.buffer_pool().clone(),
            payload.types(),
            aggregate_spill_radix_bits(1, usize::MAX),
            table_memory.clone(),
        )
        .expect("payload spill");
        spill.append_payload(&payload, &hashes).expect("spill rows");
        spilled_payloads.push(spill.seal_for_grouping(grouping_idx));
    }

    let aggregate_objects = aggregate_objects(&spec).expect("aggregate objects");
    let mut state = HashAggregateRuntimeState {
        tables: Vec::new(),
        pending_radix_merges: Vec::new(),
        distinct: Default::default(),
        spilled_payloads: Vec::new(),
        spilled_states: Vec::new(),
        spilled_outputs: None,
        ordered_collectors: Vec::new(),
    };
    spill_payload_partitions_to_outputs(
        &mut ctx,
        &spec,
        &aggregate_objects,
        &group_refs,
        &grouping_sets,
        &mut state,
        &spilled_payloads,
        &[],
        None,
    )
    .expect("grouping-set spill replay");

    let mut actual = Vec::new();
    for (grouping_idx, output) in state
        .spilled_outputs
        .take()
        .expect("spilled outputs")
        .into_iter()
        .enumerate()
    {
        let mut reader = output.expect("domain output").into_reader();
        let mut chunk =
            Chunk::try_initialize(&spec.output_types, 8, allocator.clone()).expect("output chunk");
        while reader.read_next(&mut chunk).expect("read domain output") > 0 {
            actual.extend((0..chunk.size()).map(|row| {
                (
                    grouping_idx,
                    chunk.column(0).unwrap().get_value(row),
                    chunk.column(1).unwrap().get_value(row),
                    chunk.column(2).unwrap().get_i64(row).unwrap(),
                )
            }));
        }
    }
    actual.sort_by_key(|row| (row.0, row.1.to_string(), row.2.to_string()));
    assert_eq!(
        actual,
        vec![
            (0, Value::Integer(1), Value::Null(LogicalType::Integer), 2),
            (0, Value::Integer(2), Value::Null(LogicalType::Integer), 1),
            (1, Value::Null(LogicalType::Integer), Value::Integer(10), 2),
            (1, Value::Null(LogicalType::Integer), Value::Integer(20), 1),
            (
                2,
                Value::Null(LogicalType::Integer),
                Value::Null(LogicalType::Integer),
                3,
            ),
        ]
    );
}

#[test]
fn empty_grouping_set_owns_an_identity_domain_without_input() {
    let query = query_context();
    let mut spec = grouped_count_spec();
    spec.grouping_sets = Box::new([Box::new([0]), Box::new([])]);
    let mut state = HashAggregateRuntimeState {
        tables: Vec::new(),
        pending_radix_merges: Vec::new(),
        distinct: Default::default(),
        spilled_payloads: Vec::new(),
        spilled_states: Vec::new(),
        spilled_outputs: None,
        ordered_collectors: Vec::new(),
    };

    ensure_grouping_domains(&query, &spec, &mut state).expect("initialize grouping domains");

    assert_eq!(state.tables.len(), 2);
    assert_eq!(state.tables[0].count(), 0);
    assert_eq!(state.tables[1].count(), 1);
}

#[test]
fn external_grouping_domains_keep_empty_set_identity_without_payload() {
    let allocator = paro_common::test_utils::test_allocator();
    let query = query_context();
    let thread = ThreadContext::single_threaded();
    let memory = TaskMemoryGrants::detached(allocator.clone());
    let wake = OperatorWakeScope {
        task_id: PipelineTaskId(44),
        generation: WakeGeneration(0),
    };
    let mut profiler = OperatorProfiler::disabled();
    let mut ctx = OperatorFinishContext {
        query: &query,
        pipeline: PipelineId::new(0),
        operator: RuntimeOperatorId::new(0),
        finish_task: None,
        thread: &thread,
        memory: memory.call_scope(),
        cancel: &query.cancellation,
        wake: &wake,
        profiler: &mut profiler,
    };
    let mut spec = grouped_count_spec();
    spec.grouping_sets = Box::new([Box::new([0]), Box::new([])]);
    let aggregate_objects = aggregate_objects(&spec).expect("aggregate objects");
    let group_refs = group_payload_refs(&spec).expect("group refs");
    let grouping_sets = normalized_grouping_sets(&spec)
        .expect("grouping sets")
        .into_iter()
        .map(Vec::into_boxed_slice)
        .collect::<Vec<_>>();
    let mut state = HashAggregateRuntimeState {
        tables: Vec::new(),
        pending_radix_merges: Vec::new(),
        distinct: Default::default(),
        spilled_payloads: Vec::new(),
        spilled_states: Vec::new(),
        spilled_outputs: None,
        ordered_collectors: Vec::new(),
    };

    spill_payload_partitions_to_outputs(
        &mut ctx,
        &spec,
        &aggregate_objects,
        &group_refs,
        &grouping_sets,
        &mut state,
        &[],
        &[],
        None,
    )
    .expect("external empty grouping domain");

    let mut outputs = state.spilled_outputs.take().expect("spilled outputs");
    assert!(outputs[0].is_none());
    let mut reader = outputs[1].take().expect("identity output").into_reader();
    let mut output = Chunk::try_initialize(&spec.output_types, 1, allocator).expect("output chunk");
    assert_eq!(reader.read_next(&mut output).expect("read identity"), 1);
    assert_eq!(
        output.column(0).unwrap().get_value(0),
        Value::Null(LogicalType::Integer)
    );
    assert_eq!(output.column(1).unwrap().get_i64(0).unwrap(), 0);
}

#[test]
fn mixed_spilled_payload_and_global_state_writes_bounded_output() {
    let allocator = paro_common::test_utils::test_allocator();
    let query = query_context();
    let thread = ThreadContext::single_threaded();
    let memory = TaskMemoryGrants::detached(allocator.clone());
    let wake = OperatorWakeScope {
        task_id: PipelineTaskId(42),
        generation: WakeGeneration(0),
    };
    let mut profiler = OperatorProfiler::disabled();
    let mut ctx = OperatorFinishContext {
        query: &query,
        pipeline: PipelineId::new(0),
        operator: RuntimeOperatorId::new(0),
        finish_task: None,
        thread: &thread,
        memory: memory.call_scope(),
        cancel: &query.cancellation,
        wake: &wake,
        profiler: &mut profiler,
    };

    let spec = grouped_count_spec();
    let aggregate_objects = aggregate_objects(&spec).expect("aggregate objects");
    let group_refs = group_payload_refs(&spec).expect("group refs");
    let grouping_sets = normalized_grouping_sets(&spec)
        .expect("grouping sets")
        .into_iter()
        .map(Vec::into_boxed_slice)
        .collect::<Vec<_>>();
    let table_memory =
        MemoryAccountingContext::detached(MemoryTag::HashTable, MemoryAccountingClass::Revocable);
    let mut tables =
        create_hash_aggregate_tables(&spec, allocator.clone(), table_memory.clone(), 1)
            .expect("tables");
    let global_payload = int_payload(&[1, 2, 1], allocator.clone());
    let mut addresses =
        paro_common::test_utils::test_vector_with_capacity(LogicalType::BigInt, VECTOR_SIZE);
    let mut new_groups = paro_common::test_utils::test_selection_with_capacity(VECTOR_SIZE);
    let global_groups = build_groups_chunk(&global_payload, &group_refs).expect("groups");
    update_hash_aggregate_tables(
        &spec,
        &aggregate_objects,
        &global_payload,
        &global_groups,
        &grouping_sets,
        &mut tables,
        &mut addresses,
        &mut new_groups,
    )
    .expect("build global table");

    let spilled_payload = int_payload(&[1, 3, 2], allocator.clone());
    let groups = build_groups_chunk(&spilled_payload, &group_refs).expect("groups");
    let hashes = hash_group_columns(&groups).expect("hashes");
    let mut payload_spill = AggregatePayloadSpillBuffer::new(
        query.session.buffer_pool().clone(),
        spilled_payload.types(),
        aggregate_spill_radix_bits(1, usize::MAX),
        table_memory,
    )
    .expect("payload spill");
    payload_spill
        .append_payload(&spilled_payload, &hashes)
        .expect("append payload spill");
    let spilled_payloads = vec![payload_spill.seal()];

    let mut state = HashAggregateRuntimeState {
        tables,
        pending_radix_merges: Vec::new(),
        distinct: Default::default(),
        spilled_payloads: Vec::new(),
        spilled_states: Vec::new(),
        spilled_outputs: None,
        ordered_collectors: Vec::new(),
    };
    let spilled_bytes = spill_payload_partitions_to_outputs(
        &mut ctx,
        &spec,
        &aggregate_objects,
        &group_refs,
        &grouping_sets,
        &mut state,
        &spilled_payloads,
        &[],
        None,
    )
    .expect("spill mixed replay output");
    assert!(spilled_bytes > 0);
    assert!(state.tables.is_empty());

    let outputs = state.spilled_outputs.take().expect("spilled outputs");
    let mut reader = outputs
        .into_iter()
        .flatten()
        .next()
        .expect("first spilled output")
        .into_reader();
    let mut output =
        Chunk::try_initialize(&[LogicalType::Integer, LogicalType::BigInt], 8, allocator)
            .expect("output chunk");
    let mut actual = Vec::new();
    loop {
        let count = reader.read_next(&mut output).expect("read output");
        if count == 0 {
            break;
        }
        actual.extend((0..output.size()).map(|row| {
            (
                output.column(0).unwrap().get_i32(row).unwrap(),
                output.column(1).unwrap().get_i64(row).unwrap(),
            )
        }));
    }
    actual.sort_unstable();
    assert_eq!(actual, vec![(1, 3), (2, 2), (3, 1)]);
}

#[test]
fn mixed_spilled_payload_and_serialized_string_state_writes_bounded_output() {
    let allocator = paro_common::test_utils::test_allocator();
    let query = query_context();
    let thread = ThreadContext::single_threaded();
    let memory = TaskMemoryGrants::detached(allocator.clone());
    let wake = OperatorWakeScope {
        task_id: PipelineTaskId(43),
        generation: WakeGeneration(0),
    };
    let mut profiler = OperatorProfiler::disabled();
    let mut ctx = OperatorFinishContext {
        query: &query,
        pipeline: PipelineId::new(0),
        operator: RuntimeOperatorId::new(0),
        finish_task: None,
        thread: &thread,
        memory: memory.call_scope(),
        cancel: &query.cancellation,
        wake: &wake,
        profiler: &mut profiler,
    };

    let spec = grouped_string_agg_spec();
    let aggregate_objects = aggregate_objects(&spec).expect("aggregate objects");
    assert!(hash_aggregate_state_spill_supported(
        &spec,
        &aggregate_objects
    ));
    assert_eq!(
        hash_aggregate_state_spill_encoding(&aggregate_objects),
        AggregateStateEncoding::FunctionSerialized
    );

    let group_refs = group_payload_refs(&spec).expect("group refs");
    let grouping_sets = normalized_grouping_sets(&spec)
        .expect("grouping sets")
        .into_iter()
        .map(Vec::into_boxed_slice)
        .collect::<Vec<_>>();
    let table_memory =
        MemoryAccountingContext::detached(MemoryTag::HashTable, MemoryAccountingClass::Revocable);
    let mut tables =
        create_hash_aggregate_tables(&spec, allocator.clone(), table_memory.clone(), 1)
            .expect("tables");
    let global_payload =
        string_agg_payload(&[(1, "alpha"), (2, "solo"), (1, "beta")], allocator.clone());
    let mut addresses =
        paro_common::test_utils::test_vector_with_capacity(LogicalType::BigInt, VECTOR_SIZE);
    let mut new_groups = paro_common::test_utils::test_selection_with_capacity(VECTOR_SIZE);
    let global_groups = build_groups_chunk(&global_payload, &group_refs).expect("groups");
    update_hash_aggregate_tables(
        &spec,
        &aggregate_objects,
        &global_payload,
        &global_groups,
        &grouping_sets,
        &mut tables,
        &mut addresses,
        &mut new_groups,
    )
    .expect("build global table");

    let spilled_payload = string_agg_payload(&[(1, "gamma"), (3, "fresh")], allocator.clone());
    let groups = build_groups_chunk(&spilled_payload, &group_refs).expect("groups");
    let hashes = hash_group_columns(&groups).expect("hashes");
    let mut payload_spill = AggregatePayloadSpillBuffer::new(
        query.session.buffer_pool().clone(),
        spilled_payload.types(),
        aggregate_spill_radix_bits(1, usize::MAX),
        table_memory,
    )
    .expect("payload spill");
    payload_spill
        .append_payload(&spilled_payload, &hashes)
        .expect("append payload spill");
    let spilled_payloads = vec![payload_spill.seal()];

    let mut state = HashAggregateRuntimeState {
        tables,
        pending_radix_merges: Vec::new(),
        distinct: Default::default(),
        spilled_payloads: Vec::new(),
        spilled_states: Vec::new(),
        spilled_outputs: None,
        ordered_collectors: Vec::new(),
    };
    let spilled_bytes = spill_payload_partitions_to_outputs(
        &mut ctx,
        &spec,
        &aggregate_objects,
        &group_refs,
        &grouping_sets,
        &mut state,
        &spilled_payloads,
        &[],
        None,
    )
    .expect("spill mixed replay output");
    assert!(spilled_bytes > 0);
    assert!(state.tables.is_empty());

    let outputs = state.spilled_outputs.take().expect("spilled outputs");
    let mut reader = outputs
        .into_iter()
        .flatten()
        .next()
        .expect("first spilled output")
        .into_reader();
    let mut output =
        Chunk::try_initialize(&[LogicalType::Integer, LogicalType::Varchar], 8, allocator)
            .expect("output chunk");
    let mut actual = Vec::new();
    loop {
        let count = reader.read_next(&mut output).expect("read output");
        if count == 0 {
            break;
        }
        actual.extend((0..output.size()).map(|row| {
            (
                output.column(0).unwrap().get_i32(row).unwrap(),
                output
                    .column(1)
                    .unwrap()
                    .get_string(row)
                    .unwrap()
                    .to_string(),
            )
        }));
    }
    actual.sort_unstable_by_key(|(key, _)| *key);
    assert_eq!(
        actual,
        vec![
            (1, "alpha,beta,gamma".to_string()),
            (2, "solo".to_string()),
            (3, "fresh".to_string()),
        ]
    );
}
