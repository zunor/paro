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
use crate::physical::specs::GroupKeyEncoding;
use crate::pipeline::graph::PipelineId;
use crate::runtime::parameter::ParameterBindings;
use crate::runtime::scratch::TaskMemoryGrants;
use crate::runtime::{
    OperatorWakeScope, PipelineTaskId, QueryOutputPort, QueryRuntimeContext, RuntimeOperatorId,
    WakeGeneration,
};
use crate::thread_context::ThreadContext;

fn reference(index: usize, ty: LogicalType) -> Expression {
    Expression::Reference(ReferenceExpression::new(index, ty))
}

fn grouped_count_spec() -> AggregateSpec {
    AggregateSpec {
        grouping_key_count: 1,
        state_output_projection: Box::new([]),
        estimated_input_rows: None,
        projection_exprs: Box::new([]),
        payload_types: Box::new([LogicalType::Integer]),
        groups: Box::new([reference(0, LogicalType::Integer)]),
        group_key_encodings: Box::new([GroupKeyEncoding::Identity]),
        grouping_sets: Box::new([]),
        aggregates: Box::new([Expression::Aggregate(AggregateExpression::new(
            get_count_star_function(),
            vec![],
            LogicalType::BigInt,
        ))]),
        grouping_functions: Box::new([]),
        aggregate_inputs: Box::new([Box::new([])]),
        aggregate_filters: Box::new([None]),
        aggregate_orders: Box::new([Box::new([])]),
        post_reduction: None,
        having_filter: Box::new([]),
        spill_policy: crate::physical::specs::SpillExecutionPolicy::Allowed,
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
        state_output_projection: Box::new([]),
        estimated_input_rows: None,
        projection_exprs: Box::new([]),
        payload_types: Box::new([LogicalType::Integer, LogicalType::Varchar]),
        groups: Box::new([reference(0, LogicalType::Integer)]),
        group_key_encodings: Box::new([GroupKeyEncoding::Identity]),
        grouping_sets: Box::new([]),
        aggregates: Box::new([Expression::Aggregate(AggregateExpression::new(
            string_agg,
            vec![reference(1, LogicalType::Varchar)],
            LogicalType::Varchar,
        ))]),
        grouping_functions: Box::new([]),
        aggregate_inputs: Box::new([Box::new([1])]),
        aggregate_filters: Box::new([None]),
        aggregate_orders: Box::new([Box::new([])]),
        post_reduction: None,
        having_filter: Box::new([]),
        spill_policy: crate::physical::specs::SpillExecutionPolicy::Allowed,
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

fn query_context() -> QueryRuntimeContext {
    QueryRuntimeContext::new(
        TestStatementContextBuilder::minimal().build(),
        Arc::new(ParameterBindings::empty()),
        Arc::new(QueryMemoryPool::unbounded()),
        QueryOutputPort::unbounded(),
    )
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
            aggregate_spill_radix_bits(1),
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
        aggregate_spill_radix_bits(1),
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
        aggregate_spill_radix_bits(1),
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
