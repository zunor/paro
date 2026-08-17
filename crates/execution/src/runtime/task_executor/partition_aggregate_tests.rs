// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::breaker_tests::{run_two_stage_breaker, run_two_stage_breaker_with_profile};
use super::*;

fn partition_aggregate_window_graph(
    key_type: LogicalType,
    input_rows: Vec<Vec<Expression>>,
) -> PipelineGraph {
    let input_types = Box::new([key_type.clone(), LogicalType::Integer]);
    let mut aggregate = grouped_count_spec(None);
    aggregate.projection_exprs = Box::new([
        reference(0, key_type.clone()),
        reference(1, LogicalType::Integer),
    ]);
    aggregate.payload_types = input_types.clone();
    aggregate.groups = Box::new([reference(0, key_type.clone())]);
    aggregate.output_types = Box::new([key_type.clone(), LogicalType::BigInt]);
    let spec = PartitionAggregateWindowSpec {
        input_types: input_types.clone(),
        detail_columns: Box::new([0, 1]),
        aggregate,
        output_names: Box::new(["grp".to_string(), "v".to_string(), "count".to_string()]),
        output_types: Box::new([key_type, LogicalType::Integer, LogicalType::BigInt]),
    };
    partition_aggregate_window_graph_from_spec(spec, input_rows)
}

fn partition_aggregate_window_graph_from_spec(
    spec: PartitionAggregateWindowSpec,
    input_rows: Vec<Vec<Expression>>,
) -> PipelineGraph {
    let input_types = spec.input_types.to_vec();
    partition_aggregate_window_graph_from_source(
        spec,
        SourceSpec::Values(values_spec(input_rows, input_types)),
    )
}

fn partition_aggregate_window_graph_from_source(
    spec: PartitionAggregateWindowSpec,
    source: SourceSpec,
) -> PipelineGraph {
    spec.verify().expect("partition aggregate window spec");
    let input = RowType::new(
        (0..spec.input_types.len())
            .map(|index| format!("input_{index}"))
            .collect(),
        spec.input_types.to_vec(),
    );
    let output = RowType::new(spec.output_names.to_vec(), spec.output_types.to_vec());
    let mut handles = BreakerHandleCatalogBuilder::default();
    let handle = handles.register(
        BreakerHandleKind::PartitionAggregateWindow,
        output.clone(),
        PipelineProperties::default(),
    );
    let build = PipelineId::new(0);
    let emit = PipelineId::new(1);
    handles.set_producer(handle, build).expect("producer");
    handles.add_consumer(handle, emit).expect("consumer");
    PipelineGraph {
        pipelines: vec![
            PipelineSpec {
                id: build,
                source,
                transforms: Vec::new(),
                sink: SinkSpec::PartitionAggregateWindowBuild(
                    PartitionAggregateWindowBuildSinkSpec {
                        handle,
                        spec: spec.clone(),
                    },
                ),
                sink_sharing: SinkSharing::Exclusive,
                properties: PipelineProperties::default(),
                output: input,
            },
            PipelineSpec {
                id: emit,
                source: SourceSpec::PartitionAggregateWindowEmit(
                    PartitionAggregateWindowEmitSourceSpec { handle, spec },
                ),
                transforms: Vec::new(),
                sink: SinkSpec::ClientResult(ClientResultSpec::default()),
                sink_sharing: SinkSharing::Exclusive,
                properties: PipelineProperties::default(),
                output,
            },
        ],
        dependencies: vec![PipelineDependency {
            producer: build,
            consumer: emit,
            kind: DependencyKind::FinalizeBeforeEmit,
        }],
        handles: handles.finish(),
        control_regions: Vec::new(),
        root: PipelineRoot::Pipeline(emit),
    }
}

#[test]
fn partition_aggregate_window_replays_ties_and_null_partition() {
    let output = QueryOutputPort::unbounded();
    let query = query_context(output.clone());
    let graph = partition_aggregate_window_graph(
        LogicalType::Integer,
        vec![
            vec![int_constant(1), int_constant(10)],
            vec![null_constant(LogicalType::Integer), int_constant(20)],
            vec![int_constant(1), int_constant(30)],
            vec![null_constant(LogicalType::Integer), int_constant(40)],
            vec![int_constant(2), int_constant(50)],
        ],
    );
    let thread = ThreadContext::single_threaded();
    let wake = OperatorWakeScope {
        task_id: PipelineTaskId(32),
        generation: WakeGeneration(0),
    };
    run_two_stage_breaker(graph, &query, &thread, &wake);

    let mut rows = Vec::new();
    while let Some(chunk) = output.pop_front() {
        rows.extend((0..chunk.size()).map(|row| {
            (
                chunk.column(0).unwrap().get_value(row),
                chunk.column(1).unwrap().get_i32(row).unwrap(),
                chunk.column(2).unwrap().get_i64(row).unwrap(),
            )
        }));
    }
    assert_eq!(rows.len(), 5);
    assert_eq!(rows[0], (Value::Integer(1), 10, 2));
    assert!(rows[1].0.is_null());
    assert_eq!((rows[1].1, rows[1].2), (20, 2));
    assert_eq!(rows[2], (Value::Integer(1), 30, 2));
    assert!(rows[3].0.is_null());
    assert_eq!((rows[3].1, rows[3].2), (40, 2));
    assert_eq!(rows[4], (Value::Integer(2), 50, 1));
}

#[test]
fn partition_aggregate_window_executes_bigint_key_domain() {
    let output = QueryOutputPort::unbounded();
    let query = query_context(output.clone());
    let bigint = |value| {
        Expression::Constant(ConstantExpression::new(
            Value::BigInt(value),
            LogicalType::BigInt,
        ))
    };
    let graph = partition_aggregate_window_graph(
        LogicalType::BigInt,
        vec![
            vec![bigint(i64::MAX), int_constant(10)],
            vec![bigint(i64::MAX - 1), int_constant(20)],
            vec![bigint(i64::MAX), int_constant(30)],
        ],
    );
    let thread = ThreadContext::single_threaded();
    let wake = OperatorWakeScope {
        task_id: PipelineTaskId(33),
        generation: WakeGeneration(0),
    };
    run_two_stage_breaker(graph, &query, &thread, &wake);

    let chunk = output.pop_front().expect("BIGINT partition output");
    assert_eq!(chunk.size(), 3);
    assert_eq!(chunk.column(0).unwrap().get_i64(0), Some(i64::MAX));
    assert_eq!(chunk.column(2).unwrap().get_i64(0), Some(2));
    assert_eq!(chunk.column(0).unwrap().get_i64(1), Some(i64::MAX - 1));
    assert_eq!(chunk.column(2).unwrap().get_i64(1), Some(1));
    assert_eq!(chunk.column(0).unwrap().get_i64(2), Some(i64::MAX));
    assert_eq!(chunk.column(2).unwrap().get_i64(2), Some(2));
}

#[test]
fn partition_aggregate_window_forced_external_replays_raw_payload() {
    let output = QueryOutputPort::unbounded();
    let query = query_context_with_limits(
        output.clone(),
        RuntimeLimits {
            max_threads: 1,
            max_memory: 64 * 1024 * 1024,
            use_temporary_directory: true,
            temporary_directory: unique_temp_dir("paro_partition_aggregate_spill"),
            max_temp_directory_size: None,
            force_external: true,
            rowset_scan_pushdown: true,
            parallel_scheduler: false,
        },
    );
    let graph = partition_aggregate_window_graph(
        LogicalType::Integer,
        vec![
            vec![int_constant(1), int_constant(10)],
            vec![null_constant(LogicalType::Integer), int_constant(20)],
            vec![int_constant(1), int_constant(30)],
            vec![null_constant(LogicalType::Integer), int_constant(40)],
            vec![int_constant(2), int_constant(50)],
        ],
    );
    let thread = ThreadContext::single_threaded();
    let wake = OperatorWakeScope {
        task_id: PipelineTaskId(34),
        generation: WakeGeneration(0),
    };
    let profile = run_two_stage_breaker_with_profile(graph, &query, &thread, &wake);

    let mut rows = Vec::new();
    while let Some(chunk) = output.pop_front() {
        rows.extend((0..chunk.size()).map(|row| {
            (
                chunk.column(0).unwrap().get_value(row),
                chunk.column(1).unwrap().get_i32(row).unwrap(),
                chunk.column(2).unwrap().get_i64(row).unwrap(),
            )
        }));
    }
    rows.sort_by_key(|row| row.1);
    assert_eq!(rows.len(), 5);
    assert_eq!(rows[0], (Value::Integer(1), 10, 2));
    assert!(rows[1].0.is_null());
    assert_eq!((rows[1].1, rows[1].2), (20, 2));
    assert_eq!(rows[2], (Value::Integer(1), 30, 2));
    assert!(rows[3].0.is_null());
    assert_eq!((rows[3].1, rows[3].2), (40, 2));
    assert_eq!(rows[4], (Value::Integer(2), 50, 1));
    assert!(profile.operators.values().any(|actual| {
        actual.runtime.spilled == Some(true)
            && actual.runtime.spilled_bytes.unwrap_or(0) > 0
            && actual.runtime.repartition_depth == Some(1)
    }));
}

#[test]
fn partition_aggregate_window_forced_external_preserves_filter_payload() {
    let output = QueryOutputPort::unbounded();
    let query = query_context_with_limits(
        output.clone(),
        RuntimeLimits {
            max_threads: 1,
            max_memory: 64 * 1024 * 1024,
            use_temporary_directory: true,
            temporary_directory: unique_temp_dir("paro_partition_aggregate_filter_spill"),
            max_temp_directory_size: None,
            force_external: true,
            rowset_scan_pushdown: true,
            parallel_scheduler: false,
        },
    );
    let (count, targets) = get_count_function()
        .bind(&[LogicalType::Integer])
        .expect("bind count(integer)");
    assert_eq!(targets, vec![LogicalType::Integer]);
    let count = Expression::Aggregate(
        AggregateExpression::new(
            count,
            vec![reference(1, LogicalType::Integer)],
            LogicalType::BigInt,
        )
        .with_filter(Some(reference(2, LogicalType::Boolean))),
    );
    let input_types = Box::new([
        LogicalType::Integer,
        LogicalType::Integer,
        LogicalType::Boolean,
    ]);
    let spec = PartitionAggregateWindowSpec {
        input_types: input_types.clone(),
        detail_columns: Box::new([0, 1]),
        aggregate: AggregateSpec {
            grouping_key_count: 1,
            state_output_projection: Box::new([]),
            estimated_input_rows: None,
            projection_exprs: Box::new([
                reference(0, LogicalType::Integer),
                reference(1, LogicalType::Integer),
                reference(2, LogicalType::Boolean),
            ]),
            payload_types: input_types,
            groups: Box::new([reference(0, LogicalType::Integer)]),
            group_key_encodings: Box::new([crate::physical::specs::GroupKeyEncoding::Identity]),
            grouping_sets: Box::new([]),
            aggregates: Box::new([count]),
            grouping_functions: Box::new([]),
            aggregate_inputs: Box::new([Box::new([1])]),
            aggregate_filters: Box::new([Some(2)]),
            aggregate_orders: Box::new([Box::new([])]),
            post_reduction: None,
            having_filter: Box::new([]),
            perfect_hash: None,
            output_names: Box::new(["k".to_string(), "count".to_string()]),
            output_types: Box::new([LogicalType::Integer, LogicalType::BigInt]),
        },
        output_names: Box::new(["grp".to_string(), "v".to_string(), "count".to_string()]),
        output_types: Box::new([
            LogicalType::Integer,
            LogicalType::Integer,
            LogicalType::BigInt,
        ]),
    };
    let graph = partition_aggregate_window_graph_from_spec(
        spec,
        vec![
            vec![int_constant(1), int_constant(10), bool_constant(true)],
            vec![int_constant(1), int_constant(20), bool_constant(false)],
            vec![
                int_constant(1),
                int_constant(30),
                null_constant(LogicalType::Boolean),
            ],
            vec![int_constant(2), int_constant(40), bool_constant(true)],
        ],
    );
    let thread = ThreadContext::single_threaded();
    let wake = OperatorWakeScope {
        task_id: PipelineTaskId(35),
        generation: WakeGeneration(0),
    };
    run_two_stage_breaker(graph, &query, &thread, &wake);

    let mut rows = Vec::new();
    while let Some(chunk) = output.pop_front() {
        rows.extend((0..chunk.size()).map(|row| {
            (
                chunk.column(0).unwrap().get_i32(row).unwrap(),
                chunk.column(1).unwrap().get_i32(row).unwrap(),
                chunk.column(2).unwrap().get_i64(row).unwrap(),
            )
        }));
    }
    rows.sort_unstable_by_key(|row| row.1);
    assert_eq!(rows, vec![(1, 10, 1), (1, 20, 1), (1, 30, 1), (2, 40, 1)]);
}

#[test]
fn partition_aggregate_window_spills_after_columnar_growth_hits_query_cap() {
    let allocator = paro_common::test_utils::test_allocator();
    let row_count = 40_000usize;
    let mut chunks = Vec::new();
    for start in (0..row_count).step_by(VECTOR_SIZE) {
        let count = (row_count - start).min(VECTOR_SIZE);
        let mut chunk = Chunk::try_initialize(
            &[LogicalType::Integer, LogicalType::Integer],
            count,
            allocator.clone(),
        )
        .expect("input chunk");
        chunk.set_cardinality(count);
        for row in 0..count {
            let value = (start + row) as i32;
            chunk.set_value(0, row, &Value::Integer(value)).unwrap();
            chunk.set_value(1, row, &Value::Integer(value)).unwrap();
        }
        chunks.push(chunk);
    }
    let input_types = Box::new([LogicalType::Integer, LogicalType::Integer]);
    let mut aggregate = grouped_count_spec(None);
    aggregate.projection_exprs = Box::new([
        reference(0, LogicalType::Integer),
        reference(1, LogicalType::Integer),
    ]);
    aggregate.payload_types = input_types.clone();
    let spec = PartitionAggregateWindowSpec {
        input_types: input_types.clone(),
        detail_columns: Box::new([0, 1]),
        aggregate,
        output_names: Box::new(["grp".to_string(), "v".to_string(), "count".to_string()]),
        output_types: Box::new([
            LogicalType::Integer,
            LogicalType::Integer,
            LogicalType::BigInt,
        ]),
    };
    let graph = partition_aggregate_window_graph_from_source(
        spec,
        SourceSpec::Chunk(ChunkScanSpec {
            chunks: Arc::from(chunks.into_boxed_slice()),
            output_names: Box::new(["grp".to_string(), "v".to_string()]),
            output_types: input_types,
        }),
    );
    let output = QueryOutputPort::discarding();
    let query = query_context_with_limits(
        output.clone(),
        RuntimeLimits {
            max_threads: 1,
            max_memory: 2 * 1024 * 1024,
            use_temporary_directory: true,
            temporary_directory: unique_temp_dir("paro_partition_aggregate_dynamic_spill"),
            max_temp_directory_size: None,
            force_external: false,
            rowset_scan_pushdown: true,
            parallel_scheduler: false,
        },
    );
    let thread = ThreadContext::single_threaded();
    let wake = OperatorWakeScope {
        task_id: PipelineTaskId(36),
        generation: WakeGeneration(0),
    };
    let profile = run_two_stage_breaker_with_profile(graph, &query, &thread, &wake);
    assert!(profile
        .operators
        .values()
        .any(|actual| actual.runtime.spilled == Some(true)));
    assert_eq!(output.stats().pushed_rows, row_count);
}
