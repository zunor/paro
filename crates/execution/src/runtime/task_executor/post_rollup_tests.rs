// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! End-to-end runtime coverage for perfect-aggregate input rollups.

use paro_function::scalar::cast::decimal_casts::bind_decimal_casts;
use paro_function::scalar::cast::CastFunctionSet;
use paro_planner::expression::CastExpression;

use super::*;
use crate::pipeline::PipelineProgramSet;
use crate::runtime::breaker::{AggregateHandle, AggregateRuntimeState};

fn build_pipeline() -> PipelineId {
    PipelineId::new(0)
}

fn emit_pipeline() -> PipelineId {
    PipelineId::new(1)
}

fn decimal_type(precision: u8, scale: u8) -> LogicalType {
    LogicalType::Decimal { precision, scale }
}

fn decimal_cast(child: Expression, target_type: LogicalType) -> Expression {
    let source_type = child.return_type();
    let mut casts = CastFunctionSet::new();
    casts.register_bind_function(bind_decimal_casts);
    let cast_info = casts
        .get_cast_function(&source_type, &target_type)
        .expect("bind canonical DECIMAL cast");
    Expression::Cast(CastExpression::new(child, target_type, cast_info, false))
}

fn projected_reference(
    index: usize,
    source_type: &LogicalType,
    projected_type: &LogicalType,
) -> Expression {
    let reference = reference(index, source_type.clone());
    if source_type == projected_type {
        reference
    } else {
        decimal_cast(reference, projected_type.clone())
    }
}

fn decimal_sum_rollup_spec(
    input_type: LogicalType,
    projected_type: LogicalType,
    comparison: ComparisonType,
    max_local_tables: usize,
) -> AggregateSpec {
    let (source_function, arguments) = get_sum_function()
        .bind(std::slice::from_ref(&input_type))
        .expect("bind source DECIMAL SUM");
    assert_eq!(arguments, vec![input_type.clone()]);
    let output_type = source_function.return_type.clone();
    let reducer_function = source_function
        .partial_merge_function()
        .expect("DECIMAL SUM declares its finalized-partial reducer");
    assert_eq!(reducer_function.return_type, output_type);

    let source_aggregate = Expression::Aggregate(AggregateExpression::new(
        source_function,
        vec![reference(1, input_type.clone())],
        output_type.clone(),
    ));
    let reducer = Expression::Aggregate(AggregateExpression::new(
        reducer_function,
        vec![reference(0, output_type.clone())],
        output_type.clone(),
    ));
    let scalar_expression = projected_reference(0, &output_type, &projected_type);
    let predicate = Expression::Comparison(ComparisonExpression::new(
        comparison,
        projected_reference(0, &output_type, &projected_type),
        reference(1, projected_type.clone()),
    ));
    let post_reduction = PostAggregateReductionSpec {
        aggregate_types: Box::new([output_type.clone()]),
        reducers: Box::new([reducer]),
        reducer_types: Box::new([output_type.clone()]),
        scalar_expressions: Box::new([scalar_expression]),
        scalar_types: Box::new([projected_type]),
        predicate,
        input_rollup_sources: Some(vec![0usize].into_boxed_slice()),
    };
    let spec = AggregateSpec {
        grouping_key_count: 1,
        state_output_projection: Box::new([]),
        estimated_input_rows: None,
        projection_exprs: Box::new([]),
        payload_types: Box::new([LogicalType::Integer, input_type]),
        groups: Box::new([reference(0, LogicalType::Integer)]),
        group_key_encodings: Box::new([crate::physical::specs::GroupKeyEncoding::Identity]),
        grouping_sets: Box::new([]),
        aggregates: Box::new([source_aggregate]),
        grouping_functions: Box::new([]),
        aggregate_inputs: Box::new([Box::new([1])]),
        aggregate_filters: Box::new([None]),
        aggregate_orders: Box::new([Box::new([])]),
        post_reduction: Some(post_reduction),
        having_filter: Box::new([]),
        perfect_hash: Some(PerfectHashAggregatePlan {
            group_minima: Box::new([1]),
            group_cardinalities: Box::new([4]),
            max_local_tables,
        }),
        output_names: Box::new(["key".to_string(), "sum".to_string()]),
        output_types: Box::new([LogicalType::Integer, output_type]),
    };
    spec.verify_post_reduction()
        .expect("test rollup spec must satisfy the runtime contract");
    spec
}

fn decimal_chunk(input_type: &LogicalType, rows: &[(i32, Option<i128>)]) -> Chunk {
    let LogicalType::Decimal { precision, scale } = input_type else {
        panic!("test input must be DECIMAL")
    };
    let mut chunk = Chunk::try_initialize(
        &[LogicalType::Integer, input_type.clone()],
        rows.len().max(1),
        paro_common::test_utils::test_allocator(),
    )
    .expect("allocate input chunk");
    chunk.set_cardinality(rows.len());
    for (row_index, (key, value)) in rows.iter().enumerate() {
        chunk
            .set_value(0, row_index, &Value::Integer(*key))
            .expect("write group key");
        chunk
            .set_value(
                1,
                row_index,
                &value.map_or_else(
                    || Value::Null(input_type.clone()),
                    |value| Value::Decimal(value, *precision, *scale),
                ),
            )
            .expect("write aggregate input");
    }
    chunk
}

fn perfect_rollup_graph(spec: AggregateSpec, chunks: Vec<Chunk>) -> PipelineGraph {
    let input = RowType::new(
        vec!["key".to_string(), "value".to_string()],
        spec.payload_types.to_vec(),
    );
    let output = RowType::new(spec.output_names.to_vec(), spec.output_types.to_vec());
    let mut handles = BreakerHandleCatalogBuilder::default();
    let handle = handles.register(
        BreakerHandleKind::Aggregate,
        output.clone(),
        PipelineProperties::default(),
    );
    handles
        .set_producer(handle, build_pipeline())
        .expect("register producer");
    handles
        .add_consumer(handle, emit_pipeline())
        .expect("register consumer");
    PipelineGraph {
        pipelines: vec![
            PipelineSpec {
                id: build_pipeline(),
                source: SourceSpec::Chunk(ChunkScanSpec {
                    chunks: Arc::from(chunks.into_boxed_slice()),
                    output_names: input.names.to_vec().into_boxed_slice(),
                    output_types: input.types.to_vec().into_boxed_slice(),
                }),
                transforms: Vec::new(),
                sink: SinkSpec::PerfectHashAggregate(PerfectHashAggregateSinkSpec {
                    handle,
                    spec: spec.clone(),
                }),
                sink_sharing: SinkSharing::Exclusive,
                properties: PipelineProperties::default(),
                output: input,
            },
            PipelineSpec {
                id: emit_pipeline(),
                source: SourceSpec::PerfectHashAggregateEmit(PerfectHashAggregateEmitSourceSpec {
                    handle,
                    spec,
                }),
                transforms: Vec::new(),
                sink: SinkSpec::ClientResult(ClientResultSpec::default()),
                sink_sharing: SinkSharing::Exclusive,
                properties: PipelineProperties::default(),
                output,
            },
        ],
        dependencies: vec![PipelineDependency {
            producer: build_pipeline(),
            consumer: emit_pipeline(),
            kind: DependencyKind::FinalizeBeforeEmit,
        }],
        handles: handles.finish(),
        control_regions: Vec::new(),
        root: PipelineRoot::Pipeline(emit_pipeline()),
    }
}

fn parallel_query(output: QueryOutputPort) -> Arc<QueryRuntimeContext> {
    Arc::new(query_context_with_limits(
        output,
        RuntimeLimits {
            max_threads: 2,
            max_memory: 64 * 1024 * 1024,
            use_temporary_directory: false,
            temporary_directory: String::new(),
            max_temp_directory_size: None,
            force_external: false,
            rowset_scan_pushdown: true,
            parallel_scheduler: true,
        },
    ))
}

fn run_build_stage(
    graph: &PipelineGraph,
    query: Arc<QueryRuntimeContext>,
) -> paro_common::error::Result<(PipelineProgramSet, Arc<BreakerHandleRegistry>)> {
    let programs = PipelineProgramBuilder::default().build_program_set(graph)?;
    let handles = Arc::new(BreakerHandleRegistry::from_catalog(&graph.handles)?);
    let runtime = Arc::new(PipelineRuntime::with_registry(
        programs
            .get(build_pipeline())
            .expect("build program")
            .clone(),
        handles.clone(),
        query.params.clone(),
        query.as_ref(),
    )?);
    crate::runtime::scheduler::run_bound_pipeline_runtime(
        runtime,
        graph
            .pipeline(build_pipeline())
            .expect("build pipeline")
            .properties
            .capabilities
            .parallelism,
        query,
        paro_common::test_utils::test_allocator(),
    )?;
    Ok((programs, handles))
}

fn run_emit_stage(
    graph: &PipelineGraph,
    programs: &PipelineProgramSet,
    handles: Arc<BreakerHandleRegistry>,
    query: Arc<QueryRuntimeContext>,
) -> paro_common::error::Result<()> {
    let runtime = Arc::new(PipelineRuntime::with_registry(
        programs.get(emit_pipeline()).expect("emit program").clone(),
        handles,
        query.params.clone(),
        query.as_ref(),
    )?);
    crate::runtime::scheduler::run_bound_pipeline_runtime(
        runtime,
        graph
            .pipeline(emit_pipeline())
            .expect("emit pipeline")
            .properties
            .capabilities
            .parallelism,
        query,
        paro_common::test_utils::test_allocator(),
    )
}

fn inspect_finalized_table(
    handles: &BreakerHandleRegistry,
    expected_preselection: bool,
    expected_scalar: Option<&Value>,
) {
    let handle = handles
        .get(HandleRef::<AggregateHandle>::new(BreakerHandleId::new(0)))
        .expect("aggregate handle");
    let scalar = handle
        .post_reduction_values()
        .expect("published post-reduction scalar");
    assert_eq!(scalar.len(), 1);
    match expected_scalar {
        Some(expected) => assert_eq!(&scalar[0].get_value(0), expected),
        None => assert!(scalar[0].is_null(0)),
    }
    handle
        .with_state_mut(|state| {
            let AggregateRuntimeState::Perfect(state) = state else {
                panic!("expected perfect aggregate runtime state")
            };
            assert!(state.pending_tables.is_empty());
            assert!(state.input_rollup.is_none());
            let finalized = state
                .finalized_table
                .as_ref()
                .expect("finalized perfect table");
            assert_eq!(finalized.has_preselection(), expected_preselection);
            assert_eq!(
                finalized.post_reduction_visit_count(),
                0,
                "an input-rollup scalar must make the finalized-group reduction redundant"
            );
            Ok(())
        })
        .expect("inspect finalized aggregate state");
}

fn collect_decimal_rows(output: &QueryOutputPort) -> Vec<(i32, Value)> {
    let mut rows = Vec::new();
    while let Some(chunk) = output.pop_front() {
        rows.extend((0..chunk.size()).map(|row| {
            (
                chunk.column(0).unwrap().get_i32(row).unwrap(),
                chunk.column(1).unwrap().get_value(row),
            )
        }));
    }
    rows.sort_by_key(|(key, _)| *key);
    rows
}

#[test]
fn q11_decimal_cast_rollup_preselects_during_multi_local_perfect_merge() {
    let input_type = decimal_type(15, 2);
    let projected_type = decimal_type(38, 4);
    let spec = decimal_sum_rollup_spec(
        input_type.clone(),
        projected_type,
        ComparisonType::GreaterThan,
        2,
    );
    let graph = perfect_rollup_graph(
        spec,
        vec![
            decimal_chunk(&input_type, &[(1, Some(3_000)), (2, Some(-1_000))]),
            decimal_chunk(&input_type, &[(1, Some(3_000)), (2, Some(-1_000))]),
        ],
    );
    let output = QueryOutputPort::unbounded();
    let query = parallel_query(output.clone());

    let (programs, handles) =
        run_build_stage(&graph, query.clone()).expect("parallel perfect build");
    // SUM(raw inputs)=40.00, projected to DECIMAL(38,4). The stable
    // preselection marker proves that the two local tables were filtered
    // during merge, rather than by the later generic emit predicate.
    inspect_finalized_table(
        handles.as_ref(),
        true,
        Some(&Value::Decimal(400_000, 38, 4)),
    );
    run_emit_stage(&graph, &programs, handles, query).expect("perfect emit");

    assert_eq!(
        collect_decimal_rows(&output),
        vec![(1, Value::Decimal(6_000, 38, 2))]
    );
}

#[test]
fn q11_decimal_cast_single_local_uses_rollup_scalar_without_group_scan() {
    let input_type = decimal_type(15, 2);
    let spec = decimal_sum_rollup_spec(
        input_type.clone(),
        decimal_type(38, 4),
        ComparisonType::GreaterThan,
        2,
    );
    let graph = perfect_rollup_graph(
        spec,
        vec![decimal_chunk(
            &input_type,
            &[
                (1, Some(3_000)),
                (1, Some(3_000)),
                (2, Some(-1_000)),
                (2, Some(-1_000)),
            ],
        )],
    );
    let output = QueryOutputPort::unbounded();
    let query = parallel_query(output.clone());

    let (programs, handles) =
        run_build_stage(&graph, query.clone()).expect("single-local perfect build");
    inspect_finalized_table(
        handles.as_ref(),
        false,
        Some(&Value::Decimal(400_000, 38, 4)),
    );
    run_emit_stage(&graph, &programs, handles, query).expect("perfect emit");

    assert_eq!(
        collect_decimal_rows(&output),
        vec![(1, Value::Decimal(6_000, 38, 2))]
    );
}

#[test]
fn rejected_group_decimal_finalize_overflow_is_not_hidden_by_merge_preselection() {
    let input_type = decimal_type(38, 0);
    let spec = decimal_sum_rollup_spec(
        input_type.clone(),
        decimal_type(38, 0),
        ComparisonType::LessThan,
        2,
    );
    let max_decimal = 10_i128.pow(38) - 1;
    let graph = perfect_rollup_graph(
        spec,
        vec![
            decimal_chunk(
                &input_type,
                &[(1, Some(max_decimal)), (2, Some(-max_decimal))],
            ),
            decimal_chunk(&input_type, &[(1, Some(1))]),
        ],
    );
    let query = parallel_query(QueryOutputPort::unbounded());

    let error = run_build_stage(&graph, query)
        .expect_err("group 1 exceeds DECIMAL(38) before its false comparison is usable");
    assert!(
        error
            .to_string()
            .contains("Decimal SUM result exceeds precision 38"),
        "unexpected finalize error: {error}"
    );
}

#[test]
fn empty_and_all_null_rollup_scalars_make_the_post_predicate_unknown() {
    let input_type = decimal_type(15, 2);
    let cases = [
        Vec::new(),
        vec![
            decimal_chunk(&input_type, &[(1, None)]),
            decimal_chunk(&input_type, &[(2, None)]),
        ],
    ];
    for chunks in cases {
        let spec = decimal_sum_rollup_spec(
            input_type.clone(),
            decimal_type(38, 4),
            ComparisonType::GreaterThan,
            2,
        );
        let graph = perfect_rollup_graph(spec, chunks);
        let output = QueryOutputPort::unbounded();
        let query = parallel_query(output.clone());

        let (programs, handles) =
            run_build_stage(&graph, query.clone()).expect("NULL-domain perfect build");
        inspect_finalized_table(handles.as_ref(), false, None);
        run_emit_stage(&graph, &programs, handles, query).expect("NULL-domain perfect emit");
        assert!(collect_decimal_rows(&output).is_empty());
    }
}
