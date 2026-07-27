// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;
use std::sync::OnceLock;

use paro_common::runtime_value::Value;
use paro_common::typed_parameters::{ParameterSlot, RuntimeParamId};
use paro_common::types::LogicalType;
use paro_common::vector::Vector;
use paro_common::vector::VECTOR_SIZE;
use paro_context::{
    NoopStatementTimeoutDriver, RuntimeLimits, StatementCancelReason, StatementCancellation,
    StatementContext, TestStatementContextBuilder,
};
use paro_function::aggregate::distributive::count::get_count_star_function;
use paro_planner::binder::context::BindContext;
use paro_planner::binder::ir::OrderByNode;
use paro_planner::expression::{
    AggregateExpression, ConstantExpression, Expression, ParameterExpression, ReferenceExpression,
};
use paro_planner::operator::join::{Join, JoinCondition, JoinType};
use paro_planner::operator::{
    Aggregate as LogicalAggregate, ExpressionGet, LogicalOperator, Order as LogicalOrder,
};
use paro_planner::plan::LogicalPlan;

use crate::memory_runtime::QueryMemoryPool;
use crate::physical::children::{PlanChildren, PlanChildrenArena};
use crate::physical::generator::{PhysicalPlanGenerator, PlanBuildContext};
use crate::physical::ids::PhysicalPlanNodeId;
use crate::physical::node::{OperatorLabel, PhysicalPlanNode};
use crate::physical::plan::{PhysicalPlan, PhysicalPlanNodeArena};
use crate::physical::properties::{Parallelism, PipelineProperties, PlanPropertyMap};
use crate::physical::specs::PhysicalNodeKind;
use crate::physical::{ChunkScanSpec, DummyScanSpec, RowType};
use crate::pipeline::graph::{
    ClientResultSpec, ControlRegion, CorrelatedSubqueryRegion, DelimJoinSide, DependencyKind,
    MaterializeSinkSpec, MaterializedSourceSpec, PipelineDependency, PipelineGraph, PipelineId,
    PipelineRoot, PipelineSpec, PipelineSubgraphRoot, SinkSharing, SinkSpec, SourceSpec,
};
use crate::pipeline::handles::{BreakerHandleCatalogBuilder, BreakerHandleId, BreakerHandleKind};
use crate::pipeline::lowerer::PipelineLowerer;
use crate::pipeline::{PipelineProgramBuilder, StatementProgram};
use crate::query_executor::pipeline_driver::{PipelineDriveResult, PipelineExecutionDriver};
use crate::query_executor::program_executor::{
    control_region_pipeline_members, control_region_root_pipelines, execute_program,
    run_pipeline_graph_with_registry_for_test, start_program, start_program_with_output_for_test,
    ProgramExecution,
};
use crate::query_executor::stream::ResultHandler;
use crate::runtime::{
    BreakerHandleRegistry, CleanupStatus, ParameterBindingEpoch, ParameterBindings,
    QueryOutputPort, QueryOutputPortStats, QueryRuntimeContext,
};
use tokio_util::sync::CancellationToken;

#[test]
fn execute_program_uses_compiled_parameter_bindings() {
    let ctx = BindContext::new();
    let param = Expression::Parameter(ParameterExpression::new(ParameterSlot::new(
        RuntimeParamId::new(0),
        LogicalType::Integer,
    )));
    let logical = LogicalPlan::new(
        &ctx,
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            0,
            vec![vec![param]],
            vec!["p".to_string()],
            vec![LogicalType::Integer],
        )),
    );

    let mut generator = PhysicalPlanGenerator::new(PlanBuildContext::default());
    let plan = Arc::new(generator.generate(&logical).expect("physical plan"));
    let mut lowerer = PipelineLowerer::new(plan.as_ref());
    let graph = Arc::new(
        lowerer
            .lower_to_pipeline_graph(plan.root)
            .expect("pipeline graph"),
    );
    let programs = PipelineProgramBuilder::default()
        .build_program_set(graph.as_ref())
        .expect("pipeline programs");
    let statement = StatementProgram::Pipeline {
        plan,
        graph,
        programs,
    };
    let params = Arc::new(
        ParameterBindings::new(
            vec![Value::Integer(42)],
            vec![LogicalType::Integer],
            ParameterBindingEpoch::new(1),
        )
        .expect("params"),
    );

    let execution = execute_program(
        TestStatementContextBuilder::minimal().build(),
        &statement,
        params,
        Arc::new(QueryMemoryPool::unbounded()),
        paro_common::test_utils::test_allocator(),
    )
    .expect("execute program");

    let chunk = execution
        .query
        .output
        .pop_front()
        .expect("parameter output");
    assert_eq!(chunk.size(), 1);
    assert_eq!(chunk.column(0).unwrap().get_i32(0), Some(42));
    assert!(execution.query.output.pop_front().is_none());
}

#[test]
fn start_program_drives_root_output_on_fetch() {
    let output_type = LogicalType::Integer;
    let allocator = paro_common::test_utils::test_allocator();
    let chunks = vec![i32_chunk(&[1, 2, 3])];
    let row_type = RowType::new(vec!["v".to_string()], vec![output_type.clone()]);
    let chunk_spec = ChunkScanSpec {
        chunks: Arc::from(chunks.into_boxed_slice()),
        output_names: vec!["v".to_string()].into_boxed_slice(),
        output_types: vec![output_type.clone()].into_boxed_slice(),
    };
    let graph = Arc::new(PipelineGraph {
        pipelines: vec![PipelineSpec {
            id: PipelineId::new(0),
            source: SourceSpec::Chunk(chunk_spec.clone()),
            transforms: Vec::new(),
            sink: SinkSpec::ClientResult(ClientResultSpec::default()),
            sink_sharing: SinkSharing::Exclusive,
            properties: PipelineProperties::default(),
            output: row_type,
        }],
        dependencies: Vec::new(),
        handles: BreakerHandleCatalogBuilder::default().finish(),
        control_regions: Vec::new(),
        root: PipelineRoot::Pipeline(PipelineId::new(0)),
    });
    let programs = PipelineProgramBuilder::default()
        .build_program_set(graph.as_ref())
        .expect("pipeline programs");
    let statement = StatementProgram::Pipeline {
        plan: Arc::new(single_node_plan(
            PhysicalNodeKind::ChunkScan(chunk_spec),
            RowType::new(vec!["v".to_string()], vec![output_type]),
        )),
        graph,
        programs,
    };

    let mut execution = start_program(
        TestStatementContextBuilder::minimal().build(),
        &statement,
        Arc::new(ParameterBindings::empty()),
        Arc::new(QueryMemoryPool::unbounded()),
        allocator,
    )
    .expect("start program");

    assert!(execution.driver.is_some());
    assert!(execution.query.output.is_empty());
    let driver = execution.driver.as_mut().expect("fetch-driven driver");
    assert_eq!(
        driver
            .drive_until_output_or_finished(&execution.query)
            .expect("drive"),
        PipelineDriveResult::ChunkReady
    );
    assert_eq!(execution.query.output.len(), 1);
    let chunk = execution.query.output.pop_front().expect("output chunk");
    assert_eq!(chunk.size(), 3);
    assert_eq!(chunk.column(0).unwrap().get_i32(0), Some(1));
    assert_eq!(chunk.column(0).unwrap().get_i32(1), Some(2));
    assert_eq!(chunk.column(0).unwrap().get_i32(2), Some(3));
    assert_eq!(
        driver
            .drive_until_output_or_finished(&execution.query)
            .expect("finish"),
        PipelineDriveResult::Finished
    );
}

#[test]
fn fetch_driven_materialized_dag_keeps_root_output_bounded() {
    let allocator = paro_common::test_utils::test_allocator();
    let chunk_count = 8usize;
    let chunks = (0..chunk_count)
        .map(|idx| i32_chunk(&[idx as i32]))
        .collect::<Vec<_>>();
    let (statement, _handle) = materialized_statement(chunks);
    let execution = start_program_with_output_for_test(
        TestStatementContextBuilder::minimal().build(),
        &statement,
        Arc::new(ParameterBindings::empty()),
        Arc::new(QueryMemoryPool::unbounded()),
        allocator.clone(),
        QueryOutputPort::bounded_with_stats(2),
        QueryOutputPort::unbounded_with_stats(),
        true,
    )
    .expect("start program");
    let output = execution.query.output.clone();
    assert_eq!(output.capacity(), 2);

    let mut handler = ResultHandler::from_program_execution(
        vec!["v".to_string()],
        vec![LogicalType::Integer],
        execution,
        allocator,
        None,
    )
    .expect("handler");

    let mut values = Vec::new();
    while let Some(chunk) = handler.fetch().expect("fetch") {
        values.push(chunk.column(0).unwrap().get_i32(0).unwrap());
    }

    assert_eq!(values, (0..chunk_count as i32).collect::<Vec<_>>());
    let stats = output.stats();
    assert_eq!(stats.pushed_chunks, chunk_count);
    assert_eq!(stats.popped_chunks, chunk_count);
    assert_eq!(stats.pushed_rows, chunk_count);
    assert_eq!(stats.popped_rows, chunk_count);
    assert_eq!(stats.blocked_pushes, 0);
    assert!(
        stats.peak_queue_chunks <= output.capacity(),
        "fetch-driven output peak must stay bounded: stats={stats:?}"
    );
}

#[test]
fn fetch_driven_hash_join_dag_keeps_root_output_bounded() {
    let row_count = VECTOR_SIZE * 3 + 17;
    let statement = statement_from_logical(hash_join_logical_plan(row_count));
    assert_pipeline_count_at_least(&statement, 2);

    let (rows, stats, capacity) = collect_fetch_driven_rows_with_bounded_output(
        &statement,
        vec!["lk", "lv", "rk", "rv"],
        vec![
            LogicalType::Integer,
            LogicalType::Integer,
            LogicalType::Integer,
            LogicalType::Integer,
        ],
    );

    assert_eq!(rows.len(), row_count);
    assert_eq!(stats.pushed_rows, row_count);
    assert_eq!(stats.popped_rows, row_count);
    assert!(
        stats.peak_queue_chunks <= capacity,
        "hash join fetch-driven output must stay bounded: stats={stats:?}"
    );
}

#[test]
fn fetch_driven_sort_dag_keeps_root_output_bounded() {
    let row_count = VECTOR_SIZE * 3 + 17;
    let statement = statement_from_logical(sort_logical_plan(row_count));
    assert_pipeline_count_at_least(&statement, 2);

    let (rows, stats, capacity) = collect_fetch_driven_rows_with_bounded_output(
        &statement,
        vec!["v"],
        vec![LogicalType::Integer],
    );

    assert_eq!(rows.len(), row_count);
    let values = rows
        .iter()
        .map(|row| match row.as_slice() {
            [Value::Integer(value)] => *value,
            other => panic!("unexpected sort row: {other:?}"),
        })
        .collect::<Vec<_>>();
    assert!(values.windows(2).all(|pair| pair[0] <= pair[1]));
    assert_eq!(values.first().copied(), Some(0));
    assert_eq!(values.last().copied(), Some(row_count as i32 - 1));
    assert!(
        stats.peak_queue_chunks <= capacity,
        "sort fetch-driven output must stay bounded: stats={stats:?}"
    );
}

#[test]
fn fetch_driven_aggregate_dag_keeps_root_output_bounded() {
    let row_count = VECTOR_SIZE * 3 + 17;
    let statement = statement_from_logical(grouped_aggregate_logical_plan(row_count));
    assert_pipeline_count_at_least(&statement, 2);

    let (rows, stats, capacity) = collect_fetch_driven_rows_with_bounded_output(
        &statement,
        vec!["k", "count_star"],
        vec![LogicalType::Integer, LogicalType::BigInt],
    );

    assert_eq!(rows.len(), row_count);
    let total_count = rows
        .iter()
        .map(|row| match row.as_slice() {
            [Value::Integer(_), Value::BigInt(count)] => *count,
            other => panic!("unexpected aggregate row: {other:?}"),
        })
        .sum::<i64>();
    assert_eq!(total_count, row_count as i64);
    assert!(
        stats.peak_queue_chunks <= capacity,
        "aggregate fetch-driven output must stay bounded: stats={stats:?}"
    );
}

#[test]
fn completed_output_parallel_scheduler_consumes_chunk_morsels() {
    let allocator = paro_common::test_utils::test_allocator();
    let chunks = (0..64).map(|value| i32_chunk(&[value])).collect::<Vec<_>>();
    let output_type = LogicalType::Integer;
    let row_type = RowType::new(vec!["v".to_string()], vec![output_type.clone()]);
    let chunk_spec = ChunkScanSpec {
        chunks: Arc::from(chunks.into_boxed_slice()),
        output_names: vec!["v".to_string()].into_boxed_slice(),
        output_types: vec![output_type.clone()].into_boxed_slice(),
    };
    let mut properties = PipelineProperties::default();
    properties.capabilities.parallelism = Parallelism::unbounded();
    let graph = Arc::new(PipelineGraph {
        pipelines: vec![PipelineSpec {
            id: PipelineId::new(0),
            source: SourceSpec::Chunk(chunk_spec.clone()),
            transforms: Vec::new(),
            sink: SinkSpec::ClientResult(ClientResultSpec::default()),
            sink_sharing: SinkSharing::Exclusive,
            properties,
            output: row_type.clone(),
        }],
        dependencies: Vec::new(),
        handles: BreakerHandleCatalogBuilder::default().finish(),
        control_regions: Vec::new(),
        root: PipelineRoot::Pipeline(PipelineId::new(0)),
    });
    let programs = PipelineProgramBuilder::default()
        .build_program_set(graph.as_ref())
        .expect("pipeline programs");
    let statement = StatementProgram::Pipeline {
        plan: Arc::new(single_node_plan(
            PhysicalNodeKind::ChunkScan(chunk_spec),
            row_type,
        )),
        graph,
        programs,
    };
    let session = TestStatementContextBuilder::minimal()
        .with_limits(RuntimeLimits {
            max_threads: 4,
            max_memory: 64 * 1024 * 1024,
            use_temporary_directory: false,
            temporary_directory: String::new(),
            max_temp_directory_size: None,
            force_external: false,
            rowset_scan_pushdown: true,
            parallel_scheduler: true,
        })
        .build();
    session.scheduler().set_threads(4).expect("worker threads");

    let execution = execute_program(
        session,
        &statement,
        Arc::new(ParameterBindings::empty()),
        Arc::new(QueryMemoryPool::unbounded()),
        allocator,
    )
    .expect("execute parallel scheduler program");

    let mut values = Vec::new();
    while let Some(chunk) = execution.query.output.pop_front() {
        values.push(chunk.column(0).unwrap().get_i32(0).unwrap());
    }
    values.sort_unstable();
    assert_eq!(values, (0..64).collect::<Vec<_>>());
}

#[test]
fn completed_output_materializes_root_output_until_client_fetch() {
    let allocator = paro_common::test_utils::test_allocator();
    let chunk_count = 8usize;
    let chunks = (0..chunk_count)
        .map(|idx| i32_chunk(&[idx as i32]))
        .collect::<Vec<_>>();
    let (statement, _handle) = materialized_statement(chunks);

    let execution = start_program_with_output_for_test(
        TestStatementContextBuilder::minimal().build(),
        &statement,
        Arc::new(ParameterBindings::empty()),
        Arc::new(QueryMemoryPool::unbounded()),
        allocator,
        QueryOutputPort::bounded_with_stats(2),
        QueryOutputPort::unbounded_with_stats(),
        false,
    )
    .expect("execute program");

    let stats = execution.query.output.stats();
    assert_eq!(execution.query.output.len(), chunk_count);
    assert_eq!(stats.pushed_chunks, chunk_count);
    assert_eq!(stats.popped_chunks, 0);
    assert_eq!(stats.pushed_rows, chunk_count);
    assert_eq!(stats.popped_rows, 0);
    assert_eq!(stats.blocked_pushes, 0);
    assert_eq!(stats.peak_queue_chunks, chunk_count);
    assert!(
        stats.peak_queue_chunks > 2,
        "completed-output path should expose materialization in this comparison test"
    );
}

#[test]
fn result_handler_cleans_fetch_driver_on_normal_finish() {
    let allocator = paro_common::test_utils::test_allocator();
    let (statement, handle) = materialized_statement(vec![i32_chunk(&[1, 2, 3])]);
    let execution = start_program(
        TestStatementContextBuilder::minimal().build(),
        &statement,
        Arc::new(ParameterBindings::empty()),
        Arc::new(QueryMemoryPool::unbounded()),
        allocator.clone(),
    )
    .expect("start program");
    let handles = execution
        .driver
        .as_ref()
        .expect("fetch-driven driver")
        .handles_for_test();

    let mut handler = ResultHandler::from_program_execution(
        vec!["v".to_string()],
        vec![LogicalType::Integer],
        execution,
        allocator,
        None,
    )
    .expect("handler");

    let chunk = handler.fetch().expect("first fetch").expect("chunk");
    assert_eq!(chunk.size(), 3);
    assert!(handler.fetch().expect("finish").is_none());
    assert_handle_status(&handles, handle, CleanupStatus::Finished);
}

#[test]
fn result_handler_cleans_fetch_driver_on_client_close() {
    let allocator = paro_common::test_utils::test_allocator();
    let (statement, handle) = materialized_statement(vec![i32_chunk(&[1, 2, 3])]);
    let execution = start_program(
        TestStatementContextBuilder::minimal().build(),
        &statement,
        Arc::new(ParameterBindings::empty()),
        Arc::new(QueryMemoryPool::unbounded()),
        allocator.clone(),
    )
    .expect("start program");
    let handles = execution
        .driver
        .as_ref()
        .expect("fetch-driven driver")
        .handles_for_test();

    let mut handler = ResultHandler::from_program_execution(
        vec!["v".to_string()],
        vec![LogicalType::Integer],
        execution,
        allocator,
        None,
    )
    .expect("handler");

    assert!(handler.fetch().expect("first fetch").is_some());
    handler.close();
    assert_handle_status(&handles, handle, CleanupStatus::Cancelled);
}

#[test]
fn result_handler_cleans_fetch_driver_on_operator_error() {
    let allocator = paro_common::test_utils::test_allocator();
    let (statement, handle) = unsealed_materialized_statement();
    let execution = start_program(
        TestStatementContextBuilder::minimal().build(),
        &statement,
        Arc::new(ParameterBindings::empty()),
        Arc::new(QueryMemoryPool::unbounded()),
        allocator.clone(),
    )
    .expect("start program");
    let handles = execution
        .driver
        .as_ref()
        .expect("fetch-driven driver")
        .handles_for_test();

    let mut handler = ResultHandler::from_program_execution(
        vec!["v".to_string()],
        vec![LogicalType::Integer],
        execution,
        allocator,
        None,
    )
    .expect("handler");

    let err = handler.fetch().expect_err("unsealed source should fail");
    assert!(err
        .message()
        .contains("materialized source was scheduled before producer sealed the handle"));
    assert_handle_status(&handles, handle, CleanupStatus::Failed);
}

#[test]
fn result_handler_cleans_fetch_driver_on_blocked_internal_error() {
    let allocator = paro_common::test_utils::test_allocator();
    let (graph, programs, handle) = materialized_graph(vec![i32_chunk(&[1, 2, 3])]);
    let query = query_context(QueryOutputPort::bounded(0));
    let driver =
        PipelineExecutionDriver::new(graph, programs, &query, allocator.clone()).expect("driver");
    let handles = driver.handles_for_test();
    let execution = ProgramExecution {
        query,
        driver: Some(driver),
        background: None,
    };
    let mut handler = ResultHandler::from_program_execution(
        vec!["v".to_string()],
        vec![LogicalType::Integer],
        execution,
        allocator,
        None,
    )
    .expect("handler");

    let err = handler
        .fetch()
        .expect_err("zero-capacity output should block fetch-driven driver");
    assert!(err
        .message()
        .contains("typed streaming execution blocked without client output"));
    assert_handle_status(&handles, handle, CleanupStatus::Failed);
}

#[test]
fn result_handler_cleans_fetch_driver_on_cancellation() {
    let allocator = paro_common::test_utils::test_allocator();
    let connection_token = CancellationToken::new();
    let statement_token = connection_token.child_token();
    let cancel_reason = Arc::new(OnceLock::new());
    let cancellation = StatementCancellation::from_parts(
        connection_token,
        statement_token.clone(),
        None,
        cancel_reason.clone(),
        Arc::new(NoopStatementTimeoutDriver),
    );
    let statement_context = statement_context_with_cancellation(cancellation);
    let (statement, handle) = materialized_statement(vec![i32_chunk(&[1, 2, 3])]);
    let execution = start_program(
        statement_context,
        &statement,
        Arc::new(ParameterBindings::empty()),
        Arc::new(QueryMemoryPool::unbounded()),
        allocator.clone(),
    )
    .expect("start program");
    let handles = execution
        .driver
        .as_ref()
        .expect("fetch-driven driver")
        .handles_for_test();

    let mut handler = ResultHandler::from_program_execution(
        vec!["v".to_string()],
        vec![LogicalType::Integer],
        execution,
        allocator,
        None,
    )
    .expect("handler");

    let _ = cancel_reason.set(StatementCancelReason::UserRequest);
    statement_token.cancel();
    let err = handler
        .fetch()
        .expect_err("fetch should surface cancellation");
    assert!(err.is_query_canceled());
    assert_handle_status(&handles, handle, CleanupStatus::Cancelled);
}

#[test]
fn completed_output_cleans_breakers_on_normal_finish() {
    let allocator = paro_common::test_utils::test_allocator();
    let (graph, programs, handle) = materialized_graph(vec![i32_chunk(&[1, 2, 3])]);
    let handles = Arc::new(BreakerHandleRegistry::from_catalog(&graph.handles).expect("handles"));
    let query = query_context(QueryOutputPort::unbounded());

    run_pipeline_graph_with_registry_for_test(
        graph.as_ref(),
        &programs,
        handles.clone(),
        &query,
        allocator,
    )
    .expect("completed output run");

    assert_handle_status(&handles, handle, CleanupStatus::Finished);
}

#[test]
fn completed_output_cleans_breakers_on_operator_error() {
    let allocator = paro_common::test_utils::test_allocator();
    let (graph, programs, handle) = unsealed_materialized_graph();
    let handles = Arc::new(BreakerHandleRegistry::from_catalog(&graph.handles).expect("handles"));
    let query = query_context(QueryOutputPort::unbounded());

    let err = run_pipeline_graph_with_registry_for_test(
        graph.as_ref(),
        &programs,
        handles.clone(),
        &query,
        allocator,
    )
    .expect_err("unsealed source should fail");

    assert!(err
        .message()
        .contains("materialized source was scheduled before producer sealed the handle"));
    assert_handle_status(&handles, handle, CleanupStatus::Failed);
}

#[test]
fn completed_output_cleans_breakers_on_blocked_internal_error() {
    let allocator = paro_common::test_utils::test_allocator();
    let (graph, programs, handle) = materialized_graph(vec![i32_chunk(&[1, 2, 3])]);
    let handles = Arc::new(BreakerHandleRegistry::from_catalog(&graph.handles).expect("handles"));
    let query = query_context(QueryOutputPort::bounded(0));

    let err = run_pipeline_graph_with_registry_for_test(
        graph.as_ref(),
        &programs,
        handles.clone(),
        &query,
        allocator,
    )
    .expect_err("zero-capacity output should block completed-output driver");

    assert!(err
        .message()
        .contains("completed-output/control-region pipeline blocked on root output backpressure"));
    assert_handle_status(&handles, handle, CleanupStatus::Failed);
}

#[test]
fn control_region_members_include_nested_region_root_pipelines() {
    let row_type = RowType::new(Vec::new(), Vec::new());
    let pipeline = |id| PipelineSpec {
        id: PipelineId::new(id),
        source: SourceSpec::Dummy(DummyScanSpec),
        transforms: Vec::new(),
        sink: SinkSpec::ClientResult(ClientResultSpec::default()),
        sink_sharing: SinkSharing::Exclusive,
        properties: PipelineProperties::default(),
        output: row_type.clone(),
    };
    let graph = PipelineGraph {
        pipelines: (0..4).map(pipeline).collect(),
        dependencies: Vec::new(),
        handles: BreakerHandleCatalogBuilder::default().finish(),
        control_regions: vec![
            ControlRegion::CorrelatedSubquery(CorrelatedSubqueryRegion {
                side: DelimJoinSide::Left,
                capture: PipelineId::new(0),
                dependent_roots: Vec::new(),
                join: PipelineId::new(1),
                delim_values: crate::pipeline::handles::BreakerHandleId::new(0),
                cached_outer: None,
            }),
            ControlRegion::CorrelatedSubquery(CorrelatedSubqueryRegion {
                side: DelimJoinSide::Left,
                capture: PipelineId::new(1),
                dependent_roots: vec![PipelineSubgraphRoot::Pipeline(PipelineId::new(2))],
                join: PipelineId::new(3),
                delim_values: crate::pipeline::handles::BreakerHandleId::new(1),
                cached_outer: None,
            }),
        ],
        root: PipelineRoot::ControlRegion(crate::pipeline::graph::ControlRegionId::new(1)),
    };

    let roots = control_region_root_pipelines(&graph).expect("region roots");
    let members = control_region_pipeline_members(&graph, &roots).expect("region members");

    assert_eq!(members[0], vec![PipelineId::new(0), PipelineId::new(1)]);
    assert_eq!(
        members[1],
        vec![
            PipelineId::new(0),
            PipelineId::new(1),
            PipelineId::new(2),
            PipelineId::new(3)
        ]
    );
}

fn i32_chunk(values: &[i32]) -> paro_common::chunk::Chunk {
    paro_common::test_utils::test_chunk_from_vectors(vec![Vector::try_from_i32(
        values,
        paro_common::test_utils::test_allocator(),
    )
    .expect("vector")])
}

fn collect_fetch_driven_rows_with_bounded_output(
    statement: &StatementProgram,
    names: Vec<&str>,
    types: Vec<LogicalType>,
) -> (Vec<Vec<Value>>, QueryOutputPortStats, usize) {
    let allocator = paro_common::test_utils::test_allocator();
    let execution = start_program_with_output_for_test(
        TestStatementContextBuilder::minimal().build(),
        statement,
        Arc::new(ParameterBindings::empty()),
        Arc::new(QueryMemoryPool::unbounded()),
        allocator.clone(),
        QueryOutputPort::bounded_with_stats(2),
        QueryOutputPort::unbounded_with_stats(),
        true,
    )
    .expect("start fetch-driven program");
    assert!(
        execution.driver.is_some(),
        "statement must use fetch-driven path"
    );

    let output = execution.query.output.clone();
    let capacity = output.capacity();
    assert_eq!(capacity, 2);

    let mut handler = ResultHandler::from_program_execution(
        names.into_iter().map(str::to_string).collect(),
        types,
        execution,
        allocator,
        None,
    )
    .expect("result handler");

    let mut rows = Vec::new();
    while let Some(chunk) = handler.fetch().expect("fetch") {
        for row_idx in 0..chunk.size() {
            let mut row = Vec::with_capacity(chunk.column_count());
            for col_idx in 0..chunk.column_count() {
                row.push(
                    chunk
                        .get_value(col_idx, row_idx)
                        .expect("test output value"),
                );
            }
            rows.push(row);
        }
    }

    let stats = output.stats();
    assert_eq!(stats.pushed_rows, stats.popped_rows);
    assert_eq!(stats.pushed_chunks, stats.popped_chunks);
    (rows, stats, capacity)
}

fn assert_pipeline_count_at_least(statement: &StatementProgram, expected: usize) {
    let StatementProgram::Pipeline { graph, .. } = statement else {
        panic!("expected pipeline statement");
    };
    assert!(
        graph.pipelines.len() >= expected,
        "expected at least {expected} pipelines, got {}",
        graph.pipelines.len()
    );
}

fn statement_from_logical(logical: LogicalPlan) -> StatementProgram {
    let mut generator = PhysicalPlanGenerator::new(PlanBuildContext::default());
    let plan = Arc::new(generator.generate(&logical).expect("physical plan"));
    let mut lowerer = PipelineLowerer::new(plan.as_ref());
    let graph = Arc::new(
        lowerer
            .lower_to_pipeline_graph(plan.root)
            .expect("pipeline graph"),
    );
    let programs = PipelineProgramBuilder::default()
        .build_program_set(graph.as_ref())
        .expect("pipeline programs");
    StatementProgram::Pipeline {
        plan,
        graph,
        programs,
    }
}

fn hash_join_logical_plan(row_count: usize) -> LogicalPlan {
    let ctx = BindContext::new();
    let left_rows = (0..row_count)
        .map(|idx| {
            vec![
                int_constant(idx as i32),
                int_constant((idx as i32).saturating_mul(10)),
            ]
        })
        .collect::<Vec<_>>();
    let right_rows = (0..row_count)
        .map(|idx| {
            vec![
                int_constant(idx as i32),
                int_constant((idx as i32).saturating_mul(100)),
            ]
        })
        .collect::<Vec<_>>();
    let left = int_values(&ctx, 0, vec!["lk", "lv"], left_rows);
    let right = int_values(&ctx, 1, vec!["rk", "rv"], right_rows);
    let condition = JoinCondition::equality(
        Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
        Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
    );
    LogicalPlan::new(
        &ctx,
        LogicalOperator::Join(Join::comparison(
            JoinType::Inner,
            left,
            right,
            vec![condition],
        )),
    )
}

fn sort_logical_plan(row_count: usize) -> LogicalPlan {
    let ctx = BindContext::new();
    let rows = (0..row_count)
        .rev()
        .map(|idx| vec![int_constant(idx as i32)])
        .collect::<Vec<_>>();
    let values = int_values(&ctx, 0, vec!["v"], rows);
    let order = OrderByNode {
        expression: Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
        ascending: true,
        nulls_first: false,
    };
    LogicalPlan::new(
        &ctx,
        LogicalOperator::Order(LogicalOrder::new(values, vec![order])),
    )
}

fn grouped_aggregate_logical_plan(row_count: usize) -> LogicalPlan {
    let ctx = BindContext::new();
    let rows = (0..row_count)
        .map(|idx| vec![int_constant(idx as i32)])
        .collect::<Vec<_>>();
    let values = int_values(&ctx, 0, vec!["k"], rows);
    LogicalPlan::new(
        &ctx,
        LogicalOperator::Aggregate(LogicalAggregate::new(
            1,
            2,
            3,
            values,
            vec![Expression::Reference(ReferenceExpression::new(
                0,
                LogicalType::Integer,
            ))],
            Vec::new(),
            vec![Expression::Aggregate(AggregateExpression::new(
                get_count_star_function(),
                Vec::new(),
                LogicalType::BigInt,
            ))],
            Vec::new(),
        )),
    )
}

fn int_values(
    ctx: &BindContext,
    table_index: usize,
    names: Vec<&str>,
    rows: Vec<Vec<Expression>>,
) -> LogicalPlan {
    let column_count = names.len();
    LogicalPlan::new(
        ctx,
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            table_index,
            rows,
            names.into_iter().map(str::to_string).collect(),
            vec![LogicalType::Integer; column_count],
        )),
    )
}

fn int_constant(value: i32) -> Expression {
    Expression::Constant(ConstantExpression::new(
        Value::Integer(value),
        LogicalType::Integer,
    ))
}

fn materialized_statement(
    chunks: Vec<paro_common::chunk::Chunk>,
) -> (StatementProgram, BreakerHandleId) {
    let (graph, programs, handle) = materialized_graph(chunks);
    let row_type = int_row_type();
    let chunk_spec = chunk_scan_spec(Vec::new());
    let plan = Arc::new(single_node_plan(
        PhysicalNodeKind::ChunkScan(chunk_spec),
        row_type,
    ));
    (
        StatementProgram::Pipeline {
            plan,
            graph,
            programs,
        },
        handle,
    )
}

fn unsealed_materialized_statement() -> (StatementProgram, BreakerHandleId) {
    let (graph, programs, handle) = unsealed_materialized_graph();
    let plan = Arc::new(single_node_plan(
        PhysicalNodeKind::DummyScan(DummyScanSpec),
        int_row_type(),
    ));
    (
        StatementProgram::Pipeline {
            plan,
            graph,
            programs,
        },
        handle,
    )
}

fn materialized_graph(
    chunks: Vec<paro_common::chunk::Chunk>,
) -> (
    Arc<PipelineGraph>,
    crate::pipeline::PipelineProgramSet,
    BreakerHandleId,
) {
    let row_type = int_row_type();
    let mut handles = BreakerHandleCatalogBuilder::default();
    let handle = handles.register(
        BreakerHandleKind::Materialized,
        row_type.clone(),
        PipelineProperties::default(),
    );
    let chunk_spec = chunk_scan_spec(chunks);
    let graph = Arc::new(PipelineGraph {
        pipelines: vec![
            PipelineSpec {
                id: PipelineId::new(0),
                source: SourceSpec::Chunk(chunk_spec),
                transforms: Vec::new(),
                sink: SinkSpec::Materialize(MaterializeSinkSpec {
                    handle,
                    required: Default::default(),
                }),
                sink_sharing: SinkSharing::Exclusive,
                properties: PipelineProperties::default(),
                output: row_type.clone(),
            },
            PipelineSpec {
                id: PipelineId::new(1),
                source: SourceSpec::Materialized(MaterializedSourceSpec { handle }),
                transforms: Vec::new(),
                sink: SinkSpec::ClientResult(ClientResultSpec::default()),
                sink_sharing: SinkSharing::Exclusive,
                properties: PipelineProperties::default(),
                output: row_type,
            },
        ],
        dependencies: vec![PipelineDependency {
            producer: PipelineId::new(0),
            consumer: PipelineId::new(1),
            kind: DependencyKind::MaterializeBeforeRead,
        }],
        handles: handles.finish(),
        control_regions: Vec::new(),
        root: PipelineRoot::Pipeline(PipelineId::new(1)),
    });
    let programs = PipelineProgramBuilder::default()
        .build_program_set(graph.as_ref())
        .expect("pipeline programs");
    (graph, programs, handle)
}

fn unsealed_materialized_graph() -> (
    Arc<PipelineGraph>,
    crate::pipeline::PipelineProgramSet,
    BreakerHandleId,
) {
    let row_type = int_row_type();
    let mut handles = BreakerHandleCatalogBuilder::default();
    let handle = handles.register(
        BreakerHandleKind::Materialized,
        row_type.clone(),
        PipelineProperties::default(),
    );
    let graph = Arc::new(PipelineGraph {
        pipelines: vec![PipelineSpec {
            id: PipelineId::new(0),
            source: SourceSpec::Materialized(MaterializedSourceSpec { handle }),
            transforms: Vec::new(),
            sink: SinkSpec::ClientResult(ClientResultSpec::default()),
            sink_sharing: SinkSharing::Exclusive,
            properties: PipelineProperties::default(),
            output: row_type,
        }],
        dependencies: Vec::new(),
        handles: handles.finish(),
        control_regions: Vec::new(),
        root: PipelineRoot::Pipeline(PipelineId::new(0)),
    });
    let programs = PipelineProgramBuilder::default()
        .build_program_set(graph.as_ref())
        .expect("pipeline programs");
    (graph, programs, handle)
}

fn chunk_scan_spec(chunks: Vec<paro_common::chunk::Chunk>) -> ChunkScanSpec {
    ChunkScanSpec {
        chunks: Arc::from(chunks.into_boxed_slice()),
        output_names: vec!["v".to_string()].into_boxed_slice(),
        output_types: vec![LogicalType::Integer].into_boxed_slice(),
    }
}

fn int_row_type() -> RowType {
    RowType::new(vec!["v".to_string()], vec![LogicalType::Integer])
}

fn query_context(output: QueryOutputPort) -> QueryRuntimeContext {
    QueryRuntimeContext::new(
        TestStatementContextBuilder::minimal().build(),
        Arc::new(ParameterBindings::empty()),
        Arc::new(QueryMemoryPool::unbounded()),
        output,
    )
}

fn statement_context_with_cancellation(
    cancellation: StatementCancellation,
) -> Arc<StatementContext> {
    let context = TestStatementContextBuilder::minimal().build();
    let mut context =
        Arc::try_unwrap(context).expect("test statement context should have a single owner");
    context.cancellation = cancellation;
    Arc::new(context)
}

fn assert_handle_status(
    handles: &BreakerHandleRegistry,
    handle: BreakerHandleId,
    expected: CleanupStatus,
) {
    let status = handles
        .get_by_id(handle)
        .expect("registered breaker handle")
        .cleanup_status();
    assert_eq!(status, expected);
}

fn single_node_plan(kind: PhysicalNodeKind, output: RowType) -> PhysicalPlan {
    let mut nodes = PhysicalPlanNodeArena::default();
    let root = nodes.push(PhysicalPlanNode {
        id: PhysicalPlanNodeId::INVALID,
        output,
        cardinality: None,
        kind,
        children: PlanChildren::Empty,
        label: OperatorLabel::new(paro_planner::plan::PlanNodeId::SYNTHETIC, "TEST"),
    });
    PhysicalPlan::new(root, nodes, PlanChildrenArena::default(), PlanPropertyMap)
}
