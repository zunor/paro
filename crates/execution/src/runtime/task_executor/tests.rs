// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::any::Any;
use std::sync::{Arc, Mutex, OnceLock};

use paro_common::chunk::Chunk;
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_common::vector::{Vector, VECTOR_SIZE};
use paro_context::{
    NoopStatementTimeoutDriver, RuntimeLimits, StatementCancelReason, StatementCancellation,
    TestStatementContextBuilder,
};
use paro_function::aggregate::distributive::count::{get_count_function, get_count_star_function};
use paro_function::aggregate::distributive::minmax::get_max_function;
use paro_function::aggregate::distributive::sum::get_sum_function;
use paro_function::table::{
    LocalTableFunctionState, TableFunction, TableFunctionInitInput, TableFunctionInput,
    TableFunctionResult,
};
use paro_function::window::WindowFunction;
use paro_planner::binder::ir::OrderByNode;
use paro_planner::expression::{
    AggregateExpression, AggregateType, ComparisonExpression, ComparisonType, ConstantExpression,
    Expression, OrderByExpression, ReferenceExpression, WindowExpression, WindowFrame,
};
use paro_planner::operator::join::{AntiJoinMode, JoinComparisonType, JoinCondition, JoinType};

use crate::explain::profiler::{ExplainProfileSnapshot, ExplainProfiler, OperatorProfiler};
use crate::memory_runtime::QueryMemoryPool;
use crate::physical::properties::{MemoryClass, PipelineProperties, PropertyRepairKind};
use crate::physical::row_type::RowType;
use crate::physical::specs::{
    AggregateSpec, ChunkScanSpec, DummyScanSpec, EmptyResultSpec, ExpressionScanSpec, FilterSpec,
    LimitSpec, PerfectHashAggregatePlan, PostAggregateReductionSpec, ProjectSpec,
    TableFunctionScanSpec, TopNSpec, ValuesSpec, WindowSpec,
};
use crate::pipeline::graph::{
    ClientResultSpec, CrossProductBuildSinkSpec, CrossProductProbeSpec, CteMaterializeSinkSpec,
    CteScanSourceSpec, DelimCaptureSinkSpec, DelimScanSourceSpec, DependencyKind,
    HashAggregateBuildSinkSpec, HashAggregateEmitSourceSpec, HashJoinBuildSinkSpec,
    HashJoinProbeSpec, HashJoinSpillReplaySourceSpec, HashJoinUnmatchedSourceSpec,
    MaterializeSinkSpec, MaterializedSourceSpec, PerfectHashAggregateEmitSourceSpec,
    PerfectHashAggregateSinkSpec, PipelineDependency, PipelineGraph, PipelineId, PipelineRoot,
    PipelineSpec, PropertyRepairSpec, SinkSharing, SinkSpec, SortBuildSinkSpec, SortEmitSourceSpec,
    SortRangeJoinProbeSpec, SourceSpec, TopNBuildSinkSpec, TopNEmitSourceSpec, TransformSpec,
    UngroupedAggregateEmitSourceSpec, UngroupedAggregateSinkSpec, WindowBuildSinkSpec,
    WindowEmitSourceSpec,
};
use crate::pipeline::handles::{BreakerHandleCatalogBuilder, BreakerHandleId, BreakerHandleKind};
use crate::pipeline::program::PipelineProgramBuilder;
use crate::runtime::{
    BlockReason, BreakerHandleRegistry, DelimHandle, FinishTaskId, HandleRef,
    OperatorCleanupContext, OperatorFinishContext, ParallelFinishDriver, ParameterBindings,
    PipelineRuntime, PipelineTaskId, QueryErrorId, QueryOutputPort, SharedSinkCoordinator,
    SharedSinkState, WakeGeneration, WakeSource, WakeToken,
};
use tokio_util::sync::CancellationToken;

use super::*;

fn query_context(output: QueryOutputPort) -> QueryRuntimeContext {
    QueryRuntimeContext::new(
        TestStatementContextBuilder::minimal().build(),
        Arc::new(ParameterBindings::empty()),
        Arc::new(QueryMemoryPool::unbounded()),
        output,
    )
}

fn query_context_with_limits(
    output: QueryOutputPort,
    limits: RuntimeLimits,
) -> QueryRuntimeContext {
    let max_memory = limits.max_memory;
    QueryRuntimeContext::new(
        TestStatementContextBuilder::minimal()
            .with_limits(limits)
            .build(),
        Arc::new(ParameterBindings::empty()),
        Arc::new(QueryMemoryPool::new(max_memory)),
        output,
    )
}

fn unique_temp_dir(prefix: &str) -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock")
        .as_nanos();
    std::env::temp_dir()
        .join(format!("{}_{}_{}", prefix, std::process::id(), now))
        .to_string_lossy()
        .to_string()
}

fn empty_runtime(query: &QueryRuntimeContext) -> Arc<PipelineRuntime> {
    runtime_from_spec(
        query,
        PipelineSpec {
            id: PipelineId::new(0),
            source: SourceSpec::Empty(EmptyResultSpec),
            transforms: Vec::new(),
            sink: SinkSpec::ClientResult(ClientResultSpec::default()),
            sink_sharing: SinkSharing::Exclusive,
            properties: PipelineProperties::default(),
            output: RowType::new(Vec::new(), Vec::<LogicalType>::new()),
        },
    )
}

fn runtime_from_spec(query: &QueryRuntimeContext, spec: PipelineSpec) -> Arc<PipelineRuntime> {
    let program = Arc::new(
        PipelineProgramBuilder::default()
            .build_program(&spec)
            .expect("program build"),
    );
    Arc::new(
        PipelineRuntime::from_catalog(program, &Default::default(), query.params.clone(), query)
            .expect("runtime init"),
    )
}

fn runtimes_from_graph(
    query: &QueryRuntimeContext,
    graph: &PipelineGraph,
) -> (Arc<PipelineRuntime>, Arc<PipelineRuntime>) {
    let programs = PipelineProgramBuilder::default()
        .build_program_set(graph)
        .expect("program set build");
    let handles =
        Arc::new(BreakerHandleRegistry::from_catalog(&graph.handles).expect("handle registry"));
    let build = Arc::new(
        PipelineRuntime::with_registry(
            programs
                .get(PipelineId::new(0))
                .expect("build program")
                .clone(),
            handles.clone(),
            query.params.clone(),
            query,
        )
        .expect("build runtime"),
    );
    let emit = Arc::new(
        PipelineRuntime::with_registry(
            programs
                .get(PipelineId::new(1))
                .expect("emit program")
                .clone(),
            handles,
            query.params.clone(),
            query,
        )
        .expect("emit runtime"),
    );
    (build, emit)
}

fn values_spec(rows: Vec<Vec<Expression>>, types: Vec<LogicalType>) -> ValuesSpec {
    ValuesSpec {
        table_index: 0,
        expressions: rows
            .into_iter()
            .map(Vec::into_boxed_slice)
            .collect::<Vec<_>>()
            .into_boxed_slice(),
        output_names: (0..types.len())
            .map(|idx| format!("col_{idx}"))
            .collect::<Vec<_>>()
            .into_boxed_slice(),
        output_types: types.into_boxed_slice(),
    }
}

fn int_constant(value: i32) -> Expression {
    Expression::Constant(ConstantExpression::new(
        Value::Integer(value),
        LogicalType::Integer,
    ))
}

fn bigint_constant(value: i64) -> Expression {
    Expression::Constant(ConstantExpression::new(
        Value::BigInt(value),
        LogicalType::BigInt,
    ))
}

fn varchar_constant(value: &str) -> Expression {
    Expression::Constant(ConstantExpression::new(
        Value::Varchar(value.to_string()),
        LogicalType::Varchar,
    ))
}

fn null_constant(ty: LogicalType) -> Expression {
    Expression::Constant(ConstantExpression::new(Value::Null(ty.clone()), ty))
}

fn bool_constant(value: bool) -> Expression {
    Expression::Constant(ConstantExpression::new(
        Value::Boolean(value),
        LogicalType::Boolean,
    ))
}

fn reference(index: usize, ty: LogicalType) -> Expression {
    Expression::Reference(ReferenceExpression::new(index, ty))
}

fn order_by_ref(index: usize, ty: LogicalType) -> paro_planner::binder::ir::OrderByNode {
    paro_planner::binder::ir::OrderByNode {
        expression: reference(index, ty),
        ascending: true,
        nulls_first: false,
    }
}

fn ordering_on_first_column() -> crate::physical::properties::OrderingSpec {
    crate::physical::properties::OrderingSpec::new(vec![
        crate::physical::properties::OrderingColumn {
            column: 0,
            direction: crate::physical::properties::OrderingDirection::Asc,
            nulls: crate::physical::properties::NullOrdering::Last,
        },
    ])
}

fn join_condition() -> JoinCondition {
    JoinCondition::equality(
        reference(0, LogicalType::Integer),
        reference(0, LogicalType::Integer),
    )
}

fn count_star_expression() -> Expression {
    Expression::Aggregate(AggregateExpression::new(
        get_count_star_function(),
        vec![],
        LogicalType::BigInt,
    ))
}

fn grouped_count_spec(perfect_hash: Option<PerfectHashAggregatePlan>) -> AggregateSpec {
    AggregateSpec {
        grouping_key_count: 1,
        state_output_projection: Box::new([]),
        estimated_input_rows: None,
        projection_exprs: Box::new([]),
        payload_types: Box::new([]),
        groups: vec![reference(0, LogicalType::Integer)].into_boxed_slice(),
        group_key_encodings: vec![crate::physical::specs::GroupKeyEncoding::Identity]
            .into_boxed_slice(),
        grouping_sets: Box::new([]),
        aggregates: vec![count_star_expression()].into_boxed_slice(),
        grouping_functions: Box::new([]),
        aggregate_inputs: vec![Vec::<usize>::new().into_boxed_slice()].into_boxed_slice(),
        aggregate_filters: vec![None].into_boxed_slice(),
        aggregate_orders: vec![Vec::<usize>::new().into_boxed_slice()].into_boxed_slice(),
        post_reduction: None,
        having_filter: Box::new([]),
        perfect_hash,
        output_names: vec!["k".to_string(), "count".to_string()].into_boxed_slice(),
        output_types: vec![LogicalType::Integer, LogicalType::BigInt].into_boxed_slice(),
    }
}

/// Physical grouped SUM followed by a hidden MAX reduction over every
/// finalized group. The dynamic equality predicate retains all ties for the
/// global maximum without adding the reduced scalar to the public row type.
fn grouped_sum_post_max_spec(
    key_type: LogicalType,
    perfect_hash: Option<PerfectHashAggregatePlan>,
    having_filter: Box<[Expression]>,
) -> AggregateSpec {
    let (sum, _) = get_sum_function()
        .bind(&[LogicalType::Integer])
        .expect("bind sum(integer)");
    assert_eq!(sum.return_type, LogicalType::BigInt);
    let (max, _) = get_max_function()
        .bind(&[LogicalType::BigInt])
        .expect("bind max(bigint)");
    let post_reduction = PostAggregateReductionSpec {
        aggregate_types: Box::new([LogicalType::BigInt]),
        reducers: Box::new([Expression::Aggregate(AggregateExpression::new(
            max,
            vec![reference(0, LogicalType::BigInt)],
            LogicalType::BigInt,
        ))]),
        reducer_types: Box::new([LogicalType::BigInt]),
        scalar_expressions: Box::new([reference(0, LogicalType::BigInt)]),
        scalar_types: Box::new([LogicalType::BigInt]),
        predicate: Expression::Comparison(ComparisonExpression::new(
            ComparisonType::Equal,
            reference(0, LogicalType::BigInt),
            reference(1, LogicalType::BigInt),
        )),
        input_rollup_sources: None,
    };
    AggregateSpec {
        grouping_key_count: 1,
        state_output_projection: Box::new([]),
        estimated_input_rows: None,
        projection_exprs: Box::new([]),
        payload_types: Box::new([key_type.clone(), LogicalType::Integer]),
        groups: Box::new([reference(0, key_type.clone())]),
        group_key_encodings: Box::new([crate::physical::specs::GroupKeyEncoding::Identity]),
        grouping_sets: Box::new([]),
        aggregates: Box::new([Expression::Aggregate(AggregateExpression::new(
            sum,
            vec![reference(1, LogicalType::Integer)],
            LogicalType::BigInt,
        ))]),
        grouping_functions: Box::new([]),
        aggregate_inputs: Box::new([Box::new([1])]),
        aggregate_filters: Box::new([None]),
        aggregate_orders: Box::new([Box::new([])]),
        post_reduction: Some(post_reduction),
        having_filter,
        perfect_hash,
        output_names: Box::new(["k".to_string(), "sum".to_string()]),
        output_types: Box::new([key_type, LogicalType::BigInt]),
    }
}

fn ungrouped_count_spec() -> AggregateSpec {
    AggregateSpec {
        grouping_key_count: 0,
        state_output_projection: Box::new([]),
        estimated_input_rows: None,
        projection_exprs: Box::new([]),
        payload_types: Box::new([]),
        groups: Box::new([]),
        group_key_encodings: Box::new([]),
        grouping_sets: Box::new([]),
        aggregates: vec![count_star_expression()].into_boxed_slice(),
        grouping_functions: Box::new([]),
        aggregate_inputs: vec![Vec::<usize>::new().into_boxed_slice()].into_boxed_slice(),
        aggregate_filters: vec![None].into_boxed_slice(),
        aggregate_orders: vec![Vec::<usize>::new().into_boxed_slice()].into_boxed_slice(),
        post_reduction: None,
        having_filter: Box::new([]),
        perfect_hash: None,
        output_names: vec!["count".to_string()].into_boxed_slice(),
        output_types: vec![LogicalType::BigInt].into_boxed_slice(),
    }
}

fn ungrouped_distinct_count_spec() -> AggregateSpec {
    let (function, _) = get_count_function()
        .bind(&[LogicalType::Integer])
        .expect("bind count(integer)");
    AggregateSpec {
        grouping_key_count: 0,
        state_output_projection: Box::new([]),
        estimated_input_rows: None,
        projection_exprs: Box::new([]),
        payload_types: Box::new([LogicalType::Integer]),
        groups: Box::new([]),
        group_key_encodings: Box::new([]),
        grouping_sets: Box::new([]),
        aggregates: vec![Expression::Aggregate(
            AggregateExpression::new(
                function,
                vec![reference(0, LogicalType::Integer)],
                LogicalType::BigInt,
            )
            .with_aggr_type(AggregateType::Distinct),
        )]
        .into_boxed_slice(),
        grouping_functions: Box::new([]),
        aggregate_inputs: vec![vec![0].into_boxed_slice()].into_boxed_slice(),
        aggregate_filters: vec![None].into_boxed_slice(),
        aggregate_orders: vec![Vec::<usize>::new().into_boxed_slice()].into_boxed_slice(),
        post_reduction: None,
        having_filter: Box::new([]),
        perfect_hash: None,
        output_names: vec!["count".to_string()].into_boxed_slice(),
        output_types: vec![LogicalType::BigInt].into_boxed_slice(),
    }
}

fn grouped_distinct_count_spec() -> AggregateSpec {
    let (function, _) = get_count_function()
        .bind(&[LogicalType::Integer])
        .expect("bind count(integer)");
    AggregateSpec {
        grouping_key_count: 1,
        state_output_projection: Box::new([]),
        estimated_input_rows: None,
        projection_exprs: Box::new([]),
        payload_types: Box::new([LogicalType::Integer, LogicalType::Integer]),
        groups: Box::new([reference(0, LogicalType::Integer)]),
        group_key_encodings: Box::new([crate::physical::specs::GroupKeyEncoding::Identity]),
        grouping_sets: Box::new([]),
        aggregates: Box::new([Expression::Aggregate(
            AggregateExpression::new(
                function,
                vec![reference(1, LogicalType::Integer)],
                LogicalType::BigInt,
            )
            .with_aggr_type(AggregateType::Distinct),
        )]),
        grouping_functions: Box::new([]),
        aggregate_inputs: Box::new([Box::new([1])]),
        aggregate_filters: Box::new([None]),
        aggregate_orders: Box::new([Box::new([])]),
        post_reduction: None,
        having_filter: Box::new([]),
        perfect_hash: None,
        output_names: Box::new(["k".to_string(), "count".to_string()]),
        output_types: Box::new([LogicalType::Integer, LogicalType::BigInt]),
    }
}

fn run_to_done(
    executor: &mut PipelineTaskExecutor,
    query: &QueryRuntimeContext,
    thread: &ThreadContext,
    wake: &OperatorWakeScope,
    profiler: &mut OperatorProfiler,
) {
    for _ in 0..32 {
        let result = executor
            .step(&mut step_context(query, thread, wake, profiler))
            .expect("task step");
        if matches!(result, TaskStepResult::Done) {
            return;
        }
    }
    panic!("pipeline task did not finish");
}

fn step_context<'a>(
    query: &'a QueryRuntimeContext,
    thread: &'a ThreadContext,
    wake: &'a OperatorWakeScope,
    profiler: &'a mut OperatorProfiler,
) -> PipelineTaskStepContext<'a> {
    PipelineTaskStepContext {
        query,
        thread,
        wake,
        profiler,
    }
}

fn install_statement_cancellation(
    query: &mut QueryRuntimeContext,
    reason: StatementCancelReason,
) -> CancellationToken {
    let connection_token = CancellationToken::new();
    let statement_token = connection_token.child_token();
    let cancel_reason = Arc::new(OnceLock::new());
    let _ = cancel_reason.set(reason);
    query.cancellation = StatementCancellation::from_parts(
        connection_token,
        statement_token.clone(),
        None,
        cancel_reason,
        Arc::new(NoopStatementTimeoutDriver),
    );
    statement_token
}

#[path = "breaker_tests.rs"]
mod breaker_tests;
#[path = "completion_tests.rs"]
mod completion_tests;
#[path = "hash_join_spill_tests.rs"]
mod hash_join_spill_tests;
#[path = "join_tests.rs"]
mod join_tests;
#[path = "post_rollup_tests.rs"]
mod post_rollup_tests;
#[path = "running_tests.rs"]
mod running_tests;

trait QueryOutputWriteExt {
    fn assert_written(self);
}

impl QueryOutputWriteExt for QueryOutputWrite {
    fn assert_written(self) {
        assert!(matches!(self, QueryOutputWrite::Written));
    }
}
