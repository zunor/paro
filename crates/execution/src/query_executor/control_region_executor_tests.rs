// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use paro_common::types::LogicalType;
use paro_common::vector::Vector;
use paro_context::TestStatementContextBuilder;
use paro_planner::expression::{Expression, ReferenceExpression};
use paro_planner::operator::{ExplainFormat, ExplainSpec};
use paro_planner::plan::PlanNodeId;

use crate::memory_runtime::QueryMemoryPool;
use crate::physical::children::{PlanChildren, PlanChildrenArena};
use crate::physical::ids::PhysicalPlanNodeId;
use crate::physical::node::{OperatorLabel, PhysicalPlanNode};
use crate::physical::plan::{PhysicalPlan, PhysicalPlanNodeArena};
use crate::physical::properties::{PipelineProperties, PlanPropertyMap};
use crate::physical::specs::PhysicalNodeKind;
use crate::physical::{ChunkScanSpec, DummyScanSpec, RowType};
use crate::pipeline::graph::{
    ClientResultSpec, ControlRegion, ControlRegionId, CorrelatedSubqueryRegion,
    DelimCaptureSinkSpec, DelimJoinSide, DelimScanSourceSpec, DependencyKind, PipelineDependency,
    PipelineGraph, PipelineId, PipelineRoot, PipelineSpec, RecursiveCteDedup, RecursiveCteRegion,
    RecursiveTableAppendSinkSpec, RecursiveTableScanSourceSpec, RecursiveTermination, SinkSharing,
    SinkSpec, SourceSpec,
};
use crate::pipeline::handles::{BreakerHandleCatalogBuilder, BreakerHandleKind};
use crate::pipeline::{PipelineProgramBuilder, StatementProgram};
use crate::query_executor::program_executor::{
    start_program_with_output_for_test, ProgramExecution,
};
use crate::runtime::{ParameterBindings, QueryOutputPort, RUNTIME_WAIT_TIMEOUT};

#[test]
fn recursive_control_region_root_uses_bounded_background_output() {
    let allocator = paro_common::test_utils::test_allocator();
    let statement = recursive_control_region_statement();
    let mut execution = start_program_with_output_for_test(
        TestStatementContextBuilder::minimal().build(),
        &statement,
        Arc::new(ParameterBindings::empty()),
        Arc::new(QueryMemoryPool::unbounded()),
        allocator,
        QueryOutputPort::bounded_with_stats(2),
        QueryOutputPort::unbounded_with_stats(),
        true,
    )
    .expect("control-region program starts");

    assert!(
        execution.background.is_some(),
        "control-region roots should use a background bounded-output producer"
    );
    assert_eq!(execution.query.output.capacity(), 2);
    assert_eq!(collect_i32_background_output(&mut execution), vec![1, 2, 3]);

    let stats = execution.query.output.stats();
    assert_eq!(stats.pushed_rows, 3);
    assert!(stats.peak_queue_chunks <= 2);
}

#[test]
fn correlated_control_region_root_uses_bounded_background_output() {
    let allocator = paro_common::test_utils::test_allocator();
    let statement = correlated_control_region_statement();
    let mut execution = start_program_with_output_for_test(
        TestStatementContextBuilder::minimal().build(),
        &statement,
        Arc::new(ParameterBindings::empty()),
        Arc::new(QueryMemoryPool::unbounded()),
        allocator,
        QueryOutputPort::bounded_with_stats(2),
        QueryOutputPort::unbounded_with_stats(),
        true,
    )
    .expect("correlated control-region program starts");

    assert!(
        execution.background.is_some(),
        "correlated control regions should use a background bounded-output producer"
    );
    assert_eq!(execution.query.output.capacity(), 2);
    assert_eq!(collect_i32_background_output(&mut execution), vec![1, 2, 3]);

    let stats = execution.query.output.stats();
    assert_eq!(stats.pushed_rows, 3);
    assert!(stats.peak_queue_chunks <= 2);
}

#[test]
fn explain_analyze_recursive_control_region_reports_iteration_profile() {
    let allocator = paro_common::test_utils::test_allocator();
    let statement = StatementProgram::ExplainAnalyze {
        target: Box::new(recursive_control_region_statement()),
        spec: ExplainSpec::text_analyze(),
    };
    let execution = start_program_with_output_for_test(
        TestStatementContextBuilder::minimal().build(),
        &statement,
        Arc::new(ParameterBindings::empty()),
        Arc::new(QueryMemoryPool::unbounded()),
        allocator,
        QueryOutputPort::bounded_with_stats(2),
        QueryOutputPort::unbounded_with_stats(),
        true,
    )
    .expect("EXPLAIN ANALYZE control-region program starts");

    let text = collect_string_output(&execution).join("\n");
    assert!(
        text.contains("CONTROL_REGION 0 RECURSIVE_CTE (iterations=1 termination=empty_delta)"),
        "{text}"
    );
    assert!(
        text.contains("  ITERATION 1 (delta_rows=3 working_rows=3)"),
        "{text}"
    );
}

#[test]
fn explain_analyze_recursive_control_region_reports_json_profile() {
    let allocator = paro_common::test_utils::test_allocator();
    let statement = StatementProgram::ExplainAnalyze {
        target: Box::new(recursive_control_region_statement()),
        spec: ExplainSpec {
            format: ExplainFormat::Json,
            ..ExplainSpec::text_analyze()
        },
    };
    let execution = start_program_with_output_for_test(
        TestStatementContextBuilder::minimal().build(),
        &statement,
        Arc::new(ParameterBindings::empty()),
        Arc::new(QueryMemoryPool::unbounded()),
        allocator,
        QueryOutputPort::bounded_with_stats(2),
        QueryOutputPort::unbounded_with_stats(),
        true,
    )
    .expect("EXPLAIN ANALYZE JSON control-region program starts");

    let lines = collect_string_output(&execution);
    assert_eq!(lines.len(), 1);
    let value: serde_json::Value = serde_json::from_str(&lines[0]).expect("json explain");
    let region = &value["control_regions"][0];
    assert_eq!(region["kind"], "recursive_cte");
    assert_eq!(region["region"], 0);
    assert_eq!(region["iterations"], 1);
    assert_eq!(region["termination"], "empty_delta");
    assert_eq!(region["iteration_stats"][0]["iteration"], 1);
    assert_eq!(region["iteration_stats"][0]["delta_rows"], 3);
    assert_eq!(region["iteration_stats"][0]["working_rows"], 3);
}

#[test]
fn explain_analyze_recursive_control_region_reports_max_iteration_profile() {
    let allocator = paro_common::test_utils::test_allocator();
    let statement = StatementProgram::ExplainAnalyze {
        target: Box::new(recursive_control_region_statement_with_termination(
            RecursiveTermination::MaxIterations(2),
        )),
        spec: ExplainSpec::text_analyze(),
    };
    let execution = start_program_with_output_for_test(
        TestStatementContextBuilder::minimal().build(),
        &statement,
        Arc::new(ParameterBindings::empty()),
        Arc::new(QueryMemoryPool::unbounded()),
        allocator,
        QueryOutputPort::bounded_with_stats(2),
        QueryOutputPort::unbounded_with_stats(),
        true,
    )
    .expect("EXPLAIN ANALYZE max-iteration control-region program starts");

    let text = collect_string_output(&execution).join("\n");
    assert!(
        text.contains("CONTROL_REGION 0 RECURSIVE_CTE (iterations=2 termination=max_iterations)"),
        "{text}"
    );
    assert!(
        text.contains("  ITERATION 1 (delta_rows=3 working_rows=3)"),
        "{text}"
    );
    assert!(
        text.contains("  ITERATION 2 (delta_rows=0 working_rows=0)"),
        "{text}"
    );
}

fn recursive_control_region_statement() -> StatementProgram {
    recursive_control_region_statement_with_termination(RecursiveTermination::UntilEmpty)
}

fn recursive_control_region_statement_with_termination(
    termination: RecursiveTermination,
) -> StatementProgram {
    let row_type = int_row_type();
    let mut handles = BreakerHandleCatalogBuilder::default();
    let working = handles.register(
        BreakerHandleKind::RecursiveTable,
        row_type.clone(),
        PipelineProperties::default(),
    );
    let intermediate = handles.register(
        BreakerHandleKind::RecursiveTable,
        row_type.clone(),
        PipelineProperties::default(),
    );
    let accumulated = handles.register(
        BreakerHandleKind::RecursiveTable,
        row_type.clone(),
        PipelineProperties::default(),
    );

    let graph = Arc::new(PipelineGraph {
        pipelines: vec![
            PipelineSpec {
                id: PipelineId::new(0),
                source: SourceSpec::Chunk(chunk_scan_spec(vec![i32_chunk(&[1, 2, 3])])),
                transforms: Vec::new(),
                sink: SinkSpec::RecursiveTableAppend(RecursiveTableAppendSinkSpec {
                    handle: intermediate,
                    required: Default::default(),
                }),
                sink_sharing: SinkSharing::Exclusive,
                properties: PipelineProperties::default(),
                output: row_type.clone(),
            },
            PipelineSpec {
                id: PipelineId::new(1),
                source: SourceSpec::Chunk(chunk_scan_spec(Vec::new())),
                transforms: Vec::new(),
                sink: SinkSpec::RecursiveTableAppend(RecursiveTableAppendSinkSpec {
                    handle: intermediate,
                    required: Default::default(),
                }),
                sink_sharing: SinkSharing::Exclusive,
                properties: PipelineProperties::default(),
                output: row_type.clone(),
            },
            PipelineSpec {
                id: PipelineId::new(2),
                source: SourceSpec::RecursiveTableScan(RecursiveTableScanSourceSpec {
                    handle: accumulated,
                }),
                transforms: Vec::new(),
                sink: SinkSpec::ClientResult(ClientResultSpec::default()),
                sink_sharing: SinkSharing::Exclusive,
                properties: PipelineProperties::default(),
                output: row_type.clone(),
            },
        ],
        dependencies: vec![
            PipelineDependency {
                producer: PipelineId::new(0),
                consumer: PipelineId::new(1),
                kind: DependencyKind::LoopEntry(ControlRegionId::new(0)),
            },
            PipelineDependency {
                producer: PipelineId::new(1),
                consumer: PipelineId::new(1),
                kind: DependencyKind::LoopBack(ControlRegionId::new(0)),
            },
        ],
        handles: handles.finish(),
        control_regions: vec![ControlRegion::RecursiveCte(RecursiveCteRegion {
            anchor: PipelineId::new(0),
            recursive: vec![PipelineId::new(1)],
            emit: PipelineId::new(2),
            working,
            intermediate,
            accumulated: Some(accumulated),
            termination,
            dedup: RecursiveCteDedup::None,
        })],
        root: PipelineRoot::ControlRegion(ControlRegionId::new(0)),
    });
    statement_from_graph(graph, row_type)
}

fn correlated_control_region_statement() -> StatementProgram {
    let row_type = int_row_type();
    let mut handles = BreakerHandleCatalogBuilder::default();
    let delim_values = handles.register(
        BreakerHandleKind::Delim,
        row_type.clone(),
        PipelineProperties::default(),
    );

    let graph = Arc::new(PipelineGraph {
        pipelines: vec![
            PipelineSpec {
                id: PipelineId::new(0),
                source: SourceSpec::Chunk(chunk_scan_spec(vec![i32_chunk(&[1, 2, 2, 3])])),
                transforms: Vec::new(),
                sink: SinkSpec::DelimCapture(DelimCaptureSinkSpec {
                    handle: delim_values,
                    duplicate_keys: vec![Expression::Reference(ReferenceExpression::new(
                        0,
                        LogicalType::Integer,
                    ))]
                    .into_boxed_slice(),
                    cached_outer: None,
                    required: Default::default(),
                }),
                sink_sharing: SinkSharing::Exclusive,
                properties: PipelineProperties::default(),
                output: row_type.clone(),
            },
            PipelineSpec {
                id: PipelineId::new(1),
                source: SourceSpec::DelimScan(DelimScanSourceSpec {
                    handle: delim_values,
                }),
                transforms: Vec::new(),
                sink: SinkSpec::ClientResult(ClientResultSpec::default()),
                sink_sharing: SinkSharing::Exclusive,
                properties: PipelineProperties::default(),
                output: row_type.clone(),
            },
        ],
        dependencies: Vec::new(),
        handles: handles.finish(),
        control_regions: vec![ControlRegion::CorrelatedSubquery(
            CorrelatedSubqueryRegion {
                side: DelimJoinSide::Left,
                capture: PipelineId::new(0),
                dependent_roots: Vec::new(),
                join: PipelineId::new(1),
                delim_values,
                cached_outer: None,
            },
        )],
        root: PipelineRoot::ControlRegion(ControlRegionId::new(0)),
    });
    statement_from_graph(graph, row_type)
}

fn statement_from_graph(graph: Arc<PipelineGraph>, row_type: RowType) -> StatementProgram {
    let programs = PipelineProgramBuilder::default()
        .build_program_set(graph.as_ref())
        .expect("pipeline programs");
    StatementProgram::Pipeline {
        plan: Arc::new(single_node_plan(
            PhysicalNodeKind::DummyScan(DummyScanSpec),
            row_type,
        )),
        graph,
        programs,
    }
}

fn collect_i32_background_output(execution: &mut ProgramExecution) -> Vec<i32> {
    let mut values = Vec::new();
    loop {
        while let Some(chunk) = execution.query.output.pop_front() {
            for row in 0..chunk.size() {
                values.push(chunk.column(0).unwrap().get_i32(row).unwrap());
            }
        }
        let Some(background) = execution.background.as_ref() else {
            break;
        };
        if background.is_finished() {
            execution
                .background
                .as_mut()
                .expect("background checked")
                .join()
                .expect("background control-region execution");
            break;
        }
        execution
            .query
            .output
            .wait_for_change_timeout(RUNTIME_WAIT_TIMEOUT);
    }
    values
}

fn collect_string_output(execution: &ProgramExecution) -> Vec<String> {
    let mut values = Vec::new();
    while let Some(chunk) = execution.query.output.pop_front() {
        for row in 0..chunk.size() {
            values.push(
                chunk
                    .column(0)
                    .unwrap()
                    .get_string(row)
                    .unwrap()
                    .to_string(),
            );
        }
    }
    values
}

fn i32_chunk(values: &[i32]) -> paro_common::chunk::Chunk {
    paro_common::test_utils::test_chunk_from_vectors(vec![Vector::try_from_i32(
        values,
        paro_common::test_utils::test_allocator(),
    )
    .expect("vector")])
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

fn single_node_plan(kind: PhysicalNodeKind, output: RowType) -> PhysicalPlan {
    let mut nodes = PhysicalPlanNodeArena::default();
    let root = nodes.push(PhysicalPlanNode {
        id: PhysicalPlanNodeId::INVALID,
        output,
        cardinality: None,
        kind,
        children: PlanChildren::Empty,
        label: OperatorLabel::new(PlanNodeId::SYNTHETIC, "TEST"),
    });
    PhysicalPlan::new(root, nodes, PlanChildrenArena::default(), PlanPropertyMap)
}
