// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[test]
fn topn_and_window_stream_through_typed_transforms() {
    let output = QueryOutputPort::unbounded();
    let query = query_context(output.clone());
    let spec = PipelineSpec {
        id: PipelineId::new(0),
        source: SourceSpec::Values(values_spec(
            vec![
                vec![int_constant(3)],
                vec![int_constant(1)],
                vec![int_constant(2)],
            ],
            vec![LogicalType::Integer],
        )),
        transforms: vec![TransformSpec::StreamingTopN(TopNSpec {
            orders: vec![OrderByNode {
                expression: reference(0, LogicalType::Integer),
                ascending: true,
                nulls_first: true,
            }]
            .into_boxed_slice(),
            limit: 2,
            offset: 0,
            hnsw_ef_hint: None,
            output_names: vec!["v".to_string()].into_boxed_slice(),
            output_types: vec![LogicalType::Integer].into_boxed_slice(),
        })],
        sink: SinkSpec::ClientResult(ClientResultSpec::default()),
        sink_sharing: SinkSharing::Exclusive,
        properties: PipelineProperties::default(),
        output: RowType::new(vec!["v".to_string()], vec![LogicalType::Integer]),
    };
    let runtime = runtime_from_spec(&query, spec);
    let task = runtime
        .create_task_state(&query, paro_common::test_utils::test_allocator())
        .expect("task state");
    let mut executor = PipelineTaskExecutor::new(runtime, task);
    let thread = ThreadContext::single_threaded();
    let wake = OperatorWakeScope {
        task_id: PipelineTaskId(17),
        generation: WakeGeneration(0),
    };
    let mut profiler = OperatorProfiler::disabled();
    run_to_done(&mut executor, &query, &thread, &wake, &mut profiler);
    assert!(query.memory.metadata_bytes() > 0);
    assert!(query.memory.non_revocable_bytes() > 0);
    assert_eq!(query.memory.revocable_bytes(), 0);
    let chunk = output.pop_front().expect("topn output");
    assert_eq!(chunk.size(), 2);
    assert_eq!(chunk.column(0).unwrap().get_i32(0), Some(1));
    assert_eq!(chunk.column(0).unwrap().get_i32(1), Some(2));
    drop(executor);
    assert!(query.memory.non_revocable_bytes() > 0);
    drop(chunk);
    assert_eq!(query.memory.issued_bytes(), 0);

    let output = QueryOutputPort::unbounded();
    let query = query_context(output.clone());
    let row_number = WindowFunction::row_number();
    let spec = PipelineSpec {
        id: PipelineId::new(0),
        source: SourceSpec::Values(values_spec(
            vec![vec![int_constant(10)], vec![int_constant(20)]],
            vec![LogicalType::Integer],
        )),
        transforms: vec![TransformSpec::StreamingWindow(WindowSpec {
            window_index: 0,
            expressions: vec![WindowExpression::native(
                row_number.clone(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
                WindowFrame::get_default_frame(&row_number),
                false,
            )]
            .into_boxed_slice(),
            input_width: 1,
            output_names: vec!["v".to_string(), "rn".to_string()].into_boxed_slice(),
            output_types: vec![LogicalType::Integer, LogicalType::BigInt].into_boxed_slice(),
        })],
        sink: SinkSpec::ClientResult(ClientResultSpec::default()),
        sink_sharing: SinkSharing::Exclusive,
        properties: PipelineProperties::default(),
        output: RowType::new(
            vec!["v".to_string(), "rn".to_string()],
            vec![LogicalType::Integer, LogicalType::BigInt],
        ),
    };
    let runtime = runtime_from_spec(&query, spec);
    let task = runtime
        .create_task_state(&query, paro_common::test_utils::test_allocator())
        .expect("task state");
    let mut executor = PipelineTaskExecutor::new(runtime, task);
    let mut profiler = OperatorProfiler::disabled();
    run_to_done(&mut executor, &query, &thread, &wake, &mut profiler);
    let chunk = output.pop_front().expect("window output");
    assert_eq!(chunk.column(0).unwrap().get_i32(0), Some(10));
    assert_eq!(chunk.column(1).unwrap().get_i64(0), Some(1));
    assert_eq!(chunk.column(0).unwrap().get_i32(1), Some(20));
    assert_eq!(chunk.column(1).unwrap().get_i64(1), Some(2));
}
