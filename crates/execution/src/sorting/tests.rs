use std::sync::Arc;

use paro_common::chunk::Chunk;
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_common::vector::{Vector, VECTOR_SIZE};
use paro_planner::binder::ir::OrderByNode;
use paro_planner::expression::{Expression, ReferenceExpression};
use paro_storage::buffer::BufferPool;

use crate::execution_context::ExecutionContext;
use crate::result_type::SourceResultType;
use crate::sorting::sort::Sort;
use crate::sorting::sorted_run::RunBuilder;
use crate::sorting::sorted_run::SortedRun;
use crate::sorting::sorted_run_merger::{
    SortedRunMerger, SortedRunMergerGlobalState, SortedRunMergerLocalState,
};
use crate::thread_context::ThreadContext;
use paro_context::{test_support::TestStatementContextBuilder, StatementContext};

fn build_int_sort() -> Sort {
    Sort::new(
        vec![OrderByNode {
            expression: Expression::Reference(ReferenceExpression {
                index: 0,
                return_type: LogicalType::Integer,
            }),
            ascending: true,
            nulls_first: false,
        }],
        vec![LogicalType::Integer, LogicalType::Varchar],
        vec![],
        false,
    )
    .unwrap()
}

fn build_varchar_sort() -> Sort {
    Sort::new(
        vec![OrderByNode {
            expression: Expression::Reference(ReferenceExpression {
                index: 0,
                return_type: LogicalType::Varchar,
            }),
            ascending: true,
            nulls_first: false,
        }],
        vec![LogicalType::Varchar, LogicalType::Integer],
        vec![],
        false,
    )
    .unwrap()
}

fn test_session() -> Arc<StatementContext> {
    TestStatementContextBuilder::minimal().build()
}

fn test_runtime() -> ExecutionContext<'static> {
    let thread = Box::leak(Box::new(ThreadContext::single_threaded()));
    ExecutionContext::new(test_session(), thread, None)
}

fn build_int_run(sort: &Sort, rows: &[(i32, &str)], external: bool) -> SortedRun {
    let buffer_pool = Arc::new(BufferPool::new(16 * 1024 * 1024));
    let mut builder = RunBuilder::new(
        Arc::clone(&buffer_pool),
        Arc::clone(sort.key_layout()),
        Arc::clone(sort.payload_layout()),
        Arc::clone(sort.sort_key_encoding()),
    );

    let mut keys = Vector::with_capacity(LogicalType::Integer, rows.len());
    let mut payloads = Vector::with_capacity(LogicalType::Varchar, rows.len());
    for (idx, (key, payload)) in rows.iter().enumerate() {
        keys.set_i32(idx, *key);
        payloads.set_string(idx, payload);
    }
    keys.set_count(rows.len());
    payloads.set_count(rows.len());

    let key_chunk = Chunk::from_vectors(vec![keys]);
    let payload_chunk = Chunk::from_vectors(vec![payloads]);
    builder.sink(&key_chunk, &payload_chunk).unwrap();
    builder.finish(external).unwrap()
}

fn build_external_run(sort: &Sort, rows: &[(&str, i32)]) -> SortedRun {
    let buffer_pool = Arc::new(BufferPool::new(16 * 1024 * 1024));
    let mut builder = RunBuilder::new(
        Arc::clone(&buffer_pool),
        Arc::clone(sort.key_layout()),
        Arc::clone(sort.payload_layout()),
        Arc::clone(sort.sort_key_encoding()),
    );

    let mut keys = Vector::with_capacity(LogicalType::Varchar, rows.len());
    let mut payloads = Vector::with_capacity(LogicalType::Integer, rows.len());
    for (idx, (key, payload)) in rows.iter().enumerate() {
        keys.set_string(idx, key);
        payloads.set_i32(idx, *payload);
    }
    keys.set_count(rows.len());
    payloads.set_count(rows.len());

    let key_chunk = Chunk::from_vectors(vec![keys]);
    let payload_chunk = Chunk::from_vectors(vec![payloads]);
    builder.sink(&key_chunk, &payload_chunk).unwrap();
    builder.finish(true).unwrap()
}

fn collect_int_pairs(chunk: &Chunk) -> Vec<(i32, String)> {
    (0..chunk.size())
        .map(|idx| {
            let key = match chunk.get_value(0, idx) {
                Some(Value::Integer(value)) => value,
                other => panic!("expected integer key, got {other:?}"),
            };
            let payload = match chunk.get_value(1, idx) {
                Some(Value::Varchar(value)) => value,
                other => panic!("expected varchar payload, got {other:?}"),
            };
            (key, payload)
        })
        .collect()
}

fn collect_varchar_rows(chunk: &Chunk) -> Vec<(String, i32)> {
    (0..chunk.size())
        .map(|idx| {
            let key = match chunk.get_value(0, idx) {
                Some(Value::Varchar(value)) => value,
                other => panic!("expected varchar key, got {other:?}"),
            };
            let payload = match chunk.get_value(1, idx) {
                Some(Value::Integer(value)) => value,
                other => panic!("expected integer payload, got {other:?}"),
            };
            (key, payload)
        })
        .collect()
}

#[test]
fn sort_builds_row_layouts_and_projection() {
    let sort = build_int_sort();

    assert_eq!(sort.key_layout().column_count(), 1);
    assert_eq!(sort.payload_layout().column_count(), 1);
    assert_eq!(sort.key_layout().types(), &[LogicalType::Integer]);
    assert_eq!(sort.payload_layout().types(), &[LogicalType::Varchar]);
    assert_eq!(sort.output_projection_columns().len(), 2);
    assert!(!sort.output_projection_columns()[0].is_payload);
    assert!(sort.output_projection_columns()[1].is_payload);
}

#[test]
fn in_memory_run_scans_sorted_rows() {
    let sort = build_int_sort();
    let run = build_int_run(&sort, &[(30, "c"), (10, "a"), (20, "b")], false);

    let mut output = Chunk::initialize(&[LogicalType::Integer, LogicalType::Varchar], VECTOR_SIZE);
    run.scan(&mut output, 0, sort.output_projection_columns())
        .unwrap();

    assert_eq!(
        collect_int_pairs(&output),
        vec![
            (10, "a".to_string()),
            (20, "b".to_string()),
            (30, "c".to_string()),
        ]
    );
    assert!(run.sort_indices().is_some());
}

#[test]
fn large_in_memory_run_builds_gather_plan_cache() {
    let sort = build_int_sort();
    let rows = (0..(VECTOR_SIZE * 3))
        .map(|idx| ((VECTOR_SIZE * 3 - idx) as i32, format!("row-{idx}")))
        .collect::<Vec<_>>();
    let row_refs = rows
        .iter()
        .map(|(key, payload)| (*key, payload.as_str()))
        .collect::<Vec<_>>();

    let run = build_int_run(&sort, &row_refs, false);
    assert!(run.has_gather_plan_cache());
}

#[test]
fn external_run_release_frontier_keeps_suffix_scan_valid() {
    let sort = build_varchar_sort();
    let run = build_external_run(&sort, &[("delta", 4), ("alpha", 1), ("charlie", 3)]);

    assert!(run.read_sort_key_at(0).is_ok());
    run.advance_release_frontier(1).unwrap();
    assert!(run.read_sort_key_at(0).is_ok());

    let mut output = Chunk::initialize(&[LogicalType::Varchar, LogicalType::Integer], VECTOR_SIZE);
    run.scan(&mut output, 1, sort.output_projection_columns())
        .unwrap();
    assert_eq!(
        collect_varchar_rows(&output),
        vec![("charlie".to_string(), 3), ("delta".to_string(), 4),]
    );
}

#[test]
fn merger_merges_variable_external_runs() {
    let sort = Arc::new(build_varchar_sort());
    let run1 = build_external_run(&sort, &[("delta", 4), ("alpha", 1)]);
    let run2 = build_external_run(&sort, &[("charlie", 3), ("bravo", 2)]);
    let merger = SortedRunMerger::new(Arc::clone(&sort), vec![run1, run2], 2, true);
    let gstate = SortedRunMergerGlobalState::new(merger.total_count(), 2, true, 1);
    let mut lstate = SortedRunMergerLocalState::new();

    let mut all_rows = Vec::new();
    loop {
        let mut output =
            Chunk::initialize(&[LogicalType::Varchar, LogicalType::Integer], VECTOR_SIZE);
        let result = merger.get_data(&mut output, &gstate, &mut lstate).unwrap();
        all_rows.extend(collect_varchar_rows(&output));
        if result == SourceResultType::Finished {
            break;
        }
    }

    assert_eq!(
        all_rows,
        vec![
            ("alpha".to_string(), 1),
            ("bravo".to_string(), 2),
            ("charlie".to_string(), 3),
            ("delta".to_string(), 4),
        ]
    );
}

#[test]
fn sort_get_data_initializes_empty_output_chunk() {
    let ctx = test_runtime();
    let sort = build_int_sort();
    let gstate = sort.get_global_sink_state(&ctx).unwrap();
    let mut lstate = sort.get_local_sink_state(&ctx).unwrap();
    let input = Chunk::from_vectors(vec![
        Vector::from_i32(&[30, 10, 20]),
        Vector::from_strings(&["c", "a", "b"]),
    ]);

    sort.sink(&ctx, &input, gstate.as_ref(), lstate.as_mut())
        .unwrap();
    sort.combine(&ctx, gstate.as_ref(), lstate.as_mut())
        .unwrap();
    sort.finalize(gstate.as_ref()).unwrap();

    let source = sort.get_global_source_state(&ctx, gstate.as_ref()).unwrap();
    let mut local_source = sort.get_local_source_state(&ctx, source.as_ref()).unwrap();
    let mut output = Chunk::new();
    let result = sort
        .get_data(&ctx, &mut output, source.as_ref(), local_source.as_mut())
        .unwrap();

    assert_eq!(result, SourceResultType::Finished);
    assert_eq!(
        collect_int_pairs(&output),
        vec![
            (10, "a".to_string()),
            (20, "b".to_string()),
            (30, "c".to_string()),
        ]
    );
}
