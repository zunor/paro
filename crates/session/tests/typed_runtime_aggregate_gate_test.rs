// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

#[path = "common/exec_ok.rs"]
mod exec_ok;
#[path = "common/query_i64_col.rs"]
mod query_i64_col;

use exec_ok::exec_ok;
use paro_common::runtime_value::Value;
use paro_instance::{Instance, InstanceConfig};
use paro_session::{CollectingSink, Session, StatementCompletion};
use query_i64_col::query_i64_col;
use std::collections::BTreeMap;

#[tokio::test]
async fn group_by_low_cardinality_ordered_matches_typed_runtime_gate() {
    let instance = Instance::new_in_memory_with_config(
        InstanceConfig::in_memory().with_max_memory(4 * 1024 * 1024),
    )
    .expect("ordered aggregate test instance");
    let mut session = Session::new(1, instance);
    let mut sink = CollectingSink::new();

    exec_ok(
        &mut session,
        &mut sink,
        "CREATE TABLE bench_agg (
            id BIGINT PRIMARY KEY,
            group_low INT,
            group_high INT,
            metric INT
        )",
    )
    .await;
    exec_ok(
        &mut session,
        &mut sink,
        "INSERT INTO bench_agg
         SELECT
             i,
             (i % 10)::INT,
             (((i - 1) % 1000) + 1)::INT,
             ((i * 17) % 1000)::INT
         FROM generate_series(1, 1000) AS t(i)",
    )
    .await;

    exec_ok(
        &mut session,
        &mut sink,
        "SELECT group_low, count(*)
         FROM bench_agg
         GROUP BY group_low",
    )
    .await;

    let result = sink.assert_single_result();
    assert_eq!(result.completion, StatementCompletion::Select { rows: 10 });
    let mut counts_by_group = BTreeMap::new();
    let groups = query_i64_col(&sink, 0);
    let counts = query_i64_col(&sink, 1);
    for (group, count) in groups.into_iter().zip(counts) {
        counts_by_group.insert(group, count);
    }
    assert_eq!(
        counts_by_group,
        (0..10)
            .map(|group| (group, 100))
            .collect::<BTreeMap<_, _>>()
    );

    exec_ok(
        &mut session,
        &mut sink,
        "SELECT group_low, count(*)
         FROM bench_agg
         GROUP BY group_low
         ORDER BY group_low",
    )
    .await;

    let result = sink.assert_single_result();
    assert_eq!(result.completion, StatementCompletion::Select { rows: 10 });
    assert_eq!(query_i64_col(&sink, 0), (0..10).collect::<Vec<_>>());
    assert_eq!(query_i64_col(&sink, 1), vec![100; 10]);
}

#[tokio::test]
async fn correlated_count_restores_its_typed_empty_input_value() {
    let instance = Instance::new_in_memory_with_config(
        InstanceConfig::in_memory().with_max_memory(8 * 1024 * 1024),
    )
    .expect("correlated aggregate test instance");
    let mut session = Session::new(1, instance);
    let mut sink = CollectingSink::new();

    exec_ok(
        &mut session,
        &mut sink,
        "CREATE TABLE scalar_outer (k INT, available INT);
         CREATE TABLE scalar_inner (k INT);
         INSERT INTO scalar_outer VALUES (1, 2), (2, 2);
         INSERT INTO scalar_inner VALUES (1)",
    )
    .await;
    exec_ok(
        &mut session,
        &mut sink,
        "SELECT o.k
         FROM scalar_outer AS o
         WHERE o.available > (
             SELECT count(*) FROM scalar_inner AS i WHERE i.k = o.k
         )
         ORDER BY o.k",
    )
    .await;

    assert_eq!(query_i64_col(&sink, 0), vec![1, 2]);

    exec_ok(
        &mut session,
        &mut sink,
        "SELECT o.k,
                (SELECT CASE
                            WHEN COUNT(*) = 1 THEN NULL::BIGINT
                            ELSE COUNT(*)
                        END
                   FROM scalar_inner AS i
                  WHERE i.k = o.k)
           FROM scalar_outer AS o
          ORDER BY o.k",
    )
    .await;
    let result = sink.assert_single_result();
    let chunk = result.chunks.first().expect("correlated scalar output");
    assert_eq!(chunk.column(0).unwrap().get_value(0), Value::Integer(1));
    assert_eq!(
        chunk.column(1).unwrap().get_value(0),
        Value::Null(paro_common::types::LogicalType::BigInt)
    );
    assert_eq!(chunk.column(0).unwrap().get_value(1), Value::Integer(2));
    assert_eq!(chunk.column(1).unwrap().get_value(1), Value::BigInt(0));
}

#[tokio::test]
async fn correlated_count_wrappers_preserve_the_empty_input_value() {
    let instance = Instance::new_in_memory_with_config(
        InstanceConfig::in_memory().with_max_memory(8 * 1024 * 1024),
    )
    .expect("correlated aggregate wrapper test instance");
    let mut session = Session::new(1, instance);
    let mut sink = CollectingSink::new();
    exec_ok(
        &mut session,
        &mut sink,
        "CREATE TABLE scalar_outer_wrapped (k INT);
         CREATE TABLE scalar_inner_wrapped (k INT);
         INSERT INTO scalar_outer_wrapped VALUES (1), (2);
         INSERT INTO scalar_inner_wrapped VALUES (1)",
    )
    .await;

    exec_ok(
        &mut session,
        &mut sink,
        "SELECT o.k,
                (SELECT COUNT(*)
                   FROM scalar_inner_wrapped AS i
                  WHERE i.k = o.k
                  LIMIT 1)
           FROM scalar_outer_wrapped AS o
          ORDER BY o.k",
    )
    .await;
    assert_eq!(query_i64_col(&sink, 0), vec![1, 2]);
    assert_eq!(query_i64_col(&sink, 1), vec![1, 0]);

    exec_ok(
        &mut session,
        &mut sink,
        "SELECT o.k,
                (SELECT COUNT(*)
                   FROM scalar_inner_wrapped AS i
                  WHERE i.k = o.k
                  ORDER BY COUNT(*)
                  LIMIT 1)
           FROM scalar_outer_wrapped AS o
          ORDER BY o.k",
    )
    .await;
    assert_eq!(query_i64_col(&sink, 0), vec![1, 2]);
    assert_eq!(query_i64_col(&sink, 1), vec![1, 0]);
}

#[tokio::test]
async fn empty_grouping_set_emits_its_identity_row() {
    let instance = Instance::new_in_memory();
    let mut session = Session::new(1, instance);
    let mut sink = CollectingSink::new();

    exec_ok(&mut session, &mut sink, "CREATE TABLE empty_groups (a INT)").await;
    exec_ok(
        &mut session,
        &mut sink,
        "SELECT GROUPING(a), a, COUNT(*)
         FROM empty_groups
         GROUP BY GROUPING SETS ((a), ())",
    )
    .await;

    let result = sink.assert_single_result();
    assert_eq!(result.completion, StatementCompletion::Select { rows: 1 });
    let chunk = result.chunks.first().expect("identity output chunk");
    assert_eq!(chunk.column(0).unwrap().get_value(0), Value::BigInt(1));
    assert_eq!(
        chunk.column(1).unwrap().get_value(0),
        Value::Null(paro_common::types::LogicalType::Integer)
    );
    assert_eq!(chunk.column(2).unwrap().get_value(0), Value::BigInt(0));

    exec_ok(&mut session, &mut sink, "SET temp_directory = ''").await;
    exec_ok(
        &mut session,
        &mut sink,
        "SELECT a, b, COUNT(*)
           FROM (VALUES (1, 10), (2, 20)) AS t(a, b)
          GROUP BY GROUPING SETS ((a), (b))",
    )
    .await;
    let result = sink.assert_single_result();
    assert_eq!(result.completion, StatementCompletion::Select { rows: 4 });
    assert_eq!(query_i64_col(&sink, 2), vec![1, 1, 1, 1]);
}
