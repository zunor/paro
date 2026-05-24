// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

#[path = "common/exec_ok.rs"]
mod exec_ok;
#[path = "common/query_i64_col.rs"]
mod query_i64_col;

use exec_ok::exec_ok;
use paro_instance::Instance;
use paro_session::{CollectingSink, Session, StatementCompletion};
use query_i64_col::query_i64_col;
use std::collections::BTreeMap;

#[tokio::test]
async fn group_by_low_cardinality_ordered_matches_typed_runtime_gate() {
    let instance = Instance::new_in_memory();
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
