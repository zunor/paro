// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

#[path = "common/exec_ok.rs"]
mod exec_ok;
#[path = "common/query_i64_col.rs"]
mod query_i64_col;

use exec_ok::exec_ok;
use paro_instance::{Instance, InstanceConfig};
use paro_session::{CollectingSink, Session};
use query_i64_col::query_i64_col;

#[tokio::test]
async fn shared_cte_over_snapshot_unbounded_input_is_executable() {
    let instance = Instance::new_in_memory_with_config(
        InstanceConfig::in_memory().with_max_memory(16 * 1024 * 1024),
    )
    .expect("materialized CTE test instance");
    let mut session = Session::new(1, instance);
    let mut sink = CollectingSink::new();

    exec_ok(&mut session, &mut sink, "SET threads = 4").await;
    exec_ok(
        &mut session,
        &mut sink,
        "CREATE TABLE cte_input (k INT);
         INSERT INTO cte_input
         SELECT (i % 10)::INT FROM generate_series(1, 10000) AS t(i)",
    )
    .await;
    exec_ok(
        &mut session,
        &mut sink,
        "WITH shared AS MATERIALIZED (
             SELECT k, COUNT(*) AS n FROM cte_input GROUP BY k
         )
         SELECT SUM(left_side.n + right_side.n)::BIGINT
         FROM shared AS left_side
         JOIN shared AS right_side ON left_side.k = right_side.k",
    )
    .await;

    assert_eq!(query_i64_col(&sink, 0), vec![20_000]);

    exec_ok(&mut session, &mut sink, "SET force_external = true").await;
    exec_ok(
        &mut session,
        &mut sink,
        "WITH shared AS MATERIALIZED (
             SELECT k, COUNT(*) AS n FROM cte_input GROUP BY k
         )
         SELECT SUM(left_side.n + right_side.n)::BIGINT
         FROM shared AS left_side
         JOIN shared AS right_side ON left_side.k = right_side.k",
    )
    .await;

    assert_eq!(query_i64_col(&sink, 0), vec![20_000]);

    exec_ok(&mut session, &mut sink, "SET force_external = false").await;
    exec_ok(
        &mut session,
        &mut sink,
        "CREATE TABLE cte_dimension (k INT);
         INSERT INTO cte_dimension
         SELECT i::INT FROM generate_series(0, 9) AS t(i)",
    )
    .await;
    exec_ok(
        &mut session,
        &mut sink,
        "WITH first_stage AS MATERIALIZED (
             SELECT k FROM cte_input
         ), second_stage AS MATERIALIZED (
             SELECT first_stage.k
             FROM first_stage
             JOIN cte_dimension ON first_stage.k = cte_dimension.k
         )
         SELECT COUNT(*)::BIGINT FROM second_stage",
    )
    .await;

    assert_eq!(query_i64_col(&sink, 0), vec![10_000]);
}
