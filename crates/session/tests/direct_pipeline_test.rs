// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

#[path = "common/exec_ok.rs"]
mod exec_ok;
#[path = "common/query_i64_col.rs"]
mod query_i64_col;

use exec_ok::exec_ok;
use paro_common::runtime_value::Value;
use paro_instance::{Instance, InstanceConfig};
use paro_session::{CollectingSink, Session};
use query_i64_col::query_i64_col;

fn rows(sink: &CollectingSink) -> Vec<Vec<Value>> {
    let result = sink.assert_single_result();
    result
        .chunks
        .iter()
        .flat_map(|chunk| {
            (0..chunk.len()).map(|row| {
                (0..result.types.len())
                    .map(|column| chunk.column(column).unwrap().get_value(row))
                    .collect()
            })
        })
        .collect()
}

#[tokio::test]
async fn direct_pipeline_executes_relational_boundaries_without_memo() {
    let instance = Instance::new_in_memory_with_config(
        InstanceConfig::in_memory().with_max_memory(256 * 1024 * 1024),
    )
    .unwrap();
    let mut session = Session::new(1, instance);
    let mut sink = CollectingSink::new();
    for sql in [
        "SET threads=2",
        "CREATE TABLE pipe_dim(k INT PRIMARY KEY, label VARCHAR)",
        "CREATE TABLE pipe_fact(k INT, v INT)",
        "INSERT INTO pipe_dim VALUES (1,'shared'),(2,'shared'),(3,'other'),(4,'empty')",
        "INSERT INTO pipe_fact VALUES (1,10),(1,20),(2,3),(3,NULL),(NULL,4)",
    ] {
        exec_ok(&mut session, &mut sink, sql).await;
    }
    for sql in [
        "SELECT d.label, SUM(f.v)::BIGINT AS s, COUNT(*) AS n FROM pipe_fact f JOIN pipe_dim d ON f.k=d.k GROUP BY d.label ORDER BY d.label",
        "SELECT d.k, COUNT(f.v) AS n FROM pipe_dim d LEFT JOIN pipe_fact f ON f.k=d.k GROUP BY d.k ORDER BY d.k",
        "SELECT k FROM pipe_dim d WHERE EXISTS (SELECT 1 FROM pipe_fact f WHERE f.k=d.k AND f.v>2) ORDER BY k",
        "SELECT k FROM pipe_dim WHERE k NOT IN (SELECT k FROM pipe_fact) ORDER BY k",
        "SELECT k, (SELECT COUNT(*) FROM pipe_fact f WHERE f.k=d.k) AS n FROM pipe_dim d ORDER BY k",
        "SELECT DISTINCT k FROM pipe_fact ORDER BY k NULLS FIRST",
        "SELECT COUNT(DISTINCT k) AS n, SUM(DISTINCT v)::BIGINT AS s FROM pipe_fact",
        "WITH p AS MATERIALIZED (SELECT k,v FROM pipe_fact UNION ALL SELECT k,v FROM pipe_fact) SELECT a.k,SUM(a.v+b.v)::BIGINT AS s FROM p a JOIN p b ON a.k=b.k WHERE a.v>5 AND b.v<30 GROUP BY a.k ORDER BY a.k",
    ] {
        exec_ok(&mut session, &mut sink, "SET optimizer_search_policy='quality'").await;
        exec_ok(&mut session, &mut sink, sql).await;
        let expected = rows(&sink);
        let types = sink.assert_single_result().types.clone();
        let names = sink.assert_single_result().names.clone();
        exec_ok(&mut session, &mut sink, "SET optimizer_search_policy='pipeline'").await;
        exec_ok(&mut session, &mut sink, sql).await;
        assert_eq!(rows(&sink), expected, "{sql}");
        assert_eq!(sink.assert_single_result().types, types, "{sql}");
        assert_eq!(sink.assert_single_result().names, names, "{sql}");
    }
    // Independent expected value: two dimension keys sharing one label must
    // merge into one SQL group, not escape as separate partial groups.
    exec_ok(&mut session, &mut sink, "SELECT SUM(f.v)::BIGINT FROM pipe_fact f JOIN pipe_dim d ON f.k=d.k WHERE d.label='shared' GROUP BY d.label").await;
    assert_eq!(query_i64_col(&sink, 0), vec![33]);
}

#[tokio::test]
async fn direct_pipeline_keeps_real_external_cte_execution() {
    let instance = Instance::new_in_memory_with_config(
        InstanceConfig::in_memory().with_max_memory(32 * 1024 * 1024),
    )
    .unwrap();
    let mut session = Session::new(1, instance);
    let mut sink = CollectingSink::new();
    for sql in [
        "SET threads=1",
        "CREATE TABLE pipe_spill(k INT)",
        "INSERT INTO pipe_spill SELECT (i % 10)::INT FROM generate_series(1,10000) AS t(i)",
        "SET optimizer_search_policy='pipeline'",
        "SET force_external=true",
    ] {
        exec_ok(&mut session, &mut sink, sql).await;
    }
    exec_ok(
        &mut session,
        &mut sink,
        "WITH p AS MATERIALIZED (SELECT k,COUNT(*) AS n FROM pipe_spill GROUP BY k)
        SELECT SUM(a.n+b.n)::BIGINT FROM p a JOIN p b ON a.k=b.k",
    )
    .await;
    assert_eq!(query_i64_col(&sink, 0), vec![20_000]);
}
