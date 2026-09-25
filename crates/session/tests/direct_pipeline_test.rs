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
async fn order_by_result_namespace_is_not_the_input_namespace() {
    let mut session = Session::new(
        11,
        Instance::new_in_memory_with_config(
            InstanceConfig::in_memory().with_max_memory(256 * 1024 * 1024),
        )
        .unwrap(),
    );
    let mut sink = CollectingSink::new();
    for sql in [
        "CREATE TABLE order_a(item_id INT, v INT)",
        "CREATE TABLE order_b(item_id INT, v INT)",
        "INSERT INTO order_a VALUES (2,10),(1,20)",
        "INSERT INTO order_b VALUES (4,10),(3,20)",
    ] {
        exec_ok(&mut session, &mut sink, sql).await;
    }
    for repetition in 0..2 {
        for sql in [
            "SELECT a.item_id FROM order_a a JOIN order_b b ON a.v=b.v ORDER BY item_id",
            "WITH a AS (SELECT item_id,v FROM order_a), b AS (SELECT item_id,v FROM order_b) SELECT a.item_id FROM a,b WHERE a.v=b.v ORDER BY item_id",
            "SELECT a.item_id AS v FROM order_a a ORDER BY v",
            "SELECT a.item_id AS \"MixedName\" FROM order_a a ORDER BY \"MixedName\"",
            "SELECT a.item_id,a.item_id FROM order_a a ORDER BY item_id",
        ] {
            exec_ok(&mut session, &mut sink, sql).await;
            assert_eq!(query_i64_col(&sink, 0), vec![1,2], "{repetition}: {sql}");
        }
        // A compound ORDER expression binds its inputs, not output aliases.
        exec_ok(
            &mut session,
            &mut sink,
            "SELECT item_id AS v FROM order_a ORDER BY v+0",
        )
        .await;
        assert_eq!(query_i64_col(&sink, 0), vec![2, 1]);
        for sql in [
            "SELECT a.item_id,b.item_id FROM order_a a JOIN order_b b ON a.v=b.v ORDER BY item_id",
            "SELECT a.item_id FROM order_a a JOIN order_b b ON a.v=b.v WHERE item_id=1",
        ] {
            sink.clear();
            assert!(
                session.execute_simple_query(sql, &mut sink).await.is_err(),
                "{repetition}: {sql}"
            );
        }
    }
}

#[tokio::test]
async fn pipeline_writes_preserve_input_and_transaction_contracts() {
    let mut session = Session::new(
        12,
        Instance::new_in_memory_with_config(
            InstanceConfig::in_memory().with_max_memory(256 * 1024 * 1024),
        )
        .unwrap(),
    );
    let mut sink = CollectingSink::new();
    for sql in [
        "SET optimizer_verify=true",
        "CREATE TABLE pipe_write(k INT PRIMARY KEY, v INT)",
        "INSERT INTO pipe_write VALUES (1,10),(2,20),(3,30)",
        "INSERT INTO pipe_write SELECT k+3,v FROM pipe_write",
        "UPDATE pipe_write SET v=v+1 WHERE k<4",
        "BEGIN",
        "DELETE FROM pipe_write WHERE k>3",
        "ROLLBACK",
    ] {
        exec_ok(&mut session, &mut sink, sql).await;
    }
    exec_ok(
        &mut session,
        &mut sink,
        "SELECT k,v FROM pipe_write ORDER BY k",
    )
    .await;
    assert_eq!(
        rows(&sink),
        [(1, 11), (2, 21), (3, 31), (4, 10), (5, 20), (6, 30)]
            .into_iter()
            .map(|(k, v)| vec![Value::Integer(k), Value::Integer(v)])
            .collect::<Vec<_>>()
    );
    exec_ok(
        &mut session,
        &mut sink,
        "UPDATE pipe_write SET k=k+10 WHERE k<4",
    )
    .await;
    exec_ok(
        &mut session,
        &mut sink,
        "SELECT k FROM pipe_write WHERE k>10",
    )
    .await;
    let mut keys = query_i64_col(&sink, 0);
    keys.sort();
    assert_eq!(keys, vec![11, 12, 13]);
    exec_ok(
        &mut session,
        &mut sink,
        "SELECT v FROM pipe_write WHERE k>10",
    )
    .await;
    let mut values = query_i64_col(&sink, 0);
    values.sort();
    assert_eq!(values, vec![11, 21, 31]);
    exec_ok(&mut session, &mut sink, "DELETE FROM pipe_write WHERE k>10").await;
    sink.clear();
    assert!(session
        .execute_simple_query(
            "INSERT INTO pipe_write SELECT CAST('bad' AS INT),999",
            &mut sink
        )
        .await
        .is_err());
    exec_ok(&mut session, &mut sink, "SELECT COUNT(*) FROM pipe_write").await;
    assert_eq!(query_i64_col(&sink, 0), vec![3]);
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
        "CREATE TABLE pipe_labels(label VARCHAR, bucket INT)",
        "INSERT INTO pipe_dim VALUES (1,'shared'),(2,'shared'),(3,'other'),(4,'empty')",
        "INSERT INTO pipe_fact VALUES (1,10),(1,20),(2,3),(3,NULL),(NULL,4)",
        "INSERT INTO pipe_labels VALUES ('shared',1),('shared',1),('other',2),(NULL,3)",
    ] {
        exec_ok(&mut session, &mut sink, sql).await;
    }
    for sql in [
        "SELECT * FROM generate_series(1,3) AS t(i) ORDER BY i",
        "SELECT constraint_name,table_name FROM information_schema.table_constraints WHERE table_name='pipe_dim' ORDER BY constraint_name",
        "SELECT d.label, SUM(f.v)::BIGINT AS s, COUNT(*) AS n FROM pipe_fact f JOIN pipe_dim d ON f.k=d.k GROUP BY d.label ORDER BY d.label",
        "SELECT d.k, COUNT(f.v) AS n FROM pipe_dim d LEFT JOIN pipe_fact f ON f.k=d.k GROUP BY d.k ORDER BY d.k",
        "SELECT k FROM pipe_dim d WHERE EXISTS (SELECT 1 FROM pipe_fact f WHERE f.k=d.k AND f.v>2) ORDER BY k",
        "SELECT k FROM pipe_dim WHERE k NOT IN (SELECT k FROM pipe_fact) ORDER BY k",
        "SELECT k, (SELECT COUNT(*) FROM pipe_fact f WHERE f.k=d.k) AS n FROM pipe_dim d ORDER BY k",
        "SELECT DISTINCT k FROM pipe_fact ORDER BY k NULLS FIRST",
        "SELECT a.k,b.k FROM pipe_dim a CROSS JOIN pipe_dim b ORDER BY a.k,b.k",
        "SELECT d.k,l.bucket,f.v FROM pipe_fact f CROSS JOIN pipe_labels l JOIN pipe_dim d ON f.k=d.k AND l.label=d.label ORDER BY d.k,l.bucket,f.v",
        "SELECT a.k,b.k,f.v FROM pipe_dim a JOIN pipe_dim b ON a.label=b.label AND a.k<b.k JOIN pipe_fact f ON f.k=a.k AND f.v>b.k ORDER BY a.k,b.k,f.v",
        "SELECT a.k,b.k,f.v FROM pipe_dim a JOIN pipe_dim b ON a.k<b.k+1 LEFT JOIN pipe_fact f ON f.k=a.k AND f.v>b.k ORDER BY a.k,b.k,f.v NULLS FIRST",
        "SELECT a.k,b.k,c.k FROM pipe_dim a JOIN pipe_dim b ON a.label=b.label AND a.k<b.k JOIN pipe_dim c ON b.k=c.k AND c.k>a.k+0 ORDER BY a.k,b.k,c.k",
        "SELECT a.k,b.k,c.k FROM pipe_dim a JOIN pipe_dim b ON a.label=b.label JOIN pipe_dim c ON a.k=c.k WHERE b.k=c.k AND a.k<=b.k ORDER BY a.k,b.k,c.k",
        "SELECT a.k,b.k FROM pipe_dim a LEFT JOIN pipe_dim b ON a.label=b.label AND a.k=b.k AND a.k<b.k ORDER BY a.k,b.k NULLS FIRST",
        "SELECT a.k,b.k FROM pipe_dim a FULL JOIN pipe_dim b ON a.label=b.label AND a.k=b.k AND a.k<b.k WHERE a.k IS NULL OR b.k IS NULL ORDER BY a.k NULLS FIRST,b.k NULLS FIRST",
        "SELECT k,label,SUM(k) OVER(PARTITION BY label),COUNT(*) OVER() FROM pipe_dim ORDER BY k",
        "SELECT a.k,b.k,c.k FROM pipe_dim a, pipe_dim b, pipe_dim c WHERE a.k=b.k AND b.k=c.k AND CASE WHEN a.k>0 THEN (a.k+b.k)::DOUBLE/c.k ELSE 0 END > 1.5 ORDER BY a.k,b.k,c.k",
        "SELECT d.label, AVG(f.v), MIN(f.v), MAX(f.v), COUNT(f.v) FROM pipe_fact f JOIN pipe_dim d ON f.k=d.k GROUP BY d.label ORDER BY d.label",
        "SELECT d.label,COUNT(DISTINCT f.v) FROM pipe_fact f JOIN pipe_dim d ON f.k=d.k GROUP BY d.label ORDER BY d.label",
        "SELECT l.bucket,SUM(f.v)::BIGINT,COUNT(f.v),COUNT(*),MIN(f.v),MAX(f.v) FROM pipe_fact f JOIN pipe_dim d ON f.k=d.k JOIN pipe_labels l ON d.label=l.label GROUP BY l.bucket ORDER BY l.bucket",
        "SELECT l.bucket,SUM(f.v) FILTER (WHERE f.v>5)::BIGINT,COUNT(f.v) FILTER (WHERE f.v>5) FROM pipe_fact f JOIN pipe_dim d ON f.k=d.k JOIN pipe_labels l ON d.label=l.label GROUP BY l.bucket ORDER BY l.bucket",
        "SELECT l.bucket,AVG(f.v),COUNT(DISTINCT f.v) FROM pipe_fact f JOIN pipe_dim d ON f.k=d.k JOIN pipe_labels l ON d.label=l.label GROUP BY l.bucket ORDER BY l.bucket",
        "SELECT l.bucket,SUM(f.v)::BIGINT FROM pipe_fact f JOIN pipe_dim d ON f.k=d.k JOIN pipe_labels l ON d.label=l.label WHERE f.v<0 GROUP BY l.bucket ORDER BY l.bucket",
        "SELECT COUNT(DISTINCT k) AS n, SUM(DISTINCT v)::BIGINT AS s FROM pipe_fact",
        "WITH p AS MATERIALIZED (SELECT k,v FROM pipe_fact UNION ALL SELECT k,v FROM pipe_fact) SELECT a.k,SUM(a.v+b.v)::BIGINT AS s FROM p a JOIN p b ON a.k=b.k WHERE a.v>5 AND b.v<30 GROUP BY a.k ORDER BY a.k",
    ] {
        exec_ok(&mut session, &mut sink, sql).await;
        let expected = rows(&sink);
        let types = sink.assert_single_result().types.clone();
        let names = sink.assert_single_result().names.clone();
        exec_ok(&mut session, &mut sink, sql).await;
        assert_eq!(rows(&sink), expected, "{sql}");
        assert_eq!(sink.assert_single_result().types, types, "{sql}");
        assert_eq!(sink.assert_single_result().names, names, "{sql}");
        exec_ok(&mut session, &mut sink, sql).await;
        assert_eq!(rows(&sink), expected, "repeat: {sql}");
        assert_eq!(sink.assert_single_result().types, types, "{sql}");
    }
    // Independent expected value: two dimension keys sharing one label must
    // merge into one SQL group, not escape as separate partial groups.
    exec_ok(&mut session, &mut sink, "SELECT SUM(f.v)::BIGINT FROM pipe_fact f JOIN pipe_dim d ON f.k=d.k WHERE d.label='shared' GROUP BY d.label").await;
    assert_eq!(query_i64_col(&sink, 0), vec![33]);
    exec_ok(&mut session, &mut sink, "SELECT SUM(f.v)::BIGINT FROM pipe_fact f JOIN pipe_dim d ON f.k=d.k JOIN pipe_labels l ON d.label=l.label WHERE l.bucket=1 GROUP BY l.bucket").await;
    assert_eq!(
        query_i64_col(&sink, 0),
        vec![66],
        "duplicate dimensions must multiply partial states before the final merge"
    );
    exec_ok(&mut session, &mut sink, "SELECT SUM(f.v)::BIGINT FROM pipe_dim a JOIN pipe_dim b ON a.label=b.label AND a.k<b.k JOIN pipe_fact f ON f.k=a.k AND f.v>b.k").await;
    assert_eq!(query_i64_col(&sink, 0), vec![30]);
}

#[tokio::test]
async fn correlated_ranges_survive_predicate_canonicalization() {
    let instance = Instance::new_in_memory_with_config(
        InstanceConfig::in_memory().with_max_memory(256 * 1024 * 1024),
    )
    .unwrap();
    let mut session = Session::new(1, instance);
    let mut sink = CollectingSink::new();
    for sql in [
        "CREATE TABLE corr_o(id INT, grp INT, threshold INT)",
        "CREATE TABLE corr_d(grp INT, seq INT, score INT)",
        "INSERT INTO corr_o VALUES (1,10,6),(2,20,5),(3,20,8),(4,30,2),(5,NULL,4),(6,40,1)",
        "INSERT INTO corr_d VALUES (10,1,4),(10,2,9),(10,3,7),(20,1,5),(20,2,8),(20,3,6),(20,4,8),(30,1,3),(30,2,1),(NULL,1,50)",
    ] { exec_ok(&mut session, &mut sink, sql).await; }
    for repetition in 0..2 {
        exec_ok(&mut session, &mut sink,
            "SELECT o.id, EXISTS(SELECT 1 FROM corr_d d WHERE d.grp=o.grp AND d.score>=o.threshold) FROM corr_o o ORDER BY o.id").await;
        assert_eq!(
            rows(&sink),
            (1..=6)
                .map(|id| vec![Value::Integer(id), Value::Boolean(id <= 4)])
                .collect::<Vec<_>>(),
            "{repetition}"
        );
        exec_ok(&mut session, &mut sink,
            "SELECT o.id FROM corr_o o WHERE EXISTS(SELECT 1 FROM corr_d d WHERE d.grp=o.grp AND d.score>=o.threshold) ORDER BY o.id").await;
        assert_eq!(query_i64_col(&sink, 0), vec![1, 2, 3, 4], "{repetition}");
        exec_ok(&mut session, &mut sink,
            "SELECT o.id,p.score FROM corr_o o CROSS JOIN LATERAL (SELECT d.score FROM corr_d d WHERE d.grp=o.grp AND d.score>=o.threshold ORDER BY d.score DESC,d.seq LIMIT 1) p ORDER BY o.id").await;
        assert_eq!(
            rows(&sink),
            [(1, 9), (2, 8), (3, 8), (4, 3)]
                .into_iter()
                .map(|(id, score)| vec![Value::Integer(id), Value::Integer(score)])
                .collect::<Vec<_>>(),
            "{repetition}"
        );
    }
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
