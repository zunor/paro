// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

#[path = "common/exec_ok.rs"]
mod exec_ok;
#[path = "common/graph.rs"]
mod graph;
#[path = "common/instance_persistent.rs"]
mod instance_persistent;
#[path = "common/unique_test_dir.rs"]
mod unique_test_dir;

use std::sync::Arc;

use exec_ok::exec_ok;
use graph::graph_runtime_key;
use instance_persistent::create_persistent_instance;
use paro_common::runtime_value::Value;
use paro_instance::DatabaseCloseAction;
use paro_session::{CollectingSink, Session};
use tokio::time::{sleep, Duration, Instant};
use unique_test_dir::create_unique_test_dir;

fn query_rows(sink: &CollectingSink) -> Vec<(i64, String, String, i64)> {
    let result = sink.assert_single_result();
    let mut rows = Vec::new();
    for chunk in &result.chunks {
        let tenant_col = chunk.column(0).expect("missing tenant column");
        let src_col = chunk.column(1).expect("missing src column");
        let dst_col = chunk.column(2).expect("missing dst column");
        let since_col = chunk.column(3).expect("missing since column");
        for row_idx in 0..chunk.len() {
            let tenant = match tenant_col.get_value(row_idx) {
                Value::BigInt(v) => v,
                other => panic!("unexpected tenant value: {:?}", other),
            };
            let src = match src_col.get_value(row_idx) {
                Value::Varchar(v) => v,
                other => panic!("unexpected src value: {:?}", other),
            };
            let dst = match dst_col.get_value(row_idx) {
                Value::Varchar(v) => v,
                other => panic!("unexpected dst value: {:?}", other),
            };
            let since = match since_col.get_value(row_idx) {
                Value::Integer(v) => i64::from(v),
                Value::BigInt(v) => v,
                other => panic!("unexpected since value: {:?}", other),
            };
            rows.push((tenant, src, dst, since));
        }
    }
    rows
}

fn query_graph_meta(sink: &CollectingSink) -> (String, i64, i64) {
    let result = sink.assert_single_result();
    let chunk = result.chunks.first().expect("expected at least one chunk");
    assert_eq!(chunk.len(), 1, "expected one graph metadata row");
    let state = match chunk.column(0).expect("missing state").get_value(0) {
        Value::Varchar(v) => v,
        other => panic!("unexpected state value: {:?}", other),
    };
    let delta_size = match chunk.column(1).expect("missing delta_size").get_value(0) {
        Value::BigInt(v) => v,
        other => panic!("unexpected delta_size value: {:?}", other),
    };
    let edge_count = match chunk.column(2).expect("missing edge_count").get_value(0) {
        Value::BigInt(v) => v,
        other => panic!("unexpected edge_count value: {:?}", other),
    };
    (state, delta_size, edge_count)
}

#[tokio::test]
async fn restart_recovers_composite_key_graph() {
    let base_dir = create_unique_test_dir("graph_background_maintenance", "composite_restart");
    let graph_name = "composite_key_graph";

    {
        let instance = create_persistent_instance(&base_dir);
        let mut session = Session::new(1, Arc::clone(&instance));
        let mut sink = CollectingSink::new();

        exec_ok(
            &mut session,
            &mut sink,
            "CREATE TABLE composite_person (tenant_id BIGINT, person_code VARCHAR, name VARCHAR, PRIMARY KEY (tenant_id, person_code))",
        )
        .await;
        exec_ok(
            &mut session,
            &mut sink,
            "CREATE TABLE composite_knows (tenant_id BIGINT, src_code VARCHAR, dst_code VARCHAR, since INT)",
        )
        .await;
        exec_ok(
            &mut session,
            &mut sink,
            "INSERT INTO composite_person VALUES
             (1, 'alice', 'Alice'),
             (1, 'bob', 'Bob'),
             (1, 'carol', 'Carol'),
             (2, 'alice', 'Alice T2'),
             (2, 'bob', 'Bob T2')",
        )
        .await;
        exec_ok(
            &mut session,
            &mut sink,
            "INSERT INTO composite_knows VALUES
             (1, 'alice', 'bob', 2020),
             (1, 'bob', 'carol', 2021),
             (2, 'alice', 'bob', 2030)",
        )
        .await;
        exec_ok(
            &mut session,
            &mut sink,
            "CREATE PROPERTY GRAPH composite_key_graph
             VERTEX TABLES (
                 composite_person LABEL Person
             )
             EDGE TABLES (
                 composite_knows
                     SOURCE KEY (tenant_id, src_code) REFERENCES composite_person (tenant_id, person_code)
                     DESTINATION KEY (tenant_id, dst_code) REFERENCES composite_person (tenant_id, person_code)
                     LABEL Knows
             )",
        )
        .await;

        exec_ok(
            &mut session,
            &mut sink,
            "SELECT tenant, src, dst, since FROM GRAPH_TABLE(composite_key_graph
             MATCH (a:Person)-[k:Knows]->(b:Person)
             COLUMNS (a.tenant_id AS tenant, a.name AS src, b.name AS dst, k.since AS since)
            ) gt
            ORDER BY tenant, src, dst",
        )
        .await;
        assert_eq!(
            query_rows(&sink),
            vec![
                (1, "Alice".to_string(), "Bob".to_string(), 2020),
                (1, "Bob".to_string(), "Carol".to_string(), 2021),
                (2, "Alice T2".to_string(), "Bob T2".to_string(), 2030),
            ]
        );

        instance
            .database_registry()
            .get_database("postgres")
            .expect("default database")
            .close(DatabaseCloseAction::Checkpoint)
            .expect("checkpoint close should succeed");
    }

    let cleanup_instance = create_persistent_instance(&base_dir);
    cleanup_instance
        .graph_manager()
        .unregister(&graph_runtime_key(graph_name));
    drop(cleanup_instance);
    let restarted = create_persistent_instance(&base_dir);
    let mut session = Session::new(2, Arc::clone(&restarted));
    let mut sink = CollectingSink::new();

    exec_ok(
        &mut session,
        &mut sink,
        "SELECT tenant, src, dst, since FROM GRAPH_TABLE(composite_key_graph
         MATCH (a:Person)-[k:Knows]->(b:Person)
         COLUMNS (a.tenant_id AS tenant, a.name AS src, b.name AS dst, k.since AS since)
        ) gt
        ORDER BY tenant, src, dst",
    )
    .await;
    assert_eq!(
        query_rows(&sink),
        vec![
            (1, "Alice".to_string(), "Bob".to_string(), 2020),
            (1, "Bob".to_string(), "Carol".to_string(), 2021),
            (2, "Alice T2".to_string(), "Bob T2".to_string(), 2030),
        ]
    );

    let _ = std::fs::remove_dir_all(&base_dir);
}

#[tokio::test]
async fn background_compaction_eventually_rebuilds_delta_generation() {
    let base_dir = create_unique_test_dir("graph_background_maintenance", "delta_compaction");
    let instance = create_persistent_instance(&base_dir);
    let mut session = Session::new(1, Arc::clone(&instance));
    let mut sink = CollectingSink::new();

    exec_ok(
        &mut session,
        &mut sink,
        "CREATE TABLE compact_person (id BIGINT PRIMARY KEY, name VARCHAR)",
    )
    .await;
    exec_ok(
        &mut session,
        &mut sink,
        "CREATE TABLE compact_knows (src_id BIGINT, dst_id BIGINT)",
    )
    .await;
    exec_ok(
        &mut session,
        &mut sink,
        "INSERT INTO compact_person VALUES
         (1, 'Alice'),
         (2, 'Bob'),
         (3, 'Carol'),
         (4, 'Dave'),
         (5, 'Eve'),
         (6, 'Frank')",
    )
    .await;
    exec_ok(
        &mut session,
        &mut sink,
        "INSERT INTO compact_knows VALUES (1, 2), (2, 3), (3, 4), (4, 5), (5, 6)",
    )
    .await;
    exec_ok(
        &mut session,
        &mut sink,
        "CREATE PROPERTY GRAPH delta_compaction_graph
         VERTEX TABLES (compact_person LABEL Person)
         EDGE TABLES (
             compact_knows
                 SOURCE KEY (src_id) REFERENCES compact_person (id)
                 DESTINATION KEY (dst_id) REFERENCES compact_person (id)
                 LABEL Knows
         )",
    )
    .await;

    exec_ok(
        &mut session,
        &mut sink,
        "INSERT INTO compact_knows VALUES (1, 3), (1, 4)",
    )
    .await;

    let deadline = Instant::now() + Duration::from_secs(5);
    let mut saw_delta = false;
    loop {
        exec_ok(
            &mut session,
            &mut sink,
            "SELECT state, delta_size, edge_count
             FROM paro_property_graphs()
             WHERE graph_name = 'delta_compaction_graph'",
        )
        .await;
        let (state, delta_size, edge_count) = query_graph_meta(&sink);
        assert_eq!(state, "READY");
        if delta_size > 0 {
            saw_delta = true;
        }
        if delta_size == 0 && edge_count == 7 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "background compaction did not finish in time: delta_size={}, edge_count={}, saw_delta={}",
            delta_size,
            edge_count,
            saw_delta
        );
        sleep(Duration::from_millis(50)).await;
    }

    exec_ok(
        &mut session,
        &mut sink,
        "SELECT src, dst FROM GRAPH_TABLE(delta_compaction_graph
         MATCH (a:Person)-[k:Knows]->(b:Person)
         COLUMNS (a.name AS src, b.name AS dst)
        ) gt
        ORDER BY src, dst",
    )
    .await;
    let rows = sink.assert_single_result();
    assert_eq!(rows.total_rows(), 7);

    let _ = std::fs::remove_dir_all(&base_dir);
}

#[tokio::test]
async fn background_rebuild_eventually_clears_stale_after_vertex_insert() {
    let base_dir = create_unique_test_dir("graph_background_maintenance", "stale_rebuild");
    let instance = create_persistent_instance(&base_dir);
    let mut session = Session::new(1, Arc::clone(&instance));
    let mut sink = CollectingSink::new();

    exec_ok(
        &mut session,
        &mut sink,
        "CREATE TABLE stale_person (id BIGINT PRIMARY KEY, name VARCHAR)",
    )
    .await;
    exec_ok(
        &mut session,
        &mut sink,
        "CREATE TABLE stale_knows (src_id BIGINT, dst_id BIGINT)",
    )
    .await;
    exec_ok(
        &mut session,
        &mut sink,
        "INSERT INTO stale_person VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol')",
    )
    .await;
    exec_ok(
        &mut session,
        &mut sink,
        "INSERT INTO stale_knows VALUES (1, 2), (2, 3)",
    )
    .await;
    exec_ok(
        &mut session,
        &mut sink,
        "CREATE PROPERTY GRAPH stale_rebuild_graph
         VERTEX TABLES (stale_person LABEL Person)
         EDGE TABLES (
             stale_knows
                 SOURCE KEY (src_id) REFERENCES stale_person (id)
                 DESTINATION KEY (dst_id) REFERENCES stale_person (id)
                 LABEL Knows
         )",
    )
    .await;

    exec_ok(
        &mut session,
        &mut sink,
        "INSERT INTO stale_person VALUES (4, 'Dora')",
    )
    .await;

    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        exec_ok(
            &mut session,
            &mut sink,
            "SELECT state, delta_size, vertex_count
             FROM paro_property_graphs()
             WHERE graph_name = 'stale_rebuild_graph'",
        )
        .await;
        let (state, delta_size, vertex_count) = query_graph_meta(&sink);
        if state == "READY" && delta_size == 0 && vertex_count == 4 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "background rebuild did not finish in time: state={}, delta_size={}, vertex_count={}",
            state,
            delta_size,
            vertex_count
        );
        sleep(Duration::from_millis(50)).await;
    }

    exec_ok(
        &mut session,
        &mut sink,
        "SELECT * FROM GRAPH_TABLE(stale_rebuild_graph
         MATCH (n:Person WHERE n.name = 'Dora')
         COLUMNS (n.name AS name)
        ) gt",
    )
    .await;
    assert_eq!(sink.total_rows(), 1);

    let _ = std::fs::remove_dir_all(&base_dir);
}
