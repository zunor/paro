#[path = "common/exec_ok.rs"]
mod exec_ok;
#[path = "common/graph.rs"]
mod graph;
#[path = "common/instance_persistent.rs"]
mod instance_persistent;
#[path = "common/query_string_pairs.rs"]
mod query_string_pairs;
#[path = "common/unique_test_dir.rs"]
mod unique_test_dir;

use std::sync::Arc;

use exec_ok::exec_ok;
use graph::graph_runtime_key;
use instance_persistent::create_persistent_instance;
use paro_catalog::mvcc::CatalogSnapshot;
use paro_common::runtime_value::Value;
use paro_instance::DatabaseCloseAction;
use paro_session::{CollectingSink, Session};
use paro_storage::index::graph::VertexKey;
use query_string_pairs::query_string_pairs;
use unique_test_dir::create_unique_test_dir;

#[derive(Debug, Clone, PartialEq, Eq)]
struct GraphMetaRow {
    state: String,
    delta_size: i64,
    vertex_count: i64,
    edge_count: i64,
}

fn query_graph_meta(sink: &CollectingSink) -> GraphMetaRow {
    let result = sink.assert_single_result();
    let chunk = result.chunks.first().expect("expected at least one chunk");
    assert_eq!(chunk.len(), 1, "expected exactly one graph metadata row");
    let state = match chunk.column(0).expect("missing state").get_value(0) {
        Value::Varchar(v) => v,
        other => panic!("unexpected state value: {:?}", other),
    };
    let delta_size = match chunk.column(1).expect("missing delta_size").get_value(0) {
        Value::BigInt(v) => v,
        other => panic!("unexpected delta_size value: {:?}", other),
    };
    let vertex_count = match chunk.column(2).expect("missing vertex_count").get_value(0) {
        Value::BigInt(v) => v,
        other => panic!("unexpected vertex_count value: {:?}", other),
    };
    let edge_count = match chunk.column(3).expect("missing edge_count").get_value(0) {
        Value::BigInt(v) => v,
        other => panic!("unexpected edge_count value: {:?}", other),
    };
    GraphMetaRow {
        state,
        delta_size,
        vertex_count,
        edge_count,
    }
}

#[tokio::test]
async fn restart_recovers_stale_graph_back_to_ready() {
    let base_dir = create_unique_test_dir("graph_consistency", "restart");
    let graph_name = "restart_ready_graph";

    {
        let instance = create_persistent_instance(&base_dir);
        let mut session = Session::new(1, Arc::clone(&instance));
        let mut sink = CollectingSink::new();

        exec_ok(
            &mut session,
            &mut sink,
            "CREATE TABLE restart_person (id BIGINT PRIMARY KEY, name VARCHAR)",
        )
        .await;
        exec_ok(
            &mut session,
            &mut sink,
            "CREATE TABLE restart_knows (src_id BIGINT, dst_id BIGINT)",
        )
        .await;
        exec_ok(
            &mut session,
            &mut sink,
            "INSERT INTO restart_person VALUES \
             (1, 'Alice'), (2, 'Bob'), (3, 'Carol'), (4, 'Dave'), (5, 'Eve'), (6, 'Frank')",
        )
        .await;
        exec_ok(
            &mut session,
            &mut sink,
            "INSERT INTO restart_knows VALUES (1, 2), (2, 3), (3, 4), (4, 5), (5, 6)",
        )
        .await;
        exec_ok(
            &mut session,
            &mut sink,
            "CREATE PROPERTY GRAPH restart_ready_graph \
             VERTEX TABLES (restart_person LABEL Person) \
             EDGE TABLES (restart_knows SOURCE KEY (src_id) REFERENCES restart_person (id) DESTINATION KEY (dst_id) REFERENCES restart_person (id) LABEL Knows)",
        )
        .await;

        exec_ok(
            &mut session,
            &mut sink,
            "INSERT INTO restart_knows VALUES (1, 3)",
        )
        .await;
        exec_ok(
            &mut session,
            &mut sink,
            "SELECT state, delta_size, vertex_count, edge_count \
             FROM paro_property_graphs() WHERE graph_name = 'restart_ready_graph'",
        )
        .await;
        assert_eq!(
            query_graph_meta(&sink),
            GraphMetaRow {
                state: "READY".to_string(),
                delta_size: 1,
                vertex_count: 6,
                edge_count: 6,
            }
        );

        exec_ok(
            &mut session,
            &mut sink,
            "INSERT INTO restart_person VALUES (7, 'Grace')",
        )
        .await;
        exec_ok(
            &mut session,
            &mut sink,
            "SELECT state, delta_size, vertex_count, edge_count \
             FROM paro_property_graphs() WHERE graph_name = 'restart_ready_graph'",
        )
        .await;
        assert_eq!(
            query_graph_meta(&sink),
            GraphMetaRow {
                state: "STALE".to_string(),
                delta_size: 1,
                vertex_count: 6,
                edge_count: 6,
            }
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
        "SELECT state, delta_size, vertex_count, edge_count \
         FROM paro_property_graphs() WHERE graph_name = 'restart_ready_graph'",
    )
    .await;
    assert_eq!(
        query_graph_meta(&sink),
        GraphMetaRow {
            state: "READY".to_string(),
            delta_size: 0,
            vertex_count: 7,
            edge_count: 6,
        }
    );

    exec_ok(
        &mut session,
        &mut sink,
        "SELECT src, dst FROM GRAPH_TABLE(restart_ready_graph \
         MATCH (a:Person)-[e:Knows]->(b:Person) \
         COLUMNS (a.name AS src, b.name AS dst)) gt \
         ORDER BY src, dst",
    )
    .await;
    assert_eq!(
        query_string_pairs(&sink),
        vec![
            ("Alice".to_string(), "Bob".to_string()),
            ("Alice".to_string(), "Carol".to_string()),
            ("Bob".to_string(), "Carol".to_string()),
            ("Carol".to_string(), "Dave".to_string()),
            ("Dave".to_string(), "Eve".to_string()),
            ("Eve".to_string(), "Frank".to_string()),
        ]
    );

    let _ = std::fs::remove_dir_all(&base_dir);
}

#[tokio::test]
async fn pinned_snapshot_stays_stable_across_concurrent_edge_commit() {
    let base_dir = create_unique_test_dir("graph_consistency", "pinned_snapshot");
    let instance = create_persistent_instance(&base_dir);
    let mut session1 = Session::new(1, Arc::clone(&instance));
    let mut session2 = Session::new(2, Arc::clone(&instance));
    let mut sink = CollectingSink::new();
    let graph_name = "pinned_snapshot_graph";

    exec_ok(
        &mut session1,
        &mut sink,
        "CREATE TABLE snapshot_person (id BIGINT PRIMARY KEY, name VARCHAR)",
    )
    .await;
    exec_ok(
        &mut session1,
        &mut sink,
        "CREATE TABLE snapshot_knows (src_id BIGINT, dst_id BIGINT)",
    )
    .await;
    exec_ok(
        &mut session1,
        &mut sink,
        "INSERT INTO snapshot_person VALUES \
         (1, 'Alice'), (2, 'Bob'), (3, 'Carol'), (4, 'Dave'), (5, 'Eve'), (6, 'Frank')",
    )
    .await;
    exec_ok(
        &mut session1,
        &mut sink,
        "INSERT INTO snapshot_knows VALUES (1, 2), (2, 3), (3, 4), (4, 5), (5, 6)",
    )
    .await;
    exec_ok(
        &mut session1,
        &mut sink,
        "CREATE PROPERTY GRAPH pinned_snapshot_graph \
         VERTEX TABLES (snapshot_person LABEL Person) \
         EDGE TABLES (snapshot_knows SOURCE KEY (src_id) REFERENCES snapshot_person (id) DESTINATION KEY (dst_id) REFERENCES snapshot_person (id) LABEL Knows)",
    )
    .await;

    let snapshot_before = instance
        .graph_manager()
        .snapshot(&graph_runtime_key(graph_name))
        .expect("graph snapshot before concurrent DML");
    let alice_local = snapshot_before
        .base()
        .vertex_map("Person")
        .expect("person label")
        .key_to_local(&VertexKey::Int64(1))
        .expect("Alice local id");
    let mut old_scratch = Vec::new();
    let old_view = snapshot_before
        .neighbors_forward("Knows", alice_local, &mut old_scratch)
        .expect("old neighbor view should exist");
    assert_eq!(
        old_view.len(),
        1,
        "old snapshot should only see Alice -> Bob"
    );
    assert_eq!(snapshot_before.delta_size(), 0);

    exec_ok(
        &mut session2,
        &mut sink,
        "INSERT INTO snapshot_knows VALUES (1, 3)",
    )
    .await;

    let snapshot_after = instance
        .graph_manager()
        .snapshot(&graph_runtime_key(graph_name))
        .expect("graph snapshot after concurrent DML");
    assert!(
        snapshot_after.generation_id() > snapshot_before.generation_id(),
        "committed edge DML should publish a new graph generation"
    );
    assert_eq!(snapshot_after.delta_size(), 1);

    let mut new_scratch = Vec::new();
    let new_view = snapshot_after
        .neighbors_forward("Knows", alice_local, &mut new_scratch)
        .expect("new neighbor view should exist");
    assert_eq!(
        new_view.len(),
        2,
        "new snapshot should see Alice -> Bob and Alice -> Carol"
    );

    let mut old_scratch_after = Vec::new();
    let old_view_after = snapshot_before
        .neighbors_forward("Knows", alice_local, &mut old_scratch_after)
        .expect("old pinned snapshot should still be usable");
    assert_eq!(
        old_view_after.len(),
        1,
        "pinned snapshot should remain stable after another session commits edge DML"
    );

    exec_ok(
        &mut session1,
        &mut sink,
        "SELECT src, dst FROM GRAPH_TABLE(pinned_snapshot_graph \
         MATCH (a:Person)-[e:Knows]->(b:Person) \
         COLUMNS (a.name AS src, b.name AS dst)) gt \
         ORDER BY src, dst",
    )
    .await;
    assert_eq!(
        query_string_pairs(&sink),
        vec![
            ("Alice".to_string(), "Bob".to_string()),
            ("Alice".to_string(), "Carol".to_string()),
            ("Bob".to_string(), "Carol".to_string()),
            ("Carol".to_string(), "Dave".to_string()),
            ("Dave".to_string(), "Eve".to_string()),
            ("Eve".to_string(), "Frank".to_string()),
        ]
    );

    let txn = CatalogSnapshot::default();
    assert!(
        session1
            .current_database
            .catalog()
            .scan_property_graphs(&txn)
            .iter()
            .any(|graph| graph.info.graph_name == graph_name),
        "graph catalog entry should remain present"
    );

    let _ = std::fs::remove_dir_all(&base_dir);
}
