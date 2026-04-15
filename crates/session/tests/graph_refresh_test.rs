use std::sync::Arc;

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

use exec_ok::exec_ok;
use graph::graph_runtime_key;
use instance_persistent::create_persistent_instance;
use paro_session::{CollectingSink, Session};
use paro_storage::index::graph::GraphStatsProvider;
use query_string_pairs::query_string_pairs;
use unique_test_dir::create_unique_test_dir;

#[tokio::test]
async fn refresh_property_graph_publishes_edge_delta() {
    let base_dir = create_unique_test_dir("graph_refresh", "delta");
    let graph_name = "refresh_delta_graph";
    let instance = create_persistent_instance(&base_dir);
    let mut session = Session::new(1, Arc::clone(&instance));
    let mut sink = CollectingSink::new();

    exec_ok(
        &mut session,
        &mut sink,
        "CREATE TABLE rg_person (id BIGINT PRIMARY KEY, name VARCHAR)",
    )
    .await;
    exec_ok(
        &mut session,
        &mut sink,
        "CREATE TABLE rg_knows (src_id BIGINT, dst_id BIGINT)",
    )
    .await;
    exec_ok(
        &mut session,
        &mut sink,
        "INSERT INTO rg_person VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol'), (4, 'Dave'), (5, 'Eve'), (6, 'Frank')",
    )
    .await;
    exec_ok(
        &mut session,
        &mut sink,
        "INSERT INTO rg_knows VALUES (1, 2), (2, 3), (3, 4), (4, 5), (5, 6)",
    )
    .await;
    exec_ok(
        &mut session,
        &mut sink,
        "CREATE PROPERTY GRAPH refresh_delta_graph \
         VERTEX TABLES (rg_person LABEL Person) \
         EDGE TABLES (rg_knows SOURCE KEY (src_id) REFERENCES rg_person (id) DESTINATION KEY (dst_id) REFERENCES rg_person (id) LABEL Knows)",
    )
    .await;
    let initial_generation_id = instance
        .graph_manager()
        .snapshot(&graph_runtime_key(graph_name))
        .expect("graph snapshot should exist after create")
        .generation_id();

    exec_ok(
        &mut session,
        &mut sink,
        "INSERT INTO rg_knows VALUES (1, 3)",
    )
    .await;
    exec_ok(
        &mut session,
        &mut sink,
        "REFRESH PROPERTY GRAPH refresh_delta_graph",
    )
    .await;

    exec_ok(
        &mut session,
        &mut sink,
        "SELECT src, dst FROM GRAPH_TABLE(refresh_delta_graph \
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

    let snapshot = instance
        .graph_manager()
        .snapshot(&graph_runtime_key(graph_name))
        .expect("graph snapshot should exist");
    assert!(
        snapshot.generation_id() > initial_generation_id,
        "refresh path should publish a newer generation"
    );
    assert!(
        snapshot
            .generation()
            .committed_edge_deltas
            .contains_key("Knows"),
        "edge-only refresh should publish committed delta"
    );
    assert_eq!(snapshot.statistics().edge_count("Knows"), Some(6));
    assert_eq!(snapshot.statistics().vertex_count("Person"), Some(6));
    assert_eq!(
        snapshot
            .statistics()
            .pattern_step_count("Person", "Knows", "Person"),
        Some(6)
    );

    instance
        .graph_manager()
        .unregister(&graph_runtime_key(graph_name));
    let _ = std::fs::remove_dir_all(&base_dir);
}

#[tokio::test]
async fn refresh_property_graph_rebuilds_on_vertex_change() {
    let base_dir = create_unique_test_dir("graph_refresh", "rebuild");
    let graph_name = "refresh_rebuild_graph";
    let instance = create_persistent_instance(&base_dir);
    let mut session = Session::new(2, Arc::clone(&instance));
    let mut sink = CollectingSink::new();

    exec_ok(
        &mut session,
        &mut sink,
        "CREATE TABLE rv_person (id BIGINT PRIMARY KEY, name VARCHAR)",
    )
    .await;
    exec_ok(
        &mut session,
        &mut sink,
        "CREATE TABLE rv_knows (src_id BIGINT, dst_id BIGINT)",
    )
    .await;
    exec_ok(
        &mut session,
        &mut sink,
        "INSERT INTO rv_person VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol')",
    )
    .await;
    exec_ok(
        &mut session,
        &mut sink,
        "INSERT INTO rv_knows VALUES (1, 2), (2, 3)",
    )
    .await;
    exec_ok(
        &mut session,
        &mut sink,
        "CREATE PROPERTY GRAPH refresh_rebuild_graph \
         VERTEX TABLES (rv_person LABEL Person) \
         EDGE TABLES (rv_knows SOURCE KEY (src_id) REFERENCES rv_person (id) DESTINATION KEY (dst_id) REFERENCES rv_person (id) LABEL Knows)",
    )
    .await;
    let initial_generation_id = instance
        .graph_manager()
        .snapshot(&graph_runtime_key(graph_name))
        .expect("graph snapshot should exist after create")
        .generation_id();

    exec_ok(
        &mut session,
        &mut sink,
        "INSERT INTO rv_person VALUES (4, 'Dave')",
    )
    .await;
    exec_ok(
        &mut session,
        &mut sink,
        "INSERT INTO rv_knows VALUES (3, 4)",
    )
    .await;
    exec_ok(
        &mut session,
        &mut sink,
        "REFRESH PROPERTY GRAPH refresh_rebuild_graph",
    )
    .await;

    exec_ok(
        &mut session,
        &mut sink,
        "SELECT src, dst FROM GRAPH_TABLE(refresh_rebuild_graph \
         MATCH (a:Person)-[e:Knows]->(b:Person) \
         COLUMNS (a.name AS src, b.name AS dst)) gt \
         ORDER BY src, dst",
    )
    .await;
    assert_eq!(
        query_string_pairs(&sink),
        vec![
            ("Alice".to_string(), "Bob".to_string()),
            ("Bob".to_string(), "Carol".to_string()),
            ("Carol".to_string(), "Dave".to_string()),
        ]
    );

    let snapshot = instance
        .graph_manager()
        .snapshot(&graph_runtime_key(graph_name))
        .expect("graph snapshot should exist");
    assert!(
        snapshot.generation_id() > initial_generation_id,
        "refresh rebuild should publish a newer generation"
    );
    assert_eq!(
        snapshot
            .base()
            .vertex_map("Person")
            .expect("person vertex map")
            .num_vertices(),
        4
    );
    assert!(
        snapshot.generation().committed_edge_deltas.is_empty(),
        "vertex change should trigger rebuild instead of edge delta publish"
    );

    instance
        .graph_manager()
        .unregister(&graph_runtime_key(graph_name));
    let _ = std::fs::remove_dir_all(&base_dir);
}

#[tokio::test]
async fn refresh_property_graph_compacts_large_delta() {
    let base_dir = create_unique_test_dir("graph_refresh", "compact");
    let graph_name = "refresh_compact_graph";
    let instance = create_persistent_instance(&base_dir);
    let mut session = Session::new(3, Arc::clone(&instance));
    let mut sink = CollectingSink::new();

    exec_ok(
        &mut session,
        &mut sink,
        "CREATE TABLE rc_person (id BIGINT PRIMARY KEY, name VARCHAR)",
    )
    .await;
    exec_ok(
        &mut session,
        &mut sink,
        "CREATE TABLE rc_knows (src_id BIGINT, dst_id BIGINT)",
    )
    .await;
    exec_ok(
        &mut session,
        &mut sink,
        "INSERT INTO rc_person VALUES (1, 'A'), (2, 'B'), (3, 'C'), (4, 'D'), (5, 'E'), (6, 'F')",
    )
    .await;
    exec_ok(
        &mut session,
        &mut sink,
        "INSERT INTO rc_knows VALUES (1, 2), (2, 3), (3, 4), (4, 5), (5, 6)",
    )
    .await;
    exec_ok(
        &mut session,
        &mut sink,
        "CREATE PROPERTY GRAPH refresh_compact_graph \
         VERTEX TABLES (rc_person LABEL Person) \
         EDGE TABLES (rc_knows SOURCE KEY (src_id) REFERENCES rc_person (id) DESTINATION KEY (dst_id) REFERENCES rc_person (id) LABEL Knows)",
    )
    .await;
    let initial_generation_id = instance
        .graph_manager()
        .snapshot(&graph_runtime_key(graph_name))
        .expect("graph snapshot should exist after create")
        .generation_id();

    exec_ok(
        &mut session,
        &mut sink,
        "INSERT INTO rc_knows VALUES (1, 3), (2, 4)",
    )
    .await;
    exec_ok(
        &mut session,
        &mut sink,
        "REFRESH PROPERTY GRAPH refresh_compact_graph",
    )
    .await;

    exec_ok(
        &mut session,
        &mut sink,
        "SELECT src, dst FROM GRAPH_TABLE(refresh_compact_graph \
         MATCH (a:Person)-[e:Knows]->(b:Person) \
         COLUMNS (a.name AS src, b.name AS dst)) gt \
         ORDER BY src, dst",
    )
    .await;
    assert_eq!(query_string_pairs(&sink).len(), 7);

    let snapshot = instance
        .graph_manager()
        .snapshot(&graph_runtime_key(graph_name))
        .expect("graph snapshot should exist");
    assert!(
        snapshot.generation_id() > initial_generation_id,
        "refresh compaction should publish a newer generation"
    );
    assert!(
        snapshot.generation().committed_edge_deltas.is_empty(),
        "large delta should compact into a rebuilt base generation"
    );
    assert_eq!(
        snapshot
            .base()
            .forward_csr("Knows")
            .expect("knows csr")
            .num_edges(),
        7
    );

    instance
        .graph_manager()
        .unregister(&graph_runtime_key(graph_name));
    let _ = std::fs::remove_dir_all(&base_dir);
}
