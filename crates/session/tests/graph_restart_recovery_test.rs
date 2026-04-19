// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

#[path = "common/exec_ok.rs"]
mod exec_ok;
#[path = "common/graph.rs"]
mod graph;
#[path = "common/instance_persistent.rs"]
mod instance_persistent;
#[path = "common/query_i64_col.rs"]
mod query_i64_col;
#[path = "common/query_string_col.rs"]
mod query_string_col;
#[path = "common/query_string_pairs.rs"]
mod query_string_pairs;
#[path = "common/unique_test_dir.rs"]
mod unique_test_dir;

use std::path::Path;
use std::sync::Arc;

use exec_ok::exec_ok;
use graph::graph_runtime_key;
use instance_persistent::create_persistent_instance;
use paro_catalog::entry::CatalogEntryEnum;
use paro_catalog::mvcc::CatalogSnapshot;
use paro_instance::{DatabaseCloseAction, RecoveryHookResult, StartupIssueKind};
use paro_session::test_support::durable_commit_without_post_commit;
use paro_session::{CollectingSink, Session};
use paro_storage::tablet::TabletReaderParams;
use query_i64_col::query_i64_col;
use query_string_col::query_string_col;
use query_string_pairs::query_string_pairs;
use tokio::time::{sleep, Duration, Instant};
use unique_test_dir::create_unique_test_dir;

fn graph_manifest_path(base_dir: &Path, graph_name: &str) -> std::path::PathBuf {
    base_dir
        .join("databases")
        .join("db-1")
        .join("graph")
        .join(graph_name)
        .join("meta.json")
}

fn table_rowids(session: &Session, schema_name: &str, table_name: &str) -> Vec<u64> {
    let txn = CatalogSnapshot::default();
    let table_entry = session
        .current_database
        .catalog()
        .get_table(&txn, schema_name, table_name)
        .expect("table should exist");
    let table = match table_entry.as_ref() {
        CatalogEntryEnum::Table(table) => table,
        other => panic!("expected table entry, got {:?}", other.entry_type()),
    };
    let storage = table.get_storage().expect("table storage");
    let params = TabletReaderParams::with_version(storage.max_version()).with_emit_row_id(true);
    let mut reader = storage.create_reader(params).expect("reader");
    reader.prepare().expect("prepare");

    let mut rowids = Vec::new();
    while let Some(chunk) = reader.get_next_chunk().expect("chunk") {
        let rowid_col = chunk
            .column(chunk.column_count() - 1)
            .expect("rowid column should exist");
        for row in 0..chunk.size() {
            rowids.push(rowid_col.get_u64(row).expect("rowid value"));
        }
    }
    rowids.sort_unstable();
    rowids
}

fn query_graph_state_and_vertex_count(sink: &CollectingSink) -> (String, i64) {
    let state = query_string_col(sink, 0)
        .into_iter()
        .next()
        .expect("graph state row should exist");
    let vertex_count = query_i64_col(sink, 1)
        .into_iter()
        .next()
        .expect("graph vertex count row should exist");
    (state, vertex_count)
}

#[tokio::test]
async fn property_graph_is_recovered_on_restart() {
    let base_dir = create_unique_test_dir("graph_restart_recovery", "restart");
    let graph_name = "restart_graph";

    {
        let instance = create_persistent_instance(&base_dir);
        let mut session = Session::new(1, Arc::clone(&instance));
        let mut sink = CollectingSink::new();

        exec_ok(
            &mut session,
            &mut sink,
            "CREATE TABLE g_person (id BIGINT PRIMARY KEY, name VARCHAR, age INT)",
        )
        .await;
        exec_ok(
            &mut session,
            &mut sink,
            "CREATE TABLE g_knows (src_id BIGINT, dst_id BIGINT)",
        )
        .await;
        exec_ok(
            &mut session,
            &mut sink,
            "INSERT INTO g_person VALUES (1, 'Alice', 30), (2, 'Bob', NULL), (3, 'Carol', 40)",
        )
        .await;
        exec_ok(
            &mut session,
            &mut sink,
            "INSERT INTO g_knows VALUES (1, 2), (2, 3)",
        )
        .await;
        exec_ok(
            &mut session,
            &mut sink,
            "CREATE PROPERTY GRAPH restart_graph \
             VERTEX TABLES (g_person LABEL Person) \
             EDGE TABLES (g_knows SOURCE KEY (src_id) REFERENCES g_person (id) DESTINATION KEY (dst_id) REFERENCES g_person (id) LABEL Knows)",
        )
        .await;

        assert!(
            instance
                .graph_manager()
                .get(&graph_runtime_key(graph_name))
                .is_some(),
            "graph should be registered before restart"
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
    assert!(
        cleanup_instance
            .graph_manager()
            .get(&graph_runtime_key(graph_name))
            .is_none(),
        "graph manager should be cleared before restart validation"
    );
    drop(cleanup_instance);

    let restarted = create_persistent_instance(&base_dir);
    let startup_report = restarted.startup_report();
    let mut session = Session::new(2, Arc::clone(&restarted));
    let mut sink = CollectingSink::new();

    exec_ok(
        &mut session,
        &mut sink,
        "SELECT name FROM g_person ORDER BY name",
    )
    .await;
    assert_eq!(
        query_string_col(&sink, 0),
        vec!["Alice".to_string(), "Bob".to_string(), "Carol".to_string()]
    );

    let graph = restarted
        .graph_manager()
        .get(&graph_runtime_key(graph_name))
        .expect("graph should be registered after restart");
    let graph_vertex_rowids = (0..graph
        .vertex_map("Person")
        .expect("person label")
        .num_vertices())
        .map(|local_id| graph.vertex_map("Person").unwrap().local_to_rowid(local_id))
        .collect::<Vec<_>>();
    let live_vertex_rowids = table_rowids(&session, "public", "g_person");
    assert_eq!(graph_vertex_rowids, live_vertex_rowids);

    exec_ok(
        &mut session,
        &mut sink,
        "SELECT src, dst FROM GRAPH_TABLE(restart_graph \
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
        ]
    );
    assert!(
        restarted
            .graph_manager()
            .get(&graph_runtime_key(graph_name))
            .is_some(),
        "graph should be recovered during startup"
    );

    let postgres = startup_report
        .databases
        .iter()
        .find(|entry| entry.name == "postgres")
        .expect("startup report should include postgres");
    assert_eq!(
        postgres
            .recovery_report
            .as_ref()
            .expect("postgres should include recovery report")
            .hook_results,
        vec![RecoveryHookResult::Reused],
        "valid persisted graph projection should be reused during startup"
    );

    let _ = std::fs::remove_dir_all(&base_dir);
}

#[tokio::test]
async fn property_graph_is_rebuilt_when_projection_dir_is_missing_on_restart() {
    let base_dir = create_unique_test_dir("graph_restart_recovery", "rebuild");
    let graph_name = "restart_rebuild_graph";

    {
        let instance = create_persistent_instance(&base_dir);
        let mut session = Session::new(1, Arc::clone(&instance));
        let mut sink = CollectingSink::new();

        exec_ok(
            &mut session,
            &mut sink,
            "CREATE TABLE g_person (id BIGINT PRIMARY KEY, name VARCHAR)",
        )
        .await;
        exec_ok(
            &mut session,
            &mut sink,
            "CREATE TABLE g_knows (src_id BIGINT, dst_id BIGINT)",
        )
        .await;
        exec_ok(
            &mut session,
            &mut sink,
            "INSERT INTO g_person VALUES (1, 'Alice'), (2, 'Bob')",
        )
        .await;
        exec_ok(&mut session, &mut sink, "INSERT INTO g_knows VALUES (1, 2)").await;
        exec_ok(
            &mut session,
            &mut sink,
            "CREATE PROPERTY GRAPH restart_rebuild_graph \
             VERTEX TABLES (g_person LABEL Person) \
             EDGE TABLES (g_knows SOURCE KEY (src_id) REFERENCES g_person (id) DESTINATION KEY (dst_id) REFERENCES g_person (id) LABEL Knows)",
        )
        .await;

        instance
            .database_registry()
            .get_database("postgres")
            .expect("default database")
            .close(DatabaseCloseAction::Checkpoint)
            .expect("checkpoint close should succeed");
    }

    let graph_dir = base_dir
        .join("databases")
        .join("db-1")
        .join("graph")
        .join(graph_name);
    std::fs::remove_dir_all(&graph_dir).expect("remove persisted graph projection");

    let restarted = create_persistent_instance(&base_dir);
    let startup_report = restarted.startup_report();

    assert!(
        restarted
            .graph_manager()
            .get(&graph_runtime_key(graph_name))
            .is_some(),
        "graph should be rebuilt and registered during startup"
    );

    let postgres = startup_report
        .databases
        .iter()
        .find(|entry| entry.name == "postgres")
        .expect("startup report should include postgres");
    let hook_results = &postgres
        .recovery_report
        .as_ref()
        .expect("postgres should include recovery report")
        .hook_results;
    assert_eq!(hook_results.len(), 1, "expected exactly one recovery hook");
    match &hook_results[0] {
        RecoveryHookResult::Rebuilt { detail, .. } => {
            assert!(
                detail
                    .as_deref()
                    .unwrap_or_default()
                    .contains("rebuilt 1 graph projection"),
                "rebuild hook detail should mention rebuilt graph count"
            );
        }
        other => panic!("expected graph recovery rebuild result, got {:?}", other),
    }

    let _ = std::fs::remove_dir_all(&base_dir);
}

#[tokio::test]
async fn property_graph_manifest_mismatch_is_repaired_on_restart() {
    let base_dir = create_unique_test_dir("graph_restart_recovery", "manifest_mismatch");
    let graph_name = "restart_manifest_graph";

    {
        let instance = create_persistent_instance(&base_dir);
        let mut session = Session::new(1, Arc::clone(&instance));
        let mut sink = CollectingSink::new();

        exec_ok(
            &mut session,
            &mut sink,
            "CREATE TABLE g_person (id BIGINT PRIMARY KEY, name VARCHAR)",
        )
        .await;
        exec_ok(
            &mut session,
            &mut sink,
            "CREATE TABLE g_knows (src_id BIGINT, dst_id BIGINT)",
        )
        .await;
        exec_ok(
            &mut session,
            &mut sink,
            "INSERT INTO g_person VALUES (1, 'Alice'), (2, 'Bob')",
        )
        .await;
        exec_ok(&mut session, &mut sink, "INSERT INTO g_knows VALUES (1, 2)").await;
        exec_ok(
            &mut session,
            &mut sink,
            "CREATE PROPERTY GRAPH restart_manifest_graph \
             VERTEX TABLES (g_person LABEL Person) \
             EDGE TABLES (g_knows SOURCE KEY (src_id) REFERENCES g_person (id) DESTINATION KEY (dst_id) REFERENCES g_person (id) LABEL Knows)",
        )
        .await;

        instance
            .database_registry()
            .get_database("postgres")
            .expect("default database")
            .close(DatabaseCloseAction::Checkpoint)
            .expect("checkpoint close should succeed");
    }

    let manifest_path = graph_manifest_path(&base_dir, graph_name);
    let mut manifest: serde_json::Value = serde_json::from_slice(
        &std::fs::read(&manifest_path).expect("read persisted graph manifest"),
    )
    .expect("deserialize graph manifest");
    manifest["graph_name"] = serde_json::Value::String("corrupted_graph_name".to_string());
    std::fs::write(
        &manifest_path,
        serde_json::to_vec_pretty(&manifest).expect("serialize corrupted manifest"),
    )
    .expect("write corrupted graph manifest");
    let corrupted_manifest: serde_json::Value = serde_json::from_slice(
        &std::fs::read(&manifest_path).expect("read corrupted graph manifest"),
    )
    .expect("deserialize corrupted graph manifest");
    assert_eq!(
        corrupted_manifest["graph_name"],
        serde_json::Value::String("corrupted_graph_name".to_string())
    );

    let restarted = create_persistent_instance(&base_dir);
    assert!(
        restarted
            .graph_manager()
            .get(&graph_runtime_key(graph_name))
            .is_some(),
        "graph should be rebuilt and registered after manifest mismatch"
    );

    let startup_report = restarted.startup_report();

    let postgres = startup_report
        .databases
        .iter()
        .find(|entry| entry.name == "postgres")
        .expect("startup report should include postgres");
    let hook_results = &postgres
        .recovery_report
        .as_ref()
        .expect("postgres should include recovery report")
        .hook_results;
    assert_eq!(hook_results.len(), 1, "expected exactly one recovery hook");
    let repaired_manifest: serde_json::Value = serde_json::from_slice(
        &std::fs::read(&manifest_path).expect("read repaired graph manifest"),
    )
    .expect("deserialize repaired graph manifest");
    assert_eq!(
        repaired_manifest["graph_name"],
        serde_json::Value::String(graph_name.to_string()),
        "restart should repair the persisted manifest back to the catalog graph name"
    );
    match &hook_results[0] {
        RecoveryHookResult::Rebuilt { issues, .. } => {
            assert!(
                !issues.is_empty(),
                "manifest mismatch rebuild should surface a structured hook issue"
            );
            assert!(
                issues.iter().any(|issue| {
                    issue.kind == paro_instance::RecoveryHookIssueKind::ManifestMismatch
                        && issue
                            .object_name
                            .as_deref()
                            .is_some_and(|name| name == graph_name)
                        && issue.detail.contains("manifest graph name mismatch")
                }),
                "hook issue should explain why the graph was rebuilt"
            );
        }
        RecoveryHookResult::Reused => {}
        other => panic!(
            "expected graph recovery hook to reuse a repaired graph or report a rebuild, got {:?}",
            other
        ),
    }

    if let Some(manifest_issue) = startup_report
        .issues
        .iter()
        .find(|issue| issue.kind == StartupIssueKind::ManifestMismatch)
    {
        assert!(
            manifest_issue.detail.contains(graph_name)
                && manifest_issue
                    .detail
                    .contains("manifest graph name mismatch"),
            "top-level manifest mismatch issue should explain why the graph was rebuilt"
        );
    }

    let _ = std::fs::remove_dir_all(&base_dir);
}

#[tokio::test]
async fn graph_dml_crash_recovery_redelivers_post_commit_backlog() {
    let base_dir = create_unique_test_dir("graph_restart_recovery", "post_commit_backlog");

    {
        let instance = create_persistent_instance(&base_dir);
        let mut session = Session::new(1, Arc::clone(&instance));
        let mut sink = CollectingSink::new();

        exec_ok(
            &mut session,
            &mut sink,
            "CREATE TABLE repair_person (id BIGINT PRIMARY KEY, name VARCHAR)",
        )
        .await;
        exec_ok(
            &mut session,
            &mut sink,
            "CREATE TABLE repair_knows (src_id BIGINT, dst_id BIGINT)",
        )
        .await;
        exec_ok(
            &mut session,
            &mut sink,
            "INSERT INTO repair_person VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol')",
        )
        .await;
        exec_ok(
            &mut session,
            &mut sink,
            "INSERT INTO repair_knows VALUES (1, 2), (2, 3)",
        )
        .await;
        exec_ok(
            &mut session,
            &mut sink,
            "CREATE PROPERTY GRAPH restart_post_commit_graph \
             VERTEX TABLES (repair_person LABEL Person) \
             EDGE TABLES (repair_knows SOURCE KEY (src_id) REFERENCES repair_person (id) DESTINATION KEY (dst_id) REFERENCES repair_person (id) LABEL Knows)",
        )
        .await;

        instance
            .database_registry()
            .get_database("postgres")
            .expect("default database")
            .close(DatabaseCloseAction::Checkpoint)
            .expect("checkpoint close should succeed");
    }

    {
        let instance = create_persistent_instance(&base_dir);
        let mut session = Session::new(2, Arc::clone(&instance));
        let mut sink = CollectingSink::new();

        session
            .begin_explicit_transaction()
            .expect("begin explicit transaction");
        exec_ok(
            &mut session,
            &mut sink,
            "INSERT INTO repair_person VALUES (4, 'Dora')",
        )
        .await;
        durable_commit_without_post_commit(&mut session)
            .expect("durable graph insert should commit before simulated crash");
    }

    let restarted = create_persistent_instance(&base_dir);
    let startup_report = restarted.startup_report();
    let mut session = Session::new(3, Arc::clone(&restarted));
    let mut sink = CollectingSink::new();

    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        exec_ok(
            &mut session,
            &mut sink,
            "SELECT state, vertex_count
             FROM paro_property_graphs()
             WHERE graph_name = 'restart_post_commit_graph'",
        )
        .await;
        let (state, vertex_count) = query_graph_state_and_vertex_count(&sink);
        if state == "READY" && vertex_count == 4 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "startup deferred graph maintenance did not converge in time: state={}, vertex_count={}",
            state,
            vertex_count
        );
        sleep(Duration::from_millis(50)).await;
    }

    exec_ok(
        &mut session,
        &mut sink,
        "SELECT * FROM GRAPH_TABLE(restart_post_commit_graph
         MATCH (n:Person WHERE n.name = 'Dora')
         COLUMNS (n.name AS name)
        ) gt",
    )
    .await;
    assert_eq!(sink.total_rows(), 1);

    let postgres = startup_report
        .databases
        .iter()
        .find(|entry| entry.name == "postgres")
        .expect("startup report should include postgres");
    let hook_results = &postgres
        .recovery_report
        .as_ref()
        .expect("postgres should include recovery report")
        .hook_results;
    assert!(
        hook_results.iter().any(|result| {
            matches!(
                result,
                RecoveryHookResult::Rebuilt {
                    detail: Some(detail),
                    ..
                } if detail.contains("graph maintenance redelivered for 1 graph")
            )
        }),
        "startup should report deferred graph maintenance redelivery, got {:?}",
        hook_results
    );

    let _ = std::fs::remove_dir_all(&base_dir);
}
