#[path = "common/exec_ok.rs"]
mod exec_ok;
#[path = "common/instance_persistent.rs"]
mod instance_persistent;
#[path = "common/query_string_pairs.rs"]
mod query_string_pairs;
#[path = "common/unique_test_dir.rs"]
mod unique_test_dir;

use std::any::Any;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use exec_ok::exec_ok;
use instance_persistent::create_persistent_instance;
use paro_catalog::mvcc::CatalogSnapshot;
use paro_common::error::ParoError;
use paro_common::runtime_value::Value;
use paro_instance::{DatabaseCloseAction, Instance};
use paro_session::{CollectingSink, Session, SessionContextState};
use query_string_pairs::query_string_pairs;
use unique_test_dir::create_unique_test_dir;

#[derive(Clone, Copy, Debug)]
enum CommitPath {
    Auto,
    Explicit,
    Implicit,
}

impl CommitPath {
    fn label(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Explicit => "explicit",
            Self::Implicit => "implicit",
        }
    }
}

#[derive(Debug, Default)]
struct TxnLifecycleState {
    begin_count: AtomicU32,
    commit_count: AtomicU32,
    rollback_count: AtomicU32,
    rollback_with_error_count: AtomicU32,
}

impl SessionContextState for TxnLifecycleState {
    fn transaction_begin(&mut self) {
        self.begin_count.fetch_add(1, Ordering::Relaxed);
    }

    fn transaction_commit(&mut self) {
        self.commit_count.fetch_add(1, Ordering::Relaxed);
    }

    fn transaction_rollback(&mut self, error: Option<&ParoError>) {
        self.rollback_count.fetch_add(1, Ordering::Relaxed);
        if error.is_some() {
            self.rollback_with_error_count
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

fn wal_size(session: &Session) -> u64 {
    session
        .current_database
        .wal()
        .expect("persistent database should expose a WAL")
        .file_size()
}

fn query_i64_values(sink: &CollectingSink, col_idx: usize) -> Vec<i64> {
    let result = sink.assert_single_result();
    let mut out = Vec::new();
    for chunk in &result.chunks {
        let col = chunk
            .column(col_idx)
            .expect("missing expected result column");
        for row in 0..chunk.len() {
            match col.get_value(row) {
                Value::TinyInt(v) => out.push(v as i64),
                Value::SmallInt(v) => out.push(v as i64),
                Value::Integer(v) => out.push(v as i64),
                Value::BigInt(v) => out.push(v),
                other => panic!("unexpected integer result value: {:?}", other),
            }
        }
    }
    out
}

fn assert_txn_counts(
    session: &Session,
    expected_begin: u32,
    expected_commit: u32,
    expected_rollback: u32,
    expected_rollback_with_error: u32,
) {
    let state = session
        .get_state("txn")
        .expect("registered state should exist");
    let guard = state.lock().expect("txn state lock");
    let txn = guard
        .as_any()
        .downcast_ref::<TxnLifecycleState>()
        .expect("txn state type");
    assert_eq!(txn.begin_count.load(Ordering::Relaxed), expected_begin);
    assert_eq!(txn.commit_count.load(Ordering::Relaxed), expected_commit);
    assert_eq!(
        txn.rollback_count.load(Ordering::Relaxed),
        expected_rollback
    );
    assert_eq!(
        txn.rollback_with_error_count.load(Ordering::Relaxed),
        expected_rollback_with_error
    );
}

async fn run_graph_commit_path(path: CommitPath) {
    let base_dir = create_unique_test_dir(
        "transaction_finalization",
        &format!("graph_{}", path.label()),
    );
    let instance = create_persistent_instance(&base_dir);
    let mut session = Session::new(1, Arc::clone(&instance));
    let mut sink = CollectingSink::new();

    let prefix = format!("txn_finalization_{}", path.label());
    let person_table = format!("{prefix}_person");
    let edge_table = format!("{prefix}_knows");
    let graph_name = format!("{prefix}_graph");

    exec_ok(
        &mut session,
        &mut sink,
        &format!("CREATE TABLE {person_table} (id BIGINT PRIMARY KEY, name VARCHAR)"),
    )
    .await;
    exec_ok(
        &mut session,
        &mut sink,
        &format!("CREATE TABLE {edge_table} (src_id BIGINT, dst_id BIGINT)"),
    )
    .await;
    exec_ok(
        &mut session,
        &mut sink,
        &format!("INSERT INTO {person_table} VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol')"),
    )
    .await;
    exec_ok(
        &mut session,
        &mut sink,
        &format!("INSERT INTO {edge_table} VALUES (1, 2)"),
    )
    .await;
    exec_ok(
        &mut session,
        &mut sink,
        &format!(
            "CREATE PROPERTY GRAPH {graph_name} \
             VERTEX TABLES ({person_table} LABEL Person) \
             EDGE TABLES ({edge_table} SOURCE KEY (src_id) REFERENCES {person_table} (id) DESTINATION KEY (dst_id) REFERENCES {person_table} (id) LABEL Knows)"
        ),
    )
    .await;

    session.register_state("txn", TxnLifecycleState::default());

    match path {
        CommitPath::Auto => {
            exec_ok(
                &mut session,
                &mut sink,
                &format!("INSERT INTO {edge_table} VALUES (2, 3)"),
            )
            .await;
        }
        CommitPath::Explicit => {
            session
                .begin_explicit_transaction()
                .expect("explicit BEGIN should succeed");
            exec_ok(
                &mut session,
                &mut sink,
                &format!("INSERT INTO {edge_table} VALUES (2, 3)"),
            )
            .await;
            session
                .commit_transaction()
                .expect("explicit COMMIT should succeed");
        }
        CommitPath::Implicit => {
            exec_ok(
                &mut session,
                &mut sink,
                &format!(
                    "INSERT INTO {edge_table} VALUES (2, 3); SELECT COUNT(*) FROM {person_table}"
                ),
            )
            .await;
        }
    }
    assert_txn_counts(&session, 1, 1, 0, 0);

    exec_ok(
        &mut session,
        &mut sink,
        &format!("SELECT edge_count FROM paro_property_graphs() WHERE graph_name = '{graph_name}'"),
    )
    .await;
    assert_eq!(query_i64_values(&sink, 0), vec![2], "{path:?} edge count");

    exec_ok(
        &mut session,
        &mut sink,
        &format!(
            "SELECT src, dst FROM GRAPH_TABLE({graph_name} \
             MATCH (a:Person)-[e:Knows]->(b:Person) \
             COLUMNS (a.name AS src, b.name AS dst)) gt \
             ORDER BY src, dst"
        ),
    )
    .await;
    assert_eq!(
        query_string_pairs(&sink),
        vec![
            ("Alice".to_string(), "Bob".to_string()),
            ("Bob".to_string(), "Carol".to_string()),
        ],
        "{path:?} graph rows"
    );

    let _ = std::fs::remove_dir_all(&base_dir);
}

#[tokio::test]
async fn ddl_commit_paths_write_wal_and_survive_restart() {
    for path in [CommitPath::Auto, CommitPath::Explicit, CommitPath::Implicit] {
        let base_dir = create_unique_test_dir("transaction_finalization", path.label());
        let instance = create_persistent_instance(&base_dir);
        let mut session = Session::new(1, Arc::clone(&instance));
        let mut sink = CollectingSink::new();
        let before_wal = wal_size(&session);

        let tables: Vec<(String, i64)> = match path {
            CommitPath::Auto => {
                let table = format!("{}_ddl_items", path.label());
                exec_ok(
                    &mut session,
                    &mut sink,
                    &format!("CREATE TABLE {table} (id INT)"),
                )
                .await;
                vec![(table, 0)]
            }
            CommitPath::Explicit => {
                let table = format!("{}_ddl_items", path.label());
                session
                    .begin_explicit_transaction()
                    .expect("explicit BEGIN should succeed");
                exec_ok(
                    &mut session,
                    &mut sink,
                    &format!("CREATE TABLE {table} (id INT)"),
                )
                .await;
                exec_ok(
                    &mut session,
                    &mut sink,
                    &format!("INSERT INTO {table} VALUES (1)"),
                )
                .await;
                session
                    .commit_transaction()
                    .expect("explicit COMMIT should succeed");
                vec![(table, 1)]
            }
            CommitPath::Implicit => {
                let table = format!("{}_ddl_items", path.label());
                exec_ok(
                    &mut session,
                    &mut sink,
                    &format!("CREATE TABLE {table} (id INT); INSERT INTO {table} VALUES (1)"),
                )
                .await;
                vec![(table, 1)]
            }
        };

        let after_wal = wal_size(&session);
        assert!(
            after_wal > before_wal,
            "{path:?} commit path should append DDL to WAL: before={before_wal}, after={after_wal}"
        );

        session
            .current_database
            .close(DatabaseCloseAction::Checkpoint)
            .expect("checkpoint close should succeed");
        drop(session);
        drop(instance);

        let restarted = create_persistent_instance(&base_dir);
        let mut restarted_session = Session::new(2, Arc::clone(&restarted));
        for (table, expected_rows) in &tables {
            exec_ok(
                &mut restarted_session,
                &mut sink,
                &format!("SELECT COUNT(*) FROM {table}"),
            )
            .await;
            assert_eq!(
                query_i64_values(&sink, 0),
                vec![*expected_rows],
                "{path:?} table {table} should survive restart with the expected rows"
            );
        }

        let _ = std::fs::remove_dir_all(&base_dir);
    }
}

#[tokio::test]
async fn graph_dml_hooks_run_across_all_commit_paths() {
    for path in [CommitPath::Auto, CommitPath::Explicit, CommitPath::Implicit] {
        run_graph_commit_path(path).await;
    }
}

#[tokio::test]
async fn create_property_graph_rollback_discards_staging_and_never_publishes_final_dir() {
    let base_dir = create_unique_test_dir("transaction_finalization", "graph_staging_rollback");
    let instance = create_persistent_instance(&base_dir);
    let mut session = Session::new(1, Arc::clone(&instance));
    let mut sink = CollectingSink::new();

    exec_ok(
        &mut session,
        &mut sink,
        "CREATE TABLE pg_stage_person (id BIGINT PRIMARY KEY, name VARCHAR)",
    )
    .await;
    exec_ok(
        &mut session,
        &mut sink,
        "CREATE TABLE pg_stage_knows (src_id BIGINT, dst_id BIGINT)",
    )
    .await;
    exec_ok(
        &mut session,
        &mut sink,
        "INSERT INTO pg_stage_person VALUES (1, 'Alice'), (2, 'Bob')",
    )
    .await;
    exec_ok(
        &mut session,
        &mut sink,
        "INSERT INTO pg_stage_knows VALUES (1, 2)",
    )
    .await;

    session
        .begin_explicit_transaction()
        .expect("explicit BEGIN should succeed");
    let raw_txn_id = session.active_transaction().expect("active txn").id;
    let graph_name = "pg_stage_graph";
    exec_ok(
        &mut session,
        &mut sink,
        "CREATE PROPERTY GRAPH pg_stage_graph \
         VERTEX TABLES (pg_stage_person LABEL Person) \
         EDGE TABLES (pg_stage_knows SOURCE KEY (src_id) REFERENCES pg_stage_person (id) DESTINATION KEY (dst_id) REFERENCES pg_stage_person (id) LABEL Knows)",
    )
    .await;

    let db_path = PathBuf::from(session.current_database.path());
    let staging_dir = db_path
        .join(".txn-staging")
        .join(raw_txn_id.to_string())
        .join("graph")
        .join(graph_name);
    let final_dir = db_path.join("graph").join(graph_name);
    assert!(
        staging_dir.exists(),
        "statement should materialize graph staging"
    );
    assert!(
        !final_dir.exists(),
        "final graph dir should remain untouched until commit"
    );

    session
        .rollback_transaction()
        .expect("explicit ROLLBACK should succeed");

    assert!(
        !staging_dir.exists(),
        "rollback should discard staged property graph artifact"
    );
    assert!(
        !final_dir.exists(),
        "rollback must not publish the final property graph dir"
    );
    let txn = session.catalog_txn_view();
    let schema = session
        .current_database
        .catalog()
        .get_schema(&txn, "public")
        .expect("public schema");
    assert!(
        schema.get_property_graph(&txn, graph_name).is_err(),
        "rolled-back property graph should stay invisible in catalog"
    );
}

#[tokio::test]
async fn mixed_catalog_objects_commit_and_rollback_recover_cleanly_on_restart() {
    let base_dir = create_unique_test_dir("transaction_finalization", "mixed_catalog_restart");
    let mut sink = CollectingSink::new();

    {
        let instance = create_persistent_instance(&base_dir);
        let mut session = Session::new(1, Arc::clone(&instance));

        exec_ok(&mut session, &mut sink, "CREATE SCHEMA committed_mix").await;
        session
            .set_schema("committed_mix")
            .expect("set committed schema should succeed");

        session
            .begin_explicit_transaction()
            .expect("explicit BEGIN should succeed");
        exec_ok(
            &mut session,
            &mut sink,
            "CREATE TABLE person (id BIGINT PRIMARY KEY, name VARCHAR)",
        )
        .await;
        exec_ok(
            &mut session,
            &mut sink,
            "CREATE TABLE knows (src_id BIGINT, dst_id BIGINT)",
        )
        .await;
        exec_ok(
            &mut session,
            &mut sink,
            "CREATE VIEW person_view AS SELECT name FROM person",
        )
        .await;
        exec_ok(
            &mut session,
            &mut sink,
            "CREATE INDEX idx_person_name_fts ON person USING GIN (to_tsvector('simple', name))",
        )
        .await;
        exec_ok(
            &mut session,
            &mut sink,
            "CREATE PROPERTY GRAPH mix_graph \
             VERTEX TABLES (person LABEL Person) \
             EDGE TABLES (knows SOURCE KEY (src_id) REFERENCES person (id) DESTINATION KEY (dst_id) REFERENCES person (id) LABEL Knows)",
        )
        .await;
        session
            .commit_transaction()
            .expect("explicit COMMIT should succeed");
        exec_ok(
            &mut session,
            &mut sink,
            "INSERT INTO person VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol')",
        )
        .await;
        exec_ok(
            &mut session,
            &mut sink,
            "INSERT INTO knows VALUES (1, 2), (2, 3)",
        )
        .await;

        session
            .begin_explicit_transaction()
            .expect("explicit BEGIN should succeed");
        exec_ok(&mut session, &mut sink, "CREATE SCHEMA rolled_back_schema").await;
        session
            .set_schema("rolled_back_schema")
            .expect("set staged schema should succeed inside transaction");
        exec_ok(
            &mut session,
            &mut sink,
            "CREATE TABLE rollback_person (id BIGINT PRIMARY KEY, name VARCHAR)",
        )
        .await;
        exec_ok(
            &mut session,
            &mut sink,
            "CREATE TABLE rollback_knows (src_id BIGINT, dst_id BIGINT)",
        )
        .await;
        exec_ok(
            &mut session,
            &mut sink,
            "CREATE VIEW rollback_view AS SELECT name FROM rollback_person",
        )
        .await;
        exec_ok(
            &mut session,
            &mut sink,
            "CREATE INDEX rollback_idx ON rollback_person USING GIN (to_tsvector('simple', name))",
        )
        .await;
        exec_ok(
            &mut session,
            &mut sink,
            "CREATE PROPERTY GRAPH rollback_graph \
             VERTEX TABLES (rollback_person LABEL Person) \
             EDGE TABLES (rollback_knows SOURCE KEY (src_id) REFERENCES rollback_person (id) DESTINATION KEY (dst_id) REFERENCES rollback_person (id) LABEL Knows)",
        )
        .await;
        session
            .rollback_transaction()
            .expect("explicit ROLLBACK should succeed");
        session
            .set_schema("committed_mix")
            .expect("reset schema after rollback should succeed");

        session
            .current_database
            .close(DatabaseCloseAction::Checkpoint)
            .expect("checkpoint close should succeed");
    }

    {
        let restarted = create_persistent_instance(&base_dir);
        let mut session = Session::new(2, Arc::clone(&restarted));
        session
            .set_schema("committed_mix")
            .expect("set schema after restart should succeed");

        exec_ok(&mut session, &mut sink, "SELECT COUNT(*) FROM person_view").await;
        assert_eq!(query_i64_values(&sink, 0), vec![3]);

        exec_ok(
            &mut session,
            &mut sink,
            "SELECT src, dst FROM GRAPH_TABLE(mix_graph \
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

        let txn = CatalogSnapshot::read_only(u64::MAX);
        let committed_schema = session
            .current_database
            .catalog()
            .get_schema(&txn, "committed_mix")
            .expect("committed schema should survive restart");
        assert!(committed_schema
            .get_table(txn.transaction_id, txn.start_time, "person")
            .is_some());
        assert!(committed_schema
            .get_view(txn.transaction_id, txn.start_time, "person_view")
            .is_some());
        assert!(committed_schema
            .get_index(txn.transaction_id, txn.start_time, "idx_person_name_fts")
            .is_some());
        assert!(committed_schema
            .get_property_graph(&txn, "mix_graph")
            .is_ok());

        assert!(
            session
                .current_database
                .catalog()
                .get_schema(&txn, "rolled_back_schema")
                .is_err(),
            "rolled-back schema should not survive restart"
        );
    }

    let _ = std::fs::remove_dir_all(&base_dir);
}

#[tokio::test]
async fn auto_transaction_rollback_notifies_with_error_cause() {
    let instance = Instance::new_in_memory();
    let mut session = Session::new(1, instance);
    let mut sink = CollectingSink::new();

    session.register_state("txn", TxnLifecycleState::default());

    let result = session
        .execute_simple_query("SELECT * FROM missing_rollback_table", &mut sink)
        .await;
    assert!(result.is_err(), "missing table should fail compilation");
    assert!(
        sink.has_errors(),
        "failing query should be reported through the result sink"
    );
    assert!(
        !session.has_active_transaction(),
        "auto rollback should clear the failed auto-transaction"
    );
    assert_txn_counts(&session, 1, 0, 1, 1);
}
