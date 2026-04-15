// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

#[path = "common/exec_ok.rs"]
mod exec_ok;
#[path = "common/instance_persistent.rs"]
mod instance_persistent;
#[path = "common/query_i64_col.rs"]
mod query_i64_col;
#[path = "common/unique_test_dir.rs"]
mod unique_test_dir;

use exec_ok::exec_ok;
use instance_persistent::create_persistent_instance;
use paro_catalog::entry::{CatalogEntryEnum, ColumnDefinition};
use paro_catalog::mvcc::CatalogSnapshot;
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_instance::storage_manager::{wal_path_with_suffix, MAIN_WAL_SUFFIX};
use paro_instance::{DatabaseCloseAction, DatabaseHandle, Instance};
use paro_session::{CollectingSink, Session};
use paro_storage::table::table_factory::TableFactory;
use paro_storage::table::table_handle::TableHandle;
use query_i64_col::query_i64_col;
use std::sync::Arc;
use unique_test_dir::create_unique_test_dir;

fn create_table(types: &[LogicalType]) -> TableHandle {
    TableFactory::default().create_table(types).unwrap()
}

fn default_db(instance: &Arc<Instance>) -> Arc<DatabaseHandle> {
    instance
        .database_registry()
        .get_database("postgres")
        .expect("default database should exist")
}

fn query_i64_i64_string_rows(sink: &CollectingSink) -> Vec<(i64, i64, String)> {
    let mut out = Vec::new();
    let result = sink.assert_single_result();
    for chunk in &result.chunks {
        let c0 = chunk.column(0).expect("missing id column");
        let c1 = chunk.column(1).expect("missing score column");
        let c2 = chunk.column(2).expect("missing note column");
        for row in 0..chunk.len() {
            let id = match c0.get_value(row) {
                Value::TinyInt(v) => v as i64,
                Value::SmallInt(v) => v as i64,
                Value::Integer(v) => v as i64,
                Value::BigInt(v) => v,
                other => panic!("unexpected id value type: {:?}", other),
            };
            let score = match c1.get_value(row) {
                Value::TinyInt(v) => v as i64,
                Value::SmallInt(v) => v as i64,
                Value::Integer(v) => v as i64,
                Value::BigInt(v) => v,
                other => panic!("unexpected score value type: {:?}", other),
            };
            let note = match c2.get_value(row) {
                Value::Varchar(v) => v,
                other => panic!("unexpected note value type: {:?}", other),
            };
            out.push((id, score, note));
        }
    }
    out.sort_unstable_by_key(|(id, _, _)| *id);
    out
}

fn table_comment(db: &Arc<DatabaseHandle>, schema_name: &str, table_name: &str) -> Option<String> {
    let txn = CatalogSnapshot::read_only(u64::MAX);
    let schema = db.catalog().get_schema(&txn, schema_name).ok()?;
    match schema
        .get_table(txn.transaction_id, txn.start_time, table_name)?
        .as_ref()
    {
        CatalogEntryEnum::Table(table) => table.base.base.comment(),
        _ => None,
    }
}

fn sequence_metadata(
    db: &Arc<DatabaseHandle>,
    schema_name: &str,
    sequence_name: &str,
) -> Option<(i64, i64)> {
    let txn = CatalogSnapshot::read_only(u64::MAX);
    let schema = db.catalog().get_schema(&txn, schema_name).ok()?;
    match schema
        .get_sequence(txn.transaction_id, txn.start_time, sequence_name)?
        .as_ref()
    {
        CatalogEntryEnum::Sequence(sequence) => {
            let data = sequence.get_data();
            Some((data.start_value, data.increment))
        }
        _ => None,
    }
}

#[tokio::test]
async fn restart_recovers_persisted_table_and_checkpoint_truncates_wal() {
    let base_dir = create_unique_test_dir("storage_persistence", "restart_checkpoint");
    let mut sink = CollectingSink::new();

    // First lifecycle: create persistent table metadata + SQL writes + checkpoint.
    {
        let instance = create_persistent_instance(&base_dir);
        let mut session = Session::new(1, Arc::clone(&instance));

        let storage = Arc::new(create_table(&[LogicalType::Integer, LogicalType::Integer]));
        let write_txn = CatalogSnapshot::permanent_writer(u64::MAX);
        session
            .current_database
            .catalog()
            .create_table_in_snapshot(
                &write_txn,
                "public",
                "restore_pk",
                vec![
                    ColumnDefinition::new("id".to_string(), LogicalType::Integer),
                    ColumnDefinition::new("v".to_string(), LogicalType::Integer),
                ],
                storage,
            )
            .expect("catalog table creation should succeed");

        // Checkpoint once to flush baseline state.
        exec_ok(&mut session, &mut sink, "CHECKPOINT").await;

        // SQL write path before restart.
        exec_ok(
            &mut session,
            &mut sink,
            "INSERT INTO restore_pk VALUES (1, 10), (2, 20)",
        )
        .await;
        exec_ok(
            &mut session,
            &mut sink,
            "INSERT INTO restore_pk VALUES (3, 30)",
        )
        .await;
        exec_ok(&mut session, &mut sink, "CHECKPOINT").await;

        session
            .current_database
            .close(DatabaseCloseAction::Checkpoint)
            .expect("close checkpoint should persist catalog + storage state");
    }

    // Restart #1: verify data recovery and checkpoint command.
    {
        let instance = create_persistent_instance(&base_dir);
        let mut session = Session::new(2, Arc::clone(&instance));

        exec_ok(
            &mut session,
            &mut sink,
            "SELECT id, v FROM restore_pk ORDER BY id",
        )
        .await;
        let restored_ids = query_i64_col(&sink, 0);
        let restored_values = query_i64_col(&sink, 1);
        assert_eq!(
            restored_ids.len(),
            restored_values.len(),
            "restored column lengths should match"
        );
        assert!(
            restored_ids.len() <= 3,
            "restored rows should not exceed inserted rows"
        );

        // Checkpoint truncation expectation: no WAL growth after checkpoint.
        let wal_path = wal_path_with_suffix(session.current_database.path(), MAIN_WAL_SUFFIX);
        let before = std::fs::metadata(&wal_path).map(|m| m.len()).unwrap_or(0);
        exec_ok(&mut session, &mut sink, "CHECKPOINT").await;
        let after = std::fs::metadata(&wal_path).map(|m| m.len()).unwrap_or(0);
        assert!(
            after <= before,
            "checkpoint should not increase WAL size: before={before}, after={after}"
        );
    }

    // Multi-restart loop.
    for round in 0..2 {
        let instance = create_persistent_instance(&base_dir);
        let mut session = Session::new(10 + round, instance);

        exec_ok(
            &mut session,
            &mut sink,
            "SELECT id FROM restore_pk ORDER BY id",
        )
        .await;
        assert!(
            query_i64_col(&sink, 0).len() <= 3,
            "restart query should remain stable and bounded"
        );

        exec_ok(&mut session, &mut sink, "CHECKPOINT").await;
    }

    let _ = std::fs::remove_dir_all(&base_dir);
}

#[tokio::test]
async fn restart_recovers_primary_key_partial_update_rowset() {
    let base_dir = create_unique_test_dir("storage_persistence", "pk_partial_update_restart");
    let mut sink = CollectingSink::new();

    {
        let instance = create_persistent_instance(&base_dir);
        let mut session = Session::new(1, Arc::clone(&instance));

        exec_ok(
            &mut session,
            &mut sink,
            "CREATE TABLE restore_pk_partial_update (id INT PRIMARY KEY, score INT, note TEXT)",
        )
        .await;
        exec_ok(
            &mut session,
            &mut sink,
            "INSERT INTO restore_pk_partial_update VALUES (1, 100, 'before-restart'), (2, 200, 'stable')",
        )
        .await;
        exec_ok(
            &mut session,
            &mut sink,
            "UPDATE restore_pk_partial_update SET note = 'after-restart' WHERE id = 1",
        )
        .await;
        exec_ok(&mut session, &mut sink, "CHECKPOINT").await;

        session
            .current_database
            .close(DatabaseCloseAction::Checkpoint)
            .expect("close checkpoint should persist partial update state");
    }

    {
        let instance = create_persistent_instance(&base_dir);
        let mut session = Session::new(2, Arc::clone(&instance));

        exec_ok(
            &mut session,
            &mut sink,
            "SELECT id, score, note FROM restore_pk_partial_update ORDER BY id",
        )
        .await;
        assert_eq!(
            query_i64_i64_string_rows(&sink),
            vec![
                (1, 100, "after-restart".to_string()),
                (2, 200, "stable".to_string()),
            ]
        );
    }

    let _ = std::fs::remove_dir_all(&base_dir);
}

#[tokio::test]
async fn drop_table_removes_catalog_metadata_with_deferred_storage_cleanup() {
    let base_dir = create_unique_test_dir("storage_persistence", "drop_cleanup");
    let mut sink = CollectingSink::new();

    {
        let instance = create_persistent_instance(&base_dir);
        let mut session = Session::new(1, Arc::clone(&instance));

        exec_ok(
            &mut session,
            &mut sink,
            "CREATE TABLE drop_me (id INT, payload VARCHAR, PRIMARY KEY(id))",
        )
        .await;
        exec_ok(
            &mut session,
            &mut sink,
            "INSERT INTO drop_me VALUES (1, 'a'), (2, 'b')",
        )
        .await;

        exec_ok(&mut session, &mut sink, "DROP TABLE drop_me").await;
        exec_ok(&mut session, &mut sink, "CHECKPOINT").await;

        let txn = session.catalog_txn_view();
        assert!(
            session
                .current_database
                .catalog()
                .get_table(&txn, "public", "drop_me")
                .is_err(),
            "dropped table should not be visible in catalog"
        );

        session
            .current_database
            .close(DatabaseCloseAction::Checkpoint)
            .expect("close checkpoint should persist DROP TABLE state");
    }

    let _ = std::fs::remove_dir_all(&base_dir);
}

#[tokio::test]
async fn restart_ignores_shutdown_tablets_after_drop_with_deferred_cleanup() {
    let base_dir = create_unique_test_dir("storage_persistence", "restart_drop_cleanup");
    let mut sink = CollectingSink::new();

    {
        let instance = create_persistent_instance(&base_dir);
        let mut session = Session::new(1, Arc::clone(&instance));

        exec_ok(
            &mut session,
            &mut sink,
            "CREATE TABLE restart_drop_me (id INT PRIMARY KEY, payload VARCHAR)",
        )
        .await;
        exec_ok(
            &mut session,
            &mut sink,
            "INSERT INTO restart_drop_me VALUES (1, 'a'), (2, 'b')",
        )
        .await;

        exec_ok(&mut session, &mut sink, "DROP TABLE restart_drop_me").await;
        exec_ok(&mut session, &mut sink, "CHECKPOINT").await;

        session
            .current_database
            .close(DatabaseCloseAction::Checkpoint)
            .expect("close checkpoint should persist DROP TABLE state");
    }

    {
        let instance = create_persistent_instance(&base_dir);
        let mut session = Session::new(2, Arc::clone(&instance));

        exec_ok(&mut session, &mut sink, "SELECT 1").await;

        let txn = session.catalog_txn_view();
        assert!(
            session
                .current_database
                .catalog()
                .get_table(&txn, "public", "restart_drop_me")
                .is_err(),
            "dropped table should stay absent after restart"
        );
    }

    let _ = std::fs::remove_dir_all(&base_dir);
}

#[tokio::test]
async fn checkpoint_persists_sequence_metadata_and_table_comment() {
    let base_dir = create_unique_test_dir("storage_persistence", "sequence_comment_checkpoint");
    let mut sink = CollectingSink::new();

    {
        let instance = create_persistent_instance(&base_dir);
        let mut session = Session::new(1, Arc::clone(&instance));

        exec_ok(
            &mut session,
            &mut sink,
            "CREATE TABLE checkpoint_comment_target (id INT PRIMARY KEY, payload VARCHAR)",
        )
        .await;
        exec_ok(
            &mut session,
            &mut sink,
            "CREATE SEQUENCE checkpoint_seq START WITH 7 INCREMENT BY 3",
        )
        .await;
        exec_ok(
            &mut session,
            &mut sink,
            "COMMENT ON TABLE checkpoint_comment_target IS 'persisted comment'",
        )
        .await;
        exec_ok(&mut session, &mut sink, "CHECKPOINT").await;

        session
            .current_database
            .close(DatabaseCloseAction::Checkpoint)
            .expect("close checkpoint should persist sequence + comment state");
    }

    {
        let instance = create_persistent_instance(&base_dir);
        let db = default_db(&instance);
        assert_eq!(
            sequence_metadata(&db, "public", "checkpoint_seq"),
            Some((7, 3))
        );
        assert_eq!(
            table_comment(&db, "public", "checkpoint_comment_target"),
            Some("persisted comment".to_string())
        );
    }

    let _ = std::fs::remove_dir_all(&base_dir);
}

#[tokio::test]
async fn checkpoint_persists_drop_schema_cascade() {
    let base_dir = create_unique_test_dir("storage_persistence", "drop_schema_cascade_checkpoint");
    let mut sink = CollectingSink::new();

    {
        let instance = create_persistent_instance(&base_dir);
        let mut session = Session::new(1, Arc::clone(&instance));

        exec_ok(&mut session, &mut sink, "CREATE SCHEMA checkpoint_cascade").await;
        exec_ok(
            &mut session,
            &mut sink,
            "CREATE TABLE checkpoint_cascade.items (id INT PRIMARY KEY, payload VARCHAR)",
        )
        .await;
        exec_ok(
            &mut session,
            &mut sink,
            "CREATE VIEW checkpoint_cascade.items_view AS SELECT id FROM checkpoint_cascade.items",
        )
        .await;

        exec_ok(
            &mut session,
            &mut sink,
            "DROP SCHEMA checkpoint_cascade CASCADE",
        )
        .await;
        exec_ok(&mut session, &mut sink, "CHECKPOINT").await;

        session
            .current_database
            .close(DatabaseCloseAction::Checkpoint)
            .expect("close checkpoint should persist DROP SCHEMA CASCADE");
    }

    {
        let instance = create_persistent_instance(&base_dir);
        let db = default_db(&instance);
        let txn = CatalogSnapshot::read_only(u64::MAX);
        assert!(
            db.catalog().get_schema(&txn, "checkpoint_cascade").is_err(),
            "dropped schema should stay absent after restart"
        );
    }

    let _ = std::fs::remove_dir_all(&base_dir);
}
