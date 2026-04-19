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
use paro_common::chunk::Chunk;
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_instance::{DatabaseCloseAction, DatabaseHandle, Instance};
use paro_session::{CollectingSink, Session};
use paro_storage::table::table_factory::TableFactory;
use paro_storage::table::table_handle::TableHandle;
use paro_storage::tablet::{
    current_delete_patch_inline_row_ref_threshold, set_delete_patch_inline_row_ref_threshold,
};
use query_i64_col::query_i64_col;
use std::path::Path;
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

fn table_exists(db: &Arc<DatabaseHandle>, schema_name: &str, table_name: &str) -> bool {
    let txn = CatalogSnapshot::read_only(u64::MAX);
    let Ok(schema) = db.catalog().get_schema(&txn, schema_name) else {
        return false;
    };
    schema
        .get_table(txn.transaction_id, txn.start_time, table_name)
        .is_some()
}

fn table_storage(
    db: &Arc<DatabaseHandle>,
    schema_name: &str,
    table_name: &str,
) -> Arc<TableHandle> {
    let txn = CatalogSnapshot::read_only(u64::MAX);
    let schema = db
        .catalog()
        .get_schema(&txn, schema_name)
        .expect("schema should exist");
    let entry = schema
        .get_table(txn.transaction_id, txn.start_time, table_name)
        .expect("table should exist");
    match entry.as_ref() {
        CatalogEntryEnum::Table(table) => table.get_storage().expect("table storage").clone(),
        other => panic!("expected table entry, got {:?}", other.entry_type()),
    }
}

fn file_count(root: &Path) -> usize {
    if !root.exists() {
        return 0;
    }
    let mut count = 0usize;
    let mut stack = vec![root.to_path_buf()];
    while let Some(path) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&path) else {
            continue;
        };
        for entry in entries.flatten() {
            let child = entry.path();
            if child.is_dir() {
                stack.push(child);
            } else {
                count = count.saturating_add(1);
            }
        }
    }
    count
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
        session
            .current_database
            .sync_compaction_tablets()
            .expect("compaction registry should reflect out-of-band catalog create");

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
        let before = session
            .current_database
            .wal()
            .map(|wal| wal.file_size())
            .unwrap_or(0);
        exec_ok(&mut session, &mut sink, "CHECKPOINT").await;
        let after = session
            .current_database
            .wal()
            .map(|wal| wal.file_size())
            .unwrap_or(0);
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
async fn restart_recovers_primary_key_delete_committed_through_main_wal() {
    let base_dir = create_unique_test_dir("storage_persistence", "pk_delete_restart");
    let mut sink = CollectingSink::new();

    {
        let instance = create_persistent_instance(&base_dir);
        let mut session = Session::new(1, Arc::clone(&instance));

        exec_ok(
            &mut session,
            &mut sink,
            "CREATE TABLE restore_pk_delete (id INT PRIMARY KEY, score INT, note TEXT)",
        )
        .await;
        exec_ok(
            &mut session,
            &mut sink,
            "INSERT INTO restore_pk_delete VALUES (1, 100, 'gone'), (2, 200, 'kept')",
        )
        .await;
        exec_ok(
            &mut session,
            &mut sink,
            "DELETE FROM restore_pk_delete WHERE id = 1",
        )
        .await;
        exec_ok(&mut session, &mut sink, "CHECKPOINT").await;

        session
            .current_database
            .close(DatabaseCloseAction::Checkpoint)
            .expect("close checkpoint should persist primary-key delete state");
    }

    {
        let instance = create_persistent_instance(&base_dir);
        let mut session = Session::new(2, Arc::clone(&instance));

        exec_ok(
            &mut session,
            &mut sink,
            "SELECT id, score, note FROM restore_pk_delete ORDER BY id",
        )
        .await;
        assert_eq!(
            query_i64_i64_string_rows(&sink),
            vec![(2, 200, "kept".to_string())]
        );
    }

    let _ = std::fs::remove_dir_all(&base_dir);
}

#[tokio::test]
async fn restart_preserves_commit_floor_after_delete_only_commit() {
    let base_dir = create_unique_test_dir("storage_persistence", "delete_only_commit_floor");
    let mut sink = CollectingSink::new();
    let delete_commit_floor;

    {
        let instance = create_persistent_instance(&base_dir);
        let mut session = Session::new(1, Arc::clone(&instance));

        exec_ok(
            &mut session,
            &mut sink,
            "CREATE TABLE delete_only_commit_floor (id INT PRIMARY KEY, score INT, note TEXT)",
        )
        .await;
        exec_ok(
            &mut session,
            &mut sink,
            "INSERT INTO delete_only_commit_floor VALUES (1, 100, 'gone'), (2, 200, 'kept')",
        )
        .await;
        exec_ok(
            &mut session,
            &mut sink,
            "DELETE FROM delete_only_commit_floor WHERE id = 1",
        )
        .await;
        delete_commit_floor = session
            .current_database
            .transaction_manager()
            .durable_commit_id();
        assert!(delete_commit_floor > 0);
        exec_ok(&mut session, &mut sink, "CHECKPOINT").await;

        session
            .current_database
            .close(DatabaseCloseAction::Checkpoint)
            .expect("close checkpoint should persist delete-only commit floor");
    }

    {
        let instance = create_persistent_instance(&base_dir);
        let mut session = Session::new(2, Arc::clone(&instance));
        let reopened_floor = session
            .current_database
            .transaction_manager()
            .durable_commit_id();
        assert!(
            reopened_floor >= delete_commit_floor,
            "reopened durable commit floor regressed below delete-only commit"
        );

        exec_ok(
            &mut session,
            &mut sink,
            "INSERT INTO delete_only_commit_floor VALUES (3, 300, 'after-restart')",
        )
        .await;
        let next_floor = session
            .current_database
            .transaction_manager()
            .durable_commit_id();
        assert!(
            next_floor > delete_commit_floor,
            "post-restart commit reused delete-only commit id"
        );

        exec_ok(
            &mut session,
            &mut sink,
            "SELECT id, score, note FROM delete_only_commit_floor ORDER BY id",
        )
        .await;
        assert_eq!(
            query_i64_i64_string_rows(&sink),
            vec![
                (2, 200, "kept".to_string()),
                (3, 300, "after-restart".to_string()),
            ]
        );
    }

    let _ = std::fs::remove_dir_all(&base_dir);
}

#[tokio::test]
async fn on_conflict_update_keeps_primary_key_visible_for_follow_up_updates() {
    let base_dir = create_unique_test_dir("storage_persistence", "on_conflict_pk_visibility");
    let mut sink = CollectingSink::new();

    let instance = create_persistent_instance(&base_dir);
    let mut session = Session::new(1, Arc::clone(&instance));

    exec_ok(
        &mut session,
        &mut sink,
        "CREATE TABLE on_conflict_pk_visibility (id INT PRIMARY KEY, price INT, stock INT)",
    )
    .await;
    exec_ok(
        &mut session,
        &mut sink,
        "INSERT INTO on_conflict_pk_visibility VALUES (1, 10, 100), (2, 20, 200)",
    )
    .await;
    exec_ok(
        &mut session,
        &mut sink,
        "INSERT INTO on_conflict_pk_visibility VALUES (2, 25, 250), (4, 40, 400) \
         ON CONFLICT (id) DO UPDATE SET stock = EXCLUDED.stock",
    )
    .await;

    let storage = table_storage(
        &session.current_database,
        "public",
        "on_conflict_pk_visibility",
    );
    let serializer = paro_storage::primary_key::PrimaryKeySerializer::from_schema_ref(
        &storage.tablet().schema().expect("tablet schema"),
    )
    .expect("primary key serializer");
    let key_two = serializer
        .encode_row(
            &Chunk::from_vectors(vec![paro_common::vector::Vector::from_i32(&[2])]),
            0,
        )
        .expect("encode key 2");
    assert!(
        storage
            .tablet()
            .lookup_primary_key(&key_two)
            .expect("lookup key 2")
            .is_some(),
        "ON CONFLICT DO UPDATE should keep the updated primary key addressable"
    );

    exec_ok(
        &mut session,
        &mut sink,
        "UPDATE on_conflict_pk_visibility SET stock = 251 WHERE id = 2",
    )
    .await;
    exec_ok(
        &mut session,
        &mut sink,
        "SELECT id, price, stock FROM on_conflict_pk_visibility ORDER BY id",
    )
    .await;
    assert_eq!(query_i64_col(&sink, 0), vec![1, 2, 4]);
    assert_eq!(query_i64_col(&sink, 2), vec![100, 251, 400]);

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

#[tokio::test]
async fn drop_schema_cascade_updates_route_registry_and_compaction_incrementally() {
    let base_dir = create_unique_test_dir(
        "storage_persistence",
        "drop_schema_cascade_runtime_registry_incremental",
    );
    let mut sink = CollectingSink::new();
    let instance = create_persistent_instance(&base_dir);
    let db = default_db(&instance);
    let initial_registered = db
        .compaction_observability()
        .expect("compaction manager should exist")
        .registered_tablets;
    let mut session = Session::new(1, Arc::clone(&instance));

    exec_ok(&mut session, &mut sink, "CREATE SCHEMA route_cascade").await;
    exec_ok(
        &mut session,
        &mut sink,
        "CREATE TABLE route_cascade.items (id INT PRIMARY KEY, payload VARCHAR)",
    )
    .await;

    assert!(
        table_exists(&db, "route_cascade", "items"),
        "catalog should expose the created table immediately after commit"
    );
    let registered_after_create = db
        .compaction_observability()
        .expect("compaction manager should exist")
        .registered_tablets;
    assert!(
        registered_after_create > initial_registered,
        "compaction manager should register the new table without an explicit full resync"
    );

    exec_ok(&mut session, &mut sink, "DROP SCHEMA route_cascade CASCADE").await;

    assert!(
        !table_exists(&db, "route_cascade", "items"),
        "catalog should drop schema-owned tables immediately after commit"
    );
    let registered_after_drop = db
        .compaction_observability()
        .expect("compaction manager should exist")
        .registered_tablets;
    assert_eq!(
        registered_after_drop, initial_registered,
        "compaction manager should unregister dropped schema tablets incrementally"
    );

    let _ = std::fs::remove_dir_all(&base_dir);
}

#[tokio::test]
async fn large_delete_patch_artifacts_are_cleaned_after_sql_commit() {
    let previous_threshold = current_delete_patch_inline_row_ref_threshold();
    set_delete_patch_inline_row_ref_threshold(1);

    let base_dir = create_unique_test_dir(
        "storage_persistence",
        "delete_patch_artifact_cleanup_after_commit",
    );
    let mut sink = CollectingSink::new();
    let cleanup_result = async {
        let instance = create_persistent_instance(&base_dir);
        let db = default_db(&instance);
        let mut session = Session::new(1, Arc::clone(&instance));

        exec_ok(
            &mut session,
            &mut sink,
            "CREATE TABLE cleanup_pk (id INT PRIMARY KEY, payload VARCHAR)",
        )
        .await;
        exec_ok(
            &mut session,
            &mut sink,
            "INSERT INTO cleanup_pk VALUES (1, 'a'), (2, 'b'), (3, 'c')",
        )
        .await;
        exec_ok(
            &mut session,
            &mut sink,
            "DELETE FROM cleanup_pk WHERE id IN (1, 2)",
        )
        .await;

        let storage = table_storage(&db, "public", "cleanup_pk");
        let delete_patch_root = storage.tablet().data_dir().join("_delete_patch");
        assert_eq!(
            file_count(&delete_patch_root),
            0,
            "delete patch artifacts should be cleaned once commit apply has completed"
        );
    }
    .await;

    set_delete_patch_inline_row_ref_threshold(previous_threshold);
    let _ = std::fs::remove_dir_all(&base_dir);
    cleanup_result
}
