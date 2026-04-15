// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_catalog::entry::CatalogEntryEnum;
use paro_catalog::mvcc::CatalogSnapshot;
use paro_session::{CollectingSink, Session};

#[path = "common/exec_err.rs"]
mod exec_err;
#[path = "common/exec_ok.rs"]
mod exec_ok;
#[path = "common/instance_memory.rs"]
mod instance_memory;
#[path = "common/query_single_i64.rs"]
mod query_single_i64;

use exec_err::exec_err;
use exec_ok::exec_ok;
use instance_memory::create_in_memory_instance;
use query_single_i64::query_single_i64;

fn table_column_comment(
    session: &Session,
    schema_name: &str,
    table_name: &str,
    column_name: &str,
) -> Option<String> {
    let txn = CatalogSnapshot::read_only(u64::MAX);
    let schema = session
        .current_database
        .catalog()
        .get_schema(&txn, schema_name)
        .ok()?;
    let entry = schema
        .get_table(txn.transaction_id, txn.start_time, table_name)?
        .clone();
    let CatalogEntryEnum::Table(table) = entry.as_ref() else {
        return None;
    };
    table
        .get_column(column_name)
        .and_then(|column| column.comment.clone())
}

#[tokio::test]
async fn comment_on_column_updates_catalog_and_leaves_table_usable() {
    let instance = create_in_memory_instance();
    let mut session = Session::new(1, instance);
    let mut sink = CollectingSink::new();

    exec_ok(
        &mut session,
        &mut sink,
        "CREATE TABLE alter_remaining_comment_col (id BIGINT PRIMARY KEY, note VARCHAR)",
    )
    .await;
    exec_ok(
        &mut session,
        &mut sink,
        "INSERT INTO alter_remaining_comment_col VALUES (1, 'keep me')",
    )
    .await;

    exec_ok(
        &mut session,
        &mut sink,
        "COMMENT ON COLUMN alter_remaining_comment_col.note IS 'column comment'",
    )
    .await;

    exec_ok(
        &mut session,
        &mut sink,
        "SELECT COUNT(*) FROM alter_remaining_comment_col",
    )
    .await;
    assert_eq!(query_single_i64(&sink), 1);
    assert_eq!(
        table_column_comment(&session, "public", "alter_remaining_comment_col", "note"),
        Some("column comment".to_string())
    );
}

#[tokio::test]
async fn rename_table_across_schema_moves_visibility_to_target_schema() {
    let instance = create_in_memory_instance();
    let mut session = Session::new(1, instance);
    let mut sink = CollectingSink::new();

    exec_ok(&mut session, &mut sink, "CREATE SCHEMA alter_src").await;
    exec_ok(&mut session, &mut sink, "CREATE SCHEMA alter_dst").await;
    exec_ok(
        &mut session,
        &mut sink,
        "CREATE TABLE alter_src.move_me (id BIGINT PRIMARY KEY, v BIGINT)",
    )
    .await;
    exec_ok(
        &mut session,
        &mut sink,
        "INSERT INTO alter_src.move_me VALUES (1, 10)",
    )
    .await;

    exec_ok(
        &mut session,
        &mut sink,
        "RENAME TABLE alter_src.move_me TO alter_dst.move_me_v2",
    )
    .await;

    exec_ok(
        &mut session,
        &mut sink,
        "SELECT COUNT(*) FROM alter_dst.move_me_v2",
    )
    .await;
    assert_eq!(query_single_i64(&sink), 1);

    let err = exec_err(
        &mut session,
        &mut sink,
        "SELECT COUNT(*) FROM alter_src.move_me",
    )
    .await;
    assert!(
        err.contains("does not exist") || err.contains("not found"),
        "unexpected error after cross-schema rename: {err}"
    );
}

#[tokio::test]
async fn rename_table_conflict_can_be_retried_after_savepoint() {
    let instance = create_in_memory_instance();
    let mut session = Session::new(1, instance);
    let mut sink = CollectingSink::new();

    exec_ok(
        &mut session,
        &mut sink,
        "CREATE TABLE alter_remaining_conflict_a (id BIGINT PRIMARY KEY, v BIGINT)",
    )
    .await;
    exec_ok(
        &mut session,
        &mut sink,
        "CREATE TABLE alter_remaining_conflict_b (id BIGINT PRIMARY KEY, v BIGINT)",
    )
    .await;
    exec_ok(
        &mut session,
        &mut sink,
        "INSERT INTO alter_remaining_conflict_a VALUES (1, 10)",
    )
    .await;

    exec_ok(&mut session, &mut sink, "BEGIN").await;
    exec_ok(&mut session, &mut sink, "SAVEPOINT before_conflict").await;

    let err = exec_err(
        &mut session,
        &mut sink,
        "RENAME TABLE alter_remaining_conflict_a TO alter_remaining_conflict_b",
    )
    .await;
    assert!(
        err.contains("already exists") || err.contains("object exists"),
        "unexpected rename conflict error: {err}"
    );

    exec_ok(
        &mut session,
        &mut sink,
        "ROLLBACK TO SAVEPOINT before_conflict",
    )
    .await;
    exec_ok(
        &mut session,
        &mut sink,
        "RENAME TABLE alter_remaining_conflict_a TO alter_remaining_conflict_a_v2",
    )
    .await;
    exec_ok(&mut session, &mut sink, "COMMIT").await;

    exec_ok(
        &mut session,
        &mut sink,
        "SELECT COUNT(*) FROM alter_remaining_conflict_a_v2",
    )
    .await;
    assert_eq!(query_single_i64(&sink), 1);

    let err = exec_err(
        &mut session,
        &mut sink,
        "SELECT COUNT(*) FROM alter_remaining_conflict_a",
    )
    .await;
    assert!(
        err.contains("does not exist") || err.contains("not found"),
        "unexpected error after rename retry: {err}"
    );
}
