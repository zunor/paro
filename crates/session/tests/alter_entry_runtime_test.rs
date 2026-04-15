use paro_common::runtime_value::Value;
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

fn query_all_i64(sink: &CollectingSink) -> Vec<i64> {
    let mut out = Vec::new();
    let result = sink.assert_single_result();
    for chunk in &result.chunks {
        let col = chunk.column(0).expect("missing value");
        for row in 0..chunk.len() {
            let value = match col.get_value(row) {
                Value::BigInt(v) => v,
                Value::Integer(v) => i64::from(v),
                other => panic!("unexpected scalar value: {:?}", other),
            };
            out.push(value);
        }
    }
    out
}

#[tokio::test]
async fn rename_table_updates_visible_name_and_hides_old_name() {
    let instance = create_in_memory_instance();
    let mut session = Session::new(1, instance);
    let mut sink = CollectingSink::new();

    exec_ok(
        &mut session,
        &mut sink,
        "CREATE TABLE alter_runtime_rename_table (id BIGINT PRIMARY KEY, v BIGINT)",
    )
    .await;
    exec_ok(
        &mut session,
        &mut sink,
        "INSERT INTO alter_runtime_rename_table VALUES (1, 10), (2, 20)",
    )
    .await;

    exec_ok(&mut session, &mut sink, "BEGIN").await;
    exec_ok(
        &mut session,
        &mut sink,
        "ALTER TABLE alter_runtime_rename_table RENAME TO alter_runtime_rename_table_v2",
    )
    .await;
    exec_ok(&mut session, &mut sink, "COMMIT").await;

    exec_ok(
        &mut session,
        &mut sink,
        "SELECT COUNT(*) FROM alter_runtime_rename_table_v2",
    )
    .await;
    assert_eq!(query_single_i64(&sink), 2);

    let err = exec_err(
        &mut session,
        &mut sink,
        "SELECT COUNT(*) FROM alter_runtime_rename_table",
    )
    .await;
    assert!(
        err.contains("does not exist") || err.contains("not found"),
        "unexpected error after rename: {err}"
    );
}

#[tokio::test]
async fn rename_column_updates_visible_name_and_hides_old_name() {
    let instance = create_in_memory_instance();
    let mut session = Session::new(1, instance);
    let mut sink = CollectingSink::new();

    exec_ok(
        &mut session,
        &mut sink,
        "CREATE TABLE alter_runtime_rename_column (id BIGINT PRIMARY KEY, old_name BIGINT)",
    )
    .await;
    exec_ok(
        &mut session,
        &mut sink,
        "INSERT INTO alter_runtime_rename_column VALUES (1, 10), (2, 20)",
    )
    .await;

    exec_ok(&mut session, &mut sink, "BEGIN").await;
    exec_ok(
        &mut session,
        &mut sink,
        "ALTER TABLE alter_runtime_rename_column RENAME COLUMN old_name TO new_name",
    )
    .await;
    exec_ok(&mut session, &mut sink, "COMMIT").await;

    exec_ok(
        &mut session,
        &mut sink,
        "SELECT new_name FROM alter_runtime_rename_column ORDER BY id",
    )
    .await;
    assert_eq!(query_all_i64(&sink), vec![10, 20]);

    let err = exec_err(
        &mut session,
        &mut sink,
        "SELECT old_name FROM alter_runtime_rename_column",
    )
    .await;
    assert!(
        err.contains("does not exist")
            || err.contains("unknown column")
            || err.contains("Column not found"),
        "unexpected error after column rename: {err}"
    );
}

#[tokio::test]
async fn rename_table_statement_updates_visible_name_and_hides_old_name() {
    let instance = create_in_memory_instance();
    let mut session = Session::new(1, instance);
    let mut sink = CollectingSink::new();

    exec_ok(
        &mut session,
        &mut sink,
        "CREATE TABLE alter_runtime_rename_table_stmt (id BIGINT PRIMARY KEY, v BIGINT)",
    )
    .await;
    exec_ok(
        &mut session,
        &mut sink,
        "INSERT INTO alter_runtime_rename_table_stmt VALUES (1, 10), (2, 20)",
    )
    .await;

    exec_ok(&mut session, &mut sink, "BEGIN").await;
    exec_ok(
        &mut session,
        &mut sink,
        "RENAME TABLE alter_runtime_rename_table_stmt TO alter_runtime_rename_table_stmt_v2",
    )
    .await;
    exec_ok(&mut session, &mut sink, "COMMIT").await;

    exec_ok(
        &mut session,
        &mut sink,
        "SELECT COUNT(*) FROM alter_runtime_rename_table_stmt_v2",
    )
    .await;
    assert_eq!(query_single_i64(&sink), 2);

    let err = exec_err(
        &mut session,
        &mut sink,
        "SELECT COUNT(*) FROM alter_runtime_rename_table_stmt",
    )
    .await;
    assert!(
        err.contains("does not exist") || err.contains("not found"),
        "unexpected error after rename: {err}"
    );
}
