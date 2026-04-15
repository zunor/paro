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

#[tokio::test]
async fn create_table_then_insert_same_transaction_is_allowed() {
    let instance = create_in_memory_instance();
    let mut session = Session::new(1, instance);
    let mut sink = CollectingSink::new();

    exec_ok(&mut session, &mut sink, "BEGIN").await;
    exec_ok(
        &mut session,
        &mut sink,
        "CREATE TABLE admission_ctas (id BIGINT PRIMARY KEY, v BIGINT)",
    )
    .await;
    exec_ok(
        &mut session,
        &mut sink,
        "INSERT INTO admission_ctas VALUES (1, 10), (2, 20)",
    )
    .await;
    exec_ok(&mut session, &mut sink, "COMMIT").await;

    exec_ok(
        &mut session,
        &mut sink,
        "SELECT COUNT(*) FROM admission_ctas",
    )
    .await;
    assert_eq!(query_single_i64(&sink), 2);
}

#[tokio::test]
async fn create_index_then_dml_on_same_table_is_rejected() {
    let instance = create_in_memory_instance();
    let mut session = Session::new(1, instance);
    let mut sink = CollectingSink::new();

    exec_ok(
        &mut session,
        &mut sink,
        "CREATE TABLE admission_idx (id BIGINT PRIMARY KEY, content VARCHAR)",
    )
    .await;
    exec_ok(
        &mut session,
        &mut sink,
        "INSERT INTO admission_idx VALUES (1, 'alpha')",
    )
    .await;

    exec_ok(&mut session, &mut sink, "BEGIN").await;
    exec_ok(
        &mut session,
        &mut sink,
        "CREATE INDEX admission_idx_id ON admission_idx USING GIN (to_tsvector('simple', content))",
    )
    .await;
    let err = exec_err(
        &mut session,
        &mut sink,
        "INSERT INTO admission_idx VALUES (2, 'beta')",
    )
    .await;
    assert!(
        err.contains("pending DDL") || err.contains("same transaction"),
        "unexpected error: {err}"
    );
    exec_ok(&mut session, &mut sink, "ROLLBACK").await;
}

#[tokio::test]
async fn dml_then_drop_table_is_rejected() {
    let instance = create_in_memory_instance();
    let mut session = Session::new(1, instance);
    let mut sink = CollectingSink::new();

    exec_ok(
        &mut session,
        &mut sink,
        "CREATE TABLE admission_drop (id BIGINT PRIMARY KEY, v BIGINT)",
    )
    .await;

    exec_ok(&mut session, &mut sink, "BEGIN").await;
    exec_ok(
        &mut session,
        &mut sink,
        "INSERT INTO admission_drop VALUES (1, 10)",
    )
    .await;
    let err = exec_err(&mut session, &mut sink, "DROP TABLE admission_drop").await;
    assert!(
        err.contains("DROP TABLE") || err.contains("same transaction"),
        "unexpected error: {err}"
    );
    exec_ok(&mut session, &mut sink, "ROLLBACK").await;
}

#[tokio::test]
async fn metadata_only_ddl_can_mix_with_disjoint_dml() {
    let instance = create_in_memory_instance();
    let mut session = Session::new(1, instance);
    let mut sink = CollectingSink::new();

    exec_ok(
        &mut session,
        &mut sink,
        "CREATE TABLE admission_meta (id BIGINT PRIMARY KEY, v BIGINT)",
    )
    .await;

    exec_ok(&mut session, &mut sink, "BEGIN").await;
    exec_ok(
        &mut session,
        &mut sink,
        "CREATE VIEW admission_meta_view AS SELECT id, v FROM admission_meta",
    )
    .await;
    exec_ok(
        &mut session,
        &mut sink,
        "INSERT INTO admission_meta VALUES (1, 10), (2, 20)",
    )
    .await;
    exec_ok(&mut session, &mut sink, "COMMIT").await;

    exec_ok(
        &mut session,
        &mut sink,
        "SELECT COUNT(*) FROM admission_meta_view",
    )
    .await;
    assert_eq!(query_single_i64(&sink), 2);
}

#[tokio::test]
async fn rollback_to_savepoint_discards_pending_admission_rule() {
    let instance = create_in_memory_instance();
    let mut session = Session::new(1, instance);
    let mut sink = CollectingSink::new();

    exec_ok(
        &mut session,
        &mut sink,
        "CREATE TABLE admission_savepoint (id BIGINT PRIMARY KEY, content VARCHAR)",
    )
    .await;

    exec_ok(&mut session, &mut sink, "BEGIN").await;
    exec_ok(&mut session, &mut sink, "SAVEPOINT before_idx").await;
    exec_ok(
        &mut session,
        &mut sink,
        "CREATE INDEX admission_savepoint_idx ON admission_savepoint USING GIN (to_tsvector('simple', content))",
    )
    .await;
    exec_ok(&mut session, &mut sink, "ROLLBACK TO SAVEPOINT before_idx").await;
    exec_ok(
        &mut session,
        &mut sink,
        "INSERT INTO admission_savepoint VALUES (1, 'after rollback')",
    )
    .await;
    exec_ok(&mut session, &mut sink, "COMMIT").await;

    exec_ok(
        &mut session,
        &mut sink,
        "SELECT COUNT(*) FROM admission_savepoint",
    )
    .await;
    assert_eq!(query_single_i64(&sink), 1);
}

#[tokio::test]
async fn alter_table_comment_then_dml_is_rejected() {
    let instance = create_in_memory_instance();
    let mut session = Session::new(1, instance);
    let mut sink = CollectingSink::new();

    exec_ok(
        &mut session,
        &mut sink,
        "CREATE TABLE admission_alter (id BIGINT PRIMARY KEY, v BIGINT)",
    )
    .await;

    exec_ok(&mut session, &mut sink, "BEGIN").await;
    exec_ok(
        &mut session,
        &mut sink,
        "COMMENT ON TABLE admission_alter IS 'pending comment'",
    )
    .await;
    let err = exec_err(
        &mut session,
        &mut sink,
        "INSERT INTO admission_alter VALUES (1, 10)",
    )
    .await;
    assert!(
        err.contains("pending DDL") || err.contains("same transaction"),
        "unexpected error: {err}"
    );
    exec_ok(&mut session, &mut sink, "ROLLBACK").await;
}

#[tokio::test]
async fn create_sequence_allows_disjoint_dml() {
    let instance = create_in_memory_instance();
    let mut session = Session::new(1, instance);
    let mut sink = CollectingSink::new();

    exec_ok(
        &mut session,
        &mut sink,
        "CREATE TABLE admission_seq_target (id BIGINT PRIMARY KEY, v BIGINT)",
    )
    .await;

    exec_ok(&mut session, &mut sink, "BEGIN").await;
    exec_ok(
        &mut session,
        &mut sink,
        "CREATE SEQUENCE admission_seq START WITH 7 INCREMENT BY 3",
    )
    .await;
    exec_ok(
        &mut session,
        &mut sink,
        "INSERT INTO admission_seq_target VALUES (1, 10), (2, 20)",
    )
    .await;
    exec_ok(&mut session, &mut sink, "COMMIT").await;

    exec_ok(
        &mut session,
        &mut sink,
        "SELECT COUNT(*) FROM admission_seq_target",
    )
    .await;
    assert_eq!(query_single_i64(&sink), 2);
}

#[tokio::test]
async fn drop_schema_cascade_after_dml_is_rejected() {
    let instance = create_in_memory_instance();
    let mut session = Session::new(1, instance);
    let mut sink = CollectingSink::new();

    exec_ok(&mut session, &mut sink, "CREATE SCHEMA admission_cascade").await;
    exec_ok(
        &mut session,
        &mut sink,
        "CREATE TABLE admission_cascade.items (id BIGINT PRIMARY KEY, v BIGINT)",
    )
    .await;

    exec_ok(&mut session, &mut sink, "BEGIN").await;
    exec_ok(
        &mut session,
        &mut sink,
        "INSERT INTO admission_cascade.items VALUES (1, 10)",
    )
    .await;
    let err = exec_err(
        &mut session,
        &mut sink,
        "DROP SCHEMA admission_cascade CASCADE",
    )
    .await;
    assert!(
        err.contains("DROP SCHEMA")
            || err.contains("dependent table")
            || err.contains("same transaction"),
        "unexpected error: {err}"
    );
    exec_ok(&mut session, &mut sink, "ROLLBACK").await;
}
