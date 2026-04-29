// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_session::{CollectingSink, Session};

#[path = "common/exec_err.rs"]
mod exec_err;
#[path = "common/exec_ok.rs"]
mod exec_ok;
#[path = "common/query_string_col.rs"]
mod query_string_col;

use exec_err::exec_err;
use exec_ok::exec_ok;
use query_string_col::query_string_col;

fn show_value(sink: &CollectingSink) -> String {
    query_string_col(sink, 0)
        .into_iter()
        .next()
        .expect("SHOW should return one row")
}

#[tokio::test]
async fn transaction_isolation_tracks_session_defaults_and_current_overrides() {
    let instance = paro_instance::Instance::new_in_memory();
    let mut session = Session::new(1, instance);
    let mut sink = CollectingSink::new();

    exec_ok(&mut session, &mut sink, "SHOW transaction_isolation").await;
    assert_eq!(show_value(&sink), "serializable");

    exec_ok(
        &mut session,
        &mut sink,
        "SET default_transaction_isolation = 'snapshot'",
    )
    .await;
    exec_ok(&mut session, &mut sink, "SHOW transaction_isolation").await;
    assert_eq!(show_value(&sink), "snapshot");

    exec_ok(&mut session, &mut sink, "BEGIN").await;
    exec_ok(&mut session, &mut sink, "SHOW transaction_isolation").await;
    assert_eq!(show_value(&sink), "snapshot");
    exec_ok(&mut session, &mut sink, "COMMIT").await;

    exec_ok(&mut session, &mut sink, "BEGIN").await;
    exec_ok(
        &mut session,
        &mut sink,
        "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE",
    )
    .await;
    exec_ok(&mut session, &mut sink, "SHOW transaction_isolation").await;
    assert_eq!(show_value(&sink), "serializable");
    exec_ok(&mut session, &mut sink, "COMMIT").await;

    exec_ok(&mut session, &mut sink, "SHOW transaction_isolation").await;
    assert_eq!(show_value(&sink), "snapshot");

    exec_ok(&mut session, &mut sink, "BEGIN ISOLATION LEVEL SNAPSHOT").await;
    exec_ok(
        &mut session,
        &mut sink,
        "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL SERIALIZABLE",
    )
    .await;
    exec_ok(&mut session, &mut sink, "SHOW transaction_isolation").await;
    assert_eq!(show_value(&sink), "snapshot");
    exec_ok(&mut session, &mut sink, "COMMIT").await;
    exec_ok(&mut session, &mut sink, "SHOW transaction_isolation").await;
    assert_eq!(show_value(&sink), "serializable");
}

#[tokio::test]
async fn set_transaction_requires_open_block_before_first_query() {
    let instance = paro_instance::Instance::new_in_memory();
    let mut session = Session::new(2, instance);
    let mut sink = CollectingSink::new();

    let err = exec_err(
        &mut session,
        &mut sink,
        "SET TRANSACTION ISOLATION LEVEL SNAPSHOT",
    )
    .await;
    assert!(err.contains("transaction blocks"));

    exec_ok(&mut session, &mut sink, "BEGIN").await;
    exec_ok(&mut session, &mut sink, "SELECT 1").await;
    let err = exec_err(
        &mut session,
        &mut sink,
        "SET TRANSACTION ISOLATION LEVEL SNAPSHOT",
    )
    .await;
    assert!(err.contains("before the first query"));
    exec_ok(&mut session, &mut sink, "ROLLBACK").await;
}

#[tokio::test]
async fn read_only_transaction_modes_reject_writes() {
    let instance = paro_instance::Instance::new_in_memory();
    let mut session = Session::new(3, instance);
    let mut sink = CollectingSink::new();

    exec_ok(
        &mut session,
        &mut sink,
        "CREATE TABLE txn_read_only_modes (id INT)",
    )
    .await;

    exec_ok(&mut session, &mut sink, "BEGIN READ ONLY").await;
    let err = exec_err(
        &mut session,
        &mut sink,
        "INSERT INTO txn_read_only_modes VALUES (1)",
    )
    .await;
    assert!(err.contains("read-only transaction"));
    exec_ok(&mut session, &mut sink, "ROLLBACK").await;

    exec_ok(
        &mut session,
        &mut sink,
        "SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY",
    )
    .await;
    let err = exec_err(
        &mut session,
        &mut sink,
        "INSERT INTO txn_read_only_modes VALUES (2)",
    )
    .await;
    assert!(err.contains("read-only transaction"));

    exec_ok(
        &mut session,
        &mut sink,
        "SET SESSION CHARACTERISTICS AS TRANSACTION READ WRITE",
    )
    .await;
    exec_ok(
        &mut session,
        &mut sink,
        "INSERT INTO txn_read_only_modes VALUES (3)",
    )
    .await;
}
