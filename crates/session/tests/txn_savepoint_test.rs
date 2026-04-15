// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_session::{CollectingSink, Session};

#[path = "common/exec_ok.rs"]
mod exec_ok;
#[path = "common/instance_memory.rs"]
mod instance_memory;
#[path = "common/query_single_i64.rs"]
mod query_single_i64;

use exec_ok::exec_ok;
use instance_memory::create_in_memory_instance;
use query_single_i64::query_single_i64;

#[tokio::test]
async fn rollback_to_savepoint_discards_writer_backed_insert_batch() {
    let instance = create_in_memory_instance();
    let mut session = Session::new(1, instance);
    let mut sink = CollectingSink::new();

    exec_ok(
        &mut session,
        &mut sink,
        "CREATE TABLE savepoint_dml (id BIGINT PRIMARY KEY, v BIGINT)",
    )
    .await;

    exec_ok(&mut session, &mut sink, "BEGIN").await;
    exec_ok(
        &mut session,
        &mut sink,
        "INSERT INTO savepoint_dml VALUES (1, 10), (2, 20)",
    )
    .await;
    exec_ok(&mut session, &mut sink, "SAVEPOINT keep_first_batch").await;
    exec_ok(
        &mut session,
        &mut sink,
        "INSERT INTO savepoint_dml VALUES (3, 30), (4, 40)",
    )
    .await;
    exec_ok(
        &mut session,
        &mut sink,
        "ROLLBACK TO SAVEPOINT keep_first_batch",
    )
    .await;
    exec_ok(&mut session, &mut sink, "COMMIT").await;

    exec_ok(
        &mut session,
        &mut sink,
        "SELECT COUNT(*) FROM savepoint_dml",
    )
    .await;
    assert_eq!(query_single_i64(&sink), 2);

    exec_ok(&mut session, &mut sink, "SELECT MAX(id) FROM savepoint_dml").await;
    assert_eq!(query_single_i64(&sink), 2);
}
