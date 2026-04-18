// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

#[path = "common/exec_ok.rs"]
mod exec_ok;
#[path = "common/query_i64_col.rs"]
mod query_i64_col;
#[path = "common/query_string_col.rs"]
mod query_string_col;

use exec_ok::exec_ok;
use paro_instance::Instance;
use paro_session::{CollectingSink, Session};
use query_i64_col::query_i64_col;
use query_string_col::query_string_col;

#[tokio::test]
async fn paro_wal_metrics_is_queryable_from_sql() {
    let instance = Instance::new_in_memory();
    let mut session = Session::new(1, instance);
    let mut sink = CollectingSink::new();

    exec_ok(
        &mut session,
        &mut sink,
        "CREATE TABLE wal_metrics_probe (id INT PRIMARY KEY, v INT)",
    )
    .await;
    exec_ok(
        &mut session,
        &mut sink,
        "INSERT INTO wal_metrics_probe VALUES (1, 10), (2, 20)",
    )
    .await;
    exec_ok(
        &mut session,
        &mut sink,
        "SELECT database_name, recovery_mode, journal_apply_queue_depth, journal_apply_published_lag,
                journal_commit_bytes_total, journal_group_size_last
         FROM paro_wal_metrics()
         WHERE database_name = current_database()",
    )
    .await;

    assert_eq!(query_string_col(&sink, 0), vec!["postgres".to_string()]);
    assert_eq!(query_string_col(&sink, 1).len(), 1);
    assert_eq!(query_i64_col(&sink, 2).len(), 1);
    assert_eq!(query_i64_col(&sink, 3).len(), 1);
    assert_eq!(query_i64_col(&sink, 4).len(), 1);
    assert_eq!(query_i64_col(&sink, 5).len(), 1);
}
