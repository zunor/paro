// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

#[path = "common/exec_ok.rs"]
mod exec_ok;
#[path = "common/query_bool_col.rs"]
mod query_bool_col;
#[path = "common/query_i64_col.rs"]
mod query_i64_col;
#[path = "common/query_string_col.rs"]
mod query_string_col;

use exec_ok::exec_ok;
use paro_instance::Instance;
use paro_session::{CollectingSink, Session};
use query_bool_col::query_bool_col;
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

#[tokio::test]
async fn paro_transaction_metrics_is_queryable_from_sql() {
    let instance = Instance::new_in_memory();
    let mut session = Session::new(1, instance);
    let mut sink = CollectingSink::new();

    exec_ok(
        &mut session,
        &mut sink,
        "CREATE TABLE txn_metrics_probe (id INT PRIMARY KEY, v INT)",
    )
    .await;
    exec_ok(
        &mut session,
        &mut sink,
        "INSERT INTO txn_metrics_probe VALUES (1, 10)",
    )
    .await;
    exec_ok(
        &mut session,
        &mut sink,
        "SELECT database_name, txn_begin_count, txn_commit_count,
                group_commit_fence_us_total, write_conflict_index_size,
                durable_published_lag_commits, durable_published_lag_ms,
                backpressure_throttle_count,
                lock_wait_count, lock_wait_duration_us,
                lock_wound_wait_abort_count, lock_deadlock_abort_count,
                ssi_validation_abort_count,
                ssi_abort_due_to_coarse_scan_marker, read_tracker_record_count,
                read_tracker_coarsened_count, derived_index_lag_ts,
                tail_exact_merge_cost, commit_participant_count,
                inflight_batch_conflict_reject_count,
                retention_watermark_lag_ms, oldest_active_rw_lag_ms,
                read_snapshot_lease_count, active_rw_txn_count,
                commit_ack_mode
         FROM paro_transaction_metrics()
         WHERE database_name = current_database()",
    )
    .await;

    assert_eq!(query_string_col(&sink, 0), vec!["postgres".to_string()]);
    assert!(query_i64_col(&sink, 1)[0] >= 1);
    assert!(query_i64_col(&sink, 2)[0] >= 1);
    assert_eq!(query_i64_col(&sink, 3).len(), 1);
    assert_eq!(query_i64_col(&sink, 4).len(), 1);
    assert_eq!(query_i64_col(&sink, 5).len(), 1);
    assert_eq!(query_i64_col(&sink, 6).len(), 1);
    assert_eq!(query_i64_col(&sink, 7).len(), 1);
    for col in 8..24 {
        assert_eq!(query_i64_col(&sink, col).len(), 1);
    }
    assert_eq!(
        query_string_col(&sink, 24),
        vec!["required_published".to_string()]
    );
}

#[tokio::test]
async fn paro_commit_debug_functions_are_queryable_from_sql() {
    let instance = Instance::new_in_memory();
    let mut session = Session::new(1, instance);
    let mut sink = CollectingSink::new();

    exec_ok(
        &mut session,
        &mut sink,
        "CREATE TABLE commit_debug_probe (id INT PRIMARY KEY, v INT)",
    )
    .await;
    exec_ok(
        &mut session,
        &mut sink,
        "INSERT INTO commit_debug_probe VALUES (1, 10)",
    )
    .await;
    exec_ok(
        &mut session,
        &mut sink,
        "SELECT database_name, durable_commit_id, published_commit_id,
                durable_commit_bytes, published_commit_bytes,
                notify_suppressed_count, publish_failure_count
         FROM paro_commit_frontiers()
         WHERE database_name = current_database()",
    )
    .await;

    assert_eq!(query_string_col(&sink, 0), vec!["postgres".to_string()]);
    assert!(query_i64_col(&sink, 1)[0] >= 1);
    assert!(query_i64_col(&sink, 2)[0] >= 1);
    assert_eq!(query_i64_col(&sink, 3).len(), 1);
    assert_eq!(query_i64_col(&sink, 4).len(), 1);
    assert_eq!(query_i64_col(&sink, 5).len(), 1);
    assert_eq!(query_i64_col(&sink, 6), vec![0]);

    exec_ok(
        &mut session,
        &mut sink,
        "SELECT database_name, admission_open, poisoned
         FROM paro_commit_poison()
         WHERE database_name = current_database()",
    )
    .await;

    assert_eq!(query_string_col(&sink, 0), vec!["postgres".to_string()]);
    assert_eq!(query_bool_col(&sink, 1), vec![true]);
    assert_eq!(query_bool_col(&sink, 2), vec![false]);
}
