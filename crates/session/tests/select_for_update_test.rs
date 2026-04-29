// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

#[path = "common/exec_err.rs"]
mod exec_err;
#[path = "common/exec_ok.rs"]
mod exec_ok;

use exec_err::exec_err;
use exec_ok::exec_ok;
use paro_instance::Instance;
use paro_session::{CollectingSink, Session};

#[tokio::test]
async fn select_for_update_holds_table_write_lock_until_commit() {
    let instance = Instance::new_in_memory();
    let mut writer = Session::new(1, instance.clone());
    let mut contender = Session::new(2, instance);
    let mut sink = CollectingSink::new();

    exec_ok(
        &mut writer,
        &mut sink,
        "CREATE TABLE for_update_probe (id INT PRIMARY KEY, v INT)",
    )
    .await;
    exec_ok(
        &mut writer,
        &mut sink,
        "INSERT INTO for_update_probe VALUES (1, 10)",
    )
    .await;

    exec_ok(&mut writer, &mut sink, "BEGIN").await;
    exec_ok(
        &mut writer,
        &mut sink,
        "SELECT * FROM for_update_probe WHERE id = 1 FOR UPDATE",
    )
    .await;

    exec_ok(&mut contender, &mut sink, "BEGIN").await;
    let error = exec_err(
        &mut contender,
        &mut sink,
        "INSERT INTO for_update_probe VALUES (2, 20)",
    )
    .await;
    assert!(error.contains("lock") || error.contains("WouldWait"));
    exec_ok(&mut contender, &mut sink, "ROLLBACK").await;

    exec_ok(&mut contender, &mut sink, "BEGIN").await;
    let error = exec_err(
        &mut contender,
        &mut sink,
        "SELECT * FROM for_update_probe WHERE id = 1 FOR UPDATE",
    )
    .await;
    assert!(error.contains("lock") || error.contains("WouldWait"));
    exec_ok(&mut contender, &mut sink, "ROLLBACK").await;

    exec_ok(&mut writer, &mut sink, "COMMIT").await;
    exec_ok(
        &mut contender,
        &mut sink,
        "SELECT * FROM for_update_probe WHERE id = 1 FOR UPDATE",
    )
    .await;
}
