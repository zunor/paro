// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

#[path = "common/exec_ok.rs"]
mod exec_ok;
#[path = "common/query_single_i64.rs"]
mod query_single_i64;

use exec_ok::exec_ok;
use paro_instance::{Instance, InstanceConfig};
use paro_session::{CollectingSink, Session};
use query_single_i64::query_single_i64;

#[tokio::test]
async fn memory_system_function_context_is_isolated_per_instance() {
    const FIRST_LIMIT: usize = 24 * 1024 * 1024;
    const SECOND_LIMIT: usize = 56 * 1024 * 1024;

    let first = Instance::new(InstanceConfig::in_memory().with_max_memory(FIRST_LIMIT))
        .expect("first instance should open");
    let second = Instance::new(InstanceConfig::in_memory().with_max_memory(SECOND_LIMIT))
        .expect("second instance should open");
    let mut first_session = Session::new(1, first);
    let mut second_session = Session::new(2, second);
    let mut sink = CollectingSink::new();

    exec_ok(
        &mut first_session,
        &mut sink,
        "SELECT memory_limit FROM pragma_database_size()",
    )
    .await;
    assert_eq!(query_single_i64(&sink), FIRST_LIMIT as i64);

    exec_ok(
        &mut second_session,
        &mut sink,
        "SELECT memory_limit FROM pragma_database_size()",
    )
    .await;
    assert_eq!(query_single_i64(&sink), SECOND_LIMIT as i64);

    exec_ok(
        &mut first_session,
        &mut sink,
        "SELECT memory_limit FROM pragma_database_size()",
    )
    .await;
    assert_eq!(query_single_i64(&sink), FIRST_LIMIT as i64);
}
