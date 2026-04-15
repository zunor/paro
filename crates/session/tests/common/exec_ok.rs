// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::time::Duration;

use paro_session::{CollectingSink, Session};

pub async fn exec_ok(session: &mut Session, sink: &mut CollectingSink, sql: &str) {
    sink.clear();
    let result = tokio::time::timeout(
        Duration::from_secs(10),
        session.execute_simple_query(sql, sink),
    )
    .await
    .unwrap_or_else(|_| panic!("sql timed out: {sql}"));
    assert!(
        result.is_ok(),
        "sql should succeed: {sql}, error={:?}, sink_errors={:?}",
        result.err(),
        sink.errors()
    );
    assert!(
        !sink.has_errors(),
        "sql should not produce sink errors: {sql}, sink_errors={:?}",
        sink.errors()
    );
}
