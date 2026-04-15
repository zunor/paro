// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::time::Duration;

use paro_session::{CollectingSink, Session};

pub async fn exec_err(session: &mut Session, sink: &mut CollectingSink, sql: &str) -> String {
    sink.clear();
    let result = tokio::time::timeout(
        Duration::from_secs(10),
        session.execute_simple_query(sql, sink),
    )
    .await
    .unwrap_or_else(|_| panic!("sql timed out: {sql}"));
    assert!(
        result.is_err() || sink.has_errors(),
        "sql should fail: {sql}"
    );
    if let Some(err) = result.err() {
        return err.to_string();
    }
    sink.errors()
        .first()
        .map(|err| err.message.clone())
        .unwrap_or_else(|| "unknown error".to_string())
}
