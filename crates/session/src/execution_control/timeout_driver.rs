// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_context::{StatementCancelReason, StatementTimeoutDriver};
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Default)]
pub struct TokioStatementTimeoutDriver;

impl StatementTimeoutDriver for TokioStatementTimeoutDriver {
    fn arm(
        &self,
        statement_token: &CancellationToken,
        cancel_reason: &Arc<OnceLock<StatementCancelReason>>,
        timeout_lifetime: &Arc<CancellationToken>,
        timeout: Duration,
    ) {
        let statement_token = statement_token.clone();
        let cancel_reason = cancel_reason.clone();
        let timeout_lifetime = timeout_lifetime.clone();

        tokio::spawn(async move {
            tokio::select! {
                _ = timeout_lifetime.cancelled() => {}
                _ = statement_token.cancelled() => {}
                _ = tokio::time::sleep(timeout) => {
                    if statement_token.is_cancelled() || timeout_lifetime.is_cancelled() {
                        return;
                    }
                    let _ = cancel_reason.set(StatementCancelReason::StatementTimeout);
                    statement_token.cancel();
                }
            }
        });
    }
}
