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
        timeout_lifetime: &CancellationToken,
        timeout: Duration,
    ) {
        let statement_token = statement_token.clone();
        let cancel_reason = cancel_reason.clone();
        let timeout_lifetime = timeout_lifetime.clone();
        // Capture the deadline at arm time, not when this task is first polled.
        let Some(deadline) = tokio::time::Instant::now().checked_add(timeout) else {
            return;
        };

        tokio::spawn(async move {
            tokio::select! {
                _ = timeout_lifetime.cancelled() => {}
                _ = statement_token.cancelled() => {}
                _ = tokio::time::sleep_until(deadline) => {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "current_thread")]
    async fn timer_includes_time_before_its_first_poll() {
        let statement = CancellationToken::new();
        let lifetime = CancellationToken::new();
        let reason = Arc::new(OnceLock::new());
        TokioStatementTimeoutDriver.arm(&statement, &reason, &lifetime, Duration::from_millis(100));
        // Model synchronous compiler work that starves this runtime's timer.
        std::thread::sleep(Duration::from_millis(150));
        tokio::time::timeout(Duration::from_millis(50), statement.cancelled())
            .await
            .unwrap();
        assert_eq!(reason.get(), Some(&StatementCancelReason::StatementTimeout));
    }
}
