// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_context::{
    NoopStatementTimeoutDriver, StatementCancelReason, StatementCancellation,
    StatementTimeoutDriver,
};
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

pub struct ActiveStatementControl {
    cancellation: StatementCancellation,
    statement_token: CancellationToken,
    cancel_reason: Arc<OnceLock<StatementCancelReason>>,
}

impl std::fmt::Debug for ActiveStatementControl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ActiveStatementControl")
            .field("is_cancelled", &self.cancellation.is_cancelled())
            .field("reason", &self.cancellation.reason())
            .finish()
    }
}

impl ActiveStatementControl {
    pub fn new(
        connection_shutdown: &CancellationToken,
        statement_timeout: Option<Duration>,
    ) -> Self {
        Self::with_timeout_driver(
            connection_shutdown,
            statement_timeout,
            Arc::new(NoopStatementTimeoutDriver),
        )
    }

    pub fn with_timeout_driver(
        connection_shutdown: &CancellationToken,
        statement_timeout: Option<Duration>,
        timeout_driver: Arc<dyn StatementTimeoutDriver>,
    ) -> Self {
        let cancel_reason = Arc::new(OnceLock::new());
        let statement_token = connection_shutdown.child_token();
        let cancellation = StatementCancellation::from_parts(
            connection_shutdown.clone(),
            statement_token.clone(),
            statement_timeout,
            cancel_reason.clone(),
            timeout_driver,
        );
        Self {
            cancellation,
            statement_token,
            cancel_reason,
        }
    }

    pub fn cancellation(&self) -> StatementCancellation {
        self.cancellation.clone()
    }

    pub fn statement_token(&self) -> CancellationToken {
        self.statement_token.clone()
    }

    pub fn cancel(&self, reason: StatementCancelReason) {
        let _ = self.cancel_reason.set(reason);
        self.statement_token.cancel();
    }

    pub fn cancel_for_connection_shutdown(&self) {
        self.statement_token.cancel();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statement_cancel_records_reason() {
        let connection_shutdown = CancellationToken::new();
        let control = ActiveStatementControl::new(&connection_shutdown, None);

        control.cancel(StatementCancelReason::UserRequest);

        assert!(control.cancellation().is_cancelled());
        assert_eq!(
            control.cancellation().reason(),
            Some(StatementCancelReason::UserRequest)
        );
    }
}
