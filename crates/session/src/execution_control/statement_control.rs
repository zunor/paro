// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use arc_swap::ArcSwapOption;
use paro_context::{
    NoopStatementTimeoutDriver, StatementCancelReason, StatementCancellation,
    StatementExecutionTracker, StatementTimeoutDriver,
};
use paro_scheduler::coordinator::EventCoordinator;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

pub struct ActiveStatementControl {
    cancellation: StatementCancellation,
    statement_token: CancellationToken,
    coordinator: ArcSwapOption<EventCoordinator>,
    cancel_reason: Arc<OnceLock<StatementCancelReason>>,
}

impl std::fmt::Debug for ActiveStatementControl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ActiveStatementControl")
            .field("is_cancelled", &self.cancellation.is_cancelled())
            .field("reason", &self.cancellation.reason())
            .field("has_coordinator", &self.coordinator.load_full().is_some())
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
            coordinator: ArcSwapOption::empty(),
            cancel_reason,
        }
    }

    pub fn cancellation(&self) -> StatementCancellation {
        self.cancellation.clone()
    }

    pub fn statement_token(&self) -> CancellationToken {
        self.statement_token.clone()
    }

    pub fn set_coordinator(&self, coordinator: Arc<EventCoordinator>) {
        self.coordinator.store(Some(coordinator));
    }

    pub fn clear_coordinator(&self) {
        self.coordinator.store(None);
    }

    pub fn coordinator(&self) -> Option<Arc<EventCoordinator>> {
        self.coordinator.load_full()
    }

    pub fn cancel(&self, reason: StatementCancelReason) {
        let _ = self.cancel_reason.set(reason);
        self.statement_token.cancel();
        if let Some(coordinator) = self.coordinator() {
            coordinator.cancel();
        }
    }

    pub fn cancel_for_connection_shutdown(&self) {
        self.statement_token.cancel();
        if let Some(coordinator) = self.coordinator() {
            coordinator.cancel();
        }
    }
}

impl StatementExecutionTracker for ActiveStatementControl {
    fn bind_coordinator(&self, coordinator: Arc<EventCoordinator>) {
        self.set_coordinator(coordinator);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_scheduler::scheduler::TaskScheduler;

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

    #[test]
    fn statement_cancel_propagates_to_coordinator() {
        let connection_shutdown = CancellationToken::new();
        let control = ActiveStatementControl::new(&connection_shutdown, None);
        let coordinator = Arc::new(EventCoordinator::new(Arc::new(TaskScheduler::new())));
        control.set_coordinator(coordinator.clone());

        control.cancel(StatementCancelReason::UserRequest);

        assert!(coordinator.is_cancelled());
    }
}
