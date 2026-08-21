// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_common::error::{self as paro_error, Result};
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

pub trait StatementTimeoutDriver: Send + Sync {
    fn arm(
        &self,
        _statement_token: &CancellationToken,
        _cancel_reason: &Arc<OnceLock<StatementCancelReason>>,
        _timeout_lifetime: &CancellationToken,
        _timeout: Duration,
    ) {
    }
}

#[derive(Debug, Default)]
pub struct NoopStatementTimeoutDriver;

impl StatementTimeoutDriver for NoopStatementTimeoutDriver {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatementCancelReason {
    UserRequest,
    StatementTimeout,
}

struct TimeoutLifetime {
    token: CancellationToken,
}

impl TimeoutLifetime {
    fn new() -> Self {
        Self {
            token: CancellationToken::new(),
        }
    }
}

impl Drop for TimeoutLifetime {
    fn drop(&mut self) {
        self.token.cancel();
    }
}

#[derive(Clone)]
pub struct StatementCancellation {
    connection_token: CancellationToken,
    statement_token: CancellationToken,
    statement_timeout: Option<Duration>,
    cancel_reason: Arc<OnceLock<StatementCancelReason>>,
    timeout_driver: Arc<dyn StatementTimeoutDriver>,
    _timeout_lifetime: Arc<TimeoutLifetime>,
}

impl std::fmt::Debug for StatementCancellation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StatementCancellation")
            .field("reason", &self.reason())
            .field("statement_timeout", &self.statement_timeout)
            .finish_non_exhaustive()
    }
}

impl StatementCancellation {
    pub fn new(connection_token: CancellationToken, statement_timeout: Option<Duration>) -> Self {
        Self::with_timeout_driver(
            connection_token,
            statement_timeout,
            Arc::new(NoopStatementTimeoutDriver),
        )
    }

    pub fn with_timeout_driver(
        connection_token: CancellationToken,
        statement_timeout: Option<Duration>,
        timeout_driver: Arc<dyn StatementTimeoutDriver>,
    ) -> Self {
        let statement_token = connection_token.child_token();
        Self::from_parts(
            connection_token,
            statement_token,
            statement_timeout,
            Arc::new(OnceLock::new()),
            timeout_driver,
        )
    }

    pub fn from_parts(
        connection_token: CancellationToken,
        statement_token: CancellationToken,
        statement_timeout: Option<Duration>,
        cancel_reason: Arc<OnceLock<StatementCancelReason>>,
        timeout_driver: Arc<dyn StatementTimeoutDriver>,
    ) -> Self {
        let timeout_lifetime = Arc::new(TimeoutLifetime::new());
        if let Some(timeout) = statement_timeout {
            timeout_driver.arm(
                &statement_token,
                &cancel_reason,
                &timeout_lifetime.token,
                timeout,
            );
        }
        Self {
            connection_token,
            statement_token,
            statement_timeout,
            cancel_reason,
            timeout_driver,
            _timeout_lifetime: timeout_lifetime,
        }
    }

    pub fn child_execution_attempt(&self) -> Self {
        let statement_token = self.statement_token.child_token();
        Self::from_parts(
            self.connection_token.clone(),
            statement_token,
            self.statement_timeout,
            self.cancel_reason.clone(),
            self.timeout_driver.clone(),
        )
    }

    pub fn is_cancelled(&self) -> bool {
        self.statement_token.is_cancelled()
    }

    /// Wait until this statement, or its owning connection, is cancelled.
    pub async fn cancelled(&self) {
        self.statement_token.cancelled().await;
    }

    pub fn connection_cancelled(&self) -> bool {
        self.connection_token.is_cancelled()
    }

    pub fn timeout_configured(&self) -> bool {
        self.statement_timeout.is_some()
    }

    pub fn reason(&self) -> Option<StatementCancelReason> {
        self.cancel_reason.get().copied()
    }

    pub fn check(&self) -> Result<()> {
        if !self.is_cancelled() {
            return Ok(());
        }

        match self.reason() {
            Some(StatementCancelReason::UserRequest) => Err(paro_error::query_canceled()),
            Some(StatementCancelReason::StatementTimeout) => Err(paro_error::statement_timeout()),
            None => Err(paro_error::query_canceled()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    #[derive(Default)]
    struct RecordingTimeoutDriver {
        arms: AtomicUsize,
    }

    impl StatementTimeoutDriver for RecordingTimeoutDriver {
        fn arm(
            &self,
            _statement_token: &CancellationToken,
            _cancel_reason: &Arc<OnceLock<StatementCancelReason>>,
            _timeout_lifetime: &CancellationToken,
            _timeout: Duration,
        ) {
            self.arms.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[derive(Default)]
    struct CapturingTimeoutDriver {
        timeout_lifetime: Mutex<Option<CancellationToken>>,
    }

    impl StatementTimeoutDriver for CapturingTimeoutDriver {
        fn arm(
            &self,
            _statement_token: &CancellationToken,
            _cancel_reason: &Arc<OnceLock<StatementCancelReason>>,
            timeout_lifetime: &CancellationToken,
            _timeout: Duration,
        ) {
            *self.timeout_lifetime.lock().unwrap() = Some(timeout_lifetime.clone());
        }
    }

    #[test]
    fn timeout_lifetime_ends_exactly_when_the_last_cancellation_clone_drops() {
        let driver = Arc::new(CapturingTimeoutDriver::default());
        let cancellation = StatementCancellation::with_timeout_driver(
            CancellationToken::new(),
            Some(Duration::from_secs(1)),
            driver.clone(),
        );
        let second = cancellation.clone();
        let third = cancellation.clone();
        let timeout_lifetime = driver
            .timeout_lifetime
            .lock()
            .unwrap()
            .clone()
            .expect("timeout driver should capture its lifetime");

        drop(cancellation);
        drop(second);
        assert!(!timeout_lifetime.is_cancelled());
        drop(third);
        assert!(timeout_lifetime.is_cancelled());
    }

    #[test]
    fn statement_timeout_driver_is_armed_for_each_execution_attempt() {
        let driver = Arc::new(RecordingTimeoutDriver::default());
        let cancellation = StatementCancellation::with_timeout_driver(
            CancellationToken::new(),
            Some(Duration::from_millis(250)),
            driver.clone(),
        );

        assert!(cancellation.timeout_configured());
        assert_eq!(driver.arms.load(Ordering::SeqCst), 1);

        let retry = cancellation.child_execution_attempt();
        assert!(retry.timeout_configured());
        assert_eq!(driver.arms.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn connection_cancel_propagates_to_statement_without_reversing_direction() {
        let connection_token = CancellationToken::new();
        let first_statement = connection_token.child_token();
        let cancellation = StatementCancellation::from_parts(
            connection_token.clone(),
            first_statement.clone(),
            None,
            Arc::new(OnceLock::new()),
            Arc::new(NoopStatementTimeoutDriver),
        );

        assert!(!cancellation.is_cancelled());
        assert!(!cancellation.connection_cancelled());

        first_statement.cancel();
        assert!(cancellation.is_cancelled());
        assert!(!cancellation.connection_cancelled());

        let second = StatementCancellation::new(connection_token.clone(), None);
        connection_token.cancel();
        assert!(second.is_cancelled());
        assert!(second.connection_cancelled());
    }

    #[test]
    fn check_maps_statement_reasons_to_structured_errors() {
        let connection_token = CancellationToken::new();
        let statement_token = connection_token.child_token();
        let cancel_reason = Arc::new(OnceLock::new());
        let cancellation = StatementCancellation::from_parts(
            connection_token,
            statement_token.clone(),
            None,
            cancel_reason.clone(),
            Arc::new(NoopStatementTimeoutDriver),
        );

        let _ = cancel_reason.set(StatementCancelReason::UserRequest);
        statement_token.cancel();
        let err = cancellation
            .check()
            .expect_err("statement should be cancelled");
        assert!(err.is_query_canceled());
    }

    #[test]
    fn check_uses_distinct_statement_timeout_sqlstate() {
        let connection_token = CancellationToken::new();
        let statement_token = connection_token.child_token();
        let cancel_reason = Arc::new(OnceLock::new());
        let cancellation = StatementCancellation::from_parts(
            connection_token,
            statement_token.clone(),
            Some(Duration::from_secs(1)),
            cancel_reason.clone(),
            Arc::new(NoopStatementTimeoutDriver),
        );

        let _ = cancel_reason.set(StatementCancelReason::StatementTimeout);
        statement_token.cancel();
        let err = cancellation
            .check()
            .expect_err("statement timeout should be surfaced as an error");
        assert!(err.is_query_canceled());
        assert!(err.is(paro_common::error::codes::operator::STATEMENT_TIMEOUT));
    }
}
