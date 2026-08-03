// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::statement_control::ActiveStatementControl;
use super::TokioStatementTimeoutDriver;
use parking_lot::Mutex;
use paro_common::error::{self as paro_error, Result};
use paro_context::{StatementCancelReason, StatementCancellation, StatementTimeoutDriver};
use paro_instance::{ConnectionShutdownReason, SessionExecutionHandle};
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

pub struct SessionExecutionControl {
    connection_shutdown: CancellationToken,
    shutdown_reason: Arc<OnceLock<ConnectionShutdownReason>>,
    active_statement: Mutex<Option<Arc<ActiveStatementControl>>>,
    statement_timeout_driver: Arc<dyn StatementTimeoutDriver>,
}

impl std::fmt::Debug for SessionExecutionControl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SessionExecutionControl")
            .field(
                "connection_shutdown_requested",
                &self.connection_shutdown.is_cancelled(),
            )
            .field("shutdown_reason", &self.shutdown_reason())
            .field("has_active_statement", &self.active_statement().is_some())
            .finish()
    }
}

impl Default for SessionExecutionControl {
    fn default() -> Self {
        Self::new()
    }
}

impl SessionExecutionControl {
    pub fn new() -> Self {
        Self::with_timeout_driver(Arc::new(TokioStatementTimeoutDriver))
    }

    pub fn with_timeout_driver(statement_timeout_driver: Arc<dyn StatementTimeoutDriver>) -> Self {
        Self {
            connection_shutdown: CancellationToken::new(),
            shutdown_reason: Arc::new(OnceLock::new()),
            active_statement: Mutex::new(None),
            statement_timeout_driver,
        }
    }

    pub(crate) fn begin_statement(
        &self,
        statement_timeout: Option<Duration>,
    ) -> Result<Arc<ActiveStatementControl>> {
        let mut active = self.active_statement.lock();
        if active.is_some() {
            return Err(paro_error::internal(
                "cannot begin a statement while another statement is active",
            ));
        }

        let control = Arc::new(ActiveStatementControl::with_timeout_driver(
            &self.connection_shutdown,
            statement_timeout,
            self.statement_timeout_driver.clone(),
        ));
        *active = Some(control.clone());
        Ok(control)
    }

    pub(crate) fn finish_statement(&self, statement: &Arc<ActiveStatementControl>) {
        let mut active = self.active_statement.lock();
        if active
            .as_ref()
            .is_some_and(|current| Arc::ptr_eq(current, statement))
        {
            active.take();
        }
    }

    pub fn active_statement(&self) -> Option<Arc<ActiveStatementControl>> {
        self.active_statement.lock().clone()
    }

    pub fn cancel_active_statement(&self, reason: StatementCancelReason) -> bool {
        if self.connection_shutdown_requested() {
            return false;
        }
        let Some(statement) = self.active_statement() else {
            return false;
        };
        statement.cancel(reason);
        true
    }

    pub fn request_connection_shutdown(&self, reason: ConnectionShutdownReason) {
        let _ = self.shutdown_reason.set(reason);
        self.connection_shutdown.cancel();
        if let Some(statement) = self.active_statement() {
            statement.cancel_for_connection_shutdown();
        }
    }

    pub fn connection_shutdown_requested(&self) -> bool {
        self.connection_shutdown.is_cancelled()
    }

    pub fn shutdown_reason(&self) -> Option<ConnectionShutdownReason> {
        self.shutdown_reason.get().copied()
    }

    pub fn connection_shutdown_token(&self) -> CancellationToken {
        self.connection_shutdown.clone()
    }

    pub fn compile_scope_cancellation(
        &self,
        statement_timeout: Option<Duration>,
    ) -> StatementCancellation {
        StatementCancellation::with_timeout_driver(
            self.connection_shutdown_token(),
            statement_timeout,
            self.statement_timeout_driver.clone(),
        )
    }
}

impl SessionExecutionHandle for SessionExecutionControl {
    fn cancel_active_statement(&self, reason: StatementCancelReason) -> bool {
        SessionExecutionControl::cancel_active_statement(self, reason)
    }

    fn request_connection_shutdown(&self, reason: ConnectionShutdownReason) {
        SessionExecutionControl::request_connection_shutdown(self, reason);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connection_shutdown_cancels_active_statement_without_setting_statement_reason() {
        let control = SessionExecutionControl::with_timeout_driver(Arc::new(
            paro_context::NoopStatementTimeoutDriver,
        ));
        let statement = control.begin_statement(None).unwrap();

        control.request_connection_shutdown(ConnectionShutdownReason::ForceClosed);

        assert!(control.connection_shutdown_requested());
        assert_eq!(
            control.shutdown_reason(),
            Some(ConnectionShutdownReason::ForceClosed)
        );
        assert!(statement.cancellation().is_cancelled());
        assert_eq!(statement.cancellation().reason(), None);
    }

    #[test]
    fn compile_scope_cancellation_uses_shared_timeout_driver() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        #[derive(Default)]
        struct RecordingTimeoutDriver {
            arms: AtomicUsize,
        }

        impl StatementTimeoutDriver for RecordingTimeoutDriver {
            fn arm(
                &self,
                _statement_token: &CancellationToken,
                _cancel_reason: &Arc<OnceLock<StatementCancelReason>>,
                _timeout_lifetime: &Arc<CancellationToken>,
                _timeout: Duration,
            ) {
                self.arms.fetch_add(1, Ordering::SeqCst);
            }
        }

        let driver = Arc::new(RecordingTimeoutDriver::default());
        let control = SessionExecutionControl::with_timeout_driver(driver.clone());

        let _statement = control
            .begin_statement(Some(Duration::from_millis(10)))
            .unwrap();
        let _compile = control.compile_scope_cancellation(Some(Duration::from_millis(10)));

        assert_eq!(driver.arms.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn begin_statement_rejects_overlapping_statement() {
        let control = SessionExecutionControl::with_timeout_driver(Arc::new(
            paro_context::NoopStatementTimeoutDriver,
        ));
        let first = control.begin_statement(None).unwrap();

        let error = control
            .begin_statement(None)
            .expect_err("overlapping statement must be rejected");

        assert!(error.to_string().contains("another statement is active"));
        assert!(Arc::ptr_eq(
            control.active_statement().as_ref().unwrap(),
            &first
        ));
    }
}
