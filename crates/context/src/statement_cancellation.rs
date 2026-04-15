use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

pub trait StatementTimeoutDriver: Send + Sync {
    fn arm(&self, _statement_token: &CancellationToken, _timeout: Duration) {}
}

#[derive(Debug, Default)]
pub struct NoopStatementTimeoutDriver;

impl StatementTimeoutDriver for NoopStatementTimeoutDriver {}

#[derive(Clone)]
pub struct StatementCancellation {
    pub session_token: CancellationToken,
    pub statement_token: CancellationToken,
    pub statement_timeout: Option<Duration>,
    timeout_driver: Arc<dyn StatementTimeoutDriver>,
}

impl std::fmt::Debug for StatementCancellation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StatementCancellation")
            .field("statement_timeout", &self.statement_timeout)
            .finish_non_exhaustive()
    }
}

impl StatementCancellation {
    pub fn new(session_token: CancellationToken, statement_timeout: Option<Duration>) -> Self {
        Self::with_timeout_driver(
            session_token,
            statement_timeout,
            Arc::new(NoopStatementTimeoutDriver),
        )
    }

    pub fn with_timeout_driver(
        session_token: CancellationToken,
        statement_timeout: Option<Duration>,
        timeout_driver: Arc<dyn StatementTimeoutDriver>,
    ) -> Self {
        let statement_token = session_token.child_token();
        if let Some(timeout) = statement_timeout {
            timeout_driver.arm(&statement_token, timeout);
        }
        Self {
            session_token,
            statement_token,
            statement_timeout,
            timeout_driver,
        }
    }

    pub fn child_execution_attempt(&self) -> Self {
        let statement_token = self.statement_token.child_token();
        if let Some(timeout) = self.statement_timeout {
            self.timeout_driver.arm(&statement_token, timeout);
        }
        Self {
            session_token: self.session_token.clone(),
            statement_token,
            statement_timeout: self.statement_timeout,
            timeout_driver: self.timeout_driver.clone(),
        }
    }

    pub fn is_cancelled(&self) -> bool {
        self.statement_token.is_cancelled()
    }

    pub fn session_cancelled(&self) -> bool {
        self.session_token.is_cancelled()
    }

    pub fn timeout_configured(&self) -> bool {
        self.statement_timeout.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Default)]
    struct RecordingTimeoutDriver {
        arms: AtomicUsize,
    }

    impl StatementTimeoutDriver for RecordingTimeoutDriver {
        fn arm(&self, _statement_token: &CancellationToken, _timeout: Duration) {
            self.arms.fetch_add(1, Ordering::SeqCst);
        }
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
    fn session_cancel_propagates_to_statement_without_reversing_direction() {
        let session_token = CancellationToken::new();
        let cancellation = StatementCancellation::new(session_token.clone(), None);

        assert!(!cancellation.is_cancelled());
        assert!(!cancellation.session_cancelled());

        cancellation.statement_token.cancel();
        assert!(cancellation.is_cancelled());
        assert!(!cancellation.session_cancelled());

        let second = StatementCancellation::new(session_token.clone(), None);
        session_token.cancel();
        assert!(second.is_cancelled());
        assert!(second.session_cancelled());
    }
}
