//! Transaction-related error constructors.

use crate::error::{codes, ErrorData, ParoError, Severity};
use std::borrow::Cow;

/// Transaction is aborted, commands are ignored.
pub fn transaction_aborted() -> ParoError {
    ParoError::new(ErrorData::new(
        Severity::Error,
        codes::transaction::IN_FAILED_SQL_TRANSACTION,
        "current transaction is aborted, commands ignored until end of transaction block",
    ))
}

/// No active transaction.
pub fn no_transaction() -> ParoError {
    ParoError::new(
        ErrorData::new(
            Severity::Error,
            codes::transaction::NO_ACTIVE_SQL_TRANSACTION,
            "there is no transaction in progress",
        )
        .hint("Use BEGIN to start a transaction."),
    )
}

/// Transaction already active.
pub fn transaction_active() -> ParoError {
    ParoError::new(ErrorData::new(
        Severity::Error,
        codes::transaction::ACTIVE_SQL_TRANSACTION,
        "there is already a transaction in progress",
    ))
}

/// Read-only transaction.
pub fn read_only_transaction() -> ParoError {
    ParoError::new(
        ErrorData::new(
            Severity::Error,
            codes::transaction::READ_ONLY_SQL_TRANSACTION,
            "cannot execute statement in a read-only transaction",
        )
        .hint("Use SET TRANSACTION READ WRITE to allow writes."),
    )
}

/// Invalid transaction state (generic).
pub fn invalid_transaction_state(message: impl Into<Cow<'static, str>>) -> ParoError {
    ParoError::new(ErrorData::new(
        Severity::Error,
        codes::transaction::INVALID_TRANSACTION_STATE,
        message,
    ))
}

/// Idle in transaction session timeout.
pub fn idle_in_transaction_timeout() -> ParoError {
    ParoError::new(ErrorData::new(
        Severity::Fatal,
        codes::transaction::IDLE_IN_TRANSACTION_SESSION_TIMEOUT,
        "terminating connection due to idle-in-transaction timeout",
    ))
}

/// Transaction timeout.
pub fn transaction_timeout() -> ParoError {
    ParoError::new(ErrorData::new(
        Severity::Error,
        codes::transaction::TRANSACTION_TIMEOUT,
        "canceling statement due to transaction timeout",
    ))
}

/// Serialization failure (write-write conflict).
pub fn serialization_failure(message: impl Into<Cow<'static, str>>) -> ParoError {
    ParoError::new(ErrorData::new(
        Severity::Error,
        codes::rollback::SERIALIZATION_FAILURE,
        message,
    ))
}
