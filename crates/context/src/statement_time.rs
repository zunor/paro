// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Wall-clock anchors whose values are fixed by statement and transaction lifecycles.

use std::time::{SystemTime, UNIX_EPOCH};

/// Immutable wall-clock timestamps captured before a statement is compiled.
///
/// Keeping these values in the frozen statement context prevents physical operators and scalar
/// functions from observing the clock independently. It also gives transaction-stable and
/// statement-stable SQL functions distinct lifecycle-owned anchors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StatementTimeContext {
    transaction_started_at: SystemTime,
    statement_started_at: SystemTime,
}

impl StatementTimeContext {
    /// Capture a statement timestamp and pair it with the active transaction timestamp.
    ///
    /// Compile-only contexts may not own a transaction. In that case both anchors use the
    /// statement capture; executable statements install an active transaction before freezing.
    pub fn capture(transaction_started_at: Option<SystemTime>) -> Self {
        let statement_started_at = SystemTime::now();
        Self {
            transaction_started_at: transaction_started_at.unwrap_or(statement_started_at),
            statement_started_at,
        }
    }

    pub fn new(transaction_started_at: SystemTime, statement_started_at: SystemTime) -> Self {
        Self {
            transaction_started_at,
            statement_started_at,
        }
    }

    #[inline]
    pub fn transaction_started_at(&self) -> SystemTime {
        self.transaction_started_at
    }

    #[inline]
    pub fn statement_started_at(&self) -> SystemTime {
        self.statement_started_at
    }

    #[inline]
    pub fn transaction_timestamp_micros(&self) -> i64 {
        system_time_to_unix_micros(self.transaction_started_at)
    }

    #[inline]
    pub fn statement_timestamp_micros(&self) -> i64 {
        system_time_to_unix_micros(self.statement_started_at)
    }
}

fn system_time_to_unix_micros(time: SystemTime) -> i64 {
    match time.duration_since(UNIX_EPOCH) {
        Ok(duration) => i64::try_from(duration.as_micros()).unwrap_or(i64::MAX),
        Err(error) => -i64::try_from(error.duration().as_micros()).unwrap_or(i64::MAX),
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn exposes_independent_transaction_and_statement_anchors() {
        let transaction = UNIX_EPOCH + Duration::from_micros(11);
        let statement = UNIX_EPOCH + Duration::from_micros(29);
        let context = StatementTimeContext::new(transaction, statement);

        assert_eq!(context.transaction_timestamp_micros(), 11);
        assert_eq!(context.statement_timestamp_micros(), 29);
    }

    #[test]
    fn preserves_timestamps_before_the_unix_epoch() {
        let context = StatementTimeContext::new(
            UNIX_EPOCH - Duration::from_micros(7),
            UNIX_EPOCH - Duration::from_micros(3),
        );

        assert_eq!(context.transaction_timestamp_micros(), -7);
        assert_eq!(context.statement_timestamp_micros(), -3);
    }
}
