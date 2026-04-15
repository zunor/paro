//! Class 40 - Transaction Rollback
use crate::error::SqlState;

pub const TRANSACTION_ROLLBACK: SqlState = SqlState::new(*b"40000");
pub const SERIALIZATION_FAILURE: SqlState = SqlState::new(*b"40001");
pub const STATEMENT_COMPLETION_UNKNOWN: SqlState = SqlState::new(*b"40003");
pub const DEADLOCK_DETECTED: SqlState = SqlState::new(*b"40P01");
