//! Class 25 - Invalid Transaction State
use crate::error::SqlState;

pub const INVALID_TRANSACTION_STATE: SqlState = SqlState::new(*b"25000");
pub const ACTIVE_SQL_TRANSACTION: SqlState = SqlState::new(*b"25001");
pub const READ_ONLY_SQL_TRANSACTION: SqlState = SqlState::new(*b"25006");
pub const NO_ACTIVE_SQL_TRANSACTION: SqlState = SqlState::new(*b"25P01");
pub const IN_FAILED_SQL_TRANSACTION: SqlState = SqlState::new(*b"25P02");
pub const IDLE_IN_TRANSACTION_SESSION_TIMEOUT: SqlState = SqlState::new(*b"25P03");
pub const TRANSACTION_TIMEOUT: SqlState = SqlState::new(*b"25P04");
