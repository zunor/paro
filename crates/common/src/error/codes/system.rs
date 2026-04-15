//! Class 58 - System Error
use crate::error::SqlState;

pub const SYSTEM_ERROR: SqlState = SqlState::new(*b"58000");
pub const IO_ERROR: SqlState = SqlState::new(*b"58030");
pub const UNDEFINED_FILE: SqlState = SqlState::new(*b"58P01");
pub const DUPLICATE_FILE: SqlState = SqlState::new(*b"58P02");
