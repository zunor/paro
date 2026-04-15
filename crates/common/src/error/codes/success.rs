//! Class 00 - Successful Completion
use crate::error::SqlState;
pub const SUCCESSFUL_COMPLETION: SqlState = SqlState::new(*b"00000");
