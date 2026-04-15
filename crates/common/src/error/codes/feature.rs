//! Class 0A - Feature Not Supported
use crate::error::SqlState;
pub const FEATURE_NOT_SUPPORTED: SqlState = SqlState::new(*b"0A000");
