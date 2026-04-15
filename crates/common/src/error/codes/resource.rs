//! Class 53 - Insufficient Resources
use crate::error::SqlState;

pub const INSUFFICIENT_RESOURCES: SqlState = SqlState::new(*b"53000");
pub const DISK_FULL: SqlState = SqlState::new(*b"53100");
pub const OUT_OF_MEMORY: SqlState = SqlState::new(*b"53200");
pub const TOO_MANY_CONNECTIONS: SqlState = SqlState::new(*b"53300");
pub const CONFIGURATION_LIMIT_EXCEEDED: SqlState = SqlState::new(*b"53400");
