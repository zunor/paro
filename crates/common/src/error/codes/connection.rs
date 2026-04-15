//! Class 08 - Connection Exception
use crate::error::SqlState;

pub const CONNECTION_EXCEPTION: SqlState = SqlState::new(*b"08000");
pub const CONNECTION_DOES_NOT_EXIST: SqlState = SqlState::new(*b"08003");
pub const CONNECTION_FAILURE: SqlState = SqlState::new(*b"08006");
pub const SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION: SqlState = SqlState::new(*b"08001");
pub const PROTOCOL_VIOLATION: SqlState = SqlState::new(*b"08P01");
