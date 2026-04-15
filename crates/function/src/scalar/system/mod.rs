//! System Functions
//!
//! PostgreSQL-compatible system functions for pg_catalog schema.
//!
//!
//!
//! ## Functions
//! - `pg_get_userbyid(oid)`: Returns the username for a given OID
//! - `pg_encoding_to_char(encoding)`: Returns the encoding name for a given encoding ID
//! - `version()`: Returns the database version string
//! - `current_database()`: Returns the current database name
//! - `current_schema()`: Returns the current schema name
//! - `current_user()`: Returns the current user name
//! - `array_to_string(array, delimiter [, null_string])`: Converts array/list to string

mod array_length;
mod array_to_string;
mod current_database;
mod current_schema;
mod current_setting;
mod current_user;
mod pg_encoding_to_char;
mod pg_get_userbyid;
mod version;

pub use array_length::*;
pub use array_to_string::*;
pub use current_database::*;
pub use current_schema::*;
pub use current_setting::*;
pub use current_user::*;
pub use pg_encoding_to_char::*;
pub use pg_get_userbyid::*;
pub use version::*;

use crate::ScalarFunctionSet;

/// Register all system functions and return them as a vector of function sets.
pub fn register_system_functions() -> Vec<ScalarFunctionSet> {
    vec![
        get_array_length_functions(),
        get_pg_get_userbyid_functions(),
        get_pg_encoding_to_char_functions(),
        get_version_functions(),
        get_current_database_functions(),
        get_current_setting_functions(),
        get_current_schema_functions(),
        get_current_user_functions(),
        get_array_to_string_functions(),
    ]
}
