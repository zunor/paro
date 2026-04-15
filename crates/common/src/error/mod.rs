//! Paro Error System
//!
//! A PostgreSQL-compatible error handling system providing structured errors
//! with SQLSTATE codes, severity levels, and rich context information.
//!
//! # Quick Start
//!
//! ```ignore
//! use paro_common::error;
//!
//! // Simple error creation
//! let err = error::syntax("unexpected token");
//! let err = error::table_not_found("users");
//! let err = error::not_implemented("LATERAL JOIN");
//!
//! // With additional context (chaining)
//! let err = error::table_not_found("users")
//!     .schema("public")
//!     .hint("Check if the table exists in the current schema.");
//!
//! // Wrap external parser errors
//! // let err = error::from_parser(parser_err.to_string());
//! ```

// ============================================================
// Core types
// ============================================================

mod error_class;
mod error_data;
mod error_type;
mod severity;
mod sqlstate;

pub use error_class::ErrorClass;
pub use error_data::ErrorData;
pub use error_type::ParoError;
pub use severity::Severity;
pub use sqlstate::SqlState;

// ============================================================
// SQLSTATE constants
// ============================================================

pub mod codes;

// ============================================================
// Convenience constructors
// ============================================================

mod make_catalog;
mod make_constraint;
mod make_data;
mod make_internal;
mod make_resource;
mod make_syntax;
mod make_system;
mod make_transaction;

// ============================================================
// Flat API exports
// ============================================================

// Syntax-related
pub use make_syntax::from_parser;
pub use make_syntax::from_parser_at;
pub use make_syntax::grouping_error;
pub use make_syntax::insufficient_privilege;
pub use make_syntax::not_implemented;
pub use make_syntax::not_supported;
pub use make_syntax::syntax;
pub use make_syntax::syntax_at;
pub use make_syntax::type_mismatch;
pub use make_syntax::windowing_error;

// Catalog objects
pub use make_catalog::ambiguous_column;
pub use make_catalog::ambiguous_function;
pub use make_catalog::catalog;
pub use make_catalog::column_exists;
pub use make_catalog::column_not_found;
pub use make_catalog::database_exists;
pub use make_catalog::database_not_found;
pub use make_catalog::function_not_found;
pub use make_catalog::object_exists;
pub use make_catalog::object_not_found;
pub use make_catalog::schema_exists;
pub use make_catalog::schema_not_found;
pub use make_catalog::table_exists;
pub use make_catalog::table_not_found;
pub use make_catalog::wrong_object_type;

// Data errors
pub use make_data::array_subscript_error;
pub use make_data::cannot_cast;
pub use make_data::division_by_zero;
pub use make_data::invalid_datetime;
pub use make_data::invalid_input;
pub use make_data::invalid_parameter;
pub use make_data::invalid_regex;
pub use make_data::invalid_value;
pub use make_data::null_not_allowed;
pub use make_data::out_of_range;
pub use make_data::overflow;
pub use make_data::sequence_generator_error;
pub use make_data::string_too_long;

// Constraint violations
pub use make_constraint::check_violation;
pub use make_constraint::exclusion_violation;
pub use make_constraint::foreign_key_violation;
pub use make_constraint::not_null_violation;
pub use make_constraint::restrict_violation;
pub use make_constraint::unique_violation;

// Transaction errors
pub use make_transaction::idle_in_transaction_timeout;
pub use make_transaction::invalid_transaction_state;
pub use make_transaction::no_transaction;
pub use make_transaction::read_only_transaction;
pub use make_transaction::serialization_failure;
pub use make_transaction::transaction_aborted;
pub use make_transaction::transaction_active;
pub use make_transaction::transaction_timeout;

// System errors
pub use make_resource::configuration_limit_exceeded;
pub use make_resource::disk_full;
pub use make_resource::out_of_memory;
pub use make_resource::too_many_connections;
pub use make_system::admin_shutdown;
pub use make_system::cannot_connect_now;
pub use make_system::connection_failure;
pub use make_system::io;
pub use make_system::io_error;
pub use make_system::protocol_violation;
pub use make_system::query_canceled;
pub use make_system::system_error;

// Internal errors
pub use make_internal::data_corrupted;
pub use make_internal::from_std;
pub use make_internal::index_corrupted;
pub use make_internal::internal;
pub use make_internal::internal_detail;
pub use make_internal::panic;
pub use make_internal::serialization_error;

// ============================================================
// Result type alias
// ============================================================

/// Result type alias using [`ParoError`] as the error type.
pub type Result<T> = std::result::Result<T, ParoError>;
