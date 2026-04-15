// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Class 42 - Syntax Error or Access Rule Violation (and related)
use crate::error::SqlState;

// Basic syntax errors
pub const SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION: SqlState = SqlState::new(*b"42000");
pub const SYNTAX_ERROR: SqlState = SqlState::new(*b"42601");
pub const INSUFFICIENT_PRIVILEGE: SqlState = SqlState::new(*b"42501");

// Undefined objects
pub const UNDEFINED_COLUMN: SqlState = SqlState::new(*b"42703");
pub const UNDEFINED_FUNCTION: SqlState = SqlState::new(*b"42883");
pub const UNDEFINED_TABLE: SqlState = SqlState::new(*b"42P01");
pub const UNDEFINED_PARAMETER: SqlState = SqlState::new(*b"42P02");
pub const UNDEFINED_OBJECT: SqlState = SqlState::new(*b"42704");
pub const UNDEFINED_SCHEMA: SqlState = SqlState::new(*b"3F000");
pub const UNDEFINED_DATABASE: SqlState = SqlState::new(*b"3D000");

// Duplicate objects
pub const DUPLICATE_COLUMN: SqlState = SqlState::new(*b"42701");
pub const DUPLICATE_TABLE: SqlState = SqlState::new(*b"42P07");
pub const DUPLICATE_SCHEMA: SqlState = SqlState::new(*b"42P06");
pub const DUPLICATE_OBJECT: SqlState = SqlState::new(*b"42710");
pub const DUPLICATE_ALIAS: SqlState = SqlState::new(*b"42712");
pub const DUPLICATE_FUNCTION: SqlState = SqlState::new(*b"42723");
pub const DUPLICATE_DATABASE: SqlState = SqlState::new(*b"42P04");

// Ambiguous references
pub const AMBIGUOUS_COLUMN: SqlState = SqlState::new(*b"42702");
pub const AMBIGUOUS_FUNCTION: SqlState = SqlState::new(*b"42725");
pub const AMBIGUOUS_PARAMETER: SqlState = SqlState::new(*b"42P08");

// Type errors
pub const DATATYPE_MISMATCH: SqlState = SqlState::new(*b"42804");
pub const WRONG_OBJECT_TYPE: SqlState = SqlState::new(*b"42809");
pub const INDETERMINATE_DATATYPE: SqlState = SqlState::new(*b"42P18");
pub const CANNOT_COERCE: SqlState = SqlState::new(*b"42846");

// Grouping and windowing
pub const GROUPING_ERROR: SqlState = SqlState::new(*b"42803");
pub const WINDOWING_ERROR: SqlState = SqlState::new(*b"42P20");

// Invalid definitions
pub const INVALID_COLUMN_REFERENCE: SqlState = SqlState::new(*b"42P10");
pub const INVALID_COLUMN_DEFINITION: SqlState = SqlState::new(*b"42611");
pub const INVALID_TABLE_DEFINITION: SqlState = SqlState::new(*b"42P16");
pub const INVALID_FUNCTION_DEFINITION: SqlState = SqlState::new(*b"42P13");
pub const INVALID_NAME: SqlState = SqlState::new(*b"42602");
pub const NAME_TOO_LONG: SqlState = SqlState::new(*b"42622");
