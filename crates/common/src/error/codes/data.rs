// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Class 22 - Data Exception
use crate::error::SqlState;

pub const DATA_EXCEPTION: SqlState = SqlState::new(*b"22000");
pub const DIVISION_BY_ZERO: SqlState = SqlState::new(*b"22012");
pub const NUMERIC_VALUE_OUT_OF_RANGE: SqlState = SqlState::new(*b"22003");
pub const STRING_DATA_RIGHT_TRUNCATION: SqlState = SqlState::new(*b"22001");
pub const DATETIME_FIELD_OVERFLOW: SqlState = SqlState::new(*b"22008");
pub const INVALID_DATETIME_FORMAT: SqlState = SqlState::new(*b"22007");
pub const INVALID_PARAMETER_VALUE: SqlState = SqlState::new(*b"22023");
pub const INVALID_TEXT_REPRESENTATION: SqlState = SqlState::new(*b"22P02");
pub const INVALID_BINARY_REPRESENTATION: SqlState = SqlState::new(*b"22P03");
pub const NULL_VALUE_NOT_ALLOWED: SqlState = SqlState::new(*b"22004");
pub const INVALID_CHARACTER_VALUE_FOR_CAST: SqlState = SqlState::new(*b"22018");
pub const INVALID_ESCAPE_SEQUENCE: SqlState = SqlState::new(*b"22025");
pub const ARRAY_SUBSCRIPT_ERROR: SqlState = SqlState::new(*b"2202E");
pub const INVALID_REGULAR_EXPRESSION: SqlState = SqlState::new(*b"2201B");
pub const FLOATING_POINT_EXCEPTION: SqlState = SqlState::new(*b"22P01");
pub const SEQUENCE_GENERATOR_ERROR: SqlState = SqlState::new(*b"2200H");
