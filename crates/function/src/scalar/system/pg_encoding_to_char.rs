//! pg_encoding_to_char Function
//!
//! PostgreSQL-compatible function that returns the encoding name for a given encoding ID.
//!
//!
//!
//! ## PostgreSQL Reference
//! `pg_encoding_to_char(encoding_id) -> name`
//! Returns the encoding name for a given encoding ID.
//!
//! ## Paro Implementation
//! Common encodings:
//! - 0: SQL_ASCII
//! - 6: UTF8
//! - 8: LATIN1
//!
//! For unknown encoding IDs, returns an empty string (PostgreSQL behavior).

use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_common::vector::Vector;

use crate::{ExpressionState, ScalarFunction, ScalarFunctionSet};

/// PostgreSQL encoding ID to name mapping.
///
/// Based on PostgreSQL's pg_enc enum in mb/pg_wchar.h
fn encoding_id_to_name(encoding_id: i32) -> &'static str {
    match encoding_id {
        0 => "SQL_ASCII",
        1 => "EUC_JP",
        2 => "EUC_CN",
        3 => "EUC_KR",
        4 => "EUC_TW",
        5 => "EUC_JIS_2004",
        6 => "UTF8",
        7 => "MULE_INTERNAL",
        8 => "LATIN1",
        9 => "LATIN2",
        10 => "LATIN3",
        11 => "LATIN4",
        12 => "LATIN5",
        13 => "LATIN6",
        14 => "LATIN7",
        15 => "LATIN8",
        16 => "LATIN9",
        17 => "LATIN10",
        18 => "WIN1256",
        19 => "WIN1258",
        20 => "WIN866",
        21 => "WIN874",
        22 => "KOI8R",
        23 => "WIN1251",
        24 => "WIN1252",
        25 => "ISO_8859_5",
        26 => "ISO_8859_6",
        27 => "ISO_8859_7",
        28 => "ISO_8859_8",
        29 => "WIN1250",
        30 => "WIN1253",
        31 => "WIN1254",
        32 => "WIN1255",
        33 => "WIN1257",
        34 => "KOI8U",
        35 => "SJIS",
        36 => "BIG5",
        37 => "GBK",
        38 => "UHC",
        39 => "GB18030",
        40 => "JOHAB",
        41 => "SHIFT_JIS_2004",
        _ => "", // Unknown encoding returns empty string (PostgreSQL behavior)
    }
}

/// Implementation of `pg_encoding_to_char(INTEGER) -> VARCHAR`.
///
/// Returns the encoding name for a given encoding ID.
fn pg_encoding_to_char_impl(
    input: &Chunk,
    _state: &dyn ExpressionState,
    result: &mut Vector,
) -> Result<()> {
    let count = input.size();
    let input_vec = input
        .column(0)
        .ok_or_else(|| paro_error::internal("Missing input column"))?;

    result.set_count(count);

    for i in 0..count {
        if input_vec.is_null(i) {
            result.validity_mut().set_null(i);
        } else {
            let encoding_id = input_vec.get_i32(i).unwrap_or(0);
            let encoding_name = encoding_id_to_name(encoding_id);
            result.set_string(i, encoding_name);
        }
    }

    Ok(())
}

/// Get `pg_encoding_to_char` function set.
pub fn get_pg_encoding_to_char_functions() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("pg_encoding_to_char".to_string());
    set.add_function(ScalarFunction::new(
        "pg_encoding_to_char".to_string(),
        vec![LogicalType::Integer],
        LogicalType::Varchar,
        pg_encoding_to_char_impl,
    ));
    set
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::any::Any;

    struct MockState;
    impl ExpressionState for MockState {
        fn current_database(&self) -> Option<&str> {
            None
        }
        fn current_schema(&self) -> Option<&str> {
            None
        }
        fn current_user(&self) -> Option<&str> {
            None
        }
        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    #[test]
    fn test_pg_encoding_to_char_utf8() {
        let input_vec = Vector::from_i32(&[6]);
        let chunk = Chunk::from_vectors(vec![input_vec]);
        let state = MockState;
        let mut result = Vector::new(LogicalType::Varchar);

        pg_encoding_to_char_impl(&chunk, &state, &mut result).unwrap();

        assert_eq!(result.get_string(0), Some("UTF8"));
    }

    #[test]
    fn test_pg_encoding_to_char_common_encodings() {
        let input_vec = Vector::from_i32(&[0, 6, 8, 24]);
        let chunk = Chunk::from_vectors(vec![input_vec]);
        let state = MockState;
        let mut result = Vector::new(LogicalType::Varchar);

        pg_encoding_to_char_impl(&chunk, &state, &mut result).unwrap();

        assert_eq!(result.get_string(0), Some("SQL_ASCII"));
        assert_eq!(result.get_string(1), Some("UTF8"));
        assert_eq!(result.get_string(2), Some("LATIN1"));
        assert_eq!(result.get_string(3), Some("WIN1252"));
    }

    #[test]
    fn test_pg_encoding_to_char_unknown() {
        let input_vec = Vector::from_i32(&[999, -1]);
        let chunk = Chunk::from_vectors(vec![input_vec]);
        let state = MockState;
        let mut result = Vector::new(LogicalType::Varchar);

        pg_encoding_to_char_impl(&chunk, &state, &mut result).unwrap();

        assert_eq!(result.get_string(0), Some(""));
        assert_eq!(result.get_string(1), Some(""));
    }

    #[test]
    fn test_pg_encoding_to_char_with_null() {
        let mut input_vec = Vector::from_i32(&[6, 0]);
        input_vec.validity_mut().set_null(1);
        let chunk = Chunk::from_vectors(vec![input_vec]);
        let state = MockState;
        let mut result = Vector::new(LogicalType::Varchar);

        pg_encoding_to_char_impl(&chunk, &state, &mut result).unwrap();

        assert_eq!(result.get_string(0), Some("UTF8"));
        assert!(result.is_null(1));
    }

    #[test]
    fn test_pg_encoding_to_char_function_set() {
        let set = get_pg_encoding_to_char_functions();
        assert_eq!(set.name, "pg_encoding_to_char");
        assert_eq!(set.functions.len(), 1);

        let func = &set.functions[0];
        assert_eq!(func.arguments, vec![LogicalType::Integer]);
        assert_eq!(func.return_type, LogicalType::Varchar);
    }
}
