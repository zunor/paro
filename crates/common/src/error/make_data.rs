//! Data-related error constructors.

use crate::error::{codes, ErrorData, ParoError, Severity};
use std::borrow::Cow;

/// Division by zero.
pub fn division_by_zero() -> ParoError {
    ParoError::new(ErrorData::new(
        Severity::Error,
        codes::data::DIVISION_BY_ZERO,
        "division by zero",
    ))
}

/// Numeric value out of range.
pub fn out_of_range(message: impl Into<Cow<'static, str>>) -> ParoError {
    ParoError::new(ErrorData::new(
        Severity::Error,
        codes::data::NUMERIC_VALUE_OUT_OF_RANGE,
        message,
    ))
}

/// Integer/numeric overflow.
pub fn overflow(datatype: impl AsRef<str>) -> ParoError {
    ParoError::new(ErrorData::new(
        Severity::Error,
        codes::data::NUMERIC_VALUE_OUT_OF_RANGE,
        format!("{} out of range", datatype.as_ref()),
    ))
}

/// Invalid input syntax for a type.
pub fn invalid_value(datatype: impl AsRef<str>, value: impl AsRef<str>) -> ParoError {
    ParoError::new(ErrorData::new(
        Severity::Error,
        codes::data::INVALID_TEXT_REPRESENTATION,
        format!(
            "invalid input syntax for type {}: \"{}\"",
            datatype.as_ref(),
            value.as_ref()
        ),
    ))
}

/// NULL value not allowed.
pub fn null_not_allowed(context: impl Into<Cow<'static, str>>) -> ParoError {
    ParoError::new(ErrorData::new(
        Severity::Error,
        codes::data::NULL_VALUE_NOT_ALLOWED,
        context,
    ))
}

/// Type cast/conversion failed.
pub fn cannot_cast(from: impl AsRef<str>, to: impl AsRef<str>) -> ParoError {
    ParoError::new(ErrorData::new(
        Severity::Error,
        codes::data::INVALID_CHARACTER_VALUE_FOR_CAST,
        format!("cannot cast type {} to {}", from.as_ref(), to.as_ref()),
    ))
}

/// Invalid datetime format.
pub fn invalid_datetime(value: impl AsRef<str>) -> ParoError {
    ParoError::new(ErrorData::new(
        Severity::Error,
        codes::data::INVALID_DATETIME_FORMAT,
        format!(
            "invalid input syntax for type timestamp: \"{}\"",
            value.as_ref()
        ),
    ))
}

/// String data too long.
pub fn string_too_long(max_length: usize) -> ParoError {
    ParoError::new(ErrorData::new(
        Severity::Error,
        codes::data::STRING_DATA_RIGHT_TRUNCATION,
        format!("value too long for type character varying({})", max_length),
    ))
}

/// Array subscript error.
pub fn array_subscript_error(message: impl Into<Cow<'static, str>>) -> ParoError {
    ParoError::new(ErrorData::new(
        Severity::Error,
        codes::data::ARRAY_SUBSCRIPT_ERROR,
        message,
    ))
}

/// Invalid regular expression.
pub fn invalid_regex(message: impl Into<Cow<'static, str>>) -> ParoError {
    ParoError::new(ErrorData::new(
        Severity::Error,
        codes::data::INVALID_REGULAR_EXPRESSION,
        message,
    ))
}

/// Invalid parameter value.
pub fn invalid_parameter(message: impl Into<Cow<'static, str>>) -> ParoError {
    ParoError::new(ErrorData::new(
        Severity::Error,
        codes::data::INVALID_PARAMETER_VALUE,
        message,
    ))
}

/// Sequence generator error.
pub fn sequence_generator_error(message: impl Into<Cow<'static, str>>) -> ParoError {
    ParoError::new(ErrorData::new(
        Severity::Error,
        codes::data::SEQUENCE_GENERATOR_ERROR,
        message,
    ))
}

/// Invalid input.
pub fn invalid_input(message: impl Into<Cow<'static, str>>) -> ParoError {
    ParoError::new(ErrorData::new(
        Severity::Error,
        codes::data::INVALID_PARAMETER_VALUE,
        message,
    ))
}
