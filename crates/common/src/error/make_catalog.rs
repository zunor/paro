//! Catalog object error constructors.

use crate::error::{codes, ErrorData, ParoError, Severity};
use std::borrow::Cow;

// ========== Object not found ==========

/// Table/relation not found.
pub fn table_not_found(name: impl AsRef<str>) -> ParoError {
    let name = name.as_ref();
    ParoError::new(
        ErrorData::new(
            Severity::Error,
            codes::syntax::UNDEFINED_TABLE,
            format!("relation \"{}\" does not exist", name),
        )
        .table(name.to_string()),
    )
}

/// Column not found.
pub fn column_not_found(name: impl AsRef<str>) -> ParoError {
    let name = name.as_ref();
    ParoError::new(
        ErrorData::new(
            Severity::Error,
            codes::syntax::UNDEFINED_COLUMN,
            format!("column \"{}\" does not exist", name),
        )
        .column(name.to_string()),
    )
}

/// Function not found.
pub fn function_not_found(signature: impl Into<Cow<'static, str>>) -> ParoError {
    let signature = signature.into();
    ParoError::new(ErrorData::new(
        Severity::Error,
        codes::syntax::UNDEFINED_FUNCTION,
        format!("function {} does not exist", signature),
    ))
}

/// Schema not found.
pub fn schema_not_found(name: impl AsRef<str>) -> ParoError {
    let name = name.as_ref();
    ParoError::new(
        ErrorData::new(
            Severity::Error,
            codes::syntax::UNDEFINED_SCHEMA,
            format!("schema \"{}\" does not exist", name),
        )
        .schema(name.to_string()),
    )
}

/// Database not found.
pub fn database_not_found(name: impl AsRef<str>) -> ParoError {
    let name = name.as_ref();
    ParoError::new(ErrorData::new(
        Severity::Error,
        codes::syntax::UNDEFINED_DATABASE,
        format!("database \"{}\" does not exist", name),
    ))
}

/// Object not found (generic).
pub fn object_not_found(kind: impl AsRef<str>, name: impl AsRef<str>) -> ParoError {
    ParoError::new(ErrorData::new(
        Severity::Error,
        codes::syntax::UNDEFINED_OBJECT,
        format!("{} \"{}\" does not exist", kind.as_ref(), name.as_ref()),
    ))
}

// ========== Object already exists ==========

/// Table/relation already exists.
pub fn table_exists(name: impl AsRef<str>) -> ParoError {
    let name = name.as_ref();
    ParoError::new(
        ErrorData::new(
            Severity::Error,
            codes::syntax::DUPLICATE_TABLE,
            format!("relation \"{}\" already exists", name),
        )
        .table(name.to_string()),
    )
}

/// Column already exists.
pub fn column_exists(name: impl AsRef<str>) -> ParoError {
    let name = name.as_ref();
    ParoError::new(
        ErrorData::new(
            Severity::Error,
            codes::syntax::DUPLICATE_COLUMN,
            format!("column \"{}\" already exists", name),
        )
        .column(name.to_string()),
    )
}

/// Schema already exists.
pub fn schema_exists(name: impl AsRef<str>) -> ParoError {
    let name = name.as_ref();
    ParoError::new(
        ErrorData::new(
            Severity::Error,
            codes::syntax::DUPLICATE_SCHEMA,
            format!("schema \"{}\" already exists", name),
        )
        .schema(name.to_string()),
    )
}

/// Database already exists.
pub fn database_exists(name: impl AsRef<str>) -> ParoError {
    let name = name.as_ref();
    ParoError::new(ErrorData::new(
        Severity::Error,
        codes::syntax::DUPLICATE_DATABASE,
        format!("database \"{}\" already exists", name),
    ))
}

/// Object already exists (generic).
pub fn object_exists(kind: impl AsRef<str>, name: impl AsRef<str>) -> ParoError {
    ParoError::new(ErrorData::new(
        Severity::Error,
        codes::syntax::DUPLICATE_OBJECT,
        format!("{} \"{}\" already exists", kind.as_ref(), name.as_ref()),
    ))
}

// ========== Ambiguous references ==========

/// Ambiguous column reference.
pub fn ambiguous_column(name: impl AsRef<str>) -> ParoError {
    let name = name.as_ref();
    ParoError::new(
        ErrorData::new(
            Severity::Error,
            codes::syntax::AMBIGUOUS_COLUMN,
            format!("column reference \"{}\" is ambiguous", name),
        )
        .column(name.to_string()),
    )
}

/// Ambiguous function call.
pub fn ambiguous_function(signature: impl Into<Cow<'static, str>>) -> ParoError {
    let signature = signature.into();
    ParoError::new(ErrorData::new(
        Severity::Error,
        codes::syntax::AMBIGUOUS_FUNCTION,
        format!("function {} is not unique", signature),
    ))
}

/// Wrong object type.
pub fn wrong_object_type(expected: impl AsRef<str>, got: impl AsRef<str>) -> ParoError {
    ParoError::new(ErrorData::new(
        Severity::Error,
        codes::syntax::WRONG_OBJECT_TYPE,
        format!("\"{}\" is not a {}", got.as_ref(), expected.as_ref()),
    ))
}

/// Generic catalog error.
pub fn catalog(message: impl Into<Cow<'static, str>>) -> ParoError {
    ParoError::new(ErrorData::new(
        Severity::Error,
        codes::syntax::SYNTAX_ERROR,
        message,
    ))
}
