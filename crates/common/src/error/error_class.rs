// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Error class enumeration for matching SQLSTATE categories.
//!
//! This provides a Rust-friendly way to match error categories using pattern matching,
//! following PostgreSQL's SQLSTATE class conventions.

/// Error class based on SQLSTATE's first two characters (the error class).
///
/// This enum enables Rust pattern matching on error categories, similar to
/// PostgreSQL's `ERRCODE_TO_CATEGORY` macro.
///
/// # Example
///
/// ```ignore
/// use paro_common::error::{ParoError, ErrorClass};
///
/// fn handle_error(err: &ParoError) {
///     match err.error_class() {
///         ErrorClass::Syntax => { /* handle syntax error */ }
///         ErrorClass::Constraint => { /* handle constraint violation */ }
///         ErrorClass::Internal => { /* escalate internal error */ }
///         _ => { /* fallback handling */ }
///     }
/// }
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ErrorClass {
    /// Class 00 - Successful Completion
    Success,
    /// Class 01 - Warning
    Warning,
    /// Class 02 - No Data
    NoData,
    /// Class 08 - Connection Exception
    Connection,
    /// Class 0A - Feature Not Supported
    FeatureNotSupported,
    /// Class 22 - Data Exception
    Data,
    /// Class 23 - Integrity Constraint Violation
    Constraint,
    /// Class 25 - Invalid Transaction State
    Transaction,
    /// Class 3D - Invalid Catalog Name
    InvalidCatalogName,
    /// Class 3F - Invalid Schema Name
    InvalidSchemaName,
    /// Class 40 - Transaction Rollback
    TransactionRollback,
    /// Class 42 - Syntax Error or Access Rule Violation
    Syntax,
    /// Class 53 - Insufficient Resources
    Resource,
    /// Class 54 - Program Limit Exceeded
    ProgramLimit,
    /// Class 55 - Object Not In Prerequisite State
    ObjectState,
    /// Class 57 - Operator Intervention
    Operator,
    /// Class 58 - System Error
    System,
    /// Class XX - Internal Error
    Internal,
    /// Unknown or unrecognized error class
    Other,
}

impl ErrorClass {
    /// Create an ErrorClass from a 2-character class code string.
    #[inline]
    pub fn from_class_code(class: &str) -> Self {
        match class {
            "00" => Self::Success,
            "01" => Self::Warning,
            "02" => Self::NoData,
            "08" => Self::Connection,
            "0A" => Self::FeatureNotSupported,
            "22" => Self::Data,
            "23" => Self::Constraint,
            "25" => Self::Transaction,
            "3D" => Self::InvalidCatalogName,
            "3F" => Self::InvalidSchemaName,
            "40" => Self::TransactionRollback,
            "42" => Self::Syntax,
            "53" => Self::Resource,
            "54" => Self::ProgramLimit,
            "55" => Self::ObjectState,
            "57" => Self::Operator,
            "58" => Self::System,
            "XX" => Self::Internal,
            _ => Self::Other,
        }
    }

    /// Returns the class code as a string (e.g., "42" for Syntax).
    #[inline]
    pub fn as_code(&self) -> &'static str {
        match self {
            Self::Success => "00",
            Self::Warning => "01",
            Self::NoData => "02",
            Self::Connection => "08",
            Self::FeatureNotSupported => "0A",
            Self::Data => "22",
            Self::Constraint => "23",
            Self::Transaction => "25",
            Self::InvalidCatalogName => "3D",
            Self::InvalidSchemaName => "3F",
            Self::TransactionRollback => "40",
            Self::Syntax => "42",
            Self::Resource => "53",
            Self::ProgramLimit => "54",
            Self::ObjectState => "55",
            Self::Operator => "57",
            Self::System => "58",
            Self::Internal => "XX",
            Self::Other => "??",
        }
    }

    /// Returns a human-readable description of the error class.
    #[inline]
    pub fn description(&self) -> &'static str {
        match self {
            Self::Success => "Successful Completion",
            Self::Warning => "Warning",
            Self::NoData => "No Data",
            Self::Connection => "Connection Exception",
            Self::FeatureNotSupported => "Feature Not Supported",
            Self::Data => "Data Exception",
            Self::Constraint => "Integrity Constraint Violation",
            Self::Transaction => "Invalid Transaction State",
            Self::InvalidCatalogName => "Invalid Catalog Name",
            Self::InvalidSchemaName => "Invalid Schema Name",
            Self::TransactionRollback => "Transaction Rollback",
            Self::Syntax => "Syntax Error or Access Rule Violation",
            Self::Resource => "Insufficient Resources",
            Self::ProgramLimit => "Program Limit Exceeded",
            Self::ObjectState => "Object Not In Prerequisite State",
            Self::Operator => "Operator Intervention",
            Self::System => "System Error",
            Self::Internal => "Internal Error",
            Self::Other => "Unknown Error",
        }
    }
}

impl std::fmt::Display for ErrorClass {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.description())
    }
}
