// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0
//
// Derived from Databend (https://github.com/datafuselabs/databend),
// Copyright 2021 Datafuse Labs, also licensed under Apache License 2.0.

use std::fmt::Display;
use std::fmt::Formatter;

use paro_common::error::ParoError;

use crate::span::pretty_print_error;
use crate::span::Span;

#[derive(Debug)]
pub struct ParseError {
    pub span: Span,
    pub message: String,
}

impl ParseError {
    pub fn new(span: Span, message: impl Into<String>) -> Self {
        Self {
            span,
            message: message.into(),
        }
    }

    pub fn without_span(message: impl Into<String>) -> Self {
        Self::new(None, message)
    }

    /// Pretty display the error message onto source if span is available.
    pub fn display_with_source(mut self, source: &str) -> Self {
        if let Some(span) = self.span.take() {
            self.message = pretty_print_error(source, vec![(span, self.message.clone())]);
        }
        self
    }
}

impl Display for ParseError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl From<ParseError> for ParoError {
    fn from(err: ParseError) -> Self {
        paro_common::error::from_parser(err.message)
    }
}
