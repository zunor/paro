// Copyright 2024-2026 Zunor
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Derived from Databend (https://github.com/datafuselabs/databend),
// Copyright 2021 Datafuse Labs, also licensed under Apache License 2.0.

//! # Paro SQL Parser
//!
//! SQL parser for Paro with a PostgreSQL-first public API.
//!
//! ## Features
//! - PostgreSQL dialect by default
//! - Uses `logos` for lexical analysis
//! - Uses `nom` + `pratt` for parsing
//!
//! ## Example
//!
//! ```rust
//! use paro_parser::parse_one;
//!
//! let sql = "SELECT a, b FROM table_1 WHERE a > 10";
//! let statement = parse_one(sql).unwrap();
//! assert!(matches!(statement.stmt, paro_parser::ast::Statement::Query(..)));
//! ```

#[allow(clippy::collapsible_match)]
pub mod ast;
pub(crate) mod parser;
mod parser_error;
pub mod span;
mod visitor;

pub use ast::{Expr, Query, Statement, StatementWithFormat};
pub use parser::entry::{parse_expr_tokens, parse_one_tokens, parse_tokens, tokenize_sql};
pub use parser::token::Token;
pub use parser_error::ParseError;
pub use span::{Range, Span};
pub use visitor::{ExprRewriter, StatementReplacer, StatementVisitor};

pub type Result<T> = std::result::Result<T, ParseError>;

/// Parse a SQL string into multiple statements.
pub fn parse(sql: &str) -> Result<Vec<StatementWithFormat>> {
    let tokens = tokenize_sql(sql)?;
    parse_tokens(&tokens)
}

/// Parse a SQL string into a single statement.
pub fn parse_one(sql: &str) -> Result<StatementWithFormat> {
    let tokens = tokenize_sql(sql)?;
    parse_one_tokens(&tokens)
}

/// Parse a SQL string into a single expression.
pub fn parse_expr(sql: &str) -> Result<Expr> {
    let tokens = tokenize_sql(sql)?;
    parse_expr_tokens(&tokens)
}

#[cfg(feature = "internal-testing")]
pub mod parser_testing {
    pub mod entry {
        use crate::ast::Statement;
        use crate::parser::entry::parse_insert_partial as parse_insert_partial_impl;
        use crate::parser::token::Token;
        use crate::Result;

        pub use crate::parser::entry::{parse_expr_tokens, parse_one_tokens, parse_tokens};

        pub fn parse_insert_partial(tokens: &[Token]) -> Result<Statement> {
            parse_insert_partial_impl(tokens)
        }
    }

    pub mod expr {
        pub use crate::parser::expr::*;
    }

    pub mod query {
        pub use crate::parser::query::*;
    }

    pub mod script {
        pub use crate::parser::script::*;
    }

    pub mod statement {
        pub use crate::parser::statement::dispatch::statement_body;
    }

    pub mod token {
        pub use crate::parser::token::*;
    }

    pub use crate::parser::common::{match_text, match_token, IResult};
    #[cfg(debug_assertions)]
    pub use crate::parser::common::{
        parser_stack_stats_snapshot, reset_parser_stack_stats, ParserStackStats,
    };
    pub use crate::parser::error::{display_parser_error, Backtrace, Error, ErrorKind};
    pub use crate::parser::input::{Input, ParseMode};
    pub use crate::parser::token::all_reserved_keywords;
}
