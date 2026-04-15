// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0
//
// Derived from Databend (https://github.com/datafuselabs/databend),
// Copyright 2021 Datafuse Labs, also licensed under Apache License 2.0.

use crate::ast::ExplainKind;
use crate::ast::Expr;
use crate::ast::Literal;
use crate::ast::SelectTarget;
use crate::ast::Statement;
use crate::ast::StatementWithFormat;
use crate::parser::common::maybe_grow_parser_stack;
use crate::parser::common::IResult;
use crate::parser::error::display_parser_error;
use crate::parser::error::Error;
use crate::parser::expr::expr;
use crate::parser::input::Input;
use crate::parser::input::ParseMode;
use crate::parser::statement::dml::insert_stmt;
use crate::parser::statement::dml::replace_stmt;
use crate::parser::statement::statement;
use crate::parser::statement::statement_body_with_format;
use crate::parser::token::Token;
use crate::parser::token::TokenKind;
use crate::parser::token::Tokenizer;
use crate::parser::Backtrace;
use crate::ParseError;
use crate::Range;
use crate::Result;
use derive_visitor::DriveMut;
use derive_visitor::VisitorMut;
use nom::Offset;
#[cfg(debug_assertions)]
use pretty_assertions::assert_eq;

#[derive(Debug, Clone, Copy)]
pub(crate) struct ParseOptions {
    pub mode: ParseMode,
    pub allow_partial: bool,
}

impl ParseOptions {
    pub const fn new(mode: ParseMode) -> Self {
        Self {
            mode,
            allow_partial: false,
        }
    }
}

impl Default for ParseOptions {
    fn default() -> Self {
        Self::new(ParseMode::Default)
    }
}

pub fn tokenize_sql(sql: &str) -> Result<Vec<Token<'_>>> {
    Tokenizer::new(sql).collect::<Result<Vec<_>>>()
}

/// Parse a SQL string into a single `Statement`.
#[fastrace::trace]
pub fn parse_one_tokens(tokens: &[Token]) -> Result<StatementWithFormat> {
    let stmt = run_parser(tokens, ParseOptions::default(), statement)?;

    #[cfg(debug_assertions)]
    maybe_grow_parser_stack(|| {
        assert_reparse(tokens[0].source, stmt.clone());
    });

    Ok(stmt)
}

/// Parse a SQL token slice into multiple statements.
pub fn parse_tokens(tokens: &[Token]) -> Result<Vec<StatementWithFormat>> {
    let mut cursor = 0;
    let mut result = Vec::new();
    let backtrace = Backtrace::new();

    loop {
        while cursor < tokens.len() && tokens[cursor].kind == TokenKind::SemiColon {
            cursor += 1;
        }

        if cursor >= tokens.len() || tokens[cursor].kind == TokenKind::EOI {
            break;
        }

        backtrace.clear();

        let input = Input {
            tokens: &tokens[cursor..],
            mode: ParseMode::Default,
            backtrace: &backtrace,
        };

        match maybe_grow_parser_stack(|| statement_body_with_format(input)) {
            Ok((rest, stmt)) => {
                cursor += input.offset(&rest);
                result.push(stmt);
            }
            Err(nom::Err::Error(err) | nom::Err::Failure(err)) => {
                let source = tokens.first().map(|t| t.source).unwrap_or("");
                return Err(ParseError::without_span(display_parser_error(err, source)));
            }
            Err(nom::Err::Incomplete(_)) => unreachable!(),
        }
    }

    Ok(result)
}

/// Parse udf function into Expr
pub fn parse_expr_tokens(tokens: &[Token]) -> Result<Expr> {
    run_parser(tokens, ParseOptions::default(), expr)
}

#[allow(dead_code)]
pub(crate) fn parse_insert_partial(tokens: &[Token]) -> Result<Statement> {
    run_parser(tokens, ParseOptions::default(), insert_stmt(true, false))
}

#[allow(dead_code)]
pub(crate) fn parse_insert_streaming(tokens: &[Token]) -> Result<Statement> {
    run_parser(tokens, ParseOptions::default(), insert_stmt(true, true))
}

#[allow(dead_code)]
pub(crate) fn parse_replace_partial(tokens: &[Token]) -> Result<Statement> {
    run_parser(tokens, ParseOptions::default(), replace_stmt(true))
}

pub(crate) fn run_parser<O>(
    tokens: &[Token],
    options: ParseOptions,
    mut parser: impl FnMut(Input) -> IResult<O>,
) -> Result<O> {
    let backtrace = Backtrace::new();
    let input = Input {
        tokens,
        mode: options.mode,
        backtrace: &backtrace,
    };
    maybe_grow_parser_stack(|| match parser(input) {
        Ok((rest, res)) => {
            let is_complete = rest.is_empty() || rest[0].kind == TokenKind::EOI;
            if is_complete || options.allow_partial {
                Ok(res)
            } else {
                let source = tokens[0].source;
                let err = Error::from_error_kind(
                    rest,
                    crate::parser::ErrorKind::Other("unable to parse rest of the sql"),
                );
                Err(ParseError::without_span(display_parser_error(err, source)))
            }
        }
        Err(nom::Err::Error(err) | nom::Err::Failure(err)) => {
            let source = tokens[0].source;
            Err(ParseError::without_span(display_parser_error(err, source)))
        }
        Err(nom::Err::Incomplete(_)) => unreachable!(),
    })
}

/// Check that the statement can be displayed and reparsed without loss
#[allow(dead_code)]
fn assert_reparse(sql: &str, stmt: crate::ast::StatementWithFormat) {
    let stmt = reset_ast(stmt);

    let new_sql = stmt.to_string();
    let new_tokens = tokenize_sql(&new_sql).unwrap();
    let new_stmt = run_parser(&new_tokens, ParseOptions::default(), statement)
        .map_err(|err| panic!("{} in {}", err.message, new_sql))
        .unwrap();

    let new_stmt = reset_ast(new_stmt);
    assert_eq!(stmt, new_stmt, "\nleft:\n{}\nright:\n{}", sql, new_sql);
}

#[allow(dead_code)]
fn reset_ast(mut stmt: StatementWithFormat) -> StatementWithFormat {
    #[derive(VisitorMut)]
    #[visitor(Range(enter), Literal(enter), ExplainKind(enter), SelectTarget(enter))]
    struct ResetAST;

    impl ResetAST {
        fn enter_range(&mut self, range: &mut Range) {
            range.start = 0;
            range.end = 0;
        }

        fn enter_literal(&mut self, literal: &mut Literal) {
            *literal = Literal::Null;
        }

        fn enter_explain_kind(&mut self, kind: &mut ExplainKind) {
            match kind {
                ExplainKind::Ast(_) => *kind = ExplainKind::Ast("".to_string()),
                ExplainKind::Syntax(_) => *kind = ExplainKind::Syntax("".to_string()),
                ExplainKind::Memo(_) => *kind = ExplainKind::Memo("".to_string()),
                _ => (),
            }
        }

        fn enter_select_target(&mut self, target: &mut SelectTarget) {
            if let SelectTarget::StarColumns { column_filter, .. } = target {
                *column_filter = None
            }
        }
    }

    stmt.drive_mut(&mut ResetAST);

    stmt
}
