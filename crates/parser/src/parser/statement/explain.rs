// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0
//
// Derived from Databend (https://github.com/datafuselabs/databend),
// Copyright 2021 Datafuse Labs, also licensed under Apache License 2.0.

use nom::Parser;
use nom_rule::rule;

use crate::ast::*;
use crate::parser::common::*;
use crate::parser::input::Input;
use crate::parser::statement::dispatch::statement_body;
use crate::parser::token::Token;
use crate::parser::token::TokenKind;
use crate::parser::token::TokenKind::*;
use crate::span::merge_span;

pub(crate) fn explain(i: Input) -> IResult<Statement> {
    map_res(
        rule! {
            EXPLAIN ~ ( "(" ~ #comma_separated_list1(explain_option) ~ ")" )? ~ ( AST | SYNTAX | PIPELINE | JOIN | GRAPH | FRAGMENTS | RAW | OPTIMIZED | MEMO | DECORRELATED | PERF)? ~ #statement_body
        },
        |(_, options, opt_kind, statement)| {
            Ok(Statement::Explain {
                kind: match opt_kind.map(|token| token.kind) {
                    Some(TokenKind::SYNTAX) | Some(TokenKind::AST) => {
                        ExplainKind::Syntax(statement.to_string())
                    }
                    Some(TokenKind::PIPELINE) => ExplainKind::Pipeline,
                    Some(TokenKind::JOIN) => ExplainKind::Join,
                    Some(TokenKind::GRAPH) => ExplainKind::Graph,
                    Some(TokenKind::FRAGMENTS) => ExplainKind::Fragments,
                    Some(TokenKind::RAW) => ExplainKind::Raw,
                    Some(TokenKind::OPTIMIZED) => ExplainKind::Optimized,
                    Some(TokenKind::DECORRELATED) => ExplainKind::Decorrelated,
                    Some(TokenKind::MEMO) => ExplainKind::Memo("".to_string()),
                    Some(TokenKind::GRAPHICAL) => ExplainKind::Graphical,
                    Some(TokenKind::PERF) => ExplainKind::Perf,
                    None => ExplainKind::Plan,
                    _ => unreachable!(),
                },
                options: options
                    .map(|(a, opts, b)| (merge_span(Some(a.span), Some(b.span)), opts))
                    .unwrap_or_default(),
                query: Box::new(statement),
            })
        },
    )
    .parse(i)
}

pub(crate) fn explain_analyze(i: Input) -> IResult<Statement> {
    map(
        rule! {
            EXPLAIN ~ ANALYZE ~ (PARTIAL|GRAPHICAL)? ~ #statement_body
        },
        |(_, _, opt_partial_or_graphical, statement)| {
            let (partial, graphical) = match opt_partial_or_graphical {
                Some(Token {
                    kind: TokenKind::PARTIAL,
                    ..
                }) => (true, false),
                Some(Token {
                    kind: TokenKind::GRAPHICAL,
                    ..
                }) => (false, true),
                _ => (false, false),
            };
            Statement::ExplainAnalyze {
                partial,
                graphical,
                query: Box::new(statement),
            }
        },
    )
    .parse(i)
}

pub(crate) fn explain_option(i: Input) -> IResult<ExplainOption> {
    map(
        rule! {
            VERBOSE | LOGICAL | OPTIMIZED | DECORRELATED
        },
        |opt| match &opt.kind {
            VERBOSE => ExplainOption::Verbose,
            LOGICAL => ExplainOption::Logical,
            OPTIMIZED => ExplainOption::Optimized,
            DECORRELATED => ExplainOption::Decorrelated,
            _ => unreachable!(),
        },
    )
    .parse(i)
}
