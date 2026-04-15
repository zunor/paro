// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0
//
// Derived from Databend (https://github.com/datafuselabs/databend),
// Copyright 2021 Datafuse Labs, also licensed under Apache License 2.0.

#![allow(dead_code)]

use std::fmt::Debug;
use std::fmt::Display;
use std::io::Write;

use goldenfile::Mint;
use nom::Parser;
use nom_rule::rule;
use paro_parser::ast::Expr;
use paro_parser::parser_testing::expr::expr;
use paro_parser::parser_testing::match_token;
use paro_parser::parser_testing::token::TokenKind::EOI;
use paro_parser::parser_testing::{display_parser_error, Backtrace, IResult, Input, ParseMode};
use paro_parser::{parse_one_tokens, tokenize_sql};

pub(crate) const GOLDEN_ROOT: &str = "tests/golden";
pub(crate) const STATEMENT_GOLDEN_ROOT: &str = "tests/golden/statement";

pub(crate) fn run_parser<P, O>(file: &mut dyn Write, parser: P, src: &str)
where
    P: FnMut(Input) -> IResult<O>,
    O: Debug + Display,
{
    run_parser_with_mode(file, parser, ParseMode::Default, src)
}

pub(crate) fn run_parser_with_mode<P, O>(
    file: &mut dyn Write,
    parser: P,
    mode: ParseMode,
    src: &str,
) where
    P: FnMut(Input) -> IResult<O>,
    O: Debug + Display,
{
    let src = unindent::unindent(src);
    let src = src.trim();
    let tokens = tokenize_sql(src).unwrap();
    let backtrace = Backtrace::new();
    let input = Input {
        tokens: &tokens,
        mode,
        backtrace: &backtrace,
    };
    let parser = parser;
    let mut parser = rule! { #parser ~ &EOI };
    match parser.parse(input) {
        Ok((input, (output, _))) => {
            assert_eq!(
                input[0].kind,
                paro_parser::parser_testing::token::TokenKind::EOI
            );
            writeln!(file, "---------- Input ----------").unwrap();
            writeln!(file, "{src}").unwrap();
            writeln!(file, "---------- Output ---------").unwrap();
            writeln!(file, "{output}").unwrap();
            writeln!(file, "---------- AST ------------").unwrap();
            writeln!(file, "{output:#?}").unwrap();
            writeln!(file, "\n").unwrap();
        }
        Err(nom::Err::Error(err) | nom::Err::Failure(err)) => {
            let report = display_parser_error(err, src).trim_end().to_string();
            writeln!(file, "---------- Input ----------").unwrap();
            writeln!(file, "{src}").unwrap();
            writeln!(file, "---------- Output ---------").unwrap();
            writeln!(file, "{report}").unwrap();
            writeln!(file, "\n").unwrap();
        }
        Err(nom::Err::Incomplete(_)) => unreachable!(),
    }
}

pub(crate) fn parse_expr_case(src: &str) -> Expr {
    let tokens = tokenize_sql(src).unwrap();
    let backtrace = Backtrace::new();
    let input = Input {
        tokens: &tokens,
        mode: ParseMode::Default,
        backtrace: &backtrace,
    };
    let mut parser = rule! { #expr ~ &EOI };
    let (_, (output, _)) = parser.parse(input).expect("expression should parse");
    output
}

pub(crate) fn check_parser<P, O>(parser: P, src: &str, expected: &str)
where
    P: FnMut(Input) -> IResult<O>,
    O: Debug + Display,
{
    let src = unindent::unindent(src);
    let src = src.trim();
    let tokens = tokenize_sql(src).unwrap();
    let backtrace = Backtrace::new();
    let input = Input {
        tokens: &tokens,
        mode: ParseMode::Default,
        backtrace: &backtrace,
    };
    let parser = parser;
    let mut parser = rule! { #parser ~ &EOI };
    let (_, (output, _)) = parser.parse(input).expect("parser should succeed");
    assert_eq!(output.to_string(), expected);
}

pub(crate) fn write_statement_cases(snapshot_name: &str, cases: &[&str]) {
    let mut mint = Mint::new(STATEMENT_GOLDEN_ROOT);
    let file = &mut mint.new_goldenfile(snapshot_name).unwrap();

    for case in cases {
        let src = unindent::unindent(case);
        let src = src.trim();
        let tokens = tokenize_sql(src).unwrap();
        let stmt = parse_one_tokens(&tokens).unwrap();
        writeln!(file, "---------- Input ----------").unwrap();
        writeln!(file, "{src}").unwrap();
        writeln!(file, "---------- Output ---------").unwrap();
        writeln!(file, "{}", &stmt.stmt).unwrap();
        writeln!(file, "---------- AST ------------").unwrap();
        writeln!(file, "{:#?}", &stmt.stmt).unwrap();
        writeln!(file, "\n").unwrap();
        if let Some(format) = stmt.format {
            writeln!(file, "---------- FORMAT ------------").unwrap();
            writeln!(file, "{format:#?}").unwrap();
        }
    }
}

pub(crate) fn write_statement_error_cases(snapshot_name: &str, cases: &[&str]) {
    let mut mint = Mint::new(STATEMENT_GOLDEN_ROOT);
    let file = &mut mint.new_goldenfile(snapshot_name).unwrap();

    for case in cases {
        let case = unindent::unindent(case);
        let case = case.trim();
        let tokens = tokenize_sql(case).unwrap();
        let err = parse_one_tokens(&tokens).unwrap_err();
        writeln!(file, "---------- Input ----------").unwrap();
        writeln!(file, "{case}").unwrap();
        writeln!(file, "---------- Output ---------").unwrap();
        writeln!(file, "{}", err.message).unwrap();
    }
}
