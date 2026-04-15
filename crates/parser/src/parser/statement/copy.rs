// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0
//
// Derived from Databend (https://github.com/datafuselabs/databend),
// Copyright 2021 Datafuse Labs, also licensed under Apache License 2.0.

use nom::Parser;
use nom_rule::rule;

use crate::ast::CopyDirection;
use crate::ast::CopyOptionValue;
use crate::ast::CopySource;
use crate::ast::CopyStmt;
use crate::ast::CopyTarget;
use crate::ast::LiteralStringOrVariable;
use crate::ast::Statement;
use crate::ast::Statement::Copy as CopyStatement;
use crate::parser::common::comma_separated_list1;
use crate::parser::common::ident;
use crate::parser::common::IResult;
use crate::parser::common::*;
use crate::parser::expr::expr;
use crate::parser::expr::literal_bool;
use crate::parser::expr::literal_string;
use crate::parser::expr::literal_u64;
use crate::parser::query::query;
use crate::parser::statement::helpers::table_ref;
use crate::parser::token::TokenKind::*;
use crate::parser::ErrorKind;
use crate::parser::Input;

fn keyword_eq_ignore_case(text: &'static str) -> impl FnMut(Input) -> IResult<()> + 'static {
    move |i| match any_token(i) {
        Ok((i2, token)) if token.text().eq_ignore_ascii_case(text) => Ok((i2, ())),
        Ok(_) => Err(nom::Err::Error(crate::parser::Error::from_error_kind(
            i,
            ErrorKind::ExpectText(text),
        ))),
        Err(err) => Err(err),
    }
}

pub fn copy_stmt(i: Input) -> IResult<Statement> {
    alt((copy_query_stmt, copy_table_stmt)).parse(i)
}

fn copy_table_stmt(i: Input) -> IResult<Statement> {
    map(
        rule! {
            COPY
            ~ #table_ref
            ~ ( "(" ~ #comma_separated_list1(ident) ~ ")" )?
            ~ #copy_direction
            ~ #copy_source
            ~ #copy_old_option*
            ~ #copy_new_options?
            ~ ( WHERE ~ ^#expr )?
        },
        |(_, table, columns, direction, source, old_options, new_options, where_clause)| {
            let mut options = old_options;
            if let Some(mut new_options) = new_options {
                options.append(&mut new_options);
            }
            let where_clause = where_clause.map(|(_, expr)| Box::new(expr));
            CopyStatement(CopyStmt {
                target: CopyTarget::Table {
                    name: table,
                    columns: columns.map(|(_, columns, _)| columns),
                },
                direction,
                source,
                options,
                where_clause,
            })
        },
    )
    .parse(i)
}

fn copy_query_stmt(i: Input) -> IResult<Statement> {
    map(
        rule! {
            COPY ~ "(" ~ #query ~ ")" ~ TO ~ #copy_source ~ #copy_old_option* ~ #copy_new_options?
        },
        |(_, _, query, _, _, source, old_options, new_options)| {
            let mut options = old_options;
            if let Some(mut new_options) = new_options {
                options.append(&mut new_options);
            }
            CopyStatement(CopyStmt {
                target: CopyTarget::Query(Box::new(query)),
                direction: CopyDirection::To,
                source,
                options,
                where_clause: None,
            })
        },
    )
    .parse(i)
}

fn copy_direction(i: Input) -> IResult<CopyDirection> {
    alt((
        value(CopyDirection::From, rule! { FROM }),
        value(CopyDirection::To, rule! { TO }),
    ))
    .parse(i)
}

fn copy_source(i: Input) -> IResult<CopySource> {
    alt((
        map(
            rule! { #keyword_eq_ignore_case("PROGRAM") ~ #literal_string },
            |(_, command)| CopySource::Program(command),
        ),
        map(rule! { #keyword_eq_ignore_case("STDIN") }, |_| {
            CopySource::Stdin
        }),
        map(rule! { #keyword_eq_ignore_case("STDOUT") }, |_| {
            CopySource::Stdout
        }),
        map(literal_string, CopySource::File),
    ))
    .parse(i)
}

fn copy_new_options(i: Input) -> IResult<Vec<(String, CopyOptionValue)>> {
    map(
        rule! { WITH ~ "(" ~ #comma_separated_list1(copy_option_kv) ~ ")" },
        |(_, _, options, _)| options,
    )
    .parse(i)
}

fn copy_option_kv(i: Input) -> IResult<(String, CopyOptionValue)> {
    map(
        rule! { #copy_option_key ~ ( "=" )? ~ #copy_option_value },
        |(key, _, value)| (key, value),
    )
    .parse(i)
}

fn copy_option_key(i: Input) -> IResult<String> {
    match any_token(i) {
        Ok((i2, token)) if token.kind == Ident || token.kind.is_keyword() => {
            Ok((i2, token.text().to_lowercase()))
        }
        Ok(_) => Err(nom::Err::Error(crate::parser::Error::from_error_kind(
            i,
            ErrorKind::ExpectToken(Ident),
        ))),
        Err(err) => Err(err),
    }
}

fn copy_option_ident_value(i: Input) -> IResult<String> {
    match any_token(i) {
        Ok((i2, token)) if token.kind == Ident || token.kind.is_keyword() => {
            Ok((i2, token.text().to_string()))
        }
        Ok(_) => Err(nom::Err::Error(crate::parser::Error::from_error_kind(
            i,
            ErrorKind::ExpectToken(Ident),
        ))),
        Err(err) => Err(err),
    }
}

fn copy_option_list_item(i: Input) -> IResult<String> {
    alt((literal_string, copy_option_ident_value)).parse(i)
}

fn copy_option_list(i: Input) -> IResult<Vec<String>> {
    map(
        rule! { "(" ~ #comma_separated_list1(copy_option_list_item) ~ ")" },
        |(_, items, _)| items,
    )
    .parse(i)
}

fn copy_option_value(i: Input) -> IResult<CopyOptionValue> {
    alt((
        value(CopyOptionValue::Star, rule! { "*" }),
        value(CopyOptionValue::Default, rule! { DEFAULT }),
        map(literal_bool, CopyOptionValue::Boolean),
        map(literal_u64, CopyOptionValue::Number),
        map(literal_string, CopyOptionValue::String),
        map(copy_option_list, CopyOptionValue::List),
        map(copy_option_ident_value, CopyOptionValue::String),
    ))
    .parse(i)
}

fn copy_force_columns(i: Input) -> IResult<CopyOptionValue> {
    map(
        rule! { "(" ~ #comma_separated_list1(ident) ~ ")" },
        |(_, columns, _)| {
            CopyOptionValue::List(columns.into_iter().map(|col| col.to_string()).collect())
        },
    )
    .parse(i)
}

fn copy_force_columns_or_star(i: Input) -> IResult<CopyOptionValue> {
    alt((
        value(CopyOptionValue::Star, rule! { "*" }),
        copy_force_columns,
    ))
    .parse(i)
}

fn copy_old_option(i: Input) -> IResult<(String, CopyOptionValue)> {
    alt((
        map(
            rule! { #keyword_eq_ignore_case("DELIMITER") ~ ( AS )? ~ #literal_string },
            |(_, _, value)| ("delimiter".to_string(), CopyOptionValue::String(value)),
        ),
        map(
            rule! { NULL ~ ( AS )? ~ #literal_string },
            |(_, _, value)| ("null".to_string(), CopyOptionValue::String(value)),
        ),
        map(rule! { CSV }, |_| {
            (
                "format".to_string(),
                CopyOptionValue::String("csv".to_string()),
            )
        }),
        map(rule! { #keyword_eq_ignore_case("HEADER") }, |_| {
            ("header".to_string(), CopyOptionValue::Boolean(true))
        }),
        map(
            rule! { QUOTE ~ ( AS )? ~ #literal_string },
            |(_, _, value)| ("quote".to_string(), CopyOptionValue::String(value)),
        ),
        map(
            rule! { ESCAPE ~ ( AS )? ~ #literal_string },
            |(_, _, value)| ("escape".to_string(), CopyOptionValue::String(value)),
        ),
        map(
            rule! { FORCE ~ QUOTE ~ #copy_force_columns_or_star },
            |(_, _, value)| ("force_quote".to_string(), value),
        ),
        map(
            rule! { FORCE ~ NOT ~ NULL ~ #copy_force_columns },
            |(_, _, _, value)| ("force_not_null".to_string(), value),
        ),
        map(
            rule! { FORCE ~ NULL ~ #copy_force_columns },
            |(_, _, value)| ("force_null".to_string(), value),
        ),
        map(
            rule! { #keyword_eq_ignore_case("ENCODING") ~ #literal_string },
            |(_, value)| ("encoding".to_string(), CopyOptionValue::String(value)),
        ),
        map(rule! { BINARY }, |_| {
            (
                "format".to_string(),
                CopyOptionValue::String("binary".to_string()),
            )
        }),
        map(rule! { #keyword_eq_ignore_case("FREEZE") }, |_| {
            ("freeze".to_string(), CopyOptionValue::Boolean(true))
        }),
    ))
    .parse(i)
}

pub fn literal_string_or_variable(i: Input) -> IResult<LiteralStringOrVariable> {
    alt((
        map(literal_string, LiteralStringOrVariable::Literal),
        map(variable_ident, LiteralStringOrVariable::Variable),
    ))
    .parse(i)
}
