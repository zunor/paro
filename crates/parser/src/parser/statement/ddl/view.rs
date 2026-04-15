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
use crate::parser::query::query;
use crate::parser::statement::helpers::{parse_create_option, show_limit};
use crate::parser::token::TokenKind::*;

pub(crate) fn create_view(i: Input) -> IResult<Statement> {
    map_res(
        rule! {
            CREATE ~ ( OR ~ ^REPLACE )? ~ VIEW ~ ( IF ~ ^NOT ~ ^EXISTS )?
            ~ #dot_separated_idents_1_to_3
            ~ ( "(" ~ #comma_separated_list1(ident) ~ ")" )?
            ~ AS ~ #query
        },
        |(
            _,
            opt_or_replace,
            _,
            opt_if_not_exists,
            (catalog, database, view),
            opt_columns,
            _,
            query,
        )| {
            let create_option =
                parse_create_option(opt_or_replace.is_some(), opt_if_not_exists.is_some())?;
            Ok(Statement::CreateView(CreateViewStmt {
                create_option,
                database: catalog,
                schema: database,
                view,
                columns: opt_columns
                    .map(|(_, columns, _)| columns)
                    .unwrap_or_default(),
                query: Box::new(query),
            }))
        },
    )
    .parse(i)
}

pub(crate) fn drop_view(i: Input) -> IResult<Statement> {
    map(
        rule! {
            DROP ~ VIEW ~ ( IF ~ ^EXISTS )? ~ #dot_separated_idents_1_to_3
        },
        |(_, _, opt_if_exists, (catalog, database, view))| {
            Statement::DropView(DropViewStmt {
                if_exists: opt_if_exists.is_some(),
                database: catalog,
                schema: database,
                view,
            })
        },
    )
    .parse(i)
}

pub(crate) fn alter_view(i: Input) -> IResult<Statement> {
    map(
        rule! {
            ALTER ~ VIEW
            ~ #dot_separated_idents_1_to_3
            ~ ( "(" ~ #comma_separated_list1(ident) ~ ")" )?
            ~ AS ~ #query
        },
        |(_, _, (catalog, database, view), opt_columns, _, query)| {
            Statement::AlterView(AlterViewStmt {
                database: catalog,
                schema: database,
                view,
                columns: opt_columns
                    .map(|(_, columns, _)| columns)
                    .unwrap_or_default(),
                query: Box::new(query),
            })
        },
    )
    .parse(i)
}

pub(crate) fn show_views(i: Input) -> IResult<Statement> {
    map(
        rule! {
            SHOW ~ FULL? ~ VIEWS ~ HISTORY? ~ ( ( FROM | IN ) ~ #dot_separated_idents_1_to_2 )? ~ #show_limit?
        },
        |(_, opt_full, _, opt_history, ctl_db, limit)| {
            let (catalog, database) = match ctl_db {
                Some((_, (Some(c), d))) => (Some(c), Some(d)),
                Some((_, (None, d))) => (None, Some(d)),
                _ => (None, None),
            };
            Statement::ShowViews(ShowViewsStmt {
                database: catalog,
                schema: database,
                full: opt_full.is_some(),
                limit,
                with_history: opt_history.is_some(),
            })
        },
    )
    .parse(i)
}

pub(crate) fn describe_view(i: Input) -> IResult<Statement> {
    map(
        rule! {
            ( DESC | DESCRIBE ) ~ VIEW ~ #dot_separated_idents_1_to_3
        },
        |(_, _, (catalog, database, view))| {
            Statement::DescribeView(DescribeViewStmt {
                database: catalog,
                schema: database,
                view,
            })
        },
    )
    .parse(i)
}
