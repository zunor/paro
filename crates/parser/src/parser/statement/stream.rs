// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0
//
// Derived from Databend (https://github.com/datafuselabs/databend),
// Copyright 2021 Datafuse Labs, also licensed under Apache License 2.0.

use nom::Parser;
use nom_rule::rule;

use crate::ast::CreateStreamStmt;
use crate::ast::DescribeStreamStmt;
use crate::ast::DropStreamStmt;
use crate::ast::ShowStreamsStmt;
use crate::ast::Statement;
use crate::parser::common::dot_separated_idents_1_to_2;
use crate::parser::common::dot_separated_idents_1_to_3;
use crate::parser::common::map_res;
use crate::parser::common::IResult;
use crate::parser::common::*;
use crate::parser::expr::literal_bool;
use crate::parser::expr::literal_string;
use crate::parser::query::travel_point;
use crate::parser::statement::helpers::parse_create_option;
use crate::parser::statement::helpers::show_limit;
use crate::parser::token::TokenKind::*;
use crate::parser::Input;

pub fn create_stream(i: Input) -> IResult<Statement> {
    map_res(
        rule! {
            CREATE ~ ( OR ~ ^REPLACE )? ~ STREAM ~ ( IF ~ ^NOT ~ ^EXISTS )?
            ~ #dot_separated_idents_1_to_3
            ~ ON ~ TABLE ~ #dot_separated_idents_1_to_2
            ~ ( AT ~ ^#travel_point )?
            ~ ( APPEND_ONLY ~ "=" ~ #literal_bool )?
            ~ ( COMMENT ~ "=" ~ #literal_string )?
        },
        |(
            _,
            opt_or_replace,
            _,
            opt_if_not_exists,
            (catalog, database, stream),
            _,
            _,
            (table_database, table),
            opt_travel_point,
            opt_append_only,
            opt_comment,
        )| {
            let create_option =
                parse_create_option(opt_or_replace.is_some(), opt_if_not_exists.is_some())?;
            Ok(Statement::CreateStream(CreateStreamStmt {
                create_option,
                database: catalog,
                schema: database,
                stream,
                table_schema: table_database,
                table,
                travel_point: opt_travel_point.map(|p| p.1),
                append_only: opt_append_only
                    .map(|(_, _, append_only)| append_only)
                    .unwrap_or(true),
                comment: opt_comment.map(|(_, _, comment)| comment),
            }))
        },
    )(i)
}

pub fn drop_stream(i: Input) -> IResult<Statement> {
    map(
        rule! {
            DROP ~ STREAM ~ ( IF ~ ^EXISTS )? ~ #dot_separated_idents_1_to_3
        },
        |(_, _, opt_if_exists, (catalog, database, stream))| {
            Statement::DropStream(DropStreamStmt {
                if_exists: opt_if_exists.is_some(),
                database: catalog,
                schema: database,
                stream,
            })
        },
    )
    .parse(i)
}

pub fn show_streams(i: Input) -> IResult<Statement> {
    map(
        rule! {
            SHOW ~ FULL? ~ STREAMS ~ ( ( FROM | IN ) ~ #dot_separated_idents_1_to_2 )? ~ #show_limit?
        },
        |(_, opt_full, _, ctl_db, limit)| {
            let (catalog, database) = match ctl_db {
                Some((_, (Some(c), d))) => (Some(c), Some(d)),
                Some((_, (None, d))) => (None, Some(d)),
                _ => (None, None),
            };
            Statement::ShowStreams(ShowStreamsStmt {
                database: catalog,
                schema: database,
                full: opt_full.is_some(),
                limit,
            })
        },
    ).parse(i)
}

pub fn describe_stream(i: Input) -> IResult<Statement> {
    map(
        rule! {
            ( DESC | DESCRIBE ) ~ STREAM ~ #dot_separated_idents_1_to_3
        },
        |(_, _, (catalog, database, stream))| {
            Statement::DescribeStream(DescribeStreamStmt {
                database: catalog,
                schema: database,
                stream,
            })
        },
    )
    .parse(i)
}
