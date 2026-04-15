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

use nom::Parser;
use nom_rule::rule;

use crate::ast::*;
use crate::parser::common::*;
use crate::parser::expr::expr;
use crate::parser::expr::literal_u64;
use crate::parser::input::Input;
use crate::parser::query::query;
use crate::parser::statement::helpers::{parse_create_option, show_options, table_option};
use crate::parser::token::TokenKind::*;

pub(crate) fn create_aggregating_index(i: Input) -> IResult<Statement> {
    map_res(
        rule! {
            CREATE
            ~ ( OR ~ ^REPLACE )?
            ~ ASYNC?
            ~ AGGREGATING ~ INDEX
            ~ ( IF ~ ^NOT ~ ^EXISTS )?
            ~ #ident
            ~ AS ~ #query
        },
        |(_, opt_or_replace, opt_async, _, _, opt_if_not_exists, index_name, _, query)| {
            let create_option =
                parse_create_option(opt_or_replace.is_some(), opt_if_not_exists.is_some())?;
            Ok(Statement::CreateAggregatingIndex(
                CreateAggregatingIndexStmt {
                    index_kind: IndexKind::Aggregating,
                    create_option,
                    index_name,
                    query: Box::new(query),
                    sync_creation: opt_async.is_none(),
                },
            ))
        },
    )
    .parse(i)
}

pub(crate) fn drop_index(i: Input) -> IResult<Statement> {
    map(
        rule! {
            DROP ~ INDEX ~ ( IF ~ ^EXISTS )? ~ #dot_separated_idents_1_to_3
        },
        |(_, _, opt_if_exists, (database, schema, index))| {
            Statement::DropIndex(DropIndexStmt {
                if_exists: opt_if_exists.is_some(),
                database,
                schema,
                index,
            })
        },
    )
    .parse(i)
}

pub(crate) fn refresh_aggregating_index(i: Input) -> IResult<Statement> {
    map(
        rule! {
            REFRESH ~ AGGREGATING ~ INDEX ~ #ident ~ ( LIMIT ~ #literal_u64 )?
        },
        |(_, _, _, index, opt_limit)| {
            Statement::RefreshAggregatingIndex(RefreshAggregatingIndexStmt {
                index,
                limit: opt_limit.map(|(_, limit)| limit),
            })
        },
    )
    .parse(i)
}

pub(crate) fn create_index_using(i: Input) -> IResult<Statement> {
    map_res(
        rule! {
            CREATE
            ~ ( OR ~ ^REPLACE )?
            ~ ASYNC?
            ~ INDEX
            ~ ( IF ~ ^NOT ~ ^EXISTS )?
            ~ #ident
            ~ ON ~ #dot_separated_idents_1_to_3
            ~ USING ~ #ident
            ~ ^"(" ~ ^#comma_separated_list1(expr) ~ ^")"
            ~ ( #table_option )?
        },
        |(
            _,
            opt_or_replace,
            opt_async,
            _,
            opt_if_not_exists,
            index_name,
            _,
            (database, schema, table),
            _,
            using_method,
            _,
            expressions,
            _,
            opt_index_options,
        )| {
            let create_option =
                parse_create_option(opt_or_replace.is_some(), opt_if_not_exists.is_some())?;
            Ok(Statement::CreateIndex(CreateIndexStmt {
                create_option,
                is_unique: false,
                index_name,
                index_kind: None,
                database,
                schema,
                table,
                using_method: Some(using_method),
                expressions,
                columns: Vec::new(),
                sync_creation: opt_async.is_none(),
                index_options: opt_index_options.unwrap_or_default(),
            }))
        },
    )
    .parse(i)
}

pub(crate) fn create_default_index(i: Input) -> IResult<Statement> {
    map_res(
        rule! {
            CREATE
            ~ ( OR ~ ^REPLACE )?
            ~ ASYNC?
            ~ UNIQUE?
            ~ INDEX
            ~ ( IF ~ ^NOT ~ ^EXISTS )?
            ~ #ident
            ~ ON ~ #dot_separated_idents_1_to_3
            ~ ^"(" ~ ^#comma_separated_list1(ident) ~ ^")"
            ~ ( #table_option )?
        },
        |(
            _,
            opt_or_replace,
            opt_async,
            opt_unique,
            _,
            opt_if_not_exists,
            index_name,
            _,
            (database, schema, table),
            _,
            columns,
            _,
            opt_index_options,
        )| {
            let create_option =
                parse_create_option(opt_or_replace.is_some(), opt_if_not_exists.is_some())?;
            Ok(Statement::CreateIndex(CreateIndexStmt {
                create_option,
                is_unique: opt_unique.is_some(),
                index_name,
                index_kind: None,
                database,
                schema,
                table,
                using_method: None,
                expressions: Vec::new(),
                columns,
                sync_creation: opt_async.is_none(),
                index_options: opt_index_options.unwrap_or_default(),
            }))
        },
    )
    .parse(i)
}

pub(crate) fn create_index(i: Input) -> IResult<Statement> {
    map_res(
        rule! {
            CREATE
            ~ ( OR ~ ^REPLACE )?
            ~ ASYNC?
            ~ #index_type ~ ^INDEX
            ~ ( IF ~ ^NOT ~ ^EXISTS )?
            ~ #ident
            ~ ON ~ #dot_separated_idents_1_to_3
            ~ ^"(" ~ ^#comma_separated_list1(ident) ~ ^")"
            ~ ( #table_option )?
        },
        |(
            _,
            opt_or_replace,
            opt_async,
            index_type,
            _,
            opt_if_not_exists,
            index_name,
            _,
            (database, schema, table),
            _,
            columns,
            _,
            opt_index_options,
        )| {
            let create_option =
                parse_create_option(opt_or_replace.is_some(), opt_if_not_exists.is_some())?;
            Ok(Statement::CreateIndex(CreateIndexStmt {
                create_option,
                is_unique: false,
                index_name,
                index_kind: Some(index_type),
                database,
                schema,
                table,
                using_method: None,
                expressions: Vec::new(),
                columns,
                sync_creation: opt_async.is_none(),
                index_options: opt_index_options.unwrap_or_default(),
            }))
        },
    )
    .parse(i)
}

pub(crate) fn drop_index_on_table(i: Input) -> IResult<Statement> {
    map(
        rule! {
            DROP ~ #index_type ~ ^INDEX ~ ( IF ~ ^EXISTS )? ~ #ident
            ~ ON ~ #dot_separated_idents_1_to_3
        },
        |(_, index_kind, _, opt_if_exists, index_name, _, (database, schema, table))| {
            Statement::DropIndexOnTable(DropIndexOnTableStmt {
                if_exists: opt_if_exists.is_some(),
                index_name,
                index_kind,
                database,
                schema,
                table,
            })
        },
    )
    .parse(i)
}

pub(crate) fn refresh_index_on_table(i: Input) -> IResult<Statement> {
    map(
        rule! {
            REFRESH ~ #index_type ~ ^INDEX ~ #ident ~ ON ~ #dot_separated_idents_1_to_3 ~ ( LIMIT ~ #literal_u64 )?
        },
        |(_, index_kind, _, index_name, _, (database, schema, table), opt_limit)| {
            Statement::RefreshIndexOnTable(RefreshIndexOnTableStmt {
                index_name,
                index_kind,
                database,
                schema,
                table,
                limit: opt_limit.map(|(_, limit)| limit),
            })
        },
    )
    .parse(i)
}

pub(crate) fn show_indexes(i: Input) -> IResult<Statement> {
    map(
        rule! {
            SHOW ~ INDEXES ~ #show_options?
        },
        |(_, _, show_options)| Statement::ShowIndexes { show_options },
    )
    .parse(i)
}

pub(crate) fn index_type(i: Input) -> IResult<IndexKind> {
    alt((
        value(IndexKind::Inverted, rule! { INVERTED }),
        value(IndexKind::Ngram, rule! { NGRAM }),
        value(IndexKind::Vector, rule! { VECTOR }),
    ))
    .parse(i)
}
