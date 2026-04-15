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
use crate::parser::statement::dispatch::alter_schema_action;
use crate::parser::statement::dispatch::create_database_option;
use crate::parser::statement::dispatch::CreateDatabaseOption;
use crate::parser::statement::helpers::parse_create_option;
use crate::parser::statement::helpers::show_limit;
use crate::parser::token::TokenKind::*;

pub(crate) fn show_databases(i: Input) -> IResult<Statement> {
    map(
        rule! {
            SHOW ~ DATABASES ~ #show_limit?
        },
        |(_, _, limit)| Statement::ShowDatabases(ShowDatabasesStmt { limit }),
    )
    .parse(i)
}

pub(crate) fn show_schemas(i: Input) -> IResult<Statement> {
    map(
        rule! {
            SHOW ~ FULL? ~ SCHEMAS ~ ( ( FROM | IN ) ~ ^#ident )? ~ #show_limit?
        },
        |(_, opt_full, _, opt_database, limit)| {
            Statement::ShowSchemas(ShowSchemasStmt {
                database: opt_database.map(|(_, database)| database),
                full: opt_full.is_some(),
                limit,
            })
        },
    )
    .parse(i)
}

pub(crate) fn create_database(i: Input) -> IResult<Statement> {
    map(
        rule! {
            CREATE ~ DATABASE ~ ( IF ~ ^NOT ~ ^EXISTS )? ~ #ident
        },
        |(_, _, opt_if_not_exists, database)| {
            Statement::CreateDatabase(CreateDatabaseStmt {
                if_not_exists: opt_if_not_exists.is_some(),
                database_name: database.to_string(),
            })
        },
    )
    .parse(i)
}

pub(crate) fn drop_database(i: Input) -> IResult<Statement> {
    map(
        rule! {
            DROP ~ DATABASE ~ ( IF ~ ^EXISTS )? ~ #ident
        },
        |(_, _, opt_if_exists, database)| {
            Statement::DropDatabase(DropDatabaseStmt {
                if_exists: opt_if_exists.is_some(),
                database,
            })
        },
    )
    .parse(i)
}

pub(crate) fn use_database_stmt(i: Input) -> IResult<Statement> {
    map(
        rule! {
            (SET | USE)?  ~ DATABASE ~ #ident
        },
        |(_, _, database)| Statement::UseDatabase { database },
    )
    .parse(i)
}

pub(crate) fn create_schema(i: Input) -> IResult<Statement> {
    map_res(
        rule! {
            CREATE
            ~ ( OR ~ ^REPLACE )?
            ~ SCHEMA
            ~ ( IF ~ ^NOT ~ ^EXISTS )?
            ~ #schema_ref
            ~ #create_database_option?
        },
        |(_, opt_or_replace, _, opt_if_not_exists, schema, create_database_option)| {
            let create_option =
                parse_create_option(opt_or_replace.is_some(), opt_if_not_exists.is_some())?;

            let statement = match create_database_option {
                Some(CreateDatabaseOption::DatabaseEngine(engine)) => {
                    Statement::CreateSchema(CreateSchemaStmt {
                        create_option,
                        schema,
                        engine: Some(engine),
                        options: vec![],
                    })
                }
                None => Statement::CreateSchema(CreateSchemaStmt {
                    create_option,
                    schema,
                    engine: None,
                    options: vec![],
                }),
            };

            Ok(statement)
        },
    )
    .parse(i)
}

pub(crate) fn drop_schema(i: Input) -> IResult<Statement> {
    map(
        rule! {
            DROP ~ SCHEMA ~ ( IF ~ ^EXISTS )? ~ #dot_separated_idents_1_to_2 ~ ( CASCADE )?
        },
        |(_, _, opt_if_exists, (database, schema), opt_cascade)| {
            Statement::DropSchema(DropSchemaStmt {
                if_exists: opt_if_exists.is_some(),
                database,
                schema,
                cascade: opt_cascade.is_some(),
            })
        },
    )
    .parse(i)
}

pub(crate) fn undrop_schema(i: Input) -> IResult<Statement> {
    map(
        rule! {
            UNDROP ~ SCHEMA ~ #dot_separated_idents_1_to_2
        },
        |(_, _, (database, schema))| Statement::UndropSchema(UndropSchemaStmt { database, schema }),
    )
    .parse(i)
}

pub(crate) fn alter_schema(i: Input) -> IResult<Statement> {
    map(
        rule! {
            ALTER ~ SCHEMA ~ ( IF ~ ^EXISTS )? ~ #dot_separated_idents_1_to_2 ~ #alter_schema_action
        },
        |(_, _, opt_if_exists, (database, schema), action)| {
            Statement::AlterSchema(AlterSchemaStmt {
                if_exists: opt_if_exists.is_some(),
                database,
                schema,
                action,
            })
        },
    )
    .parse(i)
}

pub(crate) fn use_schema(i: Input) -> IResult<Statement> {
    map(
        rule! {
            USE ~ #ident
        },
        |(_, schema)| Statement::UseSchema { schema },
    )
    .parse(i)
}
