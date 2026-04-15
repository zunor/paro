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
use crate::parser::expr::literal_u64;
use crate::parser::input::Input;
use crate::parser::query::query;
use crate::parser::statement::dispatch::alter_table_action;
use crate::parser::statement::dispatch::create_table_source;
use crate::parser::statement::dispatch::literal_duration;
use crate::parser::statement::dispatch::optimize_table_action;
use crate::parser::statement::dispatch::vacuum_drop_table_option;
use crate::parser::statement::dispatch::vacuum_table_option;
use crate::parser::statement::helpers::parse_create_option;
use crate::parser::statement::helpers::show_limit;
use crate::parser::statement::helpers::table_option;
use crate::parser::statement::stage::connection_options;
use crate::parser::statement::stage::uri_location;
use crate::parser::token::TokenKind::*;

pub(crate) fn show_tables(i: Input) -> IResult<Statement> {
    map(
        rule! {
            SHOW ~ FULL? ~ TABLES ~ HISTORY? ~ ( ( FROM | IN ) ~ #dot_separated_idents_1_to_2 )? ~ #show_limit?
        },
        |(_, opt_full, _, opt_history, ctl_db, limit)| {
            let (catalog, database) = match ctl_db {
                Some((_, (Some(c), d))) => (Some(c), Some(d)),
                Some((_, (None, d))) => (None, Some(d)),
                _ => (None, None),
            };
            Statement::ShowTables(ShowTablesStmt {
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

pub(crate) fn create_table(i: Input) -> IResult<Statement> {
    map_res(
        rule! {
            CREATE ~ ( OR ~ ^REPLACE )? ~ (TEMP| TEMPORARY)? ~ TABLE ~ ( IF ~ ^NOT ~ ^EXISTS )?
            ~ #dot_separated_idents_1_to_3
            ~ #create_table_source?
            ~ ( #uri_location )?
            ~ ( #table_option )?
            ~ ( PARTITION ~ ^BY ~ ^"(" ~ ^#comma_separated_list1(ident) ~ ^")" )?
            ~ ( PROPERTIES ~  #connection_options )?
            ~ ( AS ~ ^#query )?
        },
        |(
            _,
            opt_or_replace,
            opt_type,
            _,
            opt_if_not_exists,
            (catalog, database, table),
            source,
            uri_location,
            opt_table_options,
            opt_iceberg_table_partition_by,
            opt_table_properties,
            opt_as_query,
        )| {
            let create_option =
                parse_create_option(opt_or_replace.is_some(), opt_if_not_exists.is_some())?;
            let table_type = match opt_type.map(|t| t.kind) {
                None => TableType::Normal,
                Some(TEMP) | Some(TEMPORARY) => TableType::Temporary,
                _ => unreachable!(),
            };
            Ok(Statement::CreateTable(CreateTableStmt {
                create_option,
                database: catalog,
                schema: database,
                table,
                source,
                uri_location,
                table_options: opt_table_options.unwrap_or_default(),
                iceberg_table_partition: opt_iceberg_table_partition_by
                    .map(|(_, _, _, cols, _)| cols),
                table_properties: opt_table_properties.map(|(_, properties)| properties),
                as_query: opt_as_query.map(|(_, query)| Box::new(query)),
                table_type,
            }))
        },
    )
    .parse(i)
}

pub(crate) fn drop_table(i: Input) -> IResult<Statement> {
    map(
        rule! {
            DROP ~ TABLE ~ ( IF ~ ^EXISTS )? ~ #dot_separated_idents_1_to_3 ~ ALL?
        },
        |(_, _, opt_if_exists, (catalog, database, table), opt_all)| {
            Statement::DropTable(DropTableStmt {
                if_exists: opt_if_exists.is_some(),
                database: catalog,
                schema: database,
                table,
                all: opt_all.is_some(),
            })
        },
    )
    .parse(i)
}

pub(crate) fn undrop_table(i: Input) -> IResult<Statement> {
    map(
        rule! {
            UNDROP ~ TABLE ~ #dot_separated_idents_1_to_3
        },
        |(_, _, (catalog, database, table))| {
            Statement::UndropTable(UndropTableStmt {
                database: catalog,
                schema: database,
                table,
            })
        },
    )
    .parse(i)
}

pub(crate) fn alter_table(i: Input) -> IResult<Statement> {
    map(
        rule! {
            ALTER ~ TABLE ~ ( IF ~ ^EXISTS )? ~ #table_reference_only ~ #alter_table_action
        },
        |(_, _, opt_if_exists, table_reference, action)| {
            Statement::AlterTable(AlterTableStmt {
                if_exists: opt_if_exists.is_some(),
                table_reference,
                action,
            })
        },
    )
    .parse(i)
}

pub(crate) fn rename_table(i: Input) -> IResult<Statement> {
    map(
        rule! {
            RENAME ~ TABLE ~ ( IF ~ ^EXISTS )? ~ #dot_separated_idents_1_to_3 ~ TO ~ #dot_separated_idents_1_to_3
        },
        |(
            _,
            _,
            opt_if_exists,
            (catalog, database, table),
            _,
            (new_catalog, new_database, new_table),
        )| {
            Statement::RenameTable(RenameTableStmt {
                if_exists: opt_if_exists.is_some(),
                database: catalog,
                schema: database,
                table,
                new_database: new_catalog,
                new_schema: new_database,
                new_table,
            })
        },
    )
    .parse(i)
}

pub(crate) fn truncate_table(i: Input) -> IResult<Statement> {
    map(
        rule! {
            TRUNCATE ~ TABLE ~ #dot_separated_idents_1_to_3
        },
        |(_, _, (catalog, database, table))| {
            Statement::TruncateTable(TruncateTableStmt {
                database: catalog,
                schema: database,
                table,
            })
        },
    )
    .parse(i)
}

pub(crate) fn optimize_table(i: Input) -> IResult<Statement> {
    map(
        rule! {
            OPTIMIZE ~ TABLE ~ #dot_separated_idents_1_to_3 ~ #optimize_table_action ~ ( LIMIT ~ #literal_u64 )?
        },
        |(_, _, (catalog, database, table), action, opt_limit)| {
            Statement::OptimizeTable(OptimizeTableStmt {
                database: catalog,
                schema: database,
                table,
                action,
                limit: opt_limit.map(|(_, limit)| limit),
            })
        },
    )
    .parse(i)
}

pub(crate) fn vacuum_temp_files(i: Input) -> IResult<Statement> {
    map(
        rule! {
            VACUUM ~ TEMPORARY ~ FILES ~ (RETAIN ~ #literal_duration)? ~ (LIMIT ~ #literal_u64)?
        },
        |(_, _, _, retain, opt_limit)| {
            Statement::VacuumTemporaryFiles(VacuumTemporaryFiles {
                limit: opt_limit.map(|(_, limit)| limit),
                retain: retain.map(|(_, retain)| retain),
            })
        },
    )
    .parse(i)
}

pub(crate) fn vacuum_table(i: Input) -> IResult<Statement> {
    map(
        rule! {
            VACUUM ~ TABLE ~ #dot_separated_idents_1_to_3 ~ #vacuum_table_option
        },
        |(_, _, (catalog, database, table), option)| {
            Statement::VacuumTable(VacuumTableStmt {
                database: catalog,
                schema: database,
                table,
                option,
            })
        },
    )
    .parse(i)
}

pub(crate) fn vacuum_drop_table(i: Input) -> IResult<Statement> {
    map(
        rule! {
            VACUUM ~ DROP ~ TABLE ~ (FROM ~ ^#dot_separated_idents_1_to_2)? ~ #vacuum_drop_table_option
        },
        |(_, _, _, database_option, option)| {
            let (catalog, database) = database_option.map_or_else(
                || (None, None),
                |(_, catalog_database)| (catalog_database.0, Some(catalog_database.1)),
            );
            Statement::VacuumDropTable(VacuumDropTableStmt {
                database: catalog,
                schema: database,
                option,
            })
        },
    )
    .parse(i)
}

pub(crate) fn analyze_table(i: Input) -> IResult<Statement> {
    map(
        rule! {
            ANALYZE ~ TABLE ~ #dot_separated_idents_1_to_3 ~ NOSCAN?
        },
        |(_, _, (catalog, database, table), no_scan)| {
            Statement::AnalyzeTable(AnalyzeTableStmt {
                database: catalog,
                schema: database,
                table,
                no_scan: no_scan.is_some(),
            })
        },
    )
    .parse(i)
}

pub(crate) fn exists_table(i: Input) -> IResult<Statement> {
    map(
        rule! {
            EXISTS ~ TABLE ~ #dot_separated_idents_1_to_3
        },
        |(_, _, (catalog, database, table))| {
            Statement::ExistsTable(ExistsTableStmt {
                database: catalog,
                schema: database,
                table,
            })
        },
    )
    .parse(i)
}
