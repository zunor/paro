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
use crate::parser::statement::acl::show_roles;
use crate::parser::statement::acl::show_users;
use crate::parser::statement::ddl::index::show_indexes;
use crate::parser::statement::ddl::view::show_views;
use crate::parser::statement::dispatch::grant_option;
use crate::parser::statement::dispatch::limit_where;
use crate::parser::statement::dispatch::on_object_name;
use crate::parser::statement::dispatch::role_name;
use crate::parser::statement::dispatch::show_stats_stmt;
use crate::parser::statement::helpers::{show_limit, show_options};
use crate::parser::statement::sequence::sequence;
use crate::parser::statement::session::show_settings;
use crate::parser::statement::session::show_variables;
use crate::parser::statement::stream::show_streams;
use crate::parser::token::TokenKind::*;

enum ShowGrantOption {
    PrincipalIdentity(PrincipalIdentity),
    GrantObjectName(GrantObjectName),
    OfRole(String),
}

fn show_tasks(i: Input) -> IResult<Statement> {
    map(
        rule! {
            SHOW ~ TASKS ~ #show_limit?
        },
        |(_, _, limit)| Statement::ShowTasks(ShowTasksStmt { limit }),
    )
    .parse(i)
}

fn show_tags(i: Input) -> IResult<Statement> {
    map(
        rule! {
            SHOW ~ TAGS ~ #show_options?
        },
        |(_, _, opt_options)| {
            let (filter, limit) = opt_options
                .map(|opts| (opts.show_limit, opts.limit))
                .unwrap_or((None, None));
            Statement::ShowTags(ShowTagsStmt { filter, limit })
        },
    )
    .parse(i)
}

fn show_stages(i: Input) -> IResult<Statement> {
    map(
        rule! {
            SHOW ~ STAGES ~ #show_options?
        },
        |(_, _, show_options)| Statement::ShowStages { show_options },
    )
    .parse(i)
}

fn show_process_list(i: Input) -> IResult<Statement> {
    map(
        rule! {
            SHOW ~ PROCESSLIST ~ #show_options?
        },
        |(_, _, show_options)| Statement::ShowProcessList { show_options },
    )
    .parse(i)
}

fn show_metrics(i: Input) -> IResult<Statement> {
    map(
        rule! {
            SHOW ~ METRICS ~ #show_options?
        },
        |(_, _, show_options)| Statement::ShowMetrics { show_options },
    )
    .parse(i)
}

fn show_engines(i: Input) -> IResult<Statement> {
    map(
        rule! {
            SHOW ~ ENGINES ~ #show_options?
        },
        |(_, _, show_options)| Statement::ShowEngines { show_options },
    )
    .parse(i)
}

fn show_functions(i: Input) -> IResult<Statement> {
    map(
        rule! {
            SHOW ~ FUNCTIONS ~ #show_options?
        },
        |(_, _, show_options)| Statement::ShowFunctions { show_options },
    )
    .parse(i)
}

fn show_user_functions(i: Input) -> IResult<Statement> {
    map(
        rule! {
            SHOW ~ USER ~ FUNCTIONS ~ #show_options?
        },
        |(_, _, _, show_options)| Statement::ShowUserFunctions { show_options },
    )
    .parse(i)
}

fn show_table_functions(i: Input) -> IResult<Statement> {
    map(
        rule! {
            SHOW ~ TABLE_FUNCTIONS ~ #show_options?
        },
        |(_, _, show_options)| Statement::ShowTableFunctions { show_options },
    )
    .parse(i)
}

fn show_locks(i: Input) -> IResult<Statement> {
    map(
        rule! {
            SHOW ~ LOCKS ~ ( IN ~ ^ACCOUNT )? ~ #limit_where?
        },
        |(_, _, opt_in_account, limit)| {
            Statement::ShowLocks(ShowLocksStmt {
                in_account: opt_in_account.is_some(),
                limit,
            })
        },
    )
    .parse(i)
}

fn variable_show(i: Input) -> IResult<Statement> {
    map(
        rule! {
            SHOW ~ ALL
        },
        |(_, _)| {
            Statement::VariableShow(VariableShowStmt {
                target: VariableShowTarget::All,
            })
        },
    )
    .parse(i)
}

fn variable_show_name(i: Input) -> IResult<Statement> {
    map(
        rule! {
            SHOW ~ #ident
        },
        |(_, name)| {
            Statement::VariableShow(VariableShowStmt {
                target: VariableShowTarget::Name(name),
            })
        },
    )
    .parse(i)
}

fn show_create_database(i: Input) -> IResult<Statement> {
    map(
        rule! {
            SHOW ~ CREATE ~ DATABASE ~ #ident
        },
        |(_, _, _, database)| Statement::ShowCreateDatabase(ShowCreateDatabaseStmt { database }),
    )
    .parse(i)
}

fn show_online_nodes(i: Input) -> IResult<Statement> {
    map(
        rule! {
            SHOW ~ ONLINE ~ NODES
        },
        |(_, _, _)| Statement::ShowOnlineNodes(ShowOnlineNodesStmt {}),
    )
    .parse(i)
}

fn show_warehouses(i: Input) -> IResult<Statement> {
    map(
        rule! {
            SHOW ~ WAREHOUSES
        },
        |(_, _)| Statement::ShowWarehouses(ShowWarehousesStmt {}),
    )
    .parse(i)
}

fn show_workload_groups(i: Input) -> IResult<Statement> {
    map(
        rule! {
            SHOW ~ WORKLOAD ~ GROUPS
        },
        |(_, _, _)| Statement::ShowWorkloadGroups(ShowWorkloadGroupsStmt {}),
    )
    .parse(i)
}

fn show_drop_schemas(i: Input) -> IResult<Statement> {
    map(
        rule! {
            SHOW ~ DROP ~ SCHEMAS ~ ( FROM ~ ^#ident )? ~ #show_limit?
        },
        |(_, _, _, opt_database, limit)| {
            Statement::ShowDropSchemas(ShowDropSchemasStmt {
                database: opt_database.map(|(_, database)| database),
                limit,
            })
        },
    )
    .parse(i)
}

fn show_create_schema(i: Input) -> IResult<Statement> {
    map(
        rule! {
            SHOW ~ CREATE ~ SCHEMA ~ #dot_separated_idents_1_to_2
        },
        |(_, _, _, (database, schema))| {
            Statement::ShowCreateSchema(ShowCreateSchemaStmt { database, schema })
        },
    )
    .parse(i)
}

fn from_tables(i: Input) -> IResult<(Option<Identifier>, Option<Identifier>, Identifier)> {
    let from_dot_table = map(
        rule! {
           ( FROM | IN ) ~ ^#dot_separated_idents_1_to_3
        },
        |(_, (catalog, database, table))| (catalog, database, table),
    );

    let from_table = map(
        rule! {
            ( FROM | IN ) ~ #ident
            ~ ( FROM | IN ) ~ ^#dot_separated_idents_1_to_2
        },
        |(_, table, _, (catalog, database))| (catalog, Some(database), table),
    );

    rule!(
        #from_table
        | #from_dot_table
    )
    .parse(i)
}

fn show_columns(i: Input) -> IResult<Statement> {
    map(
        rule! {
            SHOW
            ~ FULL? ~ COLUMNS
            ~ #from_tables
            ~ #show_limit?
        },
        |(_, opt_full, _, (catalog, database, table), limit)| {
            Statement::ShowColumns(ShowColumnsStmt {
                database: catalog,
                schema: database,
                table,
                full: opt_full.is_some(),
                limit,
            })
        },
    )
    .parse(i)
}

fn show_create_table(i: Input) -> IResult<Statement> {
    map(
        rule! {
            SHOW ~ CREATE ~ TABLE ~ #dot_separated_idents_1_to_3 ~ ( WITH ~ ^QUOTED_IDENTIFIERS )?
        },
        |(_, _, _, (catalog, database, table), comment_opt)| {
            Statement::ShowCreateTable(ShowCreateTableStmt {
                database: catalog,
                schema: database,
                table,
                with_quoted_ident: comment_opt.is_some(),
            })
        },
    )
    .parse(i)
}

fn show_fields(i: Input) -> IResult<Statement> {
    map(
        rule! {
            SHOW ~ FIELDS ~ FROM ~ #dot_separated_idents_1_to_3
        },
        |(_, _, _, (catalog, database, table))| {
            Statement::DescribeTable(DescribeTableStmt {
                database: catalog,
                schema: database,
                table,
            })
        },
    )
    .parse(i)
}

fn show_statistics(i: Input) -> IResult<Statement> {
    map(
        rule! {
            SHOW ~ STATISTICS ~ ( FROM ~ #show_stats_stmt)?
        },
        |(_, _, opt_stmt)| {
            Statement::ShowStatistics(opt_stmt.map_or(Default::default(), |(_, stmt)| stmt))
        },
    )
    .parse(i)
}

fn show_tables_status(i: Input) -> IResult<Statement> {
    map(
        rule! {
            SHOW ~ ( TABLES | TABLE ) ~ STATUS ~ ( FROM ~ ^#ident )? ~ #show_limit?
        },
        |(_, _, _, opt_database, limit)| {
            Statement::ShowTablesStatus(ShowTablesStatusStmt {
                schema: opt_database.map(|(_, database)| database),
                limit,
            })
        },
    )
    .parse(i)
}

fn show_drop_tables_status(i: Input) -> IResult<Statement> {
    map(
        rule! {
            SHOW ~ DROP ~ ( TABLES | TABLE ) ~ ( FROM ~ ^#ident )? ~ #show_limit?
        },
        |(_, _, _, opt_database, limit)| {
            Statement::ShowDropTables(ShowDropTablesStmt {
                schema: opt_database.map(|(_, database)| database),
                limit,
            })
        },
    )
    .parse(i)
}

fn show_dictionaries(i: Input) -> IResult<Statement> {
    map(
        rule! {
            SHOW ~ DICTIONARIES ~ ((FROM|IN) ~ #ident)? ~ #show_limit?
        },
        |(_, _, db, limit)| {
            let schema = match db {
                Some((_, d)) => Some(d),
                _ => None,
            };
            Statement::ShowDictionaries(ShowDictionariesStmt { schema, limit })
        },
    )
    .parse(i)
}

fn show_create_dictionary(i: Input) -> IResult<Statement> {
    map(
        rule! {
            SHOW ~ CREATE ~ DICTIONARY ~ #dot_separated_idents_1_to_3
        },
        |(_, _, _, (database, schema, dictionary_name))| {
            Statement::ShowCreateDictionary(ShowCreateDictionaryStmt {
                database,
                schema,
                dictionary_name,
            })
        },
    )
    .parse(i)
}

fn show_virtual_columns(i: Input) -> IResult<Statement> {
    map(
        rule! {
            SHOW ~ VIRTUAL ~ COLUMNS ~ ( ( FROM | IN ) ~ #ident )? ~ ( ( FROM | IN ) ~ ^#dot_separated_idents_1_to_2 )? ~ #show_limit?
        },
        |(_, _, _, opt_table, opt_db, limit)| {
            let table = opt_table.map(|(_, table)| table);
            let (database, schema) = match opt_db {
                Some((_, (Some(d), s))) => (Some(d), Some(s)),
                Some((_, (None, s))) => (None, Some(s)),
                _ => (None, None),
            };
            Statement::ShowVirtualColumns(ShowVirtualColumnsStmt {
                database,
                schema,
                table,
                limit,
            })
        },
    )
    .parse(i)
}

fn show_grant_option(i: Input) -> IResult<ShowGrantOption> {
    alt((
        map(
            rule! {
                FOR ~ #grant_option
            },
            |(_, principal)| ShowGrantOption::PrincipalIdentity(principal),
        ),
        map(
            rule! {
                ON ~ #on_object_name
            },
            |(_, object)| ShowGrantOption::GrantObjectName(object),
        ),
        map(
            rule! {
                OF ~ ROLE ~ #role_name
            },
            |(_, _, role_name)| ShowGrantOption::OfRole(role_name),
        ),
    ))
    .parse(i)
}

fn show_grants(i: Input) -> IResult<Statement> {
    map(
        rule! {
            SHOW ~ GRANTS ~ #show_grant_option? ~ ^#show_options?
        },
        |(_, _, show_grant_option, opt_limit)| match show_grant_option {
            Some(ShowGrantOption::PrincipalIdentity(principal)) => Statement::ShowGrants {
                principal: Some(principal),
                show_options: opt_limit,
            },
            None => Statement::ShowGrants {
                principal: None,
                show_options: opt_limit,
            },
            Some(ShowGrantOption::GrantObjectName(object)) => {
                Statement::ShowObjectPrivileges(ShowObjectPrivilegesStmt {
                    object,
                    show_option: opt_limit,
                })
            }
            Some(ShowGrantOption::OfRole(name)) => {
                Statement::ShowGrantsOfRole(ShowGranteesOfRoleStmt {
                    name,
                    show_option: opt_limit,
                })
            }
        },
    )
    .parse(i)
}

fn show_connections(i: Input) -> IResult<Statement> {
    map(
        rule! {
            SHOW ~ CONNECTIONS
        },
        |(_, _)| Statement::ShowConnections(ShowConnectionsStmt {}),
    )
    .parse(i)
}

fn show_file_formats(i: Input) -> IResult<Statement> {
    value(Statement::ShowFileFormats, rule! { SHOW ~ FILE ~ FORMATS }).parse(i)
}

fn show_network_policies(i: Input) -> IResult<Statement> {
    value(
        Statement::ShowNetworkPolicies,
        rule! { SHOW ~ NETWORK ~ POLICIES },
    )
    .parse(i)
}

fn show_password_policies(i: Input) -> IResult<Statement> {
    map(
        rule! {
            SHOW ~ PASSWORD ~ POLICIES ~ ^#show_options?
        },
        |(_, _, _, show_options)| Statement::ShowPasswordPolicies { show_options },
    )
    .parse(i)
}

fn show_procedures(i: Input) -> IResult<Statement> {
    map(
        rule! {
            SHOW ~ PROCEDURES ~ #show_options?
        },
        |(_, _, show_options)| Statement::ShowProcedures { show_options },
    )
    .parse(i)
}

pub(crate) fn show_stmt(i: Input) -> IResult<Statement> {
    rule!(
        (
            #show_settings : "`SHOW SETTINGS [<show_limit>]`"
            | #show_variables : "`SHOW VARIABLES [<show_limit>]`"
            | #variable_show : "`SHOW ALL`"
            | #variable_show_name : "`SHOW <variable>`"
            | #show_tasks : "`SHOW TASKS [<show_limit>]`"
            | #show_tags : "`SHOW TAGS [<show_limit>]`"
            | #show_stages : "`SHOW STAGES`"
            | #show_process_list : "`SHOW PROCESSLIST`"
            | #show_metrics : "`SHOW METRICS`"
            | #show_engines : "`SHOW ENGINES`"
            | #show_functions : "`SHOW FUNCTIONS [<show_limit>]`"
            | #show_user_functions : "`SHOW USER FUNCTIONS [<show_limit>]`"
            | #show_table_functions : "`SHOW TABLE_FUNCTIONS [<show_limit>]`"
            | #show_indexes : "`SHOW INDEXES`"
            | #show_locks : "`SHOW LOCKS [IN ACCOUNT] [WHERE ...]`"
        )
        | (
            #show_create_database : "`SHOW CREATE DATABASE <database>`"
            | #show_online_nodes: "`SHOW ONLINE NODES`"
            | #show_warehouses: "`SHOW WAREHOUSES`"
            | #show_workload_groups: "`SHOW WORKLOAD GROUPS`"
            | #show_drop_schemas : "`SHOW DROP SCHEMAS [FROM <schema>] [<show_limit>]`"
            | #show_create_schema : "`SHOW CREATE SCHEMA <schema>`"
        )
        | (
            #show_columns : "`SHOW [FULL] COLUMNS FROM <table> [FROM|IN <database>.<schema>] [<show_limit>]`"
            | #show_create_table : "`SHOW CREATE TABLE [<database>.]<table>`"
            | #show_fields : "`SHOW FIELDS FROM [<database>.]<table>`"
            | #show_statistics: "`SHOW STATISTICS [FROM DATABASE [<database>.]<schema> | FROM TABLE [<database>.]<schema>.<table>]`"
            | #show_tables_status : "`SHOW TABLES STATUS [FROM <database>] [<show_limit>]`"
            | #show_drop_tables_status : "`SHOW DROP TABLES [FROM <database>]`"
            | #show_views : "`SHOW [FULL] VIEWS [FROM <database>] [<show_limit>]`"
            | #show_virtual_columns : "`SHOW VIRTUAL COLUMNS FROM <table> [FROM|IN <database>.<schema>] [<show_limit>]`"
        )
        | (
            #show_dictionaries : "`SHOW DICTIONARIES [<show_option>, ...]`"
            | #show_create_dictionary : "`SHOW CREATE DICTIONARY <dictionary_name> `"
            | #show_users : "`SHOW USERS`"
            | #show_roles : "`SHOW ROLES`"
            | #show_grants : "`SHOW GRANTS {FOR  { ROLE <role_name> | USER <user> }] | ON {DATABASE <db_name> | TABLE <db_name>.<table_name>} }`"
            | #show_connections: "`SHOW CONNECTIONS`"
            | #show_file_formats: "`SHOW FILE FORMATS`"
            | #show_network_policies: "`SHOW NETWORK POLICIES`"
            | #show_password_policies: "`SHOW PASSWORD POLICIES [<show_options>]`"
            | #show_procedures : "`SHOW PROCEDURES [<show_options>]()`"
            | #show_streams: "`SHOW [FULL] STREAMS [FROM <database>] [<show_limit>]`"
            | #sequence
        )
    )
    .parse(i)
}
