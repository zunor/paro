// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0
//
// Derived from Databend (https://github.com/datafuselabs/databend),
// Copyright 2021 Datafuse Labs, also licensed under Apache License 2.0.

use std::collections::BTreeMap;
use std::time::Duration;

use educe::Educe;
use nom::Parser;
use nom_rule::rule;

use super::acl::alter_role as acl_alter_role;
use super::acl::alter_user as acl_alter_user;
use super::acl::create_role as acl_create_role;
use super::acl::create_user as acl_create_user;
use super::acl::describe_user as acl_describe_user;
use super::acl::drop_role as acl_drop_role;
use super::acl::drop_user as acl_drop_user;
use super::acl::grant as acl_grant;
use super::acl::grant_ownership as acl_grant_ownership;
use super::acl::revoke as acl_revoke;
use super::ddl::graph::create_property_graph;
use super::ddl::graph::drop_property_graph;
use super::ddl::graph::refresh_property_graph;
use super::ddl::index::create_aggregating_index;
use super::ddl::index::create_default_index;
use super::ddl::index::create_index;
use super::ddl::index::create_index_using;
use super::ddl::index::drop_index;
use super::ddl::index::drop_index_on_table;
use super::ddl::index::index_type;
use super::ddl::index::refresh_aggregating_index;
use super::ddl::index::refresh_index_on_table;
use super::ddl::schema::alter_schema as schema_alter_schema;
use super::ddl::schema::create_database as schema_create_database;
use super::ddl::schema::create_schema as schema_create_schema;
use super::ddl::schema::drop_database as schema_drop_database;
use super::ddl::schema::drop_schema as schema_drop_schema;
use super::ddl::schema::show_databases as schema_show_databases;
use super::ddl::schema::show_schemas as schema_show_schemas;
use super::ddl::schema::undrop_schema as schema_undrop_schema;
use super::ddl::schema::use_database_stmt as schema_use_database_stmt;
use super::ddl::schema::use_schema as schema_use_schema;
use super::ddl::table::alter_table as table_alter_table;
use super::ddl::table::analyze_table as table_analyze_table;
use super::ddl::table::create_table as table_create_table;
use super::ddl::table::drop_table as table_drop_table;
use super::ddl::table::exists_table as table_exists_table;
use super::ddl::table::optimize_table as table_optimize_table;
use super::ddl::table::rename_table as table_rename_table;
use super::ddl::table::show_tables as table_show_tables;
use super::ddl::table::truncate_table as table_truncate_table;
use super::ddl::table::undrop_table as table_undrop_table;
use super::ddl::table::vacuum_drop_table as table_vacuum_drop_table;
use super::ddl::table::vacuum_table as table_vacuum_table;
use super::ddl::table::vacuum_temp_files as table_vacuum_temp_files;
use super::ddl::view::alter_view;
use super::ddl::view::create_view;
use super::ddl::view::describe_view;
use super::ddl::view::drop_view;
use super::dml::conditional_multi_table_insert as dml_conditional_multi_table_insert;
use super::dml::delete as dml_delete;
use super::dml::insert_stmt as dml_insert_stmt;
use super::dml::merge as dml_merge;
use super::dml::replace_stmt as dml_replace_stmt;
use super::dml::unconditional_multi_table_insert as dml_unconditional_multi_table_insert;
use super::dml::update as dml_update;
use super::explain::explain;
use super::explain::explain_analyze;
use super::sequence::sequence;
use super::session::reset_stmt as session_reset_stmt;
use super::session::set_role as session_set_role;
use super::session::set_secondary_roles as session_set_secondary_roles;
use super::session::set_secondary_specify_roles as session_set_secondary_specify_roles;
use super::session::set_stmt as session_set_stmt;
use super::session::show_settings as session_show_settings;
use super::session::show_variables as session_show_variables;
use super::session::unset_stmt as session_unset_stmt;
use super::session::use_warehouse as session_use_warehouse;
use super::show::show_stmt;
use super::stream::create_stream;
use super::stream::describe_stream;
use super::transaction::checkpoint_stmt;
use super::transaction::close_cursor_stmt;
use super::transaction::deallocate_stmt;
use super::transaction::declare_cursor_stmt;
use super::transaction::discard_stmt;
use super::transaction::execute_prepared_stmt;
use super::transaction::fetch_stmt;
use super::transaction::move_stmt;
use super::transaction::prepare_stmt;
use super::transaction::transaction_stmt;
use super::utility::execute_immediate as utility_execute_immediate;
use super::utility::kill_stmt as utility_kill_stmt;
use super::utility::presign as utility_presign;
use super::utility::set_priority as utility_set_priority;
use super::utility::system_action as utility_system_action;
use crate::ast::*;
use crate::parser::common::*;
use crate::parser::expr::subexpr;
use crate::parser::expr::*;
use crate::parser::input::Input;
use crate::parser::query::*;
use crate::parser::statement::comment::comment;
use crate::parser::statement::copy::copy_stmt;
use crate::parser::statement::data_mask::data_mask_policy;
use crate::parser::statement::dynamic_table::dynamic_table;
use crate::parser::statement::stage::*;
use crate::parser::statement::stream::drop_stream;
use crate::parser::token::*;
use crate::parser::Error;
use crate::parser::ErrorKind;

#[derive(Clone)]
pub enum CreateDatabaseOption {
    DatabaseEngine(DatabaseEngine),
}

fn procedure_type_name(i: Input) -> IResult<Vec<TypeName>> {
    let procedure_type_names = map(
        rule! {
            "(" ~ #comma_separated_list1(type_name) ~ ")"
        },
        |(_, args, _)| args,
    );
    let procedure_empty_types = map(
        rule! {
            "(" ~ ")"
        },
        |(_, _)| vec![],
    );
    rule!(#procedure_empty_types: "()"
            | #procedure_type_names: "(<type_name>, ...)")
    .parse(i)
}

fn query_statement(i: Input) -> IResult<Statement> {
    map(query, |query| Statement::Query(Box::new(query))).parse(i)
}

pub fn statement_body(i: Input) -> IResult<Statement> {
    let query_setting = map_res(
        rule! {
            SETTINGS ~ #query_statement_setting? ~ #statement_body
        },
        |(_, opt_settings, statement)| {
            Ok(Statement::StatementWithSettings {
                settings: opt_settings,
                stmt: Box::new(statement),
            })
        },
    );
    let report = map_res(rule! { REPORT ~ ISSUE ~ #rest_str }, |(_, _, (sql, _))| {
        Ok(Statement::ReportIssue(sql))
    });

    let create_task = map_res(
        rule! {
            CREATE ~ ( OR ~ ^REPLACE )? ~ TASK ~ ( IF ~ ^NOT ~ ^EXISTS )?
            ~ #ident
            ~ #create_task_option*
            ~ #set_table_option?
            ~ AS ~ #task_sql_block
        },
        |(
            _,
            opt_or_replace,
            _,
            opt_if_not_exists,
            task,
            create_task_opts,
            session_opts,
            _,
            sql,
        )| {
            let session_opts = session_opts.unwrap_or_default();
            let create_option =
                parse_create_option(opt_or_replace.is_some(), opt_if_not_exists.is_some())?;

            let mut stmt = CreateTaskStmt {
                create_option,
                name: task.to_string(),
                warehouse: None,
                schedule_opts: None,
                suspend_task_after_num_failures: None,
                comments: None,
                after: vec![],
                error_integration: None,
                when_condition: None,
                sql,
                session_parameters: session_opts,
            };
            for opt in create_task_opts {
                stmt.apply_opt(opt);
            }
            Ok(Statement::CreateTask(stmt))
        },
    );

    let alter_task = map(
        rule! {
            ALTER ~ TASK ~ ( IF ~ ^EXISTS )?
            ~ #ident ~ #alter_task_option
        },
        |(_, _, opt_if_exists, task, options)| {
            Statement::AlterTask(AlterTaskStmt {
                if_exists: opt_if_exists.is_some(),
                name: task.to_string(),
                options,
            })
        },
    );

    let drop_task = map(
        rule! {
            DROP ~ TASK ~ ( IF ~ ^EXISTS )?
            ~ #ident
        },
        |(_, _, opt_if_exists, task)| {
            Statement::DropTask(DropTaskStmt {
                if_exists: opt_if_exists.is_some(),
                name: task.to_string(),
            })
        },
    );
    let _show_tasks = map(
        rule! {
            SHOW ~ TASKS ~ #show_limit?
        },
        |(_, _, limit)| Statement::ShowTasks(ShowTasksStmt { limit }),
    );

    let execute_task = map(
        rule! {
            EXECUTE ~ TASK ~ #ident
        },
        |(_, _, task)| Statement::ExecuteTask(ExecuteTaskStmt { name: task }),
    );

    let desc_task = map(
        rule! {
            ( DESC | DESCRIBE ) ~ TASK ~ #ident
        },
        |(_, _, task)| Statement::DescribeTask(DescribeTaskStmt { name: task }),
    );

    // databases
    let _show_create_database = map(
        rule! {
            SHOW ~ CREATE ~ DATABASE ~ #ident
        },
        |(_, _, _, database)| Statement::ShowCreateDatabase(ShowCreateDatabaseStmt { database }),
    );
    let _connect_to = map(
        rule! {
            CONNECT ~ TO ~ #ident
        },
        |(_, _, database)| Statement::ConnectTo(ConnectToStmt { database }),
    );

    let _show_online_nodes = map(
        rule! {
            SHOW ~ ONLINE ~ NODES
        },
        |(_, _, _)| Statement::ShowOnlineNodes(ShowOnlineNodesStmt {}),
    );

    let _show_warehouses = map(
        rule! {
            SHOW ~ WAREHOUSES
        },
        |(_, _)| Statement::ShowWarehouses(ShowWarehousesStmt {}),
    );

    let _use_warehouse = map(
        rule! {
            USE ~ WAREHOUSE ~ #ident
        },
        |(_, _, warehouse)| Statement::UseWarehouse(UseWarehouseStmt { warehouse }),
    );

    let create_warehouse = map(
        rule! {
            CREATE ~ WAREHOUSE ~ #ident ~ ("(" ~ #assign_nodes_list ~ ")")? ~ (WITH ~ #warehouse_cluster_option)?
        },
        |(_, _, warehouse, nodes, options)| {
            Statement::CreateWarehouse(CreateWarehouseStmt {
                warehouse,
                node_list: nodes.map(|(_, nodes, _)| nodes).unwrap_or_else(Vec::new),
                options: options.map(|(_, x)| x).unwrap_or_else(BTreeMap::new),
            })
        },
    );

    let drop_warehouse = map(
        rule! {
            DROP ~ WAREHOUSE ~ #ident
        },
        |(_, _, warehouse)| Statement::DropWarehouse(DropWarehouseStmt { warehouse }),
    );

    let rename_warehouse = map(
        rule! {
            RENAME ~ WAREHOUSE ~ #ident ~ TO ~ #ident
        },
        |(_, _, warehouse, _, new_warehouse)| {
            Statement::RenameWarehouse(RenameWarehouseStmt {
                warehouse,
                new_warehouse,
            })
        },
    );

    let resume_warehouse = map(
        rule! {
            RESUME ~ WAREHOUSE ~ #ident
        },
        |(_, _, warehouse)| Statement::ResumeWarehouse(ResumeWarehouseStmt { warehouse }),
    );

    let suspend_warehouse = map(
        rule! {
            SUSPEND ~ WAREHOUSE ~ #ident
        },
        |(_, _, warehouse)| Statement::SuspendWarehouse(SuspendWarehouseStmt { warehouse }),
    );

    let inspect_warehouse = map(
        rule! {
            INSPECT ~ WAREHOUSE ~ #ident
        },
        |(_, _, warehouse)| Statement::InspectWarehouse(InspectWarehouseStmt { warehouse }),
    );

    let add_warehouse_cluster = map(
        rule! {
            ALTER ~ WAREHOUSE ~ #ident ~ ADD ~ CLUSTER ~ #ident ~ ("(" ~ #assign_nodes_list ~ ")")? ~ (WITH ~ #warehouse_cluster_option)?
        },
        |(_, _, warehouse, _, _, cluster, nodes, options)| {
            Statement::AddWarehouseCluster(AddWarehouseClusterStmt {
                warehouse,
                cluster,
                node_list: nodes.map(|(_, nodes, _)| nodes).unwrap_or_else(Vec::new),
                options: options.map(|(_, x)| x).unwrap_or_else(BTreeMap::new),
            })
        },
    );

    let drop_warehouse_cluster = map(
        rule! {
            ALTER ~ WAREHOUSE ~ #ident ~ DROP ~ CLUSTER ~ #ident
        },
        |(_, _, warehouse, _, _, cluster)| {
            Statement::DropWarehouseCluster(DropWarehouseClusterStmt { warehouse, cluster })
        },
    );

    let rename_warehouse_cluster = map(
        rule! {
            ALTER ~ WAREHOUSE ~ #ident ~ RENAME ~ CLUSTER ~ #ident ~ TO ~ #ident
        },
        |(_, _, warehouse, _, _, cluster, _, new_cluster)| {
            Statement::RenameWarehouseCluster(RenameWarehouseClusterStmt {
                warehouse,
                cluster,
                new_cluster,
            })
        },
    );

    let assign_warehouse_nodes = map(
        rule! {
            ALTER ~ WAREHOUSE ~ #ident ~ ASSIGN ~ NODES ~ "(" ~ #assign_warehouse_nodes_list ~ ")"
        },
        |(_, _, warehouse, _, _, _, nodes, _)| {
            Statement::AssignWarehouseNodes(AssignWarehouseNodesStmt {
                warehouse,
                node_list: nodes,
            })
        },
    );

    let unassign_warehouse_nodes = map(
        rule! {
            ALTER ~ WAREHOUSE ~ #ident ~ UNASSIGN ~ NODES ~ "(" ~ #unassign_warehouse_nodes_list ~ ")"
        },
        |(_, _, warehouse, _, _, _, nodes, _)| {
            Statement::UnassignWarehouseNodes(UnassignWarehouseNodesStmt {
                warehouse,
                node_list: nodes,
            })
        },
    );

    let _show_workload_groups = map(
        rule! {
            SHOW ~ WORKLOAD ~ GROUPS
        },
        |(_, _, _)| Statement::ShowWorkloadGroups(ShowWorkloadGroupsStmt {}),
    );

    let create_workload_group = map(
        rule! {
            CREATE ~ WORKLOAD ~ GROUP ~ ( IF ~ ^NOT ~ ^EXISTS )? ~ #ident ~ WITH ~ #workload_quotas
        },
        |(_, _, _, if_not_exists, name, _, quotas)| {
            Statement::CreateWorkloadGroup(CreateWorkloadGroupStmt {
                name,
                quotas,
                if_not_exists: if_not_exists.is_some(),
            })
        },
    );

    let drop_workload_group = map(
        rule! {
            DROP ~ WORKLOAD ~ GROUP ~ ( IF ~ ^EXISTS )? ~ #ident
        },
        |(_, _, _, if_exists, name)| {
            Statement::DropWorkloadGroup(DropWorkloadGroupStmt {
                name,
                if_exists: if_exists.is_some(),
            })
        },
    );

    let rename_workload_group = map(
        rule! {
            RENAME ~ WORKLOAD ~ GROUP ~ #ident ~ TO ~ #ident
        },
        |(_, _, _, name, _, new_name)| {
            Statement::RenameWorkloadGroup(RenameWorkloadGroupStmt { name, new_name })
        },
    );

    let set_workload_group_quotas = map(
        rule! {
            ALTER ~ WORKLOAD ~ GROUP ~ #ident ~ SET ~ #workload_quotas
        },
        |(_, _, _, name, _, quotas)| {
            Statement::SetWorkloadQuotasGroup(SetWorkloadGroupQuotasStmt { name, quotas })
        },
    );

    let unset_workload_group_quotas = map(
        rule! {
            ALTER ~ WORKLOAD ~ GROUP ~ #ident ~ UNSET ~ #unset_source
        },
        |(_, _, _, name, _, quotas)| {
            Statement::UnsetWorkloadQuotasGroup(UnsetWorkloadGroupQuotasStmt { name, quotas })
        },
    );

    let _show_tags = map(
        rule! {
            SHOW ~ TAGS ~ #show_options?
        },
        |(_, _, opt_options)| {
            let (filter, limit) = opt_options
                .map(|opts| (opts.show_limit, opts.limit))
                .unwrap_or((None, None));
            Statement::ShowTags(ShowTagsStmt { filter, limit })
        },
    );

    let _show_drop_schemas = map(
        rule! {
            SHOW ~ DROP ~ SCHEMAS ~ ( FROM ~ ^#ident )? ~ #show_limit?
        },
        |(_, _, _, opt_database, limit)| {
            Statement::ShowDropSchemas(ShowDropSchemasStmt {
                database: opt_database.map(|(_, database)| database),
                limit,
            })
        },
    );

    let _show_create_schema = map(
        rule! {
            SHOW ~ CREATE ~ SCHEMA ~ #dot_separated_idents_1_to_2
        },
        |(_, _, _, (database, schema))| {
            Statement::ShowCreateSchema(ShowCreateSchemaStmt { database, schema })
        },
    );

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

    let _show_columns = map(
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
    );
    let _show_create_table = map(
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
    );
    let describe_table = map(
        rule! {
            ( DESC | DESCRIBE ) ~ TABLE? ~ #dot_separated_idents_1_to_3
        },
        |(_, _, (catalog, database, table))| {
            Statement::DescribeTable(DescribeTableStmt {
                database: catalog,
                schema: database,
                table,
            })
        },
    );

    // parse `show fields from` statement
    let _show_fields = map(
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
    );

    let _show_statistics = map(
        rule! {
            SHOW ~ STATISTICS ~ ( FROM ~ #show_stats_stmt)?
        },
        |(_, _, opt_stmt)| {
            Statement::ShowStatistics(opt_stmt.map_or(Default::default(), |(_, stmt)| stmt))
        },
    );

    let _show_tables_status = map(
        rule! {
            SHOW ~ ( TABLES | TABLE ) ~ STATUS ~ ( FROM ~ ^#ident )? ~ #show_limit?
        },
        |(_, _, _, opt_database, limit)| {
            Statement::ShowTablesStatus(ShowTablesStatusStmt {
                schema: opt_database.map(|(_, database)| database),
                limit,
            })
        },
    );
    let _show_drop_tables_status = map(
        rule! {
            SHOW ~ DROP ~ ( TABLES | TABLE ) ~ ( FROM ~ ^#ident )? ~ #show_limit?
        },
        |(_, _, _, opt_database, limit)| {
            Statement::ShowDropTables(ShowDropTablesStmt {
                schema: opt_database.map(|(_, database)| database),
                limit,
            })
        },
    );

    let attach_table = map(
        rule! {
            ATTACH ~ TABLE ~ #dot_separated_idents_1_to_3 ~ ("(" ~ #comma_separated_list1(ident) ~ ")")? ~ #uri_location
        },
        |(_, _, (catalog, database, table), columns_opt, uri_location)| {
            let columns_opt = columns_opt.map(|(_, v, _)| v);
            Statement::AttachTable(AttachTableStmt {
                database: catalog,
                schema: database,
                table,
                columns_opt,
                uri_location,
            })
        },
    );
    // DICTIONARY
    let create_dictionary = map_res(
        rule! {
            CREATE ~ ( OR ~ ^REPLACE )? ~ DICTIONARY ~ ( IF ~ ^NOT ~ ^EXISTS )?
            ~ #dot_separated_idents_1_to_3
            ~ "(" ~ ^#comma_separated_list1(column_def) ~ ^")"
            ~ PRIMARY ~ ^KEY  ~ ^#comma_separated_list1(ident)
            ~ ^SOURCE ~ ^"(" ~ ^#ident ~ ^"("
            ~ ( #table_option )?
            ~ ^")" ~ ^")"
            ~ ( COMMENT ~ ^#literal_string )?
        },
        |(
            _,
            opt_or_replace,
            _,
            opt_if_not_exists,
            (database, schema, dictionary_name),
            _,
            columns,
            _,
            _,
            _,
            primary_keys,
            _,
            _,
            source_name,
            _,
            opt_source_options,
            _,
            _,
            opt_comment,
        )| {
            let create_option =
                parse_create_option(opt_or_replace.is_some(), opt_if_not_exists.is_some())?;
            Ok(Statement::CreateDictionary(CreateDictionaryStmt {
                create_option,
                database,
                schema,
                dictionary_name,
                columns,
                primary_keys,
                source_name,
                source_options: opt_source_options.unwrap_or_default(),
                comment: opt_comment.map(|(_, comment)| comment),
            }))
        },
    );
    let drop_dictionary = map(
        rule! {
            DROP ~ DICTIONARY ~ ( IF ~ ^EXISTS )? ~ #dot_separated_idents_1_to_3
        },
        |(_, _, opt_if_exists, (database, schema, dictionary_name))| {
            Statement::DropDictionary(DropDictionaryStmt {
                if_exists: opt_if_exists.is_some(),
                database,
                schema,
                dictionary_name,
            })
        },
    );
    let _show_dictionaries = map(
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
    );
    let _show_create_dictionary = map(
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
    );
    let rename_dictionary = map(
        rule! {
            RENAME ~ DICTIONARY ~ ( IF ~ ^EXISTS )? ~ #dot_separated_idents_1_to_3 ~ TO ~ #dot_separated_idents_1_to_3
        },
        |(
            _,
            _,
            opt_if_exists,
            (database, schema, dictionary),
            _,
            (new_database, new_schema, new_dictionary),
        )| {
            Statement::RenameDictionary(RenameDictionaryStmt {
                if_exists: opt_if_exists.is_some(),
                database,
                schema,
                dictionary,
                new_database,
                new_schema,
                new_dictionary,
            })
        },
    );

    let refresh_virtual_column = map(
        rule! {
            REFRESH ~ VIRTUAL ~ ^COLUMN ~ ^( FOR | ON ) ~ ^#dot_separated_idents_1_to_3 ~ ( WHERE ~ ^#expr )? ~ ( LIMIT ~ ^#literal_u64 )? ~ OVERWRITE?
        },
        |(_, _, _, _, (database, schema, table), opt_selection, opt_limit, opt_overwrite)| {
            Statement::RefreshVirtualColumn(RefreshVirtualColumnStmt {
                database,
                schema,
                table,
                selection: opt_selection.map(|(_, selection)| Box::new(selection)),
                limit: opt_limit.map(|(_, limit)| limit),
                overwrite: opt_overwrite.is_some(),
            })
        },
    );
    let refresh_property_graph = refresh_property_graph;

    let _show_virtual_columns = map(
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
    );

    let create_function = map_res(
        rule! {
            CREATE ~ ( OR ~ ^REPLACE )? ~ FUNCTION ~ ( IF ~ ^NOT ~ ^EXISTS )?
            ~ #function_name_ref
            ~ "(" ~ #comma_separated_list0(function_argument) ~ ")"
            ~ RETURNS ~ #function_return
            ~ #create_function_option*
            ~ AS ~ ^(#code_string | #literal_string)
        },
        |(
            _,
            opt_or_replace,
            _,
            opt_if_not_exists,
            name,
            _,
            arguments,
            _,
            _,
            return_type,
            options,
            _,
            definition,
        )| {
            let create_option =
                parse_create_option(opt_or_replace.is_some(), opt_if_not_exists.is_some())?;
            let mut language = None;
            let mut volatility = None;
            let mut strict = false;
            let mut security = FunctionSecurity::Invoker;
            let mut handler = None;
            let mut packages = Vec::new();
            let mut imports = Vec::new();
            let mut rows = None;
            let mut capability_profile = None;

            for option in options {
                match option {
                    CreateFunctionOption::Language(value) => language = Some(value),
                    CreateFunctionOption::Volatility(value) => volatility = Some(value),
                    CreateFunctionOption::Strict => strict = true,
                    CreateFunctionOption::Security(value) => security = value,
                    CreateFunctionOption::Handler(value) => handler = Some(value),
                    CreateFunctionOption::Packages(value) => packages = value,
                    CreateFunctionOption::Imports(value) => imports = value,
                    CreateFunctionOption::Rows(value) => rows = Some(value),
                    CreateFunctionOption::CapabilityProfile(value) => {
                        capability_profile = Some(value)
                    }
                }
            }

            let Some(language) = language else {
                return Err(nom::Err::Failure(ErrorKind::Other(
                    "CREATE FUNCTION requires LANGUAGE",
                )));
            };

            if rows.is_some() && !matches!(return_type, FunctionReturn::Table(_)) {
                return Err(nom::Err::Failure(ErrorKind::Other(
                    "ROWS is only valid for RETURNS TABLE routines",
                )));
            }

            Ok(Statement::CreateFunction(CreateFunctionStmt {
                create_option,
                name,
                arguments,
                return_type,
                language,
                volatility,
                strict,
                security,
                handler,
                packages,
                imports,
                rows,
                capability_profile,
                definition,
            }))
        },
    );
    let drop_function = map(
        rule! {
            DROP ~ FUNCTION ~ ( IF ~ ^EXISTS )? ~ #function_identity
        },
        |(_, _, opt_if_exists, identity)| {
            Statement::DropFunction(DropFunctionStmt {
                if_exists: opt_if_exists.is_some(),
                identity,
            })
        },
    );

    // row policy
    // CREATE ROW ACCESS POLICY [ IF NOT EXISTS ] <name> AS
    // ( <arg_name> <arg_type> [ , ... ] ) RETURNS BOOLEAN -> <body>
    // [ COMMENT = '<string_literal>' ]
    let create_row_access = map(
        rule! {
            CREATE ~ ROW ~ ACCESS ~ POLICY ~ ( IF ~ ^NOT ~ ^EXISTS )?
            ~ #ident ~ #row_access_definition
            ~ ( COMMENT ~ ^"=" ~ ^#literal_string )?
        },
        |(_, _, _, _, opt_if_not_exists, name, definition, opt_comment)| {
            Statement::CreateRowAccessPolicy(CreateRowAccessPolicyStmt {
                if_not_exists: opt_if_not_exists.is_some(),
                name,
                description: opt_comment.map(|(_, _, description)| description),
                definition,
            })
        },
    );
    let drop_row_access = map(
        rule! {
            DROP ~ ROW ~ ACCESS ~ POLICY ~ ( IF ~ ^EXISTS )? ~ #ident
        },
        |(_, _, _, _, opt_if_exists, name)| {
            let stmt = DropRowAccessPolicyStmt {
                if_exists: opt_if_exists.is_some(),
                name: name.to_string(),
            };
            Statement::DropRowAccessPolicy(stmt)
        },
    );
    let describe_row_access = map(
        rule! {
            ( DESC | DESCRIBE ) ~ ROW ~ ACCESS ~ POLICY ~ #ident
        },
        |(_, _, _, _, name)| {
            Statement::DescRowAccessPolicy(DescRowAccessPolicyStmt {
                name: name.to_string(),
            })
        },
    );

    #[derive(Clone)]
    enum CreateTagOption {
        AllowedValues(Vec<Literal>),
        Comment(String),
    }

    let tag_allowed_values = map(
        rule! {
            ALLOWED_VALUES ~ ^"=" ~ ^"(" ~ #comma_separated_list1(literal) ~ ^")"
        },
        |(_, _, _, values, _)| CreateTagOption::AllowedValues(values),
    );

    let tag_comment = map(
        rule! {
            COMMENT ~ ^"=" ~ ^#literal_string
        },
        |(_, _, comment)| CreateTagOption::Comment(comment),
    );

    let tag_options = map(rule! { ( #tag_allowed_values | #tag_comment )* }, |opts| {
        opts
    });

    let create_tag = map_res(
        rule! {
            CREATE ~ TAG ~ ( IF ~ ^NOT ~ ^EXISTS )?
            ~ #ident
            ~ #tag_options
        },
        |(_, _, opt_if_not_exists, name, options)| {
            let create_option = parse_create_option(false, opt_if_not_exists.is_some())?;
            let mut allowed_values = None;
            let mut comment = None;
            for opt in options {
                match opt {
                    CreateTagOption::AllowedValues(values) => {
                        allowed_values = Some(values);
                    }
                    CreateTagOption::Comment(text) => {
                        comment = Some(text);
                    }
                }
            }
            Ok(Statement::CreateTag(CreateTagStmt {
                create_option,
                name,
                allowed_values,
                comment,
            }))
        },
    );

    let drop_tag = map(
        rule! {
            DROP ~ TAG ~ ( IF ~ ^EXISTS )? ~ #ident
        },
        |(_, _, opt_if_exists, name)| {
            Statement::DropTag(DropTagStmt {
                if_exists: opt_if_exists.is_some(),
                name,
            })
        },
    );

    // stages
    let create_stage = map_res(
        rule! {
            CREATE ~ ( OR ~ ^REPLACE )? ~ STAGE ~ ( IF ~ ^NOT ~ ^EXISTS )?
            ~ ( #stage_name )
            ~ ( (URL ~ ^"=")? ~ #uri_location )?
            ~ ( #file_format_clause )?
            ~ ( (COMMENT | COMMENTS) ~ ^"=" ~ ^#literal_string )?
        },
        |(
            _,
            opt_or_replace,
            _,
            opt_if_not_exists,
            stage,
            url_opt,
            file_format_opt,
            comment_opt,
        )| {
            let create_option =
                parse_create_option(opt_or_replace.is_some(), opt_if_not_exists.is_some())?;
            Ok(Statement::CreateStage(CreateStageStmt {
                create_option,
                stage_name: stage.to_string(),
                location: url_opt.map(|(_, location)| location),
                file_format_options: file_format_opt.unwrap_or_default(),
                comments: comment_opt.map(|v| v.2).unwrap_or_default(),
            }))
        },
    );

    let list_stage = map(
        rule! {
            LIST ~ #at_string ~ (PATTERN ~ "=" ~ #literal_string)?
        },
        |(_, location, opt_pattern)| Statement::ListStage {
            location,
            pattern: opt_pattern.map(|v| v.2),
        },
    );

    let remove_stage = map(
        rule! {
            REMOVE ~ #at_string ~ (PATTERN ~ "=" ~ #literal_string)?
        },
        |(_, location, opt_pattern)| Statement::RemoveStage {
            location,
            pattern: opt_pattern.map(|v| v.2).unwrap_or_default(),
        },
    );

    let drop_stage = map(
        rule! {
            DROP ~ STAGE ~ ( IF ~ ^EXISTS )? ~ #stage_name
        },
        |(_, _, opt_if_exists, stage_name)| Statement::DropStage {
            if_exists: opt_if_exists.is_some(),
            stage_name: stage_name.to_string(),
        },
    );

    let desc_stage = map(
        rule! {
            (DESC | DESCRIBE) ~ STAGE ~ #ident
        },
        |(_, _, stage_name)| Statement::DescribeStage {
            stage_name: stage_name.to_string(),
        },
    );

    // connections
    let connection_opt = connection_opt("=");
    let create_connection = map_res(
        rule! {
            CREATE ~ ( OR ~ ^REPLACE )? ~ CONNECTION ~ ( IF ~ ^NOT ~ ^EXISTS )?
            ~ #ident ~ STORAGE_TYPE ~ "=" ~  #literal_string ~ #connection_opt*
        },
        |(
            _,
            opt_or_replace,
            _,
            opt_if_not_exists,
            connection_name,
            _,
            _,
            storage_type,
            options,
        )| {
            let create_option =
                parse_create_option(opt_or_replace.is_some(), opt_if_not_exists.is_some())?;
            let options =
                BTreeMap::from_iter(options.iter().map(|(k, v)| (k.to_lowercase(), v.clone())));
            Ok(Statement::CreateConnection(CreateConnectionStmt {
                create_option,
                name: connection_name,
                storage_type,
                storage_params: options,
            }))
        },
    );

    let drop_connection = map(
        rule! {
            DROP ~ CONNECTION ~ ( IF ~ ^EXISTS )? ~ #ident
        },
        |(_, _, opt_if_exists, connection_name)| {
            Statement::DropConnection(DropConnectionStmt {
                if_exists: opt_if_exists.is_some(),
                name: connection_name,
            })
        },
    );

    let desc_connection = map(
        rule! {
            (DESC | DESCRIBE) ~ CONNECTION ~ #ident
        },
        |(_, _, name)| Statement::DescribeConnection(DescribeConnectionStmt { name }),
    );

    let _show_connections = map(
        rule! {
            SHOW ~ CONNECTIONS
        },
        |(_, _)| Statement::ShowConnections(ShowConnectionsStmt {}),
    );

    let _call = map(
        rule! {
            CALL ~ #ident ~ "(" ~ #comma_separated_list0(parameter_to_string) ~ ")"
        },
        |(_, name, _, args, _)| Statement::Call(CallStmt { name, args }),
    );

    let vacuum_temporary_tables = map(
        rule! {
            VACUUM ~ TEMPORARY ~ TABLES ~ ( LIMIT ~ ^#literal_u64 )?
        },
        |(_, _, _, opt_limit)| {
            Statement::Call(CallStmt {
                name: Identifier::from_name(None, "fuse_vacuum_temporary_table"),
                args: opt_limit.map(|v| v.1.to_string()).into_iter().collect(),
            })
        },
    );

    let create_file_format = map_res(
        rule! {
            CREATE ~ ( OR ~ ^REPLACE )? ~ FILE ~ FORMAT ~ ( IF ~ ^NOT ~ ^EXISTS )?
            ~ #ident ~ #format_options
        },
        |(_, opt_or_replace, _, _, opt_if_not_exists, name, file_format_options)| {
            let create_option =
                parse_create_option(opt_or_replace.is_some(), opt_if_not_exists.is_some())?;
            Ok(Statement::CreateFileFormat {
                create_option,
                name: name.to_string(),
                file_format_options,
            })
        },
    );

    let drop_file_format = map(
        rule! {
            DROP ~ FILE ~ FORMAT ~ ( IF ~  EXISTS )? ~ #ident
        },
        |(_, _, _, opt_if_exists, name)| Statement::DropFileFormat {
            if_exists: opt_if_exists.is_some(),
            name: name.to_string(),
        },
    );

    let _show_file_formats = value(Statement::ShowFileFormats, rule! { SHOW ~ FILE ~ FORMATS });

    // data mark policy
    let create_data_mask_policy = map(
        rule! {
            CREATE ~ MASKING ~ POLICY ~ ( IF ~ ^NOT ~ ^EXISTS )? ~ #ident ~ #data_mask_policy
        },
        |(_, _, _, opt_if_not_exists, name, policy)| {
            let stmt = CreateDatamaskPolicyStmt {
                if_not_exists: opt_if_not_exists.is_some(),
                name: name.to_string(),
                policy,
            };
            Statement::CreateDatamaskPolicy(stmt)
        },
    );
    let drop_data_mask_policy = map(
        rule! {
            DROP ~ MASKING ~ POLICY ~ ( IF ~ ^EXISTS )? ~ #ident
        },
        |(_, _, _, opt_if_exists, name)| {
            let stmt = DropDatamaskPolicyStmt {
                if_exists: opt_if_exists.is_some(),
                name: name.to_string(),
            };
            Statement::DropDatamaskPolicy(stmt)
        },
    );
    let describe_data_mask_policy = map(
        rule! {
            ( DESC | DESCRIBE ) ~ MASKING ~ POLICY ~ #ident
        },
        |(_, _, _, name)| {
            Statement::DescDatamaskPolicy(DescDatamaskPolicyStmt {
                name: name.to_string(),
            })
        },
    );

    let create_network_policy = map_res(
        rule! {
            CREATE ~  ( OR ~ ^REPLACE )? ~ NETWORK ~ POLICY ~ ( IF ~ ^NOT ~ ^EXISTS )? ~ ^#ident
             ~ ALLOWED_IP_LIST ~ ^Eq ~ ^"(" ~ ^#comma_separated_list0(literal_string) ~ ^")"
             ~ ( BLOCKED_IP_LIST ~ ^Eq ~ ^"(" ~ ^#comma_separated_list0(literal_string) ~ ^")" ) ?
             ~ ( COMMENT ~ ^Eq ~ ^#literal_string)?
        },
        |(
            _,
            opt_or_replace,
            _,
            _,
            opt_if_not_exists,
            name,
            _,
            _,
            _,
            allowed_ip_list,
            _,
            opt_blocked_ip_list,
            opt_comment,
        )| {
            let create_option =
                parse_create_option(opt_or_replace.is_some(), opt_if_not_exists.is_some())?;
            let stmt = CreateNetworkPolicyStmt {
                create_option,
                name: name.to_string(),
                allowed_ip_list,
                blocked_ip_list: match opt_blocked_ip_list {
                    Some(opt) => Some(opt.3),
                    None => None,
                },
                comment: match opt_comment {
                    Some(opt) => Some(opt.2),
                    None => None,
                },
            };
            Ok(Statement::CreateNetworkPolicy(stmt))
        },
    );
    let alter_network_policy = map(
        rule! {
            ALTER ~ NETWORK ~ POLICY ~ ( IF ~ ^EXISTS )? ~ ^#ident ~ SET
             ~ ( ALLOWED_IP_LIST ~ ^Eq ~ ^"(" ~ ^#comma_separated_list0(literal_string) ~ ^")" ) ?
             ~ ( BLOCKED_IP_LIST ~ ^Eq ~ ^"(" ~ ^#comma_separated_list0(literal_string) ~ ^")" ) ?
             ~ ( COMMENT ~ ^Eq ~ ^#literal_string)?
        },
        |(
            _,
            _,
            _,
            opt_if_exists,
            name,
            _,
            opt_allowed_ip_list,
            opt_blocked_ip_list,
            opt_comment,
        )| {
            let stmt = AlterNetworkPolicyStmt {
                if_exists: opt_if_exists.is_some(),
                name: name.to_string(),
                allowed_ip_list: match opt_allowed_ip_list {
                    Some(opt) => Some(opt.3),
                    None => None,
                },
                blocked_ip_list: match opt_blocked_ip_list {
                    Some(opt) => Some(opt.3),
                    None => None,
                },
                comment: match opt_comment {
                    Some(opt) => Some(opt.2),
                    None => None,
                },
            };
            Statement::AlterNetworkPolicy(stmt)
        },
    );
    let drop_network_policy = map(
        rule! {
            DROP ~ NETWORK ~ POLICY ~ ( IF ~ ^EXISTS )? ~ ^#ident
        },
        |(_, _, _, opt_if_exists, name)| {
            let stmt = DropNetworkPolicyStmt {
                if_exists: opt_if_exists.is_some(),
                name: name.to_string(),
            };
            Statement::DropNetworkPolicy(stmt)
        },
    );
    let describe_network_policy = map(
        rule! {
            ( DESC | DESCRIBE ) ~ NETWORK ~ POLICY ~ #ident
        },
        |(_, _, _, name)| {
            Statement::DescNetworkPolicy(DescNetworkPolicyStmt {
                name: name.to_string(),
            })
        },
    );
    let _show_network_policies = value(
        Statement::ShowNetworkPolicies,
        rule! { SHOW ~ NETWORK ~ POLICIES },
    );

    let create_password_policy = map_res(
        rule! {
            CREATE ~ ( OR ~ ^REPLACE )? ~ PASSWORD ~ POLICY ~ ( IF ~ ^NOT ~ ^EXISTS )? ~ ^#ident
             ~ #password_set_options
        },
        |(_, opt_or_replace, _, _, opt_if_not_exists, name, set_options)| {
            let create_option =
                parse_create_option(opt_or_replace.is_some(), opt_if_not_exists.is_some())?;
            let stmt = CreatePasswordPolicyStmt {
                create_option,
                name: name.to_string(),
                set_options,
            };
            Ok(Statement::CreatePasswordPolicy(stmt))
        },
    );
    let alter_password_policy = map(
        rule! {
            ALTER ~ PASSWORD ~ POLICY ~ ( IF ~ ^EXISTS )? ~ ^#ident
             ~ #alter_password_action
        },
        |(_, _, _, opt_if_exists, name, action)| {
            let stmt = AlterPasswordPolicyStmt {
                if_exists: opt_if_exists.is_some(),
                name: name.to_string(),
                action,
            };
            Statement::AlterPasswordPolicy(stmt)
        },
    );
    let drop_password_policy = map(
        rule! {
            DROP ~ PASSWORD ~ POLICY ~ ( IF ~ ^EXISTS )? ~ ^#ident
        },
        |(_, _, _, opt_if_exists, name)| {
            let stmt = DropPasswordPolicyStmt {
                if_exists: opt_if_exists.is_some(),
                name: name.to_string(),
            };
            Statement::DropPasswordPolicy(stmt)
        },
    );
    let describe_password_policy = map(
        rule! {
            ( DESC | DESCRIBE ) ~ PASSWORD ~ POLICY ~ #ident
        },
        |(_, _, _, name)| {
            Statement::DescPasswordPolicy(DescPasswordPolicyStmt {
                name: name.to_string(),
            })
        },
    );
    let _show_password_policies = map(
        rule! {
            SHOW ~ PASSWORD ~ POLICIES ~ ^#show_options?
        },
        |(_, _, _, show_options)| Statement::ShowPasswordPolicies { show_options },
    );

    let create_pipe = map(
        rule! {
            CREATE ~ PIPE ~ ( IF ~ ^NOT ~ ^EXISTS )?
            ~ #ident
            ~ ( AUTO_INGEST ~ "=" ~ #literal_bool )?
            ~ ( (COMMENT | COMMENTS) ~ ^"=" ~ ^#literal_string )?
            ~ AS ~ #copy_stmt
        },
        |(_, _, opt_if_not_exists, pipe, ingest, comment_opt, _, copy_stmt)| {
            let copy_stmt = match copy_stmt {
                Statement::Copy(stmt) => stmt,
                _ => {
                    unreachable!()
                }
            };
            Statement::CreatePipe(CreatePipeStmt {
                if_not_exists: opt_if_not_exists.is_some(),
                name: pipe.to_string(),
                auto_ingest: ingest.map(|v| v.2).unwrap_or_default(),
                comments: comment_opt.map(|v| v.2).unwrap_or_default(),
                copy_stmt,
            })
        },
    );

    let alter_pipe = map(
        rule! {
            ALTER ~ PIPE ~ ( IF ~ ^EXISTS )?
            ~ #ident ~ #alter_pipe_option
        },
        |(_, _, opt_if_exists, task, options)| {
            Statement::AlterPipe(AlterPipeStmt {
                if_exists: opt_if_exists.is_some(),
                name: task.to_string(),
                options,
            })
        },
    );

    let drop_pipe = map(
        rule! {
            DROP ~ PIPE ~ ( IF ~ ^EXISTS )?
            ~ #ident
        },
        |(_, _, opt_if_exists, task)| {
            Statement::DropPipe(DropPipeStmt {
                if_exists: opt_if_exists.is_some(),
                name: task.to_string(),
            })
        },
    );

    let desc_pipe = map(
        rule! {
            ( DESC | DESCRIBE ) ~ PIPE ~ #ident
        },
        |(_, _, task)| {
            Statement::DescribePipe(DescribePipeStmt {
                name: task.to_string(),
            })
        },
    );
    let create_notification = map(
        rule! {
            CREATE ~ NOTIFICATION ~ INTEGRATION
            ~ ( IF ~ ^NOT ~ ^EXISTS )?
            ~ #ident
            ~ TYPE ~ "=" ~ #ident
            ~ ENABLED ~ "=" ~ #literal_bool
            ~ #notification_webhook_clause?
            ~ ( (COMMENT | COMMENTS) ~ ^"=" ~ ^#literal_string )?
        },
        |(
            _,
            _,
            _,
            if_not_exists,
            name,
            _,
            _,
            notification_type,
            _,
            _,
            enabled,
            webhook,
            comment,
        )| {
            Statement::CreateNotification(CreateNotificationStmt {
                if_not_exists: if_not_exists.is_some(),
                name: name.to_string(),
                notification_type: notification_type.to_string(),
                enabled,
                webhook_opts: webhook,
                comments: comment.map(|(_, _, comments)| comments),
            })
        },
    );

    let drop_notification = map(
        rule! {
            DROP ~ NOTIFICATION ~ INTEGRATION ~ ( IF ~ ^EXISTS )?
            ~ #ident
        },
        |(_, _, _, if_exists, name)| {
            Statement::DropNotification(DropNotificationStmt {
                if_exists: if_exists.is_some(),
                name: name.to_string(),
            })
        },
    );

    let alter_notification = map(
        rule! {
            ALTER ~ NOTIFICATION ~ INTEGRATION ~ ( IF ~ ^EXISTS )?
            ~ #ident
            ~ #alter_notification_options
        },
        |(_, _, _, if_exists, name, options)| {
            Statement::AlterNotification(AlterNotificationStmt {
                if_exists: if_exists.is_some(),
                name: name.to_string(),
                options,
            })
        },
    );

    let desc_notification = map(
        rule! {
            ( DESC | DESCRIBE ) ~ NOTIFICATION ~ INTEGRATION ~ #ident
        },
        |(_, _, _, name)| {
            Statement::DescribeNotification(DescribeNotificationStmt {
                name: name.to_string(),
            })
        },
    );

    pub fn procedure_type(i: Input) -> IResult<ProcedureType> {
        map(rule! { #ident ~ #type_name }, |(name, data_type)| {
            ProcedureType {
                name: Some(name.to_string()),
                data_type,
            }
        })
        .parse(i)
    }

    fn procedure_return(i: Input) -> IResult<Vec<ProcedureType>> {
        let procedure_table_return = map(
            rule! {
                TABLE ~ "(" ~ #comma_separated_list1(procedure_type) ~ ")"
            },
            |(_, _, test, _)| test,
        );
        let procedure_single_return = map(rule! { #type_name }, |data_type| {
            vec![ProcedureType {
                name: None,
                data_type,
            }]
        });
        rule!(#procedure_single_return: "<type_name>"
            | #procedure_table_return: "TABLE(<var_name> <type_name>, ...)")
        .parse(i)
    }

    fn procedure_arg(i: Input) -> IResult<Option<Vec<ProcedureType>>> {
        let procedure_args = map(
            rule! {
                "(" ~ #comma_separated_list1(procedure_type) ~ ")"
            },
            |(_, args, _)| Some(args),
        );
        let procedure_empty_args = map(
            rule! {
                "(" ~ ")"
            },
            |(_, _)| None,
        );
        rule!(#procedure_empty_args: "()"
            | #procedure_args: "(<var_name> <type_name>, ...)")
        .parse(i)
    }

    // CREATE [ OR REPLACE ] PROCEDURE <name> ()
    // RETURNS { <result_data_type> }[ NOT NULL ]
    // LANGUAGE SQL
    // [ COMMENT = '<string_literal>' ] AS <procedure_definition>
    let create_procedure = map_res(
        rule! {
            CREATE ~ ( OR ~ ^REPLACE )? ~ PROCEDURE ~ ( IF ~ ^NOT ~ ^EXISTS )? ~ #ident ~ #procedure_arg ~ RETURNS ~ #procedure_return ~ LANGUAGE ~ SQL  ~ (COMMENT ~ "=" ~ #literal_string)? ~ AS ~ #code_string
        },
        |(
            _,
            opt_or_replace,
            _,
            opt_if_not_exists,
            name,
            args,
            _,
            return_type,
            _,
            _,
            opt_comment,
            _,
            script,
        )| {
            let create_option =
                parse_create_option(opt_or_replace.is_some(), opt_if_not_exists.is_some())?;

            let name = ProcedureIdentity {
                name: name.to_string(),
                args_type: if let Some(args) = &args {
                    args.iter().map(|arg| arg.data_type.clone()).collect()
                } else {
                    vec![]
                },
            };
            let stmt = CreateProcedureStmt {
                create_option,
                name,
                args,
                return_type,
                language: ProcedureLanguage::SQL,
                comment: match opt_comment {
                    Some(opt) => Some(opt.2),
                    None => None,
                },
                script,
            };
            Ok(Statement::CreateProcedure(stmt))
        },
    );

    let _show_procedures = map(
        rule! {
            SHOW ~ PROCEDURES ~ #show_options?
        },
        |(_, _, show_options)| Statement::ShowProcedures { show_options },
    );

    // fn procedure_type_name(i: Input) -> IResult<Vec<TypeName>> {
    // let procedure_type_names = map(
    // rule! {
    // "(" ~ #comma_separated_list1(type_name) ~ ")"
    // },
    // |(_, args, _)| args,
    // );
    // let procedure_empty_types = map(
    // rule! {
    // "(" ~ ")"
    // },
    // |(_, _)| vec![],
    // );
    // rule!(#procedure_empty_types: "()"
    // | #procedure_type_names: "(<type_name>, ...)").parse(i)
    // }

    let _call_procedure = map(
        rule! {
            CALL ~ PROCEDURE ~ #ident ~ "(" ~ #comma_separated_list0(subexpr(0))? ~ ")"
        },
        |(_, _, name, _, opt_args, _)| {
            Statement::CallProcedure(CallProcedureStmt {
                name,
                args: opt_args.unwrap_or_default(),
            })
        },
    );

    let drop_procedure = map(
        rule! {
            DROP ~ PROCEDURE ~ ( IF ~ ^EXISTS )? ~ #ident ~ #procedure_type_name
        },
        |(_, _, opt_if_exists, name, args)| {
            Statement::DropProcedure(DropProcedureStmt {
                if_exists: opt_if_exists.is_some(),
                name: ProcedureIdentity {
                    name: name.to_string(),
                    args_type: args,
                },
            })
        },
    );

    let describe_procedure = map(
        rule! {
            ( DESC | DESCRIBE ) ~ PROCEDURE ~ #ident ~ #procedure_type_name
        },
        |(_, _, name, args)| {
            Statement::DescProcedure(DescProcedureStmt {
                name: ProcedureIdentity {
                    name: name.to_string(),
                    args_type: args,
                },
            })
        },
    );

    try_dispatch!(i, false,
        SELECT | VALUES => query_statement(i),
        WITH => rule!(
            #dml_delete
            | #dml_update
            | #dml_insert_stmt(false, false)
            | #query_statement
        ).parse(i),
        HintPrefix | LParen | FROM => query_statement(i),
        EXPLAIN => rule!(
            #explain : "`EXPLAIN [PIPELINE | GRAPH] <statement>`"
            | #explain_analyze : "`EXPLAIN ANALYZE <statement>`"
        ).parse(i),
        REPORT => rule!(#report: "`REPORT ISSUE <statement>`").parse(i),
        SETTINGS => rule!(#query_setting : "SETTINGS ( {<name> = <value> | (<name>, ...) = (<value>, ...)} )  Statement").parse(i),
        SHOW => rule!(
            #session_show_settings
            | #session_show_variables
            | #schema_show_databases
            | #schema_show_schemas
            | #table_show_tables
            | #show_stmt
        ).parse(i),
        USE => rule!(
            #session_use_warehouse: "`USE WAREHOUSE <warehouse>`"
            | #schema_use_database_stmt: "`USE DATABASE <database>`"
            | #schema_use_schema: "`USE <schema>`"
        ).parse(i),
        KILL => rule!(#utility_kill_stmt : "`KILL (QUERY | CONNECTION) <object_id>`").parse(i),
        SET => rule!(
            #utility_set_priority: "`SET PRIORITY (HIGH | MEDIUM | LOW) <object_id>`"
            | #transaction_stmt: "`SET TRANSACTION <transaction_mode> [, ...] | SET SESSION CHARACTERISTICS AS TRANSACTION <transaction_mode> [, ...]`"
            | #session_set_role: "`SET [DEFAULT] ROLE <role>`"
            | #session_set_secondary_roles: "`SET SECONDARY ROLES (ALL | NONE)`"
            | #session_set_secondary_specify_roles: "`SET SECONDARY ROLES [role_name,...]`"
            | #schema_use_database_stmt: "`SET DATABASE <database>`"
            | #session_set_stmt : "`SET [variable] {<name> = <value> | (<name>, ...) = (<value>, ...)}`"
        ).parse(i),
        UNSET => rule!(#session_unset_stmt : "`UNSET [variable] {<name> | (<name>, ...)}`").parse(i),
        RESET => rule!(#session_reset_stmt : "`RESET [variable] {<name> | (<name>, ...)}`").parse(i),
        BEGIN | COMMIT | ABORT | ROLLBACK | SAVEPOINT | RELEASE | START => rule!(
            #transaction_stmt : "`BEGIN | START TRANSACTION | COMMIT | ABORT | ROLLBACK | SAVEPOINT | RELEASE | ROLLBACK TO | PREPARE TRANSACTION | COMMIT PREPARED | ROLLBACK PREPARED`"
        ).parse(i),
        PREPARE => rule!(
            #transaction_stmt : "`PREPARE TRANSACTION`"
            | #prepare_stmt : "`PREPARE <name> [(<type>, ...)] AS <query>`"
        ).parse(i),
        DEALLOCATE => rule!(#deallocate_stmt : "`DEALLOCATE [PREPARE] <name> | DEALLOCATE [PREPARE] ALL`").parse(i),
        DECLARE => rule!(#declare_cursor_stmt : "`DECLARE <name> [SCROLL | NO SCROLL] CURSOR [WITH HOLD] FOR <query>`").parse(i),
        FETCH => rule!(#fetch_stmt : "`FETCH <direction> FROM <cursor>`").parse(i),
        MOVE => rule!(#move_stmt : "`MOVE <direction> FROM <cursor>`").parse(i),
        CLOSE => rule!(#close_cursor_stmt : "`CLOSE <name> | CLOSE ALL`").parse(i),
        DISCARD => rule!(#discard_stmt : "`DISCARD {ALL | TEMP | TEMPORARY | PLANS | SEQUENCES}`").parse(i),
        CHECKPOINT => rule!(#checkpoint_stmt : "`CHECKPOINT`").parse(i),
        SYSTEM => rule!(#utility_system_action: "`SYSTEM (ENABLE | DISABLE) EXCEPTION_BACKTRACE`"
            ).parse(i),
        MERGE => rule!(#dml_merge : "`MERGE INTO <target_table> USING <source> ON <join_expr> { matchedClause | notMatchedClause } [ ... ]`"
            ).parse(i),
        DELETE => rule!(#dml_delete : "`DELETE FROM <table> [WHERE ...]`"
            ).parse(i),
        UPDATE => rule!(#dml_update : "`UPDATE <table> SET <column> = <expr> [, <column> = <expr> , ... ] [WHERE ...]`"
            ).parse(i),
        INSERT => rule!(
            #dml_conditional_multi_table_insert() : "`INSERT [OVERWRITE] {FIRST|ALL} { WHEN <condition> THEN intoClause [ ... ] } [ ... ] [ ELSE intoClause ] <subquery>`"
            | #dml_unconditional_multi_table_insert() : "`INSERT [OVERWRITE] ALL intoClause [ ... ] <subquery>`"
            | #dml_insert_stmt(false, false) : "`INSERT INTO [TABLE] <table> [(<column>, ...)] (VALUES <values> | <query>)`"
        ).parse(i),
        REPLACE => rule!(#dml_replace_stmt(false) : "`REPLACE INTO [TABLE] <table> [(<column>, ...)] (FORMAT <format> | VALUES <values> | <query>)`"
            ).parse(i),
        COPY => rule!(#copy_stmt).parse(i),
        TRUNCATE => rule!(#table_truncate_table : "`TRUNCATE TABLE [<database>.]<table>`"
            ).parse(i),
        OPTIMIZE => rule!(#table_optimize_table : "`OPTIMIZE TABLE [<database>.]<table>`"
            ).parse(i),
        VACUUM => rule!(
            #table_vacuum_temp_files : "VACUUM TEMPORARY FILES [RETAIN number SECONDS|DAYS] [LIMIT number]"
            | #table_vacuum_table : "`VACUUM TABLE [<database>.]<table> [RETAIN number HOURS] [DRY RUN | DRY RUN SUMMARY]`"
            | #table_vacuum_drop_table : "`VACUUM DROP TABLE [FROM [<database>.]<schema>] [RETAIN number HOURS] [DRY RUN | DRY RUN SUMMARY]`"
            | #vacuum_temporary_tables
        ).parse(i),
        ANALYZE => rule!(#table_analyze_table : "`ANALYZE TABLE [<database>.]<table>`"
            ).parse(i),
        EXISTS => rule!(#table_exists_table : "`EXISTS TABLE [<database>.]<table>`"
            ).parse(i),
        UNDROP => rule!(
            #schema_undrop_schema : "`UNDROP SCHEMA <schema>`"
            | #table_undrop_table : "`UNDROP TABLE [<database>.]<table>`"
        ).parse(i),
        ATTACH => rule!(#attach_table : "`ATTACH TABLE [<database>.]<table> <uri>`"
            ).parse(i),
        REFRESH => rule!(
            #refresh_aggregating_index: "`REFRESH AGGREGATING INDEX <index> [LIMIT <limit>]`"
            | #refresh_index_on_table: "`REFRESH <index_type> INDEX <index> ON [<database>.]<table> [LIMIT <limit>]`"
            | #refresh_virtual_column: "`REFRESH VIRTUAL COLUMN FOR [<database>.]<table>`"
            | #refresh_property_graph: "`REFRESH PROPERTY GRAPH <graph_name>`"
        ).parse(i),
        LIST => rule!(#list_stage: "`LIST @<stage_name> [pattern = '<pattern>']`"
            ).parse(i),
        REMOVE => rule!(#remove_stage: "`REMOVE @<stage_name> [pattern = '<pattern>']`"
            ).parse(i),
        PRESIGN => rule!(#utility_presign: "`PRESIGN [{DOWNLOAD | UPLOAD}] <location> [EXPIRE = 3600]`"
            ).parse(i),
        EXECUTE => rule!(
            #execute_task: "`EXECUTE TASK <name>`"
            | #utility_execute_immediate : "`EXECUTE IMMEDIATE $$ <script> $$`"
            | #execute_prepared_stmt : "`EXECUTE <statement_name> [(<arg>, ...)]`"
        ).parse(i),
        GRANT => rule!(
            #acl_grant : "`GRANT { ROLE <role_name> | schemaObjectPrivileges | ALL [ PRIVILEGES ] ON <privileges_level> } TO { [ROLE <role_name>] | [USER] <user> }`"
            | #acl_grant_ownership : "GRANT OWNERSHIP ON <privileges_level> TO ROLE <role_name>"
        ).parse(i),
        REVOKE => rule!(#acl_revoke : "`REVOKE { ROLE <role_name> | schemaObjectPrivileges | ALL [ PRIVILEGES ] ON <privileges_level> } FROM { [ROLE <role_name>] | [USER] <user> }`"
            ).parse(i),
        COMMENT => rule!(#comment).parse(i),
        DESC | DESCRIBE => rule!(
            #desc_task : "`DESC | DESCRIBE TASK <name>`"
            | #describe_view : "`DESCRIBE VIEW [<database>.]<view>`"
            | #acl_describe_user: "`DESCRIBE USER <user_name>`"
            | #describe_row_access : "`DESC[RIBE] ROW ACCESS POLICY <name>`"
            | #desc_stage: "`DESC STAGE <stage_name>`"
            | #describe_data_mask_policy: "`DESC MASKING POLICY mask_name`"
            | #describe_network_policy: "`DESC NETWORK POLICY name`"
            | #describe_password_policy: "`DESC PASSWORD POLICY name`"
            | #desc_pipe : "`DESC | DESCRIBE PIPE <name>`"
            | #desc_notification : "`DESC | DESCRIBE NOTIFICATION INTEGRATION <name>`"
            | #desc_connection: "`DESC | DESCRIBE CONNECTION  <connection_name>`"
            | #describe_procedure : "`DESC PROCEDURE <procedure_name>()`"
            | #describe_stream : "`DESCRIBE STREAM [<database>.]<stream>`"
            | #describe_table : "`DESCRIBE [<database>.]<table>`"
            | #sequence
        ).parse(i),
        CREATE => rule!(
            (
                #create_task : "`CREATE TASK [ IF NOT EXISTS ] <name>
  [ { WAREHOUSE = <string> } ]
  [ SCHEDULE = { <num> MINUTE | USING CRON <expr> <time_zone> } ]
  [ AFTER <string>, <string>...]
  [ WHEN boolean_expr ]
  [ SUSPEND_TASK_AFTER_NUM_FAILURES = <num> ]
  [ ERROR_INTEGRATION = <string_literal> ]
  [ COMMENT = '<string_literal>' ]
AS
  <sql>`"
                | #schema_create_database: "`CREATE DATABASE [IF NOT EXISTS] <database>`"
                | #create_warehouse: "`CREATE WAREHOUSE <warehouse> [(ASSIGN <node_size> NODES [FROM <node_group>] [, ...])] WITH [warehouse_size = <warehouse_size>]`"
                | #create_workload_group: "`CREATE WORKLOAD GROUP [IF NOT EXISTS] <name> WITH [<workload_group_quotas>]`"
                | #schema_create_schema : "`CREATE [OR REPLACE] SCHEMA [IF NOT EXISTS] <schema> [ENGINE = <engine>]`"
                | #table_create_table : "`CREATE [OR REPLACE] TABLE [IF NOT EXISTS] [<database>.]<table> [<source>] [<table_options>]`"
                | #create_dictionary : "`CREATE [OR REPLACE] DICTIONARY [IF NOT EXISTS] <dictionary_name> [(<column>, ...)] PRIMARY KEY [<primary_key>, ...] SOURCE (<source_name> ([<source_options>])) [COMMENT <comment>] `"
                | #create_view : "`CREATE [OR REPLACE] VIEW [IF NOT EXISTS] [<database>.]<view> [(<column>, ...)] AS SELECT ...`"
                | #create_aggregating_index: "`CREATE [OR REPLACE] AGGREGATING INDEX [IF NOT EXISTS] <index> AS SELECT ...`"
                | #create_index_using: "`CREATE [OR REPLACE] INDEX [IF NOT EXISTS] <index> ON [<database>.]<table> USING <method> (<expr>, ...)`"
                | #create_default_index: "`CREATE [OR REPLACE] [UNIQUE] INDEX [IF NOT EXISTS] <index> ON [<database>.]<table> (<column>, ...)`"
                | #create_index: "`CREATE [OR REPLACE] <index_type> INDEX [IF NOT EXISTS] <index> ON [<database>.]<table>(<column>, ...)`"
            )
            | (
                #create_stage: "`CREATE [OR REPLACE] STAGE [ IF NOT EXISTS ] <stage_name>
                [ FILE_FORMAT = ( { TYPE = { CSV | PARQUET } [ formatTypeOptions ] ) } ]
                [ COPY_OPTIONS = ( copyOptions ) ]
                [ COMMENT = '<string_literal>' ]`"
                | #create_tag: "`CREATE TAG [IF NOT EXISTS] <tag_name> [ALLOWED_VALUES = ('v1', ...)] [COMMENT = '<comment>']`"
                | #create_file_format: "`CREATE FILE FORMAT [ IF NOT EXISTS ] <format_name> formatTypeOptions`"
                | #create_pipe : "`CREATE PIPE [ IF NOT EXISTS ] <name>
  [ AUTO_INGEST = [ TRUE | FALSE ] ]
  [ COMMENT = '<string_literal>' ]
AS
  <copy_sql>`"
                | #create_notification : "`CREATE NOTIFICATION INTEGRATION [ IF NOT EXISTS ] <name>
    TYPE = <type>
    ENABLED = <bool>
    [ WEBHOOK = ( url = <string_literal>, method = <string_literal>, authorization_header = <string_literal> ) ]
    [ COMMENT = '<string_literal>' ]`"
                | #create_connection: "`CREATE [OR REPLACE] CONNECTION [IF NOT EXISTS] <connection_name> STORAGE_TYPE = <type> <storage_configs>`"
                | #create_row_access : "`CREATE ROW ACCESS POLICY [ IF NOT EXISTS ] <name> AS ( <arg_name> <arg_type> [ , ... ] ) RETURNS BOOLEAN -> <body> [ COMMENT = '<string_literal>' ]`"
                | #acl_create_user : "`CREATE [OR REPLACE] USER [IF NOT EXISTS] '<username>' IDENTIFIED [WITH <auth_type>] [BY <password>] [WITH <user_option>, ...]`"
                | #acl_create_role : "`CREATE ROLE [IF NOT EXISTS] <role_name> [COMMENT ='<string_literal>']`"
                | #create_function : "`CREATE [OR REPLACE] FUNCTION [IF NOT EXISTS] [<database>.][<schema>.]<name>(<arg_name arg_type>, ...) RETURNS <type> LANGUAGE <language> [IMMUTABLE|STABLE|VOLATILE] [STRICT] [SECURITY INVOKER|DEFINER] [HANDLER '<handler>'] [PACKAGES ('pkg', ...)] [IMPORTS ('uri', ...)] [ROWS <n>] [CAPABILITY PROFILE <profile>] AS <code>`"
                | #create_data_mask_policy: "`CREATE MASKING POLICY [IF NOT EXISTS] mask_name as (val1 val_type1 [, val type]) return type -> case`"
                | #create_network_policy: "`CREATE NETWORK POLICY [IF NOT EXISTS] name ALLOWED_IP_LIST = ('ip1' [, 'ip2']) [BLOCKED_IP_LIST = ('ip1' [, 'ip2'])] [COMMENT = '<string_literal>']`"
                | #create_password_policy: "`CREATE PASSWORD POLICY [IF NOT EXISTS] name [PASSWORD_MIN_LENGTH = <u64_literal>] ... [COMMENT = '<string_literal>']`"
                | #create_procedure : "`CREATE [ OR REPLACE ] PROCEDURE <procedure_name>() RETURNS { <result_data_type> [ NOT NULL ] | TABLE(<var_name> <data_type>, ...)} LANGUAGE SQL [ COMMENT = '<string_literal>' ] AS <procedure_definition>`"
            )
            | (
                #dynamic_table
                | #create_stream: "`CREATE [OR REPLACE] STREAM [IF NOT EXISTS] [<database>.]<stream> ON TABLE [<database>.]<table> [<travel_point>] [COMMENT = '<string_literal>']`"
                | #create_property_graph: "`CREATE PROPERTY GRAPH [IF NOT EXISTS] <graph_name> VERTEX TABLES (...) EDGE TABLES (...)`"
                | #sequence
            )
        ).parse(i),
        DROP => rule!(
            (
                #drop_task : "`DROP TASK [ IF EXISTS ] <name>`"
                | #schema_drop_database: "`DROP DATABASE [IF EXISTS] <database>`"
                | #drop_warehouse: "`DROP WAREHOUSE <warehouse>`"
                | #drop_warehouse_cluster: "`ALTER WAREHOUSE <warehouse> DROP CLUSTER <cluster>`"
                | #drop_workload_group: "`DROP WORKLOAD GROUP [ IF EXISTS ] <name>`"
                | #schema_drop_schema : "`DROP SCHEMA [IF EXISTS] <schema>`"
                | #table_drop_table : "`DROP TABLE [IF EXISTS] [<database>.]<table>`"
                | #drop_dictionary : "`DROP DICTIONARY [IF EXISTS] <dictionary_name>`"
                | #drop_view : "`DROP VIEW [IF EXISTS] [<database>.]<view>`"
                | #drop_index: "`DROP INDEX [IF EXISTS] [<database>.][<schema>.]<index>`"
                | #drop_index_on_table: "`DROP <index_type> INDEX [IF EXISTS] <index> ON [<database>.]<table>`"
            )
            | (
                #drop_stage: "`DROP STAGE <stage_name>`"
                | #drop_tag: "`DROP TAG [IF EXISTS] <tag_name>`"
                | #drop_file_format: "`DROP FILE FORMAT  [ IF EXISTS ] <format_name>`"
                | #drop_pipe : "`DROP PIPE [ IF EXISTS ] <name>`"
                | #drop_notification : "`DROP NOTIFICATION INTEGRATION [ IF EXISTS ] <name>`"
                | #drop_connection: "`DROP CONNECTION [IF EXISTS] <connection_name>`"
                | #drop_row_access : "`DROP ROW ACCESS POLICY [ IF EXISTS ] <name>`"
                | #acl_drop_user : "`DROP USER [IF EXISTS] '<username>'`"
                | #acl_drop_role : "`DROP ROLE [IF EXISTS] <role_name>`"
                | #drop_function : "`DROP FUNCTION [IF EXISTS] [<database>.][<schema>.]<name>(<arg_type>, ...)`"
                | #drop_data_mask_policy: "`DROP MASKING POLICY [IF EXISTS] mask_name`"
                | #drop_network_policy: "`DROP NETWORK POLICY [IF EXISTS] name`"
                | #drop_password_policy: "`DROP PASSWORD POLICY [IF EXISTS] name`"
                | #drop_procedure : "`DROP PROCEDURE <procedure_name>()`"
            )
            | (
                #drop_stream: "`DROP STREAM [IF EXISTS] [<database>.]<stream>`"
                | #drop_property_graph: "`DROP PROPERTY GRAPH [IF EXISTS] <graph_name>`"
                | #sequence
            )
        ).parse(i),
        ALTER => rule!(
            #alter_task : "`ALTER TASK [ IF EXISTS ] <name> SUSPEND | RESUME | SET <option> = <value>` | UNSET <option> | MODIFY AS <sql> | MODIFY WHEN <boolean_expr> | ADD/REMOVE AFTER <string>, <string>...`"
            | #add_warehouse_cluster: "`ALTER WAREHOUSE <warehouse> ADD CLUSTER <cluster> [(ASSIGN <node_size> NODES [FROM <node_group>] [, ...])] WITH [cluster_size = <cluster_size>]`"
            | #drop_warehouse_cluster: "`ALTER WAREHOUSE <warehouse> DROP CLUSTER <cluster>`"
            | #rename_warehouse_cluster: "`ALTER WAREHOUSE <warehouse> RENAME CLUSTER <cluster> TO <new_cluster>`"
            | #assign_warehouse_nodes: "`ALTER WAREHOUSE <warehouse> ASSIGN NODES ( ASSIGN <node_size> NODES [FROM <node_group>] FOR <cluster> [, ...] )`"
            | #unassign_warehouse_nodes: "`ALTER WAREHOUSE <warehouse> UNASSIGN NODES ( UNASSIGN <node_size> NODES [FROM <node_group>] FOR <cluster> [, ...] )`"
            | #set_workload_group_quotas: "`ALTER WORKLOAD GROUP <name> SET [<workload_group_quotas>]`"
            | #unset_workload_group_quotas: "`ALTER WORKLOAD GROUP <name> UNSET {<name> | (<name>, ...)}`"
            | #schema_alter_schema : "`ALTER SCHEMA [IF EXISTS] <action>`"
            | #table_alter_table : "`ALTER TABLE [<database>.]<table> <action>`"
            | #alter_view : "`ALTER VIEW [<database>.]<view> [(<column>, ...)] AS SELECT ...`"
            | #acl_alter_user : "`ALTER USER ('<username>' | USER()) [IDENTIFIED [WITH <auth_type>] [BY <password>]] [WITH <user_option>, ...]`"
            | #acl_alter_role : "`ALTER ROLE [IF EXISTS] <role_name> SET COMMENT = '<string_literal>' | UNSET COMMENT`"
            | #alter_network_policy: "`ALTER NETWORK POLICY [IF EXISTS] name SET [ALLOWED_IP_LIST = ('ip1' [, 'ip2'])] [BLOCKED_IP_LIST = ('ip1' [, 'ip2'])] [COMMENT = '<string_literal>']`"
            | #alter_password_policy: "`ALTER PASSWORD POLICY [IF EXISTS] name SET [PASSWORD_MIN_LENGTH = <u64_literal>] ... [COMMENT = '<string_literal>']`"
            | #alter_pipe : "`ALTER PIPE [ IF EXISTS ] <name> SET <option> = <value>` | REFRESH <option> = <value>`"
            | #alter_notification : "`ALTER NOTIFICATION INTEGRATION [ IF EXISTS ] <name> SET <option> = <value>`"
        ).parse(i),
        RENAME => rule!(
            #rename_warehouse: "`RENAME WAREHOUSE <warehouse> TO <new_warehouse>`"
            | #rename_workload_group: "`RENAME WORKLOAD GROUP <old_name> TO <new_name>`"
            | #table_rename_table : "`RENAME TABLE [<database>.]<table> TO <new_table>`"
            | #rename_dictionary: "`RENAME DICTIONARY [<database>.]<old_dict_name> TO <new_dict_name>`"
        ).parse(i),
        RESUME => rule!(#resume_warehouse: "`RESUME WAREHOUSE <warehouse>`"
            ).parse(i),
        SUSPEND => rule!(#suspend_warehouse: "`SUSPEND WAREHOUSE <warehouse>`"
            ).parse(i),
        INSPECT => rule!(#inspect_warehouse: "`INSPECT WAREHOUSE <warehouse>`"
            ).parse(i),
    );
    Err(nom::Err::Error(Error::from_error_kind(
        i,
        ErrorKind::Other("expecting SQL statement"),
    )))
}

pub fn statement_body_with_format(i: Input) -> IResult<StatementWithFormat> {
    map(
        rule! {
            #statement_body ~ ( FORMAT ~ ^#ident )?
        },
        |(stmt, opt_format)| StatementWithFormat {
            stmt,
            format: opt_format.map(|(_, format)| format.name),
        },
    )
    .parse(i)
}

pub fn statement(i: Input) -> IResult<StatementWithFormat> {
    map(
        rule! {
            #statement_body_with_format ~ ";"? ~ &EOI
        },
        |(stmt, _, _)| stmt,
    )
    .parse(i)
}

pub fn parse_create_option(
    opt_or_replace: bool,
    opt_if_not_exists: bool,
) -> Result<CreateOption, nom::Err<ErrorKind>> {
    match (opt_or_replace, opt_if_not_exists) {
        (false, false) => Ok(CreateOption::Create),
        (true, false) => Ok(CreateOption::CreateOrReplace),
        (false, true) => Ok(CreateOption::CreateIfNotExists),
        (true, true) => Err(nom::Err::Failure(ErrorKind::Other(
            "option IF NOT EXISTS and OR REPLACE are incompatible.",
        ))),
    }
}

pub fn unset_source(i: Input) -> IResult<Vec<Identifier>> {
    //#ident ~ ( "(" ~ ^#comma_separated_list1(ident) ~ ")")?
    let var = map(
        rule! {
            #ident
        },
        |variable| vec![variable],
    );
    let vars = map(
        rule! {
            "(" ~ ^#comma_separated_list1(ident) ~ ")"
        },
        |(_, variables, _)| variables,
    );

    rule!(
        #var
        | #vars
    )
    .parse(i)
}

pub fn query_setting(i: Input) -> IResult<(Identifier, Expr)> {
    map(
        rule! {
            #ident ~ "=" ~ #subexpr(0)
        },
        |(id, _, value)| (id, value),
    )
    .parse(i)
}

pub fn query_statement_setting(i: Input) -> IResult<Settings> {
    let query_set = map(
        rule! {
            "(" ~ #comma_separated_list0(query_setting) ~ ")"
        },
        |(_, query_setting, _)| {
            let mut ids = Vec::with_capacity(query_setting.len());
            let mut values = Vec::with_capacity(query_setting.len());
            for (id, value) in query_setting {
                ids.push(id);
                values.push(value);
            }
            Settings {
                set_type: SetType::SettingsQuery,
                identifiers: ids,
                values: SetValues::Expr(values.into_iter().map(|x| x.into()).collect()),
            }
        },
    );
    rule!(#query_set: "(SETTING_NAME = VALUE, ...)").parse(i)
}
pub fn rest_str(i: Input) -> IResult<(String, usize)> {
    let first_token = i.tokens.first().unwrap();
    let mut last_idx = i.tokens.len() - 1;
    let mut found_semi = false;
    for (idx, token) in i.tokens.iter().enumerate() {
        if token.kind == TokenKind::SemiColon {
            last_idx = idx;
            found_semi = true;
            break;
        }
        if token.kind == TokenKind::EOI {
            last_idx = idx;
            break;
        }
    }

    let text = if found_semi {
        let last_token = &i.tokens[last_idx];
        first_token.source[first_token.span.start()..last_token.span.start()].to_string()
    } else {
        let last_token = &i.tokens[last_idx];
        first_token.source[first_token.span.start()..last_token.span.end()].to_string()
    };

    Ok((i.slice(last_idx..), (text, first_token.span.start())))
}

pub fn column_def(i: Input) -> IResult<ColumnDefinition> {
    #[derive(Clone)]
    enum ColumnConstraint {
        Nullable(bool),
        DefaultExpr(Box<Expr>),
        VirtualExpr(Box<Expr>),
        StoredExpr(Box<Expr>),
        CheckExpr(Box<Expr>),
        PrimaryKey,
        AutoIncrement {
            start: u64,
            step: i64,
            is_ordered: bool,
        },
    }

    let nullable = alt((
        value(ColumnConstraint::Nullable(true), rule! { NULL }),
        value(ColumnConstraint::Nullable(false), rule! { NOT ~ ^NULL }),
    ));
    let primary_key = value(ColumnConstraint::PrimaryKey, rule! { PRIMARY ~ ^KEY });
    let identity_params = alt((
        map(
            rule! {
                "(" ~ ^#literal_u64 ~ ^"," ~ ^#literal_i64 ~ ^")"
            },
            |(_, start, _, step, _)| (start, step),
        ),
        map(
            rule! {
                START ~ ^#literal_u64 ~ ^INCREMENT ~ ^#literal_i64
            },
            |(_, start, _, step)| (start, step),
        ),
    ));

    let expr = alt((
        map(
            rule! {
                DEFAULT ~ ^#subexpr(NOT_PREC)
            },
            |(_, default_expr)| ColumnConstraint::DefaultExpr(Box::new(default_expr)),
        ),
        map(
            rule! {
                (GENERATED ~ ^ALWAYS)? ~ AS ~ ^"(" ~ ^#subexpr(NOT_PREC) ~ ^")" ~ VIRTUAL
            },
            |(_, _, _, virtual_expr, _, _)| ColumnConstraint::VirtualExpr(Box::new(virtual_expr)),
        ),
        map(
            rule! {
                (GENERATED ~ ^ALWAYS)? ~ AS ~ ^"(" ~ ^#subexpr(NOT_PREC) ~ ^")" ~ STORED
            },
            |(_, _, _, stored_expr, _, _)| ColumnConstraint::StoredExpr(Box::new(stored_expr)),
        ),
        map(
            rule! {
                CHECK ~ ^"(" ~ ^#subexpr(NOT_PREC) ~ ^")"
            },
            |(_, _, expr, _)| ColumnConstraint::CheckExpr(Box::new(expr)),
        ),
        map(
            rule! {
                (AUTOINCREMENT | IDENTITY)
                ~ #identity_params?
                ~ (ORDER | NOORDER)?
            },
            |(_, params, order_token)| {
                let (start, step) = params.unwrap_or((0, 1));
                let is_ordered = order_token
                    .map(|token| token.text().eq_ignore_ascii_case("order"))
                    .unwrap_or(true);

                ColumnConstraint::AutoIncrement {
                    start,
                    step,
                    is_ordered,
                }
            },
        ),
    ));

    let comment = map(
        rule! {
            COMMENT ~ #literal_string
        },
        |(_, comment)| comment,
    );

    let (i, (mut def, constraints)) = map(
        rule! {
            #ident
            ~ #type_name
            ~ ( #nullable | #primary_key | #expr )*
            ~ ( #comment )?
            : "`<column name> <type> [PRIMARY KEY] [DEFAULT <expr>] [AS (<expr>) VIRTUAL] [AS (<expr>) STORED] [CHECK (<expr>)] [COMMENT '<comment>']`"
        },
        |(name, data_type, constraints, comment)| {
            let def = ColumnDefinition {
                name,
                data_type,
                expr: None,
                is_primary_key: false,
                check: None,
                comment,
            };
            (def, constraints)
        },
    ).parse(i)?;

    for constraint in constraints {
        match constraint {
            ColumnConstraint::Nullable(nullable) => {
                if (nullable && matches!(def.data_type, TypeName::NotNull(_)))
                    || (!nullable && matches!(def.data_type, TypeName::Nullable(_)))
                {
                    return Err(nom::Err::Error(Error::from_error_kind(
                        i,
                        ErrorKind::Other("ambiguous NOT NULL constraint"),
                    )));
                }
                if nullable {
                    def.data_type = def.data_type.wrap_nullable();
                } else {
                    def.data_type = def.data_type.wrap_not_null();
                }
            }
            ColumnConstraint::PrimaryKey => {
                def.is_primary_key = true;
            }
            ColumnConstraint::DefaultExpr(default_expr) => {
                if matches!(def.expr, Some(ColumnExpr::AutoIncrement { .. })) {
                    return Err(nom::Err::Error(Error::from_error_kind(
                        i,
                        ErrorKind::Other(
                            "DEFAULT and AUTO INCREMENT cannot exist at the same time",
                        ),
                    )));
                }
                def.expr = Some(ColumnExpr::Default(default_expr))
            }
            ColumnConstraint::VirtualExpr(virtual_expr) => {
                def.expr = Some(ColumnExpr::Virtual(virtual_expr))
            }
            ColumnConstraint::StoredExpr(stored_expr) => {
                def.expr = Some(ColumnExpr::Stored(stored_expr))
            }
            ColumnConstraint::CheckExpr(check) => def.check = Some(*check),
            ColumnConstraint::AutoIncrement {
                start,
                step,
                is_ordered,
            } => {
                if matches!(def.expr, Some(ColumnExpr::Default(_))) {
                    return Err(nom::Err::Error(Error::from_error_kind(
                        i,
                        ErrorKind::Other("DEFAULT and AUTOINCREMENT cannot exist at the same time"),
                    )));
                }
                def.expr = Some(ColumnExpr::AutoIncrement {
                    start,
                    step,
                    is_ordered,
                })
            }
        }
    }

    Ok((i, def))
}

pub fn table_index_def(i: Input) -> IResult<TableIndexDefinition> {
    map_res(
        rule! {
            ASYNC?
            ~ #index_type ~ ^INDEX
            ~ #ident
            ~ ^"(" ~ ^#comma_separated_list1(ident) ~ ^")"
            ~ ( #table_option )?
        },
        |(opt_async, index_type, _, index_name, _, columns, _, opt_index_options)| {
            Ok(TableIndexDefinition {
                index_name,
                index_type,
                columns,
                sync_creation: opt_async.is_none(),
                index_options: opt_index_options.unwrap_or_default(),
            })
        },
    )(i)
}

pub fn constraint_def(i: Input) -> IResult<ConstraintDefinition> {
    let check_constraint = map(
        rule! {
            CHECK ~ ^"(" ~ ^#expr ~ ^")"
        },
        |(_, _, expr, _)| ConstraintType::Check(expr),
    );
    let primary_key_constraint = map(
        rule! {
            PRIMARY ~ ^KEY ~ ^"(" ~ ^#comma_separated_list1(ident) ~ ^")"
        },
        |(_, _, _, columns, _)| ConstraintType::PrimaryKey(columns),
    );
    let unique_not_enforced_constraint = map(
        rule! {
            UNIQUE ~ ^"(" ~ ^#comma_separated_list1(ident) ~ ^")" ~ ^NOT ~ ^ENFORCED
        },
        |(_, _, columns, _, _, _)| ConstraintType::UniqueNotEnforced(columns),
    );

    map(
        rule! {
            (CONSTRAINT ~ #ident)?
            ~ ( #check_constraint | #primary_key_constraint | #unique_not_enforced_constraint )
        },
        |(opt_constraint_name, constraint_type)| ConstraintDefinition {
            name: opt_constraint_name.map(|(_, name)| name),
            constraint_type,
        },
    )
    .parse(i)
}

pub fn create_def(i: Input) -> IResult<CreateDefinition> {
    alt((
        map(rule! { #column_def }, CreateDefinition::Column),
        map(rule! { #table_index_def }, CreateDefinition::TableIndex),
        map(rule! { #constraint_def }, CreateDefinition::Constraint),
    ))
    .parse(i)
}

pub fn role_name(i: Input) -> IResult<String> {
    let role_ident = map_res(
        rule! {
            #ident
        },
        |role_name| {
            let name = role_name.name;
            let mut chars = name.chars();
            while let Some(c) = chars.next() {
                match c {
                    '\\' => match chars.next() {
                        Some('f') | Some('b') => {
                            return Err(nom::Err::Failure(ErrorKind::Other(
                                "' or \" or \\f or \\b are not allowed in role name",
                            )));
                        }
                        _ => {}
                    },
                    '\'' | '"' => {
                        return Err(nom::Err::Failure(ErrorKind::Other(
                            "' or \" or \\f or \\b are not allowed in role name",
                        )));
                    }
                    _ => {}
                }
            }
            Ok(name)
        },
    );
    let role_lit = map(
        rule! {
            #literal_string
        },
        |role_name| role_name,
    );

    rule!(
        #role_ident : "<role_name>"
        | #role_lit : "'<role_name>'"
    )
    .parse(i)
}

pub fn grant_source(i: Input) -> IResult<AccountMgrSource> {
    let role = map(
        rule! {
            ROLE ~ #role_name
        },
        |(_, role_name)| AccountMgrSource::Role { role: role_name },
    );
    let privs = map(
        rule! {
            #comma_separated_list1(priv_type) ~ ON ~ #grant_level
        },
        |(privs, _, level)| AccountMgrSource::Privs {
            privileges: privs,
            level,
        },
    );
    let all = map(
        rule! { ALL ~ PRIVILEGES? ~ ON ~ #grant_all_level },
        |(_, _, _, level)| AccountMgrSource::ALL { level },
    );

    let udf_privs = map(
        rule! {
            USAGE ~ ON ~ UDF ~ #ident
        },
        |(_, _, _, udf)| AccountMgrSource::Privs {
            privileges: vec![UserPrivilegeType::Usage],
            level: AccountMgrLevel::UDF(udf.to_string()),
        },
    );

    let udf_all_privs = map(
        rule! {
            ALL ~ PRIVILEGES? ~ ON ~ UDF ~ #ident
        },
        |(_, _, _, _, udf)| AccountMgrSource::Privs {
            privileges: vec![UserPrivilegeType::Usage],
            level: AccountMgrLevel::UDF(udf.to_string()),
        },
    );

    let stage_privs = map(
        rule! {
            #comma_separated_list1(stage_priv_type) ~ ON ~ STAGE ~ #ident
        },
        |(privileges, _, _, stage_name)| AccountMgrSource::Privs {
            privileges,
            level: AccountMgrLevel::Stage(stage_name.to_string()),
        },
    );

    let warehouse_privs = map(
        rule! {
            USAGE ~ ON ~ WAREHOUSE ~ #ident
        },
        |(_, _, _, w)| AccountMgrSource::Privs {
            privileges: vec![UserPrivilegeType::Usage],
            level: AccountMgrLevel::Warehouse(w.to_string()),
        },
    );

    let warehouse_all_privs = map(
        rule! {
            ALL ~ PRIVILEGES? ~ ON ~ WAREHOUSE ~ #ident
        },
        |(_, _, _, _, w)| AccountMgrSource::Privs {
            privileges: vec![UserPrivilegeType::Usage],
            level: AccountMgrLevel::Warehouse(w.to_string()),
        },
    );

    let connection_privs = map(
        rule! {
            ACCESS ~ CONNECTION ~ ON ~ CONNECTION ~ #ident
        },
        |(_, _, _, _, c)| AccountMgrSource::Privs {
            privileges: vec![UserPrivilegeType::AccessConnection],
            level: AccountMgrLevel::Connection(c.to_string()),
        },
    );

    let connection_all_privs = map(
        rule! {
            ALL ~ PRIVILEGES? ~ ON ~ CONNECTION ~ #ident
        },
        |(_, _, _, _, w)| AccountMgrSource::Privs {
            privileges: vec![UserPrivilegeType::AccessConnection],
            level: AccountMgrLevel::Connection(w.to_string()),
        },
    );

    let seq_privs = map(
        rule! {
            ACCESS ~ SEQUENCE ~ ON ~ SEQUENCE ~ #ident
        },
        |(_, _, _, _, c)| AccountMgrSource::Privs {
            privileges: vec![UserPrivilegeType::AccessSequence],
            level: AccountMgrLevel::Sequence(c.to_string()),
        },
    );

    let seq_all_privs = map(
        rule! {
            ALL ~ PRIVILEGES? ~ ON ~ SEQUENCE ~ #ident
        },
        |(_, _, _, _, w)| AccountMgrSource::Privs {
            privileges: vec![UserPrivilegeType::AccessSequence],
            level: AccountMgrLevel::Sequence(w.to_string()),
        },
    );

    let procedure_privs = map(
        rule! {
            ACCESS ~ PROCEDURE ~ ON ~ PROCEDURE ~ #ident ~ #procedure_type_name
        },
        |(_, _, _, _, name, args)| AccountMgrSource::Privs {
            privileges: vec![UserPrivilegeType::AccessProcedure],
            level: AccountMgrLevel::Procedure(ProcedureIdentity {
                name: name.to_string(),
                args_type: args,
            }),
        },
    );

    let procedure_all_privs = map(
        rule! {
            ALL ~ PRIVILEGES? ~ ON ~ PROCEDURE ~ #ident ~ #procedure_type_name
        },
        |(_, _, _, _, name, args)| AccountMgrSource::Privs {
            privileges: vec![UserPrivilegeType::AccessProcedure],
            level: AccountMgrLevel::Procedure(ProcedureIdentity {
                name: name.to_string(),
                args_type: args,
            }),
        },
    );

    let masking_policy_privs = map(
        rule! {
            APPLY ~ ON ~ MASKING ~ POLICY ~ #ident
        },
        |(_, _, _, _, name)| AccountMgrSource::Privs {
            privileges: vec![UserPrivilegeType::ApplyMaskingPolicy],
            level: AccountMgrLevel::MaskingPolicy(name.to_string()),
        },
    );

    let masking_policy_all_privs = map(
        rule! {
            ALL ~ PRIVILEGES? ~ ON ~ MASKING ~ POLICY ~ #ident
        },
        |(_, _, _, _, _, name)| AccountMgrSource::Privs {
            privileges: vec![UserPrivilegeType::ApplyMaskingPolicy],
            level: AccountMgrLevel::MaskingPolicy(name.to_string()),
        },
    );

    let row_access_policy_privs = map(
        rule! {
            APPLY ~ ON ~ ROW ~ ACCESS ~ POLICY ~ #ident
        },
        |(_, _, _, _, _, name)| AccountMgrSource::Privs {
            privileges: vec![UserPrivilegeType::ApplyRowAccessPolicy],
            level: AccountMgrLevel::RowAccessPolicy(name.to_string()),
        },
    );

    let row_access_policy_all_privs = map(
        rule! {
            ALL ~ PRIVILEGES? ~ ON ~ ROW ~ ACCESS ~ POLICY ~ #ident
        },
        |(_, _, _, _, _, _, name)| AccountMgrSource::Privs {
            privileges: vec![UserPrivilegeType::ApplyRowAccessPolicy],
            level: AccountMgrLevel::RowAccessPolicy(name.to_string()),
        },
    );

    rule!(
        #role : "ROLE <role_name>"
        | #warehouse_all_privs: "ALL [ PRIVILEGES ] ON WAREHOUSE <warehouse_name>"
        | #connection_all_privs: "ALL [ PRIVILEGES ] ON CONNECTION <connection_name>"
        | #seq_all_privs: "ALL [ PRIVILEGES ] ON SEQUENCE <seq_name>"
        | #udf_privs: "USAGE ON UDF <udf_name>"
        | #warehouse_privs: "USAGE ON WAREHOUSE <warehouse_name>"
        | #connection_privs: "ACCESS CONNECTION ON CONNECTION <connection_name>"
        | #seq_privs: "ACCESS SEQUENCE ON CONNECTION <seq_name>"
        | #masking_policy_privs: "APPLY ON MASKING POLICY <policy_name>"
        | #masking_policy_all_privs: "ALL [ PRIVILEGES ] ON MASKING POLICY <policy_name>"
        | #row_access_policy_privs: "APPLY ON ROW ACCESS POLICY <policy_name>"
        | #row_access_policy_all_privs: "ALL [ PRIVILEGES ] ON ROW ACCESS POLICY <policy_name>"
        | #privs : "<privileges> ON <privileges_level>"
        | #stage_privs : "<stage_privileges> ON STAGE <stage_name>"
        | #udf_all_privs: "ALL [ PRIVILEGES ] ON UDF <udf_name>"
        | #procedure_privs: "ACCESS PROCEDURE ON PROCEDURE <procedure_identity>"
        | #procedure_all_privs: "ALL [ PRIVILEGES ] ON PROCEDURE <procedure_identity>"
        | #all : "ALL [ PRIVILEGES ] ON <privileges_level>"
    )
    .parse(i)
}

pub fn priv_type(i: Input) -> IResult<UserPrivilegeType> {
    let usage = value(UserPrivilegeType::Usage, rule! { USAGE });
    let select = value(UserPrivilegeType::Select, rule! { SELECT });
    let insert = value(UserPrivilegeType::Insert, rule! { INSERT });
    let update = value(UserPrivilegeType::Update, rule! { UPDATE });
    let delete = value(UserPrivilegeType::Delete, rule! { DELETE });
    let alter = value(UserPrivilegeType::Alter, rule! { ALTER });
    let super_priv = value(UserPrivilegeType::Super, rule! { SUPER });
    let create_user = value(UserPrivilegeType::CreateUser, rule! { CREATE ~ USER });
    let create_database = value(
        UserPrivilegeType::CreateDatabase,
        rule! { CREATE ~ DATABASE },
    );
    let create_warehouse = value(
        UserPrivilegeType::CreateWarehouse,
        rule! { CREATE ~ WAREHOUSE },
    );
    let create_connection = value(
        UserPrivilegeType::CreateConnection,
        rule! { CREATE ~ CONNECTION },
    );
    let access_connection = value(
        UserPrivilegeType::AccessConnection,
        rule! { ACCESS ~ CONNECTION },
    );
    let create_sequence = value(
        UserPrivilegeType::CreateSequence,
        rule! { CREATE ~ SEQUENCE },
    );
    let access_sequence = value(
        UserPrivilegeType::AccessSequence,
        rule! { ACCESS ~ SEQUENCE },
    );
    let create_procedure = value(
        UserPrivilegeType::CreateProcedure,
        rule! { CREATE ~ PROCEDURE },
    );
    let access_procedure = value(
        UserPrivilegeType::AccessProcedure,
        rule! { ACCESS ~ PROCEDURE },
    );
    let drop_user = value(UserPrivilegeType::DropUser, rule! { DROP ~ USER });
    let create_role = value(UserPrivilegeType::CreateRole, rule! { CREATE ~ ROLE });
    let drop_role = value(UserPrivilegeType::DropRole, rule! { DROP ~ ROLE });
    let grant = value(UserPrivilegeType::Grant, rule! { GRANT });
    let create_stage = value(UserPrivilegeType::CreateStage, rule! { CREATE ~ STAGE });
    let set = value(UserPrivilegeType::Set, rule! { SET });
    let drop = value(UserPrivilegeType::Drop, rule! { DROP });
    let create = value(UserPrivilegeType::Create, rule! { CREATE });
    let create_masking_policy = value(
        UserPrivilegeType::CreateMaskingPolicy,
        rule! { CREATE ~ MASKING ~ POLICY },
    );
    let apply_masking_policy = value(
        UserPrivilegeType::ApplyMaskingPolicy,
        rule! { APPLY ~ MASKING ~ POLICY },
    );
    let create_row_access_policy = value(
        UserPrivilegeType::CreateRowAccessPolicy,
        rule! { CREATE ~ ROW ~ ACCESS ~ POLICY },
    );
    let apply_row_access_policy = value(
        UserPrivilegeType::ApplyRowAccessPolicy,
        rule! { APPLY ~ ROW ~ ACCESS ~ POLICY },
    );

    alt((
        rule!(
            #usage
            | #select
            | #insert
            | #update
            | #delete
            | #alter
            | #super_priv
            | #create_user
            | #create_database
            | #create_warehouse
        ),
        rule!(
            #create_connection
            | #access_connection
            | #access_sequence
            | #create_sequence
            | #access_procedure
            | #create_procedure
            | #drop_user
            | #create_role
            | #drop_role
            | #grant
            | #create_stage
            | #set
            | #drop
            | #create_masking_policy
            | #apply_masking_policy
            | #create_row_access_policy
            | #apply_row_access_policy
            | #create
        ),
    ))
    .parse(i)
}

pub fn stage_priv_type(i: Input) -> IResult<UserPrivilegeType> {
    alt((
        value(UserPrivilegeType::Read, rule! { READ }),
        value(UserPrivilegeType::Write, rule! { WRITE }),
    ))
    .parse(i)
}

pub fn on_object_name(i: Input) -> IResult<GrantObjectName> {
    let database = map(
        rule! {
            DATABASE ~ #ident
        },
        |(_, database)| GrantObjectName::Database(database.to_string()),
    );

    // `db01`.'tb1' or `db01`.`tb1` or `db01`.tb1
    let table = map(
        rule! {
            TABLE ~  #dot_separated_idents_1_to_2
        },
        |(_, (database, table))| {
            GrantObjectName::Table(database.map(|db| db.to_string()), table.to_string())
        },
    );

    let stage = map(rule! { STAGE ~ #ident}, |(_, stage_name)| {
        GrantObjectName::Stage(stage_name.to_string())
    });

    let udf = map(rule! { UDF ~ #ident}, |(_, udf_name)| {
        GrantObjectName::UDF(udf_name.to_string())
    });

    let warehouse = map(rule! { WAREHOUSE ~ #ident}, |(_, w)| {
        GrantObjectName::Warehouse(w.to_string())
    });

    let connection = map(rule! { CONNECTION ~ #ident}, |(_, w)| {
        GrantObjectName::Connection(w.to_string())
    });

    let seq = map(rule! { SEQUENCE ~ #ident}, |(_, w)| {
        GrantObjectName::Sequence(w.to_string())
    });

    let procedure = map(
        rule! { PROCEDURE ~ #ident ~ #procedure_type_name},
        |(_, name, args)| {
            GrantObjectName::Procedure(ProcedureIdentity {
                name: name.to_string(),
                args_type: args,
            })
        },
    );

    let masking_policy = map(rule! { MASKING ~ POLICY ~ #ident }, |(_, _, name)| {
        GrantObjectName::MaskingPolicy(name.to_string())
    });

    let row_access_policy = map(
        rule! { ROW ~ ACCESS ~ POLICY ~ #ident },
        |(_, _, _, name)| GrantObjectName::RowAccessPolicy(name.to_string()),
    );

    rule!(
        #database : "DATABASE <database>"
        | #table : "TABLE <database>.<table>"
        | #stage : "STAGE <stage_name>"
        | #udf : "UDF <udf_name>"
        | #warehouse : "WAREHOUSE <warehouse_name>"
        | #connection : "CONNECTION <connection_name>"
        | #seq : "SEQUENCE <seq_name>"
        | #procedure : "PROCEDURE <procedure_identity>"
        | #masking_policy : "MASKING POLICY <policy_name>"
        | #row_access_policy : "ROW ACCESS POLICY <policy_name>"
    )
    .parse(i)
}

pub fn grant_level(i: Input) -> IResult<AccountMgrLevel> {
    // *.*
    let global = map(rule! { "*" ~ "." ~ "*" }, |_| AccountMgrLevel::Global);
    // db.*
    // "*": as current db or "table" with current db
    let db = map(
        rule! {
            ( #ident ~ "." )? ~ "*"
        },
        |(database, _)| AccountMgrLevel::Database(database.map(|(database, _)| database.name)),
    );

    // `db01`.'tb1' or `db01`.`tb1` or `db01`.tb1
    let table = map(
        rule! {
            ( #ident ~ "." )? ~ #parameter_to_string
        },
        |(database, table)| {
            AccountMgrLevel::Table(database.map(|(database, _)| database.name), table)
        },
    );

    let masking_policy = map(rule! { MASKING ~ POLICY ~ #ident }, |(_, _, name)| {
        AccountMgrLevel::MaskingPolicy(name.to_string())
    });

    let row_access_policy = map(
        rule! { ROW ~ ACCESS ~ POLICY ~ #ident },
        |(_, _, _, name)| AccountMgrLevel::RowAccessPolicy(name.to_string()),
    );

    rule!(
        #global : "*.*"
        | #db : "<database>.*"
        | #table : "<database>.<table>"
        | #masking_policy : "MASKING POLICY <policy_name>"
        | #row_access_policy : "ROW ACCESS POLICY <policy_name>"
    )
    .parse(i)
}

pub fn grant_all_level(i: Input) -> IResult<AccountMgrLevel> {
    // *.*
    let global = map(rule! { "*" ~ "." ~ "*" }, |_| AccountMgrLevel::Global);
    // db.*
    // "*": as current db or "table" with current db
    let db = map(
        rule! {
            ( #ident ~ "." )? ~ "*"
        },
        |(database, _)| AccountMgrLevel::Database(database.map(|(database, _)| database.name)),
    );

    // `db01`.'tb1' or `db01`.`tb1` or `db01`.tb1
    let table = map(
        rule! {
            ( #ident ~ "." )? ~ #parameter_to_string
        },
        |(database, table)| {
            AccountMgrLevel::Table(database.map(|(database, _)| database.name), table)
        },
    );

    let stage = map(rule! { STAGE ~ #ident}, |(_, stage_name)| {
        AccountMgrLevel::Stage(stage_name.to_string())
    });

    let warehouse = map(rule! { WAREHOUSE ~ #ident}, |(_, w)| {
        AccountMgrLevel::Warehouse(w.to_string())
    });
    rule!(
        #global : "*.*"
        | #db : "<database>.*"
        | #table : "<database>.<table>"
        | #stage : "STAGE <stage_name>"
        | #warehouse : "WAREHOUSE <warehouse_name>"
    )
    .parse(i)
}

pub fn grant_ownership_level(i: Input) -> IResult<AccountMgrLevel> {
    // db.*
    // "*": as current db or "table" with current db
    let db = map(
        rule! {
            ( #grant_ident ~ "." )? ~ "*"
        },
        |(database, _)| AccountMgrLevel::Database(database.map(|(database, _)| database.name)),
    );

    // `db01`.'tb1' or `db01`.`tb1` or `db01`.tb1
    let table = map(
        rule! {
            ( #grant_ident ~ "." )? ~ #parameter_to_grant_string
        },
        |(database, table)| {
            AccountMgrLevel::Table(database.map(|(database, _)| database.name), table)
        },
    );

    #[derive(Clone)]
    enum Object {
        Stage,
        Udf,
        Warehouse,
        Connection,
        Sequence,
        MaskingPolicy,
        RowAccessPolicy,
    }
    let object = alt((
        value(Object::Udf, rule! { UDF }),
        value(Object::Stage, rule! { STAGE }),
        value(Object::Warehouse, rule! { WAREHOUSE }),
        value(Object::Connection, rule! { CONNECTION }),
        value(Object::Sequence, rule! { SEQUENCE }),
        value(Object::MaskingPolicy, rule! { MASKING ~ POLICY }),
        value(Object::RowAccessPolicy, rule! { ROW ~ ACCESS ~ POLICY }),
    ));

    // Object object_name
    let object = map(
        rule! { #object ~ #grant_ident },
        |(object, object_name)| match object {
            Object::Stage => AccountMgrLevel::Stage(object_name.to_string()),
            Object::Udf => AccountMgrLevel::UDF(object_name.to_string()),
            Object::Warehouse => AccountMgrLevel::Warehouse(object_name.to_string()),
            Object::Connection => AccountMgrLevel::Connection(object_name.to_string()),
            Object::Sequence => AccountMgrLevel::Sequence(object_name.to_string()),
            Object::MaskingPolicy => AccountMgrLevel::MaskingPolicy(object_name.to_string()),
            Object::RowAccessPolicy => AccountMgrLevel::RowAccessPolicy(object_name.to_string()),
        },
    );

    let procedure = map(
        rule! {
            PROCEDURE ~ #grant_ident ~ #procedure_type_name
        },
        |(_, name, args)| {
            let name = ProcedureIdentity {
                name: name.to_string(),
                args_type: args,
            };
            AccountMgrLevel::Procedure(name)
        },
    );
    rule!(
        #db : "<database>.*"
        | #table : "<database>.<table>"
        | #object : "STAGE | UDF | WAREHOUSE | CONNECTION | SEQUENCE <object_name>"
        | #procedure : "PROCEDURE <procedure_identity>"
    )
    .parse(i)
}

pub fn grant_option(i: Input) -> IResult<PrincipalIdentity> {
    let role = map(
        rule! {
            ROLE ~ #role_name
        },
        |(_, role_name)| PrincipalIdentity::Role(role_name),
    );

    let user = map(
        rule! {
            USER? ~ #user_identity
        },
        |(_, user)| PrincipalIdentity::User(user),
    );

    rule!(
        #role
        | #user
    )
    .parse(i)
}

pub fn create_table_source(i: Input) -> IResult<CreateTableSource> {
    let columns = map(
        rule! {
            "(" ~ ^#comma_separated_list1(create_def) ~ ^")"
        },
        |(_, create_defs, _)| {
            let mut columns = Vec::with_capacity(create_defs.len());
            let mut table_indexes = Vec::new();
            let mut column_constraints = Vec::new();
            let mut table_constraints = Vec::new();
            for create_def in create_defs {
                match create_def {
                    CreateDefinition::Column(column) => {
                        if let Some(expr) = &column.check {
                            column_constraints.push(ConstraintDefinition {
                                name: None,
                                constraint_type: ConstraintType::Check(expr.clone()),
                            });
                        }
                        columns.push(column);
                    }
                    CreateDefinition::TableIndex(table_index) => {
                        table_indexes.push(table_index);
                    }
                    CreateDefinition::Constraint(constraint) => {
                        table_constraints.push(constraint);
                    }
                }
            }
            let opt_table_indexes = if !table_indexes.is_empty() {
                Some(table_indexes)
            } else {
                None
            };
            let opt_column_constraints = if !column_constraints.is_empty() {
                Some(column_constraints)
            } else {
                None
            };
            let opt_table_constraints = if !table_constraints.is_empty() {
                Some(table_constraints)
            } else {
                None
            };
            CreateTableSource::Columns {
                columns,
                opt_table_indexes,
                opt_column_constraints,
                opt_table_constraints,
            }
        },
    );
    let like = map(
        rule! {
            LIKE ~ #dot_separated_idents_1_to_3
        },
        |(_, (database, schema, table))| CreateTableSource::Like {
            database,
            schema,
            table,
        },
    );

    rule!(
        #columns
        | #like
    )
    .parse(i)
}

pub fn alter_schema_action(i: Input) -> IResult<AlterSchemaAction> {
    let rename_schema = map(
        rule! {
            RENAME ~ TO ~ #ident
        },
        |(_, _, new_schema)| AlterSchemaAction::RenameSchema { new_schema },
    );

    let refresh_cache = map(
        rule! {
            REFRESH ~ CACHE
        },
        |(_, _)| AlterSchemaAction::RefreshSchemaCache,
    );

    rule!(
        #rename_schema
        | #refresh_cache
    )
    .parse(i)
}

pub fn modify_column_type(i: Input) -> IResult<ColumnDefinition> {
    #[derive(Educe)]
    #[educe(Clone(bound = false, attrs = "#[recursive::recursive]"))]
    enum ColumnConstraint {
        Nullable(bool),
        DefaultExpr(Box<Expr>),
    }

    let nullable = alt((
        value(ColumnConstraint::Nullable(true), rule! { NULL }),
        value(ColumnConstraint::Nullable(false), rule! { NOT ~ ^NULL }),
    ));
    let expr = alt((map(
        rule! {
            DEFAULT ~ ^#subexpr(NOT_PREC)
        },
        |(_, default_expr)| ColumnConstraint::DefaultExpr(Box::new(default_expr)),
    ),));

    let comment = map(
        rule! {
            COMMENT ~ #literal_string
        },
        |(_, comment)| comment,
    );

    map_res(
        rule! {
            #ident
            ~ #type_name
            ~ ( #nullable | #expr )*
            ~ ( #comment )?
            : "`<column name> <type> [DEFAULT <expr>] [CHECK <expr>] [COMMENT '<comment>']`"
        },
        |(name, data_type, constraints, comment)| {
            let mut def = ColumnDefinition {
                name,
                data_type,
                expr: None,
                is_primary_key: false,
                check: None,
                comment,
            };
            for constraint in constraints {
                match constraint {
                    ColumnConstraint::Nullable(nullable) => {
                        if (nullable && matches!(def.data_type, TypeName::NotNull(_)))
                            || (!nullable && matches!(def.data_type, TypeName::Nullable(_)))
                        {
                            return Err(nom::Err::Failure(ErrorKind::Other(
                                "ambiguous NOT NULL constraint",
                            )));
                        }
                        if nullable {
                            def.data_type = def.data_type.wrap_nullable();
                        } else {
                            def.data_type = def.data_type.wrap_not_null();
                        }
                    }
                    ColumnConstraint::DefaultExpr(default_expr) => {
                        def.expr = Some(ColumnExpr::Default(default_expr))
                    }
                }
            }
            Ok(def)
        },
    )(i)
}

pub fn modify_column_comment(i: Input) -> IResult<ColumnComment> {
    let comment = map(
        rule! {
            COMMENT ~ #literal_string
        },
        |(_, comment)| comment,
    );
    map_res(
        rule! {
            #ident
            ~ #comment
            : "`<column name> COMMENT '<comment>'`"
        },
        |(name, comment)| Ok(ColumnComment { name, comment }),
    )(i)
}

pub fn modify_column_action(i: Input) -> IResult<ModifyColumnAction> {
    // Parse: <column> SET MASKING POLICY <policy_name> [USING (masked_col_name, cond_col_1, ...)]
    let set_mask_policy = map(
        rule! {
            #ident ~ SET ~ MASKING ~ POLICY ~ #ident ~ (USING ~ "(" ~ #comma_separated_list1(ident) ~ ")")?
        },
        |(column, _, _, _, mask_name, opt_using)| {
            ModifyColumnAction::SetMaskingPolicy(
                column,
                mask_name.to_string(),
                opt_using.map(|(_, _, using_columns, _)| using_columns),
            )
        },
    );

    let unset_mask_policy = map(
        rule! {
            #ident ~ UNSET ~ MASKING ~ POLICY
        },
        |(column, _, _, _)| ModifyColumnAction::UnsetMaskingPolicy(column),
    );

    let convert_stored_computed_column = map(
        rule! {
            #ident ~ DROP ~ STORED
        },
        |(column, _, _)| ModifyColumnAction::ConvertStoredComputedColumn(column),
    );

    let modify_column_type = map(
        rule! {
            #modify_column_type ~ ("," ~ COLUMN? ~ #modify_column_type)*
        },
        |(column_def, column_def_vec)| {
            let mut column_defs = vec![column_def];
            column_def_vec
                .iter()
                .for_each(|(_, _, column_def)| column_defs.push(column_def.clone()));
            ModifyColumnAction::SetDataType(column_defs)
        },
    );

    let modify_column_comment = map(
        rule! {
            #modify_column_comment ~ ("," ~ COLUMN? ~ #modify_column_comment)*
        },
        |(column_def, column_def_vec)| {
            let mut column_defs = vec![column_def];
            column_def_vec
                .iter()
                .for_each(|(_, _, column_def)| column_defs.push(column_def.clone()));
            ModifyColumnAction::Comment(column_defs)
        },
    );

    rule!(
        #set_mask_policy
        | #unset_mask_policy
        | #convert_stored_computed_column
        | #modify_column_type
        | #modify_column_comment
    )
    .parse(i)
}

pub fn alter_table_action(i: Input) -> IResult<AlterTableAction> {
    let rename_table = map(
        rule! {
           RENAME ~ TO ~ #ident
        },
        |(_, _, new_table)| AlterTableAction::RenameTable { new_table },
    );

    let rename_column = map(
        rule! {
            RENAME ~ COLUMN? ~ #ident ~ TO ~ #ident
        },
        |(_, _, old_column, _, new_column)| AlterTableAction::RenameColumn {
            old_column,
            new_column,
        },
    );
    let modify_table_comment = map(
        rule! {
            COMMENT ~ ^"=" ~ ^#literal_string
        },
        |(_, _, new_comment)| AlterTableAction::ModifyTableComment { new_comment },
    );
    let add_column = map(
        rule! {
            ADD ~ COLUMN? ~ #column_def ~ ( #add_column_option )?
        },
        |(_, _, column, option)| AlterTableAction::AddColumn {
            column,
            option: option.unwrap_or(AddColumnOption::End),
        },
    );

    let modify_column = map(
        rule! {
            MODIFY ~ COLUMN? ~ #modify_column_action
        },
        |(_, _, action)| AlterTableAction::ModifyColumn { action },
    );

    let add_constraint = map(
        rule! {
            ADD ~ #constraint_def
        },
        |(_, constraint)| AlterTableAction::AddConstraint { constraint },
    );

    let drop_constraint = map(
        rule! {
            DROP ~ CONSTRAINT ~ #ident
        },
        |(_, _, constraint_name)| AlterTableAction::DropConstraint { constraint_name },
    );

    let add_row_access_policy = map(
        rule! {
            ADD ~ ROW ~ ACCESS ~ POLICY ~ #ident ~ ON ~ "(" ~ ^#comma_separated_list1(ident) ~ ^")"
        },
        |(_, _, _, _, policy, _, _, columns, _)| AlterTableAction::AddRowAccessPolicy {
            columns,
            policy,
        },
    );

    let drop_row_access_policy = map(
        rule! {
            DROP ~ ROW ~ ACCESS ~ POLICY ~ #ident
        },
        |(_, _, _, _, policy)| AlterTableAction::DropRowAccessPolicy { policy },
    );

    let drop_all_row_access_polices = map(
        rule! {
            DROP ~ ALL ~ ROW ~ ACCESS ~ POLICIES
        },
        |(_, _, _, _, _)| AlterTableAction::DropAllRowAccessPolicies,
    );

    let drop_column = map(
        rule! {
            DROP ~ COLUMN? ~ #ident
        },
        |(_, _, column)| AlterTableAction::DropColumn { column },
    );

    let revert_table = map(
        rule! {
            FLASHBACK ~ TO ~ #travel_point
        },
        |(_, _, point)| AlterTableAction::FlashbackTo { point },
    );

    let set_table_options = map(
        rule! {
            SET ~ OPTIONS ~ "(" ~ #set_table_option ~ ")"
        },
        |(_, _, _, set_options, _)| AlterTableAction::SetOptions { set_options },
    );

    let unset_table_options = map(
        rule! {
            UNSET ~ OPTIONS ~ #unset_source
        },
        |(_, _, targets)| AlterTableAction::UnsetOptions { targets },
    );

    let refresh_cache = map(
        rule! {
            REFRESH ~ CACHE
        },
        |(_, _)| AlterTableAction::RefreshTableCache,
    );

    let modify_table_connection = map(
        rule! {
            CONNECTION ~ ^"=" ~ #connection_options
        },
        |(_, _, connection_options)| AlterTableAction::ModifyConnection {
            new_connection: connection_options,
        },
    );

    rule!(
        #drop_constraint
        | #rename_table
        | #rename_column
        | #modify_table_comment
        | #add_column
        | #drop_column
        | #modify_column
        | #revert_table
        | #set_table_options
        | #unset_table_options
        | #refresh_cache
        | #modify_table_connection
        | #drop_all_row_access_polices
        | #drop_row_access_policy
        | #add_row_access_policy
        | #add_constraint
    )
    .parse(i)
}

pub fn add_column_option(i: Input) -> IResult<AddColumnOption> {
    alt((
        value(AddColumnOption::First, rule! { FIRST }),
        map(rule! { AFTER ~ #ident }, |(_, ident)| {
            AddColumnOption::After(ident)
        }),
    ))
    .parse(i)
}

pub fn optimize_table_action(i: Input) -> IResult<OptimizeTableAction> {
    alt((
        value(OptimizeTableAction::All, rule! { ALL }),
        map(
            rule! { PURGE ~ (BEFORE ~ ^#travel_point)? },
            |(_, opt_travel_point)| OptimizeTableAction::Purge {
                before: opt_travel_point.map(|(_, p)| p),
            },
        ),
        map(rule! { COMPACT ~ SEGMENT? }, |(_, opt_segment)| {
            OptimizeTableAction::Compact {
                target: opt_segment.map_or(CompactTarget::Block, |_| CompactTarget::Segment),
            }
        }),
    ))
    .parse(i)
}

pub fn literal_duration(i: Input) -> IResult<Duration> {
    let seconds = map(
        rule! {
            #literal_u64 ~ SECONDS
        },
        |(v, _)| Duration::from_secs(v),
    );

    let days = map(
        rule! {
            #literal_u64 ~ DAYS
        },
        |(v, _)| Duration::from_secs(v * 60 * 60 * 24),
    );

    rule!(
        #days
        | #seconds
    )
    .parse(i)
}

pub fn vacuum_drop_table_option(i: Input) -> IResult<VacuumDropTableOption> {
    alt((map(
        rule! {
            (DRY ~ ^RUN ~ SUMMARY?)? ~ (LIMIT ~ #literal_u64)?
        },
        |(opt_dry_run, opt_limit)| VacuumDropTableOption {
            dry_run: opt_dry_run.map(|dry_run| dry_run.2.is_some()),
            limit: opt_limit.map(|(_, limit)| limit as usize),
        },
    ),))
    .parse(i)
}

pub fn vacuum_table_option(i: Input) -> IResult<VacuumTableOption> {
    alt((map(
        rule! {
            (DRY ~ ^RUN ~ SUMMARY?)?
        },
        |opt_dry_run| VacuumTableOption {
            dry_run: opt_dry_run.map(|dry_run| dry_run.2.is_some()),
        },
    ),))
    .parse(i)
}

pub fn task_sql_block(i: Input) -> IResult<TaskSql> {
    let single_statement = map(
        rule! {
            #statement
        },
        |stmt| {
            let sql = format!("{}", stmt.stmt);
            TaskSql::SingleStatement(sql)
        },
    );
    let task_block = map(
        rule! {
            BEGIN
            ~ #semicolon_terminated_list1(statement_body)
            ~ END
        },
        |(_, stmts, _)| {
            let sql = stmts
                .iter()
                .map(|stmt| format!("{}", stmt))
                .collect::<Vec<String>>();
            TaskSql::ScriptBlock(sql)
        },
    );
    alt((single_statement, task_block)).parse(i)
}

pub fn alter_task_option(i: Input) -> IResult<AlterTaskOptions> {
    let suspend = map(
        rule! {
             SUSPEND
        },
        |_| AlterTaskOptions::Suspend,
    );
    let resume = map(
        rule! {
             RESUME
        },
        |_| AlterTaskOptions::Resume,
    );
    let modify_as = map(
        rule! {
             MODIFY ~ AS ~ #task_sql_block
        },
        |(_, _, sql)| AlterTaskOptions::ModifyAs(sql),
    );
    let modify_when = map(
        rule! {
             MODIFY ~ WHEN ~ #expr
        },
        |(_, _, expr)| AlterTaskOptions::ModifyWhen(expr),
    );
    let add_after = map(
        rule! {
             ADD ~ AFTER ~ #comma_separated_list0(literal_string)
        },
        |(_, _, after)| AlterTaskOptions::AddAfter(after),
    );
    let remove_after = map(
        rule! {
             REMOVE ~ AFTER ~ #comma_separated_list0(literal_string)
        },
        |(_, _, after)| AlterTaskOptions::RemoveAfter(after),
    );

    let set = map(
        rule! {
             SET
             ~ #alter_task_set_option*
             ~ #set_table_option?
        },
        |(_, task_set_options, session_opts)| {
            let mut set = AlterTaskOptions::Set {
                session_parameters: session_opts,
                warehouse: None,
                schedule: None,
                suspend_task_after_num_failures: None,
                comments: None,
                error_integration: None,
            };
            for opt in task_set_options {
                set.apply_opt(opt);
            }
            set
        },
    );
    let unset = map(
        rule! {
             UNSET ~ WAREHOUSE
        },
        |_| AlterTaskOptions::Unset { warehouse: true },
    );
    rule!(
        #suspend
        | #resume
        | #modify_as
        | #set
        | #unset
        | #modify_when
        | #add_after
        | #remove_after
    )
    .parse(i)
}

pub fn alter_pipe_option(i: Input) -> IResult<AlterPipeOptions> {
    let set = map(
        rule! {
             SET
             ~ ( PIPE_EXECUTION_PAUSED ~ "=" ~ #literal_bool )?
             ~ ( COMMENT ~ "=" ~ #literal_string )?
        },
        |(_, execution_parsed, comment)| AlterPipeOptions::Set {
            execution_paused: execution_parsed.map(|(_, _, paused)| paused),
            comments: comment.map(|(_, _, comment)| comment),
        },
    );
    let refresh = map(
        rule! {
             REFRESH
             ~ ( PREFIX ~ "=" ~ #literal_string )?
             ~ ( MODIFIED_AFTER ~ "=" ~ #literal_string )?
        },
        |(_, prefix, modified_after)| AlterPipeOptions::Refresh {
            prefix: prefix.map(|(_, _, prefix)| prefix),
            modified_after: modified_after.map(|(_, _, modified_after)| modified_after),
        },
    );
    rule!(
        #set
        | #refresh
    )
    .parse(i)
}

pub fn task_warehouse_option(i: Input) -> IResult<WarehouseOptions> {
    alt((map(
        rule! {
            (WAREHOUSE  ~ "=" ~ #literal_string)?
        },
        |warehouse_opt| {
            let warehouse = match warehouse_opt {
                Some(warehouse) => Some(warehouse.2),
                None => None,
            };
            WarehouseOptions { warehouse }
        },
    ),))
    .parse(i)
}

pub fn assign_nodes_list(i: Input) -> IResult<Vec<(Option<String>, u64)>> {
    let nodes_list = map(
        rule! {
            ASSIGN ~ #literal_u64 ~ NODES ~ (FROM ~ #option_to_string)?
        },
        |(_, node_size, _, node_group)| (node_group.map(|(_, x)| x), node_size),
    );

    map(comma_separated_list1(nodes_list), |opts| {
        opts.into_iter().collect()
    })
    .parse(i)
}

pub fn assign_warehouse_nodes_list(i: Input) -> IResult<Vec<(Identifier, Option<String>, u64)>> {
    let nodes_list = map(
        rule! {
            ASSIGN ~ #literal_u64 ~ NODES ~ (FROM ~ #option_to_string)? ~ FOR ~ #ident
        },
        |(_, node_size, _, node_group, _, cluster)| {
            (cluster, node_group.map(|(_, x)| x), node_size)
        },
    );

    map(comma_separated_list1(nodes_list), |opts| {
        opts.into_iter().collect()
    })
    .parse(i)
}

pub fn unassign_warehouse_nodes_list(i: Input) -> IResult<Vec<(Identifier, Option<String>, u64)>> {
    let nodes_list = map(
        rule! {
            UNASSIGN ~ #literal_u64 ~ NODES ~ (FROM ~ #option_to_string)? ~ FOR ~ #ident
        },
        |(_, node_size, _, node_group, _, cluster)| {
            (cluster, node_group.map(|(_, x)| x), node_size)
        },
    );

    map(comma_separated_list1(nodes_list), |opts| {
        opts.into_iter().collect()
    })
    .parse(i)
}

pub fn warehouse_cluster_option(i: Input) -> IResult<BTreeMap<String, String>> {
    let option = map(
        rule! {
           #ident ~ "=" ~ #option_to_string
        },
        |(k, _, v)| (k, v),
    );
    map(comma_separated_list1(option), |opts| {
        opts.into_iter()
            .map(|(k, v)| (k.name.to_lowercase(), v.clone()))
            .collect()
    })
    .parse(i)
}

pub fn workload_quotas(i: Input) -> IResult<BTreeMap<String, QuotaValueStmt>> {
    let option = map(
        rule! {
           #ident ~ "=" ~ #option_to_string
        },
        |(k, _, v)| (k, v),
    );

    map_res(comma_separated_list1(option), |opts| {
        let mut quotas = BTreeMap::new();
        for (name, value) in opts {
            let name = name.name.to_lowercase();
            match QuotaValueStmt::new(&name, value) {
                Ok(value) => {
                    quotas.insert(name, value);
                }
                Err(error_desc) => {
                    return Err(nom::Err::Failure(ErrorKind::Other(error_desc)));
                }
            }
        }

        Ok(quotas)
    })(i)
}

pub fn task_schedule_option(i: Input) -> IResult<ScheduleOptions> {
    let interval = map(
        rule! {
             #literal_u64 ~ MINUTE
        },
        |(mins, _)| ScheduleOptions::IntervalSecs(mins * 60, 0),
    );
    let cron_expr = map(
        rule! {
            USING ~ CRON ~ #literal_string ~ #literal_string?
        },
        |(_, _, expr, timezone)| ScheduleOptions::CronExpression(expr, timezone),
    );
    let interval_sec = map(
        rule! {
             #literal_u64 ~ SECOND
        },
        |(secs, _)| ScheduleOptions::IntervalSecs(secs, 0),
    );
    let interval_millis = map(
        rule! {
             #literal_u64 ~ MILLISECOND
        },
        |(millis, _)| ScheduleOptions::IntervalSecs(0, millis),
    );
    rule!(
        #interval
        | #cron_expr
        | #interval_sec
        | #interval_millis
    )
    .parse(i)
}

pub fn limit_where(i: Input) -> IResult<ShowLimit> {
    map(
        rule! {
            WHERE ~ #expr
        },
        |(_, selection)| ShowLimit::Where {
            selection: Box::new(selection),
        },
    )
    .parse(i)
}

pub fn limit_like(i: Input) -> IResult<ShowLimit> {
    map(
        rule! {
            LIKE ~ #literal_string
        },
        |(_, pattern)| ShowLimit::Like { pattern },
    )
    .parse(i)
}

pub fn show_limit(i: Input) -> IResult<ShowLimit> {
    rule!(
        #limit_like
        | #limit_where
    )
    .parse(i)
}

pub fn show_options(i: Input) -> IResult<ShowOptions> {
    map(
        rule! {
            #show_limit? ~ ( LIMIT ~ ^#literal_u64 )?
        },
        |(show_limit, opt_limit)| ShowOptions {
            show_limit,
            limit: opt_limit.map(|(_, limit)| limit),
        },
    )
    .parse(i)
}

pub fn show_stats_stmt(i: Input) -> IResult<ShowStatisticsStmt> {
    alt((
        map(
            rule! {
                DATABASE ~ #dot_separated_idents_1_to_2
            },
            |(_, (database, schema))| ShowStatisticsStmt {
                database,
                schema: Some(schema),
                target: ShowStatsTarget::Database,
            },
        ),
        map(
            rule! {
                TABLE ~ #dot_separated_idents_1_to_3
            },
            |(_, (database, schema, table))| ShowStatisticsStmt {
                database,
                schema,
                target: ShowStatsTarget::Table(table),
            },
        ),
    ))
    .parse(i)
}

pub fn table_option(i: Input) -> IResult<BTreeMap<String, String>> {
    map(
        rule! {
           ( #ident ~ "=" ~ #option_to_string )*
        },
        |opts| {
            BTreeMap::from_iter(
                opts.iter()
                    .map(|(k, _, v)| (k.name.to_lowercase(), v.clone())),
            )
        },
    )
    .parse(i)
}

pub fn set_table_option(i: Input) -> IResult<BTreeMap<String, String>> {
    let option = map(
        rule! {
           #ident ~ "=" ~ #option_to_string
        },
        |(k, _, v)| (k, v),
    );

    map(comma_separated_list1(option), |opts| {
        opts.into_iter()
            .map(|(k, v)| (k.name.to_lowercase(), v.clone()))
            .collect()
    })
    .parse(i)
}

pub fn option_to_string(i: Input) -> IResult<String> {
    let bool_to_string = |i| map(literal_bool, |v| v.to_string()).parse(i);

    rule!(
        #bool_to_string
        | #parameter_to_string
    )
    .parse(i)
}

pub fn database_engine(i: Input) -> IResult<DatabaseEngine> {
    value(DatabaseEngine::Default, rule! { DEFAULT }).parse(i)
}

pub fn create_database_option(i: Input) -> IResult<CreateDatabaseOption> {
    let mut create_db_engine = parser_fn(map(
        rule! {
            ENGINE ~  ^"=" ~ ^#database_engine
        },
        |(_, _, option)| CreateDatabaseOption::DatabaseEngine(option),
    ));

    rule!(
        #create_db_engine
    )
    .parse(i)
}

pub fn user_option(i: Input) -> IResult<UserOptionItem> {
    let tenant_setting = value(UserOptionItem::TenantSetting(true), rule! { TENANTSETTING });
    let no_tenant_setting = value(
        UserOptionItem::TenantSetting(false),
        rule! { NOTENANTSETTING },
    );
    let default_role_option = map(
        rule! {
            DEFAULT_ROLE ~ ^"=" ~ ^#role_name
        },
        |(_, _, role)| UserOptionItem::DefaultRole(role),
    );
    let set_network_policy = map(
        rule! {
            SET ~ NETWORK ~ POLICY ~ ^"=" ~ ^#literal_string
        },
        |(_, _, _, _, policy)| UserOptionItem::SetNetworkPolicy(policy),
    );
    let unset_network_policy = map(
        rule! {
            UNSET ~ NETWORK ~ POLICY
        },
        |(_, _, _)| UserOptionItem::UnsetNetworkPolicy,
    );
    let set_disabled_option = map(
        rule! {
            DISABLED ~ ^"=" ~ #literal_bool
        },
        |(_, _, disabled)| UserOptionItem::Disabled(disabled),
    );
    let set_password_policy = map(
        rule! {
            SET ~ PASSWORD ~ POLICY ~ ^"=" ~ ^#literal_string
        },
        |(_, _, _, _, policy)| UserOptionItem::SetPasswordPolicy(policy),
    );
    let unset_password_policy = map(
        rule! {
            UNSET ~ PASSWORD ~ POLICY
        },
        |(_, _, _)| UserOptionItem::UnsetPasswordPolicy,
    );
    let must_change_password = map(
        rule! {
            MUST_CHANGE_PASSWORD ~ ^"=" ~ ^#literal_bool
        },
        |(_, _, val)| UserOptionItem::MustChangePassword(val),
    );
    let set_workload_group = map(
        rule! {
            SET ~ WORKLOAD ~ GROUP ~ ^"=" ~ ^#literal_string
        },
        |(_, _, _, _, wg)| UserOptionItem::SetWorkloadGroup(wg),
    );
    let unset_workload_group = map(
        rule! {
            UNSET ~ WORKLOAD ~ GROUP
        },
        |(_, _, _)| UserOptionItem::UnsetWorkloadGroup,
    );

    rule!(
        #tenant_setting
        | #no_tenant_setting
        | #default_role_option
        | #set_network_policy
        | #unset_network_policy
        | #set_password_policy
        | #unset_password_policy
        | #set_disabled_option
        | #must_change_password
        | #set_workload_group
        | #unset_workload_group
    )
    .parse(i)
}

pub fn user_identity(i: Input) -> IResult<UserIdentity> {
    map(
        rule! {
            #parameter_to_string ~ ( "@" ~ "'%'" )?
        },
        |(username, _)| {
            let hostname = "%".to_string();
            UserIdentity { username, hostname }
        },
    )
    .parse(i)
}

pub fn auth_type(i: Input) -> IResult<AuthType> {
    alt((
        value(AuthType::NoPassword, rule! { NO_PASSWORD }),
        value(AuthType::Sha256Password, rule! { SHA256_PASSWORD }),
        value(AuthType::DoubleSha1Password, rule! { DOUBLE_SHA1_PASSWORD }),
        value(AuthType::JWT, rule! { JWT }),
    ))
    .parse(i)
}

pub fn table_reference_with_alias(i: Input) -> IResult<TableReference> {
    map(
        consumed(rule! {
            #dot_separated_idents_1_to_3 ~ #alias_name?
        }),
        |(span, ((database, schema, table), alias))| TableReference::Table {
            span: transform_span(span.tokens),
            database,
            schema,
            table,
            alias: alias.map(|v| TableAlias {
                name: v,
                columns: vec![],
                keep_schema_name: false,
            }),
            temporal: None,
            with_options: None,
            pivot: None,
            unpivot: None,
            sample: None,
        },
    )
    .parse(i)
}

fn function_name_ref(i: Input) -> IResult<FunctionName> {
    map(
        rule! {
            #dot_separated_idents_1_to_3
        },
        |(database, schema, name)| FunctionName {
            database,
            schema,
            name,
        },
    )
    .parse(i)
}

fn function_argument(i: Input) -> IResult<FunctionArgument> {
    let named = map(
        rule! {
            #ident ~ ^#type_name
        },
        |(name, data_type)| FunctionArgument {
            name: Some(name),
            data_type,
        },
    );
    let unnamed = map(rule! { #type_name }, |data_type| FunctionArgument {
        name: None,
        data_type,
    });

    rule!(#named | #unnamed).parse(i)
}

fn function_table_column(i: Input) -> IResult<FunctionTableColumn> {
    map(
        rule! {
            #ident ~ ^#type_name
        },
        |(name, data_type)| FunctionTableColumn { name, data_type },
    )
    .parse(i)
}

fn function_return(i: Input) -> IResult<FunctionReturn> {
    let scalar = map(rule! { #type_name }, FunctionReturn::Scalar);
    let table = map(
        rule! {
            TABLE ~ "(" ~ #comma_separated_list0(function_table_column) ~ ")"
        },
        |(_, _, columns, _)| FunctionReturn::Table(columns),
    );

    rule!(#table | #scalar).parse(i)
}

#[derive(Clone)]
enum CreateFunctionOption {
    Language(Identifier),
    Volatility(FunctionVolatility),
    Strict,
    Security(FunctionSecurity),
    Handler(String),
    Packages(Vec<String>),
    Imports(Vec<String>),
    Rows(u64),
    CapabilityProfile(Identifier),
}

fn create_function_option(i: Input) -> IResult<CreateFunctionOption> {
    let language = map(
        rule! {
            LANGUAGE ~ #ident
        },
        |(_, language)| CreateFunctionOption::Language(language),
    );
    let immutable = value(
        CreateFunctionOption::Volatility(FunctionVolatility::Immutable),
        rule! { IMMUTABLE },
    );
    let stable = value(
        CreateFunctionOption::Volatility(FunctionVolatility::Stable),
        rule! { #match_text("STABLE") },
    );
    let volatile = value(
        CreateFunctionOption::Volatility(FunctionVolatility::Volatile),
        rule! { VOLATILE },
    );
    let strict = value(
        CreateFunctionOption::Strict,
        rule! { #match_text("STRICT") },
    );
    let security_invoker = value(
        CreateFunctionOption::Security(FunctionSecurity::Invoker),
        rule! {
            #match_text("SECURITY") ~ #match_text("INVOKER")
        },
    );
    let security_definer = value(
        CreateFunctionOption::Security(FunctionSecurity::Definer),
        rule! {
            #match_text("SECURITY") ~ #match_text("DEFINER")
        },
    );
    let handler = map(
        rule! {
            HANDLER ~ ^#literal_string
        },
        |(_, handler)| CreateFunctionOption::Handler(handler),
    );
    let packages = map(
        rule! {
            PACKAGES ~ "(" ~ #comma_separated_list0(literal_string) ~ ")"
        },
        |(_, _, packages, _)| CreateFunctionOption::Packages(packages),
    );
    let imports = map(
        rule! {
            IMPORTS ~ "(" ~ #comma_separated_list0(literal_string) ~ ")"
        },
        |(_, _, imports, _)| CreateFunctionOption::Imports(imports),
    );
    let rows = map(
        rule! {
            ROWS ~ #literal_u64
        },
        |(_, rows)| CreateFunctionOption::Rows(rows),
    );
    let capability_profile = map(
        rule! {
            #match_text("CAPABILITY") ~ #match_text("PROFILE") ~ #ident
        },
        |(_, _, profile)| CreateFunctionOption::CapabilityProfile(profile),
    );

    rule!(
        #language
        | #immutable
        | #stable
        | #volatile
        | #strict
        | #security_invoker
        | #security_definer
        | #handler
        | #packages
        | #imports
        | #rows
        | #capability_profile
    )
    .parse(i)
}

fn function_identity(i: Input) -> IResult<FunctionIdentity> {
    map(
        rule! {
            #function_name_ref ~ "(" ~ #comma_separated_list0(type_name) ~ ")"
        },
        |(name, _, arg_types, _)| FunctionIdentity { name, arg_types },
    )
    .parse(i)
}

pub fn row_access_definition(i: Input) -> IResult<RowAccessPolicyDefinition> {
    pub fn row_access_type(i: Input) -> IResult<RowAccessPolicyType> {
        map(rule! { #ident ~ #type_name }, |(name, data_type)| {
            RowAccessPolicyType {
                name: name.to_string(),
                data_type,
            }
        })
        .parse(i)
    }

    let row_access_def = map(
        rule! {
            AS ~ "(" ~ #comma_separated_list1(row_access_type) ~ ")" ~ RETURNS ~ BOOLEAN
            ~ "->" ~ #expr
        },
        |(_, _, parameters, _, _, _, _, definition)| RowAccessPolicyDefinition {
            parameters,
            definition: Box::new(definition),
        },
    );

    rule!(
        #row_access_def: "AS (<arg_name> <arg_type> [ , ... ]) RETURNS BOOLEAN -> <definition expr>"
    )
    .parse(i)
}

pub fn password_set_options(i: Input) -> IResult<PasswordSetOptions> {
    map(
        rule! {
             ( PASSWORD_MIN_LENGTH ~ Eq ~ ^#literal_u64 )?
             ~ ( PASSWORD_MAX_LENGTH ~ Eq ~ ^#literal_u64 )?
             ~ ( PASSWORD_MIN_UPPER_CASE_CHARS ~ Eq ~ ^#literal_u64 )?
             ~ ( PASSWORD_MIN_LOWER_CASE_CHARS ~ Eq ~ ^#literal_u64 )?
             ~ ( PASSWORD_MIN_NUMERIC_CHARS ~ Eq ~ ^#literal_u64 )?
             ~ ( PASSWORD_MIN_SPECIAL_CHARS ~ Eq ~ ^#literal_u64 )?
             ~ ( PASSWORD_MIN_AGE_DAYS ~ Eq ~ ^#literal_u64 )?
             ~ ( PASSWORD_MAX_AGE_DAYS ~ Eq ~ ^#literal_u64 )?
             ~ ( PASSWORD_MAX_RETRIES ~ Eq ~ ^#literal_u64 )?
             ~ ( PASSWORD_LOCKOUT_TIME_MINS ~ Eq ~ ^#literal_u64 )?
             ~ ( PASSWORD_HISTORY ~ Eq ~ ^#literal_u64 )?
             ~ ( COMMENT ~ Eq ~ ^#literal_string)?
        },
        |(
            opt_min_length,
            opt_max_length,
            opt_min_upper_case_chars,
            opt_min_lower_case_chars,
            opt_min_numeric_chars,
            opt_min_special_chars,
            opt_min_age_days,
            opt_max_age_days,
            opt_max_retries,
            opt_lockout_time_mins,
            opt_history,
            opt_comment,
        )| {
            PasswordSetOptions {
                min_length: opt_min_length.map(|opt| opt.2),
                max_length: opt_max_length.map(|opt| opt.2),
                min_upper_case_chars: opt_min_upper_case_chars.map(|opt| opt.2),
                min_lower_case_chars: opt_min_lower_case_chars.map(|opt| opt.2),
                min_numeric_chars: opt_min_numeric_chars.map(|opt| opt.2),
                min_special_chars: opt_min_special_chars.map(|opt| opt.2),
                min_age_days: opt_min_age_days.map(|opt| opt.2),
                max_age_days: opt_max_age_days.map(|opt| opt.2),
                max_retries: opt_max_retries.map(|opt| opt.2),
                lockout_time_mins: opt_lockout_time_mins.map(|opt| opt.2),
                history: opt_history.map(|opt| opt.2),
                comment: opt_comment.map(|opt| opt.2),
            }
        },
    )
    .parse(i)
}

pub fn password_unset_options(i: Input) -> IResult<PasswordUnSetOptions> {
    map(
        rule! {
             PASSWORD_MIN_LENGTH?
             ~ PASSWORD_MAX_LENGTH?
             ~ PASSWORD_MIN_UPPER_CASE_CHARS?
             ~ PASSWORD_MIN_LOWER_CASE_CHARS?
             ~ PASSWORD_MIN_NUMERIC_CHARS?
             ~ PASSWORD_MIN_SPECIAL_CHARS?
             ~ PASSWORD_MIN_AGE_DAYS?
             ~ PASSWORD_MAX_AGE_DAYS?
             ~ PASSWORD_MAX_RETRIES?
             ~ PASSWORD_LOCKOUT_TIME_MINS?
             ~ PASSWORD_HISTORY?
             ~ COMMENT?
        },
        |(
            opt_min_length,
            opt_max_length,
            opt_min_upper_case_chars,
            opt_min_lower_case_chars,
            opt_min_numeric_chars,
            opt_min_special_chars,
            opt_min_age_days,
            opt_max_age_days,
            opt_max_retries,
            opt_lockout_time_mins,
            opt_history,
            opt_comment,
        )| {
            PasswordUnSetOptions {
                min_length: opt_min_length.is_some(),
                max_length: opt_max_length.is_some(),
                min_upper_case_chars: opt_min_upper_case_chars.is_some(),
                min_lower_case_chars: opt_min_lower_case_chars.is_some(),
                min_numeric_chars: opt_min_numeric_chars.is_some(),
                min_special_chars: opt_min_special_chars.is_some(),
                min_age_days: opt_min_age_days.is_some(),
                max_age_days: opt_max_age_days.is_some(),
                max_retries: opt_max_retries.is_some(),
                lockout_time_mins: opt_lockout_time_mins.is_some(),
                history: opt_history.is_some(),
                comment: opt_comment.is_some(),
            }
        },
    )
    .parse(i)
}

pub fn alter_password_action(i: Input) -> IResult<AlterPasswordAction> {
    let set_options = map(
        rule! {
           SET ~ #password_set_options
        },
        |(_, set_options)| AlterPasswordAction::SetOptions(set_options),
    );
    let unset_options = map(
        rule! {
           UNSET ~ #password_unset_options
        },
        |(_, unset_options)| AlterPasswordAction::UnSetOptions(unset_options),
    );

    rule!(
        #set_options
        | #unset_options
    )
    .parse(i)
}

pub fn create_task_option(i: Input) -> IResult<CreateTaskOption> {
    let warehouse_opt = map(
        rule! {
            (WAREHOUSE  ~ "=" ~ #literal_string)
        },
        |(_, _, warehouse)| CreateTaskOption::Warehouse(warehouse),
    );
    let schedule_opt = map(
        rule! {
            SCHEDULE ~ "=" ~ #task_schedule_option
        },
        |(_, _, schedule)| CreateTaskOption::Schedule(schedule),
    );
    let after_opt = map(
        rule! {
            AFTER ~ #comma_separated_list0(literal_string)
        },
        |(_, after)| CreateTaskOption::After(after),
    );
    let when_opt = map(
        rule! {
            WHEN ~ #expr
        },
        |(_, expr)| CreateTaskOption::When(expr),
    );
    let suspend_task_after_num_failures_opt = map(
        rule! {
            SUSPEND_TASK_AFTER_NUM_FAILURES ~ "=" ~ #literal_u64
        },
        |(_, _, num)| CreateTaskOption::SuspendTaskAfterNumFailures(num),
    );
    let error_integration_opt = map(
        rule! {
            ERROR_INTEGRATION ~ "=" ~ #literal_string
        },
        |(_, _, integration)| CreateTaskOption::ErrorIntegration(integration),
    );
    let comment_opt = map(
        rule! {
            (COMMENT | COMMENTS) ~ "=" ~ #literal_string
        },
        |(_, _, comment)| CreateTaskOption::Comment(comment),
    );

    map(
        rule! {
            #warehouse_opt
            | #schedule_opt
            | #after_opt
            | #when_opt
            | #suspend_task_after_num_failures_opt
            | #error_integration_opt
            | #comment_opt
        },
        |opt| opt,
    )
    .parse(i)
}

fn alter_task_set_option(i: Input) -> IResult<AlterTaskSetOption> {
    let warehouse_opt = map(
        rule! {
            (WAREHOUSE  ~ "=" ~ #literal_string)
        },
        |(_, _, warehouse)| AlterTaskSetOption::Warehouse(warehouse),
    );
    let schedule_opt = map(
        rule! {
            SCHEDULE ~ "=" ~ #task_schedule_option
        },
        |(_, _, schedule)| AlterTaskSetOption::Schedule(schedule),
    );
    let suspend_task_after_num_failures_opt = map(
        rule! {
            SUSPEND_TASK_AFTER_NUM_FAILURES ~ "=" ~ #literal_u64
        },
        |(_, _, num)| AlterTaskSetOption::SuspendTaskAfterNumFailures(num),
    );
    let error_integration_opt = map(
        rule! {
            ERROR_INTEGRATION ~ "=" ~ #literal_string
        },
        |(_, _, integration)| AlterTaskSetOption::ErrorIntegration(integration),
    );
    let comment_opt = map(
        rule! {
            (COMMENT | COMMENTS) ~ "=" ~ #literal_string
        },
        |(_, _, comment)| AlterTaskSetOption::Comment(comment),
    );

    map(
        rule! {
            #warehouse_opt
            | #schedule_opt
            | #suspend_task_after_num_failures_opt
            | #error_integration_opt
            | #comment_opt
        },
        |opt| opt,
    )
    .parse(i)
}

pub fn notification_webhook_options(i: Input) -> IResult<NotificationWebhookOptions> {
    let url_option = map(
        rule! {
            URL ~ "=" ~ #literal_string
        },
        |(_, _, v)| ("url".to_string(), v.to_string()),
    );
    let method_option = map(
        rule! {
            METHOD ~ "=" ~ #literal_string
        },
        |(_, _, v)| ("method".to_string(), v.to_string()),
    );
    let auth_option = map(
        rule! {
            AUTHORIZATION_HEADER ~ "=" ~ #literal_string
        },
        |(_, _, v)| ("authorization_header".to_string(), v.to_string()),
    );

    map(
        rule! { ((
        #url_option
        | #method_option
        | #auth_option) ~ ","?)* },
        |opts| {
            NotificationWebhookOptions::from_iter(
                opts.iter().map(|((k, v), _)| (k.to_uppercase(), v.clone())),
            )
        },
    )
    .parse(i)
}

pub fn notification_webhook_clause(i: Input) -> IResult<NotificationWebhookOptions> {
    map(
        rule! { WEBHOOK ~ ^"=" ~ ^"(" ~ ^#notification_webhook_options ~ ^")" },
        |(_, _, _, opts, _)| opts,
    )
    .parse(i)
}

pub fn alter_notification_options(i: Input) -> IResult<AlterNotificationOptions> {
    let enabled = map(
        rule! {
            SET ~ ENABLED ~ ^"=" ~ #literal_bool
        },
        |(_, _, _, enabled)| {
            AlterNotificationOptions::Set(AlterNotificationSetOptions::enabled(enabled))
        },
    );
    let webhook = map(
        rule! {
            SET ~ #notification_webhook_clause
        },
        |(_, webhook)| {
            AlterNotificationOptions::Set(AlterNotificationSetOptions::webhook_opts(webhook))
        },
    );
    let comment = map(
        rule! {
            SET ~ (COMMENT | COMMENTS) ~ ^"=" ~ #literal_string
        },
        |(_, _, _, comment)| {
            AlterNotificationOptions::Set(AlterNotificationSetOptions::comments(comment))
        },
    );
    map(
        rule! {
            #enabled
            | #webhook
            | #comment
        },
        |opts| opts,
    )
    .parse(i)
}
