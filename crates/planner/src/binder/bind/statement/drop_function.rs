// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use itertools::Itertools;
use paro_catalog::entry::CatalogType;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_parser::ast::DropFunctionStmt;

use crate::binder::bind::statement::drop::{BoundDropInfo, DropType};
use crate::binder::bind::type_name::bind_logical_type;
use crate::binder::ir::BoundStatementKind;
use crate::binder::Binder;

fn format_routine_signature(name: &str, arg_types: &[LogicalType]) -> String {
    format!(
        "{}({})",
        name,
        arg_types.iter().map(ToString::to_string).join(", ")
    )
}

pub fn bind_drop_function(
    binder: &mut Binder,
    drop_function: DropFunctionStmt,
) -> Result<BoundStatementKind> {
    let current_database = binder.catalog().name().to_string();
    let arg_types = drop_function
        .identity
        .arg_types
        .iter()
        .map(bind_logical_type)
        .collect::<Result<Vec<_>>>()?;
    let object_name = drop_function.identity.name.name.name.clone();

    let explicit_database = drop_function
        .identity
        .name
        .database
        .as_ref()
        .map(|ident| ident.name.clone());
    let explicit_schema = drop_function
        .identity
        .name
        .schema
        .as_ref()
        .map(|ident| ident.name.clone());

    let (database_name, schema_name) = if explicit_database.is_none() && explicit_schema.is_none() {
        let mut found_name = None;
        for search_entry in binder.session_context().search_path() {
            let catalog_name = if search_entry.catalog.is_empty() {
                current_database.clone()
            } else {
                search_entry.catalog.clone()
            };
            let catalog = if catalog_name == current_database {
                Some(binder.catalog())
            } else {
                binder
                    .session_context()
                    .database(&catalog_name)
                    .map(|db| db.catalog.clone())
            };
            let Some(catalog) = catalog else {
                continue;
            };
            let Ok(entry) = catalog.get_any_entry(
                &binder.catalog_txn_view(),
                &search_entry.schema,
                CatalogType::Routine,
                &object_name,
            ) else {
                continue;
            };
            let routine = entry
                .as_routine()
                .ok_or_else(|| paro_error::wrong_object_type("routine", &object_name))?;
            found_name = Some((
                catalog_name,
                search_entry.schema.clone(),
                routine.find_exact(&arg_types).is_some(),
            ));
            break;
        }

        match found_name {
            Some((database_name, schema_name, true)) => (database_name, schema_name),
            Some((database_name, schema_name, false)) => {
                if drop_function.if_exists {
                    return Ok(BoundStatementKind::Drop(BoundDropInfo {
                        drop_type: DropType::Routine,
                        database_name,
                        schema_name,
                        object_name,
                        if_exists: true,
                        cascade: false,
                        routine_arg_types: arg_types,
                    }));
                }
                return Err(paro_error::function_not_found(format_routine_signature(
                    &object_name,
                    &arg_types,
                )));
            }
            None if drop_function.if_exists => {
                return Ok(BoundStatementKind::Drop(BoundDropInfo {
                    drop_type: DropType::Routine,
                    database_name: current_database.clone(),
                    schema_name: binder.session_context().current_schema().to_string(),
                    object_name,
                    if_exists: true,
                    cascade: false,
                    routine_arg_types: arg_types,
                }));
            }
            None => {
                return Err(paro_error::function_not_found(format_routine_signature(
                    &object_name,
                    &arg_types,
                )));
            }
        }
    } else {
        let database_name = explicit_database.unwrap_or_else(|| current_database.clone());
        if database_name != current_database {
            return Err(paro_error::not_implemented(format!(
                "Cross-database DROP FUNCTION ({database_name})",
            )));
        }
        let schema_name = explicit_schema
            .unwrap_or_else(|| binder.session_context().current_schema().to_string());
        let entry = binder.catalog().get_any_entry(
            &binder.catalog_txn_view(),
            &schema_name,
            CatalogType::Routine,
            &object_name,
        );
        match entry {
            Ok(entry) => {
                let routine = entry
                    .as_routine()
                    .ok_or_else(|| paro_error::wrong_object_type("routine", &object_name))?;
                if routine.find_exact(&arg_types).is_none() && !drop_function.if_exists {
                    return Err(paro_error::function_not_found(format_routine_signature(
                        &object_name,
                        &arg_types,
                    )));
                }
            }
            Err(err) => {
                if !drop_function.if_exists {
                    return Err(err);
                }
            }
        }
        (database_name, schema_name)
    };

    Ok(BoundStatementKind::Drop(BoundDropInfo {
        drop_type: DropType::Routine,
        database_name,
        schema_name,
        object_name,
        if_exists: drop_function.if_exists,
        cascade: false,
        routine_arg_types: arg_types,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binder::test_utils::test_binder_with_search_path;
    use paro_catalog::catalog::Catalog;
    use paro_catalog::entry::{CreateRoutineInfo, OnCreateConflict};
    use paro_catalog::mvcc::CatalogSnapshot;
    use paro_catalog::search_path::CatalogSearchEntry;
    use paro_external::routine::capability::CapabilityProfile;
    use paro_external::routine::env::{DeclaredEnvSpec, PythonRuntimeSelector};
    use paro_external::routine::permission::{PermissionSpec, RoutineSecurityMode};
    use paro_external::routine::spec::{
        PythonEntrypointRef, PythonImplementationRef, RoutineArgument, RoutineExecutionContract,
        RoutineFamily, RoutineImplementationRef, RoutineNullPolicy, RoutineOwner, RoutineReturn,
        RoutineSemantics, RoutineSideEffects, RoutineStability, RowSemantics,
        ScalarRoutineContract, SourceBlobRef,
    };
    use paro_parser::{ast::Statement, parse_one};

    fn parse_drop_function(sql: &str) -> DropFunctionStmt {
        match parse_one(sql).expect("statement should parse").stmt {
            Statement::DropFunction(stmt) => stmt,
            other => panic!("expected DROP FUNCTION, got {:?}", other),
        }
    }

    fn routine_info(schema: &str, name: &str, arg_types: Vec<LogicalType>) -> CreateRoutineInfo {
        CreateRoutineInfo {
            catalog: "paro".to_string(),
            schema: schema.to_string(),
            name: name.to_string(),
            owner: RoutineOwner {
                principal: "paro".to_string(),
            },
            arguments: arg_types
                .into_iter()
                .enumerate()
                .map(|(index, data_type)| RoutineArgument {
                    name: Some(format!("arg{}", index + 1)),
                    data_type,
                })
                .collect(),
            family: RoutineFamily::ScalarBatch,
            return_type: RoutineReturn::Scalar(LogicalType::Integer),
            execution_contract: RoutineExecutionContract::Scalar(ScalarRoutineContract),
            semantics: RoutineSemantics {
                stability: RoutineStability::Immutable,
                null_policy: RoutineNullPolicy::Strict,
                side_effects: RoutineSideEffects::None,
                row_semantics: RowSemantics::RowPreserving,
                may_block: false,
            },
            implementation: RoutineImplementationRef::Python(PythonImplementationRef {
                source_blob: SourceBlobRef {
                    id: format!("inline:{schema}:{name}"),
                    inline_source: "return 1".to_string(),
                },
                entrypoint: PythonEntrypointRef::Batch {
                    handler: "batch".to_string(),
                },
                runtime: PythonRuntimeSelector::SystemDefault,
            }),
            environment: DeclaredEnvSpec::empty(PythonRuntimeSelector::SystemDefault),
            permissions: PermissionSpec {
                security_mode: RoutineSecurityMode::Invoker,
                capability_profile: CapabilityProfile::process_default(),
            },
            on_conflict: OnCreateConflict::ErrorOnConflict,
            sql: format!(
                "CREATE FUNCTION {}.{}() RETURNS INTEGER LANGUAGE python AS $$return 1$$",
                schema, name
            ),
        }
    }

    fn install_routine(
        binder: &Binder,
        schema_name: &str,
        name: &str,
        arg_types: Vec<LogicalType>,
    ) {
        let catalog = binder.catalog();
        catalog.initialize(false);
        let txn = CatalogSnapshot::permanent_writer(u64::MAX);
        if !schema_name.eq_ignore_ascii_case("public")
            && catalog.get_schema(&txn, schema_name).is_err()
        {
            catalog
                .create_schema_with_snapshot(&txn, schema_name)
                .expect("create schema");
        }
        let schema = catalog
            .get_schema(&txn, schema_name)
            .expect("schema should exist");
        schema
            .create_routine(&txn, routine_info(schema_name, name, arg_types))
            .expect("install routine");
    }

    #[test]
    fn drop_function_uses_selected_schema_before_signature_resolution() {
        let mut binder = test_binder_with_search_path(vec![
            CatalogSearchEntry {
                catalog: "".to_string(),
                schema: "s1".to_string(),
            },
            CatalogSearchEntry {
                catalog: "".to_string(),
                schema: "public".to_string(),
            },
        ]);
        install_routine(&binder, "s1", "py_add", vec![LogicalType::Integer]);
        install_routine(&binder, "public", "py_add", vec![LogicalType::BigInt]);

        let stmt = parse_drop_function("DROP FUNCTION py_add(BIGINT)");
        let err = bind_drop_function(&mut binder, stmt).expect_err("expected signature miss");
        assert!(err
            .to_string()
            .contains("function py_add(BIGINT) does not exist"));
    }

    #[test]
    fn drop_function_with_explicit_schema_binds_matching_overload() {
        let mut binder = test_binder_with_search_path(vec![CatalogSearchEntry {
            catalog: "".to_string(),
            schema: "s1".to_string(),
        }]);
        install_routine(&binder, "s1", "py_add", vec![LogicalType::Integer]);
        install_routine(&binder, "public", "py_add", vec![LogicalType::BigInt]);

        let stmt = parse_drop_function("DROP FUNCTION public.py_add(BIGINT)");
        let BoundStatementKind::Drop(bound) =
            bind_drop_function(&mut binder, stmt).expect("bind drop function")
        else {
            panic!("expected bound drop");
        };
        assert_eq!(bound.drop_type, DropType::Routine);
        assert_eq!(bound.schema_name, "public");
        assert_eq!(bound.object_name, "py_add");
        assert_eq!(bound.routine_arg_types, vec![LogicalType::BigInt]);
    }
}
