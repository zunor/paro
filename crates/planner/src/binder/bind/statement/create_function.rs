// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use crate::binder::bind::type_name::bind_logical_type;
use crate::binder::ir::BoundStatementKind;
use crate::binder::Binder;
use paro_catalog::entry::{CreateRoutineInfo, OnCreateConflict};
use paro_common::error::{self as paro_error, Result};
use paro_external::routine::capability::{CapabilityProfile, CapabilityProfilePreset};
use paro_external::routine::env::{DeclaredEnvSpec, PackageRequirement, PythonRuntimeSelector};
use paro_external::routine::permission::{PermissionSpec, RoutineSecurityMode};
use paro_external::routine::spec::{
    PythonEntrypointRef, PythonImplementationRef, RoutineArgument, RoutineExecutionContract,
    RoutineFamily, RoutineImplementationRef, RoutineNullPolicy, RoutineOwner, RoutineReturn,
    RoutineSemantics, RoutineSideEffects, RoutineStability, RowSemantics, ScalarRoutineContract,
    SourceBlobRef, TableRoutineContract,
};
use paro_parser::ast::{
    CreateFunctionStmt, CreateOption, FunctionReturn, FunctionSecurity, FunctionVolatility,
};

#[derive(Debug, Clone)]
pub struct BoundCreateRoutineInfo {
    pub database_name: String,
    pub schema_name: String,
    pub routine_name: String,
    pub create_info: CreateRoutineInfo,
}

impl BoundCreateRoutineInfo {
    pub fn to_create_routine_info(&self) -> CreateRoutineInfo {
        self.create_info.clone()
    }
}

fn inline_source_blob_id(
    schema_name: &str,
    routine_name: &str,
    definition: &str,
    handler: &str,
    packages: &[String],
    imports: &[String],
) -> String {
    let mut hasher = DefaultHasher::new();
    schema_name.hash(&mut hasher);
    routine_name.hash(&mut hasher);
    definition.hash(&mut hasher);
    handler.hash(&mut hasher);
    packages.hash(&mut hasher);
    imports.hash(&mut hasher);
    format!("inline:sip64:{:016x}", hasher.finish())
}

fn is_python_language(language: &str) -> bool {
    matches!(language, "python" | "plpython" | "plpython3u" | "python3")
}

fn routine_stability(volatility: Option<FunctionVolatility>) -> RoutineStability {
    match volatility.unwrap_or(FunctionVolatility::Volatile) {
        FunctionVolatility::Immutable => RoutineStability::Immutable,
        FunctionVolatility::Stable => RoutineStability::Stable,
        FunctionVolatility::Volatile => RoutineStability::Volatile,
    }
}

fn require_routine_privilege(
    binder: &Binder,
    stmt: &CreateFunctionStmt,
    capability_profile: &CapabilityProfile,
) -> Result<()> {
    let auth = binder.session_context().auth();
    if !auth.can_create_routine {
        return Err(paro_error::insufficient_privilege(
            "CREATE FUNCTION ... LANGUAGE python requires CREATE ROUTINE privilege",
        ));
    }
    if stmt.security == FunctionSecurity::Definer && !auth.can_create_elevated_routine {
        return Err(paro_error::insufficient_privilege(
            "SECURITY DEFINER routine creation requires elevated routine privilege",
        ));
    }
    if capability_profile.is_high_risk_override() && !auth.can_create_elevated_routine {
        return Err(paro_error::insufficient_privilege(
            "capability profile override requires elevated routine privilege",
        ));
    }
    Ok(())
}

fn resolve_capability_profile(stmt: &CreateFunctionStmt) -> Result<CapabilityProfile> {
    let Some(profile) = stmt.capability_profile.as_ref() else {
        return Ok(CapabilityProfile::process_default());
    };

    CapabilityProfile::resolve_preset(&profile.name).ok_or_else(|| {
        paro_error::invalid_input(format!(
            "unknown capability profile `{}`; supported profiles: {}",
            profile.name,
            CapabilityProfilePreset::supported_names().join(", ")
        ))
    })
}

pub fn bind_create_function(
    binder: &mut Binder,
    stmt: CreateFunctionStmt,
) -> Result<BoundStatementKind> {
    let sql = stmt.to_string();
    let database_name = stmt
        .name
        .database
        .as_ref()
        .map(|ident| ident.name.clone())
        .unwrap_or_else(|| binder.catalog().name().to_string());
    if database_name != binder.catalog().name() {
        return Err(paro_error::not_implemented(format!(
            "Cross-database CREATE FUNCTION ({database_name})",
        )));
    }

    let schema_name = stmt
        .name
        .schema
        .as_ref()
        .map(|ident| ident.name.clone())
        .unwrap_or_else(|| binder.session_context().current_schema().to_string());
    let routine_name = stmt.name.name.name.clone();

    let _schema = binder
        .catalog()
        .get_schema(&binder.catalog_txn_view(), &schema_name)?;

    let language = stmt.language.name.to_ascii_lowercase();
    if !is_python_language(&language) {
        return Err(paro_error::not_supported(format!(
            "CREATE FUNCTION LANGUAGE {}",
            stmt.language.name
        )));
    }

    let capability_profile = resolve_capability_profile(&stmt)?;
    require_routine_privilege(binder, &stmt, &capability_profile)?;
    binder
        .session_context()
        .ensure_python_runtime_ready_for_ddl()?;

    let arguments = stmt
        .arguments
        .iter()
        .map(|argument| {
            Ok(RoutineArgument {
                name: argument.name.as_ref().map(|ident| ident.name.clone()),
                data_type: bind_logical_type(&argument.data_type)?,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let (family, return_type, execution_contract, row_semantics) = match &stmt.return_type {
        FunctionReturn::Scalar(return_type) => (
            RoutineFamily::ScalarBatch,
            RoutineReturn::Scalar(bind_logical_type(return_type)?),
            RoutineExecutionContract::Scalar(ScalarRoutineContract),
            RowSemantics::RowPreserving,
        ),
        FunctionReturn::Table(columns) => (
            RoutineFamily::TableBatch,
            RoutineReturn::Table(
                columns
                    .iter()
                    .map(|column| {
                        Ok(paro_external::routine::spec::RoutineTableColumn {
                            name: column.name.name.clone(),
                            data_type: bind_logical_type(&column.data_type)?,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?,
            ),
            RoutineExecutionContract::Table(TableRoutineContract {
                rows_hint: stmt.rows,
            }),
            RowSemantics::RelationExpanding,
        ),
    };

    let stability = routine_stability(stmt.volatility.clone());
    let semantics = RoutineSemantics {
        null_policy: if stmt.strict {
            RoutineNullPolicy::Strict
        } else {
            RoutineNullPolicy::CalledOnNullInput
        },
        side_effects: if stability == RoutineStability::Volatile {
            RoutineSideEffects::HasSideEffects
        } else {
            RoutineSideEffects::None
        },
        stability,
        row_semantics,
        may_block: false,
    };

    let handler = stmt.handler.clone().unwrap_or_else(|| "batch".to_string());
    let implementation = RoutineImplementationRef::Python(PythonImplementationRef {
        source_blob: SourceBlobRef {
            id: inline_source_blob_id(
                &schema_name,
                &routine_name,
                &stmt.definition,
                &handler,
                &stmt.packages,
                &stmt.imports,
            ),
            inline_source: stmt.definition.clone(),
        },
        entrypoint: PythonEntrypointRef::Batch { handler },
        runtime: PythonRuntimeSelector::SystemDefault,
    });

    let environment = DeclaredEnvSpec {
        runtime: PythonRuntimeSelector::SystemDefault,
        packages: stmt
            .packages
            .iter()
            .map(|spec| PackageRequirement {
                spec: spec.clone(),
                source: None,
            })
            .collect(),
        imports: stmt
            .imports
            .iter()
            .map(|path| paro_external::routine::env::ImportRef {
                uri: path.clone(),
                expected_digest: None,
                expected_size: None,
            })
            .collect(),
    };

    let permissions = PermissionSpec {
        security_mode: match stmt.security {
            FunctionSecurity::Invoker => RoutineSecurityMode::Invoker,
            FunctionSecurity::Definer => RoutineSecurityMode::Definer,
        },
        capability_profile,
    };

    let on_conflict = match stmt.create_option {
        CreateOption::Create => OnCreateConflict::ErrorOnConflict,
        CreateOption::CreateIfNotExists => OnCreateConflict::IgnoreOnConflict,
        CreateOption::CreateOrReplace => OnCreateConflict::ReplaceOnConflict,
    };

    Ok(BoundStatementKind::CreateRoutine(BoundCreateRoutineInfo {
        database_name,
        schema_name: schema_name.clone(),
        routine_name: routine_name.clone(),
        create_info: CreateRoutineInfo {
            catalog: binder.catalog().name().to_string(),
            schema: schema_name,
            name: routine_name,
            owner: RoutineOwner {
                principal: binder.session_context().current_user().to_string(),
            },
            arguments,
            family,
            return_type,
            execution_contract,
            semantics,
            implementation,
            environment,
            permissions,
            on_conflict,
            sql,
        },
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binder::test_utils::test_binder;
    use paro_context::test_support::TestStatementContextBuilder;
    use paro_external::runtime::host::{
        ExternalRuntimeHost, PythonRuntimeProbe, PythonRuntimeProbeResult,
    };
    use paro_parser::{ast::Statement, parse_one};
    use std::sync::Arc;

    #[derive(Debug)]
    struct DisabledProbe;

    impl PythonRuntimeProbe for DisabledProbe {
        fn probe(&self) -> PythonRuntimeProbeResult {
            PythonRuntimeProbeResult::disabled_by_config("Python runtime is disabled by test")
        }
    }

    fn parse_create_function(sql: &str) -> CreateFunctionStmt {
        match parse_one(sql).expect("statement should parse").stmt {
            Statement::CreateFunction(stmt) => stmt,
            other => panic!("expected CREATE FUNCTION, got {:?}", other),
        }
    }

    #[test]
    fn bind_scalar_function_maps_into_routine_catalog_model() {
        let mut binder = test_binder();
        let stmt = parse_create_function(
            "CREATE FUNCTION public.py_add(a INTEGER, b INTEGER) RETURNS INTEGER \
             LANGUAGE python IMMUTABLE STRICT HANDLER 'vectorized' \
             PACKAGES ('numpy==2.1.1') IMPORTS ('stage://bucket/mod.py') \
             AS $$return a + b$$",
        );

        let BoundStatementKind::CreateRoutine(bound) =
            bind_create_function(&mut binder, stmt).expect("bind create function")
        else {
            panic!("expected create routine bound statement");
        };

        assert_eq!(bound.database_name, "paro");
        assert_eq!(bound.schema_name, "public");
        assert_eq!(bound.routine_name, "py_add");
        assert_eq!(bound.create_info.owner.principal, "paro");
        assert_eq!(bound.create_info.arguments.len(), 2);
        assert!(matches!(
            bound.create_info.family,
            RoutineFamily::ScalarBatch
        ));
        assert!(matches!(
            bound.create_info.return_type,
            RoutineReturn::Scalar(_)
        ));
        assert!(matches!(
            bound.create_info.execution_contract,
            RoutineExecutionContract::Scalar(_)
        ));
        assert_eq!(bound.create_info.environment.packages.len(), 1);
        assert_eq!(bound.create_info.environment.imports.len(), 1);
        assert!(matches!(
            bound.create_info.permissions.security_mode,
            RoutineSecurityMode::Invoker
        ));
    }

    #[test]
    fn bind_table_function_preserves_rows_hint() {
        let mut binder = test_binder();
        let stmt = parse_create_function(
            "CREATE FUNCTION py_expand(a INTEGER) RETURNS TABLE (value INTEGER) \
             LANGUAGE python ROWS 128 AS $$yield a$$",
        );

        let BoundStatementKind::CreateRoutine(bound) =
            bind_create_function(&mut binder, stmt).expect("bind create function")
        else {
            panic!("expected create routine bound statement");
        };

        assert!(matches!(
            bound.create_info.family,
            RoutineFamily::TableBatch
        ));
        let RoutineExecutionContract::Table(contract) = &bound.create_info.execution_contract
        else {
            panic!("expected table execution contract");
        };
        assert_eq!(contract.rows_hint, Some(128));
    }

    #[test]
    fn bind_create_function_requires_create_routine_privilege() {
        let context = TestStatementContextBuilder::minimal()
            .with_current_user("alice")
            .with_routine_creation_privilege(false)
            .build();
        let mut binder = Binder::new(context);
        let stmt = parse_create_function(
            "CREATE FUNCTION py_add(a INTEGER) RETURNS INTEGER LANGUAGE python AS $$return a$$",
        );
        let err = bind_create_function(&mut binder, stmt).expect_err("expected privilege error");
        assert!(err.to_string().contains("CREATE ROUTINE"));
    }

    #[test]
    fn bind_security_definer_requires_elevated_privilege() {
        let context = TestStatementContextBuilder::minimal()
            .with_routine_creation_privilege(true)
            .with_elevated_routine_creation_privilege(false)
            .build();
        let mut binder = Binder::new(context);
        let stmt = parse_create_function(
            "CREATE FUNCTION py_add(a INTEGER) RETURNS INTEGER LANGUAGE python \
             SECURITY DEFINER AS $$return a$$",
        );
        let err = bind_create_function(&mut binder, stmt).expect_err("expected privilege error");
        assert!(err.to_string().contains("SECURITY DEFINER"));
    }

    #[test]
    fn bind_capability_profile_override_requires_elevated_privilege() {
        let context = TestStatementContextBuilder::minimal()
            .with_routine_creation_privilege(true)
            .with_elevated_routine_creation_privilege(false)
            .build();
        let mut binder = Binder::new(context);
        let stmt = parse_create_function(
            "CREATE FUNCTION py_add(a INTEGER) RETURNS INTEGER LANGUAGE python \
             CAPABILITY PROFILE trusted_subinterpreter AS $$return a$$",
        );
        let err = bind_create_function(&mut binder, stmt).expect_err("expected privilege error");
        assert!(err.to_string().contains("capability profile override"));
    }

    #[test]
    fn bind_create_function_rejects_unknown_capability_profile() {
        let mut binder = test_binder();
        let stmt = parse_create_function(
            "CREATE FUNCTION py_add(a INTEGER) RETURNS INTEGER LANGUAGE python \
             CAPABILITY PROFILE trusted AS $$return a$$",
        );
        let err = bind_create_function(&mut binder, stmt).expect_err("expected invalid profile");
        assert!(err
            .to_string()
            .contains("unknown capability profile `trusted`"));
    }

    #[test]
    fn bind_create_function_fails_when_python_runtime_is_unavailable() {
        let runtime = Arc::new(ExternalRuntimeHost::new().with_probe(Arc::new(DisabledProbe)));
        let context = TestStatementContextBuilder::minimal()
            .with_python_runtime(runtime)
            .build();
        let mut binder = Binder::new(context);
        let stmt = parse_create_function(
            "CREATE FUNCTION py_add(a INTEGER) RETURNS INTEGER LANGUAGE python AS $$return a$$",
        );
        let err = bind_create_function(&mut binder, stmt)
            .expect_err("expected runtime availability error");
        assert!(err.to_string().contains("Python runtime"));
        assert!(err.to_string().contains("disabled"));
    }
}
