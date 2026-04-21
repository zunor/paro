// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Logical operator for `CREATE FUNCTION`.

use crate::binder::ir::statement::BoundCreateRoutineInfo;

#[derive(Debug, Clone)]
pub struct CreateRoutine {
    pub info: BoundCreateRoutineInfo,
}

impl CreateRoutine {
    pub fn new(info: BoundCreateRoutineInfo) -> Self {
        Self { info }
    }

    pub fn schema_name(&self) -> &str {
        &self.info.schema_name
    }

    pub fn routine_name(&self) -> &str {
        &self.info.routine_name
    }

    pub fn full_name(&self) -> String {
        format!("{}.{}", self.info.schema_name, self.info.routine_name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_catalog::entry::{CreateRoutineInfo, OnCreateConflict};
    use paro_common::types::LogicalType;
    use paro_routine::{
        CapabilityProfile, DeclaredEnvSpec, PermissionSpec, PythonEntrypointRef,
        PythonImplementationRef, PythonRuntimeSelector, RoutineArgument, RoutineExecutionContract,
        RoutineFamily, RoutineImplementationRef, RoutineNullPolicy, RoutineOwner, RoutineReturn,
        RoutineSecurityMode, RoutineSemantics, RoutineSideEffects, RoutineStability, RowSemantics,
        ScalarRoutineContract, SourceBlobRef,
    };

    fn create_test_info() -> BoundCreateRoutineInfo {
        BoundCreateRoutineInfo {
            database_name: "test".to_string(),
            schema_name: "public".to_string(),
            routine_name: "py_add".to_string(),
            create_info: CreateRoutineInfo {
                catalog: "test".to_string(),
                schema: "public".to_string(),
                name: "py_add".to_string(),
                owner: RoutineOwner {
                    principal: "paro".to_string(),
                },
                arguments: vec![RoutineArgument {
                    name: Some("a".to_string()),
                    data_type: LogicalType::Integer,
                }],
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
                        id: "inline:test".to_string(),
                        inline_source: "return a".to_string(),
                    },
                    entrypoint: PythonEntrypointRef::Batch {
                        handler: "batch".to_string(),
                    },
                    runtime: PythonRuntimeSelector::SystemDefault,
                }),
                environment: DeclaredEnvSpec {
                    runtime: PythonRuntimeSelector::SystemDefault,
                    packages: Vec::new(),
                    imports: Vec::new(),
                },
                permissions: PermissionSpec {
                    security_mode: RoutineSecurityMode::Invoker,
                    capability_profile: CapabilityProfile::process_default(),
                },
                on_conflict: OnCreateConflict::ErrorOnConflict,
                sql: "CREATE FUNCTION public.py_add(a INTEGER) RETURNS INTEGER LANGUAGE python AS $$return a$$".to_string(),
            },
        }
    }

    #[test]
    fn create_routine_exposes_fully_qualified_name() {
        let op = CreateRoutine::new(create_test_info());
        assert_eq!(op.schema_name(), "public");
        assert_eq!(op.routine_name(), "py_add");
        assert_eq!(op.full_name(), "public.py_add");
    }
}
