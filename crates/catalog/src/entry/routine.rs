// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::catalog_entry::{
    allocate_object_id, AlterInfo, CatalogEntry, CatalogObjectId, CatalogType, CreateInfo,
    DependencyList, InCatalogEntry, OnCreateConflict, SchemaEntryMeta, StandardEntry,
};
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_routine::{
    resolve_best_match, DeclaredEnvSpec, PermissionSpec, RoutineArgument, RoutineExecutionContract,
    RoutineFamily, RoutineId, RoutineIdentity, RoutineImplementationRef, RoutineOwner,
    RoutineReturn, RoutineSemantics, RoutineSignature, RoutineSpec,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Weak};

#[derive(Debug, Clone)]
pub struct CreateRoutineInfo {
    pub catalog: String,
    pub schema: String,
    pub name: String,
    pub owner: RoutineOwner,
    pub arguments: Vec<RoutineArgument>,
    pub family: RoutineFamily,
    pub return_type: RoutineReturn,
    pub execution_contract: RoutineExecutionContract,
    pub semantics: RoutineSemantics,
    pub implementation: RoutineImplementationRef,
    pub environment: DeclaredEnvSpec,
    pub permissions: PermissionSpec,
    pub on_conflict: OnCreateConflict,
    pub sql: String,
}

impl CreateRoutineInfo {
    pub fn signature(&self) -> RoutineSignature {
        RoutineSignature {
            argument_types: self
                .arguments
                .iter()
                .map(|arg| arg.data_type.clone())
                .collect(),
        }
    }

    pub fn materialize_spec(
        &self,
        identity: RoutineIdentity,
        schema: impl Into<String>,
        name: impl Into<String>,
    ) -> RoutineSpec {
        RoutineSpec {
            identity,
            name: name.into(),
            schema: schema.into(),
            owner: self.owner.clone(),
            arguments: self.arguments.clone(),
            family: self.family.clone(),
            return_type: self.return_type.clone(),
            execution_contract: self.execution_contract.clone(),
            semantics: self.semantics.clone(),
            implementation: self.implementation.clone(),
            environment: self.environment.clone(),
            permissions: self.permissions.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct DropRoutineInfo {
    pub arg_types: Vec<LogicalType>,
    pub if_exists: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredRoutineOverload {
    pub spec: RoutineSpec,
    pub sql: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct SerializedRoutineCatalogEntry {
    object_id: u64,
    timestamp: u64,
    schema_name: String,
    name: String,
    temporary: bool,
    overloads: Vec<StoredRoutineOverload>,
}

#[derive(Debug)]
pub struct RoutineCatalogEntry {
    pub base: SchemaEntryMeta,
    overloads: Vec<StoredRoutineOverload>,
}

impl RoutineCatalogEntry {
    pub fn new(info: CreateRoutineInfo, timestamp: u64, catalog: String) -> Self {
        Self::with_object_id(info, timestamp, catalog, allocate_object_id())
    }

    pub fn with_object_id(
        info: CreateRoutineInfo,
        timestamp: u64,
        catalog: String,
        object_id: CatalogObjectId,
    ) -> Self {
        let base = SchemaEntryMeta::new(
            CatalogType::Routine,
            catalog,
            info.schema.clone(),
            info.name.clone(),
            object_id,
            timestamp,
        );
        base.set_dependencies(DependencyList::new());

        let overload = StoredRoutineOverload {
            spec: info.materialize_spec(
                RoutineIdentity {
                    id: RoutineId::from_raw(allocate_object_id().raw()),
                    generation: 1,
                },
                info.schema.clone(),
                info.name.clone(),
            ),
            sql: info.sql,
        };

        Self {
            base,
            overloads: vec![overload],
        }
    }

    pub fn with_overloads(
        catalog: String,
        schema_name: String,
        name: String,
        object_id: CatalogObjectId,
        timestamp: u64,
        overloads: Vec<StoredRoutineOverload>,
    ) -> Self {
        let base = SchemaEntryMeta::new(
            CatalogType::Routine,
            catalog,
            schema_name,
            name,
            object_id,
            timestamp,
        );
        base.set_dependencies(DependencyList::new());
        Self { base, overloads }
    }

    pub fn overloads(&self) -> &[StoredRoutineOverload] {
        &self.overloads
    }

    pub fn find_exact(&self, arg_types: &[LogicalType]) -> Option<&StoredRoutineOverload> {
        self.overloads
            .iter()
            .find(|overload| overload.spec.signature().exact_match(arg_types))
    }

    pub fn resolve(&self, arg_types: &[LogicalType]) -> Result<&StoredRoutineOverload> {
        let spec = resolve_best_match(
            self.overloads.iter().map(|overload| &overload.spec),
            arg_types,
        )?;
        self.overloads
            .iter()
            .find(|overload| overload.spec.identity == spec.identity)
            .ok_or_else(|| paro_error::internal("routine overload resolution lost matched entry"))
    }

    pub fn serialize_to_bytes(&self) -> Result<Vec<u8>> {
        let payload = SerializedRoutineCatalogEntry {
            object_id: self.base.base.object_id.raw(),
            timestamp: self.base.base.timestamp(),
            schema_name: self.base.schema_name.clone(),
            name: self.base.base.name.clone(),
            temporary: self.base.base.temporary,
            overloads: self.overloads.clone(),
        };
        bincode::serialize(&payload).map_err(|error| {
            paro_error::serialization_error(format!(
                "failed to serialize routine entry {}.{}: {error}",
                self.base.schema_name, self.base.base.name
            ))
        })
    }

    pub fn deserialize(bytes: &[u8], catalog: String) -> Result<Self> {
        let payload: SerializedRoutineCatalogEntry =
            bincode::deserialize(bytes).map_err(|error| {
                paro_error::serialization_error(format!(
                    "failed to deserialize routine entry: {error}"
                ))
            })?;
        let mut entry = Self::with_overloads(
            catalog,
            payload.schema_name,
            payload.name,
            CatalogObjectId::from_raw(payload.object_id),
            payload.timestamp,
            payload.overloads,
        );
        entry.base.base.temporary = payload.temporary;
        Ok(entry)
    }

    pub fn render_sql(&self) -> String {
        self.overloads
            .iter()
            .map(|overload| overload.sql.clone())
            .collect::<Vec<_>>()
            .join("\n")
    }
}

impl CatalogEntry for RoutineCatalogEntry {
    fn object_id(&self) -> CatalogObjectId {
        self.base.base.object_id
    }

    fn name(&self) -> &str {
        &self.base.base.name
    }

    fn entry_type(&self) -> CatalogType {
        CatalogType::Routine
    }

    fn catalog_name(&self) -> &str {
        &self.base.base.catalog
    }

    fn timestamp(&self) -> u64 {
        self.base.base.timestamp()
    }

    fn set_timestamp(&self, ts: u64) {
        self.base.base.set_timestamp(ts);
    }

    fn is_deleted(&self) -> bool {
        self.base.base.is_deleted()
    }

    fn set_deleted(&self, deleted: bool) {
        self.base.base.set_deleted(deleted);
    }

    fn child(&self) -> Option<Arc<dyn CatalogEntry>> {
        self.base.base.child()
    }

    fn set_child(&self, child: Option<Arc<dyn CatalogEntry>>) {
        self.base.base.set_child(child);
    }

    fn parent(&self) -> Option<Arc<dyn CatalogEntry>> {
        self.base.base.parent()
    }

    fn set_parent(&self, parent: Option<Weak<dyn CatalogEntry>>) {
        self.base.base.set_parent(parent);
    }

    fn is_temporary(&self) -> bool {
        self.base.base.temporary
    }

    fn is_internal(&self) -> bool {
        self.base.base.internal
    }

    fn comment(&self) -> Option<&str> {
        None
    }

    fn set_comment(&self, comment: Option<String>) {
        self.base.base.set_comment(comment);
    }

    fn tags(&self) -> &HashMap<String, String> {
        static EMPTY: LazyLock<HashMap<String, String>> = LazyLock::new(HashMap::new);
        &EMPTY
    }

    fn set_tags(&self, tags: HashMap<String, String>) {
        self.base.base.set_tags(tags);
    }

    fn alter(&self, _info: &AlterInfo) -> Result<Arc<dyn CatalogEntry>> {
        Err(paro_error::not_implemented("ALTER ROUTINE"))
    }

    fn undo_alter(&self, _info: &AlterInfo) -> Result<()> {
        Ok(())
    }

    fn rollback(&self, _prev_entry: &dyn CatalogEntry) -> Result<()> {
        Ok(())
    }

    fn copy(&self) -> Result<Arc<dyn CatalogEntry>> {
        Ok(Arc::new(Self::with_overloads(
            self.base.base.catalog.clone(),
            self.base.schema_name.clone(),
            self.base.base.name.clone(),
            self.base.base.object_id,
            self.base.base.timestamp(),
            self.overloads.clone(),
        )))
    }

    fn get_info(&self) -> Result<CreateInfo> {
        Ok(CreateInfo::new(
            CatalogType::Routine,
            self.base.base.catalog.clone(),
            self.base.schema_name.clone(),
            self.base.base.name.clone(),
        ))
    }

    fn set_as_root(&self) {}

    fn to_sql(&self) -> String {
        self.render_sql()
    }

    fn serialize(&self, writer: &mut dyn std::io::Write) -> Result<()> {
        writer.write_all(&self.serialize_to_bytes()?)?;
        Ok(())
    }
}

impl StandardEntry for RoutineCatalogEntry {
    fn schema_name(&self) -> &str {
        &self.base.schema_name
    }

    fn dependencies(&self) -> &DependencyList {
        static EMPTY: LazyLock<DependencyList> = LazyLock::new(DependencyList::new);
        &EMPTY
    }

    fn set_dependencies(&self, dependencies: DependencyList) {
        self.base.set_dependencies(dependencies);
    }
}

impl InCatalogEntry for RoutineCatalogEntry {}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_routine::{
        CapabilityProfile, PackageRequirement, PythonEntrypointRef, PythonImplementationRef,
        PythonRuntimeSelector, RoutineNullPolicy, RoutineSecurityMode, RoutineSideEffects,
        RoutineStability, RowSemantics, ScalarRoutineContract, SourceBlobRef,
    };

    fn create_info() -> CreateRoutineInfo {
        CreateRoutineInfo {
            catalog: "main".to_string(),
            schema: "public".to_string(),
            name: "py_add".to_string(),
            owner: RoutineOwner {
                principal: "paro".to_string(),
            },
            arguments: vec![
                RoutineArgument {
                    name: Some("a".to_string()),
                    data_type: LogicalType::Integer,
                },
                RoutineArgument {
                    name: Some("b".to_string()),
                    data_type: LogicalType::Integer,
                },
            ],
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
                    id: "blob:1".to_string(),
                    inline_source: "def batch(ctx, a, b):\n    return a + b".to_string(),
                },
                entrypoint: PythonEntrypointRef::Batch {
                    handler: "batch".to_string(),
                },
                runtime: PythonRuntimeSelector::SystemDefault,
            }),
            environment: DeclaredEnvSpec {
                runtime: PythonRuntimeSelector::SystemDefault,
                packages: vec![PackageRequirement {
                    spec: "numpy==2.1.1".to_string(),
                    source: None,
                }],
                imports: Vec::new(),
            },
            permissions: PermissionSpec {
                security_mode: RoutineSecurityMode::Invoker,
                capability_profile: CapabilityProfile::process_default(),
            },
            on_conflict: OnCreateConflict::ErrorOnConflict,
            sql: "CREATE FUNCTION public.py_add(a INTEGER, b INTEGER) RETURNS INTEGER LANGUAGE plpython3u STRICT AS $$\ndef batch(ctx, a, b):\n    return a + b\n$$".to_string(),
        }
    }

    #[test]
    fn routine_entry_roundtrips() {
        let entry = RoutineCatalogEntry::new(create_info(), 42, "main".to_string());
        let bytes = entry.serialize_to_bytes().expect("serialize");
        let decoded =
            RoutineCatalogEntry::deserialize(&bytes, "main".to_string()).expect("deserialize");
        assert_eq!(decoded.base.base.name, "py_add");
        assert_eq!(decoded.overloads.len(), 1);
        assert_eq!(decoded.overloads[0].spec.identity.generation, 1);
    }

    #[test]
    fn routine_entry_resolves_best_overload_by_signature_cost() {
        let int_info = create_info();
        let int_spec = int_info.materialize_spec(
            RoutineIdentity {
                id: RoutineId::from_raw(11),
                generation: 1,
            },
            "public",
            "py_add",
        );

        let mut bigint_info = create_info();
        bigint_info.arguments = vec![
            RoutineArgument {
                name: Some("a".to_string()),
                data_type: LogicalType::BigInt,
            },
            RoutineArgument {
                name: Some("b".to_string()),
                data_type: LogicalType::BigInt,
            },
        ];
        bigint_info.return_type = RoutineReturn::Scalar(LogicalType::BigInt);
        let bigint_spec = bigint_info.materialize_spec(
            RoutineIdentity {
                id: RoutineId::from_raw(22),
                generation: 1,
            },
            "public",
            "py_add",
        );

        let entry = RoutineCatalogEntry::with_overloads(
            "main".to_string(),
            "public".to_string(),
            "py_add".to_string(),
            CatalogObjectId::from_raw(7),
            0,
            vec![
                StoredRoutineOverload {
                    spec: bigint_spec,
                    sql: "CREATE FUNCTION ... BIGINT".to_string(),
                },
                StoredRoutineOverload {
                    spec: int_spec,
                    sql: "CREATE FUNCTION ... INTEGER".to_string(),
                },
            ],
        );

        let resolved = entry
            .resolve(&[LogicalType::Integer, LogicalType::Integer])
            .expect("resolve integer overload");

        assert_eq!(resolved.spec.identity.id.raw(), 11);
        assert_eq!(
            resolved.spec.signature().argument_types,
            vec![LogicalType::Integer, LogicalType::Integer]
        );
    }
}
