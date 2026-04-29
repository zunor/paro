// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::WriteClass;
use paro_common::ddl::{DdlObjectKey, DdlObjectKind};
use paro_common::error::{self as paro_error, Result};
use paro_transaction::{LockMode, LockNamespace, LockRequest, LockResource, TableId};
use std::hash::{Hash, Hasher};
use std::sync::Mutex;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatalogEffect {
    MetadataOnly,
    CreateOwnedObject,
    DropOwnedObject,
    AttachSubobject,
    DetachSubobject,
    AlterExistingObject,
    CascadeDropContainer,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeEffect {
    None,
    AttachIndexState,
    DetachIndexState,
    RegisterGraphRuntime,
    UnregisterGraphRuntime,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MixedDmlPolicy {
    AllowDisjoint,
    AllowOnNewObject,
    Deny,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DdlExecutionProfile {
    pub catalog: CatalogEffect,
    pub runtime: RuntimeEffect,
    pub mixed_dml: MixedDmlPolicy,
}

impl DdlExecutionProfile {
    pub const fn metadata_only() -> Self {
        Self {
            catalog: CatalogEffect::MetadataOnly,
            runtime: RuntimeEffect::None,
            mixed_dml: MixedDmlPolicy::AllowDisjoint,
        }
    }

    pub const fn create_owned_object() -> Self {
        Self {
            catalog: CatalogEffect::CreateOwnedObject,
            runtime: RuntimeEffect::None,
            mixed_dml: MixedDmlPolicy::AllowOnNewObject,
        }
    }

    pub const fn drop_owned_object() -> Self {
        Self {
            catalog: CatalogEffect::DropOwnedObject,
            runtime: RuntimeEffect::None,
            mixed_dml: MixedDmlPolicy::Deny,
        }
    }

    pub const fn attach_index_state() -> Self {
        Self {
            catalog: CatalogEffect::AttachSubobject,
            runtime: RuntimeEffect::AttachIndexState,
            mixed_dml: MixedDmlPolicy::Deny,
        }
    }

    pub const fn detach_index_state() -> Self {
        Self {
            catalog: CatalogEffect::DetachSubobject,
            runtime: RuntimeEffect::DetachIndexState,
            mixed_dml: MixedDmlPolicy::AllowDisjoint,
        }
    }

    pub const fn register_graph_runtime() -> Self {
        Self {
            catalog: CatalogEffect::CreateOwnedObject,
            runtime: RuntimeEffect::RegisterGraphRuntime,
            mixed_dml: MixedDmlPolicy::Deny,
        }
    }

    pub const fn unregister_graph_runtime() -> Self {
        Self {
            catalog: CatalogEffect::DropOwnedObject,
            runtime: RuntimeEffect::UnregisterGraphRuntime,
            mixed_dml: MixedDmlPolicy::Deny,
        }
    }

    pub const fn alter_existing_object() -> Self {
        Self {
            catalog: CatalogEffect::AlterExistingObject,
            runtime: RuntimeEffect::None,
            mixed_dml: MixedDmlPolicy::Deny,
        }
    }

    pub const fn cascade_drop_container() -> Self {
        Self {
            catalog: CatalogEffect::CascadeDropContainer,
            runtime: RuntimeEffect::None,
            mixed_dml: MixedDmlPolicy::Deny,
        }
    }

    pub fn lock_requests(
        self,
        namespace: LockNamespace,
        object: &DdlObjectKey,
        dml_targets: &[DdlObjectKey],
    ) -> Vec<LockRequest> {
        ddl_lock_requests(namespace, self, object, dml_targets)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingDdlAdmission {
    pub object: DdlObjectKey,
    pub profile: DdlExecutionProfile,
    pub dml_targets: Vec<DdlObjectKey>,
}

#[derive(Debug, Default)]
struct TxnAdmissionInner {
    pending_ddl: Vec<PendingDdlAdmission>,
}

#[derive(Debug, Default)]
pub struct TxnAdmissionState {
    inner: Mutex<TxnAdmissionInner>,
}

pub fn dml_table_lock_requests(
    namespace: LockNamespace,
    target: &DdlObjectKey,
) -> Vec<LockRequest> {
    vec![
        LockRequest::new(
            schema_resource(namespace, target),
            LockMode::SchemaStability,
        ),
        LockRequest::new(table_resource(namespace, target), LockMode::IX),
    ]
}

pub fn ddl_lock_requests(
    namespace: LockNamespace,
    profile: DdlExecutionProfile,
    object: &DdlObjectKey,
    dml_targets: &[DdlObjectKey],
) -> Vec<LockRequest> {
    let mut requests = Vec::with_capacity(2 + dml_targets.len() * 2);
    match profile.catalog {
        CatalogEffect::CreateOwnedObject => {
            requests.push(LockRequest::new(
                schema_resource(namespace, object),
                LockMode::IX,
            ));
            if object.kind == DdlObjectKind::Table {
                requests.push(LockRequest::new(
                    table_resource(namespace, object),
                    LockMode::X,
                ));
            } else {
                requests.push(LockRequest::new(
                    catalog_object_resource(namespace, object),
                    LockMode::X,
                ));
            }
        }
        CatalogEffect::DropOwnedObject | CatalogEffect::CascadeDropContainer => {
            if object.kind == DdlObjectKind::Schema {
                requests.push(LockRequest::new(
                    schema_resource(namespace, object),
                    LockMode::SchemaModification,
                ));
            } else if object.kind == DdlObjectKind::Table {
                requests.push(LockRequest::new(
                    table_resource(namespace, object),
                    LockMode::X,
                ));
            } else {
                requests.push(LockRequest::new(
                    catalog_object_resource(namespace, object),
                    LockMode::X,
                ));
            }
        }
        CatalogEffect::AttachSubobject | CatalogEffect::DetachSubobject => {
            requests.push(LockRequest::new(
                catalog_object_resource(namespace, object),
                LockMode::X,
            ));
            for target in dml_targets {
                requests.push(LockRequest::new(
                    schema_resource(namespace, target),
                    LockMode::SchemaStability,
                ));
                requests.push(LockRequest::new(
                    table_resource(namespace, target),
                    LockMode::SchemaStability,
                ));
            }
        }
        CatalogEffect::AlterExistingObject => {
            if object.kind == DdlObjectKind::Table {
                requests.push(LockRequest::new(
                    table_resource(namespace, object),
                    LockMode::X,
                ));
            } else if object.kind == DdlObjectKind::Schema {
                requests.push(LockRequest::new(
                    schema_resource(namespace, object),
                    LockMode::SchemaModification,
                ));
            } else {
                requests.push(LockRequest::new(
                    catalog_object_resource(namespace, object),
                    LockMode::X,
                ));
            }
        }
        CatalogEffect::MetadataOnly => {
            requests.push(LockRequest::new(
                catalog_object_resource(namespace, object),
                LockMode::X,
            ));
        }
    }

    match profile.runtime {
        RuntimeEffect::None => {}
        RuntimeEffect::AttachIndexState | RuntimeEffect::DetachIndexState => {
            requests.push(LockRequest::new(
                runtime_resource(namespace, object, 1),
                LockMode::X,
            ));
        }
        RuntimeEffect::RegisterGraphRuntime | RuntimeEffect::UnregisterGraphRuntime => {
            requests.push(LockRequest::new(
                runtime_resource(namespace, object, 2),
                LockMode::X,
            ));
            for target in dml_targets {
                requests.push(LockRequest::new(
                    table_resource(namespace, target),
                    LockMode::SchemaStability,
                ));
            }
        }
    }

    requests.sort_by(|left, right| left.resource.cmp(&right.resource));
    requests.dedup_by(|left, right| {
        if left.resource == right.resource {
            left.mode = left.mode.strongest(right.mode);
            true
        } else {
            false
        }
    });
    requests
}

pub fn schema_resource(namespace: LockNamespace, object: &DdlObjectKey) -> LockResource {
    LockResource::Schema {
        namespace,
        schema_id: hash_schema(object),
    }
}

pub fn table_resource(namespace: LockNamespace, object: &DdlObjectKey) -> LockResource {
    LockResource::Table {
        namespace,
        table_id: TableId::new(hash_object_key(object)),
    }
}

pub fn catalog_object_resource(namespace: LockNamespace, object: &DdlObjectKey) -> LockResource {
    LockResource::CatalogObject {
        namespace,
        object_kind: object_kind_id(&object.kind),
        object_id: hash_object_key(object),
    }
}

fn runtime_resource(
    namespace: LockNamespace,
    object: &DdlObjectKey,
    runtime_kind: u16,
) -> LockResource {
    LockResource::CatalogObject {
        namespace,
        object_kind: 10_000 + runtime_kind,
        object_id: hash_object_key(object),
    }
}

impl TxnAdmissionState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_ddl(&self, rule: PendingDdlAdmission) -> Result<()> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| paro_error::internal("txn admission state poisoned"))?;
        inner.pending_ddl.push(rule);
        Ok(())
    }

    pub fn mark(&self) -> usize {
        self.inner
            .lock()
            .map(|inner| inner.pending_ddl.len())
            .unwrap_or_default()
    }

    pub fn rollback_to_mark(&self, mark: usize) {
        if let Ok(mut inner) = self.inner.lock() {
            inner.pending_ddl.truncate(mark);
        }
    }

    pub fn clear(&self) {
        if let Ok(mut inner) = self.inner.lock() {
            inner.pending_ddl.clear();
        }
    }

    pub fn admit_table_dml(
        &self,
        current_write_class: WriteClass,
        target: &DdlObjectKey,
        is_new_object: bool,
    ) -> Result<()> {
        let inner = self
            .inner
            .lock()
            .map_err(|_| paro_error::internal("txn admission state poisoned"))?;
        if inner.pending_ddl.is_empty()
            || matches!(current_write_class, WriteClass::Clean | WriteClass::HasDml)
        {
            return Ok(());
        }

        let allow_on_new_targets: Vec<_> = inner
            .pending_ddl
            .iter()
            .filter(|rule| rule.profile.mixed_dml == MixedDmlPolicy::AllowOnNewObject)
            .flat_map(|rule| rule.dml_targets.iter())
            .collect();

        if !allow_on_new_targets.is_empty() {
            let allowed = is_new_object
                && allow_on_new_targets
                    .iter()
                    .any(|rule_target| same_object(rule_target, target));
            if !allowed {
                return Err(paro_error::invalid_transaction_state(format!(
                    "DML on {} is only allowed for objects created in the same transaction",
                    describe_table(target)
                )));
            }
        }

        for rule in &inner.pending_ddl {
            if rule.profile.mixed_dml != MixedDmlPolicy::Deny {
                continue;
            }
            if !rule
                .dml_targets
                .iter()
                .any(|rule_target| same_object(rule_target, target))
            {
                continue;
            }
            return Err(paro_error::invalid_transaction_state(format!(
                "cannot modify {} while pending DDL on {} is still uncommitted",
                describe_table(target),
                describe_object(&rule.object),
            )));
        }

        Ok(())
    }
}

fn same_object(left: &DdlObjectKey, right: &DdlObjectKey) -> bool {
    left.kind == right.kind
        && left.database.eq_ignore_ascii_case(&right.database)
        && left
            .schema
            .as_deref()
            .unwrap_or_default()
            .eq_ignore_ascii_case(right.schema.as_deref().unwrap_or_default())
        && left.name.eq_ignore_ascii_case(&right.name)
}

fn hash_schema(object: &DdlObjectKey) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    object.database.to_ascii_lowercase().hash(&mut hasher);
    object
        .schema
        .as_deref()
        .unwrap_or_else(|| {
            if object.kind == DdlObjectKind::Schema {
                &object.name
            } else {
                "public"
            }
        })
        .to_ascii_lowercase()
        .hash(&mut hasher);
    hasher.finish()
}

fn hash_object_key(object: &DdlObjectKey) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    object.database.to_ascii_lowercase().hash(&mut hasher);
    object
        .schema
        .as_deref()
        .unwrap_or_default()
        .to_ascii_lowercase()
        .hash(&mut hasher);
    object.name.to_ascii_lowercase().hash(&mut hasher);
    object_kind_id(&object.kind).hash(&mut hasher);
    hasher.finish()
}

fn object_kind_id(kind: &DdlObjectKind) -> u16 {
    match kind {
        DdlObjectKind::Schema => 1,
        DdlObjectKind::Table => 2,
        DdlObjectKind::View => 3,
        DdlObjectKind::Index => 4,
        DdlObjectKind::Sequence => 5,
        DdlObjectKind::Routine => 6,
        DdlObjectKind::PropertyGraph => 7,
        DdlObjectKind::Database => 8,
    }
}

fn describe_table(target: &DdlObjectKey) -> String {
    match target.kind {
        DdlObjectKind::Table => {
            let schema = target.schema.as_deref().unwrap_or("public");
            format!("table \"{}.{}\"", schema, target.name)
        }
        _ => describe_object(target),
    }
}

fn describe_object(target: &DdlObjectKey) -> String {
    let schema = target.schema.as_deref().unwrap_or_default();
    match target.kind {
        DdlObjectKind::Schema => format!("schema \"{}\"", target.name),
        DdlObjectKind::Table => format!("table \"{}.{}\"", schema, target.name),
        DdlObjectKind::View => format!("view \"{}.{}\"", schema, target.name),
        DdlObjectKind::Index => format!("index \"{}.{}\"", schema, target.name),
        DdlObjectKind::Sequence => format!("sequence \"{}.{}\"", schema, target.name),
        DdlObjectKind::Routine => format!("routine \"{}.{}\"", schema, target.name),
        DdlObjectKind::PropertyGraph => format!("property graph \"{}.{}\"", schema, target.name),
        DdlObjectKind::Database => format!("database \"{}\"", target.name),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_transaction::{DatabaseId, LockNamespace};

    fn table(name: &str) -> DdlObjectKey {
        DdlObjectKey::new("main", Some("public"), name, DdlObjectKind::Table)
    }

    fn ns() -> LockNamespace {
        LockNamespace::single_tenant(DatabaseId::new(1))
    }

    #[test]
    fn metadata_only_ddl_allows_disjoint_dml() {
        let state = TxnAdmissionState::new();
        state
            .record_ddl(PendingDdlAdmission {
                object: DdlObjectKey::new("main", None::<String>, "s1", DdlObjectKind::Schema),
                profile: DdlExecutionProfile::metadata_only(),
                dml_targets: Vec::new(),
            })
            .unwrap();

        state
            .admit_table_dml(WriteClass::HasDdl, &table("t1"), false)
            .unwrap();
    }

    #[test]
    fn create_owned_object_only_allows_new_object_dml() {
        let state = TxnAdmissionState::new();
        state
            .record_ddl(PendingDdlAdmission {
                object: table("t1"),
                profile: DdlExecutionProfile::create_owned_object(),
                dml_targets: vec![table("t1")],
            })
            .unwrap();

        state
            .admit_table_dml(WriteClass::HasDdl, &table("t1"), true)
            .unwrap();
        assert!(state
            .admit_table_dml(WriteClass::HasDdl, &table("t2"), false)
            .is_err());
    }

    #[test]
    fn runtime_effect_ddl_blocks_same_table_dml() {
        let state = TxnAdmissionState::new();
        state
            .record_ddl(PendingDdlAdmission {
                object: DdlObjectKey::new("main", Some("public"), "idx_t1", DdlObjectKind::Index),
                profile: DdlExecutionProfile::attach_index_state(),
                dml_targets: vec![table("t1")],
            })
            .unwrap();

        assert!(state
            .admit_table_dml(WriteClass::HasDdl, &table("t1"), false)
            .is_err());
        state
            .admit_table_dml(WriteClass::HasDdl, &table("t2"), false)
            .unwrap();
    }

    #[test]
    fn create_table_profile_derives_schema_intent_and_table_x_locks() {
        let requests = DdlExecutionProfile::create_owned_object().lock_requests(
            ns(),
            &table("t1"),
            &[table("t1")],
        );

        assert!(requests.iter().any(|request| matches!(
            request.resource,
            LockResource::Schema { .. }
        ) && request.mode == LockMode::IX));
        assert!(requests.iter().any(|request| request.resource
            == table_resource(ns(), &table("t1"))
            && request.mode == LockMode::X));
    }

    #[test]
    fn attach_index_profile_derives_table_schema_stability_lock() {
        let index = DdlObjectKey::new("main", Some("public"), "idx_t1", DdlObjectKind::Index);
        let requests =
            DdlExecutionProfile::attach_index_state().lock_requests(ns(), &index, &[table("t1")]);

        assert!(requests.iter().any(|request| request.resource
            == table_resource(ns(), &table("t1"))
            && request.mode == LockMode::SchemaStability));
        assert!(requests.iter().any(|request| matches!(
            request.resource,
            LockResource::CatalogObject { .. }
        ) && request.mode == LockMode::X));
    }
}
