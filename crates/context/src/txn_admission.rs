use crate::WriteClass;
use paro_common::ddl::{DdlObjectKey, DdlObjectKind};
use paro_common::error::{self as paro_error, Result};
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
    AttachIndexRuntime,
    DetachIndexRuntime,
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

    pub const fn attach_index_runtime() -> Self {
        Self {
            catalog: CatalogEffect::AttachSubobject,
            runtime: RuntimeEffect::AttachIndexRuntime,
            mixed_dml: MixedDmlPolicy::Deny,
        }
    }

    pub const fn detach_index_runtime() -> Self {
        Self {
            catalog: CatalogEffect::DetachSubobject,
            runtime: RuntimeEffect::DetachIndexRuntime,
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
        DdlObjectKind::PropertyGraph => format!("property graph \"{}.{}\"", schema, target.name),
        DdlObjectKind::Database => format!("database \"{}\"", target.name),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table(name: &str) -> DdlObjectKey {
        DdlObjectKey::new("main", Some("public"), name, DdlObjectKind::Table)
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
                profile: DdlExecutionProfile::attach_index_runtime(),
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
}
