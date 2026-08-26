// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_catalog::dependency::{DependencyDelta, DependencyGraph};
use paro_catalog::entry::{
    AlterEntryAction, AlterEntryInfo, CatalogEntryEnum, CatalogObjectId, CatalogObjectRef,
    CatalogType, ColumnDefinition, Constraint, ConstraintType, CreateIndexInfo,
    CreatePropertyGraphInfo, CreateRoutineInfo, CreateSchemaInfo, CreateSequenceInfo,
    CreateTableInfo, CreateViewInfo, Dependency, DependencyType, DropEntryInfo, DropRoutineInfo,
    IndexBuildState, IndexCatalogEntry, OnCreateConflict, PropertyGraphCatalogEntry,
    RoutineCatalogEntry, SchemaEntry, SequenceCatalogEntry, StoredRoutineOverload,
    TableCatalogEntry, ViewCatalogEntry,
};
use paro_catalog::mvcc::CatalogSnapshot;
use paro_common::ddl::{
    AlterEntryPayload, CreateIndexPayload, CreatePropertyGraphPayload, CreateRoutinePayload,
    CreateSchemaPayload, CreateSequencePayload, CreateTablePayload, CreateViewPayload, DdlChange,
    DdlChangeRecord, DdlDependencyObjectRef, DdlDependencyRef, DdlObjectKey, DdlObjectKind,
    DdlStorageDescriptor, DdlWalColumnInfo, DdlWalConstraint, DropIndexPayload,
    DropPropertyGraphPayload, DropRoutinePayload, DropSchemaPayload, DropSequencePayload,
    DropTablePayload, DropViewPayload, PropertyGraphEdgePayload, PropertyGraphVertexPayload,
};
use paro_common::effect::{
    CleanupDescriptor, RuntimeTransitionDescriptor, StagedArtifactDescriptor, StagingArtifactId,
    StorageCommitOp, TabletApplyOp, TabletMutation,
};
use paro_common::error::{self as paro_error, Result};
use paro_context::{
    DdlApplyContext, DdlExecutionProfile, IndexBuildHandle, PendingDdlAdmission,
    PreparedIndexArtifact, StatementCancellation, TxnAdmissionState, WriteGuard,
};
use paro_instance::DatabaseHandle;
use paro_storage::search::{
    SearchBuildStopCheck, SearchFreshnessPolicy, SearchIndexDefinition, SearchIndexKind,
    StagedSearchGeneration,
};
use paro_storage::table::table_factory::TableFactory;
use paro_storage::table::table_handle::TableColumnSpec;
use paro_storage::transaction::txn::Transaction;
use paro_transaction::{DatabaseId, LockNamespace};
use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use crate::transaction::ddl_changes::{
    CatalogOpBatch, IndexPostCommitAction, PreparedCatalogOp, SearchGenerationRetirementAction,
    TableDropCleanupAction, TransientCatalogRuntime,
};
use crate::transaction::index_backfill::{lease_index_backfill, IndexBackfillPlan};

pub struct SessionDdlBridge {
    db: Arc<DatabaseHandle>,
    ddl_state: Arc<Mutex<CatalogOpBatch>>,
    txn_admission: Arc<TxnAdmissionState>,
    txn_write: Arc<WriteGuard>,
    active_txn: Arc<Transaction>,
    txn_id: u64,
    start_time: u64,
}

struct SessionCreateIndexHandle {
    info: CreateIndexInfo,
    table: Arc<TableCatalogEntry>,
    entry: Option<Arc<IndexCatalogEntry>>,
    catalog: Option<paro_catalog::collection::StagedCatalogMutation>,
    dependencies: Option<DependencyDelta>,
    backfill: Option<IndexBackfillPlan>,
    staged_search_generation: Option<Arc<StagedSearchGeneration>>,
    skip_build: bool,
}

impl IndexBuildHandle for SessionCreateIndexHandle {
    fn into_any(self: Box<Self>) -> Box<dyn std::any::Any> {
        self
    }

    fn skip_build(&self) -> bool {
        self.skip_build
    }
}

fn search_index_kind(index_type: paro_catalog::entry::IndexType) -> Option<SearchIndexKind> {
    match index_type {
        paro_catalog::entry::IndexType::HNSW => Some(SearchIndexKind::Hnsw),
        paro_catalog::entry::IndexType::Sparse => Some(SearchIndexKind::Sparse),
        paro_catalog::entry::IndexType::FullText => Some(SearchIndexKind::FullText),
        _ => None,
    }
}

fn search_index_expression(info: &CreateIndexInfo) -> Option<String> {
    if info.index_type != paro_catalog::entry::IndexType::FullText {
        return None;
    }
    let binding = info.fulltext.as_ref()?;
    let column_id = info.column_ids.first()?.index;
    Some(format!(
        "to_tsvector('{}', col_{})",
        binding.config, column_id
    ))
}

impl SessionDdlBridge {
    pub fn new(
        db: Arc<DatabaseHandle>,
        ddl_state: Arc<Mutex<CatalogOpBatch>>,
        txn_admission: Arc<TxnAdmissionState>,
        txn_write: Arc<WriteGuard>,
        active_txn: Arc<Transaction>,
        txn_id: u64,
        start_time: u64,
    ) -> Self {
        Self {
            db,
            ddl_state,
            txn_admission,
            txn_write,
            active_txn,
            txn_id,
            start_time,
        }
    }

    fn record_change(&self, change: PreparedCatalogOp) -> Result<()> {
        self.record_change_inner(change, true)
    }

    fn record_change_with_locks_held(&self, change: PreparedCatalogOp) -> Result<()> {
        self.record_change_inner(change, false)
    }

    fn record_change_inner(
        &self,
        mut change: PreparedCatalogOp,
        acquire_locks: bool,
    ) -> Result<()> {
        if acquire_locks {
            if let Err(error) = self
                .active_txn
                .acquire_lock_requests(change.profile.lock_requests(
                    self.active_txn.lock_namespace(),
                    &change.record.key,
                    &change.dml_targets,
                ))
            {
                Self::discard_unrecorded_change(&mut change);
                return Err(error);
            }
        }
        let admission_mark = self.txn_admission.mark();
        if let Err(error) = self.txn_admission.record_ddl(PendingDdlAdmission {
            object: change.record.key.clone(),
            profile: change.profile,
            dml_targets: change.dml_targets.clone(),
        }) {
            Self::discard_unrecorded_change(&mut change);
            return Err(error);
        }
        let mut ddl_state = match self.ddl_state.lock() {
            Ok(ddl_state) => ddl_state,
            Err(_) => {
                self.txn_admission.rollback_to_mark(admission_mark);
                Self::discard_unrecorded_change(&mut change);
                return Err(paro_error::internal("ddl state poisoned"));
            }
        };
        ddl_state.record(change);
        Ok(())
    }

    fn discard_unrecorded_change(change: &mut PreparedCatalogOp) {
        if let Some(delta) = change.dependencies.take() {
            delta.discard();
        }
        if let Some(catalog) = change.catalog.take() {
            if let Err(error) = catalog.discard() {
                tracing::warn!(
                    target: paro_common::logging::targets::TRANSACTION,
                    error = %error,
                    "failed to discard unrecorded catalog mutation"
                );
            }
        }
    }

    fn discard_index_build_staging(handle: &mut SessionCreateIndexHandle) {
        if let Some(delta) = handle.dependencies.take() {
            delta.discard();
        }
        if let Some(catalog) = handle.catalog.take() {
            if let Err(error) = catalog.discard() {
                tracing::warn!(
                    target: paro_common::logging::targets::TRANSACTION,
                    index = %handle.info.name,
                    error = %error,
                    "failed to discard CREATE INDEX catalog staging"
                );
            }
        }
        handle.staged_search_generation.take();
        handle.backfill.take();
    }

    fn begin_object_ddl(&self) -> Result<()> {
        self.txn_write
            .begin_object_ddl_in_database(DatabaseId::new(self.db.id()), self.db.name())
    }

    fn ddl_constraint(constraint: &Constraint) -> Result<DdlWalConstraint> {
        let columns = constraint
            .columns
            .iter()
            .map(|value| {
                u32::try_from(*value).map_err(|_| {
                    paro_error::serialization_error(format!(
                        "constraint column index {} out of range",
                        value
                    ))
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let referenced_columns = constraint
            .referenced_columns
            .as_ref()
            .map(|values| {
                values
                    .iter()
                    .map(|value| {
                        u32::try_from(*value).map_err(|_| {
                            paro_error::serialization_error(format!(
                                "referenced constraint column index {} out of range",
                                value
                            ))
                        })
                    })
                    .collect::<Result<Vec<_>>>()
            })
            .transpose()?;

        let constraint_type = match constraint.constraint_type {
            ConstraintType::NotNull => "not_null",
            ConstraintType::Unique => "unique",
            ConstraintType::PrimaryKey => "primary_key",
            ConstraintType::ForeignKey => "foreign_key",
            ConstraintType::Check => "check",
        };

        Ok(DdlWalConstraint {
            constraint_type: constraint_type.to_string(),
            columns,
            expression: constraint.expression.clone(),
            referenced_table: constraint.referenced_table.clone(),
            referenced_columns,
        })
    }

    fn ddl_columns(columns: &[ColumnDefinition]) -> Vec<DdlWalColumnInfo> {
        columns
            .iter()
            .map(|column| DdlWalColumnInfo {
                name: column.name.clone(),
                logical_type: column.logical_type.clone(),
                nullable: !column.not_null,
            })
            .collect()
    }

    fn ddl_storage_descriptor(
        descriptor: Option<&paro_storage::table::storage_descriptor::TableStorageDescriptor>,
    ) -> Option<DdlStorageDescriptor> {
        descriptor.map(|descriptor| DdlStorageDescriptor {
            format_version: descriptor.format_version,
            tablet_id: descriptor.tablet_id,
            table_id: descriptor.table_id,
            partition_id: descriptor.partition_id,
            schema_id: descriptor.schema_id,
            schema_version: descriptor.schema_version,
            schema_hash: descriptor.schema_hash,
            data_dir: descriptor.data_dir.clone(),
            keys_type: descriptor.keys_type,
        })
    }

    fn path_components(path: &std::path::Path) -> Vec<String> {
        path.components()
            .filter_map(|component| match component {
                std::path::Component::Normal(value) => Some(value.to_string_lossy().to_string()),
                std::path::Component::RootDir => Some("/".to_string()),
                _ => None,
            })
            .collect()
    }

    fn table_key(
        &self,
        schema_name: impl Into<String>,
        table_name: impl Into<String>,
    ) -> DdlObjectKey {
        DdlObjectKey::new(
            self.db.name(),
            Some(schema_name.into()),
            table_name.into(),
            DdlObjectKind::Table,
        )
    }

    fn entry_table_key(&self, table: &TableCatalogEntry) -> DdlObjectKey {
        DdlObjectKey::new(
            table.base.base.catalog.clone(),
            Some(table.base.schema_name.clone()),
            table.base.base.name.clone(),
            DdlObjectKind::Table,
        )
    }

    fn reject_if_table_touched(&self, table: &TableCatalogEntry, ddl_label: &str) -> Result<()> {
        if self
            .active_txn
            .has_dml_on_table(table.base.base.object_id.raw())
        {
            return Err(paro_error::invalid_transaction_state(format!(
                "cannot {ddl_label} after DML on table \"{}.{}\" in the same transaction",
                table.base.schema_name, table.base.base.name
            )));
        }
        Ok(())
    }

    fn reject_if_any_table_touched<I>(&self, table_oids: I, ddl_label: &str) -> Result<()>
    where
        I: IntoIterator<Item = u64>,
    {
        if self.active_txn.has_dml_on_any_table(table_oids) {
            return Err(paro_error::invalid_transaction_state(format!(
                "cannot {ddl_label} after DML on a dependent table in the same transaction"
            )));
        }
        Ok(())
    }

    fn staged_entry_object_id(
        handle: &paro_catalog::collection::StagedCatalogMutation,
        ddl_kind: &str,
        name: &str,
    ) -> Result<u64> {
        let Some(entry) = handle.entry() else {
            return Err(paro_error::internal(format!(
                "staged {} entry for \"{}\" is missing object identity",
                ddl_kind, name
            )));
        };
        Ok(match entry.as_ref() {
            CatalogEntryEnum::Schema(schema) => schema.base.object_id.raw(),
            CatalogEntryEnum::Table(table) => table.base.base.object_id.raw(),
            CatalogEntryEnum::View(view) => view.base.base.object_id.raw(),
            CatalogEntryEnum::Index(index) => index.base.base.object_id.raw(),
            CatalogEntryEnum::PropertyGraph(graph) => graph.base.base.object_id.raw(),
            CatalogEntryEnum::Sequence(sequence) => sequence.base.base.object_id.raw(),
            CatalogEntryEnum::Routine(routine) => routine.base.base.object_id.raw(),
            other => {
                return Err(paro_error::internal(format!(
                    "staged {} entry for \"{}\" has unsupported type {}",
                    ddl_kind,
                    name,
                    other.entry_type().as_str()
                )));
            }
        })
    }

    fn schema_object_ref(schema: &SchemaEntry) -> CatalogObjectRef {
        CatalogObjectRef::schema(
            schema.base.object_id,
            schema.base.catalog.clone(),
            schema.base.name.clone(),
        )
    }

    fn staged_entry_object_ref(
        handle: &paro_catalog::collection::StagedCatalogMutation,
        schema_id: Option<CatalogObjectId>,
        ddl_kind: &str,
        name: &str,
    ) -> Result<CatalogObjectRef> {
        handle
            .entry()
            .map(|entry| entry.object_ref(schema_id))
            .ok_or_else(|| {
                paro_error::internal(format!(
                    "staged {} entry for \"{}\" is missing dependency identity",
                    ddl_kind, name
                ))
            })
    }

    fn created_entry_dependency_delta(
        handle: &paro_catalog::collection::StagedCatalogMutation,
        schema: Option<&SchemaEntry>,
        ddl_kind: &str,
        name: &str,
    ) -> Result<DependencyDelta> {
        let mut delta = DependencyDelta::new();
        let schema_id = schema.map(|schema| schema.base.object_id);
        let object_ref = Self::staged_entry_object_ref(handle, schema_id, ddl_kind, name)?;
        let Some(entry) = handle.entry() else {
            return Err(paro_error::internal(format!(
                "staged {} entry for \"{}\" is missing dependency payload",
                ddl_kind, name
            )));
        };
        delta.add_object(object_ref.clone());
        if let Some(schema) = schema {
            delta.add_dependency(
                object_ref.id,
                schema.base.object_id,
                DependencyType::OwnedBy,
            );
        }
        delta.add_dependencies(object_ref.id, &entry.dependency_list());
        Ok(delta)
    }

    fn stage_create_routine_handle(
        &self,
        schema: &SchemaEntry,
        txn: &CatalogSnapshot,
        info: &CreateRoutineInfo,
    ) -> Result<Option<paro_catalog::collection::StagedCatalogMutation>> {
        let collection = schema
            .collection(CatalogType::Routine)
            .expect("routine collection");
        let timestamp = txn.write_timestamp()?;
        let existing = schema.get_routine(self.txn_id, self.start_time, &info.name);

        if let Some(existing_entry) = existing {
            let existing_routine = existing_entry
                .as_routine()
                .ok_or_else(|| paro_error::wrong_object_type("routine", &info.name))?;
            let mut overloads = existing_routine.overloads().to_vec();
            if let Some(index) = overloads.iter().position(|overload| {
                overload
                    .spec
                    .signature()
                    .exact_match(&info.signature().argument_types)
            }) {
                match info.on_conflict {
                    OnCreateConflict::ErrorOnConflict => {
                        return Err(paro_error::object_exists("routine", &info.name));
                    }
                    OnCreateConflict::IgnoreOnConflict => return Ok(None),
                    OnCreateConflict::ReplaceOnConflict | OnCreateConflict::AlterOnConflict => {
                        let previous = &overloads[index];
                        overloads[index] = StoredRoutineOverload {
                            spec: info.materialize_spec(
                                paro_external::routine::spec::RoutineIdentity {
                                    id: previous.spec.identity.id,
                                    generation: previous.spec.identity.generation + 1,
                                },
                                schema.base.name.clone(),
                                info.name.clone(),
                            ),
                            sql: info.sql.clone(),
                        };
                    }
                }
            } else {
                overloads.push(StoredRoutineOverload {
                    spec: info.materialize_spec(
                        paro_external::routine::spec::RoutineIdentity {
                            id: paro_external::routine::spec::RoutineId::from_raw(
                                schema.object_id_allocator().allocate().raw(),
                            ),
                            generation: 1,
                        },
                        schema.base.name.clone(),
                        info.name.clone(),
                    ),
                    sql: info.sql.clone(),
                });
            }

            let replacement = Arc::new(CatalogEntryEnum::Routine(Arc::new(
                RoutineCatalogEntry::with_overloads(
                    self.db.catalog().name().to_string(),
                    schema.base.name.clone(),
                    info.name.clone(),
                    existing_routine.base.base.object_id,
                    timestamp,
                    overloads,
                ),
            )));
            return collection.stage_replace(txn, &info.name, replacement);
        }

        // A routine family and each executable overload have distinct persisted identities.
        let entry = Arc::new(CatalogEntryEnum::Routine(Arc::new(
            RoutineCatalogEntry::new(
                info.clone(),
                timestamp,
                self.db.catalog().name().to_string(),
                schema.object_id_allocator().allocate(),
                paro_external::routine::spec::RoutineId::from_raw(
                    schema.object_id_allocator().allocate().raw(),
                ),
            ),
        )));
        collection.stage_create(txn, &info.name, entry)
    }

    fn rename_dependency_delta(
        handle: &paro_catalog::collection::StagedCatalogMutation,
        source_schema: &SchemaEntry,
        target_schema: &SchemaEntry,
        ddl_kind: &str,
        name: &str,
    ) -> Result<DependencyDelta> {
        let mut delta = DependencyDelta::new();
        let object_ref = Self::staged_entry_object_ref(
            handle,
            Some(target_schema.base.object_id),
            ddl_kind,
            name,
        )?;
        delta.add_object(object_ref.clone());
        if source_schema.base.object_id != target_schema.base.object_id {
            delta.remove_edge(paro_catalog::dependency::DependencyEdgeKey {
                dependent_id: object_ref.id,
                subject_id: source_schema.base.object_id,
                dependency_type: DependencyType::OwnedBy,
            });
            delta.add_dependency(
                object_ref.id,
                target_schema.base.object_id,
                DependencyType::OwnedBy,
            );
        }
        Ok(delta)
    }

    fn dependency_type_label(dependency_type: DependencyType) -> &'static str {
        match dependency_type {
            DependencyType::Regular => "regular",
            DependencyType::Automatic => "automatic",
            DependencyType::Owns => "owns",
            DependencyType::OwnedBy => "owned_by",
        }
    }

    fn ddl_dependency_ref(dependency: &Dependency) -> DdlDependencyRef {
        DdlDependencyRef {
            object: DdlDependencyObjectRef {
                object_id: dependency.entry.id.raw(),
                kind: dependency.entry.kind.as_str().to_string(),
                catalog_name: dependency.entry.catalog_name.clone(),
                schema_id: dependency.entry.schema_id.map(|id| id.raw()),
                schema_name: dependency.entry.schema_name.clone(),
                name: dependency.entry.name.clone(),
            },
            dependency_type: Self::dependency_type_label(dependency.dependency_type).to_string(),
        }
    }

    fn planned_dependency_graph(&self, snapshot: &CatalogSnapshot) -> Result<DependencyGraph> {
        self.db.catalog().build_dependency_graph_snapshot(snapshot)
    }

    fn planned_drop_delta(
        &self,
        snapshot: &CatalogSnapshot,
        object_id: CatalogObjectId,
    ) -> Result<DependencyDelta> {
        Ok(self
            .planned_dependency_graph(snapshot)?
            .drop_delta(object_id))
    }

    fn lookup_drop_object_ref(
        &self,
        snapshot: &CatalogSnapshot,
        schema_name: &str,
        info: &DropEntryInfo,
    ) -> Result<Option<CatalogObjectRef>> {
        match info.entry_type {
            CatalogType::Schema => match self.db.catalog().get_schema(snapshot, &info.name) {
                Ok(schema) => Ok(Some(Self::schema_object_ref(schema.as_ref()))),
                Err(err) if info.if_exists => Ok(None),
                Err(err) => Err(err),
            },
            kind => {
                let schema = self.db.catalog().get_schema(snapshot, schema_name)?;
                let entry = match kind {
                    CatalogType::Table => {
                        schema.get_table(self.txn_id, self.start_time, &info.name)
                    }
                    CatalogType::View => schema.get_view(self.txn_id, self.start_time, &info.name),
                    CatalogType::Index => {
                        schema.get_index(self.txn_id, self.start_time, &info.name)
                    }
                    CatalogType::Sequence => {
                        schema.get_sequence(self.txn_id, self.start_time, &info.name)
                    }
                    _ => None,
                };
                match entry {
                    Some(entry) => Ok(Some(entry.object_ref(Some(schema.base.object_id)))),
                    None if info.if_exists => Ok(None),
                    None => Err(paro_error::object_not_found(kind.as_str(), &info.name)),
                }
            }
        }
    }

    fn stage_drop_planned_object(
        &self,
        snapshot: &CatalogSnapshot,
        object_ref: &CatalogObjectRef,
        cascade_root: bool,
    ) -> Result<Option<PreparedCatalogOp>> {
        let dependencies = Some(self.planned_drop_delta(snapshot, object_ref.id)?);
        match object_ref.kind {
            CatalogType::Schema => {
                let handle = self
                    .db
                    .catalog()
                    .get_schema_collection()
                    .stage_drop(snapshot, &object_ref.name)?;
                Ok(handle.map(|handle| PreparedCatalogOp {
                    record: DdlChangeRecord {
                        key: DdlObjectKey::new(
                            self.db.name(),
                            None::<String>,
                            object_ref.name.clone(),
                            DdlObjectKind::Schema,
                        ),
                        change: DdlChange::DropSchema(DropSchemaPayload {
                            cascade: cascade_root,
                            if_exists: false,
                        }),
                    },
                    profile: if cascade_root {
                        DdlExecutionProfile::cascade_drop_container()
                    } else {
                        DdlExecutionProfile::metadata_only()
                    },
                    catalog: Some(handle),
                    dependencies,
                    dml_targets: Vec::new(),
                    staged_artifacts: Vec::new(),
                    storage_ops: Vec::new(),
                    runtime_transitions: Vec::new(),
                    cleanups: Vec::new(),
                    post_commit_hooks: Vec::new(),
                    transient_runtime: None,
                }))
            }
            CatalogType::Table => {
                let schema_name = object_ref.schema_name.clone().ok_or_else(|| {
                    paro_error::internal("table dependency ref missing schema name")
                })?;
                let schema = self.db.catalog().get_schema(snapshot, &schema_name)?;
                let table = schema
                    .get_table(self.txn_id, self.start_time, &object_ref.name)
                    .and_then(|entry| match entry.as_ref() {
                        CatalogEntryEnum::Table(table) => Some(Arc::clone(table)),
                        _ => None,
                    })
                    .ok_or_else(|| paro_error::object_not_found("table", &object_ref.name))?;
                self.reject_if_table_touched(table.as_ref(), "DROP TABLE")?;
                let handle = schema
                    .collection(CatalogType::Table)
                    .expect("table collection")
                    .stage_drop(snapshot, &object_ref.name)?;
                let transient_runtime = table.get_storage().map(|storage| {
                    TransientCatalogRuntime::DropTable(TableDropCleanupAction {
                        storage: Arc::clone(storage),
                        move_to_trash: true,
                    })
                });
                let cleanups = table
                    .get_storage_descriptor()
                    .cloned()
                    .or_else(|| {
                        table
                            .get_storage()
                            .and_then(|storage| storage.to_descriptor().ok())
                    })
                    .map(|descriptor| {
                        vec![CleanupDescriptor::ShutdownTablet {
                            tablet_id: descriptor.tablet_id,
                            data_dir_components: Self::path_components(std::path::Path::new(
                                &descriptor.data_dir,
                            )),
                            move_to_trash: true,
                        }]
                    })
                    .unwrap_or_default();
                Ok(handle.map(|handle| PreparedCatalogOp {
                    record: DdlChangeRecord {
                        key: DdlObjectKey::new(
                            self.db.name(),
                            Some(schema_name.clone()),
                            object_ref.name.clone(),
                            DdlObjectKind::Table,
                        ),
                        change: DdlChange::DropTable(DropTablePayload { if_exists: false }),
                    },
                    profile: DdlExecutionProfile::drop_owned_object(),
                    catalog: Some(handle),
                    dependencies,
                    dml_targets: vec![self.table_key(schema_name, object_ref.name.clone())],
                    staged_artifacts: Vec::new(),
                    storage_ops: Vec::new(),
                    runtime_transitions: Vec::new(),
                    cleanups,
                    post_commit_hooks: Vec::new(),
                    transient_runtime,
                }))
            }
            CatalogType::View => {
                let schema_name = object_ref.schema_name.clone().ok_or_else(|| {
                    paro_error::internal("view dependency ref missing schema name")
                })?;
                let schema = self.db.catalog().get_schema(snapshot, &schema_name)?;
                let handle = schema
                    .collection(CatalogType::View)
                    .expect("view collection")
                    .stage_drop(snapshot, &object_ref.name)?;
                Ok(handle.map(|handle| PreparedCatalogOp {
                    record: DdlChangeRecord {
                        key: DdlObjectKey::new(
                            self.db.name(),
                            Some(schema_name),
                            object_ref.name.clone(),
                            DdlObjectKind::View,
                        ),
                        change: DdlChange::DropView(DropViewPayload { if_exists: false }),
                    },
                    profile: DdlExecutionProfile::metadata_only(),
                    catalog: Some(handle),
                    dependencies,
                    dml_targets: Vec::new(),
                    staged_artifacts: Vec::new(),
                    storage_ops: Vec::new(),
                    runtime_transitions: Vec::new(),
                    cleanups: Vec::new(),
                    post_commit_hooks: Vec::new(),
                    transient_runtime: None,
                }))
            }
            CatalogType::Index => {
                let schema_name = object_ref.schema_name.clone().ok_or_else(|| {
                    paro_error::internal("index dependency ref missing schema name")
                })?;
                let schema = self.db.catalog().get_schema(snapshot, &schema_name)?;
                let index = schema
                    .get_index(self.txn_id, self.start_time, &object_ref.name)
                    .and_then(|entry| match entry.as_ref() {
                        CatalogEntryEnum::Index(index) => Some(Arc::clone(index)),
                        _ => None,
                    })
                    .ok_or_else(|| paro_error::object_not_found("index", &object_ref.name))?;
                let table = schema
                    .get_table(self.txn_id, self.start_time, &index.table_name)
                    .and_then(|entry| match entry.as_ref() {
                        CatalogEntryEnum::Table(table) => Some(Arc::clone(table)),
                        _ => None,
                    })
                    .ok_or_else(|| paro_error::object_not_found("table", &index.table_name))?;
                self.reject_if_table_touched(table.as_ref(), "DROP INDEX")?;
                let retirement = if search_index_kind(index.index_type).is_some() {
                    let storage = table.get_storage().cloned().ok_or_else(|| {
                        paro_error::internal(format!(
                            "table '{}' has no storage for DROP INDEX retirement",
                            index.table_name
                        ))
                    })?;
                    Some(SearchGenerationRetirementAction {
                        storage,
                        definition_id: index.base.base.object_id.raw(),
                    })
                } else {
                    None
                };
                let storage_ops = retirement
                    .as_ref()
                    .map(|retirement| {
                        vec![StorageCommitOp::Tablet(TabletApplyOp {
                            tablet_id: retirement.storage.tablet_id(),
                            mutations: vec![TabletMutation::RetireSearchGeneration {
                                definition_id: retirement.definition_id,
                            }],
                        })]
                    })
                    .unwrap_or_default();
                let key = DdlObjectKey::new(
                    self.db.name(),
                    Some(schema_name.clone()),
                    object_ref.name.clone(),
                    DdlObjectKind::Index,
                );
                let handle = schema
                    .collection(CatalogType::Index)
                    .expect("index collection")
                    .stage_drop(snapshot, &object_ref.name)?;
                Ok(handle.map(|handle| PreparedCatalogOp {
                    record: DdlChangeRecord {
                        key: key.clone(),
                        change: DdlChange::DropIndex(DropIndexPayload { if_exists: false }),
                    },
                    profile: DdlExecutionProfile::detach_index_state(),
                    catalog: Some(handle),
                    dependencies,
                    dml_targets: vec![self.table_key(schema_name, index.table_name.clone())],
                    staged_artifacts: Vec::new(),
                    storage_ops,
                    runtime_transitions: vec![RuntimeTransitionDescriptor::DetachIndexState {
                        index: key,
                        table_name: index.table_name.clone(),
                        index_type: index.index_type.as_str().to_string(),
                        column_ids: index.column_ids.iter().map(|column| column.index).collect(),
                        fulltext_config: index
                            .fulltext_binding()
                            .map(|binding| binding.config.clone()),
                    }],
                    cleanups: Vec::new(),
                    post_commit_hooks: Vec::new(),
                    transient_runtime: retirement
                        .map(TransientCatalogRuntime::RetireSearchGeneration),
                }))
            }
            CatalogType::Sequence => {
                let schema_name = object_ref.schema_name.clone().ok_or_else(|| {
                    paro_error::internal("sequence dependency ref missing schema name")
                })?;
                let schema = self.db.catalog().get_schema(snapshot, &schema_name)?;
                let handle = schema
                    .collection(CatalogType::Sequence)
                    .expect("sequence collection")
                    .stage_drop(snapshot, &object_ref.name)?;
                Ok(handle.map(|handle| PreparedCatalogOp {
                    record: DdlChangeRecord {
                        key: DdlObjectKey::new(
                            self.db.name(),
                            Some(schema_name),
                            object_ref.name.clone(),
                            DdlObjectKind::Sequence,
                        ),
                        change: DdlChange::DropSequence(DropSequencePayload { if_exists: false }),
                    },
                    profile: DdlExecutionProfile::metadata_only(),
                    catalog: Some(handle),
                    dependencies,
                    dml_targets: Vec::new(),
                    staged_artifacts: Vec::new(),
                    storage_ops: Vec::new(),
                    runtime_transitions: Vec::new(),
                    cleanups: Vec::new(),
                    post_commit_hooks: Vec::new(),
                    transient_runtime: None,
                }))
            }
            CatalogType::PropertyGraph => {
                let schema_name = object_ref.schema_name.clone().ok_or_else(|| {
                    paro_error::internal("property graph dependency ref missing schema name")
                })?;
                let schema = self.db.catalog().get_schema(snapshot, &schema_name)?;
                let graph = schema
                    .get_property_graph(snapshot, &object_ref.name)
                    .map_err(|_| {
                        paro_error::object_not_found("property graph", &object_ref.name)
                    })?;
                self.reject_if_any_table_touched(
                    graph
                        .info
                        .vertex_tables
                        .iter()
                        .map(|vertex| vertex.table_oid)
                        .chain(graph.info.edge_tables.iter().map(|edge| edge.table_oid)),
                    "DROP PROPERTY GRAPH",
                )?;
                let key = DdlObjectKey::new(
                    self.db.name(),
                    Some(schema_name.clone()),
                    object_ref.name.clone(),
                    DdlObjectKind::PropertyGraph,
                );
                let handle = schema
                    .collection(CatalogType::PropertyGraph)
                    .expect("property graph collection")
                    .stage_drop(snapshot, &object_ref.name)?;
                Ok(handle.map(|handle| PreparedCatalogOp {
                    record: DdlChangeRecord {
                        key: key.clone(),
                        change: DdlChange::DropPropertyGraph(DropPropertyGraphPayload {
                            if_exists: false,
                        }),
                    },
                    profile: DdlExecutionProfile::unregister_graph_runtime(),
                    catalog: Some(handle),
                    dependencies,
                    dml_targets: graph
                        .info
                        .vertex_tables
                        .iter()
                        .map(|vertex| {
                            self.table_key(schema_name.clone(), vertex.table_name.clone())
                        })
                        .chain(graph.info.edge_tables.iter().map(|edge| {
                            self.table_key(schema_name.clone(), edge.table_name.clone())
                        }))
                        .collect(),
                    staged_artifacts: Vec::new(),
                    storage_ops: Vec::new(),
                    runtime_transitions: vec![
                        RuntimeTransitionDescriptor::UnregisterGraphRuntime { graph: key },
                    ],
                    cleanups: vec![CleanupDescriptor::RemoveDirectory {
                        path_components: vec![
                            self.db.path().to_string(),
                            "graph".to_string(),
                            object_ref.name.clone(),
                        ],
                        recursive: true,
                    }],
                    post_commit_hooks: Vec::new(),
                    transient_runtime: None,
                }))
            }
            other => Err(paro_error::not_supported(format!(
                "DDL bridge does not support DROP {} yet",
                other.as_str()
            ))),
        }
    }
}

impl DdlApplyContext for SessionDdlBridge {
    fn apply_create_table(&self, mut info: CreateTableInfo) -> Result<()> {
        self.begin_object_ddl()?;
        info.catalog = self.db.catalog().name().to_string();

        let pk_cols: HashSet<usize> = info
            .constraints
            .iter()
            .filter(|constraint| constraint.constraint_type == ConstraintType::PrimaryKey)
            .flat_map(|constraint| constraint.columns.iter().copied())
            .collect();
        let not_null_cols: HashSet<usize> = info
            .constraints
            .iter()
            .filter(|constraint| constraint.constraint_type == ConstraintType::NotNull)
            .flat_map(|constraint| constraint.columns.iter().copied())
            .collect();
        let specs: Vec<TableColumnSpec> = info
            .columns
            .iter()
            .enumerate()
            .map(|(idx, column)| TableColumnSpec {
                name: column.name.clone(),
                logical_type: column.logical_type.clone(),
                is_key: pk_cols.contains(&idx),
                not_null: column.not_null || not_null_cols.contains(&idx) || pk_cols.contains(&idx),
            })
            .collect();
        let tablet_meta = self
            .db
            .tablet_meta_manager()
            .ok_or_else(|| paro_error::internal("database has no tablet meta manager"))?;
        let storage = Arc::new(
            TableFactory::new(Some(tablet_meta))
                .with_transaction_locks(
                    self.db.transaction_manager().lock_manager(),
                    LockNamespace::single_tenant(DatabaseId::new(self.db.id())),
                )
                .create_table_from_specs(&specs)?,
        );

        let schema_txn = CatalogSnapshot::writer(self.txn_id, self.start_time);
        let schema = self.db.catalog().get_schema(&schema_txn, &info.schema)?;
        let table_entry = Arc::new(CatalogEntryEnum::Table(Arc::new(
            TableCatalogEntry::from_info(
                info.clone(),
                storage,
                self.db.catalog().object_id_allocator().allocate(),
                0,
            )?,
        )));
        let handle = schema
            .collection(CatalogType::Table)
            .expect("table collection")
            .stage_create(&schema_txn, &info.name, table_entry)?;

        if let Some(handle) = handle {
            let object_id = Self::staged_entry_object_id(&handle, "CREATE TABLE", &info.name)?;
            let constraints = info
                .constraints
                .iter()
                .map(Self::ddl_constraint)
                .collect::<Result<Vec<_>>>()?;
            let dependencies = Some(Self::created_entry_dependency_delta(
                &handle,
                Some(schema.as_ref()),
                "CREATE TABLE",
                &info.name,
            )?);
            self.record_change(PreparedCatalogOp {
                record: DdlChangeRecord {
                    key: DdlObjectKey::new(
                        self.db.name(),
                        Some(info.schema.clone()),
                        info.name.clone(),
                        DdlObjectKind::Table,
                    ),
                    change: DdlChange::CreateTable(CreateTablePayload {
                        object_id,
                        columns: Self::ddl_columns(&info.columns),
                        constraints,
                        if_not_exists: info.on_conflict == OnCreateConflict::IgnoreOnConflict,
                        storage: handle.entry().and_then(|entry| match entry.as_ref() {
                            CatalogEntryEnum::Table(table) => {
                                let descriptor = table
                                    .get_storage()
                                    .and_then(|storage| storage.to_descriptor().ok())
                                    .or_else(|| table.get_storage_descriptor().cloned());
                                Self::ddl_storage_descriptor(descriptor.as_ref())
                            }
                            _ => None,
                        }),
                    }),
                },
                profile: DdlExecutionProfile::create_owned_object(),
                catalog: Some(handle),
                dependencies,
                dml_targets: vec![self.table_key(info.schema.clone(), info.name.clone())],
                staged_artifacts: Vec::new(),
                storage_ops: Vec::new(),
                runtime_transitions: Vec::new(),
                cleanups: Vec::new(),
                post_commit_hooks: Vec::new(),
                transient_runtime: None,
            })?;
        }

        Ok(())
    }

    fn apply_create_schema(&self, mut info: CreateSchemaInfo) -> Result<()> {
        self.begin_object_ddl()?;
        info.catalog = self.db.catalog().name().to_string();
        let entry = Arc::new(CatalogEntryEnum::Schema(Arc::new(SchemaEntry::from_info(
            &info,
            Arc::clone(self.db.catalog().object_id_allocator()),
            self.db.catalog().gc_epoch_handle(),
            0,
        ))));
        let catalog_txn = CatalogSnapshot::writer(self.txn_id, self.start_time);
        let handle = self.db.catalog().get_schema_collection().stage_create(
            &catalog_txn,
            &info.name,
            entry,
        )?;
        if let Some(handle) = handle {
            let object_id = Self::staged_entry_object_id(&handle, "CREATE SCHEMA", &info.name)?;
            let dependencies = Some(Self::created_entry_dependency_delta(
                &handle,
                None,
                "CREATE SCHEMA",
                &info.name,
            )?);
            self.record_change(PreparedCatalogOp {
                record: DdlChangeRecord {
                    key: DdlObjectKey::new(
                        self.db.name(),
                        None::<String>,
                        info.name.clone(),
                        DdlObjectKind::Schema,
                    ),
                    change: DdlChange::CreateSchema(CreateSchemaPayload {
                        object_id,
                        if_not_exists: info.on_conflict == OnCreateConflict::IgnoreOnConflict,
                    }),
                },
                profile: DdlExecutionProfile::metadata_only(),
                catalog: Some(handle),
                dependencies,
                dml_targets: Vec::new(),
                staged_artifacts: Vec::new(),
                storage_ops: Vec::new(),
                runtime_transitions: Vec::new(),
                cleanups: Vec::new(),
                post_commit_hooks: Vec::new(),
                transient_runtime: None,
            })?;
        }
        Ok(())
    }

    fn apply_create_sequence(&self, mut info: CreateSequenceInfo) -> Result<()> {
        self.begin_object_ddl()?;
        info.catalog = self.db.catalog().name().to_string();
        let txn = CatalogSnapshot::writer(self.txn_id, self.start_time);
        let schema = self.db.catalog().get_schema(&txn, &info.schema)?;
        let entry = Arc::new(CatalogEntryEnum::Sequence(Arc::new(
            SequenceCatalogEntry::new(
                info.clone(),
                0,
                self.db.catalog().name().to_string(),
                self.db.catalog().object_id_allocator().allocate(),
            )?,
        )));
        let handle = schema
            .collection(CatalogType::Sequence)
            .expect("sequence collection")
            .stage_create(&txn, &info.name, entry)?;
        if let Some(handle) = handle {
            let object_id = Self::staged_entry_object_id(&handle, "CREATE SEQUENCE", &info.name)?;
            let dependencies = Some(Self::created_entry_dependency_delta(
                &handle,
                Some(schema.as_ref()),
                "CREATE SEQUENCE",
                &info.name,
            )?);
            self.record_change(PreparedCatalogOp {
                record: DdlChangeRecord {
                    key: DdlObjectKey::new(
                        self.db.name(),
                        Some(info.schema.clone()),
                        info.name.clone(),
                        DdlObjectKind::Sequence,
                    ),
                    change: DdlChange::CreateSequence(CreateSequencePayload {
                        object_id,
                        if_not_exists: info.on_conflict == OnCreateConflict::IgnoreOnConflict,
                        increment: info.increment,
                        min_value: info.min_value,
                        max_value: info.max_value,
                        start_value: info.start_value,
                        cycle: info.cycle,
                    }),
                },
                profile: DdlExecutionProfile::metadata_only(),
                catalog: Some(handle),
                dependencies,
                dml_targets: Vec::new(),
                staged_artifacts: Vec::new(),
                storage_ops: Vec::new(),
                runtime_transitions: Vec::new(),
                cleanups: Vec::new(),
                post_commit_hooks: Vec::new(),
                transient_runtime: None,
            })?;
        }
        Ok(())
    }

    fn apply_create_view(&self, mut info: CreateViewInfo) -> Result<()> {
        self.begin_object_ddl()?;
        info.catalog = self.db.catalog().name().to_string();
        let txn = CatalogSnapshot::writer(self.txn_id, self.start_time);
        let schema = self.db.catalog().get_schema(&txn, &info.schema)?;
        let entry = Arc::new(CatalogEntryEnum::View(Arc::new(ViewCatalogEntry::new(
            info.clone(),
            0,
            self.db.catalog().name().to_string(),
            self.db.catalog().object_id_allocator().allocate(),
        ))));
        let handle = schema
            .collection(CatalogType::View)
            .expect("view collection")
            .stage_create(&txn, &info.name, entry)?;
        if let Some(handle) = handle {
            let object_id = Self::staged_entry_object_id(&handle, "CREATE VIEW", &info.name)?;
            let dependencies = Some(Self::created_entry_dependency_delta(
                &handle,
                Some(schema.as_ref()),
                "CREATE VIEW",
                &info.name,
            )?);
            self.record_change(PreparedCatalogOp {
                record: DdlChangeRecord {
                    key: DdlObjectKey::new(
                        self.db.name(),
                        Some(info.schema.clone()),
                        info.name.clone(),
                        DdlObjectKind::View,
                    ),
                    change: DdlChange::CreateView(CreateViewPayload {
                        object_id,
                        sql: info.sql.clone().unwrap_or_default(),
                        column_aliases: info.aliases.clone(),
                        dependencies: info
                            .dependencies
                            .iter()
                            .map(Self::ddl_dependency_ref)
                            .collect(),
                        if_not_exists: info.on_conflict == OnCreateConflict::IgnoreOnConflict,
                    }),
                },
                profile: DdlExecutionProfile::metadata_only(),
                catalog: Some(handle),
                dependencies,
                dml_targets: Vec::new(),
                staged_artifacts: Vec::new(),
                storage_ops: Vec::new(),
                runtime_transitions: Vec::new(),
                cleanups: Vec::new(),
                post_commit_hooks: Vec::new(),
                transient_runtime: None,
            })?;
        }
        Ok(())
    }

    fn apply_create_routine(&self, mut info: CreateRoutineInfo) -> Result<()> {
        self.begin_object_ddl()?;
        info.catalog = self.db.catalog().name().to_string();
        let signature = info.signature();
        let txn = CatalogSnapshot::writer(self.txn_id, self.start_time);
        let schema = self.db.catalog().get_schema(&txn, &info.schema)?;
        info.schema = schema.base.name.clone();
        let handle = self.stage_create_routine_handle(schema.as_ref(), &txn, &info)?;

        if let Some(handle) = handle {
            let object_id = Self::staged_entry_object_id(&handle, "CREATE FUNCTION", &info.name)?;
            let Some(entry) = handle.entry() else {
                return Err(paro_error::internal(format!(
                    "staged CREATE FUNCTION entry for \"{}\" is missing routine payload",
                    info.name
                )));
            };
            let Some(routine) = entry.as_routine() else {
                return Err(paro_error::internal(format!(
                    "staged CREATE FUNCTION entry for \"{}\" is not a routine",
                    info.name
                )));
            };
            let overload = routine
                .find_exact(&signature.argument_types)
                .ok_or_else(|| {
                    paro_error::internal(format!(
                        "staged CREATE FUNCTION entry for \"{}\" is missing overload {:?}",
                        info.name, signature.argument_types
                    ))
                })?;
            let spec_json = serde_json::to_string(&overload.spec).map_err(|error| {
                paro_error::serialization_error(format!(
                    "failed to encode routine spec for \"{}\": {}",
                    info.name, error
                ))
            })?;
            let dependencies = Some(Self::created_entry_dependency_delta(
                &handle,
                Some(schema.as_ref()),
                "CREATE FUNCTION",
                &info.name,
            )?);
            self.record_change(PreparedCatalogOp {
                record: DdlChangeRecord {
                    key: DdlObjectKey::new(
                        self.db.name(),
                        Some(info.schema.clone()),
                        info.name.clone(),
                        DdlObjectKind::Routine,
                    ),
                    change: DdlChange::CreateRoutine(CreateRoutinePayload {
                        object_id,
                        routine_id: overload.spec.identity.id.raw(),
                        spec_json,
                        sql: overload.sql.clone(),
                    }),
                },
                profile: DdlExecutionProfile::metadata_only(),
                catalog: Some(handle),
                dependencies,
                dml_targets: Vec::new(),
                staged_artifacts: Vec::new(),
                storage_ops: Vec::new(),
                runtime_transitions: Vec::new(),
                cleanups: Vec::new(),
                post_commit_hooks: Vec::new(),
                transient_runtime: None,
            })?;
        }

        Ok(())
    }

    fn apply_alter_entry(
        &self,
        schema_name: String,
        info: AlterEntryInfo,
        sql: String,
    ) -> Result<()> {
        self.begin_object_ddl()?;
        let txn = CatalogSnapshot::writer(self.txn_id, self.start_time);
        let schema = self.db.catalog().get_schema(&txn, &schema_name)?;
        let key = DdlObjectKey::new(
            self.db.name(),
            Some(schema_name.clone()),
            info.name.clone(),
            match info.entry_type {
                CatalogType::Table => DdlObjectKind::Table,
                CatalogType::View => DdlObjectKind::View,
                CatalogType::Index => DdlObjectKind::Index,
                CatalogType::Sequence => DdlObjectKind::Sequence,
                CatalogType::Schema => DdlObjectKind::Schema,
                other => {
                    return Err(paro_error::not_supported(format!(
                        "ALTER ENTRY does not support {}",
                        other.as_str()
                    )))
                }
            },
        );

        let (handle, dml_targets, dependencies) = match (info.entry_type, &info.action) {
            (
                CatalogType::Table,
                AlterEntryAction::RenameColumn {
                    old_column_name,
                    new_column_name,
                },
            ) => {
                let existing_table = schema.get_table(self.txn_id, self.start_time, &info.name);
                let Some(CatalogEntryEnum::Table(table)) = existing_table.as_deref() else {
                    return Err(paro_error::object_not_found("table", &info.name));
                };
                self.reject_if_table_touched(table, "ALTER TABLE")?;
                let new_entry = Arc::new(CatalogEntryEnum::Table(Arc::new(
                    table.clone_with_renamed_column(old_column_name, new_column_name.clone(), 0)?,
                )));
                let handle = schema
                    .collection(CatalogType::Table)
                    .expect("table collection")
                    .stage_replace(&txn, &info.name, new_entry)?;
                (handle, vec![self.entry_table_key(table)], None)
            }
            (
                CatalogType::Table,
                AlterEntryAction::Move {
                    new_name,
                    new_schema,
                },
            ) => {
                let existing_table = schema.get_table(self.txn_id, self.start_time, &info.name);
                let Some(CatalogEntryEnum::Table(table)) = existing_table.as_deref() else {
                    return Err(paro_error::object_not_found("table", &info.name));
                };
                self.reject_if_table_touched(table, "RENAME TABLE")?;
                let target_schema_name = new_schema.as_deref().unwrap_or(&schema_name).to_string();
                let handle = if target_schema_name.eq_ignore_ascii_case(&schema_name) {
                    let new_entry = Arc::new(CatalogEntryEnum::Table(Arc::new(
                        table.clone_with_new_schema_and_name(
                            table.base.schema_name.clone(),
                            new_name.clone(),
                            0,
                        ),
                    )));
                    schema
                        .collection(CatalogType::Table)
                        .expect("table collection")
                        .stage_rename(&txn, &info.name, new_name, new_entry)?
                } else {
                    let target_schema = self.db.catalog().get_schema(&txn, &target_schema_name)?;
                    let new_entry = Arc::new(CatalogEntryEnum::Table(Arc::new(
                        table.clone_with_new_schema_and_name(
                            target_schema_name.clone(),
                            new_name.clone(),
                            0,
                        ),
                    )));
                    schema
                        .collection(CatalogType::Table)
                        .expect("table collection")
                        .stage_move(
                            &txn,
                            &info.name,
                            target_schema
                                .collection(CatalogType::Table)
                                .expect("table collection"),
                            new_name,
                            new_entry,
                        )?
                };
                let dependencies = handle
                    .as_ref()
                    .map(|handle| {
                        let target_schema = if target_schema_name.eq_ignore_ascii_case(&schema_name)
                        {
                            Arc::clone(&schema)
                        } else {
                            self.db.catalog().get_schema(&txn, &target_schema_name)?
                        };
                        Self::rename_dependency_delta(
                            handle,
                            schema.as_ref(),
                            target_schema.as_ref(),
                            "RENAME TABLE",
                            &info.name,
                        )
                    })
                    .transpose()?;
                (
                    handle,
                    vec![
                        self.entry_table_key(table),
                        self.table_key(target_schema_name, new_name.clone()),
                    ],
                    dependencies,
                )
            }
            (CatalogType::Table, AlterEntryAction::SetTableComment { new_comment }) => {
                let existing_table = schema.get_table(self.txn_id, self.start_time, &info.name);
                let Some(CatalogEntryEnum::Table(table)) = existing_table.as_deref() else {
                    return Err(paro_error::object_not_found("table", &info.name));
                };
                self.reject_if_table_touched(table, "ALTER TABLE")?;
                let new_entry = Arc::new(CatalogEntryEnum::Table(Arc::new(
                    table.clone_with_comment(Some(new_comment.clone()), 0),
                )));
                let handle = schema
                    .collection(CatalogType::Table)
                    .expect("table collection")
                    .stage_replace(&txn, &info.name, new_entry)?;
                (handle, vec![self.entry_table_key(table)], None)
            }
            (CatalogType::Table, AlterEntryAction::SetColumnComments { comments }) => {
                let existing_table = schema.get_table(self.txn_id, self.start_time, &info.name);
                let Some(CatalogEntryEnum::Table(table)) = existing_table.as_deref() else {
                    return Err(paro_error::object_not_found("table", &info.name));
                };
                self.reject_if_table_touched(table, "ALTER TABLE")?;
                let updates = comments
                    .iter()
                    .map(|comment| (comment.column_name.clone(), comment.comment.clone()))
                    .collect::<Vec<_>>();
                let new_entry = Arc::new(CatalogEntryEnum::Table(Arc::new(
                    table.clone_with_column_comments(&updates, 0)?,
                )));
                let handle = schema
                    .collection(CatalogType::Table)
                    .expect("table collection")
                    .stage_replace(&txn, &info.name, new_entry)?;
                (handle, vec![self.entry_table_key(table)], None)
            }
            (other, _) => {
                return Err(paro_error::not_supported(format!(
                    "ALTER ENTRY does not support {} yet",
                    other.as_str()
                )))
            }
        };

        if let Some(handle) = handle {
            self.record_change(PreparedCatalogOp {
                record: DdlChangeRecord {
                    key,
                    change: DdlChange::AlterEntry(AlterEntryPayload { sql }),
                },
                profile: DdlExecutionProfile::alter_existing_object(),
                catalog: Some(handle),
                dependencies,
                dml_targets,
                staged_artifacts: Vec::new(),
                storage_ops: Vec::new(),
                runtime_transitions: Vec::new(),
                cleanups: Vec::new(),
                post_commit_hooks: Vec::new(),
                transient_runtime: None,
            })?;
        }
        Ok(())
    }

    fn apply_create_property_graph(
        &self,
        mut info: CreatePropertyGraphInfo,
        staging: StagingArtifactId,
        schema_fingerprint: String,
    ) -> Result<()> {
        self.reject_if_any_table_touched(
            info.vertex_tables
                .iter()
                .map(|vertex| vertex.table_oid)
                .chain(info.edge_tables.iter().map(|edge| edge.table_oid)),
            "CREATE PROPERTY GRAPH",
        )?;
        self.begin_object_ddl()?;
        info.catalog = self.db.catalog().name().to_string();
        let txn = CatalogSnapshot::writer(self.txn_id, self.start_time);
        let schema = self.db.catalog().get_schema(&txn, &info.schema)?;
        let entry = Arc::new(CatalogEntryEnum::PropertyGraph(Arc::new(
            PropertyGraphCatalogEntry::new(
                info.clone(),
                0,
                self.db.catalog().name().to_string(),
                self.db.catalog().object_id_allocator().allocate(),
            ),
        )));
        let handle = schema
            .collection(CatalogType::PropertyGraph)
            .expect("property graph collection")
            .stage_create(&txn, &info.graph_name, entry)?;
        if let Some(handle) = handle {
            let object_id =
                Self::staged_entry_object_id(&handle, "CREATE PROPERTY GRAPH", &info.graph_name)?;
            let key = DdlObjectKey::new(
                self.db.name(),
                Some(info.schema.clone()),
                info.graph_name.clone(),
                DdlObjectKind::PropertyGraph,
            );
            let dependencies = Some(Self::created_entry_dependency_delta(
                &handle,
                Some(schema.as_ref()),
                "CREATE PROPERTY GRAPH",
                &info.graph_name,
            )?);
            self.record_change(PreparedCatalogOp {
                record: DdlChangeRecord {
                    key: key.clone(),
                    change: DdlChange::CreatePropertyGraph(CreatePropertyGraphPayload {
                        object_id,
                        schema: info.schema.clone(),
                        graph_name: info.graph_name.clone(),
                        if_not_exists: info.if_not_exists,
                        vertex_tables: info
                            .vertex_tables
                            .iter()
                            .map(|vertex| PropertyGraphVertexPayload {
                                table_name: vertex.table_name.clone(),
                                table_oid: vertex.table_oid,
                                key_column_ids: vertex.key_column_ids.clone(),
                                label: vertex.label.clone(),
                                property_column_ids: vertex.property_column_ids.clone(),
                            })
                            .collect(),
                        edge_tables: info
                            .edge_tables
                            .iter()
                            .map(|edge| PropertyGraphEdgePayload {
                                table_name: edge.table_name.clone(),
                                table_oid: edge.table_oid,
                                key_column_ids: edge.key_column_ids.clone(),
                                source_key_column_ids: edge.source_key_column_ids.clone(),
                                source_vertex_table: edge.source_vertex_table.clone(),
                                source_ref_column_ids: edge.source_ref_column_ids.clone(),
                                destination_key_column_ids: edge.destination_key_column_ids.clone(),
                                destination_vertex_table: edge.destination_vertex_table.clone(),
                                destination_ref_column_ids: edge.destination_ref_column_ids.clone(),
                                label: edge.label.clone(),
                                property_column_ids: edge.property_column_ids.clone(),
                            })
                            .collect(),
                    }),
                },
                profile: DdlExecutionProfile::register_graph_runtime(),
                catalog: Some(handle),
                dependencies,
                dml_targets: info
                    .vertex_tables
                    .iter()
                    .map(|vertex| self.table_key(info.schema.clone(), vertex.table_name.clone()))
                    .chain(
                        info.edge_tables.iter().map(|edge| {
                            self.table_key(info.schema.clone(), edge.table_name.clone())
                        }),
                    )
                    .collect(),
                staged_artifacts: vec![StagedArtifactDescriptor::PropertyGraphBuild {
                    object: key.clone(),
                    staging,
                    schema_fingerprint,
                }],
                storage_ops: Vec::new(),
                runtime_transitions: vec![RuntimeTransitionDescriptor::RegisterGraphRuntime {
                    graph: key,
                }],
                cleanups: Vec::new(),
                post_commit_hooks: Vec::new(),
                transient_runtime: None,
            })?;
        }
        Ok(())
    }

    fn apply_drop_property_graph(
        &self,
        _catalog_name: String,
        schema_name: String,
        graph_name: String,
        if_exists: bool,
    ) -> Result<()> {
        let txn = CatalogSnapshot::writer(self.txn_id, self.start_time);
        let schema = self.db.catalog().get_schema(&txn, &schema_name)?;
        let existing_graph = schema.get_property_graph(&txn, &graph_name).ok();
        let graph_dml_targets = schema
            .get_property_graph(&txn, &graph_name)
            .ok()
            .map(|graph| {
                graph
                    .info
                    .vertex_tables
                    .iter()
                    .map(|vertex| self.table_key(schema_name.clone(), vertex.table_name.clone()))
                    .chain(
                        graph.info.edge_tables.iter().map(|edge| {
                            self.table_key(schema_name.clone(), edge.table_name.clone())
                        }),
                    )
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        if let Some(graph) = existing_graph.as_ref() {
            self.reject_if_any_table_touched(
                graph
                    .info
                    .vertex_tables
                    .iter()
                    .map(|vertex| vertex.table_oid)
                    .chain(graph.info.edge_tables.iter().map(|edge| edge.table_oid)),
                "DROP PROPERTY GRAPH",
            )?;
        }
        self.begin_object_ddl()?;
        let handle = schema
            .collection(CatalogType::PropertyGraph)
            .expect("property graph collection")
            .stage_drop(&txn, &graph_name)?;
        if let Some(handle) = handle {
            let dependencies = existing_graph
                .as_ref()
                .map(|graph| self.planned_drop_delta(&txn, graph.base.base.object_id))
                .transpose()?;
            let key = DdlObjectKey::new(
                self.db.name(),
                Some(schema_name.clone()),
                graph_name.clone(),
                DdlObjectKind::PropertyGraph,
            );
            self.record_change(PreparedCatalogOp {
                record: DdlChangeRecord {
                    key: key.clone(),
                    change: DdlChange::DropPropertyGraph(DropPropertyGraphPayload { if_exists }),
                },
                profile: DdlExecutionProfile::unregister_graph_runtime(),
                catalog: Some(handle),
                dependencies,
                dml_targets: graph_dml_targets,
                staged_artifacts: Vec::new(),
                storage_ops: Vec::new(),
                runtime_transitions: vec![RuntimeTransitionDescriptor::UnregisterGraphRuntime {
                    graph: key,
                }],
                cleanups: vec![CleanupDescriptor::RemoveDirectory {
                    path_components: vec![
                        self.db.path().to_string(),
                        "graph".to_string(),
                        graph_name,
                    ],
                    recursive: true,
                }],
                post_commit_hooks: Vec::new(),
                transient_runtime: None,
            })?;
        }
        Ok(())
    }

    fn apply_drop_routine(
        &self,
        schema_name: String,
        name: String,
        info: DropRoutineInfo,
    ) -> Result<()> {
        self.begin_object_ddl()?;
        let txn = CatalogSnapshot::writer(self.txn_id, self.start_time);
        let schema = self.db.catalog().get_schema(&txn, &schema_name)?;
        let existing_entry = schema.get_routine(self.txn_id, self.start_time, &name);

        let Some(existing_entry) = existing_entry else {
            if info.if_exists {
                return Ok(());
            }
            return Err(paro_error::function_not_found(format!(
                "{}({})",
                name,
                info.arg_types
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            )));
        };

        let existing_routine = existing_entry
            .as_routine()
            .ok_or_else(|| paro_error::wrong_object_type("routine", &name))?;
        if existing_routine.find_exact(&info.arg_types).is_none() {
            if info.if_exists {
                return Ok(());
            }
            return Err(paro_error::function_not_found(format!(
                "{}({})",
                name,
                info.arg_types
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            )));
        }

        let handle = if existing_routine.overloads().len() == 1 {
            schema
                .collection(CatalogType::Routine)
                .expect("routine collection")
                .stage_drop(&txn, &name)?
        } else {
            let mut overloads = existing_routine.overloads().to_vec();
            overloads.retain(|overload| !overload.spec.signature().exact_match(&info.arg_types));
            let replacement = Arc::new(CatalogEntryEnum::Routine(Arc::new(
                RoutineCatalogEntry::with_overloads(
                    self.db.catalog().name().to_string(),
                    schema.base.name.clone(),
                    name.clone(),
                    existing_routine.base.base.object_id,
                    txn.write_timestamp()?,
                    overloads,
                ),
            )));
            schema
                .collection(CatalogType::Routine)
                .expect("routine collection")
                .stage_replace(&txn, &name, replacement)?
        };

        if let Some(handle) = handle {
            let dependencies = if existing_routine.overloads().len() == 1 {
                Some(self.planned_drop_delta(&txn, existing_routine.base.base.object_id)?)
            } else {
                None
            };
            self.record_change(PreparedCatalogOp {
                record: DdlChangeRecord {
                    key: DdlObjectKey::new(
                        self.db.name(),
                        Some(schema_name),
                        name,
                        DdlObjectKind::Routine,
                    ),
                    change: DdlChange::DropRoutine(DropRoutinePayload {
                        if_exists: info.if_exists,
                        arg_types: info.arg_types,
                    }),
                },
                profile: DdlExecutionProfile::metadata_only(),
                catalog: Some(handle),
                dependencies,
                dml_targets: Vec::new(),
                staged_artifacts: Vec::new(),
                storage_ops: Vec::new(),
                runtime_transitions: Vec::new(),
                cleanups: Vec::new(),
                post_commit_hooks: Vec::new(),
                transient_runtime: None,
            })?;
        }

        Ok(())
    }

    fn apply_drop(&self, schema_name: String, info: DropEntryInfo) -> Result<()> {
        self.begin_object_ddl()?;
        let txn = CatalogSnapshot::writer(self.txn_id, self.start_time);
        let Some(root_ref) = self.lookup_drop_object_ref(&txn, &schema_name, &info)? else {
            return Ok(());
        };
        let plan = self
            .planned_dependency_graph(&txn)?
            .plan_drop(root_ref.id, info.cascade)?;
        for object_ref in plan {
            if let Some(change) = self.stage_drop_planned_object(&txn, &object_ref, info.cascade)? {
                self.record_change(change)?;
            }
        }
        Ok(())
    }

    fn prepare_index_build(
        &self,
        mut info: CreateIndexInfo,
        table: Arc<TableCatalogEntry>,
        cancellation: StatementCancellation,
    ) -> Result<Box<dyn IndexBuildHandle>> {
        self.reject_if_table_touched(table.as_ref(), "CREATE INDEX")?;
        self.begin_object_ddl()?;
        info.catalog = self.db.catalog().name().to_string();
        let txn = CatalogSnapshot::writer(self.txn_id, self.start_time);
        let schema = self.db.catalog().get_schema(&txn, &info.schema)?;
        if let Some(existing) = schema
            .collection(CatalogType::Index)
            .expect("index collection")
            .get_entry(self.txn_id, self.start_time, &info.name)
        {
            if info.if_not_exists || info.on_conflict == OnCreateConflict::IgnoreOnConflict {
                return Ok(Box::new(SessionCreateIndexHandle {
                    info,
                    table,
                    entry: None,
                    catalog: None,
                    dependencies: None,
                    backfill: None,
                    staged_search_generation: None,
                    skip_build: true,
                }));
            }

            return Err(paro_error::object_exists("index", existing.name()));
        }

        let prepared_info = info
            .clone()
            .with_build_state(IndexBuildState::Building)
            .clear_failure_reason();
        let index_entry = Arc::new(IndexCatalogEntry::new(
            prepared_info,
            table.base.base.object_id.raw(),
            0,
            self.db.catalog().name().to_string(),
            self.db.catalog().object_id_allocator().allocate(),
        ));
        index_entry.mark_building();
        let mut handle = schema
            .collection(CatalogType::Index)
            .expect("index collection")
            .stage_create(
                &txn,
                &info.name,
                Arc::new(CatalogEntryEnum::Index(Arc::clone(&index_entry))),
            )?;
        let mut dependencies = handle
            .as_ref()
            .map(|handle| {
                Self::created_entry_dependency_delta(
                    handle,
                    Some(schema.as_ref()),
                    "CREATE INDEX",
                    &info.name,
                )
            })
            .transpose()?;
        let prepared_runtime = (|| {
            let search_kind = search_index_kind(info.index_type);
            let backfill = if search_kind.is_none() {
                let current_published_ts = self.db.transaction_manager().published_commit_id();
                let backfill_read_ts = current_published_ts;
                let backfill_lease = lease_index_backfill(
                    self.db.transaction_manager().retention_registry(),
                    backfill_read_ts,
                    current_published_ts,
                )?;
                Some(IndexBackfillPlan::new(
                    table.base.base.object_id.raw(),
                    index_entry.base.base.object_id.raw(),
                    backfill_read_ts,
                    current_published_ts,
                    backfill_lease,
                ))
            } else {
                None
            };

            let index_key = DdlObjectKey::new(
                self.db.name(),
                Some(info.schema.clone()),
                info.name.clone(),
                DdlObjectKind::Index,
            );
            let table_key = self.entry_table_key(table.as_ref());
            self.active_txn.acquire_lock_requests(
                DdlExecutionProfile::attach_index_state().lock_requests(
                    self.active_txn.lock_namespace(),
                    &index_key,
                    std::slice::from_ref(&table_key),
                ),
            )?;

            let staged_search_generation = if let Some(kind) = search_kind {
                let storage = table.get_storage().ok_or_else(|| {
                    paro_error::internal(format!(
                        "table '{}' has no storage for CREATE INDEX staging",
                        table.base.base.name
                    ))
                })?;
                let column_ids = info
                    .column_ids
                    .iter()
                    .map(|column| column.index)
                    .collect::<Vec<_>>();
                let expression = search_index_expression(&info);
                let config_fingerprint = SearchIndexDefinition::try_compute_config_fingerprint(
                    kind,
                    &column_ids,
                    expression.as_deref(),
                    &info.provider_config,
                )?;
                let definition = SearchIndexDefinition {
                    definition_id: index_entry.base.base.object_id.raw(),
                    table_id: storage.tablet().table_id(),
                    name: info.name.clone(),
                    kind,
                    column_ids,
                    expression,
                    freshness_policy: SearchFreshnessPolicy::default_for_kind(kind),
                    provider_config: info.provider_config.clone(),
                    config_fingerprint,
                };
                let cancellation_for_build = cancellation.clone();
                let stop_check = SearchBuildStopCheck::new(move || {
                    cancellation_for_build.is_cancelled()
                        || cancellation_for_build.connection_cancelled()
                });
                Some(Arc::new(storage.stage_search_definition_generation(
                    definition,
                    self.txn_id,
                    stop_check,
                )?))
            } else {
                None
            };
            Ok::<_, paro_common::error::ParoError>((backfill, staged_search_generation))
        })();
        let (backfill, staged_search_generation) = match prepared_runtime {
            Ok(prepared) => prepared,
            Err(error) => {
                if let Some(delta) = dependencies.take() {
                    delta.discard();
                }
                if let Some(catalog) = handle.take() {
                    if let Err(cleanup_error) = catalog.discard() {
                        tracing::warn!(
                            target: paro_common::logging::targets::TRANSACTION,
                            error = %cleanup_error,
                            "failed to discard CREATE INDEX catalog staging after build error"
                        );
                    }
                }
                return Err(error);
            }
        };

        Ok(Box::new(SessionCreateIndexHandle {
            info,
            table,
            entry: Some(index_entry),
            catalog: handle,
            dependencies,
            backfill,
            staged_search_generation,
            skip_build: false,
        }))
    }

    fn commit_index_build(
        &self,
        handle: Box<dyn IndexBuildHandle>,
        artifact: PreparedIndexArtifact,
    ) -> Result<()> {
        let handle = handle
            .into_any()
            .downcast::<SessionCreateIndexHandle>()
            .map_err(|_| paro_error::internal("invalid CREATE INDEX build handle"))?;
        let mut handle = *handle;

        if handle.skip_build {
            return Ok(());
        }

        let key = DdlObjectKey::new(
            self.db.name(),
            Some(handle.info.schema.clone()),
            handle.info.name.clone(),
            DdlObjectKind::Index,
        );
        let (built_index, mut coverage) = match artifact {
            PreparedIndexArtifact::RuntimeIndex { index, coverage } => (Some(index), coverage),
            PreparedIndexArtifact::MetadataOnly { coverage } => (None, coverage),
        };
        if let Some(staged) = &handle.staged_search_generation {
            let staged_coverage = staged.coverage();
            coverage = Some(paro_catalog::entry::IndexCoverage::from_counts(
                staged_coverage.visible_version,
                staged_coverage.visible_segment_count,
                staged_coverage.indexed_segment_count,
            ));
        }
        let Some(object_id) = handle
            .entry
            .as_ref()
            .map(|entry| entry.base.base.object_id.raw())
        else {
            Self::discard_index_build_staging(&mut handle);
            return Err(paro_error::internal("staged CREATE INDEX entry is missing"));
        };
        if handle.staged_search_generation.is_none() {
            if let Some(backfill) = &handle.backfill {
                let current_published_ts = self.db.transaction_manager().published_commit_id();
                let report = match backfill.tail_committed_records_to(current_published_ts) {
                    Ok(report) => report,
                    Err(error) => {
                        Self::discard_index_build_staging(&mut handle);
                        return Err(error);
                    }
                };
                tracing::debug!(
                    target: paro_common::logging::targets::TRANSACTION,
                    index = %handle.info.name,
                    table = %handle.info.table_name,
                    from_ts = report.from_ts,
                    to_ts = report.to_ts,
                    consumed_commits = report.consumed_commits,
                    "CREATE INDEX backfill journal tail advanced before commit publish"
                );
            }
        }

        let table_object = self.entry_table_key(handle.table.as_ref());
        let (staged_artifacts, storage_ops) = if let Some(staged) = &handle.staged_search_generation
        {
            let Some(storage) = handle.table.get_storage() else {
                Self::discard_index_build_staging(&mut handle);
                return Err(paro_error::internal(
                    "CREATE INDEX staged generation lost table storage",
                ));
            };
            (
                vec![staged.durable_descriptor(
                    table_object.clone(),
                    handle.table.base.base.object_id.raw(),
                    storage.tablet_id(),
                )],
                vec![staged.storage_op(storage.tablet_id())],
            )
        } else {
            (Vec::new(), Vec::new())
        };

        self.record_change_with_locks_held(PreparedCatalogOp {
            record: DdlChangeRecord {
                key: key.clone(),
                change: DdlChange::CreateIndex(CreateIndexPayload {
                    object_id,
                    table_name: handle.info.table_name.clone(),
                    column_ids: handle
                        .info
                        .column_ids
                        .iter()
                        .map(|column| column.index)
                        .collect(),
                    column_types: handle.info.column_types.clone(),
                    index_type: handle.info.index_type.as_str().to_string(),
                    is_unique: handle.info.is_unique(),
                    if_not_exists: handle.info.if_not_exists,
                    fulltext_config: handle
                        .info
                        .fulltext
                        .as_ref()
                        .map(|binding| binding.config.clone()),
                    provider_config_json: handle.info.provider_config.to_string(),
                }),
            },
            profile: DdlExecutionProfile::attach_index_state(),
            catalog: handle.catalog.take(),
            dependencies: handle.dependencies.take(),
            dml_targets: vec![table_object],
            staged_artifacts,
            storage_ops,
            runtime_transitions: vec![RuntimeTransitionDescriptor::AttachIndexState {
                index: key,
                table_name: handle.info.table_name.clone(),
                index_type: handle.info.index_type.as_str().to_string(),
                column_ids: handle
                    .info
                    .column_ids
                    .iter()
                    .map(|column| column.index)
                    .collect(),
                fulltext_config: handle
                    .info
                    .fulltext
                    .as_ref()
                    .map(|binding| binding.config.clone()),
            }],
            cleanups: Vec::new(),
            post_commit_hooks: Vec::new(),
            transient_runtime: handle.entry.take().map(|entry| {
                TransientCatalogRuntime::CreateIndex(IndexPostCommitAction {
                    entry,
                    table: handle.table,
                    info: handle.info,
                    built_index,
                    coverage,
                    backfill: handle.backfill,
                    staged_search_generation: handle.staged_search_generation,
                })
            }),
        })
    }

    fn abort_index_build(&self, handle: Box<dyn IndexBuildHandle>, _reason: String) {
        if let Ok(handle) = handle.into_any().downcast::<SessionCreateIndexHandle>() {
            let mut handle = *handle;
            Self::discard_index_build_staging(&mut handle);
        }
    }
}
