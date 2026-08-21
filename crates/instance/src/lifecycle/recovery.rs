// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Instance-level startup recovery orchestration.
//!
//! This module decides which durable database records should be reconciled,
//! opened, skipped, or marked broken. Single-database recovery mechanics stay
//! in `database::DatabaseOpener`.

use crate::{
    DatabaseOpenIntent, DatabaseRecord, DatabaseRecordState, DatabaseStorageIdentity, Instance,
    InstanceLifecycleState, InstanceRunState, InstanceStartupDisposition, StartupReport,
};
use paro_common::logging::targets;
use paro_storage::meta::{FileMetadataStore, MetadataStore};
use std::collections::HashSet;
use std::fs;
use std::path::Path;

pub(crate) struct InstanceRecovery;

#[derive(Debug, Clone)]
struct InstanceStartupPlan {
    disposition: InstanceStartupDisposition,
    skip_transient_reconcile: bool,
    skip_orphan_scan: bool,
    clean_state_invariant_violation: Option<String>,
}

impl InstanceRecovery {
    pub(crate) fn run(
        instance: &Instance,
        previous_run_state: Option<InstanceRunState>,
    ) -> paro_common::error::Result<StartupReport> {
        let mut catalog = instance.metadata.load_catalog()?;
        let startup_plan = Self::plan_startup(instance, &catalog, previous_run_state);
        let mut startup_report =
            StartupReport::new(instance.lifecycle.startup_policy, startup_plan.disposition);
        if let Some(detail) = &startup_plan.clean_state_invariant_violation {
            startup_report.record_clean_state_invariant_violation(detail.clone());
        }
        Self::log_startup_plan(instance, &startup_plan);

        if !startup_plan.skip_transient_reconcile {
            Self::reconcile_transient_records(instance, &mut catalog, &mut startup_report)?;
        }
        if !startup_plan.skip_orphan_scan {
            Self::scan_orphan_managed_directories(instance, &catalog, &mut startup_report)?;
        }

        let ready_records: Vec<DatabaseRecord> = catalog
            .databases
            .iter()
            .filter(|record| record.state.allows_runtime_open())
            .cloned()
            .collect();

        for record in ready_records {
            tracing::info!(
                target: targets::INSTANCE,
                db = %record.name,
                database_id = record.database_id,
                path = %record.storage_dir,
                "Recovering ready database from instance catalog"
            );

            let open_result = match instance.database_service.open_managed_database(
                &record,
                DatabaseOpenIntent::OpenExisting,
                &instance.runtime.database_open_context(
                    instance.boot_config.checkpoint,
                    instance.boot_config.compaction,
                ),
                instance.lifecycle.startup_policy,
                true,
            ) {
                Ok(result) => result,
                Err(err) => {
                    let error_detail = err.error.to_string();
                    let recovery_report = err.recovery_report.clone();
                    if let Some(record_state) = catalog.find_database_mut_by_id(record.database_id)
                    {
                        record_state.state = DatabaseRecordState::Broken;
                        record_state.last_error = Some(error_detail.clone());
                    }
                    instance.metadata.persist_catalog(&mut catalog)?;
                    if Self::is_storage_identity_error(&error_detail) {
                        startup_report.record_identity_mismatch(
                            record.database_id,
                            record.name.clone(),
                            record.storage_dir.clone(),
                            error_detail.clone(),
                        );
                    }
                    startup_report.record_failed_with_report(
                        record.database_id,
                        record.name.clone(),
                        DatabaseRecordState::Broken,
                        Some(record.storage_dir.clone()),
                        error_detail,
                        recovery_report,
                    );
                    if instance.lifecycle.startup_policy.allows_degraded_startup() {
                        continue;
                    }
                    return Err(err.error);
                }
            };

            instance
                .database_service
                .publish(&record, &open_result.handle)?;
            startup_report.record_recovered(
                record.database_id,
                record.name.clone(),
                DatabaseRecordState::Ready,
                record.storage_dir.clone(),
                open_result.recovery_report,
            );
        }

        for record in &catalog.databases {
            if startup_report.has_database(record.database_id) {
                continue;
            }

            match record.state {
                DatabaseRecordState::Offline => startup_report.record_skipped(
                    record.database_id,
                    record.name.clone(),
                    record.state,
                    "database is marked offline",
                ),
                DatabaseRecordState::Broken => startup_report.record_skipped(
                    record.database_id,
                    record.name.clone(),
                    record.state,
                    record
                        .last_error
                        .clone()
                        .unwrap_or_else(|| "database remains broken".to_string()),
                ),
                DatabaseRecordState::Provisioning | DatabaseRecordState::Dropping => {}
                DatabaseRecordState::Ready => {}
            }
        }

        if let Some(default_database_id) = catalog.default_database_id {
            instance
                .database_service
                .registry()
                .set_default_database(default_database_id)
                .map_err(|e| paro_common::error::internal(e.to_string()))?;
        }

        Ok(startup_report)
    }

    fn plan_startup(
        instance: &Instance,
        catalog: &crate::InstanceCatalog,
        previous_run_state: Option<InstanceRunState>,
    ) -> InstanceStartupPlan {
        if instance.boot_config.is_in_memory() {
            return InstanceStartupPlan {
                disposition: InstanceStartupDisposition::FullRecovery,
                skip_transient_reconcile: false,
                skip_orphan_scan: false,
                clean_state_invariant_violation: None,
            };
        }

        let Some(run_state) = previous_run_state else {
            return InstanceStartupPlan {
                disposition: InstanceStartupDisposition::FullRecovery,
                skip_transient_reconcile: false,
                skip_orphan_scan: false,
                clean_state_invariant_violation: None,
            };
        };

        if run_state.state != InstanceLifecycleState::Clean {
            return InstanceStartupPlan {
                disposition: InstanceStartupDisposition::FullRecovery,
                skip_transient_reconcile: false,
                skip_orphan_scan: false,
                clean_state_invariant_violation: None,
            };
        }

        if catalog.has_transient_records() {
            let detail = Self::describe_clean_state_invariant_violation(catalog);
            tracing::warn!(
                target: targets::INSTANCE,
                boot_id = run_state.boot_id,
                detail = %detail,
                "Instance run_state reported Clean but catalog still contains transient database records; falling back to full recovery"
            );
            return InstanceStartupPlan {
                disposition: InstanceStartupDisposition::FullRecovery,
                skip_transient_reconcile: false,
                skip_orphan_scan: false,
                clean_state_invariant_violation: Some(detail),
            };
        }

        InstanceStartupPlan {
            disposition: InstanceStartupDisposition::CleanFastPath,
            skip_transient_reconcile: true,
            skip_orphan_scan: !instance.lifecycle.startup_policy.enables_repair_actions(),
            clean_state_invariant_violation: None,
        }
    }

    fn describe_clean_state_invariant_violation(catalog: &crate::InstanceCatalog) -> String {
        let transient_records = catalog
            .databases
            .iter()
            .filter_map(|record| match record.state {
                DatabaseRecordState::Provisioning | DatabaseRecordState::Dropping => {
                    Some(format!("{}({:?})", record.name, record.state))
                }
                DatabaseRecordState::Ready
                | DatabaseRecordState::Offline
                | DatabaseRecordState::Broken => None,
            })
            .collect::<Vec<_>>();
        format!(
            "run_state reported Clean but catalog still contains transient records: {}",
            transient_records.join(", ")
        )
    }

    fn log_startup_plan(instance: &Instance, plan: &InstanceStartupPlan) {
        tracing::info!(
            target: targets::INSTANCE,
            disposition = ?plan.disposition,
            skip_transient_reconcile = plan.skip_transient_reconcile,
            skip_orphan_scan = plan.skip_orphan_scan,
            repair_actions = instance.lifecycle.startup_policy.enables_repair_actions(),
            per_database_open_path = "open_existing_unchanged",
            "Selected instance startup disposition"
        );

        if plan.disposition == InstanceStartupDisposition::CleanFastPath {
            tracing::info!(
                target: targets::INSTANCE,
                repair_actions = instance.lifecycle.startup_policy.enables_repair_actions(),
                "Using conservative clean fast path; only instance-level transient reconcile/orphan scan may be skipped, per-database open_existing recovery remains unchanged, and startup savings are expected to be small"
            );
        }
    }

    fn scan_orphan_managed_directories(
        instance: &Instance,
        catalog: &crate::InstanceCatalog,
        startup_report: &mut StartupReport,
    ) -> paro_common::error::Result<()> {
        if instance.boot_config.is_in_memory()
            || !instance.lifecycle.startup_policy.enables_repair_actions()
        {
            return Ok(());
        }

        let Some(layout) = instance.metadata.layout() else {
            return Ok(());
        };
        let managed_root = layout.databases_dir();
        if !managed_root.exists() {
            return Ok(());
        }

        let known_dirs: HashSet<String> = catalog
            .databases
            .iter()
            .map(|record| Self::normalize_path(Path::new(&record.storage_dir)))
            .collect();

        let entries = fs::read_dir(&managed_root).map_err(|e| {
            paro_common::error::internal(format!(
                "Failed to scan managed database root {}: {}",
                managed_root.display(),
                e
            ))
        })?;

        for entry in entries {
            let entry = entry.map_err(|e| {
                paro_common::error::internal(format!(
                    "Failed to read entry under managed database root {}: {}",
                    managed_root.display(),
                    e
                ))
            })?;
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }
            if known_dirs.contains(&Self::normalize_path(&path)) {
                continue;
            }

            let detail = Self::describe_orphan_directory(&path);
            tracing::warn!(
                target: targets::INSTANCE,
                path = %path.display(),
                detail = %detail,
                "Repair mode detected orphan managed database directory"
            );
            startup_report.record_orphan_directory(path.to_string_lossy().to_string(), detail);
        }

        Ok(())
    }

    fn reconcile_transient_records(
        instance: &Instance,
        catalog: &mut crate::InstanceCatalog,
        startup_report: &mut StartupReport,
    ) -> paro_common::error::Result<()> {
        let mut dirty = false;
        let record_ids: Vec<u64> = catalog
            .databases
            .iter()
            .map(|record| record.database_id)
            .collect();

        for database_id in record_ids {
            let Some(state) = catalog
                .find_database_by_id(database_id)
                .map(|record| record.state)
            else {
                continue;
            };

            match state {
                DatabaseRecordState::Provisioning => {
                    dirty |= Self::rollback_provisioning_record(
                        instance,
                        catalog,
                        startup_report,
                        database_id,
                    )?;
                }
                DatabaseRecordState::Dropping => {
                    dirty |= Self::finish_dropping_record(
                        instance,
                        catalog,
                        startup_report,
                        database_id,
                    )?;
                }
                DatabaseRecordState::Ready
                | DatabaseRecordState::Offline
                | DatabaseRecordState::Broken => {}
            }
        }

        if dirty {
            instance.metadata.persist_catalog(catalog)?;
        }
        Ok(())
    }

    fn rollback_provisioning_record(
        instance: &Instance,
        catalog: &mut crate::InstanceCatalog,
        startup_report: &mut StartupReport,
        database_id: u64,
    ) -> paro_common::error::Result<bool> {
        let Some(record) = catalog.find_database_by_id(database_id).cloned() else {
            return Ok(false);
        };

        match Self::cleanup_storage_dir(instance, &record) {
            Ok(()) => {
                tracing::warn!(
                    target: targets::INSTANCE,
                    db = %record.name,
                    database_id = record.database_id,
                    "Rolled back provisioning database during instance recovery"
                );
                startup_report.record_reconciled(
                    record.database_id,
                    record.name.clone(),
                    DatabaseRecordState::Provisioning,
                    "rolled back interrupted provisioning record",
                );
                catalog.remove_database_by_id(database_id);
                Ok(true)
            }
            Err(err) => {
                if let Some(record_state) = catalog.find_database_mut_by_id(database_id) {
                    record_state.last_error = Some(err.to_string());
                }
                tracing::error!(
                    target: targets::INSTANCE,
                    db = %record.name,
                    database_id = record.database_id,
                    err = %err,
                    "Failed to clean up provisioning database during instance recovery"
                );
                startup_report.record_failed(
                    record.database_id,
                    record.name.clone(),
                    DatabaseRecordState::Provisioning,
                    err.to_string(),
                );
                Ok(true)
            }
        }
    }

    fn finish_dropping_record(
        instance: &Instance,
        catalog: &mut crate::InstanceCatalog,
        startup_report: &mut StartupReport,
        database_id: u64,
    ) -> paro_common::error::Result<bool> {
        let Some(record) = catalog.find_database_by_id(database_id).cloned() else {
            return Ok(false);
        };

        match Self::cleanup_storage_dir(instance, &record) {
            Ok(()) => {
                tracing::info!(
                    target: targets::INSTANCE,
                    db = %record.name,
                    database_id = record.database_id,
                    "Finished dropping database during instance recovery"
                );
                startup_report.record_reconciled(
                    record.database_id,
                    record.name.clone(),
                    DatabaseRecordState::Dropping,
                    "finished dropping interrupted database",
                );
                catalog.remove_database_by_id(database_id);
                Ok(true)
            }
            Err(err) => {
                if let Some(record_state) = catalog.find_database_mut_by_id(database_id) {
                    record_state.last_error = Some(err.to_string());
                }
                tracing::error!(
                    target: targets::INSTANCE,
                    db = %record.name,
                    database_id = record.database_id,
                    err = %err,
                    "Failed to finish dropping database during instance recovery"
                );
                startup_report.record_failed(
                    record.database_id,
                    record.name.clone(),
                    DatabaseRecordState::Dropping,
                    err.to_string(),
                );
                Ok(true)
            }
        }
    }

    fn cleanup_storage_dir(
        instance: &Instance,
        record: &DatabaseRecord,
    ) -> paro_common::error::Result<()> {
        if instance.boot_config.is_in_memory() {
            return Ok(());
        }

        let path = Path::new(&record.storage_dir);
        if !path.exists() {
            return Ok(());
        }

        std::fs::remove_dir_all(path).map_err(|e| {
            paro_common::error::internal(format!(
                "Failed to remove database storage directory {}: {}",
                path.display(),
                e
            ))
        })
    }

    fn describe_orphan_directory(path: &Path) -> String {
        match Self::load_storage_identity(path) {
            Ok(Some(identity)) => format!(
                "catalog does not reference managed directory {}; storage identity database_id={} and the directory will not be revived automatically",
                path.display(),
                identity.database_id
            ),
            Ok(None) => format!(
                "catalog does not reference managed directory {}; storage identity is missing and the directory will not be revived automatically",
                path.display()
            ),
            Err(err) => format!(
                "catalog does not reference managed directory {}; storage identity could not be read ({}) and the directory will not be revived automatically",
                path.display(),
                err
            ),
        }
    }

    fn load_storage_identity(path: &Path) -> anyhow::Result<Option<DatabaseStorageIdentity>> {
        let meta_root = path.join("meta");
        if !meta_root.exists() {
            return Ok(None);
        }

        let store = FileMetadataStore::new(meta_root)?;
        let Some(payload) =
            store.get(crate::database::storage_identity::DATABASE_STORAGE_IDENTITY_KEY)?
        else {
            return Ok(None);
        };
        let identity: DatabaseStorageIdentity = serde_json::from_slice(&payload)?;
        identity.validate()?;
        Ok(Some(identity))
    }

    fn is_storage_identity_error(detail: &str) -> bool {
        detail.contains("Storage identity")
    }

    fn normalize_path(path: &Path) -> String {
        path.canonicalize()
            .unwrap_or_else(|_| path.to_path_buf())
            .to_string_lossy()
            .to_string()
    }
}
