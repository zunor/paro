//! Instance bootstrap.
//!
//! This stage ensures the system database and durable instance catalog exist.
//! It does not publish user databases into the runtime registry; once bootstrap
//! completes, all managed database opening flows back through instance recovery.

use super::recovery::InstanceRecovery;
use crate::{DatabaseHandle, InstanceDdlOwner, InstanceLifecycleState};
use crate::{Instance, InstanceCatalog};
use paro_common::logging::targets;
use std::sync::Arc;

pub(crate) struct InstanceBootstrap;

impl Instance {
    pub(crate) fn bootstrap(&self) -> paro_common::error::Result<()> {
        let _ddl_guard = self.lock_ddl(InstanceDdlOwner::Bootstrap)?;
        let previous_run_state = self.metadata.load_previous_run_state();
        self.metadata
            .persist_run_state(self.lifecycle.boot_id, InstanceLifecycleState::Starting)?;
        InstanceBootstrap::run(self)?;

        let startup_report = InstanceRecovery::run(self, previous_run_state)?;
        self.metadata
            .persist_run_state(self.lifecycle.boot_id, InstanceLifecycleState::Running)?;
        startup_report.log_summary();
        *self.lifecycle.startup_report.write().unwrap() = startup_report;
        self.database_service.registry().initialize_system_catalog();
        Ok(())
    }

    pub(crate) fn initialize_system_database(&self) -> paro_common::error::Result<()> {
        let mut system = self.database_service.registry().system.write();
        if system.is_some() {
            return Ok(());
        }

        let system_db = Arc::new(DatabaseHandle::new_system(
            0,
            Arc::clone(&self.boot_config.buffer_pool),
        ));
        system_db.bind_task_scheduler(self.runtime.scheduler().clone());
        system_db.initialize().map_err(|e| {
            paro_common::error::internal(format!("Failed to initialize system catalog: {e}"))
        })?;
        *system = Some(system_db);
        Ok(())
    }
}

impl InstanceBootstrap {
    pub(crate) fn run(instance: &Instance) -> paro_common::error::Result<()> {
        instance.initialize_system_database()?;

        if instance.metadata.catalog_store().exists().map_err(|e| {
            paro_common::error::internal(format!("Failed to probe instance catalog store: {e}"))
        })? {
            let catalog = instance.metadata.load_catalog()?;
            tracing::info!(
                target: targets::INSTANCE,
                database_count = catalog.databases.len(),
                default_database_id = ?catalog.default_database_id,
                "Opened existing instance catalog"
            );
            return Ok(());
        }

        tracing::info!(
            target: targets::INSTANCE,
            "Bootstrapping fresh instance catalog"
        );

        let mut catalog = InstanceCatalog::new_empty();
        instance.database_service.provision_managed_database(
            &instance.database_ddl_context(),
            &mut catalog,
            instance.boot_config.default_database_name(),
            true,
            false,
        )?;

        tracing::info!(
            target: targets::INSTANCE,
            database_count = catalog.databases.len(),
            default_database_id = ?catalog.default_database_id,
            "Fresh instance catalog prepared"
        );
        Ok(())
    }
}
