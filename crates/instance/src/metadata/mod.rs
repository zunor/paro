// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use self::instance_catalog_store::InstanceCatalogStore;
use self::instance_layout::InstanceLayout;
use self::instance_owner::InstanceOwnerGuard;
use self::instance_run_state::{
    InstanceLifecycleState, InstanceRunState, InstanceRunStateStore, INSTANCE_RUN_STATE_KEY,
};
use crate::InstanceCatalog;
use paro_common::logging::targets;
use std::sync::Arc;

pub mod instance_catalog;
pub mod instance_catalog_store;
pub mod instance_layout;
pub mod instance_owner;
pub mod instance_run_state;

/// Durable metadata state owned by an instance.
#[derive(Debug)]
pub struct InstanceMetadata {
    layout: Option<InstanceLayout>,
    catalog_store: Arc<InstanceCatalogStore>,
    run_state_store: Arc<InstanceRunStateStore>,
    _owner_guard: Option<InstanceOwnerGuard>,
}

impl InstanceMetadata {
    pub(crate) fn new_in_memory() -> Self {
        Self {
            layout: None,
            catalog_store: Arc::new(InstanceCatalogStore::new_in_memory()),
            run_state_store: Arc::new(InstanceRunStateStore::new_in_memory()),
            _owner_guard: None,
        }
    }

    pub(crate) fn new_persistent(
        layout: InstanceLayout,
        catalog_store: Arc<InstanceCatalogStore>,
        run_state_store: Arc<InstanceRunStateStore>,
        owner_guard: InstanceOwnerGuard,
    ) -> Self {
        Self {
            layout: Some(layout),
            catalog_store,
            run_state_store,
            _owner_guard: Some(owner_guard),
        }
    }

    pub(crate) fn layout(&self) -> Option<&InstanceLayout> {
        self.layout.as_ref()
    }

    pub(crate) fn catalog_store(&self) -> &Arc<InstanceCatalogStore> {
        &self.catalog_store
    }

    #[cfg(test)]
    pub(crate) fn run_state_store(&self) -> &Arc<InstanceRunStateStore> {
        &self.run_state_store
    }

    pub fn load_catalog(&self) -> paro_common::error::Result<InstanceCatalog> {
        self.catalog_store
            .load()
            .map_err(|e| {
                paro_common::error::internal(format!("Failed to load instance catalog: {e}"))
            })?
            .ok_or_else(|| paro_common::error::internal("Instance catalog is missing"))
    }

    pub fn persist_catalog(&self, catalog: &mut InstanceCatalog) -> paro_common::error::Result<()> {
        self.catalog_store.save(catalog).map_err(|e| {
            paro_common::error::internal(format!("Failed to persist instance catalog: {e}"))
        })
    }

    pub fn persist_run_state(
        &self,
        boot_id: u64,
        state: InstanceLifecycleState,
    ) -> paro_common::error::Result<()> {
        let last_clean = self.run_state_store.load_last_clean_summary();
        let run_state = InstanceRunState::dirty(boot_id, state, last_clean);
        self.run_state_store.save(&run_state).map_err(|e| {
            paro_common::error::internal(format!(
                "Failed to persist instance run state {state:?}: {e}"
            ))
        })
    }

    pub fn persist_dirty_run_state(&self, boot_id: u64) -> paro_common::error::Result<()> {
        let last_clean = self.run_state_store.load_last_clean_summary();
        let state =
            InstanceRunState::dirty(boot_id, InstanceLifecycleState::ShuttingDown, last_clean);
        self.run_state_store.save(&state).map_err(|e| {
            paro_common::error::internal(format!("Failed to persist dirty instance run state: {e}"))
        })
    }

    pub fn persist_clean_run_state(
        &self,
        boot_id: u64,
        database_count: u64,
        default_database_id: Option<u64>,
    ) -> paro_common::error::Result<()> {
        let state = InstanceRunState::clean(boot_id, database_count, default_database_id);
        self.run_state_store.save(&state).map_err(|e| {
            paro_common::error::internal(format!("Failed to persist clean instance run state: {e}"))
        })
    }

    pub fn load_previous_run_state(&self) -> Option<InstanceRunState> {
        match self.run_state_store.load() {
            Ok(state) => state,
            Err(err) => {
                let run_state_path = self
                    .layout()
                    .map(|layout| layout.run_state_path().display().to_string())
                    .unwrap_or_else(|| INSTANCE_RUN_STATE_KEY.to_string());
                tracing::warn!(
                    target: targets::INSTANCE,
                    path = %run_state_path,
                    err = %err,
                    "Instance run_state is unreadable; falling back to full recovery"
                );
                None
            }
        }
    }
}
