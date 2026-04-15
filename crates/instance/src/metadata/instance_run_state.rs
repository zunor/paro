// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use parking_lot::Mutex;
use paro_storage::meta::MetadataStore;
use rand::random;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

pub const INSTANCE_RUN_STATE_FORMAT_VERSION: u16 = 2;
pub const INSTANCE_RUN_STATE_KEY: &str = "run_state";

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum InstanceLifecycleState {
    Starting,
    Running,
    ShuttingDown,
    Clean,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct InstanceRunState {
    pub format_version: u16,
    pub boot_id: u64,
    pub state: InstanceLifecycleState,
    pub last_transition_ms: i64,
    pub last_clean_shutdown_ms: Option<i64>,
    pub last_clean_database_count: Option<u64>,
    pub last_clean_default_database_id: Option<u64>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct LastCleanShutdownSummary {
    pub timestamp_ms: Option<i64>,
    pub database_count: Option<u64>,
    pub default_database_id: Option<u64>,
}

enum InstanceRunStateBackend {
    Durable(Arc<dyn MetadataStore>),
    Memory(Mutex<Option<InstanceRunState>>),
}

pub struct InstanceRunStateStore {
    backend: InstanceRunStateBackend,
}

impl std::fmt::Debug for InstanceRunStateStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InstanceRunStateStore")
            .field(
                "backend",
                &match &self.backend {
                    InstanceRunStateBackend::Durable(_) => "durable",
                    InstanceRunStateBackend::Memory(_) => "memory",
                },
            )
            .finish()
    }
}

impl InstanceRunState {
    pub(crate) fn dirty(
        boot_id: u64,
        state: InstanceLifecycleState,
        last_clean: LastCleanShutdownSummary,
    ) -> Self {
        Self {
            format_version: INSTANCE_RUN_STATE_FORMAT_VERSION,
            boot_id,
            state,
            last_transition_ms: current_time_ms(),
            last_clean_shutdown_ms: last_clean.timestamp_ms,
            last_clean_database_count: last_clean.database_count,
            last_clean_default_database_id: last_clean.default_database_id,
        }
    }

    pub(crate) fn clean(
        boot_id: u64,
        database_count: u64,
        default_database_id: Option<u64>,
    ) -> Self {
        let now_ms = current_time_ms();
        Self {
            format_version: INSTANCE_RUN_STATE_FORMAT_VERSION,
            boot_id,
            state: InstanceLifecycleState::Clean,
            last_transition_ms: now_ms,
            last_clean_shutdown_ms: Some(now_ms),
            last_clean_database_count: Some(database_count),
            last_clean_default_database_id: default_database_id,
        }
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        if self.format_version != INSTANCE_RUN_STATE_FORMAT_VERSION {
            anyhow::bail!(
                "Unsupported instance run state format version {}",
                self.format_version
            );
        }
        Ok(())
    }

    pub(crate) fn last_clean_summary(&self) -> LastCleanShutdownSummary {
        LastCleanShutdownSummary {
            timestamp_ms: self.last_clean_shutdown_ms,
            database_count: self.last_clean_database_count,
            default_database_id: self.last_clean_default_database_id,
        }
    }
}

impl InstanceRunStateStore {
    pub fn new_in_memory() -> Self {
        Self {
            backend: InstanceRunStateBackend::Memory(Mutex::new(None)),
        }
    }

    pub fn with_store(store: Arc<dyn MetadataStore>) -> Self {
        Self {
            backend: InstanceRunStateBackend::Durable(store),
        }
    }

    #[cfg(test)]
    pub(crate) fn durable_store(&self) -> Option<&Arc<dyn MetadataStore>> {
        match &self.backend {
            InstanceRunStateBackend::Durable(store) => Some(store),
            InstanceRunStateBackend::Memory(_) => None,
        }
    }

    pub fn load(&self) -> anyhow::Result<Option<InstanceRunState>> {
        match &self.backend {
            InstanceRunStateBackend::Durable(store) => {
                let Some(raw) = store
                    .get(INSTANCE_RUN_STATE_KEY)
                    .map_err(|e| anyhow::anyhow!(e))?
                else {
                    return Ok(None);
                };
                let state: InstanceRunState = serde_json::from_slice(&raw)?;
                state.validate()?;
                Ok(Some(state))
            }
            InstanceRunStateBackend::Memory(slot) => Ok(slot.lock().clone()),
        }
    }

    pub fn save(&self, state: &InstanceRunState) -> anyhow::Result<()> {
        state.validate()?;

        match &self.backend {
            InstanceRunStateBackend::Durable(store) => {
                let payload = serde_json::to_vec_pretty(state)?;
                store
                    .durable_put(INSTANCE_RUN_STATE_KEY, &payload)
                    .map_err(|e| anyhow::anyhow!(e))
            }
            InstanceRunStateBackend::Memory(slot) => {
                *slot.lock() = Some(state.clone());
                Ok(())
            }
        }
    }

    pub(crate) fn load_last_clean_summary(&self) -> LastCleanShutdownSummary {
        self.load()
            .ok()
            .flatten()
            .map(|state| state.last_clean_summary())
            .unwrap_or_default()
    }
}

pub(crate) fn current_time_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or(0) as i64
}

pub(crate) fn generate_boot_id() -> u64 {
    match random::<u64>() {
        0 => 1,
        boot_id => boot_id,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        generate_boot_id, InstanceLifecycleState, InstanceRunState, InstanceRunStateStore,
        LastCleanShutdownSummary, INSTANCE_RUN_STATE_FORMAT_VERSION,
    };

    #[test]
    fn boot_id_generation_is_non_zero() {
        assert_ne!(generate_boot_id(), 0);
    }

    #[test]
    fn memory_store_round_trips_run_state() {
        let store = InstanceRunStateStore::new_in_memory();
        let state = InstanceRunState::dirty(
            42,
            InstanceLifecycleState::Running,
            LastCleanShutdownSummary::default(),
        );

        store.save(&state).unwrap();

        let loaded = store.load().unwrap().expect("run_state should exist");
        assert_eq!(loaded.boot_id, 42);
        assert_eq!(loaded.state, InstanceLifecycleState::Running);
        assert_eq!(loaded.format_version, INSTANCE_RUN_STATE_FORMAT_VERSION);
    }

    #[test]
    fn clean_state_records_shutdown_summary() {
        let state = InstanceRunState::clean(7, 3, Some(1));
        assert_eq!(state.state, InstanceLifecycleState::Clean);
        assert_eq!(state.last_clean_database_count, Some(3));
        assert_eq!(state.last_clean_default_database_id, Some(1));
        assert!(state.last_clean_shutdown_ms.is_some());
    }
}
