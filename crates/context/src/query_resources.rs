// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::ExecutionResources;
use paro_common::error::Result;
use paro_common::identity::GraphId;
use paro_function::scalar::cast::CastFunctionSet;
use paro_storage::index::graph::{
    GraphProjectionIndexManager, GraphReadSnapshot, GraphStatistics, GraphStorageGeneration,
};
use std::sync::Arc;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct QueryResourceGovernance {
    pub query_group: Option<String>,
    pub cpu_quota: Option<usize>,
    pub memory_quota: Option<usize>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ConnectionInfoSnapshot {
    pub connection_id: Option<u64>,
    pub application_name: Option<String>,
    pub client_addr: Option<String>,
}

pub trait ConnectionInfoProvider: Send + Sync {
    fn snapshot(&self) -> ConnectionInfoSnapshot;
}

pub trait SharedPlanCacheHandle: Send + Sync {}

pub trait GraphIndexProvider: Send + Sync {
    fn snapshot(&self, id: &GraphId) -> Option<GraphReadSnapshot>;

    fn statistics(&self, id: &GraphId) -> Option<Arc<GraphStatistics>> {
        self.snapshot(id)
            .map(|snapshot| snapshot.statistics().clone())
    }
}

pub trait GraphRegistry: Send + Sync {
    fn register_generation(&self, id: &GraphId, generation: GraphStorageGeneration);
    fn unregister(&self, id: &GraphId);
    fn publish_generation(&self, id: &GraphId, generation: GraphStorageGeneration) -> Result<()>;
}

#[derive(Clone)]
pub struct QueryResources {
    pub infra: Arc<ExecutionResources>,
    pub cast_functions: Arc<CastFunctionSet>,
    pub graph_index: Arc<dyn GraphIndexProvider>,
    pub governance: QueryResourceGovernance,
    pub plan_cache: Option<Arc<dyn SharedPlanCacheHandle>>,
    pub connection_info: Option<Arc<dyn ConnectionInfoProvider>>,
}

impl std::fmt::Debug for QueryResources {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryResources").finish_non_exhaustive()
    }
}

impl GraphIndexProvider for GraphProjectionIndexManager {
    fn snapshot(&self, id: &GraphId) -> Option<GraphReadSnapshot> {
        self.snapshot(&id.runtime_key())
    }
}

impl GraphRegistry for GraphProjectionIndexManager {
    fn register_generation(&self, id: &GraphId, generation: GraphStorageGeneration) {
        self.register_generation(&id.runtime_key(), generation);
    }

    fn unregister(&self, id: &GraphId) {
        self.unregister(&id.runtime_key());
    }

    fn publish_generation(&self, id: &GraphId, generation: GraphStorageGeneration) -> Result<()> {
        let _ = self.publish_generation(&id.runtime_key(), generation);
        Ok(())
    }
}
