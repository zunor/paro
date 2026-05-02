// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::routine::artifact::{ArtifactValidationState, ResolvedEnvArtifact};

use crate::runtime::artifact::gc::{ArtifactGcPolicy, ArtifactLifecycleState};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ArtifactCacheKey {
    pub tenant_or_security_domain: String,
    pub artifact_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactCacheEntry {
    pub state: ArtifactLifecycleState,
    pub artifact: ResolvedEnvArtifact,
    pub bytes: u64,
    pub last_used_at_ms: u64,
}

#[derive(Debug, Default)]
pub struct ArtifactCache {
    entries: BTreeMap<ArtifactCacheKey, ArtifactCacheEntry>,
}

impl ArtifactCache {
    pub fn insert(
        &mut self,
        key: ArtifactCacheKey,
        artifact: ResolvedEnvArtifact,
        bytes: u64,
        now_ms: u64,
    ) {
        let state = match artifact.validation {
            ArtifactValidationState::Pending => ArtifactLifecycleState::Pending,
            ArtifactValidationState::Ready { .. } => ArtifactLifecycleState::Ready,
            ArtifactValidationState::Failed { .. } => ArtifactLifecycleState::Draining,
        };
        self.entries.insert(
            key,
            ArtifactCacheEntry {
                state,
                artifact,
                bytes,
                last_used_at_ms: now_ms,
            },
        );
    }

    pub fn get_mut(
        &mut self,
        key: &ArtifactCacheKey,
        now_ms: u64,
    ) -> Option<&mut ArtifactCacheEntry> {
        let entry = self.entries.get_mut(key)?;
        entry.last_used_at_ms = now_ms;
        Some(entry)
    }

    pub fn mark_draining(&mut self, key: &ArtifactCacheKey) {
        if let Some(entry) = self.entries.get_mut(key) {
            entry.state = ArtifactLifecycleState::Draining;
        }
    }

    pub fn sweep(&mut self, policy: &ArtifactGcPolicy, now_ms: u64) -> Vec<ArtifactCacheKey> {
        let removable = self
            .entries
            .iter()
            .filter_map(|(key, entry)| {
                let age = now_ms.saturating_sub(entry.last_used_at_ms);
                policy.can_delete(entry.state, age).then_some(key.clone())
            })
            .collect::<Vec<_>>();
        for key in &removable {
            self.entries.remove(key);
        }
        removable
    }

    pub fn contains(&self, key: &ArtifactCacheKey) -> bool {
        self.entries.contains_key(key)
    }
}
