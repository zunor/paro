// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use serde::{Deserialize, Serialize};

use super::resolve::ResolvedArtifactPlan;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MaterializedArtifactRoot {
    pub artifact_id: String,
    pub filesystem_root: String,
    pub template_base: Option<String>,
}

#[derive(Debug, Default, Clone, Copy)]
pub struct ArtifactMaterializer;

impl ArtifactMaterializer {
    pub fn materialize(
        &self,
        plan: &ResolvedArtifactPlan,
        root_dir: &str,
        template_dir: Option<&str>,
    ) -> MaterializedArtifactRoot {
        MaterializedArtifactRoot {
            artifact_id: plan.artifact_id.clone(),
            filesystem_root: format!("{root_dir}/{}", plan.artifact_id),
            template_base: template_dir.map(|dir| format!("{dir}/{}", plan.artifact_id)),
        }
    }
}
