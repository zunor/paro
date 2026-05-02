// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use serde::{Deserialize, Serialize};

use crate::routine::artifact::{ResolvedEnvArtifactId, RuntimeContract};
use crate::routine::env::DeclaredEnvSpec;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolveInputs {
    pub tenant_or_security_domain: String,
    pub runtime_selector: String,
    pub env: DeclaredEnvSpec,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolvedArtifactPlan {
    pub artifact_id: ResolvedEnvArtifactId,
    pub packages_fingerprint: String,
    pub imports_fingerprint: String,
    pub runtime_contract: RuntimeContract,
}

#[derive(Debug, Default, Clone, Copy)]
pub struct ArtifactResolver;

impl ArtifactResolver {
    pub fn resolve(
        &self,
        inputs: &ResolveInputs,
        runtime_contract: RuntimeContract,
    ) -> ResolvedArtifactPlan {
        let packages_fingerprint =
            stable_fingerprint(inputs.env.packages.iter().map(|pkg| pkg.spec.as_str()));
        let imports_fingerprint =
            stable_fingerprint(inputs.env.imports.iter().map(|import| import.uri.as_str()));
        let artifact_id = format!(
            "artifact-{}-{}-{}",
            stable_fingerprint([inputs.tenant_or_security_domain.as_str()]),
            packages_fingerprint,
            imports_fingerprint
        );
        ResolvedArtifactPlan {
            artifact_id,
            packages_fingerprint,
            imports_fingerprint,
            runtime_contract,
        }
    }
}

fn stable_fingerprint<'a>(values: impl IntoIterator<Item = &'a str>) -> String {
    let mut hash = 0xcbf29ce484222325_u64;
    for value in values {
        for byte in value.as_bytes() {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x100000001b3);
        }
        hash ^= 0xff;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("{hash:016x}")
}
