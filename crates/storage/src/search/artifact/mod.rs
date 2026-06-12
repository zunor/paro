// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

mod location;

use serde::{Deserialize, Serialize};

pub use location::{ArtifactFileId, ArtifactLocation, SegmentPagePointer};

use super::stats::SearchProviderStats;

/// Provider-specific GC signal consumed by the shared scheduler.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum GcDecision {
    Skip,
    CompactOnly,
    Heal,
    Rebuild,
}

/// Generic provider GC input carried by the Phase 0 contract.
///
/// Concrete providers can stash richer summaries in `provider_stats` without
/// forcing every callsite to depend on provider-specific structs yet.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ArtifactGcContext {
    pub bytes_on_disk: u64,
    pub tombstone_ratio: Option<f32>,
    pub query_pressure: Option<f32>,
    pub provider_stats: Option<SearchProviderStats>,
}

/// Provider-specific artifact GC policy.
pub trait ArtifactGcPolicy: Send + Sync {
    fn should_gc(&self, context: &ArtifactGcContext) -> GcDecision;
}

#[cfg(test)]
mod tests {
    use super::{
        ArtifactFileId, ArtifactGcContext, ArtifactGcPolicy, ArtifactLocation, GcDecision,
        SegmentPagePointer,
    };

    struct RebuildWhenDeleteHeavy;

    impl ArtifactGcPolicy for RebuildWhenDeleteHeavy {
        fn should_gc(&self, context: &ArtifactGcContext) -> GcDecision {
            if context.tombstone_ratio.unwrap_or_default() >= 0.5 {
                GcDecision::Rebuild
            } else {
                GcDecision::Skip
            }
        }
    }

    #[test]
    fn gc_policy_stays_provider_specific_but_typed() {
        let policy = RebuildWhenDeleteHeavy;
        assert_eq!(
            policy.should_gc(&ArtifactGcContext {
                tombstone_ratio: Some(0.75),
                ..ArtifactGcContext::default()
            }),
            GcDecision::Rebuild
        );
        assert_eq!(
            policy.should_gc(&ArtifactGcContext {
                tombstone_ratio: Some(0.1),
                ..ArtifactGcContext::default()
            }),
            GcDecision::Skip
        );
    }

    #[test]
    fn artifact_location_uses_typed_inline_page_and_sidecar_file_id() {
        let inline = ArtifactLocation::Inline {
            page: SegmentPagePointer {
                rowset_id: 10,
                segment_id: 2,
                column_id: 3,
                page_offset: 128,
                page_len: 4096,
                checksum: 42,
            },
        };
        let sidecar = ArtifactLocation::SidecarArtifactFile {
            file_id: ArtifactFileId {
                definition_id: 7,
                generation_id: 9,
                package_index: 1,
            },
            offset: 128,
            len: 4096,
            checksum: 42,
        };

        assert!(matches!(inline, ArtifactLocation::Inline { .. }));
        assert!(matches!(
            sidecar,
            ArtifactLocation::SidecarArtifactFile {
                file_id: ArtifactFileId {
                    definition_id: 7,
                    generation_id: 9,
                    package_index: 1
                },
                offset: 128,
                len: 4096,
                checksum: 42,
                ..
            }
        ));
    }
}
