// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeMap;

use crate::rowset::RowsetId;

use super::artifact::GcDecision;
use super::capability::{SearchIndexDefinition, SearchIndexKind};
use super::inline_sink::{CostEstimate, HnswInlineThreshold};
pub(crate) use super::lifecycle::catch_up_planner::CatchUpPlanner;
pub(crate) use super::lifecycle::maintenance_scheduler::{
    sidecar_repack_needed, InlineSearchAdmission, MaintenanceScheduler,
};
pub use super::lifecycle::maintenance_scheduler::{
    MaintenanceAdmissionDecision, MaintenanceAdmissionGrant, MaintenanceAdmissionPolicy,
    MaintenanceAdmissionReason, MaintenanceAdmissionRequest, MaintenanceDatabaseDrainStatus,
    MaintenanceFairnessKey,
};
use super::stats::{CatchUpBacklogTier, MaintenancePriority, SearchDefinitionId};
use super::tail::{TailMutationKind, TailPendingEntry};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum SearchMaintenanceAction {
    #[default]
    Skip,
    CatchUp,
    RepackSidecar,
    Compact,
    CompactManifestDelta,
    Rebuild,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DefinitionMaintenanceReport {
    pub definition_id: u64,
    pub action: SearchMaintenanceAction,
    pub provider_request: Option<ProviderMaintenanceRequest>,
    pub admission: MaintenanceAdmissionDecision,
    pub gc_decision: GcDecision,
    pub estimate: CostEstimate,
    pub manifest_delta_compaction_requested: bool,
    pub sidecar_repack_requested: bool,
    pub tail_pending_rowsets: usize,
    pub tail_pending_rows: u64,
    pub priority: MaintenancePriority,
    pub backlog_tier: CatchUpBacklogTier,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SearchMaintenanceReport {
    pub definitions_considered: usize,
    pub definitions_updated: usize,
    pub catch_up_rowsets: usize,
    pub compaction_requested: bool,
    pub manifest_delta_compaction_requested: bool,
    pub sidecar_repack_requested: bool,
    pub definitions: Vec<DefinitionMaintenanceReport>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProviderMaintenanceRequest {
    Hnsw(HnswMaintenanceRequest),
}

impl ProviderMaintenanceRequest {
    pub fn as_hnsw(&self) -> Option<&HnswMaintenanceRequest> {
        match self {
            Self::Hnsw(request) => Some(request),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HnswMaintenanceRequest {
    pub definition_id: SearchDefinitionId,
    pub generation_id: u64,
    pub tail_window: Vec<TailPendingEntry>,
    pub rowset_refs: Vec<HnswMaintenanceRowsetRef>,
    pub estimated_graph_memory_bytes: u64,
    pub dimension: u32,
    pub freshness_priority: MaintenancePriority,
}

impl HnswMaintenanceRequest {
    pub fn new(
        definition: &SearchIndexDefinition,
        provider: &super::HnswProviderConfig,
        generation_id: u64,
        tail_window: Vec<TailPendingEntry>,
        freshness_priority: MaintenancePriority,
    ) -> Option<Self> {
        if definition.kind != SearchIndexKind::Hnsw || tail_window.is_empty() {
            return None;
        }
        let rowset_refs = hnsw_rowset_refs(&tail_window);
        let vector_count = rowset_refs.iter().map(|rowset| rowset.row_count).sum();
        Some(Self {
            definition_id: definition.definition_id,
            generation_id,
            tail_window,
            rowset_refs,
            estimated_graph_memory_bytes: HnswInlineThreshold::estimate_graph_memory_bytes(
                vector_count,
                provider.dimension,
                provider.m,
            ),
            dimension: provider.dimension,
            freshness_priority,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HnswMaintenanceRowsetRef {
    pub rowset_id: RowsetId,
    pub segment_ids: Vec<u32>,
    pub row_count: u64,
    pub byte_count: u64,
}

fn hnsw_rowset_refs(tail_window: &[TailPendingEntry]) -> Vec<HnswMaintenanceRowsetRef> {
    let mut refs = BTreeMap::<RowsetId, HnswMaintenanceRowsetRef>::new();
    for entry in tail_window
        .iter()
        .filter(|entry| entry.mutation != TailMutationKind::Delete)
    {
        let rowset_ref = refs
            .entry(entry.rowset_id)
            .or_insert_with(|| HnswMaintenanceRowsetRef {
                rowset_id: entry.rowset_id,
                segment_ids: Vec::new(),
                row_count: 0,
                byte_count: 0,
            });
        rowset_ref.row_count = rowset_ref.row_count.saturating_add(entry.row_count);
        rowset_ref.byte_count = rowset_ref.byte_count.saturating_add(entry.byte_count);
        rowset_ref
            .segment_ids
            .extend(entry.segment_ids.iter().copied());
        rowset_ref.segment_ids.sort_unstable();
        rowset_ref.segment_ids.dedup();
    }
    refs.into_values().collect()
}
#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::Arc;

    use super::*;
    use crate::search::artifact::ArtifactGcContext;
    use crate::search::capability::{CoverageState, SearchFreshnessPolicy};
    use crate::search::cursor::GenerationArtifactSet;
    use crate::search::inline_sink::{
        AdmissionDecision, AdmissionRejectReason, AdmissionWaitReason, FlushSearchMode,
        HnswInlineBuildEstimate, HnswInlineThreshold, InlineAdmissionRequest, MaintenanceBenefit,
        MaintenanceCost, SearchAdmission,
    };
    use crate::search::lifecycle::maintenance_scheduler::ManifestDeltaPressure;
    use crate::search::manifest::{
        GenerationManifestRoot, LoadedManifest, ManifestCodecKind, ManifestFileRef,
        DELTA_COUNT_HARD_LIMIT, DELTA_COUNT_SOFT_LIMIT,
    };
    use crate::search::stats::{
        BuildWatermarks, ExecutionModes, GenerationMaintenanceState, GenerationRecoveryState,
        GenerationStats,
    };
    use crate::search::tail::{TailEntryId, TailPendingEntry, TailRowImageRef};

    fn sample_loaded_manifest(tail_rows: u64, recent_delta_count: usize) -> LoadedManifest {
        let tail_pending_entries = if tail_rows == 0 {
            Vec::new()
        } else {
            vec![TailPendingEntry {
                entry_id: TailEntryId(1),
                rowset_id: 11,
                segment_ids: vec![0],
                mutation: TailMutationKind::Append,
                row_count: tail_rows,
                byte_count: 1024,
                row_image_ref: Some(TailRowImageRef::WholeRowset),
            }]
        };
        LoadedManifest {
            root: GenerationManifestRoot {
                definition_id: 7,
                generation_id: 1,
                build_epoch: 1,
                build_snapshot_version: 1,
                indexed_through_ts: 1,
                config_fingerprint: 99,
                coverage: if tail_rows == 0 {
                    CoverageState::Complete
                } else {
                    CoverageState::TailPending {
                        pending_rowsets: 1,
                        pending_segments: 1,
                        pending_rows: tail_rows,
                        exact_tail_merge: true,
                    }
                },
                generation_stats: GenerationStats::default(),
                next_tail_entry_id: TailEntryId(2),
                execution_modes: ExecutionModes::default(),
                maintenance_state: GenerationMaintenanceState {
                    build_watermarks: BuildWatermarks::default(),
                    recovery: GenerationRecoveryState {
                        tail_pending_rowsets: usize::from(tail_rows > 0),
                        tail_pending_rows: tail_rows,
                        ..Default::default()
                    },
                    tombstone_rows: 0,
                    tombstone_ratio_millis: 0,
                },
                root_version: 1,
                checksum: 0,
                shard_files: Vec::new(),
                recent_delta_files: (0..recent_delta_count)
                    .map(|ordinal| ManifestFileRef {
                        file_name: format!("delta_{ordinal}.json"),
                        codec: ManifestCodecKind::JSON_DEBUG_V1,
                    })
                    .collect(),
                materialized_state_file: None,
            },
            root_path: PathBuf::new(),
            shard_paths: Vec::new(),
            delta_paths: Vec::new(),
            materialized_state_path: None,
            embedded_materialized_state: false,
            artifacts: Arc::new(GenerationArtifactSet::default()),
            tail_pending_entries,
        }
    }

    #[test]
    fn maintenance_scheduler_marks_tail_backlog_as_catch_up_with_cost_benefit() {
        let manifest = sample_loaded_manifest(2, 0);

        let decision = MaintenanceScheduler::default().decide_definition(
            &SearchIndexDefinition {
                definition_id: 7,
                table_id: 11,
                name: "docs_fts".to_string(),
                kind: SearchIndexKind::FullText,
                column_ids: vec![0],
                expression: None,
                provider_config: serde_json::json!({"version": 1, "config": "simple"}),
                freshness_policy: SearchFreshnessPolicy::default_for_kind(
                    SearchIndexKind::FullText,
                ),
                config_fingerprint: 99,
            },
            &manifest,
            GcDecision::Skip,
            &ArtifactGcContext::default(),
            0,
        );

        assert_eq!(decision.action, SearchMaintenanceAction::CatchUp);
        assert!(decision.admission.is_admitted());
        assert_eq!(decision.estimate.benefit.expected_tail_rows_drained, 2);
        assert!(decision.estimate.cost.cpu_ns > 0);
        assert!(decision.estimate.cost.publish_bytes > 0);
    }

    #[test]
    fn maintenance_scheduler_requests_manifest_delta_compaction_over_soft_window() {
        let manifest = sample_loaded_manifest(0, DELTA_COUNT_SOFT_LIMIT + 1);

        let decision = MaintenanceScheduler::default().decide_definition(
            &SearchIndexDefinition {
                definition_id: 7,
                table_id: 11,
                name: "docs_fts".to_string(),
                kind: SearchIndexKind::FullText,
                column_ids: vec![0],
                expression: None,
                provider_config: serde_json::json!({"version": 1, "config": "simple"}),
                freshness_policy: SearchFreshnessPolicy::default_for_kind(
                    SearchIndexKind::FullText,
                ),
                config_fingerprint: 99,
            },
            &manifest,
            GcDecision::Skip,
            &ArtifactGcContext::default(),
            0,
        );

        assert_eq!(
            decision.action,
            SearchMaintenanceAction::CompactManifestDelta
        );
        assert!(decision.manifest_delta_compaction_requested);
        assert!(decision.admission.is_admitted());
        assert!(decision.estimate.cost.publish_bytes > 0);
        assert!(decision.estimate.benefit.expected_open_cost_saved_us > 0);
    }

    #[test]
    fn maintenance_scheduler_promotes_hard_delta_window_to_queryability_priority() {
        let scheduler = MaintenanceScheduler::with_policy(MaintenanceAdmissionPolicy {
            fulltext_concurrency: 1,
            ..MaintenanceAdmissionPolicy::default()
        });
        let manifest = sample_loaded_manifest(0, DELTA_COUNT_HARD_LIMIT + 1);
        let definition = SearchIndexDefinition {
            definition_id: 7,
            table_id: 11,
            name: "docs_fts".to_string(),
            kind: SearchIndexKind::FullText,
            column_ids: vec![0],
            expression: None,
            provider_config: serde_json::json!({"version": 1, "config": "simple"}),
            freshness_policy: SearchFreshnessPolicy::default_for_kind(SearchIndexKind::FullText),
            config_fingerprint: 99,
        };
        let decision = scheduler.plan_definition(
            &definition,
            &manifest,
            GcDecision::Skip,
            &ArtifactGcContext::default(),
            0,
        );
        let hard_delta_request = scheduler.admission_request(&definition, &manifest, &decision);
        let opportunistic = admission_request(
            8,
            SearchIndexKind::FullText,
            MaintenancePriority::Opportunistic,
            CatchUpBacklogTier::Healthy,
            CostEstimate {
                benefit: MaintenanceBenefit {
                    expected_tail_rows_drained: 1_000_000,
                    ..Default::default()
                },
                ..Default::default()
            },
        );

        assert_eq!(
            decision.manifest_delta_pressure,
            ManifestDeltaPressure::Hard
        );
        assert_eq!(
            hard_delta_request.action,
            SearchMaintenanceAction::CompactManifestDelta
        );
        assert_eq!(hard_delta_request.priority, MaintenancePriority::Critical);
        assert_eq!(
            hard_delta_request.backlog_tier,
            CatchUpBacklogTier::Degraded
        );

        let decisions = scheduler.admit_requests(&[opportunistic, hard_delta_request]);
        assert_eq!(
            decisions[0],
            MaintenanceAdmissionDecision::Deferred {
                reason: MaintenanceAdmissionReason::ProviderConcurrency,
            }
        );
        assert!(decisions[1].is_admitted());
    }

    #[test]
    fn maintenance_scheduler_queue_runs_hard_delta_compaction_before_opportunistic_work() {
        let scheduler = Arc::new(MaintenanceScheduler::with_policy(
            MaintenanceAdmissionPolicy {
                fulltext_concurrency: 2,
                ..MaintenanceAdmissionPolicy::default()
            },
        ));
        let opportunistic = admission_request(
            8,
            SearchIndexKind::FullText,
            MaintenancePriority::Opportunistic,
            CatchUpBacklogTier::Healthy,
            CostEstimate {
                benefit: MaintenanceBenefit {
                    expected_tail_rows_drained: 1_000_000,
                    ..Default::default()
                },
                ..Default::default()
            },
        );
        let mut hard_delta = admission_request(
            7,
            SearchIndexKind::FullText,
            MaintenancePriority::Critical,
            CatchUpBacklogTier::Degraded,
            CostEstimate {
                benefit: MaintenanceBenefit {
                    expected_open_cost_saved_us: 128_000,
                    ..Default::default()
                },
                ..Default::default()
            },
        );
        hard_delta.action = SearchMaintenanceAction::CompactManifestDelta;

        assert!(scheduler.schedule_requests(&[opportunistic])[0].is_admitted());
        assert!(scheduler.schedule_requests(&[hard_delta])[0].is_admitted());
        assert_eq!(scheduler.queued_task_count(), 2);

        let first = scheduler.pop_next_task().expect("first queued task");
        assert_eq!(first.request.definition_id, 7);
        assert_eq!(
            first.request.action,
            SearchMaintenanceAction::CompactManifestDelta
        );
        drop(scheduler.scoped_task_lease(&first));

        let second = scheduler.pop_next_task().expect("second queued task");
        assert_eq!(second.request.definition_id, 8);
        assert_eq!(second.request.action, SearchMaintenanceAction::CatchUp);
        drop(scheduler.scoped_task_lease(&second));
        assert_eq!(scheduler.queued_task_count(), 0);

        let follow_up = admission_request(
            9,
            SearchIndexKind::FullText,
            MaintenancePriority::Critical,
            CatchUpBacklogTier::Degraded,
            CostEstimate::default(),
        );
        assert!(
            scheduler.admit_requests(&[follow_up])[0].is_admitted(),
            "queued task leases must release active provider slots after execution"
        );
    }

    #[test]
    fn maintenance_admission_prioritizes_critical_over_opportunistic() {
        let scheduler = MaintenanceScheduler::with_policy(MaintenanceAdmissionPolicy {
            fulltext_concurrency: 1,
            ..MaintenanceAdmissionPolicy::default()
        });
        let requests = vec![
            admission_request(
                11,
                SearchIndexKind::FullText,
                MaintenancePriority::Opportunistic,
                CatchUpBacklogTier::Healthy,
                CostEstimate {
                    benefit: MaintenanceBenefit {
                        expected_tail_rows_drained: 10,
                        ..Default::default()
                    },
                    ..Default::default()
                },
            ),
            admission_request(
                12,
                SearchIndexKind::FullText,
                MaintenancePriority::Critical,
                CatchUpBacklogTier::Degraded,
                CostEstimate {
                    benefit: MaintenanceBenefit {
                        expected_tail_rows_drained: 1,
                        ..Default::default()
                    },
                    ..Default::default()
                },
            ),
        ];

        let decisions = scheduler.admit_requests(&requests);

        assert_eq!(
            decisions[0],
            MaintenanceAdmissionDecision::Deferred {
                reason: MaintenanceAdmissionReason::ProviderConcurrency,
            }
        );
        assert!(decisions[1].is_admitted());
    }

    #[test]
    fn maintenance_admission_defers_when_resource_budget_is_exhausted() {
        let scheduler = MaintenanceScheduler::with_policy(MaintenanceAdmissionPolicy {
            cpu_ns_budget: 100,
            ..MaintenanceAdmissionPolicy::default()
        });
        let request = admission_request(
            11,
            SearchIndexKind::Sparse,
            MaintenancePriority::Elevated,
            CatchUpBacklogTier::Elevated,
            CostEstimate {
                cost: MaintenanceCost {
                    cpu_ns: 101,
                    ..Default::default()
                },
                ..Default::default()
            },
        );

        let decisions = scheduler.admit_requests(&[request]);

        assert_eq!(
            decisions,
            vec![MaintenanceAdmissionDecision::Deferred {
                reason: MaintenanceAdmissionReason::CpuBudget,
            }]
        );
    }

    #[test]
    fn maintenance_admission_reserves_foreground_read_io_before_background_catch_up() {
        let scheduler = MaintenanceScheduler::with_policy(MaintenanceAdmissionPolicy {
            io_read_bytes_budget: 1_024,
            foreground_io_read_bytes_reserved: 900,
            ..MaintenanceAdmissionPolicy::default()
        });
        let background = admission_request(
            11,
            SearchIndexKind::FullText,
            MaintenancePriority::Opportunistic,
            CatchUpBacklogTier::Healthy,
            CostEstimate {
                cost: MaintenanceCost {
                    io_read_bytes: 256,
                    ..Default::default()
                },
                ..Default::default()
            },
        );
        let tiny = admission_request(
            12,
            SearchIndexKind::FullText,
            MaintenancePriority::Opportunistic,
            CatchUpBacklogTier::Healthy,
            CostEstimate {
                cost: MaintenanceCost {
                    io_read_bytes: 64,
                    ..Default::default()
                },
                ..Default::default()
            },
        );

        let decisions = scheduler.admit_requests(&[background, tiny]);

        assert_eq!(
            decisions[0],
            MaintenanceAdmissionDecision::Deferred {
                reason: MaintenanceAdmissionReason::IoReadBudget,
            }
        );
        assert!(decisions[1].is_admitted());
    }

    #[test]
    fn maintenance_admission_does_not_let_oversized_hnsw_starve_small_fulltext_catch_up() {
        let scheduler = MaintenanceScheduler::with_policy(MaintenanceAdmissionPolicy {
            memory_peak_bytes_budget: 64 * 1024 * 1024,
            hnsw_concurrency: 1,
            fulltext_concurrency: 1,
            ..MaintenanceAdmissionPolicy::default()
        });
        let hnsw_large = admission_request(
            11,
            SearchIndexKind::Hnsw,
            MaintenancePriority::Elevated,
            CatchUpBacklogTier::Elevated,
            CostEstimate {
                cost: MaintenanceCost {
                    memory_peak_bytes: 128 * 1024 * 1024,
                    ..Default::default()
                },
                benefit: MaintenanceBenefit {
                    expected_tail_rows_drained: 1_000_000,
                    ..Default::default()
                },
            },
        );
        let fulltext_small = admission_request(
            12,
            SearchIndexKind::FullText,
            MaintenancePriority::Elevated,
            CatchUpBacklogTier::Elevated,
            CostEstimate {
                cost: MaintenanceCost {
                    memory_peak_bytes: 1024 * 1024,
                    ..Default::default()
                },
                benefit: MaintenanceBenefit {
                    expected_tail_rows_drained: 10,
                    ..Default::default()
                },
            },
        );

        let decisions = scheduler.admit_requests(&[hnsw_large, fulltext_small]);

        assert_eq!(
            decisions[0],
            MaintenanceAdmissionDecision::Deferred {
                reason: MaintenanceAdmissionReason::MemoryBudget,
            }
        );
        assert!(
            decisions[1].is_admitted(),
            "a deferred oversized HNSW task must not consume the budget needed by a small FullText catch-up"
        );
    }

    #[test]
    fn maintenance_admission_enforces_provider_specific_limits() {
        let scheduler = MaintenanceScheduler::with_policy(MaintenanceAdmissionPolicy {
            fulltext_concurrency: 1,
            sparse_concurrency: 1,
            hnsw_concurrency: 1,
            memory_peak_bytes_budget: 64 * 1024 * 1024,
            ..MaintenanceAdmissionPolicy::default()
        });
        let requests = vec![
            admission_request(
                11,
                SearchIndexKind::FullText,
                MaintenancePriority::Elevated,
                CatchUpBacklogTier::Elevated,
                CostEstimate::default(),
            ),
            admission_request(
                12,
                SearchIndexKind::FullText,
                MaintenancePriority::Elevated,
                CatchUpBacklogTier::Elevated,
                CostEstimate::default(),
            ),
            admission_request(
                13,
                SearchIndexKind::Sparse,
                MaintenancePriority::Elevated,
                CatchUpBacklogTier::Elevated,
                CostEstimate::default(),
            ),
            admission_request(
                14,
                SearchIndexKind::Hnsw,
                MaintenancePriority::Elevated,
                CatchUpBacklogTier::Elevated,
                CostEstimate {
                    cost: MaintenanceCost {
                        memory_peak_bytes: 32 * 1024 * 1024,
                        ..Default::default()
                    },
                    ..Default::default()
                },
            ),
            admission_request(
                15,
                SearchIndexKind::Hnsw,
                MaintenancePriority::Elevated,
                CatchUpBacklogTier::Elevated,
                CostEstimate {
                    cost: MaintenanceCost {
                        memory_peak_bytes: 128 * 1024 * 1024,
                        ..Default::default()
                    },
                    ..Default::default()
                },
            ),
        ];

        let decisions = scheduler.admit_requests(&requests);

        assert!(decisions[0].is_admitted());
        assert_eq!(
            decisions[1],
            MaintenanceAdmissionDecision::Deferred {
                reason: MaintenanceAdmissionReason::ProviderConcurrency,
            }
        );
        assert!(decisions[2].is_admitted());
        assert!(decisions[3].is_admitted());
        assert_eq!(
            decisions[4],
            MaintenanceAdmissionDecision::Deferred {
                reason: MaintenanceAdmissionReason::ProviderConcurrency,
            }
        );
    }

    #[test]
    fn maintenance_scheduler_holds_resources_until_grant_release() {
        let scheduler = MaintenanceScheduler::with_policy(MaintenanceAdmissionPolicy {
            cpu_ns_budget: 100,
            fulltext_concurrency: 1,
            ..MaintenanceAdmissionPolicy::default()
        });
        let first = admission_request(
            11,
            SearchIndexKind::FullText,
            MaintenancePriority::Elevated,
            CatchUpBacklogTier::Elevated,
            CostEstimate {
                cost: MaintenanceCost {
                    cpu_ns: 100,
                    ..Default::default()
                },
                ..Default::default()
            },
        );
        let second = admission_request(
            12,
            SearchIndexKind::FullText,
            MaintenancePriority::Elevated,
            CatchUpBacklogTier::Elevated,
            CostEstimate {
                cost: MaintenanceCost {
                    cpu_ns: 1,
                    ..Default::default()
                },
                ..Default::default()
            },
        );

        let first_decision = scheduler.admit_requests(&[first])[0];
        let grant = first_decision.grant().expect("first grant");
        let held = scheduler.admit_requests(std::slice::from_ref(&second));

        assert_eq!(
            held,
            vec![MaintenanceAdmissionDecision::Deferred {
                reason: MaintenanceAdmissionReason::ProviderConcurrency,
            }]
        );

        assert!(scheduler.release(grant.grant_id));
        let released = scheduler.admit_requests(&[second]);
        assert!(released[0].is_admitted());
    }

    #[test]
    fn maintenance_admission_defers_sidecar_repack_when_write_budget_is_exhausted() {
        let scheduler = MaintenanceScheduler::with_policy(MaintenanceAdmissionPolicy {
            io_write_bytes_budget: 100,
            ..MaintenanceAdmissionPolicy::default()
        });
        let request = MaintenanceAdmissionRequest {
            definition_id: 21,
            action: SearchMaintenanceAction::RepackSidecar,
            fairness_key: MaintenanceFairnessKey {
                database_id: 0,
                table_id: 42,
                provider: SearchIndexKind::FullText,
            },
            priority: MaintenancePriority::Opportunistic,
            backlog_tier: CatchUpBacklogTier::Healthy,
            estimate: CostEstimate {
                cost: MaintenanceCost {
                    io_write_bytes: 101,
                    ..Default::default()
                },
                ..Default::default()
            },
        };

        let decisions = scheduler.admit_requests(&[request]);

        assert_eq!(
            decisions,
            vec![MaintenanceAdmissionDecision::Deferred {
                reason: MaintenanceAdmissionReason::IoWriteBudget,
            }]
        );
    }

    #[test]
    fn maintenance_admission_rejects_draining_database() {
        let scheduler = MaintenanceScheduler::with_policy(
            MaintenanceAdmissionPolicy::default().with_draining_database(3),
        );
        let mut request = admission_request(
            11,
            SearchIndexKind::FullText,
            MaintenancePriority::Critical,
            CatchUpBacklogTier::Degraded,
            CostEstimate::default(),
        );
        request.fairness_key.database_id = 3;

        let decisions = scheduler.admit_requests(&[request]);

        assert_eq!(
            decisions,
            vec![MaintenanceAdmissionDecision::Rejected {
                reason: MaintenanceAdmissionReason::DatabaseDraining,
            }]
        );
    }

    #[test]
    fn maintenance_database_drain_waits_for_active_grants_and_rejects_new_work() {
        let scheduler = MaintenanceScheduler::with_policy(MaintenanceAdmissionPolicy {
            fulltext_concurrency: 2,
            ..MaintenanceAdmissionPolicy::default()
        });
        let mut first = admission_request(
            11,
            SearchIndexKind::FullText,
            MaintenancePriority::Elevated,
            CatchUpBacklogTier::Elevated,
            CostEstimate::default(),
        );
        first.fairness_key.database_id = 3;
        let first_decision = scheduler.admit_requests(&[first])[0];
        let grant = first_decision.grant().expect("first grant");

        let draining = scheduler.begin_database_drain(3);

        assert_eq!(
            draining,
            MaintenanceDatabaseDrainStatus {
                database_id: 3,
                active_grants: 1,
                is_drained: false,
            }
        );

        let mut new_work = admission_request(
            12,
            SearchIndexKind::FullText,
            MaintenancePriority::Critical,
            CatchUpBacklogTier::Degraded,
            CostEstimate::default(),
        );
        new_work.fairness_key.database_id = 3;
        let rejected = scheduler.admit_requests(&[new_work]);
        assert_eq!(
            rejected,
            vec![MaintenanceAdmissionDecision::Rejected {
                reason: MaintenanceAdmissionReason::DatabaseDraining,
            }]
        );

        assert!(scheduler.release(grant.grant_id));
        let drained = scheduler.begin_database_drain(3);
        assert_eq!(
            drained,
            MaintenanceDatabaseDrainStatus {
                database_id: 3,
                active_grants: 0,
                is_drained: true,
            }
        );
    }

    #[test]
    fn inline_search_admission_maps_budget_deferral_to_wait() {
        let admission = InlineSearchAdmission::with_policy(MaintenanceAdmissionPolicy {
            cpu_ns_budget: 100,
            ..MaintenanceAdmissionPolicy::default()
        });
        let decisions = admission
            .request_inline_batch(&[InlineAdmissionRequest {
                table_id: 42,
                definition_id: 11,
                provider: SearchIndexKind::FullText,
                flush_mode: FlushSearchMode::InlineRequired,
                estimated_cost: MaintenanceCost {
                    cpu_ns: 101,
                    ..Default::default()
                },
                row_count: 10,
                hnsw_inline: None,
            }])
            .unwrap();

        assert!(matches!(
            decisions.as_slice(),
            [AdmissionDecision::Wait {
                reason: AdmissionWaitReason::CpuBudget,
                ..
            }]
        ));
    }

    #[test]
    fn inline_search_admission_release_returns_scheduler_budget() {
        let admission = InlineSearchAdmission::with_policy(MaintenanceAdmissionPolicy {
            cpu_ns_budget: 100,
            fulltext_concurrency: 1,
            ..MaintenanceAdmissionPolicy::default()
        });
        let request = InlineAdmissionRequest {
            table_id: 42,
            definition_id: 11,
            provider: SearchIndexKind::FullText,
            flush_mode: FlushSearchMode::InlineRequired,
            estimated_cost: MaintenanceCost {
                cpu_ns: 100,
                ..Default::default()
            },
            row_count: 10,
            hnsw_inline: None,
        };

        let first = admission
            .request_inline_batch(std::slice::from_ref(&request))
            .unwrap();
        let grant_id = match first.as_slice() {
            [AdmissionDecision::Proceed(grant)] => grant.grant_id,
            other => panic!("expected proceed, got {other:?}"),
        };
        let held = admission
            .request_inline_batch(std::slice::from_ref(&request))
            .unwrap();
        assert!(matches!(
            held.as_slice(),
            [AdmissionDecision::Wait {
                reason: AdmissionWaitReason::CpuBudget | AdmissionWaitReason::ProviderConcurrency,
                ..
            }]
        ));

        admission.release(grant_id);
        let released = admission.request_inline_batch(&[request]).unwrap();
        assert!(matches!(
            released.as_slice(),
            [AdmissionDecision::Proceed(_)]
        ));
    }

    #[test]
    fn inline_search_admission_rejects_hnsw_over_inline_threshold() {
        let admission = InlineSearchAdmission::with_policy(MaintenanceAdmissionPolicy {
            memory_peak_bytes_budget: u64::MAX,
            cpu_ns_budget: u64::MAX,
            ..MaintenanceAdmissionPolicy::default()
        });
        let decisions = admission
            .request_inline_batch(&[InlineAdmissionRequest {
                table_id: 42,
                definition_id: 12,
                provider: SearchIndexKind::Hnsw,
                flush_mode: FlushSearchMode::InlineIfAdmitted,
                estimated_cost: MaintenanceCost {
                    memory_peak_bytes: 16,
                    ..Default::default()
                },
                row_count: 10,
                hnsw_inline: Some(HnswInlineBuildEstimate {
                    vector_count: 10,
                    dimension: 8,
                    estimated_graph_memory_bytes: 1024,
                    threshold: HnswInlineThreshold {
                        max_vector_count: 10,
                        max_graph_memory_bytes: 512,
                        max_dimension: 8,
                    },
                }),
            }])
            .unwrap();

        assert!(matches!(
            decisions.as_slice(),
            [AdmissionDecision::Reject {
                reason: AdmissionRejectReason::InlineThresholdExceeded,
            }]
        ));
    }

    #[test]
    fn hnsw_maintenance_request_groups_tail_rowset_refs() {
        let provider_config = crate::search::HnswProviderConfig {
            version: crate::search::HNSW_PROVIDER_CONFIG_VERSION,
            dimension: 16,
            distance: crate::index::hnsw::DistanceMetric::Euclidean,
            m: 8,
            ef_construct: 64,
            ef_search: 100,
            plain_scan_threshold: 10_000,
            filtered_plain_scan_threshold: 0,
            build_seed: crate::search::DEFAULT_HNSW_BUILD_SEED,
            inline_threshold: crate::search::HnswInlineConfig {
                enabled: true,
                max_vector_count: 4_096,
                max_graph_memory_bytes: 64 * 1024 * 1024,
                max_dimension: 1_536,
            },
        }
        .validated()
        .unwrap()
        .to_value()
        .unwrap();
        let definition = SearchIndexDefinition {
            definition_id: 9,
            table_id: 42,
            name: "vec_hnsw".to_string(),
            kind: SearchIndexKind::Hnsw,
            column_ids: vec![1],
            expression: None,
            provider_config,
            freshness_policy: SearchFreshnessPolicy::default_for_kind(SearchIndexKind::Hnsw),
            config_fingerprint: 77,
        };
        let tail_window = vec![
            TailPendingEntry {
                entry_id: TailEntryId(1),
                rowset_id: 10,
                segment_ids: vec![0],
                mutation: TailMutationKind::Append,
                row_count: 4,
                byte_count: 400,
                row_image_ref: None,
            },
            TailPendingEntry {
                entry_id: TailEntryId(2),
                rowset_id: 10,
                segment_ids: vec![1, 0],
                mutation: TailMutationKind::Replace,
                row_count: 2,
                byte_count: 200,
                row_image_ref: None,
            },
            TailPendingEntry {
                entry_id: TailEntryId(3),
                rowset_id: 11,
                segment_ids: vec![0],
                mutation: TailMutationKind::Delete,
                row_count: 1,
                byte_count: 100,
                row_image_ref: None,
            },
        ];

        let request = HnswMaintenanceRequest::new(
            &definition,
            &definition.hnsw_provider_config().unwrap(),
            17,
            tail_window.clone(),
            MaintenancePriority::Elevated,
        )
        .expect("hnsw request");

        assert_eq!(request.definition_id, 9);
        assert_eq!(request.generation_id, 17);
        assert_eq!(request.tail_window, tail_window);
        assert_eq!(request.dimension, 16);
        assert_eq!(request.freshness_priority, MaintenancePriority::Elevated);
        assert_eq!(
            request.rowset_refs,
            vec![HnswMaintenanceRowsetRef {
                rowset_id: 10,
                segment_ids: vec![0, 1],
                row_count: 6,
                byte_count: 600,
            }]
        );
        assert!(request.estimated_graph_memory_bytes > 0);
    }

    fn admission_request(
        definition_id: u64,
        provider: SearchIndexKind,
        priority: MaintenancePriority,
        backlog_tier: CatchUpBacklogTier,
        estimate: CostEstimate,
    ) -> MaintenanceAdmissionRequest {
        MaintenanceAdmissionRequest {
            definition_id,
            action: SearchMaintenanceAction::CatchUp,
            fairness_key: MaintenanceFairnessKey {
                database_id: 0,
                table_id: 42,
                provider,
            },
            priority,
            backlog_tier,
            estimate,
        }
    }
}
