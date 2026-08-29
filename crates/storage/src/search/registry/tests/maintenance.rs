// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Catch-up, compaction, repack, and durable maintenance invariants.

use super::*;

#[test]
fn fulltext_catch_up_publishes_cover_tail_delta() {
    let _metrics_guard = crate::metrics::storage_metrics_test_guard();
    storage_metrics().reset_for_tests();
    let root = TempDir::new().unwrap();
    let table = create_table_with_root(root.path(), &[LogicalType::Varchar]);
    table
        .append(&test_chunk_from_vectors(vec![test_string_vector(&[
            "late indexed graph",
        ])]))
        .unwrap();

    let provider_config = json!({"version": 1, "config": "simple"});
    let definition = SearchIndexDefinition {
        definition_id: 44,
        table_id: table.tablet_id(),
        name: "docs_fts".to_string(),
        kind: SearchIndexKind::FullText,
        column_ids: vec![0],
        expression: Some("to_tsvector('simple', col_0)".to_string()),
        freshness_policy: SearchFreshnessPolicy::default_for_kind(SearchIndexKind::FullText),
        config_fingerprint: SearchIndexDefinition::compute_config_fingerprint(
            SearchIndexKind::FullText,
            &[0],
            Some("to_tsvector('simple', col_0)"),
            &provider_config,
        ),
        provider_config,
    };
    table.register_search_definition(definition).unwrap();
    {
        let current = table.search_registry().view.load();
        let manifest = current
            .definitions
            .get(&44)
            .and_then(|state| state.manifest.as_ref())
            .expect("manifest before catch up");
        assert_eq!(manifest.tail_pending_entries.len(), 1);
        assert_eq!(manifest.tail_pending_entries[0].entry_id, TailEntryId(1));
        assert!(matches!(
            manifest.root.coverage,
            CoverageState::TailPending {
                pending_rowsets: 1,
                pending_segments: 1,
                pending_rows: 1,
                ..
            }
        ));
        assert!(manifest.root.recent_delta_files.is_empty());
        assert_eq!(manifest.next_tail_entry_id(), TailEntryId(2));
    }

    let touched = table.search_registry().catch_up_definition(44).unwrap();
    assert_eq!(touched, 1);

    let delta_entries = load_manifest_delta_entries(&table, 44);
    assert!(delta_entries.iter().any(|entry| matches!(
        entry,
        ManifestDeltaEntry::AddArtifact(artifact)
            if artifact.kind == SearchIndexKind::FullText
                && singleton_artifact_segment(artifact) == (ArtifactSegmentRef {
                    rowset_id: 1,
                    segment_id: 0,
                })
                && matches!(artifact.location, ArtifactLocation::SidecarArtifactFile { .. })
    )));
    assert!(delta_entries
        .iter()
        .any(|entry| matches!(entry, ManifestDeltaEntry::CoverTail(TailEntryId(1)))));
    assert!(delta_entries.iter().any(|entry| matches!(
        entry,
        ManifestDeltaEntry::StatsDelta(SearchStatsDelta::FullText(delta))
            if delta.stats.total_docs > 0
    )));

    let current = table.search_registry().view.load();
    let manifest = current
        .definitions
        .get(&44)
        .and_then(|state| state.manifest.as_ref())
        .expect("manifest after catch up");
    assert!(manifest.tail_pending_entries.is_empty());
    assert!(manifest.root.coverage.is_complete());
    assert_eq!(manifest.next_tail_entry_id(), TailEntryId(2));
    assert_eq!(manifest.artifacts.artifacts.len(), 1);
    assert!(matches!(
        manifest.artifacts.artifacts[0].location,
        ArtifactLocation::SidecarArtifactFile { .. }
    ));

    let query = FullTextIndex::new_default().parse_query("graph").unwrap();
    let opened = table
        .open_fulltext_filter_cursor(
            0,
            &query,
            "simple",
            None,
            table.max_version(),
            &crate::search::SearchReadOptions::ungoverned(),
        )
        .expect("query catch-up sidecar fulltext artifact");
    let chunks =
        drain_search_cursor(&table, opened, &[0], false, 1).expect("materialize sidecar query");
    let mut docs = Vec::new();
    for chunk in chunks {
        let text_col = chunk.column(0).expect("text projection");
        for row in 0..chunk.size() {
            docs.push(text_col.get_string(row).expect("text value").to_string());
        }
    }
    assert_eq!(docs, vec!["late indexed graph"]);

    let metrics = storage_metrics().snapshot();
    let sidecar_build = metrics
        .search_sidecar_build_by_key
        .iter()
        .find(|series| {
            series.key
                == crate::metrics::SearchSidecarBuildMetricKey {
                    definition_id: 44,
                    provider: SearchIndexKind::FullText,
                }
        })
        .expect("fulltext sidecar build metric");
    assert!(sidecar_build.counters.rows_total >= 1);
    assert!(sidecar_build.counters.read_bytes_total > 0);
    assert!(sidecar_build.counters.write_bytes_total > 0);
    assert!(sidecar_build.counters.artifact_bytes_total > 0);
    assert!(
        sidecar_build
            .counters
            .latency_us_buckets
            .iter()
            .sum::<u64>()
            > 0
    );
}

#[test]
fn sidecar_catch_up_publish_failure_cleans_package_and_delta_candidate() {
    let root = TempDir::new().unwrap();
    let table = create_table_with_root(root.path(), &[LogicalType::Varchar]);
    table
        .append(&test_chunk_from_vectors(vec![test_string_vector(&[
            "publish failure cleanup",
        ])]))
        .unwrap();

    let provider_config = json!({"version": 1, "config": "simple"});
    let definition = SearchIndexDefinition {
        definition_id: 144,
        table_id: table.tablet_id(),
        name: "docs_fts_cleanup".to_string(),
        kind: SearchIndexKind::FullText,
        column_ids: vec![0],
        expression: Some("to_tsvector('simple', col_0)".to_string()),
        freshness_policy: SearchFreshnessPolicy::default_for_kind(SearchIndexKind::FullText),
        config_fingerprint: SearchIndexDefinition::compute_config_fingerprint(
            SearchIndexKind::FullText,
            &[0],
            Some("to_tsvector('simple', col_0)"),
            &provider_config,
        ),
        provider_config,
    };
    table.register_search_definition(definition).unwrap();

    let current = table.search_registry().view.load();
    let state = current.definitions.get(&144).unwrap();
    let failing_root_version = state
        .manifest
        .as_ref()
        .unwrap()
        .root
        .root_version
        .checked_add(1)
        .unwrap();
    let root_path = table
        .search_registry()
        .manifests
        .generation_dir(144, 1)
        .join(format!(
            "manifest_root_g1_v{}_f{}.json",
            failing_root_version, state.definition.config_fingerprint
        ));
    drop(current);
    std::fs::create_dir(&root_path).unwrap();

    let store = SidecarArtifactStore::new(table.tablet().data_dir().clone());
    let package_path = store.package_path(SidecarArtifactStore::default_shard_file_id(144, 1));
    let delta_path = table
        .search_registry()
        .manifests
        .generation_dir(144, 1)
        .join(format!("delta_g1_v{failing_root_version}_0.json"));

    let err = table
        .search_registry()
        .catch_up_definition(144)
        .expect_err("root path directory must make manifest root publish fail");
    assert!(
        err.to_string()
            .contains("immutable search manifest fragment"),
        "{err}"
    );
    assert!(
        !package_path.exists(),
        "sidecar package finalized before manifest root failure must be removed"
    );
    assert!(
        !delta_path.exists(),
        "manifest delta candidate must be removed when root publish fails"
    );
}

#[test]
fn missing_sidecar_artifact_degrades_recovered_generation_to_tail_pending() {
    let root = TempDir::new().unwrap();
    let table = create_table_with_root(root.path(), &[LogicalType::Varchar]);
    table
        .append(&test_chunk_from_vectors(vec![test_string_vector(&[
            "recover missing sidecar",
        ])]))
        .unwrap();

    let provider_config = json!({"version": 1, "config": "simple"});
    let definition = SearchIndexDefinition {
        definition_id: 46,
        table_id: table.tablet_id(),
        name: "docs_fts_required".to_string(),
        kind: SearchIndexKind::FullText,
        column_ids: vec![0],
        expression: Some("to_tsvector('simple', col_0)".to_string()),
        freshness_policy: SearchFreshnessPolicy::Required,
        config_fingerprint: SearchIndexDefinition::compute_config_fingerprint(
            SearchIndexKind::FullText,
            &[0],
            Some("to_tsvector('simple', col_0)"),
            &provider_config,
        ),
        provider_config,
    };
    table
        .register_search_definition(definition.clone())
        .unwrap();
    assert_eq!(table.search_registry().catch_up_definition(46).unwrap(), 1);

    let sidecar_package = {
        let current = table.search_registry().view.load();
        let manifest = current
            .definitions
            .get(&46)
            .and_then(|state| state.manifest.as_ref())
            .expect("complete manifest before corruption");
        assert!(manifest.root.coverage.is_complete());
        assert!(manifest.tail_pending_entries.is_empty());
        let artifact = manifest
            .artifacts
            .artifacts
            .iter()
            .find(|artifact| {
                matches!(
                    artifact.location,
                    ArtifactLocation::SidecarArtifactFile { .. }
                )
            })
            .expect("sidecar artifact");
        let ArtifactLocation::SidecarArtifactFile { file_id, .. } = &artifact.location else {
            unreachable!("matched sidecar artifact");
        };
        SidecarArtifactStore::new(table.tablet().data_dir().clone()).package_path(*file_id)
    };
    assert!(sidecar_package.exists());
    std::fs::remove_file(&sidecar_package).unwrap();

    let descriptor = table.to_descriptor().expect("descriptor");
    drop(table);
    let reopened = reopen_table_with_root(root.path(), &[LogicalType::Varchar], &descriptor);
    reopened
        .register_search_definition(definition)
        .expect("reload required definition with missing sidecar");

    let current = reopened.search_registry().view.load();
    let state = current.definitions.get(&46).expect("recovered definition");
    let manifest = state.manifest.as_ref().expect("recovered manifest");
    assert!(
        manifest.artifacts.artifacts.is_empty(),
        "missing sidecar artifact must not remain in active artifact set"
    );
    assert_eq!(manifest.tail_pending_entries.len(), 1);
    assert!(matches!(
        state
            .generation
            .as_ref()
            .expect("recovered generation")
            .coverage,
        CoverageState::TailPending {
            pending_rowsets: 1,
            pending_segments: 1,
            pending_rows: 1,
            exact_tail_merge: true,
        }
    ));
    let capability = state.capability.as_ref().expect("recovered capability");
    assert_eq!(
        capability.capability_state(),
        SearchCapabilityState::NotQueryable {
            reason: SearchNotQueryableReason::FreshnessRequired
        }
    );
}

#[test]
fn orphan_sidecar_package_is_not_recovered_as_artifact() {
    let root = TempDir::new().unwrap();
    let table = create_table_with_root(root.path(), &[LogicalType::Varchar]);

    let provider_config = json!({"version": 1, "config": "simple"});
    let definition = SearchIndexDefinition {
        definition_id: 47,
        table_id: table.tablet_id(),
        name: "empty_fts".to_string(),
        kind: SearchIndexKind::FullText,
        column_ids: vec![0],
        expression: Some("to_tsvector('simple', col_0)".to_string()),
        freshness_policy: SearchFreshnessPolicy::Required,
        config_fingerprint: SearchIndexDefinition::compute_config_fingerprint(
            SearchIndexKind::FullText,
            &[0],
            Some("to_tsvector('simple', col_0)"),
            &provider_config,
        ),
        provider_config,
    };
    table
        .register_search_definition(definition.clone())
        .unwrap();

    let store = SidecarArtifactStore::new(table.tablet().data_dir().clone());
    let orphan_file_id = ArtifactFileId {
        definition_id: 47,
        generation_id: 1,
        package_index: 19,
    };
    let mut orphan = store.create_package_writer(orphan_file_id).unwrap();
    orphan
        .append_artifact(b"not referenced by manifest")
        .unwrap();
    let orphan_path = orphan.finalize().unwrap();
    assert!(orphan_path.exists());

    let descriptor = table.to_descriptor().expect("descriptor");
    drop(table);
    let reopened = reopen_table_with_root(root.path(), &[LogicalType::Varchar], &descriptor);
    reopened
        .register_search_definition(definition)
        .expect("reload definition with orphan sidecar package");

    let current = reopened.search_registry().view.load();
    let state = current.definitions.get(&47).expect("recovered definition");
    let manifest = state.manifest.as_ref().expect("recovered manifest");
    assert!(
        manifest.artifacts.artifacts.is_empty(),
        "orphan sidecar package must not become queryability evidence"
    );
    assert!(manifest.tail_pending_entries.is_empty());
    assert!(
        orphan_path.exists(),
        "manifest load must not delete orphan sidecar packages outside explicit GC"
    );
}

#[test]
fn sparse_catch_up_publishes_sidecar_artifact_and_covers_tail() {
    let _metrics_guard = crate::metrics::storage_metrics_test_guard();
    storage_metrics().reset_for_tests();
    let root = TempDir::new().unwrap();
    let table = create_table_with_root(root.path(), &[LogicalType::Blob]);
    table
        .append(&test_chunk_from_vectors(vec![test_sparse_blob_vector(&[
            SparseVector::new(vec![1, 3], vec![1.0, 0.5]).unwrap(),
        ])]))
        .unwrap();

    let provider_config = json!({"version": 1, "physical_encoding": "binary-v1" });
    let definition = SearchIndexDefinition {
        definition_id: 45,
        table_id: table.tablet_id(),
        name: "docs_sparse".to_string(),
        kind: SearchIndexKind::Sparse,
        column_ids: vec![0],
        expression: None,
        freshness_policy: SearchFreshnessPolicy::default_for_kind(SearchIndexKind::Sparse),
        config_fingerprint: SearchIndexDefinition::compute_config_fingerprint(
            SearchIndexKind::Sparse,
            &[0],
            None,
            &provider_config,
        ),
        provider_config,
    };
    table.register_search_definition(definition).unwrap();

    let touched = table.search_registry().catch_up_definition(45).unwrap();
    assert_eq!(touched, 1);

    let delta_entries = load_manifest_delta_entries(&table, 45);
    assert!(delta_entries.iter().any(|entry| matches!(
        entry,
        ManifestDeltaEntry::AddArtifact(artifact)
            if artifact.kind == SearchIndexKind::Sparse
                && singleton_artifact_segment(artifact).rowset_id == 1
                && matches!(artifact.location, ArtifactLocation::SidecarArtifactFile { .. })
    )));
    assert!(delta_entries
        .iter()
        .any(|entry| matches!(entry, ManifestDeltaEntry::CoverTail(TailEntryId(1)))));
    assert!(delta_entries.iter().any(|entry| matches!(
        entry,
        ManifestDeltaEntry::StatsDelta(SearchStatsDelta::Sparse(delta))
            if delta.row_count == 1 && delta.nnz == 2
    )));

    let current = table.search_registry().view.load();
    let manifest = current
        .definitions
        .get(&45)
        .and_then(|state| state.manifest.as_ref())
        .expect("manifest after sparse catch up");
    assert!(manifest.tail_pending_entries.is_empty());
    assert!(manifest.root.coverage.is_complete());
    assert_eq!(manifest.artifacts.artifacts.len(), 1);
    assert!(matches!(
        manifest.artifacts.artifacts[0].location,
        ArtifactLocation::SidecarArtifactFile { .. }
    ));
    let provider_stats = manifest
        .root
        .generation_stats
        .sparse_provider_stats()
        .expect("sparse provider stats");
    assert_eq!(manifest.root.generation_stats.indexed_rows, 1);
    assert_eq!(provider_stats.row_count, 1);
    assert_eq!(provider_stats.nnz, 2);

    let metrics = storage_metrics().snapshot();
    let sidecar_build = metrics
        .search_sidecar_build_by_key
        .iter()
        .find(|series| {
            series.key
                == crate::metrics::SearchSidecarBuildMetricKey {
                    definition_id: 45,
                    provider: SearchIndexKind::Sparse,
                }
        })
        .expect("sparse sidecar build metric");
    assert!(sidecar_build.counters.rows_total >= 1);
    assert!(sidecar_build.counters.read_bytes_total > 0);
    assert!(sidecar_build.counters.write_bytes_total > 0);
    assert!(sidecar_build.counters.artifact_bytes_total > 0);
    assert!(
        sidecar_build
            .counters
            .latency_us_buckets
            .iter()
            .sum::<u64>()
            > 0
    );
}

#[test]
fn run_maintenance_pass_repacks_fragmented_sidecar_packages() {
    let root = TempDir::new().unwrap();
    let table = create_table_with_root(root.path(), &[LogicalType::Varchar]);
    table
        .append(&test_chunk_from_vectors(vec![test_string_vector(&[
            "first graph document",
        ])]))
        .unwrap();

    let provider_config = json!({"version": 1, "config": "simple"});
    let definition = SearchIndexDefinition {
        definition_id: 47,
        table_id: table.tablet_id(),
        name: "docs_fts".to_string(),
        kind: SearchIndexKind::FullText,
        column_ids: vec![0],
        expression: Some("to_tsvector('simple', col_0)".to_string()),
        freshness_policy: SearchFreshnessPolicy::Opportunistic,
        config_fingerprint: SearchIndexDefinition::compute_config_fingerprint(
            SearchIndexKind::FullText,
            &[0],
            Some("to_tsvector('simple', col_0)"),
            &provider_config,
        ),
        provider_config,
    };
    table.register_search_definition(definition).unwrap();
    assert_eq!(table.search_registry().catch_up_definition(47).unwrap(), 1);

    table
        .append(&test_chunk_from_vectors(vec![test_string_vector(&[
            "second graph document",
        ])]))
        .unwrap();
    assert_eq!(table.search_registry().catch_up_definition(47).unwrap(), 1);

    let store = SidecarArtifactStore::new(table.tablet().data_dir().clone());
    let package_0 = store.package_path(ArtifactFileId {
        definition_id: 47,
        generation_id: 1,
        package_index: 0,
    });
    let package_1 = store.package_path(ArtifactFileId {
        definition_id: 47,
        generation_id: 1,
        package_index: 1,
    });
    assert!(package_0.exists());
    assert!(package_1.exists());

    let before = table.search_registry().view.load();
    let before_manifest = before
        .definitions
        .get(&47)
        .and_then(|state| state.manifest.as_ref())
        .expect("manifest before sidecar repack");
    assert!(crate::search::maintenance::sidecar_repack_needed(
        SearchIndexKind::FullText,
        before_manifest
    ));
    drop(before);

    let report = table.search_registry().run_maintenance_pass().unwrap();
    let definition_report = report
        .definitions
        .iter()
        .find(|definition| definition.definition_id == 47)
        .expect("sidecar repack report");
    assert_eq!(
        definition_report.action,
        SearchMaintenanceAction::RepackSidecar
    );
    assert!(definition_report.sidecar_repack_requested);
    assert!(report.sidecar_repack_requested);

    let after = table.search_registry().view.load();
    let after_manifest = after
        .definitions
        .get(&47)
        .and_then(|state| state.manifest.as_ref())
        .expect("manifest after sidecar repack");
    assert_eq!(after_manifest.artifacts.artifacts.len(), 2);
    assert!(!crate::search::maintenance::sidecar_repack_needed(
        SearchIndexKind::FullText,
        after_manifest
    ));
    let package_ids = after_manifest
        .artifacts
        .artifacts
        .iter()
        .map(|artifact| match artifact.location {
            ArtifactLocation::SidecarArtifactFile { file_id, .. } => file_id,
            ArtifactLocation::Inline { .. } => panic!("expected sidecar artifact after repack"),
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(package_ids.len(), 1);
    let repacked_file = package_ids.iter().next().copied().unwrap();
    assert_eq!(repacked_file.package_index, 2);
    assert!(store.package_path(repacked_file).exists());
    assert!(!package_0.exists());
    assert!(!package_1.exists());
}

#[test]
fn maintenance_report_carries_cost_benefit_for_tail_catch_up() {
    let root = TempDir::new().unwrap();
    let table = create_table_with_root(root.path(), &[LogicalType::Varchar]);
    table
        .append(&test_chunk_from_vectors(vec![test_string_vector(&[
            "late indexed graph",
        ])]))
        .unwrap();

    let provider_config = json!({"version": 1, "config": "simple"});
    let definition = SearchIndexDefinition {
        definition_id: 46,
        table_id: table.tablet_id(),
        name: "docs_fts".to_string(),
        kind: SearchIndexKind::FullText,
        column_ids: vec![0],
        expression: Some("to_tsvector('simple', col_0)".to_string()),
        freshness_policy: SearchFreshnessPolicy::default_for_kind(SearchIndexKind::FullText),
        config_fingerprint: SearchIndexDefinition::compute_config_fingerprint(
            SearchIndexKind::FullText,
            &[0],
            Some("to_tsvector('simple', col_0)"),
            &provider_config,
        ),
        provider_config,
    };
    table.register_search_definition(definition).unwrap();

    let report = table.search_registry().run_maintenance_pass().unwrap();
    assert_eq!(report.definitions_updated, 1);
    assert_eq!(report.catch_up_rowsets, 1);
    let definition_report = report
        .definitions
        .iter()
        .find(|definition| definition.definition_id == 46)
        .expect("definition maintenance report");

    assert_eq!(definition_report.action, SearchMaintenanceAction::CatchUp);
    assert_eq!(definition_report.tail_pending_rowsets, 1);
    assert_eq!(definition_report.tail_pending_rows, 1);
    assert_eq!(
        definition_report
            .estimate
            .benefit
            .expected_tail_rows_drained,
        1
    );
    assert!(definition_report.estimate.cost.cpu_ns > 0);
    assert!(definition_report.estimate.cost.publish_bytes > 0);
}

#[test]
fn run_maintenance_pass_reports_and_compacts_manifest_delta_window() {
    let root = TempDir::new().unwrap();
    let table = create_table_with_root(root.path(), &[LogicalType::Varchar]);
    let provider_config = json!({"version": 1, "config": "simple"});
    let definition = SearchIndexDefinition {
        definition_id: 47,
        table_id: table.tablet_id(),
        name: "docs_fts".to_string(),
        kind: SearchIndexKind::FullText,
        column_ids: vec![0],
        expression: Some("to_tsvector('simple', col_0)".to_string()),
        freshness_policy: SearchFreshnessPolicy::default_for_kind(SearchIndexKind::FullText),
        config_fingerprint: SearchIndexDefinition::compute_config_fingerprint(
            SearchIndexKind::FullText,
            &[0],
            Some("to_tsvector('simple', col_0)"),
            &provider_config,
        ),
        provider_config,
    };
    table.register_search_definition(definition).unwrap();
    table
        .append(&test_chunk_from_vectors(vec![test_string_vector(&[
            "manifest compact",
        ])]))
        .unwrap();

    let synthetic_head = {
        let current = table.search_registry().view.load();
        let state = current.definitions.get(&47).expect("definition state");
        let manifest = state.manifest.as_ref().expect("manifest");
        let mut root = manifest.root.clone();
        root.root_version = root.root_version.saturating_add(1);
        let store = &table.search_registry().manifests;
        for ordinal in 0..=DELTA_COUNT_SOFT_LIMIT {
            let delta_ref = store
                .write_delta(
                    47,
                    root.generation_id,
                    root.root_version,
                    ordinal,
                    &ManifestDelta::default(),
                )
                .expect("write synthetic delta");
            root.recent_delta_files.push(delta_ref);
        }
        root.recompute_checksum().unwrap();
        store.write_root(47, &root).unwrap();
        store.head_for_root(&root)
    };
    table
        .tablet()
        .apply_search_generation_publish(&TabletMutation::PublishSearchGeneration {
            publication: SearchGenerationPublication::AdvanceInstalled,
            generation_ref: table
                .search_registry()
                .manifests
                .generation_ref(47, synthetic_head.generation_id)
                .unwrap(),
            head: synthetic_head.clone(),
        })
        .unwrap();
    {
        let loaded = table
            .search_registry()
            .manifests
            .load_manifest_for_head(&synthetic_head)
            .expect("load synthetic over-soft manifest")
            .expect("manifest exists");
        table
            .search_registry()
            .update_registry_view(|view| {
                let state = view
                    .definitions
                    .get(&47)
                    .expect("definition state")
                    .clone()
                    .with_manifest(loaded);
                view.definitions.insert(47, state);
                Ok((true, ()))
            })
            .unwrap();
    }

    let report = table.search_registry().run_maintenance_pass().unwrap();
    let definition_report = report
        .definitions
        .iter()
        .find(|definition| definition.definition_id == 47)
        .expect("definition report");
    assert!(report.manifest_delta_compaction_requested);
    assert!(definition_report.manifest_delta_compaction_requested);
    assert_eq!(
        definition_report.action,
        SearchMaintenanceAction::CompactManifestDelta
    );

    let current = table.search_registry().view.load();
    let manifest = current
        .definitions
        .get(&47)
        .and_then(|state| state.manifest.as_ref())
        .expect("compacted manifest");
    assert!(manifest.root.recent_delta_files.is_empty());
    let compacted = table.search_registry().compact_manifest_deltas().unwrap();
    assert_eq!(compacted, 0);
}

#[test]
fn rowset_publish_compacts_delta_window_without_leaking_transient_delta() {
    let root = TempDir::new().unwrap();
    let table = create_table_with_root(root.path(), &[LogicalType::Varchar]);
    let provider_config = json!({"version": 1, "config": "simple"});
    let definition = SearchIndexDefinition {
        definition_id: 48,
        table_id: table.tablet_id(),
        name: "docs_fts".to_string(),
        kind: SearchIndexKind::FullText,
        column_ids: vec![0],
        expression: Some("to_tsvector('simple', col_0)".to_string()),
        freshness_policy: SearchFreshnessPolicy::default_for_kind(SearchIndexKind::FullText),
        config_fingerprint: SearchIndexDefinition::compute_config_fingerprint(
            SearchIndexKind::FullText,
            &[0],
            Some("to_tsvector('simple', col_0)"),
            &provider_config,
        ),
        provider_config,
    };
    table.register_search_definition(definition).unwrap();
    table
        .append(&test_chunk_from_vectors(vec![test_string_vector(&[
            "manifest base",
        ])]))
        .unwrap();

    let synthetic_head = {
        let current = table.search_registry().view.load();
        let state = current.definitions.get(&48).expect("definition state");
        let manifest = state.manifest.as_ref().expect("manifest");
        let mut root = manifest.root.clone();
        root.root_version = root.root_version.checked_add(1).unwrap();
        let store = &table.search_registry().manifests;
        for ordinal in 0..=DELTA_COUNT_SOFT_LIMIT {
            let delta_ref = store
                .write_delta(
                    48,
                    root.generation_id,
                    root.root_version,
                    ordinal,
                    &ManifestDelta::default(),
                )
                .expect("write synthetic delta");
            root.recent_delta_files.push(delta_ref);
        }
        root.recompute_checksum().unwrap();
        store.write_root(48, &root).unwrap();
        store.head_for_root(&root)
    };
    table
        .tablet()
        .apply_search_generation_publish(&TabletMutation::PublishSearchGeneration {
            publication: SearchGenerationPublication::AdvanceInstalled,
            generation_ref: table
                .search_registry()
                .manifests
                .generation_ref(48, synthetic_head.generation_id)
                .unwrap(),
            head: synthetic_head.clone(),
        })
        .unwrap();
    {
        let loaded = table
            .search_registry()
            .manifests
            .load_manifest_for_head(&synthetic_head)
            .unwrap()
            .unwrap();
        table
            .search_registry()
            .update_registry_view(|view| {
                let state = view
                    .definitions
                    .get(&48)
                    .unwrap()
                    .clone()
                    .with_manifest(loaded);
                view.definitions.insert(48, state);
                Ok((true, ()))
            })
            .unwrap();
    }

    table
        .append(&test_chunk_from_vectors(vec![test_string_vector(&[
            "manifest delta compaction",
        ])]))
        .expect("base-table append must survive threshold-triggered compaction");

    let durable_head = table.tablet().search_generation_head(48).unwrap();
    assert_eq!(
        durable_head.root_version,
        synthetic_head.root_version + 1,
        "one rowset publish must consume exactly one manifest revision"
    );
    let loaded = table
        .search_registry()
        .manifests
        .load_manifest_for_head(&durable_head)
        .unwrap()
        .unwrap();
    assert!(loaded.root.recent_delta_files.is_empty());
    assert_eq!(loaded.root.shard_files.len(), 1);

    let transient_prefix = format!(
        "delta_g{}_v{}_",
        durable_head.generation_id, durable_head.root_version
    );
    let generation_dir = table
        .search_registry()
        .manifests
        .generation_dir(48, durable_head.generation_id);
    assert!(
        fs::read_dir(generation_dir)
            .unwrap()
            .flatten()
            .all(|entry| !entry
                .file_name()
                .to_string_lossy()
                .starts_with(&transient_prefix)),
        "delta absorbed into the committed shard must not leak"
    );
}

#[test]
fn fulltext_rowset_replacement_publishes_remove_artifact_delta() {
    let root = TempDir::new().unwrap();
    let table = create_table_with_root(root.path(), &[LogicalType::Varchar]);
    let provider_config = json!({"version": 1, "config": "simple"});
    let definition = SearchIndexDefinition {
        definition_id: 45,
        table_id: table.tablet_id(),
        name: "docs_fts".to_string(),
        kind: SearchIndexKind::FullText,
        column_ids: vec![0],
        expression: Some("to_tsvector('simple', col_0)".to_string()),
        freshness_policy: SearchFreshnessPolicy::default_for_kind(SearchIndexKind::FullText),
        config_fingerprint: SearchIndexDefinition::compute_config_fingerprint(
            SearchIndexKind::FullText,
            &[0],
            Some("to_tsvector('simple', col_0)"),
            &provider_config,
        ),
        provider_config,
    };
    table.register_search_definition(definition).unwrap();
    table
        .append(&test_chunk_from_vectors(vec![test_string_vector(&[
            "vector one",
        ])]))
        .unwrap();
    table
        .append(&test_chunk_from_vectors(vec![test_string_vector(&[
            "vector two",
        ])]))
        .unwrap();

    {
        let current = table.search_registry().view.load();
        let manifest = current
            .definitions
            .get(&45)
            .and_then(|state| state.manifest.as_ref())
            .expect("manifest before compaction");
        let rowset_ids = manifest
            .artifacts
            .artifacts
            .iter()
            .flat_map(|artifact| {
                artifact
                    .coverage
                    .segments()
                    .iter()
                    .map(|span| span.segment.rowset_id)
            })
            .collect::<BTreeSet<_>>();
        assert_eq!(rowset_ids, BTreeSet::from([1, 2]));
    }

    assert!(
        table.optimize_compact().unwrap(),
        "expected compaction output"
    );
    table.search_registry().refresh_all_definitions();

    let delta_entries = load_manifest_delta_entries(&table, 45);
    assert!(delta_entries.iter().any(|entry| matches!(
        entry,
        ManifestDeltaEntry::RemoveArtifact(coverage)
            if coverage.contains_segment(ArtifactSegmentRef {
                rowset_id: 1,
                segment_id: 0,
            })
    )));
    assert!(delta_entries.iter().any(|entry| matches!(
        entry,
        ManifestDeltaEntry::RemoveArtifact(coverage)
            if coverage.contains_segment(ArtifactSegmentRef {
                rowset_id: 2,
                segment_id: 0,
            })
    )));
    assert!(delta_entries.iter().any(|entry| matches!(
        entry,
        ManifestDeltaEntry::AddArtifact(artifact)
            if artifact.kind == SearchIndexKind::FullText
                && singleton_artifact_segment(artifact).rowset_id != 1
                && singleton_artifact_segment(artifact).rowset_id != 2
    )));

    let current = table.search_registry().view.load();
    let manifest = current
        .definitions
        .get(&45)
        .and_then(|state| state.manifest.as_ref())
        .expect("manifest after compaction");
    assert!(manifest.artifacts.artifacts.iter().all(|artifact| {
        !artifact.coverage.contains_rowset(1) && !artifact.coverage.contains_rowset(2)
    }));
    assert_eq!(manifest.root.generation_stats.indexed_rows, 2);
    assert_eq!(
        manifest
            .root
            .generation_stats
            .fulltext_provider_stats()
            .expect("fulltext stats")
            .total_docs,
        2
    );

    let query = FullTextIndex::new_default().parse_query("vector").unwrap();
    let opened = table
        .open_fulltext_filter_cursor(
            0,
            &query,
            "simple",
            None,
            table.max_version(),
            &crate::search::SearchReadOptions::ungoverned(),
        )
        .unwrap();
    let mut cursor = opened.cursor;
    let snapshot = opened.snapshot;
    let batch = SearchBatchConfig {
        row_limit: 1024,
        preferred_bytes: 1 << 20,
    };
    let mut budget = ResourceBudget::standalone(64 * 1024 * 1024, 1024, 4);
    let mut row_count = 0usize;
    loop {
        match cursor.next_batch(&batch, &mut budget).unwrap() {
            SearchBatchState::Ready(batch) if batch.is_empty() => continue,
            SearchBatchState::Ready(batch) => {
                row_count += table
                    .materialize_search_batch(
                        &snapshot,
                        batch,
                        &[0],
                        false,
                        Arc::new(default_allocator()),
                    )
                    .unwrap()
                    .size();
            }
            SearchBatchState::Exhausted => break,
        }
    }
    assert_eq!(row_count, 2);
}

#[test]
fn compaction_output_absorbs_tail_pending_fulltext_rowsets() {
    let root = TempDir::new().unwrap();
    let table = create_table_with_root(root.path(), &[LogicalType::Varchar]);
    table
        .append(&test_chunk_from_vectors(vec![test_string_vector(&[
            "tail one",
        ])]))
        .unwrap();
    table
        .append(&test_chunk_from_vectors(vec![test_string_vector(&[
            "tail two",
        ])]))
        .unwrap();

    let provider_config = json!({"version": 1, "config": "simple"});
    let definition = SearchIndexDefinition {
        definition_id: 91,
        table_id: table.tablet_id(),
        name: "docs_fts".to_string(),
        kind: SearchIndexKind::FullText,
        column_ids: vec![0],
        expression: Some("to_tsvector('simple', col_0)".to_string()),
        freshness_policy: SearchFreshnessPolicy::BoundedLag {
            max_tail_rows: 100,
            max_lag_millis: 0,
        },
        config_fingerprint: SearchIndexDefinition::compute_config_fingerprint(
            SearchIndexKind::FullText,
            &[0],
            Some("to_tsvector('simple', col_0)"),
            &provider_config,
        ),
        provider_config,
    };
    table.register_search_definition(definition).unwrap();

    {
        let current = table.search_registry().view.load();
        let manifest = current
            .definitions
            .get(&91)
            .and_then(|state| state.manifest.as_ref())
            .expect("tail-pending manifest");
        assert!(manifest.artifacts.artifacts.is_empty());
        assert_eq!(manifest.tail_pending_entries.len(), 2);
    }

    assert!(
        table.optimize_compact().unwrap(),
        "expected compaction output"
    );
    table.search_registry().refresh_all_definitions();

    let current = table.search_registry().view.load();
    let manifest = current
        .definitions
        .get(&91)
        .and_then(|state| state.manifest.as_ref())
        .expect("manifest after compaction absorption");
    assert!(
        manifest.tail_pending_entries.is_empty(),
        "compaction output should cover input tail entries instead of leaving catch-up work"
    );
    assert!(manifest.root.coverage.is_complete());
    assert_eq!(manifest.artifacts.artifacts.len(), 1);
    let output_artifact = &manifest.artifacts.artifacts[0];
    assert_eq!(output_artifact.kind, SearchIndexKind::FullText);
    assert_ne!(singleton_artifact_segment(output_artifact).rowset_id, 1);
    assert_ne!(singleton_artifact_segment(output_artifact).rowset_id, 2);
    assert!(matches!(
        output_artifact.location,
        ArtifactLocation::Inline { .. }
    ));

    let delta_entries = load_manifest_delta_entries(&table, 91);
    assert!(delta_entries
        .iter()
        .any(|entry| matches!(entry, ManifestDeltaEntry::CoverTail(TailEntryId(1)))));
    assert!(delta_entries
        .iter()
        .any(|entry| matches!(entry, ManifestDeltaEntry::CoverTail(TailEntryId(2)))));
    assert!(delta_entries.iter().any(|entry| matches!(
        entry,
        ManifestDeltaEntry::AddArtifact(artifact)
            if artifact.kind == SearchIndexKind::FullText
                && artifact.coverage == output_artifact.coverage
    )));
    assert_eq!(manifest.root.generation_stats.indexed_rows, 2);
    assert_eq!(
        manifest
            .root
            .generation_stats
            .fulltext_provider_stats()
            .expect("fulltext stats")
            .total_docs,
        2
    );
    assert_eq!(table.search_registry().catch_up_definition(91).unwrap(), 0);
}

#[test]
fn compaction_output_absorbs_tail_pending_sparse_rowsets() {
    let root = TempDir::new().unwrap();
    let table = create_table_with_root(root.path(), &[LogicalType::Blob]);
    table
        .append(&test_chunk_from_vectors(vec![test_sparse_blob_vector(&[
            SparseVector::new(vec![1, 3], vec![1.0, 0.5]).unwrap(),
        ])]))
        .unwrap();
    table
        .append(&test_chunk_from_vectors(vec![test_sparse_blob_vector(&[
            SparseVector::new(vec![1, 2], vec![0.7, 0.2]).unwrap(),
        ])]))
        .unwrap();

    let provider_config = json!({"version": 1, "physical_encoding": "binary-v1" });
    let definition = SearchIndexDefinition {
        definition_id: 94,
        table_id: table.tablet_id(),
        name: "docs_sparse".to_string(),
        kind: SearchIndexKind::Sparse,
        column_ids: vec![0],
        expression: None,
        freshness_policy: SearchFreshnessPolicy::BoundedLag {
            max_tail_rows: 100,
            max_lag_millis: 0,
        },
        config_fingerprint: SearchIndexDefinition::compute_config_fingerprint(
            SearchIndexKind::Sparse,
            &[0],
            None,
            &provider_config,
        ),
        provider_config,
    };
    table.register_search_definition(definition).unwrap();

    {
        let current = table.search_registry().view.load();
        let manifest = current
            .definitions
            .get(&94)
            .and_then(|state| state.manifest.as_ref())
            .expect("tail-pending manifest");
        assert!(manifest.artifacts.artifacts.is_empty());
        assert_eq!(manifest.tail_pending_entries.len(), 2);
    }

    assert!(
        table.optimize_compact().unwrap(),
        "expected compaction output"
    );
    table.search_registry().refresh_all_definitions();

    let current = table.search_registry().view.load();
    let manifest = current
        .definitions
        .get(&94)
        .and_then(|state| state.manifest.as_ref())
        .expect("manifest after compaction absorption");
    assert!(manifest.tail_pending_entries.is_empty());
    assert!(manifest.root.coverage.is_complete());
    assert_eq!(manifest.artifacts.artifacts.len(), 1);
    let output_artifact = &manifest.artifacts.artifacts[0];
    assert_eq!(output_artifact.kind, SearchIndexKind::Sparse);
    assert_ne!(singleton_artifact_segment(output_artifact).rowset_id, 1);
    assert_ne!(singleton_artifact_segment(output_artifact).rowset_id, 2);
    assert!(matches!(
        output_artifact.location,
        ArtifactLocation::Inline { .. }
    ));

    let delta_entries = load_manifest_delta_entries(&table, 94);
    assert!(delta_entries
        .iter()
        .any(|entry| matches!(entry, ManifestDeltaEntry::CoverTail(TailEntryId(1)))));
    assert!(delta_entries
        .iter()
        .any(|entry| matches!(entry, ManifestDeltaEntry::CoverTail(TailEntryId(2)))));
    assert!(delta_entries.iter().any(|entry| matches!(
        entry,
        ManifestDeltaEntry::AddArtifact(artifact)
            if artifact.kind == SearchIndexKind::Sparse
                && artifact.coverage == output_artifact.coverage
    )));
    assert_eq!(manifest.root.generation_stats.indexed_rows, 2);
    let provider_stats = manifest
        .root
        .generation_stats
        .sparse_provider_stats()
        .expect("sparse stats");
    assert_eq!(provider_stats.row_count, 2);
    assert_eq!(provider_stats.nnz, 4);
    assert_eq!(table.search_registry().catch_up_definition(94).unwrap(), 0);
}

#[test]
fn shared_fulltext_payload_definitions_replay_compaction_output_once() {
    let root = TempDir::new().unwrap();
    let table = create_table_with_root(root.path(), &[LogicalType::Varchar]);
    let provider_config = json!({"version": 1, "config": "simple"});

    for definition_id in [92, 93] {
        let definition = SearchIndexDefinition {
            definition_id,
            table_id: table.tablet_id(),
            name: format!("docs_fts_{definition_id}"),
            kind: SearchIndexKind::FullText,
            column_ids: vec![0],
            expression: Some("to_tsvector('simple', col_0)".to_string()),
            freshness_policy: SearchFreshnessPolicy::Required,
            config_fingerprint: SearchIndexDefinition::compute_config_fingerprint(
                SearchIndexKind::FullText,
                &[0],
                Some("to_tsvector('simple', col_0)"),
                &provider_config,
            ),
            provider_config: provider_config.clone(),
        };
        table.register_search_definition(definition).unwrap();
    }

    table
        .append(&test_chunk_from_vectors(vec![test_string_vector(&[
            "shared payload one",
        ])]))
        .unwrap();
    table
        .append(&test_chunk_from_vectors(vec![test_string_vector(&[
            "shared payload two",
        ])]))
        .unwrap();

    {
        let current = table.search_registry().view.load();
        for definition_id in [92, 93] {
            let manifest = current
                .definitions
                .get(&definition_id)
                .and_then(|state| state.manifest.as_ref())
                .expect("manifest before compaction");
            assert_eq!(manifest.artifacts.artifacts.len(), 2);
            assert!(manifest.tail_pending_entries.is_empty());
        }
    }

    assert!(
        table.optimize_compact().unwrap(),
        "expected compaction output"
    );
    table.search_registry().refresh_all_definitions();

    let current = table.search_registry().view.load();
    let mut output_locations = Vec::new();
    for definition_id in [92, 93] {
        let manifest = current
            .definitions
            .get(&definition_id)
            .and_then(|state| state.manifest.as_ref())
            .expect("manifest after compaction");
        assert!(manifest.root.coverage.is_complete());
        assert!(manifest.tail_pending_entries.is_empty());
        assert_eq!(manifest.artifacts.artifacts.len(), 1);
        let artifact = &manifest.artifacts.artifacts[0];
        assert_ne!(singleton_artifact_segment(artifact).rowset_id, 1);
        assert_ne!(singleton_artifact_segment(artifact).rowset_id, 2);
        let ArtifactLocation::Inline { page } = artifact.location else {
            panic!("expected inline compaction output artifact");
        };
        assert_ne!(page.checksum, 0);
        output_locations.push((
            page.rowset_id,
            page.segment_id,
            page.column_id,
            page.page_offset,
            page.page_len,
        ));
    }
    assert_eq!(
        output_locations[0], output_locations[1],
        "shared physical payload definitions should replay the same compaction output page"
    );
}

#[test]
fn hnsw_tail_pending_delta_records_upsert_tail_entry() {
    let root = TempDir::new().unwrap();
    let table = create_table_without_default_indexes(
        root.path(),
        &[LogicalType::Array(Box::new(LogicalType::Float), 1)],
    );
    let mut provider =
        crate::search::HnswProviderConfig::from_value(&test_hnsw_provider_config(1, 16, 64, 0))
            .unwrap();
    provider.maintenance = crate::search::HnswMaintenancePolicy {
        target_vector_bytes: 16_384 * 4,
        max_pending_vector_bytes: 16_384 * 4,
        compaction_fanout: 8,
        compaction_min_idle_ms: crate::search::DEFAULT_HNSW_MAINTENANCE_COMPACTION_MIN_IDLE_MS,
    };
    let provider_config = provider.to_value().unwrap();
    let definition = SearchIndexDefinition {
        definition_id: 88,
        table_id: table.tablet_id(),
        name: "vec_hnsw".to_string(),
        kind: SearchIndexKind::Hnsw,
        column_ids: vec![0],
        expression: None,
        freshness_policy: SearchFreshnessPolicy::default_for_kind(SearchIndexKind::Hnsw),
        config_fingerprint: SearchIndexDefinition::compute_config_fingerprint(
            SearchIndexKind::Hnsw,
            &[0],
            None,
            &provider_config,
        ),
        provider_config,
    };

    table.register_search_definition(definition).unwrap();
    let oversized = table
        .tablet()
        .acquire_search_rowset_publish_admission(32_768, 8 * 1024 * 1024)
        .unwrap()
        .expect("an isolated large transaction must not be rejected by a derived index");
    {
        let admission = table.search_registry().ingest_admission.lock().unwrap();
        assert_eq!(admission.reserved_rows, 32_768);
    }
    drop(oversized);
    table
        .append(&test_chunk_from_vectors(vec![test_embedding_vector(
            &[vec![10.0]],
            1,
        )]))
        .unwrap();
    table
        .append(&test_chunk_from_vectors(vec![test_embedding_vector(
            &[vec![20.0]],
            1,
        )]))
        .unwrap();

    {
        let admission = table.search_registry().ingest_admission.lock().unwrap();
        let blocker = table
            .search_registry()
            .hnsw_ingest_admission_blocker(16_383, &admission)
            .expect("committed rowsets must consume freshness capacity before refresh");
        assert_eq!(blocker.pending_rows, 2);
        assert_eq!(blocker.row_limit, 16_384);
    }

    // Deferred HNSW rowsets are not serialized into the foreground
    // transaction. The background reconciliation owns the coalesced
    // manifest revision.
    table.search_registry().refresh_all_definitions();

    let delta_entries = load_manifest_delta_entries(&table, 88);
    assert!(delta_entries.iter().any(|entry| matches!(
        entry,
        ManifestDeltaEntry::UpsertTail(tail)
            if tail.entry_id == TailEntryId(1)
                && tail.rowset_id == 1
                && tail.segment_ids == vec![0]
                && tail.mutation == TailMutationKind::Append
                && tail.row_count == 1
    )));
    assert!(delta_entries.iter().any(|entry| matches!(
        entry,
        ManifestDeltaEntry::UpsertTail(tail)
            if tail.entry_id == TailEntryId(2)
                && tail.rowset_id == 2
                && tail.segment_ids == vec![0]
                && tail.mutation == TailMutationKind::Append
                && tail.row_count == 1
    )));
    assert!(!delta_entries
        .iter()
        .any(|entry| matches!(entry, ManifestDeltaEntry::AddArtifact(_))));

    let current = table.search_registry().view.load();
    let manifest = current
        .definitions
        .get(&88)
        .and_then(|state| state.manifest.as_ref())
        .expect("manifest");
    assert!(matches!(
        manifest.root.coverage,
        CoverageState::TailPending {
            pending_rowsets: 2,
            pending_segments: 2,
            pending_rows: 2,
            ..
        }
    ));
    let admission_state = table.search_registry().ingest_admission.lock().unwrap();
    assert!(admission_state.unmanifested_hnsw.is_empty());
    let blocker = table
        .search_registry()
        .hnsw_ingest_admission_blocker(16_383, &admission_state)
        .expect("row high watermark");
    assert_eq!(blocker.definition_id, 88);
    assert_eq!(blocker.pending_rows, 2);
    assert_eq!(blocker.row_limit, 16_384);
    drop(admission_state);

    let admission = table
        .tablet()
        .acquire_search_rowset_publish_admission(8_000, 1_234)
        .unwrap()
        .expect("search registry is bound");
    let reserved = table.search_registry().ingest_admission.lock().unwrap();
    let (reserved_rows, reserved_bytes) = (reserved.reserved_rows, reserved.reserved_bytes);
    assert_eq!((reserved_rows, reserved_bytes), (8_000, 1_234));
    assert!(
        table
            .search_registry()
            .hnsw_ingest_admission_blocker(8_383, &reserved)
            .is_some(),
        "prepared transactions must reserve capacity instead of racing on a check-only watermark"
    );
    drop(reserved);
    drop(admission);
    let released = table.search_registry().ingest_admission.lock().unwrap();
    assert_eq!(released.reserved_rows, 0);
    assert_eq!(released.reserved_bytes, 0);
}

#[test]
fn hnsw_generation_compaction_coalesces_graphs_without_rewriting_rowsets() {
    let root = TempDir::new().unwrap();
    let table = create_table_without_default_indexes(
        root.path(),
        &[LogicalType::Array(Box::new(LogicalType::Float), 1)],
    );
    table.bind_search_task_scheduler(Some(Arc::new(TaskScheduler::new())));
    let mut provider =
        crate::search::HnswProviderConfig::from_value(&test_hnsw_provider_config(1, 16, 64, 0))
            .unwrap();
    // Keep two level-zero artifacts live so this test exercises the
    // independently admitted generation-compaction path. Freshness
    // catch-up only appends immutable level-zero artifacts.
    provider.maintenance.compaction_fanout = 3;
    let provider_config = provider.to_value().unwrap();
    let definition = SearchIndexDefinition {
        definition_id: 188,
        table_id: table.tablet_id(),
        name: "coalesced_vec_hnsw".to_string(),
        kind: SearchIndexKind::Hnsw,
        column_ids: vec![0],
        expression: None,
        freshness_policy: SearchFreshnessPolicy::default_for_kind(SearchIndexKind::Hnsw),
        config_fingerprint: SearchIndexDefinition::compute_config_fingerprint(
            SearchIndexKind::Hnsw,
            &[0],
            None,
            &provider_config,
        ),
        provider_config,
    };
    table.register_search_definition(definition).unwrap();

    for value in [10.0, 20.0] {
        table
            .append(&test_chunk_from_vectors(vec![test_embedding_vector(
                &[vec![value]],
                1,
            )]))
            .unwrap();
        table.search_registry().refresh_all_definitions();
        assert_eq!(table.search_registry().catch_up_definition(188).unwrap(), 1);
    }
    let rowsets_before = table
        .tablet()
        .capture_consistent_rowsets(table.max_version())
        .unwrap()
        .into_iter()
        .map(|rowset| rowset.rowset_id())
        .collect::<Vec<_>>();
    assert_eq!(
        table.search_registry().generation_artifact_count(188),
        Some(2)
    );
    {
        let current = table.search_registry().view.load();
        let manifest = current
            .definitions
            .get(&188)
            .and_then(|state| state.manifest.as_ref())
            .unwrap();
        assert!(
                !crate::search::maintenance::sidecar_repack_needed(
                    SearchIndexKind::Hnsw,
                    manifest
                ),
                "HNSW graph fan-out must be reduced by graph compaction, not by copying live graph bytes into another package"
            );
    }

    let foreground_admission = table
        .tablet()
        .acquire_search_rowset_publish_admission(1, 64)
        .unwrap()
        .expect("HNSW registry must govern foreground publication");
    assert!(
        !table
            .search_registry()
            .compact_hnsw_generation(188, true)
            .unwrap(),
        "optional generation compaction must yield to admitted ingest"
    );
    drop(foreground_admission);

    assert!(
        crate::compaction::plan::CompactionPlanner::plan(table.tablet().as_ref())
            .unwrap()
            .is_some()
    );
    assert!(
            table
                .search_registry()
                .compact_hnsw_generation(188, true)
                .unwrap(),
            "derived graph compaction must not be permanently suppressed merely because a physical rewrite is eligible"
        );
    let rowsets_after = table
        .tablet()
        .capture_consistent_rowsets(table.max_version())
        .unwrap()
        .into_iter()
        .map(|rowset| rowset.rowset_id())
        .collect::<Vec<_>>();
    assert_eq!(rowsets_after, rowsets_before);
    assert_eq!(
        table.search_registry().generation_artifact_count(188),
        Some(1)
    );
    let current = table.search_registry().view.load();
    let manifest = current
        .definitions
        .get(&188)
        .and_then(|state| state.manifest.as_ref())
        .unwrap();
    assert_eq!(manifest.artifacts.artifacts[0].coverage.segments().len(), 2);
    assert!(manifest.tail_pending_entries.is_empty());
}

#[test]
fn hnsw_tail_pending_maintenance_report_carries_provider_request() {
    let root = TempDir::new().unwrap();
    let table = create_table_without_default_indexes(
        root.path(),
        &[LogicalType::Array(Box::new(LogicalType::Float), 4)],
    );
    let provider_config = test_hnsw_provider_config(4, 16, 64, 0);
    let definition = SearchIndexDefinition {
        definition_id: 90,
        table_id: table.tablet_id(),
        name: "vec_hnsw".to_string(),
        kind: SearchIndexKind::Hnsw,
        column_ids: vec![0],
        expression: None,
        freshness_policy: SearchFreshnessPolicy::default_for_kind(SearchIndexKind::Hnsw),
        config_fingerprint: SearchIndexDefinition::compute_config_fingerprint(
            SearchIndexKind::Hnsw,
            &[0],
            None,
            &provider_config,
        ),
        provider_config,
    };

    table.register_search_definition(definition).unwrap();
    table
        .append(&test_chunk_from_vectors(vec![test_embedding_vector(
            &[vec![10.0, 0.0, 0.0, 0.0]],
            4,
        )]))
        .unwrap();

    let report = table.search_registry().run_maintenance_pass().unwrap();
    let definition_report = report
        .definitions
        .iter()
        .find(|definition| definition.definition_id == 90)
        .expect("hnsw definition report");
    let request = definition_report
        .provider_request
        .as_ref()
        .and_then(ProviderMaintenanceRequest::as_hnsw)
        .expect("hnsw provider maintenance request");
    assert_eq!(request.definition_id, 90);
    assert_eq!(request.dimension, 4);
    assert_eq!(request.tail_window.len(), 1);
    assert_eq!(request.rowset_refs.len(), 1);
    assert_eq!(request.rowset_refs[0].row_count, 1);
    assert_eq!(
        request.freshness_priority, definition_report.priority,
        "request should carry scheduler freshness priority"
    );
    assert!(request.estimated_build_peak_memory_bytes > 0);
}

#[test]
fn hnsw_tail_pending_maintenance_publishes_and_queries_one_multi_segment_partition() {
    let root = TempDir::new().unwrap();
    let table = create_table_without_default_indexes(
        root.path(),
        &[LogicalType::Array(Box::new(LogicalType::Float), 2)],
    );
    table.bind_search_task_scheduler(Some(Arc::new(TaskScheduler::new())));
    let provider_config = test_hnsw_provider_config(2, 8, 32, 0);
    let definition = SearchIndexDefinition {
        definition_id: 94,
        table_id: table.tablet_id(),
        name: "vec_hnsw".to_string(),
        kind: SearchIndexKind::Hnsw,
        column_ids: vec![0],
        expression: None,
        freshness_policy: SearchFreshnessPolicy::default_for_kind(SearchIndexKind::Hnsw),
        config_fingerprint: SearchIndexDefinition::compute_config_fingerprint(
            SearchIndexKind::Hnsw,
            &[0],
            None,
            &provider_config,
        ),
        provider_config,
    };
    table.register_search_definition(definition).unwrap();
    table
        .append(&test_chunk_from_vectors(vec![test_embedding_vector(
            &[vec![1.0, 0.0], vec![0.0, 1.0]],
            2,
        )]))
        .unwrap();
    table
        .append(&test_chunk_from_vectors(vec![test_embedding_vector(
            &[vec![2.0, 0.0], vec![0.0, 2.0]],
            2,
        )]))
        .unwrap();

    {
        let current = table.search_registry().view.load();
        let manifest = current
            .definitions
            .get(&94)
            .and_then(|state| state.manifest.as_ref())
            .expect("tail-pending manifest");
        assert!(manifest.artifacts.artifacts.is_empty());
        assert!(manifest.tail_pending_entries.is_empty());
    }

    // The durable generation head deliberately remains unchanged until
    // maintenance. Query correctness comes from the read snapshot's
    // version-derived exact tail, not from a foreground manifest rewrite.
    let opened = table
        .open_vector_search_cursor(
            0,
            &[1.0, 0.0],
            DistanceMetric::Euclidean,
            4,
            SearchParams::default(),
            None,
            table.max_version(),
            &crate::search::SearchReadOptions::ungoverned(),
        )
        .unwrap();
    let mut tail_cursor = opened.cursor;
    let batch = SearchBatchConfig {
        row_limit: 16,
        preferred_bytes: 1 << 20,
    };
    let mut budget = ResourceBudget::standalone(64 * 1024 * 1024, 16, 2);
    let mut tail_rows = 0;
    while let SearchBatchState::Ready(batch) = tail_cursor.next_batch(&batch, &mut budget).unwrap()
    {
        tail_rows += batch.rows.len();
    }
    assert_eq!(tail_rows, 4);

    let report = table.search_registry().run_maintenance_pass().unwrap();
    assert_eq!(report.definitions_updated, 0);
    assert_eq!(report.catch_up_rowsets, 0);
    let definition_report = report
        .definitions
        .iter()
        .find(|definition| definition.definition_id == 94)
        .expect("hnsw definition report");
    assert_eq!(definition_report.action, SearchMaintenanceAction::Skip);

    // Sub-target HNSW rows remain the exact L0 during background
    // maintenance. An explicit materialization request is the policy
    // boundary that seals the final partial digit into one graph.
    table.search_registry().materialize_definition(94).unwrap();

    let current = table.search_registry().view.load();
    let manifest = current
        .definitions
        .get(&94)
        .and_then(|state| state.manifest.as_ref())
        .expect("manifest after hnsw catch-up");
    assert!(manifest.tail_pending_entries.is_empty());
    assert!(manifest.root.coverage.is_complete());
    assert_eq!(manifest.artifacts.artifacts.len(), 1);
    let artifact = &manifest.artifacts.artifacts[0];
    assert_eq!(artifact.kind, SearchIndexKind::Hnsw);
    assert!(matches!(
        artifact.location,
        ArtifactLocation::SidecarArtifactFile { .. }
    ));
    assert_eq!(artifact.coverage.segments().len(), 2);
    assert_eq!(artifact.coverage.row_count(), 4);
    let provider_stats = manifest
        .root
        .generation_stats
        .hnsw_provider_stats()
        .expect("hnsw provider stats");
    assert_eq!(provider_stats.vector_count, 4);
    assert_eq!(provider_stats.dimension, 2);

    let delta_entries = load_manifest_delta_entries(&table, 94);
    assert!(delta_entries
        .iter()
        .any(|entry| matches!(entry, ManifestDeltaEntry::CoverTail(TailEntryId(1)))));
    assert!(delta_entries
        .iter()
        .any(|entry| matches!(entry, ManifestDeltaEntry::CoverTail(TailEntryId(2)))));
    assert!(delta_entries.iter().any(|entry| matches!(
        entry,
        ManifestDeltaEntry::AddArtifact(artifact) if artifact.kind == SearchIndexKind::Hnsw
    )));
    assert!(delta_entries.iter().any(|entry| matches!(
        entry,
        ManifestDeltaEntry::StatsDelta(SearchStatsDelta::Hnsw(delta))
            if delta.vector_count == 4 && delta.dimension == 2
    )));

    let rowsets = table
        .tablet()
        .capture_consistent_rowsets(table.max_version())
        .unwrap();
    for rowset in &rowsets {
        assert!(
            rowset.segments()[0].hnsw_index(0).is_none(),
            "HNSW TailOnly catch-up must not patch published segment footers"
        );
    }

    let opened = table
        .open_vector_search_cursor(
            0,
            &[1.0, 0.0],
            DistanceMetric::Euclidean,
            4,
            SearchParams::default(),
            None,
            table.max_version(),
            &crate::search::SearchReadOptions::ungoverned(),
        )
        .unwrap();
    let mut cursor = opened.cursor;
    let batch = SearchBatchConfig {
        row_limit: 16,
        preferred_bytes: 1 << 20,
    };
    let mut budget = ResourceBudget::standalone(64 * 1024 * 1024, 16, 2);
    let mut returned = BTreeSet::new();
    while let SearchBatchState::Ready(batch) = cursor.next_batch(&batch, &mut budget).unwrap() {
        returned.extend(
            batch
                .rows
                .into_iter()
                .map(|row| (row.rowset_id, row.segment_id, row.row_offset.get())),
        );
    }
    assert_eq!(returned.len(), 4);
    assert_eq!(
        returned
            .iter()
            .map(|(rowset_id, _, _)| *rowset_id)
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([1, 2])
    );
}

#[test]
fn hnsw_full_snapshot_stores_tail_in_shard_not_root() {
    let root = TempDir::new().unwrap();
    let table = create_table_without_default_indexes(
        root.path(),
        &[LogicalType::Array(Box::new(LogicalType::Float), 1)],
    );
    table
        .append(&test_chunk_from_vectors(vec![test_embedding_vector(
            &[vec![10.0]],
            1,
        )]))
        .unwrap();

    let provider_config = test_hnsw_provider_config(1, 16, 64, 0);
    let definition = SearchIndexDefinition {
        definition_id: 89,
        table_id: table.tablet_id(),
        name: "vec_hnsw".to_string(),
        kind: SearchIndexKind::Hnsw,
        column_ids: vec![0],
        expression: None,
        freshness_policy: SearchFreshnessPolicy::default_for_kind(SearchIndexKind::Hnsw),
        config_fingerprint: SearchIndexDefinition::compute_config_fingerprint(
            SearchIndexKind::Hnsw,
            &[0],
            None,
            &provider_config,
        ),
        provider_config,
    };

    table.register_search_definition(definition).unwrap();
    {
        let current = table.search_registry().view.load();
        let manifest = current
            .definitions
            .get(&89)
            .and_then(|state| state.manifest.as_ref())
            .expect("manifest");
        assert_eq!(manifest.tail_pending_entries.len(), 1);
        assert_eq!(manifest.tail_pending_entries[0].entry_id, TailEntryId(1));
        assert_eq!(manifest.next_tail_entry_id(), TailEntryId(2));
        assert!(manifest.root.recent_delta_files.is_empty());
        assert_eq!(manifest.root.shard_files.len(), 1);

        let definition_dir = table
            .search_registry()
            .manifests
            .generation_dir(89, manifest.root.generation_id);
        let root_bytes = std::fs::read(&manifest.root_path).expect("read root");
        let root_json: serde_json::Value =
            serde_json::from_slice(&root_bytes).expect("decode root json");
        assert!(
            root_json.get("tail_pending_entries").is_none(),
            "manifest root must stay small and must not duplicate compacted tail entries"
        );

        let shard_bytes =
            std::fs::read(definition_dir.join(&manifest.root.shard_files[0].file_name))
                .expect("read shard");
        let shard: ManifestShard = serde_json::from_slice(&shard_bytes).expect("decode shard");
        assert_eq!(shard.tail_pending_entries.len(), 1);
        assert_eq!(shard.tail_pending_entries[0].entry_id, TailEntryId(1));
        assert_eq!(shard.tail_pending_entries[0].rowset_id, 1);
    }

    table
        .append(&test_chunk_from_vectors(vec![test_embedding_vector(
            &[vec![20.0]],
            1,
        )]))
        .unwrap();

    table.search_registry().refresh_all_definitions();

    let delta_entries = load_manifest_delta_entries(&table, 89);
    assert!(delta_entries.iter().any(|entry| matches!(
        entry,
        ManifestDeltaEntry::UpsertTail(tail)
            if tail.entry_id == TailEntryId(2)
                && tail.rowset_id == 2
                && tail.segment_ids == vec![0]
                && tail.mutation == TailMutationKind::Append
                && tail.row_count == 1
    )));
    let current = table.search_registry().view.load();
    let manifest = current
        .definitions
        .get(&89)
        .and_then(|state| state.manifest.as_ref())
        .expect("manifest after delta");
    assert_eq!(manifest.tail_pending_entries.len(), 2);
    assert_eq!(manifest.next_tail_entry_id(), TailEntryId(3));

    let root_bytes = std::fs::read(&manifest.root_path).expect("read root");
    let root_json: serde_json::Value =
        serde_json::from_slice(&root_bytes).expect("decode root json");
    assert!(root_json.get("tail_pending_entries").is_none());
}
