// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Definition installation, replacement, recovery, and retirement invariants.

use super::*;

#[test]
fn concurrent_view_publications_preserve_distinct_definitions() {
    const DEFINITION_COUNT: u64 = 16;

    let root = TempDir::new().unwrap();
    let table = create_table_without_default_indexes(root.path(), &[LogicalType::Varchar]);
    let registry = Arc::clone(&table.search_registry);
    registry
        .update_registry_view(|view| {
            for definition_id in 1..=DEFINITION_COUNT {
                view.definitions.insert(
                    definition_id,
                    SearchDefinitionState::new(
                        fulltext_test_definition(definition_id),
                        SearchDefinitionOrigin::catalog(definition_id),
                    )
                    .unwrap(),
                );
            }
            Ok((true, ()))
        })
        .unwrap();
    let barrier = Arc::new(std::sync::Barrier::new(DEFINITION_COUNT as usize));

    std::thread::scope(|scope| {
        for definition_id in 1..=DEFINITION_COUNT {
            let registry = Arc::clone(&registry);
            let barrier = Arc::clone(&barrier);
            scope.spawn(move || {
                let expected = registry
                    .view
                    .load()
                    .definitions
                    .get(&definition_id)
                    .cloned()
                    .unwrap();
                let mut next = expected.clone();
                next.next_build_epoch = next.next_build_epoch.saturating_add(1);
                barrier.wait();
                registry.publish_definition_state(&expected, next).unwrap();
            });
        }
    });

    let view = registry.view.load();
    assert_eq!(view.definitions.len(), DEFINITION_COUNT as usize);
    assert_eq!(view.version, DEFINITION_COUNT + 1);
    for definition_id in 1..=DEFINITION_COUNT {
        assert_eq!(
            view.definitions
                .get(&definition_id)
                .map(|state| state.next_build_epoch),
            Some(2)
        );
    }
}

#[test]
fn stale_definition_publication_cannot_resurrect_removed_definition() {
    let root = TempDir::new().unwrap();
    let table = create_table_without_default_indexes(root.path(), &[LogicalType::Varchar]);
    let registry = table.search_registry();
    let definition = fulltext_test_definition(101);
    let state =
        SearchDefinitionState::new(definition, SearchDefinitionOrigin::catalog(101)).unwrap();
    registry
        .update_registry_view(|view| {
            view.definitions.insert(101, state);
            Ok((true, ()))
        })
        .unwrap();

    let stale = registry.view.load().definitions.get(&101).cloned().unwrap();
    registry
        .update_registry_view(|view| {
            view.definitions.remove(&101);
            Ok((true, ()))
        })
        .unwrap();

    let mut stale_refresh = stale.clone();
    stale_refresh.next_build_epoch = stale_refresh.next_build_epoch.saturating_add(1);
    assert!(registry
        .publish_definition_state(&stale, stale_refresh)
        .is_err());
    assert!(!registry.view.load().definitions.contains_key(&101));
}

#[test]
fn active_generation_lease_delays_retired_artifact_reclamation() {
    let root = TempDir::new().unwrap();
    let table = create_table_without_default_indexes(root.path(), &[LogicalType::Varchar]);
    let registry = table.search_registry();
    let definition = fulltext_test_definition(102);
    let definition_id = definition.definition_id;
    let retired_path = root.path().join("leased-search-artifact");
    std::fs::write(&retired_path, b"leased").unwrap();
    let manifest = LoadedManifest {
        root: GenerationManifestRoot {
            definition_id,
            generation_id: 1,
            build_epoch: 1,
            build_snapshot_version: 1,
            indexed_through_ts: 1,
            config_fingerprint: definition.config_fingerprint,
            coverage: CoverageState::Complete,
            generation_stats: GenerationStats::default(),
            persisted_tail_entry_id_seed: TailEntryId(1),
            execution_modes: ExecutionModes::default(),
            maintenance_state: GenerationMaintenanceState::default(),
            root_version: 1,
            checksum: 0,
            shard_files: Vec::new(),
            recent_delta_files: Vec::new(),
        },
        root_path: root.path().join("manifest-root"),
        shard_paths: Vec::new(),
        delta_paths: Vec::new(),
        tail_entry_id_allocator: TailEntryId(1),
        publication_lease: None,
        artifacts: Arc::new(GenerationArtifactSet::default()),
        tail_pending_entries: Vec::new(),
    };
    let state = SearchDefinitionState::new(
        definition.clone(),
        SearchDefinitionOrigin::catalog(definition_id),
    )
    .unwrap()
    .with_manifest(manifest);
    let snapshot = generation_read_snapshot(definition_id, &state)
        .unwrap()
        .unwrap();
    let lease = crate::search::cursor::GenerationReadLease::from_snapshot(&snapshot);
    let manifest = state.manifest.as_ref().unwrap();
    assert!(Arc::ptr_eq(&snapshot.artifacts, &manifest.artifacts));

    registry.retire_manifest_paths(definition.kind, manifest, vec![retired_path.clone()]);
    drop(snapshot);
    drop(state);
    registry.sweep_retired();
    assert!(retired_path.exists());

    drop(lease);
    registry.sweep_retired();
    assert!(!retired_path.exists());
}

#[test]
fn artifact_replacement_stats_rebuilds_irreversible_fulltext_summary() {
    let definition = fulltext_test_definition(91);
    let removed = fulltext_test_artifact(91, 1, 4, 8, 4, 8, 10);
    let kept = fulltext_test_artifact(91, 2, 6, 18, 5, 12, 6);
    let added = fulltext_test_artifact(91, 3, 2, 4, 2, 3, 3);
    let current =
        generation_stats_from_artifacts(&definition, &[removed.clone(), kept.clone()]).unwrap();
    let materialized = vec![kept, added.clone()];

    let next = generation_stats_after_artifact_replacement(
        &definition,
        &current,
        &[removed],
        &[added],
        &materialized,
    )
    .unwrap();

    let fulltext = next.fulltext_provider_stats().expect("fulltext stats");
    assert_eq!(next.indexed_rows, 8);
    assert_eq!(next.artifact_count, 2);
    assert_eq!(fulltext.total_docs, 8);
    assert_eq!(fulltext.total_terms, 22);
    assert_eq!(fulltext.unique_terms, 7);
    assert_eq!(fulltext.total_postings, 15);
    assert_eq!(fulltext.max_posting_list_len, 6);
}

#[test]
fn invalidated_partition_preserves_still_visible_spans_as_exact_tail() {
    let mut artifact = fulltext_test_artifact(91, 1, 4, 8, 4, 8, 10);
    artifact.coverage = SearchPartitionCoverage::try_new(vec![
        ArtifactSegmentSpan {
            segment: ArtifactSegmentRef {
                rowset_id: 1,
                segment_id: 0,
            },
            row_count: 2,
        },
        ArtifactSegmentSpan {
            segment: ArtifactSegmentRef {
                rowset_id: 2,
                segment_id: 3,
            },
            row_count: 2,
        },
    ])
    .unwrap();
    artifact.location = ArtifactLocation::SidecarArtifactFile {
        file_id: ArtifactFileId {
            definition_id: 91,
            generation_id: 1,
            package_index: 0,
        },
        offset: 0,
        len: 64,
        checksum: 7,
    };
    artifact.validate().unwrap();

    let tails = surviving_partition_tail_entries(&[artifact], &BTreeSet::from([1]));
    assert_eq!(tails.len(), 1);
    assert_eq!(tails[0].rowset_id, 2);
    assert_eq!(tails[0].segment_ids, vec![3]);
    assert_eq!(tails[0].row_count, 2);
    assert_eq!(tails[0].mutation, TailMutationKind::Append);
    assert_eq!(tails[0].row_image_ref, Some(TailRowImageRef::WholeRowset));
}

#[test]
fn artifact_replacement_stats_rebuilds_irreversible_sparse_summary() {
    let definition = sparse_test_definition(92);
    let removed = sparse_test_artifact(92, 1, 4, 12, 3, 3.0);
    let kept = sparse_test_artifact(92, 2, 6, 20, 5, 4.0);
    let added = sparse_test_artifact(92, 3, 2, 8, 6, 5.0);
    let current =
        generation_stats_from_artifacts(&definition, &[removed.clone(), kept.clone()]).unwrap();
    let materialized = vec![kept, added.clone()];

    let next = generation_stats_after_artifact_replacement(
        &definition,
        &current,
        &[removed],
        &[added],
        &materialized,
    )
    .unwrap();

    let sparse = next.sparse_provider_stats().expect("sparse stats");
    assert_eq!(next.indexed_rows, 8);
    assert_eq!(next.artifact_count, 2);
    assert_eq!(sparse.row_count, 8);
    assert_eq!(sparse.nnz, 28);
    assert_eq!(sparse.posting_fanout, 28);
    assert_eq!(sparse.unique_dimensions, 11);
    assert_eq!(sparse.max_l2_norm, 5.0);
    assert!((sparse.avg_vector_nnz - 3.5).abs() < 1e-6);
}

#[test]
fn artifact_replacement_stats_rebuilds_irreversible_hnsw_summary() {
    let definition = hnsw_test_definition(93);
    let removed = hnsw_test_artifact(93, 1, 4, 2, 24);
    let kept = hnsw_test_artifact(93, 2, 6, 3, 32);
    let added = hnsw_test_artifact(93, 3, 2, 5, 48);
    let current =
        generation_stats_from_artifacts(&definition, &[removed.clone(), kept.clone()]).unwrap();
    let materialized = vec![kept, added.clone()];

    let next = generation_stats_after_artifact_replacement(
        &definition,
        &current,
        &[removed],
        &[added],
        &materialized,
    )
    .unwrap();

    let hnsw = next.hnsw_provider_stats().expect("hnsw stats");
    assert_eq!(next.indexed_rows, 8);
    assert_eq!(next.artifact_count, 2);
    assert_eq!(hnsw.vector_count, 8);
    assert_eq!(hnsw.dimension, 128);
    assert_eq!(hnsw.max_level, 5);
    assert_eq!(hnsw.max_level0_degree, 48);
    assert_eq!(hnsw.graph_memory_bytes, 8 * 256);
    assert_eq!(hnsw.vector_storage_bytes, 8 * 512);
    assert_eq!(hnsw.total_graph_links, 8 * 18);
    assert_eq!(hnsw.level0_graph_links, 8 * 12);
    assert!((hnsw.avg_level0_degree - 12.0).abs() < 1e-6);
}

#[test]
fn full_snapshot_tail_id_assignment_reuses_existing_ids_and_root_cursor() {
    let existing_tail = TailPendingEntry {
        entry_id: TailEntryId(7),
        rowset_id: 11,
        segment_ids: vec![0],
        mutation: TailMutationKind::Append,
        row_count: 10,
        byte_count: 1024,
        row_image_ref: Some(TailRowImageRef::WholeRowset),
    };
    let manifest = LoadedManifest {
        root: GenerationManifestRoot {
            definition_id: 44,
            generation_id: 1,
            build_epoch: 1,
            build_snapshot_version: 1,
            indexed_through_ts: 1,
            config_fingerprint: 99,
            coverage: CoverageState::TailPending {
                pending_rowsets: 1,
                pending_segments: 1,
                pending_rows: 10,
                exact_tail_merge: true,
            },
            generation_stats: GenerationStats::default(),
            persisted_tail_entry_id_seed: TailEntryId(10),
            execution_modes: ExecutionModes::default(),
            maintenance_state: GenerationMaintenanceState::default(),
            root_version: 1,
            checksum: 0,
            shard_files: Vec::new(),
            recent_delta_files: Vec::new(),
        },
        root_path: std::path::PathBuf::new(),
        shard_paths: Vec::new(),
        delta_paths: Vec::new(),
        tail_entry_id_allocator: TailEntryId(10),
        publication_lease: None,
        artifacts: Arc::new(GenerationArtifactSet::default()),
        tail_pending_entries: vec![existing_tail.clone()],
    };
    let mut snapshot_entries = vec![
        TailPendingEntry {
            entry_id: TailEntryId::UNASSIGNED,
            ..existing_tail
        },
        TailPendingEntry {
            entry_id: TailEntryId::UNASSIGNED,
            rowset_id: 12,
            segment_ids: vec![0],
            mutation: TailMutationKind::Append,
            row_count: 20,
            byte_count: 2048,
            row_image_ref: Some(TailRowImageRef::WholeRowset),
        },
    ];

    let next_id = assign_tail_entry_ids_for_full_snapshot(&mut snapshot_entries, Some(&manifest));

    assert_eq!(snapshot_entries[0].entry_id, TailEntryId(7));
    assert_eq!(snapshot_entries[1].entry_id, TailEntryId(10));
    assert_eq!(next_id, TailEntryId(11));
}

#[test]
fn catch_up_append_rebases_only_over_live_pending_tail() {
    let definition = hnsw_test_definition(45);
    let manifest = LoadedManifest {
        root: GenerationManifestRoot {
            definition_id: 45,
            generation_id: 1,
            build_epoch: 1,
            build_snapshot_version: 1,
            indexed_through_ts: 1,
            config_fingerprint: definition.config_fingerprint,
            coverage: CoverageState::TailPending {
                pending_rowsets: 2,
                pending_segments: 2,
                pending_rows: 30,
                exact_tail_merge: true,
            },
            generation_stats: GenerationStats::default(),
            persisted_tail_entry_id_seed: TailEntryId(3),
            execution_modes: ExecutionModes::default(),
            maintenance_state: GenerationMaintenanceState::default(),
            root_version: 2,
            checksum: 0,
            shard_files: Vec::new(),
            recent_delta_files: Vec::new(),
        },
        root_path: PathBuf::new(),
        shard_paths: Vec::new(),
        delta_paths: Vec::new(),
        tail_entry_id_allocator: TailEntryId(3),
        publication_lease: None,
        artifacts: Arc::new(GenerationArtifactSet::default()),
        tail_pending_entries: vec![
            TailPendingEntry {
                entry_id: TailEntryId(1),
                rowset_id: 11,
                segment_ids: vec![0],
                mutation: TailMutationKind::Append,
                row_count: 10,
                byte_count: 1_024,
                row_image_ref: Some(TailRowImageRef::WholeRowset),
            },
            TailPendingEntry {
                entry_id: TailEntryId(2),
                rowset_id: 12,
                segment_ids: vec![0],
                mutation: TailMutationKind::Append,
                row_count: 20,
                byte_count: 2_048,
                row_image_ref: Some(TailRowImageRef::WholeRowset),
            },
        ],
    };
    let state = SearchDefinitionState::new(definition, SearchDefinitionOrigin::catalog(45))
        .unwrap()
        .with_manifest(manifest);
    let built_prefix = hnsw_test_artifact(45, 11, 10, 1, 16);

    assert!(SearchIndexRegistry::catch_up_append_rebaseable(
        &state,
        std::slice::from_ref(&built_prefix)
    ));

    let mut compacted = state.clone();
    compacted
        .manifest
        .as_mut()
        .unwrap()
        .tail_pending_entries
        .remove(0);
    assert!(!SearchIndexRegistry::catch_up_append_rebaseable(
        &compacted,
        std::slice::from_ref(&built_prefix)
    ));

    let mut wrong_generation = built_prefix;
    wrong_generation.generation_id = 2;
    assert!(!SearchIndexRegistry::catch_up_append_rebaseable(
        &state,
        &[wrong_generation]
    ));
}

#[test]
fn registry_write_context_carries_inline_builder_set() {
    let mut view = SearchView::default();
    let fulltext_definition = SearchIndexDefinition {
        definition_id: 10,
        table_id: 20,
        name: "body_fts".to_string(),
        kind: SearchIndexKind::FullText,
        column_ids: vec![1],
        expression: None,
        provider_config: json!({"version": 1, "config": "simple"}),
        freshness_policy: SearchFreshnessPolicy::default_for_kind(SearchIndexKind::FullText),
        config_fingerprint: 100,
    };
    let sparse_definition = SearchIndexDefinition {
        definition_id: 11,
        table_id: 20,
        name: "emb_sparse".to_string(),
        kind: SearchIndexKind::Sparse,
        column_ids: vec![2],
        expression: None,
        provider_config: json!({"version": 1, "physical_encoding": "binary-v1"}),
        freshness_policy: SearchFreshnessPolicy::default_for_kind(SearchIndexKind::Sparse),
        config_fingerprint: 101,
    };

    view.definitions.insert(
        fulltext_definition.definition_id,
        SearchDefinitionState::new(
            fulltext_definition.clone(),
            SearchDefinitionOrigin::catalog(fulltext_definition.definition_id),
        )
        .unwrap(),
    );
    view.definitions.insert(
        sparse_definition.definition_id,
        SearchDefinitionState::new(
            sparse_definition.clone(),
            SearchDefinitionOrigin::catalog(sparse_definition.definition_id),
        )
        .unwrap(),
    );

    let admission: Arc<dyn SearchAdmission> = Arc::new(InlineSearchAdmission::default());
    let context = view.write_context(Some(admission)).unwrap();
    assert_eq!(context.plan.fulltext.len(), 1);
    assert_eq!(context.plan.sparse.len(), 1);
    assert_eq!(context.inline_builders.len(), 2);
    assert!(context.inline_builders.admission().is_some());
    assert!(context
        .inline_builders
        .entries()
        .iter()
        .any(|entry| entry.definition.kind == SearchIndexKind::FullText));
    assert!(context
        .inline_builders
        .entries()
        .iter()
        .any(|entry| entry.definition.kind == SearchIndexKind::Sparse));
    assert!(context
        .inline_builders
        .entries()
        .iter()
        .all(|entry| entry.generation_id == 1));
}

#[test]
fn inline_builder_set_coalesces_duplicate_fulltext_payloads_with_strict_policy() {
    let mut view = SearchView::default();
    let physical_config = json!({"version": 1, "config": "simple"});
    let opportunistic = SearchIndexDefinition {
        definition_id: 12,
        table_id: 20,
        name: "docs_fts_opportunistic".to_string(),
        kind: SearchIndexKind::FullText,
        column_ids: vec![0],
        expression: Some("to_tsvector('simple', col_0)".to_string()),
        provider_config: physical_config.clone(),
        freshness_policy: SearchFreshnessPolicy::Opportunistic,
        config_fingerprint: 201,
    };
    let required = SearchIndexDefinition {
        definition_id: 13,
        table_id: 20,
        name: "docs_fts_required".to_string(),
        kind: SearchIndexKind::FullText,
        column_ids: vec![0],
        expression: Some("to_tsvector('simple', col_0)".to_string()),
        provider_config: physical_config,
        freshness_policy: SearchFreshnessPolicy::Required,
        config_fingerprint: 202,
    };
    view.definitions.insert(
        opportunistic.definition_id,
        SearchDefinitionState::new(
            opportunistic.clone(),
            SearchDefinitionOrigin::catalog(opportunistic.definition_id),
        )
        .unwrap(),
    );
    view.definitions.insert(
        required.definition_id,
        SearchDefinitionState::new(
            required.clone(),
            SearchDefinitionOrigin::catalog(required.definition_id),
        )
        .unwrap(),
    );

    let context = view.write_context(None).unwrap();
    assert_eq!(context.plan.fulltext.len(), 1);
    assert_eq!(context.inline_builders.len(), 1);
    let entry = &context.inline_builders.entries()[0];
    assert_eq!(entry.definition.definition_id, required.definition_id);
    assert_eq!(entry.flush_mode(), FlushSearchMode::InlineRequired);
}

#[test]
fn schema_seeded_hnsw_definition_is_registered() {
    let root = TempDir::new().unwrap();
    let table = create_schema_seeded_hnsw_table(
        root.path(),
        &[LogicalType::Array(Box::new(LogicalType::Float), 4)],
        0,
    );

    assert!(table.search_registry().definition_count() >= 1);
    assert!(table
        .vector_capability(0, DistanceMetric::Euclidean)
        .is_some());
    assert!(
        table.vector_capability(0, DistanceMetric::Cosine).is_none(),
        "metric mismatch must not expose an HNSW capability"
    );
}

#[test]
fn cancelled_staged_generation_removes_workspace_and_releases_layout_lease() {
    let root = TempDir::new().unwrap();
    let table = create_table_without_default_indexes(
        root.path(),
        &[LogicalType::Array(Box::new(LogicalType::Float), 4)],
    );
    let provider_config = test_hnsw_provider_config(4, 16, 64, 4_096);
    let definition = SearchIndexDefinition {
        definition_id: 91,
        table_id: table.tablet_id(),
        name: "cancelled_vec_hnsw".to_string(),
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
    let txn_id = 77;
    let workspace = table
        .search_registry()
        .manifests
        .staged_generation_workspace(txn_id, definition.definition_id, 1);
    let checks = Arc::new(AtomicUsize::new(0));
    let stop_checks = Arc::clone(&checks);
    let result = table.search_registry().stage_definition_generation(
        definition,
        txn_id,
        SearchBuildStopCheck::new(move || stop_checks.fetch_add(1, Ordering::Relaxed) >= 3),
    );

    assert!(result.is_err());
    assert!(!workspace.exists());
    assert!(table
        .tablet()
        .try_acquire_compaction_layout_lease()
        .unwrap()
        .is_some());
}

#[test]
fn corrupt_hnsw_authentication_is_persisted_as_exact_tail_recovery() {
    use std::io::{Read, Seek, SeekFrom, Write};

    let root = TempDir::new().unwrap();
    let table = create_table_with_root(
        root.path(),
        &[LogicalType::Array(Box::new(LogicalType::Float), 4)],
    );
    let embeddings = (0..1_024)
        .map(|row| {
            let row = row as f32;
            vec![row, row + 0.25, row + 0.5, row + 0.75]
        })
        .collect::<Vec<_>>();
    table
        .append(&test_chunk_from_vectors(vec![test_embedding_vector(
            &embeddings,
            4,
        )]))
        .unwrap();

    let provider_config = test_hnsw_provider_config(4, 16, 64, 4_096);
    let definition = SearchIndexDefinition {
        definition_id: 92,
        table_id: table.tablet_id(),
        name: "published_vec_hnsw".to_string(),
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
    let scheduler = Arc::new(TaskScheduler::new());
    table
        .bind_hnsw_integrity_scheduler(Some(Arc::new(
            crate::index::hnsw::HnswIntegrityScheduler::new(Arc::clone(&scheduler)),
        )))
        .unwrap();
    let staged = table
        .stage_search_definition_generation(
            definition.clone(),
            8_001,
            SearchBuildStopCheck::new(|| false),
        )
        .unwrap();
    staged.prepare_durable_handoff().unwrap();
    table
        .apply_search_generation_publish(&staged.mutation())
        .unwrap();
    staged.mark_published().unwrap();
    let adopted_runtime =
        SearchReaderRuntime::new(SidecarArtifactStore::new(table.tablet().data_dir().clone()));
    assert_eq!(
        staged
            .adopt_prepared_readers_into(&adopted_runtime)
            .unwrap(),
        1,
        "durable publication must inherit the authenticated staged reader"
    );
    assert_eq!(
        staged
            .adopt_prepared_readers_into(&adopted_runtime)
            .unwrap(),
        0,
        "reader adoption must be idempotent"
    );
    drop(adopted_runtime);

    let head = table
        .tablet()
        .search_generation_head(definition.definition_id)
        .expect("published generation head");
    let manifest = table
        .search_registry()
        .manifests
        .load_manifest_for_head(&head)
        .unwrap()
        .expect("published generation manifest");
    let artifact = manifest
        .artifacts
        .artifacts
        .first()
        .expect("published HNSW artifact")
        .clone();
    let ArtifactLocation::SidecarArtifactFile {
        file_id,
        offset,
        len,
        ..
    } = artifact.location.clone()
    else {
        panic!("staged HNSW generation must use a sidecar artifact");
    };
    let corrupt_offset = offset + len / 2;
    let package =
        SidecarArtifactStore::new(table.tablet().data_dir().clone()).package_path(file_id);
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(package)
        .unwrap();
    file.seek(SeekFrom::Start(corrupt_offset)).unwrap();
    let mut byte = [0u8; 1];
    file.read_exact(&mut byte).unwrap();
    byte[0] ^= 0x5a;
    file.seek(SeekFrom::Start(corrupt_offset)).unwrap();
    file.write_all(&byte).unwrap();
    file.sync_all().unwrap();

    let prepared_runtime =
        SearchReaderRuntime::new(SidecarArtifactStore::new(table.tablet().data_dir().clone()));
    let provider = definition.hnsw_provider_config().unwrap();
    let visible_rowsets = table
        .tablet()
        .capture_consistent_rowsets(table.tablet().max_version())
        .unwrap();
    let prepared_error = prewarm_hnsw_generation_readers(
        &prepared_runtime,
        std::slice::from_ref(&artifact),
        &visible_rowsets,
        0,
        provider.dimension as usize,
        &provider.build_contract(),
        None,
        HnswReaderActivationPolicy::prepared_publication(
            crate::index::hnsw::HnswBuildExecutionPolicy::Foreground,
        ),
        None,
    )
    .expect_err("a private corrupt artifact must fail before publication");
    assert!(prepared_error.is(paro_common::error::codes::internal::DATA_CORRUPTED));

    table
        .register_published_search_definition(definition.clone())
        .expect("reader activation must not synchronously scan the artifact");
    assert!(
        table
            .vector_capability(0, DistanceMetric::Euclidean)
            .is_some(),
        "lazy checksum validation keeps the exact fallback queryable"
    );

    let marker = std::sync::atomic::AtomicBool::new(true);
    for _ in 0..32 {
        scheduler.execute_tasks(&marker, 1);
    }
    assert_eq!(
        table
            .search_registry()
            .recover_hnsw_integrity_failures()
            .unwrap(),
        1
    );

    let next_head = table
        .tablet()
        .search_generation_head(definition.definition_id)
        .expect("quarantine revision head");
    assert!(next_head.root_version > head.root_version);
    let next = table
        .search_registry()
        .manifests
        .load_manifest_for_head(&next_head)
        .unwrap()
        .expect("quarantine manifest");
    assert!(
        next.artifacts
            .artifacts
            .iter()
            .all(|current| current != &artifact),
        "a checksum-failed artifact must not survive in durable recovery state"
    );
    assert!(
        !next.tail_pending_entries.is_empty(),
        "failed secondary coverage must remain queryable through exact tail"
    );

    table.tablet().save_meta().unwrap();
    let descriptor = table.to_descriptor().expect("table descriptor");
    drop(manifest);
    drop(table);
    let reopened = reopen_table_with_root(
        root.path(),
        &[LogicalType::Array(Box::new(LogicalType::Float), 4)],
        &descriptor,
    );
    reopened
        .register_search_definition(definition)
        .expect("recovery must attach the durable exact-tail revision");
    let recovered = reopened.search_registry().view.load();
    let recovered_manifest = recovered
        .definitions
        .get(&92)
        .and_then(|state| state.manifest.as_ref())
        .expect("recovered quarantine manifest");
    assert_eq!(recovered_manifest.root.root_version, next_head.root_version);
    assert!(recovered_manifest
        .artifacts
        .artifacts
        .iter()
        .all(|current| current != &artifact));
}

#[test]
fn explicit_hnsw_definition_overrides_and_restores_schema_seed_origin() {
    let root = TempDir::new().unwrap();
    let table = create_schema_seeded_hnsw_table(
        root.path(),
        &[LogicalType::Array(Box::new(LogicalType::Float), 4)],
        0,
    );
    let seed_definition_id = SCHEMA_SEED_BIT;
    {
        let current = table.search_registry().view.load();
        let seed_state = current
            .definitions
            .get(&seed_definition_id)
            .expect("schema seed definition");
        assert_eq!(seed_state.origin, SearchDefinitionOrigin::schema_seed(0));
    }

    let provider_config = test_hnsw_provider_config(4, 16, 64, 4_096);
    let definition = SearchIndexDefinition {
        definition_id: 77,
        table_id: table.tablet_id(),
        name: "explicit_vec_hnsw".to_string(),
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
        assert!(!current.definitions.contains_key(&seed_definition_id));
        let catalog_state = current.definitions.get(&77).expect("catalog definition");
        assert_eq!(catalog_state.origin, SearchDefinitionOrigin::catalog(77));
    }

    table.unregister_search_definition(77).unwrap();
    let current = table.search_registry().view.load();
    let restored = current
        .definitions
        .get(&seed_definition_id)
        .expect("restored schema seed");
    assert_eq!(restored.origin, SearchDefinitionOrigin::schema_seed(0));
}

#[test]
fn hnsw_schema_seed_definition_recovers_after_reopen() {
    let root = TempDir::new().unwrap();
    let vector_type = LogicalType::Array(Box::new(LogicalType::Float), 4);
    let table =
        create_schema_seeded_hnsw_table(root.path(), &[LogicalType::Integer, vector_type], 1);
    table.bind_search_task_scheduler(Some(Arc::new(TaskScheduler::new())));
    table
        .append(&test_chunk_from_vectors(vec![
            test_i32_vector(&[1, 2]),
            test_embedding_vector(&[vec![1.0, 0.0, 0.0, 0.0], vec![0.0, 1.0, 0.0, 0.0]], 4),
        ]))
        .unwrap();
    let seed_definition_id = SCHEMA_SEED_BIT | 1;
    table
        .search_registry()
        .materialize_definition(seed_definition_id)
        .unwrap();
    {
        let current = table.search_registry().view.load();
        let seed_state = current
            .definitions
            .get(&seed_definition_id)
            .expect("schema seed definition");
        assert_eq!(seed_state.origin, SearchDefinitionOrigin::schema_seed(1));
        let generation = seed_state.generation.as_ref().expect("seed generation");
        assert!(generation.coverage.is_complete());
        assert_eq!(generation.generation_stats.artifact_count, 1);
        let manifest = seed_state.manifest.as_ref().expect("seed manifest");
        assert_eq!(manifest.artifacts.artifacts.len(), 1);
        assert!(matches!(
            manifest.artifacts.artifacts[0].location,
            ArtifactLocation::SidecarArtifactFile { .. }
        ));
    }

    let descriptor = table.to_descriptor().expect("descriptor");
    drop(table);
    let reopened = reopen_table_with_root(root.path(), &[], &descriptor);
    let recovered_capability = reopened
        .vector_capability(1, DistanceMetric::Euclidean)
        .expect("recovered schema seed capability");
    assert_eq!(recovered_capability.definition_id, seed_definition_id);
    assert!(recovered_capability.coverage.is_complete());
    assert_eq!(recovered_capability.generation_stats.artifact_count, 1);
    let recovered_stats = reopened
        .hnsw_generation_statistics(seed_definition_id)
        .unwrap()
        .expect("recovered generation HNSW statistics");
    assert_eq!(recovered_stats.num_indexed_vectors, 2);
    assert_eq!(recovered_stats.dimension, 4);
    {
        let current = reopened.search_registry().view.load();
        let seed_state = current
            .definitions
            .get(&seed_definition_id)
            .expect("recovered schema seed definition");
        assert_eq!(seed_state.origin, SearchDefinitionOrigin::schema_seed(1));
    }

    let provider_config = test_hnsw_provider_config(4, 16, 64, 4_096);
    let explicit = SearchIndexDefinition {
        definition_id: 78,
        table_id: reopened.tablet_id(),
        name: "explicit_recovered_vec_hnsw".to_string(),
        kind: SearchIndexKind::Hnsw,
        column_ids: vec![1],
        expression: None,
        freshness_policy: SearchFreshnessPolicy::default_for_kind(SearchIndexKind::Hnsw),
        config_fingerprint: SearchIndexDefinition::compute_config_fingerprint(
            SearchIndexKind::Hnsw,
            &[1],
            None,
            &provider_config,
        ),
        provider_config,
    };
    reopened.register_search_definition(explicit).unwrap();
    {
        let current = reopened.search_registry().view.load();
        assert!(!current.definitions.contains_key(&seed_definition_id));
        assert_eq!(
            current
                .definitions
                .get(&78)
                .expect("explicit definition")
                .origin,
            SearchDefinitionOrigin::catalog(78)
        );
    }

    reopened.unregister_search_definition(78).unwrap();
    {
        let current = reopened.search_registry().view.load();
        assert_eq!(
            current
                .definitions
                .get(&seed_definition_id)
                .expect("restored schema seed")
                .origin,
            SearchDefinitionOrigin::schema_seed(1)
        );
    }

    let descriptor = reopened.to_descriptor().expect("descriptor after restore");
    drop(reopened);
    let reopened_again = reopen_table_with_root(root.path(), &[], &descriptor);
    let current = reopened_again.search_registry().view.load();
    let seed_state = current
        .definitions
        .get(&seed_definition_id)
        .expect("schema seed restored after second reopen");
    assert_eq!(seed_state.origin, SearchDefinitionOrigin::schema_seed(1));
    assert!(reopened_again
        .vector_capability(1, DistanceMetric::Euclidean)
        .is_some());

    let opened = reopened_again
        .open_vector_search_cursor(
            1,
            &[1.0, 0.0, 0.0, 0.0],
            DistanceMetric::Euclidean,
            1,
            SearchParams {
                ef: Some(16),
                rerank_window: None,
                objective: crate::index::hnsw::HnswSearchObjective::CostOptimized,
                random_entry_point: Some(false),
            },
            None,
            reopened_again.max_version(),
            &crate::search::SearchReadOptions::ungoverned(),
        )
        .expect("query restored schema seed generation");
    let chunks = drain_search_cursor(&reopened_again, opened, &[0], false, 1)
        .expect("materialize restored schema seed query");
    let mut ids = Vec::new();
    for chunk in chunks {
        let id_col = chunk.column(0).expect("id projection");
        for row in 0..chunk.size() {
            ids.push(id_col.get_i32(row).expect("id value"));
        }
    }
    assert_eq!(ids, vec![1]);
}
