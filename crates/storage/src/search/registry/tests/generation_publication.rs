// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Generation visibility, freshness, head publication, and rowset observer invariants.

use super::*;

#[test]
fn explicit_fulltext_definition_publishes_manifest_and_survives_reload() {
    let root = TempDir::new().unwrap();
    let table = create_table_with_root(root.path(), &[LogicalType::Varchar]);

    let provider_config = json!({"version": 1, "config": "simple"});
    let definition = SearchIndexDefinition {
        definition_id: 42,
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

    table
        .register_search_definition(definition.clone())
        .expect("register fulltext definition");
    table
        .append(&test_chunk_from_vectors(vec![test_string_vector(&[
            "graph vector",
        ])]))
        .unwrap();
    let capability = table
        .search_registry()
        .capability(
            SearchIndexKind::FullText,
            0,
            Some(definition.config_fingerprint),
        )
        .expect("fulltext capability");
    assert_eq!(capability.definition_id, 42);
    let snapshot = table
        .open_search_generation_snapshot(42)
        .unwrap()
        .expect("generation snapshot");
    assert_eq!(snapshot.artifacts.artifacts.len(), 1);
    let artifact = &snapshot.artifacts.artifacts[0];
    assert_eq!(
        singleton_artifact_segment(artifact),
        ArtifactSegmentRef {
            rowset_id: 1,
            segment_id: 0,
        }
    );
    assert_eq!(artifact.column_id, 0);
    match &artifact.location {
        ArtifactLocation::Inline { page } => {
            assert_eq!(page.rowset_id, 1);
            assert_eq!(page.segment_id, 0);
            assert_eq!(page.column_id, 0);
            assert!(page.page_offset > 0);
            assert!(page.page_len > 0);
            assert_ne!(page.checksum, 0);
        }
        other => panic!("expected inline artifact location, got {other:?}"),
    }
    assert_ne!(artifact.checksum, 0);

    let reopened = reopen_table_with_root(
        root.path(),
        &[LogicalType::Varchar],
        &table.to_descriptor().expect("descriptor"),
    );
    reopened
        .register_search_definition(definition)
        .expect("reload definition");
    assert!(reopened.fulltext_capability(0, "simple").is_some());
}

#[test]
fn token_open_validates_generation_head_before_snapshot() {
    let root = TempDir::new().unwrap();
    let table = create_table_with_root(root.path(), &[LogicalType::Varchar]);
    let provider_config = json!({"version": 1, "config": "simple"});
    let definition = SearchIndexDefinition {
        definition_id: 43,
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

    table
        .register_search_definition(definition.clone())
        .unwrap();
    table
        .append(&test_chunk_from_vectors(vec![test_string_vector(&[
            "token stale guard",
        ])]))
        .unwrap();

    let capability = table
        .search_registry()
        .capability(
            SearchIndexKind::FullText,
            0,
            Some(definition.config_fingerprint),
        )
        .expect("queryable capability");
    let token = capability.capability_token();

    match table
        .open_search_generation_snapshot_with_token(&token)
        .unwrap()
    {
        OpenSearchCursorResult::Opened(snapshot) => {
            assert_eq!(snapshot.definition_id, 43);
            assert_eq!(snapshot.generation_id, token.generation_id);
        }
        other => panic!("expected opened snapshot, got {other:?}"),
    }

    let mut stale = token.clone();
    stale.generation_id = stale.generation_id.saturating_add(1);
    assert!(matches!(
        table
            .open_search_generation_snapshot_with_token(&stale)
            .unwrap(),
        OpenSearchCursorResult::CapabilityTokenStale
    ));

    let mut not_queryable = token;
    not_queryable.capability_state = SearchCapabilityState::NotQueryable {
        reason: SearchNotQueryableReason::CoverageIncomplete,
    };
    assert!(matches!(
        table
            .open_search_generation_snapshot_with_token(&not_queryable)
            .unwrap(),
        OpenSearchCursorResult::NotQueryable
    ));
}

#[test]
fn required_freshness_tail_pending_waits_for_catch_up_before_capability() {
    let root = TempDir::new().unwrap();
    let table = create_table_with_root(root.path(), &[LogicalType::Varchar]);
    table
        .append(&test_chunk_from_vectors(vec![test_string_vector(&[
            "required freshness waits",
        ])]))
        .unwrap();

    let provider_config = json!({"version": 1, "config": "simple"});
    let definition = SearchIndexDefinition {
        definition_id: 48,
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

    let capability = table
        .search_registry()
        .capability(
            SearchIndexKind::FullText,
            0,
            Some(definition.config_fingerprint),
        )
        .expect("required freshness capability after catch up");
    assert!(capability.is_queryable());
    assert_eq!(
        capability.capability_state(),
        SearchCapabilityState::Queryable
    );
    assert_eq!(capability.tail_summary.pending_rows, 0);
    let current = table.search_registry().view.load();
    let manifest = current
        .definitions
        .get(&48)
        .and_then(|state| state.manifest.as_ref())
        .expect("manifest after required freshness wait");
    assert!(manifest.root.coverage.is_complete());
    assert!(manifest.tail_pending_entries.is_empty());
    assert!(manifest.artifacts.artifacts.iter().any(|artifact| matches!(
        artifact.location,
        ArtifactLocation::SidecarArtifactFile { .. }
    )));

    assert!(matches!(
        table
            .open_search_generation_snapshot_with_token(&capability.capability_token())
            .unwrap(),
        OpenSearchCursorResult::Opened(_)
    ));
}

#[test]
fn explicit_materialization_does_not_publish_incomplete_definition_as_ready() {
    let root = TempDir::new().unwrap();
    let table = create_table_with_root(root.path(), &[LogicalType::Varchar]);
    table
        .append(&test_chunk_from_vectors(vec![test_string_vector(&[
            "create index backfills visible rows",
        ])]))
        .unwrap();

    let provider_config = json!({"version": 1, "config": "simple"});
    let definition = SearchIndexDefinition {
        definition_id: 49,
        table_id: table.tablet_id(),
        name: "docs_fts_materialized".to_string(),
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
    assert!(!table
        .search_generation_coverage(49)
        .unwrap()
        .expect("coverage before materialization")
        .is_complete());

    let coverage = table
        .search_registry()
        .materialize_catalog_definition_by_name("docs_fts_materialized")
        .unwrap();
    assert!(coverage.is_complete());
    assert_eq!(
        coverage.indexed_segment_count,
        coverage.visible_segment_count
    );
    let current = table.search_registry().view.load();
    let manifest = current
        .definitions
        .get(&49)
        .and_then(|state| state.manifest.as_ref())
        .expect("manifest after explicit materialization");
    assert!(manifest.tail_pending_entries.is_empty());
    assert_eq!(manifest.artifacts.artifacts.len(), 1);
}

#[test]
fn token_open_rechecks_same_generation_freshness_degradation() {
    let root = TempDir::new().unwrap();
    let table = create_table_with_root(root.path(), &[LogicalType::Varchar]);
    table
        .append(&test_chunk_from_vectors(vec![test_string_vector(&[
            "bounded lag initially queryable",
        ])]))
        .unwrap();

    let provider_config = json!({"version": 1, "config": "simple"});
    let definition = SearchIndexDefinition {
        definition_id: 49,
        table_id: table.tablet_id(),
        name: "docs_fts_bounded".to_string(),
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
    table
        .register_search_definition(definition.clone())
        .unwrap();

    let token = table
        .search_registry()
        .capability(
            SearchIndexKind::FullText,
            0,
            Some(definition.config_fingerprint),
        )
        .expect("bounded lag capability")
        .capability_token();
    assert!(token.is_queryable());

    table
        .search_registry()
        .update_registry_view(|view| {
            let state = view
                .definitions
                .get_mut(&49)
                .expect("definition state to tighten freshness");
            state.definition.freshness_policy = SearchFreshnessPolicy::Required;
            if let Some(capability) = state.capability.as_mut() {
                capability.freshness_policy = SearchFreshnessPolicy::Required;
            }
            Ok((true, ()))
        })
        .unwrap();

    assert!(matches!(
        table
            .open_search_generation_snapshot_with_token(&token)
            .unwrap(),
        OpenSearchCursorResult::NotQueryable
    ));
}

#[test]
fn rowset_publish_observer_eagerly_refreshes_search_manifest() {
    let root = TempDir::new().unwrap();
    let table = create_table_with_root(root.path(), &[LogicalType::Varchar]);
    let provider_config = json!({"version": 1, "config": "simple"});
    let definition = SearchIndexDefinition {
        definition_id: 43,
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
    let replay_count_before_append = table.search_registry().manifests.full_replay_count();
    table
        .append(&test_chunk_from_vectors(vec![test_string_vector(&[
            "observer refresh",
        ])]))
        .unwrap();
    assert_eq!(
        table.search_registry().manifests.full_replay_count(),
        replay_count_before_append,
        "rowset publish must install its prepared in-memory manifest without disk replay"
    );

    let current = table.search_registry().view.load();
    let state = current.definitions.get(&43).expect("definition state");
    let manifest = state.manifest.as_ref().expect("manifest after append");
    assert_eq!(manifest.root.build_snapshot_version, table.max_version());
    assert_eq!(manifest.artifacts.artifacts.len(), 1);
    assert_eq!(
        singleton_artifact_segment(&manifest.artifacts.artifacts[0]).rowset_id,
        1
    );
}

#[test]
fn unpublished_rowset_with_inline_artifact_is_not_queryability_truth() {
    let root = TempDir::new().unwrap();
    let table = create_table_with_root(root.path(), &[LogicalType::Varchar]);
    let provider_config = json!({"version": 1, "config": "simple"});
    let definition = SearchIndexDefinition {
        definition_id: 143,
        table_id: table.tablet_id(),
        name: "docs_fts_unpublished".to_string(),
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

    let rowset_id = 900;
    let rowset_path = table.tablet().canonical_rowset_path(rowset_id);
    let schema = table.tablet().schema().expect("schema");
    let write_context = table.search_registry().write_context().unwrap();
    let context = RowsetWriterContext::new(
        schema,
        table.tablet_id(),
        Version::singleton(0),
        &rowset_path,
    )
    .with_rowset_id(rowset_id)
    .with_search_inline_builders(write_context.inline_builders);
    let mut writer = RowsetWriter::create(context).unwrap();
    writer
        .add_chunk(&[ColumnData::new(
            encode_varlen(&["orphan inline artifact"]),
            1,
        )])
        .unwrap();
    let rowset = writer.build().unwrap();
    assert!(
        rowset.segments()[0].fulltext_index(0).is_some(),
        "test setup must write a real inline artifact before publish"
    );

    assert!(!table.search_registry().has_queryable_artifact(
        SearchIndexKind::FullText,
        rowset_id,
        0,
        0
    ));
    let current = table.search_registry().view.load();
    let manifest = current
        .definitions
        .get(&143)
        .and_then(|state| state.manifest.as_ref())
        .expect("manifest");
    assert!(manifest.artifacts.artifacts.is_empty());
}

#[test]
fn fulltext_registry_refresh_appends_delta_for_new_rowsets() {
    let root = TempDir::new().unwrap();
    let table = create_table_with_root(root.path(), &[LogicalType::Varchar]);
    let provider_config = json!({"version": 1, "config": "simple"});
    let definition = SearchIndexDefinition {
        definition_id: 7,
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
            "graph alpha",
        ])]))
        .unwrap();

    table
        .append(&test_chunk_from_vectors(vec![test_string_vector(&[
            "graph beta",
        ])]))
        .unwrap();
    let delta_entries = load_manifest_delta_entries(&table, 7);
    assert!(delta_entries.iter().any(|entry| matches!(
        entry,
        ManifestDeltaEntry::AddArtifact(artifact)
            if artifact.kind == SearchIndexKind::FullText
    )));
    assert!(delta_entries.iter().any(|entry| matches!(
        entry,
        ManifestDeltaEntry::StatsDelta(SearchStatsDelta::FullText(delta))
            if delta.stats.total_docs > 0
    )));

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
        .unwrap();
    let mut cursor = opened.cursor;
    let snapshot = opened.snapshot;
    let mut chunks = Vec::new();
    let batch = SearchBatchConfig {
        row_limit: 1024,
        preferred_bytes: 1 << 20,
    };
    let mut budget = ResourceBudget::standalone(64 * 1024 * 1024, 1024, 4);
    loop {
        match cursor.next_batch(&batch, &mut budget).unwrap() {
            SearchBatchState::Ready(batch) if batch.is_empty() => continue,
            SearchBatchState::Ready(batch) => chunks.push(
                table
                    .materialize_search_batch(
                        &snapshot,
                        batch,
                        &[0],
                        false,
                        Arc::new(default_allocator()),
                    )
                    .unwrap(),
            ),
            SearchBatchState::Exhausted => break,
        }
    }
    assert_eq!(chunks.iter().map(|chunk| chunk.size()).sum::<usize>(), 2);
    assert!(table.fulltext_capability(0, "simple").is_some());
}

#[test]
fn rowset_publish_persists_generation_head_with_tablet_meta() {
    let root = TempDir::new().unwrap();
    let table = create_table_with_root(root.path(), &[LogicalType::Varchar]);
    let definition = fulltext_test_definition(107);
    table
        .register_search_definition(definition.clone())
        .unwrap();

    table
        .append(&test_chunk_from_vectors(vec![test_string_vector(&[
            "generation head is part of rowset publish",
        ])]))
        .unwrap();

    let manager = meta_manager(root.path());
    let meta = manager
        .load_tablet_meta(table.tablet_id())
        .unwrap()
        .expect("tablet meta");
    let head = meta
        .search_generation_heads()
        .iter()
        .find(|head| head.definition_id == 107)
        .expect("search generation head");
    assert_eq!(head.root_version, 2);
    assert!(head.root_file_name.starts_with("manifest_root_g1_v2"));

    let manifest = table
        .search_registry()
        .manifests
        .load_manifest_for_head(head)
        .unwrap()
        .expect("manifest by durable head");
    assert_eq!(manifest.root.build_snapshot_version, table.max_version());
    assert_eq!(
        manifest.root.config_fingerprint,
        definition.config_fingerprint
    );
    assert!(manifest.artifacts.artifacts.iter().any(|artifact| {
        artifact.kind == SearchIndexKind::FullText
            && singleton_artifact_segment(artifact).rowset_id > 0
    }));
}

#[test]
fn rowset_publish_failure_preserves_failed_index_head_and_commits_base_rowset() {
    let root = TempDir::new().unwrap();
    let table = create_table_with_root(root.path(), &[LogicalType::Varchar, LogicalType::Blob]);
    let fulltext = SearchIndexDefinition {
        table_id: table.tablet_id(),
        ..fulltext_test_definition(200)
    };
    table.register_search_definition(fulltext).unwrap();

    let provider_config = json!({"version": 1, "physical_encoding": "binary-v1" });
    let sparse = SearchIndexDefinition {
        definition_id: 201,
        table_id: table.tablet_id(),
        name: "sparse_201".to_string(),
        kind: SearchIndexKind::Sparse,
        column_ids: vec![1],
        expression: None,
        freshness_policy: SearchFreshnessPolicy::default_for_kind(SearchIndexKind::Sparse),
        config_fingerprint: SearchIndexDefinition::compute_config_fingerprint(
            SearchIndexKind::Sparse,
            &[1],
            None,
            &provider_config,
        ),
        provider_config,
    };
    table.register_search_definition(sparse).unwrap();

    let initial_fulltext_root = {
        let current = table.search_registry().view.load();
        current
            .definitions
            .get(&200)
            .and_then(|state| state.manifest.as_ref())
            .expect("fulltext manifest")
            .root
            .root_version
    };
    let initial_sparse_head = table
        .tablet()
        .search_generation_head(201)
        .expect("initial sparse head");
    let failing_sparse_root = table
        .search_registry()
        .manifests
        .generation_dir(201, 1)
        .join(format!(
            "manifest_root_g1_v2_f{}.json",
            table
                .search_registry()
                .view
                .load()
                .definitions
                .get(&201)
                .unwrap()
                .definition
                .config_fingerprint
        ));
    std::fs::create_dir(&failing_sparse_root).unwrap();

    table
        .append(&test_chunk_from_vectors(vec![
            test_string_vector(&["first definition prepares a candidate"]),
            test_sparse_blob_vector(&[SparseVector::new(vec![1], vec![1.0]).unwrap()]),
        ]))
        .expect("derived sparse manifest failure must not roll back base rowset");
    assert_eq!(table.max_version(), 0);

    let current = table.search_registry().view.load();
    let fulltext_manifest = current
        .definitions
        .get(&200)
        .and_then(|state| state.manifest.as_ref())
        .expect("fulltext manifest after partial search publish");
    assert!(fulltext_manifest.root.root_version > initial_fulltext_root);
    drop(current);

    let meta = meta_manager(root.path())
        .load_tablet_meta(table.tablet_id())
        .unwrap()
        .expect("tablet meta");
    let durable_head = meta
        .search_generation_heads()
        .iter()
        .find(|head| head.definition_id == 200)
        .expect("durable fulltext head");
    assert!(durable_head.root_version > initial_fulltext_root);
    assert_eq!(
        meta.search_generation_heads()
            .iter()
            .find(|head| head.definition_id == 201),
        Some(&initial_sparse_head),
        "a failed derived revision must preserve the last durable head"
    );
    let current = table.search_registry().view.load();
    let sparse_state = current.definitions.get(&201).expect("sparse state");
    assert!(sparse_state.manifest.is_some());
    assert!(
        sparse_state.capability.is_none(),
        "the stale in-memory capability must be disabled without deleting its recovery head"
    );
}

#[test]
fn registry_reopen_ignores_unreferenced_versioned_root_candidate() {
    let root = TempDir::new().unwrap();
    let table = create_table_with_root(root.path(), &[LogicalType::Varchar]);
    let definition = fulltext_test_definition(108);
    table
        .register_search_definition(definition.clone())
        .unwrap();

    let current = table.search_registry().view.load();
    let manifest = current
        .definitions
        .get(&108)
        .and_then(|state| state.manifest.as_ref())
        .expect("manifest");
    let durable_root_version = manifest.root.root_version;
    let mut orphan_root = manifest.root.clone();
    drop(current);
    orphan_root.root_version = 99;
    orphan_root.build_snapshot_version = 99;
    orphan_root.indexed_through_ts = 99;
    orphan_root.recompute_checksum().unwrap();
    table
        .search_registry()
        .manifests
        .write_root(108, &orphan_root)
        .unwrap();

    let descriptor = table.to_descriptor().expect("descriptor");
    drop(table);
    let reopened = reopen_table_with_root(root.path(), &[LogicalType::Varchar], &descriptor);
    reopened.register_search_definition(definition).unwrap();

    let current = reopened.search_registry().view.load();
    let reopened_manifest = current
        .definitions
        .get(&108)
        .and_then(|state| state.manifest.as_ref())
        .expect("reopened manifest");
    assert_eq!(reopened_manifest.root.root_version, durable_root_version);
    assert_ne!(reopened_manifest.root.root_version, 99);
}
