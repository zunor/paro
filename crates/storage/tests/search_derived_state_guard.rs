// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Copy)]
struct AllowedOccurrence {
    rel_path: &'static str,
    pattern: &'static str,
    count: usize,
}

const LEGACY_MATERIALIZATION_ALLOWLIST: &[AllowedOccurrence] = &[];

// The single row_fetch occurrence opens a column iterator so the search layer
// can call batched read_by_rowids(); it is not a per-row seek/read fallback.
const SEARCH_HOT_PATH_ALLOWLIST: &[AllowedOccurrence] = &[AllowedOccurrence {
    rel_path: "crates/storage/src/search/row_fetch.rs",
    pattern: "segment.new_column_iterator",
    count: 1,
}];

#[test]
fn legacy_search_materialization_callers_do_not_grow() {
    let workspace = workspace_root();
    assert_allowlist_exact(
        &workspace,
        &[
            "crates/storage/src/search",
            "crates/storage/src/write",
            "crates/storage/src/index/fulltext",
            "crates/storage/src/index/sparse",
        ],
        &["materialize_rowset_artifacts", "patch_segment_footer("],
        LEGACY_MATERIALIZATION_ALLOWLIST,
    );
}

#[test]
fn search_hot_path_antipatterns_do_not_spread() {
    let workspace = workspace_root();
    assert_allowlist_exact(
        &workspace,
        &["crates/storage/src/search"],
        &[
            "segment.new_column_iterator",
            "rowset.reload()",
            "seek_to_ordinal(",
            "next_batch(1)",
        ],
        SEARCH_HOT_PATH_ALLOWLIST,
    );
}

#[test]
fn rowset_writer_search_contract_stays_registry_free() {
    let workspace = workspace_root();
    let rel_path = "crates/storage/src/rowset/rowset_writer.rs";
    let text = fs::read_to_string(workspace.join(rel_path)).expect("read rowset writer");

    assert!(
        text.contains("SearchInlineBuilderSet"),
        "RowsetWriterContext should keep the typed builder-set contract as the only search entry point"
    );
    for forbidden in [
        "SearchIndexRegistry",
        "SearchWritePlan",
        "materialize_rowset_artifacts",
        "search::registry",
        "search::write_path",
    ] {
        assert!(
            !text.contains(forbidden),
            "RowsetWriter must not depend on legacy search registry/materialization symbol `{}`",
            forbidden
        );
    }
}

#[test]
fn manifest_recovery_does_not_scan_rowsets_for_coverage() {
    let workspace = workspace_root();
    let rel_path = "crates/storage/src/search/manifest/mod.rs";
    let text = fs::read_to_string(workspace.join(rel_path)).expect("read search manifest");

    for forbidden in [
        "capture_consistent_rowsets",
        "collect_rowset_snapshot",
        "rowset.load()",
        "segment.new_column_iterator",
        "materialize_rowset_artifacts",
    ] {
        assert!(
            !text.contains(forbidden),
            "manifest recovery must replay durable manifest/tail metadata instead of scanning rowsets; found `{}`",
            forbidden
        );
    }
}

#[test]
fn search_directory_structure_stays_split_by_responsibility() {
    let workspace = workspace_root();
    for rel_path in [
        "crates/storage/src/search/definition/freshness.rs",
        "crates/storage/src/search/definition/origin.rs",
        "crates/storage/src/search/definition/validation.rs",
        "crates/storage/src/search/generation/coverage.rs",
        "crates/storage/src/search/generation/head.rs",
        "crates/storage/src/search/generation/maintenance_state.rs",
        "crates/storage/src/search/manifest/mod.rs",
        "crates/storage/src/search/manifest/binary.rs",
        "crates/storage/src/search/tail/mod.rs",
        "crates/storage/src/search/tail/exact_merge.rs",
        "crates/storage/src/search/lifecycle/catch_up_planner.rs",
        "crates/storage/src/search/lifecycle/maintenance_scheduler.rs",
        "crates/storage/src/search/lifecycle/publisher.rs",
        "crates/storage/src/search/providers/fulltext/inline.rs",
        "crates/storage/src/search/providers/fulltext/search.rs",
        "crates/storage/src/search/providers/sparse/inline.rs",
        "crates/storage/src/search/providers/sparse/row_image.rs",
        "crates/storage/src/search/providers/sparse/search.rs",
        "crates/storage/src/search/providers/hnsw/search.rs",
    ] {
        assert!(
            workspace.join(rel_path).is_file(),
            "expected long-term search module `{}`",
            rel_path
        );
    }

    for rel_path in [
        "crates/storage/src/search/manifest.rs",
        "crates/storage/src/search/manifest_binary.rs",
        "crates/storage/src/search/tail.rs",
        "crates/storage/src/search/tail_exact_merge.rs",
        "crates/storage/src/search/fulltext_search.rs",
        "crates/storage/src/search/inline_fulltext.rs",
        "crates/storage/src/search/inline_sparse.rs",
        "crates/storage/src/search/sparse_search.rs",
        "crates/storage/src/search/sparse_row_image.rs",
        "crates/storage/src/search/vector_search.rs",
    ] {
        assert!(
            !workspace.join(rel_path).exists(),
            "search implementation should not return to flat transitional module `{}`",
            rel_path
        );
    }
}

#[test]
fn varlen_rowid_lookup_does_not_reintroduce_dictionary_per_row_code_reads() {
    let workspace = workspace_root();
    let rel_path = "crates/storage/src/rowset/column/column_iterator.rs";
    let text = fs::read_to_string(workspace.join(rel_path)).expect("read column iterator");

    assert!(
        !text.contains("next_dict_codes(1)"),
        "dictionary varlen read_by_rowids must batch codes by page-local rowid spans"
    );
}

#[test]
fn fixed_width_rowid_lookup_does_not_reintroduce_per_row_seek_loop() {
    let workspace = workspace_root();
    let rel_path = "crates/storage/src/rowset/column/column_iterator.rs";
    let text = fs::read_to_string(workspace.join(rel_path)).expect("read column iterator");

    for forbidden in ["seek_to_ordinal(rowid)?", "next_batch(1)?"] {
        assert!(
            !text.contains(forbidden),
            "fixed-width read_by_rowids must batch rows by page-local rowid spans; found `{}`",
            forbidden
        );
    }
}

#[test]
fn legacy_runtime_search_api_names_do_not_return() {
    let workspace = workspace_root();
    assert_allowlist_exact(
        &workspace,
        &[
            "crates/storage/src",
            "crates/execution/src",
            "crates/planner/src",
            "crates/optimizer/src",
            "crates/session/src",
            "crates/instance/src",
        ],
        &[
            "build_runtime_hnsw_index",
            "build_runtime_sparse_index",
            "build_runtime_fulltext_index",
            "attach_metadata_only_index_runtime",
            "apply_descriptor_only_index_runtime",
            "requires_runtime_build",
        ],
        &[],
    );
}

#[test]
fn planner_queryability_does_not_use_schema_flags_or_declared_sets() {
    let workspace = workspace_root();
    assert_allowlist_exact(
        &workspace,
        &[
            "crates/planner/src",
            "crates/optimizer/src",
            "crates/execution/src/physical",
            "crates/execution/src/operators/search",
        ],
        &[
            "index_hnsw",
            "declared_art_columns",
            "declared_art_indexes",
            "has_hnsw_index",
            "has_sparse_index",
            "has_fulltext_index",
        ],
        &[],
    );
}

fn assert_allowlist_exact(
    workspace: &Path,
    roots: &[&str],
    patterns: &[&str],
    allowlist: &[AllowedOccurrence],
) {
    let mut actual = BTreeMap::<(String, &'static str), usize>::new();
    for root in roots {
        for file in rust_files(workspace.join(root)) {
            let rel_path = file
                .strip_prefix(workspace)
                .expect("file under workspace")
                .to_string_lossy()
                .replace('\\', "/");
            let text = fs::read_to_string(&file).expect("read rust source");
            for pattern in patterns {
                let count = text.matches(pattern).count();
                if count > 0 {
                    actual.insert((rel_path.clone(), *pattern), count);
                }
            }
        }
    }

    let expected = allowlist
        .iter()
        .map(|entry| ((entry.rel_path.to_string(), entry.pattern), entry.count))
        .collect::<BTreeMap<_, _>>();

    assert_eq!(
        actual, expected,
        "search derived-state legacy anti-pattern allowlist changed; update the refactor plan before adding new callsites"
    );
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("storage crate is under workspace/crates")
        .to_path_buf()
}

fn rust_files(path: PathBuf) -> Vec<PathBuf> {
    if path.is_file() {
        return vec![path];
    }

    let mut files = Vec::new();
    let mut stack = vec![path];
    while let Some(dir) = stack.pop() {
        if !dir.exists() {
            continue;
        }
        for entry in fs::read_dir(dir).expect("read source dir") {
            let entry = entry.expect("read source entry");
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().is_some_and(|ext| ext == "rs") {
                files.push(path);
            }
        }
    }
    files
}
