// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::fs;
use std::path::{Path, PathBuf};

#[test]
fn hot_path_files_stay_split_and_role_files_stay_thin() {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let checked_roots = [
        "src/runtime/source.rs",
        "src/runtime/transform.rs",
        "src/runtime/sink.rs",
        "src/runtime/state.rs",
        "src/runtime/task_executor",
        "src/pipeline/lowerer",
        "src/physical/generator",
    ];

    for root in checked_roots {
        for file in rust_files(manifest.join(root)) {
            let line_count = fs::read_to_string(&file)
                .expect("read rust source")
                .lines()
                .count();
            let limit = if is_test_source(&file) { 1_200 } else { 800 };
            assert!(
                line_count <= limit,
                "{} has {} lines, above operator-runtime structure limit {}",
                file.display(),
                line_count,
                limit
            );
        }
    }

    assert!(
        !manifest.join("src/runtime/task_executor.rs").exists(),
        "task executor must remain split under runtime/task_executor/"
    );
    assert!(
        !manifest.join("src/pipeline/lowerer.rs").exists(),
        "pipeline lowerer must remain split under pipeline/lowerer/"
    );
    assert!(
        !manifest.join("src/physical/generator.rs").exists(),
        "physical generator must remain split under physical/generator/"
    );
}

#[test]
fn role_files_do_not_reabsorb_domain_helpers() {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let source = read(&manifest, "src/runtime/source.rs");
    for pattern in [
        "fn poll_expression_rows",
        "SearchSourceSpecRef",
        "create_search_driver",
        "parse_fulltext",
    ] {
        assert!(
            !source.contains(pattern),
            "runtime/source.rs reabsorbed domain helper `{pattern}`"
        );
    }

    let transform = read(&manifest, "src/runtime/transform.rs");
    assert!(
        !transform.contains("ensure_transform_output"),
        "runtime/transform.rs must not own output-shape helpers"
    );

    let dml_helpers = read(&manifest, "src/operators/dml/helpers.rs");
    for pattern in ["external_table_global", "external_table_local"] {
        assert!(
            !dml_helpers.contains(pattern),
            "DML helpers still contain stale external table helper `{pattern}`"
        );
    }
}

#[test]
fn sorting_state_compat_layer_stays_out_of_typed_runtime() {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let lib = read(&manifest, "src/lib.rs");
    assert!(
        lib.contains("pub(crate) mod operator_state;")
            && !lib.contains("pub mod operator_state;"),
        "operator_state must stay crate-private; it is a sorting compatibility layer, not public runtime API"
    );

    let allowed_suffixes = [
        "src/lib.rs",
        "src/operator_state.rs",
        "src/sorting/sort.rs",
        "src/sorting/sorted_run_merger.rs",
    ];
    for file in rust_files(manifest.join("src")) {
        let text = fs::read_to_string(&file).expect("read rust source");
        if !text.contains("operator_state") {
            continue;
        }
        let path = file.to_string_lossy();
        assert!(
            allowed_suffixes.iter().any(|suffix| path.ends_with(suffix)),
            "{} imports operator_state outside the sorting compatibility boundary",
            file.display()
        );
    }
}

fn read(root: &Path, rel: &str) -> String {
    fs::read_to_string(root.join(rel)).expect("read source file")
}

fn is_test_source(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name == "tests.rs" || name.ends_with("_tests.rs"))
}

fn rust_files(path: PathBuf) -> Vec<PathBuf> {
    if path.is_file() {
        return vec![path];
    }

    let mut files = Vec::new();
    let mut stack = vec![path];
    while let Some(dir) = stack.pop() {
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
