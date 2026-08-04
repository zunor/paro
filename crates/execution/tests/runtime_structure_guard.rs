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
            let limit = 1_500;
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
fn old_sorting_state_compat_layer_is_gone() {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let lib = read(&manifest, "src/lib.rs");
    assert!(
        !contains_operator_state_boundary(&lib),
        "operator_state compatibility module must not re-enter typed runtime"
    );

    for file in rust_files(manifest.join("src")) {
        let text = fs::read_to_string(&file).expect("read rust source");
        assert!(
            !contains_operator_state_boundary(&text),
            "{} reintroduced operator_state compatibility boundary",
            file.display()
        );
    }
}

#[test]
fn legacy_execution_context_adapter_is_gone() {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    assert!(
        !manifest.join("src/execution_context.rs").exists(),
        "the typed runtime must not retain the legacy execution context adapter"
    );

    let lib = read(&manifest, "src/lib.rs");
    assert!(
        !lib.contains("mod execution_context"),
        "the legacy execution context module must stay removed"
    );

    for file in rust_files(manifest.join("src")) {
        let text = fs::read_to_string(&file).expect("read rust source");
        assert!(
            !text.contains("crate::execution_context")
                && !text.contains("execution_context::ExecutionContext"),
            "{} reintroduced the legacy execution context boundary",
            file.display()
        );
    }
}

fn contains_operator_state_boundary(text: &str) -> bool {
    text.contains("mod operator_state")
        || text.contains("crate::operator_state")
        || text.contains("::operator_state::")
}

fn read(root: &Path, rel: &str) -> String {
    fs::read_to_string(root.join(rel)).expect("read source file")
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
