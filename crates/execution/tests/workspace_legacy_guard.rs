// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::fs;
use std::path::{Path, PathBuf};

const LEGACY_PATTERNS: &[&str] = &[
    "paro_execution::operator::",
    "crate::operator::",
    "dyn PhysicalOperator",
    "PhysicalOperatorType",
    "build_pipelines",
    "LegacyPhysicalPlan",
    "from_legacy_buffer",
    "StreamExecutionResult",
    "bind_execution_coordinator",
    "StatementExecutionTracker",
    "QueryOutputBuffer",
    "EventCoordinator",
    "mod query_output_buffer",
];

#[test]
fn workspace_runtime_roots_have_no_legacy_operator_path() {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace = manifest
        .parent()
        .and_then(Path::parent)
        .expect("execution crate is under workspace/crates");

    for removed_file in [
        "crates/execution/src/memory_runtime/query_output_buffer.rs",
        "crates/execution/src/query_executor/stream.rs",
    ] {
        assert!(
            !workspace.join(removed_file).exists(),
            "{removed_file} must stay deleted"
        );
    }

    for required_file in [
        "crates/execution/src/query_executor/stream/mod.rs",
        "crates/execution/src/query_executor/stream/typed_streaming.rs",
        "crates/execution/src/query_executor/stream/completed_output.rs",
    ] {
        assert!(
            workspace.join(required_file).is_file(),
            "{required_file} must exist"
        );
    }

    for root in [
        "crates/execution/src/query_executor",
        "crates/execution/src/memory_runtime",
        "crates/session/src",
        "crates/session/tests",
        "crates/instance/src",
        "crates/compiler/src",
        "crates/context/src",
    ] {
        for file in rust_files(workspace.join(root)) {
            let text = fs::read_to_string(&file).expect("read rust source");
            for pattern in LEGACY_PATTERNS {
                assert!(
                    !text.contains(pattern),
                    "{} contains legacy operator-runtime pattern `{}`",
                    file.display(),
                    pattern
                );
            }
        }
    }
}

#[test]
fn stream_handler_stays_split_by_role() {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let stream_dir = manifest.join("src/query_executor/stream");

    for file_name in ["mod.rs", "typed_streaming.rs", "completed_output.rs"] {
        let path = stream_dir.join(file_name);
        let line_count = fs::read_to_string(&path)
            .expect("read stream source")
            .lines()
            .count();
        assert!(
            line_count <= 400,
            "{} has {} lines, above stream role-file limit 400",
            path.display(),
            line_count
        );
    }
}

fn rust_files(path: PathBuf) -> Vec<PathBuf> {
    if !path.exists() {
        return Vec::new();
    }
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
