// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::fs;
use std::path::{Path, PathBuf};

#[test]
fn typed_runtime_entry_has_no_legacy_hot_path() {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace = manifest
        .parent()
        .and_then(Path::parent)
        .expect("execution crate is under workspace/crates");

    let compiled = read(&manifest, "src/query_executor/compiled.rs");
    assert!(
        !compiled.contains("pub physical_plan:"),
        "CompiledStatement must not expose a legacy physical_plan field"
    );
    assert!(
        compiled.contains("image: Arc<CompiledStatementImage>")
            && compiled.contains("compile_environment: CompileEnvironmentKey")
            && compiled.contains("pub struct ExecutionRequest"),
        "compiled programs must carry immutable provenance and separate execution inputs"
    );
    assert!(
        !compiled.contains("pub parameter_bindings:")
            && !compiled.contains("pub executable:")
            && !compiled.contains("pub result_schema:"),
        "CompiledStatement must not expose mutable or execution-local image state"
    );
    assert!(
        !compiled.contains("LegacyPhysicalPlan") && !compiled.contains("legacy_physical_plan"),
        "legacy compiled executable construction must be removed"
    );

    let executor = read(&manifest, "src/query_executor/executor.rs");
    assert!(
        executor.contains("CompiledExecutable::Program(program)"),
        "Executor must dispatch typed StatementProgram as the primary path"
    );
    assert!(
        executor.contains("pub fn execute(&self, request: ExecutionRequest)")
            && executor.contains("let (compiled, parameter_bindings) = request.into_parts()"),
        "Executor must require an explicit plan-plus-bindings execution request"
    );
    assert!(
        !executor.contains("CompiledExecutable::LegacyPhysicalPlan")
            && !executor.contains("execute_legacy_physical_plan"),
        "executor must not carry a legacy PhysicalOperator path"
    );
    assert!(
        !executor.contains("fn build_pipelines"),
        "legacy build_pipelines adapter must be removed"
    );
    assert!(
        executor.contains("from_program_execution"),
        "typed execution must enter through the program execution result handler"
    );
    assert!(
        executor.contains("program_executor::start_program")
            && executor.contains("program_executor::execute_program"),
        "executor must choose typed fetch-driven or completed execution directly"
    );
    assert!(
        !executor.contains("QueryOutputBuffer::detached"),
        "typed executor must not copy completed output into a second result buffer"
    );
    let stream_mod = read(&manifest, "src/query_executor/stream/mod.rs");
    let stream_typed = read(&manifest, "src/query_executor/stream/typed_streaming.rs");
    let stream_completed = read(&manifest, "src/query_executor/stream/completed_output.rs");
    assert!(
        stream_mod.contains("enum ResultOutput")
            && stream_mod.contains("ResultOutput::FetchDriven")
            && stream_mod.contains("ResultOutput::Completed"),
        "result stream state must be modeled as exclusive typed output variants"
    );
    assert!(
        stream_typed.contains("drive_typed_pipeline")
            && stream_typed.contains("drive_until_output_or_finished")
            && stream_completed.contains("fetch_completed_output"),
        "fetch-driven and completed-output stream paths must remain split by role"
    );
    for (name, text) in [
        ("stream/mod.rs", stream_mod.as_str()),
        ("stream/typed_streaming.rs", stream_typed.as_str()),
        ("stream/completed_output.rs", stream_completed.as_str()),
    ] {
        assert!(
            !text.contains("buffer:")
                && !text.contains("coordinator:")
                && !text.contains("from_legacy_buffer")
                && !text.contains("QueryOutputBuffer"),
            "{name} must not reintroduce the legacy buffer/coordinator result path"
        );
    }

    let compiler = read(workspace, "crates/compiler/src/compile.rs");
    assert!(
        compiler.contains("fn compile_regular_statement(")
            && compiler.contains(
                "let arena_plan = match generate_typed_physical_plan(ctx, optimized_plan)"
            )
            && compiler.contains("fn generate_typed_physical_plan("),
        "compiler must build the typed arena physical plan image"
    );
    assert!(
        compiler.contains("StatementProgram::from_physical_plan"),
        "compiler must lower into StatementProgram before execution"
    );
    assert!(
        !compiler.contains(".plan(&mut optimized_plan)"),
        "compiler must not construct legacy Arc<dyn PhysicalOperator> plans"
    );

    assert_legacy_compiled_construction_is_test_only(&manifest);
}

#[test]
fn typed_runtime_modules_do_not_reintroduce_dyn_state() {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let checked_roots = [
        "src/runtime",
        "src/physical",
        "src/pipeline/program.rs",
        "src/pipeline/lowerer",
        "src/pipeline/graph.rs",
    ];
    let forbidden = [
        "dyn PhysicalOperator",
        "Mutex<Option<Arc<dyn Global",
        "set_sink_state(",
        "clear_sink_state(",
        "fn sink_state",
    ];

    for root in checked_roots {
        for file in rust_files(manifest.join(root)) {
            let text = fs::read_to_string(&file).expect("read rust source");
            for pattern in forbidden {
                assert!(
                    !text.contains(pattern),
                    "{} contains forbidden legacy runtime pattern `{}`",
                    file.display(),
                    pattern
                );
            }
        }
    }
}

fn read(root: &Path, rel: &str) -> String {
    fs::read_to_string(root.join(rel)).expect("read source file")
}

fn assert_legacy_compiled_construction_is_test_only(manifest: &Path) {
    let forbidden = [
        "legacy_physical_plan(",
        "CompiledExecutable::LegacyPhysicalPlan",
    ];
    for file in rust_files(manifest.join("src")) {
        let text = fs::read_to_string(&file).expect("read rust source");
        for pattern in forbidden {
            assert_pattern_only_in_cfg_test_scope(&file, &text, pattern);
        }
    }
}

fn assert_pattern_only_in_cfg_test_scope(file: &Path, text: &str, pattern: &str) {
    let mut brace_depth = 0isize;
    let mut pending_cfg_test = false;
    let mut cfg_test_scopes = Vec::new();

    for (idx, line) in text.lines().enumerate() {
        let protected = pending_cfg_test || !cfg_test_scopes.is_empty();
        assert!(
            !line.contains(pattern) || protected,
            "{}:{} contains `{}` outside #[cfg(test)]",
            file.display(),
            idx + 1,
            pattern
        );

        if line.trim() == "#[cfg(test)]" {
            pending_cfg_test = true;
        }

        let opens = line.chars().filter(|ch| *ch == '{').count() as isize;
        let closes = line.chars().filter(|ch| *ch == '}').count() as isize;
        if pending_cfg_test && opens > 0 {
            cfg_test_scopes.push(brace_depth + opens);
            pending_cfg_test = false;
        }
        brace_depth += opens - closes;
        while cfg_test_scopes
            .last()
            .is_some_and(|scope_depth| brace_depth < *scope_depth)
        {
            cfg_test_scopes.pop();
        }
    }
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
