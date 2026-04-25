#!/usr/bin/env python3
# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

"""Reject regressions to infallible Vector copy/materialization/decode APIs."""

from __future__ import annotations

from pathlib import Path
import re
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[2]

CHECK_PREFIXES = (
    Path("crates/common/src"),
    Path("crates/execution/src"),
    Path("crates/function/src"),
    Path("crates/storage/src"),
)

VECTOR_COPY_FILES = {
    Path("crates/common/src/vector/copy.rs"),
    Path("crates/common/src/vector/vector_ops.rs"),
    Path("crates/common/src/vector/view.rs"),
    Path("crates/common/src/chunk/ops.rs"),
}

MATERIALIZATION_GUARD_FILES = VECTOR_COPY_FILES | {
    Path("crates/storage/src/row/raw/gather.rs"),
    Path("crates/storage/src/row/raw/scatter.rs"),
}

ARC_MAKE_MUT_GUARD_FILES = {
    Path("crates/common/src/vector/copy.rs"),
    Path("crates/common/src/vector/vector_ops.rs"),
    Path("crates/common/src/chunk/ops.rs"),
    Path("crates/storage/src/row/raw/gather.rs"),
    Path("crates/storage/src/row/raw/scatter.rs"),
}

SETTER_GUARD_PATHS = VECTOR_COPY_FILES | {
    Path("crates/storage/src/codec/vector_decoder.rs"),
    Path("crates/storage/src/column/partitioned.rs"),
    Path("crates/storage/src/mutation/updater.rs"),
    Path("crates/storage/src/mutation/upsert.rs"),
    Path("crates/storage/src/row/raw/collection.rs"),
    Path("crates/storage/src/row/raw/gather.rs"),
    Path("crates/storage/src/rowid_resolver.rs"),
    Path("crates/storage/src/write/memtable.rs"),
}

SETTER_GUARD_GLOBS = (
    "crates/storage/src/row/codec/*.rs",
)

BANNED_GLOBAL = {
    "Vector::copy_at call": re.compile(r"\.copy_at\s*\("),
    "Vector::copy_at definition": re.compile(r"\bpub\s+fn\s+copy_at\s*\("),
    "StringHeap allocation panic": re.compile(r"StringHeap allocation failed"),
    "list child growth panic": re.compile(r"list child growth allocation failed"),
}

BANNED_VECTOR_CHUNK_SYMBOLS = re.compile(
    r"\b(?:Vector|Chunk)::(?:copy_at|flatten|slice|merge|merge_full)\s*\("
)

BANNED_CORE_DEFINITIONS = re.compile(
    r"\bpub\s+fn\s+"
    r"(?:copy_at|flatten|slice|merge|merge_full|"
    r"to_view|to_varlen_view|to_array_view|decode_ref|decode_tree|decode)"
    r"\s*\("
)

BANNED_CORE_METHOD_CALLS = re.compile(
    r"(?<!try_)\.(?:flatten|slice|merge|merge_full)\s*\("
)

BANNED_DECODE_VIEW_WRAPPERS = re.compile(
    r"\.(?:decode|decode_ref|decode_tree|to_view|to_varlen_view|to_array_view)\s*\("
)

BANNED_MATERIALIZATION_EXPECTS = {
    "vector buffer allocation panic": re.compile(r"vector buffer allocation failed"),
    "decoded vector allocation panic": re.compile(r"decoded vector allocation failed"),
    "vector flatten allocation panic": re.compile(r"vector flatten allocation failed"),
    "validity allocation panic": re.compile(r"ValidityMask allocation failed"),
    "combined child selection allocation panic": re.compile(
        r"combined child selection allocation failed"
    ),
}

BANNED_ARC_MAKE_MUT = re.compile(r"\b(?:std::sync::)?Arc::make_mut\s*\(")

BANNED_SETTERS = re.compile(
    r"\.(?:set_count|set_len|set_null|set_cardinality)\s*\("
)
SELECTION_SET_LEN = re.compile(r"\b\w*(?:sel|selection)\w*\.set_len\s*\(")

SETTER_ALLOWLIST = {
    (
        Path("crates/common/src/vector/view.rs"),
        "result.set_len(count);",
    ): "SelectionVector pre-sized materialization; not a Vector/Chunk setter.",
}


def git_files(pattern: str) -> list[Path]:
    result = subprocess.run(
        ["git", "-C", str(ROOT), "ls-files", pattern],
        check=True,
        capture_output=True,
        text=True,
    )
    return [Path(line) for line in result.stdout.splitlines() if line]


def rust_files() -> list[Path]:
    return [
        path
        for path in git_files("crates/**/*.rs")
        if any(path.is_relative_to(prefix) for prefix in CHECK_PREFIXES)
    ]


def setter_guard_files() -> set[Path]:
    files = set(SETTER_GUARD_PATHS)
    for pattern in SETTER_GUARD_GLOBS:
        files.update(git_files(pattern))
    return files


def production_lines(path: Path) -> list[tuple[int, str]]:
    lines = path.read_text(encoding="utf-8").splitlines()
    result: list[tuple[int, str]] = []
    skip_rest = False
    for line_no, line in enumerate(lines, start=1):
        if line.strip() == "#[cfg(test)]":
            skip_rest = True
        if not skip_rest:
            result.append((line_no, line))
    return result


def all_lines(path: Path) -> list[tuple[int, str]]:
    return list(enumerate(path.read_text(encoding="utf-8").splitlines(), start=1))


def report(
    violations: list[str],
    rel_path: Path,
    line_no: int,
    name: str,
    line: str,
) -> None:
    violations.append(f"{rel_path}:{line_no}: banned {name}: {line.strip()}")


def main() -> int:
    violations: list[str] = []
    setter_files = setter_guard_files()

    for rel_path in rust_files():
        path = ROOT / rel_path

        for line_no, line in all_lines(path):
            for name, pattern in BANNED_GLOBAL.items():
                if pattern.search(line):
                    report(violations, rel_path, line_no, name, line)

        for line_no, line in production_lines(path):
            if BANNED_VECTOR_CHUNK_SYMBOLS.search(line):
                report(violations, rel_path, line_no, "legacy Vector/Chunk materialization call", line)
            if BANNED_DECODE_VIEW_WRAPPERS.search(line):
                report(violations, rel_path, line_no, "infallible vector decode/view wrapper", line)

            if rel_path in VECTOR_COPY_FILES:
                if BANNED_CORE_DEFINITIONS.search(line):
                    report(violations, rel_path, line_no, "legacy Vector/Chunk API definition", line)
                if BANNED_CORE_METHOD_CALLS.search(line):
                    report(violations, rel_path, line_no, "legacy Vector/Chunk materialization method", line)

            if rel_path in MATERIALIZATION_GUARD_FILES:
                if "default_allocator()" in line:
                    report(violations, rel_path, line_no, "default allocator fallback in materialization path", line)
                for name, pattern in BANNED_MATERIALIZATION_EXPECTS.items():
                    if pattern.search(line):
                        report(violations, rel_path, line_no, name, line)

            if rel_path in ARC_MAKE_MUT_GUARD_FILES and BANNED_ARC_MAKE_MUT.search(line):
                report(violations, rel_path, line_no, "Arc::make_mut in fallible copy path", line)

            if rel_path in setter_files and BANNED_SETTERS.search(line):
                key = (rel_path, line.strip())
                if key in SETTER_ALLOWLIST or SELECTION_SET_LEN.search(line):
                    continue
                report(violations, rel_path, line_no, "infallible Vector/Chunk setter in copy path", line)

    if violations:
        print("Vector copy/materialization fallibility guard failed:", file=sys.stderr)
        for violation in violations:
            print(f"  {violation}", file=sys.stderr)
        print(
            "\nUse try_copy_*, try_flatten/try_slice/try_set_* and fallible decode/view helpers.",
            file=sys.stderr,
        )
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
