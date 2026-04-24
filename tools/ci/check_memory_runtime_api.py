# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

"""Reject legacy infallible Chunk/Vector allocation helpers."""

from __future__ import annotations

from pathlib import Path
import re
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[2]

ALLOWLIST = {
    Path("crates/common/src/test_utils.rs"),
}

BANNED = {
    "Chunk::new": re.compile(r"\bChunk::new\s*\("),
    "Chunk::initialize": re.compile(r"\bChunk::initialize\s*\("),
    "Chunk::init_empty": re.compile(r"\bChunk::init_empty\s*\("),
    "Chunk::append": re.compile(r"\bChunk::append\s*\("),
    "Chunk::deep_copy_with_allocator": re.compile(r"\bChunk::deep_copy_with_allocator\s*\("),
    "Vector::new": re.compile(r"\bVector::new\s*\("),
    "Vector::with_capacity": re.compile(r"\bVector::with_capacity\s*\("),
    "Vector::new_array": re.compile(r"\bVector::new_array\s*\("),
    "Vector::from_array": re.compile(r"\bVector::from_array\s*\("),
    "Vector::constant": re.compile(r"\bVector::constant(?:\s*::\s*<[^>]+>)?\s*\("),
    "Vector::constant_null": re.compile(r"\bVector::constant_null\s*\("),
    "Vector::sequence": re.compile(r"\bVector::sequence\s*\("),
    "Vector::dictionary": re.compile(r"\bVector::dictionary\s*\("),
    "Vector::with_dictionary": re.compile(r"\bVector::with_dictionary\s*\("),
    "Vector::from_i64": re.compile(r"\bVector::from_i64\s*\("),
    "Vector::from_i32": re.compile(r"\bVector::from_i32\s*\("),
    "Vector::from_f64": re.compile(r"\bVector::from_f64\s*\("),
    "Vector::from_f32": re.compile(r"\bVector::from_f32\s*\("),
    "Vector::from_bool": re.compile(r"\bVector::from_bool\s*\("),
    "Vector::from_strings": re.compile(r"\bVector::from_strings\s*\("),
    "Vector::from_embeddings": re.compile(r"\bVector::from_embeddings\s*\("),
    "Vector::*_with_allocator": re.compile(r"\bVector::[A-Za-z0-9_]*_with_allocator\s*(?:::<[^>]+>)?\s*\("),
    "SelectionVector::with_capacity": re.compile(r"\bSelectionVector::with_capacity\s*\("),
    "SelectionVector::from_indices": re.compile(r"\bSelectionVector::from_indices\s*\("),
    "SelectionVector::incremental": re.compile(r"\bSelectionVector::incremental\s*\("),
    "SelectionVector::constant": re.compile(r"\bSelectionVector::constant\s*\("),
}


def rust_files() -> list[Path]:
    result = subprocess.run(
        ["git", "-C", str(ROOT), "ls-files", "crates/**/*.rs"],
        check=True,
        capture_output=True,
        text=True,
    )
    return [Path(line) for line in result.stdout.splitlines() if line]


def main() -> int:
    violations: list[str] = []
    for rel_path in rust_files():
        if rel_path in ALLOWLIST:
            continue
        path = ROOT / rel_path
        for line_no, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
            for name, pattern in BANNED.items():
                if pattern.search(line):
                    violations.append(f"{rel_path}:{line_no}: banned {name}: {line.strip()}")

    if violations:
        print("Legacy Chunk/Vector allocation helpers are not allowed:", file=sys.stderr)
        for violation in violations:
            print(f"  {violation}", file=sys.stderr)
        print(
            "\nUse allocator-explicit try_* APIs in production and paro_common::test_utils in tests.",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
