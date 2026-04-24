#!/usr/bin/env python3
# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

"""Guard Phase 3 memory-runtime migrations from regressing to raw retained containers."""

from __future__ import annotations

from pathlib import Path
import re
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[2]

PHASE3_BANNED: dict[Path, dict[str, re.Pattern[str]]] = {
    Path("crates/execution/src/operator/scan/column_data_scan.rs"): {
        "MaterializedChunkCollection raw Vec<Chunk>": re.compile(r"Mutex\s*<\s*Vec\s*<\s*Chunk\s*>\s*>"),
    },
    Path("crates/execution/src/operator/join/left_delim_join.rs"): {
        "delim raw HashSet": re.compile(r"(?<!Accounted)HashSet\s*<\s*DelimKey\s*>"),
    },
    Path("crates/execution/src/operator/join/right_delim_join.rs"): {
        "delim raw HashSet": re.compile(r"(?<!Accounted)HashSet\s*<\s*DelimKey\s*>"),
    },
    Path("crates/execution/src/operator/join/cross_product.rs"): {
        "cross product raw Vec<Chunk>": re.compile(r"Vec\s*<\s*Chunk\s*>"),
        "cross product snapshot clone": re.compile(r"snapshot\s*\("),
    },
    Path("crates/execution/src/operator/join/piecewise_merge_join.rs"): {
        "piecewise payload raw Vec<Chunk>": re.compile(r"payload_chunks\s*:\s*Vec\s*<\s*Chunk\s*>"),
        "piecewise key raw Vec<Value>": re.compile(r"key_values\s*:\s*Vec\s*<\s*Value\s*>"),
        "piecewise location raw Vec": re.compile(r"key_locations\s*:\s*Vec\s*<\s*SortedRowLocation\s*>"),
    },
}

GRAPH_TMM_DIRECT_PATTERN = re.compile(r"TemporaryMemoryState|temporary_memory_manager\s*\(")
GRAPH_RAW_WORKSET_FIELD = re.compile(
    r"^\s*(?:"
    r"forward_neighbor_scratch|backward_neighbor_scratch|"
    r"lane_seen_scratch|lane_visit_scratch|lane_visit_next_scratch|"
    r"seen|visited_depth|terminal_edge_to_root|visit|visit_next|"
    r"frontier|next_frontier|input_vals|next_seen|rows|path_rows"
    r")\s*:\s*Vec\s*<",
    re.MULTILINE,
)


def git_files(pattern: str) -> list[Path]:
    result = subprocess.run(
        ["git", "-C", str(ROOT), "ls-files", pattern],
        check=True,
        capture_output=True,
        text=True,
    )
    return [Path(line) for line in result.stdout.splitlines() if line]


def main() -> int:
    violations: list[str] = []

    for rel_path, banned in PHASE3_BANNED.items():
        text = (ROOT / rel_path).read_text(encoding="utf-8").split("#[cfg(test)]", 1)[0]
        for name, pattern in banned.items():
            for line_no, line in enumerate(text.splitlines(), start=1):
                if pattern.search(line):
                    violations.append(f"{rel_path}:{line_no}: banned {name}: {line.strip()}")

    for rel_path in git_files("crates/execution/src/operator/graph/*.rs"):
        text = (ROOT / rel_path).read_text(encoding="utf-8")
        for line_no, line in enumerate(text.splitlines(), start=1):
            if GRAPH_TMM_DIRECT_PATTERN.search(line):
                violations.append(f"{rel_path}:{line_no}: graph operators must not touch TMM: {line.strip()}")
        production_text = text.split("#[cfg(test)]", 1)[0]
        for match in GRAPH_RAW_WORKSET_FIELD.finditer(production_text):
            line_no = production_text.count("\n", 0, match.start()) + 1
            violations.append(
                f"{rel_path}:{line_no}: graph retained workset field must be accounted: {match.group(0).strip()}"
            )

    if violations:
        print("Phase 3 memory-runtime migration regressions found:", file=sys.stderr)
        for violation in violations:
            print(f"  {violation}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
