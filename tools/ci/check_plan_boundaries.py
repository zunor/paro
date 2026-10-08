#!/usr/bin/env python3
# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

"""Check normal/build dependency ownership, including target-specific edges.

Test-only optimizer fixtures may construct plans for execution tests. They must
not turn into a production dependency, directly or through another crate.
"""

from pathlib import Path
import re
import sys
import tomllib


def production_dependencies(manifest, workspace):
    tables = [manifest, *manifest.get("target", {}).values()]
    for table in tables:
        for kind in ("dependencies", "build-dependencies"):
            for alias, spec in table.get(kind, {}).items():
                if isinstance(spec, dict) and spec.get("workspace"):
                    spec = workspace.get(alias, spec)
                yield spec.get("package", alias) if isinstance(spec, dict) else alias


def dependency_path(graph, start, target):
    pending = [(start, [start])]
    visited = set()
    while pending:
        node, path = pending.pop()
        if node == target:
            return path
        if node in visited:
            continue
        visited.add(node)
        pending.extend((child, [*path, child]) for child in sorted(graph.get(node, [])))
    return None


def check(root):
    workspace = tomllib.loads((root / "Cargo.toml").read_text())["workspace"]["dependencies"]
    graph = {}
    for path in sorted((root / "crates").glob("*/Cargo.toml")):
        manifest = tomllib.loads(path.read_text())
        graph[manifest["package"]["name"]] = set(production_dependencies(manifest, workspace))
    errors = []
    for source, forbidden in (
        ("paro-planner", "paro-optimizer"),
        ("paro-planner", "paro-execution"),
        ("paro-execution", "paro-optimizer"),
    ):
        path = dependency_path(graph, source, forbidden)
        if path:
            errors.append(" -> ".join(path))
    errors.extend(check_optimizer_surface(root))
    return errors


def check_optimizer_surface(root):
    """Keep the public facade small; test fixtures are an explicit feature.

    This is a deliberately small source guard, not a Rust parser. rustc checks
    actual reachability; Cargo dependency checks above protect its consumers.
    """
    source = root / "crates/optimizer/src"
    facade = (source / "lib.rs").read_text()
    allowed = {
        "pub use optimizer::{OptimizedStatement, Optimizer};",
        "pub use diagnostics::work::begin as begin_optimizer_observation;",
        "pub mod test_support {",
    }
    exports = re.findall(r"^pub (?:use|mod|struct|enum|trait|fn|type|const) .*$", facade, re.M)
    errors = [f"unreviewed optimizer public surface: {item}" for item in exports if item not in allowed]
    if '#[cfg(feature = "test-support")]\npub mod test_support' not in facade:
        errors.append("optimizer fixtures must be gated by test-support")
    for path in sorted(source.rglob("*.rs")):
        if path.stem in {"misc", "helpers", "common"}:
            errors.append(f"optimizer module needs a responsibility name: {path.relative_to(root)}")
        lines = len(path.read_text().splitlines())
        if lines > 1500:
            print(f"warning: {path.relative_to(root)} has {lines} lines; consider a responsibility split", file=sys.stderr)
    if (source / "cascades").exists():
        errors.append("production optimizer has a second global search engine")
    return errors


if __name__ == "__main__":
    errors = check(Path(__file__).resolve().parents[2])
    if errors:
        print("Plan ownership dependency violations:\n" + "\n".join(errors), file=sys.stderr)
        sys.exit(1)
    print("Plan ownership: planner and execution are independent of optimizer implementation")
