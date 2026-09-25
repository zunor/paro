#!/usr/bin/env python3
# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

"""Collect known-cardinality SQL boundaries in a fresh owned Paro database.

This is a plan-quality gate, not a performance comparator. Ordinary SELECT
provides the actual row count, checked against the independently authored
fixture oracle. EXPLAIN JSON provides estimates for that same named boundary.
We deliberately do not zip runtime pipeline IDs with logical EXPLAIN nodes:
those are different coordinate systems.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import tempfile
import tomllib
from pathlib import Path
from typing import Any

import psycopg
from psycopg import sql

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from harness.executor import _split_sql_statements
from harness.quality_gate import evaluate
from corpora.benchmark_evidence import (ManagedParoServer, build_benchmark_server,
                                       content_digest, repository_identity)


def digest(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def plan_metrics(root: dict[str, Any]) -> dict[str, int]:
    counts: dict[str, int] = {}
    pending = [root]
    while pending:
        node = pending.pop()
        operator = node["operator"]
        counts[operator] = counts.get(operator, 0) + 1
        children = node.get("children", [])
        if not isinstance(children, list):
            raise ValueError("EXPLAIN children must be an array")
        pending.extend(children)
    return {"aggregate_nodes": counts.get("AGGREGATE", 0),
            "materialized_cte_nodes": counts.get("MATERIALIZED_CTE", 0)}


def plan_occurrences(root: dict[str, Any], selected: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    """Capture structural plan occurrences without using runtime coordinates."""
    occurrences: list[dict[str, Any]] = []

    def visit(node: dict[str, Any], path: tuple[int, ...]) -> None:
        properties = node.get("properties", {})
        if not isinstance(properties, dict):
            raise ValueError("EXPLAIN properties must be an object")
        occurrence = {
            "path": list(path),
            "physical_node_id": node.get("node_id"),
            "logical_node_id": node.get("logical_node_id"),
            "operator": node.get("operator"),
            "relation": node.get("relation"),
            "estimated_rows": node.get("estimated_rows"),
            "column_ids": properties.get("Column IDs", []),
            "selected": node is selected,
        }
        if not isinstance(occurrence["column_ids"], list):
            raise ValueError("EXPLAIN Column IDs must be an array")
        occurrences.append(occurrence)
        children = node.get("children", [])
        if not isinstance(children, list):
            raise ValueError("EXPLAIN children must be an array")
        for index, child in enumerate(children):
            if not isinstance(child, dict):
                raise ValueError("EXPLAIN child must be an object")
            visit(child, (*path, index))

    visit(root, ())
    return occurrences


def validate_selector(selector: dict[str, Any]) -> None:
    if not isinstance(selector, dict):
        raise ValueError("semantic boundary selector must be an object")
    if "alternatives" in selector:
        alternatives = selector["alternatives"]
        if set(selector) != {"alternatives"} or not isinstance(alternatives, list) or not alternatives:
            raise ValueError("boundary alternatives must be a nonempty list with no sibling fields")
        for alternative in alternatives:
            validate_selector(alternative)
        return
    if (not isinstance(selector.get("operator"), str)
            or set(selector) - {"operator", "relation", "properties", "child"}
            or not isinstance(selector.get("properties", {}), dict)):
        raise ValueError(f"invalid semantic boundary selector: {selector}")
    if "child" in selector:
        validate_selector(selector["child"])


def matches_boundary(node: dict[str, Any], selector: dict[str, Any]) -> bool:
    if "alternatives" in selector:
        return any(matches_boundary(node, alternative) for alternative in selector["alternatives"])
    if (node.get("operator") != selector["operator"]
            or ("relation" in selector and node.get("relation") != selector["relation"])
            or any(node.get("properties", {}).get(key) != value
                   for key, value in selector.get("properties", {}).items())):
        return False
    children = node.get("children", [])
    return "child" not in selector or (len(children) == 1 and matches_boundary(children[0], selector["child"]))


def select_boundary(root: dict[str, Any], selector: dict[str, Any] | None) -> dict[str, Any]:
    if selector is None:
        return root
    validate_selector(selector)
    matches, pending = [], [root]
    while pending:
        node = pending.pop()
        if matches_boundary(node, selector):
            matches.append(node)
        pending.extend(node.get("children", []))
    if len(matches) != 1:
        raise ValueError(f"semantic boundary must match exactly once, got {len(matches)}: {selector}")
    return matches[0]


def capture(connection: Any, case: dict[str, Any]) -> dict[str, Any]:
    query = case["sql"].strip().rstrip(";")
    document = json.loads(connection.execute("EXPLAIN " + query + " FORMAT JSON", prepare=False).fetchone()[0])
    if document.get("format_version") != 2:
        raise ValueError("unsupported EXPLAIN format")
    root = document["plan"]
    selected = select_boundary(root, case.get("selector"))
    occurrences = plan_occurrences(root, selected)
    selected_occurrences = [item for item in occurrences if item["selected"]]
    if len(selected_occurrences) != 1:
        raise ValueError("selected semantic boundary has no unique plan occurrence")
    result_rows = len(connection.execute(query, prepare=False).fetchall())
    expected_result = case.get("expected_result_rows", case["expected_rows"])
    if result_rows != expected_result:
        raise ValueError(f"{case['query']}: expected result {expected_result} rows, got {result_rows}")
    oracle = case.get("oracle_sql")
    if case.get("selector") and not oracle:
        raise ValueError("an internal semantic boundary requires its own cardinality oracle")
    actual = len(connection.execute(oracle, prepare=False).fetchall()) if oracle else result_rows
    if actual != case["expected_rows"]:
        raise ValueError(f"{case['query']}: expected {case['expected_rows']} rows, got {actual}")
    selected_occurrence = selected_occurrences[0]
    return {"query": case["query"], "sql_sha256": digest(query), "status": "ok",
            "operator": selected["operator"], "estimated_rows": selected["estimated_rows"],
            "actual_rows": actual, "result_rows": result_rows, "boundary_node_id": selected["node_id"],
            "boundary_logical_node_id": selected.get("logical_node_id"),
            "plan": document, "plan_metrics": plan_metrics(root),
            "plan_occurrences": occurrences,
            "statistics_evidence": {
                "schema_version": 1,
                "source": "independent_semantic_boundary_oracle",
                "coordinate_system": "physical_plan_occurrence_with_logical_node_id",
                "physical_node_id": selected_occurrence["physical_node_id"],
                "logical_node_id": selected_occurrence["logical_node_id"],
                "estimated_rows": selected["estimated_rows"],
                "actual_rows": actual,
            }}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--baseline", type=Path)
    parser.add_argument("--listen", default="127.0.0.1:6442")
    parser.add_argument("--build-jobs", type=int, default=4)
    args = parser.parse_args()
    repo = Path(__file__).resolve().parents[2]
    corpus = Path(__file__).with_suffix("")
    cases_path, setup_path = corpus / "cases.toml", corpus / "setup.sql"
    cases = tomllib.loads(cases_path.read_text())["cases"]
    tracked = [Path(__file__).resolve(), Path(__file__).with_name("benchmark_evidence.py"),
               repo / "benchmark/harness/quality_gate.py", repo / "benchmark/harness/executor.py"]
    original_hashes = {str(path): content_digest(path) for path in [*tracked, cases_path, setup_path]}
    settings = {"threads": 4, "memory_limit": "1GB", "optimizer_verify": True,

                "statement_timeout_ms": 30000}
    binary, build = build_benchmark_server(repo, args.build_jobs)
    report: dict[str, Any] = {
        "schema_version": 1,
        "contract": {
            "corpus_sha256": digest(original_hashes[str(cases_path)] + original_hashes[str(setup_path)]),
            "collector_sha256": digest("".join(original_hashes[str(path)] for path in tracked)),
            "settings": settings,
            "cases": [{"query": case["query"], "sql_sha256": digest(case["sql"].strip().rstrip(";")),
                       "expected_rows": case["expected_rows"],
                       "expected_result_rows": case.get("expected_result_rows", case["expected_rows"]),
                       "selector": case.get("selector"),
                       "oracle_sql_sha256": digest(case["oracle_sql"]) if case.get("oracle_sql") else None,
                       "nonincreasing_metrics": case.get("nonincreasing_metrics", [])} for case in cases],
        },
        "evidence": {"build": build, "files": original_hashes},
        "observations": [],
    }
    args.report.parent.mkdir(parents=True, exist_ok=True)
    # A failed startup/setup must not leave an older successful report at the
    # requested path. This incomplete checkpoint deliberately fails the gate.
    args.report.write_text(json.dumps(report, indent=2, sort_keys=True, allow_nan=False) + "\n")
    with tempfile.TemporaryDirectory(prefix="paro-plan-quality-") as temporary:
        data = Path(temporary) / "data"
        data.mkdir()
        with ManagedParoServer(binary, data, args.listen, args.report.with_suffix(".parod.log"),
                               max_memory=settings["memory_limit"], threads=settings["threads"]) as server:
            report["evidence"]["server"] = server.identity()
            host, port = args.listen.rsplit(":", 1)
            with psycopg.connect(host=host, port=int(port), dbname="postgres", user="paro",
                                 autocommit=True, connect_timeout=10) as connection:
                connection.execute("SET optimizer_verify=true")
                connection.execute("SET threads=4")
                connection.execute("SET memory_limit='1GB'")
                connection.execute("SET statement_timeout=30000")
                for statement in _split_sql_statements(setup_path.read_text()):
                    connection.execute(statement, prepare=False)
                for case in cases:
                    try:
                        result = capture(connection, case)
                    except Exception as error:
                        result = {"query": case["query"], "status": "error", "error": str(error)}
                    report["observations"].append(result)
                    print(f"{case['query']}: {result['status']}; "
                          f"estimate={result.get('estimated_rows')} actual={result.get('actual_rows')}; "
                          f"{result.get('error', '')}", flush=True)
    if (repository_identity(repo) != build["source"] or content_digest(binary) != build["binary_sha256"]
            or any(content_digest(Path(path)) != identity for path, identity in original_hashes.items())):
        report["invalidated"] = "source, corpus, collector or binary changed during collection"
    args.report.write_text(json.dumps(report, indent=2, sort_keys=True, allow_nan=False) + "\n")
    try:
        result = evaluate(report, json.loads(args.baseline.read_text()) if args.baseline else None)
    except (ValueError, TypeError, KeyError) as error:
        result = {"passed": False, "error": str(error)}
    print(json.dumps(result, indent=2, sort_keys=True, allow_nan=False))
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
