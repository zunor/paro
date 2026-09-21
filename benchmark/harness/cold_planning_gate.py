#!/usr/bin/env python3
# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

"""Strict fresh-process planning regression gate (not an execution benchmark)."""

from __future__ import annotations

import argparse
import json
import math
import statistics
from pathlib import Path
from typing import Any

from .receipt_contract import ReceiptContractError, validate_compile_document

VERSION = 6
COUNTERS = ("search_complete", "memo_group_count", "memo_logical_expression_count",
            "memo_physical_expression_count", "settlement_local_hit_count", "settlement_local_miss_count",
            "search_rule_failure_count", "search_deadline_reached")
METRICS = ("explain_wall_ms", "optimizer_ms", "peak_rss_bytes")


def positive(value: Any) -> float:
    if isinstance(value, bool) or not isinstance(value, (float, int)):
        raise ValueError("measurement must be numeric")
    if not math.isfinite(value) or value <= 0:
        raise ValueError("measurement must be finite and positive")
    return float(value)


def validate(report: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    if report.get("schema_version") != VERSION:
        raise ValueError("unsupported cold planning report version")
    if report.get("invalidated"):
        raise ValueError("measurement provenance was invalidated")
    evidence = report["evidence"]
    runtime_environment = report["configuration"]["runtime_environment"]
    if set(runtime_environment) != {"RUST_LOG", "PARO_STATEMENT_TRACE"} \
            or runtime_environment.get("PARO_STATEMENT_TRACE") != "0":
        raise ValueError("compile-document diagnostic configuration is not explicit")
    if (report["configuration"].get("cohort") != "diagnostic"
            or report["configuration"].get("trace_mode") != "off"
            or report["configuration"].get("compile_document")
            != "EXPLAIN (COMPILE, DETAIL, FORMAT JSON)"):
        raise ValueError("cold planning gate requires the compile-document diagnostic cohort")
    for value in (evidence["build"]["binary_sha256"], evidence["build"]["source"]["commit"],
                  evidence["build"]["source"]["working_tree_sha256"], evidence["harness_sha256"],
                  evidence["dataset_sha256"]):
        if not isinstance(value, str) or not value:
            raise ValueError("missing build, harness, source or dataset provenance")
    queries = report["queries"]
    if not queries or len({q["name"] for q in queries}) != len(queries):
        raise ValueError("query manifest must be nonempty and unique")
    expected = report["configuration"]["process_blocks"]
    if not isinstance(expected, int) or expected < 1:
        raise ValueError("invalid process block count")
    result = {}
    snapshots = set()
    for query in queries:
        if not query.get("sql_sha256"):
            raise ValueError("missing SQL identity")
        samples = query["samples"]
        if len(samples) != expected or {s["block"] for s in samples} != set(range(expected)):
            raise ValueError("incomplete or duplicate fresh-process blocks")
        for sample in samples:
            if sample.get("status") != "ok" or not sample.get("server", {}).get("pid"):
                raise ValueError(f"unsuccessful sample: {query['name']}")
            if sample["server"]["sha256"] != evidence["build"]["binary_sha256"]:
                raise ValueError("sample did not run the attested binary")
            snapshot = sample["server"].get("input_snapshot") or {}
            directory = sample["server"].get("data_dir")
            if (snapshot.get("policy") != "private_copy_per_process"
                    or snapshot.get("seed_path") != evidence["dataset_path"]
                    or snapshot.get("seed_sha256") != evidence["dataset_sha256"]
                    or snapshot.get("initial_sha256") != evidence["dataset_sha256"]
                    or not directory or directory == evidence["dataset_path"]
                    or directory in snapshots):
                raise ValueError("sample has no independent verified seed snapshot")
            snapshots.add(directory)
            for metric in METRICS:
                positive(sample[metric])
            if sample["optimizer_ms"] > sample["explain_wall_ms"] * 1.01:
                raise ValueError("optimizer time exceeds its enclosing statement")
            if not sample.get("plan_sha256"):
                raise ValueError("missing plan evidence")
            for counter in COUNTERS:
                value = sample["counters"][counter]
                if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                    raise ValueError("invalid search counter")
            if sample["counters"]["search_complete"] not in (0, 1):
                raise ValueError("invalid completion state")
            if sample["counters"]["search_rule_failure_count"]:
                raise ValueError("advisory rule failure in a performance sample")
            if sample["counters"]["search_deadline_reached"]:
                raise ValueError("deadline-limited search is not qualifying latency evidence")
            document = sample.get("compile_document")
            try:
                validate_compile_document(document)
            except ReceiptContractError as error:
                raise ValueError(str(error)) from error
            if sample.get("compile_query_fingerprint") is None:
                raise ValueError("compile document lacks query identity")
        result[query["name"]] = samples
    return result


def evaluate(report: dict[str, Any], baseline: dict[str, Any] | None = None,
             max_ratio: float = 1.15) -> dict[str, Any]:
    positive(max_ratio)
    samples = validate(report)
    summary = {name: {metric: {"median": statistics.median(s[metric] for s in block),
                             "maximum": max(s[metric] for s in block)}
                      for metric in METRICS} for name, block in samples.items()}
    result: dict[str, Any] = {"passed": True, "summary": summary, "regressions": []}
    if baseline is None:
        return result
    previous = validate(baseline)
    if report["configuration"] != baseline["configuration"]:
        raise ValueError("measurement settings differ")
    for key in ("dataset_sha256", "harness_sha256", "machine"):
        if report["evidence"][key] != baseline["evidence"][key]:
            raise ValueError(f"incomparable {key}")
    identities = lambda value: {q["name"]: q["sql_sha256"] for q in value["queries"]}
    if identities(report) != identities(baseline):
        raise ValueError("query coverage or SQL identity differs")
    if report["configuration"]["process_blocks"] < 3:
        raise ValueError("qualifying comparison requires at least three fresh processes")
    for name, block in samples.items():
        for metric in METRICS:
            old = statistics.median(s[metric] for s in previous[name])
            new = summary[name][metric]["median"]
            if new > old * max_ratio:
                result["regressions"].append(f"{name}: {metric} {new / old:.3f}x")
        # Faster incomplete search is not silently accepted as optimization.
        if min(s["counters"]["search_complete"] for s in block) < min(
                s["counters"]["search_complete"] for s in previous[name]):
            result["regressions"].append(f"{name}: lost complete search")
        reasons = {key for s in block for key in s["counters"] if key.startswith("budget_exhaustion_")}
        for reason in reasons:
            if max(s["counters"].get(reason, 0) for s in block) > max(
                    s["counters"].get(reason, 0) for s in previous[name]):
                result["regressions"].append(f"{name}: increased {reason}")
    result["passed"] = not result["regressions"]
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--baseline", type=Path)
    parser.add_argument("--max-ratio", type=float, default=1.15)
    args = parser.parse_args()
    result = evaluate(json.loads(args.report.read_text()),
                      json.loads(args.baseline.read_text()) if args.baseline else None,
                      args.max_ratio)
    print(json.dumps(result, indent=2, allow_nan=False))
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
