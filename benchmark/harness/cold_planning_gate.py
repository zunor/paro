#!/usr/bin/env python3
# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

"""Strict fresh-process planning regression gate (not an execution benchmark)."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import statistics
from pathlib import Path
from typing import Any

try:
    from .receipt_contract import (
        EVIDENCE_SCHEMA_VERSION,
        ReceiptContractError,
        validate_benchmark_payload,
        validate_campaign_summary,
        validate_compile_document,
    )
except ImportError:  # pragma: no cover - documented script invocation
    from receipt_contract import (  # type: ignore[no-redef]
        EVIDENCE_SCHEMA_VERSION,
        ReceiptContractError,
        validate_benchmark_payload,
        validate_campaign_summary,
        validate_compile_document,
    )

VERSION = EVIDENCE_SCHEMA_VERSION
COUNTERS = ("search_complete", "memo_group_count", "memo_logical_expression_count",
            "memo_physical_expression_count", "settlement_local_hit_count", "settlement_local_miss_count",
            "search_rule_failure_count", "search_deadline_reached")
METRICS = ("explain_wall_ms", "optimizer_ms", "peak_rss_bytes")


def _owned_json(root: Path, relative: str) -> dict[str, Any]:
    path = (root / relative).resolve()
    try:
        path.relative_to(root.resolve())
    except ValueError as error:
        raise ValueError("RunOutput reference escapes its run root") from error
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError(f"RunOutput reference is unreadable: {relative}") from error
    if not isinstance(value, dict):
        raise ValueError(f"RunOutput reference is not an object: {relative}")
    return value


def load_run_output_report(path: Path) -> tuple[dict[str, Any], Path]:
    """Read one sealed v3 RunOutput and build the bounded gate view.

    The gate never consumes a free-standing legacy report.  It reads the
    sealed manifest, then exactly one completed cell attempt per registered
    cell, and projects the producer-owned query payload without copying raw
    captures into the campaign control plane.
    """
    root = path.resolve()
    if root.is_file():
        if root.name not in {"campaign.json", "manifest.json"}:
            raise ValueError("cold-planning gate requires a v3 RunOutput root")
        root = root.parent
    if not root.is_dir():
        raise ValueError("cold-planning gate RunOutput root does not exist")
    manifest = _owned_json(root, "manifest.json")
    if manifest.get("schema_version") != EVIDENCE_SCHEMA_VERSION:
        raise ValueError("unsupported RunOutput schema version")
    if manifest.get("status") != "Completed":
        raise ValueError(f"RunOutput is not completed: {manifest.get('status')!r}")
    try:
        validate_campaign_summary(_owned_json(root, "campaign.json"), manifest)
    except (OSError, ValueError, ReceiptContractError) as error:
        raise ValueError(f"CampaignSummary does not match the sealed manifest: {error}") from error
    registration = manifest.get("registration")
    cells = registration.get("cells") if isinstance(registration, dict) else None
    if not isinstance(cells, list) or not cells:
        raise ValueError("RunOutput has no registered cells")
    attempts = manifest.get("attempts")
    if not isinstance(attempts, list):
        raise ValueError("RunOutput manifest has no attempt index")

    reports: list[dict[str, Any]] = []
    shared_configuration: dict[str, Any] | None = None
    shared_evidence: dict[str, Any] | None = None
    for cell in cells:
        if not isinstance(cell, dict):
            raise ValueError("RunOutput cell index is malformed")
        cell_attempts = [
            attempt for attempt in attempts
            if attempt.get("query_case") == cell.get("query_case")
            and attempt.get("arm_id") == cell.get("arm_id")
        ]
        if len(cell_attempts) != 1:
            raise ValueError("cold-planning gate cannot guess among cell attempts")
        attempt = cell_attempts[0]
        if attempt.get("status") != "Completed" or not isinstance(attempt.get("result"), str):
            raise ValueError("cold-planning cell attempt is not completed")
        payload = _owned_json(root, attempt["result"])
        try:
            validate_benchmark_payload(payload)
        except ReceiptContractError as error:
            raise ValueError(f"cell payload violates the shared contract: {error}") from error
        query_payload = payload.get("query")
        if not isinstance(query_payload, dict):
            raise ValueError("cold-planning cell has no query payload")
        if query_payload.get("schema_version") != EVIDENCE_SCHEMA_VERSION:
            raise ValueError("cold-planning query envelope is not on the current schema")
        configuration = query_payload.get("configuration")
        evidence = query_payload.get("evidence")
        query = query_payload.get("query")
        if not isinstance(configuration, dict) or not isinstance(evidence, dict) \
                or not isinstance(query, dict):
            raise ValueError("cold-planning cell payload is incomplete")
        if shared_configuration is None:
            shared_configuration = configuration
            shared_evidence = evidence
        elif configuration != shared_configuration or evidence != shared_evidence:
            raise ValueError("cold-planning cells have incomparable configuration or provenance")
        reports.append(query)
    assert shared_configuration is not None and shared_evidence is not None
    return {
        "schema_version": VERSION,
        "compile_evidence_schema_version": EVIDENCE_SCHEMA_VERSION,
        "configuration": shared_configuration,
        "evidence": shared_evidence,
        "queries": reports,
    }, root


def positive(value: Any) -> float:
    if isinstance(value, bool) or not isinstance(value, (float, int)):
        raise ValueError("measurement must be numeric")
    if not math.isfinite(value) or value <= 0:
        raise ValueError("measurement must be finite and positive")
    return float(value)


def _sample_compile_document(
    sample: dict[str, Any], *, report_root: Path | None
) -> dict[str, Any]:
    document = sample.get("compile_document")
    if isinstance(document, dict) and document.get("status") == "Captured":
        if report_root is None:
            raise ValueError("compile capture reference cannot be resolved without report path")
        relative = document.get("path")
        expected_sha = document.get("sha256")
        if not isinstance(relative, str) or not isinstance(expected_sha, str):
            raise ValueError("compile capture reference is incomplete")
        capture = (report_root / relative).resolve()
        try:
            capture.relative_to(report_root.resolve())
        except ValueError as error:
            raise ValueError("compile capture escapes its owned attempt") from error
        raw = capture.read_bytes()
        actual_sha = hashlib.sha256(raw).hexdigest()
        if actual_sha != expected_sha:
            raise ValueError("compile capture identity does not match its reference")
        try:
            loaded = json.loads(raw)
        except json.JSONDecodeError as error:
            raise ValueError("compile capture is not valid JSON") from error
        if not isinstance(loaded, dict):
            raise ValueError("compile capture is not an object")
        return loaded
    if not isinstance(document, dict):
        raise ValueError("sample has no compile document")
    return document


def validate(
    report: dict[str, Any], *, report_root: Path | None = None
) -> dict[str, list[dict[str, Any]]]:
    if report.get("schema_version") != VERSION:
        raise ValueError("unsupported cold planning report version")
    if report.get("compile_evidence_schema_version") != EVIDENCE_SCHEMA_VERSION:
        raise ValueError("cold planning report is not on the current compile evidence contract")
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
            != "EXPLAIN (COMPILE, DETAIL, FORMAT JSON)"
            or report["configuration"].get("compile_metrics_source")
            != "EXPLAIN (COMPILE, DETAIL, FORMAT JSON) typed document"):
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
            if sample.get("compile_metrics_source") != report["configuration"]["compile_metrics_source"]:
                raise ValueError("compile metrics are not sourced from the typed document")
            if sample.get("omitted_search_counters", 0) != 0:
                raise ValueError("compile search counters are incomplete")
            if not sample.get("plan_structure_id"):
                raise ValueError("missing typed plan structure identity")
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
            document = _sample_compile_document(sample, report_root=report_root)
            try:
                validate_compile_document(document)
            except ReceiptContractError as error:
                raise ValueError(str(error)) from error
            if sample.get("compile_query_fingerprint") is None:
                raise ValueError("compile document lacks query identity")
        result[query["name"]] = samples
    return result


def evaluate(
    report: dict[str, Any],
    baseline: dict[str, Any] | None = None,
    max_ratio: float = 1.15,
    *,
    report_root: Path | None = None,
    baseline_root: Path | None = None,
) -> dict[str, Any]:
    positive(max_ratio)
    samples = validate(report, report_root=report_root)
    summary = {name: {metric: {"median": statistics.median(s[metric] for s in block),
                             "maximum": max(s[metric] for s in block)}
                      for metric in METRICS} for name, block in samples.items()}
    result: dict[str, Any] = {"passed": True, "summary": summary, "regressions": []}
    if baseline is None:
        return result
    previous = validate(baseline, report_root=baseline_root)
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
    report, report_root = load_run_output_report(args.report)
    baseline = None
    baseline_root = None
    if args.baseline:
        baseline, baseline_root = load_run_output_report(args.baseline)
    result = evaluate(
        report,
        baseline,
        args.max_ratio,
        report_root=report_root,
        baseline_root=baseline_root,
    )
    print(json.dumps(result, indent=2, allow_nan=False))
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
