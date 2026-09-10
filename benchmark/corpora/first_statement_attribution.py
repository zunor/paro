#!/usr/bin/env python3
# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

"""Derive auditable D1-Q/D6 attribution from a validated cold-plan report.

This consumes the existing diagnostic trace and optimizer diagnostic rows.  It
does not enable tracing, replay a plan, or participate in a normal C1 sample.
Missing trajectory or first-construction fields are represented explicitly
instead of being inferred from final plan identity or elapsed time.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any

from benchmark.corpora.benchmark_evidence import validate_statement_trace


RULE_METRIC_UNITS = {
    "attempts": "attempts",
    "insertions": "insertions",
}


def _content_digest(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _event_index(trace: dict[str, Any]) -> dict[str, dict[str, Any]]:
    events = trace.get("events")
    if not isinstance(events, list):
        raise ValueError("target trace has no events")
    index: dict[str, dict[str, Any]] = {}
    for event in events:
        name = event.get("event")
        if not isinstance(name, str) or name in index:
            raise ValueError(f"target trace has duplicate or invalid event: {name!r}")
        index[name] = event
    return index


def _event_payload(event: dict[str, Any] | None) -> dict[str, int | None] | None:
    if event is None:
        return None
    return {
        "elapsed_us": event.get("elapsed_us"),
        "duration_us": event.get("duration_us"),
        "value": event.get("value"),
    }


def _first_rule_values(events: dict[str, dict[str, Any]], rule: str) -> dict[str, int | None]:
    values: dict[str, int | None] = {}
    for phase in ("discovered", "matched", "applicable", "published"):
        event = events.get(f"rule.{rule}.first_{phase}_us")
        values[f"first_{phase}_us"] = event.get("value") if event else None
    return values


def _rule_attribution(
    diagnostics: list[dict[str, Any]], events: dict[str, dict[str, Any]]
) -> list[dict[str, Any]]:
    by_rule: dict[str, dict[str, dict[str, Any]]] = {}
    for row in diagnostics:
        if row.get("kind") not in {"transformation_rule", "transformation_rule_attempt"}:
            continue
        name = row.get("name")
        unit = row.get("metric_unit")
        if not isinstance(name, str) or unit not in RULE_METRIC_UNITS.values():
            continue
        by_rule.setdefault(name, {})[unit] = row

    output: list[dict[str, Any]] = []
    for name in sorted(by_rule):
        rows = by_rule[name]
        attempts = rows.get("attempts")
        insertions = rows.get("insertions")
        output.append(
            {
                "rule": name,
                "attempts": attempts.get("metric_value") if attempts else None,
                "insertions": insertions.get("metric_value") if insertions else None,
                # The diagnostic's elapsed field is accumulated over the same
                # rule attempt path; report it once, not once per metric row.
                "elapsed_ms": (
                    (attempts or insertions or {}).get("last_elapsed_us", 0) / 1000.0
                    if attempts or insertions
                    else None
                ),
                **_first_rule_values(events, name),
                "first_constructed_us": None,
                "first_inserted_us": None,
                "coverage": {
                    "first_constructed": "uncovered",
                    "first_inserted": "uncovered",
                    "reason": "current trace exposes counts and first published event only",
                },
            }
        )
    return output


def _component_times(diagnostics: list[dict[str, Any]]) -> dict[str, float]:
    names = {
        "semantic_normalization",
        "query_ir_construction",
        "direct_physical_search",
        "memo_exploration",
        "physical_extraction",
        "winner_verification",
    }
    return {
        row["name"]: float(row.get("last_elapsed_us", 0)) / 1000.0
        for row in diagnostics
        if row.get("name") in names
    }


def _milestones(events: dict[str, dict[str, Any]]) -> dict[str, Any]:
    names = (
        "first_safe_us",
        "first_optional_ready_us",
        "first_optional_selected_us",
        "first_logical_publication_us",
        "compiler_call_entry",
        "compiler_call_return",
        "compiler_return",
        "executable_image_frozen",
        "planning_state_released",
        "admission_entry",
        "resource_grant_published",
        "pipeline_dispatch_entry",
        "pipeline_initialized",
        "first_page_ready",
        "fetch_drain",
        "command_complete_sent",
        "commit_published",
        "statement_scope_return",
        "statement_complete",
    )
    return {name: _event_payload(events.get(name)) for name in names if name in events}


def _attribute_sample(
    sample: dict[str, Any], *, expected_process_id: int, expected_sample_id: str, expected_fp: int
) -> dict[str, Any]:
    traces = sample.get("target_statement_traces")
    if not isinstance(traces, list) or len(traces) != 1:
        raise ValueError("sample does not contain exactly one target statement trace")
    trace = traces[0]
    validate_statement_trace(
        trace,
        expected_process_id=expected_process_id,
        expected_sample_id=expected_sample_id,
        expected_query_fingerprint=expected_fp,
    )
    events = _event_index(trace)
    diagnostics = sample.get("diagnostics")
    if not isinstance(diagnostics, list):
        raise ValueError("sample has no optimizer diagnostic rows")
    return {
        "block": sample.get("block"),
        "status": sample.get("status"),
        "plan_sha256": sample.get("plan_sha256"),
        "optimizer_ms": sample.get("optimizer_ms"),
        "explain_wall_ms": sample.get("explain_wall_ms"),
        "components_ms": _component_times(diagnostics),
        "milestones": _milestones(events),
        "rules": _rule_attribution(diagnostics, events),
        "physical_decision_trajectory": {
            "status": "uncovered",
            "reason": "cold-planning trace records milestones and final plan identity, not every candidate transition",
        },
        "allocation": {
            "status": "uncovered",
            "reason": "sample was collected without allocation metrics",
        },
        "dependency_wait_publication": {
            "status": "partial",
            "task_kind_counts": {
                key: value
                for key, value in sample.get("counters", {}).items()
                if "task_registry" in key or "physical_subproblem" in key
            },
            "reason": "counts are available; CPU/wall/lock attribution is not",
        },
    }


def build_attribution(report: dict[str, Any], source_path: Path) -> dict[str, Any]:
    queries = report.get("queries")
    if not isinstance(queries, list) or len(queries) != 1:
        raise ValueError("attribution requires one query in the cold-planning report")
    query = queries[0]
    samples = query.get("samples")
    if not isinstance(samples, list) or not samples:
        raise ValueError("cold-planning report contains no samples")

    output_samples = []
    for sample in samples:
        server = sample.get("server") or {}
        output_samples.append(
            _attribute_sample(
                sample,
                expected_process_id=int(server["pid"]),
                expected_sample_id=str(server["statement_trace_sample_id"]),
                expected_fp=int(sample["trace_query_fingerprint"]),
            )
        )
    return {
        "schema_version": 1,
        "kind": "first_statement_attribution",
        "source_report": str(source_path.resolve()),
        "source_report_sha256": _content_digest(source_path),
        "query": {
            "name": query.get("name"),
            "path": query.get("path"),
            "sql_sha256": query.get("sql_sha256"),
        },
        "coverage": {
            "D1-Q": "partial",
            "D6": "phase-and-execution-profile-partial",
            "missing": [
                "exact physical candidate replay",
                "first constructed/inserted candidate event",
                "allocation bytes and per-task CPU/wait/lock wall attribution",
            ],
        },
        "samples": output_samples,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    report = json.loads(args.report.read_text(encoding="utf-8"))
    attribution = build_attribution(report, args.report)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(attribution, indent=2, ensure_ascii=False, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    print(json.dumps({"output": str(args.output), "samples": len(attribution["samples"])}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
