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
import statistics
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


def _q_error(estimated: Any, actual: Any) -> float | None:
    if not isinstance(estimated, (int, float)) or isinstance(estimated, bool):
        return None
    if not isinstance(actual, (int, float)) or isinstance(actual, bool):
        return None
    if estimated < 0 or actual < 0 or (estimated == 0 and actual == 0):
        return 1.0 if estimated == actual else None
    if estimated == 0 or actual == 0:
        return None
    return max(float(estimated) / float(actual), float(actual) / float(estimated))


def _semantic_roles(node: dict[str, Any]) -> list[str]:
    """Return structural hints, never proof of a statistics domain.

    The labels make the Q11 diagnostic mapping reviewable without making the
    production optimizer depend on a query name.  Condition-domain evidence
    remains explicitly uncovered until an independent E8 observation supplies
    it.
    """

    operator = node.get("operator")
    properties = node.get("properties", {})
    if not isinstance(properties, dict):
        properties = {}
    relation = node.get("relation")
    text = " ".join(
        str(value)
        for value in (
            relation,
            properties.get("Join Condition"),
            properties.get("Group Key"),
            properties.get("Filter"),
            properties.get("CTE Name"),
        )
    ).lower()
    roles: list[str] = []
    if operator == "ROWSET_SCAN":
        roles.append("scan")
    if operator == "HASH_JOIN":
        roles.append("root_join" if "customer_id" in text and "year_total" in text else "join")
        if "date_dim" in text or "date_sk" in text or "sold_date" in text:
            roles.append("date_join")
    if operator == "AGGREGATE":
        roles.append("partial_aggregate" if "d_year" in text else "final_aggregate")
    if operator == "FILTER" and "sum(" in text:
        roles.append("residual_filter")
    if operator == "MATERIALIZED_CTE":
        roles.append("cte_producer")
    if operator == "CTE_SCAN":
        roles.append("cte_consumer")
    return roles


def _execution_observations(
    execution_sample: dict[str, Any] | None,
) -> dict[int, list[dict[str, Any]]]:
    by_logical_id: dict[int, list[dict[str, Any]]] = {}
    if not isinstance(execution_sample, dict):
        return by_logical_id
    operators = execution_sample.get("operators", [])
    if not isinstance(operators, list):
        raise ValueError("execution profile operators must be an array")
    for operator in operators:
        if not isinstance(operator, dict):
            raise ValueError("execution profile operator must be an object")
        logical_node_id = operator.get("logical_node_id")
        if not isinstance(logical_node_id, int) or isinstance(logical_node_id, bool):
            continue
        by_logical_id.setdefault(logical_node_id, []).append(
            {
                "tree_path": operator.get("tree_path"),
                "operator": operator.get("operator"),
                "runtime_node_id": operator.get("node_id"),
                "rows": operator.get("rows"),
                "loops": operator.get("loops"),
                "startup_time_ms": operator.get("startup_time_ms"),
                "total_time_ms": operator.get("total_time_ms"),
            }
        )
    return by_logical_id


def _plan_quality_signals(occurrences: list[dict[str, Any]]) -> dict[str, Any]:
    """Classify the generic Q11 quality chain from plan facts only.

    This is a diagnostic gate, not a cost-model admission decision.  It
    deliberately requires the observable structural ingredients and reports
    missing pieces instead of treating a short plan or low estimated root
    cardinality as evidence of a good execution path.
    """

    date_scans = [
        occurrence
        for occurrence in occurrences
        if occurrence.get("operator") == "ROWSET_SCAN"
        and occurrence.get("relation") == "public.date_dim"
    ]
    pushed_date_scans = [
        occurrence
        for occurrence in date_scans
        if "d_year" in json.dumps(occurrence.get("properties", {})).lower()
        and "pushed predicate" in json.dumps(occurrence.get("properties", {})).lower()
    ]
    partial_aggregates = [
        occurrence
        for occurrence in occurrences
        if occurrence.get("operator") == "AGGREGATE"
        and any(
            token in str(occurrence.get("properties", {}).get("Group Key", ""))
            for token in ("ss_customer_sk", "ws_bill_customer_sk")
        )
    ]
    wide_aggregates = [
        occurrence
        for occurrence in occurrences
        if occurrence.get("operator") == "AGGREGATE"
        and "c_customer_id" in str(occurrence.get("properties", {}).get("Group Key", ""))
    ]
    runtime_filter_occurrences = [
        occurrence
        for occurrence in occurrences
        if "runtime filter" in json.dumps(occurrence).lower()
    ]
    missing = []
    if len(pushed_date_scans) < len(date_scans):
        missing.append("year_domain_pushdown")
    if not partial_aggregates:
        missing.append("narrow_key_preaggregation")
    if not wide_aggregates:
        missing.append("late_wide_attribute_merge")
    if not runtime_filter_occurrences:
        missing.append("runtime_filter")
    return {
        "status": "present" if not missing else "incomplete",
        "date_scan_count": len(date_scans),
        "date_scan_with_year_pushdown": len(pushed_date_scans),
        "narrow_partial_aggregate_count": len(partial_aggregates),
        "wide_attribute_aggregate_count": len(wide_aggregates),
        "runtime_filter_occurrence_count": len(runtime_filter_occurrences),
        "missing": missing,
        "evidence": "structural_plan_only",
    }


def _plan_coordinates(
    raw_plan: Any,
    execution_sample: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a stable physical-occurrence/semantic-coordinate artifact.

    Physical runtime ids and tree paths are retained as observations only.
    The join key is the logical PlanNodeId carried by the lowering lineage;
    missing or ambiguous lineage is reported, never inferred from ordinal
    position.
    """

    if not isinstance(raw_plan, str):
        raise ValueError("cold sample plan is not a JSON string")
    document = json.loads(raw_plan)
    if document.get("format_version") != 2 or not isinstance(document.get("plan"), dict):
        raise ValueError("cold sample plan does not use EXPLAIN JSON format version 2")
    runtime = _execution_observations(execution_sample)
    occurrences: list[dict[str, Any]] = []

    def visit(node: dict[str, Any], path: tuple[int, ...]) -> None:
        properties = node.get("properties", {})
        if not isinstance(properties, dict):
            raise ValueError("plan node properties must be an object")
        column_ids = properties.get("Column IDs", [])
        if not isinstance(column_ids, list):
            raise ValueError("plan node Column IDs must be an array")
        logical_node_id = node.get("logical_node_id")
        path_label = "/".join(str(index) for index in path) or "root"
        observations = runtime.get(logical_node_id, []) if isinstance(logical_node_id, int) else []
        observed_rows = [
            item["rows"]
            for item in observations
            if isinstance(item.get("rows"), int) and not isinstance(item.get("rows"), bool)
        ]
        observed_times = [
            item["total_time_ms"]
            for item in observations
            if isinstance(item.get("total_time_ms"), (int, float))
            and not isinstance(item.get("total_time_ms"), bool)
        ]
        entry: dict[str, Any] = {
            "occurrence_id": (
                f"logical_node:{logical_node_id}"
                if isinstance(logical_node_id, int)
                else f"physical_path:{path_label}"
            ),
            "physical_occurrence_id": f"path:{path_label};node:{node.get('node_id')}",
            "structural_path": list(path),
            "physical_node_id": node.get("node_id"),
            "logical_node_id": logical_node_id,
            "operator": node.get("operator"),
            "relation": node.get("relation"),
            "estimated_rows": node.get("estimated_rows"),
            "estimated_cardinality": node.get("estimated_cardinality"),
            "column_ids": column_ids,
            "properties": properties,
            "roles": _semantic_roles(node),
            "runtime": {
                "status": (
                    "matched"
                    if len(observations) == 1
                    else "ambiguous"
                    if len(observations) > 1
                    else "uncovered"
                ),
                "observations": observations,
                "actual_rows": statistics.median(observed_rows) if observed_rows else None,
                "actual_time_ms": statistics.median(observed_times) if observed_times else None,
                "source": "d6_execution_profile" if observations else None,
            },
            "q_error": _q_error(
                node.get("estimated_rows"),
                statistics.median(observed_rows) if len(observed_rows) == 1 else None,
            ),
            "evidence": {
                "condition_domain": "uncovered",
                "coverage": "runtime_coordinate_only" if observations else "structural_only",
                "source": "EXPLAIN structural plan plus optional same-query diagnostic profile",
                "calibration_revision": None,
            },
        }
        occurrences.append(entry)
        children = node.get("children", [])
        if not isinstance(children, list):
            raise ValueError("plan node children must be an array")
        for index, child in enumerate(children):
            if not isinstance(child, dict):
                raise ValueError("plan child must be an object")
            visit(child, (*path, index))

    visit(document["plan"], ())
    logical_ids = [
        entry["logical_node_id"]
        for entry in occurrences
        if isinstance(entry["logical_node_id"], int)
        and not isinstance(entry["logical_node_id"], bool)
    ]
    role_counts: dict[str, dict[str, int]] = {}
    for entry in occurrences:
        for role in entry["roles"]:
            bucket = role_counts.setdefault(role, {"occurrences": 0, "runtime_matched": 0})
            bucket["occurrences"] += 1
            if entry["runtime"]["status"] == "matched":
                bucket["runtime_matched"] += 1
    return {
        "coordinate_system": "physical_occurrence_with_logical_plan_node_id",
        "format_version": document["format_version"],
        "node_count": len(occurrences),
        "logical_node_id_unique": len(logical_ids) == len(set(logical_ids)),
        "role_coverage": role_counts,
        "quality_signals": _plan_quality_signals(occurrences),
        "occurrences": occurrences,
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
    sample: dict[str, Any],
    *,
    expected_process_id: int,
    expected_sample_id: str,
    expected_fp: int,
    execution_sample: dict[str, Any] | None = None,
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
    plan = sample.get("plan")
    return {
        "block": sample.get("block"),
        "status": sample.get("status"),
        "plan_sha256": sample.get("plan_sha256"),
        "optimizer_ms": sample.get("optimizer_ms"),
        "explain_wall_ms": sample.get("explain_wall_ms"),
        "components_ms": _component_times(diagnostics),
        "milestones": _milestones(events),
        "rules": _rule_attribution(diagnostics, events),
        "plan_coordinates": (
            _plan_coordinates(plan, execution_sample)
            if isinstance(plan, str)
            else {
                "status": "uncovered",
                "reason": "sample has no structural EXPLAIN JSON plan",
            }
        ),
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


def _load_execution_profile(path: Path) -> tuple[dict[str, Any], dict[int, dict[str, Any]]]:
    profile = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(profile, dict) or profile.get("mode") != "d6_execution_profile":
        raise ValueError("execution profile is not a D6 diagnostic profile")
    samples = profile.get("samples")
    if not isinstance(samples, list):
        raise ValueError("execution profile contains no samples")
    by_block: dict[int, dict[str, Any]] = {}
    for sample in samples:
        if not isinstance(sample, dict):
            raise ValueError("execution profile sample must be an object")
        block = sample.get("block")
        if not isinstance(block, int) or isinstance(block, bool) or block in by_block:
            raise ValueError("execution profile blocks must be unique integers")
        by_block[block] = sample
    return profile, by_block


def _execution_profile_compatibility(
    report: dict[str, Any], query: dict[str, Any], profile: dict[str, Any]
) -> dict[str, Any]:
    evidence = report.get("evidence")
    evidence = evidence if isinstance(evidence, dict) else {}
    build = evidence.get("build")
    build = build if isinstance(build, dict) else {}
    seed = profile.get("seed")
    seed = seed if isinstance(seed, dict) else {}
    binary = profile.get("binary")
    binary = binary if isinstance(binary, dict) else {}
    expected_sql = query.get("sql_sha256")
    actual_sql = profile.get("query_sha256")
    expected_seed = evidence.get("dataset_sha256")
    actual_seed = seed.get("sha256")
    expected_binary = build.get("binary_sha256")
    actual_binary = binary.get("binary_sha256")
    checks = {
        "sql_sha256": expected_sql == actual_sql and isinstance(expected_sql, str),
        "dataset_sha256": expected_seed == actual_seed and isinstance(expected_seed, str),
        "binary_sha256": expected_binary == actual_binary and isinstance(expected_binary, str),
    }
    return {
        "accepted": all(checks.values()),
        "checks": checks,
        "join_key": "(block, logical_node_id)",
        "statement_boundary": "diagnostic EXPLAIN ANALYZE is separate from structural EXPLAIN FORMAT JSON",
        "rejected_when": "SQL, immutable data seed, or binary identity differs",
    }


def build_attribution(
    report: dict[str, Any],
    source_path: Path,
    execution_profile_path: Path | None = None,
) -> dict[str, Any]:
    queries = report.get("queries")
    if not isinstance(queries, list) or len(queries) != 1:
        raise ValueError("attribution requires one query in the cold-planning report")
    query = queries[0]
    samples = query.get("samples")
    if not isinstance(samples, list) or not samples:
        raise ValueError("cold-planning report contains no samples")

    execution_profile = None
    execution_samples: dict[int, dict[str, Any]] = {}
    compatibility = None
    if execution_profile_path is not None:
        execution_profile, execution_samples = _load_execution_profile(execution_profile_path)
        compatibility = _execution_profile_compatibility(report, query, execution_profile)
        if not compatibility["accepted"]:
            execution_samples = {}

    output_samples = []
    for sample in samples:
        server = sample.get("server") or {}
        block = sample.get("block")
        execution_sample = (
            execution_samples.get(block)
            if isinstance(block, int) and not isinstance(block, bool)
            else None
        )
        output_samples.append(
            _attribute_sample(
                sample,
                expected_process_id=int(server["pid"]),
                expected_sample_id=str(server["statement_trace_sample_id"]),
                expected_fp=int(sample["trace_query_fingerprint"]),
                execution_sample=execution_sample,
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
        "execution_profile": (
            {
                "path": str(execution_profile_path.resolve()),
                "sha256": _content_digest(execution_profile_path),
                "schema_version": execution_profile.get("schema_version"),
                "compatibility": compatibility,
                "coordinates_used": bool(execution_samples),
            }
            if execution_profile_path is not None and execution_profile is not None
            else None
        ),
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
    parser.add_argument(
        "--execution-profile",
        type=Path,
        help="optional D6 profile; joined only when SQL/data/binary identities match",
    )
    args = parser.parse_args()
    report = json.loads(args.report.read_text(encoding="utf-8"))
    attribution = build_attribution(report, args.report, args.execution_profile)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(attribution, indent=2, ensure_ascii=False, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    print(json.dumps({"output": str(args.output), "samples": len(attribution["samples"])}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
