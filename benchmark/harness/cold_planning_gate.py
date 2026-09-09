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

VERSION = 5
TRACE_SCHEMA_VERSION = 2
COUNTERS = ("search_complete", "memo_group_count", "memo_logical_expression_count",
            "memo_physical_expression_count", "settlement_local_hit_count", "settlement_local_miss_count",
            "search_rule_failure_count", "search_deadline_reached")
METRICS = ("explain_wall_ms", "optimizer_ms", "peak_rss_bytes")


def validate_trace(
    trace: dict[str, Any],
    *,
    process_id: int,
    sample_id: str,
    fingerprint: int | None = None,
    require_cold: bool,
) -> None:
    if trace.get("schema_version") != TRACE_SCHEMA_VERSION:
        raise ValueError("unsupported statement trace schema")
    if trace.get("process_id") != process_id:
        raise ValueError("statement trace belongs to another process")
    if trace.get("trace_sample_id") != sample_id:
        raise ValueError("statement trace belongs to another sample")
    if trace.get("operation_id") != trace.get("statement_id"):
        raise ValueError("operation identity does not match statement identity")
    if fingerprint is not None and trace.get("query_fingerprint") != fingerprint:
        raise ValueError("statement trace query identity differs from sample")
    events = trace.get("events")
    if not isinstance(events, list) or not events:
        raise ValueError("statement trace has no events")
    sequences = [event.get("sequence") for event in events]
    if sequences != list(range(len(events))):
        raise ValueError("statement trace sequence is not unique and contiguous")
    elapsed: list[int] = []
    positions: dict[str, list[int]] = {}
    for index, event in enumerate(events):
        if not isinstance(event, dict) or not isinstance(event.get("phase"), str) \
                or not event["phase"] or not isinstance(event.get("event"), str) \
                or not event["event"]:
            raise ValueError("statement trace has an invalid event record")
        event_elapsed = event.get("elapsed_us")
        if (isinstance(event_elapsed, bool) or not isinstance(event_elapsed, int)
                or event_elapsed < 0):
            raise ValueError("statement trace elapsed time is invalid")
        elapsed.append(event_elapsed)
        for field in ("duration_us", "value"):
            field_value = event.get(field)
            if (field_value is not None
                    and (isinstance(field_value, bool)
                         or not isinstance(field_value, int)
                         or field_value < 0)):
                raise ValueError(f"statement trace {field} is invalid")
        positions.setdefault(event["event"], []).append(index)
    if elapsed != sorted(elapsed):
        raise ValueError("statement trace elapsed time is not monotonic")
    terminals = [name for name in ("statement_complete", "statement_error", "statement_aborted")
                 if name in positions]
    if len(terminals) != 1 or positions[terminals[0]][0] != len(events) - 1:
        raise ValueError("statement trace has no unique final lifecycle state")
    if require_cold:
        for required in (
            "parse_entry", "compiler_call_entry", "compiler_call_return",
            "statement_scope_begin", "statement_scope_return",
        ):
            if len(positions.get(required, [])) != 1:
                raise ValueError(f"statement trace requires exactly one {required}")
        if terminals != ["statement_complete"]:
            raise ValueError("cold statement trace did not complete successfully")
        if len(positions.get("plan_cache_miss", [])) != 1:
            raise ValueError("cold statement trace does not prove one cache miss")
        if any(positions.get(name) for name in ("plan_cache_hit", "instance_plan_cache_hit")):
            raise ValueError("cold statement trace contains a cache hit")
        order = {name: values[0] for name, values in positions.items() if len(values) == 1}
        if not (order["parse_entry"] < order["compiler_call_entry"]
                < order["compiler_call_return"]):
            raise ValueError("parse/compiler trace order is invalid")
        if not (order["statement_scope_begin"] < order["statement_scope_return"]
                < order[terminals[0]]):
            raise ValueError("statement lifecycle order is invalid")


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
            or runtime_environment.get("PARO_STATEMENT_TRACE") != "1":
        raise ValueError("diagnostic trace configuration is not explicit")
    if (report["configuration"].get("cohort") != "diagnostic"
            or report["configuration"].get("trace_mode") != "on"):
        raise ValueError("cold planning gate requires the diagnostic cohort")
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
            if sample.get("phase_trace_schema_version") != TRACE_SCHEMA_VERSION \
                    or not sample.get("statement_traces"):
                raise ValueError("missing per-statement phase trace evidence")
            traces = sample["statement_traces"]
            if not isinstance(traces, list) or not traces:
                raise ValueError("malformed per-statement phase trace evidence")
            server = sample["server"]
            sample_id = server.get("statement_trace_sample_id")
            if server.get("statement_trace") is not True or not isinstance(sample_id, str) \
                    or not sample_id:
                raise ValueError("diagnostic trace configuration is not attested")
            expected_process_id = server["pid"]
            for item in traces:
                validate_trace(
                    item,
                    process_id=expected_process_id,
                    sample_id=sample_id,
                    require_cold=False,
                )
            target_traces = sample.get("target_statement_traces")
            if not isinstance(target_traces, list) or len(target_traces) != 1:
                raise ValueError("phase trace is not uniquely correlated with the measured target")
            all_keys = {
                (item.get("process_id"), item.get("session_id"), item.get("statement_id"))
                for item in traces
            }
            target_keys = {
                (item.get("process_id"), item.get("session_id"), item.get("statement_id"))
                for item in target_traces
            }
            if len(target_keys) != 1 or not target_keys.issubset(all_keys):
                raise ValueError("target trace is not an operation from this sample")
            validate_trace(
                target_traces[0],
                process_id=expected_process_id,
                sample_id=sample_id,
                fingerprint=sample.get("trace_query_fingerprint"),
                require_cold=True,
            )
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
