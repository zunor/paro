#!/usr/bin/env python3
# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

"""Fail-closed cardinality and plan-shape gate over an attested SQL corpus.

Compare complete raw reports, not caller-supplied summary percentiles. Every
named semantic boundary must remain covered; better estimates for one query
cannot hide worse estimates for another. Source revisions may differ, but
fixtures, SQL, oracle cardinalities, collector and settings must agree.
"""

from __future__ import annotations

import argparse
import json
import math
import statistics
from pathlib import Path
from typing import Any, Iterable


def cardinality(value: Any) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError("cardinality must be numeric")
    if not math.isfinite(value) or value < 0:
        raise ValueError("cardinality must be finite and non-negative")
    return float(value)


def q_error(estimated_rows: float, actual_rows: float) -> float:
    estimated_rows, actual_rows = cardinality(estimated_rows), cardinality(actual_rows)
    if estimated_rows == actual_rows == 0:
        return 1.0
    if estimated_rows == 0 or actual_rows == 0:
        return math.inf
    return max(estimated_rows / actual_rows, actual_rows / estimated_rows)


def _json_float(value: float) -> float | str:
    return value if math.isfinite(value) else "inf"


def summarize(values: Iterable[float]) -> dict[str, float | int | str]:
    ordered = sorted(values)
    if not ordered or any(math.isnan(value) or value < 1 for value in ordered):
        raise ValueError("quality gate requires nonempty valid q-errors")

    def percentile(fraction: float) -> float | str:
        return _json_float(ordered[round((len(ordered) - 1) * fraction)])

    return {"count": len(ordered), "q50": percentile(.50), "q95": percentile(.95),
            "q99": percentile(.99), "max": _json_float(ordered[-1]),
            "mean": _json_float(statistics.fmean(ordered))}


def validate(report: dict[str, Any]) -> dict[str, dict[str, Any]]:
    if report.get("schema_version") != 1 or report.get("invalidated"):
        raise ValueError("missing, unsupported or invalidated quality evidence")
    contract, evidence = report.get("contract"), report.get("evidence")
    if not isinstance(contract, dict) or not isinstance(evidence, dict):
        raise ValueError("quality report requires contract and build evidence")
    for name in ("corpus_sha256", "collector_sha256", "settings", "cases"):
        if not contract.get(name):
            raise ValueError(f"quality contract is missing {name}")
    if not evidence.get("build", {}).get("binary_sha256"):
        raise ValueError("quality evidence is missing its binary identity")
    cases = contract["cases"]
    if not isinstance(cases, list) or not cases:
        raise ValueError("quality contract has no cases")
    expected = {}
    for case in cases:
        query = case.get("query")
        if not isinstance(query, str) or not query or query in expected or not case.get("sql_sha256"):
            raise ValueError("case identities must be present and unique")
        cardinality(case["expected_rows"])
        metrics = case.get("nonincreasing_metrics", [])
        if not isinstance(metrics, list) or len(set(metrics)) != len(metrics):
            raise ValueError("invalid plan metric contract")
        expected[query] = case
    observations = report.get("observations")
    if not isinstance(observations, list):
        raise ValueError("quality observations must be an array")
    result = {}
    for item in observations:
        query = item.get("query")
        if query not in expected or query in result or item.get("status") != "ok":
            raise ValueError("unknown, duplicate or failed quality observation")
        case = expected[query]
        if item.get("sql_sha256") != case["sql_sha256"]:
            raise ValueError(f"{query}: SQL identity differs from corpus")
        cardinality(item["estimated_rows"])
        if cardinality(item["actual_rows"]) != cardinality(case["expected_rows"]):
            raise ValueError(f"{query}: independent cardinality oracle failed")
        if cardinality(item["result_rows"]) != cardinality(case["expected_result_rows"]):
            raise ValueError(f"{query}: full result cardinality oracle failed")
        if not item.get("operator") or not item.get("plan"):
            raise ValueError(f"{query}: missing captured plan")
        for metric in case.get("nonincreasing_metrics", []):
            cardinality(item.get("plan_metrics", {}).get(metric))
        result[query] = item
    if result.keys() != expected.keys():
        raise ValueError("quality report omitted corpus cases")
    return result


def evaluate(report: dict[str, Any], baseline: dict[str, Any] | None = None) -> dict[str, Any]:
    observations = validate(report)
    values = {query: q_error(item["estimated_rows"], item["actual_rows"])
              for query, item in observations.items()}
    regressions, improvements = [], []
    if baseline is not None:
        previous = validate(baseline)
        if report["contract"] != baseline["contract"]:
            raise ValueError("quality corpus/collector/settings contracts differ")
        for case in report["contract"]["cases"]:
            query = case["query"]
            old = previous[query]
            old_error = q_error(old["estimated_rows"], old["actual_rows"])
            if values[query] > old_error:
                regressions.append(f"{query}: q-error {old_error:g} -> {values[query]:g}")
            elif values[query] < old_error:
                improvements.append(f"{query}: q-error {old_error:g} -> {values[query]:g}")
            for metric in case.get("nonincreasing_metrics", []):
                before, after = old["plan_metrics"][metric], observations[query]["plan_metrics"][metric]
                if after > before:
                    regressions.append(f"{query}: {metric} {before} -> {after}")
                elif after < before:
                    improvements.append(f"{query}: {metric} {before} -> {after}")
    return {"summary": summarize(values.values()), "passed": not regressions,
            "q_errors": {query: _json_float(value) for query, value in values.items()},
            "regressions": regressions, "improvements": improvements}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--baseline", type=Path)
    parser.add_argument("--write-summary", type=Path)
    args = parser.parse_args()
    try:
        result = evaluate(json.loads(args.report.read_text()),
                          json.loads(args.baseline.read_text()) if args.baseline else None)
    except (ValueError, TypeError, KeyError) as error:
        result = {"passed": False, "error": str(error)}
    payload = json.dumps(result, indent=2, sort_keys=True, allow_nan=False) + "\n"
    if args.write_summary:
        args.write_summary.write_text(payload)
    print(payload, end="")
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
