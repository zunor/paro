#!/usr/bin/env python3
"""Estimate-vs-actual cardinality quality gate.

The SQL regression suite intentionally compares plans textually.  This gate
keeps cardinality quality separate: a plan may change shape, but a change in
q-error must be explicit and reviewable.  Input is a JSON object containing an
``observations`` array with ``query``, ``operator``, ``estimated_rows`` and
``actual_rows`` fields.  The baseline uses the same format and stores only
the aggregate distribution, never a runtime-dependent plan string.
"""

from __future__ import annotations

import argparse
import json
import math
import statistics
from pathlib import Path
from typing import Any, Iterable


def q_error(estimated_rows: float, actual_rows: float) -> float:
    """Return the symmetric cardinality error for one operator."""

    if estimated_rows < 0 or actual_rows < 0:
        raise ValueError("cardinality values must be non-negative")
    if estimated_rows == 0 and actual_rows == 0:
        return 1.0
    if estimated_rows == 0 or actual_rows == 0:
        return math.inf
    return max(estimated_rows / actual_rows, actual_rows / estimated_rows)


def _json_float(value: float) -> float | str:
    """Encode an extended-real metric using strict JSON values.

    Python's ``json`` module emits the non-standard token ``Infinity`` by
    default.  A zero-cardinality observation legitimately has infinite
    q-error, so use a stable string token instead of producing a report that
    strict parsers reject.
    """

    return value if math.isfinite(value) else "inf"


def summarize(values: Iterable[float]) -> dict[str, float | int | str]:
    ordered = sorted(values)
    if not ordered:
        raise ValueError("quality gate requires at least one observation")

    def percentile(fraction: float) -> float:
        index = round((len(ordered) - 1) * fraction)
        return ordered[max(0, min(len(ordered) - 1, index))]

    return {
        "count": len(ordered),
        "q50": _json_float(percentile(0.50)),
        "q95": _json_float(percentile(0.95)),
        "q99": _json_float(percentile(0.99)),
        "max": _json_float(ordered[-1]),
        "mean": _json_float(statistics.fmean(ordered)),
    }


def evaluate(report: dict[str, Any], baseline: dict[str, Any] | None = None) -> dict[str, Any]:
    observations = report.get("observations")
    if not isinstance(observations, list):
        raise ValueError("report.observations must be an array")
    values = [
        q_error(float(item["estimated_rows"]), float(item["actual_rows"]))
        for item in observations
    ]
    summary = summarize(values)
    result: dict[str, Any] = {"summary": summary, "passed": True}
    if baseline is not None:
        expected = baseline.get("summary", {})
        # A gate is deliberately conservative.  Any q-error tail regression
        # is visible; callers can bless a new baseline as an explicit change.
        for field in ("q95", "q99", "max"):
            old_value = expected.get(field, math.inf)
            old = math.inf if old_value is None else float(old_value)
            new = float(summary[field])
            if not math.isfinite(old) and math.isfinite(new):
                continue
            if new > old:
                result["passed"] = False
        result["baseline"] = expected
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--baseline", type=Path)
    parser.add_argument("--write-summary", type=Path)
    args = parser.parse_args()
    report = json.loads(args.report.read_text(encoding="utf-8"))
    baseline = (
        json.loads(args.baseline.read_text(encoding="utf-8"))
        if args.baseline
        else None
    )
    result = evaluate(report, baseline)
    payload = json.dumps(result, indent=2, sort_keys=True) + "\n"
    if args.write_summary:
        args.write_summary.write_text(payload, encoding="utf-8")
    print(payload, end="")
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
