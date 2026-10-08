# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

"""Validate and generate the checked-in optimizer calibration artifact."""

from __future__ import annotations

import argparse
import re
from pathlib import Path
import sys
import tomllib

from optimizer_calibration_receipt import INACTIVE_CLASSES, finite_number, validate_receipt


REPO_ROOT = Path(__file__).resolve().parents[2]
SOURCE = REPO_ROOT / "benchmark/calibration/optimizer_cost_model.toml"
OUTPUT = REPO_ROOT / "crates/optimizer/src/cost/calibration/generated.rs"
REQUIRED_CLASSES = {*range(1, 18), 20_001, 20_002, 20_003, 20_004}
DIMENSIONS = {
    "Cpu",
    "MemoryRead",
    "MemoryWrite",
    "SequentialIo",
    "RandomIo",
    "Network",
}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--write", action="store_true")
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    if args.write == args.check:
        parser.error("choose exactly one of --write or --check")

    payload = tomllib.loads(SOURCE.read_text())
    _validate(payload)
    generated = _render(payload)
    if args.write:
        OUTPUT.parent.mkdir(parents=True, exist_ok=True)
        OUTPUT.write_text(generated)
        print(f"generated {OUTPUT.relative_to(REPO_ROOT)}")
        return 0
    if not OUTPUT.exists() or OUTPUT.read_text() != generated:
        print(
            "optimizer calibration artifact is stale; run "
            "python3 tools/ci/generate_optimizer_calibration.py --write",
            file=sys.stderr,
        )
        return 1
    print("optimizer calibration artifact is current")
    return 0


def _validate(payload: dict, *, repo_root: Path = REPO_ROOT) -> None:
    artifact = payload["artifact"]
    if artifact["provenance"] not in {"bootstrap", "measured"}:
        raise ValueError("calibration provenance must be bootstrap or measured")
    workload_name = artifact["validation_workload"]
    workload_path = repo_root / f"benchmark/workloads/{workload_name}/workload.toml"
    workload = tomllib.loads(workload_path.read_text())
    query_ids = {query["id"] for query in workload.get("query", [])}
    finite_number(artifact["risk_weight"], "risk_weight")
    fitting = payload["fitting"]
    inactive = fitting["inactive_classes"]
    uncovered = fitting["uncovered_classes"]
    for label, ids in (("inactive", inactive), ("uncovered", uncovered)):
        if (not isinstance(ids, list) or any(type(value) is not int for value in ids)
                or len(set(ids)) != len(ids) or not set(ids).issubset(REQUIRED_CLASSES)):
            raise ValueError(f"invalid {label} fitting classes")
    if set(inactive) & set(uncovered):
        raise ValueError("fitting classes cannot be both inactive and uncovered")
    if set(inactive) != INACTIVE_CLASSES:
        raise ValueError("fitting eligibility must preserve the known inactive work classes")
    if fitting.get("inactive_parallelism") != ["blocking_merge_serial_fraction"]:
        raise ValueError("inactive parallelism must identify the unused blocking-merge parameter")
    if artifact["provenance"] == "measured":
        if inactive or uncovered:
            raise ValueError("inactive/uncovered classes cannot be published as a fully measured bundle")
        measurement = payload.get("measurement")
        if not isinstance(measurement, dict):
            raise ValueError("measured calibration requires a measurement record")
        if not re.fullmatch(r"[0-9a-f]{64}", measurement.get("result_sha256", "")):
            raise ValueError("measurement result_sha256 must be a lowercase SHA-256")
        for field in ("observed_at", "git_commit", "result_file"):
            if not measurement.get(field):
                raise ValueError(f"measurement record is missing {field}")

    parallelism = payload["parallelism"]
    fractions = [
        finite_number(parallelism["expected_worker_efficiency"], "expected_worker_efficiency"),
        finite_number(parallelism["risk_worker_efficiency"], "risk_worker_efficiency"),
        finite_number(parallelism["pipeline_serial_fraction"], "pipeline_serial_fraction"),
        finite_number(parallelism["blocking_merge_serial_fraction"], "blocking_merge_serial_fraction"),
    ]
    if any(value <= 0.0 or value > 1.0 for value in fractions[:2]):
        raise ValueError("parallel worker efficiencies must be in (0, 1]")
    if any(value < 0.0 or value > 1.0 for value in fractions[2:]):
        raise ValueError("parallel serial fractions must be in [0, 1]")
    if parallelism["risk_worker_efficiency"] > parallelism["expected_worker_efficiency"]:
        raise ValueError("risk worker efficiency cannot exceed expected efficiency")
    if any(
        finite_number(parallelism[field], field) < 0.0
        for field in ("coordination_latency_expected", "coordination_latency_upper")
    ):
        raise ValueError("parallel coordination latency cannot be negative")
    if (
        parallelism["coordination_latency_upper"]
        < parallelism["coordination_latency_expected"]
    ):
        raise ValueError("parallel coordination upper must cover its expectation")

    coefficients = payload.get("coefficient", [])
    ids = [int(coefficient["id"]) for coefficient in coefficients]
    if len(ids) != len(set(ids)):
        raise ValueError("calibration contains duplicate OpClass ids")
    if set(ids) != REQUIRED_CLASSES:
        missing = sorted(REQUIRED_CLASSES - set(ids))
        extra = sorted(set(ids) - REQUIRED_CLASSES)
        raise ValueError(
            f"calibration class coverage mismatch: missing={missing}, extra={extra}"
        )
    for coefficient in coefficients:
        if coefficient["dimension"] not in DIMENSIONS:
            raise ValueError(f"unknown resource dimension: {coefficient['dimension']}")
        values = [
            finite_number(coefficient["expected"], "expected"),
            finite_number(coefficient["risk"], "risk"),
            finite_number(coefficient["latency_expected"], "latency_expected"),
            finite_number(coefficient["latency_upper"], "latency_upper"),
        ]
        if any(value < 0 for value in values):
            raise ValueError(f"negative coefficient for class {coefficient['id']}")
        if values[1] < values[0] or values[3] < values[2]:
            raise ValueError(
                f"invalid risk/latency envelope for class {coefficient['id']}"
            )
        if "source_queries" in coefficient:
            raise ValueError(
                "source_queries falsely implies measured provenance; use validation_queries"
            )
        unknown_queries = set(coefficient.get("validation_queries", [])) - query_ids
        if unknown_queries:
            raise ValueError(
                f"class {coefficient['id']} references unknown corpus queries: "
                f"{sorted(unknown_queries)}"
            )
        if coefficient["id"] in set(inactive) | set(uncovered) and coefficient.get("validation_queries"):
            raise ValueError("inactive/uncovered classes cannot claim a validation fixture")
    if artifact["provenance"] == "measured":
        reference = {"file": measurement["result_file"], "sha256": measurement["result_sha256"]}
        validate_receipt(
            repo_root, reference, REQUIRED_CLASSES,
            {int(coefficient["id"]): float(coefficient["latency_expected"]) for coefficient in coefficients},
        )


def _render(payload: dict) -> str:
    artifact = payload["artifact"]
    parallelism = payload["parallelism"]
    coefficients = sorted(
        payload["coefficient"], key=lambda coefficient: int(coefficient["id"])
    )
    lines = [
        "// Copyright 2024-2026 Zunor",
        "// SPDX-License-Identifier: Apache-2.0",
        "",
        "//! Generated by `tools/ci/generate_optimizer_calibration.py`.",
        "//! Do not edit by hand; update the benchmark calibration artifact.",
        "",
        "use super::{BuiltinCoefficient, OpClassId, ResourceDimension};",
        "",
        f"pub(super) const REVISION: u32 = {int(artifact['revision'])};",
        f"pub(super) const HARDWARE_CLASS: &str = {_rust_string(artifact['hardware_class'])};",
        f"pub(super) const CORPUS_ID: &str = {_rust_string(artifact['corpus_id'])};",
        f"pub(super) const PROVENANCE: &str = {_rust_string(artifact['provenance'])};",
        f"pub(super) const RISK_WEIGHT: f64 = {float(artifact['risk_weight']):.6f};",
        f"pub(super) const EXPECTED_WORKER_EFFICIENCY: f64 = {float(parallelism['expected_worker_efficiency']):.6f};",
        f"pub(super) const RISK_WORKER_EFFICIENCY: f64 = {float(parallelism['risk_worker_efficiency']):.6f};",
        f"pub(super) const COORDINATION_LATENCY_EXPECTED: f64 = {float(parallelism['coordination_latency_expected']):.6f};",
        f"pub(super) const COORDINATION_LATENCY_UPPER: f64 = {float(parallelism['coordination_latency_upper']):.6f};",
        f"pub(super) const PIPELINE_SERIAL_FRACTION: f64 = {float(parallelism['pipeline_serial_fraction']):.6f};",
        f"pub(super) const BLOCKING_MERGE_SERIAL_FRACTION: f64 = {float(parallelism['blocking_merge_serial_fraction']):.6f};",
        "",
        "pub(super) const COEFFICIENTS: &[BuiltinCoefficient] = &[",
    ]
    for coefficient in coefficients:
        validations = ", ".join(coefficient.get("validation_queries", [])) or "none (Uncovered)"
        lines.extend(
            [
                f"    // {coefficient['name']}; coverage queries: {validations}",
                "    BuiltinCoefficient {",
                f"        class: OpClassId({int(coefficient['id'])}),",
                f"        dimension: ResourceDimension::{coefficient['dimension']},",
                f"        expected: {float(coefficient['expected']):.6f},",
                f"        risk: {float(coefficient['risk']):.6f},",
                f"        latency_expected: {float(coefficient['latency_expected']):.6f},",
                f"        latency_upper: {float(coefficient['latency_upper']):.6f},",
                "    },",
            ]
        )
    lines.append("];")
    lines.append("")
    return "\n".join(lines)


def _rust_string(value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


if __name__ == "__main__":
    raise SystemExit(main())
