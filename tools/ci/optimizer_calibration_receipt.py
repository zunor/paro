# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

"""Validate fit inputs and holdouts without trusting a reported rank/pass.

This module consumes measurements; it neither times operators nor fits a model.
"""

from __future__ import annotations

import hashlib
import argparse
import json
import math
from pathlib import Path
import re
import statistics
import subprocess
from typing import Any


FEATURE_UNITS = {
    **{key: "row" for key in (1, 2, 5, 6, 7, 9, 13, 14, 15, 20001)},
    3: "pair", 4: "comparison_proxy", 8: "group", 10: "slot",
    11: "row_key", 12: "lookup", 16: "byte_block_32", 17: "byte_block_32",
    20002: "comparison_proxy", 20003: "fetch", 20004: "page_4096",
}
INACTIVE_CLASSES = {20002, 20003, 20004}
TIMER_PHASES = {"runtime_init", "consume", "merge", "finish", "publication", "drop", "input_construction"}


def _timer_phases(timer: dict) -> tuple[frozenset[str], frozenset[str]]:
    classified = []
    for key in ("includes", "excludes"):
        phases = timer.get(key)
        if (not isinstance(phases, list) or not phases
                or any(not isinstance(phase, str) or phase not in TIMER_PHASES for phase in phases)
                or len(set(phases)) != len(phases)):
            raise ValueError("timer phase lists must be nonempty, unique, and use required phases")
        classified.append(frozenset(phases))
    includes, excludes = classified
    if includes & excludes or includes | excludes != TIMER_PHASES:
        raise ValueError("timer phases require complete non-overlapping classification")
    if "input_construction" not in excludes:
        raise ValueError("timer scope must exclude input_construction")
    return includes, excludes


def finite_number(value: Any, name: str, *, positive: bool = False) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} must be a numeric value")
    number = float(value)
    if not math.isfinite(number) or number < 0 or (positive and number == 0):
        raise ValueError(f"{name} must be finite and {'positive' if positive else 'nonnegative'}")
    return number


def _path(root: Path, value: Any) -> Path:
    if not isinstance(value, str) or not value:
        raise ValueError("receipt file references must be nonempty repository-relative paths")
    path = (root / value).resolve()
    if Path(value).is_absolute() or not path.is_relative_to(root.resolve()):
        raise ValueError("receipt file reference escapes the repository")
    return path


def _digest(value: Any) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{64}", value):
        raise ValueError("receipt digest must be a lowercase SHA-256")
    return value


def _read(root: Path, reference: dict) -> dict:
    data = _path(root, reference["file"]).read_bytes()
    if hashlib.sha256(data).hexdigest() != _digest(reference["sha256"]):
        raise ValueError(f"receipt input hash mismatch: {reference['file']}")
    result = json.loads(data)
    if not isinstance(result, dict):
        raise ValueError("receipt inputs must be JSON objects")
    return result


def _source(root: Path, source: dict) -> None:
    commit = source.get("git_commit")
    if not isinstance(commit, str) or not re.fullmatch(r"[0-9a-f]{40}", commit):
        raise ValueError("source git_commit must be a full lowercase commit identity")
    mode = source.get("mode")
    if mode not in {"git", "snapshot"}:
        raise ValueError("source mode must be git or snapshot")
    if mode == "snapshot":
        _digest(source.get("working_tree_sha256"))
    manifest = _read(root, source["manifest"])
    if manifest.get("scope") != "recorded-source-subset":
        raise ValueError("source manifest must declare its recorded subset coverage")
    if manifest.get("identity") != {key: value for key, value in source.items() if key != "manifest"}:
        raise ValueError("source manifest identity does not match the receipt")
    files = manifest.get("files")
    if not isinstance(files, list) or not files:
        raise ValueError("source manifest requires file identities")
    paths: set[str] = set()
    for entry in files:
        path = entry["path"]
        _path(root, path)
        if path in paths:
            raise ValueError("source manifest contains duplicate paths")
        paths.add(path)
        if mode == "git":
            completed = subprocess.run(
                ["git", "show", f"{commit}:{path}"], cwd=root, capture_output=True, check=False,
            )
            if completed.returncode:
                raise ValueError(f"source commit blob is unavailable: {path}")
            data = completed.stdout
        else:
            # Snapshot mode supports HEAD + tracked patch + new files. Its
            # Captured contents identify the recorded subset, not a full build.
            data = _path(root, entry["snapshot_file"]).read_bytes()
        if hashlib.sha256(data).hexdigest() != _digest(entry["sha256"]):
            raise ValueError(f"source file hash mismatch: {path}")
    if not {"Cargo.toml", "Cargo.lock", "rust-toolchain.toml"}.issubset(paths):
        raise ValueError("source manifest omits toolchain/dependency/profile inputs")


def matrix_rank(rows: list[list[float]], columns: int) -> int:
    """Column-normalized elimination with a fixed numerical-rank tolerance."""
    scales = [max(abs(row[col]) for row in rows) for col in range(columns)]
    matrix = [[value / (scales[col] or 1.0) for col, value in enumerate(row)] for row in rows]
    rank = 0
    for col in range(columns):
        pivot = max(range(rank, len(matrix)), key=lambda row: abs(matrix[row][col]), default=None)
        if pivot is None or abs(matrix[pivot][col]) <= 1e-10:
            continue
        matrix[rank], matrix[pivot] = matrix[pivot], matrix[rank]
        divisor = matrix[rank][col]
        matrix[rank] = [value / divisor for value in matrix[rank]]
        for row in range(rank + 1, len(matrix)):
            factor = matrix[row][col]
            matrix[row] = [a - factor * b for a, b in zip(matrix[row], matrix[rank])]
        rank += 1
    return rank


def validate_receipt(
    root: Path, reference: dict, expected_classes: set[int],
    latency_coefficients: dict[int, float] | None = None,
) -> dict:
    if not expected_classes or expected_classes & INACTIVE_CLASSES:
        raise ValueError("inactive classes cannot be fitted")
    receipt = _read(root, reference)
    if receipt.get("schema_version") != 1:
        raise ValueError("unsupported optimizer calibration receipt version")
    source = receipt["source"]
    _source(root, source)
    _digest(receipt["binary_sha256"])
    units = receipt["units"]
    if units.get("time") != "ns" or units.get("latency") != "reference-normalized":
        raise ValueError("receipt requires ns samples and reference-normalized latency")
    reference_ns = finite_number(units["reference_ns"], "reference_ns", positive=True)
    if not isinstance(units.get("reference_id"), str) or not units["reference_id"]:
        raise ValueError("receipt requires the reference measurement identity")
    columns = receipt["columns"]
    if not isinstance(columns, list) or not columns:
        raise ValueError("receipt requires design columns")
    names: set[str] = set()
    classes: set[int] = set()
    for column in columns:
        name = column["name"]
        if not isinstance(name, str) or not name or name in names:
            raise ValueError("design column names must be nonempty and unique")
        names.add(name)
        class_id = column.get("class_id")
        if class_id is None:
            if column.get("feature_unit") != "invocation":
                raise ValueError("nuisance columns must be explicit invocation intercepts")
        else:
            if type(class_id) is not int or class_id not in expected_classes or class_id in classes:
                raise ValueError("unexpected, inactive or duplicate fitted class")
            classes.add(class_id)
            if column.get("feature_unit") != FEATURE_UNITS[class_id]:
                raise ValueError("fitted class feature unit does not match the model")
    if classes != expected_classes:
        raise ValueError("measurement lacks required fitted classes")
    coefficients = [finite_number(value, "coefficient_ns") for value in receipt["coefficients_ns"]]
    if len(coefficients) != len(columns):
        raise ValueError("coefficient/design width mismatch")
    for column, coefficient in zip(columns, coefficients):
        if latency_coefficients is not None and column.get("class_id") is not None and not math.isclose(
            coefficient / reference_ns, latency_coefficients[column["class_id"]],
            rel_tol=1e-9, abs_tol=1e-12,
        ):
            raise ValueError("fitted latency does not match the generated coefficient")
    cells: dict[str, dict] = {}
    reference_samples: list[float] = []
    runs = receipt["runs"]
    if not isinstance(runs, list) or not runs:
        raise ValueError("receipt requires measured run files")
    timer_phases = None
    for run_reference in runs:
        run = _read(root, run_reference)
        if any(run.get(key) != receipt[key] for key in ("source", "binary_sha256", "units", "columns")):
            raise ValueError("run identity/units/design do not match the fit receipt")
        if run.get("schema_version") != 1:
            raise ValueError("unsupported fit-input run version")
        if run.get("holdout_gate") != receipt["holdout_gate"]:
            raise ValueError("holdout gate does not match the recorded run contract")
        timer = run.get("timer_scope", {})
        if (not isinstance(timer, dict) or timer.get("kind") != "operator_work" or timer.get("admitted_workers") != 1
                or timer.get("observed_workers") != 1 or timer.get("profile_mode") != "off"
                or type(timer.get("admitted_workers")) is not int
                or type(timer.get("observed_workers")) is not int):
            raise ValueError("operator coefficient inputs require declared serial trace-off timer scope")
        phases = _timer_phases(timer)
        if timer_phases is not None and phases != timer_phases:
            raise ValueError("timer phase classification differs between fit-input runs")
        timer_phases = phases
        measured_reference = run.get("reference_measurement")
        if measured_reference is not None:
            if measured_reference.get("id") != units["reference_id"]:
                raise ValueError("reference measurement identity mismatch")
            reference_samples.extend(
                finite_number(value, "reference sample_ns", positive=True)
                for value in measured_reference["samples_ns"]
            )
        for cell in run["cells"]:
            cell_id = cell["id"]
            if (not isinstance(cell_id, str) or not cell_id or cell_id in cells
                    or cell.get("status") != "Completed"):
                raise ValueError("duplicate or incomplete measurement cell")
            features = [finite_number(value, "feature") for value in cell["features"]]
            samples = [finite_number(value, "sample_ns", positive=True) for value in cell["samples_ns"]]
            if len(features) != len(columns) or not samples:
                raise ValueError("measurement cell lacks features or samples")
            if not isinstance(cell.get("split_key"), str) or not cell["split_key"]:
                raise ValueError("measurement requires a fixture/scale/seed split key")
            cells[cell_id] = {**cell, "features": features, "samples_ns": samples}
    if not reference_samples or not math.isclose(
        statistics.median(reference_samples), reference_ns, rel_tol=1e-9, abs_tol=1e-12,
    ):
        raise ValueError("reference_ns does not match retained reference measurements")
    split = receipt["split"]
    train, holdout = split["train"], split["holdout"]
    if (not train or not holdout or len(set(train)) != len(train) or len(set(holdout)) != len(holdout)
            or set(train) & set(holdout) or set(train) | set(holdout) != set(cells)):
        raise ValueError("fit/holdout split must cover each retained cell exactly once")
    if {cells[key]["split_key"] for key in train} & {cells[key]["split_key"] for key in holdout}:
        raise ValueError("holdout reuses a training fixture/scale/seed split key")
    rank = matrix_rank([cells[key]["features"] for key in train], len(columns))
    if rank != len(columns):
        raise ValueError("calibration design matrix is rank deficient")
    limit = finite_number(receipt["holdout_gate"]["max_relative_error"], "holdout error limit")
    errors = []
    for key in holdout:
        observed = statistics.median(cells[key]["samples_ns"])
        predicted = sum(a * b for a, b in zip(cells[key]["features"], coefficients))
        if not math.isfinite(predicted):
            raise ValueError("holdout prediction is non-finite")
        errors.append(abs(predicted - observed) / observed)
    if max(errors) > limit:
        raise ValueError("recomputed calibration holdout error exceeds its declared limit")
    return {"rank": rank, "holdout_cells": len(holdout), "max_relative_error": max(errors)}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--receipt", required=True, help="repository-relative fit receipt JSON")
    parser.add_argument("--sha256", required=True, help="expected immutable receipt digest")
    parser.add_argument("--classes", required=True, help="comma-separated classes in this experimental fit")
    args = parser.parse_args()
    result = validate_receipt(
        Path(__file__).resolve().parents[2], {"file": args.receipt, "sha256": args.sha256},
        {int(value) for value in args.classes.split(",")},
    )
    # Experimental input validation never promotes an artifact or requires the
    # candidate coefficients to equal the currently shipped bootstrap model.
    print(json.dumps({"scope": "experimental-fit-inputs", "artifact_binding": "Uncovered",
                      "source_coverage": "RecordedSubset", **result}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
