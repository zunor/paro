#!/usr/bin/env python3
"""Registered oracle for a keyed self-join of filtered small integer aggregates.

Inputs and relation roles are explicit evidence, not inferred from result
positions or a query hash. Unsupported shapes/boundaries fail closed. Raw
cross-engine exact differences are never overwritten.
"""
import argparse
from collections import Counter, defaultdict
from dataclasses import asdict
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "corpora"))
from numeric_result_contract import (
    VERSION, Uncovered, assert_numeric_bijection, sample_moments, schedule_envelope,
)
from analyze_integer_moment_oracle import ulps


def decode(cell):
    kind = cell["python_type"]
    if kind == "NoneType":
        return None
    if kind == "int":
        return int(cell["text"])
    if kind == "float":
        return float.fromhex(cell["float_hex"])
    if kind == "Decimal":
        return Decimal(cell["text"])
    raise ValueError(f"uncovered wire value {kind}")


def verify(inputs, output, spec):
    if spec.get("contract") != VERSION:
        raise ValueError("unregistered numerical contract version")
    if "limit" in spec or "approximate_order" in spec:
        raise Uncovered("approximate ORDER BY/LIMIT boundary requires a separate proof")
    decoded = {side: [tuple(map(decode, row)) for row in inputs[side]]
               for side in ["actual", "expected"]}
    if Counter(decoded["actual"]) != Counter(decoded["expected"]):
        raise ValueError("input bags differ")
    keys, value = spec["input_keys"], spec["input_value"]
    input_width = len(keys) + 1
    if sorted(keys + [value]) != list(range(input_width)):
        raise ValueError("every input column must have a role")
    bags = defaultdict(list)
    for row in decoded["actual"]:
        if len(row) != input_width:
            raise ValueError("input row width differs from registered roles")
        bags[tuple(row[k] for k in keys)].append(row[value])
    selected, mathematical, numerical = {}, {}, {}
    boundary_count = 0
    for key, bag in bags.items():
        mean, variance, cv = schedule_envelope(bag)
        mathematical[key] = sample_moments(bag)
        numerical[key] = (mean, variance, cv)
        if cv is not None:
            if cv.lower == cv.upper == spec["threshold"]:
                boundary_count += 1
            if cv.greater_than(spec["threshold"]):
                selected[key] = (mean, cv)
    join_keys = spec["join_keys"]
    selector = spec["selector_key"]
    left, right = {}, defaultdict(list)
    for key in selected:
        identity = tuple(key[i] for i in join_keys)
        if key[selector] == spec["left_value"]:
            left[key] = identity
        if key[selector] == spec["right_value"]:
            right[identity].append(key)
    reference = []
    width = spec["output_width"]
    occupied = [c for role in spec["roles"] for c in role["keys"] + [role["mean"], role["cv"]]]
    if sorted(occupied) != list(range(width)) or len(spec["roles"]) != 2:
        raise ValueError("exact output roles required")
    for lkey, identity in left.items():
        for rkey in right[identity]:
            row = [None]*width
            for key, role in zip((lkey, rkey), spec["roles"]):
                for column, key_value in zip(role["keys"], key):
                    row[column] = key_value
                row[role["mean"]], row[role["cv"]] = selected[key]
            reference.append(tuple(row))
    # A unique exact ORDER BY prefix certifies the full order, independently
    # of approximate suffixes. Ties / floating LIMIT boundaries are uncovered.
    prefix = spec["unique_order_prefix"]
    expected_order = sorted(tuple(row[i] for i in prefix) for row in reference)
    if len(set(expected_order)) != len(expected_order):
        raise ValueError("order prefix not unique; approximate peer boundary Uncovered")
    report = {"contract": VERSION, "input_groups": len(bags),
              "input_rows": len(decoded["actual"]), "selected_groups": len(selected),
              "exact_threshold_groups_excluded": boundary_count,
              "reference_rows": len(reference), "raw_exact_status": output["oracle_status"],
              "engines": {}}
    for side in ["actual", "expected"]:
        rows = [tuple(map(decode, row)) for row in output[side]]
        assert_numeric_bijection(rows, reference)
        if [tuple(row[i] for i in prefix) for row in rows] != expected_order:
            raise ValueError("exact relational ORDER BY prefix differs")
        errors = []
        for ordinal, row in enumerate(rows):
            for role in spec["roles"]:
                key = tuple(row[i] for i in role["keys"])
                for column, ref_index in [(role["mean"], 0), (role["cv"], 2)]:
                    exact = mathematical[key][ref_index]
                    errors.append({"row": ordinal, "column": column,
                                   "key": key, "reference_100_digits": str(exact),
                                   "observed_hex": row[column].hex(),
                                   "ulp": ulps(row[column], float(exact)),
                                   "enclosure": asdict(numerical[key][ref_index])})
        report["engines"][side] = {"rows": len(rows), "relational_bag_and_order": "pass",
                                   "numeric_schedule_enclosure": "pass", "errors": errors}
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for flag in ["inputs", "result", "spec", "output"]:
        parser.add_argument("--"+flag, type=Path, required=True)
    args = parser.parse_args()
    inputs = json.loads(args.inputs.read_text())
    output = json.loads(args.result.read_text())
    for identity in ["seed_sha256", "duckdb_database_sha256", "duckdb_version",
                     "duckdb_extension_sha256"]:
        if not inputs.get(identity) or inputs[identity] != output.get(identity):
            raise ValueError("input/result identity differs: " + identity)
    if len(inputs["result_sets"]) != 1 or len(output["result_sets"]) != 1:
        raise Uncovered("oracle registration requires exactly one result set")
    item = output["result_sets"][0]
    actual_schema = [(c["name"], c["logical_type"]) for c in item["actual_schema"]]
    expected_schema = [(c["name"], c["logical_type"]) for c in item["expected_schema"]]
    if actual_schema != expected_schema and item.get("checks", {}).get("schema", {}).get("status") != "pass":
        raise Uncovered("schema identity has no independent passing certificate")
    report = verify(inputs["result_sets"][0], item, json.loads(args.spec.read_text()))
    report["manifest"] = {name: {"path": str(path),
        "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
        for name, path in [("inputs", args.inputs), ("result", args.result), ("spec", args.spec)]}
    args.output.write_text(json.dumps(report, indent=2)+"\n")
    print(json.dumps({key: val for key, val in report.items() if key not in ["engines", "manifest"]}))


if __name__ == "__main__":
    main()
