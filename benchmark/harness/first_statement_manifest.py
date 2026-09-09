#!/usr/bin/env python3
# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

"""Load and validate the frozen first-statement family manifest."""

from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any


MANIFEST_PATH = (
    Path(__file__).resolve().parents[1]
    / "evidence"
    / "first-statement"
    / "family-manifest-v1.json"
)
REQUIRED_CASE_BUCKETS = ("positive", "boundary_or_not_applicable", "held_out")
REQUIRED_GATE_SECTIONS = ("correctness", "cold_c1", "warm_execution", "resources")
REQUIRED_GATE_FIELDS = {
    "correctness": (
        "complete_typed_result",
        "schema_exact",
        "ordering_and_bag_exact",
    ),
    "cold_c1": (
        "median_regression_ratio_max",
        "p95_regression_ratio_max",
        "samples_per_engine_min",
    ),
    "warm_execution": (
        "median_regression_ratio_max",
        "p95_regression_ratio_max",
    ),
    "resources": (
        "peak_rss_regression_ratio_max",
        "planning_reserved_bytes_regression_ratio_max",
        "execution_dop_must_be_declared",
        "planning_dop_must_be_declared",
    ),
}


def load_first_statement_manifest(path: Path = MANIFEST_PATH) -> dict[str, Any]:
    document = json.loads(path.read_text(encoding="utf-8"))
    validate_first_statement_manifest(document)
    return document


def validate_first_statement_manifest(document: dict[str, Any]) -> None:
    if not isinstance(document, dict) or document.get("schema_version") != 1:
        raise ValueError("first-statement manifest must use schema version 1")
    if not document.get("manifest_id") or document.get("production_identity_inputs") != []:
        raise ValueError("manifest must be frozen independently of production SQL identity")
    gate = document.get("default_gate")
    if not isinstance(gate, dict):
        raise ValueError("manifest is missing default_gate")
    profile_id = gate.get("profile_id")
    if not isinstance(profile_id, str) or not profile_id:
        raise ValueError("manifest default_gate is missing profile_id")
    for section in REQUIRED_GATE_SECTIONS:
        if not isinstance(gate.get(section), dict) or not gate[section]:
            raise ValueError(f"manifest gate is missing {section}")
        missing = set(REQUIRED_GATE_FIELDS[section]) - set(gate[section])
        if missing:
            raise ValueError(f"manifest gate {section} is missing {sorted(missing)}")
    for field in REQUIRED_GATE_FIELDS["correctness"]:
        if gate["correctness"][field] is not True:
            raise ValueError(f"manifest correctness gate {field} must be true")
    for section in ("cold_c1", "warm_execution", "resources"):
        for field, value in gate[section].items():
            if field.endswith("_must_be_declared"):
                if value is not True:
                    raise ValueError(f"manifest resource gate {field} must be true")
            elif field == "samples_per_engine_min":
                if not isinstance(value, int) or isinstance(value, bool) or value < 1:
                    raise ValueError("manifest cold_c1 samples_per_engine_min must be a positive integer")
            elif not isinstance(value, (int, float)) or isinstance(value, bool) or not math.isfinite(value) or value <= 0:
                raise ValueError(f"manifest gate {section}.{field} must be finite and positive")
    if gate.get("missing_evidence") != "Uncovered":
        raise ValueError("manifest gate missing_evidence must be Uncovered")
    families = document.get("families")
    if not isinstance(families, list) or len(families) != 7:
        raise ValueError("manifest must register exactly F1 through F7")
    identifiers = []
    for family in families:
        if not isinstance(family, dict):
            raise ValueError("family entry must be an object")
        family_id = family.get("id")
        identifiers.append(family_id)
        if family_id not in {f"F{index}" for index in range(1, 8)}:
            raise ValueError(f"unknown family id: {family_id!r}")
        if not family.get("capabilities") or not family.get("unsupported_scope"):
            raise ValueError(f"{family_id} must declare capabilities and unsupported_scope")
        if family.get("gate_profile") != profile_id:
            raise ValueError(f"{family_id} gate_profile must bind every case to {profile_id}")
        for bucket in REQUIRED_CASE_BUCKETS:
            cases = family.get(bucket)
            if not isinstance(cases, list) or not cases:
                raise ValueError(f"{family_id} must have a non-empty {bucket} bucket")
            if any(not isinstance(case, str) or "/" not in case for case in cases):
                raise ValueError(f"{family_id} has an invalid case reference in {bucket}")
    if identifiers != [f"F{index}" for index in range(1, 8)]:
        raise ValueError("families must be ordered F1 through F7")


def case_gate_profiles(document: dict[str, Any]) -> dict[str, str]:
    """Return the frozen gate profile for every registered case reference.

    The family manifest stores one pre-registered profile binding per family;
    expanding it here makes the per-case coverage auditable without copying a
    second mutable threshold object into every bucket.
    """

    validate_first_statement_manifest(document)
    result: dict[str, str] = {}
    for family in document["families"]:
        profile = family["gate_profile"]
        for bucket in REQUIRED_CASE_BUCKETS:
            for case in family[bucket]:
                if case in result:
                    raise ValueError(f"case reference is registered more than once: {case}")
                result[case] = profile
    return result
