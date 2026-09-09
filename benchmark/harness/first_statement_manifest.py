#!/usr/bin/env python3
# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

"""Load and validate the frozen first-statement family manifest."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


MANIFEST_PATH = (
    Path(__file__).resolve().parents[1]
    / "evidence"
    / "first-statement"
    / "family-manifest-v1.json"
)
REQUIRED_CASE_BUCKETS = ("positive", "boundary_or_not_applicable", "held_out")


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
    for section in ("correctness", "cold_c1", "warm_execution", "resources"):
        if not isinstance(gate.get(section), dict) or not gate[section]:
            raise ValueError(f"manifest gate is missing {section}")
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
        for bucket in REQUIRED_CASE_BUCKETS:
            cases = family.get(bucket)
            if not isinstance(cases, list) or not cases:
                raise ValueError(f"{family_id} must have a non-empty {bucket} bucket")
            if any(not isinstance(case, str) or "/" not in case for case in cases):
                raise ValueError(f"{family_id} has an invalid case reference in {bucket}")
    if identifiers != [f"F{index}" for index in range(1, 8)]:
        raise ValueError("families must be ordered F1 through F7")

