# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import copy

import pytest

from harness.first_statement_manifest import (
    case_gate_profiles,
    load_first_statement_manifest,
    validate_first_statement_manifest,
)


def test_first_statement_manifest_is_frozen_and_complete() -> None:
    manifest = load_first_statement_manifest()
    assert manifest["production_identity_inputs"] == []
    assert [family["id"] for family in manifest["families"]] == [f"F{i}" for i in range(1, 8)]
    profiles = case_gate_profiles(manifest)
    assert profiles
    assert set(profiles.values()) == {manifest["default_gate"]["profile_id"]}


@pytest.mark.parametrize("bucket", ["positive", "boundary_or_not_applicable", "held_out"])
def test_first_statement_manifest_rejects_empty_coverage_bucket(bucket: str) -> None:
    manifest = copy.deepcopy(load_first_statement_manifest())
    manifest["families"][0][bucket] = []
    with pytest.raises(ValueError, match=bucket):
        validate_first_statement_manifest(manifest)


def test_first_statement_manifest_rejects_production_identity() -> None:
    manifest = copy.deepcopy(load_first_statement_manifest())
    manifest["production_identity_inputs"] = ["q11.sql"]
    with pytest.raises(ValueError, match="production SQL identity"):
        validate_first_statement_manifest(manifest)


def test_first_statement_manifest_rejects_unbound_family_gate() -> None:
    manifest = copy.deepcopy(load_first_statement_manifest())
    manifest["families"][0].pop("gate_profile")
    with pytest.raises(ValueError, match="gate_profile"):
        validate_first_statement_manifest(manifest)


def test_first_statement_manifest_rejects_invalid_threshold() -> None:
    manifest = copy.deepcopy(load_first_statement_manifest())
    manifest["default_gate"]["cold_c1"]["p95_regression_ratio_max"] = 0
    with pytest.raises(ValueError, match="finite and positive"):
        validate_first_statement_manifest(manifest)


def test_case_gate_profiles_reject_duplicate_case_references() -> None:
    manifest = copy.deepcopy(load_first_statement_manifest())
    manifest["families"][1]["positive"].append(manifest["families"][0]["positive"][0])
    with pytest.raises(ValueError, match="more than once"):
        case_gate_profiles(manifest)
