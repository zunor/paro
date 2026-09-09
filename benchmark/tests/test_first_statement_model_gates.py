# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import copy

import pytest

from harness.first_statement_model_gates import (
    load_first_statement_model_gates,
    validate_first_statement_model_gates,
)


def test_first_statement_model_gates_are_registered_but_not_admitted() -> None:
    document = load_first_statement_model_gates()
    assert document["status"] == "registered_not_admitted"
    assert document["observations"] == []
    assert document["scope"]["held_out_required"] is True


def test_first_statement_model_gates_reject_missing_q11_node() -> None:
    document = copy.deepcopy(load_first_statement_model_gates())
    document["scope"]["occurrence_nodes"].pop()
    with pytest.raises(ValueError, match="complete Q11 occurrence"):
        validate_first_statement_model_gates(document)


def test_first_statement_model_gates_reject_admission_without_observations() -> None:
    document = copy.deepcopy(load_first_statement_model_gates())
    document["status"] = "admitted"
    with pytest.raises(ValueError, match="not be admitted"):
        validate_first_statement_model_gates(document)


def test_first_statement_model_gates_reject_nonpositive_threshold() -> None:
    document = copy.deepcopy(load_first_statement_model_gates())
    document["gates"]["G-Cost"]["thresholds"]["candidate_selection_loss_p95_ratio_max"] = 0
    with pytest.raises(ValueError, match="finite and positive"):
        validate_first_statement_model_gates(document)
