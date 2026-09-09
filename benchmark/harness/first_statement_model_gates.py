# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

"""Load the pre-registered D1-C/G-Stats/G-Cost acceptance contract."""

from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any


MODEL_GATES_PATH = (
    Path(__file__).resolve().parents[1]
    / "evidence"
    / "first-statement"
    / "model-gates-v1.json"
)
REQUIRED_NODES = (
    "scan",
    "date_join",
    "partial_aggregate",
    "final_aggregate",
    "residual_filter",
    "cte_producer",
    "cte_consumer",
    "root_join",
)
REQUIRED_GATE_THRESHOLDS = {
    "G-Stats": (
        "q_error_p95_max",
        "q_error_max",
        "conditional_domain_coverage_min",
        "held_out_coverage_min",
    ),
    "G-Cost": (
        "phase_time_prediction_p95_ratio_max",
        "candidate_selection_loss_p95_ratio_max",
        "cold_first_statement_prediction_p95_ratio_max",
        "held_out_coverage_min",
    ),
}


def load_first_statement_model_gates(path: Path = MODEL_GATES_PATH) -> dict[str, Any]:
    document = json.loads(path.read_text(encoding="utf-8"))
    validate_first_statement_model_gates(document)
    return document


def validate_first_statement_model_gates(document: dict[str, Any]) -> None:
    if not isinstance(document, dict) or document.get("schema_version") != 1:
        raise ValueError("first-statement model gates must use schema version 1")
    if document.get("manifest_id") != "first-statement-model-gates-v1":
        raise ValueError("model gates manifest identity is not frozen")
    if document.get("status") != "registered_not_admitted":
        raise ValueError("model gates may not be admitted by registration alone")
    scope = document.get("scope")
    if not isinstance(scope, dict) or scope.get("held_out_required") is not True:
        raise ValueError("model gates must require held-out evidence")
    nodes = scope.get("occurrence_nodes")
    if nodes != list(REQUIRED_NODES):
        raise ValueError("model gates must register the complete Q11 occurrence node list")
    identities = scope.get("required_identity")
    if not isinstance(identities, list) or len(set(identities)) != len(identities):
        raise ValueError("model gates require unique occurrence and evidence identities")
    gates = document.get("gates")
    if not isinstance(gates, dict) or set(gates) != set(REQUIRED_GATE_THRESHOLDS):
        raise ValueError("model gates must register exactly G-Stats and G-Cost")
    for name, required in REQUIRED_GATE_THRESHOLDS.items():
        gate = gates[name]
        if not isinstance(gate, dict) or gate.get("admission") is None:
            raise ValueError(f"{name} must declare an admission rule")
        thresholds = gate.get("thresholds")
        if not isinstance(thresholds, dict) or set(required) - set(thresholds):
            raise ValueError(f"{name} is missing a registered threshold")
        for field in required:
            value = thresholds[field]
            if not isinstance(value, (int, float)) or isinstance(value, bool) or not math.isfinite(value) or value <= 0:
                raise ValueError(f"{name}.{field} must be finite and positive")
    if document.get("missing_evidence") != "Uncovered":
        raise ValueError("model gates must fail closed as Uncovered")
    if document.get("observations") != []:
        raise ValueError("pre-registered model gates cannot contain calibration observations")
