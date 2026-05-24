# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

"""Performance gate evaluation."""

from .baseline import (
    BaselineError,
    BaselineMeasurement,
    GateBaseline,
    aggregate_measurement_runs,
    baseline_payload_for_source,
    build_baseline_payload,
    is_missing_fingerprint_value,
    load_baseline,
    project_measurement_payload,
    validate_baseline_for_check,
    validate_existing_baseline_for_bless,
)
from .entry import GateEntry, GateEntryKind, GateStatus
from .evaluator import evaluate_gate
from .fingerprint import (
    FingerprintError,
    GateFingerprint,
    collect_fingerprint,
    platform_key,
    validate_bless_fingerprint,
)
from .outcome import GateEnforcement, GateOutcome
from .policy import (
    CalibrationPolicy,
    CoveragePolicy,
    GatePolicy,
    PolicyError,
    SourcePolicy,
    StatisticsPolicy,
    load_policy,
)
from .staging import StagingError, StagingQuery, load_staging_queries

__all__ = [
    "BaselineError",
    "BaselineMeasurement",
    "CalibrationPolicy",
    "CoveragePolicy",
    "FingerprintError",
    "GateBaseline",
    "GateEntry",
    "GateEntryKind",
    "GateEnforcement",
    "GateFingerprint",
    "GateOutcome",
    "GatePolicy",
    "GateStatus",
    "PolicyError",
    "SourcePolicy",
    "StagingError",
    "StagingQuery",
    "StatisticsPolicy",
    "aggregate_measurement_runs",
    "baseline_payload_for_source",
    "build_baseline_payload",
    "collect_fingerprint",
    "evaluate_gate",
    "is_missing_fingerprint_value",
    "load_baseline",
    "load_policy",
    "load_staging_queries",
    "platform_key",
    "project_measurement_payload",
    "validate_baseline_for_check",
    "validate_bless_fingerprint",
    "validate_existing_baseline_for_bless",
]
