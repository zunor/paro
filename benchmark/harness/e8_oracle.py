# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

"""Small independent semantics and calibration oracle for D1-C/E8.

This module intentionally does not import the optimizer, EXPLAIN parser, or
the production quality gate.  It models only finite bag semantics and the
evidence rules needed to decide whether a statistic can be used as an exact
conditional-domain claim.  A passing oracle test is not a Q11 calibration
result; it prevents a broken evidence representation from being admitted.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Iterable, Mapping, Sequence


Row = Mapping[str, Any]


class EvidenceStatus(str, Enum):
    OBSERVED_FULL = "ObservedFull"
    OBSERVED_PARTIAL = "ObservedPartial"
    DERIVED = "Derived"
    UNKNOWN = "Unknown"


@dataclass(frozen=True)
class DomainEvidence:
    """A conditional value domain with explicit provenance and coverage."""

    column_id: str
    values: frozenset[Any]
    status: EvidenceStatus
    condition_key: str | None = None
    coverage: float = 0.0
    source: str = ""

    def validate(self) -> None:
        if not self.column_id:
            raise ValueError("domain evidence requires a stable column id")
        if not math.isfinite(self.coverage) or not 0 <= self.coverage <= 1:
            raise ValueError("domain evidence coverage must be finite in [0, 1]")
        if self.status is EvidenceStatus.OBSERVED_FULL and self.coverage != 1:
            raise ValueError("ObservedFull evidence requires complete coverage")
        if self.status not in (EvidenceStatus.OBSERVED_PARTIAL, EvidenceStatus.UNKNOWN) \
                and self.coverage == 0:
            raise ValueError("non-partial evidence needs a nonzero coverage claim")


def observed_domain(rows: Iterable[Row], column_id: str, *, condition_key: str | None = None,
                    source: str = "finite_bag") -> DomainEvidence:
    result = DomainEvidence(column_id, frozenset(row.get(column_id) for row in rows),
                            EvidenceStatus.OBSERVED_FULL, condition_key, 1.0, source)
    result.validate()
    return result


def partial_domain(values: Iterable[Any], column_id: str, coverage: float,
                   *, condition_key: str | None = None, source: str = "sketch") -> DomainEvidence:
    result = DomainEvidence(column_id, frozenset(values), EvidenceStatus.OBSERVED_PARTIAL,
                            condition_key, coverage, source)
    result.validate()
    return result


def derived_domain(values: Iterable[Any], column_id: str, *, condition_key: str | None = None,
                   source: str = "model") -> DomainEvidence:
    result = DomainEvidence(column_id, frozenset(values), EvidenceStatus.DERIVED,
                            condition_key, 1.0, source)
    result.validate()
    return result


def unknown_domain(column_id: str, *, source: str = "missing") -> DomainEvidence:
    return DomainEvidence(column_id, frozenset(), EvidenceStatus.UNKNOWN, None, 0.0, source)


def exact_values(evidence: DomainEvidence) -> frozenset[Any] | None:
    """Return values only when the evidence is a full exact observation."""

    evidence.validate()
    if evidence.status is not EvidenceStatus.OBSERVED_FULL:
        return None
    return evidence.values


def filter_domain(evidence: DomainEvidence, allowed: Iterable[Any], condition_key: str,
                  *, source: str = "filter") -> DomainEvidence:
    """Apply one predicate once; partial/model domains cannot shrink safely."""

    evidence.validate()
    if evidence.condition_key == condition_key:
        return evidence
    values = exact_values(evidence)
    if values is None:
        return unknown_domain(evidence.column_id, source=f"{source}:non_exact_input")
    result = DomainEvidence(evidence.column_id, values.intersection(allowed),
                            EvidenceStatus.OBSERVED_FULL, condition_key, 1.0, source)
    result.validate()
    return result


def equality_join_domain(left: DomainEvidence, right: DomainEvidence) -> DomainEvidence:
    """Intersect two complete equality domains, retaining NULL semantics."""

    left.validate()
    right.validate()
    left_values, right_values = exact_values(left), exact_values(right)
    if left_values is None or right_values is None:
        return unknown_domain(left.column_id, source="join:non_exact_input")
    return DomainEvidence(left.column_id, frozenset(
        value for value in left_values.intersection(right_values) if value is not None
    ), EvidenceStatus.OBSERVED_FULL, "equality_join", 1.0, "finite_bag_join")


def sql_filter(rows: Iterable[Row], predicate: Callable[[Row], bool | None]) -> list[Row]:
    """SQL WHERE retains TRUE and rejects FALSE/UNKNOWN, preserving bag order."""

    return [row for row in rows if predicate(row) is True]


def inner_join(left: Sequence[Row], right: Sequence[Row], left_key: str,
               right_key: str) -> list[dict[str, Any]]:
    """Nested-loop bag join: duplicate keys multiply and NULL never equals NULL."""

    result: list[dict[str, Any]] = []
    for left_row in left:
        for right_row in right:
            key = left_row.get(left_key)
            if key is not None and key == right_row.get(right_key):
                result.append({"left": dict(left_row), "right": dict(right_row)})
    return result


def left_outer_join(left: Sequence[Row], right: Sequence[Row], left_key: str,
                    right_key: str, right_columns: Iterable[str]) -> list[dict[str, Any]]:
    """Left outer bag join with one NULL-extended row for an unmatched input."""

    columns = tuple(right_columns)
    result: list[dict[str, Any]] = []
    for left_row in left:
        matches = [right_row for right_row in right
                   if left_row.get(left_key) is not None
                   and left_row.get(left_key) == right_row.get(right_key)]
        if matches:
            result.extend({"left": dict(left_row), "right": dict(right_row)}
                          for right_row in matches)
        else:
            result.append({"left": dict(left_row),
                          "right": {column: None for column in columns}})
    return result


def group_count(rows: Iterable[Row], key: str) -> dict[Any, int]:
    """Group a bag without erasing duplicate rows."""

    result: dict[Any, int] = {}
    for row in rows:
        value = row.get(key)
        result[value] = result.get(value, 0) + 1
    return result


@dataclass(frozen=True)
class NodeObservation:
    occurrence_id: str
    column_id: str
    estimated_rows: float
    actual_rows: float
    coverage: float
    held_out: bool = False


@dataclass(frozen=True)
class CalibrationGate:
    q_error_p95_max: float
    q_error_max: float
    coverage_min: float
    held_out_coverage_min: float = 1.0


def _cardinality(value: float) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError("cardinality must be numeric")
    if not math.isfinite(value) or value < 0:
        raise ValueError("cardinality must be finite and non-negative")
    return float(value)


def oracle_q_error(estimated_rows: float, actual_rows: float) -> float:
    estimated_rows, actual_rows = _cardinality(estimated_rows), _cardinality(actual_rows)
    if estimated_rows == actual_rows == 0:
        return 1.0
    if estimated_rows == 0 or actual_rows == 0:
        return math.inf
    return max(estimated_rows / actual_rows, actual_rows / estimated_rows)


def _nearest_rank(values: Sequence[float], fraction: float) -> float:
    ordered = sorted(values)
    if not ordered:
        raise ValueError("calibration requires at least one observation")
    index = max(0, min(len(ordered) - 1, math.ceil(fraction * len(ordered)) - 1))
    return ordered[index]


def calibrate_g_stats(observations: Sequence[NodeObservation], gate: CalibrationGate) -> dict[str, Any]:
    """Independently evaluate q-error and coverage, failing closed on gaps."""

    if not observations:
        raise ValueError("G-Stats calibration has no observations")
    if any(not item.occurrence_id or not item.column_id for item in observations):
        raise ValueError("G-Stats observations require occurrence and column identities")
    if len({item.occurrence_id for item in observations}) != len(observations):
        raise ValueError("G-Stats occurrence identities must be unique")
    if any(not math.isfinite(item.coverage) or not 0 <= item.coverage <= 1 for item in observations):
        raise ValueError("G-Stats coverage must be finite in [0, 1]")
    values = [oracle_q_error(item.estimated_rows, item.actual_rows) for item in observations]
    held_out = [item for item in observations if item.held_out]
    coverage = min(item.coverage for item in observations)
    held_out_coverage = min((item.coverage for item in held_out), default=0.0)
    p95 = _nearest_rank(values, .95)
    passed = (all(math.isfinite(value) and value <= gate.q_error_max for value in values)
              and p95 <= gate.q_error_p95_max
              and coverage >= gate.coverage_min
              and held_out_coverage >= gate.held_out_coverage_min)
    return {"status": "passed" if passed else "rejected", "count": len(values),
            "q_error_p95": p95, "q_error_max": max(values), "coverage_min": coverage,
            "held_out_coverage_min": held_out_coverage}


@dataclass(frozen=True)
class CandidateObservation:
    candidate_id: str
    predicted_cost: float
    actual_cost: float
    selected: bool


def candidate_selection_loss(candidates: Sequence[CandidateObservation]) -> float:
    """Compare selected actual cost with the exact finite-domain optimum."""

    if not candidates or sum(candidate.selected for candidate in candidates) != 1:
        raise ValueError("candidate oracle requires exactly one selected candidate")
    if any(not math.isfinite(candidate.actual_cost) or candidate.actual_cost < 0
           for candidate in candidates):
        raise ValueError("candidate actual costs must be finite and non-negative")
    selected = next(candidate for candidate in candidates if candidate.selected)
    optimum = min(candidate.actual_cost for candidate in candidates)
    if optimum == 0:
        return 1.0 if selected.actual_cost == 0 else math.inf
    return selected.actual_cost / optimum
