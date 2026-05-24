# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

"""Typed performance gate entries."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from ..baseline_index import QueryKey


class GateEntryKind(str, Enum):
    COMPARED_METRIC = "ComparedMetric"
    POWER_DEFICIT = "PowerDeficit"
    MISSING_BASELINE_QUERY = "MissingBaselineQuery"
    MISSING_CURRENT_QUERY = "MissingCurrentQuery"
    MISSING_BASELINE_METRIC = "MissingBaselineMetric"
    MISSING_CURRENT_METRIC = "MissingCurrentMetric"
    QUERY_EXECUTION_ERROR = "QueryExecutionError"
    INVALID_BASELINE = "InvalidBaseline"
    STAGING_QUERY = "StagingQuery"


class GateStatus(str, Enum):
    OK = "OK"
    IMPROVE = "IMPROVE"
    REGRESS = "REGRESS"
    UNMEASURABLE = "UNMEASURABLE"


@dataclass(frozen=True)
class GateEntry:
    kind: GateEntryKind
    key: QueryKey
    metric: str
    status: GateStatus
    baseline_value: float | None = None
    current_value: float | None = None
    change_percent: float | None = None
    p_value: float | None = None
    holm_alpha: float | None = None
    noise_floor_abs: float | None = None
    noise_floor_percent: float | None = None
    calibration_samples: int = 0
    statistical_power: float | None = None
    detail: str | None = None

    @property
    def workload(self) -> str:
        return self.key.workload

    @property
    def query_id(self) -> str:
        return self.key.query_id
