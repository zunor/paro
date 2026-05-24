# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

"""Shared measurement source data objects."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from ..baseline_index import QueryKey
from ..performance_gate.policy import SourcePolicy


@dataclass(frozen=True)
class SourceContext:
    root_dir: Path
    pid: int
    runner_module: object
    retry_query_keys: frozenset[QueryKey] = frozenset()
    minimum_sample_count: int = 1


@dataclass(frozen=True)
class SourceMeasurement:
    source: SourcePolicy
    payload: dict
    result_path: Path
    summary_path: Path
    failed: bool
