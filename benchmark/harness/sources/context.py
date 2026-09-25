# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

"""Shared measurement source data objects."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from ..baseline_index import QueryKey
from ..performance_gate.policy import SourcePolicy
from ..run_output import AttemptOutput, RunOutput


@dataclass(frozen=True)
class SourceContext:
    root_dir: Path
    pid: int
    runner_module: object
    retry_query_keys: frozenset[QueryKey] = frozenset()
    minimum_sample_count: int = 1
    run_output: RunOutput | None = None
    attempt: AttemptOutput | None = None

    @property
    def output_dir(self) -> Path:
        """The only directory a source adapter may write to."""
        if self.attempt is not None:
            return self.attempt.root
        # Direct adapter tests may not have a command owner yet.  Keep their
        # fixture local without reviving the production-wide report path.
        return self.root_dir / "report" / "unowned-test-source"


@dataclass(frozen=True)
class SourceMeasurement:
    source: SourcePolicy
    payload: dict
    result_path: Path
    summary_path: Path
    failed: bool
    run_id: str | None = None
    source_id: str | None = None
    attempt_id: str | None = None
    query_case: str | None = None
    arm_id: str | None = None
    attempt_status: str = "Uncovered"
