# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

"""Measurement source registry for benchmark gates."""

from __future__ import annotations

from typing import Protocol

from ..performance_gate.policy import SourcePolicy
from .context import SourceContext, SourceMeasurement


class MeasurementSource(Protocol):
    source_type: str

    def execute(
        self,
        source: SourcePolicy,
        context: SourceContext,
    ) -> SourceMeasurement:
        ...


class SourceRegistry:
    def __init__(self) -> None:
        self._sources: dict[str, MeasurementSource] = {}

    def register(self, source: MeasurementSource) -> None:
        self._sources[source.source_type] = source

    def get(self, source_type: str) -> MeasurementSource:
        try:
            return self._sources[source_type]
        except KeyError as exc:
            raise ValueError(f"unknown measurement source type: {source_type}") from exc


def default_registry() -> SourceRegistry:
    from .divan_bench import DivanBenchSource
    from .sql_suite import SqlSuiteSource

    registry = SourceRegistry()
    registry.register(DivanBenchSource())
    registry.register(SqlSuiteSource())
    return registry
