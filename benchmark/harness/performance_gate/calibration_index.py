# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

"""Index calibration observations by source/query/metric."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Any

from ..baseline_index import QueryKey, index_queries
from .metric_accessor import metric_value


@dataclass(frozen=True)
class CalibrationIndex:
    _values: dict[tuple[str, QueryKey, str], tuple[float, ...]]

    @classmethod
    def from_payload(cls, payload: dict[str, Any] | None) -> "CalibrationIndex":
        values: dict[tuple[str, QueryKey, str], list[float]] = defaultdict(list)
        if payload is None:
            return cls({})
        observations = payload.get("observations")
        if not isinstance(observations, list):
            return cls({})
        for observation in observations:
            if not isinstance(observation, dict):
                continue
            sources = observation.get("sources")
            if not isinstance(sources, list):
                continue
            for source in sources:
                if not isinstance(source, dict):
                    continue
                source_name = source.get("name")
                source_payload = source.get("payload")
                if not isinstance(source_name, str) or not isinstance(source_payload, dict):
                    continue
                for key, record in index_queries(source_payload).items():
                    for metric in _metric_names(record.query):
                        value = metric_value(record.query, metric)
                        if value is not None:
                            values[(source_name, key, metric)].append(value)
        return cls({key: tuple(items) for key, items in values.items()})

    def values(self, *, source_name: str, key: QueryKey, metric: str) -> tuple[float, ...]:
        return self._values.get((source_name, key, metric), ())


def _metric_names(query: dict[str, Any]) -> set[str]:
    names: set[str] = set()
    stats = query.get("stats")
    if isinstance(stats, dict):
        names.update(str(key) for key in stats)
    rss = query.get("rss")
    if isinstance(rss, dict):
        names.update(f"rss_{key}" for key in rss)
    memory = query.get("memory")
    if isinstance(memory, dict):
        names.update(f"memory_{key}" for key in memory)
    spill = query.get("spill_metrics")
    if isinstance(spill, dict):
        names.update(f"spill_{key}" for key in spill)
    return names
