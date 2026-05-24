# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

"""Metric access helpers over benchmark query payloads."""

from __future__ import annotations

from typing import Any


def metric_value(query: dict[str, Any], metric: str) -> float | None:
    stats = query.get("stats")
    if isinstance(stats, dict):
        value = stats.get(metric)
        if is_positive_number(value):
            return float(value)

    rss = query.get("rss")
    if isinstance(rss, dict) and metric.startswith("rss_"):
        value = rss.get(metric.removeprefix("rss_"))
        if is_positive_number(value):
            return float(value)

    memory = query.get("memory")
    if isinstance(memory, dict) and metric.startswith("memory_"):
        value = memory.get(metric.removeprefix("memory_"))
        if is_number(value):
            return float(value)

    spill = query.get("spill_metrics")
    if isinstance(spill, dict) and metric.startswith("spill_"):
        value = spill.get(metric.removeprefix("spill_"))
        if is_number(value):
            return float(value)

    return None


def is_positive_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and float(value) > 0


def is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def is_resource_metric(metric: str) -> bool:
    return metric.startswith(("rss_", "memory_", "spill_"))


def sample_count(query: dict[str, Any]) -> int:
    samples_count = query.get("samples_count")
    if isinstance(samples_count, int) and not isinstance(samples_count, bool):
        return samples_count
    samples = query.get("samples_ms")
    if not isinstance(samples, list):
        return 0
    return len(samples)
