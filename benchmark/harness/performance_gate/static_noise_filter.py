# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

"""Static fallback noise rules for gates without usable calibration."""

from __future__ import annotations

from typing import Any

from .entry import GateStatus
from .metric_accessor import is_resource_metric, metric_value, sample_count
from .policy import GatePolicy


def metric_status(
    *,
    metric: str,
    current: float,
    baseline: float,
    change_percent: float,
    policy: GatePolicy,
    apply_static_noise: bool,
) -> GateStatus:
    if metric == "throughput_per_second":
        baseline_ms = 1000.0 / baseline
        current_ms = 1000.0 / current
        if apply_static_noise and abs(current_ms - baseline_ms) <= policy.latency_noise_floor_ms:
            return GateStatus.OK
        ratio = current / baseline
        if ratio < policy.throughput_min_ratio:
            return GateStatus.REGRESS
        if ratio > (1.0 / max(policy.throughput_min_ratio, 0.001)):
            return GateStatus.IMPROVE
        return GateStatus.OK

    noise_floor = _metric_noise_floor(metric, policy)
    if apply_static_noise and noise_floor > 0.0 and abs(current - baseline) <= noise_floor:
        return GateStatus.OK

    threshold = policy.resource_regression_percent if is_resource_metric(metric) else policy.latency_regression_percent
    if change_percent > threshold:
        return GateStatus.REGRESS
    if change_percent < -threshold:
        return GateStatus.IMPROVE
    return GateStatus.OK


def apply_static_noise_filters(
    *,
    status: GateStatus,
    metric: str,
    current: float,
    baseline: float,
    current_query: dict[str, Any],
    baseline_query: dict[str, Any],
    policy: GatePolicy,
) -> GateStatus:
    if status != GateStatus.REGRESS:
        return status

    latency_noise_floor_ms = _effective_latency_noise_floor(current_query, baseline_query, policy)
    if metric in {"p50", "p90"} and _latency_regression_is_noise(current, baseline, latency_noise_floor_ms):
        return GateStatus.OK
    if metric == "throughput_per_second" and (
        _throughput_latency_regression_is_noise(current, baseline, latency_noise_floor_ms)
        or _short_query_sample_noise(current_query, baseline_query, policy, latency_noise_floor_ms)
    ):
        return GateStatus.OK
    if metric in {"p99", "p999"} and _tail_only_regression_is_noise(
        current_query,
        baseline_query,
        policy,
        latency_noise_floor_ms,
    ):
        return GateStatus.OK
    return status


def _metric_noise_floor(metric: str, policy: GatePolicy) -> float:
    if metric.startswith("rss_"):
        return policy.rss_noise_floor_kb
    if metric.startswith(("memory_", "spill_")):
        return policy.resource_noise_floor_kb
    if metric in {"p99", "p999"}:
        return policy.p99_latency_noise_floor_ms
    return policy.latency_noise_floor_ms


def _effective_latency_noise_floor(
    query: dict[str, Any],
    baseline_query: dict[str, Any],
    policy: GatePolicy,
) -> float:
    if policy.short_query_latency_noise_floor_ms <= policy.latency_noise_floor_ms:
        return policy.latency_noise_floor_ms
    if not _is_short_query(query, baseline_query, policy):
        return policy.latency_noise_floor_ms
    return policy.short_query_latency_noise_floor_ms


def _latency_regression_is_noise(
    current: float,
    baseline: float,
    latency_noise_floor_ms: float,
) -> bool:
    return latency_noise_floor_ms > 0.0 and abs(current - baseline) <= latency_noise_floor_ms


def _throughput_latency_regression_is_noise(
    current_qps: float,
    baseline_qps: float,
    latency_noise_floor_ms: float,
) -> bool:
    if current_qps <= 0.0 or baseline_qps <= 0.0:
        return False
    baseline_ms = 1000.0 / baseline_qps
    current_ms = 1000.0 / current_qps
    return _latency_regression_is_noise(current_ms, baseline_ms, latency_noise_floor_ms)


def _tail_only_regression_is_noise(
    query: dict[str, Any],
    baseline_query: dict[str, Any],
    policy: GatePolicy,
    latency_noise_floor_ms: float,
) -> bool:
    current_qps = metric_value(query, "throughput_per_second")
    baseline_qps = metric_value(baseline_query, "throughput_per_second")
    if current_qps is None or baseline_qps is None or baseline_qps <= 0.0:
        return False
    qps_ok = current_qps / baseline_qps >= policy.throughput_min_ratio
    if not qps_ok:
        qps_ok = _short_query_sample_noise(query, baseline_query, policy, latency_noise_floor_ms)
    return qps_ok and _p50_within_policy(query, baseline_query, policy, latency_noise_floor_ms)


def _short_query_sample_noise(
    query: dict[str, Any],
    baseline_query: dict[str, Any],
    policy: GatePolicy,
    latency_noise_floor_ms: float,
) -> bool:
    if policy.tail_sample_min_count <= 0:
        return False
    count = sample_count(query)
    if count <= 0 or count >= policy.tail_sample_min_count:
        return False
    if not _is_short_query(query, baseline_query, policy):
        return False
    return _p50_within_policy(query, baseline_query, policy, latency_noise_floor_ms)


def _p50_within_policy(
    query: dict[str, Any],
    baseline_query: dict[str, Any],
    policy: GatePolicy,
    latency_noise_floor_ms: float,
) -> bool:
    current_p50 = metric_value(query, "p50")
    baseline_p50 = metric_value(baseline_query, "p50")
    if current_p50 is None or baseline_p50 is None or baseline_p50 <= 0.0:
        return False
    p50_change = ((current_p50 - baseline_p50) / baseline_p50) * 100.0
    return (
        _latency_regression_is_noise(current_p50, baseline_p50, latency_noise_floor_ms)
        or p50_change <= policy.latency_regression_percent
    )


def _is_short_query(
    query: dict[str, Any],
    baseline_query: dict[str, Any],
    policy: GatePolicy,
) -> bool:
    if policy.short_query_max_p50_ms <= 0.0:
        return False
    current_p50 = metric_value(query, "p50")
    baseline_p50 = metric_value(baseline_query, "p50")
    if current_p50 is None or baseline_p50 is None:
        return False
    return max(current_p50, baseline_p50) <= policy.short_query_max_p50_ms
