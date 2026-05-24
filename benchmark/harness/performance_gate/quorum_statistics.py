# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

"""Statistical helpers for quorum performance gates."""

from __future__ import annotations

from dataclasses import dataclass
import math
from statistics import NormalDist
from typing import Iterable


try:
    from scipy.stats import mannwhitneyu as _SCIPY_MANNWHITNEYU  # type: ignore
except Exception:
    _SCIPY_MANNWHITNEYU = None


MANN_WHITNEY_NORMAL_ARE = 3.0 / math.pi


@dataclass(frozen=True)
class HolmDecision:
    raw_p_value: float
    alpha: float
    rejected: bool


def higher_is_worse(metric: str) -> bool:
    return metric != "throughput_per_second"


def directional_delta(metric: str, value: float, baseline: float) -> float:
    if higher_is_worse(metric):
        return max(0.0, value - baseline)
    return max(0.0, baseline - value)


def directional_percent(metric: str, value: float, baseline: float) -> float:
    if baseline == 0.0:
        return 0.0
    return (directional_delta(metric, value, baseline) / abs(baseline)) * 100.0


def rolling_p95_delta(metric: str, baseline: float, calibration_values: Iterable[float]) -> tuple[float, float]:
    deltas = sorted(directional_delta(metric, value, baseline) for value in calibration_values)
    absolute = percentile(deltas, 0.95)
    percent = (absolute / abs(baseline)) * 100.0 if baseline != 0.0 else 0.0
    return absolute, percent


def percentile(sorted_or_unsorted_values: Iterable[float], quantile: float) -> float:
    values = sorted(float(value) for value in sorted_or_unsorted_values)
    if not values:
        return 0.0
    index = max(0, math.ceil(quantile * len(values)) - 1)
    return values[min(index, len(values) - 1)]


def mann_whitney_one_sided(
    *,
    metric: str,
    current_values: Iterable[float],
    calibration_values: Iterable[float],
) -> float:
    current = [float(value) for value in current_values]
    calibration = [float(value) for value in calibration_values]
    if not current or not calibration:
        return 1.0

    if not higher_is_worse(metric):
        current = [-value for value in current]
        calibration = [-value for value in calibration]

    scipy_p = _scipy_mann_whitney_greater(current, calibration)
    if scipy_p is not None:
        return scipy_p
    return _normal_mann_whitney_greater(current, calibration)


def holm_step_down(p_values: dict[object, float], family_alpha: float) -> dict[object, HolmDecision]:
    if not p_values:
        return {}
    ordered = sorted(
        (
            (order, key, max(0.0, min(1.0, float(value))))
            for order, (key, value) in enumerate(p_values.items())
        ),
        key=lambda item: (item[2], item[0]),
    )
    decisions: dict[object, HolmDecision] = {}
    total = len(ordered)
    still_rejecting = True
    for index, (_, key, p_value) in enumerate(ordered):
        alpha_i = family_alpha / (total - index)
        rejected = still_rejecting and p_value <= alpha_i
        if not rejected:
            still_rejecting = False
        decisions[key] = HolmDecision(raw_p_value=p_value, alpha=alpha_i, rejected=rejected)
    return decisions


def estimate_effect_power(
    *,
    metric: str,
    baseline: float,
    calibration_values: Iterable[float],
    current_sample_count: int,
    effect_percent: float = 10.0,
    alpha: float = 0.01,
) -> float:
    """Return a conservative normal-approximation power estimate for MW gating.

    The variance uses the Mann-Whitney normal asymptotic relative efficiency for
    normal data so the estimate does not overstate power as aggressively as a
    plain two-sample z-test at small current sample counts.
    """

    values = [float(value) for value in calibration_values]
    if len(values) < 3 or current_sample_count <= 0 or baseline <= 0.0:
        return 0.0

    deltas = [directional_delta(metric, value, baseline) for value in values]
    mean = sum(deltas) / len(deltas)
    variance = sum((value - mean) ** 2 for value in deltas) / (len(deltas) - 1)
    stddev = math.sqrt(max(variance, 0.0))
    effect = abs(baseline) * (effect_percent / 100.0)
    if stddev == 0.0:
        return 1.0 if effect > 0.0 else 0.0

    effective_current = max(current_sample_count * MANN_WHITNEY_NORMAL_ARE, 1e-9)
    effective_calibration = max(len(values) * MANN_WHITNEY_NORMAL_ARE, 1e-9)
    stderr = stddev * math.sqrt((1.0 / effective_current) + (1.0 / effective_calibration))
    if stderr == 0.0:
        return 1.0
    normal = NormalDist()
    z_alpha = normal.inv_cdf(1.0 - max(0.0, min(1.0, alpha)))
    effect_z = effect / stderr
    return max(0.0, min(1.0, 1.0 - normal.cdf(z_alpha - effect_z)))


def _scipy_mann_whitney_greater(current: list[float], calibration: list[float]) -> float | None:
    if _SCIPY_MANNWHITNEYU is None:
        return None
    try:
        result = _SCIPY_MANNWHITNEYU(current, calibration, alternative="greater", method="auto")
    except TypeError:
        result = _SCIPY_MANNWHITNEYU(current, calibration, alternative="greater")
    return max(0.0, min(1.0, float(result.pvalue)))


def _normal_mann_whitney_greater(current: list[float], calibration: list[float]) -> float:
    n_current = len(current)
    n_calibration = len(calibration)
    combined = [(value, 0) for value in current] + [(value, 1) for value in calibration]
    combined.sort(key=lambda item: item[0])

    ranks = [0.0] * len(combined)
    tie_counts: list[int] = []
    index = 0
    while index < len(combined):
        end = index + 1
        while end < len(combined) and combined[end][0] == combined[index][0]:
            end += 1
        average_rank = (index + 1 + end) / 2.0
        for rank_index in range(index, end):
            ranks[rank_index] = average_rank
        tie_counts.append(end - index)
        index = end

    rank_sum_current = sum(rank for rank, (_, group) in zip(ranks, combined) if group == 0)
    u_current = rank_sum_current - (n_current * (n_current + 1) / 2.0)
    mean = n_current * n_calibration / 2.0
    total = n_current + n_calibration
    tie_term = sum(count**3 - count for count in tie_counts)
    variance = n_current * n_calibration / 12.0
    if total > 1:
        variance *= (total + 1) - (tie_term / (total * (total - 1)))
    if variance <= 0.0:
        return 1.0

    z = (u_current - mean - 0.5) / math.sqrt(variance)
    return max(0.0, min(1.0, 1.0 - NormalDist().cdf(z)))
