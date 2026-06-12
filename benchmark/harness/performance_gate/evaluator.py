# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

"""Performance gate evaluator."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import date
from typing import Any

from ..baseline_index import QueryKey, index_queries
from .calibration_index import CalibrationIndex
from .entry import GateEntry, GateEntryKind, GateStatus
from .metric_accessor import is_resource_metric, metric_value
from .outcome import GateOutcome
from .policy import GatePolicy
from .quorum_statistics import (
    directional_delta,
    directional_percent,
    estimate_effect_power,
    holm_step_down,
    mann_whitney_one_sided,
    rolling_p95_delta,
)
from .static_noise_filter import apply_static_noise_filters, metric_status
from .staging import StagingMap


STATISTICS_MODEL = "mann_whitney_rolling_p95_holm"
NOISE_FLOOR_FORMULA = "directional_rolling_p95_delta"


@dataclass(frozen=True)
class _StatisticalCandidate:
    entry_index: int
    p_value: float
    power: float


def evaluate_gate(
    *,
    policy: GatePolicy,
    current_payload: dict[str, Any],
    baseline_payload: dict[str, Any],
    calibration_payload: dict[str, Any] | None = None,
    source_name: str | None = None,
    confirmation_payloads: tuple[dict[str, Any], ...] = (),
    only_keys: frozenset[QueryKey] | None = None,
    statistical_phase: str = "first",
    staging_queries: StagingMap | None = None,
    today: date | None = None,
) -> GateOutcome:
    current_index = index_queries(current_payload)
    baseline_index = index_queries(baseline_payload)
    keys = sorted(set(current_index) | set(baseline_index))
    if only_keys is not None:
        keys = [key for key in keys if key in only_keys]
    confirmation_indexes = tuple(index_queries(payload) for payload in confirmation_payloads)
    use_statistics = (
        calibration_payload is not None
        and source_name is not None
        and policy.statistics.model == STATISTICS_MODEL
        and policy.calibration.noise_floor_formula == NOISE_FLOOR_FORMULA
    )
    calibration_index = CalibrationIndex.from_payload(calibration_payload) if use_statistics else None
    entries: list[GateEntry] = []
    candidates: list[_StatisticalCandidate] = []
    active_date = date.today() if today is None else today
    staging = staging_queries or {}

    for key in keys:
        current_record = current_index.get(key)
        baseline_record = baseline_index.get(key)

        if current_record is None:
            entries.append(
                GateEntry(
                    kind=GateEntryKind.MISSING_CURRENT_QUERY,
                    key=key,
                    metric="query",
                    status=GateStatus.REGRESS,
                    detail="query is present in baseline but missing from current run",
                )
            )
            continue

        query_error = current_record.query.get("error")
        if query_error:
            entries.append(
                GateEntry(
                    kind=GateEntryKind.QUERY_EXECUTION_ERROR,
                    key=key,
                    metric="query",
                    status=GateStatus.REGRESS,
                    detail=str(query_error),
                )
            )
            continue

        if baseline_record is None:
            staging_query = staging.get((key.workload, key.query_id))
            if staging_query is not None and staging_query.active_on(active_date):
                entries.append(
                    GateEntry(
                        kind=GateEntryKind.STAGING_QUERY,
                        key=key,
                        metric="query",
                        status=GateStatus.UNMEASURABLE,
                        detail=f"query is staging until {staging_query.staging_until.isoformat()}",
                    )
                )
                continue
            entries.append(
                GateEntry(
                    kind=GateEntryKind.MISSING_BASELINE_QUERY,
                    key=key,
                    metric="query",
                    status=GateStatus.REGRESS,
                    detail=_missing_baseline_detail(key, staging, active_date),
                )
            )
            continue

        for metric in policy.metrics:
            entry = _evaluate_metric(
                key=key,
                metric=metric,
                current_query=current_record.query,
                baseline_query=baseline_record.query,
                policy=policy,
                apply_static_noise=not use_statistics,
            )
            if use_statistics and calibration_index is not None and source_name is not None:
                entry, candidate = _apply_statistical_model(
                    entry=entry,
                    key=key,
                    metric=metric,
                    policy=policy,
                    statistical_phase=statistical_phase,
                    source_name=source_name,
                    calibration_index=calibration_index,
                    confirmation_indexes=confirmation_indexes,
                    current_query=current_record.query,
                )
                if candidate is not None:
                    candidates.append(replace(candidate, entry_index=len(entries)))
            entries.append(entry)

    entries = _apply_holm(entries, candidates, policy=policy, statistical_phase=statistical_phase)
    entries.extend(_power_deficit_entries(entries, candidates, policy=policy))
    return GateOutcome(
        gate=policy.name,
        enforcement=policy.enforcement,
        entries=tuple(sorted(
            entries,
            key=lambda entry: (entry.workload, entry.query_id, entry.metric, entry.kind.value),
        )),
    )


def _missing_baseline_detail(
    key: QueryKey,
    staging: StagingMap,
    today: date,
) -> str:
    staging_query = staging.get((key.workload, key.query_id))
    if staging_query is not None and not staging_query.active_on(today):
        return f"query staging expired on {staging_query.staging_until.isoformat()}"
    return "query is missing from performance baseline"


def _evaluate_metric(
    *,
    key: QueryKey,
    metric: str,
    current_query: dict[str, Any],
    baseline_query: dict[str, Any],
    policy: GatePolicy,
    apply_static_noise: bool,
) -> GateEntry:
    current_value = metric_value(current_query, metric)
    baseline_value = metric_value(baseline_query, metric)

    if baseline_value is None:
        return GateEntry(
            kind=GateEntryKind.MISSING_BASELINE_METRIC,
            key=key,
            metric=metric,
            status=GateStatus.REGRESS,
            current_value=current_value,
            detail="baseline metric is missing",
        )
    if current_value is None:
        return GateEntry(
            kind=GateEntryKind.MISSING_CURRENT_METRIC,
            key=key,
            metric=metric,
            status=GateStatus.REGRESS,
            baseline_value=baseline_value,
            detail="current metric is missing",
        )
    max_value = _absolute_bound(policy.absolute_max, key, metric)
    min_value = _absolute_bound(policy.absolute_min, key, metric)
    if baseline_value < 0.0:
        return GateEntry(
            kind=GateEntryKind.INVALID_BASELINE,
            key=key,
            metric=metric,
            status=GateStatus.REGRESS,
            baseline_value=baseline_value,
            current_value=current_value,
            detail="baseline metric must be positive",
        )
    if baseline_value == 0.0:
        status = GateStatus.OK if current_value == 0.0 else GateStatus.REGRESS
        detail = None if status == GateStatus.OK else "current metric changed from zero baseline"
        if max_value is not None and current_value > max_value:
            status = GateStatus.REGRESS
            detail = f"current metric exceeds absolute max {max_value:g}"
        if min_value is not None and current_value < min_value:
            status = GateStatus.REGRESS
            detail = f"current metric is below absolute min {min_value:g}"
        return GateEntry(
            kind=GateEntryKind.COMPARED_METRIC,
            key=key,
            metric=metric,
            status=status,
            baseline_value=baseline_value,
            current_value=current_value,
            change_percent=0.0 if current_value == 0.0 else None,
            detail=detail,
        )

    change = ((current_value - baseline_value) / baseline_value) * 100.0
    status = metric_status(
        metric=metric,
        current=current_value,
        baseline=baseline_value,
        change_percent=change,
        policy=policy,
        apply_static_noise=apply_static_noise,
    )

    if apply_static_noise:
        status = apply_static_noise_filters(
            status=status,
            metric=metric,
            current=current_value,
            baseline=baseline_value,
            current_query=current_query,
            baseline_query=baseline_query,
            policy=policy,
        )

    detail = None
    if max_value is not None and current_value > max_value:
        status = GateStatus.REGRESS
        detail = f"current metric exceeds absolute max {max_value:g}"
    if min_value is not None and current_value < min_value:
        status = GateStatus.REGRESS
        detail = f"current metric is below absolute min {min_value:g}"

    return GateEntry(
        kind=GateEntryKind.COMPARED_METRIC,
        key=key,
        metric=metric,
        status=status,
        baseline_value=baseline_value,
        current_value=current_value,
        change_percent=change,
        detail=detail,
    )


def _absolute_bound(bounds: dict[str, float], key: QueryKey, metric: str) -> float | None:
    for bound_key in (
        f"{key.workload}.{key.query_id}.{metric}",
        f"{key.query_id}.{metric}",
        metric,
    ):
        value = bounds.get(bound_key)
        if value is not None:
            return value
    return None


def _apply_statistical_model(
    *,
    entry: GateEntry,
    key: QueryKey,
    metric: str,
    policy: GatePolicy,
    statistical_phase: str,
    source_name: str,
    calibration_index: CalibrationIndex,
    confirmation_indexes: tuple[dict[QueryKey, Any], ...],
    current_query: dict[str, Any],
) -> tuple[GateEntry, _StatisticalCandidate | None]:
    if entry.kind != GateEntryKind.COMPARED_METRIC or entry.status != GateStatus.REGRESS:
        return entry, None
    if entry.baseline_value is None or entry.current_value is None or entry.baseline_value <= 0.0:
        return entry, None

    calibration_values = calibration_index.values(source_name=source_name, key=key, metric=metric)
    required_samples = _required_calibration_samples(policy, source_name)
    if len(calibration_values) < required_samples:
        return replace(
            entry,
            status=GateStatus.UNMEASURABLE,
            calibration_samples=len(calibration_values),
            detail=(
                "calibration lacks enough per-run aggregates for statistical evaluation "
                f"({len(calibration_values)}/{required_samples})"
            ),
        ), None

    current_values = _current_values(
        key=key,
        metric=metric,
        current_query=current_query,
        confirmation_indexes=confirmation_indexes,
    )
    if not current_values:
        return entry, None

    noise_abs, noise_pct = rolling_p95_delta(metric, entry.baseline_value, calibration_values)
    entry = replace(
        entry,
        noise_floor_abs=noise_abs,
        noise_floor_percent=noise_pct,
        calibration_samples=len(calibration_values),
    )
    if not all(_exceeds_policy_threshold(metric, value, entry.baseline_value, policy) for value in current_values):
        return replace(entry, status=GateStatus.OK, detail="quorum sample no longer exceeds policy threshold"), None
    if not all(directional_delta(metric, value, entry.baseline_value) > noise_abs for value in current_values):
        return replace(entry, status=GateStatus.OK, detail="within calibration rolling P95 noise floor"), None
    if statistical_phase == "first":
        return replace(
            entry,
            detail="first-run regression exceeded threshold and rolling P95; quorum retry required",
        ), None

    p_value = mann_whitney_one_sided(
        metric=metric,
        current_values=current_values,
        calibration_values=calibration_values,
    )
    power = estimate_effect_power(
        metric=metric,
        baseline=entry.baseline_value,
        calibration_values=calibration_values,
        current_sample_count=len(current_values),
        alpha=policy.statistics.family_alpha_confirmed,
    )
    return replace(entry, p_value=p_value, statistical_power=power), _StatisticalCandidate(
        entry_index=-1,
        p_value=p_value,
        power=power,
    )


def _current_values(
    *,
    key: QueryKey,
    metric: str,
    current_query: dict[str, Any],
    confirmation_indexes: tuple[dict[QueryKey, Any], ...],
) -> tuple[float, ...]:
    values = []
    current_value = metric_value(current_query, metric)
    if current_value is not None:
        values.append(current_value)
    for index in confirmation_indexes:
        record = index.get(key)
        if record is None:
            continue
        value = metric_value(record.query, metric)
        if value is not None:
            values.append(value)
    return tuple(values)


def _required_calibration_samples(policy: GatePolicy, source_name: str) -> int:
    for source in policy.sources:
        if source.name != source_name:
            continue
        if source.measurement_class == "rust_micro":
            return max(1, policy.calibration.min_clean_observations_rust_micro)
        return max(1, policy.calibration.min_clean_observations_sql_macro)
    return max(1, policy.calibration.min_clean_observations_sql_macro)


def _exceeds_policy_threshold(metric: str, current: float, baseline: float, policy: GatePolicy) -> bool:
    if metric == "throughput_per_second":
        return current / baseline < policy.throughput_min_ratio
    threshold = policy.resource_regression_percent if is_resource_metric(metric) else policy.latency_regression_percent
    return directional_percent(metric, current, baseline) > threshold


def _apply_holm(
    entries: list[GateEntry],
    candidates: list[_StatisticalCandidate],
    *,
    policy: GatePolicy,
    statistical_phase: str,
) -> list[GateEntry]:
    alpha = (
        policy.statistics.family_alpha_confirmed
        if statistical_phase == "confirmed"
        else policy.statistics.family_alpha_first_run
    )
    decisions = holm_step_down({candidate.entry_index: candidate.p_value for candidate in candidates}, alpha)
    if not decisions:
        return entries
    updated = list(entries)
    for candidate in candidates:
        decision = decisions[candidate.entry_index]
        entry = updated[candidate.entry_index]
        detail = f"MW p={decision.raw_p_value:.6g}, Holm alpha={decision.alpha:.6g}"
        if decision.rejected:
            updated[candidate.entry_index] = replace(
                entry,
                status=GateStatus.REGRESS,
                p_value=decision.raw_p_value,
                holm_alpha=decision.alpha,
                detail=detail,
            )
        else:
            updated[candidate.entry_index] = replace(
                entry,
                status=GateStatus.OK,
                p_value=decision.raw_p_value,
                holm_alpha=decision.alpha,
                detail=f"{detail}; statistical test did not confirm regression",
            )
    return updated


def _power_deficit_entries(
    entries: list[GateEntry],
    candidates: list[_StatisticalCandidate],
    *,
    policy: GatePolicy,
) -> list[GateEntry]:
    by_index = {candidate.entry_index: candidate for candidate in candidates}
    power_entries: list[GateEntry] = []
    for index, entry in enumerate(entries):
        candidate = by_index.get(index)
        if candidate is None or entry.statistical_power is None:
            continue
        if entry.statistical_power >= policy.statistics.true_positive_target:
            continue
        status = GateStatus.REGRESS if policy.enforcement.value == "hard" else GateStatus.UNMEASURABLE
        power_entries.append(
            GateEntry(
                kind=GateEntryKind.POWER_DEFICIT,
                key=entry.key,
                metric=entry.metric,
                status=status,
                baseline_value=entry.baseline_value,
                current_value=entry.current_value,
                change_percent=entry.change_percent,
                calibration_samples=entry.calibration_samples,
                statistical_power=entry.statistical_power,
                detail=(
                    f"10% effect power {entry.statistical_power:.3f} below "
                    f"target {policy.statistics.true_positive_target:.3f}"
                ),
            )
        )
    return power_entries
