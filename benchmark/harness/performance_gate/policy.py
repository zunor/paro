# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

"""Performance gate policy loading."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
from pathlib import Path
import tomllib
from typing import Any

from .outcome import GateEnforcement


class PolicyError(ValueError):
    """Raised when a gate policy is malformed."""


@dataclass(frozen=True)
class SourcePolicy:
    name: str
    type: str
    suite: str | None = None
    crate: str | None = None
    bench: str | None = None
    measurement_class: str = "sql_macro"
    required: bool = True


@dataclass(frozen=True)
class StatisticsPolicy:
    model: str
    family_alpha_first_run: float
    family_alpha_confirmed: float
    false_positive_target: float
    true_positive_target: float


@dataclass(frozen=True)
class CalibrationPolicy:
    source: str
    min_clean_observations_sql_macro: int
    min_clean_observations_rust_micro: int
    rolling_window_sql_macro: int
    rolling_window_rust_micro: int
    per_run_sample_count_sql_macro: int
    per_run_sample_count_rust_micro: int
    noise_floor_formula: str


@dataclass(frozen=True)
class CoveragePolicy:
    new_query_staging_days: int


@dataclass(frozen=True)
class GatePolicy:
    schema_version: int
    name: str
    version: int
    sha256: str
    enforcement: GateEnforcement
    metrics: tuple[str, ...]
    sources: tuple[SourcePolicy, ...]
    statistics: StatisticsPolicy
    calibration: CalibrationPolicy
    coverage: CoveragePolicy
    latency_regression_percent: float
    resource_regression_percent: float
    throughput_min_ratio: float
    latency_noise_floor_ms: float
    p99_latency_noise_floor_ms: float
    short_query_latency_noise_floor_ms: float
    short_query_max_p50_ms: float
    tail_sample_min_count: int
    resource_noise_floor_kb: float
    rss_noise_floor_kb: float
    absolute_max: dict[str, float]
    absolute_min: dict[str, float]


def load_policy(path: Path) -> GatePolicy:
    try:
        raw = path.read_bytes()
        payload = tomllib.loads(raw.decode("utf-8"))
    except FileNotFoundError as exc:
        raise PolicyError(f"policy not found: {path}") from exc
    except tomllib.TOMLDecodeError as exc:
        raise PolicyError(f"invalid policy TOML {path}: {exc}") from exc

    metrics = _required_string_list(_table(payload, "metrics"), "required")
    sources = tuple(_load_sources(payload.get("sources")))
    if not sources:
        raise PolicyError("policy must define at least one [[sources]] entry")

    thresholds = _table(payload, "thresholds")
    noise = _table(payload, "noise_floors")
    latency_noise_floor_ms = _required_float(noise, "latency_ms")
    statistics = _load_statistics(_table(payload, "statistics"))
    calibration = _load_calibration(_table(payload, "calibration"))
    coverage = _load_coverage(_table(payload, "coverage"))

    return GatePolicy(
        schema_version=_required_int(payload, "schema_version"),
        name=_required_str(payload, "name"),
        version=_required_int(payload, "version"),
        sha256=hashlib.sha256(raw).hexdigest(),
        enforcement=_enforcement(_required_str(payload, "enforcement")),
        metrics=tuple(metrics),
        sources=sources,
        statistics=statistics,
        calibration=calibration,
        coverage=coverage,
        latency_regression_percent=_required_float(thresholds, "latency_regression_percent"),
        resource_regression_percent=_optional_float(
            thresholds,
            "resource_regression_percent",
            default=25.0,
        ),
        throughput_min_ratio=_optional_float(thresholds, "throughput_min_ratio", default=0.85),
        latency_noise_floor_ms=latency_noise_floor_ms,
        p99_latency_noise_floor_ms=_optional_float(
            noise,
            "p99_latency_ms",
            default=latency_noise_floor_ms,
        ),
        short_query_latency_noise_floor_ms=_optional_float(
            noise,
            "short_query_latency_ms",
            default=latency_noise_floor_ms,
        ),
        short_query_max_p50_ms=_optional_float(noise, "short_query_max_p50_ms", default=0.0),
        tail_sample_min_count=_optional_int(noise, "tail_sample_min_count", default=0),
        resource_noise_floor_kb=_optional_float(noise, "resource_kb", default=0.0),
        rss_noise_floor_kb=_optional_float(noise, "rss_kb", default=0.0),
        absolute_max=_float_map(payload.get("absolute_max")),
        absolute_min=_float_map(payload.get("absolute_min")),
    )


def _load_sources(value: Any) -> list[SourcePolicy]:
    if not isinstance(value, list):
        raise PolicyError("policy must define [[sources]]")
    sources = []
    for index, raw in enumerate(value, start=1):
        if not isinstance(raw, dict):
            raise PolicyError(f"source #{index} must be a table")
        sources.append(
            SourcePolicy(
                name=_required_str(raw, "name"),
                type=_required_str(raw, "type"),
                suite=_optional_str(raw, "suite"),
                crate=_optional_str(raw, "crate"),
                bench=_optional_str(raw, "bench"),
                measurement_class=_optional_str(raw, "measurement_class") or "sql_macro",
                required=_optional_bool(raw, "required", default=True),
            )
        )
    return sources


def _load_statistics(payload: dict[str, Any]) -> StatisticsPolicy:
    return StatisticsPolicy(
        model=_required_str(payload, "model"),
        family_alpha_first_run=_required_float(payload, "family_alpha_first_run"),
        family_alpha_confirmed=_required_float(payload, "family_alpha_confirmed"),
        false_positive_target=_required_float(payload, "false_positive_target"),
        true_positive_target=_required_float(payload, "true_positive_target"),
    )


def _load_calibration(payload: dict[str, Any]) -> CalibrationPolicy:
    return CalibrationPolicy(
        source=_required_str(payload, "source"),
        min_clean_observations_sql_macro=_required_int(payload, "min_clean_observations_sql_macro"),
        min_clean_observations_rust_micro=_required_int(payload, "min_clean_observations_rust_micro"),
        rolling_window_sql_macro=_required_int(payload, "rolling_window_sql_macro"),
        rolling_window_rust_micro=_required_int(payload, "rolling_window_rust_micro"),
        per_run_sample_count_sql_macro=_positive_optional_int(
            payload,
            "per_run_sample_count_sql_macro",
            default=1,
        ),
        per_run_sample_count_rust_micro=_positive_optional_int(
            payload,
            "per_run_sample_count_rust_micro",
            default=50,
        ),
        noise_floor_formula=_required_str(payload, "noise_floor_formula"),
    )


def _load_coverage(payload: dict[str, Any]) -> CoveragePolicy:
    days = _required_int(payload, "new_query_staging_days")
    if days < 0:
        raise PolicyError("policy field new_query_staging_days must be >= 0")
    return CoveragePolicy(new_query_staging_days=days)


def _enforcement(value: str) -> GateEnforcement:
    try:
        return GateEnforcement(value)
    except ValueError as exc:
        raise PolicyError(f"unsupported enforcement: {value}") from exc


def _table(payload: dict[str, Any], key: str) -> dict[str, Any]:
    value = payload.get(key)
    if not isinstance(value, dict):
        raise PolicyError(f"policy [{key}] must be a table")
    return value


def _required_string_list(payload: dict[str, Any], key: str) -> list[str]:
    value = payload.get(key)
    if not isinstance(value, list):
        raise PolicyError(f"policy field {key} must be an array")
    result = [item.strip() for item in value if isinstance(item, str) and item.strip()]
    if not result:
        raise PolicyError(f"policy field {key} must not be empty")
    return result


def _required_str(payload: dict[str, Any], key: str) -> str:
    value = payload.get(key)
    if isinstance(value, str) and value.strip():
        return value.strip()
    raise PolicyError(f"policy field {key} must be a non-empty string")


def _optional_str(payload: dict[str, Any], key: str) -> str | None:
    value = payload.get(key)
    if value is None:
        return None
    if isinstance(value, str):
        return value.strip() or None
    raise PolicyError(f"policy field {key} must be a string")


def _required_int(payload: dict[str, Any], key: str) -> int:
    value = payload.get(key)
    if isinstance(value, bool):
        raise PolicyError(f"policy field {key} must be an integer")
    if isinstance(value, int):
        return value
    raise PolicyError(f"policy field {key} must be an integer")


def _required_float(payload: dict[str, Any], key: str) -> float:
    value = payload.get(key)
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    raise PolicyError(f"policy field {key} must be a number")


def _optional_int(payload: dict[str, Any], key: str, *, default: int) -> int:
    value = payload.get(key, default)
    if isinstance(value, bool):
        raise PolicyError(f"policy field {key} must be an integer")
    if isinstance(value, int):
        return value
    raise PolicyError(f"policy field {key} must be an integer")


def _positive_optional_int(payload: dict[str, Any], key: str, *, default: int) -> int:
    value = _optional_int(payload, key, default=default)
    if value < 1:
        raise PolicyError(f"policy field {key} must be >= 1")
    return value


def _optional_float(payload: dict[str, Any], key: str, *, default: float) -> float:
    value = payload.get(key, default)
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    raise PolicyError(f"policy field {key} must be a number")


def _optional_bool(payload: dict[str, Any], key: str, *, default: bool) -> bool:
    value = payload.get(key, default)
    if isinstance(value, bool):
        return value
    raise PolicyError(f"policy field {key} must be boolean")


def _float_map(value: Any) -> dict[str, float]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise PolicyError("absolute metric bounds must be tables")
    return {
        str(key): float(raw)
        for key, raw in value.items()
        if isinstance(raw, (int, float)) and not isinstance(raw, bool)
    }
