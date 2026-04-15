"""Text-level normalizers for non-deterministic output."""

from __future__ import annotations

import re
from typing import Callable

_ACTUAL_TIME_RE = re.compile(r"actual time=[\d.]+\.\.[\d.]+")
_PLANNING_TIME_RE = re.compile(r"Planning Time:\s*[\d.]+\s*ms")
_EXECUTION_TIME_RE = re.compile(r"Execution Time:\s*[\d.]+\s*ms")
_BYTES_FIELD_RE = re.compile(
    r"\b(Memory|Disk|Peak Memory|Temp Storage|Total Temp Storage):\s*[\d.]+\s*(?:B|kB|MB|GB|TB)\b"
)
_JSON_OPERATOR_TIMING_RE = re.compile(r'"(startup_time_ms|total_time_ms)"\s*:\s*[\d.]+')
_JSON_PLANNING_TIME_RE = re.compile(r'"Planning Time"\s*:\s*"[\d.]+\s*ms"')
_JSON_EXECUTION_TIME_RE = re.compile(r'"Execution Time"\s*:\s*"[\d.]+\s*ms"')
_JSON_BYTES_FIELD_RE = re.compile(
    r'"(Memory|Disk|Peak Memory|Temp Storage|Total Temp Storage|peak_memory_bytes|temp_storage_bytes)"\s*:\s*\d+'
)
_COPY_ROWCOUNT_RE = re.compile(r"^COPY\s+\d+$")


def normalize_explain_operator_timing(lines: list[str]) -> list[str]:
    """Normalize operator-level EXPLAIN ANALYZE timing fields."""
    result: list[str] = []
    for line in lines:
        line = _ACTUAL_TIME_RE.sub("actual time=<time-range>", line)
        line = _JSON_OPERATOR_TIMING_RE.sub(lambda m: f'"{m.group(1)}": 0.0', line)
        result.append(line)
    return result


def normalize_explain_summary_timing(lines: list[str]) -> list[str]:
    """Normalize summary-level EXPLAIN timing fields."""
    result: list[str] = []
    for line in lines:
        line = _PLANNING_TIME_RE.sub("Planning Time: <time-ms>", line)
        line = _EXECUTION_TIME_RE.sub("Execution Time: <time-ms>", line)
        line = _JSON_PLANNING_TIME_RE.sub('"Planning Time": "<time-ms>"', line)
        line = _JSON_EXECUTION_TIME_RE.sub('"Execution Time": "<time-ms>"', line)
        result.append(line)
    return result


def normalize_explain_runtime_bytes(lines: list[str]) -> list[str]:
    """Normalize volatile runtime byte fields in EXPLAIN output."""
    result: list[str] = []
    for line in lines:
        line = _BYTES_FIELD_RE.sub(r"\1: <bytes>", line)
        line = _JSON_BYTES_FIELD_RE.sub(lambda m: f'"{m.group(1)}": 0', line)
        result.append(line)
    return result


def normalize_explain_runtime(lines: list[str]) -> list[str]:
    """Legacy alias combining operator + summary timing normalization."""
    return normalize_explain_summary_timing(normalize_explain_operator_timing(lines))


def normalize_copy_rowcount(lines: list[str]) -> list[str]:
    """Normalize statement status lines with non-deterministic COPY row counts."""
    result: list[str] = []
    for line in lines:
        if _COPY_ROWCOUNT_RE.match(line):
            result.append("COPY <rows>")
        else:
            result.append(line)
    return result


# stable: normalize per-operator timing volatility.
# stable: normalize summary timing volatility.
# stable: normalize runtime byte volatility for spill/memory observability.
# transitional: legacy alias kept for gradual migration from explain_runtime.
NORMALIZERS: dict[str, Callable[[list[str]], list[str]]] = {
    "explain_operator_timing": normalize_explain_operator_timing,
    "explain_summary_timing": normalize_explain_summary_timing,
    "explain_runtime_bytes": normalize_explain_runtime_bytes,
    "explain_runtime": normalize_explain_runtime,
    "copy_rowcount": normalize_copy_rowcount,
}


def normalizer_profiles() -> tuple[str, ...]:
    """Return the registered normalizer profile names in declaration order."""
    return tuple(NORMALIZERS)


def is_known_normalizer(profile: str) -> bool:
    """Return whether a normalizer profile is registered."""
    return profile in NORMALIZERS


def apply_normalizers(lines: list[str], profiles: tuple[str, ...]) -> list[str]:
    """Apply normalizer profiles in order."""
    normalized = list(lines)
    for profile in profiles:
        fn = NORMALIZERS.get(profile)
        if fn is None:
            raise ValueError(f"unknown normalizer profile: {profile!r}")
        normalized = fn(normalized)
    return normalized


__all__ = [
    "NORMALIZERS",
    "apply_normalizers",
    "is_known_normalizer",
    "normalize_explain_operator_timing",
    "normalize_copy_rowcount",
    "normalize_explain_runtime",
    "normalize_explain_runtime_bytes",
    "normalize_explain_summary_timing",
    "normalizer_profiles",
]
