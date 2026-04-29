# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

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
_ROUTINE_ID_RE = re.compile(r"(\bRoutine(?:s)?:\s+[^\[]+)\[(\d+)@(\d+)\]")
_EXTERNAL_LATENCY_RE = re.compile(
    r"Latency\(us\):\s*acquire=\d+\s+queue=\d+\s+kernel=\d+\s+encode_decode=\d+"
)
_ROWS_LOOPS_RE = re.compile(r"rows=\d+\s+loops=\d+")
_JSON_OPERATOR_TIMING_RE = re.compile(r'"(startup_time_ms|total_time_ms)"\s*:\s*[\d.]+')
_JSON_OPERATOR_COUNTERS_RE = re.compile(r'"(rows|loops)"\s*:\s*\d+')
_JSON_PLANNING_TIME_RE = re.compile(r'"Planning Time"\s*:\s*"[\d.]+\s*ms"')
_JSON_EXECUTION_TIME_RE = re.compile(r'"Execution Time"\s*:\s*"[\d.]+\s*ms"')
_JSON_BYTES_FIELD_RE = re.compile(
    r'"(Memory|Disk|Peak Memory|Temp Storage|Total Temp Storage|peak_memory_bytes|temp_storage_bytes)"\s*:\s*\d+'
)
_COPY_ROWCOUNT_RE = re.compile(r"^COPY\s+\d+$")
_TXN_NUMERIC_ID_RE = re.compile(
    r"\b(TxnId|ReadTs|CommitTs|TableId|DatabaseId)\(\d+\)"
)
_TRANSACTION_ID_TEXT_RE = re.compile(r"\btransaction\s+\d+\b", re.IGNORECASE)
_TXN_STRUCT_FIELD_RE = re.compile(r"\b(ssi_state_epoch|tenant_id|tablet_id):\s*\d+\b")
_REGRESS_PATH_RE = re.compile(
    r"(?<!<repo>)/(?:[^'\"\s)]+/)*regress/(?:report/fixtures|fixtures)/[^'\"\s)]+"
)


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


def normalize_explain_routine_ids(lines: list[str]) -> list[str]:
    """Normalize volatile catalog ids embedded in EXPLAIN routine labels."""
    result: list[str] = []
    for line in lines:
        line = _ROUTINE_ID_RE.sub(r"\1[<routine-id>@\3]", line)
        result.append(line)
    return result


def normalize_explain_external_runtime(lines: list[str]) -> list[str]:
    """Normalize volatile external runtime latency fields in EXPLAIN output."""
    result: list[str] = []
    for line in lines:
        line = _EXTERNAL_LATENCY_RE.sub(
            "Latency(us): acquire=<us> queue=<us> kernel=<us> encode_decode=<us>",
            line,
        )
        result.append(line)
    return result


def normalize_explain_operator_counters(lines: list[str]) -> list[str]:
    """Normalize volatile EXPLAIN ANALYZE row/loop counters."""
    result: list[str] = []
    for line in lines:
        line = _ROWS_LOOPS_RE.sub("rows=<rows> loops=<loops>", line)
        line = _JSON_OPERATOR_COUNTERS_RE.sub(lambda m: f'"{m.group(1)}": 0', line)
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


def normalize_transaction_ids(lines: list[str]) -> list[str]:
    """Normalize volatile transaction/catalog ids in concurrency error text."""

    def _replace_typed_id(match: re.Match[str]) -> str:
        return f"{match.group(1)}(<id>)"

    result: list[str] = []
    for line in lines:
        line = _TXN_NUMERIC_ID_RE.sub(_replace_typed_id, line)
        line = _TRANSACTION_ID_TEXT_RE.sub("transaction <id>", line)
        line = _TXN_STRUCT_FIELD_RE.sub(lambda m: f"{m.group(1)}: <id>", line)
        result.append(line)
    return result


def normalize_regress_paths(lines: list[str]) -> list[str]:
    """Normalize workspace-specific regress fixture paths to a stable repo placeholder."""

    def _replace(match: re.Match[str]) -> str:
        path = match.group(0)
        suffix_idx = path.find("/regress/")
        if suffix_idx == -1:
            return path
        return f"<repo>{path[suffix_idx:]}"

    result: list[str] = []
    for line in lines:
        result.append(_REGRESS_PATH_RE.sub(_replace, line))
    return result


# stable: normalize per-operator timing volatility.
# stable: normalize summary timing volatility.
# stable: normalize runtime byte volatility for spill/memory observability.
# stable: normalize repo-local regress fixture/report absolute paths.
# stable: normalize volatile transaction/catalog ids in concurrency errors.
# transitional: legacy alias kept for gradual migration from explain_runtime.
NORMALIZERS: dict[str, Callable[[list[str]], list[str]]] = {
    "explain_operator_timing": normalize_explain_operator_timing,
    "explain_operator_counters": normalize_explain_operator_counters,
    "explain_summary_timing": normalize_explain_summary_timing,
    "explain_runtime_bytes": normalize_explain_runtime_bytes,
    "explain_routine_ids": normalize_explain_routine_ids,
    "explain_external_runtime": normalize_explain_external_runtime,
    "explain_runtime": normalize_explain_runtime,
    "copy_rowcount": normalize_copy_rowcount,
    "transaction_ids": normalize_transaction_ids,
    "regress_paths": normalize_regress_paths,
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
    "normalize_explain_operator_counters",
    "normalize_explain_operator_timing",
    "normalize_copy_rowcount",
    "normalize_explain_external_runtime",
    "normalize_explain_routine_ids",
    "normalize_explain_runtime",
    "normalize_explain_runtime_bytes",
    "normalize_explain_summary_timing",
    "normalize_regress_paths",
    "normalize_transaction_ids",
    "normalizer_profiles",
]
