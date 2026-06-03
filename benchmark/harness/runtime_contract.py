# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

"""Runtime contract fields shared by benchmark reports and gate fingerprints."""

from __future__ import annotations

import os
from typing import Any


UNKNOWN = "unknown"
_CONTRACT_ENV_KEYS: dict[str, tuple[str, ...]] = {
    "thread_count": ("PARO_BENCH_THREAD_COUNT", "PARO_NUM_THREADS"),
    "query_memory_cap": ("PARO_BENCH_QUERY_MEMORY_CAP", "PARO_QUERY_MEMORY_CAP"),
    "temp_directory": ("PARO_BENCH_TEMP_DIRECTORY", "PARO_TEMP_DIRECTORY"),
    "data_scale": ("PARO_BENCH_DATA_SCALE",),
    "random_seed": ("PARO_BENCH_RANDOM_SEED",),
    "single_task_fast_path": ("PARO_BENCH_SINGLE_TASK_FAST_PATH",),
    "allocator_audit": ("PARO_ALLOC_AUDIT", "PARO_BENCH_ALLOC_AUDIT"),
}
_BENCH_ENV_PREFIXES = ("PARO_PERF_", "PARO_BENCH_")


def runtime_contract_payload(*, include_environment: bool = False) -> dict[str, Any]:
    payload = {
        field: _first_env(*keys)
        for field, keys in _CONTRACT_ENV_KEYS.items()
    }
    if include_environment:
        extra_env = benchmark_environment()
        if extra_env:
            payload["performance_env"] = extra_env
    return payload


def benchmark_environment() -> dict[str, str]:
    represented_keys = {key for keys in _CONTRACT_ENV_KEYS.values() for key in keys}
    return {
        key: value
        for key, value in sorted(os.environ.items())
        if key.startswith(_BENCH_ENV_PREFIXES) and key not in represented_keys
    }


def _first_env(*keys: str) -> str:
    for key in keys:
        value = os.getenv(key)
        if value is not None and value.strip():
            return value.strip()
    return UNKNOWN
