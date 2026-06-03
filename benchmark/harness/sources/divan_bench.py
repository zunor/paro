# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

"""Structured Divan benchmark measurement source."""

from __future__ import annotations

from datetime import datetime
import json
import math
import os
from pathlib import Path
import statistics
import subprocess
from typing import Any

from ..baseline_index import QueryKey
from ..performance_gate.policy import SourcePolicy
from ..reporter import BenchmarkReporter
from .context import SourceContext, SourceMeasurement


DEFAULT_SAMPLE_COUNT = 50
DEFAULT_TIMEOUT_SECONDS = 600


class DivanBenchSource:
    source_type = "divan_bench"

    def execute(self, source: SourcePolicy, context: SourceContext) -> SourceMeasurement:
        if not source.crate:
            raise ValueError(f"source '{source.name}' is missing crate")
        if not source.bench:
            raise ValueError(f"source '{source.name}' is missing bench")

        report_dir = context.root_dir / "report" / _safe_path_name(source.name)
        raw_path = report_dir / "divan-raw.json"
        env = _divan_env(raw_path, source, context)
        command = ["cargo", "bench", "--locked", "-p", source.crate, "--bench", source.bench]

        try:
            completed = subprocess.run(
                command,
                cwd=context.root_dir.parent,
                env=env,
                text=True,
                capture_output=True,
                check=False,
                timeout=_timeout_seconds(),
            )
        except subprocess.TimeoutExpired as exc:
            raise ValueError(
                "structured Divan bench timed out "
                f"({source.crate}/{source.bench}, timeout={_timeout_seconds()}s): "
                f"{_tail(_timeout_output(exc))}"
            ) from exc
        if completed.returncode != 0:
            raise ValueError(
                "structured Divan bench failed "
                f"({source.crate}/{source.bench}): {_tail(completed.stderr or completed.stdout)}"
            )
        if not raw_path.exists():
            raise ValueError(f"structured Divan bench did not write result: {raw_path}")

        try:
            raw_payload = json.loads(raw_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise ValueError(f"invalid structured Divan JSON {raw_path}: {exc}") from exc

        payload = normalize_divan_payload(
            source,
            raw_payload,
            retry_query_keys=context.retry_query_keys,
            minimum_sample_count=context.minimum_sample_count,
        )
        reporter = BenchmarkReporter(context.root_dir)
        result_path, summary_path = reporter.write_reports(payload, report_dir / "result.json")
        return SourceMeasurement(
            source=source,
            payload=payload,
            result_path=result_path,
            summary_path=summary_path,
            failed=False,
        )


def normalize_divan_payload(
    source: SourcePolicy,
    raw_payload: dict[str, Any],
    *,
    retry_query_keys: frozenset[QueryKey] = frozenset(),
    minimum_sample_count: int = 1,
) -> dict[str, Any]:
    if raw_payload.get("kind") != "divan_bench_result":
        raise ValueError("structured Divan JSON kind must be divan_bench_result")
    if source.crate and raw_payload.get("crate") != source.crate:
        raise ValueError(f"structured Divan JSON crate does not match source '{source.name}'")
    if source.bench and raw_payload.get("bench") != source.bench:
        raise ValueError(f"structured Divan JSON bench does not match source '{source.name}'")
    benches = raw_payload.get("benches")
    if not isinstance(benches, list):
        raise ValueError("structured Divan JSON must contain benches array")

    allowed = _allowed_retry_ids(source, retry_query_keys)
    queries = []
    for bench in benches:
        if not isinstance(bench, dict):
            raise ValueError("structured Divan bench entry must be an object")
        bench_id = _required_str(bench, "id")
        if allowed is not None and bench_id not in allowed:
            continue
        samples = _samples_ms(bench)
        if len(samples) < minimum_sample_count:
            raise ValueError(
                f"structured Divan bench '{bench_id}' has {len(samples)} samples; "
                f"expected at least {minimum_sample_count}"
            )
        query = {
            "id": bench_id,
            "samples_ms": samples,
            "samples_count": len(samples),
            "stats": _compute_stats(samples),
            "divan": {
                "items": _optional_positive_int(bench.get("items")),
            },
        }
        audit = _optional_audit(bench.get("audit"))
        if audit:
            query["audit"] = audit
        queries.append(query)

    if not queries:
        raise ValueError(f"structured Divan result for source '{source.name}' contains no selected benches")

    return {
        "version": 2,
        "timestamp": datetime.now().astimezone().isoformat(timespec="seconds"),
        "source": {
            "name": source.name,
            "type": source.type,
            "crate": source.crate,
            "bench": source.bench,
            "measurement_class": source.measurement_class,
        },
        "workloads": [
            {
                "name": source.bench or source.name,
                "params": {},
                "queries": queries,
            }
        ],
    }


def _divan_env(raw_path: Path, source: SourcePolicy, context: SourceContext) -> dict[str, str]:
    env = dict(os.environ)
    env["PARO_DIVAN_JSON_OUT"] = str(raw_path)
    env["PARO_DIVAN_SAMPLE_COUNT"] = str(_sample_count(minimum=context.minimum_sample_count))
    selected_ids = _allowed_retry_ids(source, context.retry_query_keys)
    if selected_ids:
        env["PARO_DIVAN_BENCH_FILTER"] = ",".join(sorted(selected_ids))
    return env


def _sample_count(*, minimum: int) -> int:
    raw = os.getenv("PARO_DIVAN_SAMPLE_COUNT")
    try:
        value = int(raw) if raw is not None else DEFAULT_SAMPLE_COUNT
    except ValueError:
        value = DEFAULT_SAMPLE_COUNT
    value = value if value > 0 else DEFAULT_SAMPLE_COUNT
    if value < minimum:
        raise ValueError(f"PARO_DIVAN_SAMPLE_COUNT={value} is below required minimum {minimum}")
    return value


def _timeout_seconds() -> int:
    raw = os.getenv("PARO_DIVAN_BENCH_TIMEOUT_S")
    try:
        value = int(raw) if raw is not None else DEFAULT_TIMEOUT_SECONDS
    except ValueError:
        value = DEFAULT_TIMEOUT_SECONDS
    return value if value > 0 else DEFAULT_TIMEOUT_SECONDS


def _allowed_retry_ids(
    source: SourcePolicy,
    retry_query_keys: frozenset[QueryKey],
) -> frozenset[str] | None:
    if not retry_query_keys:
        return None
    workload = source.bench or source.name
    return frozenset(key.query_id for key in retry_query_keys if key.workload == workload)


def _samples_ms(bench: dict[str, Any]) -> list[float]:
    raw = bench.get("samples_ms")
    if not isinstance(raw, list):
        bench_id = bench.get("id", "<unknown>")
        raise ValueError(f"structured Divan bench '{bench_id}' is missing samples_ms")
    samples = [
        float(value)
        for value in raw
        if isinstance(value, (int, float)) and not isinstance(value, bool)
    ]
    if not samples:
        bench_id = bench.get("id", "<unknown>")
        raise ValueError(f"structured Divan bench '{bench_id}' has no numeric samples")
    return samples


def _compute_stats(samples: list[float]) -> dict[str, float]:
    sorted_samples = sorted(samples)
    mean = float(statistics.mean(sorted_samples))
    median = float(statistics.median(sorted_samples))
    stddev = float(statistics.stdev(sorted_samples)) if len(sorted_samples) > 1 else 0.0
    return {
        "min": float(sorted_samples[0]),
        "median": median,
        "p50": median,
        "mean": mean,
        "p90": _percentile(sorted_samples, 0.9),
        "p99": _percentile(sorted_samples, 0.99),
        "p999": _percentile(sorted_samples, 0.999),
        "max": float(sorted_samples[-1]),
        "stddev": stddev,
        "throughput_per_second": 1000.0 / mean if mean > 0 else 0.0,
    }


def _percentile(sorted_samples: list[float], quantile: float) -> float:
    index = max(0, math.ceil(quantile * len(sorted_samples)) - 1)
    return float(sorted_samples[min(index, len(sorted_samples) - 1)])


def _required_str(payload: dict[str, Any], key: str) -> str:
    value = payload.get(key)
    if isinstance(value, str) and value.strip():
        return value.strip()
    raise ValueError(f"structured Divan bench entry field '{key}' must be a non-empty string")


def _optional_positive_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int) and value > 0:
        return value
    return None


def _optional_audit(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    audit: dict[str, Any] = {}
    chunk_count = _optional_positive_int(value.get("chunk_count"))
    if chunk_count is not None:
        audit["chunk_count"] = chunk_count
    for key in ("allocator_tracking_event_counts", "allocator_tracking_events_per_chunk"):
        raw = value.get(key)
        if not isinstance(raw, list):
            continue
        numbers = [
            float(item)
            for item in raw
            if isinstance(item, (int, float)) and not isinstance(item, bool)
        ]
        if not numbers:
            continue
        audit[key] = numbers
        audit[f"{key}_median"] = float(statistics.median(numbers))
        audit[f"{key}_max"] = float(max(numbers))
    return audit or None


def _safe_path_name(value: str) -> str:
    safe = "".join(
        char if char.isalnum() or char in "_.-" else "-"
        for char in value.strip()
    ).strip(".-")
    return safe or "divan-bench"


def _timeout_output(exc: subprocess.TimeoutExpired) -> str:
    parts = []
    for value in (exc.stderr, exc.stdout):
        if value is None:
            continue
        if isinstance(value, bytes):
            parts.append(value.decode("utf-8", errors="replace"))
        else:
            parts.append(str(value))
    return "\n".join(parts)


def _tail(value: str, *, max_chars: int = 8000) -> str:
    value = value.strip()
    if len(value) <= max_chars:
        return value
    return value[-max_chars:]
