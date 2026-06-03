# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

"""Performance gate baseline schema and validation."""

from __future__ import annotations

import copy
from dataclasses import dataclass
from datetime import datetime, timezone
import subprocess
from pathlib import Path
import statistics
from typing import Any

from ..baseline_index import QueryKey, index_queries, params_key
from .fingerprint import GateFingerprint, UNKNOWN
from .outcome import GateEnforcement
from .policy import GatePolicy, SourcePolicy


BASELINE_SCHEMA_VERSION = 2
MIN_FINGERPRINT_COVERAGE = 0.80
SourcePayloadMap = dict[str, dict[str, Any]]


class BaselineError(ValueError):
    """Raised when a performance baseline cannot be used."""


@dataclass(frozen=True)
class GateBaseline:
    path: Path
    schema_version: int
    gate: str
    platform: str
    policy_name: str
    policy_version: int
    policy_sha256: str
    system_fingerprint: dict[str, Any]
    build_fingerprint: dict[str, Any]
    runtime_fingerprint: dict[str, Any]
    payload_by_source: SourcePayloadMap


@dataclass(frozen=True)
class BaselineMeasurement:
    source: SourcePolicy
    payload: dict[str, Any]


def load_baseline(path: Path) -> GateBaseline:
    import json

    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise BaselineError(f"baseline not found: {path}") from exc
    except json.JSONDecodeError as exc:
        raise BaselineError(f"invalid baseline JSON {path}: {exc}") from exc
    if not isinstance(raw, dict):
        raise BaselineError(f"baseline JSON must be an object: {path}")

    schema_version = _int(raw, "schema_version")
    if schema_version != BASELINE_SCHEMA_VERSION:
        raise BaselineError(
            f"baseline {path} has schema_version={schema_version}; expected {BASELINE_SCHEMA_VERSION}"
        )
    policy = _table(raw, "policy")
    measurements = _table(raw, "measurements")
    sources = measurements.get("sources")
    if not isinstance(sources, list) or not sources:
        raise BaselineError("baseline measurements.sources must be a non-empty array")

    payload_by_source: SourcePayloadMap = {}
    for index, source in enumerate(sources, start=1):
        if not isinstance(source, dict):
            raise BaselineError(f"baseline source #{index} must be an object")
        name = _str(source, "name")
        payload = source.get("payload")
        if not isinstance(payload, dict):
            raise BaselineError(f"baseline source '{name}' payload must be an object")
        payload_by_source[name] = payload

    return GateBaseline(
        path=path,
        schema_version=schema_version,
        gate=_str(raw, "gate"),
        platform=_str(raw, "platform"),
        policy_name=_str(policy, "name"),
        policy_version=_int(policy, "version"),
        policy_sha256=_str(policy, "sha256"),
        system_fingerprint=_table(raw, "system_fingerprint"),
        build_fingerprint=_table(raw, "build_fingerprint"),
        runtime_fingerprint=_table(raw, "runtime_fingerprint"),
        payload_by_source=payload_by_source,
    )


def baseline_payload_for_source(baseline: GateBaseline, source_name: str) -> dict[str, Any]:
    try:
        return baseline.payload_by_source[source_name]
    except KeyError as exc:
        raise BaselineError(f"baseline has no measurement source '{source_name}'") from exc


def validate_baseline_for_check(
    *,
    baseline: GateBaseline,
    policy: GatePolicy,
    platform_key: str,
    fingerprint: GateFingerprint,
    execution_mode: str,
) -> None:
    if baseline.gate != policy.name:
        raise BaselineError(f"baseline gate '{baseline.gate}' does not match policy '{policy.name}'")
    if baseline.platform != platform_key:
        raise BaselineError(f"baseline platform '{baseline.platform}' does not match current '{platform_key}'")
    if baseline.policy_name != policy.name:
        raise BaselineError(f"baseline policy '{baseline.policy_name}' does not match '{policy.name}'")
    if baseline.policy_version != policy.version or baseline.policy_sha256 != policy.sha256:
        raise BaselineError(
            "baseline policy reference does not match current policy; refresh baseline through policy evolution"
        )

    mismatches = _fingerprint_mismatches(
        baseline=baseline,
        current=fingerprint,
        include_system=execution_mode == "strict",
        include_runtime=_requires_runtime_fingerprint(policy),
    )
    if mismatches and policy.enforcement == GateEnforcement.HARD:
        joined = ", ".join(mismatches[:5])
        suffix = "" if len(mismatches) <= 5 else f", +{len(mismatches) - 5} more"
        raise BaselineError(f"fingerprint mismatch refuses hard compare: {joined}{suffix}")


def validate_existing_baseline_for_bless(
    *,
    baseline: GateBaseline,
    policy: GatePolicy,
    platform_key: str,
    policy_evolution: bool,
) -> None:
    if baseline.platform != platform_key:
        raise BaselineError(f"cannot bless platform '{platform_key}' over baseline '{baseline.platform}'")
    if baseline.gate != policy.name:
        raise BaselineError(f"cannot bless gate '{policy.name}' over baseline gate '{baseline.gate}'")
    if baseline.policy_sha256 == policy.sha256 and baseline.policy_version == policy.version:
        return
    if not policy_evolution:
        raise BaselineError("policy changed; use gate bless --policy-evolution to refresh this baseline")
    if policy.version <= baseline.policy_version:
        raise BaselineError("policy evolution requires policy.version to increase")


def build_baseline_payload(
    *,
    gate: str,
    platform_key: str,
    policy: GatePolicy,
    fingerprint: GateFingerprint,
    measurements: list[Any],
) -> dict[str, Any]:
    return {
        "schema_version": BASELINE_SCHEMA_VERSION,
        "gate": gate,
        "platform": platform_key,
        "policy": {
            "name": policy.name,
            "version": policy.version,
            "sha256": policy.sha256,
        },
        "source": {
            "git_commit": _git("rev-parse", "HEAD"),
            "created_at": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
            "runner": _runner_name(),
        },
        "system_fingerprint": fingerprint.system,
        "build_fingerprint": fingerprint.build,
        "runtime_fingerprint": fingerprint.runtime,
        "audit": fingerprint.audit,
        "measurements": {
            "sources": [
                {
                    "name": measurement.source.name,
                    "type": measurement.source.type,
                    "measurement_class": measurement.source.measurement_class,
                    "payload": project_measurement_payload(measurement.payload),
                }
                for measurement in measurements
            ],
        },
    }


def aggregate_measurement_runs(measurement_runs: list[list[Any]]) -> list[BaselineMeasurement]:
    if not measurement_runs:
        raise BaselineError("cannot build a baseline from zero measurement runs")
    first_sources = [measurement.source.name for measurement in measurement_runs[0]]
    aggregated: list[BaselineMeasurement] = []
    for source_index, source_name in enumerate(first_sources):
        source = measurement_runs[0][source_index].source
        payloads = []
        for run_index, measurements in enumerate(measurement_runs, start=1):
            names = [measurement.source.name for measurement in measurements]
            if names != first_sources:
                raise BaselineError(
                    f"bless run {run_index} source set {names} does not match first run {first_sources}"
                )
            payloads.append(project_measurement_payload(measurements[source_index].payload))
        aggregated.append(
            BaselineMeasurement(
                source=source,
                payload=_aggregate_projected_payloads(payloads, source_name=source_name),
            )
        )
    return aggregated


def project_measurement_payload(payload: dict[str, Any]) -> dict[str, Any]:
    projected: dict[str, Any] = {"version": payload.get("version", BASELINE_SCHEMA_VERSION), "workloads": []}
    for workload in payload.get("workloads", []):
        if not isinstance(workload, dict):
            continue
        projected_workload = {
            "name": workload.get("name", ""),
            "params": copy.deepcopy(workload.get("params", {})),
            "queries": [],
        }
        queries = workload.get("queries", [])
        if not isinstance(queries, list):
            continue
        for query in queries:
            if not isinstance(query, dict):
                continue
            projected_query: dict[str, Any] = {"id": query.get("id", "")}
            for field in ("stats", "rss", "memory", "spill_metrics", "audit"):
                value = query.get(field)
                if isinstance(value, dict) and value:
                    projected_query[field] = copy.deepcopy(value)
            samples_count = _samples_count(query)
            if samples_count is not None:
                projected_query["samples_count"] = samples_count
            if query.get("error"):
                projected_query["error"] = query["error"]
            projected_workload["queries"].append(projected_query)
        projected["workloads"].append(projected_workload)
    return projected


def _aggregate_projected_payloads(payloads: list[dict[str, Any]], *, source_name: str) -> dict[str, Any]:
    base = copy.deepcopy(payloads[0])
    indexes = [index_queries(payload) for payload in payloads]
    for workload in base.get("workloads", []):
        workload_name = str(workload.get("name", ""))
        workload_params_key = params_key(workload.get("params", {}))
        for query in workload.get("queries", []):
            key = QueryKey(
                workload=workload_name,
                query_id=str(query.get("id", "")),
                params_key=workload_params_key,
            )
            if key not in indexes[0]:
                raise BaselineError(f"baseline projection lost query identity in source '{source_name}'")
            run_queries = []
            for run_index, index in enumerate(indexes, start=1):
                record = index.get(key)
                if record is None:
                    raise BaselineError(
                        f"bless run {run_index} is missing {key.label} for source '{source_name}'"
                    )
                run_queries.append(record.query)
            for field in ("stats", "rss", "memory", "spill_metrics", "audit"):
                merged = _median_numeric_map([run_query.get(field) for run_query in run_queries])
                if merged:
                    query[field] = merged
                else:
                    query.pop(field, None)
            counts = [
                run_query.get("samples_count")
                for run_query in run_queries
                if isinstance(run_query.get("samples_count"), int)
            ]
            if counts:
                query["samples_count"] = int(statistics.median(counts))
            elif "samples_count" in query:
                query.pop("samples_count", None)
    return base


def _median_numeric_map(values: list[Any]) -> dict[str, float]:
    keys: set[str] = set()
    for value in values:
        if isinstance(value, dict):
            keys.update(str(key) for key in value)
    merged: dict[str, float] = {}
    for key in sorted(keys):
        samples = [
            float(value[key])
            for value in values
            if isinstance(value, dict)
            and isinstance(value.get(key), (int, float))
            and not isinstance(value.get(key), bool)
        ]
        if samples:
            merged[key] = float(statistics.median(samples))
    return merged


def _samples_count(query: dict[str, Any]) -> int | None:
    value = query.get("samples_count")
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    samples = query.get("samples_ms")
    if isinstance(samples, list):
        return len(samples)
    return None


def _fingerprint_mismatches(
    *,
    baseline: GateBaseline,
    current: GateFingerprint,
    include_system: bool,
    include_runtime: bool,
) -> list[str]:
    mismatches: list[str] = []
    if include_system:
        mismatches.extend(_dict_mismatches("system", baseline.system_fingerprint, current.system))
    mismatches.extend(_dict_mismatches("build", baseline.build_fingerprint, current.build))
    if include_runtime:
        mismatches.extend(_dict_mismatches("runtime", baseline.runtime_fingerprint, current.runtime))
    return mismatches


def _requires_runtime_fingerprint(policy: GatePolicy) -> bool:
    return any(source.measurement_class == "sql_macro" for source in policy.sources)


def _dict_mismatches(prefix: str, expected: dict[str, Any], current: dict[str, Any]) -> list[str]:
    mismatches = []
    observed = 0
    total = 0
    for key in sorted(set(expected) | set(current)):
        left = expected.get(key, UNKNOWN)
        right = current.get(key, UNKNOWN)
        total += 1
        if is_missing_fingerprint_value(left) or is_missing_fingerprint_value(right):
            mismatches.append(f"{prefix}.{key}:missing")
            continue
        observed += 1
        if left != right:
            mismatches.append(f"{prefix}.{key}")
    if total > 0 and observed / total < MIN_FINGERPRINT_COVERAGE:
        mismatches.insert(0, f"{prefix}.coverage<{int(MIN_FINGERPRINT_COVERAGE * 100)}% ({observed}/{total} observed)")
    return mismatches


def is_missing_fingerprint_value(value: Any) -> bool:
    return value is None or value == "" or value == UNKNOWN


def _table(payload: dict[str, Any], key: str) -> dict[str, Any]:
    value = payload.get(key)
    if not isinstance(value, dict):
        raise BaselineError(f"baseline field {key} must be an object")
    return value


def _str(payload: dict[str, Any], key: str) -> str:
    value = payload.get(key)
    if isinstance(value, str) and value:
        return value
    raise BaselineError(f"baseline field {key} must be a non-empty string")


def _int(payload: dict[str, Any], key: str) -> int:
    value = payload.get(key)
    if isinstance(value, bool):
        raise BaselineError(f"baseline field {key} must be an integer")
    if isinstance(value, int):
        return value
    raise BaselineError(f"baseline field {key} must be an integer")


def _git(*args: str) -> str:
    try:
        return subprocess.check_output(
            ("git",) + args,
            stderr=subprocess.DEVNULL,
            text=True,
            timeout=2,
        ).strip()
    except (OSError, subprocess.SubprocessError):
        return UNKNOWN


def _runner_name() -> str:
    import os

    return os.getenv("GITHUB_RUNNER_NAME") or os.getenv("RUNNER_NAME") or "local"
