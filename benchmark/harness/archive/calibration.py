# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

"""Calibration archive reads, writes, and recovery helpers."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
import os
from pathlib import Path
import re
import subprocess
from typing import Any

from ..performance_gate.baseline import project_measurement_payload
from ..performance_gate.fingerprint import GateFingerprint, UNKNOWN, platform_key
from ..performance_gate.outcome import GateEnforcement
from ..performance_gate.policy import GatePolicy, load_policy
from .manifest import ArchiveManifest, load_manifest, manifest_relative_path
from .store import (
    ArchiveCorrupt,
    ArchiveError,
    ArchiveMissing,
    ArchiveStore,
    ArchiveUnavailable,
    ArchiveWrite,
    canonical_json_bytes,
    sha256_bytes,
)


ARCHIVE_PAYLOAD_SCHEMA_VERSION = 1


class ArchivePayloadKind(str, Enum):
    GATE_RESULT = "gate_result"
    CALIBRATION_OBSERVATION = "calibration_observation"
    CALIBRATION_RECOMPUTE = "calibration_recompute"


class ArchiveHealthStatus(str, Enum):
    OK = "OK"
    DISABLED = "ArchiveDisabled"
    UNAVAILABLE = "ArchiveUnavailable"
    CALIBRATION_INSUFFICIENT = "CalibrationInsufficient"
    CALIBRATION_CORRUPT = "CalibrationCorrupt"


@dataclass(frozen=True)
class ArchiveHealth:
    status: ArchiveHealthStatus
    message: str
    policy_enforcement: GateEnforcement
    effective_enforcement: GateEnforcement
    manifest_path: str | None = None
    calibration_path: str | None = None
    cache_hit: bool = False
    clean_observations: int = 0
    required_observations: int = 0
    calibration_payload: dict[str, Any] | None = None

    @property
    def degraded(self) -> bool:
        return self.effective_enforcement != self.policy_enforcement

    @property
    def ok(self) -> bool:
        return self.status in {ArchiveHealthStatus.OK, ArchiveHealthStatus.DISABLED}


def load_calibration_health(
    *,
    store: ArchiveStore,
    gate: str,
    platform: str,
    policy: GatePolicy,
) -> ArchiveHealth:
    if policy.calibration.source != "archive":
        return ArchiveHealth(
            status=ArchiveHealthStatus.DISABLED,
            message=f"calibration source is {policy.calibration.source}",
            policy_enforcement=policy.enforcement,
            effective_enforcement=policy.enforcement,
        )

    required = required_clean_observations(policy)
    try:
        manifest = load_manifest(
            store,
            gate=gate,
            platform=platform,
            policy_version=policy.version,
        )
    except ArchiveMissing as exc:
        return _health(
            ArchiveHealthStatus.CALIBRATION_INSUFFICIENT,
            str(exc),
            policy,
            required_observations=required,
        )
    except ArchiveUnavailable as exc:
        return _health(ArchiveHealthStatus.UNAVAILABLE, str(exc), policy, required_observations=required)
    except ArchiveCorrupt as exc:
        return _health(ArchiveHealthStatus.CALIBRATION_CORRUPT, str(exc), policy, required_observations=required)

    if not manifest.calibrations:
        return _health(
            ArchiveHealthStatus.CALIBRATION_INSUFFICIENT,
            f"manifest has no calibration entries: {manifest.relative_path}",
            policy,
            manifest_path=manifest.relative_path,
            required_observations=required,
        )

    entry = _latest_calibration_entry(manifest.calibrations)
    try:
        read = store.read_json_with_cache(entry.path, expected_sha256=entry.sha256)
    except ArchiveUnavailable as exc:
        return _health(
            ArchiveHealthStatus.UNAVAILABLE,
            str(exc),
            policy,
            manifest_path=manifest.relative_path,
            calibration_path=entry.path,
            required_observations=required,
        )
    except ArchiveMissing as exc:
        return _health(
            ArchiveHealthStatus.CALIBRATION_CORRUPT,
            str(exc),
            policy,
            manifest_path=manifest.relative_path,
            calibration_path=entry.path,
            required_observations=required,
        )
    except ArchiveCorrupt as exc:
        return _health(
            ArchiveHealthStatus.CALIBRATION_CORRUPT,
            str(exc),
            policy,
            manifest_path=manifest.relative_path,
            calibration_path=entry.path,
            required_observations=required,
        )

    try:
        observations = _observation_count(read.payload)
    except ArchiveCorrupt as exc:
        return _health(
            ArchiveHealthStatus.CALIBRATION_CORRUPT,
            str(exc),
            policy,
            manifest_path=manifest.relative_path,
            calibration_path=entry.path,
            required_observations=required,
        )
    if observations < required:
        return _health(
            ArchiveHealthStatus.CALIBRATION_INSUFFICIENT,
            f"calibration has {observations}/{required} required clean observations",
            policy,
            manifest_path=manifest.relative_path,
            calibration_path=entry.path,
            cache_hit=read.cache_hit,
            clean_observations=observations,
            required_observations=required,
            calibration_payload=read.payload,
        )

    return ArchiveHealth(
        status=ArchiveHealthStatus.OK,
        message=f"calibration ready with {observations} clean observations",
        policy_enforcement=policy.enforcement,
        effective_enforcement=policy.enforcement,
        manifest_path=manifest.relative_path,
        calibration_path=entry.path,
        cache_hit=read.cache_hit,
        clean_observations=observations,
        required_observations=required,
        calibration_payload=read.payload,
    )


def append_gate_result(
    *,
    store: ArchiveStore,
    gate: str,
    platform: str,
    policy: GatePolicy,
    fingerprint: GateFingerprint,
    measurements: list[Any],
    run_id: str,
) -> ArchiveWrite:
    payload = build_archive_result_payload(
        gate=gate,
        platform=platform,
        policy=policy,
        fingerprint=fingerprint,
        measurements=measurements,
        run_id=run_id,
    )
    relative_path = f"results/{gate}/{platform}/{_git_commit()}-{_safe_id(run_id)}.json"
    return store.write_json_append_only(relative_path, payload)


def append_calibration_observation(
    *,
    store: ArchiveStore,
    gate: str,
    platform: str,
    policy: GatePolicy,
    fingerprint: GateFingerprint,
    measurements: list[Any],
    run_id: str,
) -> ArchiveWrite:
    payload = build_calibration_observation_payload(
        gate=gate,
        platform=platform,
        policy=policy,
        fingerprint=fingerprint,
        measurements=measurements,
        run_id=run_id,
    )
    timestamp = _timestamp_id()
    fingerprint_id = _fingerprint_id(fingerprint)
    relative_path = (
        f"calibrations/{gate}/{platform}/policy-v{policy.version}/"
        f"{fingerprint_id}-{timestamp}-{_safe_id(run_id)}.json"
    )
    return store.write_json_append_only(relative_path, payload)


def build_archive_result_payload(
    *,
    gate: str,
    platform: str,
    policy: GatePolicy,
    fingerprint: GateFingerprint,
    measurements: list[Any],
    run_id: str,
) -> dict[str, Any]:
    return _archive_payload(
        kind=ArchivePayloadKind.GATE_RESULT,
        gate=gate,
        platform=platform,
        policy=policy,
        fingerprint=fingerprint,
        measurements=measurements,
        run_id=run_id,
        observations=None,
    )


def build_calibration_observation_payload(
    *,
    gate: str,
    platform: str,
    policy: GatePolicy,
    fingerprint: GateFingerprint,
    measurements: list[Any],
    run_id: str,
) -> dict[str, Any]:
    observations = [{"sources": [_measurement_payload(measurement) for measurement in measurements]}]
    return _archive_payload(
        kind=ArchivePayloadKind.CALIBRATION_OBSERVATION,
        gate=gate,
        platform=platform,
        policy=policy,
        fingerprint=fingerprint,
        measurements=measurements,
        run_id=run_id,
        observations=observations,
    )


def recompute_calibration_from_manifest(
    *,
    store: ArchiveStore,
    manifest: ArchiveManifest,
    policy: GatePolicy,
    limit: int | None = None,
) -> ArchiveWrite:
    entries = list(manifest.results)
    if limit is None:
        limit = rolling_window_observations(policy)
    if limit is not None:
        entries = entries[-limit:]
    observations: list[dict[str, Any]] = []
    fingerprint_payload: dict[str, Any] | None = None
    for entry in entries:
        read = store.read_json_with_cache(entry.path, expected_sha256=entry.sha256)
        payload = read.payload
        if payload.get("kind") != ArchivePayloadKind.GATE_RESULT.value:
            continue
        payload_policy = payload.get("policy")
        if not isinstance(payload_policy, dict) or payload_policy.get("sha256") != policy.sha256:
            continue
        measurement = payload.get("measurements")
        if not isinstance(measurement, dict):
            continue
        sources = measurement.get("sources")
        if isinstance(sources, list):
            observations.append({"sources": sources})
        if fingerprint_payload is None:
            fingerprint_payload = {
                "system": payload.get("system_fingerprint", {}),
                "build": payload.get("build_fingerprint", {}),
                "runtime": payload.get("runtime_fingerprint", {}),
                "audit": payload.get("audit", {}),
            }

    if fingerprint_payload is None:
        fingerprint_payload = {"system": {}, "build": {}, "runtime": {}, "audit": {}}
    if not observations:
        raise ArchiveError("no matching gate result observations found for calibration recompute")
    fingerprint = GateFingerprint(
        system=_dict_or_empty(fingerprint_payload.get("system")),
        build=_dict_or_empty(fingerprint_payload.get("build")),
        runtime=_dict_or_empty(fingerprint_payload.get("runtime")),
        audit=_dict_or_empty(fingerprint_payload.get("audit")),
    )
    timestamp = _timestamp_id()
    payload = _archive_payload(
        kind=ArchivePayloadKind.CALIBRATION_RECOMPUTE,
        gate=manifest.gate,
        platform=manifest.platform,
        policy=policy,
        fingerprint=fingerprint,
        measurements=[],
        run_id=f"recompute-{timestamp}",
        observations=observations,
    )
    relative_path = (
        f"calibrations/{manifest.gate}/{manifest.platform}/policy-v{policy.version}/"
        f"recompute-{timestamp}.json"
    )
    return store.write_json_append_only(relative_path, payload)


def required_clean_observations(policy: GatePolicy) -> int:
    required = 0
    for source in policy.sources:
        if source.measurement_class == "rust_micro":
            required = max(required, policy.calibration.min_clean_observations_rust_micro)
        else:
            required = max(required, policy.calibration.min_clean_observations_sql_macro)
    return required


def rolling_window_observations(policy: GatePolicy) -> int:
    window = 0
    for source in policy.sources:
        if source.measurement_class == "rust_micro":
            window = max(window, policy.calibration.rolling_window_rust_micro)
        else:
            window = max(window, policy.calibration.rolling_window_sql_macro)
    return window


def default_run_id() -> str:
    for key in ("GITHUB_RUN_ID", "BUILD_BUILDID", "CI_PIPELINE_ID"):
        value = os.getenv(key)
        if value:
            attempt = os.getenv("GITHUB_RUN_ATTEMPT")
            return f"{value}-{attempt}" if attempt else value
    return f"local-{_timestamp_id()}-{os.getpid()}"


def _archive_payload(
    *,
    kind: ArchivePayloadKind,
    gate: str,
    platform: str,
    policy: GatePolicy,
    fingerprint: GateFingerprint,
    measurements: list[Any],
    run_id: str,
    observations: list[dict[str, Any]] | None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema_version": ARCHIVE_PAYLOAD_SCHEMA_VERSION,
        "kind": kind.value,
        "gate": gate,
        "platform": platform,
        "policy": {
            "name": policy.name,
            "version": policy.version,
            "sha256": policy.sha256,
        },
        "source": {
            "git_commit": _git_commit(),
            "run_id": run_id,
            "created_at": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        },
        "system_fingerprint": fingerprint.system,
        "build_fingerprint": fingerprint.build,
        "runtime_fingerprint": fingerprint.runtime,
        "audit": fingerprint.audit,
        "measurements": {
            "sources": [_measurement_payload(measurement) for measurement in measurements],
        },
    }
    if observations is not None:
        payload["observations"] = observations
    return payload


def _measurement_payload(measurement: Any) -> dict[str, Any]:
    return {
        "name": measurement.source.name,
        "type": measurement.source.type,
        "measurement_class": measurement.source.measurement_class,
        "payload": project_measurement_payload(measurement.payload),
    }


def _health(
    status: ArchiveHealthStatus,
    message: str,
    policy: GatePolicy,
    *,
    manifest_path: str | None = None,
    calibration_path: str | None = None,
    cache_hit: bool = False,
    clean_observations: int = 0,
    required_observations: int = 0,
    calibration_payload: dict[str, Any] | None = None,
) -> ArchiveHealth:
    effective = GateEnforcement.SOFT if policy.enforcement == GateEnforcement.HARD else policy.enforcement
    return ArchiveHealth(
        status=status,
        message=message,
        policy_enforcement=policy.enforcement,
        effective_enforcement=effective,
        manifest_path=manifest_path,
        calibration_path=calibration_path,
        cache_hit=cache_hit,
        clean_observations=clean_observations,
        required_observations=required_observations,
        calibration_payload=calibration_payload,
    )


def _observation_count(payload: dict[str, Any]) -> int:
    observations = payload.get("observations")
    if isinstance(observations, list):
        return len(observations)
    raise ArchiveCorrupt("calibration payload must contain an observations array")


def _latest_calibration_entry(entries: tuple[Any, ...]) -> Any:
    return max(entries, key=lambda entry: (_calibration_timestamp(entry.path), entry.path))


def _calibration_timestamp(path: str) -> datetime:
    match = re.search(r"\d{8}T\d{6}Z", Path(path).stem)
    if match is None:
        return datetime.min.replace(tzinfo=timezone.utc)
    try:
        return datetime.strptime(match.group(0), "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
    except ValueError:
        return datetime.min.replace(tzinfo=timezone.utc)


def _fingerprint_id(fingerprint: GateFingerprint) -> str:
    payload = {
        "system": fingerprint.system,
        "build": fingerprint.build,
        "runtime": fingerprint.runtime,
    }
    return sha256_bytes(canonical_json_bytes(payload))[:16]


def _timestamp_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _git_commit() -> str:
    try:
        return subprocess.check_output(
            ("git", "rev-parse", "HEAD"),
            stderr=subprocess.DEVNULL,
            text=True,
            timeout=2,
        ).strip()
    except (OSError, subprocess.SubprocessError):
        return UNKNOWN


def _safe_id(value: str) -> str:
    normalized = re.sub(r"[^A-Za-z0-9_.-]+", "-", value.strip())
    return normalized.strip("-") or "run"


def _dict_or_empty(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Recover Paro performance archive calibration files")
    subparsers = parser.add_subparsers(dest="action", required=True)

    recompute = subparsers.add_parser("recompute", help="Recompute calibration from manifest results")
    recompute.add_argument("--archive", type=Path, required=True)
    recompute.add_argument("--cache", type=Path)
    recompute.add_argument("--policy", type=Path, required=True)
    recompute.add_argument("--gate", required=True)
    recompute.add_argument("--platform", default="auto")
    recompute.add_argument("--limit", type=int)

    args = parser.parse_args(argv)
    cache = args.cache if args.cache is not None else args.archive / ".cache"
    store = ArchiveStore(root=args.archive, cache_root=cache)
    if args.action == "recompute":
        policy = load_policy(args.policy)
        platform = platform_key() if args.platform == "auto" else args.platform
        manifest = load_manifest(
            store,
            gate=args.gate,
            platform=platform,
            policy_version=policy.version,
        )
        write = recompute_calibration_from_manifest(
            store=store,
            manifest=manifest,
            policy=policy,
            limit=args.limit,
        )
        print(f"calibration recomputed: {write.relative_path} sha256={write.sha256}")
        print(f"rebuild manifest after recompute: {manifest_relative_path(args.gate, args.platform, policy.version)}")
        return 0
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
