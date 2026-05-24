# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

"""`runner.py gate bisect` implementation."""

from __future__ import annotations

import argparse
from dataclasses import replace
from pathlib import Path

from ..archive.calibration import ArchivePayloadKind, load_calibration_health
from ..archive.manifest import result_prefix
from ..archive.store import ArchiveError, ArchiveStore
from ..performance_gate import evaluate_gate, platform_key
from ..reporter import BenchmarkReporter
from .common import (
    GateCommandError,
    archive_store,
    load_policy_for_gate,
    load_staging_queries_checked,
    policy_for_source_family,
    resolve_pid,
    run_sources,
    with_archive_health,
)


def run_bisect(args: argparse.Namespace, *, root_dir: Path, runner_module: object) -> int:
    policy = load_policy_for_gate(root_dir, args.gate, args.policy)
    store = archive_store(root_dir, args)
    platform = platform_key()
    archived_payload = load_archived_gate_result(
        store=store,
        gate=args.gate,
        platform=platform,
        policy=policy,
        against=args.against,
    )
    archived_sources = archived_source_payloads(archived_payload)
    archive_health = load_calibration_health(
        store=store,
        gate=args.gate,
        platform=platform,
        policy=policy,
    )
    effective_policy = replace(policy, enforcement=archive_health.effective_enforcement)
    pid = resolve_pid(args, root_dir=root_dir, policy=policy)
    staging_queries = load_staging_queries_checked(root_dir, policy)
    measurements = run_sources(args, policy=policy, root_dir=root_dir, runner_module=runner_module, pid=pid)
    if not measurements:
        raise GateCommandError("gate selected no measurement sources")
    family_policy = policy_for_source_family(effective_policy, len(measurements))

    reporter = BenchmarkReporter(root_dir)
    results = []
    for measurement in measurements:
        try:
            baseline_payload = archived_sources[measurement.source.name]
        except KeyError as exc:
            raise GateCommandError(
                f"archived result for {args.against} has no source '{measurement.source.name}'"
            ) from exc
        results.append((
            measurement,
            with_archive_health(
                evaluate_gate(
                    policy=family_policy,
                    current_payload=measurement.payload,
                    baseline_payload=baseline_payload,
                    calibration_payload=archive_health.calibration_payload if archive_health.ok else None,
                    source_name=measurement.source.name,
                    staging_queries=staging_queries,
                ),
                archive_health,
            ),
        ))

    for measurement, outcome in results:
        reporter.print_gate_outcome(outcome)
        reporter.append_gate_outcome_to_summary(measurement.summary_path, outcome)
    reporter.write_gate_report(
        gate=args.gate,
        outcomes=[outcome for _, outcome in results],
        archive_health=archive_health,
    )
    return 1 if any(measurement.failed or outcome.failed for measurement, outcome in results) else 0


def load_archived_gate_result(
    *,
    store: ArchiveStore,
    gate: str,
    platform: str,
    policy,
    against: str,
) -> dict:
    prefix = result_prefix(gate, platform)
    try:
        candidates = [
            path
            for path in store.list_json(prefix)
            if Path(path).name.startswith(against)
        ]
    except ArchiveError as exc:
        raise GateCommandError(str(exc)) from exc
    if not candidates:
        raise GateCommandError(f"archive has no result for {gate}/{platform} commit prefix {against}")
    if len(candidates) > 1 and len(against) < 12:
        raise GateCommandError(
            f"archive result prefix {against} is ambiguous; use a longer commit prefix "
            f"({len(candidates)} matches)"
        )
    selected = sorted(candidates)[-1]
    try:
        payload = store.read_json_with_cache(selected).payload
    except ArchiveError as exc:
        raise GateCommandError(str(exc)) from exc
    if payload.get("kind") != ArchivePayloadKind.GATE_RESULT.value:
        raise GateCommandError(f"archive object is not a gate result: {selected}")
    if payload.get("gate") != gate or payload.get("platform") != platform:
        raise GateCommandError(f"archive object does not match {gate}/{platform}: {selected}")
    payload_policy = payload.get("policy")
    if not isinstance(payload_policy, dict) or payload_policy.get("sha256") != policy.sha256:
        raise GateCommandError(
            f"archive result {selected} was produced with a different policy; "
            "refresh archive or compare against a matching policy version"
        )
    return payload


def archived_source_payloads(payload: dict) -> dict[str, dict]:
    measurements = payload.get("measurements")
    if not isinstance(measurements, dict):
        raise GateCommandError("archive result missing measurements object")
    sources = measurements.get("sources")
    if not isinstance(sources, list):
        raise GateCommandError("archive result measurements.sources must be an array")
    by_name: dict[str, dict] = {}
    for index, source in enumerate(sources, start=1):
        if not isinstance(source, dict):
            raise GateCommandError(f"archive result source #{index} must be an object")
        name = source.get("name")
        payload = source.get("payload")
        if not isinstance(name, str) or not name:
            raise GateCommandError(f"archive result source #{index} has invalid name")
        if not isinstance(payload, dict):
            raise GateCommandError(f"archive result source '{name}' payload must be an object")
        by_name[name] = payload
    return by_name
