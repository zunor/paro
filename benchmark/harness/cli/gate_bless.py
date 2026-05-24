# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

"""`runner.py gate bless` implementation."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

from ..archive.store import canonical_json_bytes
from ..performance_gate import (
    BaselineError,
    aggregate_measurement_runs,
    baseline_payload_for_source,
    build_baseline_payload,
    collect_fingerprint,
    evaluate_gate,
    platform_key,
    validate_existing_baseline_for_bless,
)
from .common import (
    GateCommandError,
    load_baseline_checked,
    load_policy_for_gate,
    load_staging_queries_checked,
    platform_from_baseline_path,
    resolve_baseline,
    resolve_pid,
    run_sources,
    validate_bless_fingerprint_checked,
)


def run_bless(args: argparse.Namespace, *, root_dir: Path, runner_module: object) -> int:
    policy = load_policy_for_gate(root_dir, args.gate, args.policy)
    baseline_path = resolve_baseline(root_dir, args.gate, args.baseline, must_exist=False)
    current_platform = platform_key()
    path_platform = platform_from_baseline_path(baseline_path, gate=args.gate)
    if path_platform is not None and path_platform != current_platform:
        raise GateCommandError(f"cannot bless platform '{path_platform}' from current platform '{current_platform}'")
    pid = resolve_pid(args, root_dir=root_dir, policy=policy)
    fingerprint = collect_fingerprint(root_dir, pid=pid)
    validate_bless_fingerprint_checked(policy, fingerprint)
    existing_baseline = load_baseline_checked(baseline_path) if baseline_path.exists() else None
    if existing_baseline is not None:
        try:
            validate_existing_baseline_for_bless(
                baseline=existing_baseline,
                policy=policy,
                platform_key=current_platform,
                policy_evolution=args.policy_evolution,
            )
        except BaselineError as exc:
            raise GateCommandError(str(exc)) from exc

    if args.bless_runs < 1:
        raise GateCommandError("--bless-runs must be >= 1")
    staging_queries = load_staging_queries_checked(root_dir, policy)
    measurement_runs = []
    for run_index in range(args.bless_runs):
        measurements = run_sources(args, policy=policy, root_dir=root_dir, runner_module=runner_module, pid=pid)
        measurement_runs.append(measurements)
        if not measurements:
            raise GateCommandError("gate selected no measurement sources")
        if any(measurement.failed for measurement in measurements):
            raise GateCommandError(f"bless run {run_index + 1}/{args.bless_runs} failed; refusing baseline update")
        if existing_baseline is not None:
            _validate_bless_run_against_baseline(
                policy=policy,
                existing_baseline=existing_baseline,
                measurements=measurements,
                staging_queries=staging_queries,
                run_label=f"{run_index + 1}/{args.bless_runs}",
            )

    try:
        baseline_measurements = aggregate_measurement_runs(measurement_runs)
    except BaselineError as exc:
        raise GateCommandError(str(exc)) from exc

    payload = build_baseline_payload(
        gate=args.gate,
        platform_key=current_platform,
        policy=policy,
        fingerprint=fingerprint,
        measurements=baseline_measurements,
    )
    baseline_path.parent.mkdir(parents=True, exist_ok=True)
    baseline_path.write_bytes(canonical_json_bytes(payload) + b"\n")
    print(f"baseline updated: {baseline_path}")
    return 0


def _validate_bless_run_against_baseline(
    *,
    policy,
    existing_baseline,
    measurements,
    staging_queries,
    run_label: str,
) -> None:
    warn_only = existing_baseline.policy_sha256 != policy.sha256
    for measurement in measurements:
        try:
            baseline_payload = baseline_payload_for_source(existing_baseline, measurement.source.name)
        except BaselineError as exc:
            raise GateCommandError(str(exc)) from exc
        outcome = evaluate_gate(
            policy=policy,
            current_payload=measurement.payload,
            baseline_payload=baseline_payload,
            staging_queries=staging_queries,
        )
        if outcome.failed and warn_only:
            print(
                f"policy evolution warning: bless run {run_label} "
                f"source {measurement.source.name} regressed against previous baseline",
                file=sys.stderr,
            )
        elif outcome.failed:
            raise GateCommandError(
                f"bless run {run_label} source {measurement.source.name} regressed against current baseline"
            )
