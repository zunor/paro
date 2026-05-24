# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

"""`runner.py gate calibrate` implementation."""

from __future__ import annotations

import argparse
from pathlib import Path

from ..archive.calibration import append_calibration_observation, append_gate_result, default_run_id
from ..performance_gate import BaselineError, baseline_payload_for_source, collect_fingerprint, evaluate_gate, platform_key
from .common import (
    GateCommandError,
    archive_store,
    load_baseline_checked,
    load_policy_for_gate,
    load_staging_queries_checked,
    resolve_baseline,
    resolve_pid,
    run_sources,
)


def run_calibrate(args: argparse.Namespace, *, root_dir: Path, runner_module: object) -> int:
    policy = load_policy_for_gate(root_dir, args.gate, args.policy)
    baseline = load_baseline_checked(resolve_baseline(root_dir, args.gate, args.baseline))
    current_platform = platform_key()
    pid = resolve_pid(args, root_dir=root_dir, policy=policy)
    fingerprint = collect_fingerprint(root_dir, pid=pid)
    staging_queries = load_staging_queries_checked(root_dir, policy)
    measurements = run_sources(args, policy=policy, root_dir=root_dir, runner_module=runner_module, pid=pid)
    if not measurements:
        raise GateCommandError("gate selected no measurement sources")
    if any(measurement.failed for measurement in measurements):
        raise GateCommandError("calibration source run failed; refusing archive append")
    for measurement in measurements:
        try:
            baseline_payload = baseline_payload_for_source(baseline, measurement.source.name)
        except BaselineError as exc:
            raise GateCommandError(str(exc)) from exc
        outcome = evaluate_gate(
            policy=policy,
            current_payload=measurement.payload,
            baseline_payload=baseline_payload,
            staging_queries=staging_queries,
        )
        if outcome.failed:
            raise GateCommandError(
                f"calibration source {measurement.source.name} regressed against baseline; refusing archive append"
            )

    store = archive_store(root_dir, args)
    run_id = default_run_id() if args.run_id == "auto" else args.run_id
    try:
        result = append_gate_result(
            store=store,
            gate=args.gate,
            platform=current_platform,
            policy=policy,
            fingerprint=fingerprint,
            measurements=measurements,
            run_id=run_id,
        )
        calibration = append_calibration_observation(
            store=store,
            gate=args.gate,
            platform=current_platform,
            policy=policy,
            fingerprint=fingerprint,
            measurements=measurements,
            run_id=run_id,
        )
    except ValueError as exc:
        raise GateCommandError(str(exc)) from exc

    print(f"archive result appended: {result.relative_path} sha256={result.sha256}")
    print(f"archive calibration appended: {calibration.relative_path} sha256={calibration.sha256}")
    print("manifest not updated by calibrate; run archive manifest rebuild from listing")
    return 0
