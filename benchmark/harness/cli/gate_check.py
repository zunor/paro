# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

"""`runner.py gate check` implementation."""

from __future__ import annotations

import argparse
from dataclasses import replace
from pathlib import Path

from ..archive.calibration import load_calibration_health
from ..baseline_index import QueryKey
from ..performance_gate import (
    BaselineError,
    GateEnforcement,
    GateEntry,
    GateEntryKind,
    GateOutcome,
    GatePolicy,
    GateStatus,
    baseline_payload_for_source,
    collect_fingerprint,
    evaluate_gate,
)
from ..reporter import BenchmarkReporter
from ..sources import SourceMeasurement
from .common import (
    GateCommandError,
    archive_store,
    load_baseline_checked,
    load_policy_for_gate,
    load_staging_queries_checked,
    policy_for_source_family,
    resolve_baseline,
    resolve_pid,
    run_sources,
    validate_baseline_for_gate,
    with_archive_health,
)


def run_check(args: argparse.Namespace, *, root_dir: Path, runner_module: object) -> int:
    policy = load_policy_for_gate(root_dir, args.gate, args.policy)
    baseline_path = resolve_baseline(
        root_dir,
        args.gate,
        args.baseline,
        must_exist=args.baseline != "auto",
    )
    if not baseline_path.exists():
        return report_missing_auto_baseline(
            args,
            root_dir=root_dir,
            policy=policy,
            baseline_path=baseline_path,
        )
    baseline = load_baseline_checked(baseline_path)
    pid = resolve_pid(args, root_dir=root_dir, policy=policy)
    fingerprint = collect_fingerprint(root_dir, pid=pid)
    validate_baseline_for_gate(
        baseline=baseline,
        policy=policy,
        fingerprint=fingerprint,
        execution_mode=args.execution_mode,
    )
    archive_health = load_calibration_health(
        store=archive_store(root_dir, args),
        gate=args.gate,
        platform=baseline.platform,
        policy=policy,
    )
    effective_policy = replace(policy, enforcement=archive_health.effective_enforcement)
    staging_queries = load_staging_queries_checked(root_dir, policy)
    measurements = run_sources(args, policy=policy, root_dir=root_dir, runner_module=runner_module, pid=pid)
    if not measurements:
        raise GateCommandError("gate selected no measurement sources")
    if args.quorum_retries < 1:
        raise GateCommandError("--quorum-retries must be >= 1")
    family_policy = policy_for_source_family(effective_policy, len(measurements))

    reporter = BenchmarkReporter(root_dir)
    results = []
    for measurement in measurements:
        try:
            baseline_payload = baseline_payload_for_source(baseline, measurement.source.name)
        except BaselineError as exc:
            raise GateCommandError(str(exc)) from exc
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

    retry_failed: dict[str, bool] = {}
    retry_keys_by_source = retry_keys_by_source_for(results)
    if args.execution_mode == "quorum" and retry_keys_by_source:
        retry_runs = [
            run_sources(
                args,
                policy=policy,
                root_dir=root_dir,
                runner_module=runner_module,
                pid=pid,
                retry_query_keys_by_source=retry_keys_by_source,
            )
            for _ in range(args.quorum_retries)
        ]
        retry_by_source: dict[str, list[SourceMeasurement]] = {}
        for retry_measurements in retry_runs:
            for retry_measurement in retry_measurements:
                retry_by_source.setdefault(retry_measurement.source.name, []).append(retry_measurement)
        confirmed_results = []
        for measurement, first_outcome in results:
            source_retries = retry_by_source.get(measurement.source.name, [])
            retry_keys = retry_keys_by_source.get(measurement.source.name)
            if not source_retries or not retry_keys:
                confirmed_results.append((measurement, first_outcome))
                continue
            retry_failed[measurement.source.name] = any(retry.failed for retry in source_retries)
            try:
                baseline_payload = baseline_payload_for_source(baseline, measurement.source.name)
            except BaselineError as exc:
                raise GateCommandError(str(exc)) from exc
            confirmed = with_archive_health(
                evaluate_gate(
                    policy=family_policy,
                    current_payload=source_retries[-1].payload,
                    baseline_payload=baseline_payload,
                    calibration_payload=archive_health.calibration_payload if archive_health.ok else None,
                    source_name=measurement.source.name,
                    confirmation_payloads=(
                        measurement.payload,
                        *(retry.payload for retry in source_retries[:-1]),
                    ),
                    only_keys=retry_keys,
                    statistical_phase="confirmed",
                    staging_queries=staging_queries,
                ),
                archive_health,
            )
            confirmed_results.append((measurement, merge_confirmed_outcome(first_outcome, confirmed, retry_keys)))
        results = confirmed_results

    for measurement, outcome in results:
        reporter.print_gate_outcome(outcome)
        reporter.append_gate_outcome_to_summary(measurement.summary_path, outcome)
    reporter.write_gate_report(
        gate=args.gate,
        outcomes=[outcome for _, outcome in results],
        archive_health=archive_health,
    )

    return 1 if any(
        measurement.failed
        or retry_failed.get(measurement.source.name, False)
        or outcome.blocking_failed
        for measurement, outcome in results
    ) else 0


def report_missing_auto_baseline(
    args: argparse.Namespace,
    *,
    root_dir: Path,
    policy: GatePolicy,
    baseline_path: Path,
) -> int:
    status = (
        GateStatus.REGRESS
        if policy.enforcement == GateEnforcement.HARD
        else GateStatus.UNMEASURABLE
    )
    outcome = GateOutcome(
        gate=args.gate,
        enforcement=policy.enforcement,
        entries=(
            GateEntry(
                kind=GateEntryKind.INVALID_BASELINE,
                key=QueryKey("baseline", baseline_path.stem, "{}"),
                metric="baseline",
                status=status,
                detail=f"auto baseline not found: {baseline_path}",
            ),
        ),
    )
    reporter = BenchmarkReporter(root_dir)
    reporter.print_gate_outcome(outcome)
    reporter.write_gate_report(gate=args.gate, outcomes=[outcome])
    return 1 if outcome.blocking_failed else 0


def retry_keys_by_source_for(results: list[tuple[SourceMeasurement, GateOutcome]]) -> dict[str, frozenset[QueryKey]]:
    retry: dict[str, set[QueryKey]] = {}
    for measurement, outcome in results:
        for entry in outcome.entries:
            if entry.kind != GateEntryKind.COMPARED_METRIC or entry.status != GateStatus.REGRESS:
                continue
            retry.setdefault(measurement.source.name, set()).add(entry.key)
    return {source: frozenset(keys) for source, keys in retry.items() if keys}


def merge_confirmed_outcome(
    first: GateOutcome,
    confirmed: GateOutcome,
    retry_keys: frozenset[QueryKey],
) -> GateOutcome:
    confirmed_compared = {
        (entry.key, entry.metric): entry
        for entry in confirmed.entries
        if entry.kind == GateEntryKind.COMPARED_METRIC
    }
    merged = []
    for entry in first.entries:
        if entry.key in retry_keys and entry.kind == GateEntryKind.POWER_DEFICIT:
            continue
        if entry.key in retry_keys and entry.kind == GateEntryKind.COMPARED_METRIC:
            replacement = confirmed_compared.pop((entry.key, entry.metric), None)
            if replacement is not None:
                merged.append(replacement)
            continue
        merged.append(entry)
    merged.extend(confirmed_compared.values())
    merged.extend(entry for entry in confirmed.entries if entry.kind == GateEntryKind.POWER_DEFICIT)
    return replace(
        first,
        entries=tuple(sorted(
            merged,
            key=lambda entry: (entry.workload, entry.query_id, entry.metric, entry.kind.value),
        )),
    )
