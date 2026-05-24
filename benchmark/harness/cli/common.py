# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

"""Shared helpers for `runner.py gate` subcommands."""

from __future__ import annotations

import argparse
from dataclasses import replace
from pathlib import Path

from ..archive.calibration import ArchiveHealth
from ..archive.store import ArchiveStore
from ..performance_gate import (
    BaselineError,
    FingerprintError,
    GateOutcome,
    PolicyError,
    StagingError,
    collect_fingerprint,
    load_baseline,
    load_policy,
    load_staging_queries,
    platform_key,
    validate_baseline_for_check,
    validate_bless_fingerprint,
)
from ..performance_gate.policy import GatePolicy
from ..process_probe import ProcessProbeError, resolve_parod_process
from ..sources import SourceContext, SourceMeasurement, default_registry


class GateCommandError(RuntimeError):
    pass


def load_policy_for_gate(root_dir: Path, gate: str, value: str) -> GatePolicy:
    policy_path = resolve_policy(root_dir, gate, value)
    try:
        policy = load_policy(policy_path)
    except PolicyError as exc:
        raise GateCommandError(str(exc)) from exc
    if policy.name != gate:
        raise GateCommandError(f"policy name '{policy.name}' does not match gate '{gate}'")
    return policy


def load_baseline_checked(path: Path):
    try:
        return load_baseline(path)
    except BaselineError as exc:
        raise GateCommandError(str(exc)) from exc


def validate_baseline_for_gate(**kwargs) -> None:
    try:
        validate_baseline_for_check(
            platform_key=platform_key(),
            **kwargs,
        )
    except BaselineError as exc:
        raise GateCommandError(str(exc)) from exc


def load_staging_queries_checked(root_dir: Path, policy: GatePolicy):
    try:
        return load_staging_queries(root_dir=root_dir, policy=policy)
    except StagingError as exc:
        raise GateCommandError(str(exc)) from exc


def validate_bless_fingerprint_checked(policy: GatePolicy, fingerprint) -> None:
    try:
        validate_bless_fingerprint(
            fingerprint,
            require_fresh_runtime=requires_fresh_runtime(policy),
        )
    except FingerprintError as exc:
        raise GateCommandError(str(exc)) from exc


def resolve_policy(root_dir: Path, gate: str, value: str) -> Path:
    if value != "auto":
        path = Path(value)
        if not path.is_absolute():
            cwd_path = (Path.cwd() / path).resolve()
            root_path = (root_dir / path).resolve()
            path = cwd_path if cwd_path.exists() else root_path
        if not path.exists():
            raise GateCommandError(f"policy not found: {path}")
        return path

    path = root_dir / "policies" / f"{gate}.toml"
    if not path.exists():
        raise GateCommandError(f"policy not found: {path}")
    return path


def resolve_baseline(root_dir: Path, gate: str, value: str, *, must_exist: bool = True) -> Path:
    if value != "auto":
        path = Path(value)
        if not path.is_absolute():
            cwd_path = (Path.cwd() / path).resolve()
            root_path = (root_dir / path).resolve()
            path = cwd_path if cwd_path.exists() else root_path
        if must_exist and not path.exists():
            raise GateCommandError(f"baseline not found: {path}")
        return path

    platform = platform_key()
    path = root_dir / "baselines" / gate / f"{platform}.json"
    if must_exist and not path.exists():
        raise GateCommandError(f"baseline not found: {path}")
    return path


def platform_from_baseline_path(path: Path, *, gate: str) -> str | None:
    if path.parent.name != gate:
        return None
    if path.suffix != ".json":
        return None
    return path.stem


def archive_store(root_dir: Path, args: argparse.Namespace) -> ArchiveStore:
    defaults = ArchiveStore.from_env(root_dir)
    archive = defaults.root if args.archive == "auto" else resolve_path(root_dir, args.archive)
    cache = defaults.cache_root if args.archive_cache == "auto" else resolve_path(root_dir, args.archive_cache)
    return ArchiveStore(root=archive, cache_root=cache, cache_ttl_seconds=defaults.cache_ttl_seconds)


def resolve_path(root_dir: Path, value: str) -> Path:
    path = Path(value)
    if path.is_absolute():
        return path
    return (root_dir / path).resolve()


def resolve_pid(args: argparse.Namespace, *, root_dir: Path, policy: GatePolicy) -> int:
    try:
        return resolve_parod_process(
            args.pid,
            root_dir=root_dir,
            require=requires_parod_pid(policy),
        ).pid
    except ProcessProbeError as exc:
        raise GateCommandError(str(exc)) from exc


def requires_parod_pid(policy: GatePolicy) -> bool:
    return "rss_peak_kb" in policy.metrics


def requires_fresh_runtime(policy: GatePolicy) -> bool:
    return any(source.measurement_class == "sql_macro" for source in policy.sources)


def minimum_sample_count(policy: GatePolicy, source_name: str) -> int:
    source = next((item for item in policy.sources if item.name == source_name), None)
    if source is None:
        return 1
    if source.measurement_class == "rust_micro":
        return policy.calibration.per_run_sample_count_rust_micro
    return policy.calibration.per_run_sample_count_sql_macro


def run_sources(
    args: argparse.Namespace,
    *,
    policy: GatePolicy,
    root_dir: Path,
    runner_module: object,
    pid: int,
    retry_query_keys_by_source=None,
) -> list[SourceMeasurement]:
    include_sources = frozenset(args.include_source)
    skip_sources = frozenset(args.skip_source)
    registry = default_registry()
    measurements = []
    for source in policy.sources:
        if include_sources and source.name not in include_sources:
            continue
        if source.name in skip_sources:
            continue
        adapter = registry.get(source.type)
        retry_keys = frozenset()
        if retry_query_keys_by_source is not None:
            retry_keys = retry_query_keys_by_source.get(source.name, frozenset())
            if not retry_keys:
                continue
        source_context = SourceContext(
            root_dir=root_dir,
            pid=pid,
            runner_module=runner_module,
            retry_query_keys=retry_keys,
            minimum_sample_count=minimum_sample_count(policy, source.name),
        )
        try:
            measurements.append(adapter.execute(source, source_context))
        except NotImplementedError as exc:
            raise GateCommandError(str(exc)) from exc
        except ValueError as exc:
            raise GateCommandError(str(exc)) from exc
    return measurements


def policy_for_source_family(policy: GatePolicy, source_count: int) -> GatePolicy:
    divisor = max(1, source_count)
    if divisor == 1:
        return policy
    return replace(
        policy,
        statistics=replace(
            policy.statistics,
            family_alpha_first_run=policy.statistics.family_alpha_first_run / divisor,
            family_alpha_confirmed=policy.statistics.family_alpha_confirmed / divisor,
        ),
    )


def with_archive_health(outcome: GateOutcome, archive_health: ArchiveHealth) -> GateOutcome:
    return replace(outcome, archive_health=archive_health)
