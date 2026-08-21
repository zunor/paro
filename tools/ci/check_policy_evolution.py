# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

"""Validate that policy-evolution PRs can refresh enforced gate baselines."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import sys


DEFAULT_REQUIRED_PLATFORMS = "linux-amd64,macos-arm64"
REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "benchmark"))

from harness.performance_gate import (  # noqa: E402
    BaselineError,
    GateEnforcement,
    GateFingerprint,
    load_baseline,
    load_policy,
    validate_baseline_for_check,
)
from harness.archive.calibration import load_calibration_health  # noqa: E402
from harness.archive.store import ArchiveStore  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    parser.add_argument(
        "--available-platforms",
        default=os.getenv("PARO_POLICY_EVOLUTION_PLATFORMS", "linux-amd64"),
        help="Comma-separated platform keys with configured policy-evolution runners.",
    )
    parser.add_argument(
        "--archive",
        type=Path,
        default=Path(os.getenv("PARO_PERF_ARCHIVE_DIR", ".ci/paro-perf-archive")),
        help="Performance archive containing promotion calibration.",
    )
    parser.add_argument(
        "--required-platforms",
        default=os.getenv("PARO_POLICY_EVOLUTION_REQUIRED_PLATFORMS", DEFAULT_REQUIRED_PLATFORMS),
        help="Comma-separated platform keys required before a non-shadow policy can be promoted.",
    )
    args = parser.parse_args(argv)

    repo_root = args.repo_root.resolve()
    available = _csv_set(args.available_platforms)
    required = _csv_set(args.required_platforms)
    errors = _validate(
        repo_root=repo_root,
        available_platforms=available,
        required_platforms=required,
        archive_root=args.archive.resolve(),
    )
    if errors:
        print("policy evolution guard failed:", file=sys.stderr)
        for error in errors:
            print(f"- {error}", file=sys.stderr)
        return 1
    print("policy evolution guard passed")
    return 0


def _validate(
    *,
    repo_root: Path,
    available_platforms: set[str],
    required_platforms: set[str],
    archive_root: Path | None = None,
) -> list[str]:
    errors: list[str] = []
    archive_root = archive_root or repo_root / ".ci" / "paro-perf-archive"
    archive = ArchiveStore(
        root=archive_root,
        cache_root=repo_root / ".ci" / "policy-evolution-archive-cache",
    )
    policy_dir = repo_root / "benchmark" / "policies"
    for policy_path in sorted(policy_dir.glob("*.toml")):
        try:
            policy = load_policy(policy_path)
        except ValueError as exc:
            errors.append(str(exc))
            continue
        if policy.enforcement is GateEnforcement.SHADOW:
            continue
        missing_runners = sorted(required_platforms - available_platforms)
        if missing_runners:
            errors.append(
                f"{policy.name} is {policy.enforcement.value}, but policy-evolution runners are missing for "
                f"{', '.join(missing_runners)}"
            )
            continue
        for platform in sorted(required_platforms):
            baseline_path = repo_root / "benchmark" / "baselines" / policy.name / f"{platform}.json"
            if not baseline_path.exists():
                errors.append(f"{policy.name} is {policy.enforcement.value}, but baseline is missing: {baseline_path}")
                continue
            try:
                baseline = load_baseline(baseline_path)
                validate_baseline_for_check(
                    baseline=baseline,
                    policy=policy,
                    platform_key=platform,
                    fingerprint=GateFingerprint(
                        system=baseline.system_fingerprint,
                        build=baseline.build_fingerprint,
                        runtime=baseline.runtime_fingerprint,
                        audit={},
                    ),
                    execution_mode="quorum",
                )
            except BaselineError as exc:
                errors.append(f"{baseline_path}: {exc}")
            if policy.calibration.source == "archive":
                health = load_calibration_health(
                    store=archive,
                    gate=policy.name,
                    platform=platform,
                    policy=policy,
                )
                if not health.ok:
                    errors.append(
                        f"{policy.name} cannot leave shadow on {platform}: {health.message}"
                    )
    return errors


def _csv_set(value: str) -> set[str]:
    return {item.strip() for item in value.split(",") if item.strip()}


if __name__ == "__main__":
    raise SystemExit(main())
