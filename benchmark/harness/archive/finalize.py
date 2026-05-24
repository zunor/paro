# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

"""Finalize archive manifests and rolling calibration in one process."""

from __future__ import annotations

import argparse
from pathlib import Path

from ..performance_gate.fingerprint import platform_key
from ..performance_gate.policy import load_policy
from .calibration import recompute_calibration_from_manifest
from .manifest import load_manifest, rebuild_manifest
from .store import ArchiveStore


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", type=Path, required=True)
    parser.add_argument("--cache", type=Path)
    parser.add_argument("--gate", required=True)
    parser.add_argument("--platform", default="auto")
    parser.add_argument("--policy", type=Path)
    parser.add_argument("--policy-version", default="auto")
    args = parser.parse_args(argv)

    cache = args.cache if args.cache is not None else args.archive / ".cache"
    store = ArchiveStore(root=args.archive, cache_root=cache)
    policy = load_policy(args.policy if args.policy is not None else Path("policies") / f"{args.gate}.toml")
    if policy.name != args.gate:
        raise SystemExit(f"policy name '{policy.name}' does not match gate '{args.gate}'")
    policy_version = _resolve_policy_version(policy.version, args.policy_version)
    platform = platform_key() if args.platform == "auto" else args.platform

    first = rebuild_manifest(
        store,
        gate=args.gate,
        platform=platform,
        policy_version=policy_version,
    )
    manifest = load_manifest(
        store,
        gate=args.gate,
        platform=platform,
        policy_version=policy_version,
    )
    calibration = recompute_calibration_from_manifest(
        store=store,
        manifest=manifest,
        policy=policy,
    )
    final = rebuild_manifest(
        store,
        gate=args.gate,
        platform=platform,
        policy_version=policy_version,
    )

    print(f"manifest rebuilt: {first.relative_path} sha256={first.sha256}")
    print(f"calibration recomputed: {calibration.relative_path} sha256={calibration.sha256}")
    print(f"manifest finalized: {final.relative_path} sha256={final.sha256}")
    return 0


def _resolve_policy_version(policy_version: int, value: str) -> int:
    if value == "auto":
        return policy_version
    try:
        parsed = int(value)
    except ValueError as exc:
        raise SystemExit(f"--policy-version must be an integer or auto: {value}") from exc
    if parsed < 1:
        raise SystemExit("--policy-version must be >= 1")
    if parsed != policy_version:
        raise SystemExit(
            f"--policy-version {parsed} does not match loaded policy version {policy_version}"
        )
    return parsed


if __name__ == "__main__":
    raise SystemExit(main())
