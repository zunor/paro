# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

"""Manifest rebuild and verification for the performance archive."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ..performance_gate.fingerprint import platform_key
from ..performance_gate.policy import load_policy
from .store import ArchiveCorrupt, ArchiveStore, ArchiveWrite, canonical_json_bytes, sha256_bytes


MANIFEST_SCHEMA_VERSION = 1


@dataclass(frozen=True)
class ManifestEntry:
    path: str
    sha256: str
    size_bytes: int


@dataclass(frozen=True)
class ArchiveManifest:
    gate: str
    platform: str
    policy_version: int
    results: tuple[ManifestEntry, ...]
    calibrations: tuple[ManifestEntry, ...]
    relative_path: str


def manifest_relative_path(gate: str, platform: str, policy_version: int) -> str:
    return f"manifests/{gate}/{platform}/policy-v{policy_version}.json"


def result_prefix(gate: str, platform: str) -> str:
    return f"results/{gate}/{platform}"


def calibration_prefix(gate: str, platform: str, policy_version: int) -> str:
    return f"calibrations/{gate}/{platform}/policy-v{policy_version}"


def rebuild_manifest(
    store: ArchiveStore,
    *,
    gate: str,
    platform: str,
    policy_version: int,
) -> ArchiveWrite:
    payload = build_manifest_payload(
        store,
        gate=gate,
        platform=platform,
        policy_version=policy_version,
    )
    relative_path = manifest_relative_path(gate, platform, policy_version)
    return _write_manifest_replace(store, relative_path, payload)


def build_manifest_payload(
    store: ArchiveStore,
    *,
    gate: str,
    platform: str,
    policy_version: int,
) -> dict[str, Any]:
    results = _entries_for_paths(store, store.list_json(result_prefix(gate, platform)))
    calibrations = _entries_for_paths(store, store.list_json(calibration_prefix(gate, platform, policy_version)))
    return {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "gate": gate,
        "platform": platform,
        "policy_version": policy_version,
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "results": [entry.__dict__ for entry in results],
        "calibrations": [entry.__dict__ for entry in calibrations],
    }


def load_manifest(
    store: ArchiveStore,
    *,
    gate: str,
    platform: str,
    policy_version: int,
) -> ArchiveManifest:
    relative_path = manifest_relative_path(gate, platform, policy_version)
    read = store.read_json_with_cache(relative_path)
    payload = read.payload
    if payload.get("schema_version") != MANIFEST_SCHEMA_VERSION:
        raise ArchiveCorrupt(f"manifest {relative_path} has unsupported schema_version")
    if payload.get("gate") != gate or payload.get("platform") != platform:
        raise ArchiveCorrupt(f"manifest {relative_path} does not match requested gate/platform")
    if payload.get("policy_version") != policy_version:
        raise ArchiveCorrupt(f"manifest {relative_path} does not match requested policy version")
    return ArchiveManifest(
        gate=gate,
        platform=platform,
        policy_version=policy_version,
        results=tuple(_parse_entries(payload.get("results"), "results", relative_path)),
        calibrations=tuple(_parse_entries(payload.get("calibrations"), "calibrations", relative_path)),
        relative_path=relative_path,
    )


def verify_manifest(store: ArchiveStore, manifest: ArchiveManifest) -> None:
    for entry in (*manifest.results, *manifest.calibrations):
        store.read_json_with_cache(entry.path, expected_sha256=entry.sha256)


def _entries_for_paths(store: ArchiveStore, paths: list[str]) -> tuple[ManifestEntry, ...]:
    entries: list[ManifestEntry] = []
    for relative_path in paths:
        path = store.root / relative_path
        data = path.read_bytes()
        entries.append(
            ManifestEntry(
                path=relative_path,
                sha256=sha256_bytes(data),
                size_bytes=len(data),
            )
        )
    return tuple(entries)


def _parse_entries(raw: Any, field: str, relative_path: str) -> list[ManifestEntry]:
    if raw is None:
        return []
    if not isinstance(raw, list):
        raise ArchiveCorrupt(f"manifest {relative_path} field {field} must be an array")
    entries: list[ManifestEntry] = []
    for index, item in enumerate(raw, start=1):
        if not isinstance(item, dict):
            raise ArchiveCorrupt(f"manifest {relative_path} {field}[{index}] must be an object")
        path = item.get("path")
        digest = item.get("sha256")
        size = item.get("size_bytes")
        if not isinstance(path, str) or not path:
            raise ArchiveCorrupt(f"manifest {relative_path} {field}[{index}].path must be non-empty")
        if not isinstance(digest, str) or len(digest) != 64:
            raise ArchiveCorrupt(f"manifest {relative_path} {field}[{index}].sha256 must be a sha256 hex string")
        if not isinstance(size, int) or isinstance(size, bool) or size < 0:
            raise ArchiveCorrupt(f"manifest {relative_path} {field}[{index}].size_bytes must be >= 0")
        entries.append(ManifestEntry(path=path, sha256=digest, size_bytes=size))
    return entries


def _write_manifest_replace(
    store: ArchiveStore,
    relative_path: str,
    payload: dict[str, Any],
) -> ArchiveWrite:
    target = store.root / relative_path
    target.parent.mkdir(parents=True, exist_ok=True)
    data = canonical_json_bytes(payload)
    tmp = target.with_name(f".{target.name}.tmp")
    tmp.write_bytes(data)
    try:
        tmp.replace(target)
    except Exception:
        if tmp.exists():
            tmp.unlink()
        raise
    return ArchiveWrite(relative_path=relative_path, sha256=sha256_bytes(data), size_bytes=len(data))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Maintain Paro performance archive manifests")
    subparsers = parser.add_subparsers(dest="action", required=True)

    rebuild = subparsers.add_parser("rebuild", help="Rebuild manifest from archive listing")
    rebuild.add_argument("--archive", type=Path, required=True)
    rebuild.add_argument("--cache", type=Path)
    rebuild.add_argument("--gate", required=True)
    rebuild.add_argument("--platform", default="auto")
    rebuild.add_argument("--policy", type=Path)
    rebuild.add_argument("--policy-version", default="auto")

    args = parser.parse_args(argv)
    cache = args.cache if args.cache is not None else args.archive / ".cache"
    store = ArchiveStore(root=args.archive, cache_root=cache)
    if args.action == "rebuild":
        platform = platform_key() if args.platform == "auto" else args.platform
        policy_version = _resolve_policy_version(args.gate, args.policy, args.policy_version)
        write = rebuild_manifest(
            store,
            gate=args.gate,
            platform=platform,
            policy_version=policy_version,
        )
        print(f"manifest rebuilt: {write.relative_path} sha256={write.sha256}")
        return 0
    return 2


def _resolve_policy_version(gate: str, policy_path: Path | None, value: str) -> int:
    if value != "auto":
        try:
            version = int(value)
        except ValueError as exc:
            raise SystemExit(f"--policy-version must be an integer or auto: {value}") from exc
        if version < 1:
            raise SystemExit("--policy-version must be >= 1")
        return version
    policy = load_policy(policy_path if policy_path is not None else Path("policies") / f"{gate}.toml")
    return policy.version


if __name__ == "__main__":
    raise SystemExit(main())
