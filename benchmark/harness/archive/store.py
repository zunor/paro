# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

"""Append-only performance archive storage."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
from typing import Any


DEFAULT_CACHE_TTL_SECONDS = 72 * 60 * 60


class ArchiveError(ValueError):
    """Raised when the performance archive cannot satisfy its contract."""


class ArchiveUnavailable(ArchiveError):
    """Raised when the primary archive is unavailable."""


class ArchiveMissing(ArchiveError):
    """Raised when an expected archive object does not exist."""


class ArchiveCorrupt(ArchiveError):
    """Raised when an archive object fails checksum or schema validation."""


@dataclass(frozen=True)
class ArchiveRead:
    relative_path: str
    payload: dict[str, Any]
    sha256: str
    cache_hit: bool


@dataclass(frozen=True)
class ArchiveWrite:
    relative_path: str
    sha256: str
    size_bytes: int


@dataclass(frozen=True)
class RetentionCandidate:
    relative_path: str
    age_days: int


@dataclass(frozen=True)
class ArchiveStore:
    root: Path
    cache_root: Path
    cache_ttl_seconds: int = DEFAULT_CACHE_TTL_SECONDS

    @classmethod
    def from_env(cls, benchmark_root: Path) -> "ArchiveStore":
        workspace_root = benchmark_root.parent
        archive_env = os.getenv("PARO_PERF_ARCHIVE_DIR")
        cache_env = os.getenv("PARO_PERF_ARCHIVE_CACHE_DIR")
        root = Path(archive_env) if archive_env else workspace_root / ".ci" / "paro-perf-archive"
        cache_root = Path(cache_env) if cache_env else workspace_root / ".ci" / "perf-archive-cache"
        ttl = _env_int("PARO_PERF_ARCHIVE_CACHE_TTL_SECONDS", DEFAULT_CACHE_TTL_SECONDS)
        return cls(root=root, cache_root=cache_root, cache_ttl_seconds=ttl)

    def write_json_append_only(self, relative_path: str, payload: dict[str, Any]) -> ArchiveWrite:
        normalized = normalize_relative_path(relative_path)
        target = self.root / normalized
        if target.exists():
            raise ArchiveError(f"archive object already exists: {normalized}")
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
        digest = sha256_bytes(data)
        return ArchiveWrite(
            relative_path=normalized,
            sha256=digest,
            size_bytes=len(data),
        )

    def read_json_with_cache(
        self,
        relative_path: str,
        *,
        expected_sha256: str | None = None,
    ) -> ArchiveRead:
        normalized = normalize_relative_path(relative_path)
        primary_error: ArchiveError | None = None
        try:
            data = self._read_primary_bytes(normalized)
            digest = sha256_bytes(data)
            _validate_expected_sha(normalized, digest, expected_sha256)
            payload = _decode_json(normalized, data)
            self._write_cache_bytes(normalized, data)
            return ArchiveRead(normalized, payload, digest, cache_hit=False)
        except ArchiveUnavailable as exc:
            primary_error = exc
        except (ArchiveCorrupt, ArchiveMissing):
            raise

        try:
            data = self._read_cache_bytes(normalized)
            digest = sha256_bytes(data)
            _validate_expected_sha(normalized, digest, expected_sha256)
            return ArchiveRead(normalized, _decode_json(normalized, data), digest, cache_hit=True)
        except ArchiveError:
            if primary_error is not None:
                raise primary_error
            raise

    def list_json(self, prefix: str) -> list[str]:
        normalized = normalize_prefix(prefix)
        base = self.root / normalized
        if not self.root.exists():
            raise ArchiveUnavailable(f"archive root is unavailable: {self.root}")
        if not base.exists():
            return []
        return sorted(
            path.relative_to(self.root).as_posix()
            for path in base.rglob("*.json")
            if path.is_file()
        )

    def retention_candidates(
        self,
        prefix: str,
        *,
        older_than_days: int,
        now: datetime | None = None,
    ) -> list[RetentionCandidate]:
        active_now = datetime.now(timezone.utc) if now is None else now.astimezone(timezone.utc)
        cutoff = active_now - timedelta(days=older_than_days)
        candidates: list[RetentionCandidate] = []
        for relative_path in self.list_json(prefix):
            path = self.root / relative_path
            modified = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
            if modified >= cutoff:
                continue
            age_days = max(0, (active_now - modified).days)
            candidates.append(RetentionCandidate(relative_path=relative_path, age_days=age_days))
        return candidates

    def _read_primary_bytes(self, relative_path: str) -> bytes:
        if not self.root.exists():
            raise ArchiveUnavailable(f"archive root is unavailable: {self.root}")
        path = self.root / relative_path
        if not path.exists():
            raise ArchiveMissing(f"archive object not found: {relative_path}")
        return path.read_bytes()

    def _read_cache_bytes(self, relative_path: str) -> bytes:
        path = self.cache_root / relative_path
        if not path.exists():
            raise ArchiveMissing(f"archive cache object not found: {relative_path}")
        modified = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
        age = datetime.now(timezone.utc) - modified
        if age.total_seconds() > self.cache_ttl_seconds:
            raise ArchiveUnavailable(f"archive cache object expired: {relative_path}")
        return path.read_bytes()

    def _write_cache_bytes(self, relative_path: str, data: bytes) -> None:
        path = self.cache_root / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)


def canonical_json_bytes(payload: dict[str, Any]) -> bytes:
    return (json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n").encode("utf-8")


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def normalize_relative_path(path: str) -> str:
    if not path:
        raise ArchiveError("archive path must not be empty")
    normalized = Path(path)
    if normalized.is_absolute() or ".." in normalized.parts:
        raise ArchiveError(f"archive path must be relative and stay inside archive root: {path}")
    return normalized.as_posix()


def normalize_prefix(prefix: str) -> str:
    normalized = normalize_relative_path(prefix)
    return normalized.rstrip("/")


def _decode_json(relative_path: str, data: bytes) -> dict[str, Any]:
    try:
        payload = json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ArchiveCorrupt(f"archive object is not valid JSON: {relative_path}") from exc
    if not isinstance(payload, dict):
        raise ArchiveCorrupt(f"archive object must be a JSON object: {relative_path}")
    return payload


def _validate_expected_sha(relative_path: str, digest: str, expected_sha256: str | None) -> None:
    if expected_sha256 is not None and digest != expected_sha256:
        raise ArchiveCorrupt(
            f"archive checksum mismatch for {relative_path}: expected {expected_sha256}, got {digest}"
        )


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise ArchiveError(f"{name} must be an integer") from exc
    if value < 0:
        raise ArchiveError(f"{name} must be >= 0")
    return value
