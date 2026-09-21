# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

"""Owned, append-only output for one benchmark command.

The benchmark used to let each source invent a path below ``report/``.  That
made a retry, a second gate invocation, or two source adapters race on the
same result.json.  This module is deliberately small: it owns only run and
attempt identity and the filesystem boundary.  It is not a runner or a
second evidence store.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import uuid
from typing import Any


RUN_OUTPUT_SCHEMA_VERSION = 1
RUN_SUMMARY_SCHEMA_VERSION = 1
CAMPAIGN_TOTAL_LIMIT_BYTES = 64 * 1024 * 1024
SUMMARY_LIMIT_BYTES = 200_000
_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,95}$")


class RunOutputError(ValueError):
    """The requested output identity cannot be safely allocated."""


def validate_output_id(value: str, *, label: str) -> str:
    if not isinstance(value, str) or not _ID_RE.fullmatch(value):
        raise RunOutputError(
            f"{label} must be one path segment containing only letters, digits, '.', '_' or '-': {value!r}"
        )
    return value


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def atomic_write_json(path: Path, payload: dict[str, Any], *, overwrite: bool = False) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not overwrite and path.exists():
        raise RunOutputError(f"refusing to overwrite owned output: {path}")
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
    temporary.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    try:
        if not overwrite and path.exists():
            raise RunOutputError(f"refusing to overwrite owned output: {path}")
        temporary.replace(path)
    finally:
        if temporary.exists():
            temporary.unlink()


def atomic_write_text(path: Path, text: str, *, overwrite: bool = True) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not overwrite and path.exists():
        raise RunOutputError(f"refusing to overwrite owned output: {path}")
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
    temporary.write_text(text, encoding="utf-8")
    try:
        if not overwrite and path.exists():
            raise RunOutputError(f"refusing to overwrite owned output: {path}")
        temporary.replace(path)
    finally:
        if temporary.exists():
            temporary.unlink()


# Kept private at the call sites by convention, but expose one named helper
# for reporters and adapters so every JSON artifact uses the same atomic,
# no-overwrite boundary.
_atomic_write_json = atomic_write_json


@dataclass(frozen=True)
class AttemptOutput:
    """Exclusive filesystem owner for one source invocation."""

    run: "RunOutput"
    source_id: str
    attempt_id: str
    root: Path

    @property
    def result_path(self) -> Path:
        return self.root / "result.json"

    @property
    def summary_path(self) -> Path:
        return self.root / "summary.md"

    @property
    def failure_path(self) -> Path:
        return self.root / "failure.json"

    def write_failure(self, *, status: str, error: str) -> Path:
        if status not in {"Failed", "Cancelled", "Incomplete"}:
            raise RunOutputError(f"invalid failed attempt status: {status}")
        _atomic_write_json(
            self.failure_path,
            {
                "schema_version": RUN_SUMMARY_SCHEMA_VERSION,
                "run_id": self.run.run_id,
                "source_id": self.source_id,
                "attempt_id": self.attempt_id,
                "status": status,
                "error": error,
            },
            overwrite=False,
        )
        self.seal(status=status, failure_path=self.failure_path)
        return self.failure_path

    def seal(
        self,
        *,
        status: str,
        result_path: Path | None = None,
        summary_path: Path | None = None,
        failure_path: Path | None = None,
    ) -> None:
        if status not in {"Completed", "Failed", "Cancelled", "Incomplete"}:
            raise RunOutputError(f"invalid attempt status: {status}")
        metadata = {
            "schema_version": RUN_SUMMARY_SCHEMA_VERSION,
            "run_id": self.run.run_id,
            "source_id": self.source_id,
            "attempt_id": self.attempt_id,
            "status": status,
            "result": _relative_or_none(result_path, self.run.root),
            "summary": _relative_or_none(summary_path, self.run.root),
            "failure": _relative_or_none(failure_path, self.run.root),
            "sealed_at": _now(),
        }
        _atomic_write_json(self.root / "attempt.json", metadata, overwrite=True)
        self.run._update_attempt(metadata)


@dataclass
class RunOutput:
    """The sole owner of a command's report root and attempt registry."""

    report_root: Path
    run_id: str
    root: Path
    _manifest: dict[str, Any]

    @classmethod
    def create(cls, report_root: Path, *, run_id: str | None = None) -> "RunOutput":
        report_root = report_root.resolve()
        requested = _generated_run_id() if run_id in (None, "", "auto") else validate_output_id(run_id, label="run id")
        report_root.mkdir(parents=True, exist_ok=True)
        root = report_root / requested
        try:
            root.mkdir()
        except FileExistsError as exc:
            raise RunOutputError(f"run id already exists; refusing to resume or overwrite: {requested}") from exc
        manifest: dict[str, Any] = {
            "schema_version": RUN_OUTPUT_SCHEMA_VERSION,
            "run_id": requested,
            "status": "Running",
            "created_at": _now(),
            "sealed_at": None,
            "attempts": [],
            "registration": {
                "schema_version": 1,
                "cells": [],
                "budget_bytes": 0,
                "total_limit_bytes": CAMPAIGN_TOTAL_LIMIT_BYTES,
                "status": "Unregistered",
            },
        }
        _atomic_write_json(root / "manifest.json", manifest, overwrite=False)
        return cls(report_root=report_root, run_id=requested, root=root, _manifest=manifest)

    @property
    def gate_path(self) -> Path:
        return self.root / "gate.json"

    def owned_path(self, path: Path) -> Path:
        """Return a path only when it stays inside this run's ownership tree."""
        resolved = path.resolve()
        try:
            resolved.relative_to(self.root.resolve())
        except ValueError as exc:
            raise RunOutputError(f"output is outside owned run root: {path}") from exc
        return resolved

    def begin_attempt(self, source_id: str) -> AttemptOutput:
        source_id = validate_output_id(source_id, label="source id")
        attempts_root = self.root / "sources" / source_id / "attempts"
        attempts_root.mkdir(parents=True, exist_ok=True)
        for ordinal in range(1, 100_000):
            attempt_id = f"attempt-{ordinal:04d}"
            attempt_root = attempts_root / attempt_id
            try:
                attempt_root.mkdir()
            except FileExistsError:
                continue
            attempt = AttemptOutput(self, source_id, attempt_id, attempt_root)
            metadata = {
                "schema_version": RUN_SUMMARY_SCHEMA_VERSION,
                "run_id": self.run_id,
                "source_id": source_id,
                "attempt_id": attempt_id,
                "status": "Running",
                "result": None,
                "summary": None,
                "failure": None,
                "started_at": _now(),
            }
            _atomic_write_json(attempt_root / "attempt.json", metadata, overwrite=False)
            self._update_attempt(metadata)
            return attempt
        raise RunOutputError(f"too many attempts for source {source_id!r}")

    def finalize(self, *, status: str) -> None:
        if status not in {"Completed", "Failed", "Cancelled", "Incomplete"}:
            raise RunOutputError(f"invalid run status: {status}")
        self._manifest["status"] = status
        self._manifest["sealed_at"] = _now()
        _atomic_write_json(self.root / "manifest.json", self._manifest, overwrite=True)

    def register_cell(
        self,
        *,
        cell_id: str,
        query_cases: int,
        sample_rows: int,
        product_receipts: int,
        calibration_rows: int = 0,
        summary_captures: int = 0,
    ) -> None:
        """Freeze one finite campaign-cell budget before its source runs."""
        cell_id = validate_output_id(cell_id, label="cell id")
        if min(query_cases, sample_rows, product_receipts, calibration_rows, summary_captures) < 0:
            raise RunOutputError("campaign registration counts must be >= 0")
        registration = self._manifest["registration"]
        if any(cell.get("cell_id") == cell_id for cell in registration["cells"]):
            raise RunOutputError(f"campaign cell already registered: {cell_id}")
        cells = [*registration["cells"], {
            "cell_id": cell_id,
            "query_cases": query_cases,
            "sample_rows": sample_rows,
            "product_receipts": product_receipts,
            "calibration_rows": calibration_rows,
            "summary_captures": summary_captures,
        }]
        arms = 1
        queries = sum(cell["query_cases"] for cell in cells)
        captures = sum(cell["summary_captures"] for cell in cells)
        manifest_bytes = 32_000 + 1_024 * (arms + queries + len(cells) + captures)
        cells_bytes = sum(
            4_096
            + 1_024 * cell["sample_rows"]
            + 2_048 * cell["product_receipts"]
            + 512 * cell["calibration_rows"]
            for cell in cells
        )
        budget = manifest_bytes + 20_000 + cells_bytes + 200_000 * captures
        if budget > CAMPAIGN_TOTAL_LIMIT_BYTES:
            raise RunOutputError(
                f"campaign registration exceeds {CAMPAIGN_TOTAL_LIMIT_BYTES} bytes: {budget}"
            )
        registration["cells"] = cells
        registration["budget_bytes"] = budget
        registration["status"] = "WithinBudget"
        _atomic_write_json(self.root / "manifest.json", self._manifest, overwrite=True)

    def _update_attempt(self, metadata: dict[str, Any]) -> None:
        attempts = self._manifest.setdefault("attempts", [])
        identity = (metadata.get("source_id"), metadata.get("attempt_id"))
        for index, current in enumerate(attempts):
            if (current.get("source_id"), current.get("attempt_id")) == identity:
                attempts[index] = dict(metadata)
                break
        else:
            attempts.append(dict(metadata))
        _atomic_write_json(self.root / "manifest.json", self._manifest, overwrite=True)


def _generated_run_id() -> str:
    stamp = datetime.now(timezone.utc).strftime("run-%Y%m%dT%H%M%S%fZ")
    return f"{stamp}-{os.getpid()}-{uuid.uuid4().hex[:10]}"


def _relative_or_none(path: Path | None, root: Path) -> str | None:
    if path is None:
        return None
    try:
        return path.resolve().relative_to(root.resolve()).as_posix()
    except ValueError as exc:
        raise RunOutputError(f"owned output is outside run root: {path}") from exc
