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
# Terminal metadata remains writable after payload capacity is exhausted so a
# cancelled/failed attempt can be sealed.  It is still bounded and accounted
# for; the emergency path is not an unlimited escape hatch from the campaign
# contract.
CONTROL_OUTPUT_LIMIT_BYTES = 256 * 1024
MANIFEST_LIMIT_BYTES = CONTROL_OUTPUT_LIMIT_BYTES
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


def _encode_json(payload: dict[str, Any]) -> bytes:
    """Encode one owned JSON record exactly as it will be published."""
    return (json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True) + "\n").encode(
        "utf-8"
    )


def _write_bounded_json(
    path: Path,
    payload: dict[str, Any],
    *,
    limit_bytes: int,
    overwrite: bool,
) -> int:
    """Publish JSON only after enforcing the encoded UTF-8 record limit."""
    encoded = _encode_json(payload)
    if len(encoded) > limit_bytes:
        raise RunOutputError(
            f"JSON record capacity exceeded: {len(encoded)} > {limit_bytes} bytes: {path}"
        )
    _write_encoded_atomically(path, encoded, overwrite=overwrite)
    return len(encoded)


@dataclass(frozen=True)
class AttemptOutput:
    """Exclusive filesystem owner for one source invocation."""

    run: "RunOutput"
    source_id: str
    attempt_id: str
    query_case: str
    arm_id: str
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
        current = json.loads((self.root / "attempt.json").read_text(encoding="utf-8"))
        if current.get("status") != "Running":
            raise RunOutputError(
                f"attempt {self.attempt_id} is already terminal: {current.get('status')}"
            )
        writer = (
            self.run.write_control_json
            if self.run.capacity_exceeded
            else self.run.write_json
        )
        error = _bounded_error_text(error)
        writer(
            self.failure_path,
            {
                "schema_version": RUN_SUMMARY_SCHEMA_VERSION,
                "run_id": self.run.run_id,
                "campaign_id": self.run.campaign_id,
                "source_id": self.source_id,
                "attempt_id": self.attempt_id,
                "query_case": self.query_case,
                "arm_id": self.arm_id,
                "status": status,
                "error": error,
            },
            overwrite=False,
        )
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
        current = json.loads((self.root / "attempt.json").read_text(encoding="utf-8"))
        if current.get("status") != "Running":
            raise RunOutputError(
                f"attempt {self.attempt_id} is already terminal: {current.get('status')}"
            )
        metadata = {
            "schema_version": RUN_SUMMARY_SCHEMA_VERSION,
            "run_id": self.run.run_id,
            "campaign_id": self.run.campaign_id,
            "source_id": self.source_id,
            "attempt_id": self.attempt_id,
            "query_case": self.query_case,
            "arm_id": self.arm_id,
            "status": status,
            "result": _relative_or_none(result_path, self.run.root),
            "summary": _relative_or_none(summary_path, self.run.root),
            "failure": _relative_or_none(failure_path, self.run.root),
            "sealed_at": _now(),
        }
        # The running metadata is the one owned state machine record that may
        # be replaced: seal() has already verified the current state and the
        # replacement is still charged through the run writer.
        writer = (
            self.run.write_control_json
            if self.run.capacity_exceeded
            else self.run.write_json
        )
        writer(self.root / "attempt.json", metadata, overwrite=True)
        self.run._update_attempt(metadata)


@dataclass
class RunOutput:
    """The sole owner of a command's report root and attempt registry."""

    report_root: Path
    campaign_id: str
    run_id: str
    root: Path
    _manifest: dict[str, Any]

    @classmethod
    def create(
        cls,
        report_root: Path,
        *,
        run_id: str | None = None,
        campaign_id: str | None = None,
    ) -> "RunOutput":
        report_root = report_root.resolve()
        requested = _generated_run_id() if run_id in (None, "", "auto") else validate_output_id(run_id, label="run id")
        campaign = (
            _generated_campaign_id()
            if campaign_id in (None, "", "auto")
            else validate_output_id(campaign_id, label="campaign id")
        )
        report_root.mkdir(parents=True, exist_ok=True)
        root = report_root / requested
        try:
            root.mkdir()
        except FileExistsError as exc:
            raise RunOutputError(f"run id already exists; refusing to resume or overwrite: {requested}") from exc
        manifest: dict[str, Any] = {
            "schema_version": RUN_OUTPUT_SCHEMA_VERSION,
            "campaign_id": campaign,
            "run_id": requested,
            "status": "Running",
            "created_at": _now(),
            "sealed_at": None,
            "attempts": [],
            "registration": {
                "schema_version": 1,
                "cells": [],
                "budget_bytes": 0,
                "written_bytes": 0,
                "control_written_bytes": 0,
                "control_limit_bytes": CONTROL_OUTPUT_LIMIT_BYTES,
                "manifest_bytes": 0,
                "manifest_limit_bytes": MANIFEST_LIMIT_BYTES,
                "total_limit_bytes": CAMPAIGN_TOTAL_LIMIT_BYTES,
                "status": "Unregistered",
            },
        }
        _write_bounded_json(
            root / "manifest.json",
            manifest,
            limit_bytes=MANIFEST_LIMIT_BYTES,
            overwrite=False,
        )
        return cls(
            report_root=report_root,
            campaign_id=campaign,
            run_id=requested,
            root=root,
            _manifest=manifest,
        )

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

    def begin_attempt(
        self,
        source_id: str,
        *,
        query_case: str | None = None,
        arm_id: str = "default",
    ) -> AttemptOutput:
        source_id = validate_output_id(source_id, label="source id")
        query_case = validate_output_id(query_case or source_id, label="query case")
        arm_id = validate_output_id(arm_id, label="arm id")
        attempts_root = self.root / "sources" / source_id / "attempts"
        attempts_root.mkdir(parents=True, exist_ok=True)
        for ordinal in range(1, 100_000):
            attempt_id = f"attempt-{ordinal:04d}"
            attempt_root = attempts_root / attempt_id
            try:
                attempt_root.mkdir()
            except FileExistsError:
                continue
            attempt = AttemptOutput(
                self, source_id, attempt_id, query_case, arm_id, attempt_root
            )
            metadata = {
                "schema_version": RUN_SUMMARY_SCHEMA_VERSION,
                "campaign_id": self.campaign_id,
                "run_id": self.run_id,
                "source_id": source_id,
                "attempt_id": attempt_id,
                "query_case": query_case,
                "arm_id": arm_id,
                "status": "Running",
                "result": None,
                "summary": None,
                "failure": None,
                "started_at": _now(),
            }
            self.write_json(attempt_root / "attempt.json", metadata, overwrite=False)
            self._update_attempt(metadata)
            return attempt
        raise RunOutputError(f"too many attempts for source {source_id!r}")

    def finalize(self, *, status: str) -> None:
        if status not in {"Completed", "Failed", "Cancelled", "Incomplete"}:
            raise RunOutputError(f"invalid run status: {status}")
        current = self._manifest.get("status")
        if current != "Running":
            if current != status:
                raise RunOutputError(f"run is already terminal: {current}")
            return
        if status == "Completed" and any(
            attempt.get("status") != "Completed" for attempt in self._manifest.get("attempts", [])
        ):
            raise RunOutputError("cannot complete a run with failed, cancelled, or incomplete attempts")
        if status == "Completed" and self._manifest.get("registration", {}).get(
            "status"
        ) == "CapacityExceeded":
            raise RunOutputError("cannot complete a run after campaign capacity was exceeded")
        self._manifest["status"] = status
        self._manifest["sealed_at"] = _now()
        self._persist_manifest()

    @property
    def capacity_exceeded(self) -> bool:
        return self._manifest.get("registration", {}).get("status") == "CapacityExceeded"

    def write_control_json(
        self, path: Path, payload: dict[str, Any], *, overwrite: bool = False
    ) -> None:
        """Write minimal terminal metadata after payload capacity is exhausted.

        This path is intentionally not a second evidence writer: it is only
        available for the state-machine metadata needed to explain why the
        run stopped.  Once payload capacity is exceeded no new result,
        summary, receipt, or trace may use it.
        """
        path = self.owned_path(path)
        encoded = (json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True) + "\n").encode("utf-8")
        self._charge_control_bytes(len(encoded))
        _write_encoded_atomically(path, encoded, overwrite=overwrite)
        self._persist_manifest()

    def write_control_text(self, path: Path, text: str, *, overwrite: bool = False) -> None:
        """Write only terminal explanation metadata after capacity refusal."""
        path = self.owned_path(path)
        encoded = text.encode("utf-8")
        self._charge_control_bytes(len(encoded))
        _write_encoded_atomically(path, encoded, overwrite=overwrite)
        self._persist_manifest()

    def register_cell(
        self,
        *,
        cell_id: str,
        query_cases: int,
        sample_rows: int,
        product_receipts: int,
        calibration_rows: int = 0,
        summary_captures: int = 0,
        query_case: str | None = None,
        arm_id: str = "default",
    ) -> None:
        """Freeze one finite campaign-cell budget before its source runs."""
        cell_id = validate_output_id(cell_id, label="cell id")
        query_case = validate_output_id(query_case or cell_id, label="query case")
        arm_id = validate_output_id(arm_id, label="arm id")
        if min(query_cases, sample_rows, product_receipts, calibration_rows, summary_captures) < 0:
            raise RunOutputError("campaign registration counts must be >= 0")
        registration = self._manifest["registration"]
        expected_cell_id = f"{query_case}--{arm_id}"
        if cell_id != expected_cell_id:
            raise RunOutputError(
                f"cell id must be query_case--arm_id ({expected_cell_id}), got {cell_id}"
            )
        for current in registration["cells"]:
            if current.get("cell_id") != cell_id:
                continue
            requested = {
                "query_cases": query_cases,
                "sample_rows": sample_rows,
                "product_receipts": product_receipts,
                "calibration_rows": calibration_rows,
                "summary_captures": summary_captures,
                "query_case": query_case,
                "arm_id": arm_id,
            }
            if all(current.get(key) == value for key, value in requested.items()):
                return
            raise RunOutputError(
                f"campaign cell already registered with different contract: {cell_id}"
            )
        cells = [*registration["cells"], {
            "cell_id": cell_id,
            "query_case": query_case,
            "arm_id": arm_id,
            "query_cases": query_cases,
            "sample_rows": sample_rows,
            "product_receipts": product_receipts,
            "calibration_rows": calibration_rows,
            "summary_captures": summary_captures,
        }]
        arms = len({cell["arm_id"] for cell in cells})
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
        self._persist_manifest()

    def _charge_bytes(self, encoded_bytes: int) -> None:
        registration = self._manifest["registration"]
        total = int(registration.get("written_bytes", 0)) + encoded_bytes
        limit = int(registration.get("total_limit_bytes", CAMPAIGN_TOTAL_LIMIT_BYTES))
        if total > limit:
            registration["status"] = "CapacityExceeded"
            registration["written_bytes"] = total
            self._manifest["status"] = "Incomplete"
            self._manifest["sealed_at"] = _now()
            self._persist_manifest()
            raise RunOutputError(
                f"campaign output capacity exceeded: {total} > {limit} bytes"
            )
        registration["written_bytes"] = total

    def _charge_control_bytes(self, encoded_bytes: int) -> None:
        registration = self._manifest["registration"]
        total = int(registration.get("control_written_bytes", 0)) + encoded_bytes
        limit = int(registration.get("control_limit_bytes", CONTROL_OUTPUT_LIMIT_BYTES))
        if total > limit:
            raise RunOutputError(
                f"terminal metadata capacity exceeded: {total} > {limit} bytes"
            )
        registration["control_written_bytes"] = total

    def _persist_manifest(self) -> None:
        registration = self._manifest.setdefault("registration", {})
        limit = int(registration.get("manifest_limit_bytes", MANIFEST_LIMIT_BYTES))
        registration["manifest_bytes"] = 0
        encoded = _encode_json(self._manifest)
        if len(encoded) > limit:
            raise RunOutputError(
                f"manifest capacity exceeded: {len(encoded)} > {limit} bytes"
            )
        registration["manifest_bytes"] = len(encoded)
        encoded = _encode_json(self._manifest)
        if len(encoded) > limit:
            raise RunOutputError(
                f"manifest capacity exceeded after accounting: {len(encoded)} > {limit} bytes"
            )
        _write_encoded_atomically(self.root / "manifest.json", encoded, overwrite=True)

    def write_json(self, path: Path, payload: dict[str, Any], *, overwrite: bool = False) -> None:
        path = self.owned_path(path)
        encoded = _encode_json(payload)
        self._charge_bytes(len(encoded))
        _write_encoded_atomically(path, encoded, overwrite=overwrite)

    def write_text(self, path: Path, text: str, *, overwrite: bool = False) -> None:
        path = self.owned_path(path)
        encoded = text.encode("utf-8")
        self._charge_bytes(len(encoded))
        _write_encoded_atomically(path, encoded, overwrite=overwrite)

    def _update_attempt(self, metadata: dict[str, Any]) -> None:
        attempts = self._manifest.setdefault("attempts", [])
        identity = (metadata.get("source_id"), metadata.get("attempt_id"))
        for index, current in enumerate(attempts):
            if (current.get("source_id"), current.get("attempt_id")) == identity:
                attempts[index] = dict(metadata)
                break
        else:
            attempts.append(dict(metadata))
        self._persist_manifest()


def _generated_run_id() -> str:
    stamp = datetime.now(timezone.utc).strftime("run-%Y%m%dT%H%M%S%fZ")
    return f"{stamp}-{os.getpid()}-{uuid.uuid4().hex[:10]}"


def _generated_campaign_id() -> str:
    stamp = datetime.now(timezone.utc).strftime("campaign-%Y%m%dT%H%M%S%fZ")
    return f"{stamp}-{os.getpid()}-{uuid.uuid4().hex[:10]}"


def _relative_or_none(path: Path | None, root: Path) -> str | None:
    if path is None:
        return None
    try:
        return path.resolve().relative_to(root.resolve()).as_posix()
    except ValueError as exc:
        raise RunOutputError(f"owned output is outside run root: {path}") from exc


def _write_encoded_atomically(path: Path, encoded: bytes, *, overwrite: bool) -> None:
    """Publish already-accounted bytes without creating an untracked writer."""
    path.parent.mkdir(parents=True, exist_ok=True)
    if not overwrite and path.exists():
        raise RunOutputError(f"refusing to overwrite owned output: {path}")
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
    temporary.write_bytes(encoded)
    try:
        if not overwrite and path.exists():
            raise RunOutputError(f"refusing to overwrite owned output: {path}")
        temporary.replace(path)
    finally:
        if temporary.exists():
            temporary.unlink()


def _bounded_error_text(error: str, limit: int = 16 * 1024) -> str:
    """Keep terminal failure metadata within the emergency writer budget."""
    text = str(error)
    encoded = text.encode("utf-8")
    if len(encoded) <= limit:
        return text
    marker = "\n[error truncated by bounded run-output contract]"
    room = max(0, limit - len(marker.encode("utf-8")))
    return encoded[:room].decode("utf-8", errors="ignore") + marker
