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

from dataclasses import dataclass, field as dataclass_field
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import threading
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


def _encode_json(payload: dict[str, Any], *, limit_bytes: int | None = None) -> bytes:
    """Encode one owned JSON record without first building an unbounded string.

    ``JSONEncoder.iterencode`` is important here: a rejected capture must not
    allocate the complete diagnostic document just to discover that the
    registered byte lease has already been exhausted.
    """
    encoder = json.JSONEncoder(indent=2, ensure_ascii=False, sort_keys=True)
    output = bytearray()
    for chunk in encoder.iterencode(payload):
        encoded = chunk.encode("utf-8")
        if limit_bytes is not None and len(output) + len(encoded) + 1 > limit_bytes:
            raise RunOutputError(
                f"JSON record capacity exceeded while encoding: "
                f"> {limit_bytes} bytes"
            )
        output.extend(encoded)
    output.extend(b"\n")
    return bytes(output)


def _write_bounded_json(
    path: Path,
    payload: dict[str, Any],
    *,
    limit_bytes: int,
    overwrite: bool,
) -> int:
    """Publish JSON only after enforcing the encoded UTF-8 record limit."""
    encoded = _encode_json(payload, limit_bytes=limit_bytes)
    if len(encoded) > limit_bytes:
        raise RunOutputError(
            f"JSON record capacity exceeded: {len(encoded)} > {limit_bytes} bytes: {path}"
        )
    _write_encoded_atomically(path, encoded, overwrite=overwrite)
    return len(encoded)


@dataclass(frozen=True)
class PreparedWrite:
    """An encoded, capacity-checked write waiting for publication."""

    writer: Any
    path: Path
    encoded: bytes
    overwrite: bool
    replacing_bytes: int

    def publish(self) -> Path:
        return self.writer._publish(self)


@dataclass(frozen=True)
class CampaignRegistration:
    """Explicit registration authority for one bounded campaign."""

    run: "RunOutput"

    def cell(
        self,
        *,
        query_case: str,
        arm_id: str,
        query_cases: int,
        sample_rows: int,
        product_receipts: int,
        calibration_rows: int = 0,
        summary_captures: int = 0,
    ) -> "CellWriter":
        return self.run.register_cell(
            cell_id=f"{query_case}--{arm_id}",
            query_cases=query_cases,
            sample_rows=sample_rows,
            product_receipts=product_receipts,
            calibration_rows=calibration_rows,
            summary_captures=summary_captures,
            query_case=query_case,
            arm_id=arm_id,
        )

    def seal(self) -> None:
        self.run.seal_registration()


@dataclass(frozen=True)
class ControlWriter:
    """Writer for bounded lifecycle metadata, never evidence payloads."""

    run: "RunOutput"
    root: Path

    def _path(self, name: str | Path) -> Path:
        return _owned_relative_path(self.root, name, label="control output")

    def prepare_json(
        self, name: str | Path, payload: dict[str, Any], *, overwrite: bool = False
    ) -> PreparedWrite:
        path = self._path(name)
        encoded = _encode_json(payload, limit_bytes=CONTROL_OUTPUT_LIMIT_BYTES)
        replacing = path.stat().st_size if overwrite and path.exists() else 0
        return PreparedWrite(self, path, encoded, overwrite, replacing)

    def prepare_text(
        self, name: str | Path, text: str, *, overwrite: bool = False
    ) -> PreparedWrite:
        path = self._path(name)
        encoded = text.encode("utf-8")
        if len(encoded) > CONTROL_OUTPUT_LIMIT_BYTES:
            raise RunOutputError(
                f"control output capacity exceeded: {len(encoded)} > "
                f"{CONTROL_OUTPUT_LIMIT_BYTES} bytes"
            )
        replacing = path.stat().st_size if overwrite and path.exists() else 0
        return PreparedWrite(self, path, encoded, overwrite, replacing)

    def write_json(
        self, name: str | Path, payload: dict[str, Any], *, overwrite: bool = False
    ) -> Path:
        return self.prepare_json(name, payload, overwrite=overwrite).publish()

    def write_text(self, name: str | Path, text: str, *, overwrite: bool = False) -> Path:
        return self.prepare_text(name, text, overwrite=overwrite).publish()

    def _publish(self, prepared: PreparedWrite) -> Path:
        with self.run._lock:
            try:
                self.run._commit_control(prepared)
                return prepared.path
            except RunOutputError as exc:
                if not self.run.capacity_exceeded:
                    self.run._mark_publication_unknown(prepared.path, exc)
                raise
            except Exception as exc:
                self.run._mark_publication_unknown(prepared.path, exc)
                raise


@dataclass(frozen=True)
class CellWriter:
    """Writer bound to one registered QueryCase×ArmId cell and attempt."""

    run: "RunOutput"
    cell_id: str
    root: Path

    def _path(self, name: str | Path) -> Path:
        return _owned_relative_path(self.root, name, label="cell output")

    def prepare_json(
        self, name: str | Path, payload: dict[str, Any], *, overwrite: bool = False
    ) -> PreparedWrite:
        path = self._path(name)
        replacing = path.stat().st_size if overwrite and path.exists() else 0
        limit = self.run._available_bytes_for_cell(
            self.cell_id, replacing_bytes=replacing
        )
        try:
            encoded = _encode_json(payload, limit_bytes=limit)
        except RunOutputError:
            self.run._mark_capacity_exceeded()
            raise
        return PreparedWrite(self, path, encoded, overwrite, replacing)

    def prepare_text(
        self, name: str | Path, text: str, *, overwrite: bool = False
    ) -> PreparedWrite:
        path = self._path(name)
        encoded = text.encode("utf-8")
        replacing = path.stat().st_size if overwrite and path.exists() else 0
        limit = self.run._available_bytes_for_cell(
            self.cell_id, replacing_bytes=replacing
        )
        if len(encoded) > limit:
            self.run._mark_capacity_exceeded()
            raise RunOutputError(
                f"cell output capacity exceeded: {len(encoded)} > {limit} bytes"
            )
        return PreparedWrite(self, path, encoded, overwrite, replacing)

    def write_json(
        self, name: str | Path, payload: dict[str, Any], *, overwrite: bool = False
    ) -> Path:
        return self.prepare_json(name, payload, overwrite=overwrite).publish()

    def write_text(self, name: str | Path, text: str, *, overwrite: bool = False) -> Path:
        return self.prepare_text(name, text, overwrite=overwrite).publish()

    def _publish(self, prepared: PreparedWrite) -> Path:
        with self.run._lock:
            try:
                self.run._commit_cell(self.cell_id, prepared)
                return prepared.path
            except RunOutputError as exc:
                if not self.run.capacity_exceeded:
                    self.run._mark_publication_unknown(prepared.path, exc)
                raise
            except Exception as exc:
                self.run._mark_publication_unknown(prepared.path, exc)
                raise


def _owned_relative_path(root: Path, name: str | Path, *, label: str) -> Path:
    relative = Path(name)
    if relative.is_absolute() or ".." in relative.parts:
        raise RunOutputError(f"{label} must be a relative path inside its owner: {name}")
    resolved = (root / relative).resolve()
    try:
        resolved.relative_to(root.resolve())
    except ValueError as exc:
        raise RunOutputError(f"{label} escapes its owner: {name}") from exc
    return resolved


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

    def control_writer(self) -> ControlWriter:
        return ControlWriter(self.run, self.root)

    def cell_writer(self) -> CellWriter:
        return self.run.cell_writer(
            query_case=self.query_case,
            arm_id=self.arm_id,
            root=self.root,
        )

    def write_failure(self, *, status: str, error: str) -> Path:
        if status not in {"Failed", "Cancelled", "Incomplete"}:
            raise RunOutputError(f"invalid failed attempt status: {status}")
        current = json.loads((self.root / "attempt.json").read_text(encoding="utf-8"))
        if current.get("status") != "Running":
            raise RunOutputError(
                f"attempt {self.attempt_id} is already terminal: {current.get('status')}"
            )
        error = _bounded_error_text(error)
        self.control_writer().write_json(
            "failure.json",
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
        # be replaced. It is control metadata, not an evidence payload, and
        # therefore uses the explicit control writer even after evidence
        # capacity is exhausted.
        self.control_writer().write_json("attempt.json", metadata, overwrite=True)
        self.run._update_attempt(metadata)


@dataclass
class CorpusOutput:
    """Small adapter for standalone corpus collectors.

    Corpus programs are not runners: they already own their server loop and
    measurement semantics.  They still use the same RunOutput transaction and
    receipt envelope as gate sources.  ``report_path`` is an allocation hint;
    the durable result lives below the returned run/attempt and an existing
    run is never overwritten.
    """

    run: "RunOutput"
    attempt: AttemptOutput

    @classmethod
    def create(
        cls,
        report_path: Path,
        *,
        source_id: str,
        query_case: str,
        arm_id: str,
        query_cases: int = 1,
        sample_rows: int = 1,
        product_receipts: int = 1,
        summary_captures: int = 0,
    ) -> "CorpusOutput":
        report_path = report_path.resolve()
        run_id = validate_output_id(f"{report_path.stem}-run", label="run id")
        run = RunOutput.create(report_path.parent, run_id=run_id)
        run.registration.cell(
            query_case=query_case,
            arm_id=arm_id,
            query_cases=query_cases,
            sample_rows=sample_rows,
            product_receipts=product_receipts,
            summary_captures=summary_captures,
        )
        # A standalone collector has a finite manifest before it starts a
        # server.  No later code can silently add a cell or enlarge its lease.
        run.registration.seal()
        attempt = run.begin_attempt(source_id, query_case=query_case, arm_id=arm_id)
        return cls(run, attempt)

    @property
    def result_path(self) -> Path:
        return self.attempt.result_path

    @property
    def summary_path(self) -> Path:
        return self.attempt.summary_path

    def publish_json(self, payload: dict[str, Any]) -> Path:
        """Publish a collector-owned payload transactionally.

        The collector may replace its in-progress snapshot within the same
        AttemptId.  A retry or a second process gets a new run/attempt and can
        never overwrite this path.
        """
        return self.attempt.cell_writer().write_json(
            "result.json", payload, overwrite=True
        )

    def publish_summary(self, text: str) -> Path:
        return self.attempt.control_writer().write_text(
            "summary.md", text, overwrite=True
        )

    def finish(self, *, status: str, error: str | None = None) -> None:
        if status == "Completed":
            self.attempt.seal(status=status, result_path=self.result_path, summary_path=self.summary_path)
        else:
            failure = None
            if error is not None:
                self.attempt.write_failure(status=status, error=error)
                failure = self.attempt.failure_path
            self.attempt.seal(
                status=status,
                result_path=self.result_path if self.result_path.exists() else None,
                summary_path=self.summary_path if self.summary_path.exists() else None,
                failure_path=failure,
            )
        self.run.finalize(status=status)


@dataclass
class CampaignOutput:
    """Typed output boundary for a multi-cell corpus collector.

    This is deliberately only an adapter over :class:`RunOutput`: registration,
    attempts, quota accounting and terminal sealing remain owned by the same
    implementation used by normal benchmark sources.  It does not execute
    queries or introduce another archive format.
    """

    run: "RunOutput"
    attempts: dict[tuple[str, str], AttemptOutput]

    @classmethod
    def create(
        cls,
        report_path: Path,
        *,
        source_id: str,
        cells: list[dict[str, Any]],
    ) -> "CampaignOutput":
        report_path = report_path.resolve()
        run_id = validate_output_id(f"{report_path.stem}-run", label="run id")
        run = RunOutput.create(report_path.parent, run_id=run_id)
        for cell in cells:
            run.registration.cell(**cell)
        run.registration.seal()
        attempts: dict[tuple[str, str], AttemptOutput] = {}
        for cell in cells:
            query_case = validate_output_id(cell["query_case"], label="query case")
            arm_id = validate_output_id(cell["arm_id"], label="arm id")
            cell_source = validate_output_id(
                f"{source_id}-{query_case}-{arm_id}", label="source id"
            )
            attempts[(query_case, arm_id)] = run.begin_attempt(
                cell_source, query_case=query_case, arm_id=arm_id
            )
        return cls(run, attempts)

    @property
    def control(self) -> ControlWriter:
        return self.run.control_writer()

    def publish_cell_json(
        self, *, query_case: str, arm_id: str, payload: dict[str, Any]
    ) -> Path:
        attempt = self.attempts[(query_case, arm_id)]
        return attempt.cell_writer().write_json("result.json", payload, overwrite=True)

    def publish_campaign_json(self, payload: dict[str, Any]) -> Path:
        return self.control.write_json("campaign.json", payload, overwrite=True)

    def publish_cell_summary(
        self, *, query_case: str, arm_id: str, text: str
    ) -> Path:
        attempt = self.attempts[(query_case, arm_id)]
        return attempt.control_writer().write_text("summary.md", text, overwrite=True)

    def finish(
        self,
        *,
        status: str,
        errors: dict[tuple[str, str], str] | None = None,
    ) -> None:
        errors = errors or {}
        for key, attempt in self.attempts.items():
            error = errors.get(key)
            # A campaign terminal state describes the campaign as a whole;
            # it must not rewrite a successfully sealed cell when a later
            # cell fails.  Each attempt owns its own terminal record and is
            # therefore sealed from its own evidence/error state first.
            if error is None and attempt.result_path.exists():
                attempt.seal(
                    status="Completed",
                    result_path=attempt.result_path,
                    summary_path=attempt.summary_path
                    if attempt.summary_path.exists()
                    else None,
                )
            else:
                terminal = "Incomplete" if status == "Completed" else status
                failure = None
                if error is not None:
                    attempt.write_failure(status=terminal, error=error)
                    failure = attempt.failure_path
                attempt.seal(
                    status=terminal,
                    result_path=attempt.result_path
                    if attempt.result_path.exists()
                    else None,
                    summary_path=attempt.summary_path
                    if attempt.summary_path.exists()
                    else None,
                    failure_path=failure,
                )
        self.run.finalize(status=status)


@dataclass
class RunOutput:
    """The sole owner of a command's report root and attempt registry."""

    report_root: Path
    campaign_id: str
    run_id: str
    root: Path
    _manifest: dict[str, Any]
    _lock: threading.RLock = dataclass_field(
        default_factory=threading.RLock, repr=False, compare=False
    )

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
                "registration_sealed": False,
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

    @property
    def registration(self) -> CampaignRegistration:
        return CampaignRegistration(self)

    def control_writer(self) -> ControlWriter:
        return ControlWriter(self, self.root)

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
            ControlWriter(self, attempt_root).write_json(
                "attempt.json", metadata, overwrite=False
            )
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
        ) in {"CapacityExceeded", "PublicationUnknown"}:
            raise RunOutputError(
                "cannot complete a run after capacity or publication uncertainty"
            )
        self._manifest["status"] = status
        self._manifest["sealed_at"] = _now()
        self._persist_manifest()

    @property
    def capacity_exceeded(self) -> bool:
        return self._manifest.get("registration", {}).get("status") == "CapacityExceeded"

    def seal_registration(self) -> None:
        registration = self._manifest["registration"]
        registration["registration_sealed"] = True
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
    ) -> CellWriter:
        """Freeze one finite campaign-cell budget before its source runs."""
        cell_id = validate_output_id(cell_id, label="cell id")
        query_case = validate_output_id(query_case or cell_id, label="query case")
        arm_id = validate_output_id(arm_id, label="arm id")
        if min(query_cases, sample_rows, product_receipts, calibration_rows, summary_captures) < 0:
            raise RunOutputError("campaign registration counts must be >= 0")
        registration = self._manifest["registration"]
        if registration.get("registration_sealed"):
            raise RunOutputError("campaign registration is sealed after the first owned payload")
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
                return CellWriter(self, cell_id, self.root / "sources")
            raise RunOutputError(
                f"campaign cell already registered with different contract: {cell_id}"
            )
        cell_budget = (
            4_096
            + 1_024 * query_cases
            + 2_048 * product_receipts
            + 512 * calibration_rows
            + 200_000 * summary_captures
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
            "budget_bytes": cell_budget,
            "written_bytes": 0,
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
        return CellWriter(self, cell_id, self.root / "sources")

    def cell_writer(
        self, *, query_case: str, arm_id: str, root: Path
    ) -> CellWriter:
        query_case = validate_output_id(query_case, label="query case")
        arm_id = validate_output_id(arm_id, label="arm id")
        cell_id = f"{query_case}--{arm_id}"
        cell = next(
            (
                item
                for item in self._manifest["registration"].get("cells", [])
                if item.get("cell_id") == cell_id
            ),
            None,
        )
        if cell is None:
            raise RunOutputError(f"cell is not registered: {cell_id}")
        resolved_root = root.resolve()
        try:
            resolved_root.relative_to(self.root.resolve())
        except ValueError as exc:
            raise RunOutputError("cell writer root is outside the run") from exc
        return CellWriter(self, cell_id, resolved_root)

    def _registered_cell(self, cell_id: str) -> dict[str, Any]:
        for cell in self._manifest["registration"].get("cells", []):
            if cell.get("cell_id") == cell_id:
                return cell
        raise RunOutputError(f"cell is not registered: {cell_id}")

    def _available_bytes_for_cell(
        self, cell_id: str, *, replacing_bytes: int = 0
    ) -> int:
        registration = self._manifest["registration"]
        if registration.get("status") in {"CapacityExceeded", "PublicationUnknown"}:
            raise RunOutputError(
                "cell payload publication is closed after a terminal capacity/publication failure"
            )
        cell = self._registered_cell(cell_id)
        registered = int(registration.get("budget_bytes", 0))
        campaign_limit = int(
            registration.get("total_limit_bytes", CAMPAIGN_TOTAL_LIMIT_BYTES)
        )
        limit = min(campaign_limit, registered) if registered else campaign_limit
        available = limit - max(
            0, int(registration.get("written_bytes", 0)) - replacing_bytes
        )
        available = min(
            available,
            int(cell.get("budget_bytes", 0))
            - max(0, int(cell.get("written_bytes", 0)) - replacing_bytes),
        )
        return max(0, available)

    def _commit_cell(self, cell_id: str, prepared: PreparedWrite) -> None:
        registration = self._manifest["registration"]
        cell = self._registered_cell(cell_id)
        encoded_bytes = len(prepared.encoded)
        available = self._available_bytes_for_cell(
            cell_id, replacing_bytes=prepared.replacing_bytes
        )
        if encoded_bytes > available:
            self._mark_capacity_exceeded()
            raise RunOutputError(
                f"campaign output capacity exceeded: {encoded_bytes} > {available} bytes"
            )
        _write_encoded_atomically(
            prepared.path, prepared.encoded, overwrite=prepared.overwrite
        )
        registration["written_bytes"] = max(
            0, int(registration.get("written_bytes", 0)) - prepared.replacing_bytes
        ) + encoded_bytes
        cell["written_bytes"] = max(
            0, int(cell.get("written_bytes", 0)) - prepared.replacing_bytes
        ) + encoded_bytes
        registration["registration_sealed"] = True
        self._persist_manifest()

    def _commit_control(self, prepared: PreparedWrite) -> None:
        registration = self._manifest["registration"]
        encoded_bytes = len(prepared.encoded)
        current = int(registration.get("control_written_bytes", 0))
        total = max(0, current - prepared.replacing_bytes) + encoded_bytes
        limit = int(registration.get("control_limit_bytes", CONTROL_OUTPUT_LIMIT_BYTES))
        if total > limit:
            raise RunOutputError(
                f"terminal metadata capacity exceeded: {total} > {limit} bytes"
            )
        _write_encoded_atomically(
            prepared.path, prepared.encoded, overwrite=prepared.overwrite
        )
        registration["control_written_bytes"] = total
        self._persist_manifest()

    def _persist_manifest(self) -> None:
        registration = self._manifest.setdefault("registration", {})
        limit = int(registration.get("manifest_limit_bytes", MANIFEST_LIMIT_BYTES))
        registration["manifest_bytes"] = 0
        encoded = _encode_json(self._manifest, limit_bytes=limit)
        if len(encoded) > limit:
            raise RunOutputError(
                f"manifest capacity exceeded: {len(encoded)} > {limit} bytes"
            )
        registration["manifest_bytes"] = len(encoded)
        encoded = _encode_json(self._manifest, limit_bytes=limit)
        if len(encoded) > limit:
            raise RunOutputError(
                f"manifest capacity exceeded after accounting: {len(encoded)} > {limit} bytes"
            )
        _write_encoded_atomically(self.root / "manifest.json", encoded, overwrite=True)

    def _mark_capacity_exceeded(self) -> None:
        registration = self._manifest["registration"]
        registration["status"] = "CapacityExceeded"
        self._manifest["status"] = "Incomplete"
        self._manifest["sealed_at"] = _now()
        try:
            self._persist_manifest()
        except Exception as exc:
            self._mark_publication_unknown(self.root / "manifest.json", exc)
            raise

    def _mark_publication_unknown(self, path: Path, error: Exception) -> None:
        registration = self._manifest["registration"]
        registration["status"] = "PublicationUnknown"
        self._manifest["status"] = "Incomplete"
        self._manifest["sealed_at"] = _now()
        try:
            self._persist_manifest()
        except Exception:
            pass
        marker = self.root / "publication-unknown.json"
        payload = {
            "schema_version": 1,
            "status": "PublicationUnknown",
            "path": _relative_or_none(path, self.root),
            "error": _bounded_error_text(f"{type(error).__name__}: {error}"),
            "recorded_at": _now(),
        }
        try:
            encoded = _encode_json(payload, limit_bytes=CONTROL_OUTPUT_LIMIT_BYTES)
            _write_encoded_atomically(marker, encoded, overwrite=True)
        except Exception:
            # If the run root is unavailable, the original exception is the
            # only truthful signal left to the caller.
            pass

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
    """Publish already-accounted bytes with durable rename semantics.

    The accounting lease is acquired before this function is called.  A
    short-lived private file is therefore the only untracked writer: its
    contents are flushed before the atomic rename and the containing
    directory is flushed after it.  This keeps a process interruption from
    leaving a manifest claiming a publication that never reached the owned
    directory entry.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    if not overwrite and path.exists():
        raise RunOutputError(f"refusing to overwrite owned output: {path}")
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
    fd = os.open(
        temporary,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL,
        0o600,
    )
    try:
        with os.fdopen(fd, "wb") as stream:
            fd = -1
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        if not overwrite and path.exists():
            raise RunOutputError(f"refusing to overwrite owned output: {path}")
        os.replace(temporary, path)
        directory_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        if fd >= 0:
            os.close(fd)
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
