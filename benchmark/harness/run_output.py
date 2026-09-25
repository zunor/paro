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
import hashlib
import json
import os
from pathlib import Path
import re
import threading
import uuid
from typing import Any


# RunOutput is part of the same current Compile Evidence contract. Historical
# run directories are not reopened by this writer.
RUN_OUTPUT_SCHEMA_VERSION = 3
RUN_SUMMARY_SCHEMA_VERSION = 3
RUN_REGISTRATION_SCHEMA_VERSION = 3
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


class CapacityExceededError(RunOutputError):
    """A bounded record did not fit in its registered byte lease."""

    def __init__(self, message: str, *, attempted_bytes: int, limit_bytes: int):
        super().__init__(message)
        self.attempted_bytes = attempted_bytes
        self.limit_bytes = limit_bytes


def _default_sample_ids(query_case: str, sample_rows: int) -> list[str]:
    return [f"{query_case}-sample-{index:04d}" for index in range(sample_rows)]


def _validate_sample_ids(sample_ids: list[str], *, sample_rows: int) -> list[str]:
    if len(sample_ids) != sample_rows or len(set(sample_ids)) != len(sample_ids):
        raise RunOutputError(
            "registered sample_ids must be unique and have exactly sample_rows entries"
        )
    for sample_id in sample_ids:
        validate_output_id(sample_id, label="sample id")
    return list(sample_ids)


def _cell_budget_bytes(
    *,
    query_cases: int,
    sample_rows: int,
    product_receipts: int,
    calibration_rows: int,
    summary_captures: int,
    attempts: int,
) -> int:
    """Charge every registered unit through one deterministic formula."""
    return (
        4_096
        # Query metadata includes both engines' typed output schemas and ORDER
        # contracts. Wide analytical results (for example 44-column Q66) need
        # a fixed metadata allowance independent of sample/receipt counts.
        + 16_384 * query_cases
        + 1_024 * sample_rows
        # A v3 association retains compile, admission and execution payloads.
        # Real producer receipts exceed 2 KiB even with compact JSON. Reserve
        # their typed envelope, without multiplying the logical receipt count.
        + 4_096 * product_receipts
        + 512 * calibration_rows
        + 200_000 * summary_captures
        + 256 * attempts
    )


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
    # Machine-owned evidence has one canonical compact representation. Pretty
    # indentation scales with receipt nesting, not with retained information.
    encoder = json.JSONEncoder(separators=(",", ":"), ensure_ascii=False, sort_keys=True)
    output = bytearray()
    for chunk in encoder.iterencode(payload):
        encoded = chunk.encode("utf-8")
        if limit_bytes is not None and len(output) + len(encoded) + 1 > limit_bytes:
            raise CapacityExceededError(
                f"JSON record capacity exceeded while encoding: "
                f"> {limit_bytes} bytes",
                attempted_bytes=len(output) + len(encoded) + 1,
                limit_bytes=limit_bytes,
            )
        output.extend(encoded)
    output.extend(b"\n")
    return bytes(output)


def _iter_capture_references(value: Any):
    """Yield producer-owned capture references embedded in a cell payload.

    Capture references are deliberately discovered by their typed status and
    fields rather than by a collector-specific JSON path.  The cell envelope
    remains generic, while the writer can still enforce that every registered
    capture is present before an attempt becomes Completed.
    """
    if isinstance(value, dict):
        if value.get("status") == "Captured" and {
            "status", "path", "sha256", "schema_version"
        }.issubset(value):
            yield value
        for child in value.values():
            yield from _iter_capture_references(child)
    elif isinstance(value, list):
        for child in value:
            yield from _iter_capture_references(child)


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


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
    observed_size: int
    observed_mtime_ns: int | None

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
        attempts: int = 1,
        sample_ids: list[str] | None = None,
    ) -> "CellWriter":
        return self.run.register_cell(
            cell_id=f"{query_case}--{arm_id}",
            query_cases=query_cases,
            sample_rows=sample_rows,
            product_receipts=product_receipts,
            calibration_rows=calibration_rows,
            summary_captures=summary_captures,
            attempts=attempts,
            sample_ids=sample_ids,
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
    allow_terminal: bool = False

    def _path(self, name: str | Path) -> Path:
        return _owned_relative_path(self.root, name, label="control output")

    def prepare_json(
        self, name: str | Path, payload: dict[str, Any], *, overwrite: bool = False
    ) -> PreparedWrite:
        path = self._path(name)
        encoded = _encode_json(payload, limit_bytes=CONTROL_OUTPUT_LIMIT_BYTES)
        stat = path.stat() if path.exists() else None
        replacing = stat.st_size if overwrite and stat is not None else 0
        return PreparedWrite(
            self, path, encoded, overwrite, replacing,
            stat.st_size if stat is not None else 0,
            stat.st_mtime_ns if stat is not None else None,
        )

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
        stat = path.stat() if path.exists() else None
        replacing = stat.st_size if overwrite and stat is not None else 0
        return PreparedWrite(
            self, path, encoded, overwrite, replacing,
            stat.st_size if stat is not None else 0,
            stat.st_mtime_ns if stat is not None else None,
        )

    def write_json(
        self, name: str | Path, payload: dict[str, Any], *, overwrite: bool = False
    ) -> Path:
        return self.prepare_json(name, payload, overwrite=overwrite).publish()

    def write_text(self, name: str | Path, text: str, *, overwrite: bool = False) -> Path:
        return self.prepare_text(name, text, overwrite=overwrite).publish()

    def _publish(self, prepared: PreparedWrite) -> Path:
        with self.run._lock:
            # A terminal run owns its already-published state.  Reject a late
            # writer before entering the publication-error path: treating a
            # caller that raced with finalization as PublicationUnknown would
            # overwrite a legitimate terminal outcome.
            self.run._ensure_running_for_publish(prepared)
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
        stat = path.stat() if path.exists() else None
        replacing = stat.st_size if overwrite and stat is not None else 0
        limit = self.run._available_bytes_for_cell(
            self.cell_id, replacing_bytes=replacing
        )
        try:
            encoded = _encode_json(payload, limit_bytes=limit)
        except CapacityExceededError as error:
            self.run._mark_capacity_exceeded(
                omitted_bytes=max(0, error.attempted_bytes - error.limit_bytes)
            )
            raise
        return PreparedWrite(
            self, path, encoded, overwrite, replacing,
            stat.st_size if stat is not None else 0,
            stat.st_mtime_ns if stat is not None else None,
        )

    def prepare_text(
        self, name: str | Path, text: str, *, overwrite: bool = False
    ) -> PreparedWrite:
        path = self._path(name)
        encoded = text.encode("utf-8")
        stat = path.stat() if path.exists() else None
        replacing = stat.st_size if overwrite and stat is not None else 0
        limit = self.run._available_bytes_for_cell(
            self.cell_id, replacing_bytes=replacing
        )
        if len(encoded) > limit:
            self.run._mark_capacity_exceeded(omitted_bytes=len(encoded) - limit)
            raise RunOutputError(
                f"cell output capacity exceeded: {len(encoded)} > {limit} bytes"
            )
        return PreparedWrite(
            self, path, encoded, overwrite, replacing,
            stat.st_size if stat is not None else 0,
            stat.st_mtime_ns if stat is not None else None,
        )

    def write_json(
        self, name: str | Path, payload: dict[str, Any], *, overwrite: bool = False
    ) -> Path:
        return self.prepare_json(name, payload, overwrite=overwrite).publish()

    def write_text(self, name: str | Path, text: str, *, overwrite: bool = False) -> Path:
        return self.prepare_text(name, text, overwrite=overwrite).publish()

    def _publish(self, prepared: PreparedWrite) -> Path:
        with self.run._lock:
            self.run._ensure_running_for_publish(prepared)
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

    @property
    def status(self) -> str:
        return str(
            json.loads((self.root / "attempt.json").read_text(encoding="utf-8"))["status"]
        )

    def control_writer(self) -> ControlWriter:
        return ControlWriter(self.run, self.root)

    def _lifecycle_writer(self) -> ControlWriter:
        """Complete only this attempt's already-started terminal transition."""
        return ControlWriter(self.run, self.root, allow_terminal=True)

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
        self._lifecycle_writer().write_json(
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
        if status == "Completed" and not self.run._attempt_payload_complete(self):
            raise RunOutputError(
                "cannot complete an attempt without a registered, owned payload "
                "covering every declared sample"
            )
        metadata = {
            "schema_version": RUN_SUMMARY_SCHEMA_VERSION,
            "run_id": self.run.run_id,
            "campaign_id": self.run.campaign_id,
            "source_id": self.source_id,
            "attempt_id": self.attempt_id,
            "attempt_index": self.run._attempt_index(
                query_case=self.query_case, arm_id=self.arm_id, attempt_id=self.attempt_id
            ),
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
        self._lifecycle_writer().write_json("attempt.json", metadata, overwrite=True)
        self.run._update_attempt(metadata)

    def accept(self) -> None:
        """Explicitly accept this completed attempt for its cell.

        Selection is a producer decision, not an inference from list order or
        recency.  An attempt can only be accepted after its own terminal
        record is sealed successfully.
        """
        if self.status != "Completed":
            raise RunOutputError("only a completed attempt can be accepted")
        self.run.accept_attempt(
            query_case=self.query_case,
            arm_id=self.arm_id,
            attempt_id=self.attempt_id,
        )


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
        from .receipt_contract import ReceiptContractError, validate_benchmark_payload

        try:
            validate_benchmark_payload(payload)
        except ReceiptContractError as error:
            # The writer boundary exposes one owned error type.  A malformed
            # producer payload must not bypass the RunOutput state machine or
            # be mistaken for a filesystem failure by a caller.
            raise RunOutputError(str(error)) from error
        self.run._validate_payload_owner(self.attempt, payload)
        return self.attempt.cell_writer().write_json(
            "result.json", payload, overwrite=True
        )

    def publish_capture_text(self, name: str, text: str) -> Path:
        """Publish one immutable producer capture without duplicating it in result.json."""
        return self.attempt.cell_writer().write_text(
            Path("captures") / validate_output_id(name, label="capture name"),
            text,
            overwrite=False,
        )

    def publish_summary(self, text: str) -> Path:
        return self.attempt.control_writer().write_text(
            "summary.md", text, overwrite=True
        )

    def finish(self, *, status: str, error: str | None = None) -> None:
        if self.attempt.status != "Running":
            current = self.run._manifest.get("status", "Incomplete")
            if status != current:
                raise RunOutputError(
                    f"run is already terminal: {current}; cannot finish as {status}"
                )
            return
        if status == "Completed" and not self.run._attempt_payload_complete(self.attempt):
            status = "Incomplete"
            error = error or "declared samples and receipts are not complete"
        if status == "Completed":
            self.attempt.seal(status=status, result_path=self.result_path, summary_path=self.summary_path)
            self.attempt.accept()
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
        terminal_status = (
            "Incomplete"
            if self.run._manifest.get("registration", {}).get("status")
            in {"CapacityExceeded", "PublicationUnknown"}
            else status
        )
        self.run.finalize(status=terminal_status)


@dataclass(frozen=True)
class CampaignSummary:
    """The bounded, typed campaign index written by the producer."""

    schema_version: int
    campaign_id: str
    run_id: str
    status: str | None
    registration_status: str | None
    cells: tuple[dict[str, Any], ...]

    @classmethod
    def from_run(cls, run: "RunOutput") -> "CampaignSummary":
        cells: list[dict[str, Any]] = []
        for cell in run._manifest.get("registration", {}).get("cells", []):
            attempts = [
                metadata
                for metadata in run._manifest.get("attempts", [])
                if metadata.get("query_case") == cell.get("query_case")
                and metadata.get("arm_id") == cell.get("arm_id")
            ]
            cells.append({
                "cell_id": cell["cell_id"],
                "query_case": cell.get("query_case"),
                "arm_id": cell.get("arm_id"),
                "declared_samples": cell.get("sample_rows"),
                "sample_ids": cell.get("sample_ids", []),
                "declared_receipts": cell.get("product_receipts"),
                "declared_captures": cell.get("summary_captures"),
                "accepted_attempt_id": cell.get("accepted_attempt_id"),
                "attempts": [
                    {
                        "source_id": item.get("source_id"),
                        "attempt_id": item.get("attempt_id"),
                        "attempt_index": item.get("attempt_index"),
                        "status": item.get("status"),
                        "result": item.get("result"),
                        "summary": item.get("summary"),
                        "failure": item.get("failure"),
                        "metadata": item.get("metadata"),
                    }
                    for item in attempts
                ],
            })
        return cls(
            schema_version=RUN_SUMMARY_SCHEMA_VERSION,
            campaign_id=run.campaign_id,
            run_id=run.run_id,
            status=run._manifest.get("status"),
            registration_status=run._manifest.get("registration", {}).get("status"),
            cells=tuple(cells),
        )

    def to_payload(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "kind": "CampaignSummary",
            "campaign_id": self.campaign_id,
            "run_id": self.run_id,
            "status": self.status,
            "registration_status": self.registration_status,
            "cells": list(self.cells),
        }


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
        # Keep the producer/consumer contract at the shared writer boundary;
        # a collector cannot publish an arbitrary JSON object as a cell.
        from .receipt_contract import ReceiptContractError, validate_benchmark_payload

        try:
            validate_benchmark_payload(payload)
        except ReceiptContractError as error:
            raise RunOutputError(str(error)) from error
        attempt = self.attempts[(query_case, arm_id)]
        self.run._validate_payload_owner(attempt, payload)
        return attempt.cell_writer().write_json("result.json", payload, overwrite=True)


    def publish_capture_text(
        self, *, query_case: str, arm_id: str, name: str, text: str
    ) -> Path:
        """Store one immutable EXPLAIN capture under its owning cell."""
        attempt = self.attempts[(query_case, arm_id)]
        return attempt.cell_writer().write_text(
            Path("captures") / validate_output_id(name, label="capture name"),
            text,
            overwrite=False,
        )

    def publish_campaign_summary(self, *, allow_terminal: bool = False) -> Path:
        """Publish only the bounded campaign index and terminal metadata.

        Domain reports and Detail captures belong to their cell/attempt owners.
        Keeping this method payload-free prevents a collector from smuggling an
        unbounded report or a second, incompatible campaign schema into the
        control plane.
        """
        payload = CampaignSummary.from_run(self.run).to_payload()
        writer = ControlWriter(self.run, self.run.root, allow_terminal=allow_terminal)
        return writer.write_json("campaign.json", payload, overwrite=True)

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
            if attempt.status != "Running":
                continue
            error = errors.get(key)
            # A campaign terminal state describes the campaign as a whole;
            # it must not rewrite a successfully sealed cell when a later
            # cell fails.  Each attempt owns its own terminal record and is
            # therefore sealed from its own evidence/error state first.
            if error is None and self.run._attempt_payload_complete(attempt):
                attempt.seal(
                    status="Completed",
                    result_path=attempt.result_path,
                    summary_path=attempt.summary_path
                    if attempt.summary_path.exists()
                    else None,
                )
                attempt.accept()
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
        terminal_status = (
            "Incomplete"
            if any(attempt.status != "Completed" for attempt in self.attempts.values())
            or self.run._manifest.get("status") == "Incomplete"
            or self.run._manifest.get("registration", {}).get("status")
            in {"CapacityExceeded", "PublicationUnknown"}
            else status
        )
        self.run.finalize(status=terminal_status)
        # The manifest is the lifecycle authority.  Refresh the bounded
        # campaign index only after that transition so consumers never see a
        # completed attempt paired with a stale Running campaign summary.
        self.publish_campaign_summary(allow_terminal=True)


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
                "schema_version": RUN_REGISTRATION_SCHEMA_VERSION,
                "cells": [],
                "budget_bytes": 0,
                "written_bytes": 0,
                "control_written_bytes": 0,
                "control_limit_bytes": CONTROL_OUTPUT_LIMIT_BYTES,
                "manifest_bytes": 0,
                "omitted_count": 0,
                "omitted_bytes": 0,
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
                "attempt_index": self._attempt_index(
                    query_case=query_case, arm_id=arm_id, attempt_id=attempt_id
                ),
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

    def _attempt_index(self, *, query_case: str, arm_id: str, attempt_id: str) -> int:
        indexes = [
            int(item["attempt_index"])
            for item in self._manifest.get("attempts", [])
            if item.get("query_case") == query_case
            and item.get("arm_id") == arm_id
            and isinstance(item.get("attempt_index"), int)
            and item.get("attempt_id") != attempt_id
        ]
        return max(indexes, default=-1) + 1

    def accept_attempt(self, *, query_case: str, arm_id: str, attempt_id: str) -> None:
        """Seal the producer's explicit retry decision for one cell."""
        with self._lock:
            cell = self._registered_cell(f"{query_case}--{arm_id}")
            attempt = next(
                (
                    item
                    for item in self._manifest.get("attempts", [])
                    if item.get("attempt_id") == attempt_id
                    and item.get("query_case") == query_case
                    and item.get("arm_id") == arm_id
                ),
                None,
            )
            if attempt is None:
                raise RunOutputError("cannot accept an unknown cell attempt")
            if attempt.get("status") != "Completed":
                raise RunOutputError("only a completed attempt can be accepted")
            existing = cell.get("accepted_attempt_id")
            if existing is not None and existing != attempt_id:
                raise RunOutputError("cell already has a different accepted attempt")
            cell["accepted_attempt_id"] = attempt_id
            self._persist_manifest()

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
        if status == "Completed":
            for cell in self._manifest.get("registration", {}).get("cells", []):
                accepted = cell.get("accepted_attempt_id")
                if not isinstance(accepted, str) or not accepted:
                    raise RunOutputError(
                        f"cannot complete a run without an accepted attempt for {cell.get('cell_id')}"
                    )
                if not any(
                    attempt.get("attempt_id") == accepted
                    and attempt.get("status") == "Completed"
                    for attempt in self._manifest.get("attempts", [])
                ):
                    raise RunOutputError(
                        f"accepted attempt is not completed for {cell.get('cell_id')}"
                    )
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
        if registration.get("status") in {
            "CapacityExceeded", "PublicationUnknown"
        }:
            raise RunOutputError(
                "cannot seal registration after a terminal capacity/publication failure"
            )
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
        attempts: int = 1,
        sample_ids: list[str] | None = None,
        query_case: str | None = None,
        arm_id: str = "default",
    ) -> CellWriter:
        """Freeze one finite campaign-cell budget before its source runs."""
        cell_id = validate_output_id(cell_id, label="cell id")
        query_case = validate_output_id(query_case or cell_id, label="query case")
        arm_id = validate_output_id(arm_id, label="arm id")
        if min(query_cases, sample_rows, product_receipts, calibration_rows, summary_captures, attempts) < 0:
            raise RunOutputError("campaign registration counts must be >= 0")
        registration = self._manifest["registration"]
        if registration.get("registration_sealed") or registration.get("status") in {
            "CapacityExceeded", "PublicationUnknown"
        }:
            raise RunOutputError("campaign registration is sealed after the first owned payload")
        expected_cell_id = f"{query_case}--{arm_id}"
        if cell_id != expected_cell_id:
            raise RunOutputError(
                f"cell id must be query_case--arm_id ({expected_cell_id}), got {cell_id}"
            )
        sample_ids = _validate_sample_ids(
            sample_ids
            if sample_ids is not None
            else _default_sample_ids(query_case, sample_rows),
            sample_rows=sample_rows,
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
                "attempts": attempts,
                "query_case": query_case,
                "arm_id": arm_id,
                "sample_ids": sample_ids,
            }
            if all(
                (current.get(key, 1) if key == "attempts" else current.get(key)) == value
                for key, value in requested.items()
            ):
                return CellWriter(self, cell_id, self.root / "sources")
            raise RunOutputError(
                f"campaign cell already registered with different contract: {cell_id}"
            )
        cell_budget = _cell_budget_bytes(
            query_cases=query_cases,
            sample_rows=sample_rows,
            product_receipts=product_receipts,
            calibration_rows=calibration_rows,
            summary_captures=summary_captures,
            attempts=attempts,
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
            "attempts": attempts,
            "accepted_attempt_id": None,
            "sample_ids": sample_ids,
            "budget_bytes": cell_budget,
            "written_bytes": 0,
        }]
        arms = len({cell["arm_id"] for cell in cells})
        queries = sum(cell["query_cases"] for cell in cells)
        captures = sum(cell["summary_captures"] for cell in cells)
        manifest_bytes = 32_000 + 1_024 * (arms + queries + len(cells) + captures)
        cells_bytes = sum(
            _cell_budget_bytes(
                query_cases=cell["query_cases"],
                sample_rows=cell["sample_rows"],
                product_receipts=cell["product_receipts"],
                calibration_rows=cell["calibration_rows"],
                summary_captures=cell["summary_captures"],
                attempts=cell.get("attempts", 1),
            )
            for cell in cells
        )
        attempts_total = sum(int(cell.get("attempts", 1)) for cell in cells)
        sample_rows_total = sum(int(cell["sample_rows"]) for cell in cells)
        budget = (
            manifest_bytes
            + 20_000
            + cells_bytes
            + 256 * len(cells)
            + 256 * attempts_total
            + 256 * sample_rows_total
            + 200_000 * captures
        )
        if budget > CAMPAIGN_TOTAL_LIMIT_BYTES:
            self._mark_capacity_exceeded(
                omitted_bytes=budget - CAMPAIGN_TOTAL_LIMIT_BYTES
            )
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

    def _validate_payload_owner(
        self, attempt: AttemptOutput, payload: dict[str, Any]
    ) -> None:
        ownership = payload.get("ownership")
        if not isinstance(ownership, dict):
            raise RunOutputError("cell payload has no ownership envelope")
        expected = {
            "campaign_id": self.campaign_id,
            "run_id": self.run_id,
            "source_id": attempt.source_id,
            "attempt_id": attempt.attempt_id,
            "query_case": attempt.query_case,
            "arm_id": attempt.arm_id,
        }
        for field, value in expected.items():
            if ownership.get(field) != value:
                raise RunOutputError(
                    f"cell payload ownership mismatch for {field}: "
                    f"expected {value!r}, got {ownership.get(field)!r}"
                )
        cell = self._registered_cell(f"{attempt.query_case}--{attempt.arm_id}")
        if ownership.get("sample_ids") != cell.get("sample_ids"):
            raise RunOutputError("cell payload sample_ids differ from registration")

    def _attempt_payload_complete(self, attempt: AttemptOutput) -> bool:
        if not attempt.result_path.exists():
            return False
        try:
            payload = json.loads(attempt.result_path.read_text(encoding="utf-8"))
            from .receipt_contract import validate_benchmark_payload

            validate_benchmark_payload(payload)
            self._validate_payload_owner(attempt, payload)
        except (OSError, ValueError, TypeError, KeyError, RunOutputError):
            return False
        cell = self._registered_cell(f"{attempt.query_case}--{attempt.arm_id}")
        capture_references = list(_iter_capture_references(payload))
        declared_captures = int(cell.get("summary_captures", 0))
        if len(capture_references) != declared_captures:
            return False
        capture_paths: set[str] = set()
        attempt_root = attempt.root.resolve()
        for reference in capture_references:
            path_value = reference.get("path")
            digest_value = reference.get("sha256")
            if (
                not isinstance(path_value, str)
                or not isinstance(digest_value, str)
                or len(digest_value) != 64
                or any(character not in "0123456789abcdef" for character in digest_value)
                or reference.get("schema_version") != RUN_OUTPUT_SCHEMA_VERSION
            ):
                return False
            try:
                relative_path = Path(path_value)
                if relative_path.is_absolute() or ".." in relative_path.parts:
                    return False
                capture_path = self.owned_path(self.root / path_value)
                capture_path.relative_to(attempt_root)
            except (RunOutputError, ValueError):
                return False
            relative = capture_path.relative_to(self.root.resolve()).as_posix()
            if relative in capture_paths or not capture_path.is_file():
                return False
            try:
                if _file_sha256(capture_path) != digest_value:
                    return False
            except OSError:
                return False
            capture_paths.add(relative)
        receipts: list[Any] = []
        for workload in payload.get("workloads", []):
            for query in workload.get("queries", []):
                if isinstance(query.get("compile_receipts"), list):
                    receipts.extend(query["compile_receipts"])
                elif query.get("compile_receipt") is not None:
                    receipts.append(query["compile_receipt"])
        return (
            len(receipts) == len(cell.get("sample_ids", []))
            and len(receipts) == int(cell.get("product_receipts", 0))
        )

    def _ensure_running_for_publish(self, prepared: PreparedWrite) -> None:
        status = self._manifest.get("status")
        if status != "Running" and not getattr(prepared.writer, "allow_terminal", False):
            raise RunOutputError(
                f"run is already terminal: {status}; cannot publish new output"
            )

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
        self._check_prepared_generation(prepared)
        registration = self._manifest["registration"]
        cell = self._registered_cell(cell_id)
        encoded_bytes = len(prepared.encoded)
        available = self._available_bytes_for_cell(
            cell_id, replacing_bytes=prepared.replacing_bytes
        )
        if encoded_bytes > available:
            self._mark_capacity_exceeded(omitted_bytes=encoded_bytes - available)
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
        self._check_prepared_generation(prepared)
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

    @staticmethod
    def _check_prepared_generation(prepared: PreparedWrite) -> None:
        """Reject a prepared write whose target changed before commit.

        Capacity is reserved under the RunOutput lock, but preparation can
        happen before another writer acquires it.  Rechecking both size and
        mtime makes the replacing-byte accounting a generation check instead
        of a caller convention.
        """
        stat = prepared.path.stat() if prepared.path.exists() else None
        size = stat.st_size if stat is not None else 0
        mtime_ns = stat.st_mtime_ns if stat is not None else None
        if size != prepared.observed_size or mtime_ns != prepared.observed_mtime_ns:
            raise RunOutputError(
                f"prepared output changed before commit: {prepared.path}"
            )

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

    def _mark_capacity_exceeded(self, *, omitted_bytes: int = 0) -> None:
        registration = self._manifest["registration"]
        registration["omitted_count"] = int(registration.get("omitted_count", 0)) + 1
        registration["omitted_bytes"] = int(registration.get("omitted_bytes", 0)) + max(
            0, int(omitted_bytes)
        )
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
        persist_error: Exception | None = None
        try:
            self._persist_manifest()
        except Exception as exc:
            # A manifest write failure is itself part of the publication
            # outcome.  Still try the separate marker so recovery tooling can
            # see the terminal classification, but never hide this error.
            persist_error = exc
        marker = self.root / "publication-unknown.json"
        payload = {
            "schema_version": RUN_REGISTRATION_SCHEMA_VERSION,
            "status": "PublicationUnknown",
            "path": _relative_or_none(path, self.root),
            "error": _bounded_error_text(f"{type(error).__name__}: {error}"),
            "recorded_at": _now(),
        }
        try:
            encoded = _encode_json(payload, limit_bytes=CONTROL_OUTPUT_LIMIT_BYTES)
            _write_encoded_atomically(marker, encoded, overwrite=True)
        except Exception as marker_error:
            raise RunOutputError(
                "publication outcome is unknown and its durable marker could not be written: "
                f"{type(error).__name__}: {error}; "
                f"manifest error: {type(persist_error).__name__}: {persist_error}; "
                f"marker error: {type(marker_error).__name__}: {marker_error}"
            ) from marker_error
        if persist_error is not None:
            raise RunOutputError(
                "publication outcome is unknown and its manifest could not be persisted: "
                f"{type(error).__name__}: {error}; "
                f"manifest error: {type(persist_error).__name__}: {persist_error}"
            ) from persist_error

    def _update_attempt(self, metadata: dict[str, Any]) -> None:
        attempts = self._manifest.setdefault("attempts", [])
        identity = (metadata.get("source_id"), metadata.get("attempt_id"))
        source_id = metadata.get("source_id")
        attempt_id = metadata.get("attempt_id")
        if not isinstance(source_id, str) or not isinstance(attempt_id, str):
            raise RunOutputError("attempt metadata lacks stable source/attempt identity")
        # The manifest is an index, not a second copy of every attempt's
        # lifecycle record.  The bounded, durable attempt.json under the
        # source owner is authoritative for timestamps and terminal details;
        # these fields are the minimum needed for campaign enumeration and
        # completion checks.
        entry = {
            "source_id": source_id,
            "attempt_id": attempt_id,
            "attempt_index": metadata.get("attempt_index"),
            "query_case": metadata.get("query_case"),
            "arm_id": metadata.get("arm_id"),
            "status": metadata.get("status"),
            "result": metadata.get("result"),
            "summary": metadata.get("summary"),
            "failure": metadata.get("failure"),
            "metadata": (
                Path("sources")
                / source_id
                / "attempts"
                / attempt_id
                / "attempt.json"
            ).as_posix(),
        }
        for position, current in enumerate(attempts):
            if (current.get("source_id"), current.get("attempt_id")) == identity:
                attempts[position] = entry
                break
        else:
            attempts.append(entry)
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
