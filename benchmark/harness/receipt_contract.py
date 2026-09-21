# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

"""Finite schema checks for normal benchmark receipt associations."""

from __future__ import annotations

import json
from typing import Any

from .run_output import SUMMARY_LIMIT_BYTES


RECEIPT_ASSOCIATION_SCHEMA_VERSION = 1


class ReceiptContractError(ValueError):
    """A result contains a malformed identity or admission association."""


def validate_receipt_association(value: Any) -> str:
    if value is None:
        return "Uncovered"
    if not isinstance(value, dict):
        raise ReceiptContractError("compile_receipt must be an object or null")
    if value.get("schema_version") != RECEIPT_ASSOCIATION_SCHEMA_VERSION:
        raise ReceiptContractError("unsupported compile receipt schema version")
    status = value.get("status")
    if status == "Uncovered":
        if not isinstance(value.get("reason"), str) or not value["reason"]:
            raise ReceiptContractError("Uncovered receipt must preserve a reason")
        return status
    if status != "Verified":
        raise ReceiptContractError(f"unknown compile receipt status: {status!r}")
    if value.get("association_basis") != "latest_execution_same_artifact":
        raise ReceiptContractError("verified receipt has no recognized association basis")
    identity = value.get("artifact_identity")
    _validate_identity(identity)
    if not isinstance(value.get("query_fingerprint"), str):
        raise ReceiptContractError("verified receipt lacks query fingerprint")
    if not isinstance(value.get("occurrence"), int) or value["occurrence"] < 0:
        raise ReceiptContractError("verified receipt has invalid occurrence")
    if value.get("compilation") not in {"Executed", "CacheHit"}:
        raise ReceiptContractError("verified receipt has invalid compilation state")
    compile_state = value.get("compile_state")
    if compile_state not in {"Executed", "NotExecuted"}:
        raise ReceiptContractError("verified receipt lacks compile execution state")
    if (value["compilation"] == "CacheHit") != (compile_state == "NotExecuted"):
        raise ReceiptContractError("cache-hit compilation state is inconsistent")
    if not isinstance(value.get("execution_id"), int) or value["execution_id"] < 0:
        raise ReceiptContractError("verified receipt has invalid execution id")
    compile_identity = _validate_receipt_fields(value.get("compile"), required_identity=True)
    execution_identity = _validate_receipt_fields(value.get("execution"), required_identity=True)
    if compile_identity != identity or execution_identity != identity:
        raise ReceiptContractError("nested receipt identity does not match association identity")
    selection = value.get("selection")
    if not isinstance(selection, dict):
        raise ReceiptContractError("verified receipt lacks actual selection contract")
    if selection.get("admission") != "Selected":
        raise ReceiptContractError("verified receipt is not a selected execution")
    if not isinstance(selection.get("actual_class"), int):
        raise ReceiptContractError("verified receipt lacks actual grant class")
    fingerprint = selection.get("actual_fingerprint")
    if (
        not isinstance(fingerprint, list)
        or len(fingerprint) != 2
        or any(not isinstance(word, int) or word < 0 or word >= 1 << 64 for word in fingerprint)
    ):
        raise ReceiptContractError("verified receipt lacks actual physical fingerprint")
    if not isinstance(selection.get("resources"), dict):
        raise ReceiptContractError("verified receipt lacks resource contract")
    if selection.get("image") not in {"Ready", "NotReady"}:
        raise ReceiptContractError("verified receipt lacks executable-image status")
    if selection.get("terminal") not in {
        "Running", "Completed", "Failed", "Cancelled", "Dropped", "NotExecuted"
    }:
        raise ReceiptContractError("verified receipt lacks execution terminal")
    if selection["terminal"] == "NotExecuted":
        raise ReceiptContractError("verified selected execution cannot be NotExecuted")
    return status


def validate_benchmark_payload(payload: dict[str, Any], *, require_receipts: bool = False) -> None:
    if not isinstance(payload, dict):
        raise ReceiptContractError("benchmark payload must be an object")
    if payload.get("version") != 3:
        raise ReceiptContractError("benchmark payload must use schema version 3")
    ownership = payload.get("ownership")
    if not isinstance(ownership, dict) or ownership.get("schema_version") != 1:
        raise ReceiptContractError("benchmark payload lacks ownership schema")
    queries = 0
    for workload in payload.get("workloads", []):
        if not isinstance(workload, dict):
            raise ReceiptContractError("workload entry must be an object")
        for query in workload.get("queries", []):
            if not isinstance(query, dict):
                raise ReceiptContractError("query entry must be an object")
            queries += 1
            status = validate_receipt_association(query.get("compile_receipt"))
            if require_receipts and status != "Verified":
                raise ReceiptContractError(
                    f"query {query.get('id', '<unknown>')} lacks a verified compile receipt"
                )
    if queries == 0:
        raise ReceiptContractError("benchmark payload contains no query cells")


def validate_summary_bytes(summary: str) -> None:
    size = len(summary.encode("utf-8"))
    if size > SUMMARY_LIMIT_BYTES:
        raise ReceiptContractError(
            f"Summary exceeds {SUMMARY_LIMIT_BYTES} bytes: {size}"
        )


def _validate_identity(identity: Any) -> None:
    if not isinstance(identity, dict) or set(identity) != {
        "schema_version", "artifact", "structure", "dependencies"
    }:
        raise ReceiptContractError(
            "artifact identity must contain schema_version/artifact/structure/dependencies"
        )
    if (
        not isinstance(identity["schema_version"], int)
        or isinstance(identity["schema_version"], bool)
        or not 0 < identity["schema_version"] < 1 << 32
    ):
        raise ReceiptContractError("artifact identity has an invalid schema version")
    for key in ("artifact", "structure", "dependencies"):
        words = identity[key]
        if not isinstance(words, list) or len(words) != 2 or any(
            not isinstance(word, int) or word < 0 or word >= 1 << 64 for word in words
        ):
            raise ReceiptContractError(f"artifact identity {key} is not two u64 words")


def _validate_receipt_fields(value: Any, *, required_identity: bool) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        raise ReceiptContractError("receipt detail must be an object")
    if required_identity:
        identity = value.get("artifact_identity")
        _validate_identity(identity)
        return identity
    return None


def payload_size_bytes(payload: dict[str, Any]) -> int:
    return len(json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))
