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
    if value.get("association_basis") != "statement_decision_id":
        raise ReceiptContractError("verified receipt has no exact statement association")
    if (
        not isinstance(value.get("statement_decision_id"), int)
        or isinstance(value["statement_decision_id"], bool)
        or value["statement_decision_id"] < 0
    ):
        raise ReceiptContractError("verified receipt has invalid statement decision id")
    identity = value.get("artifact_identity")
    _validate_identity(identity)
    fingerprint = value.get("query_fingerprint")
    if (
        not isinstance(fingerprint, str)
        or len(fingerprint) != 16
        or any(character not in "0123456789abcdef" for character in fingerprint)
    ):
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
    compile_detail = value["compile"]
    if compile_detail.get("decision_id") != value["statement_decision_id"]:
        raise ReceiptContractError("compile receipt is bound to another statement decision")
    compile_receipt = compile_detail.get("receipt")
    _validate_compile_receipt(compile_receipt, identity)
    if compile_detail.get("cache_hit") not in {True, False}:
        raise ReceiptContractError("compile receipt lacks cache decision")
    if (compile_detail["cache_hit"] is True) != (value["compilation"] == "CacheHit"):
        raise ReceiptContractError("cache decision and compilation state differ")
    execution_detail = value["execution"]
    if execution_detail.get("execution_id") != value["execution_id"]:
        raise ReceiptContractError("execution receipt id does not match association")
    selection = value.get("selection")
    if not isinstance(selection, dict):
        raise ReceiptContractError("verified receipt lacks actual selection contract")
    if selection.get("admission") != "Selected":
        raise ReceiptContractError("verified receipt is not a selected execution")
    expected_class = selection.get("expected_class")
    if expected_class is not None and (
        not isinstance(expected_class, int)
        or isinstance(expected_class, bool)
        or expected_class < 0
    ):
        raise ReceiptContractError("verified receipt has invalid expected grant class")
    observed_expected = compile_receipt.get("expected_class")
    if isinstance(observed_expected, dict) and set(observed_expected) == {"Observed"}:
        if expected_class != observed_expected["Observed"]:
            raise ReceiptContractError("compile and admission expected classes differ")
    raw_execution = execution_detail.get("raw")
    if not isinstance(raw_execution, dict):
        raise ReceiptContractError("execution receipt lacks its producer record")
    _validate_execution_producer_record(
        raw_execution,
        execution_id=value["execution_id"],
        decision_id=value["statement_decision_id"],
        identity=identity,
        selection=selection,
    )
    if (
        not isinstance(selection.get("actual_class"), int)
        or isinstance(selection["actual_class"], bool)
        or selection["actual_class"] < 0
    ):
        raise ReceiptContractError("verified receipt lacks actual grant class")
    fingerprint = selection.get("actual_fingerprint")
    if (
        not isinstance(fingerprint, list)
        or len(fingerprint) != 2
        or any(not isinstance(word, int) or word < 0 or word >= 1 << 64 for word in fingerprint)
    ):
        raise ReceiptContractError("verified receipt lacks actual physical fingerprint")
    resources = selection.get("resources")
    if not isinstance(resources, dict):
        raise ReceiptContractError("verified receipt lacks resource contract")
    required_resources = {
        "class", "minimum_memory_bytes", "working_set_memory_bytes",
        "memory_ceiling_bytes", "memory_completion", "max_parallel_tasks",
        "external_worker_slots",
    }
    if set(resources) != required_resources:
        raise ReceiptContractError("verified receipt has incomplete resource contract")
    if resources["class"] != selection["actual_class"]:
        raise ReceiptContractError("resource and selected grant classes differ")
    for field in (
        "minimum_memory_bytes",
        "working_set_memory_bytes",
        "memory_ceiling_bytes",
        "max_parallel_tasks",
        "external_worker_slots",
    ):
        if (
            not isinstance(resources[field], int)
            or isinstance(resources[field], bool)
            or resources[field] < 0
        ):
            raise ReceiptContractError(f"resource field {field} is invalid")
    if resources["max_parallel_tasks"] == 0:
        raise ReceiptContractError("resource contract has no execution capacity")
    if (
        resources["working_set_memory_bytes"] < resources["minimum_memory_bytes"]
        or resources["memory_ceiling_bytes"] < resources["minimum_memory_bytes"]
    ):
        raise ReceiptContractError("resource memory bounds are inconsistent")
    completion = resources["memory_completion"]
    if completion not in {"Guaranteed", "RuntimeCappedUnbounded"} and not (
        isinstance(completion, dict)
        and set(completion) == {"RuntimeCappedKnown"}
        and isinstance(completion["RuntimeCappedKnown"], dict)
        and set(completion["RuntimeCappedKnown"]) == {"uncapped_memory_bytes"}
    ):
        raise ReceiptContractError("unknown memory completion contract")
    if selection.get("image") not in {"Ready", "NotReady"}:
        raise ReceiptContractError("verified receipt lacks executable-image status")
    if selection.get("terminal") not in {
        "Running", "Completed", "Failed", "Cancelled", "Dropped", "NotExecuted"
    }:
        raise ReceiptContractError("verified receipt lacks execution terminal")
    if selection["terminal"] == "NotExecuted":
        raise ReceiptContractError("verified selected execution cannot be NotExecuted")
    if selection.get("reservation") not in {"Committed", "Failed"}:
        raise ReceiptContractError("selected execution lacks a reservation phase")
    if selection.get("lowering") not in {"NotStarted", "Ready", "Failed"}:
        raise ReceiptContractError("selected execution lacks a lowering phase")
    if selection["lowering"] == "Failed" and not isinstance(selection.get("lowering_error"), str):
        raise ReceiptContractError("failed lowering lacks its original error")
    if selection["reservation"] == "Failed" and selection["terminal"] == "Completed":
        raise ReceiptContractError("completed execution has a failed reservation")
    if selection["reservation"] == "Failed" and (
        selection["terminal"] != "Failed"
        or selection["lowering"] != "NotStarted"
        or selection["image"] != "NotReady"
        or not isinstance(selection.get("terminal_error"), str)
    ):
        raise ReceiptContractError("failed reservation has an inconsistent execution lifecycle")
    if selection["lowering"] == "Failed" and (
        selection["terminal"] != "Failed"
        or selection["image"] != "NotReady"
        or not isinstance(selection.get("terminal_error"), str)
    ):
        raise ReceiptContractError("failed lowering has an inconsistent execution lifecycle")
    if selection["lowering"] == "NotStarted" and selection["image"] != "NotReady":
        raise ReceiptContractError("image is ready before lowering")
    if selection["terminal"] in {"Failed", "Cancelled"} and not isinstance(
        selection.get("terminal_error"), str
    ):
        raise ReceiptContractError("terminal failure lacks its original error")
    if selection["terminal"] == "Completed" and (
        selection["reservation"] != "Committed"
        or selection["lowering"] != "Ready"
    ):
        raise ReceiptContractError("completed execution has incomplete lifecycle phases")
    if selection["terminal"] == "Completed" and selection["image"] != "Ready":
        raise ReceiptContractError("completed execution must have a ready image")
    return status


def _validate_execution_producer_record(
    raw: dict[str, Any],
    *,
    execution_id: int,
    decision_id: int,
    identity: dict[str, Any],
    selection: dict[str, Any],
) -> None:
    if raw.get("schema_version") != 1:
        raise ReceiptContractError("unsupported execution receipt schema version")
    if raw.get("execution_id") != execution_id:
        raise ReceiptContractError("producer execution id differs from association")
    if raw.get("statement_decision_id") != decision_id:
        raise ReceiptContractError("producer statement decision differs from association")
    _validate_identity(raw.get("artifact_identity"))
    if raw["artifact_identity"] != identity:
        raise ReceiptContractError("producer execution identity differs from association")
    for field in (
        "admission", "fallback", "reservation", "lowering", "lowering_error",
        "image", "terminal", "terminal_error", "expected_class", "actual_class",
        "actual_fingerprint", "resources",
    ):
        if raw.get(field) != selection.get(field):
            raise ReceiptContractError(
                f"projected execution field differs from producer: {field}"
            )


def validate_benchmark_payload(payload: dict[str, Any], *, require_receipts: bool = False) -> None:
    if not isinstance(payload, dict):
        raise ReceiptContractError("benchmark payload must be an object")
    if payload.get("version") != 3:
        raise ReceiptContractError("benchmark payload must use schema version 3")
    ownership = payload.get("ownership")
    if not isinstance(ownership, dict) or ownership.get("schema_version") != 1:
        raise ReceiptContractError("benchmark payload lacks ownership schema")
    for field in ("campaign_id", "run_id"):
        if not isinstance(ownership.get(field), str) or not ownership[field]:
            raise ReceiptContractError(f"benchmark payload lacks ownership {field}")
    queries = 0
    for workload in payload.get("workloads", []):
        if not isinstance(workload, dict):
            raise ReceiptContractError("workload entry must be an object")
        for query in workload.get("queries", []):
            if not isinstance(query, dict):
                raise ReceiptContractError("query entry must be an object")
            queries += 1
            receipts = query.get("compile_receipts")
            if receipts is not None:
                if not isinstance(receipts, list) or not receipts:
                    if require_receipts:
                        raise ReceiptContractError(
                            f"query {query.get('id', '<unknown>')} lacks per-sample receipts"
                        )
                    continue
                statuses = [validate_receipt_association(receipt) for receipt in receipts]
            else:
                statuses = [validate_receipt_association(query.get("compile_receipt"))]
            if require_receipts and any(status != "Verified" for status in statuses):
                raise ReceiptContractError(
                    f"query {query.get('id', '<unknown>')} lacks verified per-sample receipts"
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
        or identity["schema_version"] != 1
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


def _validate_compile_receipt(value: Any, identity: dict[str, Any]) -> None:
    if not isinstance(value, dict):
        raise ReceiptContractError("verified association lacks the immutable compile receipt")
    if value.get("schema_version") != 1:
        raise ReceiptContractError("unsupported compile receipt schema version")
    if value.get("artifact_identity") != identity:
        raise ReceiptContractError("compile receipt identity differs from the artifact")
    for field in (
        "search_stop",
        "search_complete",
        "quality_policy_satisfied",
        "budget_limited",
        "obligations",
        "groups",
        "logical_expressions",
        "physical_expressions",
        "expected_class",
        "variant_count",
    ):
        if field not in value:
            raise ReceiptContractError(f"compile receipt lacks {field}")
    if not isinstance(value.get("omitted_variants"), int) or value["omitted_variants"] < 0:
        raise ReceiptContractError("compile receipt has invalid omitted variant count")
    if not isinstance(value.get("search_stop"), dict) or set(value["search_stop"]) != {"Observed"}:
        raise ReceiptContractError("compile receipt has no observed search stop")
    allowed_stops = {"Complete", "Incomplete", "Deadline", "BudgetLimited", "RuleFailure", "QualityPolicySatisfied"}
    if value["search_stop"]["Observed"] not in allowed_stops:
        raise ReceiptContractError("compile receipt has an unsupported search stop")
    for field in ("search_complete", "quality_policy_satisfied", "budget_limited"):
        observed = value[field]
        if not isinstance(observed, dict) or set(observed) != {"Observed"} or not isinstance(observed["Observed"], bool):
            raise ReceiptContractError(f"compile receipt has invalid {field}")
    for field in ("obligations", "groups", "logical_expressions", "physical_expressions"):
        observed = value[field]
        if not isinstance(observed, dict) or set(observed) != {"Observed"}:
            raise ReceiptContractError(f"compile receipt has invalid {field}")
        number = observed["Observed"]
        if not isinstance(number, int) or isinstance(number, bool) or number < 0:
            raise ReceiptContractError(f"compile receipt has invalid {field} value")
    for field in ("expected_class", "variant_count"):
        observed = value[field]
        if not isinstance(observed, dict):
            raise ReceiptContractError(f"compile receipt has invalid {field}")
        if isinstance(observed, dict):
            if set(observed) == {"Uncovered"}:
                if observed["Uncovered"] not in {"NotInstrumented", "FutureBoundary", "Capacity"}:
                    raise ReceiptContractError(f"compile receipt has invalid {field}")
                continue
            if set(observed) != {"Observed"}:
                raise ReceiptContractError(f"compile receipt has invalid {field}")
            number = observed["Observed"]
            if not isinstance(number, int) or isinstance(number, bool) or number < 0:
                raise ReceiptContractError(f"compile receipt has invalid {field} value")
    compile_work = value.get("compile_work")
    if compile_work is not None:
        if not isinstance(compile_work, dict) or any(
            not isinstance(compile_work.get(field), int) or compile_work[field] < 0
            for field in (
                "compiler_elapsed_us",
                "optimizer_elapsed_us",
                "rule_elapsed_us",
                "child_combination_cost_synthesis_count",
            )
        ):
            raise ReceiptContractError("compile receipt has invalid work summary")


def payload_size_bytes(payload: dict[str, Any]) -> int:
    return len(json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))
