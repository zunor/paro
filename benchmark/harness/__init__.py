# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

"""Benchmark harness modules."""

from .executor import BenchmarkExecutor, QueryExecutionResult, WorkloadExecutionResult
from .loader import QueryDef, WorkloadDef, load_named_workload, load_workloads, select_queries_exact
from .performance_gate import (
    GateEnforcement,
    GateEntry,
    GateEntryKind,
    GateOutcome,
    GatePolicy,
    GateStatus,
    PolicyError,
    SourcePolicy,
    load_policy,
    evaluate_gate,
)
from .reporter import BenchmarkReporter
from .receipt_contract import (
    ReceiptContractError,
    validate_benchmark_payload,
    validate_compile_document,
    validate_receipt_association,
)
from .run_output import AttemptOutput, CampaignOutput, CorpusOutput, RunOutput, RunOutputError
from .validator import BenchmarkValidator, STRONG_VALIDATE_MODES

__all__ = [
    "BenchmarkExecutor",
    "BenchmarkReporter",
    "AttemptOutput",
    "CampaignOutput",
    "CorpusOutput",
    "BenchmarkValidator",
    "GateEntry",
    "GateEntryKind",
    "GateEnforcement",
    "GateOutcome",
    "GatePolicy",
    "GateStatus",
    "PolicyError",
    "ReceiptContractError",
    "RunOutput",
    "RunOutputError",
    "QueryDef",
    "QueryExecutionResult",
    "SourcePolicy",
    "STRONG_VALIDATE_MODES",
    "WorkloadDef",
    "WorkloadExecutionResult",
    "evaluate_gate",
    "validate_benchmark_payload",
    "validate_compile_document",
    "validate_receipt_association",
    "load_policy",
    "load_named_workload",
    "load_workloads",
    "select_queries_exact",
]
