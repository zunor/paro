# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

"""Benchmark reporting."""

from __future__ import annotations

from datetime import datetime
import json
import math
import os
from pathlib import Path
import platform
import statistics
import subprocess
import sys
from typing import TYPE_CHECKING, Any

from .executor import QueryExecutionResult, WorkloadExecutionResult
from .performance_gate import GateOutcome
from .run_output import CellWriter, ControlWriter, RunOutput
from .receipt_contract import (
    EVIDENCE_SCHEMA_VERSION,
    uncovered_receipt,
    validate_benchmark_payload,
    validate_summary_bytes,
)
from .runtime_contract import runtime_contract_payload

if TYPE_CHECKING:
    from .archive.calibration import ArchiveHealth


def _registered_sample_ids(
    run_output: RunOutput,
    *,
    query_case: str | None,
    arm_id: str | None,
) -> list[str]:
    if not query_case or not arm_id:
        raise ValueError("owned benchmark payload requires query_case and arm_id")
    cell_id = f"{query_case}--{arm_id}"
    for cell in run_output._manifest.get("registration", {}).get("cells", []):
        if cell.get("cell_id") == cell_id:
            sample_ids = cell.get("sample_ids")
            if isinstance(sample_ids, list) and sample_ids:
                return list(sample_ids)
    raise ValueError(f"benchmark payload has no registered cell: {cell_id}")


class BenchmarkReporter:
    def __init__(self, root_dir: Path):
        self._root_dir = root_dir
        self._color_enabled = sys.stdout.isatty() and os.getenv("NO_COLOR") is None

    def build_payload(
        self,
        *,
        workloads: list[WorkloadExecutionResult],
        iterations: int,
        warmup: int,
        timeout_seconds: int,
        collect_memory: bool,
        collect_explain_profile: bool,
        collect_compile_receipts: bool = False,
        run_output: RunOutput | None = None,
        source_id: str | None = None,
        attempt_id: str | None = None,
        query_case: str | None = None,
        arm_id: str | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "schema_version": EVIDENCE_SCHEMA_VERSION,
            "version": 3,
            "timestamp": datetime.now().astimezone().isoformat(timespec="seconds"),
            "git": self._collect_git_info(),
            "system": self._collect_system_info(),
            "runtime": runtime_contract_payload(include_environment=True),
            "config": {
                "iterations": iterations,
                "warmup": warmup,
                "timeout_seconds": timeout_seconds,
                "collect_memory": collect_memory,
                "collect_explain_profile": collect_explain_profile,
                "collect_compile_receipts": collect_compile_receipts,
            },
            "workloads": [],
        }
        registered_sample_ids: list[str] | None = None
        sample_cursor = 0
        if run_output is not None:
            registered_sample_ids = _registered_sample_ids(
                run_output, query_case=query_case, arm_id=arm_id
            )
            payload["ownership"] = {
                "schema_version": EVIDENCE_SCHEMA_VERSION,
                "run_id": run_output.run_id,
                "campaign_id": run_output.campaign_id,
                "source_id": source_id,
                "attempt_id": attempt_id,
                "query_case": query_case,
                "arm_id": arm_id,
                "sample_ids": registered_sample_ids,
                "artifact": "result.json",
                "receipt_channel": "paro_optimizers_post_statement",
                "receipt_statuses": ["Verified", "Uncovered"],
            }

        for workload in workloads:
            workload_entry: dict[str, Any] = {
                "name": workload.name,
                "params": workload.params,
                "build_time_ms": workload.build_time_ms,
                "queries": [],
            }
            for query in workload.queries:
                stats = _compute_stats(query.samples_ms)
                memory = None
                if query.memory_before_bytes is not None and query.memory_after_bytes is not None:
                    memory = {
                        "before_bytes": query.memory_before_bytes,
                        "after_bytes": query.memory_after_bytes,
                        "delta_bytes": query.memory_after_bytes - query.memory_before_bytes,
                    }
                rss = None
                if (
                    query.rss_before_kb is not None
                    or query.rss_after_kb is not None
                    or query.rss_peak_kb is not None
                ):
                    rss = {
                        "before_kb": query.rss_before_kb,
                        "after_kb": query.rss_after_kb,
                        "peak_kb": query.rss_peak_kb,
                    }
                memory_tags = _build_memory_tags_payload(query)
                spill_metrics = _build_spill_metrics_payload(query)
                receipt_associations = list(query.receipt_associations)
                if registered_sample_ids is not None:
                    sample_count = len(query.samples_ms)
                    if len(receipt_associations) > sample_count:
                        raise ValueError(
                            f"query {query.id} has more receipts than timed samples"
                        )
                    if not receipt_associations:
                        receipt_associations = [
                            uncovered_receipt(
                                "normal sample has no post-statement receipt"
                            )
                            for _ in range(sample_count)
                        ]
                    elif len(receipt_associations) < sample_count:
                        receipt_associations.extend(
                            uncovered_receipt(
                                "normal sample receipt was not observable"
                            )
                            for _ in range(
                                sample_count - len(receipt_associations)
                            )
                        )
                    end = sample_cursor + len(receipt_associations)
                    if end > len(registered_sample_ids):
                        raise ValueError(
                            "registered sample_ids do not cover benchmark samples"
                        )
                    bound_receipts = []
                    for sample_id, receipt in zip(
                        registered_sample_ids[sample_cursor:end],
                        receipt_associations,
                        strict=True,
                    ):
                        if not isinstance(receipt, dict):
                            raise ValueError(
                                f"query {query.id} has a non-object receipt"
                            )
                        bound = dict(receipt)
                        for key, value in (
                            ("sample_id", sample_id),
                            ("query_case", query_case),
                            ("arm_id", arm_id),
                        ):
                            if key in bound and bound[key] != value:
                                raise ValueError(
                                    f"query {query.id} receipt disagrees with {key}"
                                )
                            bound[key] = value
                        bound_receipts.append(bound)
                    receipt_associations = bound_receipts
                    sample_cursor = end
                query_entry: dict[str, Any] = {
                    "id": query.id,
                    "validate_mode": query.validate_mode,
                    "samples_ms": query.samples_ms,
                    "stats": stats,
                    "memory": memory,
                    "rss": rss,
                    "memory_tags": memory_tags,
                    "spill_metrics": spill_metrics,
                    "relative_median": {
                        "baseline": query.relative_median_baseline,
                        "ratio": query.relative_median_ratio,
                        "limit": query.relative_median_limit,
                    },
                    "validation": {
                        "result": query.validation_result,
                        "detail": query.validation_detail,
                        "plan_guard": query.plan_guard,
                        "plan_detail": query.plan_guard_detail,
                    },
                    "explain_profile": {
                        "status": query.explain_profile_status,
                        "detail": query.explain_profile_detail,
                        "wall_time_ms": query.explain_profile_time_ms,
                        "execution_time_ms": query.explain_profile_execution_time_ms,
                        "overhead_ratio": query.explain_profile_overhead_ratio,
                        "raw_json": query.explain_profile_raw_json,
                        "operators": query.operator_profiles,
                    },
                    "compile_receipts": receipt_associations,
                    "error": query.error,
                }
                workload_entry["queries"].append(query_entry)
            payload["workloads"].append(workload_entry)
        if registered_sample_ids is not None and sample_cursor != len(registered_sample_ids):
            raise ValueError(
                "registered sample_ids do not match the number of timed samples"
            )
        return payload

    @staticmethod
    def attach_run_ownership(
        payload: dict[str, Any],
        run_output: RunOutput,
        *,
        source_id: str | None = None,
        attempt_id: str | None = None,
        query_case: str | None = None,
        arm_id: str | None = None,
    ) -> dict[str, Any]:
        sample_ids = _registered_sample_ids(
            run_output, query_case=query_case, arm_id=arm_id
        )
        sample_cursor = 0
        for workload in payload.get("workloads", []):
            for query in workload.get("queries", []):
                if not isinstance(query, dict):
                    raise ValueError("benchmark workload query must be an object")
                raw_receipts = query.get("compile_receipts")
                if raw_receipts is None:
                    raw_receipt = query.pop("compile_receipt", None)
                    raw_receipts = [] if raw_receipt is None else [raw_receipt]
                if not isinstance(raw_receipts, list):
                    raise ValueError("query compile receipts must be a list")
                sample_values = query.get("samples_ms")
                sample_count = (
                    len(sample_values)
                    if isinstance(sample_values, list) and sample_values
                    else int(query.get("samples_count", len(raw_receipts) or 1))
                )
                if sample_count < 1 or len(raw_receipts) > sample_count:
                    raise ValueError("query receipt/sample cardinality is invalid")
                if not raw_receipts:
                    raw_receipts = [
                        uncovered_receipt(
                            "source produced no compile receipt for this sample"
                        )
                        for _ in range(sample_count)
                    ]
                elif len(raw_receipts) < sample_count:
                    raw_receipts = [
                        *raw_receipts,
                        *(
                            uncovered_receipt(
                                "source compile receipt was not observable"
                            )
                            for _ in range(sample_count - len(raw_receipts))
                        ),
                    ]
                end = sample_cursor + len(raw_receipts)
                if end > len(sample_ids):
                    raise ValueError("registered sample_ids do not cover payload samples")
                bound_receipts = []
                for sample_id, receipt in zip(
                    sample_ids[sample_cursor:end], raw_receipts, strict=True
                ):
                    if not isinstance(receipt, dict):
                        raise ValueError("query compile receipt must be an object")
                    bound = dict(receipt)
                    for key, value in (
                        ("sample_id", sample_id),
                        ("query_case", query_case),
                        ("arm_id", arm_id),
                    ):
                        if key in bound and bound[key] != value:
                            raise ValueError(f"receipt disagrees with {key}")
                        bound[key] = value
                    bound_receipts.append(bound)
                query["compile_receipts"] = bound_receipts
                sample_cursor = end
        if sample_cursor != len(sample_ids):
            raise ValueError("registered sample_ids do not match payload samples")
        payload["version"] = 3
        payload["schema_version"] = EVIDENCE_SCHEMA_VERSION
        payload["ownership"] = {
            "schema_version": EVIDENCE_SCHEMA_VERSION,
            "run_id": run_output.run_id,
            "campaign_id": run_output.campaign_id,
            "source_id": source_id,
            "attempt_id": attempt_id,
            "query_case": query_case,
            "arm_id": arm_id,
            "sample_ids": sample_ids,
            "artifact": "result.json",
            "receipt_channel": "not-configured",
            "receipt_statuses": ["Uncovered"],
        }
        payload.setdefault("config", {})["collect_compile_receipts"] = False
        return payload

    def write_reports(
        self,
        payload: dict[str, Any],
        writer: CellWriter,
        *,
        output_name: str = "result.json",
    ) -> tuple[Path, Path]:
        if payload.get("ownership") is not None:
            validate_benchmark_payload(
                payload,
                # A normal timing sample remains valid when the bounded
                # post-statement channel is unavailable.  The association is
                # then explicitly Uncovered; only a certified consumer may
                # ask validate_benchmark_payload(..., require_receipts=True).
                require_receipts=False,
            )
        output_path = writer.write_json(output_name, payload, overwrite=False)
        summary = self._render_summary_markdown(payload)
        validate_summary_bytes(summary)
        summary_path = writer.write_text("summary.md", summary, overwrite=False)
        return output_path, summary_path

    def print_terminal_summary(self, workloads: list[WorkloadExecutionResult], report_path: Path) -> None:
        total_queries = 0
        passed_queries = 0
        failed_queries = 0

        for workload in workloads:
            print(f"benchmark: {workload.name}")
            print(f"  SETUP    ... {self._stage_text(workload.setup_status)}")
            if workload.setup_error:
                print(f"            {workload.setup_error}")

            if workload.build_status != "SKIP":
                build_text = self._stage_text(workload.build_status)
                if workload.build_time_ms is not None:
                    print(f"  BUILD    ... {build_text} ({workload.build_time_ms:.2f}ms)")
                else:
                    print(f"  BUILD    ... {build_text}")
                if workload.build_error:
                    print(f"            {workload.build_error}")

            for query in workload.queries:
                total_queries += 1
                stats = _compute_stats(query.samples_ms)
                median = f"{stats['median']:.2f}ms" if stats else "n/a"
                status = self.query_status(query)
                plan = query.plan_guard
                explain = query.explain_profile_status
                line = (
                    f"  QUERY    {query.id:<16} {median:>10} "
                    f"({len(query.samples_ms)} iters)  {self._status_text(status)}  "
                    f"plan:{self._plan_text(plan)} explain:{explain}"
                )
                print(line)
                if query.error:
                    print(f"            {query.error}")
                elif query.explain_profile_status == "ERROR" and query.explain_profile_detail:
                    print(f"            explain: {query.explain_profile_detail}")
                elif query.validation_detail and query.validation_result != "PASS":
                    print(f"            {query.validation_detail}")

                if status == "PASS":
                    passed_queries += 1
                else:
                    failed_queries += 1

            print(f"  TEARDOWN ... {self._stage_text(workload.teardown_status)}")
            if workload.teardown_error:
                print(f"            {workload.teardown_error}")
            print("")

        print("-" * 50)
        print(
            f"Results: {len(workloads)} workloads, {total_queries} queries, "
            f"{passed_queries} passed, {failed_queries} failed"
        )
        print(f"Report:  {report_path}")

    def has_failures(self, workloads: list[WorkloadExecutionResult]) -> bool:
        for workload in workloads:
            if workload.setup_status != "PASS":
                return True
            if workload.build_status == "FAIL":
                return True
            if workload.teardown_status != "PASS":
                return True
            for query in workload.queries:
                if self.query_status(query) != "PASS":
                    return True
        return False

    def print_gate_outcome(self, outcome: GateOutcome) -> None:
        if not outcome.entries and outcome.archive_health is None:
            return
        print(f"Performance gate: {outcome.gate}")
        if outcome.archive_health is not None:
            self._print_archive_health(outcome.archive_health)
        for entry in outcome.entries:
            colored_status = self._status_text(entry.status)
            change = "-" if entry.change_percent is None else f"{entry.change_percent:+.2f}%"
            detail = f" ({entry.detail})" if entry.detail else ""
            print(
                f"  {entry.workload}.{entry.query_id}.{entry.metric:<24} {colored_status:<10} "
                f"{_fmt_num(entry.baseline_value)} -> {_fmt_num(entry.current_value)} "
                f"({change}){detail}"
            )

    def append_gate_outcome_to_summary(
        self,
        summary_path: Path,
        outcome: GateOutcome,
        *,
        writer: CellWriter,
    ) -> None:
        if not outcome.entries and outcome.archive_health is None:
            return
        lines = [
            "",
            f"## Performance Gate: {outcome.gate}",
            "",
        ]
        if outcome.archive_health is not None:
            lines.extend(_render_archive_health_markdown(outcome.archive_health))
        if outcome.entries:
            lines.extend([
                "",
                "| Workload | Query | Kind | Metric | Baseline | Current | Change | Status |",
                "|----------|-------|------|--------|----------|---------|--------|--------|",
            ])
        for entry in outcome.entries:
            change = "-" if entry.change_percent is None else f"{entry.change_percent:+.2f}%"
            status = entry.status.value
            lines.append(
                "| {workload} | {query} | {kind} | {metric} | {base} | {curr} | {change} | {status} |".format(
                    workload=entry.workload,
                    query=entry.query_id,
                    kind=entry.kind.value,
                    metric=entry.metric,
                    base=_fmt_num(entry.baseline_value),
                    curr=_fmt_num(entry.current_value),
                    change=change,
                    status=status if entry.detail is None else f"{status}: {entry.detail}",
                )
            )
        current = summary_path.read_text(encoding="utf-8")
        updated = current + "\n".join(lines) + "\n"
        validate_summary_bytes(updated)
        writer.write_text("summary.md", updated, overwrite=True)

    def write_gate_report(
        self,
        *,
        gate: str,
        outcomes: list[GateOutcome],
        archive_health: Any | None = None,
        writer: ControlWriter,
    ) -> Path:
        payload: dict[str, Any] = {
            "schema_version": EVIDENCE_SCHEMA_VERSION,
            "gate": gate,
            "timestamp": datetime.now().astimezone().isoformat(timespec="seconds"),
            "outcomes": [_gate_outcome_payload(outcome) for outcome in outcomes],
        }
        if archive_health is not None:
            payload["archive"] = _archive_health_payload(archive_health)
        path = writer.write_json("gate.json", payload, overwrite=False)
        return path

    def _print_archive_health(self, archive_health: "ArchiveHealth") -> None:
        status = archive_health.status.value
        suffix = ""
        if archive_health.degraded:
            suffix = (
                f" enforcement:{archive_health.policy_enforcement.value}"
                f"->{archive_health.effective_enforcement.value}"
            )
        cache = " cache:hit" if archive_health.cache_hit else ""
        print(f"  Archive   {status}{suffix}{cache}  {archive_health.message}")

    def query_status(self, query: QueryExecutionResult) -> str:
        if query.error:
            return "FAIL"
        if query.validation_result != "PASS":
            return "FAIL"
        if query.plan_guard == "FAIL":
            return "FAIL"
        if query.explain_profile_status == "ERROR":
            return "FAIL"
        return "PASS"

    def _render_summary_markdown(self, payload: dict[str, Any]) -> str:
        git = payload.get("git", {})
        timestamp = payload.get("timestamp", "")
        lines = [
            "# Paro Benchmark Report",
            "",
            f"**Date**: {timestamp}  |  **Commit**: {git.get('commit', 'unknown')}  |  "
            f"**Branch**: {git.get('branch', 'unknown')}",
            "",
        ]

        for workload in payload.get("workloads", []):
            lines.append(f"## {workload.get('name', 'unknown')}")
            lines.append("")
            lines.append(
                "| Query | P50 (ms) | P99 (ms) | P999 (ms) | QPS | RSS Peak | Validation | Plan Guard | Explain |"
            )
            lines.append(
                "|-------|----------|----------|-----------|-----|----------|------------|------------|---------|"
            )
            for query in workload.get("queries", []):
                stats = query.get("stats") or {}
                p50 = _fmt_num(stats.get("p50"))
                p99 = _fmt_num(stats.get("p99"))
                p999 = _fmt_num(stats.get("p999"))
                qps = _fmt_num(stats.get("throughput_per_second"))
                rss_peak = _fmt_rss(query.get("rss"))
                validation = (query.get("validation") or {}).get("result", "n/a")
                plan_guard = (query.get("validation") or {}).get("plan_guard", "n/a")
                explain = (query.get("explain_profile") or {}).get("status", "SKIP")
                lines.append(
                    f"| {query.get('id', '')} | {p50} | {p99} | {p999} | {qps} | {rss_peak} | {validation} | {plan_guard} | {explain} |"
                )
            explain_lines = _render_explain_summary(workload.get("queries", []))
            if explain_lines:
                lines.append("")
                lines.append("Explain profile:")
                lines.extend(explain_lines)
            memory_lines = _render_memory_summary(workload.get("queries", []))
            if memory_lines:
                lines.append("")
                lines.append("Memory tags:")
                lines.extend(memory_lines)
            spill_lines = _render_spill_summary(workload.get("queries", []))
            if spill_lines:
                lines.append("")
                lines.append("Spill metrics:")
                lines.extend(spill_lines)
            build_time_ms = workload.get("build_time_ms")
            if isinstance(build_time_ms, (int, float)):
                lines.append("")
                lines.append(f"Build time: {build_time_ms:.2f} ms")
            lines.append("")
        return "\n".join(lines).rstrip() + "\n"

    def _collect_git_info(self) -> dict[str, str]:
        commit = _git("rev-parse", "--short", "HEAD", root_dir=self._root_dir) or "unknown"
        branch = _git("rev-parse", "--abbrev-ref", "HEAD", root_dir=self._root_dir) or "unknown"
        return {"commit": commit, "branch": branch}

    def _collect_system_info(self) -> dict[str, Any]:
        return {
            "os": f"{platform.system()} {platform.release()}",
            "arch": platform.machine(),
            "cpu": platform.processor() or "unknown",
            "cores": os.cpu_count() or 0,
            "memory_gb": _physical_memory_gb(),
        }

    def _stage_text(self, status: str) -> str:
        if status == "PASS":
            return self._color(status, "32")
        if status == "FAIL":
            return self._color(status, "31")
        return status

    def _status_text(self, status: str) -> str:
        status = getattr(status, "value", status)
        if status in {"PASS", "OK"}:
            return self._color(status, "32")
        if status == "IMPROVE":
            return self._color(status, "36")
        if status in {"FAIL", "REGRESS"}:
            return self._color(status, "31")
        return status

    def _plan_text(self, status: str) -> str:
        if status == "PASS":
            return self._color("PASS", "32")
        if status == "FAIL":
            return self._color("FAIL", "31")
        return status

    def _color(self, text: str, code: str) -> str:
        if not self._color_enabled:
            return text
        return f"\033[{code}m{text}\033[0m"


def _compute_stats(samples: list[float]) -> dict[str, float] | None:
    if not samples:
        return None
    sorted_samples = sorted(samples)
    n = len(sorted_samples)
    mean = float(statistics.mean(sorted_samples))
    if n == 1:
        stddev = 0.0
    else:
        stddev = statistics.stdev(sorted_samples)
    median = float(statistics.median(sorted_samples))
    return {
        "min": float(sorted_samples[0]),
        "median": median,
        "p50": median,
        "mean": mean,
        "p90": _percentile(sorted_samples, 0.9),
        "p99": _percentile(sorted_samples, 0.99),
        "p999": _percentile(sorted_samples, 0.999),
        "max": float(sorted_samples[-1]),
        "stddev": float(stddev),
        "throughput_per_second": 1000.0 / mean if mean > 0 else 0.0,
    }


def _percentile(sorted_samples: list[float], quantile: float) -> float:
    index = max(0, math.ceil(quantile * len(sorted_samples)) - 1)
    return float(sorted_samples[min(index, len(sorted_samples) - 1)])


def _fmt_num(value: Any) -> str:
    if isinstance(value, (int, float)):
        return f"{float(value):.2f}"
    return "-"


def _fmt_rss(value: Any) -> str:
    if not isinstance(value, dict):
        return "-"
    peak_kb = value.get("peak_kb")
    if not isinstance(peak_kb, int) or isinstance(peak_kb, bool):
        return "-"
    return _fmt_bytes(peak_kb * 1024)


def _gate_outcome_payload(outcome: GateOutcome) -> dict[str, Any]:
    return {
        "gate": outcome.gate,
        "enforcement": outcome.enforcement.value,
        "failed": outcome.failed,
        "blocking_failed": outcome.blocking_failed,
        "entries": [
            {
                "kind": entry.kind.value,
                "workload": entry.workload,
                "query": entry.query_id,
                "metric": entry.metric,
                "status": entry.status.value,
                "baseline_value": entry.baseline_value,
                "current_value": entry.current_value,
                "change_percent": entry.change_percent,
                "p_value": entry.p_value,
                "holm_alpha": entry.holm_alpha,
                "noise_floor_abs": entry.noise_floor_abs,
                "noise_floor_percent": entry.noise_floor_percent,
                "calibration_samples": entry.calibration_samples,
                "statistical_power": entry.statistical_power,
                "detail": entry.detail,
            }
            for entry in outcome.entries
        ],
    }


def _archive_health_payload(archive_health: "ArchiveHealth") -> dict[str, Any]:
    return {
        "status": archive_health.status.value,
        "message": archive_health.message,
        "policy_enforcement": archive_health.policy_enforcement.value,
        "effective_enforcement": archive_health.effective_enforcement.value,
        "manifest_path": archive_health.manifest_path,
        "calibration_path": archive_health.calibration_path,
        "cache_hit": archive_health.cache_hit,
        "clean_observations": archive_health.clean_observations,
        "required_observations": archive_health.required_observations,
    }


def _render_archive_health_markdown(archive_health: "ArchiveHealth") -> list[str]:
    lines = [
        "Archive health:",
        f"- Status: `{archive_health.status.value}`",
        f"- Message: {archive_health.message}",
        (
            f"- Enforcement: `{archive_health.policy_enforcement.value}`"
            f" -> `{archive_health.effective_enforcement.value}`"
        ),
        f"- Cache hit: `{str(archive_health.cache_hit).lower()}`",
    ]
    if archive_health.manifest_path:
        lines.append(f"- Manifest: `{archive_health.manifest_path}`")
    if archive_health.calibration_path:
        lines.append(f"- Calibration: `{archive_health.calibration_path}`")
    if archive_health.required_observations:
        lines.append(
            f"- Observations: `{archive_health.clean_observations}/{archive_health.required_observations}`"
        )
    return lines


def _render_explain_summary(queries: Any) -> list[str]:
    if not isinstance(queries, list):
        return []
    lines: list[str] = []
    for query in queries:
        if not isinstance(query, dict):
            continue
        query_id = str(query.get("id", ""))
        explain = query.get("explain_profile")
        if not isinstance(explain, dict):
            continue
        status = str(explain.get("status", "SKIP"))
        if status == "SKIP":
            continue
        if status == "ERROR":
            detail = str(explain.get("detail", "unknown error"))
            lines.append(f"- `{query_id}`: explain profile error: {detail}")
            continue
        operators = explain.get("operators")
        if not isinstance(operators, list):
            lines.append(f"- `{query_id}`: explain profile collected, but operator list is unavailable")
            continue
        summary = _summarize_operator_profiles(operators)
        ratio = explain.get("overhead_ratio")
        if isinstance(ratio, (int, float)) and not isinstance(ratio, bool):
            summary = f"{summary}; profile/primary execution ratio: `{ratio:.2f}x`"
        lines.append(f"- `{query_id}`: {summary}")
    return lines


def _render_memory_summary(queries: Any) -> list[str]:
    if not isinstance(queries, list):
        return []
    lines: list[str] = []
    for query in queries:
        if not isinstance(query, dict):
            continue
        query_id = str(query.get("id", ""))
        payload = query.get("memory_tags")
        if not isinstance(payload, dict):
            continue
        delta = payload.get("delta")
        if not isinstance(delta, list):
            continue
        summary = _summarize_memory_tag_delta(delta)
        if summary:
            lines.append(f"- `{query_id}`: {summary}")
    return lines


def _render_spill_summary(queries: Any) -> list[str]:
    if not isinstance(queries, list):
        return []
    lines: list[str] = []
    for query in queries:
        if not isinstance(query, dict):
            continue
        query_id = str(query.get("id", ""))
        payload = query.get("spill_metrics")
        if not isinstance(payload, dict):
            continue
        delta = payload.get("delta")
        if not isinstance(delta, dict):
            continue
        summary = _summarize_spill_delta(delta)
        if summary:
            lines.append(f"- `{query_id}`: {summary}")
    return lines


def _summarize_operator_profiles(operators: list[Any]) -> str:
    spilled_ops: list[str] = []
    max_memory_bytes: int | None = None
    max_memory_operator: str | None = None

    for operator in operators:
        if not isinstance(operator, dict):
            continue
        operator_name = str(operator.get("operator", "") or "UNKNOWN")
        if operator.get("spilled") is True and operator_name not in spilled_ops:
            spilled_ops.append(operator_name)
        memory_bytes = operator.get("reported_memory_bytes")
        if isinstance(memory_bytes, int) and not isinstance(memory_bytes, bool):
            if max_memory_bytes is None or memory_bytes > max_memory_bytes:
                max_memory_bytes = memory_bytes
                max_memory_operator = operator_name

    parts: list[str] = []
    if spilled_ops:
        parts.append("spilled operators: " + ", ".join(f"`{name}`" for name in spilled_ops))
    else:
        parts.append("no spilled operators reported")

    if max_memory_bytes is not None:
        parts.append(
            "max reported memory: "
            f"`{_fmt_bytes(max_memory_bytes)}` at `{max_memory_operator or 'UNKNOWN'}`"
        )
    return "; ".join(parts)


def _build_memory_tags_payload(query: QueryExecutionResult) -> dict[str, Any] | None:
    if query.memory_tags_before is None or query.memory_tags_after is None:
        return None
    return {
        "before": query.memory_tags_before,
        "after": query.memory_tags_after,
        "delta": _build_memory_tag_delta(query.memory_tags_before, query.memory_tags_after),
    }


def _build_spill_metrics_payload(query: QueryExecutionResult) -> dict[str, Any] | None:
    if query.spill_metrics_before is None or query.spill_metrics_after is None:
        return None
    return {
        "before": query.spill_metrics_before,
        "after": query.spill_metrics_after,
        "delta": _build_spill_metrics_delta(query.spill_metrics_before, query.spill_metrics_after),
    }


def _build_memory_tag_delta(
    before: list[dict[str, Any]],
    after: list[dict[str, Any]],
) -> list[dict[str, int | str]]:
    before_map = _memory_tag_map(before)
    after_map = _memory_tag_map(after)
    tags = sorted(set(before_map) | set(after_map))
    delta_rows: list[dict[str, int | str]] = []
    for tag in tags:
        before_value = before_map.get(tag, 0)
        after_value = after_map.get(tag, 0)
        delta_rows.append(
            {
                "tag": tag,
                "before_bytes": before_value,
                "after_bytes": after_value,
                "delta_bytes": after_value - before_value,
            }
        )
    return delta_rows


def _build_spill_metrics_delta(
    before: dict[str, int],
    after: dict[str, int],
) -> dict[str, int]:
    keys = sorted(set(before) | set(after))
    return {key: int(after.get(key, 0)) - int(before.get(key, 0)) for key in keys}


def _memory_tag_map(rows: list[dict[str, Any]]) -> dict[str, int]:
    result: dict[str, int] = {}
    for row in rows:
        tag = row.get("tag")
        memory_usage = row.get("memory_usage_bytes")
        if isinstance(tag, str) and isinstance(memory_usage, int) and not isinstance(memory_usage, bool):
            result[tag] = int(memory_usage)
    return result


def _summarize_memory_tag_delta(delta_rows: list[Any]) -> str | None:
    best_tag: str | None = None
    best_delta = 0
    for row in delta_rows:
        if not isinstance(row, dict):
            continue
        tag = row.get("tag")
        delta = row.get("delta_bytes")
        if not isinstance(tag, str):
            continue
        if not isinstance(delta, int) or isinstance(delta, bool):
            continue
        if best_tag is None or abs(delta) > abs(best_delta):
            best_tag = tag
            best_delta = delta
    if best_tag is None:
        return None
    return f"largest tag delta: `{best_tag}` {_fmt_signed_bytes(best_delta)}"


def _summarize_spill_delta(delta: dict[str, Any]) -> str | None:
    parts: list[str] = []
    for key in ("swap_usage", "write_bytes", "read_bytes", "file_count", "swap_limit_hits"):
        value = delta.get(key)
        if not isinstance(value, int) or isinstance(value, bool):
            continue
        if value == 0:
            continue
        if key.endswith("_bytes") or key == "swap_usage":
            parts.append(f"`{key}` {_fmt_signed_bytes(value)}")
        else:
            parts.append(f"`{key}` {value:+d}")
    if not parts:
        parts.append("no spill metric delta")
    return "; ".join(parts)


def _fmt_bytes(value: int) -> str:
    units = ["B", "KiB", "MiB", "GiB", "TiB"]
    size = float(value)
    unit_index = 0
    while size >= 1024.0 and unit_index < len(units) - 1:
        size /= 1024.0
        unit_index += 1
    if unit_index == 0:
        return f"{int(size)} {units[unit_index]}"
    return f"{size:.2f} {units[unit_index]}"


def _fmt_signed_bytes(value: int) -> str:
    if value == 0:
        return "0 B"
    sign = "+" if value > 0 else "-"
    return f"{sign}{_fmt_bytes(abs(value))}"


def _git(*args: str, root_dir: Path) -> str | None:
    try:
        output = subprocess.check_output(
            ["git", "-C", str(root_dir), *args],
            stderr=subprocess.DEVNULL,
            text=True,
        )
    except Exception:
        return None
    value = output.strip()
    return value or None


def _physical_memory_gb() -> float:
    if hasattr(os, "sysconf"):
        try:
            pages = os.sysconf("SC_PHYS_PAGES")
            page_size = os.sysconf("SC_PAGE_SIZE")
            if isinstance(pages, int) and isinstance(page_size, int):
                return round((pages * page_size) / (1024**3), 2)
        except (OSError, ValueError):
            pass
    return 0.0
