"""Reporting and baseline comparison."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import json
import math
import os
from pathlib import Path
import platform
import shutil
import statistics
import subprocess
import sys
from typing import Any

from .executor import QueryExecutionResult, WorkloadExecutionResult
from .validator import STRONG_VALIDATE_MODES


@dataclass(frozen=True)
class RegressionEntry:
    workload: str
    query_id: str
    params: dict[str, Any]
    baseline_median: float
    current_median: float
    change_percent: float
    status: str


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
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "version": 1,
            "timestamp": datetime.now().astimezone().isoformat(timespec="seconds"),
            "git": self._collect_git_info(),
            "system": self._collect_system_info(),
            "config": {
                "iterations": iterations,
                "warmup": warmup,
                "timeout_seconds": timeout_seconds,
                "collect_memory": collect_memory,
                "collect_explain_profile": collect_explain_profile,
            },
            "workloads": [],
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
                memory_tags = _build_memory_tags_payload(query)
                spill_metrics = _build_spill_metrics_payload(query)
                query_entry: dict[str, Any] = {
                    "id": query.id,
                    "validate_mode": query.validate_mode,
                    "samples_ms": query.samples_ms,
                    "stats": stats,
                    "memory": memory,
                    "memory_tags": memory_tags,
                    "spill_metrics": spill_metrics,
                    "validation": {
                        "result": query.validation_result,
                        "detail": query.validation_detail,
                        "plan_guard": query.plan_guard,
                        "plan_detail": query.plan_guard_detail,
                    },
                    "explain_profile": {
                        "status": query.explain_profile_status,
                        "detail": query.explain_profile_detail,
                        "raw_json": query.explain_profile_raw_json,
                        "operators": query.operator_profiles,
                    },
                    "error": query.error,
                }
                workload_entry["queries"].append(query_entry)
            payload["workloads"].append(workload_entry)
        return payload

    def write_reports(self, payload: dict[str, Any], output_path: Path) -> tuple[Path, Path]:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(
            json.dumps(payload, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        summary_path = output_path.with_name("summary.md")
        summary_path.write_text(self._render_summary_markdown(payload), encoding="utf-8")
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

    def compare_with_baseline(
        self,
        current_payload: dict[str, Any],
        baseline_path: Path,
        threshold_percent: float,
    ) -> list[RegressionEntry]:
        baseline_payload = json.loads(baseline_path.read_text(encoding="utf-8"))
        baseline_index = _index_queries(baseline_payload)
        entries: list[RegressionEntry] = []

        for workload in current_payload.get("workloads", []):
            workload_name = str(workload.get("name", ""))
            params = workload.get("params", {})
            params_key = _params_key(params)
            for query in workload.get("queries", []):
                validate_mode = str(query.get("validate_mode", "none"))
                if validate_mode not in STRONG_VALIDATE_MODES:
                    continue
                validation = query.get("validation", {})
                if validation.get("result") != "PASS":
                    continue
                stats = query.get("stats") or {}
                current_median = stats.get("median")
                if not isinstance(current_median, (int, float)):
                    continue

                query_id = str(query.get("id", ""))
                key = (workload_name, query_id, params_key)
                baseline_query = baseline_index.get(key)
                if baseline_query is None:
                    continue
                baseline_stats = baseline_query.get("stats") or {}
                baseline_median = baseline_stats.get("median")
                if not isinstance(baseline_median, (int, float)) or baseline_median <= 0:
                    continue

                change = ((current_median - baseline_median) / baseline_median) * 100.0
                if change > threshold_percent:
                    status = "REGRESS"
                elif change < -threshold_percent:
                    status = "IMPROVE"
                else:
                    status = "OK"

                entries.append(
                    RegressionEntry(
                        workload=workload_name,
                        query_id=query_id,
                        params=params if isinstance(params, dict) else {},
                        baseline_median=float(baseline_median),
                        current_median=float(current_median),
                        change_percent=float(change),
                        status=status,
                    )
                )

        entries.sort(key=lambda item: (item.workload, item.query_id))
        return entries

    def print_regression(self, entries: list[RegressionEntry], threshold_percent: float) -> None:
        if not entries:
            print("Regression: no comparable queries found in baseline")
            return
        print(f"Regression threshold: ±{threshold_percent:.2f}%")
        for entry in entries:
            colored_status = self._status_text(entry.status)
            print(
                f"  {entry.workload}.{entry.query_id:<20} {colored_status:<10} "
                f"{entry.baseline_median:.2f}ms -> {entry.current_median:.2f}ms "
                f"({entry.change_percent:+.2f}%)"
            )

    def append_regression_to_summary(
        self,
        summary_path: Path,
        entries: list[RegressionEntry],
        threshold_percent: float,
    ) -> None:
        if not entries:
            return
        lines = [
            "",
            "## Regression Compare",
            "",
            f"Threshold: ±{threshold_percent:.2f}%",
            "",
            "| Workload | Query | Baseline (ms) | Current (ms) | Change | Status |",
            "|----------|-------|---------------|--------------|--------|--------|",
        ]
        for entry in entries:
            lines.append(
                "| {workload} | {query} | {base:.2f} | {curr:.2f} | {change:+.2f}% | {status} |".format(
                    workload=entry.workload,
                    query=entry.query_id,
                    base=entry.baseline_median,
                    curr=entry.current_median,
                    change=entry.change_percent,
                    status=entry.status,
                )
            )
        with summary_path.open("a", encoding="utf-8") as fp:
            fp.write("\n".join(lines) + "\n")

    def bless_baseline(self, result_path: Path, baseline_path: Path) -> None:
        baseline_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(result_path, baseline_path)

    def has_regression(self, entries: list[RegressionEntry]) -> bool:
        return any(entry.status == "REGRESS" for entry in entries)

    def query_status(self, query: QueryExecutionResult) -> str:
        if query.error:
            return "FAIL"
        if query.validation_result != "PASS":
            return "FAIL"
        if query.plan_guard == "FAIL":
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
            lines.append("| Query | Median (ms) | P90 (ms) | Validation | Plan Guard | Explain |")
            lines.append("|-------|-------------|----------|------------|------------|---------|")
            for query in workload.get("queries", []):
                stats = query.get("stats") or {}
                median = _fmt_num(stats.get("median"))
                p90 = _fmt_num(stats.get("p90"))
                validation = (query.get("validation") or {}).get("result", "n/a")
                plan_guard = (query.get("validation") or {}).get("plan_guard", "n/a")
                explain = (query.get("explain_profile") or {}).get("status", "SKIP")
                lines.append(
                    f"| {query.get('id', '')} | {median} | {p90} | {validation} | {plan_guard} | {explain} |"
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
    p90_idx = max(0, math.ceil(0.9 * n) - 1)
    if n == 1:
        stddev = 0.0
    else:
        stddev = statistics.stdev(sorted_samples)
    return {
        "min": float(sorted_samples[0]),
        "median": float(statistics.median(sorted_samples)),
        "mean": float(statistics.mean(sorted_samples)),
        "p90": float(sorted_samples[p90_idx]),
        "max": float(sorted_samples[-1]),
        "stddev": float(stddev),
    }


def _fmt_num(value: Any) -> str:
    if isinstance(value, (int, float)):
        return f"{float(value):.2f}"
    return "-"


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
        lines.append(f"- `{query_id}`: {_summarize_operator_profiles(operators)}")
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


def _index_queries(payload: dict[str, Any]) -> dict[tuple[str, str, str], dict[str, Any]]:
    index: dict[tuple[str, str, str], dict[str, Any]] = {}
    for workload in payload.get("workloads", []):
        workload_name = str(workload.get("name", ""))
        params_key = _params_key(workload.get("params", {}))
        for query in workload.get("queries", []):
            query_id = str(query.get("id", ""))
            index[(workload_name, query_id, params_key)] = query
    return index


def _params_key(params: Any) -> str:
    if isinstance(params, dict):
        return json.dumps(params, sort_keys=True, ensure_ascii=False)
    return json.dumps({})
