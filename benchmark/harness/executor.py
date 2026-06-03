# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

"""Workload execution and timing."""

from __future__ import annotations

from contextlib import suppress
from dataclasses import dataclass, field
from datetime import date, datetime, time
from decimal import Decimal
import json
import subprocess
import threading
import time as time_module
from typing import Any, Mapping

from .loader import QueryDef, WorkloadDef
from .validator import BenchmarkValidator


VECTOR_SIZE = 2048


class QueryTimeoutError(RuntimeError):
    """Raised when query execution exceeds timeout."""


class ExplainProfileUnsupported(RuntimeError):
    """Raised when a query shape cannot support an explain sidecar."""


@dataclass
class QueryExecutionResult:
    id: str
    validate_mode: str
    expected: Any
    samples_ms: list[float] = field(default_factory=list)
    result_rows: list[list[Any]] = field(default_factory=list)
    memory_before_bytes: int | None = None
    memory_after_bytes: int | None = None
    memory_tags_before: list[dict[str, Any]] | None = None
    memory_tags_after: list[dict[str, Any]] | None = None
    spill_metrics_before: dict[str, int] | None = None
    spill_metrics_after: dict[str, int] | None = None
    validation_result: str = "SKIP"
    validation_detail: str | None = None
    plan_guard: str = "SKIP"
    plan_guard_detail: str | None = None
    explain_profile_status: str = "SKIP"
    explain_profile_detail: str | None = None
    explain_profile_raw_json: str | None = None
    explain_profile_time_ms: float | None = None
    explain_profile_execution_time_ms: float | None = None
    explain_profile_overhead_ratio: float | None = None
    operator_profiles: list[dict[str, Any]] = field(default_factory=list)
    rss_before_kb: int | None = None
    rss_after_kb: int | None = None
    rss_peak_kb: int | None = None
    error: str | None = None


@dataclass
class WorkloadExecutionResult:
    name: str
    params: dict[str, Any]
    build_time_ms: float | None = None
    build_status: str = "SKIP"
    build_error: str | None = None
    setup_status: str = "PASS"
    setup_error: str | None = None
    teardown_status: str = "PASS"
    teardown_error: str | None = None
    queries: list[QueryExecutionResult] = field(default_factory=list)


class BenchmarkExecutor:
    def __init__(
        self,
        *,
        connection: Mapping[str, Any],
        iterations: int,
        warmup: int,
        timeout_seconds: int,
        collect_memory: bool,
        profile_pid: int = 0,
    ):
        self._connection = dict(connection)
        self._iterations = max(int(iterations), 1)
        self._warmup = max(int(warmup), 0)
        self._timeout_seconds = max(int(timeout_seconds), 1)
        self._collect_memory = bool(collect_memory)
        self._profile_pid = max(int(profile_pid), 0)

    def connection_factory(self) -> Any:
        try:
            import psycopg
        except ImportError as exc:  # pragma: no cover - runtime dependency
            raise RuntimeError("missing dependency: psycopg[binary]>=3.2,<4") from exc

        conn = psycopg.connect(
            host=self._connection.get("host", "localhost"),
            port=self._connection.get("port", 6432),
            dbname=self._connection.get("database", "postgres"),
            user=self._connection.get("user", "postgres"),
            password=self._connection.get("password", ""),
            prepare_threshold=0,
        )
        conn.autocommit = True
        return conn

    def execute_script(self, conn: Any, script: str) -> None:
        self._execute_script(conn, script)

    def execute_sql(self, conn: Any, sql: str, *, fetch: bool) -> list[tuple[Any, ...]]:
        return self._execute_sql(conn, sql, fetch=fetch)

    def run_workload(
        self,
        workload: WorkloadDef,
        validator: BenchmarkValidator,
    ) -> WorkloadExecutionResult:
        result = WorkloadExecutionResult(name=workload.name, params=dict(workload.params))
        conn = None
        try:
            conn = self.connection_factory()
            self._execute_script(conn, workload.setup_sql)
        except Exception as exc:
            result.setup_status = "FAIL"
            result.setup_error = _format_error(exc)
            _safe_close(conn)
            conn = None

        if conn is not None and workload.build_sql:
            try:
                build_start = time_module.perf_counter()
                self._execute_script(conn, workload.build_sql)
                result.build_time_ms = (time_module.perf_counter() - build_start) * 1000.0
                result.build_status = "PASS"
            except Exception as exc:
                result.build_status = "FAIL"
                result.build_error = _format_error(exc)
                _safe_close(conn)
                conn = None

        if conn is not None:
            for query in workload.queries:
                query_result = self._run_query(conn, query, validator)
                result.queries.append(query_result)
                if query_result.error and query_result.error.startswith("TIMEOUT:"):
                    # Timeout closes connection. Remaining queries are skipped for this workload.
                    conn = None
                    break
        else:
            for query in workload.queries:
                result.queries.append(
                    QueryExecutionResult(
                        id=query.id,
                        validate_mode=query.validate,
                        expected=query.expected,
                        validation_result="FAIL",
                        validation_detail="setup/build failed",
                        error="SKIPPED: setup/build stage failed",
                    )
                )

        teardown_conn = None
        try:
            teardown_conn = conn if conn is not None else self.connection_factory()
            self._execute_script(teardown_conn, workload.teardown_sql)
        except Exception as exc:
            result.teardown_status = "FAIL"
            result.teardown_error = _format_error(exc)
        finally:
            _safe_close(conn)
            if teardown_conn is not None and teardown_conn is not conn:
                _safe_close(teardown_conn)

        return result

    def _run_query(
        self,
        conn: Any,
        query: QueryDef,
        validator: BenchmarkValidator,
    ) -> QueryExecutionResult:
        query_result = QueryExecutionResult(
            id=query.id,
            validate_mode=query.validate,
            expected=query.expected,
        )

        try:
            if query.setup_sql:
                self._execute_script(conn, query.setup_sql)

            plan_guard = validator.check_plan(query, conn)
            query_result.plan_guard = plan_guard.status
            query_result.plan_guard_detail = plan_guard.detail

            for _ in range(self._warmup):
                self._execute_sql(conn, query.sql, fetch=True)

            if self._collect_memory:
                query_result.memory_tags_before = self._fetch_memory_tag_rows(conn)
                query_result.memory_before_bytes = _sum_memory_tag_rows(query_result.memory_tags_before)
                query_result.spill_metrics_before = self._fetch_spill_metrics(conn)

            if self._profile_pid > 0:
                query_result.rss_before_kb = _read_process_rss_kb(self._profile_pid)

            last_rows: list[tuple[Any, ...]] = []
            for _ in range(self._iterations):
                rss_sampler = RssSampler(self._profile_pid)
                rss_sampler.start()
                start = time_module.perf_counter()
                try:
                    rows = self._execute_sql(conn, query.sql, fetch=True)
                finally:
                    elapsed_ms = (time_module.perf_counter() - start) * 1000.0
                    rss_sampler.stop()
                query_result.samples_ms.append(elapsed_ms)
                if rss_sampler.peak_kb is not None:
                    query_result.rss_peak_kb = max(
                        query_result.rss_peak_kb or 0,
                        rss_sampler.peak_kb,
                    )
                last_rows = rows

            if self._collect_memory:
                query_result.memory_tags_after = self._fetch_memory_tag_rows(conn)
                query_result.memory_after_bytes = _sum_memory_tag_rows(query_result.memory_tags_after)
                query_result.spill_metrics_after = self._fetch_spill_metrics(conn)

            if self._profile_pid > 0:
                query_result.rss_after_kb = _read_process_rss_kb(self._profile_pid)

            query_result.result_rows = [_normalize_row(row) for row in last_rows]
            outcome = validator.validate_query(query, query_result.result_rows)
            query_result.validation_result = outcome.status
            query_result.validation_detail = outcome.detail
            self._collect_explain_profile(conn, query, query_result)
        except QueryTimeoutError as exc:
            query_result.error = f"TIMEOUT: {exc}"
            query_result.validation_result = "FAIL"
            query_result.validation_detail = str(exc)
            query_result.explain_profile_status = "SKIP"
            query_result.explain_profile_detail = "primary query timed out"
        except Exception as exc:
            query_result.error = _format_error(exc)
            query_result.validation_result = "FAIL"
            query_result.validation_detail = _format_error(exc)
            query_result.explain_profile_status = "SKIP"
            query_result.explain_profile_detail = "primary query failed"
        finally:
            if query.teardown_sql:
                try:
                    self._execute_script(conn, query.teardown_sql)
                except Exception as exc:
                    if query_result.error is None:
                        query_result.error = f"QUERY TEARDOWN: {_format_error(exc)}"
                        query_result.validation_result = "FAIL"
                        query_result.validation_detail = _format_error(exc)
        return query_result

    def _collect_explain_profile(
        self,
        conn: Any,
        query: QueryDef,
        query_result: QueryExecutionResult,
    ) -> None:
        if not query.collect_explain_profile:
            query_result.explain_profile_status = "SKIP"
            query_result.explain_profile_detail = "collect_explain_profile disabled"
            return
        if not query.allow_reexecute:
            query_result.explain_profile_status = "SKIP"
            query_result.explain_profile_detail = "allow_reexecute is false"
            return

        try:
            started_at = time_module.perf_counter()
            raw_json = self._fetch_explain_profile_json(conn, query.sql)
            query_result.explain_profile_time_ms = (
                time_module.perf_counter() - started_at
            ) * 1000.0
            query_result.explain_profile_execution_time_ms = (
                _extract_explain_execution_time_ms(raw_json)
            )
            primary_median = _median(query_result.samples_ms)
            if (
                primary_median is not None
                and primary_median > 0.0
                and query_result.explain_profile_execution_time_ms is not None
            ):
                query_result.explain_profile_overhead_ratio = (
                    query_result.explain_profile_execution_time_ms / primary_median
                )
            query_result.explain_profile_raw_json = raw_json
            query_result.operator_profiles = _flatten_explain_profile(raw_json)
            query_result.explain_profile_status = "PASS"
            query_result.explain_profile_detail = None
        except ExplainProfileUnsupported as exc:
            query_result.explain_profile_status = "SKIP"
            query_result.explain_profile_detail = str(exc)
        except Exception as exc:
            query_result.explain_profile_status = "ERROR"
            query_result.explain_profile_detail = _format_error(exc)

    def _fetch_explain_profile_json(self, conn: Any, sql: str) -> str:
        explain_sql = _build_explain_analyze_sql(sql)
        rows = self._execute_sql(conn, explain_sql, fetch=True)
        if not rows or not rows[0]:
            raise ValueError("EXPLAIN ANALYZE FORMAT JSON returned no rows")
        raw = rows[0][0]
        if not isinstance(raw, str) or not raw.strip():
            raise ValueError("EXPLAIN ANALYZE FORMAT JSON returned empty payload")
        return raw

    def _execute_script(self, conn: Any, script: str) -> None:
        for statement in _split_sql_statements(script):
            self._execute_sql(conn, statement, fetch=False)

    def _execute_sql(self, conn: Any, sql: str, *, fetch: bool) -> list[tuple[Any, ...]]:
        holder: dict[str, Any] = {}

        def worker() -> None:
            try:
                with conn.cursor() as cur:
                    cur.execute(sql, prepare=False)
                    if fetch and cur.description is not None:
                        holder["rows"] = cur.fetchall()
                    else:
                        holder["rows"] = []
            except BaseException as exc:  # pragma: no cover - thread boundary
                holder["error"] = exc

        thread = threading.Thread(target=worker, daemon=True)
        thread.start()
        thread.join(self._timeout_seconds)
        if thread.is_alive():
            with suppress(Exception):
                conn.close()
            thread.join(0.2)
            raise QueryTimeoutError(f"query exceeded {self._timeout_seconds}s")
        if "error" in holder:
            raise holder["error"]
        rows = holder.get("rows")
        if isinstance(rows, list):
            return rows
        return []

    def _fetch_memory_tag_rows(self, conn: Any) -> list[dict[str, Any]]:
        try:
            rows = self._execute_sql(
                conn,
                "SELECT tag, memory_usage_bytes FROM paro_memory() ORDER BY tag",
                fetch=True,
            )
            tag_rows: list[dict[str, Any]] = []
            for row in rows:
                if len(row) < 2:
                    continue
                tag = row[0]
                bytes_value = row[1]
                if not isinstance(tag, str):
                    continue
                if isinstance(bytes_value, bool) or not isinstance(bytes_value, int):
                    continue
                tag_rows.append({"tag": tag, "memory_usage_bytes": int(bytes_value)})
            return tag_rows
        except Exception:
            return []

    def _fetch_spill_metrics(self, conn: Any) -> dict[str, int] | None:
        try:
            rows = self._execute_sql(
                conn,
                "SELECT write_bytes, read_bytes, file_count, swap_usage, swap_limit_hits "
                "FROM paro_temporary_files() LIMIT 1",
                fetch=True,
            )
            if not rows:
                return {
                    "write_bytes": 0,
                    "read_bytes": 0,
                    "file_count": 0,
                    "swap_usage": 0,
                    "swap_limit_hits": 0,
                }
            row = rows[0]
            if len(row) < 5:
                return None
            values: list[int] = []
            for index in range(5):
                value = row[index]
                if isinstance(value, bool) or not isinstance(value, int):
                    return None
                values.append(int(value))
            return {
                "write_bytes": values[0],
                "read_bytes": values[1],
                "file_count": values[2],
                "swap_usage": values[3],
                "swap_limit_hits": values[4],
            }
        except Exception:
            return None


def _normalize_row(row: tuple[Any, ...]) -> list[Any]:
    return [_normalize_value(value) for value in row]


def _build_explain_analyze_sql(sql: str) -> str:
    stripped = sql.strip()
    if not stripped:
        raise ValueError("query SQL is empty")
    while stripped.endswith(";"):
        stripped = stripped[:-1].rstrip()
    if stripped.upper().startswith("EXPLAIN"):
        raise ExplainProfileUnsupported("query already starts with EXPLAIN; explain sidecar skipped")
    return f"EXPLAIN ANALYZE {stripped} FORMAT JSON"


def _sum_memory_tag_rows(rows: list[dict[str, Any]] | None) -> int:
    if not rows:
        return 0
    total = 0
    for row in rows:
        memory_usage = row.get("memory_usage_bytes")
        if isinstance(memory_usage, int) and not isinstance(memory_usage, bool):
            total += memory_usage
    return total


def _median(samples: list[float]) -> float | None:
    if not samples:
        return None
    ordered = sorted(float(sample) for sample in samples)
    mid = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[mid]
    return (ordered[mid - 1] + ordered[mid]) / 2.0


def _extract_explain_execution_time_ms(raw_json: str) -> float | None:
    try:
        document = json.loads(raw_json)
    except json.JSONDecodeError:
        return None
    if not isinstance(document, dict):
        return None
    summary = document.get("summary")
    if not isinstance(summary, dict):
        return None
    execution_time = summary.get("Execution Time")
    if isinstance(execution_time, (int, float)) and not isinstance(execution_time, bool):
        return float(execution_time)
    if not isinstance(execution_time, str):
        return None
    first_token = execution_time.strip().split(" ", 1)[0]
    try:
        return float(first_token)
    except ValueError:
        return None


class RssSampler:
    def __init__(self, pid: int, interval_seconds: float = 0.05):
        self._pid = max(int(pid), 0)
        self._interval_seconds = max(float(interval_seconds), 0.01)
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self.peak_kb: int | None = None

    def start(self) -> None:
        if self._pid <= 0:
            return
        self._record(_read_process_rss_kb(self._pid))
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        if self._thread is None:
            return
        self._stop.set()
        self._thread.join(0.2)
        self._record(_read_process_rss_kb(self._pid))

    def _run(self) -> None:
        while not self._stop.wait(self._interval_seconds):
            self._record(_read_process_rss_kb(self._pid))

    def _record(self, rss_kb: int | None) -> None:
        if rss_kb is None:
            return
        self.peak_kb = max(self.peak_kb or 0, rss_kb)


def _read_process_rss_kb(pid: int) -> int | None:
    if pid <= 0:
        return None
    try:
        output = subprocess.check_output(
            ["ps", "-o", "rss=", "-p", str(pid)],
            stderr=subprocess.DEVNULL,
            text=True,
        )
    except Exception:
        return None
    value = output.strip().splitlines()
    if not value:
        return None
    try:
        rss = int(value[0].strip())
    except ValueError:
        return None
    return rss if rss > 0 else None


def _flatten_explain_profile(raw_json: str) -> list[dict[str, Any]]:
    try:
        document = json.loads(raw_json)
    except json.JSONDecodeError as exc:
        raise ValueError(f"invalid explain JSON: {exc}") from exc
    if not isinstance(document, dict):
        raise ValueError("explain JSON document must be an object")
    operators = document.get("operators")
    profile_fields = _document_profile_fields(document)
    if isinstance(operators, list):
        profiles: list[dict[str, Any]] = []
        for index, operator in enumerate(operators):
            if not isinstance(operator, dict):
                continue
            actual = operator.get("actual")
            actual_map = actual if isinstance(actual, dict) else {}
            profiles.append(
                _operator_profile_entry(
                    node_id=_optional_int(operator.get("runtime_id")),
                    operator=str(operator.get("operator", "")),
                    tree_path=str(index),
                    actual_map=actual_map,
                    profile_fields=profile_fields,
                )
            )
        return profiles
    plan = document.get("plan")
    if not isinstance(plan, dict):
        raise ValueError("explain JSON document missing 'operators' array or object field 'plan'")
    profiles: list[dict[str, Any]] = []
    _append_operator_profile(plan, profiles, "0", profile_fields)
    return profiles


def _document_profile_fields(document: Mapping[str, Any]) -> dict[str, Any]:
    profile_events = document.get("profile_events")
    profile_summary = document.get("profile")
    profile_map = profile_summary if isinstance(profile_summary, dict) else {}
    parallelism = profile_map.get("parallelism")
    parallel_map = parallelism if isinstance(parallelism, dict) else {}
    memory = profile_map.get("memory")
    memory_map = memory if isinstance(memory, dict) else {}
    runtime_filters = profile_map.get("runtime_filters")
    filter_map = runtime_filters if isinstance(runtime_filters, dict) else {}
    return {
        "profile_schema_version": _optional_int(document.get("profile_schema_version")),
        "query_id": _optional_int(document.get("query_id")),
        "profile_event_count": len(profile_events) if isinstance(profile_events, list) else None,
        "profile_parallelism": _optional_int(parallel_map.get("max_threads")),
        "profile_observed_workers": _optional_int(parallel_map.get("observed_workers")),
        "profile_worker_utilization": _optional_float(parallel_map.get("worker_utilization")),
        "profile_ready_time_us": _optional_int(parallel_map.get("ready_time_us")),
        "profile_wait_time_us": _optional_int(parallel_map.get("wait_time_us")),
        "profile_backpressure_count": _optional_int(parallel_map.get("backpressure_count")),
        "profile_runtime_filter_installed_count": _optional_int(filter_map.get("installed_count")),
        "profile_runtime_filter_no_wait_count": _optional_int(
            filter_map.get("no_wait_fallback_count")
        ),
        "profile_grant_bytes": _optional_int(memory_map.get("grant_bytes")),
        "profile_revoked_bytes": _optional_int(memory_map.get("revoked_bytes")),
        "profile_spill_bytes": _optional_int(memory_map.get("spill_bytes")),
        "profile_yield_latency_us": _optional_int(memory_map.get("yield_latency_us")),
    }


def _operator_profile_entry(
    *,
    node_id: int | None,
    operator: str,
    tree_path: str,
    actual_map: Mapping[str, Any],
    profile_fields: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "node_id": node_id,
        "operator": operator,
        "tree_path": tree_path,
        "rows": _optional_int(actual_map.get("rows")),
        "loops": _optional_int(actual_map.get("loops")),
        "startup_time_ms": _optional_float(actual_map.get("startup_time_ms")),
        "total_time_ms": _optional_float(actual_map.get("total_time_ms")),
        "spilled": _optional_bool(actual_map.get("spilled")),
        "reported_memory_bytes": _optional_int(actual_map.get("peak_memory_bytes")),
        "temp_storage_bytes": _optional_int(actual_map.get("temp_storage_bytes")),
        "spilled_bytes": _optional_int(actual_map.get("spilled_bytes")),
        "spill_latency_us": _optional_int(actual_map.get("spill_latency_us")),
        "scheduler_worker_count": _optional_int(actual_map.get("scheduler_worker_count")),
        "scheduler_morsel_count": _optional_int(actual_map.get("scheduler_morsel_count")),
        "scheduler_ready_time_us": _optional_int(actual_map.get("scheduler_ready_time_us")),
        "scheduler_wait_time_us": _optional_int(actual_map.get("scheduler_wait_time_us")),
        "output_backpressure_count": _optional_int(
            actual_map.get("output_backpressure_count")
        ),
        "runtime_filter_installed_count": _optional_int(
            actual_map.get("runtime_filter_installed_count")
        ),
        "allocator_tracking_event_count": _optional_int(
            actual_map.get("allocator_tracking_event_count")
        ),
        "allocator_tracking_events_per_chunk": _per_chunk(
            actual_map.get("allocator_tracking_event_count"),
            actual_map.get("rows"),
        ),
        **profile_fields,
    }


def _append_operator_profile(
    node: Mapping[str, Any],
    profiles: list[dict[str, Any]],
    tree_path: str,
    profile_fields: Mapping[str, Any],
) -> None:
    actual = node.get("actual")
    actual_map = actual if isinstance(actual, dict) else {}

    profiles.append(
        _operator_profile_entry(
            node_id=_optional_int(node.get("node_id")),
            operator=str(node.get("operator", "")),
            tree_path=tree_path,
            actual_map=actual_map,
            profile_fields=profile_fields,
        )
    )

    children = node.get("children")
    if not isinstance(children, list):
        return
    for index, child in enumerate(children):
        if isinstance(child, dict):
            _append_operator_profile(child, profiles, f"{tree_path}/{index}", profile_fields)


def _optional_int(value: Any) -> int | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    return None


def _optional_float(value: Any) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    return None


def _optional_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    return None


def _per_chunk(numerator: Any, rows: Any, chunk_size: int = VECTOR_SIZE) -> float | None:
    count = _optional_int(numerator)
    row_count = _optional_int(rows)
    if count is None or row_count is None or row_count <= 0:
        return None
    return (float(count) * float(chunk_size)) / float(row_count)


def _normalize_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (date, datetime, time)):
        return value.isoformat()
    if isinstance(value, memoryview):
        return value.tobytes().hex()
    if isinstance(value, bytes):
        return value.hex()
    if isinstance(value, tuple):
        return [_normalize_value(v) for v in value]
    if isinstance(value, list):
        return [_normalize_value(v) for v in value]
    if isinstance(value, dict):
        return {k: _normalize_value(v) for k, v in value.items()}
    return value


def _split_sql_statements(script: str) -> list[str]:
    statements: list[str] = []
    current: list[str] = []

    in_single_quote = False
    in_double_quote = False
    in_line_comment = False
    in_block_comment = False

    i = 0
    while i < len(script):
        ch = script[i]
        nxt = script[i + 1] if i + 1 < len(script) else ""

        if in_line_comment:
            current.append(ch)
            if ch == "\n":
                in_line_comment = False
            i += 1
            continue

        if in_block_comment:
            current.append(ch)
            if ch == "*" and nxt == "/":
                current.append(nxt)
                in_block_comment = False
                i += 2
                continue
            i += 1
            continue

        if not in_single_quote and not in_double_quote:
            if ch == "-" and nxt == "-":
                current.append(ch)
                current.append(nxt)
                in_line_comment = True
                i += 2
                continue
            if ch == "/" and nxt == "*":
                current.append(ch)
                current.append(nxt)
                in_block_comment = True
                i += 2
                continue

        if ch == "'" and not in_double_quote:
            if in_single_quote and nxt == "'":
                current.append(ch)
                current.append(nxt)
                i += 2
                continue
            in_single_quote = not in_single_quote
            current.append(ch)
            i += 1
            continue

        if ch == '"' and not in_single_quote:
            in_double_quote = not in_double_quote
            current.append(ch)
            i += 1
            continue

        if ch == ";" and not in_single_quote and not in_double_quote:
            statement = "".join(current).strip()
            if statement:
                statements.append(statement)
            current = []
            i += 1
            continue

        current.append(ch)
        i += 1

    tail = "".join(current).strip()
    if tail:
        statements.append(tail)
    return statements


def _format_error(exc: Exception) -> str:
    diag = getattr(exc, "diag", None)
    primary = getattr(diag, "message_primary", None) or str(exc)
    primary = str(primary).splitlines()[0]

    suffixes: list[str] = []
    sqlstate = getattr(exc, "sqlstate", None) or getattr(diag, "sqlstate", None)
    if sqlstate:
        suffixes.append(f"SQLSTATE={sqlstate}")

    detail = getattr(diag, "message_detail", None)
    if detail:
        suffixes.append(f"DETAIL={str(detail).splitlines()[0]}")

    hint = getattr(diag, "message_hint", None)
    if hint:
        suffixes.append(f"HINT={str(hint).splitlines()[0]}")

    if suffixes:
        return f"{type(exc).__name__}: {primary} ({'; '.join(suffixes)})"
    return f"{type(exc).__name__}: {primary}"


def _safe_close(conn: Any) -> None:
    if conn is None:
        return
    with suppress(Exception):
        conn.close()
