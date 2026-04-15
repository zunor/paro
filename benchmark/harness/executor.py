# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

"""Workload execution and timing."""

from __future__ import annotations

from contextlib import suppress
from dataclasses import dataclass, field
from datetime import date, datetime, time
from decimal import Decimal
import json
import threading
import time as time_module
from typing import Any, Mapping

from .loader import QueryDef, WorkloadDef
from .validator import BenchmarkValidator


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
    operator_profiles: list[dict[str, Any]] = field(default_factory=list)
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
    ):
        self._connection = dict(connection)
        self._iterations = max(int(iterations), 1)
        self._warmup = max(int(warmup), 0)
        self._timeout_seconds = max(int(timeout_seconds), 1)
        self._collect_memory = bool(collect_memory)

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

        try:
            teardown_conn = conn if conn is not None else self.connection_factory()
            self._execute_script(teardown_conn, workload.teardown_sql)
        except Exception as exc:
            result.teardown_status = "FAIL"
            result.teardown_error = _format_error(exc)
        finally:
            _safe_close(conn)
            if "teardown_conn" in locals() and teardown_conn is not conn:
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

            last_rows: list[tuple[Any, ...]] = []
            for _ in range(self._iterations):
                start = time_module.perf_counter()
                rows = self._execute_sql(conn, query.sql, fetch=True)
                query_result.samples_ms.append((time_module.perf_counter() - start) * 1000.0)
                last_rows = rows

            if self._collect_memory:
                query_result.memory_tags_after = self._fetch_memory_tag_rows(conn)
                query_result.memory_after_bytes = _sum_memory_tag_rows(query_result.memory_tags_after)
                query_result.spill_metrics_after = self._fetch_spill_metrics(conn)

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
            raw_json = self._fetch_explain_profile_json(conn, query.sql)
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


def _flatten_explain_profile(raw_json: str) -> list[dict[str, Any]]:
    try:
        document = json.loads(raw_json)
    except json.JSONDecodeError as exc:
        raise ValueError(f"invalid explain JSON: {exc}") from exc
    if not isinstance(document, dict):
        raise ValueError("explain JSON document must be an object")
    plan = document.get("plan")
    if not isinstance(plan, dict):
        raise ValueError("explain JSON document missing object field 'plan'")
    profiles: list[dict[str, Any]] = []
    _append_operator_profile(plan, profiles, "0")
    return profiles


def _append_operator_profile(
    node: Mapping[str, Any],
    profiles: list[dict[str, Any]],
    tree_path: str,
) -> None:
    actual = node.get("actual")
    actual_map = actual if isinstance(actual, dict) else {}

    profiles.append(
        {
            "node_id": _optional_int(node.get("node_id")),
            "operator": str(node.get("operator", "")),
            "tree_path": tree_path,
            "rows": _optional_int(actual_map.get("rows")),
            "loops": _optional_int(actual_map.get("loops")),
            "startup_time_ms": _optional_float(actual_map.get("startup_time_ms")),
            "total_time_ms": _optional_float(actual_map.get("total_time_ms")),
            "spilled": _optional_bool(actual_map.get("spilled")),
            "reported_memory_bytes": _optional_int(actual_map.get("peak_memory_bytes")),
            "temp_storage_bytes": _optional_int(actual_map.get("temp_storage_bytes")),
        }
    )

    children = node.get("children")
    if not isinstance(children, list):
        return
    for index, child in enumerate(children):
        if isinstance(child, dict):
            _append_operator_profile(child, profiles, f"{tree_path}/{index}")


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
