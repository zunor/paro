# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

"""Result validation and plan guard checks."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import threading
from typing import Any, Callable

from .loader import QueryDef


STRONG_VALIDATE_MODES = {"scalar_equals", "ordered_rows", "ordered_digest"}


@dataclass(frozen=True)
class ValidationOutcome:
    status: str
    detail: str | None = None


class BenchmarkValidator:
    def __init__(self, connection_factory: Callable[[], Any], timeout_seconds: int):
        self._connection_factory = connection_factory
        self._timeout_seconds = max(int(timeout_seconds), 1)

    def validate_query(self, query: QueryDef, rows: list[list[Any]]) -> ValidationOutcome:
        mode = query.validate
        expected = query.expected

        if mode == "none":
            return ValidationOutcome("PASS")

        if mode == "scalar_equals":
            actual = _first_scalar(rows)
            if actual == expected:
                return ValidationOutcome("PASS")
            return ValidationOutcome("FAIL", f"expected {expected!r}, got {actual!r}")

        if mode == "scalar_gte":
            actual = _first_scalar(rows)
            if actual is None:
                return ValidationOutcome("FAIL", "query returned no scalar")
            try:
                if actual >= expected:
                    return ValidationOutcome("PASS")
            except TypeError:
                return ValidationOutcome("FAIL", f"cannot compare {actual!r} >= {expected!r}")
            return ValidationOutcome("FAIL", f"expected >= {expected!r}, got {actual!r}")

        if mode == "row_count":
            actual = len(rows)
            if actual == expected:
                return ValidationOutcome("PASS")
            return ValidationOutcome("FAIL", f"expected row_count={expected}, got {actual}")

        if mode == "ordered_rows":
            actual_rows = [_normalize_row(row) for row in rows]
            expected_rows = _normalize_expected_rows(expected)
            if actual_rows == expected_rows:
                return ValidationOutcome("PASS")
            return ValidationOutcome(
                "FAIL",
                f"ordered rows mismatch: expected {expected_rows!r}, got {actual_rows!r}",
            )

        if mode == "ordered_digest":
            if not isinstance(expected, str) or len(expected) != 64:
                return ValidationOutcome("FAIL", "ordered_digest expected must be a SHA-256 hex digest")
            actual = ordered_rows_digest(rows)
            if actual == expected.lower():
                return ValidationOutcome("PASS")
            preview = [_normalize_row(row) for row in rows[:5]]
            return ValidationOutcome(
                "FAIL",
                f"expected ordered digest {expected.lower()}, got {actual}; "
                f"row_count={len(rows)}, first_rows={preview!r}",
            )

        if mode == "text_contains_all":
            haystack = _render_rows_text(rows)
            expected_needles = _normalize_expected_needles(expected)
            missing = [needle for needle in expected_needles if needle not in haystack]
            if not missing:
                return ValidationOutcome("PASS")
            return ValidationOutcome("FAIL", f"missing text fragments: {missing!r}")

        return ValidationOutcome("FAIL", f"unknown validate mode: {mode}")

    def check_plan(self, query: QueryDef, conn: Any | None = None) -> ValidationOutcome:
        if not query.plan_contains:
            return ValidationOutcome("SKIP")

        owned_conn = conn is None
        conn = conn if conn is not None else self._connection_factory()
        try:
            rows = self._execute_with_timeout(conn, f"EXPLAIN {query.sql}")
            plan_text = "\n".join(str(row[0]) for row in rows if row)
            upper_plan = plan_text.upper()
            missing = [token for token in query.plan_contains if token.upper() not in upper_plan]
            if missing:
                return ValidationOutcome("FAIL", f"missing tokens: {', '.join(missing)}")
            return ValidationOutcome("PASS")
        except TimeoutError:
            return ValidationOutcome("FAIL", "EXPLAIN timed out")
        except Exception as exc:  # pragma: no cover - depends on runtime DB behavior
            return ValidationOutcome("FAIL", f"{type(exc).__name__}: {exc}")
        finally:
            if owned_conn:
                _safe_close(conn)

    def _execute_with_timeout(self, conn: Any, sql: str) -> list[tuple[Any, ...]]:
        holder: dict[str, Any] = {}

        def worker() -> None:
            try:
                with conn.cursor() as cur:
                    cur.execute(sql, prepare=False)
                    holder["rows"] = cur.fetchall()
            except BaseException as exc:  # pragma: no cover - thread boundary
                holder["error"] = exc

        thread = threading.Thread(target=worker, daemon=True)
        thread.start()
        thread.join(self._timeout_seconds)
        if thread.is_alive():
            _safe_close(conn)
            thread.join(0.2)
            raise TimeoutError(f"timed out after {self._timeout_seconds}s")
        if "error" in holder:
            raise holder["error"]
        rows = holder.get("rows")
        if isinstance(rows, list):
            return rows
        return []


def _first_scalar(rows: list[list[Any]]) -> Any:
    if not rows:
        return None
    first = rows[0]
    if not first:
        return None
    return first[0]


def _normalize_expected_rows(expected: Any) -> list[list[Any]]:
    if not isinstance(expected, list):
        raise ValueError(f"ordered_rows expected must be list[list[Any]], got: {type(expected).__name__}")
    normalized: list[list[Any]] = []
    for row in expected:
        if isinstance(row, list):
            normalized.append([_normalize_value(v) for v in row])
        else:
            normalized.append([_normalize_value(row)])
    return normalized


def ordered_rows_digest(rows: list[list[Any]]) -> str:
    """Return a stable, type-preserving digest for an ordered SQL result.

    Executor normalization has already converted exact DECIMAL and temporal
    values to strings. JSON then supplies unambiguous row/value boundaries,
    while sorted object keys keep nested values deterministic.
    """

    normalized = [_normalize_row(row) for row in rows]
    payload = json.dumps(
        normalized,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _normalize_row(row: list[Any]) -> list[Any]:
    return [_normalize_value(v) for v in row]


def _normalize_expected_needles(expected: Any) -> list[str]:
    if not isinstance(expected, list) or not all(isinstance(item, str) for item in expected):
        raise ValueError(
            f"text_contains_all expected must be list[str], got: {type(expected).__name__}"
        )
    return [item for item in (needle.strip() for needle in expected) if item]


def _render_rows_text(rows: list[list[Any]]) -> str:
    rendered_rows: list[str] = []
    for row in rows:
        rendered_rows.append("\t".join(str(_normalize_value(value)) for value in row))
    return "\n".join(rendered_rows)


def _normalize_value(value: Any) -> Any:
    if isinstance(value, tuple):
        return [_normalize_value(v) for v in value]
    if isinstance(value, list):
        return [_normalize_value(v) for v in value]
    if isinstance(value, dict):
        return {k: _normalize_value(v) for k, v in value.items()}
    return value


def _safe_close(conn: Any) -> None:
    if conn is None:
        return
    try:
        conn.close()
    except Exception:
        pass
