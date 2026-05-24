# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

"""Staging metadata for newly added gate queries."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
import tomllib
from typing import Any

from .policy import GatePolicy


class StagingError(ValueError):
    """Raised when suite staging metadata is invalid."""


@dataclass(frozen=True)
class StagingQuery:
    workload: str
    query_id: str
    staging_until: date

    def active_on(self, today: date) -> bool:
        return today <= self.staging_until


StagingMap = dict[tuple[str, str], StagingQuery]


def load_staging_queries(
    *,
    root_dir: Path,
    policy: GatePolicy,
    today: date | None = None,
) -> StagingMap:
    current_date = date.today() if today is None else today
    staging: StagingMap = {}
    for source in policy.sources:
        if source.type != "sql_suite" or not source.suite:
            continue
        suite_path = root_dir / "suites" / f"{source.suite}.toml"
        suite = _load_toml(suite_path)
        for index, include in enumerate(_include_items(suite, suite_path), start=1):
            raw_until = include.get("staging_until")
            if raw_until is None:
                continue
            workload = _required_str(include, "workload", suite_path=suite_path, index=index)
            query_ids = _query_ids(include, suite_path=suite_path, index=index)
            until = _staging_date(raw_until, suite_path=suite_path, index=index)
            max_until = current_date + timedelta(days=policy.coverage.new_query_staging_days)
            if until > max_until:
                raise StagingError(
                    f"{suite_path}: include #{index} staging_until {until.isoformat()} exceeds "
                    f"policy max {policy.coverage.new_query_staging_days} days"
                )
            for query_id in query_ids:
                staging[(workload, query_id)] = StagingQuery(
                    workload=workload,
                    query_id=query_id,
                    staging_until=until,
                )
    return staging


def _load_toml(path: Path) -> dict[str, Any]:
    try:
        return tomllib.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise StagingError(f"suite not found: {path}") from exc
    except tomllib.TOMLDecodeError as exc:
        raise StagingError(f"invalid suite TOML {path}: {exc}") from exc


def _include_items(suite: dict[str, Any], suite_path: Path) -> list[dict[str, Any]]:
    include = suite.get("include")
    if not isinstance(include, list):
        raise StagingError(f"{suite_path}: [[include]] is required")
    result = []
    for index, item in enumerate(include, start=1):
        if not isinstance(item, dict):
            raise StagingError(f"{suite_path}: include #{index} must be a table")
        result.append(item)
    return result


def _required_str(item: dict[str, Any], key: str, *, suite_path: Path, index: int) -> str:
    value = item.get(key)
    if isinstance(value, str) and value.strip():
        return value.strip()
    raise StagingError(f"{suite_path}: include #{index} {key} must be a non-empty string")


def _query_ids(item: dict[str, Any], *, suite_path: Path, index: int) -> tuple[str, ...]:
    value = item.get("queries")
    if not isinstance(value, list) or not value:
        raise StagingError(f"{suite_path}: include #{index} staging requires explicit queries")
    query_ids = tuple(query_id.strip() for query_id in value if isinstance(query_id, str) and query_id.strip())
    if not query_ids:
        raise StagingError(f"{suite_path}: include #{index} staging queries must not be empty")
    return query_ids


def _staging_date(value: Any, *, suite_path: Path, index: int) -> date:
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value)
        except ValueError as exc:
            raise StagingError(f"{suite_path}: include #{index} staging_until must be YYYY-MM-DD") from exc
    raise StagingError(f"{suite_path}: include #{index} staging_until must be a date")
