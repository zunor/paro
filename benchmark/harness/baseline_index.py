# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

"""Stable indexes over benchmark result payloads."""

from __future__ import annotations

from dataclasses import dataclass
import json
from typing import Any, Iterable


@dataclass(frozen=True, order=True)
class QueryKey:
    workload: str
    query_id: str
    params_key: str

    @property
    def label(self) -> str:
        if self.params_key == "{}":
            return f"{self.workload}.{self.query_id}"
        return f"{self.workload}.{self.query_id} params={self.params_key}"


@dataclass(frozen=True)
class QueryRecord:
    key: QueryKey
    query: dict[str, Any]


def index_queries(payload: dict[str, Any]) -> dict[QueryKey, QueryRecord]:
    return {record.key: record for record in iter_query_records(payload)}


def iter_query_records(payload: dict[str, Any]) -> Iterable[QueryRecord]:
    for workload in payload.get("workloads", []):
        if not isinstance(workload, dict):
            continue
        workload_name = str(workload.get("name", ""))
        encoded_params = params_key(workload.get("params", {}))
        queries = workload.get("queries", [])
        if not isinstance(queries, list):
            continue
        for query in queries:
            if not isinstance(query, dict):
                continue
            query_id = str(query.get("id", ""))
            key = QueryKey(
                workload=workload_name,
                query_id=query_id,
                params_key=encoded_params,
            )
            yield QueryRecord(key=key, query=query)


def params_key(params: Any) -> str:
    if isinstance(params, dict):
        return json.dumps(params, sort_keys=True, ensure_ascii=False)
    return "{}"
