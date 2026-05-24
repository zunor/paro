# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

"""SQL benchmark suite measurement source."""

from __future__ import annotations

from .context import SourceContext, SourceMeasurement


class SqlSuiteSource:
    source_type = "sql_suite"

    def execute(self, source, context: SourceContext) -> SourceMeasurement:
        if not source.suite:
            raise ValueError(f"source '{source.name}' is missing suite")

        runner = context.runner_module
        retry_queries = _retry_queries_by_workload(context)
        args = runner.BenchmarkInvocation(
            suite=source.suite,
            pid=context.pid,
            query_ids_by_workload=retry_queries,
        )
        config = runner.resolve_config(args)
        result = runner.execute_workloads(config, args, {})
        return SourceMeasurement(
            source=source,
            payload=result.payload,
            result_path=result.result_path,
            summary_path=result.summary_path,
            failed=result.failed,
        )


def _retry_queries_by_workload(context: SourceContext) -> dict[str, tuple[str, ...]] | None:
    if not context.retry_query_keys:
        return None
    grouped: dict[str, list[str]] = {}
    for key in sorted(context.retry_query_keys):
        grouped.setdefault(key.workload, []).append(key.query_id)
    return {workload: tuple(query_ids) for workload, query_ids in grouped.items()}
